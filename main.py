import os
import json
import asyncio
import jwt
import base64
import logging
import audioop  # Nota: deprecado en Python 3.13, considera usar pydub o PyAV en versiones nuevas
import numpy as np
from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request

# Opcional pero recomendado: cargar variables desde un archivo .env local
# Asegúrate de instalarlo con: pip install python-dotenv
from dotenv import load_dotenv
load_dotenv()

# Importaciones del SDK de OpenAI (basadas en tu imagen)
from agents import Agent
from agents.voice import AudioInput, SingleAgentVoiceWorkflow, VoicePipeline

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI()

# 1. CARGA DE VARIABLES DE ENTORNO
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AVAYA_SECRET_KEY = os.getenv("AVAYA_SECRET_KEY")

if not OPENAI_API_KEY:
    logging.warning("¡CUIDADO! No se encontró OPENAI_API_KEY en las variables de entorno.")

def get_current_timestamp():
    """Genera el timestamp en el formato requerido por Avaya ISO-8601 UTC."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')

def verificar_token_avaya(auth_header: str):
    """Verifica el JWT de Avaya usando la clave de seguridad."""
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token faltante o formato incorrecto")
    
    token = auth_header.split(" ")[1]
    try:
        return jwt.decode(token, AVAYA_SECRET_KEY, algorithms=["HS256"])
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token inválido: {str(e)}")

# --- Lógica del Agente de OpenAI ---
# 2. INYECCIÓN EXPLÍCITA DE LA CREDENCIAL EN EL AGENTE
agent = Agent(
    name="Assistant",
    instructions="Eres un asistente de voz útil y amable. Respondes de forma concisa.",
    model="gpt-4o-realtime-preview-2024-12-17",
    api_key=OPENAI_API_KEY  # <-- Aquí se inyecta la credencial explícitamente
)

# Clase auxiliar para convertir el flujo del WebSocket en un iterador asíncrono para AudioInput
class AsyncAudioStreamer:
    def __init__(self):
        self.queue = asyncio.Queue()

    async def add_data(self, data: bytes):
        await self.queue.put(data)

    async def __aiter__(self):
        while True:
            chunk = await self.queue.get()
            if chunk is None:  # Señal de finalización
                break
            yield chunk

@app.websocket("/avaya-rcms")
@app.websocket("/avaya-rcms/")
@app.websocket("/{path:path}")
async def avaya_rcms_endpoint(websocket: WebSocket, path: str = ""):
    auth_header = websocket.headers.get("authorization")
    try:
        verificar_token_avaya(auth_header)
        await websocket.accept()
        logging.info("Conexión WebSocket con Avaya ACEPTADA.")
    except Exception as e:
        logging.error(f"Rechazando conexión: {str(e)}")
        await websocket.close(code=1008)
        return

    session_id = None
    sequence_num = 1 
    
    # Instanciamos el puente de audio para OpenAI
    audio_streamer = AsyncAudioStreamer()
    audio_input = AudioInput(buffer=audio_streamer) 
    pipeline = VoicePipeline(workflow=SingleAgentVoiceWorkflow(agent))

    # Tarea en segundo plano para procesar las respuestas de OpenAI hacia Avaya
    async def process_openai_responses():
        try:
            result = await pipeline.run(audio_input)
            async for event in result.stream():
                if event.type == "voice_stream_event_audio":
                    # 1. Recibimos PCM crudo de OpenAI (usualmente 24kHz, 16-bit)
                    raw_pcm = event.data 
                    
                    # 2. TRANSCODIFICACIÓN: PCM 24kHz -> PCMU 8kHz (Requerido por Avaya)
                    # pcm_8k, _ = audioop.ratecv(raw_pcm, 2, 1, 24000, 8000, None)
                    # pcmu_audio = audioop.lin2ulaw(pcm_8k, 2)
                    
                    # 3. Codificar en Base64 para Avaya
                    audio_b64 = base64.b64encode(raw_pcm).decode('utf-8')  # <-- Reemplaza raw_pcm con pcmu_audio
                    
                    avaya_media_msg = {
                        "type": "media",
                        "bid": 0, 
                        "src": "rx",
                        "audio": audio_b64 
                    }
                    await websocket.send_text(json.dumps(avaya_media_msg))
        except Exception as e:
            logging.error(f"Error en el pipeline de OpenAI: {e}")

    # Arrancamos la tarea que escucha a OpenAI
    openai_task = asyncio.create_task(process_openai_responses())

    try:
        decoder = json.JSONDecoder()
        while True:
            avaya_message = await websocket.receive_text()
            
            # Procesador para manejar el batching de Avaya RCMS
            idx = 0
            msg_length = len(avaya_message)
            
            while idx < msg_length:
                while idx < msg_length and avaya_message[idx].isspace():
                    idx += 1
                if idx >= msg_length:
                    break
                
                try:
                    data, chunk_len = decoder.raw_decode(avaya_message[idx:])
                    idx += chunk_len
                except json.JSONDecodeError:
                    logging.error("Error decodificando parte del JSON de Avaya.")
                    break
                
                msg_type = data.get("type")

                if msg_type == "session.start":
                    session_id = data.get("sessionId")
                    response = {
                        "version": "1.0.0",
                        "type": "session.started",
                        "sessionId": session_id,
                        "sequenceNum": sequence_num,
                        "timestamp": get_current_timestamp(),
                        "payload": {
                            "services": ["bot"],
                            "mediaTransport": {
                                "type": "avaya-wss",
                                "mediaCodecs": [["audio", "PCMU", 8000, 1]], # Solicitamos PCMU a 8kHz
                                "transportEncoding": "base64"
                            }
                        }
                    }
                    await websocket.send_text(json.dumps(response))
                    sequence_num += 1

                elif msg_type == "bot.start":
                    response = {
                        "version": "1.0.0",
                        "type": "bot.started",
                        "sessionId": session_id,
                        "sequenceNum": sequence_num,
                        "timestamp": get_current_timestamp(),
                        "payload": {
                            "endpointId": data["payload"]["endpointId"]
                        }
                    }
                    await websocket.send_text(json.dumps(response))
                    sequence_num += 1

                elif msg_type == "media":
                    audio_base64 = data.get("audio")
                    if audio_base64:
                        # 1. Decodificamos el Base64 de Avaya a bytes crudos (PCMU)
                        pcmu_bytes = base64.b64decode(audio_base64)
                        
                        # 2. TRANSCODIFICACIÓN: PCMU 8kHz -> PCM 24kHz (Requerido por OpenAI)
                        # pcm_8k = audioop.ulaw2lin(pcmu_bytes, 2)
                        # pcm_24k, _ = audioop.ratecv(pcm_8k, 2, 1, 8000, 24000, None)
                        
                        # 3. Enviamos al buffer del agente
                        await audio_streamer.add_data(pcmu_bytes)  # <-- Reemplaza pcmu_bytes con pcm_24k
                        
    except WebSocketDisconnect:
        logging.warning(f"Desconexión del WebSocket de Avaya (Sesión: {session_id})")
    finally:
        # Enviamos None para cerrar el iterador asíncrono y apagar la tarea de OpenAI limpiamente
        await audio_streamer.add_data(None)
        openai_task.cancel()

if __name__ == "__main__":
    import uvicorn
    logging.info("Iniciando servidor Uvicorn...")
    uvicorn.run(app, host="0.0.0.0", port=8000)