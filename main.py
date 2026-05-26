import os
import json
import asyncio
import jwt
import base64
import logging
import audioop
import websockets
from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = FastAPI()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AVAYA_SECRET_KEY = os.getenv("AVAYA_SECRET_KEY")
OPENAI_WS_URL = "wss://api.openai.com/v1/realtime?model=gpt-realtime-mini"

def get_current_timestamp():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')

def verificar_token_avaya(auth_header: str):
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token faltante")
    token = auth_header.split(" ")[1]
    return jwt.decode(token, AVAYA_SECRET_KEY, algorithms=["HS256"])

@app.websocket("/avaya-rcms")
async def avaya_rcms_endpoint(websocket: WebSocket):
    auth_header = websocket.headers.get("authorization")
    try:
        verificar_token_avaya(auth_header)
        await websocket.accept()
        logging.info("Conexión WebSocket con Avaya ACEPTADA.")
    except Exception as e:
        await websocket.close(code=1008)
        return

    session_id = None
    sequence_num = 1 

    # Conexión directa al WebSocket Realtime de OpenAI
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }

    try:
        async with websockets.connect(OPENAI_WS_URL, additional_headers=headers) as openai_ws:
            logging.info("Conexión con OpenAI Realtime establecida.")

            # TAREA 1: Escuchar a OpenAI y enviar a Avaya
            async def receive_from_openai():
                async for openai_message in openai_ws:
                    data = json.loads(openai_message)
                    
                    if data.get("type") == "response.audio.delta":
                        # OpenAI envía PCM16 a 24kHz en Base64
                        pcm_24k_bytes = base64.b64decode(data["delta"])
                        
                        # Transcodificar: PCM16 24kHz -> PCMU 8kHz
                        pcm_8k, _ = audioop.ratecv(pcm_24k_bytes, 2, 1, 24000, 8000, None)
                        pcmu_bytes = audioop.lin2ulaw(pcm_8k, 2)
                        
                        # Enviar a Avaya en el formato requerido
                        avaya_media_msg = {
                            "type": "media",
                            "bid": 0, 
                            "src": "rx",
                            "audio": base64.b64encode(pcmu_bytes).decode('utf-8')
                        }
                        await websocket.send_text(json.dumps(avaya_media_msg))
                    
                    elif data.get("type") == "error":
                        logging.error(f"Error de OpenAI: {data}")

            # Iniciamos la tarea de escucha en segundo plano
            openai_listen_task = asyncio.create_task(receive_from_openai())

            # TAREA 2: Escuchar a Avaya y enviar a OpenAI
            decoder = json.JSONDecoder()
            while True:
                avaya_message = await websocket.receive_text()
                idx = 0
                msg_length = len(avaya_message)
                
                while idx < msg_length:
                    while idx < msg_length and avaya_message[idx].isspace():
                        idx += 1
                    if idx >= msg_length:
                        break
                    
                    data, chunk_len = decoder.raw_decode(avaya_message[idx:])
                    idx += chunk_len
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
                                    "mediaCodecs": [["audio", "PCMU", 8000, 1]],
                                    "transportEncoding": "base64"
                                }
                            }
                        }
                        await websocket.send_text(json.dumps(response))
                        sequence_num += 1

                        # Configurar la sesión de OpenAI (voz, instrucciones, etc.)
                        session_update = {
                            "type": "session.update",
                            "session": {
                                "type": "realtime",  # <-- PARÁMETRO REQUERIDO AGREGADO
                                "instructions": "Eres un asistente de voz conciso. Responde rápidamente.",
                                "modalities": ["text", "audio"],
                                "voice": "alloy",
                                "turn_detection": {"type": "server_vad"}
                            }
                        }
                        await openai_ws.send(json.dumps(session_update))

                    elif msg_type == "bot.start":
                        response = {
                            "version": "1.0.0",
                            "type": "bot.started",
                            "sessionId": session_id,
                            "sequenceNum": sequence_num,
                            "timestamp": get_current_timestamp(),
                            "payload": {"endpointId": data["payload"]["endpointId"]}
                        }
                        await websocket.send_text(json.dumps(response))
                        sequence_num += 1

                    elif msg_type == "media":
                        audio_base64 = data.get("audio")
                        
                        if audio_base64:
                            # Recibimos audio PCMU 8kHz de Avaya 
                            pcmu_bytes = base64.b64decode(audio_base64)
                            
                            # Transcodificar: PCMU 8kHz -> PCM16 24kHz para OpenAI
                            pcm_8k = audioop.ulaw2lin(pcmu_bytes, 2)
                            pcm_24k, _ = audioop.ratecv(pcm_8k, 2, 1, 8000, 24000, None)
                            
                            # Enviar buffer de audio al websocket de OpenAI
                            openai_audio_msg = {
                                "type": "input_audio_buffer.append",
                                "audio": base64.b64encode(pcm_24k).decode('utf-8')
                            }
                            await openai_ws.send(json.dumps(openai_audio_msg))

    except WebSocketDisconnect:
        logging.warning(f"Desconexión del WebSocket de Avaya.")
        if 'openai_listen_task' in locals():
            openai_listen_task.cancel()
    except Exception as e:
        logging.error(f"Error general: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)