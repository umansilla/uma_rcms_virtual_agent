import os
import json
import asyncio
import jwt
import base64
import logging
import audioop
import time
import websockets
from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException

from dotenv import load_dotenv
load_dotenv()

# Intentar importar la librería G.722
try:
    import G722 as g722
    G722_AVAILABLE = True
except ImportError:
    G722_AVAILABLE = False
    logging.warning("Módulo G722 no disponible. Instálalo con 'pip install g722' para mejor calidad de audio.")

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
    session_ingress_bid = 0
    
    # Variables de estado para la configuración negociada
    active_codec = "PCMU"
    active_sample_rate = 8000

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }

    try:
        async with websockets.connect(OPENAI_WS_URL, additional_headers=headers) as openai_ws:
            logging.info("Conexión con OpenAI Realtime establecida.")

            # TAREA 1: Escuchar a OpenAI y enviar a Avaya (EGRESS -> INGRESS)
            async def receive_from_openai():
                avaya_asn = 1
                
                async for openai_message in openai_ws:
                    data = json.loads(openai_message)
                    
                    if data.get("type") == "response.audio.delta":
                        pcm_24k_bytes = base64.b64decode(data["delta"])
                        
                        # 1. Cambiar la frecuencia de muestreo de OpenAI a la de Avaya
                        pcm_target, _ = audioop.ratecv(pcm_24k_bytes, 2, 1, 24000, active_sample_rate, None)
                        
                        # 2. Codificar al formato negociado por Avaya
                        if active_codec == "PCMU":
                            avaya_audio_bytes = audioop.lin2ulaw(pcm_target, 2)
                        elif active_codec == "PCMA":
                            avaya_audio_bytes = audioop.lin2alaw(pcm_target, 2)
                        elif active_codec == "G722" and G722_AVAILABLE:
                            avaya_audio_bytes = g722.encode(pcm_target)
                        else:
                            # Fallback seguro
                            avaya_audio_bytes = audioop.lin2ulaw(pcm_target, 2)
                        
                        # 3. Formatear y enviar
                        avaya_media_msg = {
                            "type": "media",
                            "bid": session_ingress_bid,
                            "asn": avaya_asn,
                            "ts": int(time.time() * 1_000_000),
                            "audio": base64.b64encode(avaya_audio_bytes).decode('utf-8')
                        }
                        await websocket.send_text(json.dumps(avaya_media_msg))
                        avaya_asn += 1
                    
                    elif data.get("type") == "error":
                        logging.error(f"Error de OpenAI: {data}")

            openai_listen_task = asyncio.create_task(receive_from_openai())

            # TAREA 2: Escuchar a Avaya y enviar a OpenAI (INGRESS -> EGRESS)
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
                        
                        # 1. LEER LOS BIDS Y FLUJOS
                        media_endpoints = data.get("payload", {}).get("mediaEndpoints", [])
                        if media_endpoints:
                            flows = media_endpoints[0].get("flows", {}).get("audio", {})
                            ingress_info = flows.get("ingress", {})
                            session_ingress_bid = ingress_info.get("bid", 0)

                        # 2. NEGOCIACIÓN DINÁMICA DE CÓDECS
                        media_transports = data.get("payload", {}).get("mediaTransports", [])
                        offered_codecs = media_transports[0].get("mediaCodecs", []) if media_transports else []
                        
                        selected_codec = None
                        # Prioridad: G722 -> PCMU -> PCMA
                        for pref in ["G722", "PCMU", "PCMA"]:
                            if pref == "G722" and not G722_AVAILABLE:
                                continue
                            for c in offered_codecs:
                                if c[1] == pref:
                                    selected_codec = c
                                    break
                            if selected_codec:
                                break
                                
                        if not selected_codec:
                            selected_codec = ["audio", "PCMU", 8000, 1]  # Fallback extremo
                            
                        active_codec = selected_codec[1]
                        active_sample_rate = selected_codec[2]
                        logging.info(f"Códec negociado: {active_codec} a {active_sample_rate}Hz")

                        # 3. RESPONDER CONFIGURANDO CÓDEC Y TRAMA (20ms)
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
                                    "preferredPTimeMs": 20,          # <-- Configuración a 20ms
                                    "mediaCodecs": [selected_codec], # <-- Se confirma el códec elegido
                                    "transportEncoding": "base64"
                                }
                            }
                        }
                        await websocket.send_text(json.dumps(response))
                        sequence_num += 1

                        # Configurar sesión en OpenAI
                        session_update = {
                            "type": "session.update",
                            "session": {
                                "type": "realtime",
                                "instructions": "Eres un asistente de voz amable. Habla en español. Responde de forma muy breve."
                            }
                        }
                        await openai_ws.send(json.dumps(session_update))

                        # Forzar saludo inicial de la IA
                        greeting = {
                            "type": "response.create",
                            "response": {
                                "instructions": "Saluda al usuario diciendo: 'Hola, el sistema está en línea'."
                            }
                        }
                        await openai_ws.send(json.dumps(greeting))

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
                            avaya_audio_bytes = base64.b64decode(audio_base64)
                            
                            # 1. Decodificar según el códec negociado
                            if active_codec == "PCMU":
                                pcm_native = audioop.ulaw2lin(avaya_audio_bytes, 2)
                            elif active_codec == "PCMA":
                                pcm_native = audioop.alaw2lin(avaya_audio_bytes, 2)
                            elif active_codec == "G722" and G722_AVAILABLE:
                                pcm_native = g722.decode(avaya_audio_bytes)
                            else:
                                pcm_native = audioop.ulaw2lin(avaya_audio_bytes, 2)
                            
                            # 2. Resamplear de la frecuencia nativa a los 24kHz de OpenAI
                            pcm_24k, _ = audioop.ratecv(pcm_native, 2, 1, active_sample_rate, 24000, None)
                            
                            openai_audio_msg = {
                                "type": "input_audio_buffer.append",
                                "audio": base64.b64encode(pcm_24k).decode('utf-8')
                            }
                            await openai_ws.send(json.dumps(openai_audio_msg))

    except WebSocketDisconnect:
        logging.warning("Desconexión del WebSocket de Avaya.")
        if 'openai_listen_task' in locals():
            openai_listen_task.cancel()
    except Exception as e:
        logging.error(f"Error general: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)