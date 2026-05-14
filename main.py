import os
import json
import asyncio
import jwt
import logging
from datetime import datetime, timezone
# ¡Aquí está la corrección! Añadimos Request
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
import websockets

# Configuración básica de logging para ver los mensajes en consola
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = FastAPI()

# Configuración mediante variables de entorno para mayor seguridad en Render
OPENAI_WS_URL = "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AVAYA_SECRET_KEY = os.getenv("AVAYA_SECRET_KEY")

def get_current_timestamp():
    """Genera el timestamp en el formato requerido por Avaya ISO-8601 UTC."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')

def verificar_token_avaya(auth_header: str):
    """
    Verifica el token JWT enviado por Avaya en el upgrade request.
    """
    logging.info("Verificando token JWT de Avaya...")
    if not auth_header or not auth_header.startswith("Bearer "):
        logging.error("Fallo de autenticación: Token faltante o formato incorrecto.")
        raise HTTPException(status_code=401, detail="Token faltante o inválido")
    
    token = auth_header.split(" ")[1]
    try:
        # Validación del JWT usando la clave de seguridad (Fase 1 usa HS256)
        payload = jwt.decode(token, AVAYA_SECRET_KEY, algorithms=["HS256"])
        logging.info("Token de Avaya verificado exitosamente.")
        return payload
    except jwt.ExpiredSignatureError:
        logging.error("Fallo de autenticación: El token ha expirado.")
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidTokenError:
        logging.error("Fallo de autenticación: Token inválido.")
        raise HTTPException(status_code=401, detail="Token inválido")

async def open_openai_connection():
    """
    Establece la conexión como cliente hacia el WebSocket de OpenAI Realtime.
    """
    logging.info(f"Iniciando conexión con OpenAI en {OPENAI_WS_URL}...")
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "OpenAI-Beta": "realtime=v1"
    }
    ws = await websockets.connect(OPENAI_WS_URL, additional_headers=headers)
    logging.info("Conexión con OpenAI establecida correctamente.")
    return ws

# 1. Endpoints HTTP de depuración (Por si la petición pierde el formato WebSocket)
@app.get("/avaya-rcms")
@app.get("/avaya-rcms/")
@app.get("/{path:path}") # Atrapa cualquier otra ruta HTTP
async def debug_http_get(request: Request, path: str = ""):
    logging.warning(f"¡Atención! Petición HTTP GET recibida en lugar de WebSocket en la ruta: /{path}")
    logging.warning(f"Headers recibidos: {request.headers}")
    return {"error": "Este endpoint espera una conexión WebSocket, no HTTP convencional."}

# 2. Endpoints WebSocket (Con catch-all para ver si Avaya pide otra ruta)
@app.websocket("/avaya-rcms")
@app.websocket("/avaya-rcms/")
@app.websocket("/{path:path}") # Atrapa cualquier otra ruta WebSocket
async def avaya_rcms_endpoint(websocket: WebSocket, path: str = ""):
    logging.info(f"NUEVA CONEXIÓN: Recibiendo solicitud WebSocket de Avaya en la ruta: /{path}")
    
    # 1. Autenticación (Validar JWT en los headers antes de aceptar)
    auth_header = websocket.headers.get("authorization")
    try:
        verificar_token_avaya(auth_header)
        await websocket.accept()
        logging.info("Conexión WebSocket con Avaya ACEPTADA.")
    except Exception as e:
        logging.error(f"Rechazando conexión WebSocket: {str(e)}")
        await websocket.close(code=1008) # Policy Violation
        return

    openai_ws = None
    session_id = None
    sequence_num = 1 # Para responder con sequence numbers dinámicos

    try:
        # 2. Conectar a OpenAI
        openai_ws = await open_openai_connection()

        # Tarea en segundo plano para leer desde OpenAI y enviar a Avaya
        async def receive_from_openai():
            logging.info("Iniciando listener para recibir mensajes de OpenAI...")
            async for openai_message in openai_ws:
                data = json.loads(openai_message)
                
                # Si OpenAI envía audio, lo empaquetamos en el formato 'media' de Avaya
                if data.get("type") == "response.audio.delta":
                    # Nota: Cambiamos a logging.debug para no inundar la consola con cada frame de audio
                    logging.debug("Audio recibido de OpenAI, reenviando a Avaya.")
                    avaya_media_msg = {
                        "type": "media",
                        "bid": 0, # Reemplazar con el Bearer ID correcto
                        "src": "rx",
                        "audio": data["delta"] # OpenAI envía base64
                    }
                    await websocket.send_text(json.dumps(avaya_media_msg))
                elif data.get("type") != "response.audio.delta":
                    # Log para ver otros eventos de OpenAI (transcripciones, status, etc.)
                    logging.info(f"Evento de OpenAI recibido: {data.get('type')}")

        asyncio.create_task(receive_from_openai())

        # 3. Bucle principal: Leer desde Avaya y procesar
        logging.info("Iniciando bucle principal para escuchar mensajes de Avaya...")
        while True:
            avaya_message = await websocket.receive_text()
            data = json.loads(avaya_message)
            msg_type = data.get("type")

            # Evitamos registrar cada paquete "media" como INFO para no saturar los logs
            if msg_type != "media":
                logging.info(f"Avaya -> Servidor: Recibido evento '{msg_type}'")

            if msg_type == "session.start":
                session_id = data.get("sessionId")
                logging.info(f"Configurando nueva sesión. SessionId: {session_id}")
                
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
                logging.info(f"Servidor -> Avaya: Enviado 'session.started' (SeqNum: {sequence_num})")
                sequence_num += 1

            elif msg_type == "bot.start":
                logging.info(f"Iniciando Bot para el endpoint: {data['payload'].get('endpointId')}")
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
                logging.info(f"Servidor -> Avaya: Enviado 'bot.started' (SeqNum: {sequence_num})")
                sequence_num += 1

            elif msg_type == "media":
                logging.debug("Audio recibido de Avaya, reenviando a OpenAI.")
                audio_base64 = data.get("audio")
                if audio_base64:
                    openai_audio_msg = {
                        "type": "input_audio_buffer.append",
                        "audio": audio_base64
                    }
                    await openai_ws.send(json.dumps(openai_audio_msg))
                    
            elif msg_type in ["session.end", "bot.end"]:
                logging.info(f"Solicitud de finalización recibida: {msg_type}")
                # Aquí idealmente enviarías un session.ended o bot.ended de respuesta

    except WebSocketDisconnect:
        logging.warning(f"Desconexión del WebSocket de Avaya (Sesión: {session_id})")
    except Exception as e:
        logging.error(f"Error inesperado en la sesión {session_id}: {str(e)}")
    finally:
        logging.info("Limpiando recursos y cerrando conexiones.")
        if openai_ws:
            await openai_ws.close()
            logging.info("Conexión con OpenAI cerrada.")

if __name__ == "__main__":
    import uvicorn
    logging.info("Iniciando servidor Uvicorn...")
    uvicorn.run(app, host="0.0.0.0", port=8000)