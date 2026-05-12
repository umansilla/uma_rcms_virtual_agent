import os
import json
import asyncio
import jwt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
import websockets

app = FastAPI()

# Configuración mediante variables de entorno para mayor seguridad en Render
OPENAI_WS_URL = "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AVAYA_SECRET_KEY = os.getenv("AVAYA_SECRET_KEY")


def verificar_token_avaya(auth_header: str):
    """
    Verifica el token JWT enviado por Avaya en el upgrade request.
    """
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token faltante o inválido")
    
    token = auth_header.split(" ")[1]
    try:
        # Validación del JWT usando la clave de seguridad (Fase 1 usa HS256)
        payload = jwt.decode(token, AVAYA_SECRET_KEY, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido")

async def open_openai_connection():
    """
    Establece la conexión como cliente hacia el WebSocket de OpenAI Realtime.
    """
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "OpenAI-Beta": "realtime=v1"
    }
    return await websockets.connect(OPENAI_WS_URL, additional_headers=headers)

@app.websocket("/avaya-rcms")
async def avaya_rcms_endpoint(websocket: WebSocket):
    # 1. Autenticación (Validar JWT en los headers antes de aceptar)
    auth_header = websocket.headers.get("authorization")
    try:
        verificar_token_avaya(auth_header)
        await websocket.accept()
    except Exception as e:
        await websocket.close(code=1008) # Policy Violation
        return

    openai_ws = None
    session_id = None

    try:
        # 2. Conectar a OpenAI
        openai_ws = await open_openai_connection()

        # Tarea en segundo plano para leer desde OpenAI y enviar a Avaya
        async def receive_from_openai():
            async for openai_message in openai_ws:
                data = json.loads(openai_message)
                # Si OpenAI envía audio, lo empaquetamos en el formato 'media' de Avaya
                if data.get("type") == "response.audio.delta":
                    avaya_media_msg = {
                        "type": "media",
                        "bid": 0, # Reemplazar con el Bearer ID correcto del session.start
                        "src": "rx",
                        "audio": data["delta"] # OpenAI envía base64
                    }
                    await websocket.send_text(json.dumps(avaya_media_msg))

        asyncio.create_task(receive_from_openai())

        # 3. Bucle principal: Leer desde Avaya y procesar
        while True:
            avaya_message = await websocket.receive_text()
            data = json.loads(avaya_message)
            msg_type = data.get("type")

            if msg_type == "session.start":
                session_id = data.get("sessionId")
                # Responder a Avaya aceptando la sesión y configurando base64
                # Nota: Avaya requiere millisecond granularity para el timestamp
                response = {
                    "version": "1.0.0",
                    "type": "session.started",
                    "sessionId": session_id,
                    "sequenceNum": 1,
                    "timestamp": "2025-01-10T22:40:31.000Z", # Generar dinámicamente
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

            elif msg_type == "bot.start":
                # Confirmar el inicio del bot
                response = {
                    "version": "1.0.0",
                    "type": "bot.started",
                    "sessionId": session_id,
                    "sequenceNum": 2,
                    "timestamp": "2025-01-10T22:40:31.000Z",
                    "payload": {
                        "endpointId": data["payload"]["endpointId"]
                    }
                }
                await websocket.send_text(json.dumps(response))

            elif msg_type == "media":
                # Extraer audio base64 de Avaya y enviarlo a OpenAI
                audio_base64 = data.get("audio")
                if audio_base64:
                    openai_audio_msg = {
                        "type": "input_audio_buffer.append",
                        "audio": audio_base64
                    }
                    await openai_ws.send(json.dumps(openai_audio_msg))

    except WebSocketDisconnect:
        print(f"Desconexión de Avaya para la sesión {session_id}")
    finally:
        if openai_ws:
            await openai_ws.close()

if __name__ == "__main__":
    import uvicorn
    # En producción, asegúrate de configurar TLS/WSS en tu servidor proxy o directamente aquí
    uvicorn.run(app, host="0.0.0.0", port=8000)