from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from kafka import KafkaConsumer
import uvicorn
import room
import json
import threading
import logging
import asyncio


@asynccontextmanager
async def lifespan(_: FastAPI):
    # startup
    yield
    # shutting down
    room.cleanup()

app = FastAPI(lifespan=lifespan)
logger = logging.getLogger(__name__)


@app.post("/api/room")
def new_room():
    # topic exists as game-room 
    room_id, p1 = room.create_room()
    logger.info(f"Room ({room_id}) created by player {p1}")
    return {
        "id": room_id,
        "p1": p1,
    }


@app.get("/api/room/{room_id}")
def join_room(room_id):
    try:
        p2 = room.join_room(room_id)
        logger.info(f"Successfully joined room {room_id} as player {p2}")
        return {"status": "success", "p2": p2}
    except Exception as e:
        logger.error(e)
        return {"status": "failed", "message": str(e)}


def display_broadcast(websocket: WebSocket, consumer: KafkaConsumer):
    for msg in consumer:
        val = json.loads(msg.value.decode('utf-8'))
        logger.info(f"Message received from kafka: \"{val}\"")

        if websocket.client_state.value != 1:  # 1 = CONNECTED
            logger.info("WebSocket disconnected, exiting consumer loop")
            break

        asyncio.run( websocket.send_json(val) )

@app.websocket("/ws/room/{room_id}/{p_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str, p_id: str):
    await websocket.accept()

    consumer = KafkaConsumer(room_id, bootstrap_servers="kafka:9092")
    thread = threading.Thread(
        target=display_broadcast,
        args=(websocket, consumer),
        daemon=True,
    )
    thread.start()
    
    try:
        room.connect(room_id, p_id)
    except Exception as e:
        logger.error(e)
        await websocket.send_json({"type": "error", "message": str(e)})
        await websocket.close()
        return

    try:
        while True:
            action = await websocket.receive_json()
            try:
                room.handle_action(p_id, room_id, action)
            except room.Board as e:
                await websocket.send_json({"type": "board", "board": json.loads(str(e))})
            except Exception as e:
                logger.error(e)
                await websocket.send_json({"type": "error", "message": str(e)})

    except WebSocketDisconnect:
        room.disconnect(room_id, p_id)
        logger.info(f"Websocket connection closed: p_id({p_id}) room_id({room_id})")

if __name__ == "__main__":
    room.configure_logger()

    uvicorn.run(app, host='0.0.0.0', port=8000)
