from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from room import Room

app = FastAPI()

rooms: dict[str, Room] = {}


@app.post("/api/room")
async def new_room():
    new = Room()
    rooms[new.id] = new
    return {
        "id": new.id,
        "p1": new.p1,
    }


@app.get("/api/room/{room_id}")
async def join_room(room_id):
    if room_id in rooms and rooms[room_id].join():
        return {"status": "success", "p2": rooms[room_id].p2}

    return {"status": "failed"}


@app.websocket("/ws/room/{room_id}/{p_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str, p_id: str):
    await websocket.accept()

    if room_id in rooms:
        await rooms[room_id].connect(p_id, websocket)
    else:
        await websocket.send_json(
            {
                "type": "error",
                "message": "Room not found",
            }
        )
        await websocket.close()
        return False

    try:
        while True:
            action = await websocket.receive_json()
            await rooms[room_id].handle_action(p_id, action, websocket)
    except WebSocketDisconnect:
        await rooms[room_id].disconnect(p_id, websocket)
