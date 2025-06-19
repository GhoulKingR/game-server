from uuid import uuid4
from fastapi import WebSocket


class Room:
    board = [""] * 9

    def __init__(self):
        id = str(uuid4()).split("-")
        self.id = id[0]
        self.p1 = id[1]
        self.p1_ws = None
        self.p2 = None
        self.p2_ws = None
        self.turn = "p1"

    def join(self):
        if self.p2 == None:
            self.p2 = str(uuid4()).split("-")[1]
            return True

        return False

    async def handle_action(self, p_id: str, action, ws: WebSocket):
        match action["type"]:
            case "chat":
                if p_id == self.p1:
                    if self.p2_ws:
                        await self.p2_ws.send_json(
                            {"type": "chat", "from": "p1", "message": action["message"]}
                        )
                elif self.p2 and p_id == self.p2:
                    if self.p1_ws:
                        await self.p1_ws.send_json(
                            {"type": "chat", "from": "p2", "message": action["message"]}
                        )

            case "board":
                await ws.send_json({"type": "board", "board": self.board})

            case "move":
                if self.board[action["pos"]] != "":
                    await ws.send_json({"type": "error", "message": "invalid move"})
                    return False

                if p_id == self.p1:
                    player = "p1"
                elif self.p2 and p_id == self.p2:
                    player = "p2"
                else:
                    await ws.send_json(
                        {
                            "type": "error",
                            "message": "you're not a valid player in this room",
                        }
                    )
                    return False

                if self.turn != player:
                    await ws.send_json(
                        {
                            "type": "error",
                            "message": "It's not your turn",
                        }
                    )
                    return False


                self.board[action["pos"]] = player
                self.turn = "p1" if self.turn == "p2" else "p2"

                if self.p1_ws:
                    await self.p1_ws.send_json({"type": "board", "board": self.board})
                if self.p2_ws:
                    await self.p2_ws.send_json({"type": "board", "board": self.board})

                await self.check_win()
                return True

            case _:
                await ws.send_json(
                    {
                        "type": "error",
                        "message": f"{action['type']} is not a valid action",
                    }
                )
                return False

        return True

    async def connect(self, p_id: str, websocket: WebSocket):
        if p_id == self.p1:
            if self.p1_ws:
                await websocket.send_json(
                    {
                        "type": "error",
                        "message": "You may still connected somewhere else",
                    }
                )
                await websocket.close()
                return False
            else:
                self.p1_ws = websocket

            if self.p2_ws:
                await self.p2_ws.send_json(
                    {"type": "chat", "from": "system", "message": "p1 is connected"}
                )
            return True

        if p_id == self.p2:
            if self.p2_ws:
                await websocket.send_json(
                    {
                        "type": "error",
                        "message": "You may still connected somewhere else",
                    }
                )
                await websocket.close()
                return False
            else:
                self.p2_ws = websocket

            if self.p1_ws:
                await self.p1_ws.send_json(
                    {"type": "chat", "from": "system", "message": "p2 is connected"}
                )
            return True

        await websocket.send_json(
            {
                "type": "error",
                "message": "You're not a valid player in this room",
            }
        )
        await websocket.close()
        return False

    async def disconnect(self, p_id: str, websocket: WebSocket):
        if p_id == self.p1:
            self.p1_ws = None
            if self.p2_ws:
                await self.p2_ws.send_json(
                    {"type": "chat", "from": "system", "message": "p1 is disconnected"}
                )
            return True
        elif self.p2 and p_id == self.p2:
            self.p2_ws = None
            if self.p1_ws:
                await self.p1_ws.send_json(
                    {"type": "chat", "from": "system", "message": "p2 is disconnected"}
                )
            return True

        await websocket.send_json(
            {
                "type": "error",
                "message": "You're not a valid player in this room",
            }
        )
        await websocket.close()
        return False

    async def broadcast_win(self, character: str, move_set: list[int]):
        if self.p1_ws:
            await self.p1_ws.send_json(
                {
                    "type": "win",
                    "wining": character,
                    "move_set": move_set,
                }
            )
        if self.p2_ws:
            await self.p2_ws.send_json(
                {
                    "type": "win",
                    "wining": character,
                    "move_set": move_set,
                }
            )

    async def check_win(self):
        b = self.board

        for i in [0, 3, 6]:
            if b[i] == b[i + 1] and b[i + 1] == b[i + 2] and b[i + 2] != "":
                await self.broadcast_win(b[i], [i, i + 1, i + 2])
                return True

        for i in [0, 1, 2]:
            if b[i] == b[i + 3] and b[i + 3] == b[i + 6] and b[i + 6] != "":
                await self.broadcast_win(b[i], [i, i + 3, i + 6])
                return True

        if b[0] == b[4] and b[4] == [8] and b[8] != "":
            await self.broadcast_win(b[0], [0, 4, 8])
            return True

        if b[2] == b[4] and b[4] == [6] and b[6] != "":
            await self.broadcast_win(b[0], [2, 4, 6])
            return True

        return False
