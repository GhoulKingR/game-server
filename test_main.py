from main import app
from fastapi.testclient import TestClient



def test_new_room():
    client = TestClient(app)
    response = client.post("/api/room")
    json = response.json()

    assert type(json["id"]) == str
    assert type(json["p1"]) == str


def test_join_room():
    client = TestClient(app)

    # create a room
    response = client.post("/api/room")
    data = response.json()

    # test the room
    join_res = client.get(f"/api/room/{data['id']}")
    join_data = join_res.json()

    assert join_data["status"] == "success"
    assert type(join_data["p2"]) == str


def prepare():
    client = TestClient(app)
    cr = client.post("/api/room")
    cr = cr.json()

    jr = client.get(f"/api/room/{cr['id']}")
    jr = jr.json()

    return cr["id"], cr["p1"], jr["p2"], client


def test_get_board():
    room, p1, p2, client = prepare()
    with client.websocket_connect(f"/ws/room/{room}/{p1}") as ws:
        ws.send_json({"type": "board"})
        res = ws.receive_json()
        assert res["type"] == "board"
        assert len(res["board"]) == 9


def test_alert_other_player():
    room, p1, p2, client = prepare()
    with client.websocket_connect(f"/ws/room/{room}/{p1}") as ws1:
        with client.websocket_connect(f"/ws/room/{room}/{p2}") as ws2:
            res = ws1.receive_json()
            assert res["type"] == "chat"
            assert res["from"] == "system"
            assert res["message"] == "p2 is connected"


def test_can_make_move():
    room, p1, p2, client = prepare()
    with client.websocket_connect(f"/ws/room/{room}/{p1}") as ws1:
        with client.websocket_connect(f"/ws/room/{room}/{p2}") as ws2:
            ws1.receive_json()
            ws1.send_json(
                {
                    "type": "move",
                    "pos": 0,
                }
            )

            res1 = ws1.receive_json()
            res2 = ws2.receive_json()

            assert res1["type"] == "board"
            assert res1["board"][0] == "p1"
            assert len(res1["board"]) == 9
            assert res2["type"] == "board"
            assert res2["board"][0] == "p1"
            assert len(res2["board"]) == 9

def test_have_to_take_turns():
    room, p1, p2, client = prepare()
    with client.websocket_connect(f"/ws/room/{room}/{p1}") as ws1:
        with client.websocket_connect(f"/ws/room/{room}/{p2}") as ws2:
            ws1.receive_json()
            ws1.send_json(
                {
                    "type": "move",
                    "pos": 1,
                }
            )


            res1 = ws1.receive_json()
            print(res1)

            ws1.send_json(
                {
                    "type": "move",
                    "pos": 2,
                }
            )
            res = ws1.receive_json()
            print(res)

            assert res["type"] == "error"
            assert res["message"] == "It's not your turn"

# test can't join a non-existent room
# test can't play an invalid move
# test can win

# test have to take turns -- not implemented yet
