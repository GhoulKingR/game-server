import typing
import redis
import json
import logging
import multiprocessing
import time
import argparse
import os
from dotenv import load_dotenv
from uuid import uuid4
from kafka.admin import NewTopic
from kafka import KafkaProducer, KafkaAdminClient

logger = logging.getLogger(__name__)

if os.getenv('WEB_NO_ENV', None) == None:
    logger.info("Loading dot_env file")
    load_dotenv('.env')

r = redis.Redis(host=os.environ['WEB_REDIS_HOST'], port=int(os.environ['WEB_REDIS_PORT']), decode_responses=True)
producer = KafkaProducer(bootstrap_servers=os.environ['WEB_KAFKA_BROKER'])
admin_client = KafkaAdminClient(bootstrap_servers=os.environ['WEB_KAFKA_BROKER'])

def configure_logger():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--info", help="Enable info logger", action="store_true")
    parser.add_argument("-e", "--error", help="Enable error logger", action="store_true")
    parser.add_argument("-d", "--debug", help="Enable debug logger", action="store_true")
    args = parser.parse_args()

    if args.info:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    if args.error:
        logging.basicConfig(level=logging.ERROR, format='%(levelname)s: %(message)s')

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

def _delete_room(room_id: str):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=os.environ['WEB_KAFKA_BROKER'])
        r = redis.Redis(host='redis', port=6379, decode_responses=True)

        admin_client.delete_topics(topics=[room_id])
        r.delete(room_id)

        logger.info("Room Deleted Successfully")

        admin_client.close()
        r.close()
    except:
        logger.error("An error occured while deleting room")


def _delete_after_an_hour(room_id: str):
    configure_logger()
    logger = logging.getLogger(__name__)
    logger.info(f"Room ({room_id}) deletion timer started")

    try:
        time.sleep(3600)
        logger.info("Timer is complete")
        raise Exception()

    except:
        _delete_room(room_id)


def create_room():
    id = str(uuid4()).split("-")
    room_id = id[0]

    r.hset(room_id, mapping={
        "p1": id[1],
        "p1_connected": 'false',
        "board": json.dumps([''] * 9),
        "turn": "p1"
    })

    new_topic = NewTopic(
        name=room_id,
        num_partitions=1,
        replication_factor=1,
    )
    admin_client.create_topics(new_topics=[new_topic], validate_only=False)

    multiprocessing.Process(
        target=_delete_after_an_hour,
        args=(room_id,)
    ).start()

    return id[0], id[1]

def cleanup():
    admin_client.close()
    r.close()
    producer.close()

def join_room(room_id: str):
    if not r.exists(room_id):
        raise Exception(f"Room {room_id} doesn't exists")

    obj: dict[typing.Any, typing.Any] = r.hgetall(room_id) # pyright: ignore
    if 'p2' not in obj or obj['p2_connected'] == 'false':
        p2 = str(uuid4()).split("-")[1]
        r.hmset(room_id, {
            "p2": p2,
            "p2_connected": 'false',
        })
        return p2

    raise Exception(f"Room {room_id} is full")


def broadcast(room_id: str, msg):
    producer.send(
        room_id,
        value=json.dumps(msg).encode('utf-8')
    )

def connect(room_id: str, p_id: str):
    if not r.exists(room_id):
        raise Exception(f"Room {room_id} doesn't exists")

    obj: dict[typing.Any, typing.Any] = r.hgetall(room_id) # pyright: ignore

    if p_id == obj['p1']:
        player = 'p1'
    elif p_id == obj['p2']:
        player = 'p2'
    else:
        raise Exception("You're not a valid player in this room")

    if obj[f'{player}_connected'] == 'true':
        raise Exception("You may still be connected elsewhere")

    r.hset(room_id, f'{player}_connected', 'true')
    broadcast(room_id, {"type": "chat", "from": "system", "message": f"{player} is connected"})
    return True


class Board(Exception):
    pass

def handle_action(p_id: str, room_id: str, action):
    if not r.exists(room_id):
        raise Exception(f"Room {room_id} doesn't exists")

    obj: dict[typing.Any, typing.Any] = r.hgetall(room_id) # pyright: ignore
    match action["type"]:
        case "chat":
            if p_id == obj['p1']:
                broadcast(
                    room_id, 
                    {"type": "chat", "from": "p1", "message": action["message"]}
                )
            elif p_id == obj['p2']:
                broadcast(
                    room_id, 
                    {"type": "chat", "from": "p2", "message": action["message"]}
                )
            else:
                raise Exception("You're not a valid payer in this game")

        case "board":
            raise Board(obj['board'])

        case "move":
            board = json.loads(obj['board'])
            turn = obj['turn']
            if board[ action["pos"] ] != "":
                raise Exception("invalid move")

            if p_id == obj['p1']:
                player = "p1"
            elif p_id == obj['p2']:
                player = "p2"
            else:
                raise Exception("you're not a valid player in this room")

            if turn != player:
                raise Exception("It's not your turn")

            board[action["pos"]] = player
            new_turn = "p1" if turn == "p2" else "p2"

            broadcast(room_id, {"type": "board", "board": board})

            r.hmset(room_id, {
                'board': json.dumps(board),
                'turn': new_turn,
            })

            _, character, status = check_win(board)
            if status != None:
                broadcast(room_id, {
                    "type": "win",
                    "wining": character,
                    "move_set": status,
                })

        case _:
            raise Exception(f"{action['type']} is not a valid action")


def check_win(b):
    for i in [0, 3, 6]:
        if b[i] == b[i + 1] and b[i + 1] == b[i + 2] and b[i + 2] != "":
            return True, b[i], [i, i + 1, i + 2]

    for i in [0, 1, 2]:
        if b[i] == b[i + 3] and b[i + 3] == b[i + 6] and b[i + 6] != "":
            return True, b[i], [i, i + 3, i + 6]

    if b[0] == b[4] and b[4] == [8] and b[8] != "":
        return True, b[0], [0, 4, 8]

    if b[2] == b[4] and b[4] == [6] and b[6] != "":
        return True, b[0], [2, 4, 6]

    return False, None, None

def disconnect(room_id: str, p_id: str):
    if not r.exists(room_id):
        raise Exception(f"Room {room_id} doesn't exists")

    obj: dict[typing.Any, typing.Any] = r.hgetall(room_id) # pyright: ignore
    if p_id == obj['p1']:
        text = 'p1'
    elif p_id == obj['p2']:
        text = 'p2'
    else:
        raise Exception("You're not a valid player in this room")

    r.hset(room_id, f"{text}_connected", 'false')
    broadcast(room_id, {"type": "chat", "from": "system", "message": f"{text} is disconnected"})

