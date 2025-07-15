# Tic-Tac-Toe game server

A tic-tac-toe game server written in Python.

## Technology stack

* FastAPI (Python): REST and WebSocket endpoints
* Redis: Caching and state management
* Apache Kafka: Event streaming in WebSocket endpoints

## Running the server

### With docker compose
1. Clone the repo to your local machine:
```bash
git clone https://github.com/GhoulKingR/game-server
cd game-server
```
2. Run the docker compose:
```bash
docker compose up
```

### Without docker compose
You may want to run this server without docker ignore.

After cloning the project to your computer, you need to have a redis instance and kafka instance running, and their URLs. Once you have them, create an `.env` file in project directory and paste the following into it:
```env
WEB_KAFKA_BROKER=broker_host:broker_port>
WEB_REDIS_HOST=redis_host
WEB_REDIS_PORT=redis_port
```

Replace, the values with the ones of your kafka and redis instance. Here's an example you can go off of:
```env
WEB_KAFKA_BROKER=localhost:9092
WEB_REDIS_HOST=localhost
WEB_REDIS_PORT=6379
```

After creating the `.env` file:
- Create a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
```
- Install the project's dependencies
```bash
pip install -r requirements.txt
```
- Run the server:
```bash
python3 main.py
```

> [!TIP]
> You can see the various options available for running the server by running the command with the `-h` or `--help` flag.
