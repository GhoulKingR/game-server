# Tic-Tac-Toe game server

A tic-tac-toe game server written in Python.

## Technology stack

* FastAPI (Python): REST and WebSocket endpoints
* Redis: Caching and state management
* Apache Kafka: Event streaming in WebSocket endpoints

## Running the server

1. Create a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
```
2. Install the project's dependencies
```bash
pip install -r requirements.txt
```
3. Run the server:
```bash
python3 main.py
```

> [!INFO]
> You can see the various options available for running the server by running the command with the `-h` or `--help` flag.
