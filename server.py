import asyncio
import json
import logging
import os
import random
import time
from contextlib import asynccontextmanager
from typing import Dict, Any, Set
from uuid import uuid4

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

# --- Configuration ---
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 8000))
STATE_FILE = "game_state.json"
GRID_SIZE = 10000
MIN_COUNTRY_DISTANCE = 50
SAVE_INTERVAL_SECONDS = 300  # 5 minutes
TILE_COOLDOWN_SECONDS = 5  # Cooldown for placing a tile
MAX_COUNTRY_NAME_LENGTH = 25
MAX_CHAT_MESSAGE_LENGTH = 200

logging.basicConfig(level=logging.INFO)


# --- Game State Management ---
class GameState:
    def __init__(self):
        # country_id -> {name, ruler, color, population, territorySize, id}
        self.countries: Dict[str, Dict[str, Any]] = {}
        # "x,y" -> {"country": country_id}
        self.grid: Dict[str, Dict[str, str]] = {}
        # client_id -> {"country": country_id, "last_tile_place": timestamp}
        self.users: Dict[str, Dict[str, Any]] = {}
        self.lock = asyncio.Lock()

    def to_dict(self):
        return {"countries": self.countries, "grid": self.grid, "users": self.users}

    def load_from_dict(self, data: Dict):
        self.countries = data.get("countries", {})
        self.grid = data.get("grid", {})
        # Only load user-country association, not sensitive data like timestamps
        loaded_users = data.get("users", {})
        for user_id, user_data in loaded_users.items():
            self.users[user_id] = {"country": user_data.get("country")}


game_state = GameState()


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(game_state.to_dict(), f)
        logging.info(f"Game state saved to {STATE_FILE}")
    except IOError as e:
        logging.error(f"Error saving game state: {e}")


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                data = json.load(f)
                game_state.load_from_dict(data)
            logging.info(f"Game state loaded from {STATE_FILE}")
        except (IOError, json.JSONDecodeError) as e:
            logging.error(f"Error loading game state: {e}. Starting fresh.")
    else:
        logging.info("No saved state file found. Starting fresh.")


# --- WebSocket Connection Manager ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, client_id: str):
        await websocket.accept()
        self.active_connections[client_id] = websocket
        if client_id not in game_state.users:
            game_state.users[client_id] = {}
        logging.info(f"Client connected: {client_id}")

    def disconnect(self, client_id: str):
        if client_id in self.active_connections:
            del self.active_connections[client_id]
        # Note: We don't remove the user from game_state.users so they can reconnect
        logging.info(f"Client disconnected: {client_id}")

    async def broadcast(self, message: Dict):
        tasks = [ws.send_text(json.dumps(message)) for ws in self.active_connections.values()]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def broadcast_to_country(self, message: Dict, country_id: str):
        if not country_id:
            return
        tasks = []
        for client_id, ws in self.active_connections.items():
            user = game_state.users.get(client_id)
            if user and user.get("country") == country_id:
                tasks.append(ws.send_text(json.dumps(message)))
        await asyncio.gather(*tasks, return_exceptions=True)


manager = ConnectionManager()


# --- Background Tasks ---
async def periodic_save():
    while True:
        await asyncio.sleep(SAVE_INTERVAL_SECONDS)
        async with game_state.lock:
            save_state()


# --- FastAPI App Lifecycle ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    load_state()
    save_task = asyncio.create_task(periodic_save())
    yield
    save_state()
    save_task.cancel()
    logging.info("Server shutting down. Final state saved.")


app = FastAPI(lifespan=lifespan)


# --- Helper Functions ---
def is_ruler(client_id: str, country_id: str) -> bool:
    """Check if a client is the ruler of a country."""
    country = game_state.countries.get(country_id)
    return country and country.get("ruler") == client_id

def sanitize_input(text: str, max_length: int) -> str:
    """Basic input sanitizer."""
    return text.strip()[:max_length]


# --- Game Logic Handlers ---
async def handle_create_country(client_id: str, data: Dict):
    country_name = sanitize_input(data.get("name", ""), MAX_COUNTRY_NAME_LENGTH)
    if not country_name:
        return {"error": "Country name is required."}

    country_id = str(uuid4()) # Use UUID for stable ID
    
    async with game_state.lock:
        if any(c['name'].lower() == country_name.lower() for c in game_state.countries.values()):
            return {"error": "A country with this name already exists."}
        
        # Find a valid starting position
        attempts = 0
        while attempts < 100:
            x = random.randint(0, GRID_SIZE - 1)
            y = random.randint(0, GRID_SIZE - 1)
            
            too_close = False
            for tile_key in game_state.grid.keys():
                gx, gy = map(int, tile_key.split(','))
                distance = ((x - gx)**2 + (y - gy)**2)**0.5
                if distance < MIN_COUNTRY_DISTANCE:
                    too_close = True
                    break
            if not too_close:
                break
            attempts += 1
        else: # If loop finishes without break
             return {"error": "Could not find a suitable starting location. The world may be too crowded."}

        color = f"#{random.randint(0, 0xFFFFFF):06x}"
        game_state.countries[country_id] = {
            "id": country_id,
            "name": country_name,
            "ruler": client_id,
            "color": color,
            "population": 1,
            "territorySize": 1,
        }
        game_state.users[client_id] = {"country": country_id, "last_tile_place": 0}
        game_state.grid[f"{x},{y}"] = {"country": country_id}

    await manager.broadcast({
        "type": "state_update",
        "payload": {"countries": game_state.countries, "grid": {f"{x},{y}": game_state.grid[f"{x},{y}"]}}
    })
    return {"type": "user_update", "payload": {"country": country_id}}


async def handle_join_country(client_id: str, data: Dict):
    country_id = data.get("country_id")
    if not country_id:
        return {"error": "Country ID is required."}

    async with game_state.lock:
        if country_id not in game_state.countries:
            return {"error": "Country not found."}
        
        # Prevent user from re-joining the same country
        if game_state.users.get(client_id, {}).get("country") == country_id:
            return

        game_state.countries[country_id]["population"] += 1
        game_state.users[client_id] = {"country": country_id, "last_tile_place": 0}

    await manager.broadcast({"type": "state_update", "payload": {"countries": game_state.countries}})
    return {"type": "user_update", "payload": {"country": country_id}}


async def handle_claim_tile(client_id: str, data: Dict):
    user = game_state.users.get(client_id)
    if not user or not user.get("country"):
        return {"error": "You must be in a country to claim tiles."}
    
    country_id = user["country"]
    last_place = user.get("last_tile_place", 0)
    
    if time.time() - last_place < TILE_COOLDOWN_SECONDS:
        return {"error": f"Please wait {int(TILE_COOLDOWN_SECONDS - (time.time() - last_place))}s before claiming another tile."}

    x, y = data.get("x"), data.get("y")
    if not (0 <= x < GRID_SIZE and 0 <= y < GRID_SIZE):
        return {"error": "Invalid tile coordinates."}

    key = f"{x},{y}"
    
    async with game_state.lock:
        if game_state.grid.get(key, {}).get("country") == country_id:
            return # Already owns the tile

        # Check for adjacency
        is_adjacent = False
        for dx in [-1, 0, 1]:
            for dy in [-1, 0, 1]:
                if dx == 0 and dy == 0: continue
                adj_key = f"{x+dx},{y+dy}"
                if game_state.grid.get(adj_key, {}).get("country") == country_id:
                    is_adjacent = True
                    break
            if is_adjacent: break
        
        if not is_adjacent:
            return {"error": "You can only claim tiles adjacent to your territory."}
        
        # Update territory size if taking over another country's tile
        if key in game_state.grid:
            old_country_id = game_state.grid[key]["country"]
            if old_country_id in game_state.countries:
                 game_state.countries[old_country_id]["territorySize"] -= 1

        game_state.grid[key] = {"country": country_id}
        game_state.countries[country_id]["territorySize"] += 1
        user["last_tile_place"] = time.time()

    await manager.broadcast({
        "type": "state_update",
        "payload": {"countries": game_state.countries, "grid": {key: game_state.grid[key]}}
    })

# --- Chat Handlers ---
async def handle_chat(client_id: str, data: Dict, chat_type: str):
    user = game_state.users.get(client_id)
    country_id = user.get("country") if user else None
    
    if chat_type == 'country' and not country_id:
        return {"error": "You must be in a country for country chat."}

    message_text = sanitize_input(data.get("message", ""), MAX_CHAT_MESSAGE_LENGTH)
    if not message_text:
        return

    country = game_state.countries.get(country_id) if country_id else None
    sender_name = country['name'] if country else f"User...{client_id[-4:]}"
    sender_color = country['color'] if country else '#888888'

    message = {
        "type": f"{chat_type}_chat_message",
        "payload": {
            "sender_name": sender_name,
            "sender_color": sender_color,
            "message": message_text,
        }
    }

    if chat_type == 'global':
        await manager.broadcast(message)
    elif chat_type == 'country':
        await manager.broadcast_to_country(message, country_id)

# --- Ruler Action Handlers ---
async def handle_ruler_action(client_id: str, data: Dict, action: str):
    user = game_state.users.get(client_id)
    if not user or "country" not in user:
        return {"error": "You are not in a country."}
    
    country_id = user["country"]
    if not is_ruler(client_id, country_id):
        return {"error": "You are not the ruler of this country."}

    async with game_state.lock:
        country = game_state.countries[country_id]
        if action == 'rename_country':
            new_name = sanitize_input(data.get("new_name", ""), MAX_COUNTRY_NAME_LENGTH)
            if not new_name: return {"error": "New name cannot be empty."}
            country["name"] = new_name
        elif action == 'change_color':
            new_color = data.get("color")
            # Basic hex color validation
            if not (new_color and new_color.startswith("#") and len(new_color) == 7):
                return {"error": "Invalid color format."}
            country["color"] = new_color
        elif action == 'abdicate':
            country["ruler"] = None
    
    # After lock is released, broadcast the change
    await manager.broadcast({"type": "state_update", "payload": {"countries": game_state.countries}})


ACTION_DISPATCHER = {
    "create_country": handle_create_country,
    "join_country": handle_join_country,
    "claim_tile": handle_claim_tile,
    "global_chat": lambda cid, d: handle_chat(cid, d, 'global'),
    "country_chat": lambda cid, d: handle_chat(cid, d, 'country'),
    "rename_country": lambda cid, d: handle_ruler_action(cid, d, 'rename_country'),
    "change_color": lambda cid, d: handle_ruler_action(cid, d, 'change_color'),
    "abdicate": lambda cid, d: handle_ruler_action(cid, d, 'abdicate'),
}


# --- API Endpoints ---
@app.get("/")
async def get_root():
    return FileResponse('index.html')


@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await manager.connect(websocket, client_id)
    initial_state_message = {"type": "initial_state", "payload": game_state.to_dict()}
    await websocket.send_text(json.dumps(initial_state_message))

    try:
        while True:
            data_str = await websocket.receive_text()
            data = json.loads(data_str)
            action_type = data.get("type")

            if action_type in ACTION_DISPATCHER:
                handler = ACTION_DISPATCHER[action_type]
                response = await handler(client_id, data)
                if response:
                    await websocket.send_text(json.dumps(response))
            else:
                await websocket.send_text(json.dumps({"error": f"Unknown action: {action_type}"}))

    except WebSocketDisconnect:
        manager.disconnect(client_id)
    except Exception as e:
        logging.error(f"WebSocket error for client {client_id}: {e}", exc_info=True)
        manager.disconnect(client_id)

# To run: uvicorn server:app --host 0.0.0.0 --port 8000 --reload