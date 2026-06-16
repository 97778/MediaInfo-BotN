import os
import logging


def _get_int(name: str, default: int = 0) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _get_chat_ids(name: str) -> list[int]:
    """Parse a comma/space separated list of chat IDs from an env var."""
    raw = os.environ.get(name, "")
    ids: list[int] = []
    for token in raw.replace(",", " ").split():
        try:
            ids.append(int(token))
        except ValueError:
            continue
    return ids


# --- Telegram API credentials (required) ---
API_ID = _get_int("API_ID")
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

# --- Access control ---
ADMIN_ID = _get_int("ADMIN_ID")
ALLOWED_CHATS = _get_chat_ids("ALLOWED_CHATS")

# --- Database ---
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")

# --- Logging ---
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
if not isinstance(logging.getLevelName(LOG_LEVEL), int):
    LOG_LEVEL = "INFO"
LOG_FORMAT = os.environ.get(
    "LOG_FORMAT",
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# --- Garbage collection thresholds (gen0, gen1, gen2) ---
GC_THRESHOLD = (
    _get_int("GC_GEN0", 700),
    _get_int("GC_GEN1", 10),
    _get_int("GC_GEN2", 10),
)

# --- Caption template ---
# Available placeholders: {title} {video_line} {duration} {audio} {subtitle}
CAPTION_TEMPLATE = os.environ.get(
    "CAPTION_TEMPLATE",
    "<b>\U0001F3AC {title}</b>\n\n"
    "\U0001F39E <b>:</b> {video_line} | "
    "\u23f3 <b>:</b> {duration}\n"
    "\U0001F50A <b>:</b> {audio}\n"
#    "\U0001F4AC <b>Subtitles:</b> {subtitle}",
)
