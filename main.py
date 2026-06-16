import asyncio
import json
import subprocess
import os
import logging
import sys
import psutil
import gc
import re
import uuid
import collections
import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import web
from aiofiles import open as aiopen
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from functools import lru_cache
from typing import Optional
from aiofiles.os import remove as aioremove
from pyrogram.errors import MessageNotModified, FloodWait, MessageIdInvalid
from collections import defaultdict
from config import (
    API_ID, API_HASH, BOT_TOKEN,
    ADMIN_ID, ALLOWED_CHATS,
    LOG_FORMAT, LOG_LEVEL,
    GC_THRESHOLD,
    CAPTION_TEMPLATE,
    MONGO_URI,
)

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT, force=True)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

# --- IN-MEMORY LOG BUFFER (for /logs command) ---
_log_buffer: collections.deque = collections.deque(maxlen=200)


class _BufferHandler(logging.Handler):
    def emit(self, record):
        _log_buffer.append(self.format(record))


_buf_handler = _BufferHandler()
_buf_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(_buf_handler)

# --- DATABASE SETUP ---
db_client = AsyncIOMotorClient(MONGO_URI)
db = db_client["MediaInfo-Bot"]
last_id_collection = db["last_processed_id"]
settings_collection = db["settings"]
stats_collection = db["stats"]
retry_collection = db["retry_queue"]


# --- RATE LIMITER CONFIG ---
# Rate limiter disabled as requested. We now rely strictly on EDIT_DELAY.
async def check_rate_limit():
    pass


# --- DYNAMIC CHAT MANAGEMENT ---
authorized_chats = set(ALLOWED_CHATS)


async def sync_chats():
    """Syncs the authorized_chats set with MongoDB on startup."""
    global authorized_chats
    data = await settings_collection.find_one({"_id": "allowed_chats"})
    if data:
        authorized_chats = set(data["chat_ids"])
    else:
        await settings_collection.update_one(
            {"_id": "allowed_chats"},
            {"$set": {"chat_ids": list(authorized_chats)}},
            upsert=True,
        )


async def get_last_id(chat_id: int) -> int:
    """Retrieve the last processed ID for a specific chat."""
    data = await last_id_collection.find_one({"chat_id": chat_id})
    return data["last_id"] if data else 1


async def save_last_id(chat_id: int, last_id: int):
    """Save the current message ID as the last processed."""
    await last_id_collection.update_one(
        {"chat_id": chat_id},
        {"$set": {"last_id": last_id}},
        upsert=True,
    )


# --- STATS HELPERS ---
async def increment_stat(chat_id: int):
    """Increment processed-file counter for a chat."""
    await stats_collection.update_one(
        {"chat_id": chat_id},
        {"$inc": {"count": 1}},
        upsert=True,
    )


async def get_all_stats() -> list[dict]:
    """Return all per-chat stats sorted by count descending."""
    return await stats_collection.find({}).sort("count", -1).to_list(length=None)


# --- ERROR COUNTER (for admin DM alerts) ---
_error_counts: dict[int, int] = defaultdict(int)
ERROR_THRESHOLD = 3


async def _report_error(chat_id: int, reason: str):
    """Track consecutive failures per chat; DM admin when threshold hit."""
    _error_counts[chat_id] += 1
    if _error_counts[chat_id] >= ERROR_THRESHOLD:
        _error_counts[chat_id] = 0
        try:
            await app.send_message(
                ADMIN_ID,
                f"\u26a0\ufe0f <b>Error Alert</b>\n"
                f"Chat: <code>{chat_id}</code>\n"
                f"Failed <b>{ERROR_THRESHOLD}x</b> in a row.\n"
                f"Reason: <code>{reason}</code>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass


def _reset_error_count(chat_id: int):
    _error_counts[chat_id] = 0


# --- RETRY QUEUE HELPERS ---
async def enqueue_retry(chat_id: int, message_id: int, caption: str):
    """Persist a failed caption edit for later retry."""
    await retry_collection.update_one(
        {"chat_id": chat_id, "message_id": message_id},
        {"$set": {
            "caption": caption,
            "attempts": 0,
            "next_retry": datetime.datetime.utcnow() + datetime.timedelta(seconds=60),
        }},
        upsert=True,
    )


async def process_retry_queue():
    """Scheduled job: retry failed caption edits."""
    now = datetime.datetime.utcnow()
    cursor = retry_collection.find({"next_retry": {"$lte": now}, "attempts": {"$lt": 5}})
    docs = await cursor.to_list(length=50)
    for doc in docs:
        chat_id = doc["chat_id"]
        message_id = doc["message_id"]
        caption = doc["caption"]
        attempts = doc.get("attempts", 0)
        try:
            await app.edit_message_caption(chat_id, message_id, caption, parse_mode=ParseMode.HTML)
            await retry_collection.delete_one({"_id": doc["_id"]})
            logger.info(f"Retry success: chat={chat_id} msg={message_id}")
        except FloodWait as e:
            await retry_collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {"next_retry": datetime.datetime.utcnow() + datetime.timedelta(seconds=e.value + 5)},
                 "$inc": {"attempts": 1}},
            )
        except MessageIdInvalid:
            await retry_collection.delete_one({"_id": doc["_id"]})
            logger.info(f"Retry dropped (message gone): chat={chat_id} msg={message_id}")
        except Exception as e:
            if attempts + 1 >= 5:
                await retry_collection.delete_one({"_id": doc["_id"]})
                logger.warning(f"Retry abandoned after 5 attempts: {e}")
            else:
                backoff = 60 * (2 ** attempts)
                await retry_collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"next_retry": datetime.datetime.utcnow() + datetime.timedelta(seconds=backoff)},
                     "$inc": {"attempts": 1}},
                )


app = Client(
    "MediaInfo-Bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=6,
    sleep_threshold=60,
)

stream_semaphore = asyncio.Semaphore(1)
channel_semaphore = asyncio.Semaphore(1)
active_users: set = set()

_last_edit: dict[int, float] = {}
channel_queues: dict[int, list] = defaultdict(list)
channel_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
# Tracks whether a worker task is already draining a channel's queue.
channel_workers: dict[int, bool] = defaultdict(bool)
last_edit_time: dict[int, float] = {}
EDIT_DELAY = 3.5

scheduler = AsyncIOScheduler()


_LANGUAGE_MAP: dict[str, str] = {
    'en': 'English', 'eng': 'English',
    'hi': 'Hindi', 'hin': 'Hindi',
    'ta': 'Tamil', 'tam': 'Tamil',
    'te': 'Telugu', 'tel': 'Telugu',
    'ml': 'Malayalam', 'mal': 'Malayalam',
    'kn': 'Kannada', 'kan': 'Kannada',
    'bn': 'Bengali', 'ben': 'Bengali',
    'mr': 'Marathi', 'mar': 'Marathi',
    'gu': 'Gujarati', 'guj': 'Gujarati',
    'pa': 'Punjabi', 'pun': 'Punjabi',
    'bho': 'Bhojpuri',
    'zh': 'Chinese', 'chi': 'Chinese', 'cmn': 'Chinese',
    'ko': 'Korean', 'kor': 'Korean',
    'pt': 'Portuguese', 'por': 'Portuguese',
    'th': 'Thai', 'tha': 'Thai',
    'tl': 'Tagalog', 'tgl': 'Tagalog', 'fil': 'Tagalog',
    'ja': 'Japanese', 'jpn': 'Japanese',
    'es': 'Spanish', 'spa': 'Spanish',
    'sv': 'Swedish', 'swe': 'Swedish',
    'fr': 'French', 'fra': 'French', 'fre': 'French',
    'de': 'German', 'deu': 'German', 'ger': 'German',
    'it': 'Italian', 'ita': 'Italian',
    'ru': 'Russian', 'rus': 'Russian',
    'ar': 'Arabic', 'ara': 'Arabic',
    'tr': 'Turkish', 'tur': 'Turkish',
    'nl': 'Dutch', 'nld': 'Dutch', 'dut': 'Dutch',
    'pl': 'Polish', 'pol': 'Polish',
    'vi': 'Vietnamese', 'vie': 'Vietnamese',
    'id': 'Indonesian', 'ind': 'Indonesian',
    'ms': 'Malay', 'msa': 'Malay', 'may': 'Malay',
    'fa': 'Persian', 'fas': 'Persian', 'per': 'Persian',
    'ur': 'Urdu', 'urd': 'Urdu',
    'he': 'Hebrew', 'heb': 'Hebrew',
    'el': 'Greek', 'ell': 'Greek', 'gre': 'Greek',
    'hu': 'Hungarian', 'hun': 'Hungarian',
    'cs': 'Czech', 'ces': 'Czech', 'cze': 'Czech',
    'ro': 'Romanian', 'ron': 'Romanian', 'rum': 'Romanian',
    'da': 'Danish', 'dan': 'Danish',
    'fi': 'Finnish', 'fin': 'Finnish',
    'no': 'Norwegian', 'nor': 'Norwegian',
    'uk': 'Ukrainian', 'ukr': 'Ukrainian',
    'ca': 'Catalan', 'cat': 'Catalan',
    'hr': 'Croatian', 'hrv': 'Croatian',
    'sk': 'Slovak', 'slk': 'Slovak', 'slo': 'Slovak',
    'sr': 'Serbian', 'srp': 'Serbian',
    'bg': 'Bulgarian', 'bul': 'Bulgarian',
    'unknown': 'Original Audio',
}


@lru_cache(maxsize=256)
def get_full_language_name(code: str) -> str:
    if not code:
        return 'Unknown'
    cleaned = code.split('(')[0].strip()
    return _LANGUAGE_MAP.get(cleaned.lower(), 'Original Audio')


@lru_cache(maxsize=64)
def get_standard_resolution(height: int) -> Optional[str]:
    if not height:
        return None
    if height <= 240:  return "240p"
    if height <= 360:  return "360p"
    if height <= 480:  return "480p"
    if height <= 720:  return "720p"
    if height <= 1080: return "1080p"
    if height <= 1440: return "1440p"
    if height <= 2160: return "2160p"
    return "2160p+"


@lru_cache(maxsize=128)
def get_video_format(codec: str, transfer: str = '', hdr: str = '', bit_depth: str = '') -> Optional[str]:
    if not codec:
        return None
    codec = codec.lower()
    parts: list[str] = []

    if any(x in codec for x in ('hevc', 'h.265', 'h265')):  parts.append('HEVC')
    elif 'av1' in codec:                                     parts.append('AV1')
    elif any(x in codec for x in ('avc', 'avc1', 'h.264', 'h264')): parts.append('x264')
    elif 'vp9' in codec:                                     parts.append('VP9')
    elif any(x in codec for x in ('mpeg4', 'xvid')):         parts.append('MPEG4')
    else:
        return None

    try:
        if bit_depth and int(bit_depth) > 8:
            parts.append(f"{bit_depth}bit")
    except (ValueError, TypeError):
        pass

    t = transfer.lower()
    h = hdr.lower()
    if any(x in t for x in ('pq', 'hlg', 'smpte', '2084', 'st 2084')) or 'hdr' in h or 'dolby' in h:
        parts.append('HDR')

    return ' '.join(parts)


def _is_video_track(track: dict) -> bool:
    t = (track.get('@type', '') or '').lower()
    fmt = (track.get('Format', '') or '').lower()
    cid = (track.get('CodecID', '') or '').lower()
    fp = (track.get('Format_Profile', '') or '').lower()
    title = (track.get('Title', '') or '').lower()
    menu = str(track.get('MenuID', '') or '').lower()

    return any([
        t == 'video',
        any(x in fmt for x in ('avc', 'hevc', 'h.264', 'h264', 'h.265', 'h265', 'av1', 'vp9', 'mpeg-4', 'mpeg4', 'xvid')),
        any(x in cid for x in ('avc', 'h264', 'hevc', 'h265', 'av1', 'vp9', 'mpeg4', 'xvid', '27')),
        'video' in menu,
        'video' in title,
        any(x in fp for x in ('main', 'high', 'baseline')),
    ])


def _has_subtitles(tracks: list) -> bool:
    for track in tracks:
        if not isinstance(track, dict):
            continue
        t = (track.get('@type', '') or '').lower()
        fmt = (track.get('Format', '') or '').lower()
        cid = (track.get('CodecID', '') or '').lower()
        enc = (track.get('Encoding', '') or '').lower()
        fi = (track.get('Format_Info', '') or '').lower()
        ttl = (track.get('Title', '') or '').lower()
        if any([
            t == 'text',
            any(x in fmt for x in ('pgs', 'subrip', 'ass', 'ssa', 'srt', 'dvb_subtitle', 'dvd_subtitle')),
            any(x in cid for x in ('s_text', 'subp', 'pgs', 'subtitle', 'dvb', 'dvd')),
            any(x in enc for x in ('utf-8', 'utf8', 'unicode', 'text')),
            any(x in fi for x in ('subtitle', 'caption', 'text')),
            'subtitle' in ttl,
        ]):
            return True
    return False


def _parse_int(value) -> int:
    try:
        return int(re.findall(r"\d+", str(value))[0])
    except Exception:
        return 0


def _parse_duration(value) -> float:
    try:
        if not value:
            return 0
        v = str(value).strip()
        if v.replace('.', '', 1).lstrip('-').isdigit():
            f = float(v)
            if f > 86_400_000:
                return f / 1_000_000
            if f > 86_400:
                return f / 1_000
            return f
        if ':' in v:
            parts = [float(p) for p in v.split(':')]
            if len(parts) == 3:
                return parts[0] * 3600 + parts[1] * 60 + parts[2]
            if len(parts) == 2:
                return parts[0] * 60 + parts[1]
    except Exception:
        pass
    return 0


def _fmt_duration(s: float) -> str:
    s = int(s)
    return f"{s//3600:02}:{(s%3600)//60:02}:{s%60:02}"


async def _run_mediainfo(path: str) -> dict:
    try:
        proc = await asyncio.create_subprocess_exec(
            'mediainfo', '--ParseSpeed=0', '--Language=raw', '--Output=JSON', path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=8)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return {}
        return json.loads(stdout.decode() or '{}')
    except Exception as e:
        logger.warning(f"mediainfo error: {e}")
        return {}


async def _run_ffprobe_full(path: str) -> dict:
    try:
        proc = await asyncio.create_subprocess_exec(
            'ffprobe', '-v', 'error', '-show_streams', '-show_format', '-of', 'json', path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=8)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return {}
        return json.loads(out.decode() or '{}')
    except Exception as e:
        logger.warning(f"ffprobe error: {e}")
        return {}


def _parse_ffprobe(data: dict) -> tuple:
    streams = data.get('streams', [])
    fmt = data.get('format', {})

    duration = 0.0
    width = height = None
    codec = bit_depth = hdr = transfer = ''
    audio_langs: set[str] = set()
    sub_langs: set[str] = set()
    has_sub = False

    dur_raw = fmt.get('duration') or ''
    if dur_raw:
        duration = _parse_duration(dur_raw)

    for s in streams:
        ctype = (s.get('codec_type') or '').lower()
        tags = s.get('tags') or {}

        if ctype == 'video':
            if not width:
                width = s.get('width') or s.get('coded_width')
            if not height:
                height = s.get('height') or s.get('coded_height')

            codec_raw = (s.get('codec_name') or '').lower()
            if 'hevc' in codec_raw or 'h265' in codec_raw: codec = 'hevc'
            elif 'h264' in codec_raw or 'avc' in codec_raw: codec = 'avc'
            elif 'av1' in codec_raw:                         codec = 'av1'
            elif 'vp9' in codec_raw:                         codec = 'vp9'
            elif 'mpeg4' in codec_raw or 'xvid' in codec_raw: codec = 'mpeg4'
            else: codec = codec_raw

            bps = str(s.get('bits_per_raw_sample') or s.get('bits_per_coded_sample') or '')
            if bps.isdigit() and bps != '0':
                bit_depth = bps

            ct = (s.get('color_transfer') or '').lower()
            cs = (s.get('color_space') or '').lower()
            if any(x in ct for x in ('smpte2084', 'arib-std-b67', 'smpte428')):
                hdr = 'HDR'
            elif 'bt2020' in cs and not hdr:
                hdr = 'HDR'

            if not duration:
                d = tags.get('DURATION') or tags.get('duration') or ''
                if d:
                    duration = _parse_duration(d)

        elif ctype == 'audio':
            lang = tags.get('language') or tags.get('LANGUAGE') or ''
            audio_langs.add(get_full_language_name(lang or 'unknown'))

        elif ctype == 'subtitle':
            has_sub = True
            lang = tags.get('language') or tags.get('LANGUAGE') or ''
            if lang:
                sub_langs.add(get_full_language_name(lang))

    audio_str = ', '.join(sorted(audio_langs)) if audio_langs else 'Original Audio'
    if sub_langs:
        sub_str = ', '.join(sorted(sub_langs))
    elif has_sub:
        sub_str = 'ESUB'
    else:
        sub_str = 'No Esubs'

    return duration, width, height, codec, bit_depth, hdr, transfer, audio_str, sub_str


def _parse_tracks(tracks: list) -> tuple:
    duration = 0.0
    width = height = None
    codec = bit_depth = hdr = transfer = ''
    audio_langs: set[str] = set()
    sub_langs: set[str] = set()

    for track in tracks:
        if not isinstance(track, dict):
            continue
        t = (track.get('@type', '') or '').lower()

        if t == 'general':
            if not duration:
                duration = _parse_duration(track.get('Duration'))

        elif _is_video_track(track):
            for field in ('Height', 'Encoded_Height', 'Sampled_Height'):
                raw = str(track.get(field, '') or '').replace('\u00a0', '').replace(',', '').split()
                raw = raw[0] if raw else ''
                if raw.isdigit() and int(raw) > 0:
                    height = int(raw)
                    break

            for field in ('Width', 'Encoded_Width', 'Sampled_Width'):
                raw = str(track.get(field, '') or '').replace('\u00a0', '').replace(',', '').split()
                raw = raw[0] if raw else ''
                if raw.isdigit() and int(raw) > 0:
                    width = int(raw)
                    break

            codec = (track.get('Format', '') or '').lower()
            bit_depth = track.get('BitDepth', '') or ''
            transfer = (track.get('transfer_characteristics', '') or
                        track.get('TransferCharacteristics', '') or '').lower()
            hdr = (track.get('HDR_Format', '') or
                   track.get('HDR_Format_Compatibility', '') or '')

            if not duration:
                duration = _parse_duration(track.get('Duration'))

            track_str = str(track).lower()
            if 'dolby vision' in track_str:
                hdr = 'Dolby Vision'
            elif 'hdr' in track_str and not hdr:
                hdr = 'HDR'

        elif t == 'audio':
            lang = None
            for field in ('Language', 'Language_String', 'Title'):
                v = track.get(field)
                if v:
                    lang = v
                    break
            audio_langs.add(get_full_language_name(lang or 'unknown'))

        elif t in ('text', 'menu', 'subtitle'):
            lang = track.get('Language') or track.get('Language_String') or 'unknown'
            sub_langs.add(get_full_language_name(lang))

    audio_str = ', '.join(sorted(audio_langs)) if audio_langs else 'Original Audio'
    sub_str = ', '.join(sorted(sub_langs)) if sub_langs else (
        'ESUB' if _has_subtitles(tracks) else 'No Esubs')

    return duration, width, height, codec, bit_depth, hdr, transfer, audio_str, sub_str


async def _probe(path: str) -> tuple:
    mi_data = await _run_mediainfo(path)
    tracks = mi_data.get('media', {}).get('track', [])
    mi = _parse_tracks(tracks)
    mi_dur, mi_w, mi_h = mi[0], mi[1], mi[2]

    fp_data = await _run_ffprobe_full(path)
    fp = _parse_ffprobe(fp_data) if fp_data else None

    if fp is None:
        return mi

    fp_dur, fp_w, fp_h = fp[0], fp[1], fp[2]

    duration = mi_dur or fp_dur
    width = mi_w or fp_w
    height = mi_h or fp_h
    codec = mi[3] or fp[3]
    bit_depth = mi[4] or fp[4]
    hdr = mi[5] or fp[5]
    transfer = mi[6] or fp[6]
    audio = mi[7] if mi[7] not in ('Unknown', 'Original Audio') else fp[7]
    subtitle = mi[8] if mi[8] not in ('No Sub', 'No Esubs') else fp[8]

    return duration, width, height, codec, bit_depth, hdr, transfer, audio, subtitle


def _safe_title(message, media) -> str:
    """Return a title safe for str.format (escapes stray braces)."""
    title = message.caption or getattr(media, 'file_name', None) or 'Video'
    return title.replace('{', '{{').replace('}', '}}')


def _build_caption(message, media, result: tuple) -> str:
    duration, width, height, codec, bit_depth, hdr, transfer, audio, sub = result

    # Always use height for resolution (standard convention: 1920x1080 = 1080p).
    # Fall back to width only if height is missing.
    res_val = height or width or 0
    quality = get_standard_resolution(res_val)
    fmt = get_video_format(codec, transfer, hdr, bit_depth)
    video_line = ' '.join(filter(None, [quality, fmt])) or 'Unknown'

    return CAPTION_TEMPLATE.format(
        title=_safe_title(message, media),
        video_line=video_line,
        duration=_fmt_duration(duration) if duration else 'Unknown',
        audio=audio,
        subtitle=sub,
    )


def _extract_res_from_text(text: str) -> Optional[str]:
    """Helper regex to parse resolution tokens out of file names or captions."""
    if not text:
        return None
    match = re.search(r'\b(240|360|480|720|1080|1440|2160)[pP]\b', text)
    if match:
        return f"{match.group(1)}p"
    if "4k" in text.lower() or "2160p" in text.lower():
        return "2160p"
    return None


def _build_caption_from_tg(message, media) -> str:
    """Fallback caption built purely from Telegram metadata with smart filename parsing."""
    raw_title = message.caption or getattr(media, 'file_name', None) or 'Video'

    quality = _extract_res_from_text(raw_title)
    if not quality:
        height = getattr(media, 'height', None)
        width = getattr(media, 'width', None)
        res_val = height or width or 0
        quality = get_standard_resolution(res_val)

    video_line = quality or 'Unknown'

    tg_dur = getattr(media, 'duration', None)
    duration_str = _fmt_duration(tg_dur) if tg_dur else 'Unknown'

    return CAPTION_TEMPLATE.format(
        title=raw_title.replace('{', '{{').replace('}', '}}'),
        video_line=video_line,
        duration=duration_str,
        audio='Original Audio',
        subtitle='No Esubs',
    )


def caption_has_media_info(caption: str) -> bool:
    if not caption:
        return False
    hits = (
        bool(re.search(r'\U0001F3AC', caption)),
        bool(re.search(r'\u23f3\s*\d{2}:\d{2}:\d{2}', caption)),
        bool(re.search(r'\U0001F50A', caption)),
        bool(re.search(r'\U0001F4AC', caption)),
    )
    return sum(hits) >= 2


_STREAM_STEPS = [
    ("16KB", 16 * 1024),
    ("1MB", 1 * 1024 * 1024),
    ("3MB", 3 * 1024 * 1024),
    ("8MB", 8 * 1024 * 1024),
]


async def _stream_chunk(media, size: int, path: str) -> bool:
    try:
        written = 0
        async with stream_semaphore:
            async with aiopen(path, 'wb') as f:
                async for chunk in app.stream_media(media):
                    if not chunk:
                        break
                    remaining = size - written
                    if remaining <= 0:
                        break
                    piece = chunk[:remaining]
                    await f.write(piece)
                    written += len(piece)
                    if written >= size:
                        break
        return os.path.exists(path) and os.path.getsize(path) > 0
    except FloodWait as e:
        logger.warning(f"stream_chunk FloodWait ({size}): waiting {e.value}s before continuing")
        await asyncio.sleep(e.value + 2)
        return False
    except Exception as e:
        logger.warning(f"stream_chunk failed ({size}): {e}")
        return False


# Global flag to track active flood wait so all queued chunks skip immediately
_dc_flood_until: float = 0.0


async def _stream_chunk_safe(media, size: int, path: str) -> bool:
    """Wrapper that skips the call entirely if a DC flood wait is still active."""
    global _dc_flood_until
    now = asyncio.get_event_loop().time()
    if now < _dc_flood_until:
        remaining = int(_dc_flood_until - now)
        logger.warning(f"stream_chunk skipped ({size}): DC flood wait active for {remaining}s more")
        return False
    try:
        written = 0
        async with stream_semaphore:
            async with aiopen(path, 'wb') as f:
                async for chunk in app.stream_media(media):
                    if not chunk:
                        break
                    left = size - written
                    if left <= 0:
                        break
                    piece = chunk[:left]
                    await f.write(piece)
                    written += len(piece)
                    if written >= size:
                        break
        return os.path.exists(path) and os.path.getsize(path) > 0
    except FloodWait as e:
        _dc_flood_until = asyncio.get_event_loop().time() + e.value + 5
        logger.warning(f"stream_chunk FloodWait ({size}): DC blocked for {e.value}s \u2014 falling back to TG metadata")
        return False
    except Exception as e:
        logger.warning(f"stream_chunk failed ({size}): {e}")
        return False


async def process_message(message, progress_msg=None) -> tuple[str, Optional[str]]:
    media = message.video or message.document

    async def _update(text: str):
        if progress_msg:
            await _safe_edit(progress_msg, text)
            await asyncio.sleep(0.3)

    await _update("\u26a1 Fast scan (16 KB)\u2026")

    for label, size in _STREAM_STEPS:
        if asyncio.get_event_loop().time() < _dc_flood_until:
            logger.warning(f"Skipping all probe steps \u2014 DC flood wait active for {int(_dc_flood_until - asyncio.get_event_loop().time())}s more")
            break

        tmp = f"probe_{label}_{message.id}_{uuid.uuid4().hex[:8]}.bin"
        try:
            await _update(f"\U0001F4E6 Scanning {label}\u2026")
            ok = await _stream_chunk_safe(media, size, tmp)
            if not ok:
                if asyncio.get_event_loop().time() < _dc_flood_until:
                    break
                continue

            result = await _probe(tmp)
            _, w, h = result[0], result[1], result[2]
            if w or h:
                return _build_caption(message, media, result), None

        except Exception as e:
            logger.warning(f"{label} probe error: {e}")
        finally:
            if os.path.exists(tmp):
                await aioremove(tmp)

    # --- FALLBACK: use Telegram's own metadata ---
    if progress_msg:
        await _update("\u26a0\ufe0f Deep scan failed \u2014 using Telegram metadata.")
    logger.warning(f"Falling back to Telegram metadata for msg {message.id}")
    return _build_caption_from_tg(message, media), None


async def _safe_edit(msg, text: str, parse_mode=None):
    if not msg:
        return
    key = msg.id
    now = asyncio.get_event_loop().time()
    if key in _last_edit and now - _last_edit[key] < 1.7:
        return
    try:
        await msg.edit_text(text, parse_mode=parse_mode)
        _last_edit[key] = now
    except MessageNotModified:
        pass
    except FloodWait as e:
        logger.warning(f"_safe_edit FloodWait: {e.value}s")
    except Exception as e:
        logger.warning(f"_safe_edit failed: {e}")


async def _process_channel_queue(channel_id: int):
    global EDIT_DELAY
    async with channel_semaphore:
        async with channel_locks[channel_id]:
            while channel_queues[channel_id]:
                message = channel_queues[channel_id].pop(0)

                await check_rate_limit()

                caption, file_path = await process_message(message)

                now = asyncio.get_event_loop().time()
                last = last_edit_time.get(channel_id, 0)
                if now - last < EDIT_DELAY:
                    await asyncio.sleep(EDIT_DELAY - (now - last))
                try:
                    await message.edit_caption(caption, parse_mode=ParseMode.HTML)
                    last_edit_time[channel_id] = asyncio.get_event_loop().time()
                    await save_last_id(channel_id, message.id)
                    await increment_stat(channel_id)
                    _reset_error_count(channel_id)
                except FloodWait as e:
                    EDIT_DELAY = max(EDIT_DELAY, e.value / 10 + 1)
                    await asyncio.sleep(e.value)
                    try:
                        await message.edit_caption(caption, parse_mode=ParseMode.HTML)
                        last_edit_time[channel_id] = asyncio.get_event_loop().time()
                        await save_last_id(channel_id, message.id)
                        await increment_stat(channel_id)
                        _reset_error_count(channel_id)
                    except MessageIdInvalid:
                        logger.warning("Edit skipped: Message deleted or invalid.")
                    except Exception as err:
                        logger.error(f"Retry edit failed: {err}")
                        await enqueue_retry(channel_id, message.id, caption)
                        await _report_error(channel_id, str(err))
                except MessageIdInvalid:
                    logger.warning("Edit skipped: Message deleted or invalid.")
                except MessageNotModified:
                    logger.warning("Edit skipped: Caption already up to date.")
                except Exception as e:
                    logger.error(f"Edit failed: {e}")
                    await enqueue_retry(channel_id, message.id, caption)
                    await _report_error(channel_id, str(e))

                if file_path and os.path.exists(file_path):
                    await aioremove(file_path)


@app.on_message(
    filters.channel & (filters.video | filters.document)
)
async def channel_handler(_, message):
    if message.chat.id not in authorized_chats:
        return
    if caption_has_media_info(message.caption or ''):
        return

    channel_id = message.chat.id
    channel_queues[channel_id].append(message)

    # Only spawn a worker if one isn't already draining this channel's queue.
    if not channel_workers[channel_id]:
        channel_workers[channel_id] = True
        asyncio.create_task(_channel_worker(channel_id))


async def _channel_worker(channel_id: int):
    try:
        await _process_channel_queue(channel_id)
    finally:
        channel_workers[channel_id] = False
        # Re-arm if new messages arrived after the queue drained.
        if channel_queues[channel_id]:
            channel_workers[channel_id] = True
            asyncio.create_task(_channel_worker(channel_id))


@app.on_message(filters.private & (filters.video | filters.document))
async def private_handler(_, message):
    user_id = message.from_user.id
    if user_id in active_users:
        await message.reply_text("\u26a0\ufe0f Please wait until your current file is processed.")
        return
    active_users.add(user_id)
    asyncio.create_task(_handle_private(message))


async def _handle_private(message):
    file_path = None
    progress_msg = None
    user_id = message.from_user.id
    try:
        await asyncio.sleep(0.5)
        progress_msg = await message.reply_text("\u23f3 Processing\u2026")
        caption, file_path = await process_message(message, progress_msg)
        await _safe_edit(progress_msg, caption, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Private handler error: {e}")
    finally:
        active_users.discard(user_id)
        if file_path and os.path.exists(file_path):
            await aioremove(file_path)


@app.on_message(filters.command("info") & filters.reply)
async def info_command(_, message):
    reply = message.reply_to_message
    if not (reply and (reply.video or reply.document)):
        return await message.reply_text("\u26a0\ufe0f Reply to a video or document.")

    media = reply.video or reply.document
    tmp = f"info_{reply.id}_{uuid.uuid4().hex[:6]}.bin"
    try:
        await _stream_chunk(media, 8 * 1024 * 1024, tmp)
        result = await _probe(tmp)
        caption = _build_caption(reply, media, result)
        await message.reply_text(caption, parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"\u274c Failed\n\n<code>{e}</code>", parse_mode=ParseMode.HTML)
    finally:
        if os.path.exists(tmp):
            await aioremove(tmp)


@app.on_message(filters.command("info") & ~filters.reply & (filters.video | filters.document))
async def info_command_forward(_, message):
    """Handle /info sent together with a video/document (no reply needed)."""
    media = message.video or message.document
    if not media:
        return await message.reply_text("\u26a0\ufe0f Send a video/document with the /info command, or reply to one.")

    tmp = f"info_fwd_{message.id}_{uuid.uuid4().hex[:6]}.bin"
    try:
        await _stream_chunk(media, 8 * 1024 * 1024, tmp)
        result = await _probe(tmp)
        caption = _build_caption(message, media, result)
        await message.reply_text(caption, parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"\u274c Failed\n\n<code>{e}</code>", parse_mode=ParseMode.HTML)
    finally:
        if os.path.exists(tmp):
            await aioremove(tmp)


# --- ADMIN COMMANDS ---

@app.on_message(filters.command("add") & filters.user(ADMIN_ID))
async def add_chat(_, message):
    try:
        if len(message.command) < 2:
            return await message.reply_text("\u274c Usage: `/add -100123456789`")

        new_id = int(message.command[1])
        authorized_chats.add(new_id)
        await settings_collection.update_one(
            {"_id": "allowed_chats"},
            {"$addToSet": {"chat_ids": new_id}},
            upsert=True,
        )
        await message.reply_text(f"\u2705 Added {new_id} to authorized chats.")
    except ValueError:
        await message.reply_text("\u274c Invalid chat ID. Use a numeric ID like `-100123456789`.")
    except Exception as e:
        await message.reply_text(f"\u274c Error: {e}")


@app.on_message(filters.command("remove") & filters.user(ADMIN_ID))
async def remove_chat(_, message):
    try:
        if len(message.command) < 2:
            return await message.reply_text("\u274c Usage: `/remove -100123456789`")

        target_id = int(message.command[1])
        if target_id in authorized_chats:
            authorized_chats.remove(target_id)
            await settings_collection.update_one(
                {"_id": "allowed_chats"},
                {"$pull": {"chat_ids": target_id}},
            )
            await message.reply_text(f"\u2705 Removed {target_id} from authorized chats.")
        else:
            await message.reply_text("\u274c Chat ID not found in list.")
    except ValueError:
        await message.reply_text("\u274c Invalid chat ID. Use a numeric ID like `-100123456789`.")
    except Exception as e:
        await message.reply_text(f"\u274c Error: {e}")


@app.on_message(filters.command("chats") & filters.user(ADMIN_ID))
async def list_chats(_, message):
    if not authorized_chats:
        return await message.reply_text("\U0001F4ED No authorized chats.")
    chat_list = "\n".join([f"<code>{cid}</code>" for cid in authorized_chats])
    await message.reply_text(f"<b>Allowed Chats:</b>\n{chat_list}", parse_mode=ParseMode.HTML)


@app.on_message(filters.command("stats") & filters.user(ADMIN_ID))
async def stats_cmd(_, message):
    all_stats = await get_all_stats()
    if not all_stats:
        return await message.reply_text("\U0001F4CA No files processed yet.")
    total = sum(s.get("count", 0) for s in all_stats)
    lines = [f"\U0001F4CA <b>Files Processed</b>\n<b>Total: {total}</b>\n"]
    for s in all_stats[:20]:
        lines.append(f"\u2022 <code>{s['chat_id']}</code> \u2192 <b>{s['count']}</b>")
    await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


@app.on_message(filters.command("broadcast") & filters.user(ADMIN_ID))
async def broadcast_cmd(_, message):
    if len(message.command) < 2:
        return await message.reply_text("\u274c Usage: `/broadcast Your message here`")
    text = message.text.split(None, 1)[1]
    sent = failed = 0
    for chat_id in list(authorized_chats):
        try:
            await app.send_message(chat_id, text)
            sent += 1
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                await app.send_message(chat_id, text)
                sent += 1
            except Exception as err:
                logger.warning(f"Broadcast failed for {chat_id}: {err}")
                failed += 1
        except Exception as e:
            logger.warning(f"Broadcast failed for {chat_id}: {e}")
            failed += 1
        await asyncio.sleep(0.4)
    await message.reply_text(f"\u2705 Broadcast done.\nSent: <b>{sent}</b> | Failed: <b>{failed}</b>", parse_mode=ParseMode.HTML)


@app.on_message(filters.command("queue") & filters.user(ADMIN_ID))
async def queue_cmd(_, message):
    lines = ["\U0001F4CB <b>Pending Queue</b>\n"]
    total = 0
    for cid, q in channel_queues.items():
        if q:
            lines.append(f"\u2022 <code>{cid}</code> \u2192 <b>{len(q)}</b> pending")
            total += len(q)
    if total == 0:
        return await message.reply_text("\u2705 All queues are empty.")
    lines.append(f"\n<b>Total pending: {total}</b>")
    await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


@app.on_message(filters.command("setdelay") & filters.user(ADMIN_ID))
async def setdelay_cmd(_, message):
    global EDIT_DELAY
    try:
        if len(message.command) < 2:
            return await message.reply_text(f"\u23f1 Current delay: <b>{EDIT_DELAY}s</b>\nUsage: <code>/setdelay 5.0</code>", parse_mode=ParseMode.HTML)
        val = float(message.command[1])
        if val < 1.0 or val > 60.0:
            return await message.reply_text("\u274c Delay must be between 1.0 and 60.0 seconds.")
        EDIT_DELAY = val
        await message.reply_text(f"\u2705 Edit delay set to <b>{EDIT_DELAY}s</b>", parse_mode=ParseMode.HTML)
    except ValueError:
        await message.reply_text("\u274c Invalid value. Use a number like `3.5`")


@app.on_message(filters.command("logs") & filters.user(ADMIN_ID))
async def logs_cmd(_, message):
    try:
        n = int(message.command[1]) if len(message.command) > 1 else 20
        n = max(1, min(n, 100))
    except ValueError:
        n = 20
    lines = list(_log_buffer)[-n:]
    if not lines:
        return await message.reply_text("\U0001F4ED No logs yet.")
    log_text = "\n".join(lines)
    if len(log_text) > 3800:
        log_text = "\u2026" + log_text[-3800:]
    await message.reply_text(f"<pre>{log_text}</pre>", parse_mode=ParseMode.HTML)


@app.on_message(filters.command("start") & filters.private)
async def start(_, m):
    await m.reply_text(
        "<b>\U0001F3AC Media Info Bot</b>\n\n"
        "Send me any video or file and I'll extract detailed media information.\n\n"
        "I provide:\n"
        "\u2022 \U0001F39E Video quality, codec &amp; bit depth\n"
        "\u2022 \u23f3 Duration\n"
        "\u2022 \U0001F50A Audio languages\n"
        "\u2022 \U0001F4AC Subtitle info\n\n"
        "<b>\u26a1 Fast \u2022 Clean \u2022 Accurate</b>\n\n"
        "\U0001F4CC <i>Note:</i> Send one file at a time.\n\n"
        "\U0001F916 Bot by @piroxbots",
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command("server") & filters.user(ADMIN_ID))
async def server_cmd(_, m):
    await m.reply_text(
        f"CPU: {psutil.cpu_percent()}%\n"
        f"RAM: {psutil.virtual_memory().percent}%\n"
        f"Disk: {psutil.disk_usage('/').percent}%"
    )


@app.on_message(filters.command("restart") & filters.user(ADMIN_ID))
async def restart_cmd(_, m):
    await m.reply_text("Restarting\u2026")
    os.execv(sys.executable, [sys.executable] + sys.argv)


@app.on_message(filters.command("shutdown") & filters.user(ADMIN_ID))
async def shutdown_cmd(_, m):
    await m.reply_text("Shutting down\u2026")
    asyncio.get_running_loop().call_soon(os._exit, 0)


@app.on_message(filters.command("update") & filters.user(ADMIN_ID))
async def update_cmd(_, m):
    await m.reply_text("Updating\u2026")
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "pull",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        await proc.communicate()
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "pip", "install", "-r", "requirements.txt",
            "--no-cache-dir", "-q",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        await proc.communicate()
        await m.reply_text("\u2705 Updated. Restarting\u2026")
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        await m.reply_text(f"Update failed: {e}")


def _install_deps():
    for binary, pkg in (("ffprobe", "ffmpeg"), ("mediainfo", "mediainfo")):
        r = subprocess.run(["which", binary], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if r.returncode != 0:
            logger.info(f"Installing {pkg}\u2026")
            subprocess.run(["apt", "update", "-y"], stdout=subprocess.DEVNULL)
            subprocess.run(["apt", "install", "-y", pkg], stdout=subprocess.DEVNULL)


# --- KOYEB HEALTH CHECK FEATURE ---
async def health_check(request):
    """Responds to Koyeb pings to keep the instance 'Healthy'."""
    return web.Response(text="Bot is running!", status=200)


async def start_health_server():
    """Starts the background web server on the port assigned by Koyeb."""
    app_web = web.Application()
    app_web.router.add_get("/", health_check)
    runner = web.AppRunner(app_web)
    await runner.setup()

    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Health Check server active on port {port}")


async def main():
    gc.set_threshold(*GC_THRESHOLD)
    _install_deps()

    await start_health_server()
    await sync_chats()

    await app.start()
    me = await app.get_me()
    logger.info(f"@{me.username} started")

    try:
        await app.send_message(ADMIN_ID, "\U0001F680 Bot Started & Health Check Online\n\nNew: /stats /queue /setdelay /logs /update")
    except Exception:
        pass

    scheduler.add_job(gc.collect, "interval", minutes=20)
    scheduler.add_job(process_retry_queue, "interval", minutes=2)
    scheduler.start()

    await asyncio.Event().wait()


if __name__ == "__main__":
    app.run(main())
