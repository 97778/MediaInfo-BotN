# MediaInfo Bot

A Telegram bot that extracts detailed media information (resolution, codec, bit
depth, HDR, duration, audio languages, subtitles) from videos and documents,
and rewrites their captions automatically in authorized channels.

## Features

- Streams only the first few MB of a file to probe metadata with `mediainfo` and `ffprobe`
- Falls back to Telegram's own metadata when a deep scan is rate-limited
- Dynamic per-chat authorization stored in MongoDB
- Persistent retry queue for failed caption edits
- Admin commands: `/add`, `/remove`, `/chats`, `/stats`, `/queue`, `/setdelay`, `/logs`, `/broadcast`, `/server`, `/restart`, `/shutdown`, `/update`
- Built-in aiohttp health check endpoint for platforms like Koyeb

## Health check

The bot serves `GET /` on the port given by the `PORT` environment variable
(default `8080`), returning `200 OK`. This is used by the Docker `HEALTHCHECK`
and by hosting platforms to confirm the instance is alive.

## Configuration

All configuration is read from environment variables (see `config.py`). Copy
`.env.example` to `.env` and fill in your values:

| Variable | Required | Description |
| --- | --- | --- |
| `API_ID` | yes | Telegram API ID |
| `API_HASH` | yes | Telegram API hash |
| `BOT_TOKEN` | yes | Bot token from @BotFather |
| `ADMIN_ID` | yes | Your Telegram user ID |
| `ALLOWED_CHATS` | no | Comma/space separated chat IDs |
| `MONGO_URI` | yes | MongoDB connection string |
| `PORT` | no | Health check port (default 8080) |
| `LOG_LEVEL` | no | Logging level (default INFO) |
| `CAPTION_TEMPLATE` | no | Caption format string |

## Run with Docker Compose

```bash
cp .env.example .env   # then edit .env
docker compose up -d --build
```

This starts the bot together with a MongoDB instance and wires up health checks
for both services.

## Run with Docker only

```bash
docker build -t mediainfo-bot .
docker run -d --env-file .env -p 8080:8080 mediainfo-bot
```

## Run locally

```bash
pip install -r requirements.txt
# ffmpeg and mediainfo must be installed on the host
export $(grep -v '^#' .env | xargs)
python main.py
```
