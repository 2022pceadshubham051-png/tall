"""
main.py — Tagoverse Bot (@Tagoverse_robot)

The Ultimate Telegram Mention Manager.

Single-file bot logic on top of Pyrogram v2. All persistence lives in
database.py. Run with:  python main.py
Requires a .env file — see the README block at the bottom of this file.
"""

from __future__ import annotations

import asyncio
import logging
import logging.handlers
import os
import platform
import random
import re
import shutil
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from pyrogram import Client, filters, __version__ as pyrogram_version
from pyrogram.enums import ChatMemberStatus, ChatType, ParseMode
from pyrogram.errors import FloodWait, RPCError, UserIsBlocked, InputUserDeactivated, PeerIdInvalid
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from database import Database

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
SUPPORT_URL = os.getenv("SUPPORT_URL", "https://t.me/")
DEVELOPER_URL = os.getenv("DEVELOPER_URL", "https://t.me/")
START_IMAGE = os.getenv("START_IMAGE", "")
HELP_IMAGE = os.getenv("HELP_IMAGE", "")
LOG_CHANNEL = int(os.getenv("LOG_CHANNEL", "0") or 0)

BOT_NAME = "Tagoverse Bot"
BOT_USERNAME = "Tagoverse_robot"
BOT_VERSION = "1.0.0"
LOG_FILE = Path("tagoverse.log")

if not BOT_TOKEN or not API_ID or not API_HASH or not OWNER_ID:
    print(
        "Missing required environment variables. Please fill BOT_TOKEN, "
        "API_ID, API_HASH and OWNER_ID in your .env file.",
        file=sys.stderr,
    )
    sys.exit(1)

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ],
)
logging.getLogger("pyrogram").setLevel(logging.INFO)
logger = logging.getLogger("tagoverse")

# --------------------------------------------------------------------------- #
# Globals
# --------------------------------------------------------------------------- #

app = Client(
    "tagoverse_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    parse_mode=ParseMode.HTML,
)
db = Database()

START_TIME = time.time()

#: chat_id -> asyncio.Event() ; when set() -> the running tag session must stop
STOP_FLAGS: dict[int, asyncio.Event] = {}
#: chat_id -> True while a tagall/tagadmins/tagrandom session is running
ACTIVE_SESSIONS: dict[int, bool] = {}

HELP_PAGES_TOTAL = 4

# --------------------------------------------------------------------------- #
# Templates — 12 emoji sets, 50+ unique emojis each
# --------------------------------------------------------------------------- #

TEMPLATES: dict[str, list[str]] = {
    "animals": [
        "🐶", "🐱", "🐭", "🐹", "🐰", "🦊", "🐻", "🐼", "🐨", "🐯",
        "🦁", "🐮", "🐷", "🐸", "🐵", "🙈", "🙉", "🙊", "🐔", "🐧",
        "🐦", "🐤", "🦆", "🦅", "🦉", "🦇", "🐺", "🐗", "🐴", "🦄",
        "🐝", "🐛", "🦋", "🐌", "🐞", "🐜", "🦂", "🐢", "🐍", "🦎",
        "🦖", "🦕", "🐙", "🦑", "🦐", "🦀", "🐬", "🐳", "🐋", "🦈",
        "🐊", "🐆", "🦓", "🦍", "🐘", "🦛", "🦏", "🐫", "🐪", "🦒",
    ],
    "gaming": [
        "🎮", "🕹️", "👾", "🎯", "🎲", "♟️", "🏆", "🥇", "🥈", "🥉",
        "🎳", "🎰", "🃏", "🀄", "🎴", "🧩", "🎪", "🎭", "🔫", "💣",
        "🛡️", "⚔️", "🗡️", "🏹", "🪄", "🧙", "🧝", "🧛", "🧟", "👑",
        "💎", "⭐", "🌟", "✨", "🔥", "⚡", "💥", "🚀", "🛸", "🤖",
        "🦸", "🦹", "🎖️", "🏅", "🎽", "🥊", "🛞", "🏎️", "🧨", "📀",
        "🎬", "🎞️", "🖲️", "⌨️", "🖥️", "📱", "🎧", "🔊", "📡", "🧭",
    ],
    "fire": [
        "🔥", "🧨", "💥", "⚡", "🌋", "☄️", "✨", "💫", "🌟", "⭐",
        "🔆", "🌡️", "🧯", "🕯️", "🪔", "💡", "🌞", "☀️", "🟠", "🔴",
        "🧡", "❤️‍🔥", "🥵", "🚨", "🏮", "🎆", "🎇", "🌅", "🌄", "🔶",
        "🔸", "🟥", "🟧", "🔺", "🔻", "⚔️", "🗡️", "💢", "🌬️", "🌪️",
        "🚀", "🛸", "🎯", "💪", "🦾", "🐉", "🐲", "🦅", "🦂", "🥇",
        "⛽", "🧪", "⚗️", "🔋", "🪫", "🧲", "🎏", "🚩", "🏁", "📛",
    ],
    "love": [
        "❤️", "🧡", "💛", "💚", "💙", "💜", "🖤", "🤍", "🤎", "💔",
        "❣️", "💕", "💞", "💓", "💗", "💖", "💘", "💝", "💟", "♥️",
        "😍", "🥰", "😘", "😻", "💋", "👩‍❤️‍👨", "👨‍❤️‍👨", "👩‍❤️‍👩", "💐", "🌹",
        "🌷", "🌸", "🌺", "🌻", "🥀", "💍", "💌", "🕊️", "💒", "👰",
        "🤵", "🫶", "🤗", "💑", "💏", "🍫", "🍬", "🧸", "🎀", "🎁",
        "🌙", "⭐", "✨", "🦢", "🍓", "🍒", "🥂", "🍾", "💃", "🕺",
    ],
    "nature": [
        "🌳", "🌲", "🌴", "🌵", "🌿", "☘️", "🍀", "🍁", "🍂", "🍃",
        "🌾", "🌱", "🌼", "🌻", "🌺", "🌸", "🌷", "🌹", "🥀", "🍄",
        "🌰", "🌊", "🌈", "☀️", "🌤️", "⛅", "🌥️", "☁️", "🌦️", "🌧️",
        "⛈️", "🌩️", "❄️", "☃️", "⛄", "🌬️", "💨", "🌪️", "🌫️", "🏔️",
        "⛰️", "🌋", "🗻", "🏕️", "🏞️", "🌅", "🌄", "🌠", "🌌", "🪨",
        "🐚", "🦋", "🐝", "🐞", "🌙", "⭐", "🍇", "🍎", "🍉", "🐿️",
    ],
    "funny": [
        "😂", "🤣", "😹", "😆", "😅", "😜", "🤪", "😝", "🤡", "🙃",
        "😏", "😎", "🤓", "🥸", "🫠", "🥴", "🤠", "👻", "💀", "☠️",
        "👽", "👾", "🤖", "🙈", "🙉", "🙊", "🐒", "🦧", "🐸", "🐔",
        "🦆", "🦥", "🐢", "🦨", "🦄", "🐷", "🐗", "🦃", "🥳", "🎠",
        "🫡", "🫢", "🫣", "🤭", "🦝", "🐵", "🐹", "🐭", "🦔", "🦦",
        "🤹", "🎪", "🎭", "🃏", "🪅", "🎈", "🎉", "🎊", "🥁", "📯",
    ],
    "premium": [
        "💎", "👑", "🏆", "🥇", "⭐", "🌟", "✨", "💰", "💵", "💴",
        "💶", "💷", "🪙", "💳", "🧧", "🎩", "🕶️", "🥂", "🍾", "🚁",
        "✈️", "🛥️", "🏎️", "🏰", "🗽", "🏛️", "🎖️", "🏵️", "🔱", "⚜️",
        "🔮", "🎇", "🎆", "🌠", "💫", "🦚", "🦢", "🐆", "🐅", "🦁",
        "🕊️", "🌹", "🍷", "🥃", "🍸", "🧿", "📿", "💠", "🔷", "🔶",
        "🟡", "🟨", "🎗️", "🏅", "🥈", "🥉", "🎯", "🧵", "🪡", "🖇️",
    ],
    "halloween": [
        "🎃", "👻", "💀", "☠️", "🧙", "🧟", "🧛", "🕷️", "🕸️", "🦇",
        "🐈‍⬛", "🌕", "🌑", "🌙", "⚰️", "🪦", "🔮", "🧪", "🩸", "🍬",
        "🍭", "🍫", "🕯️", "🏚️", "🌫️", "🌚", "🐍", "🦂", "🦉", "🐀",
        "🕵️", "🎭", "🗡️", "⚔️", "🧨", "🌩️", "⛈️", "🌪️", "🖤", "🩶",
        "😈", "👹", "👺", "👿", "🙀", "🐺", "🌘", "🌗", "🥀", "🍂",
        "🍁", "🕳️", "🧹", "🧿", "🔥", "🪄", "🧵", "🪡", "🪤", "📯",
    ],
    "festival": [
        "🎉", "🎊", "🎈", "🎆", "🎇", "🪅", "🎁", "🎀", "🎐", "🎏",
        "🧨", "✨", "🌟", "🎶", "🎵", "🎷", "🎺", "🥁", "🪘", "🎸",
        "🎻", "🪕", "🎹", "📯", "🥂", "🍾", "🍰", "🎂", "🍮", "🍩",
        "🍭", "🍬", "🍡", "🍢", "🍨", "🧁", "🍫", "🎠", "🎡", "🎢",
        "🛝", "🎪", "🎭", "🤹", "🕺", "💃", "👯", "🪩", "🕯️", "🪔",
        "🏮", "🧧", "🪄", "🌈", "☀️", "🌸", "🌼", "🥻", "👘", "🪷",
    ],
    "night": [
        "🌙", "🌚", "🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘",
        "⭐", "🌟", "✨", "💫", "🌌", "🌠", "🦉", "🦇", "🐈‍⬛", "🕯️",
        "🪔", "💡", "🔦", "🛌", "🛏️", "😴", "💤", "🌃", "🏙️", "🌉",
        "🚦", "🚥", "🚨", "🎆", "🎇", "🪩", "🎶", "🎧", "☕", "🍷",
        "🥃", "🌫️", "❄️", "☁️", "🌆", "🔭", "🛰️", "🚀", "🛸", "👽",
        "🐺", "🦊", "🐻", "🐾", "🦡", "🦨", "🖤", "🩶", "🪄", "🔮",
    ],
}
TEMPLATE_NAMES = list(TEMPLATES.keys())  # excludes "random" pseudo-template


def resolve_template(name: str) -> str:
    """Turn 'random' into an actual concrete template name for one run."""
    if name == "random" or name not in TEMPLATES:
        return random.choice(TEMPLATE_NAMES)
    return name


def emoji_cycle(template: str):
    """Infinite shuffled emoji generator that avoids immediate repeats."""
    pool = list(TEMPLATES.get(template, TEMPLATES[TEMPLATE_NAMES[0]]))
    random.shuffle(pool)
    idx = 0
    while True:
        if idx >= len(pool):
            new_pool = list(pool)
            random.shuffle(new_pool)
            # avoid same emoji at the seam
            while new_pool[0] == pool[-1] and len(new_pool) > 1:
                random.shuffle(new_pool)
            pool = new_pool
            idx = 0
        yield pool[idx]
        idx += 1


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #

def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID


async def is_group_admin(chat_id: int, user_id: int) -> bool:
    if is_owner(user_id):
        return True
    try:
        member = await app.get_chat_member(chat_id, user_id)
        return member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except RPCError:
        return await db.is_admin(chat_id, user_id)


def human_timedelta(seconds: float) -> str:
    seconds = max(int(seconds), 0)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if not parts:
        parts.append(f"{seconds}s")
    return " ".join(parts)


VACATION_RE = re.compile(r"^(\d+)([mhd])$", re.IGNORECASE)
VACATION_UNTIL_RE = re.compile(r"^until\s+(\d{2})-(\d{2})-(\d{4})$", re.IGNORECASE)


def parse_vacation_duration(text: str) -> Optional[float]:
    """Returns a unix timestamp for when the vacation should end, or None if invalid."""
    text = text.strip()
    if not text:
        return time.time() + 7 * 86400  # default: 7 days

    m = VACATION_UNTIL_RE.match(text)
    if m:
        day, month, year = (int(x) for x in m.groups())
        try:
            target = datetime(year, month, day, 23, 59, 59)
        except ValueError:
            return None
        return target.timestamp()

    m = VACATION_RE.match(text)
    if m:
        amount, unit = int(m.group(1)), m.group(2).lower()
        seconds = {"m": 60, "h": 3600, "d": 86400}[unit]
        return time.time() + amount * seconds

    return None


def mention_html(user_id: int, emoji: str) -> str:
    return f'<a href="tg://user?id={user_id}">{emoji}</a>'


def db_size_human() -> str:
    try:
        size = Path(db._path).stat().st_size
    except OSError:
        size = 0
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def cpu_ram_disk() -> tuple[str, str, str]:
    try:
        import psutil

        cpu = f"{psutil.cpu_percent(interval=0.2)}%"
        mem = psutil.virtual_memory()
        ram = f"{mem.percent}% ({mem.used // (1024**2)}MB / {mem.total // (1024**2)}MB)"
        disk = shutil.disk_usage(".")
        disk_str = f"{disk.used // (1024**3)}GB / {disk.total // (1024**3)}GB"
        return cpu, ram, disk_str
    except ImportError:
        disk = shutil.disk_usage(".")
        disk_str = f"{disk.used // (1024**3)}GB / {disk.total // (1024**3)}GB"
        return "n/a (psutil not installed)", "n/a", disk_str


# --------------------------------------------------------------------------- #
# UI builders
# --------------------------------------------------------------------------- #

def start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Add To Group", url=f"https://t.me/{BOT_USERNAME}?startgroup=true")],
            [
                InlineKeyboardButton("📖 Help", callback_data="help:1"),
                InlineKeyboardButton("🌐 Support", url=SUPPORT_URL),
            ],
            [InlineKeyboardButton("👤 Developer", url=DEVELOPER_URL)],
        ]
    )


def start_caption() -> str:
    return (
        f"👋 <b>Welcome to {BOT_NAME}!</b>\n\n"
        f"🏷️ <b>The Ultimate Telegram Mention Manager.</b>\n\n"
        f"✨ <b>Features:</b>\n"
        f"• Hidden mention tagging for huge groups (200k+ members)\n"
        f"• 12 emoji templates with smart shuffling\n"
        f"• Vacation system to opt out of mentions\n"
        f"• Per-group settings & live analytics\n\n"
        f"🔖 <b>Version:</b> <code>{BOT_VERSION}</code>\n\n"
        f"Add me to your group and unleash the tagging engine!"
    )


HELP_CONTENT = {
    1: (
        "📖 <b>Help — Page 1/4</b>\n\n"
        "<b>Introduction</b>\n"
        f"{BOT_NAME} lets admins mention every member of a group without pinging "
        "by @username — using invisible emoji mentions instead.\n\n"
        "<b>Features</b>\n"
        "• Hidden HTML mentions (no @username spam)\n"
        "• Batch tagging with automatic FloodWait recovery\n"
        "• Templates, vacation mode, per-group settings\n\n"
        "<b>Basic Commands</b>\n"
        "/start — Launch the bot\n"
        "/help — Show this menu\n"
        "/status — Show group tagging status\n"
        "/vacation — Opt out of mentions"
    ),
    2: (
        "📖 <b>Help — Page 2/4</b>\n\n"
        "<b>Admin Commands</b>\n"
        "/tagall [text] — Mention everyone\n"
        "/tagadmins — Mention admins only\n"
        "/tagrandom — Mention random members\n"
        "/stop — Stop the running tag session\n\n"
        "<b>Settings</b>\n"
        "/settings — Configure everything via buttons\n\n"
        "<b>Templates</b>\n"
        "/templates — Pick an emoji theme for mentions"
    ),
    3: (
        "📖 <b>Help — Page 3/4</b>\n\n"
        "<b>Vacation</b>\n"
        "/vacation — default 7 days\n"
        "/vacation 30m | 12h | 3d\n"
        "/vacation until 25-12-2026\n\n"
        "<b>Statistics</b>\n"
        "/stats — Group tagging statistics\n\n"
        "<b>Tagging Engine</b>\n"
        "Members are tagged in configurable batches (default 8) with an "
        "automatic delay between batches. FloodWaits are handled transparently — "
        "the session resumes right where it left off."
    ),
    4: (
        "📖 <b>Help — Page 4/4</b>\n\n"
        "<b>FAQ</b>\n"
        "<b>Q:</b> Why hidden mentions instead of @username?\n"
        "<b>A:</b> It avoids spamming the chat with unreadable walls of usernames "
        "and works even for members without a username.\n\n"
        "<b>Q:</b> Can I stop a tagging session?\n"
        "<b>A:</b> Yes, any admin can send /stop at any time.\n\n"
        "<b>Credits</b>\n"
        f"Built with Pyrogram v2 • {BOT_NAME} © {datetime.now().year}"
    ),
}


def help_keyboard(page: int) -> InlineKeyboardMarkup:
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⬅ Previous", callback_data=f"help:{page - 1}"))
    nav.append(InlineKeyboardButton("🏠 Home", callback_data="home"))
    if page < HELP_PAGES_TOTAL:
        nav.append(InlineKeyboardButton("➡ Next", callback_data=f"help:{page + 1}"))
    return InlineKeyboardMarkup([nav])


# --------------------------------------------------------------------------- #
# /start & /help
# --------------------------------------------------------------------------- #

@app.on_message(filters.command("start") & filters.private)
async def cmd_start_private(client: Client, message: Message) -> None:
    await db.upsert_user(
        message.from_user.id, message.from_user.username or "", message.from_user.first_name or ""
    )
    if START_IMAGE:
        await message.reply_photo(START_IMAGE, caption=start_caption(), reply_markup=start_keyboard())
    else:
        await message.reply_text(start_caption(), reply_markup=start_keyboard(), disable_web_page_preview=True)


@app.on_message(filters.command("start") & filters.group)
async def cmd_start_group(client: Client, message: Message) -> None:
    await db.ensure_group(message.chat.id, message.chat.title or "")
    await message.reply_text(
        f"👋 <b>{BOT_NAME}</b> is now active in this group!\nUse /help to see what I can do.",
    )


@app.on_message(filters.command("help"))
async def cmd_help(client: Client, message: Message) -> None:
    text = HELP_CONTENT[1]
    if HELP_IMAGE:
        await message.reply_photo(HELP_IMAGE, caption=text, reply_markup=help_keyboard(1))
    else:
        await message.reply_text(text, reply_markup=help_keyboard(1))


@app.on_callback_query(filters.regex(r"^help:(\d+)$"))
async def cb_help(client: Client, cq: CallbackQuery) -> None:
    page = int(cq.matches[0].group(1))
    page = max(1, min(page, HELP_PAGES_TOTAL))
    text = HELP_CONTENT[page]
    try:
        if cq.message.photo:
            await cq.message.edit_caption(text, reply_markup=help_keyboard(page))
        else:
            await cq.message.edit_text(text, reply_markup=help_keyboard(page))
    except RPCError:
        pass
    await cq.answer()


@app.on_callback_query(filters.regex(r"^home$"))
async def cb_home(client: Client, cq: CallbackQuery) -> None:
    caption = start_caption()
    try:
        if cq.message.photo:
            await cq.message.edit_caption(caption, reply_markup=start_keyboard())
        else:
            await cq.message.edit_text(caption, reply_markup=start_keyboard())
    except RPCError:
        pass
    await cq.answer()


# --------------------------------------------------------------------------- #
# /status
# --------------------------------------------------------------------------- #

@app.on_message(filters.command("status") & filters.group)
async def cmd_status(client: Client, message: Message) -> None:
    chat_id = message.chat.id
    settings = await db.get_settings(chat_id)
    vac_until = await db.get_vacation(chat_id, message.from_user.id)
    vac_text = "Not on vacation"
    if vac_until and vac_until > time.time():
        vac_text = f"On vacation — {human_timedelta(vac_until - time.time())} remaining"

    text = (
        "📊 <b>Status</b>\n\n"
        f"🏝️ <b>Vacation:</b> {vac_text}\n"
        f"🎨 <b>Template:</b> {settings['template']}\n"
        f"📦 <b>Batch Size:</b> {settings['batch_size']}\n"
        f"⏱️ <b>Delay:</b> {settings['delay']}s\n"
        f"🏃 <b>Session Running:</b> {'Yes' if ACTIVE_SESSIONS.get(chat_id) else 'No'}"
    )
    await message.reply_text(text)


# --------------------------------------------------------------------------- #
# /vacation
# --------------------------------------------------------------------------- #

@app.on_message(filters.command("vacation") & filters.group)
async def cmd_vacation(client: Client, message: Message) -> None:
    chat_id = message.chat.id
    user_id = message.from_user.id
    arg_text = message.text.split(maxsplit=1)[1] if len(message.command) > 1 else ""

    current = await db.get_vacation(chat_id, user_id)
    if arg_text.lower() in ("off", "cancel", "stop") :
        await db.clear_vacation(chat_id, user_id)
        await message.reply_text("✅ Vacation cancelled — you're back in the tagging pool.")
        return

    until_ts = parse_vacation_duration(arg_text)
    if until_ts is None:
        await message.reply_text(
            "❌ Invalid format. Use:\n"
            "<code>/vacation</code> — 7 days\n"
            "<code>/vacation 30m</code> / <code>12h</code> / <code>3d</code>\n"
            "<code>/vacation until 25-12-2026</code>\n"
            "<code>/vacation off</code> — cancel"
        )
        return

    await db.set_vacation(chat_id, user_id, until_ts)
    remaining = human_timedelta(until_ts - time.time())
    await message.reply_text(f"🏝️ Vacation activated. You'll be skipped in mentions for {remaining}.")


# --------------------------------------------------------------------------- #
# Vacation auto-expiry background task
# --------------------------------------------------------------------------- #

async def vacation_expiry_loop() -> None:
    while True:
        try:
            cleared = await db.expire_vacations()
            if cleared:
                logger.info("Auto-restored %d user(s) from vacation", cleared)
        except Exception:
            logger.exception("Vacation expiry loop failed")
        await asyncio.sleep(60)


# --------------------------------------------------------------------------- #
# Membership sync helper (used before tagging)
# --------------------------------------------------------------------------- #

async def sync_members(chat_id: int) -> None:
    """Populate group_members from Telegram so tagging has a member list."""
    members = []
    try:
        async for m in app.get_chat_members(chat_id):
            if m.user is None:
                continue
            is_admin = m.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
            members.append((m.user.id, is_admin))
            await db.upsert_user(m.user.id, m.user.username or "", m.user.first_name or "", m.user.is_bot)
            if len(members) >= 500:
                await db.bulk_upsert_members(chat_id, members)
                members = []
    except RPCError as exc:
        logger.warning("Could not fully sync members for %s: %s", chat_id, exc)
    if members:
        await db.bulk_upsert_members(chat_id, members)


# --------------------------------------------------------------------------- #
# Tagging engine
# --------------------------------------------------------------------------- #

@dataclass
class TagResult:
    tagged: int = 0
    failed: int = 0
    flood_waits: int = 0


async def run_tag_session(
    message: Message,
    chat_id: int,
    user_ids: list[int],
    custom_text: str,
    reply_to: Optional[Message],
) -> TagResult:
    settings = await db.get_settings(chat_id)
    batch_size = max(1, int(settings.get("batch_size", 8)))
    delay = max(0.5, float(settings.get("delay", 3.0)))
    template_choice = settings.get("template", "random")
    template = resolve_template(template_choice)
    emojis = emoji_cycle(template)

    stop_event = STOP_FLAGS.setdefault(chat_id, asyncio.Event())
    stop_event.clear()
    ACTIVE_SESSIONS[chat_id] = True

    result = TagResult()
    progress_enabled = settings.get("progress", True)
    progress_msg: Optional[Message] = None
    total = len(user_ids)

    if progress_enabled and total > batch_size:
        try:
            progress_msg = await message.reply_text(f"🏷️ Tagging 0/{total}…")
        except RPCError:
            progress_msg = None

    try:
        for i in range(0, total, batch_size):
            if stop_event.is_set():
                break
            batch = user_ids[i : i + batch_size]
            mentions = " ".join(mention_html(uid, next(emojis)) for uid in batch)
            text = f"{custom_text}\n{mentions}" if custom_text else mentions

            sent = False
            attempts = 0
            while not sent and attempts < 5:
                attempts += 1
                try:
                    if reply_to is not None:
                        await reply_to.reply_text(text, disable_web_page_preview=True)
                    else:
                        await message.reply_text(text, disable_web_page_preview=True)
                    sent = True
                    result.tagged += len(batch)
                except FloodWait as fw:
                    result.flood_waits += 1
                    await db.log_event("FLOODWAIT", f"chat={chat_id} wait={fw.value}s")
                    logger.warning("FloodWait %ss in chat %s — sleeping", fw.value, chat_id)
                    await asyncio.sleep(fw.value + 1)
                except RPCError as exc:
                    result.failed += len(batch)
                    logger.error("Tag batch failed in %s: %s", chat_id, exc)
                    sent = True  # don't retry non-flood errors indefinitely

            if progress_msg is not None:
                try:
                    await progress_msg.edit_text(
                        f"🏷️ Tagging {min(i + batch_size, total)}/{total}…"
                    )
                except RPCError:
                    pass

            if stop_event.is_set():
                break
            await asyncio.sleep(delay)
    finally:
        ACTIVE_SESSIONS[chat_id] = False

    if progress_msg is not None:
        try:
            status = "⏹️ Stopped" if stop_event.is_set() else "✅ Completed"
            await progress_msg.edit_text(
                f"{status} — tagged {result.tagged}/{total} "
                f"(failed: {result.failed}, flood waits: {result.flood_waits})"
            )
        except RPCError:
            pass

    return result


async def start_tag_command(
    client: Client, message: Message, mode: str
) -> None:
    """mode: 'tagall' | 'tagadmins' | 'tagrandom'"""
    chat_id = message.chat.id

    if not await is_group_admin(chat_id, message.from_user.id):
        await message.reply_text("🚫 Only group admins can use this command.")
        return

    if ACTIVE_SESSIONS.get(chat_id):
        await message.reply_text("⚠️ A tagging session is already running. Use /stop to cancel it first.")
        return

    status_msg = await message.reply_text("🔄 Preparing member list…")
    await sync_members(chat_id)

    if mode == "tagall":
        all_ids = await db.get_member_ids(chat_id)
    elif mode == "tagadmins":
        all_ids = await db.get_admin_ids(chat_id)
    else:  # tagrandom
        all_ids = await db.get_member_ids(chat_id)
        random.shuffle(all_ids)
        all_ids = all_ids[: min(len(all_ids), 20)]

    settings = await db.get_settings(chat_id)
    if settings.get("vacation_mode", True):
        vacationing = await db.get_vacationing_ids(chat_id)
        all_ids = [uid for uid in all_ids if uid not in vacationing]

    if settings.get("ignore_bots", True):
        # bots were recorded during sync_members via users table
        bot_ids = set()
        for uid in list(all_ids):
            row = await db._execute("SELECT is_bot FROM users WHERE user_id=?", (uid,), fetch="one")
            if row and row.get("is_bot"):
                bot_ids.add(uid)
        all_ids = [uid for uid in all_ids if uid not in bot_ids]

    if not all_ids:
        await status_msg.edit_text("⚠️ No eligible members found to tag.")
        return

    custom_text = ""
    reply_to = None
    if len(message.command) > 1:
        custom_text = message.text.split(maxsplit=1)[1]
    if message.reply_to_message:
        reply_to = message.reply_to_message

    try:
        await status_msg.delete()
    except RPCError:
        pass

    start_ts = time.time()
    result = await run_tag_session(message, chat_id, all_ids, custom_text, reply_to)
    duration = time.time() - start_ts

    await db.log_session(
        chat_id=chat_id,
        user_id=message.from_user.id,
        command=mode,
        tagged_count=result.tagged,
        duration=duration,
        template=await db.get_template(chat_id),
        flood_waits=result.flood_waits,
        failed=result.failed,
    )


@app.on_message(filters.command("tagall") & filters.group)
async def cmd_tagall(client: Client, message: Message) -> None:
    await start_tag_command(client, message, "tagall")


@app.on_message(filters.command("tagadmins") & filters.group)
async def cmd_tagadmins(client: Client, message: Message) -> None:
    await start_tag_command(client, message, "tagadmins")


@app.on_message(filters.command("tagrandom") & filters.group)
async def cmd_tagrandom(client: Client, message: Message) -> None:
    await start_tag_command(client, message, "tagrandom")


@app.on_message(filters.command("stop") & filters.group)
async def cmd_stop(client: Client, message: Message) -> None:
    chat_id = message.chat.id
    if not await is_group_admin(chat_id, message.from_user.id):
        await message.reply_text("🚫 Only group admins can use this command.")
        return
    event = STOP_FLAGS.get(chat_id)
    if event and not event.is_set() and ACTIVE_SESSIONS.get(chat_id):
        event.set()
        await message.reply_text("⏹️ Stopping the active tag session…")
    else:
        await message.reply_text("ℹ️ No tag session is currently running.")


# --------------------------------------------------------------------------- #
# /templates
# --------------------------------------------------------------------------- #

def templates_keyboard(current: str) -> InlineKeyboardMarkup:
    rows = []
    row = []
    options = ["random"] + TEMPLATE_NAMES
    for name in options:
        label = f"✅ {name.title()}" if name == current else name.title()
        row.append(InlineKeyboardButton(label, callback_data=f"tpl:{name}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


@app.on_message(filters.command("templates") & filters.group)
async def cmd_templates(client: Client, message: Message) -> None:
    if not await is_group_admin(message.chat.id, message.from_user.id):
        await message.reply_text("🚫 Only group admins can use this command.")
        return
    current = await db.get_template(message.chat.id)
    await message.reply_text(
        "🎨 <b>Select an emoji template</b>\nThe current template is highlighted.",
        reply_markup=templates_keyboard(current),
    )


@app.on_callback_query(filters.regex(r"^tpl:(\w+)$"))
async def cb_template(client: Client, cq: CallbackQuery) -> None:
    chat_id = cq.message.chat.id
    if not await is_group_admin(chat_id, cq.from_user.id):
        await cq.answer("Only admins can change the template.", show_alert=True)
        return
    name = cq.matches[0].group(1)
    await db.set_template(chat_id, name)
    try:
        await cq.message.edit_text(
            "🎨 <b>Select an emoji template</b>\nThe current template is highlighted.",
            reply_markup=templates_keyboard(name),
        )
    except RPCError:
        pass
    await cq.answer(f"Template set to {name.title()}")


# --------------------------------------------------------------------------- #
# /settings
# --------------------------------------------------------------------------- #

def settings_keyboard(s: dict) -> InlineKeyboardMarkup:
    def onoff(key: str) -> str:
        return "✅ ON" if s.get(key) else "❌ OFF"

    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"🎨 Template: {s['template'].title()}", callback_data="set:template")],
            [InlineKeyboardButton(f"🎲 Random Templates: {onoff('random_templates')}", callback_data="set:toggle:random_templates")],
            [InlineKeyboardButton(f"📦 Batch Size: {s['batch_size']}", callback_data="set:batch")],
            [InlineKeyboardButton(f"⏱️ Delay: {s['delay']}s", callback_data="set:delay")],
            [InlineKeyboardButton(f"🤖 Ignore Bots: {onoff('ignore_bots')}", callback_data="set:toggle:ignore_bots")],
            [InlineKeyboardButton(f"👤 Ignore Deleted: {onoff('ignore_deleted')}", callback_data="set:toggle:ignore_deleted")],
            [InlineKeyboardButton(f"🏝️ Vacation Mode: {onoff('vacation_mode')}", callback_data="set:toggle:vacation_mode")],
            [InlineKeyboardButton(f"📊 Progress: {onoff('progress')}", callback_data="set:toggle:progress")],
            [InlineKeyboardButton(f"🔐 Admin Only: {onoff('admin_only')}", callback_data="set:toggle:admin_only")],
            [InlineKeyboardButton("♻️ Reset Settings", callback_data="set:reset")],
        ]
    )


@app.on_message(filters.command("settings") & filters.group)
async def cmd_settings(client: Client, message: Message) -> None:
    if not await is_group_admin(message.chat.id, message.from_user.id):
        await message.reply_text("🚫 Only group admins can use this command.")
        return
    s = await db.get_settings(message.chat.id)
    await message.reply_text("⚙️ <b>Group Settings</b>", reply_markup=settings_keyboard(s))


@app.on_callback_query(filters.regex(r"^set:"))
async def cb_settings(client: Client, cq: CallbackQuery) -> None:
    chat_id = cq.message.chat.id
    if not await is_group_admin(chat_id, cq.from_user.id):
        await cq.answer("Only admins can change settings.", show_alert=True)
        return

    data = cq.data.split(":")
    action = data[1]

    if action == "toggle":
        key = data[2]
        s = await db.get_settings(chat_id)
        s = await db.update_settings(chat_id, **{key: not s.get(key, False)})
        await cq.message.edit_text("⚙️ <b>Group Settings</b>", reply_markup=settings_keyboard(s))
        await cq.answer("Updated")
        return

    if action == "reset":
        s = await db.reset_settings(chat_id)
        await cq.message.edit_text("⚙️ <b>Group Settings</b>", reply_markup=settings_keyboard(s))
        await cq.answer("Settings reset to defaults")
        return

    if action == "template":
        current = await db.get_template(chat_id)
        await cq.message.edit_text(
            "🎨 <b>Select an emoji template</b>",
            reply_markup=templates_keyboard(current),
        )
        await cq.answer()
        return

    if action == "batch":
        await cq.message.edit_text(
            "📦 <b>Select batch size</b>",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(str(n), callback_data=f"set:batchval:{n}")
                        for n in (4, 8, 16, 32)
                    ],
                    [InlineKeyboardButton("🔙 Back", callback_data="set:back")],
                ]
            ),
        )
        await cq.answer()
        return

    if action == "batchval":
        n = int(data[2])
        s = await db.update_settings(chat_id, batch_size=n)
        await cq.message.edit_text("⚙️ <b>Group Settings</b>", reply_markup=settings_keyboard(s))
        await cq.answer(f"Batch size set to {n}")
        return

    if action == "delay":
        await cq.message.edit_text(
            "⏱️ <b>Select delay between batches</b>",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(f"{n}s", callback_data=f"set:delayval:{n}")
                        for n in (1, 2, 3, 5, 8)
                    ],
                    [InlineKeyboardButton("🔙 Back", callback_data="set:back")],
                ]
            ),
        )
        await cq.answer()
        return

    if action == "delayval":
        n = float(data[2])
        s = await db.update_settings(chat_id, delay=n)
        await cq.message.edit_text("⚙️ <b>Group Settings</b>", reply_markup=settings_keyboard(s))
        await cq.answer(f"Delay set to {n}s")
        return

    if action == "back":
        s = await db.get_settings(chat_id)
        await cq.message.edit_text("⚙️ <b>Group Settings</b>", reply_markup=settings_keyboard(s))
        await cq.answer()
        return


# --------------------------------------------------------------------------- #
# /stats
# --------------------------------------------------------------------------- #

@app.on_message(filters.command("stats") & filters.group)
async def cmd_stats(client: Client, message: Message) -> None:
    chat_id = message.chat.id
    s = await db.get_settings(chat_id)
    members = await db.get_member_ids(chat_id)
    vacationing = await db.get_vacationing_ids(chat_id)
    gs = await db.group_stats(chat_id)

    text = (
        "📈 <b>Group Statistics</b>\n\n"
        f"👥 <b>Members:</b> {len(members)}\n"
        f"🏝️ <b>On Vacation:</b> {len(vacationing)}\n"
        f"🎨 <b>Template:</b> {s['template']}\n"
        f"📦 <b>Batch Size:</b> {s['batch_size']}\n"
        f"⏱️ <b>Delay:</b> {s['delay']}s\n"
        f"🏷️ <b>Tag Sessions:</b> {gs['sessions']}\n"
        f"⚡ <b>Average Speed:</b> {round(gs['avg_speed'], 2)} users/sec"
    )
    await message.reply_text(text)


# --------------------------------------------------------------------------- #
# Owner: /broadcast, /restart, /logs, /botstats
# --------------------------------------------------------------------------- #

@app.on_message(filters.command("broadcast") & filters.private)
async def cmd_broadcast(client: Client, message: Message) -> None:
    if not is_owner(message.from_user.id):
        return
    if len(message.command) < 2 and not message.reply_to_message:
        await message.reply_text("Usage: /broadcast <message> (or reply to a message)")
        return

    text = message.text.split(maxsplit=1)[1] if len(message.command) > 1 else None
    source_msg = message.reply_to_message

    status = await message.reply_text("📢 Broadcasting…")
    start_ts = time.time()
    success = failed = 0

    user_ids = await db.all_user_ids()
    group_ids = await db.all_group_ids()
    targets = [("user", uid) for uid in user_ids] + [("group", gid) for gid in group_ids]

    for kind, target_id in targets:
        try:
            if source_msg:
                await source_msg.copy(target_id)
            else:
                await app.send_message(target_id, text)
            success += 1
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)
            try:
                if source_msg:
                    await source_msg.copy(target_id)
                else:
                    await app.send_message(target_id, text)
                success += 1
            except RPCError:
                failed += 1
        except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid):
            failed += 1
        except RPCError:
            failed += 1
        await asyncio.sleep(0.05)

    duration = time.time() - start_ts
    await db.log_broadcast(message.from_user.id, "all", success, failed, duration)
    await status.edit_text(
        f"✅ <b>Broadcast complete</b>\n\n"
        f"Success: {success}\nFailed: {failed}\nTime taken: {human_timedelta(duration)}"
    )


@app.on_message(filters.command("restart") & filters.private)
async def cmd_restart(client: Client, message: Message) -> None:
    if not is_owner(message.from_user.id):
        return
    await db.log_restart("manual /restart command")
    await message.reply_text("♻️ Restarting…")
    logger.info("Restart requested by owner")
    await client.stop()
    os.execv(sys.executable, [sys.executable] + sys.argv)


@app.on_message(filters.command("logs") & filters.private)
async def cmd_logs(client: Client, message: Message) -> None:
    if not is_owner(message.from_user.id):
        return
    if not LOG_FILE.exists():
        await message.reply_text("No log file found yet.")
        return
    await message.reply_document(str(LOG_FILE), caption="📄 Latest log file")


BOTSTATS_PAGES = [
    "overview", "users", "groups", "tags", "templates",
    "vacation", "database", "performance", "errors", "commands",
]


def botstats_keyboard(page: int) -> InlineKeyboardMarkup:
    total = len(BOTSTATS_PAGES)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ Previous", callback_data=f"bstats:{page - 1}"))
    nav.append(InlineKeyboardButton("🏠 Home", callback_data="bstats:0"))
    if page < total - 1:
        nav.append(InlineKeyboardButton("➡ Next", callback_data=f"bstats:{page + 1}"))
    return InlineKeyboardMarkup(
        [nav, [InlineKeyboardButton("🔄 Refresh", callback_data=f"bstats:{page}")]]
    )


async def render_botstats_page(page: int) -> str:
    name = BOTSTATS_PAGES[page]
    header = f"📊 <b>Bot Statistics — {name.title()}</b> ({page + 1}/{len(BOTSTATS_PAGES)})\n\n"

    if name == "overview":
        cpu, ram, disk = cpu_ram_disk()
        return header + (
            f"⏱️ <b>Uptime:</b> {human_timedelta(time.time() - START_TIME)}\n"
            f"🐍 <b>Python:</b> {platform.python_version()}\n"
            f"📡 <b>Pyrogram:</b> {pyrogram_version}\n"
            f"🗄️ <b>SQLite:</b> {sqlite3.sqlite_version}\n"
            f"🏓 <b>Ping:</b> {round((app.ping() if hasattr(app, 'ping') else 0), 2)}ms\n"
            f"🧮 <b>CPU:</b> {cpu}\n"
            f"💾 <b>RAM:</b> {ram}\n"
            f"💽 <b>Disk:</b> {disk}\n"
            f"🗃️ <b>Database Size:</b> {db_size_human()}"
        )

    if name == "users":
        total = await db.total_users()
        daily = await db.users_since(86400)
        weekly = await db.users_since(7 * 86400)
        monthly = await db.users_since(30 * 86400)
        return header + (
            f"👥 <b>Total Users:</b> {total}\n"
            f"📅 <b>Daily Active:</b> {daily}\n"
            f"🗓️ <b>Weekly Active:</b> {weekly}\n"
            f"📆 <b>Monthly Active:</b> {monthly}"
        )

    if name == "groups":
        total = await db.group_count()
        largest = await db.largest_group()
        smallest = await db.smallest_group()
        avg = await db.average_group_size()
        active, inactive = await db.active_inactive_groups()
        largest_txt = f"{largest['title'] or largest['chat_id']} ({largest['members']} members)" if largest else "n/a"
        smallest_txt = f"{smallest['title'] or smallest['chat_id']} ({smallest['members']} members)" if smallest else "n/a"
        return header + (
            f"🏘️ <b>Total Groups:</b> {total}\n"
            f"📈 <b>Largest:</b> {largest_txt}\n"
            f"📉 <b>Smallest:</b> {smallest_txt}\n"
            f"⚖️ <b>Average Members:</b> {avg}\n"
            f"🟢 <b>Active (7d):</b> {active}\n"
            f"🔴 <b>Inactive:</b> {inactive}"
        )

    if name == "tags":
        total = await db.total_sessions()
        today = await db.stats_since(86400)
        weekly = await db.stats_since(7 * 86400)
        monthly = await db.stats_since(30 * 86400)
        avg_speed = await db.average_speed()
        avg_batch = await db.average_batch_size()
        fw = await db.total_flood_waits()
        failed = await db.total_failed()
        rate = await db.success_rate()
        return header + (
            f"🏷️ <b>Total Sessions:</b> {total}\n"
            f"📅 <b>Today:</b> {today['c']}\n"
            f"🗓️ <b>This Week:</b> {weekly['c']}\n"
            f"📆 <b>This Month:</b> {monthly['c']}\n"
            f"⚡ <b>Average Speed:</b> {avg_speed} users/sec\n"
            f"📦 <b>Average Batch Size:</b> {avg_batch}\n"
            f"⏳ <b>FloodWait Count:</b> {fw}\n"
            f"❌ <b>Failed Mentions:</b> {failed}\n"
            f"✅ <b>Success Rate:</b> {rate}%"
        )

    if name == "templates":
        usage = await db.template_usage()
        if not usage:
            return header + "No template usage data yet."
        total_uses = sum(u["c"] for u in usage)
        lines = [f"• {u['template'].title()}: {u['c']} ({round(u['c'] / total_uses * 100, 1)}%)" for u in usage]
        most = usage[0]["template"].title()
        least = usage[-1]["template"].title()
        return header + "\n".join(lines) + f"\n\n🏆 <b>Most Popular:</b> {most}\n🥱 <b>Least Popular:</b> {least}"

    if name == "vacation":
        active = await db.total_vacationing()
        expired_today = await db.vacation_expired_today()
        return header + (
            f"🏝️ <b>Users on Vacation:</b> {active}\n"
            f"⏳ <b>Expired Today:</b> {expired_today}"
        )

    if name == "database":
        tables = await db.table_info()
        rows_txt = "\n".join(f"• {t['table']}: {t['rows']} rows" for t in tables)
        idx = await db.index_count()
        return header + f"{rows_txt}\n\n📐 <b>Indexes:</b> {idx}\n🗃️ <b>Size:</b> {db_size_human()}"

    if name == "performance":
        top_cmds = await db.top_commands()
        top_groups = await db.top_groups()
        top_admins = await db.top_admins()
        cmds_txt = "\n".join(f"• {c['command']}: {c['c']}" for c in top_cmds) or "n/a"
        groups_txt = "\n".join(f"• {g['chat_id']}: {g['c']}" for g in top_groups) or "n/a"
        admins_txt = "\n".join(f"• {a['user_id']}: {a['c']}" for a in top_admins) or "n/a"
        return header + (
            f"<b>Top Commands</b>\n{cmds_txt}\n\n"
            f"<b>Top Groups</b>\n{groups_txt}\n\n"
            f"<b>Top Admins</b>\n{admins_txt}"
        )

    if name == "errors":
        restarts = await db.restart_count()
        errors = await db.error_count("ERROR")
        floods = await db.flood_wait_log_count()
        db_fail = await db.db_failure_count()
        return header + (
            f"♻️ <b>Restart Count:</b> {restarts}\n"
            f"💥 <b>Exceptions:</b> {errors}\n"
            f"⏳ <b>FloodWaits Logged:</b> {floods}\n"
            f"🗄️ <b>Database Failures:</b> {db_fail}"
        )

    if name == "commands":
        top_cmds = await db.top_commands(limit=10)
        cmds_txt = "\n".join(f"• /{c['command']}: {c['c']} uses" for c in top_cmds) or "No usage yet."
        return header + cmds_txt

    return header + "No data."


@app.on_message(filters.command("botstats") & filters.private)
async def cmd_botstats(client: Client, message: Message) -> None:
    if not is_owner(message.from_user.id):
        return
    text = await render_botstats_page(0)
    await message.reply_text(text, reply_markup=botstats_keyboard(0))


@app.on_callback_query(filters.regex(r"^bstats:(\d+)$"))
async def cb_botstats(client: Client, cq: CallbackQuery) -> None:
    if not is_owner(cq.from_user.id):
        await cq.answer("Owner only.", show_alert=True)
        return
    page = int(cq.matches[0].group(1))
    page = max(0, min(page, len(BOTSTATS_PAGES) - 1))
    text = await render_botstats_page(page)
    try:
        await cq.message.edit_text(text, reply_markup=botstats_keyboard(page))
    except RPCError:
        pass
    await cq.answer()


# --------------------------------------------------------------------------- #
# Passive tracking — keep users/groups/admin status fresh
# --------------------------------------------------------------------------- #

@app.on_message(filters.group & ~filters.service, group=1)
async def track_activity(client: Client, message: Message) -> None:
    try:
        await db.ensure_group(message.chat.id, message.chat.title or "")
        if message.from_user and not message.from_user.is_bot:
            await db.upsert_user(
                message.from_user.id,
                message.from_user.username or "",
                message.from_user.first_name or "",
            )
            await db.upsert_member(message.chat.id, message.from_user.id)
    except Exception:
        logger.exception("track_activity failed")


# --------------------------------------------------------------------------- #
# Global error boundary for message handlers
# --------------------------------------------------------------------------- #

async def safe_wrapper(handler):
    async def wrapped(client: Client, update):
        try:
            await handler(client, update)
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Unhandled error in handler %s", handler.__name__)
            await db.log_event("ERROR", f"{handler.__name__}: {exc}")
            if LOG_CHANNEL:
                try:
                    await app.send_message(LOG_CHANNEL, f"⚠️ Error in {handler.__name__}: {exc}")
                except RPCError:
                    pass
    return wrapped


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #

async def main() -> None:
    await db.connect()
    await app.start()
    logger.info("%s started as @%s", BOT_NAME, BOT_USERNAME)
    asyncio.create_task(vacation_expiry_loop())
    if LOG_CHANNEL:
        try:
            await app.send_message(LOG_CHANNEL, f"✅ {BOT_NAME} started.")
        except RPCError:
            pass
    await asyncio.Event().wait()  # run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")

# --------------------------------------------------------------------------- #
# .env template (copy to a file named ".env" next to this script):
#
# BOT_TOKEN=123456:ABC-DEF...
# API_ID=123456
# API_HASH=your_api_hash
# OWNER_ID=123456789
# SUPPORT_URL=https://t.me/your_support_chat
# DEVELOPER_URL=https://t.me/your_username
# START_IMAGE=https://example.com/start_banner.jpg
# HELP_IMAGE=https://example.com/help_banner.jpg
# LOG_CHANNEL=-1001234567890
#
# requirements.txt:
# pyrogram>=2.0.106
# tgcrypto
# python-dotenv
# psutil
# --------------------------------------------------------------------------- #
