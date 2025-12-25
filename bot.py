"""
╔══════════════════════════════════════════════════════════════════════╗
║            ⚡ 𝐇𝐘𝐏𝐄𝐑-𝐗 𝐌𝐮𝐥𝐭𝐢-𝐁𝐨𝐭 𝐂𝐨𝐧𝐭𝐫𝐨𝐥𝐥𝐞𝐫 ⚡                  ║
╠══════════════════════════════════════════════════════════════════════╣
║  Multi-User | Each User Has Own Bot Space | HYPER-X v3.0 Powered     ║
╠══════════════════════════════════════════════════════════════════════╣
║  Credit: Dev 🎀👑😛 (@god_olds)                                       ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import os
import pytz
import time
import random
import logging
import json
from typing import Dict, List, Set, Optional
from datetime import datetime

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, 
    CallbackQueryHandler, ConversationHandler, filters, ContextTypes
)
from telegram.constants import ChatType, ParseMode
from telegram.error import RetryAfter, TimedOut, NetworkError, BadRequest, Forbidden

logging.basicConfig(
    format="%(asctime)s - [HYPER-X] - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("HyperX")

DB_DIR = "data"
CONTROLLER_TOKEN = "8492426300:AAEasavi51hrI8OqrbUQzkDmdf9OViSDS6c"

WAITING_TOKEN = 1
WAITING_OWNER_ID = 2
WAITING_NC_TEMPLATE = 3
WAITING_SPAM_TEMPLATE = 4
WAITING_BROADCAST = 5

HEART_EMOJIS = ['❤️', '🧡', '💛', '💚', '💙', '💜', '🤎', '🖤', '🤍', '💘', '💝', '💖', '💗', '💓', '💞', '💌', '💕', '💟', '♥️', '❣️', '💔']

DEFAULT_NC_MESSAGES = [
    "DEV TERA BAAP  🔥⃤⃟⃝🐦‍🔥『🚩』",
    "{target} TERI BHEN KA BHOSADA 🔥⃤⃟⃝🐦‍🔥『🚩』",
    "{target} TERI MAA DEV KE LUND PR 🔥⃤⃟⃝🐦‍🔥『🚩』",
    "{target} TERI MAA KA BHOSADA CHUDA 🔥⃤⃟⃝🐦‍🔥『🚩』",
    "{target} TERI CHUDAYI BY DEV PAPA 🔥⃤⃟⃝🐦‍🔥『🚩』",
    "{target} CVR LE RANDI KE BACCHE 🔥⃤⃟⃝🐦‍🔥『🚩』",
    "{target} TERI MAA RANDI 🔥⃤⃟⃝🐦‍🔥 『🚩』",
    "{target} TERI BHEN KAALI CHUT 🔥⃤⃟⃝🐦‍🔥『🚩』",
]

DEFAULT_REPLY_MESSAGES = [
    "{target} ---RDI🐣",
    "{target} चुद गया -!",
    "Aʟᴏᴏ Kʜᴀᴋᴇ {target} Kɪ Mᴀ Cʜᴏᴅ Dᴜɴɢᴀ!",
    "{target} Cʜᴜᴅᴀ🦖🪽",
    "{target} Bᴏʟᴇ ᴅᴇᴠ ᴘᴀᴘᴀ पिताश्री Mᴇʀɪ Mᴀ Cʜᴏᴅ Dᴏ",
    "{target} Kɪ Mᴀ Bᴏʟᴇ ᴅᴇᴠ ᴘᴀᴘᴀ Sᴇ Cʜᴜᴅᴜɴɢɪ",
    "{target} Kɪ Bᴇʜɴ Kɪ Cʜᴜᴛ Kᴀʟɪ Kᴀʟɪ",
    "{target} Kɪ Mᴀ Rᴀɴᴅɪ",
    "{target} ɢᴀʀᴇᴇʙ ᴋᴀ ʙᴀᴄʜʜᴀ",
    "{target} ᴄʜᴜᴅ ᴋᴇ ᴘᴀɢᴀʟ ʜᴏɢᴀʏᴀ",
]

DEFAULT_SPAM_MESSAGES = [
    "✝ 𝐀ɴᴛᴀ𝐑 𝐌ᴀɴ𝐓ᴀʀ 𝐒ʜᴀɪ𝐓ᴀɴ𝐈 𝐊ʜᴏ𝐏ᴀᴅ𝐀 {target} 𝐆ᴀ𝐑ɪ𝐁 𝐊ɪ 𝐀ᴍᴍ𝐈 𝐊ᴀ 𝐊ᴀʟ𝐀 𝐁ʜᴏs𝐃ᴀ",
    "{target} 𝙏𝙀𝙍𝙄 𝙈𝘼𝘼 𝘽𝘼𝙃𝘼𝙉 𝘿𝙊𝙉𝙊 𝙆𝙊 𝙍𝘼𝙉𝘿𝙄 𝙆𝙊 𝘾𝙃𝙊𝘿𝙐 🤣",
    "{target} 𝐓𝐄𝐑𝐈 𝐌𝐀𝐀_𝐁𝐀𝐇𝐀𝐍 𝐊𝐎 𝐂𝐇𝐎𝐃𝐔 𝐁𝐈𝐍𝐀 𝐂𝐎𝐍𝐃𝐎𝐌 𝐊𝐄 😝",
]

UNAUTHORIZED_MSG = "𝐃𝐄𝐕 𝐏𝐀𝐏𝐀 𝐒𝐄 𝐁𝐈𝐊𝐇 𝐌𝐀𝐍𝐆 🤣🎀😻"


def init_database():
    os.makedirs(DB_DIR, exist_ok=True)
    files = {
        "users.json": {},
        "user_bots.json": {},
        "bot_authorized_users.json": [],
        "user_nc_templates.json": [],
        "user_spam_templates.json": [],
        "user_reply_templates.json": [],
        "permissions.json": []
    }
    for fname, default in files.items():
        fpath = os.path.join(DB_DIR, fname)
        if not os.path.exists(fpath):
            with open(fpath, 'w') as f:
                json.dump(default, f)
    logger.info("Database initialized!")


class Database:
    _bot_id_counter = 0
    _template_counters = {}

    @staticmethod
    def _read_json(filename):
        fpath = os.path.join(DB_DIR, filename)
        try:
            with open(fpath, 'r') as f:
                return json.load(f)
        except:
            return {} if filename.endswith('.json') and 'users' in filename else []

    @staticmethod
    def _write_json(filename, data):
        fpath = os.path.join(DB_DIR, filename)
        with open(fpath, 'w') as f:
            json.dump(data, f, indent=2)

    @staticmethod
    def register_user(user_id: int, username: str = None):
        users = Database._read_json("users.json")
        users[str(user_id)] = {
            "user_id": user_id,
            "username": username,
            "is_sudo": users.get(str(user_id), {}).get("is_sudo", False),
            "created_at": users.get(str(user_id), {}).get("created_at", datetime.now().isoformat())
        }
        Database._write_json("users.json", users)

    @staticmethod
    def is_sudo(user_id: int) -> bool:
        users = Database._read_json("users.json")
        return users.get(str(user_id), {}).get("is_sudo", False)

    @staticmethod
    def add_sudo(user_id: int) -> bool:
        users = Database._read_json("users.json")
        if str(user_id) not in users:
            users[str(user_id)] = {"user_id": user_id, "username": None, "created_at": datetime.now().isoformat()}
        users[str(user_id)]["is_sudo"] = True
        Database._write_json("users.json", users)
        return True

    @staticmethod
    def remove_sudo(user_id: int) -> bool:
        users = Database._read_json("users.json")
        if str(user_id) in users:
            users[str(user_id)]["is_sudo"] = False
            Database._write_json("users.json", users)
            return True
        return False

    @staticmethod
    def get_sudos() -> List[int]:
        users = Database._read_json("users.json")
        return [int(uid) for uid, data in users.items() if data.get("is_sudo", False)]

    @staticmethod
    def add_bot(user_id: int, token: str, username: str = None, name: str = None) -> Optional[int]:
        bots = Database._read_json("user_bots.json")
        for bot in bots.values():
            if bot["token"] == token:
                return None
        Database._bot_id_counter += 1
        bot_id = Database._bot_id_counter
        bots[str(bot_id)] = {
            "id": bot_id,
            "user_id": user_id,
            "token": token,
            "bot_username": username,
            "bot_name": name,
            "is_running": False,
            "created_at": datetime.now().isoformat()
        }
        Database._write_json("user_bots.json", bots)
        return bot_id

    @staticmethod
    def remove_bot(user_id: int, bot_id: int) -> bool:
        bots = Database._read_json("user_bots.json")
        if str(bot_id) in bots and bots[str(bot_id)]["user_id"] == user_id:
            del bots[str(bot_id)]
            Database._write_json("user_bots.json", bots)
            return True
        return False

    @staticmethod
    def get_user_bots(user_id: int) -> List[dict]:
        bots = Database._read_json("user_bots.json")
        return [bot for bot in bots.values() if bot["user_id"] == user_id]

    @staticmethod
    def get_bot(bot_id: int) -> Optional[dict]:
        bots = Database._read_json("user_bots.json")
        return bots.get(str(bot_id))

    @staticmethod
    def get_bot_by_token(token: str) -> Optional[dict]:
        bots = Database._read_json("user_bots.json")
        for bot in bots.values():
            if bot["token"] == token:
                return bot
        return None

    @staticmethod
    def update_bot_status(bot_id: int, is_running: bool):
        bots = Database._read_json("user_bots.json")
        if str(bot_id) in bots:
            bots[str(bot_id)]["is_running"] = is_running
            Database._write_json("user_bots.json", bots)

    @staticmethod
    def update_bot_info(token: str, username: str, name: str):
        bots = Database._read_json("user_bots.json")
        for bot in bots.values():
            if bot["token"] == token:
                bot["bot_username"] = username
                bot["bot_name"] = name
                Database._write_json("user_bots.json", bots)
                return

    @staticmethod
    def add_bot_user(bot_id: int, user_id: int, added_by: int) -> bool:
        auth = Database._read_json("bot_authorized_users.json")
        for entry in auth:
            if entry["bot_id"] == bot_id and entry["user_id"] == user_id:
                return False
        auth.append({
            "id": len(auth) + 1,
            "bot_id": bot_id,
            "user_id": user_id,
            "added_by": added_by,
            "created_at": datetime.now().isoformat()
        })
        Database._write_json("bot_authorized_users.json", auth)
        return True

    @staticmethod
    def remove_bot_user(bot_id: int, user_id: int) -> bool:
        auth = Database._read_json("bot_authorized_users.json")
        original_len = len(auth)
        auth = [e for e in auth if not (e["bot_id"] == bot_id and e["user_id"] == user_id)]
        if len(auth) < original_len:
            Database._write_json("bot_authorized_users.json", auth)
            return True
        return False

    @staticmethod
    def get_bot_users(bot_id: int) -> List[int]:
        auth = Database._read_json("bot_authorized_users.json")
        return [e["user_id"] for e in auth if e["bot_id"] == bot_id]

    @staticmethod
    def _add_template(filename, user_id, template):
        data = Database._read_json(filename)
        tid = max([t.get("id", 0) for t in data], default=0) + 1
        data.append({"id": tid, "user_id": user_id, "template": template, "created_at": datetime.now().isoformat()})
        Database._write_json(filename, data)
        return tid

    @staticmethod
    def _get_templates(filename, user_id):
        data = Database._read_json(filename)
        return [t for t in data if t["user_id"] == user_id]

    @staticmethod
    def _remove_templates(filename, user_id, template_id=None):
        data = Database._read_json(filename)
        original_len = len(data)
        if template_id:
            data = [t for t in data if not (t["user_id"] == user_id and t["id"] == template_id)]
        else:
            data = [t for t in data if t["user_id"] != user_id]
        Database._write_json(filename, data)
        return original_len - len(data)

    @staticmethod
    def add_nc_template(user_id: int, template: str) -> int:
        return Database._add_template("user_nc_templates.json", user_id, template)

    @staticmethod
    def get_nc_templates(user_id: int) -> List[dict]:
        return Database._get_templates("user_nc_templates.json", user_id)

    @staticmethod
    def remove_nc_template(user_id: int, template_id: int = None) -> int:
        return Database._remove_templates("user_nc_templates.json", user_id, template_id)

    @staticmethod
    def add_spam_template(user_id: int, template: str) -> int:
        return Database._add_template("user_spam_templates.json", user_id, template)

    @staticmethod
    def get_spam_templates(user_id: int) -> List[dict]:
        return Database._get_templates("user_spam_templates.json", user_id)

    @staticmethod
    def remove_spam_template(user_id: int, template_id: int = None) -> int:
        return Database._remove_templates("user_spam_templates.json", user_id, template_id)

    @staticmethod
    def add_reply_template(user_id: int, template: str) -> int:
        return Database._add_template("user_reply_templates.json", user_id, template)

    @staticmethod
    def get_reply_templates(user_id: int) -> List[dict]:
        return Database._get_templates("user_reply_templates.json", user_id)

    @staticmethod
    def remove_reply_template(user_id: int, template_id: int = None) -> int:
        return Database._remove_templates("user_reply_templates.json", user_id, template_id)

    @staticmethod
    def grant_permission(user_id: int, permission: str, granted_by: int) -> bool:
        perms = Database._read_json("permissions.json")
        for p in perms:
            if p["user_id"] == user_id and p["permission"] == permission:
                return False
        perms.append({
            "id": len(perms) + 1,
            "user_id": user_id,
            "permission": permission,
            "granted_by": granted_by,
            "created_at": datetime.now().isoformat()
        })
        Database._write_json("permissions.json", perms)
        return True

    @staticmethod
    def revoke_permission(user_id: int, permission: str = None) -> bool:
        perms = Database._read_json("permissions.json")
        original_len = len(perms)
        if permission:
            perms = [p for p in perms if not (p["user_id"] == user_id and p["permission"] == permission)]
        else:
            perms = [p for p in perms if p["user_id"] != user_id]
        Database._write_json("permissions.json", perms)
        return len(perms) < original_len

    @staticmethod
    def get_permissions(user_id: int) -> List[str]:
        perms = Database._read_json("permissions.json")
        return [p["permission"] for p in perms if p["user_id"] == user_id]

    @staticmethod
    def has_permission(user_id: int, permission: str) -> bool:
        perms = Database.get_permissions(user_id)
        return 'all' in perms or permission in perms


COMMAND_LOCKS: Dict[int, asyncio.Lock] = {}

def get_lock(chat_id: int) -> asyncio.Lock:
    if chat_id not in COMMAND_LOCKS:
        COMMAND_LOCKS[chat_id] = asyncio.Lock()
    return COMMAND_LOCKS[chat_id]


class ChildBot:
    def __init__(self, bot_id: int, token: str, owner_id: int):
        self.bot_id = bot_id
        self.token = token
        self.owner_id = owner_id
        self.active_spam: Dict[int, List[asyncio.Task]] = {}
        self.active_nc: Dict[int, List[asyncio.Task]] = {}
        self.active_custom_nc: Dict[int, List[asyncio.Task]] = {}
        self.active_reply: Dict[int, asyncio.Task] = {}
        self.reply_targets: Dict[int, str] = {}
        self.pending_replies: Dict[int, List[int]] = {}
        self.delays: Dict[int, float] = {}
        self.threads: Dict[int, int] = {}
        self.locks: Dict[int, asyncio.Lock] = {}
        self.stats = {"sent": 0, "errors": 0, "start": time.time()}
        self.running = True
        self.app = None
        self.stop_event = asyncio.Event()

    def get_lock(self, chat_id):
        if chat_id not in self.locks:
            self.locks[chat_id] = asyncio.Lock()
        return self.locks[chat_id]

    def is_authorized(self, user_id: int) -> bool:
        if user_id == self.owner_id:
            return True
        return user_id in Database.get_bot_users(self.bot_id)

    async def check_auth(self, update) -> bool:
        if not self.is_authorized(update.effective_user.id):
            try:
                await update.message.reply_text(UNAUTHORIZED_MSG)
            except:
                pass
            return False
        return True

    async def cancel_tasks(self, tasks: List[asyncio.Task]):
        for t in tasks:
            if not t.done():
                t.cancel()
        for t in tasks:
            try:
                await asyncio.wait_for(asyncio.shield(t), timeout=2.0)
            except:
                pass

    async def stop_all(self):
        all_tasks = []
        for tasks in list(self.active_spam.values()):
            all_tasks.extend(tasks)
        self.active_spam.clear()
        for tasks in list(self.active_nc.values()):
            all_tasks.extend(tasks)
        self.active_nc.clear()
        for tasks in list(self.active_custom_nc.values()):
            all_tasks.extend(tasks)
        self.active_custom_nc.clear()
        for task in list(self.active_reply.values()):
            all_tasks.append(task)
        self.active_reply.clear()
        self.reply_targets.clear()
        self.pending_replies.clear()
        await self.cancel_tasks(all_tasks)
        return len(all_tasks)

    def get_nc_messages(self):
        bot = Database.get_bot(self.bot_id)
        if bot:
            templates = Database.get_nc_templates(bot['user_id'])
            if templates:
                return [t['template'] for t in templates]
        return DEFAULT_NC_MESSAGES

    def get_spam_messages(self):
        bot = Database.get_bot(self.bot_id)
        if bot:
            templates = Database.get_spam_templates(bot['user_id'])
            if templates:
                return [t['template'] for t in templates]
        return DEFAULT_SPAM_MESSAGES

    def get_reply_messages(self):
        bot = Database.get_bot(self.bot_id)
        if bot:
            templates = Database.get_reply_templates(bot['user_id'])
            if templates:
                return [t['template'] for t in templates]
        return DEFAULT_REPLY_MESSAGES

    async def nc_loop(self, chat_id, target, context, worker_id=1):
        idx = 0
        msgs = self.get_nc_messages()
        count = 0
        while self.running:
            try:
                delay = self.delays.get(chat_id, 0)
                msg = msgs[idx % len(msgs)].format(target=target)
                await context.bot.set_chat_title(chat_id=chat_id, title=msg)
                idx += 1
                count += 1
                self.stats["sent"] += 1
                if delay > 0:
                    await asyncio.sleep(delay)
            except asyncio.CancelledError:
                break
            except RetryAfter as e:
                await asyncio.sleep(int(e.retry_after) + 0.1)
            except (TimedOut, NetworkError):
                pass
            except (BadRequest, Forbidden):
                await asyncio.sleep(0.1)
                idx += 1
            except:
                self.stats["errors"] += 1
                idx += 1

    async def custom_nc_loop(self, chat_id, custom, context, worker_id=1):
        count = 0
        while self.running:
            try:
                delay = self.delays.get(chat_id, 0)
                heart = random.choice(HEART_EMOJIS)
                await context.bot.set_chat_title(chat_id=chat_id, title=f"{custom} {heart}")
                count += 1
                self.stats["sent"] += 1
                if delay > 0:
                    await asyncio.sleep(delay)
            except asyncio.CancelledError:
                break
            except RetryAfter as e:
                await asyncio.sleep(int(e.retry_after) + 0.1)
            except (TimedOut, NetworkError):
                pass
            except (BadRequest, Forbidden):
                await asyncio.sleep(0.1)
            except:
                self.stats["errors"] += 1

    async def spam_loop(self, chat_id, target, context, worker_id):
        count = 0
        while self.running:
            try:
                delay = self.delays.get(chat_id, 0)
                msgs = self.get_spam_messages()
                msg = random.choice(msgs).format(target=target)
                await context.bot.send_message(chat_id=chat_id, text=msg)
                count += 1
                self.stats["sent"] += 1
                if delay > 0:
                    await asyncio.sleep(delay)
            except asyncio.CancelledError:
                break
            except RetryAfter as e:
                await asyncio.sleep(int(e.retry_after) + 0.1)
            except (TimedOut, NetworkError):
                pass
            except (BadRequest, Forbidden):
                await asyncio.sleep(0.1)
            except:
                self.stats["errors"] += 1

    async def reply_loop(self, chat_id, target, context):
        count = 0
        while self.running:
            try:
                delay = self.delays.get(chat_id, 0)
                if chat_id in self.pending_replies and self.pending_replies[chat_id]:
                    async with self.get_lock(chat_id):
                        msgs = self.pending_replies[chat_id].copy()
                        self.pending_replies[chat_id] = []
                    msgs_list = self.get_reply_messages()
                    for msg_id in msgs:
                        try:
                            reply = random.choice(msgs_list).format(target=target)
                            await context.bot.send_message(chat_id=chat_id, text=reply, reply_to_message_id=msg_id)
                            count += 1
                            self.stats["sent"] += 1
                            if delay > 0:
                                await asyncio.sleep(delay)
                        except asyncio.CancelledError:
                            raise
                        except:
                            pass
                else:
                    await asyncio.sleep(0.02)
            except asyncio.CancelledError:
                break

    async def msg_collector(self, update, context):
        if not update.message:
            return
        chat_id = update.effective_chat.id
        if chat_id in self.reply_targets:
            async with self.get_lock(chat_id):
                if chat_id not in self.pending_replies:
                    self.pending_replies[chat_id] = []
                self.pending_replies[chat_id].append(update.message.message_id)

    async def cmd_start(self, update, context):
        if not await self.check_auth(update):
            return
        help_text = f"""
𓆩 𝐁𝐎𝐓 #{self.bot_id} 𓆪 - ⚡ 𝐇𝐘𝐏𝐄𝐑-𝐗 𝐯𝟑.𝟎 ⚡

━━━━ 𝐀𝐓𝐓𝐀𝐂𝐊 𝐂𝐎𝐌𝐌𝐀𝐍𝐃𝐒 ━━━━
/target <name> - NC + SPAM together!
/nc <name> - Name change LOOP
/ctmnc <custom> - Custom name + heart!
/spam <target> - Spam LOOP
/reply <target> - Reply to every message!

━━━━ 𝐂𝐎𝐍𝐓𝐑𝐎𝐋 ━━━━
/delay <seconds> - Set delay (default: 0)
/threads <1-50> - Set threads

━━━━ 𝐒𝐓𝐎𝐏 ━━━━
/stopnc - Stop NC loop
/stopctmnc - Stop custom NC
/stopspam - Stop spam loop
/stopreply - Stop reply loop
/stopall - Stop ALL loops

━━━━ 𝐔𝐓𝐈𝐋𝐈𝐓𝐘 ━━━━
/ping - Bot latency
/status - Live stats
"""
        await update.message.reply_text(help_text)

    async def cmd_nc(self, update, context):
        if not await self.check_auth(update):
            return
        chat = update.effective_chat
        if chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
            await update.message.reply_text("Groups only!")
            return
        if not context.args:
            await update.message.reply_text("Usage: /nc <name>")
            return
        target = " ".join(context.args)
        chat_id = chat.id
        async with get_lock(chat_id):
            if chat_id in self.active_nc:
                await self.cancel_tasks(self.active_nc[chat_id])
            num = self.threads.get(chat_id, 1)
            tasks = [asyncio.create_task(self.nc_loop(chat_id, target, context, i+1)) for i in range(num)]
            self.active_nc[chat_id] = tasks
        await update.message.reply_text(f"[Bot #{self.bot_id}] NC started with {num} threads!")

    async def cmd_stop_nc(self, update, context):
        if not await self.check_auth(update):
            return
        chat_id = update.effective_chat.id
        async with get_lock(chat_id):
            if chat_id in self.active_nc:
                await self.cancel_tasks(self.active_nc[chat_id])
                del self.active_nc[chat_id]
                await update.message.reply_text(f"[Bot #{self.bot_id}] NC stopped!")
            else:
                await update.message.reply_text("No active NC loop!")

    async def cmd_ctmnc(self, update, context):
        if not await self.check_auth(update):
            return
        chat = update.effective_chat
        if chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
            await update.message.reply_text("Groups only!")
            return
        if not context.args:
            await update.message.reply_text("Usage: /ctmnc <custom name>")
            return
        custom = " ".join(context.args)
        chat_id = chat.id
        async with get_lock(chat_id):
            if chat_id in self.active_custom_nc:
                await self.cancel_tasks(self.active_custom_nc[chat_id])
            num = self.threads.get(chat_id, 1)
            tasks = [asyncio.create_task(self.custom_nc_loop(chat_id, custom, context, i+1)) for i in range(num)]
            self.active_custom_nc[chat_id] = tasks
        await update.message.reply_text(f"[Bot #{self.bot_id}] Custom NC started!")

    async def cmd_stop_ctmnc(self, update, context):
        if not await self.check_auth(update):
            return
        chat_id = update.effective_chat.id
        async with get_lock(chat_id):
            if chat_id in self.active_custom_nc:
                await self.cancel_tasks(self.active_custom_nc[chat_id])
                del self.active_custom_nc[chat_id]
                await update.message.reply_text(f"[Bot #{self.bot_id}] Custom NC stopped!")
            else:
                await update.message.reply_text("No active custom NC loop!")

    async def cmd_spam(self, update, context):
        if not await self.check_auth(update):
            return
        chat = update.effective_chat
        if chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
            await update.message.reply_text("Groups only!")
            return
        if not context.args:
            await update.message.reply_text("Usage: /spam <target>")
            return
        target = " ".join(context.args)
        chat_id = chat.id
        async with get_lock(chat_id):
            if chat_id in self.active_spam:
                await self.cancel_tasks(self.active_spam[chat_id])
            num = self.threads.get(chat_id, 1)
            tasks = [asyncio.create_task(self.spam_loop(chat_id, target, context, i+1)) for i in range(num)]
            self.active_spam[chat_id] = tasks
        await update.message.reply_text(f"[Bot #{self.bot_id}] Spam started with {num} threads!")

    async def cmd_stop_spam(self, update, context):
        if not await self.check_auth(update):
            return
        chat_id = update.effective_chat.id
        async with get_lock(chat_id):
            if chat_id in self.active_spam:
                await self.cancel_tasks(self.active_spam[chat_id])
                del self.active_spam[chat_id]
                await update.message.reply_text(f"[Bot #{self.bot_id}] Spam stopped!")
            else:
                await update.message.reply_text("No active spam loop!")

    async def cmd_reply(self, update, context):
        if not await self.check_auth(update):
            return
        chat = update.effective_chat
        if chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
            await update.message.reply_text("Groups only!")
            return
        if not context.args:
            await update.message.reply_text("Usage: /reply <target>")
            return
        target = " ".join(context.args)
        chat_id = chat.id
        async with get_lock(chat_id):
            if chat_id in self.active_reply:
                self.active_reply[chat_id].cancel()
            self.reply_targets[chat_id] = target
            self.pending_replies[chat_id] = []
            task = asyncio.create_task(self.reply_loop(chat_id, target, context))
            self.active_reply[chat_id] = task
        await update.message.reply_text(f"[Bot #{self.bot_id}] Reply started for {target}!")

    async def cmd_stop_reply(self, update, context):
        if not await self.check_auth(update):
            return
        chat_id = update.effective_chat.id
        async with get_lock(chat_id):
            if chat_id in self.active_reply:
                self.active_reply[chat_id].cancel()
                del self.active_reply[chat_id]
                self.reply_targets.pop(chat_id, None)
                self.pending_replies.pop(chat_id, None)
                await update.message.reply_text(f"[Bot #{self.bot_id}] Reply stopped!")
            else:
                await update.message.reply_text("No active reply loop!")

    async def cmd_target(self, update, context):
        if not await self.check_auth(update):
            return
        chat = update.effective_chat
        if chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
            await update.message.reply_text("Groups only!")
            return
        if not context.args:
            await update.message.reply_text("Usage: /target <name>")
            return
        target = " ".join(context.args)
        chat_id = chat.id
        async with get_lock(chat_id):
            if chat_id in self.active_nc:
                await self.cancel_tasks(self.active_nc[chat_id])
            if chat_id in self.active_spam:
                await self.cancel_tasks(self.active_spam[chat_id])
            num = self.threads.get(chat_id, 1)
            nc_tasks = [asyncio.create_task(self.nc_loop(chat_id, target, context, i+1)) for i in range(num)]
            spam_tasks = [asyncio.create_task(self.spam_loop(chat_id, target, context, i+1)) for i in range(num)]
            self.active_nc[chat_id] = nc_tasks
            self.active_spam[chat_id] = spam_tasks
        await update.message.reply_text(f"[Bot #{self.bot_id}] TARGET mode: NC + SPAM started!")

    async def cmd_delay(self, update, context):
        if not await self.check_auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /delay <seconds>")
            return
        try:
            delay = float(context.args[0])
            self.delays[update.effective_chat.id] = delay
            await update.message.reply_text(f"Delay set to {delay}s!")
        except ValueError:
            await update.message.reply_text("Invalid delay!")

    async def cmd_threads(self, update, context):
        if not await self.check_auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /threads <1-50>")
            return
        try:
            num = int(context.args[0])
            if num < 1 or num > 50:
                await update.message.reply_text("Threads: 1-50!")
                return
            self.threads[update.effective_chat.id] = num
            await update.message.reply_text(f"Threads set to {num}!")
        except ValueError:
            await update.message.reply_text("Invalid threads!")

    async def cmd_stopall(self, update, context):
        if not await self.check_auth(update):
            return
        count = await self.stop_all()
        await update.message.reply_text(f"[Bot #{self.bot_id}] Stopped {count} tasks!")

    async def cmd_ping(self, update, context):
        if not await self.check_auth(update):
            return
        start = time.time()
        msg = await update.message.reply_text("🏓...")
        await msg.edit_text(f"🏓 {(time.time()-start)*1000:.2f}ms")

    async def cmd_status(self, update, context):
        if not await self.check_auth(update):
            return
        up = time.time() - self.stats["start"]
        h, r = divmod(int(up), 3600)
        m, s = divmod(r, 60)
        await update.message.reply_text(f"""
📊 Bot #{self.bot_id} Stats:
✅ Sent: {self.stats['sent']}
❌ Errors: {self.stats['errors']}
⏱ Uptime: {h}h {m}m {s}s
🧵 Active: NC={len(self.active_nc)} Spam={len(self.active_spam)} Reply={len(self.active_reply)}
""")

    def build(self) -> Application:
        app = Application.builder().token(self.token).build()
        app.add_handler(CommandHandler("start", self.cmd_start))
        app.add_handler(CommandHandler("help", self.cmd_start))
        app.add_handler(CommandHandler("nc", self.cmd_nc))
        app.add_handler(CommandHandler("stopnc", self.cmd_stop_nc))
        app.add_handler(CommandHandler("ctmnc", self.cmd_ctmnc))
        app.add_handler(CommandHandler("stopctmnc", self.cmd_stop_ctmnc))
        app.add_handler(CommandHandler("spam", self.cmd_spam))
        app.add_handler(CommandHandler("stopspam", self.cmd_stop_spam))
        app.add_handler(CommandHandler("reply", self.cmd_reply))
        app.add_handler(CommandHandler("stopreply", self.cmd_stop_reply))
        app.add_handler(CommandHandler("target", self.cmd_target))
        app.add_handler(CommandHandler("delay", self.cmd_delay))
        app.add_handler(CommandHandler("threads", self.cmd_threads))
        app.add_handler(CommandHandler("stopall", self.cmd_stopall))
        app.add_handler(CommandHandler("ping", self.cmd_ping))
        app.add_handler(CommandHandler("status", self.cmd_status))
        app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, self.msg_collector))
        return app

    async def run(self):
        try:
            self.app = self.build()
            logger.info(f"[ChildBot {self.bot_id}] Starting...")
            await self.app.initialize()
            await self.app.start()
            await self.app.updater.start_polling(drop_pending_updates=True)
            Database.update_bot_status(self.bot_id, True)
            logger.info(f"[ChildBot {self.bot_id}] Running!")

            while not self.stop_event.is_set():
                await asyncio.sleep(1)

            logger.info(f"[ChildBot {self.bot_id}] Stopping...")
            self.running = False
            await self.stop_all()
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()
            Database.update_bot_status(self.bot_id, False)
        except Exception as e:
            logger.error(f"[ChildBot {self.bot_id}] Error: {e}")
            Database.update_bot_status(self.bot_id, False)


RUNNING_BOTS: Dict[int, ChildBot] = {}


class ControllerBot:
    def __init__(self):
        self.user_data: Dict[int, dict] = {}

    def get_main_menu(self):
        keyboard = [
            [InlineKeyboardButton("➕ Add Bot", callback_data="menu_addbot"),
             InlineKeyboardButton("📋 My Bots", callback_data="menu_listbots")],
            [InlineKeyboardButton("▶️ Start All", callback_data="menu_startall"),
             InlineKeyboardButton("⏹ Stop All", callback_data="menu_stopall")],
            [InlineKeyboardButton("📝 Templates", callback_data="menu_templates"),
             InlineKeyboardButton("📊 Status", callback_data="menu_status")],
            [InlineKeyboardButton("❓ Help", callback_data="menu_help")]
        ]
        return InlineKeyboardMarkup(keyboard)

    def get_templates_menu(self):
        keyboard = [
            [InlineKeyboardButton("📝 NC Templates", callback_data="tpl_nc"),
             InlineKeyboardButton("💬 Spam Templates", callback_data="tpl_spam")],
            [InlineKeyboardButton("💭 Reply Templates", callback_data="tpl_reply")],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_back")]
        ]
        return InlineKeyboardMarkup(keyboard)

    def get_bot_actions(self, bot_id: int):
        keyboard = [
            [InlineKeyboardButton("▶️ Start", callback_data=f"bot_start_{bot_id}"),
             InlineKeyboardButton("⏹ Stop", callback_data=f"bot_stop_{bot_id}")],
            [InlineKeyboardButton("ℹ️ Info", callback_data=f"bot_info_{bot_id}"),
             InlineKeyboardButton("🗑 Remove", callback_data=f"bot_remove_{bot_id}")],
            [InlineKeyboardButton("👥 Users", callback_data=f"bot_users_{bot_id}")],
            [InlineKeyboardButton("🔙 Back to Bots", callback_data="menu_listbots")]
        ]
        return InlineKeyboardMarkup(keyboard)

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        Database.register_user(user.id, user.username)

        welcome_text = f"""
╔══════════════════════════════════════════╗
║   ⚡ 𝐇𝐘𝐏𝐄𝐑-𝐗 𝐌𝐮𝐥𝐭𝐢-𝐁𝐨𝐭 𝐂𝐨𝐧𝐭𝐫𝐨𝐥𝐥𝐞𝐫 ⚡   ║
╚══════════════════════════════════════════╝

Welcome, **{user.first_name}**!

Your ID: `{user.id}`

Use the buttons below to manage your bots:
"""
        await update.message.reply_text(welcome_text, reply_markup=self.get_main_menu(), parse_mode=ParseMode.MARKDOWN)

    async def callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        user_id = query.from_user.id

        if data == "menu_back" or data == "menu_main":
            await query.edit_message_text(
                f"╔══════════════════════════════════════════╗\n"
                f"║   ⚡ 𝐇𝐘𝐏𝐄𝐑-𝐗 𝐌𝐮𝐥𝐭𝐢-𝐁𝐨𝐭 𝐂𝐨𝐧𝐭𝐫𝐨𝐥𝐥𝐞𝐫 ⚡   ║\n"
                f"╚══════════════════════════════════════════╝\n\n"
                f"Your ID: `{user_id}`\n\n"
                f"Use the buttons below:",
                reply_markup=self.get_main_menu(),
                parse_mode=ParseMode.MARKDOWN
            )

        elif data == "menu_addbot":
            context.user_data['adding_bot'] = True
            context.user_data['bot_step'] = 'token'
            await query.edit_message_text(
                "🤖 **Add New Bot**\n\n"
                "Please send me the bot token from @BotFather:\n\n"
                "Example: `123456789:ABCdefGHIjklMNOpqrsTUVwxyz`\n\n"
                "Send /cancel to cancel.",
                parse_mode=ParseMode.MARKDOWN
            )

        elif data == "menu_listbots":
            bots = Database.get_user_bots(user_id)
            if not bots:
                keyboard = [[InlineKeyboardButton("➕ Add Bot", callback_data="menu_addbot")],
                           [InlineKeyboardButton("🔙 Back", callback_data="menu_back")]]
                await query.edit_message_text(
                    "📭 You have no bots yet!\n\nClick below to add one:",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return

            text = "🤖 **Your Bots:**\n\n"
            keyboard = []
            for b in bots:
                status = "🟢" if b['id'] in RUNNING_BOTS else "🔴"
                text += f"{status} **#{b['id']}** - @{b['bot_username'] or 'Unknown'}\n"
                keyboard.append([InlineKeyboardButton(
                    f"{status} #{b['id']} - @{b['bot_username'] or 'Bot'}",
                    callback_data=f"bot_select_{b['id']}"
                )])
            keyboard.append([InlineKeyboardButton("➕ Add New Bot", callback_data="menu_addbot")])
            keyboard.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_back")])
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

        elif data.startswith("bot_select_"):
            bot_id = int(data.split("_")[2])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await query.edit_message_text("❌ Bot not found!")
                return
            status = "🟢 Running" if bot_id in RUNNING_BOTS else "🔴 Stopped"
            users = Database.get_bot_users(bot_id)
            text = f"""
📊 **Bot #{bot_id}**

👤 Username: @{bot['bot_username'] or 'Unknown'}
📛 Name: {bot['bot_name'] or 'N/A'}
📍 Status: {status}
👥 Authorized: {len(users)} users
"""
            await query.edit_message_text(text, reply_markup=self.get_bot_actions(bot_id), parse_mode=ParseMode.MARKDOWN)

        elif data.startswith("bot_start_"):
            bot_id = int(data.split("_")[2])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await query.edit_message_text("❌ Bot not found!")
                return
            if bot_id in RUNNING_BOTS:
                await query.answer("Bot is already running!", show_alert=True)
                return
            child = ChildBot(bot_id, bot['token'], bot['user_id'])
            RUNNING_BOTS[bot_id] = child
            asyncio.create_task(child.run())
            await query.answer(f"✅ Bot #{bot_id} started!", show_alert=True)
            await query.edit_message_text(
                f"✅ **Bot #{bot_id} Started!**\n\n@{bot['bot_username']} is now running.",
                reply_markup=self.get_bot_actions(bot_id),
                parse_mode=ParseMode.MARKDOWN
            )

        elif data.startswith("bot_stop_"):
            bot_id = int(data.split("_")[2])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await query.edit_message_text("❌ Bot not found!")
                return
            if bot_id not in RUNNING_BOTS:
                await query.answer("Bot is not running!", show_alert=True)
                return
            RUNNING_BOTS[bot_id].stop_event.set()
            del RUNNING_BOTS[bot_id]
            await query.answer(f"✅ Bot #{bot_id} stopped!", show_alert=True)
            await query.edit_message_text(
                f"⏹ **Bot #{bot_id} Stopped!**\n\n@{bot['bot_username']} is now offline.",
                reply_markup=self.get_bot_actions(bot_id),
                parse_mode=ParseMode.MARKDOWN
            )

        elif data.startswith("bot_info_"):
            bot_id = int(data.split("_")[2])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await query.edit_message_text("❌ Bot not found!")
                return
            status = "🟢 Running" if bot_id in RUNNING_BOTS else "🔴 Stopped"
            users = Database.get_bot_users(bot_id)
            text = f"""
📊 **Bot #{bot_id} Details**

👤 Username: @{bot['bot_username'] or 'Unknown'}
📛 Name: {bot['bot_name'] or 'N/A'}
👑 Owner ID: `{bot['user_id']}`
📅 Added: {bot['created_at']}
📍 Status: {status}
👥 Authorized Users: {len(users)}
"""
            await query.edit_message_text(text, reply_markup=self.get_bot_actions(bot_id), parse_mode=ParseMode.MARKDOWN)

        elif data.startswith("bot_remove_"):
            bot_id = int(data.split("_")[2])
            keyboard = [
                [InlineKeyboardButton("✅ Yes, Remove", callback_data=f"bot_confirm_remove_{bot_id}"),
                 InlineKeyboardButton("❌ Cancel", callback_data=f"bot_select_{bot_id}")]
            ]
            await query.edit_message_text(
                f"⚠️ **Remove Bot #{bot_id}?**\n\nThis action cannot be undone!",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )

        elif data.startswith("bot_confirm_remove_"):
            bot_id = int(data.split("_")[3])
            if bot_id in RUNNING_BOTS:
                RUNNING_BOTS[bot_id].stop_event.set()
                del RUNNING_BOTS[bot_id]
            if Database.remove_bot(user_id, bot_id):
                await query.answer("Bot removed!", show_alert=True)
                keyboard = [[InlineKeyboardButton("🔙 Back to Bots", callback_data="menu_listbots")]]
                await query.edit_message_text("✅ Bot removed successfully!", reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await query.edit_message_text("❌ Failed to remove bot!")

        elif data.startswith("bot_users_"):
            bot_id = int(data.split("_")[2])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await query.edit_message_text("❌ Bot not found!")
                return
            users = Database.get_bot_users(bot_id)
            text = f"👥 **Authorized Users for Bot #{bot_id}:**\n\n"
            text += f"👑 Owner: `{bot['user_id']}`\n\n"
            if users:
                for u in users:
                    text += f"• `{u}`\n"
            else:
                text += "_No additional users authorized_\n"
            text += f"\nTo add: `/adduser {bot_id} <user_id>`\nTo remove: `/removeuser {bot_id} <user_id>`"
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data=f"bot_select_{bot_id}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

        elif data == "menu_startall":
            bots = Database.get_user_bots(user_id)
            started = 0
            for bot in bots:
                if bot['id'] not in RUNNING_BOTS:
                    child = ChildBot(bot['id'], bot['token'], bot['user_id'])
                    RUNNING_BOTS[bot['id']] = child
                    asyncio.create_task(child.run())
                    started += 1
            
            if started > 0:
                text = f"✅ **All Bots Started!**\n\n🟢 Started {started} bot(s)"
            else:
                text = "⚠️ **All bots are already running!**"
            
            keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_back")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

        elif data == "menu_stopall":
            bots = Database.get_user_bots(user_id)
            stopped = 0
            for bot in bots:
                if bot['id'] in RUNNING_BOTS:
                    RUNNING_BOTS[bot['id']].stop_event.set()
                    del RUNNING_BOTS[bot['id']]
                    stopped += 1
            await query.answer(f"Stopped {stopped} bots!", show_alert=True)

        elif data == "menu_templates":
            nc_count = len(Database.get_nc_templates(user_id))
            spam_count = len(Database.get_spam_templates(user_id))
            reply_count = len(Database.get_reply_templates(user_id))
            text = f"📝 **Your Templates**\n\nNC Templates: {nc_count}\nSpam Templates: {spam_count}\nReply Templates: {reply_count}"
            await query.edit_message_text(text, reply_markup=self.get_templates_menu(), parse_mode=ParseMode.MARKDOWN)

        elif data == "tpl_nc":
            templates = Database.get_nc_templates(user_id)
            text = "📝 **NC Templates:**\n\n"
            if templates:
                for t in templates:
                    text += f"**#{t['id']}:** {t['template'][:40]}...\n"
            else:
                text += "_No templates. Using defaults._\n"
            text += "\nTo add: `/addnc <template>`\nTo remove: `/removenc <id/all>`"
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="menu_templates")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

        elif data == "tpl_spam":
            templates = Database.get_spam_templates(user_id)
            text = "💬 **Spam Templates:**\n\n"
            if templates:
                for t in templates:
                    text += f"**#{t['id']}:** {t['template'][:40]}...\n"
            else:
                text += "_No templates. Using defaults._\n"
            text += "\nTo add: `/addspam <template>`\nTo remove: `/removespam <id/all>`"
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="menu_templates")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

        elif data == "tpl_reply":
            templates = Database.get_reply_templates(user_id)
            text = "💭 **Reply Templates:**\n\n"
            if templates:
                for t in templates:
                    text += f"**#{t['id']}:** {t['template'][:40]}...\n"
            else:
                text += "_No templates. Using defaults._\n"
            text += "\nTo add: `/addreply <template>`\nTo remove: `/removereply <id/all>`"
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="menu_templates")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

        elif data == "menu_status":
            bots = Database.get_user_bots(user_id)
            running = sum(1 for b in bots if b['id'] in RUNNING_BOTS)
            nc_count = len(Database.get_nc_templates(user_id))
            spam_count = len(Database.get_spam_templates(user_id))
            reply_count = len(Database.get_reply_templates(user_id))
            text = f"""
📊 **Your Status**

👤 Your ID: `{user_id}`
🤖 Total Bots: {len(bots)}
🟢 Running: {running}
🔴 Stopped: {len(bots) - running}

📝 NC Templates: {nc_count}
💬 Spam Templates: {spam_count}
💭 Reply Templates: {reply_count}
"""
            keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_back")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

        elif data == "menu_help":
            help_text = """
❓ **Help & Commands**

━━ 🤖 Bot Management ━━
• Add bots via the menu
• Each bot needs an owner ID
• Only the owner controls the bot

━━ 📝 Templates ━━
`/addnc <text>` - Add NC template
`/addspam <text>` - Add spam template
Use `{target}` as placeholder

━━ 👥 User Management ━━
`/adduser <bot_id> <user_id>`
`/removeuser <bot_id> <user_id>`

━━ ⚡ Child Bot Commands ━━
`/nc <target>` - Start NC loop
`/spam <target>` - Start spam
`/ctmnc <name>` - Custom NC
`/reply <target>` - Auto reply
`/target <name>` - NC + Spam
`/delay <sec>` - Set delay
`/threads <1-50>` - Set threads
`/stopall` - Stop all tasks
"""
            keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_back")]]
            await query.edit_message_text(help_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

    async def cmd_addbot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        Database.register_user(user_id, update.effective_user.username)
        await update.message.reply_text(
            "🤖 **Add New Bot**\n\n"
            "Please send me the bot token from @BotFather:\n\n"
            "Example: `123456789:ABCdefGHIjklMNOpqrsTUVwxyz`\n\n"
            "Send /cancel to cancel.",
            parse_mode=ParseMode.MARKDOWN
        )
        return WAITING_TOKEN

    async def receive_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        token = update.message.text.strip()

        if Database.get_bot_by_token(token):
            await update.message.reply_text("❌ This bot is already registered!")
            if 'adding_bot' in context.user_data:
                context.user_data.pop('adding_bot', None)
                context.user_data.pop('bot_step', None)
            return ConversationHandler.END

        try:
            test_app = Application.builder().token(token).build()
            await test_app.initialize()
            bot_info = await test_app.bot.get_me()
            await test_app.shutdown()

            context.user_data['pending_token'] = token
            context.user_data['pending_bot_username'] = bot_info.username
            context.user_data['pending_bot_name'] = bot_info.first_name

            if 'adding_bot' in context.user_data:
                context.user_data['bot_step'] = 'owner_id'

            await update.message.reply_text(
                f"✅ **Token Valid!**\n\n"
                f"Bot: @{bot_info.username} ({bot_info.first_name})\n\n"
                f"Now send the **Owner's Telegram User ID** who will control this bot:\n\n"
                f"(Get your ID from @userinfobot on Telegram)\n\n"
                f"Send /cancel to cancel.",
                parse_mode=ParseMode.MARKDOWN
            )
            return WAITING_OWNER_ID

        except Exception as e:
            await update.message.reply_text(f"❌ Invalid token or error: {e}")
            if 'adding_bot' in context.user_data:
                context.user_data.pop('adding_bot', None)
                context.user_data.pop('bot_step', None)
            return ConversationHandler.END

    async def receive_owner_id(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            owner_id = int(update.message.text.strip())
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID! Please send a numeric Telegram user ID.")
            return WAITING_OWNER_ID

        token = context.user_data.get('pending_token')
        bot_username = context.user_data.get('pending_bot_username')
        bot_name = context.user_data.get('pending_bot_name')

        if not token:
            await update.message.reply_text("❌ Session expired. Please start again with /addbot")
            return ConversationHandler.END

        user_bots = Database.get_user_bots(owner_id)
        if len(user_bots) >= 30:
            await update.message.reply_text("❌ **Bot Limit Reached!**\n\nYou can only add maximum **30 bots** per account.\n\nPlease remove a bot first using /removebot <bot_id>")
            return ConversationHandler.END

        bot_id = Database.add_bot(owner_id, token, bot_username, bot_name)
        if bot_id:
            await update.message.reply_text(
                f"✅ **Bot Added Successfully!**\n\n"
                f"🆔 Bot ID: `{bot_id}`\n"
                f"👤 Username: @{bot_username}\n"
                f"📛 Name: {bot_name}\n"
                f"👑 Owner ID: `{owner_id}`\n\n"
                f"Only user `{owner_id}` can control this bot!\n\n"
                f"Use `/startbot {bot_id}` to start it!",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text("❌ Failed to add bot. Token might already exist.")

        context.user_data.clear()
        return ConversationHandler.END

    async def cmd_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("❌ Operation cancelled.")
        return ConversationHandler.END

    async def cmd_removebot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("Usage: /removebot <bot_id>")
            return
        try:
            bot_id = int(context.args[0])
            if bot_id in RUNNING_BOTS:
                RUNNING_BOTS[bot_id].stop_event.set()
                del RUNNING_BOTS[bot_id]
            if Database.remove_bot(user_id, bot_id):
                await update.message.reply_text(f"✅ Bot #{bot_id} removed!")
            else:
                await update.message.reply_text("❌ Bot not found or not yours!")
        except ValueError:
            await update.message.reply_text("Invalid bot ID!")

    async def cmd_listbots(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        bots = Database.get_user_bots(user_id)
        if not bots:
            await update.message.reply_text("📭 You have no bots yet. Use /addbot to add one!")
            return

        text = "🤖 **Your Bots:**\n\n"
        for b in bots:
            status = "🟢 Running" if b['id'] in RUNNING_BOTS else "🔴 Stopped"
            text += f"**#{b['id']}** - @{b['bot_username'] or 'Unknown'}\n"
            text += f"   📛 {b['bot_name'] or 'N/A'} | {status}\n\n"

        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

    async def cmd_botinfo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("Usage: /botinfo <bot_id>")
            return
        try:
            bot_id = int(context.args[0])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await update.message.reply_text("❌ Bot not found or not yours!")
                return

            status = "🟢 Running" if bot_id in RUNNING_BOTS else "🔴 Stopped"
            users = Database.get_bot_users(bot_id)

            text = f"""
📊 **Bot #{bot_id} Info**

👤 Username: @{bot['bot_username'] or 'Unknown'}
📛 Name: {bot['bot_name'] or 'N/A'}
📅 Added: {bot['created_at']}
📍 Status: {status}
👥 Authorized Users: {len(users)}
"""
            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        except ValueError:
            await update.message.reply_text("Invalid bot ID!")

    async def cmd_startbot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("Usage: /startbot <bot_id>")
            return
        try:
            bot_id = int(context.args[0])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await update.message.reply_text("❌ Bot not found or not yours!")
                return

            if bot_id in RUNNING_BOTS:
                await update.message.reply_text("⚠️ Bot is already running!")
                return

            child = ChildBot(bot_id, bot['token'], user_id)
            RUNNING_BOTS[bot_id] = child
            asyncio.create_task(child.run())

            await update.message.reply_text(f"✅ Bot #{bot_id} started!")
        except ValueError:
            await update.message.reply_text("Invalid bot ID!")

    async def cmd_stopbot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("Usage: /stopbot <bot_id>")
            return
        try:
            bot_id = int(context.args[0])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await update.message.reply_text("❌ Bot not found or not yours!")
                return

            if bot_id not in RUNNING_BOTS:
                await update.message.reply_text("⚠️ Bot is not running!")
                return

            RUNNING_BOTS[bot_id].stop_event.set()
            del RUNNING_BOTS[bot_id]

            await update.message.reply_text(f"✅ Bot #{bot_id} stopped!")
        except ValueError:
            await update.message.reply_text("Invalid bot ID!")

    async def cmd_startall(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        bots = Database.get_user_bots(user_id)
        started = 0
        for bot in bots:
            if bot['id'] not in RUNNING_BOTS:
                child = ChildBot(bot['id'], bot['token'], user_id)
                RUNNING_BOTS[bot['id']] = child
                asyncio.create_task(child.run())
                started += 1
        await update.message.reply_text(f"✅ Started {started} bots!")

    async def cmd_stopall(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        bots = Database.get_user_bots(user_id)
        stopped = 0
        for bot in bots:
            if bot['id'] in RUNNING_BOTS:
                RUNNING_BOTS[bot['id']].stop_event.set()
                del RUNNING_BOTS[bot['id']]
                stopped += 1
        await update.message.reply_text(f"✅ Stopped {stopped} bots!")

    async def cmd_addnc(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("Usage: /addnc <template>\nUse {target} as placeholder.")
            return
        template = " ".join(context.args)
        tid = Database.add_nc_template(user_id, template)
        await update.message.reply_text(f"✅ NC template #{tid} added!")

    async def cmd_listnc(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        templates = Database.get_nc_templates(user_id)
        if not templates:
            await update.message.reply_text("📭 No NC templates. Default ones will be used.")
            return
        text = "📝 **Your NC Templates:**\n\n"
        for t in templates:
            text += f"**#{t['id']}:** {t['template'][:50]}...\n"
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

    async def cmd_removenc(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("Usage: /removenc <id/all>")
            return
        arg = context.args[0].lower()
        if arg == "all":
            count = Database.remove_nc_template(user_id)
            await update.message.reply_text(f"✅ Removed {count} NC templates!")
        else:
            try:
                tid = int(arg)
                if Database.remove_nc_template(user_id, tid):
                    await update.message.reply_text(f"✅ NC template #{tid} removed!")
                else:
                    await update.message.reply_text("❌ Template not found!")
            except ValueError:
                await update.message.reply_text("Invalid ID!")

    async def cmd_addspam(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("Usage: /addspam <template>\nUse {target} as placeholder.")
            return
        template = " ".join(context.args)
        tid = Database.add_spam_template(user_id, template)
        await update.message.reply_text(f"✅ Spam template #{tid} added!")

    async def cmd_listspam(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        templates = Database.get_spam_templates(user_id)
        if not templates:
            await update.message.reply_text("📭 No spam templates. Default ones will be used.")
            return
        text = "📝 **Your Spam Templates:**\n\n"
        for t in templates:
            text += f"**#{t['id']}:** {t['template'][:50]}...\n"
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

    async def cmd_removespam(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("Usage: /removespam <id/all>")
            return
        arg = context.args[0].lower()
        if arg == "all":
            count = Database.remove_spam_template(user_id)
            await update.message.reply_text(f"✅ Removed {count} spam templates!")
        else:
            try:
                tid = int(arg)
                if Database.remove_spam_template(user_id, tid):
                    await update.message.reply_text(f"✅ Spam template #{tid} removed!")
                else:
                    await update.message.reply_text("❌ Template not found!")
            except ValueError:
                await update.message.reply_text("Invalid ID!")

    async def cmd_addreply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("Usage: /addreply <template>\nUse {target} as placeholder.")
            return
        template = " ".join(context.args)
        tid = Database.add_reply_template(user_id, template)
        await update.message.reply_text(f"✅ Reply template #{tid} added!")

    async def cmd_listreply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        templates = Database.get_reply_templates(user_id)
        if not templates:
            await update.message.reply_text("📭 No reply templates. Default ones will be used.")
            return
        text = "💬 **Your Reply Templates:**\n\n"
        for t in templates:
            text += f"**#{t['id']}:** {t['template'][:50]}...\n"
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

    async def cmd_removereply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("Usage: /removereply <id/all>")
            return
        arg = context.args[0].lower()
        if arg == "all":
            count = Database.remove_reply_template(user_id)
            await update.message.reply_text(f"✅ Removed {count} reply templates!")
        else:
            try:
                tid = int(arg)
                if Database.remove_reply_template(user_id, tid):
                    await update.message.reply_text(f"✅ Reply template #{tid} removed!")
                else:
                    await update.message.reply_text("❌ Template not found!")
            except ValueError:
                await update.message.reply_text("Invalid ID!")

    async def cmd_adduser(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /adduser <bot_id> <user_id>")
            return
        try:
            bot_id = int(context.args[0])
            target_id = int(context.args[1])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await update.message.reply_text("❌ Bot not found or not yours!")
                return
            if Database.add_bot_user(bot_id, target_id, user_id):
                await update.message.reply_text(f"✅ User {target_id} authorized for Bot #{bot_id}!")
            else:
                await update.message.reply_text("⚠️ User already authorized!")
        except ValueError:
            await update.message.reply_text("Invalid IDs!")

    async def cmd_removeuser(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /removeuser <bot_id> <user_id>")
            return
        try:
            bot_id = int(context.args[0])
            target_id = int(context.args[1])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await update.message.reply_text("❌ Bot not found or not yours!")
                return
            if Database.remove_bot_user(bot_id, target_id):
                await update.message.reply_text(f"✅ User {target_id} removed from Bot #{bot_id}!")
            else:
                await update.message.reply_text("❌ User not found!")
        except ValueError:
            await update.message.reply_text("Invalid IDs!")

    async def cmd_listusers(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("Usage: /listusers <bot_id>")
            return
        try:
            bot_id = int(context.args[0])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await update.message.reply_text("❌ Bot not found or not yours!")
                return
            users = Database.get_bot_users(bot_id)
            if not users:
                await update.message.reply_text(f"📭 No authorized users for Bot #{bot_id}")
                return
            text = f"👥 **Authorized Users for Bot #{bot_id}:**\n\n"
            for u in users:
                text += f"• `{u}`\n"
            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        except ValueError:
            await update.message.reply_text("Invalid bot ID!")

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        bots = Database.get_user_bots(user_id)
        running = sum(1 for b in bots if b['id'] in RUNNING_BOTS)

        text = f"""
📊 **Controller Status**

👤 Your ID: `{user_id}`
🤖 Your Bots: {len(bots)}
🟢 Running: {running}
🔴 Stopped: {len(bots) - running}

📝 NC Templates: {len(Database.get_nc_templates(user_id))}
📝 Spam Templates: {len(Database.get_spam_templates(user_id))}
💬 Reply Templates: {len(Database.get_reply_templates(user_id))}
"""
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

    async def handle_bot_flow_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.user_data.get('adding_bot'):
            return
        
        if context.user_data.get('bot_step') == 'token':
            await self.receive_token(update, context)
            if context.user_data.get('bot_step') == 'owner_id':
                pass
        elif context.user_data.get('bot_step') == 'owner_id':
            await self.receive_owner_id(update, context)
            context.user_data.pop('adding_bot', None)
            context.user_data.pop('bot_step', None)

    def build(self) -> Application:
        app = Application.builder().token(CONTROLLER_TOKEN).build()

        conv_handler = ConversationHandler(
            entry_points=[CommandHandler("addbot", self.cmd_addbot)],
            states={
                WAITING_TOKEN: [
                    CommandHandler("cancel", self.cmd_cancel),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_token)
                ],
                WAITING_OWNER_ID: [
                    CommandHandler("cancel", self.cmd_cancel),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_owner_id)
                ],
            },
            fallbacks=[CommandHandler("cancel", self.cmd_cancel)],
        )

        app.add_handler(conv_handler)
        app.add_handler(CommandHandler("start", self.cmd_start))
        app.add_handler(CommandHandler("help", self.cmd_start))
        app.add_handler(CallbackQueryHandler(self.callback_handler))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_bot_flow_message))
        app.add_handler(CommandHandler("removebot", self.cmd_removebot))
        app.add_handler(CommandHandler("listbots", self.cmd_listbots))
        app.add_handler(CommandHandler("botinfo", self.cmd_botinfo))
        app.add_handler(CommandHandler("startbot", self.cmd_startbot))
        app.add_handler(CommandHandler("stopbot", self.cmd_stopbot))
        app.add_handler(CommandHandler("startall", self.cmd_startall))
        app.add_handler(CommandHandler("stopall", self.cmd_stopall))
        app.add_handler(CommandHandler("addnc", self.cmd_addnc))
        app.add_handler(CommandHandler("listnc", self.cmd_listnc))
        app.add_handler(CommandHandler("removenc", self.cmd_removenc))
        app.add_handler(CommandHandler("addspam", self.cmd_addspam))
        app.add_handler(CommandHandler("listspam", self.cmd_listspam))
        app.add_handler(CommandHandler("removespam", self.cmd_removespam))
        app.add_handler(CommandHandler("addreply", self.cmd_addreply))
        app.add_handler(CommandHandler("listreply", self.cmd_listreply))
        app.add_handler(CommandHandler("removereply", self.cmd_removereply))
        app.add_handler(CommandHandler("adduser", self.cmd_adduser))
        app.add_handler(CommandHandler("removeuser", self.cmd_removeuser))
        app.add_handler(CommandHandler("listusers", self.cmd_listusers))
        app.add_handler(CommandHandler("status", self.cmd_status))

        return app


async def main():
    print("=" * 60)
    print("⚡ HYPER-X Multi-Bot Controller v3.0 ⚡")
    print("=" * 60)

    if not CONTROLLER_TOKEN:
        print("❌ ERROR: Set CONTROLLER_BOT_TOKEN environment variable!")
        return

    init_database()

    controller = ControllerBot()
    app = controller.build()

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    print("✅ Controller Bot is running!")
    print("🌐 Open for everyone - no restrictions!")

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        for bot_id, bot in list(RUNNING_BOTS.items()):
            bot.stop_event.set()
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
