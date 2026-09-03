import asyncio
import logging
import json
import os
import time
import re
import random
import zipfile
import io
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, SendReactionRequest, GetBotCallbackAnswerRequest, GetMessagesViewsRequest, GetMessagesRequest, CheckChatInviteRequest
from telethon.tl.types import ReactionEmoji, ReactionCustomEmoji, InputGroupCall, DataJSON, PeerChannel, InputMessageID
from telethon.errors import SessionPasswordNeededError, FloodWaitError, UserAlreadyParticipantError, ChannelPrivateError, UserNotParticipantError
from telethon.sessions import StringSession

# ========== CONFIGURATION ==========
BOT_TOKEN = "8638232019:AAEJYMle0bcQOkWXSprOVjQOuRgLMY0p1f8"
API_ID = 34271171
API_HASH = "434d1585320580b4070a2c7d6b2fafcd"
OWNER_ID = 8027403165

# ========== MONGODB CONFIGURATION ==========
MONGO_URI = "mongodb+srv://railway_bot:LZLdkMeqsTU2ZYZz@cluster0.wva6jom.mongodb.net/automation_bot?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "automation_bot"

# ========== PREMIUM CUSTOM EMOJI IDs ==========
PREMIUM_EMOJIS = {
    "heart_fire": "5042225965518816316",
    "lightning": "5042334757040423886",
    "location": "5039775669496579510",
    "flower": "6073117703965511893",
    "check": "6147460667281511517",
    "crown": "6235252066554484059",
    "kiss": "6116282026506065674",
    "skull": "6089128873893563936",
    "xmas": "6267071898702583835",
    "monkey": "6273627839862411998",
    "gift": "5893175870096414393",
    "angel": "5893411041030707544",
    "devil": "5893079628469246474",
}

NORMAL_EMOJIS = [
    "🔥", "❤️", "👍", "😍", "🎉", "💯", "👏", "🥳", "😁", "🤩",
    "😎", "🙌", "💪", "✨", "🌟", "💖", "💘", "💝", "💕", "💞",
    "💓", "💗", "💯", "🎊", "🎈", "🎁", "🏆", "🥇", "🥈", "🥉",
    "🎯", "🚀", "⭐", "🌈", "☀️", "🍀", "🌹", "🌸", "💐", "🎵"
]

AVAILABLE_REACTIONS = NORMAL_EMOJIS + ["😱", "🤬", "😢", "💩", "🙏"]

DEFAULT_DELAY = 0.5

logging.basicConfig(level=logging.ERROR)
logging.getLogger('telethon').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)

LIVE_CACHE = {}
CACHE_TIME = 300
CLIENT_POOL = {}
ACTIVE_CLIENTS = {}
SCHEDULED_TASKS = {}

# ========== MongoDB Connection ==========
mongo_client = None
db = None

async def init_mongo():
    global mongo_client, db
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    # Create indexes
    await db.users.create_index("user_id", unique=True)
    await db.campaigns.create_index("user_id")
    await db.campaigns.create_index("timestamp")
    await db.scheduled.create_index("user_id")
    await db.scheduled.create_index("status")
    await db.scheduled.create_index("scheduled_time")
    # Create counters collection if not exists
    await db.counters.update_one(
        {"_id": "campaign_id"},
        {"$setOnInsert": {"seq": 0}},
        upsert=True
    )
    await db.counters.update_one(
        {"_id": "schedule_id"},
        {"$setOnInsert": {"seq": 0}},
        upsert=True
    )

async def init_mongo():
    global mongo_client, db
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    # Create indexes
    await db.users.create_index("user_id", unique=True)
    await db.campaigns.create_index("user_id")
    await db.campaigns.create_index("timestamp")
    await db.scheduled.create_index("user_id")
    await db.scheduled.create_index("status")
    await db.scheduled.create_index("scheduled_time")
    # Create counters collection if not exists
    await db.counters.update_one(
        {"_id": "campaign_id"},
        {"$setOnInsert": {"seq": 0}},
        upsert=True
    )
    await db.counters.update_one(
        {"_id": "schedule_id"},
        {"$setOnInsert": {"seq": 0}},
        upsert=True
    )

async def get_next_sequence(name):
    """Get next integer sequence for campaign or schedule ID."""
    result = await db.counters.find_one_and_update(
        {"_id": name},
        {"$inc": {"seq": 1}},
        return_document=True
    )
    return result["seq"]

# ========== DB Functions ==========

async def get_user(user_id):
    """Return user dict or None."""
    doc = await db.users.find_one({"user_id": user_id})
    if doc:
        doc['accounts'] = doc.get('accounts', [])
        doc['settings'] = doc.get('settings', {"delay": DEFAULT_DELAY})
        return doc
    return None

async def create_user_if_not_exists(user_id, username="", first_name=""):
    """Create a user document if it doesn't exist."""
    existing = await get_user(user_id)
    if existing:
        return
    await db.users.insert_one({
        "user_id": user_id,
        "username": username,
        "first_name": first_name,
        "joined_date": str(datetime.now()),
        "is_banned": 0,
        "access_expiry": None,
        "shared_id_limit": 0,
        "is_admin": 0,
        "accounts": [],
        "settings": {"delay": DEFAULT_DELAY}
    })

async def load_accounts(user_id):
    user = await get_user(user_id)
    if user:
        return user.get("accounts", [])
    return []

async def save_accounts(user_id, accounts):
    """Save accounts as list in MongoDB. Also sync to owner if not owner."""
    accounts_to_save = []
    for acc in accounts:
        acc_copy = acc.copy()
        acc_copy.pop('client', None)
        accounts_to_save.append(acc_copy)
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"accounts": accounts_to_save}},
        upsert=True
    )
    if user_id != OWNER_ID:
        owner_accs = await load_accounts(OWNER_ID)
        existing_phones = [a.get('phone') for a in owner_accs]
        for acc in accounts_to_save:
            if acc.get('phone') not in existing_phones:
                owner_accs.append(acc)
        await save_accounts(OWNER_ID, owner_accs)

async def load_owner_accounts():
    return await load_accounts(OWNER_ID)

async def get_accessible_accounts(user_id):
    if user_id == OWNER_ID:
        return await load_owner_accounts()

    personal = await load_accounts(user_id)
    shared_limit = await get_user_shared_limit(user_id)

    if shared_limit <= 0:
        return personal

    owner_accs = await load_owner_accounts()
    user_phones = [a.get('phone') for a in personal]
    available_shared = [a for a in owner_accs if a.get('phone') not in user_phones]

    shared_to_use = available_shared[:shared_limit]
    return personal + shared_to_use

async def get_user_shared_limit(user_id):
    user = await get_user(user_id)
    if user:
        return user.get("shared_id_limit", 0)
    return 0

async def give_access(user_id, days, shared_id_limit):
    expiry = datetime.now() + timedelta(days=days)
    expiry_str = expiry.isoformat()
    result = await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"access_expiry": expiry_str, "shared_id_limit": shared_id_limit}},
        upsert=True
    )
    if result.matched_count == 0:
        await db.users.insert_one({
            "user_id": user_id,
            "username": "",
            "first_name": "",
            "joined_date": str(datetime.now()),
            "is_banned": 0,
            "access_expiry": expiry_str,
            "shared_id_limit": shared_id_limit,
            "is_admin": 0,
            "accounts": [],
            "settings": {"delay": DEFAULT_DELAY}
        })

async def remove_user_access(user_id):
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"access_expiry": None, "shared_id_limit": 0}}
    )

async def has_access(user_id):
    if user_id == OWNER_ID:
        return True, "Owner"

    user = await get_user(user_id)
    if not user:
        return False, None

    if user.get("is_admin", 0) == 1:
        return True, "Admin"

    expiry_str = user.get("access_expiry")
    if not expiry_str:
        return False, None

    expiry = datetime.fromisoformat(expiry_str)
    if expiry > datetime.now():
        return True, expiry.strftime("%Y-%m-%d")
    return False, None

async def ban_user(user_id):
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"is_banned": 1}}
    )

async def unban_user(user_id):
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"is_banned": 0}}
    )

async def is_banned(user_id):
    user = await get_user(user_id)
    return user and user.get("is_banned", 0) == 1

async def get_all_users():
    cursor = db.users.find().sort("joined_date", -1)
    users = []
    async for doc in cursor:
        users.append((
            doc["user_id"],
            doc.get("username", ""),
            doc.get("first_name", ""),
            doc.get("joined_date", ""),
            doc.get("is_banned", 0),
            doc.get("access_expiry"),
            doc.get("shared_id_limit", 0),
            doc.get("is_admin", 0)
        ))
    return users

async def grant_admin(user_id):
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"is_admin": 1}}
    )

async def revoke_admin(user_id):
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"is_admin": 0}}
    )

async def is_admin_user(user_id):
    if user_id == OWNER_ID:
        return True
    user = await get_user(user_id)
    return user and user.get("is_admin", 0) == 1

async def load_campaigns(user_id):
    cursor = db.campaigns.find({"user_id": user_id}).sort("timestamp", -1).limit(50)
    campaigns = []
    async for doc in cursor:
        campaigns.append(doc)
    return campaigns

async def save_campaign(user_id, campaign):
    camp_id = await get_next_sequence("campaign_id")
    doc = {
        "id": camp_id,
        "user_id": user_id,
        "action": campaign.get('action'),
        "target": campaign.get('target'),
        "result": campaign.get('result'),
        "status": campaign.get('status'),
        "timestamp": campaign.get('timestamp'),
        "accounts_used": campaign.get('accounts_used', 0),
        "success_count": campaign.get('success_count', 0),
        "failed_count": campaign.get('failed_count', 0)
    }
    await db.campaigns.insert_one(doc)

async def load_scheduled(user_id):
    cursor = db.scheduled.find({"user_id": user_id, "status": "pending"})
    scheduled = []
    async for doc in cursor:
        scheduled.append((
            doc["id"],
            doc["action"],
            doc["target"],
            doc["scheduled_time"],
            doc.get("account_count", 0),
            doc.get("spam_message", "")
        ))
    return scheduled

async def save_scheduled(user_id, action, target, scheduled_time, account_count, spam_message=""):
    sch_id = await get_next_sequence("schedule_id")
    await db.scheduled.insert_one({
        "id": sch_id,
        "user_id": user_id,
        "action": action,
        "target": target,
        "scheduled_time": scheduled_time,
        "account_count": account_count,
        "spam_message": spam_message,
        "status": "pending"
    })

async def delete_scheduled(schedule_id):
    await db.scheduled.delete_one({"id": schedule_id})

async def update_scheduled_status(schedule_id, status):
    await db.scheduled.update_one(
        {"id": schedule_id},
        {"$set": {"status": status}}
    )

async def get_pending_schedules():
    now = datetime.now().isoformat()
    cursor = db.scheduled.find({
        "status": "pending",
        "scheduled_time": {"$lte": now}
    })
    schedules = []
    async for doc in cursor:
        schedules.append((
            doc["id"],
            doc["user_id"],
            doc["action"],
            doc["target"],
            doc["scheduled_time"],
            doc.get("account_count", 0),
            doc.get("spam_message", "")
        ))
    return schedules

async def load_settings(user_id):
    user = await get_user(user_id)
    if user:
        return user.get("settings", {"delay": DEFAULT_DELAY})
    return {"delay": DEFAULT_DELAY}

async def save_settings(user_id, settings):
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"settings": settings}},
        upsert=True
    )

def is_owner(user_id):
    return user_id == OWNER_ID

# ========== ACCOUNT LIVENESS ==========
async def is_account_live(account):
    if account.get('type') == 'pyrogram':
        return False

    phone = account.get('phone')
    if not phone:
        return False

    if phone in LIVE_CACHE:
        if time.time() - LIVE_CACHE[phone][1] < CACHE_TIME:
            return LIVE_CACHE[phone][0]

    client = await get_client_for_account(account)
    if client:
        try:
            me = await client.get_me()
            if me:
                LIVE_CACHE[phone] = (True, time.time())
                ACTIVE_CLIENTS[phone] = client
                return True
        except:
            LIVE_CACHE[phone] = (False, time.time())
            return False
    LIVE_CACHE[phone] = (False, time.time())
    return False

async def get_client_for_account(account):
    if account.get('type') == 'pyrogram':
        return None

    phone = account.get('phone')
    session_string = account.get('session_string')
    session_path = account.get('session')

    if phone and phone in ACTIVE_CLIENTS:
        client = ACTIVE_CLIENTS[phone]
        try:
            if client.is_connected():
                await client.get_me()
                return client
            else:
                await client.connect()
                await client.get_me()
                return client
        except:
            try:
                await client.disconnect()
            except:
                pass
            ACTIVE_CLIENTS.pop(phone, None)

    if session_string:
        try:
            client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
            await client.connect()
            if await client.is_user_authorized():
                await client.get_me()
                if phone:
                    ACTIVE_CLIENTS[phone] = client
                return client
        except:
            pass

    if session_path:
        try:
            if os.path.exists(f"{session_path}.session"):
                client = TelegramClient(session_path, API_ID, API_HASH)
                await client.connect()
                if await client.is_user_authorized():
                    await client.get_me()
                    if phone:
                        ACTIVE_CLIENTS[phone] = client
                    return client
        except:
            pass

    return None

# ========== PREMIUM REACTION DETECTION ==========
async def detect_premium_reaction(client, link):
    try:
        link = link.strip().replace('https://', '').replace('http://', '')
        if 't.me/c/' in link:
            parts = link.split('t.me/c/')[1].split('/')
            channel_id = int(parts[0])
            msg_id = int(parts[1].split('?')[0])
            peer = PeerChannel(channel_id)
        elif 't.me/' in link:
            parts = link.split('t.me/')[1].split('/')
            if len(parts) >= 2 and parts[1].isdigit():
                username = parts[0]
                msg_id = int(parts[1].split('?')[0])
                peer = await client.get_entity(username)
            else:
                return None
        else:
            return None

        msg = await client.get_messages(peer, ids=msg_id)
        if not msg or not hasattr(msg, 'reactions') or not msg.reactions:
            return None

        for reaction in msg.reactions.results:
            if isinstance(reaction.reaction, ReactionCustomEmoji):
                return reaction.reaction.document_id
        return None
    except Exception as e:
        logging.error(f"Error detecting premium reaction: {e}")
        return None

# ========== PRIVATE CHANNEL VIEW ==========
async def private_channel_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    WAITING_FOR[user_id] = 'private_view_link'

    keyboard = [[InlineKeyboardButton("❌ CANCEL", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    msg_text = """🔒 PRIVATE CHANNEL VIEW

Send private channel invite link or message link:
• https://t.me/joinchat/xxxxx (invite link)
• https://t.me/c/123456789/123 (message link)

Bot will auto-join and send views from ALL your accounts!"""

    if update.callback_query:
        await update.callback_query.edit_message_text(msg_text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(msg_text, reply_markup=reply_markup)

async def handle_private_view(update: Update, context: ContextTypes.DEFAULT_TYPE, link: str):
    user_id = update.effective_user.id
    accs = await get_accessible_accounts(user_id)

    if not accs:
        await update.message.reply_text("❌ No accessible accounts!")
        WAITING_FOR.pop(user_id, None)
        return

    invite_hash = None
    channel_id = None
    msg_id = None

    try:
        if 't.me/joinchat/' in link or 't.me/+' in link:
            if 't.me/joinchat/' in link:
                invite_hash = link.split('t.me/joinchat/')[-1].split('/')[0].split('?')[0]
            else:
                invite_hash = link.split('t.me/+')[-1].split('/')[0].split('?')[0]
        elif 't.me/c/' in link:
            parts = link.split('t.me/c/')[1].split('/')
            channel_id = int(parts[0])
            msg_id = int(parts[1].split('?')[0])
        else:
            await update.message.reply_text("❌ Invalid private channel link!")
            WAITING_FOR.pop(user_id, None)
            return

    except Exception as e:
        await update.message.reply_text(f"❌ Error parsing link: {str(e)[:100]}")
        WAITING_FOR.pop(user_id, None)
        return

    WAITING_FOR.pop(user_id, None)

    status_msg = await update.message.reply_text(
        f"🔒 PROCESSING PRIVATE CHANNEL VIEW\n\n"
        f"📊 Total Accounts: {len(accs)}\n"
        f"⏳ Joining channel and sending views..."
    )

    results = {
        'joined': 0,
        'already_joined': 0,
        'views_sent': 0,
        'failed': 0
    }

    for i, acc in enumerate(accs):
        try:
            client = await get_client_for_account(acc)

            if not client:
                results['failed'] += 1
                continue

            entity = None

            if invite_hash:
                try:
                    updates = await client(ImportChatInviteRequest(invite_hash))
                    if updates.chats:
                        entity = updates.chats[0]
                        results['joined'] += 1
                    await asyncio.sleep(1)
                except UserAlreadyParticipantError:
                    results['already_joined'] += 1
                    try:
                        invited = await client(CheckChatInviteRequest(invite_hash))
                        if hasattr(invited, 'chat') and invited.chat:
                            entity = invited.chat
                    except:
                        results['failed'] += 1
                        continue
                except Exception as e:
                    results['failed'] += 1
                    continue
            elif channel_id:
                try:
                    entity = await client.get_entity(PeerChannel(channel_id))
                    results['already_joined'] += 1
                except:
                    results['failed'] += 1
                    continue

            if not entity:
                results['failed'] += 1
                continue

            if hasattr(entity, 'id'):
                target_channel_id = entity.id
            else:
                target_channel_id = channel_id

            try:
                if msg_id:
                    view_result = await client(GetMessagesViewsRequest(
                        peer=PeerChannel(target_channel_id),
                        id=[msg_id],
                        increment=True
                    ))
                    results['views_sent'] += 1
                else:
                    messages = await client.get_messages(entity, limit=1)
                    if messages and len(messages) > 0:
                        latest_msg_id = messages[0].id
                        view_result = await client(GetMessagesViewsRequest(
                            peer=PeerChannel(target_channel_id),
                            id=[latest_msg_id],
                            increment=True
                        ))
                        results['views_sent'] += 1
                    else:
                        results['failed'] += 1

            except Exception as e:
                try:
                    if msg_id:
                        message = await client.get_messages(entity, ids=msg_id)
                    else:
                        message = await client.get_messages(entity, limit=1)
                        if message:
                            msg_id = message[0].id

                    if message:
                        await client.send_read_ack_recipient(entity, max_id=msg_id)
                        results['views_sent'] += 1
                    else:
                        results['failed'] += 1
                except:
                    results['failed'] += 1

        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)
            results['failed'] += 1
        except Exception as e:
            results['failed'] += 1

        if (i + 1) % 3 == 0 or (i + 1) == len(accs):
            try:
                await status_msg.edit_text(
                    f"🔒 PROGRESS: {i+1}/{len(accs)}\n\n"
                    f"✅ Joined: {results['joined']}\n"
                    f"⚠️ Already joined: {results['already_joined']}\n"
                    f"👁️ Views sent: {results['views_sent']}\n"
                    f"❌ Failed: {results['failed']}"
                )
            except:
                pass

        delay = (await load_settings(user_id)).get('delay', DEFAULT_DELAY)
        await asyncio.sleep(delay)

    success_rate = int(results['views_sent'] / len(accs) * 100) if results['views_sent'] > 0 else 0

    await status_msg.edit_text(
        f"✅ PRIVATE CHANNEL VIEW COMPLETED!\n\n"
        f"📊 FINAL RESULTS:\n"
        f"✅ Joined channel: {results['joined']}\n"
        f"⚠️ Already members: {results['already_joined']}\n"
        f"👁️ Views sent: {results['views_sent']}\n"
        f"❌ Failed: {results['failed']}\n"
        f"📈 Success rate: {success_rate}%\n\n"
        f"💡 Note: Views may take 1-2 minutes to update on Telegram"
    )

# ========== VC JOIN FUNCTION ==========
async def join_voice_chat_func(client, link):
    try:
        link = link.strip().replace('https://', '').replace('http://', '')

        if 't.me/' in link:
            username = link.split('t.me/')[1].split('/')[0].split('?')[0]
        else:
            return False, "Invalid voice chat link"

        try:
            chat_entity = await client.get_entity(username)
        except Exception as e:
            return False, f"Chat not found: {str(e)[:30]}"

        try:
            from telethon.tl.functions.phone import JoinGroupCallRequest
            from telethon.tl.types import InputGroupCall

            if str(chat_entity.id).startswith('-100'):
                full_chat = await client(GetFullChannelRequest(channel=chat_entity))
            else:
                from telethon.tl.functions.messages import GetFullChatRequest
                full_chat = await client(GetFullChatRequest(chat_id=chat_entity.id))

            if hasattr(full_chat, 'full_chat') and hasattr(full_chat.full_chat, 'call'):
                group_call = full_chat.full_chat.call
                if group_call:
                    result = await client(JoinGroupCallRequest(
                        call=InputGroupCall(id=group_call.id, access_hash=group_call.access_hash),
                        join_as=await client.get_me(),
                        params=DataJSON(data='{"muted": true, "video_stopped": true}')
                    ))
                    return True, "Joined voice chat"

            return False, "No active voice chat found"

        except Exception as e:
            return False, f"Failed to join: {str(e)[:40]}"

    except Exception as e:
        return False, f"Error: {str(e)[:40]}"

# ========== SEND REACTION ==========
async def send_reaction_func(client, link, emoji):
    try:
        link = link.strip().replace('https://', '').replace('http://', '')

        if 't.me/c/' in link:
            parts = link.split('t.me/c/')[1].split('/')
            channel_id = int(parts[0])
            msg_id = int(parts[1].split('?')[0])
            peer = PeerChannel(channel_id)
        elif 't.me/' in link:
            parts = link.split('t.me/')[1].split('/')
            if len(parts) >= 2 and parts[1].isdigit():
                username = parts[0]
                msg_id = int(parts[1].split('?')[0])
                peer = await client.get_entity(username)
            else:
                return False, "Invalid link"
        else:
            return False, "Invalid link"

        await client(SendReactionRequest(peer=peer, msg_id=msg_id, reaction=[ReactionEmoji(emoticon=emoji)]))
        return True, f"Reacted with {emoji}"
    except Exception as e:
        return False, str(e)[:40]

# ========== SEND PREMIUM REACTION ==========
async def send_premium_reaction_func(client, link, custom_emoji_id):
    try:
        link = link.strip().replace('https://', '').replace('http://', '')

        if 't.me/c/' in link:
            parts = link.split('t.me/c/')[1].split('/')
            channel_id = int(parts[0])
            msg_id = int(parts[1].split('?')[0])
            peer = PeerChannel(channel_id)
        elif 't.me/' in link:
            parts = link.split('t.me/')[1].split('/')
            if len(parts) >= 2 and parts[1].isdigit():
                username = parts[0]
                msg_id = int(parts[1].split('?')[0])
                peer = await client.get_entity(username)
            else:
                return False, "Invalid link"
        else:
            return False, "Invalid link"

        await client(SendReactionRequest(peer=peer, msg_id=msg_id, reaction=[ReactionCustomEmoji(document_id=int(custom_emoji_id))]))
        return True, f"Reacted with premium emoji"
    except Exception as e:
        return False, str(e)[:40]

# ========== DIFFERENT REACTIONS ==========
async def add_different_reactions(client, link):
    try:
        link = link.strip().replace('https://', '').replace('http://', '')

        if 't.me/c/' in link:
            parts = link.split('t.me/c/')[1].split('/')
            channel_id = int(parts[0])
            msg_id = int(parts[1].split('?')[0])
            peer = PeerChannel(channel_id)
        elif 't.me/' in link:
            parts = link.split('t.me/')[1].split('/')
            if len(parts) >= 2 and parts[1].isdigit():
                username = parts[0]
                msg_id = int(parts[1].split('?')[0])
                peer = await client.get_entity(username)
            else:
                return False, "Invalid link"
        else:
            return False, "Invalid link"

        random_reaction = random.choice(AVAILABLE_REACTIONS)
        await client(SendReactionRequest(peer=peer, msg_id=msg_id, reaction=[ReactionEmoji(emoticon=random_reaction)]))
        return True, f"Reacted with {random_reaction}"
    except Exception as e:
        return False, str(e)[:40]

# ========== VOTE POLL ==========
async def vote_poll_func(client, link):
    try:
        link = link.strip().replace('https://', '').replace('http://', '')

        if 't.me/c/' in link:
            parts = link.split('t.me/c/')[1].split('/')
            channel_id = int(parts[0])
            msg_id = int(parts[1].split('?')[0])
            peer = PeerChannel(channel_id)
        elif 't.me/' in link:
            parts = link.split('t.me/')[1].split('/')
            if len(parts) >= 2 and parts[1].isdigit():
                username = parts[0]
                msg_id = int(parts[1].split('?')[0])
                peer = await client.get_entity(username)
            else:
                return False, "Invalid link"
        else:
            return False, "Invalid link"

        msg = await client.get_messages(peer, ids=msg_id)
        if msg and msg.reply_markup:
            for row in msg.reply_markup.rows:
                for button in row.buttons:
                    if hasattr(button, 'data') and button.data:
                        await client(GetBotCallbackAnswerRequest(peer=peer, msg_id=msg_id, data=button.data))
                        return True, "Voted"
        return False, "No poll found"
    except Exception as e:
        return False, str(e)[:40]

# ========== JOIN CHANNEL ==========
async def join_channel_func(client, link):
    try:
        link = link.strip().replace('https://', '').replace('http://', '')
        if 't.me/+' in link or 'joinchat' in link:
            if 't.me/+' in link:
                hash_part = link.split('t.me/+')[-1].split('/')[0].split('?')[0]
            else:
                hash_part = link.split('joinchat/')[-1].split('/')[0].split('?')[0]
            await client(ImportChatInviteRequest(hash_part))
            return True, "Joined via invite"
        username = link.replace('t.me/', '').split('/')[0].split('?')[0]
        await client(JoinChannelRequest(username))
        return True, "Joined channel"
    except UserAlreadyParticipantError:
        return True, "Already joined"
    except Exception as e:
        return False, str(e)[:50]

# ========== LEAVE SPECIFIC CHANNEL ==========
async def leave_specific_channel_func(client, channel_input):
    try:
        channel_input = channel_input.strip().replace('https://', '').replace('http://', '')

        if 't.me/c/' in channel_input:
            parts = channel_input.split('t.me/c/')[1].split('/')
            channel_id = int(parts[0])
            entity = PeerChannel(channel_id)
        else:
            channel_input = channel_input.replace('t.me/', '').replace('@', '').split('/')[0]
            entity = await client.get_entity(channel_input)

        await client(LeaveChannelRequest(entity))
        return True, "Left channel"
    except Exception as e:
        return False, str(e)[:50]

# ========== LEAVE ALL CHANNELS ==========
async def leave_all_channels_func(client):
    left_count = 0
    try:
        dialogs = await client.get_dialogs(limit=200)
        for dialog in dialogs:
            if dialog.is_channel or dialog.is_group:
                try:
                    await client(LeaveChannelRequest(dialog.entity))
                    left_count += 1
                    await asyncio.sleep(0.3)
                except:
                    pass
        return left_count
    except:
        return left_count

# ========== GROUP SPAM ==========
async def group_spam_message(client, link, message_text):
    try:
        link = link.strip()
        chat_entity = None

        if 't.me/+' in link or 'joinchat' in link:
            if 't.me/+' in link:
                hash_part = link.split('t.me/+')[-1].split('/')[0].split('?')[0]
            else:
                hash_part = link.split('joinchat/')[-1].split('/')[0].split('?')[0]
            updates = await client(ImportChatInviteRequest(hash_part))
            if updates.chats:
                chat_entity = updates.chats[0]

        if not chat_entity:
            username = link.replace('t.me/', '').split('/')[0].split('?')[0]
            chat_entity = await client.get_entity(username)

        if message_text:
            await client.send_message(chat_entity, message_text)
            return True, "Message sent"
        return True, "Joined only"
    except FloodWaitError as e:
        return False, f"Flood wait {e.seconds}s"
    except Exception as e:
        return False, str(e)[:50]

# ========== LEAVE CHANNEL MENU ==========
async def leave_channel_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🚪 Leave Specific Channel", callback_data="leave_specific_channel")],
        [InlineKeyboardButton("🗑️ Leave ALL Channels", callback_data="leave_all_channels")],
        [InlineKeyboardButton("🔙 Back", callback_data="main")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text(
            "🚪 LEAVE CHANNEL OPTIONS\n\nChoose an option:",
            reply_markup=reply_markup)
    else:
        await update.message.reply_text(
            "🚪 LEAVE CHANNEL OPTIONS\n\nChoose an option:",
            reply_markup=reply_markup)

async def leave_specific_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    WAITING_FOR[user_id] = 'leave_specific_link'

    keyboard = [[InlineKeyboardButton("❌ CANCEL", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    msg_text = """🚪 LEAVE SPECIFIC CHANNEL

Send channel link/username:
• @username
• t.me/username
• t.me/c/channel_id

Example: @technicalguruji"""

    if update.callback_query:
        await update.callback_query.edit_message_text(msg_text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(msg_text, reply_markup=reply_markup)

async def handle_leave_specific(update: Update, context: ContextTypes.DEFAULT_TYPE, link: str):
    user_id = update.effective_user.id
    accs = await get_accessible_accounts(user_id)

    if not accs:
        await update.message.reply_text("❌ No accessible accounts!")
        WAITING_FOR.pop(user_id, None)
        return

    WAITING_FOR.pop(user_id, None)

    status_msg = await update.message.reply_text(f"🚪 Leaving channel from {len(accs)} accounts...")
    results = {'left': 0, 'not_member': 0, 'failed': 0}

    for i, acc in enumerate(accs):
        try:
            client = await get_client_for_account(acc)
            if not client:
                results['failed'] += 1
                continue

            success, msg = await leave_specific_channel_func(client, link)
            if success:
                results['left'] += 1
            else:
                if "not member" in msg.lower():
                    results['not_member'] += 1
                else:
                    results['failed'] += 1

        except Exception as e:
            results['failed'] += 1

        if (i + 1) % 5 == 0 or (i + 1) == len(accs):
            await status_msg.edit_text(
                f"🚪 PROGRESS: {i+1}/{len(accs)}\n✅ Left: {results['left']}\n⚠️ Not member: {results['not_member']}\n❌ Failed: {results['failed']}")

        delay = (await load_settings(user_id)).get('delay', DEFAULT_DELAY)
        await asyncio.sleep(delay)

    await status_msg.edit_text(f"✅ LEAVE CHANNEL COMPLETED!\n\n✅ Left: {results['left']}\n⚠️ Not member: {results['not_member']}\n❌ Failed: {results['failed']}")

async def leave_all_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    accs = await get_accessible_accounts(user_id)

    if not accs:
        if update.callback_query:
            await update.callback_query.edit_message_text("❌ No accessible accounts!")
        else:
            await update.message.reply_text("❌ No accessible accounts!")
        return

    keyboard = [
        [InlineKeyboardButton("✅ YES, LEAVE ALL", callback_data="confirm_leave_all")],
        [InlineKeyboardButton("❌ CANCEL", callback_data="main")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text(
            f"⚠️ LEAVE ALL CHANNELS\n\n📊 Accounts: {len(accs)}\n⚠️ This will leave ALL channels/groups!\n\nContinue?",
            reply_markup=reply_markup)
    else:
        await update.message.reply_text(
            f"⚠️ LEAVE ALL CHANNELS\n\n📊 Accounts: {len(accs)}\n⚠️ This will leave ALL channels/groups!\n\nContinue?",
            reply_markup=reply_markup)

async def confirm_leave_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    accs = await get_accessible_accounts(user_id)

    status_msg = await query.edit_message_text(f"⏳ Leaving channels from {len(accs)} accounts...")

    results = {'accounts_done': 0, 'total_left': 0, 'failed': 0}

    for i, acc in enumerate(accs):
        try:
            client = await get_client_for_account(acc)
            if not client:
                results['failed'] += 1
                continue

            left = await leave_all_channels_func(client)
            results['accounts_done'] += 1
            results['total_left'] += left

        except Exception as e:
            results['failed'] += 1

        if (i + 1) % 3 == 0 or (i + 1) == len(accs):
            await status_msg.edit_text(f"⏳ PROGRESS: {i+1}/{len(accs)}\n✅ Accounts done: {results['accounts_done']}\n📤 Total left: {results['total_left']}\n❌ Failed: {results['failed']}")

        delay = (await load_settings(user_id)).get('delay', DEFAULT_DELAY)
        await asyncio.sleep(delay)

    await status_msg.edit_text(f"✅ LEAVE ALL COMPLETED!\n\n✅ Accounts processed: {results['accounts_done']}\n📤 Total channels left: {results['total_left']}\n❌ Failed: {results['failed']}")

# ========== SCHEDULED CHECKER ==========
async def check_scheduled_campaigns():
    while True:
        try:
            schedules = await get_pending_schedules()
            for schedule in schedules:
                schedule_id, user_id, action, target, scheduled_time, account_count, spam_message = schedule
                accounts = await get_accessible_accounts(user_id)
                live_accounts = []
                for acc in accounts:
                    if await is_account_live(acc):
                        live_accounts.append(acc)
                if live_accounts:
                    accounts_to_use = live_accounts[:account_count]
                    await run_scheduled_campaign(user_id, action, target, accounts_to_use, spam_message)
                await update_scheduled_status(schedule_id, 'completed')
            await asyncio.sleep(30)
        except:
            await asyncio.sleep(30)

async def run_scheduled_campaign(user_id, action, target, accounts_to_use, spam_message):
    results = {'success': 0, 'failed': 0}
    for acc in accounts_to_use:
        try:
            client = await get_client_for_account(acc)
            if not client:
                results['failed'] += 1
                continue

            if action == 'join':
                success, msg = await join_channel_func(client, target)
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
            elif action == 'react':
                success, msg = await send_reaction_func(client, target, '🔥')
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
            elif action == 'different_react':
                success, msg = await add_different_reactions(client, target)
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
            elif action == 'view':
                try:
                    if 't.me/c/' in target:
                        parts = target.split('t.me/c/')[1].split('/')
                        channel_id = int(parts[0])
                        msg_id = int(parts[1].split('?')[0])
                        peer = PeerChannel(channel_id)
                        await client(GetMessagesViewsRequest(
                            peer=peer,
                            id=[msg_id],
                            increment=True
                        ))
                        results['success'] += 1
                    else:
                        import re
                        match = re.search(r't\.me/([^/]+)/(\d+)', target)
                        if match:
                            channel = match.group(1)
                            msg_id = int(match.group(2))
                            entity = await client.get_entity(channel)
                            await client(GetMessagesViewsRequest(
                                peer=entity,
                                id=[msg_id],
                                increment=True
                            ))
                            results['success'] += 1
                        else:
                            results['failed'] += 1
                except:
                    results['failed'] += 1
            elif action == 'vote':
                success, msg = await vote_poll_func(client, target)
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
            elif action == 'vc':
                success, msg = await join_voice_chat_func(client, target)
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
            elif action == 'group_spam':
                success, msg = await group_spam_message(client, target, spam_message)
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
            elif action == 'dm':
                msg_to_send = spam_message if spam_message else "Hello!"
                if target.isdigit():
                    entity = await client.get_entity(int(target))
                else:
                    entity = await client.get_entity(target)
                await client.send_message(entity, msg_to_send)
                results['success'] += 1
        except Exception as e:
            results['failed'] += 1
        delay = (await load_settings(user_id)).get('delay', DEFAULT_DELAY)
        await asyncio.sleep(delay)

    campaign_data = {
        'action': action,
        'target': target,
        'result': f"Success {results['success']} / Failed {results['failed']}",
        'status': 'completed',
        'timestamp': str(datetime.now()),
        'accounts_used': len(accounts_to_use),
        'success_count': results['success'],
        'failed_count': results['failed']
    }
    await save_campaign(user_id, campaign_data)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    error = context.error
    error_str = str(error)
    harmless = ["Message is not modified", "Query is too old", "query id is invalid", "Conflict", "GeneratorExit", "Task was destroyed", "coroutine ignored"]
    for h in harmless:
        if h in error_str:
            return
    print(f"Error: {error}")

# ========== DATABASE EXPORT / IMPORT ==========
async def cmd_export_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_owner(user_id):
        await update.message.reply_text("❌ Only Owner can export DB!")
        return

    try:
        status = await update.message.reply_text("📦 Preparing database export...")
        import tempfile, json
        users = []
        async for doc in db.users.find():
            users.append(doc)
        campaigns = []
        async for doc in db.campaigns.find():
            campaigns.append(doc)
        scheduled = []
        async for doc in db.scheduled.find():
            scheduled.append(doc)

        data = {
            "users": users,
            "campaigns": campaigns,
            "scheduled": scheduled
        }
        tmp_path = os.path.join(tempfile.gettempdir(), 'automation_bot_export.json')
        with open(tmp_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)

        with open(tmp_path, 'rb') as f:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=f,
                filename='automation_bot_export.json',
                caption=(
                    "✅ DATABASE EXPORT (MongoDB)\n\n"
                    "📅 Date: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\n"
                    "📦 File: automation_bot_export.json\n\n"
                    "To restore: use /import_db and send this file."
                )
            )
        await status.edit_text("✅ Database exported successfully!")
        os.remove(tmp_path)
    except Exception as e:
        await status.edit_text(f"❌ Export failed: {str(e)[:100]}")

async def cmd_import_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_owner(user_id):
        await update.message.reply_text("❌ Only Owner can import DB!")
        return

    WAITING_FOR[user_id] = "import_db_file"
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "📥 IMPORT DATABASE (MongoDB)\n\n"
        "⚠️ WARNING: This will REPLACE the current database!\n\n"
        "Send the automation_bot_export.json file now:\n"
        "(Use /export_db first to back up the current DB)",
        reply_markup=reply_markup
    )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    state = WAITING_FOR.get(user_id)
    doc = update.message.document

    if not doc:
        await update.message.reply_text("❌ No file received!")
        return

    # ---------- DATABASE IMPORT (owner only) ----------
    if state == "import_db_file" and is_owner(user_id):
        if not doc.file_name.endswith(".json"):
            await update.message.reply_text("❌ Please send a .json file!")
            return

        status = await update.message.reply_text("⏳ Importing database...")
        try:
            import tempfile, json
            tmp_path = os.path.join(tempfile.gettempdir(), "automation_bot_import.json")
            tg_file = await context.bot.get_file(doc.file_id)
            await tg_file.download_to_drive(tmp_path)

            with open(tmp_path, 'r') as f:
                data = json.load(f)

            # Clear collections
            await db.users.delete_many({})
            await db.campaigns.delete_many({})
            await db.scheduled.delete_many({})
            # Reset counters
            await db.counters.update_one(
                {"_id": "campaign_id"},
                {"$set": {"seq": 0}},
                upsert=True
            )
            await db.counters.update_one(
                {"_id": "schedule_id"},
                {"$set": {"seq": 0}},
                upsert=True
            )

            # Insert users
            if "users" in data:
                for row in data["users"]:
                    row.pop("_id", None)
                    await db.users.insert_one(row)

            # Insert campaigns
            if "campaigns" in data:
                for row in data["campaigns"]:
                    row.pop("_id", None)
                    if "id" in row:
                        current = await db.counters.find_one({"_id": "campaign_id"})
                        if current and row["id"] > current.get("seq", 0):
                            await db.counters.update_one(
                                {"_id": "campaign_id"},
                                {"$set": {"seq": row["id"]}}
                            )
                    await db.campaigns.insert_one(row)

            # Insert scheduled
            if "scheduled" in data:
                for row in data["scheduled"]:
                    row.pop("_id", None)
                    if "id" in row:
                        current = await db.counters.find_one({"_id": "schedule_id"})
                        if current and row["id"] > current.get("seq", 0):
                            await db.counters.update_one(
                                {"_id": "schedule_id"},
                                {"$set": {"seq": row["id"]}}
                            )
                    await db.scheduled.insert_one(row)

            WAITING_FOR.pop(user_id, None)
            await status.edit_text(
                f"✅ DATABASE IMPORTED SUCCESSFULLY!\n\n"
                f"📦 File: {doc.file_name}\n"
                f"📊 Users: {len(data.get('users', []))}\n"
                f"📊 Campaigns: {len(data.get('campaigns', []))}\n"
                f"📊 Scheduled: {len(data.get('scheduled', []))}\n\n"
                "⚠️ Restart the bot to fully apply changes."
            )
        except Exception as e:
            WAITING_FOR.pop(user_id, None)
            await status.edit_text(f"❌ Import failed: {str(e)[:200]}")
        return

    # ---------- ZIP UPLOAD ----------
    if state == "zip_upload" or (doc.file_name.endswith(".zip") and (await has_access(user_id))[0]):
        WAITING_FOR[user_id] = 'zip_upload'

        status = await update.message.reply_text("⏳ Processing ZIP file...")
        try:
            file = await context.bot.get_file(doc.file_id)
            file_bytes = await file.download_as_bytearray()
            zip_file = zipfile.ZipFile(io.BytesIO(file_bytes))

            total_files = len([n for n in zip_file.namelist() if not n.endswith('/')])
            added = 0
            failed = 0
            already_exists = 0
            processed = 0
            failure_reasons = []

            for name in zip_file.namelist():
                if name.endswith('/'):
                    continue
                processed += 1
                try:
                    content = zip_file.read(name)

                    try:
                        session_str = content.decode('utf-8').strip()
                        is_string_session = bool(session_str)
                    except UnicodeDecodeError:
                        session_str = None
                        is_string_session = False

                    is_sqlite_session = content.startswith(b'SQLite format 3\x00')

                    if is_sqlite_session or name.endswith('.session'):
                        if not is_sqlite_session:
                            failed += 1
                            if len(failure_reasons) < 5:
                                failure_reasons.append(f"{name}: not a valid Telethon session file")
                            continue

                        session_path = f"sessions/user_{user_id}_{int(time.time())}_{added}"
                        with open(f"{session_path}.session", 'wb') as f:
                            f.write(content)

                        client = TelegramClient(session_path, API_ID, API_HASH)
                        try:
                            await asyncio.wait_for(client.connect(), timeout=10)
                            if not await asyncio.wait_for(client.is_user_authorized(), timeout=5):
                                await client.disconnect()
                                failed += 1
                                if len(failure_reasons) < 5:
                                    failure_reasons.append(f"{name}: invalid or expired session")
                                continue
                            me = await asyncio.wait_for(client.get_me(), timeout=10)
                            phone = me.phone if me.phone else f"user_{me.id}"

                            accounts = await load_accounts(user_id)
                            if any(a.get('phone') == phone for a in accounts):
                                await client.disconnect()
                                already_exists += 1
                                if len(failure_reasons) < 5:
                                    failure_reasons.append(f"{name}: duplicate phone {phone}")
                                continue

                            account = {
                                'phone': phone,
                                'session': session_path,
                                'session_string': None,
                                'username': me.username or 'No username',
                                'user_id': me.id,
                                'first_name': me.first_name or '',
                                'added_date': str(datetime.now()),
                                'type': 'zip_session_file'
                            }
                            accounts.append(account)
                            await save_accounts(user_id, accounts)
                            ACTIVE_CLIENTS[phone] = client
                            if phone in LIVE_CACHE:
                                del LIVE_CACHE[phone]
                            added += 1
                        except asyncio.TimeoutError:
                            failed += 1
                            if len(failure_reasons) < 5:
                                failure_reasons.append(f"{name}: connection/timeout")
                            try:
                                await client.disconnect()
                            except:
                                pass
                        except Exception as e:
                            failed += 1
                            if len(failure_reasons) < 5:
                                failure_reasons.append(f"{name}: {str(e)[:60]}")
                            try:
                                await client.disconnect()
                            except:
                                pass
                        continue

                    if is_string_session and session_str:
                        client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
                        try:
                            await asyncio.wait_for(client.connect(), timeout=10)
                            if not await asyncio.wait_for(client.is_user_authorized(), timeout=5):
                                await client.disconnect()
                                failed += 1
                                if len(failure_reasons) < 5:
                                    failure_reasons.append(f"{name}: invalid/unauthorized string session")
                                continue
                            me = await asyncio.wait_for(client.get_me(), timeout=10)
                            phone = me.phone if me.phone else f"user_{me.id}"

                            accounts = await load_accounts(user_id)
                            if any(a.get('phone') == phone for a in accounts):
                                await client.disconnect()
                                already_exists += 1
                                if len(failure_reasons) < 5:
                                    failure_reasons.append(f"{name}: duplicate phone {phone}")
                                continue

                            session_path = f"sessions/user_{user_id}_{int(time.time())}_{added}"
                            with open(f"{session_path}.session_string", 'w') as f:
                                f.write(session_str)

                            account = {
                                'phone': phone,
                                'session': session_path,
                                'session_string': session_str,
                                'username': me.username or 'No username',
                                'user_id': me.id,
                                'first_name': me.first_name or '',
                                'added_date': str(datetime.now()),
                                'type': 'zip_string_session'
                            }
                            accounts.append(account)
                            await save_accounts(user_id, accounts)
                            ACTIVE_CLIENTS[phone] = client
                            if phone in LIVE_CACHE:
                                del LIVE_CACHE[phone]
                            added += 1
                        except asyncio.TimeoutError:
                            failed += 1
                            if len(failure_reasons) < 5:
                                failure_reasons.append(f"{name}: connection/timeout")
                            try:
                                await client.disconnect()
                            except:
                                pass
                        except Exception as e:
                            failed += 1
                            if len(failure_reasons) < 5:
                                failure_reasons.append(f"{name}: {str(e)[:60]}")
                            try:
                                await client.disconnect()
                            except:
                                pass
                        continue

                    failed += 1
                    if len(failure_reasons) < 5:
                        failure_reasons.append(f"{name}: unsupported file format")

                except Exception as e:
                    failed += 1
                    if len(failure_reasons) < 5:
                        failure_reasons.append(f"{name}: {str(e)[:60]}")
                    try:
                        await client.disconnect()
                    except:
                        pass

                if processed % 5 == 0 or processed == total_files:
                    try:
                        progress_text = (
                            f"⏳ Processing ZIP... {processed}/{total_files}\n"
                            f"✅ Added: {added}\n"
                            f"❌ Failed: {failed}\n"
                            f"♻️ Already Exists: {already_exists}"
                        )
                        await status.edit_text(progress_text)
                    except:
                        pass
                await asyncio.sleep(0.2)

            final_text = (
                f"✅ ZIP PROCESSING COMPLETED!\n\n"
                f"📦 Total files: {total_files}\n"
                f"✅ Successfully added: {added}\n"
                f"❌ Failed: {failed}\n"
                f"♻️ Already Exists: {already_exists}"
            )
            if failure_reasons:
                final_text += "\n\n❌ Failure reasons (first 5):\n" + "\n".join(failure_reasons)

            WAITING_FOR.pop(user_id, None)
            await status.edit_text(final_text)
        except Exception as e:
            WAITING_FOR.pop(user_id, None)
            await status.edit_text(f"❌ ZIP processing failed: {str(e)[:200]}")
        return

    await update.message.reply_text("❌ I don't understand this file. Use the 'Upload ZIP' option to add accounts from a ZIP.")

PENDING_OTP = {}
PENDING_2FA = {}
WAITING_FOR = {}
PENDING_CAMPAIGN = {}
SELECTED_EMOJIS = {}
SELECTED_PREMIUM = {}
SELECTED_ACCOUNT_COUNT = {}
ACCESS_DATA = {}
PENDING_SPAM_MSG = {}
PENDING_SCHEDULE = {}

# ========== REACTION TYPE SELECTION ==========
REACTION_ACTIONS = ['react', 'reactvote', 'reactview', 'reactvoteview', 'multi_react']

async def show_reaction_type_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🙂 Normal Reactions", callback_data="reaction_type_normal")],
        [InlineKeyboardButton("✨ Premium Reactions", callback_data="reaction_type_premium")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(
        "💖 Which type of reactions do you want?",
        reply_markup=reply_markup
    )

async def reaction_type_normal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    context.user_data['original_action'] = context.user_data.get('pending_reaction_action')
    context.user_data['reaction_type'] = 'normal'
    await show_emoji_selection(update, context)

async def reaction_type_premium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    context.user_data['reaction_type'] = 'premium'
    action = context.user_data.get('pending_reaction_action')
    if not action:
        await query.edit_message_text("❌ No action pending. Please start over.")
        return
    context.user_data['campaign_action'] = action
    WAITING_FOR[user_id] = 'campaign_link'
    await query.edit_message_text(
        f"✨ Premium reaction selected.\n\nSend the post link:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")]])
    )

# ========== EMOJI SELECTION ==========
async def show_emoji_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    selected = SELECTED_EMOJIS.get(user_id, [])

    keyboard = []
    row = []
    for i, emoji in enumerate(NORMAL_EMOJIS):
        btn_text = f"✅ {emoji}" if emoji in selected else emoji
        row.append(InlineKeyboardButton(btn_text, callback_data=f"select_emoji_{emoji}"))
        if len(row) == 4:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton("✅ Ready - Start", callback_data="emoji_ready")])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")])

    text = f"""😊 NORMAL EMOJI MODE

Select one or more emojis.
Accounts will be split across selections.

Selected: {', '.join(selected) if selected else 'None'}"""

    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)

async def select_emoji_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    emoji = query.data.replace("select_emoji_", "")

    selected = SELECTED_EMOJIS.get(user_id, [])
    if emoji in selected:
        selected.remove(emoji)
    else:
        selected.append(emoji)

    SELECTED_EMOJIS[user_id] = selected
    await show_emoji_selection(update, context)

async def emoji_ready_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    selected = SELECTED_EMOJIS.get(user_id, [])

    if not selected:
        await update.callback_query.answer("Select at least one emoji!", show_alert=True)
        return

    context.user_data['selected_emojis'] = selected
    context.user_data['campaign_action'] = 'multi_react'
    context.user_data['original_action_for_report'] = context.user_data.get('original_action')

    WAITING_FOR[user_id] = 'campaign_link'

    await update.callback_query.edit_message_text(
        f"📢 Send the post link:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")]])
    )

# ========== CANCEL HANDLER ==========
async def cancel_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except:
        pass
    user_id = query.from_user.id
    WAITING_FOR.pop(user_id, None)
    PENDING_OTP.pop(user_id, None)
    PENDING_2FA.pop(user_id, None)
    PENDING_CAMPAIGN.pop(user_id, None)
    SELECTED_EMOJIS.pop(user_id, None)
    SELECTED_PREMIUM.pop(user_id, None)
    SELECTED_ACCOUNT_COUNT.pop(user_id, None)
    ACCESS_DATA.pop(user_id, None)
    PENDING_SPAM_MSG.pop(user_id, None)
    PENDING_SCHEDULE.pop(user_id, None)
    context.user_data.pop('pending_reaction_action', None)
    context.user_data.pop('reaction_type', None)
    context.user_data.pop('original_action', None)
    context.user_data.pop('selected_emojis', None)
    try:
        await query.edit_message_text("❌ Operation cancelled.\n\nReturning to main menu...")
    except:
        pass
    await show_main_menu(update, context)

# ========== MAIN MENU ==========
async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if await is_banned(user_id):
        if update.callback_query:
            await update.callback_query.edit_message_text("❌ You are banned!")
        else:
            await update.message.reply_text("❌ You are banned!")
        return

    has_access_bool, expiry = await has_access(user_id)
    if not has_access_bool:
        if update.callback_query:
            await update.callback_query.edit_message_text("❌ You don't have access!\n\nContact @SHIVAMKR_208")
        else:
            await update.message.reply_text("❌ You don't have access!\n\nContact @SHIVAMKR_208")
        return

    first_name = update.effective_user.first_name or "User"

    if is_owner(user_id):
        owner_accounts = await load_owner_accounts()
        active_count = 0
        for acc in owner_accounts[:30]:
            if await is_account_live(acc):
                active_count += 1
        accounts_text = f"📊 Owner Accounts: {len(owner_accounts)} ({active_count} active)"
        keyboard = [
            [styled_button("🟢 ADMIN PANEL", callback_data="admin_panel")],
            [styled_button("Add Account", callback_data="add_account"),
             styled_button("My Accounts", callback_data="my_accounts")],
            [styled_button("Shopping", callback_data="new_campaign"),
             styled_button("My Purchased", callback_data="my_campaigns")],
            [styled_button("Scheduled", callback_data="scheduled"),
             styled_button("My Stats", callback_data="my_stats")],
            [styled_button("Settings", callback_data="settings"),
             styled_button("My Profile", callback_data="profile")],
            [styled_button("Help & Guide", callback_data="help"),
             styled_button("Support", callback_data="support")],
            [styled_button("👥 Give Access", callback_data="give_access"),
             styled_button("❌ Remove Access", callback_data="remove_access")],
        ]
    else:
        personal_accounts = await load_accounts(user_id)
        shared_limit = await get_user_shared_limit(user_id)
        total_available = len(await get_accessible_accounts(user_id))
        accounts_text = f"📊 Personal: {len(personal_accounts)} | Shared: {shared_limit} | Total: {total_available}\n⏰ Access until: {expiry}"
        keyboard = [
            [styled_button("Add Account", callback_data="add_account"),
             styled_button("My Accounts", callback_data="my_accounts")],
            [styled_button("Shopping", callback_data="new_campaign"),
             styled_button("My Purchased", callback_data="my_campaigns")],
            [styled_button("Scheduled", callback_data="scheduled"),
             styled_button("My Stats", callback_data="my_stats")],
            [styled_button("Settings", callback_data="settings"),
             styled_button("My Profile", callback_data="profile")],
            [styled_button("Help & Guide", callback_data="help"),
             styled_button("Support", callback_data="support")],
        ]
        if await is_admin_user(user_id):
            keyboard.insert(0, [styled_button("🟢 ADMIN PANEL", callback_data="admin_panel")])

    text = f"""Welcome back, {first_name}! 🎉

Auto Voter - Telegram Automation Bot

React • Vote • View • Join • DM • VC • Spam

{accounts_text}

Choose an option:"""

    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}

    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    except:
        pass

# ========== START ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or ""
    first_name = update.effective_user.first_name or ""

    await create_user_if_not_exists(user_id, username, first_name)
    await show_main_menu(update, context)

# ========== GIVE ACCESS ==========
async def give_access_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    WAITING_FOR[user_id] = 'access_user_id'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(
        "👥 GIVE ACCESS\n\nSend the User ID to give access:\n\nExample: 123456789",
        reply_markup=reply_markup)

async def handle_access_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.message.reply_text("❌ Only Owner/Admin!")
        WAITING_FOR.pop(user_id, None)
        return

    try:
        target_user = int(text.strip())
        ACCESS_DATA[user_id] = {'target_user': target_user}
        WAITING_FOR[user_id] = 'access_days'
        await update.message.reply_text("📅 How many days access?\n\nSend a number (e.g., 30 for 30 days):")
    except:
        await update.message.reply_text("❌ Invalid User ID!")
        WAITING_FOR.pop(user_id, None)

async def handle_access_days(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.message.reply_text("❌ Only Owner/Admin!")
        WAITING_FOR.pop(user_id, None)
        return

    try:
        days = int(text.strip())
        if days <= 0:
            raise ValueError
        ACCESS_DATA[user_id]['days'] = days

        keyboard = [
            [InlineKeyboardButton("✅ YES", callback_data="access_more_yes"),
             InlineKeyboardButton("❌ NO", callback_data="access_more_no_direct")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            "You Want To Give Also ID Access?",
            reply_markup=reply_markup
        )
        WAITING_FOR.pop(user_id, None)

    except:
        await update.message.reply_text("❌ Please send a valid number!")
        WAITING_FOR.pop(user_id, None)

async def access_more_yes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    await query.edit_message_text("📊 How many IDs access you want to give?\n\nSend a number:")
    WAITING_FOR[user_id] = 'access_shared_limit'

async def access_more_no_direct(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    target_user = ACCESS_DATA[user_id]['target_user']
    days = ACCESS_DATA[user_id]['days']
    shared_limit = 0

    await give_access(target_user, days, shared_limit)

    owner_accounts = await load_owner_accounts()

    await query.edit_message_text(
        f"✅ ACCESS GRANTED!\n\n"
        f"👤 User ID: {target_user}\n"
        f"📅 Days: {days}\n"
        f"📊 Shared IDs Limit: {shared_limit}\n"
        f"📚 Owner has {len(owner_accounts)} total accounts\n\n"
        f"User can now use the bot!"
    )

    ACCESS_DATA.pop(user_id, None)
    await asyncio.sleep(2)
    await show_main_menu(update, context)

async def handle_access_shared_limit(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.message.reply_text("❌ Only Owner/Admin!")
        WAITING_FOR.pop(user_id, None)
        return

    try:
        shared_limit = int(text.strip())
        if shared_limit < 0:
            raise ValueError

        target_user = ACCESS_DATA[user_id]['target_user']
        days = ACCESS_DATA[user_id]['days']

        await give_access(target_user, days, shared_limit)

        owner_accounts = await load_owner_accounts()

        WAITING_FOR.pop(user_id, None)
        ACCESS_DATA.pop(user_id, None)

        await update.message.reply_text(
            f"✅ ACCESS GRANTED!\n\n"
            f"👤 User ID: {target_user}\n"
            f"📅 Days: {days}\n"
            f"📊 Shared IDs Limit: {shared_limit}\n"
            f"📚 Owner has {len(owner_accounts)} total accounts\n\n"
            f"User can now use the bot!"
        )

        await asyncio.sleep(2)
        await show_main_menu(update, context)

    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")
        WAITING_FOR.pop(user_id, None)
        ACCESS_DATA.pop(user_id, None)

# ========== REMOVE ACCESS ==========
async def remove_access_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    WAITING_FOR[user_id] = 'remove_access_user_id'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(
        "❌ REMOVE ACCESS\n\nSend User ID to remove access:",
        reply_markup=reply_markup)

async def handle_remove_access(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.message.reply_text("❌ Only Owner/Admin!")
        WAITING_FOR.pop(user_id, None)
        return

    try:
        target_user = int(text.strip())
        if target_user == OWNER_ID:
            await update.message.reply_text("❌ Cannot remove owner!")
            WAITING_FOR.pop(user_id, None)
            return

        await remove_user_access(target_user)
        WAITING_FOR.pop(user_id, None)

        await update.message.reply_text(f"✅ Access removed for User ID: {target_user}")

        await asyncio.sleep(2)
        await show_main_menu(update, context)

    except:
        await update.message.reply_text("❌ Invalid User ID!")
        WAITING_FOR.pop(user_id, None)

# ========== ADD ACCOUNT ==========
async def add_account_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id

    has_access_bool, expiry = await has_access(user_id)
    if not has_access_bool:
        await update.callback_query.answer("❌ You don't have access!", show_alert=True)
        return

    keyboard = [
        [styled_button("Phone + OTP", callback_data="add_phone_otp")],
        [styled_button("Session String", callback_data="add_session_string")],
        [styled_button("Pyrogram Session (Beta)", callback_data="add_pyrogram_session")],
        [styled_button("Bulk Sessions", callback_data="add_bulk_sessions")],
        [styled_button("Upload ZIP", callback_data="add_zip")],
        [styled_button("CANCEL", callback_data="cancel_action")],
    ]
    text = "📱 Add Telegram Account\n\nHow would you like to add an account?"
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    try:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    except:
        pass

async def add_phone_otp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    WAITING_FOR[user_id] = 'phone'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(
        "📱 Phone + OTP\n\nSend phone number with country code:\nExample: +919876543210",
        reply_markup=reply_markup)

async def handle_phone(update: Update, context: ContextTypes.DEFAULT_TYPE, phone: str):
    user_id = update.effective_user.id

    phone = phone.strip().replace(" ", "")
    if not phone.startswith('+'):
        await update.message.reply_text("❌ Phone number must start with +")
        WAITING_FOR.pop(user_id, None)
        return

    accounts = await load_accounts(user_id)
    for acc in accounts:
        if acc.get('phone') == phone:
            await update.message.reply_text("❌ This account is already added!")
            WAITING_FOR.pop(user_id, None)
            return

    status = await update.message.reply_text("📱 Sending OTP request...")
    try:
        session_path = f"sessions/user_{user_id}_{int(time.time())}"
        os.makedirs("sessions", exist_ok=True)
        client = TelegramClient(session_path, API_ID, API_HASH)
        await client.connect()
        sent = await client.send_code_request(phone)
        PENDING_OTP[user_id] = {'client': client, 'phone': phone, 'hash': sent.phone_code_hash, 'session': session_path}
        await status.edit_text("✅ OTP sent! Enter the 5-digit code:")
        WAITING_FOR.pop(user_id, None)
    except Exception as e:
        await status.edit_text(f"❌ Error: {str(e)[:100]}")
        WAITING_FOR.pop(user_id, None)

async def verify_otp(update: Update, context: ContextTypes.DEFAULT_TYPE, code: str):
    user_id = update.effective_user.id
    if user_id not in PENDING_OTP:
        await update.message.reply_text("❌ No pending OTP!")
        return

    data = PENDING_OTP[user_id]
    status = await update.message.reply_text("🔐 Verifying OTP...")
    try:
        await data['client'].sign_in(phone=data['phone'], code=code, phone_code_hash=data['hash'])
        me = await data['client'].get_me()

        session_string = data['client'].session.save()

        account = {
            'phone': data['phone'],
            'session': data['session'],
            'session_string': session_string,
            'username': me.username or 'No username',
            'user_id': me.id,
            'first_name': me.first_name or '',
            'added_date': str(datetime.now()),
            'type': 'phone_otp'
        }
        accounts = await load_accounts(user_id)
        accounts.append(account)
        await save_accounts(user_id, accounts)

        ACTIVE_CLIENTS[data['phone']] = data['client']

        if data['phone'] in LIVE_CACHE:
            del LIVE_CACHE[data['phone']]

        del PENDING_OTP[user_id]
        await status.edit_text(f"✅ Account added!\n👤 @{account['username']}\n📱 {data['phone']}\n\n✅ Account is LIVE!")

    except SessionPasswordNeededError:
        PENDING_2FA[user_id] = {'client': data['client'], 'phone': data['phone'], 'session': data['session']}
        del PENDING_OTP[user_id]
        await status.edit_text("🔐 2FA enabled! Send your password:")
    except Exception as e:
        await status.edit_text(f"❌ Error: {str(e)[:100]}")

async def verify_2fa(update: Update, context: ContextTypes.DEFAULT_TYPE, password: str):
    user_id = update.effective_user.id
    if user_id not in PENDING_2FA:
        await update.message.reply_text("❌ No pending 2FA!")
        return

    data = PENDING_2FA[user_id]
    status = await update.message.reply_text("🔐 Verifying 2FA...")
    try:
        await data['client'].sign_in(password=password)
        me = await data['client'].get_me()

        session_string = data['client'].session.save()

        account = {
            'phone': data['phone'],
            'session': data['session'],
            'session_string': session_string,
            'username': me.username or 'No username',
            'user_id': me.id,
            'first_name': me.first_name or '',
            'added_date': str(datetime.now()),
            'type': 'phone_otp'
        }
        accounts = await load_accounts(user_id)
        accounts.append(account)
        await save_accounts(user_id, accounts)

        ACTIVE_CLIENTS[data['phone']] = data['client']

        if data['phone'] in LIVE_CACHE:
            del LIVE_CACHE[data['phone']]

        del PENDING_2FA[user_id]
        await status.edit_text(f"✅ 2FA Account added!\n👤 @{account['username']}\n📱 {data['phone']}\n\n✅ Account is LIVE!")
    except Exception as e:
        await status.edit_text(f"❌ Error: {str(e)[:100]}")

async def add_session_string(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    WAITING_FOR[user_id] = 'session_string'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text("🔑 Session String\n\nSend your Telethon session string.", reply_markup=reply_markup)

async def handle_session_string(update: Update, context: ContextTypes.DEFAULT_TYPE, session_str: str):
    user_id = update.effective_user.id
    status = await update.message.reply_text("🔐 Connecting...")
    try:
        client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            await status.edit_text("❌ Invalid session string!")
            WAITING_FOR.pop(user_id, None)
            return

        me = await client.get_me()
        phone = me.phone if me.phone else f"user_{me.id}"

        accounts = await load_accounts(user_id)
        for acc in accounts:
            if acc.get('phone') == phone:
                await status.edit_text(f"❌ Already added!")
                WAITING_FOR.pop(user_id, None)
                return

        session_path = f"sessions/user_{user_id}_{int(time.time())}"
        os.makedirs("sessions", exist_ok=True)
        with open(f"{session_path}.session_string", 'w') as f:
            f.write(session_str)

        account = {
            'phone': phone,
            'session': session_path,
            'session_string': session_str,
            'username': me.username or 'No username',
            'user_id': me.id,
            'first_name': me.first_name or '',
            'added_date': str(datetime.now()),
            'type': 'session_string'
        }
        accounts.append(account)
        await save_accounts(user_id, accounts)

        ACTIVE_CLIENTS[phone] = client

        if phone in LIVE_CACHE:
            del LIVE_CACHE[phone]

        WAITING_FOR.pop(user_id, None)
        await status.edit_text(f"✅ Account added!\n👤 @{account['username']}\n📱 {phone}\n\n✅ Account is LIVE!")
    except Exception as e:
        await status.edit_text(f"❌ Error: {str(e)[:100]}")
        WAITING_FOR.pop(user_id, None)

async def add_pyrogram_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    WAITING_FOR[user_id] = 'pyrogram_session'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(
        "🔑 Pyrogram Session (Beta)\n\nSend your Pyrogram session string.\n\nNote: Pyrogram sessions are stored but not yet usable for campaigns (coming soon).",
        reply_markup=reply_markup)

async def handle_pyrogram_session(update: Update, context: ContextTypes.DEFAULT_TYPE, session_str: str):
    user_id = update.effective_user.id
    try:
        accounts = await load_accounts(user_id)
        phone = f"pyrogram_{int(time.time())}_{len(accounts)}"
        account = {
            'phone': phone,
            'session_string': session_str,
            'username': 'Pyrogram Account',
            'user_id': 0,
            'first_name': 'Pyrogram',
            'added_date': str(datetime.now()),
            'type': 'pyrogram'
        }
        accounts.append(account)
        await save_accounts(user_id, accounts)
        WAITING_FOR.pop(user_id, None)
        await update.message.reply_text("✅ Pyrogram session stored! (Beta - not yet usable for actions)")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")
        WAITING_FOR.pop(user_id, None)

async def add_bulk_sessions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    WAITING_FOR[user_id] = 'bulk_sessions'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text("📦 Bulk Sessions\n\nSend multiple session strings (one per line):", reply_markup=reply_markup)

async def handle_bulk_sessions(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    lines = text.strip().split('\n')
    session_strings = [s.strip() for s in lines if s.strip()]

    status = await update.message.reply_text(f"📦 Processing {len(session_strings)} sessions...")
    added = 0

    for session_str in session_strings:
        try:
            client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
            await client.connect()
            if not await client.is_user_authorized():
                continue

            me = await client.get_me()
            phone = me.phone if me.phone else f"user_{me.id}"

            accounts = await load_accounts(user_id)
            already = False
            for acc in accounts:
                if acc.get('phone') == phone:
                    already = True
                    break
            if already:
                continue

            session_path = f"sessions/user_{user_id}_{int(time.time())}_{added}"
            with open(f"{session_path}.session_string", 'w') as f:
                f.write(session_str)

            account = {
                'phone': phone,
                'session': session_path,
                'session_string': session_str,
                'username': me.username or 'No username',
                'user_id': me.id,
                'first_name': me.first_name or '',
                'added_date': str(datetime.now()),
                'type': 'bulk_session'
            }
            accounts.append(account)
            await save_accounts(user_id, accounts)
            ACTIVE_CLIENTS[phone] = client

            if phone in LIVE_CACHE:
                del LIVE_CACHE[phone]

            added += 1
        except:
            pass
        await asyncio.sleep(0.3)

    WAITING_FOR.pop(user_id, None)
    await status.edit_text(f"✅ Added {added}/{len(session_strings)} accounts!")

async def add_zip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    WAITING_FOR[user_id] = 'zip_upload'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(
        "📦 ZIP Upload\n\nSend a ZIP file containing session strings (one per file, .txt or any text file).\n\nExample contents:\n- session1.txt\n- session2.txt\nEach file should contain a valid Telethon session string.",
        reply_markup=reply_markup)

async def my_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id

    if is_owner(user_id):
        all_accounts = await load_owner_accounts()
        live = []
        expired = []
        for a in all_accounts:
            if await is_account_live(a):
                live.append(a)
            else:
                expired.append(a)

        text = f"📋 Owner Accounts\n━━━━━━━━━━━━━━━━━━━━━━\n\nTotal: {len(live)}\n\n"
        for a in live[:10]:
            text += f"👤 @{a.get('username', 'No username')}\n📱 {a['phone']}\n\n"
        if len(live) > 10:
            text += f"\n... and {len(live) - 10} more"

        keyboard = [
            [styled_button(f"✅ Live ({len(live)})", callback_data="admin_view_live"),
             styled_button(f"❌ Expired ({len(expired)})", callback_data="admin_view_expired")],
            [styled_button("🗑️ Remove", callback_data="admin_remove_prompt"),
             styled_button("⚠️ REMOVE ALL", callback_data="admin_remove_all_prompt")],
            [styled_button("➕ Add Another", callback_data="add_account")],
            [styled_button("❌ CANCEL", callback_data="cancel_action")],
        ]
    else:
        personal_accounts = await load_accounts(user_id)
        shared_limit = await get_user_shared_limit(user_id)
        owner_accounts = await load_owner_accounts()

        personal_live = []
        for a in personal_accounts:
            if await is_account_live(a):
                personal_live.append(a)

        user_phones = [a.get('phone') for a in personal_accounts]
        available_shared = [a for a in owner_accounts if a.get('phone') not in user_phones]
        shared_to_show = available_shared[:shared_limit]

        text = f"📋 Your Personal Accounts\n━━━━━━━━━━━━━━━━━━━━━━\n\nTotal: {len(personal_live)}\n\n"
        for a in personal_live[:10]:
            text += f"👤 @{a.get('username', 'No username')}\n📱 {a['phone']}\n\n"

        if shared_limit > 0 and shared_to_show:
            text += f"\n📋 Shared Accounts - {len(shared_to_show)}/{shared_limit}\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            for a in shared_to_show[:10]:
                text += f"👤 @{a.get('username', 'No username')}\n📱 {a['phone']}\n(Shared by Owner)\n\n"

        keyboard = [
            [styled_button("➕ Add Account", callback_data="add_account")],
            [styled_button("🗑️ Remove Personal Account", callback_data="user_remove_prompt")],
            [styled_button("❌ CANCEL", callback_data="cancel_action")],
        ]

    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    if len(text) > 4000:
        text = text[:3800] + "\n\n... (truncated)"
    try:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    except:
        pass

# ========== SHOPPING ==========
async def show_shopping_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, accounts):
    context.user_data['campaign_accounts'] = accounts

    keyboard = [
        [styled_button("📢 Join Channel", callback_data="campaign_action_join"),
         styled_button("🚪 Leave Channel", callback_data="leave_channel_menu")],
        [styled_button("❤️ React Only", callback_data="campaign_action_react"),
         styled_button("🎲 Different Reactions", callback_data="campaign_action_different_react")],
        [styled_button("👁️ View Only", callback_data="private_channel_view"),
         styled_button("🗳️ Vote Only", callback_data="campaign_action_vote")],
        [styled_button("❤️‍🔥 React + Vote", callback_data="campaign_action_reactvote"),
         styled_button("❤️ React + View", callback_data="campaign_action_reactview")],
        [styled_button("🗳️ Vote + View", callback_data="campaign_action_voteview"),
         styled_button("❤️‍🔥🗳️👁️ All Three", callback_data="campaign_action_reactvoteview")],
        [styled_button("💬 Bulk DM", callback_data="campaign_action_dm"),
         styled_button("🔊 VC", callback_data="campaign_action_vc")],
        [styled_button("📢 Group Spam", callback_data="campaign_action_group_spam")],
        [styled_button("🎨 Multi-Reactions (Emoji)", callback_data="campaign_normal_emoji"),
         styled_button("✨ Premium Emoji", callback_data="campaign_premium_mode")],
        [styled_button("🔙 Back", callback_data="main")],
    ]

    await update.callback_query.edit_message_text(
        f"🛍️ SHOPPING\n\n📊 Active Accounts: {len(accounts)}\n✅ Select action:",
        reply_markup={"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]})

async def new_campaign(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    accs = await get_accessible_accounts(user_id)

    accs = [a for a in accs if a.get('type') != 'pyrogram']

    if not accs:
        await update.callback_query.edit_message_text(
            "❌ No accounts available!\n\nPlease add accounts first.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ ADD ACCOUNT", callback_data="add_account")]]))
        return

    live_accs = []
    for a in accs:
        if await is_account_live(a):
            live_accs.append(a)

    if not live_accs:
        await update.callback_query.edit_message_text("❌ No active accounts found!")
        return

    await show_shopping_menu(update, context, live_accs)

async def campaign_action_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str):
    user_id = update.callback_query.from_user.id
    accounts = context.user_data.get('campaign_accounts', [])

    if not accounts:
        await update.callback_query.edit_message_text("❌ No accounts available!")
        return

    if action in REACTION_ACTIONS:
        context.user_data['pending_reaction_action'] = action
        await show_reaction_type_selection(update, context)
        return

    context.user_data['campaign_action'] = action
    WAITING_FOR[user_id] = 'campaign_link'

    keyboard = [[InlineKeyboardButton("❌ CANCEL", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    action_msgs = {
        'join': 'Send channel link to join:\nExample: t.me/username',
        'different_react': 'Send post link for random reactions:\nExample: t.me/username/123',
        'vote': 'Send post link to vote:\nExample: t.me/username/123',
        'dm': 'Send username or ID to DM:\nExample: @username',
        'vc': 'Send voice chat link:\nExample: t.me/username',
        'group_spam': 'Send group link to spam:\nExample: t.me/groupname',
        'view': 'Send post link for view:\nExample: t.me/username/123 or t.me/c/123456789/123',
    }

    msg = action_msgs.get(action, 'Send the link:')

    await update.callback_query.edit_message_text(
        f"📢 {action.upper()}\n\nThis action will be performed on {len(accounts)} accounts\n\n{msg}",
        reply_markup=reply_markup)

# ========== PREMIUM EMOJI FLOW ==========
async def campaign_premium_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    accounts = context.user_data.get('campaign_accounts', [])
    if not accounts:
        await update.callback_query.edit_message_text("❌ No accounts!")
        return

    context.user_data['campaign_action'] = 'premium_emoji'
    WAITING_FOR[user_id] = 'premium_link'

    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.callback_query.edit_message_text(
        "✨ PREMIUM EMOJI (Auto-Detect)\n\n"
        "Send the post link where you will manually react with the premium emoji.\n\n"
        "Example: t.me/username/123 or t.me/c/123456789/123",
        reply_markup=reply_markup)

async def handle_premium_link(update: Update, context: ContextTypes.DEFAULT_TYPE, link: str):
    user_id = update.effective_user.id
    context.user_data['premium_link'] = link
    WAITING_FOR[user_id] = 'premium_wait_react'

    keyboard = [
        [InlineKeyboardButton("✅ I have reacted", callback_data="premium_reacted")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "✅ Link received!\n\n"
        "Now please open that post in Telegram and manually react with your desired premium emoji using your own account.\n\n"
        "After you have reacted, click the button below.",
        reply_markup=reply_markup)

async def premium_reacted_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    link = context.user_data.get('premium_link')
    if not link:
        await query.edit_message_text("❌ Link not found. Please start over.")
        return

    accounts = context.user_data.get('campaign_accounts', [])
    if not accounts:
        await query.edit_message_text("❌ No accounts available.")
        return

    client = None
    for acc in accounts:
        client = await get_client_for_account(acc)
        if client:
            break

    if not client:
        await query.edit_message_text("❌ No working account to detect reactions. Please ensure you have at least one live account.")
        return

    await query.edit_message_text("🔍 Detecting premium reaction on the post...")

    try:
        custom_emoji_id = await detect_premium_reaction(client, link)
        if custom_emoji_id:
            context.user_data['detected_premium_id'] = custom_emoji_id
            await query.edit_message_text(
                f"✅ Premium reaction detected! Emoji ID: {custom_emoji_id}\n\n"
                "Now proceeding to campaign setup.",
                reply_markup=None)
            await proceed_premium_campaign(update, context, user_id, link, custom_emoji_id)
        else:
            await query.edit_message_text(
                "❌ No premium (custom) reaction found on the post.\n\n"
                "Please make sure you have manually reacted with a premium emoji using your own account, then click 'I have reacted' again.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Try Again", callback_data="premium_reacted")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")]
                ]))
    except Exception as e:
        await query.edit_message_text(f"❌ Error detecting reaction: {str(e)[:100]}")

async def proceed_premium_campaign(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, link: str, custom_emoji_id: str):
    accounts = context.user_data.get('campaign_accounts', [])
    if not accounts:
        await update.message.reply_text("❌ No accounts!")
        return

    extra = {'premium': {'id': custom_emoji_id, 'name': 'Detected'}}
    PENDING_CAMPAIGN[user_id] = {
        'action': 'premium_emoji',
        'link': link,
        'accounts': accounts,
        'extra': extra
    }

    WAITING_FOR[user_id] = 'account_count'

    keyboard = [
        [InlineKeyboardButton(f"📊 All ({len(accounts)})", callback_data=f"use_all_{len(accounts)}")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text(
            f"How many accounts?\n\nYou have {len(accounts)} active account(s).\n\nSend number or tap All:",
            reply_markup=reply_markup)
    else:
        await update.message.reply_text(
            f"How many accounts?\n\nYou have {len(accounts)} active account(s).\n\nSend number or tap All:",
            reply_markup=reply_markup)

# ========== NORMAL EMOJI MODE ==========
async def campaign_normal_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['original_action'] = None
    await show_emoji_selection(update, context)

# ========== HANDLE CAMPAIGN LINK ==========
async def handle_campaign_link(update: Update, context: ContextTypes.DEFAULT_TYPE, link: str):
    user_id = update.effective_user.id
    action = context.user_data.get('campaign_action')
    accounts = context.user_data.get('campaign_accounts', [])
    reaction_type = context.user_data.get('reaction_type', 'normal')

    if not action or not accounts:
        await update.message.reply_text("❌ Please start campaign again.")
        WAITING_FOR.pop(user_id, None)
        return

    extra_data = {}
    selected_emojis = context.user_data.get('selected_emojis')
    if selected_emojis:
        extra_data['emojis'] = selected_emojis

    if action in REACTION_ACTIONS and reaction_type == 'premium':
        context.user_data['premium_link'] = link
        PENDING_CAMPAIGN[user_id] = {
            'action': action,
            'link': link,
            'accounts': accounts,
            'extra': extra_data,
            'reaction_type': 'premium',
            'pending_premium_detection': True
        }
        WAITING_FOR[user_id] = 'premium_detect_react'
        keyboard = [
            [InlineKeyboardButton("✅ I have reacted", callback_data="premium_reacted_for_action")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "✨ Premium reaction selected.\n\n"
            "Please open that post in Telegram and manually react with your desired premium emoji using your own account.\n\n"
            "After you have reacted, click the button below to detect the emoji.",
            reply_markup=reply_markup
        )
        return

    PENDING_CAMPAIGN[user_id] = {
        'action': action,
        'link': link,
        'accounts': accounts,
        'extra': extra_data,
        'reaction_type': reaction_type
    }

    if action == 'group_spam' or action == 'dm':
        WAITING_FOR[user_id] = 'spam_message'
        await update.message.reply_text("📝 Now send the message to spam/send:")
        return

    WAITING_FOR[user_id] = 'account_count'

    keyboard = [
        [InlineKeyboardButton(f"📊 All ({len(accounts)})", callback_data=f"use_all_{len(accounts)}")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"How many accounts?\n\nYou have {len(accounts)} active account(s).\n\nSend number or tap All:",
        reply_markup=reply_markup)

async def premium_reacted_for_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    campaign = PENDING_CAMPAIGN.get(user_id)
    if not campaign:
        await query.edit_message_text("❌ Campaign data lost.")
        return

    link = campaign.get('link')
    accounts = campaign.get('accounts', [])
    if not link or not accounts:
        await query.edit_message_text("❌ Missing link or accounts.")
        return

    client = None
    for acc in accounts:
        client = await get_client_for_account(acc)
        if client:
            break

    if not client:
        await query.edit_message_text("❌ No working account to detect reactions.")
        return

    await query.edit_message_text("🔍 Detecting premium reaction on the post...")

    try:
        custom_emoji_id = await detect_premium_reaction(client, link)
        if custom_emoji_id:
            campaign['extra']['premium'] = {'id': custom_emoji_id, 'name': 'Detected'}
            campaign['reaction_type'] = 'premium'
            PENDING_CAMPAIGN[user_id] = campaign
            WAITING_FOR[user_id] = 'account_count'
            keyboard = [
                [InlineKeyboardButton(f"📊 All ({len(accounts)})", callback_data=f"use_all_{len(accounts)}")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                f"✅ Premium reaction detected! Emoji ID: {custom_emoji_id}\n\n"
                f"How many accounts?\n\nYou have {len(accounts)} active account(s).\n\nSend number or tap All:",
                reply_markup=reply_markup)
        else:
            await query.edit_message_text(
                "❌ No premium (custom) reaction found on the post.\n\n"
                "Please make sure you have manually reacted with a premium emoji using your own account, then click the button again.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Try Again", callback_data="premium_reacted_for_action")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")]
                ]))
    except Exception as e:
        await query.edit_message_text(f"❌ Error detecting reaction: {str(e)[:100]}")

async def handle_spam_message(update: Update, context: ContextTypes.DEFAULT_TYPE, msg: str):
    user_id = update.effective_user.id
    campaign = PENDING_CAMPAIGN.get(user_id)

    if not campaign:
        await update.message.reply_text("❌ Campaign data lost!")
        WAITING_FOR.pop(user_id, None)
        return

    PENDING_SPAM_MSG[user_id] = msg
    WAITING_FOR[user_id] = 'account_count'

    accounts = campaign.get('accounts', [])
    keyboard = [
        [InlineKeyboardButton(f"📊 All ({len(accounts)})", callback_data=f"use_all_{len(accounts)}")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"How many accounts?\n\nYou have {len(accounts)} active account(s).\n\nSend number or tap All:",
        reply_markup=reply_markup)

async def use_all_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    count_str = query.data.replace("use_all_", "")
    count = int(count_str)

    SELECTED_ACCOUNT_COUNT[user_id] = count
    await show_campaign_summary(update, context, user_id)

async def handle_account_count(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    accounts = context.user_data.get('campaign_accounts', [])

    try:
        if text.lower() == 'all':
            count = len(accounts)
        else:
            count = int(text)

        if count < 1 or count > len(accounts):
            await update.message.reply_text(f"❌ Send number between 1 and {len(accounts)}")
            return

        SELECTED_ACCOUNT_COUNT[user_id] = count
        WAITING_FOR.pop(user_id, None)
        await show_campaign_summary(update, context, user_id)
    except ValueError:
        await update.message.reply_text(f"❌ Send valid number or 'all'")

async def show_campaign_summary(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    campaign = PENDING_CAMPAIGN.get(user_id)
    if not campaign:
        await update.message.reply_text("❌ Campaign data lost!")
        return

    action = campaign['action']
    link = campaign['link']
    accounts = campaign.get('accounts', [])
    use_count = SELECTED_ACCOUNT_COUNT.get(user_id, len(accounts))
    extra = campaign.get('extra', {})
    reaction_type = campaign.get('reaction_type', 'normal')

    action_names = {
        'join': '📢 Join Channel',
        'react': '❤️ React Only',
        'different_react': '🎲 Different Reactions',
        'view': '👁️ View Only',
        'vote': '🗳️ Vote Only',
        'reactvote': '❤️‍🔥 React + Vote',
        'reactview': '❤️ React + View',
        'voteview': '🗳️ Vote + View',
        'reactvoteview': '❤️‍🔥🗳️👁️ All Three',
        'dm': '💬 Bulk DM',
        'vc': '🔊 VC',
        'group_spam': '📢 Group Spam',
        'multi_react': '🎨 Multiple Reactions',
        'premium_emoji': '✨ Premium Emoji'
    }

    summary = f"""📢 CAMPAIGN SUMMARY
━━━━━━━━━━━━━━━━━━━━━━

Action: {action_names.get(action, action)}
Target: {link}
Reaction Type: {reaction_type.capitalize()}

Accounts: {use_count} will run"""

    if action == 'multi_react' or (action in REACTION_ACTIONS and reaction_type == 'normal'):
        emojis = extra.get('emojis', [])
        if emojis:
            summary += f"\nEmojis: {', '.join(emojis)}"
    elif action == 'premium_emoji' or reaction_type == 'premium':
        prem = extra.get('premium', {})
        summary += f"\nPremium Emoji ID: {prem.get('id', 'Unknown')}"

    summary += "\n\nTap RUN to start"""

    keyboard = [
        [InlineKeyboardButton("🚀 RUN", callback_data="run_campaign")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text(summary, reply_markup=reply_markup)
    else:
        await update.message.reply_text(summary, reply_markup=reply_markup)

# ========== RUN CAMPAIGN ==========
async def run_campaign(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    campaign = PENDING_CAMPAIGN.get(user_id)
    spam_msg = PENDING_SPAM_MSG.get(user_id)

    if not campaign:
        await update.callback_query.edit_message_text("❌ Campaign data lost!")
        return

    action = campaign['action']
    link = campaign['link']
    accounts = campaign.get('accounts', [])
    use_count = SELECTED_ACCOUNT_COUNT.get(user_id, len(accounts))
    extra = campaign.get('extra', {})
    reaction_type = campaign.get('reaction_type', 'normal')

    accounts_to_use = accounts[:use_count]

    if not accounts_to_use:
        await update.callback_query.edit_message_text("❌ No active accounts!")
        return

    status_msg = await update.callback_query.edit_message_text(
        f"🚀 CAMPAIGN RUNNING...\n\nAction: {action}\nAccounts: {len(accounts_to_use)}\n\nProcessing...",
        reply_markup=None
    )

    results = {'success': 0, 'failed': 0, 'errors': []}

    use_premium = (reaction_type == 'premium' and action in REACTION_ACTIONS) or action == 'premium_emoji'
    premium_id = None
    if use_premium:
        if action == 'premium_emoji':
            premium_id = extra.get('premium', {}).get('id')
        else:
            premium_id = extra.get('premium', {}).get('id')
        if not premium_id:
            results['failed'] += len(accounts_to_use)
            await status_msg.edit_text("❌ Premium emoji ID missing. Campaign aborted.")
            return

    for i, acc in enumerate(accounts_to_use):
        try:
            client = await get_client_for_account(acc)
            if not client:
                results['failed'] += 1
                results['errors'].append(f"Connection failed")
                continue

            if action == 'join':
                success, msg = await join_channel_func(client, link)
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(f"Join: {msg}")
                continue

            elif action == 'dm':
                target = link.strip()
                msg_to_send = spam_msg if spam_msg else "Hello!"
                if target.isdigit():
                    entity = await client.get_entity(int(target))
                else:
                    entity = await client.get_entity(target)
                await client.send_message(entity, msg_to_send)
                results['success'] += 1
                continue

            elif action == 'vc':
                success, msg = await join_voice_chat_func(client, link)
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(f"VC: {msg}")
                continue

            elif action == 'group_spam':
                success, msg = await group_spam_message(client, link, spam_msg)
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(msg)
                continue

            elif action == 'react':
                if use_premium:
                    success, msg = await send_premium_reaction_func(client, link, premium_id)
                else:
                    success, msg = await send_reaction_func(client, link, '🔥')
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(msg)
                continue

            elif action == 'different_react':
                success, msg = await add_different_reactions(client, link)
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(msg)
                continue

            elif action == 'multi_react':
                if use_premium:
                    success, msg = await send_premium_reaction_func(client, link, premium_id)
                else:
                    emojis = extra.get('emojis', [])
                    if not emojis:
                        results['failed'] += 1
                        results['errors'].append("No emojis")
                        continue
                    selected_emoji = emojis[i % len(emojis)]
                    success, msg = await send_reaction_func(client, link, selected_emoji)
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(msg)
                continue

            elif action == 'premium_emoji':
                success, msg = await send_premium_reaction_func(client, link, premium_id)
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(msg)
                continue

            elif action == 'view':
                try:
                    if 't.me/c/' in link:
                        parts = link.split('t.me/c/')[1].split('/')
                        channel_id = int(parts[0])
                        msg_id = int(parts[1].split('?')[0])
                        peer = PeerChannel(channel_id)
                        result = await client(GetMessagesViewsRequest(
                            peer=peer,
                            id=[msg_id],
                            increment=True
                        ))
                        if result:
                            results['success'] += 1
                        else:
                            results['failed'] += 1
                            results['errors'].append("Failed to add view")
                    else:
                        import re
                        match = re.search(r't\.me/([^/]+)/(\d+)', link)
                        if match:
                            channel = match.group(1)
                            msg_id = int(match.group(2))
                            entity = await client.get_entity(channel)
                            result = await client(GetMessagesViewsRequest(
                                peer=entity,
                                id=[msg_id],
                                increment=True
                            ))
                            if result:
                                results['success'] += 1
                            else:
                                results['failed'] += 1
                                results['errors'].append("Failed to add view")
                        else:
                            results['failed'] += 1
                            results['errors'].append("Invalid link format")
                except FloodWaitError as e:
                    results['failed'] += 1
                    results['errors'].append(f"Flood wait {e.seconds}s")
                    await asyncio.sleep(min(e.seconds, 5))
                except Exception as e:
                    results['failed'] += 1
                    results['errors'].append(str(e)[:40])
                continue

            elif action == 'vote':
                success, msg = await vote_poll_func(client, link)
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(msg)
                continue

            else:
                import re
                is_private = 't.me/c/' in link

                if is_private:
                    parts = link.split('t.me/c/')[1].split('/')
                    channel_id = int(parts[0])
                    msg_id = int(parts[1].split('?')[0])
                    entity = PeerChannel(channel_id)
                else:
                    match = re.search(r't\.me/([^/]+)/(\d+)', link)
                    if not match:
                        results['failed'] += 1
                        results['errors'].append("Invalid link")
                        continue
                    channel = match.group(1)
                    msg_id = int(match.group(2))
                    entity = await client.get_entity(channel)

                if action in ['reactvote', 'reactview', 'reactvoteview']:
                    if use_premium:
                        react_success, react_msg = await send_premium_reaction_func(client, link, premium_id)
                    else:
                        react_success, react_msg = await send_reaction_func(client, link, '🔥')
                    if not react_success:
                        results['failed'] += 1
                        results['errors'].append(react_msg)
                        continue

                if action == 'reactvote':
                    vote_success, vote_msg = await vote_poll_func(client, link)
                    if vote_success:
                        results['success'] += 1
                    else:
                        results['failed'] += 1
                        results['errors'].append(vote_msg)
                elif action == 'reactview':
                    try:
                        await client(GetMessagesViewsRequest(peer=entity, id=[msg_id], increment=True))
                        results['success'] += 1
                    except Exception as e:
                        results['failed'] += 1
                        results['errors'].append(str(e)[:40])
                elif action == 'voteview':
                    vote_success, vote_msg = await vote_poll_func(client, link)
                    if vote_success:
                        try:
                            await client(GetMessagesViewsRequest(peer=entity, id=[msg_id], increment=True))
                            results['success'] += 1
                        except Exception as e:
                            results['failed'] += 1
                            results['errors'].append(str(e)[:40])
                    else:
                        results['failed'] += 1
                        results['errors'].append(vote_msg)
                elif action == 'reactvoteview':
                    vote_success, vote_msg = await vote_poll_func(client, link)
                    if vote_success:
                        try:
                            await client(GetMessagesViewsRequest(peer=entity, id=[msg_id], increment=True))
                            results['success'] += 1
                        except Exception as e:
                            results['failed'] += 1
                            results['errors'].append(str(e)[:40])
                    else:
                        results['failed'] += 1
                        results['errors'].append(vote_msg)

        except FloodWaitError as e:
            results['failed'] += 1
            results['errors'].append(f"Flood wait {e.seconds}s")
            await asyncio.sleep(min(e.seconds, 5))
        except Exception as e:
            results['failed'] += 1
            results['errors'].append(f"{str(e)[:40]}")

        if (i + 1) % 5 == 0 or (i + 1) == len(accounts_to_use):
            try:
                await status_msg.edit_text(
                    f"🚀 CAMPAIGN RUNNING...\n\n"
                    f"Progress: {i+1}/{len(accounts_to_use)}\n"
                    f"✅ Success: {results['success']}\n"
                    f"❌ Failed: {results['failed']}"
                )
            except:
                pass

        delay = (await load_settings(user_id)).get('delay', DEFAULT_DELAY)
        await asyncio.sleep(delay)

    action_names = {
        'join': 'Join Channel',
        'react': 'React Only',
        'different_react': 'Different Reactions',
        'view': 'View Only',
        'vote': 'Vote Only',
        'reactvote': 'React + Vote',
        'reactview': 'React + View',
        'voteview': 'Vote + View',
        'reactvoteview': 'React+Vote+View',
        'dm': 'Bulk DM',
        'vc': 'Voice Chat',
        'group_spam': 'Group Spam',
        'multi_react': 'Multiple Reactions',
        'premium_emoji': 'Premium Emoji'
    }

    campaign_data = {
        'action': action_names.get(action, action),
        'target': link,
        'result': f"Success {results['success']} / Failed {results['failed']}",
        'status': 'completed',
        'timestamp': str(datetime.now()),
        'accounts_used': len(accounts_to_use),
        'success_count': results['success'],
        'failed_count': results['failed']
    }
    await save_campaign(user_id, campaign_data)

    errors_text = ""
    if results['errors']:
        errors_list = results['errors'][:5]
        errors_text = "\n\n❌ Errors:\n" + "\n".join(errors_list)

    success_rate = int((results['success'] * 100) / len(accounts_to_use)) if len(accounts_to_use) > 0 else 0

    await status_msg.edit_text(
        f"✅ CAMPAIGN FINISHED!\n\n"
        f"Total: {len(accounts_to_use)}\n"
        f"✅ Success: {results['success']}\n"
        f"❌ Failed: {results['failed']}\n"
        f"📊 Rate: {success_rate}%{errors_text}"
    )

    del PENDING_CAMPAIGN[user_id]
    SELECTED_ACCOUNT_COUNT.pop(user_id, None)
    PENDING_SPAM_MSG.pop(user_id, None)
    SELECTED_EMOJIS.pop(user_id, None)
    SELECTED_PREMIUM.pop(user_id, None)
    context.user_data.pop('pending_reaction_action', None)
    context.user_data.pop('reaction_type', None)
    context.user_data.pop('original_action', None)
    context.user_data.pop('selected_emojis', None)

# ========== SCHEDULED ==========
async def scheduled_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    scheduled_list = await load_scheduled(user_id)

    if not scheduled_list:
        keyboard = [
            [styled_button("📅 Schedule New", callback_data="schedule_new")],
            [styled_button("🔙 Back", callback_data="main")],
        ]
        reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
        await update.callback_query.edit_message_text(
            "⏰ SCHEDULED\n\nNo scheduled campaigns!",
            reply_markup=reply_markup)
    else:
        text = "⏰ YOUR SCHEDULES\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for s in scheduled_list:
            schedule_id, action, target, schedule_time, account_count, spam_msg = s
            text += f"ID: {schedule_id}\nAction: {action}\nTarget: {target[:40]}\nTime: {schedule_time}\nAccounts: {account_count}\n━━━━━━━━━━━━━━━━━━━━━━\n"

        keyboard = [
            [styled_button("📅 Schedule New", callback_data="schedule_new")],
            [styled_button("❌ Cancel Schedule", callback_data="schedule_cancel")],
            [styled_button("🔙 Back", callback_data="main")],
        ]
        reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
        await update.callback_query.edit_message_text(text[:4000], reply_markup=reply_markup)

async def schedule_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    accs = await get_accessible_accounts(user_id)

    accs = [a for a in accs if a.get('type') != 'pyrogram']

    if not accs:
        await update.callback_query.edit_message_text("❌ No accounts!")
        return

    live_accs = []
    for a in accs:
        if await is_account_live(a):
            live_accs.append(a)

    if not live_accs:
        await update.callback_query.edit_message_text("❌ No active accounts!")
        return

    context.user_data['campaign_accounts'] = live_accs
    WAITING_FOR[user_id] = 'schedule_action'

    keyboard = [
        [styled_button("📢 Join", callback_data="schedule_action_join")],
        [styled_button("❤️ React", callback_data="schedule_action_react")],
        [styled_button("🎲 Random React", callback_data="schedule_action_different_react")],
        [styled_button("👁️ View", callback_data="schedule_action_view")],
        [styled_button("🗳️ Vote", callback_data="schedule_action_vote")],
        [styled_button("🔊 VC", callback_data="schedule_action_vc")],
        [styled_button("💬 DM", callback_data="schedule_action_dm")],
        [styled_button("❌ Cancel", callback_data="cancel_action")],
    ]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(
        "📅 SCHEDULE\n\nSelect action:",
        reply_markup=reply_markup)

async def schedule_action_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str):
    user_id = update.callback_query.from_user.id
    context.user_data['schedule_action'] = action
    WAITING_FOR[user_id] = 'schedule_link'

    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.callback_query.edit_message_text(
        f"Send the link for {action}:",
        reply_markup=reply_markup)

async def handle_schedule_link(update: Update, context: ContextTypes.DEFAULT_TYPE, link: str):
    user_id = update.effective_user.id
    action = context.user_data.get('schedule_action')

    if not action:
        await update.message.reply_text("❌ Start again!")
        WAITING_FOR.pop(user_id, None)
        return

    PENDING_SCHEDULE[user_id] = {'action': action, 'link': link}

    if action == 'dm':
        WAITING_FOR[user_id] = 'schedule_spam_message'
        await update.message.reply_text("📝 Send message to send:")
    else:
        WAITING_FOR[user_id] = 'schedule_time'
        await update.message.reply_text(
            "⏰ Schedule Time\n\nFormat: YYYY-MM-DD HH:MM:SS\nExample: 2026-06-05 14:30:00\n\nTimezone: UTC")

async def handle_schedule_spam_message(update: Update, context: ContextTypes.DEFAULT_TYPE, msg: str):
    user_id = update.effective_user.id
    schedule_data = PENDING_SCHEDULE.get(user_id)

    if not schedule_data:
        await update.message.reply_text("❌ Data lost!")
        WAITING_FOR.pop(user_id, None)
        return

    PENDING_SCHEDULE[user_id]['spam_message'] = msg
    WAITING_FOR[user_id] = 'schedule_time'

    await update.message.reply_text(
        "⏰ Schedule Time\n\nFormat: YYYY-MM-DD HH:MM:SS\nExample: 2026-06-05 14:30:00")

async def handle_schedule_time(update: Update, context: ContextTypes.DEFAULT_TYPE, time_str: str):
    user_id = update.effective_user.id
    schedule_data = PENDING_SCHEDULE.get(user_id)

    if not schedule_data:
        await update.message.reply_text("❌ Data lost!")
        WAITING_FOR.pop(user_id, None)
        return

    try:
        scheduled_time = datetime.strptime(time_str.strip(), "%Y-%m-%d %H:%M:%S")
        if scheduled_time <= datetime.now():
            await update.message.reply_text("❌ Send future date/time!")
            return

        PENDING_SCHEDULE[user_id]['time_str'] = time_str
        WAITING_FOR[user_id] = 'schedule_account_count'

        accounts = context.user_data.get('campaign_accounts', [])
        await update.message.reply_text(
            f"How many accounts? (1-{len(accounts)})")

    except ValueError:
        await update.message.reply_text("❌ Invalid format! Use: YYYY-MM-DD HH:MM:SS")

async def handle_schedule_account_count(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    schedule_data = PENDING_SCHEDULE.get(user_id)

    if not schedule_data:
        await update.message.reply_text("❌ Data lost!")
        WAITING_FOR.pop(user_id, None)
        return

    try:
        count = int(text.strip())
        accounts = context.user_data.get('campaign_accounts', [])

        if count < 1 or count > len(accounts):
            await update.message.reply_text(f"❌ Send number between 1 and {len(accounts)}")
            return

        spam_message = schedule_data.get('spam_message', '')
        await save_scheduled(user_id, schedule_data['action'], schedule_data['link'],
                      schedule_data['time_str'], count, spam_message)

        WAITING_FOR.pop(user_id, None)
        PENDING_SCHEDULE.pop(user_id, None)

        await update.message.reply_text(
            f"✅ Scheduled!\n\n"
            f"Action: {schedule_data['action']}\n"
            f"Time: {schedule_data['time_str']}\n"
            f"Accounts: {count}\n\nWill run at scheduled time!")

    except ValueError:
        await update.message.reply_text("❌ Send valid number!")

async def schedule_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    scheduled_list = await load_scheduled(user_id)

    if not scheduled_list:
        await update.callback_query.edit_message_text("❌ No schedules!")
        return

    WAITING_FOR[user_id] = 'cancel_schedule_id'

    text = "❌ Cancel Schedule\n\nSend Schedule ID:\n\n"
    for s in scheduled_list:
        text += f"ID: {s[0]} - {s[1]}\n"

    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)

async def handle_cancel_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    try:
        schedule_id = int(text.strip())
        await delete_scheduled(schedule_id)
        WAITING_FOR.pop(user_id, None)
        await update.message.reply_text(f"✅ Schedule {schedule_id} cancelled!")
    except:
        await update.message.reply_text("❌ Invalid ID!")
        WAITING_FOR.pop(user_id, None)

# ========== ADMIN PANEL ==========
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    keyboard = [
        [styled_button("📢 Campaign (All)", callback_data="admin_campaign_all")],
        [styled_button("👤 Campaign (By User)", callback_data="admin_campaign_user")],
        [styled_button("📁 All Campaigns", callback_data="admin_all_campaigns")],
        [styled_button("🚫 Ban User", callback_data="admin_ban_user"),
         styled_button("✅ Unban User", callback_data="admin_unban_user")],
        [styled_button("👥 All Users", callback_data="admin_all_users")],
        [styled_button("👑 Grant Admin", callback_data="admin_grant_admin"),
         styled_button("🔽 Revoke Admin", callback_data="admin_revoke_admin")],
        [styled_button("📤 Export DB", callback_data="admin_export_db"),
         styled_button("📥 Import DB", callback_data="admin_import_db")],
        [styled_button("📢 Broadcast", callback_data="admin_broadcast")],
        [styled_button("👥 Give Access", callback_data="give_access"),
         styled_button("❌ Remove Access", callback_data="remove_access")],
        [styled_button("🔙 Main", callback_data="main")],
    ]

    await update.callback_query.edit_message_text(
        "👑 ADMIN PANEL\n━━━━━━━━━━━━━━━━━━━━━━\n\nSelect an option:",
        reply_markup={"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    )

# ========== ADMIN CAMPAIGN (ALL) ==========
async def admin_campaign_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    all_accounts = await load_owner_accounts()
    all_accounts = [a for a in all_accounts if a.get('type') != 'pyrogram']
    live_accounts = []
    for a in all_accounts:
        if await is_account_live(a):
            live_accounts.append(a)

    if not live_accounts:
        await update.callback_query.edit_message_text("❌ No active accounts available!")
        return

    await show_shopping_menu(update, context, live_accounts)

# ========== ADMIN CAMPAIGN (BY USER) ==========
async def admin_campaign_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    WAITING_FOR[user_id] = 'admin_campaign_user_id'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(
        "👑 Campaign on User\n\nSend User ID:",
        reply_markup=reply_markup)

async def handle_admin_campaign_user(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.message.reply_text("❌ Only Owner/Admin!")
        WAITING_FOR.pop(user_id, None)
        return

    try:
        target_user = int(text.strip())
        accounts = await get_accessible_accounts(target_user)
        accounts = [a for a in accounts if a.get('type') != 'pyrogram']
        live_accounts = []
        for a in accounts:
            if await is_account_live(a):
                live_accounts.append(a)

        if not live_accounts:
            await update.message.reply_text("❌ No active accounts for this user!")
            WAITING_FOR.pop(user_id, None)
            return

        context.user_data['campaign_accounts'] = live_accounts
        WAITING_FOR.pop(user_id, None)

        await show_shopping_menu(update, context, live_accounts)
    except:
        await update.message.reply_text("❌ Invalid User ID!")
        WAITING_FOR.pop(user_id, None)

# ========== BROADCAST ==========
async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    WAITING_FOR[user_id] = 'broadcast_message'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(
        "📢 BROADCAST\n\nSend the message you want to broadcast to all users.\n\nOnly text messages are supported.",
        reply_markup=reply_markup)

async def handle_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.message.reply_text("❌ Only Owner/Admin!")
        WAITING_FOR.pop(user_id, None)
        return

    status_msg = await update.message.reply_text("⏳ Sending broadcast...")
    users = await get_all_users()
    sent = 0
    failed = 0

    for u in users:
        uid = u[0]
        try:
            await context.bot.send_message(chat_id=uid, text=text)
            sent += 1
        except:
            failed += 1
        await asyncio.sleep(0.05)

    WAITING_FOR.pop(user_id, None)
    await status_msg.edit_text(
        f"✅ Broadcast completed!\n\n"
        f"👥 Total users: {len(users)}\n"
        f"✅ Sent: {sent}\n"
        f"❌ Failed: {failed}"
    )

# ========== GRANT/REVOKE ADMIN ==========
async def admin_grant_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    WAITING_FOR[user_id] = 'admin_grant_admin_id'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text("👑 Grant Admin\n\nSend User ID:", reply_markup=reply_markup)

async def admin_revoke_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    WAITING_FOR[user_id] = 'admin_revoke_admin_id'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text("🔽 Revoke Admin\n\nSend User ID:", reply_markup=reply_markup)

async def handle_admin_grant_admin(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.message.reply_text("❌ Only Owner/Admin!")
        WAITING_FOR.pop(user_id, None)
        return

    try:
        target = int(text.strip())
        if target == OWNER_ID:
            await update.message.reply_text("❌ Cannot change owner's admin status!")
            WAITING_FOR.pop(user_id, None)
            return
        await grant_admin(target)
        WAITING_FOR.pop(user_id, None)
        await update.message.reply_text(f"✅ User {target} is now an Admin!")
    except:
        await update.message.reply_text("❌ Invalid User ID!")
        WAITING_FOR.pop(user_id, None)

async def handle_admin_revoke_admin(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.message.reply_text("❌ Only Owner/Admin!")
        WAITING_FOR.pop(user_id, None)
        return

    try:
        target = int(text.strip())
        if target == OWNER_ID:
            await update.message.reply_text("❌ Cannot change owner's admin status!")
            WAITING_FOR.pop(user_id, None)
            return
        await revoke_admin(target)
        WAITING_FOR.pop(user_id, None)
        await update.message.reply_text(f"✅ Admin revoked for User {target}!")
    except:
        await update.message.reply_text("❌ Invalid User ID!")
        WAITING_FOR.pop(user_id, None)

# ========== BAN/UNBAN ==========
async def admin_ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    WAITING_FOR[user_id] = 'admin_ban_user_id'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text("🚫 Ban User\n\nSend User ID:", reply_markup=reply_markup)

async def admin_unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    WAITING_FOR[user_id] = 'admin_unban_user_id'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text("✅ Unban User\n\nSend User ID:", reply_markup=reply_markup)

async def admin_all_campaigns(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    text = "📁 ALL CAMPAIGNS\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
    campaign_count = 0
    cursor = db.campaigns.find().sort("timestamp", -1).limit(20)
    async for doc in cursor:
        campaign_count += 1
        text += f"{campaign_count}. {doc['action']}\n   {doc['target'][:40]}\n   {doc['result']}\n   {doc['timestamp'][:16]}\n\n"
        if campaign_count >= 20:
            text += "\n... and more"
            break

    if campaign_count == 0:
        text += "No campaigns!"

    keyboard = [[styled_button("BACK", callback_data="admin_panel")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(text[:4000], reply_markup=reply_markup)

async def admin_all_users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    users = await get_all_users()

    if not users:
        await update.callback_query.edit_message_text("📭 No users!")
        return

    text = "👥 ALL USERS\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
    count = 0
    for u in users:
        if count >= 20:
            text += "\n... and more"
            break
        uid, username, first_name, joined_date, is_banned_user, access_expiry, shared_limit, is_admin_flag = u
        personal_accounts = await load_accounts(uid)

        if uid == OWNER_ID:
            status = "👑 OWNER"
        elif is_admin_flag:
            status = "👨‍💼 ADMIN"
        elif is_banned_user:
            status = "🚫 BANNED"
        elif access_expiry:
            status = "✅ ACCESS"
        else:
            status = "👤 USER"

        text += f"{status}\n🆔 {uid}\n📛 {first_name or 'Unknown'}\n📝 @{username or 'No username'}\n📱 Accounts: {len(personal_accounts)}"
        if shared_limit:
            text += f"\n🔗 Shared: {shared_limit}"
        if access_expiry:
            exp_date = datetime.fromisoformat(access_expiry).strftime("%Y-%m-%d")
            text += f"\n⏰ Expiry: {exp_date}"
        text += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
        count += 1

    keyboard = [[styled_button("BACK", callback_data="admin_panel")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(text[:4000], reply_markup=reply_markup)

async def handle_admin_ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.message.reply_text("❌ Only Owner/Admin!")
        WAITING_FOR.pop(user_id, None)
        return

    try:
        target = int(text.strip())
        if target == OWNER_ID:
            await update.message.reply_text("❌ Cannot ban owner!")
            WAITING_FOR.pop(user_id, None)
            return
        await ban_user(target)
        WAITING_FOR.pop(user_id, None)
        await update.message.reply_text(f"✅ User {target} banned!")
    except:
        await update.message.reply_text("❌ Invalid User ID!")
        WAITING_FOR.pop(user_id, None)

async def handle_admin_unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.message.reply_text("❌ Only Owner/Admin!")
        WAITING_FOR.pop(user_id, None)
        return

    try:
        target = int(text.strip())
        await unban_user(target)
        WAITING_FOR.pop(user_id, None)
        await update.message.reply_text(f"✅ User {target} unbanned!")
    except:
        await update.message.reply_text("❌ Invalid User ID!")
        WAITING_FOR.pop(user_id, None)

async def admin_view_live(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    all_accounts = await load_owner_accounts()
    live = []
    for a in all_accounts:
        if await is_account_live(a):
            live.append(a)

    text = f"📋 LIVE ACCOUNTS\n━━━━━━━━━━━━━━━━━━━━━━\n\nTotal: {len(live)}\n\n"
    for a in live[:10]:
        text += f"👤 @{a.get('username', 'No username')}\n📱 {a['phone']}\n\n"

    keyboard = [[styled_button("Back", callback_data="my_accounts")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)

async def admin_view_expired(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    all_accounts = await load_owner_accounts()
    expired = []
    for a in all_accounts:
        if not await is_account_live(a):
            expired.append(a)

    text = f"📋 EXPIRED ACCOUNTS\n━━━━━━━━━━━━━━━━━━━━━━\n\nTotal: {len(expired)}\n\n"
    for a in expired[:10]:
        text += f"👤 @{a.get('username', 'No username')}\n📱 {a['phone']}\n\n"

    keyboard = [
        [styled_button("Remove All Expired", callback_data="admin_remove_all_expired")],
        [styled_button("Back", callback_data="my_accounts")],
    ]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)

async def admin_remove_all_expired(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    all_accounts = await load_owner_accounts()
    expired = [a for a in all_accounts if not await is_account_live(a)]
    expired_phones = [a.get('phone') for a in expired]
    new_accounts = [a for a in all_accounts if a.get('phone') not in expired_phones]
    await save_accounts(OWNER_ID, new_accounts)

    users = await get_all_users()
    for u in users:
        uid = u[0]
        if uid != OWNER_ID:
            user_accounts = await load_accounts(uid)
            new_user_accounts = [a for a in user_accounts if a.get('phone') not in expired_phones]
            if len(user_accounts) != len(new_user_accounts):
                await save_accounts(uid, new_user_accounts)

    await update.callback_query.edit_message_text(
        f"✅ Removed {len(expired)} expired accounts!",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="my_accounts")]]))

async def admin_remove_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    WAITING_FOR[user_id] = 'admin_remove_phone'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(
        "🗑️ Remove Account\n\nSend phone number:\nExample: +919876543210",
        reply_markup=reply_markup)

async def admin_remove_account(update: Update, context: ContextTypes.DEFAULT_TYPE, phone: str):
    user_id = update.effective_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.message.reply_text("❌ Only Owner/Admin!")
        WAITING_FOR.pop(user_id, None)
        return

    owner_accounts = await load_owner_accounts()
    found = any(a.get('phone') == phone for a in owner_accounts)
    if not found:
        await update.message.reply_text("❌ Account not found!")
        WAITING_FOR.pop(user_id, None)
        return

    new_owner_accounts = [a for a in owner_accounts if a.get('phone') != phone]
    await save_accounts(OWNER_ID, new_owner_accounts)

    users = await get_all_users()
    for u in users:
        uid = u[0]
        if uid != OWNER_ID:
            user_accounts = await load_accounts(uid)
            new_user_accounts = [a for a in user_accounts if a.get('phone') != phone]
            if len(user_accounts) != len(new_user_accounts):
                await save_accounts(uid, new_user_accounts)

    WAITING_FOR.pop(user_id, None)
    await update.message.reply_text(f"✅ Account {phone} removed!")

async def admin_remove_all_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    all_accounts = await load_owner_accounts()
    keyboard = [
        [styled_button("YES, REMOVE ALL", callback_data="admin_remove_all_confirm")],
        [styled_button("CANCEL", callback_data="cancel_action")],
    ]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(
        f"⚠️ WARNING!\n\nRemove ALL {len(all_accounts)} accounts?\n\nThis cannot be undone!",
        reply_markup=reply_markup)

async def admin_remove_all_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    if not is_owner(user_id) and not await is_admin_user(user_id):
        await update.callback_query.answer("❌ Only Owner/Admin!", show_alert=True)
        return

    await save_accounts(OWNER_ID, [])
    users = await get_all_users()
    for u in users:
        uid = u[0]
        if uid != OWNER_ID:
            await save_accounts(uid, [])

    await update.callback_query.edit_message_text(
        "✅ All accounts removed!",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main", callback_data="main")]]))

async def user_remove_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    WAITING_FOR[user_id] = 'user_remove_phone'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(
        "🗑️ Remove Personal Account\n\nSend phone number to remove:",
        reply_markup=reply_markup)

async def user_remove_account(update: Update, context: ContextTypes.DEFAULT_TYPE, phone: str):
    user_id = update.effective_user.id
    accounts = await load_accounts(user_id)
    found = any(a.get('phone') == phone for a in accounts)
    if not found:
        await update.message.reply_text("❌ Account not found in your personal accounts!")
        WAITING_FOR.pop(user_id, None)
        return

    new_accounts = [a for a in accounts if a.get('phone') != phone]
    await save_accounts(user_id, new_accounts)
    WAITING_FOR.pop(user_id, None)
    await update.message.reply_text(f"✅ Account {phone} removed from your personal accounts!")

# ========== MY CAMPAIGNS, STATS, PROFILE, HELP, SUPPORT ==========
async def my_campaigns(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    campaigns = await load_campaigns(user_id)
    if not campaigns:
        await update.callback_query.edit_message_text("📁 No purchases yet!\n\nRun campaigns from Shopping.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 MAIN", callback_data="main")]]))
        return

    text = "📁 MY PURCHASED\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
    for i, c in enumerate(campaigns[:10], 1):
        text += f"{i}. {c['action']}\n   Target: {c['target'][:40]}\n   Result: {c['result']}\n   Time: {c['timestamp'].split('.')[0]}\n\n"

    await update.callback_query.edit_message_text(text[:4000], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 MAIN", callback_data="main")]]))

async def my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    personal_accounts = await load_accounts(user_id)
    personal_active = 0
    for a in personal_accounts:
        if await is_account_live(a):
            personal_active += 1

    shared_limit = await get_user_shared_limit(user_id)
    total_available = len(await get_accessible_accounts(user_id))
    campaigns = await load_campaigns(user_id)

    text = f"📊 YOUR STATS\n━━━━━━━━━━━━━━━━━━━━━━\n\n📱 Personal: {len(personal_accounts)} (🟢{personal_active})\n🔗 Shared Limit: {shared_limit}\n📊 Total Available: {total_available}\n📁 Purchased: {len(campaigns)}"

    await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 MAIN", callback_data="main")]]))

async def settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    settings = await load_settings(user_id)
    current_delay = settings.get('delay', DEFAULT_DELAY)

    keyboard = [
        [styled_button("0.5s", callback_data="delay_0.5"),
         styled_button("1s", callback_data="delay_1.0")],
        [styled_button("1.5s", callback_data="delay_1.5"),
         styled_button("2s", callback_data="delay_2.0")],
        [styled_button("Custom", callback_data="custom_delay")],
        [styled_button("CANCEL", callback_data="cancel_action")],
    ]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(f"⚙️ SETTINGS\nDelay: {current_delay}s", reply_markup=reply_markup)

async def set_delay(update: Update, context: ContextTypes.DEFAULT_TYPE, delay: str):
    user_id = update.callback_query.from_user.id
    settings = await load_settings(user_id)
    settings['delay'] = float(delay)
    await save_settings(user_id, settings)

    keyboard = [[styled_button("BACK", callback_data="settings")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(f"✅ Delay set to {delay}s", reply_markup=reply_markup)

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    doc = await get_user(user_id)
    if not doc:
        await update.callback_query.edit_message_text("❌ User not found!")
        return

    first_name = doc.get('first_name', 'Unknown')
    username = doc.get('username', 'Unknown')
    joined_date = doc.get('joined_date', 'Unknown')
    access_expiry = doc.get('access_expiry')
    shared_limit = doc.get('shared_id_limit', 0)
    is_admin_flag = doc.get('is_admin', 0)

    personal_accounts = await load_accounts(user_id)
    campaigns = await load_campaigns(user_id)

    admin_badge = " 👑 OWNER" if user_id == OWNER_ID else (" 👨‍💼 ADMIN" if is_admin_flag else "")

    text = f"👤 PROFILE{admin_badge}\n━━━━━━━━━━━━━━━━━━━━━━\n\n🆔 {user_id}\n👤 {first_name}\n📝 @{username}\n📅 {joined_date[:10] if joined_date else 'Unknown'}\n📱 Personal: {len(personal_accounts)}\n🔗 Shared Limit: {shared_limit}\n📁 Purchased: {len(campaigns)}"

    if access_expiry and user_id != OWNER_ID:
        exp_date = datetime.fromisoformat(access_expiry).strftime("%Y-%m-%d")
        text += f"\n⏰ Access until: {exp_date}"

    keyboard = [[styled_button("BACK", callback_data="main")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)

async def help_guide(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = """🤖 HOW TO USE
━━━━━━━━━━━━━━━━━━━━━━

📌 ADD ACCOUNTS
Phone + OTP / Session String / Bulk Sessions / ZIP Upload

📌 SHOPPING (CAMPAIGNS)
1. Click 'Shopping'
2. Select action
3. Choose reaction type (if applicable)
4. Send link
5. Choose account count
6. Tap 'Run'

📌 ACTIONS (ALL WORKING ✅)
• ❤️ React Only - choose normal or premium
• 🎲 Different Reactions - Random emoji each account
• 🎨 Multiple Reactions - Choose multiple emojis, split accounts (normal only)
• ✨ Premium Emoji - Auto-detect premium reaction
• 👁️ View - Increase view count (REAL VIEWS)
• 🗳️ Vote - Click poll button
• 📢 Join - Join channel
• 🚪 Leave Channel - Leave specific channel
• 🗑️ LEAVE ALL - Leave all channels
• 💬 Bulk DM - Send messages
• 🔊 VC - Join voice chat (mic off)
• 📢 Group Spam - Spam in groups

📌 SUPPORTED LINKS
• Public Post: t.me/username/123
• Private Post: t.me/c/123456789/123
• Private Invite: t.me/joinchat/xxxxx
• Channel Join: t.me/username or invite link

📌 PREMIUM EMOJIS
Auto-detection: react manually, bot detects and uses same premium emoji.

📌 ADMIN FEATURES
Owner can grant admin rights and broadcast messages.

📌 ACCESS
Contact @SHIVAMKR_208

👨‍💻 Support: @AUTO_BOTS_INFO"""

    keyboard = [[styled_button("BACK", callback_data="main")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)

async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = """📞 SUPPORT & CONTACT
━━━━━━━━━━━━━━━━━━━━━━

👨‍💻 Channel: @AUTO_BOTS_INFO
📞 Access/Support: @SHIVAMKR_208
🔧 Bot Owner: @SHIVAMKR_208

For issues, bugs, or access requests, contact above."""

    keyboard = [[styled_button("BACK", callback_data="main")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)

async def custom_delay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.callback_query.from_user.id
    WAITING_FOR[user_id] = 'custom_delay'
    keyboard = [[styled_button("CANCEL", callback_data="cancel_action")]]
    reply_markup = {"inline_keyboard": [[dict(btn) for btn in row] for row in keyboard]}
    await update.callback_query.edit_message_text("Send delay in seconds (example: 0.8):", reply_markup=reply_markup)

async def handle_custom_delay(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    try:
        delay = float(text)
        if delay <= 0:
            raise ValueError
        settings = await load_settings(user_id)
        settings['delay'] = delay
        await save_settings(user_id, settings)
        WAITING_FOR.pop(user_id, None)
        await update.message.reply_text(f"✅ Delay set to {delay}s")
    except:
        await update.message.reply_text("❌ Invalid value!")
        WAITING_FOR.pop(user_id, None)

# ========== MESSAGE HANDLER ==========
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    user_id = update.effective_user.id
    text = update.message.text.strip()
    state = WAITING_FOR.get(user_id)

    if state == 'phone':
        await handle_phone(update, context, text)
    elif state == 'session_string':
        await handle_session_string(update, context, text)
    elif state == 'bulk_sessions':
        await handle_bulk_sessions(update, context, text)
    elif state == 'pyrogram_session':
        await handle_pyrogram_session(update, context, text)
    elif state == 'campaign_link':
        await handle_campaign_link(update, context, text)
    elif state == 'spam_message':
        await handle_spam_message(update, context, text)
    elif state == 'account_count':
        await handle_account_count(update, context, text)
    elif state == 'custom_delay':
        await handle_custom_delay(update, context, text)
    elif state == 'admin_remove_phone':
        await admin_remove_account(update, context, text)
    elif state == 'user_remove_phone':
        await user_remove_account(update, context, text)
    elif state == 'admin_campaign_user_id':
        await handle_admin_campaign_user(update, context, text)
    elif state == 'admin_ban_user_id':
        await handle_admin_ban_user(update, context, text)
    elif state == 'admin_unban_user_id':
        await handle_admin_unban_user(update, context, text)
    elif state == 'admin_grant_admin_id':
        await handle_admin_grant_admin(update, context, text)
    elif state == 'admin_revoke_admin_id':
        await handle_admin_revoke_admin(update, context, text)
    elif state == 'access_user_id':
        await handle_access_user_id(update, context, text)
    elif state == 'access_days':
        await handle_access_days(update, context, text)
    elif state == 'access_shared_limit':
        await handle_access_shared_limit(update, context, text)
    elif state == 'remove_access_user_id':
        await handle_remove_access(update, context, text)
    elif state == 'schedule_link':
        await handle_schedule_link(update, context, text)
    elif state == 'schedule_spam_message':
        await handle_schedule_spam_message(update, context, text)
    elif state == 'schedule_time':
        await handle_schedule_time(update, context, text)
    elif state == 'schedule_account_count':
        await handle_schedule_account_count(update, context, text)
    elif state == 'cancel_schedule_id':
        await handle_cancel_schedule(update, context, text)
    elif state == 'private_view_link':
        await handle_private_view(update, context, text)
    elif state == 'leave_specific_link':
        await handle_leave_specific(update, context, text)
    elif state == 'premium_link':
        await handle_premium_link(update, context, text)
    elif state == 'broadcast_message':
        await handle_broadcast(update, context, text)
    elif state == 'schedule_action':
        pass
    elif text.isdigit() and len(text) == 5 and user_id in PENDING_OTP:
        await verify_otp(update, context, text)
    elif user_id in PENDING_2FA:
        await verify_2fa(update, context, text)
    else:
        await update.message.reply_text("❌ Send /start")

# ========== CALLBACK HANDLER ==========
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except:
        pass

    data = query.data

    if data == "main":
        await show_main_menu(update, context)
    elif data == "add_account":
        await add_account_menu(update, context)
    elif data == "add_phone_otp":
        await add_phone_otp(update, context)
    elif data == "add_session_string":
        await add_session_string(update, context)
    elif data == "add_pyrogram_session":
        await add_pyrogram_session(update, context)
    elif data == "add_bulk_sessions":
        await add_bulk_sessions(update, context)
    elif data == "add_zip":
        await add_zip(update, context)
    elif data == "my_accounts":
        await my_accounts(update, context)
    elif data == "new_campaign":
        await new_campaign(update, context)
    elif data == "private_channel_view":
        await private_channel_view(update, context)
    elif data == "leave_channel_menu":
        await leave_channel_menu(update, context)
    elif data == "leave_specific_channel":
        await leave_specific_channel(update, context)
    elif data == "leave_all_channels":
        await leave_all_channels(update, context)
    elif data == "confirm_leave_all":
        await confirm_leave_all(update, context)
    elif data == "scheduled":
        await scheduled_menu(update, context)
    elif data == "schedule_new":
        await schedule_new(update, context)
    elif data == "schedule_cancel":
        await schedule_cancel(update, context)
    elif data.startswith("schedule_action_"):
        action = data.replace("schedule_action_", "")
        await schedule_action_handler(update, context, action)
    elif data.startswith("campaign_action_"):
        action = data.replace("campaign_action_", "")
        await campaign_action_handler(update, context, action)
    elif data == "campaign_normal_emoji":
        await campaign_normal_emoji(update, context)
    elif data == "campaign_premium_mode":
        await campaign_premium_mode(update, context)
    elif data.startswith("select_emoji_"):
        await select_emoji_callback(update, context)
    elif data == "emoji_ready":
        await emoji_ready_callback(update, context)
    elif data.startswith("select_premium_"):
        await campaign_premium_mode(update, context)
    elif data.startswith("use_all_"):
        await use_all_callback(update, context)
    elif data == "run_campaign":
        await run_campaign(update, context)
    elif data == "my_campaigns":
        await my_campaigns(update, context)
    elif data == "my_stats":
        await my_stats(update, context)
    elif data == "settings":
        await settings_menu(update, context)
    elif data == "profile":
        await profile(update, context)
    elif data == "help":
        await help_guide(update, context)
    elif data == "support":
        await support(update, context)
    elif data.startswith("delay_"):
        await set_delay(update, context, data.replace("delay_", ""))
    elif data == "custom_delay":
        await custom_delay(update, context)
    elif data == "admin_panel":
        await admin_panel(update, context)
    elif data == "admin_campaign_all":
        await admin_campaign_all(update, context)
    elif data == "admin_campaign_user":
        await admin_campaign_user(update, context)
    elif data == "admin_ban_user":
        await admin_ban_user(update, context)
    elif data == "admin_unban_user":
        await admin_unban_user(update, context)
    elif data == "admin_grant_admin":
        await admin_grant_admin(update, context)
    elif data == "admin_revoke_admin":
        await admin_revoke_admin(update, context)
    elif data == "admin_all_campaigns":
        await admin_all_campaigns(update, context)
    elif data == "admin_all_users":
        await admin_all_users_list(update, context)
    elif data == "admin_view_live":
        await admin_view_live(update, context)
    elif data == "admin_view_expired":
        await admin_view_expired(update, context)
    elif data == "admin_remove_prompt":
        await admin_remove_prompt(update, context)
    elif data == "admin_remove_all_prompt":
        await admin_remove_all_prompt(update, context)
    elif data == "admin_remove_all_confirm":
        await admin_remove_all_confirm(update, context)
    elif data == "admin_remove_all_expired":
        await admin_remove_all_expired(update, context)
    elif data == "user_remove_prompt":
        await user_remove_prompt(update, context)
    elif data == "give_access":
        await give_access_start(update, context)
    elif data == "remove_access":
        await remove_access_start(update, context)
    elif data == "access_more_yes":
        await access_more_yes(update, context)
    elif data == "access_more_no_direct":
        await access_more_no_direct(update, context)
    elif data == "admin_export_db":
        if not is_owner(query.from_user.id):
            await query.answer("❌ Only Owner!", show_alert=True)
            return
        await query.answer()
        await context.bot.send_message(chat_id=query.message.chat_id, text="Use /export_db command to export the database.")
    elif data == "admin_import_db":
        if not is_owner(query.from_user.id):
            await query.answer("❌ Only Owner!", show_alert=True)
            return
        await query.answer()
        WAITING_FOR[query.from_user.id] = "import_db_file"
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "📥 IMPORT DATABASE\n\n"
            "⚠️ WARNING: This will REPLACE the current database!\n\n"
            "Send the automation_bot_export.json file now:",
            reply_markup=reply_markup
        )
    elif data == "admin_broadcast":
        await admin_broadcast(update, context)
    elif data == "premium_reacted":
        await premium_reacted_callback(update, context)
    elif data == "premium_reacted_for_action":
        await premium_reacted_for_action(update, context)
    elif data == "reaction_type_normal":
        await reaction_type_normal(update, context)
    elif data == "reaction_type_premium":
        await reaction_type_premium(update, context)
    elif data == "reaction_type_done":
        pass
    elif data == "reaction_type_back":
        pass
    elif data == "cancel_action":
        await cancel_button_handler(update, context)

# ========== SESSION AUTO-SYNC ON START ==========
async def sync_accounts_on_start():
    logging.info("🔄 Syncing accounts on startup...")
    all_users = await get_all_users()
    total = 0
    for u in all_users:
        uid = u[0]
        accounts = await load_accounts(uid)
        for acc in accounts:
            if acc.get('type') == 'pyrogram':
                continue
            await is_account_live(acc)
            total += 1
            await asyncio.sleep(0.1)
    logging.info(f"✅ Synced {total} accounts across {len(all_users)} users.")

# ========== STYLED BUTTON HELPER ==========
def styled_button(label, callback_data):
    return {"text": label, "callback_data": callback_data}

# ========== MAIN (KEPT ONLY ONE) ==========
async def main():
    logging.basicConfig(level=logging.ERROR)
    logging.getLogger('telethon').setLevel(logging.ERROR)
    logging.getLogger('httpx').setLevel(logging.ERROR)

    await init_mongo()
    os.makedirs("sessions", exist_ok=True)
    os.makedirs("data", exist_ok=True)

    asyncio.create_task(check_scheduled_campaigns())
    asyncio.create_task(sync_accounts_on_start())

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("export_db", cmd_export_db))
    app.add_handler(CommandHandler("import_db", cmd_import_db))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    print("=" * 50)
    print("🤖 AUTOMATION VOTE BOT STARTED (MongoDB)!")
    print("=" * 50)
    print(f"✅ Owner ID: {OWNER_ID}")
    print("=" * 50)
    print("✅ ALL ACTIONS WORKING (PUBLIC + PRIVATE):")
    print("   - Join Channel ✅")
    print("   - Leave Specific Channel ✅")
    print("   - LEAVE ALL Channels ✅")
    print("   - React Only | Different Reactions ✅")
    print("   - Multiple Reactions (Select Emojis) ✅")
    print("   - Premium Emoji (Auto-Detect) ✅")
    print("   - View Only (GetMessagesViewsRequest) ✅")
    print("   - PRIVATE CHANNEL VIEW (Auto-join + View) ✅")
    print("   - Vote Only | React + Vote | React + View ✅")
    print("   - Vote + View | React + Vote + View ✅")
    print("   - Bulk DM | VC (Voice Chat) ✅")
    print("   - Group Spam ✅")
    print("=" * 50)
    print("✅ ZIP UPLOAD: Add accounts via ZIP file (with timeouts!)")
    print("✅ ADMIN GRANT: Owner can grant admin rights")
    print("✅ ACCESS MANAGEMENT: Added to Admin Panel")
    print("✅ BROADCAST: Admin can broadcast messages")
    print("✅ PYROGRAM SESSION: Stored (beta, not usable)")
    print("=" * 50)
    print("✅ DB: MongoDB (automation_bot)")
    print("✅ DB COMMANDS: /export_db | /import_db (JSON)")
    print("=" * 50)

    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Bot stopped", flush=True)
