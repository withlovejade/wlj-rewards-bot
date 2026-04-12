# ================= IMPORTS =================
import asyncio
import base64
import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# ================= CONFIG =================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
ADMIN_CHAT_ID = int(os.environ["ADMIN_CHAT_ID"].strip())
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"].strip()

b64 = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON_B64"].strip().replace("\n", "")
b64 += "=" * (-len(b64) % 4)
GOOGLE_SERVICE_ACCOUNT_JSON = base64.b64decode(b64).decode("utf-8")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ================= TIERS =================
TIER_THRESHOLDS = {
    "Bean": 0,
    "Water": 1500,
    "Icy": 3000,
    "Glassy": 10000,
}

TIER_ORDER = ["Bean", "Water", "Icy", "Glassy"]

# ================= HELPERS =================
def utc_now():
    return datetime.now(timezone.utc).isoformat()

def make_code(prefix):
    return f"{prefix}-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"

def normalize_instagram(value):
    return value.strip().lower().lstrip("@")

def parse_amount_to_points(value):
    raw = str(value).replace("$","").replace(",","").strip()
    try:
        return int(Decimal(raw))
    except:
        return 0

def parse_birthday(value):
    try:
        return datetime.strptime(value, "%d-%m-%Y").strftime("%d-%m-%Y")
    except:
        return None

def get_tier(points):
    if points >= 10000: return "Glassy"
    if points >= 3000: return "Icy"
    if points >= 1500: return "Water"
    return "Bean"

def get_next_tier(points):
    tier = get_tier(points)
    idx = TIER_ORDER.index(tier)
    if idx == len(TIER_ORDER)-1:
        return None
    next_t = TIER_ORDER[idx+1]
    return next_t, TIER_THRESHOLDS[next_t]

def progress_bar(current, target):
    filled = int((current/target)*10) if target else 10
    return "█"*filled + "░"*(10-filled)

# ================= GOOGLE SHEETS =================
class Sheets:
    def __init__(self):
        creds = Credentials.from_service_account_info(
            json.loads(GOOGLE_SERVICE_ACCOUNT_JSON), scopes=SCOPES
        )
        self.service = build("sheets","v4",credentials=creds,cache_discovery=False)

    def read(self, sheet):
        res = self.service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{sheet}!A:Z",
            valueRenderOption="UNFORMATTED_VALUE"
        ).execute()
        vals = res.get("values",[])
        if not vals: return [],[]
        headers = vals[0]
        rows = []
        for r in vals[1:]:
            d={}
            for i,h in enumerate(headers):
                d[h]=r[i] if i<len(r) else ""
            rows.append(d)
        return headers,rows

    def append(self,sheet,values):
        self.service.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{sheet}!A:Z",
            valueInputOption="USER_ENTERED",
            body={"values":[values]}
        ).execute()

store = Sheets()

# ================= STATES =================
(
    MENU,
    IG,
    BIRTHDAY,
    CHANGE,
    RETURN_DATE,
    RETURN_QTY,
    RETURN_CONFIRM
) = range(7)

MENU_KEYBOARD = [
    ["Return Packaging","Check Points"],
    ["Redeem Rewards","How It Works"],
    ["Change Handle","Contact Admin"]
]

# ================= START =================
async def start(update:Update,context:ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Welcome to WLJ Rewards!\n\nEnter your Instagram handle (no @)",
        reply_markup=ReplyKeyboardRemove()
    )
    return IG

# ================= IG =================
async def ig(update,context):
    ig = normalize_instagram(update.message.text)
    context.user_data["ig"]=ig

    await update.message.reply_text(
        "Enter your birthday (DD-MM-YYYY)"
    )
    return BIRTHDAY

# ================= BIRTHDAY =================
async def birthday(update,context):
    b = parse_birthday(update.message.text)
    if not b:
        await update.message.reply_text("Invalid format. Try DD-MM-YYYY")
        return BIRTHDAY

    user = update.effective_user
    store.append("Customers",[
        user.id,user.username or "",context.user_data["ig"],
        b,0,"Bean",0,utc_now(),utc_now(),"",""
    ])

    await update.message.reply_text("Saved! Welcome 🎉",reply_markup=ReplyKeyboardMarkup(MENU_KEYBOARD,resize_keyboard=True))
    return MENU

# ================= MENU =================
async def menu(update,context):
    t = update.message.text

    if t=="Check Points":
        return await checkpoints(update,context)

    if t=="Return Packaging":
        await update.message.reply_text("Enter preferred collection date/time")
        return RETURN_DATE

    if t=="Change Handle":
        await update.message.reply_text("Enter new IG handle",reply_markup=ReplyKeyboardRemove())
        return CHANGE

    return MENU

# ================= CHANGE HANDLE =================
async def change(update,context):
    ig = normalize_instagram(update.message.text)
    # simple update logic omitted for brevity
    await update.message.reply_text(f"Updated to @{ig}",reply_markup=ReplyKeyboardMarkup(MENU_KEYBOARD,resize_keyboard=True))
    return MENU

# ================= CHECKPOINTS =================
async def checkpoints(update,context):
    # simplified demo logic
    points = 0
    tier = get_tier(points)

    next_t = get_next_tier(points)

    text = f"Tier: {tier}\nPoints: {points}\n"

    if next_t:
        nt,th = next_t
        text += f"\nProgress to {nt}\n{progress_bar(points,th)} {points}/{th}\n"
        text += f"You’re only {th-points} points away from {nt}"

    await update.message.reply_text(text)
    return MENU

# ================= MAIN =================
def main():
    app = Application.builder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start",start)],
        states={
            IG:[MessageHandler(filters.TEXT,ig)],
            BIRTHDAY:[MessageHandler(filters.TEXT,birthday)],
            MENU:[MessageHandler(filters.TEXT,menu)],
            CHANGE:[MessageHandler(filters.TEXT,change)],
        },
        fallbacks=[]
    )

    app.add_handler(conv)

    print("Bot running...")
    app.run_polling()

if __name__=="__main__":
    main()
