import os
import json
import datetime
import base64
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# =========================
# CONFIG
# =========================
TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
SHEET_ID = os.environ["GOOGLE_SHEET_ID"]

# decode service account JSON
SERVICE_JSON = json.loads(base64.b64decode(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]))

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

creds = Credentials.from_service_account_info(SERVICE_JSON, scopes=SCOPES)
service = build("sheets", "v4", credentials=creds)

CUSTOMERS_SHEET = "Customers"
PURCHASES_SHEET = "2026 Purchases"

# =========================
# HELPERS
# =========================

def normalize_instagram(value: str) -> str:
    return value.strip().lower().lstrip("@")

def now():
    return datetime.datetime.utcnow().isoformat()

def get_sheet(sheet):
    result = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"{sheet}!A:Z"
    ).execute()
    values = result.get("values", [])
    headers = values[0]
    rows = values[1:]
    return headers, rows

def append_row(sheet, row):
    service.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"{sheet}!A:Z",
        valueInputOption="RAW",
        body={"values": [row]}
    ).execute()

def update_cell(sheet, row_index, col_index, value):
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{sheet}!{chr(65+col_index)}{row_index+2}",
        valueInputOption="RAW",
        body={"values": [[value]]}
    ).execute()

# =========================
# CUSTOMER FUNCTIONS
# =========================

def find_customer(telegram_id):
    headers, rows = get_sheet(CUSTOMERS_SHEET)
    for i, row in enumerate(rows):
        if len(row) > 0 and row[0] == str(telegram_id):
            return headers, row, i
    return headers, None, None

def create_customer(user):
    append_row(CUSTOMERS_SHEET, [
        user.id,
        user.username or "",
        "",
        "",
        0,
        "Bean",
        0,
        now(),
        now(),
        "",
        ""
    ])

# =========================
# POINTS LOGIC
# =========================

def get_tier(points):
    if points >= 10000:
        return "Glassy"
    elif points >= 3000:
        return "Icy"
    elif points >= 1500:
        return "Water"
    return "Bean"

def get_multiplier(tier):
    return {
        "Bean": 1,
        "Water": 1.5,
        "Icy": 2,
        "Glassy": 3
    }[tier]

def calculate_points(instagram):
    headers, rows = get_sheet(PURCHASES_SHEET)

    total_points = 0
    last6m_points = 0

    six_months_ago = datetime.datetime.utcnow() - datetime.timedelta(days=180)

    for row in rows:
        if len(row) < 21:
            continue

        handle = normalize_instagram(row[7])
        points_awarded = row[19] if len(row) > 19 else ""
        timestamp = row[20] if len(row) > 20 else ""

        if handle != instagram:
            continue

        if points_awarded != "yes":
            continue

        try:
            dt = datetime.datetime.fromisoformat(timestamp)
        except:
            continue

        amount = float(row[14])

        # base points = amount
        base_points = int(amount)

        total_points += base_points

        if dt >= six_months_ago:
            last6m_points += base_points

    return total_points, last6m_points

def build_progress_bar(current, target):
    filled = int((current / target) * 10)
    return "🟩" * filled + "⬜" * (10 - filled)

# =========================
# BOT HANDLERS
# =========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    headers, customer, idx = find_customer(user.id)

    if not customer:
        create_customer(user)
        await update.message.reply_text(
            "Welcome to WLJ Rewards!\n\nPlease enter your Instagram handle (without @):"
        )
        context.user_data["awaiting_handle"] = True
        return

    # check missing fields
    if not customer[2]:
        await update.message.reply_text("Please enter your Instagram handle (without @):")
        context.user_data["awaiting_handle"] = True
        return

    if not customer[3]:
        await update.message.reply_text("Please enter your birthday (DD-MM-YYYY):")
        context.user_data["awaiting_birthday"] = True
        return

    await update.message.reply_text("You're back at the main menu.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text.strip()

    headers, customer, idx = find_customer(user.id)

    if context.user_data.get("awaiting_handle"):
        handle = normalize_instagram(text)
        update_cell(CUSTOMERS_SHEET, idx, 2, handle)

        context.user_data["awaiting_handle"] = False
        context.user_data["awaiting_birthday"] = True

        await update.message.reply_text("Please enter your birthday (DD-MM-YYYY):")
        return

    if context.user_data.get("awaiting_birthday"):
        update_cell(CUSTOMERS_SHEET, idx, 3, text)

        context.user_data["awaiting_birthday"] = False

        await update.message.reply_text("Registration complete 🎉")
        return

    # =========================
    # CHECK POINTS
    # =========================
    if text.lower() in ["check points", "/checkpoints"]:
        instagram = normalize_instagram(customer[2])

        total, last6m = calculate_points(instagram)

        tier = get_tier(last6m)

        update_cell(CUSTOMERS_SHEET, idx, 4, total)
        update_cell(CUSTOMERS_SHEET, idx, 5, tier)
        update_cell(CUSTOMERS_SHEET, idx, 6, last6m)

        target = {
            "Bean": 1500,
            "Water": 3000,
            "Icy": 10000,
            "Glassy": 10000
        }[tier]

        progress = build_progress_bar(last6m, target)

        await update.message.reply_text(
            f"""Instagram: @{instagram}
Current usable points: {total}
Tier: {tier}
Points (last 6 months): {last6m}

Progress:
{progress} {last6m}/{target}

You're {target - last6m} points away from next tier 🚀
"""
        )
        return

    # =========================
    # CHANGE HANDLE
    # =========================
    if text.lower() in ["change handle", "/changehandle"]:
        context.user_data["awaiting_handle"] = True
        await update.message.reply_text("Enter your new Instagram handle:")
        return

# =========================
# RUN
# =========================

app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

app.run_polling()
