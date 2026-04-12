import os
import json
import base64
from datetime import datetime, timedelta

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ======================
# ENV VARIABLES
# ======================

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

SERVICE_JSON = json.loads(
    base64.b64decode(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
)

SPREADSHEET_ID = "YOUR_SHEET_ID"

CUSTOMERS_SHEET = "Customers"
PURCHASES_SHEET = "2026 Purchases"

# ======================
# GOOGLE SETUP
# ======================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

creds = Credentials.from_service_account_info(SERVICE_JSON, scopes=SCOPES)
service = build("sheets", "v4", credentials=creds)

# ======================
# HELPERS
# ======================

def now_iso():
    return datetime.utcnow().isoformat()

def normalize_handle(value):
    return value.strip().lower().lstrip("@")

def get_rows(sheet_name):
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!A:Z"
    ).execute()

    rows = result.get("values", [])
    headers = rows[0]
    data = rows[1:]
    return headers, data

def find_customer(telegram_id):
    headers, rows = get_rows(CUSTOMERS_SHEET)

    for i, row in enumerate(rows):
        row_dict = dict(zip(headers, row))
        if str(row_dict.get("telegram_user_id")) == str(telegram_id):
            return i + 2, row_dict  # row index in sheet

    return None, None

def update_cell(row, col_name, value):
    headers, _ = get_rows(CUSTOMERS_SHEET)
    col_index = headers.index(col_name)

    col_letter = chr(ord('A') + col_index)

    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{CUSTOMERS_SHEET}!{col_letter}{row}",
        valueInputOption="RAW",
        body={"values": [[value]]}
    ).execute()

def append_customer(telegram_id, username):
    headers, _ = get_rows(CUSTOMERS_SHEET)

    row = [""] * len(headers)

    row[headers.index("telegram_user_id")] = str(telegram_id)
    row[headers.index("telegram_username")] = username
    row[headers.index("created_at")] = now_iso()

    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=CUSTOMERS_SHEET,
        valueInputOption="RAW",
        body={"values": [row]}
    ).execute()

def calculate_points(instagram):
    headers, rows = get_rows(PURCHASES_SHEET)

    total = 0
    last_6m = 0

    now = datetime.utcnow()

    for row in rows:
        row_dict = dict(zip(headers, row))

        if normalize_handle(row_dict.get("instagram_handle", "")) != instagram:
            continue

        if row_dict.get("points_awarded") != "yes":
            continue

        try:
            points = int(float(row_dict.get("NGI", 0)))
        except:
            points = 0

        total += points

        ts = row_dict.get("points_awarded_at")
        if ts:
            dt = datetime.fromisoformat(ts.replace("Z", ""))
            if now - dt <= timedelta(days=180):
                last_6m += points

    return total, last_6m

def determine_tier(points_6m):
    if points_6m >= 10000:
        return "Glassy"
    elif points_6m >= 3000:
        return "Icy"
    elif points_6m >= 1500:
        return "Water"
    else:
        return "Bean"

# ======================
# BOT FLOW
# ======================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    row, customer = find_customer(user.id)

    if not customer:
        append_customer(user.id, user.username)
        await update.message.reply_text(
            "Welcome! Please enter your Instagram handle (without @)."
        )
        context.user_data["state"] = "WAIT_HANDLE"
        return

    if not customer.get("instagram_handle"):
        await update.message.reply_text("Please enter your Instagram handle.")
        context.user_data["state"] = "WAIT_HANDLE"
        return

    if not customer.get("birthday"):
        await update.message.reply_text("Please enter your birthday (DD-MM-YYYY).")
        context.user_data["state"] = "WAIT_BDAY"
        return

    await show_menu(update)

async def show_menu(update):
    await update.message.reply_text(
        "Main Menu:\n- Check Points\n- Change Handle"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user = update.effective_user

    row, customer = find_customer(user.id)

    state = context.user_data.get("state")

    # ================= HANDLE =================
    if state == "WAIT_HANDLE":
        handle = normalize_handle(text)
        update_cell(row, "instagram_handle", handle)
        context.user_data["state"] = None

        await update.message.reply_text(f"Saved as @{handle}")

        if not customer.get("birthday"):
            await update.message.reply_text("Enter birthday (DD-MM-YYYY)")
            context.user_data["state"] = "WAIT_BDAY"
            return

        await show_menu(update)
        return

    # ================= BDAY =================
    if state == "WAIT_BDAY":
        update_cell(row, "birthday", text)
        context.user_data["state"] = None

        await update.message.reply_text("Birthday saved 🎂")
        await show_menu(update)
        return

    # ================= CHECK POINTS =================
    if text.lower() == "check points":
        if not customer.get("instagram_handle"):
            context.user_data["state"] = "WAIT_HANDLE"
            await update.message.reply_text("Enter your Instagram handle.")
            return

        if not customer.get("birthday"):
            context.user_data["state"] = "WAIT_BDAY"
            await update.message.reply_text("Enter your birthday.")
            return

        instagram = normalize_handle(customer["instagram_handle"])

        total, last6 = calculate_points(instagram)
        tier = determine_tier(last6)

        await update.message.reply_text(
            f"Instagram: @{instagram}\n"
            f"Usable points: {total}\n"
            f"Tier: {tier}\n"
            f"Points (6m): {last6}"
        )
        return

    # ================= CHANGE HANDLE =================
    if text.lower() == "change handle":
        context.user_data["state"] = "WAIT_HANDLE"
        await update.message.reply_text("Enter new handle.")
        return

    await update.message.reply_text("Unknown command")

# ======================
# RUN
# ======================

app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

app.run_polling()
