
import os
import base64
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"].strip()

b64 = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON_B64"].strip().replace("\n", "")
b64 += "=" * (-len(b64) % 4)
GOOGLE_SERVICE_ACCOUNT_JSON = base64.b64decode(b64).decode("utf-8")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

CUSTOMERS_SHEET = "Customers"
LEDGER_SHEET = "Ledger"

MENU, IG_CAPTURE, BIRTHDAY_CAPTURE, CHANGE_HANDLE_CAPTURE = range(4)

MENU_KEYBOARD = [
    ["Check Points", "How It Works"],
    ["Change Handle"],
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_instagram(value: str) -> str:
    return str(value).strip().lower().lstrip("@")


def normalize_telegram_id(value) -> str:
    raw = str(value).strip()
    if raw.endswith(".0"):
        raw = raw[:-2]
    return raw


def parse_birthday_ddmmyyyy(value: str) -> Optional[str]:
    try:
        return datetime.strptime(str(value).strip(), "%d-%m-%Y").strftime("%d-%m-%Y")
    except ValueError:
        return None


class SheetsStore:
    def __init__(self, spreadsheet_id: str, service_account_json: str):
        info = json.loads(service_account_json)
        credentials = Credentials.from_service_account_info(info, scopes=SCOPES)
        self.service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
        self.spreadsheet_id = spreadsheet_id

    def read_sheet(self, sheet_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
        result = (
            self.service.spreadsheets()
            .values()
            .get(
                spreadsheetId=self.spreadsheet_id,
                range=f"{sheet_name}!A:AZ",
                valueRenderOption="UNFORMATTED_VALUE",
            )
            .execute()
        )
        values = result.get("values", [])
        if not values:
            return [], []

        headers = values[0]
        rows = []
        for row in values[1:]:
            row_dict = {}
            for i, header in enumerate(headers):
                row_dict[header] = row[i] if i < len(row) else ""
            rows.append(row_dict)
        return headers, rows

    def append_row(self, sheet_name: str, values: List[str]) -> None:
        body = {"values": [values]}
        (
            self.service.spreadsheets()
            .values()
            .append(
                spreadsheetId=self.spreadsheet_id,
                range=f"{sheet_name}!A:AZ",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body,
            )
            .execute()
        )

    def update_latest_row_by_key(
        self,
        sheet_name: str,
        key_column: str,
        key_value: str,
        updates: Dict[str, str],
    ) -> bool:
        headers, rows = self.read_sheet(sheet_name)
        if not headers:
            return False

        target = normalize_telegram_id(key_value) if key_column == "telegram_user_id" else str(key_value)
        target_row_index = None
        target_row = None

        for row_index, row in enumerate(rows, start=2):
            current = row.get(key_column, "")
            current = normalize_telegram_id(current) if key_column == "telegram_user_id" else str(current)
            if current == target:
                target_row_index = row_index
                target_row = row

        if target_row_index is None or target_row is None:
            return False

        new_row = [target_row.get(header, "") for header in headers]
        for col_name, new_value in updates.items():
            if col_name in headers:
                idx = headers.index(col_name)
                new_row[idx] = str(new_value)

        body = {"values": [new_row]}
        (
            self.service.spreadsheets()
            .values()
            .update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{sheet_name}!A{target_row_index}:AZ{target_row_index}",
                valueInputOption="USER_ENTERED",
                body=body,
            )
            .execute()
        )
        return True

    def get_customer_by_telegram_id(self, telegram_user_id: int) -> Optional[Dict[str, str]]:
        target = normalize_telegram_id(telegram_user_id)
        _, rows = self.read_sheet(CUSTOMERS_SHEET)
        matches = [
            row for row in rows
            if normalize_telegram_id(row.get("telegram_user_id", "")) == target
        ]
        if not matches:
            return None
        return matches[-1]

    def ensure_customer_exists(self, telegram_user_id: int, telegram_username: str = "") -> None:
        existing = self.get_customer_by_telegram_id(telegram_user_id)
        if existing:
            return
        self.append_row(
            CUSTOMERS_SHEET,
            [
                str(telegram_user_id),
                telegram_username or "",
                "",
                "",
                "0",
                "Bean",
                "0",
                utc_now(),
                utc_now(),
                "",
                "",
            ],
        )

    def upsert_instagram(self, telegram_user_id: int, telegram_username: str, instagram_handle: str) -> None:
        self.ensure_customer_exists(telegram_user_id, telegram_username)
        self.update_latest_row_by_key(
            CUSTOMERS_SHEET,
            "telegram_user_id",
            str(telegram_user_id),
            {
                "telegram_username": telegram_username or "",
                "instagram_handle": instagram_handle,
                "last_activity_at": utc_now(),
            },
        )

    def upsert_birthday(self, telegram_user_id: int, telegram_username: str, birthday: str) -> None:
        self.ensure_customer_exists(telegram_user_id, telegram_username)
        self.update_latest_row_by_key(
            CUSTOMERS_SHEET,
            "telegram_user_id",
            str(telegram_user_id),
            {
                "telegram_username": telegram_username or "",
                "birthday": birthday,
                "last_activity_at": utc_now(),
            },
        )

    def get_points_balance(self, telegram_user_id: int) -> int:
        target = normalize_telegram_id(telegram_user_id)
        _, rows = self.read_sheet(LEDGER_SHEET)
        total = 0

        for row in rows:
            if normalize_telegram_id(row.get("telegram_user_id", "")) != target:
                continue
            try:
                total += int(str(row.get("points_change", "0") or "0"))
            except ValueError:
                continue

        return max(total, 0)


store = SheetsStore(GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON)


def main_menu_markup() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(MENU_KEYBOARD, resize_keyboard=True)


def get_saved_instagram(user_id: int) -> Optional[str]:
    customer = store.get_customer_by_telegram_id(user_id)
    if customer and str(customer.get("instagram_handle", "")).strip():
        return str(customer.get("instagram_handle", "")).strip()
    return None


def get_saved_birthday(user_id: int) -> Optional[str]:
    customer = store.get_customer_by_telegram_id(user_id)
    if customer and str(customer.get("birthday", "")).strip():
        return str(customer.get("birthday", "")).strip()
    return None


async def show_main_menu(update: Update, text: str = "Please choose an option.") -> int:
    await update.effective_message.reply_text(text, reply_markup=main_menu_markup())
    return MENU


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    user = update.effective_user
    store.ensure_customer_exists(user.id, user.username or "")

    saved_instagram = get_saved_instagram(user.id)
    saved_birthday = get_saved_birthday(user.id)

    if saved_instagram and saved_birthday:
        return await show_main_menu(
            update,
            f"Welcome back! Your saved Instagram handle is @{saved_instagram}.\n\nPlease choose an option.",
        )

    if not saved_instagram:
        await update.effective_message.reply_text(
            "Welcome to WLJ Family Rewards! I am your friendly WLJ Rewards Bot.\n\n"
            "Please enter your Instagram handle without the @ symbol. "
            "If you are a Tiktok user, you can fill in your Tiktok username. Instagram is preferred.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return IG_CAPTURE

    await update.effective_message.reply_text(
        "Please enter your birthday in DD-MM-YYYY format.\n\nExample:\n14-09-1996",
        reply_markup=ReplyKeyboardRemove(),
    )
    return BIRTHDAY_CAPTURE


async def capture_instagram(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    instagram_handle = normalize_instagram(update.message.text)
    user = update.effective_user

    if not instagram_handle:
        await update.message.reply_text("Please enter your Instagram handle without the @ symbol.")
        return IG_CAPTURE

    store.upsert_instagram(user.id, user.username or "", instagram_handle)

    saved_birthday = get_saved_birthday(user.id)
    if saved_birthday:
        return await show_main_menu(
            update,
            f"Thanks! Your Instagram handle has been saved as @{instagram_handle}.\n\nPlease choose an option.",
        )

    await update.message.reply_text(
        "Please enter your birthday in DD-MM-YYYY format.\n\nExample:\n14-09-1996"
    )
    return BIRTHDAY_CAPTURE


async def capture_birthday(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    birthday = parse_birthday_ddmmyyyy(update.message.text)
    if not birthday:
        await update.message.reply_text(
            "Please enter your birthday in DD-MM-YYYY format.\n\nExample:\n14-09-1996"
        )
        return BIRTHDAY_CAPTURE

    user = update.effective_user
    store.upsert_birthday(user.id, user.username or "", birthday)
    return await show_main_menu(update, "Thanks! Your birthday has been saved.\n\nPlease choose an option.")


async def changehandle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "Enter your new Instagram handle (without @):",
        reply_markup=ReplyKeyboardRemove(),
    )
    return CHANGE_HANDLE_CAPTURE


async def capture_changed_handle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    new_handle = normalize_instagram(update.message.text)
    if not new_handle:
        await update.message.reply_text("Please enter a valid Instagram handle without @.")
        return CHANGE_HANDLE_CAPTURE

    user = update.effective_user
    store.upsert_instagram(user.id, user.username or "", new_handle)
    return await show_main_menu(update, f"Your handle has been updated to @{new_handle}")


async def checkpoints_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    store.ensure_customer_exists(user.id, user.username or "")
    balance = store.get_points_balance(user.id)

    customer = store.get_customer_by_telegram_id(user.id)
    instagram_handle = ""
    if customer:
        instagram_handle = str(customer.get("instagram_handle", "")).strip()

    text = f"Current usable points: {balance}"
    if instagram_handle:
        text = f"Instagram: @{instagram_handle}\n" + text

    return await show_main_menu(update, text)


async def howitworks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (
        "✨ WLJ Rewards – How It Works ✨\n\n"
        "🏆 Membership Tiers\n"
        "Bean: 0+ points\n"
        "Water: 1500+ points\n"
        "Icy: 3000+ points\n"
        "Glassy: 10000+ points\n\n"
        "🎁 Rewards\n"
        "Use your points to redeem rewards later."
    )
    return await show_main_menu(update, text)


async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    choice = update.message.text.strip()

    if choice == "Check Points":
        return await checkpoints_entry(update, context)
    if choice == "How It Works":
        return await howitworks(update, context)
    if choice == "Change Handle":
        return await changehandle(update, context)

    return await show_main_menu(update, "Please choose one of the menu options.")


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    return await show_main_menu(update, "Action cancelled.")


def main() -> None:
    app = Application.builder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("checkpoints", checkpoints_entry),
        ],
        states={
            MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, menu_handler)],
            IG_CAPTURE: [MessageHandler(filters.TEXT & ~filters.COMMAND, capture_instagram)],
            BIRTHDAY_CAPTURE: [MessageHandler(filters.TEXT & ~filters.COMMAND, capture_birthday)],
            CHANGE_HANDLE_CAPTURE: [MessageHandler(filters.TEXT & ~filters.COMMAND, capture_changed_handle)],
        },
        fallbacks=[CommandHandler("cancel", cancel), CommandHandler("start", start)],
        allow_reentry=True,
    )

    app.add_handler(conv_handler)
    logger.info("WLJ no-loop checkpoints bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()
