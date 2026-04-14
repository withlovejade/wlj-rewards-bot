
import os
import json
import base64
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"].strip()
ADMIN_CHAT_ID = int(os.environ["ADMIN_CHAT_ID"].strip())

b64 = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON_B64"].strip().replace("\n", "")
b64 += "=" * (-len(b64) % 4)
GOOGLE_SERVICE_ACCOUNT_JSON = base64.b64decode(b64).decode("utf-8")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
POINTS_SHEET = "2026 Purchases"
LOCAL_STATE_FILE = "local_user_state.json"

MENU, IG_CAPTURE, BIRTHDAY_CAPTURE, CHANGE_HANDLE_CAPTURE = range(4)

MENU_KEYBOARD = [
    ["Check Points", "How It Works"],
    ["Change Handle", "Contact Admin"],
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_instagram(value: str) -> str:
    return str(value).strip().lower().lstrip("@")


def normalize_user_id(value) -> str:
    return str(value).strip()


def parse_birthday_ddmmyyyy(value: str) -> Optional[str]:
    raw = str(value).strip().replace("-", "").replace("/", "").replace(" ", "")
    if len(raw) != 8 or not raw.isdigit():
        return None
    try:
        dt = datetime.strptime(raw, "%d%m%Y")
        return dt.strftime("%d%m%Y")
    except ValueError:
        return None


def birthday_pretty_ddmmyy(value: str) -> str:
    dt = datetime.strptime(value, "%d%m%Y")
    return dt.strftime("%d%m%y")


def date_serial_to_datetime(value) -> Optional[datetime]:
    if value in ("", None):
        return None
    try:
        serial = float(value)
        base = datetime(1899, 12, 30)
        return base + timedelta(days=serial)
    except Exception:
        return None


class LocalState:
    def __init__(self, path: str):
        self.path = path
        self.data = self._load()

    def _load(self) -> Dict[str, Dict[str, str]]:
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

    def get_user(self, telegram_user_id: int) -> Dict[str, str]:
        return self.data.get(normalize_user_id(telegram_user_id), {})

    def set_instagram(self, telegram_user_id: int, instagram_handle: str) -> None:
        key = normalize_user_id(telegram_user_id)
        row = self.data.get(key, {})
        row["instagram_handle"] = instagram_handle
        self.data[key] = row
        self._save()

    def set_birthday(self, telegram_user_id: int, birthday_ddmmyyyy: str) -> None:
        key = normalize_user_id(telegram_user_id)
        row = self.data.get(key, {})
        row["birthday"] = birthday_ddmmyyyy
        row["birthday_request_sent_at"] = utc_now()
        self.data[key] = row
        self._save()

    def get_instagram(self, telegram_user_id: int) -> Optional[str]:
        return self.get_user(telegram_user_id).get("instagram_handle")

    def get_birthday(self, telegram_user_id: int) -> Optional[str]:
        return self.get_user(telegram_user_id).get("birthday")


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
                range=f"{sheet_name}!A:G",
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

    def find_rows_by_instagram(self, instagram_handle: str) -> List[Dict[str, str]]:
        _, rows = self.read_sheet(POINTS_SHEET)
        target = normalize_instagram(instagram_handle)
        return [
            row for row in rows
            if normalize_instagram(row.get("instagram_handle", "")) == target
        ]


state = LocalState(LOCAL_STATE_FILE)
store = SheetsStore(GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON)


def main_menu_markup() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(MENU_KEYBOARD, resize_keyboard=True)


def calculate_points_summary(instagram_handle: str) -> Dict[str, object]:
    rows = store.find_rows_by_instagram(instagram_handle)

    total_usable = 0.0
    expiring_soon = 0.0
    now = datetime.now()

    for row in rows:
        redeem_status = str(row.get("redeem_status", "")).strip().lower()
        expired_flag = str(row.get("expired_flag", "")).strip().lower()

        if redeem_status == "yes":
            continue
        if expired_flag == "yes":
            continue

        try:
            usable_points = float(row.get("usable_points", 0) or 0)
        except Exception:
            usable_points = 0.0

        total_usable += usable_points

        expires_at = date_serial_to_datetime(row.get("expires_at", ""))
        if expires_at is None:
            continue

        if now <= expires_at <= (now + timedelta(days=30)):
            expiring_soon += usable_points

    return {
        "total_usable": round(total_usable, 2),
        "expiring_soon": round(expiring_soon, 2),
        "rows_found": len(rows),
    }


async def show_main_menu(update: Update, text: str = "Please choose an option.") -> int:
    await update.effective_message.reply_text(text, reply_markup=main_menu_markup())
    return MENU


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    saved_instagram = state.get_instagram(user.id)
    saved_birthday = state.get_birthday(user.id)

    if saved_instagram and saved_birthday:
        return await show_main_menu(
            update,
            f"Welcome back! Your saved Instagram handle is @{saved_instagram}.\n\nPlease choose an option.",
        )

    if not saved_instagram:
        await update.effective_message.reply_text(
            "Welcome to WLJ Family Rewards! I am your friendly WLJ Rewards Bot.\n\n"
            "Please enter your Instagram handle without the @ symbol.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return IG_CAPTURE

    await update.effective_message.reply_text(
        "Please enter your birthday in DDMMYYYY format.\n\nExample:\n14091996",
        reply_markup=ReplyKeyboardRemove(),
    )
    return BIRTHDAY_CAPTURE


async def capture_instagram(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    instagram_handle = normalize_instagram(update.message.text)
    user = update.effective_user

    if not instagram_handle:
        await update.message.reply_text("Please enter your Instagram handle without the @ symbol.")
        return IG_CAPTURE

    state.set_instagram(user.id, instagram_handle)

    saved_birthday = state.get_birthday(user.id)
    if saved_birthday:
        return await show_main_menu(
            update,
            f"Thanks! Your Instagram handle has been saved as @{instagram_handle}.\n\nPlease choose an option.",
        )

    await update.message.reply_text(
        "Please enter your birthday in DDMMYYYY format.\n\nExample:\n14091996"
    )
    return BIRTHDAY_CAPTURE


async def send_birthday_request_to_admin(
    context: ContextTypes.DEFAULT_TYPE,
    instagram_handle: str,
    birthday_ddmmyyyy: str,
) -> None:
    code = f"bday|{instagram_handle}|{birthday_ddmmyyyy}"
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Approve", callback_data=f"{code}|approve"),
            InlineKeyboardButton("Reject", callback_data=f"{code}|reject"),
        ]
    ])

    message = (
        f"Birthday voucher request: {instagram_handle}\n"
        f"Birthday on {birthday_pretty_ddmmyy(birthday_ddmmyyyy)}\n"
        "Expire in 1 month"
    )

    await context.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=message,
        reply_markup=keyboard,
    )


async def capture_birthday(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    birthday = parse_birthday_ddmmyyyy(update.message.text)
    if not birthday:
        await update.message.reply_text(
            "Please enter your birthday in DDMMYYYY format.\n\nExample:\n14091996"
        )
        return BIRTHDAY_CAPTURE

    user = update.effective_user
    instagram_handle = state.get_instagram(user.id)
    if not instagram_handle:
        await update.message.reply_text(
            "Please enter your Instagram handle without the @ symbol.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return IG_CAPTURE

    state.set_birthday(user.id, birthday)
    await send_birthday_request_to_admin(context, instagram_handle, birthday)

    return await show_main_menu(
        update,
        "Thanks! Your birthday has been saved.\n\nPlease choose an option."
    )


async def checkpoints_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    instagram_handle = state.get_instagram(user.id)

    if not instagram_handle:
        await update.effective_message.reply_text(
            "Please enter your Instagram handle without the @ symbol.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return IG_CAPTURE

    summary = calculate_points_summary(instagram_handle)

    lines = [
        f"Instagram: @{instagram_handle}",
        f"Current usable points: {summary['total_usable']:.2f}",
    ]

    if summary["rows_found"] == 0:
        lines.append("No points records found yet.")
    elif summary["expiring_soon"] > 0:
        lines.append(f"{summary['expiring_soon']:.2f} points will expire within 30 days.")
    else:
        lines.append("No points are expiring within 30 days.")

    lines.append("Reward points may expire, so please redeem them before expiry.")

    return await show_main_menu(update, "\n".join(lines))


async def howitworks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (
        "✨ WLJ Rewards – How It Works ✨\n\n"
        "🏆 Membership Tiers\n"
        "Bean: 0+ points\n"
        "Water: 1500+ points\n"
        "Icy: 3000+ points\n"
        "Glassy: 10000+ points\n\n"
        "⏳ Reward points may expire, so please redeem them before expiry.\n\n"
        "🎁 Reward claims are handled by WLJ admin/backend."
    )
    return await show_main_menu(update, text)


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

    state.set_instagram(update.effective_user.id, new_handle)
    return await show_main_menu(update, f"Your handle has been updated to @{new_handle}")


async def contactadmin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await show_main_menu(
        update,
        "Please contact WLJ admin through your usual WLJ contact channel."
    )


async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    choice = update.message.text.strip()

    if choice == "Check Points":
        return await checkpoints_entry(update, context)
    if choice == "How It Works":
        return await howitworks(update, context)
    if choice == "Change Handle":
        return await changehandle(update, context)
    if choice == "Contact Admin":
        return await contactadmin(update, context)

    return await show_main_menu(update, "Please choose one of the menu options.")


async def birthday_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    parts = query.data.split("|")
    if len(parts) != 4 or parts[0] != "bday":
        await query.edit_message_text("Invalid birthday action.")
        return

    _, instagram_handle, birthday_ddmmyyyy, action = parts

    if action == "approve":
        await query.edit_message_text(
            f"Approved birthday voucher for {instagram_handle} (Birthday on {birthday_pretty_ddmmyy(birthday_ddmmyyyy)})."
        )
        return

    if action == "reject":
        await query.edit_message_text(
            f"Rejected birthday voucher for {instagram_handle} (Birthday on {birthday_pretty_ddmmyy(birthday_ddmmyyyy)})."
        )
        return


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await show_main_menu(update, "Action cancelled.")


def main() -> None:
    app = Application.builder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start), CommandHandler("checkpoints", checkpoints_entry)],
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
    app.add_handler(CallbackQueryHandler(birthday_admin_callback, pattern=r"^bday\|"))

    logger.info("WLJ ultra-simple bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()
