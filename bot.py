import asyncio
import base64
import json
import logging
import os
import uuid
from calendar import monthrange
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

ADMIN_CHAT_ID = int(os.environ["ADMIN_CHAT_ID"].strip())
CONTACT_ADMIN_TEXT = os.environ.get(
    "CONTACT_ADMIN_TEXT",
    "Please contact WLJ admin through your usual WLJ contact channel.",
)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

CUSTOMERS_SHEET = "Customers"
PURCHASES_SHEET = "2026 Purchases"
LEDGER_SHEET = "Ledger"
PACKAGING_RETURNS_SHEET = "Packaging Returns"
REDEMPTIONS_SHEET = "Redemptions"
BIRTHDAY_VOUCHERS_SHEET = "Birthday Vouchers"

REWARD_OPTIONS = {
    50: "$1 voucher",
    100: "$3 voucher",
    500: "$15 voucher",
}

TIER_THRESHOLDS = {
    "Bean": 0,
    "Water": 1500,
    "Icy": 3000,
    "Glassy": 10000,
}

TIER_ORDER = ["Bean", "Water", "Icy", "Glassy"]

PURCHASE_MULTIPLIERS = {
    "Bean": Decimal("1.0"),
    "Water": Decimal("1.5"),
    "Icy": Decimal("2.0"),
    "Glassy": Decimal("3.0"),
}

(
    MENU,
    IG_CAPTURE,
    BIRTHDAY_CAPTURE,
    CHANGE_HANDLE_CAPTURE,
    RETURN_PREFERRED_DATETIME,
    RETURN_POUCH_QTY,
    RETURN_CONFIRM,
) = range(7)

MENU_KEYBOARD = [
    ["Return Packaging", "Check Points"],
    ["Redeem Rewards", "How It Works"],
    ["Change Handle", "Contact Admin"],
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def now_dt() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def make_code(prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"


def make_birthday_code() -> str:
    return f"BIRTHDAY-{datetime.now(timezone.utc).strftime('%Y%m')}-{uuid.uuid4().hex[:6].upper()}"


def normalize_instagram(value: str) -> str:
    return str(value).strip().lower().lstrip("@")


def normalize_telegram_id(value) -> str:
    raw = str(value).strip()
    if raw.endswith(".0"):
        raw = raw[:-2]
    return raw


def parse_int(value: str) -> int:
    return int(str(value).strip())


def parse_amount_to_points(value: str) -> int:
    raw = str(value).strip()
    raw = raw.replace("$", "").replace(",", "").replace("SGD", "").strip()
    try:
        amount = Decimal(raw)
    except InvalidOperation:
        return 0
    if amount < 0:
        return 0
    return int(amount)


def parse_iso_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def parse_birthday_ddmmyyyy(value: str) -> Optional[str]:
    try:
        return datetime.strptime(str(value).strip(), "%d-%m-%Y").strftime("%d-%m-%Y")
    except ValueError:
        return None


def get_tier_name(points_6m: int) -> str:
    if points_6m >= TIER_THRESHOLDS["Glassy"]:
        return "Glassy"
    if points_6m >= TIER_THRESHOLDS["Icy"]:
        return "Icy"
    if points_6m >= TIER_THRESHOLDS["Water"]:
        return "Water"
    return "Bean"


def get_next_tier(points_6m: int) -> Optional[tuple]:
    current = get_tier_name(points_6m)
    current_index = TIER_ORDER.index(current)
    if current_index == len(TIER_ORDER) - 1:
        return None
    next_tier = TIER_ORDER[current_index + 1]
    return next_tier, TIER_THRESHOLDS[next_tier]


def make_progress_bar(current: int, target: int, length: int = 10) -> str:
    if target <= 0:
        return "█" * length
    ratio = max(0, min(current / target, 1))
    filled = int(ratio * length)
    return "█" * filled + "░" * (length - filled)


def end_of_birthday_month(year: int, month: int) -> datetime:
    last_day = monthrange(year, month)[1]
    return datetime(year, month, last_day, 23, 59, 59, tzinfo=timezone.utc)


class SheetsStore:
    def __init__(self, spreadsheet_id: str, service_account_json: str):
        info = json.loads(service_account_json)
        credentials = Credentials.from_service_account_info(info, scopes=SCOPES)
        self.service = build(
            "sheets",
            "v4",
            credentials=credentials,
            cache_discovery=False,
        )
        self.spreadsheet_id = spreadsheet_id

    def read_sheet(self, sheet_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
        result = (
            self.service.spreadsheets()
            .values()
            .get(
                spreadsheetId=self.spreadsheet_id,
                range=f"{sheet_name}!A:Z",
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
                range=f"{sheet_name}!A:Z",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body,
            )
            .execute()
        )

    def update_row_by_key(
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

        for row_index, row in enumerate(rows, start=2):
            current = row.get(key_column, "")
            current = normalize_telegram_id(current) if key_column == "telegram_user_id" else str(current)
            if current == target:
                new_row = [row.get(header, "") for header in headers]
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
                        range=f"{sheet_name}!A{row_index}:Z{row_index}",
                        valueInputOption="USER_ENTERED",
                        body=body,
                    )
                    .execute()
                )
                return True
        return False

    def get_customer_by_telegram_id(self, telegram_user_id: int) -> Optional[Dict[str, str]]:
        target = normalize_telegram_id(telegram_user_id)
        _, rows = self.read_sheet(CUSTOMERS_SHEET)
        for row in rows:
            if normalize_telegram_id(row.get("telegram_user_id", "")) == target:
                return row
        return None

    def get_customer_by_instagram_handle(self, instagram_handle: str) -> Optional[Dict[str, str]]:
        target = normalize_instagram(instagram_handle)
        if not target:
            return None
        _, rows = self.read_sheet(CUSTOMERS_SHEET)
        for row in rows:
            if normalize_instagram(row.get("instagram_handle", "")) == target:
                return row
        return None

    def get_all_customers(self) -> List[Dict[str, str]]:
        _, rows = self.read_sheet(CUSTOMERS_SHEET)
        return rows

    def update_customer_fields(self, telegram_user_id: int, updates: Dict[str, str]) -> None:
        self.update_row_by_key(
            CUSTOMERS_SHEET,
            "telegram_user_id",
            str(telegram_user_id),
            updates,
        )

    def upsert_customer(
        self,
        telegram_user_id: int,
        telegram_username: str,
        instagram_handle: str,
    ) -> None:
        existing = self.get_customer_by_telegram_id(telegram_user_id)
        if existing:
            self.update_customer_fields(
                telegram_user_id,
                {
                    "telegram_username": telegram_username or "",
                    "instagram_handle": instagram_handle,
                    "last_activity_at": utc_now(),
                },
            )
            return

        self.append_row(
            CUSTOMERS_SHEET,
            [
                str(telegram_user_id),        # telegram_user_id
                telegram_username or "",      # telegram_username
                instagram_handle,             # instagram_handle
                "",                           # birthday
                "0",                          # points_balance
                "Bean",                       # tier
                "0",                          # tier_points_6m
                utc_now(),                    # created_at
                utc_now(),                    # last_activity_at
                "",                           # last_synced_at
                "",                           # last_tier_update
            ],
        )

    def get_points_balance(self, telegram_user_id: int) -> int:
        customer = self.get_customer_by_telegram_id(telegram_user_id)
        if not customer:
            return 0
        try:
            return int(str(customer.get("points_balance", "0") or "0"))
        except ValueError:
            return 0

    def set_points_balance(self, telegram_user_id: int, new_balance: int) -> None:
        if new_balance < 0:
            new_balance = 0
        self.update_customer_fields(
            telegram_user_id,
            {
                "points_balance": str(new_balance),
                "last_activity_at": utc_now(),
            },
        )

    def recalculate_points_balance(self, telegram_user_id: int) -> int:
        target = normalize_telegram_id(telegram_user_id)
        _, rows = self.read_sheet(LEDGER_SHEET)

        total = 0
        for row in rows:
            if normalize_telegram_id(row.get("telegram_user_id", "")) != target:
                continue
            status = str(row.get("status", "")).strip().lower()
            expired_flag = str(row.get("expired_flag", "")).strip().lower()
            if status != "approved":
                continue
            if expired_flag == "yes":
                continue
            try:
                total += int(str(row.get("points_change", "0") or "0"))
            except ValueError:
                pass

        if total < 0:
            total = 0

        self.set_points_balance(telegram_user_id, total)
        return total

    def set_customer_last_synced_at(self, telegram_user_id: int, synced_at: str) -> None:
        self.update_customer_fields(
            telegram_user_id,
            {
                "last_synced_at": synced_at,
            },
        )

    def add_ledger_entry(
        self,
        telegram_user_id: int,
        instagram_handle: str,
        tx_type: str,
        reference_code: str,
        points_change: int,
        notes: str,
        status: str = "approved",
        expires_at: str = "",
        expired_flag: str = "no",
    ) -> str:
        tx_id = make_code("TX")
        self.append_row(
            LEDGER_SHEET,
            [
                tx_id,                        # tx_id
                str(telegram_user_id),        # telegram_user_id
                instagram_handle,             # instagram_handle
                tx_type,                      # type
                reference_code,               # reference_code
                str(points_change),           # points_change
                status,                       # status
                notes,                        # notes
                utc_now(),                    # created_at
                expires_at,                   # expires_at
                expired_flag,                 # expired_flag
            ],
        )
        return tx_id

    def add_points(
        self,
        telegram_user_id: int,
        instagram_handle: str,
        points: int,
        tx_type: str,
        reference_code: str,
        notes: str,
        status: str = "approved",
        expires_at: str = "",
        expired_flag: str = "no",
    ) -> int:
        current = self.get_points_balance(telegram_user_id)
        new_balance = current + points
        if new_balance < 0:
            new_balance = 0

        self.set_points_balance(telegram_user_id, new_balance)
        self.add_ledger_entry(
            telegram_user_id=telegram_user_id,
            instagram_handle=instagram_handle,
            tx_type=tx_type,
            reference_code=reference_code,
            points_change=points,
            notes=notes,
            status=status,
            expires_at=expires_at,
            expired_flag=expired_flag,
        )
        return new_balance

    def get_recent_ledger(self, telegram_user_id: int, limit: int = 5) -> List[Dict[str, str]]:
        target = normalize_telegram_id(telegram_user_id)
        _, rows = self.read_sheet(LEDGER_SHEET)
        filtered = [
            row for row in rows
            if normalize_telegram_id(row.get("telegram_user_id", "")) == target
        ]
        return filtered[-limit:]

    def get_tier_points_6m(self, telegram_user_id: int) -> int:
        target = normalize_telegram_id(telegram_user_id)
        _, rows = self.read_sheet(LEDGER_SHEET)

        cutoff = datetime.now(timezone.utc) - timedelta(days=183)
        total = 0

        for row in rows:
            if normalize_telegram_id(row.get("telegram_user_id", "")) != target:
                continue

            if str(row.get("type", "")).strip() != "purchase":
                continue

            created_at = parse_iso_datetime(str(row.get("created_at", "")))
            if not created_at or created_at < cutoff:
                continue

            expired_flag = str(row.get("expired_flag", "")).strip().lower()
            status = str(row.get("status", "")).strip().lower()
            if expired_flag == "yes" or status != "approved":
                continue

            try:
                pts = int(str(row.get("points_change", "0") or "0"))
            except ValueError:
                pts = 0

            if pts > 0:
                total += pts

        return total

    def update_customer_tier(self, telegram_user_id: int) -> tuple:
        points_6m = self.get_tier_points_6m(telegram_user_id)
        tier = get_tier_name(points_6m)

        self.update_customer_fields(
            telegram_user_id,
            {
                "tier": tier,
                "tier_points_6m": str(points_6m),
                "last_tier_update": utc_now(),
            },
        )

        return tier, points_6m

    def sync_purchase_points(self, telegram_user_id: int, instagram_handle: str) -> int:
        _, rows = self.read_sheet(PURCHASES_SHEET)
        total_added = 0

        normalized_ig = normalize_instagram(instagram_handle)

        current_tier, _ = self.update_customer_tier(telegram_user_id)
        multiplier = PURCHASE_MULTIPLIERS[current_tier]

        for row in rows:
            purchase_ig = normalize_instagram(str(row.get("instagram_handle", "")))

            if purchase_ig != normalized_ig:
                continue

            payment_status = str(row.get("payment_status", "")).strip().lower()
            points_awarded = str(row.get("points_awarded", "")).strip().lower()

            if payment_status != "paid":
                continue

            if points_awarded == "yes":
                continue

            purchase_id = str(row.get("purchase_id", "")).strip()
            if not purchase_id:
                continue

            base_points = parse_amount_to_points(row.get("amount_paid", "0"))
            if base_points <= 0:
                continue

            awarded_points = int(
                (Decimal(base_points) * multiplier).to_integral_value(rounding="ROUND_FLOOR")
            )

            expires_at = (now_dt() + timedelta(days=183)).isoformat()

            self.add_points(
                telegram_user_id=telegram_user_id,
                instagram_handle=instagram_handle,
                points=awarded_points,
                tx_type="purchase",
                reference_code=purchase_id,
                notes=f"Purchase {purchase_id} ({current_tier} {multiplier}x)",
                expires_at=expires_at,
                expired_flag="no",
            )

            self.update_row_by_key(
                PURCHASES_SHEET,
                "purchase_id",
                purchase_id,
                {
                    "points_awarded": "yes",
                    "points_awarded_at": utc_now(),
                },
            )

            total_added += awarded_points

            current_tier, _ = self.update_customer_tier(telegram_user_id)
            multiplier = PURCHASE_MULTIPLIERS[current_tier]

        self.set_customer_last_synced_at(telegram_user_id, utc_now())
        self.recalculate_points_balance(telegram_user_id)
        return total_added

    def create_packaging_return(
        self,
        code: str,
        telegram_user_id: int,
        telegram_username: str,
        instagram_handle: str,
        preferred_collection_datetime: str,
        pouch_quantity: int,
    ) -> None:
        self.append_row(
            PACKAGING_RETURNS_SHEET,
            [
                code,                           # return_request_code
                str(telegram_user_id),          # telegram_user_id
                telegram_username or "",        # telegram_username
                instagram_handle,               # instagram_handle
                preferred_collection_datetime,  # preferred_collection_datetime
                str(pouch_quantity),            # pouch_quantity
                str(pouch_quantity),            # points_requested
                "pending",                      # status
                "",                             # admin_notes
                utc_now(),                      # created_at
                "",                             # approved_at
            ],
        )

    def get_packaging_return(self, code: str) -> Optional[Dict[str, str]]:
        _, rows = self.read_sheet(PACKAGING_RETURNS_SHEET)
        for row in rows:
            if str(row.get("return_request_code", "")) == str(code):
                return row
        return None

    def update_packaging_return(self, code: str, updates: Dict[str, str]) -> bool:
        return self.update_row_by_key(
            PACKAGING_RETURNS_SHEET,
            "return_request_code",
            code,
            updates,
        )

    def create_redemption(
        self,
        code: str,
        telegram_user_id: int,
        telegram_username: str,
        instagram_handle: str,
        reward_points: int,
        reward_value: str,
        issued_at: str,
        expires_at: str,
    ) -> None:
        self.append_row(
            REDEMPTIONS_SHEET,
            [
                code,                    # redemption_code
                str(telegram_user_id),   # telegram_user_id
                telegram_username or "", # telegram_username
                instagram_handle,        # instagram_handle
                str(reward_points),      # reward_points
                reward_value,            # reward_value
                issued_at,               # issued_at
                expires_at,              # expires_at
                "active",                # status
                "",                      # used_at
                "no",                    # used_flag
                "no",                    # expired_flag
                "no",                    # notified_flag
                "",                      # notes
            ],
        )

    def get_redemption(self, code: str) -> Optional[Dict[str, str]]:
        _, rows = self.read_sheet(REDEMPTIONS_SHEET)
        for row in rows:
            if str(row.get("redemption_code", "")) == str(code):
                return row
        return None

    def get_all_redemptions(self) -> List[Dict[str, str]]:
        _, rows = self.read_sheet(REDEMPTIONS_SHEET)
        return rows

    def update_redemption(self, code: str, updates: Dict[str, str]) -> bool:
        return self.update_row_by_key(
            REDEMPTIONS_SHEET,
            "redemption_code",
            code,
            updates,
        )

    def create_birthday_voucher(
        self,
        code: str,
        telegram_user_id: int,
        telegram_username: str,
        instagram_handle: str,
        issued_at: str,
        expires_at: str,
        year_issued: str,
    ) -> None:
             self.append_row(
            BIRTHDAY_VOUCHERS_SHEET,
            [
                code,                    # birthday_code
                str(telegram_user_id),   # telegram_user_id
                telegram_username or "", # telegram_username
                instagram_handle,        # instagram_handle
                "$18 off any purchase",  # reward_value
                issued_at,               # issued_at
                expires_at,              # expires_at
                "active",                # status
                "",                      # admin_notes
                year_issued,             # year_issued
                "no",                    # redeemed_flag
                "no",                    # expired_flag
                "",                      # extra field (safety)
            ],
        )
    def get_all_birthday_vouchers(self) -> List[Dict[str, str]]:
        _, rows = self.read_sheet(BIRTHDAY_VOUCHERS_SHEET)
        return rows

    def get_birthday_voucher(self, code: str) -> Optional[Dict[str, str]]:
        _, rows = self.read_sheet(BIRTHDAY_VOUCHERS_SHEET)
        for row in rows:
            if str(row.get("birthday_code", "")) == str(code):
                return row
        return None

    def update_birthday_voucher(self, code: str, updates: Dict[str, str]) -> bool:
        return self.update_row_by_key(
            BIRTHDAY_VOUCHERS_SHEET,
            "birthday_code",
            code,
            updates,
        )


store = SheetsStore(GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON)


def main_menu_markup() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(MENU_KEYBOARD, resize_keyboard=True)


def yes_no_markup() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["Yes", "No"]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


async def show_main_menu(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str = "Welcome to WLJ Rewards Bot.\n\nPlease choose an option.",
) -> int:
    await update.effective_message.reply_text(text, reply_markup=main_menu_markup())
    return MENU


def ensure_instagram_prompt(context: ContextTypes.DEFAULT_TYPE, next_action: str) -> None:
    context.user_data["pending_action"] = next_action


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


async def ask_for_instagram(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    next_action: str,
) -> int:
    ensure_instagram_prompt(context, next_action)
    await update.effective_message.reply_text(
        "Please enter your Instagram handle without the @ symbol.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return IG_CAPTURE


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()

    customer = store.get_customer_by_telegram_id(update.effective_user.id)

    if customer:
        saved_instagram = str(customer.get("instagram_handle", "")).strip()
        saved_birthday = str(customer.get("birthday", "")).strip()

        if saved_instagram and saved_birthday:
            return await show_main_menu(
                update,
                context,
                f"Welcome back! Your saved Instagram handle is @{saved_instagram}.\n\nPlease choose an option.",
            )

        if saved_instagram and not saved_birthday:
            context.user_data["instagram_handle"] = saved_instagram
            await update.effective_message.reply_text(
                "Please enter your birthday in DD-MM-YYYY format.\n\n"
                "Example:\n"
                "14-09-1996\n\n"
                "We use this to issue your WLJ birthday voucher later.",
                reply_markup=ReplyKeyboardRemove(),
            )
            return BIRTHDAY_CAPTURE

    await update.effective_message.reply_text(
        "Welcome to WLJ Family Rewards! I am your friendly WLJ Rewards Bot. Nice to meet you!\n\n"
        "For us to log your purchases backend, please enter your Instagram handle without the @ symbol. "
        "If you are a Tiktok user, you can fill in your Tiktok account username. Instagram handle is preferred.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return IG_CAPTURE


async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    choice = update.message.text.strip()

    if choice == "Return Packaging":
        return await returnpackaging_entry(update, context)
    if choice == "Check Points":
        return await checkpoints_entry(update, context)
    if choice == "Redeem Rewards":
        return await redeemrewards_entry(update, context)
    if choice == "How It Works":
        return await howitworks(update, context)
    if choice == "Contact Admin":
        return await contactadmin(update, context)
    if choice == "Change Handle":
        return await changehandle(update, context)
    if choice == "View My Vouchers":
        return await view_my_vouchers(update, context)

    return await show_main_menu(update, context, "Please choose one of the menu options.")


async def capture_instagram(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    instagram_handle = normalize_instagram(update.message.text)
    user = update.effective_user

    if not instagram_handle:
        await update.message.reply_text("Please enter your Instagram handle without the @ symbol.")
        return IG_CAPTURE

    store.upsert_customer(
        telegram_user_id=user.id,
        telegram_username=user.username or "",
        instagram_handle=instagram_handle,
    )

    context.user_data["instagram_handle"] = instagram_handle

    saved_birthday = get_saved_birthday(user.id)
    if not saved_birthday:
        await update.message.reply_text(
            "Please enter your birthday in DD-MM-YYYY format.\n\n"
            "Example:\n"
            "14-09-1996\n\n"
            "We use this to issue your WLJ birthday voucher later."
        )
        return BIRTHDAY_CAPTURE

    next_action = context.user_data.pop("pending_action", None)

    if next_action == "returnpackaging":
        await update.message.reply_text(
            "Please enter your preferred collection date and time for the coming week.\n\n"
            "Example:\n"
            "Tuesday 7pm\n"
            "or\n"
            "18 Apr 2026, 2pm"
        )
        return RETURN_PREFERRED_DATETIME

    if next_action == "checkpoints":
        return await run_checkpoints(update, context, instagram_handle)

    if next_action == "redeemrewards":
        return await run_redeem_entry(update, context, instagram_handle)

    if next_action == "viewvouchers":
        return await run_view_my_vouchers(update, context)

    return await show_main_menu(
        update,
        context,
        f"Thanks! Your Instagram handle has been saved as @{instagram_handle}.\n\nPlease choose an option.",
    )


async def capture_birthday(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    birthday = parse_birthday_ddmmyyyy(update.message.text)

    if not birthday:
        await update.message.reply_text(
            "Please enter your birthday in DD-MM-YYYY format.\n\n"
            "Example:\n"
            "14-09-1996"
        )
        return BIRTHDAY_CAPTURE

    user = update.effective_user
    store.update_customer_fields(
        user.id,
        {
            "birthday": birthday,
            "last_activity_at": utc_now(),
        },
    )

    next_action = context.user_data.pop("pending_action", None)
    instagram_handle = get_saved_instagram(user.id) or context.user_data.get("instagram_handle", "")

    if next_action == "returnpackaging":
        await update.message.reply_text(
            "Please enter your preferred collection date and time for the coming week.\n\n"
            "Example:\n"
            "Tuesday 7pm\n"
            "or\n"
            "18 Apr 2026, 2pm"
        )
        return RETURN_PREFERRED_DATETIME

    if next_action == "checkpoints":
        return await run_checkpoints(update, context, instagram_handle)

    if next_action == "redeemrewards":
        return await run_redeem_entry(update, context, instagram_handle)

    if next_action == "viewvouchers":
        return await run_view_my_vouchers(update, context)

    return await show_main_menu(
        update,
        context,
        "Thanks! Your birthday has been saved.\n\nPlease choose an option.",
    )


# =========================
# CHECK POINTS
# =========================

async def checkpoints_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id

    instagram_handle = get_saved_instagram(user_id)
    birthday = get_saved_birthday(user_id)

    if not instagram_handle:
        return await ask_for_instagram(update, context, "checkpoints")

    if not birthday:
        context.user_data["pending_action"] = "checkpoints"
        await update.message.reply_text(
            "Please enter your birthday in DD-MM-YYYY format.\n\n"
            "Example:\n"
            "14-09-1996",
            reply_markup=ReplyKeyboardRemove(),
        )
        return BIRTHDAY_CAPTURE

    return await run_checkpoints(update, context, instagram_handle)


async def run_checkpoints(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    instagram_handle: str,
) -> int:
    user = update.effective_user

    synced = store.sync_purchase_points(user.id, instagram_handle)
    expire_old_points_for_user(user.id)
    balance = store.get_points_balance(user.id)
    tier, points_6m = store.update_customer_tier(user.id)
    recent = store.get_recent_ledger(user.id)

    lines = [
        f"Instagram: @{instagram_handle}",
        f"Current usable points: {balance}",
        f"Tier: {tier}",
        f"Points earned in the last 6 months: {points_6m}",
    ]

    next_tier_info = get_next_tier(points_6m)
    if next_tier_info:
        next_tier, next_threshold = next_tier_info
        progress_bar = make_progress_bar(points_6m, next_threshold)
        needed = max(0, next_threshold - points_6m)

        lines.extend([
            "",
            f"Progress to {next_tier}:",
            f"{progress_bar} {points_6m}/{next_threshold}",
            f"You’re only {needed} points away from {next_tier} ✨",
        ])
    else:
        lines.extend([
            "",
            "You are already at the highest tier: Glassy ✨",
        ])

    if synced:
        lines.extend([
            "",
            f"New purchase points synced: {synced}",
        ])

    if recent:
        lines.append("")
        lines.append("Recent activity:")
        for row in recent:
            lines.append(
                f"- {row.get('type')}: {row.get('points_change')} pts ({row.get('reference_code')})"
            )

    return await show_main_menu(update, context, "\n".join(lines))


# =========================
# REDEEM
# =========================

async def redeemrewards_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id

    instagram_handle = get_saved_instagram(user_id)
    birthday = get_saved_birthday(user_id)

    if not instagram_handle:
        return await ask_for_instagram(update, context, "redeemrewards")

    if not birthday:
        context.user_data["pending_action"] = "redeemrewards"
        await update.message.reply_text(
            "Please enter your birthday in DD-MM-YYYY format.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return BIRTHDAY_CAPTURE

    return await run_redeem_entry(update, context, instagram_handle)


async def run_redeem_entry(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    instagram_handle: str,
) -> int:
    user = update.effective_user

    store.sync_purchase_points(user.id, instagram_handle)
    expire_old_points_for_user(user.id)
    balance = store.get_points_balance(user.id)

    eligible = [pts for pts in sorted(REWARD_OPTIONS.keys()) if balance >= pts]

    if not eligible:
        return await show_main_menu(
            update,
            context,
            f"You currently have {balance} point(s), which is not enough to redeem yet.",
        )

    keyboard = [
        [InlineKeyboardButton(f"{pts} pts = {REWARD_OPTIONS[pts]}", callback_data=f"redeem|{pts}")]
        for pts in eligible
    ]

    await update.effective_message.reply_text(
        "Choose your reward:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return MENU


async def redeem_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    _, pts_raw = query.data.split("|")
    pts = int(pts_raw)

    user = query.from_user
    instagram_handle = get_saved_instagram(user.id)

    if not instagram_handle:
        await query.edit_message_text("No saved Instagram handle found. Please restart with /start.")
        return

    expire_old_points_for_user(user.id)
    balance = store.get_points_balance(user.id)

    if balance < pts:
        await query.edit_message_text("Not enough points.")
        return

    code = make_code("RED")
    value = REWARD_OPTIONS[pts]
    issued_at = utc_now()
    expires_at = (now_dt() + timedelta(days=30)).isoformat()

    store.create_redemption(
        code=code,
        telegram_user_id=user.id,
        telegram_username=user.username or "",
        instagram_handle=instagram_handle,
        reward_points=pts,
        reward_value=value,
        issued_at=issued_at,
        expires_at=expires_at,
    )

    new_balance = store.add_points(
        telegram_user_id=user.id,
        instagram_handle=instagram_handle,
        points=-pts,
        tx_type="redemption",
        reference_code=code,
        notes=f"Redeemed {value}",
    )

    await query.edit_message_text(
        f"Voucher issued 🎉\n\n"
        f"Code: {code}\n"
        f"Value: {value}\n"
        f"Valid until: {expires_at[:10]}\n"
        f"Remaining points: {new_balance}"
    )


# =========================
# CHANGE HANDLE
# =========================

async def changehandle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["pending_action"] = "changehandle"
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

    store.update_customer_fields(
        update.effective_user.id,
        {
            "instagram_handle": new_handle,
            "last_activity_at": utc_now(),
        },
    )

    context.user_data.pop("pending_action", None)

    return await show_main_menu(
        update,
        context,
        f"Your handle has been updated to @{new_handle}",
    )


# =========================
# HOW IT WORKS
# =========================

async def howitworks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (
        "✨ WLJ Rewards – How It Works ✨\n\n"
        "🛍️ Earning Points\n"
        "- Every purchase earns points based on the amount spent.\n"
        "- Your tier multiplier increases how many points you earn.\n"
        "- Packaging returns also earn points.\n"
        "- Points expire after 6 months.\n\n"
        "🏆 Membership Tiers\n"
        "- Bean: 0 to 1499 points in the last 6 months\n"
        "- Water: 1500 to 2999 points in the last 6 months\n"
        "- Icy: 3000 to 9999 points in the last 6 months\n"
        "- Glassy: 10000+ points in the last 6 months\n\n"
        "⚡ Tier Multipliers\n"
        "- Bean: Not applicable for multiplier\n"
        "- Water: 50% extra bonus points\n"
        "- Icy: 100% extra bonus points\n"
        "- Glassy: 200% extra bonus points\n\n"
        "♻️ Packaging Returns\n"
        "- 1 embroidered pouch returned = 1 point\n"
        "- Return requests are submitted through the bot and approved by admin\n\n"
        "🎁 Reward Redemptions\n"
        "- 50 points = $1 voucher\n"
        "- 100 points = $3 voucher\n"
        "- 500 points = $15 voucher\n\n"
        "🎂 Birthday Reward\n"
        "- Save your birthday in the bot\n"
        "- On your birthday month, you may receive a special birthday voucher\n\n"
        "The more you shop, the higher your tier, and the faster you earn 💖"
    )
    return await show_main_menu(update, context, text)


# =========================
# CONTACT ADMIN
# =========================

async def contactadmin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await show_main_menu(update, context, CONTACT_ADMIN_TEXT)


# =========================
# VIEW VOUCHERS
# =========================

def get_active_redemptions_for_user(user_id: int) -> List[Dict[str, str]]:
    target = normalize_telegram_id(user_id)
    redemptions = store.get_all_redemptions()
    now = now_dt()

    active = []
    for row in redemptions:
        if normalize_telegram_id(row.get("telegram_user_id", "")) != target:
            continue
        if str(row.get("status", "")).strip().lower() != "active":
            continue
        if str(row.get("redeemed_flag", "")).strip().lower() == "yes":
            continue
        expires_at = parse_iso_datetime(str(row.get("expires_at", "")))
        if expires_at and expires_at < now:
            continue
        active.append(row)
    return active


def get_active_birthday_vouchers_for_user(user_id: int) -> List[Dict[str, str]]:
    target = normalize_telegram_id(user_id)
    vouchers = store.get_all_birthday_vouchers()
    now = now_dt()

    active = []
    for row in vouchers:
        if normalize_telegram_id(row.get("telegram_user_id", "")) != target:
            continue
        if str(row.get("status", "")).strip().lower() != "active":
            continue
        if str(row.get("redeemed_flag", "")).strip().lower() == "yes":
            continue
        expires_at = parse_iso_datetime(str(row.get("expires_at", "")))
        if expires_at and expires_at < now:
            continue
        active.append(row)
    return active


async def view_my_vouchers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    instagram_handle = get_saved_instagram(user_id)
    birthday = get_saved_birthday(user_id)

    if not instagram_handle:
        return await ask_for_instagram(update, context, "viewvouchers")

    if not birthday:
        context.user_data["pending_action"] = "viewvouchers"
        await update.message.reply_text(
            "Please enter your birthday in DD-MM-YYYY format.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return BIRTHDAY_CAPTURE

    return await run_view_my_vouchers(update, context)


async def run_view_my_vouchers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id

    expire_old_redemptions()
    expire_old_birthday_vouchers()

    redemptions = get_active_redemptions_for_user(user_id)
    birthdays = get_active_birthday_vouchers_for_user(user_id)

    if not redemptions and not birthdays:
        return await show_main_menu(
            update,
            context,
            "You do not have any active vouchers right now.",
        )

    lines = ["Your active vouchers:"]

    if birthdays:
        lines.append("")
        lines.append("Birthday vouchers:")
        for row in birthdays:
            lines.append(
                f"- {row.get('birthday_code')} | {row.get('reward_value')} | expires {str(row.get('expires_at', ''))[:10]}"
            )

    if redemptions:
        lines.append("")
        lines.append("Reward vouchers:")
        for row in redemptions:
            lines.append(
                f"- {row.get('redemption_code')} | {row.get('reward_value')} | expires {str(row.get('expires_at', ''))[:10]}"
            )

    return await show_main_menu(update, context, "\n".join(lines))


# =========================
# CANCEL
# =========================

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    return await show_main_menu(update, context, "Action cancelled.")


# =========================
# RETURN PACKAGING
# =========================

async def returnpackaging_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id

    instagram_handle = get_saved_instagram(user_id)
    birthday = get_saved_birthday(user_id)

    if not instagram_handle:
        return await ask_for_instagram(update, context, "returnpackaging")

    if not birthday:
        context.user_data["pending_action"] = "returnpackaging"
        await update.message.reply_text(
            "Please enter your birthday in DD-MM-YYYY format.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return BIRTHDAY_CAPTURE

    context.user_data["instagram_handle"] = instagram_handle

    await update.message.reply_text(
        "Please enter your preferred collection date and time for the coming week.\n\n"
        "Example:\n"
        "Tuesday 7pm\n"
        "or\n"
        "18 Apr 2026, 2pm",
        reply_markup=ReplyKeyboardRemove(),
    )
    return RETURN_PREFERRED_DATETIME


async def return_preferred_datetime(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    preferred_dt = update.message.text.strip()

    if not preferred_dt:
        await update.message.reply_text("Please enter your preferred collection date and time.")
        return RETURN_PREFERRED_DATETIME

    context.user_data["preferred_collection_datetime"] = preferred_dt

    await update.message.reply_text("How many embroidered pouches are you returning?")
    return RETURN_POUCH_QTY


async def return_pouch_qty(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        qty = parse_int(update.message.text)
    except ValueError:
        await update.message.reply_text("Please enter a whole number, such as 1, 2, or 5.")
        return RETURN_POUCH_QTY

    if qty <= 0:
        await update.message.reply_text("Please enter a number greater than 0.")
        return RETURN_POUCH_QTY

    context.user_data["pouch_quantity"] = qty

    instagram_handle = context.user_data.get("instagram_handle", "")
    preferred_dt = context.user_data.get("preferred_collection_datetime", "")

    await update.message.reply_text(
        "Please review your request:\n\n"
        f"Instagram handle: @{instagram_handle}\n"
        f"Preferred collection date and time: {preferred_dt}\n"
        f"Number of embroidered pouches: {qty}\n"
        f"Points to be requested after approval: {qty}\n\n"
        "WLJ will contact you on Instagram with the arranged collection details.\n\n"
        "Important:\n"
        "Please snap a picture of the delivery person when they come to collect the packaging. "
        "You may need this later for approval support.\n\n"
        "Reply Yes to submit or No to cancel.",
        reply_markup=yes_no_markup(),
    )
    return RETURN_CONFIRM


async def return_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    answer = update.message.text.strip().lower()

    if answer not in ["yes", "no"]:
        await update.message.reply_text("Please reply with Yes or No.")
        return RETURN_CONFIRM

    if answer == "no":
        context.user_data.pop("preferred_collection_datetime", None)
        context.user_data.pop("pouch_quantity", None)
        context.user_data.pop("pending_action", None)
        return await show_main_menu(update, context, "Return request cancelled.")

    user = update.effective_user
    instagram_handle = context.user_data.get("instagram_handle") or get_saved_instagram(user.id) or ""
    preferred_dt = context.user_data.get("preferred_collection_datetime", "").strip()
    qty = int(context.user_data["pouch_quantity"])
    code = make_code("RET")

    store.create_packaging_return(
        code=code,
        telegram_user_id=user.id,
        telegram_username=user.username or "",
        instagram_handle=instagram_handle,
        preferred_collection_datetime=preferred_dt,
        pouch_quantity=qty,
    )

    keyboard = InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("Approve", callback_data=f"pr|a|{code}"),
            InlineKeyboardButton("Reject", callback_data=f"pr|r|{code}"),
        ]]
    )

    summary = (
        "Packaging Return Request\n\n"
        f"Request code: {code}\n"
        f"Telegram user ID: {user.id}\n"
    )

    if user.username:
        summary += f"Telegram username: @{user.username}\n"

    summary += (
        f"Instagram: @{instagram_handle}\n"
        f"Preferred collection date/time: {preferred_dt}\n"
        f"Embroidered pouches: {qty}\n"
        f"Points requested: {qty}\n\n"
        "Please arrange collection through Instagram/backend flow."
    )

    await context.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=summary,
        reply_markup=keyboard,
    )

    context.user_data.pop("preferred_collection_datetime", None)
    context.user_data.pop("pouch_quantity", None)
    context.user_data.pop("pending_action", None)

    return await show_main_menu(
        update,
        context,
        "Thank you. Your packaging return request has been submitted.\n\n"
        f"Request code: {code}\n\n"
        "WLJ will contact you on Instagram with the arranged collection details.\n\n"
        "Please remember to snap a picture of the delivery person when they collect the packaging.\n\n"
        "You’re back at the main menu.",
    )


# =========================
# ADMIN ACTIONS
# =========================

async def handle_packaging_admin_action(query, context, action: str, code: str) -> None:
    request_row = store.get_packaging_return(code)
    if not request_row:
        await query.edit_message_text(f"Packaging return {code} not found.")
        return

    if str(request_row.get("status", "")).lower() != "pending":
        await query.edit_message_text(f"Packaging return {code} is already processed.")
        return

    user_id = int(normalize_telegram_id(request_row["telegram_user_id"]))
    instagram_handle = str(request_row.get("instagram_handle", ""))
    qty = int(str(request_row.get("points_requested", "0") or "0"))

    if action == "a":
        expires_at = (now_dt() + timedelta(days=183)).isoformat()

        new_balance = store.add_points(
            telegram_user_id=user_id,
            instagram_handle=instagram_handle,
            points=qty,
            tx_type="packaging_return",
            reference_code=code,
            notes=f"Approved packaging return for {qty} embroidered pouch(es)",
            expires_at=expires_at,
            expired_flag="no",
        )

        store.update_packaging_return(
            code,
            {
                "status": "approved",
                "admin_notes": "Approved in Telegram",
                "approved_at": utc_now(),
            },
        )

        await query.edit_message_text(
            f"Approved packaging return {code}. Added {qty} point(s)."
        )

        await context.bot.send_message(
            chat_id=user_id,
            text=(
                f"Your packaging return request {code} was approved.\n"
                f"Points added: {qty}\n"
                f"Current balance: {new_balance}"
            ),
        )
        return

    store.update_packaging_return(
        code,
        {
            "status": "rejected",
            "admin_notes": "Rejected in Telegram",
        },
    )

    await query.edit_message_text(f"Rejected packaging return {code}.")

    await context.bot.send_message(
        chat_id=user_id,
        text=(
            f"Your packaging return request {code} was rejected.\n"
            "No points were added. Please contact WLJ admin if you need clarification."
        ),
    )


# =========================
# EXPIRY JOBS
# =========================

def expire_old_redemptions() -> None:
    rows = store.get_all_redemptions()
    now = now_dt()

    for row in rows:
        code = str(row.get("redemption_code", "")).strip()
        if not code:
            continue

        status = str(row.get("status", "")).strip().lower()
        expired_flag = str(row.get("expired_flag", "")).strip().lower()
        redeemed_flag = str(row.get("redeemed_flag", "")).strip().lower()
        expires_at = parse_iso_datetime(str(row.get("expires_at", "")))

        if status != "active" or expired_flag == "yes" or redeemed_flag == "yes":
            continue

        if expires_at and expires_at < now:
            store.update_redemption(
                code,
                {
                    "status": "expired",
                    "expired_flag": "yes",
                },
            )


def expire_old_birthday_vouchers() -> None:
    rows = store.get_all_birthday_vouchers()
    now = now_dt()

    for row in rows:
        code = str(row.get("birthday_code", "")).strip()
        if not code:
            continue

        status = str(row.get("status", "")).strip().lower()
        expired_flag = str(row.get("expired_flag", "")).strip().lower()
        expires_at_raw = str(row.get("expires_at", "")).strip()

        if status != "active" or expired_flag == "yes":
            continue

        expires_at = parse_iso_datetime(expires_at_raw)
        if not expires_at:
            continue

        if now > expires_at:
            store.update_birthday_voucher(
                code,
                {
                    "status": "expired",
                    "expired_flag": "yes",
                },
            )


# =========================
# DAILY JOB RUNNER
# =========================

async def run_birthday_voucher_job(context: ContextTypes.DEFAULT_TYPE):
    customers = store.get_all_customers()
    vouchers = store.get_all_birthday_vouchers()

    today = datetime.now(timezone.utc)
    today_str = today.strftime("%d-%m")
    year = today.strftime("%Y")

    for customer in customers:
        user_id = str(customer.get("telegram_user_id", "")).strip()
        birthday = str(customer.get("birthday", "")).strip()
        instagram = str(customer.get("instagram_handle", "")).strip()
        username = str(customer.get("telegram_username", "")).strip()

        if not user_id or not birthday or not instagram:
            continue

        try:
            bday = datetime.strptime(birthday, "%d-%m-%Y")
        except ValueError:
            continue

        if bday.strftime("%d-%m") != today_str:
            continue

        already_issued = any(
            str(v.get("telegram_user_id")) == user_id
            and str(v.get("year_issued")) == year
            for v in vouchers
        )

        if already_issued:
            continue

        code = make_birthday_code()
        expires_at = end_of_birthday_month(today.year, today.month).isoformat()

        store.create_birthday_voucher(
            code=code,
            telegram_user_id=int(user_id),
            telegram_username=username,
            instagram_handle=instagram,
            issued_at=utc_now(),
            expires_at=expires_at,
            year_issued=year,
        )

        try:
            await context.bot.send_message(
                chat_id=int(user_id),
                text=(
                    "🎉 Happy Birthday from WLJ! 🎉\n\n"
                    f"🎟 Voucher Code: {code}\n"
                    "💰 $18 off any purchase\n\n"
                    "Valid until end of this month 💖"
                ),
            )
        except Exception as e:
            logger.error(f"Failed to send birthday message: {e}")


async def daily_jobs(context: ContextTypes.DEFAULT_TYPE):
    try:
        await run_birthday_voucher_job(context)
    except Exception as e:
        logger.error(f"Birthday job failed: {e}")

    try:
        expire_old_birthday_vouchers()
    except Exception as e:
        logger.error(f"Expiry job failed: {e}")


# =========================
# MAIN / HANDLERS
# =========================

async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    data = query.data

    if data.startswith("pr|"):
        try:
            _, action, code = data.split("|", 2)
        except ValueError:
            await query.edit_message_text("Invalid callback payload.")
            return

        await handle_packaging_admin_action(query, context, action, code)
        return

    if data.startswith("redeem|"):
        await redeem_select(update, context)
        return


def main() -> None:
    app = Application.builder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("checkpoints", checkpoints_entry),
            CommandHandler("redeemrewards", redeemrewards_entry),
            CommandHandler("changehandle", changehandle),
            CommandHandler("returnpackaging", returnpackaging_entry),
            CommandHandler("howitworks", howitworks),
            CommandHandler("contactadmin", contactadmin),
        ],
        states={
            MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, menu_handler)],
            IG_CAPTURE: [MessageHandler(filters.TEXT & ~filters.COMMAND, capture_instagram)],
            BIRTHDAY_CAPTURE: [MessageHandler(filters.TEXT & ~filters.COMMAND, capture_birthday)],
            CHANGE_HANDLE_CAPTURE: [MessageHandler(filters.TEXT & ~filters.COMMAND, capture_changed_handle)],
            RETURN_PREFERRED_DATETIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, return_preferred_datetime)],
            RETURN_POUCH_QTY: [MessageHandler(filters.TEXT & ~filters.COMMAND, return_pouch_qty)],
            RETURN_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, return_confirm)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CommandHandler("start", start),
        ],
        allow_reentry=True,
    )

    app.add_handler(conv_handler)

    # Callback buttons
    app.add_handler(CallbackQueryHandler(admin_callback, pattern=r"^(pr\||redeem\|)"))

    # =========================
    # SCHEDULER (IMPORTANT)
    # =========================
    app.job_queue.run_repeating(
        daily_jobs,
        interval=86400,  # every 24h
        first=10
    )

    logger.info("WLJ Rewards bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()
