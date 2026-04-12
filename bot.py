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
                str(telegram_user_id),
                telegram_username or "",
                instagram_handle,
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

    def get_points_balance(self, telegram_user_id: int) -> int:
        customer = self.get_customer_by_telegram_id(telegram_user_id)
        if not customer:
            return 0
        try:
            return int(str(customer.get("points_balance", "0") or "0"))
        except ValueError:
            return 0

    def set_points_balance(self, telegram_user_id: int, new_balance: int) -> None:
        self.update_customer_fields(
            telegram_user_id,
            {
                "points_balance": str(new_balance),
                "last_activity_at": utc_now(),
            },
        )

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
                tx_id,
                str(telegram_user_id),
                instagram_handle,
                tx_type,
                reference_code,
                str(points_change),
                status,
                notes,
                utc_now(),
                expires_at,
                expired_flag,
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

        return total_added
