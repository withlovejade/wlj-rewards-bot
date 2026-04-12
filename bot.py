import asyncio
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

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_CHAT_ID = int(os.environ["ADMIN_CHAT_ID"])
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
import base64

GOOGLE_SERVICE_ACCOUNT_JSON = base64.b64decode(
    os.environ["GOOGLE_SERVICE_ACCOUNT_JSON_B64"]
).decode("utf-8")
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

REWARD_OPTIONS = {
    50: "$1 voucher",
    100: "$3 voucher",
    500: "$15 voucher",
}

(
    MENU,
    IG_CAPTURE,
    RETURN_POUCH_QTY,
    RETURN_CONFIRM,
) = range(4)

MENU_KEYBOARD = [
    ["Return Packaging", "Check Points"],
    ["Redeem Rewards", "How It Works"],
    ["Contact Admin"],
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def make_code(prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"


def normalize_instagram(value: str) -> str:
    return value.strip().lower().lstrip("@")


def parse_int(value: str) -> int:
    return int(str(value).strip())


def parse_amount_to_points(value: str) -> int:
    try:
        amount = Decimal(str(value).strip())
    except InvalidOperation:
        return 0
    if amount < 0:
        return 0
    return int(amount)  # 1 point per whole dollar spent


def parse_iso_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


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

        for row_index, row in enumerate(rows, start=2):
            if row.get(key_column, "") == key_value:
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
        _, rows = self.read_sheet(CUSTOMERS_SHEET)
        for row in rows:
            if row.get("telegram_user_id", "") == str(telegram_user_id):
                return row
        return None

    def get_all_customers(self) -> List[Dict[str, str]]:
        _, rows = self.read_sheet(CUSTOMERS_SHEET)
        return rows

    def upsert_customer(
        self,
        telegram_user_id: int,
        telegram_username: str,
        instagram_handle: str,
    ) -> None:
        existing = self.get_customer_by_telegram_id(telegram_user_id)
        if existing:
            self.update_row_by_key(
                CUSTOMERS_SHEET,
                "telegram_user_id",
                str(telegram_user_id),
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
                "0",
                utc_now(),
                utc_now(),
                "",
            ],
        )

    def get_points_balance(self, telegram_user_id: int) -> int:
        customer = self.get_customer_by_telegram_id(telegram_user_id)
        if not customer:
            return 0
        try:
            return int(customer.get("points_balance", "0") or "0")
        except ValueError:
            return 0

    def set_points_balance(self, telegram_user_id: int, new_balance: int) -> None:
        self.update_row_by_key(
            CUSTOMERS_SHEET,
            "telegram_user_id",
            str(telegram_user_id),
            {
                "points_balance": str(new_balance),
                "last_activity_at": utc_now(),
            },
        )

    def set_customer_last_synced_at(self, telegram_user_id: int, synced_at: str) -> None:
        self.update_row_by_key(
            CUSTOMERS_SHEET,
            "telegram_user_id",
            str(telegram_user_id),
            {
                "last_synced_at": synced_at,
            },
        )

    def add_points(
        self,
        telegram_user_id: int,
        instagram_handle: str,
        points: int,
        tx_type: str,
        reference_code: str,
        notes: str,
        status: str = "approved",
    ) -> int:
        current = self.get_points_balance(telegram_user_id)
        new_balance = current + points
        self.set_points_balance(telegram_user_id, new_balance)
        self.append_row(
            LEDGER_SHEET,
            [
                make_code("TX"),
                str(telegram_user_id),
                instagram_handle,
                tx_type,
                reference_code,
                str(points),
                status,
                notes,
                utc_now(),
            ],
        )
        return new_balance

    def get_recent_ledger(self, telegram_user_id: int, limit: int = 5) -> List[Dict[str, str]]:
        _, rows = self.read_sheet(LEDGER_SHEET)
        filtered = [
            row for row in rows if row.get("telegram_user_id", "") == str(telegram_user_id)
        ]
        return filtered[-limit:]

    def sync_purchase_points(self, telegram_user_id: int, instagram_handle: str) -> int:
        _, rows = self.read_sheet(PURCHASES_SHEET)
        total_added = 0
        normalized_ig = normalize_instagram(instagram_handle)

        for row in rows:
            purchase_ig = normalize_instagram(row.get("instagram_handle", ""))
            if purchase_ig != normalized_ig:
                continue

            payment_status = row.get("payment_status", "").strip().lower()
            points_awarded = row.get("points_awarded", "").strip().lower()

            if payment_status != "paid":
                continue
            if points_awarded == "yes":
                continue

            purchase_id = row.get("purchase_id", "").strip()
            if not purchase_id:
                continue

            points = parse_amount_to_points(row.get("amount_paid", "0"))
            if points <= 0:
                self.update_row_by_key(
                    PURCHASES_SHEET,
                    "purchase_id",
                    purchase_id,
                    {
                        "points_awarded": "yes",
                        "points_awarded_at": utc_now(),
                        "notes": "No points awarded because amount was invalid or zero.",
                    },
                )
                continue

            self.add_points(
                telegram_user_id=telegram_user_id,
                instagram_handle=instagram_handle,
                points=points,
                tx_type="purchase",
                reference_code=purchase_id,
                notes=f"Purchase reward for paid purchase {purchase_id}",
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
            total_added += points

        self.set_customer_last_synced_at(telegram_user_id, utc_now())
        return total_added

    def create_packaging_return(
        self,
        code: str,
        telegram_user_id: int,
        telegram_username: str,
        instagram_handle: str,
        pouch_quantity: int,
    ) -> None:
        self.append_row(
            PACKAGING_RETURNS_SHEET,
            [
                code,
                str(telegram_user_id),
                telegram_username or "",
                instagram_handle,
                str(pouch_quantity),
                str(pouch_quantity),
                "pending",
                "",
                utc_now(),
                "",
            ],
        )

    def get_packaging_return(self, code: str) -> Optional[Dict[str, str]]:
        _, rows = self.read_sheet(PACKAGING_RETURNS_SHEET)
        for row in rows:
            if row.get("return_request_code", "") == code:
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
                code,
                str(telegram_user_id),
                telegram_username or "",
                instagram_handle,
                str(reward_points),
                reward_value,
                issued_at,
                expires_at,
                "active",
                "",
                "no",
                "no",
                "no",
                "",
            ],
        )

    def get_redemption(self, code: str) -> Optional[Dict[str, str]]:
        _, rows = self.read_sheet(REDEMPTIONS_SHEET)
        for row in rows:
            if row.get("redemption_code", "") == code:
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


store = SheetsStore(GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON)


def main_menu_markup() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(MENU_KEYBOARD, resize_keyboard=True)


def yes_no_markup() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([["Yes", "No"]], resize_keyboard=True, one_time_keyboard=True)


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
    if customer and customer.get("instagram_handle", "").strip():
        return customer["instagram_handle"].strip()
    return None


async def ask_for_instagram(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    next_action: str,
) -> int:
    ensure_instagram_prompt(context, next_action)
    await update.effective_message.reply_text(
        "Please enter your Instagram handle.\n"
        "We use this to match your WLJ purchases and contact records.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return IG_CAPTURE


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    return await show_main_menu(update, context)


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

    return await show_main_menu(update, context, "Please choose one of the menu options.")


async def capture_instagram(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    instagram_handle = normalize_instagram(update.message.text)
    user = update.effective_user

    store.upsert_customer(
        telegram_user_id=user.id,
        telegram_username=user.username or "",
        instagram_handle=instagram_handle,
    )

    next_action = context.user_data.get("pending_action")

    if next_action == "returnpackaging":
        await update.message.reply_text("How many embroidered pouches are you returning?")
        return RETURN_POUCH_QTY

    if next_action == "checkpoints":
        return await run_checkpoints(update, context, instagram_handle)

    if next_action == "redeemrewards":
        return await run_redeem_entry(update, context, instagram_handle)

    return await show_main_menu(update, context)


async def returnpackaging_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    instagram_handle = get_saved_instagram(update.effective_user.id)
    if not instagram_handle:
        return await ask_for_instagram(update, context, "returnpackaging")

    await update.effective_message.reply_text(
        "How many embroidered pouches are you returning?",
        reply_markup=ReplyKeyboardRemove(),
    )
    return RETURN_POUCH_QTY


async def checkpoints_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    instagram_handle = get_saved_instagram(update.effective_user.id)
    if not instagram_handle:
        return await ask_for_instagram(update, context, "checkpoints")
    return await run_checkpoints(update, context, instagram_handle)


async def run_checkpoints(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    instagram_handle: str,
) -> int:
    user = update.effective_user
    synced = store.sync_purchase_points(user.id, instagram_handle)
    balance = store.get_points_balance(user.id)
    recent = store.get_recent_ledger(user.id)

    lines = [
        f"Instagram: @{instagram_handle}",
        f"Current points: {balance}",
    ]
    if synced:
        lines.append(f"New purchase points synced just now: {synced}")

    if recent:
        lines.append("")
        lines.append("Recent activity:")
        for row in recent:
            change = row.get("points_change", "0")
            tx_type = row.get("type", "")
            ref = row.get("reference_code", "")
            lines.append(f"- {tx_type}: {change} points ({ref})")

    lines.append("")
    lines.append("Rewards:")
    lines.append("- 50 points = $1 voucher")
    lines.append("- 100 points = $3 voucher")
    lines.append("- 500 points = $15 voucher")

    await update.effective_message.reply_text("\n".join(lines))
    return await show_main_menu(update, context, "You’re back at the main menu.")


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
    await update.message.reply_text(
        f"You are returning {qty} embroidered pouch(es).\n"
        f"That request will be worth {qty} point(s) after admin approval.\n\n"
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
        return await show_main_menu(update, context, "Return request cancelled.")

    user = update.effective_user
    instagram_handle = get_saved_instagram(user.id) or ""
    qty = int(context.user_data["pouch_quantity"])
    code = make_code("RET")

    store.create_packaging_return(
        code=code,
        telegram_user_id=user.id,
        telegram_username=user.username or "",
        instagram_handle=instagram_handle,
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
        f"Embroidered pouches: {qty}\n"
        f"Points requested: {qty}"
    )

    await context.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=summary,
        reply_markup=keyboard,
    )

    return await show_main_menu(
        update,
        context,
        f"Your packaging return request has been submitted.\n\nRequest code: {code}\n\nYou’re back at the main menu.",
    )


async def redeemrewards_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    instagram_handle = get_saved_instagram(update.effective_user.id)
    if not instagram_handle:
        return await ask_for_instagram(update, context, "redeemrewards")
    return await run_redeem_entry(update, context, instagram_handle)


async def run_redeem_entry(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    instagram_handle: str,
) -> int:
    user = update.effective_user
    store.sync_purchase_points(user.id, instagram_handle)
    balance = store.get_points_balance(user.id)

    eligible = [pts for pts in sorted(REWARD_OPTIONS.keys()) if balance >= pts]
    if not eligible:
        await update.effective_message.reply_text(
            f"You currently have {balance} point(s), which is not enough for a reward yet."
        )
        return await show_main_menu(update, context, "You’re back at the main menu.")

    keyboard = [
        [InlineKeyboardButton(f"{pts} points = {REWARD_OPTIONS[pts]}", callback_data=f"redeem|{pts}")]
        for pts in eligible
    ]

    await update.effective_message.reply_text(
        "Choose the reward you want to redeem:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return MENU


async def redeem_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    try:
        _, points_raw = query.data.split("|", 1)
        points = int(points_raw)
    except ValueError:
        await query.edit_message_text("Invalid reward selection.")
        return

    if points not in REWARD_OPTIONS:
        await query.edit_message_text("That reward option is not available.")
        return

    user = query.from_user
    instagram_handle = get_saved_instagram(user.id) or ""

    store.sync_purchase_points(user.id, instagram_handle)
    balance = store.get_points_balance(user.id)

    if balance < points:
        await query.edit_message_text(
            f"You currently have {balance} point(s), which is not enough for this reward."
        )
        return

    reward_value = REWARD_OPTIONS[points]
    code = make_code("RED")
    issued_dt = datetime.now(timezone.utc).replace(microsecond=0)
    expires_dt = issued_dt + timedelta(days=30)

    new_balance = store.add_points(
        telegram_user_id=user.id,
        instagram_handle=instagram_handle,
        points=-points,
        tx_type="redemption",
        reference_code=code,
        notes=f"Voucher issued for {reward_value}. Valid until {expires_dt.date().isoformat()}",
    )

    store.create_redemption(
        code=code,
        telegram_user_id=user.id,
        telegram_username=user.username or "",
        instagram_handle=instagram_handle,
        reward_points=points,
        reward_value=reward_value,
        issued_at=issued_dt.isoformat(),
        expires_at=expires_dt.isoformat(),
    )

    admin_keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Mark Redeemed", callback_data=f"markredeemed|{code}")]]
    )

    await context.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=(
            "Reward Voucher Issued\n\n"
            f"Redemption code: {code}\n"
            f"Telegram user ID: {user.id}\n"
            f"Instagram: @{instagram_handle}\n"
            f"Reward: {reward_value}\n"
            f"Points deducted: {points}\n"
            f"Issued at: {issued_dt.date().isoformat()}\n"
            f"Expires at: {expires_dt.date().isoformat()}"
        ),
        reply_markup=admin_keyboard,
    )

    await query.edit_message_text(
        "Your voucher has been issued.\n\n"
        f"Voucher code: {code}\n"
        f"Reward: {reward_value}\n"
        f"Points deducted: {points}\n"
        f"Current balance: {new_balance}\n"
        f"Valid until: {expires_dt.date().isoformat()}\n\n"
        "Please keep this code safe.\n"
        "Reply /redeemrewards to redeem more points."
    )


async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    try:
        kind, action, code = query.data.split("|", 2)
    except ValueError:
        await query.edit_message_text("Invalid callback payload.")
        return

    if kind == "pr":
        await handle_packaging_admin_action(query, context, action, code)
        return

    if kind == "markredeemed":
        await mark_redeemed(update, context)
        return


async def handle_packaging_admin_action(query, context, action: str, code: str) -> None:
    request_row = store.get_packaging_return(code)
    if not request_row:
        await query.edit_message_text(f"Packaging return {code} not found.")
        return

    if request_row.get("status", "").lower() != "pending":
        await query.edit_message_text(f"Packaging return {code} is already processed.")
        return

    user_id = int(request_row["telegram_user_id"])
    instagram_handle = request_row.get("instagram_handle", "")
    qty = int(request_row.get("points_requested", "0") or "0")

    if action == "a":
        new_balance = store.add_points(
            telegram_user_id=user_id,
            instagram_handle=instagram_handle,
            points=qty,
            tx_type="packaging_return",
            reference_code=code,
            notes=f"Approved packaging return for {qty} embroidered pouch(es)",
        )
        store.update_packaging_return(
            code,
            {
                "status": "approved",
                "admin_notes": "Approved in Telegram",
                "approved_at": utc_now(),
            },
        )
        await query.edit_message_text(f"Approved packaging return {code}. Added {qty} point(s).")
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
        text=f"Your packaging return request {code} was rejected.",
    )


async def mark_redeemed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    try:
        _, code = query.data.split("|", 1)
    except ValueError:
        await query.edit_message_text("Invalid redemption callback.")
        return

    redemption = store.get_redemption(code)
    if not redemption:
        await query.edit_message_text(f"Redemption {code} not found.")
        return

    status = redemption.get("redemption_status", "").strip().lower()
    if status == "redeemed":
        await query.edit_message_text(f"{code} is already marked as redeemed.")
        return
    if status == "expired":
        await query.edit_message_text(f"{code} has already expired.")
        return
    if status == "cancelled":
        await query.edit_message_text(f"{code} has been cancelled.")
        return

    store.update_redemption(
        code,
        {
            "redemption_status": "redeemed",
            "redeemed_at": utc_now(),
            "admin_notes": "Marked redeemed in Telegram.",
        },
    )

    user_id = int(redemption["telegram_user_id"])

    await query.edit_message_text(f"{code} marked as redeemed.")
    await context.bot.send_message(
        chat_id=user_id,
        text=f"Your voucher {code} has been successfully used. Thank you!",
    )


async def process_redemption_reminders_and_expiry(app: Application) -> None:
    rows = store.get_all_redemptions()
    now_dt = datetime.now(timezone.utc)

    for row in rows:
        code = row.get("redemption_code", "")
        if not code:
            continue

        status = row.get("redemption_status", "").strip().lower()
        expired_flag = row.get("expired_flag", "").strip().lower()
        redeemed_at = row.get("redeemed_at", "").strip()

        if status in ["redeemed", "cancelled", "expired"]:
            continue
        if redeemed_at:
            continue

        expires_at = parse_iso_datetime(row.get("expires_at", ""))
        if not expires_at:
            continue

        user_id_raw = row.get("telegram_user_id", "").strip()
        if not user_id_raw:
            continue

        try:
            user_id = int(user_id_raw)
        except ValueError:
            continue

        reward_value = row.get("reward_value", "")
        expires_date = expires_at.date()
        days_left = (expires_date - now_dt.date()).days

        if now_dt >= expires_at and expired_flag != "yes":
            store.update_redemption(
                code,
                {
                    "redemption_status": "expired",
                    "expired_flag": "yes",
                    "admin_notes": "Expired automatically by bot.",
                },
            )
            try:
                await app.bot.send_message(
                    chat_id=user_id,
                    text=(
                        f"Your WLJ voucher {code} for {reward_value} has expired.\n"
                        "Expired vouchers cannot be used."
                    ),
                )
            except Exception as exc:
                logger.warning("Failed to send expiry message for %s: %s", code, exc)
            continue

        if days_left == 7 and row.get("reminder_sent_7d", "").strip().lower() != "yes":
            try:
                await app.bot.send_message(
                    chat_id=user_id,
                    text=(
                        "Hi! 👋\n\n"
                        f"Your WLJ voucher {code} for {reward_value} will expire in 7 days on {expires_date.isoformat()}.\n\n"
                        "Please remember to use it before it expires.\n"
                        "Reply /redeemrewards to redeem more points."
                    ),
                )
                store.update_redemption(code, {"reminder_sent_7d": "yes"})
            except Exception as exc:
                logger.warning("Failed to send 7-day reminder for %s: %s", code, exc)

        if days_left == 1 and row.get("reminder_sent_1d", "").strip().lower() != "yes":
            try:
                await app.bot.send_message(
                    chat_id=user_id,
                    text=(
                        "Reminder: your WLJ voucher is expiring soon.\n\n"
                        f"Voucher code: {code}\n"
                        f"Reward: {reward_value}\n"
                        f"Expiry date: {expires_date.isoformat()}\n\n"
                        "This voucher will expire in 1 day.\n"
                        "Reply /redeemrewards to redeem more points."
                    ),
                )
                store.update_redemption(code, {"reminder_sent_1d": "yes"})
            except Exception as exc:
                logger.warning("Failed to send 1-day reminder for %s: %s", code, exc)


async def process_scheduled_purchase_sync(app: Application) -> None:
    customers = store.get_all_customers()

    for customer in customers:
        telegram_user_id_raw = customer.get("telegram_user_id", "").strip()
        instagram_handle = customer.get("instagram_handle", "").strip()

        if not telegram_user_id_raw or not instagram_handle:
            continue

        try:
            telegram_user_id = int(telegram_user_id_raw)
        except ValueError:
            continue

        try:
            added = store.sync_purchase_points(telegram_user_id, instagram_handle)
            if added > 0:
                new_balance = store.get_points_balance(telegram_user_id)
                try:
                    await app.bot.send_message(
                        chat_id=telegram_user_id,
                        text=(
                            f"Good news! {added} purchase point(s) have just been added to your WLJ Rewards account.\n"
                            f"Current balance: {new_balance}"
                        ),
                    )
                except Exception as exc:
                    logger.warning(
                        "Could not send purchase sync message to user %s: %s",
                        telegram_user_id,
                        exc,
                    )
        except Exception as exc:
            logger.exception(
                "Scheduled purchase sync failed for telegram_user_id=%s instagram=%s: %s",
                telegram_user_id,
                instagram_handle,
                exc,
            )


async def reminder_loop(app: Application) -> None:
    await app.wait_until_running()
    while True:
        try:
            await process_redemption_reminders_and_expiry(app)
        except Exception as exc:
            logger.exception("Reminder loop error: %s", exc)
        await asyncio.sleep(86400)


async def purchase_sync_loop(app: Application) -> None:
    await app.wait_until_running()
    while True:
        try:
            await process_scheduled_purchase_sync(app)
        except Exception as exc:
            logger.exception("Purchase sync loop error: %s", exc)
        await asyncio.sleep(3600)


async def on_startup(app: Application) -> None:
    app.create_task(reminder_loop(app))
    app.create_task(purchase_sync_loop(app))
    logger.info("Reminder loop started.")
    logger.info("Purchase sync loop started.")


async def howitworks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "How WLJ Rewards works:\n\n"
        "- 1 embroidered pouch returned = 1 point\n"
        "- 1 dollar spent on a paid purchase = 1 point\n"
        "- Purchase points are synced automatically from WLJ's paid purchase records\n"
        "- Packaging returns need admin approval\n"
        "- Rewards are issued as unique voucher codes\n"
        "- Voucher redemptions deduct points immediately\n"
        "- Each voucher is valid for 30 days from the points exchange date\n"
        "- Unused vouchers expire automatically after 30 days\n"
        "- The bot sends reminder messages 7 days before expiry and 1 day before expiry\n"
        "- Rewards: 50 points = $1 voucher, 100 points = $3 voucher, 500 points = $15 voucher"
    )
    return await show_main_menu(update, context, "You’re back at the main menu.")


async def contactadmin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(CONTACT_ADMIN_TEXT)
    return await show_main_menu(update, context, "You’re back at the main menu.")


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    return await show_main_menu(update, context, "Action cancelled.")


def main() -> None:
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(False)
        .post_init(on_startup)
        .build()
    )

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("returnpackaging", returnpackaging_entry),
            CommandHandler("checkpoints", checkpoints_entry),
            CommandHandler("redeemrewards", redeemrewards_entry),
            CommandHandler("howitworks", howitworks),
            CommandHandler("contactadmin", contactadmin),
        ],
        states={
            MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, menu_handler)],
            IG_CAPTURE: [MessageHandler(filters.TEXT & ~filters.COMMAND, capture_instagram)],
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
    app.add_handler(CallbackQueryHandler(redeem_select, pattern=r"^redeem\|"))
    app.add_handler(CallbackQueryHandler(mark_redeemed, pattern=r"^markredeemed\|"))
    app.add_handler(CallbackQueryHandler(admin_callback, pattern=r"^pr\|"))

    logger.info("WLJrewardsbot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()
