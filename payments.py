import os
import logging
import uuid
import asyncio
import hashlib
import time
import re as _re
from datetime import datetime, timedelta
from typing import Optional, Dict
from dotenv import load_dotenv

import aiohttp
from aiogram import Router, F, Bot
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode

# База данных
try:
    from database import (
        save_deposit, save_withdrawal, update_user_info,
        db_get_all_users, db_set_balance, db_update_field, db_get_user
    )
except ImportError:
    async def save_deposit(user_id, amount, crypto_invoice_id): pass
    async def save_withdrawal(user_id, amount): pass
    async def update_user_info(user_id, **kwargs): pass
    def db_get_all_users(): return []
    def db_set_balance(user_id, amount): pass
    def db_update_field(user_id, field, value): pass
    def db_get_user(user_id): return {}

load_dotenv()

# Настройки Cryptobot
CRYPTO_BOT_TOKEN = os.getenv('CRYPTO_BOT_TOKEN')
CRYPTOBOT_API_URL = "https://pay.crypt.bot/api"

# Минимальные суммы
MIN_DEPOSIT = 0.1
MIN_WITHDRAWAL = 2.0

# Задержка между выводами (3 минуты)
WITHDRAWAL_COOLDOWN = 180

# Время жизни счета (5 минут)
INVOICE_LIFETIME = 300

# Эмодзи
EMOJI_BACK    = "5906771962734057347"
EMOJI_LINK    = "5271604874419647061"

payment_router = Router()
bot: Bot = None

# Функции владельца сообщений — инжектируются из main.py
set_owner_fn = None
is_owner_fn  = None


# ========== ХРАНИЛИЩЕ ==========
class Storage:
    def __init__(self):
        self.users: Dict[int, dict] = {}
        self.invoices: Dict[str, dict] = {}
        self.check_tasks: Dict[str, asyncio.Task] = {}
        self.pending_action: Dict[int, str] = {}

        # ── Защита от дублей ──────────────────────────────────────────────────
        self._paid_crypto_ids: set = set()
        self._processed_invoices: set = set()
        self._user_locks: Dict[int, asyncio.Lock] = {}
        self._deposit_requests: Dict[str, float] = {}
        self._withdraw_requests: Dict[str, float] = {}
        self._balance_lock = asyncio.Lock()

        # ── Загружаем всех пользователей из SQLite при старте ─────────────────
        self._load_from_db()

    def _load_from_db(self):
        """Загружает всех пользователей из SQLite в память при запуске бота."""
        try:
            rows = db_get_all_users()
            for row in rows:
                uid = int(row["user_id"])
                self.users[uid] = {
                    'balance':           float(row.get("balance", 0.0) or 0.0),
                    'first_name':        row.get("first_name", "") or "",
                    'username':          row.get("username", "") or "",
                    'last_withdrawal':   None,  # храним в памяти как datetime
                    'total_deposits':    float(row.get("total_deposits", 0.0) or 0.0),
                    'total_withdrawals': float(row.get("total_withdrawals", 0.0) or 0.0),
                    'join_date':         row.get("join_date", datetime.now().strftime('%Y-%m-%d')),
                }
            logging.info(f"[Storage] Загружено пользователей из БД: {len(self.users)}")
        except Exception as e:
            logging.error(f"[Storage] Ошибка загрузки из БД: {e}")

    def _save_balance_to_db(self, user_id: int):
        """Синхронно сохраняет текущий баланс пользователя в SQLite."""
        try:
            user = self.users.get(user_id)
            if user is None:
                return
            db_set_balance(user_id, user['balance'])
        except Exception as e:
            logging.error(f"[Storage] Ошибка сохранения баланса user={user_id}: {e}")

    # ── Блокировки на пользователя ────────────────────────────────────────────
    def get_user_lock(self, user_id: int) -> asyncio.Lock:
        if user_id not in self._user_locks:
            self._user_locks[user_id] = asyncio.Lock()
        return self._user_locks[user_id]

    # ── Защита от повторного зачисления одного и того же счёта Cryptobot ──────
    def is_crypto_invoice_paid(self, crypto_invoice_id: int) -> bool:
        return crypto_invoice_id in self._paid_crypto_ids

    def mark_crypto_invoice_paid(self, crypto_invoice_id: int):
        self._paid_crypto_ids.add(crypto_invoice_id)

    # ── Защита от повторной обработки внутреннего invoice ─────────────────────
    def is_invoice_processed(self, invoice_id: str) -> bool:
        return invoice_id in self._processed_invoices

    def mark_invoice_processed(self, invoice_id: str):
        self._processed_invoices.add(invoice_id)

    # ── Дедупликация быстрых повторных запросов (двойное нажатие) ─────────────
    def _request_key(self, user_id: int, amount: float, action: str) -> str:
        window = int(time.time() // 10)
        raw = f"{action}:{user_id}:{amount:.4f}:{window}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def is_duplicate_request(self, user_id: int, amount: float, action: str) -> bool:
        key = self._request_key(user_id, amount, action)
        now = time.time()
        expired = [k for k, t in self._deposit_requests.items() if now - t > 30]
        for k in expired:
            self._deposit_requests.pop(k, None)
            self._withdraw_requests.pop(k, None)

        store = self._deposit_requests if action == 'deposit' else self._withdraw_requests
        if key in store:
            return True
        store[key] = now
        return False

    def set_pending(self, user_id: int, action: str):
        self.pending_action[user_id] = action

    def get_pending(self, user_id: int) -> Optional[str]:
        return self.pending_action.get(user_id)

    def clear_pending(self, user_id: int):
        self.pending_action.pop(user_id, None)

    def get_user(self, user_id: int) -> dict:
        if user_id not in self.users:
            # Пробуем загрузить из БД (вдруг пользователь там уже есть)
            try:
                row = db_get_user(user_id)
                self.users[user_id] = {
                    'balance':           float(row.get("balance", 0.0) or 0.0),
                    'first_name':        row.get("first_name", "") or "",
                    'username':          row.get("username", "") or "",
                    'last_withdrawal':   None,
                    'total_deposits':    float(row.get("total_deposits", 0.0) or 0.0),
                    'total_withdrawals': float(row.get("total_withdrawals", 0.0) or 0.0),
                    'join_date':         row.get("join_date", datetime.now().strftime('%Y-%m-%d')),
                }
            except Exception:
                self.users[user_id] = {
                    'balance':           0.0,
                    'first_name':        '',
                    'username':          '',
                    'last_withdrawal':   None,
                    'total_deposits':    0.0,
                    'total_withdrawals': 0.0,
                    'join_date':         datetime.now().strftime('%Y-%m-%d'),
                }
        return self.users[user_id]

    def get_balance(self, user_id: int) -> float:
        return float(self.get_user(user_id).get('balance', 0.0))

    # ── Изменение баланса БЕЗ влияния на статистику депозитов/выводов ─────────
    def add_balance(self, user_id: int, amount: float):
        """Просто пополняет баланс. НЕ считается депозитом."""
        user = self.get_user(user_id)
        user['balance'] = round(user['balance'] + float(amount), 8)
        self._save_balance_to_db(user_id)

    def deduct_balance(self, user_id: int, amount: float) -> bool:
        """Просто списывает баланс. НЕ считается выводом."""
        user = self.get_user(user_id)
        if user['balance'] >= float(amount):
            user['balance'] = round(user['balance'] - float(amount), 8)
            self._save_balance_to_db(user_id)
            return True
        return False

    # ── Реальные депозиты и выводы через Cryptobot ────────────────────────────
    def record_deposit(self, user_id: int, amount: float, crypto_invoice_id: int) -> bool:
        """
        Вызывается ТОЛЬКО когда Cryptobot подтвердил оплату.
        Возвращает False если счёт уже был зачислен (дюп).
        """
        if self.is_crypto_invoice_paid(crypto_invoice_id):
            logging.warning(
                f"[DUPE] Попытка повторного зачисления crypto_invoice_id={crypto_invoice_id} "
                f"для user_id={user_id}, сумма={amount}"
            )
            return False
        self.mark_crypto_invoice_paid(crypto_invoice_id)
        user = self.get_user(user_id)
        user['balance'] = round(user['balance'] + float(amount), 8)
        user['total_deposits'] = round(user.get('total_deposits', 0.0) + float(amount), 8)
        self._save_balance_to_db(user_id)
        try:
            db_update_field(user_id, "total_deposits", user['total_deposits'])
        except Exception as e:
            logging.error(f"[Storage] Ошибка сохранения total_deposits: {e}")
        return True

    def record_withdrawal(self, user_id: int, amount: float) -> bool:
        """Вызывается ТОЛЬКО при успешном выводе через Cryptobot."""
        user = self.get_user(user_id)
        if user['balance'] >= float(amount):
            user['balance'] = round(user['balance'] - float(amount), 8)
            user['total_withdrawals'] = round(user.get('total_withdrawals', 0.0) + float(amount), 8)
            self._save_balance_to_db(user_id)
            try:
                db_update_field(user_id, "total_withdrawals", user['total_withdrawals'])
            except Exception as e:
                logging.error(f"[Storage] Ошибка сохранения total_withdrawals: {e}")
            return True
        return False

    def rollback_withdrawal(self, user_id: int, amount: float):
        """
        Откатывает record_withdrawal — возвращает баланс И уменьшает total_withdrawals.
        Вызывать только если чек Cryptobot не был создан после record_withdrawal.
        """
        user = self.get_user(user_id)
        user['balance'] = round(user['balance'] + float(amount), 8)
        user['total_withdrawals'] = max(
            0.0,
            round(user.get('total_withdrawals', 0.0) - float(amount), 8)
        )
        self._save_balance_to_db(user_id)
        try:
            db_update_field(user_id, "total_withdrawals", user['total_withdrawals'])
        except Exception as e:
            logging.error(f"[Storage] Ошибка отката total_withdrawals user={user_id}: {e}")
        logging.info(
            f"[ROLLBACK] Откат вывода user={user_id}, amount={amount}, "
            f"новый баланс={user['balance']}, total_withdrawals={user['total_withdrawals']}"
        )

    def can_withdraw(self, user_id: int) -> tuple:
        user = self.get_user(user_id)
        last = user.get('last_withdrawal')
        if not last:
            return True, None
        seconds = (datetime.now() - last).total_seconds()
        if seconds < WITHDRAWAL_COOLDOWN:
            return False, int(WITHDRAWAL_COOLDOWN - seconds)
        return True, None

    def set_last_withdrawal(self, user_id: int):
        self.get_user(user_id)['last_withdrawal'] = datetime.now()
        try:
            db_update_field(user_id, "last_withdrawal", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        except Exception as e:
            logging.error(f"[Storage] Ошибка сохранения last_withdrawal: {e}")

    def create_invoice(self, user_id: int, amount: float, crypto_id: int, pay_url: str) -> str:
        invoice_id = str(uuid.uuid4())
        expires_at = datetime.now() + timedelta(seconds=INVOICE_LIFETIME)
        self.invoices[invoice_id] = {
            'user_id': user_id,
            'amount': amount,
            'crypto_id': crypto_id,
            'pay_url': pay_url,
            'expires_at': expires_at,
            'status': 'pending',
            'message_id': None,
            'chat_id': None
        }
        return invoice_id

    def get_invoice(self, invoice_id: str) -> Optional[dict]:
        return self.invoices.get(invoice_id)

    def update_invoice_status(self, invoice_id: str, status: str):
        if invoice_id in self.invoices:
            self.invoices[invoice_id]['status'] = status

    def set_message_info(self, invoice_id: str, chat_id: int, message_id: int):
        if invoice_id in self.invoices:
            self.invoices[invoice_id]['chat_id'] = chat_id
            self.invoices[invoice_id]['message_id'] = message_id
            logging.info(f"[{invoice_id}] set_message_info: chat_id={chat_id}, message_id={message_id}")


storage = Storage()


# ========== ВСПОМОГАТЕЛЬНАЯ КНОПКА "В профиль" ==========
def btn_back_profile() -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text="Назад",
        callback_data="profile",
        icon_custom_emoji_id=EMOJI_BACK
    )

def kb_back_profile() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[btn_back_profile()]])


# ========== API CRYPTOBOT ==========
class CryptoBotAPI:
    def __init__(self, token: str):
        self.token = token
        self.headers = {"Crypto-Pay-API-Token": token}

    async def create_invoice(self, amount: float) -> Optional[dict]:
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.post(
                    f"{CRYPTOBOT_API_URL}/createInvoice",
                    headers=self.headers,
                    json={
                        "asset": "USDT",
                        "amount": str(amount),
                        "expires_in": INVOICE_LIFETIME
                    }
                )
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('result') if data.get('ok') else None
            except Exception as e:
                logging.error(f"Ошибка создания счета: {e}")
            return None

    async def get_invoice_status(self, invoice_id: int) -> Optional[str]:
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.post(
                    f"{CRYPTOBOT_API_URL}/getInvoices",
                    headers=self.headers,
                    json={"invoice_ids": [invoice_id]}
                )
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('ok') and data.get('result', {}).get('items'):
                        return data['result']['items'][0].get('status')
            except Exception as e:
                logging.error(f"Ошибка проверки статуса: {e}")
            return None

    async def create_check(self, amount: float, user_id: int) -> Optional[dict]:
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.post(
                    f"{CRYPTOBOT_API_URL}/createCheck",
                    headers=self.headers,
                    json={
                        "asset": "USDT",
                        "amount": str(amount),
                        "pin_to_user_id": str(user_id)
                    }
                )
                data = await resp.json()
                logging.info(f"createCheck response (status={resp.status}): {data}")
                if resp.status == 200 and data.get("ok"):
                    return data.get("result")
                else:
                    logging.error(f"createCheck error: {data}")
                    return None
            except Exception as e:
                logging.error(f"Ошибка создания чека: {e}")
            return None


crypto_api = CryptoBotAPI(CRYPTOBOT_API_KEY)


# ========== ФОНОВАЯ ПРОВЕРКА ОПЛАТЫ ==========
async def check_payment_task(invoice_id: str):
    try:
        for wait in range(10):
            await asyncio.sleep(1)
            invoice = storage.get_invoice(invoice_id)
            if invoice and invoice.get('chat_id') and invoice.get('message_id'):
                logging.info(f"[{invoice_id}] message_id получен за {wait+1} сек")
                break
        else:
            logging.error(f"[{invoice_id}] chat_id/message_id не появились за 10 сек")

        for attempt in range(150):
            invoice = storage.get_invoice(invoice_id)
            if not invoice:
                return

            if storage.is_invoice_processed(invoice_id):
                logging.warning(f"[{invoice_id}] Уже обработан, выходим из задачи")
                return

            if datetime.now() > invoice['expires_at']:
                logging.info(f"[{invoice_id}] Счет истек на попытке {attempt}")
                storage.mark_invoice_processed(invoice_id)
                if invoice.get('chat_id') and invoice.get('message_id'):
                    try:
                        await bot.edit_message_text(
                            text=(
                                f'<blockquote>❌ <b>Счет истек</b></blockquote>\n\n'
                                f'<blockquote>Время оплаты вышло. Попробуйте снова.</blockquote>'
                            ),
                            parse_mode=ParseMode.HTML,
                            chat_id=invoice['chat_id'],
                            message_id=invoice['message_id'],
                            reply_markup=kb_back_profile()
                        )
                    except Exception as e:
                        logging.error(f"[{invoice_id}] Ошибка edit (expired): {e}")
                storage.update_invoice_status(invoice_id, 'expired')
                return

            status = await crypto_api.get_invoice_status(invoice['crypto_id'])
            logging.info(f"[{invoice_id}] Попытка {attempt+1}: статус={status}")

            if status == 'paid':
                user_lock = storage.get_user_lock(invoice['user_id'])
                async with user_lock:
                    if storage.is_invoice_processed(invoice_id):
                        logging.warning(f"[{invoice_id}] Дюп: invoice уже обработан внутри локера")
                        return
                    if storage.is_crypto_invoice_paid(invoice['crypto_id']):
                        logging.warning(f"[{invoice_id}] Дюп: crypto_id={invoice['crypto_id']} уже зачислен")
                        storage.mark_invoice_processed(invoice_id)
                        storage.update_invoice_status(invoice_id, 'paid')
                        return

                    credited = storage.record_deposit(
                        invoice['user_id'],
                        invoice['amount'],
                        invoice['crypto_id']
                    )
                    storage.mark_invoice_processed(invoice_id)
                    storage.update_invoice_status(invoice_id, 'paid')

                if credited:
                    logging.info(
                        f"[{invoice_id}] ОПЛАЧЕН — начислено {invoice['amount']} USDT "
                        f"пользователю {invoice['user_id']}"
                    )
                    asyncio.create_task(save_deposit(
                        invoice['user_id'],
                        invoice['amount'],
                        invoice['crypto_id']
                    ))
                    # Записываем в лидеры по дням (чтобы фильтр по периоду работал)
                    try:
                        from leaders import record_deposit_stat
                        user_data = storage.get_user(invoice['user_id'])
                        user_name = (
                            user_data.get('first_name')
                            or user_data.get('username')
                            or f"User {invoice['user_id']}"
                        )
                        record_deposit_stat(invoice['user_id'], user_name, invoice['amount'])
                    except Exception as _le:
                        logging.error(f"[Leaders] record_deposit_stat error: {_le}")
                else:
                    logging.warning(
                        f"[{invoice_id}] ОПЛАЧЕН но зачисление отклонено (дюп) — "
                        f"crypto_id={invoice['crypto_id']}"
                    )

                if invoice.get('chat_id') and invoice.get('message_id'):
                    try:
                        await bot.edit_message_text(
                            text=(
                                f'<blockquote><tg-emoji emoji-id="5197288647275071607">💰</tg-emoji> <b>Успешное пополнение!</b></blockquote>\n\n'
                                f'<blockquote>'
                                f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji> Сумма: <code>{invoice["amount"]}</code>\n'
                                f'<tg-emoji emoji-id="5278467510604160626">💰</tg-emoji> Баланс: <code>{storage.get_balance(invoice["user_id"]):.2f}</code> <tg-emoji emoji-id="5197434882321567830">💰</tg-emoji>'
                                f'</blockquote>'
                            ),
                            parse_mode=ParseMode.HTML,
                            chat_id=invoice['chat_id'],
                            message_id=invoice['message_id'],
                            reply_markup=kb_back_profile()
                        )
                    except Exception as e:
                        logging.error(f"[{invoice_id}] Ошибка edit (paid): {e}")
                return

            await asyncio.sleep(2)

    except Exception as e:
        logging.error(f"Ошибка в задаче проверки [{invoice_id}]: {e}")
    finally:
        if invoice_id in storage.check_tasks:
            del storage.check_tasks[invoice_id]


# ========== ХЕНДЛЕР КОМАНДЫ ДЕП / DEP / DEPOSIT ==========
# Формат: [/]деп 1.5 | dep 1.5 | deposit 2 | пополнить 3 | депозит 5
# Лишний текст после числа → НЕ срабатывает (деп 10 хотел — игнор)
_DEP_RE = _re.compile(
    r'^/?(?:деп|пополнить|депозит|dep|deposit)\s+(\d+(?:\.\d+)?)$',
    _re.IGNORECASE
)

@payment_router.message(F.text.regexp(_DEP_RE))
async def handle_dep_command(message: Message):
    m = _DEP_RE.match(message.text.strip())
    if not m:
        return
    try:
        amount = float(m.group(1))
    except ValueError:
        return
    user_id = message.from_user.id
    storage.clear_pending(user_id)
    await _process_deposit(message, user_id, amount_override=amount)


# ========== ХЕНДЛЕР ВВОДА СУММЫ (через pending) ==========
@payment_router.message(F.text.regexp(r'^\d+\.?\d*$'))
async def handle_amount_input(message: Message):
    user_id = message.from_user.id
    action = storage.get_pending(user_id)

    if action == 'deposit':
        storage.clear_pending(user_id)
        await _process_deposit(message, user_id)
    elif action == 'withdraw':
        storage.clear_pending(user_id)
        await _process_withdraw(message, user_id)


# ========== ПОПОЛНЕНИЕ ==========
async def _process_deposit(message: Message, user_id: int, amount_override: float = None):
    try:
        amount = amount_override if amount_override is not None else float(message.text)

        if amount < MIN_DEPOSIT:
            await message.answer(
                f'<blockquote>❌ Минимальная сумма пополнения: <b><code>{MIN_DEPOSIT}</code><tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b></blockquote>',
                parse_mode=ParseMode.HTML,
                reply_markup=kb_back_profile()
            )
            return

        if storage.is_duplicate_request(user_id, amount, 'deposit'):
            logging.warning(f"[DUPE] Дублирующий запрос пополнения: user_id={user_id}, amount={amount}")
            await message.answer(
                '<blockquote>⏳ Запрос уже обрабатывается. Подождите несколько секунд.</blockquote>',
                parse_mode=ParseMode.HTML,
                reply_markup=kb_back_profile()
            )
            return

        invoice_data = await crypto_api.create_invoice(amount)
        if not invoice_data or 'pay_url' not in invoice_data:
            await message.answer(
                '<blockquote>❌ Ошибка создания счета. Попробуйте позже.</blockquote>',
                parse_mode=ParseMode.HTML,
                reply_markup=kb_back_profile()
            )
            return

        invoice_id = storage.create_invoice(
            user_id,
            amount,
            invoice_data['invoice_id'],
            invoice_data['pay_url']
        )

        sent_msg = await message.answer(
            text=(
                f'<b><tg-emoji emoji-id="5906482735341377395">💰</tg-emoji> Счет Создан!</b>\n\n'
                f'<blockquote>'
                f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji> Сумма: <b><code>{amount}</code></b>\n'
                f'<tg-emoji emoji-id="5906598824012420908">⌛️</tg-emoji> Действует — <b>5 минут</b>'
                f'</blockquote>\n\n'
                f'<tg-emoji emoji-id="5386367538735104399">🔵</tg-emoji> Ждем оплату!'
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Оплатить",
                        url=invoice_data['pay_url'],
                        icon_custom_emoji_id=EMOJI_LINK
                    )
                ],
                [btn_back_profile()]
            ])
        )

        storage.set_message_info(invoice_id, message.chat.id, sent_msg.message_id)
        if set_owner_fn:
            set_owner_fn(sent_msg.message_id, user_id)
        asyncio.create_task(update_user_info(
            user_id,
            first_name=message.from_user.first_name or '',
            username=message.from_user.username or ''
        ))

        if invoice_id not in storage.check_tasks:
            task = asyncio.create_task(check_payment_task(invoice_id))
            storage.check_tasks[invoice_id] = task

    except ValueError:
        await message.answer('❌ Введите число')


# ========== ВЫВОД ==========
async def _process_withdraw(message: Message, user_id: int):
    try:
        amount = float(message.text)
        balance = storage.get_balance(user_id)

        if amount < MIN_WITHDRAWAL:
            await message.answer(
                f'<blockquote>❌ Минимальная сумма вывода: <b><code>{MIN_WITHDRAWAL}</code><tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b></blockquote>',
                parse_mode=ParseMode.HTML,
                reply_markup=kb_back_profile()
            )
            return

        if amount > balance:
            await message.answer(
                f'<blockquote>❌ Недостаточно средств!</blockquote>',
                parse_mode=ParseMode.HTML,
                reply_markup=kb_back_profile()
            )
            return

        can_withdraw, wait_time = storage.can_withdraw(user_id)
        if not can_withdraw:
            minutes = wait_time // 60
            seconds = wait_time % 60
            await message.answer(
                f'<blockquote>⏳ Подождите <b>{minutes} мин {seconds} сек</b></blockquote>',
                parse_mode=ParseMode.HTML,
                reply_markup=kb_back_profile()
            )
            return

        if storage.is_duplicate_request(user_id, amount, 'withdraw'):
            logging.warning(f"[DUPE] Дублирующий запрос вывода: user_id={user_id}, amount={amount}")
            await message.answer(
                '<blockquote>⏳ Запрос уже обрабатывается. Подождите несколько секунд.</blockquote>',
                parse_mode=ParseMode.HTML,
                reply_markup=kb_back_profile()
            )
            return

        user_lock = storage.get_user_lock(user_id)
        async with user_lock:
            balance_now = storage.get_balance(user_id)
            if amount > balance_now:
                await message.answer(
                    f'<blockquote>❌ Недостаточно средств!</blockquote>',
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_back_profile()
                )
                return

            # ── Сначала резервируем (списываем) баланс, потом создаём чек ──────
            # Если создать чек ДО списания — при await event loop переключится
            # и баланс может измениться (игра), тогда record_withdrawal вернёт False,
            # но чек уже уйдёт пользователю → бесплатные деньги.
            withdrawn = storage.record_withdrawal(user_id, amount)
            if not withdrawn:
                logging.error(f"[WITHDRAW] record_withdrawal вернул False: user_id={user_id}, amount={amount}")
                await message.answer(
                    '<blockquote>❌ Ошибка списания средств. Попробуйте позже.</blockquote>',
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_back_profile()
                )
                return

        # Создаём чек уже после списания (вне лока — await безопасен)
        check = await crypto_api.create_check(amount, user_id)
        if not check or 'bot_check_url' not in check:
            # Чек не создан — полностью откатываем: баланс + total_withdrawals + лидеры
            storage.rollback_withdrawal(user_id, amount)
            try:
                from leaders import rollback_withdrawal_stat
                rollback_withdrawal_stat(user_id, amount)
            except Exception as _le:
                logging.error(f"[Leaders] rollback_withdrawal_stat error: {_le}")
            await message.answer(
                '<blockquote>❌ Ошибка создания чека! Попробуйте позже!</blockquote>',
                parse_mode=ParseMode.HTML,
                reply_markup=kb_back_profile()
            )
            return

        storage.set_last_withdrawal(user_id)
        asyncio.create_task(save_withdrawal(user_id, amount))

        # Записываем в лидеры по дням (чтобы фильтр по периоду работал)
        try:
            from leaders import record_withdrawal_stat
            user_data = storage.get_user(user_id)
            user_name = (
                user_data.get('first_name')
                or user_data.get('username')
                or f"User {user_id}"
            )
            record_withdrawal_stat(user_id, user_name, amount)
        except Exception as _le:
            logging.error(f"[Leaders] record_withdrawal_stat error: {_le}")

        # Логируем ссылку на чек — если Telegram не доставит сообщение,
        # пользователь найдёт чек в CryptoBot (pin_to_user_id), а мы видим лог
        logging.info(
            f"[WITHDRAW] Чек создан: user_id={user_id}, amount={amount}, "
            f"check_url={check['bot_check_url']}"
        )

        await message.answer(
            text=(
                f'<blockquote><tg-emoji emoji-id="5312441427764989435">💰</tg-emoji> <b>Вывод обработан!</b> ✅</blockquote>\n\n'
                f'<blockquote>'
                f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji> Сумма: <code>{amount}</code>\n'
                f'<tg-emoji emoji-id="5444856076954520455">💰</tg-emoji> Списано: <code>{amount}</code> <tg-emoji emoji-id="5197434882321567830">💰</tg-emoji>\n'
                f'<tg-emoji emoji-id="5278467510604160626">💰</tg-emoji> Баланс: <code>{storage.get_balance(user_id):.2f}</code> <tg-emoji emoji-id="5197434882321567830">💰</tg-emoji>'
                f'</blockquote>'
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Получить чек",
                        url=check['bot_check_url'],
                        icon_custom_emoji_id=EMOJI_LINK
                    )
                ],
                [btn_back_profile()]
            ])
        )

    except ValueError:
        await message.answer('❌ Введите число')


# ========== ИНИЦИАЛИЗАЦИЯ ==========
def setup_payments(bot_instance: Bot):
    global bot
    bot = bot_instance
