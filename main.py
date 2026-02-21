import asyncio
import logging
import os
import re
import json
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, Update, CallbackQuery
from aiogram.filters.command import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiohttp import web

# Импортируем модуль платежей
from payments import payment_router, setup_payments, storage, MIN_DEPOSIT, MIN_WITHDRAWAL

# Импортируем игровой модуль
from game import (
    BettingGame, show_dice_menu, show_basketball_menu, show_football_menu,
    show_darts_menu, show_bowling_menu, show_exact_number_menu, request_amount,
    cancel_bet, is_bet_command, handle_text_bet_command
)

# Импортируем модуль Мины
from mines import (
    mines_router, MinesGame, show_mines_menu, process_mines_bet, process_mines_command
)

# Импортируем модуль Башня
from tower import (
    tower_router, TowerGame, show_tower_menu, process_tower_bet, process_tower_command
)

# Импортируем реферальный модуль
from referrals import (
    referral_router, referral_storage,
    setup_referrals, process_start_referral,
    ReferralWithdraw, ref_withdraw_amount
)

# Импортируем модуль лидеров
from leaders import leaders_router, show_leaders, update_user_name

# Настройки
BOT_TOKEN = "8586332532:AAHX758cf6iOUpPNpY2sqseGBYsKJo9js4U"
WEBHOOK_PATH = "/webhook"
PORT = int(os.getenv('PORT', 8080))
RENDER_URL = os.getenv('RENDER_EXTERNAL_URL')

if RENDER_URL:
    WEBHOOK_URL = f"{RENDER_URL}{WEBHOOK_PATH}"
else:
    WEBHOOK_URL = f"https://festery.onrender.com{WEBHOOK_PATH}"

# ID кастомных эмодзи (только оригинальные из проекта)
EMOJI_WELCOME    = "5199885118214255386"
EMOJI_PROFILE    = "5906581476639513176"
EMOJI_PARTNERS   = "5906986955911993888"
EMOJI_GAMES      = "5424972470023104089"
EMOJI_LEADERS    = "5440539497383087970"
EMOJI_ABOUT      = "5251203410396458957"
EMOJI_CRYPTOBOT  = "5427054176246991778"
EMOJI_BACK       = "5906771962734057347"
EMOJI_DEVELOPMENT= "5445355530111437729"
EMOJI_WALLET     = "5443127283898405358"
EMOJI_STATS      = "5197288647275071607"
EMOJI_WITHDRAWAL = "5445355530111437729"
EMOJI_MINES      = "5307996024738395492"

# Кастомные callback_data для игр
GAME_CALLBACKS = {
    'dice':        'custom_dice_001',
    'basketball':  'custom_basketball_002',
    'football':    'custom_football_003',
    'darts':       'custom_darts_004',
    'bowling':     'custom_bowling_005',
    'exact_number':'custom_exact_006',
    'back_to_games':'custom_back_games_007'
}

# File ID для приветственного стикера
WELCOME_STICKER_ID = "CAACAgIAAxkBAAIGUWmRflo7gmuMF5MNUcs4LGpyA93yAAKaDAAC753ZS6lNRCGaKqt5OgQ"

# ID администраторов
ADMIN_IDS = [8118184388]

# Путь к файлу промокодов
PROMO_FILE = "promos.json"

# Роутер
router = Router()

# Экземпляр игры
betting_game = None


# ========== FSM ==========
class PromoState(StatesGroup):
    entering_code = State()


# ========== ПРОМОКОДЫ: ХРАНИЛИЩЕ ==========
def load_promos() -> dict:
    if not os.path.exists(PROMO_FILE):
        return {}
    try:
        with open(PROMO_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_promos(data: dict):
    with open(PROMO_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def promo_create(code: str, amount: float, activations: int) -> bool:
    data = load_promos()
    code = code.upper().strip()
    if code in data:
        return False
    data[code] = {"amount": amount, "activations": activations, "used_by": []}
    save_promos(data)
    return True


def promo_use(code: str, user_id: int):
    """Возвращает (ok: bool, amount: float, reason: str)"""
    data = load_promos()
    code = code.upper().strip()
    if code not in data:
        return False, 0, "not_found"
    promo = data[code]
    if user_id in promo["used_by"]:
        return False, 0, "already_used"
    if promo["activations"] <= 0:
        return False, 0, "expired"
    promo["used_by"].append(user_id)
    promo["activations"] -= 1
    save_promos(data)
    return True, promo["amount"], "ok"


# ========== ПРОВЕРКА КОМАНДЫ БАЛАНСА ==========
def is_balance_command(text: str) -> bool:
    if not text:
        return False
    t = text.lstrip('/')
    commands = {'б', 'b', 'бал', 'bal', 'баланс', 'balance'}
    return t.lower() in commands


# ========== СИНХРОНИЗАЦИЯ БАЛАНСОВ ==========
def sync_balances(user_id: int):
    return storage.get_balance(user_id)


# ========== КЛАВИАТУРЫ ==========
def get_main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Профиль",  callback_data="profile",  icon_custom_emoji_id=EMOJI_PROFILE),
            InlineKeyboardButton(text="Партнёры", callback_data="referrals", icon_custom_emoji_id=EMOJI_PARTNERS)
        ],
        [
            InlineKeyboardButton(text="Игры",   callback_data="games",   icon_custom_emoji_id=EMOJI_GAMES),
            InlineKeyboardButton(text="Лидеры", callback_data="leaders", icon_custom_emoji_id=EMOJI_LEADERS)
        ],
        [
            InlineKeyboardButton(text="Промокоды", callback_data="promo_menu", icon_custom_emoji_id=EMOJI_MINES),
            InlineKeyboardButton(text="О проекте", callback_data="about",      icon_custom_emoji_id=EMOJI_ABOUT)
        ]
    ])


def get_games_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎲 Кубик",    callback_data=GAME_CALLBACKS['dice']),
            InlineKeyboardButton(text="🏀 Баскетбол", callback_data=GAME_CALLBACKS['basketball'])
        ],
        [
            InlineKeyboardButton(text="⚽️ Футбол",  callback_data=GAME_CALLBACKS['football']),
            InlineKeyboardButton(text="🎯 Дартс",   callback_data=GAME_CALLBACKS['darts'])
        ],
        [
            InlineKeyboardButton(text="🎳 Боулинг", callback_data=GAME_CALLBACKS['bowling'])
        ],
        [
            InlineKeyboardButton(text="💣 Мины", callback_data="mines_menu"),
            InlineKeyboardButton(text="🏰 Башня", callback_data="tower_menu")
        ],
        [
            InlineKeyboardButton(text="Назад", callback_data="back_to_main", icon_custom_emoji_id=EMOJI_BACK)
        ]
    ])


def get_profile_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Пополнить", callback_data="deposit",  icon_custom_emoji_id=EMOJI_WALLET),
            InlineKeyboardButton(text="Вывести",   callback_data="withdraw", icon_custom_emoji_id=EMOJI_WITHDRAWAL)
        ],
        [
            InlineKeyboardButton(text="На главную", callback_data="back_to_main", icon_custom_emoji_id=EMOJI_BACK)
        ]
    ])


def get_cancel_menu():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Отмена", callback_data="profile", icon_custom_emoji_id=EMOJI_BACK)
    ]])


def get_balance_menu():
    bot_username = os.getenv("BOT_USERNAME", "your_bot")
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Пополнить",
                url=f"https://t.me/{bot_username}?start=deposit",
                icon_custom_emoji_id=EMOJI_WALLET
            ),
            InlineKeyboardButton(
                text="Вывести",
                url=f"https://t.me/{bot_username}?start=withdraw",
                icon_custom_emoji_id=EMOJI_WITHDRAWAL
            )
        ]
    ])


def get_promo_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Ввести промокод",
                callback_data="promo_enter",
                icon_custom_emoji_id=EMOJI_WALLET
            )
        ],
        [
            InlineKeyboardButton(
                text="На главную",
                callback_data="back_to_main",
                icon_custom_emoji_id=EMOJI_BACK
            )
        ]
    ])


def get_promo_cancel_menu():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="Отмена",
            callback_data="promo_menu",
            icon_custom_emoji_id=EMOJI_BACK
        )
    ]])


# ========== ТЕКСТЫ ==========
def get_main_menu_text():
    return (
        f"<blockquote><tg-emoji emoji-id=\"5197288647275071607\">🎰</tg-emoji> <b>Честные игры — прозрачные правила и реальные шансы на победу.</b>\n"
        f"<b>Без скрытых условий, всё открыто и по-настоящему честно.</b></blockquote>\n\n"
        f"<blockquote><tg-emoji emoji-id=\"5195033767969839232\">⚡</tg-emoji> <b>Быстрые выплаты — моментальный вывод средств без задержек.</b>\n"
        f"<tg-emoji emoji-id=\"5445355530111437729\">💎</tg-emoji> <b>Выводы через <tg-emoji emoji-id=\"{EMOJI_CRYPTOBOT}\">🔵</tg-emoji> <a href=\"https://t.me/send\">Cryptobot</a></b></blockquote>\n\n"
        f"<tg-emoji emoji-id=\"5907025791006283345\">💬</tg-emoji> <b><a href=\"https://t.me/your_support\">Тех. поддержка</a> | <a href=\"https://t.me/your_chat\">Наш чат</a> | <a href=\"https://t.me/your_news\">Новости</a></b>\n"
    )


def get_games_menu_text(user_id: int):
    balance = sync_balances(user_id)
    return (
        f"<blockquote><tg-emoji emoji-id=\"{EMOJI_GAMES}\">🎮</tg-emoji> <b>Игры</b></blockquote>\n\n"
        f"<blockquote><tg-emoji emoji-id=\"5278467510604160626\">🎮</tg-emoji>:<code>{balance:.2f}</code><tg-emoji emoji-id=\"5197434882321567830\">🎮</tg-emoji></blockquote>\n\n"
        f"<blockquote><b>Выберите игру:</b></blockquote>\n\n"
        f"<tg-emoji emoji-id=\"5907025791006283345\">💬</tg-emoji> <b><a href=\"https://t.me/your_support\">Тех. поддержка</a> | <a href=\"https://t.me/your_chat\">Наш чат</a> | <a href=\"https://t.me/your_news\">Новости</a></b>\n"
    )


def get_profile_text(user_first_name: str, days_in_project: int, user_id: int):
    balance = sync_balances(user_id)
    user_data = storage.get_user(user_id)
    total_deposits    = user_data.get('total_deposits', 0)
    total_withdrawals = user_data.get('total_withdrawals', 0)

    if 11 <= days_in_project <= 19:
        days_text = "дней"
    elif days_in_project % 10 == 1:
        days_text = "день"
    elif days_in_project % 10 in [2, 3, 4]:
        days_text = "дня"
    else:
        days_text = "дней"

    return (
        f"<blockquote><b><tg-emoji emoji-id=\"{EMOJI_PROFILE}\">👤</tg-emoji> Профиль</b></blockquote>\n\n"
        f"<blockquote>\n"
        f"<b><tg-emoji emoji-id=\"5278467510604160626\">💰</tg-emoji>:<code>{balance:,.2f}</code><tg-emoji emoji-id=\"5197434882321567830\">💰</tg-emoji></b>\n"
        f"<tg-emoji emoji-id=\"5443127283898405358\">📥</tg-emoji> Депозитов: <b><code>{total_deposits:,.2f}</code><tg-emoji emoji-id=\"5197434882321567830\">💰</tg-emoji></b>\n"
        f"<tg-emoji emoji-id=\"5445355530111437729\">📤</tg-emoji> Выводов: <b><code>{total_withdrawals:,.2f}</code><tg-emoji emoji-id=\"5197434882321567830\">💰</tg-emoji></b>\n"
        f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> В проекте: <b><code>{days_in_project} {days_text}</code></b>\n"
        f"</blockquote>\n\n"
        f"<tg-emoji emoji-id=\"5907025791006283345\">💬</tg-emoji> <b><a href=\"https://t.me/your_support\">Тех. поддержка</a> | <a href=\"https://t.me/your_chat\">Наш чат</a> | <a href=\"https://t.me/your_news\">Новости</a></b>\n"
    )


# ========== СТАРТ ==========
@router.message(CommandStart())
async def cmd_start(message: Message):
    try:
        args = message.text.split(maxsplit=1)
        param = args[1] if len(args) > 1 else ""

        if param == "deposit":
            storage.get_user(message.from_user.id)
            storage.set_pending(message.from_user.id, 'deposit')
            await message.answer(
                f"<b><tg-emoji emoji-id=\"{EMOJI_WALLET}\">💰</tg-emoji> Пополнение баланса</b>\n\n"
                f"<blockquote><i><tg-emoji emoji-id=\"5197269100878907942\">💸</tg-emoji> Введите сумму пополнения:</i></blockquote>",
                parse_mode=ParseMode.HTML,
                reply_markup=get_cancel_menu()
            )
            return

        elif param == "withdraw":
            storage.get_user(message.from_user.id)
            storage.set_pending(message.from_user.id, 'withdraw')
            await message.answer(
                f"<b><tg-emoji emoji-id=\"{EMOJI_WITHDRAWAL}\">💸</tg-emoji> Вывод средств</b>\n\n"
                f"<blockquote><i><tg-emoji emoji-id=\"5197269100878907942\">💸</tg-emoji> Введите сумму вывода:</i></blockquote>",
                parse_mode=ParseMode.HTML,
                reply_markup=get_cancel_menu()
            )
            return

        elif param.startswith("ref_"):
            await process_start_referral(message, param)

        else:
            referral_storage.mark_organic(message.from_user.id)

        storage.get_user(message.from_user.id)
        sync_balances(message.from_user.id)
        update_user_name(storage, message.from_user.id, message.from_user.first_name or "")

        await message.answer_sticker(sticker=WELCOME_STICKER_ID)
        await message.answer(
            get_main_menu_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_menu()
        )
    except Exception as e:
        logging.error(f"Error in start: {e}")
        await message.answer("Произошла ошибка. Попробуйте позже.")


# ========== АДМИН: /add ==========
@router.message(F.text.startswith("/add") & ~F.text.startswith("/addpromo"))
async def cmd_add_balance(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Нет доступа.")
        return

    parts = message.text.split()
    if len(parts) != 3:
        await message.answer(
            "<b>⚙️ Использование:</b>\n"
            "<code>/add [user_id] [сумма]</code>\n\n"
            "<b>Пример:</b> <code>/add 123456789 100</code>",
            parse_mode=ParseMode.HTML
        )
        return

    try:
        target_id = int(parts[1])
        amount    = float(parts[2])
    except ValueError:
        await message.answer("❌ Неверный формат. ID должен быть числом, сумма — числом.")
        return

    if amount <= 0:
        await message.answer("❌ Сумма должна быть больше 0.")
        return

    storage.get_user(target_id)
    storage.add_balance(target_id, amount)
    new_balance = storage.get_balance(target_id)

    await message.answer(
        f"<b>✅ Баланс выдан</b>\n\n"
        f"<blockquote>"
        f"👤 ID: <code>{target_id}</code>\n"
        f"➕ Выдано: <code>{amount:.2f}</code>\n"
        f"💰 Новый баланс: <code>{new_balance:.2f}</code>"
        f"</blockquote>",
        parse_mode=ParseMode.HTML
    )
    logging.info(f"Админ {message.from_user.id} выдал {amount} пользователю {target_id}. Новый баланс: {new_balance}")


# ========== АДМИН: /addpromo ==========
@router.message(F.text.startswith("/addpromo"))
async def cmd_add_promo(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Нет доступа.")
        return

    parts = message.text.split()
    if len(parts) != 4:
        await message.answer(
            f"<b><tg-emoji emoji-id=\"{EMOJI_STATS}\">📊</tg-emoji> Создание промокода</b>\n\n"
            f"<blockquote><b>Использование:</b>\n"
            f"<code>/addpromo [код] [сумма] [активации]</code>\n\n"
            f"<b>Пример:</b>\n"
            f"<code>/addpromo SUMMER25 50 100</code></blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    code = parts[1].upper().strip()
    try:
        amount      = float(parts[2])
        activations = int(parts[3])
    except ValueError:
        await message.answer(
            "❌ <b>Неверный формат.</b>\n"
            "<blockquote>Сумма — число, активации — целое число.</blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    if amount <= 0 or activations <= 0:
        await message.answer(
            "❌ <b>Сумма и количество активаций должны быть больше 0.</b>",
            parse_mode=ParseMode.HTML
        )
        return

    ok = promo_create(code, amount, activations)
    if not ok:
        await message.answer(
            f"❌ <b>Промокод <code>{code}</code> уже существует.</b>",
            parse_mode=ParseMode.HTML
        )
        return

    await message.answer(
        f"✅ <b>Промокод создан!</b>\n\n"
        f"<blockquote>"
        f"<tg-emoji emoji-id=\"{EMOJI_STATS}\">📊</tg-emoji> Код: <code>{code}</code>\n"
        f"<tg-emoji emoji-id=\"{EMOJI_WALLET}\">💰</tg-emoji> Сумма: <b><code>{amount:.2f}</code></b> <tg-emoji emoji-id=\"5197434882321567830\">💰</tg-emoji>\n"
        f"<tg-emoji emoji-id=\"{EMOJI_WELCOME}\">🔥</tg-emoji> Активаций: <b><code>{activations}</code></b>"
        f"</blockquote>",
        parse_mode=ParseMode.HTML
    )
    logging.info(f"Админ {message.from_user.id} создал промокод {code} на {amount} ({activations} активаций)")


# ========== ПРОМОКОДЫ: МЕНЮ ==========
@router.callback_query(F.data == "promo_menu")
async def promo_menu_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        f"<tg-emoji emoji-id=\"{EMOJI_MINES}\">💣</tg-emoji> <b>Промокоды</b>\n\n"
        f"<blockquote>"
        f"<tg-emoji emoji-id=\"{EMOJI_WALLET}\">💰</tg-emoji> Активируй промокод и получи бонус на баланс.\n\n"
        f"<tg-emoji emoji-id=\"{EMOJI_WELCOME}\">🔥</tg-emoji> Промокоды публикуются в нашем канале и чате."
        f"</blockquote>\n\n"
        f"<tg-emoji emoji-id=\"5907025791006283345\">💬</tg-emoji> <b><a href=\"https://t.me/your_support\">Тех. поддержка</a> | <a href=\"https://t.me/your_chat\">Наш чат</a> | <a href=\"https://t.me/your_news\">Новости</a></b>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_promo_menu()
    )
    await callback.answer()


# ========== ПРОМОКОДЫ: ВВОД ==========
@router.callback_query(F.data == "promo_enter")
async def promo_enter_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(PromoState.entering_code)
    await callback.message.edit_text(
        f"<tg-emoji emoji-id=\"{EMOJI_STATS}\">📊</tg-emoji> <b>Введите промокод</b>\n\n"
        f"<blockquote><i>Напишите код в чат — регистр не важен.</i></blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_promo_cancel_menu()
    )
    await callback.answer()


# ========== ПРОФИЛЬ ==========
@router.callback_query(F.data == "profile")
async def profile_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    from datetime import datetime
    user_data     = storage.get_user(callback.from_user.id)
    join_date_str = user_data.get('join_date', datetime.now().strftime('%Y-%m-%d'))
    join_date     = datetime.strptime(join_date_str, '%Y-%m-%d')
    days_in_project = (datetime.now() - join_date).days

    update_user_name(storage, callback.from_user.id, callback.from_user.first_name or "")

    await callback.message.edit_text(
        get_profile_text(callback.from_user.first_name, days_in_project, callback.from_user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=get_profile_menu()
    )
    await callback.answer()


# ========== ИГРЫ ==========
@router.callback_query(F.data == "games")
async def games_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        get_games_menu_text(callback.from_user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=get_games_menu()
    )
    await callback.answer()


# ========== МИНЫ — ВХОД ==========
@router.callback_query(F.data == "mines_menu")
async def mines_menu_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_mines_menu(callback, storage, betting_game)


# ========== БАШНЯ — ВХОД ==========
@router.callback_query(F.data == "tower_menu")
async def tower_menu_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_tower_menu(callback, storage, betting_game)


# ========== ОСТАЛЬНЫЕ ИГРЫ ==========
@router.callback_query(F.data == GAME_CALLBACKS['dice'])
async def dice_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_dice_menu(callback)

@router.callback_query(F.data == GAME_CALLBACKS['basketball'])
async def basketball_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_basketball_menu(callback)

@router.callback_query(F.data == GAME_CALLBACKS['football'])
async def football_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_football_menu(callback)

@router.callback_query(F.data == GAME_CALLBACKS['darts'])
async def darts_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_darts_menu(callback)

@router.callback_query(F.data == GAME_CALLBACKS['bowling'])
async def bowling_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_bowling_menu(callback)

@router.callback_query(F.data == "bet_dice_exact")
async def exact_number_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_exact_number_menu(callback)

@router.callback_query(F.data.startswith("bet_"))
async def handle_bet_selection(callback: CallbackQuery, state: FSMContext):
    await request_amount(callback, state, betting_game)

@router.callback_query(F.data == "cancel_bet")
async def handle_cancel_bet(callback: CallbackQuery, state: FSMContext):
    await cancel_bet(callback, state, betting_game)


# ========== ПОПОЛНЕНИЕ (из профиля) ==========
@router.callback_query(F.data == "deposit")
async def deposit_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    storage.set_pending(callback.from_user.id, 'deposit')
    await callback.message.edit_text(
        f"<b><tg-emoji emoji-id=\"{EMOJI_WALLET}\">💰</tg-emoji> Пополнение баланса</b>\n\n"
        f"<blockquote><i><tg-emoji emoji-id=\"5197269100878907942\">💸</tg-emoji> Введите сумму пополнения:</i></blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_cancel_menu()
    )
    await callback.answer()


# ========== ВЫВОД (из профиля) ==========
@router.callback_query(F.data == "withdraw")
async def withdraw_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    storage.set_pending(callback.from_user.id, 'withdraw')
    await callback.message.edit_text(
        f"<b><tg-emoji emoji-id=\"{EMOJI_WITHDRAWAL}\">💸</tg-emoji> Вывод средств</b>\n\n"
        f"<blockquote><i><tg-emoji emoji-id=\"5197269100878907942\">💸</tg-emoji> Введите сумму вывода:</i></blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_cancel_menu()
    )
    await callback.answer()


# ========== ТЕКСТОВЫЕ СООБЩЕНИЯ ==========

@router.message(F.text.regexp(r'(?i)^(?:/)?(?:mines|мины)\s+[\d.,]+\s+\d+$'))
async def mines_command_handler(message: Message, state: FSMContext):
    await process_mines_command(message, state, storage)


@router.message(F.text.regexp(r'(?i)^(?:/)?(?:tower|башня)\s+[\d.,]+\s+\d+$'))
async def tower_command_handler(message: Message, state: FSMContext):
    await process_tower_command(message, state, storage)


@router.message(F.text)
async def handle_text_message(message: Message, state: FSMContext):
    from payments import handle_amount_input

    # — КОМАНДА БАЛАНСА —
    if is_balance_command(message.text):
        balance = sync_balances(message.from_user.id)
        await message.reply(
            f"<blockquote><b><tg-emoji emoji-id=\"5278467510604160626\">💰</tg-emoji> "
            f"<code>{balance:,.2f}</code> "
            f"<tg-emoji emoji-id=\"5197434882321567830\">💰</tg-emoji></b></blockquote>\n\n"
            f"<blockquote><i>Выберите действие ниже <tg-emoji emoji-id=\"5201691993775818138\">💰</tg-emoji></i></blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=get_balance_menu()
        )
        return

    current_state = await state.get_state()

    # ── Ввод промокода ──────────────────────────────────────────────────
    if current_state == PromoState.entering_code.state:
        code = message.text.strip()
        ok, amount, reason = promo_use(code, message.from_user.id)

        if ok:
            storage.get_user(message.from_user.id)
            storage.add_balance(message.from_user.id, amount)
            new_balance = storage.get_balance(message.from_user.id)
            await state.clear()
            await message.answer(
                f"✅ <b>Промокод активирован!</b>\n\n"
                f"<blockquote>"
                f"<tg-emoji emoji-id=\"{EMOJI_WALLET}\">💰</tg-emoji> Начислено: <b><code>+{amount:.2f}</code></b> <tg-emoji emoji-id=\"5197434882321567830\">💰</tg-emoji>\n"
                f"<tg-emoji emoji-id=\"5278467510604160626\">💰</tg-emoji> Баланс: <b><code>{new_balance:.2f}</code></b> <tg-emoji emoji-id=\"5197434882321567830\">💰</tg-emoji>"
                f"</blockquote>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(
                        text="На главную",
                        callback_data="back_to_main",
                        icon_custom_emoji_id=EMOJI_BACK
                    )
                ]])
            )
        else:
            error_texts = {
                "not_found":    "Промокод не найден. Проверьте правильность ввода.",
                "already_used": "Вы уже активировали этот промокод.",
                "expired":      "Промокод больше не активен — все активации израсходованы.",
            }
            err_msg = error_texts.get(reason, "Неизвестная ошибка.")
            await message.answer(
                f"❌ <b>Ошибка активации</b>\n\n"
                f"<blockquote>{err_msg}</blockquote>",
                parse_mode=ParseMode.HTML,
                reply_markup=get_promo_cancel_menu()
            )
        return

    # ── Вывод реферального баланса ──────────────────────────────────────
    if current_state == ReferralWithdraw.entering_amount.state:
        await ref_withdraw_amount(message, state)
        return

    # Ставка в игре Мины
    if current_state == MinesGame.choosing_bet:
        await process_mines_bet(message, state, storage)
        return

    # Ставка в игре Башня
    if current_state == TowerGame.choosing_bet:
        await process_tower_bet(message, state, storage)
        return

    # Команды ставок
    if is_bet_command(message.text):
        await handle_text_bet_command(message, betting_game)
        return

    # Числовой ввод
    try:
        float(message.text)
        if current_state:
            from game import process_bet_amount
            await process_bet_amount(message, state, betting_game)
        else:
            await handle_amount_input(message)
    except ValueError:
        pass


# ========== ЛИДЕРЫ ==========
@router.callback_query(F.data == "leaders")
async def leaders_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_leaders(callback, storage)


# ========== О ПРОЕКТЕ ==========
@router.callback_query(F.data == "about")
async def about_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        f'<tg-emoji emoji-id="{EMOJI_ABOUT}">ℹ️</tg-emoji> <b>О проекте</b>\n\n'
        f'Мы — команда профессионалов, создающая честный гемблинг с 2020 года.\n\n'
        f'• Мгновенные выплаты\n'
        f'• Прозрачные алгоритмы\n'
        f'• Поддержка 24/7\n'
        f'• Лицензия Curacao',
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_main")
        ]])
    )
    await callback.answer()


# ========== НА ГЛАВНУЮ ==========
@router.callback_query(F.data == "back_to_main")
async def back_to_main_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    storage.clear_pending(callback.from_user.id)
    await callback.message.edit_text(
        get_main_menu_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_menu()
    )
    await callback.answer()


# ========== ЗАПУСК ==========
async def main():
    global betting_game

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher(storage=MemoryStorage())

    bot_info = await bot.get_me()
    os.environ["BOT_USERNAME"] = bot_info.username
    logging.info(f"Бот запущен как @{bot_info.username}")

    betting_game = BettingGame(bot)

    dp.include_router(router)
    dp.include_router(mines_router)
    dp.include_router(tower_router)
    dp.include_router(referral_router)
    dp.include_router(payment_router)
    dp.include_router(leaders_router)

    setup_payments(bot)
    setup_referrals(bot)

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(WEBHOOK_URL)

    logging.info(f"Бот запущен на вебхуках: {WEBHOOK_URL}")

    app = web.Application()

    async def webhook_handler(request):
        try:
            json_data = await request.json()
            update = Update.model_validate(json_data, context={"bot": bot})
            await dp.feed_update(bot, update)
            return web.Response(status=200)
        except Exception as e:
            logging.error(f"Ошибка при обработке вебхука: {e}")
            return web.Response(status=500)

    async def handle_index(request):
        return web.Response(text="Бот работает!", content_type="text/html")

    app.router.add_post(WEBHOOK_PATH, webhook_handler)
    app.router.add_get("/", handle_index)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)

    logging.info(f"Сервер запущен на порту {PORT}")
    await site.start()

    await asyncio.Event().wait()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
