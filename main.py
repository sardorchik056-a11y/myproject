import asyncio
import logging
import os
import re
import json
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters.command import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from payments import payment_router, setup_payments, storage, MIN_DEPOSIT, MIN_WITHDRAWAL
from game import (
    BettingGame, show_dice_menu, show_basketball_menu, show_football_menu,
    show_darts_menu, show_bowling_menu, show_exact_number_menu, request_amount,
    cancel_bet, is_bet_command, handle_text_bet_command
)
from mines import (
    mines_router, MinesGame, show_mines_menu, process_mines_bet, process_mines_command
)
from tower import (
    tower_router, TowerGame, show_tower_menu, process_tower_bet, process_tower_command
)
from gold import (
    gold_router, GoldGame, show_gold_menu, process_gold_bet, process_gold_command
)
from referrals import (
    referral_router, referral_storage,
    setup_referrals, process_start_referral,
    ReferralWithdraw, ref_withdraw_amount
)
from leaders import leaders_router, show_leaders, update_user_name, init_leaders_db
import leaders as _leaders_module
import mines as _mines_module
import tower as _tower_module
import gold as _gold_module
import referrals as _referrals_module
import payments as _payments_module

from duels import (
    duels_router, setup_duels,
    is_duel_command, handle_duel_command,
    is_mygames_command, handle_mygames,
    is_del_command, handle_del,
)
import duels as _duels_module

try:
    from database import init_db, import_users_from_json
except ImportError:
    async def init_db(): pass
    async def import_users_from_json(): pass

BOT_TOKEN = "8531951028:AAFWUlHwpWfRrD2MvT4BqtexO7nFsQwFpcA"

LINK_NEWS     = "https://t.me/FesteryNews"
LINK_CHAT     = "https://t.me/FesteryCasChat"
LINK_INSTRUCT = "https://t.me/Festery_info"
LINK_SUPPORT  = "https://t.me/Xyloth_1337"

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
EMOJI_PROMO      = "5444856076954520455"
EMOJI_INSTRUCT   = "5334544901428229844"
EMOJI_CHANNEL    = "5424818078833715060"
EMOJI_CHAT       = "5443038326535759644"
EMOJI_SUPORT     = "5907025791006283345"
EMOJI_PEREXOD    = "5906839307821259375"

GAME_CALLBACKS = {
    'dice':         'custom_dice_001',
    'basketball':   'custom_basketball_002',
    'football':     'custom_football_003',
    'darts':        'custom_darts_004',
    'bowling':      'custom_bowling_005',
    'exact_number': 'custom_exact_006',
    'back_to_games':'custom_back_games_007'
}

WELCOME_STICKER_ID = "CAACAgIAAxkBAAIGUWmRflo7gmuMF5MNUcs4LGpyA93yAAKaDAAC753ZS6lNRCGaKqt5OgQ"

ADMIN_IDS    = [8118184388, 8115654734]
PROMO_FILE   = "promos.json"
MIN_TRANSFER = 0.02
MAX_TRANSFER = 10000

TRANSFER_PATTERN = re.compile(r'^(?:/)?(?:pay|дать)\s+([\d.,]+)$', re.IGNORECASE)
GAMES_PATTERN    = re.compile(r'^/?(?:игры|games)$', re.IGNORECASE)
DEP_PATTERN      = re.compile(
    r'^/?(?:деп|пополнить|депозит|dep|deposit)\s+(\d+(?:\.\d+)?)$',
    re.IGNORECASE
)
GOLD_PATTERN = re.compile(r'^/?(?:gold|золото)\s+[\d.,]+$', re.IGNORECASE)

_transfer_locks: dict = {}

def _get_transfer_lock(user_id: int) -> asyncio.Lock:
    if user_id not in _transfer_locks:
        _transfer_locks[user_id] = asyncio.Lock()
    return _transfer_locks[user_id]

router       = Router()
betting_game = None
_msg_owners: dict = {}

def _set_msg_owner(message_id: int, user_id: int):
    _msg_owners[message_id] = user_id

def _is_msg_owner(message_id: int, user_id: int) -> bool:
    owner = _msg_owners.get(message_id)
    if owner is None:
        return True
    return owner == user_id

def _inject_leaders_owner_fns():
    _leaders_module.set_owner_fn   = _set_msg_owner
    _leaders_module.is_owner_fn    = _is_msg_owner
    _mines_module.set_owner_fn     = _set_msg_owner
    _mines_module.is_owner_fn      = _is_msg_owner
    _tower_module.set_owner_fn     = _set_msg_owner
    _tower_module.is_owner_fn      = _is_msg_owner
    _gold_module.set_owner_fn      = _set_msg_owner
    _gold_module.is_owner_fn       = _is_msg_owner
    _referrals_module.set_owner_fn = _set_msg_owner
    _referrals_module.is_owner_fn  = _is_msg_owner
    _payments_module.set_owner_fn  = _set_msg_owner
    _payments_module.is_owner_fn   = _is_msg_owner
    _duels_module.set_owner_fn     = _set_msg_owner
    _duels_module.is_owner_fn      = _is_msg_owner


# ========== FSM ==========
class PromoState(StatesGroup):
    entering_code = State()


# ========== ПРОМОКОДЫ ==========
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


def is_balance_command(text: str) -> bool:
    if not text:
        return False
    t = text.lstrip('/')
    return t.lower() in {'б', 'b', 'бал', 'bal', 'баланс', 'balance'}

def sync_balances(user_id: int):
    return storage.get_balance(user_id)

def links_line() -> str:
    return (
        f'<tg-emoji emoji-id="{EMOJI_SUPORT}">💬</tg-emoji> <b>'
        f'<a href="{LINK_SUPPORT}">Тех. поддержка</a> | '
        f'<a href="{LINK_CHAT}">Наш чат</a> | '
        f'<a href="{LINK_NEWS}">Новости</a></b>'
    )


# ========== КЛАВИАТУРЫ ==========
def get_main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Профиль",  callback_data="profile",   icon_custom_emoji_id=EMOJI_PROFILE),
            InlineKeyboardButton(text="Партнёры", callback_data="referrals", icon_custom_emoji_id=EMOJI_PARTNERS)
        ],
        [
            InlineKeyboardButton(text="Игры",   callback_data="games",   icon_custom_emoji_id=EMOJI_GAMES),
            InlineKeyboardButton(text="Лидеры", callback_data="leaders", icon_custom_emoji_id=EMOJI_LEADERS)
        ],
        [
            InlineKeyboardButton(text="Промокоды", callback_data="promo_menu", icon_custom_emoji_id=EMOJI_PROMO),
            InlineKeyboardButton(text="О проекте", callback_data="about",      icon_custom_emoji_id=EMOJI_ABOUT)
        ],
        [
            InlineKeyboardButton(text="Инструкция", url=LINK_INSTRUCT, icon_custom_emoji_id=EMOJI_INSTRUCT)
        ]
    ])

def get_games_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎲 Кубик",     callback_data=GAME_CALLBACKS['dice']),
            InlineKeyboardButton(text="🏀 Баскетбол", callback_data=GAME_CALLBACKS['basketball'])
        ],
        [
            InlineKeyboardButton(text="⚽️ Футбол", callback_data=GAME_CALLBACKS['football']),
            InlineKeyboardButton(text="🎯 Дартс",  callback_data=GAME_CALLBACKS['darts'])
        ],
        [
            InlineKeyboardButton(text="🎳 Боулинг", callback_data=GAME_CALLBACKS['bowling'])
        ],
        [
            InlineKeyboardButton(text="💣 Мины",  callback_data="mines_menu"),
            InlineKeyboardButton(text="🏰 Башня", callback_data="tower_menu")
        ],
        [
            InlineKeyboardButton(text="⛏ Золото", callback_data="gold_menu")
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
            InlineKeyboardButton(text="Назад", callback_data="back_to_main", icon_custom_emoji_id=EMOJI_BACK)
        ]
    ])

def get_cancel_menu():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Отмена", callback_data="profile", icon_custom_emoji_id=EMOJI_BACK)
    ]])

def get_balance_menu():
    bot_username = os.getenv("BOT_USERNAME", "your_bot")
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Пополнить", url=f"https://t.me/{bot_username}?start=deposit", icon_custom_emoji_id=EMOJI_WALLET),
        InlineKeyboardButton(text="Вывести",   url=f"https://t.me/{bot_username}?start=withdraw", icon_custom_emoji_id=EMOJI_WITHDRAWAL)
    ]])

def get_promo_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ввести промокод", callback_data="promo_enter", icon_custom_emoji_id=EMOJI_PROMO)],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_main", icon_custom_emoji_id=EMOJI_BACK)]
    ])

def get_promo_cancel_menu():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Отмена", callback_data="promo_menu", icon_custom_emoji_id=EMOJI_BACK)
    ]])


# ========== ТЕКСТЫ ==========
def get_main_menu_text():
    return (
        f'<blockquote><tg-emoji emoji-id="5197288647275071607">🎰</tg-emoji> <b>Честные игры — прозрачные правила и реальные шансы на победу.</b>\n'
        f'<b>Без скрытых условий, всё открыто и по-настоящему честно.</b></blockquote>\n\n'
        f'<blockquote><tg-emoji emoji-id="5195033767969839232">⚡</tg-emoji> <b>Быстрые выплаты — моментальный вывод средств без задержек.</b>\n'
        f'<tg-emoji emoji-id="5445355530111437729">💎</tg-emoji> <b>Выводы через <tg-emoji emoji-id="{EMOJI_CRYPTOBOT}">🔵</tg-emoji> <a href="https://t.me/send">Cryptobot</a></b></blockquote>\n\n'
        f'{links_line()}\n'
    )

def get_games_menu_text(user_id: int):
    balance = sync_balances(user_id)
    return (
        f'<blockquote><tg-emoji emoji-id="{EMOJI_GAMES}">🎮</tg-emoji> <b>Игры</b></blockquote>\n\n'
        f'<blockquote><tg-emoji emoji-id="5278467510604160626">🎮</tg-emoji>:<code>{balance:.2f}</code><tg-emoji emoji-id="5197434882321567830">🎮</tg-emoji></blockquote>\n\n'
        f'<blockquote><b>Выберите игру:</b></blockquote>\n\n'
        f'{links_line()}\n'
    )

def get_profile_text(user_first_name: str, days_in_project: int, user_id: int):
    balance           = sync_balances(user_id)
    user_data         = storage.get_user(user_id)
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
        f'<blockquote><b><tg-emoji emoji-id="{EMOJI_PROFILE}">👤</tg-emoji> Профиль</b></blockquote>\n\n'
        f'<blockquote>\n'
        f'<b><tg-emoji emoji-id="5278467510604160626">💰</tg-emoji>:<code>{balance:,.2f}</code><tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b>\n'
        f'<tg-emoji emoji-id="5443127283898405358">📥</tg-emoji> Депозитов: <b><code>{total_deposits:,.2f}</code><tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b>\n'
        f'<tg-emoji emoji-id="5445355530111437729">📤</tg-emoji> Выводов: <b><code>{total_withdrawals:,.2f}</code><tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b>\n'
        f'<tg-emoji emoji-id="5274055917766202507">📅</tg-emoji> В проекте: <b><code>{days_in_project} {days_text}</code></b>\n'
        f'</blockquote>\n\n'
        f'{links_line()}\n'
    )


# ========== СТАРТ ==========
@router.message(CommandStart())
async def cmd_start(message: Message):
    try:
        args  = message.text.split(maxsplit=1)
        param = args[1] if len(args) > 1 else ""

        if param == "deposit":
            storage.get_user(message.from_user.id)
            storage.set_pending(message.from_user.id, 'deposit')
            await message.answer(
                f'<b><tg-emoji emoji-id="{EMOJI_WALLET}">💰</tg-emoji> Пополнение баланса</b>\n\n'
                f'<blockquote><i><tg-emoji emoji-id="5197269100878907942">💸</tg-emoji> Введите сумму пополнения:</i></blockquote>',
                parse_mode=ParseMode.HTML, reply_markup=get_cancel_menu()
            )
            return
        elif param == "withdraw":
            storage.get_user(message.from_user.id)
            storage.set_pending(message.from_user.id, 'withdraw')
            await message.answer(
                f'<b><tg-emoji emoji-id="{EMOJI_WITHDRAWAL}">💸</tg-emoji> Вывод средств</b>\n\n'
                f'<blockquote><i><tg-emoji emoji-id="5197269100878907942">💸</tg-emoji> Введите сумму вывода:</i></blockquote>',
                parse_mode=ParseMode.HTML, reply_markup=get_cancel_menu()
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
        sent = await message.answer(get_main_menu_text(), parse_mode=ParseMode.HTML, reply_markup=get_main_menu())
        _set_msg_owner(sent.message_id, message.from_user.id)
    except Exception as e:
        logging.error(f"Error in start: {e}")
        await message.answer("Произошла ошибка. Попробуйте позже.")


# ========== АДМИН ==========
@router.message(F.text.startswith("/add") & ~F.text.startswith("/addpromo"))
async def cmd_add_balance(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Нет доступа.")
        return
    parts = message.text.split()
    if len(parts) != 3:
        await message.answer("<b>⚙️ Использование:</b>\n<code>/add [user_id] [сумма]</code>", parse_mode=ParseMode.HTML)
        return
    try:
        target_id = int(parts[1])
        amount    = float(parts[2])
    except ValueError:
        await message.answer("❌ Неверный формат.")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть больше 0.")
        return
    storage.get_user(target_id)
    storage.add_balance(target_id, amount)
    new_balance = storage.get_balance(target_id)
    await message.answer(
        f"<b>✅ Баланс выдан</b>\n\n<blockquote>"
        f"👤 ID: <code>{target_id}</code>\n"
        f"➕ Выдано: <code>{amount:.2f}</code>\n"
        f"💰 Новый баланс: <code>{new_balance:.2f}</code></blockquote>",
        parse_mode=ParseMode.HTML
    )


@router.message(F.text.startswith("/addpromo"))
async def cmd_add_promo(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Нет доступа.")
        return
    parts = message.text.split()
    if len(parts) != 4:
        await message.answer(f'<b>Использование:</b> <code>/addpromo [код] [сумма] [активации]</code>', parse_mode=ParseMode.HTML)
        return
    code = parts[1].upper().strip()
    try:
        amount      = float(parts[2])
        activations = int(parts[3])
    except ValueError:
        await message.answer("❌ Неверный формат.", parse_mode=ParseMode.HTML)
        return
    if amount <= 0 or activations <= 0:
        await message.answer("❌ Сумма и кол-во активаций должны быть > 0.", parse_mode=ParseMode.HTML)
        return
    ok = promo_create(code, amount, activations)
    if not ok:
        await message.answer(f"❌ Промокод <code>{code}</code> уже существует.", parse_mode=ParseMode.HTML)
        return
    await message.answer(
        f'✅ <b>Промокод создан!</b>\n\n<blockquote>'
        f'Код: <code>{code}</code>\n'
        f'Сумма: <b><code>{amount:.2f}</code>💰</b>\n'
        f'Активаций: <b><code>{activations}</code></b></blockquote>',
        parse_mode=ParseMode.HTML
    )


# ========== CALLBACK: ПРОМОКОДЫ ==========
@router.callback_query(F.data == "promo_menu")
async def promo_menu_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text(
        f'<tg-emoji emoji-id="{EMOJI_PROMO}">💣</tg-emoji> <b>Промокоды</b>\n\n'
        f'<blockquote>Активируй промокод и получи бонус на баланс.\n\n'
        f'Промокоды публикуются в нашем <a href="{LINK_CHAT}">чате</a> и <a href="{LINK_NEWS}">канале</a>.</blockquote>\n\n{links_line()}',
        parse_mode=ParseMode.HTML, reply_markup=get_promo_menu(), disable_web_page_preview=True
    )
    _set_msg_owner(callback.message.message_id, callback.from_user.id)
    await callback.answer()

@router.callback_query(F.data == "promo_enter")
async def promo_enter_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    await state.set_state(PromoState.entering_code)
    await callback.message.edit_text(
        f'<b>Введите промокод</b>\n\n<blockquote><i>Напишите код в чат — регистр не важен.</i></blockquote>',
        parse_mode=ParseMode.HTML, reply_markup=get_promo_cancel_menu()
    )
    _set_msg_owner(callback.message.message_id, callback.from_user.id)
    await callback.answer()


# ========== CALLBACK: ПРОФИЛЬ ==========
@router.callback_query(F.data == "profile")
async def profile_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваш профиль!", show_alert=True)
        return
    await state.clear()
    from datetime import datetime
    user_data       = storage.get_user(callback.from_user.id)
    join_date_str   = user_data.get('join_date', datetime.now().strftime('%Y-%m-%d'))
    join_date       = datetime.strptime(join_date_str, '%Y-%m-%d')
    days_in_project = (datetime.now() - join_date).days
    update_user_name(storage, callback.from_user.id, callback.from_user.first_name or "")
    await callback.message.edit_text(
        get_profile_text(callback.from_user.first_name, days_in_project, callback.from_user.id),
        parse_mode=ParseMode.HTML, reply_markup=get_profile_menu(), disable_web_page_preview=True
    )
    _set_msg_owner(callback.message.message_id, callback.from_user.id)
    await callback.answer()


# ========== CALLBACK: ИГРЫ ==========
@router.callback_query(F.data == "games")
async def games_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text(
        get_games_menu_text(callback.from_user.id),
        parse_mode=ParseMode.HTML, reply_markup=get_games_menu(), disable_web_page_preview=True
    )
    _set_msg_owner(callback.message.message_id, callback.from_user.id)
    await callback.answer()

@router.callback_query(F.data == "mines_menu")
async def mines_menu_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    await state.clear()
    await show_mines_menu(callback, storage, betting_game)

@router.callback_query(F.data == "tower_menu")
async def tower_menu_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    await state.clear()
    await show_tower_menu(callback, storage, betting_game)

# ── ИСПРАВЛЕНО: передаём state в show_gold_menu ──────────────────────────────
@router.callback_query(F.data == "gold_menu")
async def gold_menu_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    await state.clear()
    await show_gold_menu(callback, storage, state)   # <-- state передаётся!

@router.callback_query(F.data == GAME_CALLBACKS['dice'])
async def dice_menu(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear(); await show_dice_menu(callback)

@router.callback_query(F.data == GAME_CALLBACKS['basketball'])
async def basketball_menu(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear(); await show_basketball_menu(callback)

@router.callback_query(F.data == GAME_CALLBACKS['football'])
async def football_menu(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear(); await show_football_menu(callback)

@router.callback_query(F.data == GAME_CALLBACKS['darts'])
async def darts_menu(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear(); await show_darts_menu(callback)

@router.callback_query(F.data == GAME_CALLBACKS['bowling'])
async def bowling_menu(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear(); await show_bowling_menu(callback)

@router.callback_query(F.data == "bet_dice_exact")
async def exact_number_menu(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear(); await show_exact_number_menu(callback)

@router.callback_query(F.data.startswith("bet_"))
async def handle_bet_selection(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await request_amount(callback, state, betting_game)

@router.callback_query(F.data == "cancel_bet")
async def handle_cancel_bet(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await cancel_bet(callback, state, betting_game)


# ========== CALLBACK: ПОПОЛНЕНИЕ / ВЫВОД ==========
@router.callback_query(F.data == "deposit")
async def deposit_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваш профиль!", show_alert=True); return
    await state.clear()
    storage.set_pending(callback.from_user.id, 'deposit')
    await callback.message.edit_text(
        f'<b><tg-emoji emoji-id="{EMOJI_WALLET}">💰</tg-emoji> Пополнение баланса</b>\n\n'
        f'<blockquote><i>Введите сумму пополнения:</i></blockquote>',
        parse_mode=ParseMode.HTML, reply_markup=get_cancel_menu()
    )
    await callback.answer()

@router.callback_query(F.data == "withdraw")
async def withdraw_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваш профиль!", show_alert=True); return
    await state.clear()
    storage.set_pending(callback.from_user.id, 'withdraw')
    await callback.message.edit_text(
        f'<b><tg-emoji emoji-id="{EMOJI_WITHDRAWAL}">💸</tg-emoji> Вывод средств</b>\n\n'
        f'<blockquote><i>Введите сумму вывода:</i></blockquote>',
        parse_mode=ParseMode.HTML, reply_markup=get_cancel_menu()
    )
    await callback.answer()


# ========== ПЕРЕВОД ==========
@router.message(F.text.regexp(r'(?i)^(?:/)?(?:pay|дать)\s+[\d.,]+$'))
async def handle_transfer(message: Message, state: FSMContext):
    if not message.reply_to_message:
        await message.reply(
            f'❌<b>Команда должна быть ответом на сообщение игрока!</b>\n\n'
            f'<blockquote><i>Ответьте на сообщение нужного игрока:\n'
            f'<code>дать 100</code> или <code>/pay 100</code></i></blockquote>',
            parse_mode=ParseMode.HTML
        )
        return
    target = message.reply_to_message.from_user
    if target.id == message.from_user.id:
        await message.reply("<blockquote>❌<b>Нельзя переводить самому себе!</b></blockquote>", parse_mode=ParseMode.HTML); return
    if target.is_bot:
        await message.reply("<blockquote>❌<b>Нельзя переводить ботам!</b></blockquote>", parse_mode=ParseMode.HTML); return
    match = TRANSFER_PATTERN.match(message.text.strip())
    if not match:
        return
    try:
        amount = float(match.group(1).replace(',', '.'))
    except ValueError:
        await message.reply("<blockquote>❌<b>Неверный формат суммы!</b></blockquote>", parse_mode=ParseMode.HTML); return
    if amount < MIN_TRANSFER:
        await message.reply(f"<blockquote>❌<b>Мин. сумма перевода: <code>{MIN_TRANSFER}</code>💰</b></blockquote>", parse_mode=ParseMode.HTML); return
    if amount > MAX_TRANSFER:
        await message.reply(f"<blockquote>❌<b>Макс. сумма перевода: <code>{MAX_TRANSFER:,.0f}</code>💰</b></blockquote>", parse_mode=ParseMode.HTML); return
    if storage.get_balance(message.from_user.id) < amount:
        await message.reply("<blockquote>❌<b>Недостаточно средств!</b></blockquote>", parse_mode=ParseMode.HTML); return
    lock = _get_transfer_lock(message.from_user.id)
    if lock.locked():
        await message.reply("<blockquote>⏳<b>Перевод уже обрабатывается.</b></blockquote>", parse_mode=ParseMode.HTML); return
    async with lock:
        if storage.get_balance(message.from_user.id) < amount:
            await message.reply("<blockquote>❌<b>Недостаточно средств!</b></blockquote>", parse_mode=ParseMode.HTML); return
        storage.get_user(target.id)
        storage.add_balance(message.from_user.id, -amount)
        storage.add_balance(target.id, amount)
    await message.reply(
        f"💰 <b>Перевод выполнен!</b>\n\n<blockquote>"
        f"Вы отправили <code>{amount:,.2f}</code>💰 игроку <b>{target.first_name or 'Игрок'}</b></blockquote>",
        parse_mode=ParseMode.HTML
    )


# ========== СПЕЦИФИЧНЫЕ ТЕКСТОВЫЕ КОМАНДЫ ==========
@router.message(F.text.regexp(r'(?i)^(?:/)?(?:mines|мины)\s+[\d.,]+\s+\d+$'))
async def mines_command_handler(message: Message, state: FSMContext):
    await process_mines_command(message, state, storage)

@router.message(F.text.regexp(r'(?i)^(?:/)?(?:tower|башня)\s+[\d.,]+\s+\d+$'))
async def tower_command_handler(message: Message, state: FSMContext):
    await process_tower_command(message, state, storage)

@router.message(F.text.regexp(GOLD_PATTERN))
async def gold_command_handler(message: Message, state: FSMContext):
    await process_gold_command(message, state, storage)

@router.message(F.text.regexp(GAMES_PATTERN))
async def handle_games_command(message: Message, state: FSMContext):
    await state.clear()
    sent = await message.answer(
        get_games_menu_text(message.from_user.id),
        parse_mode=ParseMode.HTML, reply_markup=get_games_menu(), disable_web_page_preview=True
    )
    _set_msg_owner(sent.message_id, message.from_user.id)

@router.message(F.text.regexp(DEP_PATTERN))
async def handle_dep_command_main(message: Message, state: FSMContext):
    from payments import _process_deposit
    m = DEP_PATTERN.match(message.text.strip())
    if not m:
        return
    try:
        amount = float(m.group(1))
    except ValueError:
        return
    await state.clear()
    storage.clear_pending(message.from_user.id)
    await _process_deposit(message, message.from_user.id, amount_override=amount)


# ========== ГЛАВНЫЙ ОБРАБОТЧИК ТЕКСТА ==========
@router.message(F.text)
async def handle_text_message(message: Message, state: FSMContext):
    from payments import handle_amount_input

    # Баланс
    if is_balance_command(message.text):
        balance = sync_balances(message.from_user.id)
        await message.reply(
            f'<blockquote><b><tg-emoji emoji-id="5278467510604160626">💰</tg-emoji> '
            f'<code>{balance:,.2f}</code> '
            f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b></blockquote>\n\n'
            f'<blockquote><i>Выберите действие ниже</i></blockquote>',
            parse_mode=ParseMode.HTML, reply_markup=get_balance_menu()
        )
        return

    if is_mygames_command(message.text):
        await handle_mygames(message)
        return

    if is_del_command(message.text):
        await handle_del(message)
        return

    current_state = await state.get_state()

    # Промокод
    if current_state == PromoState.entering_code.state:
        code = message.text.strip()
        ok, amount, reason = promo_use(code, message.from_user.id)
        if ok:
            storage.get_user(message.from_user.id)
            storage.add_balance(message.from_user.id, amount)
            new_balance = storage.get_balance(message.from_user.id)
            await state.clear()
            await message.answer(
                f'✅ <b>Промокод активирован!</b>\n\n<blockquote>'
                f'Начислено: <b><code>+{amount:.2f}</code>💰</b>\n'
                f'Баланс: <b><code>{new_balance:.2f}</code>💰</b></blockquote>',
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="На главную", callback_data="back_to_main", icon_custom_emoji_id=EMOJI_BACK)
                ]])
            )
        else:
            error_texts = {
                "not_found":    "Промокод не найден. Проверьте правильность ввода.",
                "already_used": "Вы уже активировали этот промокод.",
                "expired":      "Промокод больше не активен — все активации израсходованы.",
            }
            await message.answer(
                f"❌ <b>Ошибка активации</b>\n\n<blockquote>{error_texts.get(reason, 'Неизвестная ошибка.')}</blockquote>",
                parse_mode=ParseMode.HTML, reply_markup=get_promo_cancel_menu()
            )
        return

    # Реферальный вывод
    if current_state == ReferralWithdraw.entering_amount.state:
        await ref_withdraw_amount(message, state)
        return

    # Мины
    if current_state == MinesGame.choosing_bet:
        await process_mines_bet(message, state, storage)
        return

    # Башня
    if current_state == TowerGame.choosing_bet:
        await process_tower_bet(message, state, storage)
        return

    # ── ЗОЛОТО: перехватываем ввод ставки ────────────────────────────────────
    if current_state == GoldGame.choosing_bet.state:
        await process_gold_bet(message, state, storage)
        return

    # Дуэли
    if is_duel_command(message.text):
        await handle_duel_command(message)
        return

    # Ставки в обычных играх
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


# ========== CALLBACK: ЛИДЕРЫ / О ПРОЕКТЕ / НАЗАД ==========
@router.callback_query(F.data == "leaders")
async def leaders_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear()
    await show_leaders(callback, storage)

@router.callback_query(F.data == "about")
async def about_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear()
    await callback.message.edit_text(
        f'<tg-emoji emoji-id="{EMOJI_ABOUT}">ℹ️</tg-emoji> <b>О проекте</b>\n\n',
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Новости",    url=LINK_NEWS,     icon_custom_emoji_id=EMOJI_CHANNEL),
                InlineKeyboardButton(text="Чат",        url=LINK_CHAT,     icon_custom_emoji_id=EMOJI_CHAT),
                InlineKeyboardButton(text="Инструкция", url=LINK_INSTRUCT, icon_custom_emoji_id=EMOJI_INSTRUCT)
            ],
            [InlineKeyboardButton(text="Поддержка", url=LINK_SUPPORT, icon_custom_emoji_id=EMOJI_SUPORT)],
            [InlineKeyboardButton(text="Назад", callback_data="back_to_main", icon_custom_emoji_id=EMOJI_BACK)]
        ])
    )
    _set_msg_owner(callback.message.message_id, callback.from_user.id)
    await callback.answer()

@router.callback_query(F.data == "back_to_main")
async def back_to_main_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear()
    storage.clear_pending(callback.from_user.id)
    await callback.message.edit_text(
        get_main_menu_text(), parse_mode=ParseMode.HTML, reply_markup=get_main_menu()
    )
    _set_msg_owner(callback.message.message_id, callback.from_user.id)
    await callback.answer()


# ========== ЗАПУСК ==========
async def main():
    global betting_game

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    await init_db()
    await import_users_from_json()
    init_leaders_db()
    logging.info("База данных инициализирована")

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher(storage=MemoryStorage())

    bot_info = await bot.get_me()
    os.environ["BOT_USERNAME"] = bot_info.username
    logging.info(f"Бот запущен как @{bot_info.username}")

    betting_game = BettingGame(bot)

    # ВАЖНО: duels_router первым — F.dice перехватывается раньше всех
    dp.include_router(duels_router)
    dp.include_router(router)
    dp.include_router(mines_router)
    dp.include_router(tower_router)
    dp.include_router(gold_router)
    dp.include_router(referral_router)
    dp.include_router(payment_router)
    dp.include_router(leaders_router)

    setup_payments(bot)
    setup_referrals(bot)
    setup_duels(bot, storage)
    _inject_leaders_owner_fns()

    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("Бот запущен в режиме поллинга")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
