"""
gold.py — Игра «Золото» для Telegram-казино.

Механика:
  - 7 уровней. На каждом 3 ячейки: [множитель] [ячейка A] [ячейка B]
  - При нажатии ЛЮБОЙ ячейки: 60% — бомба (проигрыш), 40% — золото (проход).
  - Кэшаут в любой момент после прохождения хотя бы 1 уровня.
  - Таймаут 5 минут бездействия — возврат ставки.

Команды:
  gold СУММА | /gold СУММА | золото СУММА | /золото СУММА
Меню:
  callback_data="gold_menu"
"""

import random
import re
import asyncio
import logging
from aiogram import Router, F, Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ParseMode

# ─── внешние зависимости ──────────────────────────────────────────────────────
try:
    from database import save_game_result as db_save_game_result
except ImportError:
    async def db_save_game_result(user_id, game_name, score): pass

try:
    from referrals import notify_referrer_commission
except ImportError:
    async def notify_referrer_commission(user_id: int, bet_amount: float): pass

try:
    from leaders import record_game_result
except ImportError:
    def record_game_result(user_id, name, bet, win): pass

# ─── кастомные эмодзи ─────────────────────────────────────────────────────────
EMOJI_BACK    = "5906771962734057347"
EMOJI_GOAL    = "5206607081334906820"
EMOJI_3POINT  = "5397782960512444700"
EMOJI_COIN    = "5197434882321567830"
EMOJI_WIN     = "5278467510604160626"
EMOJI_BET     = "5305699699204837855"
EMOJI_MULT    = "5330320040883411678"
EMOJI_NEXT    = "5391032818111363540"
EMOJI_FLOOR   = "5197503331215361533"
EMOJI_INPUT   = "5197269100878907942"
EMOJI_LOSS    = "5447183459602669338"
EMOJI_BOMB2   = "5210952531676504517"
EMOJI_CASHOUT = "5312441427764989435"
EMOJI_TROPHY  = "5461151367559141950"
EMOJI_MULT2   = "5429651785352501917"

# ─── ячейки ───────────────────────────────────────────────────────────────────
CELL_HIDDEN   = "🌑"
CELL_GOLD     = "💰"
CELL_BOMB     = "🧨"
CELL_EXPLODE  = "💥"
CELL_SAFE_REV = "▪️"
CELL_FUTURE   = "🌑"

# ─── конфигурация ─────────────────────────────────────────────────────────────
FLOORS             = 7
CELLS              = 2
BOMB_CHANCE        = 0.60
INACTIVITY_TIMEOUT = 300

MIN_BET = 0.1
MAX_BET = 10000.0

# Множители за каждый пройденный уровень [1й..7й]
GOLD_MULTIPLIERS = [1.9, 3.8, 7.6, 14.5, 29.9, 56.78, 116.84]

# ─── FSM ──────────────────────────────────────────────────────────────────────
class GoldGame(StatesGroup):
    choosing_bet = State()
    playing      = State()

gold_router = Router()

# ─── хранилища ────────────────────────────────────────────────────────────────
_sessions:         dict = {}
_timeout_tasks:    dict = {}
_user_locks:       dict = {}
_bet_locks:        dict = {}
_game_board_owner: dict = {}

# ─── owner-функции ────────────────────────────────────────────────────────────
def _noop_set_owner(message_id: int, user_id: int): pass
def _noop_is_owner(message_id: int, user_id: int) -> bool: return True
set_owner_fn = _noop_set_owner
is_owner_fn  = _noop_is_owner


# ══════════════════════════════════════════════════════════════════
#  Вспомогательная функция: получить отображаемое имя пользователя
#  Приоритет: first_name [+ last_name] → username → "User {id}"
# ══════════════════════════════════════════════════════════════════

def _get_display_name(from_user) -> str:
    nickname = from_user.first_name or ""
    if getattr(from_user, 'last_name', None):
        nickname += f" {from_user.last_name}"
    nickname = nickname.strip()
    if nickname:
        return nickname
    if getattr(from_user, 'username', None):
        return from_user.username
    return f"User {from_user.id}"


# ══════════════════════════════════════════════════════════════════
#  Локеры
# ══════════════════════════════════════════════════════════════════

def _get_user_lock(user_id: int) -> asyncio.Lock:
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()
    return _user_locks[user_id]


def _get_bet_lock(user_id: int) -> asyncio.Lock:
    if user_id not in _bet_locks:
        _bet_locks[user_id] = asyncio.Lock()
    return _bet_locks[user_id]


# ══════════════════════════════════════════════════════════════════
#  Таймаут бездействия
# ══════════════════════════════════════════════════════════════════

def _cancel_timeout(user_id: int):
    task = _timeout_tasks.pop(user_id, None)
    if task and not task.done():
        task.cancel()


def _start_timeout(user_id: int, bot: Bot, storage):
    _cancel_timeout(user_id)
    task = asyncio.create_task(_inactivity_watcher(user_id, bot, storage))
    _timeout_tasks[user_id] = task


async def _inactivity_watcher(user_id: int, bot: Bot, storage):
    try:
        await asyncio.sleep(INACTIVITY_TIMEOUT)
    except asyncio.CancelledError:
        return

    lock = _get_user_lock(user_id)
    async with lock:
        session = _sessions.pop(user_id, None)
        if session is None:
            return
        if session.get('finishing'):
            return
        session['finishing'] = True

    bet = session.get('bet', 0)
    if bet > 0:
        storage.add_balance(user_id, bet)
        logging.info(f"[gold] Таймаут user={user_id}, ставка {bet} возвращена.")

    msg_id  = session.get('message_id')
    chat_id = session.get('chat_id')
    if msg_id and chat_id:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=(
                    "<blockquote><b>⏰ Игра закрыта</b></blockquote>\n\n"
                    "<blockquote>"
                    f"⛏ Золото\n"
                    f'<tg-emoji emoji-id="{EMOJI_BET}">💰</tg-emoji>'
                    f"Ставка <code>{bet}</code>"
                    f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji> возвращена\n'
                    "</blockquote>\n\n"
                    "<blockquote><i>Игра завершена по таймауту (5 минут бездействия).</i></blockquote>"
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="⛏ Играть снова", callback_data="gold_menu")
                ]])
            )
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════
#  Вспомогательные функции
# ══════════════════════════════════════════════════════════════════

def _has_active_game(user_id: int) -> bool:
    return user_id in _sessions


def get_multiplier(floors_passed: int) -> float:
    if floors_passed == 0:
        return 1.0
    return GOLD_MULTIPLIERS[min(floors_passed - 1, len(GOLD_MULTIPLIERS) - 1)]


def get_next_mult(floors_passed: int) -> float:
    if floors_passed >= len(GOLD_MULTIPLIERS):
        return GOLD_MULTIPLIERS[-1]
    return GOLD_MULTIPLIERS[floors_passed]


def _active_game_error_text(session: dict) -> str:
    bet           = session['bet']
    floors_passed = session['floors_passed']
    mult          = get_multiplier(floors_passed)
    return (
        f"<blockquote><b>⚠️ У вас уже есть активная игра!</b></blockquote>\n\n"
        f"<blockquote>"
        f'<tg-emoji emoji-id="{EMOJI_BET}">💰</tg-emoji>Ставка: <code>{bet}</code>'
        f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji>\n'
        f'<tg-emoji emoji-id="{EMOJI_FLOOR}">🏗</tg-emoji>Пройдено уровней: <b>{floors_passed}/{FLOORS}</b> | '
        f'<tg-emoji emoji-id="{EMOJI_MULT}">✨</tg-emoji><b>x{mult}</b>\n'
        f"</blockquote>\n\n"
        f"<blockquote><i>Завершите текущую игру прежде чем начать новую.</i></blockquote>"
    )


def _validate_bet(bet: float) -> str | None:
    """Возвращает текст ошибки или None если всё ок."""
    import math
    if not math.isfinite(bet) or bet <= 0:
        return "Некорректная сумма ставки."
    if bet < MIN_BET:
        return f"Минимальная ставка: {MIN_BET}"
    if bet > MAX_BET:
        return f"Максимальная ставка: {int(MAX_BET):,}"
    return None


def _create_session(bet: float, chat_id: int, owner_id: int = 0) -> dict:
    floors = []
    for _ in range(FLOORS):
        floors.append({
            'bomb_col': None,
            'chosen':   None,
            'is_bomb':  None,
        })
    return {
        'bet':              bet,
        'current_floor':    0,
        'floors_passed':    0,
        'floors':           floors,
        'message_id':       None,
        'chat_id':          chat_id,
        'owner_id':         owner_id,
        'finishing':        False,
        'processing_cells': set(),
    }


# ══════════════════════════════════════════════════════════════════
#  Текст игры
# ══════════════════════════════════════════════════════════════════

def game_text(session: dict) -> str:
    bet           = session['bet']
    floors_passed = session['floors_passed']
    mult          = get_multiplier(floors_passed)
    next_mult     = get_next_mult(floors_passed)
    floor_num     = session['current_floor'] + 1

    return (
        f'<blockquote><b><tg-emoji emoji-id="{EMOJI_WIN}">💰</tg-emoji>Золото</b></blockquote>\n\n'
        f"<blockquote>"
        f'<tg-emoji emoji-id="{EMOJI_BET}">💰</tg-emoji>Ставка: <code>{bet}</code><tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji>\n'
        f'<tg-emoji emoji-id="{EMOJI_FLOOR}">🏗</tg-emoji>Уровень: <b>{floor_num}/{FLOORS}</b>\n'
        f'<tg-emoji emoji-id="{EMOJI_MULT}">✨</tg-emoji>Текущий: <b><code>x{mult}</code></b>\n'
        f'<tg-emoji emoji-id="{EMOJI_NEXT}">🔜</tg-emoji>Следующий: <b><code>x{next_mult}</code></b>\n'
        f"</blockquote>\n\n"
        f"<blockquote><b><i>Выберите ячейку — за одной спрятано золото!</i></b></blockquote>"
    )


# ══════════════════════════════════════════════════════════════════
#  Клавиатура игры
# ══════════════════════════════════════════════════════════════════

def build_gold_keyboard(session: dict, game_over: bool = False) -> InlineKeyboardMarkup:
    current_floor = session['current_floor']
    floors_passed = session['floors_passed']
    floors        = session['floors']
    rows          = []

    for floor_idx in range(FLOORS - 1, -1, -1):
        floor_data = floors[floor_idx]
        chosen     = floor_data['chosen']
        bomb_col   = floor_data['bomb_col']
        mult       = GOLD_MULTIPLIERS[floor_idx]
        btn_row    = []

        btn_row.append(InlineKeyboardButton(
            text=f"x{mult}",
            callback_data="gold_noop"
        ))

        if game_over:
            for col in range(CELLS):
                if col == chosen and bomb_col == col:
                    text = CELL_EXPLODE
                elif bomb_col == col and bomb_col is not None:
                    text = CELL_BOMB
                elif col == chosen:
                    text = CELL_GOLD
                else:
                    text = CELL_SAFE_REV
                btn_row.append(InlineKeyboardButton(text=text, callback_data="gold_noop"))

        elif floor_idx < current_floor:
            for col in range(CELLS):
                text = CELL_GOLD if col == chosen else CELL_HIDDEN
                btn_row.append(InlineKeyboardButton(text=text, callback_data="gold_noop"))

        elif floor_idx == current_floor:
            for col in range(CELLS):
                btn_row.append(InlineKeyboardButton(
                    text=CELL_HIDDEN,
                    callback_data=f"gold_cell_{floor_idx}_{col}"
                ))

        else:
            for col in range(CELLS):
                btn_row.append(InlineKeyboardButton(text=CELL_FUTURE, callback_data="gold_noop"))

        rows.append(btn_row)

    if not game_over:
        ctrl = []
        if floors_passed > 0:
            mult    = get_multiplier(floors_passed)
            cashout = round(session['bet'] * mult, 2)
            ctrl.append(InlineKeyboardButton(
                text=f"Забрать {cashout}",
                callback_data="gold_cashout",
                icon_custom_emoji_id=EMOJI_GOAL
            ))
        ctrl.append(InlineKeyboardButton(
            text="Выйти",
            callback_data="gold_exit",
            icon_custom_emoji_id=EMOJI_BACK
        ))
        rows.append(ctrl)
    else:
        rows.append([
            InlineKeyboardButton(
                text="Снова",
                callback_data="gold_play_again",
                icon_custom_emoji_id=EMOJI_3POINT
            ),
            InlineKeyboardButton(
                text="Выйти",
                callback_data="gold_exit",
                icon_custom_emoji_id=EMOJI_BACK
            ),
        ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


# ══════════════════════════════════════════════════════════════════
#  Публичная функция входа
# ══════════════════════════════════════════════════════════════════

async def show_gold_menu(callback: CallbackQuery, storage, state: FSMContext = None):
    user_id = callback.from_user.id

    if _has_active_game(user_id):
        await callback.answer(
            "⚠️ У вас уже есть активная игра! Завершите её прежде чем начать новую.",
            show_alert=True
        )
        return

    text = (
        f'<blockquote><b><tg-emoji emoji-id="{EMOJI_WIN}">🏆</tg-emoji> Золото</b></blockquote>\n\n'
        f'<blockquote><tg-emoji emoji-id="{EMOJI_INPUT}">✏️</tg-emoji> <b>Введите сумму ставки:</b></blockquote>'
    )
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="games",
                icon_custom_emoji_id=EMOJI_BACK
            )
        ]])
    )
    set_owner_fn(callback.message.message_id, user_id)

    if state is not None:
        await state.set_state(GoldGame.choosing_bet)

    await callback.answer()


# ══════════════════════════════════════════════════════════════════
#  Хендлеры callback
# ══════════════════════════════════════════════════════════════════

@gold_router.callback_query(F.data == "gold_menu")
async def gold_menu_callback(callback: CallbackQuery, state: FSMContext):
    if not is_owner_fn(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    from payments import storage as pay_storage
    await state.clear()
    await show_gold_menu(callback, pay_storage, state)


@gold_router.callback_query(F.data == "gold_play_again")
async def gold_play_again(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    caller_id   = callback.from_user.id
    msg_id      = callback.message.message_id
    board_owner = _game_board_owner.get(msg_id)
    if board_owner is None or board_owner != caller_id:
        await callback.answer("🚫 Это не ваша игра!", show_alert=True)
        return
    _sessions.pop(caller_id, None)
    _cancel_timeout(caller_id)
    await state.clear()
    await show_gold_menu(callback, pay_storage, state)


@gold_router.callback_query(F.data == "gold_exit")
async def gold_exit(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    caller_id   = callback.from_user.id
    msg_id      = callback.message.message_id
    board_owner = _game_board_owner.get(msg_id)
    if board_owner is None or board_owner != caller_id:
        await callback.answer("🚫 Это не ваша игра!", show_alert=True)
        return

    session = _sessions.get(caller_id)
    if session and not session.get('finishing'):
        bet = session.get('bet', 0)
        if bet > 0:
            pay_storage.add_balance(caller_id, bet)
        _sessions.pop(caller_id, None)
        _cancel_timeout(caller_id)

    await state.clear()
    from main import get_games_menu, get_games_menu_text
    await callback.message.edit_text(
        get_games_menu_text(caller_id),
        parse_mode="HTML",
        reply_markup=get_games_menu()
    )
    await callback.answer()


@gold_router.callback_query(F.data == "gold_noop")
async def gold_noop(callback: CallbackQuery):
    await callback.answer()


@gold_router.callback_query(F.data.startswith("gold_cell_"))
async def gold_cell_handler(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    caller_id = callback.from_user.id
    msg_id    = callback.message.message_id

    # ── Валидация callback_data ────────────────────────────────────
    parts = callback.data.split("_")
    if len(parts) != 4:
        await callback.answer()
        return
    try:
        floor_idx = int(parts[2])
        col       = int(parts[3])
    except ValueError:
        await callback.answer()
        return

    # ── Проверка диапазонов ────────────────────────────────────────
    if floor_idx < 0 or floor_idx >= FLOORS or col < 0 or col >= CELLS:
        await callback.answer()
        return

    board_owner = _game_board_owner.get(msg_id)
    if board_owner is None or board_owner != caller_id:
        await callback.answer("🚫 Это не ваша игра!", show_alert=True)
        return

    session = _sessions.get(caller_id)
    user_id = caller_id

    if not session:
        await callback.answer("🚫 Игра не найдена!", show_alert=True)
        return
    if session.get('message_id') != msg_id:
        await callback.answer("🚫 Это не ваша игра!", show_alert=True)
        return
    if session.get('finishing'):
        await callback.answer()
        return
    if floor_idx != session['current_floor']:
        await callback.answer()
        return

    processing = session.setdefault('processing_cells', set())
    if col in processing:
        await callback.answer()
        return

    lock = _get_user_lock(user_id)
    async with lock:
        session = _sessions.get(user_id)
        if not session:
            await callback.answer("🚫 Игра уже завершена!", show_alert=True)
            return
        if session.get('finishing'):
            await callback.answer()
            return
        if floor_idx != session['current_floor']:
            await callback.answer()
            return
        processing = session.setdefault('processing_cells', set())
        if col in processing:
            await callback.answer()
            return
        processing.add(col)

    try:
        _start_timeout(user_id, callback.bot, pay_storage)

        floor_data = session['floors'][floor_idx]
        floor_data['chosen'] = col

        is_bomb = random.random() < BOMB_CHANCE
        floor_data['is_bomb'] = is_bomb

        if is_bomb:
            floor_data['bomb_col'] = col
        else:
            floor_data['bomb_col'] = 1 - col

        display_name = _get_display_name(callback.from_user)

        if is_bomb:
            # ══ ПРОИГРЫШ ══════════════════════════════════════════
            bet = session['bet']

            lock = _get_user_lock(user_id)
            async with lock:
                if session.get('finishing'):
                    return
                session['finishing'] = True
                _sessions.pop(user_id, None)

            _cancel_timeout(user_id)
            await state.clear()

            for fi in range(floor_idx + 1, FLOORS):
                if session['floors'][fi]['bomb_col'] is None:
                    session['floors'][fi]['bomb_col'] = random.randint(0, 1)

            record_game_result(user_id, display_name, bet, 0.0)
            asyncio.create_task(db_save_game_result(user_id, 'gold', 0.0))

            balance = pay_storage.get_balance(user_id)
            await callback.message.edit_text(
                f'<blockquote><b><tg-emoji emoji-id="{EMOJI_BOMB2}">💥</tg-emoji>'
                f"Вы нашли бомбу!</b></blockquote>\n\n"
                f"<blockquote>"
                f'<tg-emoji emoji-id="{EMOJI_LOSS}">💸</tg-emoji>Потеряно: '
                f"<code>{bet}</code>"
                f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji>\n'
                f'<tg-emoji emoji-id="{EMOJI_WIN}">🏆</tg-emoji>Баланс: '
                f"<code>{balance:.2f}</code>"
                f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji>'
                f"</blockquote>\n\n"
                f"<blockquote><b><i>Шахта обвалилась! Попробуйте снова!</i></b></blockquote>",
                parse_mode=ParseMode.HTML,
                reply_markup=build_gold_keyboard(session, game_over=True)
            )
            set_owner_fn(callback.message.message_id, user_id)
            _game_board_owner[callback.message.message_id] = user_id
            await callback.answer("💥 Бомба!")

        else:
            # ══ УСПЕХ ═════════════════════════════════════════════
            session['floors_passed'] += 1
            session['current_floor'] += 1
            floors_passed = session['floors_passed']
            mult          = get_multiplier(floors_passed)

            if session['current_floor'] >= FLOORS:
                # ── ВСЕ УРОВНИ ПРОЙДЕНЫ ───────────────────────────
                bet = session['bet']

                lock = _get_user_lock(user_id)
                async with lock:
                    if session.get('finishing'):
                        return
                    session['finishing'] = True
                    _sessions.pop(user_id, None)

                winnings = round(bet * mult, 2)
                pay_storage.add_balance(user_id, winnings)
                _cancel_timeout(user_id)
                await state.clear()

                record_game_result(user_id, display_name, bet, winnings)
                asyncio.create_task(db_save_game_result(user_id, 'gold', winnings))

                balance = pay_storage.get_balance(user_id)
                await callback.message.edit_text(
                    f'<blockquote><b><tg-emoji emoji-id="{EMOJI_TROPHY}">🏆</tg-emoji>'
                    f"Вы добыли всё золото!</b></blockquote>\n\n"
                    f"<blockquote>"
                    f'<tg-emoji emoji-id="{EMOJI_MULT2}">✨</tg-emoji>Множитель: <b>x{mult}</b>\n'
                    f'<tg-emoji emoji-id="{EMOJI_BET}">💰</tg-emoji>Выигрыш: '
                    f"<code>{winnings}</code>"
                    f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji>\n'
                    f'<tg-emoji emoji-id="{EMOJI_WIN}">🏆</tg-emoji>Баланс: '
                    f"<code>{balance:.2f}</code>"
                    f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji>'
                    f"</blockquote>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(
                            text="Играть снова",
                            callback_data="gold_cashout_again",
                            icon_custom_emoji_id=EMOJI_3POINT
                        )],
                        [InlineKeyboardButton(
                            text="Выйти",
                            callback_data="gold_cashout_exit",
                            icon_custom_emoji_id=EMOJI_BACK
                        )],
                    ])
                )
                set_owner_fn(callback.message.message_id, user_id)
                _game_board_owner[callback.message.message_id] = user_id
                await callback.answer("🏆 Победа!")

            else:
                # ── Следующий уровень ─────────────────────────────
                await callback.message.edit_text(
                    game_text(session),
                    parse_mode=ParseMode.HTML,
                    reply_markup=build_gold_keyboard(session)
                )
                await callback.answer(f"💰 x{mult}")

    finally:
        session = _sessions.get(user_id)
        if session:
            session.get('processing_cells', set()).discard(col)


@gold_router.callback_query(F.data == "gold_cashout")
async def gold_cashout(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    user_id = callback.from_user.id
    msg_id  = callback.message.message_id

    board_owner = _game_board_owner.get(msg_id)
    if board_owner is None or board_owner != user_id:
        await callback.answer("🚫 Это не ваша игра!", show_alert=True)
        return

    lock = _get_user_lock(user_id)
    async with lock:
        session = _sessions.get(user_id)
        if not session:
            await callback.answer("Игра не найдена.", show_alert=True)
            return
        if session.get('message_id') != msg_id:
            await callback.answer("🚫 Это не ваша игра!", show_alert=True)
            return
        if session.get('finishing'):
            await callback.answer()
            return

        floors_passed = session['floors_passed']
        if floors_passed == 0:
            await callback.answer("Сначала пройдите хотя бы один уровень!", show_alert=True)
            return

        session['finishing'] = True
        _sessions.pop(user_id, None)

    bet      = session['bet']
    mult     = get_multiplier(floors_passed)
    winnings = round(bet * mult, 2)

    pay_storage.add_balance(user_id, winnings)
    _cancel_timeout(user_id)
    await state.clear()

    display_name = _get_display_name(callback.from_user)
    record_game_result(user_id, display_name, bet, winnings)
    asyncio.create_task(db_save_game_result(user_id, 'gold', winnings))

    balance = pay_storage.get_balance(user_id)
    await callback.message.edit_text(
        f'<blockquote><b><tg-emoji emoji-id="{EMOJI_CASHOUT}">💸</tg-emoji>Кэшаут!</b></blockquote>\n\n'
        f"<blockquote>"
        f'<tg-emoji emoji-id="{EMOJI_MULT2}">✨</tg-emoji>Множитель: <b>x{mult}</b>\n'
        f'<tg-emoji emoji-id="{EMOJI_BET}">💰</tg-emoji>Выигрыш: '
        f"<code>{winnings}</code>"
        f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji>\n'
        f'<tg-emoji emoji-id="{EMOJI_WIN}">🏆</tg-emoji>Баланс: '
        f"<code>{balance:.2f}</code>"
        f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji>'
        f"</blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Играть снова",
                callback_data="gold_cashout_again",
                icon_custom_emoji_id=EMOJI_3POINT
            )],
            [InlineKeyboardButton(
                text="Выйти",
                callback_data="gold_cashout_exit",
                icon_custom_emoji_id=EMOJI_BACK
            )],
        ])
    )
    set_owner_fn(callback.message.message_id, user_id)
    await callback.answer(f"💰 +{winnings}!")


@gold_router.callback_query(F.data == "gold_cashout_again")
async def gold_cashout_again(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    caller_id   = callback.from_user.id
    msg_id      = callback.message.message_id
    board_owner = _game_board_owner.get(msg_id)
    if board_owner is None or board_owner != caller_id:
        await callback.answer("🚫 Это не ваша игра!", show_alert=True)
        return
    await state.clear()
    await show_gold_menu(callback, pay_storage, state)


@gold_router.callback_query(F.data == "gold_cashout_exit")
async def gold_cashout_exit(callback: CallbackQuery, state: FSMContext):
    caller_id   = callback.from_user.id
    msg_id      = callback.message.message_id
    board_owner = _game_board_owner.get(msg_id)
    if board_owner is None or board_owner != caller_id:
        await callback.answer("🚫 Это не ваша игра!", show_alert=True)
        return
    await state.clear()
    from main import get_games_menu, get_games_menu_text
    await callback.message.edit_text(
        get_games_menu_text(caller_id),
        parse_mode="HTML",
        reply_markup=get_games_menu()
    )
    await callback.answer()


# ══════════════════════════════════════════════════════════════════
#  Внутренняя функция запуска игры
# ══════════════════════════════════════════════════════════════════

async def _start_game(
    send_fn,
    user_id: int,
    bet: float,
    chat_id: int,
    bot: Bot,
    storage,
    state: FSMContext,
    from_user,
):
    storage.deduct_balance(user_id, bet)
    asyncio.create_task(notify_referrer_commission(user_id, bet))

    session = _create_session(bet, chat_id, user_id)
    _sessions[user_id] = session
    await state.set_state(GoldGame.playing)

    sent = await send_fn(
        game_text(session),
        build_gold_keyboard(session)
    )
    session['message_id'] = sent.message_id
    set_owner_fn(sent.message_id, user_id)
    _game_board_owner[sent.message_id] = user_id
    _start_timeout(user_id, bot, storage)


# ══════════════════════════════════════════════════════════════════
#  Обработка ставки через FSM (вызов из main.py)
# ══════════════════════════════════════════════════════════════════

async def process_gold_bet(message: Message, state: FSMContext, storage):
    """Вызывается из main.py когда state == GoldGame.choosing_bet."""
    user_id = message.from_user.id

    if _has_active_game(user_id):
        await message.answer(_active_game_error_text(_sessions[user_id]), parse_mode=ParseMode.HTML)
        return

    bet_lock = _get_bet_lock(user_id)
    if bet_lock.locked():
        return

    async with bet_lock:
        if _has_active_game(user_id):
            await message.answer(_active_game_error_text(_sessions[user_id]), parse_mode=ParseMode.HTML)
            return

        try:
            bet = float(message.text.replace(',', '.'))
        except ValueError:
            await message.answer(
                f'<blockquote><tg-emoji emoji-id="{EMOJI_LOSS}">❌</tg-emoji> Введите корректную сумму.</blockquote>',
                parse_mode=ParseMode.HTML
            )
            return

        err = _validate_bet(bet)
        if err:
            await message.answer(
                f'<blockquote><b><tg-emoji emoji-id="{EMOJI_LOSS}">❌</tg-emoji> {err}</b></blockquote>',
                parse_mode=ParseMode.HTML
            )
            return

        balance = storage.get_balance(user_id)
        if bet > balance:
            await message.answer(
                f'<blockquote><b><tg-emoji emoji-id="{EMOJI_LOSS}">❌</tg-emoji> Недостаточно средств!</b>\n'
                f'<tg-emoji emoji-id="{EMOJI_WIN}">🏆</tg-emoji>Баланс: <code>{balance:.2f}</code>'
                f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji></blockquote>',
                parse_mode=ParseMode.HTML
            )
            return

        async def _send(text, kb):
            return await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)

        await _start_game(
            send_fn=_send,
            user_id=user_id,
            bet=bet,
            chat_id=message.chat.id,
            bot=message.bot,
            storage=storage,
            state=state,
            from_user=message.from_user,
        )


# ══════════════════════════════════════════════════════════════════
#  Обработка команды gold/золото (вызов из main.py)
# ══════════════════════════════════════════════════════════════════

async def process_gold_command(message: Message, state: FSMContext, storage):
    """
    Обрабатывает: gold 100 | /gold 100 | золото 100 | /золото 100
    """
    text  = message.text.strip()
    match = re.match(r'^(?:/)?(?:gold|золото)\s+([\d.,]+)$', text, re.IGNORECASE)
    if not match:
        await message.answer(
            f'<blockquote><b><tg-emoji emoji-id="{EMOJI_LOSS}">❌</tg-emoji> Неверный формат!</b>\n'
            f'Пример: <code>gold 100</code> или <code>золото 50</code></blockquote>',
            parse_mode=ParseMode.HTML
        )
        return

    try:
        bet = float(match.group(1).replace(',', '.'))
    except ValueError:
        await message.answer(
            f'<blockquote><tg-emoji emoji-id="{EMOJI_LOSS}">❌</tg-emoji> Введите корректную сумму.</blockquote>',
            parse_mode=ParseMode.HTML
        )
        return

    err = _validate_bet(bet)
    if err:
        await message.answer(
            f'<blockquote><b><tg-emoji emoji-id="{EMOJI_LOSS}">❌</tg-emoji> {err}</b></blockquote>',
            parse_mode=ParseMode.HTML
        )
        return

    user_id = message.from_user.id

    if _has_active_game(user_id):
        await message.answer(_active_game_error_text(_sessions[user_id]), parse_mode=ParseMode.HTML)
        return

    bet_lock = _get_bet_lock(user_id)
    if bet_lock.locked():
        return

    async with bet_lock:
        if _has_active_game(user_id):
            await message.answer(_active_game_error_text(_sessions[user_id]), parse_mode=ParseMode.HTML)
            return

        balance = storage.get_balance(user_id)
        if bet > balance:
            await message.answer(
                f'<blockquote><b><tg-emoji emoji-id="{EMOJI_LOSS}">❌</tg-emoji> Недостаточно средств!</b>\n'
                f'<tg-emoji emoji-id="{EMOJI_WIN}">🏆</tg-emoji>Баланс: <code>{balance:.2f}</code>'
                f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji></blockquote>',
                parse_mode=ParseMode.HTML
            )
            return

        async def _send(text, kb):
            return await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)

        await _start_game(
            send_fn=_send,
            user_id=user_id,
            bet=bet,
            chat_id=message.chat.id,
            bot=message.bot,
            storage=storage,
            state=state,
            from_user=message.from_user,
        )
