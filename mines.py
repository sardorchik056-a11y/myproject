import random
import re
import asyncio
import logging
from aiogram import Router, F, Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ParseMode

# База данных
try:
    from database import save_game_result as db_save_game_result, update_balance as db_update_balance
except ImportError:
    async def db_save_game_result(user_id, game_name, score): pass
    async def db_update_balance(user_id, amount): return None

# Реферальная система
try:
    from referrals import notify_referrer_commission
except ImportError:
    async def notify_referrer_commission(user_id: int, bet_amount: float):
        pass

# Модуль лидеров
try:
    from leaders import record_game_result
except ImportError:
    def record_game_result(user_id, name, bet, win):
        pass

# ========== EMOJI IDS ==========
EMOJI_BACK   = "5906771962734057347"
EMOJI_GOAL   = "5206607081334906820"
EMOJI_3POINT = "5397782960512444700"
EMOJI_NUMBER = "5456140674028019486"

GRID_SIZE = 5  # 5x5 = 25 клеток
INACTIVITY_TIMEOUT = 300  # 5 минут в секундах

# ========== СКРЫТЫЕ МИНЫ ==========
HIDDEN_MINES = {
    2: 1, 3: 1, 4: 2, 5: 2, 6: 2, 7: 2, 8: 2, 9: 2, 10: 2,
    11: 3, 12: 3, 13: 3, 14: 3, 15: 3, 16: 3,
    17: 2, 18: 2, 19: 2, 20: 2, 21: 2,
    22: 1, 23: 0, 24: 0,
}

CELL_CLOSED  = "🌑"
CELL_GEM     = "💎"
CELL_MINE    = "💣"
CELL_EXPLODE = "💥"

# ========== МНОЖИТЕЛИ ==========
MINES_MULTIPLIERS = {
    2:  [1.08, 1.22, 1.36, 1.52, 1.71, 1.93, 2.19, 2.50, 2.87, 3.32, 3.87, 4.55, 5.39, 6.45, 7.80, 9.55, 11.85, 14.95, 19.25, 25.25, 33.75, 55.75, 83.25],
    3:  [1.15, 1.33, 1.55, 1.82, 2.15, 2.56, 3.07, 3.72, 4.55, 5.62, 7.02, 8.87, 11.35, 14.70, 25.30, 36.70, 49.80, 79.10, 99.80, 137.50, 195.00, 415.00],
    4:  [1.20, 1.44, 1.73, 2.07, 2.49, 3.00, 3.62, 4.38, 5.32, 6.50, 7.98, 9.85, 14.20, 19.20, 27.10, 35.20, 43.80, 59.50, 85.20, 235.80, 678.80],
    5:  [1.25, 1.56, 1.95, 2.44, 3.05, 3.81, 4.77, 5.98, 7.50, 9.42, 11.85, 19.95, 28.90, 39.95, 55.40, 79.70, 123.40, 163.20, 281.10, 1004.00],
    6:  [1.30, 1.69, 2.20, 3.86, 5.71, 8.83, 11.28, 16.17, 27.62, 46.81, 78.95, 135.34, 230.34, 339.44, 551.27, 966.65, 2386.65, 5112.64, 10046.43],
    7:  [1.35, 1.82, 2.46, 3.82, 5.48, 8.05, 15.16, 26.02, 67.88, 120.08, 227.11, 536.60, 1049.41, 3366.70, 7090.05, 15121.57, 26004.12, 40021.56],
    8:  [1.43, 2.14, 3.08, 5.16, 8.33, 13.88, 23.98, 43.16, 81.52, 163.03, 349.36, 815.17, 2119.45, 6358.35, 23313.95, 116569.75, 1049127.75],
    9:  [1.52, 2.42, 3.98, 6.74, 11.8, 21.45, 40.76, 81.52, 173.22, 395.94, 989.85, 2771.59, 9007.66, 36030.65, 198168.57, 1981685.75],
    10: [1.62, 2.77, 4.9, 8.99, 17.16, 34.32, 72.46, 163.03, 395.94, 1055.84, 3167.53, 11086.35, 48040.87, 288245.2, 3170697.2],
    11: [1.73, 3.2, 6.13, 12.26, 25.74, 57.21, 135.86, 349.36, 989.85, 3167.53, 11878.24, 55431.77, 360306.5, 4323678.0],
    12: [1.87, 3.73, 7.8, 17.16, 40.04, 100.11, 271.72, 815.17, 2771.59, 11086.35, 55431.77, 388022.38, 5044291.0],
    13: [2.02, 4.41, 10.14, 24.79, 65.07, 185.92, 588.74, 2119.45, 9007.66, 48040.87, 360306.5, 5044291.0],
    14: [2.2, 5.29, 13.52, 37.18, 111.55, 371.83, 1412.97, 6358.35, 36030.65, 288245.2, 4323678.0],
    15: [2.42, 6.47, 18.59, 58.43, 204.51, 818.03, 3885.66, 23313.95, 198168.57, 3170697.2],
    16: [2.69, 8.08, 26.56, 97.38, 409.02, 2045.08, 12952.19, 116569.75, 1981685.75],
    17: [3.03, 10.39, 39.84, 175.29, 920.29, 6135.25, 58284.88, 1049127.75],
    18: [3.46, 13.86, 63.74, 350.59, 2454.1, 24541.0, 466279.0],
    19: [4.04, 19.4, 111.55, 818.03, 8589.35, 171787.0],
    20: [4.85, 29.1, 223.1, 2454.1, 15536.1],
    21: [6.06, 48.5, 557.75, 4270.5],
    22: [8.08, 97.0, 2231.0],
    23: [12.12, 277.0],
    24: [24.75],
}


# ========== FSM ==========
class MinesGame(StatesGroup):
    choosing_bet = State()
    playing      = State()


mines_router = Router()
_sessions: dict      = {}   # user_id -> session dict
_timeout_tasks: dict = {}   # user_id -> asyncio.Task

# Функции владельца — инжектируются из main.py при старте
def _noop_set_owner(message_id: int, user_id: int): pass
def _noop_is_owner(message_id: int, user_id: int) -> bool: return True
set_owner_fn = _noop_set_owner
is_owner_fn  = _noop_is_owner

# ========== ЗАЩИТА ОТ ДУБЛЕЙ ==========
# Локи на пользователя — предотвращают race condition при одновременных нажатиях
_user_locks: dict = {}          # user_id -> asyncio.Lock
# Обработанные клетки в текущей сессии хранятся прямо в session['processing_cells']
# Кэшауты/завершения игры — флаг в session['finishing']
# Ставки в процессе создания — предотвращают двойную ставку
_bet_locks: dict    = {}        # user_id -> asyncio.Lock — для создания ставки
_last_owner: dict   = {}        # user_id -> int — владелец последней игры (для post-game кнопок)
_bet_in_progress: set = set()   # user_id — ставка сейчас обрабатывается
_game_board_owner: dict = {}    # message_id -> owner_id — надёжная привязка доски к игроку


def _get_user_lock(user_id: int) -> asyncio.Lock:
    """Возвращает персональный локер для пользователя."""
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()
    return _user_locks[user_id]


def _get_bet_lock(user_id: int) -> asyncio.Lock:
    """Возвращает локер для создания ставки."""
    if user_id not in _bet_locks:
        _bet_locks[user_id] = asyncio.Lock()
    return _bet_locks[user_id]


def _check_owner(callback_user_id: int, session: dict) -> bool:
    """Проверяет что нажавший кнопку — владелец игры."""
    owner = session.get('owner_id', 0)
    return owner == 0 or callback_user_id == owner


def _check_post_game_owner(owner_user_id: int, callback_user_id: int) -> bool:
    """Проверяет владельца для post-game кнопок (когда сессии уже нет)."""
    owner = _last_owner.get(owner_user_id)
    # Если владелец неизвестен — запрещаем (не пускаем чужих)
    if owner is None:
        return False
    return callback_user_id == owner


# ========== ТАЙМАУТ БЕЗДЕЙСТВИЯ ==========

def _cancel_timeout(user_id: int):
    """Отменяет таймер бездействия если он есть."""
    task = _timeout_tasks.pop(user_id, None)
    if task and not task.done():
        task.cancel()


def _start_timeout(user_id: int, bot: Bot, storage):
    """Запускает/перезапускает таймер бездействия на 5 минут."""
    _cancel_timeout(user_id)
    task = asyncio.create_task(_inactivity_watcher(user_id, bot, storage))
    _timeout_tasks[user_id] = task


async def _inactivity_watcher(user_id: int, bot: Bot, storage):
    """Ждёт 5 минут без активности, потом удаляет игру и возвращает ставку."""
    try:
        await asyncio.sleep(INACTIVITY_TIMEOUT)
    except asyncio.CancelledError:
        return

    lock = _get_user_lock(user_id)
    async with lock:
        session = _sessions.pop(user_id, None)
        if session is None:
            return

        # Помечаем как завершённую чтобы другие обработчики не сработали
        if session.get('finishing'):
            return
        session['finishing'] = True

    # Возвращаем ставку
    bet = session.get('bet', 0)
    if bet > 0:
        storage.add_balance(user_id, bet)
        logging.info(f"[mines] Таймаут user={user_id}, ставка {bet} возвращена.")

    # Обновляем сообщение — «Игра закрыта»
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
                    "💣 Мины\n"
                    f"<tg-emoji emoji-id=\"5305699699204837855\">🎰</tg-emoji>"
                    f"Ставка <code>{bet}</code>"
                    "<tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji> возвращена\n"
                    "</blockquote>\n\n"
                    "<blockquote><i>Игра завершена по таймауту (5 минут бездействия).</i></blockquote>"
                ),
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="💣 Играть снова", callback_data="mines_menu")
                ]])
            )
        except Exception:
            pass


# ========== ХЕЛПЕРЫ ==========

def _has_active_game(user_id: int) -> bool:
    return user_id in _sessions


def _active_game_error_text(session: dict) -> str:
    mines = session['mines_count']
    bet   = session['bet']
    gems  = session.get('gems_opened', 0)
    mult  = get_multiplier(mines, gems)
    return (
        f"<blockquote><b>⚠️ У вас уже есть активная игра!</b></blockquote>\n\n"
        f"<blockquote>"
        f"💣 Мин: <b>{mines}</b>\n"
        f"<tg-emoji emoji-id=\"5305699699204837855\">🎰</tg-emoji>Ставка: <code>{bet}</code>"
        f"<tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji>\n"
        f"💎 Открыто: <b>{gems}</b> | "
        f"<tg-emoji emoji-id=\"5330320040883411678\">🎰</tg-emoji><b>x{mult}</b>\n"
        f"</blockquote>\n\n"
        f"<blockquote><i>Завершите текущую игру прежде чем начать новую.</i></blockquote>"
    )


def get_multiplier(mines_count: int, gems_opened: int) -> float:
    if gems_opened == 0:
        return 1.0
    mults = MINES_MULTIPLIERS.get(mines_count, [])
    if not mults:
        return 1.0
    return mults[min(gems_opened - 1, len(mults) - 1)]


def get_next_mult(mines_count: int, gems_opened: int) -> float:
    mults = MINES_MULTIPLIERS.get(mines_count, [])
    if not mults or gems_opened >= len(mults):
        return get_multiplier(mines_count, gems_opened)
    return mults[gems_opened]


def generate_board(mines_count: int) -> tuple:
    hidden = HIDDEN_MINES.get(mines_count, 0)
    total_mines = mines_count + hidden
    total_mines = min(total_mines, GRID_SIZE * GRID_SIZE - 1)

    all_positions  = random.sample(range(GRID_SIZE * GRID_SIZE), total_mines)
    real_positions = set(all_positions[:mines_count])

    board = [False] * (GRID_SIZE * GRID_SIZE)
    for pos in all_positions:
        board[pos] = True

    return board, real_positions


def build_game_keyboard(session: dict, game_over: bool = False) -> InlineKeyboardMarkup:
    board    = session['board']
    revealed = session['revealed']
    rows = []

    for row in range(GRID_SIZE):
        btn_row = []
        for col in range(GRID_SIZE):
            idx            = row * GRID_SIZE + col
            is_mine        = board[idx]
            is_open        = revealed[idx]
            real_positions = session.get('real_positions', set())
            is_real_mine   = idx in real_positions

            if is_open:
                text = CELL_EXPLODE if (is_mine and is_real_mine) else CELL_GEM
                cb   = "mines_noop"
            elif game_over and is_real_mine:
                text = CELL_MINE
                cb   = "mines_noop"
            elif game_over:
                text = CELL_GEM
                cb   = "mines_noop"
            else:
                text = CELL_CLOSED
                cb   = f"mines_cell_{idx}"

            btn_row.append(InlineKeyboardButton(text=text, callback_data=cb))
        rows.append(btn_row)

    if not game_over:
        gems    = session.get('gems_opened', 0)
        mult    = get_multiplier(session['mines_count'], gems)
        cashout = round(session['bet'] * mult, 2)
        ctrl = []
        if gems > 0:
            ctrl.append(InlineKeyboardButton(
                text=f"Забрать {cashout}",
                callback_data="mines_cashout",
                icon_custom_emoji_id=EMOJI_GOAL
            ))
        ctrl.append(InlineKeyboardButton(
            text="Выйти",
            callback_data="mines_exit",
            icon_custom_emoji_id=EMOJI_BACK
        ))
        rows.append(ctrl)
    else:
        rows.append([
            InlineKeyboardButton(
                text="Снова",
                callback_data="mines_play_again",
                icon_custom_emoji_id=EMOJI_3POINT
            ),
            InlineKeyboardButton(
                text="Выйти",
                callback_data="mines_exit",
                icon_custom_emoji_id=EMOJI_BACK
            ),
        ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_mines_select_keyboard() -> InlineKeyboardMarkup:
    presets = [2, 5, 10, 15, 18]
    row = [
        InlineKeyboardButton(text=f"💣 {m}", callback_data=f"mines_select_{m}")
        for m in presets
    ]
    return InlineKeyboardMarkup(inline_keyboard=[
        row,
        [InlineKeyboardButton(
            text="Ввести вручную",
            callback_data="mines_manual",
            icon_custom_emoji_id=EMOJI_NUMBER
        )],
        [InlineKeyboardButton(
            text="Назад",
            callback_data="games",
            icon_custom_emoji_id=EMOJI_BACK
        )]
    ])


def game_text(session: dict) -> str:
    mines      = session['mines_count']
    bet        = session['bet']
    gems       = session.get('gems_opened', 0)
    mult       = get_multiplier(mines, gems)
    next_mult  = get_next_mult(mines, gems)

    return (
        f"<blockquote><b>💣 Мины</b></blockquote>\n\n"
        f"<blockquote>"
        f"<tg-emoji emoji-id=\"5305699699204837855\">🎰</tg-emoji>Ставка: <code>{bet}</code><tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji>\n"
        f"💣 Мин: <b>{mines}</b>\n"
        f"<tg-emoji emoji-id=\"5330320040883411678\">🎰</tg-emoji>Текущий: <b><code>x{mult}</code></b>\n"
        f"<tg-emoji emoji-id=\"5391032818111363540\">🎰</tg-emoji>Следующий: <b><code>x{next_mult}</code></b>\n"
        f"</blockquote>\n\n"
        f"<blockquote><b><i>Игра началась! Выберите безопасную ячейку!</i></b></blockquote>"
    )


# ========== ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ СОЗДАНИЯ СЕССИИ ==========

def _create_session(mines_count: int, bet: float, chat_id: int, owner_id: int = 0) -> dict:
    board, real_positions = generate_board(mines_count)
    return {
        'board':            board,
        'real_positions':   real_positions,
        'revealed':         [False] * (GRID_SIZE * GRID_SIZE),
        'mines_count':      mines_count,
        'bet':              bet,
        'gems_opened':      0,
        'exploded_idx':     -1,
        'message_id':       None,
        'chat_id':          chat_id,
        'owner_id':         owner_id,
        'finishing':        False,
        'processing_cells': set(),
    }


# ========== ПУБЛИЧНАЯ ФУНКЦИЯ ВХОДА ==========

async def show_mines_menu(callback: CallbackQuery, storage, betting_game):
    user_id = callback.from_user.id

    if _has_active_game(user_id):
        await callback.answer(
            "⚠️ У вас уже есть активная игра! Завершите её прежде чем начать новую.",
            show_alert=True
        )
        return

    balance = storage.get_balance(user_id)
    text = (
        f"<blockquote><b>💣 Мины</b></blockquote>\n\n"
        f"<blockquote><b><tg-emoji emoji-id=\"5278467510604160626\">🎰</tg-emoji>: "
        f"<code>{balance:.2f}</code><tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji></b></blockquote>\n\n"
        f"<blockquote><b>Выберите количество мин:</b></blockquote>\n"
    )
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_mines_select_keyboard()
    )
    set_owner_fn(callback.message.message_id, callback.from_user.id)
    await callback.answer()


# ========== ХЕНДЛЕРЫ ==========

@mines_router.callback_query(F.data.startswith("mines_select_"))
async def mines_select_handler(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id

    if not is_owner_fn(callback.message.message_id, user_id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return

    if _has_active_game(user_id):
        await callback.answer(
            "⚠️ У вас уже есть активная игра! Завершите её прежде чем начать новую.",
            show_alert=True
        )
        return

    mines_count = int(callback.data.split("_")[-1])
    await state.update_data(mines_count=mines_count)
    await state.set_state(MinesGame.choosing_bet)

    await callback.message.edit_text(
        f"<blockquote><b><tg-emoji emoji-id=\"5197269100878907942\">🎰</tg-emoji>"
        f"Введите сумму ставки:</b></blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="mines_back_select",
                icon_custom_emoji_id=EMOJI_BACK
            )
        ]])
    )
    set_owner_fn(callback.message.message_id, user_id)
    await callback.answer()


@mines_router.callback_query(F.data == "mines_back_select")
async def mines_back_select(callback: CallbackQuery, state: FSMContext):
    if not is_owner_fn(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    from payments import storage as pay_storage
    await state.clear()
    await show_mines_menu(callback, pay_storage, None)


@mines_router.callback_query(F.data == "mines_manual")
async def mines_manual_handler(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id

    if not is_owner_fn(callback.message.message_id, user_id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return

    if _has_active_game(user_id):
        await callback.answer(
            "⚠️ У вас уже есть активная игра! Завершите её прежде чем начать новую.",
            show_alert=True
        )
        return

    await state.update_data(mines_count=None, waiting_manual=True)
    await state.set_state(MinesGame.choosing_bet)
    await callback.message.edit_text(
        f"<blockquote><b><tg-emoji emoji-id=\"5197269100878907942\">🎰</tg-emoji>"
        f"Введите количество мин (от 2 до 24):</b></blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="mines_back_select",
                icon_custom_emoji_id=EMOJI_BACK
            )
        ]])
    )
    set_owner_fn(callback.message.message_id, user_id)
    await callback.answer()


@mines_router.callback_query(F.data == "mines_play_again")
async def mines_play_again(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    caller_id  = callback.from_user.id
    msg_id     = callback.message.message_id
    # Проверяем: нажавший должен быть владельцем ЭТОЙ игровой доски
    board_owner = _game_board_owner.get(msg_id)
    if board_owner is None or board_owner != caller_id:
        await callback.answer("🚫 Это не ваша игра!", show_alert=True)
        return
    _sessions.pop(caller_id, None)
    _cancel_timeout(caller_id)
    await state.clear()
    await show_mines_menu(callback, pay_storage, None)


@mines_router.callback_query(F.data == "mines_exit")
async def mines_exit(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    caller_id = callback.from_user.id
    msg_id    = callback.message.message_id

    # Главная защита: только владелец этой доски может нажимать Выйти
    board_owner = _game_board_owner.get(msg_id)
    if board_owner is None or board_owner != caller_id:
        await callback.answer("🚫 Это не ваша игра!", show_alert=True)
        return

    # Ищем активную сессию (если игра ещё идёт — возвращаем ставку)
    session  = _sessions.get(caller_id)
    owner_id = caller_id

    if session:
        if not session.get('finishing'):
            bet = session.get('bet', 0)
            if bet > 0:
                pay_storage.add_balance(owner_id, bet)
        _sessions.pop(owner_id, None)
        _cancel_timeout(owner_id)
    # Если сессии нет — post-game экран, board_owner уже проверен, просто выходим

    await state.clear()
    from main import get_games_menu, get_games_menu_text
    await callback.message.edit_text(
        get_games_menu_text(caller_id),
        parse_mode="HTML",
        reply_markup=get_games_menu()
    )
    await callback.answer()


@mines_router.callback_query(F.data == "mines_noop")
async def mines_noop(callback: CallbackQuery):
    await callback.answer()


@mines_router.callback_query(F.data.startswith("mines_cell_"))
async def mines_cell_handler(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    caller_id = callback.from_user.id
    msg_id    = callback.message.message_id
    idx       = int(callback.data.split("_")[-1])

    # Главная защита: проверяем кто владелец ЭТОЙ доски по message_id
    board_owner = _game_board_owner.get(msg_id)
    if board_owner is None or board_owner != caller_id:
        await callback.answer("🚫 Это не ваша игра!", show_alert=True)
        return

    # Ищем сессию владельца доски
    session = _sessions.get(caller_id)
    user_id = caller_id

    if not session:
        await callback.answer("🚫 Игра не найдена!", show_alert=True)
        return

    # Доп. проверка: сессия привязана именно к этой доске
    if session.get('message_id') != msg_id:
        await callback.answer("🚫 Это не ваша игра!", show_alert=True)
        return

    # Защита: игра уже завершается
    if session.get('finishing'):
        await callback.answer()
        return

    # Защита от двойного клика по одной ячейке
    if session['revealed'][idx]:
        await callback.answer("Уже открыта!")
        return

    # Защита от одновременного нажатия на одну ячейку несколькими запросами
    processing = session.setdefault('processing_cells', set())
    if idx in processing:
        await callback.answer()
        return

    # Берём персональный локер — предотвращает race condition
    lock = _get_user_lock(user_id)
    async with lock:
        # Повторные проверки внутри локера (состояние могло измениться пока ждали)
        session = _sessions.get(user_id)
        if not session:
            await callback.answer("🚫 Игра уже завершена!", show_alert=True)
            return

        if session.get('finishing'):
            await callback.answer()
            return

        if session['revealed'][idx]:
            await callback.answer("Уже открыта!")
            return

        processing = session.setdefault('processing_cells', set())
        if idx in processing:
            await callback.answer()
            return

        # Помечаем ячейку как "в обработке"
        processing.add(idx)

    try:
        # Есть активность — перезапускаем таймер
        _start_timeout(user_id, callback.bot, pay_storage)

        session['revealed'][idx] = True

        if session['board'][idx]:
            # МИНА
            bet            = session['bet']
            real_positions = session.get('real_positions', set())

            if idx not in real_positions:
                if real_positions:
                    remove_one     = random.choice(list(real_positions))
                    real_positions = (real_positions - {remove_one}) | {idx}
                    session['real_positions'] = real_positions

            # Атомарно помечаем как завершённую и удаляем сессию
            lock = _get_user_lock(user_id)
            async with lock:
                if session.get('finishing'):
                    # Уже обрабатывается другим потоком
                    return
                session['finishing'] = True
                _sessions.pop(user_id, None)

            _cancel_timeout(user_id)
            await state.clear()

            # Записываем в лидерборд: ставка в оборот, выигрыш = 0
            name = callback.from_user.first_name or callback.from_user.username or f"User {user_id}"
            record_game_result(user_id, name, bet, 0.0)
            # Сохраняем проигрыш в БД
            asyncio.create_task(db_save_game_result(user_id, 'mines', 0.0))

            balance = pay_storage.get_balance(user_id)
            await callback.message.edit_text(
                f"<blockquote><b><tg-emoji emoji-id=\"5210952531676504517\">🎰</tg-emoji>"
                f"Вы попали на мину!</b></blockquote>\n\n"
                f"<blockquote>"
                f"<tg-emoji emoji-id=\"5447183459602669338\">🎰</tg-emoji>Потеряно: "
                f"<code>{bet}</code><tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji>\n"
                f"<tg-emoji emoji-id=\"5278467510604160626\">🎰</tg-emoji>Баланс: "
                f"<code>{balance:.2f}</code><tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji>"
                f"</blockquote>\n\n"
                f"<blockquote><b><i>Вы проиграли ставку! Это не повод сдаваться!</i></b></blockquote>",
                parse_mode=ParseMode.HTML,
                reply_markup=build_game_keyboard(session, game_over=True)
            )
            set_owner_fn(callback.message.message_id, user_id)
            await callback.answer("💥Мина!")

        else:
            # ГЕМ
            session['gems_opened'] += 1
            gems        = session['gems_opened']
            mines_count = session['mines_count']
            hidden      = HIDDEN_MINES.get(mines_count, 0)
            total_safe  = GRID_SIZE * GRID_SIZE - mines_count - hidden
            mult        = get_multiplier(mines_count, gems)

            if gems == total_safe:
                # ПОБЕДА — открыли все безопасные клетки
                bet      = session['bet']

                # Атомарно помечаем как завершённую
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

                # Записываем в лидерборд
                name = callback.from_user.first_name or callback.from_user.username or f"User {user_id}"
                record_game_result(user_id, name, bet, winnings)
                # Сохраняем выигрыш в БД
                asyncio.create_task(db_save_game_result(user_id, 'mines', winnings))

                balance = pay_storage.get_balance(user_id)
                await callback.message.edit_text(
                    f"<blockquote><b><tg-emoji emoji-id=\"5210952531676504517\">🎰</tg-emoji>"
                    f"Вы выиграли!</b></blockquote>\n\n"
                    f"<blockquote>"
                    f"<tg-emoji emoji-id=\"5429651785352501917\">🎰</tg-emoji>Множитель: <b>x{mult}</b>\n"
                    f"<tg-emoji emoji-id=\"5305699699204837855\">🎰</tg-emoji>Выигрыш: "
                    f"<code>{winnings}</code><tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji>\n"
                    f"<tg-emoji emoji-id=\"5278467510604160626\">🎰</tg-emoji>: "
                    f"<code>{balance:.2f}</code><tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji>"
                    f"</blockquote>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=build_game_keyboard(session, game_over=True)
                )
                set_owner_fn(callback.message.message_id, user_id)
                await callback.answer("🏆 Победа!")
            else:
                await callback.message.edit_text(
                    game_text(session),
                    parse_mode=ParseMode.HTML,
                    reply_markup=build_game_keyboard(session)
                )
                await callback.answer(f"💎x{mult}")

    finally:
        # Снимаем флаг "в обработке" с ячейки
        session = _sessions.get(user_id)
        if session:
            session.get('processing_cells', set()).discard(idx)


@mines_router.callback_query(F.data == "mines_cashout")
async def mines_cashout(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    user_id = callback.from_user.id
    msg_id  = callback.message.message_id

    # Главная защита: только владелец этой доски может кешаутить
    board_owner = _game_board_owner.get(msg_id)
    if board_owner is None or board_owner != user_id:
        await callback.answer("🚫 Это не ваша игра!", show_alert=True)
        return

    # Берём локер — предотвращает двойной кэшаут
    lock = _get_user_lock(user_id)
    async with lock:
        session = _sessions.get(user_id)

        if not session:
            await callback.answer("Игра не найдена.", show_alert=True)
            return

        # Доп. проверка: сессия привязана именно к этой доске
        if session.get('message_id') != msg_id:
            await callback.answer("🚫 Это не ваша игра!", show_alert=True)
            return

        # Защита от двойного кэшаута
        if session.get('finishing'):
            await callback.answer()
            return

        gems = session.get('gems_opened', 0)
        if gems == 0:
            await callback.answer("Сначала откройте хотя бы одну клетку!", show_alert=True)
            return

        # Атомарно помечаем и удаляем сессию
        session['finishing'] = True
        _sessions.pop(user_id, None)

    mines_count = session['mines_count']
    bet         = session['bet']
    mult        = get_multiplier(mines_count, gems)
    winnings    = round(bet * mult, 2)

    pay_storage.add_balance(user_id, winnings)
    _cancel_timeout(user_id)
    await state.clear()

    # Записываем в лидерборд
    name = callback.from_user.first_name or callback.from_user.username or f"User {user_id}"
    record_game_result(user_id, name, bet, winnings)
    # Сохраняем кэшаут в БД
    asyncio.create_task(db_save_game_result(user_id, 'mines', winnings))

    balance = pay_storage.get_balance(user_id)
    await callback.message.edit_text(
        f"<blockquote><b><tg-emoji emoji-id=\"5312441427764989435\">🎰</tg-emoji>Кэшаут!</b></blockquote>\n\n"
        f"<blockquote>"
        f"<tg-emoji emoji-id=\"5429651785352501917\">🎰</tg-emoji>Множитель: <b>x{mult}</b>\n"
        f"<tg-emoji emoji-id=\"5305699699204837855\">🎰</tg-emoji>Выигрыш: "
        f"<code>{winnings}</code><tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji>\n"
        f"<tg-emoji emoji-id=\"5278467510604160626\">🎰</tg-emoji>: "
        f"<code>{balance:.2f}</code><tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji>"
        f"</blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Играть снова",
                callback_data="mines_cashout_again",
                icon_custom_emoji_id=EMOJI_3POINT
            )],
            [InlineKeyboardButton(
                text="Выйти",
                callback_data="mines_cashout_exit",
                icon_custom_emoji_id=EMOJI_BACK
            )],
        ])
    )
    set_owner_fn(callback.message.message_id, user_id)
    await callback.answer(f"💰+{winnings}!")



@mines_router.callback_query(F.data == "mines_cashout_again")
async def mines_cashout_again(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    caller_id = callback.from_user.id
    msg_id    = callback.message.message_id
    board_owner = _game_board_owner.get(msg_id)
    if board_owner is None or board_owner != caller_id:
        await callback.answer("🚫 Это не ваша игра!", show_alert=True)
        return
    await state.clear()
    await show_mines_menu(callback, pay_storage, None)


@mines_router.callback_query(F.data == "mines_cashout_exit")
async def mines_cashout_exit(callback: CallbackQuery, state: FSMContext):
    caller_id = callback.from_user.id
    msg_id    = callback.message.message_id
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


# ========== ОБРАБОТКА СТАВКИ (вызов из main.py через FSM) ==========

async def process_mines_bet(message: Message, state: FSMContext, storage):
    user_id = message.from_user.id
    data    = await state.get_data()
    mines_count    = data.get('mines_count')
    waiting_manual = data.get('waiting_manual', False)

    # Шаг 1: ждём ввод кол-ва мин вручную
    if waiting_manual and mines_count is None:
        try:
            m = int(message.text.strip())
        except ValueError:
            await message.answer("❌ Введите целое число от 2 до 24.")
            return
        if m < 2 or m > 24:
            await message.answer("❌ Число мин должно быть от 2 до 24.")
            return
        await state.update_data(mines_count=m, waiting_manual=False)
        await message.answer(
            f"<blockquote><b><tg-emoji emoji-id=\"5197269100878907942\">🎰</tg-emoji>"
            f"Введите сумму ставки:</b></blockquote>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Назад",
                    callback_data="mines_back_select",
                    icon_custom_emoji_id=EMOJI_BACK
                )
            ]])
        )
        return

    if mines_count is None:
        await state.clear()
        return

    # Блокируем если уже есть активная игра
    if _has_active_game(user_id):
        session = _sessions[user_id]
        await message.answer(
            _active_game_error_text(session),
            parse_mode=ParseMode.HTML
        )
        return

    # Защита от двойной отправки ставки
    bet_lock = _get_bet_lock(user_id)
    if bet_lock.locked():
        logging.warning(f"[mines] Двойная ставка заблокирована: user_id={user_id}")
        return

    async with bet_lock:
        # Повторная проверка внутри локера
        if _has_active_game(user_id):
            session = _sessions[user_id]
            await message.answer(
                _active_game_error_text(session),
                parse_mode=ParseMode.HTML
            )
            return

        try:
            bet = float(message.text.replace(',', '.'))
        except ValueError:
            await message.answer("Введите корректную сумму ставки.")
            return

        if bet < 0.1:
            await message.answer("❌ Минимальная ставка: 0.1")
            return

        if bet > 10000:
            await message.answer("❌ Максимальная ставка: 10000")
            return

        balance = storage.get_balance(user_id)
        if bet > balance:
            await message.answer(
                f"<blockquote><b>❌ Недостаточно средств!</b></blockquote>\n\n",
                parse_mode=ParseMode.HTML
            )
            return

        storage.deduct_balance(user_id, bet)

        # ✅ Начисляем реферальную комиссию (2% от ставки)
        asyncio.create_task(notify_referrer_commission(user_id, bet))

        session = _create_session(mines_count, bet, message.chat.id, user_id)
        _sessions[user_id] = session
        _last_owner[user_id] = user_id
        await state.set_state(MinesGame.playing)

    sent = await message.answer(
        game_text(session),
        parse_mode=ParseMode.HTML,
        reply_markup=build_game_keyboard(session)
    )
    session['message_id'] = sent.message_id
    set_owner_fn(sent.message_id, user_id)          # фиксируем владельца игрового поля
    _game_board_owner[sent.message_id] = user_id    # надёжная привязка: доска → игрок

    # Запускаем таймер бездействия
    _start_timeout(user_id, message.bot, storage)


# ========== ОБРАБОТКА КОМАНДЫ /mines ==========

async def process_mines_command(message: Message, state: FSMContext, storage):
    """
    Обрабатывает команды:
      /mines 0.3 5  |  mines 0.3 5  |  /мины 0.3 5  |  мины 0.3 5
    """
    text  = message.text.strip()
    match = re.match(
        r'^(?:/)?(?:mines|мины)\s+([\d.,]+)\s+(\d+)$',
        text,
        re.IGNORECASE
    )

    if not match:
        await message.answer(
            "<blockquote><b>❌ Неверный формат!</b></blockquote>\n\n"
            "<blockquote>Используйте:\n"
            "<code>/mines [ставка] [мины]</code>\n\n"
            "Примеры:\n"
            "<code>/mines 0.3 5</code>\n"
            "<code>mines 1.5 10</code>\n"
            "<code>/мины 0.5 13</code></blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    try:
        bet         = float(match.group(1).replace(',', '.'))
        mines_count = int(match.group(2))
    except ValueError:
        await message.answer("❌ Неверный формат чисел.")
        return

    if mines_count < 2 or mines_count > 24:
        await message.answer(
            "<blockquote><b>❌ Количество мин должно быть от 2 до 24.</b></blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    if bet < 0.1:
        await message.answer(
            "<blockquote><b>❌ Минимальная ставка: 0.1</b></blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    if bet > 10000:
        await message.answer(
            "<blockquote><b>❌ Максимальная ставка: 10 000</b></blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    user_id = message.from_user.id

    # Блокируем если уже есть активная игра
    if _has_active_game(user_id):
        session = _sessions[user_id]
        await message.answer(
            _active_game_error_text(session),
            parse_mode=ParseMode.HTML
        )
        return

    # Защита от двойной команды
    bet_lock = _get_bet_lock(user_id)
    if bet_lock.locked():
        logging.warning(f"[mines] Двойная команда заблокирована: user_id={user_id}")
        return

    async with bet_lock:
        # Повторная проверка внутри локера
        if _has_active_game(user_id):
            session = _sessions[user_id]
            await message.answer(
                _active_game_error_text(session),
                parse_mode=ParseMode.HTML
            )
            return

        balance = storage.get_balance(user_id)
        if bet > balance:
            await message.answer(
                f"<blockquote><b><tg-emoji emoji-id=\"5447183459602669338\">❌</tg-emoji> Недостаточно средств!</b></blockquote>\n\n",
                parse_mode=ParseMode.HTML
            )
            return

        storage.deduct_balance(user_id, bet)

        # ✅ Начисляем реферальную комиссию (2% от ставки)
        asyncio.create_task(notify_referrer_commission(user_id, bet))

        session = _create_session(mines_count, bet, message.chat.id, user_id)
        _sessions[user_id] = session
        _last_owner[user_id] = user_id
        await state.set_state(MinesGame.playing)

    sent = await message.answer(
        game_text(session),
        parse_mode=ParseMode.HTML,
        reply_markup=build_game_keyboard(session)
    )
    session['message_id'] = sent.message_id
    set_owner_fn(sent.message_id, user_id)          # фиксируем владельца игрового поля
    _game_board_owner[sent.message_id] = user_id    # надёжная привязка: доска → игрок
    # Запускаем таймер бездействия
    _start_timeout(user_id, message.bot, storage)
