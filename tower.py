import random
import re
import asyncio
import logging
from aiogram import Router, F, Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ParseMode

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

FLOORS = 6           # кол-во этажей
CELLS  = 5           # кол-во кликабельных ячеек на этаж
INACTIVITY_TIMEOUT = 300  # 5 минут

CELL_FUTURE      = "🌑"   # этаж ещё не достигнут (заблокирован)
CELL_ACTIVE      = "🌑"   # активная ячейка текущего этажа (кликабельна)
CELL_CHOSEN_SAFE = "💎"   # выбранная безопасная ячейка пройденного этажа
CELL_OTHER_SAFE  = "🌑"   # другие безопасные ячейки пройденного этажа
CELL_SAFE_REVEAL = "▪️"   # безопасная ячейка (раскрывается после проигрыша)
CELL_BOMB        = "💣"   # бомба (раскрывается после проигрыша)
CELL_EXPLODE     = "💥"   # ячейка на которую нажали и попали на бомбу

# difficulty_id -> кол-во бомб на каждом этаже
DIFFICULTY_BOMBS = {1: 1, 2: 2, 3: 3, 4: 4}
DIFFICULTY_NAMES = {1: "Лёгкий", 2: "Средний", 3: "Сложный", 4: "Безумный"}
DIFFICULTY_EMOJI = {1: "🟢", 2: "🟡", 3: "🔴", 4: "💀"}

# Множители по этажам [этаж1, этаж2, ..., этаж6] для каждой сложности
TOWER_MULTIPLIERS = {
    1: [1.19, 1.42, 1.69, 2.01, 2.39, 2.85],   # 1 бомба из 5 (~80% шанс)
    2: [1.45, 2.10, 3.04, 4.41, 6.39, 9.26],   # 2 бомбы из 5 (~60% шанс)
    3: [2.08, 4.33, 9.03, 18.80, 39.2, 81.7],  # 3 бомбы из 5 (~40% шанс)
    4: [4.15, 17.2, 71.5, 297.0, 1235.0, 5144.0], # 4 бомбы из 5 (~20% шанс)
}


# ========== FSM ==========
class TowerGame(StatesGroup):
    choosing_bet = State()
    playing      = State()


tower_router = Router()
_sessions: dict      = {}  # user_id -> session dict
_timeout_tasks: dict = {}  # user_id -> asyncio.Task


# ========== ТАЙМАУТ БЕЗДЕЙСТВИЯ ==========

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

    session = _sessions.pop(user_id, None)
    if session is None:
        return

    # Возвращаем ставку при таймауте
    bet = session.get('bet', 0)
    if bet > 0:
        storage.add_balance(user_id, bet)
        logging.info(f"[tower] Таймаут user={user_id}, ставка {bet} возвращена.")

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
                    "🏰 Башня\n"
                    f"<tg-emoji emoji-id=\"5305699699204837855\">🎰</tg-emoji>"
                    f"Ставка <code>{bet}</code>"
                    "<tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji> возвращена\n"
                    "</blockquote>\n\n"
                    "<blockquote><i>Игра завершена по таймауту (5 минут бездействия).</i></blockquote>"
                ),
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🏰 Играть снова", callback_data="tower_menu")
                ]])
            )
        except Exception:
            pass


# ========== ХЕЛПЕРЫ ==========

def _has_active_game(user_id: int) -> bool:
    return user_id in _sessions


def _active_game_error_text(session: dict) -> str:
    diff         = session['difficulty']
    bet          = session['bet']
    floors_passed = session['floors_passed']
    mult         = get_multiplier(diff, floors_passed)
    return (
        f"<blockquote><b>⚠️ У вас уже есть активная игра!</b></blockquote>\n\n"
        f"<blockquote>"
        f"🏰 Сложность: <b>{DIFFICULTY_EMOJI[diff]} {DIFFICULTY_NAMES[diff]}</b>\n"
        f"<tg-emoji emoji-id=\"5305699699204837855\">🎰</tg-emoji>Ставка: <code>{bet}</code>"
        f"<tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji>\n"
        f"🏗 Пройдено этажей: <b>{floors_passed}/{FLOORS}</b> | "
        f"<tg-emoji emoji-id=\"5330320040883411678\">🎰</tg-emoji><b>x{mult}</b>\n"
        f"</blockquote>\n\n"
        f"<blockquote><i>Завершите текущую игру прежде чем начать новую.</i></blockquote>"
    )


def get_multiplier(difficulty: int, floors_passed: int) -> float:
    if floors_passed == 0:
        return 1.0
    mults = TOWER_MULTIPLIERS.get(difficulty, [])
    if not mults:
        return 1.0
    return mults[min(floors_passed - 1, len(mults) - 1)]


def get_next_mult(difficulty: int, floors_passed: int) -> float:
    mults = TOWER_MULTIPLIERS.get(difficulty, [])
    if not mults or floors_passed >= len(mults):
        return mults[-1] if mults else 1.0
    return mults[floors_passed]


def _create_session(difficulty: int, bet: float, chat_id: int) -> dict:
    """Генерируем позиции бомб для всех 6 этажей заранее."""
    bombs  = DIFFICULTY_BOMBS[difficulty]
    floors = []
    for _ in range(FLOORS):
        bomb_cols = random.sample(range(CELLS), bombs)
        floors.append({
            'bomb_cols': bomb_cols,
            'chosen':    None,  # индекс выбранной ячейки игроком
        })
    return {
        'difficulty':    difficulty,
        'bet':           bet,
        'current_floor': 0,     # 0-based, активный этаж для нажатия
        'floors_passed': 0,     # сколько этажей пройдено
        'floors':        floors,
        'message_id':    None,
        'chat_id':       chat_id,
    }


# ========== ПОСТРОЕНИЕ КЛАВИАТУРЫ ==========

def build_tower_keyboard(session: dict, game_over: bool = False) -> InlineKeyboardMarkup:
    difficulty    = session['difficulty']
    current_floor = session['current_floor']
    floors_passed = session['floors_passed']
    floors        = session['floors']
    rows          = []

    # Отображаем этажи сверху вниз: этаж 6 (индекс 5) наверху, этаж 1 (индекс 0) внизу
    for floor_idx in range(FLOORS - 1, -1, -1):
        floor_data = floors[floor_idx]
        chosen     = floor_data['chosen']
        bomb_cols  = floor_data['bomb_cols']
        mult       = TOWER_MULTIPLIERS[difficulty][floor_idx]
        btn_row    = []

        # Первая кнопка — множитель этажа (не кликабельна)
        btn_row.append(InlineKeyboardButton(
            text=f"x{mult}",
            callback_data="tower_noop"
        ))

        if game_over:
            # ===== РЕЖИМ ПРОИГРЫША: полное раскрытие ВСЕХ этажей =====
            for col in range(CELLS):
                is_bomb = col in bomb_cols
                if col == chosen and is_bomb:
                    text = CELL_EXPLODE      # игрок нажал на бомбу
                elif is_bomb:
                    text = CELL_BOMB         # остальные бомбы
                elif col == chosen:
                    text = CELL_CHOSEN_SAFE  # выбранная безопасная (пройденные этажи)
                else:
                    text = CELL_SAFE_REVEAL  # пустая безопасная ячейка
                btn_row.append(InlineKeyboardButton(text=text, callback_data="tower_noop"))

        elif floor_idx < current_floor:
            # ===== ПРОЙДЕННЫЙ ЭТАЖ =====
            for col in range(CELLS):
                if col == chosen:
                    text = CELL_CHOSEN_SAFE  # 💎 выбранная ячейка
                else:
                    text = CELL_OTHER_SAFE   # ⬜ остальные (были безопасными)
                btn_row.append(InlineKeyboardButton(text=text, callback_data="tower_noop"))

        elif floor_idx == current_floor:
            # ===== АКТИВНЫЙ ЭТАЖ: кликабельные ячейки =====
            for col in range(CELLS):
                btn_row.append(InlineKeyboardButton(
                    text=CELL_ACTIVE,
                    callback_data=f"tower_cell_{floor_idx}_{col}"
                ))

        else:
            # ===== БУДУЩИЙ ЭТАЖ: заблокирован =====
            for col in range(CELLS):
                btn_row.append(InlineKeyboardButton(text=CELL_FUTURE, callback_data="tower_noop"))

        rows.append(btn_row)

    # Кнопки управления
    if not game_over:
        ctrl = []
        if floors_passed > 0:
            mult    = get_multiplier(difficulty, floors_passed)
            cashout = round(session['bet'] * mult, 2)
            ctrl.append(InlineKeyboardButton(
                text=f"Забрать {cashout}",
                callback_data="tower_cashout",
                icon_custom_emoji_id=EMOJI_GOAL
            ))
        ctrl.append(InlineKeyboardButton(
            text="Выйти",
            callback_data="tower_exit",
            icon_custom_emoji_id=EMOJI_BACK
        ))
        rows.append(ctrl)
    else:
        rows.append([
            InlineKeyboardButton(
                text="Снова",
                callback_data="tower_play_again",
                icon_custom_emoji_id=EMOJI_3POINT
            ),
            InlineKeyboardButton(
                text="Выйти",
                callback_data="tower_exit",
                icon_custom_emoji_id=EMOJI_BACK
            ),
        ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_tower_select_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1 💣", callback_data="tower_diff_1"),
            InlineKeyboardButton(text="2 💣", callback_data="tower_diff_2"),
        ],
        [
            InlineKeyboardButton(text="3 💣", callback_data="tower_diff_3"),
            InlineKeyboardButton(text="4 💣", callback_data="tower_diff_4"),
        ],
        [
            InlineKeyboardButton(
                text="Назад",
                callback_data="games",
                icon_custom_emoji_id=EMOJI_BACK
            )
        ]
    ])


def game_text(session: dict) -> str:
    diff         = session['difficulty']
    bet          = session['bet']
    floors_passed = session['floors_passed']
    mult         = get_multiplier(diff, floors_passed)
    next_mult    = get_next_mult(diff, floors_passed)
    floor_num    = session['current_floor'] + 1  # человекочитаемый номер

    return (
        f"<blockquote><b>🏰 Башня</b></blockquote>\n\n"
        f"<blockquote>"
        f"<tg-emoji emoji-id=\"5305699699204837855\">🎰</tg-emoji>Ставка: <code>{bet}</code><tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji>\n"
        f"{DIFFICULTY_EMOJI[diff]} Сложность: <b>{DIFFICULTY_NAMES[diff]}</b>\n"
        f"<tg-emoji emoji-id=\"5197503331215361533\">🎰</tg-emoji>Этаж: <b>{floor_num}/{FLOORS}</b>\n"
        f"<tg-emoji emoji-id=\"5330320040883411678\">🎰</tg-emoji>Текущий: <b><code>x{mult}</code></b>\n"
        f"<tg-emoji emoji-id=\"5391032818111363540\">🎰</tg-emoji>Следующий: <b><code>x{next_mult}</code></b>\n"
        f"</blockquote>\n\n"
        f"<blockquote><b><i>Выберите безопасную ячейку на текущем этаже!</i></b></blockquote>"
    )


# ========== ПУБЛИЧНАЯ ФУНКЦИЯ ВХОДА ==========

async def show_tower_menu(callback: CallbackQuery, storage, betting_game=None):
    user_id = callback.from_user.id

    if _has_active_game(user_id):
        await callback.answer(
            "⚠️ У вас уже есть активная игра! Завершите её прежде чем начать новую.",
            show_alert=True
        )
        return

    balance = storage.get_balance(user_id)
    text = (
        f"<blockquote><b>🏰 Башня</b></blockquote>\n\n"
        f"<blockquote><b><tg-emoji emoji-id=\"5278467510604160626\">🎰</tg-emoji>: "
        f"<code>{balance:.2f}</code><tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji></b></blockquote>\n\n"
        f"<blockquote><b>Выберите сложность:</b></blockquote>\n"
    )
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_tower_select_keyboard()
    )
    await callback.answer()


# ========== ХЕНДЛЕРЫ ==========

@tower_router.callback_query(F.data == "tower_menu")
async def tower_menu_callback(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    await state.clear()
    await show_tower_menu(callback, pay_storage)


@tower_router.callback_query(F.data.startswith("tower_diff_"))
async def tower_diff_handler(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id

    if _has_active_game(user_id):
        await callback.answer(
            "⚠️ У вас уже есть активная игра! Завершите её прежде чем начать новую.",
            show_alert=True
        )
        return

    difficulty = int(callback.data.split("_")[-1])
    await state.update_data(tower_difficulty=difficulty)
    await state.set_state(TowerGame.choosing_bet)

    await callback.message.edit_text(
        f"<blockquote><b><tg-emoji emoji-id=\"5197269100878907942\">🎰</tg-emoji>Введите сумму ставки:</b></blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="tower_back_select",
                icon_custom_emoji_id=EMOJI_BACK
            )
        ]])
    )
    await callback.answer()


@tower_router.callback_query(F.data == "tower_back_select")
async def tower_back_select(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    await state.clear()
    await show_tower_menu(callback, pay_storage)


@tower_router.callback_query(F.data == "tower_play_again")
async def tower_play_again(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    user_id = callback.from_user.id
    _sessions.pop(user_id, None)
    _cancel_timeout(user_id)
    await state.clear()
    await show_tower_menu(callback, pay_storage)


@tower_router.callback_query(F.data == "tower_exit")
async def tower_exit(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    user_id = callback.from_user.id
    _sessions.pop(user_id, None)
    _cancel_timeout(user_id)
    await state.clear()
    from main import get_games_menu, get_games_menu_text
    await callback.message.edit_text(
        get_games_menu_text(user_id),
        parse_mode="HTML",
        reply_markup=get_games_menu()
    )
    await callback.answer()


@tower_router.callback_query(F.data == "tower_noop")
async def tower_noop(callback: CallbackQuery):
    await callback.answer()


@tower_router.callback_query(F.data.startswith("tower_cell_"))
async def tower_cell_handler(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    user_id = callback.from_user.id

    # tower_cell_{floor_idx}_{col}
    parts     = callback.data.split("_")
    floor_idx = int(parts[2])
    col       = int(parts[3])

    session = _sessions.get(user_id)
    if not session:
        await callback.answer("Игра не найдена. Начните заново.", show_alert=True)
        return

    # Защита от нажатия не на активный этаж
    if floor_idx != session['current_floor']:
        await callback.answer()
        return

    # Перезапускаем таймер бездействия
    _start_timeout(user_id, callback.bot, pay_storage)

    floor_data = session['floors'][floor_idx]
    bomb_cols  = floor_data['bomb_cols']
    floor_data['chosen'] = col

    if col in bomb_cols:
        # ===== БОМБА =====
        bet = session['bet']
        _sessions.pop(user_id, None)
        _cancel_timeout(user_id)
        await state.clear()

        # Записываем в лидерборд: проигрыш
        name = callback.from_user.first_name or callback.from_user.username or f"User {user_id}"
        record_game_result(user_id, name, bet, 0.0)

        balance = pay_storage.get_balance(user_id)
        await callback.message.edit_text(
            f"<blockquote><b><tg-emoji emoji-id=\"5210952531676504517\">🎰</tg-emoji>"
            f"Вы попали на бомбу!</b></blockquote>\n\n"
            f"<blockquote>"
            f"<tg-emoji emoji-id=\"5447183459602669338\">🎰</tg-emoji>Потеряно: "
            f"<code>{bet}</code><tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji>\n"
            f"<tg-emoji emoji-id=\"5278467510604160626\">🎰</tg-emoji>Баланс: "
            f"<code>{balance:.2f}</code><tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji>"
            f"</blockquote>\n\n"
            f"<blockquote><b><i>Башня рухнула! Попробуйте снова!</i></b></blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=build_tower_keyboard(session, game_over=True)
        )
        await callback.answer("💥 Бомба!")

    else:
        # ===== БЕЗОПАСНО =====
        session['floors_passed'] += 1
        session['current_floor'] += 1
        floors_passed = session['floors_passed']
        difficulty    = session['difficulty']
        mult          = get_multiplier(difficulty, floors_passed)

        if session['current_floor'] >= FLOORS:
            # ===== ПОБЕДА — все этажи пройдены =====
            bet      = session['bet']
            winnings = round(bet * mult, 2)
            pay_storage.add_balance(user_id, winnings)
            _sessions.pop(user_id, None)
            _cancel_timeout(user_id)
            await state.clear()

            # Записываем в лидерборд: победа
            name = callback.from_user.first_name or callback.from_user.username or f"User {user_id}"
            record_game_result(user_id, name, bet, winnings)

            balance = pay_storage.get_balance(user_id)
            await callback.message.edit_text(
                f"<blockquote><b><tg-emoji emoji-id=\"5461151367559141950\">🎰</tg-emoji>"
                f"Вы прошли все этажи!</b></blockquote>\n\n"
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
                        callback_data="tower_menu",
                        icon_custom_emoji_id=EMOJI_3POINT
                    )],
                    [InlineKeyboardButton(
                        text="Выйти",
                        callback_data="games",
                        icon_custom_emoji_id=EMOJI_BACK
                    )],
                ])
            )
            await callback.answer("🏆 Победа!")

        else:
            # Следующий этаж
            await callback.message.edit_text(
                game_text(session),
                parse_mode=ParseMode.HTML,
                reply_markup=build_tower_keyboard(session)
            )
            await callback.answer(f"✅ x{mult}")


@tower_router.callback_query(F.data == "tower_cashout")
async def tower_cashout(callback: CallbackQuery, state: FSMContext):
    from payments import storage as pay_storage
    user_id = callback.from_user.id
    session = _sessions.get(user_id)

    if not session:
        await callback.answer("Игра не найдена.", show_alert=True)
        return

    floors_passed = session['floors_passed']
    if floors_passed == 0:
        await callback.answer("Сначала пройдите хотя бы один этаж!", show_alert=True)
        return

    difficulty = session['difficulty']
    bet        = session['bet']
    mult       = get_multiplier(difficulty, floors_passed)
    winnings   = round(bet * mult, 2)

    pay_storage.add_balance(user_id, winnings)
    _sessions.pop(user_id, None)
    _cancel_timeout(user_id)
    await state.clear()

    # Записываем в лидерборд: кэшаут
    name = callback.from_user.first_name or callback.from_user.username or f"User {user_id}"
    record_game_result(user_id, name, bet, winnings)

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
                callback_data="tower_menu",
                icon_custom_emoji_id=EMOJI_3POINT
            )],
            [InlineKeyboardButton(
                text="Выйти",
                callback_data="games",
                icon_custom_emoji_id=EMOJI_BACK
            )],
        ])
    )
    await callback.answer(f"💰 +{winnings}!")


# ========== ОБРАБОТКА СТАВКИ (вызов из main.py через FSM) ==========

async def process_tower_bet(message: Message, state: FSMContext, storage):
    user_id = message.from_user.id
    data    = await state.get_data()
    difficulty = data.get('tower_difficulty')

    if difficulty is None:
        await state.clear()
        return

    # Блокируем если уже есть активная игра
    if _has_active_game(user_id):
        session = _sessions[user_id]
        await message.answer(_active_game_error_text(session), parse_mode=ParseMode.HTML)
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
            f"<blockquote><b>❌ Недостаточно средств!</b></blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    storage.deduct_balance(user_id, bet)

    # ✅ Начисляем реферальную комиссию (2% от ставки)
    asyncio.create_task(notify_referrer_commission(user_id, bet))

    session = _create_session(difficulty, bet, message.chat.id)
    _sessions[user_id] = session
    await state.set_state(TowerGame.playing)

    sent = await message.answer(
        game_text(session),
        parse_mode=ParseMode.HTML,
        reply_markup=build_tower_keyboard(session)
    )
    session['message_id'] = sent.message_id
    _start_timeout(user_id, message.bot, storage)


# ========== ОБРАБОТКА КОМАНДЫ /tower ==========

async def process_tower_command(message: Message, state: FSMContext, storage):
    """
    Обрабатывает команды:
      /tower 0.5 1  |  tower 0.5 2  |  /башня 1.0 3  |  башня 0.5 1
    Сложность: 1 (лёгкий), 2 (средний), 3 (сложный)
    """
    text  = message.text.strip()
    match = re.match(
        r'^(?:/)?(?:tower|башня)\s+([\d.,]+)\s+(\d+)$',
        text,
        re.IGNORECASE
    )

    if not match:
        await message.answer(
            "<blockquote><b>❌ Неверный формат!</b></blockquote>\n\n",
            parse_mode=ParseMode.HTML
        )
        return

    try:
        bet        = float(match.group(1).replace(',', '.'))
        difficulty = int(match.group(2))
    except ValueError:
        await message.answer("❌ Неверный формат чисел.")
        return

    if difficulty not in (1, 2, 3, 4):
        await message.answer(
            "<blockquote><b>❌ Сложность должна быть 1, 2, 3 или 4.</b></blockquote>",
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

    if _has_active_game(user_id):
        session = _sessions[user_id]
        await message.answer(_active_game_error_text(session), parse_mode=ParseMode.HTML)
        return

    balance = storage.get_balance(user_id)
    if bet > balance:
        await message.answer(
            f"<blockquote><b>❌ Недостаточно средств!</b>\n"
            f"Баланс: <code>{balance:.2f}</code></blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    storage.deduct_balance(user_id, bet)

    # ✅ Начисляем реферальную комиссию (2% от ставки)
    asyncio.create_task(notify_referrer_commission(user_id, bet))

    session = _create_session(difficulty, bet, message.chat.id)
    _sessions[user_id] = session
    await state.set_state(TowerGame.playing)

    sent = await message.answer(
        game_text(session),
        parse_mode=ParseMode.HTML,
        reply_markup=build_tower_keyboard(session)
    )
    session['message_id'] = sent.message_id
    _start_timeout(user_id, message.bot, storage)
