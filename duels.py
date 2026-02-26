"""
duels.py — Модуль дуэлей для Telegram-казино.

Поддерживаемые игры: Кубик, Дартс, Боулинг, Футбол, Баскетбол
Команды (с/без /, точное совпадение — лишний текст не срабатывает):
  Кубик:      кубх[N] СУММА  | cubx[N] СУММА  | cubex[N] СУММА
  Дартс:      дартх[N] СУММА | dartx[N] СУММА | dartsx[N] СУММА
  Боулинг:    боулх[N] СУММА | bowlx[N] СУММА | bowlingx[N] СУММА
  Футбол:     футх[N] СУММА  | footx[N] СУММА | footballx[N] СУММА | football[N] СУММА
  Баскетбол:  баскх[N] СУММА | basketx[N] СУММА | basketballx[N] СУММА | basketball[N] СУММА
  N — кол-во бросков (1..5), по умолчанию 1.

Логика:
  1. Игрок 1 вводит команду → ставка снимается, создаётся объявление.
  2. Игрок 2 нажимает «Принять» → ставка снимается, начинается игра.
  3. Каждый игрок бросает нужное кол-во раз, отвечая (reply) на сообщение дуэли
     соответствующим эмодзи (🎲 / 🎯 / 🎳 / ⚽ / 🏀).
  4. Когда оба бросили все броски — подводится итог.
     Победитель получает 95% банка. При ничьей — возврат ставок.
  5. Если никто не принял в течение 5 минут — дуэль отменяется, ставка возвращается.
"""

import asyncio
import logging
import re
import time
from aiogram import Router, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.enums import ParseMode

# ─── внешние зависимости, инжектируются из main.py ────────────────────────────
set_owner_fn = None
is_owner_fn  = None
_storage     = None      # storage объект из payments.py
_bot         = None      # экземпляр Bot

def setup_duels(bot, storage):
    """Вызвать из main.py после инициализации бота."""
    global _bot, _storage
    _bot     = bot
    _storage = storage

# ─── роутер ───────────────────────────────────────────────────────────────────
duels_router = Router()

# ─── хранилище дуэлей ─────────────────────────────────────────────────────────
# duel_id → dict с данными дуэли
_duels: dict[str, dict] = {}
# message_id → duel_id (для быстрого поиска по reply)
_msg_to_duel: dict[int, str] = {}
_duel_counter: int = 0

# ─── константы ────────────────────────────────────────────────────────────────
COMMISSION      = 0.05        # 5% комиссия
MAX_THROWS      = 5
MIN_BET         = 0.02
DUEL_TIMEOUT    = 300         # 5 минут ожидания игрока 2

GAME_EMOJI: dict[str, str] = {
    'dice':       '🎲',
    'darts':      '🎯',
    'bowling':    '🎳',
    'football':   '⚽',
    'basketball': '🏀',
}
GAME_NAMES: dict[str, str] = {
    'dice':       'Кубик',
    'darts':      'Дартс',
    'bowling':    'Боулинг',
    'football':   'Футбол',
    'basketball': 'Баскетбол',
}

# ─── паттерны команд ──────────────────────────────────────────────────────────
# Строгие: начало строки, опциональный /, название, опциональное число бросков,
#          пробел, сумма, КОНЕЦ строки. Лишний текст → не срабатывает.
_CMD_AMOUNT = r'([\d.,]+)'
_CMD_THROWS = r'(\d+)?'

DICE_PAT       = re.compile(r'^/?(?:кубх|cubx|cubex)' + _CMD_THROWS + r'\s+' + _CMD_AMOUNT + r'$', re.I)
DARTS_PAT      = re.compile(r'^/?(?:дартх|dartx|dartsx)' + _CMD_THROWS + r'\s+' + _CMD_AMOUNT + r'$', re.I)
BOWLING_PAT    = re.compile(r'^/?(?:боулх|bowlx|bowlingx)' + _CMD_THROWS + r'\s+' + _CMD_AMOUNT + r'$', re.I)
FOOTBALL_PAT   = re.compile(r'^/?(?:футх|futx|footx|footballx|football)' + _CMD_THROWS + r'\s+' + _CMD_AMOUNT + r'$', re.I)
BASKETBALL_PAT = re.compile(r'^/?(?:баскх|basketx|basketballx|basketball)' + _CMD_THROWS + r'\s+' + _CMD_AMOUNT + r'$', re.I)

DUEL_PATTERNS: list[tuple] = [
    (DICE_PAT,       'dice'),
    (DARTS_PAT,      'darts'),
    (BOWLING_PAT,    'bowling'),
    (FOOTBALL_PAT,   'football'),
    (BASKETBALL_PAT, 'basketball'),
]

# ─── вспомогательные функции ──────────────────────────────────────────────────
def _new_duel_id() -> str:
    global _duel_counter
    _duel_counter += 1
    return f"dl{_duel_counter}_{int(time.time())}"


def _throws_word(n: int) -> str:
    if 11 <= n % 100 <= 19:
        return "бросков"
    if n % 10 == 1:
        return "бросок"
    if n % 10 in (2, 3, 4):
        return "броска"
    return "бросков"


def parse_duel_command(text: str) -> tuple | None:
    """
    Возвращает (game_type, throws, amount) или None.
    Только если текст точно соответствует шаблону.
    """
    if not text:
        return None
    t = text.strip()
    for pat, game_type in DUEL_PATTERNS:
        m = pat.match(t)
        if m:
            throws_raw = m.group(1)
            amount_raw = m.group(2)
            throws = int(throws_raw) if throws_raw else 1
            throws = max(1, min(throws, MAX_THROWS))
            try:
                amount = float(amount_raw.replace(',', '.'))
            except ValueError:
                return None
            return game_type, throws, amount
    return None


def is_duel_command(text: str) -> bool:
    return parse_duel_command(text) is not None


# ─── построение текста дуэли ──────────────────────────────────────────────────
def _duel_text(duel: dict, *, status_line: str = "") -> str:
    gt    = duel['game_type']
    emoji = GAME_EMOJI[gt]
    name  = GAME_NAMES[gt]
    n     = duel['throws']
    amt   = duel['amount']
    bank  = amt * 2
    prize = bank * (1 - COMMISSION)
    p1    = duel['player1_name']
    p2    = duel['player2_name'] or "???"

    header = (
        f"⚔️ <b>Дуэль</b>\n\n"
        f"<blockquote>"
        f"{emoji} Игра: <b>{name}</b>\n"
        f"🎯 Бросков: <b>{n} {_throws_word(n)}</b>\n"
        f"💰 Ставка: <b><code>{amt:.2f}</code> × 2</b>\n"
        f"🏆 Приз: <b><code>{prize:.2f}</code>💰</b> (95% банка)\n"
        f"👤 <b>{p1}</b> vs 👤 <b>{p2}</b>"
        f"</blockquote>"
    )

    scores_block = ""
    if duel['status'] == 'playing':
        def fmt_scores(scores: list, total: int) -> str:
            if not scores:
                return "—"
            parts = " + ".join(str(s) for s in scores)
            if len(scores) > 1:
                parts += f" = <b>{sum(scores)}</b>"
            return f"{parts} ({len(scores)}/{total})"

        p1s = fmt_scores(duel['player1_scores'], n)
        p2s = fmt_scores(duel['player2_scores'], n)
        scores_block = (
            f"\n\n<blockquote>"
            f"📊 Счёт:\n"
            f"👤 {p1}: {p1s}\n"
            f"👤 {p2}: {p2s}"
            f"</blockquote>"
        )

    extra = f"\n\n{status_line}" if status_line else ""
    return header + scores_block + extra


def _join_kb(duel_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚔️ Принять дуэль", callback_data=f"duel_join:{duel_id}")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data=f"duel_cancel:{duel_id}")],
    ])


def _instruction_text(duel: dict) -> str:
    gt    = duel['game_type']
    emoji = GAME_EMOJI[gt]
    n     = duel['throws']
    return (
        f"⚔️ <b>Дуэль началась!</b>\n\n"
        f"<blockquote>"
        f"Оба игрока: ответьте <b>reply</b> на это сообщение и отправьте\n"
        f"{emoji} — <b>{n} {_throws_word(n)}</b>\n\n"
        f"<i>Нажмите на поле ввода → иконка {emoji} → отправьте как ответ (reply).</i>"
        f"</blockquote>"
    )


# ─── создание дуэли ───────────────────────────────────────────────────────────
async def handle_duel_command(message: Message) -> None:
    """Вызывается из main.py при получении команды дуэли."""
    result = parse_duel_command(message.text)
    if result is None:
        return

    game_type, throws, amount = result
    user_id = message.from_user.id

    if amount < MIN_BET:
        await message.reply(
            f"❌ <b>Минимальная ставка: <code>{MIN_BET}</code>💰</b>",
            parse_mode=ParseMode.HTML
        )
        return

    balance = _storage.get_balance(user_id)
    if balance < amount:
        await message.reply(
            f"❌ <b>Недостаточно средств!</b>\n"
            f"<blockquote>"
            f"💰 Баланс: <code>{balance:.2f}</code>💰\n"
            f"🎯 Ставка: <code>{amount:.2f}</code>💰"
            f"</blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    _storage.add_balance(user_id, -amount)

    duel_id = _new_duel_id()
    duel = {
        'game_type':      game_type,
        'throws':         throws,
        'amount':         amount,
        'player1':        user_id,
        'player1_name':   message.from_user.first_name or "Игрок 1",
        'player2':        None,
        'player2_name':   None,
        'player1_scores': [],
        'player2_scores': [],
        'status':         'waiting',   # waiting | playing | finished | cancelled
        'chat_id':        message.chat.id,
        'message_id':     None,
        'created_at':     time.time(),
    }
    _duels[duel_id] = duel

    text = _duel_text(duel, status_line="⏳ <i>Ожидаем второго игрока...</i>")
    sent = await message.answer(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=_join_kb(duel_id)
    )
    duel['message_id'] = sent.message_id
    _msg_to_duel[sent.message_id] = duel_id

    asyncio.create_task(_timeout_task(duel_id, sent.chat.id, sent.message_id))
    logging.info(f"[Duels] Создана {duel_id}: {game_type}x{throws} на {amount}$ игроком {user_id}")


# ─── таймаут ──────────────────────────────────────────────────────────────────
async def _timeout_task(duel_id: str, chat_id: int, msg_id: int) -> None:
    await asyncio.sleep(DUEL_TIMEOUT)
    duel = _duels.get(duel_id)
    if not duel or duel['status'] != 'waiting':
        return

    duel['status'] = 'cancelled'
    _storage.add_balance(duel['player1'], duel['amount'])
    _msg_to_duel.pop(msg_id, None)

    try:
        await _bot.edit_message_text(
            "⏱ <b>Дуэль отменена</b>\n\n"
            "<blockquote>Время ожидания вышло. Ставка возвращена.</blockquote>",
            chat_id=chat_id,
            message_id=msg_id,
            parse_mode=ParseMode.HTML,
            reply_markup=None
        )
    except Exception:
        pass
    logging.info(f"[Duels] {duel_id} отменена по таймауту.")


# ─── CALLBACK: принять дуэль ──────────────────────────────────────────────────
@duels_router.callback_query(F.data.startswith("duel_join:"))
async def cb_duel_join(callback: CallbackQuery) -> None:
    duel_id = callback.data.split(":", 1)[1]
    duel = _duels.get(duel_id)

    if not duel:
        await callback.answer("❌ Дуэль не найдена.", show_alert=True)
        return
    if duel['status'] != 'waiting':
        await callback.answer("❌ Дуэль уже началась или завершена.", show_alert=True)
        return
    if callback.from_user.id == duel['player1']:
        await callback.answer("❌ Нельзя принять собственную дуэль!", show_alert=True)
        return

    user_id = callback.from_user.id
    balance = _storage.get_balance(user_id)
    amount  = duel['amount']

    if balance < amount:
        await callback.answer(
            f"❌ Недостаточно средств!\n"
            f"Нужно: {amount:.2f}💰  Баланс: {balance:.2f}💰",
            show_alert=True
        )
        return

    _storage.add_balance(user_id, -amount)

    duel['player2']      = user_id
    duel['player2_name'] = callback.from_user.first_name or "Игрок 2"
    duel['status']       = 'playing'

    text = _duel_text(duel) + "\n\n" + _instruction_text(duel)

    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=None
    )
    await callback.answer("⚔️ Дуэль принята! Начинайте бросать.")
    logging.info(
        f"[Duels] {duel_id}: игрок {user_id} принял дуэль. "
        f"{duel['player1']} vs {user_id}"
    )


# ─── CALLBACK: отменить дуэль ─────────────────────────────────────────────────
@duels_router.callback_query(F.data.startswith("duel_cancel:"))
async def cb_duel_cancel(callback: CallbackQuery) -> None:
    duel_id = callback.data.split(":", 1)[1]
    duel = _duels.get(duel_id)

    if not duel:
        await callback.answer("❌ Дуэль не найдена.", show_alert=True)
        return
    if duel['status'] != 'waiting':
        await callback.answer("❌ Дуэль уже нельзя отменить.", show_alert=True)
        return
    if callback.from_user.id != duel['player1']:
        await callback.answer("❌ Только создатель может отменить дуэль.", show_alert=True)
        return

    duel['status'] = 'cancelled'
    _storage.add_balance(duel['player1'], duel['amount'])
    _msg_to_duel.pop(duel.get('message_id'), None)

    await callback.message.edit_text(
        "❌ <b>Дуэль отменена</b>\n\n"
        "<blockquote>Создатель отменил дуэль. Ставка возвращена.</blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=None
    )
    await callback.answer("Дуэль отменена, ставка возвращена.")
    logging.info(f"[Duels] {duel_id} отменена создателем.")


# ─── обработчик броска кубиков ────────────────────────────────────────────────
@duels_router.message(F.dice)
async def handle_dice_throw(message: Message) -> None:
    """
    Перехватывает все сообщения с dice-эмодзи.
    Обрабатывает только те, что являются reply на сообщение активной дуэли.
    """
    # Нужен reply на сообщение дуэли
    if not message.reply_to_message:
        return

    replied_id = message.reply_to_message.message_id
    duel_id    = _msg_to_duel.get(replied_id)
    if duel_id is None:
        return

    duel = _duels.get(duel_id)
    if not duel or duel['status'] != 'playing':
        return

    user_id = message.from_user.id
    p1, p2  = duel['player1'], duel['player2']

    # Проверяем, что это один из участников
    if user_id not in (p1, p2):
        await message.reply("❌ Вы не участник этой дуэли.")
        return

    # Проверяем тип эмодзи
    expected_emoji = GAME_EMOJI[duel['game_type']]
    sent_emoji     = message.dice.emoji
    if sent_emoji != expected_emoji:
        await message.reply(
            f"❌ Неверный тип! В этой дуэли нужно бросать <b>{expected_emoji}</b>",
            parse_mode=ParseMode.HTML
        )
        return

    # Определяем очередность
    n = duel['throws']
    if user_id == p1:
        scores = duel['player1_scores']
        name   = duel['player1_name']
    else:
        scores = duel['player2_scores']
        name   = duel['player2_name']

    if len(scores) >= n:
        await message.reply(
            f"⚠️ Вы уже сделали все <b>{n} {_throws_word(n)}</b>. Ждём соперника.",
            parse_mode=ParseMode.HTML
        )
        return

    # Записываем бросок
    value = message.dice.value
    scores.append(value)
    remaining = n - len(scores)

    if remaining > 0:
        await message.reply(
            f"✅ <b>{name}</b>: бросок {sent_emoji} = <b>{value}</b>\n"
            f"<blockquote>Осталось бросков: <b>{remaining}</b></blockquote>",
            parse_mode=ParseMode.HTML
        )
    else:
        await message.reply(
            f"✅ <b>{name}</b>: последний бросок {sent_emoji} = <b>{value}</b>\n"
            f"<blockquote>Все броски сделаны! Ждём соперника.</blockquote>",
            parse_mode=ParseMode.HTML
        )

    # Обновляем текст дуэли (счёт)
    p1_done = len(duel['player1_scores'])
    p2_done = len(duel['player2_scores'])
    try:
        status_hint = ""
        if p1_done < n and p2_done < n:
            status_hint = "⚔️ <i>Игра идёт...</i>"
        elif p1_done >= n and p2_done < n:
            status_hint = f"⏳ <i>Ждём броски от {duel['player2_name']}...</i>"
        elif p1_done < n and p2_done >= n:
            status_hint = f"⏳ <i>Ждём броски от {duel['player1_name']}...</i>"

        await _bot.edit_message_text(
            _duel_text(duel, status_line=status_hint),
            chat_id=duel['chat_id'],
            message_id=duel['message_id'],
            parse_mode=ParseMode.HTML,
            reply_markup=None
        )
    except Exception:
        pass

    # Проверяем завершение
    if p1_done >= n and p2_done >= n:
        await _finish_duel(duel_id, message)


# ─── завершение дуэли ─────────────────────────────────────────────────────────
async def _finish_duel(duel_id: str, trigger_msg: Message) -> None:
    duel = _duels.get(duel_id)
    if not duel or duel['status'] != 'playing':
        return

    duel['status'] = 'finished'
    _msg_to_duel.pop(duel.get('message_id'), None)

    p1     = duel['player1']
    p2     = duel['player2']
    p1n    = duel['player1_name']
    p2n    = duel['player2_name']
    p1sum  = sum(duel['player1_scores'])
    p2sum  = sum(duel['player2_scores'])
    amount = duel['amount']
    bank   = amount * 2
    prize  = round(bank * (1 - COMMISSION), 8)
    emoji  = GAME_EMOJI[duel['game_type']]

    p1_detail = " + ".join(str(s) for s in duel['player1_scores'])
    p2_detail = " + ".join(str(s) for s in duel['player2_scores'])

    if p1sum > p2sum:
        winner_id, winner_name = p1, p1n
        loser_name             = p2n
        _storage.add_balance(winner_id, prize)
        result_text = (
            f"🏆 <b>Победитель: {winner_name}!</b>\n\n"
            f"<blockquote>"
            f"{emoji} {p1n}: {p1_detail} = <b>{p1sum}</b>\n"
            f"{emoji} {p2n}: {p2_detail} = <b>{p2sum}</b>\n\n"
            f"💰 Приз: <code>+{prize:.2f}</code>💰 (банк {bank:.2f}, комиссия 5%)\n"
            f"🎉 {winner_name} побеждает {loser_name}!"
            f"</blockquote>"
        )
        logging.info(
            f"[Duels] {duel_id} завершена. Победитель: {winner_id} ({winner_name}), "
            f"приз: {prize}"
        )

    elif p2sum > p1sum:
        winner_id, winner_name = p2, p2n
        loser_name             = p1n
        _storage.add_balance(winner_id, prize)
        result_text = (
            f"🏆 <b>Победитель: {winner_name}!</b>\n\n"
            f"<blockquote>"
            f"{emoji} {p1n}: {p1_detail} = <b>{p1sum}</b>\n"
            f"{emoji} {p2n}: {p2_detail} = <b>{p2sum}</b>\n\n"
            f"💰 Приз: <code>+{prize:.2f}</code>💰 (банк {bank:.2f}, комиссия 5%)\n"
            f"🎉 {winner_name} побеждает {loser_name}!"
            f"</blockquote>"
        )
        logging.info(
            f"[Duels] {duel_id} завершена. Победитель: {winner_id} ({winner_name}), "
            f"приз: {prize}"
        )

    else:
        # Ничья — возвращаем ставки
        _storage.add_balance(p1, amount)
        _storage.add_balance(p2, amount)
        result_text = (
            f"🤝 <b>Ничья!</b>\n\n"
            f"<blockquote>"
            f"{emoji} {p1n}: {p1_detail} = <b>{p1sum}</b>\n"
            f"{emoji} {p2n}: {p2_detail} = <b>{p2sum}</b>\n\n"
            f"💰 Ставки возвращены: <code>{amount:.2f}</code>💰 каждому."
            f"</blockquote>"
        )
        logging.info(f"[Duels] {duel_id} завершена ничьей.")

    # Финальное обновление исходного сообщения дуэли
    final_duel_text = _duel_text(duel)
    try:
        await _bot.edit_message_text(
            final_duel_text,
            chat_id=duel['chat_id'],
            message_id=duel['message_id'],
            parse_mode=ParseMode.HTML,
            reply_markup=None
        )
    except Exception:
        pass

    # Отправляем итог в чат
    await trigger_msg.answer(result_text, parse_mode=ParseMode.HTML)
