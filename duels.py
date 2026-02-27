"""
duels.py — Модуль дуэлей для Telegram-казино.

Поддерживаемые игры: Кубик, Дартс, Боулинг, Футбол, Баскетбол
Команды (с/без /, точное совпадение — лишний текст не срабатывает):
  Кубик:      кубх[N] СУММА  | cubx[N] СУММА  | cubex[N] СУММА
  Дартс:      дартх[N] СУММА | dartx[N] СУММА | dartsx[N] СУММА
  Боулинг:    боулх[N] СУММА | boulx[N] СУММА | bowlx[N] СУММА | bowlingx[N] СУММА
  Футбол:     футх[N] СУММА  | footx[N] СУММА | footballx[N] СУММА | football[N] СУММА
  Баскетбол:  баскх[N] СУММА | basketx[N] СУММА | basketballx[N] СУММА | basketball[N] СУММА
  N — кол-во бросков (1..5). Если N > 5 — выдаётся ошибка.

Логика:
  1. Игрок 1 вводит команду → ставка снимается, карточка дуэли с кнопкой «Принять».
     Кнопки «Отменить» НЕТ.
  2. Игрок 2 нажимает «Принять» → ставка снимается, игра начинается.
  3. Каждый игрок бросает нужное кол-во раз, делая reply на карточку дуэли.
     Сообщения с бросками НЕ удаляются. Результаты записываются, карточка обновляется.
  4. Когда оба бросили все броски — карточка обновляется финальным счётом,
     затем отдельным сообщением выводится результат.
  5. Таймаут активности: если во время игры один из игроков не бросает 5 минут:
     - Карточка дуэли обновляется текстом "Игра закрыта!"
     - В чат дуэли отправляется уведомление о возврате
     - Ставки возвращаются обоим без комиссии
     Каждый бросок сбрасывает таймер.
  6. Ожидание игрока 2: таймаута нет (дуэль ждёт вечно).
  7. Победитель получает 95% банка.
  8. При ничьей — каждый получает обратно 95% своей ставки (5% комиссия).
  9. Вместо имён используются @username (если есть).

Команды управления:
  /mygames | /myg | /моиигры  — список ваших активных дуэлей
  /del | /дел               — отменить все ваши дуэли без соперника (возврат ставки)
"""

import asyncio
import logging
import re
import time
from datetime import datetime
from aiogram import Router, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.enums import ParseMode

# ─── внешние зависимости ──────────────────────────────────────────────────────
set_owner_fn = None
is_owner_fn  = None
_storage     = None
_bot         = None


def setup_duels(bot, storage):
    global _bot, _storage
    _bot     = bot
    _storage = storage


# ─── роутер ───────────────────────────────────────────────────────────────────
duels_router = Router()

# ─── хранилище ────────────────────────────────────────────────────────────────
_duels: dict[str, dict] = {}
_msg_to_duel: dict[int, str] = {}
_duel_counter: int = 0

# ─── константы ────────────────────────────────────────────────────────────────
COMMISSION       = 0.05   # 5% с банка победителю не уходит / при ничьей с каждого
MAX_THROWS       = 5
MIN_BET          = 0.3
ACTIVITY_TIMEOUT = 300    # секунд без броска → отмена

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
_A = r'([\d.,]+)'
_N = r'(\d+)?'

DICE_PAT       = re.compile(r'^/?(?:кубх|cubx|cubex|dicex)'                      + _N + r'\s+' + _A + r'$', re.I)
DARTS_PAT      = re.compile(r'^/?(?:дартх|dartx|dartsx)'                    + _N + r'\s+' + _A + r'$', re.I)
BOWLING_PAT    = re.compile(r'^/?(?:боулх|boulx|bowlx|bowlingx)'            + _N + r'\s+' + _A + r'$', re.I)
FOOTBALL_PAT   = re.compile(r'^/?(?:футх|futx|footx|footballx|football)'    + _N + r'\s+' + _A + r'$', re.I)
BASKETBALL_PAT = re.compile(r'^/?(?:баскх|basketx|basketballx|basketball)'  + _N + r'\s+' + _A + r'$', re.I)

DUEL_PATTERNS: list[tuple] = [
    (DICE_PAT,       'dice'),
    (DARTS_PAT,      'darts'),
    (BOWLING_PAT,    'bowling'),
    (FOOTBALL_PAT,   'football'),
    (BASKETBALL_PAT, 'basketball'),
]

MYGAMES_PAT = re.compile(r'^/?(?:mygames|myg|моиигры)$', re.I)
DEL_PAT     = re.compile(r'^/?(?:del|дел)$', re.I)


# ─── вспомогательные ──────────────────────────────────────────────────────────
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


def _sanitize(text: str) -> str:
    """Экранируем HTML-спецсимволы в именах пользователей."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def _fmt_user(first_name: str, username: str | None) -> str:
    if username:
        return f"@{_sanitize(username)}"
    return _sanitize(first_name) if first_name else "Игрок"


def parse_duel_command(text: str):
    """
    Возвращает (game_type, throws, amount) или None.
    Если бросков > MAX_THROWS → ('error_throws', N, 0).
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
            if throws > MAX_THROWS:
                return 'error_throws', throws, 0
            throws = max(1, throws)
            try:
                amount = float(amount_raw.replace(',', '.'))
            except ValueError:
                return None
            return game_type, throws, amount
    return None


def is_duel_command(text: str) -> bool:
    return parse_duel_command(text) is not None


def is_mygames_command(text: str) -> bool:
    return bool(text and MYGAMES_PAT.match(text.strip()))


def is_del_command(text: str) -> bool:
    return bool(text and DEL_PAT.match(text.strip()))


# ─── текст карточки ───────────────────────────────────────────────────────────
def _duel_card_text(duel: dict, *, extra: str = "") -> str:
    gt    = duel['game_type']
    emoji = GAME_EMOJI[gt]
    name  = GAME_NAMES[gt]
    n     = duel['throws']
    amt   = duel['amount']
    bank  = amt * 2
    prize = bank * (1 - COMMISSION)
    p1t   = duel['player1_tag']
    p2t   = duel['player2_tag'] or "???"

    header = (
        f"⚔️ <b>Дуэль</b>\n\n"
        f"<blockquote>"
        f'{emoji}<b>{name}</b>\n'
        f'<tg-emoji emoji-id="5400362079783770689">🎯</tg-emoji> <b>{n} {_throws_word(n)}</b>\n'
        f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji> Ставка: <b><code>{amt:.2f}</code></b>\n'
        f'<tg-emoji emoji-id="5278467510604160626">🏆</tg-emoji> Приз: <b><code>{prize:.2f}</code><tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b>\n'
        f'<tg-emoji emoji-id="5906581476639513176">👤</tg-emoji> <b>{p1t}</b>  vs  <tg-emoji emoji-id="5906581476639513176">👤</tg-emoji> <b>{p2t}</b>'
        f"</blockquote>"
    )

    scores_block = ""
    if duel['status'] == 'playing':
        def fmt(scores: list) -> str:
            cnt = len(scores)
            if cnt == 0:
                return f"—  (0/{n})"
            parts = " + ".join(str(s) for s in scores)
            if cnt > 1:
                return f"{parts} = <b>{sum(scores)}</b>  ({cnt}/{n})"
            return f"{parts}  ({cnt}/{n})"

        scores_block = (
            f"\n\n<blockquote>"
            f"📊 <b>Счёт:</b>\n"
            f"👤 {p1t}: {fmt(duel['player1_scores'])}\n"
            f"👤 {p2t}: {fmt(duel['player2_scores'])}"
            f"</blockquote>"
        )

    extra_block = f"\n\n{extra}" if extra else ""
    return header + scores_block + extra_block


def _join_kb(duel_id: str) -> InlineKeyboardMarkup:
    """Только кнопка «Принять» — без отмены."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="⚔️ Принять дуэль",
            callback_data=f"duel_join:{duel_id}",
            icon_custom_emoji_id="5906986955911993888"
        )],
    ])


def _playing_hint(duel: dict) -> str:
    emoji = GAME_EMOJI[duel['game_type']]
    n     = duel['throws']
    p1t   = duel['player1_tag']
    p2t   = duel['player2_tag']
    p1d   = len(duel['player1_scores'])
    p2d   = len(duel['player2_scores'])

    def st(done: int) -> str:
        if done >= n:
            return "✅ готово"
        left = n - done
        return f"⏳ осталось {left} {_throws_word(left)}"

    return (
        f"\n\n<blockquote>"
        f'<tg-emoji emoji-id="5906986955911993888">⚔️</tg-emoji> <b>Игра идёт!</b>  Делайте reply на это сообщение и бросайте {emoji}\n'
        f'<tg-emoji emoji-id="5906581476639513176">👤</tg-emoji> {p1t} — {st(p1d)}\n'
        f'<tg-emoji emoji-id="5906581476639513176">👤</tg-emoji> {p2t} — {st(p2d)}'
        f"</blockquote>"
    )


# ─── таймер активности ────────────────────────────────────────────────────────
def _cancel_activity_task(duel: dict):
    task = duel.get('activity_task')
    if task and not task.done():
        task.cancel()
    duel['activity_task'] = None


def _start_activity_task(duel_id: str):
    duel = _duels.get(duel_id)
    if not duel:
        return
    _cancel_activity_task(duel)
    # ✅ ИСПРАВЛЕНИЕ: asyncio.get_running_loop() вместо asyncio.get_event_loop()
    # get_event_loop() deprecated в Python 3.10+ внутри корутин и может вернуть
    # неверный loop, из-за чего таск создаётся некорректно и edit_message_text
    # не выполняется. get_running_loop() всегда возвращает текущий активный loop.
    loop = asyncio.get_running_loop()
    task = loop.create_task(_activity_timeout(duel_id))

    def _on_done(t):
        if t.cancelled():
            print(f"[Duels] Таск {duel_id} ОТМЕНЁН в {datetime.now().strftime('%H:%M:%S')}", flush=True)
        elif t.exception():
            print(f"[Duels] Таск {duel_id} УПАЛ: {t.exception()}", flush=True)
        else:
            print(f"[Duels] Таск {duel_id} завершён OK", flush=True)

    task.add_done_callback(_on_done)
    duel['activity_task'] = task
    print(f"[Duels] Таймер запущен для {duel_id} в {datetime.now().strftime('%H:%M:%S')}", flush=True)


async def _activity_timeout(duel_id: str):
    print(f"[Duels] sleep начат для {duel_id} в {datetime.now().strftime('%H:%M:%S')}", flush=True)
    await asyncio.sleep(ACTIVITY_TIMEOUT)
    print(f"[Duels] sleep закончен для {duel_id} в {datetime.now().strftime('%H:%M:%S')}", flush=True)
    duel = _duels.get(duel_id)
    if not duel or duel['status'] != 'playing':
        return

    duel['activity_task'] = None
    _msg_to_duel.pop(duel.get('message_id'), None)

    p1       = duel['player1']
    p2       = duel['player2']
    p1t      = duel['player1_tag']
    p2t      = duel['player2_tag']
    p1s      = duel['player1_scores']
    p2s      = duel['player2_scores']
    amount   = duel['amount']
    bank     = amount * 2
    prize    = round(bank * (1 - COMMISSION), 8)
    emoji    = GAME_EMOJI[duel['game_type']]
    empty_kb = InlineKeyboardMarkup(inline_keyboard=[])

    # Случай 1: кто-то вообще не бросал → возврат ставок + личка
    if not p1s or not p2s:
        duel['status'] = 'cancelled'
        _storage.add_balance(p1, amount)
        _storage.add_balance(p2, amount)

        try:
            await _bot.edit_message_text(
                "🕐 <b>Игра закрыта!</b>",
                chat_id=duel['chat_id'],
                message_id=duel['message_id'],
                parse_mode=ParseMode.HTML,
                reply_markup=empty_kb
            )
        except Exception as e:
            print(f"[Duels] ОШИБКА edit: {e}", flush=True)

        for pid, opponent_tag in ((p1, p2t), (p2, p1t)):
            try:
                await _bot.send_message(
                    chat_id=pid,
                    text=(
                        f"⚔️ Дуэль против {opponent_tag} закрыта!\n"
                        f"💰 Ставка <code>{amount:.2f}</code>💰 возвращена."
                    ),
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                print(f"[Duels] ОШИБКА личка {pid}: {e}", flush=True)

        logging.info(f"[Duels] {duel_id} таймаут — один не бросал, ставки возвращены.")
        return

    # Случай 2: оба бросали → победитель по очкам, результат в чат как обычный финал
    duel['status'] = 'finished'
    p1sum  = sum(p1s)
    p2sum  = sum(p2s)
    p1_det = " + ".join(str(s) for s in p1s)
    p2_det = " + ".join(str(s) for s in p2s)

    if p1sum > p2sum:
        winner_id, winner_tag, loser_tag = p1, p1t, p2t
        _storage.add_balance(winner_id, prize)
        result_msg = (
            f"🏆 <b>Победитель: {winner_tag}!</b>\n\n"
            f"<blockquote>"
            f"{emoji} {p1t}: {p1_det} = <b>{p1sum}</b>\n"
            f"{emoji} {p2t}: {p2_det} = <b>{p2sum}</b>\n\n"
            f"💰 Приз: <code>+{prize:.2f}</code>💰  (банк {bank:.2f}, комиссия 5%)\n"
            f"🎉 {winner_tag} побеждает {loser_tag}!"
            f"</blockquote>"
        )
        logging.info(f"[Duels] {duel_id} таймаут-победа {winner_tag}, приз {prize}")
    elif p2sum > p1sum:
        winner_id, winner_tag, loser_tag = p2, p2t, p1t
        _storage.add_balance(winner_id, prize)
        result_msg = (
            f"🏆 <b>Победитель: {winner_tag}!</b>\n\n"
            f"<blockquote>"
            f"{emoji} {p1t}: {p1_det} = <b>{p1sum}</b>\n"
            f"{emoji} {p2t}: {p2_det} = <b>{p2sum}</b>\n\n"
            f"💰 Приз: <code>+{prize:.2f}</code>💰  (банк {bank:.2f}, комиссия 5%)\n"
            f"🎉 {winner_tag} побеждает {loser_tag}!"
            f"</blockquote>"
        )
        logging.info(f"[Duels] {duel_id} таймаут-победа {winner_tag}, приз {prize}")
    else:
        refund = round(amount * (1 - COMMISSION), 8)
        _storage.add_balance(p1, refund)
        _storage.add_balance(p2, refund)
        result_msg = (
            f"🤝 <b>Ничья!</b>\n\n"
            f"<blockquote>"
            f"{emoji} {p1t}: {p1_det} = <b>{p1sum}</b>\n"
            f"{emoji} {p2t}: {p2_det} = <b>{p2sum}</b>\n\n"
            f"💰 Возврат с комиссией 5%: по <code>{refund:.2f}</code>💰 каждому\n"
            f"<i>(от ставки <code>{amount:.2f}</code>💰)</i>"
            f"</blockquote>"
        )
        logging.info(f"[Duels] {duel_id} таймаут-ничья, возврат {refund}")

    # Удаляем карточку дуэли
    try:
        await _bot.delete_message(
            chat_id=duel['chat_id'],
            message_id=duel['message_id']
        )
    except Exception as e:
        print(f"[Duels] ОШИБКА delete: {e}", flush=True)

    # Отправляем результат новым сообщением
    await _bot.send_message(
        chat_id=duel['chat_id'],
        text=result_msg,
        parse_mode=ParseMode.HTML
    )

    logging.info(f"[Duels] {duel_id} таймаут завершён.")

# ─── создание дуэли ───────────────────────────────────────────────────────────
async def handle_duel_command(message: Message) -> None:
    result = parse_duel_command(message.text)
    if result is None:
        return

    game_type, throws, amount = result

    if game_type == 'error_throws':
        await message.reply(
            f"❌ <b>Максимальное количество бросков: {MAX_THROWS}!</b>\n"
            f"<blockquote>Вы указали <code>{throws}</code>. Допустимо: от 1 до {MAX_THROWS}.</blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    user_id = message.from_user.id

    MAX_BET = 10000.0
    import math
    if not math.isfinite(amount) or amount <= 0:
        await message.reply(
            "❌ <b>Некорректная сумма ставки.</b>",
            parse_mode=ParseMode.HTML
        )
        return

    if amount < MIN_BET:
        await message.reply(
            f"❌ <b>Минимальная ставка: <code>{MIN_BET}</code>💰</b>",
            parse_mode=ParseMode.HTML
        )
        return

    if amount > MAX_BET:
        await message.reply(
            f"❌ <b>Максимальная ставка: <code>{MAX_BET:,.0f}</code>💰</b>",
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

    tag1    = _fmt_user(message.from_user.first_name, message.from_user.username)
    duel_id = _new_duel_id()
    duel = {
        'game_type':      game_type,
        'throws':         throws,
        'amount':         amount,
        'player1':        user_id,
        'player1_tag':    tag1,
        'player2':        None,
        'player2_tag':    None,
        'player1_scores': [],
        'player2_scores': [],
        'status':         'waiting',
        'chat_id':        message.chat.id,
        'message_id':     None,
        'activity_task':  None,
    }
    _duels[duel_id] = duel

    text = _duel_card_text(duel, extra="⏳ <i>Ожидаем второго игрока...</i>")
    sent = await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=_join_kb(duel_id))
    duel['message_id'] = sent.message_id
    _msg_to_duel[sent.message_id] = duel_id

    logging.info(f"[Duels] Создана {duel_id}: {game_type}x{throws} на {amount}$ игроком {user_id}")


# ─── CALLBACK: принять дуэль ──────────────────────────────────────────────────
@duels_router.callback_query(F.data.startswith("duel_join:"))
async def cb_duel_join(callback: CallbackQuery) -> None:
    duel_id = callback.data.split(":", 1)[1]
    duel    = _duels.get(duel_id)

    if not duel:
        await callback.answer("❌ Дуэль не найдена.", show_alert=True)
        return
    if duel['status'] not in ('waiting',):
        await callback.answer("❌ Дуэль уже началась или завершена.", show_alert=True)
        return
    if callback.from_user.id == duel['player1']:
        await callback.answer("❌ Нельзя принять собственную дуэль!", show_alert=True)
        return

    user_id = callback.from_user.id
    amount  = duel['amount']

    # Атомарно резервируем дуэль — защита от race condition при одновременных нажатиях
    duel['status'] = 'joining'
    balance = _storage.get_balance(user_id)

    if balance < amount:
        duel['status'] = 'waiting'  # откатываем если нет денег
        await callback.answer(
            f"❌ Недостаточно средств!\nНужно: {amount:.2f}💰  Баланс: {balance:.2f}💰",
            show_alert=True
        )
        return

    _storage.add_balance(user_id, -amount)

    duel['player2']     = user_id
    duel['player2_tag'] = _fmt_user(callback.from_user.first_name, callback.from_user.username)
    duel['status']      = 'playing'

    text = _duel_card_text(duel) + _playing_hint(duel)
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=None)
    await callback.answer("⚔️ Дуэль принята! Начинайте бросать.")

    _start_activity_task(duel_id)

    logging.info(f"[Duels] {duel_id}: игрок {user_id} принял дуэль.")


# ─── обработчик броска ────────────────────────────────────────────────────────
@duels_router.message(F.dice)
async def handle_dice_throw(message: Message) -> None:
    """
    Ловит все dice-сообщения.
    Обрабатывает только reply на карточку активной дуэли.
    Сообщения с бросками НЕ удаляются.
    Только записывает результат и обновляет карточку.
    """
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

    if user_id not in (p1, p2):
        return

    expected = GAME_EMOJI[duel['game_type']]
    if message.dice.emoji != expected:
        return

    n      = duel['throws']
    scores = duel['player1_scores'] if user_id == p1 else duel['player2_scores']

    if len(scores) >= n:
        return

    # Записываем результат броска
    scores.append(message.dice.value)

    # Повторная проверка после записи — защита от race condition
    # (два одновременных броска могли оба пройти проверку выше)
    if len(scores) > n:
        scores.pop()
        return

    # Сбрасываем таймер активности
    _start_activity_task(duel_id)

    p1_done = len(duel['player1_scores'])
    p2_done = len(duel['player2_scores'])

    # Оба завершили — финал
    if p1_done >= n and p2_done >= n:
        await _finish_duel(duel_id, message)
        return

    # Обновляем карточку
    try:
        await _bot.edit_message_text(
            _duel_card_text(duel) + _playing_hint(duel),
            chat_id=duel['chat_id'],
            message_id=duel['message_id'],
            parse_mode=ParseMode.HTML,
            reply_markup=None
        )
    except Exception:
        pass


# ─── завершение дуэли ─────────────────────────────────────────────────────────
async def _finish_duel(duel_id: str, trigger_msg: Message) -> None:
    duel = _duels.get(duel_id)
    if not duel or duel['status'] != 'playing':
        return

    # Атомарная смена статуса — защита от двойного вызова финала
    # (оба игрока делают последний бросок почти одновременно)
    duel['status'] = 'finishing'

    # Повторная проверка после смены — если кто-то успел раньше, выходим
    if duel.get('_finishing_lock'):
        duel['status'] = 'finished'
        return
    duel['_finishing_lock'] = True
    duel['status'] = 'finished'
    _cancel_activity_task(duel)
    _msg_to_duel.pop(duel.get('message_id'), None)

    p1     = duel['player1']
    p2     = duel['player2']
    p1t    = duel['player1_tag']
    p2t    = duel['player2_tag']
    p1sum  = sum(duel['player1_scores'])
    p2sum  = sum(duel['player2_scores'])
    amount = duel['amount']
    bank   = amount * 2
    prize  = round(bank * (1 - COMMISSION), 8)
    emoji  = GAME_EMOJI[duel['game_type']]

    p1_det = " + ".join(str(s) for s in duel['player1_scores'])
    p2_det = " + ".join(str(s) for s in duel['player2_scores'])

    if p1sum > p2sum:
        winner_id, winner_tag, loser_tag = p1, p1t, p2t
        _storage.add_balance(winner_id, prize)
        result_msg = (
            f"🏆 <b>Победитель: {winner_tag}!</b>\n\n"
            f"<blockquote>"
            f"{emoji} {p1t}: {p1_det} = <b>{p1sum}</b>\n"
            f"{emoji} {p2t}: {p2_det} = <b>{p2sum}</b>\n\n"
            f"💰 Приз: <code>+{prize:.2f}</code>💰  (банк {bank:.2f}, комиссия 5%)\n"
            f"🎉 {winner_tag} побеждает {loser_tag}!"
            f"</blockquote>"
        )
        logging.info(f"[Duels] {duel_id} завершена. Победитель {winner_id} ({winner_tag}), приз {prize}")

    elif p2sum > p1sum:
        winner_id, winner_tag, loser_tag = p2, p2t, p1t
        _storage.add_balance(winner_id, prize)
        result_msg = (
            f"🏆 <b>Победитель: {winner_tag}!</b>\n\n"
            f"<blockquote>"
            f"{emoji} {p1t}: {p1_det} = <b>{p1sum}</b>\n"
            f"{emoji} {p2t}: {p2_det} = <b>{p2sum}</b>\n\n"
            f"💰 Приз: <code>+{prize:.2f}</code>💰  (банк {bank:.2f}, комиссия 5%)\n"
            f"🎉 {winner_tag} побеждает {loser_tag}!"
            f"</blockquote>"
        )
        logging.info(f"[Duels] {duel_id} завершена. Победитель {winner_id} ({winner_tag}), приз {prize}")

    else:
        refund = round(amount * (1 - COMMISSION), 8)
        _storage.add_balance(p1, refund)
        _storage.add_balance(p2, refund)
        result_msg = (
            f"🤝 <b>Ничья!</b>\n\n"
            f"<blockquote>"
            f"{emoji} {p1t}: {p1_det} = <b>{p1sum}</b>\n"
            f"{emoji} {p2t}: {p2_det} = <b>{p2sum}</b>\n\n"
            f"💰 Возврат с комиссией 5%: по <code>{refund:.2f}</code>💰 каждому\n"
            f"<i>(от ставки <code>{amount:.2f}</code>💰)</i>"
            f"</blockquote>"
        )
        logging.info(f"[Duels] {duel_id} ничья. Каждый получил {refund} (с комиссией 5%)")

    # Удаляем карточку дуэли
    try:
        await _bot.delete_message(
            chat_id=duel['chat_id'],
            message_id=duel['message_id']
        )
    except Exception:
        pass

    # Отправляем результат новым сообщением
    await trigger_msg.answer(result_msg, parse_mode=ParseMode.HTML)


# ─── /mygames ─────────────────────────────────────────────────────────────────
async def handle_mygames(message: Message) -> None:
    user_id = message.from_user.id
    active  = [
        (did, d) for did, d in _duels.items()
        if d['status'] in ('waiting', 'playing')
        and user_id in (d['player1'], d['player2'])
    ]

    if not active:
        await message.reply(
            "📋 <b>У вас нет активных дуэлей.</b>",
            parse_mode=ParseMode.HTML
        )
        return

    lines = ["📋 <b>Ваши активные дуэли:</b>\n"]
    for did, d in active:
        e    = GAME_EMOJI[d['game_type']]
        name = GAME_NAMES[d['game_type']]
        n    = d['throws']
        amt  = d['amount']
        p2t  = d['player2_tag'] or "???"
        st   = "⏳ ждёт игрока" if d['status'] == 'waiting' else "⚔️ идёт"

        if d['status'] == 'playing':
            p1d = len(d['player1_scores'])
            p2d = len(d['player2_scores'])
            sc  = f"📊 {d['player1_tag']} {p1d}/{n}  —  {p2t} {p2d}/{n}"
        else:
            sc = f"🎯 vs {p2t}"

        lines.append(f"{e} <b>{name}</b> x{n}  💰<code>{amt:.2f}</code>  [{st}]\n{sc}\n")

    await message.reply("\n".join(lines), parse_mode=ParseMode.HTML)


# ─── /del ─────────────────────────────────────────────────────────────────────
async def handle_del(message: Message) -> None:
    user_id = message.from_user.id
    waiting = [
        (did, d) for did, d in _duels.items()
        if d['status'] == 'waiting' and d['player1'] == user_id
    ]

    if not waiting:
        await message.reply(
            "📋 <b>Нет дуэлей без соперника для удаления.</b>\n"
            "<blockquote><i>Удаляются только дуэли, которые ещё никто не принял.</i></blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    total = 0.0
    for did, d in waiting:
        d['status'] = 'cancelled'
        _msg_to_duel.pop(d.get('message_id'), None)
        _storage.add_balance(user_id, d['amount'])
        total += d['amount']
        try:
            await _bot.edit_message_text(
                "❌ <b>Дуэль отменена</b>\n\n"
                "<blockquote>Создатель отменил все свои дуэли (/del).</blockquote>",
                chat_id=d['chat_id'],
                message_id=d['message_id'],
                parse_mode=ParseMode.HTML,
                reply_markup=None
            )
        except Exception:
            pass

    count = len(waiting)
    await message.reply(
        f"✅ <b>Отменено дуэлей: {count}</b>\n"
        f"<blockquote>💰 Возвращено: <code>{total:.2f}</code>💰</blockquote>",
        parse_mode=ParseMode.HTML
    )
    logging.info(f"[Duels] Игрок {user_id} удалил {count} дуэль(-ей), возврат {total}")
