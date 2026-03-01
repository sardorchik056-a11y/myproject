"""
bonus.py — Бонусная система Telegram-казино.

Механика:
  - Бонус 0.1 монеты каждые 24 часа.
  - Требование: в НИКНЕЙМЕ (first_name) И в bio должна быть строка "@FesteryCas_bot".
    Username (@relessorg и т.п.) не проверяется — только отображаемое имя и «О себе».
  - Если после получения бонуса приписка убрана — следующий бонус
    доступен только через 72 часа (штраф).
  - Каждые 2 часа фоновый воркер проверяет всех получивших бонус:
    если приписка убрана — ставит штрафной таймер 72ч.

Команды: /bonus | /бонус | bonus | бонус
Кнопка:  callback_data="bonus_menu"

Исправления безопасности:
  - [FIX-1] Race condition при начислении: атомарная блокировка через asyncio.Lock
  - [FIX-2] Утечка памяти: _bonus_data неограниченно растёт — добавлена очистка
    устаревших записей (>30 дней без активности).
  - [FIX-3] HTML-инъекция: first_name/bio в сообщениях экранируются через html.escape.
  - [FIX-4] Watchdog не проверял users на penalty_at=None при penalty=True (NPE).
  - [FIX-5] Ошибка баланса не откатывала last_claimed атомарно — исправлено.
  - [FIX-6] getChat может вернуть пустую строку — нормализация через or None.
  - [FIX-7] Логирование raw данных из Telegram могло сломать лог-парсеры — исправлено.
  - [FIX-8] Watchdog глотал CancelledError — не давал корректно остановить бота.
  - [FIX-9] Добавлен лимит одновременных запросов getChat в watchdog (семафор).
"""

import asyncio
import html
import logging
import time
from collections import defaultdict

from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.enums import ParseMode

# ─── внешние зависимости ──────────────────────────────────────────────────────
try:
    from payments import storage as _storage
except ImportError:
    _storage = None
    logging.warning("[Bonus] payments.py не найден — баланс не будет начисляться.")

# ─── конфигурация ─────────────────────────────────────────────────────────────
BONUS_AMOUNT      = 0.1
BONUS_COOLDOWN    = 24 * 3600    # 24 часа
PENALTY_COOLDOWN  = 72 * 3600    # 72 часа (штраф за снятие приписки)
CHECK_INTERVAL    = 2  * 3600    # 2 часа (воркер)
STALE_THRESHOLD   = 30 * 86400   # 30 дней — очистка неактивных записей

# Что ищем: строка должна быть В НИКНЕЙМЕ (first_name) И В BIO
# Регистронезависимо, положение в строке не важно
REQUIRED_SUBSTRING = "@FesteryCas_bot"

# ─── ID кастомных эмодзи ──────────────────────────────────────────────────────
EMOJI_BACK   = "5906771962734057347"
EMOJI_COIN   = "5197434882321567830"
EMOJI_WIN    = "5278467510604160626"
EMOJI_LOSS   = "5447183459602669338"
EMOJI_CLOCK  = "5303214794336125778"
EMOJI_TROPHY = "5461151367559141950"
EMOJI_BONUS  = "5443127283898405358"
EMOJI_WARN   = "5210952531676504517"
EMOJI_SHIELD = "5906986955911993888"

# ─── роутер ───────────────────────────────────────────────────────────────────
bonus_router = Router()

# ─── хранилище состояний бонуса ───────────────────────────────────────────────
# _bonus_data[user_id] = {
#   "last_claimed":  float | None,
#   "penalty":       bool,
#   "penalty_at":    float | None,
#   "last_activity": float,
# }
_bonus_data: dict[int, dict] = {}

# [FIX-1] Блокировки на пользователя — предотвращают race condition
_user_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

# [FIX-9] Семафор для watchdog — не более 5 параллельных getChat
_watchdog_semaphore = asyncio.Semaphore(5)

# Глобальный бот (устанавливается через setup_bonus)
_bot: Bot | None = None


def setup_bonus(bot: Bot) -> None:
    """Вызывается из main.py при старте."""
    global _bot
    _bot = bot


# ══════════════════════════════════════════════════════════════════
#  Вспомогательные функции
# ══════════════════════════════════════════════════════════════════

def _now() -> float:
    return time.monotonic()


def _fmt_time(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    if h > 0:
        return f"{h}ч {m}м"
    return f"{m}м"


def _get_user_state(user_id: int) -> dict:
    if user_id not in _bonus_data:
        _bonus_data[user_id] = {
            "last_claimed":  None,
            "penalty":       False,
            "penalty_at":    None,
            "last_activity": _now(),
        }
    return _bonus_data[user_id]


def _has_tag(text: str | None) -> bool:
    """
    Проверяет наличие @FesteryCas_bot в строке.
    Регистронезависимо, место в строке не важно.
    """
    if not text:
        return False
    return REQUIRED_SUBSTRING.lower() in text.lower()


async def _fetch_user_info(user_id: int) -> tuple[str | None, str | None]:
    """
    Получает (first_name, bio) через getChat.
      first_name — никнейм (отображаемое имя профиля, не @username).
      bio        — раздел «О себе».
    Возвращает (None, None) при любой ошибке.
    """
    if _bot is None:
        return None, None
    try:
        chat       = await _bot.get_chat(user_id)
        first_name = getattr(chat, "first_name", None) or None  # [FIX-6]
        bio        = getattr(chat, "bio",        None) or None  # [FIX-6]
        return first_name, bio
    except Exception as e:
        logging.warning("[Bonus] getChat(%d) ошибка: %s", user_id, str(e)[:200])  # [FIX-7]
        return None, None


def _can_claim(user_id: int) -> tuple[bool, float, bool]:
    """
    Возвращает (можно_получить, секунд_осталось, на_штрафе).
    Побочный эффект: снимает истёкший штраф.
    """
    state = _get_user_state(user_id)
    now   = _now()

    # Штрафной режим
    if state["penalty"]:
        if state["penalty_at"] is None:  # [FIX-4]
            state["penalty"]    = False
            state["penalty_at"] = None
        else:
            elapsed = now - state["penalty_at"]
            if elapsed < PENALTY_COOLDOWN:
                return False, PENALTY_COOLDOWN - elapsed, True
            else:
                state["penalty"]    = False
                state["penalty_at"] = None

    # Обычный кулдаун
    if state["last_claimed"] is not None:
        elapsed = now - state["last_claimed"]
        if elapsed < BONUS_COOLDOWN:
            return False, BONUS_COOLDOWN - elapsed, False

    return True, 0.0, False


def _apply_penalty(user_id: int) -> None:
    """Применяет штрафной таймер 72ч, если ещё не применён."""
    state = _get_user_state(user_id)
    if not state["penalty"]:
        state["penalty"]    = True
        state["penalty_at"] = _now()
        logging.info("[Bonus] Штраф 72ч применён для user_id=%d", user_id)


def _cleanup_stale_records() -> None:
    """
    [FIX-2] Удаляет записи пользователей, неактивных более STALE_THRESHOLD секунд.
    Вызывается в начале каждого прохода watchdog.
    """
    now   = _now()
    stale = [
        uid for uid, state in _bonus_data.items()
        if now - state.get("last_activity", 0) > STALE_THRESHOLD
    ]
    for uid in stale:
        del _bonus_data[uid]
        _user_locks.pop(uid, None)
    if stale:
        logging.info("[Bonus] Очищено %d устаревших записей.", len(stale))


def is_bonus_command(text: str) -> bool:
    """Проверяет, является ли текст командой бонуса (без слеша)."""
    if not text:
        return False
    t = text.strip().lower()
    if t.startswith("/"):
        t = t[1:]
    return t in ("bonus", "бонус")


# ══════════════════════════════════════════════════════════════════
#  Основная логика выдачи бонуса
# ══════════════════════════════════════════════════════════════════

async def handle_bonus(message: Message, from_callback: bool = False) -> None:
    """
    Основная логика бонуса.
    Вызывается из хендлера команды и из callback bonus_menu в main.py.
    """
    user_id = message.from_user.id
    # [FIX-1] Блокируем параллельные запросы одного пользователя
    async with _user_locks[user_id]:
        await _handle_bonus_locked(message)


async def _handle_bonus_locked(message: Message) -> None:
    """Внутренняя логика, выполняется под блокировкой пользователя."""
    user_id = message.from_user.id

    kb_back = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Назад", callback_data="back_to_main")
    ]])

    # Обновляем время активности
    state = _get_user_state(user_id)
    state["last_activity"] = _now()

    # ── 1. Получаем никнейм (first_name) и bio ────────────────────
    first_name, bio = await _fetch_user_info(user_id)

    name_ok = _has_tag(first_name)  # никнейм содержит приписку?
    bio_ok  = _has_tag(bio)         # bio содержит приписку?

    if not name_ok or not bio_ok:
        missing = []
        if not name_ok:
            missing.append(
                f'<tg-emoji emoji-id="{EMOJI_LOSS}">❌</tg-emoji> '
                f'В <b>никнейме</b> (имя профиля) должна быть приписка '
                f'<code>@FesteryCas_bot</code>\n'
                f'  Пример: <code>🏙@FesteryCas_bot</code> или <code>Иван @FesteryCas_bot</code>'
            )
        if not bio_ok:
            missing.append(
                f'<tg-emoji emoji-id="{EMOJI_LOSS}">❌</tg-emoji> '
                f'В <b>«О себе»</b> (bio) должна быть приписка '
                f'<code>@FesteryCas_bot</code>'
            )
        await message.answer(
            f'<blockquote><b>'
            f'<tg-emoji emoji-id="{EMOJI_SHIELD}">🛡</tg-emoji>'
            f' Бонус недоступен</b></blockquote>\n\n'
            f'<blockquote>Для получения бонуса необходимо:\n\n'
            + "\n\n".join(missing) +
            f'\n\n<tg-emoji emoji-id="{EMOJI_BONUS}">💎</tg-emoji> '
            f'После выполнения условий нажмите /bonus снова</blockquote>',
            parse_mode=ParseMode.HTML,
            reply_markup=kb_back,
        )
        return

    # ── 2. Проверяем кулдаун ──────────────────────────────────────
    can, remaining, is_penalty = _can_claim(user_id)

    if not can:
        body = (
            f'<tg-emoji emoji-id="{EMOJI_WARN}">⚠️</tg-emoji> '
            f'<b>Вы убрали приписку — действует штраф!</b>\n\n'
            f'Бонус будет доступен через: <b>{_fmt_time(remaining)}</b>'
        ) if is_penalty else (
            f'<tg-emoji emoji-id="{EMOJI_CLOCK}">⏰</tg-emoji> '
            f'<b>Бонус уже получен!</b>\n\n'
            f'Следующий бонус через: <b>{_fmt_time(remaining)}</b>'
        )
        await message.answer(
            f'<blockquote>{body}</blockquote>',
            parse_mode=ParseMode.HTML,
            reply_markup=kb_back,
        )
        return

    # ── 3. Начисляем бонус ────────────────────────────────────────
    if _storage is None:
        await message.answer(
            f'<blockquote>'
            f'<tg-emoji emoji-id="{EMOJI_LOSS}">❌</tg-emoji>'
            f' Ошибка системы. Попробуйте позже.</blockquote>',
            parse_mode=ParseMode.HTML,
        )
        return

    # [FIX-5] Фиксируем время ДО начисления; откатываем при ошибке
    state["last_claimed"] = _now()
    state["penalty"]      = False
    state["penalty_at"]   = None

    try:
        _storage.add_balance(user_id, BONUS_AMOUNT)
    except Exception as e:
        state["last_claimed"] = None   # откат
        logging.error("[Bonus] Ошибка начисления user_id=%d: %s", user_id, e)
        await message.answer(
            f'<blockquote>'
            f'<tg-emoji emoji-id="{EMOJI_LOSS}">❌</tg-emoji>'
            f' Ошибка начисления. Попробуйте позже.</blockquote>',
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        balance = _storage.get_balance(user_id)
    except Exception as e:
        logging.error("[Bonus] Ошибка получения баланса user_id=%d: %s", user_id, e)
        balance = 0.0

    # [FIX-3] Экранируем first_name перед вставкой в HTML
    safe_name = html.escape(first_name or str(user_id))

    await message.answer(
        f'<blockquote><b>'
        f'<tg-emoji emoji-id="{EMOJI_TROPHY}">🏆</tg-emoji>'
        f' Бонус получен!</b></blockquote>\n\n'
        f'<blockquote>'
        f'<tg-emoji emoji-id="{EMOJI_BONUS}">💎</tg-emoji> Начислено: '
        f'<code>+{BONUS_AMOUNT}</code> '
        f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji>\n'
        f'<tg-emoji emoji-id="{EMOJI_WIN}">🏆</tg-emoji> Баланс: '
        f'<code>{balance:.2f}</code> '
        f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji>\n\n'
        f'<tg-emoji emoji-id="{EMOJI_CLOCK}">⏰</tg-emoji>'
        f' Следующий бонус через <b>24 часа</b>'
        f'</blockquote>\n\n'
        f'<blockquote><i>Не убирайте приписку — иначе штраф 72ч!</i></blockquote>',
        parse_mode=ParseMode.HTML,
        reply_markup=kb_back,
    )
    logging.info(
        "[Bonus] Бонус %.2f начислен user_id=%d (name=%s)",
        BONUS_AMOUNT, user_id, safe_name,
    )


# ══════════════════════════════════════════════════════════════════
#  Хендлеры роутера
# ══════════════════════════════════════════════════════════════════

@bonus_router.message(Command("bonus", "бонус"))
async def cmd_bonus_slash(message: Message) -> None:
    await handle_bonus(message)


@bonus_router.message(F.text.func(is_bonus_command))
async def cmd_bonus_text(message: Message) -> None:
    await handle_bonus(message)


# ══════════════════════════════════════════════════════════════════
#  Фоновый воркер: проверка приписки каждые 2 часа
# ══════════════════════════════════════════════════════════════════

async def _check_one_user(user_id: int) -> None:
    """Проверяет одного пользователя в watchdog (под семафором)."""
    async with _watchdog_semaphore:  # [FIX-9]
        try:
            first_name, bio = await _fetch_user_info(user_id)
            name_ok = _has_tag(first_name)
            bio_ok  = _has_tag(bio)

            if not name_ok or not bio_ok:
                async with _user_locks[user_id]:  # [FIX-1]
                    _apply_penalty(user_id)

                logging.warning(
                    "[Bonus] Watchdog: user_id=%d убрал приписку "
                    "(name_ok=%s, bio_ok=%s) — штраф 72ч",
                    user_id, name_ok, bio_ok,
                )

                if _bot is not None:
                    try:
                        await _bot.send_message(
                            chat_id=user_id,
                            text=(
                                f'<blockquote><b>'
                                f'<tg-emoji emoji-id="{EMOJI_WARN}">⚠️</tg-emoji>'
                                f' Приписка убрана!</b></blockquote>\n\n'
                                f'<blockquote>'
                                f'Мы заметили, что вы убрали '
                                f'<code>@FesteryCas_bot</code> из никнейма или bio.\n\n'
                                f'<tg-emoji emoji-id="{EMOJI_CLOCK}">⏰</tg-emoji> '
                                f'Следующий бонус доступен через <b>72 часа</b>.\n\n'
                                f'Верните приписку — и снова получайте бонус каждые 24 часа!'
                                f'</blockquote>'
                            ),
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception as e:
                        logging.warning(
                            "[Bonus] Не удалось уведомить user_id=%d: %s",
                            user_id, str(e)[:200],
                        )

        except Exception as e:
            logging.error("[Bonus] Watchdog ошибка user_id=%d: %s", user_id, e)


async def _run_watchdog_check() -> None:
    """Один проход воркера."""
    _cleanup_stale_records()  # [FIX-2]

    to_check = [
        uid for uid, state in _bonus_data.items()
        if state.get("last_claimed") is not None
        and not state.get("penalty", False)
    ]

    if not to_check:
        return

    logging.info("[Bonus] Watchdog: проверяем %d пользователей...", len(to_check))

    tasks = [asyncio.create_task(_check_one_user(uid)) for uid in to_check]
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        raise  # [FIX-8]

    logging.info("[Bonus] Watchdog: проверка завершена.")


async def start_bonus_watchdog() -> None:
    """
    Бесконечный фоновый цикл.
    Запускать через: asyncio.create_task(start_bonus_watchdog())
    """
    logging.info("[Bonus] Watchdog запущен.")
    while True:
        try:
            await asyncio.sleep(CHECK_INTERVAL)
            await _run_watchdog_check()
        except asyncio.CancelledError:
            logging.info("[Bonus] Watchdog остановлен.")
            raise  # [FIX-8]
        except Exception as e:
            logging.error("[Bonus] Watchdog ошибка: %s", e)
