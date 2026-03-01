"""
bonus.py - Бонусная система Telegram-казино.

Логика:
  - Проверяется ТОЛЬКО никнейм (first_name) через getChat - работает надёжно.
  - Bio НЕ проверяется технически, но в требованиях показываем что надо
    поставить и в «О себе» - большинство поставят оба условия.
  - Бонус 0.1 монеты каждые 24 часа.
  - Штраф 72ч если убрал приписку из ника.
  - Воркер проверяет ник каждые 2 часа.

Команды: /bonus | /бонус | bonus | бонус
"""

import asyncio
import html
import logging
import time
from collections import defaultdict

import aiohttp

from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.enums import ParseMode

try:
    from payments import storage as _storage
except ImportError:
    _storage = None
    logging.warning("[Bonus] payments.py не найден.")

BONUS_AMOUNT     = 0.1
BONUS_COOLDOWN   = 24 * 3600
PENALTY_COOLDOWN = 72 * 3600
CHECK_INTERVAL   = 2  * 3600
STALE_THRESHOLD  = 30 * 86400

# Проверяем ТОЛЬКО это (в никнейме)
REQUIRED_NAME_SUBSTRING = "@FesteryCas_bot"

EMOJI_COIN   = "5197434882321567830"
EMOJI_WIN    = "5278467510604160626"
EMOJI_LOSS   = "5447183459602669338"
EMOJI_CLOCK  = "5303214794336125778"
EMOJI_TROPHY = "5461151367559141950"
EMOJI_BONUS  = "5443127283898405358"
EMOJI_WARN   = "5210952531676504517"
EMOJI_SHIELD = "5906986955911993888"
EMOJI_ERROR = "5420323339723881652"
bonus_router = Router()

_bonus_data: dict[int, dict] = {}
_user_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
_watchdog_semaphore = asyncio.Semaphore(5)
_bot: Bot | None = None
_bot_token: str | None = None


def setup_bonus(bot: Bot) -> None:
    global _bot, _bot_token
    _bot = bot
    _bot_token = bot.token


def _now() -> float:
    return time.monotonic()


def _fmt_time(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h}ч {m}м" if h > 0 else f"{m}м"


def _get_user_state(user_id: int) -> dict:
    if user_id not in _bonus_data:
        _bonus_data[user_id] = {
            "last_claimed":  None,
            "penalty":       False,
            "penalty_at":    None,
            "last_activity": _now(),
        }
    return _bonus_data[user_id]


def _check_name(first_name: str | None) -> bool:
    if not first_name or not first_name.strip():
        return False
    return REQUIRED_NAME_SUBSTRING.lower() in first_name.strip().lower()


async def _fetch_first_name(user_id: int) -> str | None:
    """Получает first_name через прямой HTTP-запрос к Bot API."""
    if _bot_token is None:
        return None
    url = f"https://api.telegram.org/bot{_bot_token}/getChat"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                params={"chat_id": user_id},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if not data.get("ok"):
                    return None
                return data["result"].get("first_name") or None
    except Exception as e:
        logging.warning("[Bonus] getChat exception user_id=%d: %s", user_id, str(e)[:200])
        return None


def _can_claim(user_id: int) -> tuple[bool, float, bool]:
    state = _get_user_state(user_id)
    now   = _now()

    if state["penalty"]:
        if state["penalty_at"] is None:
            state["penalty"] = False
        else:
            elapsed = now - state["penalty_at"]
            if elapsed < PENALTY_COOLDOWN:
                return False, PENALTY_COOLDOWN - elapsed, True
            state["penalty"]    = False
            state["penalty_at"] = None

    if state["last_claimed"] is not None:
        elapsed = now - state["last_claimed"]
        if elapsed < BONUS_COOLDOWN:
            return False, BONUS_COOLDOWN - elapsed, False

    return True, 0.0, False


def _apply_penalty(user_id: int) -> None:
    state = _get_user_state(user_id)
    if not state["penalty"]:
        state["penalty"]    = True
        state["penalty_at"] = _now()
        logging.info("[Bonus] Штраф 72ч user_id=%d", user_id)


def _cleanup_stale_records() -> None:
    now   = _now()
    stale = [
        uid for uid, st in _bonus_data.items()
        if now - st.get("last_activity", 0) > STALE_THRESHOLD
    ]
    for uid in stale:
        del _bonus_data[uid]
        _user_locks.pop(uid, None)
    if stale:
        logging.info("[Bonus] Очищено %d записей.", len(stale))


def is_bonus_command(text: str) -> bool:
    if not text:
        return False
    return text.strip().lower().lstrip("/") in ("bonus", "бонус")


# ── Основная логика ───────────────────────────────────────────────

async def handle_bonus(message: Message, user_id: int | None = None) -> None:
    actual_user_id = user_id if user_id is not None else message.from_user.id
    async with _user_locks[actual_user_id]:
        await _handle_bonus_locked(message, actual_user_id)


async def _handle_bonus_locked(message: Message, user_id: int) -> None:
    state = _get_user_state(user_id)
    state["last_activity"] = _now()

    # ── Проверяем кулдаун сначала (без лишних запросов к API) ────
    can, remaining, is_penalty = _can_claim(user_id)
    if not can:
        body = (
            "<tg-emoji emoji-id=\"" + EMOJI_WARN + "\">⚠️</tg-emoji> "
            "<b>Вы убрали приписку — действует штраф!</b>\n\n"
            "Бонус будет доступен через: <b>" + _fmt_time(remaining) + "</b>"
        ) if is_penalty else (
            "<tg-emoji emoji-id=\"" + EMOJI_CLOCK + "\">⏰</tg-emoji> "
            "<b>Бонус уже получен!</b>\n\n"
            "Следующий бонус через: <b>" + _fmt_time(remaining) + "</b>"
        )
        await message.answer(
            "<blockquote>" + body + "</blockquote>",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Проверяем никнейм ────────────────────────────────────────
    first_name = await _fetch_first_name(user_id)
    name_ok    = _check_name(first_name)

    if not name_ok:
        safe_fn = html.escape(first_name or "не удалось получить")
        await message.answer(
            "<blockquote><b>"
            "<tg-emoji emoji-id=\"" + EMOJI_ERROR + "\">🛡</tg-emoji>"
            " Бонус недоступен</b></blockquote>\n\n"
            "<blockquote>"
            "Для получения бонуса выполни оба условия:\n\n"
            "<tg-emoji emoji-id=\"" + EMOJI_LOSS + "\">❌</tg-emoji> "
            "В <b>никнейме</b> должна быть: <code>@FesteryCas_bot</code>\n"
            "  Пример: <code>Иван @FesteryCas_bot</code>\n\n"
            "<tg-emoji emoji-id=\"" + EMOJI_LOSS + "\">❌</tg-emoji> "
            "В <b>«О себе»</b> должна быть:\n"
            "  <code>честная игровая зона-@FesteryCas_bot</code>\n\n"
            "После выполнения введите /bonus снова!</blockquote>",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Начисляем ────────────────────────────────────────────────
    if _storage is None:
        await message.answer(
            "<blockquote>"
            "<tg-emoji emoji-id=\"" + EMOJI_LOSS + "\">❌</tg-emoji>"
            " Ошибка системы. Попробуйте позже.</blockquote>",
            parse_mode=ParseMode.HTML,
        )
        return

    state["last_claimed"] = _now()
    state["penalty"]      = False
    state["penalty_at"]   = None

    try:
        _storage.add_balance(user_id, BONUS_AMOUNT)
    except Exception as e:
        state["last_claimed"] = None
        logging.error("[Bonus] Ошибка начисления user_id=%d: %s", user_id, e)
        await message.answer(
            "<blockquote>"
            "<tg-emoji emoji-id=\"" + EMOJI_LOSS + "\">❌</tg-emoji>"
            " Ошибка начисления. Попробуйте позже.</blockquote>",
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        balance = _storage.get_balance(user_id)
    except Exception as e:
        logging.error("[Bonus] Ошибка баланса user_id=%d: %s", user_id, e)
        balance = 0.0

    safe_name = html.escape(first_name or str(user_id))
    logging.info("[Bonus] Бонус %.2f начислен user_id=%d name=%s", BONUS_AMOUNT, user_id, safe_name)

    await message.answer(
        "<blockquote><b>"
        "<tg-emoji emoji-id=\"" + EMOJI_TROPHY + "\">🏆</tg-emoji>"
        " Бонус получен!</b></blockquote>\n\n"
        "<blockquote>"
        "<tg-emoji emoji-id=\"" + EMOJI_BONUS + "\">💎</tg-emoji> Начислено: "
        "<code>+" + str(BONUS_AMOUNT) + "</code> "
        "<tg-emoji emoji-id=\"" + EMOJI_COIN + "\">💰</tg-emoji>\n"
        "<tg-emoji emoji-id=\"" + EMOJI_WIN + "\">🏆</tg-emoji> Баланс: "
        "<code>" + f"{balance:.2f}" + "</code> "
        "<tg-emoji emoji-id=\"" + EMOJI_COIN + "\">💰</tg-emoji>\n\n"
        "<tg-emoji emoji-id=\"" + EMOJI_CLOCK + "\">⏰</tg-emoji>"
        " Следующий бонус через <b>24 часа</b>"
        "</blockquote>\n\n"
        "<blockquote><i>Не убирайте приписку из ника — иначе штраф 72ч!</i></blockquote>",
        parse_mode=ParseMode.HTML,
    )


# ── Хендлеры ─────────────────────────────────────────────────────

@bonus_router.message(Command("bonus", "бонус"))
async def cmd_bonus_slash(message: Message) -> None:
    await handle_bonus(message)


@bonus_router.message(F.text.func(is_bonus_command))
async def cmd_bonus_text(message: Message) -> None:
    await handle_bonus(message)


# ── Воркер (проверяет ник каждые 2ч) ─────────────────────────────

async def _check_one_user(user_id: int) -> None:
    async with _watchdog_semaphore:
        try:
            first_name = await _fetch_first_name(user_id)
            if not _check_name(first_name):
                async with _user_locks[user_id]:
                    _apply_penalty(user_id)
                logging.warning("[Bonus] Watchdog: user_id=%d убрал ник — штраф 72ч", user_id)
                if _bot is not None:
                    try:
                        await _bot.send_message(
                            chat_id=user_id,
                            text=(
                                "<blockquote><b>"
                                "<tg-emoji emoji-id=\"" + EMOJI_WARN + "\">⚠️</tg-emoji>"
                                " Приписка убрана!</b></blockquote>\n\n"
                                "<blockquote>"
                                "Вы убрали <code>@FesteryCas_bot</code> из никнейма.\n\n"
                                "<tg-emoji emoji-id=\"" + EMOJI_CLOCK + "\">⏰</tg-emoji> "
                                "Следующий бонус через <b>72 часа</b>.\n\n"
                                "Верните приписку — и снова получайте бонус каждые 24ч!"
                                "</blockquote>"
                            ),
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception as e:
                        logging.warning("[Bonus] Уведомление не доставлено user_id=%d: %s", user_id, str(e)[:200])
        except Exception as e:
            logging.error("[Bonus] Watchdog ошибка user_id=%d: %s", user_id, e)


async def _run_watchdog_check() -> None:
    _cleanup_stale_records()
    to_check = [
        uid for uid, st in _bonus_data.items()
        if st.get("last_claimed") is not None and not st.get("penalty", False)
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
        raise
    logging.info("[Bonus] Watchdog завершён.")


async def start_bonus_watchdog() -> None:
    """asyncio.create_task(start_bonus_watchdog())"""
    logging.info("[Bonus] Watchdog запущен.")
    while True:
        try:
            await asyncio.sleep(CHECK_INTERVAL)
            await _run_watchdog_check()
        except asyncio.CancelledError:
            logging.info("[Bonus] Watchdog остановлен.")
            raise
        except Exception as e:
            logging.error("[Bonus] Watchdog ошибка: %s", e)
