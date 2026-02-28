"""
bonus.py — Бонусная система Telegram-казино.

Механика:
  - Бонус 0.1 монеты каждые 24 часа.
  - Требование: в username пользователя должна быть приписка @FesteryCas_bot,
    а в описании (bio) профиля — строка "@FesteryCas_bot".
  - Если после получения бонуса приписка убрана — следующий бонус
    доступен только через 72 часа (штраф).
  - Каждые 2 часа фоновый воркер проверяет всех получивших бонус:
    если приписка убрана — ставит штрафной таймер 72ч.

Команды: /bonus | /бонус | bonus | бонус
"""

import asyncio
import logging
import re
import time
from datetime import datetime, timezone

from aiogram import Router, Bot
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode

# ─── внешние зависимости ──────────────────────────────────────────────────────
try:
    from payments import storage as _storage
except ImportError:
    _storage = None
    logging.warning("[Bonus] payments.py не найден — баланс не будет начисляться.")

# ─── конфигурация ─────────────────────────────────────────────────────────────
BONUS_AMOUNT       = 0.1          # размер бонуса
BONUS_COOLDOWN     = 24 * 3600    # 24 часа (секунды) — обычный интервал
PENALTY_COOLDOWN   = 72 * 3600    # 72 часа — штраф за снятие приписки
CHECK_INTERVAL     = 2  * 3600    # 2 часа — интервал фонового воркера

REQUIRED_USERNAME_SUFFIX = "FesteryCas_bot"   # без @, проверка case-insensitive
REQUIRED_BIO_SUBSTRING   = "@FesteryCas_bot"  # регистронезависимо

# ─── ID кастомных эмодзи ──────────────────────────────────────────────────────
EMOJI_BACK     = "5906771962734057347"
EMOJI_COIN     = "5197434882321567830"
EMOJI_WIN      = "5278467510604160626"
EMOJI_LOSS     = "5447183459602669338"
EMOJI_CHECK    = "5197269100878907942"
EMOJI_CLOCK    = "5303214794336125778"
EMOJI_TROPHY   = "5461151367559141950"
EMOJI_BONUS    = "5443127283898405358"
EMOJI_WARN     = "5210952531676504517"
EMOJI_SHIELD   = "5906986955911993888"

# ─── роутер ───────────────────────────────────────────────────────────────────
bonus_router = Router()

# ─── хранилище состояний бонуса ───────────────────────────────────────────────
# _bonus_data[user_id] = {
#   "last_claimed": float (unix timestamp) или None,
#   "penalty":      bool  — True если применён штраф 72ч,
#   "penalty_at":   float (unix timestamp) начала штрафа или None,
# }
_bonus_data: dict[int, dict] = {}

# Глобальный бот (устанавливается через setup_bonus)
_bot: Bot | None = None


def setup_bonus(bot: Bot):
    """Вызывается из main.py при старте."""
    global _bot
    _bot = bot


# ══════════════════════════════════════════════════════════════════
#  Вспомогательные функции
# ══════════════════════════════════════════════════════════════════

def _now() -> float:
    return time.time()


def _fmt_time(seconds: float) -> str:
    """Форматирует оставшееся время в 'Xч Yм'."""
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    if h > 0:
        return f"{h}ч {m}м"
    return f"{m}м"


def _get_user_state(user_id: int) -> dict:
    if user_id not in _bonus_data:
        _bonus_data[user_id] = {
            "last_claimed": None,
            "penalty":      False,
            "penalty_at":   None,
        }
    return _bonus_data[user_id]


def _check_username(username: str | None) -> bool:
    """
    Проверяет, содержит ли username приписку FesteryCas_bot.
    Принимает username БЕЗ символа @.
    Логика: username должен заканчиваться на 'FesteryCas_bot'
    (регистронезависимо) — например: ivan_FesteryCas_bot.
    """
    if not username:
        return False
    return username.lower().endswith(REQUIRED_USERNAME_SUFFIX.lower())


def _check_bio(bio: str | None) -> bool:
    """Проверяет, содержит ли bio подстроку '@FesteryCas_bot' (регистронезависимо)."""
    if not bio:
        return False
    return REQUIRED_BIO_SUBSTRING.lower() in bio.lower()


async def _fetch_full_user(user_id: int) -> tuple[str | None, str | None]:
    """
    Получает (username, bio) пользователя через getChat.
    Возвращает (None, None) при ошибке.
    """
    if _bot is None:
        return None, None
    try:
        chat = await _bot.get_chat(user_id)
        username = getattr(chat, 'username', None)
        bio      = getattr(chat, 'bio', None)
        return username, bio
    except Exception as e:
        logging.warning(f"[Bonus] getChat({user_id}) ошибка: {e}")
        return None, None


def _can_claim(user_id: int) -> tuple[bool, float, bool]:
    """
    Возвращает (можно_получить, секунд_осталось, на_штрафе).
    секунд_осталось = 0 если можно получить прямо сейчас.
    """
    state = _get_user_state(user_id)
    now   = _now()

    # Штрафной режим
    if state["penalty"] and state["penalty_at"] is not None:
        elapsed = now - state["penalty_at"]
        if elapsed < PENALTY_COOLDOWN:
            return False, PENALTY_COOLDOWN - elapsed, True
        else:
            # Штраф истёк — снимаем
            state["penalty"]    = False
            state["penalty_at"] = None

    # Обычный кулдаун
    if state["last_claimed"] is not None:
        elapsed = now - state["last_claimed"]
        if elapsed < BONUS_COOLDOWN:
            return False, BONUS_COOLDOWN - elapsed, False

    return True, 0.0, False


def _apply_penalty(user_id: int):
    """Применяет штрафной таймер 72ч."""
    state = _get_user_state(user_id)
    if not state["penalty"]:
        state["penalty"]    = True
        state["penalty_at"] = _now()
        logging.info(f"[Bonus] Штраф 72ч применён для user_id={user_id}")


# ══════════════════════════════════════════════════════════════════
#  Команда /bonus
# ══════════════════════════════════════════════════════════════════

async def handle_bonus(message: Message):
    """Обработчик команды bonus/бонус."""
    user_id  = message.from_user.id

    # ── 1. Проверяем username и bio через getChat ──────────────────
    username, bio = await _fetch_full_user(user_id)

    username_ok = _check_username(username)
    bio_ok      = _check_bio(bio)

    if not username_ok or not bio_ok:
        missing = []
        if not username_ok:
            missing.append(
                f'<tg-emoji emoji-id="{EMOJI_LOSS}">❌</tg-emoji> '
                f'Никнейм должен оканчиваться на <b>{REQUIRED_USERNAME_SUFFIX}</b>\n'
                f'  Пример: <code>ivan_{REQUIRED_USERNAME_SUFFIX}</code>'
            )
        if not bio_ok:
            missing.append(
                f'<tg-emoji emoji-id="{EMOJI_LOSS}">❌</tg-emoji> '
                f'В описании профиля должна быть строка <b>{REQUIRED_BIO_SUBSTRING}</b>'
            )

        await message.answer(
            f'<blockquote><b><tg-emoji emoji-id="{EMOJI_SHIELD}">🛡</tg-emoji> Бонус недоступен</b></blockquote>\n\n'
            f'<blockquote>Для получения бонуса необходимо:\n\n'
            + "\n\n".join(missing) +
            f'\n\n<tg-emoji emoji-id="{EMOJI_BONUS}">💎</tg-emoji> '
            f'После выполнения условий — повторите команду /bonus</blockquote>',
            parse_mode=ParseMode.HTML
        )
        return

    # ── 2. Проверяем кулдаун ──────────────────────────────────────
    can, remaining, is_penalty = _can_claim(user_id)

    if not can:
        if is_penalty:
            penalty_text = (
                f'<tg-emoji emoji-id="{EMOJI_WARN}">⚠️</tg-emoji> '
                f'<b>Вы убрали приписку — действует штраф!</b>\n\n'
                f'Бонус будет доступен через: <b>{_fmt_time(remaining)}</b>'
            )
        else:
            penalty_text = (
                f'<tg-emoji emoji-id="{EMOJI_CLOCK}">⏰</tg-emoji> '
                f'<b>Бонус уже получен!</b>\n\n'
                f'Следующий бонус через: <b>{_fmt_time(remaining)}</b>'
            )

        await message.answer(
            f'<blockquote>{penalty_text}</blockquote>',
            parse_mode=ParseMode.HTML
        )
        return

    # ── 3. Начисляем бонус ────────────────────────────────────────
    if _storage is None:
        await message.answer(
            f'<blockquote><tg-emoji emoji-id="{EMOJI_LOSS}">❌</tg-emoji> '
            f'Ошибка системы. Попробуйте позже.</blockquote>',
            parse_mode=ParseMode.HTML
        )
        return

    state = _get_user_state(user_id)
    state["last_claimed"] = _now()
    state["penalty"]      = False
    state["penalty_at"]   = None

    try:
        _storage.add_balance(user_id, BONUS_AMOUNT)
    except Exception as e:
        logging.error(f"[Bonus] Ошибка начисления баланса user_id={user_id}: {e}")
        # Откат состояния
        state["last_claimed"] = None
        await message.answer(
            f'<blockquote><tg-emoji emoji-id="{EMOJI_LOSS}">❌</tg-emoji> '
            f'Ошибка начисления. Попробуйте позже.</blockquote>',
            parse_mode=ParseMode.HTML
        )
        return

    balance = _storage.get_balance(user_id)

    await message.answer(
        f'<blockquote><b><tg-emoji emoji-id="{EMOJI_TROPHY}">🏆</tg-emoji> Бонус получен!</b></blockquote>\n\n'
        f'<blockquote>'
        f'<tg-emoji emoji-id="{EMOJI_BONUS}">💎</tg-emoji> Начислено: '
        f'<code>+{BONUS_AMOUNT}</code>'
        f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji>\n'
        f'<tg-emoji emoji-id="{EMOJI_WIN}">🏆</tg-emoji> Баланс: '
        f'<code>{balance:.2f}</code>'
        f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji>\n\n'
        f'<tg-emoji emoji-id="{EMOJI_CLOCK}">⏰</tg-emoji> Следующий бонус через <b>24 часа</b>'
        f'</blockquote>\n\n'
        f'<blockquote><i>Не убирайте приписку — иначе штраф 72ч!</i></blockquote>',
        parse_mode=ParseMode.HTML
    )
    logging.info(f"[Bonus] Бонус {BONUS_AMOUNT} начислен user_id={user_id} (@{username})")


# ══════════════════════════════════════════════════════════════════
#  Фоновый воркер: проверка приписки каждые 2 часа
# ══════════════════════════════════════════════════════════════════

async def _bonus_watchdog():
    """
    Фоновая задача: каждые 2 часа проходит по всем пользователям,
    получившим бонус, и проверяет наличие приписки.
    Если приписка убрана — применяет штраф 72ч.
    """
    logging.info("[Bonus] Watchdog запущен.")
    while True:
        try:
            await asyncio.sleep(CHECK_INTERVAL)
            await _run_watchdog_check()
        except asyncio.CancelledError:
            logging.info("[Bonus] Watchdog остановлен.")
            break
        except Exception as e:
            logging.error(f"[Bonus] Watchdog ошибка: {e}")


async def _run_watchdog_check():
    """
    Проверяет всех пользователей, у которых есть запись о бонусе
    и которые не на штрафе — проверяем актуальность приписки.
    """
    now = _now()
    # Только тех, кто получал бонус и у кого нет активного штрафа
    to_check = [
        uid for uid, state in _bonus_data.items()
        if state.get("last_claimed") is not None
        and not state.get("penalty", False)
    ]

    if not to_check:
        return

    logging.info(f"[Bonus] Watchdog: проверяем {len(to_check)} пользователей...")

    for user_id in to_check:
        try:
            username, bio = await _fetch_full_user(user_id)
            username_ok   = _check_username(username)
            bio_ok        = _check_bio(bio)

            if not username_ok or not bio_ok:
                _apply_penalty(user_id)
                logging.warning(
                    f"[Bonus] Watchdog: user_id={user_id} убрал приписку "
                    f"(username_ok={username_ok}, bio_ok={bio_ok}) — штраф 72ч"
                )
                # Уведомляем пользователя в личку
                if _bot is not None:
                    try:
                        await _bot.send_message(
                            chat_id=user_id,
                            text=(
                                f'<blockquote><b>'
                                f'<tg-emoji emoji-id="{EMOJI_WARN}">⚠️</tg-emoji> '
                                f'Приписка убрана!</b></blockquote>\n\n'
                                f'<blockquote>Мы заметили, что вы убрали приписку '
                                f'<b>{REQUIRED_BIO_SUBSTRING}</b> из профиля.\n\n'
                                f'<tg-emoji emoji-id="{EMOJI_CLOCK}">⏰</tg-emoji> '
                                f'Следующий бонус будет доступен через <b>72 часа</b>.\n\n'
                                f'Верните приписку и снова получайте бонус каждые 24 часа!</blockquote>'
                            ),
                            parse_mode=ParseMode.HTML
                        )
                    except Exception as e:
                        logging.warning(f"[Bonus] Не удалось отправить уведомление user_id={user_id}: {e}")

            # Небольшая пауза между запросами к API чтобы не превысить лимиты
            await asyncio.sleep(0.5)

        except Exception as e:
            logging.error(f"[Bonus] Watchdog ошибка для user_id={user_id}: {e}")

    logging.info("[Bonus] Watchdog: проверка завершена.")


def start_bonus_watchdog() -> asyncio.Task:
    """Запускает фоновый воркер. Вызывать из main.py при старте бота."""
    task = asyncio.create_task(_bonus_watchdog())
    logging.info("[Bonus] Watchdog task создан.")
    return task


# ══════════════════════════════════════════════════════════════════
#  Публичная проверка — используется как фильтр в main.py
# ══════════════════════════════════════════════════════════════════

def is_bonus_command(text: str) -> bool:
    """Проверяет, является ли текст командой бонуса."""
    if not text:
        return False
    t = text.strip().lower()
    if t.startswith('/'):
        t = t[1:]
    return t in ('bonus', 'бонус')


# ══════════════════════════════════════════════════════════════════
#  Регистрация хендлера (вызывается из main.py)
# ══════════════════════════════════════════════════════════════════

def register_bonus_handlers(router):
    """
    Регистрирует обработчик команды бонуса в переданный роутер.

    Использование в main.py:
        from bonus import register_bonus_handlers, setup_bonus, start_bonus_watchdog
        register_bonus_handlers(dp)          # или любой другой роутер
        setup_bonus(bot)
        ...
        asyncio.create_task(start_bonus_watchdog())
    """
    from aiogram import F
    from aiogram.filters import Command

    @router.message(Command("bonus", "бонус"))
    async def _cmd_bonus(message: Message):
        await handle_bonus(message)

    @router.message(F.text.func(is_bonus_command))
    async def _text_bonus(message: Message):
        await handle_bonus(message)
