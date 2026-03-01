"""
helper.py — Модуль помощи. Команда /help.
"""

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.enums import ParseMode

helper_router = Router()

HELP_TEXT = (
    "<blockquote>"
    "🎰 <b>FesteryCas — список команд</b>"
    "</blockquote>\n\n"

    # ── Мины ──────────────────────────────────────────────────────
    "<blockquote>"
    "💣 <b>МИНЫ</b>\n"
    "<code>/мины (сумма) (количество мин)</code>\n"
    "Пример: <code>/мины 1.5 3</code>"
    "</blockquote>\n\n"

    # ── Башня ─────────────────────────────────────────────────────
    "<blockquote>"
    "🏰 <b>БАШНЯ</b>\n"
    "<code>/башня (сумма) (сложность)</code>\n"
    "Пример: <code>/башня 1.5 2</code>"
    "</blockquote>\n\n"

    # ── Игры с кубиком/мячом ──────────────────────────────────────
    "<blockquote>"
    "🎲 <b>ОБЫЧНЫЕ ИГРЫ</b>\n\n"
    "🎲 Кубик\n"
    "<code>/куб (исход) (сумма)</code>\n"
    "Исходы: <code>1 2 3 4 5 6</code> или <code>чёт</code> / <code>нечёт</code>\n\n"
    "⚽️ Футбол\n"
    "<code>/фут (исход) (сумма)</code>\n"
    "Исходы: <code>гол</code> / <code>мимо</code>\n\n"
    "🏀 Баскетбол\n"
    "<code>/баскет (исход) (сумма)</code>\n"
    "Исходы: <code>гол</code> / <code>мимо</code>\n\n"
    "🎯 Дартс\n"
    "<code>/дартс (исход) (сумма)</code>\n"
    "Исходы: <code>1 2 3 4 5 6</code> или <code>булл</code>\n\n"
    "🎳 Боулинг\n"
    "<code>/боул (исход) (сумма)</code>\n"
    "Исходы: <code>страйк</code> / <code>мимо</code>"
    "</blockquote>\n\n"

    # ── Дуэли ─────────────────────────────────────────────────────
    "<blockquote>"
    "⚔️ <b>ДУЭЛИ</b>\n\n"
    "🎳 Боулинг x3\n"
    "<code>/boulx3 (сумма)</code>\n\n"
    "🎲 Кубик x3\n"
    "<code>/dicex3 (сумма)</code>\n\n"
    "🏀 Баскетбол x3\n"
    "<code>/basketx3 (сумма)</code>\n\n"
    "⚽️ Футбол x3\n"
    "<code>/footx3 (сумма)</code>\n\n"
    "🎯 Дартс x3\n"
    "<code>/dartx3 (сумма)</code>"
    "</blockquote>\n\n"

    # ── Быстрые команды ───────────────────────────────────────────
    "<blockquote>"
    "⚡️ <b>БЫСТРЫЕ КОМАНДЫ</b>\n\n"
    "<code>b</code> / <code>bal</code> / <code>balance</code> — баланс\n"
    "<code>games</code> / <code>игры</code> — меню игр\n"
    "<code>/bonus</code> — получить ежедневный бонус"
    "</blockquote>"
)


@helper_router.message(Command("help", "помощь"))
async def cmd_help(message: Message) -> None:
    await message.answer(HELP_TEXT, parse_mode=ParseMode.HTML)
