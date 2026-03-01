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
    '<tg-emoji emoji-id=\"5334544901428229844\">🎉</tg-emoji><b>Инструкция:</b>'
    "</blockquote>\n"

    "<blockquote>"
    "💣 <b>МИНЫ</b>\n"
    "<code>/мины [сумма] [кол-во мин]</code>\n"
    "Пример: <code>/мины 1.5 3</code>"
    "</blockquote>\n"

    "<blockquote>"
    "🏰 <b>БАШНЯ</b>\n"
    "<code>/башня [сумма] [сложность]</code>\n"
    "Пример: <code>/башня 1.5 2</code>"
    "</blockquote>\n"

    "<blockquote>"
    "🎲 <b>ОБЫЧНЫЕ ИГРЫ</b>\n\n"
    "🎲 Кубик\n"
    "<code>/куб [исход] [сумма]</code>\n"
    "Исходы: <code>больше</code> / <code>меньше</code> / <code>чёт</code> / <code>нечёт</code>\n\n"
    "⚽️ Футбол\n"
    "<code>/фут [исход] [сумма]</code>\n"
    "Исходы: <code>гол</code> / <code>мимо</code>\n\n"
    "🏀 Баскетбол\n"
    "<code>/баскет [исход] [сумма]</code>\n"
    "Исходы: <code>3очка</code> / <code>гол</code> / <code>мимо</code>\n\n"
    "🎯 Дартс\n"
    "<code>/дартс [исход] [сумма]</code>\n"
    "Исходы: <code>центр</code> / <code>красное</code> / <code>белое</code>\n\n"
    "🎳 Боулинг\n"
    "<code>/боул [исход] [сумма]</code>\n"
    "Исходы: <code>страйк</code> / <code>поражение</code> <code>победа</code>"
    "</blockquote>\n"

    "<blockquote>"
    "⚔️ <b>ДУЭЛИ</b>\n\n"
    "🎳 <code>/boulx3 [сумма]</code>\n"
    "🎲 <code>/dicex3 [сумма]</code>\n"
    "🏀 <code>/basketx3 [сумма]</code>\n"
    "⚽️ <code>/footx3 [сумма]</code>\n"
    "🎯 <code>/dartx3 [сумма]</code>"
    "</blockquote>\n"

    "<blockquote>"
    '<tg-emoji emoji-id=\"5323442290708985472\">🎉</tg-emoji><b>БЫСТРЫЕ КОМАНДЫ</b>\n\n'
    "<code>b</code> / <code>bal</code> / <code>balance</code> — баланс\n"
    "<code>games</code> / <code>игры</code> — меню игр\n"
    "<code>/bonus</code> — ежедневный бонус"
    "</blockquote>"
)


@helper_router.message(Command("help", "помощь"))
async def cmd_help(message: Message) -> None:
    await message.answer(HELP_TEXT, parse_mode=ParseMode.HTML)
