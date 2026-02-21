import logging
from aiogram import Router, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.enums import ParseMode

leaders_router = Router()

# ── ID кастомных эмодзи ───────────────────────────────────────────────────────
EMOJI_LEADERS  = "5440539497383087970"
EMOJI_BACK     = "5906771962734057347"
EMOJI_TURNOVER = "5402186569006210455"
EMOJI_WIN      = "5278467510604160626"
EMOJI_DEPOSIT  = "5443127283898405358"
EMOJI_WITHDRAW = "5445355530111437729"
EMOJI_COIN     = "5197434882321567830"

# ── Кастомные эмодзи для каждого места (замени ID на свои) ───────────────────
EMOJI_TOP1  = "5440539497383087970"   # 🏆  место 1
EMOJI_TOP2  = "5447203607294265305"   # место 2
EMOJI_TOP3  = "5453902265922376865"   # место 3
EMOJI_TOP4  = "5382054253403577563"   # место 4
EMOJI_TOP5  = "5391197405553107640"   # место 5
EMOJI_TOP6  = "5390966190283694453"   # место 6
EMOJI_TOP7  = "5382132232829804982"   # место 7
EMOJI_TOP8  = "5391038994274329680"   # место 8
EMOJI_TOP9  = "5391234698754138414"   # место 9
EMOJI_TOP10 = "5393480373944459905"   # место 10

# Список по порядку (индекс 0 = 1-е место)
EMOJI_TOP_LIST = [
    EMOJI_TOP1, EMOJI_TOP2, EMOJI_TOP3, EMOJI_TOP4, EMOJI_TOP5,
    EMOJI_TOP6, EMOJI_TOP7, EMOJI_TOP8, EMOJI_TOP9, EMOJI_TOP10,
]

# ── Типы и периоды ────────────────────────────────────────────────────────────
LEADER_TYPES   = ["turnover", "wins", "deposits", "withdrawals"]
LEADER_PERIODS = ["today", "yesterday", "week", "month"]

TYPE_LABELS = {
    "turnover":    ("Оборот",   EMOJI_TURNOVER),
    "wins":        ("Выигрыш",  EMOJI_WIN),
    "deposits":    ("Депозиты", EMOJI_DEPOSIT),
    "withdrawals": ("Выводы",   EMOJI_WITHDRAW),
}

PERIOD_LABELS = {
    "today":     "Сегодня",
    "yesterday": "Вчера",
    "week":      "Неделя",
    "month":     "Месяц",
}

# ── Внутреннее хранилище игровой статистики ───────────────────────────────────
# _stats[user_id][date_str] = {"turnover": float, "wins": float, "name": str}
_stats: dict = {}


def _today_str() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _dates_for_period(period: str) -> list:
    from datetime import datetime, timedelta, timezone
    today = datetime.now(timezone.utc).date()
    if period == "today":
        return [str(today)]
    elif period == "yesterday":
        return [str(today - timedelta(days=1))]
    elif period == "week":
        return [str(today - timedelta(days=i)) for i in range(7)]
    elif period == "month":
        return [str(today - timedelta(days=i)) for i in range(30)]
    return [str(today)]


# ── Публичная функция записи результата игры ─────────────────────────────────
def record_game_result(user_id: int, name: str, bet: float, win: float):
    """
    Вызывается из mines.py / tower.py / game.py после каждой завершённой ставки.
    bet  — размер ставки (оборот)
    win  — сумма выплаты (0 при проигрыше; сумма при выигрыше)
    """
    date = _today_str()
    if user_id not in _stats:
        _stats[user_id] = {}
    if date not in _stats[user_id]:
        _stats[user_id][date] = {"turnover": 0.0, "wins": 0.0, "name": name}

    _stats[user_id][date]["turnover"] += bet
    _stats[user_id][date]["wins"]     += win
    _stats[user_id][date]["name"]      = name   # обновляем имя при каждой игре


# ── Сохранение имени пользователя в storage (вызывать при любом обращении) ───
def update_user_name(storage, user_id: int, first_name: str):
    """
    Записывает имя в payments.storage.users чтобы депозиты/выводы
    отображали ник, а не ID.
    """
    try:
        user = storage.get_user(user_id)
        if first_name:
            user['first_name'] = first_name
    except Exception:
        pass


# ── Топ-10 ───────────────────────────────────────────────────────────────────
def get_top10(storage, leader_type: str, period: str) -> list:
    dates = _dates_for_period(period)
    results = {}

    if leader_type in ("turnover", "wins"):
        for uid, day_data in _stats.items():
            total = 0.0
            name  = f"User {uid}"
            for d in dates:
                if d in day_data:
                    total += day_data[d].get(leader_type, 0.0)
                    name   = day_data[d].get("name", name)
            if total > 0:
                results[uid] = {"user_id": uid, "name": name, "value": total}

    elif leader_type in ("deposits", "withdrawals"):
        try:
            users_data = storage.users
        except AttributeError:
            users_data = {}

        field = "total_deposits" if leader_type == "deposits" else "total_withdrawals"
        for uid, data in users_data.items():
            value = float(data.get(field, 0) or 0)
            if value <= 0:
                continue

            # Приоритет: first_name → username → имя из _stats → "User {id}"
            name = (
                data.get("first_name")
                or data.get("username")
                or _get_name_from_stats(uid)
                or f"User {uid}"
            )
            results[uid] = {"user_id": uid, "name": str(name), "value": value}

    sorted_list = sorted(results.values(), key=lambda x: x["value"], reverse=True)
    return sorted_list[:10]


def _get_name_from_stats(user_id: int) -> str:
    """Ищет последнее известное имя игрока в _stats."""
    day_data = _stats.get(user_id, {})
    for date in sorted(day_data.keys(), reverse=True):
        name = day_data[date].get("name", "")
        if name:
            return name
    return ""


# ── Клавиатура ────────────────────────────────────────────────────────────────
def get_leaders_keyboard(active_type: str, active_period: str) -> InlineKeyboardMarkup:
    def type_btn(t_id: str):
        label, emoji_id = TYPE_LABELS[t_id]
        mark = "✦ " if t_id == active_type else ""
        return InlineKeyboardButton(
            text=f"{mark}{label}",
            callback_data=f"leaders:{t_id}:{active_period}",
            icon_custom_emoji_id=emoji_id
        )

    def period_btn(p_id: str):
        mark = "✦ " if p_id == active_period else ""
        return InlineKeyboardButton(
            text=f"{mark}{PERIOD_LABELS[p_id]}",
            callback_data=f"leaders:{active_type}:{p_id}"
        )

    return InlineKeyboardMarkup(inline_keyboard=[
        [type_btn("turnover"), type_btn("wins"), type_btn("deposits"), type_btn("withdrawals")],
        [period_btn("today"), period_btn("yesterday"), period_btn("week"), period_btn("month")],
        [InlineKeyboardButton(
            text="Назад",
            callback_data="back_to_main",
            icon_custom_emoji_id=EMOJI_BACK
        )]
    ])


# ── Текст таблицы ─────────────────────────────────────────────────────────────
def build_leaders_text(storage, leader_type: str, period: str) -> str:
    type_label, type_emoji_id = TYPE_LABELS[leader_type]
    period_label = PERIOD_LABELS[period]
    top = get_top10(storage, leader_type, period)

    header = (
        f'<tg-emoji emoji-id="{EMOJI_LEADERS}">🏆</tg-emoji> '
        f'<b>Таблица лидеров</b>\n'
        f'<blockquote>'
        f'<tg-emoji emoji-id="{type_emoji_id}">⭐</tg-emoji> <b>{type_label}</b> · {period_label}'
        f'</blockquote>\n\n'
    )

    if not top:
        body = '<i>Пока нет данных за выбранный период.</i>\n'
    else:
        lines = []
        for i, entry in enumerate(top, start=1):
            emoji_id = EMOJI_TOP_LIST[i - 1]   # кастомное эмодзи для места
            lines.append(
                f'<tg-emoji emoji-id="{emoji_id}">🏅</tg-emoji> '
                f'<b>{entry["name"]}</b> — '
                f'<code>{entry["value"]:,.2f}</code>'
                f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji>'
            )
        body = "\n".join(lines) + "\n"

    return header + body


# ── Публичная функция входа ───────────────────────────────────────────────────
async def show_leaders(callback: CallbackQuery, storage_obj):
    text = build_leaders_text(storage_obj, "turnover", "today")
    kb   = get_leaders_keyboard("turnover", "today")
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    await callback.answer()


# ── Хендлер переключения ─────────────────────────────────────────────────────
@leaders_router.callback_query(F.data.startswith("leaders:"))
async def leaders_switch(callback: CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer()
        return

    _, leader_type, period = parts

    if leader_type not in LEADER_TYPES or period not in LEADER_PERIODS:
        await callback.answer("Неверные параметры", show_alert=True)
        return

    try:
        from payments import storage as payment_storage
    except ImportError:
        await callback.answer("Ошибка загрузки данных", show_alert=True)
        return

    try:
        text = build_leaders_text(payment_storage, leader_type, period)
        kb   = get_leaders_keyboard(leader_type, period)
        await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    except Exception as e:
        logging.error(f"Leaders error: {e}")

    await callback.answer()
