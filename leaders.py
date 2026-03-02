import logging
import sqlite3
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

EMOJI_TOP1  = "5440539497383087970"
EMOJI_TOP2  = "5447203607294265305"
EMOJI_TOP3  = "5453902265922376865"
EMOJI_TOP4  = "5382054253403577563"
EMOJI_TOP5  = "5391197405553107640"
EMOJI_TOP6  = "5390966190283694453"
EMOJI_TOP7  = "5382132232829804982"
EMOJI_TOP8  = "5391038994274329680"
EMOJI_TOP9  = "5391234698754138414"
EMOJI_TOP10 = "5393480373944459905"

EMOJI_TOP_LIST = [
    EMOJI_TOP1, EMOJI_TOP2, EMOJI_TOP3, EMOJI_TOP4, EMOJI_TOP5,
    EMOJI_TOP6, EMOJI_TOP7, EMOJI_TOP8, EMOJI_TOP9, EMOJI_TOP10,
]

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

DB_PATH = "casino.db"

# ── Кэш в памяти ─────────────────────────────────────────────────────────────
# _stats[user_id][date_str] = {
#   "turnover": float, "wins": float,
#   "deposits": float, "withdrawals": float,
#   "name": str   ← first_name [+ last_name], иначе username, иначе "User {id}"
# }
_stats: dict = {}

def _noop_set_owner(message_id: int, user_id: int): pass
def _noop_is_owner(message_id: int, user_id: int) -> bool: return True

set_owner_fn = _noop_set_owner
is_owner_fn  = _noop_is_owner


# ══════════════════════════════════════════════════════════════════
#  Вспомогательная: единое правило определения имени
#  Приоритет: first_name [+ last_name] → username → "User {uid}"
# ══════════════════════════════════════════════════════════════════

def _resolve_display_name(
    user_id: int,
    username: str = "",
    first_name: str = "",
    last_name: str = "",
) -> str:
    nickname = (first_name or "").strip()
    if last_name:
        nickname = f"{nickname} {last_name.strip()}".strip()
    if nickname:
        return nickname
    if username:
        return username
    return f"User {user_id}"


# ══════════════════════════════════════════════════════════════════
#  SQLite: инициализация таблицы
# ══════════════════════════════════════════════════════════════════

def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_leaders_db():
    """
    Создаёт таблицу leaders_stats если её нет.
    Загружает данные в память (_stats) и синхронизирует имена из users.
    """
    try:
        with _db_connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS leaders_stats (
                    user_id     INTEGER NOT NULL,
                    date        TEXT    NOT NULL,
                    name        TEXT    DEFAULT '',
                    turnover    REAL    DEFAULT 0.0,
                    wins        REAL    DEFAULT 0.0,
                    deposits    REAL    DEFAULT 0.0,
                    withdrawals REAL    DEFAULT 0.0,
                    PRIMARY KEY (user_id, date)
                )
            """)
            # Миграция: добавляем колонки если таблица уже существовала без них
            for col in ("deposits", "withdrawals"):
                try:
                    conn.execute(f"ALTER TABLE leaders_stats ADD COLUMN {col} REAL DEFAULT 0.0")
                except Exception:
                    pass
            conn.commit()
        logging.info("[Leaders] Таблица leaders_stats готова.")
        _load_stats_from_db()
        sync_names_from_db()
    except Exception as e:
        logging.error(f"[Leaders] Ошибка инициализации БД: {e}")


def _load_stats_from_db():
    """Загружает всю таблицу leaders_stats в память (_stats)."""
    global _stats
    try:
        with _db_connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT user_id, date, name, turnover, wins, deposits, withdrawals "
                "FROM leaders_stats"
            )
            rows = cur.fetchall()
        _stats = {}
        for row in rows:
            uid  = int(row["user_id"])
            date = row["date"]
            if uid not in _stats:
                _stats[uid] = {}
            _stats[uid][date] = {
                "turnover":    float(row["turnover"]    or 0.0),
                "wins":        float(row["wins"]        or 0.0),
                "deposits":    float(row["deposits"]    or 0.0),
                "withdrawals": float(row["withdrawals"] or 0.0),
                "name":        row["name"] or f"User {uid}",
            }
        logging.info(f"[Leaders] Загружено записей из БД: {len(rows)}")
    except Exception as e:
        logging.error(f"[Leaders] Ошибка загрузки stats из БД: {e}")


def _save_stat_to_db(user_id: int, date: str):
    """Сохраняет одну запись (user_id, date) из _stats в leaders_stats."""
    try:
        day = _stats.get(user_id, {}).get(date)
        if day is None:
            return
        with _db_connect() as conn:
            conn.execute("""
                INSERT INTO leaders_stats (user_id, date, name, turnover, wins, deposits, withdrawals)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, date) DO UPDATE SET
                    name        = excluded.name,
                    turnover    = excluded.turnover,
                    wins        = excluded.wins,
                    deposits    = excluded.deposits,
                    withdrawals = excluded.withdrawals
            """, (
                user_id, date,
                day["name"],
                day["turnover"],
                day["wins"],
                day["deposits"],
                day["withdrawals"],
            ))
            conn.commit()
    except Exception as e:
        logging.error(f"[Leaders] Ошибка сохранения stat в БД: {e}")


def _ensure_day(user_id: int, date: str, name: str = ""):
    """Гарантирует наличие записи для (user_id, date) в _stats."""
    if user_id not in _stats:
        _stats[user_id] = {}
    if date not in _stats[user_id]:
        _stats[user_id][date] = {
            "turnover": 0.0, "wins": 0.0,
            "deposits": 0.0, "withdrawals": 0.0,
            "name": name or f"User {user_id}",
        }
    if name:
        _stats[user_id][date]["name"] = name


# ══════════════════════════════════════════════════════════════════
#  Синхронизация имён из таблицы users
#  Приоритет: first_name [+ last_name] → username → "User {uid}"
# ══════════════════════════════════════════════════════════════════

def sync_names_from_db():
    """
    Подтягивает актуальные имена из таблицы users в кэш _stats.
    """
    try:
        with _db_connect() as conn:
            cur = conn.cursor()
            # Пробуем выбрать last_name — если колонки нет, fallback без неё
            try:
                cur.execute("SELECT user_id, first_name, last_name, username FROM users")
                rows = cur.fetchall()
                has_last_name = True
            except Exception:
                cur.execute("SELECT user_id, first_name, username FROM users")
                rows = cur.fetchall()
                has_last_name = False

        updated = 0
        for row in rows:
            uid        = int(row["user_id"])
            first_name = row["first_name"] or ""
            username   = row["username"]   or ""
            last_name  = (row["last_name"] or "") if has_last_name else ""
            display    = _resolve_display_name(uid, username, first_name, last_name)
            if uid in _stats:
                for date in _stats[uid]:
                    _stats[uid][date]["name"] = display
                updated += 1
        logging.info(f"[Leaders] Имена синхронизированы из users: {updated} пользователей.")
    except Exception as e:
        logging.warning(f"[Leaders] sync_names_from_db пропущен: {e}")


# ══════════════════════════════════════════════════════════════════
#  Вспомогательные функции дат
# ══════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════
#  Публичные функции записи результатов
# ══════════════════════════════════════════════════════════════════

def record_game_result(user_id: int, name: str, bet: float, win: float):
    """
    Вызывается из mines.py / tower.py / gold.py / game.py / duels.py
    после завершённой ставки.

    name — уже готовое отображаемое имя (first_name [+ last_name] или username),
           сформированное через _get_display_name() / _fmt_user() в игровом модуле.
    bet  — размер ставки (оборот)
    win  — сумма выплаты (0.0 при проигрыше)
    """
    date = _today_str()
    _ensure_day(user_id, date, name)
    _stats[user_id][date]["turnover"] += bet
    _stats[user_id][date]["wins"]     += win
    _stats[user_id][date]["name"]      = name
    _save_stat_to_db(user_id, date)

    _save_to_game_results_sync(user_id, "gold", win)


def _save_to_game_results_sync(user_id: int, game_name: str, win_amount: float):
    """Синхронная запись в game_results (таблица из database.py)."""
    try:
        with _db_connect() as conn:
            conn.execute("""
                INSERT INTO game_results (user_id, game_name, win_amount)
                VALUES (?, ?, ?)
            """, (user_id, game_name, win_amount))
            conn.commit()
    except Exception as e:
        logging.error(f"[Leaders] Ошибка записи в game_results: {e}")


def record_deposit_stat(user_id: int, name: str, amount: float):
    """
    Вызывается из payment.py после подтверждения депозита.
    name должен быть уже разрешён через _resolve_display_name.
    """
    date = _today_str()
    _ensure_day(user_id, date, name)
    _stats[user_id][date]["deposits"] += amount
    _stats[user_id][date]["name"]      = name
    _save_stat_to_db(user_id, date)
    logging.info(f"[Leaders] Депозит записан: user_id={user_id}, amount={amount}, date={date}")


def record_withdrawal_stat(user_id: int, name: str, amount: float):
    """
    Вызывается из payment.py после успешного вывода.
    name должен быть уже разрешён через _resolve_display_name.
    """
    date = _today_str()
    _ensure_day(user_id, date, name)
    _stats[user_id][date]["withdrawals"] += amount
    _stats[user_id][date]["name"]         = name
    _save_stat_to_db(user_id, date)
    logging.info(f"[Leaders] Вывод записан: user_id={user_id}, amount={amount}, date={date}")


def rollback_withdrawal_stat(user_id: int, amount: float):
    """Откатывает record_withdrawal_stat — вызывается если чек не был создан."""
    date = _today_str()
    day  = _stats.get(user_id, {}).get(date)
    if day is None:
        return
    day["withdrawals"] = max(0.0, round(day["withdrawals"] - amount, 8))
    _save_stat_to_db(user_id, date)
    logging.info(f"[Leaders] Откат вывода в стате: user_id={user_id}, amount={amount}")


def update_user_name(storage, user_id: int, first_name: str):
    try:
        user = storage.get_user(user_id)
        if first_name:
            user['first_name'] = first_name
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════
#  Топ-10
# ══════════════════════════════════════════════════════════════════

def get_top10(storage, leader_type: str, period: str) -> list:
    # Проверка допустимых значений (защита от инъекций через leader_type/period)
    if leader_type not in LEADER_TYPES or period not in LEADER_PERIODS:
        return []

    dates   = _dates_for_period(period)
    results = {}

    for uid, day_data in _stats.items():
        total = 0.0
        name  = f"User {uid}"
        for d in dates:
            if d in day_data:
                total += day_data[d].get(leader_type, 0.0)
                name   = day_data[d].get("name", name)
        if total > 0:
            results[uid] = {"user_id": uid, "name": name, "value": total}

    sorted_list = sorted(results.values(), key=lambda x: x["value"], reverse=True)
    return sorted_list[:10]


# ══════════════════════════════════════════════════════════════════
#  Клавиатура
# ══════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════
#  Текст таблицы
# ══════════════════════════════════════════════════════════════════

def build_leaders_text(storage, leader_type: str, period: str) -> str:
    # Защита от недопустимых значений
    if leader_type not in LEADER_TYPES:
        leader_type = "turnover"
    if period not in LEADER_PERIODS:
        period = "today"

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
            emoji_id = EMOJI_TOP_LIST[i - 1]
            lines.append(
                f'<tg-emoji emoji-id="{emoji_id}">🏅</tg-emoji> '
                f'<b>{entry["name"]}</b> — '
                f'<code>{entry["value"]:,.2f}</code>'
                f'<tg-emoji emoji-id="{EMOJI_COIN}">💰</tg-emoji>'
            )
        body = "\n".join(lines) + "\n"

    return header + body


# ══════════════════════════════════════════════════════════════════
#  Публичная функция входа
# ══════════════════════════════════════════════════════════════════

async def show_leaders(callback: CallbackQuery, storage_obj):
    text = build_leaders_text(storage_obj, "turnover", "today")
    kb   = get_leaders_keyboard("turnover", "today")
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    set_owner_fn(callback.message.message_id, callback.from_user.id)
    await callback.answer()


# ══════════════════════════════════════════════════════════════════
#  Хендлер переключения
# ══════════════════════════════════════════════════════════════════

@leaders_router.callback_query(F.data.startswith("leaders:"))
async def leaders_switch(callback: CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer()
        return

    _, leader_type, period = parts

    # ── Валидация входных данных ──────────────────────────────────
    if leader_type not in LEADER_TYPES or period not in LEADER_PERIODS:
        await callback.answer("Неверные параметры", show_alert=True)
        return

    msg_id = callback.message.message_id
    if not is_owner_fn(msg_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
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
