import json
import logging
import os
from datetime import datetime
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ParseMode

# ──────────────────────────────────────────────
#  НАСТРОЙКИ
# ──────────────────────────────────────────────
REFERRAL_PERCENT   = 2
MIN_REF_WITHDRAWAL = 1.0
REFERRALS_FILE     = "referrals.json"

# ──────────────────────────────────────────────
#  EMOJI
# ──────────────────────────────────────────────
EMOJI_PARTNERS   = "5906986955911993888"
EMOJI_BACK       = "5906771962734057347"
EMOJI_WALLET     = "5445355530111437729"
EMOJI_WITHDRAWAL = "5445355530111437729"
EMOJI_LEADERS    = "5440539497383087970"
EMOJI_STATS      = "5231200819986047254"
EMOJI_COIN       = "5197434882321567830"
EMOJI_CHECK      = "5197269100878907942"
EMOJI_NUMBER     = "5271604874419647061"
EMOJI_REF_USER   = "5906581476639513176"   # замени на нужный


# ──────────────────────────────────────────────
#  FSM
# ──────────────────────────────────────────────
class ReferralWithdraw(StatesGroup):
    entering_amount = State()


# ──────────────────────────────────────────────
#  ХРАНИЛИЩЕ РЕФЕРАЛОВ
# ──────────────────────────────────────────────
class ReferralStorage:
    def __init__(self, filepath: str = REFERRALS_FILE):
        self.filepath = filepath
        self._data: dict = {}
        self._load()

    def _load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            except Exception as ex:
                logging.error(f"[ReferralStorage] Ошибка загрузки: {ex}")
                self._data = {}

    def _save(self):
        try:
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False, indent=2)
        except Exception as ex:
            logging.error(f"[ReferralStorage] Ошибка сохранения: {ex}")

    def _get(self, user_id: int) -> dict:
        key = str(user_id)
        if key not in self._data:
            self._data[key] = {
                "referrer_id":     None,
                "referrals":       [],
                "ref_balance":     0.0,
                "total_earned":    0.0,
                "total_withdrawn": 0.0,
                "join_date":       datetime.now().strftime("%Y-%m-%d"),
                # joined_organically=True означает что юзер пришёл сам,
                # без реф-ссылки — он навсегда заблокирован от реф-системы
                "joined_organically": False,
            }
            self._save()
        return self._data[key]

    def mark_organic(self, user_id: int):
        """
        Вызывается из main.py когда /start пришёл БЕЗ реф-параметра.
        Если запись уже есть — ничего не меняем (юзер уже был зарегистрирован).
        Если записи нет — создаём с флагом joined_organically=True.
        """
        key = str(user_id)
        if key not in self._data:
            # Первый визит, без реф-ссылки — помечаем навсегда
            self._data[key] = {
                "referrer_id":        None,
                "referrals":          [],
                "ref_balance":        0.0,
                "total_earned":       0.0,
                "total_withdrawn":    0.0,
                "join_date":          datetime.now().strftime("%Y-%m-%d"),
                "joined_organically": True,
            }
            self._save()
            logging.info(f"[Referral] {user_id} пришёл без реф-ссылки → заблокирован от реф-системы")

    def register_referral(self, new_user_id: int, referrer_id: int) -> bool:
        # 1. Нельзя быть рефералом самого себя
        if new_user_id == referrer_id:
            logging.info(f"[Referral] {new_user_id} попытался стать рефералом самого себя")
            return False

        key = str(new_user_id)

        # 2. Если юзер уже есть в базе (пришёл раньше без реф-ссылки или уже чей-то реферал)
        if key in self._data:
            record = self._data[key]

            # Уже чей-то реферал
            if record.get("referrer_id") is not None:
                logging.info(f"[Referral] {new_user_id} уже является рефералом {record['referrer_id']}")
                return False

            # Пришёл органически (без реф-ссылки) — навсегда заблокирован
            if record.get("joined_organically", False):
                logging.info(f"[Referral] {new_user_id} пришёл органически ранее — реф-регистрация запрещена")
                return False

        # 3. Проверяем реферера
        referrer_key = str(referrer_id)
        if referrer_key not in self._data:
            # Реферера вообще нет в базе — невалидная ссылка
            logging.info(f"[Referral] Реферер {referrer_id} не найден в базе")
            return False

        referrer_record = self._data[referrer_key]

        # 4. Защита от дублей в списке рефералов реферера
        if new_user_id in referrer_record["referrals"]:
            logging.info(f"[Referral] {new_user_id} уже в списке рефералов {referrer_id}")
            return False

        # 5. Регистрируем
        record = self._get(new_user_id)
        record["referrer_id"]       = referrer_id
        record["joined_organically"] = False
        referrer_record["referrals"].append(new_user_id)
        self._save()
        logging.info(f"[Referral] {new_user_id} → реферал {referrer_id} ✅")
        return True

    def accrue_commission(self, referral_user_id: int, bet_amount: float) -> float:
        record = self._get(referral_user_id)
        referrer_id = record["referrer_id"]
        if referrer_id is None:
            return 0.0
        commission = round(bet_amount * REFERRAL_PERCENT / 100, 4)
        ref_record = self._get(referrer_id)
        ref_record["ref_balance"]  = round(ref_record["ref_balance"]  + commission, 4)
        ref_record["total_earned"] = round(ref_record["total_earned"] + commission, 4)
        self._save()
        logging.info(f"[Referral] +{commission} USDT → {referrer_id} (ставка {referral_user_id})")
        return commission

    def get_ref_balance(self, user_id: int) -> float:
        return self._get(user_id)["ref_balance"]

    def get_stats(self, user_id: int) -> dict:
        r = self._get(user_id)
        return {
            "referrals_count": len(r["referrals"]),
            "referrals_list":  r["referrals"],
            "ref_balance":     r["ref_balance"],
            "total_earned":    r["total_earned"],
            "total_withdrawn": r["total_withdrawn"],
        }

    def withdraw_ref_balance(self, user_id: int, amount: float) -> bool:
        record = self._get(user_id)
        if record["ref_balance"] < amount:
            return False
        record["ref_balance"]     = round(record["ref_balance"]     - amount, 4)
        record["total_withdrawn"] = round(record["total_withdrawn"] + amount, 4)
        self._save()
        return True

    def get_referrer_id(self, user_id: int) -> int | None:
        return self._get(user_id)["referrer_id"]


# ──────────────────────────────────────────────
#  ГЛОБАЛЬНЫЙ ЭКЗЕМПЛЯР
# ──────────────────────────────────────────────
referral_storage = ReferralStorage()
_bot: Bot | None = None


def setup_referrals(bot: Bot):
    global _bot
    _bot = bot


# ──────────────────────────────────────────────
#  УТИЛИТЫ
# ──────────────────────────────────────────────
def get_referral_link(user_id: int) -> str:
    bot_username = os.getenv("BOT_USERNAME", "YourBotUsername")
    return f"https://t.me/{bot_username}?start=ref_{user_id}"


def e(eid: str, fallback: str = "•") -> str:
    return f'<tg-emoji emoji-id="{eid}">{fallback}</tg-emoji>'


# ──────────────────────────────────────────────
#  КЛАВИАТУРЫ
# ──────────────────────────────────────────────
def kb_referrals_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Статистика",
                callback_data="ref_stats",
                icon_custom_emoji_id=EMOJI_STATS
            ),
            InlineKeyboardButton(
                text="Вывести",
                callback_data="ref_withdraw",
                icon_custom_emoji_id=EMOJI_WALLET
            ),
        ],
        [
            InlineKeyboardButton(
                text="Моя ссылка",
                callback_data="ref_link",
                icon_custom_emoji_id=EMOJI_NUMBER
            ),
        ],
        [
            InlineKeyboardButton(
                text="Назад",
                callback_data="back_to_main",
                icon_custom_emoji_id=EMOJI_BACK
            ),
        ],
    ])


def kb_ref_back() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="Назад",
            callback_data="referrals",
            icon_custom_emoji_id=EMOJI_BACK
        )
    ]])


def kb_ref_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="Отмена",
            callback_data="referrals",
            icon_custom_emoji_id=EMOJI_BACK
        )
    ]])


# ──────────────────────────────────────────────
#  ТЕКСТЫ
# ──────────────────────────────────────────────
def text_referrals_main(user_id: int) -> str:
    stats = referral_storage.get_stats(user_id)
    link  = get_referral_link(user_id)

    cnt = stats["referrals_count"]
    if 11 <= cnt % 100 <= 19:
        ref_word = "рефералов"
    elif cnt % 10 == 1:
        ref_word = "реферал"
    elif cnt % 10 in (2, 3, 4):
        ref_word = "реферала"
    else:
        ref_word = "рефералов"

    return (
        f"{e(EMOJI_PARTNERS,'🤝')} <b>Реферальная программа</b>\n\n"
        f"<blockquote>"
        f"<tg-emoji emoji-id=\"5332724926216428039\">🎰</tg-emoji><b>Приглашено:</b> <code>{cnt} {ref_word}</code>\n"
        f"<tg-emoji emoji-id=\"5278467510604160626\">🎰</tg-emoji><b>Реф-баланс:</b> <code>{stats['ref_balance']:.4f}</code> "
        f"<tg-emoji emoji-id=\"5197434882321567830\">🎰</tg-emoji>\n"
        f"<tg-emoji emoji-id=\"5427168083074628963\">🎰</tg-emoji><b>Заработано:</b> <code>{stats['total_earned']:.4f}</code> "
        f"{e(EMOJI_COIN,'💎')}\n"
        f"{e(EMOJI_WITHDRAWAL,'📤')} <b>Выведено:</b> <code>{stats['total_withdrawn']:.4f}</code> "
        f"{e(EMOJI_COIN,'💎')}\n"
        f"</blockquote>\n\n"
        f"<blockquote>"
        f"<tg-emoji emoji-id=\"5294167145079395967\">🎰</tg-emoji><b>Получайте 2% от выигрышей друзей!</b>\n"
        f"</blockquote>\n\n"
        f"<blockquote>"
        f"<tg-emoji emoji-id=\"5271604874419647061\">🎰</tg-emoji><b>Ваша ссылка:</b>\n"
        f"<code>{link}</code>"
        f"</blockquote>"
    )


def text_ref_stats(user_id: int) -> str:
    stats = referral_storage.get_stats(user_id)
    refs  = stats["referrals_list"]

    last_5 = list(reversed(refs[-5:])) if refs else []
    lines = [
        f"{e(EMOJI_REF_USER,'👤')} <code>{uid}</code>"
        for uid in last_5
    ]
    refs_block = "\n".join(lines) if lines else "  <i>Рефералов пока нет</i>"
    more = f"\n{e(EMOJI_STATS,'📊')} <i>... и ещё {len(refs) - 5}</i>" if len(refs) > 5 else ""

    return (
        f"{e(EMOJI_STATS,'📊')} <b>Детальная статистика</b>\n\n"
        f"<blockquote>"
        f"<tg-emoji emoji-id=\"5278467510604160626\">🎰</tg-emoji>Реф-баланс: <code>{stats['ref_balance']:.4f}</code>\n"
        f"<tg-emoji emoji-id=\"5427168083074628963\">🎰</tg-emoji>Заработано: <code>{stats['total_earned']:.4f}</code>\n"
        f"{e(EMOJI_WITHDRAWAL,'📤')}Выведено: <code>{stats['total_withdrawn']:.4f}</code>\n"
        f"<tg-emoji emoji-id=\"5332724926216428039\">🎰</tg-emoji>рефералов: <code>{stats['referrals_count']}</code>\n"
        f"</blockquote>\n\n"
        f"<blockquote>"
        f"<b>Последние рефералы:</b>\n"
        f"{refs_block}{more}"
        f"</blockquote>"
    )


def text_ref_link(user_id: int) -> str:
    link = get_referral_link(user_id)
    return (
        f"<blockquote><tg-emoji emoji-id=\"5271604874419647061\">🎰</tg-emoji><b>Реферальная ссылка</b></blockquote>\n\n"
        f"<blockquote><code>{link}</code></blockquote>"
    )


# ──────────────────────────────────────────────
#  ХЕНДЛЕРЫ
# ──────────────────────────────────────────────
referral_router = Router()


@referral_router.callback_query(F.data == "referrals")
async def referrals_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        text_referrals_main(callback.from_user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_referrals_main()
    )
    await callback.answer()


@referral_router.callback_query(F.data == "ref_stats")
async def ref_stats(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        text_ref_stats(callback.from_user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_ref_back()
    )
    await callback.answer()


@referral_router.callback_query(F.data == "ref_link")
async def ref_link(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        text_ref_link(callback.from_user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_ref_back()
    )
    await callback.answer()


@referral_router.callback_query(F.data == "ref_withdraw")
async def ref_withdraw_start(callback: CallbackQuery, state: FSMContext):
    ref_balance = referral_storage.get_ref_balance(callback.from_user.id)

    await state.set_state(ReferralWithdraw.entering_amount)
    await callback.message.edit_text(
        f"{e(EMOJI_WITHDRAWAL,'📤')} <b>Вывод реферального баланса</b>\n\n"
        f"<blockquote><i><tg-emoji emoji-id=\"5197269100878907942\">🎰</tg-emoji>Введите сумму для вывода:</i></blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_ref_cancel()
    )
    await callback.answer()


# ── Вызывается напрямую из main.py (handle_text_message) ──
async def ref_withdraw_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text.replace(",", ".").strip())
    except ValueError:
        await message.answer(
            "❌ <b>Неверный формат.</b> Введите число, например: <code>5.00</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_ref_cancel()
        )
        return

    if amount < MIN_REF_WITHDRAWAL:
        await message.answer(
            f"❌ <b>Минимальная сумма:</b> <code>{MIN_REF_WITHDRAWAL:.2f}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_ref_cancel()
        )
        return

    ref_balance = referral_storage.get_ref_balance(message.from_user.id)
    if amount > ref_balance:
        await message.answer(
            f"❌ <b>Недостаточно средств.</b>\n"
            f"Реф-баланс: <code>{ref_balance:.4f}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_ref_cancel()
        )
        return

    success = referral_storage.withdraw_ref_balance(message.from_user.id, amount)
    if not success:
        await message.answer(
            "❌ Ошибка при выводе. Попробуйте позже.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_ref_cancel()
        )
        return

    # Зачисляем на основной игровой баланс
    try:
        from payments import storage as pay_storage
        pay_storage.add_balance(message.from_user.id, amount)
        new_pay_balance = pay_storage.get_balance(message.from_user.id)
        try:
            from main import betting_game
            if betting_game:
                betting_game.user_balances[message.from_user.id] = new_pay_balance
                betting_game.save_balances()
        except Exception:
            pass
    except Exception as ex:
        logging.error(f"[Referral] Ошибка зачисления: {ex}")

    await state.clear()
    new_ref_balance = referral_storage.get_ref_balance(message.from_user.id)

    await message.answer(
        f"<tg-emoji emoji-id=\"5206607081334906820\">🎰</tg-emoji><b>Успешно выведено!</b>\n\n",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_ref_back()
    )
    logging.info(f"[Referral] {message.from_user.id} вывел {amount} USDT с реф-баланса")


@referral_router.message(ReferralWithdraw.entering_amount, F.text)
async def ref_withdraw_amount_handler(message: Message, state: FSMContext):
    await ref_withdraw_amount(message, state)


# ──────────────────────────────────────────────
#  ХЕЛПЕР: начисление комиссии — тихо, без уведомлений
# ──────────────────────────────────────────────
async def notify_referrer_commission(referral_user_id: int, bet_amount: float):
    commission = referral_storage.accrue_commission(referral_user_id, bet_amount)
    if commission > 0:
        logging.info(f"[Referral] Комиссия {commission} USDT начислена тихо рефереру")


# ──────────────────────────────────────────────
#  ХЕЛПЕР: обработка /start ref_XXXXXX
# ──────────────────────────────────────────────
async def process_start_referral(message: Message, start_param: str) -> bool:
    if not start_param.startswith("ref_"):
        return False
    try:
        referrer_id = int(start_param[4:])
    except ValueError:
        return False

    new_user_id = message.from_user.id
    registered  = referral_storage.register_referral(new_user_id, referrer_id)

    if registered and _bot is not None:
        try:
            await _bot.send_message(
                chat_id=referrer_id,
                text=(
                    f"<blockquote><tg-emoji emoji-id=\"5222079954421818267\">🎰</tg-emoji><b>Новый реферал!</b></blockquote>\n\n"
                ),
                parse_mode=ParseMode.HTML
            )
        except Exception as ex:
            logging.warning(f"[Referral] Не удалось уведомить {referrer_id}: {ex}")

    return registered
