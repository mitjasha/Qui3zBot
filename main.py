import asyncio
import os
from dataclasses import dataclass
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties

from db import (
    init_db, set_topic, get_topic,
    set_state, get_state,
    upsert_user,
    add_points_alltime, add_points_session, add_points_event,
    top_alltime, top_session, top_period,
    create_session, end_session
)
from quiz import QuizEngine, now_ts
from textnorm import normalize
import math
import random

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
QUESTIONS_PATH = "questions"

# Timing
QUESTION_TTL_SEC = 25
PAUSE_BETWEEN_SEC = 4

# Scoring with hints
MAX_POINTS = 5
MIN_POINTS = 1

TZ = ZoneInfo("Europe/Berlin")

bot = Bot(
    BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

quiz = QuizEngine(QUESTIONS_PATH)
current_question = None  # in-memory current question dict

# pending setup for /quiz_start menu
pending_setup: dict[tuple[int, int], dict] = {}

@dataclass
class HintPlan:
    hint_total: int
    interval_sec: int

def allowed_topic_from_message(message: Message, allowed_chat_id: int, allowed_thread_id: int) -> bool:
    thread_id = getattr(message, "message_thread_id", None)
    return message.chat.id == allowed_chat_id and thread_id == allowed_thread_id

def allowed_topic_from_callback(cb: CallbackQuery, allowed_chat_id: int, allowed_thread_id: int) -> bool:
    if not cb.message:
        return False
    thread_id = getattr(cb.message, "message_thread_id", None)
    return cb.message.chat.id == allowed_chat_id and thread_id == allowed_thread_id

async def ensure_topic_or_hint(message: Message) -> bool:
    allowed_chat_id, allowed_thread_id = await get_topic()
    if not allowed_chat_id or not allowed_thread_id:
        await message.reply("Сначала привяжи меня к теме: открой нужную тему и отправь <b>/set_topic</b>")
        return False
    if getattr(message, "message_thread_id", None) is None:
        return False
    if not allowed_topic_from_message(message, allowed_chat_id, allowed_thread_id):
        return False
    return True

def build_tags_kb(tags: list[str]):
    kb = InlineKeyboardBuilder()
    kb.button(text="all", callback_data="qs_tag:all")
    for t in tags:
        kb.button(text=t, callback_data=f"qs_tag:{t}")
    kb.adjust(3)
    return kb.as_markup()

def build_categories_kb(categories: list[str]):
    kb = InlineKeyboardBuilder()
    for c in categories:
        kb.button(text=c, callback_data=f"qs_cat:{c}")
    kb.adjust(2)  # 2 кнопки в ряд, чтобы не было каши
    return kb.as_markup()

def build_rounds_kb():
    rounds = [5, 10, 15, 20, 30, 50]
    kb = InlineKeyboardBuilder()
    for r in rounds:
        kb.button(text=str(r), callback_data=f"qs_round:{r}")
    kb.button(text="Отмена", callback_data="qs_cancel")
    kb.adjust(3)
    return kb.as_markup()

def choose_hint_plan(answer_display: str) -> HintPlan:
    # Hint count depends on length (letters/digits only). More length => more hints, but capped.
    letters = [ch for ch in answer_display if ch.isalnum()]
    n = len(letters)

    if n <= 2:
        hint_total = 0
    elif n <= 4:
        hint_total = 1
    elif n <= 7:
        hint_total = 2
    elif n <= 10:
        hint_total = 3
    else:
        hint_total = 4

    # Interval: spread hints across TTL (leave some time for final attempts)
    # If no hints -> interval irrelevant
    if hint_total <= 0:
        return HintPlan(hint_total=0, interval_sec=9999)

    # reserve last ~7 seconds without new hints
    available = max(5, QUESTION_TTL_SEC - 7)
    interval = max(4, available // (hint_total + 1))
    return HintPlan(hint_total=hint_total, interval_sec=interval)

def make_hint_random(answer_display: str, reveal_positions: set[int]) -> str:
    # spaces and hyphens are shown as-is
    out = []
    for idx, ch in enumerate(answer_display):
        if ch == " " or ch == "-":
            out.append(ch)
            continue

        if ch.isalnum():
            out.append(ch if idx in reveal_positions else "_")
        else:
            # other punctuation: keep as-is (or hide if you prefer)
            out.append(ch)
    return "".join(out)

def shuffled_alnum_positions(answer_display: str, seed: int) -> list[int]:
    # positions of letters/digits ONLY (spaces/hyphens excluded)
    pos = [i for i, ch in enumerate(answer_display) if ch.isalnum()]
    rnd = random.Random(seed)  # deterministic "random"
    rnd.shuffle(pos)
    return pos

def points_for_hint_level(hint_level: int) -> int:
    return max(MIN_POINTS, MAX_POINTS - hint_level)

def format_rows(title: str, rows):
    if not rows:
        return f"{title}\nПока пусто"
    lines = [title]
    for i, (uid, pts, full_name, username) in enumerate(rows, start=1):
        label = full_name or f"User {uid}"
        if username:
            label = f"{label} (@{username})"
        lines.append(f"{i}) {label} — <b>{int(pts)}</b>")
    return "\n".join(lines)

def day_range_ts(now: datetime):
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp())

def week_range_ts(now: datetime):
    # ISO week: Monday 00:00 to next Monday 00:00
    start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=now.weekday())
    end = start + timedelta(days=7)
    return int(start.timestamp()), int(end.timestamp())

async def start_quiz(chat_id: int, thread_id: int, tag: str, rounds: int):
    session_id = await create_session(chat_id, thread_id, tag, rounds)
    await set_state(
        active=1,
        current_qid=None,
        winner_user_id=None,
        deadline_ts=None,
        tag=tag,
        round_total=rounds,
        round_current=0,
        session_id=session_id,
        hint_level=0,
        hint_total=0,
        hint_answer=None,
        next_hint_ts=None
    )
    await post_next_question(chat_id, thread_id)

async def stop_quiz():
    st = await get_state()
    if st["session_id"]:
        await end_session(st["session_id"])
    await set_state(
        active=0,
        current_qid=None,
        winner_user_id=None,
        deadline_ts=None,
        tag="all",
        round_total=None,
        round_current=0,
        session_id=None,
        hint_level=0,
        hint_total=0,
        hint_answer=None,
        next_hint_ts=None
    )

async def post_next_question(chat_id: int, thread_id: int):
    global current_question

    st = await get_state()
    category = st.get("category")
    total = st["round_total"]
    cur = st["round_current"] or 0
    session_id = st["session_id"]

    if total is not None and cur >= total:
        # end quiz + show session rating
        await set_state(active=0, deadline_ts=None, next_hint_ts=None)
        if session_id:
            await end_session(session_id)
            rows = await top_session(session_id, chat_id_for_names=chat_id, limit=10)
            msg = format_rows("🏁 <b>Игра завершена</b>\n🏆 <b>Рейтинг за игру</b>", rows)
            msg += "\n\nТакже доступно: /rating /rating_day /rating_week"
            await bot.send_message(chat_id, msg, message_thread_id=thread_id)
        else:
            await bot.send_message(chat_id, "🏁 Игра завершена", message_thread_id=thread_id)
        return

    q = quiz.next_question(category=category) if category else quiz.next_question(tag="all")
    current_question = q

    cur += 1
    deadline = now_ts() + QUESTION_TTL_SEC

    # Hints based on the "display answer" (first answer)
    answer_display = (q.get("answers") or [""])[0]
    plan = choose_hint_plan(answer_display)
    next_hint_ts = None
    if plan.hint_total > 0:
        next_hint_ts = now_ts() + plan.interval_sec

    await set_state(
        active=1,
        current_qid=q["id"],
        winner_user_id=None,
        deadline_ts=deadline,
        round_current=cur,
        hint_level=0,
        hint_total=plan.hint_total,
        hint_answer=answer_display,
        next_hint_ts=next_hint_ts
    )

    header = f"❓ <b>Вопрос {cur}"
    if total is not None:
        header += f"/{total}"
    header += "</b>"

    await bot.send_message(
        chat_id,
        f"{header}\n{q['question']}\n\n📚 <b>{category}</b> | ⏳ <b>{QUESTION_TTL_SEC}</b> сек\n"
        f"💎 Очки: <b>{MAX_POINTS}</b> без подсказок (уменьшаются с каждой подсказкой)",
        message_thread_id=thread_id
    )

async def hint_watcher():
    while True:
        st = await get_state()
        allowed_chat_id, allowed_thread_id = await get_topic()

        if st["active"] and allowed_chat_id and allowed_thread_id:
            # only if no winner yet
            if st["winner_user_id"] is None and st["hint_total"] and st["hint_level"] < st["hint_total"]:
                nht = st["next_hint_ts"]
                if nht and now_ts() >= nht and st["hint_answer"]:
                    # reveal more letters each hint: spread across total hints
                    hint_level_next = st["hint_level"] + 1
                    answer_display = st["hint_answer"]

                    # reveal percent of length, but in RANDOM positions (deterministic shuffle by question id)
                    total_positions = shuffled_alnum_positions(answer_display, seed=st["current_qid"] or 0)
                    n = len(total_positions)

                    # percent grows with hint level: level/(hint_total+1)
                    frac = hint_level_next / (st["hint_total"] + 1)
                    k = max(1, math.ceil(n * frac))  # how many letters to show now

                    reveal_positions = set(total_positions[:k])
                    hint_text = make_hint_random(answer_display, reveal_positions=reveal_positions)

                    pts_now = points_for_hint_level(hint_level_next)

                    await bot.send_message(
                        allowed_chat_id,
                        f"💡 <b>Подсказка {hint_level_next}/{st['hint_total']}</b>\n"
                        f"<code>{hint_text}</code>\n"
                        f"💎 Очки сейчас: <b>{pts_now}</b>",
                        message_thread_id=allowed_thread_id
                    )

                    # schedule next hint
                    # use same interval logic as in choose_hint_plan (recompute)
                    plan = choose_hint_plan(answer_display)
                    next_hint_ts = None
                    if hint_level_next < st["hint_total"]:
                        next_hint_ts = now_ts() + plan.interval_sec

                    await set_state(
                        hint_level=hint_level_next,
                        next_hint_ts=next_hint_ts
                    )

        await asyncio.sleep(1)

async def timeout_watcher():
    while True:
        st = await get_state()
        allowed_chat_id, allowed_thread_id = await get_topic()

        if st["active"] and allowed_chat_id and allowed_thread_id:
            if st["deadline_ts"] and now_ts() >= st["deadline_ts"]:
                if st["winner_user_id"] is None:
                    ans = st["hint_answer"] or ((current_question.get("answers") or ["—"])[0] if current_question else "—")
                    await bot.send_message(
                        allowed_chat_id,
                        f"⌛ Время вышло!\n✅ Правильный ответ: <b>{ans}</b>",
                        message_thread_id=allowed_thread_id
                    )
                    await asyncio.sleep(PAUSE_BETWEEN_SEC)
                    await post_next_question(allowed_chat_id, allowed_thread_id)

        await asyncio.sleep(1)

@dp.message(Command("set_topic"))
async def cmd_set_topic(message: Message):
    thread_id = getattr(message, "message_thread_id", None)
    if thread_id is None:
        await message.reply("Эта команда должна быть отправлена <b>внутри темы</b> (Topics).")
        return
    await set_topic(message.chat.id, thread_id)
    await message.reply("✅ Привязал бота к этой теме. Теперь команды и игра работают только здесь.")

@dp.message(Command("tags"))
async def cmd_tags(message: Message):
    ok = await ensure_topic_or_hint(message)
    if not ok:
        return
    tags = ["all"] + quiz.list_tags()
    await message.reply("📚 Категории: " + ", ".join([f"<code>{t}</code>" for t in tags]))

@dp.message(Command("quiz_start"))
async def cmd_quiz_start(message: Message):
    ok = await ensure_topic_or_hint(message)
    if not ok:
        return

    st = await get_state()
    if st["active"]:
        await message.reply("Квиз уже запущен. Используй /quiz_stop чтобы остановить.")
        return

    parts = (message.text or "").split()
    thread_id = message.message_thread_id

    # No params -> inline menu
    if len(parts) == 1:
        categories = quiz.list_categories()
        pending_setup[(message.chat.id, thread_id)] = {"category": categories[0] if categories else None}
        await message.reply(
            "Выбери <b>категорию</b> для квиза:",
            reply_markup=build_categories_kb(categories),
        )
        return


    # /quiz_start <tag> <rounds>
    tag = "all"
    rounds = 10
    if len(parts) >= 2:
        tag = parts[1].strip()
    if len(parts) >= 3:
        try:
            rounds = int(parts[2])
        except ValueError:
            rounds = 10

    allowed_set = set(["all"] + quiz.list_tags())
    if tag not in allowed_set:
        await message.reply("Не знаю такую категорию. Посмотри /tags или запусти /quiz_start без параметров.")
        return

    rounds = max(1, min(rounds, 50))

    await message.reply(
        f"🎮 <b>Квиз запущен!</b>\n📚 <b>{tag}</b> | 🔢 Раундов: <b>{rounds}</b>\n"
        f"✍️ Отвечайте текстом. Первый правильный получает очки."
    )
    await start_quiz(message.chat.id, thread_id, tag, rounds)

@dp.callback_query(F.data.startswith("qs_tag:"))
async def cb_choose_tag(cb: CallbackQuery):
    allowed_chat_id, allowed_thread_id = await get_topic()
    if not allowed_chat_id or not allowed_thread_id:
        await cb.answer()
        return
    if not allowed_topic_from_callback(cb, allowed_chat_id, allowed_thread_id):
        await cb.answer()
        return

    tag = cb.data.split(":", 1)[1]
    key = (cb.message.chat.id, cb.message.message_thread_id)
    pending_setup.setdefault(key, {})["tag"] = tag

    await cb.message.edit_text(
        f"Категория: <b>{tag}</b>\nТеперь выбери <b>количество раундов</b>:",
        reply_markup=build_rounds_kb()
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("qs_round:"))
async def cb_choose_round(cb: CallbackQuery):
    allowed_chat_id, allowed_thread_id = await get_topic()
    if not allowed_chat_id or not allowed_thread_id:
        await cb.answer()
        return
    if not allowed_topic_from_callback(cb, allowed_chat_id, allowed_thread_id):
        await cb.answer()
        return

    st = await get_state()
    if st["active"]:
        await cb.answer("Квиз уже запущен", show_alert=True)
        return

    try:
        rounds = int(cb.data.split(":", 1)[1])
    except ValueError:
        rounds = 10
    rounds = max(1, min(rounds, 50))

    key = (cb.message.chat.id, cb.message.message_thread_id)
    category = pending_setup.get(key, {}).get("category")

    if not category:
        category = "Общие знания"  # fallback, если вдруг

    await cb.message.edit_text(
        f"🎮 <b>Квиз запущен!</b>\n"
        f"📚 Категория: <b>{category}</b>\n"
        f"🔢 Раундов: <b>{rounds}</b>\n"
        f"✍️ Отвечайте текстом. Первый правильный получает очки."
    )

    pending_setup.pop(key, None)

    # ВАЖНО: сохраняем category в state, а не tag
    await set_state(
        active=True,
        current_qid=None,
        winner_user_id=None,
        deadline_ts=None,
        tag=None,                 # можно оставить, но не используем
        round_total=rounds,
        round_current=0,
        session_id=await create_session(cb.message.chat.id, cb.message.message_thread_id, category, rounds),
        hint_level=0,
        hint_total=0,
        hint_answer=None,
        next_hint_ts=None,
        category=category         # <-- добавим новое поле (см. пункт 5)
    )

    await post_next_question(cb.message.chat.id, cb.message.message_thread_id)
    await cb.answer()


@dp.callback_query(F.data == "qs_cancel")
async def cb_cancel(cb: CallbackQuery):
    allowed_chat_id, allowed_thread_id = await get_topic()
    if not allowed_chat_id or not allowed_thread_id:
        await cb.answer()
        return
    if not allowed_topic_from_callback(cb, allowed_chat_id, allowed_thread_id):
        await cb.answer()
        return
    key = (cb.message.chat.id, cb.message.message_thread_id)
    pending_setup.pop(key, None)
    await cb.message.edit_text("❌ Запуск квиза отменён.")
    await cb.answer()

@dp.message(Command("quiz_stop"))
async def cmd_quiz_stop(message: Message):
    ok = await ensure_topic_or_hint(message)
    if not ok:
        return

    # print session rating before stop (optional)
    st = await get_state()
    session_id = st["session_id"]
    await stop_quiz()

    if session_id:
        rows = await top_session(session_id, chat_id_for_names=message.chat.id, limit=10)
        msg = format_rows("🛑 <b>Квиз остановлен</b>\n🏆 <b>Рейтинг за игру</b>", rows)
        await message.reply(msg)
    else:
        await message.reply("🛑 Квиз остановлен.")

@dp.message(Command("skip"))
async def cmd_skip(message: Message):
    ok = await ensure_topic_or_hint(message)
    if not ok:
        return
    st = await get_state()
    if not st["active"]:
        await message.reply("Квиз не запущен.")
        return
    await message.reply("⏭️ Пропускаю вопрос.")
    await asyncio.sleep(1)
    await post_next_question(message.chat.id, message.message_thread_id)

@dp.message(Command("rating"))
async def cmd_rating_all(message: Message):
    ok = await ensure_topic_or_hint(message)
    if not ok:
        return
    rows = await top_alltime(message.chat.id, message.message_thread_id, limit=10)
    await message.reply(format_rows("🏆 <b>Рейтинг общий</b>", rows))

@dp.message(Command("rating_game"))
async def cmd_rating_game(message: Message):
    ok = await ensure_topic_or_hint(message)
    if not ok:
        return
    st = await get_state()
    if not st["session_id"]:
        await message.reply("Сейчас нет активной игры. Рейтинг за игру появится во время/после игры.")
        return
    rows = await top_session(st["session_id"], chat_id_for_names=message.chat.id, limit=10)
    await message.reply(format_rows("🏆 <b>Рейтинг за игру</b>", rows))

@dp.message(Command("rating_day"))
async def cmd_rating_day(message: Message):
    ok = await ensure_topic_or_hint(message)
    if not ok:
        return
    now = datetime.now(TZ)
    ts_from, ts_to = day_range_ts(now)
    rows = await top_period(message.chat.id, message.message_thread_id, ts_from, ts_to, limit=10)
    await message.reply(format_rows("📅 <b>Рейтинг за день</b>", rows))

@dp.message(Command("rating_week"))
async def cmd_rating_week(message: Message):
    ok = await ensure_topic_or_hint(message)
    if not ok:
        return
    now = datetime.now(TZ)
    ts_from, ts_to = week_range_ts(now)
    rows = await top_period(message.chat.id, message.message_thread_id, ts_from, ts_to, limit=10)
    await message.reply(format_rows("🗓️ <b>Рейтинг за неделю</b>", rows))

@dp.message(Command("my"))
async def cmd_my(message: Message):
    ok = await ensure_topic_or_hint(message)
    if not ok:
        return
    # show session points (if active)
    st = await get_state()
    if st["session_id"]:
        # compute from session top quickly by querying event table would be heavier; keep simple:
        # show overall points via /rating for now; session points are visible in game leaderboard
        await message.reply("Посмотри /rating_game для очков в текущей игре или /rating для общего рейтинга")
    else:
        await message.reply("Нет активной игры. Используй /quiz_start")

@dp.callback_query(F.data.startswith("qs_cat:"))
async def cb_choose_category(cb: CallbackQuery):
    allowed_chat_id, allowed_thread_id = await get_topic()
    if not allowed_chat_id or not allowed_thread_id:
        await cb.answer()
        return
    if not allowed_topic_from_callback(cb, allowed_chat_id, allowed_thread_id):
        await cb.answer()
        return

    category = cb.data.split(":", 1)[1]
    key = (cb.message.chat.id, cb.message.message_thread_id)
    pending_setup.setdefault(key, {})["category"] = category

    await cb.message.edit_text(
        f"Категория выбрана: <b>{category}</b>\nТеперь выбери <b>количество раундов</b>:",
        reply_markup=build_rounds_kb(),
    )
    await cb.answer()

@dp.message(F.text)
async def on_text_answer(message: Message):
    ok = await ensure_topic_or_hint(message)
    if not ok:
        return

    await upsert_user(message.chat.id, message.from_user.id, message.from_user.full_name, message.from_user.username)

    st = await get_state()
    if not st["active"]:
        return
    if st["winner_user_id"] is not None:
        return

    global current_question
    q = current_question
    if not q:
        return

    # ignore very short spam
    if len(normalize(message.text)) < 1:
        return

    if quiz.check_answer(q, message.text):
        hint_level = st["hint_level"] or 0
        pts = points_for_hint_level(hint_level)
        ans = (q.get("answers") or ["—"])[0]
        name = message.from_user.full_name
        session_id = st["session_id"] or 0

        # lock winner + stop hints/timer
        await set_state(winner_user_id=message.from_user.id, deadline_ts=None, next_hint_ts=None)

        # add points everywhere
        await add_points_alltime(message.chat.id, message.message_thread_id, message.from_user.id, pts)
        if session_id:
            await add_points_session(session_id, message.from_user.id, pts)
        await add_points_event(now_ts(), message.chat.id, message.message_thread_id, session_id, message.from_user.id, pts, "correct")

        await message.reply(
            f"✅ <b>{name}</b> ответил(а) первым!\n"
            f"💎 Очки: <b>+{pts}</b> (подсказок: <b>{hint_level}</b>)\n"
            f"🎯 Ответ: <b>{ans}</b>"
        )

        await asyncio.sleep(PAUSE_BETWEEN_SEC)
        await post_next_question(message.chat.id, message.message_thread_id)

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN in .env")

    await init_db()
    asyncio.create_task(hint_watcher())
    asyncio.create_task(timeout_watcher())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
