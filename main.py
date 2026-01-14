# main.py
import os
import random
import time
import html
import re
import threading
import json
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Optional, Set, Dict, Tuple

from http.server import BaseHTTPRequestHandler, HTTPServer

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaDocument,
)
from telegram.constants import ParseMode
from telegram.error import RetryAfter, BadRequest, TimedOut, NetworkError
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from licensing import LicenseManager, LicenseConfig


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("drivetest-bot")


# =========================================================
# SMALL HELPERS
# =========================================================

def _chat_id_from_update(update: Update) -> Optional[int]:
    """Безопасно добываем chat_id из любого апдейта."""
    try:
        if update.effective_chat:
            return update.effective_chat.id
    except Exception:
        pass
    try:
        if update.callback_query and update.callback_query.message:
            return update.callback_query.message.chat_id
    except Exception:
        pass
    try:
        if update.message:
            return update.message.chat_id
    except Exception:
        pass
    return None


def kst_today_str() -> str:
    """
    День по Корее (KST). Нам важно именно 'сегодня' по Корее,
    чтобы бесплатные 30 вопросов обновлялись в 00:00 KST.
    KST = UTC+9 без DST.
    """
    # избегаем внешних зависимостей (ZoneInfo) и берём стабильный UTC+9
    now_utc = datetime.now(timezone.utc)
    kst = now_utc.timestamp() + 9 * 3600
    return time.strftime("%Y-%m-%d", time.gmtime(kst))


@asynccontextmanager
async def timed(label: str, **kv):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt = (time.perf_counter() - t0) * 1000
        extra = " ".join([f"{k}={v}" for k, v in kv.items()])
        log.info("TIMER %s %.1fms %s", label, dt, extra)


async def to_thread_timeout(func, *, timeout: float = 12.0, label: str = "to_thread"):
    async with timed(label):
        return await asyncio.wait_for(asyncio.to_thread(func), timeout=timeout)


# =========================================================
# CONFIG
# =========================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

CACHE_TTL = int(os.environ.get("CACHE_TTL", "300"))

SHEET_ID = os.environ.get("SHEET_ID")
if not SHEET_ID:
    raise RuntimeError("SHEET_ID is not set")

SHEET_NAME = os.environ.get("SHEET_NAME", "Sheet1")
SHEET_TAB = os.environ.get("SHEET_TAB", "").strip()

SHEET_RANGE = os.environ.get("SHEET_RANGE", "").strip()
SHEET_RANGE_FALLBACK = os.environ.get("SHEET_RANGE_FALLBACK", "").strip()

IMAGE_CACHE_SHEET = os.environ.get("IMAGE_CACHE_SHEET", "image_cache")
TRIAL_SHEET = os.environ.get("TRIAL_SHEET", "trial")
KEYS_SHEET = os.environ.get("KEYS_SHEET", "keys")
SUBS_SHEET = os.environ.get("SUBS_SHEET", "subs")

ADMIN_IDS: Set[int] = set()
_raw_admins = os.environ.get("ADMIN_IDS", "")
for x in _raw_admins.split(","):
    x = x.strip()
    if x.isdigit():
        ADMIN_IDS.add(int(x))

CAPTION_SAFE_LIMIT = 950  # держим запас под HTML/эмодзи

# Если 1 — не пытаемся держать картинку+текст в одном media-сообщении,
# когда caption слишком длинный — делаем "картинка отдельно".
SINGLE_SCREEN = os.environ.get("SINGLE_SCREEN", "1").strip() == "1"

PRICE_WEEK = "₩30 000"
PRICE_MONTH = "₩100 000"

PRICING_BLOCK = (
    "💳 <b>Полный доступ</b>\n"
    f"• {PRICE_WEEK} — 1 неделя\n"
    f"• {PRICE_MONTH} — 1 месяц\n"
    "⏱ Обычно хватает 2–4 недель обучения, чтобы уверенно пройти экзамен.\n"
)

TRIAL_QUESTIONS = 30  # бесплатный набор вопросов


def _is_admin(update: Update) -> bool:
    if not ADMIN_IDS:
        return False
    return bool(update.effective_user) and update.effective_user.id in ADMIN_IDS


# =========================================================
# DAILY TRIAL STATE (30 free questions per KST day)
# =========================================================
# Важно: это хранится в памяти процесса.
# Если сервер перезапустится — состояние может обнулиться.
# (Мы не трогаем licensing.py / схему trial-таблицы, чтобы не ломать остальное.)
DAILY_TRIAL_POOL: Dict[Tuple[int, str], List[dict]] = {}
DAILY_TRIAL_EXHAUSTED: Dict[Tuple[int, str], bool] = {}


# =========================================================
# HTTP server (Render keep-alive)
# =========================================================

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()


def run_http():
    port = int(os.environ.get("PORT", "10000"))
    server = HTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()


# =========================================================
# TELEGRAM RETRY HELPERS
# =========================================================

def _is_doc_as_photo(err: Exception) -> bool:
    s = str(err).lower()
    return ("document as photo" in s) or ("type document" in s) or ("can't use file of type document" in s)


async def tg_retry(
    call,
    tries: int = 5,
    base_sleep: float = 0.6,
    label: str = "",
    no_retry_badrequest: bool = False
):
    """
    Важно: BadRequest ловим раньше NetworkError.
    Для 'document as photo' делаем fast-fail (без ретраев).
    """
    last_exc: Optional[Exception] = None
    for attempt in range(tries):
        try:
            return await call()

        except RetryAfter as e:
            wait = float(getattr(e, "retry_after", 1.0)) + 0.25
            log.warning("RetryAfter sleep=%.2fs %s", wait, label)
            await asyncio.sleep(wait)
            last_exc = e

        except BadRequest as e:
            if _is_doc_as_photo(e):
                raise
            if no_retry_badrequest:
                raise
            if attempt == 0:
                await asyncio.sleep(0.15)
                last_exc = e
                continue
            raise

        except (TimedOut, NetworkError) as e:
            sleep = min(base_sleep * (2 ** attempt) + random.random() * 0.25, 6.0)
            log.warning("Net/Timeout sleep=%.2fs attempt=%d %s err=%r", sleep, attempt + 1, label, e)
            await asyncio.sleep(sleep)
            last_exc = e

        except Exception as e:
            last_exc = e
            sleep = min(base_sleep * (2 ** attempt) + random.random() * 0.25, 6.0)
            log.warning("Exception sleep=%.2fs attempt=%d %s err=%r", sleep, attempt + 1, label, e)
            await asyncio.sleep(sleep)

    raise last_exc  # type: ignore[misc]


async def safe_answer(query, text: Optional[str] = None, show_alert: bool = False):
    """Не даём падать на 'Query is too old' / double-answer."""
    if not query:
        return
    try:
        await query.answer(text=text, show_alert=show_alert)
    except BadRequest:
        pass
    except Exception:
        pass


# =========================================================
# TELEGRAM MEDIA HELPERS (photo vs document) + safe edit
# =========================================================

async def tg_send_media(
    *,
    bot,
    chat_id: int,
    file_id: str,
    caption: str = "",
    parse_mode: str = ParseMode.HTML,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
):
    """
    Пытаемся отправить как PHOTO.
    Если file_id от DOCUMENT -> отправляем как DOCUMENT.
    Возвращает (message, kind) где kind in {"photo","document"}.
    """
    async def _send_photo():
        return await bot.send_photo(
            chat_id=chat_id,
            photo=file_id,
            caption=caption,
            parse_mode=parse_mode,
            reply_markup=reply_markup
        )

    async def _send_doc():
        return await bot.send_document(
            chat_id=chat_id,
            document=file_id,
            caption=caption,
            parse_mode=parse_mode,
            reply_markup=reply_markup
        )

    try:
        msg = await tg_retry(_send_photo, label="send_photo", no_retry_badrequest=True)
        log.info("MEDIA send=photo chat=%s file_id=%s", chat_id, file_id)
        return msg, "photo"
    except BadRequest as e:
        if _is_doc_as_photo(e):
            msg = await tg_retry(_send_doc, label="send_document", no_retry_badrequest=True)
            log.info("MEDIA send=document chat=%s file_id=%s", chat_id, file_id)
            return msg, "document"
        raise


async def tg_edit_media_with_fallback(
    *,
    bot,
    chat_id: int,
    message_id: int,
    file_id: str,
    caption: str,
    reply_markup: InlineKeyboardMarkup,
):
    """
    Пытаемся edit_message_media:
      1) photo (fast-fail если doc-as-photo)
      2) document
    Если нельзя edit — delete + resend.
    Возвращает (new_message_id, kind, did_resend: bool)
    """
    async def _edit_photo():
        media = InputMediaPhoto(media=file_id, caption=caption, parse_mode=ParseMode.HTML)
        return await bot.edit_message_media(
            chat_id=chat_id,
            message_id=message_id,
            media=media,
            reply_markup=reply_markup
        )

    async def _edit_doc():
        media = InputMediaDocument(media=file_id, caption=caption, parse_mode=ParseMode.HTML)
        return await bot.edit_message_media(
            chat_id=chat_id,
            message_id=message_id,
            media=media,
            reply_markup=reply_markup
        )

    async def _delete():
        return await bot.delete_message(chat_id=chat_id, message_id=message_id)

    try:
        await tg_retry(_edit_photo, label="edit_media_photo", no_retry_badrequest=True)
        log.info("MEDIA edit=photo chat=%s msg=%s file_id=%s", chat_id, message_id, file_id)
        return message_id, "photo", False
    except BadRequest as e1:
        if not _is_doc_as_photo(e1):
            log.warning("edit_photo failed (not doc-as-photo) msg=%s err=%r", message_id, e1)
        else:
            log.info("edit_photo -> doc fallback msg=%s", message_id)

    try:
        await tg_retry(_edit_doc, label="edit_media_document", no_retry_badrequest=True)
        log.info("MEDIA edit=document chat=%s msg=%s file_id=%s", chat_id, message_id, file_id)
        return message_id, "document", False
    except BadRequest as e2:
        log.warning("edit_document failed msg=%s err=%r", message_id, e2)

    # delete + resend
    try:
        await tg_retry(_delete, label="delete_message")
    except Exception:
        pass

    msg, kind = await tg_send_media(
        bot=bot,
        chat_id=chat_id,
        file_id=file_id,
        caption=caption,
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup,
    )
    return msg.message_id, kind, True


# =========================================================
# GOOGLE SHEETS SERVICE + BACKOFF (SYNC; используем через to_thread)
# =========================================================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_SHEETS_SERVICE = None


def get_sheets_service():
    global _SHEETS_SERVICE
    if _SHEETS_SERVICE is not None:
        return _SHEETS_SERVICE

    sa_raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not set")

    sa_json = json.loads(sa_raw)
    creds = service_account.Credentials.from_service_account_info(sa_json, scopes=SCOPES)
    _SHEETS_SERVICE = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _SHEETS_SERVICE


def gs_execute(req, tries: int = 7, base_sleep: float = 1.0):
    last_exc = None
    for attempt in range(tries):
        try:
            return req.execute()
        except HttpError as e:
            last_exc = e
            status = getattr(e.resp, "status", None)
            if status in (429, 503):
                sleep_s = min(base_sleep * (2 ** attempt) + random.random(), 20.0)
                time.sleep(sleep_s)
                continue
            raise
    if last_exc:
        return req.execute()
    return req.execute()


def _fetch_range(range_a1: str) -> List[List[str]]:
    service = get_sheets_service()
    resp = gs_execute(service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=range_a1
    ))
    return resp.get("values", [])


def _header_has_anchors(values: List[List[str]]) -> bool:
    if not values or not values[0]:
        return False
    header = [str(h).strip() for h in values[0]]
    return ("Anchors_EN" in header) and ("Correct_Anchors_EN" in header)


def _get_spreadsheet_sheet_titles() -> List[str]:
    service = get_sheets_service()
    meta = gs_execute(service.spreadsheets().get(spreadsheetId=SHEET_ID))
    sheets = meta.get("sheets", []) or []
    titles = []
    for sh in sheets:
        props = sh.get("properties", {}) or {}
        t = props.get("title")
        if t:
            titles.append(str(t))
    return titles


def _pick_sheet_tab(titles: List[str]) -> str:
    if SHEET_TAB and SHEET_TAB in titles:
        return SHEET_TAB
    for t in titles:
        if t.strip() == SHEET_NAME.strip():
            return t
    return titles[0]


def _a1(tab: str, rng: str) -> str:
    safe = (tab or "").replace("'", "''")
    return f"'{safe}'!{rng}"


def load_rows_from_sheets() -> List[List[str]]:
    titles = _get_spreadsheet_sheet_titles()
    if not titles:
        raise RuntimeError("No sheets found in spreadsheet")

    tab = _pick_sheet_tab(titles)

    candidates = []
    if SHEET_RANGE:
        candidates.append(SHEET_RANGE)
    if SHEET_RANGE_FALLBACK:
        candidates.append(SHEET_RANGE_FALLBACK)

    candidates.extend([
        _a1(tab, "A1:AA10000"),
        _a1(tab, "A:AA"),
        _a1(tab, "A1:Z10000"),
    ])

    last_values = None
    last_err = None

    for rng in candidates:
        try:
            values = _fetch_range(rng)
            if not values:
                continue
            last_values = values
            if _header_has_anchors(values):
                return values
        except HttpError as e:
            last_err = e
            continue

    if last_values is not None:
        return last_values
    if last_err:
        raise last_err
    raise RuntimeError("Unable to load sheet values (no data returned)")


# =========================================================
# IMAGE CACHE in GOOGLE SHEETS (qid -> telegram file_id)
# =========================================================

class ImageCache:
    def __init__(self, service, spreadsheet_id: str, sheet_name: str, ttl: int = 300):
        self.service = service
        self.spreadsheet_id = spreadsheet_id
        self.sheet = sheet_name
        self.ttl = ttl

        self._loaded_at = 0.0
        self._map: Dict[str, str] = {}
        self._row: Dict[str, int] = {}
        self._has_header = False

        self._lock = threading.Lock()

    def _read_all(self):
        req = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.sheet}!A:C"
        )
        return gs_execute(req).get("values", [])

    def refresh(self, force: bool = False):
        with self._lock:
            now = time.time()
            if (not force) and self._map and (now - self._loaded_at) < self.ttl:
                return

            values = self._read_all()
            self._map.clear()
            self._row.clear()
            self._has_header = False

            if not values:
                self._loaded_at = now
                return

            if len(values[0]) >= 2 and str(values[0][0]).lower() == "qid":
                self._has_header = True
                data = values[1:]
                start_row = 2
            else:
                data = values
                start_row = 1

            for i, row in enumerate(data, start=start_row):
                if len(row) >= 2:
                    qid = row[0]
                    fid = row[1]
                    if qid:
                        self._map[str(qid).strip().upper()] = str(fid).strip()
                        self._row[str(qid).strip().upper()] = i

            self._loaded_at = now

    def ensure_header(self):
        with self._lock:
            self.refresh(force=False)
            if self._has_header:
                return
            req = self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.sheet}!A1:C1",
                valueInputOption="RAW",
                body={"values": [["qid", "file_id", "updated_at"]]}
            )
            gs_execute(req)
            self._has_header = True
            self.refresh(force=True)

    def get(self, qid: str) -> Optional[str]:
        self.refresh()
        with self._lock:
            return self._map.get(str(qid).strip().upper())

    def set(self, qid: str, file_id: str) -> None:
        self.ensure_header()
        now = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        qid_u = str(qid).strip().upper()

        with self._lock:
            row_idx = self._row.get(qid_u)
            if row_idx:
                req = self.service.spreadsheets().values().update(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"{self.sheet}!B{row_idx}:C{row_idx}",
                    valueInputOption="RAW",
                    body={"values": [[file_id, now]]}
                )
                gs_execute(req)
            else:
                req = self.service.spreadsheets().values().append(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"{self.sheet}!A:C",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": [[qid_u, file_id, now]]}
                )
                gs_execute(req)
                self.refresh(force=True)

            self._map[qid_u] = file_id


_IMAGE_CACHE: Optional[ImageCache] = None
_LICENSE: Optional[LicenseManager] = None


def get_license_mgr() -> LicenseManager:
    global _LICENSE
    if _LICENSE is None:
        svc = get_sheets_service()
        cfg = LicenseConfig(
            sheet_id=SHEET_ID,
            keys_sheet=KEYS_SHEET,
            subs_sheet=SUBS_SHEET,
            trial_sheet=TRIAL_SHEET,
        )
        _LICENSE = LicenseManager(service=svc, gs_execute=gs_execute, cfg=cfg)
    return _LICENSE


def get_image_cache() -> ImageCache:
    global _IMAGE_CACHE
    if _IMAGE_CACHE is None:
        svc = get_sheets_service()
        _IMAGE_CACHE = ImageCache(svc, SHEET_ID, IMAGE_CACHE_SHEET, ttl=CACHE_TTL)
    return _IMAGE_CACHE


async def get_image_file_id_if_any_async(q: dict) -> Optional[str]:
    media_raw = (q.get("media") or "")
    media = str(media_raw).strip().lower()

    if not media:
        return None
    ok = (
        media in ("photo", "image", "img", "pic", "pict", "yes", "y", "1", "true")
        or media.startswith("photo")
        or media.startswith("image")
        or media.startswith("img")
    )
    if not ok:
        return None

    qid = str(q["id"]).strip().upper()

    def _get():
        return get_image_cache().get(qid)

    try:
        async with timed("image_cache.get", qid=qid):
            return await tg_retry(
                lambda: to_thread_timeout(_get, timeout=8, label="image_cache.get.thread"),
                label="image_cache.get.retry"
            )
    except Exception as e:
        log.warning("image_cache.get failed qid=%s err=%r", qid, e)
        return None


# =========================================================
# QUESTIONS CACHE + INDEX
# =========================================================

QUESTIONS: List[dict] = []
QINDEX: Dict[str, int] = {}
LAST_LOAD = 0.0
_LOAD_LOCK = asyncio.Lock()


def normalize_qid(arg: str) -> Optional[str]:
    if arg is None:
        return None
    s = str(arg).strip().upper()
    m = re.search(r"(\d+)", s)
    if not m:
        return None
    num = int(m.group(1))
    return f"Q{num}"


def parse_correct_to_set(raw: str) -> Optional[Set[str]]:
    if raw is None:
        return None
    s = str(raw).strip().upper()
    if not s:
        return None

    s = re.sub(r"[;/|]+", ",", s)
    s = re.sub(r"\s+", ",", s)
    s = s.strip(",")

    parts = [p for p in s.split(",") if p]
    m_num = {"1": "A", "2": "B", "3": "C", "4": "D", "5": "E"}

    out: Set[str] = set()
    for p in parts:
        if p in {"A", "B", "C", "D", "E"}:
            out.add(p)
        elif p.isdigit() and p in m_num:
            out.add(m_num[p])

    return out if out else None


def parse_rows(values: List[List[str]]) -> List[dict]:
    if not values:
        return []

    header = [str(h).strip() for h in values[0]]
    col = {h: i for i, h in enumerate(header) if h}

    def g(row, key) -> str:
        i = col.get(key)
        return row[i].strip() if i is not None and i < len(row) and row[i] is not None else ""

    has_e_en = "E_EN" in col
    has_e_ru = "E_RU" in col

    qs = []
    for row in values[1:]:
        raw_id = g(row, "Q_ID")
        qid = normalize_qid(raw_id)
        if not qid:
            continue

        correct_set = parse_correct_to_set(g(row, "Correct_EN"))
        if not correct_set:
            continue

        media = (g(row, "Image_Type") or "").strip().lower()

        opts_en = {k: g(row, f"{k}_EN") for k in "ABCD"}
        opts_ru = {k: g(row, f"{k}_RU") for k in "ABCD"}

        e_en = g(row, "E_EN") if has_e_en else ""
        e_ru = g(row, "E_RU") if has_e_ru else ""
        opts_en["E"] = e_en
        opts_ru["E"] = e_ru

        has_e = bool(e_en.strip() or e_ru.strip() or ("E" in correct_set))
        letters = "ABCDE" if has_e else "ABCD"
        if "E" in correct_set:
            letters = "ABCDE"

        qs.append({
            "id": qid,
            "media": media,
            "letters": letters,

            "q_en": g(row, "Question_EN"),
            "q_ru": g(row, "Question_RU"),

            "opts_en": opts_en,
            "opts_ru": opts_ru,

            "correct_set": correct_set,
            "multi": (len(correct_set) > 1),

            "anchors_en": g(row, "Anchors_EN"),
            "anchors_ru": g(row, "Anchors_RU"),
            "correct_anchors_en": g(row, "Correct_Anchors_EN"),
            "correct_anchors_ru": g(row, "Correct_Anchors_RU"),
        })
    return qs


async def ensure_loaded_async(force: bool = False):
    global QUESTIONS, QINDEX, LAST_LOAD
    now = time.time()
    if (not force) and QUESTIONS and (now - LAST_LOAD <= CACHE_TTL):
        return

    async with _LOAD_LOCK:
        now2 = time.time()
        if (not force) and QUESTIONS and (now2 - LAST_LOAD <= CACHE_TTL):
            return

        def _load():
            return load_rows_from_sheets()

        async with timed("sheets.load_rows"):
            values = await tg_retry(
                lambda: to_thread_timeout(_load, timeout=25, label="sheets.load_rows.thread"),
                label="sheets.load_rows.retry"
            )

        async with timed("parse_rows"):
            parsed = parse_rows(values)

        QUESTIONS[:] = parsed
        QINDEX.clear()
        QINDEX.update({q["id"]: i for i, q in enumerate(QUESTIONS)})
        LAST_LOAD = time.time()
        log.info("Loaded QUESTIONS=%d", len(QUESTIONS))


def get_letters(q: dict) -> str:
    opts_en = q.get("opts_en") or {}
    opts_ru = q.get("opts_ru") or {}
    correct_set = set(q.get("correct_set") or [])
    has_e = bool((opts_en.get("E", "") or "").strip() or (opts_ru.get("E", "") or "").strip() or ("E" in correct_set))
    return "ABCDE" if has_e else "ABCD"


def q_correct_set(q: dict) -> Set[str]:
    return set(q.get("correct_set") or [])


# =========================================================
# RENDER
# =========================================================

def render(
    q: dict,
    *,
    reveal: Optional[str] = None,
    reveal_set: Optional[Set[str]] = None,
    show_correct_only: bool = False,
) -> str:
    letters = get_letters(q)
    correct_set = q_correct_set(q)

    def esc(s):
        return html.escape(s or "")

    parts = [f"<b>{esc(q.get('q_en', ''))}</b>"]
    q_ru = (q.get("q_ru") or "").strip()
    if q_ru:
        parts.append(esc(q_ru))

    lines = []
    for k in letters:
        opt_en = (q["opts_en"].get(k, "") or "")
        opt_ru = (q["opts_ru"].get(k, "") or "")

        label = k
        if q.get("multi"):
            picked = reveal_set or set()
            if show_correct_only:
                if k in correct_set:
                    label = f"✅ {k}"
            else:
                if picked:
                    if k in correct_set:
                        label = f"✅ {k}"
                    elif k in picked:
                        label = f"❌ {k}"
        else:
            if show_correct_only:
                if k in correct_set:
                    label = f"✅ {k}"
            else:
                if reveal in set("ABCDE"):
                    if k in correct_set:
                        label = f"✅ {k}"
                    elif k == reveal:
                        label = f"❌ {k}"

        line = f"{label}) {esc(opt_en)}"
        if (opt_ru or "").strip():
            line += f"\n   {esc(opt_ru)}"
        lines.append(line)

    parts.append("\n\n" + "\n\n".join(lines))
    return "\n".join(parts)


# =========================================================
# ANCHORS (отдельным сообщением, но всегда НИЖЕ вопроса)
# =========================================================

def _anchors_message_text(q: dict) -> str:
    qid = html.escape(str(q.get("id", "")))

    a_en = str(q.get("anchors_en", "") or "").strip()
    a_ru = str(q.get("anchors_ru", "") or "").strip()
    c_en = str(q.get("correct_anchors_en", "") or "").strip()
    c_ru = str(q.get("correct_anchors_ru", "") or "").strip()

    parts = [f"<b>Якоря — выучить — важно</b>\n<code>{qid}</code>"]

    def add_block(title: str, raw: str):
        if not raw:
            return
        parts.append(f"\n<i>{html.escape(title)}:</i>\n<pre>{html.escape(raw)}</pre>")

    add_block("Anchors_EN", a_en)
    add_block("Anchors_RU", a_ru)
    add_block("Correct_Anchors_EN", c_en)
    add_block("Correct_Anchors_RU", c_ru)

    if len(parts) == 1:
        parts.append("\n<i>(якоря пустые)</i>")

    return "\n".join(parts)


async def _delete_anchors_if_any(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    a = context.user_data.get("_anchors")
    if not a:
        return
    if a.get("chat_id") != chat_id or not a.get("message_id"):
        context.user_data.pop("_anchors", None)
        context.user_data.pop("_anchors_last_qid", None)
        return
    try:
        await tg_retry(
            lambda: context.bot.delete_message(chat_id=chat_id, message_id=a["message_id"]),
            label="delete_anchors"
        )
    except Exception:
        pass
    context.user_data.pop("_anchors", None)
    context.user_data.pop("_anchors_last_qid", None)


async def _upsert_anchors_message_below(update: Update, context: ContextTypes.DEFAULT_TYPE, q: dict):
    if context.user_data.get("mode") != "learn":
        return

    chat_id = _chat_id_from_update(update)
    if chat_id is None:
        return

    qid = str(q.get("id", ""))

    if context.user_data.get("_anchors_last_qid") == qid:
        return
    context.user_data["_anchors_last_qid"] = qid

    text = _anchors_message_text(q)

    a = context.user_data.get("_anchors")
    if a and a.get("chat_id") == chat_id and a.get("message_id"):
        mid = a["message_id"]
        try:
            await tg_retry(
                lambda: context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=mid,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True
                ),
                label="edit_anchors"
            )
            return
        except Exception:
            try:
                await tg_retry(
                    lambda: context.bot.delete_message(chat_id=chat_id, message_id=mid),
                    label="delete_anchors_old"
                )
            except Exception:
                pass

    msg = await tg_retry(
        lambda: context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        ),
        label="send_anchors"
    )
    context.user_data["_anchors"] = {"chat_id": chat_id, "message_id": msg.message_id}


# =========================================================
# KEYBOARDS
# =========================================================

async def _edit_only_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE, reply_markup):
    query = update.callback_query
    if not query or not query.message:
        return
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=query.message.chat_id,
            message_id=query.message.message_id,
            reply_markup=reply_markup
        )
    except Exception:
        pass


def _build_jump_rows(total_questions: int):
    rows = []
    if total_questions <= 0:
        return rows

    starts = list(range(1, total_questions + 1, 100))
    buttons = []
    for s in starts:
        if s >= 901:
            buttons.append(InlineKeyboardButton("901-…", callback_data="jump:901"))
            break
        e = s + 99
        buttons.append(InlineKeyboardButton(f"{s}-{e}", callback_data=f"jump:{s}"))

    for i in range(0, len(buttons), 3):
        rows.append(buttons[i:i + 3])
    return rows


def kb_learn(letters: str, total_questions_for_jump: int, *, trial: bool):
    row1 = [InlineKeyboardButton(k, callback_data=k) for k in letters]

    if trial:
        row2 = [
            InlineKeyboardButton("⬅️ Предыдущий", callback_data="prev"),
            InlineKeyboardButton("➡️ Следующий", callback_data="next"),
        ]
        return InlineKeyboardMarkup([row1, row2])

    row2 = [
        InlineKeyboardButton("🔎 Перейти", callback_data="goto_help"),
        InlineKeyboardButton("⬅️ Предыдущий", callback_data="prev"),
        InlineKeyboardButton("➡️ Следующий", callback_data="next"),
    ]
    return InlineKeyboardMarkup([row1, row2] + _build_jump_rows(total_questions_for_jump))


def kb_drill(letters: str, show_answer: bool):
    row1 = [InlineKeyboardButton(k, callback_data=k) for k in letters]
    row2 = [
        InlineKeyboardButton("✅ Ответ" if not show_answer else "✅ Скрыть", callback_data="toggle_answer"),
        InlineKeyboardButton("🔎 Перейти", callback_data="goto_help"),
        InlineKeyboardButton("➡️ Следующий", callback_data="next"),
    ]
    return InlineKeyboardMarkup([row1, row2])


def kb_exam(letters: str):
    return InlineKeyboardMarkup([[InlineKeyboardButton(k, callback_data=k) for k in letters]])


def kb_multi_learn(letters: str, selected: Set[str], total_questions_for_jump: int, *, trial: bool):
    row1 = []
    for k in letters:
        row1.append(InlineKeyboardButton(f"✅ {k}" if k in selected else k, callback_data=f"pick:{k}"))

    if trial:
        row2 = [
            InlineKeyboardButton("✅ Готово", callback_data="done"),
            InlineKeyboardButton("⬅️ Предыдущий", callback_data="prev"),
            InlineKeyboardButton("➡️ Следующий", callback_data="next"),
        ]
        return InlineKeyboardMarkup([row1, row2])

    row2 = [
        InlineKeyboardButton("✅ Готово", callback_data="done"),
        InlineKeyboardButton("🔎 Перейти", callback_data="goto_help"),
        InlineKeyboardButton("⬅️ Предыдущий", callback_data="prev"),
        InlineKeyboardButton("➡️ Следующий", callback_data="next"),
    ]
    return InlineKeyboardMarkup([row1, row2] + _build_jump_rows(total_questions_for_jump))


def kb_multi_drill(letters: str, selected: Set[str], show_answer: bool):
    row1 = []
    for k in letters:
        row1.append(InlineKeyboardButton(f"✅ {k}" if k in selected else k, callback_data=f"pick:{k}"))

    row2 = [
        InlineKeyboardButton("✅ Ответ" if not show_answer else "✅ Скрыть", callback_data="toggle_answer"),
        InlineKeyboardButton("✅ Готово", callback_data="done"),
        InlineKeyboardButton("➡️ Следующий", callback_data="next"),
    ]
    return InlineKeyboardMarkup([row1, row2])


def kb_multi_exam(letters: str, selected: Set[str]):
    row1 = []
    for k in letters:
        row1.append(InlineKeyboardButton(f"✅ {k}" if k in selected else k, callback_data=f"pick:{k}"))
    row2 = [InlineKeyboardButton("✅ Готово", callback_data="done")]
    return InlineKeyboardMarkup([row1, row2])


def kb_multi_result_learn(letters: str, chosen: Set[str], correct: Set[str], total_questions_for_jump: int, *, trial: bool):
    row1 = []
    for k in letters:
        if k in correct:
            label = f"✅ {k}"
        elif k in chosen:
            label = f"❌ {k}"
        else:
            label = k
        row1.append(InlineKeyboardButton(label, callback_data="noop"))

    if trial:
        row2 = [
            InlineKeyboardButton("⬅️ Предыдущий", callback_data="prev"),
            InlineKeyboardButton("➡️ Следующий", callback_data="next"),
        ]
        return InlineKeyboardMarkup([row1, row2])

    row2 = [
        InlineKeyboardButton("🔎 Перейти", callback_data="goto_help"),
        InlineKeyboardButton("⬅️ Предыдущий", callback_data="prev"),
        InlineKeyboardButton("➡️ Следующий", callback_data="next"),
    ]
    return InlineKeyboardMarkup([row1, row2] + _build_jump_rows(total_questions_for_jump))


# =========================================================
# EXAM POOL + SCORE
# =========================================================

def build_exam_pool_stratified(qs: List[dict], chunks: int = 40) -> List[dict]:
    n = len(qs)
    if n == 0:
        return []
    if n <= chunks:
        out = qs[:]
        random.shuffle(out)
        return out

    pool = []
    used = set()
    for i in range(chunks):
        start = (i * n) // chunks
        end = ((i + 1) * n) // chunks
        slice_ = qs[start:end]
        if not slice_:
            continue
        candidates = [q for q in slice_ if q["id"] not in used] or slice_
        qq = random.choice(candidates)
        pool.append(qq)
        used.add(qq["id"])

    if len(pool) < chunks:
        remaining = [q for q in qs if q["id"] not in used]
        random.shuffle(remaining)
        pool.extend(remaining[:chunks - len(pool)])

    return pool[:chunks]


def calc_points_0_100(correct_count: int, total: int) -> int:
    if total <= 0:
        return 0
    pts = round((correct_count / total) * 100)
    return max(0, min(100, pts))


# =========================================================
# SCREEN MESSAGE (вопрос)
# =========================================================

async def _delete_image_if_any(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    img = context.user_data.get("_img")
    if not img:
        return
    if img.get("chat_id") != chat_id or not img.get("message_id"):
        context.user_data.pop("_img", None)
        return
    try:
        await tg_retry(
            lambda: context.bot.delete_message(chat_id=chat_id, message_id=img["message_id"]),
            label="delete_img"
        )
    except Exception:
        pass
    context.user_data.pop("_img", None)


async def _upsert_image_message_above(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    file_id: str,
):
    """Картинка отдельным сообщением (без caption). Делает delete+send."""
    chat_id = _chat_id_from_update(update)
    if chat_id is None:
        return

    await _delete_image_if_any(context, chat_id)

    msg, kind = await tg_send_media(
        bot=context.bot,
        chat_id=chat_id,
        file_id=file_id,
        caption="",
        parse_mode=ParseMode.HTML,
        reply_markup=None
    )
    context.user_data["_img"] = {"chat_id": chat_id, "message_id": msg.message_id, "kind": kind}


def _learn_total_for_header(context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("trial"):
        return len(context.user_data.get("trial_pool") or [])
    return len(QUESTIONS) if QUESTIONS else 0


def _learn_total_for_jump(context: ContextTypes.DEFAULT_TYPE) -> int:
    # В trial НЕ показываем диапазоны
    if context.user_data.get("trial"):
        return 0
    return len(QUESTIONS) if QUESTIONS else 0


def _mode_question_header(context: ContextTypes.DEFAULT_TYPE, q: dict) -> str:
    mode = context.user_data.get("mode")
    qid = q.get("id", "")

    if mode == "learn":
        idx = int(context.user_data.get("learn_idx", 0))
        total = _learn_total_for_header(context)
        return f"<b>Вопрос {idx + 1} / {total}</b> · <code>{html.escape(qid)}</code>\n\n"

    if mode == "drill":
        idx = int(context.user_data.get("drill_idx", 0))
        pool = context.user_data.get("drill_pool") or []
        total = len(pool)
        return f"<b>Вопрос {idx + 1} / {total}</b> · <code>{html.escape(qid)}</code>\n\n"

    if mode == "exam":
        idx = int(context.user_data.get("exam_idx", 0))
        pool = context.user_data.get("exam_pool") or []
        total = len(pool)
        return f"<b>Экзамен</b>\nВопрос {idx + 1} / {total}\n\n"

    return ""


async def _send_or_edit_question_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    text: str,
    reply_markup: InlineKeyboardMarkup,
    image_file_id: Optional[str],
):
    """
    Единая логика экрана:
    - если есть image_file_id и text влезает в caption -> media
    - иначе -> text
    - edit media: photo -> fallback document -> delete+resend
    - edit text: если не получается -> resend
    """
    chat_id = _chat_id_from_update(update)
    if chat_id is None:
        return
    bot = context.bot

    wants_media = bool(image_file_id) and (len(text) <= CAPTION_SAFE_LIMIT)
    if SINGLE_SCREEN and image_file_id and (len(text) > CAPTION_SAFE_LIMIT):
        wants_media = False
        image_file_id = None

    screen = context.user_data.get("_screen")  # {"chat_id":..,"message_id":..,"has_media":bool}

    async def _delete_old_screen_if_any():
        old = context.user_data.get("_screen")
        if old and old.get("chat_id") == chat_id and old.get("message_id"):
            try:
                await tg_retry(
                    lambda: bot.delete_message(chat_id=chat_id, message_id=old["message_id"]),
                    label="delete_old_screen"
                )
            except Exception:
                pass

    async def _send_new_screen():
        await _delete_old_screen_if_any()
        qid_now = (context.user_data.get("q") or {}).get("id", "")

        if wants_media and image_file_id:
            msg, kind = await tg_send_media(
                bot=bot,
                chat_id=chat_id,
                file_id=image_file_id,
                caption=text,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup
            )
            context.user_data["_screen"] = {"chat_id": chat_id, "message_id": msg.message_id, "has_media": True}
            log.info("SCREEN send new media=%s qid=%s msg=%s", kind, qid_now, msg.message_id)
        else:
            msg = await tg_retry(
                lambda: bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup
                ),
                label="send_text_screen"
            )
            context.user_data["_screen"] = {"chat_id": chat_id, "message_id": msg.message_id, "has_media": False}
            log.info("SCREEN send new text qid=%s msg=%s", qid_now, msg.message_id)

    if not screen or screen.get("chat_id") != chat_id or not screen.get("message_id"):
        await _send_new_screen()
        return

    msg_id = int(screen["message_id"])
    has_media = bool(screen.get("has_media"))
    qid_now = (context.user_data.get("q") or {}).get("id", "")

    async with timed("screen.update", qid=qid_now, wants_media=wants_media, has_media=has_media):
        if wants_media and image_file_id:
            if not has_media:
                await _send_new_screen()
                return

            new_mid, kind, did_resend = await tg_edit_media_with_fallback(
                bot=bot,
                chat_id=chat_id,
                message_id=msg_id,
                file_id=image_file_id,
                caption=text,
                reply_markup=reply_markup
            )
            context.user_data["_screen"]["message_id"] = new_mid
            context.user_data["_screen"]["has_media"] = True
            log.info("SCREEN edit media=%s did_resend=%s qid=%s msg=%s", kind, did_resend, qid_now, new_mid)
            return

        # wants text
        if has_media:
            await _send_new_screen()
            return

        try:
            await tg_retry(
                lambda: bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup
                ),
                label="edit_text_screen",
                no_retry_badrequest=False
            )
            context.user_data["_screen"]["has_media"] = False
            return
        except Exception as e:
            log.warning("edit_text failed -> resend qid=%s msg=%s err=%r", qid_now, msg_id, e)
            await _send_new_screen()
            return


# =========================================================
# SHARED: show current question
# =========================================================

def _reset_answer_state(context: ContextTypes.DEFAULT_TYPE, mode: str):
    context.user_data["learn_reveal"] = None
    context.user_data["drill_reveal"] = None
    context.user_data["multi_sel"] = []
    context.user_data["multi_sel_order"] = []
    context.user_data["multi_reveal"] = []
    context.user_data["multi_locked"] = False
    if mode == "drill":
        context.user_data["show_answer"] = False


async def _clear_screens(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    scr = context.user_data.get("_screen")
    if scr and scr.get("chat_id") == chat_id and scr.get("message_id"):
        try:
            await tg_retry(
                lambda: context.bot.delete_message(chat_id=chat_id, message_id=scr["message_id"]),
                label="delete_screen"
            )
        except Exception:
            pass
    context.user_data.pop("_screen", None)

    await _delete_image_if_any(context, chat_id)
    await _delete_anchors_if_any(context, chat_id)


async def show_current(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    prefix: str = "",
    send_anchors: bool = False
):
    mode = context.user_data.get("mode")
    q = context.user_data.get("q")
    chat_id = _chat_id_from_update(update)

    if not mode or not q or chat_id is None:
        # мягко сообщаем в чат, если можем
        try:
            if update.callback_query and update.callback_query.message:
                await tg_retry(
                    lambda: context.bot.send_message(
                        chat_id=update.callback_query.message.chat_id,
                        text="Сессия сбросилась. Запусти: /learn, /drill или /exam"
                    ),
                    label="session_reset_msg"
                )
            elif update.message:
                await tg_retry(lambda: update.message.reply_text("Запусти: /learn, /drill или /exam"))
        except Exception:
            pass
        return

    letters = get_letters(q)
    is_multi = bool(q.get("multi"))
    show_answer = bool(context.user_data.get("show_answer", False))

    reveal = None
    if mode == "drill":
        reveal = context.user_data.get("drill_reveal")
    if mode == "learn":
        reveal = context.user_data.get("learn_reveal")

    selected = set(context.user_data.get("multi_sel") or [])
    reveal_set = set(context.user_data.get("multi_reveal") or [])

    header = _mode_question_header(context, q)
    show_correct_only = bool(mode == "drill" and show_answer)

    multi_hint = ""
    if is_multi and mode in ("learn", "drill", "exam"):
        multi_hint = "<i>Выбери ДВА ответа и нажми ✅ Готово</i>\n\n"

    body = render(
        q,
        reveal=reveal,
        reveal_set=(reveal_set if (is_multi and reveal_set) else None),
        show_correct_only=show_correct_only,
    )

    text = (prefix + header + multi_hint + body).strip()
    image_file_id = await get_image_file_id_if_any_async(q)

    log.info("IMG decide qid=%s media=%s file_id=%s text_len=%d",
             q.get("id"), q.get("media"), image_file_id, len(text))

    # Если картинка есть, но caption слишком длинный — делаем стек:
    # [картинка] -> [вопрос] -> [якоря]
    if image_file_id and len(text) > CAPTION_SAFE_LIMIT:
        await _clear_screens(context, chat_id)
        await _upsert_image_message_above(update, context, file_id=image_file_id)
        image_for_screen = None
    else:
        if not image_file_id:
            await _delete_image_if_any(context, chat_id)
        image_for_screen = image_file_id

    # totals
    if mode == "learn":
        total_for_jump = _learn_total_for_jump(context)   # 0 в trial
    else:
        total_for_jump = len(QUESTIONS) if QUESTIONS else 0

    # markup
    if mode == "learn":
        trial = bool(context.user_data.get("trial"))
        if is_multi:
            if context.user_data.get("multi_locked", False) and reveal_set:
                correct = q_correct_set(q)
                markup = kb_multi_result_learn(letters, reveal_set, correct, total_for_jump, trial=trial)
            else:
                markup = kb_multi_learn(letters, selected, total_for_jump, trial=trial)
        else:
            markup = kb_learn(letters, total_for_jump, trial=trial)

    elif mode == "drill":
        if is_multi:
            markup = kb_multi_drill(letters, selected, show_answer)
        else:
            markup = kb_drill(letters, show_answer)

    else:  # exam
        if is_multi:
            markup = kb_multi_exam(letters, selected)
        else:
            markup = kb_exam(letters)

    await _send_or_edit_question_message(
        update,
        context,
        text=text,
        reply_markup=markup,
        image_file_id=image_for_screen,
    )

    if mode == "learn" and send_anchors:
        await _upsert_anchors_message_below(update, context, q)
    elif mode != "learn":
        await _delete_anchors_if_any(context, chat_id)


# =========================================================
# TRIAL (daily random 30)
# =========================================================

async def _pick_trial_pool_async() -> List[dict]:
    """TRIAL_QUESTIONS рандомных, часть с картинками (если возможно)."""
    await ensure_loaded_async()
    qs = QUESTIONS[:]
    random.shuffle(qs)

    img_candidates = [q for q in qs if (q.get("media") or "").lower().strip() in ("photo", "image")]
    non_img_candidates = [q for q in qs if q not in img_candidates]

    picked: List[dict] = []

    want_img = min(8, TRIAL_QUESTIONS)
    for q in img_candidates:
        fid = await get_image_file_id_if_any_async(q)
        if fid:
            picked.append(q)
            if len(picked) >= want_img:
                break

    seen = {q["id"] for q in picked}
    for q in non_img_candidates + img_candidates:
        if q["id"] in seen:
            continue
        picked.append(q)
        seen.add(q["id"])
        if len(picked) >= TRIAL_QUESTIONS:
            break

    return picked[:TRIAL_QUESTIONS]


async def _setup_trial_if_needed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[List[dict]]:
    """
    Возвращает trial_pool (список questions) если подписки нет.
    Иначе возвращает None.
    """
    uid = update.effective_user.id

    def _paid():
        return get_license_mgr().is_active(uid)

    paid = await tg_retry(
        lambda: to_thread_timeout(_paid, timeout=10, label="license.is_active.thread"),
        label="license.is_active.retry"
    )
    if paid:
        context.user_data.pop("trial_pool", None)
        return None

    today = kst_today_str()
    key = (uid, today)

    # если на сегодня уже исчерпал — /learn должен сразу вернуть "закончились"
    if DAILY_TRIAL_EXHAUSTED.get(key, False):
        return []

    # если пул на сегодня уже есть — использовать тот же (чтобы всегда были одни и те же 30 в течение дня)
    pool = DAILY_TRIAL_POOL.get(key)
    if pool and len(pool) == TRIAL_QUESTIONS:
        context.user_data["trial_pool"] = pool
        return pool

    # иначе — новый дневной пул
    pool = await _pick_trial_pool_async()
    DAILY_TRIAL_POOL[key] = pool
    DAILY_TRIAL_EXHAUSTED[key] = False
    context.user_data["trial_pool"] = pool
    return pool


# =========================================================
# COMMANDS
# =========================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await tg_retry(
        lambda: update.message.reply_text(
            "👋 Вас приветствует тренажёр подготовки к письменному экзамену на водительские права в Корее.\n\n"
            "📚 Перед вами — одна из самых полных баз вопросов из официального банка, полностью переведённая на русский язык.\n"
            "Вопросы и варианты ответов здесь выглядят так же, как на настоящем экзамене.\n\n"
            "🌐 Важно: сам экзамен проводится на корейском или английском языке.\n"
            "Русский перевод нужен для понимания смысла — так материал усваивается быстрее и увереннее.\n\n"
            "🎯 Чтобы успешно сдать экзамен, нужно выучить «якорные фразы» из вопросов и правильных ответов — это самый быстрый и эффективный способ пройти тест.\n\n"
            "🏠 Чтобы вернуться на главный экран, наберите команду: /start\n\n"
            "Режимы:\n\n"
            "/learn — обучение с правильными ответами и якорями\n"
            "/drill — интенсивная тренировка\n"
            "/exam — экзамен (40 вопросов)\n\n"
            "🛠️ Для получения расширенного доступа пишите разрабочику: @luv2win.\n\n"
            "🍀 Удачи на экзамене!"
        ),
        label="start"
    )


async def learn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_loaded_async()
    if not QUESTIONS:
        await tg_retry(lambda: update.message.reply_text("База вопросов пустая."))
        return

    chat_id = _chat_id_from_update(update)
    if chat_id is None:
        return

    # ВАЖНО: сначала чистим, потом setup_trial (чтобы не стирать его записи)
    await _clear_screens(context, chat_id)
    context.user_data.clear()

    trial_pool = await _setup_trial_if_needed(update, context)

    # если free и на сегодня уже исчерпано — _setup_trial_if_needed вернул []
    if trial_pool == []:
        await tg_retry(
            lambda: update.message.reply_text(
                f"🔒 Бесплатные {TRIAL_QUESTIONS} вопросов на сегодня закончились. Завтра будут доступны еще 30.\n\n"
                + PRICING_BLOCK + "\n"
                "За расширенным доступом обратитесь к разработчику: @luv2win",
                parse_mode=ParseMode.HTML
            ),
            label="trial_exhausted_learn_msg"
        )
        return

    if trial_pool:
        context.user_data.update({
            "mode": "learn",
            "trial": True,
            "learn_idx": 0,
            "q": trial_pool[0],
            "trial_pool": trial_pool,

            "learn_reveal": None,
            "multi_sel": [],
            "multi_sel_order": [],
            "multi_reveal": [],
            "multi_locked": False,

            "_anchors_last_qid": None,
            "_anchors": None,
            "_screen": None,
        })
    else:
        context.user_data.update({
            "mode": "learn",
            "trial": False,
            "learn_idx": 0,
            "q": QUESTIONS[0],

            "learn_reveal": None,
            "multi_sel": [],
            "multi_sel_order": [],
            "multi_reveal": [],
            "multi_locked": False,

            "_anchors_last_qid": None,
            "_anchors": None,
            "_screen": None,
        })

    await show_current(update, context, send_anchors=True)


async def drill(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # если нет подписки — запрещаем
    def _paid():
        return get_license_mgr().is_active(update.effective_user.id)

    paid = await tg_retry(
        lambda: to_thread_timeout(_paid, timeout=10, label="license.is_active.thread"),
        label="license.is_active.retry"
    )
    if not paid:
        await tg_retry(
            lambda: update.message.reply_text(
                "🔒 Интенсив доступен по подписке.\n"
                "Активируй ключ: /redeem KR-XXXX-XXXX-XXXX\n"
                f"Бесплатно доступно: /learn ({TRIAL_QUESTIONS} вопросов) и 1 раз /exam"
            )
        )
        return

    await ensure_loaded_async()
    if not QUESTIONS:
        await tg_retry(lambda: update.message.reply_text("База вопросов пустая."))
        return

    k = min(200, len(QUESTIONS))
    pool = random.sample(QUESTIONS, k=k)

    chat_id = _chat_id_from_update(update)
    if chat_id is None:
        return

    await _clear_screens(context, chat_id)
    context.user_data.clear()

    context.user_data.update({
        "mode": "drill",
        "drill_pool": pool,
        "drill_idx": 0,
        "q": pool[0],

        "show_answer": False,
        "drill_reveal": None,

        "multi_sel": [],
        "multi_sel_order": [],
        "multi_reveal": [],
        "multi_locked": False,

        "_screen": None,
        "_anchors": None,
    })
    await show_current(update, context, prefix="<b>Интенсив</b>\n\n", send_anchors=False)


async def exam(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_loaded_async()
    if not QUESTIONS:
        await tg_retry(lambda: update.message.reply_text("База вопросов пустая."))
        return

    uid = update.effective_user.id

    def _paid_and_trial():
        mgr = get_license_mgr()
        return mgr.is_active(uid), mgr.trial_exam_available(uid)

    paid, trial_exam_ok = await tg_retry(
        lambda: to_thread_timeout(_paid_and_trial, timeout=12, label="exam.access.thread"),
        label="exam.access.retry"
    )

    if (not paid) and (not trial_exam_ok):
        await tg_retry(
            lambda: update.message.reply_text(
                "🔒 Бесплатная попытка экзамена уже использована.\n\n"
                + PRICING_BLOCK + "\n"
                "Активируй ключ: /redeem KR-XXXX-XXXX-XXXX",
                parse_mode=ParseMode.HTML
            )
        )
        return

    pool = build_exam_pool_stratified(QUESTIONS, chunks=40)

    chat_id = _chat_id_from_update(update)
    if chat_id is None:
        return

    await _clear_screens(context, chat_id)
    context.user_data.clear()

    context.user_data.update({
        "mode": "exam",
        "exam_pool": pool,
        "exam_idx": 0,
        "exam_score": 0,
        "exam_results": [],

        "q": pool[0],

        "multi_sel": [],
        "multi_sel_order": [],
        "multi_reveal": [],
        "multi_locked": False,

        "_screen": None,
        "_anchors": None,
    })

    await show_current(update, context, send_anchors=False)


async def goto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_loaded_async()
    if not QUESTIONS:
        await tg_retry(lambda: update.message.reply_text("База вопросов пустая."))
        return

    mode = context.user_data.get("mode")
    if mode == "exam":
        await tg_retry(lambda: update.message.reply_text("В экзамене переход по номеру отключён."))
        return

    if not mode:
        await tg_retry(lambda: update.message.reply_text("Сначала запусти /learn или /drill"))
        return

    # trial: запрещаем /goto
    if context.user_data.get("trial"):
        await tg_retry(
            lambda: update.message.reply_text(
                "🔒 В бесплатном режиме переход по номеру отключён.\n\n"
                + PRICING_BLOCK + "\n"
                "Активируй ключ: /redeem KR-XXXX-XXXX-XXXX",
                parse_mode=ParseMode.HTML
            )
        )
        return

    if not context.args:
        await tg_retry(lambda: update.message.reply_text("Формат: /goto 888 или /goto Q888"))
        return

    qid = normalize_qid(context.args[0])
    if not qid:
        await tg_retry(lambda: update.message.reply_text("Неверный формат. Пример: /goto 888 или /goto Q888"))
        return

    idx = QINDEX.get(qid)
    if idx is None:
        await tg_retry(lambda: update.message.reply_text(f"Не нашла вопрос {qid}."))
        return

    if mode == "learn":
        context.user_data["learn_idx"] = idx
        context.user_data["q"] = QUESTIONS[idx]
        _reset_answer_state(context, "learn")
        await show_current(update, context, send_anchors=True)
        return

    if mode == "drill":
        pool = context.user_data.get("drill_pool") or []
        pos = next((i for i, qq in enumerate(pool) if qq["id"] == qid), None)
        if pos is None:
            await tg_retry(lambda: update.message.reply_text(
                f"{qid} не входит в текущий интенсив (200).\n"
                f"Сделай /drill заново или перейди через /learn + /goto {qid}"
            ))
            return
        context.user_data["drill_idx"] = pos
        context.user_data["q"] = pool[pos]
        _reset_answer_state(context, "drill")
        await show_current(update, context, prefix="<b>Интенсив</b>\n\n")
        return

    await tg_retry(lambda: update.message.reply_text("Сначала запусти /learn или /drill"))


async def redeem_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not context.args:
        await tg_retry(lambda: update.message.reply_text("Формат: /redeem KR-XXXX-XXXX-XXXX"))
        return
    key = context.args[0].strip()

    def _redeem():
        return get_license_mgr().redeem(user_id=update.effective_user.id, key=key)

    res = await tg_retry(
        lambda: to_thread_timeout(_redeem, timeout=20, label="license.redeem.thread"),
        label="license.redeem.retry"
    )

    if not res.get("ok"):
        reason = res.get("reason")
        msg = {
            "empty_key": "Ключ пустой.",
            "not_found": "Ключ не найден.",
            "revoked": "Ключ отключён.",
            "used": "Этот ключ уже использован.",
        }.get(reason, "Не получилось активировать ключ.")
        await tg_retry(lambda: update.message.reply_text(msg))
        return

    await tg_retry(lambda: update.message.reply_text(
        "✅ Доступ активирован\n"
        f"План: {res['plan']}\n"
        f"Действует до: {res['expires_at']}"
    ))


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    uid = update.effective_user.id

    def _status():
        mgr = get_license_mgr()
        return mgr.is_active(uid), mgr.get_user_sub(uid), mgr.get_trial(uid)

    active, sub, trial = await tg_retry(
        lambda: to_thread_timeout(_status, timeout=15, label="license.status.thread"),
        label="license.status.retry"
    )

    if active and sub:
        await tg_retry(lambda: update.message.reply_text(
            "🟢 Доступ активен\n"
            f"До: {sub.get('expires_at', '')}\n"
            f"План: {sub.get('plan', '')}"
        ))
        return

    # free-status: показываем дневной лимит (30/день) без изменения прочего
    today = kst_today_str()
    exhausted = DAILY_TRIAL_EXHAUSTED.get((uid, today), False)

    await tg_retry(lambda: update.message.reply_text(
        "🔓 Сейчас ты в бесплатном режиме.\n"
        f"Бесплатные вопросы: {TRIAL_QUESTIONS} в день (обновление в 00:00 KST)\n"
        f"Сегодня: {'закончились' if exhausted else 'доступны'}\n\n"
        + PRICING_BLOCK + "\n"
        "Активировать ключ: /redeem KR-XXXX-XXXX-XXXX",
        parse_mode=ParseMode.HTML
    ))


async def mkkeys_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not _is_admin(update):
        await tg_retry(lambda: update.message.reply_text("Нет прав."))
        return

    if len(context.args) < 2:
        await tg_retry(lambda: update.message.reply_text("Формат: /mkkeys week 10  (или /mkkeys month 10)"))
        return

    plan = context.args[0].strip().lower()
    try:
        count = int(context.args[1])
    except Exception:
        count = 1

    def _mk():
        return get_license_mgr().create_keys(plan=plan, count=count, created_by=update.effective_user.id)

    res = await tg_retry(
        lambda: to_thread_timeout(_mk, timeout=25, label="license.mkkeys.thread"),
        label="license.mkkeys.retry"
    )

    keys_text = "\n".join(res["keys"])
    if len(keys_text) > 3500:
        keys_text = keys_text[:3500] + "\n…"

    await tg_retry(lambda: update.message.reply_text(
        f"✅ Создала ключи ({res['plan']}, {res['count']} шт):\n\n{keys_text}"
    ))


async def revoke_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not _is_admin(update):
        await tg_retry(lambda: update.message.reply_text("Нет прав."))
        return
    if not context.args:
        await tg_retry(lambda: update.message.reply_text("Формат: /revoke KR-XXXX-XXXX-XXXX"))
        return

    key = context.args[0].strip().upper()
    revoked_by = update.effective_user.id if update.effective_user else 0

    def _revoke():
        return get_license_mgr().revoke_key(key=key, revoked_by=revoked_by)

    res = await tg_retry(
        lambda: to_thread_timeout(_revoke, timeout=20, label="license.revoke.thread"),
        label="license.revoke.retry"
    )

    if not res.get("ok"):
        reason = res.get("reason")
        msg = {
            "empty_key": "Ключ пустой.",
            "not_found": "Ключ не найден.",
        }.get(reason, f"Не получилось отозвать ключ: {reason}")
        await tg_retry(lambda: update.message.reply_text(msg))
        return

    if res.get("already"):
        await tg_retry(lambda: update.message.reply_text("✅ Ключ уже был отозван."))
        return

    await tg_retry(
        lambda: update.message.reply_text(
            f"✅ Ключ отозван (status=revoked). Было: {res.get('was','')}"
        )
    )


# =========================================================
# EXAM FLOW
# =========================================================

def _split_text_chunks(text: str, limit: int = 3500) -> List[str]:
    if len(text) <= limit:
        return [text]
    lines = text.split("\n")
    out, cur = [], ""
    for ln in lines:
        if not cur:
            cur = ln
            continue
        if len(cur) + 1 + len(ln) <= limit:
            cur += "\n" + ln
        else:
            out.append(cur)
            cur = ln
    if cur:
        out.append(cur)
    return out


async def _exam_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = _chat_id_from_update(update)
    if chat_id is None:
        return

    pool = context.user_data.get("exam_pool") or []
    total = len(pool)
    score = int(context.user_data.get("exam_score", 0))
    points = calc_points_0_100(score, total)
    passed = points >= 60
    status = "🟢 <b>ВЫ ПРОШЛИ</b>" if passed else "🔴 <b>ВЫ НЕ ПРОШЛИ</b>"

    head = (
        "<b>Экзамен завершен</b>\n\n"
        f"<b>ВАШИ БАЛЛЫ:</b> {points}/100\n"
        f"{status}\n"
        f"<i>Правильных ответов:</i> {score} / {total}\n\n"
        "Разобрать ошибку:\n"
        "1) <code>/learn</code>\n"
        "2) <code>/goto Q123</code>\n"
    )

    try:
        await tg_retry(
            lambda: context.bot.send_message(chat_id=chat_id, text=head, parse_mode=ParseMode.HTML),
            label="exam_finish_head"
        )
    except Exception:
        pass

    results = context.user_data.get("exam_results") or []
    lines = []
    for i, r in enumerate(results, start=1):
        qid = html.escape(str(r.get("qid", "")))
        ok = bool(r.get("ok", False))
        mark = "✅" if ok else "❌"
        lines.append(f"{i:02d}. {mark} <code>{qid}</code>")

    list_text = "<b>Список вопросов экзамена:</b>\n" + ("\n".join(lines) if lines else "(нет истории)")
    for chunk in _split_text_chunks(list_text, limit=3500):
        await tg_retry(
            lambda: context.bot.send_message(chat_id=chat_id, text=chunk, parse_mode=ParseMode.HTML),
            label="exam_finish_list"
        )

    # отметить бесплатную попытку экзамена как использованную (если НЕ paid)
    uid = None
    if query and query.from_user:
        uid = query.from_user.id
    if uid:
        def _mark():
            mgr = get_license_mgr()
            if not mgr.is_active(uid):
                mgr.mark_trial_exam_used(uid)

        try:
            await tg_retry(
                lambda: to_thread_timeout(_mark, timeout=15, label="trial.exam_mark.thread"),
                label="trial.exam_mark.retry"
            )
        except Exception:
            pass


async def _advance_exam(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pool = context.user_data.get("exam_pool") or []
    idx = int(context.user_data.get("exam_idx", 0)) + 1
    if idx >= len(pool):
        await _exam_finish(update, context)
        return

    context.user_data["exam_idx"] = idx
    context.user_data["q"] = pool[idx]
    context.user_data["multi_sel"] = []
    context.user_data["multi_sel_order"] = []
    context.user_data["multi_reveal"] = []
    context.user_data["multi_locked"] = False
    await show_current(update, context)


# =========================================================
# CALLBACKS
# =========================================================

async def on_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    data = query.data or ""
    mode = context.user_data.get("mode")
    q = context.user_data.get("q", {}) or {}

    letters_str = get_letters(q)
    letters = set(letters_str)

    # ВАЖНО: отвечаем callback ровно один раз (с текстом — если надо)
    if data == "goto_help":
        await safe_answer(query, "Прыжок: /goto 888 или /goto Q888", show_alert=False)
        return
    else:
        await safe_answer(query)

    if data == "noop":
        return

    if data == "prev":
        if mode != "learn":
            return

        await ensure_loaded_async()
        pool = context.user_data.get("trial_pool") if context.user_data.get("trial") else QUESTIONS
        if not pool:
            return

        idx = int(context.user_data.get("learn_idx", 0)) - 1
        if idx < 0:
            idx = len(pool) - 1

        context.user_data["learn_idx"] = idx
        context.user_data["q"] = pool[idx]
        _reset_answer_state(context, "learn")
        await show_current(update, context, send_anchors=True)
        return

    if data.startswith("jump:"):
        if context.user_data.get("trial"):
            # уже answered; просто игнорим тихо
            return
        if mode != "learn":
            return

        await ensure_loaded_async()
        start_num_s = data.split(":", 1)[1].strip()
        qid = normalize_qid(start_num_s)
        if not qid:
            return
        idx = QINDEX.get(qid)
        if idx is None:
            return

        context.user_data["learn_idx"] = idx
        context.user_data["q"] = QUESTIONS[idx]
        _reset_answer_state(context, "learn")
        await show_current(update, context, send_anchors=True)
        return

    if data == "toggle_answer":
        if mode != "drill":
            return
        context.user_data["show_answer"] = not bool(context.user_data.get("show_answer", False))
        context.user_data["drill_reveal"] = None
        await show_current(update, context, prefix="<b>Интенсив</b>\n\n")
        return

    if data.startswith("pick:"):
        if mode not in {"learn", "drill", "exam"}:
            return
        if not q.get("multi"):
            return
        if bool(context.user_data.get("multi_locked", False)):
            return

        k = data.split(":", 1)[1]
        if k not in letters:
            return

        sel = set(context.user_data.get("multi_sel") or [])
        order = list(context.user_data.get("multi_sel_order") or [])

        if k in sel:
            sel.remove(k)
            order = [x for x in order if x != k]
        else:
            sel.add(k)
            order.append(k)

        if len(order) > 2:
            order = order[-2:]
            sel = set(order)

        context.user_data["multi_sel"] = list(sel)
        context.user_data["multi_sel_order"] = order

        # 👉 ВАЖНО: пересобираем ТОЛЬКО клавиатуру
        letters_str = get_letters(q)
        selected = set(context.user_data.get("multi_sel") or [])

        if mode == "learn":
            trial = bool(context.user_data.get("trial"))
            markup = kb_multi_learn(
                letters_str,
                selected,
                _learn_total_for_jump(context),
                trial=trial
            )
        elif mode == "drill":
            markup = kb_multi_drill(
                letters_str,
                selected,
                bool(context.user_data.get("show_answer", False))
            )
        else:  # exam
            markup = kb_multi_exam(letters_str, selected)

        await _edit_only_keyboard(update, context, markup)
        return

    if data == "done":
        if mode not in {"learn", "drill", "exam"}:
            return
        if not q.get("multi"):
            return

        sel = set(context.user_data.get("multi_sel") or [])
        if len(sel) < 2:
            return

        context.user_data["multi_reveal"] = list(sel)
        context.user_data["multi_locked"] = True

        correct = q_correct_set(q)
        ok = (sel == correct)

        if mode == "exam":
            qid = q.get("id", "")
            context.user_data.setdefault("exam_results", []).append({"qid": qid, "ok": ok})
            if ok:
                context.user_data["exam_score"] = int(context.user_data.get("exam_score", 0)) + 1
            await _advance_exam(update, context)
            return

        if mode == "learn":
            await show_current(update, context, send_anchors=True)
        else:
            await show_current(update, context, prefix="<b>Интенсив</b>\n\n")
        return

    if data == "next":
        await ensure_loaded_async()

        if mode == "learn":
            pool = context.user_data.get("trial_pool") if context.user_data.get("trial") else QUESTIONS
            if not pool:
                return

            idx = int(context.user_data.get("learn_idx", 0)) + 1

            if context.user_data.get("trial"):
                if idx >= len(pool):
                    # отметить дневной лимит как исчерпанный
                    today = kst_today_str()
                    uid = query.from_user.id if query.from_user else 0
                    if uid:
                        DAILY_TRIAL_EXHAUSTED[(uid, today)] = True

                    chat_id = _chat_id_from_update(update)
                    if chat_id is not None:
                        await tg_retry(
                            lambda: context.bot.send_message(
                                chat_id=chat_id,
                                text=(
                                    f"🔒 Бесплатные {TRIAL_QUESTIONS} вопросов на сегодня закончились. Завтра будут доступны еще 30.\n\n"
                                    + PRICING_BLOCK + "\n"
                                    "За расширенным доступом обратитесь к разработчику: @luv2win"
                                ),
                                parse_mode=ParseMode.HTML
                            ),
                            label="trial_end_msg"
                        )
                    return
            else:
                if idx >= len(pool):
                    idx = 0

            context.user_data["learn_idx"] = idx
            context.user_data["q"] = pool[idx]
            _reset_answer_state(context, "learn")
            await show_current(update, context, send_anchors=True)
            return

        if mode == "drill":
            pool = context.user_data.get("drill_pool") or []
            if not pool:
                return
            idx = int(context.user_data.get("drill_idx", 0)) + 1
            if idx >= len(pool):
                idx = 0
            context.user_data["drill_idx"] = idx
            context.user_data["q"] = pool[idx]
            _reset_answer_state(context, "drill")
            await show_current(update, context, prefix="<b>Интенсив</b>\n\n")
            return

        if mode == "exam":
            await _advance_exam(update, context)
            return

        return

    if data in set("ABCDE"):
        if data not in letters:
            return
        if q.get("multi"):
            return

        correct_set = q_correct_set(q)
        correct_single = next(iter(correct_set), None)

        if mode == "learn":
            context.user_data["learn_reveal"] = data
            await show_current(update, context, send_anchors=True)
            return

        if mode == "drill":
            context.user_data["drill_reveal"] = data
            await show_current(update, context, prefix="<b>Интенсив</b>\n\n")
            return

        if mode == "exam":
            ok = (data == correct_single)
            qid = q.get("id", "")
            context.user_data.setdefault("exam_results", []).append({"qid": qid, "ok": ok})
            if ok:
                context.user_data["exam_score"] = int(context.user_data.get("exam_score", 0)) + 1
            await _advance_exam(update, context)
            return


# =========================================================
# IMAGE UPLOAD (ADMIN)
# =========================================================

async def img_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update):
        await tg_retry(lambda: update.message.reply_text("Нет прав на загрузку картинок."))
        return

    context.user_data.pop("imgfrom_active", None)
    context.user_data.pop("imgfrom_next_num", None)
    context.user_data.pop("imgfrom_count", None)

    if not context.args:
        await tg_retry(lambda: update.message.reply_text("Формат: /img 888 или /img Q888"))
        return

    qid = normalize_qid(context.args[0])
    if not qid:
        await tg_retry(lambda: update.message.reply_text(
            "Неверно.\nПример: /img 888 или /img Q888"
        ))
        return

    context.user_data["upload_qid"] = qid
    await tg_retry(lambda: update.message.reply_text(f"Ок, жду картинку для {qid}."))


async def imgfrom_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update):
        await tg_retry(lambda: update.message.reply_text("Нет прав на загрузку картинок."))
        return

    context.user_data.pop("upload_qid", None)

    if not context.args:
        await tg_retry(lambda: update.message.reply_text("Формат: /imgfrom 888 (или /imgfrom Q888)"))
        return

    start_qid = normalize_qid(context.args[0])
    if not start_qid:
        await tg_retry(lambda: update.message.reply_text("Неверно. Пример: /imgfrom 888 или /imgfrom Q888"))
        return

    start_num = int(start_qid[1:])

    context.user_data["imgfrom_active"] = True
    context.user_data["imgfrom_next_num"] = start_num
    context.user_data["imgfrom_count"] = 0

    await tg_retry(lambda: update.message.reply_text(
        "Ок. Массовая загрузка включена ✅\n"
        f"Следующая картинка будет записана как Q{start_num}.\n"
        "Остановить: /imgstop\n"
        "⚠️ В batch бот не отвечает на каждую картинку."
    ))


async def imgstop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update):
        return
    count = int(context.user_data.get("imgfrom_count", 0) or 0)
    context.user_data.pop("imgfrom_active", None)
    context.user_data.pop("imgfrom_next_num", None)
    context.user_data.pop("imgfrom_count", None)
    context.user_data.pop("upload_qid", None)
    await tg_retry(lambda: update.message.reply_text(f"Массовая загрузка выключена. Сохранено: {count}"))


async def photo_upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not _is_admin(update):
        return

    caption = (update.message.caption or "").strip().upper()

    file_id = None
    file_name = None

    if update.message.photo:
        file_id = update.message.photo[-1].file_id
    elif (
        update.message.document
        and update.message.document.mime_type
        and update.message.document.mime_type.startswith("image/")
    ):
        file_id = update.message.document.file_id
        file_name = (update.message.document.file_name or "").upper()

    if not file_id:
        return

    qid = None
    if caption:
        qid = normalize_qid(caption)

    if not qid and file_name:
        m = re.match(r"^(Q\d+)\.", file_name)
        if m:
            qid = m.group(1)

    assigned_by_batch = False
    if not qid and context.user_data.get("imgfrom_active"):
        next_num = context.user_data.get("imgfrom_next_num")
        if isinstance(next_num, int) and next_num > 0:
            qid = f"Q{next_num}"
            context.user_data["imgfrom_next_num"] = next_num + 1
            assigned_by_batch = True

    if not qid:
        qid = context.user_data.get("upload_qid")

    if not qid:
        await tg_retry(lambda: update.message.reply_text("Не вижу номер вопроса. Сделай /img 888 или /imgfrom 888."))
        return

    def _set():
        cache = get_image_cache()
        cache.set(qid, file_id)
        return cache.get(qid)

    try:
        back = await tg_retry(
            lambda: to_thread_timeout(_set, timeout=15, label="image_cache.set.thread"),
            label="image_cache.set.retry"
        )
        if (not assigned_by_batch) and back != file_id:
            await tg_retry(lambda: update.message.reply_text("⚠️ Записал, но readback не совпал."))
            return
    except Exception as e:
        await tg_retry(lambda: update.message.reply_text(
            f"❌ Не смог записать/прочитать image_cache: {type(e).__name__}: {e}"
        ))
        return

    if context.user_data.get("upload_qid") == qid:
        context.user_data.pop("upload_qid", None)

    if assigned_by_batch:
        context.user_data["imgfrom_count"] = int(context.user_data.get("imgfrom_count", 0) or 0) + 1
        return

    await tg_retry(lambda: update.message.reply_text(f"✅ Картинка сохранена для {qid}"))


# =========================================================
# ERROR HANDLER
# =========================================================

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    err = context.error
    try:
        log.exception("ERROR: %r", err)

        if isinstance(err, HttpError):
            msg = str(err)
            if len(msg) > 1200:
                msg = msg[:1200] + "…"
            text = f"❌ Ошибка Google Sheets\n<pre>{html.escape(msg)}</pre>"
        else:
            msg = f"{type(err).__name__}: {err}"
            if len(msg) > 1200:
                msg = msg[:1200] + "…"
            text = f"❌ Ошибка\n<pre>{html.escape(msg)}</pre>"

        if update and hasattr(update, "effective_chat") and update.effective_chat:
            await tg_retry(
                lambda: context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=text,
                    parse_mode=ParseMode.HTML
                ),
                label="send_error_to_chat"
            )
    except Exception:
        pass


# =========================================================
# STARTUP / MAIN
# =========================================================

async def on_startup(app: Application):
    await app.bot.delete_webhook(drop_pending_updates=True)
    me = await app.bot.get_me()
    log.info("BOT STARTED: @%s id=%s", me.username, me.id)
    log.info("SHEET_ID: %s", SHEET_ID)
    log.info("SHEET_NAME (desired): %s", SHEET_NAME)
    try:
        titles = await to_thread_timeout(_get_spreadsheet_sheet_titles, timeout=15, label="sheets.meta_titles")
        log.info("SHEETS (actual): %s", titles)
    except Exception as e:
        log.warning("SHEETS meta error: %r", e)


def main():
    threading.Thread(target=run_http, daemon=True).start()
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .build()
    )

# --- Railway HTTP keep-alive (Web Service requirement) ---
    threading.Thread(
        target=run_http,
        daemon=True
    ).start()

    app.add_handler(CommandHandler("start", start))

    app.add_handler(CommandHandler("learn", learn))
    app.add_handler(CommandHandler("drill", drill))
    app.add_handler(CommandHandler("exam", exam))
    app.add_handler(CommandHandler("goto", goto))

    app.add_handler(CommandHandler("redeem", redeem_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("mkkeys", mkkeys_cmd))
    app.add_handler(CommandHandler("revoke", revoke_cmd))

    app.add_handler(CommandHandler("img", img_cmd))
    app.add_handler(CommandHandler("imgfrom", imgfrom_cmd))
    app.add_handler(CommandHandler("imgstop", imgstop_cmd))

    app.add_handler(MessageHandler(filters.PHOTO, photo_upload_handler))
    app.add_handler(MessageHandler(filters.Document.IMAGE, photo_upload_handler))

    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_error_handler(on_error)

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
