#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==========================================================
#  multi_pairs_bot_v3_PRO.py
#
#  ✅ الإصلاحات السابقة (14 إصلاح) — محتفظ بها كاملة
#  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🆕 FEATURE #1 — News Filter
#       • جلب تقويم ForexFactory كل ساعة (JSON مجاني)
#       • تجميد التداول 30 دق قبل + 15 دق بعد أي خبر HIGH
#       • إغلاق الصفقات تلقائياً قبل NFP/FOMC/CPI بـ60 دق
#       • كاش محلي لتجنب الضغط على API
#
#  🆕 FEATURE #2 — Multi-Timeframe Confirmation (H1)
#       • Supertrend H1 + EMA50 كفلتر اتجاه كبير
#       • M15 Flip + H1 نفس الاتجاه  ✅ → إشارة قوية
#       • M15 Flip + H1 عكس الاتجاه  ❌ → تجاهل تام
#       • H1 محايد → تداول بحجم 60%
#
#  🆕 FEATURE #3 — Market Regime Detection (ADX)
#       • ADX >= 25 → TRENDING  → تداول بحجم كامل ✅
#       • ADX 15-25 → WEAK      → تداول بحجم 50% ⚠️
#       • ADX < 15  → RANGING   → لا تداول إطلاقاً 🚫
#       • +DI / -DI لتأكيد اتجاه الزخم
# ==========================================================

import os, csv, time, math, sqlite3, requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone, timedelta
from threading import Lock
from dotenv import load_dotenv

load_dotenv()

# ═══════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════
API_KEY    = os.getenv('CAPITAL_API_KEY',  'BbmFhEF3FffkcR0Y')
EMAIL      = os.getenv('CAPITAL_EMAIL',    'almorese2013@gmail.com')
PASSWORD   = os.getenv('CAPITAL_PASSWORD', 'Ba050326>')
TG_TOKEN   = os.getenv('TG_TOKEN',         '8782238258:AAEtuQg7OYAmoemhWfLqKdYpqIxfWwyKRSQ')
TG_CHAT_ID = os.getenv('TG_CHAT_ID',       '533243705')


BASE_URL  = 'https://api-capital.backend-capital.com'
DEMO_MODE = os.getenv('DEMO_MODE', 'false').lower() == 'true'

PAIRS = {
    'GOLD':   {'epic': 'GOLD',   'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'BTCUSD': {'epic': 'BTCUSD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'EURUSD': {'epic': 'EURUSD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'GBPUSD': {'epic': 'GBPUSD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'US100':  {'epic': 'US100',  'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'US500':  {'epic': 'US500',  'allow_buy': True, 'allow_sell': True, 'size_override': None},
}

STRATEGY_TF   = 'MINUTE_15'
HTF_TF        = 'HOUR'          # الإطار العلوي للـ MTF
CANDLES_COUNT = 500
HTF_COUNT     = 150
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '180'))
M15_MINUTES   = 15

# ═══════════════════════════════════════════════════════
# SUPERTREND
# ═══════════════════════════════════════════════════════
SUPERTREND_PERIOD = int(os.getenv('SUPERTREND_PERIOD', '10'))
SUPERTREND_MULT   = float(os.getenv('SUPERTREND_MULT', '3.0'))
ATR_METHOD        = os.getenv('ATR_METHOD', 'RMA')

EMA_PERIOD     = 20
HTF_EMA_PERIOD = 50        # EMA للفريم العلوي H1
ATR_PERIOD     = 14
SL_ATR_MULT    = 1.5
TP_ATR_MULT    = 3.0
SPREAD_ATR_MAX = 0.25

# ═══════════════════════════════════════════════════════
# 🆕 FEATURE #3 — ADX SETTINGS
# ═══════════════════════════════════════════════════════
ADX_PERIOD           = int(os.getenv('ADX_PERIOD', '14'))
ADX_TRENDING_MIN     = float(os.getenv('ADX_TRENDING_MIN', '25'))
ADX_WEAK_MIN         = float(os.getenv('ADX_WEAK_MIN', '15'))
ADX_WEAK_SIZE_FACTOR = 0.5   # تقليل الحجم 50% في الترند الضعيف

# ═══════════════════════════════════════════════════════
# 🆕 FEATURE #1 — NEWS FILTER SETTINGS
# ═══════════════════════════════════════════════════════
NEWS_FILTER_ENABLED  = os.getenv('NEWS_FILTER', 'true').lower() == 'true'
NEWS_PRE_MINUTES     = int(os.getenv('NEWS_PRE_MIN',  '30'))
NEWS_POST_MINUTES    = int(os.getenv('NEWS_POST_MIN', '15'))
NEWS_CACHE_MINUTES   = 60

# الأخبار التي تُغلق الصفقات المفتوحة تلقائياً (NFP / FOMC / CPI ...)
CRITICAL_NEWS_KEYWORDS = [
    'non-farm', 'nfp', 'fomc', 'federal reserve', 'interest rate decision',
    'cpi', 'inflation', 'gdp', 'unemployment rate', 'payrolls',
    'ecb', 'bank of england', 'boe', 'fed chair',
]

# العملات المؤثرة على كل زوج
CURRENCY_PAIR_MAP = {
    'USD': ['GOLD', 'EURUSD', 'GBPUSD', 'US100', 'US500', 'BTCUSD'],
    'EUR': ['EURUSD'],
    'GBP': ['GBPUSD'],
    'XAU': ['GOLD'],
}

# ═══════════════════════════════════════════════════════
# RISK MANAGEMENT
# ═══════════════════════════════════════════════════════
BASE_RISK_PERCENT   = 0.01
MAX_RISK_PERCENT    = 0.03
MIN_RISK_PERCENT    = 0.005
MAX_DAILY_RISK      = 0.05
MAX_WEEKLY_RISK     = 0.10
DAILY_PROFIT_TARGET = 0.03

SESSIONS = {
    'ASIA':        {'start': 0,  'end': 7,  'risk_mult': 0.5, 'name': 'آسيا (هادئ)'},
    'LONDON_OPEN': {'start': 7,  'end': 10, 'risk_mult': 1.2, 'name': 'فتح لندن'},
    'LONDON_MID':  {'start': 10, 'end': 12, 'risk_mult': 1.0, 'name': 'منتصف لندن'},
    'LONDON_NY':   {'start': 12, 'end': 16, 'risk_mult': 1.5, 'name': 'تداخل لندن-نيويورك ⭐'},
    'NY_PM':       {'start': 16, 'end': 20, 'risk_mult': 0.7, 'name': 'بعد الظهر الأمريكي'},
    'QUIET':       {'start': 20, 'end': 24, 'risk_mult': 0.3, 'name': 'هادئ'},
}

VOLATILITY_THRESHOLDS   = {'EXTREME': 2.0, 'HIGH': 1.5, 'LOW': 0.6}
MAX_TRADE_DURATION_BARS = 24
TRAILING_START_R        = 2.5
TRAILING_ATR_MULT       = 1.0

STAGE1_TP_R, STAGE1_PCT = 1.5, 0.50
STAGE2_TP_R, STAGE2_PCT = 2.5, 0.30
FINAL_TP_R,  FINAL_PCT  = 3.5, 0.50

PROGRESSIVE_LOCK = {2.0: 0.5, 2.5: 1.0, 3.0: 1.5, 3.5: 2.0, 4.5: 3.0, 6.0: 4.0}

MAX_OPEN_TRADES      = int(os.getenv('MAX_OPEN_TRADES', '6'))
MAX_CONSECUTIVE_LOSS = int(os.getenv('MAX_CONSEC_LOSS', '3'))
ACCOUNT_BALANCE      = float(os.getenv('ACCOUNT_BALANCE', '1000'))

_BASE_DIR  = os.getenv('DATA_DIR', os.path.dirname(os.path.abspath(__file__)))
DB_FILE    = os.path.join(_BASE_DIR, 'multi_bot.db')
TRADES_CSV = os.path.join(_BASE_DIR, 'trades_log.csv')

db_lock, session_headers = Lock(), {}
_meta_cache   = {}
_candle_cache = {}

# كاش الأخبار
_news_cache = {'events': [], 'last_update': None}

CSV_HEADERS = [
    'date', 'time_utc', 'pair', 'direction', 'entry', 'sl', 'tp', 'exit_price',
    'atr', 'size', 'sl_dist', 'pnl_usd', 'pnl_r', 'result', 'bars_held', 'spread', 'tf',
    'stage1_done', 'stage2_done', 'stage3_done', 'final_locked_r', 'exit_type',
    'session_used', 'risk_percent',
    'adx_val', 'adx_regime', 'htf_dir',  # columns جديدة
    'indicator', 'notes'
]


# ═══════════════════════════════════════════════════════
# VALIDATION
# ═══════════════════════════════════════════════════════

def validate_config():
    missing = []
    if not API_KEY:  missing.append('CAPITAL_API_KEY')
    if not EMAIL:    missing.append('CAPITAL_EMAIL')
    if not PASSWORD: missing.append('CAPITAL_PASSWORD')
    if missing:
        raise EnvironmentError(
            f'❌ مفقود: {", ".join(missing)} — أضفها في .env'
        )
    log('✅ Config OK')


# ═══════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════

def _migrate_database(conn):
    try:
        t_cols = {c[1] for c in conn.execute("PRAGMA table_info(trades)").fetchall()}
        for col, typ in {
            'pnl_r': 'REAL DEFAULT 0', 'pnl_usd': 'REAL DEFAULT 0',
            'exit_price': 'REAL', 'bars_held': 'INTEGER DEFAULT 0',
            'risk_percent': 'REAL DEFAULT 0',
            'adx_val': 'REAL DEFAULT 0', 'adx_regime': 'TEXT DEFAULT ""',
            'htf_dir': 'INTEGER DEFAULT 0',
        }.items():
            if col not in t_cols:
                conn.execute(f'ALTER TABLE trades ADD COLUMN {col} {typ}')
                log(f'  ✅ DB: added trades.{col}')

        op_cols = {c[1] for c in conn.execute("PRAGMA table_info(open_positions)").fetchall()}
        for col, typ in {
            'last_trail_r': 'REAL DEFAULT 0',
            'opened_at':    'TEXT',
        }.items():
            if col not in op_cols:
                conn.execute(f'ALTER TABLE open_positions ADD COLUMN {col} {typ}')
                log(f'  ✅ DB: added open_positions.{col}')
    except Exception as ex:
        log(f'  ⚠️ migrate: {ex}')


def db_init():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id             INTEGER PRIMARY KEY,
            key            TEXT UNIQUE,
            pair           TEXT,
            direction      TEXT,
            timestamp      TEXT,
            entry          REAL,
            sl             REAL,
            tp             REAL,
            atr            REAL,
            size           REAL,
            spread         REAL DEFAULT 0,
            status         TEXT DEFAULT 'PENDING',
            stage1_done    INTEGER DEFAULT 0,
            stage2_done    INTEGER DEFAULT 0,
            stage3_done    INTEGER DEFAULT 0,
            final_locked_r REAL DEFAULT 0,
            exit_type      TEXT,
            session_used   TEXT,
            risk_percent   REAL DEFAULT 0,
            pnl_r          REAL DEFAULT 0,
            pnl_usd        REAL DEFAULT 0,
            exit_price     REAL,
            bars_held      INTEGER DEFAULT 0,
            adx_val        REAL DEFAULT 0,
            adx_regime     TEXT DEFAULT "",
            htf_dir        INTEGER DEFAULT 0
        )''')
        _migrate_database(conn)
        conn.execute('''CREATE TABLE IF NOT EXISTS open_positions (
            deal_id        TEXT PRIMARY KEY,
            pair           TEXT,
            direction      TEXT,
            entry          REAL,
            sl             REAL,
            tp             REAL,
            atr            REAL,
            size           REAL,
            db_key         TEXT,
            opened_at      TEXT,
            stage1_done    INTEGER DEFAULT 0,
            stage2_done    INTEGER DEFAULT 0,
            stage3_done    INTEGER DEFAULT 0,
            final_locked_r REAL DEFAULT 0,
            bars_held      INTEGER DEFAULT 0,
            last_trail_r   REAL DEFAULT 0
        )''')
        conn.commit()


def db_save(key, pair, direction, entry, sl, tp, atr, size, spread,
            risk_pct, session, adx_val=0.0, adx_regime='', htf_dir=0):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT INTO trades (key,pair,direction,timestamp,entry,sl,tp,atr,size,'
                    'spread,risk_percent,session_used,adx_val,adx_regime,htf_dir) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (key, pair, direction, utc_now(), entry, sl, tp, atr, size,
                     spread, risk_pct, session, round(adx_val, 2), adx_regime, htf_dir)
                )
                conn.commit()
            except sqlite3.IntegrityError:
                pass


def db_update(key, status, exit_type=None):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            if exit_type:
                conn.execute('UPDATE trades SET status=?,exit_type=? WHERE key=?',
                             (status, exit_type, key))
            else:
                conn.execute('UPDATE trades SET status=? WHERE key=?', (status, key))
            conn.commit()


def db_is_dup(key):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            return conn.execute('SELECT id FROM trades WHERE key=?', (key,)).fetchone() is not None


def db_get_recent_trades(pair=None, limit=20):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            q = "SELECT pair,direction,status,pnl_r,timestamp FROM trades WHERE status IN ('WIN','LOSS')"
            p = []
            if pair:
                q += ' AND pair=?'; p.append(pair)
            q += ' ORDER BY id DESC LIMIT ?'; p.append(limit)
            return conn.execute(q, p).fetchall()


def _update_trade_pnl(db_key, pnl_r, pnl_usd, exit_price, bars_held):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'UPDATE trades SET pnl_r=?,pnl_usd=?,exit_price=?,bars_held=? WHERE key=?',
                    (pnl_r, pnl_usd, exit_price, bars_held, db_key)
                )
                conn.commit()
            except Exception as ex:
                log(f'  ⚠️ PnL update: {ex}')


def op_save(deal_id, pair, direction, entry, sl, tp, atr, size, db_key):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT OR IGNORE INTO open_positions '
                    '(deal_id,pair,direction,entry,sl,tp,atr,size,db_key,opened_at) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (deal_id, pair, direction, entry, sl, tp, atr, size, db_key, utc_now())
                )
                conn.commit()
            except Exception as ex:
                log(f'op_save: {ex}')


def op_get_all():
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(r) for r in conn.execute('SELECT * FROM open_positions').fetchall()]


def op_update(deal_id, **kwargs):
    ALLOWED = {'sl','tp','stage1_done','stage2_done','stage3_done',
               'final_locked_r','bars_held','last_trail_r'}
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            for col, val in kwargs.items():
                if col in ALLOWED:
                    conn.execute(f'UPDATE open_positions SET {col}=? WHERE deal_id=?',
                                 (val, deal_id))
            conn.commit()


def op_delete(deal_id):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('DELETE FROM open_positions WHERE deal_id=?', (deal_id,))
            conn.commit()


def op_get_open_pairs():
    return {p['pair'] for p in op_get_all()}


# ═══════════════════════════════════════════════════════
# 🆕 FEATURE #1 — NEWS FILTER
# ═══════════════════════════════════════════════════════

def _fetch_forexfactory_calendar() -> list:
    """
    ForexFactory JSON API (مجاني بدون مفتاح).
    يُعيد قائمة أحداث HIGH/MEDIUM لهذا الأسبوع.
    """
    try:
        url = 'https://nfs.faireconomy.media/ff_calendar_thisweek.json'
        r   = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            log(f'  ⚠️ News API HTTP {r.status_code}')
            return []

        events = []
        for item in r.json():
            impact = item.get('impact', '').lower()
            if impact not in ('high', 'medium'):
                continue
            try:
                raw    = item['date']   # "01-17-2025 08:30am"
                dt     = datetime.strptime(raw, '%m-%d-%Y %I:%M%p')
                # تحويل EST/EDT → UTC
                offset = 4 if 3 <= dt.month <= 10 else 5
                dt_utc = dt.replace(tzinfo=timezone.utc) + timedelta(hours=offset)
                events.append({
                    'time_utc': dt_utc,
                    'currency': item.get('currency', '').upper(),
                    'impact':   impact,
                    'title':    item.get('title', '').lower(),
                })
            except Exception:
                continue

        log(f'  📰 News loaded: {len(events)} أحداث HIGH/MEDIUM')
        return events
    except Exception as ex:
        log(f'  ⚠️ News fetch error: {ex}')
        return []


def _refresh_news_cache():
    """تحديث الكاش كل NEWS_CACHE_MINUTES دقيقة فقط"""
    now  = datetime.now(timezone.utc)
    last = _news_cache['last_update']
    if last and (now - last).total_seconds() < NEWS_CACHE_MINUTES * 60:
        return
    _news_cache['events']      = _fetch_forexfactory_calendar()
    _news_cache['last_update'] = now


def is_news_blocking(pair: str):
    """
    هل يوجد خبر HIGH/MEDIUM يؤثر على هذا الزوج خلال النافذة الزمنية؟
    Returns: (True, reason) أو (False, 'OK')
    """
    if not NEWS_FILTER_ENABLED:
        return False, 'OK'

    _refresh_news_cache()
    now = datetime.now(timezone.utc)

    for ev in _news_cache['events']:
        ccy = ev['currency']
        if pair not in CURRENCY_PAIR_MAP.get(ccy, []):
            continue

        mins_to   = (ev['time_utc'] - now).total_seconds() / 60
        mins_from = (now - ev['time_utc']).total_seconds() / 60

        in_pre_window  = 0 < mins_to <= NEWS_PRE_MINUTES
        in_post_window = 0 <= mins_from <= NEWS_POST_MINUTES

        if in_pre_window or in_post_window:
            when = f'بعد {int(mins_to)} دق' if in_pre_window else f'منذ {int(mins_from)} دق'
            return True, f'📰 {ccy} [{ev["impact"].upper()}] {ev["title"][:35]} — {when}'

    return False, 'OK'


def is_critical_news_imminent(pair: str) -> bool:
    """
    هل خبر ⭐⭐⭐ (NFP/FOMC/CPI) قادم خلال 60 دقيقة؟
    إذا نعم → أغلق الصفقات المفتوحة الآن.
    """
    if not NEWS_FILTER_ENABLED:
        return False
    _refresh_news_cache()
    now = datetime.now(timezone.utc)

    for ev in _news_cache['events']:
        if ev['impact'] != 'high':
            continue
        if not any(kw in ev['title'] for kw in CRITICAL_NEWS_KEYWORDS):
            continue
        ccy = ev['currency']
        if pair not in CURRENCY_PAIR_MAP.get(ccy, []):
            continue
        mins_to = (ev['time_utc'] - now).total_seconds() / 60
        if 0 <= mins_to <= 60:
            log(f'  ⚠️ خبر حرج قادم: "{ev["title"][:40]}" — {int(mins_to)} دق')
            return True
    return False


# ═══════════════════════════════════════════════════════
# RISK MANAGEMENT
# ═══════════════════════════════════════════════════════

def calculate_pnl_since(since_dt):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT pnl_usd FROM trades WHERE timestamp >= ? AND status IN ('WIN','LOSS')",
                (since_dt.strftime('%Y-%m-%d %H:%M UTC'),)
            ).fetchall()
            return sum(r[0] for r in rows if r[0] is not None)


def check_drawdown_limits():
    now     = datetime.now(timezone.utc)
    balance = get_current_balance()
    if balance <= 0:
        return False, '⚠️ Balance=0', 0.0

    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_pnl   = calculate_pnl_since(day_start)
    if day_pnl <= -balance * MAX_DAILY_RISK:
        return False, f'🛑 DAILY LIMIT: {day_pnl/balance:.1%}', day_pnl
    if day_pnl >= balance * DAILY_PROFIT_TARGET:
        return False, f'🔒 DAILY TARGET: +{day_pnl/balance:.1%}', day_pnl

    week_start = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )
    week_pnl = calculate_pnl_since(week_start)
    if week_pnl <= -balance * MAX_WEEKLY_RISK:
        return False, f'🛑 WEEKLY LIMIT: {week_pnl/balance:.1%}', day_pnl

    return True, 'OK', day_pnl


def get_pair_stats(pair, lookback=20):
    trades = db_get_recent_trades(pair, lookback)
    if len(trades) < 5:
        return None
    wins   = [t for t in trades if t[2] == 'WIN']
    losses = [t for t in trades if t[2] == 'LOSS']
    consec = 0
    for t in trades:
        if t[2] == 'LOSS': consec += 1
        else:               break
    return {
        'win_rate':           len(wins) / len(trades),
        'consecutive_losses': consec,
    }


def calculate_dynamic_risk(pair, base_risk=BASE_RISK_PERCENT):
    stats = get_pair_stats(pair)
    if not stats:
        return base_risk, 'default'
    risk = base_risk
    if   stats['consecutive_losses'] >= 3: risk *= 0.4
    elif stats['consecutive_losses'] == 2: risk *= 0.6
    return max(MIN_RISK_PERCENT, min(risk, MAX_RISK_PERCENT)), 'dynamic'


# ═══════════════════════════════════════════════════════
# SESSION & VOLATILITY
# ═══════════════════════════════════════════════════════

def get_session_info():
    hour = datetime.now(timezone.utc).hour
    for name, cfg in SESSIONS.items():
        if cfg['start'] <= hour < cfg['end']:
            return cfg['risk_mult'], cfg['name'], name
    return 0.0, 'مغلقة', 'CLOSED'


def check_volatility_regime(epic):
    df = fetch_candles(epic, STRATEGY_TF, 100)
    if df.empty or len(df) < 50:
        return 'NORMAL', 1.0
    atr_now  = calc_atr_series(df.iloc[:-1],  ATR_PERIOD).iloc[-1]
    atr_hist = calc_atr_series(df.iloc[:-20], ATR_PERIOD).iloc[-20:].mean()
    ratio = atr_now / atr_hist if atr_hist > 0 else 1.0
    if   ratio > VOLATILITY_THRESHOLDS['EXTREME']: return 'EXTREME', 0.0
    elif ratio > VOLATILITY_THRESHOLDS['HIGH']:    return 'HIGH',    0.7
    elif ratio < VOLATILITY_THRESHOLDS['LOW']:     return 'LOW',     0.6
    else:                                           return 'NORMAL',  1.0


def should_trade():
    allowed, reason, day_pnl = check_drawdown_limits()
    if not allowed:
        return False, reason, 0.0, None, 0.0
    session_mult, session_name, _ = get_session_info()
    if session_mult == 0:
        return False, f'⏸ {session_name}', 0.0, None, 0.0
    return True, 'OK', session_mult, session_name, day_pnl


# ═══════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════

def utc_now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')


def log(msg):
    print(f'[{utc_now()}] {msg}', flush=True)


def csv_init():
    if not Path(TRADES_CSV).exists():
        with open(TRADES_CSV, 'w', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()


def csv_log_trade(pos, exit_price, stage1=0, stage2=0, stage3=0,
                  final_r=0, exit_type=''):
    try:
        entry, sl, size = pos['entry'], pos['sl'], pos['size']
        dir_, pair      = pos['direction'], pos['pair']
        sl_dist = abs(entry - sl)
        pnl_pts = (exit_price - entry) if dir_ == 'BUY' else (entry - exit_price)
        pnl_r   = round(pnl_pts / sl_dist, 2) if sl_dist > 0 else 0
        result  = 'WIN' if pnl_pts > 0 else ('LOSS' if pnl_pts < 0 else 'BE')
        meta    = _meta_cache.get(pair, {}).get('data')
        cs      = meta[3] if meta else 1.0
        pnl_usd = round(pnl_pts * size * cs, 2)

        bars_held = 0
        opened_at = pos.get('opened_at', '')
        if opened_at:
            try:
                odt = datetime.strptime(opened_at, '%Y-%m-%d %H:%M UTC').replace(tzinfo=timezone.utc)
                bars_held = int((datetime.now(timezone.utc) - odt).total_seconds() / 60 / M15_MINUTES)
            except Exception:
                bars_held = pos.get('bars_held', 0)

        _update_trade_pnl(pos['db_key'], pnl_r, pnl_usd, exit_price, bars_held)

        now = datetime.now(timezone.utc)
        row = {
            'date': now.strftime('%Y-%m-%d'), 'time_utc': now.strftime('%H:%M'),
            'pair': pair, 'direction': dir_,
            'entry': entry, 'sl': sl, 'tp': pos['tp'], 'exit_price': exit_price,
            'atr': pos['atr'], 'size': size, 'sl_dist': round(sl_dist, 5),
            'pnl_usd': pnl_usd, 'pnl_r': pnl_r, 'result': result,
            'bars_held': bars_held, 'spread': pos.get('spread', 0),
            'tf': STRATEGY_TF, 'stage1_done': stage1, 'stage2_done': stage2,
            'stage3_done': stage3, 'final_locked_r': final_r, 'exit_type': exit_type,
            'session_used': pos.get('session_used', ''),
            'risk_percent': pos.get('risk_percent', 0),
            'adx_val':    pos.get('adx_val', 0),
            'adx_regime': pos.get('adx_regime', ''),
            'htf_dir':    pos.get('htf_dir', 0),
            'indicator':  f'ST({SUPERTREND_PERIOD},{SUPERTREND_MULT}|{ATR_METHOD})'
                          f'+EMA{EMA_PERIOD}+ADX{ADX_PERIOD}+H1MTF',
            'notes': ''
        }
        with open(TRADES_CSV, 'a', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writerow(row)

        icon = '✅' if result == 'WIN' else ('❌' if result == 'LOSS' else '🔵')
        log(f'  {icon} {pair} {dir_} | ${pnl_usd:+.2f} ({pnl_r:+.2f}R) | {bars_held} bars')
        nl = '\n'
        tg(f'{icon} *{pair} {dir_}*{nl}`${pnl_usd:+.2f}` | `{pnl_r:+.2f}R`{nl}_{utc_now()}_')
        return result, pnl_usd
    except Exception as ex:
        log(f'  csv_log ERROR: {ex}')
        return 'ERROR', 0


# ═══════════════════════════════════════════════════════
# API HELPERS
# ═══════════════════════════════════════════════════════

def _get(path, params=None, retries=3):
    for i in range(retries):
        try:
            r = requests.get(BASE_URL + path, headers=session_headers,
                             params=params, timeout=15)
            if r.status_code == 429:
                time.sleep(5 * (i + 1)); continue
            return r
        except requests.exceptions.RequestException as ex:
            log(f'  GET {path} [{i+1}]: {ex}')
            time.sleep(3 * (i + 1))
    return None


def _post(path, body, retries=2):
    for i in range(retries):
        try:
            return requests.post(BASE_URL + path, headers=session_headers,
                                 json=body, timeout=15)
        except requests.exceptions.RequestException as ex:
            log(f'  POST {path} [{i+1}]: {ex}')
            time.sleep(3 * (i + 1))
    return None


def _put(path, body, retries=3):
    for i in range(retries):
        try:
            r = requests.put(BASE_URL + path, headers=session_headers,
                             json=body, timeout=10)
            if r.status_code == 429:
                time.sleep(5 * (i + 1)); continue
            return r
        except Exception as ex:
            log(f'  PUT {path} [{i+1}]: {ex}')
            time.sleep(3 * (i + 1))
    return None


def _delete(path, body=None, retries=3):
    for i in range(retries):
        try:
            r = requests.delete(BASE_URL + path, headers=session_headers,
                                json=body, timeout=15)
            if r.status_code == 429:
                time.sleep(5 * (i + 1)); continue
            return r
        except Exception as ex:
            log(f'  DELETE {path} [{i+1}]: {ex}')
            time.sleep(3 * (i + 1))
    return None


def create_session():
    url  = BASE_URL + '/api/v1/session'
    hdrs = {'X-CAP-API-KEY': API_KEY, 'Content-Type': 'application/json'}
    try:
        r = requests.post(url, headers=hdrs,
                          json={'identifier': EMAIL, 'password': PASSWORD,
                                'encryptedPassword': False}, timeout=15)
        if r.status_code == 200:
            session_headers.update({
                'X-SECURITY-TOKEN': r.headers.get('X-SECURITY-TOKEN'),
                'CST':              r.headers.get('CST'),
                'Content-Type':     'application/json',
            })
            log('✅ Session OK')
            return True
        log(f'❌ Session FAILED {r.status_code}')
    except Exception as ex:
        log(f'❌ Session: {ex}')
    return False


def ping_session():
    r = _get('/api/v1/ping')
    if not r or r.status_code != 200:
        log('⚠️ Ping failed')
        return False
    return True


def get_current_balance():
    global ACCOUNT_BALANCE
    r = _get('/api/v1/accounts')
    if r and r.status_code == 200:
        accs = r.json().get('accounts', [])
        if accs:
            ACCOUNT_BALANCE = float(
                accs[0].get('balance', {}).get('available', ACCOUNT_BALANCE)
            )
    return ACCOUNT_BALANCE


def get_open_positions():
    r = _get('/api/v1/positions')
    return r.json().get('positions', []) if r and r.status_code == 200 else []


def get_instrument_meta(epic):
    now    = time.time()
    cached = _meta_cache.get(epic)
    if cached and (now - cached['ts']) < 300:
        return cached['data']
    r = _get(f'/api/v1/markets/{epic}')
    if not r or r.status_code != 200:
        return 0.0, 0.0, 0.0, 100.0, 0.1, 1000.0
    data = r.json()
    snap = data.get('snapshot', {})
    inst = data.get('instrument', {})
    deal = data.get('dealingRules', {})
    bid  = float(snap.get('bid', 0) or 0)
    ask  = float(snap.get('offer', 0) or 0)
    res  = (bid, ask, round(ask - bid, 5),
            float(inst.get('contractSize', 100) or 100),
            float((deal.get('minDealSize') or {}).get('value', 0.1) or 0.1),
            float((deal.get('maxDealSize') or {}).get('value', 1000) or 1000))
    _meta_cache[epic] = {'ts': now, 'data': res}
    return res


def get_current_price(epic):
    meta = get_instrument_meta(epic)
    return (meta[0] + meta[1]) / 2 if meta[0] > 0 else 0


def get_closed_deal_price(deal_id, fallback):
    try:
        r = _get('/api/v1/history/activity',
                 params={'dealId': deal_id, 'pageSize': 10})
        if r and r.status_code == 200:
            for act in r.json().get('activities', []):
                for action in act.get('details', {}).get('actions', []):
                    if action.get('actionType') == 'POSITION_CLOSED':
                        lvl = action.get('level') or action.get('dealPrice')
                        if lvl: return float(lvl)
    except Exception:
        pass
    return fallback


def update_sl_api(deal_id, new_sl, tp):
    r = _put(f'/api/v1/positions/{deal_id}', {'stopLevel': new_sl, 'profitLevel': tp})
    if r and r.status_code == 200:
        log(f'  ✅ SL→{new_sl}'); return True
    log(f'  ⚠️ SL fail: {r.status_code if r else "None"}')
    return False


def close_partial_api(deal_id, size):
    r = _delete(f'/api/v1/positions/{deal_id}', body={'dealSize': size})
    if r and r.status_code == 200:
        log(f'  💰 Partial: {size}'); return True
    log(f'  ⚠️ Partial fail: {r.status_code if r else "None"}')
    return False


def close_full_api(deal_id):
    r = _delete(f'/api/v1/positions/{deal_id}')
    return r and r.status_code == 200


# ═══════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════

def tg(text):
    if not TG_TOKEN: return
    try:
        requests.post(f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
                      data={'chat_id': TG_CHAT_ID, 'text': text,
                            'parse_mode': 'Markdown'}, timeout=10)
    except Exception:
        pass


def tg_signal(sig, session_info):
    icon    = '🟢' if sig['direction'] == 'BUY' else '🔴'
    mode    = 'DEMO' if DEMO_MODE else 'LIVE'
    adx_ic  = '🔥' if sig['adx_regime'] == 'TRENDING' else '〰️'
    htf_ic  = '⬆️' if sig['htf_dir'] == 1 else ('⬇️' if sig['htf_dir'] == -1 else '↔️')
    nl = '\n'
    tg(f'{icon} *{sig["pair"]} {sig["direction"]}* [{mode}]{nl}'
       f'Entry:`{sig["entry"]}` SL:`{sig["sl"]}` TP:`{sig["tp"]}`{nl}'
       f'Risk:`{sig["risk_percent"]:.2%}`{nl}'
       f'ADX:`{sig["adx_val"]:.1f}` {adx_ic}`{sig["adx_regime"]}` | H1:{htf_ic}{nl}'
       f'Session:`{session_info}`{nl}'
       f'_{utc_now()}_')


# ═══════════════════════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════════════════════

def fetch_candles(epic, resolution, count=500):
    ck  = f'{epic}_{resolution}'
    now = time.time()
    if ck in _candle_cache:
        c = _candle_cache[ck]
        if now - c['ts'] < 60:
            return c['df']
    r = _get(f'/api/v1/prices/{epic}',
             params={'resolution': resolution, 'max': count})
    if not r or r.status_code != 200:
        _candle_cache.pop(ck, None); return pd.DataFrame()
    prices = r.json().get('prices', [])
    if len(prices) < SUPERTREND_PERIOD * 3 + ATR_PERIOD:
        return pd.DataFrame()
    try:
        rows = [{
            'time':  p['snapshotTimeUTC'],
            'open':  (p['openPrice']['bid']  + p['openPrice']['ask'])  / 2,
            'high':  (p['highPrice']['bid']  + p['highPrice']['ask'])  / 2,
            'low':   (p['lowPrice']['bid']   + p['lowPrice']['ask'])   / 2,
            'close': (p['closePrice']['bid'] + p['closePrice']['ask']) / 2,
        } for p in prices]
    except (KeyError, TypeError):
        _candle_cache.pop(ck, None); return pd.DataFrame()
    df = pd.DataFrame(rows)
    df['time'] = pd.to_datetime(df['time'], utc=True)
    df = df.sort_values('time').reset_index(drop=True)
    _candle_cache[ck] = {'ts': now, 'df': df}
    return df


def calc_atr_series(df, period=14, method=None):
    if method is None: method = ATR_METHOD
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low']  - df['close'].shift()).abs(),
    ], axis=1).max(axis=1)
    if method.upper() == 'SMA':
        return tr.rolling(window=period, min_periods=1).mean()
    return tr.ewm(span=period, adjust=False).mean()


def calc_supertrend(df, period=10, mult=3.0):
    atr   = calc_atr_series(df, period)
    hl2   = (df['high'] + df['low']) / 2
    upper = (hl2 + mult * atr).values
    lower = (hl2 - mult * atr).values
    close = df['close'].values
    n     = len(df)
    fu, fl    = upper.copy(), lower.copy()
    st        = np.zeros(n)
    direction = np.ones(n, dtype=int)
    for i in range(1, n):
        fu[i] = upper[i] if (upper[i] < fu[i-1] or close[i-1] > fu[i-1]) else fu[i-1]
        fl[i] = lower[i] if (lower[i] > fl[i-1] or close[i-1] < fl[i-1]) else fl[i-1]
        if st[i-1] == fu[i-1]:
            direction[i] = 1  if close[i] <= fu[i] else -1
        else:
            direction[i] = -1 if close[i] >= fl[i] else  1
        st[i] = fl[i] if direction[i] == 1 else fu[i]
    return pd.Series(st, index=df.index), pd.Series(direction, index=df.index)


def calc_ema(s, p):
    return s.ewm(span=p, adjust=False).mean()


# ═══════════════════════════════════════════════════════
# 🆕 FEATURE #3 — ADX
# ═══════════════════════════════════════════════════════

def calc_adx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """
    ADX + DI± الحقيقي (Wilder's Smoothing = EWM).
    يُعيد DataFrame بأعمدة: adx, plus_di, minus_di
    """
    high  = df['high']
    low   = df['low']
    close = df['close']

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs(),
    ], axis=1).max(axis=1)

    up   = high - high.shift()
    down = low.shift() - low

    plus_dm  = np.where((up > down)   & (up > 0),   up,   0.0)
    minus_dm = np.where((down > up)   & (down > 0), down,  0.0)

    atr_s     = tr.ewm(span=period, adjust=False).mean()
    plus_s    = pd.Series(plus_dm,  index=df.index).ewm(span=period, adjust=False).mean()
    minus_s   = pd.Series(minus_dm, index=df.index).ewm(span=period, adjust=False).mean()

    plus_di  = 100 * plus_s  / atr_s.replace(0, np.nan)
    minus_di = 100 * minus_s / atr_s.replace(0, np.nan)

    dx  = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.ewm(span=period, adjust=False).mean()

    return pd.DataFrame({'adx': adx, 'plus_di': plus_di, 'minus_di': minus_di},
                        index=df.index)


def check_market_regime(epic):
    """
    Returns: (regime, adx_val, plus_di, minus_di, size_factor)
    regime: 'TRENDING' | 'WEAK' | 'RANGING' | 'UNKNOWN'
    """
    df = fetch_candles(epic, STRATEGY_TF, 100)
    if df.empty or len(df) < ADX_PERIOD * 3:
        return 'UNKNOWN', 0.0, 0.0, 0.0, 1.0

    df_c   = df.iloc[:-1].copy().reset_index(drop=True)
    adx_df = calc_adx(df_c, ADX_PERIOD)

    if adx_df.empty or adx_df['adx'].isna().all():
        return 'UNKNOWN', 0.0, 0.0, 0.0, 1.0

    adx_val  = float(adx_df['adx'].iloc[-1])
    plus_di  = float(adx_df['plus_di'].iloc[-1])
    minus_di = float(adx_df['minus_di'].iloc[-1])

    if   adx_val >= ADX_TRENDING_MIN: regime, sf = 'TRENDING', 1.0
    elif adx_val >= ADX_WEAK_MIN:     regime, sf = 'WEAK',     ADX_WEAK_SIZE_FACTOR
    else:                              regime, sf = 'RANGING',  0.0

    return regime, adx_val, plus_di, minus_di, sf


# ═══════════════════════════════════════════════════════
# 🆕 FEATURE #2 — MULTI-TIMEFRAME CONFIRMATION (H1)
# ═══════════════════════════════════════════════════════

def check_htf_confirmation(epic, m15_signal: str):
    """
    يتحقق أن H1 يدعم إشارة M15.
    Returns: (confirmed, htf_dir, reason)
      confirmed : True = السماح بالتداول
      htf_dir   : 1=صاعد | -1=هابط | 0=محايد
    """
    df_h1 = fetch_candles(epic, HTF_TF, HTF_COUNT)
    if df_h1.empty or len(df_h1) < max(SUPERTREND_PERIOD * 3 + ATR_PERIOD,
                                       HTF_EMA_PERIOD + 10):
        # بيانات H1 غير كافية → نسمح بالتداول بحجم محافظ
        return True, 0, 'NO_H1_DATA'

    df_c         = df_h1.iloc[:-1].copy().reset_index(drop=True)
    _, st_dir_h1 = calc_supertrend(df_c, SUPERTREND_PERIOD, SUPERTREND_MULT)
    ema50_h1     = calc_ema(df_c['close'], HTF_EMA_PERIOD)

    h1_st_dir = int(st_dir_h1.iloc[-1])
    h1_close  = float(df_c['close'].iloc[-1])
    h1_ema50  = float(ema50_h1.iloc[-1])

    # تحديد اتجاه H1: Supertrend + السعر فوق/تحت EMA50
    if   h1_st_dir == 1  and h1_close > h1_ema50:
        htf_dir, label = 1,  'H1 صاعد ⬆️'
    elif h1_st_dir == -1 and h1_close < h1_ema50:
        htf_dir, label = -1, 'H1 هابط ⬇️'
    else:
        htf_dir, label = 0,  'H1 محايد ↔️'

    # تطابق مع إشارة M15
    if m15_signal == 'BUY'  and htf_dir == 1:
        return True,  htf_dir, f'✅ {label} — يدعم BUY'
    if m15_signal == 'SELL' and htf_dir == -1:
        return True,  htf_dir, f'✅ {label} — يدعم SELL'
    if htf_dir == 0:
        # محايد → سماح بحجم مخفض (60%)
        return True,  htf_dir, f'⚠️ {label} — تداول محافظ'

    # H1 عكس الإشارة → رفض تام
    return False, htf_dir, f'❌ {label} عكس {m15_signal}'


# ═══════════════════════════════════════════════════════
# CORRELATION FILTER
# ═══════════════════════════════════════════════════════

def check_correlation_filter(new_pair, new_direction):
    correlated = {
        'EURUSD': ['GBPUSD'], 'GBPUSD': ['EURUSD'],
        'GOLD':   ['EURUSD'], 'BTCUSD': [],
        'US100':  ['US500'],  'US500':  ['US100'],
    }
    live = {p['pair']: p['direction'] for p in op_get_all()}
    if new_pair in correlated:
        for cp in correlated[new_pair]:
            if cp in live and live[cp] == new_direction:
                return False, f'{cp} open ({live[cp]})'
    return True, 'OK'


# ═══════════════════════════════════════════════════════
# SMART EXITS
# ═══════════════════════════════════════════════════════

def calculate_sl_at_r(pos, locked_r):
    d = abs(pos['entry'] - pos['sl'])
    if pos['direction'] == 'BUY':
        return round(pos['entry'] + locked_r * d, 5)
    return round(pos['entry'] - locked_r * d, 5)


def get_progressive_lock(profit_r):
    for t, l in sorted(PROGRESSIVE_LOCK.items(), reverse=True):
        if profit_r >= t: return l
    return 0


def should_move_sl(cur, new, direction):
    return new > cur + 0.00001 if direction == 'BUY' else new < cur - 0.00001


def calculate_trailing_sl(pos, price, atr):
    d = atr * TRAILING_ATR_MULT
    return round(price - d, 5) if pos['direction'] == 'BUY' else round(price + d, 5)


def manage_smart_exits():
    tracked = op_get_all()
    if not tracked: return

    live_pos = get_open_positions()
    live_ids = {p.get('position', {}).get('dealId', '') for p in live_pos}

    for pos in tracked:
        deal_id = pos['deal_id']
        pair    = pos['pair']

        # ── مغلقة بالسوق ──────────────────────────────────
        if deal_id not in live_ids:
            ep = get_closed_deal_price(deal_id, get_current_price(pair))
            if ep > 0:
                result, _ = csv_log_trade(
                    pos, ep, pos['stage1_done'], pos['stage2_done'],
                    pos['stage3_done'], pos['final_locked_r'], 'STOP_OUT'
                )
                if result != 'ERROR':
                    db_update(pos['db_key'],
                              result.upper() if result in ('WIN','LOSS','BE') else 'CLOSED',
                              'STOP_OUT')
                    op_delete(deal_id)
            continue

        # ── 🆕 إغلاق قبل خبر حرج ─────────────────────────
        if is_critical_news_imminent(pair):
            cp = get_current_price(pair)
            if close_full_api(deal_id):
                result, _ = csv_log_trade(
                    pos, cp, pos['stage1_done'], pos['stage2_done'],
                    pos['stage3_done'], pos['final_locked_r'], 'NEWS_CLOSE'
                )
                db_update(pos['db_key'],
                          result if result != 'ERROR' else 'CLOSED',
                          'NEWS_CLOSE')
                op_delete(deal_id)
                tg(f'📰 *{pair}* أُغلقت قبل خبر حرج | `{utc_now()}`')
            continue

        cur_price = get_current_price(pair)
        if cur_price <= 0: continue

        entry, sl, tp = pos['entry'], pos['sl'], pos['tp']
        size, dir_    = pos['size'],  pos['direction']
        sl_dist = abs(entry - sl)
        if sl_dist <= 0: continue

        profit_pts = (cur_price - entry) if dir_ == 'BUY' else (entry - cur_price)
        profit_r   = profit_pts / sl_dist

        # bars_held من وقت الفتح
        bars_held = 0
        opened_at = pos.get('opened_at', '')
        if opened_at:
            try:
                odt = datetime.strptime(opened_at, '%Y-%m-%d %H:%M UTC').replace(tzinfo=timezone.utc)
                bars_held = int((datetime.now(timezone.utc) - odt).total_seconds() / 60 / M15_MINUTES)
            except Exception:
                bars_held = pos.get('bars_held', 0) + 1
        op_update(deal_id, bars_held=bars_held)

        s1, s2, s3 = pos['stage1_done'], pos['stage2_done'], pos['stage3_done']

        # Time exit
        if bars_held > MAX_TRADE_DURATION_BARS and profit_r < 0.5:
            if close_full_api(deal_id):
                result, _ = csv_log_trade(pos, cur_price, s1, s2, s3,
                                          pos['final_locked_r'], 'TIME')
                db_update(pos['db_key'], result, 'TIME')
                op_delete(deal_id)
            continue

        _, _, _, cs, min_sz, _ = get_instrument_meta(pair)
        stage_action = False

        # Stage 1 — 50% عند 1.5R + Break-even
        if not s1 and profit_r >= STAGE1_TP_R:
            partial = math.floor(size * STAGE1_PCT * 100) / 100
            if partial >= min_sz and close_partial_api(deal_id, partial):
                stage_action = True
                be_buf = pos['atr'] * 0.1
                be_sl  = round(entry + be_buf, 5) if dir_ == 'BUY' else round(entry - be_buf, 5)
                if should_move_sl(sl, be_sl, dir_) and update_sl_api(deal_id, be_sl, tp):
                    op_update(deal_id, stage1_done=1, sl=be_sl)
                else:
                    op_update(deal_id, stage1_done=1)

        # Stage 2 — 30% عند 2.5R
        elif s1 and not s2 and profit_r >= STAGE2_TP_R:
            rem  = size * (1 - STAGE1_PCT)
            s2sz = math.floor(rem * (STAGE2_PCT / (1 - STAGE1_PCT)) * 100) / 100
            if s2sz >= min_sz and close_partial_api(deal_id, s2sz):
                stage_action = True
                nsl = calculate_sl_at_r(pos, 0.5)
                if nsl and update_sl_api(deal_id, nsl, tp):
                    op_update(deal_id, stage2_done=1, final_locked_r=0.5, sl=nsl)
                else:
                    op_update(deal_id, stage2_done=1)

        # Stage 3 — 50% متبقي عند 3.5R
        elif s1 and s2 and not s3 and profit_r >= FINAL_TP_R:
            rem  = size * (1 - STAGE1_PCT) * (1 - STAGE2_PCT / (1 - STAGE1_PCT))
            fs   = math.floor(rem * FINAL_PCT * 100) / 100
            if fs >= min_sz and close_partial_api(deal_id, fs):
                stage_action = True
                nsl = calculate_sl_at_r(pos, 2.0)
                if nsl and update_sl_api(deal_id, nsl, tp):
                    op_update(deal_id, stage3_done=1, final_locked_r=2.0, sl=nsl)
                else:
                    op_update(deal_id, stage3_done=1)

        # SL Management (مستقل عن الـ stages)
        if not stage_action:
            if s2:
                nlr = get_progressive_lock(profit_r)
                if nlr > pos['final_locked_r']:
                    nsl = calculate_sl_at_r(pos, nlr)
                    if nsl and should_move_sl(sl, nsl, dir_) and update_sl_api(deal_id, nsl, tp):
                        op_update(deal_id, final_locked_r=nlr, sl=nsl)

            if profit_r >= TRAILING_START_R:
                ltr = pos.get('last_trail_r', 0.0) or 0.0
                if profit_r > ltr + 0.5:
                    dft = fetch_candles(PAIRS[pair]['epic'], STRATEGY_TF, 50)
                    if not dft.empty:
                        av = float(calc_atr_series(dft.iloc[:-1], ATR_PERIOD).iloc[-1])
                        if av > 0:
                            tsl = calculate_trailing_sl(pos, cur_price, av)
                            if should_move_sl(sl, tsl, dir_) and update_sl_api(deal_id, tsl, tp):
                                op_update(deal_id, sl=tsl, last_trail_r=profit_r)


# ═══════════════════════════════════════════════════════
# STARTUP RECONCILIATION
# ═══════════════════════════════════════════════════════

def reconcile_positions():
    log('🔄 Reconcile...')
    live_pos = get_open_positions()
    db_pos   = op_get_all()
    live_ids = {p.get('position', {}).get('dealId', '') for p in live_pos}
    db_ids   = {p['deal_id'] for p in db_pos}

    for pos in db_pos:
        if pos['deal_id'] not in live_ids:
            log(f'  ⚠️ Reconcile close: {pos["pair"]}')
            ep = get_closed_deal_price(pos['deal_id'], get_current_price(pos['pair']))
            if ep > 0:
                csv_log_trade(pos, ep, pos['stage1_done'], pos['stage2_done'],
                              pos['stage3_done'], pos['final_locked_r'], 'RECONCILE_CLOSE')
            op_delete(pos['deal_id'])

    for li in live_pos:
        did = li.get('position', {}).get('dealId', '')
        if did and did not in db_ids:
            pd_ = li.get('position', {})
            epic = li.get('market', {}).get('epic', '')
            pn   = next((k for k, v in PAIRS.items() if v['epic'] == epic), epic)
            log(f'  ⚠️ Reconcile add: {pn}')
            dk = f'{pn}_rec_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")}'
            op_save(did, pn, pd_.get('direction',''),
                    float(pd_.get('openLevel',0) or 0),
                    float(pd_.get('stopLevel',0) or 0),
                    float(pd_.get('profitLevel',0) or 0),
                    0.0, float(pd_.get('dealSize',0) or 0), dk)

    log(f'  ✅ Reconcile OK | Live:{len(live_ids)} DB:{len(db_ids)}')


# ═══════════════════════════════════════════════════════
# SIGNAL DETECTION — M15 + H1(MTF) + ADX + NEWS
# ═══════════════════════════════════════════════════════

def check_signal(pair_name, config, session_mult):
    """
    Pipeline الكاملة:
    1. Volatility Regime
    2. M15 Fresh Flip + EMA20
    3. 🆕 News Filter
    4. 🆕 ADX Market Regime
    5. 🆕 H1 MTF Confirmation
    6. Correlation Filter
    7. SL/TP + Position Size
    """
    epic       = config['epic']
    allow_buy  = config['allow_buy']
    allow_sell = config['allow_sell']
    if not allow_buy and not allow_sell:
        return None

    # 1. Volatility
    vol_regime, vol_mult = check_volatility_regime(epic)
    if vol_regime == 'EXTREME':
        log(f'  {pair_name}: تقلب شديد'); return None

    # 2. M15 Candles + Fresh Flip
    df = fetch_candles(epic, STRATEGY_TF, CANDLES_COUNT)
    if df.empty or len(df) < max(SUPERTREND_PERIOD * 3 + ATR_PERIOD, 100):
        return None

    df_c = df.iloc[:-1].copy().reset_index(drop=True)
    n    = len(df_c)
    if n < 3: return None

    _, st_dir = calc_supertrend(df_c, SUPERTREND_PERIOD, SUPERTREND_MULT)
    ema_20    = calc_ema(df_c['close'], EMA_PERIOD)

    li, lp      = n - 1, n - 2
    st_dir_now  = int(st_dir.iloc[li])
    st_dir_prev = int(st_dir.iloc[lp])

    if st_dir_now == st_dir_prev:
        return None   # لا Flip → لا إشارة

    lc, la   = float(df_c['close'].iloc[-1]), float(calc_atr_series(df_c, ATR_PERIOD).iloc[-1])
    ema_val  = float(ema_20.iloc[li])
    if np.isnan(la) or la <= 0 or np.isnan(ema_val): return None

    bid, ask, sp, cs, min_sz, max_sz = get_instrument_meta(epic)
    if bid <= 0: return None
    if sp > la * SPREAD_ATR_MAX:
        log(f'  {pair_name}: spread عالٍ'); return None

    signal, entry = None, None
    if allow_buy  and st_dir_now == 1  and lc > ema_val: signal, entry = 'BUY',  ask
    elif allow_sell and st_dir_now == -1 and lc < ema_val: signal, entry = 'SELL', bid
    if not signal: return None

    # 3. 🆕 News Filter
    blocked, news_reason = is_news_blocking(pair_name)
    if blocked:
        log(f'  {pair_name}: 📰 {news_reason}')
        tg(f'📰 *NEWS BLOCK* — {pair_name}\n_{news_reason}_\n_{utc_now()}_')
        return None

    # 4. 🆕 ADX Market Regime
    adx_regime, adx_val, plus_di, minus_di, adx_sf = check_market_regime(epic)
    if adx_regime == 'RANGING':
        log(f'  {pair_name}: 〰️ ADX={adx_val:.1f} سوق بلا اتجاه')
        return None

    # تأكيد DI
    if signal == 'BUY'  and adx_val > 0 and plus_di  < minus_di:
        log(f'  {pair_name}: +DI<-DI مع BUY — ضعيف'); return None
    if signal == 'SELL' and adx_val > 0 and minus_di < plus_di:
        log(f'  {pair_name}: -DI<+DI مع SELL — ضعيف'); return None

    log(f'  {pair_name}: ADX={adx_val:.1f}[{adx_regime}] +DI={plus_di:.1f} -DI={minus_di:.1f}')

    # 5. 🆕 H1 MTF Confirmation
    htf_ok, htf_dir, htf_reason = check_htf_confirmation(epic, signal)
    log(f'  {pair_name}: {htf_reason}')
    if not htf_ok:
        return None

    htf_sf = 1.0 if htf_dir != 0 else 0.6   # محايد → 60%

    # 6. Correlation
    corr_ok, corr_msg = check_correlation_filter(pair_name, signal)
    if not corr_ok:
        log(f'  {pair_name}: Corr [{corr_msg}]'); return None

    # 7. SL / TP
    if signal == 'BUY':
        sl = round(entry - SL_ATR_MULT * la - sp, 5)
        tp = round(entry + TP_ATR_MULT * la, 5)
    else:
        sl = round(entry + SL_ATR_MULT * la + sp, 5)
        tp = round(entry - TP_ATR_MULT * la, 5)

    sld = abs(entry - sl)
    if sld < la * 0.1:
        log(f'  {pair_name}: SL صغير جداً'); return None

    # Risk = dynamic × session × vol × ADX × H1
    dyn_risk, _ = calculate_dynamic_risk(pair_name, BASE_RISK_PERCENT)
    final_risk  = dyn_risk * session_mult * vol_mult * adx_sf * htf_sf
    final_risk  = max(MIN_RISK_PERCENT, min(final_risk, MAX_RISK_PERCENT))

    log(f'  {pair_name}: {signal} Risk={final_risk:.2%} '
        f'[dyn={dyn_risk:.2%}×sess={session_mult:.1f}×vol={vol_mult:.1f}'
        f'×adx={adx_sf:.1f}×htf={htf_sf:.1f}]')

    # Position Size
    sz = config.get('size_override')
    if sz:
        size = max(min(float(sz), max_sz), min_sz)
    else:
        risk_usd = get_current_balance() * final_risk
        raw      = risk_usd / (sld * cs) if (sld * cs) > 0 else min_sz
        size     = max(min(math.floor(raw * 100) / 100, max_sz), min_sz)

    return {
        'pair':         pair_name, 'epic':      epic,
        'direction':    signal,    'entry':     round(entry, 5),
        'sl':           sl,        'tp':        tp,
        'atr':          round(la, 5), 'size':   size,
        'spread':       round(sp, 5), 'risk_percent': round(final_risk, 6),
        'adx_val':      adx_val,   'adx_regime': adx_regime,
        'htf_dir':      htf_dir,
    }


# ═══════════════════════════════════════════════════════
# EXECUTE & SCAN
# ═══════════════════════════════════════════════════════

def execute_order(sig):
    body = {
        'epic': sig['epic'], 'direction': sig['direction'], 'size': sig['size'],
        'guaranteedStop': False, 'trailingStop': False,
        'stopLevel': sig['sl'], 'profitLevel': sig['tp'],
    }
    log(f'  📤 {sig["pair"]} {sig["direction"]} @ {sig["entry"]} '
        f'| Sz:{sig["size"]} Risk:{sig["risk_percent"]:.2%} '
        f'| ADX:{sig["adx_val"]:.1f}[{sig["adx_regime"]}] H1:{sig["htf_dir"]:+d}')
    r = _post('/api/v1/positions', body)
    if not r: return 'ERROR', 'no response'
    data = r.json()
    if r.status_code == 200:
        ref = data.get('dealReference', 'N/A')
        time.sleep(2)
        rc = _get(f'/api/v1/confirms/{ref}')
        if rc and rc.status_code == 200:
            c       = rc.json()
            status  = c.get('dealStatus', 'UNKNOWN')
            deal_id = c.get('dealId', ref)
            if status in ('ACCEPTED', 'SUCCESS'):
                dk = f'{sig["pair"]}_{datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")}'
                op_save(deal_id, sig['pair'], sig['direction'],
                        sig['entry'], sig['sl'], sig['tp'],
                        sig['atr'], sig['size'], dk)
                log(f'  ✅ Confirmed: {deal_id[:12]}')
            else:
                log(f'  ⚠️ Not accepted: {status}')
            return status, ref
        return 'UNKNOWN', ref
    log(f'  ❌ Failed {r.status_code}: {data.get("errorCode","")}')
    return 'FAILED', data.get('errorCode')


def db_consec_losses(pair):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT status FROM trades WHERE pair=? AND status IN ('WIN','LOSS') "
                "ORDER BY id DESC LIMIT 8", (pair,)
            ).fetchall()
            c = 0
            for row in rows:
                if row[0] == 'LOSS': c += 1
                else:                break
            return c


def run_scan():
    can_trade, reason, session_mult, session_name, day_pnl = should_trade()
    if not can_trade:
        log(f'⏸ {reason}'); return

    log('═' * 62)
    log(f'🔍 SCAN | {session_name} | Mult:{session_mult:.1f}x | Day:${day_pnl:+.2f}')
    log('═' * 62)

    get_current_balance()
    manage_smart_exits()

    open_pos   = get_open_positions()
    open_pairs = op_get_open_pairs()
    log(f'  Open:{len(open_pos)}/{MAX_OPEN_TRADES} | {open_pairs or "none"}')

    if len(open_pos) >= MAX_OPEN_TRADES: return

    ts_key = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H%M')

    for pair_name, config in PAIRS.items():
        if len(open_pos) >= MAX_OPEN_TRADES: break
        if pair_name in open_pairs:          continue
        if db_consec_losses(pair_name) >= MAX_CONSECUTIVE_LOSS:
            log(f'  {pair_name}: حد الخسائر'); continue
        key = f'{pair_name}_{ts_key}'
        if db_is_dup(key): continue

        sig = check_signal(pair_name, config, session_mult)
        if not sig: continue

        db_save(key, pair_name, sig['direction'],
                sig['entry'], sig['sl'], sig['tp'],
                sig['atr'], sig['size'], sig['spread'],
                sig['risk_percent'], session_name,
                sig['adx_val'], sig['adx_regime'], sig['htf_dir'])
        tg_signal(sig, session_name)

        status, ref = execute_order(sig)
        db_update(key, status)
        log(f'  {pair_name}: {status} ref:{ref}')

        open_pos   = get_open_positions()
        open_pairs = op_get_open_pairs()
        time.sleep(2)


# ═══════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════

def main_loop():
    session_age = 0
    while True:
        try:
            if session_age == 0:
                if not create_session():
                    log('❌ Session failed — 60s retry')
                    time.sleep(60); continue
                reconcile_positions()
                _refresh_news_cache()    # تحميل الأخبار فور البدء
            else:
                if not ping_session():
                    log('⚠️ Ping fail — re-auth')
                    session_age = 0; continue

            session_age = (session_age + 1) % 15
            run_scan()
            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            log('🛑 Bot stopped'); break
        except Exception as ex:
            log(f'❌ ERROR: {ex}')
            import traceback; traceback.print_exc()
            time.sleep(30)


# ═══════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════

def start_bot():
    validate_config()
    db_init()
    csv_init()
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    w    = 65
    print('=' * w, flush=True)
    print(f'  🚀 Supertrend PRO v3 | {mode}', flush=True)
    print(f'  ST({SUPERTREND_PERIOD},{SUPERTREND_MULT}|{ATR_METHOD}) + EMA{EMA_PERIOD}', flush=True)
    print(f'  ─' * (w // 2), flush=True)
    print(f'  🆕 News Filter : {"✅ ON" if NEWS_FILTER_ENABLED else "❌ OFF"}'
          f' | -{NEWS_PRE_MINUTES}m / +{NEWS_POST_MINUTES}m', flush=True)
    print(f'  🆕 H1 MTF      : ✅ ON | EMA{HTF_EMA_PERIOD} + ST', flush=True)
    print(f'  🆕 ADX Regime  : ✅ ON | Trend≥{ADX_TRENDING_MIN}'
          f' Weak≥{ADX_WEAK_MIN} Ranging=SKIP', flush=True)
    print(f'  ─' * (w // 2), flush=True)
    print(f'  SL:{SL_ATR_MULT}xATR | TP:{TP_ATR_MULT}xATR'
          f' | MaxTrades:{MAX_OPEN_TRADES} | Risk:{BASE_RISK_PERCENT:.1%}', flush=True)
    print('=' * w, flush=True)

    nl = '\n'
    tg(f'🚀 *Supertrend PRO v3* [{mode}]{nl}'
       f'📰 News:{"✅" if NEWS_FILTER_ENABLED else "❌"} '
       f'📊 MTF-H1:✅ ADX:✅{nl}'
       f'ST({SUPERTREND_PERIOD},{SUPERTREND_MULT})+EMA{EMA_PERIOD}{nl}'
       f'SL:{SL_ATR_MULT}×ATR TP:{TP_ATR_MULT}×ATR{nl}'
       f'_{utc_now()}_')

    main_loop()


if __name__ == '__main__':
    start_bot()
