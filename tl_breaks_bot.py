#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==========================================================
# multi_pairs_bot_v4_SHIELD.py
#
# ── الإصلاحات (v2) ──────────────────────────────────────
# ✅ FIX #1  — Fresh Flip Detection
# ✅ FIX #2  — Same-Pair Open Check
# ✅ FIX #3  — calculate_pnl_since صحيحة
# ✅ FIX #4  — Trailing Stop يعمل فعلياً
# ✅ FIX #5  — bars_held بالزمن الحقيقي
# ✅ FIX #6  — حذف Fallbacks للـ Credentials
# ✅ FIX #7  — Break-even بعد Stage 1
# ✅ FIX #8  — تصحيح Double Risk
# ✅ FIX #9  — Startup Reconciliation
# ✅ FIX #10 — عمود last_trail_r في DB
# ✅ FIX #11 — week_start مع UTC timezone
# ✅ FIX #12 — close_partial_api صحيحة
# ✅ FIX #13 — _put/_delete مع Retry
# ✅ FIX #14 — Validation عند البدء
#
# ── المميزات (v3 PRO) ───────────────────────────────────
# 🆕 FEAT #1 — News Filter (ForexFactory)
# 🆕 FEAT #2 — Multi-Timeframe Confirmation (H1)
# 🆕 FEAT #3 — Market Regime Detection (ADX)
#
# ── المميزات الجديدة (v4 SHIELD) ────────────────────────
# 🛡️ SHIELD #1 — Equity Curve Filter
#   • يتتبع Peak Equity تلقائياً
#   • يوقف التداول عند Drawdown 10% من الـ Peak
#   • يستأنف فقط عند التعافي فوق 95% من الـ Peak
#   • يُرسل تنبيه تيليجرام فوري عند الإيقاف والاستئناف
#   • حالات: NORMAL / WARNING / HALTED / RECOVERING
#
# 🛡️ SHIELD #2 — Portfolio Heat Monitor
#   • يحسب إجمالي المخاطرة المفتوحة في كل لحظة
#   • يمنع فتح صفقات جديدة إذا تجاوزت الـ Heat 4%
#   • يُقلل حجم الصفقة الجديدة إذا اقتربت من الحد
#   • يعرض Heat% في كل Log رسالة
#   • يُرسل تحذير تيليجرام عند الاقتراب من الحد
#
# 🛡️ SHIELD #3 — Intelligent Cooldown System
#   • بعد 3 خسائر → توقف ساعة + تقليل حجم 50%
#   • بعد 4 خسائر → توقف 4 ساعات + تقليل حجم 30%
#   • بعد 5 خسائر → توقف يوم كامل + تنبيه طارئ
#   • Cooldown مستقل لكل زوج وآخر للبوت كله
#   • يُسجَّل في DB ويُستعاد عند إعادة التشغيل
#   • يُرسل تقرير تيليجرام عند الدخول والخروج من Cooldown
# ==========================================================

import os, csv, json, time, math, sqlite3, requests
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
HTF_TF        = 'HOUR'
CANDLES_COUNT = 500
HTF_CANDLES   = 100
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '180'))
M15_MINUTES   = 15

# ═══════════════════════════════════════════════════════
# SUPERTREND
# ═══════════════════════════════════════════════════════
SUPERTREND_PERIOD = int(os.getenv('SUPERTREND_PERIOD', '10'))
SUPERTREND_MULT   = float(os.getenv('SUPERTREND_MULT', '3.0'))
ATR_METHOD        = os.getenv('ATR_METHOD', 'RMA')
EMA_PERIOD        = 20
ATR_PERIOD        = 14
SL_ATR_MULT       = 1.5
TP_ATR_MULT       = 3.0
SPREAD_ATR_MAX    = 0.25

# ═══════════════════════════════════════════════════════
# NEWS FILTER (FEAT #1)
# ═══════════════════════════════════════════════════════
NEWS_FILTER_ENABLED = os.getenv('NEWS_FILTER', 'true').lower() == 'true'
NEWS_BUFFER_MINUTES = int(os.getenv('NEWS_BUFFER_MIN', '30'))
NEWS_CLOSE_MINUTES  = int(os.getenv('NEWS_CLOSE_MIN', '15'))
NEWS_CALENDAR_URL   = 'https://nfs.faireconomy.media/ff_calendar_thisweek.json'
NEWS_CACHE_TTL      = 3600

PAIR_CURRENCIES = {
    'GOLD':   ['USD', 'XAU'], 'BTCUSD': ['USD'],
    'EURUSD': ['EUR', 'USD'], 'GBPUSD': ['GBP', 'USD'],
    'US100':  ['USD'],        'US500':  ['USD'],
}

# ═══════════════════════════════════════════════════════
# ADX MARKET REGIME (FEAT #3)
# ═══════════════════════════════════════════════════════
ADX_PERIOD         = int(os.getenv('ADX_PERIOD', '14'))
ADX_STRONG_TREND   = float(os.getenv('ADX_STRONG', '25'))
ADX_WEAK_TREND     = float(os.getenv('ADX_WEAK',   '20'))
ADX_NO_TRADE       = float(os.getenv('ADX_DEAD',   '15'))
ADX_SIZE_REDUCTION = float(os.getenv('ADX_REDUCE', '0.5'))

# ═══════════════════════════════════════════════════════
# 🛡️ SHIELD #1 — EQUITY CURVE FILTER
# ═══════════════════════════════════════════════════════
ECF_ENABLED         = os.getenv('ECF_ENABLED', 'true').lower() == 'true'
ECF_MAX_DD_PCT      = float(os.getenv('ECF_MAX_DD',      '0.10'))  # وقف عند -10% من Peak
ECF_RESUME_PCT      = float(os.getenv('ECF_RESUME',      '0.95'))  # استئناف عند 95% من Peak
ECF_WARNING_PCT     = float(os.getenv('ECF_WARNING',     '0.07'))  # تحذير عند -7%
ECF_SIZE_REDUCE_PCT = float(os.getenv('ECF_SIZE_REDUCE', '0.60'))  # تقليل الحجم 60% في WARNING

# ═══════════════════════════════════════════════════════
# 🛡️ SHIELD #2 — PORTFOLIO HEAT MONITOR
# ═══════════════════════════════════════════════════════
HEAT_MAX_PCT     = float(os.getenv('HEAT_MAX_PCT',  '0.04'))  # 4% حد أقصى إجمالي
HEAT_WARN_PCT    = float(os.getenv('HEAT_WARN_PCT', '0.03'))  # 3% تحذير
HEAT_REDUCE_PCT  = float(os.getenv('HEAT_REDUCE',   '0.50'))  # تقليل 50% عند الاقتراب

# ═══════════════════════════════════════════════════════
# 🛡️ SHIELD #3 — COOLDOWN SYSTEM
# ═══════════════════════════════════════════════════════
# مدة التوقف (بالدقائق) والمضاعف لكل مستوى خسائر
COOLDOWN_RULES = {
    3: {'minutes': 60,   'size_factor': 0.50, 'label': 'تحذير'},    # 3 خسائر → ساعة
    4: {'minutes': 240,  'size_factor': 0.30, 'label': 'خطر'},      # 4 خسائر → 4 ساعات
    5: {'minutes': 1440, 'size_factor': 0.00, 'label': 'إيقاف كامل'}, # 5 خسائر → يوم كامل
}
COOLDOWN_GLOBAL_ENABLED = os.getenv('COOLDOWN_GLOBAL', 'true').lower() == 'true'
COOLDOWN_PAIR_ENABLED   = os.getenv('COOLDOWN_PAIR',   'true').lower() == 'true'

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
    'ASIA':        {'start': 0,  'end': 7,  'risk_mult': 0.5, 'name': 'آسيا'},
    'LONDON_OPEN': {'start': 7,  'end': 10, 'risk_mult': 1.2, 'name': 'فتح لندن'},
    'LONDON_MID':  {'start': 10, 'end': 12, 'risk_mult': 1.0, 'name': 'منتصف لندن'},
    'LONDON_NY':   {'start': 12, 'end': 16, 'risk_mult': 1.5, 'name': 'لندن-نيويورك ⭐'},
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

db_lock         = Lock()
session_headers = {}
_meta_cache     = {}
_candle_cache   = {}
_news_cache     = {'data': [], 'ts': 0}

# ── State in-memory (يُحمَّل من DB عند البدء) ──────────
_equity_state  = {
    'peak_equity':   0.0,   # أعلى رصيد تاريخي
    'current_dd':    0.0,   # Drawdown الحالي %
    'status':        'NORMAL',  # NORMAL / WARNING / HALTED / RECOVERING
    'halted_at':     None,  # وقت الإيقاف
    'last_notified': '',    # آخر حالة أُرسل بها تنبيه
}

_cooldown_state = {
    # 'GLOBAL': {'until': datetime, 'level': 3, 'size_factor': 0.5}
    # 'GOLD':   {'until': datetime, 'level': 3, 'size_factor': 0.5}
}

CSV_HEADERS = [
    'date', 'time_utc', 'pair', 'direction', 'entry', 'sl', 'tp', 'exit_price',
    'atr', 'size', 'sl_dist', 'pnl_usd', 'pnl_r', 'result', 'bars_held', 'spread', 'tf',
    'stage1_done', 'stage2_done', 'stage3_done', 'final_locked_r', 'exit_type',
    'session_used', 'risk_percent', 'adx_value', 'regime', 'htf_confirmed',
    'portfolio_heat', 'ecf_status', 'cooldown_active', 'indicator', 'notes'
]


# ═══════════════════════════════════════════════════════
# VALIDATION
# ═══════════════════════════════════════════════════════
def validate_config():
    missing = [k for k, v in [
        ('CAPITAL_API_KEY', API_KEY),
        ('CAPITAL_EMAIL',   EMAIL),
        ('CAPITAL_PASSWORD', PASSWORD)
    ] if not v]
    if missing:
        raise EnvironmentError(f'❌ متغيرات ناقصة: {", ".join(missing)}')
    log('✅ Config OK')


# ═══════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════
def _migrate_database(conn):
    try:
        t_cols = {c[1] for c in conn.execute("PRAGMA table_info(trades)").fetchall()}
        for col, typ in {
            'pnl_r':          'REAL DEFAULT 0',
            'pnl_usd':        'REAL DEFAULT 0',
            'exit_price':     'REAL',
            'bars_held':      'INTEGER DEFAULT 0',
            'risk_percent':   'REAL DEFAULT 0',
            'adx_value':      'REAL DEFAULT 0',
            'regime':         'TEXT DEFAULT ""',
            'htf_confirmed':  'INTEGER DEFAULT 0',
            'portfolio_heat': 'REAL DEFAULT 0',    # 🛡️ SHIELD #2
            'ecf_status':     'TEXT DEFAULT ""',   # 🛡️ SHIELD #1
            'cooldown_active':'INTEGER DEFAULT 0', # 🛡️ SHIELD #3
        }.items():
            if col not in t_cols:
                conn.execute(f'ALTER TABLE trades ADD COLUMN {col} {typ}')
                log(f'  ✅ DB trades: +{col}')

        op_cols = {c[1] for c in conn.execute("PRAGMA table_info(open_positions)").fetchall()}
        for col, typ in {
            'last_trail_r': 'REAL DEFAULT 0',
            'opened_at':    'TEXT',
        }.items():
            if col not in op_cols:
                conn.execute(f'ALTER TABLE open_positions ADD COLUMN {col} {typ}')
                log(f'  ✅ DB open_positions: +{col}')

        # 🛡️ جدول لتخزين حالة النظام (ECF Peak + Cooldown)
        conn.execute('''CREATE TABLE IF NOT EXISTS bot_state (
            key   TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT
        )''')

    except Exception as ex:
        log(f'  ⚠️ DB migrate: {ex}')


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
            spread         REAL    DEFAULT 0,
            status         TEXT    DEFAULT 'PENDING',
            stage1_done    INTEGER DEFAULT 0,
            stage2_done    INTEGER DEFAULT 0,
            stage3_done    INTEGER DEFAULT 0,
            final_locked_r REAL    DEFAULT 0,
            exit_type      TEXT,
            session_used   TEXT,
            risk_percent   REAL,
            pnl_r          REAL    DEFAULT 0,
            pnl_usd        REAL    DEFAULT 0,
            exit_price     REAL,
            bars_held      INTEGER DEFAULT 0,
            adx_value      REAL    DEFAULT 0,
            regime         TEXT    DEFAULT "",
            htf_confirmed  INTEGER DEFAULT 0,
            portfolio_heat REAL    DEFAULT 0,
            ecf_status     TEXT    DEFAULT "",
            cooldown_active INTEGER DEFAULT 0
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
            final_locked_r REAL    DEFAULT 0,
            bars_held      INTEGER DEFAULT 0,
            last_trail_r   REAL    DEFAULT 0
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS bot_state (
            key        TEXT PRIMARY KEY,
            value      TEXT,
            updated_at TEXT
        )''')
        conn.commit()


# ── bot_state helpers ──────────────────────────────────
def state_set(key, value):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute(
                'INSERT OR REPLACE INTO bot_state (key,value,updated_at) VALUES (?,?,?)',
                (key, json.dumps(value), utc_now())
            )
            conn.commit()


def state_get(key, default=None):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            row = conn.execute(
                'SELECT value FROM bot_state WHERE key=?', (key,)
            ).fetchone()
            if row:
                try:
                    return json.loads(row[0])
                except Exception:
                    return row[0]
            return default


# ── trades helpers ─────────────────────────────────────
def db_save(key, pair, direction, entry, sl, tp, atr, size, spread,
            risk_pct, session, adx_val=0.0, regime='', htf_ok=0,
            heat=0.0, ecf_st='', cd_active=0):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT INTO trades '
                    '(key,pair,direction,timestamp,entry,sl,tp,atr,size,spread,'
                    'risk_percent,session_used,adx_value,regime,htf_confirmed,'
                    'portfolio_heat,ecf_status,cooldown_active) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (key, pair, direction, utc_now(), entry, sl, tp, atr, size, spread,
                     risk_pct, session, adx_val, regime, htf_ok,
                     heat, ecf_st, cd_active)
                )
                conn.commit()
            except sqlite3.IntegrityError:
                pass


def db_update(key, status, exit_type=None):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            if exit_type:
                conn.execute(
                    'UPDATE trades SET status=?,exit_type=? WHERE key=?',
                    (status, exit_type, key)
                )
            else:
                conn.execute('UPDATE trades SET status=? WHERE key=?', (status, key))
            conn.commit()


def db_is_dup(key):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            return conn.execute(
                'SELECT id FROM trades WHERE key=?', (key,)
            ).fetchone() is not None


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
    ALLOWED = {
        'sl', 'tp', 'stage1_done', 'stage2_done', 'stage3_done',
        'final_locked_r', 'bars_held', 'last_trail_r'
    }
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            for col, val in kwargs.items():
                if col in ALLOWED:
                    conn.execute(
                        f'UPDATE open_positions SET {col}=? WHERE deal_id=?',
                        (val, deal_id)
                    )
            conn.commit()


def op_delete(deal_id):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('DELETE FROM open_positions WHERE deal_id=?', (deal_id,))
            conn.commit()


def op_get_open_pairs():
    return {p['pair'] for p in op_get_all()}


# ═══════════════════════════════════════════════════════
# 🛡️ SHIELD #1 — EQUITY CURVE FILTER
# ═══════════════════════════════════════════════════════

def ecf_load_state():
    """تحميل حالة ECF من DB عند بدء البوت"""
    saved = state_get('ecf_state')
    if saved and isinstance(saved, dict):
        _equity_state.update(saved)
        log(f'  📊 ECF: Peak=${_equity_state["peak_equity"]:.2f} | '
            f'Status={_equity_state["status"]}')
    else:
        # أول تشغيل — نضبط الـ Peak على الرصيد الحالي
        bal = get_current_balance()
        _equity_state['peak_equity'] = bal
        _equity_state['status']      = 'NORMAL'
        ecf_save_state()
        log(f'  📊 ECF: Peak مبدئي = ${bal:.2f}')


def ecf_save_state():
    """حفظ حالة ECF في DB"""
    state_set('ecf_state', {
        'peak_equity':   _equity_state['peak_equity'],
        'current_dd':    _equity_state['current_dd'],
        'status':        _equity_state['status'],
        'halted_at':     _equity_state['halted_at'],
        'last_notified': _equity_state['last_notified'],
    })


def ecf_update(current_balance):
    """
    يحدّث حالة ECF بناءً على الرصيد الحالي.

    الحالات:
        NORMAL     → أقل من -7%  drawdown
        WARNING    → بين -7% و -10%  (تقليل الحجم)
        HALTED     → تجاوز -10%  (إيقاف كامل)
        RECOVERING → بعد HALTED وقبل الوصول لـ 95% من Peak
    """
    global _equity_state
    peak   = _equity_state['peak_equity']

    # تحديث الـ Peak إذا ارتفع الرصيد
    if current_balance > peak:
        old_peak = peak
        _equity_state['peak_equity'] = current_balance
        peak = current_balance
        if old_peak > 0 and current_balance > old_peak * 1.05:
            log(f'  📊 ECF: 🎉 Peak جديد! ${old_peak:.2f} → ${current_balance:.2f}')

    dd_pct = (peak - current_balance) / peak if peak > 0 else 0
    _equity_state['current_dd'] = dd_pct

    old_status = _equity_state['status']

    # تحديد الحالة الجديدة
    if _equity_state['status'] == 'HALTED':
        # نخرج من HALTED فقط عند التعافي فوق ECF_RESUME_PCT
        if current_balance >= peak * ECF_RESUME_PCT:
            _equity_state['status'] = 'NORMAL'
            log(f'  📊 ECF: ✅ تعافي كامل → NORMAL')
        elif current_balance >= peak * (1 - ECF_MAX_DD_PCT + 0.02):
            _equity_state['status'] = 'RECOVERING'
        # else: نبقى في HALTED

    elif _equity_state['status'] == 'RECOVERING':
        if current_balance >= peak * ECF_RESUME_PCT:
            _equity_state['status'] = 'NORMAL'
            log(f'  📊 ECF: ✅ تعافي → NORMAL')
        elif dd_pct >= ECF_MAX_DD_PCT:
            _equity_state['status'] = 'HALTED'

    else:  # NORMAL / WARNING
        if dd_pct >= ECF_MAX_DD_PCT:
            _equity_state['status']    = 'HALTED'
            _equity_state['halted_at'] = utc_now()
        elif dd_pct >= ECF_WARNING_PCT:
            _equity_state['status'] = 'WARNING'
        else:
            _equity_state['status'] = 'NORMAL'

    # إرسال تنبيه عند تغيير الحالة
    if _equity_state['status'] != old_status:
        _ecf_notify(old_status, _equity_state['status'], current_balance, peak, dd_pct)
        ecf_save_state()

    return _equity_state['status'], dd_pct


def _ecf_notify(old_st, new_st, balance, peak, dd_pct):
    """إرسال تنبيه تيليجرام عند تغيير حالة ECF"""
    icons = {
        'NORMAL': '✅', 'WARNING': '⚠️',
        'HALTED': '🛑', 'RECOVERING': '🔄'
    }
    nl = '\n'
    msg = (
        f'{icons.get(new_st,"❓")} *ECF: {old_st} → {new_st}*{nl}'
        f'Balance: `${balance:,.2f}` | Peak: `${peak:,.2f}`{nl}'
        f'Drawdown: `{dd_pct:.1%}`{nl}'
    )
    if new_st == 'HALTED':
        msg += f'🛑 *التداول متوقف حتى التعافي فوق ${peak * ECF_RESUME_PCT:,.2f}*{nl}'
    elif new_st == 'WARNING':
        msg += f'⚠️ *تحذير: حجم الصفقات مقلَّص {int((1-ECF_SIZE_REDUCE_PCT)*100)}%*{nl}'
    elif new_st == 'NORMAL':
        msg += '✅ *البوت عاد للعمل بشكل طبيعي*{nl}'
    msg += f'_{utc_now()}_'
    tg(msg)
    log(f'  📊 ECF: {old_st} → {new_st} | DD={dd_pct:.1%}')


def ecf_check():
    """
    الدالة الرئيسية للفحص — تُستدعى من should_trade().

    Returns:
        (allowed: bool, size_factor: float, status: str, dd_pct: float)
    """
    if not ECF_ENABLED:
        return True, 1.0, 'DISABLED', 0.0

    balance         = get_current_balance()
    status, dd_pct  = ecf_update(balance)

    if status == 'HALTED':
        return False, 0.0, status, dd_pct

    if status == 'RECOVERING':
        # في فترة التعافي: تداول بحذر شديد (40% حجم)
        return True, 0.40, status, dd_pct

    if status == 'WARNING':
        return True, ECF_SIZE_REDUCE_PCT, status, dd_pct

    return True, 1.0, 'NORMAL', dd_pct


# ═══════════════════════════════════════════════════════
# 🛡️ SHIELD #2 — PORTFOLIO HEAT MONITOR
# ═══════════════════════════════════════════════════════

def heat_calculate():
    """
    يحسب إجمالي المخاطرة للصفقات المفتوحة حالياً.

    Heat = مجموع risk_percent لكل صفقة مفتوحة
    (كل صفقة مفتوحة تعني خطر خسارة risk_percent من الرصيد)

    Returns:
        (total_heat: float, heat_pct: str, positions_count: int)
    """
    open_pos  = op_get_all()
    n         = len(open_pos)
    if n == 0:
        return 0.0, '0.00%', 0

    # نجلب risk_percent من جدول trades عبر db_key
    total_heat = 0.0
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            for pos in open_pos:
                row = conn.execute(
                    'SELECT risk_percent FROM trades WHERE key=?',
                    (pos['db_key'],)
                ).fetchone()
                if row and row[0]:
                    total_heat += float(row[0])
                else:
                    # fallback: افترض BASE_RISK
                    total_heat += BASE_RISK_PERCENT

    return total_heat, f'{total_heat:.2%}', n


def heat_check(new_trade_risk):
    """
    يفحص إذا كان يمكن فتح صفقة جديدة بمخاطرة new_trade_risk.

    Returns:
        (allowed: bool, size_factor: float, reason: str, current_heat: float)
    """
    current_heat, heat_str, n = heat_calculate()
    projected = current_heat + new_trade_risk

    if projected > HEAT_MAX_PCT:
        return (False, 0.0,
                f'🌡️ Portfolio Heat: {heat_str} + {new_trade_risk:.2%} = '
                f'{projected:.2%} > حد {HEAT_MAX_PCT:.0%}',
                current_heat)

    if projected > HEAT_WARN_PCT:
        # تقليل الحجم بنسبة HEAT_REDUCE_PCT لعدم تجاوز الحد
        available_room = HEAT_MAX_PCT - current_heat
        actual_factor  = min(available_room / new_trade_risk, HEAT_REDUCE_PCT)
        return (True, max(actual_factor, 0.1),
                f'🌡️ Heat قريب من الحد: {heat_str} → حجم مقلَّص',
                current_heat)

    return True, 1.0, f'🌡️ Heat OK: {heat_str}', current_heat


def heat_log_status():
    """طباعة حالة الـ Heat في كل Scan"""
    total, heat_str, n = heat_calculate()
    bar_filled = int(total / HEAT_MAX_PCT * 20)
    bar = '█' * bar_filled + '░' * (20 - bar_filled)
    heat_icon = '🔴' if total >= HEAT_MAX_PCT else ('🟡' if total >= HEAT_WARN_PCT else '🟢')
    log(f'  {heat_icon} Portfolio Heat: [{bar}] {heat_str} / {HEAT_MAX_PCT:.0%} | {n} صفقات')


# ═══════════════════════════════════════════════════════
# 🛡️ SHIELD #3 — INTELLIGENT COOLDOWN SYSTEM
# ═══════════════════════════════════════════════════════

def cooldown_load_state():
    """تحميل حالات Cooldown من DB عند بدء البوت"""
    saved = state_get('cooldown_state')
    if saved and isinstance(saved, dict):
        now = datetime.now(timezone.utc)
        for key, val in saved.items():
            if isinstance(val, dict) and 'until' in val:
                try:
                    until_dt = datetime.fromisoformat(val['until'])
                    if until_dt > now:
                        _cooldown_state[key] = {
                            'until':       until_dt,
                            'level':       val.get('level', 3),
                            'size_factor': val.get('size_factor', 0.5),
                            'label':       val.get('label', ''),
                        }
                        remaining = int((until_dt - now).total_seconds() / 60)
                        log(f'  ⏳ Cooldown مُستعاد: {key} | {val.get("label","")} | {remaining} دقيقة متبقية')
                except Exception:
                    pass
        log(f'  ✅ Cooldown state loaded | Active: {list(_cooldown_state.keys())}')


def cooldown_save_state():
    """حفظ حالات Cooldown في DB"""
    serializable = {}
    for key, val in _cooldown_state.items():
        serializable[key] = {
            'until':       val['until'].isoformat(),
            'level':       val['level'],
            'size_factor': val['size_factor'],
            'label':       val.get('label', ''),
        }
    state_set('cooldown_state', serializable)


def cooldown_check_and_trigger(pair_name):
    """
    يفحص عدد الخسائر المتتالية ويُفعِّل Cooldown إذا لزم.
    يُستدعى بعد كل خسارة مسجَّلة.

    يتحقق من:
        1. الخسائر المتتالية للزوج المحدد (Pair Cooldown)
        2. إجمالي الخسائر المتتالية لكل الأزواج (Global Cooldown)
    """
    now = datetime.now(timezone.utc)

    # ── Pair-level Cooldown ──────────────────────────────
    if COOLDOWN_PAIR_ENABLED:
        pair_losses = _count_recent_consecutive_losses(pair_name)
        _maybe_activate_cooldown(pair_name, pair_losses, now)

    # ── Global-level Cooldown ────────────────────────────
    if COOLDOWN_GLOBAL_ENABLED:
        global_losses = _count_global_consecutive_losses()
        _maybe_activate_cooldown('GLOBAL', global_losses, now)


def _count_recent_consecutive_losses(pair_name):
    """عدد الخسائر المتتالية الأخيرة لزوج محدد"""
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT status FROM trades WHERE pair=? AND status IN ('WIN','LOSS') "
                "ORDER BY id DESC LIMIT 10",
                (pair_name,)
            ).fetchall()
    count = 0
    for row in rows:
        if row[0] == 'LOSS':
            count += 1
        else:
            break
    return count


def _count_global_consecutive_losses():
    """عدد الخسائر المتتالية الأخيرة لجميع الأزواج مجتمعة"""
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT status FROM trades WHERE status IN ('WIN','LOSS') "
                "ORDER BY id DESC LIMIT 15"
            ).fetchall()
    count = 0
    for row in rows:
        if row[0] == 'LOSS':
            count += 1
        else:
            break
    return count


def _maybe_activate_cooldown(key, loss_count, now):
    """تفعيل Cooldown إذا وصل عدد الخسائر للحد"""
    # البحث عن أعلى مستوى ينطبق
    rule = None
    for level in sorted(COOLDOWN_RULES.keys(), reverse=True):
        if loss_count >= level:
            rule = (level, COOLDOWN_RULES[level])
            break
    if not rule:
        return

    level, cfg = rule
    current    = _cooldown_state.get(key, {})

    # لا تُعيد التفعيل إذا كان نفس المستوى نشطاً
    if current.get('level') == level and current.get('until', now) > now:
        return

    until = now + timedelta(minutes=cfg['minutes'])
    _cooldown_state[key] = {
        'until':       until,
        'level':       level,
        'size_factor': cfg['size_factor'],
        'label':       cfg['label'],
    }
    cooldown_save_state()

    scope = 'GLOBAL 🌐' if key == 'GLOBAL' else f'{key}'
    nl    = '\n'
    log(f'  ⏳ COOLDOWN [{scope}]: {cfg["label"]} | '
        f'{cfg["minutes"]}د | حجم×{cfg["size_factor"]} | حتى {until.strftime("%H:%M UTC")}')

    tg(f'⏳ *Cooldown — {cfg["label"]}*{nl}'
       f'Scope: `{scope}` | خسائر: `{loss_count}`{nl}'
       f'مدة: `{cfg["minutes"]} دقيقة`{nl}'
       f'حجم الصفقات: `×{cfg["size_factor"]}`{nl}'
       f'ينتهي: `{until.strftime("%Y-%m-%d %H:%M UTC")}`{nl}'
       f'_{utc_now()}_')


def cooldown_get_factor(pair_name):
    """
    يُعيد مضاعف الحجم المناسب بعد التحقق من Cooldown.

    Returns:
        (allowed: bool, size_factor: float, reason: str)
        allowed = False → لا تداول على الإطلاق (level 5)
    """
    now = datetime.now(timezone.utc)

    # فحص Global أولاً (أولوية أعلى)
    global_cd = _cooldown_state.get('GLOBAL')
    if global_cd and global_cd['until'] > now:
        remaining  = int((global_cd['until'] - now).total_seconds() / 60)
        factor     = global_cd['size_factor']
        label      = global_cd['label']
        if factor == 0.0:
            return False, 0.0, f'⏳ GLOBAL Cooldown [{label}] | {remaining} دقيقة متبقية'
        return True, factor, f'⏳ GLOBAL [{label}] | حجم×{factor} | {remaining}د'
    elif global_cd and global_cd['until'] <= now:
        # انتهى الـ Cooldown
        _expire_cooldown('GLOBAL')

    # فحص Pair
    pair_cd = _cooldown_state.get(pair_name)
    if pair_cd and pair_cd['until'] > now:
        remaining = int((pair_cd['until'] - now).total_seconds() / 60)
        factor    = pair_cd['size_factor']
        label     = pair_cd['label']
        if factor == 0.0:
            return False, 0.0, f'⏳ {pair_name} Cooldown [{label}] | {remaining}د'
        return True, factor, f'⏳ {pair_name} [{label}] | حجم×{factor} | {remaining}د'
    elif pair_cd and pair_cd['until'] <= now:
        _expire_cooldown(pair_name)

    return True, 1.0, 'لا cooldown'


def _expire_cooldown(key):
    """إزالة Cooldown منتهي الصلاحية"""
    if key in _cooldown_state:
        label = _cooldown_state[key].get('label', '')
        del _cooldown_state[key]
        cooldown_save_state()
        scope = 'GLOBAL 🌐' if key == 'GLOBAL' else key
        log(f'  ✅ Cooldown انتهى: [{scope}] [{label}]')
        tg(f'✅ *Cooldown انتهى*\nScope: `{scope}` | `{label}`\n_{utc_now()}_')


def cooldown_status_all():
    """طباعة حالة جميع Cooldowns النشطة"""
    now    = datetime.now(timezone.utc)
    active = [(k, v) for k, v in _cooldown_state.items() if v['until'] > now]
    if not active:
        return
    log(f'  ⏳ Cooldowns نشطة: {len(active)}')
    for key, val in active:
        remaining = int((val['until'] - now).total_seconds() / 60)
        log(f'     • {key}: {val["label"]} | {remaining}د | ×{val["size_factor"]}')


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
        return False, '⚠️ Balance = 0', 0.0
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
    win_rate = len(wins) / len(trades)
    consec = cur = 0
    for t in sorted(trades, key=lambda x: x[4]):
        if t[2] == 'LOSS':
            cur += 1; consec = max(consec, cur)
        else:
            cur = 0
    return {'total': len(trades), 'win_rate': win_rate, 'consecutive_losses': consec}


def calculate_dynamic_risk(pair, base_risk=BASE_RISK_PERCENT):
    stats = get_pair_stats(pair)
    if not stats:
        return base_risk, 'default'
    risk = base_risk
    if   stats['consecutive_losses'] >= 3: risk *= 0.4
    elif stats['consecutive_losses'] == 2: risk *= 0.6
    return max(MIN_RISK_PERCENT, min(risk, MAX_RISK_PERCENT)), 'dynamic'


def get_session_info():
    hour = datetime.now(timezone.utc).hour
    for name, cfg in SESSIONS.items():
        if cfg['start'] <= hour < cfg['end']:
            return cfg['risk_mult'], cfg['name'], name
    return 0.0, 'مغلقة', 'CLOSED'


def check_volatility_regime(epic, tf=STRATEGY_TF):
    df = fetch_candles(epic, tf, 100)
    if df.empty or len(df) < 50:
        return 'NORMAL', 1.0
    atr_cur  = calc_atr_series(df.iloc[:-1],  ATR_PERIOD).iloc[-1]
    atr_hist = calc_atr_series(df.iloc[:-20], ATR_PERIOD).iloc[-20:].mean()
    ratio    = atr_cur / atr_hist if atr_hist > 0 else 1.0
    if   ratio > VOLATILITY_THRESHOLDS['EXTREME']: return 'EXTREME', 0.0
    elif ratio > VOLATILITY_THRESHOLDS['HIGH']:    return 'HIGH',    0.7
    elif ratio < VOLATILITY_THRESHOLDS['LOW']:     return 'LOW',     0.6
    return 'NORMAL', 1.0


def should_trade():
    """
    الفحص الشامل قبل أي تداول.
    يدمج: Daily/Weekly limits + ECF + Session
    """
    # Daily/Weekly Limits
    allowed, reason, day_pnl = check_drawdown_limits()
    if not allowed:
        return False, reason, 0.0, None, 0.0, 'NORMAL', 1.0

    # 🛡️ SHIELD #1: ECF
    ecf_ok, ecf_factor, ecf_status, dd_pct = ecf_check()
    if not ecf_ok:
        peak = _equity_state['peak_equity']
        bal  = get_current_balance()
        return (False,
                f'🛑 ECF HALTED | DD={dd_pct:.1%} | '
                f'Balance=${bal:,.2f} | Peak=${peak:,.2f}',
                0.0, None, 0.0, ecf_status, ecf_factor)

    # Session
    session_mult, session_name, _ = get_session_info()
    if session_mult == 0:
        return False, f'⏸ Session: {session_name}', 0.0, None, 0.0, ecf_status, ecf_factor

    return True, 'OK', session_mult, session_name, day_pnl, ecf_status, ecf_factor


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
        sl_dist  = abs(entry - sl)
        pnl_pts  = (exit_price - entry) if dir_ == 'BUY' else (entry - exit_price)
        pnl_r    = round(pnl_pts / sl_dist, 2) if sl_dist > 0 else 0
        result   = 'WIN' if pnl_pts > 0 else ('LOSS' if pnl_pts < 0 else 'BE')
        meta     = _meta_cache.get(pair, {}).get('data')
        cs       = meta[3] if meta else 1.0
        pnl_usd  = round(pnl_pts * size * cs, 2)

        bars_held = 0
        opened_at = pos.get('opened_at', '')
        if opened_at:
            try:
                opened_dt   = datetime.strptime(opened_at, '%Y-%m-%d %H:%M UTC').replace(tzinfo=timezone.utc)
                elapsed_min = (datetime.now(timezone.utc) - opened_dt).total_seconds() / 60
                bars_held   = int(elapsed_min / M15_MINUTES)
            except Exception:
                bars_held = pos.get('bars_held', 0)

        _update_trade_pnl(pos['db_key'], pnl_r, pnl_usd, exit_price, bars_held)

        # 🛡️ SHIELD #3: تحقق من Cooldown بعد كل خسارة
        if result == 'LOSS':
            cooldown_check_and_trigger(pair)

        # 🛡️ SHIELD #1: تحديث ECF بعد كل صفقة
        ecf_update(get_current_balance())

        now = datetime.now(timezone.utc)
        heat_total, heat_str, _ = heat_calculate()
        row = {
            'date': now.strftime('%Y-%m-%d'), 'time_utc': now.strftime('%H:%M'),
            'pair': pair, 'direction': dir_, 'entry': entry, 'sl': sl,
            'tp': pos['tp'], 'exit_price': exit_price, 'atr': pos['atr'],
            'size': size, 'sl_dist': round(sl_dist, 5), 'pnl_usd': pnl_usd,
            'pnl_r': pnl_r, 'result': result, 'bars_held': bars_held,
            'spread': pos.get('spread', 0), 'tf': STRATEGY_TF,
            'stage1_done': stage1, 'stage2_done': stage2, 'stage3_done': stage3,
            'final_locked_r': final_r, 'exit_type': exit_type,
            'session_used':   pos.get('session_used', ''),
            'risk_percent':   pos.get('risk_percent', 0),
            'adx_value':      pos.get('adx_value', 0),
            'regime':         pos.get('regime', ''),
            'htf_confirmed':  pos.get('htf_confirmed', 0),
            'portfolio_heat': round(heat_total, 4),
            'ecf_status':     _equity_state['status'],
            'cooldown_active': 1 if _cooldown_state else 0,
            'indicator': f'ST({SUPERTREND_PERIOD},{SUPERTREND_MULT}|{ATR_METHOD})'
                         f'+EMA{EMA_PERIOD}+ADX{ADX_PERIOD}',
            'notes': ''
        }
        with open(TRADES_CSV, 'a', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writerow(row)

        icon = '✅' if result == 'WIN' else ('❌' if result == 'LOSS' else '🔵')
        log(f'  {icon} {pair} {dir_} | PnL=${pnl_usd:+.2f} ({pnl_r:+.2f}R) | Bars:{bars_held}')
        nl = '\n'
        tg(f'{icon} *{pair} {dir_}*{nl}'
           f'PnL: `${pnl_usd:+.2f}` | `{pnl_r:+.2f}R`{nl}'
           f'ECF: `{_equity_state["status"]}` | DD: `{_equity_state["current_dd"]:.1%}`{nl}'
           f'Heat: `{heat_str}`{nl}'
           f'_{utc_now()}_')
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
            log(f'  GET {path} [{i+1}/{retries}]: {ex}')
            time.sleep(3 * (i + 1))
    return None


def _post(path, body, retries=2):
    for i in range(retries):
        try:
            return requests.post(BASE_URL + path, headers=session_headers,
                                 json=body, timeout=15)
        except requests.exceptions.RequestException as ex:
            log(f'  POST {path} [{i+1}/{retries}]: {ex}')
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
            log(f'  PUT {path} [{i+1}/{retries}]: {ex}')
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
            log(f'  DELETE {path} [{i+1}/{retries}]: {ex}')
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
                'Content-Type':     'application/json'
            })
            log('✅ Session OK')
            return True
        log(f'❌ Session FAILED: {r.status_code}')
    except Exception as ex:
        log(f'❌ Session: {ex}')
    return False


def ping_session():
    r = _get('/api/v1/ping')
    return r and r.status_code == 200


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
    result = (
        bid, ask, round(ask - bid, 5),
        float(inst.get('contractSize', 100) or 100),
        float((deal.get('minDealSize') or {}).get('value', 0.1) or 0.1),
        float((deal.get('maxDealSize') or {}).get('value', 1000) or 1000)
    )
    _meta_cache[epic] = {'ts': now, 'data': result}
    return result


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
                        if lvl:
                            return float(lvl)
    except Exception:
        pass
    return fallback


def update_sl_api(deal_id, new_sl, tp):
    r = _put(f'/api/v1/positions/{deal_id}', {'stopLevel': new_sl, 'profitLevel': tp})
    if r and r.status_code == 200:
        log(f'  ✅ SL → {new_sl}')
        return True
    return False


def close_partial_api(deal_id, size):
    r = _delete(f'/api/v1/positions/{deal_id}', body={'dealSize': size})
    if r and r.status_code == 200:
        log(f'  💰 Partial: {size} lots')
        return True
    return False


def close_full_api(deal_id):
    r = _delete(f'/api/v1/positions/{deal_id}')
    return r and r.status_code == 200


# ═══════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════

def tg(text):
    if not TG_TOKEN:
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
            data={'chat_id': TG_CHAT_ID, 'text': text, 'parse_mode': 'Markdown'},
            timeout=10
        )
    except Exception:
        pass


def tg_signal(sig, session_info, heat_str, ecf_status, cd_reason):
    icon  = '🟢' if sig['direction'] == 'BUY' else '🔴'
    mode  = 'DEMO' if DEMO_MODE else 'LIVE'
    r_ico = {'TRENDING': '📈', 'WEAK': '📊', 'SIDEWAYS': '📉'}.get(sig.get('regime', ''), '')
    nl    = '\n'
    tg(f'{icon} *{sig["pair"]} {sig["direction"]}* [{mode}]{nl}'
       f'Entry:`{sig["entry"]}` SL:`{sig["sl"]}` TP:`{sig["tp"]}`{nl}'
       f'Risk:`{sig["risk_percent"]:.2%}` Size:`{sig["size"]}`{nl}'
       f'ADX:`{sig.get("adx_value",0):.1f}` {r_ico} `{sig.get("regime","")}`{nl}'
       f'H1:`{"✅" if sig.get("htf_confirmed") else "⚠️"}` '
       f'ECF:`{ecf_status}` Heat:`{heat_str}`{nl}'
       f'CD:`{cd_reason}`{nl}'
       f'Session:`{session_info}`{nl}'
       f'_{utc_now()}_')


# ═══════════════════════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════════════════════

def fetch_candles(epic, resolution, count=500):
    cache_key = f'{epic}_{resolution}'
    now = time.time()
    if cache_key in _candle_cache:
        c = _candle_cache[cache_key]
        if now - c['ts'] < 60:
            return c['df']
    r = _get(f'/api/v1/prices/{epic}', params={'resolution': resolution, 'max': count})
    if not r or r.status_code != 200:
        _candle_cache.pop(cache_key, None)
        return pd.DataFrame()
    prices = r.json().get('prices', [])
    if len(prices) < SUPERTREND_PERIOD * 3 + ATR_PERIOD:
        return pd.DataFrame()
    try:
        rows = [{
            'time':  p['snapshotTimeUTC'],
            'open':  (p['openPrice']['bid']  + p['openPrice']['ask'])  / 2,
            'high':  (p['highPrice']['bid']  + p['highPrice']['ask'])  / 2,
            'low':   (p['lowPrice']['bid']   + p['lowPrice']['ask'])   / 2,
            'close': (p['closePrice']['bid'] + p['closePrice']['ask']) / 2
        } for p in prices]
    except (KeyError, TypeError):
        _candle_cache.pop(cache_key, None)
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df['time'] = pd.to_datetime(df['time'], utc=True)
    df = df.sort_values('time').reset_index(drop=True)
    _candle_cache[cache_key] = {'ts': now, 'df': df}
    return df


def calc_atr_series(df, period=14, method=None):
    if method is None:
        method = ATR_METHOD
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low']  - df['close'].shift()).abs()
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
    fu = upper.copy(); fl = lower.copy()
    st = np.zeros(n);  dire = np.ones(n, dtype=int)
    for i in range(1, n):
        fu[i] = upper[i] if (upper[i] < fu[i-1] or close[i-1] > fu[i-1]) else fu[i-1]
        fl[i] = lower[i] if (lower[i] > fl[i-1] or close[i-1] < fl[i-1]) else fl[i-1]
        if st[i-1] == fu[i-1]:
            dire[i] = 1  if close[i] <= fu[i] else -1
        else:
            dire[i] = -1 if close[i] >= fl[i] else  1
        st[i] = fl[i] if dire[i] == 1 else fu[i]
    return pd.Series(st, index=df.index), pd.Series(dire, index=df.index)


def calc_ema(s, p):
    return s.ewm(span=p, adjust=False).mean()


def calc_adx(df, period=14):
    high = df['high']; low = df['low']; close = df['close']
    tr   = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs()
    ], axis=1).max(axis=1)
    up   = high - high.shift()
    dn   = low.shift() - low
    pdm  = np.where((up > dn)  & (up > 0), up, 0.0)
    ndm  = np.where((dn > up)  & (dn > 0), dn, 0.0)
    atr_s = pd.Series(tr).ewm(span=period, adjust=False).mean()
    pdi   = 100 * pd.Series(pdm).ewm(span=period, adjust=False).mean() / atr_s.replace(0, np.nan)
    ndi   = 100 * pd.Series(ndm).ewm(span=period, adjust=False).mean() / atr_s.replace(0, np.nan)
    dx    = 100 * (pdi - ndi).abs() / (pdi + ndi).replace(0, np.nan)
    return dx.ewm(span=period, adjust=False).mean().fillna(0)


def check_market_regime(epic, tf=STRATEGY_TF):
    df = fetch_candles(epic, tf, 100)
    if df.empty or len(df) < ADX_PERIOD * 2 + 5:
        return 'UNKNOWN', 1.0, 0.0, 'بيانات ADX غير كافية'
    df_c    = df.iloc[:-1].copy().reset_index(drop=True)
    adx_val = float(calc_adx(df_c, ADX_PERIOD).iloc[-1])
    if np.isnan(adx_val):
        return 'UNKNOWN', 1.0, 0.0, 'ADX=NaN'
    if adx_val >= ADX_STRONG_TREND:
        return 'TRENDING', 1.0,              adx_val, f'ADX={adx_val:.1f} ✅'
    elif adx_val >= ADX_WEAK_TREND:
        return 'WEAK',     ADX_SIZE_REDUCTION, adx_val, f'ADX={adx_val:.1f} ⚠️ حجم×{ADX_SIZE_REDUCTION}'
    return 'SIDEWAYS', 0.0, adx_val, f'ADX={adx_val:.1f} 🚫 عرضي'


def check_htf_confirmation(epic, signal_direction):
    df_h1 = fetch_candles(epic, HTF_TF, HTF_CANDLES)
    if df_h1.empty or len(df_h1) < SUPERTREND_PERIOD * 3 + 10:
        return True, 'UNKNOWN', 'H1 بيانات ناقصة'
    df_h1c    = df_h1.iloc[:-1].copy().reset_index(drop=True)
    _, st_dir = calc_supertrend(df_h1c, SUPERTREND_PERIOD, SUPERTREND_MULT)
    htf_val   = int(st_dir.iloc[-1])
    htf_str   = 'BULLISH' if htf_val == 1 else 'BEARISH'
    if signal_direction == 'BUY'  and htf_val ==  1: return True,  htf_str, 'H1 ✅ BUY'
    if signal_direction == 'SELL' and htf_val == -1: return True,  htf_str, 'H1 ✅ SELL'
    return False, htf_str, f'H1 {htf_str} ≠ {signal_direction}'


def check_correlation_filter(new_pair, new_direction):
    corr = {
        'EURUSD': ['GBPUSD'], 'GBPUSD': ['EURUSD'],
        'GOLD':   ['EURUSD'], 'BTCUSD': [],
        'US100':  ['US500'],  'US500':  ['US100'],
    }
    live = {p['pair']: p['direction'] for p in op_get_all()}
    if new_pair in corr:
        for cp in corr[new_pair]:
            if cp in live and live[cp] == new_direction:
                return False, f'{cp} open'
    return True, 'OK'


# ═══════════════════════════════════════════════════════
# NEWS FILTER
# ═══════════════════════════════════════════════════════

def _fetch_news_calendar():
    now = time.time()
    if now - _news_cache['ts'] < NEWS_CACHE_TTL and _news_cache['data']:
        return _news_cache['data']
    try:
        r = requests.get(NEWS_CALENDAR_URL,
                         headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
        if r.status_code == 200:
            events = [e for e in r.json()
                      if e.get('impact', '').lower() in ('high', 'medium')]
            _news_cache.update({'data': events, 'ts': now})
            log(f'  📰 News: {len(events)} حدث هذا الأسبوع')
            return events
    except Exception as ex:
        log(f'  ⚠️ News fetch: {ex}')
    return _news_cache['data']


def check_news_filter(pair_name):
    if not NEWS_FILTER_ENABLED:
        return True, '', False
    events = _fetch_news_calendar()
    now    = datetime.now(timezone.utc)
    currs  = PAIR_CURRENCIES.get(pair_name, ['USD'])
    for ev in events:
        if ev.get('currency', '').upper() not in currs:
            continue
        impact = ev.get('impact', '').lower()
        try:
            ev_time = datetime.fromisoformat(ev.get('date', ''))
            if ev_time.tzinfo is None:
                ev_time = ev_time.replace(tzinfo=timezone.utc)
            ev_time = ev_time.astimezone(timezone.utc)
        except Exception:
            continue
        until  = (ev_time - now).total_seconds() / 60
        since  = (now - ev_time).total_seconds() / 60
        title  = ev.get('title', '')
        if impact == 'high':
            if -NEWS_BUFFER_MINUTES <= until <= NEWS_BUFFER_MINUTES or \
               0 <= since <= NEWS_BUFFER_MINUTES:
                is_crit = abs(until) <= NEWS_CLOSE_MINUTES
                dir_str = f'بعد {int(since)}د' if since > 0 else f'خلال {int(until)}د'
                return False, f'📰 HIGH: {title} ({ev.get("currency","")}) {dir_str}', is_crit
        elif impact == 'medium' and 0 <= until <= 15:
            return False, f'📰 MEDIUM: {title} خلال {int(until)}د', False
    return True, '', False


def news_close_critical_positions():
    for pos in op_get_all():
        _, _, is_crit = check_news_filter(pos['pair'])
        if is_crit:
            cur = get_current_price(pos['pair'])
            log(f'  📰 إغلاق طارئ: {pos["pair"]}')
            if close_full_api(pos['deal_id']):
                result, _ = csv_log_trade(
                    pos, cur,
                    pos['stage1_done'], pos['stage2_done'], pos['stage3_done'],
                    pos['final_locked_r'], 'NEWS_CLOSE'
                )
                db_update(pos['db_key'], result, 'NEWS_CLOSE')
                op_delete(pos['deal_id'])
                tg(f'📰 *إغلاق طارئ — خبر*\n`{pos["pair"]}`\n_{utc_now()}_')


# ═══════════════════════════════════════════════════════
# SMART EXITS
# ═══════════════════════════════════════════════════════

def calculate_sl_at_r(pos, locked_r):
    d = abs(pos['entry'] - pos['sl'])
    if pos['direction'] == 'BUY':
        return round(pos['entry'] + locked_r * d, 5)
    return round(pos['entry'] - locked_r * d, 5)


def get_progressive_lock(profit_r):
    for thr, locked in sorted(PROGRESSIVE_LOCK.items(), reverse=True):
        if profit_r >= thr:
            return locked
    return 0


def should_move_sl(cur_sl, new_sl, direction):
    if direction == 'BUY':  return new_sl > cur_sl + 0.00001
    return new_sl < cur_sl - 0.00001


def calculate_trailing_sl(pos, cur_price, atr):
    d = atr * TRAILING_ATR_MULT
    if pos['direction'] == 'BUY': return round(cur_price - d, 5)
    return round(cur_price + d, 5)


def manage_smart_exits():
    news_close_critical_positions()
    tracked = op_get_all()
    if not tracked:
        return
    live_pos = get_open_positions()
    live_ids = {p.get('position', {}).get('dealId', '') for p in live_pos}

    for pos in tracked:
        did = pos['deal_id']
        if did not in live_ids:
            ep = get_closed_deal_price(did, get_current_price(pos['pair']))
            if ep > 0:
                result, _ = csv_log_trade(
                    pos, ep, pos['stage1_done'], pos['stage2_done'],
                    pos['stage3_done'], pos['final_locked_r'], 'STOP_OUT'
                )
                if result != 'ERROR':
                    db_update(pos['db_key'],
                              result.upper() if result in ('WIN','LOSS','BE') else 'CLOSED',
                              'STOP_OUT')
                    op_delete(did)
            continue

        cur = get_current_price(pos['pair'])
        if cur <= 0: continue
        entry = pos['entry']; sl = pos['sl']; tp = pos['tp']
        size  = pos['size'];  dir_ = pos['direction']
        sld   = abs(entry - sl)
        if sld <= 0: continue

        profit_r = ((cur - entry) if dir_ == 'BUY' else (entry - cur)) / sld

        bars_held = 0
        if pos.get('opened_at'):
            try:
                od = datetime.strptime(pos['opened_at'], '%Y-%m-%d %H:%M UTC').replace(tzinfo=timezone.utc)
                bars_held = int((datetime.now(timezone.utc) - od).total_seconds() / 60 / M15_MINUTES)
            except Exception:
                bars_held = pos.get('bars_held', 0) + 1
        op_update(did, bars_held=bars_held)

        s1, s2, s3 = pos['stage1_done'], pos['stage2_done'], pos['stage3_done']

        if bars_held > MAX_TRADE_DURATION_BARS and profit_r < 0.5:
            if close_full_api(did):
                result, _ = csv_log_trade(pos, cur, s1, s2, s3, pos['final_locked_r'], 'TIME')
                db_update(pos['db_key'], result, 'TIME')
                op_delete(did)
            continue

        _, _, _, cs, min_sz, _ = get_instrument_meta(pos['pair'])
        stage_action = False

        if not s1 and profit_r >= STAGE1_TP_R:
            psz = math.floor(size * STAGE1_PCT * 100) / 100
            if psz >= min_sz and close_partial_api(did, psz):
                stage_action = True
                be_sl = round(entry + pos['atr'] * 0.1, 5) if dir_ == 'BUY' \
                        else round(entry - pos['atr'] * 0.1, 5)
                if should_move_sl(sl, be_sl, dir_) and update_sl_api(did, be_sl, tp):
                    op_update(did, stage1_done=1, sl=be_sl)
                else:
                    op_update(did, stage1_done=1)

        elif s1 and not s2 and profit_r >= STAGE2_TP_R:
            rem = size * (1 - STAGE1_PCT)
            s2sz = math.floor(rem * (STAGE2_PCT / (1 - STAGE1_PCT)) * 100) / 100
            if s2sz >= min_sz and close_partial_api(did, s2sz):
                stage_action = True
                nsl = calculate_sl_at_r(pos, 0.5)
                if nsl and update_sl_api(did, nsl, tp):
                    op_update(did, stage2_done=1, final_locked_r=0.5, sl=nsl)
                else:
                    op_update(did, stage2_done=1)

        elif s1 and s2 and not s3 and profit_r >= FINAL_TP_R:
            rem  = size * (1 - STAGE1_PCT) * (1 - STAGE2_PCT / (1 - STAGE1_PCT))
            fnsz = math.floor(rem * FINAL_PCT * 100) / 100
            if fnsz >= min_sz and close_partial_api(did, fnsz):
                stage_action = True
                nsl = calculate_sl_at_r(pos, 2.0)
                if nsl and update_sl_api(did, nsl, tp):
                    op_update(did, stage3_done=1, final_locked_r=2.0, sl=nsl)
                else:
                    op_update(did, stage3_done=1)

        if not stage_action:
            if s2:
                nlr = get_progressive_lock(profit_r)
                if nlr > pos['final_locked_r']:
                    nsl = calculate_sl_at_r(pos, nlr)
                    if nsl and should_move_sl(sl, nsl, dir_) and update_sl_api(did, nsl, tp):
                        op_update(did, final_locked_r=nlr, sl=nsl)
            if profit_r >= TRAILING_START_R:
                ltr = pos.get('last_trail_r', 0.0) or 0.0
                if profit_r > ltr + 0.5:
                    dft = fetch_candles(PAIRS[pos['pair']]['epic'], STRATEGY_TF, 50)
                    if not dft.empty:
                        av = float(calc_atr_series(dft.iloc[:-1], ATR_PERIOD).iloc[-1])
                        if av > 0:
                            tsl = calculate_trailing_sl(pos, cur, av)
                            if should_move_sl(sl, tsl, dir_) and update_sl_api(did, tsl, tp):
                                op_update(did, sl=tsl, last_trail_r=profit_r)


# ═══════════════════════════════════════════════════════
# STARTUP RECONCILIATION
# ═══════════════════════════════════════════════════════

def reconcile_positions():
    log('🔄 Reconcile...')
    live = get_open_positions()
    db   = op_get_all()
    lids = {p.get('position', {}).get('dealId', '') for p in live}
    dids = {p['deal_id'] for p in db}
    for pos in db:
        if pos['deal_id'] not in lids:
            ep = get_closed_deal_price(pos['deal_id'], get_current_price(pos['pair']))
            if ep > 0:
                csv_log_trade(pos, ep, pos['stage1_done'], pos['stage2_done'],
                              pos['stage3_done'], pos['final_locked_r'], 'RECONCILE_CLOSE')
            op_delete(pos['deal_id'])
    for lp in live:
        did = lp.get('position', {}).get('dealId', '')
        if did and did not in dids:
            pd_ = lp.get('position', {}); epic = lp.get('market', {}).get('epic', '')
            pn  = next((k for k, v in PAIRS.items() if v['epic'] == epic), epic)
            dk  = f'{pn}_reconciled_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")}'
            op_save(did, pn, pd_.get('direction', ''),
                    float(pd_.get('openLevel', 0) or 0),
                    float(pd_.get('stopLevel', 0) or 0),
                    float(pd_.get('profitLevel', 0) or 0),
                    0.0, float(pd_.get('dealSize', 0) or 0), dk)
    log(f'  ✅ Reconcile OK | Live:{len(lids)} DB:{len(dids)}')


# ═══════════════════════════════════════════════════════
# SIGNAL DETECTION
# ═══════════════════════════════════════════════════════

def check_signal(pair_name, config, session_mult, ecf_factor):
    epic = config['epic']
    if not config['allow_buy'] and not config['allow_sell']:
        return None

    # News
    news_ok, news_reason, _ = check_news_filter(pair_name)
    if not news_ok:
        log(f'  {pair_name}: {news_reason}')
        return None

    # Volatility
    vol_regime, vol_mult = check_volatility_regime(epic)
    if vol_regime == 'EXTREME':
        return None

    # ADX Regime
    regime, adx_sz, adx_val, adx_reason = check_market_regime(epic)
    log(f'  {pair_name}: {adx_reason}')
    if adx_sz == 0.0:
        return None

    # 🛡️ SHIELD #3: Cooldown Check
    cd_ok, cd_factor, cd_reason = cooldown_get_factor(pair_name)
    if not cd_ok:
        log(f'  {pair_name}: {cd_reason}')
        return None
    log(f'  {pair_name}: {cd_reason}')

    # Risk
    dynamic_risk, _ = calculate_dynamic_risk(pair_name, BASE_RISK_PERCENT)
    final_risk       = dynamic_risk * session_mult * vol_mult
    if final_risk < MIN_RISK_PERCENT * 0.5:
        return None

    # Candles M15
    df = fetch_candles(epic, STRATEGY_TF, CANDLES_COUNT)
    if df.empty or len(df) < max(SUPERTREND_PERIOD * 3 + ATR_PERIOD, 100):
        return None
    df_c = df.iloc[:-1].copy().reset_index(drop=True)
    if len(df_c) < 3:
        return None

    st_line, st_dir = calc_supertrend(df_c, SUPERTREND_PERIOD, SUPERTREND_MULT)
    ema_20          = calc_ema(df_c['close'], EMA_PERIOD)
    li, lp          = len(df_c) - 1, len(df_c) - 2

    # Fresh Flip
    if int(st_dir.iloc[li]) == int(st_dir.iloc[lp]):
        return None

    st_now  = int(st_dir.iloc[li])
    lc      = float(df_c['close'].iloc[-1])
    la      = float(calc_atr_series(df_c, ATR_PERIOD).iloc[-1])
    ema_val = float(ema_20.iloc[li])
    if np.isnan(la) or la <= 0 or np.isnan(ema_val):
        return None

    bid, ask, sp, cs, min_sz, max_sz = get_instrument_meta(epic)
    if bid <= 0 or sp > la * SPREAD_ATR_MAX:
        return None

    signal, entry = None, None
    if config['allow_buy']  and st_now ==  1 and lc > ema_val:
        signal, entry = 'BUY',  ask
    elif config['allow_sell'] and st_now == -1 and lc < ema_val:
        signal, entry = 'SELL', bid
    if not signal:
        return None

    # HTF Confirmation
    htf_ok, _, htf_reason = check_htf_confirmation(epic, signal)
    log(f'  {pair_name}: {htf_reason}')
    if not htf_ok:
        return None

    # Correlation
    corr_ok, _ = check_correlation_filter(pair_name, signal)
    if not corr_ok:
        return None

    # SL / TP
    if signal == 'BUY':
        sl = round(entry - SL_ATR_MULT * la - sp, 5)
        tp = round(entry + TP_ATR_MULT * la, 5)
    else:
        sl = round(entry + SL_ATR_MULT * la + sp, 5)
        tp = round(entry - TP_ATR_MULT * la, 5)

    sld = abs(entry - sl)
    if sld < la * 0.1:
        return None

    # 🛡️ SHIELD #2: Heat Check
    heat_ok, heat_factor, heat_reason, cur_heat = heat_check(final_risk)
    log(f'  {pair_name}: {heat_reason}')
    if not heat_ok:
        return None

    # Position Size — تطبيق جميع المضاعفات
    # ECF factor × ADX factor × Cooldown factor × Heat factor
    combined_factor = ecf_factor * adx_sz * cd_factor * heat_factor
    combined_factor = max(0.1, min(combined_factor, 1.0))

    sz_override = config.get('size_override')
    if sz_override:
        size = max(min(float(sz_override), max_sz), min_sz)
    else:
        risk_usd = get_current_balance() * final_risk
        raw_size = risk_usd / (sld * cs) if (sld * cs) > 0 else min_sz
        raw_size *= combined_factor
        size = max(min(math.floor(raw_size * 100) / 100, max_sz), min_sz)

    log(f'  {pair_name}: ✅ {signal} | Size:{size} | '
        f'Factor={combined_factor:.2f} (ECF×{ecf_factor:.1f} ADX×{adx_sz:.1f} '
        f'CD×{cd_factor:.1f} Heat×{heat_factor:.1f})')

    return {
        'pair':          pair_name,
        'epic':          epic,
        'direction':     signal,
        'entry':         round(entry, 5),
        'sl':            sl,
        'tp':            tp,
        'atr':           round(la, 5),
        'size':          size,
        'spread':        round(sp, 5),
        'risk_percent':  round(final_risk, 6),
        'adx_value':     round(adx_val, 2),
        'regime':        regime,
        'htf_confirmed': 1,
        'portfolio_heat': round(cur_heat, 4),
        'ecf_status':    _equity_state['status'],
        'cooldown_active': 0 if cd_factor == 1.0 else 1,
    }


# ═══════════════════════════════════════════════════════
# EXECUTE
# ═══════════════════════════════════════════════════════

def execute_order(sig):
    body = {
        'epic': sig['epic'], 'direction': sig['direction'], 'size': sig['size'],
        'guaranteedStop': False, 'trailingStop': False,
        'stopLevel': sig['sl'], 'profitLevel': sig['tp']
    }
    log(f'  📤 {sig["pair"]} {sig["direction"]} @ {sig["entry"]} | '
        f'Size:{sig["size"]} Risk:{sig["risk_percent"]:.2%}')
    r = _post('/api/v1/positions', body)
    if not r:
        return 'ERROR', 'no response'
    data = r.json()
    if r.status_code == 200:
        ref = data.get('dealReference', 'N/A')
        time.sleep(2)
        rc = _get(f'/api/v1/confirms/{ref}')
        if rc and rc.status_code == 200:
            c      = rc.json()
            status = c.get('dealStatus', 'UNKNOWN')
            did    = c.get('dealId', ref)
            if status in ('ACCEPTED', 'SUCCESS'):
                dk = f'{sig["pair"]}_{datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")}'
                op_save(did, sig['pair'], sig['direction'],
                        sig['entry'], sig['sl'], sig['tp'],
                        sig['atr'], sig['size'], dk)
                log(f'  ✅ Confirmed: {did[:12]}')
            return status, ref
        return 'UNKNOWN', ref
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
        else: break
    return c


# ═══════════════════════════════════════════════════════
# SCAN
# ═══════════════════════════════════════════════════════

def run_scan():
    result = should_trade()
    can_trade, reason = result[0], result[1]
    session_mult, session_name, day_pnl = result[2], result[3], result[4]
    ecf_status,   ecf_factor             = result[5], result[6]

    if not can_trade:
        log(f'⏸ {reason}')
        return

    log('─' * 65)
    log(f'🔍 SCAN | {session_name} | ECF:{ecf_status}(×{ecf_factor:.1f}) | PnL:${day_pnl:+.2f}')
    log('─' * 65)

    get_current_balance()
    manage_smart_exits()
    heat_log_status()
    cooldown_status_all()

    open_pos   = get_open_positions()
    open_pairs = op_get_open_pairs()
    log(f'  Open:{len(open_pos)}/{MAX_OPEN_TRADES}')
    if len(open_pos) >= MAX_OPEN_TRADES:
        return

    now    = datetime.now(timezone.utc)
    ts_key = now.strftime('%Y-%m-%d_%H%M')

    for pair_name, config in PAIRS.items():
        if len(open_pos) >= MAX_OPEN_TRADES:
            break
        if pair_name in open_pairs:
            continue
        if db_consec_losses(pair_name) >= MAX_CONSECUTIVE_LOSS:
            continue
        key = f'{pair_name}_{ts_key}'
        if db_is_dup(key):
            continue

        sig = check_signal(pair_name, config, session_mult, ecf_factor)
        if not sig:
            continue

        _, heat_str, _ = heat_calculate()
        cd_ok, cd_factor, cd_reason = cooldown_get_factor(pair_name)

        db_save(
            key, pair_name, sig['direction'],
            sig['entry'], sig['sl'], sig['tp'],
            sig['atr'], sig['size'], sig['spread'],
            sig['risk_percent'], session_name,
            sig.get('adx_value', 0), sig.get('regime', ''), sig.get('htf_confirmed', 0),
            sig.get('portfolio_heat', 0), ecf_status, sig.get('cooldown_active', 0)
        )
        tg_signal(sig, session_name, heat_str, ecf_status, cd_reason)

        status, ref = execute_order(sig)
        db_update(key, status)
        log(f'  {pair_name}: {status}')

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
                    time.sleep(60); continue
                reconcile_positions()
                _fetch_news_calendar()
                ecf_update(get_current_balance())
            else:
                if not ping_session():
                    session_age = 0; continue

            session_age = (session_age + 1) % 15
            run_scan()
            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            log('🛑 Bot stopped')
            ecf_save_state()
            cooldown_save_state()
            break
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

    # تحميل الحالات المحفوظة
    ecf_load_state()
    cooldown_load_state()

    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    print('=' * 70, flush=True)
    print(f'  🚀 Bot v4 SHIELD | {mode}', flush=True)
    print(f'  ST({SUPERTREND_PERIOD},{SUPERTREND_MULT}|{ATR_METHOD})+EMA{EMA_PERIOD}+ADX{ADX_PERIOD}+H1', flush=True)
    print(f'  🛡️  ECF: Max DD={ECF_MAX_DD_PCT:.0%} | Warning={ECF_WARNING_PCT:.0%} | Resume={ECF_RESUME_PCT:.0%}', flush=True)
    print(f'  🌡️  Heat: Max={HEAT_MAX_PCT:.0%} | Warn={HEAT_WARN_PCT:.0%}', flush=True)
    print(f'  ⏳ Cooldown: 3L→{COOLDOWN_RULES[3]["minutes"]}د | '
          f'4L→{COOLDOWN_RULES[4]["minutes"]}د | '
          f'5L→{COOLDOWN_RULES[5]["minutes"]}د', flush=True)
    print('=' * 70, flush=True)

    nl = '\n'
    tg(f'🚀 *Bot v4 SHIELD* [{mode}]{nl}'
       f'🛡️ ECF DD≤{ECF_MAX_DD_PCT:.0%} | 🌡️ Heat≤{HEAT_MAX_PCT:.0%} | ⏳ Cooldown ON{nl}'
       f'📰 News | 🕐 H1 | 📊 ADX{nl}'
       f'_{utc_now()}_')

    main_loop()


if __name__ == '__main__':
    start_bot()
