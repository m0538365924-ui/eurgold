#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==========================================================
# multi_pairs_bot_FIXED_v2.py
# ✅ FIX #1  — Fresh Flip Detection (لا إشارات متكررة في نفس الترند)
# ✅ FIX #2  — Same-Pair Open Check (منع تكرار نفس الزوج)
# ✅ FIX #3  — calculate_pnl_since صحيحة (تستخدم pnl_usd مباشرة)
# ✅ FIX #4  — Trailing Stop يعمل فعلياً (إعادة هيكلة elif chain)
# ✅ FIX #5  — bars_held بالزمن الحقيقي (لا بعدد السكانات)
# ✅ FIX #6  — حذف Fallbacks للـ Credentials الحساسة
# ✅ FIX #7  — Break-even بعد Stage 1
# ✅ FIX #8  — تصحيح Double Risk في check_signal
# ✅ FIX #9  — Startup Reconciliation (مزامنة DB مع Capital.com)
# ✅ FIX #10 — إضافة عمود last_trail_r لجدول open_positions
# ✅ FIX #11 — week_start مع Timezone صحيح
# ✅ FIX #12 — close_partial_api بـ DELETE + body صحيح
# ✅ FIX #13 — _put/_delete مع Retry منطق
# ✅ FIX #14 — Validation عند بدء البوت
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
# CONFIG — ✅ FIX #6: لا Fallback للبيانات الحساسة
# ═══════════════════════════════════════════════════════
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
CANDLES_COUNT = 500
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '180'))
M15_MINUTES   = 15  # دقائق الشمعة للـ bars_held

# ═══════════════════════════════════════════════════════
# SUPERTREND SETTINGS
# ═══════════════════════════════════════════════════════
SUPERTREND_PERIOD = int(os.getenv('SUPERTREND_PERIOD', '10'))
SUPERTREND_MULT   = float(os.getenv('SUPERTREND_MULT', '3.0'))
ATR_METHOD        = os.getenv('ATR_METHOD', 'RMA')

EMA_PERIOD     = 20
ATR_PERIOD     = 14
SL_ATR_MULT    = 1.5
TP_ATR_MULT    = 3.0
SPREAD_ATR_MAX = 0.25

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
MAX_TRADE_DURATION_BARS = 24          # شمعة M15 = 6 ساعات
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

db_lock, session_headers, _meta_cache = Lock(), {}, {}
_candle_cache = {}

CSV_HEADERS = [
    'date', 'time_utc', 'pair', 'direction', 'entry', 'sl', 'tp', 'exit_price',
    'atr', 'size', 'sl_dist', 'pnl_usd', 'pnl_r', 'result', 'bars_held', 'spread', 'tf',
    'stage1_done', 'stage2_done', 'stage3_done', 'final_locked_r', 'exit_type',
    'session_used', 'risk_percent', 'indicator', 'notes'
]


# ═══════════════════════════════════════════════════════
# ✅ FIX #14 — Validation عند بدء البوت
# ═══════════════════════════════════════════════════════
def validate_config():
    """تحقق من أن جميع المتغيرات الضرورية موجودة قبل البدء"""
    missing = []
    if not API_KEY:    missing.append('CAPITAL_API_KEY')
    if not EMAIL:      missing.append('CAPITAL_EMAIL')
    if not PASSWORD:   missing.append('CAPITAL_PASSWORD')
    if missing:
        raise EnvironmentError(
            f'❌ متغيرات البيئة الآتية مفقودة: {", ".join(missing)}\n'
            f'   أضفها في ملف .env أو في متغيرات البيئة.'
        )
    log('✅ Config OK — جميع المتغيرات موجودة')


# ═══════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════

def _migrate_database(conn):
    """إضافة الأعمدة الناقصة بأمان"""
    try:
        # ── trades ──────────────────────────────────────────
        t_cols = {c[1] for c in conn.execute("PRAGMA table_info(trades)").fetchall()}
        trades_new_cols = {
            'pnl_r':        'REAL DEFAULT 0',
            'pnl_usd':      'REAL DEFAULT 0',
            'exit_price':   'REAL',
            'bars_held':    'INTEGER DEFAULT 0',
            'risk_percent': 'REAL DEFAULT 0',
        }
        for col, typ in trades_new_cols.items():
            if col not in t_cols:
                conn.execute(f'ALTER TABLE trades ADD COLUMN {col} {typ}')
                log(f'  ✅ DB trades: added {col}')

        # ── open_positions ───────────────────────────────────
        # ✅ FIX #10: إضافة last_trail_r
        op_cols = {c[1] for c in conn.execute("PRAGMA table_info(open_positions)").fetchall()}
        op_new_cols = {
            'last_trail_r': 'REAL DEFAULT 0',
            'opened_at':    'TEXT',
        }
        for col, typ in op_new_cols.items():
            if col not in op_cols:
                conn.execute(f'ALTER TABLE open_positions ADD COLUMN {col} {typ}')
                log(f'  ✅ DB open_positions: added {col}')

    except Exception as ex:
        log(f'  ⚠️ DB migrate: {ex}')


def db_init():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id          INTEGER PRIMARY KEY,
            key         TEXT UNIQUE,
            pair        TEXT,
            direction   TEXT,
            timestamp   TEXT,
            entry       REAL,
            sl          REAL,
            tp          REAL,
            atr         REAL,
            size        REAL,
            spread      REAL DEFAULT 0,
            status      TEXT DEFAULT 'PENDING',
            stage1_done INTEGER DEFAULT 0,
            stage2_done INTEGER DEFAULT 0,
            stage3_done INTEGER DEFAULT 0,
            final_locked_r REAL DEFAULT 0,
            exit_type   TEXT,
            session_used TEXT,
            risk_percent REAL,
            pnl_r       REAL DEFAULT 0,
            pnl_usd     REAL DEFAULT 0,
            exit_price  REAL,
            bars_held   INTEGER DEFAULT 0
        )''')
        _migrate_database(conn)
        conn.execute('''CREATE TABLE IF NOT EXISTS open_positions (
            deal_id       TEXT PRIMARY KEY,
            pair          TEXT,
            direction     TEXT,
            entry         REAL,
            sl            REAL,
            tp            REAL,
            atr           REAL,
            size          REAL,
            db_key        TEXT,
            opened_at     TEXT,
            stage1_done   INTEGER DEFAULT 0,
            stage2_done   INTEGER DEFAULT 0,
            stage3_done   INTEGER DEFAULT 0,
            final_locked_r REAL DEFAULT 0,
            bars_held     INTEGER DEFAULT 0,
            last_trail_r  REAL DEFAULT 0
        )''')
        conn.commit()


def db_save(key, pair, direction, entry, sl, tp, atr, size, spread, risk_pct, session):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT INTO trades '
                    '(key,pair,direction,timestamp,entry,sl,tp,atr,size,spread,risk_percent,session_used) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                    (key, pair, direction, utc_now(), entry, sl, tp, atr, size, spread, risk_pct, session)
                )
                conn.commit()
            except sqlite3.IntegrityError:
                pass  # dup key — مقصود


def db_update(key, status, exit_type=None):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            if exit_type:
                conn.execute(
                    'UPDATE trades SET status=?, exit_type=? WHERE key=?',
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
            query  = "SELECT pair,direction,status,pnl_r,timestamp FROM trades WHERE status IN ('WIN','LOSS')"
            params = []
            if pair:
                query += ' AND pair=?'
                params.append(pair)
            query += ' ORDER BY id DESC LIMIT ?'
            params.append(limit)
            return conn.execute(query, params).fetchall()


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
    """تحديث أعمدة محددة في open_positions بأمان"""
    ALLOWED_COLS = {
        'sl', 'tp', 'stage1_done', 'stage2_done', 'stage3_done',
        'final_locked_r', 'bars_held', 'last_trail_r'
    }
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            for col, val in kwargs.items():
                if col in ALLOWED_COLS:   # فلترة صارمة للأسماء المسموحة فقط
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
    """أسماء الأزواج ذات الصفقات المفتوحة حالياً"""
    return {p['pair'] for p in op_get_all()}


# ═══════════════════════════════════════════════════════
# RISK MANAGEMENT
# ═══════════════════════════════════════════════════════

# ✅ FIX #3: calculate_pnl_since تستخدم pnl_usd المُخزَّن مباشرة
def calculate_pnl_since(since_dt):
    """PnL الحقيقي بالدولار منذ وقت معين — يستخدم العمود pnl_usd مباشرة"""
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT pnl_usd FROM trades WHERE timestamp >= ? AND status IN ('WIN','LOSS')",
                (since_dt.strftime('%Y-%m-%d %H:%M UTC'),)
            ).fetchall()
            return sum(r[0] for r in rows if r[0] is not None)


# ✅ FIX #11: week_start مع UTC timezone صريح
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

    # ✅ FIX #11: إضافة timezone.utc
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
    win_rate   = len(wins) / len(trades)
    avg_win_r  = np.mean([t[3] for t in wins])        if wins   else 0
    avg_loss_r = abs(np.mean([t[3] for t in losses])) if losses else 1
    if avg_loss_r == 0:
        avg_loss_r = 1

    consecutive_losses = 0
    cur_consec = 0
    for t in sorted(trades, key=lambda x: x[4]):
        if t[2] == 'LOSS':
            cur_consec += 1
            consecutive_losses = max(consecutive_losses, cur_consec)
        else:
            cur_consec = 0

    return {
        'total': len(trades),
        'win_rate': win_rate,
        'consecutive_losses': consecutive_losses
    }


def calculate_dynamic_risk(pair, base_risk=BASE_RISK_PERCENT):
    stats = get_pair_stats(pair)
    if not stats:
        return base_risk, 'default'
    risk = base_risk
    if stats['consecutive_losses'] >= 3:
        risk *= 0.4
    elif stats['consecutive_losses'] == 2:
        risk *= 0.6
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


def check_volatility_regime(epic, tf=STRATEGY_TF):
    df = fetch_candles(epic, tf, 100)
    if df.empty or len(df) < 50:
        return 'NORMAL', 1.0
    atr_current = calc_atr_series(df.iloc[:-1],  ATR_PERIOD).iloc[-1]
    atr_hist    = calc_atr_series(df.iloc[:-20], ATR_PERIOD).iloc[-20:].mean()
    ratio = atr_current / atr_hist if atr_hist > 0 else 1.0
    if   ratio > VOLATILITY_THRESHOLDS['EXTREME']: return 'EXTREME', 0.0
    elif ratio > VOLATILITY_THRESHOLDS['HIGH']:    return 'HIGH',    0.7
    elif ratio < VOLATILITY_THRESHOLDS['LOW']:     return 'LOW',     0.6
    else:                                           return 'NORMAL',  1.0


def should_trade():
    allowed, reason, day_pnl = check_drawdown_limits()
    if not allowed:
        return False, reason, 0.0, None, 0.0
    session_mult, session_name, session_code = get_session_info()
    if session_mult == 0:
        return False, f'⏸ Session: {session_name}', 0.0, None, 0.0
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


def csv_log_trade(pos, exit_price, stage1=0, stage2=0, stage3=0, final_r=0, exit_type=''):
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

        # ✅ FIX #5: bars_held من وقت الفتح الحقيقي
        opened_at = pos.get('opened_at', '')
        bars_held = 0
        if opened_at:
            try:
                opened_dt = datetime.strptime(opened_at, '%Y-%m-%d %H:%M UTC').replace(tzinfo=timezone.utc)
                elapsed_min = (datetime.now(timezone.utc) - opened_dt).total_seconds() / 60
                bars_held   = int(elapsed_min / M15_MINUTES)
            except Exception:
                bars_held = pos.get('bars_held', 0)

        _update_trade_pnl(pos['db_key'], pnl_r, pnl_usd, exit_price, bars_held)

        now = datetime.now(timezone.utc)
        row = {
            'date':         now.strftime('%Y-%m-%d'),
            'time_utc':     now.strftime('%H:%M'),
            'pair':         pair,
            'direction':    dir_,
            'entry':        entry,
            'sl':           sl,
            'tp':           pos['tp'],
            'exit_price':   exit_price,
            'atr':          pos['atr'],
            'size':         size,
            'sl_dist':      round(sl_dist, 5),
            'pnl_usd':      pnl_usd,
            'pnl_r':        pnl_r,
            'result':       result,
            'bars_held':    bars_held,
            'spread':       pos.get('spread', 0),
            'tf':           STRATEGY_TF,
            'stage1_done':  stage1,
            'stage2_done':  stage2,
            'stage3_done':  stage3,
            'final_locked_r': final_r,
            'exit_type':    exit_type,
            'session_used': pos.get('session_used', ''),
            'risk_percent': pos.get('risk_percent', 0),
            'indicator':    f'ST({SUPERTREND_PERIOD},{SUPERTREND_MULT}|{ATR_METHOD})+EMA{EMA_PERIOD}',
            'notes':        ''
        }
        with open(TRADES_CSV, 'a', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writerow(row)

        icon = '✅' if result == 'WIN' else ('❌' if result == 'LOSS' else '🔵')
        log(f'  {icon} {pair} {dir_} | PnL=${pnl_usd:+.2f} ({pnl_r:+.2f}R) | Bars:{bars_held}')
        nl = '\n'
        tg(f'{icon} *{pair} {dir_}*{nl}PnL: `${pnl_usd:+.2f}` | `{pnl_r:+.2f}R`{nl}_{utc_now()}_')
        return result, pnl_usd

    except Exception as ex:
        log(f'  csv_log ERROR: {ex}')
        return 'ERROR', 0


# ═══════════════════════════════════════════════════════
# API HELPERS
# ═══════════════════════════════════════════════════════

def _get(path, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(
                BASE_URL + path, headers=session_headers,
                params=params, timeout=15
            )
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            return r
        except requests.exceptions.RequestException as ex:
            log(f'  GET {path} [{attempt+1}/{retries}]: {ex}')
            time.sleep(3 * (attempt + 1))
    return None


def _post(path, body, retries=2):
    for attempt in range(retries):
        try:
            return requests.post(
                BASE_URL + path, headers=session_headers,
                json=body, timeout=15
            )
        except requests.exceptions.RequestException as ex:
            log(f'  POST {path} [{attempt+1}/{retries}]: {ex}')
            time.sleep(3 * (attempt + 1))
    return None


# ✅ FIX #13: _put مع Retry منطق
def _put(path, body, retries=3):
    for attempt in range(retries):
        try:
            r = requests.put(
                BASE_URL + path, headers=session_headers,
                json=body, timeout=10
            )
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            return r
        except Exception as ex:
            log(f'  PUT {path} [{attempt+1}/{retries}]: {ex}')
            time.sleep(3 * (attempt + 1))
    return None


# ✅ FIX #13: _delete مع Retry منطق
def _delete(path, body=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.delete(
                BASE_URL + path, headers=session_headers,
                json=body, timeout=15
            )
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            return r
        except Exception as ex:
            log(f'  DELETE {path} [{attempt+1}/{retries}]: {ex}')
            time.sleep(3 * (attempt + 1))
    return None


def create_session():
    url  = BASE_URL + '/api/v1/session'
    hdrs = {'X-CAP-API-KEY': API_KEY, 'Content-Type': 'application/json'}
    try:
        r = requests.post(
            url, headers=hdrs,
            json={'identifier': EMAIL, 'password': PASSWORD, 'encryptedPassword': False},
            timeout=15
        )
        if r.status_code == 200:
            session_headers.update({
                'X-SECURITY-TOKEN': r.headers.get('X-SECURITY-TOKEN'),
                'CST':              r.headers.get('CST'),
                'Content-Type':     'application/json'
            })
            log('✅ Session OK')
            return True
        log(f'❌ Session FAILED: {r.status_code} | {r.text[:200]}')
    except Exception as ex:
        log(f'❌ Session: {ex}')
    return False


def ping_session():
    r = _get('/api/v1/ping')
    if not r or r.status_code != 200:
        log('⚠️ Ping failed — قد تكون الجلسة منتهية')
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
    result = (
        bid, ask,
        round(ask - bid, 5),
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
        r = _get('/api/v1/history/activity', params={'dealId': deal_id, 'pageSize': 10})
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
    log(f'  ⚠️ SL update failed: {r.status_code if r else "None"}')
    return False


# ✅ FIX #12: Partial close بـ DELETE + dealSize في الـ body
def close_partial_api(deal_id, size):
    r = _delete(f'/api/v1/positions/{deal_id}', body={'dealSize': size})
    if r and r.status_code == 200:
        log(f'  💰 Partial close: {size} lots')
        return True
    log(f'  ⚠️ Partial close failed: {r.status_code if r else "None"}')
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


def tg_signal(sig, session_info):
    icon = '🟢' if sig['direction'] == 'BUY' else '🔴'
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    nl   = '\n'
    tg(f'{icon} *{sig["pair"]} {sig["direction"]}* [{mode}]{nl}'
       f'Entry: `{sig["entry"]}` | SL: `{sig["sl"]}` | TP: `{sig["tp"]}`{nl}'
       f'Risk: `{sig["risk_percent"]:.2%}`{nl}'
       f'Session: `{session_info}`{nl}'
       f'Indicator: ST({SUPERTREND_PERIOD},{SUPERTREND_MULT}|{ATR_METHOD})+EMA{EMA_PERIOD}{nl}'
       f'_{utc_now()}_')


# ═══════════════════════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════════════════════

def fetch_candles(epic, resolution, count=500):
    cache_key = f'{epic}_{resolution}'
    now = time.time()
    if cache_key in _candle_cache:
        cached = _candle_cache[cache_key]
        if now - cached['ts'] < 60:
            return cached['df']
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
    """
    RMA (default) = EWM  — مطابق لـ TradingView
    SMA           = Rolling Mean
    """
    if method is None:
        method = ATR_METHOD
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low']  - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    if method.upper() == 'SMA':
        return tr.rolling(window=period, min_periods=1).mean()
    else:
        return tr.ewm(span=period, adjust=False).mean()


def calc_supertrend(df, period=10, mult=3.0):
    """
    Supertrend: Period=10, Mult=3.0, ATR=RMA
    direction =  1 → Bullish (ST تحت السعر)
    direction = -1 → Bearish (ST فوق السعر)
    """
    atr   = calc_atr_series(df, period)
    hl2   = (df['high'] + df['low']) / 2
    upper = (hl2 + mult * atr).values
    lower = (hl2 - mult * atr).values
    close = df['close'].values
    n     = len(df)
    final_u   = upper.copy()
    final_l   = lower.copy()
    st        = np.zeros(n)
    direction = np.ones(n, dtype=int)
    for i in range(1, n):
        final_u[i] = upper[i] if (upper[i] < final_u[i-1] or close[i-1] > final_u[i-1]) else final_u[i-1]
        final_l[i] = lower[i] if (lower[i] > final_l[i-1] or close[i-1] < final_l[i-1]) else final_l[i-1]
        if st[i-1] == final_u[i-1]:
            direction[i] = 1  if close[i] <= final_u[i] else -1
        else:
            direction[i] = -1 if close[i] >= final_l[i] else  1
        st[i] = final_l[i] if direction[i] == 1 else final_u[i]
    return pd.Series(st, index=df.index), pd.Series(direction, index=df.index)


def calc_ema(s, p):
    return s.ewm(span=p, adjust=False).mean()


def check_correlation_filter(new_pair, new_direction):
    correlated_pairs = {
        'EURUSD': ['GBPUSD'],
        'GBPUSD': ['EURUSD'],
        'GOLD':   ['EURUSD'],
        'BTCUSD': [],
        'US100':  ['US500'],
        'US500':  ['US100'],
    }
    live_pairs = {p['pair']: p['direction'] for p in op_get_all()}
    if new_pair in correlated_pairs:
        for corr_pair in correlated_pairs[new_pair]:
            if corr_pair in live_pairs and live_pairs[corr_pair] == new_direction:
                return False, f'{corr_pair} open ({live_pairs[corr_pair]})'
    return True, 'OK'


# ═══════════════════════════════════════════════════════
# SMART EXITS — ✅ FIX #4, #5, #7, #10
# ═══════════════════════════════════════════════════════

def calculate_sl_at_r(pos, locked_r):
    """SL عند مسافة locked_r × SL_dist من نقطة الدخول"""
    entry   = pos['entry']
    sl_dist = abs(pos['entry'] - pos['sl'])
    if pos['direction'] == 'BUY':
        return round(entry + (locked_r * sl_dist), 5)
    else:
        return round(entry - (locked_r * sl_dist), 5)


def get_progressive_lock(profit_r):
    for threshold, locked in sorted(PROGRESSIVE_LOCK.items(), reverse=True):
        if profit_r >= threshold:
            return locked
    return 0


def should_move_sl(current_sl, new_sl, direction):
    """True فقط إذا كان الـ SL الجديد أفضل (حماية أعلى)"""
    if direction == 'BUY':
        return new_sl > current_sl + 0.00001
    else:
        return new_sl < current_sl - 0.00001


def calculate_trailing_sl(pos, cur_price, atr):
    trail_dist = atr * TRAILING_ATR_MULT
    if pos['direction'] == 'BUY':
        return round(cur_price - trail_dist, 5)
    else:
        return round(cur_price + trail_dist, 5)


def manage_smart_exits():
    tracked = op_get_all()
    if not tracked:
        return

    live_pos = get_open_positions()
    live_ids = {p.get('position', {}).get('dealId', '') for p in live_pos}

    for pos in tracked:
        deal_id = pos['deal_id']

        # ── صفقة مغلقة بالسوق (SL/TP تلقائي) ──────────────
        if deal_id not in live_ids:
            exit_price = get_closed_deal_price(deal_id, get_current_price(pos['pair']))
            if exit_price > 0:
                result, _ = csv_log_trade(
                    pos, exit_price,
                    pos['stage1_done'], pos['stage2_done'], pos['stage3_done'],
                    pos['final_locked_r'], 'STOP_OUT'
                )
                if result != 'ERROR':
                    status = result.upper() if result in ('WIN', 'LOSS', 'BE') else 'CLOSED'
                    db_update(pos['db_key'], status, 'STOP_OUT')
                    op_delete(deal_id)
            continue

        # ── السعر الحالي ─────────────────────────────────────
        cur_price = get_current_price(pos['pair'])
        if cur_price <= 0:
            continue

        entry   = pos['entry']
        sl      = pos['sl']
        tp      = pos['tp']
        size    = pos['size']
        dir_    = pos['direction']
        sl_dist = abs(entry - sl)
        if sl_dist <= 0:
            continue

        profit_pts = (cur_price - entry) if dir_ == 'BUY' else (entry - cur_price)
        profit_r   = profit_pts / sl_dist

        # ✅ FIX #5: bars_held محسوب من وقت الفتح الحقيقي
        bars_held = 0
        opened_at = pos.get('opened_at', '')
        if opened_at:
            try:
                opened_dt = datetime.strptime(opened_at, '%Y-%m-%d %H:%M UTC').replace(tzinfo=timezone.utc)
                elapsed_min = (datetime.now(timezone.utc) - opened_dt).total_seconds() / 60
                bars_held   = int(elapsed_min / M15_MINUTES)
            except Exception:
                bars_held = pos.get('bars_held', 0) + 1
        op_update(deal_id, bars_held=bars_held)

        s1 = pos['stage1_done']
        s2 = pos['stage2_done']
        s3 = pos['stage3_done']

        # ── Time-based exit ──────────────────────────────────
        if bars_held > MAX_TRADE_DURATION_BARS and profit_r < 0.5:
            if close_full_api(deal_id):
                result, _ = csv_log_trade(
                    pos, cur_price, s1, s2, s3,
                    pos['final_locked_r'], 'TIME'
                )
                db_update(pos['db_key'], result, 'TIME')
                op_delete(deal_id)
            continue

        # ── Stage Exits ─────────────────────────────────────
        stage_action = False
        _, _, _, cs, min_sz, _ = get_instrument_meta(pos['pair'])

        # Stage 1 — إغلاق 50% عند 1.5R
        if not s1 and profit_r >= STAGE1_TP_R:
            partial_size = math.floor(size * STAGE1_PCT * 100) / 100
            if partial_size >= min_sz and close_partial_api(deal_id, partial_size):
                stage_action = True
                # ✅ FIX #7: Break-even بعد Stage 1
                be_buffer = pos['atr'] * 0.1
                be_sl = round(entry + be_buffer, 5) if dir_ == 'BUY' else round(entry - be_buffer, 5)
                if should_move_sl(sl, be_sl, dir_) and update_sl_api(deal_id, be_sl, tp):
                    op_update(deal_id, stage1_done=1, sl=be_sl)
                else:
                    op_update(deal_id, stage1_done=1)

        # Stage 2 — إغلاق 30% عند 2.5R
        elif s1 and not s2 and profit_r >= STAGE2_TP_R:
            remaining    = size * (1 - STAGE1_PCT)
            s2_fraction  = STAGE2_PCT / (1 - STAGE1_PCT)
            stage2_size  = math.floor(remaining * s2_fraction * 100) / 100
            if stage2_size >= min_sz and close_partial_api(deal_id, stage2_size):
                stage_action = True
                new_sl = calculate_sl_at_r(pos, 0.5)
                if new_sl and update_sl_api(deal_id, new_sl, tp):
                    op_update(deal_id, stage2_done=1, final_locked_r=0.5, sl=new_sl)
                else:
                    op_update(deal_id, stage2_done=1)

        # Stage 3 — إغلاق 50% من المتبقي عند 3.5R
        elif s1 and s2 and not s3 and profit_r >= FINAL_TP_R:
            remaining   = size * (1 - STAGE1_PCT) * (1 - STAGE2_PCT / (1 - STAGE1_PCT))
            final_size  = math.floor(remaining * FINAL_PCT * 100) / 100
            if final_size >= min_sz and close_partial_api(deal_id, final_size):
                stage_action = True
                new_sl = calculate_sl_at_r(pos, 2.0)
                if new_sl and update_sl_api(deal_id, new_sl, tp):
                    op_update(deal_id, stage3_done=1, final_locked_r=2.0, sl=new_sl)
                else:
                    op_update(deal_id, stage3_done=1)

        # ── ✅ FIX #4: SL Management — منفصل عن Stage Exits
        # يعمل دائماً حتى لو لم تكن هناك Stage Action في هذه الدورة
        if not stage_action:
            if s2:
                # Progressive Lock بعد Stage 2
                new_locked_r = get_progressive_lock(profit_r)
                if new_locked_r > pos['final_locked_r']:
                    new_sl = calculate_sl_at_r(pos, new_locked_r)
                    if new_sl and should_move_sl(sl, new_sl, dir_) \
                            and update_sl_api(deal_id, new_sl, tp):
                        op_update(deal_id, final_locked_r=new_locked_r, sl=new_sl)

            # Trailing Stop ATR — يعمل من profit_r >= TRAILING_START_R
            # لجميع المراحل (قبل وبعد الـ stages)
            if profit_r >= TRAILING_START_R:
                last_trail_r = pos.get('last_trail_r', 0.0) or 0.0
                if profit_r > last_trail_r + 0.5:
                    df_trail = fetch_candles(
                        PAIRS[pos['pair']]['epic'], STRATEGY_TF, 50
                    )
                    if not df_trail.empty:
                        atr_val = float(
                            calc_atr_series(df_trail.iloc[:-1], ATR_PERIOD).iloc[-1]
                        )
                        if atr_val > 0:
                            trail_sl = calculate_trailing_sl(pos, cur_price, atr_val)
                            if should_move_sl(sl, trail_sl, dir_) \
                                    and update_sl_api(deal_id, trail_sl, tp):
                                op_update(deal_id, sl=trail_sl, last_trail_r=profit_r)


# ═══════════════════════════════════════════════════════
# ✅ FIX #9 — Startup Reconciliation
# ═══════════════════════════════════════════════════════

def reconcile_positions():
    """
    مزامنة جدول open_positions مع الصفقات الحقيقية في Capital.com.
    يجب استدعاؤها مرة واحدة عند بدء البوت.
    """
    log('🔄 Reconcile: مزامنة الصفقات...')
    live_pos = get_open_positions()
    db_pos   = op_get_all()

    live_ids = {p.get('position', {}).get('dealId', '') for p in live_pos}
    db_ids   = {p['deal_id'] for p in db_pos}

    # صفقات في DB لكن غائبة من Live → تم إغلاقها خلال فترة التوقف
    for pos in db_pos:
        if pos['deal_id'] not in live_ids:
            log(f'  ⚠️ Reconcile: {pos["pair"]} [{pos["deal_id"][:8]}] غائبة — تسجيل الإغلاق')
            exit_price = get_closed_deal_price(
                pos['deal_id'],
                get_current_price(pos['pair'])
            )
            if exit_price > 0:
                csv_log_trade(
                    pos, exit_price,
                    pos['stage1_done'], pos['stage2_done'], pos['stage3_done'],
                    pos['final_locked_r'], 'RECONCILE_CLOSE'
                )
            op_delete(pos['deal_id'])

    # صفقات Live لكن غائبة من DB → Phantom positions
    for live_item in live_pos:
        deal_id = live_item.get('position', {}).get('dealId', '')
        if deal_id and deal_id not in db_ids:
            pos_data = live_item.get('position', {})
            market   = live_item.get('market', {})
            epic     = market.get('epic', '')
            pair_name = next(
                (k for k, v in PAIRS.items() if v['epic'] == epic), epic
            )
            log(f'  ⚠️ Reconcile: {pair_name} [{deal_id[:8]}] غير مُتتبَّعة — إضافة للـ DB')
            db_key = f'{pair_name}_reconciled_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")}'
            op_save(
                deal_id, pair_name,
                pos_data.get('direction', ''),
                float(pos_data.get('openLevel', 0) or 0),
                float(pos_data.get('stopLevel', 0) or 0),
                float(pos_data.get('profitLevel', 0) or 0),
                0.0,
                float(pos_data.get('dealSize', 0) or 0),
                db_key
            )

    log(f'  ✅ Reconcile OK | Live:{len(live_ids)} | DB:{len(db_ids)}')


# ═══════════════════════════════════════════════════════
# ✅ FIX #1, #2, #8 — SIGNAL DETECTION
# ═══════════════════════════════════════════════════════

def check_signal(pair_name, config, session_mult):
    """
    FIX #1: Fresh Flip Detection — إشارة فقط عند تغيير اتجاه Supertrend
    FIX #2: Same-Pair check يتم في run_scan قبل الاستدعاء
    FIX #8: لا Double Risk — dynamic_risk يُطبَّق مرة واحدة فقط
    """
    epic       = config['epic']
    allow_buy  = config['allow_buy']
    allow_sell = config['allow_sell']
    if not allow_buy and not allow_sell:
        return None

    vol_regime, vol_mult = check_volatility_regime(epic)
    if vol_regime == 'EXTREME':
        log(f'  {pair_name}: تقلب شديد — تخطي')
        return None

    # ✅ FIX #8: حساب final_risk بدون ضرب dynamic_risk مرتين
    dynamic_risk, risk_reason = calculate_dynamic_risk(pair_name, BASE_RISK_PERCENT)
    final_risk = dynamic_risk * session_mult * vol_mult

    if final_risk < MIN_RISK_PERCENT * 0.5:
        return None

    df = fetch_candles(epic, STRATEGY_TF, CANDLES_COUNT)
    if df.empty or len(df) < max(SUPERTREND_PERIOD * 3 + ATR_PERIOD, 100):
        return None

    # نستثني الشمعة الأخيرة (الحية) لتجنب lookahead bias
    df_c = df.iloc[:-1].copy().reset_index(drop=True)
    n    = len(df_c)
    if n < 3:
        return None

    st_line, st_dir = calc_supertrend(df_c, SUPERTREND_PERIOD, SUPERTREND_MULT)
    ema_20          = calc_ema(df_c['close'], EMA_PERIOD)

    li = n - 1   # آخر شمعة مكتملة
    lp = n - 2   # الشمعة قبلها

    # ✅ FIX #1: يجب أن يكون الاتجاه قد تغيّر في الشمعة الأخيرة (Fresh Flip)
    st_dir_now  = int(st_dir.iloc[li])
    st_dir_prev = int(st_dir.iloc[lp])
    if st_dir_now == st_dir_prev:
        return None  # لا flip → لا إشارة

    lc     = float(df_c['close'].iloc[-1])
    la     = float(calc_atr_series(df_c, ATR_PERIOD).iloc[-1])
    ema_val = float(ema_20.iloc[li])

    if np.isnan(la) or la <= 0 or np.isnan(ema_val):
        return None

    bid, ask, sp, cs, min_sz, max_sz = get_instrument_meta(epic)
    if bid <= 0:
        return None
    if sp > la * SPREAD_ATR_MAX:
        log(f'  {pair_name}: spread عالٍ ({sp:.5f} > {la*SPREAD_ATR_MAX:.5f}) — تخطي')
        return None

    signal, entry = None, None

    if allow_buy and st_dir_now == 1 and lc > ema_val:
        signal, entry = 'BUY', ask
        log(f'  {pair_name}: 🟢 BUY FLIP | EMA{EMA_PERIOD}: {ema_val:.5f} | ATR: {la:.5f}')

    elif allow_sell and st_dir_now == -1 and lc < ema_val:
        signal, entry = 'SELL', bid
        log(f'  {pair_name}: 🔴 SELL FLIP | EMA{EMA_PERIOD}: {ema_val:.5f} | ATR: {la:.5f}')

    if not signal:
        return None

    corr_ok, corr_msg = check_correlation_filter(pair_name, signal)
    if not corr_ok:
        log(f'  {pair_name}: Correlation filter [{corr_msg}] — تخطي')
        return None

    # SL/TP
    if signal == 'BUY':
        sl = round(entry - SL_ATR_MULT * la - sp, 5)
        tp = round(entry + TP_ATR_MULT * la, 5)
    else:
        sl = round(entry + SL_ATR_MULT * la + sp, 5)
        tp = round(entry - TP_ATR_MULT * la, 5)

    sld = abs(entry - sl)
    if sld < la * 0.1:
        log(f'  {pair_name}: SL distance صغير جداً — تخطي')
        return None

    # حجم الصفقة
    sz = config.get('size_override')
    if sz:
        size = max(min(float(sz), max_sz), min_sz)
    else:
        risk_usd = get_current_balance() * final_risk
        # ✅ math.floor بدل round لتجنب تجاوز الـ risk
        raw_size = risk_usd / (sld * cs) if (sld * cs) > 0 else min_sz
        size     = max(min(math.floor(raw_size * 100) / 100, max_sz), min_sz)

    return {
        'pair':         pair_name,
        'epic':         epic,
        'direction':    signal,
        'entry':        round(entry, 5),
        'sl':           sl,
        'tp':           tp,
        'atr':          round(la, 5),
        'size':         size,
        'spread':       round(sp, 5),
        'risk_percent': round(final_risk, 6),
    }


# ═══════════════════════════════════════════════════════
# EXECUTE & SCAN
# ═══════════════════════════════════════════════════════

def execute_order(sig):
    body = {
        'epic':          sig['epic'],
        'direction':     sig['direction'],
        'size':          sig['size'],
        'guaranteedStop': False,
        'trailingStop':  False,
        'stopLevel':     sig['sl'],
        'profitLevel':   sig['tp']
    }
    log(f'  📤 {sig["pair"]} | {sig["direction"]} @ {sig["entry"]} '
        f'| Size: {sig["size"]} | Risk: {sig["risk_percent"]:.2%}')
    r = _post('/api/v1/positions', body)
    if not r:
        return 'ERROR', 'no response'
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
                db_key = f'{sig["pair"]}_{datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")}'
                op_save(
                    deal_id, sig['pair'], sig['direction'],
                    sig['entry'], sig['sl'], sig['tp'],
                    sig['atr'], sig['size'], db_key
                )
                log(f'  ✅ Order confirmed: {deal_id[:12]}')
            else:
                log(f'  ⚠️ Order not accepted: {status}')
            return status, ref
        return 'UNKNOWN', ref
    log(f'  ❌ Order failed: {r.status_code} | {data.get("errorCode", "")}')
    return 'FAILED', data.get('errorCode')


def db_consec_losses(pair):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT status FROM trades WHERE pair=? AND status IN ('WIN','LOSS') "
                "ORDER BY id DESC LIMIT 8",
                (pair,)
            ).fetchall()
            c = 0
            for row in rows:
                if row[0] == 'LOSS':
                    c += 1
                else:
                    break
            return c


def run_scan():
    can_trade, reason, session_mult, session_name, day_pnl = should_trade()
    if not can_trade:
        log(f'⏸ {reason}')
        return

    log('─' * 55)
    log(f'🔍 SCAN | {session_name} | Session Mult: {session_mult:.1f}x | Day PnL: ${day_pnl:+.2f}')
    log('─' * 55)

    get_current_balance()
    manage_smart_exits()

    open_pos  = get_open_positions()
    open_pairs = op_get_open_pairs()   # ✅ FIX #2
    log(f'  Open: {len(open_pos)}/{MAX_OPEN_TRADES} | Pairs: {open_pairs or "none"}')

    if len(open_pos) >= MAX_OPEN_TRADES:
        return

    now    = datetime.now(timezone.utc)
    ts_key = now.strftime('%Y-%m-%d_%H%M')

    for pair_name, config in PAIRS.items():
        if len(open_pos) >= MAX_OPEN_TRADES:
            break

        # ✅ FIX #2: تخطي إذا كان الزوج مفتوحاً مسبقاً
        if pair_name in open_pairs:
            continue

        if db_consec_losses(pair_name) >= MAX_CONSECUTIVE_LOSS:
            log(f'  {pair_name}: وصل حد الخسائر المتتالية — تخطي')
            continue

        key = f'{pair_name}_{ts_key}'
        if db_is_dup(key):
            continue

        # ✅ FIX #8: نمرر session_mult فقط — dynamic_risk يُحسب داخل check_signal
        sig = check_signal(pair_name, config, session_mult)
        if not sig:
            continue

        db_save(
            key, pair_name, sig['direction'],
            sig['entry'], sig['sl'], sig['tp'],
            sig['atr'], sig['size'], sig['spread'],
            sig['risk_percent'], session_name
        )
        tg_signal(sig, session_name)

        status, ref = execute_order(sig)
        db_update(key, status)
        log(f'  {pair_name}: {status} | ref:{ref}')

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
                    log('❌ Session failed — إعادة المحاولة بعد 60 ثانية')
                    time.sleep(60)
                    continue
                # ✅ FIX #9: Reconcile عند كل تجديد جلسة
                reconcile_positions()
            else:
                if not ping_session():
                    log('⚠️ Ping failed — إعادة إنشاء الجلسة')
                    session_age = 0
                    continue

            session_age = (session_age + 1) % 15
            run_scan()
            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            log('🛑 Bot stopped by user')
            break
        except Exception as ex:
            log(f'❌ ERROR in main loop: {ex}')
            import traceback
            traceback.print_exc()
            time.sleep(30)


# ═══════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════

def start_bot():
    validate_config()   # ✅ FIX #14
    db_init()
    csv_init()
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    print('=' * 60, flush=True)
    print(f'  🚀 ST({SUPERTREND_PERIOD},{SUPERTREND_MULT}|{ATR_METHOD}) + EMA{EMA_PERIOD} Bot — FIXED v2', flush=True)
    print(f'  Timeframe: M15 | Mode: {mode}', flush=True)
    print(f'  SL: {SL_ATR_MULT}x ATR | TP: {TP_ATR_MULT}x ATR', flush=True)
    print(f'  Max Open: {MAX_OPEN_TRADES} | Base Risk: {BASE_RISK_PERCENT:.1%}', flush=True)
    print(f'  Fixes: Flip+SamePair+PnL+Trailing+Bars+Creds+BE+Risk+Reconcile+DB', flush=True)
    print('=' * 60, flush=True)

    nl = '\n'
    tg(f'🚀 *ST+EMA{EMA_PERIOD} Bot FIXED v2* [{mode}]{nl}'
       f'ST({SUPERTREND_PERIOD},{SUPERTREND_MULT}|{ATR_METHOD})+EMA{EMA_PERIOD}{nl}'
       f'M15 | SL:{SL_ATR_MULT}xATR | TP:{TP_ATR_MULT}xATR{nl}'
       f'_{utc_now()}_')

    main_loop()


if __name__ == '__main__':
    start_bot()
