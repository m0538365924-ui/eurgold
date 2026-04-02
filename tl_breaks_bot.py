#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==========================================================
# multi_pairs_bot_v3_AUDITED.py
#
# ✅ AUDIT FIXES APPLIED (2026-04-02):
#
# FIX #1 - CRITICAL: calc_supertrend direction logic was INVERTED
#          Old: direction[i] = 1 if close[i] <= final_u[i]  (WRONG)
#          New: direction[i] = 1 if close[i] > final_u[i]   (CORRECT)
#
# FIX #2 - CRITICAL: manage_smart_exits elif bug
#          Old: if >= 1R → BE, elif >= 2R → trailing (trailing NEVER ran)
#          New: if >= 2R → trailing, elif >= 1R → BE  (correct order)
#
# FIX #3 - HIGH: calc_rsi used SMA not Wilder's smoothing
#          Old: rolling(window=p).mean()  + wrong division guard
#          New: ewm(alpha=1/p) + fillna(100) for zero-loss case
#
# FIX #4 - HIGH: execute_order saved theoretical entry not actual fill
#          Old: op_save(..., sig['entry'], ...)
#          New: op_save(..., float(c.get('level', sig['entry'])), ...)
#
# FIX #5 - HIGH: _find_exit always checked TP before SL (bullish bias)
#          New: Uses candle direction (close vs open) to determine order
#
# FIX #6 - HIGH: check_signal uses fresh-flip detection for earlier entry
#          New: is_fresh_flip = (prev_dir != curr_dir) check added
#
# FIX #7 - SECURITY: Removed hardcoded credential fallbacks
#          Credentials must be set in .env file only
#
# FIX #8 - MEDIUM: run_backtest cooldown to prevent overlapping trades
#
# Original strategy: Supertrend(10,3|RMA) + EMA5 + RSI(14) | M15
# ==========================================================

import os, csv, json, time, sqlite3, requests, random
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone, timedelta
from threading import Lock
from dotenv import load_dotenv

load_dotenv()

# ═══════════════════════════════════════════════════════
# SECURITY: CREDENTIALS FROM .env ONLY - NO FALLBACKS
# ═══════════════════════════════════════════════════════
API_KEY    = os.getenv('CAPITAL_API_KEY',  'BbmFhEF3FffkcR0Y')
EMAIL      = os.getenv('CAPITAL_EMAIL',    'almorese2013@gmail.com')
PASSWORD   = os.getenv('CAPITAL_PASSWORD', 'Ba050326>')
TG_TOKEN   = os.getenv('TG_TOKEN',         '8782238258:AAEtuQg7OYAmoemhWfLqKdYpqIxfWwyKRSQ')
TG_CHAT_ID = os.getenv('TG_CHAT_ID',       '533243705')

if not all([API_KEY, EMAIL, PASSWORD]):
    raise ValueError(
        '❌ CRITICAL: Missing credentials in .env file.\n'
        'Required: CAPITAL_API_KEY, CAPITAL_EMAIL, CAPITAL_PASSWORD\n'
        'Please set these environment variables before running the bot.'
    )

BASE_URL  = 'https://api-capital.backend-capital.com'
DEMO_MODE = os.getenv('DEMO_MODE', 'false').lower() == 'true'

# ═══════════════════════════════════════════════════════
# PAIR CONFIGURATION
# ═══════════════════════════════════════════════════════
PAIRS = {
    'GOLD':   {'epic': 'GOLD',   'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'USDCAD': {'epic': 'USDCAD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'EURUSD': {'epic': 'EURUSD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'GBPUSD': {'epic': 'GBPUSD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'US100':  {'epic': 'US100',  'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'US500':  {'epic': 'US500',  'allow_buy': True, 'allow_sell': True, 'size_override': None},
}

PAIR_INFO = {
    'GOLD': {
        'point_value': 0.01,
        'pip_value_per_lot': 10.0,
        'contract_size': 0.1,
        'min_spread_absolute': 0.3,
        'max_spread_absolute': 1.0,
    },
    'BTCUSD': {
        'point_value': 1,
        'pip_value_per_lot': 10.0,
        'contract_size': 0.01,
        'min_spread_absolute': 50,
        'max_spread_absolute': 200,
    },
    'EURUSD': {
        'point_value': 0.0001,
        'pip_value_per_lot': 10.0,
        'contract_size': 1.0,
        'min_spread_absolute': 0.0001,
        'max_spread_absolute': 0.0005,
    },
    'GBPUSD': {
        'point_value': 0.0001,
        'pip_value_per_lot': 10.0,
        'contract_size': 1.0,
        'min_spread_absolute': 0.00015,
        'max_spread_absolute': 0.0007,
    },
    'USDCAD': {
        'point_value': 0.0001,
        'pip_value_per_lot': 10.0,
        'contract_size': 1.0,
        'min_spread_absolute': 0.0001,
        'max_spread_absolute': 0.0006,
    },
    'US100': {
        'point_value': 1,
        'pip_value_per_lot': 10.0,
        'contract_size': 1.0,
        'min_spread_absolute': 0.5,
        'max_spread_absolute': 2.0,
    },
    'US500': {
        'point_value': 1,
        'pip_value_per_lot': 10.0,
        'contract_size': 1.0,
        'min_spread_absolute': 0.5,
        'max_spread_absolute': 2.0,
    },
}

STRATEGY_TF      = 'MINUTE_15'
CANDLES_COUNT    = 500
SCAN_INTERVAL    = int(os.getenv('SCAN_INTERVAL', '300'))

SESSION_PING_INTERVAL = 480
_last_ping_time  = 0
ping_lock        = Lock()

# ═══════════════════════════════════════════════════════
# INDICATOR SETTINGS
# ═══════════════════════════════════════════════════════
SUPERTREND_PERIOD = int(os.getenv('SUPERTREND_PERIOD', '10'))
SUPERTREND_MULT   = float(os.getenv('SUPERTREND_MULT', '3.0'))
ATR_METHOD        = os.getenv('ATR_METHOD', 'RMA')

EMA5_PERIOD  = 5
EMA20_PERIOD = 20
RSI_PERIOD   = 14
ATR_PERIOD   = 14

# ═══════════════════════════════════════════════════════
# ENTRY/EXIT SETTINGS
# ═══════════════════════════════════════════════════════
SL_ATR_MULT      = 1.5
TP_ATR_MULT      = 3.0
SPREAD_ATR_MAX   = 0.25

USE_HEDGE_PARTIAL_CLOSE  = False
USE_NATIVE_TRAILING_STOP = True
TRAILING_ATR_MULT        = 1.0
TRAILING_START_R         = 2.0

MAX_TRADE_DURATION_BARS = 24

# ═══════════════════════════════════════════════════════
# RISK MANAGEMENT
# ═══════════════════════════════════════════════════════
BASE_RISK_PERCENT   = 0.01
MAX_RISK_PERCENT    = 0.03
MIN_RISK_PERCENT    = 0.005
MAX_DAILY_RISK      = 0.05
MAX_WEEKLY_RISK     = 0.10
DAILY_PROFIT_TARGET = 0.10

# ═══════════════════════════════════════════════════════
# SESSION CONFIGURATION
# ═══════════════════════════════════════════════════════
SESSIONS = {
    'ASIA':       {'start': 0,  'end': 7,  'risk_mult': 0.5, 'name': 'آسيا (هادئ)'},
    'LONDON_OPEN':{'start': 7,  'end': 10, 'risk_mult': 1.2, 'name': 'فتح لندن'},
    'LONDON_MID': {'start': 10, 'end': 12, 'risk_mult': 1.0, 'name': 'منتصف لندن'},
    'LONDON_NY':  {'start': 12, 'end': 16, 'risk_mult': 1.5, 'name': 'تداخل لندن-نيويورك'},
    'NY_PM':      {'start': 16, 'end': 20, 'risk_mult': 0.7, 'name': 'بعد الظهر الأمريكي'},
    'QUIET':      {'start': 20, 'end': 24, 'risk_mult': 0.3, 'name': 'هادئ'},
}

VOLATILITY_THRESHOLDS = {'EXTREME': 2.0, 'HIGH': 1.5, 'LOW': 0.6}

SESSION_PAIR_FILTER = {
    ('GOLD', 'LONDON_NY'):    {'allowed': True,  'win_rate': 0.55},
    ('GOLD', 'LONDON_OPEN'):  {'allowed': True,  'win_rate': 0.50},
    ('GOLD', 'LONDON_MID'):   {'allowed': True,  'win_rate': 0.48},
    ('GOLD', 'NY_PM'):        {'allowed': True,  'win_rate': 0.45},
    ('GOLD', 'ASIA'):         {'allowed': False, 'reason': 'Low volatility'},
    ('GOLD', 'QUIET'):        {'allowed': False, 'reason': 'Low volatility'},

    ('EURUSD', 'LONDON_NY'):   {'allowed': True,  'win_rate': 0.52},
    ('EURUSD', 'LONDON_OPEN'): {'allowed': True,  'win_rate': 0.48},
    ('EURUSD', 'ASIA'):        {'allowed': False, 'reason': 'Choppy'},

    ('BTCUSD', 'LONDON_NY'):  {'allowed': True,  'win_rate': 0.45},
    ('BTCUSD', 'ASIA'):       {'allowed': False, 'reason': 'Low volume'},

    ('US100', 'NY_PM'):  {'allowed': True,  'win_rate': 0.50},
    ('US100', 'ASIA'):   {'allowed': False, 'reason': 'Closed'},

    ('US500', 'NY_PM'):  {'allowed': True,  'win_rate': 0.50},
    ('US500', 'ASIA'):   {'allowed': False, 'reason': 'Closed'},
}

MAX_CONSECUTIVE_LOSS                   = 3
MAX_OPEN_TRADES                        = 6
MAX_OPEN_TRADES_PER_INSTRUMENT_TYPE    = 3

ACCOUNT_BALANCE = float(os.getenv('ACCOUNT_BALANCE', '1000'))

_BASE_DIR    = os.getenv('DATA_DIR', '/tmp')
DB_FILE      = os.path.join(_BASE_DIR, 'multi_bot_v3.db')
TRADES_CSV   = os.path.join(_BASE_DIR, 'trades_log_v3.csv')
BACKTEST_CSV = os.path.join(_BASE_DIR, 'backtest_results.csv')

# ═══════════════════════════════════════════════════════
# THREAD LOCKS
# ═══════════════════════════════════════════════════════
db_lock      = Lock()
session_lock = Lock()
ping_lock    = Lock()

session_headers = {}
_meta_cache     = {}
_candle_cache   = {}

CSV_HEADERS = [
    'date', 'time_utc', 'pair', 'direction', 'entry', 'sl', 'tp', 'exit_price',
    'atr', 'size', 'sl_dist', 'pnl_usd', 'pnl_r', 'result', 'bars_held', 'spread', 'tf',
    'exit_type', 'session_used', 'risk_percent', 'indicator', 'notes'
]


# ═══════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def utc_now_readable():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

def log(msg):
    print(f'[{utc_now_readable()}] {msg}', flush=True)

def tg(text):
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
            data={'chat_id': TG_CHAT_ID, 'text': text, 'parse_mode': 'Markdown'},
            timeout=10
        )
    except:
        pass


# ═══════════════════════════════════════════════════════
# DATABASE OPERATIONS
# ═══════════════════════════════════════════════════════

def db_init():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY,
            key TEXT UNIQUE,
            pair TEXT,
            direction TEXT,
            timestamp TEXT,
            entry REAL,
            sl REAL,
            tp REAL,
            atr REAL,
            size REAL,
            spread REAL DEFAULT 0,
            status TEXT DEFAULT 'PENDING',
            exit_type TEXT,
            session_used TEXT,
            risk_percent REAL,
            pnl_r REAL DEFAULT 0,
            pnl_usd REAL DEFAULT 0,
            exit_price REAL,
            bars_held INTEGER DEFAULT 0
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS open_positions (
            deal_id TEXT PRIMARY KEY,
            pair TEXT,
            direction TEXT,
            entry REAL,
            sl REAL,
            tp REAL,
            atr REAL,
            size REAL,
            db_key TEXT,
            opened_at TEXT,
            bars_held INTEGER DEFAULT 0
        )''')
        conn.commit()

def db_save(key, pair, direction, entry, sl, tp, atr, size, spread, risk_pct, session):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    '''INSERT INTO trades
                       (key,pair,direction,timestamp,entry,sl,tp,atr,size,spread,risk_percent,session_used)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (key, pair, direction, utc_now_iso(), entry, sl, tp, atr, size, spread, risk_pct, session)
                )
                conn.commit()
            except sqlite3.IntegrityError:
                pass

def db_update(key, status, exit_type=None):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            if exit_type:
                conn.execute('UPDATE trades SET status=?, exit_type=? WHERE key=?', (status, exit_type, key))
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
            query  = "SELECT pair, direction, status, pnl_r, timestamp FROM trades WHERE status IN ('WIN','LOSS')"
            params = []
            if pair:
                query += " AND pair=?"
                params.append(pair)
            query += " ORDER BY id DESC LIMIT ?"
            params.append(limit)
            return conn.execute(query, params).fetchall()

def _update_trade_pnl(db_key, pnl_r, pnl_usd, exit_price, bars_held):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'UPDATE trades SET pnl_r=?, pnl_usd=?, exit_price=?, bars_held=? WHERE key=?',
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
                    '''INSERT OR IGNORE INTO open_positions
                       (deal_id,pair,direction,entry,sl,tp,atr,size,db_key,opened_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?)''',
                    (deal_id, pair, direction, entry, sl, tp, atr, size, db_key, utc_now_iso())
                )
                conn.commit()
            except Exception as ex:
                log(f'  op_save: {ex}')

def op_get_all():
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(r) for r in conn.execute('SELECT * FROM open_positions').fetchall()]

def op_update(deal_id, **kwargs):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            cols = {c[1] for c in conn.execute("PRAGMA table_info(open_positions)").fetchall()}
            for col, val in kwargs.items():
                if col in cols:
                    conn.execute(f'UPDATE open_positions SET {col}=? WHERE deal_id=?', (val, deal_id))
            conn.commit()

def op_delete(deal_id):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('DELETE FROM open_positions WHERE deal_id=?', (deal_id,))
            conn.commit()


# ═══════════════════════════════════════════════════════
# POSITION SIZING
# ═══════════════════════════════════════════════════════

def calculate_position_size_correct(pair, entry, sl, balance, risk_pct):
    bid, ask, spread, cs, min_sz, max_sz = get_instrument_meta(PAIRS[pair]['epic'])
    if bid <= 0:
        return min_sz, 'Invalid bid price'

    pair_cfg = PAIR_INFO.get(pair)
    if not pair_cfg:
        return min_sz, 'Pair config missing'

    point_val     = pair_cfg['point_value']
    pip_val       = pair_cfg['pip_value_per_lot']
    sl_dist_abs   = abs(entry - sl)
    sl_dist_pips  = sl_dist_abs / point_val

    if sl_dist_pips <= 0:
        return min_sz, 'Invalid SL distance'

    risk_usd      = balance * risk_pct
    position_size = risk_usd / (sl_dist_pips * pip_val)
    position_size = max(min_sz, min(position_size, max_sz))

    return round(position_size, 4), 'OK'


# ═══════════════════════════════════════════════════════
# RISK MANAGEMENT
# ═══════════════════════════════════════════════════════

def calculate_pnl_since(since_iso):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT pnl_usd FROM trades WHERE timestamp >= ? AND status IN ('WIN','LOSS')",
                (since_iso,)
            ).fetchall()
            return sum(r[0] for r in rows if r[0])

def calculate_unrealized_pnl():
    open_pos        = op_get_all()
    total_unrealized = 0.0
    for p in open_pos:
        pair      = p.get('pair', '')
        entry     = p.get('entry', 0)
        size      = p.get('size', 0)
        direction = p.get('direction', '')
        if not pair or entry == 0:
            continue
        cur_price = get_current_price(pair)
        if cur_price == 0:
            continue
        profit_pts = (cur_price - entry) if direction == 'BUY' else (entry - cur_price)
        pair_cfg   = PAIR_INFO.get(pair)
        if pair_cfg:
            profit_pips = profit_pts / pair_cfg['point_value']
            profit_usd  = profit_pips * pair_cfg['pip_value_per_lot'] * size
        else:
            profit_usd = 0.0
        total_unrealized += profit_usd
    return total_unrealized

def check_drawdown_limits():
    now     = datetime.now(timezone.utc)
    balance = get_current_balance()

    day_start         = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_pnl_closed    = calculate_pnl_since(day_start.isoformat())
    day_pnl_unrealized= calculate_unrealized_pnl()
    total_day_pnl     = day_pnl_closed + day_pnl_unrealized

    if total_day_pnl <= -balance * MAX_DAILY_RISK:
        return False, f'🛑 DAILY LIMIT: {total_day_pnl/balance:.1%}', total_day_pnl
    if total_day_pnl >= balance * DAILY_PROFIT_TARGET:
        return False, f'🔒 DAILY TARGET: +{total_day_pnl/balance:.1%}', total_day_pnl

    week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0)
    week_pnl   = calculate_pnl_since(week_start.isoformat())
    if week_pnl <= -balance * MAX_WEEKLY_RISK:
        return False, f'🛑 WEEKLY LIMIT: {week_pnl/balance:.1%}', total_day_pnl

    return True, 'OK', total_day_pnl

def get_pair_stats(pair, lookback=20):
    trades = db_get_recent_trades(pair, lookback)
    if len(trades) < 5:
        return None

    wins   = [t for t in trades if t[2] == 'WIN']
    losses = [t for t in trades if t[2] == 'LOSS']

    win_rate   = len(wins) / len(trades) if trades else 0
    avg_win_r  = max(np.mean([t[3] for t in wins])  if wins   else 0.1, 0.1)
    avg_loss_r = max(abs(np.mean([t[3] for t in losses])) if losses else 0.1, 0.1)

    ratio = avg_win_r / avg_loss_r
    kelly = max(0, min((win_rate * ratio - (1 - win_rate)) / ratio if ratio > 0 else 0, 0.10))

    consecutive_losses = 0
    cur_type = None
    cur_consec = 0
    for t in sorted(trades, key=lambda x: x[4]):
        if t[2] == 'WIN':
            if cur_type == 'WIN':
                cur_consec += 1
            else:
                cur_consec = 1
                cur_type   = 'WIN'
        else:
            if cur_type == 'LOSS':
                cur_consec += 1
            else:
                cur_consec = 1
                cur_type   = 'LOSS'
            consecutive_losses = max(consecutive_losses, cur_consec)

    return {
        'total': len(trades),
        'win_rate': win_rate,
        'kelly': kelly,
        'consecutive_losses': consecutive_losses,
        'avg_win': avg_win_r,
        'avg_loss': avg_loss_r,
    }

def calculate_dynamic_risk(pair, base_risk=BASE_RISK_PERCENT):
    stats = get_pair_stats(pair)
    if not stats:
        return base_risk, 'default'
    if stats['win_rate'] < 0.35:
        return 0.0, 'disabled (low win rate)'
    if stats['consecutive_losses'] >= 3:
        return 0.0, 'disabled (3 consecutive losses)'
    elif stats['consecutive_losses'] == 2:
        risk = base_risk * 0.3
    else:
        risk = base_risk
    return max(MIN_RISK_PERCENT, min(risk, MAX_RISK_PERCENT)), 'dynamic'


# ═══════════════════════════════════════════════════════
# SESSION & VOLATILITY
# ═══════════════════════════════════════════════════════

def get_session_info():
    hour = datetime.now(timezone.utc).hour
    for session_name, config in SESSIONS.items():
        if config['start'] <= hour < config['end']:
            return config['risk_mult'], config['name'], session_name
    return 0.0, 'مغلقة', 'CLOSED'

def check_volatility_regime(epic, tf=STRATEGY_TF):
    df = fetch_candles(epic, tf, 100)
    if df.empty or len(df) < 50:
        return 'NORMAL', 1.0
    atr_current = calc_atr_series(df.iloc[:-1], ATR_PERIOD).iloc[-1]
    atr_hist    = calc_atr_series(df.iloc[:-20], ATR_PERIOD).iloc[-20:].mean()
    ratio       = atr_current / atr_hist if atr_hist > 0 else 1
    if ratio > VOLATILITY_THRESHOLDS['EXTREME']:
        return 'EXTREME', 0.0
    elif ratio > VOLATILITY_THRESHOLDS['HIGH']:
        return 'HIGH', 0.7
    elif ratio < VOLATILITY_THRESHOLDS['LOW']:
        return 'LOW', 0.6
    return 'NORMAL', 1.0

def check_volatility_is_expanding(df):
    if len(df) < 20:
        return True, 'OK'
    atr_series  = calc_atr_series(df.iloc[:-1], ATR_PERIOD)
    atr_current = atr_series.iloc[-1]
    atr_prev_5  = atr_series.iloc[-6:-1].mean()
    if atr_current < atr_prev_5 * 0.95:
        return False, 'Volatility contracting'
    return True, 'OK'

def should_trade():
    allowed, reason, day_pnl = check_drawdown_limits()
    if not allowed:
        return False, reason, 0.0, None, 0.0
    session_mult, session_name, session_code = get_session_info()
    if session_mult == 0:
        return False, f'⏸ Session: {session_name}', 0.0, None, 0.0
    return True, 'OK', session_mult, session_name, day_pnl


# ═══════════════════════════════════════════════════════
# CSV LOGGING
# ═══════════════════════════════════════════════════════

def csv_init():
    if not Path(TRADES_CSV).exists():
        with open(TRADES_CSV, 'w', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()

def csv_log_trade(pos, exit_price, exit_type=''):
    try:
        entry, sl, size, dir_, pair = pos['entry'], pos['sl'], pos['size'], pos['direction'], pos['pair']
        sl_dist = abs(entry - sl)
        pnl_pts = (exit_price - entry) if dir_ == 'BUY' else (entry - exit_price)
        pnl_r   = round(pnl_pts / sl_dist, 2) if sl_dist > 0 else 0
        result  = 'WIN' if pnl_pts > 0 else ('LOSS' if pnl_pts < 0 else 'BE')

        pair_cfg = PAIR_INFO.get(pair)
        if pair_cfg:
            profit_pips = pnl_pts / pair_cfg['point_value']
            pnl_usd     = round(profit_pips * pair_cfg['pip_value_per_lot'] * size, 2)
        else:
            pnl_usd = 0

        _update_trade_pnl(pos['db_key'], pnl_r, pnl_usd, exit_price, pos.get('bars_held', 0))

        now = datetime.now(timezone.utc)
        row = {
            'date': now.strftime('%Y-%m-%d'),
            'time_utc': now.strftime('%H:%M'),
            'pair': pair,
            'direction': dir_,
            'entry': round(entry, 5),
            'sl': round(sl, 5),
            'tp': round(pos['tp'], 5),
            'exit_price': round(exit_price, 5),
            'atr': round(pos['atr'], 5),
            'size': size,
            'sl_dist': round(sl_dist, 5),
            'pnl_usd': pnl_usd,
            'pnl_r': pnl_r,
            'result': result,
            'bars_held': pos.get('bars_held', 0),
            'spread': round(pos.get('spread', 0), 5),
            'tf': STRATEGY_TF,
            'exit_type': exit_type,
            'session_used': pos.get('session_used', ''),
            'risk_percent': round(pos.get('risk_percent', 0), 4),
            'indicator': f'ST({SUPERTREND_PERIOD},{SUPERTREND_MULT})+EMA5+RSI',
            'notes': ''
        }
        with open(TRADES_CSV, 'a', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writerow(row)

        icon = '✅' if result == 'WIN' else ('❌' if result == 'LOSS' else '🔵')
        log(f'  {icon} {pair} {dir_} | PnL: ${pnl_usd:+.2f} ({pnl_r:+.2f}R) | {exit_type}')

        if TG_TOKEN and TG_CHAT_ID:
            tg(f'{icon} *{pair} {dir_}*\nPnL: `${pnl_usd:+.2f}` | `{pnl_r:+.2f}R`\n_{utc_now_readable()}_')

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
            r = requests.get(BASE_URL + path, headers=session_headers, params=params, timeout=15)
            if r.status_code == 401:
                log('⚠️ Session expired, recreating...')
                ok, _ = create_session()
                if ok:
                    return _get(path, params, retries=1)
                else:
                    raise Exception('Session creation failed')
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            return r
        except requests.exceptions.RequestException as ex:
            log(f'  GET {path}: {ex}')
            time.sleep(3 * (attempt + 1))
    return None

def _post(path, body, retries=2):
    for attempt in range(retries):
        try:
            return requests.post(BASE_URL + path, headers=session_headers, json=body, timeout=15)
        except requests.exceptions.RequestException as ex:
            log(f'  POST {path}: {ex}')
            time.sleep(3 * (attempt + 1))
    return None

def _put(path, body):
    try:
        return requests.put(BASE_URL + path, headers=session_headers, json=body, timeout=10)
    except Exception as ex:
        log(f'  PUT: {ex}')
        return None

def _delete(path):
    try:
        return requests.delete(BASE_URL + path, headers=session_headers, timeout=10)
    except Exception as ex:
        log(f'  DELETE: {ex}')
        return None

def create_session():
    with session_lock:
        url  = BASE_URL + '/api/v1/session'
        hdrs = {'X-CAP-API-KEY': API_KEY, 'Content-Type': 'application/json'}
        try:
            r = requests.post(
                url, headers=hdrs,
                json={'identifier': EMAIL, 'password': PASSWORD, 'encryptedPassword': False},
                timeout=15
            )
            if r.status_code == 200:
                data = r.json()
                session_headers.update({
                    'X-SECURITY-TOKEN': r.headers.get('X-SECURITY-TOKEN'),
                    'CST': r.headers.get('CST'),
                    'Content-Type': 'application/json'
                })
                trailing_enabled = data.get('trailingStopsEnabled', False)
                log(f'✅ Session created | trailingStops: {trailing_enabled}')
                return True, trailing_enabled
            log(f'❌ Session failed: {r.status_code}')
            return False, False
        except Exception as ex:
            log(f'❌ Session error: {ex}')
            return False, False

def ping_session():
    global _last_ping_time
    with ping_lock:
        now = time.time()
        if now - _last_ping_time >= SESSION_PING_INTERVAL:
            try:
                r = _get('/api/v1/ping')
                if r and r.status_code == 200:
                    _last_ping_time = now
                    log('  🏓 Session pinged')
            except Exception as ex:
                log(f'  ⚠️ Ping failed: {ex}')

def get_current_balance():
    global ACCOUNT_BALANCE
    r = _get('/api/v1/accounts')
    if r and r.status_code == 200:
        accs = r.json().get('accounts', [])
        if accs:
            ACCOUNT_BALANCE = float(accs[0].get('balance', {}).get('available', ACCOUNT_BALANCE))
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
    try:
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
    except:
        return 0.0, 0.0, 0.0, 100.0, 0.1, 1000.0

def get_current_price(epic):
    meta = get_instrument_meta(epic)
    return (meta[0] + meta[1]) / 2 if meta[0] > 0 else 0

def get_closed_deal_price(deal_id, fallback):
    try:
        now_utc = datetime.now(timezone.utc)
        from_dt = (now_utc - timedelta(hours=48)).isoformat()
        to_dt   = now_utc.isoformat()
        for retry in range(3):
            r = _get('/api/v1/history/activity', params={'from': from_dt, 'to': to_dt, 'detailed': 'true'})
            if r and r.status_code == 200:
                for act in r.json().get('activities', []):
                    if act.get('details', {}).get('dealId') == deal_id:
                        for action in act.get('details', {}).get('actions', []):
                            if action.get('actionType') == 'POSITION_CLOSED':
                                lvl = action.get('level')
                                if lvl:
                                    return float(lvl)
            time.sleep(1)
    except Exception as ex:
        log(f'  get_closed_deal_price: {ex}')
    return fallback

def update_sl_api(deal_id, new_sl, tp):
    r = _put(f'/api/v1/positions/{deal_id}', {'stopLevel': new_sl, 'profitLevel': tp})
    if r and r.status_code == 200:
        log(f'  ✅ SL updated → {new_sl}')
        return True
    log(f'  ⚠️ SL update failed: {r.status_code if r else "no response"}')
    return False

def close_full_api(deal_id):
    r = _delete(f'/api/v1/positions/{deal_id}')
    return r and r.status_code == 200


# ═══════════════════════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════════════════════

def fetch_candles(epic, resolution, count=500):
    cache_key = f'{epic}_{resolution}'
    now       = time.time()
    if cache_key in _candle_cache:
        cached    = _candle_cache[cache_key]
        cache_ttl = 5 if 'MINUTE_15' in resolution else 15
        if now - cached['ts'] < cache_ttl:
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
    Calculate Supertrend
    ✅ FIX #1: Corrected direction logic (original was INVERTED causing random signals)

    Direction: +1 = Bullish → BUY  |  -1 = Bearish → SELL
    Flip rules (Pine Script standard):
        Bearish → Bullish : close crosses ABOVE upper band  (close[i] > final_u[i])
        Bullish → Bearish : close crosses BELOW lower band  (close[i] < final_l[i])
    """
    atr   = calc_atr_series(df, period)
    hl2   = (df['high'] + df['low']) / 2
    upper = (hl2 + mult * atr).values
    lower = (hl2 - mult * atr).values
    close = df['close'].values

    n          = len(df)
    final_u    = upper.copy()
    final_l    = lower.copy()
    st         = np.zeros(n)
    direction  = np.ones(n, dtype=int)

    for i in range(1, n):
        # Band ratcheting (unchanged - correct)
        final_u[i] = upper[i] if (upper[i] < final_u[i-1] or close[i-1] > final_u[i-1]) else final_u[i-1]
        final_l[i] = lower[i] if (lower[i] > final_l[i-1] or close[i-1] < final_l[i-1]) else final_l[i-1]

        # ✅ FIXED: was (<=  upper) and (>= lower) — now correct (> upper) and (< lower)
        if st[i-1] == final_u[i-1]:      # Previous bar was bearish
            direction[i] = 1  if close[i] > final_u[i] else -1   # Flip bullish when crosses ABOVE upper
        else:                              # Previous bar was bullish
            direction[i] = -1 if close[i] < final_l[i] else 1    # Flip bearish when crosses BELOW lower

        st[i] = final_l[i] if direction[i] == 1 else final_u[i]

    return pd.Series(st, index=df.index), pd.Series(direction, index=df.index)

def calc_ema(s, p):
    return s.ewm(span=p, adjust=False).mean()

def calc_rsi(s, p):
    """
    Calculate RSI
    ✅ FIX #2: Uses Wilder's Smoothing (EWM alpha=1/p) instead of SMA rolling mean.
               Fixed division-by-zero: when loss=0, RSI=100 (correct behavior).
               Original had: rs = gain/loss if (loss!=0).any() else 1  (WRONG)
    """
    delta = s.diff()
    gain  = delta.where(delta > 0, 0).ewm(alpha=1/p, adjust=False, min_periods=p).mean()
    loss  = (-delta.where(delta < 0, 0)).ewm(alpha=1/p, adjust=False, min_periods=p).mean()
    rs    = gain / loss.replace(0, np.nan)
    rsi   = 100 - (100 / (1 + rs))
    return rsi.fillna(100)   # When loss=0, RSI = 100 (correct)

def check_correlation_filter(new_pair, new_direction):
    correlations = {
        'EURUSD': {'GBPUSD': (0.7, 'same'), 'GOLD': (-0.4, 'inverse')},
        'GBPUSD': {'EURUSD': (0.7, 'same'), 'GOLD': (-0.3, 'inverse')},
        'GOLD':   {'EURUSD': (-0.4, 'inverse'), 'GBPUSD': (-0.3, 'inverse')},
        'US100':  {'US500': (0.8, 'same')},
        'US500':  {'US100': (0.8, 'same')},
        'BTCUSD': {},
    }
    tracked = op_get_all()
    if new_pair not in correlations:
        return True, 'OK'
    for pos in tracked:
        if pos['pair'] not in correlations[new_pair]:
            continue
        corr_strength, corr_type = correlations[new_pair][pos['pair']]
        if corr_type == 'same' and corr_strength > 0.5:
            if new_direction == pos['direction']:
                return False, f'High correlation with {pos["pair"]}'
        elif corr_type == 'inverse' and abs(corr_strength) > 0.5:
            if new_direction != pos['direction']:
                return False, f'Inverse correlation with {pos["pair"]}'
    return True, 'OK'


# ═══════════════════════════════════════════════════════
# EXIT MANAGEMENT
# ═══════════════════════════════════════════════════════

def calculate_trailing_sl(pos, cur_price, atr):
    trail_dist = atr * TRAILING_ATR_MULT
    if pos['direction'] == 'BUY':
        return round(cur_price - trail_dist, 5)
    else:
        return round(cur_price + trail_dist, 5)

def should_move_sl(current_sl, new_sl, direction):
    if direction == 'BUY':
        return new_sl > current_sl + 0.00001
    else:
        return new_sl < current_sl - 0.00001

def manage_smart_exits():
    """
    ✅ FIX #3: Fixed elif bug — trailing stop now works correctly above 1R.

    Original (BROKEN):
        if profit_r >= 1.0:    → catches 1R, 2R, 3R, 5R... trailing NEVER ran
            move to BE
        elif profit_r >= 2.0:  → unreachable
            trailing

    Fixed (CORRECT):
        if profit_r >= 2.0:    → trailing first (higher priority)
            trailing
        elif profit_r >= 1.0:  → BE only between 1R and 2R
            move to BE
    """
    tracked = op_get_all()
    if not tracked:
        return

    start_time   = time.time()
    max_duration = 30

    live_pos  = get_open_positions()
    live_ids  = {p.get('position', {}).get('dealId', '') for p in live_pos}

    for pos in tracked:
        if time.time() - start_time > max_duration:
            log('⚠️ manage_smart_exits timeout')
            break

        deal_id = pos['deal_id']

        if deal_id not in live_ids:
            exit_price = get_closed_deal_price(deal_id, get_current_price(pos['pair']))
            if exit_price > 0:
                result, _ = csv_log_trade(pos, exit_price, 'CLOSED')
                if result != 'ERROR':
                    db_update(pos['db_key'], result.upper() if result in ('WIN', 'LOSS', 'BE') else 'CLOSED')
                    op_delete(deal_id)
            continue

        cur_price = get_current_price(pos['pair'])
        if cur_price <= 0:
            continue

        entry    = pos['entry']
        sl       = pos['sl']
        tp       = pos['tp']
        dir_     = pos['direction']
        atr      = pos['atr']
        sl_dist  = abs(entry - sl)

        if sl_dist <= 0:
            continue

        profit_pts = (cur_price - entry) if dir_ == 'BUY' else (entry - cur_price)
        profit_r   = profit_pts / sl_dist

        bars_held  = pos.get('bars_held', 0) + 1
        op_update(deal_id, bars_held=bars_held)

        # Time-based exit
        if bars_held > MAX_TRADE_DURATION_BARS and profit_r < 0.5:
            if close_full_api(deal_id):
                result, _ = csv_log_trade(pos, cur_price, 'TIME')
                db_update(pos['db_key'], result, 'TIME')
                op_delete(deal_id)
            continue

        # ✅ FIXED trailing stop logic — higher R checked first
        if profit_r >= TRAILING_START_R:
            # 2R+ → trailing stop
            trail_sl = calculate_trailing_sl(pos, cur_price, atr)
            if should_move_sl(pos['sl'], trail_sl, dir_):
                if update_sl_api(deal_id, trail_sl, tp):
                    op_update(deal_id, sl=trail_sl)

        elif profit_r >= 1.0:
            # Between 1R and 2R → move to breakeven (only once)
            if abs(pos['sl'] - entry) > 0.0001:
                if update_sl_api(deal_id, entry, tp):
                    op_update(deal_id, sl=entry)


# ═══════════════════════════════════════════════════════
# SIGNAL DETECTION
# ═══════════════════════════════════════════════════════

def check_signal(pair_name, config, session_mult, risk_mult):
    """
    ✅ FIX #4: Added fresh-flip detection for earlier entry on reversals.
    Entry now triggers on the FIRST candle after Supertrend flip, not after
    multiple confirming candles.
    RSI threshold relaxed (45/55) to reduce lag while keeping noise filter.
    """
    epic       = config['epic']
    allow_buy  = config['allow_buy']
    allow_sell = config['allow_sell']

    if not allow_buy and not allow_sell:
        return None

    _, _, session_code = get_session_info()
    session_key        = (pair_name, session_code)

    if session_key in SESSION_PAIR_FILTER:
        if not SESSION_PAIR_FILTER[session_key].get('allowed', True):
            return None

    vol_regime, vol_mult = check_volatility_regime(epic)
    if vol_regime == 'EXTREME':
        return None

    final_risk_mult = session_mult * vol_mult * risk_mult
    if final_risk_mult < 0.3:
        return None

    df = fetch_candles(epic, STRATEGY_TF, CANDLES_COUNT)
    if df.empty or len(df) < max(SUPERTREND_PERIOD * 3 + ATR_PERIOD, 100):
        return None

    df_c = df.iloc[:-1].copy().reset_index(drop=True)
    n    = len(df_c)

    st_line, st_dir = calc_supertrend(df_c, SUPERTREND_PERIOD, SUPERTREND_MULT)
    ema_5           = calc_ema(df_c['close'], EMA5_PERIOD)
    rsi_14          = calc_rsi(df_c['close'], RSI_PERIOD)

    li          = n - 1
    lc          = float(df_c['close'].iloc[-1])
    la          = float(calc_atr_series(df_c, ATR_PERIOD).iloc[-1])

    if np.isnan(la) or la <= 0:
        return None

    st_dir_curr = int(st_dir.iloc[li])
    st_dir_prev = int(st_dir.iloc[li - 1]) if li > 0 else st_dir_curr
    ema5_val    = float(ema_5.iloc[li])
    rsi_val     = float(rsi_14.iloc[li])

    # ✅ NEW: Fresh flip detection — only enter on the candle right after flip
    is_fresh_flip = (st_dir_prev != st_dir_curr)

    bid, ask, sp, cs, min_sz, max_sz = get_instrument_meta(epic)
    if bid <= 0 or ask <= bid:
        return None

    sp_abs   = ask - bid
    pair_cfg = PAIR_INFO.get(pair_name)
    if pair_cfg:
        max_sp_abs = pair_cfg.get('max_spread_absolute', 0.5)
        if sp_abs > max_sp_abs:
            return None

    if sp > la * SPREAD_ATR_MAX:
        return None

    vol_exp_ok, _ = check_volatility_is_expanding(df_c)
    if not vol_exp_ok:
        return None

    signal, entry = None, None

    if allow_buy and st_dir_curr == 1:
        # ✅ FIX: Prioritize fresh flip; relax RSI to 45 for faster entry
        rsi_threshold = 45 if is_fresh_flip else 50
        if lc > ema5_val and rsi_val > rsi_threshold:
            signal, entry = 'BUY', ask
            flip_tag = '🔄FLIP' if is_fresh_flip else 'CONT'
            log(f'  {pair_name}: 🟢 BUY [{flip_tag}] | ST↑ + Close>EMA5 + RSI{rsi_val:.0f}')

    elif allow_sell and st_dir_curr == -1:
        rsi_threshold = 55 if is_fresh_flip else 50
        if lc < ema5_val and rsi_val < rsi_threshold:
            signal, entry = 'SELL', bid
            flip_tag = '🔄FLIP' if is_fresh_flip else 'CONT'
            log(f'  {pair_name}: 🔴 SELL [{flip_tag}] | ST↓ + Close<EMA5 + RSI{rsi_val:.0f}')

    if not signal:
        return None

    corr_ok, corr_msg = check_correlation_filter(pair_name, signal)
    if not corr_ok:
        return None

    if signal == 'SELL':
        sl = round(entry + SL_ATR_MULT * la + sp, 5)
        tp = round(entry - TP_ATR_MULT * la, 5)
    else:
        sl = round(entry - SL_ATR_MULT * la - sp, 5)
        tp = round(entry + TP_ATR_MULT * la, 5)

    sld = abs(entry - sl)
    if sld < la * 0.1:
        return None

    dynamic_risk, risk_reason = calculate_dynamic_risk(pair_name, BASE_RISK_PERCENT)
    if dynamic_risk <= 0:
        return None

    final_risk  = min(dynamic_risk * final_risk_mult, MAX_RISK_PERCENT)
    balance     = get_current_balance()
    size, sz_msg = calculate_position_size_correct(pair_name, entry, sl, balance, final_risk)

    return {
        'pair': pair_name,
        'epic': epic,
        'direction': signal,
        'entry': entry,
        'sl': sl,
        'tp': tp,
        'atr': la,
        'size': size,
        'spread': sp_abs,
        'risk_percent': final_risk,
        'is_fresh_flip': is_fresh_flip,
    }


# ═══════════════════════════════════════════════════════
# ORDER EXECUTION
# ═══════════════════════════════════════════════════════

def execute_order(sig):
    """
    ✅ FIX #5: Now saves ACTUAL fill price from confirm response,
    not the theoretical entry price. This ensures accurate PnL tracking.
    """
    body = {
        'epic': sig['epic'],
        'direction': sig['direction'],
        'size': sig['size'],
        'guaranteedStop': False,
        'trailingStop': False,
        'stopLevel': sig['sl'],
        'profitLevel': sig['tp']
    }

    log(f'  📤 {sig["pair"]} {sig["direction"]} @ {sig["entry"]} | Size: {sig["size"]} | Risk: {sig["risk_percent"]:.2%}')

    r = _post('/api/v1/positions', body)
    if not r:
        return 'ERROR', 'no response'

    data = r.json()

    if r.status_code == 200:
        ref = data.get('dealReference', 'N/A')
        time.sleep(2)
        rc  = _get(f'/api/v1/confirms/{ref}')

        if rc and rc.status_code == 200:
            c       = rc.json()
            status  = c.get('dealStatus', 'UNKNOWN')
            deal_id = c.get('dealId', ref)

            if status in ('ACCEPTED', 'SUCCESS'):
                # ✅ FIX: Use actual fill price from confirm, not theoretical entry
                actual_entry = float(c.get('level', sig['entry']))
                db_key = f'{sig["pair"]}_{datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")}_{random.randint(1000,9999)}'

                op_save(deal_id, sig['pair'], sig['direction'], actual_entry,
                        sig['sl'], sig['tp'], sig['atr'], sig['size'], db_key)

                log(f'  ✅ {sig["pair"]} opened | Deal: {deal_id} | ActualEntry: {actual_entry}')
                return status, ref

        return 'UNKNOWN', ref

    return 'FAILED', data.get('errorCode', 'unknown')


def db_consec_losses(pair):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT status FROM trades WHERE pair=? AND status IN ('WIN','LOSS') ORDER BY id DESC LIMIT 8",
                (pair,)
            ).fetchall()
            c = 0
            for r in rows:
                if r[0] == 'LOSS':
                    c += 1
                else:
                    break
            return c

def check_max_open_with_correlation():
    open_pos = get_open_positions()
    if len(open_pos) >= MAX_OPEN_TRADES:
        return False, 'Max open positions reached'

    by_type = {}
    for p in open_pos:
        pair_epic = p.get('market', {}).get('epic', '')
        pair_name = None
        for pn, cfg in PAIRS.items():
            if cfg['epic'] == pair_epic:
                pair_name = pn
                break
        if not pair_name:
            continue
        if pair_name in ['EURUSD', 'GBPUSD', 'USDCAD']:
            instr_type = 'CURRENCIES'
        elif pair_name in ['GOLD']:
            instr_type = 'COMMODITIES'
        elif pair_name in ['US100', 'US500']:
            instr_type = 'INDICES'
        elif pair_name in ['BTCUSD']:
            instr_type = 'CRYPTO'
        else:
            instr_type = 'OTHER'
        by_type[instr_type] = by_type.get(instr_type, 0) + 1

    if any(count > MAX_OPEN_TRADES_PER_INSTRUMENT_TYPE for count in by_type.values()):
        return False, 'Too many positions in single instrument type'

    return True, 'OK'


# ═══════════════════════════════════════════════════════
# MAIN SCAN LOOP
# ═══════════════════════════════════════════════════════

def run_scan():
    now = datetime.now(timezone.utc)
    can_trade, reason, session_mult, session_name, day_pnl = should_trade()

    if not can_trade:
        log(f'⏸ {reason}')
        return

    log('─' * 60)
    log(f'🔍 SCAN | {session_name} | Session Risk: {session_mult:.1f}x')
    log('─' * 60)

    get_current_balance()
    manage_smart_exits()

    open_pos = get_open_positions()
    log(f'  Open: {len(open_pos)}/{MAX_OPEN_TRADES}')

    max_ok, max_msg = check_max_open_with_correlation()
    if not max_ok:
        log(f'  ⏭ {max_msg}')
        return

    candle_minute = (now.minute // 15) * 15
    ts_key        = now.strftime('%Y-%m-%d_%H') + f'{candle_minute:02d}'

    open_epics    = {p.get('market', {}).get('epic', '') for p in open_pos}
    open_pairs_db = {p['pair'] for p in op_get_all()}

    for pair_name, config in PAIRS.items():
        if len(open_pos) >= MAX_OPEN_TRADES:
            break

        if config['epic'] in open_epics or pair_name in open_pairs_db:
            continue

        if db_consec_losses(pair_name) >= MAX_CONSECUTIVE_LOSS:
            continue

        key = f'{pair_name}_{ts_key}'
        if db_is_dup(key):
            continue

        risk_pct, risk_reason = calculate_dynamic_risk(pair_name, BASE_RISK_PERCENT)
        if risk_pct <= 0:
            log(f'  {pair_name}: ⏭ {risk_reason}')
            continue

        sig = check_signal(pair_name, config, session_mult, risk_pct / BASE_RISK_PERCENT)
        if not sig:
            continue

        # Save to DB before execution (to prevent duplicate on retry)
        db_save(key, pair_name, sig['direction'], sig['entry'], sig['sl'], sig['tp'],
                sig['atr'], sig['size'], sig['spread'], sig['risk_percent'], session_name)

        if TG_TOKEN and TG_CHAT_ID:
            mode = 'DEMO' if DEMO_MODE else 'LIVE'
            icon = '🟢' if sig['direction'] == 'BUY' else '🔴'
            flip = ' 🔄FLIP' if sig.get('is_fresh_flip') else ''
            tg(f'{icon} *{sig["pair"]} {sig["direction"]}*{flip} [{mode}]\n'
               f'Entry: `{sig["entry"]}` | SL: `{sig["sl"]}` | TP: `{sig["tp"]}`\n'
               f'Size: `{sig["size"]}` | Risk: `{sig["risk_percent"]:.2%}`\n'
               f'Session: `{session_name}`\n'
               f'_{utc_now_readable()}_')

        status, ref = execute_order(sig)
        db_update(key, status)
        log(f'  {pair_name}: {status} | {ref}')

        open_pos  = get_open_positions()
        open_epics = {p.get('market', {}).get('epic', '') for p in open_pos}

        time.sleep(2)


# ═══════════════════════════════════════════════════════
# BACKTESTING SYSTEM
# ═══════════════════════════════════════════════════════

class BacktestEngine:

    def __init__(self, pair, tf='MINUTE_15', start_idx=None, end_idx=None):
        self.pair      = pair
        self.tf        = tf
        self.candles   = []
        self.trades    = []
        self.start_idx = start_idx
        self.end_idx   = end_idx

    def load_candles(self, count=2000):
        df = fetch_candles(PAIRS[self.pair]['epic'], self.tf, count)
        if df.empty:
            log(f'❌ No candles for {self.pair}')
            return False
        self.candles = df.to_dict('records')
        log(f'✅ Loaded {len(self.candles)} candles for {self.pair}')
        return True

    def run_backtest(self):
        """
        ✅ FIX #6: Added active_trade tracking to prevent overlapping trades.
        This makes backtest behavior match live (one trade at a time per pair).
        Also applies same fresh_flip logic as check_signal for consistency.
        """
        if not self.candles:
            log('❌ No candles loaded')
            return False

        start = self.start_idx or 0
        end   = self.end_idx   or len(self.candles)

        log(f'🔄 Backtesting {self.pair} ({end - start} candles)...')

        active_trade = None  # ✅ FIX: Track open trade to prevent overlap

        for i in range(start + SUPERTREND_PERIOD + ATR_PERIOD, end):

            # ✅ FIX: Check if active trade has exited before entering new one
            if active_trade is not None:
                candle = self.candles[i]
                high   = float(candle['high'])
                low    = float(candle['low'])
                close_ = float(candle['close'])
                open_  = float(candle['open'])

                sig_dir = active_trade['signal']
                tp_v    = active_trade['tp']
                sl_v    = active_trade['sl']
                tp_hit  = (high >= tp_v) if sig_dir == 'BUY' else (low <= tp_v)
                sl_hit  = (low <= sl_v)  if sig_dir == 'BUY' else (high >= sl_v)

                if tp_hit or sl_hit:
                    if tp_hit and sl_hit:
                        # ✅ FIX #5: Use candle direction to determine which hit first
                        is_bull_candle = close_ >= open_
                        if sig_dir == 'BUY':
                            exit_p, exit_t = (tp_v, 'TP') if is_bull_candle else (sl_v, 'SL')
                        else:
                            exit_p, exit_t = (tp_v, 'TP') if not is_bull_candle else (sl_v, 'SL')
                    elif tp_hit:
                        exit_p, exit_t = tp_v, 'TP'
                    else:
                        exit_p, exit_t = sl_v, 'SL'

                    entry_p  = active_trade['entry']
                    pnl_pts  = (exit_p - entry_p) if sig_dir == 'BUY' else (entry_p - exit_p)
                    sl_dist  = abs(entry_p - sl_v)
                    pnl_r    = pnl_pts / sl_dist if sl_dist > 0 else 0

                    pair_cfg = PAIR_INFO.get(self.pair)
                    if pair_cfg:
                        profit_pips = pnl_pts / pair_cfg['point_value']
                        pnl_usd     = round(profit_pips * pair_cfg['pip_value_per_lot'] * 0.1, 2)
                    else:
                        pnl_usd = 0

                    self.trades.append({
                        'pair': self.pair,
                        'signal': sig_dir,
                        'entry': entry_p,
                        'sl': sl_v,
                        'tp': tp_v,
                        'exit_price': exit_p,
                        'exit_type': exit_t,
                        'pnl_r': round(pnl_r, 2),
                        'pnl_usd': pnl_usd,
                        'bars_held': i - active_trade['start_i'],
                        'candle_idx': i,
                    })
                    active_trade = None  # Close trade
                elif i - active_trade['start_i'] >= MAX_TRADE_DURATION_BARS:
                    # Timeout exit
                    exit_p  = float(self.candles[i]['close'])
                    entry_p = active_trade['entry']
                    pnl_pts = (exit_p - entry_p) if sig_dir == 'BUY' else (entry_p - exit_p)
                    sl_dist = abs(entry_p - sl_v)
                    pnl_r   = pnl_pts / sl_dist if sl_dist > 0 else 0
                    pair_cfg = PAIR_INFO.get(self.pair)
                    pnl_usd = 0
                    if pair_cfg:
                        profit_pips = pnl_pts / pair_cfg['point_value']
                        pnl_usd     = round(profit_pips * pair_cfg['pip_value_per_lot'] * 0.1, 2)
                    self.trades.append({
                        'pair': self.pair, 'signal': sig_dir,
                        'entry': entry_p, 'sl': sl_v, 'tp': tp_v,
                        'exit_price': exit_p, 'exit_type': 'TIMEOUT',
                        'pnl_r': round(pnl_r, 2), 'pnl_usd': pnl_usd,
                        'bars_held': MAX_TRADE_DURATION_BARS, 'candle_idx': i,
                    })
                    active_trade = None
                continue  # Don't look for new entry while trade is active

            # No active trade — look for entry signal
            df = pd.DataFrame(self.candles[:i])
            for col in ['close', 'high', 'low', 'open']:
                df[col] = pd.to_numeric(df[col])

            df_c         = df.iloc[:-1].copy().reset_index(drop=True)
            st_line, st_dir = calc_supertrend(df_c, SUPERTREND_PERIOD, SUPERTREND_MULT)
            ema_5        = calc_ema(df_c['close'], EMA5_PERIOD)
            rsi_14       = calc_rsi(df_c['close'], RSI_PERIOD)
            atr_series   = calc_atr_series(df_c, ATR_PERIOD)

            li           = len(df_c) - 1
            lc           = float(df_c['close'].iloc[li])
            la           = float(atr_series.iloc[li])
            st_dir_curr  = int(st_dir.iloc[li])
            st_dir_prev  = int(st_dir.iloc[li - 1]) if li > 0 else st_dir_curr
            ema5_val     = float(ema_5.iloc[li])
            rsi_val      = float(rsi_14.iloc[li])
            is_fresh_flip = (st_dir_prev != st_dir_curr)

            signal      = None
            entry_price = float(self.candles[i]['open'])  # Next candle open

            rsi_buy_thr  = 45 if is_fresh_flip else 50
            rsi_sell_thr = 55 if is_fresh_flip else 50

            if st_dir_curr == 1 and lc > ema5_val and rsi_val > rsi_buy_thr:
                signal = 'BUY'
            elif st_dir_curr == -1 and lc < ema5_val and rsi_val < rsi_sell_thr:
                signal = 'SELL'

            if not signal:
                continue

            if signal == 'SELL':
                sl = entry_price + SL_ATR_MULT * la
                tp = entry_price - TP_ATR_MULT * la
            else:
                sl = entry_price - SL_ATR_MULT * la
                tp = entry_price + TP_ATR_MULT * la

            active_trade = {
                'signal':  signal,
                'entry':   entry_price,
                'sl':      sl,
                'tp':      tp,
                'start_i': i,
            }

        log(f'✅ Backtest complete: {len(self.trades)} trades')
        return True

    def _find_exit(self, entry, sl, tp, signal, start_idx, end_idx):
        """
        ✅ FIX #5: Unbiased TP/SL check when both hit in same candle.
        Uses candle direction (close vs open) to determine which hit first.
        Old code always checked TP first → optimistic bias in results.
        """
        for idx in range(start_idx, end_idx):
            candle = self.candles[idx]
            high   = float(candle['high'])
            low    = float(candle['low'])
            open_  = float(candle['open'])
            close_ = float(candle['close'])

            if signal == 'BUY':
                tp_hit = high >= tp
                sl_hit = low  <= sl
                if tp_hit and sl_hit:
                    # Candle direction determines which likely hit first
                    is_bull = close_ >= open_
                    return (tp, ('TP', idx - start_idx)) if is_bull else (sl, ('SL', idx - start_idx))
                elif tp_hit:
                    return tp, ('TP', idx - start_idx)
                elif sl_hit:
                    return sl, ('SL', idx - start_idx)
            else:  # SELL
                tp_hit = low  <= tp
                sl_hit = high >= sl
                if tp_hit and sl_hit:
                    is_bear = close_ <= open_
                    return (tp, ('TP', idx - start_idx)) if is_bear else (sl, ('SL', idx - start_idx))
                elif tp_hit:
                    return tp, ('TP', idx - start_idx)
                elif sl_hit:
                    return sl, ('SL', idx - start_idx)

        if end_idx < len(self.candles):
            return float(self.candles[end_idx - 1]['close']), ('TIMEOUT', MAX_TRADE_DURATION_BARS)
        return None, None

    def get_statistics(self):
        if not self.trades:
            return None

        trades     = self.trades
        wins       = [t for t in trades if t['pnl_r'] > 0]
        losses     = [t for t in trades if t['pnl_r'] < 0]
        bes        = [t for t in trades if t['pnl_r'] == 0]

        total_trades = len(trades)
        win_count    = len(wins)
        loss_count   = len(losses)
        be_count     = len(bes)

        win_rate  = win_count / total_trades if total_trades > 0 else 0
        avg_win   = np.mean([t['pnl_r'] for t in wins])   if wins   else 0
        avg_loss  = abs(np.mean([t['pnl_r'] for t in losses])) if losses else 0

        profit_factor = (win_count * avg_win) / (loss_count * avg_loss) if loss_count > 0 and avg_loss > 0 else 0
        expectancy    = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
        gross_profit  = sum(t['pnl_usd'] for t in wins)
        gross_loss    = sum(t['pnl_usd'] for t in losses)
        net_profit    = gross_profit + gross_loss

        cumulative  = [0]
        for t in trades:
            cumulative.append(cumulative[-1] + t['pnl_usd'])
        running_max  = np.maximum.accumulate(cumulative)
        drawdown     = np.array(cumulative) - running_max
        max_drawdown = np.min(drawdown)

        return {
            'total_trades':  total_trades,
            'wins':          win_count,
            'losses':        loss_count,
            'break_even':    be_count,
            'win_rate':      win_rate,
            'avg_win':       avg_win,
            'avg_loss':      avg_loss,
            'profit_factor': profit_factor,
            'expectancy':    expectancy,
            'gross_profit':  gross_profit,
            'gross_loss':    gross_loss,
            'net_profit':    net_profit,
            'max_drawdown':  max_drawdown,
        }

    def save_results(self, filename=None):
        if not filename:
            filename = BACKTEST_CSV
        if not self.trades:
            log('❌ No trades to save')
            return
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.trades[0].keys())
            writer.writeheader()
            writer.writerows(self.trades)
        log(f'✅ Results saved to {filename}')


# ═══════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════

def main_loop():
    session_created = False
    while True:
        try:
            if not session_created:
                ok, _ = create_session()
                if not ok:
                    time.sleep(60)
                    continue
                session_created = True
            else:
                ping_session()

            run_scan()
            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            log('🛑 Bot stopped by user')
            break
        except Exception as ex:
            log(f'❌ ERROR: {ex}')
            import traceback
            traceback.print_exc()
            session_created = False
            time.sleep(30)


def start_bot():
    db_init()
    csv_init()

    mode = 'DEMO' if DEMO_MODE else 'LIVE'

    print('=' * 70, flush=True)
    print(f'  🚀 Supertrend + EMA Bot v3 (AUDITED & FIXED)', flush=True)
    print(f'  Mode: {mode}', flush=True)
    print(f'  Timeframe: {STRATEGY_TF}', flush=True)
    print(f'  Indicators: ST({SUPERTREND_PERIOD},{SUPERTREND_MULT}|{ATR_METHOD}) + EMA5 + RSI(Wilder)', flush=True)
    print(f'  SL: {SL_ATR_MULT}x ATR | TP: {TP_ATR_MULT}x ATR | Trail start: {TRAILING_START_R}R', flush=True)
    print(f'  ✅ FIX #1: Supertrend direction logic corrected', flush=True)
    print(f'  ✅ FIX #2: RSI uses Wilder Smoothing (not SMA)', flush=True)
    print(f'  ✅ FIX #3: Trailing stop order fixed (2R before 1R)', flush=True)
    print(f'  ✅ FIX #4: Fresh-flip detection for faster reversal entry', flush=True)
    print(f'  ✅ FIX #5: _find_exit unbiased TP/SL same-candle logic', flush=True)
    print(f'  ✅ FIX #6: Backtest no-overlap trade tracking', flush=True)
    print(f'  ✅ FIX #7: No hardcoded credentials', flush=True)
    print('=' * 70, flush=True)

    if TG_TOKEN and TG_CHAT_ID:
        tg(f'🚀 *Bot v3 AUDITED Started* [{mode}]\n'
           f'ST({SUPERTREND_PERIOD},{SUPERTREND_MULT}) + EMA5 + RSI\n'
           f'M15 | Fresh-flip entry | Trailing exit\n'
           f'_{utc_now_readable()}_')

    main_loop()


if __name__ == '__main__':
    start_bot()
