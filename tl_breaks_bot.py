#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==========================================================
# multi_pairs_bot_pure_supertrend.py
# ✅ Supertrend (10,3 | ATR: RMA) + EMA 20
# ✅ فريم 15 دقيقة - النسخة المُصلَحة
# ✅ إصلاح: تكرار الصفقات + فحص الزوج المفتوح + Cache
# ==========================================================

import os, csv, json, time, sqlite3, requests
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
CANDLES_COUNT = 500
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '180'))

# ═══════════════════════════════════════════════════════
# ✅ SUPERTREND SETTINGS
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
KELLY_FRACTION      = 0.25
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

RISK_PERCENT         = float(os.getenv('RISK_PERCENT', '0.01'))
MAX_OPEN_TRADES      = int(os.getenv('MAX_OPEN_TRADES', '6'))
MAX_CONSECUTIVE_LOSS = int(os.getenv('MAX_CONSEC_LOSS', '3'))
ACCOUNT_BALANCE      = float(os.getenv('ACCOUNT_BALANCE', '1000'))

_BASE_DIR = os.getenv('DATA_DIR', '/tmp')
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
# DATABASE
# ═══════════════════════════════════════════════════════

def _migrate_database(conn):
    try:
        cols = conn.execute("PRAGMA table_info(trades)").fetchall()
        col_names = {c[1] for c in cols}
        new_cols = {
            'pnl_r': 'REAL DEFAULT 0', 'pnl_usd': 'REAL DEFAULT 0',
            'exit_price': 'REAL', 'bars_held': 'INTEGER DEFAULT 0',
            'risk_percent': 'REAL DEFAULT 0'
        }
        for col, col_type in new_cols.items():
            if col not in col_names:
                conn.execute(f'ALTER TABLE trades ADD COLUMN {col} {col_type}')
                log(f'  ✅ DB: added {col}')
    except Exception as ex:
        log(f'  ⚠️ DB: {ex}')

def db_init():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY, key TEXT UNIQUE, pair TEXT, direction TEXT,
            timestamp TEXT, entry REAL, sl REAL, tp REAL, atr REAL, size REAL,
            spread REAL DEFAULT 0, status TEXT DEFAULT 'PENDING',
            stage1_done INTEGER DEFAULT 0, stage2_done INTEGER DEFAULT 0,
            stage3_done INTEGER DEFAULT 0, final_locked_r REAL DEFAULT 0,
            exit_type TEXT, session_used TEXT, risk_percent REAL,
            pnl_r REAL DEFAULT 0, pnl_usd REAL DEFAULT 0,
            exit_price REAL, bars_held INTEGER DEFAULT 0
        )''')
        _migrate_database(conn)
        conn.execute('''CREATE TABLE IF NOT EXISTS open_positions (
            deal_id TEXT PRIMARY KEY, pair TEXT, direction TEXT,
            entry REAL, sl REAL, tp REAL, atr REAL, size REAL,
            db_key TEXT, opened_at TEXT,
            stage1_done INTEGER DEFAULT 0, stage2_done INTEGER DEFAULT 0,
            stage3_done INTEGER DEFAULT 0, final_locked_r REAL DEFAULT 0,
            bars_held INTEGER DEFAULT 0
        )''')
        conn.commit()

def db_save(key, pair, direction, entry, sl, tp, atr, size, spread, risk_pct, session):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT INTO trades (key,pair,direction,timestamp,entry,sl,tp,atr,size,spread,risk_percent,session_used) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                    (key, pair, direction, utc_now(), entry, sl, tp, atr, size, spread, risk_pct, session)
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
            query = "SELECT pair, direction, status, pnl_r, timestamp FROM trades WHERE status IN ('WIN','LOSS')"
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
                    'INSERT OR IGNORE INTO open_positions (deal_id,pair,direction,entry,sl,tp,atr,size,db_key,opened_at) VALUES (?,?,?,?,?,?,?,?,?,?)',
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
# RISK MANAGEMENT
# ═══════════════════════════════════════════════════════

def calculate_pnl_since(since_dt):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT pnl_r, sl, size FROM trades WHERE timestamp >= ? AND status IN ('WIN','LOSS')",
                (since_dt.strftime('%Y-%m-%d %H:%M UTC'),)
            ).fetchall()
            total_pnl = 0.0
            for pnl_r, sl, size in rows:
                if pnl_r and sl:
                    total_pnl += pnl_r * abs(sl) * size * 100
            return total_pnl

def check_drawdown_limits():
    now     = datetime.now(timezone.utc)
    balance = get_current_balance()
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_pnl   = calculate_pnl_since(day_start)
    if day_pnl <= -balance * MAX_DAILY_RISK:
        return False, f'🛑 DAILY LIMIT: {day_pnl/balance:.1%}', 0.0
    if day_pnl >= balance * DAILY_PROFIT_TARGET:
        return False, f'🔒 DAILY TARGET: +{day_pnl/balance:.1%}', 0.0
    week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0)
    week_pnl   = calculate_pnl_since(week_start)
    if week_pnl <= -balance * MAX_WEEKLY_RISK:
        return False, f'🛑 WEEKLY LIMIT: {week_pnl/balance:.1%}', 0.0
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
    if avg_loss_r == 0: avg_loss_r = 1
    kelly = (win_rate * (avg_win_r/avg_loss_r) - (1 - win_rate)) / (avg_win_r/avg_loss_r) if avg_win_r > 0 else 0
    kelly = max(0, min(kelly, 0.10))
    consecutive_losses = 0
    cur_type = None
    cur_consec = 0
    for t in sorted(trades, key=lambda x: x[4]):
        if t[2] == 'WIN':
            if cur_type == 'WIN': cur_consec += 1
            else: cur_consec = 1; cur_type = 'WIN'
        else:
            if cur_type == 'LOSS': cur_consec += 1
            else: cur_consec = 1; cur_type = 'LOSS'
            consecutive_losses = max(consecutive_losses, cur_consec)
    return {'total': len(trades), 'win_rate': win_rate, 'kelly': kelly, 'consecutive_losses': consecutive_losses}

def calculate_dynamic_risk(pair, base_risk=BASE_RISK_PERCENT):
    stats = get_pair_stats(pair)
    if not stats:
        return base_risk, 'default'
    risk = base_risk
    if stats['consecutive_losses'] >= 3:   risk *= 0.4
    elif stats['consecutive_losses'] == 2: risk *= 0.6
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
    ratio = atr_current / atr_hist if atr_hist > 0 else 1
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
        entry, sl, size, dir_, pair = pos['entry'], pos['sl'], pos['size'], pos['direction'], pos['pair']
        sl_dist = abs(entry - sl)
        pnl_pts = (exit_price - entry) if dir_ == 'BUY' else (entry - exit_price)
        pnl_r   = round(pnl_pts / sl_dist, 2) if sl_dist > 0 else 0
        result  = 'WIN' if pnl_pts > 0 else ('LOSS' if pnl_pts < 0 else 'BE')
        meta    = _meta_cache.get(pair, {}).get('data')
        pnl_usd = round(pnl_pts * size * (meta[3] if meta else 1), 2)
        _update_trade_pnl(pos['db_key'], pnl_r, pnl_usd, exit_price, pos.get('bars_held', 0))
        now = datetime.now(timezone.utc)
        row = {
            'date': now.strftime('%Y-%m-%d'), 'time_utc': now.strftime('%H:%M'),
            'pair': pair, 'direction': dir_, 'entry': entry, 'sl': sl, 'tp': pos['tp'],
            'exit_price': exit_price, 'atr': pos['atr'], 'size': size,
            'sl_dist': round(sl_dist, 5), 'pnl_usd': pnl_usd, 'pnl_r': pnl_r,
            'result': result, 'bars_held': pos.get('bars_held', 0),
            'spread': pos.get('spread', 0), 'tf': STRATEGY_TF,
            'stage1_done': stage1, 'stage2_done': stage2, 'stage3_done': stage3,
            'final_locked_r': final_r, 'exit_type': exit_type,
            'session_used': pos.get('session_used', ''),
            'risk_percent': pos.get('risk_percent', 0),
            'indicator': f'ST({SUPERTREND_PERIOD},{SUPERTREND_MULT})+EMA{EMA_PERIOD}', 'notes': ''
        }
        with open(TRADES_CSV, 'a', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writerow(row)
        icon = '✅' if result == 'WIN' else ('❌' if result == 'LOSS' else '🔵')
        log(f'  {icon} {pair} {dir_} | PnL=${pnl_usd:+.2f} ({pnl_r:+.2f}R)')
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
            r = requests.get(BASE_URL + path, headers=session_headers, params=params, timeout=15)
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

def _delete(path):
    try:
        return requests.delete(BASE_URL + path, headers=session_headers, timeout=10)
    except Exception as ex:
        log(f'  DELETE: {ex}')

def create_session():
    url  = BASE_URL + '/api/v1/session'
    hdrs = {'X-CAP-API-KEY': API_KEY, 'Content-Type': 'application/json'}
    try:
        r = requests.post(url, headers=hdrs,
                          json={'identifier': EMAIL, 'password': PASSWORD, 'encryptedPassword': False},
                          timeout=15)
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
    _get('/api/v1/ping')

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
        r = _get('/api/v1/history/activity', params={'dealId': deal_id, 'pageSize': 10})
        if r and r.status_code == 200:
            for act in r.json().get('activities', []):
                for action in act.get('details', {}).get('actions', []):
                    if action.get('actionType') in ('POSITION_CLOSED',):
                        lvl = action.get('level') or action.get('stopLevel') or action.get('dealPrice')
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
    r = _post(f'/api/v1/positions/{deal_id}', {'size': size})
    if r and r.status_code == 200:
        log(f'  💰 TP: {size}')
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
        requests.post(f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
                      data={'chat_id': TG_CHAT_ID, 'text': text, 'parse_mode': 'Markdown'}, timeout=10)
    except:
        pass

def tg_signal(sig, session_info):
    icon = '🟢' if sig['direction'] == 'BUY' else '🔴'
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    nl   = '\n'
    tg(f'{icon} *{sig["pair"]} {sig["direction"]}* [{mode}]{nl}'
       f'Entry: `{sig["entry"]}` | SL: `{sig["sl"]}` | TP: `{sig["tp"]}`{nl}'
       f'Risk: `{sig["risk_percent"]:.2%}`{nl}'
       f'Session: `{session_info}`{nl}'
       f'Indicator: Supertrend({SUPERTREND_PERIOD},{SUPERTREND_MULT}|{ATR_METHOD}) + EMA{EMA_PERIOD}{nl}'
       f'_{utc_now()}_')


# ═══════════════════════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════════════════════

def fetch_candles(epic, resolution, count=500):
    cache_key = f'{epic}_{resolution}'
    now = time.time()
    if cache_key in _candle_cache:
        cached = _candle_cache[cache_key]
        # ✅ إصلاح 3: 30 ثانية على M15 لتفادي الدخول المتأخر
        cache_ttl = 30 if 'MINUTE_15' in resolution else 60
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
        'EURUSD': ['GBPUSD'], 'GBPUSD': ['EURUSD'],
        'GOLD':   ['EURUSD'], 'BTCUSD': [],
        'US100':  ['US500'],  'US500':  ['US100'],
    }
    tracked    = op_get_all()
    live_pairs = {p['pair']: p['direction'] for p in tracked}
    if new_pair in correlated_pairs:
        for corr_pair in correlated_pairs[new_pair]:
            if corr_pair in live_pairs and live_pairs[corr_pair] == new_direction:
                return False, f'{corr_pair} open'
    return True, 'OK'


# ═══════════════════════════════════════════════════════
# SMART EXITS
# ═══════════════════════════════════════════════════════

def calculate_sl_at_r(pos, locked_r):
    entry   = pos['entry']
    dir_    = pos['direction']
    sl_dist = abs(pos['entry'] - pos['sl'])
    if dir_ == 'BUY':
        return round(entry + (locked_r * sl_dist), 5)
    else:
        return round(entry - (locked_r * sl_dist), 5)

def get_progressive_lock(profit_r):
    for threshold, locked in sorted(PROGRESSIVE_LOCK.items(), reverse=True):
        if profit_r >= threshold:
            return locked
    return 0

def should_move_sl(current_sl, new_sl, direction):
    if direction == 'BUY':
        return new_sl > current_sl + 0.00001
    else:
        return new_sl < current_sl - 0.00001

def calculate_trailing_sl(pos, cur_price, atr):
    dir_       = pos['direction']
    trail_dist = atr * TRAILING_ATR_MULT
    if dir_ == 'BUY':
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
        if deal_id not in live_ids:
            exit_price = get_closed_deal_price(deal_id, get_current_price(pos['pair']))
            if exit_price > 0:
                result, _ = csv_log_trade(pos, exit_price, pos['stage1_done'],
                                          pos['stage2_done'], pos['stage3_done'],
                                          pos['final_locked_r'], 'STOP_OUT')
                if result != 'ERROR':
                    db_update(pos['db_key'], result.upper() if result in ('WIN','LOSS','BE') else 'CLOSED', 'STOP_OUT')
                    op_delete(deal_id)
            continue

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
        bars_held  = pos.get('bars_held', 0) + 1
        op_update(deal_id, bars_held=bars_held)
        s1, s2, s3 = pos['stage1_done'], pos['stage2_done'], pos['stage3_done']

        if bars_held > MAX_TRADE_DURATION_BARS and profit_r < 0.5:
            if close_full_api(deal_id):
                result, _ = csv_log_trade(pos, cur_price, s1, s2, s3, pos['final_locked_r'], 'TIME')
                db_update(pos['db_key'], result, 'TIME')
                op_delete(deal_id)
            continue

        if not s1 and profit_r >= STAGE1_TP_R:
            partial_size = round(size * STAGE1_PCT, 2)
            _, _, _, cs, min_sz, _ = get_instrument_meta(pos['pair'])
            if partial_size >= min_sz and close_partial_api(deal_id, partial_size):
                op_update(deal_id, stage1_done=1)

        elif s1 and not s2 and profit_r >= STAGE2_TP_R:
            remaining   = size * (1 - STAGE1_PCT)
            stage2_size = round(remaining * (STAGE2_PCT / (1 - STAGE1_PCT)), 2)
            _, _, _, cs, min_sz, _ = get_instrument_meta(pos['pair'])
            if stage2_size >= min_sz and close_partial_api(deal_id, stage2_size):
                new_sl = calculate_sl_at_r(pos, 0.5)
                if new_sl and update_sl_api(deal_id, new_sl, tp):
                    op_update(deal_id, stage2_done=1, final_locked_r=0.5, sl=new_sl)

        elif s2 and not s3 and profit_r >= FINAL_TP_R:
            remaining  = size * (1 - STAGE1_PCT) * (1 - STAGE2_PCT / (1 - STAGE1_PCT))
            final_size = round(remaining * FINAL_PCT, 2)
            _, _, _, cs, min_sz, _ = get_instrument_meta(pos['pair'])
            if final_size >= min_sz and close_partial_api(deal_id, final_size):
                new_sl = calculate_sl_at_r(pos, 2.0)
                if new_sl and update_sl_api(deal_id, new_sl, tp):
                    op_update(deal_id, stage3_done=1, final_locked_r=2.0, sl=new_sl)

        elif s2:
            new_locked_r = get_progressive_lock(profit_r)
            if new_locked_r > pos['final_locked_r']:
                new_sl = calculate_sl_at_r(pos, new_locked_r)
                if new_sl and should_move_sl(pos['sl'], new_sl, dir_) and update_sl_api(deal_id, new_sl, tp):
                    op_update(deal_id, final_locked_r=new_locked_r, sl=new_sl)

        elif (s3 or profit_r >= TRAILING_START_R) and profit_r > pos.get('last_trail_r', 0) + 0.5:
            df = fetch_candles(PAIRS[pos['pair']]['epic'], STRATEGY_TF, 50)
            if not df.empty:
                atr = float(calc_atr_series(df.iloc[:-1], ATR_PERIOD).iloc[-1])
                if atr > 0:
                    trail_sl = calculate_trailing_sl(pos, cur_price, atr)
                    if should_move_sl(pos['sl'], trail_sl, dir_) and update_sl_api(deal_id, trail_sl, tp):
                        op_update(deal_id, sl=trail_sl, last_trail_r=profit_r)


# ═══════════════════════════════════════════════════════
# SIGNAL DETECTION — SUPERTREND + EMA 20
# ═══════════════════════════════════════════════════════

def check_signal(pair_name, config, session_mult, risk_mult):
    epic       = config['epic']
    allow_buy  = config['allow_buy']
    allow_sell = config['allow_sell']
    if not allow_buy and not allow_sell:
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
    ema_20          = calc_ema(df_c['close'], EMA_PERIOD)

    li = n - 1
    lc = float(df_c['close'].iloc[-1])
    la = float(calc_atr_series(df_c, ATR_PERIOD).iloc[-1])

    if np.isnan(la) or la <= 0:
        return None

    st_dir_val = int(st_dir.iloc[li])
    ema_20_val = float(ema_20.iloc[li])

    bid, ask, sp, cs, min_sz, max_sz = get_instrument_meta(epic)
    if bid <= 0 or sp > la * SPREAD_ATR_MAX:
        return None

    signal, entry = None, None

    if allow_buy and st_dir_val == 1 and lc > ema_20_val:
        signal, entry = 'BUY', ask
        log(f'  {pair_name}: 🟢 BUY | ST↑ | EMA{EMA_PERIOD}: {ema_20_val:.5f}')

    if not signal and allow_sell and st_dir_val == -1 and lc < ema_20_val:
        signal, entry = 'SELL', bid
        log(f'  {pair_name}: 🔴 SELL | ST↓ | EMA{EMA_PERIOD}: {ema_20_val:.5f}')

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
    final_risk = dynamic_risk * final_risk_mult

    sz = config.get('size_override')
    if sz:
        size = max(min(float(sz), max_sz), min_sz)
    else:
        risk_usd = get_current_balance() * final_risk
        size     = max(min(round(risk_usd / (sld * cs), 2), max_sz), min_sz)

    return {
        'pair': pair_name, 'epic': epic, 'direction': signal,
        'entry': round(entry, 5), 'sl': sl, 'tp': tp,
        'atr': round(la, 5), 'size': size,
        'spread': round(sp, 5), 'risk_percent': final_risk
    }


# ═══════════════════════════════════════════════════════
# EXECUTE & SCAN
# ═══════════════════════════════════════════════════════

def execute_order(sig):
    body = {
        'epic': sig['epic'], 'direction': sig['direction'], 'size': sig['size'],
        'guaranteedStop': False, 'trailingStop': False,
        'stopLevel': sig['sl'], 'profitLevel': sig['tp']
    }
    log(f'  📤 {sig["pair"]} | {sig["direction"]} @ {sig["entry"]} | Risk: {sig["risk_percent"]:.2%}')
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
                op_save(deal_id, sig['pair'], sig['direction'], sig['entry'],
                        sig['sl'], sig['tp'], sig['atr'], sig['size'], db_key)
            return status, ref
        return 'UNKNOWN', ref
    return 'FAILED', data.get('errorCode')

def db_consec_losses(pair):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT status FROM trades WHERE pair=? AND status IN ('WIN','LOSS') ORDER BY id DESC LIMIT 8",
                (pair,)
            ).fetchall()
            c = 0
            for r in rows:
                if r[0] == 'LOSS': c += 1
                else: break
            return c

def run_scan():
    now = datetime.now(timezone.utc)
    can_trade, reason, session_mult, session_name, day_pnl = should_trade()
    if not can_trade:
        log(f'⏸ {reason}')
        return
    log('─' * 50)
    log(f'🔍 SCAN | {session_name} | Risk: {session_mult:.1f}x')
    log('─' * 50)
    get_current_balance()
    manage_smart_exits()
    open_pos = get_open_positions()
    log(f'  Open: {len(open_pos)}/{MAX_OPEN_TRADES}')
    if len(open_pos) >= MAX_OPEN_TRADES:
        return

    # ✅ إصلاح 1: مفتاح الشمعة — يتغير كل 15 دقيقة فقط (منع التكرار)
    candle_minute = (now.minute // 15) * 15
    ts_key = now.strftime('%Y-%m-%d_%H') + f'{candle_minute:02d}'

    # ✅ إصلاح 2: منع صفقة ثانية على نفس الزوج
    open_epics    = {p.get('market', {}).get('epic', '') for p in open_pos}
    open_pairs_db = {p['pair'] for p in op_get_all()}

    for pair_name, config in PAIRS.items():
        if len(open_pos) >= MAX_OPEN_TRADES:
            break
        if db_consec_losses(pair_name) >= MAX_CONSECUTIVE_LOSS:
            continue

        # ✅ إصلاح 2: تخطَّ الزوج إذا كان مفتوحاً
        if config['epic'] in open_epics or pair_name in open_pairs_db:
            log(f'  {pair_name}: ⏭ مفتوح بالفعل، تجاوز')
            continue

        key = f'{pair_name}_{ts_key}'
        if db_is_dup(key):
            continue

        risk_pct, _ = calculate_dynamic_risk(pair_name, BASE_RISK_PERCENT)
        sig = check_signal(pair_name, config, session_mult, risk_pct / BASE_RISK_PERCENT)
        if not sig:
            continue

        db_save(key, pair_name, sig['direction'], sig['entry'], sig['sl'], sig['tp'],
                sig['atr'], sig['size'], sig['spread'], sig['risk_percent'], session_name)
        tg_signal(sig, session_name)
        status, ref = execute_order(sig)
        db_update(key, status)
        log(f'  {pair_name}: {status}')
        open_pos      = get_open_positions()
        open_epics    = {p.get('market', {}).get('epic', '') for p in open_pos}
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
                    time.sleep(60)
                    continue
            else:
                ping_session()
            session_age = (session_age + 1) % 15
            run_scan()
            time.sleep(SCAN_INTERVAL)
        except KeyboardInterrupt:
            log('🛑 Bot stopped')
            break
        except Exception as ex:
            log(f'ERROR: {ex}')
            time.sleep(30)


# ═══════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════

def start_bot():
    db_init()
    csv_init()
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    print('=' * 60, flush=True)
    print(f'  🚀 Supertrend({SUPERTREND_PERIOD},{SUPERTREND_MULT}|{ATR_METHOD}) + EMA{EMA_PERIOD} Bot', flush=True)
    print(f'  Timeframe: M15 | Mode: {mode}', flush=True)
    print(f'  SL: {SL_ATR_MULT}x ATR | TP: {TP_ATR_MULT}x ATR', flush=True)
    print(f'  ✅ Fix: No duplicate trades | No double pair | Cache 30s', flush=True)
    print('=' * 60, flush=True)
    nl = '\n'
    tg(f'🚀 *ST+EMA{EMA_PERIOD} Bot* [{mode}]{nl}'
       f'Supertrend({SUPERTREND_PERIOD},{SUPERTREND_MULT}|{ATR_METHOD}) + EMA{EMA_PERIOD}{nl}'
       f'M15 | SL:{SL_ATR_MULT}xATR | TP:{TP_ATR_MULT}xATR{nl}'
       f'✅ إصلاح: تكرار الصفقات محلول{nl}'
       f'_{utc_now()}_')
    main_loop()


if __name__ == '__main__':
    start_bot()
