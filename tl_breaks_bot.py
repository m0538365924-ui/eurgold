#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==========================================================
# tl_breaks_bot.py — Trendlines with Breaks Bot (v2)
# GOLD + EURUSD | Capital.com API | Railway Ready
# ==========================================================
# المميزات:
#  [1] Position Sizing احترافي  → risk / (sl_dist × contractSize)
#  [2] Break-even               → SL ينتقل لنقطة الدخول عند 1R
#  [3] Trailing Stop            → ATR أو Structure
#  [4] Spread guard             → يتجاهل إشارة إذا spread كبير
#  [5] CSV Logger               → يسجل كل صفقة مع إحصائيات
#  [6] Daily Summary            → ملخص يومي على Telegram
#  [7] Railway Ready            → لا ملفات محلية حساسة
#  [8] Session Filter           → 07:00 – 17:00 UTC فقط
# ==========================================================

import os, csv, json, time, sqlite3, requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from threading import Lock
from dotenv import load_dotenv

load_dotenv()


# ═══════════════════════════════════════════════════════
# CONFIG — كل القيم من .env (Railway Variables)
# ═══════════════════════════════════════════════════════
API_KEY    = os.getenv('CAPITAL_API_KEY',  'BbmFhEF3FffkcR0Y')
EMAIL      = os.getenv('CAPITAL_EMAIL',    'almorese2013@gmail.com')
PASSWORD   = os.getenv('CAPITAL_PASSWORD', 'Ba050326>')
TG_TOKEN   = os.getenv('TG_TOKEN',         '8782238258:AAEtuQg7OYAmoemhWfLqKdYpqIxfWwyKRSQ')
TG_CHAT_ID = os.getenv('TG_CHAT_ID',       '533243705')

BASE_URL   = 'https://api-capital.backend-capital.com'
DEMO_MODE  = os.getenv('DEMO_MODE', 'false').lower() == 'true'

# ── Pairs ──
PAIRS = {
    'GOLD': {
        'epic':          'GOLD',
        'allow_buy':     os.getenv('GOLD_BUY',  'false').lower() == 'true',
        'allow_sell':    os.getenv('GOLD_SELL', 'true').lower()  == 'true',
        'size_override': None,
    },
    'EURUSD': {
        'epic':          'EURUSD',
        'allow_buy':     os.getenv('EURUSD_BUY',  'true').lower() == 'true',
        'allow_sell':    os.getenv('EURUSD_SELL', 'true').lower() == 'true',
        'size_override': None,
    },
}

# ── Timeframe ──
STRATEGY_TF   = os.getenv('STRATEGY_TF',    'MINUTE_15')
CANDLES_COUNT = 300
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '300'))

# ── Strategy ──
LENGTH       = int(os.getenv('LENGTH',       '10'))
SLOPE_MULT   = float(os.getenv('SLOPE_MULT', '1.0'))
SLOPE_METHOD = os.getenv('SLOPE_METHOD',    'ATR')
ATR_PERIOD   = 14
SL_ATR_MULT  = float(os.getenv('SL_ATR_MULT', '1.5'))
TP_ATR_MULT  = float(os.getenv('TP_ATR_MULT', '2.5'))

# ── Filters ──
MAX_SPREAD_ATR_RATIO = float(os.getenv('MAX_SPREAD_ATR', '0.30'))

# ── Break-even ──
BE_TRIGGER_R = float(os.getenv('BE_TRIGGER', '1.0'))

# ── Trailing ──
TRAIL_MODE      = os.getenv('TRAIL_MODE',      'STRUCTURE')
TRAIL_TRIGGER_R = float(os.getenv('TRAIL_TRIGGER', '1.5'))
TRAIL_ATR_MULT  = float(os.getenv('TRAIL_ATR',     '2.0'))
SWING_LOOKBACK  = int(os.getenv('SWING_LOOKBACK',  '5'))

# ── Risk ──
RISK_PERCENT         = float(os.getenv('RISK_PERCENT',     '0.01'))
MAX_OPEN_TRADES      = int(os.getenv('MAX_OPEN_TRADES',    '3'))
MAX_CONSECUTIVE_LOSS = int(os.getenv('MAX_CONSEC_LOSS',    '5'))
ACCOUNT_BALANCE      = float(os.getenv('ACCOUNT_BALANCE', '1000'))

# ── Files ──
_BASE_DIR   = os.getenv('DATA_DIR', '/tmp')
DB_FILE     = os.path.join(_BASE_DIR, 'tl_breaks_bot.db')
TRADES_CSV  = os.path.join(_BASE_DIR, 'trades_log.csv')

db_lock         = Lock()
session_headers = {}
_meta_cache: dict = {}

CSV_HEADERS = [
    'date', 'time_utc', 'pair', 'direction',
    'entry', 'sl', 'tp', 'exit_price',
    'atr', 'size', 'sl_dist',
    'pnl_usd', 'pnl_r', 'result',
    'bars_held', 'spread', 'tf', 'note',
]


# ═══════════════════════════════════════════════════════
# UTILS
# ═══════════════════════════════════════════════════════
def utc_now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

def log(msg):
    print(f'[{utc_now()}] {msg}', flush=True)


# ═══════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════
def db_init():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            key       TEXT UNIQUE,
            pair      TEXT,
            direction TEXT,
            timestamp TEXT,
            entry     REAL, sl REAL, tp REAL,
            atr       REAL, size REAL,
            spread    REAL DEFAULT 0,
            status    TEXT DEFAULT 'PENDING'
        )''')
        for col, defn in [('spread', 'REAL DEFAULT 0')]:
            try:
                conn.execute(f'ALTER TABLE trades ADD COLUMN {col} {defn}')
            except Exception:
                pass
        conn.commit()

def db_save(key, pair, direction, entry, sl, tp, atr, size, spread=0.0):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT INTO trades'
                    ' (key,pair,direction,timestamp,entry,sl,tp,atr,size,spread)'
                    ' VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (key, pair, direction, utc_now(),
                     entry, sl, tp, atr, size, spread)
                )
                conn.commit()
            except sqlite3.IntegrityError:
                pass

def db_update(key, status):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute(
                'UPDATE trades SET status=? WHERE key=?', (status, key)
            )
            conn.commit()

def db_is_dup(key):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            return conn.execute(
                'SELECT id FROM trades WHERE key=?', (key,)
            ).fetchone() is not None

def db_consec_losses(pair):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT status FROM trades"
                " WHERE pair=? AND status IN ('WIN','LOSS')"
                " ORDER BY id DESC LIMIT 8", (pair,)
            ).fetchall()
            count = 0
            for r in rows:
                if r[0] == 'LOSS': count += 1
                else: break
            return count

def db_get_pending(pair):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            row = conn.execute(
                "SELECT entry,sl,tp,atr,size,spread FROM trades"
                " WHERE pair=? AND status='PENDING'"
                " ORDER BY id DESC LIMIT 1", (pair,)
            ).fetchone()
            return row


# ═══════════════════════════════════════════════════════
# [5] CSV LOGGER
# ═══════════════════════════════════════════════════════
def csv_init():
    if not Path(TRADES_CSV).exists():
        with open(TRADES_CSV, 'w', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()
        log(f'📄 CSV جاهز: {TRADES_CSV}')

def csv_log_trade(pair, direction, entry, sl, tp, exit_price,
                  atr, size, spread=0.0, bars_held=0, note=''):
    try:
        sl_dist = abs(entry - sl)
        pnl_pts = (exit_price - entry) if direction == 'BUY' else (entry - exit_price)

        cached  = _meta_cache.get(pair)
        cs_val  = cached['data'][3] if cached else 100.0
        pnl_usd = round(pnl_pts * size * cs_val, 2)
        pnl_r   = round(pnl_pts / sl_dist, 2) if sl_dist > 0 else 0.0
        result  = 'WIN' if pnl_pts > 0 else ('LOSS' if pnl_pts < 0 else 'BE')

        now = datetime.now(timezone.utc)
        row = {
            'date':       now.strftime('%Y-%m-%d'),
            'time_utc':   now.strftime('%H:%M'),
            'pair':       pair,
            'direction':  direction,
            'entry':      entry,
            'sl':         sl,
            'tp':         tp,
            'exit_price': exit_price,
            'atr':        atr,
            'size':       size,
            'sl_dist':    round(sl_dist, 5),
            'pnl_usd':    pnl_usd,
            'pnl_r':      pnl_r,
            'result':     result,
            'bars_held':  bars_held,
            'spread':     spread,
            'tf':         STRATEGY_TF,
            'note':       note,
        }

        with open(TRADES_CSV, 'a', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writerow(row)

        icon = '✅' if result == 'WIN' else ('❌' if result == 'LOSS' else '🔵')
        log(f'  {icon} TRADE CLOSED: {pair} {direction} | '
            f'PnL=${pnl_usd:+.2f} ({pnl_r:+.2f}R) | {result}')

        nl = '\n'
        tg(
            f'{icon} *{pair} {direction} CLOSED — {result}*{nl}'
            f'Entry: `{entry}` → Exit: `{exit_price}`{nl}'
            f'PnL: `${pnl_usd:+.2f}` | `{pnl_r:+.2f}R`{nl}'
            f'Size: `{size}` | SL dist: `{sl_dist:.5f}`{nl}'
            f'Note: _{note}_{nl}'
            f'_{now.strftime("%Y-%m-%d %H:%M UTC")}_'
        )
        return result

    except Exception as ex:
        log(f'  csv_log_trade ERROR: {ex}')
        return 'ERROR'

def csv_summary(period='daily'):
    if not Path(TRADES_CSV).exists():
        return
    try:
        df = pd.read_csv(TRADES_CSV, encoding='utf-8-sig')
        if df.empty:
            log('  CSV فارغ — لا ملخص')
            return

        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        if period == 'daily':
            df = df[df['date'] == today]
            label = f'Daily — {today}'
        elif period == 'weekly':
            df['date'] = pd.to_datetime(df['date'])
            week_start = pd.Timestamp.now(tz='UTC').floor('D') - pd.Timedelta(days=7)
            df = df[df['date'] >= week_start]
            label = 'Last 7 Days'
        else:
            label = 'All Time'

        if df.empty:
            log(f'  لا صفقات للفترة: {label}')
            return

        total     = len(df)
        wins      = (df['result'] == 'WIN').sum()
        losses    = (df['result'] == 'LOSS').sum()
        wr        = round(wins / total * 100, 1)
        total_pnl = round(df['pnl_usd'].sum(), 2)
        total_r   = round(df['pnl_r'].sum(),   2)
        avg_r     = round(df['pnl_r'].mean(),  2)
        best      = df.loc[df['pnl_usd'].idxmax()]
        worst     = df.loc[df['pnl_usd'].idxmin()]
        gross_win = df[df['pnl_usd'] > 0]['pnl_usd'].sum()
        gross_los = abs(df[df['pnl_usd'] < 0]['pnl_usd'].sum())
        pf        = round(gross_win / gross_los, 2) if gross_los > 0 else 0.0

        mc = cur = 0
        for r in df['result']:
            if r == 'LOSS': cur += 1; mc = max(mc, cur)
            else: cur = 0

        print(f'\n{"═"*50}', flush=True)
        print(f'  📊 {label}', flush=True)
        print(f'{"═"*50}', flush=True)
        print(f'  Trades       : {total}', flush=True)
        print(f'  WIN / LOSS   : {wins} / {losses}', flush=True)
        print(f'  Win Rate     : {wr}%', flush=True)
        print(f'  Profit Factor: {pf}', flush=True)
        print(f'  Total PnL    : ${total_pnl:+.2f}', flush=True)
        print(f'  Total R      : {total_r:+.2f}R', flush=True)
        print(f'  Avg R/trade  : {avg_r:+.2f}R', flush=True)
        print(f'  Max consec L : {mc}', flush=True)
        print(f'  Best Trade   : ${best["pnl_usd"]:+.2f} ({best["pair"]})', flush=True)
        print(f'  Worst Trade  : ${worst["pnl_usd"]:+.2f} ({worst["pair"]})', flush=True)
        print(f'{"═"*50}\n', flush=True)

        nl = '\n'
        tg(
            f'📊 *Summary — {label}*{nl}'
            f'Trades: `{total}` | WR: `{wr}%` | PF: `{pf}`{nl}'
            f'PnL: `${total_pnl:+.2f}` | R: `{total_r:+.2f}R`{nl}'
            f'Avg R: `{avg_r:+.2f}R` | MaxL: `{mc}`{nl}'
            f'Best: `${best["pnl_usd"]:+.2f}` | Worst: `${worst["pnl_usd"]:+.2f}`'
        )

    except Exception as ex:
        log(f'  csv_summary ERROR: {ex}')


# ═══════════════════════════════════════════════════════
# API HELPERS — retries + backoff
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

def _put(path, body):
    try:
        return requests.put(
            BASE_URL + path, headers=session_headers,
            json=body, timeout=10
        )
    except Exception as ex:
        log(f'  PUT {path}: {ex}')
    return None


# ═══════════════════════════════════════════════════════
# SESSION
# ═══════════════════════════════════════════════════════
def create_session():
    url  = BASE_URL + '/api/v1/session'
    hdrs = {'X-CAP-API-KEY': API_KEY, 'Content-Type': 'application/json'}
    body = {'identifier': EMAIL, 'password': PASSWORD,
            'encryptedPassword': False}
    try:
        r = requests.post(url, headers=hdrs, json=body, timeout=15)
        if r.status_code == 200:
            session_headers.update({
                'X-SECURITY-TOKEN': r.headers.get('X-SECURITY-TOKEN'),
                'CST':              r.headers.get('CST'),
                'Content-Type':     'application/json',
            })
            log('✅ Session OK')
            return True
        log(f'❌ Session FAILED [{r.status_code}]: {r.text[:100]}')
    except Exception as ex:
        log(f'❌ Session ERROR: {ex}')
    return False

def ping_session():
    _get('/api/v1/ping')

def get_balance():
    global ACCOUNT_BALANCE
    r = _get('/api/v1/accounts')
    if r and r.status_code == 200:
        accs = r.json().get('accounts', [])
        if accs:
            ACCOUNT_BALANCE = float(
                accs[0].get('balance', {}).get('available', ACCOUNT_BALANCE)
            )
            log(f'💰 Balance: ${round(ACCOUNT_BALANCE, 2)}')

def get_open_positions():
    r = _get('/api/v1/positions')
    if r and r.status_code == 200:
        return r.json().get('positions', [])
    return []

def get_instrument_meta(epic):
    now    = time.time()
    cached = _meta_cache.get(epic)
    if cached and (now - cached['ts']) < 300:
        return cached['data']
    r = _get(f'/api/v1/markets/{epic}')
    if not r or r.status_code != 200:
        return 0.0, 0.0, 0.0, 100.0, 0.1, 1000.0
    data       = r.json()
    snap       = data.get('snapshot',     {})
    instrument = data.get('instrument',   {})
    dealing    = data.get('dealingRules', {})
    bid    = float(snap.get('bid',   0) or 0)
    ask    = float(snap.get('offer', 0) or 0)
    spread = round(ask - bid, 5)
    cs     = float(instrument.get('contractSize', 100) or 100)
    min_sz = float((dealing.get('minDealSize') or {}).get('value', 0.1)  or 0.1)
    max_sz = float((dealing.get('maxDealSize') or {}).get('value', 1000) or 1000)
    result = (bid, ask, spread, cs, min_sz, max_sz)
    _meta_cache[epic] = {'ts': now, 'data': result}
    return result


# ═══════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════
def tg(text):
    if not TG_TOKEN:
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
            data={'chat_id': TG_CHAT_ID, 'text': text,
                  'parse_mode': 'Markdown'},
            timeout=10
        )
    except Exception:
        pass

def tg_signal(sig):
    icon = '🟢' if sig['direction'] == 'BUY' else '🔴'
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    rr   = round(TP_ATR_MULT / SL_ATR_MULT, 1)
    nl   = '\n'
    tg(
        f'{icon} *{sig["pair"]} {sig["direction"]}* [{mode}]{nl}'
        f'Entry: `{sig["entry"]}` | SL: `{sig["sl"]}` | TP: `{sig["tp"]}`{nl}'
        f'R:R: `1:{rr}` | Size: `{sig["size"]}`{nl}'
        f'ATR: `{sig["atr"]}` | Spread: `{sig["spread"]}`{nl}'
        f'TF: `{STRATEGY_TF}` | Trail: `{TRAIL_MODE}`{nl}'
        f'_{utc_now()}_'
    )

def tg_result(pair, direction, status, ref, error=''):
    icon = '✅' if status in ('ACCEPTED', 'SUCCESS') else '❌'
    nl   = '\n'
    msg  = f'{icon} *{pair} {direction} {status}*{nl}Ref: `{ref}`'
    if error:
        msg += f'{nl}Err: `{error[:80]}`'
    tg(msg)

def tg_mgmt(pair, action, old_sl, new_sl):
    icons = {'BE': '🔒', 'TRAIL': '📈'}
    tg(f'{icons.get(action,"⚙️")} *{action}* {pair}\nSL: `{old_sl}` → `{new_sl}`')


# ═══════════════════════════════════════════════════════
# CANDLES
# ═══════════════════════════════════════════════════════
def fetch_candles(epic, resolution, count=300):
    r = _get(f'/api/v1/prices/{epic}',
             params={'resolution': resolution, 'max': count})
    if not r or r.status_code != 200:
        log(f'  fetch_candles FAILED ({epic})')
        return pd.DataFrame()
    prices = r.json().get('prices', [])
    if len(prices) < LENGTH * 3 + ATR_PERIOD:
        return pd.DataFrame()
    try:
        rows = [{
            'time':  p['snapshotTimeUTC'],
            'open':  (p['openPrice']['bid']  + p['openPrice']['ask'])  / 2,
            'high':  (p['highPrice']['bid']  + p['highPrice']['ask'])  / 2,
            'low':   (p['lowPrice']['bid']   + p['lowPrice']['ask'])   / 2,
            'close': (p['closePrice']['bid'] + p['closePrice']['ask']) / 2,
        } for p in prices]
    except (KeyError, TypeError) as ex:
        log(f'  candle parse ERROR: {ex}')
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df['time'] = pd.to_datetime(df['time'], utc=True)
    return df.sort_values('time').reset_index(drop=True)


# ═══════════════════════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════════════════════
def calc_atr_series(df, period=14):
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low']  - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()

def find_pivot_high(high_series, length):
    n, pivots = len(high_series), [np.nan] * len(high_series)
    for i in range(length, n - length):
        if high_series.iloc[i] == high_series.iloc[i-length: i+length+1].max():
            pivots[i] = high_series.iloc[i]
    return pivots

def find_pivot_low(low_series, length):
    n, pivots = len(low_series), [np.nan] * len(low_series)
    for i in range(length, n - length):
        if low_series.iloc[i] == low_series.iloc[i-length: i+length+1].min():
            pivots[i] = low_series.iloc[i]
    return pivots

def get_slope_val(method, df, idx, length, mult, atr_series):
    atr_val = float(atr_series.iloc[idx]) if not np.isnan(atr_series.iloc[idx]) else 1e-6
    if method == 'Stdev':
        start = max(0, idx - length + 1)
        stdev = df['close'].iloc[start:idx+1].std()
        return (stdev * mult / length) if (not np.isnan(stdev) and stdev > 0) \
               else (atr_val * mult / length)
    elif method == 'Linreg':
        start = max(0, idx - length + 1)
        y = df['close'].iloc[start:idx+1].values
        if len(y) >= 2:
            return abs(np.polyfit(np.arange(len(y)), y, 1)[0]) * mult
    return atr_val * mult / length

def tl_value(anchor_idx, anchor_val, slope, goes_up, cur_idx):
    bars = cur_idx - anchor_idx
    return anchor_val + slope * bars if goes_up else anchor_val - slope * bars


# ═══════════════════════════════════════════════════════
# [1] POSITION SIZING
# ═══════════════════════════════════════════════════════
def calc_position_size(risk_usd, sl_dist, contract_size, min_size, max_size):
    if sl_dist <= 0 or contract_size <= 0:
        return min_size
    size = round(risk_usd / (sl_dist * contract_size), 2)
    return max(min(size, max_size), min_size)


# ═══════════════════════════════════════════════════════
# SIGNAL DETECTION
# ═══════════════════════════════════════════════════════
def check_signal(pair_name, config):
    epic       = config['epic']
    allow_buy  = config['allow_buy']
    allow_sell = config['allow_sell']
    if not allow_buy and not allow_sell:
        return None

    df = fetch_candles(epic, STRATEGY_TF, CANDLES_COUNT)
    if df.empty:
        return None

    df_c  = df.iloc[:-1].copy().reset_index(drop=True)
    atr_s = calc_atr_series(df_c, ATR_PERIOD)
    ph_arr = find_pivot_high(df_c['high'], LENGTH)
    pl_arr = find_pivot_low(df_c['low'],   LENGTH)

    n        = len(df_c)
    upper_tl = None
    lower_tl = None

    for i in range(LENGTH + ATR_PERIOD, n):
        atr_i = float(atr_s.iloc[i])
        if np.isnan(atr_i) or atr_i <= 0:
            continue
        ph = ph_arr[i]
        pl = pl_arr[i]
        if not np.isnan(ph):
            slope    = get_slope_val(SLOPE_METHOD, df_c, i, LENGTH, SLOPE_MULT, atr_s)
            upper_tl = (i, float(ph), slope)
        if not np.isnan(pl):
            slope    = get_slope_val(SLOPE_METHOD, df_c, i, LENGTH, SLOPE_MULT, atr_s)
            lower_tl = (i, float(pl), slope)

    last_idx   = n - 1
    last_close = float(df_c['close'].iloc[last_idx])
    last_atr   = float(atr_s.iloc[last_idx])

    if np.isnan(last_atr) or last_atr <= 0:
        return None

    bid, ask, spread, cs, min_sz, max_sz = get_instrument_meta(epic)
    if bid <= 0 or ask <= 0:
        return None

    spread_ratio = spread / last_atr
    if spread_ratio > MAX_SPREAD_ATR_RATIO:
        log(f'  {pair_name}: spread {spread_ratio:.1%} > {MAX_SPREAD_ATR_RATIO:.1%} — skip')
        return None

    signal = None

    if allow_buy and upper_tl is not None:
        ai, av, sl_v = upper_tl
        if ai < last_idx - 1:
            u_val = tl_value(ai, av, sl_v, False, last_idx)
            if last_close > u_val:
                signal = 'BUY'
                log(f'  {pair_name}: 🟢 BUY | close={last_close:.5f} > TL={u_val:.5f}')

    if allow_sell and signal is None and lower_tl is not None:
        ai, av, sl_v = lower_tl
        if ai < last_idx - 1:
            l_val = tl_value(ai, av, sl_v, True, last_idx)
            if last_close < l_val:
                signal = 'SELL'
                log(f'  {pair_name}: 🔴 SELL | close={last_close:.5f} < TL={l_val:.5f}')

    if not signal:
        return None

    entry = ask if signal == 'BUY' else bid
    sl    = round(entry - SL_ATR_MULT * last_atr, 5) if signal == 'BUY' \
            else round(entry + SL_ATR_MULT * last_atr, 5)
    tp    = round(entry + TP_ATR_MULT * last_atr, 5) if signal == 'BUY' \
            else round(entry - TP_ATR_MULT * last_atr, 5)

    sl_dist = abs(entry - sl)
    if sl_dist < last_atr * 0.1:
        log(f'  {pair_name}: SL صغير جداً — skip')
        return None

    if config.get('size_override') is not None:
        size = max(min(float(config['size_override']), max_sz), min_sz)
    else:
        risk_usd = ACCOUNT_BALANCE * RISK_PERCENT
        size     = calc_position_size(risk_usd, sl_dist, cs, min_sz, max_sz)

    log(f'  {pair_name}: size={size} risk≈${round(size*sl_dist*cs,2)}'
        f' cs={cs} sl_dist={sl_dist:.5f}')

    return {
        'pair':      pair_name,
        'epic':      epic,
        'direction': signal,
        'entry':     round(entry, 5),
        'sl':        sl,
        'tp':        tp,
        'atr':       round(last_atr, 5),
        'size':      size,
        'spread':    round(spread, 5),
    }


# ═══════════════════════════════════════════════════════
# ORDER EXECUTION
# ═══════════════════════════════════════════════════════
def execute_order(sig):
    body = {
        'epic':           sig['epic'],
        'direction':      sig['direction'],
        'size':           sig['size'],
        'guaranteedStop': False,
        'trailingStop':   False,
        'stopLevel':      sig['sl'],
        'profitLevel':    sig['tp'],
    }
    log(f'  📤 {sig["pair"]} {sig["direction"]} | '
        f'entry≈{sig["entry"]} SL={sig["sl"]} TP={sig["tp"]} size={sig["size"]}')

    r = _post('/api/v1/positions', body)
    if not r:
        tg_result(sig['pair'], sig['direction'], 'ERROR', 'N/A', 'no response')
        return 'ERROR', 'no response'

    data = r.json()
    log(f'  RESP [{r.status_code}]: {json.dumps(data)[:200]}')

    if r.status_code == 200:
        deal_ref = data.get('dealReference', 'N/A')
        time.sleep(2)
        rc = _get(f'/api/v1/confirms/{deal_ref}')
        if rc and rc.status_code == 200:
            confirm = rc.json()
            status  = confirm.get('dealStatus', 'UNKNOWN')
            reason  = confirm.get('reason',     '')
            tg_result(sig['pair'], sig['direction'], status, deal_ref, reason)
            return status, deal_ref
        return 'UNKNOWN', deal_ref
    else:
        err = data.get('errorCode', str(data)[:80])
        tg_result(sig['pair'], sig['direction'], 'FAILED', 'N/A', err)
        return 'FAILED', err


# ═══════════════════════════════════════════════════════
# [2][3] TRADE MANAGEMENT — BE + Trailing
# ═══════════════════════════════════════════════════════
def _swing_sl(df_c, direction, lookback):
    if len(df_c) < lookback:
        return None
    recent = df_c.iloc[-lookback:]
    return (round(float(recent['low'].min()),  5) if direction == 'BUY'
            else round(float(recent['high'].max()), 5))

def manage_open_positions(positions):
    open_deal_ids = {
        pos['position']['dealId']
        for pos in positions
        if pos.get('position', {}).get('dealId')
    }

    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            pending = conn.execute(
                "SELECT key,pair,direction,entry,sl,tp,atr,size,spread"
                " FROM trades WHERE status='PENDING'"
            ).fetchall()

    for row in pending:
        key, pair, direction, entry, sl, tp, atr, size, spread = row
        still_open = any(
            pos.get('market', {}).get('epic', '') == PAIRS.get(pair, {}).get('epic', '')
            for pos in positions
        )
        if not still_open:
            epic = PAIRS.get(pair, {}).get('epic', pair)
            bid, ask, _, _, _, _ = get_instrument_meta(epic)
            exit_price = bid if direction == 'BUY' else ask
            if exit_price > 0:
                result = csv_log_trade(
                    pair, direction, entry, sl, tp, exit_price,
                    atr, size, spread=spread, note='auto-detected close'
                )
                db_update(key, result)

    for pos in positions:
        try:
            p         = pos.get('position', {})
            deal_id   = p.get('dealId',    '')
            direction = p.get('direction', '')
            entry     = float(p.get('level',     0) or 0)
            cur_sl    = float(p.get('stopLevel', 0) or 0)
            epic      = pos.get('market', {}).get('epic', '')

            if not deal_id or not entry or not epic:
                continue

            bid, ask, _, _, _, _ = get_instrument_meta(epic)
            price = bid if direction == 'BUY' else ask
            if not price:
                continue

            sl_dist = abs(entry - cur_sl)
            if sl_dist <= 0:
                continue

            profit_r = ((price - entry) if direction == 'BUY'
                        else (entry - price)) / sl_dist

            df   = fetch_candles(epic, STRATEGY_TF, 60)
            if df.empty:
                continue
            df_c    = df.iloc[:-1].copy().reset_index(drop=True)
            atr_s   = calc_atr_series(df_c, ATR_PERIOD)
            cur_atr = float(atr_s.iloc[-1])

            new_sl = None

            if profit_r >= BE_TRIGGER_R:
                be_sl = round(entry, 5)
                if direction == 'BUY'  and cur_sl < be_sl:
                    new_sl = be_sl
                    log(f'  🔒 BE {deal_id}: {cur_sl} → {new_sl}')
                    tg_mgmt(epic, 'BE', cur_sl, new_sl)
                elif direction == 'SELL' and cur_sl > be_sl:
                    new_sl = be_sl
                    log(f'  🔒 BE {deal_id}: {cur_sl} → {new_sl}')
                    tg_mgmt(epic, 'BE', cur_sl, new_sl)

            if profit_r >= TRAIL_TRIGGER_R and cur_atr > 0:
                base_sl = new_sl or cur_sl
                if TRAIL_MODE == 'STRUCTURE':
                    swing = _swing_sl(df_c, direction, SWING_LOOKBACK)
                    if swing is not None:
                        better = (swing > base_sl if direction == 'BUY'
                                  else swing < base_sl)
                        if better:
                            new_sl = swing
                            log(f'  📈 TRAIL(STRUCT) {deal_id}: → {new_sl}')
                            tg_mgmt(epic, 'TRAIL', cur_sl, new_sl)
                else:
                    if direction == 'BUY':
                        atr_sl = round(price - TRAIL_ATR_MULT * cur_atr, 5)
                        if atr_sl > base_sl + 1e-6:
                            new_sl = atr_sl
                            log(f'  📈 TRAIL(ATR) {deal_id}: → {new_sl}')
                            tg_mgmt(epic, 'TRAIL', cur_sl, new_sl)
                    else:
                        atr_sl = round(price + TRAIL_ATR_MULT * cur_atr, 5)
                        if atr_sl < base_sl - 1e-6:
                            new_sl = atr_sl
                            log(f'  📈 TRAIL(ATR) {deal_id}: → {new_sl}')
                            tg_mgmt(epic, 'TRAIL', cur_sl, new_sl)

            if new_sl is not None:
                r = _put(f'/api/v1/positions/{deal_id}', {'stopLevel': new_sl})
                if r:
                    log(f'  SL UPDATE [{r.status_code}]')

        except Exception as ex:
            log(f'  manage_positions ERROR: {ex}')


# ═══════════════════════════════════════════════════════
# SCAN
# ═══════════════════════════════════════════════════════
def run_scan():
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        log('⏸  عطلة نهاية الأسبوع')
        return

    # ── [8] فلتر الجلسة: 07:00 – 17:00 UTC (10:00 – 20:00 بتوقيتك) ──
    if not (7 <= now.hour < 17):
        log(f'⏸  خارج جلسة التداول (UTC {now.hour:02d}:00 | يبدأ 07:00 وينتهي 17:00)')
        return

    # [6] ملخص يومي عند 17:00 UTC
    if now.hour == 17 and now.minute < (SCAN_INTERVAL // 60 + 1):
        csv_summary(period='daily')

    log('─' * 55)
    log(f'🔍 Scan | TF={STRATEGY_TF} | {"DEMO" if DEMO_MODE else "LIVE"}')
    log('─' * 55)

    get_balance()
    open_pos = get_open_positions()
    log(f'  صفقات مفتوحة: {len(open_pos)} / {MAX_OPEN_TRADES}')

    manage_open_positions(open_pos)

    if len(open_pos) >= MAX_OPEN_TRADES:
        log('  ⏸  الحد الأقصى للصفقات')
        return

    ts_key = now.strftime('%Y-%m-%d_%H')

    for pair_name, config in PAIRS.items():
        consec = db_consec_losses(pair_name)
        if consec >= MAX_CONSECUTIVE_LOSS:
            log(f'  {pair_name}: ⚠️  {consec} خسائر — تخطّي')
            continue

        key = f'{pair_name}_{ts_key}'
        if db_is_dup(key):
            log(f'  {pair_name}: ⏸  مكرر ({key})')
            continue

        log(f'  {pair_name}: فحص ...')
        sig = check_signal(pair_name, config)

        if sig is None:
            log(f'  {pair_name}: لا إشارة')
            continue

        db_save(key, pair_name, sig['direction'],
                sig['entry'], sig['sl'], sig['tp'],
                sig['atr'], sig['size'], sig['spread'])
        tg_signal(sig)

        status, ref = execute_order(sig)
        db_update(key, status)
        log(f'  {pair_name}: {status} | {ref}')

        open_pos = get_open_positions()
        if len(open_pos) >= MAX_OPEN_TRADES:
            break
        time.sleep(2)


# ═══════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════
def start_bot():
    db_init()
    csv_init()

    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    nl   = '\n'

    print('=' * 55, flush=True)
    print(f'  TL Breaks Bot v2 [{mode}] — Railway', flush=True)
    print(f'  TF       : {STRATEGY_TF}',            flush=True)
    print(f'  Method   : {SLOPE_METHOD}',            flush=True)
    print(f'  SL/TP    : {SL_ATR_MULT}/{TP_ATR_MULT}×ATR', flush=True)
    print(f'  BE       : {BE_TRIGGER_R}R',           flush=True)
    print(f'  Trail    : {TRAIL_MODE} @ {TRAIL_TRIGGER_R}R', flush=True)
    print(f'  Spread   : max {MAX_SPREAD_ATR_RATIO:.0%} ATR', flush=True)
    print(f'  Session  : 07:00 – 17:00 UTC (10:00 – 20:00 AST)', flush=True)
    print(f'  DB       : {DB_FILE}',                 flush=True)
    print(f'  CSV      : {TRADES_CSV}',              flush=True)
    for pn, pc in PAIRS.items():
        b = '✅' if pc['allow_buy']  else '❌'
        s = '✅' if pc['allow_sell'] else '❌'
        print(f'  {pn:<8}: BUY={b} SELL={s}', flush=True)
    print('=' * 55, flush=True)

    tg(
        f'🚀 *TL Breaks Bot v2* [{mode}]{nl}'
        f'TF: `{STRATEGY_TF}` | Trail: `{TRAIL_MODE}`{nl}'
        f'SL/TP: `{SL_ATR_MULT}/{TP_ATR_MULT}×ATR` | BE: `{BE_TRIGGER_R}R`{nl}'
        f'Session: `07:00 – 17:00 UTC`{nl}'
        f'CSV: ✅ | DB: ✅{nl}'
        + ''.join(
            f'{pn}: BUY={"✅" if pc["allow_buy"] else "❌"} '
            f'SELL={"✅" if pc["allow_sell"] else "❌"}{nl}'
            for pn, pc in PAIRS.items()
        )
        + f'_{utc_now()}_'
    )

    session_age = 0
    while True:
        try:
            if session_age == 0:
                if not create_session():
                    time.sleep(60)
                    continue
            else:
                ping_session()
            session_age = (session_age + 1) % 25
            run_scan()

        except KeyboardInterrupt:
            log('🛑 Bot stopped')
            csv_summary(period='all')
            tg('🛑 TL Breaks Bot v2 stopped')
            break
        except Exception as ex:
            log(f'LOOP ERROR: {ex}')
            tg(f'❌ Bot Error: `{str(ex)[:100]}`')

        time.sleep(SCAN_INTERVAL)


if __name__ == '__main__':
    start_bot()
