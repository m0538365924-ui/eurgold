#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==========================================================
# tl_breaks_bot.py — Trendlines with Breaks Bot
# GOLD + EURUSD | Capital.com API | Railway Ready
# ==========================================================
# الإصلاحات:
# 1. GOLD: BUY فقط (بناءً على الباكتيست PF 2.46)
# 2. contractSize صحيح لحساب الحجم
# 3. فلتر الاتجاه H4 EMA قبل الدخول
# 4. Spread filter
# 5. ts_key بالتاريخ + الساعة بشكل صحيح
# ==========================================================

import os, json, time, sqlite3, requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from threading import Lock
from dotenv import load_dotenv

load_dotenv()

# ================================================
# ⚙️  CONFIG
# ================================================
API_KEY    = os.getenv('CAPITAL_API_KEY',  '')
EMAIL      = os.getenv('CAPITAL_EMAIL',    '')
PASSWORD   = os.getenv('CAPITAL_PASSWORD', '')
TG_TOKEN   = os.getenv('TG_TOKEN',        '')
TG_CHAT_ID = os.getenv('TG_CHAT_ID',      '')

BASE_URL  = 'https://api-capital.backend-capital.com'
DEMO_MODE = os.getenv('DEMO_MODE', 'true').lower() == 'true'

# ================================================
# 📊 PAIRS CONFIG
# ================================================
PAIRS = {
    'GOLD': {
        'epic':        'GOLD',
        'allow_buy':   True,    # ✅ BUY فقط — أثبتناه بالباكتيست
        'allow_sell':  False,
        'size_override': None,
        'max_spread':  2.0,     # دولار
    },
    'EURUSD': {
        'epic':        'EURUSD',
        'allow_buy':   True,
        'allow_sell':  True,
        'size_override': None,
        'max_spread':  0.001,
    },
}

# ================================================
# 📈 STRATEGY PARAMETERS
# ================================================
STRATEGY_TF   = os.getenv('STRATEGY_TF',   'HOUR')       # H1
CANDLES_COUNT = 300
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '3600'))   # كل ساعة لـ H1

LENGTH       = int(os.getenv('LENGTH',      '10'))
SLOPE_MULT   = float(os.getenv('SLOPE_MULT','1.0'))
SLOPE_METHOD = os.getenv('SLOPE_METHOD',    'ATR')
ATR_PERIOD   = 14

SL_ATR_MULT  = float(os.getenv('SL_ATR_MULT', '1.5'))
TP_ATR_MULT  = float(os.getenv('TP_ATR_MULT', '3.0'))    # R:R 1:2

# فلتر الاتجاه H4
H4_EMA_FAST  = 21
H4_EMA_MID   = 50
H4_EMA_SLOW  = 200

# ================================================
# 💰 RISK
# ================================================
RISK_PERCENT         = float(os.getenv('RISK_PERCENT',   '0.01'))
MAX_OPEN_TRADES      = int(os.getenv('MAX_OPEN_TRADES',  '2'))
MAX_CONSECUTIVE_LOSS = int(os.getenv('MAX_CONSEC_LOSS',  '4'))
ACCOUNT_BALANCE      = float(os.getenv('ACCOUNT_BALANCE','1000'))

# ================================================
# 🗄️  DATABASE
# ================================================
DB_FILE         = 'tl_breaks_bot.db'
db_lock         = Lock()
session_headers = {}

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
            status    TEXT DEFAULT 'PENDING')''')
        conn.commit()

def db_save(key, pair, direction, entry, sl, tp, atr, size):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT INTO trades (key,pair,direction,timestamp,entry,sl,tp,atr,size)'
                    ' VALUES (?,?,?,?,?,?,?,?,?)',
                    (key, pair, direction, utc_now(), entry, sl, tp, atr, size)
                )
                conn.commit()
            except sqlite3.IntegrityError:
                pass

def db_update(key, status):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('UPDATE trades SET status=? WHERE key=?', (status, key))
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
                "SELECT status FROM trades WHERE pair=? AND status IN ('ACCEPTED','FAILED')"
                " ORDER BY id DESC LIMIT 8", (pair,)
            ).fetchall()
            count = 0
            for r in rows:
                if r[0] == 'FAILED': count += 1
                else: break
            return count

# ================================================
# 🔧 UTILS
# ================================================
def utc_now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

def log(msg):
    print(f'[{utc_now()}] {msg}', flush=True)

# ================================================
# 🌐 SESSION
# ================================================
def create_session():
    url  = BASE_URL + '/api/v1/session'
    hdrs = {'X-CAP-API-KEY': API_KEY, 'Content-Type': 'application/json'}
    body = {'identifier': EMAIL, 'password': PASSWORD, 'encryptedPassword': False}
    try:
        r = requests.post(url, headers=hdrs, json=body, timeout=15)
        if r.status_code == 200:
            session_headers.update({
                'X-SECURITY-TOKEN': r.headers.get('X-SECURITY-TOKEN'),
                'CST':              r.headers.get('CST'),
                'Content-Type':     'application/json'
            })
            log('✅ Session OK')
            return True
        log('❌ Session FAILED: ' + r.text[:100])
    except Exception as ex:
        log('❌ Session ERROR: ' + str(ex))
    return False

def ping_session():
    try:
        requests.get(BASE_URL + '/api/v1/ping',
                     headers=session_headers, timeout=10)
    except: pass

def get_balance():
    global ACCOUNT_BALANCE
    try:
        r = requests.get(BASE_URL + '/api/v1/accounts',
                         headers=session_headers, timeout=10)
        if r.status_code == 200:
            accs = r.json().get('accounts', [])
            if accs:
                ACCOUNT_BALANCE = float(
                    accs[0].get('balance', {}).get('available', ACCOUNT_BALANCE))
                log(f'💰 Balance: ${round(ACCOUNT_BALANCE, 2)}')
    except Exception as ex:
        log('Balance ERROR: ' + str(ex))

def get_open_positions():
    try:
        r = requests.get(BASE_URL + '/api/v1/positions',
                         headers=session_headers, timeout=10)
        if r.status_code == 200:
            return [p for p in r.json().get('positions', [])
                    if p.get('position', {}).get('dealStatus', 'OPEN')
                    not in ('CLOSED', 'DELETED', 'REJECTED')]
    except Exception as ex:
        log('Positions ERROR: ' + str(ex))
    return []

def get_market_info(epic):
    try:
        r = requests.get(BASE_URL + '/api/v1/markets/' + epic,
                         headers=session_headers, timeout=10)
        if r.status_code == 200:
            data     = r.json()
            snap     = data.get('snapshot', {})
            inst     = data.get('instrument', {})
            dealing  = data.get('dealingRules', {})
            bid      = snap.get('bid', 0)
            ask      = snap.get('offer', 0)
            spread   = round(ask - bid, 5)
            cs       = float(inst.get('contractSize', 100))
            min_size = float(dealing.get('minDealSize', {}).get('value', 0.1))
            return bid, ask, spread, cs, min_size
    except Exception as ex:
        log(f'market_info ERROR ({epic}): {ex}')
    return 0, 0, 0, 100, 0.1

# ================================================
# 💵 POSITION SIZING — إصلاح 2: contractSize صحيح
# ================================================
def calc_size(sl_dist, contract_size, min_size):
    if sl_dist <= 0 or contract_size <= 0:
        return min_size
    risk_usd    = ACCOUNT_BALANCE * RISK_PERCENT
    point_value = contract_size * 0.01
    sl_points   = sl_dist / 0.01
    size        = risk_usd / (sl_points * point_value / 100)
    size        = max(round(size, 2), min_size)
    size        = min(size, 10.0)
    log(f'  💵 Size={size} | SL_dist={sl_dist} | CS={contract_size} | Risk=${round(risk_usd,2)}')
    return size

# ================================================
# 📡 TELEGRAM
# ================================================
def tg(text):
    if not TG_TOKEN: return
    try:
        requests.post(
            f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
            data={'chat_id': TG_CHAT_ID, 'text': text, 'parse_mode': 'Markdown'},
            timeout=10
        )
    except: pass

def tg_signal(pair, direction, entry, sl, tp, atr, size):
    icon = '🟢' if direction == 'BUY' else '🔴'
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    nl   = '\n'
    tg(f'{icon} *{pair} {direction}* [{mode}]{nl}'
       f'Entry: `{entry}` | SL: `{sl}` | TP: `{tp}`{nl}'
       f'ATR: `{atr}` | Size: `{size}`{nl}'
       f'TF: `{STRATEGY_TF}` | _{utc_now()}_')

def tg_result(pair, direction, status, ref, error=''):
    icon = '✅' if status in ('ACCEPTED','SUCCESS') else '❌'
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    nl   = '\n'
    msg  = f'{icon} *{pair} {direction} {status}* [{mode}]{nl}Ref: `{ref}`'
    if error: msg += f'{nl}Err: `{error}`'
    tg(msg)

# ================================================
# 📈 CANDLES
# ================================================
def fetch_candles(epic, resolution, count=300):
    try:
        r = requests.get(
            BASE_URL + '/api/v1/prices/' + epic,
            headers=session_headers,
            params={'resolution': resolution, 'max': count},
            timeout=20
        )
        if r.status_code == 429:
            time.sleep(5)
            r = requests.get(BASE_URL + '/api/v1/prices/' + epic,
                             headers=session_headers,
                             params={'resolution': resolution, 'max': count},
                             timeout=20)
        if r.status_code != 200: return pd.DataFrame()
        prices = r.json().get('prices', [])
        if len(prices) < LENGTH * 3: return pd.DataFrame()
        rows = [{
            'time':  p['snapshotTimeUTC'],
            'open':  (p['openPrice']['bid']  + p['openPrice']['ask'])  / 2,
            'high':  (p['highPrice']['bid']  + p['highPrice']['ask'])  / 2,
            'low':   (p['lowPrice']['bid']   + p['lowPrice']['ask'])   / 2,
            'close': (p['closePrice']['bid'] + p['closePrice']['ask']) / 2,
        } for p in prices]
        df = pd.DataFrame(rows)
        df['time'] = pd.to_datetime(df['time'], utc=True)
        return df.sort_values('time').reset_index(drop=True)
    except Exception as ex:
        log(f'fetch_candles ERROR ({epic}): {ex}')
        return pd.DataFrame()

# ================================================
# 📐 INDICATORS
# ================================================
def calc_atr(df, p=14):
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low']  - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(span=p, adjust=False).mean()

def calc_ema(s, p):
    return s.ewm(span=p, adjust=False).mean()

# إصلاح 3: فلتر الاتجاه H4
def get_h4_trend(epic):
    """يُرجع 'UP' | 'DOWN' | 'SIDEWAYS'"""
    try:
        h4 = fetch_candles(epic, 'HOUR_4', 60)
        if h4.empty or len(h4) < 20: return 'SIDEWAYS'
        e21  = float(calc_ema(h4['close'], H4_EMA_FAST).iloc[-1])
        e50  = float(calc_ema(h4['close'], H4_EMA_MID).iloc[-1])
        e200 = float(calc_ema(h4['close'], H4_EMA_SLOW).iloc[-1])
        if e21 > e50 > e200: return 'UP'
        if e21 < e50 < e200: return 'DOWN'
        return 'SIDEWAYS'
    except:
        return 'SIDEWAYS'

def find_pivot_high(high_series, length):
    n = len(high_series); pivots = [np.nan] * n
    for i in range(length, n - length):
        window = high_series.iloc[i - length: i + length + 1]
        if high_series.iloc[i] == window.max():
            pivots[i] = high_series.iloc[i]
    return pivots

def find_pivot_low(low_series, length):
    n = len(low_series); pivots = [np.nan] * n
    for i in range(length, n - length):
        window = low_series.iloc[i - length: i + length + 1]
        if low_series.iloc[i] == window.min():
            pivots[i] = low_series.iloc[i]
    return pivots

def get_slope_val(method, df, idx, length, mult, atr_s):
    atr_val = float(atr_s.iloc[idx]) if not np.isnan(atr_s.iloc[idx]) else 1e-6
    if method == 'ATR':
        return atr_val * mult / length
    elif method == 'Stdev':
        start = max(0, idx - length + 1)
        stdev = df['close'].iloc[start:idx+1].std()
        if np.isnan(stdev) or stdev == 0: return atr_val * mult / length
        return stdev * mult / length
    elif method == 'Linreg':
        start = max(0, idx - length + 1)
        y = df['close'].iloc[start:idx+1].values
        if len(y) < 2: return atr_val * mult / length
        slope = np.polyfit(np.arange(len(y)), y, 1)[0]
        return abs(slope) * mult
    return atr_val * mult / length

def tl_value(anchor_idx, anchor_val, slope, goes_up, cur_idx):
    bars = cur_idx - anchor_idx
    return anchor_val + slope * bars if goes_up else anchor_val - slope * bars

# ================================================
# 🔍 SIGNAL DETECTION
# ================================================
def check_signal(pair_name, config):
    epic       = config['epic']
    allow_buy  = config['allow_buy']
    allow_sell = config['allow_sell']
    max_spread = config.get('max_spread', 2.0)

    if not allow_buy and not allow_sell: return None

    # إصلاح 4: Spread filter
    bid, ask, spread, cs, min_size = get_market_info(epic)
    if bid <= 0: return None
    if spread > max_spread:
        log(f'  {pair_name}: ⏸ Spread={spread} > {max_spread}')
        return None

    # إصلاح 3: فلتر الاتجاه H4
    h4_trend = get_h4_trend(epic)
    log(f'  {pair_name}: H4 Trend = {h4_trend}')

    # إذا SIDEWAYS نتجاهل
    if h4_trend == 'SIDEWAYS':
        log(f'  {pair_name}: ⏸ H4 SIDEWAYS — تخطّي')
        return None

    # نسمح BUY فقط في ترند UP، وSELL فقط في ترند DOWN
    effective_buy  = allow_buy  and h4_trend == 'UP'
    effective_sell = allow_sell and h4_trend == 'DOWN'

    if not effective_buy and not effective_sell:
        log(f'  {pair_name}: ⏸ الاتجاه H4 لا يوافق الإعداد')
        return None

    df = fetch_candles(epic, STRATEGY_TF, CANDLES_COUNT)
    if df.empty or len(df) < LENGTH * 3 + ATR_PERIOD:
        log(f'  {pair_name}: بيانات غير كافية')
        return None

    atr_s  = calc_atr(df, ATR_PERIOD)
    ph_arr = find_pivot_high(df['high'], LENGTH)
    pl_arr = find_pivot_low(df['low'],  LENGTH)

    n          = len(df) - 1
    upper_tl   = None
    lower_tl   = None
    signal     = None

    for i in range(LENGTH + ATR_PERIOD, n):
        atr_i = float(atr_s.iloc[i])
        if np.isnan(atr_i) or atr_i <= 0: continue
        ph = ph_arr[i]; pl = pl_arr[i]

        if not np.isnan(ph):
            slope    = get_slope_val(SLOPE_METHOD, df, i, LENGTH, SLOPE_MULT, atr_s)
            upper_tl = (i, float(ph), slope)

        if not np.isnan(pl):
            slope    = get_slope_val(SLOPE_METHOD, df, i, LENGTH, SLOPE_MULT, atr_s)
            lower_tl = (i, float(pl), slope)

    last_idx   = n - 1
    last_close = float(df['close'].iloc[last_idx])
    last_atr   = float(atr_s.iloc[last_idx])
    if np.isnan(last_atr) or last_atr <= 0: return None

    # BUY: كسر خط المقاومة للأعلى مع H4 UP
    if effective_buy and upper_tl is not None:
        anchor_idx, anchor_val, slope = upper_tl
        if anchor_idx < last_idx - 1:
            u_val = tl_value(anchor_idx, anchor_val, slope, False, last_idx)
            if last_close > u_val:
                signal = 'BUY'
                log(f'  {pair_name}: 🟢 BUY | close={last_close:.5f} > TL={u_val:.5f}')

    # SELL: كسر خط الدعم للأسفل مع H4 DOWN
    if effective_sell and signal is None and lower_tl is not None:
        anchor_idx, anchor_val, slope = lower_tl
        if anchor_idx < last_idx - 1:
            l_val = tl_value(anchor_idx, anchor_val, slope, True, last_idx)
            if last_close < l_val:
                signal = 'SELL'
                log(f'  {pair_name}: 🔴 SELL | close={last_close:.5f} < TL={l_val:.5f}')

    if not signal: return None

    entry = ask if signal == 'BUY' else bid
    if signal == 'BUY':
        sl = round(entry - SL_ATR_MULT * last_atr, 5)
        tp = round(entry + TP_ATR_MULT * last_atr, 5)
    else:
        sl = round(entry + SL_ATR_MULT * last_atr, 5)
        tp = round(entry - TP_ATR_MULT * last_atr, 5)

    sl_dist = abs(entry - sl)
    if sl_dist <= 0: return None

    # إصلاح 2: حساب الحجم الصحيح
    if config.get('size_override'):
        size = config['size_override']
    else:
        size = calc_size(sl_dist, cs, min_size)

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
        'h4_trend':  h4_trend,
    }

# ================================================
# 📤 EXECUTE ORDER
# ================================================
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
    log(f'  📤 {sig["pair"]} {sig["direction"]} | entry≈{sig["entry"]}'
        f' SL={sig["sl"]} TP={sig["tp"]} size={sig["size"]}')
    try:
        r    = requests.post(BASE_URL + '/api/v1/positions',
                             headers=session_headers, json=body, timeout=15)
        data = r.json()
        log(f'  RESP [{r.status_code}]: {json.dumps(data)[:150]}')
        if r.status_code == 200:
            deal_ref = data.get('dealReference', 'N/A')
            time.sleep(2)
            confirm  = requests.get(
                BASE_URL + '/api/v1/confirms/' + deal_ref,
                headers=session_headers, timeout=10
            ).json()
            status = confirm.get('dealStatus', 'UNKNOWN')
            reason = confirm.get('reason', '')
            tg_result(sig['pair'], sig['direction'], status, deal_ref, reason)
            return status, deal_ref
        else:
            err = data.get('errorCode', str(data)[:80])
            tg_result(sig['pair'], sig['direction'], 'FAILED', 'N/A', err)
            return 'FAILED', err
    except Exception as ex:
        log(f'  EXCEPTION: {ex}')
        return 'ERROR', str(ex)

# ================================================
# 🔄 SCAN LOOP
# ================================================
def run_scan():
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        log('⏸  عطلة نهاية الأسبوع')
        return

    log('─' * 55)
    log(f'🔍 Scan | TF={STRATEGY_TF} | {"DEMO" if DEMO_MODE else "LIVE"}')
    log('─' * 55)

    get_balance()
    open_pos = get_open_positions()
    log(f'  مفتوحة: {len(open_pos)} / {MAX_OPEN_TRADES}')

    if len(open_pos) >= MAX_OPEN_TRADES:
        log('  ⏸  وصلنا الحد الأقصى للصفقات')
        return

    # إصلاح 5: ts_key بالتاريخ + الساعة
    ts_key = now.strftime('%Y-%m-%d_%H')

    for pair_name, config in PAIRS.items():
        consec = db_consec_losses(pair_name)
        if consec >= MAX_CONSECUTIVE_LOSS:
            log(f'  {pair_name}: ⚠️ {consec} خسائر متتالية — تخطّي')
            continue

        key = f'{pair_name}_{ts_key}'
        if db_is_dup(key):
            log(f'  {pair_name}: ⏸ إشارة مكررة')
            continue

        log(f'  {pair_name}: فحص ...')
        sig = check_signal(pair_name, config)

        if sig is None:
            log(f'  {pair_name}: لا إشارة')
            continue

        db_save(key, pair_name, sig['direction'],
                sig['entry'], sig['sl'], sig['tp'],
                sig['atr'], sig['size'])
        tg_signal(pair_name, sig['direction'],
                  sig['entry'], sig['sl'], sig['tp'],
                  sig['atr'], sig['size'])

        status, ref = execute_order(sig)
        db_update(key, status)
        log(f'  {pair_name}: {status} | {ref}')

        open_pos = get_open_positions()
        if len(open_pos) >= MAX_OPEN_TRADES:
            break
        time.sleep(2)

# ================================================
# 🚀 MAIN
# ================================================
def start_bot():
    db_init()
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    nl   = '\n'

    print('=' * 55, flush=True)
    print(f'  Trendlines Breaks Bot [{mode}]',    flush=True)
    print(f'  TF      : {STRATEGY_TF}',           flush=True)
    print(f'  Method  : {SLOPE_METHOD}',           flush=True)
    print(f'  Length  : {LENGTH} | Slope: {SLOPE_MULT}', flush=True)
    print(f'  SL/TP   : {SL_ATR_MULT}/{TP_ATR_MULT} × ATR', flush=True)
    print(f'  H4 Trend filter: ✅',               flush=True)
    for pn, pc in PAIRS.items():
        b = '✅' if pc['allow_buy']  else '❌'
        s = '✅' if pc['allow_sell'] else '❌'
        print(f'  {pn}: BUY={b} SELL={s}', flush=True)
    print('=' * 55, flush=True)

    tg(f'🚀 *Trendlines Breaks Bot* [{mode}]{nl}'
       f'TF: `{STRATEGY_TF}` | H4 Trend Filter: ✅{nl}'
       f'SL/TP: `{SL_ATR_MULT}/{TP_ATR_MULT}×ATR`{nl}'
       f'_{utc_now()}_')

    session_age = 0
    while True:
        try:
            if session_age == 0:
                if not create_session():
                    time.sleep(60); continue
            else:
                ping_session()
            session_age = (session_age + 1) % 25
            run_scan()
        except KeyboardInterrupt:
            log('🛑 Bot stopped')
            tg('🛑 Trendlines Breaks Bot stopped')
            break
        except Exception as ex:
            log(f'LOOP ERROR: {ex}')
            tg(f'❌ Error: `{str(ex)[:100]}`')
        time.sleep(SCAN_INTERVAL)


if __name__ == '__main__':
    start_bot()
