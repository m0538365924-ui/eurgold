#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==========================================================
# tl_breaks_bot.py — Trendlines with Breaks Bot
# GOLD + EURUSD | Capital.com API | Railway Ready
# ==========================================================
# الاستراتيجية:
#   1. كشف Pivot Highs/Lows
#   2. رسم خطوط ترند بميل ATR
#   3. Breakout: close > upper TL → BUY
#                close < lower TL → SELL
#   4. دخول على open الشمعة التالية (Market Order)
#   5. SL/TP بناءً على ATR
# ==========================================================

import os, json, time, sqlite3, requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from threading import Lock
from dotenv import load_dotenv

load_dotenv()

# ================================================
# ⚙️  CONFIG — عدّلها أو ضعها في .env
# ================================================
API_KEY    = os.getenv('CAPITAL_API_KEY',  'BbmFhEF3FffkcR0Y')
EMAIL      = os.getenv('CAPITAL_EMAIL',    'almorese2013@gmail.com')
PASSWORD   = os.getenv('CAPITAL_PASSWORD', 'Ba050326>')
TG_TOKEN   = os.getenv('TG_TOKEN',         '8782238258:AAEtuQg7OYAmoemhWfLqKdYpqIxfWwyKRSQ')
TG_CHAT_ID = os.getenv('TG_CHAT_ID',       '533243705')

BASE_URL   = 'https://api-capital.backend-capital.com'
DEMO_MODE  = os.getenv('DEMO_MODE', 'false').lower() == 'true'

# ================================================
# 📊 PAIRS CONFIG
# ================================================
# لكل زوج: epic, allow_buy, allow_sell, size_override (None=احسب تلقائي)
PAIRS = {
    'GOLD': {
        'epic':        'GOLD',
        'allow_buy':   False,   # ← SELL فقط للذهب (بناءً على الباكتيست)
        'allow_sell':  True,
        'size_override': None,
    },
    'EURUSD': {
        'epic':        'EURUSD',
        'allow_buy':   True,    # ← BUY + SELL لليورو
        'allow_sell':  True,
        'size_override': 1000,
        'correlated_with': ['GBPUSD'],
    },
}

# ================================================
# 📈 TIMEFRAME CONFIG
# ================================================
# الفريم المستخدم للاستراتيجية
# خيارات: MINUTE, MINUTE_2, MINUTE_5, MINUTE_15,
#          MINUTE_30, HOUR, HOUR_4, DAY
STRATEGY_TF     = os.getenv('STRATEGY_TF', 'MINUTE_15')
CANDLES_COUNT   = 300    # عدد الشموع المجلوبة

# مدة الانتظار بين كل scan (ثانية)
# يفضّل = مدة الفريم ÷ 4 تقريباً
SCAN_INTERVAL   = int(os.getenv('SCAN_INTERVAL', '300'))  # ثانية

# ================================================
# 🎯 STRATEGY PARAMETERS
# ================================================
LENGTH       = int(os.getenv('LENGTH',   '10'))    # فترة الـ PivotS
SLOPE_MULT   = float(os.getenv('SLOPE_MULT', '1.0'))
SLOPE_METHOD = os.getenv('SLOPE_METHOD', 'ATR')    # ATR | Stdev | Linreg
ATR_PERIOD   = 14

SL_ATR_MULT  = float(os.getenv('SL_ATR_MULT', '1.5'))
TP_ATR_MULT  = float(os.getenv('TP_ATR_MULT', '2.5'))

# ================================================
# 💰 RISK
# ================================================
RISK_PERCENT         = float(os.getenv('RISK_PERCENT', '0.01'))
MAX_OPEN_TRADES      = int(os.getenv('MAX_OPEN_TRADES', '3'))
MAX_CONSECUTIVE_LOSS = int(os.getenv('MAX_CONSEC_LOSS', '5'))
ACCOUNT_BALANCE      = float(os.getenv('ACCOUNT_BALANCE', '1000'))

# ================================================
# 🗄️  DATABASE
# ================================================
DB_FILE  = 'tl_breaks_bot.db'
db_lock  = Lock()
session_headers = {}

def db_init():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT UNIQUE,
            pair TEXT,
            direction TEXT,
            timestamp TEXT,
            entry REAL, sl REAL, tp REAL,
            atr REAL, size REAL,
            status TEXT DEFAULT 'PENDING'
        )''')
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
                "SELECT status FROM trades WHERE pair=? AND status IN ('WIN','LOSS')"
                " ORDER BY id DESC LIMIT 8",
                (pair,)
            ).fetchall()
            count = 0
            for r in rows:
                if r[0] == 'LOSS': count += 1
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
    except:
        pass

def get_balance():
    global ACCOUNT_BALANCE
    try:
        r = requests.get(BASE_URL + '/api/v1/accounts',
                         headers=session_headers, timeout=10)
        if r.status_code == 200:
            accs = r.json().get('accounts', [])
            if accs:
                ACCOUNT_BALANCE = float(
                    accs[0].get('balance', {}).get('available', ACCOUNT_BALANCE)
                )
                log(f'💰 Balance: ${round(ACCOUNT_BALANCE, 2)}')
    except Exception as ex:
        log('Balance ERROR: ' + str(ex))

def get_open_positions():
    try:
        r = requests.get(BASE_URL + '/api/v1/positions',
                         headers=session_headers, timeout=10)
        if r.status_code == 200:
            return r.json().get('positions', [])
    except Exception as ex:
        log('Positions ERROR: ' + str(ex))
    return []

def get_market_info(epic):
    try:
        r = requests.get(BASE_URL + '/api/v1/markets/' + epic,
                         headers=session_headers, timeout=10)
        if r.status_code == 200:
            data       = r.json()
            snap       = data.get('snapshot', {})
            instrument = data.get('instrument', {})
            dealing    = data.get('dealingRules', {})
            bid        = snap.get('bid', 0)
            ask        = snap.get('offer', 0)
            spread     = round(ask - bid, 5)
            cs         = float(instrument.get('contractSize', 100))
            min_size   = float(
                dealing.get('minDealSize', {}).get('value', 0.1)
            )
            return bid, ask, spread, cs, min_size
    except Exception as ex:
        log(f'market_info ERROR ({epic}): {ex}')
    return 0, 0, 0, 100, 0.1

# ================================================
# 📡 TELEGRAM
# ================================================
def tg(text):
    if not TG_TOKEN or TG_TOKEN == 'YOUR_TG_TOKEN':
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
            data={'chat_id': TG_CHAT_ID, 'text': text, 'parse_mode': 'Markdown'},
            timeout=10
        )
    except:
        pass

def tg_signal(pair, direction, entry, sl, tp, atr, size):
    icon = '🟢' if direction == 'BUY' else '🔴'
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    nl   = '\n'
    tg(
        f'{icon} *{pair} {direction}* [{mode}]{nl}'
        f'Entry: `{entry}` | SL: `{sl}` | TP: `{tp}`{nl}'
        f'ATR: `{atr}` | Size: `{size}`{nl}'
        f'TF: `{STRATEGY_TF}` | Method: `{SLOPE_METHOD}`{nl}'
        f'_{utc_now()}_'
    )

def tg_result(pair, direction, status, ref, error=''):
    icon = '✅' if status in ('ACCEPTED', 'SUCCESS') else '❌'
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    nl   = '\n'
    msg  = f'{icon} *{pair} {direction} {status}* [{mode}]{nl}Ref: `{ref}`'
    if error:
        msg += f'{nl}Err: `{error}`'
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
            r = requests.get(
                BASE_URL + '/api/v1/prices/' + epic,
                headers=session_headers,
                params={'resolution': resolution, 'max': count},
                timeout=20
            )
        if r.status_code != 200:
            return pd.DataFrame()
        prices = r.json().get('prices', [])
        if len(prices) < LENGTH * 3:
            return pd.DataFrame()
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
def calc_atr_series(df, period=14):
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low']  - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()

def find_pivot_high(high_series, length):
    n      = len(high_series)
    pivots = [np.nan] * n
    for i in range(length, n - length):
        window = high_series.iloc[i - length: i + length + 1]
        if high_series.iloc[i] == window.max():
            pivots[i] = high_series.iloc[i]
    return pivots

def find_pivot_low(low_series, length):
    n      = len(low_series)
    pivots = [np.nan] * n
    for i in range(length, n - length):
        window = low_series.iloc[i - length: i + length + 1]
        if low_series.iloc[i] == window.min():
            pivots[i] = low_series.iloc[i]
    return pivots

def get_slope_val(method, df, idx, length, mult, atr_series):
    atr_val = float(atr_series.iloc[idx]) if not np.isnan(atr_series.iloc[idx]) else 1e-6

    if method == 'ATR':
        return atr_val * mult / length

    elif method == 'Stdev':
        start = max(0, idx - length + 1)
        stdev = df['close'].iloc[start:idx+1].std()
        if np.isnan(stdev) or stdev == 0:
            return atr_val * mult / length
        return stdev * mult / length

    elif method == 'Linreg':
        start = max(0, idx - length + 1)
        y = df['close'].iloc[start:idx+1].values
        if len(y) < 2:
            return atr_val * mult / length
        x = np.arange(len(y))
        slope = np.polyfit(x, y, 1)[0]
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

    if not allow_buy and not allow_sell:
        return None

    df = fetch_candles(epic, STRATEGY_TF, CANDLES_COUNT)
    if df.empty or len(df) < LENGTH * 3 + ATR_PERIOD:
        log(f'  {pair_name}: بيانات غير كافية')
        return None

    # احسب المؤشرات
    atr_s  = calc_atr_series(df, ATR_PERIOD)
    ph_arr = find_pivot_high(df['high'], LENGTH)
    pl_arr = find_pivot_low(df['low'],  LENGTH)

    # ابحث في آخر الشموع المكتملة (نتجنب الشمعة الأخيرة غير المكتملة)
    n         = len(df) - 1   # نتجاهل الشمعة الحالية
    upper_tl  = None
    lower_tl  = None
    signal    = None

    # بنِ خطوط الترند وابحث عن كسر في آخر شمعة مكتملة
    for i in range(LENGTH + ATR_PERIOD, n):
        ph = ph_arr[i]
        pl = pl_arr[i]
        atr_i = float(atr_s.iloc[i])
        if np.isnan(atr_i) or atr_i <= 0:
            continue

        if not np.isnan(ph):
            slope    = get_slope_val(SLOPE_METHOD, df, i, LENGTH, SLOPE_MULT, atr_s)
            upper_tl = (i, float(ph), slope)   # resistance → goes down

        if not np.isnan(pl):
            slope    = get_slope_val(SLOPE_METHOD, df, i, LENGTH, SLOPE_MULT, atr_s)
            lower_tl = (i, float(pl), slope)   # support → goes up

    # تحقق من الكسر في الشمعة المكتملة الأخيرة (n-1)
    last_idx   = n - 1
    last_close = float(df['close'].iloc[last_idx])
    last_atr   = float(atr_s.iloc[last_idx])

    if np.isnan(last_atr) or last_atr <= 0:
        return None

    if allow_buy and upper_tl is not None:
        anchor_idx, anchor_val, slope = upper_tl
        if anchor_idx < last_idx - 1:
            u_val = tl_value(anchor_idx, anchor_val, slope, False, last_idx)
            if last_close > u_val:
                signal = 'BUY'
                log(f'  {pair_name}: 🟢 BUY breakout | close={last_close:.5f} > TL={u_val:.5f}')

    if allow_sell and signal is None and lower_tl is not None:
        anchor_idx, anchor_val, slope = lower_tl
        if anchor_idx < last_idx - 1:
            l_val = tl_value(anchor_idx, anchor_val, slope, True, last_idx)
            if last_close < l_val:
                signal = 'SELL'
                log(f'  {pair_name}: 🔴 SELL breakout | close={last_close:.5f} < TL={l_val:.5f}')

    if not signal:
        return None

    # SL/TP بناءً على ATR
    bid, ask, spread, cs, min_size = get_market_info(epic)
    if bid <= 0:
        return None

    entry = ask if signal == 'BUY' else bid

    if signal == 'BUY':
        sl = round(entry - SL_ATR_MULT * last_atr, 5)
        tp = round(entry + TP_ATR_MULT * last_atr, 5)
    else:
        sl = round(entry + SL_ATR_MULT * last_atr, 5)
        tp = round(entry - TP_ATR_MULT * last_atr, 5)

    sl_dist = abs(entry - sl)
    if sl_dist <= 0:
        return None

    # حساب الحجم
    if config['size_override']:
        size = config['size_override']
    else:
        risk_usd = ACCOUNT_BALANCE * RISK_PERCENT
        size     = max(round(risk_usd / sl_dist, 2), min_size)
        size     = min(size, 10.0)   # حد أقصى للأمان

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

# ================================================
# 📤 EXECUTE ORDER
# ================================================
def execute_order(sig):
    direction = sig['direction']
    epic      = sig['epic']
    body = {
        'epic':           epic,
        'direction':      direction,
        'size':           sig['size'],
        'guaranteedStop': False,
        'trailingStop':   False,
        'stopLevel':      sig['sl'],
        'profitLevel':    sig['tp'],
    }

    log(f'  📤 ORDER {sig["pair"]} {direction} | entry≈{sig["entry"]}'
        f' SL={sig["sl"]} TP={sig["tp"]} size={sig["size"]}')

    try:
        r    = requests.post(BASE_URL + '/api/v1/positions',
                             headers=session_headers, json=body, timeout=15)
        data = r.json()
        log(f'  RESP [{r.status_code}]: {json.dumps(data)[:200]}')

        if r.status_code == 200:
            deal_ref = data.get('dealReference', 'N/A')
            time.sleep(2)
            confirm  = requests.get(
                BASE_URL + '/api/v1/confirms/' + deal_ref,
                headers=session_headers, timeout=10
            ).json()
            status = confirm.get('dealStatus', 'UNKNOWN')
            reason = confirm.get('reason', '')
            tg_result(sig['pair'], direction, status, deal_ref, reason)
            return status, deal_ref
        else:
            err = data.get('errorCode', str(data)[:80])
            tg_result(sig['pair'], direction, 'FAILED', 'N/A', err)
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
    log(f'  صفقات مفتوحة: {len(open_pos)} / {MAX_OPEN_TRADES}')

    if len(open_pos) >= MAX_OPEN_TRADES:
        log('  ⏸  وصلنا الحد الأقصى للصفقات')
        return

    ts_key = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H')

    for pair_name, config in PAIRS.items():
        # فحص الخسائر المتتالية لكل زوج
        consec = db_consec_losses(pair_name)
        if consec >= MAX_CONSECUTIVE_LOSS:
            log(f'  {pair_name}: ⚠️  {consec} خسائر متتالية — تخطّي')
            continue

        # منع الإشارة المكررة في نفس الساعة
        key = f'{pair_name}_{ts_key}'
        if db_is_dup(key):
            log(f'  {pair_name}: ⏸  إشارة مكررة ({key})')
            continue

        log(f'  {pair_name}: فحص ...')
        sig = check_signal(pair_name, config)

        if sig is None:
            log(f'  {pair_name}: لا إشارة')
            continue

        # سجّل الإشارة
        db_save(key, pair_name, sig['direction'],
                sig['entry'], sig['sl'], sig['tp'],
                sig['atr'], sig['size'])

        tg_signal(pair_name, sig['direction'],
                  sig['entry'], sig['sl'], sig['tp'],
                  sig['atr'], sig['size'])

        # تنفيذ
        status, ref = execute_order(sig)
        db_update(key, status)
        log(f'  {pair_name}: {status} | {ref}')

        # لا ندخل أكثر من صفقة واحدة لكل scan إذا وصلنا الحد
        open_pos = get_open_positions()
        if len(open_pos) >= MAX_OPEN_TRADES:
            break

        time.sleep(2)   # تأخير بسيط بين الأزواج

# ================================================
# 🚀 BOT ENTRY
# ================================================
def start_bot():
    db_init()
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    nl   = '\n'

    print('=' * 55, flush=True)
    print(f'  Trendlines Breaks Bot [{mode}]', flush=True)
    print(f'  TF     : {STRATEGY_TF}', flush=True)
    print(f'  Method : {SLOPE_METHOD}', flush=True)
    print(f'  Length : {LENGTH} | Slope: {SLOPE_MULT}', flush=True)
    print(f'  SL/TP  : {SL_ATR_MULT}/{TP_ATR_MULT} × ATR', flush=True)
    print(f'  Pairs  :', flush=True)
    for pn, pc in PAIRS.items():
        b = '✅' if pc['allow_buy']  else '❌'
        s = '✅' if pc['allow_sell'] else '❌'
        print(f'    {pn}: BUY={b} SELL={s}', flush=True)
    print('=' * 55, flush=True)

    tg(
        f'🚀 *Trendlines Breaks Bot* [{mode}]{nl}'
        f'TF: `{STRATEGY_TF}` | Method: `{SLOPE_METHOD}`{nl}'
        f'Length: `{LENGTH}` | SL/TP: `{SL_ATR_MULT}/{TP_ATR_MULT}×ATR`{nl}'
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
            tg('🛑 Trendlines Breaks Bot stopped')
            break
        except Exception as ex:
            log(f'LOOP ERROR: {ex}')
            tg(f'❌ Bot Error: `{str(ex)[:100]}`')

        time.sleep(SCAN_INTERVAL)


if __name__ == '__main__':
    start_bot()
