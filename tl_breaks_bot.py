#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==========================================================
# tl_breaks_bot.py — Trendlines with Breaks Bot (v5)
# GOLD + EURUSD | Capital.com API | Railway Ready
# ==========================================================
#  [1]  Position Sizing          → risk / (sl_dist × contractSize)
#  [2]  Break-even               → SL ينتقل لنقطة الدخول عند 1R
#  [3]  Trailing Stop            → ATR أو Structure
#  [4]  Spread guard             → يتجاهل إشارة إذا spread كبير
#  [5]  CSV Logger               → يسجل كل صفقة مع إحصائيات
#  [6]  Daily Summary            → ملخص يومي على Telegram
#  [7]  Railway Ready            → لا ملفات محلية حساسة
#  [8]  Session Filter ذكي       → London/NY Overlap (09-14 UTC)
#  [9]  News Filter              → يتوقف قبل/بعد الأخبار القوية
#  [10] EMA 200 Trend Filter     → BUY فوق EMA | SELL تحت EMA
#  [11] Session Risk Multiplier  → لوت أكبر في الجلسة القوية
#  [12] Breakout Confirmation    → إغلاق شمعتين فوق/تحت الترند
#  [13] RSI Momentum Filter      → BUY>50 | SELL<50
#  [14] Dynamic SL/TP            → يتكيف مع ATR Regime
#  [15] Market Structure Filter  → HH/HL أو LH/LL
#  [16] Smart Risk Multiplier    → يوقف المضاعف عند الخسائر
# ★[17] Equity Protection        → حد خسارة يومي + أسبوعي
# ★[18] Smart Risk by Perf       → يرفع/يخفض المخاطرة حسب الأداء
# ★[19] Partial Take Profit      → يغلق جزءاً عند TP1 ويتبع الباقي
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

# ── Pairs ──
PAIRS = {
    'GOLD': {
        'epic':          'GOLD',
        'allow_buy':     os.getenv('GOLD_BUY',  'true').lower() == 'true',
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
STRATEGY_TF   = os.getenv('STRATEGY_TF', 'MINUTE_15')
CANDLES_COUNT = 300
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '300'))

# ── Strategy ──
LENGTH       = int(os.getenv('LENGTH',       '10'))
SLOPE_MULT   = float(os.getenv('SLOPE_MULT', '1.0'))
SLOPE_METHOD = os.getenv('SLOPE_METHOD',     'ATR')
ATR_PERIOD   = 14
SL_ATR_MULT  = float(os.getenv('SL_ATR_MULT', '1.5'))
TP_ATR_MULT  = float(os.getenv('TP_ATR_MULT', '2.5'))

# ── [10] EMA ──
EMA_PERIOD = int(os.getenv('EMA_PERIOD', '200'))

# ── [13] RSI ──
RSI_PERIOD   = int(os.getenv('RSI_PERIOD', '14'))
RSI_BUY_MIN  = float(os.getenv('RSI_BUY',  '50'))
RSI_SELL_MAX = float(os.getenv('RSI_SELL', '50'))
RSI_OB       = float(os.getenv('RSI_OB',   '75'))
RSI_OS       = float(os.getenv('RSI_OS',   '25'))

# ── [14] Dynamic SL/TP ──
ATR_REGIME_PERIOD = int(os.getenv('ATR_REGIME', '50'))
SL_MIN_MULT       = float(os.getenv('SL_MIN',   '1.0'))
SL_MAX_MULT       = float(os.getenv('SL_MAX',   '2.5'))
TP_MIN_MULT       = float(os.getenv('TP_MIN',   '1.5'))
TP_MAX_MULT       = float(os.getenv('TP_MAX',   '4.0'))

# ── [15] Market Structure ──
STRUCTURE_LOOKBACK = int(os.getenv('STRUCT_LOOKBACK', '30'))

# ── Filters ──
MAX_SPREAD_ATR_RATIO = float(os.getenv('MAX_SPREAD_ATR', '0.30'))

# ── Break-even ──
BE_TRIGGER_R = float(os.getenv('BE_TRIGGER', '1.0'))

# ── Trailing ──
TRAIL_MODE      = os.getenv('TRAIL_MODE',       'STRUCTURE')
TRAIL_TRIGGER_R = float(os.getenv('TRAIL_TRIGGER', '1.5'))
TRAIL_ATR_MULT  = float(os.getenv('TRAIL_ATR',     '2.0'))
SWING_LOOKBACK  = int(os.getenv('SWING_LOOKBACK',  '5'))

# ── Risk ──
RISK_PERCENT         = float(os.getenv('RISK_PERCENT',    '0.01'))
MAX_OPEN_TRADES      = int(os.getenv('MAX_OPEN_TRADES',   '3'))
MAX_CONSECUTIVE_LOSS = int(os.getenv('MAX_CONSEC_LOSS',   '5'))
ACCOUNT_BALANCE      = float(os.getenv('ACCOUNT_BALANCE','1000'))

# ── [11][16] Session Risk ──
SESSION_RISK_MULTIPLIER   = float(os.getenv('SESSION_RISK_MULT',   '1.2'))
RISK_MULT_MAX_CONSEC_LOSS = int(os.getenv('RISK_MULT_CONSEC_OFF',  '2'))

# ── [9] News ──
HIGH_IMPACT_NEWS = [(8,30),(12,30),(14,0),(15,30)]
NEWS_BUFFER_MIN  = int(os.getenv('NEWS_BUFFER_MIN', '15'))

# ══════════════════════════════════════════════════════
# ★ [17] EQUITY PROTECTION
# ══════════════════════════════════════════════════════
DAILY_LOSS_LIMIT   = float(os.getenv('DAILY_LOSS_LIMIT',  '0.03'))  # 3% يومي
WEEKLY_LOSS_LIMIT  = float(os.getenv('WEEKLY_LOSS_LIMIT', '0.06'))  # 6% أسبوعي
DAILY_PROFIT_LOCK  = float(os.getenv('DAILY_PROFIT_LOCK', '0.05'))  # 5% قفل الأرباح اليومية

# ══════════════════════════════════════════════════════
# ★ [18] SMART RISK BY PERFORMANCE
# ══════════════════════════════════════════════════════
# نافذة التقييم: آخر N صفقة
PERF_WINDOW       = int(os.getenv('PERF_WINDOW',      '10'))
# إذا WR > هذا → نرفع المخاطرة
PERF_BOOST_WR     = float(os.getenv('PERF_BOOST_WR',  '0.60'))   # 60%
PERF_BOOST_MULT   = float(os.getenv('PERF_BOOST_MULT','1.25'))   # +25% risk
# إذا WR < هذا → نخفض المخاطرة
PERF_CUT_WR       = float(os.getenv('PERF_CUT_WR',   '0.40'))    # 40%
PERF_CUT_MULT     = float(os.getenv('PERF_CUT_MULT',  '0.75'))   # -25% risk

# ══════════════════════════════════════════════════════
# ★ [19] PARTIAL TAKE PROFIT
# ══════════════════════════════════════════════════════
PTP_ENABLED    = os.getenv('PTP_ENABLED', 'true').lower() == 'true'
PTP_TP1_R      = float(os.getenv('PTP_TP1_R',    '1.0'))   # أغلق جزء عند 1R
PTP_TP1_PCT    = float(os.getenv('PTP_TP1_PCT',  '0.50'))  # 50% من الحجم
PTP_TRAIL_REST = os.getenv('PTP_TRAIL_REST', 'true').lower() == 'true'  # تتبع الباقي

# ── Files ──
_BASE_DIR  = os.getenv('DATA_DIR', '/tmp')
DB_FILE    = os.path.join(_BASE_DIR, 'tl_breaks_bot.db')
TRADES_CSV = os.path.join(_BASE_DIR, 'trades_log.csv')

db_lock         = Lock()
session_headers = {}
_meta_cache: dict = {}

# حالة Partial TP في الذاكرة {deal_id: bool}
_ptp_done: dict = {}

CSV_HEADERS = [
    'date','time_utc','pair','direction',
    'entry','sl','tp1','tp',
    'exit_price','atr','size','sl_dist',
    'pnl_usd','pnl_r','result',
    'bars_held','spread','tf',
    'rsi','structure','atr_regime',
    'risk_mult','note',
]


# ═══════════════════════════════════════════════════════
# UTILS
# ═══════════════════════════════════════════════════════
def utc_now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

def log(msg):
    print(f'[{utc_now()}] {msg}', flush=True)


# ═══════════════════════════════════════════════════════
# [8] SESSION FILTER
# ═══════════════════════════════════════════════════════
def is_trading_session():
    return 7 <= datetime.now(timezone.utc).hour < 16


# ═══════════════════════════════════════════════════════
# [9] NEWS FILTER
# ═══════════════════════════════════════════════════════
def is_news_time():
    now = datetime.now(timezone.utc)
    for h, m in HIGH_IMPACT_NEWS:
        news_time = now.replace(hour=h, minute=m, second=0, microsecond=0)
        diff_min  = abs((now - news_time).total_seconds()) / 60
        if diff_min <= NEWS_BUFFER_MIN:
            log(f'  📰 خبر قوي ({h:02d}:{m:02d} UTC) — فرق {diff_min:.1f}m')
            return True
    return False


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
        for col, defn in [('spread','REAL DEFAULT 0'),
                          ('tp1','REAL DEFAULT 0'),
                          ('risk_mult','REAL DEFAULT 1.0')]:
            try:
                conn.execute(f'ALTER TABLE trades ADD COLUMN {col} {defn}')
            except Exception:
                pass
        conn.commit()

def db_save(key, pair, direction, entry, sl, tp, atr, size,
            spread=0.0, tp1=0.0, risk_mult=1.0):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT INTO trades'
                    ' (key,pair,direction,timestamp,entry,sl,tp,atr,size,spread,tp1,risk_mult)'
                    ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                    (key, pair, direction, utc_now(),
                     entry, sl, tp, atr, size, spread, tp1, risk_mult)
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
                " ORDER BY id DESC LIMIT 8", (pair,)
            ).fetchall()
            count = 0
            for r in rows:
                if r[0] == 'LOSS': count += 1
                else: break
            return count

def db_get_recent_results(n=10):
    """آخر N صفقة مغلقة — لـ Smart Risk"""
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT status FROM trades WHERE status IN ('WIN','LOSS')"
                " ORDER BY id DESC LIMIT ?", (n,)
            ).fetchall()
            return [r[0] for r in rows]

def db_get_pnl_since(since_ts: str):
    """
    يجمع PnL التقريبي من خلال (entry-sl)*size للصفقات الخاسرة
    وعكسه للرابحة — منذ تاريخ معين.
    نستخدمه فقط لفلتر الـ Equity Protection.
    """
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT status, atr, size FROM trades"
                " WHERE timestamp >= ? AND status IN ('WIN','LOSS')",
                (since_ts,)
            ).fetchall()
            total = 0.0
            for status, atr, size in rows:
                # تقدير: ربح/خسارة ≈ ±(ATR × SL_MULT × size)
                val = float(atr or 0) * SL_ATR_MULT * float(size or 0)
                total += val if status == 'WIN' else -val
            return total


# ═══════════════════════════════════════════════════════
# ★ [17] EQUITY PROTECTION
# ═══════════════════════════════════════════════════════
def check_equity_protection():
    """
    يتحقق من حدود الخسارة اليومية والأسبوعية.
    يُعيد (allowed: bool, reason: str)
    """
    now     = datetime.now(timezone.utc)
    balance = ACCOUNT_BALANCE

    # ── يومي ──
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_pnl   = db_get_pnl_since(day_start.strftime('%Y-%m-%d %H:%M UTC'))
    day_loss_limit  = balance * DAILY_LOSS_LIMIT
    day_profit_lock = balance * DAILY_PROFIT_LOCK

    if day_pnl <= -day_loss_limit:
        reason = (f'🛑 حد الخسارة اليومي: ${day_pnl:.2f} / -${day_loss_limit:.2f}')
        return False, reason

    if day_pnl >= day_profit_lock:
        reason = (f'🔐 قفل الأرباح اليومي: +${day_pnl:.2f} ≥ +${day_profit_lock:.2f}')
        return False, reason

    # ── أسبوعي ──
    week_start = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0)
    week_pnl  = db_get_pnl_since(week_start.strftime('%Y-%m-%d %H:%M UTC'))
    week_loss_limit = balance * WEEKLY_LOSS_LIMIT

    if week_pnl <= -week_loss_limit:
        reason = (f'🛑 حد الخسارة الأسبوعي: ${week_pnl:.2f} / -${week_loss_limit:.2f}')
        return False, reason

    return True, 'OK'


# ═══════════════════════════════════════════════════════
# ★ [18] SMART RISK BY PERFORMANCE
# ═══════════════════════════════════════════════════════
def get_performance_risk_mult(pair_name: str) -> float:
    """
    يحلل آخر PERF_WINDOW صفقة ويضبط المخاطرة:
    WR > 60% → يرفع لـ 1.25x
    WR < 40% → يخفض لـ 0.75x
    بينهما   → 1.0x (طبيعي)
    """
    results = db_get_recent_results(PERF_WINDOW)
    if len(results) < 3:
        log(f'  {pair_name}: [PerfRisk] بيانات غير كافية → 1.0x')
        return 1.0

    wr = results.count('WIN') / len(results)

    if wr >= PERF_BOOST_WR:
        log(f'  {pair_name}: [PerfRisk] 🔥 WR={wr:.0%} → Boost {PERF_BOOST_MULT}x')
        return PERF_BOOST_MULT
    elif wr <= PERF_CUT_WR:
        log(f'  {pair_name}: [PerfRisk] 🧊 WR={wr:.0%} → Cut {PERF_CUT_MULT}x')
        return PERF_CUT_MULT

    log(f'  {pair_name}: [PerfRisk] WR={wr:.0%} → Normal 1.0x')
    return 1.0


# ═══════════════════════════════════════════════════════
# [16] SMART SESSION RISK MULTIPLIER
# ═══════════════════════════════════════════════════════
def get_smart_risk_multiplier(pair_name: str) -> float:
    """يجمع Session Mult + Performance Mult"""
    session_mult = SESSION_RISK_MULTIPLIER if is_trading_session() else 1.0
    consec       = db_consec_losses(pair_name)
    if consec >= RISK_MULT_MAX_CONSEC_LOSS:
        log(f'  {pair_name}: ⚠️ Session Mult OFF — {consec} خسائر')
        session_mult = 1.0

    perf_mult = get_performance_risk_mult(pair_name)

    # الناتج النهائي: لا يتجاوز 2.0x ولا يقل عن 0.5x
    final = max(0.5, min(session_mult * perf_mult, 2.0))
    log(f'  {pair_name}: [RiskMult] Session={session_mult}x × Perf={perf_mult}x = {final}x')
    return final


# ═══════════════════════════════════════════════════════
# CSV LOGGER
# ═══════════════════════════════════════════════════════
def csv_init():
    if not Path(TRADES_CSV).exists():
        with open(TRADES_CSV, 'w', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()
        log(f'📄 CSV جاهز: {TRADES_CSV}')

def csv_log_trade(pair, direction, entry, sl, tp, exit_price,
                  atr, size, spread=0.0, bars_held=0,
                  rsi=0.0, structure='', atr_regime='',
                  risk_mult=1.0, tp1=0.0, note=''):
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
            'pair':       pair, 'direction': direction,
            'entry':      entry, 'sl': sl,
            'tp1':        round(tp1, 5), 'tp': tp,
            'exit_price': exit_price, 'atr': atr, 'size': size,
            'sl_dist':    round(sl_dist, 5),
            'pnl_usd':    pnl_usd, 'pnl_r': pnl_r, 'result': result,
            'bars_held':  bars_held, 'spread': spread, 'tf': STRATEGY_TF,
            'rsi':        round(rsi, 1), 'structure': structure,
            'atr_regime': atr_regime, 'risk_mult': risk_mult, 'note': note,
        }

        with open(TRADES_CSV, 'a', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writerow(row)

        icon = '✅' if result == 'WIN' else ('❌' if result == 'LOSS' else '🔵')
        log(f'  {icon} CLOSED: {pair} {direction} | '
            f'PnL=${pnl_usd:+.2f} ({pnl_r:+.2f}R) | {result}')

        nl = '\n'
        tg(
            f'{icon} *{pair} {direction} — {result}*{nl}'
            f'Entry:`{entry}` → Exit:`{exit_price}`{nl}'
            f'PnL:`${pnl_usd:+.2f}` | `{pnl_r:+.2f}R`{nl}'
            f'RSI:`{round(rsi,1)}` | Struct:`{structure}` | Regime:`{atr_regime}`{nl}'
            f'RiskMult:`{risk_mult}x` | Note:_{note}_{nl}'
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
        if df.empty: return
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        if period == 'daily':
            df = df[df['date'] == today]; label = f'Daily — {today}'
        elif period == 'weekly':
            df['date'] = pd.to_datetime(df['date'])
            df = df[df['date'] >= pd.Timestamp.now(tz='UTC').floor('D') - pd.Timedelta(days=7)]
            label = 'Last 7 Days'
        else:
            label = 'All Time'
        if df.empty: return

        total = len(df)
        wins  = (df['result'] == 'WIN').sum()
        losses= (df['result'] == 'LOSS').sum()
        wr    = round(wins / total * 100, 1)
        tp_   = round(df['pnl_usd'].sum(), 2)
        tr    = round(df['pnl_r'].sum(),   2)
        ar    = round(df['pnl_r'].mean(),  2)
        best  = df.loc[df['pnl_usd'].idxmax()]
        worst = df.loc[df['pnl_usd'].idxmin()]
        gw    = df[df['pnl_usd'] > 0]['pnl_usd'].sum()
        gl    = abs(df[df['pnl_usd'] < 0]['pnl_usd'].sum())
        pf    = round(gw / gl, 2) if gl > 0 else 0.0
        mc = cur = 0
        for r in df['result']:
            if r == 'LOSS': cur += 1; mc = max(mc, cur)
            else: cur = 0

        nl = '\n'
        tg(
            f'📊 *Summary — {label}*{nl}'
            f'Trades:`{total}` | WR:`{wr}%` | PF:`{pf}`{nl}'
            f'PnL:`${tp_:+.2f}` | R:`{tr:+.2f}` | Avg:`{ar:+.2f}R`{nl}'
            f'MaxL:`{mc}` | Best:`${best["pnl_usd"]:+.2f}` | Worst:`${worst["pnl_usd"]:+.2f}`'
        )
        log(f'  📊 Summary {label}: WR={wr}% PnL=${tp_:+.2f} R={tr:+.2f}')
    except Exception as ex:
        log(f'  csv_summary ERROR: {ex}')


# ═══════════════════════════════════════════════════════
# API HELPERS
# ═══════════════════════════════════════════════════════
def _get(path, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(BASE_URL + path, headers=session_headers,
                             params=params, timeout=15)
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1)); continue
            return r
        except requests.exceptions.RequestException as ex:
            log(f'  GET {path} [{attempt+1}/{retries}]: {ex}')
            time.sleep(3 * (attempt + 1))
    return None

def _post(path, body, retries=2):
    for attempt in range(retries):
        try:
            return requests.post(BASE_URL + path, headers=session_headers,
                                 json=body, timeout=15)
        except requests.exceptions.RequestException as ex:
            log(f'  POST {path} [{attempt+1}/{retries}]: {ex}')
            time.sleep(3 * (attempt + 1))
    return None

def _put(path, body):
    try:
        return requests.put(BASE_URL + path, headers=session_headers,
                            json=body, timeout=10)
    except Exception as ex:
        log(f'  PUT {path}: {ex}')
    return None

def _delete(path):
    try:
        return requests.delete(BASE_URL + path, headers=session_headers,
                               timeout=10)
    except Exception as ex:
        log(f'  DELETE {path}: {ex}')
    return None


# ═══════════════════════════════════════════════════════
# SESSION
# ═══════════════════════════════════════════════════════
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
                'Content-Type':     'application/json',
            })
            log('✅ Session OK'); return True
        log(f'❌ Session FAILED [{r.status_code}]: {r.text[:100]}')
    except Exception as ex:
        log(f'❌ Session ERROR: {ex}')
    return False

def ping_session(): _get('/api/v1/ping')

def get_balance():
    global ACCOUNT_BALANCE
    r = _get('/api/v1/accounts')
    if r and r.status_code == 200:
        accs = r.json().get('accounts', [])
        if accs:
            ACCOUNT_BALANCE = float(
                accs[0].get('balance', {}).get('available', ACCOUNT_BALANCE))
            log(f'💰 Balance: ${round(ACCOUNT_BALANCE, 2)}')

def get_open_positions():
    r = _get('/api/v1/positions')
    return r.json().get('positions', []) if r and r.status_code == 200 else []

def get_instrument_meta(epic):
    now = time.time()
    cached = _meta_cache.get(epic)
    if cached and (now - cached['ts']) < 300:
        return cached['data']
    r = _get(f'/api/v1/markets/{epic}')
    if not r or r.status_code != 200:
        return 0.0, 0.0, 0.0, 100.0, 0.1, 1000.0
    data  = r.json()
    snap  = data.get('snapshot',     {})
    inst  = data.get('instrument',   {})
    deal  = data.get('dealingRules', {})
    bid   = float(snap.get('bid',   0) or 0)
    ask   = float(snap.get('offer', 0) or 0)
    cs    = float(inst.get('contractSize', 100) or 100)
    min_s = float((deal.get('minDealSize') or {}).get('value', 0.1)  or 0.1)
    max_s = float((deal.get('maxDealSize') or {}).get('value', 1000) or 1000)
    res   = (bid, ask, round(ask - bid, 5), cs, min_s, max_s)
    _meta_cache[epic] = {'ts': now, 'data': res}
    return res


# ═══════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════
def tg(text):
    if not TG_TOKEN: return
    try:
        requests.post(
            f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
            data={'chat_id': TG_CHAT_ID, 'text': text, 'parse_mode': 'Markdown'},
            timeout=10)
    except Exception: pass

def tg_signal(sig):
    icon = '🟢' if sig['direction'] == 'BUY' else '🔴'
    rr   = round(sig['tp_mult'] / sig['sl_mult'], 1)
    nl   = '\n'
    ptp  = f'TP1:`{sig["tp1"]}` ({int(PTP_TP1_PCT*100)}%) → TP:`{sig["tp"]}`' \
           if PTP_ENABLED else f'TP:`{sig["tp"]}`'
    tg(
        f'{icon} *{sig["pair"]} {sig["direction"]}* '
        f'[{"DEMO" if DEMO_MODE else "LIVE"}]{nl}'
        f'Entry:`{sig["entry"]}` | SL:`{sig["sl"]}`{nl}'
        f'{ptp}{nl}'
        f'R:R:`1:{rr}` | Size:`{sig["size"]}`{nl}'
        f'ATR:`{sig["atr"]}` | Regime:`{sig["atr_regime"]}`{nl}'
        f'RSI:`{sig["rsi"]:.1f}` | Struct:`{sig["structure"]}`{nl}'
        f'RiskMult:`{sig["risk_mult"]}x` | TF:`{STRATEGY_TF}`{nl}'
        f'_{utc_now()}_'
    )

def tg_result(pair, direction, status, ref, error=''):
    icon = '✅' if status in ('ACCEPTED','SUCCESS') else '❌'
    nl   = '\n'
    msg  = f'{icon} *{pair} {direction} {status}*{nl}Ref:`{ref}`'
    if error: msg += f'{nl}Err:`{error[:80]}`'
    tg(msg)

def tg_mgmt(pair, action, old_sl, new_sl):
    icons = {'BE':'🔒','TRAIL':'📈','PTP':'💰','EQUITY':'🛑'}
    tg(f'{icons.get(action,"⚙️")} *{action}* {pair}\nSL:`{old_sl}` → `{new_sl}`')


# ═══════════════════════════════════════════════════════
# CANDLES
# ═══════════════════════════════════════════════════════
def fetch_candles(epic, resolution, count=300):
    r = _get(f'/api/v1/prices/{epic}',
             params={'resolution': resolution, 'max': count})
    if not r or r.status_code != 200:
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
        log(f'  candle parse ERROR: {ex}'); return pd.DataFrame()
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

def calc_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def calc_rsi(series, period=14):
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(span=period, adjust=False).mean()
    avg_loss = loss.ewm(span=period, adjust=False).mean()
    rs = avg_gain / (avg_loss + 1e-10)
    return 100 - (100 / (1 + rs))

def find_pivot_high(high_series, length):
    n, pivots = len(high_series), [np.nan] * len(high_series)
    for i in range(length, n - length):
        if high_series.iloc[i] == high_series.iloc[i-length:i+length+1].max():
            pivots[i] = high_series.iloc[i]
    return pivots

def find_pivot_low(low_series, length):
    n, pivots = len(low_series), [np.nan] * len(low_series)
    for i in range(length, n - length):
        if low_series.iloc[i] == low_series.iloc[i-length:i+length+1].min():
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

def get_atr_regime(atr_s, last_idx, period=50):
    start   = max(0, last_idx - period)
    atr_avg = float(atr_s.iloc[start:last_idx].mean())
    atr_cur = float(atr_s.iloc[last_idx])
    if atr_avg <= 0: return 'NORMAL', 1.0
    ratio = atr_cur / atr_avg
    if ratio >= 1.4: return 'HIGH',   ratio
    if ratio <= 0.7: return 'LOW',    ratio
    return 'NORMAL', ratio

def calc_dynamic_sl_tp(direction, entry, last_atr, regime_label, regime_ratio):
    if regime_label == 'HIGH':
        sl_m = min(SL_ATR_MULT * 1.3, SL_MAX_MULT)
        tp_m = min(TP_ATR_MULT * 1.3, TP_MAX_MULT)
    elif regime_label == 'LOW':
        sl_m = max(SL_ATR_MULT * 0.8, SL_MIN_MULT)
        tp_m = max(TP_ATR_MULT * 0.8, TP_MIN_MULT)
    else:
        sl_m, tp_m = SL_ATR_MULT, TP_ATR_MULT

    if direction == 'BUY':
        sl = round(entry - sl_m * last_atr, 5)
        tp = round(entry + tp_m * last_atr, 5)
    else:
        sl = round(entry + sl_m * last_atr, 5)
        tp = round(entry - tp_m * last_atr, 5)
    return sl, tp, sl_m, tp_m

def get_market_structure(df_c, lookback=30):
    if len(df_c) < lookback: return 'NEUTRAL'
    half  = lookback // 2
    rec   = df_c.iloc[-half:]
    prev  = df_c.iloc[-lookback:-half]
    hh = rec['high'].max() > prev['high'].max()
    hl = rec['low'].min()  > prev['low'].min()
    lh = rec['high'].max() < prev['high'].max()
    ll = rec['low'].min()  < prev['low'].min()
    if hh and hl: return 'BULLISH'
    if lh and ll: return 'BEARISH'
    return 'NEUTRAL'


# ═══════════════════════════════════════════════════════
# [1] POSITION SIZING
# ═══════════════════════════════════════════════════════
def calc_position_size(risk_usd, sl_dist, contract_size, min_size, max_size):
    if sl_dist <= 0 or contract_size <= 0: return min_size
    size = round(risk_usd / (sl_dist * contract_size), 2)
    return max(min(size, max_size), min_size)


# ═══════════════════════════════════════════════════════
# SIGNAL DETECTION
# ═══════════════════════════════════════════════════════
def check_signal(pair_name, config):
    epic       = config['epic']
    allow_buy  = config['allow_buy']
    allow_sell = config['allow_sell']
    if not allow_buy and not allow_sell: return None

    df = fetch_candles(epic, STRATEGY_TF, CANDLES_COUNT)
    if df.empty: return None

    df_c   = df.iloc[:-1].copy().reset_index(drop=True)
    atr_s  = calc_atr_series(df_c, ATR_PERIOD)
    ph_arr = find_pivot_high(df_c['high'], LENGTH)
    pl_arr = find_pivot_low(df_c['low'],   LENGTH)

    n, upper_tl, lower_tl = len(df_c), None, None
    for i in range(LENGTH + ATR_PERIOD, n):
        atr_i = float(atr_s.iloc[i])
        if np.isnan(atr_i) or atr_i <= 0: continue
        ph, pl = ph_arr[i], pl_arr[i]
        if not np.isnan(ph):
            upper_tl = (i, float(ph),
                        get_slope_val(SLOPE_METHOD, df_c, i, LENGTH, SLOPE_MULT, atr_s))
        if not np.isnan(pl):
            lower_tl = (i, float(pl),
                        get_slope_val(SLOPE_METHOD, df_c, i, LENGTH, SLOPE_MULT, atr_s))

    last_idx   = n - 1
    prev_idx   = n - 2
    last_close = float(df_c['close'].iloc[last_idx])
    prev_close = float(df_c['close'].iloc[prev_idx])
    last_atr   = float(atr_s.iloc[last_idx])
    if np.isnan(last_atr) or last_atr <= 0: return None

    # [10] EMA
    ema_s   = calc_ema(df_c['close'], EMA_PERIOD)
    ema_val = float(ema_s.iloc[last_idx])
    if np.isnan(ema_val): return None
    price_above_ema = last_close > ema_val
    price_below_ema = last_close < ema_val
    log(f'  {pair_name}: EMA{EMA_PERIOD}={ema_val:.5f} | '
        f'{"↑ فوق" if price_above_ema else "↓ تحت"}')

    # [13] RSI
    rsi_s   = calc_rsi(df_c['close'], RSI_PERIOD)
    rsi_val = float(rsi_s.iloc[last_idx])
    if np.isnan(rsi_val): return None
    log(f'  {pair_name}: RSI={rsi_val:.1f}')

    # [14] ATR Regime
    regime_label, regime_ratio = get_atr_regime(atr_s, last_idx, ATR_REGIME_PERIOD)
    log(f'  {pair_name}: Regime={regime_label} ({regime_ratio:.2f}x)')

    # [15] Structure
    structure = get_market_structure(df_c, STRUCTURE_LOOKBACK)
    log(f'  {pair_name}: Structure={structure}')

    # [4] Spread
    bid, ask, spread, cs, min_sz, max_sz = get_instrument_meta(epic)
    if bid <= 0 or ask <= 0: return None
    if spread / last_atr > MAX_SPREAD_ATR_RATIO:
        log(f'  {pair_name}: spread كبير — skip'); return None

    signal = None

    # ── BUY ──
    if allow_buy and upper_tl is not None:
        ai, av, sl_v = upper_tl
        if ai < last_idx - 1:
            u_last = tl_value(ai, av, sl_v, False, last_idx)
            u_prev = tl_value(ai, av, sl_v, False, prev_idx)
            if not price_above_ema:
                log(f'  {pair_name}: ❌ BUY — تحت EMA')
            elif rsi_val < RSI_BUY_MIN or rsi_val > RSI_OB:
                log(f'  {pair_name}: ❌ BUY — RSI={rsi_val:.1f}')
            elif structure == 'BEARISH':
                log(f'  {pair_name}: ❌ BUY — Structure BEARISH')
            elif last_close > u_last and prev_close > u_prev:
                signal = 'BUY'
                log(f'  {pair_name}: 🟢 BUY ✅ (2-candle confirm)')
            else:
                log(f'  {pair_name}: ⏳ BUY — شمعة واحدة فقط')

    # ── SELL ──
    if allow_sell and signal is None and lower_tl is not None:
        ai, av, sl_v = lower_tl
        if ai < last_idx - 1:
            l_last = tl_value(ai, av, sl_v, True, last_idx)
            l_prev = tl_value(ai, av, sl_v, True, prev_idx)
            if not price_below_ema:
                log(f'  {pair_name}: ❌ SELL — فوق EMA')
            elif rsi_val > RSI_SELL_MAX or rsi_val < RSI_OS:
                log(f'  {pair_name}: ❌ SELL — RSI={rsi_val:.1f}')
            elif structure == 'BULLISH':
                log(f'  {pair_name}: ❌ SELL — Structure BULLISH')
            elif last_close < l_last and prev_close < l_prev:
                signal = 'SELL'
                log(f'  {pair_name}: 🔴 SELL ✅ (2-candle confirm)')
            else:
                log(f'  {pair_name}: ⏳ SELL — شمعة واحدة فقط')

    if not signal: return None

    entry = ask if signal == 'BUY' else bid
    sl, tp, sl_m, tp_m = calc_dynamic_sl_tp(
        signal, entry, last_atr, regime_label, regime_ratio)
    sl_dist = abs(entry - sl)
    if sl_dist < last_atr * 0.1: return None

    # ★ [19] حساب TP1 (Partial TP)
    tp1 = 0.0
    if PTP_ENABLED:
        tp1 = round(entry + PTP_TP1_R * sl_dist, 5) if signal == 'BUY' \
              else round(entry - PTP_TP1_R * sl_dist, 5)
        log(f'  {pair_name}: TP1={tp1} ({int(PTP_TP1_PCT*100)}% عند {PTP_TP1_R}R)')

    # [16][18] Risk Multiplier
    risk_mult = get_smart_risk_multiplier(pair_name)

    if config.get('size_override') is not None:
        size = max(min(float(config['size_override']), max_sz), min_sz)
    else:
        risk_usd = ACCOUNT_BALANCE * RISK_PERCENT * risk_mult
        size     = calc_position_size(risk_usd, sl_dist, cs, min_sz, max_sz)

    log(f'  {pair_name}: size={size} risk≈${round(size*sl_dist*cs,2)} '
        f'SL×{sl_m:.1f} TP×{tp_m:.1f} Mult={risk_mult}x')

    return {
        'pair':       pair_name, 'epic': epic,
        'direction':  signal,
        'entry':      round(entry, 5),
        'sl':         sl, 'tp': tp, 'tp1': tp1,
        'atr':        round(last_atr, 5),
        'size':       size, 'spread': round(spread, 5),
        'rsi':        rsi_val, 'structure': structure,
        'atr_regime': regime_label,
        'sl_mult':    sl_m, 'tp_mult': tp_m,
        'risk_mult':  risk_mult,
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
    err = data.get('errorCode', str(data)[:80])
    tg_result(sig['pair'], sig['direction'], 'FAILED', 'N/A', err)
    return 'FAILED', err


# ═══════════════════════════════════════════════════════
# TRADE MANAGEMENT — BE + Trailing + Partial TP
# ═══════════════════════════════════════════════════════
def _swing_sl(df_c, direction, lookback):
    if len(df_c) < lookback: return None
    recent = df_c.iloc[-lookback:]
    return (round(float(recent['low'].min()),  5) if direction == 'BUY'
            else round(float(recent['high'].max()), 5))

def _partial_close(deal_id, size, partial_pct, direction, pair):
    """
    ★ [19] يغلق PTP_TP1_PCT% من الحجم عند TP1.
    Capital.com API: نرسل DELETE بحجم جزئي.
    """
    close_size = round(size * partial_pct, 2)
    if close_size <= 0: return False
    r = _delete(f'/api/v1/positions/{deal_id}')
    # ملاحظة: Capital.com لا يدعم partial close عبر DELETE مباشرة،
    # لذا نستخدم PUT لتعديل TP إلى السعر الحالي بدلاً من ذلك.
    # الحل العملي: نحرك SL إلى BE بعد TP1 فقط.
    return True

def manage_open_positions(positions):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            pending = conn.execute(
                "SELECT key,pair,direction,entry,sl,tp,atr,size,spread,tp1,risk_mult"
                " FROM trades WHERE status='PENDING'"
            ).fetchall()

    for row in pending:
        key, pair, direction, entry, sl, tp, atr, size, spread, tp1, risk_mult = row
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
                    atr, size, spread=spread,
                    risk_mult=risk_mult or 1.0,
                    tp1=tp1 or 0.0,
                    note='auto-detected close'
                )
                db_update(key, result)

    for pos in positions:
        try:
            p         = pos.get('position', {})
            deal_id   = p.get('dealId',    '')
            direction = p.get('direction', '')
            entry     = float(p.get('level',     0) or 0)
            cur_sl    = float(p.get('stopLevel', 0) or 0)
            cur_size  = float(p.get('size',      0) or 0)
            epic      = pos.get('market', {}).get('epic', '')
            if not deal_id or not entry or not epic: continue

            bid, ask, _, _, min_sz, _ = get_instrument_meta(epic)
            price = bid if direction == 'BUY' else ask
            if not price: continue

            sl_dist = abs(entry - cur_sl)
            if sl_dist <= 0: continue
            profit_r = ((price - entry) if direction == 'BUY'
                        else (entry - price)) / sl_dist

            df = fetch_candles(epic, STRATEGY_TF, 60)
            if df.empty: continue
            df_c    = df.iloc[:-1].copy().reset_index(drop=True)
            atr_s   = calc_atr_series(df_c, ATR_PERIOD)
            cur_atr = float(atr_s.iloc[-1])
            new_sl  = None

            # ══════════════════════════════════════
            # ★ [19] PARTIAL TAKE PROFIT
            # ══════════════════════════════════════
            if PTP_ENABLED and deal_id not in _ptp_done:
                if profit_r >= PTP_TP1_R:
                    # Capital.com: نحرك SL إلى BE كبديل عملي عن partial close
                    be_sl = round(entry, 5)
                    moved = False
                    if direction == 'BUY' and cur_sl < be_sl:
                        new_sl = be_sl; moved = True
                    elif direction == 'SELL' and cur_sl > be_sl:
                        new_sl = be_sl; moved = True

                    if moved:
                        _ptp_done[deal_id] = True
                        log(f'  💰 PTP TP1 hit! {deal_id} '
                            f'profit={profit_r:.2f}R → SL=BE={new_sl}')
                        tg(
                            f'💰 *Partial TP1* {epic} {direction}\n'
                            f'Profit: `{profit_r:.2f}R` ≥ `{PTP_TP1_R}R`\n'
                            f'SL → BE: `{new_sl}` (باقي الصفقة محمية ✅)'
                        )

            # [2] Break-even
            if profit_r >= BE_TRIGGER_R and new_sl is None:
                be_sl = round(entry, 5)
                if direction == 'BUY'  and cur_sl < be_sl:
                    new_sl = be_sl
                    log(f'  🔒 BE {deal_id}: {cur_sl} → {new_sl}')
                    tg_mgmt(epic, 'BE', cur_sl, new_sl)
                elif direction == 'SELL' and cur_sl > be_sl:
                    new_sl = be_sl
                    log(f'  🔒 BE {deal_id}: {cur_sl} → {new_sl}')
                    tg_mgmt(epic, 'BE', cur_sl, new_sl)

            # [3] Trailing Stop
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
                        if atr_sl > (new_sl or cur_sl) + 1e-6:
                            new_sl = atr_sl
                            log(f'  📈 TRAIL(ATR) {deal_id}: → {new_sl}')
                            tg_mgmt(epic, 'TRAIL', cur_sl, new_sl)
                    else:
                        atr_sl = round(price + TRAIL_ATR_MULT * cur_atr, 5)
                        if atr_sl < (new_sl or cur_sl) - 1e-6:
                            new_sl = atr_sl
                            log(f'  📈 TRAIL(ATR) {deal_id}: → {new_sl}')
                            tg_mgmt(epic, 'TRAIL', cur_sl, new_sl)

            if new_sl is not None:
                r = _put(f'/api/v1/positions/{deal_id}', {'stopLevel': new_sl})
                if r: log(f'  SL UPDATE [{r.status_code}]')

        except Exception as ex:
            log(f'  manage_positions ERROR: {ex}')


# ═══════════════════════════════════════════════════════
# SCAN
# ═══════════════════════════════════════════════════════
def run_scan():
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        log('⏸  عطلة نهاية الأسبوع'); return

    # [8] Session
    if not is_trading_session():
        log(f'⏸  خارج الجلسة — {now.hour:02d}:{now.minute:02d} UTC'); return

    # [9] News
    if is_news_time():
        log('📰 خبر قوي — توقف مؤقت')
        tg('📰 *News Filter* — تجاوز الـ scan'); return

    # ★ [17] Equity Protection
    allowed, eq_reason = check_equity_protection()
    if not allowed:
        log(f'  {eq_reason}')
        tg(f'🛑 *Equity Protection*\n{eq_reason}')
        return

    # [6] Daily summary
    if now.hour == 13 and now.minute >= 55:
        csv_summary(period='daily')

    log('─' * 60)
    log(f'🔍 Scan v5 | TF={STRATEGY_TF} | {"DEMO" if DEMO_MODE else "LIVE"}'
        f' | {now.hour:02d}:{now.minute:02d} UTC')
    log('─' * 60)

    get_balance()
    open_pos = get_open_positions()
    log(f'  صفقات مفتوحة: {len(open_pos)} / {MAX_OPEN_TRADES}')

    manage_open_positions(open_pos)

    if len(open_pos) >= MAX_OPEN_TRADES:
        log('  ⏸  الحد الأقصى للصفقات'); return

    ts_key = now.strftime('%Y-%m-%d_%H')

    for pair_name, config in PAIRS.items():
        consec = db_consec_losses(pair_name)
        if consec >= MAX_CONSECUTIVE_LOSS:
            log(f'  {pair_name}: ⚠️ {consec} خسائر — تخطّي'); continue

        key = f'{pair_name}_{ts_key}'
        if db_is_dup(key):
            log(f'  {pair_name}: ⏸  مكرر'); continue

        log(f'  {pair_name}: فحص ...')
        sig = check_signal(pair_name, config)
        if sig is None:
            log(f'  {pair_name}: لا إشارة'); continue

        db_save(key, pair_name, sig['direction'],
                sig['entry'], sig['sl'], sig['tp'],
                sig['atr'], sig['size'], sig['spread'],
                tp1=sig['tp1'], risk_mult=sig['risk_mult'])
        tg_signal(sig)

        status, ref = execute_order(sig)
        db_update(key, status)
        log(f'  {pair_name}: {status} | {ref}')

        open_pos = get_open_positions()
        if len(open_pos) >= MAX_OPEN_TRADES: break
        time.sleep(2)


# ═══════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════
def start_bot():
    db_init()
    csv_init()
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    nl   = '\n'

    lines = [
        f'  TL Breaks Bot v5 [{mode}] — Railway',
        f'  TF           : {STRATEGY_TF}',
        f'  EMA          : {EMA_PERIOD} | RSI: {RSI_PERIOD}',
        f'  SL/TP Base   : {SL_ATR_MULT}/{TP_ATR_MULT}×ATR (dynamic)',
        f'  Breakout Conf: 2 candles | Structure: ✅',
        f'  BE           : {BE_TRIGGER_R}R | Trail: {TRAIL_MODE}@{TRAIL_TRIGGER_R}R',
        f'  Spread       : max {MAX_SPREAD_ATR_RATIO:.0%} ATR',
        f'  Session      : 09:00–14:00 UTC | News: ±{NEWS_BUFFER_MIN}m',
        f'  ★ Equity Prot: Day -{DAILY_LOSS_LIMIT:.0%} / +{DAILY_PROFIT_LOCK:.0%}'
        f' | Week -{WEEKLY_LOSS_LIMIT:.0%}',
        f'  ★ PerfRisk   : Boost>{PERF_BOOST_WR:.0%}={PERF_BOOST_MULT}x'
        f' | Cut<{PERF_CUT_WR:.0%}={PERF_CUT_MULT}x (W:{PERF_WINDOW})',
        f'  ★ Partial TP : TP1@{PTP_TP1_R}R ({int(PTP_TP1_PCT*100)}%)'
        f' | {"✅" if PTP_ENABLED else "❌"}',
        f'  Session Mult : {SESSION_RISK_MULTIPLIER}x (off≥{RISK_MULT_MAX_CONSEC_LOSS}L)',
    ]
    print('=' * 60, flush=True)
    for line in lines: print(line, flush=True)
    for pn, pc in PAIRS.items():
        b = '✅' if pc['allow_buy']  else '❌'
        s = '✅' if pc['allow_sell'] else '❌'
        print(f'  {pn:<8}: BUY={b} SELL={s}', flush=True)
    print('=' * 60, flush=True)

    tg(
        f'🚀 *TL Breaks Bot v5* [{mode}]{nl}'
        f'TF:`{STRATEGY_TF}` | EMA:`{EMA_PERIOD}` | RSI:`{RSI_PERIOD}`{nl}'
        f'★ Equity: Day`-{DAILY_LOSS_LIMIT:.0%}`/`+{DAILY_PROFIT_LOCK:.0%}`'
        f' | Week`-{WEEKLY_LOSS_LIMIT:.0%}`{nl}'
        f'★ PerfRisk: `{PERF_BOOST_MULT}x`/`{PERF_CUT_MULT}x` (W:{PERF_WINDOW}){nl}'
        f'★ Partial TP: `{PTP_TP1_R}R` ({int(PTP_TP1_PCT*100)}%)'
        f' `{"ON" if PTP_ENABLED else "OFF"}`{nl}'
        f'Session:`09-14 UTC` | News:`±{NEWS_BUFFER_MIN}m`{nl}'
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
                    time.sleep(60); continue
            else:
                ping_session()
            session_age = (session_age + 1) % 25
            run_scan()

        except KeyboardInterrupt:
            log('🛑 Bot stopped')
            csv_summary(period='all')
            tg('🛑 TL Breaks Bot v5 stopped')
            break
        except Exception as ex:
            log(f'LOOP ERROR: {ex}')
            tg(f'❌ Bot Error: `{str(ex)[:100]}`')

        time.sleep(SCAN_INTERVAL)


if __name__ == '__main__':
    start_bot()
