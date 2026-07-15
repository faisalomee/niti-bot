<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
body { margin:0; font-family:-apple-system,Segoe UI,sans-serif; background:#1e1e1e; color:#ddd; }
.bar { position:sticky; top:0; background:#2d2d2d; padding:10px 16px; display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #444; }
.bar span { font-size:13px; color:#aaa; }
button { background:#0a84ff; color:#fff; border:none; padding:8px 22px; border-radius:6px; font-size:14px; cursor:pointer; font-weight:600; }
button:active { background:#0060c0; }
pre { margin:0; padding:16px; font-size:11px; line-height:1.45; overflow-x:auto; font-family:Menlo,Consolas,monospace; white-space:pre; }
</style>
</head>
<body>
<div class="bar">
  <span>bingx_scanner_tight.py &mdash; 1680 lines &mdash; margin-skip Telegram message fix (2026-07-15)</span>
  <button onclick="copyCode(this)">Copy</button>
</div>
<pre id="code">import os, time, hmac, hashlib, requests
from flask import Flask
from threading import Thread
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

# ==================== ENV VARS ====================
API_KEY           = os.environ.get(&quot;BINGX_API_KEY&quot;)
SECRET_KEY        = os.environ.get(&quot;BINGX_SECRET_KEY&quot;)
TG_TOKEN          = os.environ.get(&quot;TG_BOT_TOKEN_TIGHT&quot;)
TG_CHAT_ID        = os.environ.get(&quot;TG_CHAT_ID_TIGHT&quot;)
TG_JOURNAL_ID     = os.environ.get(&quot;TG_JOURNAL_CHAT_ID&quot;)
FAST_TRADE_AMOUNT = float(os.environ.get(&quot;FAST_TRADE_AMOUNT&quot;, 20))

BASE_URL = &quot;https://open-api.bingx.com&quot;

# ==================== FAST SIGNAL CONFIG ====================
FAST_TIMEFRAME          = os.environ.get(&quot;FAST_TIMEFRAME&quot;, &quot;1m&quot;)   # changed from 3m (2026-07-14) - cuts breakout-detection lag ~3min-&gt;~1min per Faisal&#x27;s scalping goal
FAST_SCAN_INTERVAL_SECONDS = int(os.environ.get(&quot;FAST_SCAN_INTERVAL_SECONDS&quot;, 60))   # lowered from hardcoded 180s (2026-07-14) to match 1m timeframe - otherwise the lag fix above is pointless
FAST_MIN_QUOTE_VOL      = float(os.environ.get(&quot;FAST_MIN_QUOTE_VOL&quot;, 2_000_000))
FAST_MAX_SYMBOLS        = 150
FAST_CONSOL_LOOKBACK    = 20
FAST_BREAKOUT_ATR_MULT  = float(os.environ.get(&quot;FAST_BREAKOUT_ATR_MULT&quot;, 0.2))   # loosened from 0.3 (2026-07-11) - catch moves earlier
FAST_VOL_MULT           = 3.0
FAST_VOL_LB             = 20
FAST_ATR_LEN            = 14
FAST_SL_ATR_MULT        = 1.2   # NOTE: dead/unused - actual SL uses SL_ATR_BUFFER_MULT below. Kept only for reference.
FAST_RISK_USDT          = float(os.environ.get(&quot;FAST_RISK_USDT&quot;, 20.0))
FAST_EXCLUDE_TOP_N       = 75
FAST_EXTENSION_LOOKBACK  = 20
FAST_EXTENSION_LIMIT     = 4.0
FAST_EXTENSION_HARD_SKIP = float(os.environ.get(&quot;FAST_EXTENSION_HARD_SKIP&quot;, 7.0))   # added 2026-07-14: beyond this many x ATR, skip the trade entirely (too late/exhausted a move), not just half-size
FAST_EXTENSION_MULT      = 0.5
FAST_MARGIN_CAP_MULT     = 5.0
FAST_TP1_RR             = float(os.environ.get(&quot;FAST_TP1_RR&quot;, 0.8))
FAST_TRAIL_ACTIVATE_RR  = 1.0
FAST_MAX_CONCURRENT_TRADES = int(os.environ.get(&quot;FAST_MAX_CONCURRENT_TRADES&quot;, 3))   # added 2026-07-14 - Fast Signal had NO cap before, contributed to margin exhaustion
FAST_CLOSE_POSITION_MIN = float(os.environ.get(&quot;FAST_CLOSE_POSITION_MIN&quot;, 0.6))   # loosened from 0.7 (2026-07-11)
FAST_TRAIL_ATR_MULT     = float(os.environ.get(&quot;FAST_TRAIL_ATR_MULT&quot;, 1.5))
FAST_TRAIL_PCT_FALLBACK = float(os.environ.get(&quot;FAST_TRAIL_PCT_FALLBACK&quot;, 3.0))

SL_ATR_BUFFER_MULT = 0.3   # shared SL buffer, used by both Fast and Tight

# ---- Progress-based time exit (reworked 2026-07-11) ----
# Old behaviour: force-checked every 10 min and killed anything below a fixed R
# threshold, even trades that were quietly profitable. New behaviour: profit is
# never time-exited (trailing handles it); only flat/losing trades and trades whose
# profit has stalled (stagnant) get timed out. A long backup cap protects against
# a trade getting stuck open due to a bug/glitch.
FAST_PROGRESS_CHECK_SECONDS   = int(os.environ.get(&quot;FAST_PROGRESS_CHECK_SECONDS&quot;, 900))    # 15 min between checks
FAST_STAGNATION_CHECKS        = int(os.environ.get(&quot;FAST_STAGNATION_CHECKS&quot;, 3))           # consecutive non-improving checks (~45 min) before locking profit
FAST_STAGNATION_MIN_R_INCREASE = float(os.environ.get(&quot;FAST_STAGNATION_MIN_R_INCREASE&quot;, 0.1))
FAST_SAFETY_CAP_SECONDS       = int(os.environ.get(&quot;FAST_SAFETY_CAP_SECONDS&quot;, 12600))      # 3.5h - backup net only, should basically never trigger in normal operation

# ---- MTF trend filter (added to Fast Signal 2026-07-11, reusing the Tight-era helper) ----
# Direction-only check against the already-closed 1h candle - no extra waiting,
# checked at the same instant as the breakout candle itself.
MTF_FILTER_ENABLED = os.environ.get(&quot;MTF_FILTER_ENABLED&quot;, &quot;true&quot;).lower() == &quot;true&quot;
MTF_INTERVAL        = os.environ.get(&quot;MTF_INTERVAL&quot;, &quot;1h&quot;)
EMA200_LEN          = 200

MIN_PRICE = 0.001

# ==================== TIGHT CONFIG (Stock Niti strategy, replaces old Tight 2 - 2026-07-11) ====================
# Volume-spike -&gt; cooldown -&gt; re-entry-on-breakout strategy, all on the 1m timeframe.
TIGHT_TIMEFRAME              = &quot;1m&quot;
TIGHT_MIN_QUOTE_VOL          = float(os.environ.get(&quot;TIGHT_MIN_QUOTE_VOL&quot;, 1_000_000))   # liquidity floor for coin selection
TIGHT_MAX_SYMBOLS            = int(os.environ.get(&quot;TIGHT_MAX_SYMBOLS&quot;, 400))

TIGHT_BASELINE_CANDLES       = int(os.environ.get(&quot;TIGHT_BASELINE_CANDLES&quot;, 4320))   # 3 days of 1m candles
BINGX_KLINE_MAX_LIMIT        = 1440   # confirmed 2026-07-11 via live API error: &quot;limit: This field must be less than or equal to 1440.&quot; Baseline is paginated in chunks of this size.
TIGHT_BASELINE_REFRESH_SECONDS = int(os.environ.get(&quot;TIGHT_BASELINE_REFRESH_SECONDS&quot;, 1800))   # re-fetch baseline every 30 min per symbol, not every scan

TIGHT_SPIKE_VOL_MULT         = float(os.environ.get(&quot;TIGHT_SPIKE_VOL_MULT&quot;, 20.0))   # live in-progress candle vs 3-day baseline
TIGHT_COOLDOWN_VOL_RATIO     = float(os.environ.get(&quot;TIGHT_COOLDOWN_VOL_RATIO&quot;, 3.0))   # below this = &quot;cooling&quot;
TIGHT_COOLDOWN_MIN_CANDLES   = int(os.environ.get(&quot;TIGHT_COOLDOWN_MIN_CANDLES&quot;, 5))     # consecutive cool candles to confirm a range
TIGHT_RANGE_MAX_ATR_MULT     = float(os.environ.get(&quot;TIGHT_RANGE_MAX_ATR_MULT&quot;, 2.0))   # reject range as &quot;not sideways&quot; if wider than this x ATR (filters slow-bleed)
TIGHT_REENTRY_VOL_MULT       = float(os.environ.get(&quot;TIGHT_REENTRY_VOL_MULT&quot;, 5.0))     # volume needed on the breakout candle to re-enter
TIGHT_SL_ATR_BUFFER_MULT     = float(os.environ.get(&quot;TIGHT_SL_ATR_BUFFER_MULT&quot;, 0.3))
TIGHT_RR_TP                  = float(os.environ.get(&quot;TIGHT_RR_TP&quot;, 4.0))
TIGHT_BE_TRIGGER_R           = float(os.environ.get(&quot;TIGHT_BE_TRIGGER_R&quot;, 2.0))         # move SL to breakeven at this R, full size kept
TIGHT_MAX_COOLDOWN_WAIT_SECONDS = int(os.environ.get(&quot;TIGHT_MAX_COOLDOWN_WAIT_SECONDS&quot;, 3600))   # give up watching after 60 min with no breakout
TIGHT_MAX_CONCURRENT_TRADES  = int(os.environ.get(&quot;TIGHT_MAX_CONCURRENT_TRADES&quot;, 3))   # lowered from 4 (2026-07-14) - margin exhaustion across Tight+Fast
TIGHT_RISK_USDT              = float(os.environ.get(&quot;TIGHT_RISK_USDT&quot;, 5.0))   # lowered from 20.0 (2026-07-14) per Faisal&#x27;s decision
TIGHT_LEVERAGE               = int(os.environ.get(&quot;TIGHT_LEVERAGE&quot;, 20))   # fixed, per Faisal&#x27;s instruction (2026-07-11)
TIGHT_MAX_MARGIN_USDT        = float(os.environ.get(&quot;TIGHT_MAX_MARGIN_USDT&quot;, 40.0))   # lowered from 50.0 (2026-07-14)
TIGHT_ATR_LEN                = 14

TIGHT_SCAN_INTERVAL_SECONDS  = int(os.environ.get(&quot;TIGHT_SCAN_INTERVAL_SECONDS&quot;, 30))
# NOTE: scanning up to TIGHT_MAX_SYMBOLS pairs on a 1m timeframe every 30s is a
# meaningfully heavier REST-polling load than the old 15m/60s cadence - watch
# BingX rate-limit responses in the logs after deploying. If it gets throttled,
# either raise this interval, cut TIGHT_MAX_SYMBOLS, or (better long-term) move
# to a websocket feed instead of REST polling for the live in-progress candle.

# ==================== GLOBAL STATE ====================
tight_auto_trade_enabled = False
fast_auto_trade_enabled  = False
symbol_precision   = {}
symbol_max_lev     = {}
tight_open_trades  = {}
fast_open_trades   = {}
fast_alerted       = set()
daily_trades       = []
last_summary_date  = None

tight_watch          = {}   # symbol -&gt; spike/cooldown/ready state, see check_tight_symbol()
tight_baseline_cache = {}   # symbol -&gt; {&quot;baseline&quot;: val, &quot;ts&quot;: ...}

mtf_cache = {}

# ---- API backoff (added 2026-07-11) ----
# BingX&#x27;s rate-limit penalty for repeated bad/over-limit requests is self-renewing:
# each additional request made *while still blocked* pushes the &quot;retry after&quot; time
# further out. Continuing to scan during a block was preventing the block from
# ever clearing on its own. This makes the bot go fully quiet on market-data calls
# for API_BACKOFF_SECONDS as soon as a rate-limit response is detected, instead of
# keep hammering the endpoint and extending its own penalty.
API_BACKOFF_SECONDS = int(os.environ.get(&quot;API_BACKOFF_SECONDS&quot;, 1200))   # 20 min
_api_backoff_until  = 0.0
_api_backoff_logged = 0.0


def api_backoff_active():
    return time.time() &lt; _api_backoff_until


def trigger_api_backoff(reason=&quot;&quot;):
    global _api_backoff_until
    _api_backoff_until = time.time() + API_BACKOFF_SECONDS
    print(f&quot;[API BACKOFF] pausing all market-data scanning for {API_BACKOFF_SECONDS}s - {reason}&quot;)


def _looks_like_rate_limit(resp):
    if not isinstance(resp, dict):
        return False
    msg = str(resp.get(&quot;msg&quot;, &quot;&quot;)).lower()
    return resp.get(&quot;code&quot;) == 109429 or &quot;over&quot; in msg or &quot;too many&quot; in msg or &quot;too frequent&quot; in msg


# ==================== CORE API HELPERS ====================
def build_signed_params(params: dict) -&gt; dict:
    params[&quot;timestamp&quot;] = int(time.time() * 1000)
    qs = &quot;&amp;&quot;.join(f&quot;{k}={v}&quot; for k, v in params.items())
    params[&quot;signature&quot;] = hmac.new(
        SECRET_KEY.encode(), qs.encode(), hashlib.sha256
    ).hexdigest()
    return params


def get_futures_symbols():
    url = BASE_URL + &quot;/openApi/swap/v2/quote/contracts&quot;
    r = requests.get(url, timeout=10).json()
    symbols = []
    for c in r.get(&quot;data&quot;, []):
        if c.get(&quot;status&quot;) == 1 and &quot;USDT&quot; in c[&quot;symbol&quot;]:
            sym = c[&quot;symbol&quot;]
            symbols.append(sym)
            symbol_precision[sym] = int(c.get(&quot;quantityPrecision&quot;, 4))
            try:
                symbol_max_lev[sym] = int(float(c.get(&quot;maxLongLeverage&quot;, 20)))
            except Exception:
                symbol_max_lev[sym] = 20
    return symbols


def get_liquid_symbols(symbols, min_quote_vol, max_n=None, exclude_top_n=0):
    try:
        url = BASE_URL + &quot;/openApi/swap/v2/quote/ticker&quot;
        r = requests.get(url, timeout=10).json()
        tickers = r.get(&quot;data&quot;, [])
        if not isinstance(tickers, list):
            return symbols[:max_n] if max_n else symbols
        sym_set = set(symbols)
        liquid = []
        for t in tickers:
            sym = t.get(&quot;symbol&quot;, &quot;&quot;)
            if sym not in sym_set:
                continue
            try:
                qvol = float(t.get(&quot;quoteVolume&quot;, 0))
            except Exception:
                qvol = 0
            if qvol &gt;= min_quote_vol:
                liquid.append((sym, qvol))
        liquid.sort(key=lambda x: x[1], reverse=True)
        if exclude_top_n &gt; 0:
            liquid = liquid[exclude_top_n:]
        if max_n is not None:
            liquid = liquid[:max_n]
        return [s for s, _ in liquid]
    except Exception as e:
        print(f&quot;[LIQUID SYMBOLS ERROR] {e}&quot;)
        return symbols[:max_n] if max_n else symbols


def get_candles(symbol, limit=350, interval=&quot;15m&quot;, end_time=None):
    global _api_backoff_logged
    if api_backoff_active():
        now = time.time()
        if now - _api_backoff_logged &gt; 60:   # don&#x27;t spam the log every single call while backed off
            remaining = int(_api_backoff_until - now)
            print(f&quot;[API BACKOFF] still active - skipping candle requests for {remaining}s more&quot;)
            _api_backoff_logged = now
        return []
    params = {&quot;symbol&quot;: symbol, &quot;interval&quot;: interval, &quot;limit&quot;: limit}
    if end_time is not None:
        params[&quot;endTime&quot;] = int(end_time)
    params = build_signed_params(params)
    url = BASE_URL + &quot;/openApi/swap/v3/quote/klines&quot;
    r = requests.get(url, params=params,
                     headers={&quot;X-BX-APIKEY&quot;: API_KEY}, timeout=10).json()
    candles = r.get(&quot;data&quot;, [])
    # ---- Diagnostic logging (added 2026-07-11) ----
    # get_candles() used to fail silently: any non-list/empty &quot;data&quot; (rate-limit
    # response, error payload, API cap on `limit`, etc.) just became [] with zero
    # visibility. This logs the raw response whenever the result looks off, so a
    # genuine API-side problem shows up in the Render logs instead of just quietly
    # starving both strategies of data.
    if not isinstance(candles, list):
        if _looks_like_rate_limit(r):
            trigger_api_backoff(f&quot;{symbol} {interval}: {str(r)[:200]}&quot;)
        else:
            print(f&quot;[CANDLES ERROR] {symbol} {interval} limit={limit} - non-list response: {str(r)[:300]}&quot;)
        return []
    if len(candles) &lt; min(limit, 50):
        print(f&quot;[CANDLES SHORT] {symbol} {interval} requested={limit} got={len(candles)} - raw: {str(r)[:300]}&quot;)
    candles.sort(key=lambda x: x[&quot;time&quot;])
    return candles


def get_current_price(symbol):
    try:
        url = BASE_URL + &quot;/openApi/swap/v2/quote/price&quot;
        r = requests.get(url, params={&quot;symbol&quot;: symbol}, timeout=5).json()
        return float(r.get(&quot;data&quot;, {}).get(&quot;price&quot;, 0))
    except Exception:
        return 0


# ==================== INDICATORS ====================
def ema_series(closes, period):
    k = 2 / (period + 1)
    ema = closes[0]
    result = []
    for c in closes:
        ema = c * k + ema * (1 - k)
        result.append(ema)
    return result


def atr_series(highs, lows, closes, period=14):
    n = len(closes)
    if n &lt; period + 1:
        return [max(h - l, 0.0001) for h, l in zip(highs, lows)]
    tr_list = [highs[0] - lows[0]]
    for i in range(1, n):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        tr_list.append(tr)
    atr_vals = [sum(tr_list[:period]) / period]
    for i in range(period, n):
        atr_vals.append((atr_vals[-1] * (period - 1) + tr_list[i]) / period)
    padding = n - len(atr_vals)
    return [atr_vals[0]] * padding + atr_vals


def get_mtf_trend(symbol):
    &quot;&quot;&quot;Cached (5 min) higher-timeframe (MTF_INTERVAL) EMA200 trend direction. Used by
    Fast Signal as a direction-only filter - no timing delay, checked instantly
    against the already-closed 1h candle at the same moment as the breakout candle.&quot;&quot;&quot;
    now = time.time()
    cached = mtf_cache.get(symbol)
    if cached and now - cached[&quot;ts&quot;] &lt; 300:
        return cached[&quot;trend&quot;]
    try:
        candles = get_candles(symbol, limit=250, interval=MTF_INTERVAL)
        if len(candles) &lt; 210:
            return cached[&quot;trend&quot;] if cached else None
        closes = [cl(c) for c in candles]
        ema200 = ema_series(closes, EMA200_LEN)
        trend = &quot;UP&quot; if closes[-1] &gt; ema200[-1] else &quot;DOWN&quot;
        mtf_cache[symbol] = {&quot;trend&quot;: trend, &quot;ts&quot;: now}
        return trend
    except Exception as e:
        print(f&quot;[MTF ERROR] {symbol}: {e}&quot;)
        return cached[&quot;trend&quot;] if cached else None


def h(c):  return float(c[&quot;high&quot;])
def l(c):  return float(c[&quot;low&quot;])
def cl(c): return float(c[&quot;close&quot;])
def o(c):  return float(c[&quot;open&quot;])
def v(c):  return float(c[&quot;volume&quot;])


# ==================== TELEGRAM ====================
def send_tg(msg, chat_id=None):
    cid = chat_id or TG_CHAT_ID
    url = f&quot;https://api.telegram.org/bot{TG_TOKEN}/sendMessage&quot;
    requests.post(url, json={&quot;chat_id&quot;: cid, &quot;text&quot;: msg, &quot;parse_mode&quot;: &quot;HTML&quot;}, timeout=10)


def send_journal(msg):
    if TG_JOURNAL_ID:
        send_tg(msg, chat_id=TG_JOURNAL_ID)


def journal_closed_trade(trade):
    &quot;&quot;&quot;Single consolidated journal entry per closed trade - kept deliberately simple,
    no intermediate messages (no TP1-banked / cooldown-triggered spam).&quot;&quot;&quot;
    sign = &quot;+&quot; if trade.get(&quot;pnl&quot;, 0) &gt; 0 else &quot;&quot;
    send_journal(
        &quot;Trade Closed [&quot; + trade.get(&quot;label&quot;, &quot;?&quot;) + &quot;] - &quot; + trade[&quot;symbol&quot;] + &quot;\n&quot;
        &quot;------------------------------\n&quot;
        &quot;Side  : &quot; + trade[&quot;side&quot;] + &quot;\nEntry : &quot; + str(trade[&quot;entry&quot;]) + &quot;\n&quot;
        &quot;Result: &quot; + trade.get(&quot;result&quot;, &quot;?&quot;) + &quot;\n&quot;
        &quot;PnL   : &quot; + sign + str(trade.get(&quot;pnl&quot;, 0)) + &quot; USDT\n&quot;
        &quot;------------------------------\nNiti Journal&quot;
    )


# ==================== LEVERAGE ====================
def get_fast_leverage(symbol):
    max_lev = symbol_max_lev.get(symbol, 20)
    calc = int(max_lev * 0.20)
    lev = max(calc, 10)
    return min(lev, max_lev)


def set_leverage_api(symbol, lev):
    try:
        url = BASE_URL + &quot;/openApi/swap/v2/trade/leverage&quot;
        for side in [&quot;LONG&quot;, &quot;SHORT&quot;]:
            params = build_signed_params({&quot;symbol&quot;: symbol, &quot;side&quot;: side, &quot;leverage&quot;: lev})
            requests.post(url, params=params, headers={&quot;X-BX-APIKEY&quot;: API_KEY}, timeout=10)
    except Exception as e:
        print(f&quot;[LEVERAGE ERROR] {symbol}: {e}&quot;)


# ==================== ORDER PLACEMENT ====================
def place_market_order(symbol, side, qty, pos_side):
    url = BASE_URL + &quot;/openApi/swap/v2/trade/order&quot;
    params = build_signed_params({
        &quot;symbol&quot;: symbol, &quot;side&quot;: side,
        &quot;positionSide&quot;: pos_side, &quot;type&quot;: &quot;MARKET&quot;, &quot;quantity&quot;: qty,
    })
    r = requests.post(url, params=params, headers={&quot;X-BX-APIKEY&quot;: API_KEY}, timeout=10).json()
    oid = r.get(&quot;data&quot;, {}).get(&quot;order&quot;, {}).get(&quot;orderId&quot;, &quot;N/A&quot;)
    if oid == &quot;N/A&quot;:
        print(f&quot;[ORDER FAIL] {symbol} {side} qty={qty} positionSide={pos_side} - BingX: {r}&quot;)
    return oid


def place_sl_order(symbol, close_side, pos_side, sl_price, qty):
    url = BASE_URL + &quot;/openApi/swap/v2/trade/order&quot;
    params = build_signed_params({
        &quot;symbol&quot;: symbol, &quot;side&quot;: close_side, &quot;positionSide&quot;: pos_side,
        &quot;type&quot;: &quot;STOP_MARKET&quot;, &quot;stopPrice&quot;: round(sl_price, 6), &quot;quantity&quot;: qty,
        &quot;workingType&quot;: &quot;MARK_PRICE&quot;,
    })
    r = requests.post(url, params=params, headers={&quot;X-BX-APIKEY&quot;: API_KEY}, timeout=10).json()
    print(f&quot;[SL] {symbol}: {r}&quot;)
    return r.get(&quot;data&quot;, {}).get(&quot;order&quot;, {}).get(&quot;orderId&quot;, &quot;N/A&quot;)


def place_tp_order(symbol, close_side, pos_side, tp_price, qty):
    url = BASE_URL + &quot;/openApi/swap/v2/trade/order&quot;
    params = build_signed_params({
        &quot;symbol&quot;: symbol, &quot;side&quot;: close_side, &quot;positionSide&quot;: pos_side,
        &quot;type&quot;: &quot;TAKE_PROFIT_MARKET&quot;, &quot;stopPrice&quot;: round(tp_price, 6), &quot;quantity&quot;: qty,
        &quot;workingType&quot;: &quot;MARK_PRICE&quot;,
    })
    r = requests.post(url, params=params, headers={&quot;X-BX-APIKEY&quot;: API_KEY}, timeout=10).json()
    return r.get(&quot;data&quot;, {}).get(&quot;order&quot;, {}).get(&quot;orderId&quot;, &quot;N/A&quot;)


def cancel_order(symbol, order_id):
    &quot;&quot;&quot;NOTE: verify this endpoint/method against current BingX docs before relying
    on it live - it has not been tested against the real API in this conversation.&quot;&quot;&quot;
    try:
        url = BASE_URL + &quot;/openApi/swap/v2/trade/order&quot;
        params = build_signed_params({&quot;symbol&quot;: symbol, &quot;orderId&quot;: order_id})
        r = requests.delete(url, params=params, headers={&quot;X-BX-APIKEY&quot;: API_KEY}, timeout=10).json()
        return r
    except Exception as e:
        print(f&quot;[CANCEL ERROR] {symbol} {order_id}: {e}&quot;)
        return None


def check_order_status(order_id, symbol):
    try:
        params = build_signed_params({&quot;symbol&quot;: symbol, &quot;orderId&quot;: order_id})
        url = BASE_URL + &quot;/openApi/swap/v2/trade/order&quot;
        r = requests.get(url, params=params, headers={&quot;X-BX-APIKEY&quot;: API_KEY}, timeout=10).json()
        return r.get(&quot;data&quot;, {}).get(&quot;order&quot;, {}).get(&quot;status&quot;, &quot;&quot;)
    except Exception:
        return &quot;&quot;


def get_fill_price(order_id, symbol, fallback=0.0):
    &quot;&quot;&quot;Actual average fill price for an order, for accurate PnL - falls back to the
    nominal signal-time price if the exchange hasn&#x27;t got it yet or the call fails.&quot;&quot;&quot;
    try:
        params = build_signed_params({&quot;symbol&quot;: symbol, &quot;orderId&quot;: order_id})
        url = BASE_URL + &quot;/openApi/swap/v2/trade/order&quot;
        r = requests.get(url, params=params, headers={&quot;X-BX-APIKEY&quot;: API_KEY}, timeout=10).json()
        avg = float(r.get(&quot;data&quot;, {}).get(&quot;order&quot;, {}).get(&quot;avgPrice&quot;, 0) or 0)
        return avg if avg &gt; 0 else fallback
    except Exception:
        return fallback


def get_available_margin():
    &quot;&quot;&quot;Available USDT margin from BingX. Returns None if the call fails -
    callers must treat None as &#x27;unknown, proceed&#x27; so a flaky balance endpoint
    can never freeze all trading.&quot;&quot;&quot;
    try:
        params = build_signed_params({})
        url = BASE_URL + &quot;/openApi/swap/v2/user/balance&quot;
        r = requests.get(url, params=params, headers={&quot;X-BX-APIKEY&quot;: API_KEY}, timeout=10).json()
        bal = r.get(&quot;data&quot;, {}).get(&quot;balance&quot;, {})
        if isinstance(bal, list):
            bal = next((b for b in bal if b.get(&quot;asset&quot;) == &quot;USDT&quot;), bal[0] if bal else {})
        avail = float(bal.get(&quot;availableMargin&quot;, 0) or 0)
        return avail if avail &gt; 0 else None
    except Exception:
        return None


def place_sl_guarded(symbol, close_side, pos_side, sl_price, qty):
    &quot;&quot;&quot;Place an SL order with one retry. Returns sl_id, or None if BOTH attempts
    failed - caller MUST then emergency-close the position rather than leave it
    naked (root cause suspected in the CRV-USDT TimeExit-instead-of-SL incident).&quot;&quot;&quot;
    sl_id = place_sl_order(symbol, close_side, pos_side, sl_price, qty)
    if sl_id and sl_id != &quot;N/A&quot;:
        return sl_id
    time.sleep(1)
    sl_id = place_sl_order(symbol, close_side, pos_side, sl_price, qty)
    if sl_id and sl_id != &quot;N/A&quot;:
        return sl_id
    return None


def close_fast_position(symbol, reason=&quot;&quot;):
    if symbol not in fast_open_trades:
        return
    trade = fast_open_trades[symbol]
    try:
        pos_side   = &quot;LONG&quot;  if trade[&quot;side&quot;] == &quot;BUY&quot; else &quot;SHORT&quot;
        close_side = &quot;SELL&quot;  if trade[&quot;side&quot;] == &quot;BUY&quot; else &quot;BUY&quot;
        remaining  = trade.get(&quot;remaining_qty&quot;, 0)
        exit_price = get_current_price(symbol)
        if remaining &gt; 0 and fast_auto_trade_enabled:
            close_oid = place_market_order(symbol, close_side, remaining, pos_side)
            if close_oid and close_oid != &quot;N/A&quot;:
                time.sleep(0.5)
                exit_price = get_fill_price(close_oid, symbol, fallback=exit_price)
        if trade.get(&quot;sl_id&quot;):
            cancel_order(symbol, trade[&quot;sl_id&quot;])
        if not trade.get(&quot;tp1_filled&quot;) and trade.get(&quot;tp1_id&quot;):
            cancel_order(symbol, trade[&quot;tp1_id&quot;])
        entry_ref = trade.get(&quot;entry_fill&quot;, trade[&quot;entry&quot;])
        if trade[&quot;side&quot;] == &quot;BUY&quot;:
            leg_pnl = (exit_price - entry_ref) * remaining
        else:
            leg_pnl = (entry_ref - exit_price) * remaining
        total_pnl = round(trade.get(&quot;partial_pnl&quot;, 0.0) + leg_pnl, 2)
        trade[&quot;pnl&quot;]    = total_pnl
        trade[&quot;result&quot;] = reason
        trade[&quot;label&quot;]  = &quot;Fast&quot;
        trade[&quot;symbol&quot;] = symbol
        daily_trades.append(trade)
        journal_closed_trade(trade)
        del fast_open_trades[symbol]
        print(f&quot;[FAST CLOSE] {symbol} {reason}&quot;)
    except Exception as e:
        print(f&quot;[FAST CLOSE ERROR] {symbol}: {e}&quot;)


def close_tight_position(oid, reason=&quot;&quot;):
    trade = tight_open_trades.get(oid)
    if not trade:
        return
    try:
        symbol     = trade[&quot;symbol&quot;]
        pos_side   = trade[&quot;pos_side&quot;]
        close_side = trade[&quot;close_side&quot;]
        qty        = trade.get(&quot;remaining_qty&quot;, trade.get(&quot;total_qty&quot;, 0))
        entry_ref  = trade.get(&quot;entry_fill&quot;, trade[&quot;entry&quot;])
        exit_price = get_current_price(symbol)
        if qty &gt; 0 and tight_auto_trade_enabled:
            close_oid = place_market_order(symbol, close_side, qty, pos_side)
            if close_oid and close_oid != &quot;N/A&quot;:
                time.sleep(0.5)
                exit_price = get_fill_price(close_oid, symbol, fallback=exit_price)
        if trade.get(&quot;sl_id&quot;):
            cancel_order(symbol, trade[&quot;sl_id&quot;])
        if trade.get(&quot;tp_id&quot;):
            cancel_order(symbol, trade[&quot;tp_id&quot;])
        if trade[&quot;side&quot;] == &quot;BUY&quot;:
            leg_pnl = (exit_price - entry_ref) * qty
        else:
            leg_pnl = (entry_ref - exit_price) * qty
        trade[&quot;pnl&quot;]    = round(trade.get(&quot;tp1_pnl&quot;, 0) + leg_pnl, 2)
        trade[&quot;result&quot;] = reason
        trade[&quot;label&quot;]  = &quot;Tight&quot;
        daily_trades.append(trade)
        journal_closed_trade(trade)
        tight_open_trades.pop(oid, None)
        print(f&quot;[TIGHT CLOSE] {symbol} {reason}&quot;)
    except Exception as e:
        print(f&quot;[TIGHT CLOSE ERROR] {oid}: {e}&quot;)


def place_fast_order(symbol, side, entry, sl_price, tp1_price, atr_now, risk_usdt=FAST_RISK_USDT):
    try:
        lev       = get_fast_leverage(symbol)
        set_leverage_api(symbol, lev)
        precision  = symbol_precision.get(symbol, 4)
        risk_dist  = abs(entry - sl_price)
        if risk_dist &lt;= 0:
            return None

        risk_qty      = risk_usdt / risk_dist
        margin_cap_qty = (FAST_TRADE_AMOUNT * FAST_MARGIN_CAP_MULT * lev) / entry
        total_qty     = round(min(risk_qty, margin_cap_qty), precision)
        half_qty      = round(total_qty / 2, precision)
        if total_qty &lt;= 0 or half_qty &lt;= 0:
            return None
        pos_side   = &quot;LONG&quot;  if side == &quot;BUY&quot; else &quot;SHORT&quot;
        close_side = &quot;SELL&quot;  if side == &quot;BUY&quot; else &quot;BUY&quot;
        sl_pct = abs(entry - sl_price) / entry * 100

        # Pre-trade margin check: skip cleanly instead of firing an order BingX will
        # reject with 101204. None (endpoint failure) = unknown -&gt; proceed.
        required_margin = total_qty * entry / lev
        avail = get_available_margin()
        if avail is not None and avail &lt; required_margin * 1.05:
            print(f&quot;[FAST MARGIN SKIP] {symbol} need ~${required_margin:.2f}, available ${avail:.2f} - skipping&quot;)
            return &quot;MARGIN_SKIP&quot;

        order_id = place_market_order(symbol, side, total_qty, pos_side)
        print(f&quot;[FAST ORDER] {symbol} {side} lev={lev}x qty={total_qty} risk=${risk_usdt}: {order_id}&quot;)
        if order_id != &quot;N/A&quot;:
            time.sleep(0.5)
            entry_fill = get_fill_price(order_id, symbol, fallback=entry)

            # Recompute risk distance and TP1 from the REAL fill price so the stated
            # R-multiples are real (see 2026-07-15 finding: nominal-entry levels made
            # TP1/Trail profits tiny). SL stays structural (unchanged).
            risk_dist = abs(entry_fill - sl_price)
            if risk_dist &lt;= 0:
                risk_dist = abs(entry - sl_price)
            if side == &quot;BUY&quot;:
                tp1_price = round(entry_fill + risk_dist * FAST_TP1_RR, precision)
            else:
                tp1_price = round(entry_fill - risk_dist * FAST_TP1_RR, precision)

            # SL first, guarded: if it can&#x27;t be placed even on retry, close the
            # position immediately instead of leaving it naked.
            sl_id = place_sl_guarded(symbol, close_side, pos_side, sl_price, total_qty)
            if sl_id is None:
                print(f&quot;[FAST SL GUARD] {symbol} SL placement failed twice - emergency closing position&quot;)
                place_market_order(symbol, close_side, total_qty, pos_side)
                send_tg(f&quot;⚠️ FAST {symbol}: SL placement failed - position emergency-closed for safety&quot;)
                return None
            tp1_id = place_tp_order(symbol, close_side, pos_side, tp1_price, half_qty)
            fast_open_trades[symbol] = {
                &quot;symbol&quot;: symbol, &quot;side&quot;: side, &quot;entry&quot;: entry, &quot;entry_fill&quot;: entry_fill,
                &quot;sl&quot;: sl_price, &quot;sl_id&quot;: sl_id, &quot;tp1&quot;: tp1_price, &quot;tp1_id&quot;: tp1_id, &quot;lev&quot;: lev,
                &quot;sl_pct&quot;: sl_pct, &quot;close_side&quot;: close_side, &quot;pos_side&quot;: pos_side,
                &quot;total_qty&quot;: total_qty, &quot;remaining_qty&quot;: total_qty, &quot;tp1_filled&quot;: False, &quot;partial_pnl&quot;: 0.0,
                &quot;trail_price&quot;: entry_fill, &quot;activated&quot;: False, &quot;order_id&quot;: order_id,
                &quot;atr_at_entry&quot;: atr_now, &quot;opened_ts&quot;: time.time(),
                &quot;risk_dist&quot;: risk_dist, &quot;be_price&quot;: None,
                &quot;next_check_ts&quot;: time.time() + FAST_PROGRESS_CHECK_SECONDS,
                &quot;best_favorable_r&quot;: 0.0, &quot;stagnation_count&quot;: 0,
                &quot;time&quot;: datetime.now(timezone.utc).strftime(&quot;%H:%M UTC&quot;),
            }
        return order_id
    except Exception as e:
        print(f&quot;[FAST ORDER ERROR] {symbol}: {e}&quot;)
        return None


def track_fast_trades():
    for symbol in list(fast_open_trades.keys()):
        try:
            trade = fast_open_trades[symbol]

            if not trade.get(&quot;tp1_filled&quot;) and trade.get(&quot;tp1_id&quot;):
                status = check_order_status(trade[&quot;tp1_id&quot;], symbol)
                if status == &quot;FILLED&quot;:
                    half_qty  = round(trade[&quot;total_qty&quot;] / 2, symbol_precision.get(symbol, 4))
                    entry_ref = trade.get(&quot;entry_fill&quot;, trade[&quot;entry&quot;])
                    tp1_fill  = get_fill_price(trade[&quot;tp1_id&quot;], symbol, fallback=trade[&quot;tp1&quot;])
                    trade[&quot;tp1_fill&quot;] = tp1_fill
                    leg_pnl   = (tp1_fill - entry_ref) * half_qty if trade[&quot;side&quot;] == &quot;BUY&quot; else (entry_ref - tp1_fill) * half_qty
                    trade[&quot;partial_pnl&quot;]   = trade.get(&quot;partial_pnl&quot;, 0.0) + leg_pnl
                    trade[&quot;remaining_qty&quot;] = trade[&quot;total_qty&quot;] - half_qty
                    trade[&quot;tp1_filled&quot;]    = True

                    if trade.get(&quot;sl_id&quot;):
                        cancel_order(symbol, trade[&quot;sl_id&quot;])
                    # True breakeven = the REAL fill price, not the nominal signal price
                    new_sl_id = place_sl_order(
                        symbol, trade[&quot;close_side&quot;], trade[&quot;pos_side&quot;], entry_ref, trade[&quot;remaining_qty&quot;]
                    )
                    trade[&quot;sl_id&quot;]    = new_sl_id
                    trade[&quot;sl&quot;]       = entry_ref
                    trade[&quot;be_price&quot;] = entry_ref

            sl_status = check_order_status(trade[&quot;sl_id&quot;], symbol) if trade.get(&quot;sl_id&quot;) else &quot;&quot;
            if sl_status == &quot;FILLED&quot;:
                if not trade.get(&quot;tp1_filled&quot;) and trade.get(&quot;tp1_id&quot;):
                    cancel_order(symbol, trade[&quot;tp1_id&quot;])
                remaining = trade.get(&quot;remaining_qty&quot;, 0)
                entry_ref = trade.get(&quot;entry_fill&quot;, trade[&quot;entry&quot;])
                sl_fill   = get_fill_price(trade[&quot;sl_id&quot;], symbol, fallback=trade[&quot;sl&quot;])
                if trade[&quot;side&quot;] == &quot;BUY&quot;:
                    leg_pnl = (sl_fill - entry_ref) * remaining
                else:
                    leg_pnl = (entry_ref - sl_fill) * remaining
                total_pnl = round(trade.get(&quot;partial_pnl&quot;, 0.0) + leg_pnl, 2)
                trade[&quot;pnl&quot;]    = total_pnl
                trade[&quot;result&quot;] = &quot;BE&quot; if trade.get(&quot;tp1_filled&quot;) else &quot;SL&quot;
                trade[&quot;label&quot;]  = &quot;Fast&quot;
                trade[&quot;symbol&quot;] = symbol
                daily_trades.append(trade)
                journal_closed_trade(trade)
                del fast_open_trades[symbol]
                continue

            # ---- Progress-based time exit (reworked 2026-07-11) ----
            # Profit (favorable_r &gt; 0): never TimeExit - hold for trailing to manage,
            # unless it stagnates (see stagnation_count below).
            # Flat/loss (favorable_r &lt;= 0): exit at the next check, same as before.
            # A long safety cap (backup net only) applies regardless of state.
            now_ts = time.time()

            opened_ts = trade.get(&quot;opened_ts&quot;, now_ts)
            if now_ts - opened_ts &gt;= FAST_SAFETY_CAP_SECONDS:
                print(f&quot;[FAST SAFETY CAP] {symbol} - {FAST_SAFETY_CAP_SECONDS}s backup cap reached&quot;)
                close_fast_position(symbol, &quot;SafetyCap&quot;)
                continue

            if now_ts &gt;= trade.get(&quot;next_check_ts&quot;, now_ts + 1):
                current = get_current_price(symbol)
                risk_dist = trade.get(&quot;risk_dist&quot;, 0)
                if current &gt; 0 and risk_dist &gt; 0:
                    entry_ref = trade.get(&quot;entry_fill&quot;, trade[&quot;entry&quot;])
                    if trade[&quot;side&quot;] == &quot;BUY&quot;:
                        favorable_r = (current - entry_ref) / risk_dist
                    else:
                        favorable_r = (entry_ref - current) / risk_dist

                    if favorable_r &gt; 0:
                        best_r = trade.get(&quot;best_favorable_r&quot;, 0.0)
                        if favorable_r &gt; best_r + FAST_STAGNATION_MIN_R_INCREASE:
                            trade[&quot;best_favorable_r&quot;]  = favorable_r
                            trade[&quot;stagnation_count&quot;]  = 0
                            trade[&quot;next_check_ts&quot;]     = now_ts + FAST_PROGRESS_CHECK_SECONDS
                            print(f&quot;[FAST PROGRESS] {symbol} improving - {favorable_r:.2f}R&quot;)
                        else:
                            trade[&quot;stagnation_count&quot;] = trade.get(&quot;stagnation_count&quot;, 0) + 1
                            if trade[&quot;stagnation_count&quot;] &gt;= FAST_STAGNATION_CHECKS:
                                print(f&quot;[FAST STAGNANT] {symbol} - locking {favorable_r:.2f}R, no longer improving&quot;)
                                close_fast_position(symbol, &quot;Stagnant&quot;)
                                continue
                            else:
                                trade[&quot;next_check_ts&quot;] = now_ts + FAST_PROGRESS_CHECK_SECONDS
                                print(f&quot;[FAST PROGRESS] {symbol} stalled ({trade[&#x27;stagnation_count&#x27;]}/{FAST_STAGNATION_CHECKS}) - {favorable_r:.2f}R&quot;)
                    else:
                        print(f&quot;[FAST TIME EXIT] {symbol} - flat/dead ({favorable_r:.2f}R)&quot;)
                        close_fast_position(symbol, &quot;TimeExit&quot;)
                        continue
                else:
                    trade[&quot;next_check_ts&quot;] = now_ts + FAST_PROGRESS_CHECK_SECONDS
        except Exception as e:
            print(f&quot;[FAST TRACK ERROR] {symbol}: {e}&quot;)


def update_fast_trailing():
    for symbol in list(fast_open_trades.keys()):
        try:
            trade   = fast_open_trades[symbol]
            current = get_current_price(symbol)
            if current &lt;= 0:
                continue
            side = trade[&quot;side&quot;]
            entry_ref = trade.get(&quot;entry_fill&quot;, trade[&quot;entry&quot;])
            activate_pct = (FAST_TRAIL_ACTIVATE_RR * trade.get(&quot;sl_pct&quot;, 1.5)) / 100
            atr_ref    = trade.get(&quot;atr_at_entry&quot;, 0)
            trail_dist = atr_ref * FAST_TRAIL_ATR_MULT if atr_ref &gt; 0 else entry_ref * (FAST_TRAIL_PCT_FALLBACK / 100)

            if not trade.get(&quot;activated&quot;):
                if side == &quot;BUY&quot; and current &gt;= entry_ref * (1 + activate_pct):
                    fast_open_trades[symbol][&quot;activated&quot;]   = True
                    fast_open_trades[symbol][&quot;trail_price&quot;] = current
                elif side == &quot;SELL&quot; and current &lt;= entry_ref * (1 - activate_pct):
                    fast_open_trades[symbol][&quot;activated&quot;]   = True
                    fast_open_trades[symbol][&quot;trail_price&quot;] = current
                continue

            if side == &quot;BUY&quot;:
                if current &gt; trade[&quot;trail_price&quot;]:
                    fast_open_trades[symbol][&quot;trail_price&quot;] = current
                trail_sl = trade[&quot;trail_price&quot;] - trail_dist
                if current &lt;= trail_sl:
                    close_fast_position(symbol, &quot;Trail&quot;)
            else:
                if current &lt; trade[&quot;trail_price&quot;]:
                    fast_open_trades[symbol][&quot;trail_price&quot;] = current
                trail_sl = trade[&quot;trail_price&quot;] + trail_dist
                if current &gt;= trail_sl:
                    close_fast_position(symbol, &quot;Trail&quot;)
        except Exception as e:
            print(f&quot;[TRAIL ERROR] {symbol}: {e}&quot;)


# ==================== FAST SIGNAL ENTRY LOGIC ====================
def check_fast(symbol):
    try:
        candles = get_candles(symbol, limit=100, interval=FAST_TIMEFRAME)
        min_needed = max(FAST_CONSOL_LOOKBACK + FAST_VOL_LB + FAST_ATR_LEN + 5, FAST_EXTENSION_LOOKBACK + 5)
        if len(candles) &lt; min_needed:
            return

        confirmed = candles[:-1]
        closes = [cl(c) for c in confirmed]
        opens  = [o(c)  for c in confirmed]
        highs  = [h(c)  for c in confirmed]
        lows   = [l(c)  for c in confirmed]
        vols   = [v(c)  for c in confirmed]

        i = len(confirmed) - 1
        if i &lt; min_needed:
            return

        entry = closes[i]
        if entry &lt; MIN_PRICE:
            return

        atr_vals = atr_series(highs, lows, closes, FAST_ATR_LEN)
        atr_now  = atr_vals[i]

        ext_move  = abs(closes[i] - closes[i - FAST_EXTENSION_LOOKBACK])
        extension = ext_move / atr_now if atr_now &gt; 0 else 0
        is_extended = extension &gt; FAST_EXTENSION_LIMIT
        if extension &gt; FAST_EXTENSION_HARD_SKIP:
            print(f&quot;[FAST SKIP] {symbol} extension={extension:.1f}x ATR exceeds hard-skip limit ({FAST_EXTENSION_HARD_SKIP}x) - move too exhausted, no trade&quot;)
            return

        box_high = max(highs[i - FAST_CONSOL_LOOKBACK:i])
        box_low  = min(lows[i - FAST_CONSOL_LOOKBACK:i])

        avg_vol = sum(vols[i - FAST_VOL_LB:i]) / FAST_VOL_LB
        ratio   = vols[i] / avg_vol if avg_vol &gt; 0 else 0
        vol_ok  = ratio &gt;= FAST_VOL_MULT

        candle_range = highs[i] - lows[i]
        bull_close_strength = (closes[i] - lows[i]) / candle_range if candle_range &gt; 0 else 0
        bear_close_strength = (highs[i] - closes[i]) / candle_range if candle_range &gt; 0 else 0

        bull_breakout = (closes[i] &gt; box_high + atr_now * FAST_BREAKOUT_ATR_MULT
                         and bull_close_strength &gt;= FAST_CLOSE_POSITION_MIN)
        bear_breakout = (closes[i] &lt; box_low  - atr_now * FAST_BREAKOUT_ATR_MULT
                         and bear_close_strength &gt;= FAST_CLOSE_POSITION_MIN)

        long_signal  = bull_breakout and vol_ok
        short_signal = bear_breakout and vol_ok

        if not long_signal and not short_signal:
            return

        # ---- MTF trend filter (added 2026-07-11) - direction-only, no timing delay ----
        if MTF_FILTER_ENABLED and (long_signal or short_signal):
            mtf_trend = get_mtf_trend(symbol)
            if mtf_trend is not None:
                if long_signal and mtf_trend != &quot;UP&quot;:
                    print(f&quot;[MTF SKIP] {symbol} long blocked - {MTF_INTERVAL} trend is {mtf_trend}&quot;)
                    long_signal = False
                if short_signal and mtf_trend != &quot;DOWN&quot;:
                    print(f&quot;[MTF SKIP] {symbol} short blocked - {MTF_INTERVAL} trend is {mtf_trend}&quot;)
                    short_signal = False
        if not long_signal and not short_signal:
            return

        if symbol in fast_open_trades:
            current_side = fast_open_trades[symbol][&quot;side&quot;]
            if (current_side == &quot;BUY&quot; and short_signal) or (current_side == &quot;SELL&quot; and long_signal):
                print(f&quot;[FAST CLOSE - OPPOSITE] {symbol}&quot;)
                close_fast_position(symbol, &quot;Opposite&quot;)
            return

        sig_id_long  = (symbol, &quot;BUY&quot;,  int(confirmed[i][&quot;time&quot;]))
        sig_id_short = (symbol, &quot;SELL&quot;, int(confirmed[i][&quot;time&quot;]))

        if long_signal and sig_id_long not in fast_alerted:
            fast_alerted.add(sig_id_long)
            lev        = get_fast_leverage(symbol)
            sl_price   = round(box_low - atr_now * SL_ATR_BUFFER_MULT, 6)
            risk       = entry - sl_price
            if risk &lt;= 0:
                return
            tp1_price  = round(entry + risk * FAST_TP1_RR, 6)
            risk_usdt  = FAST_RISK_USDT * FAST_EXTENSION_MULT if is_extended else FAST_RISK_USDT
            ext_tag    = &quot;extended (half size)&quot; if is_extended else &quot;fresh move&quot;
            print(f&quot;[FAST] {symbol} BUY | vol={ratio:.1f}x | lev={lev}x | {ext_tag} | ext={extension:.1f}x ATR&quot;)
            trade_status = &quot;&quot;
            if fast_auto_trade_enabled and len(fast_open_trades) &gt;= FAST_MAX_CONCURRENT_TRADES:
                trade_status = &quot;\nSkipped - max concurrent trades (&quot; + str(FAST_MAX_CONCURRENT_TRADES) + &quot;) reached&quot;
            elif fast_auto_trade_enabled:
                oid = place_fast_order(symbol, &quot;BUY&quot;, entry, sl_price, tp1_price, atr_now, risk_usdt)
                if oid == &quot;MARGIN_SKIP&quot;:
                    trade_status = &quot;\nSkipped - insufficient margin&quot;
                elif oid and oid != &quot;N/A&quot;:
                    trade_status = &quot;\nOrder: &quot; + str(oid)
                else:
                    trade_status = &quot;\nOrder failed&quot;
            else:
                trade_status = &quot;\nAuto-trade OFF&quot;
            send_tg(
                &quot;FAST SIGNAL - BUY - &quot; + symbol + &quot;\n------------------------------\n&quot;
                &quot;Entry: &quot; + str(round(entry, 6)) + &quot; | SL: &quot; + str(sl_price) + &quot;\n&quot;
                &quot;TP1 (&quot; + str(FAST_TP1_RR) + &quot;R): &quot; + str(tp1_price) + &quot; | ATR-trail after &quot; + str(FAST_TRAIL_ACTIVATE_RR) + &quot;R\n&quot;
                &quot;Breakout vol: &quot; + str(round(ratio, 1)) + &quot;x | Close-strength: &quot; + str(round(bull_close_strength, 2)) +
                &quot; | Lev: &quot; + str(lev) + &quot;x | &quot; + ext_tag + &quot; | Risk: $&quot; + str(risk_usdt) +
                trade_status + &quot;\n------------------------------\nNiti Fast Signal&quot;
            )

        elif short_signal and sig_id_short not in fast_alerted:
            fast_alerted.add(sig_id_short)
            lev        = get_fast_leverage(symbol)
            sl_price   = round(box_high + atr_now * SL_ATR_BUFFER_MULT, 6)
            risk       = sl_price - entry
            if risk &lt;= 0:
                return
            tp1_price  = round(entry - risk * FAST_TP1_RR, 6)
            risk_usdt  = FAST_RISK_USDT * FAST_EXTENSION_MULT if is_extended else FAST_RISK_USDT
            ext_tag    = &quot;extended (half size)&quot; if is_extended else &quot;fresh move&quot;
            print(f&quot;[FAST] {symbol} SELL | vol={ratio:.1f}x | lev={lev}x | {ext_tag} | ext={extension:.1f}x ATR&quot;)
            trade_status = &quot;&quot;
            if fast_auto_trade_enabled and len(fast_open_trades) &gt;= FAST_MAX_CONCURRENT_TRADES:
                trade_status = &quot;\nSkipped - max concurrent trades (&quot; + str(FAST_MAX_CONCURRENT_TRADES) + &quot;) reached&quot;
            elif fast_auto_trade_enabled:
                oid = place_fast_order(symbol, &quot;SELL&quot;, entry, sl_price, tp1_price, atr_now, risk_usdt)
                if oid == &quot;MARGIN_SKIP&quot;:
                    trade_status = &quot;\nSkipped - insufficient margin&quot;
                elif oid and oid != &quot;N/A&quot;:
                    trade_status = &quot;\nOrder: &quot; + str(oid)
                else:
                    trade_status = &quot;\nOrder failed&quot;
            else:
                trade_status = &quot;\nAuto-trade OFF&quot;
            send_tg(
                &quot;FAST SIGNAL - SELL - &quot; + symbol + &quot;\n------------------------------\n&quot;
                &quot;Entry: &quot; + str(round(entry, 6)) + &quot; | SL: &quot; + str(sl_price) + &quot;\n&quot;
                &quot;TP1 (&quot; + str(FAST_TP1_RR) + &quot;R): &quot; + str(tp1_price) + &quot; | ATR-trail after &quot; + str(FAST_TRAIL_ACTIVATE_RR) + &quot;R\n&quot;
                &quot;Breakout vol: &quot; + str(round(ratio, 1)) + &quot;x | Close-strength: &quot; + str(round(bear_close_strength, 2)) +
                &quot; | Lev: &quot; + str(lev) + &quot;x | &quot; + ext_tag + &quot; | Risk: $&quot; + str(risk_usdt) +
                trade_status + &quot;\n------------------------------\nNiti Fast Signal&quot;
            )

    except Exception as e:
        print(f&quot;[FAST {symbol}] error: {e}&quot;)


# ==================== TIGHT ENTRY LOGIC (Stock Niti: spike -&gt; cooldown -&gt; breakout) ====================
def get_tight_volume_baseline(symbol):
    &quot;&quot;&quot;Cached 3-day average 1m volume per symbol, refreshed every
    TIGHT_BASELINE_REFRESH_SECONDS rather than on every scan (expensive call).

    Paginated (fixed 2026-07-11): BingX&#x27;s kline endpoint caps `limit` at 1440 per
    request (confirmed via a live 109400 error - the original single-call
    limit=4320 request was failing on every symbol, every cycle, which is why the
    baseline was always None and Tight never watched anything). This now fetches
    TIGHT_BASELINE_CANDLES worth of history in &lt;=1440-candle chunks, walking
    backwards with `endTime`, with a small sleep between chunks to avoid bursting.&quot;&quot;&quot;
    now = time.time()
    cached = tight_baseline_cache.get(symbol)
    if cached and now - cached[&quot;ts&quot;] &lt; TIGHT_BASELINE_REFRESH_SECONDS:
        return cached[&quot;baseline&quot;]
    try:
        all_vols   = []
        end_time   = None
        remaining  = TIGHT_BASELINE_CANDLES
        first_chunk = True
        while remaining &gt; 0:
            chunk_limit = min(remaining, BINGX_KLINE_MAX_LIMIT)
            candles = get_candles(symbol, limit=chunk_limit, interval=TIGHT_TIMEFRAME, end_time=end_time)
            if not candles:
                break
            # exclude the live in-progress candle - only present in the first
            # (most recent, end_time=None) chunk
            chunk = candles[:-1] if first_chunk else candles
            all_vols.extend(v(c) for c in chunk)
            end_time = int(candles[0][&quot;time&quot;]) - 1
            remaining -= len(candles)
            first_chunk = False
            if len(candles) &lt; chunk_limit:
                break   # exchange returned fewer than asked - no more history available
            time.sleep(0.3)
        if len(all_vols) &lt; 100:
            return cached[&quot;baseline&quot;] if cached else None
        baseline = sum(all_vols) / len(all_vols)
        tight_baseline_cache[symbol] = {&quot;baseline&quot;: baseline, &quot;ts&quot;: now}
        return baseline
    except Exception as e:
        print(f&quot;[TIGHT BASELINE ERROR] {symbol}: {e}&quot;)
        return cached[&quot;baseline&quot;] if cached else None


def place_tight_order(symbol, side, entry, sl, tp):
    try:
        set_leverage_api(symbol, TIGHT_LEVERAGE)
        precision = symbol_precision.get(symbol, 4)
        risk_dist = abs(entry - sl)
        if risk_dist &lt;= 0:
            return None
        # Risk-based sizing: qty is set so that hitting SL always loses ~TIGHT_RISK_USDT,
        # regardless of how tight or wide the SL distance is for this particular setup.
        risk_qty       = TIGHT_RISK_USDT / risk_dist
        margin_cap_qty = (TIGHT_MAX_MARGIN_USDT * TIGHT_LEVERAGE) / entry
        total_qty = round(min(risk_qty, margin_cap_qty), precision)
        if total_qty &lt;= 0:
            return None
        if risk_qty &gt; margin_cap_qty:
            print(f&quot;[TIGHT SIZE CAP] {symbol} SL too tight for full risk qty, margin-capped at ${TIGHT_MAX_MARGIN_USDT}&quot;)
        pos_side   = &quot;LONG&quot; if side == &quot;BUY&quot; else &quot;SHORT&quot;
        close_side = &quot;SELL&quot; if side == &quot;BUY&quot; else &quot;BUY&quot;

        # Pre-trade margin check: skip cleanly instead of firing an order BingX will
        # reject with 101204. None (endpoint failure) = unknown -&gt; proceed.
        required_margin = total_qty * entry / TIGHT_LEVERAGE
        avail = get_available_margin()
        if avail is not None and avail &lt; required_margin * 1.05:
            print(f&quot;[TIGHT MARGIN SKIP] {symbol} need ~${required_margin:.2f}, available ${avail:.2f} - skipping&quot;)
            return &quot;MARGIN_SKIP&quot;

        half_qty = round(total_qty / 2, precision)

        order_id = place_market_order(symbol, side, total_qty, pos_side)
        print(f&quot;[TIGHT ORDER] {symbol} {side} qty={total_qty} risk=${TIGHT_RISK_USDT}: {order_id}&quot;)
        if order_id != &quot;N/A&quot;:
            time.sleep(0.5)
            entry_fill = get_fill_price(order_id, symbol, fallback=entry)

            # Recompute risk distance and BOTH take-profit levels from the REAL fill
            # price, not the nominal signal price. Otherwise slippage compresses the
            # actual R-multiples: &quot;TP1 at 2R&quot; measured from a worse real entry can be
            # only ~0.5-1R away in reality (confirmed 2026-07-15: Trail wins of
            # +$0.68-$2 against -$5-6 SLs). SL itself stays structural (unchanged).
            risk_dist = abs(entry_fill - sl)
            if risk_dist &lt;= 0:
                risk_dist = abs(entry - sl)
            if side == &quot;BUY&quot;:
                tp1_price = round(entry_fill + risk_dist * TIGHT_BE_TRIGGER_R, precision)
                tp        = round(entry_fill + risk_dist * TIGHT_RR_TP, precision)
            else:
                tp1_price = round(entry_fill - risk_dist * TIGHT_BE_TRIGGER_R, precision)
                tp        = round(entry_fill - risk_dist * TIGHT_RR_TP, precision)

            # SL first, guarded: if it can&#x27;t be placed even on retry, close the
            # position immediately instead of leaving it naked.
            sl_id = place_sl_guarded(symbol, close_side, pos_side, sl, total_qty)
            if sl_id is None:
                print(f&quot;[TIGHT SL GUARD] {symbol} SL placement failed twice - emergency closing position&quot;)
                place_market_order(symbol, close_side, total_qty, pos_side)
                send_tg(f&quot;⚠️ TIGHT {symbol}: SL placement failed - position emergency-closed for safety&quot;)
                return None
            tp1_id = place_tp_order(symbol, close_side, pos_side, tp1_price, half_qty)
            tp_id  = place_tp_order(symbol, close_side, pos_side, tp, total_qty - half_qty)
            tight_open_trades[str(order_id)] = {
                &quot;symbol&quot;: symbol, &quot;side&quot;: side, &quot;entry&quot;: entry, &quot;entry_fill&quot;: entry_fill, &quot;sl&quot;: sl, &quot;tp&quot;: tp,
                &quot;total_qty&quot;: total_qty, &quot;half_qty&quot;: half_qty, &quot;remaining_qty&quot;: total_qty - half_qty,
                &quot;tp1&quot;: tp1_price, &quot;tp1_id&quot;: tp1_id, &quot;tp1_filled&quot;: False,
                &quot;sl_id&quot;: sl_id, &quot;tp_id&quot;: tp_id,
                &quot;close_side&quot;: close_side, &quot;pos_side&quot;: pos_side,
                &quot;risk_dist&quot;: risk_dist, &quot;be_done&quot;: False,
                &quot;trail_price&quot;: None, &quot;be_price&quot;: None,
                &quot;time&quot;: datetime.now(timezone.utc).strftime(&quot;%H:%M UTC&quot;),
            }
        return order_id
    except Exception as e:
        print(f&quot;[TIGHT ORDER ERROR] {symbol}: {e}&quot;)
        return None


def check_tight_symbol(symbol):
    try:
        baseline = get_tight_volume_baseline(symbol)
        if not baseline or baseline &lt;= 0:
            return

        candles = get_candles(symbol, limit=30, interval=TIGHT_TIMEFRAME)
        if len(candles) &lt; 20:
            return
        live_candle = candles[-1]      # in-progress
        confirmed   = candles[:-1]     # closed candles

        live_vol   = v(live_candle)
        live_ratio = live_vol / baseline

        state = tight_watch.get(symbol)

        # ---- Not yet watching: look for the initial spike trigger ----
        if state is None:
            if live_ratio &gt;= TIGHT_SPIKE_VOL_MULT:
                direction = &quot;UP&quot; if cl(live_candle) &gt; o(live_candle) else &quot;DOWN&quot;
                tight_watch[symbol] = {
                    &quot;state&quot;: &quot;COOLDOWN&quot;, &quot;direction&quot;: direction,
                    &quot;spike_ts&quot;: time.time(),
                    &quot;cooldown_highs&quot;: [], &quot;cooldown_lows&quot;: [],
                    &quot;last_processed_time&quot;: None,
                }
                send_tg(
                    &quot;TIGHT SPIKE - &quot; + symbol + &quot; [&quot; + direction + &quot;]\n&quot;
                    &quot;Vol: &quot; + str(round(live_ratio, 1)) + &quot;x 3-day baseline\n&quot;
                    &quot;Watching for cooldown + re-entry...\nNiti Tight&quot;
                )
            return

        # ---- Give up watching if it&#x27;s been too long ----
        if time.time() - state[&quot;spike_ts&quot;] &gt; TIGHT_MAX_COOLDOWN_WAIT_SECONDS:
            tight_watch.pop(symbol, None)
            return

        if state[&quot;state&quot;] == &quot;COOLDOWN&quot;:
            if not confirmed:
                return
            last_confirmed = confirmed[-1]
            if state.get(&quot;last_processed_time&quot;) == last_confirmed[&quot;time&quot;]:
                return   # already processed this closed candle
            state[&quot;last_processed_time&quot;] = last_confirmed[&quot;time&quot;]

            confirmed_ratio = v(last_confirmed) / baseline
            if confirmed_ratio &lt; TIGHT_COOLDOWN_VOL_RATIO:
                state[&quot;cooldown_highs&quot;].append(h(last_confirmed))
                state[&quot;cooldown_lows&quot;].append(l(last_confirmed))
            else:
                # volume picked back up before a range was confirmed - restart the range build
                state[&quot;cooldown_highs&quot;] = []
                state[&quot;cooldown_lows&quot;]  = []

            if len(state[&quot;cooldown_highs&quot;]) &gt;= TIGHT_COOLDOWN_MIN_CANDLES:
                recent_highs = state[&quot;cooldown_highs&quot;][-TIGHT_COOLDOWN_MIN_CANDLES:]
                recent_lows  = state[&quot;cooldown_lows&quot;][-TIGHT_COOLDOWN_MIN_CANDLES:]
                range_high = max(recent_highs)
                range_low  = min(recent_lows)

                closes_r = [cl(c) for c in confirmed[-30:]]
                highs_r  = [h(c)  for c in confirmed[-30:]]
                lows_r   = [l(c)  for c in confirmed[-30:]]
                atr_vals = atr_series(highs_r, lows_r, closes_r, TIGHT_ATR_LEN)
                atr_now  = atr_vals[-1] if atr_vals else (range_high - range_low)

                range_width = range_high - range_low
                if atr_now &gt; 0 and range_width &lt;= atr_now * TIGHT_RANGE_MAX_ATR_MULT:
                    state[&quot;state&quot;]      = &quot;READY&quot;
                    state[&quot;range_high&quot;] = range_high
                    state[&quot;range_low&quot;]  = range_low
                    state[&quot;atr_now&quot;]    = atr_now
                # else: too wide to call &quot;sideways&quot; (likely slow-bleed, not real
                # consolidation) - keep accumulating candles and re-check next time

        elif state[&quot;state&quot;] == &quot;READY&quot;:
            range_high = state[&quot;range_high&quot;]
            range_low  = state[&quot;range_low&quot;]
            live_close = cl(live_candle)

            breakout_up   = live_close &gt; range_high and live_ratio &gt;= TIGHT_REENTRY_VOL_MULT
            breakout_down = live_close &lt; range_low  and live_ratio &gt;= TIGHT_REENTRY_VOL_MULT

            if not breakout_up and not breakout_down:
                return

            if len(tight_open_trades) &gt;= TIGHT_MAX_CONCURRENT_TRADES:
                print(f&quot;[TIGHT SKIP] {symbol} breakout ignored - max concurrent trades ({TIGHT_MAX_CONCURRENT_TRADES}) reached&quot;)
                tight_watch.pop(symbol, None)
                return

            side     = &quot;BUY&quot; if breakout_up else &quot;SELL&quot;
            entry    = live_close
            atr_now  = state[&quot;atr_now&quot;]
            if side == &quot;BUY&quot;:
                sl = range_low - atr_now * TIGHT_SL_ATR_BUFFER_MULT
            else:
                sl = range_high + atr_now * TIGHT_SL_ATR_BUFFER_MULT
            risk = abs(entry - sl)
            tight_watch.pop(symbol, None)
            if risk &lt;= 0:
                return
            tp = round(entry + risk * TIGHT_RR_TP, 6) if side == &quot;BUY&quot; else round(entry - risk * TIGHT_RR_TP, 6)
            sl = round(sl, 6)

            trade_status = &quot;&quot;
            if tight_auto_trade_enabled:
                oid = place_tight_order(symbol, side, entry, sl, tp)
                if oid == &quot;MARGIN_SKIP&quot;:
                    trade_status = &quot;\nSkipped - insufficient margin&quot;
                elif oid and oid != &quot;N/A&quot;:
                    trade_status = &quot;\nOrder: &quot; + str(oid)
                else:
                    trade_status = &quot;\nOrder failed&quot;
            else:
                trade_status = &quot;\nAuto-trade OFF&quot;
            send_tg(
                &quot;TIGHT SIGNAL - &quot; + side + &quot; - &quot; + symbol + &quot;\n------------------------------\n&quot;
                &quot;Entry: &quot; + str(round(entry, 6)) + &quot; | SL: &quot; + str(sl) + &quot; | TP (1:&quot; + str(TIGHT_RR_TP) + &quot;): &quot; + str(tp) + &quot;\n&quot;
                &quot;Breakout vol: &quot; + str(round(live_ratio, 1)) + &quot;x | BE at 1:&quot; + str(TIGHT_BE_TRIGGER_R) + &quot;R&quot; +
                trade_status + &quot;\n------------------------------\nNiti Tight&quot;
            )
    except Exception as e:
        print(f&quot;[TIGHT {symbol}] error: {e}&quot;)


TIGHT_TRAIL_R_MULT = float(os.environ.get(&quot;TIGHT_TRAIL_R_MULT&quot;, 1.0))  # trailing SL stays this many R behind the best price, once TP1 is taken

def track_tight_trades():
    for oid in list(tight_open_trades.keys()):
        trade = tight_open_trades.get(oid)
        if not trade:
            continue
        try:
            symbol    = trade[&quot;symbol&quot;]
            risk_dist = trade.get(&quot;risk_dist&quot;, 0)

            # ---- TP1 (half) filled at 2R: book real profit, move remaining half to
            # breakeven, then start trailing its SL toward the final 4R target ----
            if not trade.get(&quot;tp1_filled&quot;) and trade.get(&quot;tp1_id&quot;):
                status = check_order_status(trade[&quot;tp1_id&quot;], symbol)
                if status == &quot;FILLED&quot;:
                    trade[&quot;tp1_filled&quot;] = True
                    entry_ref = trade.get(&quot;entry_fill&quot;, trade[&quot;entry&quot;])
                    tp1_fill  = get_fill_price(trade[&quot;tp1_id&quot;], symbol, fallback=trade[&quot;tp1&quot;])
                    trade[&quot;tp1_fill&quot;] = tp1_fill
                    leg_pnl = (tp1_fill - entry_ref) * trade[&quot;half_qty&quot;] if trade[&quot;side&quot;] == &quot;BUY&quot; else (entry_ref - tp1_fill) * trade[&quot;half_qty&quot;]
                    trade[&quot;tp1_pnl&quot;] = round(leg_pnl, 2)
                    if trade.get(&quot;sl_id&quot;):
                        cancel_order(symbol, trade[&quot;sl_id&quot;])
                    # True breakeven = the REAL fill price, not the nominal signal price
                    new_sl_id = place_sl_order(symbol, trade[&quot;close_side&quot;], trade[&quot;pos_side&quot;], entry_ref, trade[&quot;remaining_qty&quot;])
                    trade[&quot;sl_id&quot;]      = new_sl_id
                    trade[&quot;sl&quot;]         = entry_ref
                    trade[&quot;be_price&quot;]   = entry_ref
                    trade[&quot;be_done&quot;]    = True
                    trade[&quot;trail_price&quot;] = entry_ref
                    print(f&quot;[TIGHT TP1] {symbol} - half closed at {tp1_fill}, remaining {trade[&#x27;remaining_qty&#x27;]} moved to BE ({entry_ref})&quot;)

            # ---- Trail the remaining half&#x27;s SL once TP1 is banked ----
            if trade.get(&quot;tp1_filled&quot;) and risk_dist &gt; 0:
                current = get_current_price(symbol)
                if current &gt; 0:
                    trail_dist = risk_dist * TIGHT_TRAIL_R_MULT
                    if trade[&quot;side&quot;] == &quot;BUY&quot;:
                        if current &gt; trade[&quot;trail_price&quot;]:
                            trade[&quot;trail_price&quot;] = current
                            new_sl = round(trade[&quot;trail_price&quot;] - trail_dist, symbol_precision.get(symbol, 4))
                            if new_sl &gt; trade[&quot;sl&quot;]:
                                if trade.get(&quot;sl_id&quot;):
                                    cancel_order(symbol, trade[&quot;sl_id&quot;])
                                trade[&quot;sl_id&quot;] = place_sl_order(symbol, trade[&quot;close_side&quot;], trade[&quot;pos_side&quot;], new_sl, trade[&quot;remaining_qty&quot;])
                                trade[&quot;sl&quot;] = new_sl
                    else:
                        if current &lt; trade[&quot;trail_price&quot;]:
                            trade[&quot;trail_price&quot;] = current
                            new_sl = round(trade[&quot;trail_price&quot;] + trail_dist, symbol_precision.get(symbol, 4))
                            if new_sl &lt; trade[&quot;sl&quot;]:
                                if trade.get(&quot;sl_id&quot;):
                                    cancel_order(symbol, trade[&quot;sl_id&quot;])
                                trade[&quot;sl_id&quot;] = place_sl_order(symbol, trade[&quot;close_side&quot;], trade[&quot;pos_side&quot;], new_sl, trade[&quot;remaining_qty&quot;])
                                trade[&quot;sl&quot;] = new_sl

            sl_status = check_order_status(trade[&quot;sl_id&quot;], symbol) if trade.get(&quot;sl_id&quot;) else &quot;&quot;
            if sl_status == &quot;FILLED&quot;:
                if trade.get(&quot;tp_id&quot;):
                    cancel_order(symbol, trade[&quot;tp_id&quot;])
                entry_ref = trade.get(&quot;entry_fill&quot;, trade[&quot;entry&quot;])
                sl_fill   = get_fill_price(trade[&quot;sl_id&quot;], symbol, fallback=trade[&quot;sl&quot;])
                if trade.get(&quot;tp1_filled&quot;):
                    result   = &quot;Trail&quot; if trade[&quot;sl&quot;] != trade.get(&quot;be_price&quot;, trade[&quot;entry&quot;]) else &quot;BE&quot;
                    rem_qty  = trade[&quot;remaining_qty&quot;]
                    leg_pnl  = (sl_fill - entry_ref) * rem_qty if trade[&quot;side&quot;] == &quot;BUY&quot; else (entry_ref - sl_fill) * rem_qty
                    trade[&quot;pnl&quot;] = round(trade.get(&quot;tp1_pnl&quot;, 0) + leg_pnl, 2)
                else:
                    result = &quot;SL&quot;
                    leg_pnl = (sl_fill - entry_ref) * trade[&quot;total_qty&quot;] if trade[&quot;side&quot;] == &quot;BUY&quot; else (entry_ref - sl_fill) * trade[&quot;total_qty&quot;]
                    trade[&quot;pnl&quot;] = round(leg_pnl, 2)
                trade[&quot;result&quot;] = result
                trade[&quot;label&quot;]  = &quot;Tight&quot;
                daily_trades.append(trade)
                journal_closed_trade(trade)
                tight_open_trades.pop(oid, None)
                continue

            tp_status = check_order_status(trade[&quot;tp_id&quot;], symbol) if trade.get(&quot;tp_id&quot;) else &quot;&quot;
            if tp_status == &quot;FILLED&quot;:
                if trade.get(&quot;sl_id&quot;):
                    cancel_order(symbol, trade[&quot;sl_id&quot;])
                entry_ref = trade.get(&quot;entry_fill&quot;, trade[&quot;entry&quot;])
                tp_fill   = get_fill_price(trade[&quot;tp_id&quot;], symbol, fallback=trade[&quot;tp&quot;])
                rem_qty = trade[&quot;remaining_qty&quot;] if trade.get(&quot;tp1_filled&quot;) else trade[&quot;total_qty&quot;]
                leg_pnl = (tp_fill - entry_ref) * rem_qty if trade[&quot;side&quot;] == &quot;BUY&quot; else (entry_ref - tp_fill) * rem_qty
                trade[&quot;pnl&quot;]    = round(trade.get(&quot;tp1_pnl&quot;, 0) + leg_pnl, 2)
                trade[&quot;result&quot;] = &quot;TP&quot;
                trade[&quot;label&quot;]  = &quot;Tight&quot;
                daily_trades.append(trade)
                journal_closed_trade(trade)
                tight_open_trades.pop(oid, None)
                continue
        except Exception as e:
            print(f&quot;[TIGHT TRACK ERROR] {oid}: {e}&quot;)


def send_daily_summary():
    global daily_trades, last_summary_date
    nzt   = timezone(timedelta(hours=12))
    now   = datetime.now(nzt)
    today = now.date()
    if last_summary_date == today:
        return
    if now.hour != 23 or now.minute &lt; 55:
        return
    last_summary_date = today
    total_pnl = round(sum(t.get(&quot;pnl&quot;, 0) for t in daily_trades), 2)
    wins      = sum(1 for t in daily_trades if t.get(&quot;pnl&quot;, 0) &gt; 0)
    losses    = sum(1 for t in daily_trades if t.get(&quot;pnl&quot;, 0) &lt;= 0)
    total     = len(daily_trades)
    win_rate  = round(wins / total * 100, 1) if total &gt; 0 else 0
    sign      = &quot;+&quot; if total_pnl &gt; 0 else &quot;&quot;
    lines = [&quot;Daily Summary - &quot; + today.strftime(&quot;%b %d, %Y&quot;), &quot;------------------------------&quot;]
    for idx, t in enumerate(daily_trades, 1):
        p  = t.get(&quot;pnl&quot;, 0)
        ps = &quot;+&quot; if p &gt; 0 else &quot;&quot;
        lines.append(str(idx) + &quot;. [&quot; + t.get(&quot;label&quot;, &quot;?&quot;) + &quot;] &quot; + t[&quot;symbol&quot;] + &quot; &quot; + t[&quot;side&quot;] + &quot; | &quot; + t.get(&quot;result&quot;, &quot;?&quot;) + &quot; | &quot; + ps + str(p) + &quot; USDT&quot;)
    lines.append(&quot;------------------------------&quot;)
    lines.append(&quot;Trades: &quot; + str(total) + &quot; (&quot; + str(wins) + &quot;W/&quot; + str(losses) + &quot;L) | WR: &quot; + str(win_rate) + &quot;%&quot;)
    lines.append(&quot;Total PnL: &quot; + sign + str(total_pnl) + &quot; USDT&quot;)
    lines.append(&quot;------------------------------\nNiti Journal&quot;)
    send_journal(&quot;\n&quot;.join(lines))
    daily_trades = []


# ==================== TELEGRAM COMMANDS ====================
def handle_telegram_commands():
    global tight_auto_trade_enabled, fast_auto_trade_enabled
    offset = None
    # On startup, discard any backlog of old pending updates (e.g. a /start sent
    # before a previous crash/redeploy) so they don&#x27;t get silently replayed and
    # flip auto-trade ON without a fresh command from Faisal.
    try:
        flush = requests.get(f&quot;https://api.telegram.org/bot{TG_TOKEN}/getUpdates&quot;, params={&quot;timeout&quot;: 0}, timeout=10).json()
        pending = flush.get(&quot;result&quot;, [])
        if pending:
            offset = pending[-1][&quot;update_id&quot;] + 1
            requests.get(f&quot;https://api.telegram.org/bot{TG_TOKEN}/getUpdates&quot;, params={&quot;offset&quot;: offset, &quot;timeout&quot;: 0}, timeout=10)
            print(f&quot;[TG CMD] Flushed {len(pending)} stale pending update(s) on startup&quot;)
    except Exception as e:
        print(f&quot;[TG CMD] startup flush error: {e}&quot;)
    while True:
        try:
            url    = f&quot;https://api.telegram.org/bot{TG_TOKEN}/getUpdates&quot;
            params = {&quot;timeout&quot;: 30}
            if offset:
                params[&quot;offset&quot;] = offset
            r = requests.get(url, params=params, timeout=35).json()
            for update in r.get(&quot;result&quot;, []):
                offset  = update[&quot;update_id&quot;] + 1
                msg     = update.get(&quot;message&quot;, {})
                text    = msg.get(&quot;text&quot;, &quot;&quot;).strip().lower()
                chat_id = str(msg.get(&quot;chat&quot;, {}).get(&quot;id&quot;, &quot;&quot;))
                if chat_id != str(TG_CHAT_ID):
                    continue
                if text == &quot;/start&quot;:
                    tight_auto_trade_enabled = True
                    send_tg(&quot;Tight Auto-trade ON.&quot;)
                elif text == &quot;/stop&quot;:
                    tight_auto_trade_enabled = False
                    send_tg(&quot;Tight Auto-trade OFF.&quot;)
                elif text == &quot;/status&quot;:
                    t = &quot;ON&quot; if tight_auto_trade_enabled else &quot;OFF&quot;
                    f = &quot;ON&quot; if fast_auto_trade_enabled  else &quot;OFF&quot;
                    backoff = &quot;&quot;
                    if api_backoff_active():
                        backoff = f&quot;\nAPI BACKOFF ACTIVE - {int(_api_backoff_until - time.time())}s remaining&quot;
                    send_tg(&quot;Tight: &quot; + t + &quot; | Watching: &quot; + str(len(tight_watch)) + &quot; | Open: &quot; + str(len(tight_open_trades)) +
                            &quot;\nFast Signal: &quot; + f + &quot; | Open: &quot; + str(len(fast_open_trades)) + backoff)
                elif text == &quot;/fast_start&quot;:
                    fast_auto_trade_enabled = True
                    send_tg(&quot;Fast Signal Auto-trade ON.&quot;)
                elif text == &quot;/fast_stop&quot;:
                    fast_auto_trade_enabled = False
                    send_tg(&quot;Fast Signal Auto-trade OFF.&quot;)
                elif text == &quot;/fast_status&quot;:
                    f = &quot;ON&quot; if fast_auto_trade_enabled else &quot;OFF&quot;
                    send_tg(&quot;Fast Signal: &quot; + f)
        except Exception as e:
            print(f&quot;[TG CMD] error: {e}&quot;)
        time.sleep(1)


def trailing_loop():
    while True:
        try:
            if fast_open_trades:
                track_fast_trades()
                update_fast_trailing()
            if tight_open_trades:
                track_tight_trades()
        except Exception as e:
            print(f&quot;[TRAIL LOOP ERROR] {e}&quot;)
        time.sleep(30)


def fast_diagnostic_check():
    &quot;&quot;&quot;Prints a sample calculation for a known-liquid coin every cycle, so we can see
    in the logs whether the data pipeline (candles -&gt; ratios) is actually alive,
    independent of whether a real signal happens to fire.&quot;&quot;&quot;
    try:
        candles = get_candles(&quot;BTC-USDT&quot;, limit=100, interval=FAST_TIMEFRAME)
        if len(candles) &lt; 60:
            print(f&quot;[FAST DIAG] BTC-USDT - only got {len(candles)} candles (need ~60+)&quot;)
            return
        confirmed = candles[:-1]
        closes = [cl(c) for c in confirmed]
        highs  = [h(c) for c in confirmed]
        lows   = [l(c) for c in confirmed]
        vols   = [v(c) for c in confirmed]
        i = len(confirmed) - 1
        avg_vol = sum(vols[i - FAST_VOL_LB:i]) / FAST_VOL_LB
        ratio = vols[i] / avg_vol if avg_vol &gt; 0 else 0
        box_high = max(highs[i - FAST_CONSOL_LOOKBACK:i])
        box_low  = min(lows[i - FAST_CONSOL_LOOKBACK:i])
        print(f&quot;[FAST DIAG] BTC-USDT close={closes[i]} vol_ratio={ratio:.2f}x box=({box_low},{box_high}) candles={len(candles)}&quot;)
    except Exception as e:
        print(f&quot;[FAST DIAG ERROR] {e}&quot;)


def fast_scan_loop():
    print(f&quot;Fast Signal loop started - {FAST_TIMEFRAME} | consolidation breakout | MTF filter + reworked time-exit (2026-07-11)&quot;)
    all_symbols = []
    while True:
        try:
            if not all_symbols:
                all_symbols = get_futures_symbols() or []
            liquid = get_liquid_symbols(
                all_symbols, min_quote_vol=FAST_MIN_QUOTE_VOL, max_n=None, exclude_top_n=FAST_EXCLUDE_TOP_N
            )
            print(f&quot;[FAST SCAN] Scanning {len(liquid)} small/mid-cap liquid pairs for breakouts...&quot;)
            fast_diagnostic_check()
            for sym in liquid:
                check_fast(sym)
                time.sleep(0.15)
            print(f&quot;[FAST SCAN] Done. Sleeping {FAST_SCAN_INTERVAL_SECONDS}s...&quot;)
        except Exception as e:
            print(f&quot;[FAST LOOP ERROR] {e}&quot;)
        time.sleep(FAST_SCAN_INTERVAL_SECONDS)


def tight_diagnostic_check():
    &quot;&quot;&quot;Prints BTC-USDT&#x27;s live volume ratio vs its 3-day baseline every cycle, so we
    can see in the logs whether the baseline fetch (limit=TIGHT_BASELINE_CANDLES)
    and the live-candle ratio calculation are actually working, independent of
    whether a real 20x spike happens to occur.&quot;&quot;&quot;
    try:
        baseline = get_tight_volume_baseline(&quot;BTC-USDT&quot;)
        if not baseline:
            print(&quot;[TIGHT DIAG] BTC-USDT - baseline is None/empty, see CANDLES ERROR/SHORT logs above&quot;)
            return
        candles = get_candles(&quot;BTC-USDT&quot;, limit=5, interval=TIGHT_TIMEFRAME)
        if len(candles) &lt; 2:
            print(f&quot;[TIGHT DIAG] BTC-USDT - only got {len(candles)} recent candles&quot;)
            return
        live_ratio = v(candles[-1]) / baseline
        print(f&quot;[TIGHT DIAG] BTC-USDT baseline_vol={baseline:.2f} live_vol={v(candles[-1]):.2f} ratio={live_ratio:.2f}x (need {TIGHT_SPIKE_VOL_MULT}x)&quot;)
    except Exception as e:
        print(f&quot;[TIGHT DIAG ERROR] {e}&quot;)


def tight_scan_loop():
    print(&quot;Tight loop started - Stock Niti strategy: 1m volume-spike -&gt; cooldown -&gt; breakout re-entry (2026-07-11)&quot;)
    all_symbols = []
    while True:
        try:
            if not all_symbols:
                all_symbols = get_futures_symbols() or []
            symbols = get_liquid_symbols(all_symbols, min_quote_vol=TIGHT_MIN_QUOTE_VOL, max_n=TIGHT_MAX_SYMBOLS)
            print(f&quot;[TIGHT SCAN] Scanning {len(symbols)}/{len(all_symbols)} liquid pairs | Watching={len(tight_watch)} | Open={len(tight_open_trades)} | Auto={tight_auto_trade_enabled}&quot;)
            tight_diagnostic_check()
            for sym in symbols:
                check_tight_symbol(sym)
                time.sleep(0.15)
            send_daily_summary()
            print(&quot;[TIGHT SCAN] Done.&quot;)
        except Exception as e:
            print(f&quot;[TIGHT LOOP ERROR] {e}&quot;)
        time.sleep(TIGHT_SCAN_INTERVAL_SECONDS)


@app.route(&quot;/&quot;)
def health():
    return &quot;Niti Tight (Stock Niti: Vol-Spike+Cooldown+Breakout, 1:4RR) + Fast Signal (Breakout+ATRTrail+ReworkedTimeExit+MTF)&quot;, 200




# ==================== COMBINED PATCH: TIGHT 2 SPEED FIX + FAST RISK FIX (2026-07-11) ====================
# Everything below OVERRIDES the earlier definitions (Python uses the last def).
# Fixes: (1) baseline fetch moved to a background thread so a scan pass drops
# from 15-20 min to ~90s, (2) spike detection also checks the last 3 CLOSED
# candles so spikes finishing between passes aren&#x27;t missed, (3) cooldown now
# catches up on ALL closed candles since the last pass, (4) breakout check does
# the same. Strategy rules (20x spike / 5x re-entry / 1:4 / BE at 2R) unchanged.

# ---- FAST SIGNAL RISK FIX: risk per trade $1.5 -&gt; $5 (2026-07-11) ----
# Position size = risk / SL-distance, so margins were tiny ($1.4-1.6) when SL
# was wide. $5 risk =&gt; ~3.3x bigger positions. &quot;extended (half size)&quot; trades
# will now risk $2.5. Overridden by the FAST_RISK_USDT env var if set on Render.
FAST_RISK_USDT = float(os.environ.get(&quot;FAST_RISK_USDT&quot;, 20.0))

TIGHT_SPIKE_CONFIRMED_LOOKBACK = int(os.environ.get(&quot;TIGHT_SPIKE_CONFIRMED_LOOKBACK&quot;, 3))
TIGHT_SCAN_SYMBOL_GAP          = float(os.environ.get(&quot;TIGHT_SCAN_SYMBOL_GAP&quot;, 0.1))
TIGHT_BASELINE_SYMBOL_GAP      = float(os.environ.get(&quot;TIGHT_BASELINE_SYMBOL_GAP&quot;, 0.5))

tight_symbols_current = []   # current scan universe, shared with the baseline refresher thread


def fetch_tight_baseline(symbol):
    &quot;&quot;&quot;Fetch + cache the 3-day average 1m volume for ONE symbol. Called ONLY
    from the background refresher thread, never inline from the scan loop.&quot;&quot;&quot;
    cached = tight_baseline_cache.get(symbol)
    try:
        all_vols    = []
        end_time    = None
        remaining   = TIGHT_BASELINE_CANDLES
        first_chunk = True
        while remaining &gt; 0:
            chunk_limit = min(remaining, BINGX_KLINE_MAX_LIMIT)
            candles = get_candles(symbol, limit=chunk_limit, interval=TIGHT_TIMEFRAME, end_time=end_time)
            if not candles:
                break
            chunk = candles[:-1] if first_chunk else candles   # drop live in-progress candle
            all_vols.extend(v(c) for c in chunk)
            end_time = int(candles[0][&quot;time&quot;]) - 1
            remaining -= len(candles)
            first_chunk = False
            if len(candles) &lt; chunk_limit:
                break
            time.sleep(0.3)
        if len(all_vols) &lt; 100:
            return cached[&quot;baseline&quot;] if cached else None
        baseline = sum(all_vols) / len(all_vols)
        tight_baseline_cache[symbol] = {&quot;baseline&quot;: baseline, &quot;ts&quot;: time.time()}
        return baseline
    except Exception as e:
        print(f&quot;[TIGHT BASELINE ERROR] {symbol}: {e}&quot;)
        return cached[&quot;baseline&quot;] if cached else None


def get_cached_baseline(symbol):
    &quot;&quot;&quot;Cache-only lookup for the scan loop - NEVER triggers a fetch.&quot;&quot;&quot;
    cached = tight_baseline_cache.get(symbol)
    return cached[&quot;baseline&quot;] if cached else None


def tight_baseline_loop():
    &quot;&quot;&quot;Background thread: keeps every symbol&#x27;s 3-day baseline fresh so the scan
    loop never blocks. First full population of ~400 symbols takes ~10-15 min
    (watch Baseline=X/Y in the [TIGHT SCAN] log line).&quot;&quot;&quot;
    print(&quot;Tight baseline refresher started (background thread)&quot;)
    while True:
        try:
            symbols = list(tight_symbols_current)
            if not symbols:
                time.sleep(5)
                continue
            now = time.time()
            stale = [s for s in symbols
                     if s not in tight_baseline_cache
                     or now - tight_baseline_cache[s][&quot;ts&quot;] &gt;= TIGHT_BASELINE_REFRESH_SECONDS]
            if not stale:
                time.sleep(10)
                continue
            for sym in stale:
                if api_backoff_active():
                    time.sleep(30)
                    break
                fetch_tight_baseline(sym)
                time.sleep(TIGHT_BASELINE_SYMBOL_GAP)
        except Exception as e:
            print(f&quot;[TIGHT BASELINE LOOP ERROR] {e}&quot;)
            time.sleep(10)


def check_tight_symbol(symbol):
    &quot;&quot;&quot;OVERRIDES the earlier version. Returns True if it hit the API, False if
    skipped (no baseline yet) so the scan loop can skip the pacing sleep.&quot;&quot;&quot;
    try:
        baseline = get_cached_baseline(symbol)
        if not baseline or baseline &lt;= 0:
            return False

        candles = get_candles(symbol, limit=30, interval=TIGHT_TIMEFRAME)
        if len(candles) &lt; 20:
            return True
        live_candle = candles[-1]
        confirmed   = candles[:-1]

        live_ratio = v(live_candle) / baseline
        state = tight_watch.get(symbol)

        # ---- Spike trigger: live candle OR last few closed candles ----
        if state is None:
            spike_candle = None
            spike_ratio  = 0.0
            if live_ratio &gt;= TIGHT_SPIKE_VOL_MULT:
                spike_candle, spike_ratio = live_candle, live_ratio
            else:
                for c in confirmed[-TIGHT_SPIKE_CONFIRMED_LOOKBACK:]:
                    r_c = v(c) / baseline
                    if r_c &gt;= TIGHT_SPIKE_VOL_MULT and r_c &gt; spike_ratio:
                        spike_candle, spike_ratio = c, r_c
            if spike_candle is not None:
                direction = &quot;UP&quot; if cl(spike_candle) &gt; o(spike_candle) else &quot;DOWN&quot;
                tight_watch[symbol] = {
                    &quot;state&quot;: &quot;COOLDOWN&quot;, &quot;direction&quot;: direction,
                    &quot;spike_ts&quot;: time.time(),
                    &quot;cooldown_highs&quot;: [], &quot;cooldown_lows&quot;: [],
                    &quot;last_processed_time&quot;: int(spike_candle[&quot;time&quot;]),
                }
                print(f&quot;[TIGHT SPIKE] {symbol} [{direction}] {round(spike_ratio,1)}x - watching&quot;)
            return True

        if time.time() - state[&quot;spike_ts&quot;] &gt; TIGHT_MAX_COOLDOWN_WAIT_SECONDS:
            tight_watch.pop(symbol, None)
            return True

        if state[&quot;state&quot;] == &quot;COOLDOWN&quot;:
            # Catch up on EVERY closed candle since the last pass
            last_t = int(state.get(&quot;last_processed_time&quot;) or 0)
            for c in [x for x in confirmed if int(x[&quot;time&quot;]) &gt; last_t]:
                state[&quot;last_processed_time&quot;] = int(c[&quot;time&quot;])
                if v(c) / baseline &lt; TIGHT_COOLDOWN_VOL_RATIO:
                    state[&quot;cooldown_highs&quot;].append(h(c))
                    state[&quot;cooldown_lows&quot;].append(l(c))
                else:
                    state[&quot;cooldown_highs&quot;] = []
                    state[&quot;cooldown_lows&quot;]  = []
                    continue

                if len(state[&quot;cooldown_highs&quot;]) &gt;= TIGHT_COOLDOWN_MIN_CANDLES:
                    range_high = max(state[&quot;cooldown_highs&quot;][-TIGHT_COOLDOWN_MIN_CANDLES:])
                    range_low  = min(state[&quot;cooldown_lows&quot;][-TIGHT_COOLDOWN_MIN_CANDLES:])
                    closes_r = [cl(x) for x in confirmed[-30:]]
                    highs_r  = [h(x)  for x in confirmed[-30:]]
                    lows_r   = [l(x)  for x in confirmed[-30:]]
                    atr_vals = atr_series(highs_r, lows_r, closes_r, TIGHT_ATR_LEN)
                    atr_now  = atr_vals[-1] if atr_vals else (range_high - range_low)
                    if atr_now &gt; 0 and (range_high - range_low) &lt;= atr_now * TIGHT_RANGE_MAX_ATR_MULT:
                        state[&quot;state&quot;]      = &quot;READY&quot;
                        state[&quot;range_high&quot;] = range_high
                        state[&quot;range_low&quot;]  = range_low
                        state[&quot;atr_now&quot;]    = atr_now
                        break

        # plain `if` so COOLDOWN -&gt; READY in this same pass checks breakout immediately
        if state.get(&quot;state&quot;) == &quot;READY&quot;:
            range_high = state[&quot;range_high&quot;]
            range_low  = state[&quot;range_low&quot;]
            last_t = int(state.get(&quot;last_processed_time&quot;) or 0)
            candidates = [c for c in confirmed if int(c[&quot;time&quot;]) &gt; last_t] + [live_candle]

            breakout_up = breakout_down = False
            trigger_ratio = 0.0
            for c in candidates:
                if c is not live_candle:
                    state[&quot;last_processed_time&quot;] = int(c[&quot;time&quot;])
                c_ratio = v(c) / baseline
                if cl(c) &gt; range_high and c_ratio &gt;= TIGHT_REENTRY_VOL_MULT:
                    breakout_up, trigger_ratio = True, c_ratio
                    break
                if cl(c) &lt; range_low and c_ratio &gt;= TIGHT_REENTRY_VOL_MULT:
                    breakout_down, trigger_ratio = True, c_ratio
                    break

            if not breakout_up and not breakout_down:
                return True

            if len(tight_open_trades) &gt;= TIGHT_MAX_CONCURRENT_TRADES:
                print(f&quot;[TIGHT SKIP] {symbol} breakout ignored - max concurrent trades reached&quot;)
                tight_watch.pop(symbol, None)
                return True

            side    = &quot;BUY&quot; if breakout_up else &quot;SELL&quot;
            entry   = cl(live_candle)   # market order fills at current price
            atr_now = state[&quot;atr_now&quot;]
            sl = range_low - atr_now * TIGHT_SL_ATR_BUFFER_MULT if side == &quot;BUY&quot; else range_high + atr_now * TIGHT_SL_ATR_BUFFER_MULT
            risk = abs(entry - sl)
            tight_watch.pop(symbol, None)
            if risk &lt;= 0:
                return True
            tp = round(entry + risk * TIGHT_RR_TP, 6) if side == &quot;BUY&quot; else round(entry - risk * TIGHT_RR_TP, 6)
            sl = round(sl, 6)

            trade_status = &quot;&quot;
            if tight_auto_trade_enabled:
                oid = place_tight_order(symbol, side, entry, sl, tp)
                if oid == &quot;MARGIN_SKIP&quot;:
                    trade_status = &quot;\nSkipped - insufficient margin&quot;
                elif oid and oid != &quot;N/A&quot;:
                    trade_status = &quot;\nOrder: &quot; + str(oid)
                else:
                    trade_status = &quot;\nOrder failed&quot;
            else:
                trade_status = &quot;\nAuto-trade OFF&quot;
            send_tg(
                &quot;TIGHT SIGNAL - &quot; + side + &quot; - &quot; + symbol + &quot;\n------------------------------\n&quot;
                &quot;Entry: &quot; + str(round(entry, 6)) + &quot; | SL: &quot; + str(sl) + &quot; | TP (1:&quot; + str(TIGHT_RR_TP) + &quot;): &quot; + str(tp) + &quot;\n&quot;
                &quot;Breakout vol: &quot; + str(round(trigger_ratio, 1)) + &quot;x | BE at 1:&quot; + str(TIGHT_BE_TRIGGER_R) + &quot;R&quot; +
                trade_status + &quot;\n------------------------------\nNiti Tight&quot;
            )
        return True
    except Exception as e:
        print(f&quot;[TIGHT {symbol}] error: {e}&quot;)
        return True


def tight_diagnostic_check():
    &quot;&quot;&quot;OVERRIDES the earlier version - cache-only baseline lookup.&quot;&quot;&quot;
    try:
        baseline = get_cached_baseline(&quot;BTC-USDT&quot;)
        if not baseline:
            print(f&quot;[TIGHT DIAG] BTC-USDT baseline not cached yet - refresher warming up ({len(tight_baseline_cache)} symbols covered)&quot;)
            return
        candles = get_candles(&quot;BTC-USDT&quot;, limit=5, interval=TIGHT_TIMEFRAME)
        if len(candles) &lt; 2:
            print(f&quot;[TIGHT DIAG] BTC-USDT - only got {len(candles)} recent candles&quot;)
            return
        live_ratio = v(candles[-1]) / baseline
        print(f&quot;[TIGHT DIAG] BTC-USDT baseline_vol={baseline:.2f} live_vol={v(candles[-1]):.2f} ratio={live_ratio:.2f}x (need {TIGHT_SPIKE_VOL_MULT}x)&quot;)
    except Exception as e:
        print(f&quot;[TIGHT DIAG ERROR] {e}&quot;)


def tight_scan_loop():
    &quot;&quot;&quot;OVERRIDES the earlier version - never blocks on baseline fetching.&quot;&quot;&quot;
    global tight_symbols_current
    print(&quot;Tight loop started - Stock Niti strategy (2026-07-11, non-blocking baseline rework)&quot;)
    all_symbols = []
    while True:
        try:
            if not all_symbols:
                all_symbols = get_futures_symbols() or []
            symbols = get_liquid_symbols(all_symbols, min_quote_vol=TIGHT_MIN_QUOTE_VOL, max_n=TIGHT_MAX_SYMBOLS)
            tight_symbols_current = symbols
            covered = sum(1 for s in symbols if s in tight_baseline_cache)
            print(f&quot;[TIGHT SCAN] Scanning {len(symbols)}/{len(all_symbols)} liquid pairs | Baseline={covered}/{len(symbols)} | Watching={len(tight_watch)} | Open={len(tight_open_trades)} | Auto={tight_auto_trade_enabled}&quot;)
            tight_diagnostic_check()
            t0 = time.time()
            for sym in symbols:
                if check_tight_symbol(sym):
                    time.sleep(TIGHT_SCAN_SYMBOL_GAP)
            print(f&quot;[TIGHT SCAN] Done in {int(time.time() - t0)}s.&quot;)
            send_daily_summary()
        except Exception as e:
            print(f&quot;[TIGHT LOOP ERROR] {e}&quot;)
        time.sleep(TIGHT_SCAN_INTERVAL_SECONDS)


Thread(target=tight_baseline_loop, daemon=True).start()
# ==================== END TIGHT 2 SPEED FIX ====================

if __name__ == &quot;__main__&quot;:
    Thread(target=tight_scan_loop,          daemon=True).start()
    Thread(target=fast_scan_loop,           daemon=True).start()
    Thread(target=trailing_loop,            daemon=True).start()
    Thread(target=handle_telegram_commands, daemon=True).start()
    app.run(host=&quot;0.0.0.0&quot;, port=int(os.environ.get(&quot;PORT&quot;, 5000)))
</pre>
<script>
function copyCode(btn) {
  const t = document.getElementById('code').textContent;
  const ta = document.createElement('textarea');
  ta.value = t;
  document.body.appendChild(ta);
  ta.select();
  document.execCommand('copy');
  document.body.removeChild(ta);
  btn.textContent = 'Copied!';
  setTimeout(() => btn.textContent = 'Copy', 2000);
}
</script>
</body>
</html>
