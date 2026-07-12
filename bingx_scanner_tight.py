import os, time, hmac, hashlib, requests
from flask import Flask
from threading import Thread
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

# ==================== ENV VARS ====================
API_KEY           = os.environ.get("BINGX_API_KEY")
SECRET_KEY        = os.environ.get("BINGX_SECRET_KEY")
TG_TOKEN          = os.environ.get("TG_BOT_TOKEN_TIGHT")
TG_CHAT_ID        = os.environ.get("TG_CHAT_ID_TIGHT")
TG_JOURNAL_ID     = os.environ.get("TG_JOURNAL_CHAT_ID")
FAST_TRADE_AMOUNT = float(os.environ.get("FAST_TRADE_AMOUNT", 20))

BASE_URL = "https://open-api.bingx.com"

# ==================== FAST SIGNAL CONFIG ====================
FAST_TIMEFRAME          = "3m"
FAST_MIN_QUOTE_VOL      = float(os.environ.get("FAST_MIN_QUOTE_VOL", 2_000_000))
FAST_MAX_SYMBOLS        = 150
FAST_CONSOL_LOOKBACK    = 20
FAST_BREAKOUT_ATR_MULT  = float(os.environ.get("FAST_BREAKOUT_ATR_MULT", 0.2))   # loosened from 0.3 (2026-07-11) - catch moves earlier
FAST_VOL_MULT           = 3.0
FAST_VOL_LB             = 20
FAST_ATR_LEN            = 14
FAST_SL_ATR_MULT        = 1.2   # NOTE: dead/unused - actual SL uses SL_ATR_BUFFER_MULT below. Kept only for reference.
FAST_RISK_USDT          = float(os.environ.get("FAST_RISK_USDT", 20.0))
FAST_EXCLUDE_TOP_N       = 75
FAST_EXTENSION_LOOKBACK  = 20
FAST_EXTENSION_LIMIT     = 4.0
FAST_EXTENSION_MULT      = 0.5
FAST_MARGIN_CAP_MULT     = 5.0
FAST_TP1_RR             = float(os.environ.get("FAST_TP1_RR", 0.8))
FAST_TRAIL_ACTIVATE_RR  = 1.0
FAST_CLOSE_POSITION_MIN = float(os.environ.get("FAST_CLOSE_POSITION_MIN", 0.6))   # loosened from 0.7 (2026-07-11)
FAST_TRAIL_ATR_MULT     = float(os.environ.get("FAST_TRAIL_ATR_MULT", 1.5))
FAST_TRAIL_PCT_FALLBACK = float(os.environ.get("FAST_TRAIL_PCT_FALLBACK", 3.0))

SL_ATR_BUFFER_MULT = 0.3   # shared SL buffer, used by both Fast and Tight

# ---- Progress-based time exit (reworked 2026-07-11) ----
# Old behaviour: force-checked every 10 min and killed anything below a fixed R
# threshold, even trades that were quietly profitable. New behaviour: profit is
# never time-exited (trailing handles it); only flat/losing trades and trades whose
# profit has stalled (stagnant) get timed out. A long backup cap protects against
# a trade getting stuck open due to a bug/glitch.
FAST_PROGRESS_CHECK_SECONDS   = int(os.environ.get("FAST_PROGRESS_CHECK_SECONDS", 900))    # 15 min between checks
FAST_STAGNATION_CHECKS        = int(os.environ.get("FAST_STAGNATION_CHECKS", 3))           # consecutive non-improving checks (~45 min) before locking profit
FAST_STAGNATION_MIN_R_INCREASE = float(os.environ.get("FAST_STAGNATION_MIN_R_INCREASE", 0.1))
FAST_SAFETY_CAP_SECONDS       = int(os.environ.get("FAST_SAFETY_CAP_SECONDS", 12600))      # 3.5h - backup net only, should basically never trigger in normal operation

# ---- MTF trend filter (added to Fast Signal 2026-07-11, reusing the Tight-era helper) ----
# Direction-only check against the already-closed 1h candle - no extra waiting,
# checked at the same instant as the breakout candle itself.
MTF_FILTER_ENABLED = os.environ.get("MTF_FILTER_ENABLED", "true").lower() == "true"
MTF_INTERVAL        = os.environ.get("MTF_INTERVAL", "1h")
EMA200_LEN          = 200

MIN_PRICE = 0.001

# ==================== TIGHT CONFIG (Stock Niti strategy, replaces old Tight 2 - 2026-07-11) ====================
# Volume-spike -> cooldown -> re-entry-on-breakout strategy, all on the 1m timeframe.
TIGHT_TIMEFRAME              = "1m"
TIGHT_MIN_QUOTE_VOL          = float(os.environ.get("TIGHT_MIN_QUOTE_VOL", 1_000_000))   # liquidity floor for coin selection
TIGHT_MAX_SYMBOLS            = int(os.environ.get("TIGHT_MAX_SYMBOLS", 400))

TIGHT_BASELINE_CANDLES       = int(os.environ.get("TIGHT_BASELINE_CANDLES", 4320))   # 3 days of 1m candles
BINGX_KLINE_MAX_LIMIT        = 1440   # confirmed 2026-07-11 via live API error: "limit: This field must be less than or equal to 1440." Baseline is paginated in chunks of this size.
TIGHT_BASELINE_REFRESH_SECONDS = int(os.environ.get("TIGHT_BASELINE_REFRESH_SECONDS", 1800))   # re-fetch baseline every 30 min per symbol, not every scan

TIGHT_SPIKE_VOL_MULT         = float(os.environ.get("TIGHT_SPIKE_VOL_MULT", 20.0))   # live in-progress candle vs 3-day baseline
TIGHT_COOLDOWN_VOL_RATIO     = float(os.environ.get("TIGHT_COOLDOWN_VOL_RATIO", 3.0))   # below this = "cooling"
TIGHT_COOLDOWN_MIN_CANDLES   = int(os.environ.get("TIGHT_COOLDOWN_MIN_CANDLES", 5))     # consecutive cool candles to confirm a range
TIGHT_RANGE_MAX_ATR_MULT     = float(os.environ.get("TIGHT_RANGE_MAX_ATR_MULT", 2.0))   # reject range as "not sideways" if wider than this x ATR (filters slow-bleed)
TIGHT_REENTRY_VOL_MULT       = float(os.environ.get("TIGHT_REENTRY_VOL_MULT", 5.0))     # volume needed on the breakout candle to re-enter
TIGHT_SL_ATR_BUFFER_MULT     = float(os.environ.get("TIGHT_SL_ATR_BUFFER_MULT", 0.3))
TIGHT_RR_TP                  = float(os.environ.get("TIGHT_RR_TP", 4.0))
TIGHT_BE_TRIGGER_R           = float(os.environ.get("TIGHT_BE_TRIGGER_R", 2.0))         # move SL to breakeven at this R, full size kept
TIGHT_MAX_COOLDOWN_WAIT_SECONDS = int(os.environ.get("TIGHT_MAX_COOLDOWN_WAIT_SECONDS", 3600))   # give up watching after 60 min with no breakout
TIGHT_MAX_CONCURRENT_TRADES  = int(os.environ.get("TIGHT_MAX_CONCURRENT_TRADES", 4))
TIGHT_RISK_USDT              = float(os.environ.get("TIGHT_RISK_USDT", 20.0))
TIGHT_LEVERAGE               = int(os.environ.get("TIGHT_LEVERAGE", 20))   # fixed, per Faisal's instruction (2026-07-11)
TIGHT_ATR_LEN                = 14

TIGHT_SCAN_INTERVAL_SECONDS  = int(os.environ.get("TIGHT_SCAN_INTERVAL_SECONDS", 30))
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

tight_watch          = {}   # symbol -> spike/cooldown/ready state, see check_tight_symbol()
tight_baseline_cache = {}   # symbol -> {"baseline": val, "ts": ...}

mtf_cache = {}

# ---- API backoff (added 2026-07-11) ----
# BingX's rate-limit penalty for repeated bad/over-limit requests is self-renewing:
# each additional request made *while still blocked* pushes the "retry after" time
# further out. Continuing to scan during a block was preventing the block from
# ever clearing on its own. This makes the bot go fully quiet on market-data calls
# for API_BACKOFF_SECONDS as soon as a rate-limit response is detected, instead of
# keep hammering the endpoint and extending its own penalty.
API_BACKOFF_SECONDS = int(os.environ.get("API_BACKOFF_SECONDS", 1200))   # 20 min
_api_backoff_until  = 0.0
_api_backoff_logged = 0.0


def api_backoff_active():
    return time.time() < _api_backoff_until


def trigger_api_backoff(reason=""):
    global _api_backoff_until
    _api_backoff_until = time.time() + API_BACKOFF_SECONDS
    print(f"[API BACKOFF] pausing all market-data scanning for {API_BACKOFF_SECONDS}s - {reason}")


def _looks_like_rate_limit(resp):
    if not isinstance(resp, dict):
        return False
    msg = str(resp.get("msg", "")).lower()
    return resp.get("code") == 109429 or "over" in msg or "too many" in msg or "too frequent" in msg


# ==================== CORE API HELPERS ====================
def build_signed_params(params: dict) -> dict:
    params["timestamp"] = int(time.time() * 1000)
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    params["signature"] = hmac.new(
        SECRET_KEY.encode(), qs.encode(), hashlib.sha256
    ).hexdigest()
    return params


def get_futures_symbols():
    url = BASE_URL + "/openApi/swap/v2/quote/contracts"
    r = requests.get(url, timeout=10).json()
    symbols = []
    for c in r.get("data", []):
        if c.get("status") == 1 and "USDT" in c["symbol"]:
            sym = c["symbol"]
            symbols.append(sym)
            symbol_precision[sym] = int(c.get("quantityPrecision", 4))
            try:
                symbol_max_lev[sym] = int(float(c.get("maxLongLeverage", 20)))
            except Exception:
                symbol_max_lev[sym] = 20
    return symbols


def get_liquid_symbols(symbols, min_quote_vol, max_n=None, exclude_top_n=0):
    try:
        url = BASE_URL + "/openApi/swap/v2/quote/ticker"
        r = requests.get(url, timeout=10).json()
        tickers = r.get("data", [])
        if not isinstance(tickers, list):
            return symbols[:max_n] if max_n else symbols
        sym_set = set(symbols)
        liquid = []
        for t in tickers:
            sym = t.get("symbol", "")
            if sym not in sym_set:
                continue
            try:
                qvol = float(t.get("quoteVolume", 0))
            except Exception:
                qvol = 0
            if qvol >= min_quote_vol:
                liquid.append((sym, qvol))
        liquid.sort(key=lambda x: x[1], reverse=True)
        if exclude_top_n > 0:
            liquid = liquid[exclude_top_n:]
        if max_n is not None:
            liquid = liquid[:max_n]
        return [s for s, _ in liquid]
    except Exception as e:
        print(f"[LIQUID SYMBOLS ERROR] {e}")
        return symbols[:max_n] if max_n else symbols


def get_candles(symbol, limit=350, interval="15m", end_time=None):
    global _api_backoff_logged
    if api_backoff_active():
        now = time.time()
        if now - _api_backoff_logged > 60:   # don't spam the log every single call while backed off
            remaining = int(_api_backoff_until - now)
            print(f"[API BACKOFF] still active - skipping candle requests for {remaining}s more")
            _api_backoff_logged = now
        return []
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if end_time is not None:
        params["endTime"] = int(end_time)
    params = build_signed_params(params)
    url = BASE_URL + "/openApi/swap/v3/quote/klines"
    r = requests.get(url, params=params,
                     headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
    candles = r.get("data", [])
    # ---- Diagnostic logging (added 2026-07-11) ----
    # get_candles() used to fail silently: any non-list/empty "data" (rate-limit
    # response, error payload, API cap on `limit`, etc.) just became [] with zero
    # visibility. This logs the raw response whenever the result looks off, so a
    # genuine API-side problem shows up in the Render logs instead of just quietly
    # starving both strategies of data.
    if not isinstance(candles, list):
        if _looks_like_rate_limit(r):
            trigger_api_backoff(f"{symbol} {interval}: {str(r)[:200]}")
        else:
            print(f"[CANDLES ERROR] {symbol} {interval} limit={limit} - non-list response: {str(r)[:300]}")
        return []
    if len(candles) < min(limit, 50):
        print(f"[CANDLES SHORT] {symbol} {interval} requested={limit} got={len(candles)} - raw: {str(r)[:300]}")
    candles.sort(key=lambda x: x["time"])
    return candles


def get_current_price(symbol):
    try:
        url = BASE_URL + "/openApi/swap/v2/quote/price"
        r = requests.get(url, params={"symbol": symbol}, timeout=5).json()
        return float(r.get("data", {}).get("price", 0))
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
    if n < period + 1:
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
    """Cached (5 min) higher-timeframe (MTF_INTERVAL) EMA200 trend direction. Used by
    Fast Signal as a direction-only filter - no timing delay, checked instantly
    against the already-closed 1h candle at the same moment as the breakout candle."""
    now = time.time()
    cached = mtf_cache.get(symbol)
    if cached and now - cached["ts"] < 300:
        return cached["trend"]
    try:
        candles = get_candles(symbol, limit=250, interval=MTF_INTERVAL)
        if len(candles) < 210:
            return cached["trend"] if cached else None
        closes = [cl(c) for c in candles]
        ema200 = ema_series(closes, EMA200_LEN)
        trend = "UP" if closes[-1] > ema200[-1] else "DOWN"
        mtf_cache[symbol] = {"trend": trend, "ts": now}
        return trend
    except Exception as e:
        print(f"[MTF ERROR] {symbol}: {e}")
        return cached["trend"] if cached else None


def h(c):  return float(c["high"])
def l(c):  return float(c["low"])
def cl(c): return float(c["close"])
def o(c):  return float(c["open"])
def v(c):  return float(c["volume"])


# ==================== TELEGRAM ====================
def send_tg(msg, chat_id=None):
    cid = chat_id or TG_CHAT_ID
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": cid, "text": msg, "parse_mode": "HTML"}, timeout=10)


def send_journal(msg):
    if TG_JOURNAL_ID:
        send_tg(msg, chat_id=TG_JOURNAL_ID)


def journal_closed_trade(trade):
    """Single consolidated journal entry per closed trade - kept deliberately simple,
    no intermediate messages (no TP1-banked / cooldown-triggered spam)."""
    sign = "+" if trade.get("pnl", 0) > 0 else ""
    send_journal(
        "Trade Closed [" + trade.get("label", "?") + "] - " + trade["symbol"] + "\n"
        "------------------------------\n"
        "Side  : " + trade["side"] + "\nEntry : " + str(trade["entry"]) + "\n"
        "Result: " + trade.get("result", "?") + "\n"
        "PnL   : " + sign + str(trade.get("pnl", 0)) + " USDT\n"
        "------------------------------\nNiti Journal"
    )


# ==================== LEVERAGE ====================
def get_fast_leverage(symbol):
    max_lev = symbol_max_lev.get(symbol, 20)
    calc = int(max_lev * 0.20)
    lev = max(calc, 10)
    return min(lev, max_lev)


def set_leverage_api(symbol, lev):
    try:
        url = BASE_URL + "/openApi/swap/v2/trade/leverage"
        for side in ["LONG", "SHORT"]:
            params = build_signed_params({"symbol": symbol, "side": side, "leverage": lev})
            requests.post(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10)
    except Exception as e:
        print(f"[LEVERAGE ERROR] {symbol}: {e}")


# ==================== ORDER PLACEMENT ====================
def place_market_order(symbol, side, qty, pos_side):
    url = BASE_URL + "/openApi/swap/v2/trade/order"
    params = build_signed_params({
        "symbol": symbol, "side": side,
        "positionSide": pos_side, "type": "MARKET", "quantity": qty,
    })
    r = requests.post(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
    oid = r.get("data", {}).get("order", {}).get("orderId", "N/A")
    if oid == "N/A":
        print(f"[ORDER FAIL] {symbol} {side} qty={qty} positionSide={pos_side} - BingX: {r}")
    return oid


def place_sl_order(symbol, close_side, pos_side, sl_price, qty):
    url = BASE_URL + "/openApi/swap/v2/trade/order"
    params = build_signed_params({
        "symbol": symbol, "side": close_side, "positionSide": pos_side,
        "type": "STOP_MARKET", "stopPrice": round(sl_price, 6), "quantity": qty,
        "workingType": "MARK_PRICE",
    })
    r = requests.post(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
    print(f"[SL] {symbol}: {r}")
    return r.get("data", {}).get("order", {}).get("orderId", "N/A")


def place_tp_order(symbol, close_side, pos_side, tp_price, qty):
    url = BASE_URL + "/openApi/swap/v2/trade/order"
    params = build_signed_params({
        "symbol": symbol, "side": close_side, "positionSide": pos_side,
        "type": "TAKE_PROFIT_MARKET", "stopPrice": round(tp_price, 6), "quantity": qty,
        "workingType": "MARK_PRICE",
    })
    r = requests.post(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
    return r.get("data", {}).get("order", {}).get("orderId", "N/A")


def cancel_order(symbol, order_id):
    """NOTE: verify this endpoint/method against current BingX docs before relying
    on it live - it has not been tested against the real API in this conversation."""
    try:
        url = BASE_URL + "/openApi/swap/v2/trade/order"
        params = build_signed_params({"symbol": symbol, "orderId": order_id})
        r = requests.delete(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        return r
    except Exception as e:
        print(f"[CANCEL ERROR] {symbol} {order_id}: {e}")
        return None


def check_order_status(order_id, symbol):
    try:
        params = build_signed_params({"symbol": symbol, "orderId": order_id})
        url = BASE_URL + "/openApi/swap/v2/trade/order"
        r = requests.get(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        return r.get("data", {}).get("order", {}).get("status", "")
    except Exception:
        return ""


def close_fast_position(symbol, reason=""):
    if symbol not in fast_open_trades:
        return
    trade = fast_open_trades[symbol]
    try:
        pos_side   = "LONG"  if trade["side"] == "BUY" else "SHORT"
        close_side = "SELL"  if trade["side"] == "BUY" else "BUY"
        remaining  = trade.get("remaining_qty", 0)
        if remaining > 0 and fast_auto_trade_enabled:
            place_market_order(symbol, close_side, remaining, pos_side)
        if trade.get("sl_id"):
            cancel_order(symbol, trade["sl_id"])
        if not trade.get("tp1_filled") and trade.get("tp1_id"):
            cancel_order(symbol, trade["tp1_id"])
        current = get_current_price(symbol)
        if trade["side"] == "BUY":
            leg_pnl = (current - trade["entry"]) * remaining
        else:
            leg_pnl = (trade["entry"] - current) * remaining
        total_pnl = round(trade.get("partial_pnl", 0.0) + leg_pnl, 2)
        trade["pnl"]    = total_pnl
        trade["result"] = reason
        trade["label"]  = "Fast"
        trade["symbol"] = symbol
        daily_trades.append(trade)
        journal_closed_trade(trade)
        del fast_open_trades[symbol]
        print(f"[FAST CLOSE] {symbol} {reason}")
    except Exception as e:
        print(f"[FAST CLOSE ERROR] {symbol}: {e}")


def close_tight_position(oid, reason=""):
    trade = tight_open_trades.get(oid)
    if not trade:
        return
    try:
        symbol     = trade["symbol"]
        pos_side   = trade["pos_side"]
        close_side = trade["close_side"]
        qty        = trade.get("qty", 0)
        entry      = trade["entry"]
        if qty > 0 and tight_auto_trade_enabled:
            place_market_order(symbol, close_side, qty, pos_side)
        if trade.get("sl_id"):
            cancel_order(symbol, trade["sl_id"])
        if trade.get("tp_id"):
            cancel_order(symbol, trade["tp_id"])
        current = get_current_price(symbol)
        if trade["side"] == "BUY":
            leg_pnl = (current - entry) * qty
        else:
            leg_pnl = (entry - current) * qty
        trade["pnl"]    = round(leg_pnl, 2)
        trade["result"] = reason
        trade["label"]  = "Tight"
        daily_trades.append(trade)
        journal_closed_trade(trade)
        tight_open_trades.pop(oid, None)
        print(f"[TIGHT CLOSE] {symbol} {reason}")
    except Exception as e:
        print(f"[TIGHT CLOSE ERROR] {oid}: {e}")


def place_fast_order(symbol, side, entry, sl_price, tp1_price, atr_now, risk_usdt=FAST_RISK_USDT):
    try:
        lev       = get_fast_leverage(symbol)
        set_leverage_api(symbol, lev)
        precision  = symbol_precision.get(symbol, 4)
        risk_dist  = abs(entry - sl_price)
        if risk_dist <= 0:
            return None

        risk_qty      = risk_usdt / risk_dist
        margin_cap_qty = (FAST_TRADE_AMOUNT * FAST_MARGIN_CAP_MULT * lev) / entry
        total_qty     = round(min(risk_qty, margin_cap_qty), precision)
        half_qty      = round(total_qty / 2, precision)
        if total_qty <= 0 or half_qty <= 0:
            return None
        pos_side   = "LONG"  if side == "BUY" else "SHORT"
        close_side = "SELL"  if side == "BUY" else "BUY"
        sl_pct = abs(entry - sl_price) / entry * 100
        order_id = place_market_order(symbol, side, total_qty, pos_side)
        print(f"[FAST ORDER] {symbol} {side} lev={lev}x qty={total_qty} risk=${risk_usdt}: {order_id}")
        if order_id != "N/A":
            time.sleep(0.5)
            sl_id  = place_sl_order(symbol, close_side, pos_side, sl_price, total_qty)
            tp1_id = place_tp_order(symbol, close_side, pos_side, tp1_price, half_qty)
            fast_open_trades[symbol] = {
                "symbol": symbol, "side": side, "entry": entry,
                "sl": sl_price, "sl_id": sl_id, "tp1": tp1_price, "tp1_id": tp1_id, "lev": lev,
                "sl_pct": sl_pct, "close_side": close_side, "pos_side": pos_side,
                "total_qty": total_qty, "remaining_qty": total_qty, "tp1_filled": False, "partial_pnl": 0.0,
                "trail_price": entry, "activated": False, "order_id": order_id,
                "atr_at_entry": atr_now, "opened_ts": time.time(),
                "risk_dist": risk_dist,
                "next_check_ts": time.time() + FAST_PROGRESS_CHECK_SECONDS,
                "best_favorable_r": 0.0, "stagnation_count": 0,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
        return order_id
    except Exception as e:
        print(f"[FAST ORDER ERROR] {symbol}: {e}")
        return None


def track_fast_trades():
    for symbol in list(fast_open_trades.keys()):
        try:
            trade = fast_open_trades[symbol]

            if not trade.get("tp1_filled") and trade.get("tp1_id"):
                status = check_order_status(trade["tp1_id"], symbol)
                if status == "FILLED":
                    half_qty = round(trade["total_qty"] / 2, symbol_precision.get(symbol, 4))
                    leg_pnl  = (trade["tp1"] - trade["entry"]) * half_qty if trade["side"] == "BUY" else (trade["entry"] - trade["tp1"]) * half_qty
                    trade["partial_pnl"]   = trade.get("partial_pnl", 0.0) + leg_pnl
                    trade["remaining_qty"] = trade["total_qty"] - half_qty
                    trade["tp1_filled"]    = True

                    if trade.get("sl_id"):
                        cancel_order(symbol, trade["sl_id"])
                    new_sl_id = place_sl_order(
                        symbol, trade["close_side"], trade["pos_side"], trade["entry"], trade["remaining_qty"]
                    )
                    trade["sl_id"] = new_sl_id
                    trade["sl"]    = trade["entry"]

            sl_status = check_order_status(trade["sl_id"], symbol) if trade.get("sl_id") else ""
            if sl_status == "FILLED":
                if not trade.get("tp1_filled") and trade.get("tp1_id"):
                    cancel_order(symbol, trade["tp1_id"])
                remaining = trade.get("remaining_qty", 0)
                if trade["side"] == "BUY":
                    leg_pnl = (trade["sl"] - trade["entry"]) * remaining
                else:
                    leg_pnl = (trade["entry"] - trade["sl"]) * remaining
                total_pnl = round(trade.get("partial_pnl", 0.0) + leg_pnl, 2)
                trade["pnl"]    = total_pnl
                trade["result"] = "BE" if trade.get("tp1_filled") else "SL"
                trade["label"]  = "Fast"
                trade["symbol"] = symbol
                daily_trades.append(trade)
                journal_closed_trade(trade)
                del fast_open_trades[symbol]
                continue

            # ---- Progress-based time exit (reworked 2026-07-11) ----
            # Profit (favorable_r > 0): never TimeExit - hold for trailing to manage,
            # unless it stagnates (see stagnation_count below).
            # Flat/loss (favorable_r <= 0): exit at the next check, same as before.
            # A long safety cap (backup net only) applies regardless of state.
            now_ts = time.time()

            opened_ts = trade.get("opened_ts", now_ts)
            if now_ts - opened_ts >= FAST_SAFETY_CAP_SECONDS:
                print(f"[FAST SAFETY CAP] {symbol} - {FAST_SAFETY_CAP_SECONDS}s backup cap reached")
                close_fast_position(symbol, "SafetyCap")
                continue

            if now_ts >= trade.get("next_check_ts", now_ts + 1):
                current = get_current_price(symbol)
                risk_dist = trade.get("risk_dist", 0)
                if current > 0 and risk_dist > 0:
                    if trade["side"] == "BUY":
                        favorable_r = (current - trade["entry"]) / risk_dist
                    else:
                        favorable_r = (trade["entry"] - current) / risk_dist

                    if favorable_r > 0:
                        best_r = trade.get("best_favorable_r", 0.0)
                        if favorable_r > best_r + FAST_STAGNATION_MIN_R_INCREASE:
                            trade["best_favorable_r"]  = favorable_r
                            trade["stagnation_count"]  = 0
                            trade["next_check_ts"]     = now_ts + FAST_PROGRESS_CHECK_SECONDS
                            print(f"[FAST PROGRESS] {symbol} improving - {favorable_r:.2f}R")
                        else:
                            trade["stagnation_count"] = trade.get("stagnation_count", 0) + 1
                            if trade["stagnation_count"] >= FAST_STAGNATION_CHECKS:
                                print(f"[FAST STAGNANT] {symbol} - locking {favorable_r:.2f}R, no longer improving")
                                close_fast_position(symbol, "Stagnant")
                                continue
                            else:
                                trade["next_check_ts"] = now_ts + FAST_PROGRESS_CHECK_SECONDS
                                print(f"[FAST PROGRESS] {symbol} stalled ({trade['stagnation_count']}/{FAST_STAGNATION_CHECKS}) - {favorable_r:.2f}R")
                    else:
                        print(f"[FAST TIME EXIT] {symbol} - flat/dead ({favorable_r:.2f}R)")
                        close_fast_position(symbol, "TimeExit")
                        continue
                else:
                    trade["next_check_ts"] = now_ts + FAST_PROGRESS_CHECK_SECONDS
        except Exception as e:
            print(f"[FAST TRACK ERROR] {symbol}: {e}")


def update_fast_trailing():
    for symbol in list(fast_open_trades.keys()):
        try:
            trade   = fast_open_trades[symbol]
            current = get_current_price(symbol)
            if current <= 0:
                continue
            side = trade["side"]
            activate_pct = (FAST_TRAIL_ACTIVATE_RR * trade.get("sl_pct", 1.5)) / 100
            atr_ref    = trade.get("atr_at_entry", 0)
            trail_dist = atr_ref * FAST_TRAIL_ATR_MULT if atr_ref > 0 else trade["entry"] * (FAST_TRAIL_PCT_FALLBACK / 100)

            if not trade.get("activated"):
                if side == "BUY" and current >= trade["entry"] * (1 + activate_pct):
                    fast_open_trades[symbol]["activated"]   = True
                    fast_open_trades[symbol]["trail_price"] = current
                elif side == "SELL" and current <= trade["entry"] * (1 - activate_pct):
                    fast_open_trades[symbol]["activated"]   = True
                    fast_open_trades[symbol]["trail_price"] = current
                continue

            if side == "BUY":
                if current > trade["trail_price"]:
                    fast_open_trades[symbol]["trail_price"] = current
                trail_sl = trade["trail_price"] - trail_dist
                if current <= trail_sl:
                    close_fast_position(symbol, "Trail")
            else:
                if current < trade["trail_price"]:
                    fast_open_trades[symbol]["trail_price"] = current
                trail_sl = trade["trail_price"] + trail_dist
                if current >= trail_sl:
                    close_fast_position(symbol, "Trail")
        except Exception as e:
            print(f"[TRAIL ERROR] {symbol}: {e}")


# ==================== FAST SIGNAL ENTRY LOGIC ====================
def check_fast(symbol):
    try:
        candles = get_candles(symbol, limit=100, interval=FAST_TIMEFRAME)
        min_needed = max(FAST_CONSOL_LOOKBACK + FAST_VOL_LB + FAST_ATR_LEN + 5, FAST_EXTENSION_LOOKBACK + 5)
        if len(candles) < min_needed:
            return

        confirmed = candles[:-1]
        closes = [cl(c) for c in confirmed]
        opens  = [o(c)  for c in confirmed]
        highs  = [h(c)  for c in confirmed]
        lows   = [l(c)  for c in confirmed]
        vols   = [v(c)  for c in confirmed]

        i = len(confirmed) - 1
        if i < min_needed:
            return

        entry = closes[i]
        if entry < MIN_PRICE:
            return

        atr_vals = atr_series(highs, lows, closes, FAST_ATR_LEN)
        atr_now  = atr_vals[i]

        ext_move  = abs(closes[i] - closes[i - FAST_EXTENSION_LOOKBACK])
        extension = ext_move / atr_now if atr_now > 0 else 0
        is_extended = extension > FAST_EXTENSION_LIMIT

        box_high = max(highs[i - FAST_CONSOL_LOOKBACK:i])
        box_low  = min(lows[i - FAST_CONSOL_LOOKBACK:i])

        avg_vol = sum(vols[i - FAST_VOL_LB:i]) / FAST_VOL_LB
        ratio   = vols[i] / avg_vol if avg_vol > 0 else 0
        vol_ok  = ratio >= FAST_VOL_MULT

        candle_range = highs[i] - lows[i]
        bull_close_strength = (closes[i] - lows[i]) / candle_range if candle_range > 0 else 0
        bear_close_strength = (highs[i] - closes[i]) / candle_range if candle_range > 0 else 0

        bull_breakout = (closes[i] > box_high + atr_now * FAST_BREAKOUT_ATR_MULT
                         and bull_close_strength >= FAST_CLOSE_POSITION_MIN)
        bear_breakout = (closes[i] < box_low  - atr_now * FAST_BREAKOUT_ATR_MULT
                         and bear_close_strength >= FAST_CLOSE_POSITION_MIN)

        long_signal  = bull_breakout and vol_ok
        short_signal = bear_breakout and vol_ok

        if not long_signal and not short_signal:
            return

        # ---- MTF trend filter (added 2026-07-11) - direction-only, no timing delay ----
        if MTF_FILTER_ENABLED and (long_signal or short_signal):
            mtf_trend = get_mtf_trend(symbol)
            if mtf_trend is not None:
                if long_signal and mtf_trend != "UP":
                    print(f"[MTF SKIP] {symbol} long blocked - {MTF_INTERVAL} trend is {mtf_trend}")
                    long_signal = False
                if short_signal and mtf_trend != "DOWN":
                    print(f"[MTF SKIP] {symbol} short blocked - {MTF_INTERVAL} trend is {mtf_trend}")
                    short_signal = False
        if not long_signal and not short_signal:
            return

        if symbol in fast_open_trades:
            current_side = fast_open_trades[symbol]["side"]
            if (current_side == "BUY" and short_signal) or (current_side == "SELL" and long_signal):
                print(f"[FAST CLOSE - OPPOSITE] {symbol}")
                close_fast_position(symbol, "Opposite")
            return

        sig_id_long  = (symbol, "BUY",  int(confirmed[i]["time"]))
        sig_id_short = (symbol, "SELL", int(confirmed[i]["time"]))

        if long_signal and sig_id_long not in fast_alerted:
            fast_alerted.add(sig_id_long)
            lev        = get_fast_leverage(symbol)
            sl_price   = round(box_low - atr_now * SL_ATR_BUFFER_MULT, 6)
            risk       = entry - sl_price
            if risk <= 0:
                return
            tp1_price  = round(entry + risk * FAST_TP1_RR, 6)
            risk_usdt  = FAST_RISK_USDT * FAST_EXTENSION_MULT if is_extended else FAST_RISK_USDT
            ext_tag    = "extended (half size)" if is_extended else "fresh move"
            print(f"[FAST] {symbol} BUY | vol={ratio:.1f}x | lev={lev}x | {ext_tag} | ext={extension:.1f}x ATR")
            trade_status = ""
            if fast_auto_trade_enabled:
                oid = place_fast_order(symbol, "BUY", entry, sl_price, tp1_price, atr_now, risk_usdt)
                trade_status = "\nOrder: " + str(oid) if oid and oid != "N/A" else "\nOrder failed"
            else:
                trade_status = "\nAuto-trade OFF"
            send_tg(
                "FAST SIGNAL - BUY - " + symbol + "\n------------------------------\n"
                "Entry: " + str(round(entry, 6)) + " | SL: " + str(sl_price) + "\n"
                "TP1 (" + str(FAST_TP1_RR) + "R): " + str(tp1_price) + " | ATR-trail after " + str(FAST_TRAIL_ACTIVATE_RR) + "R\n"
                "Breakout vol: " + str(round(ratio, 1)) + "x | Close-strength: " + str(round(bull_close_strength, 2)) +
                " | Lev: " + str(lev) + "x | " + ext_tag + " | Risk: $" + str(risk_usdt) +
                trade_status + "\n------------------------------\nNiti Fast Signal"
            )

        elif short_signal and sig_id_short not in fast_alerted:
            fast_alerted.add(sig_id_short)
            lev        = get_fast_leverage(symbol)
            sl_price   = round(box_high + atr_now * SL_ATR_BUFFER_MULT, 6)
            risk       = sl_price - entry
            if risk <= 0:
                return
            tp1_price  = round(entry - risk * FAST_TP1_RR, 6)
            risk_usdt  = FAST_RISK_USDT * FAST_EXTENSION_MULT if is_extended else FAST_RISK_USDT
            ext_tag    = "extended (half size)" if is_extended else "fresh move"
            print(f"[FAST] {symbol} SELL | vol={ratio:.1f}x | lev={lev}x | {ext_tag} | ext={extension:.1f}x ATR")
            trade_status = ""
            if fast_auto_trade_enabled:
                oid = place_fast_order(symbol, "SELL", entry, sl_price, tp1_price, atr_now, risk_usdt)
                trade_status = "\nOrder: " + str(oid) if oid and oid != "N/A" else "\nOrder failed"
            else:
                trade_status = "\nAuto-trade OFF"
            send_tg(
                "FAST SIGNAL - SELL - " + symbol + "\n------------------------------\n"
                "Entry: " + str(round(entry, 6)) + " | SL: " + str(sl_price) + "\n"
                "TP1 (" + str(FAST_TP1_RR) + "R): " + str(tp1_price) + " | ATR-trail after " + str(FAST_TRAIL_ACTIVATE_RR) + "R\n"
                "Breakout vol: " + str(round(ratio, 1)) + "x | Close-strength: " + str(round(bear_close_strength, 2)) +
                " | Lev: " + str(lev) + "x | " + ext_tag + " | Risk: $" + str(risk_usdt) +
                trade_status + "\n------------------------------\nNiti Fast Signal"
            )

    except Exception as e:
        print(f"[FAST {symbol}] error: {e}")


# ==================== TIGHT ENTRY LOGIC (Stock Niti: spike -> cooldown -> breakout) ====================
def get_tight_volume_baseline(symbol):
    """Cached 3-day average 1m volume per symbol, refreshed every
    TIGHT_BASELINE_REFRESH_SECONDS rather than on every scan (expensive call).

    Paginated (fixed 2026-07-11): BingX's kline endpoint caps `limit` at 1440 per
    request (confirmed via a live 109400 error - the original single-call
    limit=4320 request was failing on every symbol, every cycle, which is why the
    baseline was always None and Tight never watched anything). This now fetches
    TIGHT_BASELINE_CANDLES worth of history in <=1440-candle chunks, walking
    backwards with `endTime`, with a small sleep between chunks to avoid bursting."""
    now = time.time()
    cached = tight_baseline_cache.get(symbol)
    if cached and now - cached["ts"] < TIGHT_BASELINE_REFRESH_SECONDS:
        return cached["baseline"]
    try:
        all_vols   = []
        end_time   = None
        remaining  = TIGHT_BASELINE_CANDLES
        first_chunk = True
        while remaining > 0:
            chunk_limit = min(remaining, BINGX_KLINE_MAX_LIMIT)
            candles = get_candles(symbol, limit=chunk_limit, interval=TIGHT_TIMEFRAME, end_time=end_time)
            if not candles:
                break
            # exclude the live in-progress candle - only present in the first
            # (most recent, end_time=None) chunk
            chunk = candles[:-1] if first_chunk else candles
            all_vols.extend(v(c) for c in chunk)
            end_time = int(candles[0]["time"]) - 1
            remaining -= len(candles)
            first_chunk = False
            if len(candles) < chunk_limit:
                break   # exchange returned fewer than asked - no more history available
            time.sleep(0.3)
        if len(all_vols) < 100:
            return cached["baseline"] if cached else None
        baseline = sum(all_vols) / len(all_vols)
        tight_baseline_cache[symbol] = {"baseline": baseline, "ts": now}
        return baseline
    except Exception as e:
        print(f"[TIGHT BASELINE ERROR] {symbol}: {e}")
        return cached["baseline"] if cached else None


def place_tight_order(symbol, side, entry, sl, tp):
    try:
        set_leverage_api(symbol, TIGHT_LEVERAGE)
        precision = symbol_precision.get(symbol, 4)
        risk_dist = abs(entry - sl)
        if risk_dist <= 0:
            return None
        # Fixed notional sizing: qty = (risk_usdt * leverage) / entry
        # Replaces risk/SL-distance formula which produced huge qty when SL was tight,
        # requiring hundreds of dollars of margin on a $5 risk trade.
        qty = round((TIGHT_RISK_USDT * TIGHT_LEVERAGE) / entry, precision)
        if qty <= 0:
            return None
        pos_side   = "LONG" if side == "BUY" else "SHORT"
        close_side = "SELL" if side == "BUY" else "BUY"
        order_id = place_market_order(symbol, side, qty, pos_side)
        print(f"[TIGHT ORDER] {symbol} {side} qty={qty} risk=${TIGHT_RISK_USDT}: {order_id}")
        if order_id != "N/A":
            time.sleep(0.5)
            sl_id = place_sl_order(symbol, close_side, pos_side, sl, qty)
            tp_id = place_tp_order(symbol, close_side, pos_side, tp, qty)
            tight_open_trades[str(order_id)] = {
                "symbol": symbol, "side": side, "entry": entry, "sl": sl, "tp": tp,
                "qty": qty, "sl_id": sl_id, "tp_id": tp_id,
                "close_side": close_side, "pos_side": pos_side,
                "risk_dist": risk_dist, "be_done": False,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
        return order_id
    except Exception as e:
        print(f"[TIGHT ORDER ERROR] {symbol}: {e}")
        return None


def check_tight_symbol(symbol):
    try:
        baseline = get_tight_volume_baseline(symbol)
        if not baseline or baseline <= 0:
            return

        candles = get_candles(symbol, limit=30, interval=TIGHT_TIMEFRAME)
        if len(candles) < 20:
            return
        live_candle = candles[-1]      # in-progress
        confirmed   = candles[:-1]     # closed candles

        live_vol   = v(live_candle)
        live_ratio = live_vol / baseline

        state = tight_watch.get(symbol)

        # ---- Not yet watching: look for the initial spike trigger ----
        if state is None:
            if live_ratio >= TIGHT_SPIKE_VOL_MULT:
                direction = "UP" if cl(live_candle) > o(live_candle) else "DOWN"
                tight_watch[symbol] = {
                    "state": "COOLDOWN", "direction": direction,
                    "spike_ts": time.time(),
                    "cooldown_highs": [], "cooldown_lows": [],
                    "last_processed_time": None,
                }
                send_tg(
                    "TIGHT SPIKE - " + symbol + " [" + direction + "]\n"
                    "Vol: " + str(round(live_ratio, 1)) + "x 3-day baseline\n"
                    "Watching for cooldown + re-entry...\nNiti Tight"
                )
            return

        # ---- Give up watching if it's been too long ----
        if time.time() - state["spike_ts"] > TIGHT_MAX_COOLDOWN_WAIT_SECONDS:
            tight_watch.pop(symbol, None)
            return

        if state["state"] == "COOLDOWN":
            if not confirmed:
                return
            last_confirmed = confirmed[-1]
            if state.get("last_processed_time") == last_confirmed["time"]:
                return   # already processed this closed candle
            state["last_processed_time"] = last_confirmed["time"]

            confirmed_ratio = v(last_confirmed) / baseline
            if confirmed_ratio < TIGHT_COOLDOWN_VOL_RATIO:
                state["cooldown_highs"].append(h(last_confirmed))
                state["cooldown_lows"].append(l(last_confirmed))
            else:
                # volume picked back up before a range was confirmed - restart the range build
                state["cooldown_highs"] = []
                state["cooldown_lows"]  = []

            if len(state["cooldown_highs"]) >= TIGHT_COOLDOWN_MIN_CANDLES:
                recent_highs = state["cooldown_highs"][-TIGHT_COOLDOWN_MIN_CANDLES:]
                recent_lows  = state["cooldown_lows"][-TIGHT_COOLDOWN_MIN_CANDLES:]
                range_high = max(recent_highs)
                range_low  = min(recent_lows)

                closes_r = [cl(c) for c in confirmed[-30:]]
                highs_r  = [h(c)  for c in confirmed[-30:]]
                lows_r   = [l(c)  for c in confirmed[-30:]]
                atr_vals = atr_series(highs_r, lows_r, closes_r, TIGHT_ATR_LEN)
                atr_now  = atr_vals[-1] if atr_vals else (range_high - range_low)

                range_width = range_high - range_low
                if atr_now > 0 and range_width <= atr_now * TIGHT_RANGE_MAX_ATR_MULT:
                    state["state"]      = "READY"
                    state["range_high"] = range_high
                    state["range_low"]  = range_low
                    state["atr_now"]    = atr_now
                # else: too wide to call "sideways" (likely slow-bleed, not real
                # consolidation) - keep accumulating candles and re-check next time

        elif state["state"] == "READY":
            range_high = state["range_high"]
            range_low  = state["range_low"]
            live_close = cl(live_candle)

            breakout_up   = live_close > range_high and live_ratio >= TIGHT_REENTRY_VOL_MULT
            breakout_down = live_close < range_low  and live_ratio >= TIGHT_REENTRY_VOL_MULT

            if not breakout_up and not breakout_down:
                return

            if len(tight_open_trades) >= TIGHT_MAX_CONCURRENT_TRADES:
                print(f"[TIGHT SKIP] {symbol} breakout ignored - max concurrent trades ({TIGHT_MAX_CONCURRENT_TRADES}) reached")
                tight_watch.pop(symbol, None)
                return

            side     = "BUY" if breakout_up else "SELL"
            entry    = live_close
            atr_now  = state["atr_now"]
            if side == "BUY":
                sl = range_low - atr_now * TIGHT_SL_ATR_BUFFER_MULT
            else:
                sl = range_high + atr_now * TIGHT_SL_ATR_BUFFER_MULT
            risk = abs(entry - sl)
            tight_watch.pop(symbol, None)
            if risk <= 0:
                return
            tp = round(entry + risk * TIGHT_RR_TP, 6) if side == "BUY" else round(entry - risk * TIGHT_RR_TP, 6)
            sl = round(sl, 6)

            trade_status = ""
            if tight_auto_trade_enabled:
                oid = place_tight_order(symbol, side, entry, sl, tp)
                trade_status = "\nOrder: " + str(oid) if oid and oid != "N/A" else "\nOrder failed"
            else:
                trade_status = "\nAuto-trade OFF"
            send_tg(
                "TIGHT SIGNAL - " + side + " - " + symbol + "\n------------------------------\n"
                "Entry: " + str(round(entry, 6)) + " | SL: " + str(sl) + " | TP (1:" + str(TIGHT_RR_TP) + "): " + str(tp) + "\n"
                "Breakout vol: " + str(round(live_ratio, 1)) + "x | BE at 1:" + str(TIGHT_BE_TRIGGER_R) + "R" +
                trade_status + "\n------------------------------\nNiti Tight"
            )
    except Exception as e:
        print(f"[TIGHT {symbol}] error: {e}")


def track_tight_trades():
    for oid in list(tight_open_trades.keys()):
        trade = tight_open_trades.get(oid)
        if not trade:
            continue
        try:
            symbol = trade["symbol"]

            # ---- Move SL to breakeven at TIGHT_BE_TRIGGER_R, full size kept ----
            if not trade.get("be_done"):
                current = get_current_price(symbol)
                risk_dist = trade.get("risk_dist", 0)
                if current > 0 and risk_dist > 0:
                    if trade["side"] == "BUY":
                        favorable_r = (current - trade["entry"]) / risk_dist
                    else:
                        favorable_r = (trade["entry"] - current) / risk_dist
                    if favorable_r >= TIGHT_BE_TRIGGER_R:
                        if trade.get("sl_id"):
                            cancel_order(symbol, trade["sl_id"])
                        new_sl_id = place_sl_order(symbol, trade["close_side"], trade["pos_side"], trade["entry"], trade["qty"])
                        trade["sl_id"]   = new_sl_id
                        trade["sl"]      = trade["entry"]
                        trade["be_done"] = True
                        print(f"[TIGHT BE] {symbol} - SL moved to breakeven at {favorable_r:.2f}R")

            sl_status = check_order_status(trade["sl_id"], symbol) if trade.get("sl_id") else ""
            if sl_status == "FILLED":
                if trade.get("tp_id"):
                    cancel_order(symbol, trade["tp_id"])
                result = "BE" if trade.get("be_done") else "SL"
                final_price = trade["sl"]
                leg_pnl = (final_price - trade["entry"]) * trade["qty"] if trade["side"] == "BUY" else (trade["entry"] - final_price) * trade["qty"]
                trade["pnl"]    = round(leg_pnl, 2)
                trade["result"] = result
                trade["label"]  = "Tight"
                daily_trades.append(trade)
                journal_closed_trade(trade)
                tight_open_trades.pop(oid, None)
                continue

            tp_status = check_order_status(trade["tp_id"], symbol) if trade.get("tp_id") else ""
            if tp_status == "FILLED":
                if trade.get("sl_id"):
                    cancel_order(symbol, trade["sl_id"])
                final_price = trade["tp"]
                leg_pnl = (final_price - trade["entry"]) * trade["qty"] if trade["side"] == "BUY" else (trade["entry"] - final_price) * trade["qty"]
                trade["pnl"]    = round(leg_pnl, 2)
                trade["result"] = "TP"
                trade["label"]  = "Tight"
                daily_trades.append(trade)
                journal_closed_trade(trade)
                tight_open_trades.pop(oid, None)
                continue
        except Exception as e:
            print(f"[TIGHT TRACK ERROR] {oid}: {e}")


def send_daily_summary():
    global daily_trades, last_summary_date
    nzt   = timezone(timedelta(hours=12))
    now   = datetime.now(nzt)
    today = now.date()
    if last_summary_date == today:
        return
    if now.hour != 23 or now.minute < 55:
        return
    last_summary_date = today
    total_pnl = round(sum(t.get("pnl", 0) for t in daily_trades), 2)
    wins      = sum(1 for t in daily_trades if t.get("pnl", 0) > 0)
    losses    = sum(1 for t in daily_trades if t.get("pnl", 0) <= 0)
    total     = len(daily_trades)
    win_rate  = round(wins / total * 100, 1) if total > 0 else 0
    sign      = "+" if total_pnl > 0 else ""
    lines = ["Daily Summary - " + today.strftime("%b %d, %Y"), "------------------------------"]
    for idx, t in enumerate(daily_trades, 1):
        p  = t.get("pnl", 0)
        ps = "+" if p > 0 else ""
        lines.append(str(idx) + ". [" + t.get("label", "?") + "] " + t["symbol"] + " " + t["side"] + " | " + t.get("result", "?") + " | " + ps + str(p) + " USDT")
    lines.append("------------------------------")
    lines.append("Trades: " + str(total) + " (" + str(wins) + "W/" + str(losses) + "L) | WR: " + str(win_rate) + "%")
    lines.append("Total PnL: " + sign + str(total_pnl) + " USDT")
    lines.append("------------------------------\nNiti Journal")
    send_journal("\n".join(lines))
    daily_trades = []


# ==================== TELEGRAM COMMANDS ====================
def handle_telegram_commands():
    global tight_auto_trade_enabled, fast_auto_trade_enabled
    offset = None
    while True:
        try:
            url    = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
            params = {"timeout": 30}
            if offset:
                params["offset"] = offset
            r = requests.get(url, params=params, timeout=35).json()
            for update in r.get("result", []):
                offset  = update["update_id"] + 1
                msg     = update.get("message", {})
                text    = msg.get("text", "").strip().lower()
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != str(TG_CHAT_ID):
                    continue
                if text == "/start":
                    tight_auto_trade_enabled = True
                    send_tg("Tight Auto-trade ON.")
                elif text == "/stop":
                    tight_auto_trade_enabled = False
                    send_tg("Tight Auto-trade OFF.")
                elif text == "/status":
                    t = "ON" if tight_auto_trade_enabled else "OFF"
                    f = "ON" if fast_auto_trade_enabled  else "OFF"
                    backoff = ""
                    if api_backoff_active():
                        backoff = f"\nAPI BACKOFF ACTIVE - {int(_api_backoff_until - time.time())}s remaining"
                    send_tg("Tight: " + t + " | Watching: " + str(len(tight_watch)) + " | Open: " + str(len(tight_open_trades)) +
                            "\nFast Signal: " + f + " | Open: " + str(len(fast_open_trades)) + backoff)
                elif text == "/fast_start":
                    fast_auto_trade_enabled = True
                    send_tg("Fast Signal Auto-trade ON.")
                elif text == "/fast_stop":
                    fast_auto_trade_enabled = False
                    send_tg("Fast Signal Auto-trade OFF.")
                elif text == "/fast_status":
                    f = "ON" if fast_auto_trade_enabled else "OFF"
                    send_tg("Fast Signal: " + f)
        except Exception as e:
            print(f"[TG CMD] error: {e}")
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
            print(f"[TRAIL LOOP ERROR] {e}")
        time.sleep(30)


def fast_diagnostic_check():
    """Prints a sample calculation for a known-liquid coin every cycle, so we can see
    in the logs whether the data pipeline (candles -> ratios) is actually alive,
    independent of whether a real signal happens to fire."""
    try:
        candles = get_candles("BTC-USDT", limit=100, interval=FAST_TIMEFRAME)
        if len(candles) < 60:
            print(f"[FAST DIAG] BTC-USDT - only got {len(candles)} candles (need ~60+)")
            return
        confirmed = candles[:-1]
        closes = [cl(c) for c in confirmed]
        highs  = [h(c) for c in confirmed]
        lows   = [l(c) for c in confirmed]
        vols   = [v(c) for c in confirmed]
        i = len(confirmed) - 1
        avg_vol = sum(vols[i - FAST_VOL_LB:i]) / FAST_VOL_LB
        ratio = vols[i] / avg_vol if avg_vol > 0 else 0
        box_high = max(highs[i - FAST_CONSOL_LOOKBACK:i])
        box_low  = min(lows[i - FAST_CONSOL_LOOKBACK:i])
        print(f"[FAST DIAG] BTC-USDT close={closes[i]} vol_ratio={ratio:.2f}x box=({box_low},{box_high}) candles={len(candles)}")
    except Exception as e:
        print(f"[FAST DIAG ERROR] {e}")


def fast_scan_loop():
    print("Fast Signal loop started - 3m | consolidation breakout | MTF filter + reworked time-exit (2026-07-11)")
    all_symbols = []
    while True:
        try:
            if not all_symbols:
                all_symbols = get_futures_symbols() or []
            liquid = get_liquid_symbols(
                all_symbols, min_quote_vol=FAST_MIN_QUOTE_VOL, max_n=None, exclude_top_n=FAST_EXCLUDE_TOP_N
            )
            print(f"[FAST SCAN] Scanning {len(liquid)} small/mid-cap liquid pairs for breakouts...")
            fast_diagnostic_check()
            for sym in liquid:
                check_fast(sym)
                time.sleep(0.15)
            print("[FAST SCAN] Done. Sleeping 180s...")
        except Exception as e:
            print(f"[FAST LOOP ERROR] {e}")
        time.sleep(180)


def tight_diagnostic_check():
    """Prints BTC-USDT's live volume ratio vs its 3-day baseline every cycle, so we
    can see in the logs whether the baseline fetch (limit=TIGHT_BASELINE_CANDLES)
    and the live-candle ratio calculation are actually working, independent of
    whether a real 20x spike happens to occur."""
    try:
        baseline = get_tight_volume_baseline("BTC-USDT")
        if not baseline:
            print("[TIGHT DIAG] BTC-USDT - baseline is None/empty, see CANDLES ERROR/SHORT logs above")
            return
        candles = get_candles("BTC-USDT", limit=5, interval=TIGHT_TIMEFRAME)
        if len(candles) < 2:
            print(f"[TIGHT DIAG] BTC-USDT - only got {len(candles)} recent candles")
            return
        live_ratio = v(candles[-1]) / baseline
        print(f"[TIGHT DIAG] BTC-USDT baseline_vol={baseline:.2f} live_vol={v(candles[-1]):.2f} ratio={live_ratio:.2f}x (need {TIGHT_SPIKE_VOL_MULT}x)")
    except Exception as e:
        print(f"[TIGHT DIAG ERROR] {e}")


def tight_scan_loop():
    print("Tight loop started - Stock Niti strategy: 1m volume-spike -> cooldown -> breakout re-entry (2026-07-11)")
    all_symbols = []
    while True:
        try:
            if not all_symbols:
                all_symbols = get_futures_symbols() or []
            symbols = get_liquid_symbols(all_symbols, min_quote_vol=TIGHT_MIN_QUOTE_VOL, max_n=TIGHT_MAX_SYMBOLS)
            print(f"[TIGHT SCAN] Scanning {len(symbols)}/{len(all_symbols)} liquid pairs | Watching={len(tight_watch)} | Open={len(tight_open_trades)} | Auto={tight_auto_trade_enabled}")
            tight_diagnostic_check()
            for sym in symbols:
                check_tight_symbol(sym)
                time.sleep(0.15)
            send_daily_summary()
            print("[TIGHT SCAN] Done.")
        except Exception as e:
            print(f"[TIGHT LOOP ERROR] {e}")
        time.sleep(TIGHT_SCAN_INTERVAL_SECONDS)


@app.route("/")
def health():
    return "Niti Tight (Stock Niti: Vol-Spike+Cooldown+Breakout, 1:4RR) + Fast Signal (Breakout+ATRTrail+ReworkedTimeExit+MTF)", 200




# ==================== COMBINED PATCH: TIGHT 2 SPEED FIX + FAST RISK FIX (2026-07-11) ====================
# Everything below OVERRIDES the earlier definitions (Python uses the last def).
# Fixes: (1) baseline fetch moved to a background thread so a scan pass drops
# from 15-20 min to ~90s, (2) spike detection also checks the last 3 CLOSED
# candles so spikes finishing between passes aren't missed, (3) cooldown now
# catches up on ALL closed candles since the last pass, (4) breakout check does
# the same. Strategy rules (20x spike / 5x re-entry / 1:4 / BE at 2R) unchanged.

# ---- FAST SIGNAL RISK FIX: risk per trade $1.5 -> $5 (2026-07-11) ----
# Position size = risk / SL-distance, so margins were tiny ($1.4-1.6) when SL
# was wide. $5 risk => ~3.3x bigger positions. "extended (half size)" trades
# will now risk $2.5. Overridden by the FAST_RISK_USDT env var if set on Render.
FAST_RISK_USDT = float(os.environ.get("FAST_RISK_USDT", 20.0))

TIGHT_SPIKE_CONFIRMED_LOOKBACK = int(os.environ.get("TIGHT_SPIKE_CONFIRMED_LOOKBACK", 3))
TIGHT_SCAN_SYMBOL_GAP          = float(os.environ.get("TIGHT_SCAN_SYMBOL_GAP", 0.1))
TIGHT_BASELINE_SYMBOL_GAP      = float(os.environ.get("TIGHT_BASELINE_SYMBOL_GAP", 0.5))

tight_symbols_current = []   # current scan universe, shared with the baseline refresher thread


def fetch_tight_baseline(symbol):
    """Fetch + cache the 3-day average 1m volume for ONE symbol. Called ONLY
    from the background refresher thread, never inline from the scan loop."""
    cached = tight_baseline_cache.get(symbol)
    try:
        all_vols    = []
        end_time    = None
        remaining   = TIGHT_BASELINE_CANDLES
        first_chunk = True
        while remaining > 0:
            chunk_limit = min(remaining, BINGX_KLINE_MAX_LIMIT)
            candles = get_candles(symbol, limit=chunk_limit, interval=TIGHT_TIMEFRAME, end_time=end_time)
            if not candles:
                break
            chunk = candles[:-1] if first_chunk else candles   # drop live in-progress candle
            all_vols.extend(v(c) for c in chunk)
            end_time = int(candles[0]["time"]) - 1
            remaining -= len(candles)
            first_chunk = False
            if len(candles) < chunk_limit:
                break
            time.sleep(0.3)
        if len(all_vols) < 100:
            return cached["baseline"] if cached else None
        baseline = sum(all_vols) / len(all_vols)
        tight_baseline_cache[symbol] = {"baseline": baseline, "ts": time.time()}
        return baseline
    except Exception as e:
        print(f"[TIGHT BASELINE ERROR] {symbol}: {e}")
        return cached["baseline"] if cached else None


def get_cached_baseline(symbol):
    """Cache-only lookup for the scan loop - NEVER triggers a fetch."""
    cached = tight_baseline_cache.get(symbol)
    return cached["baseline"] if cached else None


def tight_baseline_loop():
    """Background thread: keeps every symbol's 3-day baseline fresh so the scan
    loop never blocks. First full population of ~400 symbols takes ~10-15 min
    (watch Baseline=X/Y in the [TIGHT SCAN] log line)."""
    print("Tight baseline refresher started (background thread)")
    while True:
        try:
            symbols = list(tight_symbols_current)
            if not symbols:
                time.sleep(5)
                continue
            now = time.time()
            stale = [s for s in symbols
                     if s not in tight_baseline_cache
                     or now - tight_baseline_cache[s]["ts"] >= TIGHT_BASELINE_REFRESH_SECONDS]
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
            print(f"[TIGHT BASELINE LOOP ERROR] {e}")
            time.sleep(10)


def check_tight_symbol(symbol):
    """OVERRIDES the earlier version. Returns True if it hit the API, False if
    skipped (no baseline yet) so the scan loop can skip the pacing sleep."""
    try:
        baseline = get_cached_baseline(symbol)
        if not baseline or baseline <= 0:
            return False

        candles = get_candles(symbol, limit=30, interval=TIGHT_TIMEFRAME)
        if len(candles) < 20:
            return True
        live_candle = candles[-1]
        confirmed   = candles[:-1]

        live_ratio = v(live_candle) / baseline
        state = tight_watch.get(symbol)

        # ---- Spike trigger: live candle OR last few closed candles ----
        if state is None:
            spike_candle = None
            spike_ratio  = 0.0
            if live_ratio >= TIGHT_SPIKE_VOL_MULT:
                spike_candle, spike_ratio = live_candle, live_ratio
            else:
                for c in confirmed[-TIGHT_SPIKE_CONFIRMED_LOOKBACK:]:
                    r_c = v(c) / baseline
                    if r_c >= TIGHT_SPIKE_VOL_MULT and r_c > spike_ratio:
                        spike_candle, spike_ratio = c, r_c
            if spike_candle is not None:
                direction = "UP" if cl(spike_candle) > o(spike_candle) else "DOWN"
                tight_watch[symbol] = {
                    "state": "COOLDOWN", "direction": direction,
                    "spike_ts": time.time(),
                    "cooldown_highs": [], "cooldown_lows": [],
                    "last_processed_time": int(spike_candle["time"]),
                }
                print(f"[TIGHT SPIKE] {symbol} [{direction}] {round(spike_ratio,1)}x - watching")
            return True

        if time.time() - state["spike_ts"] > TIGHT_MAX_COOLDOWN_WAIT_SECONDS:
            tight_watch.pop(symbol, None)
            return True

        if state["state"] == "COOLDOWN":
            # Catch up on EVERY closed candle since the last pass
            last_t = int(state.get("last_processed_time") or 0)
            for c in [x for x in confirmed if int(x["time"]) > last_t]:
                state["last_processed_time"] = int(c["time"])
                if v(c) / baseline < TIGHT_COOLDOWN_VOL_RATIO:
                    state["cooldown_highs"].append(h(c))
                    state["cooldown_lows"].append(l(c))
                else:
                    state["cooldown_highs"] = []
                    state["cooldown_lows"]  = []
                    continue

                if len(state["cooldown_highs"]) >= TIGHT_COOLDOWN_MIN_CANDLES:
                    range_high = max(state["cooldown_highs"][-TIGHT_COOLDOWN_MIN_CANDLES:])
                    range_low  = min(state["cooldown_lows"][-TIGHT_COOLDOWN_MIN_CANDLES:])
                    closes_r = [cl(x) for x in confirmed[-30:]]
                    highs_r  = [h(x)  for x in confirmed[-30:]]
                    lows_r   = [l(x)  for x in confirmed[-30:]]
                    atr_vals = atr_series(highs_r, lows_r, closes_r, TIGHT_ATR_LEN)
                    atr_now  = atr_vals[-1] if atr_vals else (range_high - range_low)
                    if atr_now > 0 and (range_high - range_low) <= atr_now * TIGHT_RANGE_MAX_ATR_MULT:
                        state["state"]      = "READY"
                        state["range_high"] = range_high
                        state["range_low"]  = range_low
                        state["atr_now"]    = atr_now
                        break

        # plain `if` so COOLDOWN -> READY in this same pass checks breakout immediately
        if state.get("state") == "READY":
            range_high = state["range_high"]
            range_low  = state["range_low"]
            last_t = int(state.get("last_processed_time") or 0)
            candidates = [c for c in confirmed if int(c["time"]) > last_t] + [live_candle]

            breakout_up = breakout_down = False
            trigger_ratio = 0.0
            for c in candidates:
                if c is not live_candle:
                    state["last_processed_time"] = int(c["time"])
                c_ratio = v(c) / baseline
                if cl(c) > range_high and c_ratio >= TIGHT_REENTRY_VOL_MULT:
                    breakout_up, trigger_ratio = True, c_ratio
                    break
                if cl(c) < range_low and c_ratio >= TIGHT_REENTRY_VOL_MULT:
                    breakout_down, trigger_ratio = True, c_ratio
                    break

            if not breakout_up and not breakout_down:
                return True

            if len(tight_open_trades) >= TIGHT_MAX_CONCURRENT_TRADES:
                print(f"[TIGHT SKIP] {symbol} breakout ignored - max concurrent trades reached")
                tight_watch.pop(symbol, None)
                return True

            side    = "BUY" if breakout_up else "SELL"
            entry   = cl(live_candle)   # market order fills at current price
            atr_now = state["atr_now"]
            sl = range_low - atr_now * TIGHT_SL_ATR_BUFFER_MULT if side == "BUY" else range_high + atr_now * TIGHT_SL_ATR_BUFFER_MULT
            risk = abs(entry - sl)
            tight_watch.pop(symbol, None)
            if risk <= 0:
                return True
            tp = round(entry + risk * TIGHT_RR_TP, 6) if side == "BUY" else round(entry - risk * TIGHT_RR_TP, 6)
            sl = round(sl, 6)

            trade_status = ""
            if tight_auto_trade_enabled:
                oid = place_tight_order(symbol, side, entry, sl, tp)
                trade_status = "\nOrder: " + str(oid) if oid and oid != "N/A" else "\nOrder failed"
            else:
                trade_status = "\nAuto-trade OFF"
            send_tg(
                "TIGHT SIGNAL - " + side + " - " + symbol + "\n------------------------------\n"
                "Entry: " + str(round(entry, 6)) + " | SL: " + str(sl) + " | TP (1:" + str(TIGHT_RR_TP) + "): " + str(tp) + "\n"
                "Breakout vol: " + str(round(trigger_ratio, 1)) + "x | BE at 1:" + str(TIGHT_BE_TRIGGER_R) + "R" +
                trade_status + "\n------------------------------\nNiti Tight"
            )
        return True
    except Exception as e:
        print(f"[TIGHT {symbol}] error: {e}")
        return True


def tight_diagnostic_check():
    """OVERRIDES the earlier version - cache-only baseline lookup."""
    try:
        baseline = get_cached_baseline("BTC-USDT")
        if not baseline:
            print(f"[TIGHT DIAG] BTC-USDT baseline not cached yet - refresher warming up ({len(tight_baseline_cache)} symbols covered)")
            return
        candles = get_candles("BTC-USDT", limit=5, interval=TIGHT_TIMEFRAME)
        if len(candles) < 2:
            print(f"[TIGHT DIAG] BTC-USDT - only got {len(candles)} recent candles")
            return
        live_ratio = v(candles[-1]) / baseline
        print(f"[TIGHT DIAG] BTC-USDT baseline_vol={baseline:.2f} live_vol={v(candles[-1]):.2f} ratio={live_ratio:.2f}x (need {TIGHT_SPIKE_VOL_MULT}x)")
    except Exception as e:
        print(f"[TIGHT DIAG ERROR] {e}")


def tight_scan_loop():
    """OVERRIDES the earlier version - never blocks on baseline fetching."""
    global tight_symbols_current
    print("Tight loop started - Stock Niti strategy (2026-07-11, non-blocking baseline rework)")
    all_symbols = []
    while True:
        try:
            if not all_symbols:
                all_symbols = get_futures_symbols() or []
            symbols = get_liquid_symbols(all_symbols, min_quote_vol=TIGHT_MIN_QUOTE_VOL, max_n=TIGHT_MAX_SYMBOLS)
            tight_symbols_current = symbols
            covered = sum(1 for s in symbols if s in tight_baseline_cache)
            print(f"[TIGHT SCAN] Scanning {len(symbols)}/{len(all_symbols)} liquid pairs | Baseline={covered}/{len(symbols)} | Watching={len(tight_watch)} | Open={len(tight_open_trades)} | Auto={tight_auto_trade_enabled}")
            tight_diagnostic_check()
            t0 = time.time()
            for sym in symbols:
                if check_tight_symbol(sym):
                    time.sleep(TIGHT_SCAN_SYMBOL_GAP)
            print(f"[TIGHT SCAN] Done in {int(time.time() - t0)}s.")
            send_daily_summary()
        except Exception as e:
            print(f"[TIGHT LOOP ERROR] {e}")
        time.sleep(TIGHT_SCAN_INTERVAL_SECONDS)


Thread(target=tight_baseline_loop, daemon=True).start()
# ==================== END TIGHT 2 SPEED FIX ====================

if __name__ == "__main__":
    Thread(target=tight_scan_loop,          daemon=True).start()
    Thread(target=fast_scan_loop,           daemon=True).start()
    Thread(target=trailing_loop,            daemon=True).start()
    Thread(target=handle_telegram_commands, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
