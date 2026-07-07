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
LEVERAGE          = int(os.environ.get("LEVERAGE", 10))
FAST_TRADE_AMOUNT = float(os.environ.get("FAST_TRADE_AMOUNT", 20))

BASE_URL = "https://open-api.bingx.com"

# ==================== TIGHT 2 CONFIG ====================
TIMEFRAME          = "15m"
RSI_LEN            = 14
VOLUME_LOOKBACK    = 100
VOLUME_MULTIPLIER  = 2                          # UNCHANGED - user explicitly wants this kept
EMA50_LEN          = 50
EMA200_LEN         = 200
ADX_LEN            = 14
ADX_MIN            = int(os.environ.get("TIGHT_ADX_MIN", 26))     # raised from 20 - fewer false-trend entries on squeeze-driven spikes. TBD/tunable.
SWING_LOOKBACK     = 10
MIN_PRICE          = 0.001
MIN_RISK_PCT       = 0.1
MAX_RISK_PCT       = float(os.environ.get("TIGHT_MAX_RISK_PCT", 1.8))   # tightened from 3.0 - keeps fixed-$ sizing from producing oversized qty on wide sweeps
TIGHT_MIN_QUOTE_VOL = float(os.environ.get("TIGHT_MIN_QUOTE_VOL", 1_000_000))
TIGHT_MAX_SYMBOLS   = 400

# Liquidity sweep
SWEEP_LOOKBACK     = 15
SWEEP_MIN_PCT      = 0.05
SWEEP_WINDOW       = 3

# Adaptive regime detection
ATR_LEN            = 14
ATR_BASE_LEN       = 50
RANGE_ENTER_RATIO  = 0.6
TREND_ENTER_RATIO  = 1.0
RANGE_BOUNDARY_LEN = 20

# Dynamic SL / entry RR reference (TP2 removed - see TIGHT_TRAIL_* below)
SL_ATR_BUFFER_MULT = 0.3
RR_TP1             = 2.0
RANGE_RR           = 2.0

# Fixed-$ risk sizing (TIGHT 2) - replaces margin-based sizing, mirrors Fast Signal's model
TIGHT_RISK_USDT    = float(os.environ.get("TIGHT_RISK_USDT", 5.0))

# Trailing (replaces TP2 entirely) - after TP1 fills: breakeven immediately, then trail uncapped
TIGHT_TRAIL_PCT    = float(os.environ.get("TIGHT_TRAIL_PCT", 2.5))   # TBD/tunable

# Cooldown after consecutive losses
LOSS_STREAK_N      = 3
COOLDOWN_MINUTES   = 300

# Efficiency Ratio coin-quality pre-filter
ER_LOOKBACK        = 50
ER_MIN             = float(os.environ.get("TIGHT_ER_MIN", 0.38))   # raised from 0.25 - filters choppier coins. TBD/tunable.

# Confidence-based position sizing (used as a size-tier multiplier alongside fixed-$ risk)
CONF_WEAK_MIN      = 40
CONF_GOOD_MIN      = 60
CONF_STRONG_MIN    = 80
TIER_WEAK_MULT     = 0.5
TIER_NORMAL_MULT   = 1.0
TIER_STRONG_MULT   = 1.5

# ---- New market-regime filters (all independently toggleable - added 2026-07-08) ----
BTC_SYMBOL                 = "BTC-USDT"
BTC_REGIME_FILTER_ENABLED  = os.environ.get("BTC_REGIME_FILTER_ENABLED", "true").lower() == "true"
BTC_UNCERTAIN_RATIO        = float(os.environ.get("BTC_UNCERTAIN_RATIO", 1.5))   # BTC's own ATR-ratio above this = elevated uncertainty -> halve alt size
VOL_COOLDOWN_ATR_RATIO     = float(os.environ.get("VOL_COOLDOWN_ATR_RATIO", 2.0))  # BTC ATR-ratio above this -> proactive market-wide cooldown

MTF_FILTER_ENABLED         = os.environ.get("MTF_FILTER_ENABLED", "true").lower() == "true"
MTF_INTERVAL               = os.environ.get("MTF_INTERVAL", "1h")

FUNDING_FILTER_ENABLED     = os.environ.get("FUNDING_FILTER_ENABLED", "true").lower() == "true"
FUNDING_RATE_EXTREME_PCT   = float(os.environ.get("FUNDING_RATE_EXTREME_PCT", 0.05))   # % per interval considered "crowded"

EXPOSURE_CAP_ENABLED       = os.environ.get("EXPOSURE_CAP_ENABLED", "true").lower() == "true"
MAX_SAME_DIRECTION_TRADES  = int(os.environ.get("MAX_SAME_DIRECTION_TRADES", 4))

# ==================== FAST SIGNAL CONFIG ====================
FAST_TIMEFRAME          = "3m"
FAST_MIN_QUOTE_VOL      = float(os.environ.get("FAST_MIN_QUOTE_VOL", 2_000_000))
FAST_MAX_SYMBOLS        = 150
FAST_CONSOL_LOOKBACK    = 20
FAST_BREAKOUT_ATR_MULT  = 0.3
FAST_VOL_MULT           = 3.0                    # UNCHANGED
FAST_VOL_LB             = 20
FAST_ATR_LEN            = 14
FAST_SL_ATR_MULT        = 1.2
FAST_RISK_USDT          = float(os.environ.get("FAST_RISK_USDT", 1.5))   # UNCHANGED - already correct model, used as the reference for Tight 2's fix
FAST_EXCLUDE_TOP_N       = 75
FAST_EXTENSION_LOOKBACK  = 20
FAST_EXTENSION_LIMIT     = 4.0
FAST_EXTENSION_MULT      = 0.5
FAST_MARGIN_CAP_MULT     = 5.0
FAST_TP1_RR             = float(os.environ.get("FAST_TP1_RR", 0.8))   # smaller/more aggressive first target (was 2.0) - locks profit fast on pump-and-fade moves. TBD/tunable.
FAST_TRAIL_ACTIVATE_RR  = 1.0

# Retest-confirm REMOVED (was adding a 1-candle delay that hurt entry timing on fast-moving
# small caps). Replaced with a same-candle close-position filter - no waiting required.
FAST_CLOSE_POSITION_MIN = float(os.environ.get("FAST_CLOSE_POSITION_MIN", 0.7))   # breakout candle must close in the outer 30% of its own range

# ATR-adaptive trailing (replaces fixed FAST_TRAIL_PCT) - a fixed % trail can't keep up with
# the intra-candle jumps these coins produce.
FAST_TRAIL_ATR_MULT     = float(os.environ.get("FAST_TRAIL_ATR_MULT", 1.5))    # TBD/tunable
FAST_TRAIL_PCT_FALLBACK = float(os.environ.get("FAST_TRAIL_PCT_FALLBACK", 3.0))  # only used if ATR unavailable

# Progress-based time exit (replaces the old fixed-timeout version - 2026-07-08) - instead
# of force-closing every trade at a fixed age regardless of price action, check at each
# interval whether the trade has moved favorably; if yes, extend; if it's flat/dead, exit.
FAST_PROGRESS_CHECK_SECONDS = int(os.environ.get("FAST_PROGRESS_CHECK_SECONDS", 600))   # 10 min - how often to check
FAST_PROGRESS_MIN_R         = float(os.environ.get("FAST_PROGRESS_MIN_R", 0.3))         # must have moved at least this many R favorably to earn an extension
FAST_MAX_HOLD_CAP_SECONDS   = int(os.environ.get("FAST_MAX_HOLD_CAP_SECONDS", 2400))    # 40 min - absolute hard cap regardless of progress

# ==================== GLOBAL STATE ====================
tight_auto_trade_enabled = False
fast_auto_trade_enabled  = False
symbol_precision   = {}
symbol_max_lev     = {}
tight_open_trades  = {}
tight_alerted      = set()
fast_open_trades   = {}
fast_alerted       = set()
daily_trades       = []
last_summary_date  = None

regime_state         = {}
tight_loss_streak    = 0
tight_cooldown_until = None

btc_regime_cache = {"ratio": 1.0, "ts": 0.0}
mtf_cache        = {}


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


def get_liquid_symbols(symbols, min_quote_vol=FAST_MIN_QUOTE_VOL, max_n=FAST_MAX_SYMBOLS, exclude_top_n=0):
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


def get_candles(symbol, limit=350, interval="15m"):
    params = build_signed_params({"symbol": symbol, "interval": interval, "limit": limit})
    url = BASE_URL + "/openApi/swap/v3/quote/klines"
    r = requests.get(url, params=params,
                     headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
    candles = r.get("data", [])
    if not isinstance(candles, list):
        return []
    candles.sort(key=lambda x: x["time"])
    return candles


def get_current_price(symbol):
    try:
        url = BASE_URL + "/openApi/swap/v2/quote/price"
        r = requests.get(url, params={"symbol": symbol}, timeout=5).json()
        return float(r.get("data", {}).get("price", 0))
    except Exception:
        return 0


def get_funding_rate(symbol):
    """NOTE: endpoint/field names not yet verified against current BingX docs in this
    conversation - verify before relying on this live, same caveat as cancel_order()."""
    try:
        url = BASE_URL + "/openApi/swap/v2/quote/premiumIndex"
        r = requests.get(url, params={"symbol": symbol}, timeout=5).json()
        return float(r.get("data", {}).get("lastFundingRate", 0)) * 100
    except Exception as e:
        print(f"[FUNDING ERROR] {symbol}: {e}")
        return 0.0


# ==================== INDICATORS ====================
def ema_series(closes, period):
    k = 2 / (period + 1)
    ema = closes[0]
    result = []
    for c in closes:
        ema = c * k + ema * (1 - k)
        result.append(ema)
    return result


def adx_series(highs, lows, closes, period=14):
    n = len(closes)
    if n < period + 2:
        return [0.0] * n

    tr_list, pdm_list, ndm_list = [], [], []
    for i in range(1, n):
        high_diff = highs[i] - highs[i-1]
        low_diff  = lows[i-1] - lows[i]
        tr  = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        pdm = high_diff if high_diff > low_diff and high_diff > 0 else 0
        ndm = low_diff  if low_diff > high_diff and low_diff  > 0 else 0
        tr_list.append(tr)
        pdm_list.append(pdm)
        ndm_list.append(ndm)

    def smooth(lst, p):
        result = [sum(lst[:p])]
        for i in range(p, len(lst)):
            result.append(result[-1] - result[-1] / p + lst[i])
        return result

    atr = smooth(tr_list,  period)
    pDM = smooth(pdm_list, period)
    nDM = smooth(ndm_list, period)

    dx_list = []
    for i in range(len(atr)):
        pdi = 100 * pDM[i] / atr[i] if atr[i] != 0 else 0
        ndi = 100 * nDM[i] / atr[i] if atr[i] != 0 else 0
        dx  = 100 * abs(pdi - ndi) / (pdi + ndi) if (pdi + ndi) != 0 else 0
        dx_list.append(dx)

    adx_smooth = [sum(dx_list[:period]) / period]
    for i in range(period, len(dx_list)):
        adx_smooth.append((adx_smooth[-1] * (period - 1) + dx_list[i]) / period)

    padding = n - len(adx_smooth)
    return [0.0] * padding + adx_smooth


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


def vwap_series(candles):
    cum_tp_vol = 0.0
    cum_vol    = 0.0
    last_day   = None
    result = []
    for c in candles:
        candle_day = datetime.fromtimestamp(int(c["time"]) / 1000, tz=timezone.utc).date()
        if candle_day != last_day:
            cum_tp_vol = 0.0
            cum_vol    = 0.0
            last_day   = candle_day
        h  = float(c["high"])
        l  = float(c["low"])
        cl = float(c["close"])
        v  = float(c["volume"])
        tp = (h + l + cl) / 3
        cum_tp_vol += tp * v
        cum_vol    += v
        result.append(cum_tp_vol / cum_vol if cum_vol > 0 else tp)
    return result


def rsi_series(closes, period=14):
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    if len(gains) < period:
        return [50.0] * len(closes)
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    rsi_vals = [50.0] * (period + 1)
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = avg_gain / avg_loss if avg_loss != 0 else 100
        rsi_vals.append(100 - 100 / (1 + rs))
    return rsi_vals


def efficiency_ratio(closes, period=ER_LOOKBACK):
    if len(closes) < period + 1:
        return 0.0
    net_change = abs(closes[-1] - closes[-1 - period])
    path_sum = sum(abs(closes[-i] - closes[-i - 1]) for i in range(1, period + 1))
    return net_change / path_sum if path_sum > 0 else 0.0


# ==================== LIQUIDITY SWEEP DETECTION ====================
def detect_bull_sweep(lows, closes, idx, lookback=SWEEP_LOOKBACK, min_pct=SWEEP_MIN_PCT):
    if idx - lookback < 0:
        return False, None, None
    pool_low = min(lows[idx - lookback:idx])
    wick_low = lows[idx]
    if wick_low < pool_low * (1 - min_pct / 100) and closes[idx] > pool_low:
        return True, wick_low, pool_low
    return False, None, None


def detect_bear_sweep(highs, closes, idx, lookback=SWEEP_LOOKBACK, min_pct=SWEEP_MIN_PCT):
    if idx - lookback < 0:
        return False, None, None
    pool_high = max(highs[idx - lookback:idx])
    wick_high = highs[idx]
    if wick_high > pool_high * (1 + min_pct / 100) and closes[idx] < pool_high:
        return True, wick_high, pool_high
    return False, None, None


def sweep_recent(highs, lows, closes, idx, bull=True, window=SWEEP_WINDOW):
    floor = max(idx - window, 0)
    for j in range(idx, floor - 1, -1):
        if bull:
            ok, pt, pool = detect_bull_sweep(lows, closes, j)
        else:
            ok, pt, pool = detect_bear_sweep(highs, closes, j)
        if ok:
            return True, pt, pool
    return False, None, None


# ==================== REGIME DETECTION ====================
def compute_atr_ratio(highs, lows, closes, atr_vals):
    """Shared helper: current ATR% vs its own baseline. Used for both per-symbol regime
    (TREND/RANGE) and the BTC-wide uncertainty/cooldown checks."""
    atr_pct_series = []
    for i in range(len(closes)):
        atr_pct_series.append((atr_vals[i] / closes[i] * 100) if closes[i] > 0 else 0.0)
    base_len = min(ATR_BASE_LEN, len(atr_pct_series))
    baseline = sum(atr_pct_series[-base_len:]) / base_len if base_len > 0 else 1.0
    atr_pct_now = atr_pct_series[-1]
    ratio = atr_pct_now / baseline if baseline > 0 else 1.0
    return ratio


def update_regime(symbol, highs, lows, closes, atr_vals):
    ratio = compute_atr_ratio(highs, lows, closes, atr_vals)
    prev = regime_state.get(symbol, "TREND")
    if ratio <= RANGE_ENTER_RATIO:
        regime_state[symbol] = "RANGE"
    elif ratio >= TREND_ENTER_RATIO:
        regime_state[symbol] = "TREND"
    else:
        regime_state[symbol] = prev
    return regime_state[symbol], ratio


def get_btc_regime():
    """Cached (60s) BTC ATR-ratio, used to gate/downsize alt entries when BTC itself is
    in an elevated-uncertainty regime, and to trigger a proactive volatility cooldown."""
    now = time.time()
    if now - btc_regime_cache["ts"] < 60:
        return btc_regime_cache["ratio"]
    try:
        candles = get_candles(BTC_SYMBOL, limit=100, interval="15m")
        if len(candles) < 60:
            return btc_regime_cache["ratio"]
        closes = [cl(c) for c in candles]
        highs  = [h(c) for c in candles]
        lows   = [l(c) for c in candles]
        atr_vals = atr_series(highs, lows, closes, ATR_LEN)
        ratio = compute_atr_ratio(highs, lows, closes, atr_vals)
        btc_regime_cache["ratio"] = ratio
        btc_regime_cache["ts"] = now
        return ratio
    except Exception as e:
        print(f"[BTC REGIME ERROR] {e}")
        return btc_regime_cache["ratio"]


def get_mtf_trend(symbol):
    """Cached (5 min) higher-timeframe (MTF_INTERVAL) EMA200 trend direction, used to
    require the 15m Tight 2 signal direction agrees with the bigger picture."""
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


def count_open_direction(side):
    return sum(1 for t in tight_open_trades.values() if t["side"] == side)


# ==================== CONFIDENCE SCORING ====================
def confidence_score(vol_ratio, adx_now, er_now, rsi_now, rsi_low, rsi_high, sweep_depth_pct):
    score = 0
    if vol_ratio >= 3.5:
        score += 25
    elif vol_ratio >= VOLUME_MULTIPLIER:
        score += 15

    if adx_now >= 35:
        score += 25
    elif adx_now >= 25:
        score += 15

    if er_now >= 0.6:
        score += 25
    elif er_now >= 0.4:
        score += 15

    mid = (rsi_low + rsi_high) / 2
    band_half = (rsi_high - rsi_low) / 2
    if band_half > 0:
        closeness = max(0.0, 1 - (abs(rsi_now - mid) / band_half))
        score += round(15 * closeness)

    depth_ratio = sweep_depth_pct / SWEEP_MIN_PCT if SWEEP_MIN_PCT > 0 else 1
    score += min(10, round(depth_ratio * 3))

    return min(100, score)


def tier_mult_for_score(score):
    if score < CONF_WEAK_MIN:
        return None
    if score < CONF_GOOD_MIN:
        return TIER_WEAK_MULT
    if score < CONF_STRONG_MIN:
        return TIER_NORMAL_MULT
    return TIER_STRONG_MULT


# ==================== TELEGRAM ====================
def send_tg(msg, chat_id=None):
    cid = chat_id or TG_CHAT_ID
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": cid, "text": msg, "parse_mode": "HTML"}, timeout=10)


def send_journal(msg):
    if TG_JOURNAL_ID:
        send_tg(msg, chat_id=TG_JOURNAL_ID)


def journal_closed_trade(trade):
    """Single consolidated journal entry - only called for trades that were actually
    placed (auto-trade ON, real order id) and have now fully closed. Replaces the old
    per-signal 'New Trade' and per-leg 'TP1 hit' journal messages."""
    sign = "+" if trade.get("pnl", 0) > 0 else ""
    send_journal(
        "Trade Closed [" + trade.get("label", "?") + "] - " + trade["symbol"] + "\n"
        "------------------------------\n"
        "Side  : " + trade["side"] + "\nEntry : " + str(trade["entry"]) + "\n"
        "Result: " + trade.get("result", "?") + "\n"
        "PnL   : " + sign + str(trade.get("pnl", 0)) + " USDT\n"
        "------------------------------\nNiti Journal"
    )


def _update_loss_streak(total_pnl):
    global tight_loss_streak, tight_cooldown_until
    if total_pnl < 0:
        tight_loss_streak += 1
    else:
        tight_loss_streak = 0
    if tight_loss_streak >= LOSS_STREAK_N:
        tight_cooldown_until = datetime.now(timezone.utc) + timedelta(minutes=COOLDOWN_MINUTES)
        tight_loss_streak = 0
        send_journal(f"Tight 2 cooldown triggered - pausing new entries for {COOLDOWN_MINUTES} minutes\nNiti Journal")


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
    return r.get("data", {}).get("order", {}).get("orderId", "N/A")


def place_sl_order(symbol, close_side, pos_side, sl_price, qty):
    """NOTE: reduceOnly added as a safety net against a dangling order re-opening an
    unintended position after the original position is already closed - verify this
    parameter name/behavior against current BingX hedge-mode docs before relying on it
    live, same caveat as cancel_order()."""
    url = BASE_URL + "/openApi/swap/v2/trade/order"
    params = build_signed_params({
        "symbol": symbol, "side": close_side, "positionSide": pos_side,
        "type": "STOP_MARKET", "stopPrice": round(sl_price, 6), "quantity": qty,
        "reduceOnly": "true",
    })
    r = requests.post(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
    print(f"[SL] {symbol}: {r}")
    return r.get("data", {}).get("order", {}).get("orderId", "N/A")


def place_tp_order(symbol, close_side, pos_side, tp_price, qty):
    """NOTE: reduceOnly added - see place_sl_order() comment above."""
    url = BASE_URL + "/openApi/swap/v2/trade/order"
    params = build_signed_params({
        "symbol": symbol, "side": close_side, "positionSide": pos_side,
        "type": "TAKE_PROFIT_MARKET", "stopPrice": round(tp_price, 6), "quantity": qty,
        "reduceOnly": "true",
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
        # Cancel BOTH counterpart resting orders - previously only sl_id was cancelled here,
        # leaving a dangling tp1_id order behind if TP1 hadn't filled yet.
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
        journal_closed_trade(trade)
        del fast_open_trades[symbol]
        print(f"[FAST CLOSE] {symbol} {reason}")
    except Exception as e:
        print(f"[FAST CLOSE ERROR] {symbol}: {e}")


def close_tight_position(oid, reason=""):
    """Closes the trailing runner leg (post-TP1, TREND mode) - market-closes the
    remaining half_qty, cancels the resting breakeven SL, journals the consolidated result."""
    trade = tight_open_trades.get(oid)
    if not trade:
        return
    try:
        symbol     = trade["symbol"]
        pos_side   = trade["pos_side"]
        close_side = trade["close_side"]
        remaining  = trade.get("half_qty", 0)
        entry      = trade["entry"]
        if remaining > 0 and tight_auto_trade_enabled:
            place_market_order(symbol, close_side, remaining, pos_side)
        if trade.get("sl_id"):
            cancel_order(symbol, trade["sl_id"])
        current = get_current_price(symbol)
        if trade["side"] == "BUY":
            leg_pnl = (current - entry) * remaining
        else:
            leg_pnl = (entry - current) * remaining
        total_pnl = round(trade.get("partial_pnl", 0.0) + leg_pnl, 2)
        trade["pnl"]    = total_pnl
        trade["result"] = reason
        trade["label"]  = "Tight"
        daily_trades.append(trade)
        _update_loss_streak(total_pnl)
        journal_closed_trade(trade)
        tight_open_trades.pop(oid, None)
        print(f"[TIGHT CLOSE] {symbol} {reason}")
    except Exception as e:
        print(f"[TIGHT CLOSE ERROR] {oid}: {e}")


def place_tight_order(symbol, side, entry, sl, tp1, trade_amount_mult, mode):
    """trade_amount_mult is the confidence-tier size multiplier (0.5 / 1.0 / 1.5);
    actual position size now comes from fixed $ risk (TIGHT_RISK_USDT), not margin."""
    try:
        set_leverage_api(symbol, LEVERAGE)
        precision   = symbol_precision.get(symbol, 4)
        risk_dist   = abs(entry - sl)
        if risk_dist <= 0:
            return None
        risk_usdt   = TIGHT_RISK_USDT * trade_amount_mult
        total_qty   = round(risk_usdt / risk_dist, precision)
        if total_qty <= 0:
            return None
        pos_side   = "LONG"  if side == "BUY" else "SHORT"
        close_side = "SELL"  if side == "BUY" else "BUY"
        order_id = place_market_order(symbol, side, total_qty, pos_side)
        print(f"[TIGHT ORDER] {symbol} {side} qty={total_qty} risk=${round(risk_usdt,2)}: {order_id}")
        if order_id != "N/A":
            time.sleep(0.5)
            sl_id = place_sl_order(symbol, close_side, pos_side, sl, total_qty)
            if mode == "TREND":
                half_qty = round(total_qty / 2, precision)
                tp1_id = place_tp_order(symbol, close_side, pos_side, tp1, half_qty)
                tight_open_trades[str(order_id)] = {
                    "symbol": symbol, "side": side, "entry": entry, "mode": mode,
                    "sl": sl, "tp1": tp1, "qty": total_qty, "half_qty": half_qty,
                    "sl_id": sl_id, "tp1_id": tp1_id,
                    "close_side": close_side, "pos_side": pos_side,
                    "breakeven_done": False, "trailing_active": False,
                    "partial_pnl": 0.0,
                    "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
                }
            else:  # RANGE mode - single target, full qty, unchanged behavior
                tp1_id = place_tp_order(symbol, close_side, pos_side, tp1, total_qty)
                tight_open_trades[str(order_id)] = {
                    "symbol": symbol, "side": side, "entry": entry, "mode": mode,
                    "sl": sl, "tp1": tp1, "qty": total_qty, "half_qty": total_qty,
                    "sl_id": sl_id, "tp1_id": tp1_id,
                    "close_side": close_side, "pos_side": pos_side,
                    "breakeven_done": True, "trailing_active": False,
                    "partial_pnl": 0.0,
                    "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
                }
        return order_id
    except Exception as e:
        print(f"[TIGHT ORDER ERROR] {symbol}: {e}")
        return None


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
                status = check_tight_order_status(trade["tp1_id"], symbol)
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
                    # Trailing engages via update_fast_trailing()'s "activated" flag from here.

            sl_status = check_tight_order_status(trade["sl_id"], symbol) if trade.get("sl_id") else ""
            if sl_status == "FILLED":
                # Cancel the counterpart TP1 order if it hasn't filled yet - previously left dangling.
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
                journal_closed_trade(trade)
                del fast_open_trades[symbol]
                continue

            # Progress-based time exit - check at each interval whether price has moved
            # favorably by at least FAST_PROGRESS_MIN_R; if yes, extend the deadline; if
            # flat/dead, exit now. A hard cap still applies regardless of progress.
            now_ts = time.time()
            if now_ts >= trade.get("next_check_ts", now_ts + 1):
                opened_ts = trade.get("opened_ts", now_ts)
                if now_ts - opened_ts >= FAST_MAX_HOLD_CAP_SECONDS:
                    print(f"[FAST TIME EXIT] {symbol} - hard cap reached")
                    close_fast_position(symbol, "TimeExit")
                    continue
                current = get_current_price(symbol)
                risk_dist = trade.get("risk_dist", 0)
                if current > 0 and risk_dist > 0:
                    if trade["side"] == "BUY":
                        favorable_r = (current - trade["entry"]) / risk_dist
                    else:
                        favorable_r = (trade["entry"] - current) / risk_dist
                    if favorable_r >= FAST_PROGRESS_MIN_R:
                        trade["next_check_ts"] = now_ts + FAST_PROGRESS_CHECK_SECONDS
                        print(f"[FAST PROGRESS] {symbol} extended - {favorable_r:.2f}R favorable")
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


def update_tight_trailing():
    """Post-TP1 runner leg (TREND mode only) - trails the remaining half_qty with no
    fixed second target, replacing the old TP2. Breakeven SL was already placed the
    moment TP1 filled (see track_tight_trades Step 1); this just trails it further."""
    for oid in list(tight_open_trades.keys()):
        trade = tight_open_trades.get(oid)
        if not trade or not trade.get("trailing_active"):
            continue
        try:
            symbol  = trade["symbol"]
            current = get_current_price(symbol)
            if current <= 0:
                continue
            side       = trade["side"]
            trail_pct  = TIGHT_TRAIL_PCT / 100
            if side == "BUY":
                if current > trade["trail_price"]:
                    trade["trail_price"] = current
                trail_sl = trade["trail_price"] * (1 - trail_pct)
                if current <= trail_sl:
                    close_tight_position(oid, "Trail")
            else:
                if current < trade["trail_price"]:
                    trade["trail_price"] = current
                trail_sl = trade["trail_price"] * (1 + trail_pct)
                if current >= trail_sl:
                    close_tight_position(oid, "Trail")
        except Exception as e:
            print(f"[TIGHT TRAIL ERROR] {oid}: {e}")


def check_tight_order_status(order_id, symbol):
    try:
        params = build_signed_params({"symbol": symbol, "orderId": order_id})
        url = BASE_URL + "/openApi/swap/v2/trade/order"
        r = requests.get(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        return r.get("data", {}).get("order", {}).get("status", "")
    except Exception:
        return ""


def track_tight_trades():
    to_remove = []
    for oid, trade in list(tight_open_trades.items()):
        try:
            symbol = trade["symbol"]
            entry  = trade["entry"]
            side   = trade["side"]
            mode   = trade.get("mode", "TREND")

            # --- Step 1: TP1 fill (TREND mode only) -> bank partial PnL, move remaining
            # to breakeven immediately, then hand off to the trailing loop (TP2 removed). ---
            if mode == "TREND" and not trade.get("breakeven_done") and trade.get("tp1_id"):
                tp1_status = check_tight_order_status(trade["tp1_id"], symbol)
                if tp1_status == "FILLED":
                    leg_qty = trade["half_qty"]
                    leg_pnl = (trade["tp1"] - entry) * leg_qty if side == "BUY" else (entry - trade["tp1"]) * leg_qty
                    trade["partial_pnl"] = trade.get("partial_pnl", 0.0) + leg_pnl
                    if trade.get("sl_id"):
                        cancel_order(symbol, trade["sl_id"])
                    new_sl_id = place_sl_order(symbol, trade["close_side"], trade["pos_side"], entry, leg_qty)
                    trade["sl_id"]           = new_sl_id
                    trade["sl"]              = entry
                    trade["breakeven_done"]  = True
                    trade["trailing_active"] = True
                    trade["trail_price"]     = trade["tp1"]
                    send_journal(
                        "TP1 hit [Tight] - " + symbol + "\nBanked: " + str(round(leg_pnl, 2)) +
                        " USDT | Remaining moved to breakeven, trailing started\nNiti Journal"
                    )
                    continue

            # --- Trailing already active: exit is driven by update_tight_trailing() /
            # close_tight_position(). Only a safety check here in case the exchange-side
            # breakeven SL fires on its own between trailing-loop ticks. ---
            if trade.get("trailing_active"):
                sl_status = check_tight_order_status(trade["sl_id"], symbol) if trade.get("sl_id") else ""
                if sl_status == "FILLED":
                    total_pnl = round(trade.get("partial_pnl", 0.0), 2)   # breakeven fill -> ~0 on this leg
                    trade["pnl"]    = total_pnl
                    trade["result"] = "BE"
                    trade["label"]  = "Tight"
                    daily_trades.append(trade)
                    to_remove.append(oid)
                    _update_loss_streak(total_pnl)
                    journal_closed_trade(trade)
                continue

            # --- RANGE mode (or TREND before TP1 fills): single exit on the full qty ---
            target_id    = trade.get("tp1_id")
            target_price = trade.get("tp1")
            final_qty    = trade["qty"]

            sl_status  = check_tight_order_status(trade["sl_id"], symbol) if trade.get("sl_id") else ""
            tgt_status = check_tight_order_status(target_id, symbol) if target_id else ""

            final_price = None
            result      = None
            if sl_status == "FILLED":
                final_price = trade["sl"]
                result      = "SL"
                if target_id:
                    cancel_order(symbol, target_id)   # cancel dangling TP - previously left resting
            elif tgt_status == "FILLED":
                final_price = target_price
                result      = "TP"
                if trade.get("sl_id"):
                    cancel_order(symbol, trade["sl_id"])   # cancel dangling SL - previously left resting

            if final_price is not None:
                leg_pnl   = (final_price - entry) * final_qty if side == "BUY" else (entry - final_price) * final_qty
                total_pnl = round(trade.get("partial_pnl", 0.0) + leg_pnl, 2)

                trade["pnl"]    = total_pnl
                trade["result"] = result
                trade["label"]  = "Tight"
                daily_trades.append(trade)
                to_remove.append(oid)
                _update_loss_streak(total_pnl)
                journal_closed_trade(trade)
        except Exception as e:
            print(f"[TRACK ERROR] {e}")
    for oid in to_remove:
        tight_open_trades.pop(oid, None)


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


def h(c):  return float(c["high"])
def l(c):  return float(c["low"])
def cl(c): return float(c["close"])
def o(c):  return float(c["open"])
def v(c):  return float(c["volume"])


# ==================== TIGHT 2 ENTRY LOGIC ====================
def check_tight(symbol, confirmed, closes, opens, vols, highs, lows,
                ema50_vals, ema200_vals, vwap_vals, rsi_vals, adx_vals, atr_vals):
    global tight_cooldown_until

    if tight_cooldown_until and datetime.now(timezone.utc) < tight_cooldown_until:
        return None

    i   = len(confirmed) - 1
    sig = i - 2
    if sig < VOLUME_LOOKBACK + EMA200_LEN:
        return None

    entry      = closes[i]
    if entry < MIN_PRICE:
        return None

    vwap_now   = vwap_vals[i]
    ema50_now  = ema50_vals[i]
    ema200_now = ema200_vals[i]
    rsi_now    = rsi_vals[i]
    adx_now    = adx_vals[i] if i < len(adx_vals) else 0
    atr_now    = atr_vals[i] if i < len(atr_vals) else (highs[i] - lows[i])

    er_now = efficiency_ratio(closes[:i+1], ER_LOOKBACK)
    if er_now < ER_MIN:
        return None

    mode, atr_ratio = update_regime(symbol, highs[:i+1], lows[:i+1], closes[:i+1], atr_vals[:i+1])

    avg_vol = sum(vols[sig - VOLUME_LOOKBACK:sig]) / VOLUME_LOOKBACK
    ratio   = vols[sig] / avg_vol if avg_vol > 0 else 0
    vol_ok  = ratio >= VOLUME_MULTIPLIER

    trend_bull = entry > ema200_now
    trend_bear = entry < ema200_now

    vwap_cross_up = any(
        closes[j] > vwap_vals[j] and closes[j-1] <= vwap_vals[j-1]
        for j in range(sig-1, sig+2) if j > 0 and j < len(closes)
    )
    vwap_cross_down = any(
        closes[j] < vwap_vals[j] and closes[j-1] >= vwap_vals[j-1]
        for j in range(sig-1, sig+2) if j > 0 and j < len(closes)
    )

    conf1_bull = closes[i-1] > opens[i-1] and closes[i-1] > vwap_vals[i-1]
    conf2_bull = closes[i]   > opens[i]   and closes[i]   > vwap_now
    conf1_bear = closes[i-1] < opens[i-1] and closes[i-1] < vwap_vals[i-1]
    conf2_bear = closes[i]   < opens[i]   and closes[i]   < vwap_now

    bull_ok, bull_pt, bull_pool = sweep_recent(highs, lows, closes, i, bull=True)
    bear_ok, bear_pt, bear_pool = sweep_recent(highs, lows, closes, i, bull=False)

    range_high = max(highs[i - RANGE_BOUNDARY_LEN:i]) if i >= RANGE_BOUNDARY_LEN else max(highs[:i+1])
    range_low  = min(lows[i - RANGE_BOUNDARY_LEN:i])  if i >= RANGE_BOUNDARY_LEN else min(lows[:i+1])
    near_range_low  = lows[i]  <= range_low  * 1.002
    near_range_high = highs[i] >= range_high * 0.998

    long_signal  = False
    short_signal = False

    if mode == "TREND":
        long_signal  = (vwap_cross_up and entry > vwap_now and entry > ema50_now
                         and trend_bull and vol_ok and 40 <= rsi_now <= 65
                         and conf1_bull and conf2_bull and bull_ok
                         and adx_now >= ADX_MIN)
        short_signal = (vwap_cross_down and entry < vwap_now and entry < ema50_now
                         and trend_bear and vol_ok and 35 <= rsi_now <= 60
                         and conf1_bear and conf2_bear and bear_ok
                         and adx_now >= ADX_MIN)
        rsi_low, rsi_high = (40, 65) if long_signal else (35, 60)

        # --- Multi-timeframe confirmation (TREND mode only) ---
        if (long_signal or short_signal) and MTF_FILTER_ENABLED:
            mtf_trend = get_mtf_trend(symbol)
            if mtf_trend is not None:
                if long_signal and mtf_trend != "UP":
                    print(f"[MTF SKIP] {symbol} long blocked - {MTF_INTERVAL} trend is {mtf_trend}")
                    long_signal = False
                if short_signal and mtf_trend != "DOWN":
                    print(f"[MTF SKIP] {symbol} short blocked - {MTF_INTERVAL} trend is {mtf_trend}")
                    short_signal = False

        # --- Exposure cap (TREND mode only) ---
        if (long_signal or short_signal) and EXPOSURE_CAP_ENABLED:
            if long_signal and count_open_direction("BUY") >= MAX_SAME_DIRECTION_TRADES:
                print(f"[EXPOSURE SKIP] {symbol} long blocked - {MAX_SAME_DIRECTION_TRADES} same-direction cap reached")
                long_signal = False
            if short_signal and count_open_direction("SELL") >= MAX_SAME_DIRECTION_TRADES:
                print(f"[EXPOSURE SKIP] {symbol} short blocked - {MAX_SAME_DIRECTION_TRADES} same-direction cap reached")
                short_signal = False

        # --- Funding rate check (TREND mode only, only queried if a signal would otherwise fire) ---
        if (long_signal or short_signal) and FUNDING_FILTER_ENABLED:
            funding_val = get_funding_rate(symbol)
            if long_signal and funding_val > FUNDING_RATE_EXTREME_PCT:
                print(f"[FUNDING SKIP] {symbol} long blocked - funding {funding_val:.3f}% (crowded long)")
                long_signal = False
            if short_signal and funding_val < -FUNDING_RATE_EXTREME_PCT:
                print(f"[FUNDING SKIP] {symbol} short blocked - funding {funding_val:.3f}% (crowded short)")
                short_signal = False
    else:  # RANGE mode - fade off the sweep at the boundary (unchanged)
        long_signal  = bull_ok and near_range_low
        short_signal = bear_ok and near_range_high
        rsi_low, rsi_high = 30, 70

    # --- BTC-wide uncertainty check - downsizes (never blocks outright) TREND-mode alt
    # entries when BTC itself is in an elevated-volatility/uncertain regime. ---
    btc_ratio = get_btc_regime() if BTC_REGIME_FILTER_ENABLED else 1.0
    btc_uncertain = BTC_REGIME_FILTER_ENABLED and mode == "TREND" and btc_ratio >= BTC_UNCERTAIN_RATIO

    if long_signal:
        sig_id = (symbol, "BUY", int(confirmed[sig]["time"]))
        if sig_id not in tight_alerted:
            sl   = bull_pt - atr_now * SL_ATR_BUFFER_MULT
            risk = entry - sl
            risk_pct = (risk / entry * 100) if entry > 0 else 0
            if risk <= 0 or risk_pct < MIN_RISK_PCT or risk_pct > MAX_RISK_PCT:
                return ratio
            sweep_depth_pct = abs(bull_pool - bull_pt) / bull_pool * 100 if bull_pool else SWEEP_MIN_PCT
            score  = confidence_score(ratio, adx_now, er_now, rsi_now, rsi_low, rsi_high, sweep_depth_pct)
            tier_mult = tier_mult_for_score(score)
            if tier_mult is None:
                return ratio
            if btc_uncertain:
                tier_mult = round(tier_mult / 2, 2)

            if mode == "TREND":
                tp1 = round(entry + risk * RR_TP1, 6)
            else:
                tp1 = round(entry + risk * RANGE_RR, 6)
            sl = round(sl, 6)

            tight_alerted.add(sig_id)
            trade_status = ""
            if tight_auto_trade_enabled:
                oid = place_tight_order(symbol, "BUY", entry, sl, tp1, tier_mult, mode)
                trade_status = "\nOrder: " + str(oid) if oid and oid != "N/A" else "\nOrder failed"
            else:
                trade_status = "\nAuto-trade OFF"
            send_tg(
                "TIGHT SIGNAL - BUY - " + symbol + " [" + mode + "]\n------------------------------\n"
                "Entry: " + str(round(entry, 6)) + " | SL: " + str(sl) + " | TP1: " + str(tp1) + "\n"
                "RSI: " + str(round(rsi_now, 1)) + " | Vol: " + str(round(ratio, 1)) + "x | ADX: " + str(round(adx_now, 1)) +
                " | ER: " + str(round(er_now, 2)) + " | Score: " + str(score) + " | SizeMult: " + str(tier_mult) +
                " | BTCratio: " + str(round(btc_ratio, 2)) +
                trade_status + "\n------------------------------\nNiti Tight 2"
            )

    if short_signal:
        sig_id = (symbol, "SELL", int(confirmed[sig]["time"]))
        if sig_id not in tight_alerted:
            sl   = bear_pt + atr_now * SL_ATR_BUFFER_MULT
            risk = sl - entry
            risk_pct = (risk / entry * 100) if entry > 0 else 0
            if risk <= 0 or risk_pct < MIN_RISK_PCT or risk_pct > MAX_RISK_PCT:
                return ratio
            sweep_depth_pct = abs(bear_pt - bear_pool) / bear_pool * 100 if bear_pool else SWEEP_MIN_PCT
            score  = confidence_score(ratio, adx_now, er_now, rsi_now, rsi_low, rsi_high, sweep_depth_pct)
            tier_mult = tier_mult_for_score(score)
            if tier_mult is None:
                return ratio
            if btc_uncertain:
                tier_mult = round(tier_mult / 2, 2)

            if mode == "TREND":
                tp1 = round(entry - risk * RR_TP1, 6)
            else:
                tp1 = round(entry - risk * RANGE_RR, 6)
            sl = round(sl, 6)

            tight_alerted.add(sig_id)
            trade_status = ""
            if tight_auto_trade_enabled:
                oid = place_tight_order(symbol, "SELL", entry, sl, tp1, tier_mult, mode)
                trade_status = "\nOrder: " + str(oid) if oid and oid != "N/A" else "\nOrder failed"
            else:
                trade_status = "\nAuto-trade OFF"
            send_tg(
                "TIGHT SIGNAL - SELL - " + symbol + " [" + mode + "]\n------------------------------\n"
                "Entry: " + str(round(entry, 6)) + " | SL: " + str(sl) + " | TP1: " + str(tp1) + "\n"
                "RSI: " + str(round(rsi_now, 1)) + " | Vol: " + str(round(ratio, 1)) + "x | ADX: " + str(round(adx_now, 1)) +
                " | ER: " + str(round(er_now, 2)) + " | Score: " + str(score) + " | SizeMult: " + str(tier_mult) +
                " | BTCratio: " + str(round(btc_ratio, 2)) +
                trade_status + "\n------------------------------\nNiti Tight 2"
            )

    return ratio


def check_symbol_tight(symbol):
    try:
        candles = get_candles(symbol, limit=350, interval="15m")
        if len(candles) < 320:
            return None
        confirmed = candles[:-1]
        closes = [cl(c) for c in confirmed]
        opens  = [o(c)  for c in confirmed]
        vols   = [v(c)  for c in confirmed]
        highs  = [h(c)  for c in confirmed]
        lows   = [l(c)  for c in confirmed]
        ema50_vals  = ema_series(closes, EMA50_LEN)
        ema200_vals = ema_series(closes, EMA200_LEN)
        vwap_vals   = vwap_series(confirmed)
        rsi_vals    = rsi_series(closes, RSI_LEN)
        adx_vals    = adx_series(highs, lows, closes, ADX_LEN)
        atr_vals    = atr_series(highs, lows, closes, ATR_LEN)
        return check_tight(symbol, confirmed, closes, opens, vols, highs, lows,
                           ema50_vals, ema200_vals, vwap_vals, rsi_vals, adx_vals, atr_vals)
    except Exception as e:
        print(f"[{symbol}] error: {e}")
        return None


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

        # Box computed strictly from the window BEFORE the current candle - no
        # self-reference, and (per updated design) no retest wait needed either.
        box_high = max(highs[i - FAST_CONSOL_LOOKBACK:i])
        box_low  = min(lows[i - FAST_CONSOL_LOOKBACK:i])

        avg_vol = sum(vols[i - FAST_VOL_LB:i]) / FAST_VOL_LB
        ratio   = vols[i] / avg_vol if avg_vol > 0 else 0
        vol_ok  = ratio >= FAST_VOL_MULT

        candle_range = highs[i] - lows[i]
        bull_close_strength = (closes[i] - lows[i]) / candle_range if candle_range > 0 else 0
        bear_close_strength = (highs[i] - closes[i]) / candle_range if candle_range > 0 else 0

        # Same-candle close-position filter, replacing the old 1-candle retest-confirm
        # delay - no waiting required, just requires a decisive (not wicky) close.
        bull_breakout = (closes[i] > box_high + atr_now * FAST_BREAKOUT_ATR_MULT
                         and bull_close_strength >= FAST_CLOSE_POSITION_MIN)
        bear_breakout = (closes[i] < box_low  - atr_now * FAST_BREAKOUT_ATR_MULT
                         and bear_close_strength >= FAST_CLOSE_POSITION_MIN)

        long_signal  = bull_breakout and vol_ok
        short_signal = bear_breakout and vol_ok

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
                    send_tg("Tight 2 Auto-trade ON.")
                elif text == "/stop":
                    tight_auto_trade_enabled = False
                    send_tg("Tight 2 Auto-trade OFF.")
                elif text == "/status":
                    t = "ON" if tight_auto_trade_enabled else "OFF"
                    f = "ON" if fast_auto_trade_enabled  else "OFF"
                    cd = ""
                    if tight_cooldown_until and datetime.now(timezone.utc) < tight_cooldown_until:
                        mins_left = int((tight_cooldown_until - datetime.now(timezone.utc)).total_seconds() / 60)
                        cd = f"\nTight 2 cooldown: {mins_left} min left"
                    send_tg("Tight 2: " + t + "\nFast Signal: " + f + cd)
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
                update_tight_trailing()
        except Exception as e:
            print(f"[TRAIL LOOP ERROR] {e}")
        time.sleep(30)


def fast_scan_loop():
    print("Fast Signal loop started - 3m | small/mid-cap universe | consolidation breakout (retest removed, close-position filter added)")
    all_symbols = []
    while True:
        try:
            if not all_symbols:
                all_symbols = get_futures_symbols() or []
            liquid = get_liquid_symbols(
                all_symbols, min_quote_vol=FAST_MIN_QUOTE_VOL, max_n=None, exclude_top_n=FAST_EXCLUDE_TOP_N
            )
            print(f"[FAST SCAN] Scanning {len(liquid)} small/mid-cap liquid pairs for breakouts...")
            for sym in liquid:
                check_fast(sym)
                time.sleep(0.15)
            print("[FAST SCAN] Done. Sleeping 180s...")
        except Exception as e:
            print(f"[FAST LOOP ERROR] {e}")
        time.sleep(180)


def monitor_loop():
    global tight_cooldown_until
    print("Monitor started - Tight 2 (15m, sweep+regime+dynamic SL+trail, fixed-$ risk, market-regime filters) | Fast Signal (3m breakout)")
    while True:
        try:
            # Proactive volatility-aware cooldown - checked once per cycle, not per symbol.
            if BTC_REGIME_FILTER_ENABLED:
                btc_ratio_now = get_btc_regime()
                already_cooling = tight_cooldown_until and datetime.now(timezone.utc) < tight_cooldown_until
                if btc_ratio_now >= VOL_COOLDOWN_ATR_RATIO and not already_cooling:
                    tight_cooldown_until = datetime.now(timezone.utc) + timedelta(minutes=COOLDOWN_MINUTES)
                    send_journal(
                        f"Tight 2 volatility cooldown triggered (BTC ATR ratio {btc_ratio_now:.2f}) - "
                        f"pausing {COOLDOWN_MINUTES} min\nNiti Journal"
                    )

            all_symbols = get_futures_symbols()
            symbols = get_liquid_symbols(all_symbols, min_quote_vol=TIGHT_MIN_QUOTE_VOL, max_n=TIGHT_MAX_SYMBOLS)
            print(f"[TIGHT SCAN] Scanning {len(symbols)}/{len(all_symbols)} liquid pairs (min 24h vol ${TIGHT_MIN_QUOTE_VOL:,.0f}) | Tight={tight_auto_trade_enabled} | Fast={fast_auto_trade_enabled}")
            ratios = []
            for sym in symbols:
                r = check_symbol_tight(sym)
                if r is not None:
                    ratios.append(r)
                time.sleep(0.2)
            track_tight_trades()
            send_daily_summary()
            if ratios:
                top5 = sorted(ratios, reverse=True)[:5]
                print(f"[VOL DEBUG] Top 5: {[round(x,2) for x in top5]}")
            print("[TIGHT SCAN] Done. Sleeping 60s...")
        except Exception as e:
            print(f"[TIGHT LOOP ERROR] {e}")
        time.sleep(60)


@app.route("/")
def health():
    return "Niti Tight 2 (Sweep+Regime+Trail+FixedRisk+RegimeFilters) + Fast Signal (Breakout+ATRTrail+TimeExit)", 200


if __name__ == "__main__":
    Thread(target=monitor_loop,             daemon=True).start()
    Thread(target=fast_scan_loop,           daemon=True).start()
    Thread(target=trailing_loop,            daemon=True).start()
    Thread(target=handle_telegram_commands, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
