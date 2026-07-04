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
VOLUME_MULTIPLIER  = 2
EMA50_LEN          = 50
EMA200_LEN         = 200
ADX_LEN            = 14
ADX_MIN            = 20
SWING_LOOKBACK     = 10
MIN_PRICE          = 0.001
MIN_RISK_PCT       = 0.1

# Liquidity sweep
SWEEP_LOOKBACK     = 15
SWEEP_MIN_PCT      = 0.05
SWEEP_WINDOW       = 3          # bars a sweep stays "valid" for triggering entry

# Adaptive regime detection (ATR% ratio vs own baseline, with hysteresis)
ATR_LEN            = 14
ATR_BASE_LEN       = 50
RANGE_ENTER_RATIO  = 0.6        # atrRatio <= this  -> RANGE mode
TREND_ENTER_RATIO  = 1.0        # atrRatio >= this  -> TREND mode
RANGE_BOUNDARY_LEN = 20         # lookback used for range-mode fade boundaries

# Dynamic SL / TP
SL_ATR_BUFFER_MULT = 0.3
RR_TP1             = 2.0
RR_TP2             = 4.0
RANGE_RR           = 2.0

# Cooldown after consecutive losses
LOSS_STREAK_N      = 3
COOLDOWN_MINUTES   = 300        # ~20 x 15m candles

# Efficiency Ratio coin-quality pre-filter (skips choppy/grinding coins)
ER_LOOKBACK        = 50
ER_MIN             = 0.25

# Confidence-based position sizing
CONF_WEAK_MIN      = 40
CONF_GOOD_MIN      = 60
CONF_STRONG_MIN    = 80
AMOUNT_WEAK        = 15.0
AMOUNT_NORMAL      = 20.0
AMOUNT_STRONG      = 30.0

# ==================== FAST SIGNAL CONFIG ====================
FAST_TIMEFRAME          = "3m"
FAST_MIN_QUOTE_VOL      = float(os.environ.get("FAST_MIN_QUOTE_VOL", 2_000_000))  # 24h liquidity filter
FAST_MAX_SYMBOLS        = 150     # safety cap per scan cycle
FAST_CONSOL_LOOKBACK    = 20      # bars that define the pre-breakout box
FAST_BREAKOUT_ATR_MULT  = 0.3     # breakout must clear the box by this many ATR
FAST_VOL_MULT           = 3.0     # volume spike required on the breakout candle itself
FAST_VOL_LB             = 20
FAST_ATR_LEN            = 14
FAST_SL_PCT             = 1.5
FAST_TP1_RR             = 2.0
FAST_TRAIL_PCT          = float(os.environ.get("FAST_TRAIL_PCT", 3.0))   # widened from 1.5
FAST_TRAIL_ACTIVATE_RR  = 1.0     # trailing only starts after price moves 1R in favor
FAST_RETEST_CONFIRM     = True    # 1-candle hold confirmation ON — reduces fakeouts, adds ~1x3min delay

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

regime_state        = {}   # symbol -> "TREND" / "RANGE", persisted for hysteresis
tight_loss_streak   = 0
tight_cooldown_until = None   # datetime or None


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


def get_liquid_symbols(symbols, min_quote_vol=FAST_MIN_QUOTE_VOL, max_n=FAST_MAX_SYMBOLS):
    """Replaces the old 'top 50 movers by 24h %change' selection for Fast Signal.
    Ranking by % change picks coins that already pumped; instead we just filter
    for adequate liquidity and let the breakout scan (check_fast) find the
    early-stage moves across the whole liquid universe."""
    try:
        url = BASE_URL + "/openApi/swap/v2/quote/ticker"
        r = requests.get(url, timeout=10).json()
        tickers = r.get("data", [])
        if not isinstance(tickers, list):
            return symbols[:max_n]
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
        return [s for s, _ in liquid[:max_n]]
    except Exception as e:
        print(f"[LIQUID SYMBOLS ERROR] {e}")
        return symbols[:max_n]


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
    result = []
    for c in candles:
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
    """1.0 = price moved in a straight line (clean trend). ~0 = pure chop."""
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
    """Looks back `window` bars from idx for a sweep event (not just same-bar)."""
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
def update_regime(symbol, highs, lows, closes, atr_vals):
    atr_pct_series = []
    for i in range(len(closes)):
        atr_pct_series.append((atr_vals[i] / closes[i] * 100) if closes[i] > 0 else 0.0)
    base_len = min(ATR_BASE_LEN, len(atr_pct_series))
    baseline = sum(atr_pct_series[-base_len:]) / base_len if base_len > 0 else 1.0
    atr_pct_now = atr_pct_series[-1]
    ratio = atr_pct_now / baseline if baseline > 0 else 1.0

    prev = regime_state.get(symbol, "TREND")
    if ratio <= RANGE_ENTER_RATIO:
        regime_state[symbol] = "RANGE"
    elif ratio >= TREND_ENTER_RATIO:
        regime_state[symbol] = "TREND"
    else:
        regime_state[symbol] = prev
    return regime_state[symbol], ratio


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


def amount_for_score(score):
    if score < CONF_WEAK_MIN:
        return None
    if score < CONF_GOOD_MIN:
        return AMOUNT_WEAK
    if score < CONF_STRONG_MIN:
        return AMOUNT_NORMAL
    return AMOUNT_STRONG


# ==================== TELEGRAM ====================
def send_tg(msg, chat_id=None):
    cid = chat_id or TG_CHAT_ID
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": cid, "text": msg, "parse_mode": "HTML"}, timeout=10)


def send_journal(msg):
    if TG_JOURNAL_ID:
        send_tg(msg, chat_id=TG_JOURNAL_ID)


# ==================== LEVERAGE ====================
def get_fast_leverage(symbol):
    max_lev = symbol_max_lev.get(symbol, 20)
    calc = int(max_lev * 0.20)
    lev = max(calc, 10)          # minimum 10x
    return min(lev, max_lev)     # never exceed the symbol's own exchange max leverage


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
    url = BASE_URL + "/openApi/swap/v2/trade/order"
    params = build_signed_params({
        "symbol": symbol, "side": close_side, "positionSide": pos_side,
        "type": "STOP_MARKET", "stopPrice": round(sl_price, 6), "quantity": qty,
    })
    r = requests.post(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
    print(f"[SL] {symbol}: {r}")
    return r.get("data", {}).get("order", {}).get("orderId", "N/A")


def place_tp_order(symbol, close_side, pos_side, tp_price, qty):
    url = BASE_URL + "/openApi/swap/v2/trade/order"
    params = build_signed_params({
        "symbol": symbol, "side": close_side, "positionSide": pos_side,
        "type": "TAKE_PROFIT_MARKET", "stopPrice": round(tp_price, 6), "quantity": qty,
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
        current = get_current_price(symbol)
        lev = trade.get("lev", 3)
        if trade["side"] == "BUY":
            pnl = round((current - trade["entry"]) / trade["entry"] * FAST_TRADE_AMOUNT * lev, 2)
        else:
            pnl = round((trade["entry"] - current) / trade["entry"] * FAST_TRADE_AMOUNT * lev, 2)
        sign = "+" if pnl > 0 else ""
        send_journal(
            "Trade Closed [Fast " + reason + "] - " + symbol + "\n"
            "------------------------------\n"
            "Side  : " + trade["side"] + "\nEntry : " + str(trade["entry"]) + "\n"
            "Exit  : " + str(round(current, 6)) + "\n"
            "PnL   : " + sign + str(pnl) + " USDT\n"
            "------------------------------\nNiti Journal"
        )
        del fast_open_trades[symbol]
        print(f"[FAST CLOSE] {symbol} {reason}")
    except Exception as e:
        print(f"[FAST CLOSE ERROR] {symbol}: {e}")


def place_tight_order(symbol, side, entry, sl, tp1, tp2, trade_amount, mode):
    try:
        set_leverage_api(symbol, LEVERAGE)
        precision = symbol_precision.get(symbol, 4)
        total_qty = round(trade_amount * LEVERAGE / entry, precision)
        if total_qty <= 0:
            return None
        pos_side   = "LONG"  if side == "BUY" else "SHORT"
        close_side = "SELL"  if side == "BUY" else "BUY"
        order_id = place_market_order(symbol, side, total_qty, pos_side)
        print(f"[TIGHT ORDER] {symbol} {side}: {order_id}")
        if order_id != "N/A":
            time.sleep(0.5)
            sl_id = place_sl_order(symbol, close_side, pos_side, sl, total_qty)
            if mode == "TREND" and tp2 is not None:
                half_qty = round(total_qty / 2, precision)
                tp1_id = place_tp_order(symbol, close_side, pos_side, tp1, half_qty)
                tp2_id = place_tp_order(symbol, close_side, pos_side, tp2, half_qty)
                tight_open_trades[str(order_id)] = {
                    "symbol": symbol, "side": side, "entry": entry, "mode": mode,
                    "sl": sl, "tp1": tp1, "tp2": tp2, "qty": total_qty, "half_qty": half_qty,
                    "sl_id": sl_id, "tp1_id": tp1_id, "tp2_id": tp2_id,
                    "close_side": close_side, "pos_side": pos_side,
                    "breakeven_done": False,
                    "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
                }
            else:
                tp1_id = place_tp_order(symbol, close_side, pos_side, tp1, total_qty)
                tight_open_trades[str(order_id)] = {
                    "symbol": symbol, "side": side, "entry": entry, "mode": mode,
                    "sl": sl, "tp1": tp1, "tp2": None, "qty": total_qty, "half_qty": total_qty,
                    "sl_id": sl_id, "tp1_id": tp1_id, "tp2_id": None,
                    "close_side": close_side, "pos_side": pos_side,
                    "breakeven_done": True,   # single-target range trade, no breakeven step needed
                    "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
                }
        return order_id
    except Exception as e:
        print(f"[TIGHT ORDER ERROR] {symbol}: {e}")
        return None


def place_fast_order(symbol, side, entry):
    try:
        lev       = get_fast_leverage(symbol)
        set_leverage_api(symbol, lev)
        precision = symbol_precision.get(symbol, 4)
        total_qty = round(FAST_TRADE_AMOUNT * lev / entry, precision)
        half_qty  = round(total_qty / 2, precision)
        if total_qty <= 0 or half_qty <= 0:
            return None
        pos_side   = "LONG"  if side == "BUY" else "SHORT"
        close_side = "SELL"  if side == "BUY" else "BUY"
        if side == "BUY":
            sl_price  = round(entry * (1 - FAST_SL_PCT / 100), 6)
            tp1_price = round(entry * (1 + FAST_SL_PCT * FAST_TP1_RR / 100), 6)
        else:
            sl_price  = round(entry * (1 + FAST_SL_PCT / 100), 6)
            tp1_price = round(entry * (1 - FAST_SL_PCT * FAST_TP1_RR / 100), 6)
        order_id = place_market_order(symbol, side, total_qty, pos_side)
        print(f"[FAST ORDER] {symbol} {side} lev={lev}x: {order_id}")
        if order_id != "N/A":
            time.sleep(0.5)
            place_sl_order(symbol, close_side, pos_side, sl_price, total_qty)
            place_tp_order(symbol, close_side, pos_side, tp1_price, half_qty)
            fast_open_trades[symbol] = {
                "symbol": symbol, "side": side, "entry": entry,
                "sl": sl_price, "tp1": tp1_price, "lev": lev,
                "total_qty": total_qty, "remaining_qty": half_qty,
                "trail_price": entry, "activated": False, "order_id": order_id,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
        return order_id
    except Exception as e:
        print(f"[FAST ORDER ERROR] {symbol}: {e}")
        return None


def update_fast_trailing():
    for symbol in list(fast_open_trades.keys()):
        try:
            trade   = fast_open_trades[symbol]
            current = get_current_price(symbol)
            if current <= 0:
                continue
            side = trade["side"]
            activate_pct = (FAST_TRAIL_ACTIVATE_RR * FAST_SL_PCT) / 100
            trail_pct    = FAST_TRAIL_PCT / 100

            if not trade.get("activated"):
                if side == "BUY" and current >= trade["entry"] * (1 + activate_pct):
                    fast_open_trades[symbol]["activated"]   = True
                    fast_open_trades[symbol]["trail_price"] = current
                elif side == "SELL" and current <= trade["entry"] * (1 - activate_pct):
                    fast_open_trades[symbol]["activated"]   = True
                    fast_open_trades[symbol]["trail_price"] = current
                continue   # not activated yet - exchange-side SL/TP1 still protect the trade

            if side == "BUY":
                if current > trade["trail_price"]:
                    fast_open_trades[symbol]["trail_price"] = current
                trail_sl = trade["trail_price"] * (1 - trail_pct)
                if current <= trail_sl:
                    close_fast_position(symbol, "Trail")
            else:
                if current < trade["trail_price"]:
                    fast_open_trades[symbol]["trail_price"] = current
                trail_sl = trade["trail_price"] * (1 + trail_pct)
                if current >= trail_sl:
                    close_fast_position(symbol, "Trail")
        except Exception as e:
            print(f"[TRAIL ERROR] {symbol}: {e}")


def check_tight_order_status(order_id, symbol):
    try:
        params = build_signed_params({"symbol": symbol, "orderId": order_id})
        url = BASE_URL + "/openApi/swap/v2/trade/order"
        r = requests.get(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        return r.get("data", {}).get("order", {}).get("status", "")
    except Exception:
        return ""


def track_tight_trades():
    global daily_trades, tight_loss_streak, tight_cooldown_until
    to_remove = []
    for oid, trade in list(tight_open_trades.items()):
        try:
            # --- move remaining runner to breakeven once TP1 has filled (trend mode only) ---
            if not trade.get("breakeven_done") and trade.get("tp1_id"):
                tp1_status = check_tight_order_status(trade["tp1_id"], trade["symbol"])
                if tp1_status == "FILLED":
                    cancel_order(trade["symbol"], trade["sl_id"])
                    new_sl_id = place_sl_order(
                        trade["symbol"], trade["close_side"], trade["pos_side"],
                        trade["entry"], trade["half_qty"]
                    )
                    trade["sl_id"] = new_sl_id
                    trade["breakeven_done"] = True
                    send_journal(
                        "TP1 hit [Tight] - " + trade["symbol"] +
                        "\nRemaining position SL moved to breakeven (" + str(trade["entry"]) + ")\nNiti Journal"
                    )

            status = check_tight_order_status(oid, trade["symbol"])
            if status in ("FILLED", "CANCELLED", "EXPIRED"):
                current = get_current_price(trade["symbol"])
                if trade["side"] == "BUY":
                    pnl    = round((current - trade["entry"]) / trade["entry"] * (trade["qty"] * trade["entry"] / LEVERAGE) * LEVERAGE, 2)
                    result = "TP" if current >= trade["tp1"] else ("SL" if current <= trade["sl"] else "Open")
                else:
                    pnl    = round((trade["entry"] - current) / trade["entry"] * (trade["qty"] * trade["entry"] / LEVERAGE) * LEVERAGE, 2)
                    result = "TP" if current <= trade["tp1"] else ("SL" if current >= trade["sl"] else "Open")
                trade["pnl"]    = pnl
                trade["result"] = result
                trade["label"]  = "Tight"
                daily_trades.append(trade)
                to_remove.append(oid)

                if pnl < 0:
                    tight_loss_streak += 1
                else:
                    tight_loss_streak = 0
                if tight_loss_streak >= LOSS_STREAK_N:
                    tight_cooldown_until = datetime.now(timezone.utc) + timedelta(minutes=COOLDOWN_MINUTES)
                    tight_loss_streak = 0
                    send_journal(f"Tight 2 cooldown triggered - pausing new entries for {COOLDOWN_MINUTES} minutes\nNiti Journal")

                sign = "+" if pnl > 0 else ""
                send_journal(
                    "Trade Closed [Tight] - " + trade["symbol"] + "\n------------------------------\n"
                    "Side: " + trade["side"] + " | Result: " + result + "\n"
                    "PnL: " + sign + str(pnl) + " USDT\n------------------------------\nNiti Journal"
                )
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
        return None   # coin-quality filter: too choppy/grinding, skip entirely

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
                         and conf1_bull and conf2_bull and bull_ok)
        short_signal = (vwap_cross_down and entry < vwap_now and entry < ema50_now
                         and trend_bear and vol_ok and 35 <= rsi_now <= 60
                         and conf1_bear and conf2_bear and bear_ok)
        rsi_low, rsi_high = (40, 65) if long_signal else (35, 60)
    else:  # RANGE mode - fade off the sweep at the boundary
        long_signal  = bull_ok and near_range_low
        short_signal = bear_ok and near_range_high
        rsi_low, rsi_high = 30, 70

    if long_signal:
        sig_id = (symbol, "BUY", int(confirmed[sig]["time"]))
        if sig_id not in tight_alerted:
            sl   = bull_pt - atr_now * SL_ATR_BUFFER_MULT
            risk = entry - sl
            if risk <= 0 or (risk / entry * 100) < MIN_RISK_PCT:
                return ratio
            sweep_depth_pct = abs(bull_pool - bull_pt) / bull_pool * 100 if bull_pool else SWEEP_MIN_PCT
            score  = confidence_score(ratio, adx_now, er_now, rsi_now, rsi_low, rsi_high, sweep_depth_pct)
            amount = amount_for_score(score)
            if amount is None:
                return ratio   # confidence too low, skip

            if mode == "TREND":
                tp1 = round(entry + risk * RR_TP1, 6)
                tp2 = round(entry + risk * RR_TP2, 6)
            else:
                tp1 = round(entry + risk * RANGE_RR, 6)
                tp2 = None
            sl = round(sl, 6)

            tight_alerted.add(sig_id)
            trade_status = ""
            if tight_auto_trade_enabled:
                oid = place_tight_order(symbol, "BUY", entry, sl, tp1, tp2, amount, mode)
                trade_status = "\nOrder: " + str(oid) if oid and oid != "N/A" else "\nOrder failed"
            else:
                trade_status = "\nAuto-trade OFF"
            tp_line = "TP1: " + str(tp1) + (" | TP2: " + str(tp2) if tp2 else "")
            send_tg(
                "TIGHT SIGNAL - BUY - " + symbol + " [" + mode + "]\n------------------------------\n"
                "Entry: " + str(round(entry, 6)) + " | SL: " + str(sl) + "\n"
                + tp_line + "\n"
                "RSI: " + str(round(rsi_now, 1)) + " | Vol: " + str(round(ratio, 1)) + "x | ADX: " + str(round(adx_now, 1)) +
                " | ER: " + str(round(er_now, 2)) + " | Score: " + str(score) + " | Amount: $" + str(amount) +
                trade_status + "\n------------------------------\nNiti Tight 2"
            )
            send_journal("New Trade [Tight-" + mode + "] - " + symbol + "\nSide: BUY | Entry: " + str(round(entry, 6)) +
                " | SL: " + str(sl) + "\n" + tp_line + "\nScore: " + str(score) + " | Amount: $" + str(amount) + "\nNiti Journal")

    if short_signal:
        sig_id = (symbol, "SELL", int(confirmed[sig]["time"]))
        if sig_id not in tight_alerted:
            sl   = bear_pt + atr_now * SL_ATR_BUFFER_MULT
            risk = sl - entry
            if risk <= 0 or (risk / entry * 100) < MIN_RISK_PCT:
                return ratio
            sweep_depth_pct = abs(bear_pt - bear_pool) / bear_pool * 100 if bear_pool else SWEEP_MIN_PCT
            score  = confidence_score(ratio, adx_now, er_now, rsi_now, rsi_low, rsi_high, sweep_depth_pct)
            amount = amount_for_score(score)
            if amount is None:
                return ratio

            if mode == "TREND":
                tp1 = round(entry - risk * RR_TP1, 6)
                tp2 = round(entry - risk * RR_TP2, 6)
            else:
                tp1 = round(entry - risk * RANGE_RR, 6)
                tp2 = None
            sl = round(sl, 6)

            tight_alerted.add(sig_id)
            trade_status = ""
            if tight_auto_trade_enabled:
                oid = place_tight_order(symbol, "SELL", entry, sl, tp1, tp2, amount, mode)
                trade_status = "\nOrder: " + str(oid) if oid and oid != "N/A" else "\nOrder failed"
            else:
                trade_status = "\nAuto-trade OFF"
            tp_line = "TP1: " + str(tp1) + (" | TP2: " + str(tp2) if tp2 else "")
            send_tg(
                "TIGHT SIGNAL - SELL - " + symbol + " [" + mode + "]\n------------------------------\n"
                "Entry: " + str(round(entry, 6)) + " | SL: " + str(sl) + "\n"
                + tp_line + "\n"
                "RSI: " + str(round(rsi_now, 1)) + " | Vol: " + str(round(ratio, 1)) + "x | ADX: " + str(round(adx_now, 1)) +
                " | ER: " + str(round(er_now, 2)) + " | Score: " + str(score) + " | Amount: $" + str(amount) +
                trade_status + "\n------------------------------\nNiti Tight 2"
            )
            send_journal("New Trade [Tight-" + mode + "] - " + symbol + "\nSide: SELL | Entry: " + str(round(entry, 6)) +
                " | SL: " + str(sl) + "\n" + tp_line + "\nScore: " + str(score) + " | Amount: $" + str(amount) + "\nNiti Journal")

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
        min_needed = FAST_CONSOL_LOOKBACK + FAST_VOL_LB + FAST_ATR_LEN + 5
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

        box_high = max(highs[i - FAST_CONSOL_LOOKBACK:i])
        box_low  = min(lows[i - FAST_CONSOL_LOOKBACK:i])

        avg_vol = sum(vols[i - FAST_VOL_LB:i]) / FAST_VOL_LB
        ratio   = vols[i] / avg_vol if avg_vol > 0 else 0
        vol_ok  = ratio >= FAST_VOL_MULT

        bull_breakout = closes[i] > box_high + atr_now * FAST_BREAKOUT_ATR_MULT
        bear_breakout = closes[i] < box_low  - atr_now * FAST_BREAKOUT_ATR_MULT

        if FAST_RETEST_CONFIRM:
            # require the breakout level to have already been broken one bar earlier
            # and still holding now (adds ~1 candle of delay, cuts more false breakouts)
            bull_breakout = bull_breakout and closes[i-1] > box_high
            bear_breakout = bear_breakout and closes[i-1] < box_low

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
            lev       = get_fast_leverage(symbol)
            sl_price  = round(entry * (1 - FAST_SL_PCT / 100), 6)
            tp1_price = round(entry * (1 + FAST_SL_PCT * FAST_TP1_RR / 100), 6)
            print(f"[FAST] {symbol} BUY | vol={ratio:.1f}x | lev={lev}x")
            trade_status = ""
            if fast_auto_trade_enabled:
                oid = place_fast_order(symbol, "BUY", entry)
                trade_status = "\nOrder: " + str(oid) if oid and oid != "N/A" else "\nOrder failed"
            else:
                trade_status = "\nAuto-trade OFF"
            send_tg(
                "FAST SIGNAL - BUY - " + symbol + "\n------------------------------\n"
                "Entry: " + str(round(entry, 6)) + " | SL: " + str(sl_price) + "\n"
                "TP1 (1:2): " + str(tp1_price) + " | Trail: " + str(FAST_TRAIL_PCT) + "% (after " + str(FAST_TRAIL_ACTIVATE_RR) + "R)\n"
                "Breakout vol: " + str(round(ratio, 1)) + "x | Lev: " + str(lev) + "x" +
                trade_status + "\n------------------------------\nNiti Fast Signal"
            )
            send_journal("New Trade [Fast] - " + symbol + "\nSide: BUY | Entry: " + str(round(entry, 6)) +
                " | SL: " + str(sl_price) + " | TP1: " + str(tp1_price) + " | Lev: " + str(lev) + "x\nNiti Journal")

        elif short_signal and sig_id_short not in fast_alerted:
            fast_alerted.add(sig_id_short)
            lev       = get_fast_leverage(symbol)
            sl_price  = round(entry * (1 + FAST_SL_PCT / 100), 6)
            tp1_price = round(entry * (1 - FAST_SL_PCT * FAST_TP1_RR / 100), 6)
            print(f"[FAST] {symbol} SELL | vol={ratio:.1f}x | lev={lev}x")
            trade_status = ""
            if fast_auto_trade_enabled:
                oid = place_fast_order(symbol, "SELL", entry)
                trade_status = "\nOrder: " + str(oid) if oid and oid != "N/A" else "\nOrder failed"
            else:
                trade_status = "\nAuto-trade OFF"
            send_tg(
                "FAST SIGNAL - SELL - " + symbol + "\n------------------------------\n"
                "Entry: " + str(round(entry, 6)) + " | SL: " + str(sl_price) + "\n"
                "TP1 (1:2): " + str(tp1_price) + " | Trail: " + str(FAST_TRAIL_PCT) + "% (after " + str(FAST_TRAIL_ACTIVATE_RR) + "R)\n"
                "Breakout vol: " + str(round(ratio, 1)) + "x | Lev: " + str(lev) + "x" +
                trade_status + "\n------------------------------\nNiti Fast Signal"
            )
            send_journal("New Trade [Fast] - " + symbol + "\nSide: SELL | Entry: " + str(round(entry, 6)) +
                " | SL: " + str(sl_price) + " | TP1: " + str(tp1_price) + " | Lev: " + str(lev) + "x\nNiti Journal")

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
                update_fast_trailing()
        except Exception as e:
            print(f"[TRAIL LOOP ERROR] {e}")
        time.sleep(30)


def fast_scan_loop():
    print("Fast Signal loop started - 3m | liquidity-filtered universe | consolidation breakout")
    all_symbols = []
    while True:
        try:
            if not all_symbols:
                all_symbols = get_futures_symbols() or []
            liquid = get_liquid_symbols(all_symbols)
            print(f"[FAST SCAN] Scanning {len(liquid)} liquid pairs for breakouts...")
            for sym in liquid:
                check_fast(sym)
                time.sleep(0.15)
            print("[FAST SCAN] Done. Sleeping 60s...")
        except Exception as e:
            print(f"[FAST LOOP ERROR] {e}")
        time.sleep(60)


def monitor_loop():
    print("Monitor started - Tight 2 (15m, sweep+regime+dynamic SL) | Fast Signal (3m breakout)")
    while True:
        try:
            symbols = get_futures_symbols()
            print(f"[TIGHT SCAN] Scanning {len(symbols)} pairs... | Tight={tight_auto_trade_enabled} | Fast={fast_auto_trade_enabled}")
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
    return "Niti Tight 2 (Sweep+Regime+DynamicSL) + Fast Signal (Breakout+AdaptiveTrail)", 200


if __name__ == "__main__":
    Thread(target=monitor_loop,             daemon=True).start()
    Thread(target=fast_scan_loop,           daemon=True).start()
    Thread(target=trailing_loop,            daemon=True).start()
    Thread(target=handle_telegram_commands, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
