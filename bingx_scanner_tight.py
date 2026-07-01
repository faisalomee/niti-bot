import os, time, hmac, hashlib, requests
from flask import Flask
from threading import Thread
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

API_KEY           = os.environ.get("BINGX_API_KEY")
SECRET_KEY        = os.environ.get("BINGX_SECRET_KEY")
TG_TOKEN          = os.environ.get("TG_BOT_TOKEN_TIGHT")
TG_CHAT_ID        = os.environ.get("TG_CHAT_ID_TIGHT")
TG_JOURNAL_ID     = os.environ.get("TG_JOURNAL_CHAT_ID")
TRADE_AMOUNT      = float(os.environ.get("TRADE_AMOUNT", 20))
LEVERAGE          = int(os.environ.get("LEVERAGE", 10))
FAST_TRADE_AMOUNT = float(os.environ.get("FAST_TRADE_AMOUNT", 20))

BASE_URL = "https://open-api.bingx.com"

# Tight 2 params
TIMEFRAME         = "15m"
RSI_LEN           = 14
VOLUME_LOOKBACK   = 100
VOLUME_MULTIPLIER = 2
EMA50_LEN         = 50
EMA200_LEN        = 200
SWING_LOOKBACK    = 10
SL_BUFFER_PCT     = 0.15
RR_TP1            = 2.0
RR_TP2            = 4.0
MIN_PRICE         = 0.001
MIN_RISK_PCT      = 0.1

# Fast Signal params
FAST_VOL_MULT     = 5.0
FAST_SL_PCT       = 1.5    # 1.5% tight SL
FAST_TRAIL_PCT    = 1.5    # 1.5% trailing stop
FAST_TP1_RR       = 2.0    # 1:2

tight_auto_trade_enabled = False
fast_auto_trade_enabled  = False
symbol_precision  = {}
symbol_max_lev    = {}

# Tight 2 tracking
tight_open_trades = {}
tight_alerted     = set()

# Fast Signal tracking
fast_open_trades  = {}  # symbol -> {side, entry, sl, trail_price, qty, order_id, lev}
fast_alerted      = set()

daily_trades      = []
last_summary_date = None


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
            except:
                symbol_max_lev[sym] = 20
    return symbols


def get_candles(symbol, limit=350):
    params = build_signed_params({"symbol": symbol, "interval": TIMEFRAME, "limit": limit})
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
    except:
        return 0


def ema_series(closes, period):
    k = 2 / (period + 1)
    ema = closes[0]
    result = []
    for c in closes:
        ema = c * k + ema * (1 - k)
        result.append(ema)
    return result


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


def send_tg(msg, chat_id=None):
    cid = chat_id or TG_CHAT_ID
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": cid, "text": msg, "parse_mode": "HTML"}, timeout=10)


def send_journal(msg):
    if TG_JOURNAL_ID:
        send_tg(msg, chat_id=TG_JOURNAL_ID)


def get_fast_leverage(symbol):
    max_lev = symbol_max_lev.get(symbol, 20)
    calc = int(max_lev * 0.20)
    return max(calc, 3)


def set_leverage_api(symbol, lev):
    try:
        url = BASE_URL + "/openApi/swap/v2/trade/leverage"
        for side in ["LONG", "SHORT"]:
            params = build_signed_params({"symbol": symbol, "side": side, "leverage": lev})
            requests.post(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10)
    except Exception as e:
        print(f"[LEVERAGE ERROR] {symbol}: {e}")


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
    requests.post(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10)


def close_fast_position(symbol):
    if symbol not in fast_open_trades:
        return
    trade = fast_open_trades[symbol]
    try:
        pos_side   = "LONG"  if trade["side"] == "BUY" else "SHORT"
        close_side = "SELL"  if trade["side"] == "BUY" else "BUY"
        remaining_qty = trade.get("remaining_qty", 0)
        if remaining_qty > 0:
            place_market_order(symbol, close_side, remaining_qty, pos_side)
            print(f"[FAST CLOSE] {symbol} {trade['side']} closed")
        del fast_open_trades[symbol]
    except Exception as e:
        print(f"[FAST CLOSE ERROR] {symbol}: {e}")


def place_tight_order(symbol, side, entry, sl, tp1, tp2):
    try:
        set_leverage_api(symbol, LEVERAGE)
        precision = symbol_precision.get(symbol, 4)
        total_qty = round(TRADE_AMOUNT * LEVERAGE / entry, precision)
        half_qty  = round(total_qty / 2, precision)
        if total_qty <= 0 or half_qty <= 0:
            return None

        pos_side   = "LONG"  if side == "BUY" else "SHORT"
        close_side = "SELL"  if side == "BUY" else "BUY"

        order_id = place_market_order(symbol, side, total_qty, pos_side)
        print(f"[TIGHT ORDER] {symbol} {side}: {order_id}")

        if order_id != "N/A":
            time.sleep(0.5)
            place_sl_order(symbol, close_side, pos_side, sl, total_qty)
            place_tp_order(symbol, close_side, pos_side, tp1, half_qty)
            place_tp_order(symbol, close_side, pos_side, tp2, half_qty)

            tight_open_trades[str(order_id)] = {
                "symbol": symbol, "side": side, "entry": entry,
                "sl": sl, "tp1": tp1, "tp2": tp2, "qty": total_qty,
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

        # SL price — 1.5% from entry
        if side == "BUY":
            sl_price = round(entry * (1 - FAST_SL_PCT / 100), 6)
            tp1_price = round(entry * (1 + FAST_SL_PCT * FAST_TP1_RR / 100), 6)
        else:
            sl_price = round(entry * (1 + FAST_SL_PCT / 100), 6)
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
                "trail_price": entry,
                "order_id": order_id,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }

        return order_id
    except Exception as e:
        print(f"[FAST ORDER ERROR] {symbol}: {e}")
        return None


def update_fast_trailing():
    for symbol, trade in list(fast_open_trades.items()):
        try:
            current = get_current_price(symbol)
            if current <= 0:
                continue

            side = trade["side"]
            trail_pct = FAST_TRAIL_PCT / 100

            if side == "BUY":
                # Update trail high
                if current > trade["trail_price"]:
                    fast_open_trades[symbol]["trail_price"] = current
                new_sl = trade["trail_price"] * (1 - trail_pct)
                # Check if trailing SL hit
                if current <= new_sl:
                    print(f"[FAST TRAIL HIT] {symbol} BUY | price={current} trail_sl={new_sl:.6f}")
                    close_fast_position(symbol)
                    pnl = round((current - trade["entry"]) / trade["entry"] * FAST_TRADE_AMOUNT * trade["lev"], 2)
                    sign = "+" if pnl > 0 else ""
                    send_journal(
                        "Trade Closed [Fast Trail] - " + symbol + "\n"
                        "------------------------------\n"
                        "Side   : BUY\nEntry  : " + str(trade["entry"]) + "\n"
                        "Exit   : " + str(round(current, 6)) + "\n"
                        "PnL    : " + sign + str(pnl) + " USDT\n"
                        "------------------------------\nNiti Journal"
                    )
            else:
                if current < trade["trail_price"]:
                    fast_open_trades[symbol]["trail_price"] = current
                new_sl = trade["trail_price"] * (1 + trail_pct)
                if current >= new_sl:
                    print(f"[FAST TRAIL HIT] {symbol} SELL | price={current} trail_sl={new_sl:.6f}")
                    close_fast_position(symbol)
                    pnl = round((trade["entry"] - current) / trade["entry"] * FAST_TRADE_AMOUNT * trade["lev"], 2)
                    sign = "+" if pnl > 0 else ""
                    send_journal(
                        "Trade Closed [Fast Trail] - " + symbol + "\n"
                        "------------------------------\n"
                        "Side   : SELL\nEntry  : " + str(trade["entry"]) + "\n"
                        "Exit   : " + str(round(current, 6)) + "\n"
                        "PnL    : " + sign + str(pnl) + " USDT\n"
                        "------------------------------\nNiti Journal"
                    )
        except Exception as e:
            print(f"[TRAIL ERROR] {symbol}: {e}")


def check_tight_order_status(order_id, symbol):
    try:
        params = build_signed_params({"symbol": symbol, "orderId": order_id})
        url = BASE_URL + "/openApi/swap/v2/trade/order"
        r = requests.get(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        return r.get("data", {}).get("order", {}).get("status", "")
    except:
        return ""


def track_tight_trades():
    global daily_trades
    to_remove = []
    for oid, trade in list(tight_open_trades.items()):
        status = check_tight_order_status(oid, trade["symbol"])
        if status in ("FILLED", "CANCELLED", "EXPIRED"):
            try:
                candles = get_candles(trade["symbol"], limit=2)
                current = float(candles[-1]["close"]) if candles else trade["entry"]
                if trade["side"] == "BUY":
                    pnl    = round((current - trade["entry"]) / trade["entry"] * TRADE_AMOUNT * LEVERAGE, 2)
                    result = "TP" if current >= trade["tp1"] else ("SL" if current <= trade["sl"] else "Open")
                else:
                    pnl    = round((trade["entry"] - current) / trade["entry"] * TRADE_AMOUNT * LEVERAGE, 2)
                    result = "TP" if current <= trade["tp1"] else ("SL" if current >= trade["sl"] else "Open")

                trade["pnl"]    = pnl
                trade["result"] = result
                trade["label"]  = "Tight"
                daily_trades.append(trade)
                to_remove.append(oid)

                sign = "+" if pnl > 0 else ""
                send_journal(
                    "Trade Closed [Tight] - " + trade["symbol"] + "\n"
                    "------------------------------\n"
                    "Side   : " + trade["side"] + "\nEntry  : " + str(trade["entry"]) + "\n"
                    "Result : " + result + "\nPnL    : " + sign + str(pnl) + " USDT\n"
                    "------------------------------\nNiti Journal"
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
    date_str  = today.strftime("%b %d, %Y")
    lines = ["Daily Summary - " + date_str, "------------------------------"]
    for idx, t in enumerate(daily_trades, 1):
        p  = t.get("pnl", 0)
        ps = "+" if p > 0 else ""
        lbl = t.get("label", "?")
        lines.append(str(idx) + ". [" + lbl + "] " + t["symbol"] + " " + t["side"] + " | " + t.get("result","?") + " | " + ps + str(p) + " USDT")
    lines.append("------------------------------")
    lines.append("Trades   : " + str(total) + " (" + str(wins) + "W / " + str(losses) + "L)")
    lines.append("Win Rate : " + str(win_rate) + "%")
    lines.append("Total PnL: " + sign + str(total_pnl) + " USDT")
    lines.append("------------------------------\nNiti Journal")
    send_journal("\n".join(lines))
    daily_trades = []


def h(c):  return float(c["high"])
def l(c):  return float(c["low"])
def cl(c): return float(c["close"])
def o(c):  return float(c["open"])
def v(c):  return float(c["volume"])


def check_tight(symbol, confirmed, closes, opens, vols, highs, lows,
                ema50_vals, ema200_vals, vwap_vals, rsi_vals):
    i   = len(confirmed) - 1
    c1  = i - 1
    sig = i - 2
    if sig < VOLUME_LOOKBACK + EMA200_LEN:
        return None

    entry      = closes[i]
    vwap_now   = vwap_vals[i]
    ema50_now  = ema50_vals[i]
    ema200_now = ema200_vals[i]
    rsi_now    = rsi_vals[i]

    trend_bull = entry > ema200_now
    trend_bear = entry < ema200_now

    avg_vol = sum(vols[sig - VOLUME_LOOKBACK:sig]) / VOLUME_LOOKBACK
    ratio   = vols[sig] / avg_vol if avg_vol > 0 else 0
    vol_ok  = ratio >= VOLUME_MULTIPLIER

    swing_low  = min(lows[i - SWING_LOOKBACK:i + 1])
    swing_high = max(highs[i - SWING_LOOKBACK:i + 1])

    vwap_cross_up = any(
        closes[j] > vwap_vals[j] and closes[j-1] <= vwap_vals[j-1]
        for j in range(sig-1, sig+2) if j > 0 and j < len(closes)
    )
    vwap_cross_down = any(
        closes[j] < vwap_vals[j] and closes[j-1] >= vwap_vals[j-1]
        for j in range(sig-1, sig+2) if j > 0 and j < len(closes)
    )

    conf1_bull = closes[c1] > opens[c1] and closes[c1] > vwap_vals[c1]
    conf2_bull = closes[i]  > opens[i]  and closes[i]  > vwap_now
    conf1_bear = closes[c1] < opens[c1] and closes[c1] < vwap_vals[c1]
    conf2_bear = closes[i]  < opens[i]  and closes[i]  < vwap_now

    if entry < MIN_PRICE:
        return ratio

    if (vwap_cross_up and entry > vwap_now and entry > ema50_now
            and trend_bull and vol_ok and 40 <= rsi_now <= 65
            and conf1_bull and conf2_bull):
        sig_id = (symbol, "BUY", int(confirmed[sig]["time"]))
        if sig_id not in tight_alerted:
            sl   = min(swing_low, vwap_now * (1 - SL_BUFFER_PCT / 100))
            risk = entry - sl
            if risk <= 0 or (risk / entry * 100) < MIN_RISK_PCT:
                return ratio
            tp1 = round(entry + risk * RR_TP1, 6)
            tp2 = round(entry + risk * RR_TP2, 6)
            sl  = round(sl, 6)
            print(f"[TIGHT] {symbol} BUY | RSI={rsi_now:.1f} | vol={ratio:.1f}x")
            tight_alerted.add(sig_id)
            trade_status = ""
            if tight_auto_trade_enabled:
                oid = place_tight_order(symbol, "BUY", entry, sl, tp1, tp2)
                trade_status = "\nOrder: " + str(oid) if oid and oid != "N/A" else "\nOrder failed"
            else:
                trade_status = "\nAuto-trade OFF"
            send_tg(
                "TIGHT SIGNAL - BUY - " + symbol + "\n------------------------------\n"
                "Entry: " + str(round(entry,6)) + " | SL: " + str(sl) + "\n"
                "TP1: " + str(tp1) + " | TP2: " + str(tp2) + "\n"
                "RSI: " + str(round(rsi_now,1)) + " | Vol: " + str(round(ratio,1)) + "x" +
                trade_status + "\n------------------------------\nNiti Tight 2"
            )
            send_journal("New Trade [Tight] - " + symbol + "\nSide: BUY\nEntry: " + str(round(entry,6)) +
                "\nSL: " + str(sl) + " | TP1: " + str(tp1) + " | TP2: " + str(tp2) + "\nNiti Journal")

    if (vwap_cross_down and entry < vwap_now and entry < ema50_now
            and trend_bear and vol_ok and 35 <= rsi_now <= 60
            and conf1_bear and conf2_bear):
        sig_id = (symbol, "SELL", int(confirmed[sig]["time"]))
        if sig_id not in tight_alerted:
            sl   = max(swing_high, vwap_now * (1 + SL_BUFFER_PCT / 100))
            risk = sl - entry
            if risk <= 0 or (risk / entry * 100) < MIN_RISK_PCT:
                return ratio
            tp1 = round(entry - risk * RR_TP1, 6)
            tp2 = round(entry - risk * RR_TP2, 6)
            sl  = round(sl, 6)
            print(f"[TIGHT] {symbol} SELL | RSI={rsi_now:.1f} | vol={ratio:.1f}x")
            tight_alerted.add(sig_id)
            trade_status = ""
            if tight_auto_trade_enabled:
                oid = place_tight_order(symbol, "SELL", entry, sl, tp1, tp2)
                trade_status = "\nOrder: " + str(oid) if oid and oid != "N/A" else "\nOrder failed"
            else:
                trade_status = "\nAuto-trade OFF"
            send_tg(
                "TIGHT SIGNAL - SELL - " + symbol + "\n------------------------------\n"
                "Entry: " + str(round(entry,6)) + " | SL: " + str(sl) + "\n"
                "TP1: " + str(tp1) + " | TP2: " + str(tp2) + "\n"
                "RSI: " + str(round(rsi_now,1)) + " | Vol: " + str(round(ratio,1)) + "x" +
                trade_status + "\n------------------------------\nNiti Tight 2"
            )
            send_journal("New Trade [Tight] - " + symbol + "\nSide: SELL\nEntry: " + str(round(entry,6)) +
                "\nSL: " + str(sl) + " | TP1: " + str(tp1) + " | TP2: " + str(tp2) + "\nNiti Journal")

    return ratio


def check_fast(symbol, confirmed, closes, opens, vols, highs, lows, ema50_vals):
    i = len(confirmed) - 1
    if i < VOLUME_LOOKBACK + EMA50_LEN + 5:
        return

    entry     = closes[i]
    ema50_now = ema50_vals[i]
    if entry < MIN_PRICE:
        return

    avg_vol = sum(vols[i - VOLUME_LOOKBACK:i]) / VOLUME_LOOKBACK
    ratio   = vols[i-1] / avg_vol if avg_vol > 0 else 0
    if ratio < FAST_VOL_MULT:
        return

    trend_bull = entry > ema50_now
    trend_bear = entry < ema50_now

    breakout_high = max(highs[i-4:i])
    breakout_low  = min(lows[i-4:i])

    long_signal  = trend_bull and closes[i] > breakout_high and closes[i] > opens[i]
    short_signal = trend_bear and closes[i] < breakout_low  and closes[i] < opens[i]

    # Opposite signal — close only, no re-entry
    if symbol in fast_open_trades:
        current_side = fast_open_trades[symbol]["side"]
        if (current_side == "BUY" and short_signal) or (current_side == "SELL" and long_signal):
            print(f"[FAST CLOSE - OPPOSITE] {symbol}")
            pnl_est = 0
            if fast_auto_trade_enabled:
                close_fast_position(symbol)
            else:
                fast_open_trades.pop(symbol, None)
            return

    # Same direction or no position
    if symbol in fast_open_trades:
        return  # Already in same direction, skip

    sig_id_long  = (symbol, "BUY",  int(confirmed[i]["time"]))
    sig_id_short = (symbol, "SELL", int(confirmed[i]["time"]))

    if long_signal and sig_id_long not in fast_alerted:
        fast_alerted.add(sig_id_long)
        print(f"[FAST] {symbol} BUY | vol={ratio:.1f}x")
        lev = get_fast_leverage(symbol)
        sl_price  = round(entry * (1 - FAST_SL_PCT / 100), 6)
        tp1_price = round(entry * (1 + FAST_SL_PCT * FAST_TP1_RR / 100), 6)
        trade_status = ""
        if fast_auto_trade_enabled:
            oid = place_fast_order(symbol, "BUY", entry)
            trade_status = "\nOrder: " + str(oid) if oid and oid != "N/A" else "\nOrder failed"
        else:
            trade_status = "\nAuto-trade OFF"
        send_tg(
            "FAST SIGNAL - BUY - " + symbol + "\n------------------------------\n"
            "Entry: " + str(round(entry,6)) + " | SL: " + str(sl_price) + "\n"
            "TP1 (1:2): " + str(tp1_price) + " | Trail: 1.5%\n"
            "Vol: " + str(round(ratio,1)) + "x | Lev: " + str(lev) + "x" +
            trade_status + "\n------------------------------\nNiti Fast Signal"
        )
        send_journal("New Trade [Fast] - " + symbol + "\nSide: BUY\nEntry: " + str(round(entry,6)) +
            "\nSL: " + str(sl_price) + " | TP1: " + str(tp1_price) + " | Lev: " + str(lev) + "x\nNiti Journal")

    elif short_signal and sig_id_short not in fast_alerted:
        fast_alerted.add(sig_id_short)
        print(f"[FAST] {symbol} SELL | vol={ratio:.1f}x")
        lev = get_fast_leverage(symbol)
        sl_price  = round(entry * (1 + FAST_SL_PCT / 100), 6)
        tp1_price = round(entry * (1 - FAST_SL_PCT * FAST_TP1_RR / 100), 6)
        trade_status = ""
        if fast_auto_trade_enabled:
            oid = place_fast_order(symbol, "SELL", entry)
            trade_status = "\nOrder: " + str(oid) if oid and oid != "N/A" else "\nOrder failed"
        else:
            trade_status = "\nAuto-trade OFF"
        send_tg(
            "FAST SIGNAL - SELL - " + symbol + "\n------------------------------\n"
            "Entry: " + str(round(entry,6)) + " | SL: " + str(sl_price) + "\n"
            "TP1 (1:2): " + str(tp1_price) + " | Trail: 1.5%\n"
            "Vol: " + str(round(ratio,1)) + "x | Lev: " + str(lev) + "x" +
            trade_status + "\n------------------------------\nNiti Fast Signal"
        )
        send_journal("New Trade [Fast] - " + symbol + "\nSide: SELL\nEntry: " + str(round(entry,6)) +
            "\nSL: " + str(sl_price) + " | TP1: " + str(tp1_price) + " | Lev: " + str(lev) + "x\nNiti Journal")


def check_symbol(symbol):
    try:
        candles = get_candles(symbol, limit=350)
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

        ratio = check_tight(symbol, confirmed, closes, opens, vols, highs, lows,
                            ema50_vals, ema200_vals, vwap_vals, rsi_vals)
        check_fast(symbol, confirmed, closes, opens, vols, highs, lows, ema50_vals)

        return ratio
    except Exception as e:
        print(f"[{symbol}] error: {e}")
        return None


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
                    t_state = "ON" if tight_auto_trade_enabled else "OFF"
                    f_state = "ON" if fast_auto_trade_enabled  else "OFF"
                    send_tg("Tight 2: " + t_state + "\nFast Signal: " + f_state)
                elif text == "/fast_start":
                    fast_auto_trade_enabled = True
                    send_tg("Fast Signal Auto-trade ON.")
                elif text == "/fast_stop":
                    fast_auto_trade_enabled = False
                    send_tg("Fast Signal Auto-trade OFF.")
                elif text == "/fast_status":
                    f_state = "ON" if fast_auto_trade_enabled else "OFF"
                    send_tg("Fast Signal Auto-trade: " + f_state)
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


def monitor_loop():
    print("Monitor started - Tight 2 + Fast Signal")
    while True:
        try:
            symbols = get_futures_symbols()
            print(f"Scanning {len(symbols)} pairs... | Tight={tight_auto_trade_enabled} | Fast={fast_auto_trade_enabled}")
            ratios = []
            for sym in symbols:
                r = check_symbol(sym)
                if r is not None:
                    ratios.append(r)
                time.sleep(0.2)
            track_tight_trades()
            send_daily_summary()
            if ratios:
                top5 = sorted(ratios, reverse=True)[:5]
                print(f"[VOL DEBUG] Top 5: {[round(x,2) for x in top5]}")
            print("Scan complete. Sleeping 60s...")
        except Exception as e:
            print(f"Loop error: {e}")
        time.sleep(60)


@app.route("/")
def health():
    return "Niti Tight 2 + Fast Signal", 200


if __name__ == "__main__":
    Thread(target=monitor_loop,   daemon=True).start()
    Thread(target=handle_telegram_commands, daemon=True).start()
    Thread(target=trailing_loop,  daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
