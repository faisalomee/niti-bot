import os, time, hmac, hashlib, requests
from flask import Flask
from threading import Thread
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

API_KEY        = os.environ.get("BINGX_API_KEY")
SECRET_KEY     = os.environ.get("BINGX_SECRET_KEY")
TG_TOKEN       = os.environ.get("TG_BOT_TOKEN_TIGHT")
TG_CHAT_ID     = os.environ.get("TG_CHAT_ID_TIGHT")
TG_JOURNAL_ID  = os.environ.get("TG_JOURNAL_CHAT_ID")
TRADE_AMOUNT   = float(os.environ.get("TRADE_AMOUNT", 20))
LEVERAGE       = int(os.environ.get("LEVERAGE", 10))

BASE_URL = "https://open-api.bingx.com"

TIMEFRAME         = "15m"
RSI_LEN           = 14
VOLUME_LOOKBACK   = 100
VOLUME_MULTIPLIER = 3
EMA_LEN           = 50
SWING_LOOKBACK    = 10
SL_BUFFER_PCT     = 0.15
RR_TP1            = 2.0
RR_TP2            = 4.0
MIN_PRICE         = 0.001
MIN_RISK_PCT      = 0.1

auto_trade_enabled = False
symbol_precision   = {}

open_trades       = {}
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
    return symbols


def get_candles(symbol, limit=200):
    params = build_signed_params({"symbol": symbol, "interval": TIMEFRAME, "limit": limit})
    url = BASE_URL + "/openApi/swap/v3/quote/klines"
    r = requests.get(url, params=params,
                     headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
    candles = r.get("data", [])
    if not isinstance(candles, list):
        return []
    candles.sort(key=lambda x: x["time"])
    return candles


def ema_series(closes, period):
    k = 2 / (period + 1)
    ema = closes[0]
    result = []
    for c in closes:
        ema = c * k + ema * (1 - k)
        result.append(ema)
    return result


def vwap_series(candles):
    cumulative_tp_vol = 0.0
    cumulative_vol    = 0.0
    result = []
    for c in candles:
        h  = float(c["high"])
        l  = float(c["low"])
        cl = float(c["close"])
        v  = float(c["volume"])
        tp = (h + l + cl) / 3
        cumulative_tp_vol += tp * v
        cumulative_vol    += v
        result.append(cumulative_tp_vol / cumulative_vol if cumulative_vol > 0 else tp)
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
    requests.post(url, json={
        "chat_id": cid,
        "text": msg,
        "parse_mode": "HTML"
    }, timeout=10)


def send_journal(msg):
    if TG_JOURNAL_ID:
        send_tg(msg, chat_id=TG_JOURNAL_ID)


def set_leverage(symbol):
    try:
        url = BASE_URL + "/openApi/swap/v2/trade/leverage"
        for side in ["LONG", "SHORT"]:
            params = build_signed_params({"symbol": symbol, "side": side, "leverage": LEVERAGE})
            requests.post(url, params=params,
                          headers={"X-BX-APIKEY": API_KEY}, timeout=10)
    except Exception as e:
        print(f"[LEVERAGE ERROR] {symbol}: {e}")


def place_order(symbol, side, entry, sl, tp1, tp2):
    try:
        set_leverage(symbol)
        precision  = symbol_precision.get(symbol, 4)
        total_qty  = round(TRADE_AMOUNT * LEVERAGE / entry, precision)
        half_qty   = round(total_qty / 2, precision)
        if total_qty <= 0 or half_qty <= 0:
            print(f"[ORDER SKIP] {symbol} qty too small")
            return None

        pos_side   = "LONG"  if side == "BUY"  else "SHORT"
        close_side = "SELL"  if side == "BUY"  else "BUY"
        url        = BASE_URL + "/openApi/swap/v2/trade/order"

        # Main entry order
        params = build_signed_params({
            "symbol":       symbol,
            "side":         side,
            "positionSide": pos_side,
            "type":         "MARKET",
            "quantity":     total_qty,
        })
        resp = requests.post(url, params=params,
                             headers={"X-BX-APIKEY": API_KEY}, timeout=10)
        r = resp.json()
        print(f"[ORDER RESPONSE] {symbol} {side}: {r}")
        order_id = r.get("data", {}).get("order", {}).get("orderId", "N/A")

        if order_id != "N/A":
            time.sleep(0.5)

            # SL — explicit quantity, no closePosition
            p_sl = build_signed_params({
                "symbol":        symbol,
                "side":          close_side,
                "positionSide":  pos_side,
                "type":          "STOP_MARKET",
                "stopPrice":     round(sl, 6),
                "quantity":      total_qty,
            })
            r_sl = requests.post(url, params=p_sl,
                                 headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
            print(f"[SL] {symbol}: {r_sl}")

            # TP1 — half quantity
            p_tp1 = build_signed_params({
                "symbol":        symbol,
                "side":          close_side,
                "positionSide":  pos_side,
                "type":          "TAKE_PROFIT_MARKET",
                "stopPrice":     round(tp1, 6),
                "quantity":      half_qty,
            })
            r_tp1 = requests.post(url, params=p_tp1,
                                  headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
            print(f"[TP1] {symbol}: {r_tp1}")

            # TP2 — half quantity
            p_tp2 = build_signed_params({
                "symbol":        symbol,
                "side":          close_side,
                "positionSide":  pos_side,
                "type":          "TAKE_PROFIT_MARKET",
                "stopPrice":     round(tp2, 6),
                "quantity":      half_qty,
            })
            r_tp2 = requests.post(url, params=p_tp2,
                                  headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
            print(f"[TP2] {symbol}: {r_tp2}")

            open_trades[str(order_id)] = {
                "symbol": symbol,
                "side":   side,
                "entry":  entry,
                "sl":     sl,
                "tp1":    tp1,
                "tp2":    tp2,
                "qty":    total_qty,
                "time":   datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }

        return order_id
    except Exception as e:
        print(f"[ORDER ERROR] {symbol}: {e}")
        return None


def check_order_status(order_id, symbol):
    try:
        params = build_signed_params({"symbol": symbol, "orderId": order_id})
        url = BASE_URL + "/openApi/swap/v2/trade/order"
        r = requests.get(url, params=params,
                         headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        return r.get("data", {}).get("order", {}).get("status", "")
    except:
        return ""


def track_open_trades():
    global open_trades, daily_trades
    to_remove = []
    for oid, trade in list(open_trades.items()):
        status = check_order_status(oid, trade["symbol"])
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
                daily_trades.append(trade)
                to_remove.append(oid)

                sign = "+" if pnl > 0 else ""
                send_journal(
                    "Trade Closed - " + trade["symbol"] + "\n"
                    "------------------------------\n"
                    "Side   : " + trade["side"] + "\n"
                    "Entry  : " + str(trade["entry"]) + "\n"
                    "Result : " + result + "\n"
                    "PnL    : " + sign + str(pnl) + " USDT\n"
                    "Time   : " + trade["time"] + "\n"
                    "------------------------------\n"
                    "Niti Journal"
                )
            except Exception as e:
                print(f"[TRACK ERROR] {e}")

    for oid in to_remove:
        open_trades.pop(oid, None)


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
        lines.append(str(idx) + ". " + t["symbol"] + " " + t["side"] + " | " + t.get("result","?") + " | " + ps + str(p) + " USDT")

    lines.append("------------------------------")
    lines.append("Trades   : " + str(total) + " (" + str(wins) + "W / " + str(losses) + "L)")
    lines.append("Win Rate : " + str(win_rate) + "%")
    lines.append("Total PnL: " + sign + str(total_pnl) + " USDT")
    lines.append("------------------------------")
    lines.append("Niti Journal")

    send_journal("\n".join(lines))
    daily_trades = []


last_alerted = set()


def h(c):  return float(c["high"])
def l(c):  return float(c["low"])
def cl(c): return float(c["close"])
def o(c):  return float(c["open"])
def v(c):  return float(c["volume"])


def check_symbol(symbol):
    try:
        candles = get_candles(symbol, limit=200)
        if len(candles) < VOLUME_LOOKBACK + RSI_LEN + EMA_LEN + 10:
            return None

        confirmed = candles[:-1]
        closes = [cl(c) for c in confirmed]
        opens  = [o(c)  for c in confirmed]
        vols   = [v(c)  for c in confirmed]
        highs  = [h(c)  for c in confirmed]
        lows   = [l(c)  for c in confirmed]

        ema_vals  = ema_series(closes, EMA_LEN)
        vwap_vals = vwap_series(confirmed)
        rsi_vals  = rsi_series(closes, RSI_LEN)

        i = len(confirmed) - 1
        p = i - 1

        if p < VOLUME_LOOKBACK + EMA_LEN:
            return None

        entry    = closes[i]
        vwap_now = vwap_vals[i]
        ema_now  = ema_vals[i]
        rsi_now  = rsi_vals[i]

        avg_vol = sum(vols[p - VOLUME_LOOKBACK:p]) / VOLUME_LOOKBACK
        ratio   = vols[p] / avg_vol if avg_vol > 0 else 0
        vol_ok  = ratio >= VOLUME_MULTIPLIER

        swing_low  = min(lows[i - SWING_LOOKBACK:i + 1])
        swing_high = max(highs[i - SWING_LOOKBACK:i + 1])

        vwap_cross_up = any(
            closes[j] > vwap_vals[j] and closes[j - 1] <= vwap_vals[j - 1]
            for j in range(p - 1, p + 2)
            if j > 0
        )
        vwap_cross_down = any(
            closes[j] < vwap_vals[j] and closes[j - 1] >= vwap_vals[j - 1]
            for j in range(p - 1, p + 2)
            if j > 0
        )

        if entry < MIN_PRICE:
            return ratio

        # LONG
        if (vwap_cross_up
                and closes[i] > vwap_now
                and closes[i] > ema_now
                and vol_ok
                and 40 <= rsi_now <= 65
                and closes[i] > opens[i]):

            sig_id = (symbol, "BUY", int(confirmed[p]["time"]))
            if sig_id not in last_alerted:
                sl   = min(swing_low, vwap_now * (1 - SL_BUFFER_PCT / 100))
                risk = entry - sl
                if risk <= 0 or (risk / entry * 100) < MIN_RISK_PCT:
                    return ratio
                tp1 = round(entry + risk * RR_TP1, 6)
                tp2 = round(entry + risk * RR_TP2, 6)
                sl  = round(sl, 6)
                print(f"[INFO] {symbol} BUY | RSI={rsi_now:.1f} | vol={ratio:.1f}x | VWAP cross")
                last_alerted.add(sig_id)
                trade_status = ""
                if auto_trade_enabled:
                    order_id = place_order(symbol, "BUY", entry, sl, tp1, tp2)
                    trade_status = "\nOrder placed: " + str(order_id) if order_id and order_id != "N/A" else "\nOrder failed"
                else:
                    trade_status = "\nAuto-trade OFF (signal only)"
                send_tg(
                    "BUY SIGNAL - " + symbol + "\n"
                    "------------------------------\n"
                    "Timeframe : 15m\n"
                    "Strategy  : VWAP Cross + EMA50 + 2x Vol\n"
                    "------------------------------\n"
                    "Entry     : " + str(round(entry, 6)) + "\n"
                    "Stop Loss : " + str(sl) + "\n"
                    "TP1 (1:2) : " + str(tp1) + "\n"
                    "TP2 (1:4) : " + str(tp2) + "\n"
                    "RSI       : " + str(round(rsi_now, 1)) + "\n"
                    "Vol Ratio : " + str(round(ratio, 1)) + "x\n"
                    "Amount    : $" + str(TRADE_AMOUNT) + " x " + str(LEVERAGE) + "x" +
                    trade_status + "\n"
                    "------------------------------\n"
                    "Niti Tight Bot 2"
                )
                send_journal(
                    "New Trade - " + symbol + "\n"
                    "------------------------------\n"
                    "Side  : BUY\n"
                    "Entry : " + str(round(entry, 6)) + "\n"
                    "SL    : " + str(sl) + "\n"
                    "TP1   : " + str(tp1) + "\n"
                    "TP2   : " + str(tp2) + "\n"
                    "RSI   : " + str(round(rsi_now, 1)) + " | Vol: " + str(round(ratio, 1)) + "x\n"
                    "------------------------------\n"
                    "Niti Journal"
                )

        # SHORT
        if (vwap_cross_down
                and closes[i] < vwap_now
                and closes[i] < ema_now
                and vol_ok
                and 35 <= rsi_now <= 60
                and closes[i] < opens[i]):

            sig_id = (symbol, "SELL", int(confirmed[p]["time"]))
            if sig_id not in last_alerted:
                sl   = max(swing_high, vwap_now * (1 + SL_BUFFER_PCT / 100))
                risk = sl - entry
                if risk <= 0 or (risk / entry * 100) < MIN_RISK_PCT:
                    return ratio
                tp1 = round(entry - risk * RR_TP1, 6)
                tp2 = round(entry - risk * RR_TP2, 6)
                sl  = round(sl, 6)
                print(f"[INFO] {symbol} SELL | RSI={rsi_now:.1f} | vol={ratio:.1f}x | VWAP cross")
                last_alerted.add(sig_id)
                trade_status = ""
                if auto_trade_enabled:
                    order_id = place_order(symbol, "SELL", entry, sl, tp1, tp2)
                    trade_status = "\nOrder placed: " + str(order_id) if order_id and order_id != "N/A" else "\nOrder failed"
                else:
                    trade_status = "\nAuto-trade OFF (signal only)"
                send_tg(
                    "SELL SIGNAL - " + symbol + "\n"
                    "------------------------------\n"
                    "Timeframe : 15m\n"
                    "Strategy  : VWAP Cross + EMA50 + 2x Vol\n"
                    "------------------------------\n"
                    "Entry     : " + str(round(entry, 6)) + "\n"
                    "Stop Loss : " + str(sl) + "\n"
                    "TP1 (1:2) : " + str(tp1) + "\n"
                    "TP2 (1:4) : " + str(tp2) + "\n"
                    "RSI       : " + str(round(rsi_now, 1)) + "\n"
                    "Vol Ratio : " + str(round(ratio, 1)) + "x\n"
                    "Amount    : $" + str(TRADE_AMOUNT) + " x " + str(LEVERAGE) + "x" +
                    trade_status + "\n"
                    "------------------------------\n"
                    "Niti Tight Bot 2"
                )
                send_journal(
                    "New Trade - " + symbol + "\n"
                    "------------------------------\n"
                    "Side  : SELL\n"
                    "Entry : " + str(round(entry, 6)) + "\n"
                    "SL    : " + str(sl) + "\n"
                    "TP1   : " + str(tp1) + "\n"
                    "TP2   : " + str(tp2) + "\n"
                    "RSI   : " + str(round(rsi_now, 1)) + " | Vol: " + str(round(ratio, 1)) + "x\n"
                    "------------------------------\n"
                    "Niti Journal"
                )

        return ratio

    except Exception as e:
        print(f"[{symbol}] error: {e}")
        return None


def handle_telegram_commands():
    global auto_trade_enabled
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
                    auto_trade_enabled = True
                    send_tg("Auto-trade ON. Signal ashle BingX-e order dewa hobe.")
                    print("[CMD] Auto-trade ENABLED")
                elif text == "/stop":
                    auto_trade_enabled = False
                    send_tg("Auto-trade OFF. Shudhu signal ashbe.")
                    print("[CMD] Auto-trade DISABLED")
                elif text == "/status":
                    state = "ON" if auto_trade_enabled else "OFF"
                    send_tg(
                        "Auto-trade: " + state + "\n"
                        "Strategy: VWAP + EMA50 + 2x Vol\n"
                        "Amount: $" + str(TRADE_AMOUNT) + " | Leverage: " + str(LEVERAGE) + "x"
                    )
        except Exception as e:
            print(f"[TG CMD] error: {e}")
        time.sleep(1)


def monitor_loop():
    print("Monitor started - 15m VWAP Cross + EMA50 + 2x Volume | TP1:1:2 TP2:1:4")
    while True:
        try:
            symbols = get_futures_symbols()
            print(f"Scanning {len(symbols)} pairs... | auto_trade={auto_trade_enabled}")

            ratios = []
            for sym in symbols:
                r = check_symbol(sym)
                if r is not None:
                    ratios.append(r)
                time.sleep(0.2)

            track_open_trades()
            send_daily_summary()

            if ratios:
                top5 = sorted(ratios, reverse=True)[:5]
                print(f"[VOL DEBUG] Top 5 ratios: {[round(x,2) for x in top5]} | threshold={VOLUME_MULTIPLIER}x")
            print("Scan complete. Sleeping 60s...")
        except Exception as e:
            print(f"Loop error: {e}")
        time.sleep(60)


@app.route("/")
def health():
    return "Niti Tight Bot 2 - VWAP + EMA50 + 2x Vol | TP1:1:2 TP2:1:4", 200


if __name__ == "__main__":
    Thread(target=monitor_loop, daemon=True).start()
    Thread(target=handle_telegram_commands, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
