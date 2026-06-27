import os, time, hmac, hashlib, requests
from flask import Flask
from threading import Thread

app = Flask(__name__)

API_KEY       = os.environ.get("BINGX_API_KEY")
SECRET_KEY    = os.environ.get("BINGX_SECRET_KEY")
TG_TOKEN      = os.environ.get("TG_BOT_TOKEN_TIGHT")
TG_CHAT_ID    = os.environ.get("TG_CHAT_ID_TIGHT")
TRADE_AMOUNT  = float(os.environ.get("TRADE_AMOUNT", 20))
LEVERAGE      = int(os.environ.get("LEVERAGE", 10))

BASE_URL = "https://open-api.bingx.com"

TIMEFRAME         = "15m"
EMA_FAST          = 9
EMA_SLOW          = 21
RSI_LEN           = 14
VOLUME_LOOKBACK   = 100
VOLUME_MULTIPLIER = 5
RR_RATIO          = 4
SL_BUFFER_PCT     = 0.15
SWING_LOOKBACK    = 5

auto_trade_enabled = False
symbol_precision   = {}


def sign(params: dict) -> str:
    qs = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    return hmac.new(SECRET_KEY.encode(), qs.encode(),
                    hashlib.sha256).hexdigest()


def get_futures_symbols():
    url = BASE_URL + "/openApi/swap/v2/quote/contracts"
    r = requests.get(url, timeout=10).json()
    symbols = []
    for c in r.get("data", []):
        if c.get("status") == 1 and "USDT" in c["symbol"]:
            sym = c["symbol"]
            symbols.append(sym)
            qty_precision = int(c.get("quantityPrecision", 4))
            symbol_precision[sym] = qty_precision
    return symbols


def get_candles(symbol, limit=200):
    ts = int(time.time() * 1000)
    params = {"symbol": symbol, "interval": TIMEFRAME,
              "limit": limit, "timestamp": ts}
    params["signature"] = sign(params)
    url = BASE_URL + "/openApi/swap/v3/quote/klines"
    r = requests.get(url, params=params,
                     headers={"X-BX-APIKEY": API_KEY},
                     timeout=10).json()
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


def send_tg(msg):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    requests.post(url, json={
        "chat_id": TG_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML"
    }, timeout=10)


def set_leverage(symbol):
    try:
        ts = int(time.time() * 1000)
        params = {
            "symbol": symbol,
            "side": "LONG",
            "leverage": LEVERAGE,
            "timestamp": ts
        }
        params["signature"] = sign(params)
        url = BASE_URL + "/openApi/swap/v2/trade/leverage"
        requests.post(url, params=params,
                      headers={"X-BX-APIKEY": API_KEY}, timeout=10)
        params["side"] = "SHORT"
        params["signature"] = sign(params)
        requests.post(url, params=params,
                      headers={"X-BX-APIKEY": API_KEY}, timeout=10)
    except Exception as e:
        print(f"[LEVERAGE] {symbol} error: {e}")


def place_order(symbol, side, entry, sl, tp):
    try:
        set_leverage(symbol)
        ts = int(time.time() * 1000)
        precision = symbol_precision.get(symbol, 4)
        quantity  = round(TRADE_AMOUNT * LEVERAGE / entry, precision)
        params = {
            "symbol": symbol,
            "side": side,
            "positionSide": "LONG" if side == "BUY" else "SHORT",
            "type": "MARKET",
            "quantity": quantity,
            "timestamp": ts
        }
        params["signature"] = sign(params)
        url = BASE_URL + "/openApi/swap/v2/trade/order"
        r = requests.post(url, params=params,
                          headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        order_id = r.get("data", {}).get("order", {}).get("orderId", "N/A")
        print(f"[ORDER] {symbol} {side} placed | orderId={order_id} qty={quantity} precision={precision}")

        sl_side  = "SELL" if side == "BUY" else "BUY"
        tp_side  = sl_side
        pos_side = "LONG" if side == "BUY" else "SHORT"

        for price, order_type in [(sl, "STOP_MARKET"), (tp, "TAKE_PROFIT_MARKET")]:
            ts2 = int(time.time() * 1000)
            p2 = {
                "symbol": symbol,
                "side": sl_side if order_type == "STOP_MARKET" else tp_side,
                "positionSide": pos_side,
                "type": order_type,
                "stopPrice": price,
                "closePosition": "true",
                "timestamp": ts2
            }
            p2["signature"] = sign(p2)
            requests.post(url, params=p2,
                          headers={"X-BX-APIKEY": API_KEY}, timeout=10)

        return order_id
    except Exception as e:
        print(f"[ORDER ERROR] {symbol}: {e}")
        return None


last_alerted = set()


def h(c):  return float(c["high"])
def l(c):  return float(c["low"])
def cl(c): return float(c["close"])
def v(c):  return float(c["volume"])


def check_symbol(symbol):
    global auto_trade_enabled
    try:
        candles = get_candles(symbol, limit=200)
        if len(candles) < VOLUME_LOOKBACK + RSI_LEN + 10:
            return

        confirmed = candles[:-1]
        closes = [cl(c) for c in confirmed]
        vols   = [v(c)  for c in confirmed]

        ema_fast_vals = ema_series(closes, EMA_FAST)
        ema_slow_vals = ema_series(closes, EMA_SLOW)
        rsi_vals      = rsi_series(closes, RSI_LEN)

        i = len(confirmed) - 1
        p = i - 1
        if p < VOLUME_LOOKBACK:
            return

        bull_cross = (ema_fast_vals[p] <= ema_slow_vals[p] and
                      ema_fast_vals[i] >  ema_slow_vals[i])
        bear_cross = (ema_fast_vals[p] >= ema_slow_vals[p] and
                      ema_fast_vals[i] <  ema_slow_vals[i])

        if not bull_cross and not bear_cross:
            return

        rsi_now      = rsi_vals[i]
        avg_vol      = sum(vols[i - VOLUME_LOOKBACK:i]) / VOLUME_LOOKBACK
        max_vol      = max(vols[p], vols[i])
        ratio        = max_vol / avg_vol if avg_vol > 0 else 0
        ema_slow_now = ema_slow_vals[i]
        entry        = closes[i]
        swing_low    = min(l(c) for c in confirmed[i - SWING_LOOKBACK:i + 1])
        swing_high   = max(h(c) for c in confirmed[i - SWING_LOOKBACK:i + 1])

        if bull_cross and 50 < rsi_now < 70:
            sig_id = (symbol, "BUY", int(confirmed[i]["time"]))
            if sig_id not in last_alerted:
                sl   = min(swing_low, ema_slow_now * (1 - SL_BUFFER_PCT / 100))
                risk = entry - sl
                if risk <= 0:
                    return
                tp = round(entry + risk * RR_RATIO, 4)
                sl = round(sl, 4)
                print(f"[INFO] {symbol} BUY | RSI={rsi_now:.1f} | vol_ratio={ratio:.1f}x")
                if ratio >= VOLUME_MULTIPLIER:
                    last_alerted.add(sig_id)
                    trade_status = ""
                    if auto_trade_enabled:
                        order_id = place_order(symbol, "BUY", entry, sl, tp)
                        trade_status = f"\n✅ Order placed | ID: {order_id}" if order_id else "\n❌ Order failed"
                    else:
                        trade_status = "\n⏸ Auto-trade OFF (signal only)"
                    send_tg(
                        f"🟢 BUY SIGNAL — {symbol}\n"
                        f"——————————————\n"
                        f"Timeframe : 15m\n"
                        f"Strategy  : EMA 9/21 + RSI + 5x Vol\n"
                        f"——————————————\n"
                        f"Entry     : {round(entry, 4)}\n"
                        f"Stop Loss : {sl}\n"
                        f"TP (1:4)  : {tp}\n"
                        f"RSI       : {rsi_now:.1f}\n"
                        f"Vol Ratio : {ratio:.1f}x\n"
                        f"Amount    : ${TRADE_AMOUNT} x {LEVERAGE}x"
                        f"{trade_status}\n"
                        f"——————————————\n"
                        f"Niti Tight Bot"
                    )

        if bear_cross and 30 < rsi_now < 50:
            sig_id = (symbol, "SELL", int(confirmed[i]["time"]))
            if sig_id not in last_alerted:
                sl   = max(swing_high, ema_slow_now * (1 + SL_BUFFER_PCT / 100))
                risk = sl - entry
                if risk <= 0:
                    return
                tp = round(entry - risk * RR_RATIO, 4)
                sl = round(sl, 4)
                print(f"[INFO] {symbol} SELL | RSI={rsi_now:.1f} | vol_ratio={ratio:.1f}x")
                if ratio >= VOLUME_MULTIPLIER:
                    last_alerted.add(sig_id)
                    trade_status = ""
                    if auto_trade_enabled:
                        order_id = place_order(symbol, "SELL", entry, sl, tp)
                        trade_status = f"\n✅ Order placed | ID: {order_id}" if order_id else "\n❌ Order failed"
                    else:
                        trade_status = "\n⏸ Auto-trade OFF (signal only)"
                    send_tg(
                        f"🔴 SELL SIGNAL — {symbol}\n"
                        f"——————————————\n"
                        f"Timeframe : 15m\n"
                        f"Strategy  : EMA 9/21 + RSI + 5x Vol\n"
                        f"——————————————\n"
                        f"Entry     : {round(entry, 4)}\n"
                        f"Stop Loss : {sl}\n"
                        f"TP (1:4)  : {tp}\n"
                        f"RSI       : {rsi_now:.1f}\n"
                        f"Vol Ratio : {ratio:.1f}x\n"
                        f"Amount    : ${TRADE_AMOUNT} x {LEVERAGE}x"
                        f"{trade_status}\n"
                        f"——————————————\n"
                        f"Niti Tight Bot"
                    )

    except Exception as e:
        print(f"[{symbol}] error: {e}")


def handle_telegram_commands():
    global auto_trade_enabled
    offset = None
    while True:
        try:
            url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
            params = {"timeout": 30}
            if offset:
                params["offset"] = offset
            r = requests.get(url, params=params, timeout=35).json()
            for update in r.get("result", []):
                offset = update["update_id"] + 1
                msg = update.get("message", {})
                text = msg.get("text", "").strip().lower()
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != str(TG_CHAT_ID):
                    continue
                if text == "/start":
                    auto_trade_enabled = True
                    send_tg("🟢 Auto-trade ON\nSignal আসলে BingX-এ order দেওয়া হবে।")
                    print("[CMD] Auto-trade ENABLED")
                elif text == "/stop":
                    auto_trade_enabled = False
                    send_tg("🔴 Auto-trade OFF\nShudhu signal আসবে, order দেওয়া হবে না।")
                    print("[CMD] Auto-trade DISABLED")
                elif text == "/status":
                    state = "🟢 ON" if auto_trade_enabled else "🔴 OFF"
                    send_tg(
                        f"Auto-trade: {state}\n"
                        f"Amount: ${TRADE_AMOUNT} | Leverage: {LEVERAGE}x"
                    )
        except Exception as e:
            print(f"[TG CMD] error: {e}")
        time.sleep(1)


def monitor_loop():
    print("Monitor started — 15m EMA 9/21 + RSI 14 + 5x Volume | RR 1:4")
    while True:
        try:
            symbols = get_futures_symbols()
            print(f"Scanning {len(symbols)} pairs... | auto_trade={auto_trade_enabled}")
            for sym in symbols:
                check_symbol(sym)
                time.sleep(0.2)
            print("Scan complete. Sleeping 60s...")
        except Exception as e:
            print(f"Loop error: {e}")
        time.sleep(60)


@app.route("/")
def health():
    return "Niti Tight Bot — 15m EMA 9/21 + RSI + 5x Vol | Auto-execution ready", 200


if __name__ == "__main__":
    Thread(target=monitor_loop, daemon=True).start()
    Thread(target=handle_telegram_commands, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
