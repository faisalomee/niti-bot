import os, time, hmac, hashlib, requests
from flask import Flask
from threading import Thread

app = Flask(__name__)

API_KEY    = os.environ.get("BINGX_API_KEY")
SECRET_KEY = os.environ.get("BINGX_SECRET_KEY")
TG_TOKEN   = os.environ.get("TG_BOT_TOKEN")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID")

BASE_URL = "https://open-api.bingx.com"


def sign(params: dict) -> str:
    qs = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    return hmac.new(SECRET_KEY.encode(), qs.encode(),
                    hashlib.sha256).hexdigest()


def get_futures_symbols():
    url = BASE_URL + "/openApi/swap/v2/quote/contracts"
    r = requests.get(url, timeout=10).json()
    return [c["symbol"] for c in r.get("data", [])
            if c.get("status") == 1 and "USDT" in c["symbol"]]


def get_candles(symbol, limit=60):
    ts = int(time.time() * 1000)
    params = {"symbol": symbol, "interval": "15m",
              "limit": limit, "timestamp": ts}
    params["signature"] = sign(params)
    url = BASE_URL + "/openApi/swap/v3/quote/klines"
    r = requests.get(url, params=params,
                     headers={"X-BX-APIKEY": API_KEY},
                     timeout=10).json()
    candles = r.get("data", [])
    candles.sort(key=lambda x: x["time"])
    return candles


def calc_ema_series(closes, period=21):
    k = 2 / (period + 1)
    ema = closes[0]
    ema_vals = []
    for c in closes:
        ema = c * k + ema * (1 - k)
        ema_vals.append(ema)
    return ema_vals


def send_tg(msg):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    requests.post(url, json={
        "chat_id": TG_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML"
    }, timeout=10)


# pending_signals: symbol -> setup dict
# last_alerted:   (symbol, direction, setup_time) -> True
pending_signals = {}
last_alerted    = {}


def close_of(c): return float(c["close"])
def open_of(c):  return float(c["open"])
def high_of(c):  return float(c["high"])
def low_of(c):   return float(c["low"])
def is_red(c):   return close_of(c) < open_of(c)
def is_green(c): return close_of(c) > open_of(c)


def check_symbol(symbol):
    try:
        candles = get_candles(symbol, limit=60)
        if len(candles) < 30:
            return

        confirmed = candles[:-1]  # skip currently open candle
        closes    = [float(c["close"]) for c in confirmed]
        ema_vals  = calc_ema_series(closes, period=21)

        # ── STEP 1: Detect C1/C2/C3 setup ────────────────────────────
        for i in range(len(confirmed) - 3, max(len(confirmed) - 8, 1), -1):
            c1     = confirmed[i]
            c2     = confirmed[i + 1]
            c3     = confirmed[i + 2]
            ema_c1 = ema_vals[i]
            ema_c2 = ema_vals[i + 1]
            ema_c3 = ema_vals[i + 2]

            # ── BUY setup ──
            # C1 close below EMA
            # C2 or C3: RED candle, close ABOVE EMA (rejection up)
            # Entry = HIGH of rejection candle
            # SL    = LOW  of rejection candle
            if close_of(c1) < ema_c1:
                rejection_candle = None
                if is_red(c2) and close_of(c2) > ema_c2:
                    rejection_candle = c2
                elif is_red(c3) and close_of(c3) > ema_c3:
                    rejection_candle = c3

                if rejection_candle is not None:
                    setup_time = int(rejection_candle["time"])
                    sig_id     = (symbol, "BUY", setup_time)
                    if sig_id not in last_alerted:
                        pending_signals[symbol] = {
                            "direction":         "BUY",
                            "entry_trigger":     high_of(rejection_candle),
                            "stop_loss":         low_of(rejection_candle),
                            "setup_candle_time": setup_time,
                            "alerted":           False,
                        }
                    break

            # ── SELL setup ──
            # C1 close above EMA
            # C2 or C3: GREEN candle, close BELOW EMA (rejection down)
            # Entry = LOW  of rejection candle
            # SL    = HIGH of rejection candle
            if close_of(c1) > ema_c1:
                rejection_candle = None
                if is_green(c2) and close_of(c2) < ema_c2:
                    rejection_candle = c2
                elif is_green(c3) and close_of(c3) < ema_c3:
                    rejection_candle = c3

                if rejection_candle is not None:
                    setup_time = int(rejection_candle["time"])
                    sig_id     = (symbol, "SELL", setup_time)
                    if sig_id not in last_alerted:
                        pending_signals[symbol] = {
                            "direction":         "SELL",
                            "entry_trigger":     low_of(rejection_candle),
                            "stop_loss":         high_of(rejection_candle),
                            "setup_candle_time": setup_time,
                            "alerted":           False,
                        }
                    break

        # ── STEP 2: C4 retest check ───────────────────────────────────
        setup = pending_signals.get(symbol)
        if setup and not setup["alerted"]:
            c4          = confirmed[-1]
            direction   = setup["direction"]
            entry       = setup["entry_trigger"]
            sl          = setup["stop_loss"]
            setup_time  = setup["setup_candle_time"]
            rr          = abs(entry - sl)

            # Expire after 4 candles (60 min)
            c4_time     = int(c4["time"])
            candle_ms   = 15 * 60 * 1000
            age_candles = (c4_time - setup_time) // candle_ms
            if age_candles > 4:
                pending_signals.pop(symbol, None)
                return

            triggered = False
            if direction == "BUY"  and high_of(c4) >= entry:
                triggered = True
            if direction == "SELL" and low_of(c4)  <= entry:
                triggered = True

            if triggered:
                sig_id           = (symbol, direction, setup_time)
                last_alerted[sig_id] = True
                setup["alerted"]     = True
                pending_signals.pop(symbol, None)

                entry_r = round(entry, 4)
                sl_r    = round(sl, 4)

                if direction == "BUY":
                    tp1 = round(entry + rr * 3, 4)
                    tp2 = round(entry + rr * 4, 4)
                    msg = (
                        f"🟢 BUY SIGNAL — {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"⏱ Timeframe : 15m\n"
                        f"📐 Strategy  : EMA21 Rejection + C4 Retest\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"🎯 Entry     : {entry_r}\n"
                        f"🛑 Stop Loss : {sl_r}\n"
                        f"💰 TP1 (1:3) : {tp1}\n"
                        f"💰 TP2 (1:4) : {tp2}\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"⚡ Niti Alert"
                    )
                else:
                    tp1 = round(entry - rr * 3, 4)
                    tp2 = round(entry - rr * 4, 4)
                    msg = (
                        f"🔴 SELL SIGNAL — {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"⏱ Timeframe : 15m\n"
                        f"📐 Strategy  : EMA21 Rejection + C4 Retest\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"🎯 Entry     : {entry_r}\n"
                        f"🛑 Stop Loss : {sl_r}\n"
                        f"💰 TP1 (1:3) : {tp1}\n"
                        f"💰 TP2 (1:4) : {tp2}\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"⚡ Niti Alert"
                    )

                send_tg(msg)
                print(f"{direction}: {symbol} | Entry={entry_r} | SL={sl_r} | TP1={tp1} | TP2={tp2}")

    except Exception as e:
        print(f"[{symbol}] error: {e}")


def monitor_loop():
    print("Monitor started — EMA21 Rejection + C4 Retest | 1:3 / 1:4 RR")
    while True:
        try:
            symbols = get_futures_symbols()
            print(f"Scanning {len(symbols)} pairs...")
            for sym in symbols:
                check_symbol(sym)
                time.sleep(0.3)
            print("Scan complete. Sleeping 14 min...")
        except Exception as e:
            print(f"Loop error: {e}")
        time.sleep(60 * 14)


@app.route("/")
def health():
    return "Niti EMA Bot v2 is running!", 200


if __name__ == "__main__":
    Thread(target=monitor_loop, daemon=True).start()
    app.run(host="0.0.0.0",
            port=int(os.environ.get("PORT", 5000)))
