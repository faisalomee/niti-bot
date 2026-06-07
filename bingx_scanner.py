import os, time, hmac, hashlib, requests
from flask import Flask
from threading import Thread

app = Flask(__name__)

API_KEY    = os.environ.get("BINGX_API_KEY")
SECRET_KEY = os.environ.get("BINGX_SECRET_KEY")
TG_TOKEN   = os.environ.get("TG_BOT_TOKEN")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID")

BASE_URL = "https://open-api.bingx.com"
CANDLE_MS = 15 * 60 * 1000  # 15 minutes in milliseconds


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


def close_of(c): return float(c["close"])
def open_of(c):  return float(c["open"])
def high_of(c):  return float(c["high"])
def low_of(c):   return float(c["low"])
def is_red(c):   return close_of(c) < open_of(c)
def is_green(c): return close_of(c) > open_of(c)


# last_alerted: set of (symbol, direction, rejection_candle_time)
# prevents duplicate alerts for the same setup
last_alerted = set()


def check_symbol(symbol):
    try:
        candles = get_candles(symbol, limit=60)
        if len(candles) < 10:
            return

        # confirmed = all fully closed candles (skip last = currently open)
        confirmed = candles[:-1]
        closes    = [float(c["close"]) for c in confirmed]
        ema_vals  = calc_ema_series(closes, period=21)

        # Build a time -> index map for fast C4 lookup
        time_to_idx = {int(c["time"]): i for i, c in enumerate(confirmed)}

        # ── Scan for C1/C2/C3 setups ─────────────────────────────────
        # Look back up to 6 candles from the end.
        # C1=i, C2=i+1, C3=i+2, C4=i+3 (must all be confirmed)
        # We need i+3 to exist in confirmed, so stop at len-4.

        scan_start = max(0, len(confirmed) - 6)
        scan_end   = len(confirmed) - 3  # need C4 = i+3 to exist

        for i in range(scan_end, scan_start - 1, -1):
            c1 = confirmed[i]
            c2 = confirmed[i + 1]
            c3 = confirmed[i + 2]
            c4 = confirmed[i + 3]   # strict: must be exactly i+3

            ema_c1 = ema_vals[i]
            ema_c2 = ema_vals[i + 1]
            ema_c3 = ema_vals[i + 2]

            # ── BUY ──────────────────────────────────────────────────
            # C1: close BELOW EMA
            # C2 or C3: RED candle that closes ABOVE EMA (EMA rejection up)
            # C4: high must touch or exceed the HIGH of the rejection candle
            # Entry = HIGH of rejection candle
            # SL    = LOW  of rejection candle

            if close_of(c1) < ema_c1:
                rej = None
                if is_red(c2) and close_of(c2) > ema_c2:
                    rej = c2
                elif is_red(c3) and close_of(c3) > ema_c3:
                    rej = c3

                if rej is not None:
                    rej_time = int(rej["time"])
                    sig_id   = (symbol, "BUY", rej_time)

                    if sig_id not in last_alerted:
                        entry = high_of(rej)
                        sl    = low_of(rej)
                        rr    = abs(entry - sl)

                        # C4 must touch entry from below
                        if high_of(c4) >= entry:
                            last_alerted.add(sig_id)
                            tp1 = round(entry + rr * 3, 4)
                            tp2 = round(entry + rr * 4, 4)
                            msg = (
                                f"🟢 BUY SIGNAL — {symbol}\n"
                                f"━━━━━━━━━━━━━━━━━━\n"
                                f"⏱ Timeframe : 15m\n"
                                f"📐 Strategy  : EMA21 Rejection + C4 Retest\n"
                                f"━━━━━━━━━━━━━━━━━━\n"
                                f"🎯 Entry     : {round(entry, 4)}\n"
                                f"🛑 Stop Loss : {round(sl, 4)}\n"
                                f"💰 TP1 (1:3) : {tp1}\n"
                                f"💰 TP2 (1:4) : {tp2}\n"
                                f"━━━━━━━━━━━━━━━━━━\n"
                                f"⚡ Niti Alert"
                            )
                            send_tg(msg)
                            print(f"BUY: {symbol} | Entry={round(entry,4)} SL={round(sl,4)}")
                    break  # found most recent setup, stop scanning

            # ── SELL ─────────────────────────────────────────────────
            # C1: close ABOVE EMA
            # C2 or C3: GREEN candle that closes BELOW EMA (EMA rejection down)
            # C4: low must touch or go below the LOW of the rejection candle
            # Entry = LOW  of rejection candle
            # SL    = HIGH of rejection candle

            if close_of(c1) > ema_c1:
                rej = None
                if is_green(c2) and close_of(c2) < ema_c2:
                    rej = c2
                elif is_green(c3) and close_of(c3) < ema_c3:
                    rej = c3

                if rej is not None:
                    rej_time = int(rej["time"])
                    sig_id   = (symbol, "SELL", rej_time)

                    if sig_id not in last_alerted:
                        entry = low_of(rej)
                        sl    = high_of(rej)
                        rr    = abs(entry - sl)

                        # C4 must touch entry from above
                        if low_of(c4) <= entry:
                            last_alerted.add(sig_id)
                            tp1 = round(entry - rr * 3, 4)
                            tp2 = round(entry - rr * 4, 4)
                            msg = (
                                f"🔴 SELL SIGNAL — {symbol}\n"
                                f"━━━━━━━━━━━━━━━━━━\n"
                                f"⏱ Timeframe : 15m\n"
                                f"📐 Strategy  : EMA21 Rejection + C4 Retest\n"
                                f"━━━━━━━━━━━━━━━━━━\n"
                                f"🎯 Entry     : {round(entry, 4)}\n"
                                f"🛑 Stop Loss : {round(sl, 4)}\n"
                                f"💰 TP1 (1:3) : {tp1}\n"
                                f"💰 TP2 (1:4) : {tp2}\n"
                                f"━━━━━━━━━━━━━━━━━━\n"
                                f"⚡ Niti Alert"
                            )
                            send_tg(msg)
                            print(f"SELL: {symbol} | Entry={round(entry,4)} SL={round(sl,4)}")
                    break  # found most recent setup, stop scanning

    except Exception as e:
        print(f"[{symbol}] error: {e}")


def monitor_loop():
    print("Monitor started — EMA21 Rejection + C4 Strict Retest | 1:3/1:4 RR")
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
    return "Niti EMA Bot v3 is running!", 200


if __name__ == "__main__":
    Thread(target=monitor_loop, daemon=True).start()
    app.run(host="0.0.0.0",
            port=int(os.environ.get("PORT", 5000)))
