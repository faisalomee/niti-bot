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


def get_candles(symbol, limit=50):
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


def calc_ema(closes, period=21):
    k = 2 / (period + 1)
    ema = closes[0]
    for c in closes[1:]:
        ema = c * k + ema * (1 - k)
    return ema


def send_tg(msg):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    requests.post(url, json={
        "chat_id": TG_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML"
    }, timeout=10)


last_signal = {}  # symbol -> (direction, candle_time)


def check_symbol(symbol):
    try:
        candles = get_candles(symbol, limit=50)
        if len(candles) < 30:
            return

        # Use only confirmed (closed) candles — skip the last (current open)
        confirmed = candles[:-1]

        closes = [float(c["close"]) for c in confirmed]
        ema_values = []
        k = 2 / (21 + 1)
        ema = closes[0]
        for c in closes:
            ema = c * k + ema * (1 - k)
            ema_values.append(ema)

        # c1 = 3rd last confirmed, c2 = 2nd last, c3 = last confirmed
        c1      = confirmed[-3]
        c2      = confirmed[-2]
        c3      = confirmed[-1]
        ema_c1  = ema_values[-3]
        ema_c2  = ema_values[-2]
        ema_c3  = ema_values[-1]

        def close_of(c): return float(c["close"])
        def open_of(c):  return float(c["open"])
        def is_red(c):   return close_of(c) < open_of(c)
        def is_green(c): return close_of(c) > open_of(c)

        now_ms        = int(time.time() * 1000)
        candle_15m_ms = 15 * 60 * 1000

        # ── BUY ──────────────────────────────────────────────────────────
        # c1 closed BELOW its EMA
        # c2 or c3: must be RED AND close ABOVE its own EMA
        # only one of c2/c3 fires (whichever comes first)

        if close_of(c1) < ema_c1:  # trigger
            # check c2 first
            if is_red(c2) and close_of(c2) > ema_c2:
                sig_time = int(c2["time"])
                sig_key  = ("BUY", sig_time)
                if (now_ms - sig_time <= candle_15m_ms * 2
                        and last_signal.get(symbol) != sig_key):
                    last_signal[symbol] = sig_key
                    send_tg(
                        f"🟢 BUY — {symbol}\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"📊 EMA21: {ema_c2:.4f}\n"
                        f"🕯 Close: {close_of(c2):.4f}\n"
                        f"⏱ 15m | EMA Reclaim\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"⚡ Niti Alert"
                    )
                    print(f"BUY (c2): {symbol}")

            # c2 did NOT fire → check c3
            elif is_red(c3) and close_of(c3) > ema_c3:
                sig_time = int(c3["time"])
                sig_key  = ("BUY", sig_time)
                if (now_ms - sig_time <= candle_15m_ms * 2
                        and last_signal.get(symbol) != sig_key):
                    last_signal[symbol] = sig_key
                    send_tg(
                        f"🟢 BUY — {symbol}\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"📊 EMA21: {ema_c3:.4f}\n"
                        f"🕯 Close: {close_of(c3):.4f}\n"
                        f"⏱ 15m | EMA Reclaim\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"⚡ Niti Alert"
                    )
                    print(f"BUY (c3): {symbol}")

        # ── SELL ─────────────────────────────────────────────────────────
        # c1 closed ABOVE its EMA
        # c2 or c3: must be GREEN AND close BELOW its own EMA

        if close_of(c1) > ema_c1:  # trigger
            if is_green(c2) and close_of(c2) < ema_c2:
                sig_time = int(c2["time"])
                sig_key  = ("SELL", sig_time)
                if (now_ms - sig_time <= candle_15m_ms * 2
                        and last_signal.get(symbol) != sig_key):
                    last_signal[symbol] = sig_key
                    send_tg(
                        f"🔴 SELL — {symbol}\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"📊 EMA21: {ema_c2:.4f}\n"
                        f"🕯 Close: {close_of(c2):.4f}\n"
                        f"⏱ 15m | EMA Rejection\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"⚡ Niti Alert"
                    )
                    print(f"SELL (c2): {symbol}")

            elif is_green(c3) and close_of(c3) < ema_c3:
                sig_time = int(c3["time"])
                sig_key  = ("SELL", sig_time)
                if (now_ms - sig_time <= candle_15m_ms * 2
                        and last_signal.get(symbol) != sig_key):
                    last_signal[symbol] = sig_key
                    send_tg(
                        f"🔴 SELL — {symbol}\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"📊 EMA21: {ema_c2:.4f}\n"
                        f"🕯 Close: {close_of(c3):.4f}\n"
                        f"⏱ 15m | EMA Rejection\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"⚡ Niti Alert"
                    )
                    print(f"SELL (c3): {symbol}")

    except Exception as e:
        print(f"[{symbol}] error: {e}")


def monitor_loop():
    print("Monitor started")
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
    return "Niti EMA Bot is running!", 200


if __name__ == "__main__":
    Thread(target=monitor_loop, daemon=True).start()
    app.run(host="0.0.0.0",
            port=int(os.environ.get("PORT", 5000)))
