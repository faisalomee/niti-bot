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
    syms = [c["symbol"] for c in r.get("data", [])
            if c.get("status") == 1 and "USDT" in c["symbol"]]
    return syms

def get_candles(symbol, limit=30):
    ts = int(time.time() * 1000)
    params = {
        "symbol": symbol,
        "interval": "15m",
        "limit": limit,
        "timestamp": ts,
    }
    params["signature"] = sign(params)
    headers = {"X-BX-APIKEY": API_KEY}
    url = BASE_URL + "/openApi/swap/v3/quote/klines"
    r = requests.get(url, params=params,
                     headers=headers, timeout=10).json()
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

# last_signal[symbol] = candle time of last sent signal
last_signal = {}

def check_symbol(symbol):
    try:
        candles = get_candles(symbol, limit=30)
        if len(candles) < 25:
            return

        closes = [float(c["close"]) for c in candles]
        ema21  = calc_ema(closes[:-1], 21)

        now_ms        = int(time.time() * 1000)
        candle_15m_ms = 15 * 60 * 1000

        # c1 = 3 bars ago, c2 = 2 bars ago, c3 = 1 bar ago (all confirmed)
        c1 = candles[-4]
        c2 = candles[-3]
        c3 = candles[-2]

        c1_close = float(c1["close"])
        c2_close = float(c2["close"])
        c3_close = float(c3["close"])
        c2_open  = float(c2["open"])
        c3_open  = float(c3["open"])

        c2_red   = c2_close < c2_open
        c3_red   = c3_close < c3_open
        c2_green = not c2_red
        c3_green = not c3_red

        # ── BUY ──────────────────────────────────────────
        # c1 below EMA, then c2 or c3: RED candle closes ABOVE EMA
        buy_trigger = c1_close < ema21
        buy_c2 = buy_trigger and c2_red and c2_close > ema21
        buy_c3 = (buy_trigger and c3_red and c3_close > ema21
                  and not (c2_red and c2_close > ema21))

        if buy_c2 or buy_c3:
            sig = c2 if buy_c2 else c3
            sig_time = int(sig["time"])
            # only fire if candle is recent (within last 2 candles)
            if now_ms - sig_time <= candle_15m_ms * 2:
                sig_key = (symbol, "BUY", sig_time)
                if last_signal.get(symbol) != sig_key:
                    last_signal[symbol] = sig_key
                    send_tg(
                        f"🟢 BUY — {symbol}\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"📊 EMA21: {ema21:.4f}\n"
                        f"🕯 Close: {float(sig['close']):.4f}\n"
                        f"⏱ 15m | EMA Reclaim\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"⚡ Niti Alert"
                    )
                    print(f"BUY signal sent: {symbol}")

        # ── SELL ─────────────────────────────────────────
        # c1 above EMA, then c2 or c3: GREEN candle closes BELOW EMA
        sell_trigger = c1_close > ema21
        sell_c2 = sell_trigger and c2_green and c2_close < ema21
        sell_c3 = (sell_trigger and c3_green and c3_close < ema21
                   and not (c2_green and c2_close < ema21))

        if sell_c2 or sell_c3:
            sig = c2 if sell_c2 else c3
            sig_time = int(sig["time"])
            if now_ms - sig_time <= candle_15m_ms * 2:
                sig_key = (symbol, "SELL", sig_time)
                if last_signal.get(symbol) != sig_key:
                    last_signal[symbol] = sig_key
                    send_tg(
                        f"🔴 SELL — {symbol}\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"📊 EMA21: {ema21:.4f}\n"
                        f"🕯 Close: {float(sig['close']):.4f}\n"
                        f"⏱ 15m | EMA Rejection\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"⚡ Niti Alert"
                    )
                    print(f"SELL signal sent: {symbol}")

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
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
