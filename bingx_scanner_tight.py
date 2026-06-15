import os, time, hmac, hashlib, requests
from flask import Flask
from threading import Thread

app = Flask(__name__)

API_KEY    = os.environ.get("BINGX_API_KEY")
SECRET_KEY = os.environ.get("BINGX_SECRET_KEY")

# Separate Telegram bot/channel for this tighter strategy
TG_TOKEN   = os.environ.get("TG_BOT_TOKEN_TIGHT")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID_TIGHT")

BASE_URL = "https://open-api.bingx.com"

# -- New filter settings --
TIMEFRAME         = "1m"
VOLUME_LOOKBACK   = 100   # number of prior candles used for average volume
VOLUME_MULTIPLIER = 40    # one of C1-C4 must have volume >= avg * this


def sign(params: dict) -> str:
    qs = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    return hmac.new(SECRET_KEY.encode(), qs.encode(),
                    hashlib.sha256).hexdigest()


def get_futures_symbols():
    url = BASE_URL + "/openApi/swap/v2/quote/contracts"
    r = requests.get(url, timeout=10).json()
    return [c["symbol"] for c in r.get("data", [])
            if c.get("status") == 1 and "USDT" in c["symbol"]]


def get_candles(symbol, limit=150):
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


last_alerted = set()


def close_of(c):  return float(c["close"])
def open_of(c):   return float(c["open"])
def high_of(c):   return float(c["high"])
def low_of(c):    return float(c["low"])
def vol_of(c):    return float(c["volume"])
def is_red(c):    return close_of(c) < open_of(c)
def is_green(c):  return close_of(c) > open_of(c)


def volume_spike_ok(confirmed, i):
    """
    i = index of C1 (so C1..C4 = confirmed[i..i+3]).
    Returns True if ANY ONE of C1, C2, C3, C4 has volume >= 40x
    the average volume of the 100 candles immediately BEFORE C1.
    """
    window = confirmed[i - VOLUME_LOOKBACK:i]
    avg_vol = sum(vol_of(c) for c in window) / VOLUME_LOOKBACK
    if avg_vol <= 0:
        return False
    threshold = avg_vol * VOLUME_MULTIPLIER
    for idx in range(i, i + 4):
        if vol_of(confirmed[idx]) >= threshold:
            return True
    return False


def check_symbol(symbol):
    try:
        candles = get_candles(symbol, limit=150)
        # need >= VOLUME_LOOKBACK candles before C1, plus C1..C4, plus the open candle
        if len(candles) < VOLUME_LOOKBACK + 6:
            return

        # skip currently open (unconfirmed) candle
        confirmed = candles[:-1]
        closes    = [float(c["close"]) for c in confirmed]
        ema_vals  = calc_ema_series(closes, period=21)

        # C1=i, C2=i+1, C3=i+2, C4=i+3
        # need i >= VOLUME_LOOKBACK so there are 100 candles before C1
        scan_end   = len(confirmed) - 4
        scan_start = max(VOLUME_LOOKBACK, scan_end - 5)

        if scan_end < VOLUME_LOOKBACK:
            return

        for i in range(scan_end, scan_start - 1, -1):
            c1 = confirmed[i]
            c2 = confirmed[i + 1]
            c3 = confirmed[i + 2]
            c4 = confirmed[i + 3]

            ema_c1 = ema_vals[i]
            ema_c2 = ema_vals[i + 1]
            ema_c3 = ema_vals[i + 2]

            # -- BUY --
            # C1 close below EMA
            # C2 or C3: RED candle, close above EMA (rejection up)
            # C4 high touches rejection candle high -> entry
            # SL = rejection candle low
            if close_of(c1) < ema_c1:
                rej = None
                if is_red(c2) and close_of(c2) > ema_c2:
                    rej = c2
                elif is_red(c3) and close_of(c3) > ema_c3:
                    rej = c3

                if rej is not None:
                    sig_id = (symbol, "BUY", int(rej["time"]))
                    if sig_id not in last_alerted:
                        entry = high_of(rej)
                        sl    = low_of(rej)
                        rr    = abs(entry - sl)
                        if high_of(c4) >= entry and volume_spike_ok(confirmed, i):
                            last_alerted.add(sig_id)
                            tp1 = round(entry + rr * 3, 4)
                            tp2 = round(entry + rr * 4, 4)
                            send_tg(
                                f"BUY SIGNAL -- {symbol}\n"
                                f"------------------\n"
                                f"Timeframe : 1m\n"
                                f"Strategy  : EMA21 Rejection + C4 Retest + 40x Volume Spike\n"
                                f"------------------\n"
                                f"Entry     : {round(entry,4)}\n"
                                f"Stop Loss : {round(sl,4)}\n"
                                f"TP1 (1:3) : {tp1}\n"
                                f"TP2 (1:4) : {tp2}\n"
                                f"------------------\n"
                                f"Niti Tight Alert"
                            )
                            print(f"BUY: {symbol} | Entry={round(entry,4)} SL={round(sl,4)}")
                break

            # -- SELL --
            # C1 close above EMA
            # C2 or C3: GREEN candle, close below EMA (rejection down)
            # C4 low touches rejection candle low -> entry
            # SL = rejection candle high
            if close_of(c1) > ema_c1:
                rej = None
                if is_green(c2) and close_of(c2) < ema_c2:
                    rej = c2
                elif is_green(c3) and close_of(c3) < ema_c3:
                    rej = c3

                if rej is not None:
                    sig_id = (symbol, "SELL", int(rej["time"]))
                    if sig_id not in last_alerted:
                        entry = low_of(rej)
                        sl    = high_of(rej)
                        rr    = abs(entry - sl)
                        if low_of(c4) <= entry and volume_spike_ok(confirmed, i):
                            last_alerted.add(sig_id)
                            tp1 = round(entry - rr * 3, 4)
                            tp2 = round(entry - rr * 4, 4)
                            send_tg(
                                f"SELL SIGNAL -- {symbol}\n"
                                f"------------------\n"
                                f"Timeframe : 1m\n"
                                f"Strategy  : EMA21 Rejection + C4 Retest + 40x Volume Spike\n"
                                f"------------------\n"
                                f"Entry     : {round(entry,4)}\n"
                                f"Stop Loss : {round(sl,4)}\n"
                                f"TP1 (1:3) : {tp1}\n"
                                f"TP2 (1:4) : {tp2}\n"
                                f"------------------\n"
                                f"Niti Tight Alert"
                            )
                            print(f"SELL: {symbol} | Entry={round(entry,4)} SL={round(sl,4)}")
                break

    except Exception as e:
        print(f"[{symbol}] error: {e}")


def monitor_loop():
    print("Monitor started -- 1m EMA21 Rejection + C4 Retest + 40x Volume Spike (any 1 of 4 candles)")
    while True:
        try:
            symbols = get_futures_symbols()
            print(f"Scanning {len(symbols)} pairs...")
            for sym in symbols:
                check_symbol(sym)
                time.sleep(0.2)
            print("Scan complete. Sleeping 20s...")
        except Exception as e:
            print(f"Loop error: {e}")
        time.sleep(20)


@app.route("/")
def health():
    return "Niti Tight Volume Bot (1m) is running!", 200


if __name__ == "__main__":
    Thread(target=monitor_loop, daemon=True).start()
    app.run(host="0.0.0.0",
            port=int(os.environ.get("PORT", 5000)))
