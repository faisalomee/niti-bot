import requests
import time
import hmac
import hashlib
import os
from datetime import datetime
import logging

# ===== Configuration =====
# Read from environment variables, fallback to hardcoded values
BINGX_API_KEY = os.getenv("BINGX_API_KEY", "sJY8Cc1aFyCPGQqlUjJjtiTOo8AqUCKwdSKX8aKHewqJ61C7GcHkCRYj05oiFYegiQBQVEEfcsln8feQbaQ")
BINGX_SECRET_KEY = os.getenv("BINGX_SECRET_KEY", "NmQxcRMV1jW2UESqsudA4JtZrOVRl7UM7Zj48mVQqvfAc46Gl2Ou1kvFw3ScA9Hy7ReiMEb5BGBXBiEcL92w")

TELEGRAM_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "8969633475:AAFW0PMIM2jDxuerx9c6xaow36R2Ir9Jhns")
TELEGRAM_CHAT_ID = os.getenv("TG_CHAT_ID", "1035800369")

BASE_URL = "https://open-api.bingx.com"
TIMEFRAME = "15m"
EMA_LENGTH = 20
SCAN_INTERVAL = 900  # 15 minutes

# ===== Logging =====
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

sent_alerts = {}

def send_telegram_alert(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code == 200:
            logger.info("Telegram alert sent")
        else:
            logger.error(f"Telegram failed: {r.text}")
    except Exception as e:
        logger.error(f"Telegram error: {e}")

def get_klines(symbol, limit=100):
    """Get 15m candles for a FUTURES symbol (public endpoint, no signature needed)."""
    try:
        url = f"{BASE_URL}/openApi/swap/v3/quote/klines"
        params = {"symbol": symbol, "interval": TIMEFRAME, "limit": limit}
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        if data.get("code") == 0 and "data" in data:
            return data["data"]
        return None
    except Exception as e:
        logger.error(f"Klines error {symbol}: {e}")
        return None

def get_all_symbols():
    """Get ALL BingX USDT-M perpetual futures symbols."""
    try:
        url = f"{BASE_URL}/openApi/swap/v2/quote/contracts"
        r = requests.get(url, timeout=15)
        data = r.json()
        if data.get("code") != 0 or "data" not in data:
            logger.error(f"Contracts endpoint bad response: {data.get('code')} {data.get('msg')}")
            return []
        symbols = []
        for c in data["data"]:
            sym = c.get("symbol", "")
            status = c.get("status", 1)
            # status 1 = online/tradable
            if sym.endswith("-USDT") and status == 1:
                symbols.append(sym)
        logger.info(f"Loaded {len(symbols)} futures symbols")
        return symbols
    except Exception as e:
        logger.error(f"get_all_symbols error: {e}")
        return []

def calculate_ema(candles, length):
    """EMA on close prices. Candle dict keys: open, close, high, low."""
    closes = [float(c["close"]) for c in candles]
    if len(closes) < length:
        return None
    ema = closes[0]
    k = 2 / (length + 1)
    for price in closes[1:]:
        ema = price * k + ema * (1 - k)
    return ema

def parse_candle(c):
    """Normalize a candle into open/high/low/close floats."""
    return {
        "open": float(c["open"]),
        "high": float(c["high"]),
        "low": float(c["low"]),
        "close": float(c["close"]),
    }

def check_buy_signal(candles):
    if len(candles) < 4:
        return False, None
    ema = calculate_ema(candles, EMA_LENGTH)
    if ema is None:
        return False, None
    # Use last 3 CLOSED candles: c1 oldest -> c3 newest closed
    c1 = parse_candle(candles[-3])
    c2 = parse_candle(candles[-2])
    c3 = parse_candle(candles[-1])

    # Candle 1: green, touches EMA from below
    c1_green = c1["close"] > c1["open"]
    c1_touch = c1["low"] < ema and c1["close"] > ema
    if not (c1_green and c1_touch):
        return False, None

    # Candle 2/3: at least one RED with close above EMA
    c2_ok = (c2["close"] < c2["open"]) and (c2["close"] > ema)
    c3_ok = (c3["close"] < c3["open"]) and (c3["close"] > ema)
    if not (c2_ok or c3_ok):
        return False, None

    entry_high = max(c2["high"], c3["high"])
    sl = min(c1["low"], c2["low"])
    risk = entry_high - sl
    tp = entry_high + 4 * risk
    return True, {"type": "BUY", "entry": entry_high, "sl": sl, "tp": tp, "ema": ema, "price": c3["close"]}

def check_sell_signal(candles):
    if len(candles) < 4:
        return False, None
    ema = calculate_ema(candles, EMA_LENGTH)
    if ema is None:
        return False, None
    c1 = parse_candle(candles[-3])
    c2 = parse_candle(candles[-2])
    c3 = parse_candle(candles[-1])

    # Candle 1: red, touches EMA from above
    c1_red = c1["close"] < c1["open"]
    c1_touch = c1["high"] > ema and c1["close"] < ema
    if not (c1_red and c1_touch):
        return False, None

    # Candle 2/3: at least one GREEN with close below EMA
    c2_ok = (c2["close"] > c2["open"]) and (c2["close"] < ema)
    c3_ok = (c3["close"] > c3["open"]) and (c3["close"] < ema)
    if not (c2_ok or c3_ok):
        return False, None

    entry_low = min(c2["low"], c3["low"])
    sl = max(c1["high"], c2["high"])
    risk = sl - entry_low
    tp = entry_low - 4 * risk
    return True, {"type": "SELL", "entry": entry_low, "sl": sl, "tp": tp, "ema": ema, "price": c3["close"]}

def scan_symbol(symbol):
    candles = get_klines(symbol, limit=100)
    if not candles or len(candles) < 4:
        return None
    # BingX returns newest-first sometimes; ensure oldest-first by time
    try:
        candles = sorted(candles, key=lambda x: int(x["time"]))
    except Exception:
        pass
    buy, info = check_buy_signal(candles)
    if buy:
        return {"symbol": symbol, **info}
    sell, info = check_sell_signal(candles)
    if sell:
        return {"symbol": symbol, **info}
    return None

def format_alert(sig):
    emoji = "🟢 BUY" if sig["type"] == "BUY" else "🔴 SELL"
    return f"""{emoji} SIGNAL READY

Pair: {sig['symbol']}
Price: {sig['price']:.6f}
EMA20: {sig['ema']:.6f}

Entry: {sig['entry']:.6f}
SL: {sig['sl']:.6f}
TP (1:4): {sig['tp']:.6f}

⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"""

def main_loop():
    logger.info("BingX Futures EMA Scanner Started")
    send_telegram_alert("🚀 BingX Futures EMA Scanner Started\nScanning all USDT-M perpetual pairs every 15 min...")

    while True:
        try:
            symbols = get_all_symbols()
            if not symbols:
                logger.error("No symbols loaded - check API/endpoint. Retrying in 60s.")
                time.sleep(60)
                continue

            logger.info(f"Scanning {len(symbols)} symbols...")
            found = 0
            for sym in symbols:
                sig = scan_symbol(sym)
                if sig:
                    key = f"{sig['symbol']}_{sig['type']}_{datetime.utcnow().strftime('%Y%m%d%H')}"
                    if key not in sent_alerts:
                        send_telegram_alert(format_alert(sig))
                        sent_alerts[key] = True
                        found += 1
                        logger.info(f"SIGNAL: {sig['symbol']} {sig['type']}")
                time.sleep(0.15)  # rate-limit friendly

            logger.info(f"Scan complete. {found} new signals. Waiting 15 min...")
            time.sleep(SCAN_INTERVAL)
        except Exception as e:
            logger.error(f"Main loop error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main_loop()
