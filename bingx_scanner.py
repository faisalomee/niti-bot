import requests
import time
import json
import hmac
import hashlib
from datetime import datetime
import logging

# Configuration
BINGX_API_KEY = "pUrJ77AlMufGU7h9KEm2PH5aYpWQa5F0xWb2KWx2sT6iWJRZd5ghh6pHdzpU7qlpUjlRfnoA15yzb8qekw"
BINGX_SECRET_KEY = "EP0Qe6lUHyFdbePheLA aNEkOM7KkigqQNuibfmEExdyAHeP8QrBANLhskt209Q2l9E2vBwy9QZ0kdOHUw"
BINGX_UID = "32922666"

TELEGRAM_BOT_TOKEN = "7284589720:AAG-1WOaFjGKC1tVvVn3DW5YZ7Q8xR9sT0U"
TELEGRAM_CHAT_ID = "779634396"

BASE_URL = "https://open-api.bingx.com"
TIMEFRAME = "15m"
EMA_LENGTH = 20

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Storage for tracking sent alerts (to avoid duplicates)
sent_alerts = {}

def send_telegram_alert(message):
    """Send alert to Telegram"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info(f"✅ Telegram alert sent")
        else:
            logger.error(f"❌ Failed to send telegram: {response.text}")
    except Exception as e:
        logger.error(f"Telegram error: {e}")

def generate_signature(timestamp, method, request_path, body_str=""):
    """Generate BingX API signature"""
    message = timestamp + method + request_path + body_str
    signature = hmac.new(
        BINGX_SECRET_KEY.encode(),
        message.encode(),
        hashlib.sha256
    ).hexdigest()
    return signature

def make_request(method, endpoint, params=None):
    """Make authenticated request to BingX API"""
    try:
        timestamp = str(int(time.time() * 1000))
        request_path = endpoint
        if params and method == "GET":
            query_str = "&".join([f"{k}={v}" for k, v in params.items()])
            request_path = f"{endpoint}?{query_str}"
        
        signature = generate_signature(timestamp, method, request_path)
        
        headers = {
            "X-BingX-API-KEY": BINGX_API_KEY,
            "X-BingX-TIMESTAMP": timestamp,
            "X-BingX-SIGN": signature,
            "Content-Type": "application/json"
        }
        
        url = BASE_URL + request_path
        
        if method == "GET":
            response = requests.get(url, headers=headers, timeout=10)
        else:
            response = requests.post(url, json=params, headers=headers, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"API Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Request error: {e}")
        return None

def get_klines(symbol, limit=100):
    """Get kline data from BingX"""
    endpoint = "/openApi/swap/v2/quote/klines"
    params = {
        "symbol": symbol,
        "interval": TIMEFRAME,
        "limit": limit
    }
    return make_request("GET", endpoint, params)

def calculate_ema(data, length):
    """Calculate EMA"""
    if len(data) < length:
        return None
    
    prices = [float(candle[4]) for candle in data]
    ema = prices[0]
    multiplier = 2 / (length + 1)
    
    for price in prices[1:]:
        ema = price * multiplier + ema * (1 - multiplier)
    
    return ema

def check_sell_signal(candles):
    """Check SELL signal"""
    if len(candles) < 3:
        return False, None
    
    c1_open = float(candles[-1][1])
    c1_high = float(candles[-1][2])
    c1_close = float(candles[-1][4])
    
    is_red = c1_close < c1_open
    
    ema = calculate_ema(candles, EMA_LENGTH)
    if ema is None:
        return False, None
    
    red_touch = is_red and c1_high > ema and c1_close < ema
    
    if not red_touch:
        return False, None
    
    c2_open = float(candles[-2][1])
    c2_close = float(candles[-2][4])
    c2_green = c2_close > c2_open
    c2_below_ema = c2_close < ema
    
    c3_open = float(candles[-3][1])
    c3_close = float(candles[-3][4])
    c3_green = c3_close > c3_open
    c3_below_ema = c3_close < ema
    
    has_green_below = (c2_green and c2_below_ema) or (c3_green and c3_below_ema)
    
    if has_green_below:
        entry_low = min(float(candles[-2][3]), float(candles[-3][3]))
        return True, {
            "type": "SELL",
            "entry_low": entry_low,
            "ema": ema,
            "price": c1_close
        }
    
    return False, None

def check_buy_signal(candles):
    """Check BUY signal"""
    if len(candles) < 3:
        return False, None
    
    c1_open = float(candles[-1][1])
    c1_low = float(candles[-1][3])
    c1_close = float(candles[-1][4])
    
    is_green = c1_close > c1_open
    
    ema = calculate_ema(candles, EMA_LENGTH)
    if ema is None:
        return False, None
    
    green_touch = is_green and c1_low < ema and c1_close > ema
    
    if not green_touch:
        return False, None
    
    c2_open = float(candles[-2][1])
    c2_close = float(candles[-2][4])
    c2_red = c2_close < c2_open
    c2_above_ema = c2_close > ema
    
    c3_open = float(candles[-3][1])
    c3_close = float(candles[-3][4])
    c3_red = c3_close < c3_open
    c3_above_ema = c3_close > ema
    
    has_red_above = (c2_red and c2_above_ema) or (c3_red and c3_above_ema)
    
    if has_red_above:
        entry_high = max(float(candles[-2][2]), float(candles[-3][2]))
        return True, {
            "type": "BUY",
            "entry_high": entry_high,
            "ema": ema,
            "price": c1_close
        }
    
    return False, None

def scan_symbol(symbol):
    """Scan a single symbol for signals"""
    try:
        data = get_klines(symbol, limit=100)
        
        if not data or "data" not in data:
            return None
        
        candles = data["data"]
        if len(candles) < 3:
            return None
        
        sell_signal, sell_info = check_sell_signal(candles)
        if sell_signal:
            return {
                "symbol": symbol,
                "signal": "SELL",
                "info": sell_info
            }
        
        buy_signal, buy_info = check_buy_signal(candles)
        if buy_signal:
            return {
                "symbol": symbol,
                "signal": "BUY",
                "info": buy_info
            }
        
        return None
    except Exception as e:
        logger.error(f"Error scanning {symbol}: {e}")
        return None

def get_all_symbols():
    """Get list of all trading pairs"""
    try:
        endpoint = "/openApi/spot/v1/public/products"
        data = make_request("GET", endpoint)
        
        if not data or "data" not in data:
            return []
        
        symbols = []
        for product in data["data"]:
            symbol = product.get("symbol", "")
            if "USDT" in symbol:
                symbols.append(symbol)
        
        return symbols[:50]
    except Exception as e:
        logger.error(f"Error getting symbols: {e}")
        return []

def main_loop():
    """Main scanning loop"""
    logger.info("🚀 BingX EMA Scanner Bot Started")
    send_telegram_alert("🚀 BingX EMA Scanner Bot Started\nScanning all USDT pairs every 15 minutes...")
    
    while True:
        try:
            logger.info(f"\n⏰ Scanning at {datetime.now()}")
            symbols = get_all_symbols()
            logger.info(f"📊 Scanning {len(symbols)} symbols...")
            
            signals_found = []
            
            for symbol in symbols:
                result = scan_symbol(symbol)
                if result:
                    signals_found.append(result)
                    logger.info(f"✅ Signal found: {result}")
                
                time.sleep(0.1)
            
            for signal in signals_found:
                symbol = signal["symbol"]
                signal_type = signal["signal"]
                info = signal["info"]
                
                alert_key = f"{symbol}_{signal_type}_{datetime.now().hour}"
                if alert_key in sent_alerts:
                    continue
                
                message = f"""
{'🔴 SELL' if signal_type == 'SELL' else '🟢 BUY'} SIGNAL READY

Pair: {symbol}
Price: {info['price']:.8f}
EMA20: {info['ema']:.8f}

⏳ Wait for Candle 4 to enter trade
📈 Entry: {'Low ' + str(round(info['entry_low'], 8)) if signal_type == 'SELL' else 'High ' + str(round(info['entry_high'], 8))}

⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
                """
                send_telegram_alert(message)
                sent_alerts[alert_key] = True
            
            logger.info("⏳ Waiting 15 minutes for next scan...")
            time.sleep(900)
            
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main_loop()
