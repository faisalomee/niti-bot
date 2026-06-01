import os
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "mysecret123")
TG_BOT_TOKEN   = os.environ.get("TG_BOT_TOKEN")
TG_CHAT_ID     = os.environ.get("TG_CHAT_ID")

def send_telegram(text):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    requests.post(url, json={
        "chat_id":    TG_CHAT_ID,
        "text":       text,
        "parse_mode": "HTML"
    }, timeout=10)

@app.route("/webhook", methods=["POST"])
def webhook():
    data   = request.get_json(silent=True) or {}
    if data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "unauthorized"}), 401

    action = data.get("action", "BUY")
    symbol = data.get("symbol", "TAOUSDT")
    price  = data.get("price",  "N/A")
    tf     = data.get("timeframe", "1m")
    tp     = data.get("tp", "N/A")
    sl     = data.get("sl", "N/A")

    emoji = "🟢" if action == "BUY" else "🔴"

    msg = (
        f"{emoji} {action} SIGNAL — {symbol}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💰 Price: {price}\n"
        f"⏱ Timeframe: {tf}\n"
        f"🎯 TP: {tp}\n"
        f"🛑 SL: {sl}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⚡ Stock Niti v3 Signal"
    )

    send_telegram(msg)
    return jsonify({"status": "ok"})

@app.route("/", methods=["GET"])
def health():
    return "Niti Alert Bot is running!", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
