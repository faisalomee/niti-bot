import os
import requests
from flask import Flask, request, jsonify
from anthropic import Anthropic

app    = Flask(__name__)
claude = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "mysecret123")
TG_BOT_TOKEN   = os.environ.get("TG_BOT_TOKEN")
TG_CHAT_ID     = os.environ.get("TG_CHAT_ID")

def send_telegram(text):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)

def claude_analysis(data):
    action = data.get("action","BUY")
    symbol = data.get("symbol","TAOUSDT")
    price  = data.get("price","N/A")
    tf     = data.get("timeframe","1m")
    prompt = f"""You are a crypto trading analyst. Analyze this signal briefly in Bengali+English mix.
Signal: {action} on {symbol} at price {price} on {tf} chart.
Give 5 short lines: Trend, Volume, RSI, Verdict (Strong/Medium/Weak), Final recommendation in Bengali.
Keep it very short."""
    resp = claude.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=250,
        messages=[{"role":"user","content":prompt}]
    )
    return resp.content[0].text.strip()

@app.route("/webhook", methods=["POST"])
def webhook():
    data   = request.get_json(silent=True) or {}
    if data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error":"unauthorized"}), 401
    action = data.get("action","BUY")
    symbol = data.get("symbol","TAOUSDT")
    price  = data.get("price","N/A")
    tf     = data.get("timeframe","1m")
    tp     = data.get("tp","N/A")
    sl     = data.get("sl","N/A")
    try:
        analysis = claude_analysis(data)
    except Exception as e:
        analysis = f"Analysis unavailable: {e}"
    emoji = "🟢" if action == "BUY" else "🔴"
    msg = (f"{emoji} {action} — {symbol}\n"
           f"Price: {price} | TF: {tf}\n\n"
           f"🤖 Claude:\n{analysis}\n\n"
           f"TP: {tp} | SL: {sl}")
    send_telegram(msg)
    return jsonify({"status":"ok"})

@app.route("/", methods=["GET"])
def health():
    return "Bot running!", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT",5000)))
