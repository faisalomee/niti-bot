import os, time, hmac, hashlib, requests
from flask import Flask
from threading import Thread
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

# ==================== ENV VARS ====================
API_KEY           = os.environ.get("BINGX_API_KEY")
SECRET_KEY        = os.environ.get("BINGX_SECRET_KEY")
TG_TOKEN          = os.environ.get("TG_BOT_TOKEN_TIGHT")
TG_CHAT_ID        = os.environ.get("TG_CHAT_ID_TIGHT")
TG_JOURNAL_ID     = os.environ.get("TG_JOURNAL_CHAT_ID")
FAST_TRADE_AMOUNT = float(os.environ.get("FAST_TRADE_AMOUNT", 20))

BASE_URL = "https://open-api.bingx.com"

# ==================== FAST SIGNAL CONFIG ====================
FAST_TIMEFRAME          = os.environ.get("FAST_TIMEFRAME", "15m")  # changed 1m->15m (2026-07-16) - VBCB redesign: 1m was pure noise-chasing (mostly TimeExits); 15m matches the consolidation-breakout setups Faisal actually wants
FAST_SCAN_INTERVAL_SECONDS = int(os.environ.get("FAST_SCAN_INTERVAL_SECONDS", 180))  # back to 180s (2026-07-16) - 15m candles, 60s scanning is wasted API calls
FAST_MIN_QUOTE_VOL      = float(os.environ.get("FAST_MIN_QUOTE_VOL", 2_000_000))
FAST_MAX_SYMBOLS        = int(os.environ.get("FAST_MAX_SYMBOLS", 150))   # BUGFIX 2026-07-23: this was defined but NEVER PASSED to get_liquid_symbols (max_n=None), so Fast was silently scanning every liquid pair above the volume floor - 300-500 symbols, not 150. A full pass took minutes and was a prime suspect for the Jul 16 rate-limit backoff. Now actually applied as a hard ceiling.
FAST_GAINER_TOP_N       = int(os.environ.get("FAST_GAINER_TOP_N", 40))   # 2026-07-23: Fast exists to scalp SMALL-CAP DAILY GAINERS, but nothing in the code ever ranked by today's move - after dropping the top FAST_EXCLUDE_TOP_N by volume it just scanned every remaining small cap equally, most of them dead. Now: exclude big caps by volume (unchanged), THEN keep only the top N by 24h % gain. Set 0 to disable ranking and fall back to the old volume ordering.
FAST_RETEST_ENABLED     = os.environ.get("FAST_RETEST_ENABLED", "false").lower() == "true"   # 2026-07-23: master switch for the Jul 19 retest-limit entry. false = enter at the breakout close (original behaviour, restored on purpose). Rationale: a coin running +100-200% in a day does NOT pull back, so the retest arm-and-wait was structurally filtering out exactly the explosive moves Fast is built to catch - they expired unfilled. Known cost, accepted by Faisal 2026-07-23: more failed breakouts WILL be taken and SL hits will rise; the bet is that the runners it now catches pay for them. Flip to true to A/B the old behaviour without a rewrite.
FAST_CONSOL_LOOKBACK    = 20
FAST_BREAKOUT_ATR_MULT  = float(os.environ.get("FAST_BREAKOUT_ATR_MULT", 0.2))   # loosened from 0.3 (2026-07-11) - catch moves earlier
FAST_VOL_MULT           = float(os.environ.get("FAST_VOL_MULT", 2.0))   # VBCB (2026-07-16): breakout vol >= 2x avg of last 20 candles (was hardcoded 3.0 on 1m)
FAST_VOL_LB             = 20
FAST_TRIGGER_TIMEFRAME  = os.environ.get("FAST_TRIGGER_TIMEFRAME", "5m")
PRICE_CACHE_SECONDS     = int(os.environ.get("PRICE_CACHE_SECONDS", 30))   # 2026-07-17: ticker cache - gate/trailing/progress checks share one price fetch per symbol per 30s
AUTO_RESUME_ON_START    = os.environ.get("AUTO_RESUME_ON_START", "false").lower() == "true"   # 2026-07-17: opt-in - resume auto-trading after a Render restart without waiting for /start
TIGHT_EXTENSION_HARD_SKIP = float(os.environ.get("TIGHT_EXTENSION_HARD_SKIP", 7.0))   # 2026-07-17: same anti-chasing guard Fast has - skip breakouts after a >7xATR run (DEXE bottom-sell class of losses)
TIGHT_EXTENSION_LOOKBACK  = int(os.environ.get("TIGHT_EXTENSION_LOOKBACK", 20))
TIGHT_MIN_SL_DIST_PCT     = float(os.environ.get("TIGHT_MIN_SL_DIST_PCT", 0.3))   # 2026-07-17: skip trades whose SL sits closer than 0.3% of price - spread/noise alone stops those out   # hybrid entry (2026-07-16): box/ATR from 15m, breakout trigger from 5m close - cuts entry lag ~10min without giving up close-confirmation
EXCLUDE_XSTOCKS_TIGHT   = os.environ.get("EXCLUDE_XSTOCKS_TIGHT", "true").lower() == "true"   # 2026-07-16: NCSK tokenized stocks gap around equity sessions, thin books - poison for crypto breakout logic (SOXL/SKHYNIX SLs on Jul 16)
EXCLUDE_XSTOCKS_FAST    = os.environ.get("EXCLUDE_XSTOCKS_FAST", "false").lower() == "true"
FAST_BOX_MAX_ATR_MULT   = float(os.environ.get("FAST_BOX_MAX_ATR_MULT", 3.0))   # VBCB core filter (2026-07-16): 20-candle box height must be <= 3x ATR to count as real consolidation - kills the late-entry/chasing problem (e.g. RAVE bought at +17% top)
FAST_ATR_LEN            = 14
FAST_SL_ATR_MULT        = 1.2   # NOTE: dead/unused - actual SL uses SL_ATR_BUFFER_MULT below. Kept only for reference.
FAST_RISK_USDT          = 2.0   # hardcoded for $100 account (2026-07-18) - env override removed so stale Render vars cannot silently change sizing. Scaling plan: $150->3, $250->4, $350->5
FAST_EXCLUDE_TOP_N       = 75
FAST_EXTENSION_LOOKBACK  = 20
FAST_EXTENSION_LIMIT     = 4.0
FAST_EXTENSION_HARD_SKIP = float(os.environ.get("FAST_EXTENSION_HARD_SKIP", 7.0))   # added 2026-07-14: beyond this many x ATR, skip the trade entirely (too late/exhausted a move), not just half-size
FAST_EXTENSION_MULT      = 0.5
FAST_MARGIN_CAP_MULT     = float(os.environ.get("FAST_MARGIN_CAP_MULT", 5.0))   # env-overridable (2026-07-17) - margin cap = FAST_TRADE_AMOUNT x this
FAST_TP1_RR             = float(os.environ.get("FAST_TP1_RR", 2.0))   # raised 0.8->2.0 (2026-07-16): 0.8R half-qty TP1 banked ~$2 vs -$5 full SL -> needed ~70% WR just to break even. 2R half banks full risk amount.
FAST_TRAIL_ACTIVATE_RR  = 1.0
FAST_MAX_CONCURRENT_TRADES = 2   # hardcoded for $100 account (2026-07-18); raise to 3 at $350+
FAST_CLOSE_POSITION_MIN = float(os.environ.get("FAST_CLOSE_POSITION_MIN", 0.6))   # loosened from 0.7 (2026-07-11)
FAST_TRAIL_ATR_MULT     = float(os.environ.get("FAST_TRAIL_ATR_MULT", 1.5))
FAST_TRAIL_PCT_FALLBACK = float(os.environ.get("FAST_TRAIL_PCT_FALLBACK", 3.0))

SL_ATR_BUFFER_MULT = 0.3   # shared SL buffer, used by both Fast and Tight

# ---- Progress-based time exit (reworked 2026-07-11) ----
# Old behaviour: force-checked every 10 min and killed anything below a fixed R
# threshold, even trades that were quietly profitable. New behaviour: profit is
# never time-exited (trailing handles it); only flat/losing trades and trades whose
# profit has stalled (stagnant) get timed out. A long backup cap protects against
# a trade getting stuck open due to a bug/glitch.
FAST_PROGRESS_CHECK_SECONDS   = int(os.environ.get("FAST_PROGRESS_CHECK_SECONDS", 1800))   # 30 min between checks (2026-07-16: was 15min - too twitchy for 15m timeframe)
FAST_STAGNATION_CHECKS        = int(os.environ.get("FAST_STAGNATION_CHECKS", 3))           # consecutive non-improving checks (~45 min) before locking profit
FAST_STAGNATION_MIN_R_INCREASE = float(os.environ.get("FAST_STAGNATION_MIN_R_INCREASE", 0.1))
FAST_SAFETY_CAP_SECONDS       = int(os.environ.get("FAST_SAFETY_CAP_SECONDS", 21600))      # 6h backup net (2026-07-16: was 3.5h - 15m swings need more room)

# ---- Retest limit entry (2026-07-19) ----
# Every VBCB signal since the 2026-07-16 redesign would have lost (Faisal verified
# manually, auto-trade was off): entering at the breakout close means buying the very
# top of the extension (close-strength 0.9 = candle closed at its extreme). The
# immediate pullback then eats the tight structural SL at 10x lev, even when the
# breakout direction later plays out. Fix: NO entry at signal time. The signal only
# arms a pending retest - price must pull back to the retest level (box edge or
# trigger-candle midpoint, whichever is nearer the breakout side) and the entry
# happens THERE. No pullback within the window = no trade. Price through the SL
# before the retest fills = breakout failed, no trade (that loss is now avoided).
FAST_RETEST_EXPIRY_SECONDS = int(os.environ.get("FAST_RETEST_EXPIRY_SECONDS", 1800))   # 30 min = two 15m candles

# ---- MTF trend filter (added to Fast Signal 2026-07-11, reusing the Tight-era helper) ----
# Direction-only check against the already-closed 1h candle - no extra waiting,
# checked at the same instant as the breakout candle itself.
MTF_FILTER_ENABLED = os.environ.get("MTF_FILTER_ENABLED", "true").lower() == "true"
MTF_INTERVAL        = os.environ.get("MTF_INTERVAL", "1h")
BTC_FILTER_ENABLED  = os.environ.get("BTC_FILTER_ENABLED", "true").lower() == "true"   # 2026-07-17: alts follow BTC - block Tight breakouts against clear BTC direction
BTC_FILTER_CANDLES  = int(os.environ.get("BTC_FILTER_CANDLES", 4))                     # how many closed 15m BTC candles define "clear" direction
EMA200_LEN          = 200

MIN_PRICE = 0.001

# ==================== TIGHT CONFIG (Stock Niti strategy, replaces old Tight 2 - 2026-07-11) ====================
# Volume-spike -> cooldown -> re-entry-on-breakout strategy, all on the 1m timeframe.
TIGHT_TIMEFRAME              = "1m"
TIGHT_MIN_QUOTE_VOL          = float(os.environ.get("TIGHT_MIN_QUOTE_VOL", 1_000_000))   # liquidity floor for coin selection
TIGHT_MAX_SYMBOLS            = int(os.environ.get("TIGHT_MAX_SYMBOLS", 400))

TIGHT_BASELINE_CANDLES       = int(os.environ.get("TIGHT_BASELINE_CANDLES", 4320))   # 3 days of 1m candles
BINGX_KLINE_MAX_LIMIT        = 1440   # confirmed 2026-07-11 via live API error: "limit: This field must be less than or equal to 1440." Baseline is paginated in chunks of this size.
TIGHT_BASELINE_REFRESH_SECONDS = int(os.environ.get("TIGHT_BASELINE_REFRESH_SECONDS", 1800))   # re-fetch baseline every 30 min per symbol, not every scan

TIGHT_SPIKE_VOL_MULT         = float(os.environ.get("TIGHT_SPIKE_VOL_MULT", 20.0))   # live in-progress candle vs 3-day baseline
TIGHT_COOLDOWN_VOL_RATIO     = float(os.environ.get("TIGHT_COOLDOWN_VOL_RATIO", 3.0))   # below this = "cooling"
TIGHT_COOLDOWN_MIN_CANDLES   = int(os.environ.get("TIGHT_COOLDOWN_MIN_CANDLES", 5))     # consecutive cool candles to confirm a range
TIGHT_RANGE_MAX_ATR_MULT     = float(os.environ.get("TIGHT_RANGE_MAX_ATR_MULT", 2.0))   # reject range as "not sideways" if wider than this x ATR (filters slow-bleed)
TIGHT_REENTRY_VOL_MULT       = float(os.environ.get("TIGHT_REENTRY_VOL_MULT", 5.0))     # volume needed on the breakout candle to re-enter
TIGHT_SL_ATR_BUFFER_MULT     = float(os.environ.get("TIGHT_SL_ATR_BUFFER_MULT", 0.3))
TIGHT_RR_TP                  = float(os.environ.get("TIGHT_RR_TP", 4.0))
TIGHT_BE_TRIGGER_R           = float(os.environ.get("TIGHT_BE_TRIGGER_R", 2.0))         # move SL to breakeven at this R, full size kept
TIGHT_TRAIL_R_MULT           = float(os.environ.get("TIGHT_TRAIL_R_MULT", 1.0))         # BUGFIX 2026-07-19: was used in track_tight_trades but NEVER DEFINED - every post-TP1 trailing pass died with a silent NameError ([TIGHT TRACK ERROR] in logs), so Tight trailing never actually ran. 1.0 = trail SL one R behind best price.
TIGHT_MAX_COOLDOWN_WAIT_SECONDS = int(os.environ.get("TIGHT_MAX_COOLDOWN_WAIT_SECONDS", 3600))   # give up watching after 60 min with no breakout
TIGHT_MAX_CONCURRENT_TRADES  = 2   # hardcoded for $100 account (2026-07-18); raise to 3 at $350+
TIGHT_RISK_USDT              = 2.0   # hardcoded for $100 account (2026-07-18). Scaling plan: $150->3, $250->4, $350->5
TIGHT_LEVERAGE               = int(os.environ.get("TIGHT_LEVERAGE", 20))   # fixed, per Faisal's instruction (2026-07-11)
TIGHT_MAX_MARGIN_USDT        = 25.0   # hardcoded for $100 account (2026-07-18)
TIGHT_ATR_LEN                = 14

TIGHT_SCAN_INTERVAL_SECONDS  = int(os.environ.get("TIGHT_SCAN_INTERVAL_SECONDS", 30))
# NOTE: scanning up to TIGHT_MAX_SYMBOLS pairs on a 1m timeframe every 30s is a
# meaningfully heavier REST-polling load than the old 15m/60s cadence - watch
# BingX rate-limit responses in the logs after deploying. If it gets throttled,
# either raise this interval, cut TIGHT_MAX_SYMBOLS, or (better long-term) move
# to a websocket feed instead of REST polling for the live in-progress candle.

# ==================== GLOBAL STATE ====================
tight_auto_trade_enabled = AUTO_RESUME_ON_START
fast_auto_trade_enabled  = AUTO_RESUME_ON_START
symbol_precision   = {}
symbol_max_lev     = {}
tight_open_trades  = {}
fast_open_trades   = {}
fast_pending_retests = {}   # symbol -> armed retest waiting for pullback fill, see check_fast_pending()
fast_alerted       = set()
daily_trades       = []
last_summary_date  = None

tight_watch          = {}   # symbol -> spike/cooldown/ready state, see check_tight_symbol()
tight_baseline_cache = {}   # symbol -> {"baseline": val, "ts": ...}

mtf_cache = {}

# ---- API backoff (added 2026-07-11) ----
# BingX's rate-limit penalty for repeated bad/over-limit requests is self-renewing:
# each additional request made *while still blocked* pushes the "retry after" time
# further out. Continuing to scan during a block was preventing the block from
# ever clearing on its own. This makes the bot go fully quiet on market-data calls
# for API_BACKOFF_SECONDS as soon as a rate-limit response is detected, instead of
# keep hammering the endpoint and extending its own penalty.
API_BACKOFF_SECONDS = int(os.environ.get("API_BACKOFF_SECONDS", 1200))   # 20 min
_api_backoff_until  = 0.0
_api_backoff_logged = 0.0


def api_backoff_active():
    return time.time() < _api_backoff_until


def trigger_api_backoff(reason=""):
    global _api_backoff_until
    was_active = api_backoff_active()
    _api_backoff_until = time.time() + API_BACKOFF_SECONDS
    print(f"[API BACKOFF] pausing all market-data scanning for {API_BACKOFF_SECONDS}s - {reason}")
    if not was_active:
        # Alert once per backoff episode (2026-07-17): the Jul 16 backoff loop ran all
        # day with zero signals and the user only found out from the Render logs.
        try:
            send_tg(f"⚠️ BingX rate limit hit - all scanning paused ~{API_BACKOFF_SECONDS // 60} min. If this repeats, check Render logs for [API BACKOFF]. Reason: {str(reason)[:120]}")
        except Exception:
            pass


def _looks_like_rate_limit(resp):
    if not isinstance(resp, dict):
        return False
    msg = str(resp.get("msg", "")).lower()
    return resp.get("code") == 109429 or "over" in msg or "too many" in msg or "too frequent" in msg


# ==================== CORE API HELPERS ====================
def build_signed_params(params: dict) -> dict:
    params["timestamp"] = int(time.time() * 1000)
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    params["signature"] = hmac.new(
        SECRET_KEY.encode(), qs.encode(), hashlib.sha256
    ).hexdigest()
    return params


def get_futures_symbols():
    url = BASE_URL + "/openApi/swap/v2/quote/contracts"
    r = requests.get(url, timeout=10).json()
    symbols = []
    for c in r.get("data", []):
        if c.get("status") == 1 and "USDT" in c["symbol"]:
            sym = c["symbol"]
            symbols.append(sym)
            symbol_precision[sym] = int(c.get("quantityPrecision", 4))
            try:
                symbol_max_lev[sym] = int(float(c.get("maxLongLeverage", 20)))
            except Exception:
                symbol_max_lev[sym] = 20
    return symbols


def _ticker_gain_pct(t):
    """Today's % move from a BingX ticker entry. The exact field name is not
    guaranteed across API versions, so try the documented one first, then a couple
    of known aliases, then fall back to computing it from open/last. Returns None
    when nothing usable is present - callers must treat None as 'no gain data'."""
    for key in ("priceChangePercent", "priceChangePercentage", "changePercent"):
        raw = t.get(key)
        if raw not in (None, ""):
            try:
                # Taken literally - NO scale guessing. An earlier version multiplied
                # values below 1 by 100 on the theory they might be fractions, which
                # turned a genuine +0.5% coin into a +50% "top gainer". Confirm the
                # real units once from the [TICKER FIELDS] log line below.
                return float(str(raw).replace("%", ""))
            except Exception:
                pass
    try:
        op = float(t.get("openPrice", 0) or 0)
        last = float(t.get("lastPrice", 0) or t.get("close", 0) or 0)
        if op > 0 and last > 0:
            return (last - op) / op * 100
    except Exception:
        pass
    return None


_gain_field_logged = False


def get_liquid_symbols(symbols, min_quote_vol, max_n=None, exclude_top_n=0, rank_by_gain=0):
    """rank_by_gain (added 2026-07-23, default 0 = OFF): when > 0, the surviving
    symbols are re-sorted by today's % gain and cut to that many. Tight and Tight 3
    do not pass it, so their universe selection is byte-for-byte unchanged."""
    global _gain_field_logged
    try:
        url = BASE_URL + "/openApi/swap/v2/quote/ticker"
        r = requests.get(url, timeout=10).json()
        tickers = r.get("data", [])
        if not isinstance(tickers, list):
            return symbols[:max_n] if max_n else symbols
        sym_set = set(symbols)
        liquid = []
        for t in tickers:
            sym = t.get("symbol", "")
            if sym not in sym_set:
                continue
            try:
                qvol = float(t.get("quoteVolume", 0))
            except Exception:
                qvol = 0
            if qvol >= min_quote_vol:
                liquid.append((sym, qvol, _ticker_gain_pct(t)))
        liquid.sort(key=lambda x: x[1], reverse=True)
        if exclude_top_n > 0:
            liquid = liquid[exclude_top_n:]

        if rank_by_gain > 0:
            if not _gain_field_logged and tickers:
                # one-time dump so the real field names can be confirmed from the
                # Render logs instead of trusted blindly
                print(f"[TICKER FIELDS] sample entry keys: {sorted(tickers[0].keys())}")
                _gain_field_logged = True
            with_gain = [x for x in liquid if x[2] is not None]
            if with_gain:
                with_gain.sort(key=lambda x: x[2], reverse=True)
                liquid = with_gain[:rank_by_gain]
            else:
                # No usable gain field -> keep the old volume ordering rather than
                # silently returning nothing. Loud, because it means the gainer
                # focus is NOT active.
                print("[GAINER RANK] no usable price-change field in ticker payload - falling back to volume ordering (check [TICKER FIELDS] above)")

        if max_n is not None:
            liquid = liquid[:max_n]
        return [s for s, _q, _g in liquid]
    except Exception as e:
        print(f"[LIQUID SYMBOLS ERROR] {e}")
        return symbols[:max_n] if max_n else symbols


def get_candles(symbol, limit=350, interval="15m", end_time=None):
    global _api_backoff_logged
    if api_backoff_active():
        now = time.time()
        if now - _api_backoff_logged > 60:   # don't spam the log every single call while backed off
            remaining = int(_api_backoff_until - now)
            print(f"[API BACKOFF] still active - skipping candle requests for {remaining}s more")
            _api_backoff_logged = now
        return []
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if end_time is not None:
        params["endTime"] = int(end_time)
    params = build_signed_params(params)
    url = BASE_URL + "/openApi/swap/v3/quote/klines"
    r = requests.get(url, params=params,
                     headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
    candles = r.get("data", [])
    # ---- Diagnostic logging (added 2026-07-11) ----
    # get_candles() used to fail silently: any non-list/empty "data" (rate-limit
    # response, error payload, API cap on `limit`, etc.) just became [] with zero
    # visibility. This logs the raw response whenever the result looks off, so a
    # genuine API-side problem shows up in the Render logs instead of just quietly
    # starving both strategies of data.
    if not isinstance(candles, list):
        if _looks_like_rate_limit(r):
            trigger_api_backoff(f"{symbol} {interval}: {str(r)[:200]}")
        else:
            print(f"[CANDLES ERROR] {symbol} {interval} limit={limit} - non-list response: {str(r)[:300]}")
        return []
    if len(candles) < min(limit, 50):
        print(f"[CANDLES SHORT] {symbol} {interval} requested={limit} got={len(candles)} - raw: {str(r)[:300]}")
    candles.sort(key=lambda x: x["time"])
    return candles


_price_cache = {}   # symbol -> (price, fetched_at)

btc_direction_cache = {"dir": None, "ts": 0}

def get_btc_direction():
    """BTC market direction for the Tight filter, cached 5 min (one BTC fetch per
    scan cycle at most - zero extra per-symbol API load).
    Returns "UP" / "DOWN" only when BOTH agree:
      - last BTC_FILTER_CANDLES closed 15m candles moved net in that direction, AND
      - price is on that side of the 1h EMA200 (reuses get_mtf_trend on BTC-USDT).
    Anything mixed returns None = no opinion, both trade sides allowed - otherwise
    the filter would choke signal flow entirely in sideways markets."""
    now = time.time()
    if btc_direction_cache["ts"] and now - btc_direction_cache["ts"] < 300:
        return btc_direction_cache["dir"]
    direction = None
    try:
        candles = get_candles("BTC-USDT", limit=BTC_FILTER_CANDLES + 3, interval="15m")
        if len(candles) >= BTC_FILTER_CANDLES + 1:
            recent = candles[:-1][-BTC_FILTER_CANDLES:]   # closed candles only
            net = cl(recent[-1]) - o(recent[0])
            mom = "UP" if net > 0 else "DOWN"
            mtf = get_mtf_trend("BTC-USDT")
            if mtf is not None and mtf == mom:
                direction = mom
    except Exception as e:
        print(f"[BTC FILTER] error: {e}")
    btc_direction_cache["dir"] = direction
    btc_direction_cache["ts"]  = now
    return direction


def get_current_price(symbol):
    hit = _price_cache.get(symbol)
    if hit and time.time() - hit[1] < PRICE_CACHE_SECONDS:
        return hit[0]
    try:
        url = BASE_URL + "/openApi/swap/v2/quote/price"
        r = requests.get(url, params={"symbol": symbol}, timeout=5).json()
        price = float(r.get("data", {}).get("price", 0))
        if price > 0:
            _price_cache[symbol] = (price, time.time())
        return price
    except Exception:
        return 0


# ==================== INDICATORS ====================
def ema_series(closes, period):
    k = 2 / (period + 1)
    ema = closes[0]
    result = []
    for c in closes:
        ema = c * k + ema * (1 - k)
        result.append(ema)
    return result


def atr_series(highs, lows, closes, period=14):
    n = len(closes)
    if n < period + 1:
        return [max(h - l, 0.0001) for h, l in zip(highs, lows)]
    tr_list = [highs[0] - lows[0]]
    for i in range(1, n):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        tr_list.append(tr)
    atr_vals = [sum(tr_list[:period]) / period]
    for i in range(period, n):
        atr_vals.append((atr_vals[-1] * (period - 1) + tr_list[i]) / period)
    padding = n - len(atr_vals)
    return [atr_vals[0]] * padding + atr_vals


def get_mtf_trend(symbol):
    """Cached (5 min) higher-timeframe (MTF_INTERVAL) EMA200 trend direction. Used by
    Fast Signal as a direction-only filter - no timing delay, checked instantly
    against the already-closed 1h candle at the same moment as the breakout candle."""
    now = time.time()
    cached = mtf_cache.get(symbol)
    if cached and now - cached["ts"] < 300:
        return cached["trend"]
    try:
        candles = get_candles(symbol, limit=250, interval=MTF_INTERVAL)
        if len(candles) < 210:
            return cached["trend"] if cached else None
        closes = [cl(c) for c in candles]
        ema200 = ema_series(closes, EMA200_LEN)
        trend = "UP" if closes[-1] > ema200[-1] else "DOWN"
        mtf_cache[symbol] = {"trend": trend, "ts": now}
        return trend
    except Exception as e:
        print(f"[MTF ERROR] {symbol}: {e}")
        return cached["trend"] if cached else None


def h(c):  return float(c["high"])
def l(c):  return float(c["low"])
def cl(c): return float(c["close"])
def o(c):  return float(c["open"])
def v(c):  return float(c["volume"])


# ==================== TELEGRAM ====================
def send_tg(msg, chat_id=None):
    cid = chat_id or TG_CHAT_ID
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": cid, "text": msg, "parse_mode": "HTML"}, timeout=10)


def send_journal(msg):
    if TG_JOURNAL_ID:
        send_tg(msg, chat_id=TG_JOURNAL_ID)


def journal_closed_trade(trade):
    """Single consolidated journal entry per closed trade - kept deliberately simple,
    no intermediate messages (no TP1-banked / cooldown-triggered spam)."""
    sign = "+" if trade.get("pnl", 0) > 0 else ""
    send_journal(
        "Trade Closed [" + trade.get("label", "?") + "] - " + trade["symbol"] + "\n"
        "------------------------------\n"
        "Side  : " + trade["side"] + "\nEntry : " + str(trade["entry"]) + "\n"
        "Result: " + trade.get("result", "?") + "\n"
        "PnL   : " + sign + str(trade.get("pnl", 0)) + " USDT\n"
        "------------------------------\nNiti Journal"
    )


# ==================== LEVERAGE ====================
def get_fast_leverage(symbol):
    max_lev = symbol_max_lev.get(symbol, 20)
    calc = int(max_lev * 0.20)
    lev = max(calc, 10)
    return min(lev, max_lev)


def set_leverage_api(symbol, lev):
    try:
        url = BASE_URL + "/openApi/swap/v2/trade/leverage"
        for side in ["LONG", "SHORT"]:
            params = build_signed_params({"symbol": symbol, "side": side, "leverage": lev})
            requests.post(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10)
    except Exception as e:
        print(f"[LEVERAGE ERROR] {symbol}: {e}")


# ==================== ORDER PLACEMENT ====================
def place_market_order(symbol, side, qty, pos_side):
    url = BASE_URL + "/openApi/swap/v2/trade/order"
    params = build_signed_params({
        "symbol": symbol, "side": side,
        "positionSide": pos_side, "type": "MARKET", "quantity": qty,
    })
    r = requests.post(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
    oid = r.get("data", {}).get("order", {}).get("orderId", "N/A")
    if oid == "N/A":
        print(f"[ORDER FAIL] {symbol} {side} qty={qty} positionSide={pos_side} - BingX: {r}")
    return oid


def place_sl_order(symbol, close_side, pos_side, sl_price, qty):
    url = BASE_URL + "/openApi/swap/v2/trade/order"
    params = build_signed_params({
        "symbol": symbol, "side": close_side, "positionSide": pos_side,
        "type": "STOP_MARKET", "stopPrice": round(sl_price, 6), "quantity": qty,
        "workingType": "MARK_PRICE",
    })
    r = requests.post(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
    print(f"[SL] {symbol}: {r}")
    return r.get("data", {}).get("order", {}).get("orderId", "N/A")


def place_tp_order(symbol, close_side, pos_side, tp_price, qty):
    url = BASE_URL + "/openApi/swap/v2/trade/order"
    params = build_signed_params({
        "symbol": symbol, "side": close_side, "positionSide": pos_side,
        "type": "TAKE_PROFIT_MARKET", "stopPrice": round(tp_price, 6), "quantity": qty,
        "workingType": "MARK_PRICE",
    })
    r = requests.post(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
    oid = r.get("data", {}).get("order", {}).get("orderId", "N/A")
    if oid == "N/A":
        # Log the FULL response - this failure used to be swallowed silently, which is
        # how positions ended up live with no TP on the exchange (US-USDT 2026-07-16:
        # price blew past 2R and nothing filled, no BE move, profit round-tripped).
        print(f"[TP FAIL] {symbol} {close_side} qty={qty} stop={tp_price} - BingX: {r}")
    return oid


def place_tp_guarded(symbol, close_side, pos_side, tp_price, qty, label=""):
    """Place a TP order with one retry (mirrors place_sl_guarded). Returns tp_id,
    or "N/A" if BOTH attempts failed - caller keeps the position (SL still protects
    it) but the user MUST be alerted so they can set the TP manually."""
    tp_id = place_tp_order(symbol, close_side, pos_side, tp_price, qty)
    if tp_id and tp_id != "N/A":
        return tp_id
    time.sleep(1)
    tp_id = place_tp_order(symbol, close_side, pos_side, tp_price, qty)
    if tp_id and tp_id != "N/A":
        print(f"[TP RETRY OK] {symbol} {label} placed on 2nd attempt")
        return tp_id
    send_tg(f"⚠️ {symbol}: {label} TP order FAILED twice (target {tp_price}) - position is SL-protected but has NO take-profit on the exchange. Set it manually NOW.")
    return "N/A"


def cancel_order(symbol, order_id):
    """NOTE: verify this endpoint/method against current BingX docs before relying
    on it live - it has not been tested against the real API in this conversation."""
    try:
        url = BASE_URL + "/openApi/swap/v2/trade/order"
        params = build_signed_params({"symbol": symbol, "orderId": order_id})
        r = requests.delete(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        return r
    except Exception as e:
        print(f"[CANCEL ERROR] {symbol} {order_id}: {e}")
        return None


def check_order_status(order_id, symbol):
    try:
        params = build_signed_params({"symbol": symbol, "orderId": order_id})
        url = BASE_URL + "/openApi/swap/v2/trade/order"
        r = requests.get(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        return r.get("data", {}).get("order", {}).get("status", "")
    except Exception:
        return ""


def get_fill_price(order_id, symbol, fallback=0.0):
    """Actual average fill price for an order, for accurate PnL - falls back to the
    nominal signal-time price if the exchange hasn't got it yet or the call fails."""
    try:
        params = build_signed_params({"symbol": symbol, "orderId": order_id})
        url = BASE_URL + "/openApi/swap/v2/trade/order"
        r = requests.get(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        avg = float(r.get("data", {}).get("order", {}).get("avgPrice", 0) or 0)
        return avg if avg > 0 else fallback
    except Exception:
        return fallback


def get_available_margin():
    """Available USDT margin from BingX. Returns None if the call fails -
    callers must treat None as 'unknown, proceed' so a flaky balance endpoint
    can never freeze all trading."""
    try:
        params = build_signed_params({})
        url = BASE_URL + "/openApi/swap/v2/user/balance"
        r = requests.get(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        bal = r.get("data", {}).get("balance", {})
        if isinstance(bal, list):
            bal = next((b for b in bal if b.get("asset") == "USDT"), bal[0] if bal else {})
        avail = float(bal.get("availableMargin", 0) or 0)
        return avail if avail > 0 else None
    except Exception:
        return None


def place_sl_guarded(symbol, close_side, pos_side, sl_price, qty):
    """Place an SL order with one retry. Returns sl_id, or None if BOTH attempts
    failed - caller MUST then emergency-close the position rather than leave it
    naked (root cause suspected in the CRV-USDT TimeExit-instead-of-SL incident)."""
    sl_id = place_sl_order(symbol, close_side, pos_side, sl_price, qty)
    if sl_id and sl_id != "N/A":
        return sl_id
    time.sleep(1)
    sl_id = place_sl_order(symbol, close_side, pos_side, sl_price, qty)
    if sl_id and sl_id != "N/A":
        return sl_id
    return None


def close_fast_position(symbol, reason=""):
    if symbol not in fast_open_trades:
        return
    trade = fast_open_trades[symbol]
    try:
        pos_side   = "LONG"  if trade["side"] == "BUY" else "SHORT"
        close_side = "SELL"  if trade["side"] == "BUY" else "BUY"
        remaining  = trade.get("remaining_qty", 0)
        exit_price = get_current_price(symbol)
        if remaining > 0 and fast_auto_trade_enabled:
            close_oid = place_market_order(symbol, close_side, remaining, pos_side)
            if close_oid and close_oid != "N/A":
                time.sleep(0.5)
                exit_price = get_fill_price(close_oid, symbol, fallback=exit_price)
        if trade.get("sl_id"):
            cancel_order(symbol, trade["sl_id"])
        if not trade.get("tp1_filled") and trade.get("tp1_id"):
            cancel_order(symbol, trade["tp1_id"])
        entry_ref = trade.get("entry_fill", trade["entry"])
        if trade["side"] == "BUY":
            leg_pnl = (exit_price - entry_ref) * remaining
        else:
            leg_pnl = (entry_ref - exit_price) * remaining
        total_pnl = round(trade.get("partial_pnl", 0.0) + leg_pnl, 2)
        trade["pnl"]    = total_pnl
        trade["result"] = reason
        trade["label"]  = "Fast"
        trade["symbol"] = symbol
        daily_trades.append(trade)
        journal_closed_trade(trade)
        del fast_open_trades[symbol]
        print(f"[FAST CLOSE] {symbol} {reason}")
    except Exception as e:
        print(f"[FAST CLOSE ERROR] {symbol}: {e}")


def close_tight_position(oid, reason=""):
    trade = tight_open_trades.get(oid)
    if not trade:
        return
    try:
        symbol     = trade["symbol"]
        pos_side   = trade["pos_side"]
        close_side = trade["close_side"]
        qty        = trade.get("remaining_qty", trade.get("total_qty", 0))
        entry_ref  = trade.get("entry_fill", trade["entry"])
        exit_price = get_current_price(symbol)
        if qty > 0 and tight_auto_trade_enabled:
            close_oid = place_market_order(symbol, close_side, qty, pos_side)
            if close_oid and close_oid != "N/A":
                time.sleep(0.5)
                exit_price = get_fill_price(close_oid, symbol, fallback=exit_price)
        if trade.get("sl_id"):
            cancel_order(symbol, trade["sl_id"])
        if trade.get("tp_id"):
            cancel_order(symbol, trade["tp_id"])
        if trade["side"] == "BUY":
            leg_pnl = (exit_price - entry_ref) * qty
        else:
            leg_pnl = (entry_ref - exit_price) * qty
        trade["pnl"]    = round(trade.get("tp1_pnl", 0) + leg_pnl, 2)
        trade["result"] = reason
        trade["label"]  = "Tight"
        daily_trades.append(trade)
        journal_closed_trade(trade)
        tight_open_trades.pop(oid, None)
        print(f"[TIGHT CLOSE] {symbol} {reason}")
    except Exception as e:
        print(f"[TIGHT CLOSE ERROR] {oid}: {e}")


def place_fast_order(symbol, side, entry, sl_price, tp1_price, atr_now, risk_usdt=FAST_RISK_USDT):
    try:
        lev       = get_fast_leverage(symbol)
        set_leverage_api(symbol, lev)
        precision  = symbol_precision.get(symbol, 4)
        risk_dist  = abs(entry - sl_price)
        if risk_dist <= 0:
            return None

        risk_qty      = risk_usdt / risk_dist
        margin_cap_qty = (FAST_TRADE_AMOUNT * FAST_MARGIN_CAP_MULT * lev) / entry
        total_qty     = round(min(risk_qty, margin_cap_qty), precision)
        half_qty      = round(total_qty / 2, precision)
        if total_qty <= 0 or half_qty <= 0:
            return None
        pos_side   = "LONG"  if side == "BUY" else "SHORT"
        close_side = "SELL"  if side == "BUY" else "BUY"
        sl_pct = abs(entry - sl_price) / entry * 100

        # Pre-trade margin check: skip cleanly instead of firing an order BingX will
        # reject with 101204. None (endpoint failure) = unknown -> proceed.
        required_margin = total_qty * entry / lev
        avail = get_available_margin()
        if avail is not None and avail < required_margin * 1.05:
            print(f"[FAST MARGIN SKIP] {symbol} need ~${required_margin:.2f}, available ${avail:.2f} - skipping")
            return "MARGIN_SKIP"

        order_id = place_market_order(symbol, side, total_qty, pos_side)
        print(f"[FAST ORDER] {symbol} {side} lev={lev}x qty={total_qty} risk=${risk_usdt}: {order_id}")
        if order_id != "N/A":
            time.sleep(0.5)
            entry_fill = get_fill_price(order_id, symbol, fallback=entry)

            # Recompute risk distance and TP1 from the REAL fill price so the stated
            # R-multiples are real (see 2026-07-15 finding: nominal-entry levels made
            # TP1/Trail profits tiny). SL stays structural (unchanged).
            risk_dist = abs(entry_fill - sl_price)
            if risk_dist <= 0:
                risk_dist = abs(entry - sl_price)
            if side == "BUY":
                tp1_price = round(entry_fill + risk_dist * FAST_TP1_RR, 6)   # price precision, NOT qty precision (2026-07-17: qty-precision rounding turned STX TP into 0.0 -> silent reject)
            else:
                tp1_price = round(entry_fill - risk_dist * FAST_TP1_RR, 6)

            # SL first, guarded: if it can't be placed even on retry, close the
            # position immediately instead of leaving it naked.
            sl_id = place_sl_guarded(symbol, close_side, pos_side, sl_price, total_qty)
            if sl_id is None:
                print(f"[FAST SL GUARD] {symbol} SL placement failed twice - emergency closing position")
                place_market_order(symbol, close_side, total_qty, pos_side)
                send_tg(f"⚠️ FAST {symbol}: SL placement failed - position emergency-closed for safety")
                return None
            tp1_id = place_tp_guarded(symbol, close_side, pos_side, tp1_price, half_qty, label="TP1")
            fast_open_trades[symbol] = {
                "symbol": symbol, "side": side, "entry": entry, "entry_fill": entry_fill,
                "sl": sl_price, "sl_id": sl_id, "tp1": tp1_price, "tp1_id": tp1_id, "lev": lev,
                "sl_pct": sl_pct, "close_side": close_side, "pos_side": pos_side,
                "total_qty": total_qty, "remaining_qty": total_qty, "tp1_filled": False, "partial_pnl": 0.0,
                "trail_price": entry_fill, "activated": False, "order_id": order_id,
                "atr_at_entry": atr_now, "opened_ts": time.time(),
                "risk_dist": risk_dist, "be_price": None,
                "next_check_ts": time.time() + FAST_PROGRESS_CHECK_SECONDS,
                "best_favorable_r": 0.0, "stagnation_count": 0,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
        return order_id
    except Exception as e:
        print(f"[FAST ORDER ERROR] {symbol}: {e}")
        return None


def track_fast_trades():
    for symbol in list(fast_open_trades.keys()):
        try:
            trade = fast_open_trades[symbol]

            if not trade.get("tp1_filled") and trade.get("tp1_id"):
                status = check_order_status(trade["tp1_id"], symbol)
                if status == "FILLED":
                    half_qty  = round(trade["total_qty"] / 2, symbol_precision.get(symbol, 4))
                    entry_ref = trade.get("entry_fill", trade["entry"])
                    tp1_fill  = get_fill_price(trade["tp1_id"], symbol, fallback=trade["tp1"])
                    trade["tp1_fill"] = tp1_fill
                    leg_pnl   = (tp1_fill - entry_ref) * half_qty if trade["side"] == "BUY" else (entry_ref - tp1_fill) * half_qty
                    trade["partial_pnl"]   = trade.get("partial_pnl", 0.0) + leg_pnl
                    trade["remaining_qty"] = trade["total_qty"] - half_qty
                    trade["tp1_filled"]    = True

                    # Guarded re-placement, new-first order (2026-07-17): place the BE SL
                    # BEFORE cancelling the old one, so a placement failure can never
                    # leave the position naked. Sub-second overlap of two SLs is the
                    # lesser risk. True breakeven = the REAL fill price.
                    new_sl_id = place_sl_guarded(
                        symbol, trade["close_side"], trade["pos_side"], entry_ref, trade["remaining_qty"]
                    )
                    if new_sl_id:
                        if trade.get("sl_id"):
                            cancel_order(symbol, trade["sl_id"])
                        trade["sl_id"]    = new_sl_id
                        trade["sl"]       = entry_ref
                        trade["be_price"] = entry_ref
                    else:
                        print(f"[FAST BE FAIL] {symbol} BE SL re-placement failed twice - keeping original SL {trade['sl']}")
                        if not trade.get("sl_move_alerted"):
                            trade["sl_move_alerted"] = True
                            send_tg(f"⚠️ FAST {symbol}: could not move SL to breakeven ({entry_ref}) - original SL {trade['sl']} still active. Consider moving it manually.")

            sl_status = check_order_status(trade["sl_id"], symbol) if trade.get("sl_id") else ""
            if sl_status == "FILLED":
                if not trade.get("tp1_filled") and trade.get("tp1_id"):
                    cancel_order(symbol, trade["tp1_id"])
                remaining = trade.get("remaining_qty", 0)
                entry_ref = trade.get("entry_fill", trade["entry"])
                sl_fill   = get_fill_price(trade["sl_id"], symbol, fallback=trade["sl"])
                if trade["side"] == "BUY":
                    leg_pnl = (sl_fill - entry_ref) * remaining
                else:
                    leg_pnl = (entry_ref - sl_fill) * remaining
                total_pnl = round(trade.get("partial_pnl", 0.0) + leg_pnl, 2)
                trade["pnl"]    = total_pnl
                trade["result"] = "BE" if trade.get("tp1_filled") else "SL"
                trade["label"]  = "Fast"
                trade["symbol"] = symbol
                daily_trades.append(trade)
                journal_closed_trade(trade)
                del fast_open_trades[symbol]
                continue

            # ---- Progress-based time exit (reworked 2026-07-11) ----
            # Profit (favorable_r > 0): never TimeExit - hold for trailing to manage,
            # unless it stagnates (see stagnation_count below).
            # Flat/loss (favorable_r <= 0): exit at the next check, same as before.
            # A long safety cap (backup net only) applies regardless of state.
            now_ts = time.time()

            opened_ts = trade.get("opened_ts", now_ts)
            if now_ts - opened_ts >= FAST_SAFETY_CAP_SECONDS:
                print(f"[FAST SAFETY CAP] {symbol} - {FAST_SAFETY_CAP_SECONDS}s backup cap reached")
                close_fast_position(symbol, "SafetyCap")
                continue

            if now_ts >= trade.get("next_check_ts", now_ts + 1):
                current = get_current_price(symbol)
                risk_dist = trade.get("risk_dist", 0)
                if current > 0 and risk_dist > 0:
                    entry_ref = trade.get("entry_fill", trade["entry"])
                    if trade["side"] == "BUY":
                        favorable_r = (current - entry_ref) / risk_dist
                    else:
                        favorable_r = (entry_ref - current) / risk_dist

                    if favorable_r > 0:
                        best_r = trade.get("best_favorable_r", 0.0)
                        if favorable_r > best_r + FAST_STAGNATION_MIN_R_INCREASE:
                            trade["best_favorable_r"]  = favorable_r
                            trade["stagnation_count"]  = 0
                            trade["next_check_ts"]     = now_ts + FAST_PROGRESS_CHECK_SECONDS
                            print(f"[FAST PROGRESS] {symbol} improving - {favorable_r:.2f}R")
                        else:
                            trade["stagnation_count"] = trade.get("stagnation_count", 0) + 1
                            if trade["stagnation_count"] >= FAST_STAGNATION_CHECKS:
                                print(f"[FAST STAGNANT] {symbol} - locking {favorable_r:.2f}R, no longer improving")
                                close_fast_position(symbol, "Stagnant")
                                continue
                            else:
                                trade["next_check_ts"] = now_ts + FAST_PROGRESS_CHECK_SECONDS
                                print(f"[FAST PROGRESS] {symbol} stalled ({trade['stagnation_count']}/{FAST_STAGNATION_CHECKS}) - {favorable_r:.2f}R")
                    else:
                        print(f"[FAST TIME EXIT] {symbol} - flat/dead ({favorable_r:.2f}R)")
                        close_fast_position(symbol, "TimeExit")
                        continue
                else:
                    trade["next_check_ts"] = now_ts + FAST_PROGRESS_CHECK_SECONDS
        except Exception as e:
            print(f"[FAST TRACK ERROR] {symbol}: {e}")


def update_fast_trailing():
    for symbol in list(fast_open_trades.keys()):
        try:
            trade   = fast_open_trades[symbol]
            current = get_current_price(symbol)
            if current <= 0:
                continue
            side = trade["side"]
            entry_ref = trade.get("entry_fill", trade["entry"])
            activate_pct = (FAST_TRAIL_ACTIVATE_RR * trade.get("sl_pct", 1.5)) / 100
            atr_ref    = trade.get("atr_at_entry", 0)
            trail_dist = atr_ref * FAST_TRAIL_ATR_MULT if atr_ref > 0 else entry_ref * (FAST_TRAIL_PCT_FALLBACK / 100)

            if not trade.get("activated"):
                if side == "BUY" and current >= entry_ref * (1 + activate_pct):
                    fast_open_trades[symbol]["activated"]   = True
                    fast_open_trades[symbol]["trail_price"] = current
                elif side == "SELL" and current <= entry_ref * (1 - activate_pct):
                    fast_open_trades[symbol]["activated"]   = True
                    fast_open_trades[symbol]["trail_price"] = current
                continue

            if side == "BUY":
                if current > trade["trail_price"]:
                    fast_open_trades[symbol]["trail_price"] = current
                trail_sl = trade["trail_price"] - trail_dist
                if current <= trail_sl:
                    close_fast_position(symbol, "Trail")
            else:
                if current < trade["trail_price"]:
                    fast_open_trades[symbol]["trail_price"] = current
                trail_sl = trade["trail_price"] + trail_dist
                if current >= trail_sl:
                    close_fast_position(symbol, "Trail")
        except Exception as e:
            print(f"[TRAIL ERROR] {symbol}: {e}")


def fast_enter_now(symbol, side, entry, sl_price, atr_now, risk_usdt, lev,
                   ratio, close_strength, ext_tag, extension):
    """Immediate entry at the breakout close - used when FAST_RETEST_ENABLED is off
    (2026-07-23). Mirrors the concurrency / auto-trade / margin handling in
    check_fast_pending() so both entry paths behave identically once an order is
    actually placed; the only difference is WHERE the entry price comes from."""
    risk = abs(entry - sl_price)
    if risk <= 0:
        return
    if side == "BUY":
        tp1_price = round(entry + risk * FAST_TP1_RR, 6)
    else:
        tp1_price = round(entry - risk * FAST_TP1_RR, 6)

    trade_status = ""
    if fast_auto_trade_enabled and len(fast_open_trades) >= FAST_MAX_CONCURRENT_TRADES:
        trade_status = "\nSkipped - max concurrent trades (" + str(FAST_MAX_CONCURRENT_TRADES) + ") reached"
    elif fast_auto_trade_enabled:
        oid = place_fast_order(symbol, side, entry, sl_price, tp1_price, atr_now, risk_usdt)
        if oid == "MARGIN_SKIP":
            trade_status = "\nSkipped - insufficient margin"
        elif oid and oid != "N/A":
            trade_status = "\nOrder: " + str(oid)
        else:
            trade_status = "\nOrder failed"
    else:
        trade_status = "\nAuto-trade OFF"

    print(f"[FAST ENTRY] {symbol} {side} @ {entry} SL {sl_price} | vol={ratio:.1f}x | lev={lev}x | {ext_tag} | ext={extension:.1f}x ATR{trade_status}")
    send_tg(
        "FAST ENTRY - " + side + " - " + symbol + "\n------------------------------\n"
        "Entry: " + str(round(entry, 6)) + " (breakout close, no retest wait) | SL: " + str(sl_price) + "\n"
        "TP1 (" + str(FAST_TP1_RR) + "R): " + str(tp1_price) + " | ATR-trail after " + str(FAST_TRAIL_ACTIVATE_RR) + "R\n"
        "Breakout vol: " + str(round(ratio, 1)) + "x | Close-strength: " + str(round(close_strength, 2)) +
        " | Lev: " + str(lev) + "x | " + ext_tag + " | Risk: $" + str(risk_usdt) +
        trade_status + "\n------------------------------\nNiti Fast Signal"
    )


# ==================== FAST SIGNAL ENTRY LOGIC ====================
def check_fast(symbol):
    try:
        candles = get_candles(symbol, limit=100, interval=FAST_TIMEFRAME)
        min_needed = max(FAST_CONSOL_LOOKBACK + FAST_VOL_LB + FAST_ATR_LEN + 5, FAST_EXTENSION_LOOKBACK + 5)
        if len(candles) < min_needed:
            return

        confirmed = candles[:-1]
        closes = [cl(c) for c in confirmed]
        opens  = [o(c)  for c in confirmed]
        highs  = [h(c)  for c in confirmed]
        lows   = [l(c)  for c in confirmed]
        vols   = [v(c)  for c in confirmed]

        i = len(confirmed) - 1
        if i < min_needed:
            return

        atr_vals = atr_series(highs, lows, closes, FAST_ATR_LEN)
        atr_now  = atr_vals[i]

        ext_move  = abs(closes[i] - closes[i - FAST_EXTENSION_LOOKBACK])
        extension = ext_move / atr_now if atr_now > 0 else 0
        is_extended = extension > FAST_EXTENSION_LIMIT
        if extension > FAST_EXTENSION_HARD_SKIP:
            print(f"[FAST SKIP] {symbol} extension={extension:.1f}x ATR exceeds hard-skip limit ({FAST_EXTENSION_HARD_SKIP}x) - move too exhausted, no trade")
            return

        box_high = max(highs[i - FAST_CONSOL_LOOKBACK:i])
        box_low  = min(lows[i - FAST_CONSOL_LOOKBACK:i])

        # ---- Hybrid entry (2026-07-16, fixed same day): box/ATR/extension from 15m,
        # trigger from 5m close. First version gated the extra 5m fetch on the live 15m
        # candle's WICK touching the box edge - with VBCB's tight box that fired on
        # almost every symbol, doubling API calls universe-wide and tripping BingX's
        # rate limit -> 20min backoff, re-triggered every cycle -> zero signals all day.
        # Fixed gate: only fetch 5m when live PRICE (cheap ticker call, not a candle
        # series) is already past the actual breakout threshold - a genuinely rare event.
        current_px = get_current_price(symbol)
        if current_px <= 0:
            return
        breakout_up   = box_high + atr_now * FAST_BREAKOUT_ATR_MULT
        breakout_down = box_low  - atr_now * FAST_BREAKOUT_ATR_MULT
        if current_px < breakout_up and current_px > breakout_down:
            return

        c5 = get_candles(symbol, limit=FAST_VOL_LB + 5, interval=FAST_TRIGGER_TIMEFRAME)
        if len(c5) < FAST_VOL_LB + 2:
            return
        conf5 = c5[:-1]           # closed 5m candles only
        trig  = conf5[-1]         # trigger = last CLOSED 5m candle
        entry = cl(trig)
        if entry < MIN_PRICE:
            return

        vols5   = [v(c) for c in conf5[:-1]][-FAST_VOL_LB:]
        avg_vol = sum(vols5) / len(vols5) if vols5 else 0
        ratio   = v(trig) / avg_vol if avg_vol > 0 else 0
        vol_ok  = ratio >= FAST_VOL_MULT

        candle_range = h(trig) - l(trig)
        bull_close_strength = (cl(trig) - l(trig)) / candle_range if candle_range > 0 else 0
        bear_close_strength = (h(trig) - cl(trig)) / candle_range if candle_range > 0 else 0

        bull_breakout = (cl(trig) > box_high + atr_now * FAST_BREAKOUT_ATR_MULT
                         and bull_close_strength >= FAST_CLOSE_POSITION_MIN)
        bear_breakout = (cl(trig) < box_low  - atr_now * FAST_BREAKOUT_ATR_MULT
                         and bear_close_strength >= FAST_CLOSE_POSITION_MIN)

        long_signal  = bull_breakout and vol_ok
        short_signal = bear_breakout and vol_ok

        if not long_signal and not short_signal:
            return

        # ---- VBCB consolidation filter (added 2026-07-16) ----
        # A breakout only counts if it breaks out of a genuinely TIGHT range.
        # Wide box = price was trending, "breakout" is just chasing an extended move.
        box_height = box_high - box_low
        if atr_now > 0 and box_height > atr_now * FAST_BOX_MAX_ATR_MULT:
            print(f"[FAST SKIP] {symbol} box={box_height/atr_now:.1f}x ATR too wide (max {FAST_BOX_MAX_ATR_MULT}x) - not a consolidation, breakout would be chasing")
            return

        # ---- MTF trend filter (added 2026-07-11) - direction-only, no timing delay ----
        if MTF_FILTER_ENABLED and (long_signal or short_signal):
            mtf_trend = get_mtf_trend(symbol)
            if mtf_trend is not None:
                if long_signal and mtf_trend != "UP":
                    print(f"[MTF SKIP] {symbol} long blocked - {MTF_INTERVAL} trend is {mtf_trend}")
                    long_signal = False
                if short_signal and mtf_trend != "DOWN":
                    print(f"[MTF SKIP] {symbol} short blocked - {MTF_INTERVAL} trend is {mtf_trend}")
                    short_signal = False
        if not long_signal and not short_signal:
            return

        if symbol in fast_open_trades:
            current_side = fast_open_trades[symbol]["side"]
            if (current_side == "BUY" and short_signal) or (current_side == "SELL" and long_signal):
                print(f"[FAST CLOSE - OPPOSITE] {symbol}")
                close_fast_position(symbol, "Opposite")
            return

        # An armed retest killed by an opposite-direction signal is a failed breakout -
        # exactly the trade we no longer want. Same-direction re-signal: keep the
        # original (its retest level came from the FIRST breakout, which is the one
        # that defines the structure).
        if symbol in fast_pending_retests:
            pending_side = fast_pending_retests[symbol]["side"]
            if (pending_side == "BUY" and short_signal) or (pending_side == "SELL" and long_signal):
                del fast_pending_retests[symbol]
                print(f"[FAST PENDING CANCEL - OPPOSITE] {symbol}")
                send_tg("FAST RETEST CANCELLED - " + symbol + "\nOpposite signal fired before the " + pending_side + " retest filled - breakout failed, no entry")
            return

        sig_id_long  = (symbol, "BUY",  int(trig["time"]))
        sig_id_short = (symbol, "SELL", int(trig["time"]))

        # ---- Retest limit entry (2026-07-19): the signal no longer enters. It ARMS
        # a pending retest; check_fast_pending() (trailing loop, 30s) does the entry
        # when price pulls back to the retest level. Retest level = the deeper of the
        # box edge and the trigger-candle midpoint, so a barely-broken-out candle
        # retests the box edge while a big extension candle only needs to give back
        # half its range - both realistic pullback targets, both a materially better
        # price than the breakout close we used to buy.
        if long_signal and sig_id_long not in fast_alerted:
            fast_alerted.add(sig_id_long)
            lev        = get_fast_leverage(symbol)
            sl_price   = round(box_low - atr_now * SL_ATR_BUFFER_MULT, 6)
            risk_usdt  = FAST_RISK_USDT * FAST_EXTENSION_MULT if is_extended else FAST_RISK_USDT
            ext_tag    = "extended (half size)" if is_extended else "fresh move"
            if not FAST_RETEST_ENABLED:
                fast_enter_now(symbol, "BUY", entry, sl_price, atr_now, risk_usdt,
                               lev, ratio, bull_close_strength, ext_tag, extension)
                return
            trig_mid   = (h(trig) + l(trig)) / 2
            retest     = round(max(box_high, trig_mid), 6)
            risk       = retest - sl_price
            if risk <= 0:
                return
            tp1_price  = round(retest + risk * FAST_TP1_RR, 6)
            fast_pending_retests[symbol] = {
                "side": "BUY", "retest": retest, "sl": sl_price, "atr": atr_now,
                "risk_usdt": risk_usdt, "lev": lev, "ratio": ratio,
                "close_strength": bull_close_strength, "ext_tag": ext_tag,
                "armed_ts": time.time(), "expiry_ts": time.time() + FAST_RETEST_EXPIRY_SECONDS,
            }
            print(f"[FAST ARMED] {symbol} BUY retest={retest} | vol={ratio:.1f}x | lev={lev}x | {ext_tag} | ext={extension:.1f}x ATR")
            send_tg(
                "FAST SIGNAL - BUY - " + symbol + "\n------------------------------\n"
                "Retest entry: " + str(retest) + " (waiting for pullback) | SL: " + str(sl_price) + "\n"
                "TP1 (" + str(FAST_TP1_RR) + "R): " + str(tp1_price) + " | ATR-trail after " + str(FAST_TRAIL_ACTIVATE_RR) + "R\n"
                "Breakout close: " + str(round(entry, 6)) + " | Breakout vol: " + str(round(ratio, 1)) + "x | Close-strength: " + str(round(bull_close_strength, 2)) +
                " | Lev: " + str(lev) + "x | " + ext_tag + " | Risk: $" + str(risk_usdt) +
                "\nExpires in " + str(FAST_RETEST_EXPIRY_SECONDS // 60) + "min if no pullback"
                "\n------------------------------\nNiti Fast Signal"
            )

        elif short_signal and sig_id_short not in fast_alerted:
            fast_alerted.add(sig_id_short)
            lev        = get_fast_leverage(symbol)
            sl_price   = round(box_high + atr_now * SL_ATR_BUFFER_MULT, 6)
            risk_usdt  = FAST_RISK_USDT * FAST_EXTENSION_MULT if is_extended else FAST_RISK_USDT
            ext_tag    = "extended (half size)" if is_extended else "fresh move"
            if not FAST_RETEST_ENABLED:
                fast_enter_now(symbol, "SELL", entry, sl_price, atr_now, risk_usdt,
                               lev, ratio, bear_close_strength, ext_tag, extension)
                return
            trig_mid   = (h(trig) + l(trig)) / 2
            retest     = round(min(box_low, trig_mid), 6)
            risk       = sl_price - retest
            if risk <= 0:
                return
            tp1_price  = round(retest - risk * FAST_TP1_RR, 6)
            fast_pending_retests[symbol] = {
                "side": "SELL", "retest": retest, "sl": sl_price, "atr": atr_now,
                "risk_usdt": risk_usdt, "lev": lev, "ratio": ratio,
                "close_strength": bear_close_strength, "ext_tag": ext_tag,
                "armed_ts": time.time(), "expiry_ts": time.time() + FAST_RETEST_EXPIRY_SECONDS,
            }
            print(f"[FAST ARMED] {symbol} SELL retest={retest} | vol={ratio:.1f}x | lev={lev}x | {ext_tag} | ext={extension:.1f}x ATR")
            send_tg(
                "FAST SIGNAL - SELL - " + symbol + "\n------------------------------\n"
                "Retest entry: " + str(retest) + " (waiting for pullback) | SL: " + str(sl_price) + "\n"
                "TP1 (" + str(FAST_TP1_RR) + "R): " + str(tp1_price) + " | ATR-trail after " + str(FAST_TRAIL_ACTIVATE_RR) + "R\n"
                "Breakout close: " + str(round(entry, 6)) + " | Breakout vol: " + str(round(ratio, 1)) + "x | Close-strength: " + str(round(bear_close_strength, 2)) +
                " | Lev: " + str(lev) + "x | " + ext_tag + " | Risk: $" + str(risk_usdt) +
                "\nExpires in " + str(FAST_RETEST_EXPIRY_SECONDS // 60) + "min if no pullback"
                "\n------------------------------\nNiti Fast Signal"
            )

    except Exception as e:
        print(f"[FAST {symbol}] error: {e}")


# ==================== TIGHT ENTRY LOGIC (Stock Niti: spike -> cooldown -> breakout) ====================
def get_tight_volume_baseline(symbol):
    """Cached 3-day average 1m volume per symbol, refreshed every
    TIGHT_BASELINE_REFRESH_SECONDS rather than on every scan (expensive call).

    Paginated (fixed 2026-07-11): BingX's kline endpoint caps `limit` at 1440 per
    request (confirmed via a live 109400 error - the original single-call
    limit=4320 request was failing on every symbol, every cycle, which is why the
    baseline was always None and Tight never watched anything). This now fetches
    TIGHT_BASELINE_CANDLES worth of history in <=1440-candle chunks, walking
    backwards with `endTime`, with a small sleep between chunks to avoid bursting."""
    now = time.time()
    cached = tight_baseline_cache.get(symbol)
    if cached and now - cached["ts"] < TIGHT_BASELINE_REFRESH_SECONDS:
        return cached["baseline"]
    try:
        all_vols   = []
        end_time   = None
        remaining  = TIGHT_BASELINE_CANDLES
        first_chunk = True
        while remaining > 0:
            chunk_limit = min(remaining, BINGX_KLINE_MAX_LIMIT)
            candles = get_candles(symbol, limit=chunk_limit, interval=TIGHT_TIMEFRAME, end_time=end_time)
            if not candles:
                break
            # exclude the live in-progress candle - only present in the first
            # (most recent, end_time=None) chunk
            chunk = candles[:-1] if first_chunk else candles
            all_vols.extend(v(c) for c in chunk)
            end_time = int(candles[0]["time"]) - 1
            remaining -= len(candles)
            first_chunk = False
            if len(candles) < chunk_limit:
                break   # exchange returned fewer than asked - no more history available
            time.sleep(0.3)
        if len(all_vols) < 100:
            return cached["baseline"] if cached else None
        baseline = sum(all_vols) / len(all_vols)
        tight_baseline_cache[symbol] = {"baseline": baseline, "ts": now}
        return baseline
    except Exception as e:
        print(f"[TIGHT BASELINE ERROR] {symbol}: {e}")
        return cached["baseline"] if cached else None


def place_tight_order(symbol, side, entry, sl, tp):
    try:
        set_leverage_api(symbol, TIGHT_LEVERAGE)
        precision = symbol_precision.get(symbol, 4)
        risk_dist = abs(entry - sl)
        if risk_dist <= 0:
            return None
        # Risk-based sizing: qty is set so that hitting SL always loses ~TIGHT_RISK_USDT,
        # regardless of how tight or wide the SL distance is for this particular setup.
        risk_qty       = TIGHT_RISK_USDT / risk_dist
        margin_cap_qty = (TIGHT_MAX_MARGIN_USDT * TIGHT_LEVERAGE) / entry
        total_qty = round(min(risk_qty, margin_cap_qty), precision)
        if total_qty <= 0:
            return None
        if risk_qty > margin_cap_qty:
            print(f"[TIGHT SIZE CAP] {symbol} SL too tight for full risk qty, margin-capped at ${TIGHT_MAX_MARGIN_USDT}")
        pos_side   = "LONG" if side == "BUY" else "SHORT"
        close_side = "SELL" if side == "BUY" else "BUY"

        half_qty = round(total_qty / 2, precision)

        # Pre-trade margin check: skip cleanly instead of firing an order BingX will
        # reject with 101204. None (endpoint failure) = unknown -> proceed.
        required_margin = total_qty * entry / TIGHT_LEVERAGE
        avail = get_available_margin()
        if avail is not None and avail < required_margin * 1.05:
            print(f"[TIGHT MARGIN SKIP] {symbol} need ~${required_margin:.2f}, available ${avail:.2f} - skipping")
            return "MARGIN_SKIP"

        order_id = place_market_order(symbol, side, total_qty, pos_side)
        print(f"[TIGHT ORDER] {symbol} {side} qty={total_qty} risk=${TIGHT_RISK_USDT}: {order_id}")
        if order_id != "N/A":
            time.sleep(0.5)
            entry_fill = get_fill_price(order_id, symbol, fallback=entry)

            # Recompute risk distance and BOTH take-profit levels from the REAL fill
            # price, not the nominal signal price. Otherwise slippage compresses the
            # actual R-multiples: "TP1 at 2R" measured from a worse real entry can be
            # only ~0.5-1R away in reality (confirmed 2026-07-15: Trail wins of
            # +$0.68-$2 against -$5-6 SLs). SL itself stays structural (unchanged).
            risk_dist = abs(entry_fill - sl)
            if risk_dist <= 0:
                risk_dist = abs(entry - sl)
            if side == "BUY":
                tp1_price = round(entry_fill + risk_dist * TIGHT_BE_TRIGGER_R, 6)   # price precision, NOT qty precision (2026-07-17)
                tp        = round(entry_fill + risk_dist * TIGHT_RR_TP, 6)
            else:
                tp1_price = round(entry_fill - risk_dist * TIGHT_BE_TRIGGER_R, 6)
                tp        = round(entry_fill - risk_dist * TIGHT_RR_TP, 6)

            # SL first, guarded: if it can't be placed even on retry, close the
            # position immediately instead of leaving it naked.
            sl_id = place_sl_guarded(symbol, close_side, pos_side, sl, total_qty)
            if sl_id is None:
                print(f"[TIGHT SL GUARD] {symbol} SL placement failed twice - emergency closing position")
                place_market_order(symbol, close_side, total_qty, pos_side)
                send_tg(f"⚠️ TIGHT {symbol}: SL placement failed - position emergency-closed for safety")
                return None
            tp1_id = place_tp_guarded(symbol, close_side, pos_side, tp1_price, half_qty, label="TP1")
            tp_id  = place_tp_guarded(symbol, close_side, pos_side, tp, total_qty - half_qty, label="TP-final")
            tight_open_trades[str(order_id)] = {
                "symbol": symbol, "side": side, "entry": entry, "entry_fill": entry_fill, "sl": sl, "tp": tp,
                "total_qty": total_qty, "half_qty": half_qty, "remaining_qty": total_qty - half_qty,
                "tp1": tp1_price, "tp1_id": tp1_id, "tp1_filled": False,
                "sl_id": sl_id, "tp_id": tp_id,
                "close_side": close_side, "pos_side": pos_side,
                "risk_dist": risk_dist, "be_done": False,
                "trail_price": None, "be_price": None,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
        return order_id
    except Exception as e:
        print(f"[TIGHT ORDER ERROR] {symbol}: {e}")
        return None


# (dead code removed 2026-07-17: the original check_tight_symbol lived here;
# it was shadowed by the cache-based rewrite further down and never executed)


def track_tight_trades():
    for oid in list(tight_open_trades.keys()):
        trade = tight_open_trades.get(oid)
        if not trade:
            continue
        try:
            symbol    = trade["symbol"]
            risk_dist = trade.get("risk_dist", 0)

            # ---- TP1 (half) filled at 2R: book real profit, move remaining half to
            # breakeven, then start trailing its SL toward the final 4R target ----
            if not trade.get("tp1_filled") and trade.get("tp1_id"):
                status = check_order_status(trade["tp1_id"], symbol)
                if status == "FILLED":
                    trade["tp1_filled"] = True
                    entry_ref = trade.get("entry_fill", trade["entry"])
                    tp1_fill  = get_fill_price(trade["tp1_id"], symbol, fallback=trade["tp1"])
                    trade["tp1_fill"] = tp1_fill
                    leg_pnl = (tp1_fill - entry_ref) * trade["half_qty"] if trade["side"] == "BUY" else (entry_ref - tp1_fill) * trade["half_qty"]
                    trade["tp1_pnl"] = round(leg_pnl, 2)
                    # ---- Post-TP1 stop placement (BUGFIX 2026-07-23) ----
                    # OLD behaviour: SL -> breakeven, and trail_price seeded at ENTRY.
                    # The trail only advanced on a NEW high above trail_price, and the
                    # 30s poll almost always detects the TP1 fill AFTER price has already
                    # slipped back below 2R. Result: the back half's stop got parked a few
                    # ticks above BE and never ratcheted again, so essentially every Tight
                    # winner banked TP1 only (~+1R on half = ~$2) and handed the rest back.
                    # That, not the signal quality, is why wins were capped near $2 while
                    # losses ran a full -1R.
                    # NEW behaviour: anchor the trail at the KNOWN TP1 fill (a genuine 2R)
                    # instead of at entry, and lock the back half at +1R whenever price is
                    # still on the right side of that level. If price has already reversed
                    # below +1R by the time we poll, fall back to plain breakeven exactly
                    # as before - the fix never places a stop on the wrong side of market.
                    lock_price = round(
                        entry_ref + risk_dist * TIGHT_TRAIL_R_MULT if trade["side"] == "BUY"
                        else entry_ref - risk_dist * TIGHT_TRAIL_R_MULT, 6
                    )
                    current_now = get_current_price(symbol)
                    lock_ok = current_now > 0 and (
                        (trade["side"] == "BUY"  and current_now > lock_price) or
                        (trade["side"] == "SELL" and current_now < lock_price)
                    )
                    protect_price = lock_price if lock_ok else entry_ref
                    # Guarded re-placement, new-first order (2026-07-17) - see Fast BE note
                    new_sl_id = place_sl_guarded(symbol, trade["close_side"], trade["pos_side"], protect_price, trade["remaining_qty"])
                    if new_sl_id:
                        if trade.get("sl_id"):
                            cancel_order(symbol, trade["sl_id"])
                        trade["sl_id"]      = new_sl_id
                        trade["sl"]         = protect_price
                        trade["be_price"]   = entry_ref
                    else:
                        print(f"[TIGHT BE FAIL] {symbol} protective SL re-placement failed twice - keeping original SL {trade['sl']}")
                        if not trade.get("sl_move_alerted"):
                            trade["sl_move_alerted"] = True
                            send_tg(f"⚠️ TIGHT {symbol}: could not move SL to {protect_price} - original SL {trade['sl']} still active. Consider moving it manually.")
                    trade["be_done"]    = True
                    # Anchor the trail at the real 2R fill, NOT at entry (this is the fix).
                    trade["trail_price"] = tp1_fill
                    lock_tag = f"locked +{TIGHT_TRAIL_R_MULT}R ({protect_price})" if lock_ok else f"BE ({entry_ref}) - price already back below +{TIGHT_TRAIL_R_MULT}R"
                    print(f"[TIGHT TP1] {symbol} - half closed at {tp1_fill}, remaining {trade['remaining_qty']} -> {lock_tag}, trail anchored at {tp1_fill}")

            # ---- Trail the remaining half's SL once TP1 is banked ----
            if trade.get("tp1_filled") and risk_dist > 0:
                current = get_current_price(symbol)
                if current > 0:
                    # BUGFIX 2026-07-23 (part 2): the new_sl computation used to sit INSIDE
                    # the "new extreme" branch, so once the extreme stopped advancing the
                    # stop stopped ratcheting even when it was still behind where it should
                    # be. It is now recomputed on every pass from the current trail anchor.
                    # Also: rounding used symbol_precision (QUANTITY precision) on a PRICE -
                    # the same class of bug fixed for TP on 2026-07-17. On a sub-cent coin
                    # that rounds the stop to 0.0 and BingX silently rejects it, which is
                    # how back halves ended up running with no working trail at all.
                    trail_dist = risk_dist * TIGHT_TRAIL_R_MULT
                    if trade["side"] == "BUY":
                        if current > trade["trail_price"]:
                            trade["trail_price"] = current
                        new_sl = round(trade["trail_price"] - trail_dist, 6)
                        # only ratchet UP, and never place a sell-stop above market
                        if new_sl > trade["sl"] and new_sl < current:
                            new_id = place_sl_guarded(symbol, trade["close_side"], trade["pos_side"], new_sl, trade["remaining_qty"])
                            if new_id:
                                if trade.get("sl_id"):
                                    cancel_order(symbol, trade["sl_id"])
                                trade["sl_id"] = new_id
                                trade["sl"] = new_sl
                                print(f"[TIGHT TRAIL] {symbol} peak {trade['trail_price']} -> SL {new_sl}")
                            else:
                                print(f"[TIGHT TRAIL FAIL] {symbol} trail SL re-placement failed - keeping SL {trade['sl']}")
                    else:
                        if current < trade["trail_price"]:
                            trade["trail_price"] = current
                        new_sl = round(trade["trail_price"] + trail_dist, 6)
                        # only ratchet DOWN, and never place a buy-stop below market
                        if new_sl < trade["sl"] and new_sl > current:
                            new_id = place_sl_guarded(symbol, trade["close_side"], trade["pos_side"], new_sl, trade["remaining_qty"])
                            if new_id:
                                if trade.get("sl_id"):
                                    cancel_order(symbol, trade["sl_id"])
                                trade["sl_id"] = new_id
                                trade["sl"] = new_sl
                                print(f"[TIGHT TRAIL] {symbol} peak {trade['trail_price']} -> SL {new_sl}")
                            else:
                                print(f"[TIGHT TRAIL FAIL] {symbol} trail SL re-placement failed - keeping SL {trade['sl']}")

            sl_status = check_order_status(trade["sl_id"], symbol) if trade.get("sl_id") else ""
            if sl_status == "FILLED":
                if trade.get("tp_id"):
                    cancel_order(symbol, trade["tp_id"])
                entry_ref = trade.get("entry_fill", trade["entry"])
                sl_fill   = get_fill_price(trade["sl_id"], symbol, fallback=trade["sl"])
                if trade.get("tp1_filled"):
                    result   = "Trail" if trade["sl"] != trade.get("be_price", trade["entry"]) else "BE"
                    rem_qty  = trade["remaining_qty"]
                    leg_pnl  = (sl_fill - entry_ref) * rem_qty if trade["side"] == "BUY" else (entry_ref - sl_fill) * rem_qty
                    trade["pnl"] = round(trade.get("tp1_pnl", 0) + leg_pnl, 2)
                else:
                    result = "SL"
                    leg_pnl = (sl_fill - entry_ref) * trade["total_qty"] if trade["side"] == "BUY" else (entry_ref - sl_fill) * trade["total_qty"]
                    trade["pnl"] = round(leg_pnl, 2)
                trade["result"] = result
                trade["label"]  = "Tight"
                daily_trades.append(trade)
                journal_closed_trade(trade)
                tight_open_trades.pop(oid, None)
                continue

            tp_status = check_order_status(trade["tp_id"], symbol) if trade.get("tp_id") else ""
            if tp_status == "FILLED":
                if trade.get("sl_id"):
                    cancel_order(symbol, trade["sl_id"])
                entry_ref = trade.get("entry_fill", trade["entry"])
                tp_fill   = get_fill_price(trade["tp_id"], symbol, fallback=trade["tp"])
                rem_qty = trade["remaining_qty"] if trade.get("tp1_filled") else trade["total_qty"]
                leg_pnl = (tp_fill - entry_ref) * rem_qty if trade["side"] == "BUY" else (entry_ref - tp_fill) * rem_qty
                trade["pnl"]    = round(trade.get("tp1_pnl", 0) + leg_pnl, 2)
                trade["result"] = "TP"
                trade["label"]  = "Tight"
                daily_trades.append(trade)
                journal_closed_trade(trade)
                tight_open_trades.pop(oid, None)
                continue
        except Exception as e:
            print(f"[TIGHT TRACK ERROR] {oid}: {e}")


def send_daily_summary():
    global daily_trades, last_summary_date
    nzt   = timezone(timedelta(hours=12))
    now   = datetime.now(nzt)
    today = now.date()
    if last_summary_date == today:
        return
    if now.hour != 23 or now.minute < 55:
        return
    last_summary_date = today
    total_pnl = round(sum(t.get("pnl", 0) for t in daily_trades), 2)
    wins      = sum(1 for t in daily_trades if t.get("pnl", 0) > 0)
    losses    = sum(1 for t in daily_trades if t.get("pnl", 0) <= 0)
    total     = len(daily_trades)
    win_rate  = round(wins / total * 100, 1) if total > 0 else 0
    sign      = "+" if total_pnl > 0 else ""
    lines = ["Daily Summary - " + today.strftime("%b %d, %Y"), "------------------------------"]
    for idx, t in enumerate(daily_trades, 1):
        p  = t.get("pnl", 0)
        ps = "+" if p > 0 else ""
        lines.append(str(idx) + ". [" + t.get("label", "?") + "] " + t["symbol"] + " " + t["side"] + " | " + t.get("result", "?") + " | " + ps + str(p) + " USDT")
    lines.append("------------------------------")
    lines.append("Trades: " + str(total) + " (" + str(wins) + "W/" + str(losses) + "L) | WR: " + str(win_rate) + "%")
    lines.append("Total PnL: " + sign + str(total_pnl) + " USDT")
    lines.append("------------------------------\nNiti Journal")
    send_journal("\n".join(lines))
    daily_trades = []


# ==================== TELEGRAM COMMANDS ====================
def handle_telegram_commands():
    global tight_auto_trade_enabled, fast_auto_trade_enabled, t3_auto_trade_enabled
    offset = None
    # On startup, discard any backlog of old pending updates (e.g. a /start sent
    # before a previous crash/redeploy) so they don't get silently replayed and
    # flip auto-trade ON without a fresh command from Faisal.
    try:
        flush = requests.get(f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates", params={"timeout": 0}, timeout=10).json()
        pending = flush.get("result", [])
        if pending:
            offset = pending[-1]["update_id"] + 1
            requests.get(f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates", params={"offset": offset, "timeout": 0}, timeout=10)
            print(f"[TG CMD] Flushed {len(pending)} stale pending update(s) on startup")
    except Exception as e:
        print(f"[TG CMD] startup flush error: {e}")
    while True:
        try:
            url    = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
            params = {"timeout": 30}
            if offset:
                params["offset"] = offset
            r = requests.get(url, params=params, timeout=35).json()
            for update in r.get("result", []):
                offset  = update["update_id"] + 1
                msg     = update.get("message", {})
                text    = msg.get("text", "").strip().lower()
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != str(TG_CHAT_ID):
                    continue
                if text == "/start":
                    tight_auto_trade_enabled = True
                    send_tg("Tight Auto-trade ON.")
                elif text == "/stop":
                    tight_auto_trade_enabled = False
                    send_tg("Tight Auto-trade OFF.")
                elif text == "/status":
                    t = "ON" if tight_auto_trade_enabled else "OFF"
                    f = "ON" if fast_auto_trade_enabled  else "OFF"
                    backoff = ""
                    if api_backoff_active():
                        backoff = f"\nAPI BACKOFF ACTIVE - {int(_api_backoff_until - time.time())}s remaining"
                    send_tg("Tight: " + t + " | Watching: " + str(len(tight_watch)) + " | Open: " + str(len(tight_open_trades)) +
                            "\nFast Signal: " + f + " | Open: " + str(len(fast_open_trades)) +
                            " | Pending retests: " + str(len(fast_pending_retests)) + backoff)
                elif text == "/fast_start":
                    fast_auto_trade_enabled = True
                    send_tg("Fast Signal Auto-trade ON.")
                elif text == "/fast_stop":
                    fast_auto_trade_enabled = False
                    send_tg("Fast Signal Auto-trade OFF.")
                elif text == "/fast_status":
                    f = "ON" if fast_auto_trade_enabled else "OFF"
                    pend = ""
                    for s, p in list(fast_pending_retests.items()):
                        mins_left = max(0, int((p["expiry_ts"] - time.time()) / 60))
                        pend += "\n" + s + " " + p["side"] + " retest " + str(p["retest"]) + " (" + str(mins_left) + "min left)"
                    send_tg("Fast Signal: " + f + " | Open: " + str(len(fast_open_trades)) +
                            " | Pending retests: " + str(len(fast_pending_retests)) + pend)
                elif text == "/t3_start":
                    t3_auto_trade_enabled = True
                    send_tg("Tight 3 Auto-trade ON.")
                elif text == "/t3_stop":
                    t3_auto_trade_enabled = False
                    send_tg("Tight 3 Auto-trade OFF.")
                elif text == "/t3_status":
                    s3 = "ON" if t3_auto_trade_enabled else "OFF"
                    lines3 = ("Tight 3: " + s3 + " | Watchlist: " + str(len(t3_watchlist)) +
                              " | Awakened: " + str(len(t3_watch)) + " | Open: " + str(len(t3_open_trades)))
                    for s, st3 in list(t3_watch.items()):
                        hrs = max(0, int((st3["expiry_ts"] - time.time()) / 3600))
                        stage = ("setup ready, trigger " + str(round(st3["trigger"], 6))) if st3.get("trigger") is not None else "waiting for pullback"
                        lines3 += "\n" + s + " " + st3["side"] + " - " + stage + " (" + str(hrs) + "h left)"
                    for _oid3, t3t in list(t3_open_trades.items()):
                        lines3 += ("\n" + t3t["symbol"] + " " + t3t["side"] + " open | peak " +
                                   str(round(t3t.get("peak_r", 0), 1)) + "R | SL " + str(t3t["sl"]))
                    send_tg(lines3)
        except Exception as e:
            print(f"[TG CMD] error: {e}")
        time.sleep(1)


def check_fast_pending():
    """Retest fill monitor (2026-07-19). Polls price every trailing-loop pass (30s)
    for each armed retest. Implementation choice: monitor + market-order-on-touch,
    NOT a real exchange limit order. Reasons: (a) cancel_order is still unverified
    against the live BingX API (see its docstring) - an uncancellable stale limit
    order is worse than 30s of fill granularity on a 15m strategy, (b) qty/leverage
    get computed at FILL time by place_fast_order exactly as before, no new sizing
    path. Known trade-off: a pullback that wicks through the retest and bounces
    entirely inside one 30s gap is missed. Acceptable at this timeframe.

    Outcomes per armed retest:
      1. price reaches SL zone first  -> breakout failed, NO entry (the old losing
         trade, now skipped - this branch firing a lot is the fix working)
      2. price pulls back to retest   -> enter there (better price, smaller SL
         distance, real pullback absorbed before entry)
      3. neither within expiry window -> skip trade
    """
    now_ts = time.time()
    for symbol in list(fast_pending_retests.keys()):
        try:
            p        = fast_pending_retests[symbol]
            side     = p["side"]
            retest   = p["retest"]
            sl_price = p["sl"]

            if now_ts >= p["expiry_ts"]:
                del fast_pending_retests[symbol]
                print(f"[FAST PENDING EXPIRED] {symbol} {side} no pullback to {retest}")
                send_tg("FAST RETEST EXPIRED - " + side + " - " + symbol +
                        "\nNo pullback to " + str(retest) + " within " + str(FAST_RETEST_EXPIRY_SECONDS // 60) + "min - trade skipped")
                continue

            px = get_current_price(symbol)
            if px <= 0:
                continue

            invalidated = (px <= sl_price) if side == "BUY" else (px >= sl_price)
            if invalidated:
                del fast_pending_retests[symbol]
                print(f"[FAST PENDING INVALIDATED] {symbol} {side} price {px} through SL {sl_price} before fill")
                send_tg("FAST RETEST INVALIDATED - " + side + " - " + symbol +
                        "\nPrice hit the SL zone (" + str(sl_price) + ") before the retest at " + str(retest) +
                        " filled - failed breakout, no entry taken (this would have been a full loss under the old entry)")
                continue

            touched = (px <= retest) if side == "BUY" else (px >= retest)
            if not touched:
                continue

            del fast_pending_retests[symbol]
            if symbol in fast_open_trades:
                continue
            risk_dist = abs(retest - sl_price)
            if risk_dist <= 0:
                continue
            if side == "BUY":
                tp1_price = round(retest + risk_dist * FAST_TP1_RR, 6)
            else:
                tp1_price = round(retest - risk_dist * FAST_TP1_RR, 6)

            print(f"[FAST RETEST FILL] {symbol} {side} @ {px} (retest {retest})")
            trade_status = ""
            if fast_auto_trade_enabled and len(fast_open_trades) >= FAST_MAX_CONCURRENT_TRADES:
                trade_status = "\nSkipped - max concurrent trades (" + str(FAST_MAX_CONCURRENT_TRADES) + ") reached"
            elif fast_auto_trade_enabled:
                oid = place_fast_order(symbol, side, retest, sl_price, tp1_price, p["atr"], p["risk_usdt"])
                if oid == "MARGIN_SKIP":
                    trade_status = "\nSkipped - insufficient margin"
                elif oid and oid != "N/A":
                    trade_status = "\nOrder: " + str(oid)
                else:
                    trade_status = "\nOrder failed"
            else:
                trade_status = "\nAuto-trade OFF"
            send_tg(
                "FAST RETEST FILLED - " + side + " - " + symbol + "\n------------------------------\n"
                "Entry: " + str(retest) + " (pullback fill @ " + str(round(px, 6)) + ") | SL: " + str(sl_price) + "\n"
                "TP1 (" + str(FAST_TP1_RR) + "R): " + str(tp1_price) + " | ATR-trail after " + str(FAST_TRAIL_ACTIVATE_RR) + "R\n"
                "Breakout vol: " + str(round(p["ratio"], 1)) + "x | Close-strength: " + str(round(p["close_strength"], 2)) +
                " | Lev: " + str(p["lev"]) + "x | " + p["ext_tag"] + " | Risk: $" + str(p["risk_usdt"]) +
                trade_status + "\n------------------------------\nNiti Fast Signal"
            )
        except Exception as e:
            print(f"[FAST PENDING {symbol}] error: {e}")


def trailing_loop():
    while True:
        try:
            if fast_pending_retests:
                check_fast_pending()
            if fast_open_trades:
                track_fast_trades()
                update_fast_trailing()
            if tight_open_trades:
                track_tight_trades()
            if t3_open_trades:
                track_t3_trades()
        except Exception as e:
            print(f"[TRAIL LOOP ERROR] {e}")
        time.sleep(30)


def fast_diagnostic_check():
    """Prints a sample calculation for a known-liquid coin every cycle, so we can see
    in the logs whether the data pipeline (candles -> ratios) is actually alive,
    independent of whether a real signal happens to fire."""
    try:
        candles = get_candles("BTC-USDT", limit=100, interval=FAST_TIMEFRAME)
        if len(candles) < 60:
            print(f"[FAST DIAG] BTC-USDT - only got {len(candles)} candles (need ~60+)")
            return
        confirmed = candles[:-1]
        closes = [cl(c) for c in confirmed]
        highs  = [h(c) for c in confirmed]
        lows   = [l(c) for c in confirmed]
        vols   = [v(c) for c in confirmed]
        i = len(confirmed) - 1
        avg_vol = sum(vols[i - FAST_VOL_LB:i]) / FAST_VOL_LB
        ratio = vols[i] / avg_vol if avg_vol > 0 else 0
        box_high = max(highs[i - FAST_CONSOL_LOOKBACK:i])
        box_low  = min(lows[i - FAST_CONSOL_LOOKBACK:i])
        print(f"[FAST DIAG] BTC-USDT close={closes[i]} vol_ratio={ratio:.2f}x box=({box_low},{box_high}) candles={len(candles)}")
    except Exception as e:
        print(f"[FAST DIAG ERROR] {e}")


def fast_scan_loop():
    print(f"Fast Signal loop started - {FAST_TIMEFRAME} | consolidation breakout | MTF filter + reworked time-exit (2026-07-11)")
    all_symbols = []
    while True:
        try:
            if not all_symbols:
                all_symbols = get_futures_symbols() or []
            liquid = get_liquid_symbols(
                all_symbols, min_quote_vol=FAST_MIN_QUOTE_VOL, max_n=FAST_MAX_SYMBOLS,
                exclude_top_n=FAST_EXCLUDE_TOP_N, rank_by_gain=FAST_GAINER_TOP_N
            )
            # NCS tokenized stocks are ALWAYS excluded on weekends (UTC Sat/Sun): the
            # underlying stock market is closed, books go dead, and stale/thin prints
            # fake "breakouts" - both Jul 19 NCSKSKHYP2USD SELL signals were weekend
            # ghosts. Weekday inclusion still controlled by EXCLUDE_XSTOCKS_FAST env.
            if EXCLUDE_XSTOCKS_FAST or datetime.now(timezone.utc).weekday() >= 5:
                liquid = [s for s in liquid if not s.startswith("NCS")]
            mode = "retest-wait" if FAST_RETEST_ENABLED else "immediate breakout entry"
            rank = f"top {FAST_GAINER_TOP_N} daily gainers" if FAST_GAINER_TOP_N > 0 else "volume-ordered"
            print(f"[FAST SCAN] Scanning {len(liquid)} pairs ({rank}, big caps excluded) | entry mode: {mode}")
            fast_diagnostic_check()
            for sym in liquid:
                check_fast(sym)
                time.sleep(0.15)
            print(f"[FAST SCAN] Done. Sleeping {FAST_SCAN_INTERVAL_SECONDS}s...")
        except Exception as e:
            print(f"[FAST LOOP ERROR] {e}")
        time.sleep(FAST_SCAN_INTERVAL_SECONDS)


# (dead code removed 2026-07-17: shadowed originals of tight_diagnostic_check and
# tight_scan_loop; the "SPEED FIX" versions below are the ones that actually run)


@app.route("/")
def health():
    return "Niti Tight (Stock Niti: Vol-Spike+Cooldown+Breakout, 1:4RR) + Fast Signal (Breakout+ATRTrail+ReworkedTimeExit+MTF) + Tight 3 (DormantAwakening: 7xMedian+PullbackEntry+Peak-2R-Trail)", 200




# ==================== COMBINED PATCH: TIGHT 2 SPEED FIX + FAST RISK FIX (2026-07-11) ====================
# Everything below OVERRIDES the earlier definitions (Python uses the last def).
# Fixes: (1) baseline fetch moved to a background thread so a scan pass drops
# from 15-20 min to ~90s, (2) spike detection also checks the last 3 CLOSED
# candles so spikes finishing between passes aren't missed, (3) cooldown now
# catches up on ALL closed candles since the last pass, (4) breakout check does
# the same. Strategy rules (20x spike / 5x re-entry / 1:4 / BE at 2R) unchanged.

# ---- FAST SIGNAL RISK FIX: risk per trade $1.5 -> $5 (2026-07-11) ----
# Position size = risk / SL-distance, so margins were tiny ($1.4-1.6) when SL
# was wide. $5 risk => ~3.3x bigger positions. "extended (half size)" trades
# will now risk $2.5. Overridden by the FAST_RISK_USDT env var if set on Render.
# (2026-07-18: removed a stray late FAST_RISK_USDT redefinition that silently overrode the top config)

TIGHT_SPIKE_CONFIRMED_LOOKBACK = int(os.environ.get("TIGHT_SPIKE_CONFIRMED_LOOKBACK", 3))
TIGHT_SCAN_SYMBOL_GAP          = float(os.environ.get("TIGHT_SCAN_SYMBOL_GAP", 0.1))
TIGHT_BASELINE_SYMBOL_GAP      = float(os.environ.get("TIGHT_BASELINE_SYMBOL_GAP", 0.5))

tight_symbols_current = []   # current scan universe, shared with the baseline refresher thread


def fetch_tight_baseline(symbol):
    """Fetch + cache the 3-day average 1m volume for ONE symbol. Called ONLY
    from the background refresher thread, never inline from the scan loop."""
    cached = tight_baseline_cache.get(symbol)
    try:
        all_vols    = []
        end_time    = None
        remaining   = TIGHT_BASELINE_CANDLES
        first_chunk = True
        while remaining > 0:
            chunk_limit = min(remaining, BINGX_KLINE_MAX_LIMIT)
            candles = get_candles(symbol, limit=chunk_limit, interval=TIGHT_TIMEFRAME, end_time=end_time)
            if not candles:
                break
            chunk = candles[:-1] if first_chunk else candles   # drop live in-progress candle
            all_vols.extend(v(c) for c in chunk)
            end_time = int(candles[0]["time"]) - 1
            remaining -= len(candles)
            first_chunk = False
            if len(candles) < chunk_limit:
                break
            time.sleep(0.3)
        if len(all_vols) < 100:
            return cached["baseline"] if cached else None
        baseline = sum(all_vols) / len(all_vols)
        tight_baseline_cache[symbol] = {"baseline": baseline, "ts": time.time()}
        return baseline
    except Exception as e:
        print(f"[TIGHT BASELINE ERROR] {symbol}: {e}")
        return cached["baseline"] if cached else None


def get_cached_baseline(symbol):
    """Cache-only lookup for the scan loop - NEVER triggers a fetch."""
    cached = tight_baseline_cache.get(symbol)
    return cached["baseline"] if cached else None


def tight_baseline_loop():
    """Background thread: keeps every symbol's 3-day baseline fresh so the scan
    loop never blocks. First full population of ~400 symbols takes ~10-15 min
    (watch Baseline=X/Y in the [TIGHT SCAN] log line)."""
    print("Tight baseline refresher started (background thread)")
    while True:
        try:
            symbols = list(tight_symbols_current)
            if not symbols:
                time.sleep(5)
                continue
            now = time.time()
            stale = [s for s in symbols
                     if s not in tight_baseline_cache
                     or now - tight_baseline_cache[s]["ts"] >= TIGHT_BASELINE_REFRESH_SECONDS]
            if not stale:
                time.sleep(10)
                continue
            for sym in stale:
                if api_backoff_active():
                    time.sleep(30)
                    break
                fetch_tight_baseline(sym)
                time.sleep(TIGHT_BASELINE_SYMBOL_GAP)
        except Exception as e:
            print(f"[TIGHT BASELINE LOOP ERROR] {e}")
            time.sleep(10)


def check_tight_symbol(symbol):
    """OVERRIDES the earlier version. Returns True if it hit the API, False if
    skipped (no baseline yet) so the scan loop can skip the pacing sleep."""
    try:
        baseline = get_cached_baseline(symbol)
        if not baseline or baseline <= 0:
            return False

        candles = get_candles(symbol, limit=30, interval=TIGHT_TIMEFRAME)
        if len(candles) < 20:
            return True
        live_candle = candles[-1]
        confirmed   = candles[:-1]

        live_ratio = v(live_candle) / baseline
        state = tight_watch.get(symbol)

        # ---- Spike trigger: live candle OR last few closed candles ----
        if state is None:
            spike_candle = None
            spike_ratio  = 0.0
            if live_ratio >= TIGHT_SPIKE_VOL_MULT:
                spike_candle, spike_ratio = live_candle, live_ratio
            else:
                for c in confirmed[-TIGHT_SPIKE_CONFIRMED_LOOKBACK:]:
                    r_c = v(c) / baseline
                    if r_c >= TIGHT_SPIKE_VOL_MULT and r_c > spike_ratio:
                        spike_candle, spike_ratio = c, r_c
            if spike_candle is not None:
                direction = "UP" if cl(spike_candle) > o(spike_candle) else "DOWN"
                tight_watch[symbol] = {
                    "state": "COOLDOWN", "direction": direction,
                    "spike_ts": time.time(),
                    "cooldown_highs": [], "cooldown_lows": [],
                    "last_processed_time": int(spike_candle["time"]),
                }
                print(f"[TIGHT SPIKE] {symbol} [{direction}] {round(spike_ratio,1)}x - watching")
            return True

        if time.time() - state["spike_ts"] > TIGHT_MAX_COOLDOWN_WAIT_SECONDS:
            tight_watch.pop(symbol, None)
            return True

        if state["state"] == "COOLDOWN":
            # Catch up on EVERY closed candle since the last pass
            last_t = int(state.get("last_processed_time") or 0)
            for c in [x for x in confirmed if int(x["time"]) > last_t]:
                state["last_processed_time"] = int(c["time"])
                if v(c) / baseline < TIGHT_COOLDOWN_VOL_RATIO:
                    state["cooldown_highs"].append(h(c))
                    state["cooldown_lows"].append(l(c))
                else:
                    state["cooldown_highs"] = []
                    state["cooldown_lows"]  = []
                    continue

                if len(state["cooldown_highs"]) >= TIGHT_COOLDOWN_MIN_CANDLES:
                    range_high = max(state["cooldown_highs"][-TIGHT_COOLDOWN_MIN_CANDLES:])
                    range_low  = min(state["cooldown_lows"][-TIGHT_COOLDOWN_MIN_CANDLES:])
                    closes_r = [cl(x) for x in confirmed[-30:]]
                    highs_r  = [h(x)  for x in confirmed[-30:]]
                    lows_r   = [l(x)  for x in confirmed[-30:]]
                    atr_vals = atr_series(highs_r, lows_r, closes_r, TIGHT_ATR_LEN)
                    atr_now  = atr_vals[-1] if atr_vals else (range_high - range_low)
                    if atr_now > 0 and (range_high - range_low) <= atr_now * TIGHT_RANGE_MAX_ATR_MULT:
                        state["state"]      = "READY"
                        state["range_high"] = range_high
                        state["range_low"]  = range_low
                        state["atr_now"]    = atr_now
                        break

        # plain `if` so COOLDOWN -> READY in this same pass checks breakout immediately
        if state.get("state") == "READY":
            range_high = state["range_high"]
            range_low  = state["range_low"]
            last_t = int(state.get("last_processed_time") or 0)
            candidates = [c for c in confirmed if int(c["time"]) > last_t] + [live_candle]

            breakout_up = breakout_down = False
            trigger_ratio = 0.0
            for c in candidates:
                if c is not live_candle:
                    state["last_processed_time"] = int(c["time"])
                c_ratio = v(c) / baseline
                if cl(c) > range_high and c_ratio >= TIGHT_REENTRY_VOL_MULT:
                    breakout_up, trigger_ratio = True, c_ratio
                    break
                if cl(c) < range_low and c_ratio >= TIGHT_REENTRY_VOL_MULT:
                    breakout_down, trigger_ratio = True, c_ratio
                    break

            if not breakout_up and not breakout_down:
                return True

            if len(tight_open_trades) >= TIGHT_MAX_CONCURRENT_TRADES:
                print(f"[TIGHT SKIP] {symbol} breakout ignored - max concurrent trades reached")
                tight_watch.pop(symbol, None)
                return True

            side    = "BUY" if breakout_up else "SELL"
            entry   = cl(live_candle)   # market order fills at current price
            atr_now = state["atr_now"]

            # ---- BTC direction filter (2026-07-17): alts follow BTC. Block breakouts
            # against a CLEAR BTC direction; sideways BTC = no opinion, both sides ok ----
            if BTC_FILTER_ENABLED:
                btc_dir = get_btc_direction()
                if btc_dir is not None:
                    if (side == "BUY" and btc_dir == "DOWN") or (side == "SELL" and btc_dir == "UP"):
                        print(f"[TIGHT BTC SKIP] {symbol} {side} blocked - BTC direction is {btc_dir}")
                        tight_watch.pop(symbol, None)
                        return True

            # ---- MTF trend filter (2026-07-17): Fast has had this since Jul 11, Tight
            # never did - counter-trend 1m breakouts were free SL donations ----
            if MTF_FILTER_ENABLED:
                mtf_trend = get_mtf_trend(symbol)
                if mtf_trend is not None:
                    if (side == "BUY" and mtf_trend != "UP") or (side == "SELL" and mtf_trend != "DOWN"):
                        print(f"[TIGHT MTF SKIP] {symbol} {side} blocked - {MTF_INTERVAL} trend is {mtf_trend}")
                        tight_watch.pop(symbol, None)
                        return True

            # ---- Extension hard-skip (2026-07-17): same anti-chasing guard as Fast.
            # Breakouts fired after a long one-way run are tail-end entries ----
            closes_e = [cl(x) for x in confirmed]
            if atr_now > 0 and len(closes_e) > TIGHT_EXTENSION_LOOKBACK:
                ext = abs(closes_e[-1] - closes_e[-1 - TIGHT_EXTENSION_LOOKBACK]) / atr_now
                if ext > TIGHT_EXTENSION_HARD_SKIP:
                    print(f"[TIGHT SKIP] {symbol} extension={ext:.1f}x ATR exceeds hard-skip limit ({TIGHT_EXTENSION_HARD_SKIP}x) - move too exhausted, no trade")
                    tight_watch.pop(symbol, None)
                    return True

            sl = range_low - atr_now * TIGHT_SL_ATR_BUFFER_MULT if side == "BUY" else range_high + atr_now * TIGHT_SL_ATR_BUFFER_MULT
            risk = abs(entry - sl)
            tight_watch.pop(symbol, None)
            if risk <= 0:
                return True

            # ---- Minimum SL distance (2026-07-17): on 1m the range-based SL sometimes
            # lands within spread/noise of entry - those stop out on nothing ----
            if entry > 0 and (risk / entry) * 100 < TIGHT_MIN_SL_DIST_PCT:
                print(f"[TIGHT SKIP] {symbol} SL distance {(risk / entry) * 100:.2f}% < min {TIGHT_MIN_SL_DIST_PCT}% - noise-range trade, skipping")
                return True
            tp = round(entry + risk * TIGHT_RR_TP, 6) if side == "BUY" else round(entry - risk * TIGHT_RR_TP, 6)
            sl = round(sl, 6)

            trade_status = ""
            if tight_auto_trade_enabled:
                oid = place_tight_order(symbol, side, entry, sl, tp)
                if oid == "MARGIN_SKIP":
                    trade_status = "\nSkipped - insufficient margin"
                elif oid and oid != "N/A":
                    trade_status = "\nOrder: " + str(oid)
                else:
                    trade_status = "\nOrder failed"
            else:
                trade_status = "\nAuto-trade OFF"
            send_tg(
                "TIGHT SIGNAL - " + side + " - " + symbol + "\n------------------------------\n"
                "Entry: " + str(round(entry, 6)) + " | SL: " + str(sl) + " | TP (1:" + str(TIGHT_RR_TP) + "): " + str(tp) + "\n"
                "Breakout vol: " + str(round(trigger_ratio, 1)) + "x | BE at 1:" + str(TIGHT_BE_TRIGGER_R) + "R" +
                trade_status + "\n------------------------------\nNiti Tight"
            )
        return True
    except Exception as e:
        print(f"[TIGHT {symbol}] error: {e}")
        return True


def tight_diagnostic_check():
    """OVERRIDES the earlier version - cache-only baseline lookup."""
    try:
        baseline = get_cached_baseline("BTC-USDT")
        if not baseline:
            print(f"[TIGHT DIAG] BTC-USDT baseline not cached yet - refresher warming up ({len(tight_baseline_cache)} symbols covered)")
            return
        candles = get_candles("BTC-USDT", limit=5, interval=TIGHT_TIMEFRAME)
        if len(candles) < 2:
            print(f"[TIGHT DIAG] BTC-USDT - only got {len(candles)} recent candles")
            return
        live_ratio = v(candles[-1]) / baseline
        print(f"[TIGHT DIAG] BTC-USDT baseline_vol={baseline:.2f} live_vol={v(candles[-1]):.2f} ratio={live_ratio:.2f}x (need {TIGHT_SPIKE_VOL_MULT}x)")
    except Exception as e:
        print(f"[TIGHT DIAG ERROR] {e}")


def tight_scan_loop():
    """OVERRIDES the earlier version - never blocks on baseline fetching."""
    global tight_symbols_current
    print("Tight loop started - Stock Niti strategy (2026-07-11, non-blocking baseline rework)")
    all_symbols = []
    while True:
        try:
            if not all_symbols:
                all_symbols = get_futures_symbols() or []
            symbols = get_liquid_symbols(all_symbols, min_quote_vol=TIGHT_MIN_QUOTE_VOL, max_n=TIGHT_MAX_SYMBOLS)
            if EXCLUDE_XSTOCKS_TIGHT:
                # "NCS" not "NCSK" (2026-07-17): the family prefix is NCS - NCSI-NIKKEI
                # slipped through the K-only filter and donated -$2.59 on Jul 17
                symbols = [s for s in symbols if not s.startswith("NCS")]
            tight_symbols_current = symbols
            covered = sum(1 for s in symbols if s in tight_baseline_cache)
            print(f"[TIGHT SCAN] Scanning {len(symbols)}/{len(all_symbols)} liquid pairs | Baseline={covered}/{len(symbols)} | Watching={len(tight_watch)} | Open={len(tight_open_trades)} | Auto={tight_auto_trade_enabled}")
            tight_diagnostic_check()
            t0 = time.time()
            for sym in symbols:
                if check_tight_symbol(sym):
                    time.sleep(TIGHT_SCAN_SYMBOL_GAP)
            print(f"[TIGHT SCAN] Done in {int(time.time() - t0)}s.")
            send_daily_summary()
        except Exception as e:
            print(f"[TIGHT LOOP ERROR] {e}")
        time.sleep(TIGHT_SCAN_INTERVAL_SECONDS)


Thread(target=tight_baseline_loop, daemon=True).start()
# ==================== END TIGHT 2 SPEED FIX ====================



# ==================== TIGHT 3: DORMANT AWAKENING (added 2026-07-19) ====================
# Catches BANK-USDT-class multi-day runners at the START of the move, which Tight 2
# structurally cannot: Tight 2's 3-day average baseline gets inflated by the pump
# itself, so by day 2-3 of a run the 20x spike test is unhittable. Tight 3 instead
# asks the Stock-Niti question directly: "was this coin ASLEEP, and did it just
# WAKE UP?" Three layers:
#   1. Dormancy scan (every 4h, daily candles): coin traded in a +/-15% close band
#      for T3_DORMANCY_DAYS days with NO volume day above 3x the MEDIAN -> watchlist.
#      Median (not mean) is the whole point - one weird day cannot poison it.
#   2. Awakening check (every 5 min on the watchlist): today's live daily volume
#      >= 7x the dormant median AND a closed 1h candle outside the dormant range.
#   3. Entry (15m): NO chasing the awakening candle. Wait for the first pullback
#      (>=3 candles without a new extreme, or a >=1x ATR retrace), then enter on
#      the break of that mini-consolidation. SL beyond the pullback extreme.
# Exit is where the 2k+ trades live: NO fixed TP. BE at 2R, then from 4R onward the
# SL trails peak-minus-2R (peak tracked LIVE every 30s pass, not on candle closes -
# a wick to 7R must ratchet the trail even if the candle closes lower).
# Expectations (agreed with Faisal 2026-07-19): 0-3 signals/week, some fake
# awakenings will hit SL, one real runner pays for the month. Runs fully alongside
# Tight 2 + Fast; nothing above this section changed for T3.

T3_MIN_QUOTE_VOL          = float(os.environ.get("T3_MIN_QUOTE_VOL", 300_000))   # dormant coins are quiet by definition - the $1M Tight floor would exclude pre-pump BANK. Known cost: thinner books, more slippage.
T3_MAX_SYMBOLS            = int(os.environ.get("T3_MAX_SYMBOLS", 600))
T3_MAX_WATCHLIST          = int(os.environ.get("T3_MAX_WATCHLIST", 150))         # keep the TIGHTEST bands if more qualify - the most compressed springs
T3_DORMANCY_DAYS          = int(os.environ.get("T3_DORMANCY_DAYS", 10))
T3_DORMANCY_RANGE_PCT     = float(os.environ.get("T3_DORMANCY_RANGE_PCT", 30.0)) # total close band width = +/-15% around mid
T3_DORMANCY_VOL_SPIKE_MAX = float(os.environ.get("T3_DORMANCY_VOL_SPIKE_MAX", 3.0))   # any day >3x median inside the window = already awakened earlier, not dormant
T3_AWAKE_VOL_MULT         = float(os.environ.get("T3_AWAKE_VOL_MULT", 7.0))      # UNTESTED starting point - if T3 stays silent for weeks try 5.0, if it spams try 10.0
T3_DORMANCY_SCAN_SECONDS  = int(os.environ.get("T3_DORMANCY_SCAN_SECONDS", 14400))   # rebuild watchlist every 4h (1 daily-candle request per symbol)
T3_AWAKE_CHECK_SECONDS    = int(os.environ.get("T3_AWAKE_CHECK_SECONDS", 300))
T3_SETUP_EXPIRY_SECONDS   = int(os.environ.get("T3_SETUP_EXPIRY_SECONDS", 172800))   # 48h to form a pullback entry after the awakening, else skip
T3_PULLBACK_MIN_CANDLES   = int(os.environ.get("T3_PULLBACK_MIN_CANDLES", 3))
T3_PULLBACK_ATR_MULT      = float(os.environ.get("T3_PULLBACK_ATR_MULT", 1.0))   # OR a retrace this deep counts as a pullback even before 3 candles
T3_MAX_SL_ATR_MULT        = float(os.environ.get("T3_MAX_SL_ATR_MULT", 3.0))     # entry-to-SL wider than this x ATR(15m) = awakening already too extended, skip
T3_BE_TRIGGER_R           = float(os.environ.get("T3_BE_TRIGGER_R", 2.0))
T3_TRAIL_START_R          = float(os.environ.get("T3_TRAIL_START_R", 4.0))       # 2R-4R: BE only, deliberately NO trail - the runner gets room to breathe
T3_TRAIL_GAP_R            = float(os.environ.get("T3_TRAIL_GAP_R", 2.0))         # trail SL = live peak R minus this
T3_TRAIL_STEP_R           = float(os.environ.get("T3_TRAIL_STEP_R", 0.5))        # only re-place the exchange SL when it improves by >=0.5R - not every 30s tick
T3_COOLDOWN_SECONDS       = int(os.environ.get("T3_COOLDOWN_SECONDS", 604800))   # 7 days per coin after a signal fires, win or lose
T3_MAX_CONCURRENT_TRADES  = 2      # hardcoded for $100 account (2026-07-18 sizing policy)
T3_RISK_USDT              = 2.0    # hardcoded for $100 account, same scaling plan as Tight/Fast: $150->3, $250->4, $350->5
T3_LEVERAGE               = int(os.environ.get("T3_LEVERAGE", 10))   # 10x not 20x - $300k-liquidity pairs, wider structural SLs
T3_MAX_MARGIN_USDT        = 25.0
T3_ATR_LEN                = 14
T3_SL_ATR_BUFFER_MULT     = 0.3

t3_auto_trade_enabled = AUTO_RESUME_ON_START
t3_watchlist   = {}   # symbol -> dormancy info (dormant range + median vol), rebuilt every 4h
t3_watch       = {}   # symbol -> AWAKENED state machine (pullback tracking -> entry)
t3_open_trades = {}   # order_id -> trade dict (managed by track_t3_trades in the 30s loop)
t3_cooldown    = {}   # symbol -> unix ts until which no new T3 signal may fire


def _median(vals):
    s = sorted(vals)
    n = len(s)
    if n == 0:
        return 0.0
    m = n // 2
    return s[m] if n % 2 else (s[m - 1] + s[m]) / 2


def t3_in_cooldown(symbol):
    until = t3_cooldown.get(symbol, 0)
    if until and time.time() < until:
        return True
    if until:
        t3_cooldown.pop(symbol, None)
    return False


def t3_symbol_has_open_trade(symbol):
    return any(t.get("symbol") == symbol for t in t3_open_trades.values())


def t3_build_watchlist(all_symbols):
    """Dormancy scan: 1 daily-candle request per candidate symbol, every 4h.
    Replaces t3_watchlist wholesale. Symbols currently awakened, in trade or in
    cooldown are excluded up front. NCS tokenized stocks are ALWAYS excluded -
    equities are 'dormant' every single weekend, which would flood the list with
    guaranteed false setups."""
    global t3_watchlist
    symbols = get_liquid_symbols(all_symbols, min_quote_vol=T3_MIN_QUOTE_VOL, max_n=T3_MAX_SYMBOLS)
    symbols = [s for s in symbols if not s.startswith("NCS")]
    found = {}
    scanned = 0
    for sym in symbols:
        if api_backoff_active():
            print("[T3 DORMANCY] aborting scan - API backoff active")
            break
        if sym in t3_watch or t3_symbol_has_open_trade(sym) or t3_in_cooldown(sym):
            continue
        try:
            daily = get_candles(sym, limit=T3_DORMANCY_DAYS + 2, interval="1d")
            scanned += 1
            if len(daily) < T3_DORMANCY_DAYS + 1:
                continue   # too young a listing to have a dormancy history
            window = daily[:-1][-T3_DORMANCY_DAYS:]   # closed days only, live day excluded
            closes = [cl(c) for c in window]
            vols   = [v(c)  for c in window]
            if min(closes) < MIN_PRICE:
                continue
            med = _median(vols)
            if med <= 0:
                continue
            band_pct = (max(closes) - min(closes)) / min(closes) * 100
            if band_pct > T3_DORMANCY_RANGE_PCT:
                continue
            if max(vols) > med * T3_DORMANCY_VOL_SPIKE_MAX:
                continue
            found[sym] = {
                "dormant_high": max(h(c) for c in window),
                "dormant_low":  min(l(c) for c in window),
                "median_vol":   med,
                "band_pct":     band_pct,
                "ts":           time.time(),
            }
        except Exception as e:
            print(f"[T3 DORMANCY] {sym} error: {e}")
        time.sleep(0.25)
    if len(found) > T3_MAX_WATCHLIST:
        keep = sorted(found.items(), key=lambda kv: kv[1]["band_pct"])[:T3_MAX_WATCHLIST]
        found = dict(keep)
    t3_watchlist = found
    print(f"[T3 DORMANCY] scan done - {scanned} candidates checked, {len(found)} dormant coins on watchlist")


def t3_check_awakening(symbol, info):
    """Awakening test for one dormant watchlist symbol: today's LIVE daily volume
    >= T3_AWAKE_VOL_MULT x the dormant MEDIAN, and the last CLOSED 1h candle
    finished outside the dormant range. Both must hold in the same check."""
    try:
        daily = get_candles(symbol, limit=2, interval="1d")
        if not daily:
            return
        today_vol = v(daily[-1])
        ratio = today_vol / info["median_vol"] if info["median_vol"] > 0 else 0
        if ratio < T3_AWAKE_VOL_MULT:
            return
        h1 = get_candles(symbol, limit=3, interval="1h")
        if len(h1) < 2:
            return
        h1_close = cl(h1[-2])   # last CLOSED 1h candle
        side = None
        if h1_close > info["dormant_high"]:
            side = "BUY"
        elif h1_close < info["dormant_low"]:
            side = "SELL"
        if side is None:
            return
        t3_watch[symbol] = {
            "side": side, "awake_ts": time.time(),
            "expiry_ts": time.time() + T3_SETUP_EXPIRY_SECONDS,
            "peak": h1_close,                     # running post-awakening extreme (high for BUY, low for SELL)
            "pullback_highs": [], "pullback_lows": [],
            "trigger": None, "setup_sl": None,
            # only 15m candles that CLOSE after this moment count - processing
            # pre-awakening history would build bogus setups out of the dormant
            # chop itself (caught in dry-run testing before deploy, 2026-07-19)
            "last_processed_time": int(time.time() * 1000),
            "vol_ratio": ratio,
            "dormant_high": info["dormant_high"], "dormant_low": info["dormant_low"],
        }
        t3_watchlist.pop(symbol, None)
        print(f"[T3 AWAKENING] {symbol} {side} vol={ratio:.1f}x dormant median | 1h close {h1_close} outside range ({info['dormant_low']}-{info['dormant_high']})")
        send_tg(
            "TIGHT 3 AWAKENING - " + side + " - " + symbol + "\n------------------------------\n"
            "Dormant " + str(T3_DORMANCY_DAYS) + "d range: " + str(info["dormant_low"]) + " - " + str(info["dormant_high"]) + "\n"
            "Today vol: " + str(round(ratio, 1)) + "x dormant median (need " + str(T3_AWAKE_VOL_MULT) + "x) | 1h close: " + str(h1_close) + "\n"
            "Watching 15m for a pullback entry (window " + str(T3_SETUP_EXPIRY_SECONDS // 3600) + "h)"
            "\n------------------------------\nNiti Tight 3"
        )
    except Exception as e:
        print(f"[T3 AWAKE {symbol}] error: {e}")


def t3_fire_entry(symbol, st, entry_px, atr_now):
    """Entry signal for an awakened symbol. Fires the 7-day cooldown regardless of
    outcome (win, lose, skip - agreed 2026-07-19), applies the extension guard,
    then trades if allowed."""
    t3_watch.pop(symbol, None)
    t3_cooldown[symbol] = time.time() + T3_COOLDOWN_SECONDS
    side = st["side"]
    sl   = st["setup_sl"]
    risk = abs(entry_px - sl)
    if risk <= 0:
        return
    if atr_now > 0 and risk > atr_now * T3_MAX_SL_ATR_MULT:
        print(f"[T3 SKIP] {symbol} entry-to-SL {risk / atr_now:.1f}x ATR exceeds {T3_MAX_SL_ATR_MULT}x - awakening too extended")
        send_tg(
            "TIGHT 3 SKIPPED - " + symbol + "\nAwakening detected but too extended: SL distance " +
            str(round(risk / atr_now, 1)) + "x ATR (max " + str(T3_MAX_SL_ATR_MULT) + "x) - position would be meaninglessly small. No trade."
        )
        return
    sl = round(sl, 6)
    trade_status = ""
    if t3_auto_trade_enabled and len(t3_open_trades) >= T3_MAX_CONCURRENT_TRADES:
        trade_status = "\nSkipped - max concurrent T3 trades (" + str(T3_MAX_CONCURRENT_TRADES) + ") reached"
    elif t3_auto_trade_enabled:
        oid = place_t3_order(symbol, side, entry_px, sl)
        if oid == "MARGIN_SKIP":
            trade_status = "\nSkipped - insufficient margin"
        elif oid and oid != "N/A":
            trade_status = "\nOrder: " + str(oid)
        else:
            trade_status = "\nOrder failed"
    else:
        trade_status = "\nAuto-trade OFF"
    print(f"[T3 ENTRY] {symbol} {side} @ {entry_px} SL {sl} vol_ratio={st.get('vol_ratio', 0):.1f}x{trade_status}")
    send_tg(
        "TIGHT 3 ENTRY - " + side + " - " + symbol + "\n------------------------------\n"
        "Entry: " + str(round(entry_px, 6)) + " | SL: " + str(sl) + " | Risk: $" + str(T3_RISK_USDT) + " | Lev: " + str(T3_LEVERAGE) + "x\n"
        "No fixed TP - BE at " + str(T3_BE_TRIGGER_R) + "R, trail from " + str(T3_TRAIL_START_R) + "R at peak-" + str(T3_TRAIL_GAP_R) + "R\n"
        "Awakening vol: " + str(round(st.get("vol_ratio", 0), 1)) + "x dormant median" +
        trade_status + "\n------------------------------\nNiti Tight 3"
    )


def t3_check_awakened(symbol):
    """15m pullback-entry state machine for one awakened symbol. Processes every
    CLOSED 15m candle exactly once (last_processed_time). A new post-awakening
    extreme resets the pullback; T3_PULLBACK_MIN_CANDLES without a new extreme (or
    a >=1x ATR retrace) forms the setup; a close through the setup trigger enters."""
    st = t3_watch.get(symbol)
    if not st:
        return
    if time.time() >= st["expiry_ts"]:
        t3_watch.pop(symbol, None)
        print(f"[T3 EXPIRED] {symbol} - no pullback entry within the window")
        send_tg("TIGHT 3 EXPIRED - " + symbol + "\nNo clean pullback entry within " + str(T3_SETUP_EXPIRY_SECONDS // 3600) + "h of the awakening - trade skipped")
        return
    try:
        candles = get_candles(symbol, limit=60, interval="15m")
        if len(candles) < T3_ATR_LEN + 5:
            return
        confirmed = candles[:-1]
        highs  = [h(c)  for c in confirmed]
        lows   = [l(c)  for c in confirmed]
        closes = [cl(c) for c in confirmed]
        atr_now = atr_series(highs, lows, closes, T3_ATR_LEN)[-1]
        side   = st["side"]
        last_t = int(st.get("last_processed_time") or 0)
        for c in [x for x in confirmed if int(x["time"]) > last_t]:
            st["last_processed_time"] = int(c["time"])

            # 1) if a setup exists, the entry check comes FIRST for this candle
            if st.get("trigger") is not None:
                if (side == "BUY" and cl(c) > st["trigger"]) or (side == "SELL" and cl(c) < st["trigger"]):
                    t3_fire_entry(symbol, st, cl(c), atr_now)
                    return

            # 2) otherwise update the peak / pullback tracking with this candle
            if side == "BUY":
                if h(c) > st["peak"]:
                    st["peak"] = h(c)
                    st["pullback_highs"], st["pullback_lows"] = [], []
                    st["trigger"], st["setup_sl"] = None, None
                else:
                    st["pullback_highs"].append(h(c))
                    st["pullback_lows"].append(l(c))
                    deep_enough = (st["peak"] - min(st["pullback_lows"])) >= atr_now * T3_PULLBACK_ATR_MULT
                    if len(st["pullback_highs"]) >= T3_PULLBACK_MIN_CANDLES or deep_enough:
                        st["trigger"]  = max(st["pullback_highs"])
                        st["setup_sl"] = min(st["pullback_lows"]) - atr_now * T3_SL_ATR_BUFFER_MULT
            else:
                if l(c) < st["peak"]:
                    st["peak"] = l(c)
                    st["pullback_highs"], st["pullback_lows"] = [], []
                    st["trigger"], st["setup_sl"] = None, None
                else:
                    st["pullback_highs"].append(h(c))
                    st["pullback_lows"].append(l(c))
                    deep_enough = (max(st["pullback_highs"]) - st["peak"]) >= atr_now * T3_PULLBACK_ATR_MULT
                    if len(st["pullback_lows"]) >= T3_PULLBACK_MIN_CANDLES or deep_enough:
                        st["trigger"]  = min(st["pullback_lows"])
                        st["setup_sl"] = max(st["pullback_highs"]) + atr_now * T3_SL_ATR_BUFFER_MULT
    except Exception as e:
        print(f"[T3 AWAKENED {symbol}] error: {e}")


def place_t3_order(symbol, side, entry, sl):
    """Market entry + guarded SL only - deliberately NO exchange TP orders (the exit
    is the trail). Same risk-based sizing, margin pre-check and naked-position
    emergency-close discipline as Tight/Fast."""
    try:
        set_leverage_api(symbol, T3_LEVERAGE)
        precision = symbol_precision.get(symbol, 4)
        risk_dist = abs(entry - sl)
        if risk_dist <= 0:
            return None
        risk_qty       = T3_RISK_USDT / risk_dist
        margin_cap_qty = (T3_MAX_MARGIN_USDT * T3_LEVERAGE) / entry
        total_qty = round(min(risk_qty, margin_cap_qty), precision)
        if total_qty <= 0:
            return None
        pos_side   = "LONG" if side == "BUY" else "SHORT"
        close_side = "SELL" if side == "BUY" else "BUY"

        required_margin = total_qty * entry / T3_LEVERAGE
        avail = get_available_margin()
        if avail is not None and avail < required_margin * 1.05:
            print(f"[T3 MARGIN SKIP] {symbol} need ~${required_margin:.2f}, available ${avail:.2f} - skipping")
            return "MARGIN_SKIP"

        order_id = place_market_order(symbol, side, total_qty, pos_side)
        print(f"[T3 ORDER] {symbol} {side} qty={total_qty} risk=${T3_RISK_USDT}: {order_id}")
        if order_id != "N/A":
            time.sleep(0.5)
            entry_fill = get_fill_price(order_id, symbol, fallback=entry)
            risk_dist = abs(entry_fill - sl)
            if risk_dist <= 0:
                risk_dist = abs(entry - sl)
            sl_id = place_sl_guarded(symbol, close_side, pos_side, sl, total_qty)
            if sl_id is None:
                print(f"[T3 SL GUARD] {symbol} SL placement failed twice - emergency closing position")
                place_market_order(symbol, close_side, total_qty, pos_side)
                send_tg(f"⚠️ TIGHT 3 {symbol}: SL placement failed - position emergency-closed for safety")
                return None
            t3_open_trades[str(order_id)] = {
                "symbol": symbol, "side": side, "entry": entry, "entry_fill": entry_fill,
                "sl": sl, "sl_id": sl_id, "total_qty": total_qty,
                "close_side": close_side, "pos_side": pos_side,
                "risk_dist": risk_dist, "be_done": False, "be_price": None,
                "trailed": False, "peak_r": 0.0,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
        return order_id
    except Exception as e:
        print(f"[T3 ORDER ERROR] {symbol}: {e}")
        return None


def track_t3_trades():
    """Runs in the 30s trailing loop. Peak R is tracked from LIVE price every pass
    (a 30s wick to 7R must ratchet the trail even if no candle ever closes there).
    Exit ladder: BE at 2R -> nothing between 2R and 4R (room to run) -> from 4R the
    SL trails peak-minus-2R, re-placed on the exchange only when it improves by
    >=T3_TRAIL_STEP_R (order-spam guard). New-first SL replacement throughout."""
    for oid in list(t3_open_trades.keys()):
        trade = t3_open_trades.get(oid)
        if not trade:
            continue
        try:
            symbol    = trade["symbol"]
            risk_dist = trade.get("risk_dist", 0)
            entry_ref = trade.get("entry_fill", trade["entry"])

            # ---- SL fill = trade over (SL / BE / Trail all end here) ----
            sl_status = check_order_status(trade["sl_id"], symbol) if trade.get("sl_id") else ""
            if sl_status == "FILLED":
                sl_fill = get_fill_price(trade["sl_id"], symbol, fallback=trade["sl"])
                if trade["side"] == "BUY":
                    leg_pnl = (sl_fill - entry_ref) * trade["total_qty"]
                else:
                    leg_pnl = (entry_ref - sl_fill) * trade["total_qty"]
                if trade.get("trailed"):
                    result = "Trail"
                elif trade.get("be_done"):
                    result = "BE"
                else:
                    result = "SL"
                trade["pnl"]    = round(leg_pnl, 2)
                trade["result"] = result
                trade["label"]  = "Tight 3"
                daily_trades.append(trade)
                journal_closed_trade(trade)
                t3_open_trades.pop(oid, None)
                print(f"[T3 CLOSE] {symbol} {result} pnl={trade['pnl']}")
                continue

            if risk_dist <= 0:
                continue
            current = get_current_price(symbol)
            if current <= 0:
                continue
            if trade["side"] == "BUY":
                fav_r = (current - entry_ref) / risk_dist
            else:
                fav_r = (entry_ref - current) / risk_dist
            if fav_r > trade.get("peak_r", 0.0):
                trade["peak_r"] = fav_r

            # ---- BE at 2R ----
            if not trade.get("be_done") and trade["peak_r"] >= T3_BE_TRIGGER_R:
                new_sl_id = place_sl_guarded(symbol, trade["close_side"], trade["pos_side"], entry_ref, trade["total_qty"])
                if new_sl_id:
                    if trade.get("sl_id"):
                        cancel_order(symbol, trade["sl_id"])
                    trade["sl_id"]    = new_sl_id
                    trade["sl"]       = entry_ref
                    trade["be_price"] = entry_ref
                    trade["be_done"]  = True
                    print(f"[T3 BE] {symbol} SL moved to breakeven ({entry_ref}) at {trade['peak_r']:.1f}R")
                    send_tg("TIGHT 3 " + symbol + " - " + str(round(trade['peak_r'], 1)) + "R reached, SL moved to breakeven (" + str(entry_ref) + "). Free trade - trail starts at " + str(T3_TRAIL_START_R) + "R.")
                else:
                    print(f"[T3 BE FAIL] {symbol} BE SL re-placement failed twice - keeping original SL {trade['sl']}")
                    if not trade.get("sl_move_alerted"):
                        trade["sl_move_alerted"] = True
                        send_tg(f"⚠️ TIGHT 3 {symbol}: could not move SL to breakeven ({entry_ref}) - original SL {trade['sl']} still active. Consider moving it manually.")

            # ---- Peak-minus-2R trail from 4R ----
            if trade.get("be_done") and trade["peak_r"] >= T3_TRAIL_START_R:
                target_r = trade["peak_r"] - T3_TRAIL_GAP_R
                if trade["side"] == "BUY":
                    desired = round(entry_ref + risk_dist * target_r, 6)
                    improve_r = (desired - trade["sl"]) / risk_dist
                else:
                    desired = round(entry_ref - risk_dist * target_r, 6)
                    improve_r = (trade["sl"] - desired) / risk_dist
                if improve_r >= T3_TRAIL_STEP_R:
                    new_id = place_sl_guarded(symbol, trade["close_side"], trade["pos_side"], desired, trade["total_qty"])
                    if new_id:
                        if trade.get("sl_id"):
                            cancel_order(symbol, trade["sl_id"])
                        trade["sl_id"]   = new_id
                        trade["sl"]      = desired
                        trade["trailed"] = True
                        print(f"[T3 TRAIL] {symbol} peak {trade['peak_r']:.1f}R -> SL locked at {target_r:.1f}R ({desired})")
                    else:
                        print(f"[T3 TRAIL FAIL] {symbol} trail SL re-placement failed - keeping SL {trade['sl']}")
        except Exception as e:
            print(f"[T3 TRACK ERROR] {oid}: {e}")


def t3_loop():
    """Tight 3 main loop. Internal timers: dormancy rescan every 4h, awakening
    check on the watchlist every 5 min, awakened symbols processed every pass
    (60s). Fully respects the global API backoff."""
    print("Tight 3 loop started - Dormant Awakening (dormancy -> 7x median awakening -> pullback entry -> peak-2R trail)")
    all_symbols   = []
    last_dormancy = 0.0
    last_awake    = 0.0
    while True:
        try:
            if api_backoff_active():
                time.sleep(30)
                continue
            if time.time() - last_dormancy >= T3_DORMANCY_SCAN_SECONDS or last_dormancy == 0:
                if not all_symbols:
                    all_symbols = get_futures_symbols() or []
                t3_build_watchlist(all_symbols)
                last_dormancy = time.time()

            for sym in list(t3_watch.keys()):
                t3_check_awakened(sym)
                time.sleep(0.2)

            if time.time() - last_awake >= T3_AWAKE_CHECK_SECONDS:
                checked = 0
                for sym, info in list(t3_watchlist.items()):
                    if api_backoff_active():
                        break
                    if sym in t3_watch or t3_symbol_has_open_trade(sym) or t3_in_cooldown(sym):
                        continue
                    t3_check_awakening(sym, info)
                    checked += 1
                    time.sleep(0.2)
                last_awake = time.time()
                print(f"[T3 SCAN] watchlist={len(t3_watchlist)} checked={checked} awakened={len(t3_watch)} open={len(t3_open_trades)} auto={t3_auto_trade_enabled}")
        except Exception as e:
            print(f"[T3 LOOP ERROR] {e}")
        time.sleep(60)
# ==================== END TIGHT 3 ====================


if __name__ == "__main__":
    Thread(target=tight_scan_loop,          daemon=True).start()
    Thread(target=fast_scan_loop,           daemon=True).start()
    Thread(target=trailing_loop,            daemon=True).start()
    Thread(target=t3_loop,                  daemon=True).start()
    Thread(target=handle_telegram_commands, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
