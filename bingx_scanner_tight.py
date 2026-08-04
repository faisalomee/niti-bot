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

# ==================== SHARED / INFRA CONFIG ====================
# Kept for the infrastructure helpers below (backoff, price cache, MTF/BTC helpers).
API_BACKOFF_SECONDS = int(os.environ.get("API_BACKOFF_SECONDS", 1200))   # 20 min full silence on a rate-limit hit (self-renewing penalty)
PRICE_CACHE_SECONDS = int(os.environ.get("PRICE_CACHE_SECONDS", 30))
AUTO_RESUME_ON_START = os.environ.get("AUTO_RESUME_ON_START", "false").lower() == "true"
MIN_PRICE           = 0.001
# get_mtf_trend / get_btc_direction are retained infra helpers - keep their constants defined.
MTF_INTERVAL        = os.environ.get("MTF_INTERVAL", "1h")
EMA200_LEN          = 200
BTC_FILTER_CANDLES  = int(os.environ.get("BTC_FILTER_CANDLES", 4))

# Tokenized-equity / commodity pairs (NCS* = xStocks, NCCO* = tokenized oil/metals).
# Thin books, gap around equity sessions - poison for crypto mean-reversion. ALWAYS
# excluded from both engines (fixes the 2026-07-24 NCCO leak: WTI fired 3x in a day).
def is_tokenized(sym):
    return sym.startswith("NCS") or sym.startswith("NCCO")

# ==================== CRASH FADE CONFIG (Market-Breadth Crash Fade, replaces Fast) ====================
# Backtest: n=922, win 77.3%, net +0.482R, all 3 months positive, 153/165 coins +ve.
# Only fades sharp drops WHILE THE WHOLE SMALL-CAP MARKET IS CRASHING (breadth gate) -
# in a panic, individual drops are forced/exaggerated selling that bounces; in calm
# markets drops are real and don't bounce.
CF_TIMEFRAME              = os.environ.get("CF_TIMEFRAME", "5m")
CF_SCAN_INTERVAL_SECONDS  = int(os.environ.get("CF_SCAN_INTERVAL_SECONDS", 180))   # 5m candles - faster scanning is wasted API load
CF_MIN_QUOTE_VOL          = float(os.environ.get("CF_MIN_QUOTE_VOL", 300_000))     # small-cap focus
CF_MAX_SYMBOLS            = int(os.environ.get("CF_MAX_SYMBOLS", 150))             # breadth sample + candidate pool; bounds API load. Bigger = truer breadth but heavier polling.
CF_EXCLUDE_TOP_N          = int(os.environ.get("CF_EXCLUDE_TOP_N", 75))            # drop the majors - the edge is small-cap only
# ---- Market-breadth index (the winning mechanism) ----
CF_BREADTH_RET_BARS       = int(os.environ.get("CF_BREADTH_RET_BARS", 12))         # each coin's 1h return = 12x 5m
CF_BREADTH_SMOOTH_BARS    = int(os.environ.get("CF_BREADTH_SMOOTH_BARS", 12))      # smooth breadth over 1h
CF_BREADTH_LAG_BARS       = int(os.environ.get("CF_BREADTH_LAG_BARS", 1))          # lag 1 bar = zero look-ahead
CF_BREADTH_GATE           = float(os.environ.get("CF_BREADTH_GATE", -0.8))         # only trade when smoothed/lagged breadth < -0.8% (market actively falling). Sweet spot where all 3 months stay strong; <-1.2 boosts total but flips July negative.
# ---- Signal ----
CF_DROP_LOOKBACK_BARS     = int(os.environ.get("CF_DROP_LOOKBACK_BARS", 12))       # coin must drop over the last 1h...
CF_DROP_MIN_PCT           = float(os.environ.get("CF_DROP_MIN_PCT", 4.0))          # ...by >=4%, then print ONE green candle (stabilising)
CF_ATR_TF                 = os.environ.get("CF_ATR_TF", "15m")
CF_ATR_LEN                = int(os.environ.get("CF_ATR_LEN", 14))
CF_SL_ATR_MULT            = float(os.environ.get("CF_SL_ATR_MULT", 2.5))           # stop 2.5x ATR15 below entry
CF_TARGET_RR              = float(os.environ.get("CF_TARGET_RR", 3.0))             # fixed 3R target (no trail)
CF_MAX_HOLD_SECONDS       = int(os.environ.get("CF_MAX_HOLD_SECONDS", 14400))      # 4h max hold
CF_COOLDOWN_SECONDS       = int(os.environ.get("CF_COOLDOWN_SECONDS", 7200))       # 24 bars/coin
# ---- Entry (dip-buy at the green candle's close) ----
# Backtest entered via a resting MAKER LIMIT bid at the green candle's close, filling
# only if price traded CF_PIERCE_PCT through it (models adverse selection - you catch
# the exact dip you wanted). LIVE we cannot rest/cancel a real exchange limit safely
# (cancel_order is unverified - see its docstring), so we mirror the proven Fast-retest
# pattern: ARM the bid, monitor price, and MARKET-fill on touch-through. Cost vs
# backtest: we pay TAKER not maker (backtest taker was still +0.314R, so positive) and
# a dip that wicks through and bounces inside one 30s poll gap is missed.
CF_PIERCE_PCT             = float(os.environ.get("CF_PIERCE_PCT", 0.1))            # price must trade 0.1% THROUGH the bid to fill
CF_PENDING_EXPIRY_SECONDS = int(os.environ.get("CF_PENDING_EXPIRY_SECONDS", 600))  # 2x 5m to get the dip, else skip
CF_RISK_USDT              = float(os.environ.get("CF_RISK_USDT", 2.0))             # hardcoded $100-account sizing (matches Tight/Fast/T3)
CF_MAX_MARGIN_USDT        = float(os.environ.get("CF_MAX_MARGIN_USDT", 25.0))
CF_MAX_CONCURRENT_TRADES  = int(os.environ.get("CF_MAX_CONCURRENT_TRADES", 2))
# ---- Progress-based exit (2026-07-29): CF previously had NO trailing/stagnation, only
# fixed 3R TP / SL / 4h-market-close. That made almost every TimeExit a small loss.
# Now: BE at 1R, trail from 1.5R (stop = peak_R - 1R), and a stagnation early-exit
# that closes a flat/barely-green trade instead of holding the full 4h. Mirrors RSI.
CF_BE_TRIGGER_R           = float(os.environ.get("CF_BE_TRIGGER_R", 1.0))
CF_TRAIL_START_R          = float(os.environ.get("CF_TRAIL_START_R", 1.5))
CF_TRAIL_GAP_R            = float(os.environ.get("CF_TRAIL_GAP_R", 1.0))
CF_TRAIL_STEP_R           = float(os.environ.get("CF_TRAIL_STEP_R", 0.25))
# Stagnation: after CF_STAGNATION_SECONDS, if the trade never reached CF_STAGNATION_MIN_R
# of favorable progress, cut it at market rather than waiting for the 4h TimeExit.
CF_STAGNATION_SECONDS     = int(os.environ.get("CF_STAGNATION_SECONDS", 5400))     # 1.5h
CF_STAGNATION_MIN_R       = float(os.environ.get("CF_STAGNATION_MIN_R", 0.5))

# ==================== RSI REVERSION CONFIG (RSI Oversold Reversion, replaces Tight 2) ====================
# Backtest: n=1584, win 55.4%, net +0.114R, ALL SIX 2-week blocks positive (the only
# Tight-2 candidate with no negative block). Coin-specific, NO breadth dependency.
RSI_TIMEFRAME             = os.environ.get("RSI_TIMEFRAME", "5m")
RSI_SCAN_INTERVAL_SECONDS = int(os.environ.get("RSI_SCAN_INTERVAL_SECONDS", 120))
RSI_MIN_QUOTE_VOL         = float(os.environ.get("RSI_MIN_QUOTE_VOL", 1_000_000))
RSI_MAX_SYMBOLS           = int(os.environ.get("RSI_MAX_SYMBOLS", 250))
RSI_EXCLUDE_TOP_N         = int(os.environ.get("RSI_EXCLUDE_TOP_N", 75))           # small-cap set, like the backtest universe
RSI_LEN                   = int(os.environ.get("RSI_LEN", 14))
RSI_ENTRY                 = float(os.environ.get("RSI_ENTRY", 15.0))               # 2026-07-26: 20->15 (deep oversold). 8mo+concurrency backtest: RSI<20 is net-negative, RSI<15 is +$45/8mo positive & robust. Biggest Tight2 fix.
RSI_ATR_TF                = os.environ.get("RSI_ATR_TF", "15m")
RSI_ATR_LEN               = int(os.environ.get("RSI_ATR_LEN", 14))
RSI_SL_ATR_MULT           = float(os.environ.get("RSI_SL_ATR_MULT", 2.5))          # 2026-07-26: 3.5->2.5 (tighter SL). Backtest: SL2.5 beats SL3.5 on 8mo. stop 2.5x ATR15
# ---- Exit: BE at 1R, then trail from 1.5R (stop = peak_R minus 1R). No fixed TP. ----
RSI_BE_TRIGGER_R          = float(os.environ.get("RSI_BE_TRIGGER_R", 1.0))
RSI_TRAIL_START_R         = float(os.environ.get("RSI_TRAIL_START_R", 1.5))
RSI_TRAIL_GAP_R           = float(os.environ.get("RSI_TRAIL_GAP_R", 1.0))          # trail stop R = live peak R - 1.0
RSI_TRAIL_STEP_R          = float(os.environ.get("RSI_TRAIL_STEP_R", 0.25))        # only re-place the exchange SL when it improves >=0.25R
RSI_MAX_HOLD_SECONDS      = int(os.environ.get("RSI_MAX_HOLD_SECONDS", 14400))     # 4h
# Stagnation early-exit (2026-07-29): RSI already trails, but a trade that never gets
# going still sat the full 4h. Cut it early if peak_R stays below the floor.
RSI_STAGNATION_SECONDS    = int(os.environ.get("RSI_STAGNATION_SECONDS", 5400))    # 1.5h
RSI_STAGNATION_MIN_R      = float(os.environ.get("RSI_STAGNATION_MIN_R", 0.5))
RSI_COOLDOWN_SECONDS      = int(os.environ.get("RSI_COOLDOWN_SECONDS", 7200))      # 24 bars/coin
RSI_DEDUP_SECONDS         = int(os.environ.get("RSI_DEDUP_SECONDS", 3600))         # skip if this coin took a Crash Fade entry within +/-12 bars - keeps the two strategies disjoint
RSI_RISK_USDT             = float(os.environ.get("RSI_RISK_USDT", 2.0))
RSI_LEVERAGE              = int(os.environ.get("RSI_LEVERAGE", 20))
RSI_MAX_MARGIN_USDT       = float(os.environ.get("RSI_MAX_MARGIN_USDT", 25.0))
RSI_MAX_CONCURRENT_TRADES = int(os.environ.get("RSI_MAX_CONCURRENT_TRADES", 2))

# ==================== GLOBAL STATE ====================
# Command mapping kept stable so existing Telegram habits / Render setup still work:
#   /start /stop        -> RSI Reversion engine   (was Tight)
#   /fast_start /fast_stop -> Crash Fade engine    (was Fast)
#   /t1_start /t1_stop   -> Tight 1 (Dormant Awakening)
rsi_auto_trade_enabled = AUTO_RESUME_ON_START
cf_auto_trade_enabled  = AUTO_RESUME_ON_START
symbol_precision   = {}
symbol_max_lev     = {}

cf_open_trades   = {}   # symbol -> open Crash Fade trade
cf_pending       = {}   # symbol -> armed dip-buy waiting for the pierce fill (cf_check_pending)
cf_cooldown      = {}   # symbol -> unix ts until which no new Crash Fade entry
cf_last_entry_ts = {}   # symbol -> ts of last Crash Fade entry (used for RSI dedup)

rsi_open_trades  = {}   # order_id -> open RSI Reversion trade
rsi_cooldown     = {}   # symbol -> unix ts until which no new RSI entry

daily_trades       = []
last_summary_date  = None

# ---- API backoff state (must be defined before the infra helpers below use it) ----
_api_backoff_until  = 0.0
_api_backoff_logged = 0.0

mtf_cache = {}   # symbol -> cached MTF trend (used by the retained get_mtf_trend helper)

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


def is_tokenized_stock(sym):
    """BingX lists tokenized STOCK perps (SanDisk, SK Hynix, SOXL, etc) that also end
    in -USDT, so a plain 'USDT in symbol' check lets them through. They share a clear
    naming pattern: an 'NCSK' prefix and/or a '2USD-USDT' quote tail (e.g.
    NCSKSNDK2USD-USDT, NCSKSKHYNIX2USD-USDT, ...SOXL2USD-USDT). All strategies are
    validated on CRYPTO only, so these must be excluded from the universe entirely."""
    s = sym.upper()
    return s.startswith("NCSK") or "2USD-USDT" in s or "2USD_USDT" in s

def get_futures_symbols():
    url = BASE_URL + "/openApi/swap/v2/quote/contracts"
    r = requests.get(url, timeout=10).json()
    symbols = []
    for c in r.get("data", []):
        if c.get("status") == 1 and "USDT" in c["symbol"]:
            sym = c["symbol"]
            if is_tokenized_stock(sym):        # skip tokenized stock perps - crypto only
                continue
            symbols.append(sym)
            symbol_precision[sym] = int(c.get("quantityPrecision", 4))
            try:
                symbol_max_lev[sym] = int(float(c.get("maxLongLeverage", 20)))
            except Exception:
                symbol_max_lev[sym] = 20
    return symbols


def _ticker_gain_raw(t):
    """Today's move from a BingX ticker entry, as (value, is_definitely_percent).

    The value is returned RAW - this function deliberately does NOT decide whether
    the exchange means 30.0 or 0.30 for "+30%". That call cannot be made safely from
    a single row (0.5 is a plausible +0.5% AND a plausible +50%), and guessing per
    row is exactly what broke on 2026-07-23. _resolve_gain_scale() decides once, from
    the whole dataset. The open/last fallback IS a true percent by construction, so
    it is flagged as such and exempted from scaling.
    """
    for key in ("priceChangePercent", "priceChangePercentage", "changePercent"):
        raw = t.get(key)
        if raw not in (None, ""):
            try:
                return (float(str(raw).replace("%", "")), False)
            except Exception:
                pass
    try:
        op = float(t.get("openPrice", 0) or 0)
        last = float(t.get("lastPrice", 0) or t.get("close", 0) or 0)
        if op > 0 and last > 0:
            return ((last - op) / op * 100.0, True)
    except Exception:
        pass
    return None


def _resolve_gain_scale(entries):
    """Decide the multiplier for unknown-unit values, ONCE, from the full ticker set.

    Logic: across several hundred crypto perpetuals, on any given day at least one
    coin moves more than 2%. So if the largest absolute value in the whole dataset is
    still below 2, the field cannot be expressed in percent - it must be a fraction
    (0.30 = +30%) and needs x100. This is what the 2026-07-23 bug got wrong: an
    8.0 percent floor was compared against fraction values that can never reach it,
    so the Fast universe was empty on every single pass for 24h ("Scanning 0 pairs").
    """
    unknown = [abs(v) for v, is_pct in entries if not is_pct]
    if not unknown:
        return 1.0
    peak = max(unknown)
    if peak < 2.0:
        print(f"[GAINER UNITS] largest raw daily move across the ticker set = {peak:.4f} "
              f"-> field is FRACTIONAL, scaling by 100")
        return 100.0
    print(f"[GAINER UNITS] largest raw daily move across the ticker set = {peak:.2f} "
          f"-> field is already PERCENT, no scaling")
    return 1.0


_gain_field_logged = False


def get_liquid_symbols(symbols, min_quote_vol, max_n=None, exclude_top_n=0,
                       rank_by_gain=0, gain_min=None, gain_max=None):
    """rank_by_gain (added 2026-07-23, default 0 = OFF): when > 0, the surviving
    symbols are filtered to the [gain_min, gain_max] daily-move band and then
    re-sorted by today's % gain and cut to that many. Tight and Tight 1 do not pass
    any of these, so their universe selection is byte-for-byte unchanged."""
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
                liquid.append((sym, qvol, _ticker_gain_raw(t)))
        liquid.sort(key=lambda x: x[1], reverse=True)
        if exclude_top_n > 0:
            liquid = liquid[exclude_top_n:]

        if rank_by_gain > 0:
            if not _gain_field_logged and tickers:
                # one-time dump so the real field names can be confirmed from the
                # Render logs instead of trusted blindly
                print(f"[TICKER FIELDS] sample entry keys: {sorted(tickers[0].keys())}")
                _gain_field_logged = True

            raw_pairs = [x for x in liquid if x[2] is not None]
            if raw_pairs:
                # Decide the unit ONCE from the whole ticker set, then normalise
                # everything to real percent before any comparison happens.
                scale = _resolve_gain_scale([x[2] for x in raw_pairs])
                scored = [(s, q, (val if is_pct else val * scale))
                          for s, q, (val, is_pct) in raw_pairs]
                scored.sort(key=lambda x: x[2], reverse=True)
                total = len(scored)

                top5 = ", ".join(f"{s}({g:+.1f}%)" for s, _q, g in scored[:5])
                print(f"[GAINER TOP] {total} coins with gain data | biggest movers: {top5}")

                banded = scored
                if gain_min is not None:
                    banded = [x for x in banded if x[2] >= gain_min]
                if gain_max is not None:
                    too_hot = [x for x in banded if x[2] > gain_max]
                    banded = [x for x in banded if x[2] <= gain_max]
                    if too_hot:
                        names = ", ".join(f"{s}({g:.0f}%)" for s, _q, g in too_hot[:5])
                        print(f"[GAINER BAND] {len(too_hot)} coin(s) above +{gain_max}% skipped as late-stage: {names}")

                if banded:
                    liquid = banded[:rank_by_gain]
                else:
                    # NEVER go silently empty (the 2026-07-24 failure: 24h of
                    # "Scanning 0 pairs" with no explanation). Gain data exists, so
                    # the band is simply out of step with today's market - ignore it
                    # for this pass, take the top movers anyway, and say so loudly.
                    liquid = scored[:rank_by_gain]
                    print(f"[GAINER BAND] NOTHING in the +{gain_min}%..+{gain_max}% band "
                          f"(of {total} coins with gain data) - band ignored this pass, "
                          f"scanning the top {len(liquid)} movers instead. If this repeats, "
                          f"widen FAST_GAINER_MIN_PCT / FAST_GAINER_MAX_PCT.")
            else:
                # No usable gain field -> keep the old volume ordering rather than
                # silently returning nothing. Loud, because it means the gainer
                # focus is NOT active.
                print("[GAINER RANK] no usable price-change field in ticker payload - falling back to volume ordering (check [TICKER FIELDS] above)")
                liquid = [(s, q, None) for s, q, _g in liquid]

        if max_n is not None:
            liquid = liquid[:max_n]
        return [x[0] for x in liquid]
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
    # NOTE: no parse_mode. With parse_mode="HTML" a bare "<" in the text (e.g. the RSI
    # message "RSI(14) < 20") is read as a broken HTML tag and Telegram rejects the
    # ENTIRE sendMessage - which is why RSI entries fired silently while the order had
    # already been placed. Plain text needs no escaping.
    requests.post(url, json={"chat_id": cid, "text": msg}, timeout=10)


def send_journal(msg):
    if TG_JOURNAL_ID:
        send_tg(msg, chat_id=TG_JOURNAL_ID)


def journal_closed_trade(trade):
    """Single consolidated journal entry per closed trade - kept deliberately simple,
    no intermediate messages (no TP1-banked / cooldown-triggered spam)."""
    sign = "+" if trade.get("pnl", 0) > 0 else ""
    r_line = ""
    if "exit_r" in trade:
        rs = "+" if trade.get("exit_r", 0) > 0 else ""
        r_line = "Closed: " + rs + str(trade.get("exit_r", 0)) + "R\n"
    send_journal(
        "Trade Closed [" + trade.get("label", "?") + "] - " + trade["symbol"] + "\n"
        "------------------------------\n"
        "Side  : " + trade["side"] + "\nEntry : " + str(trade["entry"]) + "\n"
        "Result: " + trade.get("result", "?") + "\n"
        + r_line +
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


def get_open_positions():
    """All non-zero open positions from BingX. Returns a list of
    {symbol, pos_side, amt(>0), avg}. Empty list on failure (never raises)."""
    try:
        params = build_signed_params({})
        url = BASE_URL + "/openApi/swap/v2/user/positions"
        r = requests.get(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        out = []
        for p in r.get("data", []) or []:
            try:
                amt = abs(float(p.get("positionAmt", 0) or 0))
            except Exception:
                amt = 0
            if amt <= 0:
                continue
            out.append({
                "symbol":   p.get("symbol", ""),
                "pos_side": (p.get("positionSide", "") or "").upper(),
                "amt":      amt,
                "avg":      float(p.get("avgPrice", 0) or 0),
            })
        return out
    except Exception as e:
        print(f"[POSITIONS ERROR] {e}")
        return []


def confirm_liquidated(symbol, trade):
    """A tracker saw `symbol` missing from the per-cycle open-position set and neither
    TP nor SL is filled -> it LOOKS liquidated. But an empty/short positions response
    can also be a transient BingX API hiccup, so before we journal a real liquidation we
    double-check the exchange's OPEN ORDERS for this symbol. If the position were truly
    liquidated, BingX auto-cancels its resting SL/TP, so NO stop/target order remains.
    If our SL (or TP) order is still sitting there, the position is still open and the
    positions call simply lied -> NOT liquidated, skip this cycle. This removes the old
    single-open-trade blind spot without ever false-flagging on an API glitch."""
    try:
        sl_id, _sl_px, tp_id, _tp_px = get_open_orders_for(symbol)
        # Any of our brackets still alive on the exchange => position still open.
        if sl_id or tp_id:
            return False
        return True
    except Exception as e:
        # Could not verify -> be safe, do NOT liquidate this cycle.
        print(f"[LIQ CONFIRM ERROR] {symbol}: {e}")
        return False


def journal_liquidation(trade, label):
    """A tracked position vanished from the exchange without our TP or SL filling ->
    BingX liquidated it (isolated margin hit 100%). Journal the REAL outcome instead
    of the fake 'TimeExit' the old code produced at the 4h mark with a stale in-memory
    entry price. Realized loss on a liquidation is the whole margin at risk, i.e. the
    trade's risk_usdt (the SL sat at -1R; a liquidation is at least that bad). We report
    -risk_usdt as a faithful, non-optimistic figure rather than guessing the exact
    liquidation fill.  Cancels any leftover SL/TP so no orphan orders remain."""
    try:
        symbol = trade.get("symbol", "?")
        for oid_key in ("sl_id", "tp_id"):
            if trade.get(oid_key):
                try:
                    cancel_order(symbol, trade[oid_key])
                except Exception:
                    pass
        risk_usdt = trade.get("risk_usdt")
        if risk_usdt is None:
            # fall back to strategy default via risk_dist * qty if present
            rd = trade.get("risk_dist", 0)
            qty = trade.get("total_qty", trade.get("remaining_qty", 0))
            risk_usdt = round(rd * qty, 2) if (rd and qty) else 0.0
        trade["pnl"]    = -abs(round(risk_usdt, 2))
        trade["result"] = "Liquidated"
        trade["exit_r"] = round(trade["pnl"] / risk_usdt, 2) if risk_usdt else -1.0
        trade["label"]  = label
        daily_trades.append(trade)
        journal_closed_trade(trade)
        send_tg(f"⚠️ {label} {symbol}: position LIQUIDATED on exchange (no TP/SL fill). "
                f"Logged as Liquidated, realized ~{trade['pnl']} USDT. Check margin.")
        print(f"[LIQUIDATION] {label} {symbol} pnl={trade['pnl']}")
    except Exception as e:
        print(f"[LIQUIDATION JOURNAL ERROR] {e}")


def get_open_orders_for(symbol):
    """Open orders for one symbol, split into the first STOP_MARKET and the first
    TAKE_PROFIT_MARKET found -> (sl_id, sl_price, tp_id, tp_price). Any may be None."""
    sl_id = sl_price = tp_id = tp_price = None
    try:
        params = build_signed_params({"symbol": symbol})
        url = BASE_URL + "/openApi/swap/v2/trade/openOrders"
        r = requests.get(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        orders = r.get("data", {}).get("orders", []) or []
        for od in orders:
            otype = str(od.get("type", "")).upper()
            oid   = od.get("orderId")
            try:
                stop = float(od.get("stopPrice", 0) or 0)
            except Exception:
                stop = 0
            if otype == "STOP_MARKET" and sl_id is None:
                sl_id, sl_price = oid, stop
            elif otype == "TAKE_PROFIT_MARKET" and tp_id is None:
                tp_id, tp_price = oid, stop
    except Exception as e:
        print(f"[OPEN ORDERS ERROR] {symbol}: {e}")
    return sl_id, sl_price, tp_id, tp_price


def adopt_positions_on_start():
    """Run ONCE at startup. All engine state is in-memory, so a Render restart/redeploy
    forgets every open position: the bot then re-opens past the concurrency cap and
    ORPHANS the old positions (their BE/trail management never runs again). This
    re-adopts what is actually open on BingX so the caps hold and management resumes.

    Only LONG positions are adopted (CF and RSI are always long; T3 longs too). A LONG
    that already has a TAKE_PROFIT order on the exchange is a Crash Fade bracket (SL+3R
    TP both live on BingX regardless of restart) -> tracked by track_cf_trades. A LONG
    with no TP is a trail-managed position (RSI / T3-long) -> tracked by track_rsi_trades
    (BE at 1R, then trail). If a position has no SL at all, a protective SL is placed and
    the user is alerted. SHORT positions keep their exchange SL and are only reported."""
    try:
        positions = get_open_positions()
    except Exception as e:
        print(f"[ADOPT ERROR] {e}")
        return
    if not positions:
        print("[ADOPT] no open positions on BingX to re-adopt")
        return
    tracked = {t["symbol"] for t in cf_open_trades.values()} | {t["symbol"] for t in rsi_open_trades.values()}
    adopted_cf = adopted_rsi = shorts = 0
    lines = []
    for p in positions:
        sym, amt, avg = p["symbol"], p["amt"], p["avg"]
        if p["pos_side"] != "LONG":
            shorts += 1
            lines.append(sym + " SHORT (kept on its exchange SL, not adopted)")
            continue
        if sym in tracked or avg <= 0:
            continue
        sl_id, sl_price, tp_id, tp_price = get_open_orders_for(sym)
        # Ensure the position is protected. If no SL exists, place a conservative one.
        if sl_id is None:
            fallback_sl = round(avg * (1 - 0.05), 6)   # 5% protective stop until proper management takes over
            sl_id = place_sl_guarded(sym, "SELL", "LONG", fallback_sl, amt)
            sl_price = fallback_sl
            if sl_id is None:
                send_tg("ADOPT WARNING - " + sym + " LONG has NO stop-loss on the exchange and one could not be placed. Set an SL manually NOW.")
            else:
                send_tg("ADOPT - " + sym + " LONG had no SL - placed a protective 5% stop at " + str(fallback_sl) + " until trailing takes over.")
        now = time.time()
        if tp_id is not None:
            # Crash Fade bracket (SL + 3R TP already live) -> let track_cf_trades watch it.
            cf_open_trades[sym] = {
                "symbol": sym, "side": "BUY", "entry": avg, "entry_fill": avg,
                "sl": sl_price if sl_price else round(avg * 0.95, 6), "sl_id": sl_id,
                "tp": tp_price, "tp_id": tp_id, "close_side": "SELL", "pos_side": "LONG",
                "total_qty": amt, "remaining_qty": amt,
                "risk_dist": abs(avg - (sl_price or avg * 0.95)),
                "atr_at_entry": 0.0, "opened_ts": now,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
            adopted_cf += 1
            lines.append(sym + " LONG -> Crash Fade (SL+TP bracket)")
        else:
            # Trail-managed (RSI / T3-long): no exchange TP, exit is bot-driven.
            risk_dist = abs(avg - sl_price) if sl_price else avg * 0.05
            rsi_open_trades["adopt-" + sym] = {
                "symbol": sym, "side": "BUY", "entry": avg, "entry_fill": avg,
                "sl": sl_price if sl_price else round(avg * 0.95, 6), "sl_id": sl_id,
                "close_side": "SELL", "pos_side": "LONG",
                "total_qty": amt, "remaining_qty": amt,
                "risk_dist": risk_dist if risk_dist > 0 else avg * 0.05,
                "atr_at_entry": 0.0, "opened_ts": now,
                "peak_r": 0.0, "be_done": False, "stop_r": None,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
            adopted_rsi += 1
            lines.append(sym + " LONG -> trail-managed (BE@1R then trail)")
    print(f"[ADOPT] re-adopted {adopted_cf} CF + {adopted_rsi} trail-managed, {shorts} short(s) left on their SL")
    if lines:
        send_tg("STARTUP RE-ADOPT\n------------------------------\n" + "\n".join(lines) +
                "\n------------------------------\nConcurrency caps and trailing now account for these.")


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




# ==================== EXTRA INDICATOR: WILDER RSI ====================
def rsi_series(closes, period=14):
    """Wilder's RSI. Returns a list the same length as `closes`; indices before the
    first real value are filled with 50.0 (neutral) - only the last CLOSED-candle
    value is ever used, so the padding is irrelevant. Standard smoothed RSI, matching
    the backtest definition (14-period RSI on 5m closes)."""
    n = len(closes)
    if n < period + 1:
        return [50.0] * n
    gains, losses = [], []
    for i in range(1, n):
        ch = closes[i] - closes[i - 1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    rsis = [50.0] * (period + 1)   # closes[0..period] have no defined RSI yet
    def _rsi(ag, al):
        if al == 0:
            return 100.0
        rs = ag / al
        return 100.0 - (100.0 / (1.0 + rs))
    rsis[period] = _rsi(avg_gain, avg_loss)
    for i in range(period, n - 1):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rsis.append(_rsi(avg_gain, avg_loss))
    if len(rsis) < n:
        rsis += [rsis[-1]] * (n - len(rsis))
    return rsis[:n]



# ==================== CRASH FADE ENGINE (Market-Breadth Crash Fade) ====================
def cf_in_cooldown(symbol):
    return time.time() < cf_cooldown.get(symbol, 0)


def cf_atr15(symbol):
    """14-period ATR on the CLOSED 15m candles. Returns 0 on failure."""
    candles = get_candles(symbol, limit=CF_ATR_LEN + 40, interval=CF_ATR_TF)
    closed = candles[:-1] if len(candles) > 1 else candles
    if len(closed) < CF_ATR_LEN + 1:
        return 0.0
    highs  = [h(c) for c in closed]
    lows   = [l(c) for c in closed]
    closes = [cl(c) for c in closed]
    return atr_series(highs, lows, closes, CF_ATR_LEN)[-1]


def cf_compute_breadth(candles_map):
    """The winning mechanism. Across the whole small-cap sample, compute each coin's
    1h return (12x 5m) at each of the last few CLOSED 5m bars, average CROSS-SECTIONALLY
    per bar (= how the universe is moving), then smooth over the last 12 bars and LAG
    by 1 (zero look-ahead). Returns the smoothed/lagged breadth in PERCENT.

    Fully reconstructed each cycle from closed 5m candles only - stateless and
    look-ahead-safe. All coins are fetched within one cycle so position -1 is the same
    wall-clock 5m bar across the sample."""
    ret_bars = CF_BREADTH_RET_BARS
    raw = []   # one cross-sectional mean per lagged bar
    # Skip the most recent CF_BREADTH_LAG_BARS bars (the lag), then take the next
    # CF_BREADTH_SMOOTH_BARS bars going back, and average the universe at each.
    for offset in range(CF_BREADTH_LAG_BARS, CF_BREADTH_LAG_BARS + CF_BREADTH_SMOOTH_BARS):
        bar_returns = []
        for candles in candles_map.values():
            if len(candles) >= ret_bars + offset + 1:
                c_now  = cl(candles[-1 - offset])
                c_prev = cl(candles[-1 - offset - ret_bars])
                if c_prev > 0:
                    bar_returns.append((c_now / c_prev - 1.0) * 100.0)
        if bar_returns:
            raw.append(sum(bar_returns) / len(bar_returns))
    if not raw:
        return 0.0
    return sum(raw) / len(raw)


def check_cf(symbol, candles):
    """Arm a Crash Fade dip-buy if this coin just dropped >=4% over the last 1h and
    then printed one green candle. `candles` = CLOSED 5m candles (in-progress dropped).
    The market-wide breadth gate is checked ONCE per cycle by the caller."""
    if symbol in cf_open_trades or symbol in cf_pending or cf_in_cooldown(symbol):
        return
    n = CF_DROP_LOOKBACK_BARS
    if len(candles) < n + 3:
        return
    green = candles[-1]
    if cl(green) <= o(green):            # latest closed candle must be GREEN (stabilising)
        return
    start_close = cl(candles[-2 - n])    # n bars before the candle preceding the green one
    end_close   = cl(candles[-2])        # the (red) candle just before the green stabiliser
    if start_close <= 0:
        return
    drop_pct = (end_close / start_close - 1.0) * 100.0
    if drop_pct > -CF_DROP_MIN_PCT:      # need a drop of at least CF_DROP_MIN_PCT
        return

    bid = cl(green)                      # resting maker bid = the green candle's close
    atr15 = cf_atr15(symbol)
    if atr15 <= 0:
        print(f"[CF SKIP] {symbol} - no ATR15")
        return
    sl_price = bid - CF_SL_ATR_MULT * atr15
    if sl_price <= 0 or bid - sl_price <= 0:
        return
    target = bid + CF_TARGET_RR * (bid - sl_price)
    fill_trigger = bid * (1.0 - CF_PIERCE_PCT / 100.0)   # must trade 0.1% THROUGH the bid
    now = time.time()
    cf_pending[symbol] = {
        "side": "BUY", "bid": bid, "sl": sl_price, "target": target,
        "atr15": atr15, "fill_trigger": fill_trigger, "drop_pct": drop_pct,
        "armed_ts": now, "expiry_ts": now + CF_PENDING_EXPIRY_SECONDS,
        "risk_usdt": CF_RISK_USDT,
    }
    print(f"[CF ARM] {symbol} drop={drop_pct:.1f}% bid={bid} SL={round(sl_price,6)} "
          f"TP(3R)={round(target,6)} fill<={round(fill_trigger,6)}")
    send_tg(
        "CRASH FADE ARMED - BUY - " + symbol + "\n------------------------------\n"
        "1h drop: " + str(round(drop_pct, 1)) + "% then green candle (stabilising)\n"
        "Bid: " + str(round(bid, 6)) + " (fills only on a " + str(CF_PIERCE_PCT) + "% dip through) | SL: " + str(round(sl_price, 6)) + "\n"
        "TP (" + str(CF_TARGET_RR) + "R): " + str(round(target, 6)) + " | expires " + str(CF_PENDING_EXPIRY_SECONDS // 60) + "min\n"
        "------------------------------\nNiti Crash Fade"
    )


def cf_check_pending():
    """Fill monitor for armed Crash Fade dip-buys (called every 30s trailing pass).
    LONG dip-buy: we want price to come DOWN to our bid.
      1. price falls to the SL zone first -> the crash kept going, breakout of the
         fade failed, NO entry (loss avoided)
      2. price dips through the bid       -> fill at market (catches the dip)
      3. neither within the window        -> skip"""
    now_ts = time.time()
    for symbol in list(cf_pending.keys()):
        try:
            p = cf_pending[symbol]
            if now_ts >= p["expiry_ts"]:
                del cf_pending[symbol]
                print(f"[CF EXPIRED] {symbol} - no dip to {round(p['fill_trigger'],6)}")
                send_tg("CRASH FADE EXPIRED - " + symbol + "\nNo dip to the bid within " +
                        str(CF_PENDING_EXPIRY_SECONDS // 60) + "min - skipped")
                continue
            px = get_current_price(symbol)
            if px <= 0:
                continue
            if px <= p["sl"]:
                del cf_pending[symbol]
                print(f"[CF INVALIDATED] {symbol} price {px} hit SL zone {round(p['sl'],6)} before fill")
                send_tg("CRASH FADE INVALIDATED - " + symbol +
                        "\nPrice reached the SL zone (" + str(round(p["sl"], 6)) +
                        ") before the dip filled - crash continued, no entry taken")
                continue
            if px > p["fill_trigger"]:
                continue   # not dipped enough yet
            del cf_pending[symbol]
            if symbol in cf_open_trades:
                continue
            trade_status = ""
            if cf_auto_trade_enabled and len(cf_open_trades) >= CF_MAX_CONCURRENT_TRADES:
                trade_status = "\nSkipped - max concurrent trades (" + str(CF_MAX_CONCURRENT_TRADES) + ") reached"
            elif cf_auto_trade_enabled:
                oid = place_cf_order(symbol, p["bid"], p["sl"], p["target"], p["atr15"], p["risk_usdt"])
                if oid == "MARGIN_SKIP":
                    trade_status = "\nSkipped - insufficient margin"
                elif oid and oid != "N/A":
                    trade_status = "\nOrder: " + str(oid)
                    cf_last_entry_ts[symbol] = time.time()
                    cf_cooldown[symbol] = time.time() + CF_COOLDOWN_SECONDS
                else:
                    trade_status = "\nOrder failed"
            else:
                trade_status = "\nAuto-trade OFF"
                cf_cooldown[symbol] = time.time() + CF_COOLDOWN_SECONDS
            print(f"[CF FILL] {symbol} dip filled @ {px} (bid {p['bid']}){trade_status}")
            send_tg(
                "CRASH FADE FILLED - BUY - " + symbol + "\n------------------------------\n"
                "Entry: " + str(round(p["bid"], 6)) + " (dip fill @ " + str(round(px, 6)) + ") | SL: " + str(round(p["sl"], 6)) + "\n"
                "TP (" + str(CF_TARGET_RR) + "R): " + str(round(p["target"], 6)) + " | max hold " + str(CF_MAX_HOLD_SECONDS // 3600) + "h\n"
                "1h drop: " + str(round(p["drop_pct"], 1)) + "% | Risk: $" + str(p["risk_usdt"]) +
                trade_status + "\n------------------------------\nNiti Crash Fade"
            )
        except Exception as e:
            print(f"[CF PENDING {symbol}] error: {e}")


def place_cf_order(symbol, entry, sl_price, target, atr15, risk_usdt):
    """Full-size LONG with a structural SL and a single fixed 3R TP (no half/trail).
    Mirrors the proven Tight/Fast sizing, margin pre-check and SL/TP guards."""
    try:
        lev = get_fast_leverage(symbol)
        set_leverage_api(symbol, lev)
        precision = symbol_precision.get(symbol, 4)
        risk_dist = abs(entry - sl_price)
        if risk_dist <= 0:
            return None
        risk_qty       = risk_usdt / risk_dist
        margin_cap_qty = (CF_MAX_MARGIN_USDT * lev) / entry
        total_qty      = round(min(risk_qty, margin_cap_qty), precision)
        if total_qty <= 0:
            return None
        pos_side, close_side = "LONG", "SELL"

        required_margin = total_qty * entry / lev
        avail = get_available_margin()
        if avail is not None and avail < required_margin * 1.05:
            print(f"[CF MARGIN SKIP] {symbol} need ~${required_margin:.2f}, available ${avail:.2f}")
            return "MARGIN_SKIP"

        order_id = place_market_order(symbol, "BUY", total_qty, pos_side)
        print(f"[CF ORDER] {symbol} BUY lev={lev}x qty={total_qty} risk=${risk_usdt}: {order_id}")
        if order_id != "N/A":
            time.sleep(0.5)
            entry_fill = get_fill_price(order_id, symbol, fallback=entry)
            risk_dist  = abs(entry_fill - sl_price)
            if risk_dist <= 0:
                risk_dist = abs(entry - sl_price)
            tp_price = round(entry_fill + risk_dist * CF_TARGET_RR, 6)   # price precision, not qty precision

            sl_id = place_sl_guarded(symbol, close_side, pos_side, sl_price, total_qty)
            if sl_id is None:
                print(f"[CF SL GUARD] {symbol} SL failed twice - emergency closing")
                place_market_order(symbol, close_side, total_qty, pos_side)
                send_tg(f"⚠️ CRASH FADE {symbol}: SL placement failed - position emergency-closed for safety")
                return None
            tp_id = place_tp_guarded(symbol, close_side, pos_side, tp_price, total_qty, label="TP-3R")
            cf_open_trades[symbol] = {
                "symbol": symbol, "side": "BUY", "entry": entry, "entry_fill": entry_fill,
                "sl": sl_price, "sl_id": sl_id, "tp": tp_price, "tp_id": tp_id,
                "close_side": close_side, "pos_side": pos_side,
                "total_qty": total_qty, "remaining_qty": total_qty,
                "risk_dist": risk_dist, "atr_at_entry": atr15, "opened_ts": time.time(),
                "risk_usdt": risk_usdt, "peak_r": 0.0, "be_done": False, "stop_r": None,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
        return order_id
    except Exception as e:
        print(f"[CF ORDER ERROR] {symbol}: {e}")
        return None


def close_cf_position(symbol, reason=""):
    if symbol not in cf_open_trades:
        return
    trade = cf_open_trades[symbol]
    try:
        remaining  = trade.get("remaining_qty", 0)
        exit_price = get_current_price(symbol)
        if remaining > 0 and cf_auto_trade_enabled:
            close_oid = place_market_order(symbol, trade["close_side"], remaining, trade["pos_side"])
            if close_oid and close_oid != "N/A":
                time.sleep(0.5)
                exit_price = get_fill_price(close_oid, symbol, fallback=exit_price)
        if trade.get("sl_id"):
            cancel_order(symbol, trade["sl_id"])
        if trade.get("tp_id"):
            cancel_order(symbol, trade["tp_id"])
        entry_ref = trade.get("entry_fill", trade["entry"])
        leg_pnl = (exit_price - entry_ref) * remaining   # LONG only
        trade["pnl"]    = round(leg_pnl, 2)
        trade["result"] = reason
        trade["label"]  = "CrashFade"
        daily_trades.append(trade)
        journal_closed_trade(trade)
        del cf_open_trades[symbol]
        print(f"[CF CLOSE] {symbol} {reason}")
    except Exception as e:
        print(f"[CF CLOSE ERROR] {symbol}: {e}")


def track_cf_trades(open_syms=None):
    for symbol in list(cf_open_trades.keys()):
        try:
            trade = cf_open_trades[symbol]
            entry_ref = trade.get("entry_fill", trade["entry"])

            # TP (3R) filled
            if trade.get("tp_id"):
                if check_order_status(trade["tp_id"], symbol) == "FILLED":
                    if trade.get("sl_id"):
                        cancel_order(symbol, trade["sl_id"])
                    tp_fill = get_fill_price(trade["tp_id"], symbol, fallback=trade["tp"])
                    trade["pnl"]    = round((tp_fill - entry_ref) * trade["remaining_qty"], 2)
                    trade["result"] = "TP"
                    trade["label"]  = "CrashFade"
                    daily_trades.append(trade)
                    journal_closed_trade(trade)
                    del cf_open_trades[symbol]
                    continue

            # SL filled
            if trade.get("sl_id"):
                if check_order_status(trade["sl_id"], symbol) == "FILLED":
                    if trade.get("tp_id"):
                        cancel_order(symbol, trade["tp_id"])
                    sl_fill = get_fill_price(trade["sl_id"], symbol, fallback=trade["sl"])
                    trade["pnl"]    = round((sl_fill - entry_ref) * trade["remaining_qty"], 2)
                    trade["result"] = "SL"
                    trade["label"]  = "CrashFade"
                    daily_trades.append(trade)
                    journal_closed_trade(trade)
                    del cf_open_trades[symbol]
                    continue

            # ---- Liquidation detect: position gone from exchange, no TP/SL fill ----
            # Only trust the open-set when we actually fetched it this cycle (open_syms
            # is a set); if the position-fetch failed (None) we skip and try next cycle,
            # never guessing a liquidation from a failed API call.
            if open_syms is not None and symbol not in open_syms:
                if confirm_liquidated(symbol, trade):
                    journal_liquidation(trade, "CrashFade")
                    del cf_open_trades[symbol]
                    continue
                # SL/TP still on exchange -> positions call lied, position is open.

            # ---- Progress management: BE at 1R, trail from 1.5R (peak-1R) ----
            risk_dist = trade.get("risk_dist", 0)
            current   = get_current_price(symbol)
            if current > 0 and risk_dist > 0:
                favorable_r = (current - entry_ref) / risk_dist   # LONG
                if favorable_r > trade.get("peak_r", 0.0):
                    trade["peak_r"] = favorable_r
                peak_r = trade["peak_r"]

                target_stop_r = None
                if peak_r >= CF_TRAIL_START_R:
                    target_stop_r = peak_r - CF_TRAIL_GAP_R
                elif peak_r >= CF_BE_TRIGGER_R:
                    target_stop_r = 0.0

                if target_stop_r is not None:
                    cur_stop_r = trade.get("stop_r")
                    if cur_stop_r is None or target_stop_r >= cur_stop_r + CF_TRAIL_STEP_R:
                        new_sl = round(entry_ref + target_stop_r * risk_dist, 6)
                        if new_sl > trade["sl"] and new_sl < current:
                            new_id = place_sl_guarded(symbol, trade["close_side"], trade["pos_side"], new_sl, trade["remaining_qty"])
                            if new_id:
                                if trade.get("sl_id"):
                                    cancel_order(symbol, trade["sl_id"])
                                trade["sl_id"]  = new_id
                                trade["sl"]     = new_sl
                                trade["stop_r"] = target_stop_r
                                tag = "BE" if target_stop_r <= 0.0001 else f"+{target_stop_r:.2f}R"
                                print(f"[CF TRAIL] {symbol} peak {peak_r:.2f}R -> SL {new_sl} ({tag})")
                            else:
                                print(f"[CF TRAIL FAIL] {symbol} SL re-placement failed - keeping SL {trade['sl']}")

            # ---- Stagnation early-exit: flat too long, cut before the 4h TimeExit ----
            held = time.time() - trade.get("opened_ts", time.time())
            if held >= CF_STAGNATION_SECONDS and trade.get("peak_r", 0.0) < CF_STAGNATION_MIN_R:
                print(f"[CF STAGNATION] {symbol} peak {trade.get('peak_r',0):.2f}R < {CF_STAGNATION_MIN_R}R after {held/3600:.1f}h")
                close_cf_position(symbol, "Stagnation")
                continue

            # 4h max hold
            if held >= CF_MAX_HOLD_SECONDS:
                print(f"[CF MAX HOLD] {symbol}")
                close_cf_position(symbol, "TimeExit")
                continue
        except Exception as e:
            print(f"[CF TRACK ERROR] {symbol}: {e}")


def cf_diagnostic_check(breadth, gate_on, universe_n):
    print(f"[CF DIAG] breadth(smoothed,lagged)={breadth:+.3f}% | gate(<{CF_BREADTH_GATE}%)={'ON' if gate_on else 'off'} | "
          f"sample={universe_n} | open={len(cf_open_trades)} | pending={len(cf_pending)} | auto={cf_auto_trade_enabled}")


def cf_scan_loop():
    print(f"Crash Fade loop started - {CF_TIMEFRAME} | market-breadth gate <{CF_BREADTH_GATE}% -> fade 4% drops on a green candle -> dip-buy -> 3R")
    all_symbols = []
    need = CF_BREADTH_RET_BARS + CF_BREADTH_SMOOTH_BARS + CF_BREADTH_LAG_BARS + 5
    while True:
        try:
            if api_backoff_active():
                time.sleep(30)
                continue
            if not all_symbols:
                all_symbols = get_futures_symbols() or []
            universe = get_liquid_symbols(all_symbols, min_quote_vol=CF_MIN_QUOTE_VOL,
                                          max_n=CF_MAX_SYMBOLS, exclude_top_n=CF_EXCLUDE_TOP_N)
            universe = [s for s in universe if not is_tokenized(s)]

            candles_map = {}
            for sym in universe:
                if api_backoff_active():
                    break
                c = get_candles(sym, limit=need + 5, interval=CF_TIMEFRAME)
                if len(c) > 1:
                    candles_map[sym] = c[:-1]   # drop the in-progress candle
                time.sleep(0.15)

            if len(candles_map) < 10:
                print(f"[CF SCAN] only {len(candles_map)} coins with candles - skipping this pass")
                time.sleep(CF_SCAN_INTERVAL_SECONDS)
                continue

            breadth = cf_compute_breadth(candles_map)
            gate_on = breadth < CF_BREADTH_GATE
            cf_diagnostic_check(breadth, gate_on, len(candles_map))

            if gate_on:
                armed = 0
                for sym, c in candles_map.items():
                    check_cf(sym, c)
                    if sym in cf_pending:
                        armed += 1
                print(f"[CF SCAN] gate OPEN (breadth {breadth:+.2f}%) - {armed} dip-buy(s) armed")
            else:
                print(f"[CF SCAN] gate closed (breadth {breadth:+.2f}% >= {CF_BREADTH_GATE}%) - market not crashing, no fades")
        except Exception as e:
            print(f"[CF LOOP ERROR] {e}")
        time.sleep(CF_SCAN_INTERVAL_SECONDS)



# ==================== RSI REVERSION ENGINE (RSI Oversold Reversion) ====================
def rsi_in_cooldown(symbol):
    return time.time() < rsi_cooldown.get(symbol, 0)


def rsi_symbol_open(symbol):
    return any(t["symbol"] == symbol for t in rsi_open_trades.values())


def rsi_atr15(symbol):
    candles = get_candles(symbol, limit=RSI_ATR_LEN + 40, interval=RSI_ATR_TF)
    closed = candles[:-1] if len(candles) > 1 else candles
    if len(closed) < RSI_ATR_LEN + 1:
        return 0.0
    highs  = [h(c) for c in closed]
    lows   = [l(c) for c in closed]
    closes = [cl(c) for c in closed]
    return atr_series(highs, lows, closes, RSI_ATR_LEN)[-1]


def check_rsi(symbol):
    """RSI(14) < 20 on 5m AND a green candle -> buy the reversion. Deduped against
    Crash Fade so the two engines never fire on the same coin within +/-12 bars."""
    if rsi_symbol_open(symbol) or rsi_in_cooldown(symbol):
        return
    # Dedup vs Crash Fade (+/-12 bars ~ 1h): CF and RSI stay disjoint.
    last_cf = cf_last_entry_ts.get(symbol, 0)
    if last_cf and (time.time() - last_cf) < RSI_DEDUP_SECONDS:
        return
    if symbol in cf_pending or symbol in cf_open_trades:
        return

    candles = get_candles(symbol, limit=RSI_LEN + 60, interval=RSI_TIMEFRAME)
    closed = candles[:-1] if len(candles) > 1 else candles
    if len(closed) < RSI_LEN + 2:
        return
    closes = [cl(c) for c in closed]
    rsi_val = rsi_series(closes, RSI_LEN)[-1]
    last = closed[-1]
    if rsi_val >= RSI_ENTRY:
        return
    if cl(last) <= o(last):          # candle must be green (turning up)
        return

    entry = cl(last)
    atr15 = rsi_atr15(symbol)
    if atr15 <= 0:
        print(f"[RSI SKIP] {symbol} - no ATR15")
        return
    sl_price = entry - RSI_SL_ATR_MULT * atr15
    if sl_price <= 0 or entry - sl_price <= 0:
        return

    trade_status = ""
    if rsi_auto_trade_enabled and len(rsi_open_trades) >= RSI_MAX_CONCURRENT_TRADES:
        trade_status = "\nSkipped - max concurrent trades (" + str(RSI_MAX_CONCURRENT_TRADES) + ") reached"
    elif rsi_auto_trade_enabled:
        oid = place_rsi_order(symbol, entry, sl_price, atr15)
        if oid == "MARGIN_SKIP":
            trade_status = "\nSkipped - insufficient margin"
        elif oid and oid != "N/A":
            trade_status = "\nOrder: " + str(oid)
            rsi_cooldown[symbol] = time.time() + RSI_COOLDOWN_SECONDS
        else:
            trade_status = "\nOrder failed"
    else:
        trade_status = "\nAuto-trade OFF"
        rsi_cooldown[symbol] = time.time() + RSI_COOLDOWN_SECONDS

    print(f"[RSI ENTRY] {symbol} BUY @ {entry} RSI={rsi_val:.1f} SL={round(sl_price,6)}{trade_status}")
    send_tg(
        "RSI REVERSION - BUY - " + symbol + "\n------------------------------\n"
        "RSI(14): " + str(round(rsi_val, 1)) + " (<" + str(RSI_ENTRY) + ", oversold) + green candle\n"
        "Entry: " + str(round(entry, 6)) + " | SL (" + str(RSI_SL_ATR_MULT) + "x ATR15): " + str(round(sl_price, 6)) + "\n"
        "Exit: BE at " + str(RSI_BE_TRIGGER_R) + "R, then trail from " + str(RSI_TRAIL_START_R) + "R (peak-" + str(RSI_TRAIL_GAP_R) + "R) | max hold " + str(RSI_MAX_HOLD_SECONDS // 3600) + "h\n"
        "Risk: $" + str(RSI_RISK_USDT) + trade_status + "\n------------------------------\nNiti RSI Reversion"
    )


def place_rsi_order(symbol, entry, sl_price, atr15):
    """Full-size LONG, structural SL, NO exchange TP - the exit is trail-managed
    (BE at 1R, then trail from 1.5R). Mirrors the proven sizing / margin / SL guard."""
    try:
        set_leverage_api(symbol, RSI_LEVERAGE)
        precision = symbol_precision.get(symbol, 4)
        risk_dist = abs(entry - sl_price)
        if risk_dist <= 0:
            return None
        risk_qty       = RSI_RISK_USDT / risk_dist
        margin_cap_qty = (RSI_MAX_MARGIN_USDT * RSI_LEVERAGE) / entry
        total_qty      = round(min(risk_qty, margin_cap_qty), precision)
        if total_qty <= 0:
            return None
        if risk_qty > margin_cap_qty:
            print(f"[RSI SIZE CAP] {symbol} SL too wide for full risk qty, margin-capped at ${RSI_MAX_MARGIN_USDT}")
        pos_side, close_side = "LONG", "SELL"

        required_margin = total_qty * entry / RSI_LEVERAGE
        avail = get_available_margin()
        if avail is not None and avail < required_margin * 1.05:
            print(f"[RSI MARGIN SKIP] {symbol} need ~${required_margin:.2f}, available ${avail:.2f}")
            return "MARGIN_SKIP"

        order_id = place_market_order(symbol, "BUY", total_qty, pos_side)
        print(f"[RSI ORDER] {symbol} BUY qty={total_qty} risk=${RSI_RISK_USDT}: {order_id}")
        if order_id != "N/A":
            time.sleep(0.5)
            entry_fill = get_fill_price(order_id, symbol, fallback=entry)
            risk_dist  = abs(entry_fill - sl_price)
            if risk_dist <= 0:
                risk_dist = abs(entry - sl_price)

            sl_id = place_sl_guarded(symbol, close_side, pos_side, sl_price, total_qty)
            if sl_id is None:
                print(f"[RSI SL GUARD] {symbol} SL failed twice - emergency closing")
                place_market_order(symbol, close_side, total_qty, pos_side)
                send_tg(f"⚠️ RSI {symbol}: SL placement failed - position emergency-closed for safety")
                return None
            rsi_open_trades[str(order_id)] = {
                "symbol": symbol, "side": "BUY", "entry": entry, "entry_fill": entry_fill,
                "sl": sl_price, "sl_id": sl_id, "close_side": close_side, "pos_side": pos_side,
                "total_qty": total_qty, "remaining_qty": total_qty,
                "risk_dist": risk_dist, "atr_at_entry": atr15, "opened_ts": time.time(),
                "peak_r": 0.0, "be_done": False, "stop_r": None, "risk_usdt": RSI_RISK_USDT,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
        return order_id
    except Exception as e:
        print(f"[RSI ORDER ERROR] {symbol}: {e}")
        return None


def close_rsi_position(oid, reason=""):
    trade = rsi_open_trades.get(oid)
    if not trade:
        return
    try:
        symbol = trade["symbol"]
        qty    = trade.get("remaining_qty", 0)
        entry_ref  = trade.get("entry_fill", trade["entry"])
        exit_price = get_current_price(symbol)
        if qty > 0 and rsi_auto_trade_enabled:
            close_oid = place_market_order(symbol, trade["close_side"], qty, trade["pos_side"])
            if close_oid and close_oid != "N/A":
                time.sleep(0.5)
                exit_price = get_fill_price(close_oid, symbol, fallback=exit_price)
        if trade.get("sl_id"):
            cancel_order(symbol, trade["sl_id"])
        trade["pnl"]    = round((exit_price - entry_ref) * qty, 2)   # LONG only
        trade["result"] = reason
        trade["label"]  = "RSI"
        daily_trades.append(trade)
        journal_closed_trade(trade)
        rsi_open_trades.pop(oid, None)
        print(f"[RSI CLOSE] {symbol} {reason}")
    except Exception as e:
        print(f"[RSI CLOSE ERROR] {oid}: {e}")


def track_rsi_trades(open_syms=None):
    for oid in list(rsi_open_trades.keys()):
        trade = rsi_open_trades.get(oid)
        if not trade:
            continue
        try:
            symbol    = trade["symbol"]
            risk_dist = trade.get("risk_dist", 0)
            entry_ref = trade.get("entry_fill", trade["entry"])

            # SL fill first (BE / Trail / SL depending on where the stop was sitting).
            if trade.get("sl_id") and check_order_status(trade["sl_id"], symbol) == "FILLED":
                sl_fill = get_fill_price(trade["sl_id"], symbol, fallback=trade["sl"])
                stop_r  = trade.get("stop_r")
                if stop_r is None:
                    result = "SL"
                elif stop_r <= 0.0001:
                    result = "BE"
                else:
                    result = "Trail"
                trade["pnl"]    = round((sl_fill - entry_ref) * trade["remaining_qty"], 2)
                trade["result"] = result
                trade["label"]  = "RSI"
                daily_trades.append(trade)
                journal_closed_trade(trade)
                rsi_open_trades.pop(oid, None)
                continue

            # ---- Liquidation detect: position gone from exchange, SL didn't fill ----
            if open_syms is not None and symbol not in open_syms:
                if confirm_liquidated(symbol, trade):
                    journal_liquidation(trade, "RSI")
                    rsi_open_trades.pop(oid, None)
                    continue
                # SL still on exchange -> positions call lied, position is open.

            current = get_current_price(symbol)
            if current > 0 and risk_dist > 0:
                favorable_r = (current - entry_ref) / risk_dist   # LONG
                if favorable_r > trade.get("peak_r", 0.0):
                    trade["peak_r"] = favorable_r
                peak_r = trade["peak_r"]

                # Target stop level in R: trail from 1.5R (peak-1R); BE covers 1R-1.5R.
                target_stop_r = None
                if peak_r >= RSI_TRAIL_START_R:
                    target_stop_r = peak_r - RSI_TRAIL_GAP_R
                elif peak_r >= RSI_BE_TRIGGER_R:
                    target_stop_r = 0.0

                if target_stop_r is not None:
                    cur_stop_r = trade.get("stop_r")
                    if cur_stop_r is None or target_stop_r >= cur_stop_r + RSI_TRAIL_STEP_R:
                        new_sl = round(entry_ref + target_stop_r * risk_dist, 6)
                        # ratchet UP only, never place a sell-stop above market
                        if new_sl > trade["sl"] and new_sl < current:
                            new_id = place_sl_guarded(symbol, trade["close_side"], trade["pos_side"], new_sl, trade["remaining_qty"])
                            if new_id:
                                if trade.get("sl_id"):
                                    cancel_order(symbol, trade["sl_id"])
                                trade["sl_id"]  = new_id
                                trade["sl"]     = new_sl
                                trade["stop_r"] = target_stop_r
                                tag = "BE" if target_stop_r <= 0.0001 else f"+{target_stop_r:.2f}R"
                                print(f"[RSI TRAIL] {symbol} peak {peak_r:.2f}R -> SL {new_sl} ({tag})")
                            else:
                                print(f"[RSI TRAIL FAIL] {symbol} SL re-placement failed - keeping SL {trade['sl']}")

            # ---- Stagnation early-exit: flat too long, cut before the 4h TimeExit ----
            held = time.time() - trade.get("opened_ts", time.time())
            if held >= RSI_STAGNATION_SECONDS and trade.get("peak_r", 0.0) < RSI_STAGNATION_MIN_R:
                print(f"[RSI STAGNATION] {symbol} peak {trade.get('peak_r',0):.2f}R < {RSI_STAGNATION_MIN_R}R after {held/3600:.1f}h")
                close_rsi_position(oid, "Stagnation")
                continue

            # 4h max hold
            if held >= RSI_MAX_HOLD_SECONDS:
                print(f"[RSI MAX HOLD] {symbol}")
                close_rsi_position(oid, "TimeExit")
                continue
        except Exception as e:
            print(f"[RSI TRACK ERROR] {oid}: {e}")


def rsi_diagnostic_check():
    try:
        candles = get_candles("BTC-USDT", limit=RSI_LEN + 60, interval=RSI_TIMEFRAME)
        closed = candles[:-1] if len(candles) > 1 else candles
        if len(closed) < RSI_LEN + 2:
            print(f"[RSI DIAG] BTC-USDT - only {len(closed)} closed candles")
            return
        closes = [cl(c) for c in closed]
        rv = rsi_series(closes, RSI_LEN)[-1]
        print(f"[RSI DIAG] BTC-USDT RSI(14)={rv:.1f} close={closes[-1]} (pipeline alive)")
    except Exception as e:
        print(f"[RSI DIAG ERROR] {e}")


def rsi_scan_loop():
    print(f"RSI Reversion loop started - {RSI_TIMEFRAME} | RSI(14)<{RSI_ENTRY} + green candle -> BE1R+trail-from-1.5R")
    all_symbols = []
    while True:
        try:
            if api_backoff_active():
                time.sleep(30)
                continue
            if not all_symbols:
                all_symbols = get_futures_symbols() or []
            universe = get_liquid_symbols(all_symbols, min_quote_vol=RSI_MIN_QUOTE_VOL,
                                          max_n=RSI_MAX_SYMBOLS, exclude_top_n=RSI_EXCLUDE_TOP_N)
            universe = [s for s in universe if not is_tokenized(s)]
            print(f"[RSI SCAN] scanning {len(universe)} pairs (small caps, majors excluded)")
            rsi_diagnostic_check()
            for sym in universe:
                if api_backoff_active():
                    break
                check_rsi(sym)
                time.sleep(0.15)
            print(f"[RSI SCAN] done. open={len(rsi_open_trades)} | sleeping {RSI_SCAN_INTERVAL_SECONDS}s")
        except Exception as e:
            print(f"[RSI LOOP ERROR] {e}")
        time.sleep(RSI_SCAN_INTERVAL_SECONDS)

# ==================== TIGHT 1: DORMANT AWAKENING (added 2026-07-19) ====================
# Catches BANK-USDT-class multi-day runners at the START of the move, which Tight 2
# structurally cannot: Tight 2's 3-day average baseline gets inflated by the pump
# itself, so by day 2-3 of a run the 20x spike test is unhittable. Tight 1 instead
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
T3_DORMANCY_DAYS          = int(os.environ.get("T3_DORMANCY_DAYS", 5))
T3_DORMANCY_RANGE_PCT     = float(os.environ.get("T3_DORMANCY_RANGE_PCT", 25.0)) # total close band width = +/-15% around mid
T3_DORMANCY_VOL_SPIKE_MAX = float(os.environ.get("T3_DORMANCY_VOL_SPIKE_MAX", 3.0))   # any day >3x median inside the window = already awakened earlier, not dormant
T3_AWAKE_VOL_MULT         = float(os.environ.get("T3_AWAKE_VOL_MULT", 3.0))      # 2026-07-30: 5x->3x, backtest-verified 9.4 trades/wk +$391/8mo (was 4.7/wk $213), holdout-OK both sets. Fixes "no trades for a week". If it spams try 4x; 2.5x gives more but clusters max2 slots.
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
T3_RISK_USDT              = 5.0    # 2026-08-05: 2->5 per Faisal. $100 acct, ~5% risk/trade. margin follows ~$6-10 (SL at structure, NOT moved in to inflate margin).
T3_LEVERAGE               = int(os.environ.get("T3_LEVERAGE", 10))   # 10x not 20x - $300k-liquidity pairs, wider structural SLs
T3_MAX_MARGIN_USDT        = 60.0   # 2026-08-05: 25->60 so a $5-risk trade with a wide (structure) SL is never shrunk by the margin cap below its intended risk
T3_ATR_LEN                = 14
T3_SL_ATR_BUFFER_MULT     = 0.3
T3_CHASE_SL_ATR_MULT      = float(os.environ.get("T3_CHASE_SL_ATR_MULT", 1.5))   # 2026-07-30 chase: SL = entry -/+ 1.5xATR(15m). Tune-swept best (1.0/2.0/2.5 all worse), holdout-OK.
T3_FIXED_TP_R             = float(os.environ.get("T3_FIXED_TP_R", 8.0))          # 2026-07-30: visible reduce-only TP at 8R. no-TP=$267 vs 8R=$213/8mo; the $54 buys a chart-visible TP line so drawdown is tolerable. Trail (BE2R/from4R) still the primary exit; 8R is a far ceiling only mega-runners hit.
T3_SLIP_ALERT_PCT         = float(os.environ.get("T3_SLIP_ALERT_PCT", 0.3))      # 2026-07-30: log+alert only (NO auto-skip yet) if entry fill is >this% from signal px. Collect 2-3wk live slip, then decide a skip threshold.

# ==================== TIGHT 2 (Trapped-Block Fade, SHORT) ====================
# 2026-08-02: replaces retired RSI Reversion on the /start /stop commands.
# Concept (only survivor of 4 discretionary docs): SHORT a prior high-volume
# "trapped block" resistance when price returns up into it on declining volume
# (demand exhaustion). Backtest (15m 8mo, band5/dump18, TP4/buf1.0/exh0.8):
# max2 win 52% $144 DD -$8; holdout A+1.51/B+1.09; random -0.445; 8/9 months+.
T2_RET_BAND_PCT           = float(os.environ.get("T2_RET_BAND_PCT", 6.0))    # 2026-08-04: 5->6, wider retest zone raises win 50->54% (trapped selling starts before price exactly reaches level)
T2_DUMP_PCT               = float(os.environ.get("T2_DUMP_PCT", 18.0))       # block qualifies only if its close later fell >= this % within lookahead
T2_DUMP_LOOKAHEAD_DAYS    = int(os.environ.get("T2_DUMP_LOOKAHEAD_DAYS", 5))
T2_VOL_SPIKE              = float(os.environ.get("T2_VOL_SPIKE", 1.8))       # 2026-08-04: 3->1.8, 3x too strict (only 12 live blocks); 1.8x ~3x more blocks, edge intact, fixes no-trades
T2_VOL_EXHAUSTION         = float(os.environ.get("T2_VOL_EXHAUSTION", 0))    # 2026-08-04: DISABLED (0=off). exh0.8 killed 87% of signals (only starved, not helped) - exhOFF has more trades, higher win, better holdout.
T2_SL_ATR_BUF             = float(os.environ.get("T2_SL_ATR_BUF", 0.5))      # 2026-08-04: 1.0->0.5, SL nearer level = smaller R = more R per move ($147->$190)
T2_TP_R                   = float(os.environ.get("T2_TP_R", 4.0))            # fixed reduce-only TP at 4R (win-neutral vs 3R, +PnL)
T2_BLOCK_MAX_AGE_DAYS     = int(os.environ.get("T2_BLOCK_MAX_AGE_DAYS", 10)) # 2026-08-04 biggest refinement: only fade blocks formed within last 10d. Old blocks' trapped holders already exited (level dead); fresh blocks still have active trapped selling. win 54->62%, $222->$252, DD-$11->-$8.
T2_ATR_LEN                = 14
T2_DORMANCY_DAYS          = 5      # trailing window for the block's baseline median volume
T2_BLOCK_SCAN_SECONDS     = int(os.environ.get("T2_BLOCK_SCAN_SECONDS", 14400))  # rebuild block map every 4h (1 daily request per symbol)
T2_ENTRY_CHECK_SECONDS    = int(os.environ.get("T2_ENTRY_CHECK_SECONDS", 300))
T2_COOLDOWN_SECONDS       = int(os.environ.get("T2_COOLDOWN_SECONDS", 86400))    # 1 day per coin after a fade fires
T2_RISK_USDT              = 5.0    # 2026-08-05: 2->5 per Faisal (~5% risk on $100). SL stays at block level; size scales up, margin follows (~$6-10 typical).
T2_LEVERAGE               = int(os.environ.get("T2_LEVERAGE", 10))
T2_MAX_MARGIN_USDT        = 60.0   # 2026-08-05: 25->60 so $5-risk + wide structure SL is never capped below intended risk
T2_MAX_SYMBOLS            = int(os.environ.get("T2_MAX_SYMBOLS", 250))
T2_EXCLUDE_TOP_N          = int(os.environ.get("T2_EXCLUDE_TOP_N", 0))     # 2026-08-04: 75->0, INCLUDE majors - big coins form clean trapped-blocks too, excluding them halved signals (block-syms 51->81)

# Shared T1+T2 concurrency cap: on a $100 account both engines TOGETHER = 2 open.
# (Backtest: shared-max2 $534 DD-$33 is the best risk-adj; raise once balance grows.)
SHARED_MAX_CONCURRENT     = int(os.environ.get("SHARED_MAX_CONCURRENT", 2))

t2_auto_trade_enabled = AUTO_RESUME_ON_START
t2_blocks       = {}    # symbol -> list of (resistance_level, formed_day_ms)
t2_open_trades  = {}    # order_id -> open Tight 2 fade trade
t2_last_fire    = {}    # symbol -> ts of last fired fade (cooldown)

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
            # CHASE (2026-07-30): no pullback wait. Enter at the CLOSE of the first
            # 15m candle that closes after the awakening, SL = T3_CHASE_SL_ATR_MULT x
            # ATR(15m). Backtest: chase +0.796R vs pullback +0.283R, holdout-verified.
            # Only 15m candles that CLOSE after this moment count.
            "last_processed_time": int(time.time() * 1000),
            "vol_ratio": ratio,
            "dormant_high": info["dormant_high"], "dormant_low": info["dormant_low"],
        }
        t3_watchlist.pop(symbol, None)
        print(f"[T3 AWAKENING] {symbol} {side} vol={ratio:.1f}x dormant median | 1h close {h1_close} outside range ({info['dormant_low']}-{info['dormant_high']})")
        send_tg(
            "TIGHT 1 AWAKENING - " + side + " - " + symbol + "\n------------------------------\n"
            "Dormant " + str(T3_DORMANCY_DAYS) + "d range: " + str(info["dormant_low"]) + " - " + str(info["dormant_high"]) + "\n"
            "Today vol: " + str(round(ratio, 1)) + "x dormant median (need " + str(T3_AWAKE_VOL_MULT) + "x) | 1h close: " + str(h1_close) + "\n"
            "Watching 15m for a pullback entry (window " + str(T3_SETUP_EXPIRY_SECONDS // 3600) + "h)"
            "\n------------------------------\nNiti Tight 1"
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
    # NOTE: chase SL is a fixed 1.5xATR, so the old "entry-to-SL too extended" guard
    # can never trip and is removed. Oversized-candle skip was backtest-REJECTED
    # (every candle>NxATR filter cut PnL - big breakout candles ARE the runners).
    sl = round(sl, 6)
    trade_status = ""
    if t3_auto_trade_enabled and t2_total_open() >= SHARED_MAX_CONCURRENT:
        trade_status = "\nSkipped - shared T1+T2 cap (" + str(SHARED_MAX_CONCURRENT) + ") reached"
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
        "TIGHT 1 ENTRY - " + side + " - " + symbol + "\n------------------------------\n"
        "Entry: " + str(round(entry_px, 6)) + " | SL: " + str(sl) + " | Risk: $" + str(T3_RISK_USDT) + " | Lev: " + str(T3_LEVERAGE) + "x\n"
        "Chase entry (breakout candle) | TP " + str(T3_FIXED_TP_R) + "R visible | BE " + str(T3_BE_TRIGGER_R) + "R, trail from " + str(T3_TRAIL_START_R) + "R at peak-" + str(T3_TRAIL_GAP_R) + "R\n"
        "Awakening vol: " + str(round(st.get("vol_ratio", 0), 1)) + "x dormant median" +
        trade_status + "\n------------------------------\nNiti Tight 1"
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
        send_tg("TIGHT 1 EXPIRED - " + symbol + "\nNo clean pullback entry within " + str(T3_SETUP_EXPIRY_SECONDS // 3600) + "h of the awakening - trade skipped")
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
        # CHASE: enter at the close of the FIRST 15m candle that closes after the
        # awakening. No pullback wait, no trigger/peak machine. SL = 1.5xATR(15m).
        for c in [x for x in confirmed if int(x["time"]) > last_t]:
            st["last_processed_time"] = int(c["time"])
            if atr_now <= 0:
                continue
            entry_px = cl(c)
            if side == "BUY":
                st["setup_sl"] = entry_px - atr_now * T3_CHASE_SL_ATR_MULT
            else:
                st["setup_sl"] = entry_px + atr_now * T3_CHASE_SL_ATR_MULT
            t3_fire_entry(symbol, st, entry_px, atr_now)
            return
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

            # ---- Slippage LOG + ALERT (no auto-skip yet - collecting live slip data) ----
            slip_pct = abs(entry_fill - entry) / entry * 100 if entry > 0 else 0.0
            print(f"[T3 SLIP] {symbol} signal={entry} fill={entry_fill} slip={slip_pct:.3f}%")
            if slip_pct > T3_SLIP_ALERT_PCT:
                send_tg(f"⚠️ TIGHT 1 {symbol}: chase fill slipped {slip_pct:.2f}% (signal {entry} -> fill {entry_fill}). Trade kept - logging for slip review.")

            risk_dist = abs(entry_fill - sl)
            if risk_dist <= 0:
                risk_dist = abs(entry - sl)
            sl_id = place_sl_guarded(symbol, close_side, pos_side, sl, total_qty)
            if sl_id is None:
                print(f"[T3 SL GUARD] {symbol} SL placement failed twice - emergency closing position")
                place_market_order(symbol, close_side, total_qty, pos_side)
                send_tg(f"⚠️ TIGHT 1 {symbol}: SL placement failed - position emergency-closed for safety")
                return None

            # ---- Visible 8R reduce-only TP (far ceiling; trail is the primary exit) ----
            if side == "BUY":
                tp_price = round(entry_fill + risk_dist * T3_FIXED_TP_R, 6)
            else:
                tp_price = round(entry_fill - risk_dist * T3_FIXED_TP_R, 6)
            tp_id = place_tp_guarded(symbol, close_side, pos_side, tp_price, total_qty, label="TP-" + str(T3_FIXED_TP_R) + "R")

            t3_open_trades[str(order_id)] = {
                "symbol": symbol, "side": side, "entry": entry, "entry_fill": entry_fill,
                "sl": sl, "sl_id": sl_id, "tp": tp_price, "tp_id": tp_id,
                "total_qty": total_qty, "close_side": close_side, "pos_side": pos_side,
                "risk_dist": risk_dist, "be_done": False, "be_price": None,
                "trailed": False, "peak_r": 0.0, "risk_usdt": T3_RISK_USDT,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
        return order_id
    except Exception as e:
        print(f"[T3 ORDER ERROR] {symbol}: {e}")
        return None


def track_t3_trades(open_syms=None):
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

            # ---- 8R TP fill = trade over ----
            tp_status = check_order_status(trade["tp_id"], symbol) if trade.get("tp_id") and trade["tp_id"] != "N/A" else ""
            if tp_status == "FILLED":
                tp_fill = get_fill_price(trade["tp_id"], symbol, fallback=trade["tp"])
                if trade["side"] == "BUY":
                    leg_pnl = (tp_fill - entry_ref) * trade["total_qty"]
                else:
                    leg_pnl = (entry_ref - tp_fill) * trade["total_qty"]
                if trade.get("sl_id"):
                    cancel_order(symbol, trade["sl_id"])   # cancel orphan SL
                trade["pnl"]    = round(leg_pnl, 2)
                trade["result"] = "TP"
                trade["exit_r"] = round(leg_pnl / trade.get("risk_usdt", T3_RISK_USDT), 2) if trade.get("risk_usdt") else round(T3_FIXED_TP_R, 2)
                trade["label"]  = "Tight 1"
                daily_trades.append(trade)
                journal_closed_trade(trade)
                t3_open_trades.pop(oid, None)
                print(f"[T3 CLOSE] {symbol} TP pnl={trade['pnl']}")
                continue

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
                if trade.get("tp_id") and trade["tp_id"] != "N/A":
                    cancel_order(symbol, trade["tp_id"])   # cancel orphan TP
                trade["pnl"]    = round(leg_pnl, 2)
                trade["result"] = result
                trade["exit_r"] = round(leg_pnl / trade.get("risk_usdt", T3_RISK_USDT), 2) if trade.get("risk_usdt") else 0.0
                trade["label"]  = "Tight 1"
                daily_trades.append(trade)
                journal_closed_trade(trade)
                t3_open_trades.pop(oid, None)
                print(f"[T3 CLOSE] {symbol} {result} pnl={trade['pnl']} R={trade['exit_r']}")
                continue

            # ---- Liquidation detect: position gone from exchange, SL didn't fill ----
            if open_syms is not None and symbol not in open_syms:
                if confirm_liquidated(symbol, trade):
                    journal_liquidation(trade, "Tight 1")
                    t3_open_trades.pop(oid, None)
                    continue
                # SL still on exchange -> positions call lied, position is open.

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
                    send_tg("TIGHT 1 " + symbol + " - " + str(round(trade['peak_r'], 1)) + "R reached, SL moved to breakeven (" + str(entry_ref) + "). Free trade - trail starts at " + str(T3_TRAIL_START_R) + "R.")
                else:
                    print(f"[T3 BE FAIL] {symbol} BE SL re-placement failed twice - keeping original SL {trade['sl']}")
                    if not trade.get("sl_move_alerted"):
                        trade["sl_move_alerted"] = True
                        send_tg(f"⚠️ TIGHT 1 {symbol}: could not move SL to breakeven ({entry_ref}) - original SL {trade['sl']} still active. Consider moving it manually.")

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
    """Tight 1 main loop. Internal timers: dormancy rescan every 4h, awakening
    check on the watchlist every 5 min, awakened symbols processed every pass
    (60s). Fully respects the global API backoff."""
    print("Tight 1 loop started - Dormant Awakening (dormancy -> 3x median awakening -> CHASE entry -> 8R TP + peak-2R trail)")
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
# ==================== END TIGHT 1 ====================


# ==================== TIGHT 2 (Trapped-Block Fade) ====================
def t2_total_open():
    """Shared open count across BOTH engines for the shared concurrency cap."""
    return len(t3_open_trades) + len(t2_open_trades)

def t2_in_cooldown(symbol):
    last = t2_last_fire.get(symbol, 0)
    return (time.time() - last) < T2_COOLDOWN_SECONDS

def t2_symbol_has_open_trade(symbol):
    return any(t["symbol"] == symbol for t in t2_open_trades.values())

def t2_build_blocks(all_symbols):
    """For each symbol, build the list of trapped-block resistance levels from
    DAILY candles: a day whose volume >= T2_VOL_SPIKE x trailing-5d median and
    whose close then FELL >= T2_DUMP_PCT within the next T2_DUMP_LOOKAHEAD_DAYS.
    That day's HIGH is the resistance (trapped longs). Rebuilt every 4h."""
    syms = get_liquid_symbols(all_symbols, min_quote_vol=T3_MIN_QUOTE_VOL, max_n=T2_MAX_SYMBOLS, exclude_top_n=T2_EXCLUDE_TOP_N)
    new_blocks = {}
    for sym in syms:
        if api_backoff_active():
            break
        try:
            daily = get_candles(sym, limit=60, interval="1d")
            if not daily or len(daily) < T2_DORMANCY_DAYS + T2_DUMP_LOOKAHEAD_DAYS + 2:
                continue
            highs  = [h(c)  for c in daily]
            lows   = [l(c)  for c in daily]
            closes = [cl(c) for c in daily]
            vols   = [v(c)  for c in daily]
            times  = [int(c["time"]) for c in daily]
            blocks = []
            for i in range(T2_DORMANCY_DAYS, len(daily) - T2_DUMP_LOOKAHEAD_DAYS):
                window = vols[i - T2_DORMANCY_DAYS:i]
                med = sorted(window)[len(window) // 2] if window else 0
                if med <= 0 or vols[i] < T2_VOL_SPIKE * med:
                    continue
                fut_low = min(lows[i + 1:i + 1 + T2_DUMP_LOOKAHEAD_DAYS])
                if closes[i] > 0 and (closes[i] - fut_low) / closes[i] * 100 >= T2_DUMP_PCT:
                    blocks.append((highs[i], times[i]))
            if blocks:
                new_blocks[sym] = blocks
            time.sleep(0.2)
        except Exception as e:
            print(f"[T2 BLOCK {sym}] error: {e}")
    t2_blocks.clear()
    t2_blocks.update(new_blocks)
    print(f"[T2 BLOCKS] built for {len(t2_blocks)} symbols")

def t2_check_entry(symbol, blocks):
    """On 15m: if price is returning UP into a past block level (within band) from
    below, on declining volume (< T2_VOL_EXHAUSTION x median-of-last-20), SHORT."""
    try:
        candles = get_candles(symbol, limit=40, interval="15m")
        if len(candles) < 22:
            return
        confirmed = candles[:-1]
        c_last = confirmed[-1]
        price  = cl(c_last)
        hi     = h(c_last)
        cur_vol = v(c_last)
        highs  = [h(x)  for x in confirmed]
        lows   = [l(x)  for x in confirmed]
        closes = [cl(x) for x in confirmed]
        vols   = [v(x)  for x in confirmed]
        atr_now = atr_series(highs, lows, closes, T2_ATR_LEN)[-1]
        if atr_now <= 0:
            return
        vol_window = vols[-21:-1] if len(vols) >= 21 else vols[:-1]
        vmed = sorted(vol_window)[len(vol_window) // 2] if vol_window else cur_vol
        if T2_VOL_EXHAUSTION > 0 and cur_vol > T2_VOL_EXHAUSTION * vmed:   # 0 = filter disabled
            return
        now_ms = int(c_last["time"])
        max_age_ms = T2_BLOCK_MAX_AGE_DAYS * 86400000
        for lvl, formed_ms in blocks:
            if formed_ms >= now_ms:
                continue
            if T2_BLOCK_MAX_AGE_DAYS > 0 and (now_ms - formed_ms) > max_age_ms:
                continue   # stale block - trapped holders already exited, level dead
            # price returning up INTO the level from below (band around lvl), close still below
            if price < lvl and hi >= lvl * (1 - T2_RET_BAND_PCT / 100) and hi <= lvl * (1 + T2_RET_BAND_PCT / 100):
                sl = round(lvl + atr_now * T2_SL_ATR_BUF, 6)
                if sl <= price:
                    continue
                t2_fire_entry(symbol, price, sl)
                return
    except Exception as e:
        print(f"[T2 ENTRY {symbol}] error: {e}")

def t2_fire_entry(symbol, entry_px, sl):
    if t2_symbol_has_open_trade(symbol):
        return
    risk = sl - entry_px       # short: SL above entry
    if risk <= 0:
        return
    send_tg(
        "TIGHT 2 SHORT - " + symbol + "\n"
        "Entry: " + str(round(entry_px, 6)) + " | SL: " + str(sl) + " | Risk: $" + str(T2_RISK_USDT) + " | Lev: " + str(T2_LEVERAGE) + "x\n"
        "Trapped-block fade | TP " + str(T2_TP_R) + "R\n"
    )
    place_t2_order(symbol, entry_px, sl)
    t2_last_fire[symbol] = time.time()

def place_t2_order(symbol, entry, sl):
    try:
        set_leverage_api(symbol, T2_LEVERAGE)
        precision = symbol_precision.get(symbol, 4)
        risk_dist = abs(sl - entry)
        if risk_dist <= 0:
            return None
        risk_qty       = T2_RISK_USDT / risk_dist
        margin_cap_qty = (T2_MAX_MARGIN_USDT * T2_LEVERAGE) / entry
        qty = round(min(risk_qty, margin_cap_qty), precision)
        if qty <= 0:
            return None
        side, pos_side, close_side = "SELL", "SHORT", "BUY"

        required_margin = qty * entry / T2_LEVERAGE
        avail = get_available_margin()
        if avail is not None and avail < required_margin * 1.05:
            print(f"[T2 MARGIN SKIP] {symbol} need ~${required_margin:.2f}, available ${avail:.2f}")
            return "MARGIN_SKIP"

        order_id = place_market_order(symbol, side, qty, pos_side)
        print(f"[T2 ORDER] {symbol} SHORT qty={qty} risk=${T2_RISK_USDT}: {order_id}")
        if order_id != "N/A" and order_id != "MARGIN_SKIP":
            time.sleep(0.5)
            entry_fill = get_fill_price(order_id, symbol, fallback=entry)
            slip_pct = abs(entry_fill - entry) / entry * 100 if entry > 0 else 0.0
            print(f"[T2 SLIP] {symbol} signal={entry} fill={entry_fill} slip={slip_pct:.3f}%")
            if slip_pct > T3_SLIP_ALERT_PCT:
                send_tg(f"⚠️ TIGHT 2 {symbol}: fill slipped {slip_pct:.2f}% (signal {entry} -> fill {entry_fill}). Kept - logging.")
            rd = abs(sl - entry_fill)
            if rd <= 0:
                rd = risk_dist
            sl_id = place_sl_guarded(symbol, close_side, pos_side, sl, qty)
            if sl_id is None:
                print(f"[T2 SL GUARD] {symbol} SL failed twice - emergency close")
                place_market_order(symbol, close_side, qty, pos_side)
                send_tg(f"⚠️ TIGHT 2 {symbol}: SL placement failed - position emergency-closed")
                return None
            tp_price = round(entry_fill - rd * T2_TP_R, 6)   # short TP below entry
            tp_id = place_tp_guarded(symbol, close_side, pos_side, tp_price, qty, label="TP-" + str(T2_TP_R) + "R")
            t2_open_trades[str(order_id)] = {
                "symbol": symbol, "side": side, "entry": entry, "entry_fill": entry_fill,
                "sl": sl, "sl_id": sl_id, "tp": tp_price, "tp_id": tp_id,
                "total_qty": qty, "close_side": close_side, "pos_side": pos_side,
                "risk_dist": rd, "risk_usdt": T2_RISK_USDT,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
        return order_id
    except Exception as e:
        print(f"[T2 ORDER ERROR] {symbol}: {e}")
        return None

def track_t2_trades(open_syms=None):
    for oid in list(t2_open_trades.keys()):
        trade = t2_open_trades.get(oid)
        if not trade:
            continue
        symbol = trade["symbol"]
        entry_ref = trade.get("entry_fill", trade["entry"])
        try:
            # ---- TP fill ----
            tp_status = check_order_status(trade["tp_id"], symbol) if trade.get("tp_id") and trade["tp_id"] != "N/A" else ""
            if tp_status == "FILLED":
                tp_fill = get_fill_price(trade["tp_id"], symbol, fallback=trade["tp"])
                leg_pnl = (entry_ref - tp_fill) * trade["total_qty"]     # short
                if trade.get("sl_id"):
                    cancel_order(symbol, trade["sl_id"])
                trade["pnl"]    = round(leg_pnl, 2)
                trade["result"] = "TP"
                trade["exit_r"] = round(leg_pnl / trade.get("risk_usdt", T2_RISK_USDT), 2) if trade.get("risk_usdt") else round(T2_TP_R, 2)
                trade["label"]  = "Tight 2"
                daily_trades.append(trade)
                journal_closed_trade(trade)
                t2_open_trades.pop(oid, None)
                print(f"[T2 CLOSE] {symbol} TP pnl={trade['pnl']}")
                continue
            # ---- SL fill ----
            sl_status = check_order_status(trade["sl_id"], symbol) if trade.get("sl_id") else ""
            if sl_status == "FILLED":
                sl_fill = get_fill_price(trade["sl_id"], symbol, fallback=trade["sl"])
                leg_pnl = (entry_ref - sl_fill) * trade["total_qty"]     # short
                if trade.get("tp_id") and trade["tp_id"] != "N/A":
                    cancel_order(symbol, trade["tp_id"])
                trade["pnl"]    = round(leg_pnl, 2)
                trade["result"] = "SL"
                trade["exit_r"] = round(leg_pnl / trade.get("risk_usdt", T2_RISK_USDT), 2) if trade.get("risk_usdt") else 0.0
                trade["label"]  = "Tight 2"
                daily_trades.append(trade)
                journal_closed_trade(trade)
                t2_open_trades.pop(oid, None)
                print(f"[T2 CLOSE] {symbol} SL pnl={trade['pnl']} R={trade['exit_r']}")
                continue
            # ---- liquidation (position gone, no TP/SL fill) ----
            if open_syms is not None and symbol not in open_syms:
                if confirm_liquidated(symbol, trade):
                    journal_liquidation(trade, "Tight 2")
                    t2_open_trades.pop(oid, None)
                    continue
        except Exception as e:
            print(f"[T2 TRACK {symbol}] error: {e}")

def t2_loop():
    print("Tight 2 loop started - Trapped-Block Fade (short: block map every 4h -> return-into-level + volume-exhaustion -> 4R TP)")
    all_symbols = []
    last_block  = 0.0
    last_entry  = 0.0
    while True:
        try:
            if api_backoff_active():
                time.sleep(30)
                continue
            if time.time() - last_block >= T2_BLOCK_SCAN_SECONDS or last_block == 0:
                if not all_symbols:
                    all_symbols = get_futures_symbols() or []
                t2_build_blocks(all_symbols)
                last_block = time.time()

            if time.time() - last_entry >= T2_ENTRY_CHECK_SECONDS:
                checked = 0
                for sym, blocks in list(t2_blocks.items()):
                    if api_backoff_active():
                        break
                    if not t2_auto_trade_enabled:
                        break
                    if t2_total_open() >= SHARED_MAX_CONCURRENT:   # shared T1+T2 cap
                        break
                    if t2_symbol_has_open_trade(sym) or t2_in_cooldown(sym):
                        continue
                    t2_check_entry(sym, blocks)
                    checked += 1
                    time.sleep(0.2)
                last_entry = time.time()
                print(f"[T2 SCAN] blocks={len(t2_blocks)} checked={checked} open={len(t2_open_trades)} auto={t2_auto_trade_enabled}")
        except Exception as e:
            print(f"[T2 LOOP ERROR] {e}")
        time.sleep(60)
# ==================== END TIGHT 2 ====================
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



# ==================== TRAILING / MANAGEMENT LOOP ====================
def trailing_loop():
    while True:
        try:
            if cf_pending:
                cf_check_pending()

            # ---- Liquidation detection (2026-07-29, refined) ----
            # ONE positions fetch per cycle, shared by all three trackers -> the only
            # extra API call, replaces any per-position polling, so it does NOT worsen
            # the 109429 rate limit. `fetch_ok` tells trackers whether the call itself
            # succeeded. A symbol MISSING from open_syms is only a liquidation SUSPECT;
            # the tracker then double-checks that symbol's open orders (confirm_liquidated)
            # before journaling. That verify step removes the old single-open-trade blind
            # spot: even if this position was the only one open (so positions == []), the
            # tracker still checks whether its SL/TP survived on the exchange. If the
            # fetch itself errored we pass open_syms=None so trackers skip entirely.
            open_syms = None
            have_open = bool(cf_open_trades or rsi_open_trades or t3_open_trades or t2_open_trades)
            if have_open:
                positions = get_open_positions()
                # get_open_positions returns [] on genuine flat AND on API failure. We
                # treat [] as a valid "nothing open" set here; the per-symbol open-orders
                # verify inside each tracker is what actually guards against false flags,
                # so an API glitch that also drops the open-orders call cannot liquidate.
                open_syms = {p["symbol"] for p in positions}

            if cf_open_trades:
                track_cf_trades(open_syms)
            if rsi_open_trades:
                track_rsi_trades(open_syms)
            if t3_open_trades:
                track_t3_trades(open_syms)
            if t2_open_trades:
                track_t2_trades(open_syms)
            send_daily_summary()
        except Exception as e:
            print(f"[TRAIL LOOP ERROR] {e}")
        time.sleep(30)


# ==================== TELEGRAM COMMANDS ====================
def handle_telegram_commands():
    global rsi_auto_trade_enabled, cf_auto_trade_enabled, t3_auto_trade_enabled, t2_auto_trade_enabled
    offset = None
    # Discard any stale backlog on startup so an old /start can't silently flip
    # auto-trade ON after a redeploy.
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
                # ---- Tight 2 Trapped-Block Fade: /t2_start /t2_stop (/start /stop alias) ----
                if text in ("/t2_start", "/start"):
                    t2_auto_trade_enabled = True
                    send_tg("Tight 2 (Trapped-Block Fade) Auto-trade ON.")
                elif text in ("/t2_stop", "/stop"):
                    t2_auto_trade_enabled = False
                    send_tg("Tight 2 (Trapped-Block Fade) Auto-trade OFF.")
                # ---- Tight 1 Dormant Awakening (chase): /t1_start /t1_stop ----
                elif text == "/t1_start":
                    t3_auto_trade_enabled = True
                    send_tg("Tight 1 (Dormant Awakening) Auto-trade ON.")
                elif text == "/t1_stop":
                    t3_auto_trade_enabled = False
                    send_tg("Tight 1 (Dormant Awakening) Auto-trade OFF.")
                # ---- Crash Fade: /fast_start /fast_stop ----
                elif text == "/fast_start":
                    cf_auto_trade_enabled = True
                    send_tg("Crash Fade Auto-trade ON.")
                elif text == "/fast_stop":
                    cf_auto_trade_enabled = False
                    send_tg("Crash Fade Auto-trade OFF.")
                # ---- /status : everything at a glance ----
                elif text == "/status":
                    backoff = ""
                    if api_backoff_active():
                        backoff = f"\nAPI BACKOFF ACTIVE - {int(_api_backoff_until - time.time())}s remaining"
                    send_tg(
                        "===== NITI BOT STATUS =====\n"
                        "Tight 1 (Awakening): " + ("ON" if t3_auto_trade_enabled else "OFF") +
                        " | Open: " + str(len(t3_open_trades)) + " | Watchlist: " + str(len(t3_watchlist)) + " | Awakened: " + str(len(t3_watch)) + "\n"
                        "Tight 2 (Fade): " + ("ON" if t2_auto_trade_enabled else "OFF") +
                        " | Open: " + str(len(t2_open_trades)) + " | Blocks: " + str(len(t2_blocks)) + "\n"
                        "Crash Fade: " + ("ON" if cf_auto_trade_enabled else "OFF") +
                        " | Open: " + str(len(cf_open_trades)) + " | Pending: " + str(len(cf_pending)) + "\n"
                        "Shared cap (T1+T2): " + str(t2_total_open()) + "/" + str(SHARED_MAX_CONCURRENT) + backoff
                    )
                # ---- per-strategy detail ----
                elif text == "/t2_status":
                    lines2 = ("Tight 2 (Fade): " + ("ON" if t2_auto_trade_enabled else "OFF") +
                              " | Blocks: " + str(len(t2_blocks)) + " | Open: " + str(len(t2_open_trades)))
                    for _oid2, t2t in list(t2_open_trades.items()):
                        lines2 += ("\n" + t2t["symbol"] + " SHORT | entry " + str(t2t["entry"]) +
                                   " | SL " + str(t2t["sl"]) + " | TP " + str(t2t.get("tp", "?")))
                    send_tg(lines2)
                elif text == "/t1_status":
                    lines3 = ("Tight 1 (Awakening): " + ("ON" if t3_auto_trade_enabled else "OFF") +
                              " | Watchlist: " + str(len(t3_watchlist)) +
                              " | Awakened: " + str(len(t3_watch)) + " | Open: " + str(len(t3_open_trades)))
                    for s, st3 in list(t3_watch.items()):
                        hrs = max(0, int((st3["expiry_ts"] - time.time()) / 3600))
                        lines3 += "\n" + s + " " + st3["side"] + " - awaiting chase entry (" + str(hrs) + "h left)"
                    for _oid3, t3t in list(t3_open_trades.items()):
                        lines3 += ("\n" + t3t["symbol"] + " " + t3t["side"] + " open | peak " +
                                   str(round(t3t.get("peak_r", 0), 1)) + "R | SL " + str(t3t["sl"]))
                    send_tg(lines3)
                elif text == "/fast_status":
                    cf = "ON" if cf_auto_trade_enabled else "OFF"
                    pend = ""
                    for s, p in list(cf_pending.items()):
                        mins_left = max(0, int((p["expiry_ts"] - time.time()) / 60))
                        pend += "\n" + s + " dip-buy @ " + str(round(p["bid"], 6)) + " (" + str(mins_left) + "min left)"
                    send_tg("Crash Fade: " + cf + " | Open: " + str(len(cf_open_trades)) +
                            " | Pending: " + str(len(cf_pending)) + pend)
        except Exception as e:
            print(f"[TG CMD] error: {e}")
        time.sleep(1)


@app.route("/")
def health():
    return ("Niti combined - Crash Fade (market-breadth gate + 4% drop fade + dip-buy + 3R) "
            "+ RSI Reversion (RSI<20 + green + BE1R/trail-from-1.5R) "
            "+ Tight 1 (Dormant Awakening) + consolidated journal"), 200


if __name__ == "__main__":
    # Re-adopt anything already open on BingX BEFORE the engines start, so a restart
    # can't breach the concurrency caps or orphan a trail-managed position.
    try:
        get_futures_symbols()          # populate symbol_precision / max_lev first
        adopt_positions_on_start()
    except Exception as e:
        print(f"[STARTUP ADOPT ERROR] {e}")

    Thread(target=cf_scan_loop,             daemon=True).start()
    Thread(target=t2_loop,                  daemon=True).start()   # Tight 2 fade (replaces retired RSI on /start /stop)
    Thread(target=trailing_loop,            daemon=True).start()
    Thread(target=t3_loop,                  daemon=True).start()
    Thread(target=handle_telegram_commands, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
