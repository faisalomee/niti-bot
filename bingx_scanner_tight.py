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

# ==================== SUPABASE / OI COLLECTOR CONFIG (2026-08-10) ====================
SUPABASE_URL             = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_KEY     = os.environ.get("SUPABASE_SERVICE_KEY", "")
OI_SCAN_INTERVAL_SECONDS = int(os.environ.get("OI_SCAN_INTERVAL_SECONDS", 900))   # log every 15 min
OI_REQUEST_PAUSE         = float(os.environ.get("OI_REQUEST_PAUSE", 0.15))
OI_BATCH_INSERT          = int(os.environ.get("OI_BATCH_INSERT", 50))

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

# ==================== GLOBAL STATE ====================
#   /t1_start /t1_stop   -> Tight 1 (Dormant Awakening)
symbol_precision   = {}
symbol_max_lev     = {}
symbol_launch_time = {}   # {symbol: launchTime_ms} from contracts endpoint (2026-08-12 coin-age filter)

# 2026-08-12 COIN-AGE FILTER: skip coins younger than this many days. Root cause: VELVET/HOME
# (listed after the backtest window) passed the 2M liquidity floor on current volume but are
# thin-book NEW coins that slip (VELVET slipped 1.03% live) and were never backtested. The
# liquidity floor sees VOLUME not AGE. launchTime comes free from the contracts endpoint.
MIN_COIN_AGE_DAYS  = int(os.environ.get("MIN_COIN_AGE_DAYS", 30))

# 2026-08-16 COIN WHITELIST (Faisal's fix for the new-coin problem — VELVET/HOME/ON/AIO/CYS
# repeatedly drained the account and slipped past coin_too_young() because launchTime is
# unknown for brand-new coins => that filter fails OPEN. This is an ABSOLUTE allow-list:
# ONLY these backtested coins can ever be scanned/traded. New/unlisted coins can never enter.
# Bound to the dataset: REGENERATE from the new symbol list whenever fresh candle data is uploaded.
BACKTESTED_COINS = {
    "1000000BABYDOGE-USDT", "1000000MOG-USDT", "10000SATS-USDT", "1000BONK-USDT", "1000CAT-USDT", "1000CHEEMS-USDT",
    "1000PEPE-USDT", "1INCH-USDT", "AAVE-USDT", "ACE-USDT", "ACH-USDT", "ACT-USDT",
    "ADA-USDT", "AERO-USDT", "AEVO-USDT", "AGLD-USDT", "AIXBT-USDT", "AKT-USDT",
    "ALGO-USDT", "ALICE-USDT", "ALT-USDT", "ANKR-USDT", "APE-USDT", "API3-USDT",
    "APT-USDT", "AR-USDT", "ARB-USDT", "ARK-USDT", "ARKM-USDT", "ARPA-USDT",
    "ASTR-USDT", "ATH-USDT", "ATOM-USDT", "AUCTION-USDT", "AVA-USDT", "AVAX-USDT",
    "AXL-USDT", "AXS-USDT", "BANANA-USDT", "BAT-USDT", "BB-USDT", "BCH-USDT",
    "BEAM-USDT", "BICO-USDT", "BIGTIME-USDT", "BLUR-USDT", "BNB-USDT", "BOME-USDT",
    "BRETT-USDT", "BSV-USDT", "BTC-USDT", "CAKE-USDT", "CARV-USDT", "CATI-USDT",
    "CELO-USDT", "CETUS-USDT", "CFX-USDT", "CGPT-USDT", "CHILLGUY-USDT", "CHR-USDT",
    "CHZ-USDT", "CKB-USDT", "COMP-USDT", "COTI-USDT", "COW-USDT", "CRO-USDT",
    "CRV-USDT", "CTK-USDT", "CYBER-USDT", "DASH-USDT", "DIA-USDT", "DOGE-USDT",
    "DOGS-USDT", "DOT-USDT", "DRIFT-USDT", "DUSK-USDT", "DYDX-USDT", "DYM-USDT",
    "EDU-USDT", "EGLD-USDT", "EIGEN-USDT", "ENA-USDT", "ENJ-USDT", "ENS-USDT",
    "ETC-USDT", "ETH-USDT", "ETHFI-USDT", "FARTCOIN-USDT", "FET-USDT", "FIDA-USDT",
    "FIL-USDT", "FLOKI-USDT", "FLOW-USDT", "FLUX-USDT", "G-USDT", "GALA-USDT",
    "GAS-USDT", "GLM-USDT", "GMT-USDT", "GMX-USDT", "GOAT-USDT", "GRASS-USDT",
    "GRT-USDT", "HBAR-USDT", "HIVE-USDT", "HMSTR-USDT", "HYPE-USDT", "ICP-USDT",
    "ID-USDT", "ILV-USDT", "IMX-USDT", "INJ-USDT", "IO-USDT", "IOST-USDT",
    "IOTA-USDT", "JASMY-USDT", "JST-USDT", "JTO-USDT", "JUP-USDT", "KAIA-USDT",
    "KAS-USDT", "KAVA-USDT", "KMNO-USDT", "KNC-USDT", "KSM-USDT", "LDO-USDT",
    "LINK-USDT", "LISTA-USDT", "LPT-USDT", "LQTY-USDT", "LSK-USDT", "LTC-USDT",
    "LUMIA-USDT", "LUNA-USDT", "LUNC-USDT", "MAGIC-USDT", "MANA-USDT", "MANTA-USDT",
    "MASK-USDT", "MAV-USDT", "MAVIA-USDT", "ME-USDT", "MEME-USDT", "MERL-USDT",
    "METIS-USDT", "MEW-USDT", "MINA-USDT", "MOODENG-USDT", "MORPHO-USDT", "MOVE-USDT",
    "MOVR-USDT", "MTL-USDT", "NEAR-USDT", "NEIROCTO-USDT", "NEO-USDT", "NMR-USDT",
    "NOT-USDT", "OKB-USDT", "ONDO-USDT", "ONE-USDT", "ONG-USDT", "ONT-USDT",
    "OP-USDT", "ORDER-USDT", "ORDI-USDT", "PENDLE-USDT", "PENGU-USDT", "PEOPLE-USDT",
    "PHA-USDT", "PIXEL-USDT", "PNUT-USDT", "POL-USDT", "POLYX-USDT", "POPCAT-USDT",
    "PORTAL-USDT", "PYTH-USDT", "QNT-USDT", "QTUM-USDT", "RARE-USDT", "RATS-USDT",
    "RAY-USDT", "RENDER-USDT", "REZ-USDT", "RIF-USDT", "RLC-USDT", "RON-USDT",
    "ROSE-USDT", "RPL-USDT", "RSR-USDT", "RUNE-USDT", "RVN-USDT", "SAFE-USDT",
    "SAGA-USDT", "SAND-USDT", "SANTOS-USDT", "SCRT-USDT", "SEI-USDT", "SFP-USDT",
    "SKL-USDT", "SLP-USDT", "SNX-USDT", "SOL-USDT", "SPX-USDT", "SSV-USDT",
    "STG-USDT", "STORJ-USDT", "STRK-USDT", "STX-USDT", "SUI-USDT", "SUN-USDT",
    "SUPER-USDT", "SUSHI-USDT", "SYN-USDT", "TAO-USDT", "THE-USDT", "THETA-USDT",
    "TIA-USDT", "TLM-USDT", "TNSR-USDT", "TRB-USDT", "TRX-USDT", "TURBO-USDT",
    "TWT-USDT", "UMA-USDT", "UNI-USDT", "USUAL-USDT", "VANA-USDT", "VANRY-USDT",
    "VELODROME-USDT", "VET-USDT", "VIRTUAL-USDT", "W-USDT", "WAVES-USDT", "WIF-USDT",
    "WLD-USDT", "WOO-USDT", "XAI-USDT", "XCN-USDT", "XLM-USDT", "XMR-USDT",
    "XRP-USDT", "YFI-USDT", "YGG-USDT", "ZEC-USDT", "ZEN-USDT", "ZETA-USDT",
    "ZK-USDT", "ZRO-USDT", "ZRX-USDT",
}
WHITELIST_ONLY = os.environ.get("WHITELIST_ONLY", "1") == "1"   # 1 = trade only BACKTESTED_COINS


# 2026-08-12 ORDER-BOOK DEPTH CHECK: before entry, fetch the live book and require that the
# liquidity sitting between entry and the SL price is at least this multiple of our position
# qty. If not, the SL market-order would eat too deep into a thin book and slip badly (this is
# exactly what gave PROM -$13.1 and VELVET -$8.6 when $5-risk SLs filled far past trigger).
# The coin that would slip is caught at ENTRY regardless of its age or 24h volume.
DEPTH_LIQUIDITY_MULT = float(os.environ.get("DEPTH_LIQUIDITY_MULT", 3.0))
DEPTH_CHECK_ENABLED  = os.environ.get("DEPTH_CHECK_ENABLED", "1") == "1"

# 2026-08-16 THIN/NEW-COIN GUARDS (shared by Tight 1 LONG + Tight 2 SHORT):
# - MIN_ENTRY_PRICE: skip micro-price coins (thin, high tick-slippage). Backtest-verified thin-coin
#   blocker + PnL booster (price>=0.001 recent-bear best). Note: alone it does NOT catch AIO/HOME-type
#   new coins (they pass) - the real new-coin catch is SL_DIST_CAP + coin-age + depth together.
# - SL_DIST_CAP: skip a trade whose SL sits >5% from entry. 10x isolated liquidates ~9.1% adverse,
#   so a wider SL liquidates BEFORE the SL (this is exactly how AIO liquidated: entry 0.0544, ~9.5% away).
#   Wide-SL trades are also the volatile/downtrend losers; skipping raises meanR +0.28->+0.32, liq=0.
MIN_ENTRY_PRICE      = float(os.environ.get("MIN_ENTRY_PRICE", 0.001))
SL_DIST_CAP          = float(os.environ.get("SL_DIST_CAP", 0.05))

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
            try:
                lt = int(c.get("launchTime", 0) or 0)
                if lt > 0:
                    symbol_launch_time[sym] = lt
            except Exception:
                pass
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


def coin_too_young(sym):
    """True if the coin was listed less than MIN_COIN_AGE_DAYS ago. If launchTime is
    unknown (not in the contracts response) we DO NOT block it - unknown-age coins are
    usually long-established pairs whose launchTime the API omits, and blocking on
    missing data would silently starve the universe. Only a KNOWN-recent launchTime blocks."""
    if MIN_COIN_AGE_DAYS <= 0:
        return False
    lt = symbol_launch_time.get(sym, 0)
    if lt <= 0:
        return False
    age_days = (time.time() * 1000 - lt) / 86400000.0
    return age_days < MIN_COIN_AGE_DAYS


def get_order_book(symbol, limit=100):
    """Fetch live order book. Returns dict with 'bids' and 'asks' as [[price, qty], ...]
    lists, or None on any failure."""
    try:
        url = BASE_URL + "/openApi/swap/v2/quote/depth"
        r = requests.get(url, params={"symbol": symbol, "limit": limit}, timeout=8).json()
        data = r.get("data", {})
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        if not bids or not asks:
            return None
        return {"bids": bids, "asks": asks}
    except Exception as e:
        print(f"[DEPTH FETCH {symbol}] error: {e}")
        return None


def depth_ok(symbol, entry, sl, qty, side):
    """True if the book has enough liquidity between entry and SL to absorb our position's
    SL market-order without excessive slippage. For a SHORT the SL is ABOVE entry, so the
    stop closes by BUYING -> it walks the ASKS from entry up to SL; sum ask-qty in that band.
    For a LONG the SL is BELOW entry, stop closes by SELLING -> walk the BIDS from entry down
    to SL; sum bid-qty. Require summed liquidity >= qty * DEPTH_LIQUIDITY_MULT. If depth can't
    be fetched we DO NOT block (fail-open) - a missing book shouldn't silently kill all trading,
    and the coin-age + liquidity floors are still guarding. Only a KNOWN-thin book blocks."""
    if not DEPTH_CHECK_ENABLED:
        return True
    ob = get_order_book(symbol)
    if ob is None:
        return True   # fail-open: don't block on a fetch failure
    try:
        if side == "SELL":
            lo_p, hi_p = min(entry, sl), max(entry, sl)   # SL above entry
            avail = sum(float(q) for p, q in ob["asks"] if lo_p <= float(p) <= hi_p)
        else:
            lo_p, hi_p = min(entry, sl), max(entry, sl)   # SL below entry
            avail = sum(float(q) for p, q in ob["bids"] if lo_p <= float(p) <= hi_p)
        need = qty * DEPTH_LIQUIDITY_MULT
        if avail < need:
            print(f"[DEPTH SKIP] {symbol} {side} thin book: {avail:.2f} in entry->SL band < need {need:.2f} (qty {qty} x{DEPTH_LIQUIDITY_MULT})")
            return False
        return True
    except Exception as e:
        print(f"[DEPTH CHECK {symbol}] error: {e}")
        return True   # fail-open on parse error


def get_liquid_symbols(symbols, min_quote_vol, max_n=None, exclude_top_n=0,
                       rank_by_gain=0, gain_min=None, gain_max=None):
    """rank_by_gain (added 2026-07-23, default 0 = OFF): when > 0, the surviving
    symbols are filtered to the [gain_min, gain_max] daily-move band and then
    re-sorted by today's % gain and cut to that many. Tight and Tight 1 do not pass
    any of these, so their universe selection is byte-for-byte unchanged."""
    global _gain_field_logged
    # 2026-08-16 WHITELIST GATE: restrict the entire universe to backtested coins only.
    # This is the single choke point feeding block-build + both engines, so new/unlisted
    # coins (VELVET/HOME/ON-type) can never be scanned or traded.
    if WHITELIST_ONLY:
        symbols = [s for s in symbols if s in BACKTESTED_COINS]
    try:
        url = BASE_URL + "/openApi/swap/v2/quote/ticker"
        r = requests.get(url, timeout=10).json()
        tickers = r.get("data", [])
        if not isinstance(tickers, list):
            fb = [s for s in symbols if not coin_too_young(s)]
            return fb[:max_n] if max_n else fb
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
            if qvol >= min_quote_vol and not coin_too_young(sym):
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
        fb = [s for s in symbols if not coin_too_young(s)]
        return fb[:max_n] if max_n else fb


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
        t2_loss_until[symbol] = time.time() + T2_LOSS_COOLDOWN_DAYS * 86400  # liquidation = a loss: ban this coin for N days (applies to any engine; harmless for non-T2)
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

    Only LONG positions are adopted here. Any adopted LONG is handed to the Tight 2
    tracker (it manages BE/trail/TP for both long and short). If a position has no SL at all, a protective SL is placed and
    the user is alerted. SHORT positions keep their exchange SL and are only reported."""
    try:
        positions = get_open_positions()
    except Exception as e:
        print(f"[ADOPT ERROR] {e}")
        return
    if not positions:
        print("[ADOPT] no open positions on BingX to re-adopt")
        return
    tracked = {t["symbol"] for t in t2_open_trades.values()} | {t["symbol"] for t in t3_open_trades.values()}
    adopted = shorts = 0
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
            # Adopted LONG with a live TP bracket -> hand to Tight 2 tracker.
            t2_open_trades["adopt-" + sym] = {
                "symbol": sym, "side": "BUY", "entry": avg, "entry_fill": avg,
                "sl": sl_price if sl_price else round(avg * 0.95, 6), "sl_id": sl_id,
                "tp": tp_price, "tp_id": tp_id, "close_side": "SELL", "pos_side": "LONG",
                "total_qty": amt,
                "risk_dist": abs(avg - (sl_price or avg * 0.95)), "risk_usdt": T2_RISK_USDT,
                "atr_at_entry": 0.0, "opened_ts": now,
                "peak_r": 0.0, "be_done": False, "stop_r": None,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
            adopted += 1
            lines.append(sym + " LONG -> Tight 2 (adopted SL+TP bracket)")
        else:
            # Adopted LONG with no exchange TP -> hand to Tight 2 tracker (bot-driven exit).
            risk_dist = abs(avg - sl_price) if sl_price else avg * 0.05
            t2_open_trades["adopt-" + sym] = {
                "symbol": sym, "side": "BUY", "entry": avg, "entry_fill": avg,
                "sl": sl_price if sl_price else round(avg * 0.95, 6), "sl_id": sl_id,
                "tp": None, "tp_id": None, "close_side": "SELL", "pos_side": "LONG",
                "total_qty": amt,
                "risk_dist": risk_dist if risk_dist > 0 else avg * 0.05, "risk_usdt": T2_RISK_USDT,
                "atr_at_entry": 0.0, "opened_ts": now,
                "peak_r": 0.0, "be_done": False, "stop_r": None,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            }
            adopted += 1
            lines.append(sym + " LONG -> Tight 2 (adopted, trail-managed)")
    print(f"[ADOPT] re-adopted {adopted} into Tight 2, {shorts} short(s) left on their SL")
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

T3_MIN_QUOTE_VOL          = float(os.environ.get("T3_MIN_QUOTE_VOL", 1_000_000)) # 2026-08-11: 300k->1M. Structure SL has small risk-dist so slippage hurts proportionally MORE; 1M floor cuts thin-coin slippage while keeping 9.9 trades/wk (3M would halve to 6.4). Slippage protection for the tight structure stop.
T3_MAX_SYMBOLS            = int(os.environ.get("T3_MAX_SYMBOLS", 600))
T3_MAX_WATCHLIST          = int(os.environ.get("T3_MAX_WATCHLIST", 150))         # keep the TIGHTEST bands if more qualify - the most compressed springs
T3_DORMANCY_DAYS          = int(os.environ.get("T3_DORMANCY_DAYS", 5))
T3_DORMANCY_RANGE_PCT     = float(os.environ.get("T3_DORMANCY_RANGE_PCT", 25.0)) # total close band width = +/-15% around mid
T3_DORMANCY_VOL_SPIKE_MAX = float(os.environ.get("T3_DORMANCY_VOL_SPIKE_MAX", 3.0))   # any day >3x median inside the window = already awakened earlier, not dormant
T3_AWAKE_VOL_MULT         = 1.5      # 2026-08-10 PnL: 2x->1.5x nearly doubles trades (7.7->13.4/wk), meanR unchanged +1.5, holdout strong
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
# ---- 2026-08-10 DELAYED-ENTRY + CONFLUENCE ----
T3_DELAY_MAX_DAYS         = int(os.environ.get("T3_DELAY_MAX_DAYS", 10))          # wait up to 10 days after awakening for a confirmation day
T3_DELAY_SL_ATR_MULT      = float(os.environ.get("T3_DELAY_SL_ATR_MULT", 2.0))    # (legacy ATR stop; superseded by structure SL below, kept as the min-floor multiplier)
T3_STRUCT_SL_MIN_ATR      = float(os.environ.get("T3_STRUCT_SL_MIN_ATR", 1.0))    # 2026-08-11 STRUCTURE SL: stop at confirmation-day low, but at least this x ATR15 away (avoids too-tight noise stops). Backtest: win 48->77%, meanR +1.69->+3.34, holdout A+3.28/B+3.40. THE big Tight-1 fix.
T3_CONF_RISK_USDT         = float(os.environ.get("T3_CONF_RISK_USDT", 10.0))      # confluence trade (T1+T2 agree same coin/dir within 5d) risk = $10
T3_CONF_WINDOW_SEC        = int(os.environ.get("T3_CONF_WINDOW_SEC", 432000))     # 5 days
recent_signals = {}   # symbol -> {"side","ts"} : shared T1/T2 signal log for confluence
def record_signal(symbol, side):
    recent_signals[symbol] = {"side": side, "ts": time.time()}
def is_confluence(symbol, side):
    s = recent_signals.get(symbol)
    return bool(s) and s["side"] == side and (time.time() - s["ts"]) <= T3_CONF_WINDOW_SEC

# 2026-08-11 TIME-STOP: no Tight-1/Tight-2 trade holds margin longer than this. Backtest: 100% of
# T1 & T2 wins hit TP within 3 days (most within 1-2); a trade still open at 2 days won't reach TP
# and just locks margin (e.g. ETHFI). Closing at 2d keeps win/PnL intact (verified: 2d-stop = same
# sumR as no-stop) while freeing the slot. Longs/shorts both.
TIGHT_MAX_HOLD_SECONDS   = int(os.environ.get("TIGHT_MAX_HOLD_SECONDS", 172800))   # 2 days
T3_FIXED_TP_R             = float(os.environ.get("T3_FIXED_TP_R", 8.0))          # 2026-08-11: 6R->8R with STRUCTURE SL. Tight structure stop = small risk-dist = more R per win; backtest win 75% meanR +3.9 sumR1820 (vs TP6 win77%/+3.34). Visible reduce-only TP.
T3_SLIP_ALERT_PCT         = float(os.environ.get("T3_SLIP_ALERT_PCT", 0.3))      # 2026-07-30: log+alert only (NO auto-skip yet) if entry fill is >this% from signal px. Collect 2-3wk live slip, then decide a skip threshold.

# ==================== TIGHT 2 (Trapped-Block Fade, SHORT) ====================
# /start /stop controls this engine (Tight 2). 2026-08-12: Crash Fade + RSI fully removed.
# Concept (only survivor of 4 discretionary docs): SHORT a prior high-volume
# "trapped block" resistance when price returns up into it on declining volume
# (demand exhaustion). Backtest (15m 8mo, band5/dump18, TP4/buf1.0/exh0.8):
# max2 win 52% $144 DD -$8; holdout A+1.51/B+1.09; random -0.445; 8/9 months+.
T2_RET_BAND_PCT           = 6.0    # HARDCODED. wider retest zone raises win 50->54%
T2_DUMP_PCT               = 18.0   # HARDCODED. block qualifies if price moved >= this % within lookahead
T2_DUMP_LOOKAHEAD_DAYS    = 5      # HARDCODED
T2_VOL_SPIKE              = 1.3    # 2026-08-11: 1.3 (with 3M floor + drawdown filter now guarding quality). More trades (9.9/wk) AND high win because the DD filter removes the deep-downtrend losers that 1.3 alone pulled in. Verified vol1.3+DD30 = win 62% meanR +1.59 holdout A+1.41/B+1.84.
T2_MAX_DRAWDOWN_PCT       = float(os.environ.get("T2_MAX_DRAWDOWN_PCT", 0.35))   # 2026-08-16: 0.15->0.35 (bear-trap fix). DD15 killed ~100% of longs in a downtrend (every coin >15% below 60d high) -> short-only -> account $80->$40. DD35 restores long/short balance; holdout A+0.20/B+0.16, recent-bear positive.  [was 0.30->0.15.] Demand-LONG only works on STRONG coins near their 60d high. Feature analysis: coin 5-15% below high = win 74%, but 15-30% below = win 45% (support breaks on downtrend coins). Tightening to 15% raises LONG win 47->62% and overall win 52->62%, PnL $669->$1054, 9/9 months+, holdout A67%/B66%. Keeps both-sides (bull-run safe). SHORT unaffected.
T2_VOL_EXHAUSTION         = 0      # HARDCODED. DISABLED (0=off) - exh0.8 killed 87% of signals
T2_SL_ATR_BUF             = 0.5    # HARDCODED. SL nearer level = more R per move
T2_TP_R                   = 3.0    # 2026-08-16: 2R->3R. With fresh<7d blocks the fade edge is big (meanR+1.1); TP3R captures the runners. Sweet spot: TP2.5 win higher but meanR lower, TP4 meanR ~same but win 39%. RR = 3:1.
T2_BLOCK_MAX_AGE_DAYS     = 7      # 2026-08-16: 30->7. THE REAL EDGE LEVER. A trapped-block fade only works while trapped holders are still trapped; after ~7d they've bailed and the level is dead. fresh<7d: meanR +0.27->+1.1, win 28->48%, holdout A+1.04/B+1.01, 9/9 months. Most old trades were on STALE blocks = the losers.  [was 30]
T2_LONG_SIDE              = True   # HARDCODED. both-sides (short resistance + long demand): 26.7 trades/wk $1049 vs short-only 10.4/wk $594, fully validated.
T2_ATR_LEN                = 14
T2_DORMANCY_DAYS          = 5      # trailing window for the block's baseline median volume
T2_BLOCK_SCAN_SECONDS     = 14400  # HARDCODED. rebuild block map every 4h
T2_ENTRY_CHECK_SECONDS    = 300    # HARDCODED. check entries every 5min
T2_COOLDOWN_SECONDS       = int(os.environ.get("T2_COOLDOWN_SECONDS", 14400))  # 2026-08-16: 6h->4h (ceiling push). fresh<7d+losscd5+cd4h = meanR+1.225 win50% vs cd6h +1.19/49%. Good coins recycle faster. Do NOT go below 4h (untested).  [was 24h->6h at 21600] 24h was over-strict - a coin returning to its block band 6h later is another valid fade being skipped. Backtest (TP2,DD15,cap2): 24h=10.9/wk $1088 -> 6h=15.6/wk $1666 (+43% trades, +53% PnL, win 63->60% ~unchanged, holdout A67%/B66% robust, 8/9 months+). Margin-safe (shared cap-2+conf still caps at 3 open). Do NOT go below 6h (untested).
T2_LOSS_COOLDOWN_DAYS     = int(os.environ.get("T2_LOSS_COOLDOWN_DAYS", 5))  # 2026-08-13: after a coin closes SL/Liquidated, ban THAT coin this many days (winners keep the 6h cooldown). Fixes the "same 2-3 losing coins on repeat" loop (PROM kept getting re-shorted every 6h and losing). Backtest same-pool: 6h-only win58%/$1477 -> +5d-loss-cd win71%/$2029, holdout A+0.742/B+0.727. A losing coin returns in 6h and loses again; a 5d ban forces the bot onto other coins.
T2_RISK_USDT              = 5.0    # 2026-08-05: 2->5 per Faisal (~5% risk on $100). SL stays at block level; size scales up, margin follows (~$6-10 typical).
T2_LEVERAGE               = 10     # HARDCODED
T2_MAX_MARGIN_USDT        = 60.0   # 2026-08-05: 25->60 so $5-risk + wide structure SL is never capped below intended risk
T2_MAX_SYMBOLS            = 250    # HARDCODED
T2_EXCLUDE_TOP_N          = 0      # HARDCODED. INCLUDE majors - they form clean blocks too
T2_MIN_QUOTE_VOL          = 2_000_000  # 2026-08-12: 3M->2M. Backtest: 2M recovers ~$120 vs 3M ($825 vs $704 cap2) with holdout still robust (A+1.09/B+0.89) and still excludes RIF-tier thin coins that slipped SL live. 3M was over-conservative; 2M is the sweet spot between live-slippage safety and PnL.

# Shared T1+T2 concurrency cap: on a $100 account both engines TOGETHER = 2 open.
# (Backtest: shared-max2 $534 DD-$33 is the best risk-adj; raise once balance grows.)
SHARED_MAX_CONCURRENT     = 2      # HARDCODED. T1+T2 together = max 2 open on $100 acct
CONFLUENCE_EXTRA_SLOTS    = 1      # 2026-08-12: a CONFLUENCE trade (T1+T2 agree same coin+dir within 5d)
                                   # may open ONE dedicated slot BEYOND the 2 normal slots, so the bot's
                                   # highest-conviction trades are never dropped by the cap. Backtest cap2+1conf:
                                   # $966->$2285, win 42->46%, holdout A+$1030/B+$1320. Keep conf risk $10 (not higher)
                                   # on a small account - 3 open positions is the max margin exposure.

t2_auto_trade_enabled = AUTO_RESUME_ON_START
t2_blocks       = {}    # symbol -> list of (resistance_level, formed_day_ms)
t2_open_trades  = {}    # order_id -> open Tight 2 fade trade
t2_last_fire    = {}    # symbol -> ts of last fired fade (normal 6h cooldown)
t2_loss_until   = {}    # symbol -> ts until which the coin is banned after an SL/Liquidation (loss cooldown)

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
    # cross-engine: block if ANY engine (T1/T2/CF) holds this symbol
    return symbol in all_open_symbols()


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
    record_signal(symbol, side)                       # log for confluence (T2 can see this)
    conf = is_confluence(symbol, side)                # True if T2 signalled same coin/dir within 5d
    risk_usdt = T3_CONF_RISK_USDT if conf else T3_RISK_USDT
    trade_status = ""
    if t3_auto_trade_enabled and not slot_available_for(conf):
        trade_status = "\nSkipped - shared T1+T2 cap (" + str(SHARED_MAX_CONCURRENT) + ") reached"
    elif t3_auto_trade_enabled:
        oid = place_t3_order(symbol, side, entry_px, sl, risk_usdt, conf)
        if oid == "MARGIN_SKIP":
            trade_status = "\nSkipped - insufficient margin"
        elif oid == "DEPTH_SKIP":
            trade_status = "\nSkipped - order book too thin (slippage guard)"
        elif oid and oid != "N/A":
            trade_status = "\nOrder: " + str(oid)
        else:
            trade_status = "\nOrder failed"
    else:
        trade_status = "\nAuto-trade OFF"
    conf_tag = " [CONFLUENCE $" + str(int(T3_CONF_RISK_USDT)) + "]" if conf else ""
    print(f"[T3 ENTRY]{conf_tag} {symbol} {side} @ {entry_px} SL {sl} risk=${risk_usdt} vol_ratio={st.get('vol_ratio', 0):.1f}x{trade_status}")
    send_tg(
        "TIGHT 1 ENTRY" + conf_tag + " - " + side + " - " + symbol + "\n------------------------------\n"
        "Entry: " + str(round(entry_px, 6)) + " | SL: " + str(sl) + " | Risk: $" + str(risk_usdt) + " | Lev: " + str(T3_LEVERAGE) + "x\n"
        "Delayed entry (higher-low+green day) | TP " + str(T3_FIXED_TP_R) + "R visible | BE " + str(T3_BE_TRIGGER_R) + "R, trail from " + str(T3_TRAIL_START_R) + "R at peak-" + str(T3_TRAIL_GAP_R) + "R\n"
        "Awakening vol: " + str(round(st.get("vol_ratio", 0), 1)) + "x dormant median" +
        trade_status + "\n------------------------------\nNiti Tight 1"
    )


def t3_check_awakened(symbol):
    """DELAYED daily-confirmation entry for one awakened symbol (2026-08-10). After
    awakening, waits up to T3_DELAY_MAX_DAYS for a confirmation DAY (green + higher
    low for longs; red + lower high for shorts), then enters at that day's close."""
    st = t3_watch.get(symbol)
    if not st:
        return
    if time.time() >= st["expiry_ts"]:
        t3_watch.pop(symbol, None)
        print(f"[T3 EXPIRED] {symbol} - no pullback entry within the window")
        send_tg("TIGHT 1 EXPIRED - " + symbol + "\nNo clean pullback entry within " + str(T3_SETUP_EXPIRY_SECONDS // 3600) + "h of the awakening - trade skipped")
        return
    try:
        # DELAYED ENTRY (2026-08-10, replaces chase): after the awakening, watch DAILY
        # candles. Enter on the first CLOSED day (within T3_DELAY_MAX_DAYS) that is
        # GREEN (close>open) AND makes a higher low than the prior day (mirror for
        # shorts: red + lower high). SL = T3_DELAY_SL_ATR_MULT x ATR(15m). This is the
        # backtested upgrade: day-1 breakout chase -> confirmed continuation day.
        daily = get_candles(symbol, limit=T3_DELAY_MAX_DAYS + 5, interval="1d")
        if len(daily) < 3:
            return
        confirmed = daily[:-1]                     # closed days only (drop the live day)
        prev  = confirmed[-2]
        today = confirmed[-1]
        if int(today["time"]) <= int(st.get("last_processed_time") or 0):
            return                                 # this day already processed
        st["last_processed_time"] = int(today["time"])
        side = st["side"]
        is_green   = cl(today) > o(today)
        higher_low = l(today) > l(prev)
        lower_high = h(today) < h(prev)
        # 2026-08-10 win-rate lever: confirmation day must also carry volume >= median of the
        # prior days in the fetched window (filters weak confirmations; win 48->51%, holdout stronger).
        try:
            prior_vols = sorted(v(d) for d in confirmed[:-1] if v(d) > 0)
            conf_vol_median = prior_vols[len(prior_vols)//2] if prior_vols else 0.0
            has_volume = v(today) >= conf_vol_median if conf_vol_median > 0 else True
        except Exception:
            has_volume = True
        # ATR on 15m for the stop distance
        c15 = get_candles(symbol, limit=T3_ATR_LEN + 40, interval="15m")
        c15c = c15[:-1] if len(c15) > 1 else c15
        if len(c15c) < T3_ATR_LEN + 1:
            return
        atr_now = atr_series([h(c) for c in c15c], [l(c) for c in c15c], [cl(c) for c in c15c], T3_ATR_LEN)[-1]
        if atr_now <= 0:
            return
        entry_px = cl(today)
        # 2026-08-11 STRUCTURE SL: stop at the confirmation day's low (longs) / high (shorts) -
        # the level that, if broken, invalidates the setup. Clamped to >= T3_STRUCT_SL_MIN_ATR x ATR15
        # so a too-tight structure doesn't create a noise stop. Backtest win 48->77%, meanR +3.34.
        min_dist = atr_now * T3_STRUCT_SL_MIN_ATR
        if side == "BUY" and is_green and higher_low and has_volume:
            struct_sl = l(today) * 0.999
            if entry_px - struct_sl < min_dist:
                struct_sl = entry_px - min_dist
            st["setup_sl"] = struct_sl
            t3_fire_entry(symbol, st, entry_px, atr_now)
        elif side == "SELL" and (not is_green) and lower_high and has_volume:
            struct_sl = h(today) * 1.001
            if struct_sl - entry_px < min_dist:
                struct_sl = entry_px + min_dist
            st["setup_sl"] = struct_sl
            t3_fire_entry(symbol, st, entry_px, atr_now)
        # else: no confirmation this day - keep waiting until expiry
    except Exception as e:
        print(f"[T3 AWAKENED {symbol}] error: {e}")


def place_t3_order(symbol, side, entry, sl, risk_usdt=None, conf=False):
    """Market entry + guarded SL only - deliberately NO exchange TP orders (the exit
    is the trail). Same risk-based sizing, margin pre-check and naked-position
    emergency-close discipline as Tight/Fast. risk_usdt defaults to T3_RISK_USDT;
    confluence trades pass T3_CONF_RISK_USDT."""
    try:
        if risk_usdt is None:
            risk_usdt = T3_RISK_USDT
        set_leverage_api(symbol, T3_LEVERAGE)
        precision = symbol_precision.get(symbol, 4)
        risk_dist = abs(entry - sl)
        if risk_dist <= 0:
            return None
        risk_qty       = risk_usdt / risk_dist
        margin_cap_qty = (T3_MAX_MARGIN_USDT * T3_LEVERAGE) / entry
        total_qty = round(min(risk_qty, margin_cap_qty), precision)
        if total_qty <= 0:
            return None
        # 2026-08-12 DEPTH CHECK: skip if the book is too thin between entry and SL (would slip badly)
        if not depth_ok(symbol, entry, sl, total_qty, side):
            send_tg("TIGHT 1 " + symbol + " " + side + " skipped - order book too thin (slippage guard)")
            return "DEPTH_SKIP"
        pos_side   = "LONG" if side == "BUY" else "SHORT"
        close_side = "SELL" if side == "BUY" else "BUY"

        required_margin = total_qty * entry / T3_LEVERAGE
        avail = get_available_margin()
        if avail is not None and avail < required_margin * 1.05:
            print(f"[T3 MARGIN SKIP] {symbol} need ~${required_margin:.2f}, available ${avail:.2f} - skipping")
            return "MARGIN_SKIP"

        order_id = place_market_order(symbol, side, total_qty, pos_side)
        print(f"[T3 ORDER] {symbol} {side} qty={total_qty} risk=${risk_usdt}: {order_id}")
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
                "trailed": False, "peak_r": 0.0, "risk_usdt": risk_usdt, "confluence": conf,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
                "open_ts": time.time(),
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

            # ---- 2026-08-11 TIME-STOP: close at market if held > 2 days (won't reach TP, frees margin) ----
            held = time.time() - trade.get("open_ts", time.time())
            if held >= TIGHT_MAX_HOLD_SECONDS:
                exit_price = get_current_price(symbol)
                if trade.get("total_qty", 0) > 0:
                    close_oid = place_market_order(symbol, trade["close_side"], trade["total_qty"], trade["pos_side"])
                    if close_oid and close_oid != "N/A":
                        time.sleep(0.5)
                        exit_price = get_fill_price(close_oid, symbol, fallback=exit_price)
                if trade.get("sl_id"):
                    cancel_order(symbol, trade["sl_id"])
                if trade.get("tp_id") and trade["tp_id"] != "N/A":
                    cancel_order(symbol, trade["tp_id"])
                if trade["side"] == "BUY":
                    leg_pnl = (exit_price - entry_ref) * trade["total_qty"]
                else:
                    leg_pnl = (entry_ref - exit_price) * trade["total_qty"]
                trade["pnl"]    = round(leg_pnl, 2)
                trade["result"] = "TimeExit"
                trade["exit_r"] = round(leg_pnl / trade.get("risk_usdt", T3_RISK_USDT), 2) if trade.get("risk_usdt") else 0.0
                trade["label"]  = "Tight 1"
                daily_trades.append(trade)
                journal_closed_trade(trade)
                t3_open_trades.pop(oid, None)
                print(f"[T3 TIME-STOP] {symbol} closed after {held/86400:.1f}d pnl={trade['pnl']}")
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
    """RETIRED 2026-08-16. The old dormancy->awakening Tight 1 was backtested DEAD
    (116 sig/8mo, ALL TP variants NEGATIVE meanR -0.37..-0.75). Tight 1 is now the
    LONG side of the fresh demand-block fade (meanR +0.9, holdout A+1.02/B+0.75,
    9/9 months) and is generated INSIDE t2_loop / t2_check_entry, gated by
    t3_auto_trade_enabled. This thread stays alive only so /t1_start /t1_stop still
    have something to toggle and so the process shape is unchanged; it does no
    scanning of its own. All the dormancy funcs below are dead code kept for history."""
    print("Tight 1 = LONG fresh-demand-block (runs inside the Tight 2 loop). Dormancy engine retired.")
    while True:
        time.sleep(3600)
# ==================== END TIGHT 1 ====================


# ==================== TIGHT 2 (Trapped-Block Fade) ====================
def all_open_symbols():
    """Every symbol currently held by ANY engine (T1 + T2 + CrashFade). Used so no
    engine opens a coin another engine already holds — this is the guard that was
    missing when CF + Tight 2 both opened 1000BONK at once and one got liquidated."""
    syms = set()
    for d in (t3_open_trades, t2_open_trades):
        for t in d.values():
            s = t.get("symbol")
            if s:
                syms.add(s)
    return syms

def t2_total_open():
    """Shared open count across T1 + T2 for the shared concurrency cap.
    CONFLUENCE trades are counted here too (they hold a slot), but a confluence trade
    is ALLOWED to open a dedicated extra slot beyond SHARED_MAX_CONCURRENT — see
    slot_available_for() below."""
    return len(t3_open_trades) + len(t2_open_trades)

def confluence_open_count():
    """How many currently-open trades (T1+T2) were opened as CONFLUENCE trades."""
    n = 0
    for d in (t3_open_trades, t2_open_trades):
        for t in d.values():
            if t.get("confluence"):
                n += 1
    return n

def slot_available_for(is_conf):
    """Shared-slot gate with a CONFLUENCE dedicated slot (2026-08-12).
    - Normal trades: allowed only while total_open < SHARED_MAX_CONCURRENT.
    - Confluence trades: allowed while total_open < SHARED_MAX_CONCURRENT, OR (when the
      normal slots are full) while fewer than CONFLUENCE_EXTRA_SLOTS confluence trades
      are already open. This means a high-conviction T1+T2-agreement trade is never
      dropped just because the 2 normal slots are busy. Backtest-verified best lever."""
    total = t2_total_open()
    if total < SHARED_MAX_CONCURRENT:
        return True
    if is_conf and confluence_open_count() < CONFLUENCE_EXTRA_SLOTS:
        return True
    return False

def t2_in_cooldown(symbol):
    now = time.time()
    if now < t2_loss_until.get(symbol, 0):      # coin is loss-banned (recent SL/Liquidation)
        return True
    last = t2_last_fire.get(symbol, 0)
    return (now - last) < T2_COOLDOWN_SECONDS

def t2_symbol_has_open_trade(symbol):
    # cross-engine: block if ANY engine holds this symbol, not just Tight 2
    return symbol in all_open_symbols()

def t2_build_blocks(all_symbols):
    """For each symbol, build BOTH block types from DAILY candles:
    - RESISTANCE block (short): day vol >= T2_VOL_SPIKE x trailing-5d median whose close
      then FELL >= T2_DUMP_PCT within lookahead. Day HIGH = resistance (trapped longs).
    - DEMAND block (long): same volume spike but whose price then ROSE >= T2_DUMP_PCT.
      Day LOW = support (trapped shorts). Mirror edge, validated 2026-08-05.
    Each block stored as (level, formed_ms, side) with side 'S' or 'L'. Rebuilt every 4h."""
    syms = get_liquid_symbols(all_symbols, min_quote_vol=T2_MIN_QUOTE_VOL, max_n=T2_MAX_SYMBOLS, exclude_top_n=T2_EXCLUDE_TOP_N)
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
                if closes[i] <= 0:
                    continue
                fut_low  = min(lows[i + 1:i + 1 + T2_DUMP_LOOKAHEAD_DAYS])
                fut_high = max(highs[i + 1:i + 1 + T2_DUMP_LOOKAHEAD_DAYS])
                # resistance block -> short (price fell after the spike)
                if (closes[i] - fut_low) / closes[i] * 100 >= T2_DUMP_PCT:
                    blocks.append((highs[i], times[i], "S"))
                # demand block -> long (price rose after the spike)
                if T2_LONG_SIDE and (fut_high - closes[i]) / closes[i] * 100 >= T2_DUMP_PCT:
                    blocks.append((lows[i], times[i], "L"))
            if blocks:
                new_blocks[sym] = blocks
            time.sleep(0.2)
        except Exception as e:
            print(f"[T2 BLOCK {sym}] error: {e}")
    t2_blocks.clear()
    t2_blocks.update(new_blocks)
    n_s = sum(1 for bl in t2_blocks.values() for b in bl if b[2] == "S")
    n_l = sum(1 for bl in t2_blocks.values() for b in bl if b[2] == "L")
    print(f"[T2 BLOCKS] built for {len(t2_blocks)} symbols (short-blocks={n_s} long-blocks={n_l})")

def t2_check_entry(symbol, blocks):
    """On 15m: SHORT when price returns UP into a resistance block; LONG when price
    returns DOWN into a demand block. Both within band, fresh block only."""
    try:
        candles = get_candles(symbol, limit=40, interval="15m")
        if len(candles) < 22:
            return
        confirmed = candles[:-1]
        c_last = confirmed[-1]
        price  = cl(c_last)
        hi     = h(c_last)
        lo     = l(c_last)
        cur_vol = v(c_last)
        highs  = [h(x)  for x in confirmed]
        lows   = [l(x)  for x in confirmed]
        closes = [cl(x) for x in confirmed]
        vols   = [v(x)  for x in confirmed]
        atr_now = atr_series(highs, lows, closes, T2_ATR_LEN)[-1]
        if atr_now <= 0:
            return
        if price < MIN_ENTRY_PRICE:   # 2026-08-16: block micro-price coins (thin, high tick-slippage)
            return
        vol_window = vols[-21:-1] if len(vols) >= 21 else vols[:-1]
        vmed = sorted(vol_window)[len(vol_window) // 2] if vol_window else cur_vol
        if T2_VOL_EXHAUSTION > 0 and cur_vol > T2_VOL_EXHAUSTION * vmed:   # 0 = filter disabled
            return
        now_ms = int(c_last["time"])
        max_age_ms = T2_BLOCK_MAX_AGE_DAYS * 86400000
        # 2026-08-16: TWO engines share ONE fresh-block edge, opposite sides:
        #   Tight 2 (t2_auto_trade_enabled) = SHORT resistance-block retest
        #   Tight 1 (t3_auto_trade_enabled) = LONG  demand-block  retest
        for blk in blocks:
            lvl, formed_ms = blk[0], blk[1]
            side = blk[2] if len(blk) > 2 else "S"
            if formed_ms >= now_ms:
                continue
            if T2_BLOCK_MAX_AGE_DAYS > 0 and (now_ms - formed_ms) > max_age_ms:
                continue   # stale block - trapped holders already exited, level dead
            if side == "S" and t2_auto_trade_enabled:
                # price returning UP into resistance from below (band), close still below → SHORT
                if price < lvl and hi >= lvl * (1 - T2_RET_BAND_PCT / 100) and hi <= lvl * (1 + T2_RET_BAND_PCT / 100):
                    sl = round(lvl + atr_now * T2_SL_ATR_BUF, 6)
                    if sl <= price:
                        continue
                    if (sl - price) / price > SL_DIST_CAP:   # SLcap - skip wide-SL that would liquidate before SL
                        continue
                    t2_fire_entry(symbol, price, sl, "SELL")
                    return
            elif side == "L" and t3_auto_trade_enabled:
                # price returning DOWN into demand from above (band), close still above → LONG (Tight 1)
                if price > lvl and lo <= lvl * (1 + T2_RET_BAND_PCT / 100) and lo >= lvl * (1 - T2_RET_BAND_PCT / 100):
                    sl = round(lvl - atr_now * T2_SL_ATR_BUF, 6)
                    if sl >= price:
                        continue
                    if (price - sl) / price > SL_DIST_CAP:
                        continue
                    t2_fire_entry(symbol, price, sl, "BUY")
                    return
    except Exception as e:
        print(f"[T2 ENTRY {symbol}] error: {e}")

def t2_fire_entry(symbol, entry_px, sl, side="SELL"):
    if WHITELIST_ONLY and symbol not in BACKTESTED_COINS:
        print(f"[WHITELIST BLOCK] {symbol} not in backtested set - skipped")
        return
    if t2_symbol_has_open_trade(symbol):
        return
    risk = abs(sl - entry_px)
    if risk <= 0:
        return
    # 2026-08-11 DRAWDOWN FILTER (Faisal's insight): for demand-LONG, skip coins deep below their
    # 60-day high - heavy-downtrend coins hit SL (near) but never TP (far, at old price). Longs only;
    # shorts are unaffected (they profit from downtrend).
    if side == "BUY":
        try:
            dcandles = get_candles(symbol, limit=65, interval="1d")
            if len(dcandles) >= 30:
                hi60 = max(h(d) for d in dcandles[-61:-1])
                if hi60 > 0:
                    drawdown = (hi60 - entry_px) / hi60
                    if drawdown > T2_MAX_DRAWDOWN_PCT:
                        print(f"[T2 DD SKIP] {symbol} long skipped - {drawdown*100:.0f}% below 60d high (>{T2_MAX_DRAWDOWN_PCT*100:.0f}%)")
                        return
        except Exception as e:
            print(f"[T2 DD CHECK {symbol}] error: {e}")
    # 2026-08-16: ALL trades flat $5 risk (Faisal: "shob risk $5, er beshi na"). Confluence
    # sizing-up DROPPED (it was backtest-worse anyway). No dedicated conf slot -> plain cap gate.
    if not slot_available_for(False):
        print(f"[SLOT] {symbol} skipped - shared cap ({SHARED_MAX_CONCURRENT}) reached")
        return
    if side == "SELL":
        engine = "TIGHT 2"; label = "SHORT"; kind = "Trapped-block fade"
    else:
        engine = "TIGHT 1"; label = "LONG";  kind = "Demand-block bounce"
    risk_usdt = T2_RISK_USDT   # flat $5
    send_tg(
        engine + " " + label + " - " + symbol + "\n"
        "Entry: " + str(round(entry_px, 6)) + " | SL: " + str(sl) + " | Risk: $" + str(risk_usdt) + " | Lev: " + str(T2_LEVERAGE) + "x\n"
        + kind + " | TP " + str(T2_TP_R) + "R (fresh<" + str(T2_BLOCK_MAX_AGE_DAYS) + "d)\n"
    )
    oid = place_t2_order(symbol, entry_px, sl, side, False)
    if oid == "DEPTH_SKIP":
        return   # thin book - don't set cooldown, let it retry when the book fills
    t2_last_fire[symbol] = time.time()

def place_t2_order(symbol, entry, sl, side="SELL", conf=False):
    try:
        set_leverage_api(symbol, T2_LEVERAGE)
        precision = symbol_precision.get(symbol, 4)
        risk_dist = abs(sl - entry)
        if risk_dist <= 0:
            return None
        risk_usdt      = T3_CONF_RISK_USDT if conf else T2_RISK_USDT
        risk_qty       = risk_usdt / risk_dist
        margin_cap_qty = (T2_MAX_MARGIN_USDT * T2_LEVERAGE) / entry
        qty = round(min(risk_qty, margin_cap_qty), precision)
        if qty <= 0:
            return None
        # 2026-08-12 DEPTH CHECK: skip if the book is too thin between entry and SL (would slip badly)
        if not depth_ok(symbol, entry, sl, qty, side):
            send_tg("TIGHT 2 " + symbol + " " + side + " skipped - order book too thin (slippage guard)")
            return "DEPTH_SKIP"
        if side == "SELL":
            pos_side, close_side = "SHORT", "BUY"
        else:
            pos_side, close_side = "LONG", "SELL"

        required_margin = qty * entry / T2_LEVERAGE
        avail = get_available_margin()
        if avail is not None and avail < required_margin * 1.05:
            print(f"[T2 MARGIN SKIP] {symbol} need ~${required_margin:.2f}, available ${avail:.2f}")
            return "MARGIN_SKIP"

        order_id = place_market_order(symbol, side, qty, pos_side)
        print(f"[T2 ORDER] {symbol} {pos_side} qty={qty} risk=${risk_usdt}: {order_id}")
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
            if side == "SELL":
                tp_price = round(entry_fill - rd * T2_TP_R, 6)   # short TP below entry
            else:
                tp_price = round(entry_fill + rd * T2_TP_R, 6)   # long TP above entry
            tp_id = place_tp_guarded(symbol, close_side, pos_side, tp_price, qty, label="TP-" + str(T2_TP_R) + "R")
            t2_open_trades[str(order_id)] = {
                "symbol": symbol, "side": side, "entry": entry, "entry_fill": entry_fill,
                "sl": sl, "sl_id": sl_id, "tp": tp_price, "tp_id": tp_id,
                "total_qty": qty, "close_side": close_side, "pos_side": pos_side,
                "risk_dist": rd, "risk_usdt": risk_usdt, "confluence": conf,
                "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
                "open_ts": time.time(),
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
        is_short = trade.get("side", "SELL") == "SELL"
        try:
            # ---- TP fill ----
            tp_status = check_order_status(trade["tp_id"], symbol) if trade.get("tp_id") and trade["tp_id"] != "N/A" else ""
            if tp_status == "FILLED":
                tp_fill = get_fill_price(trade["tp_id"], symbol, fallback=trade["tp"])
                leg_pnl = (entry_ref - tp_fill) * trade["total_qty"] if is_short else (tp_fill - entry_ref) * trade["total_qty"]
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
                leg_pnl = (entry_ref - sl_fill) * trade["total_qty"] if is_short else (sl_fill - entry_ref) * trade["total_qty"]
                if trade.get("tp_id") and trade["tp_id"] != "N/A":
                    cancel_order(symbol, trade["tp_id"])
                trade["pnl"]    = round(leg_pnl, 2)
                trade["result"] = "SL"
                trade["exit_r"] = round(leg_pnl / trade.get("risk_usdt", T2_RISK_USDT), 2) if trade.get("risk_usdt") else 0.0
                trade["label"]  = "Tight 2"
                daily_trades.append(trade)
                journal_closed_trade(trade)
                t2_open_trades.pop(oid, None)
                t2_loss_until[symbol] = time.time() + T2_LOSS_COOLDOWN_DAYS * 86400  # loss ban: don't re-trade this coin for N days
                print(f"[T2 CLOSE] {symbol} SL pnl={trade['pnl']} R={trade['exit_r']} -> loss-cooldown {T2_LOSS_COOLDOWN_DAYS}d")
                continue
            # ---- 2026-08-11 TIME-STOP: close at market if held > 2 days (won't reach TP, frees margin) ----
            held = time.time() - trade.get("open_ts", time.time())
            if held >= TIGHT_MAX_HOLD_SECONDS:
                exit_price = get_current_price(symbol)
                if trade.get("total_qty", 0) > 0:
                    close_oid = place_market_order(symbol, trade["close_side"], trade["total_qty"], trade["pos_side"])
                    if close_oid and close_oid != "N/A":
                        time.sleep(0.5)
                        exit_price = get_fill_price(close_oid, symbol, fallback=exit_price)
                if trade.get("sl_id"):
                    cancel_order(symbol, trade["sl_id"])
                if trade.get("tp_id") and trade["tp_id"] != "N/A":
                    cancel_order(symbol, trade["tp_id"])
                leg_pnl = (entry_ref - exit_price) * trade["total_qty"] if is_short else (exit_price - entry_ref) * trade["total_qty"]
                trade["pnl"]    = round(leg_pnl, 2)
                trade["result"] = "TimeExit"
                trade["exit_r"] = round(leg_pnl / trade.get("risk_usdt", T2_RISK_USDT), 2) if trade.get("risk_usdt") else 0.0
                trade["label"]  = "Tight 2"
                daily_trades.append(trade)
                journal_closed_trade(trade)
                t2_open_trades.pop(oid, None)
                print(f"[T2 TIME-STOP] {symbol} closed after {held/86400:.1f}d pnl={trade['pnl']}")
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
                    # run the scan if EITHER engine is on (T2=short side, T1=long side; both fresh-block)
                    if not (t2_auto_trade_enabled or t3_auto_trade_enabled):
                        break
                    if not slot_available_for(False):   # plain shared cap, no conf slot
                        break
                    if t2_symbol_has_open_trade(sym) or t2_in_cooldown(sym):
                        continue
                    t2_check_entry(sym, blocks)
                    checked += 1
                    time.sleep(0.2)
                last_entry = time.time()
                print(f"[SCAN] blocks={len(t2_blocks)} checked={checked} open={len(t2_open_trades)} T2(short)={t2_auto_trade_enabled} T1(long)={t3_auto_trade_enabled}")
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
            have_open = bool(t3_open_trades or t2_open_trades)
            if have_open:
                positions = get_open_positions()
                # get_open_positions returns [] on genuine flat AND on API failure. We
                # treat [] as a valid "nothing open" set here; the per-symbol open-orders
                # verify inside each tracker is what actually guards against false flags,
                # so an API glitch that also drops the open-orders call cannot liquidate.
                open_syms = {p["symbol"] for p in positions}

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
    global t3_auto_trade_enabled, t2_auto_trade_enabled
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
                # ---- Tight 1 = LONG fresh demand-block: /t1_start /t1_stop ----
                elif text == "/t1_start":
                    t3_auto_trade_enabled = True
                    send_tg("Tight 1 (LONG demand-block) Auto-trade ON.")
                elif text == "/t1_stop":
                    t3_auto_trade_enabled = False
                    send_tg("Tight 1 (LONG demand-block) Auto-trade OFF.")
                # ---- /status : everything at a glance ----
                elif text == "/status":
                    backoff = ""
                    if api_backoff_active():
                        backoff = f"\nAPI BACKOFF ACTIVE - {int(_api_backoff_until - time.time())}s remaining"
                    longs  = sum(1 for t in t2_open_trades.values() if t.get("side") == "BUY")
                    shorts = sum(1 for t in t2_open_trades.values() if t.get("side") == "SELL")
                    send_tg(
                        "===== NITI BOT STATUS =====\n"
                        "Tight 1 (LONG demand-block): " + ("ON" if t3_auto_trade_enabled else "OFF") + " | Open longs: " + str(longs) + "\n"
                        "Tight 2 (SHORT resistance-block): " + ("ON" if t2_auto_trade_enabled else "OFF") + " | Open shorts: " + str(shorts) + "\n"
                        "Fresh-block <" + str(T2_BLOCK_MAX_AGE_DAYS) + "d | TP " + str(T2_TP_R) + "R | risk $" + str(int(T2_RISK_USDT)) + " flat | Blocks: " + str(len(t2_blocks)) + "\n"
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
        except Exception as e:
            print(f"[TG CMD] error: {e}")
        time.sleep(1)


@app.route("/")
def health():
    return ("Niti combined - Tight 1 (Dormant Awakening) + Tight 2 (Trapped-Block Fade, both sides) "
            "+ Confluence dedicated slot + OI collector + consolidated journal"), 200


# ============================================================================
# ==================== OI COLLECTOR (add-on, 2026-08-10) =====================
# Standalone open-interest logger -> Supabase. Own thread. Reads the symbol list
# and writes to Supabase only; touches no trading engine. Needs two Render env
# vars: SUPABASE_URL and SUPABASE_SERVICE_KEY. Table oi_history must exist.
# ============================================================================
def get_open_interest(symbol):
    """BingX public open interest for one symbol (no key/signature). float or None."""
    try:
        url = BASE_URL + "/openApi/swap/v2/quote/openInterest"
        r = requests.get(url, params={"symbol": symbol}, timeout=10).json()
        if r.get("code") == 0:
            oi = r.get("data", {}).get("openInterest")
            if oi is not None:
                return float(oi)
    except Exception as e:
        print(f"[OI FETCH ERROR] {symbol}: {e}")
    return None


def supabase_insert_oi(rows):
    if not rows:
        return
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("[OI SUPABASE] SUPABASE_URL / SUPABASE_SERVICE_KEY not set - skipping insert")
        return
    try:
        url = SUPABASE_URL + "/rest/v1/oi_history"
        headers = {
            "apikey": SUPABASE_SERVICE_KEY,
            "Authorization": "Bearer " + SUPABASE_SERVICE_KEY,
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }
        resp = requests.post(url, headers=headers, json=rows, timeout=15)
        if resp.status_code not in (200, 201, 204):
            print(f"[OI SUPABASE] insert failed {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        print(f"[OI SUPABASE ERROR] {e}")


def oi_collector_loop():
    """Every OI_SCAN_INTERVAL_SECONDS: pull OI for all futures symbols, compute USD
    value (OI x price), batch-insert into Supabase. Respects the global API backoff."""
    print(f"OI collector started - logging open interest for all symbols every {OI_SCAN_INTERVAL_SECONDS // 60} min to Supabase")
    all_symbols = []
    while True:
        try:
            if api_backoff_active():
                time.sleep(30)
                continue
            if not all_symbols:
                all_symbols = get_futures_symbols() or []
            now_ms = int(time.time() * 1000)
            rows = []
            logged = 0
            for sym in all_symbols:
                if api_backoff_active():
                    break
                oi = get_open_interest(sym)
                if oi is not None:
                    price = get_current_price(sym)
                    oi_value = oi * price if price and price > 0 else 0.0
                    rows.append({"symbol": sym, "ts": now_ms, "open_interest": oi, "oi_value": oi_value})
                    logged += 1
                    if len(rows) >= OI_BATCH_INSERT:
                        supabase_insert_oi(rows)
                        rows = []
                time.sleep(OI_REQUEST_PAUSE)
            if rows:
                supabase_insert_oi(rows)
            print(f"[OI COLLECTOR] logged OI for {logged} symbols at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
        except Exception as e:
            print(f"[OI COLLECTOR ERROR] {e}")
        time.sleep(OI_SCAN_INTERVAL_SECONDS)
# ==================== END OI COLLECTOR ====================


if __name__ == "__main__":
    # Re-adopt anything already open on BingX BEFORE the engines start, so a restart
    # can't breach the concurrency caps or orphan a trail-managed position.
    try:
        get_futures_symbols()          # populate symbol_precision / max_lev first
        adopt_positions_on_start()
    except Exception as e:
        print(f"[STARTUP ADOPT ERROR] {e}")

    Thread(target=t2_loop,                  daemon=True).start()   # Tight 2 fade (replaces retired RSI on /start /stop)
    Thread(target=trailing_loop,            daemon=True).start()
    Thread(target=t3_loop,                  daemon=True).start()
    Thread(target=handle_telegram_commands, daemon=True).start()
    Thread(target=oi_collector_loop,        daemon=True).start()   # OI logger -> Supabase (2026-08-10)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
