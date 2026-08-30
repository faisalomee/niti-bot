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
        # An SL market-close sweeps the book from the BEST price outward until qty is filled;
        # it is NOT confined to the entry->SL band. The old code summed only the thin slice
        # between entry and SL, which is almost always < qty*3 -> it silently killed ~every
        # trade (the "no trades in 24h across all 3 engines" bug). Correct check: is there
        # at least qty*DEPTH_LIQUIDITY_MULT resting on the side we'd sweep, within a sane
        # slippage cap (2x the entry->SL distance) of entry.
        risk_dist = abs(sl - entry)
        cap = entry + (2 * risk_dist if side == "SELL" else -2 * risk_dist)
        if side == "SELL":
            lo_p, hi_p = entry, max(entry, cap)          # buy back -> walk asks up
            avail = sum(float(q) for p, q in ob["asks"] if lo_p <= float(p) <= hi_p)
        else:
            lo_p, hi_p = min(entry, cap), entry          # sell out -> walk bids down
            avail = sum(float(q) for p, q in ob["bids"] if lo_p <= float(p) <= hi_p)
        need = qty * DEPTH_LIQUIDITY_MULT
        if avail < need:
            print(f"[DEPTH SKIP] {symbol} {side} thin book: {avail:.2f} within 2xSL of entry < need {need:.2f} (qty {qty} x{DEPTH_LIQUIDITY_MULT})")
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
    # 2026-08-28: the threshold used to be `min(limit, 50)`, which meant a caller asking
    # for 115 and getting 114 - or even getting 51 - never logged anything. That blind
    # spot is what let the rev-engine candle bug run unnoticed. Now it reports any
    # shortfall worth acting on, while tolerating the routine off-by-a-couple that
    # happens when the newest bar has not closed yet.
    if len(candles) < limit - 2:
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


def _bar_ms(c):
    """Open-time of a candle in ms, or 0 if the field is missing/unparseable.
    Callers use `ts % interval_ms == 0` to tell a CLOSED bar from a still-forming
    one: a real kline open_time is always a multiple of the interval, while BingX
    stamps the live bar with the current server time (observed 2026-08-28:
    ...914000000, i.e. 100s past a 15m boundary). Returning 0 on failure makes the
    caller treat the bar as closed, i.e. change nothing - fail safe, not fail open."""
    try:
        return int(c.get("time") or c.get("T") or 0)
    except (TypeError, ValueError):
        return 0


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


def resolve_exit(trade, side, entry_fill):
    """2026-08-25 JOURNAL FIX. The old code guessed the outcome by comparing the CURRENT
    price (fetched minutes after the position vanished) against tp/sl, then reported a
    THEORETICAL pnl (risk x tp_r on a TP, -risk on an SL). Three consequences:
      - an SL that the price later retraced past was logged as 'closed'
      - every TP logged the same idealised figure regardless of the real fill
      - unrecognised exits logged pnl 0, so real losses vanished from the totals
    Now we ask the exchange for the actual average fill of the SL and TP orders and
    compute pnl from that. Returns (result, pnl, exit_r, exit_px).
    """
    sl_px = tp_px = 0.0
    try:
        if trade.get("sl_id") and trade["sl_id"] != "N/A":
            sl_px = get_fill_price(trade["sl_id"], trade["symbol"], 0.0)
    except Exception:
        pass
    try:
        if trade.get("tp_id") and trade["tp_id"] != "N/A":
            tp_px = get_fill_price(trade["tp_id"], trade["symbol"], 0.0)
    except Exception:
        pass

    result, exit_px = "closed", 0.0
    if tp_px > 0 and sl_px > 0:
        # both report a fill: trust whichever is closer to the recorded target
        d_tp = abs(tp_px - float(trade.get("tp", tp_px)))
        d_sl = abs(sl_px - float(trade.get("sl", sl_px)))
        result, exit_px = ("TP", tp_px) if d_tp <= d_sl else ("SL", sl_px)
    elif tp_px > 0:
        result, exit_px = "TP", tp_px
    elif sl_px > 0:
        result, exit_px = "SL", sl_px

    if exit_px <= 0:
        # exchange gave us nothing (manual close, liquidation, API hiccup) - fall back to
        # the last traded price, but keep the label honest instead of pretending it is 0.
        try:
            exit_px = get_current_price(trade["symbol"]) or 0.0
        except Exception:
            exit_px = 0.0
        result = "closed"

    qty = 0.0
    try:
        qty = float(trade.get("qty", 0) or 0)
    except Exception:
        qty = 0.0

    pnl, exit_r = 0.0, 0.0
    if exit_px > 0 and entry_fill and entry_fill > 0:
        move = (exit_px - entry_fill) if side == "BUY" else (entry_fill - exit_px)
        if qty > 0:
            pnl = move * qty
        rd = abs(entry_fill - float(trade.get("sl", 0) or 0))
        if rd > 0:
            exit_r = round(move / rd, 2)
            if qty <= 0:
                pnl = exit_r * float(trade.get("risk_usdt", 0) or 0)
    return result, round(pnl, 2), exit_r, exit_px


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
        + ("Exit  : " + str(trade["exit_px"]) + "\n" if trade.get("exit_px") else "")
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


def get_balance():
    """Total USDT equity (available + margin already committed). Used for the shared
    margin budget so the cap does not shrink as trades open. None on failure."""
    try:
        params = build_signed_params({})
        url = BASE_URL + "/openApi/swap/v2/user/balance"
        r = requests.get(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        bal = r.get("data", {}).get("balance", {})
        if isinstance(bal, list):
            bal = next((b for b in bal if b.get("asset") == "USDT"), bal[0] if bal else {})
        eq = float(bal.get("equity", 0) or bal.get("balance", 0) or 0)
        return eq if eq > 0 else None
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
    forgets every open position.

    2026-08-24 REWRITE. The old version adopted ONLY LONG positions and handed them to the
    retired block-based Tight 2 tracker. Every engine now running (T1, T2, T3) is
    predominantly SHORT, so in practice a restart orphaned essentially every live trade:
    its SL/TP survived on the exchange (no liquidation risk), but the bot no longer knew
    about it, which meant NO time-stop (margin locked indefinitely) and NO slot accounting
    (the engine could re-fill all its slots on top of the forgotten position). Observed live
    on 2026-08-24: T1 held one position from 10:47, the service restarted at 12:28, and the
    position kept running untracked.

    Now: BOTH sides are adopted, into the rev (Tight 1) tracker, which handles long and short
    and applies the time-stop. Notes on the deliberate simplifications:
      - Engine attribution is not recoverable from the exchange (a T1/T2/T3 short looks
        identical), so everything lands in Tight 1. That is fine: the tracker only needs
        entry/SL/TP/qty to manage and journal the trade, and all_open_symbols() then keeps
        every other engine off that coin.
      - open_ts is set to NOW, not to the real entry time (which is unknown after a restart).
        The time-stop therefore restarts from adoption - conservative, never premature.
      - risk_usdt is computed from the REAL distance to the stop x qty, so PnL reporting on
        the adopted trade is honest rather than assuming the flat $5.
    """
    try:
        positions = get_open_positions()
    except Exception as e:
        print(f"[ADOPT ERROR] {e}")
        return
    if not positions:
        print("[ADOPT] no open positions on BingX to re-adopt")
        return
    tracked = all_open_symbols()
    adopted = 0
    lines = []
    now = time.time()
    for p in positions:
        sym, amt, avg = p["symbol"], p["amt"], p["avg"]
        if not sym or amt <= 0 or avg <= 0:
            continue
        if sym in tracked:
            continue
        is_long = (p["pos_side"] == "LONG")
        side       = "BUY"  if is_long else "SELL"
        pos_side   = "LONG" if is_long else "SHORT"
        close_side = "SELL" if is_long else "BUY"

        sl_id, sl_price, tp_id, tp_price = get_open_orders_for(sym)

        # No stop on the exchange -> place a protective one immediately. A position with no
        # stop is the one situation that can actually blow up the account.
        if sl_id is None:
            fallback_sl = round(avg * (0.95 if is_long else 1.05), 6)
            sl_id = place_sl_guarded(sym, close_side, pos_side, fallback_sl, amt)
            sl_price = fallback_sl
            if sl_id is None:
                send_tg("\u26a0\ufe0f ADOPT WARNING - " + sym + " " + pos_side +
                        " has NO stop-loss on the exchange and one could NOT be placed. "
                        "Set an SL manually NOW.")
            else:
                send_tg("ADOPT - " + sym + " " + pos_side + " had no SL - placed a protective 5% stop at "
                        + str(fallback_sl))

        if not sl_price or sl_price <= 0:
            sl_price = round(avg * (0.95 if is_long else 1.05), 6)
        risk_dist = abs(avg - sl_price)
        if risk_dist <= 0:
            risk_dist = avg * 0.05
        risk_usdt = round(risk_dist * amt, 2) or REV_RISK_USDT

        # If there is no TP on the exchange, leave tp as None - rev_track_trades reconciles
        # on the position disappearing and on the time-stop either way.
        rev_open_trades["adopt-" + sym] = {
            "symbol": sym, "side": side, "pos_side": pos_side, "close_side": close_side,
            "entry": avg, "entry_fill": avg,
            "sl": sl_price, "tp": tp_price if tp_price else None,
            "sl_id": sl_id, "tp_id": tp_id,
            "total_qty": amt, "risk_usdt": risk_usdt,
            "open_ts": now, "gone_strikes": 0,
            "label": "TIGHT 1 (adopted)", "eng_tag": "t1", "adopted": True,
            "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
        }
        adopted += 1
        lines.append(sym + " " + pos_side + " qty " + str(amt) + " | SL " + str(sl_price) +
                     (" | TP " + str(tp_price) if tp_price else " | no TP on exchange") +
                     " -> adopted (time-stop restarts now)")

    print(f"[ADOPT] re-adopted {adopted} position(s) into the Tight 1 tracker")
    if lines:
        send_tg("STARTUP RE-ADOPT\n------------------------------\n" + "\n".join(lines) +
                "\n------------------------------\nConcurrency caps and the time-stop now account for these.")


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
T2_DUMP_PCT               = 14.0   # 2026-08-17: 18->14 (holdout-verified, test>=train, 9/9mo). 18% dump is rare in calm markets = block feedstock dried up. 14% forms more blocks, edge nearly intact (2.05R->1.85R base), ~more trades. Do NOT go below 12 (weakens).
T2_DUMP_LOOKAHEAD_DAYS    = 3      # 2026-08-17: 5->3. BIGGEST find - raises BOTH meanR AND win (LA3 lifts base ~2.25R->2.78R, win ->81%, holdout-verified) AND shrinks the always-blind confirm window from 5d to 3d (helps "no recent trades" too). Do NOT combine with TP5 (81% win does not survive that combo).
T2_VOL_SPIKE              = 1.5    # 2026-08-17: 1.3->1.5 (small clean gain, holdout-verified). Cleaner blocks; slightly fewer but stronger. Combined LA3+vol1.5+TP4 cap3 = 2.27R test2.31/win75-76% 9/9mo.  [was 1.3 with 3M floor+DD filter]
T2_MAX_DRAWDOWN_PCT       = float(os.environ.get("T2_MAX_DRAWDOWN_PCT", 0.35))   # 2026-08-16: 0.15->0.35 (bear-trap fix). DD15 killed ~100% of longs in a downtrend (every coin >15% below 60d high) -> short-only -> account $80->$40. DD35 restores long/short balance; holdout A+0.20/B+0.16, recent-bear positive.  [was 0.30->0.15.] Demand-LONG only works on STRONG coins near their 60d high. Feature analysis: coin 5-15% below high = win 74%, but 15-30% below = win 45% (support breaks on downtrend coins). Tightening to 15% raises LONG win 47->62% and overall win 52->62%, PnL $669->$1054, 9/9 months+, holdout A67%/B66%. Keeps both-sides (bull-run safe). SHORT unaffected.
T2_VOL_EXHAUSTION         = 0      # HARDCODED. DISABLED (0=off) - exh0.8 killed 87% of signals
T2_SL_ATR_BUF             = 0.5    # HARDCODED. SL nearer level = more R per move
T2_TP_R                   = 4.0    # 2026-08-17: 3->4 (holdout: TP3=1.89R/w77, TP4=2.25R/w74, TP5=2.65R/w74; TP4 chosen over TP5 - TP5 far target=longer holds, low patience). RR = 4:1.
T2_BLOCK_MAX_AGE_DAYS     = 14     # 2026-08-17: 7->14 (holdout-verified, edge intact, keeps more blocks alive in calm markets so fewer zero-trade stretches). Age barely affects meanR; DUMP% is the real trade lever.  [was 30->7]
T2_LONG_SIDE              = False  # 2026-08-20: OFF. The LONG demand-block leg WAS the old "T1"; under strict
                                   # no-lookahead timing it measured -0.211R (worse than random), so the T1 slot is
                                   # now the 24h-reversion engine below and T2 stays SHORT-only. Old note said both-sides (short resistance + long demand): 26.7 trades/wk $1049 vs short-only 10.4/wk $594, fully validated.
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
T2_EXCLUDE_TOP_N          = 20     # 2026-08-17: 0->20. Top-20 $vol coins DO form/retest blocks but fewer & weaker (~1.82R vs ~2.05R for mid-liquid). Best bucket = 2M+ EXCLUDING top-20. get_liquid_symbols already sorts by $vol desc and slices [exclude_top_n:].
T2_MIN_QUOTE_VOL          = 2_000_000  # 2026-08-12: 3M->2M. Backtest: 2M recovers ~$120 vs 3M ($825 vs $704 cap2) with holdout still robust (A+1.09/B+0.89) and still excludes RIF-tier thin coins that slipped SL live. 3M was over-conservative; 2M is the sweet spot between live-slippage safety and PnL.

# Shared T1+T2 concurrency cap: on a $100 account both engines TOGETHER = 2 open.
# (Backtest: shared-max2 $534 DD-$33 is the best risk-adj; raise once balance grows.)
SHARED_MAX_CONCURRENT     = 3      # 2026-08-17: 4->3 (drawdown control on $200 acct). Backtest cap4 keeps edge (2.27R) but maxDD 4.1R->6.2R & worst-24h -6.2R & up to 4 concurrent losers; cap3 = maxDD 5.4R, ~43 trades/mo, edge intact. T1+T2 (swing) share these slots. T3 scalp has its OWN separate cap.  [was 2->4]
CONFLUENCE_EXTRA_SLOTS    = 1      # 2026-08-12: a CONFLUENCE trade (T1+T2 agree same coin+dir within 5d)
                                   # may open ONE dedicated slot BEYOND the 2 normal slots, so the bot's
                                   # highest-conviction trades are never dropped by the cap. Backtest cap2+1conf:
                                   # $966->$2285, win 42->46%, holdout A+$1030/B+$1320. Keep conf risk $10 (not higher)
                                   # on a small account - 3 open positions is the max margin exposure.

t2_auto_trade_enabled = False   # 2026-08-20: old block-based Tight 2 RETIRED (replaced by rev2). Hard-off.
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
    missing when CF + Tight 2 both opened 1000BONK at once and one got liquidated.

    2026-08-20 BUGFIX: t3s_open_trades (OI-short scalp) and rev_open_trades were
    MISSING from this set. track_t3_scalp_trades() calls this to decide whether its
    own position is gone; because its own trades were never in the set, EVERY T3
    scalp was declared "closed" on the first tracking pass (~16 min), the bot popped
    it from its dict and sent a "closed" Telegram - while the position stayed OPEN
    and unmanaged on BingX (COMP-USDT 2026-08-19). Every engine's dict belongs here."""
    syms = set()
    for d in (t3_open_trades, t2_open_trades, t3s_open_trades, rev_open_trades,
              rev2_open_trades, rev4_open_trades, rev5_open_trades):
        for t in d.values():
            s = t.get("symbol")
            if s:
                syms.add(s)
    return syms


def exchange_has_position(symbol):
    """Ground truth from BingX, not from an in-memory dict. Returns True/False, or
    None when the API call fails (caller must treat None as 'unknown, do nothing' -
    never as 'position gone', which is what caused the phantom-close bug)."""
    try:
        positions = get_open_positions()
    except Exception as e:
        print(f"[POS CHECK] {symbol}: {e}")
        return None
    if positions is None:
        return None
    for p in positions:
        try:
            if p.get("symbol") != symbol:
                continue
            # get_open_positions() returns the size under "amt" (already abs, >0 filtered),
            # NOT "positionAmt". The old key was always 0 -> exchange_has_position always
            # returned False -> EVERY rev trade phantom-closed within ~2 cycles (~5 min),
            # leaving the real position naked on BingX. This is that fix.
            amt = p.get("amt", p.get("positionAmt", 0))
            if abs(float(amt or 0)) > 0:
                return True
        except Exception:
            continue
    return False

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
                    globals()["_t2_band_hits"] = globals().get("_t2_band_hits", 0) + 1
                    sl = round(lvl + atr_now * T2_SL_ATR_BUF, 6)
                    if sl <= price:
                        continue
                    if (sl - price) / price > SL_DIST_CAP:   # SLcap - skip wide-SL that would liquidate before SL
                        globals()["_t2_slcap_skips"] = globals().get("_t2_slcap_skips", 0) + 1
                        continue
                    t2_fire_entry(symbol, price, sl, "SELL")
                    return
            elif side == "L" and t3_auto_trade_enabled:
                # price returning DOWN into demand from above (band), close still above → LONG (Tight 1)
                if price > lvl and lo <= lvl * (1 + T2_RET_BAND_PCT / 100) and lo >= lvl * (1 - T2_RET_BAND_PCT / 100):
                    globals()["_t2_band_hits"] = globals().get("_t2_band_hits", 0) + 1
                    sl = round(lvl - atr_now * T2_SL_ATR_BUF, 6)
                    if sl >= price:
                        continue
                    if (price - sl) / price > SL_DIST_CAP:
                        globals()["_t2_slcap_skips"] = globals().get("_t2_slcap_skips", 0) + 1
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
    header = (
        engine + " " + label + " - " + symbol + "\n"
        "Entry: " + str(round(entry_px, 6)) + " | SL: " + str(sl) + " | Risk: $" + str(risk_usdt) + " | Lev: " + str(T2_LEVERAGE) + "x\n"
        + kind + " | TP " + str(T2_TP_R) + "R (fresh<" + str(T2_BLOCK_MAX_AGE_DAYS) + "d)"
    )
    # place FIRST, then send ONE alert whose last line states the real outcome
    oid = place_t2_order(symbol, entry_px, sl, side, False)
    if oid == "DEPTH_SKIP":
        send_tg(header + "\n\u26a0\ufe0f TRADE SKIPPED - order book too thin (slippage guard)")
        return   # thin book - don't set cooldown, let it retry when the book fills
    if oid == "MARGIN_SKIP":
        send_tg(header + "\n\u26a0\ufe0f TRADE SKIPPED - not enough margin")
        return
    if oid is None or oid == "N/A":
        send_tg(header + "\n\u26a0\ufe0f TRADE SKIPPED - order failed")
        return
    send_tg(header + "\n\u2705 TRADE EXECUTED")
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
                _bh = globals().get("_t2_band_hits", 0)
                _sc = globals().get("_t2_slcap_skips", 0)
                print(f"[SCAN] blocks={len(t2_blocks)} checked={checked} open={len(t2_open_trades)} band_hits={_bh} slcap_skips={_sc} T2(short)={t2_auto_trade_enabled} T1(long)={t3_auto_trade_enabled}")
                globals()["_t2_band_hits"] = 0
                globals()["_t2_slcap_skips"] = 0
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
def _regime_line():
    """One-line summary of which of the T3/T4 pair the current BTC regime has armed.
    2026-08-28: they deliberately cover opposite regimes, so exactly one should be
    armed at a time. If BTC can't be read, BOTH stand down rather than guess."""
    try:
        b = rev_btc_regime()
    except Exception:
        b = None
    if b is None:
        return "Regime: BTC 4d unreadable - T3 and T4 both standing down"
    if b < T3_REGIME_MAX_BTC:
        who = "T3 armed (bear)"
    else:
        who = "T4 armed (bull/flat)"
    return f"Regime: BTC 4d {b*100:+.1f}% -> {who}"


def handle_telegram_commands():
    global t3_auto_trade_enabled, t2_auto_trade_enabled, t3_scalp_auto_enabled, rev_auto_enabled, rev2_auto_enabled, rev4_auto_enabled, rev5_auto_enabled
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
                    if not REV2_ENGINE_ENABLED:
                        send_tg("Tight 2 (24h-reversion ceiling) is disabled at build level (REV2_ENGINE_ENABLED=0).")
                    else:
                        rev2_auto_enabled = True
                        send_tg("Tight 2 (24h-reversion) Auto-trade ON.")
                elif text in ("/t2_stop", "/stop"):
                    rev2_auto_enabled = False
                    send_tg("Tight 2 (24h-reversion) Auto-trade OFF.")
                # ---- Tight 1 = 24h-reversion: /t1_start /t1_stop (/rev_* kept as alias) ----
                elif text in ("/t1_start", "/rev_start"):
                    if not REV_ENGINE_ENABLED:
                        send_tg("Tight 1 (24h-reversion) is disabled at build level (REV_ENGINE_ENABLED=0).")
                    else:
                        rev_auto_enabled = True
                        send_tg("Tight 1 (24h-reversion) Auto-trade ON.")
                elif text in ("/t1_stop", "/rev_stop"):
                    rev_auto_enabled = False
                    send_tg("Tight 1 (24h-reversion) Auto-trade OFF.")
                # ---- Tight 3 = S/R Sweep SHORT (bear engine): /t3_start /t3_stop ----
                elif text == "/t3_start":
                    if not T3_ENGINE_ENABLED:
                        send_tg("Tight 3 (HTF clean-break retest) is disabled at build level (T3_ENGINE_ENABLED=0).")
                    else:
                        t3_scalp_auto_enabled = True
                        send_tg("Tight 3 (HTF clean-break retest) Auto-trade ON.")
                elif text == "/t3_stop":
                    t3_scalp_auto_enabled = False
                    send_tg("Tight 3 (HTF clean-break retest) Auto-trade OFF.")
                elif text == "/t3_status":
                    lines_s = ("Tight 3 (HTF clean-break retest, both sides): " + ("ON" if t3_scalp_auto_enabled else "OFF") +
                               " | Open: " + str(len(t3s_open_trades)) + "/" + str(T3_MAX_CONCURRENT) +
                               " | Pending: " + str(len(t3s_pending)) +
                               "\nLevels: " + ",".join(T3B_TIMEFRAMES) + " pivots K=" + str(T3B_PIVOT_K) +
                               " | clean break only (<=1 prior touch)" +
                               "\nBreak vol >=" + str(T3B_VOL_MULT) + "x med | ext " +
                               str(round(T3B_EXT_MIN*100)) + "-" + str(round(T3B_EXT_MAX*100)) + "%" +
                               " | retest <=" + str(T3B_RETEST_MAX) + " bars, vol >= break vol" +
                               "\nSL " + str(T3B_SL_ATR) + "xATR | TP " + str(T3B_TP_R) + "R | ATR%>=" +
                               str(round(T3B_ATRP_MIN*100, 1)) + "% | vol>=$" + str(int(T3B_MIN_QUOTE_VOL/1e6)) + "M" +
                               " | risk $" + str(int(T3_RISK_USDT)) + " flat" +
                               "\nCached levels: " + str(sum(len(x) for x in t3b_levels.values())) +
                               " on " + str(len(t3b_levels)) + " coins")
                    for _sym, _p in list(t3s_pending.items()):
                        lines_s += "\n[pending] " + _sym + " " + _p.get("dir", "?") + " @ " + str(round(_p["entry"], 6))
                    for _oid, ts in list(t3s_open_trades.items()):
                        lines_s += ("\n" + ts["symbol"] + " " + ts.get("dir", "?") + " | entry " +
                                    str(ts.get("entry_fill", ts["entry"])) + " | SL " + str(round(ts["sl"], 6)) +
                                    " | TP " + str(round(ts["tp"], 6)))
                    send_tg(lines_s)
                # ---- Tight 1 = 24h-reversion (2026-08-20) ----
                # /rev_status kept as an alias for T1 detail
                elif text in ("/rev_status", "/t1_status"):
                    _btc = rev_btc_regime()
                    lines_r = ("Tight 1 (24h-reversion): " + ("ON" if rev_auto_enabled else "OFF") +
                               " | Open: " + str(len(rev_open_trades)) + "/" + str(REV_MAX_CONCURRENT) +
                               " | Pending: " + str(len(rev_pending)) +
                               "\nTrigger: " + str(round(REV_T1["ret_thr"] * 100, 1)) + "% over 24h + range edge + vol >=" +
                               str(REV_T1["vol_mult"]) + "x" +
                               "\nLONG SL " + str(REV_LONG_SL_ATR) + "xATR TP " + str(REV_LONG_TP_R) + "R | " +
                               "SHORT SL " + str(REV_SHORT_SL_ATR) + "xATR TP " + str(REV_SHORT_TP_R) + "R" +
                               "\nBTC 4d: " + (f"{_btc*100:+.1f}%" if _btc is not None else "n/a") +
                               " (gate +/-" + str(round(REV_BTC_THR * 100)) + "%)" +
                               " | risk $" + str(int(REV_RISK_USDT)))
                    for _sym, _p in list(rev_pending.items()):
                        lines_r += "\n[pending] " + _sym + " " + _p["pos_side"] + " @ " + str(round(_p["entry"], 6))
                    for _oid, _t in list(rev_open_trades.items()):
                        lines_r += ("\n" + _t["symbol"] + " " + _t["pos_side"] + " | entry " +
                                    str(_t.get("entry_fill", _t["entry"])) + " | SL " + str(round(_t["sl"], 6)) +
                                    " | TP " + str(round(_t["tp"], 6)))
                    send_tg(lines_r)
                # ---- Tight 2 detail (ceiling 24h-reversion) ----
                elif text == "/t2_status":
                    lines2 = ("Tight 2 (24h-reversion ceiling): " + ("ON" if rev2_auto_enabled else "OFF") +
                              " | Open: " + str(len(rev2_open_trades)) + "/" + str(REV2_MAX_CONCURRENT) +
                              " | Pending: " + str(len(rev2_pending)) +
                              "\nTrigger: " + str(round(REV2_RET_THR * 100, 1)) + "% over 24h + range edge + vol " +
                              str(REV2_VOL_MULT) + "-" + str(REV2_VOL_MULT_MAX) + "x + ATR%<" + str(round(REV2_ATRP_MAX*100,1)) + "%" +
                              " | risk $" + str(int(REV2_RISK_USDT)))
                    for _sym, _p in list(rev2_pending.items()):
                        lines2 += "\n[pending] " + _sym + " " + _p["pos_side"] + " @ " + str(round(_p["entry"], 6))
                    for _oid, _t in list(rev2_open_trades.items()):
                        lines2 += ("\n" + _t["symbol"] + " " + _t["pos_side"] + " | entry " +
                                   str(_t.get("entry_fill", _t["entry"])) + " | SL " + str(round(_t["sl"], 6)) +
                                   " | TP " + str(round(_t["tp"], 6)))
                    send_tg(lines2)
                # ---- Tight 4 = 4h range-extreme reversion SHORT: /t4_start /t4_stop ----
                elif text == "/t4_start":
                    if not REV4_ENGINE_ENABLED:
                        send_tg("Tight 4 (4h-reversion short) is disabled at build level (REV4_ENGINE_ENABLED=0).")
                    else:
                        rev4_auto_enabled = True
                        send_tg("Tight 4 (4h-reversion short) Auto-trade ON.")
                elif text == "/t5_start":
                    if not REV5_ENGINE_ENABLED:
                        send_tg("Tight 5 (crash-continuation short) is disabled at build level (REV5_ENGINE_ENABLED=0).")
                    else:
                        rev5_auto_enabled = True
                        send_tg("Tight 5 (crash-continuation short) Auto-trade ON.")
                elif text == "/t5_stop":
                    rev5_auto_enabled = False
                    send_tg("Tight 5 (crash-continuation short) Auto-trade OFF.")
                elif text == "/t4_stop":
                    rev4_auto_enabled = False
                    send_tg("Tight 4 (4h-reversion short) Auto-trade OFF.")
                elif text == "/t4_status":
                    _btc4 = rev_btc_regime()
                    _armed = (_btc4 is not None and _btc4 >= REV4_REGIME_MIN_BTC)
                    lines4 = ("Tight 4 (4h-reversion, SHORT only): " + ("ON" if rev4_auto_enabled else "OFF") +
                              " | Open: " + str(len(rev4_open_trades)) + "/" + str(REV4_MAX_CONCURRENT) +
                              " | Pending: " + str(len(rev4_pending)) +
                              "\nTrigger: " + str(round(REV4_RET_THR * 100, 1)) + "% over 4h + new 4h high + vol >=" +
                              str(REV4_VOL_MULT) + "x" +
                              "\nSL " + str(REV4_SL_ATR) + "xATR | TP " + str(REV4_TP_R) + "R | hold " +
                              str(REV_HOLD_SECONDS // 3600) + "h | risk $" + str(int(REV4_RISK_USDT)) +
                              "\nBTC 4d: " + (f"{_btc4*100:+.1f}%" if _btc4 is not None else "n/a") +
                              " | regime: " + ("ARMED (bull/flat)" if _armed else "STANDBY - bear is T3's shift") +
                              " (needs >= " + str(round(REV4_REGIME_MIN_BTC * 100)) + "%)")
                    for _sym, _p in list(rev4_pending.items()):
                        lines4 += "\n[pending] " + _sym + " " + _p["pos_side"] + " @ " + str(round(_p["entry"], 6))
                    for _oid, _t in list(rev4_open_trades.items()):
                        lines4 += ("\n" + _t["symbol"] + " " + _t["pos_side"] + " | entry " +
                                   str(_t.get("entry_fill", _t["entry"])) + " | SL " + str(round(_t["sl"], 6)) +
                                   " | TP " + str(round(_t["tp"], 6)))
                    send_tg(lines4)
                # ---- /status : everything at a glance ----
                elif text == "/status":
                    backoff = ""
                    if api_backoff_active():
                        backoff = f"\nAPI BACKOFF ACTIVE - {int(_api_backoff_until - time.time())}s remaining"
                    send_tg(
                        "===== NITI BOT STATUS =====\n"
                        "Tight 1 (24h-reversion): " + ("ON" if rev_auto_enabled else "OFF") +
                        " | Open: " + str(len(rev_open_trades)) + "/" + str(REV_MAX_CONCURRENT) +
                        " | Pending: " + str(len(rev_pending)) +
                        " | " + str(round(REV_RET_THR*100)) + "% vol>=" + str(REV_VOL_MULT) + "x | risk $" + str(int(REV_RISK_USDT)) + "\n"
                        "Tight 2 (24h-reversion ceiling): " + ("ON" if rev2_auto_enabled else "OFF") +
                        " | Open: " + str(len(rev2_open_trades)) + "/" + str(REV2_MAX_CONCURRENT) +
                        " | Pending: " + str(len(rev2_pending)) +
                        " | " + str(round(REV2_RET_THR*100)) + "% vol " + str(REV2_VOL_MULT) + "-" + str(REV2_VOL_MULT_MAX) + "x ATR%<" + str(round(REV2_ATRP_MAX*100,1)) + "% | risk $" + str(int(REV2_RISK_USDT)) + "\n"
                        "Tight 3 (HTF clean-break, BEAR only): " + ("ON" if t3_scalp_auto_enabled else "OFF") +
                        " | Open: " + str(len(t3s_open_trades)) + "/" + str(T3_MAX_CONCURRENT) +
                        " | Pending: " + str(len(t3s_pending)) +
                        " | TP " + str(T3_TP_R) + "R risk $" + str(int(T3_RISK_USDT)) + "\n"
                        "Tight 5 (crash-continuation SHORT, wide-range only): " + ("ON" if rev5_auto_enabled else "OFF") +
                        " | Open: " + str(len(rev5_open_trades)) + "/" + str(REV5_MAX_CONCURRENT) +
                        " | risk $" + str(REV5_RISK_USDT) + "\n" +
                        "Tight 4 (4h-reversion SHORT, bull/flat only): " + ("ON" if rev4_auto_enabled else "OFF") +
                        " | Open: " + str(len(rev4_open_trades)) + "/" + str(REV4_MAX_CONCURRENT) +
                        " | Pending: " + str(len(rev4_pending)) +
                        " | " + str(round(REV4_RET_THR*100, 1)) + "%/4h vol>=" + str(REV4_VOL_MULT) +
                        "x TP " + str(REV4_TP_R) + "R | risk $" + str(int(REV4_RISK_USDT)) + "\n" +
                        _regime_line() + backoff
                    )
        except Exception as e:
            print(f"[TG CMD] error: {e}")
        time.sleep(1)



# ============================================================================
# ============================================================================
# ==================== TIGHT 3 = S/R SWEEP SHORT (2026-08-23) =================
# Faisal's own SMC engine (Equal-Highs / strong-S-R liquidity sweep), polished
# from fee-dead to a real fee-surviving edge and coded as T3. SEPARATE engine
# from the swing 24h-reversion (T1/T2): own cap, own /t3_start. Reuses the same
# tested order infra (place_sl_guarded / place_tp_guarded / depth_ok / journal)
# and the same resting-limit -> pending -> filled flow as the rev engines.
#
# WHY THIS SHAPE (measured, not invented):
#   The raw sweep idea was DEAD after fees: the tight sweep-SL made risk/entry
#   tiny (~0.65%), so a 0.10% round-trip ate ~0.15R/trade and every variant went
#   negative. Three fixes turned it positive and all three are load-bearing:
#     1. rf>=0.8% filter  - drop signals whose SL is <0.8% from entry (the ones
#        fees destroy). THIS is the main lever.
#     2. BTC bear_soft gate - only short when BTC 4d-return <= +3% (this is a
#        directional SHORT engine; it loses in BTC up-months, so it stays out).
#     3. strong S/R cluster (>=2 swing-highs at one level), not just a double top.
#
# SIGNAL (all on 15m):
#   - levels come from 12H/1D/2D fractal pivots (K=3), rebuilt every 6h from 1h
#     klines; a pivot is only usable K+1 HTF bars after it printed (no lookahead).
#   - CLEAN BREAK: a 15m close beyond the level, on a level touched <=1 time before.
#   - the break bar's volume >= 2x the 96-bar median; price extends 1-12% past it.
#   - within 8 bars price returns, touches the level and CLOSES holding it, on
#     volume >= the break bar's volume -> resting LIMIT entry there, break direction.
#   - SL = 2.5xATR(15m), TP = 2R, 72h time-stop, own cap separate from T1/T2.
#   - BTC gate: skip if BTC 4d-return > T3_BTC_MAX (bear_soft = +3%).
#
# BACKTEST (8mo, 249-coin 15m, maker+taker fees, realistic slippage sim):
#   ~9 trades/wk, win ~35%, meanR +0.23, +$345 at flat $5 over 8mo, holdout
#   both halves positive, random-side control strongly negative (genuine edge).
#   HONEST: this is a BEAR/CORRECTION engine. It is strong in dump months and
#   weak/negative in BTC up-months - run it as a bear hedge, not all-weather.
#   Small live test sizing until proven forward.
# Commands: /t3_start /t3_stop /t3_status  (globals named t3s_* / T3_* so the
#   existing /status + command wiring is unchanged).
# ============================================================================
T3_ENGINE_ENABLED   = os.environ.get("T3_ENGINE_ENABLED", "0") == "1"   # master build flag
t3_scalp_auto_enabled = AUTO_RESUME_ON_START   # runtime on/off via /t3_start /t3_stop (name kept for /status wiring)

# ---------------------------------------------------------------------------
# TIGHT 3 = HTF CLEAN-BREAK RETEST  (2026-08-26 rebuild; replaces the S/R sweep
# short, which was measured dead: 3,888-combo sweep, 2% of TEST cells positive).
#
# THE RULE
#   1. Build fractal pivot levels on RESAMPLED higher-timeframe bars (12H / 1D / 2D)
#      from 1h klines. A level = the extreme of +/-K HTF bars, and it is only
#      knowable K+1 HTF bars later, so there is no lookahead.
#   2. CLEAN BREAK: price closes beyond the level by > T3B_TOL, and the level had
#      been touched EXACTLY ONCE before (i.e. it never turned price away). This is
#      the core discovery and it is the OPPOSITE of the textbook - measured over
#      all six timeframe x K combinations, "clean" levels were positive in BOTH
#      halves while already-tested levels were negative on TEST in all six.
#      A level with defenders behind it is a TRAP; a virgin level really breaks.
#   3. The break bar's volume >= T3B_VOL_MULT x the 96-bar median.
#   4. Price then extends T3B_EXT_MIN..T3B_EXT_MAX past the level.
#   5. Within T3B_RETEST_MAX bars it comes back, touches the level and CLOSES
#      holding it, on volume >= the break bar's volume.
#   6. Resting LIMIT entry at that retest close, in the break direction.
#      SL = T3B_SL_ATR x ATR(15m), TP = T3B_TP_R (RR 2.0 measured optimal).
#
# BACKTEST (Nov 2025 - Jul 2026, 15m, $2M excl-top-20, 20bps, flat $5)
#   n=454 | 12.6 trades/wk | meanR +0.390 | win 48.0% | PnL +$884
#   TRAIN +0.378 / TEST +0.405   (TEST is the better half - no decay)
#   Monthly 9/9 positive (worst month +0.206)
#   Side-flip -0.188 (Aug -0.289) -> gap +0.578 | placebo (tested levels) TEST -0.083
#   Holdout A +0.325 / B +0.471 | 162 coins, 99 positive, top-3 only 15% of PnL
#   SL x TP x hold grid, 36 cells: TEST median +0.279, 100% positive
#   Slippage: 0bps +0.426 | 50bps +0.335 | 100bps +0.244  (very cost-tolerant)
#   Untouched OOS 26 Jul-24 Aug 2026: n=19, meanR +0.695, win 57.9%
#   Legs: LONG +0.306 / SHORT +0.460 - both positive in both halves, so BOTH run.
#   Per timeframe: 12H +0.404 | 1D +0.352 | 2D +0.268 - all three work alone.
# HONEST LIMITS: ext and retest-window bounds were chosen on the full 8mo (not
# TRAIN-only); Aug OOS n is only 19. Keep T3_RISK_USDT small until live n grows.
#
# WHY THIS IS NOT A SCALP: shortening it destroys it (4h hold = +$40 vs 72h = +$884).
# It is born from multi-day levels and needs 2-3 days to run.
# ---------------------------------------------------------------------------
T3B_TIMEFRAMES      = os.environ.get("T3B_TIMEFRAMES", "12H,1D,2D").split(",")
T3B_PIVOT_K         = int(os.environ.get("T3B_PIVOT_K", 3))        # +/-3 HTF bars. BIGGEST lever: K1 +0.211 -> K3 +0.308
T3B_TOL             = float(os.environ.get("T3B_TOL", 0.003))      # touch / break tolerance
T3B_HOLD_TOL        = float(os.environ.get("T3B_HOLD_TOL", 0.0015))# retest must close within this of the level
T3B_VOL_MULT        = float(os.environ.get("T3B_VOL_MULT", 2.0))   # break-bar volume vs 96-bar median
T3B_RETEST_VOL_MULT = float(os.environ.get("T3B_RETEST_VOL_MULT", 1.0))  # retest vol >= this x break vol
T3B_EXT_MIN         = float(os.environ.get("T3B_EXT_MIN", 0.01))   # must push >=1% past the level
T3B_EXT_MAX         = float(os.environ.get("T3B_EXT_MAX", 0.12))   # but <=12% (further = stop too far away)
T3B_RETEST_MAX      = int(os.environ.get("T3B_RETEST_MAX", 8))     # retest within 8 bars (2h) of the break
T3B_ATRP_MIN        = float(os.environ.get("T3B_ATRP_MIN", 0.004)) # ATR14/price floor - cost in R
T3B_MAX_LEVEL_AGE   = int(os.environ.get("T3B_MAX_LEVEL_AGE", 45)) # HTF bars a level stays live
T3B_SL_ATR          = float(os.environ.get("T3B_SL_ATR", 2.5))
T3B_TP_R            = float(os.environ.get("T3B_TP_R", 2.0))       # RR 2.0. Swept: 1.5 -> win 55% but PnL -17%; 2.5/3.0 both worse.
T3B_LEVEL_REFRESH_S = int(os.environ.get("T3B_LEVEL_REFRESH_S", 21600))  # rebuild HTF levels every 6h
T3B_LEVEL_1H_LIMIT  = int(os.environ.get("T3B_LEVEL_1H_LIMIT", 1000))    # 1h bars per symbol (~41 days)

T3_ATR_LEN          = int(os.environ.get("T3_ATR_LEN", 14))
T3_FILL_BARS        = int(os.environ.get("T3_FILL_BARS", 4))          # cancel resting limit after 4 bars (1h)
T3_HOLD_SECONDS     = int(os.environ.get("T3_HOLD_SECONDS", 288 * 15 * 60))  # 72h max hold (matches backtest)
T3_COOLDOWN_SECONDS = int(os.environ.get("T3_COOLDOWN_SECONDS", 21600))      # 6h per-coin after a signal
T3_LOSS_COOLDOWN_S  = int(os.environ.get("T3_LOSS_COOLDOWN_S", 86400))       # 1d ban after an SL
T3_SL_CAP_PCT       = float(os.environ.get("T3_SL_CAP_PCT", 0.06))    # skip if SL distance > 6% of price
T3_RISK_USDT        = float(os.environ.get("T3_RISK_USDT", 3.0))      # small until live n grows
T3_LEVERAGE         = int(os.environ.get("T3_LEVERAGE", 10))
T3_MAX_CONCURRENT   = int(os.environ.get("T3_MAX_CONCURRENT", 5))     # own slot cap, separate from swing (T1/T2)
T3_MAX_MARGIN_USDT  = float(os.environ.get("T3_MAX_MARGIN_USDT", 40))
T3B_MIN_QUOTE_VOL   = 10_000_000   # 2026-08-27: $2M -> $10M. This is the fix for coin
                                   # concentration, which was the engine's real weakness:
                                   # at $2M the top-3 coins were 41% of PnL and dropping
                                   # them left TEST at +0.035 (fragile); at $10M top-3 is
                                   # 33% and ex-top-3 TEST is +0.141 (solid). $30M breaks
                                   # it again (top-3 85%), so do not raise further.
T3_EXCLUDE_TOP_N    = int(os.environ.get("T3_EXCLUDE_TOP_N", 20))
T3_MAX_SYMBOLS      = int(os.environ.get("T3_MAX_SYMBOLS", 600))
T3_SCAN_SECONDS     = int(os.environ.get("T3_SCAN_SECONDS", 300))
T3_REGIME_MAX_BTC   = -0.03   # 2026-08-28: T3 only scans when BTC 4d return < -3%.
                              # See the long note in t3_scalp_loop for the numbers.
# legacy names kept so /status and old env vars do not break
T3_BTC_GATE         = os.environ.get("T3_BTC_GATE", "0") == "1"
T3_BTC_MAX          = float(os.environ.get("T3_BTC_MAX", 0.03))
T3_BTC_WINDOW       = int(os.environ.get("T3_BTC_WINDOW", 384))
T3_TP_R             = T3B_TP_R

# globals named t3s_* so /status + command handlers are unchanged
t3s_open_trades   = {}    # order_id -> live trade
t3s_pending       = {}    # symbol   -> resting limit awaiting fill
t3s_last_fire     = {}    # symbol   -> ts of last entry
t3s_loss_until    = {}    # symbol   -> ts until which coin is banned (after SL)


def t3s_in_cooldown(symbol):
    now = time.time()
    if now < t3s_loss_until.get(symbol, 0):
        return True
    if now - t3s_last_fire.get(symbol, 0) < T3_COOLDOWN_SECONDS:
        return True
    return False


def t3s_symbol_busy(symbol):
    if symbol in t3s_pending:
        return True
    if any(t.get("symbol") == symbol for t in t3s_open_trades.values()):
        return True
    try:
        if symbol in all_open_symbols():
            return True
    except Exception:
        pass
    return False


def t3_btc_regime():
    """BTC 4-day return. None if unreadable (then we skip, never guess)."""
    try:
        candles = get_candles("BTC-USDT", limit=T3_BTC_WINDOW + 5, interval="15m")
        if not candles or len(candles) < T3_BTC_WINDOW + 1:
            return None
        closes = [cl(c) for c in candles]
        past, now_px = closes[-(T3_BTC_WINDOW + 1)], closes[-1]
        if past <= 0:
            return None
        return now_px / past - 1.0
    except Exception as e:
        print(f"[T3 BTC] {e}")
        return None


t3b_levels     = {}    # symbol -> list of level dicts, rebuilt every T3B_LEVEL_REFRESH_S
t3b_levels_ts  = {}    # symbol -> unix ts of last rebuild


def _t3b_resample(candles, group):
    """Fold 1h candles into HTF bars of `group` hours. Returns (highs, lows)."""
    n = (len(candles) // group) * group
    highs, lows = [], []
    for i in range(0, n, group):
        chunk = candles[i:i + group]
        highs.append(max(h(c) for c in chunk))
        lows.append(min(l(c) for c in chunk))
    return highs, lows


def t3b_build_levels(symbol):
    """Fractal pivot levels on 12H / 1D / 2D bars, built from 1h klines.
    A pivot at index p is only KNOWABLE at p+K, so `ready_ms` marks the first
    millisecond the level may be used - this is what keeps it lookahead-free."""
    candles = get_candles(symbol, limit=T3B_LEVEL_1H_LIMIT, interval="1h")
    if not candles or len(candles) < 120:
        return []
    try:
        t0 = int(candles[0].get("time") or candles[0].get("T") or 0)
        t1 = int(candles[1].get("time") or candles[1].get("T") or 0)
        bar_ms = abs(t1 - t0) or 3600000
    except Exception:
        bar_ms = 3600000
    base_ms = int(candles[0].get("time") or candles[0].get("T") or 0)
    K = T3B_PIVOT_K
    out = []
    for tf in T3B_TIMEFRAMES:
        group = {"12H": 12, "1D": 24, "2D": 48}.get(tf.strip().upper())
        if not group:
            continue
        highs, lows = _t3b_resample(candles, group)
        nb = len(highs)
        if nb < 2 * K + 4:
            continue
        for p in range(K, nb - K):
            wh = highs[p - K:p + K + 1]
            wl = lows[p - K:p + K + 1]
            if highs[p] == max(wh) and wh.index(max(wh)) == K:
                out.append({"tf": tf, "level": highs[p], "is_res": True,
                            "ready_ms": base_ms + (p + K + 1) * group * bar_ms,
                            "dead_ms": base_ms + (p + K + 1 + T3B_MAX_LEVEL_AGE) * group * bar_ms})
            if lows[p] == min(wl) and wl.index(min(wl)) == K:
                out.append({"tf": tf, "level": lows[p], "is_res": False,
                            "ready_ms": base_ms + (p + K + 1) * group * bar_ms,
                            "dead_ms": base_ms + (p + K + 1 + T3B_MAX_LEVEL_AGE) * group * bar_ms})
    return out


def t3b_levels_for(symbol):
    now = time.time()
    if now - t3b_levels_ts.get(symbol, 0) > T3B_LEVEL_REFRESH_S or symbol not in t3b_levels:
        try:
            t3b_levels[symbol] = t3b_build_levels(symbol)
        except Exception as e:
            print(f"[T3 LEVELS {symbol}] {e}")
            t3b_levels.setdefault(symbol, [])
        t3b_levels_ts[symbol] = now
    return t3b_levels.get(symbol, [])


def t3b_check_signal(symbol):
    """Clean-break retest. Returns (side, entry, sl, tp) or None.
    Mirrors the backtest exactly: the retest must be the LAST CLOSED 15m bar, so
    the entry limit sits at a price that has just printed and nothing is peeked at."""
    levels = t3b_levels_for(symbol)
    if not levels:
        return None
    need = 96 + T3_ATR_LEN + T3B_RETEST_MAX + 40
    candles = get_candles(symbol, limit=need, interval="15m")
    if not candles or len(candles) < 96 + T3B_RETEST_MAX + 5:
        return None
    highs  = [h(c) for c in candles]
    lows   = [l(c) for c in candles]
    closes = [cl(c) for c in candles]
    vols   = [v(c) for c in candles]
    try:
        times = [int(c.get("time") or c.get("T") or 0) for c in candles]
    except Exception:
        times = [0] * len(candles)
    n = len(candles)
    t = n - 1                      # the retest must be THIS bar (last closed)
    atr_vals = atr_series(highs, lows, closes, T3_ATR_LEN)
    atr = atr_vals[-1] if atr_vals else 0
    if not atr or atr <= 0:
        return None
    px = closes[t]
    if px <= 0 or atr / px < T3B_ATRP_MIN:
        return None
    if (T3B_SL_ATR * atr) / px > T3_SL_CAP_PCT:
        return None
    med_vol = sorted(vols[-96:])[48] if len(vols) >= 96 else 0
    if med_vol <= 0:
        return None

    best = None
    for lv in levels:
        level = lv["level"]
        is_res = lv["is_res"]
        if level <= 0:
            continue
        # the retest bar must touch the level and CLOSE holding it
        if is_res:
            touched = lows[t] <= level * (1 + T3B_TOL)
            held    = closes[t] >= level * (1 - T3B_HOLD_TOL)
        else:
            touched = highs[t] >= level * (1 - T3B_TOL)
            held    = closes[t] <= level * (1 + T3B_HOLD_TOL)
        if not (touched and held):
            continue

        # walk back to find the break bar inside the retest window
        brk = None
        for j in range(t - 1, max(t - 1 - T3B_RETEST_MAX, 0) - 1, -1):
            if times[j] and times[j] < lv["ready_ms"]:
                break
            if times[j] and lv["dead_ms"] and times[j] > lv["dead_ms"]:
                continue
            broke = (closes[j] > level * (1 + T3B_TOL)) if is_res \
                    else (closes[j] < level * (1 - T3B_TOL))
            if broke:
                brk = j
            elif brk is not None:
                break            # found the start of the break run
        if brk is None:
            continue
        # earliest bar of that break run
        while brk - 1 >= 0 and brk - 1 > t - 1 - T3B_RETEST_MAX:
            prev_broke = (closes[brk - 1] > level * (1 + T3B_TOL)) if is_res \
                         else (closes[brk - 1] < level * (1 - T3B_TOL))
            if prev_broke:
                brk -= 1
            else:
                break
        if times[brk] and times[brk] < lv["ready_ms"]:
            continue
        # the level must have been CLEAN: touched at most once before the break
        touches = 0
        start = max(0, brk - 96)
        for j in range(start, brk):
            if times[j] and times[j] < lv["ready_ms"]:
                continue
            ref = highs[j] if is_res else lows[j]
            if abs(ref - level) / level < T3B_TOL:
                touches += 1
        if touches > 1:
            continue
        # break-bar volume
        if vols[brk] < T3B_VOL_MULT * med_vol:
            continue
        # retest volume >= break volume
        if vols[t] < T3B_RETEST_VOL_MULT * vols[brk]:
            continue
        # extension past the level between the break and the retest
        ext = 0.0
        for j in range(brk, t + 1):
            e = (highs[j] - level) / level if is_res else (level - lows[j]) / level
            if e > ext:
                ext = e
        if ext < T3B_EXT_MIN or ext > T3B_EXT_MAX:
            continue

        side = "LONG" if is_res else "SHORT"
        # ORDER-FLOW GATE (continuation): the break must be backed by aggressors on
        # the break side. Flow opposing a break is a measured loser (-0.160), so this
        # one SKIPS rather than down-sizes.
        ok, _flow, _cov = flow_allows(symbol, side, False, label=" TIGHT 3")
        if not ok:
            continue
        entry = px
        dist = T3B_SL_ATR * atr
        sl = entry - dist if is_res else entry + dist
        tp = entry + T3B_TP_R * dist if is_res else entry - T3B_TP_R * dist
        cand = (side, entry, sl, tp, ext, lv["tf"])
        # prefer the highest timeframe if several levels qualify at once
        rank = {"2D": 3, "1D": 2, "12H": 1}.get(lv["tf"], 0)
        if best is None or rank > best[0]:
            best = (rank, cand)
    if best is None:
        return None
    side, entry, sl, tp, ext, tf = best[1]
    print(f"[T3 SIGNAL] {symbol} {side} tf={tf} ext={ext*100:.1f}% entry={entry}")
    return side, entry, sl, tp


def place_t3_limit(symbol, side, entry, sl, tp):
    """RESTING LIMIT (maker) at the retest close, LONG or SHORT."""
    try:
        set_leverage_api(symbol, T3_LEVERAGE)
        precision = symbol_precision.get(symbol, 4)
        risk_dist = abs(sl - entry)
        if risk_dist <= 0:
            return None
        risk_qty       = T3_RISK_USDT / risk_dist
        margin_cap_qty = (T3_MAX_MARGIN_USDT * T3_LEVERAGE) / entry
        qty = round(min(risk_qty, margin_cap_qty), precision)
        if qty <= 0:
            return None
        if side == "LONG":
            order_side, pos_side, close_side = "BUY", "LONG", "SELL"
        else:
            order_side, pos_side, close_side = "SELL", "SHORT", "BUY"
        if not depth_ok(symbol, entry, sl, qty, order_side):
            print(f"[T3 DEPTH SKIP] {symbol}")
            return "DEPTH_SKIP"
        required_margin = qty * entry / T3_LEVERAGE
        avail = get_available_margin()
        if avail is not None and avail < required_margin * 1.05:
            print(f"[T3 MARGIN SKIP] {symbol} need ~${required_margin:.2f}, have ${avail:.2f}")
            return "MARGIN_SKIP"
        if not rev_margin_allows(required_margin, get_balance()):
            print(f"[T3 BUDGET SKIP] {symbol} need ~${required_margin:.2f}")
            return "MARGIN_SKIP"
        url = BASE_URL + "/openApi/swap/v2/trade/order"
        params = build_signed_params({
            "symbol": symbol, "side": order_side, "positionSide": pos_side,
            "type": "LIMIT", "price": round(entry, 6), "quantity": qty,
            "timeInForce": "GTC",
        })
        r = requests.post(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        oid = r.get("data", {}).get("order", {}).get("orderId", "N/A")
        if oid == "N/A":
            print(f"[T3 LIMIT FAIL] {symbol} qty={qty} px={entry} - BingX: {r}")
            return None
        t3s_pending[symbol] = {
            "order_id": oid, "symbol": symbol, "side": order_side, "pos_side": pos_side,
            "close_side": close_side, "dir": side, "entry": entry, "sl": sl, "tp": tp,
            "qty": qty, "placed_ts": time.time(),
        }
        send_tg(
            f"\u23f3 TIGHT 3 {symbol} {side} LIMIT RESTING (clean-break retest)\n"
            f"Entry: {round(entry, 6)} | SL: {round(sl, 6)} | TP: {round(tp, 6)}\n"
            f"Risk: ${T3_RISK_USDT} | cancels in {T3_FILL_BARS * 15}m"
        )
        return oid
    except Exception as e:
        print(f"[T3 ORDER {symbol}] {e}")
        return None


def track_t3_pending():
    """Promote resting T3 limits to live trades on fill (place SL+TP), cancel if unfilled."""
    if not t3s_pending:
        return
    now = time.time()
    for sym, p in list(t3s_pending.items()):
        try:
            status = check_order_status(p["order_id"], sym)
        except Exception as e:
            print(f"[T3 PEND {sym}] {e}")
            continue
        d = p.get("dir", "SHORT")
        if status == "FILLED":
            fill = get_fill_price(p["order_id"], sym, fallback=p["entry"])
            risk = (fill - p["sl"]) if d == "LONG" else (p["sl"] - fill)
            if risk <= 0:
                try:
                    place_market_order(sym, p["close_side"], p["qty"], p["pos_side"])
                except Exception as _e:
                    print(f"[T3 bad-fill close {sym}] {_e}")
                send_tg(f"\u26a0\ufe0f TIGHT 3 {sym} {d} bad fill (beyond SL) - closed, no naked risk.")
                t3s_last_fire[sym] = now
                t3s_pending.pop(sym, None)
                continue
            tp = fill + risk * T3B_TP_R if d == "LONG" else fill - risk * T3B_TP_R
            sl_id = place_sl_guarded(sym, p["close_side"], p["pos_side"], p["sl"], p["qty"])
            if sl_id is None:
                try:
                    place_market_order(sym, p["close_side"], p["qty"], p["pos_side"])
                except Exception as _e:
                    print(f"[T3 SL-FAIL emergency close {sym}] {_e}")
                send_tg(f"\u26a0\ufe0f TIGHT 3 {sym} {d} SL placement FAILED - position emergency-closed.")
                t3s_last_fire[sym] = now
                t3s_pending.pop(sym, None)
                continue
            tp_id = place_tp_guarded(sym, p["close_side"], p["pos_side"], tp, p["qty"], label="TIGHT 3")
            t3s_open_trades[p["order_id"]] = {
                "symbol": sym, "side": p["side"], "pos_side": p["pos_side"],
                "close_side": p["close_side"], "dir": d,
                "entry": p["entry"], "entry_fill": fill,
                "sl": p["sl"], "tp": tp, "total_qty": p["qty"], "risk_usdt": T3_RISK_USDT,
                "sl_id": sl_id, "tp_id": tp_id, "open_ts": now, "gone_strikes": 0,
                "label": "TIGHT 3",
            }
            t3s_last_fire[sym] = now
            t3s_pending.pop(sym, None)
            slip = abs(fill - p["entry"]) / p["entry"] * 100 if p["entry"] else 0
            _fv = p.get("flow"); _fc = p.get("flow_cover", 0)
            print(f"[FLOW LOG] {sym} {p.get('dir','?')} flow="
                  + ("n/a" if _fv is None else f"{_fv:+.3f}")
                  + f" covered={int(_fc or 0)}s")
            send_tg(
                f"\u2705 TIGHT 3 {sym} {d} FILLED (trade executed)\n"
                f"Entry: {fill} (limit {round(p['entry'], 6)}, slip {slip:.2f}%)\n"
                f"SL: {round(p['sl'], 6)} | TP: {round(tp, 6)} | Risk: ${T3_RISK_USDT}"
            )
            continue
        if now - p["placed_ts"] > T3_FILL_BARS * 15 * 60:
            try:
                cancel_order(sym, p["order_id"])
            except Exception as e:
                print(f"[T3 CANCEL {sym}] {e}")
            t3s_pending.pop(sym, None)
            send_tg(f"\u274e TIGHT 3 {sym} {d} LIMIT CANCELLED (unfilled {T3_FILL_BARS * 15}m) - margin released")


def track_t3_scalp_trades():
    """Time-stop + reconcile closed T3 positions, then journal. Two-strike exchange
    confirmation before declaring a position closed (no phantom closes).
    (Name kept as track_t3_scalp_trades so the loop wiring is unchanged.)"""
    if not t3s_open_trades:
        return
    now = time.time()
    for oid, t in list(t3s_open_trades.items()):
        sym = t["symbol"]
        d = t.get("dir", "SHORT")
        if now - t.get("open_ts", now) > T3_HOLD_SECONDS:
            try:
                place_market_order(sym, t["close_side"], t["total_qty"], t["pos_side"])
                for k in ("sl_id", "tp_id"):
                    if t.get(k) and t[k] != "N/A":
                        try:
                            cancel_order(sym, t[k])
                        except Exception:
                            pass
                ef = t.get("entry_fill", t.get("entry", 0))
                px = get_current_price(sym)
                pnl = None; exit_r = 0
                if px and ef:
                    rd = abs(ef - t["sl"])
                    if rd > 0:
                        move = (px - ef) if d == "LONG" else (ef - px)
                        pnl = move / rd * T3_RISK_USDT
                        exit_r = round(move / rd, 2)
                send_tg(f"\u23f1\ufe0f TIGHT 3 {sym} {d} time-stop closed | entry {ef}"
                        + (f" | {'+' if pnl >= 0 else '-'}${abs(round(pnl, 2))}" if pnl is not None else ""))
                journal_closed_trade({
                    "label": "TIGHT 3", "symbol": sym, "side": d, "entry": ef,
                    "result": "time-stop", "pnl": round(pnl, 2) if pnl is not None else 0,
                    "exit_r": exit_r,
                })
            except Exception as e:
                print(f"[T3 TIMESTOP {sym}] {e}")
            t3s_last_fire[sym] = now
            t3s_open_trades.pop(oid, None)
            continue
        gone = exchange_has_position(sym)
        if gone is None or gone:
            t["gone_strikes"] = 0
            continue
        t["gone_strikes"] = t.get("gone_strikes", 0) + 1
        if t["gone_strikes"] < 2:
            print(f"[T3] {sym} looks gone (strike 1/2) - waiting for confirmation")
            continue
        ef = t.get("entry_fill", t.get("entry", 0))
        t.setdefault("symbol", sym)
        t.setdefault("risk_usdt", T3_RISK_USDT)
        result, pnl, exit_r, exit_px = resolve_exit(t, "BUY" if d == "LONG" else "SELL", ef)
        if result == "SL":
            t3s_loss_until[sym] = now + T3_LOSS_COOLDOWN_S
        for k in ("sl_id", "tp_id"):
            if t.get(k) and t[k] != "N/A":
                try:
                    cancel_order(sym, t[k])
                except Exception:
                    pass
        emoji = "\u2705" if result == "TP" else ("\u274c" if result == "SL" else "\u2139\ufe0f")
        held_min = int((now - t.get("open_ts", now)) / 60)
        send_tg(f"{emoji} TIGHT 3 {sym} {d} {result} | entry {ef} | held {held_min}m")
        journal_closed_trade({
            "label": "TIGHT 3", "symbol": sym, "side": d, "entry": ef,
            "result": result, "pnl": round(pnl, 2), "exit_r": exit_r,
            "exit_px": round(exit_px, 8) if exit_px else 0,
        })
        t3s_last_fire[sym] = now
        t3s_open_trades.pop(oid, None)


def t3_scalp_loop():
    print("Tight 3 loop started - HTF CLEAN-BREAK RETEST (12H/1D/2D levels, both sides, resting-limit entry, /t3_start /t3_stop)")
    if not T3_ENGINE_ENABLED:
        print("[T3] disabled by env T3_ENGINE_ENABLED=0 - loop idle")
        while True:
            time.sleep(3600)
    # 2026-08-24: the old code scanned the raw BACKTESTED_COINS list whenever WHITELIST_ONLY
    # was on, which BYPASSED get_liquid_symbols entirely - so T3 had no liquidity floor at all
    # (that is why it scanned 249 coins) and it kept requesting de-listed symbols such as
    # VANRY-USDT, spamming [CANDLES ERROR] every cycle. Now the universe always comes from
    # get_liquid_symbols(), which applies the whitelist gate itself, enforces T3_MIN_QUOTE_VOL,
    # drops the top-N by volume, and can only return symbols the exchange currently lists.
    while True:
        try:
            track_t3_pending()
            track_t3_scalp_trades()
            if not t3_scalp_auto_enabled:
                time.sleep(30)
                continue
            if api_backoff_active():
                time.sleep(60)
                continue
            open_slots = T3_MAX_CONCURRENT - (len(t3s_open_trades) + len(t3s_pending))
            if open_slots <= 0:
                time.sleep(T3_SCAN_SECONDS)
                continue
            # 2026-08-28 BEAR-ONLY REGIME GATE. T3 is a continuation/breakout engine and
            # it only pays in falling markets. Measured 12mo, 20bps, flat $5:
            #   BEAR (BTC4d < -3%)  n=515  meanR +0.193  win 42.5%  +$497
            #   BULL (BTC4d > +3%)  n=437  meanR -0.108           -$236
            #   FLAT                n=1108 meanR -0.050           -$278
            # Ungated over the full year that nets out to about zero (-$17), so the gate
            # is what makes T3 pay - it is NOT optional. The cutoff was chosen on the
            # TRAIN half and held on TEST (+0.170/+0.229), so it is not fitted to one
            # period. Bull/flat is T4's shift; the two engines cover opposite regimes.
            # If BTC can't be read we SKIP rather than guess (same rule as the T1/T2 gate).
            _t3_btc = rev_btc_regime()
            if _t3_btc is None or _t3_btc >= T3_REGIME_MAX_BTC:
                _b = f"{_t3_btc * 100:+.1f}%" if _t3_btc is not None else "n/a"
                print(f"[T3 SCAN] skipped - not bear regime (btc4d={_b}, "
                      f"need < {T3_REGIME_MAX_BTC * 100:.0f}%)")
                time.sleep(T3_SCAN_SECONDS)
                continue
            universe = get_liquid_symbols(
                get_futures_symbols() or [], min_quote_vol=T3B_MIN_QUOTE_VOL,
                max_n=T3_MAX_SYMBOLS, exclude_top_n=T3_EXCLUDE_TOP_N,
            )
            scanned = fired = 0
            for sym in universe:
                if open_slots <= 0:
                    break
                if not t3_scalp_auto_enabled or api_backoff_active():
                    break
                if t3s_in_cooldown(sym) or t3s_symbol_busy(sym):
                    continue
                if is_tokenized(sym) or coin_too_young(sym):
                    continue
                scanned += 1
                try:
                    sig = t3b_check_signal(sym)
                except Exception as e:
                    print(f"[T3 SIG {sym}] {e}")
                    continue
                if not sig:
                    continue
                side, entry, sl, tp = sig
                res = place_t3_limit(sym, side, entry, sl, tp)
                if res and res not in ("DEPTH_SKIP", "MARGIN_SKIP", "N/A"):
                    fired += 1
                    open_slots -= 1
                time.sleep(0.2)
            print(f"[T3 SCAN] open={len(t3s_open_trades)} pending={len(t3s_pending)}/{T3_MAX_CONCURRENT} "
                  f"on={t3_scalp_auto_enabled} scanned={scanned} fired={fired}")
        except Exception as e:
            print(f"[T3 LOOP ERROR] {e}")
        time.sleep(T3_SCAN_SECONDS)



@app.route("/")
def health():
    return ("Niti combined - Tight 1 (Dormant Awakening) + Tight 2 (Trapped-Block Fade, both sides) "
            "+ Confluence dedicated slot + OI collector + consolidated journal"), 200




# ============================================================================
# ==================== TIGHT 1 = 24h-REVERSION (2026-08-20) ==================
# ============================================================================
# REPLACES the old Tight 1 (LONG demand-block). That engine measured -0.211R once
# the block-timing lookahead was removed, i.e. worse than random, and is retired.
#
# HOW THIS ONE WAS FOUND (method matters): a Spearman scan of 13 candle features
# against forward returns over 4.1M rows / 174 coins. Every return-based feature
# came back NEGATIVE - i.e. the only real signal in candle data is LONG-HORIZON
# MEAN REVERSION. Strongest: 24h return (-0.057) and position-in-96-bar-range
# (-0.051) vs the 48-bar forward return. The rules below were then built on that
# measured fact instead of being invented first.
#
# RULES
#   SHORT: alt UP >= REV_RET_THR over the last 24h (96 x 15m bars) AND printing at
#          the very top of its 96-bar range AND this bar's volume >= 1.3x the
#          96-bar median volume.
#   LONG : mirror - DOWN >= REV_RET_THR over 24h AND at the very bottom of the
#          96-bar range AND the same volume surge.
#   BTC REGIME GATE: if BTC is down > 12% over 4 days take NO LONGS; if BTC is up
#          > 12% over 4 days take NO SHORTS. This single gate turned the worst
#          month (June, BTC -20.6%) from -4.3R into +14.0R and halved max drawdown.
#   ENTRY: RESTING LIMIT at the signal bar's CLOSE, cancelled after 4 bars (1h).
#          NOT market. Measured: market entry = $765 / OOS +0.226; resting limit =
#          98% fill, $1041 / OOS +0.324. Placing the limit at a *better* price is
#          worse (fill 67% and OOS +0.055 at +0.5%) because if price comes back to
#          you the reversal was weak. A "watch and send post-only on touch" variant
#          fills only 72% and earns a third as much - the best trades snap away.
#   STOPS: LONG SL 1.5xATR14 / TP 4R.  SHORT SL 2.0xATR14 / TP 3R.  (asymmetric on
#          purpose - LONG fires after an >=8% dump when volatility is elevated).
#   Time-stop 24h, per-coin cooldown 24h, own shared cap REV_MAX_CONCURRENT.
#
# BACKTEST (8.3mo, 249-coin 15m, maker fee): n451 (~12.5/wk), meanR +0.462,
#   win 40%, +$1041 at flat $5, maxDD ~$76, 9/9 months positive.
#   OUT-OF-SAMPLE (May-Jul, never used for tuning): meanR +0.324.
#   Walk-forward positive at SIX different cut dates; 7 of 8 rolling 60d windows
#   positive; random-side control negative on all seeds.
#
# THE ONE REAL RISK - LIQUIDITY. The whole edge lives in THIN coins: raising the
#   liquidity floor destroys it ($2M -> +0.226 OOS, $5M -> -0.036, $10M -> -0.274).
#   Slippage sensitivity: 5/20bps -> +0.105, 10/40bps -> -0.016. Historical live
#   slippage of ~100bps (ONG, VELVET) would wipe this out completely. That is why
#   the resting-limit entry and the depth guard are NOT optional.
# ============================================================================
REV_ENGINE_ENABLED   = os.environ.get("REV_ENGINE_ENABLED", "1") == "1"
rev_auto_enabled     = AUTO_RESUME_ON_START   # /rev_start /rev_stop

REV_RET_WINDOW       = int(os.environ.get("REV_RET_WINDOW", 96))      # 96 x 15m = 24h
REV_RET_THR          = float(os.environ.get("REV_RET_THR", 0.06))     # >= 6% move over that window
REV_RANGE_WINDOW     = int(os.environ.get("REV_RANGE_WINDOW", 96))    # range the coin must be at the edge of
REV_VOL_MULT         = 2.0   # 2026-08-27: 1.3 -> 2.0. With the flow gate on, the
                             # full 420-config grid put 2.0x at meanR +0.263 vs +0.055
                             # for 1.3x, and ex-top-3-coin TEST +0.316 vs +0.057.
                             # Costs trades (39.5 -> 5.7/wk) and that is accepted.
                             # IF LIVE TRADES ARE TOO FEW, this is the first lever
                             # to move back to 1.3 (8.3/wk, +$709, still robust).     # volume >= 1.3x 96-bar median
REV_ATR_LEN          = int(os.environ.get("REV_ATR_LEN", 14))
REV_LONG_SL_ATR      = float(os.environ.get("REV_LONG_SL_ATR", 2.0))
REV_LONG_TP_R        = float(os.environ.get("REV_LONG_TP_R", 2.5))
REV_SHORT_SL_ATR     = float(os.environ.get("REV_SHORT_SL_ATR", 2.0))
REV_SHORT_TP_R       = float(os.environ.get("REV_SHORT_TP_R", 2.5))
REV_SL_CAP_PCT       = float(os.environ.get("REV_SL_CAP_PCT", 0.06))  # skip if SL > 6% away
REV_BTC_WINDOW       = int(os.environ.get("REV_BTC_WINDOW", 384))     # 384 x 15m = 4 days
REV_BTC_THR          = float(os.environ.get("REV_BTC_THR", 0.12))     # regime gate at +/-12%
REV_HOLD_SECONDS     = int(os.environ.get("REV_HOLD_SECONDS", 12 * 3600))  # 2026-08-30: 48h -> 12h (slot turnover; sweep 6/12/24/48h)
_REV_HOLD_OLD_NOTE   = 48 * 3600   # 2026-08-27: 24h -> 48h. Same grid; the winner
                                   # at every robust setting held 48h. Shared by T1/T2.
REV_COOLDOWN_SECONDS = int(os.environ.get("REV_COOLDOWN_SECONDS", 6 * 3600))
REV_FILL_BARS        = int(os.environ.get("REV_FILL_BARS", 4))        # cancel the resting limit after 4 bars (1h)
REV_RISK_USDT        = float(os.environ.get("REV_RISK_USDT", 1.5))   # 2026-08-30: sized for a $150 account at 10x (margin-bound, not drawdown-bound)
REV_LEVERAGE         = int(os.environ.get("REV_LEVERAGE", 10))
REV_MAX_CONCURRENT   = int(os.environ.get("REV_MAX_CONCURRENT", 30))   # 2026-08-30: 20 -> 30 (cap sweep with both filters on)
REV_MAX_MARGIN_USDT  = float(os.environ.get("REV_MAX_MARGIN_USDT", 40))

# ---- 2026-08-25 SHARED MARGIN BUDGET -------------------------------------------------
# Bug: T1 (cap 20) + T2 (cap 20) + T3 each hold their own $40 cap, so total demand was
# $1,000+ on a far smaller account. Worse, it was ASYMMETRIC: qty = risk/risk_dist, and a
# LONG's SL was tighter than a SHORT's, so a long needed ~33% more margin for the same $5
# risk and MARGIN_SKIP culled longs first. That is why Faisal saw only shorts opening.
# Now every engine draws from ONE budget, expressed as a fraction of live balance.
REV_MARGIN_BUDGET_PCT = float(os.environ.get("REV_MARGIN_BUDGET_PCT", 0.60))
REV_MARGIN_MIN_FREE   = float(os.environ.get("REV_MARGIN_MIN_FREE", 5.0))


def rev_margin_in_use():
    """Margin currently committed across every engine's open + pending trades."""
    total = 0.0
    for book in (rev_open_trades, rev_pending, rev2_open_trades, rev2_pending,
                 rev4_open_trades, rev4_pending):
        for t in list(book.values()):
            try:
                total += float(t.get("margin_used", 0) or 0)
            except Exception:
                pass
    try:
        for t in list(t3s_open_trades.values()):
            total += float(t.get("margin_used", 0) or 0)
    except Exception:
        pass
    return total


def rev_margin_allows(required, balance):
    """True if `required` USDT of margin fits inside the SHARED budget."""
    if balance is None or balance <= 0:
        return True
    budget = balance * REV_MARGIN_BUDGET_PCT
    return (rev_margin_in_use() + required) <= budget and (balance - required) >= REV_MARGIN_MIN_FREE
# --------------------------------------------------------------------------------------
REV_MIN_QUOTE_VOL    = float(os.environ.get("REV_MIN_QUOTE_VOL", 2_000_000))  # DO NOT RAISE - see header
REV_EXCLUDE_TOP_N    = int(os.environ.get("REV_EXCLUDE_TOP_N", 20))
REV_MAX_SYMBOLS      = int(os.environ.get("REV_MAX_SYMBOLS", 600))
REV_SCAN_SECONDS     = int(os.environ.get("REV_SCAN_SECONDS", 300))

rev_open_trades   = {}   # order_id -> live trade
rev_pending       = {}   # symbol -> resting limit order awaiting fill
rev_last_fire     = {}   # symbol -> ts of last entry

# ---- 2026-08-20 ceiling filters (used by BOTH engines; T1 leaves them wide, T2 tightens) ----
# vol band: T1 = >=1.3x (no upper). T2 = 1.3x..3.0x (vr>4 = FOMO continuation, measured NEGATIVE).
# ATR% ceiling: T1 = none. T2 = skip coins whose ATR14/price > 3% (high-vol coins were OOS-negative).

# Tight 1 engine descriptor (the original v8 24h-reversion, unchanged params).
REV_T1 = {
    "name": "TIGHT 1", "tag": "t1",
    "ret_thr": REV_RET_THR, "vol_mult": REV_VOL_MULT, "vol_mult_max": 0.0,   # 0 = no upper band
    "atrp_max": 0.0,                                                          # 0 = no ATR% ceiling
    "long_sl_atr": REV_LONG_SL_ATR, "long_tp_r": REV_LONG_TP_R,
    "short_sl_atr": REV_SHORT_SL_ATR, "short_tp_r": REV_SHORT_TP_R,
    "sl_cap_pct": REV_SL_CAP_PCT, "risk_usdt": REV_RISK_USDT, "leverage": REV_LEVERAGE,
    "max_concurrent": REV_MAX_CONCURRENT, "max_margin": REV_MAX_MARGIN_USDT,
    "cvd_filter": True, "range_regime": "CALM",   # 2026-08-30: reversion -> calm + CVD
    "open": rev_open_trades, "pending": rev_pending, "last_fire": rev_last_fire,
}

# ---- Tight 2 = SCALPER 24h-reversion (2026-08-25 rebuild). SAME engine as T1, but a
#      1:1 RR that exits fast. ret>=8%, NO vol ceiling, NO ATR% ceiling, SL 2.0xATR,
#      TP 1.0R, cap 20, cd 6h. Strict-sim backtest: 8mo 1.9 trades/day, win 60%, +$282;
#      Aug OOS 1.7/day, win 58%, +$22.
#      WHY THE CEILING FILTERS WENT: vol_max 3.0x cost money (+$304 -> +$187) and the
#      ATR% filter changed nothing at all (identical results at 0.00 and 0.03).
#      WHY 1:1 HERE AND 1:2.5 ON T1: T2 is the BULL-PROOF leg. Monthly PnL vs BTC shows
#      T2 positive in 9 of 10 months with win rate never leaving 54-67% regardless of
#      direction (Aug 2026, BTC +25.4%: T1 -$2, T2 +$27). T1's edge is its SHORT leg and
#      that is bear-dependent; a near TP exits before the trend can invert.
#      NOTE: with T1 at ret 6%, T2's signals are ~99% a subset of T1's. Accepted -
#      splitting them (ret band / liquidity / side) was measured and all three are worse. ----
REV2_ENGINE_ENABLED   = os.environ.get("REV2_ENGINE_ENABLED", "1") == "1"
rev2_auto_enabled     = AUTO_RESUME_ON_START   # /t2_start /t2_stop
# 2026-08-28: ret 0.08 -> 0.07 and vol 1.3 -> 1.1, from a fresh independent grid.
# Same-sim before/after (12mo, 20bps, flat $5): tr/wk 5.6 -> 7.8, win 47.3% -> 47.4%,
# PnL $193 -> $273. Everything up or flat, no trade-off. TRAIN +0.180 / TEST +0.062,
# flip control -0.210, survives 50bps, top-3 33%.
REV2_RET_THR          = float(os.environ.get("REV2_RET_THR", 0.07))
REV2_VOL_MULT         = float(os.environ.get("REV2_VOL_MULT", 1.1))
REV2_VOL_MULT_MAX     = float(os.environ.get("REV2_VOL_MULT_MAX", 0.0))
REV2_ATRP_MAX         = float(os.environ.get("REV2_ATRP_MAX", 0.0))
REV2_MAX_CONCURRENT   = int(os.environ.get("REV2_MAX_CONCURRENT", 30))  # 2026-08-30: 20 -> 30
REV2_RISK_USDT        = float(os.environ.get("REV2_RISK_USDT", 1.5))
REV2_MAX_MARGIN_USDT  = float(os.environ.get("REV2_MAX_MARGIN_USDT", 40))
REV2_SCAN_SECONDS     = int(os.environ.get("REV2_SCAN_SECONDS", 300))

rev2_open_trades = {}
rev2_pending     = {}
rev2_last_fire   = {}

REV2_LONG_SL_ATR      = float(os.environ.get("REV2_LONG_SL_ATR", 2.0))
REV2_SHORT_SL_ATR     = float(os.environ.get("REV2_SHORT_SL_ATR", 2.0))
REV2_LONG_TP_R        = 1.5   # 2026-08-27: 1.0 -> 1.5. Not for the headline PnL but
                              # for concentration: at TP 1.0R the top-3 coins were 84%
                              # of all PnL (i.e. everything else lost); at 1.5R that
                              # falls to 23%. meanR +0.024 -> +0.169, PnL +$142 -> +$438.
REV2_SHORT_TP_R       = 1.5   # see REV2_LONG_TP_R
REV2_COOLDOWN_SECONDS = int(os.environ.get("REV2_COOLDOWN_SECONDS", 6 * 3600))

REV_T2 = {
    "name": "TIGHT 2", "tag": "t2",
    "ret_thr": REV2_RET_THR, "vol_mult": REV2_VOL_MULT, "vol_mult_max": REV2_VOL_MULT_MAX,
    "atrp_max": REV2_ATRP_MAX,
    "long_sl_atr": REV2_LONG_SL_ATR, "long_tp_r": REV2_LONG_TP_R,
    "short_sl_atr": REV2_SHORT_SL_ATR, "short_tp_r": REV2_SHORT_TP_R,
    "sl_cap_pct": REV_SL_CAP_PCT, "risk_usdt": REV2_RISK_USDT, "leverage": REV_LEVERAGE,
    "max_concurrent": REV2_MAX_CONCURRENT, "max_margin": REV2_MAX_MARGIN_USDT,
    "cooldown_s": REV2_COOLDOWN_SECONDS,
    "cvd_filter": True, "range_regime": None,     # 2026-08-30: CVD yes, range filter measured WORSE for T2
    "open": rev2_open_trades, "pending": rev2_pending, "last_fire": rev2_last_fire,
}


# ---- Tight 4 = 4h RANGE-EXTREME REVERSION SHORT (2026-08-28). Same code path as
#      T1/T2, but the range/return window is 4h (16 bars) instead of 24h (96), which is
#      the whole point: `pos >= 0.999` on a 24h window is what starves T1 of trades
#      (it cuts ~1.3M candidate bars down to ~3.6k). Shrinking the window to 4h gives
#      ~20 trades/wk instead of ~6.
#      WHY SHORT-ONLY: the long leg was negative in every regime; skipping beats sizing.
#      WHY THE BEAR GATE: this engine earns in bull/flat (BULL +$421, FLAT +$121) and
#      is ~flat in bear (+$41). T3's clean-break engine covers bear instead - the two
#      are near-perfect regime complements and that pairing is the reason both are on.
#      Measured 12mo Aug2025-Jul2026, 20bps, flat $5, strict sim, gated:
#      834 trades, meanR +0.130, +$542. Ungated short-only was 20/wk +0.110 +$584,
#      TRAIN +0.140 / TEST +0.073, top-3 only 25%, survives 50bps.
#      DO NOT retune: TP 0.5-2.0R and SL 1.5-3.0xATR x hold 24/48/72h were all swept;
#      TP 2.0R + SL 2.0xATR + 48h is the grid optimum and the only cell that holds at
#      50bps. Loosening ret below 2.5% raises trade count but breaks TEST/slippage. ----
REV4_ENGINE_ENABLED   = os.environ.get("REV4_ENGINE_ENABLED", "1") == "1"
rev4_auto_enabled     = AUTO_RESUME_ON_START   # /t4_start /t4_stop
REV4_RET_THR          = 0.025   # 2.5% over 4h
REV4_VOL_MULT         = 1.3
REV4_RET_WINDOW       = 16      # 16 x 15m = 4h
REV4_RANGE_WINDOW     = 16
REV4_SL_ATR           = 2.0
REV4_TP_R             = 2.0
REV4_REGIME_MIN_BTC   = -0.03   # only run when BTC 4d >= -3% (bull/flat); bear is T3's
# 2026-08-29: T4 ONLY - liquidity floor $2M -> $1M. T4's edge lives in thin coins
# (measured: excluding the top-80 liquid names gives TEST +0.123, keeping only those
# 80 gives TEST -0.022), so lowering the floor sends it further into where the edge
# actually is rather than diluting it. Measured 12mo Aug25-Jul26, 20bps, flat $5,
# same sim both sides: tr/wk 16.2 -> 19.4 (+20%), PnL $566 -> $654 (+16%), meanR
# +0.134 -> +0.129, win 40.3% -> 40.2%, TRAIN +0.191/+0.190, TEST +0.066/+0.068,
# positive months 7/12 -> 8/12, coins 189 -> 195, top-3 share 24% -> 28%.
# RISK, stated plainly: the extra trades are in THINNER books. At 50bps the new floor
# is actually slightly WORSE ($1.86/wk vs $2.12/wk) and at 100bps clearly worse
# (-$15.95 vs -$12.42). The +16% is real only if live slippage stays near 20bps.
# Faisal's own live slippage has hit ~100bps on thin names (ONG, VELVET), so if the
# journal starts showing SL fills far past the trigger, put this back to 2_000_000.
# $0.2M was also measured (20.9/wk, $14.1/wk) and deliberately NOT taken - too thin.
REV4_MIN_QUOTE_VOL    = float(os.environ.get("REV4_MIN_QUOTE_VOL", 1_000_000))
REV4_MAX_CONCURRENT   = int(os.environ.get("REV4_MAX_CONCURRENT", 30))  # 2026-08-30: 20 -> 30
REV4_RISK_USDT        = float(os.environ.get("REV4_RISK_USDT", 1.5))
REV4_MAX_MARGIN_USDT  = float(os.environ.get("REV4_MAX_MARGIN_USDT", 40))
REV4_SCAN_SECONDS     = int(os.environ.get("REV4_SCAN_SECONDS", 300))
REV4_COOLDOWN_SECONDS = int(os.environ.get("REV4_COOLDOWN_SECONDS", 6 * 3600))

rev4_open_trades = {}
rev4_pending     = {}
rev4_last_fire   = {}

REV_T4 = {
    "name": "TIGHT 4", "tag": "t4",
    "ret_thr": REV4_RET_THR, "vol_mult": REV4_VOL_MULT, "vol_mult_max": 0.0,
    "atrp_max": 0.0,
    "ret_window": REV4_RET_WINDOW, "range_window": REV4_RANGE_WINDOW,
    "side_only": "SELL",
    "regime_min_btc": REV4_REGIME_MIN_BTC,
    "min_quote_vol": REV4_MIN_QUOTE_VOL,
    "long_sl_atr": REV4_SL_ATR, "long_tp_r": REV4_TP_R,
    "short_sl_atr": REV4_SL_ATR, "short_tp_r": REV4_TP_R,
    "sl_cap_pct": REV_SL_CAP_PCT, "risk_usdt": REV4_RISK_USDT, "leverage": REV_LEVERAGE,
    "max_concurrent": REV4_MAX_CONCURRENT, "max_margin": REV4_MAX_MARGIN_USDT,
    "cooldown_s": REV4_COOLDOWN_SECONDS,
    "cvd_filter": True, "range_regime": "CALM",   # 2026-08-30: reversion -> calm + CVD
    "open": rev4_open_trades, "pending": rev4_pending, "last_fire": rev4_last_fire,
}


# ============================================================================
# ==================== TIGHT 5 = CRASH-CONTINUATION SHORT ====================
# 2026-08-30. Derived from a retail "Heikin-Ashi + Supertrend + EMA200" doc that
# measured -0.41 meanR exactly as written. Keeping only the SHORT leg, gating it
# on a large prior fall, fixing the Supertrend parameters and sweeping the exits
# took it to +0.245. It is the only one of thirteen retail docs that produced a
# usable engine.
#
# SIGNAL (SHORT only, all coins, qv24 >= $2M, one per coin per day):
#   Gate:      24h return <= -20%   (the crash that makes the setup rare)
#   Trigger A: Heikin-Ashi Supertrend(period 7, factor 2.0) flips DOWN and
#              close < EMA200
#   Trigger B: EMA/CCI/MACD bearish zero-cross - close < EMA250, MACD hist < 0,
#              CCI(14) crosses from >=0 to <0, and CCI was above +100 in the
#              last 12 bars
#   Fire on EITHER (A OR B). They almost never coincide on the same bar (n=20 in
#   12 months) even though they share ~70% of the same coin-days: same crash,
#   different moment. AND is not viable; OR is.
# EXIT: SL 3.0 x ATR14, TP 1.5R, hold 4 days. TP distance is 4.5 ATR, so this is
#   NOT a small-TP artifact. The 36-cell SL x TP x hold grid was ALL positive on
#   the untouched test half - do not retune.
#
# MEASURED (12mo Aug2025-Jul2026, 459 coins, 20bps, flat $5, strict sim):
#   A only  780 trades, 15.7/wk, meanR +0.244, TRAIN +0.313 / TEST +0.147
#   B only  305 trades,  6.1/wk, meanR +0.423, TRAIN +0.534 / TEST +0.186
#   A OR B  872 trades, 17.5/wk, meanR +0.255, TRAIN +0.320 / TEST +0.168
#   Bootstrap P(edge>0) = 100%. Slippage: 0bps +0.281, 50bps +0.192, 100bps
#   +0.103 - by far the most slippage-tolerant engine here, because the stop is
#   wide (median 6.4% of price) so cost in R is small.
#
# THE CAVEAT, AND IT MUST BE REPEATED WHENEVER THIS ENGINE IS QUOTED:
#   October 2025 alone is 88% of the PnL (277 of 872 trades, meanR +0.609) - it
#   was the big crash month. EXCLUDING it, meanR falls +0.245 -> +0.046 and the
#   train half turns NEGATIVE. This is a CRASH/TAIL engine, not an all-weather
#   one. Quiet months are ~zero. Its value in the portfolio is that it pays
#   exactly when T1/T2/T4 (bull/flat reversion bets) lose: adding it moved
#   positive months 7/12 -> 9/12 and closed the train/test gap. Judge it on
#   drawdown smoothing, never on standalone PnL.
#
# Range regime WIDE (continuation engine). NO CVD filter - crash entries never
# print a new high so the divergence flag can never fire.
# ============================================================================

REV5_ENGINE_ENABLED   = os.environ.get("REV5_ENGINE_ENABLED", "1") == "1"
rev5_auto_enabled     = AUTO_RESUME_ON_START   # /t5_start /t5_stop
REV5_CRASH_RET        = float(os.environ.get("REV5_CRASH_RET", -0.20))   # 24h return gate
REV5_RET_WINDOW       = 96      # 96 x 15m = 24h
REV5_ST_PERIOD        = 7
REV5_ST_FACTOR        = 2.0
REV5_SL_ATR           = 3.0
REV5_TP_R             = 1.5
REV5_HOLD_SECONDS     = int(os.environ.get("REV5_HOLD_SECONDS", 96 * 3600))   # 4 days
REV5_MIN_QUOTE_VOL    = float(os.environ.get("REV5_MIN_QUOTE_VOL", 2_000_000))
REV5_MAX_CONCURRENT   = int(os.environ.get("REV5_MAX_CONCURRENT", 30))
REV5_RISK_USDT        = float(os.environ.get("REV5_RISK_USDT", 1.5))
REV5_MAX_MARGIN_USDT  = float(os.environ.get("REV5_MAX_MARGIN_USDT", 40))
REV5_SCAN_SECONDS     = int(os.environ.get("REV5_SCAN_SECONDS", 300))
REV5_COOLDOWN_SECONDS = int(os.environ.get("REV5_COOLDOWN_SECONDS", 24 * 3600))

rev5_open_trades = {}
rev5_pending     = {}
rev5_last_fire   = {}


def _ema_list(vals, n):
    if not vals:
        return []
    k = 2.0 / (n + 1.0)
    out = [vals[0]]
    for x in vals[1:]:
        out.append(x * k + out[-1] * (1.0 - k))
    return out


def _heikin(opens, highs, lows, closes):
    ha_c = [(opens[i] + highs[i] + lows[i] + closes[i]) / 4.0 for i in range(len(closes))]
    ha_o = [(opens[0] + closes[0]) / 2.0]
    for i in range(1, len(closes)):
        ha_o.append((ha_o[-1] + ha_c[i - 1]) / 2.0)
    ha_h = [max(highs[i], ha_o[i], ha_c[i]) for i in range(len(closes))]
    ha_l = [min(lows[i], ha_o[i], ha_c[i]) for i in range(len(closes))]
    return ha_o, ha_h, ha_l, ha_c


def _supertrend_dir(highs, lows, closes, period, factor):
    """Wilder-smoothed ATR Supertrend. Returns the direction list (+1 up, -1 down)."""
    n = len(closes)
    if n < period + 2:
        return None
    trs = []
    for i in range(n):
        pc = closes[i - 1] if i > 0 else closes[0]
        trs.append(max(highs[i] - lows[i], abs(highs[i] - pc), abs(lows[i] - pc)))
    a = [trs[0]]
    k = 1.0 / period
    for x in trs[1:]:
        a.append(x * k + a[-1] * (1.0 - k))
    fu = [(highs[0] + lows[0]) / 2.0 + factor * a[0]]
    fl = [(highs[0] + lows[0]) / 2.0 - factor * a[0]]
    d  = [1]
    for i in range(1, n):
        hl2 = (highs[i] + lows[i]) / 2.0
        ub, lb = hl2 + factor * a[i], hl2 - factor * a[i]
        fu.append(ub if (ub < fu[-1] or closes[i - 1] > fu[-1]) else fu[-1])
        fl.append(lb if (lb > fl[-1] or closes[i - 1] < fl[-1]) else fl[-1])
        if d[-1] == 1:
            d.append(-1 if closes[i] < fl[-1] else 1)
        else:
            d.append(1 if closes[i] > fu[-1] else -1)
    return d


def _cci_list(highs, lows, closes, n=14):
    out = []
    for i in range(len(closes)):
        if i < n - 1:
            out.append(None); continue
        tps = [(highs[j] + lows[j] + closes[j]) / 3.0 for j in range(i - n + 1, i + 1)]
        sma = sum(tps) / n
        md = sum(abs(x - sma) for x in tps) / n
        out.append(None if md == 0 else (tps[-1] - sma) / (0.015 * md))
    return out


def rev5_check_signal(symbol, btc_ret, eng):
    """T5 crash-continuation SHORT. Returns (side, entry, sl, tp) or None."""
    need = 300
    candles = get_candles(symbol, limit=need + REV_CANDLE_BUFFER, interval="15m")
    if candles:
        _t = _bar_ms(candles[-1])
        if _t and (_t % 900000) != 0:
            candles = candles[:-1]           # closed bars only
    if not candles or len(candles) < need:
        return None
    opens  = [float(c["open"]) for c in candles]
    closes = [cl(c) for c in candles]
    highs  = [h(c)  for c in candles]
    lows   = [l(c)  for c in candles]

    px = closes[-1]
    if px <= 0 or px < 0.001:
        return None

    # ---- crash gate: 24h return <= -20% ----
    past = closes[-(REV5_RET_WINDOW + 1)]
    if past <= 0:
        return None
    if (px / past - 1.0) > REV5_CRASH_RET:
        return None

    atr = atr_series(highs, lows, closes, REV_ATR_LEN)
    if not atr or atr[-1] is None or atr[-1] <= 0:
        return None
    atr_now = atr[-1]

    ema200 = _ema_list(closes, 200)
    ema250 = _ema_list(closes, 250)

    # ---- trigger A: Heikin-Ashi Supertrend flip DOWN + close < EMA200 ----
    trigger = None
    ha_o, ha_h, ha_l, ha_c = _heikin(opens, highs, lows, closes)
    d = _supertrend_dir(ha_h, ha_l, ha_c, REV5_ST_PERIOD, REV5_ST_FACTOR)
    if d and len(d) >= 2 and d[-1] == -1 and d[-2] == 1 and px < ema200[-1]:
        trigger = "ST"

    # ---- trigger B: EMA/CCI/MACD bearish zero-cross ----
    if trigger is None:
        e12 = _ema_list(closes, 12); e26 = _ema_list(closes, 26)
        macd = [e12[i] - e26[i] for i in range(len(closes))]
        sig9 = _ema_list(macd, 9)
        hist = macd[-1] - sig9[-1]
        cci = _cci_list(highs, lows, closes, 14)
        recent = [x for x in cci[-13:-1] if x is not None]
        if (px < ema250[-1] and hist < 0 and cci[-1] is not None
                and cci[-2] is not None and cci[-2] >= 0 and cci[-1] < 0
                and recent and max(recent) > 100):
            trigger = "ECM"

    if trigger is None:
        return None

    # range regime: continuation engine wants WIDE
    _ok, _reg = range_allows(symbol, eng.get("range_regime"), label=" T5")
    if not _ok:
        return None

    risk = REV5_SL_ATR * atr_now
    if risk <= 0 or (risk / px) > eng.get("sl_cap_pct", REV_SL_CAP_PCT):
        return None
    entry = px
    sl = entry + risk
    tp = entry - REV5_TP_R * risk
    print(f"[T5] {symbol} crash-short trigger={trigger} regime={_reg} "
          f"entry={entry} sl={round(sl,8)} tp={round(tp,8)}")
    return ("SELL", entry, sl, tp)


REV_T5 = {
    "name": "TIGHT 5", "tag": "t5",
    "signal_fn": rev5_check_signal,
    "ret_thr": 0.0, "vol_mult": 0.0, "vol_mult_max": 0.0, "atrp_max": 0.0,
    "side_only": "SELL",
    "range_regime": "WIDE",     # continuation engine
    "cvd_filter": False,        # crash entries never print a new high
    "min_quote_vol": REV5_MIN_QUOTE_VOL,
    "long_sl_atr": REV5_SL_ATR, "long_tp_r": REV5_TP_R,
    "short_sl_atr": REV5_SL_ATR, "short_tp_r": REV5_TP_R,
    "sl_cap_pct": REV_SL_CAP_PCT, "risk_usdt": REV5_RISK_USDT, "leverage": REV_LEVERAGE,
    "max_concurrent": REV5_MAX_CONCURRENT, "max_margin": REV5_MAX_MARGIN_USDT,
    "cooldown_s": REV5_COOLDOWN_SECONDS,
    "hold_seconds": REV5_HOLD_SECONDS,
    "open": rev5_open_trades, "pending": rev5_pending, "last_fire": rev5_last_fire,
}


def rev_in_cooldown(symbol, eng):
    cd = eng.get("cooldown_s", REV_COOLDOWN_SECONDS)
    return (time.time() - eng["last_fire"].get(symbol, 0)) < cd


import threading as _threading
_rev_claim_lock = _threading.Lock()
rev_claimed = set()   # coins a rev engine is mid-placing (T1 & T2 run in parallel threads;
                      # this makes the claim atomic so both can't open the SAME coin at once,
                      # which is exactly what doubled PENDLE 2026-08-20).


def rev_try_claim(symbol):
    """Atomically reserve a coin for a rev order. Returns True if we got it, False if
    another rev engine already holds/reserved it."""
    with _rev_claim_lock:
        if (symbol in rev_claimed or symbol in all_open_symbols()
                or symbol in rev_pending or symbol in rev2_pending
                or symbol in rev4_pending or symbol in rev5_pending):
            return False
        rev_claimed.add(symbol)
        return True


def rev_release_claim(symbol):
    with _rev_claim_lock:
        rev_claimed.discard(symbol)


def rev_symbol_busy(symbol, eng):
    # A coin held/pending/claimed by ANY engine is off-limits (prevents T1 and T2 both grabbing it).
    return (symbol in all_open_symbols() or symbol in eng["pending"]
            or symbol in rev_pending or symbol in rev2_pending
            or symbol in rev4_pending or symbol in rev5_pending
            or symbol in rev_claimed)


def rev_btc_regime():
    """BTC 4-day return. Returns None if it can't be read (then we skip, not guess)."""
    try:
        candles = get_candles("BTC-USDT", limit=REV_BTC_WINDOW + 5, interval="15m")
        if not candles or len(candles) < REV_BTC_WINDOW + 1:
            return None
        closes = [cl(c) for c in candles]
        past, now_px = closes[-(REV_BTC_WINDOW + 1)], closes[-1]
        if past <= 0:
            return None
        return now_px / past - 1.0
    except Exception as e:
        print(f"[REV BTC] {e}")
        return None


# ---------------------------------------------------------------------------
# ORDER-FLOW GATE  (2026-08-27)
#
# WHAT IT MEASURES. Over the last hour, how much volume hit the ASK (aggressive
# buying) versus the BID (aggressive selling). BingX's public trades endpoint
# marks each print with isBuyerMaker: True means the buyer was the maker, so the
# SELLER was the aggressor. flow = (buy_vol - sell_vol) / total, in [-1, +1].
# Only the SIGN is used - coin-normalising it (z-score vs the coin's own 30d
# flow) was measured and is WORSE (+0.111 vs +0.176).
#
# WHY IT HELPS, AND WHY THE DIRECTION DIFFERS PER ENGINE.
#   T1 / T2 are REVERSION bets: we want the flow to OPPOSE the move. Price up
#     6-8% but sellers are the aggressors = the rally is being distributed into,
#     so the short is good. Flow agreeing = near-zero edge (+0.026, TRAIN -0.014).
#   T3 is a CONTINUATION bet: we want the flow to CONFIRM the break. Flow
#     opposing the break is a real loser (-0.160), not just a weak trade.
# The anti-condition failing on all three engines is why this is believed to be
# a real effect rather than a fitted one.
#
# MEASURED (Binance 12mo Aug2025-Jul2026, 220-225 coins, 20bps, flat $5,
# maker limit entry, strict sim - flow came from taker-buy volume in the klines):
#   T1  +0.057 -> +0.263 meanR | 5.7 trades/wk | +$778 | monthly 9/12
#   T2  +0.021 -> +0.169 meanR | 5.0 trades/wk | +$438 | monthly 9/12
#   T3  +0.056 -> +0.186 meanR | 4.6 trades/wk | +$222 | monthly 8/12
# SKIPPING the bad trades beat down-SIZING them ($7 good/$3 bad) on every
# engine. Do not re-introduce sizing.
#
# WINDOW IS NARROW - 1 HOUR. Measured on T1: 15m +0.058 | 30m +0.055 |
# 1h +0.147 | 2h -0.032 | 24h +0.064. Do not drift off 1h.
#
# FAIL-OPEN BY DESIGN. The backtest used Binance's complete per-bar volume; live
# we get BingX's recent-trades list, which covers a different span per coin. If
# the call fails, or the trades returned span less than FLOW_MIN_COVER_S, we
# return None and the caller TAKES the trade (today's behaviour). The gate can
# only ever remove trades it is confident about. Every fill logs the flow value
# and the seconds covered so the live good/bad split can be checked after 3-4
# weeks - if coverage is short on most coins, turn the gate off.
# ---------------------------------------------------------------------------
FLOW_WINDOW_S    = 3600     # the measured-optimal 1h window
FLOW_MIN_COVER_S = 1800     # need >=30min of real trades or we do not judge
FLOW_MIN_TRADES  = 40
FLOW_TRADE_LIMIT = 500      # BingX max per call
_flow_cache = {}            # symbol -> (unix_ts, flow, covered_seconds)


def get_flow(symbol):
    """Return (flow, covered_seconds) or (None, 0) if we cannot judge.
    Cached for one 15m bar so a scan pass costs at most one call per symbol."""
    now = time.time()
    hit = _flow_cache.get(symbol)
    if hit and now - hit[0] < 900:
        return hit[1], hit[2]
    try:
        url = BASE_URL + "/openApi/swap/v2/quote/trades"
        r = requests.get(url, params={"symbol": symbol, "limit": FLOW_TRADE_LIMIT},
                         timeout=8).json()
        data = r.get("data") or []
        if not isinstance(data, list) or len(data) < FLOW_MIN_TRADES:
            _flow_cache[symbol] = (now, None, 0)
            return None, 0
        cutoff_ms = (now - FLOW_WINDOW_S) * 1000
        buy = sell = 0.0
        oldest = None
        newest = None
        for t in data:
            try:
                ts_ms = float(t.get("time") or t.get("T") or 0)
                qty   = float(t.get("qty") or t.get("q") or 0)
                price = float(t.get("price") or t.get("p") or 0)
            except Exception:
                continue
            if qty <= 0 or price <= 0 or ts_ms <= 0:
                continue
            if ts_ms < cutoff_ms:
                continue
            notional = qty * price
            maker = t.get("buyerMaker")
            if maker is None:
                maker = t.get("isBuyerMaker")
            if maker is None:
                maker = t.get("m")
            # buyerMaker True  -> the SELLER crossed the spread -> aggressive sell
            if maker in (True, "true", "True", 1, "1"):
                sell += notional
            else:
                buy += notional
            oldest = ts_ms if oldest is None else min(oldest, ts_ms)
            newest = ts_ms if newest is None else max(newest, ts_ms)
        total = buy + sell
        if total <= 0 or oldest is None:
            _flow_cache[symbol] = (now, None, 0)
            return None, 0
        covered = (newest - oldest) / 1000.0
        if covered < FLOW_MIN_COVER_S:
            _flow_cache[symbol] = (now, None, covered)
            return None, covered
        flow = (buy - sell) / total
        _flow_cache[symbol] = (now, flow, covered)
        return flow, covered
    except Exception as e:
        print(f"[FLOW {symbol}] {e}")
        _flow_cache[symbol] = (now, None, 0)
        return None, 0


# ============================================================================
# ============ CVD DIVERGENCE + RANGE-REGIME FILTERS (2026-08-30) =============
# Two filters measured on 12 months / 459 coins / 20bps. Applied together they
# took the portfolio from meanR +0.146 to +0.248 and the drawdown from 35.9R to
# 22.6R, i.e. the account needed for a given weekly target fell ~45%.
#
# 1) CVD DIVERGENCE (reversion engines only: T1, T2, T4)
#    CVD = cumulative sum of signed volume, signed by taker side. Over the last
#    24 closed 15m bars: BEARISH divergence = this bar printed the window's HIGH
#    while CVD did NOT print its window max (buyers pushed price up but the tape
#    did not follow). BULLISH is the mirror at the window low.
#    Take a SHORT only on bearish divergence, a LONG only on bullish.
#    Measured per engine (uncapped, 20bps): T1 +0.132 -> +0.196, T2 +0.263 ->
#    +0.316, T4 +0.131 -> +0.185, losing only 10-13% of trades. The removed
#    trades are strongly NEGATIVE (T1 -0.310, T4 -0.365), i.e. it cuts losers,
#    not trades at random. TRAIN and TEST both improve, which is the opposite of
#    an overfit signature.
#    NOT applied to T3/T5: T3 is a continuation bet and loses 88% of its trades
#    with TEST collapsing +0.146 -> +0.034; T5 enters on crashes, which never
#    print a new high, so the flag can never fire.
#
#    WHY BINANCE KLINES AND NOT BingX: this needs 24 bars (6 HOURS) of signed
#    volume. BingX klines carry no taker-buy field, and get_flow() below reads
#    /quote/trades which only reaches back ~1 hour - it cannot build this series.
#    Binance's public futures klines DO carry taker_buy_volume, need no API key,
#    and are exactly what the backtest used. So the FILTER reads Binance; the
#    TRADE still executes on BingX. Symbols map by stripping the dash
#    (ADA-USDT -> ADAUSDT). If Binance has no such symbol or the call fails we
#    TAKE the trade and log it - same fail-open policy as the flow gate, so a
#    Binance outage can never silently stop the bot.
#
# 2) RANGE REGIME (all five engines, direction depends on the bet type)
#    Per coin: 96-bar range as a fraction of price, versus its own trailing
#    480-bar 75th percentile. Above it = WIDE (volatile), else CALM.
#    Reversion engines (T1, T4) trade CALM only; continuation engines (T3, T5)
#    trade WIDE only; T2 is unfiltered (every variant hurt it).
#    Measured: T4 +0.131 -> +0.306 in calm and -0.045 in wide; T5 +0.266 ->
#    +0.368 in wide; T3 +0.143 -> +0.199 in wide. Mechanically sensible: in a
#    volatile tape price does not revert to the extreme it came from, and in a
#    calm tape breakouts die.
#    Computed from 1h candles (120 x 1h = the 480 x 15m window, 24 x 1h = the
#    96 x 15m window) so it costs ONE extra call per coin per bar, cached.
#
# Both windows/quantiles were swept (CVD 12/24/48 bars x range quantile
# 40/50/60/75) and 24 + 75 is the best cell by a clear margin - do not retune.
# ============================================================================

BINANCE_FAPI        = "https://fapi.binance.com"
CVD_WINDOW_BARS     = 24        # swept 12/24/48 - 24 wins
CVD_ENABLED         = os.environ.get("CVD_FILTER_ENABLED", "1") == "1"
RANGE_Q             = 0.75      # swept 0.40/0.50/0.60/0.75 - 0.75 wins
RANGE_WIN_1H        = 24        # 24 x 1h == 96 x 15m
RANGE_LOOKBACK_1H   = 120       # 120 x 1h == 480 x 15m
RANGE_ENABLED       = os.environ.get("RANGE_REGIME_ENABLED", "1") == "1"

_cvd_cache   = {}   # symbol -> (unix_ts, "BEAR"/"BULL"/"NONE"/None)
_range_cache = {}   # symbol -> (unix_ts, "WIDE"/"CALM"/None)


def _binance_symbol(symbol):
    return symbol.replace("-", "").upper()


def get_cvd_divergence(symbol):
    """Return 'BEAR', 'BULL', 'NONE' (no divergence this bar), or None (unknown).
    None means we could not judge -> callers FAIL OPEN and take the trade."""
    now = time.time()
    hit = _cvd_cache.get(symbol)
    if hit and now - hit[0] < 900:
        return hit[1]
    try:
        r = requests.get(BINANCE_FAPI + "/fapi/v1/klines",
                         params={"symbol": _binance_symbol(symbol), "interval": "15m",
                                 "limit": CVD_WINDOW_BARS + 2},
                         timeout=8)
        if r.status_code != 200:
            _cvd_cache[symbol] = (now, None)
            return None
        data = r.json()
        if not isinstance(data, list) or len(data) < CVD_WINDOW_BARS + 1:
            _cvd_cache[symbol] = (now, None)
            return None
        # Binance kline: [open_time, o, h, l, c, volume, close_time, quote_volume,
        #                 trades, taker_buy_base, taker_buy_quote, ignore]
        # Drop the last element: it is the still-forming bar.
        rows = data[:-1][-CVD_WINDOW_BARS:]
        if len(rows) < CVD_WINDOW_BARS:
            _cvd_cache[symbol] = (now, None)
            return None
        highs, lows, cvd, run = [], [], [], 0.0
        for k in rows:
            hi = float(k[2]); lo = float(k[3])
            qv = float(k[7]); tb = float(k[10])
            # signed volume = taker buy minus taker sell = 2*taker_buy - total
            run += (2.0 * tb - qv)
            highs.append(hi); lows.append(lo); cvd.append(run)
        cur_h, cur_l, cur_cvd = highs[-1], lows[-1], cvd[-1]
        out = "NONE"
        if cur_h >= max(highs) and cur_cvd < max(cvd):
            out = "BEAR"
        elif cur_l <= min(lows) and cur_cvd > min(cvd):
            out = "BULL"
        _cvd_cache[symbol] = (now, out)
        return out
    except Exception as e:
        print(f"[CVD {symbol}] {e}")
        _cvd_cache[symbol] = (now, None)
        return None


def cvd_allows(symbol, side, label=""):
    """True if the CVD divergence agrees with `side`. Fails OPEN on unknown."""
    if not CVD_ENABLED:
        return True, None
    d = get_cvd_divergence(symbol)
    if d is None:
        print(f"[CVD{label}] {symbol} unavailable - taking trade (fail-open)")
        return True, None
    want = "BEAR" if side in ("SELL", "SHORT") else "BULL"
    return (d == want), d


def get_range_regime(symbol):
    """'WIDE', 'CALM', or None if unknown (callers fail OPEN)."""
    now = time.time()
    hit = _range_cache.get(symbol)
    if hit and now - hit[0] < 900:
        return hit[1]
    try:
        candles = get_candles(symbol, limit=RANGE_LOOKBACK_1H + RANGE_WIN_1H + 5,
                              interval="1h")
        need = RANGE_LOOKBACK_1H + RANGE_WIN_1H
        if not candles or len(candles) < need:
            _range_cache[symbol] = (now, None)
            return None
        hs = [h(c) for c in candles]
        ls = [l(c) for c in candles]
        cs = [cl(c) for c in candles]
        rngs = []
        for i in range(RANGE_WIN_1H, len(candles)):
            w_hi = max(hs[i - RANGE_WIN_1H:i + 1])
            w_lo = min(ls[i - RANGE_WIN_1H:i + 1])
            px = cs[i]
            if px > 0 and w_hi > w_lo:
                rngs.append((w_hi - w_lo) / px)
        if len(rngs) < 30:
            _range_cache[symbol] = (now, None)
            return None
        cur = rngs[-1]
        hist = sorted(rngs[:-1])
        thr = hist[min(len(hist) - 1, int(RANGE_Q * len(hist)))]
        out = "WIDE" if cur > thr else "CALM"
        _range_cache[symbol] = (now, out)
        return out
    except Exception as e:
        print(f"[RANGE {symbol}] {e}")
        _range_cache[symbol] = (now, None)
        return None


def range_allows(symbol, want, label=""):
    """want is 'CALM', 'WIDE' or None (no filter). Fails OPEN on unknown."""
    if not want or not RANGE_ENABLED:
        return True, None
    reg = get_range_regime(symbol)
    if reg is None:
        print(f"[RANGE{label}] {symbol} unavailable - taking trade (fail-open)")
        return True, None
    return (reg == want), reg


def flow_allows(symbol, side, want_opposing, label=""):
    """side is 'BUY'/'LONG' or 'SELL'/'SHORT'.
    want_opposing=True  -> reversion engines: flow must be AGAINST the trade's
                           direction of travel, i.e. flow must AGREE with our side.
                           We short a pumped coin, so we want sellers aggressive
                           (flow < 0) -> for a SELL we require flow < 0.
    want_opposing=False -> T3 continuation: flow must back the break, so a LONG
                           needs flow > 0 and a SHORT needs flow < 0.
    Both cases reduce to the same test; they are named separately because the
    MEANING differs and the two engines were validated independently.
    Returns (ok, flow, covered). Fails OPEN: unknown flow -> ok=True."""
    flow, covered = get_flow(symbol)
    if flow is None:
        return True, None, covered
    is_long = side in ("BUY", "LONG")
    # a dead-flat book is no evidence either way -> do not block on it
    if abs(flow) < 0.01:
        return True, flow, covered
    ok = (flow > 0) if is_long else (flow < 0)
    if not ok:
        print(f"[FLOW SKIP]{label} {symbol} {side} flow={flow:+.3f} covered={int(covered)}s")
    return ok, flow, covered


REV_CANDLE_BUFFER = 15   # 2026-08-28: extra candles requested over the strict minimum.
                         # See the note in rev_check_signal - a zero-margin request was
                         # silently starving all three rev engines. Surplus is never read.

_rev_thin_logged = {}    # symbol -> last time we complained it had too few candles

def _rev_log_thin(symbol, eng, got, need):
    """Say it out loud when an engine skips a coin for want of candles, throttled to
    once per symbol per 10 min so a market-wide API problem is visible in the Render
    log without flooding it. Silence here is exactly what hid the bug above."""
    now = time.time()
    if now - _rev_thin_logged.get(symbol, 0) < 600:
        return
    _rev_thin_logged[symbol] = now
    print(f"[REV THIN] {eng['tag'].upper()} {symbol} skipped - got {got} candles, needs {need}")


_rev_diag_last = 0

def _rev_candle_diag():
    """Print, at most every 30 min, whether BingX's newest 15m candle is a CLOSED bar
    or a still-forming one. This exists to SETTLE that question with evidence instead
    of inference - the conditional drop in rev_check_signal was written without being
    able to verify it from the dev container. Read this line in the Render log:
      last_bar aligned=True  -> BingX returns closed bars; nothing is being dropped
      last_bar aligned=False -> the live bar was being used as if closed; now dropped
    Cost is one BTC candle fetch per half hour."""
    global _rev_diag_last
    now = time.time()
    if now - _rev_diag_last < 1800:
        return
    _rev_diag_last = now
    try:
        cs = get_candles("BTC-USDT", limit=3, interval="15m")
        if not cs:
            print("[REV DIAG] no BTC candles returned")
            return
        t = _bar_ms(cs[-1])
        aligned = bool(t) and (t % 900000 == 0)
        age_s = int(now - t / 1000) if t else -1
        print(f"[REV DIAG] last_bar ts={t} aligned={aligned} age={age_s}s "
              f"-> {'closed bar' if aligned else 'FORMING bar (dropped)'}")
    except Exception as e:
        print(f"[REV DIAG] failed: {e}")


def rev_check_signal(symbol, btc_ret, eng):
    """Return (side, entry_px, sl_px, tp_px) or None. side 'BUY' (long) / 'SELL' (short).
    Config-driven so Tight 1 (wide) and Tight 2 (ceiling filters) share one code path."""
    # 2026-08-28: windows are engine-configurable so T4 can use a 4h (16-bar) window
    # while T1/T2 keep 24h (96 bars). Engines that don't set them fall back to the globals.
    ret_win   = eng.get("ret_window", REV_RET_WINDOW)
    range_win = eng.get("range_window", REV_RANGE_WINDOW)
    need = max(ret_win, range_win) + REV_ATR_LEN + 5
    # 2026-08-28 BUGFIX - this line is why the engines could scan 148 coins and fire 0.
    # It used to request EXACTLY `need` and then reject on `len < need`, i.e. zero
    # margin: BingX returning even ONE candle fewer than asked killed the signal, on
    # every coin, every cycle, silently. Caught in the Render log as
    #   [CANDLES SHORT] PENGU-USDT 15m requested=35 got=34
    # for T4 (need=35). T1/T2 (need=115) had the same hole but never logged it - the
    # old log threshold only fired under 50 candles, so a 114-of-115 came back clean.
    # Fix is to ask for a cushion and keep requiring `need`. Asking for extra is free:
    # every calculation below indexes from the END (closes[-1], closes[-(ret_win+1)],
    # highs[-(range_win+1):]), so surplus leading candles are simply never read.
    candles = get_candles(symbol, limit=need + REV_CANDLE_BUFFER, interval="15m")
    # 2026-08-28: work on CLOSED bars only. The backtest that produced every number for
    # T1/T2/T4 measured closed 15m bars, but this function was reading candles[-1]
    # directly. If BingX includes the still-forming bar, that mismatch is severe:
    #   - vols[-1] is a PARTIAL bar's volume compared against a median of FULL bars.
    #     Scanning every 5 min on a 15m bar means ~1/3 of the volume has printed, so a
    #     1.3x filter really demands ~3.9x. On the T4 backtest signals that alone cuts
    #     1078 -> 244, i.e. ~77% of signals never fire.
    #   - pos >= 0.999 compares the live price against a window whose high includes the
    #     forming bar's own running high, so it asks "is price within 0.1% of this bar's
    #     high at this instant" instead of "did the bar CLOSE at its high".
    # Every other engine in this file already guards this (see candles[:-1] "closed
    # candles only" in get_btc_direction and the T1 daily logic); the rev path never did.
    #
    # CONDITIONAL BY DESIGN. I could not verify from here whether BingX returns the
    # forming bar, so this does not assume it. A real kline open_time is always a
    # multiple of the interval; the forming bar in Faisal's PENGU log carried a
    # non-aligned timestamp (10:46:40, 100s past the boundary). So drop the last bar
    # ONLY when its timestamp proves it is unclosed. If BingX already returns closed
    # bars, nothing is dropped and behaviour is unchanged - correct either way.
    if candles:
        _t = _bar_ms(candles[-1])
        if _t and (_t % 900000) != 0:
            candles = candles[:-1]
    if not candles or len(candles) < need:
        _rev_log_thin(symbol, eng, len(candles) if candles else 0, need)
        return None
    closes = [cl(c) for c in candles]
    highs  = [h(c)  for c in candles]
    lows   = [l(c)  for c in candles]
    vols   = [v(c)  for c in candles]

    px = closes[-1]
    if px <= 0 or px < 0.001:
        return None

    past = closes[-(ret_win + 1)]
    if past <= 0:
        return None
    ret = px / past - 1.0

    # 2026-08-25 BUGFIX #2 REVERSED: the 2026-08-20 note below was WRONG. Reproduction
    # check settles it - the filed backtest (12.5 trades/wk) only reproduces with the
    # HIGH/LOW range (12.0/wk); the CLOSE range gives 71/wk. A close-range is far
    # narrower so pos>=0.999 fires ~6x too often and the extra 5/6 have no edge.
    # Measured 8mo @20bps cap5: close = 71.0/wk meanR -0.010 -$132
    #                           high/low = 12.0/wk meanR +0.151 win 35% +$312
    # August 2026 week 33 alone: close 123 trades -$170 vs high/low 14 trades +$37.
    # 2026-08-20 BUGFIX #3: window sizing now matches backtest exactly.
    # Backtest used rolling(97) for the range (97 bars INCLUDING current) and a
    # volume median SHIFTED by 1 (the prior 96 bars, EXCLUDING current). Live was
    # off-by-one on the range window and included current volume in its own median
    # (a big current-bar volume was inflating the very median it's compared against,
    # making vr>=1.3 harder to reach). Both now match the backtest precisely.
    win_hi = max(highs[-(range_win + 1):])
    win_lo = min(lows[-(range_win + 1):])
    if win_hi <= win_lo:
        return None
    pos = (px - win_lo) / (win_hi - win_lo)

    vol_window = vols[-(range_win + 1):-1]   # prior N bars, excludes current
    if len(vol_window) < range_win:
        return None
    med_vol = sorted(vol_window)[len(vol_window) // 2]
    if med_vol <= 0:
        return None
    vr = vols[-1] / med_vol
    if vr < eng["vol_mult"]:
        return None
    # T2 ceiling: reject FOMO-continuation volume spikes (vr>3x measured NEGATIVE). T1 leaves this off (max=0).
    if eng["vol_mult_max"] > 0 and vr > eng["vol_mult_max"]:
        return None

    atr = atr_series(highs, lows, closes, REV_ATR_LEN)
    if not atr:
        return None
    atr_now = atr[-1]
    if atr_now is None or atr_now <= 0:
        return None

    # T2 ceiling: skip high-volatility coins (ATR%>3% was OOS-negative). T1 leaves this off (max=0).
    if eng["atrp_max"] > 0 and (atr_now / px) > eng["atrp_max"]:
        return None

    side = None
    # 2026-08-20 BUGFIX: was `pos >= 1.0` / `pos <= 0.0` (EXACT match to the 96-bar
    # high/low) - the close price essentially never equals the running extreme exactly,
    # so this fired almost never. The backtest that produced every number in this file
    # used 0.999/0.001 (near-extreme, not exact) - this is that same threshold, restored.
    if ret >= eng["ret_thr"] and pos >= 0.999:
        side = "SELL"
    elif ret <= -eng["ret_thr"] and pos <= 0.001:
        side = "BUY"
    if side is None:
        return None

    # 2026-08-28: T4 is SHORT-ONLY. Its long leg measured negative in EVERY regime
    # (bear -0.101, flat -0.101, bull +0.024) so it is skipped, not down-sized.
    # T1/T2 leave side_only unset and keep both legs.
    if eng.get("side_only") and side != eng["side_only"]:
        return None

    # 2026-08-30 RANGE-REGIME ROUTER. Reversion engines (T1, T4) only trade CALM
    # ranges, continuation engines trade WIDE. T2 sets nothing and is unfiltered
    # (every range variant measured worse for it). Fails OPEN if unreadable.
    _want_reg = eng.get("range_regime")
    if _want_reg:
        _ok, _reg = range_allows(symbol, _want_reg, label=" " + eng["tag"].upper())
        if not _ok:
            return None

    # 2026-08-30 CVD DIVERGENCE. Reversion engines only. Requires the tape to
    # DISAGREE with the price extreme we are fading. Fails OPEN if unreadable.
    if eng.get("cvd_filter"):
        _ok, _dv = cvd_allows(symbol, side, label=" " + eng["tag"].upper())
        if not _ok:
            return None

    if btc_ret is not None:
        if side == "BUY" and btc_ret < -REV_BTC_THR:
            return None
        if side == "SELL" and btc_ret > REV_BTC_THR:
            return None
        # 2026-08-28 REGIME GATE (T4 only): the 4h-reversion short is a BULL/FLAT
        # engine. Measured over 12mo: BULL +$421, FLAT +$121, BEAR only +$41. In bear
        # T3's clean-break engine takes over instead. Cutoff -3% was picked on the
        # TRAIN half and held on TEST (+0.181/+0.069 at >=-3%), so it is not fitted
        # to one period. Engines without regime_min_btc are unaffected.
        rmin = eng.get("regime_min_btc")
        if rmin is not None and btc_ret < rmin:
            return None

    if side == "BUY":
        sl = px - eng["long_sl_atr"] * atr_now
        risk = px - sl
        tp = px + risk * eng["long_tp_r"]
    else:
        sl = px + eng["short_sl_atr"] * atr_now
        risk = sl - px
        tp = px - risk * eng["short_tp_r"]

    if risk <= 0 or risk / px > eng["sl_cap_pct"]:
        return None

    # ORDER-FLOW GATE (reversion): only take it if the aggressors are on OUR side.
    ok, flow, covered = flow_allows(symbol, side, True, label=" " + eng["name"])
    if not ok:
        return None
    eng["last_flow"] = flow
    eng["last_flow_cover"] = covered
    return side, px, sl, tp


def place_rev_limit(symbol, side, entry, sl, tp, eng):
    """RESTING LIMIT entry at the signal close (maker, no slippage). Notifies Telegram on
    resting, and on a margin/depth skip (so a blocked trade is never silent)."""
    name = eng["name"]
    try:
        set_leverage_api(symbol, eng["leverage"])
        precision = symbol_precision.get(symbol, 4)
        risk_dist = abs(sl - entry)
        if risk_dist <= 0:
            return None
        risk_qty       = eng["risk_usdt"] / risk_dist
        margin_cap_qty = (eng["max_margin"] * eng["leverage"]) / entry
        qty = round(min(risk_qty, margin_cap_qty), precision)
        if qty <= 0:
            return None
        if not depth_ok(symbol, entry, sl, qty, side):
            print(f"[REV DEPTH SKIP] {name} {symbol} {side}")
            return "DEPTH_SKIP"

        pos_side   = "LONG" if side == "BUY" else "SHORT"
        close_side = "SELL" if side == "BUY" else "BUY"

        required_margin = qty * entry / eng["leverage"]
        avail = get_available_margin()
        if avail is not None and avail < required_margin * 1.05:
            print(f"[REV MARGIN SKIP] {name} {symbol} need ~${required_margin:.2f}, have ${avail:.2f}")
            return "MARGIN_SKIP"
        # 2026-08-25 shared budget: stops one engine (T2 cap 20) eating everything and
        # starving the others - and removes the long/short asymmetry that killed longs.
        if not rev_margin_allows(required_margin, get_balance()):
            print(f"[REV BUDGET SKIP] {name} {symbol} need ~${required_margin:.2f}, "
                  f"in use ${rev_margin_in_use():.2f}")
            return "MARGIN_SKIP"

        url = BASE_URL + "/openApi/swap/v2/trade/order"
        params = build_signed_params({
            "symbol": symbol, "side": side, "positionSide": pos_side,
            "type": "LIMIT", "price": round(entry, 6), "quantity": qty,
            "timeInForce": "GTC",
        })
        r = requests.post(url, params=params, headers={"X-BX-APIKEY": API_KEY}, timeout=10).json()
        oid = r.get("data", {}).get("order", {}).get("orderId", "N/A")
        if oid == "N/A":
            print(f"[REV LIMIT FAIL] {name} {symbol} {side} qty={qty} px={entry} - BingX: {r}")
            return None

        eng["pending"][symbol] = {
            "order_id": oid, "symbol": symbol, "side": side, "pos_side": pos_side,
            "close_side": close_side, "entry": entry, "sl": sl, "tp": tp,
            "qty": qty, "placed_ts": time.time(),
            "margin_used": required_margin, "risk_usdt": eng["risk_usdt"],
        }
        send_tg(
            f"\u23f3 {name} {symbol} {pos_side} LIMIT RESTING\n"
            f"Entry: {round(entry, 6)} | SL: {round(sl, 6)} | TP: {round(tp, 6)}\n"
            f"Risk: ${eng['risk_usdt']} | cancels in {REV_FILL_BARS * 15}m"
        )
        return oid
    except Exception as e:
        print(f"[REV ORDER {symbol}] {e}")
        return None


def rev_track_pending(eng):
    """Resting limits: promote to a live trade once filled (notify), cancel + notify (margin
    released) after REV_FILL_BARS if never filled."""
    pend = eng["pending"]
    if not pend:
        return
    name = eng["name"]
    now = time.time()
    for sym, p in list(pend.items()):
        try:
            status = check_order_status(p["order_id"], sym)
        except Exception as e:
            print(f"[REV PEND {sym}] {e}")
            continue

        if status == "FILLED":
            fill = get_fill_price(p["order_id"], sym, fallback=p["entry"])
            if p["side"] == "BUY":
                risk = fill - p["sl"]
                tp   = fill + risk * eng["long_tp_r"]
            else:
                risk = p["sl"] - fill
                tp   = fill - risk * eng["short_tp_r"]
            sl_id = place_sl_guarded(sym, p["close_side"], p["pos_side"], p["sl"], p["qty"])
            if sl_id is None:
                # SL could not be placed on BOTH attempts -> NEVER leave the position naked.
                # Market-close it immediately and alert, rather than run without a stop.
                try:
                    place_market_order(sym, p["close_side"], p["qty"], p["pos_side"])
                except Exception as _e:
                    print(f"[REV SL-FAIL emergency close {sym}] {_e}")
                send_tg(f"\u26a0\ufe0f {name} {sym} {p['pos_side']} SL placement FAILED - "
                        f"position emergency-closed (no naked risk).")
                eng["last_fire"][sym] = now
                pend.pop(sym, None)
                continue
            tp_id = place_tp_guarded(sym, p["close_side"], p["pos_side"], tp, p["qty"], label=name)
            eng["open"][p["order_id"]] = {
                "symbol": sym, "side": p["side"], "pos_side": p["pos_side"],
                "close_side": p["close_side"], "entry": p["entry"], "entry_fill": fill,
                "sl": p["sl"], "tp": tp, "total_qty": p["qty"], "qty": p["qty"],
                "risk_usdt": eng["risk_usdt"], "margin_used": p.get("margin_used", 0),
                "sl_id": sl_id, "tp_id": tp_id, "open_ts": now, "gone_strikes": 0,
                "label": name, "eng_tag": eng["tag"],
            }
            eng["last_fire"][sym] = now
            pend.pop(sym, None)
            slip = abs(fill - p["entry"]) / p["entry"] * 100 if p["entry"] else 0
            send_tg(
                f"\u2705 {name} {sym} {p['pos_side']} FILLED (trade executed)\n"
                f"Entry: {fill} (limit {round(p['entry'], 6)}, slip {slip:.2f}%)\n"
                f"SL: {round(p['sl'], 6)} | TP: {round(tp, 6)} | Risk: ${eng['risk_usdt']}"
            )
            continue

        if now - p["placed_ts"] > REV_FILL_BARS * 15 * 60:
            try:
                cancel_order(sym, p["order_id"])
            except Exception as e:
                print(f"[REV CANCEL {sym}] {e}")
            pend.pop(sym, None)
            print(f"[REV] {name} {sym} limit unfilled after {REV_FILL_BARS} bars - cancelled")
            send_tg(
                f"\u274e {name} {sym} {p['pos_side']} LIMIT CANCELLED (unfilled {REV_FILL_BARS * 15}m) "
                f"- margin released"
            )


def rev_track_trades(eng):
    """Time-stop + reconcile closed positions, then journal. Two-strike exchange
    confirmation before declaring a position closed (no phantom closes)."""
    trades = eng["open"]
    if not trades:
        return
    name = eng["name"]
    now = time.time()
    for oid, t in list(trades.items()):
        sym = t["symbol"]

        if now - t.get("open_ts", now) > eng.get("hold_seconds", REV_HOLD_SECONDS):
            try:
                place_market_order(sym, t["close_side"], t["total_qty"], t["pos_side"])
                for k in ("sl_id", "tp_id"):
                    if t.get(k) and t[k] != "N/A":
                        try:
                            cancel_order(sym, t[k])
                        except Exception:
                            pass
                ef = t.get("entry_fill", t.get("entry", 0))
                px = get_current_price(sym)
                pnl = None
                if px and ef:
                    risk_dist = abs(ef - t["sl"])
                    if risk_dist > 0:
                        d = (px - ef) if t["side"] == "BUY" else (ef - px)
                        pnl = d / risk_dist * eng["risk_usdt"]
                exit_r = None
                if px and ef:
                    rd = abs(ef - t["sl"])
                    if rd > 0:
                        exit_r = round(((px - ef) if t["side"] == "BUY" else (ef - px)) / rd, 2)
                send_tg(f"\u23f1\ufe0f {name} {sym} {t['pos_side']} time-stop closed | entry {ef}"
                        + (f" | {'+' if pnl >= 0 else '-'}${abs(round(pnl, 2))}" if pnl is not None else ""))
                journal_closed_trade({
                    "label": name, "symbol": sym, "side": t["pos_side"], "entry": ef,
                    "result": "time-stop", "pnl": round(pnl, 2) if pnl is not None else 0,
                    "exit_r": exit_r if exit_r is not None else 0,
                })
            except Exception as e:
                print(f"[REV TIMESTOP {sym}] {e}")
            eng["last_fire"][sym] = now
            trades.pop(oid, None)
            continue

        gone = exchange_has_position(sym)
        if gone is None or gone:
            t["gone_strikes"] = 0
            continue
        t["gone_strikes"] = t.get("gone_strikes", 0) + 1
        if t["gone_strikes"] < 2:
            print(f"[REV] {name} {sym} looks gone (strike 1/2) - waiting for confirmation")
            continue

        ef = t.get("entry_fill", t.get("entry", 0))
        t.setdefault("symbol", sym)
        t.setdefault("risk_usdt", eng["risk_usdt"])
        result, pnl, exit_r, exit_px = resolve_exit(t, t["side"], ef)
        for k in ("sl_id", "tp_id"):
            if t.get(k) and t[k] != "N/A":
                try:
                    cancel_order(sym, t[k])
                except Exception:
                    pass
        emoji = "\u2705" if result == "TP" else ("\u274c" if result == "SL" else "\u2139\ufe0f")
        held_min = int((now - t.get("open_ts", now)) / 60)
        send_tg(f"{emoji} {name} {sym} {t['pos_side']} {result} | entry {ef} | held {held_min}m")
        journal_closed_trade({
            "label": name, "symbol": sym, "side": t["pos_side"], "entry": ef,
            "result": result, "pnl": round(pnl, 2), "exit_r": exit_r,
            "exit_px": round(exit_px, 8) if exit_px else 0,
        })
        eng["last_fire"][sym] = now
        trades.pop(oid, None)


def _rev_engine_loop(eng, is_enabled_fn, scan_seconds):
    """Shared scan/track loop for a rev engine descriptor."""
    name = eng["name"]
    # 2026-08-28: was hardcoded "24h-reversion" for every engine, which mislabelled T4
    # (a 4h window) in the Render log. Describe the engine actually starting.
    _win_h = eng.get("range_window", REV_RANGE_WINDOW) * 15 // 60
    _sides = eng.get("side_only") or "both sides"
    print(f"{name} loop started - {_win_h}h range-extreme reversion, {_sides}, "
          f"resting-limit entry")
    while True:
        try:
            rev_track_pending(eng)
            rev_track_trades(eng)

            if not is_enabled_fn():
                time.sleep(30)
                continue
            if api_backoff_active():
                time.sleep(60)
                continue

            open_slots = eng["max_concurrent"] - (len(eng["open"]) + len(eng["pending"]))
            if open_slots <= 0:
                time.sleep(scan_seconds)
                continue

            all_syms = get_futures_symbols()
            # 2026-08-29: the floor is per-engine now. It used to be REV_MIN_QUOTE_VOL for
            # everyone, so lowering it for T4 would silently have moved T1 and T2 too -
            # and neither of those was tested at a lower floor. Engines that don't set
            # min_quote_vol keep the shared $2M default, i.e. T1/T2 are untouched.
            symbols = get_liquid_symbols(
                all_syms, min_quote_vol=eng.get("min_quote_vol", REV_MIN_QUOTE_VOL),
                max_n=REV_MAX_SYMBOLS, exclude_top_n=REV_EXCLUDE_TOP_N,
            )
            btc_ret = rev_btc_regime()
            _rev_candle_diag()
            scanned = fired = 0
            for sym in symbols:
                if open_slots <= 0:
                    break
                if rev_in_cooldown(sym, eng) or rev_symbol_busy(sym, eng):
                    continue
                if is_tokenized(sym) or coin_too_young(sym):
                    continue
                scanned += 1
                try:
                    sig = eng.get("signal_fn", rev_check_signal)(sym, btc_ret, eng)
                except Exception as e:
                    print(f"[REV SIG {sym}] {e}")
                    continue
                if not sig:
                    continue
                side, entry, sl, tp = sig
                # Atomically claim the coin so the other rev engine (parallel thread) can't
                # also open it this cycle. If we can't claim, skip.
                if not rev_try_claim(sym):
                    continue
                res = place_rev_limit(sym, side, entry, sl, tp, eng)
                # Once it's resting in eng["pending"] the pending-check covers it, so release
                # the short-lived claim. If placement failed/skipped, also release.
                rev_release_claim(sym)
                if res and res not in ("DEPTH_SKIP", "MARGIN_SKIP"):
                    fired += 1
                    open_slots -= 1
                time.sleep(0.3)

            btc_str = f"{btc_ret * 100:+.1f}%" if btc_ret is not None else "n/a"
            print(f"[{eng['tag'].upper()} SCAN] open={len(eng['open'])}/{eng['max_concurrent']} "
                  f"pending={len(eng['pending'])} on={is_enabled_fn()} scanned={scanned} "
                  f"fired={fired} btc4d={btc_str}")
        except Exception as e:
            print(f"[REV LOOP {name}] {e}")
        time.sleep(scan_seconds)


def rev_loop():
    if not REV_ENGINE_ENABLED:
        print("[REV] Tight 1 disabled by env REV_ENGINE_ENABLED=0 - loop idle")
        return
    _rev_engine_loop(REV_T1, lambda: rev_auto_enabled, REV_SCAN_SECONDS)


def rev2_loop():
    if not REV2_ENGINE_ENABLED:
        print("[REV] Tight 2 disabled by env REV2_ENGINE_ENABLED=0 - loop idle")
        return
    _rev_engine_loop(REV_T2, lambda: rev2_auto_enabled, REV2_SCAN_SECONDS)


def rev4_loop():
    if not REV4_ENGINE_ENABLED:
        print("[REV] Tight 4 disabled by env REV4_ENGINE_ENABLED=0 - loop idle")
        return
    _rev_engine_loop(REV_T4, lambda: rev4_auto_enabled, REV4_SCAN_SECONDS)


def rev5_loop():
    if not REV5_ENGINE_ENABLED:
        print("[REV] Tight 5 disabled by env REV5_ENGINE_ENABLED=0 - loop idle")
        return
    _rev_engine_loop(REV_T5, lambda: rev5_auto_enabled, REV5_SCAN_SECONDS)

# ==================== END TIGHT 1 (24h-reversion) ====================


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

    # Thread(target=t2_loop, ...) RETIRED 2026-08-20: old block-based Tight 2 (SHORT resistance-block)
    # removed. Tight 2 is now the ceiling 24h-reversion engine (rev2_loop) below.
    Thread(target=rev2_loop,                daemon=True).start()   # Tight 2 = 24h-reversion CEILING (2026-08-20)
    Thread(target=trailing_loop,            daemon=True).start()
    # Thread(target=t3_loop, ...) DISABLED 2026-08-23: old dormant/OI watchlist T3 retired; superseded by S/R Sweep SHORT (t3_scalp_loop).
    Thread(target=handle_telegram_commands, daemon=True).start()
    Thread(target=oi_collector_loop,        daemon=True).start()   # OI logger -> Supabase (2026-08-10)
    Thread(target=t3_scalp_loop,            daemon=True).start()   # Tight 3 = S/R Sweep SHORT bear engine (2026-08-23)
    Thread(target=rev_loop,                 daemon=True).start()   # Tight 1 = 24h-reversion, resting-limit entry (2026-08-20)
    Thread(target=rev4_loop,                daemon=True).start()   # Tight 4 = 4h range-extreme reversion SHORT, bull/flat only (2026-08-28)
    Thread(target=rev5_loop,                daemon=True).start()   # Tight 5 = crash-continuation SHORT (2026-08-30)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
