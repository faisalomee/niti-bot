

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
FAST_RISK_USDT = float(os.environ.get("FAST_RISK_USDT", 5.0))

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
                send_tg(
                    "TIGHT SPIKE - " + symbol + " [" + direction + "]\n"
                    "Vol: " + str(round(spike_ratio, 1)) + "x 3-day baseline\n"
                    "Watching for cooldown + re-entry...\nNiti Tight"
                )
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
            sl = range_low - atr_now * TIGHT_SL_ATR_BUFFER_MULT if side == "BUY" else range_high + atr_now * TIGHT_SL_ATR_BUFFER_MULT
            risk = abs(entry - sl)
            tight_watch.pop(symbol, None)
            if risk <= 0:
                return True
            tp = round(entry + risk * TIGHT_RR_TP, 6) if side == "BUY" else round(entry - risk * TIGHT_RR_TP, 6)
            sl = round(sl, 6)

            trade_status = ""
            if tight_auto_trade_enabled:
                oid = place_tight_order(symbol, side, entry, sl, tp)
                trade_status = "\nOrder: " + str(oid) if oid and oid != "N/A" else "\nOrder failed"
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
# ==================== END COMBINED PATCH ====================

