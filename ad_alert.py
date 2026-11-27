#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ad_alert.py - Telegram-бот: сигналы по Binance USDT-M Futures, таймфрейм 3m.

Сигнал приходит, когда на только что ЗАКРЫТОЙ 3m-свече (сигнальной) выполнено всё:
  1. AD 10 (растущие / падающие свечи за 10 свечей) на предсигнальной свече >= 8,
     а на сигнальной свече упало ниже. AD = бесконечность ИГНОРИРУЕТСЯ.
  2. CVD зелёный: дельта сигнальной свечи > 0 (покупки taker - продажи taker).
  3. Volume: сигнальная свеча красная, её объём больше объёма предсигнальной свечи.
  4. BoP сигнальной свечи (округление до 0.01) есть в словаре BOP_SIGNALS ниже.
     BoP = (close - open) / (high - low)

Сообщение для каждого сигнала (отдельное сообщение):
    ACH -
    -0.02 3-0 3%
  1-я строка: тикер и направление ("+" вверх, "-" вниз),
  2-я строка: уровень BoP, статистика и уровень TP/SL из словаря BOP_SIGNALS (текст не меняется).

Переменные окружения (Railway -> Variables):
  BOT_TOKEN, CHAT_ID  - обязательные
  MAX_PAIRS           - сколько пар смотреть (по умолчанию 600, 0 = все подходящие)
  MIN_RANGE_PCT       - отсев "неволатильных": дневной размах (high-low)/low в %, по умолчанию 1.0
  EXCLUDE             - доп. исключения через запятую, например "BTCDOMUSDT,XYZUSDT"
  AD_THRESHOLD        - порог AD (по умолчанию 8), AD_PERIOD (по умолчанию 10)
  BINANCE_URL         - по умолчанию https://fapi.binance.com
"""

import json
import logging
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

# ----------------------------------------------------------------------------
# Уровни BoP сигнальной свечи: ключ -> (направление, статистика, уровень TP/SL)
# ----------------------------------------------------------------------------
BOP_SIGNALS = {
    # группа TP/SL 3%
    "-0.02": ("-", "3-0", "3%"),
    "-0.09": ("-", "4-0", "3%"),
    "-0.28": ("-", "5-2", "3%"),
    "-0.30": ("-", "10-3", "3%"),
    "-0.39": ("-", "7-3", "3%"),
    "-0.42": ("+", "6-2", "3%"),
    "-0.46": ("-", "6-1", "3%"),
    "-0.71": ("+", "9-2", "3%"),
    "-0.82": ("-", "5-0", "3%"),
    "-0.92": ("+", "3-0", "3%"),
    # группа TP 2.4% / SL 4.4%
    "-0.04": ("+", "8-1", "4.4%"),
    "-0.05": ("+", "10-0", "4.4%"),
    "-0.08": ("+", "14-0", "4.4%"),
    "-0.15": ("-", "6-1", "4.4%"),
    "-0.17": ("+", "16-4", "4.4%"),
    "-0.23": ("+", "6-1", "4.4%"),
    "-0.35": ("-", "5-1", "4.4%"),
    "-0.38": ("+", "10-2", "4.4%"),
    "-0.40": ("-", "11-2", "4.4%"),
    "-0.41": ("+", "9-0", "4.4%"),
    "-0.45": ("-", "10-1", "4.4%"),
    "-0.55": ("+", "7-1", "4.4%"),
    "-0.59": ("-", "6-1", "4.4%"),
    "-0.63": ("+", "4-1", "4.4%"),
    "-0.64": ("+", "6-0", "4.4%"),
    "-0.65": ("-", "7-0", "4.4%"),
    "-0.72": ("-", "5-0", "4.4%"),
    "-0.75": ("-", "13-3", "4.4%"),
    "-0.76": ("-", "3-0", "4.4%"),
    "-0.78": ("+", "3-0", "4.4%"),
    "-0.79": ("-", "3-0", "4.4%"),
    "-1.00": ("+", "12-3", "4.4%"),
}

# ----------------------------------------------------------------------------
# Настройки
# ----------------------------------------------------------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = str(os.environ.get("CHAT_ID", ""))
BINANCE_URL = os.environ.get("BINANCE_URL", "https://fapi.binance.com").rstrip("/")

INTERVAL = "3m"
INTERVAL_MS = 180_000

AD_PERIOD = int(os.environ.get("AD_PERIOD", "10"))
AD_THRESHOLD = float(os.environ.get("AD_THRESHOLD", "8"))

MAX_PAIRS = int(os.environ.get("MAX_PAIRS", "600"))
MIN_RANGE_PCT = float(os.environ.get("MIN_RANGE_PCT", "1.0"))
PAIRS_REFRESH_SEC = int(os.environ.get("PAIRS_REFRESH_SEC", str(6 * 3600)))
WORKERS = int(os.environ.get("WORKERS", "20"))
CYCLE_DELAY_SEC = float(os.environ.get("CYCLE_DELAY_SEC", "4"))
SEND_GAP_SEC = float(os.environ.get("SEND_GAP_SEC", "0.4"))   # пауза между сообщениями

# Нужно AD_PERIOD+1 закрытых свечей (AD на предсигнальной и на сигнальной) + запас.
# limit < 100 у Binance = вес запроса 1 (600 пар = 600 веса за цикл, лимит 2400/мин).
KLINES_LIMIT = AD_PERIOD + 6

# Стейблы, фиат и подобное: не волатильные, отсекаем по базовому активу.
EXCLUDE_BASES = {
    "USDC", "FDUSD", "TUSD", "BUSD", "USDP", "DAI", "USDE", "EUR", "AEUR", "EURI",
    "XUSD", "USD1", "RLUSD", "USDS", "PYUSD", "UST", "USTC", "GUSD", "USDD", "FRAX",
    "LUSD", "SUSD", "BFUSD", "AUSD", "USDX", "CUSD", "ZUSD", "USDG", "USDT",
}
EXTRA_EXCLUDE = {s.strip().upper() for s in os.environ.get("EXCLUDE", "").split(",") if s.strip()}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("ad_alert")

# ----------------------------------------------------------------------------
# Общее состояние
# ----------------------------------------------------------------------------
_lock = threading.Lock()
STATE = {
    "pairs": [],
    "base": {},          # symbol -> базовый актив (для текста сигнала)
    "eligible": 0,       # сколько пар подходит после фильтров (до ограничения MAX_PAIRS)
    "pairs_updated": 0.0,
    "paused": False,
    "last_cycle_ts": 0.0,
    "last_cycle_sec": 0.0,
    "last_cycle_ok": 0,
    "last_cycle_err": 0,
    "sent_total": 0,
    "latest": {},        # symbol -> результат evaluate() по последней закрытой свече
    "last_sent": {},     # symbol -> open_time сигнальной свечи, по которой уже слали
}

_tls = threading.local()
_rate_until = 0.0  # если Binance просит подождать (429/418)


class BinanceError(Exception):
    pass


def _session():
    s = getattr(_tls, "s", None)
    if s is None:
        s = requests.Session()
        _tls.s = s
    return s


def binance_get(path, params=None):
    global _rate_until
    wait = _rate_until - time.time()
    if wait > 0:
        time.sleep(min(wait, 60))
    r = _session().get(BINANCE_URL + path, params=params, timeout=10)
    if r.status_code in (418, 429):
        retry = int(r.headers.get("Retry-After", "10"))
        _rate_until = time.time() + retry
        raise BinanceError("лимит запросов Binance (HTTP %s), пауза %s c" % (r.status_code, retry))
    if r.status_code in (403, 451):
        raise BinanceError(
            "HTTP %s: Binance закрыт для региона сервера. Смените регион в Railway (не US)." % r.status_code
        )
    r.raise_for_status()
    return r.json()


# ----------------------------------------------------------------------------
# Telegram
# ----------------------------------------------------------------------------
def tg_api(method, **params):
    url = "https://api.telegram.org/bot%s/%s" % (BOT_TOKEN, method)
    r = requests.post(url, data=params, timeout=40)
    return r.json()


def send_one(text, chat_id=None):
    """Одно сообщение; при лимите Telegram (429) ждёт и повторяет."""
    chat_id = chat_id or CHAT_ID
    for attempt in range(4):
        try:
            res = tg_api("sendMessage", chat_id=chat_id, text=text)
            if res.get("ok"):
                return True
            if res.get("error_code") == 429:
                wait = int(res.get("parameters", {}).get("retry_after", 3))
                time.sleep(wait + 1)
                continue
            log.warning("Telegram sendMessage: %s", res)
        except Exception as e:  # noqa: BLE001
            log.warning("Telegram sendMessage ошибка: %s", e)
        time.sleep(2)
    return False


def send(text, chat_id=None):
    """Длинный текст режем на части (лимит Telegram 4096 символов)."""
    chunks, cur = [], ""
    for line in text.split("\n"):
        if len(cur) + len(line) + 1 > 3800:
            chunks.append(cur)
            cur = ""
        cur += line + "\n"
    if cur.strip():
        chunks.append(cur)
    for chunk in chunks:
        send_one(chunk.rstrip("\n"), chat_id)


# ----------------------------------------------------------------------------
# Индикаторы
# ----------------------------------------------------------------------------
def parse_closed(raw, now_ms):
    """Только ЗАКРЫТЫЕ свечи (closeTime уже в прошлом)."""
    out = []
    for k in raw:
        if int(k[6]) >= now_ms:
            continue
        out.append({
            "t": int(k[0]),
            "o": float(k[1]),
            "h": float(k[2]),
            "l": float(k[3]),
            "c": float(k[4]),
            "v": float(k[5]),     # объём в монетах
            "tb": float(k[9]),    # taker buy base volume
        })
    return out


def ad_ratio(candles):
    """AD = растущие / падающие свечи. Если падающих нет -> бесконечность."""
    ups = sum(1 for x in candles if x["c"] > x["o"])
    downs = sum(1 for x in candles if x["c"] < x["o"])
    if downs == 0:
        return (math.inf if ups > 0 else 0.0), ups, downs
    return ups / downs, ups, downs


def bop_of(x):
    rng = x["h"] - x["l"]
    return (x["c"] - x["o"]) / rng if rng > 0 else 0.0


def bop_key(bop):
    """BoP, округлённый до 0.01, как строка ('-0.46'). '-0.00' приводим к '0.00'."""
    r = round(bop, 2) + 0.0
    if r == 0:
        r = 0.0
    return "%.2f" % r


def fmt_ad(v):
    return "∞" if math.isinf(v) else "%.1f" % v


def evaluate(cs):
    """cs - список закрытых свечей по времени, последняя = сигнальная."""
    if len(cs) < AD_PERIOD + 2:
        return None
    sig, pre = cs[-1], cs[-2]
    ad_now, ups, downs = ad_ratio(cs[-AD_PERIOD:])
    ad_prev, _, _ = ad_ratio(cs[-AD_PERIOD - 1:-1])

    delta = 2.0 * sig["tb"] - sig["v"]          # CVD свечи
    ad_inf = math.isinf(ad_prev)                # AD = бесконечность игнорируем
    cond_ad = (not ad_inf) and ad_prev >= AD_THRESHOLD and ad_now < ad_prev
    cond_cvd = delta > 0
    cond_vol = sig["c"] < sig["o"] and sig["v"] > pre["v"]
    bop = bop_of(sig)
    key = bop_key(bop)
    cond_bop = key in BOP_SIGNALS

    direction = stats = label = None
    if cond_ad and cond_cvd and cond_vol and cond_bop:
        direction, stats, label = BOP_SIGNALS[key]

    return {
        "t": sig["t"],
        "close": sig["c"],
        "ad_now": ad_now, "ups": ups, "downs": downs, "ad_prev": ad_prev, "ad_inf": ad_inf,
        "delta": delta, "vol": sig["v"], "vol_pre": pre["v"],
        "red": sig["c"] < sig["o"],
        "bop": bop, "bop_key": key,
        "cond_ad": cond_ad, "cond_cvd": cond_cvd, "cond_vol": cond_vol, "cond_bop": cond_bop,
        "direction": direction, "stats": stats, "label": label,
    }


def signal_text(name, r):
    """Текст сообщения для сигнала:
         ACH -
         -0.02 3-0 3%
    """
    return "%s %s\n%s %s %s" % (name, r["direction"], r["bop_key"], r["stats"], r["label"])


# ----------------------------------------------------------------------------
# Список пар
# ----------------------------------------------------------------------------
def refresh_pairs():
    info = binance_get("/fapi/v1/exchangeInfo")
    ok = {}
    for s in info["symbols"]:
        if s.get("contractType") != "PERPETUAL":
            continue
        if s.get("quoteAsset") != "USDT" or s.get("status") != "TRADING":
            continue
        base = s.get("baseAsset", "")
        if base.upper() in EXCLUDE_BASES:
            continue
        ok[s["symbol"]] = base

    tickers = binance_get("/fapi/v1/ticker/24hr")
    rows = []
    for t in tickers:
        sym = t.get("symbol")
        if sym not in ok or sym in EXTRA_EXCLUDE:
            continue
        low, high = float(t["lowPrice"]), float(t["highPrice"])
        if low <= 0:
            continue
        if (high - low) / low * 100.0 < MIN_RANGE_PCT:   # неволатильные
            continue
        rows.append((float(t["quoteVolume"]), sym))
    rows.sort(reverse=True)
    syms = [s for _, s in rows]
    if MAX_PAIRS > 0:
        syms = syms[:MAX_PAIRS]
    with _lock:
        STATE["pairs"] = syms
        STATE["eligible"] = len(rows)
        STATE["base"] = {s: ok[s] for s in syms}
        STATE["pairs_updated"] = time.time()
    log.info("Список пар обновлён: %d (подходящих всего %d)", len(syms), len(rows))
    return syms


# ----------------------------------------------------------------------------
# Один цикл проверки
# ----------------------------------------------------------------------------
def check_symbol(sym, now_ms, expected_open):
    raw = binance_get("/fapi/v1/klines", {"symbol": sym, "interval": INTERVAL, "limit": KLINES_LIMIT})
    cs = parse_closed(raw, now_ms)
    if len(cs) < AD_PERIOD + 2:
        return None
    if cs[-1]["t"] != expected_open:     # свежей закрытой свечи нет (пара без сделок / задержка)
        return None
    return evaluate(cs)


def run_cycle():
    t0 = time.time()
    now_ms = int(t0 * 1000)
    expected_open = (now_ms // INTERVAL_MS) * INTERVAL_MS - INTERVAL_MS
    with _lock:
        syms = list(STATE["pairs"])
        base = dict(STATE["base"])

    def worker(sym):
        try:
            return sym, check_symbol(sym, now_ms, expected_open), None
        except Exception as e:  # noqa: BLE001
            return sym, None, str(e)

    ok = err = 0
    first_err = None
    signals = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for sym, res, e in ex.map(worker, syms):
            if e:
                err += 1
                first_err = first_err or e
                continue
            ok += 1
            if res is None:
                continue
            with _lock:
                STATE["latest"][sym] = res
                already = STATE["last_sent"].get(sym) == res["t"]
            if res["direction"] and not already:
                signals.append((sym, res))
                with _lock:
                    STATE["last_sent"][sym] = res["t"]

    if signals:
        signals.sort(key=lambda x: x[0])
        for sym, r in signals:                      # каждый сигнал - отдельное сообщение
            name = base.get(sym) or sym.replace("USDT", "")
            send_one(signal_text(name, r))
            time.sleep(SEND_GAP_SEC)
        with _lock:
            STATE["sent_total"] += len(signals)

    dt = time.time() - t0
    with _lock:
        STATE["last_cycle_ts"] = time.time()
        STATE["last_cycle_sec"] = dt
        STATE["last_cycle_ok"] = ok
        STATE["last_cycle_err"] = err
    log.info("Цикл: пар %d, ок %d, ошибок %d, сигналов %d, %.1f c", len(syms), ok, err, len(signals), dt)
    if syms and err > len(syms) * 0.5 and first_err:
        send("Предупреждение: в цикле много ошибок (%d из %d). Пример: %s" % (err, len(syms), first_err))


# ----------------------------------------------------------------------------
# Команды Telegram
# ----------------------------------------------------------------------------
HELP = (
    "Команды:\n"
    "/status - состояние бота\n"
    "/pairs - список отслеживаемых пар\n"
    "/rules - условия сигнала и список уровней BoP\n"
    "/score - пары, у которых сейчас AD >= %g (или /score BTCUSDT - детали по паре)\n"
    "/pause - приостановить сигналы\n"
    "/resume - возобновить"
) % AD_THRESHOLD


def cmd_rules():
    g3 = [k for k, v in BOP_SIGNALS.items() if v[2] == "3%"]
    g44 = [k for k, v in BOP_SIGNALS.items() if v[2] == "4.4%"]
    fmt = lambda ks: ", ".join("%s %s" % (k, BOP_SIGNALS[k][0]) for k in ks)  # noqa: E731
    return (
        "Сигнал, когда на сигнальной свече (3m):\n"
        "1) AD был >= %g (бесконечность игнорируется) и упал ниже\n"
        "2) CVD зелёный\n"
        "3) объём красный и выше предыдущего\n"
        "4) BoP (округление до 0.01) из списка\n\n"
        "Уровни 3%%:\n%s\n\nУровни 4.4%%:\n%s"
    ) % (AD_THRESHOLD, fmt(g3), fmt(g44))


def cmd_status():
    with _lock:
        paused = STATE["paused"]
        n = len(STATE["pairs"])
        last = STATE["last_cycle_ts"]
        dt = STATE["last_cycle_sec"]
        ok, err = STATE["last_cycle_ok"], STATE["last_cycle_err"]
        sent = STATE["sent_total"]
        elig = STATE["eligible"]
    ago = ("%d c назад" % (time.time() - last)) if last else "ещё не было"
    return (
        "Статус: %s\nПар: %d\nПоследний цикл: %s (%.1f c, ок %d, ошибок %d)\n"
        "Сигналов отправлено: %d\n"
        "Подходящих пар на бирже: %d, лимит MAX_PAIRS=%d\n"
        "Правило: AD>=%g и падение (AD ∞ игнор), CVD зелёный, объём красный выше предыдущего, BoP из списка (/rules)"
    ) % ("ПАУЗА" if paused else "работает", n, ago, dt, ok, err, sent, elig, MAX_PAIRS, AD_THRESHOLD)


def cmd_pairs():
    with _lock:
        pairs = list(STATE["pairs"])
    if not pairs:
        return "Список пар пуст."
    return "Пар: %d\n%s" % (len(pairs), ", ".join(p.replace("USDT", "") for p in pairs))


def cmd_score(arg):
    with _lock:
        latest = dict(STATE["latest"])
        base = dict(STATE["base"])
    if arg:
        sym = arg.upper()
        if not sym.endswith("USDT"):
            sym += "USDT"
        r = latest.get(sym)
        if not r:
            return "Нет данных по %s (пары нет в списке или ещё не было цикла)." % sym
        itog = signal_text(base.get(sym) or sym.replace("USDT", ""), r) if r["direction"] else "сигнала нет"
        return (
            "%s (свеча %s UTC)\n"
            "AD: %s → %s (растущих %d, падающих %d)%s\n"
            "CVD свечи: %+.2f %s\n"
            "Объём: %.2f, предыдущая: %.2f, свеча %s\n"
            "BoP: %s %s\n"
            "Условия: AD %s, CVD %s, Volume %s, BoP %s\n"
            "Итог:\n%s"
        ) % (
            sym, time.strftime("%d.%m %H:%M", time.gmtime(r["t"] / 1000)),
            fmt_ad(r["ad_prev"]), fmt_ad(r["ad_now"]), r["ups"], r["downs"],
            "  [AD ∞ - игнор]" if r["ad_inf"] else "",
            r["delta"], "(зелёный)" if r["delta"] > 0 else "(красный)",
            r["vol"], r["vol_pre"], "красная" if r["red"] else "зелёная",
            r["bop_key"], "(в списке)" if r["cond_bop"] else "(нет в списке)",
            "да" if r["cond_ad"] else "нет", "да" if r["cond_cvd"] else "нет",
            "да" if r["cond_vol"] else "нет", "да" if r["cond_bop"] else "нет",
            itog,
        )
    hot = [(s, r) for s, r in latest.items() if (not r["ad_inf"]) and r["ad_now"] >= AD_THRESHOLD]
    hot.sort(key=lambda x: (-x[1]["ad_now"], x[0]))
    if not hot:
        return "Сейчас нет пар с AD >= %g (AD ∞ не считается)." % AD_THRESHOLD
    return "Пары с AD >= %g (по последней закрытой свече):\n%s" % (
        AD_THRESHOLD,
        "\n".join("%s: AD %s (%d↑ / %d↓)" % (s, fmt_ad(r["ad_now"]), r["ups"], r["downs"]) for s, r in hot[:60]),
    )


def handle_command(text):
    parts = text.strip().split()
    if not parts:
        return None
    cmd = parts[0].split("@")[0].lower()
    arg = parts[1] if len(parts) > 1 else ""
    if cmd == "/start":
        return "Бот запущен.\n\n" + HELP
    if cmd == "/status":
        return cmd_status()
    if cmd == "/pairs":
        return cmd_pairs()
    if cmd == "/rules":
        return cmd_rules()
    if cmd == "/score":
        return cmd_score(arg)
    if cmd == "/pause":
        with _lock:
            STATE["paused"] = True
        return "Сигналы приостановлены."
    if cmd == "/resume":
        with _lock:
            STATE["paused"] = False
        return "Сигналы возобновлены."
    return None


def poll_commands():
    try:
        tg_api("deleteWebhook")
    except Exception as e:  # noqa: BLE001
        log.warning("deleteWebhook: %s", e)
    offset = None
    while True:
        try:
            params = {"timeout": 30, "allowed_updates": json.dumps(["message"])}
            if offset is not None:
                params["offset"] = offset
            r = requests.get(
                "https://api.telegram.org/bot%s/getUpdates" % BOT_TOKEN, params=params, timeout=45
            ).json()
            if not r.get("ok"):
                log.warning("getUpdates: %s", r)
                time.sleep(5)
                continue
            for u in r.get("result", []):
                offset = u["update_id"] + 1
                msg = u.get("message")
                if not msg or "text" not in msg:
                    continue
                if str(msg["chat"]["id"]) != CHAT_ID:     # отвечаем только вам
                    continue
                answer = handle_command(msg["text"])
                if answer:
                    send(answer, chat_id=str(msg["chat"]["id"]))
        except Exception as e:  # noqa: BLE001
            log.warning("poll_commands: %s", e)
            time.sleep(5)


# ----------------------------------------------------------------------------
# Основной цикл
# ----------------------------------------------------------------------------
def sleep_until_next_candle():
    now = time.time()
    nxt = ((int(now * 1000) // INTERVAL_MS) + 1) * INTERVAL_MS / 1000.0 + CYCLE_DELAY_SEC
    time.sleep(max(0.0, nxt - now))


def main():
    if not BOT_TOKEN or not CHAT_ID:
        raise SystemExit("Не заданы BOT_TOKEN и/или CHAT_ID")
    threading.Thread(target=poll_commands, daemon=True).start()

    while True:
        try:
            refresh_pairs()
            break
        except Exception as e:  # noqa: BLE001
            log.error("Не удалось получить список пар: %s", e)
            send("Не удалось получить список пар: %s" % e)
            time.sleep(60)

    with _lock:
        n = len(STATE["pairs"])
        elig = STATE["eligible"]
    send("Бот запущен (версия BoP 3% / 4.4%). Пар: %d из %d подходящих (MAX_PAIRS=%d), 3m, Binance Futures.\nСписок условий: /rules" % (n, elig, MAX_PAIRS))

    while True:
        sleep_until_next_candle()
        try:
            if time.time() - STATE["pairs_updated"] > PAIRS_REFRESH_SEC:
                try:
                    refresh_pairs()
                except Exception as e:  # noqa: BLE001
                    log.warning("Обновление списка пар: %s", e)
            with _lock:
                paused = STATE["paused"]
            if not paused:
                run_cycle()
        except Exception as e:  # noqa: BLE001
            log.exception("Ошибка цикла: %s", e)


if __name__ == "__main__":
    main()
