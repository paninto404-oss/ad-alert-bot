import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

# === НАСТРОЙКИ ===
# Токен берётся из переменной окружения BOT_TOKEN (Railway -> Variables)
TELEGRAM_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = int(os.environ.get("CHAT_ID", "347263656"))

# Ваши пары (проверяются всегда)
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "NEARUSDT", "AAVEUSDT", "ZROUSDT", "1000FLOKIUSDT",
    "SOLUSDT", "XRPUSDT", "BNBUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT",
    "DOTUSDT", "SUIUSDT", "LTCUSDT", "APTUSDT", "ENAUSDT", "UNIUSDT",
    "ATOMUSDT", "ARBUSDT", "STRKUSDT", "1000PEPEUSDT", "LDOUSDT", "XLMUSDT",
    "WLDUSDT", "TRXUSDT", "OPUSDT", "ICPUSDT", "DASHUSDT", "HBARUSDT",
    "FILUSDT", "BCHUSDT", "ETCUSDT", "TWTUSDT", "DYDXUSDT", "INJUSDT",
    "CRVUSDT", "BOMEUSDT", "TAOUSDT", "TRUMPUSDT", "ZKUSDT", "FETUSDT",
    "ALGOUSDT", "CAKEUSDT", "TIAUSDT", "NOTUSDT", "ZECUSDT"
]

INTERVAL = "3m"
LOCAL_UTC_OFFSET = 3   # ваш часовой пояс (UTC+3), нужен только для команды /check ПАРА ЧЧ:ММ
CHECK_INTERVAL = 180   # длина свечи в секундах (3 минуты)
THRESHOLD = 8.0        # уровень Advance/Decline Ratio
AD_MODE = "open"       # "open": свеча растёт, если close > open; "prev_close": если close выше закрытия предыдущей свечи
CANDLE_DELAY = 5       # секунд ожидания после закрытия свечи, чтобы Binance её зафиксировал

# Автоматическое добавление пар с Binance Futures
AUTO_ADD_PAIRS = True
MAX_PAIRS = 400               # общее число пар (ваши + добавленные)
MIN_DAILY_RANGE_PCT = 1.5     # не брать пары, у которых размах цены за 24ч меньше этого процента
STABLE_BASES = {"USDC", "FDUSD", "TUSD", "USDP", "BUSD", "DAI", "USDE", "USD1", "AEUR", "EURI", "EUR"}
WORKERS = 10                  # сколько пар проверять параллельно
REFRESH_EVERY = 60            # обновлять список пар каждые N свечей (60 = раз в 3 часа)

API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# Общее состояние между мониторингом и командами
state = {"paused": False, "last_check": None, "alerts_sent": 0, "symbols": list(SYMBOLS)}


# === TELEGRAM ===
def send_telegram(message, chat_id=CHAT_ID):
    try:
        requests.post(
            f"{API}/sendMessage",
            json={"chat_id": chat_id, "text": message},
            timeout=10,
        )
    except Exception as e:
        print("Ошибка отправки в Telegram:", e)


def send_long(text, chat_id=CHAT_ID, limit=3500):
    """Отправляет длинный текст частями (лимит Telegram около 4096 символов)."""
    while text:
        if len(text) <= limit:
            send_telegram(text, chat_id)
            break
        cut = text.rfind("\n", 0, limit)
        if cut == -1:
            cut = text.rfind(", ", 0, limit)
        if cut == -1:
            cut = limit
        send_telegram(text[:cut], chat_id)
        text = text[cut:].lstrip(", \n")


# === ДАННЫЕ ===
def get_klines(symbol, interval=INTERVAL, limit=40, end_time=None):
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if end_time:
        params["endTime"] = end_time
    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        if not isinstance(data, list):
            print(f"Binance вернул ошибку для {symbol}:", data)
            return None
        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"
        ])
        for col in ["open", "high", "low", "close", "volume", "quote_volume", "taker_buy_quote"]:
            df[col] = df[col].astype(float)
        return df
    except Exception as e:
        print(f"Ошибка получения данных {symbol}:", e)
        return None


def get_spot_klines(symbol, end_time=None, limit=3):
    """Свечи спота Binance (нужны только для сверки объёма в команде /check)."""
    params = {"symbol": symbol, "interval": INTERVAL, "limit": limit}
    if end_time:
        params["endTime"] = end_time
    try:
        data = requests.get("https://api.binance.com/api/v3/klines", params=params, timeout=10).json()
        if isinstance(data, list) and len(data) >= 2:
            return data
    except Exception:
        pass
    return None


def candle_open_ms(hhmm):
    """Время открытия 3-минутной свечи (мс) по времени ЧЧ:ММ в вашем часовом поясе."""
    tz = timezone(timedelta(hours=LOCAL_UTC_OFFSET))
    now = datetime.now(tz)
    h, m = map(int, hhmm.split(":"))
    t = now.replace(hour=h, minute=m - m % 3, second=0, microsecond=0)
    if t > now:
        t -= timedelta(days=1)
    return int(t.timestamp() * 1000)


def closed_only(df):
    """Оставляет только закрытые свечи: последняя (ещё формирующаяся) отбрасывается."""
    if df is None or df.empty:
        return df
    now_ms = int(time.time() * 1000)
    if int(df["close_time"].iloc[-1]) > now_ms:
        return df.iloc[:-1]
    return df


def load_extra_symbols():
    """USDT-перпетуалы Binance Futures по убыванию объёма торгов за 24ч.
    Стейблкоины и почти не двигающиеся пары отбрасываются."""
    try:
        info = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=15).json()
        tradable = {
            x["symbol"] for x in info["symbols"]
            if x.get("status") == "TRADING"
            and x.get("contractType") == "PERPETUAL"
            and x.get("quoteAsset") == "USDT"
        }
        tickers = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=15).json()
        ranked = []
        for t in tickers:
            sym = t["symbol"]
            if sym not in tradable or sym[:-4] in STABLE_BASES:
                continue
            low, high = float(t["lowPrice"]), float(t["highPrice"])
            if low <= 0 or (high - low) / low * 100 < MIN_DAILY_RANGE_PCT:
                continue
            ranked.append((float(t["quoteVolume"]), sym))
        ranked.sort(reverse=True)
        return [sym for _, sym in ranked]
    except Exception as e:
        print("Ошибка загрузки списка пар:", e)
        return []


def build_symbols():
    """Ваш список + самые ликвидные волатильные пары до общего числа MAX_PAIRS."""
    symbols = list(SYMBOLS)
    if AUTO_ADD_PAIRS:
        for sym in load_extra_symbols():
            if len(symbols) >= MAX_PAIRS:
                break
            if sym not in symbols:
                symbols.append(sym)
    return symbols


# === ИНДИКАТОРЫ ===
def candle_flags(df, mode=AD_MODE):
    """Для каждой свечи: растёт ли она и падает ли она (два списка True/False)."""
    if mode == "prev_close":
        diff = df["close"] - df["close"].shift(1)
    else:
        diff = df["close"] - df["open"]
    return (diff > 0).tolist(), (diff < 0).tolist()


def ad_series(df, length=10, mode=AD_MODE):
    """Advance/Decline Ratio для каждой свечи (начиная с 10-й): растущие / падающие из последних 10.
    Последний элемент списка - последняя свеча."""
    ups, downs = candle_flags(df, mode)
    out = []
    for end in range(length - 1, len(df)):
        up = sum(ups[end - length + 1:end + 1])
        down = sum(downs[end - length + 1:end + 1])
        if down == 0:
            out.append(None)   # деление на ноль: на графике значения нет
        else:
            out.append(round(up / down, 2))
    return out


def first_drop_at_last(series, level=THRESHOLD):
    """True, если на ПОСЛЕДНЕЙ свече индикатор впервые пошёл вниз после достижения уровня level или выше.

    Пример: ... 5, 9, 4 -> True (9 достигнут, затем падение).
            ... 9, 8, 8, 5 -> False (первое падение было на 9 -> 8, это не оно).
    """
    reached = False   # уровень достигнут, ждём первого падения
    locked = False    # первое падение уже было, ждём нового подъёма
    last = len(series) - 1
    for i, cur in enumerate(series):
        prev = series[i - 1] if i > 0 else None
        triggered = False
        if cur is None:
            # значения нет (0 падающих свечей) - линия прерывается, начинаем отсчёт заново
            reached = False
            locked = False
        elif prev is not None and reached and cur < prev:
            triggered = True
            reached = False
            locked = True
        else:
            if locked and prev is not None and (cur > prev or cur < level):
                locked = False
            if not locked and cur >= level:
                reached = True
        if i == last:
            return triggered
    return False


def analyze(df):
    """Разбирает последнюю свечу в df. Возвращает словарь с результатами или None."""
    if df is None or len(df) < 12:
        return None
    series = ad_series(df)
    if len(series) < 2:
        return None
    last = df.iloc[-1]
    prev_candle = df.iloc[-2]
    # Дельта объёма свечи: агрессивные покупки минус агрессивные продажи.
    # Зелёная CVD-свеча = дельта больше нуля.
    delta = 2 * float(last["taker_buy_quote"]) - float(last["quote_volume"])
    drop = first_drop_at_last(series)
    # Индикатор Volume: красный столбец = свеча закрылась ниже открытия,
    # и объём (в USDT) этой свечи больше объёма предыдущей.
    vol_red = float(last["close"]) < float(last["open"])
    vol_last = float(last["quote_volume"])
    vol_prev = float(prev_candle["quote_volume"])
    vol_higher = vol_last > vol_prev
    return {
        "prev": series[-2],
        "cur": series[-1],
        "drop": drop,
        "delta": delta,
        "vol_red": vol_red,
        "vol_higher": vol_higher,
        "vol_last": vol_last,
        "vol_prev": vol_prev,
        "signal": drop and delta > 0 and vol_red and vol_higher,
        "candle": int(last["open_time"]),
    }


def check_symbol(symbol):
    """Проверяет последнюю закрытую свечу. Возвращает (symbol, info) или (symbol, None)."""
    return symbol, analyze(closed_only(get_klines(symbol)))


def candle_string(df, n=10):
    ups, downs = candle_flags(df, "open")
    return "".join("▲" if u else "▼" if d else "·" for u, d in zip(ups[-n:], downs[-n:]))


def fmt_num(x):
    return "–" if x is None else f"{x:g}"


# === МОНИТОРИНГ (работает в фоновом потоке) ===
def wait_for_candle_close():
    """Ждёт закрытия ближайшей 3-минутной свечи (+ небольшая задержка)."""
    now = time.time()
    next_close = (now // CHECK_INTERVAL + 1) * CHECK_INTERVAL
    time.sleep(max(next_close - now, 0) + CANDLE_DELAY)


def monitor_loop():
    state["symbols"] = build_symbols()
    print(f"Бот запущен. Слежу за {len(state['symbols'])} парами на 3м таймфрейме...")
    send_telegram(f"Бот запущен. Слежу за {len(state['symbols'])} парами")

    alerted = {}   # symbol -> время открытия свечи, по которой уже был алерт
    cycles = 0

    while True:
        try:
            if state["paused"]:
                time.sleep(5)
                continue

            wait_for_candle_close()

            if state["paused"]:
                continue

            cycles += 1
            if cycles % REFRESH_EVERY == 0:
                new_symbols = build_symbols()
                if len(new_symbols) >= len(state["symbols"]) * 0.8:
                    state["symbols"] = new_symbols

            with ThreadPoolExecutor(max_workers=WORKERS) as pool:
                results = list(pool.map(check_symbol, state["symbols"]))

            alerts = []
            failed = 0
            for symbol, info in results:
                if info is None:
                    failed += 1
                    continue
                if info["signal"] and alerted.get(symbol) != info["candle"]:
                    alerted[symbol] = info["candle"]
                    alerts.append(symbol)

            if alerts:
                state["alerts_sent"] += len(alerts)
                send_long("\n".join(alerts))
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Сигналов: {len(alerts)}")

            state["last_check"] = datetime.now().strftime("%H:%M:%S")
            print(f"[{state['last_check']}] Проверено {len(results) - failed} из {len(results)} пар.")

        except Exception as e:
            print("Ошибка в основном цикле:", e)
            time.sleep(30)


# === КОМАНДЫ ===
HELP_TEXT = (
    "Команды:\n"
    "/status - состояние бота\n"
    "/pairs - список отслеживаемых пар\n"
    "/check SYMBOL - разбор последней закрытой свечи (AD, CVD, Volume, сигнал)\n"
    "/check SYMBOL ЧЧ:ММ - разбор свечи за указанное время, например /check BTCUSDT 08:48\n"
    "/pause - остановить мониторинг\n"
    "/resume - возобновить мониторинг\n"
    "/help - эта справка"
)


def handle_command(text, chat_id):
    parts = text.split()
    cmd = parts[0].split("@")[0].lower()
    args = parts[1:]

    if cmd in ("/start", "/help"):
        send_telegram(
            "Привет! Я присылаю название пары, когда Advance/Decline Ratio (AD 10) после уровня "
            f"{THRESHOLD:g}+ впервые идёт вниз, CVD-свеча на этой же свече зелёная, "
            "а столбец Volume красный и выше предыдущего.\n\n" + HELP_TEXT,
            chat_id,
        )

    elif cmd == "/status":
        status = "на паузе" if state["paused"] else "работает"
        send_telegram(
            f"Статус: {status}\n"
            f"Пар: {len(state['symbols'])}\n"
            f"Таймфрейм: {INTERVAL}\n"
            f"Уровень AD: {THRESHOLD:g}\n"
            f"Последняя проверка: {state['last_check'] or 'ещё не было'}\n"
            f"Сигналов отправлено: {state['alerts_sent']}",
            chat_id,
        )

    elif cmd == "/pairs":
        send_long(", ".join(state["symbols"]), chat_id)

    elif cmd in ("/check", "/score"):
        if not args:
            send_telegram("Укажи пару, например: /check BTCUSDT или /check BTCUSDT 08:48", chat_id)
            return
        symbol = args[0].upper()
        end_time = None
        if len(args) > 1:
            try:
                end_time = candle_open_ms(args[1]) + CHECK_INTERVAL * 1000 - 1
            except Exception:
                send_telegram("Время укажи как ЧЧ:ММ, например: /check BTCUSDT 08:48", chat_id)
                return
        df = closed_only(get_klines(symbol, end_time=end_time))
        info = analyze(df)
        if info is None:
            send_telegram(f"Не удалось получить данные по {symbol}", chat_id)
            return
        tz = timezone(timedelta(hours=LOCAL_UTC_OFFSET))
        label = datetime.fromtimestamp(info["candle"] / 1000, tz).strftime("%H:%M")
        spot = get_spot_klines(symbol, end_time=info["candle"] + CHECK_INTERVAL * 1000 - 1)
        spot_line = ""
        if spot:
            sp_last, sp_prev = float(spot[-1][7]), float(spot[-2][7])
            spot_line = f"Volume спот: {sp_prev:,.0f} → {sp_last:,.0f} USDT ({'выше' if sp_last > sp_prev else 'ниже'})\n"
        send_telegram(
            f"{symbol}, свеча {label} (UTC+{LOCAL_UTC_OFFSET})\n"
            f"AD (бот, режим {AD_MODE}): {fmt_num(info['prev'])} → {fmt_num(info['cur'])}\n"
            f"AD последние 5 свечей, close>open: {', '.join(fmt_num(v) for v in ad_series(df, mode='open')[-5:])}\n"
            f"AD последние 5 свечей, к пред. закрытию: {', '.join(fmt_num(v) for v in ad_series(df, mode='prev_close')[-5:])}\n"
            f"Последние 10 свечей (▲ рост, ▼ падение, · без изменений): {candle_string(df)}\n"
            f"Первое падение после {THRESHOLD:g}+: {'да' if info['drop'] else 'нет'}\n"
            f"CVD-свеча: {'зелёная' if info['delta'] > 0 else 'красная'} (дельта {info['delta']:,.0f} USDT)\n"
            f"Свеча: {'красная' if info['vol_red'] else 'зелёная'}\n"
            f"Volume фьючерсы: {info['vol_prev']:,.0f} → {info['vol_last']:,.0f} USDT "
            f"({'выше' if info['vol_higher'] else 'ниже'})\n"
            f"{spot_line}"
            f"Сигнал: {'ДА' if info['signal'] else 'нет'}",
            chat_id,
        )

    elif cmd == "/pause":
        state["paused"] = True
        send_telegram("Мониторинг поставлен на паузу", chat_id)

    elif cmd == "/resume":
        state["paused"] = False
        send_telegram("Мониторинг возобновлён", chat_id)

    else:
        send_telegram("Неизвестная команда. /help - список команд", chat_id)


def commands_loop():
    offset = None
    while True:
        try:
            params = {"timeout": 30}
            if offset:
                params["offset"] = offset
            r = requests.get(f"{API}/getUpdates", params=params, timeout=40)
            for upd in r.json().get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or {}
                text = (msg.get("text") or "").strip()
                chat_id = msg.get("chat", {}).get("id")
                if not text.startswith("/") or chat_id is None:
                    continue
                # Бот отвечает только владельцу
                if chat_id != CHAT_ID:
                    print("Игнорирую команду от постороннего чата:", chat_id)
                    continue
                handle_command(text, chat_id)
        except Exception as e:
            print("Ошибка приёма команд:", e)
            time.sleep(5)


def main():
    threading.Thread(target=monitor_loop, daemon=True).start()
    commands_loop()


if __name__ == "__main__":
    main()
