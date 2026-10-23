import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

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
CHECK_INTERVAL = 180   # длина свечи в секундах (3 минуты)
THRESHOLD = 8.0        # уровень Advance/Decline Ratio
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
def get_klines(symbol, interval=INTERVAL, limit=40):
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
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
def window_ratio(df, end, length=10):
    """Advance/Decline Ratio на свече с индексом end: растущие / падающие свечи из последних 10."""
    window = df.iloc[end - length + 1:end + 1]
    up = int((window["close"] > window["open"]).sum())
    down = int((window["close"] < window["open"]).sum())
    if down == 0:
        ratio = float("inf") if up > 0 else 0.0
    else:
        ratio = round(up / down, 2)
    return ratio, up, down


def ad_series(df, length=10):
    """Значения индикатора для каждой свечи (начиная с 10-й). Последний элемент - последняя свеча."""
    return [window_ratio(df, end, length)[0] for end in range(length - 1, len(df))]


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
        if prev is not None and reached and cur < prev:
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


def check_symbol(symbol):
    """Проверяет последнюю закрытую свечу. Возвращает (symbol, info) или (symbol, None)."""
    df = closed_only(get_klines(symbol))
    if df is None or len(df) < 12:
        return symbol, None
    series = ad_series(df)
    if len(series) < 2:
        return symbol, None
    last = df.iloc[-1]
    # Дельта объёма свечи: агрессивные покупки минус агрессивные продажи.
    # Зелёная CVD-свеча = дельта больше нуля.
    delta = 2 * float(last["taker_buy_quote"]) - float(last["quote_volume"])
    drop = first_drop_at_last(series)
    return symbol, {
        "prev": series[-2],
        "cur": series[-1],
        "drop": drop,
        "delta": delta,
        "signal": drop and delta > 0,
        "candle": int(last["open_time"]),
    }


def fmt_num(x):
    return "∞" if x == float("inf") else f"{x:g}"


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
                    alerts.append(f"{symbol} (AD {fmt_num(info['prev'])} → {fmt_num(info['cur'])})")

            if alerts:
                state["alerts_sent"] += len(alerts)
                send_long("Сигнал (AD пошёл вниз после 8+, CVD-свеча зелёная):\n" + "\n".join(alerts))
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
    "/check SYMBOL - разбор последней закрытой свечи (AD, CVD, сигнал)\n"
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
            "Привет! Я присылаю сигнал, когда Advance/Decline Ratio (AD 10) после уровня "
            f"{THRESHOLD:g}+ впервые идёт вниз, а CVD-свеча на этой же свече зелёная.\n\n" + HELP_TEXT,
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
            send_telegram("Укажи пару, например: /check BTCUSDT", chat_id)
            return
        symbol = args[0].upper()
        _, info = check_symbol(symbol)
        if info is None:
            send_telegram(f"Не удалось получить данные по {symbol}", chat_id)
            return
        send_telegram(
            f"{symbol} (последняя закрытая свеча)\n"
            f"AD: {fmt_num(info['prev'])} → {fmt_num(info['cur'])}\n"
            f"Первое падение после {THRESHOLD:g}+: {'да' if info['drop'] else 'нет'}\n"
            f"CVD-свеча: {'зелёная' if info['delta'] > 0 else 'красная'} (дельта {info['delta']:,.0f} USDT)\n"
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
