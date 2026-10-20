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
CHECK_INTERVAL = 180
THRESHOLD = 8.0
COOLDOWN = 900       # не повторять алерт по одной паре чаще чем раз в 15 минут
CANDLE_DELAY = 5     # секунд ожидания после закрытия свечи, чтобы Binance успел её закрыть

# Автоматически добавлять к списку выше самые ликвидные USDT-фьючерсы Binance
AUTO_ADD_PAIRS = True
EXTRA_PAIRS_COUNT = 250   # сколько пар добавить (по объёму торгов за 24ч)
WORKERS = 10              # сколько пар проверять параллельно
REFRESH_EVERY = 60        # обновлять список пар каждые N проверок (60 = раз в 3 часа)

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


# === ДАННЫЕ И РАСЧЁТ ===
def get_klines(symbol, interval=INTERVAL, limit=30):
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"
        ])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        return df
    except Exception as e:
        print(f"Ошибка получения данных {symbol}:", e)
        return None


def load_extra_symbols():
    """Список USDT-перпетуалов Binance Futures, отсортированный по объёму торгов за 24ч."""
    try:
        info = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=15).json()
        tradable = {
            x["symbol"] for x in info["symbols"]
            if x.get("status") == "TRADING"
            and x.get("contractType") == "PERPETUAL"
            and x.get("quoteAsset") == "USDT"
        }
        tickers = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=15).json()
        ranked = sorted(
            (t for t in tickers if t["symbol"] in tradable),
            key=lambda t: float(t["quoteVolume"]),
            reverse=True,
        )
        return [t["symbol"] for t in ranked]
    except Exception as e:
        print("Ошибка загрузки списка пар:", e)
        return []


def build_symbols():
    """Ваш список + дополнительные самые ликвидные пары (без дублей)."""
    symbols = list(SYMBOLS)
    if AUTO_ADD_PAIRS:
        added = 0
        for sym in load_extra_symbols():
            if sym not in symbols:
                symbols.append(sym)
                added += 1
                if added >= EXTRA_PAIRS_COUNT:
                    break
    return symbols


def closed_only(df):
    """Оставляет только закрытые свечи: последняя (ещё формирующаяся) отбрасывается."""
    if df is None or df.empty:
        return df
    now_ms = int(time.time() * 1000)
    if int(df["close_time"].iloc[-1]) > now_ms:
        return df.iloc[:-1]
    return df


def calculate_ad_score(df, length=10):
    """Advance/Decline Ratio по свечам (как индикатор AD 10 на Coinglass):
    число растущих свечей / число падающих свечей за последние 10 свечей.
    Возвращает (ratio, up, down) или None, если данных нет."""
    if df is None or len(df) < length:
        return None
    last = df.tail(length)
    up = int((last["close"] > last["open"]).sum())
    down = int((last["close"] < last["open"]).sum())
    if down == 0:
        ratio = float("inf") if up > 0 else 0.0
    else:
        ratio = round(up / down, 2)
    return ratio, up, down


def fmt_score(result):
    ratio, up, down = result
    shown = "∞" if ratio == float("inf") else ratio
    return f"AD {shown} ({up} вверх / {down} вниз)"


# === МОНИТОРИНГ (работает в фоновом потоке) ===
def wait_for_candle_close():
    """Ждёт закрытия ближайшей 3-минутной свечи (+ небольшая задержка)."""
    now = time.time()
    next_close = (now // CHECK_INTERVAL + 1) * CHECK_INTERVAL
    time.sleep(max(next_close - now, 0) + CANDLE_DELAY)


def check_symbol(symbol):
    return symbol, calculate_ad_score(closed_only(get_klines(symbol)))


def monitor_loop():
    state["symbols"] = build_symbols()
    print(f"Бот запущен. Слежу за {len(state['symbols'])} парами на 3м таймфрейме (по закрытым свечам)...")
    send_telegram(f"Бот запущен. Слежу за {len(state['symbols'])} парами")

    last_alert = {}
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

            now = time.time()
            alerts = []
            failed = 0
            for symbol, result in results:
                if result is None:
                    failed += 1
                    continue
                if result[0] >= THRESHOLD:
                    if symbol not in last_alert or now - last_alert[symbol] > COOLDOWN:
                        alerts.append(f"{symbol}: {fmt_score(result)}")
                        last_alert[symbol] = now

            if alerts:
                state["alerts_sent"] += len(alerts)
                send_long("Свеча закрылась, AD 8+:\n" + "\n".join(alerts))
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Алертов: {len(alerts)}")

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
    "/score SYMBOL - текущий score пары, например /score BTCUSDT\n"
    "/pause - остановить мониторинг\n"
    "/resume - возобновить мониторинг\n"
    "/help - эта справка"
)


def handle_command(text, chat_id):
    parts = text.split()
    cmd = parts[0].split("@")[0].lower()
    args = parts[1:]

    if cmd in ("/start", "/help"):
        send_telegram("Привет! Я слежу за A/D score по парам Binance Futures.\n\n" + HELP_TEXT, chat_id)

    elif cmd == "/status":
        status = "на паузе" if state["paused"] else "работает"
        send_telegram(
            f"Статус: {status}\n"
            f"Пар: {len(state['symbols'])}\n"
            f"Таймфрейм: {INTERVAL}\n"
            f"Порог: {THRESHOLD}\n"
            f"Последняя проверка: {state['last_check'] or 'ещё не было'}\n"
            f"Алертов отправлено: {state['alerts_sent']}",
            chat_id,
        )

    elif cmd == "/pairs":
        send_long(", ".join(state["symbols"]), chat_id)

    elif cmd == "/score":
        if not args:
            send_telegram("Укажи пару, например: /score BTCUSDT", chat_id)
            return
        symbol = args[0].upper()
        df = get_klines(symbol)
        closed = calculate_ad_score(closed_only(df))
        live = calculate_ad_score(df)
        if closed is None or live is None:
            send_telegram(f"Не удалось получить данные по {symbol}", chat_id)
            return
        send_telegram(
            f"{symbol} (порог {THRESHOLD})\n"
            f"По закрытым свечам: {fmt_score(closed)}\n"
            f"С текущей свечой (как на графике): {fmt_score(live)}",
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
