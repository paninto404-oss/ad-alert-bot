import os
import time
import threading
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

API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# Общее состояние между мониторингом и командами
state = {"paused": False, "last_check": None, "alerts_sent": 0}


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
def monitor_loop():
    print("Бот запущен. Слежу за парами на 3м таймфрейме...")
    send_telegram("Бот запущен и начал мониторинг")

    last_alert = {}

    while True:
        try:
            if state["paused"]:
                time.sleep(5)
                continue

            for symbol in SYMBOLS:
                if state["paused"]:
                    break

                df = get_klines(symbol)
                if df is None:
                    continue

                result = calculate_ad_score(df)
                if result is None:
                    continue
                score = result[0]

                if score >= THRESHOLD:
                    now = time.time()
                    if symbol not in last_alert or now - last_alert[symbol] > 900:
                        send_telegram(f"{symbol}: {fmt_score(result)}")
                        state["alerts_sent"] += 1
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] Алерт: {symbol} | {fmt_score(result)}")
                        last_alert[symbol] = now

                time.sleep(0.3)

            state["last_check"] = datetime.now().strftime("%H:%M:%S")
            print(f"[{state['last_check']}] Проверка завершена. Жду 3 минуты...")
            time.sleep(CHECK_INTERVAL)

        except Exception as e:
            print("Ошибка в основном цикле:", e)
            time.sleep(60)


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
            f"Пар: {len(SYMBOLS)}\n"
            f"Таймфрейм: {INTERVAL}\n"
            f"Порог: {THRESHOLD}\n"
            f"Последняя проверка: {state['last_check'] or 'ещё не было'}\n"
            f"Алертов отправлено: {state['alerts_sent']}",
            chat_id,
        )

    elif cmd == "/pairs":
        send_telegram(", ".join(SYMBOLS), chat_id)

    elif cmd == "/score":
        if not args:
            send_telegram("Укажи пару, например: /score BTCUSDT", chat_id)
            return
        symbol = args[0].upper()
        result = calculate_ad_score(get_klines(symbol))
        if result is None:
            send_telegram(f"Не удалось получить данные по {symbol}", chat_id)
            return
        send_telegram(f"{symbol}: {fmt_score(result)} (порог {THRESHOLD})", chat_id)

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
