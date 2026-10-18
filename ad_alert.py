import requests
import time
import pandas as pd
from datetime import datetime

# === НАСТРОЙКИ ===
TELEGRAM_TOKEN = "8836581823:AAFHYfHJK7GNh2d3hZKVSbxx7DShYKFl6jo"
CHAT_ID = "347263656"

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
CHECK_INTERVAL = 180  # проверять каждые 3 минуты
THRESHOLD = 8.0

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print("Ошибка отправки в Telegram:", e)

def get_klines(symbol, interval="3m", limit=30):
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"
        ])
        df["close"] = df["close"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["volume"] = df["volume"].astype(float)
        df["open"] = df["open"].astype(float)
        return df
    except Exception as e:
        print(f"Ошибка получения данных {symbol}:", e)
        return None

def calculate_ad_score(df):
    """Похожий на AD индикатор, приведённый к шкале 0-10"""
    if df is None or len(df) < 15:
        return 0

    # Money Flow Multiplier
    mf = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / (df["high"] - df["low"] + 1e-8)
    mf = mf.fillna(0)

    # Money Flow Volume
    mfv = mf * df["volume"]

    # Кумулятивный AD
    ad = mfv.cumsum()

    # Берём последние 10 значений и нормализуем
    recent = ad.tail(10)
    if recent.max() == recent.min():
        return 5.0

    # Нормализация в диапазон 0-10
    score = ((recent.iloc[-1] - recent.min()) / (recent.max() - recent.min())) * 10
    return round(score, 2)

def main():
    print("Бот запущен. Слежу за парами на 3м таймфрейме...")
    send_telegram("Бот запущен и начал мониторинг")

    last_alert = {}  # чтобы не спамить одной и той же парой

    while True:
        try:
            for symbol in SYMBOLS:
                df = get_klines(symbol)
                if df is None:
                    continue

                score = calculate_ad_score(df)

                # Алерт только если >= 8 и не алертили недавно
                if score >= THRESHOLD:
                    now = time.time()
                    if symbol not in last_alert or now - last_alert[symbol] > 900:  # не чаще раза в 15 минут
                        message = f"{symbol}"
                        send_telegram(message)
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] Алерт: {symbol} | Score: {score}")
                        last_alert[symbol] = now

                time.sleep(0.3)  # небольшая пауза между запросами

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Проверка завершена. Жду 3 минуты...")
            time.sleep(CHECK_INTERVAL)

        except Exception as e:
            print("Ошибка в основном цикле:", e)
            time.sleep(60)

if __name__ == "__main__":
    main()
