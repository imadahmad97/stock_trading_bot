import pandas as pd


def calculate_ema(df: pd.DataFrame, span: int = 20):
    df["ema_20"] = df["close"].ewm(span=span, adjust=False).mean()
    return df


def calculate_rsi(df: pd.DataFrame, period: int = 14):
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))

    return df
