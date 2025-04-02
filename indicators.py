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


def percent_below_ema(df: pd.DataFrame) -> pd.DataFrame:
    df["percent_below_ema"] = (df["close"] - df["ema_20"]) / df["ema_20"] * 100
    return df


def buy_hold_or_sell(df: pd.DataFrame) -> pd.DataFrame:
    in_position = False  # We're not in a trade at the start
    actions = []

    for i, row in df.iterrows():
        if not in_position:
            if row["percent_below_ema"] < -0.3 and row["rsi"] < 30:
                actions.append("buy")
                in_position = True
            else:
                actions.append("hold")
        else:
            if row["percent_below_ema"] > 0.3 and row["rsi"] > 70:
                actions.append("sell")
                in_position = False
            else:
                actions.append("hold")

    df["action"] = actions
    return df
