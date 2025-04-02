import pandas as pd
import config


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
    in_position = False
    entry_price = 0
    actions = []
    profits = []

    for i, row in df.iterrows():
        if not in_position:
            if (
                row["percent_below_ema"] < config.percent_below_ema_to_buy
                and row["rsi"] < config.rsi_to_buy
            ):
                actions.append("buy")
                entry_price = row["close"]
                in_position = True
                profits.append(0)
            else:
                actions.append("hold")
                profits.append(0)
        else:
            if (
                row["percent_below_ema"] > config.percent_below_ema_to_sell
                and row["rsi"] > config.rsi_to_sell
            ):
                actions.append("sell")
                profit = row["close"] - entry_price
                profits.append(profit)
                in_position = False
                entry_price = 0
            else:
                actions.append("hold")
                profits.append(0)

    df["action"] = actions
    df["strategy_returns"] = profits
    return df
