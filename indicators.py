import pandas as pd


def calculate_ema(df: pd.DataFrame, span: int = 20):
    df["ema_20"] = df["close"].ewm(span=span, adjust=False).mean()
    return df
