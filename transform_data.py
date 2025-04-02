from indicators import calculate_ema, calculate_rsi, percent_below_ema, buy_hold_or_sell
import pandas as pd


def transform_data(path_to_csv="data/historical_data.csv"):
    df = pd.read_csv(path_to_csv, index_col="date", parse_dates=True)
    df = calculate_ema(df)
    df = calculate_rsi(df)
    df = percent_below_ema(df)
    df = buy_hold_or_sell(df)
    return df
