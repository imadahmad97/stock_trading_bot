from indicators import calculate_ema
import pandas as pd


def transform_data(path_to_csv):
    df = pd.read_csv("data/historical_data.csv", index_col="date", parse_dates=True)
    df = calculate_ema(df)
    return df
