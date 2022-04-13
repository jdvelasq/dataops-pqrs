import os
from datetime import datetime

import pandas as pd


def main():
    data = load_historical_data()
    data = transform_data(data)
    historical_letter_data = make_historical_dataset(data, "letter")
    historical_web_data = make_historical_dataset(data, "web")
    save_historical_data(historical_letter_data, "historical_letter_data.csv")
    save_historical_data(historical_web_data, "historical_web_data.csv")
    make_rdbms(historical_letter_data, "letter_rdbms.csv")
    make_rdbms(historical_web_data, "web_rdbms.csv")


def load_historical_data():
    module_path = os.path.dirname(__file__)
    filename = os.path.join(module_path, "../data/external/historical_data.csv")
    if not os.path.exists(filename):
        raise FileNotFoundError(f"File {filename} not found")
    data = pd.read_csv(filename, sep=";")
    return data


def transform_data(data):
    data = data.copy()
    data["date"] = pd.to_datetime(data.date, format="%d/%m/%Y")
    data["day_name"] = data.date.dt.day_name()
    data = data.fillna(0.0)
    data["letter"] = data["letter"].astype(float)
    return data


def make_historical_dataset(data, channel):

    channel_data = data[
        [
            "date",
            channel,
            "day_name",
        ]
    ].copy()

    neg_data = channel_data.iloc[652:, :].sample(50).copy()
    neg_data[channel] = neg_data[channel].map(lambda x: -x).sample(frac=1)
    null_data = channel_data.iloc[652:, :].sample(35).copy()
    null_data.loc[:, channel] = pd.NA
    channel_data = pd.concat([channel_data, neg_data, null_data])
    channel_data = channel_data.sort_values(by="date")
    channel_data = channel_data.reset_index(drop=True)
    return channel_data


def save_historical_data(data, filename):
    module_path = os.path.dirname(__file__)
    filename = os.path.join(module_path, "../data/rdbms/", filename)
    data.to_csv(filename, index=False)


def make_rdbms(historical_data, filename):

    module_path = os.path.dirname(__file__)
    filename = os.path.join(module_path, "../data/rdbms/", filename)

    historical_data = historical_data[historical_data.index < 533]

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    historical_data = historical_data.assign(last_modified=now)
    historical_data.to_csv(filename, index=False)


if __name__ == "__main__":
    main()
