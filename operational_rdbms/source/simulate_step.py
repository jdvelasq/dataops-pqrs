from datetime import datetime

import pandas as pd


def simulate_step(data_for_ingestion_path, rdbms_path):

    rdbms = pd.read_csv(rdbms_path)
    last_date = rdbms.date.tail(1).values[0]

    data = pd.read_csv(data_for_ingestion_path)
    data = data[data.date > last_date]

    init_index = data.index[0]
    end_index = init_index
    is_first_monday = True
    for index in data.index:
        if data.day_name[index] == "Monday":
            if is_first_monday is True:
                is_first_monday = False
            else:
                break
        end_index = index

    data_to_append = data.loc[init_index:end_index, :]

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    data_to_append = data_to_append.assign(last_modified=now)

    print(data_to_append)
    print()

    rdbms = pd.concat([rdbms, data_to_append])

    rdbms.to_csv(rdbms_path, index=False)


def main():

    simulate_step(
        data_for_ingestion_path="data/rdbms/historical_letter_data.csv",
        rdbms_path="data/rdbms/letter_rdbms.csv",
    )

    simulate_step(
        data_for_ingestion_path="data/rdbms/historical_web_data.csv",
        rdbms_path="data/rdbms/web_rdbms.csv",
    )


if __name__ == "__main__":
    main()
