import fetch
import pandas as pd

csv_url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"


def csv_file_name_from_url(csv_url):
    return csv_url.split('/')[-1]


def csv_file_path(csv_url):
    return fetch.file_path_of(csv_file_name_from_url(csv_url))


def read_csv_data(csv_url):
    return pd.read_csv(csv_file_path(csv_url))


def get_updated_csv_data(csv_url):
    fetch.update_file(csv_url, csv_file_path(csv_url))

    return read_csv_data(csv_url)


def start():
    data = get_updated_csv_data(csv_url)
    print(data.columns)


if __name__ == "__main__":
    start()
