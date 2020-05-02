import os
import requests
import pickle
from pathlib import Path
from datetime import datetime, timedelta

# constants
data_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master\
/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_\
global.csv"
data_folder_name = 'data'
csv_file_name = 'data.csv'
metadata_file_name = 'meta.pickle'
stale_days = 0.5

data_file_path = os.path.join(data_folder_name, csv_file_name)
metadata_file_path = os.path.join(data_folder_name, metadata_file_name)
stale_timedelta = timedelta(days=stale_days)


def create_initial_data():
    Path(data_folder_name).mkdir(parents=True, exist_ok=True)


def get_metadata():
    if not os.path.exists(metadata_file_path):
        return {}
    with open(metadata_file_path, 'rb') as metadata_file:
        return pickle.load(metadata_file)


def save_metadata(metadata):
    with open(metadata_file_path, 'wb') as metadata_file:
        pickle.dump(metadata, metadata_file)


def update_metadata(new_metadata):
    metadata = get_metadata()
    save_metadata({**metadata, **new_metadata})


def update_data():
    create_initial_data()

    # If data is stale
    metadata = get_metadata()
    date = metadata.get('date')
    now = datetime.now()
    if date and (date + stale_timedelta > now):
        print('Data up to date, skipping download.')
        return

    print('Data stale or unexistent, downloading...')
    # Get and save data
    request_result = requests.get(data_url, allow_redirects=True)

    open(data_file_path,
         'wb').write(request_result.content)
    print('data updated')

    # Update data staleness
    update_metadata({'date': now})


def show_charts():
    # show charts
    pass


def main():
    update_data()
    show_charts()


if __name__ == "__main__":
    main()