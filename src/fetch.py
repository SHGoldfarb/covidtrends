import os
import requests
import pickle
from pathlib import Path
from datetime import datetime, timedelta

data_folder_name = 'data'
stale_days = 0.1

stale_timedelta = timedelta(days=stale_days)


def matadata_path_of_file_path(file_path):
    return file_path + '.meta.pickle'


def ensure_data_folder_exists():
    Path(data_folder_name).mkdir(parents=True, exist_ok=True)


def file_path_of(file_name):
    ensure_data_folder_exists()
    return os.path.join(data_folder_name, file_name)


def get_metadata(file_path):
    if not os.path.exists(file_path):
        return {}
    with open(file_path, 'rb') as metadata_file:
        return pickle.load(metadata_file)


def save_metadata(metadata, file_path):
    with open(file_path, 'wb') as metadata_file:
        pickle.dump(metadata, metadata_file)


def update_metadata(new_metadata, file_path):
    metadata = get_metadata(file_path)
    save_metadata({**metadata, **new_metadata}, file_path)


def update_file(url, save_path):
    metadata_path = matadata_path_of_file_path(save_path)
    metadata = get_metadata(metadata_path)
    date = metadata.get('date')
    now = datetime.now()

    # If data is stale
    if date and (date + stale_timedelta > now):
        print('Data up to date, skipping download.')
        return

    print('Data stale or unexistent, downloading...')
    # Get and save data
    request_result = requests.get(
        url, allow_redirects=True)

    open(save_path, 'wb').write(request_result.content)
    print('data updated')

    # Update data staleness
    update_metadata({'date': now}, metadata_path)
