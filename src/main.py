import os
import requests
import pickle
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import ticker

# constants
cases_constants = {
    "data_url": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master\
/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_\
global.csv",
    "csv_file_name": "time_series_covid19_confirmed_global.csv",
    "metadata_file_name": 'cases_meta.pickle'
}

deaths_constants = {
    "data_url": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master\
/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_\
global.csv",
    "csv_file_name": "time_series_covid19_deaths_global.csv",
    "metadata_file_name": 'deaths_meta.pickle'
}

data_folder_name = 'data'
population_file_path = 'static_data/population.csv'
stale_days = 0.1
days_on_last_sum = 7

stale_timedelta = timedelta(days=stale_days)


def in_data_folder(file_name):
    return os.path.join(data_folder_name, file_name)


def create_data_folder():
    Path(data_folder_name).mkdir(parents=True, exist_ok=True)


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


def update_data():

    create_data_folder()
    for constants in [cases_constants, deaths_constants]:
        metadata_file_path = in_data_folder(constants["metadata_file_name"])
        csv_file_path = in_data_folder(constants["csv_file_name"])

        # If data is stale
        metadata = get_metadata(metadata_file_path)
        date = metadata.get('date')
        now = datetime.now()
        if date and (date + stale_timedelta > now):
            print('Data up to date, skipping download.')
            continue

        print('Data stale or unexistent, downloading...')
        # Get and save data
        request_result = requests.get(
            constants["data_url"], allow_redirects=True)

        open(csv_file_path,
             'wb').write(request_result.content)
        print('data updated')

        # Update data staleness
        update_metadata({'date': now}, metadata_file_path)


def sanitize_data(data):
    # Group countries
    data = data.groupby(by=['Country/Region']).sum()

    # Remove unwanted "countries"
    data = data.drop(
        ['West Bank and Gaza', 'Diamond Princess', 'Kosovo', 'MS Zaandam']
    )

    # Remove lat long
    data = data.iloc[:, 2:]

    return data


def get_data(file_path):
    update_data()
    data = pd.read_csv(file_path)
    return data


def get_population():
    return pd.read_csv(population_file_path).set_index('Country')


def show_charts(data):

    data = sanitize_data(data)

    # Create new dataframe with last cases

    def new_last_days(row):
        cases = row[1]

        def get_new(i):
            return cases[i] - cases[max(0, i - days_on_last_sum)]
        return map(get_new, range(len(cases)))

    data_new = pd.DataFrame([new_last_days(row)
                             for row in data.iterrows()],
                            data.index,
                            data.columns)

    # Transform dataframes: divide by population

    population_data = get_population()
    population_data = population_data

    # Set data to per million people
    millionth = 0.000001

    def ponderate_by_population(row):
        return row.div(
            population_data.loc[row.name]['Population']
        ).div(millionth)

    data = data.apply(ponderate_by_population, axis=1)
    data_new = data_new.apply(ponderate_by_population, axis=1)

    # Plot both dataframes
    _, ax = plt.subplots()
    for i in range(len(data.index)):
        if data.iloc[i].name in ['Chile', 'Argentina', 'Germany',
                                 'United Kingdom', 'Ecuador', 'US',
                                 'Brazil', 'Italy', 'Spain', 'Australia',
                                 ]:
            ax.loglog(data.iloc[i, :],
                      data_new.iloc[i, :], label=data.index[i])
    ax.get_yaxis().set_major_formatter(
        ticker.FuncFormatter(lambda x, p: str(round(x, 4))))
    ax.get_xaxis().set_major_formatter(
        ticker.FuncFormatter(lambda x, p: str(round(x, 4))))
    ax.get_yaxis().set_minor_formatter(
        ticker.FuncFormatter(lambda x, p: str(round(x, 4))))
    ax.get_xaxis().set_minor_formatter(
        ticker.FuncFormatter(lambda x, p: str(round(x, 4))))
    ax.legend()
    plt.show()


def show_cases_chart():
    file_path = in_data_folder(cases_constants["csv_file_name"])

    data = get_data(file_path)

    show_charts(data)


def show_deaths_chart():
    file_path = in_data_folder(deaths_constants["csv_file_name"])

    data = get_data(file_path)

    show_charts(data)


def main():
    show_deaths_chart()


if __name__ == "__main__":
    main()
