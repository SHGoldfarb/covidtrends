import os
import requests
import pickle
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import ticker

# constants
confirmed_data_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master\
/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_\
global.csv"

deaths_data_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master\
/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_\
global.csv"

data_folder_name = 'data'
population_file_path = 'static_data/population.csv'
stale_days = 0.1
days_on_last_sum = 7

stale_timedelta = timedelta(days=stale_days)


def csv_file_name(data_url):
    return data_url.split('/')[-1]


def metadata_file_name(data_url):
    return csv_file_name(data_url) + '.meta.pickle'


def in_data_folder(file_name):
    return os.path.join(data_folder_name, file_name)


def csv_file_path(data_url):
    return in_data_folder(csv_file_name(data_url))


def metadata_file_path(data_url):
    return in_data_folder(metadata_file_name(data_url))


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


def update_data(data_url):
    metadata = get_metadata(metadata_file_path(data_url))
    date = metadata.get('date')
    now = datetime.now()

    # If data is stale
    if date and (date + stale_timedelta > now):
        print('Data up to date, skipping download.')
        return

    print('Data stale or unexistent, downloading...')
    # Get and save data
    request_result = requests.get(
        data_url, allow_redirects=True)

    create_data_folder()

    open(csv_file_path(data_url),
         'wb').write(request_result.content)
    print('data updated')

    # Update data staleness
    update_metadata({'date': now}, metadata_file_path(data_url))


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


def get_data(data_url):
    update_data(data_url)
    data = pd.read_csv(csv_file_path(data_url))
    return data


def get_population():
    return pd.read_csv(population_file_path).set_index('Country')


def last_days(data, n_days):
    # Create new dataframe with last cases

    def new_last_days(row):
        cases = row[1]

        def get_new(i):
            return cases[i] - cases[max(0, i - n_days)]
        return map(get_new, range(len(cases)))

    data_last = pd.DataFrame([new_last_days(row)
                              for row in data.iterrows()],
                             data.index,
                             data.columns)

    return data_last


def ponderate_dataframe_by_population(data):
    # Transform dataframes: divide by population

    population_data = get_population()
    population_data = population_data

    # Set data to per million people
    millionth = 0.000001

    def ponderate_row_by_population(row):
        return row.div(
            population_data.loc[row.name]['Population']
        ).div(millionth)

    return data.apply(ponderate_row_by_population, axis=1)


def select_countries(data, countries):

    return data.drop(
        set(data.index) - set(countries)
    )


def plot_dataframes_against_eachother(data1, data2):
    # Plot both dataframes
    _, ax = plt.subplots()
    for i in range(len(data1.index)):

        ax.loglog(data1.iloc[i, :],
                  data2.iloc[i, :], label=data1.index[i])
    ax.get_yaxis().set_major_formatter(
        ticker.FuncFormatter(lambda x, p: str(round(x, 4))))
    ax.get_xaxis().set_major_formatter(
        ticker.FuncFormatter(lambda x, p: str(round(x, 4))))
    ax.get_yaxis().set_minor_formatter(
        ticker.FuncFormatter(lambda x, p: str(round(x, 4))))
    ax.get_xaxis().set_minor_formatter(
        ticker.FuncFormatter(lambda x, p: str(round(x, 4))))
    return ax


def main():
    confirmed_data = get_data(confirmed_data_url)
    deaths_data = get_data(deaths_data_url)

    confirmed_data = sanitize_data(confirmed_data)
    deaths_data = sanitize_data(deaths_data)

    confirmed_last_days_data = last_days(confirmed_data, days_on_last_sum)
    deaths_last_days_data = last_days(deaths_data, days_on_last_sum)

    confirmed_data = ponderate_dataframe_by_population(confirmed_data)
    deaths_data = ponderate_dataframe_by_population(deaths_data)

    confirmed_last_days_data = ponderate_dataframe_by_population(
        confirmed_last_days_data)
    deaths_last_days_data = ponderate_dataframe_by_population(
        deaths_last_days_data)

    countries = ['Chile', 'Germany', 'Spain', 'Brazil',
                 'US', 'Italy', 'United Kingdom']

    confirmed_data = select_countries(confirmed_data, countries)
    deaths_data = select_countries(deaths_data, countries)

    confirmed_last_days_data = select_countries(
        confirmed_last_days_data, countries)
    deaths_last_days_data = select_countries(deaths_last_days_data, countries)

    ax = plot_dataframes_against_eachother(
        confirmed_data, confirmed_last_days_data)
    ax.set_ylabel('Weekly per million people')
    ax.set_xlabel('Total per million people')
    ax.set_title('Confirmed cases per country')
    ax.legend()
    plt.show()

    ax = plot_dataframes_against_eachother(deaths_data, deaths_last_days_data)
    ax.set_ylabel('Weekly per million people')
    ax.set_xlabel('Total per million people')
    ax.set_title('Deaths per country')
    ax.legend()
    plt.show()


if __name__ == "__main__":
    main()
