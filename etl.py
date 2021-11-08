# a data pipeline for completion of ds 3002 under jason williamson
# apikey = 35a3be8d1b3661a1c28b545bb6ee34de
import requests
import time
import csv
import argparse


def to_fahrenheit(kelvin):
    """
    kelvin to fahrenheit conversion

    :param kelvin: temperature in kelvin
    :return: temperature in fahrenheit
    """
    fah = (kelvin - 273.15) * (9 / 5) + 32
    return round(fah, 2)


def transform(weather_json, filename):
    """
    processes, extracts, and loads weather_json to filename

    extracted columns –>
        1. city id
        2. city name
        3. country code
        4. local date and time
        5. temperature at date and time
        6. temperature (feels like) at date and time
        7. minimum temperature
        8. maximum temperature

    :param weather_json: json containing unprocessed weather data
    :param filename: csv file to write to
    :return: number of columns injected, number of columns processed
    """

    # iterates through json to return number of columns read (non-static variable, city-dependent)
    read_cols = 0
    for i in weather_json.keys():
        if isinstance(weather_json[i], int) or isinstance(weather_json[i], str):
            read_cols += 1
        if isinstance(weather_json[i], dict):
            read_cols += len(weather_json[i])
        if isinstance(weather_json[i], list):
            for j in weather_json[i]:
                read_cols += len(j)

    # extract raw column data for 8 columns –>
    # date, city name, temperature, feels like, min temp, max temp, country code, city id
    dt = weather_json['dt']
    city_name = weather_json['name']
    temp_kelvin = weather_json['main']['temp']
    feels_like_k = weather_json['main']['feels_like']
    min_k = weather_json['main']['temp_min']
    max_k = weather_json['main']['temp_max']
    country_code = weather_json['sys']['country']
    city_id = weather_json['sys']['id']

    # convert kelvin to fahrenheit for all temperature cols
    temp_f = to_fahrenheit(temp_kelvin)
    feels_like_f = to_fahrenheit(feels_like_k)
    min_f = to_fahrenheit(min_k)
    max_f = to_fahrenheit(max_k)

    # transform epoch time to local time
    dt_readable = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(dt))

    # format data for loading processes
    temp_arr = [city_id, city_name, country_code, dt_readable, temp_f, feels_like_f, min_f, max_f]
    csv_file = filename

    # open csv file
    with open(csv_file, 'a') as csv_file:
        writer = csv.writer(csv_file)

        # if file doesn't exist, create and write header
        if csv_file.tell() == 0:
            col_names = ['City ID', 'City Name', 'Country Code', 'Time', 'Current Temperature (F)', 'Feels Like (F)',
                         'Min Temp (F)', 'Max Temp (F)']
            writer.writerow(col_names)

        # write data to file
        writer.writerow(temp_arr)

    return len(temp_arr), read_cols


def main():
    """
    this data pipeline takes in a city name and calls OpenWeather's data api to return a JSON
    of current weather data; the pipeline extracts 8 columns and writes to an csv

    extracted columns –>
    1. city id
    2. city name
    3. country code
    4. local date and time
    5. temperature at date and time
    6. temperature (feels like) at date and time
    7. minimum temperature
    8. maximum temperature

    -c, --city = city name to pull data on (default = Charlottesville)
    -f, --freq = how often to make an api call when pulling 1+ records (default = 300)
    -n = number of api calls to make for 1 city (default = 1)
    -csv = csv file to write data to (default = weather_data.csv)
    """

    # add command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--city', dest='city_name', default='Charlottesville',
                        help='city name to pull weather data for')
    parser.add_argument('-f', '--freq', dest='frequency', default=300,
                        help='when running multiple calls, number of seconds to wait between calls')
    parser.add_argument('-n', dest='n', default=1, help='number of calls to make for 1 city')
    parser.add_argument('-csv', dest='csvfile', default='weather_data.csv', help='csv file to write to')
    args = parser.parse_args()

    # save api headers
    api_key = '35a3be8d1b3661a1c28b545bb6ee34de'
    city_name = args.city_name
    url = 'http://api.openweathermap.org/data/2.5/weather?q=' + city_name + '&appid=' + api_key

    # type-check and identify global params from args (frequency, n, csv)
    try:
        freq = int(args.frequency)
    except:
        print('invalid frequency argument')
        exit()
    try:
        n = int(args.n)
        global_n = n
    except:
        print('invalid n argument')
        exit()
    try:
        csvfile = args.csvfile
        is_csv = csvfile[len(csvfile) - 4:] == '.csv'
        if not is_csv:
            raise
    except:
        print('invalid csv argument')
        exit()

    # run through etl pipeline for each call
    while n > 0:
        # make api call and return json
        response = requests.request("GET", url)
        weather_json = response.json()

        # check if weather_json returns an error code
        if weather_json['cod'] != 200:
            print('unknown city name')
            exit()

        # print call number to command line
        print('CALL ' + str(global_n - (n - 1)))

        # transform weather_json and write to csv
        n_cols, read_cols = transform(weather_json, csvfile)

        # summarize data injection
        if n == 1:
            print()
            print('number of records PROCESSED: ' + str(global_n))
            print('number of columns READ: ' + str(read_cols))
            print('number of columns WRITTEN: ' + str(n_cols))
            exit()
        n -= 1

        # sleep until next call
        time.sleep(freq)


if __name__ == '__main__':
    main()
