import json
import csv
import urllib.request
import re


def location(lookup_tables):
    locationIDs = []
    locations = []
    longitude = []
    latitude = []
    wo_sun = []
    wo_key = []
    lookupDict = {}
    filename = open(lookup_tables, 'r')
    file = csv.DictReader(filename)
    for col in file:
        locationIDs.append(col['regionID'])
        locations.append(col['region'])
        latitude.append(col['lat'])
        longitude.append(col['lon'])
        wo_sun.append(col['weatheronline_sun'])
        wo_key.append(col['weatheronline_key'])
        if col['regionID'] not in lookupDict:
            lookupDict[col['regionID']] = [col['lat'],col['lon']]
    lookupRegion = dict(zip(locationIDs,locations))
    print(lookupDict)
    return locationIDs, locations, longitude, latitude, wo_sun, wo_key, lookupDict, lookupRegion

def urlGeneratorWeather(regions,lat,lon):
    #'https://api.tomorrow.io/v4/timelines?location=49.047094,12.108713&fields=temperature&fields=temperatureApparent&fields=dewPoint&fields=humidity&fields=windSpeed&fields=windDirection&fields=windGust&fields=cloudCover&fields=weatherCode&timesteps=1h&units=metric&apikey=kMFwv41yZDt4vjeF0AoEQ1smgbwxGLYc'
    urlsWeather = []
    location = []
    base_url = 'https://api.tomorrow.io/v4/timelines?location='
    q = '&fields=temperature&fields=temperatureApparent&fields=dewPoint&fields=humidity&fields=windSpeed&fields=windDirection&fields=windGust&fields=cloudCover&fields=weatherCode&timesteps=1h&units=metric&apikey=kMFwv41yZDt4vjeF0AoEQ1smgbwxGLYc'
    for i, value in enumerate(regions):
        location.append(str(lat[i]+','+str(lon[i])))
        url = base_url + location[i] + q
        urlsWeather.append(url)
    return urlsWeather

def loadJsonWeather(links, lookupdict):
    filepaths = []
    parsed_list = []
    for url in links:
        response = urllib.request.urlopen(url)
        current_energy = response.read()
        text = current_energy.decode('utf-8')
        parsed = json.loads(text)
        parsed_list.append(parsed)
        lat = re.findall(r'\d*.\d*,', url)
        lon = re.findall(r',\d*.\d*', url)
        lat = lat[0].replace(',','')
        lon = lon[0].replace(',','')
        region_code = [k for k, v in lookupdict.items() if (str(lat) and str(lon)) in v]
        filepath = str(region_code)+'_data_forecast_weather.json'
        filepaths.append(filepath)
        with open('raw_data/%s_data_forecast_weather.json' % region_code, 'w') as f:
            json.dump(parsed, f, ensure_ascii=False, indent=4)
    return parsed_list, filepaths

def writeForecastWeather(dataWeather):

    f = open("monitor_data/daily_weatherforecast.csv", 'w')

    print("timestamp,temperature,wind_speed,wind_direction,wind_gust,cloud_cover,weathercode", file=f)

    for item in dataWeather['data']['timelines'][0]['intervals']:
        time = item['startTime']
        temperature = item['values']['temperature']
        #temperature_apparent = item['values']['temperatureApparent']
        #dewpoint = item['values']['dewPoint']
        #humidity = item['values']['humidity']
        wind_speed = item['values']['windSpeed']
        wind_direction = item['values']['windDirection']
        wind_gust = item['values']['windGust']
        #solar_ghi = item['values']['solarGHI']
        cloud_cover = item['values']['cloudCover']
        weathercode = item['values']['weatherCode']

        parsed_t = dp.parse(time)
        timestamp = parsed_t.strftime('%s')

        expected_output = f"{timestamp},{temperature},{wind_speed},{wind_direction},{wind_gust},{cloud_cover},{weathercode}"

        print(expected_output, file=f)


    f.close()

locationIDs, locations, longitude, latitude, wo_sun, wo_key, lookupDict, lookupRegion = location('lookup_table/locations.csv')
urlsWeather = urlGeneratorWeather(locationIDs, longitude, latitude)
parsedWeather, energy_json = loadJsonWeather(urlsWeather, lookupDict)
writeForecastWeather(parsedWeather)

