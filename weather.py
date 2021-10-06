# ETL script for weather forecast API: tomorrow.io

import json
import csv
import urllib.request
import re
import dateutil.parser as dp

def location(lookup_tables):
    '''function creating lookup lists and dictionaries for download files naming'''
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
    return locationIDs, locations, longitude, latitude, wo_sun, wo_key, lookupDict, lookupRegion

def url_generator_weather(regions,lat,lon):
    '''function for generating urls for forecast energy url-requests from tomorrow.io API'''
    #'https://api.tomorrow.io/v4/timelines?location=49.047094,12.108713&fields=temperature&fields=temperatureApparent&fields=dewPoint&fields=humidity&fields=windSpeed&fields=windDirection&fields=windGust&fields=cloudCover&fields=weatherCode&timesteps=1h&units=metric&apikey=kMFwv41yZDt4vjeF0AoEQ1smgbwxGLYc'
    urlsWeather = []
    location = []
    base_url = 'https://api.tomorrow.io/v4/timelines?location='
    q = '&fields=temperature&fields=temperatureApparent&fields=dewPoint&fields=humidity&fields=windSpeed&fields=windDirection&fields=windGust&fields=cloudCover&fields=weatherCode&timesteps=1h&units=metric&apikey=kMFwv41yZDt4vjeF0AoEQ1smgbwxGLYc'
    for i, value in enumerate(regions):
        location.append(str(lon[i]+','+str(lat[i])))
        url = base_url + location[i] + q
        urlsWeather.append(url)
    return urlsWeather

def load_json_weather(links, lookupdict):
    '''function for parsed json for weather requests'''
    filenames = []
    parsed_list = []
    for url in links:
        response = urllib.request.urlopen(url)
        current_energy = response.read()
        text = current_energy.decode('utf-8')
        parsed = json.loads(text)
        parsed_list.append(parsed)
        # extract latitude and longitude from urls:
        lat = re.findall(r'\d*.\d*,', url) 
        lon = re.findall(r',\d*.\d*', url)
        lat = lat[0].replace(',','')
        lon = lon[0].replace(',','')
        # extract region code based on latitude and longitude value:
        region_code = [k for k, v in lookupdict.items() if (str(lat) and str(lon)) in v]
        # list of filenames based on extracted region code
        filename = region_code[0]+'_data_forecast_weather.json'
        filenames.append(filename)
        # write json files with corresponding region code in file naming:
        with open('raw_data/%s_data_forecast_weather.json' % region_code[0], 'w') as f:
            json.dump(parsed, f, ensure_ascii=False, indent=4)
    return parsed_list, filenames
    
    

def write_forecast_weather(dataWeather, files):
    '''write weather forecast output into structured csv-files'''
    f = open("weather_forecast/daily_weatherforecast.csv", 'w')
    print("timestamp,regionID,temperature_C,wind_speed_m/s,wind_direction,wind_gust,cloud_cover_pct,weathercode", file=f)
    # extract start time for weather forecast limitation of 24h:
    starttime = dataWeather[0]['data']['timelines'][0]['intervals'][0]['startTime']
    # convert starttime to unixtime:
    parsed_t = dp.parse(starttime)
    starttime = parsed_t.strftime('%s')
    # write csv file:
    for i, value in enumerate(dataWeather):
        for item in dataWeather[i]['data']['timelines'][0]['intervals']:
            time = item['startTime']
            temperature = item['values']['temperature']
            wind_speed = item['values']['windSpeed']
            wind_direction = item['values']['windDirection']
            wind_gust = item['values']['windGust']
            cloud_cover = item['values']['cloudCover']
            weathercode = item['values']['weatherCode']
            parsed_t = dp.parse(time)
            timestamp = parsed_t.strftime('%s')
            # include regionID extracted from file name
            regionID = re.findall(r'\d*', files[i])
            regionID = regionID[0]

            expected_output = f"{timestamp},{regionID},{temperature},{wind_speed},{wind_direction},{wind_gust},{cloud_cover},{weathercode}"
            
            if int(timestamp) < int(starttime) + 24 * 60 * 60: #time + 24h for 24h forecast
                print(expected_output, file=f)


    f.close()

locationIDs, locations, longitude, latitude, wo_sun, wo_key, lookupDict, lookupRegion = location('lookup_table/locations.csv')
urlsWeather = url_generator_weather(locationIDs, longitude, latitude)
parsedWeather, weather_json = load_json_weather(urlsWeather, lookupDict)
write_forecast_weather(parsedWeather, weather_json)

