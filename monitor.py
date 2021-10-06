import json
import csv
import urllib.request
import re

def location(lookup_tables):
    '''function creating lookup lists and dictionaries for download files naming'''
    locationIDs = []
    locations = []
    filename = open(lookup_tables, 'r')
    file = csv.DictReader(filename)
    for col in file:
        locationIDs.append(col['regionID'])
        locations.append(col['region'])
    lookupDict = dict(zip(locationIDs,locations))
    return locationIDs, locations, lookupDict

def url_generator_energy(regions):
    '''function for generating urls for current energy url-requests from energy monitor'''
    urlsEnergy = []
    base_url = 'https://api-energiemonitor.eon.com/meter-data?regionCode='
    for region in regions:
        url = base_url + region
        urlsEnergy.append(url)
    return urlsEnergy

def url_generator_weather(regions):
    '''function for generating urls for current weather url-requests from energy monitor'''
    urlsWeather = []
    base_url = 'https://api-energiemonitor.eon.com/weather-data?regionCode='
    for region in regions:
        url = base_url + region
        urlsWeather.append(url)
    return urlsWeather

def load_json_energy(links):
    '''function for parsed json for energy requests'''
    filenames = []
    parsed_list = []
    for url in links:
        response = urllib.request.urlopen(url)
        current_energy = response.read()
        text = current_energy.decode('utf-8')
        parsed = json.loads(text)
        parsed_list.append(parsed)
        # extract region code from url:
        region_code = re.findall(r'\d+', url)
        region_code=region_code[0]
        # list of filenames based on extracted region code
        filename = str(region_code)+'_data_current_energy.json'
        filenames.append(filename)
        # write json files with corresponding region code in file naming:
        with open('raw_data/%s_data_current_energy.json' % region_code, 'w') as f:
            json.dump(parsed, f, ensure_ascii=False, indent=4)
    return parsed_list, filenames

def load_json_weather(links):
    '''function for parsed json for weather requests'''
    filenames = []
    parsed_list = []
    for url in links:
        response = urllib.request.urlopen(url)
        current_energy = response.read()
        text = current_energy.decode('utf-8')
        parsed = json.loads(text)
        parsed_list.append(parsed)
        # extract region code from url:
        region_code = re.findall(r'\d+', url)
        region_code=region_code[0]
        filename = str(region_code)+'_data_current_weather.json'
        filenames.append(filename)
        # write json files with corresponding region code in file naming:
        with open('raw_data/%s_data_current_weather.json' % region_code, 'w') as f:
            json.dump(parsed, f, ensure_ascii=False, indent=4)
    return parsed_list, filenames

def write_current_energy_and_weather(dataEnergy, dataWeather):
    '''write current energy and data output into structured csv-files'''
    f = open("monitor_data/live_energydata.csv", 'w')
    print("timestamp,regionID,windPower_kWh,solarPower_kWh,windSpeed_m/s,cloudCover_pct,temperature_C", file=f)
    # create a dictionary to merge data out of two sources into one file
    dicti = {'energy' : dataEnergy, 'weather' : dataWeather}
    for i, value in enumerate(dicti['energy']):
        item1 = dicti['energy'][i]
        item2 = dicti['weather'][i]
        timestamp = item1['timestamp']['start']
        regionID = item1['regionCode']
        windPower = float(item1['feedIn']['list'][4]['usage'])
        solarPower = float(item1['feedIn']['list'][1]['usage'])
        temperature = item2['weather']['temperature']
        windSpeed = item2['weather']['windSpeed']
        cloudCover = float(item2['weather']['cloudCover'])*100

        expected_output = f"{timestamp},{regionID},{windPower},{solarPower},{windSpeed},{cloudCover},{temperature}"

        print(expected_output, file=f)

    f.close()

locationIDs, locations, lookupDict = location('lookup_table/locations.csv')
urlsEnergy = url_generator_energy(locationIDs)
urlsWeather = url_generator_weather(locationIDs)
parsedEnergy, energy_json = load_json_energy(urlsEnergy)
parsedWeather, weather_json = load_json_weather(urlsWeather)
currentMonitorData = write_current_energy_and_weather(parsedEnergy, parsedWeather)



