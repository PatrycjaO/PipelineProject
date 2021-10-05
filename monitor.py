import json
import csv
import urllib.request
import re

def location(lookup_tables):
    locationIDs = []
    locations = []
    filename = open(lookup_tables, 'r')
    file = csv.DictReader(filename)
    for col in file:
        locationIDs.append(col['regionID'])
        locations.append(col['region'])
    lookupDict = dict(zip(locationIDs,locations))
    print(lookupDict)
    return locationIDs, locations, lookupDict

def urlGeneratorEnergy(regions):
    urlsEnergy = []
    base_url = 'https://api-energiemonitor.eon.com/meter-data?regionCode='
    for region in regions:
        url = base_url + region
        urlsEnergy.append(url)
    return urlsEnergy

def urlGeneratorWeather(regions):
    urlsWeather = []
    base_url = 'https://api-energiemonitor.eon.com/weather-data?regionCode='
    for region in regions:
        url = base_url + region
        urlsWeather.append(url)
    return urlsWeather

def loadJsonEnergy(links):
    filepaths = []
    parsed_list = []
    for url in links:
        response = urllib.request.urlopen(url)
        current_energy = response.read()
        text = current_energy.decode('utf-8')
        parsed = json.loads(text)
        parsed_list.append(parsed)
        region_code = re.findall(r'\d+', url)
        region_code=region_code[0]
        filepath = str(region_code)+'_data_current_energy.json'
        filepaths.append(filepath)
        with open('raw_data/%s_data_current_energy.json' % region_code, 'w') as f:
            json.dump(parsed, f, ensure_ascii=False, indent=4)
    return parsed_list, filepaths

def loadJsonWeather(links):
    filepaths = []
    parsed_list = []
    for url in links:
        response = urllib.request.urlopen(url)
        current_energy = response.read()
        text = current_energy.decode('utf-8')
        parsed = json.loads(text)
        parsed_list.append(parsed)
        region_code = re.findall(r'\d+', url)
        region_code=region_code[0]
        filepath = str(region_code)+'_data_current_weather.json'
        filepaths.append(filepath)
        with open('raw_data/%s_data_current_weather.json' % region_code, 'w') as f:
            json.dump(parsed, f, ensure_ascii=False, indent=4)
    return parsed_list, filepaths

def writeCurrentEnergy(dataEnergy, dataWeather):

    f = open("monitor_data/live_energydata.csv", 'w')

    print("timestamp,regionCode,region,windPower_kWh,solarPower_kWh,windSpeed_m/s,cloudCover_pct,temperature_C", file=f)

    dicti = {'energy' : dataEnergy, 'weather' : dataWeather}
    for i in range(len(dicti['energy'])):
        item1 = dicti['energy'][i]
        item2 = dicti['weather'][i]

        timestamp = item1['timestamp']['start']
        regionCode = item1['regionCode']
        region = lookupDict[regionCode]
        windPower = float(item1['feedIn']['list'][4]['usage'])
        solarPower = float(item1['feedIn']['list'][1]['usage'])
        #regionCode = int(item1['regionCode'])

        temperature = item2['weather']['temperature']
        windSpeed = item2['weather']['windSpeed']
        cloudCover = float(item2['weather']['cloudCover'])

        expected_output = f"{timestamp},{regionCode},{region},{windPower},{solarPower},{windSpeed},{cloudCover},{temperature}"

        print(expected_output, file=f)

    f.close()

locationIDs, locations, lookupDict = location('lookup_table/locations.csv')
urlsEnergy = urlGeneratorEnergy(locationIDs)
urlsWeather = urlGeneratorWeather(locationIDs)
parsedEnergy, energy_json = loadJsonEnergy(urlsEnergy)
parsedWeather, weather_json = loadJsonWeather(urlsWeather)
currentMonitorData = writeCurrentEnergy(parsedEnergy, parsedWeather)



