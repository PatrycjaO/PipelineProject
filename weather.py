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
        value = list(lat,lon)
        region_code = [k for k, v in lookupdict.items() if v == value]
        filepath = str(region_code)+'_data_forecast_weather.json'
        filepaths.append(filepath)
        with open('raw_data/%s_data_forecast_weather.json' % region_code, 'w') as f:
            json.dump(parsed, f, ensure_ascii=False, indent=4)
    return parsed_list, filepaths



def writeCurrentWeather(dataWeather):

    f = open("monitor_data/live_energydata.csv", 'w')

    print("timestamp,regionCode,region,windPower_kWh,solarPower_kWh,windSpeed_m/s,cloudCover_pct,temperature_C", file=f)

    dicti = {'energy' : dataWeather, 'weather' : dataWeather}
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

locationIDs, locations, longitude, latitude, wo_sun, wo_key, lookupDict, lookupRegion = location('lookup_table/locations.csv')
urlsWeather = urlGeneratorWeather(locationIDs, longitude, latitude)
parsedWeather, energy_json = loadJsonWeather(urlsWeather, lookupDict)
#currentMonitorData = writeCurrentWeather(parsedWeather)