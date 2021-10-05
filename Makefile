current_energy_data_csv: current_weather_data_csv
	python3 20211001_live_energydata_berg.py
	python3 20211001_live_energydata_pfaffenhofen.py
	python3 20211001_live_energydata_regensburg.py
	echo "STEP 4 has finished running"

current_weather_data_csv: load_current_weather_data_json
	python3 20211001_weather_berg.py
	python3 20211001_weather_pfaffenhofen.py
	python3 20211001_weather_regensburg.py
	echo "STEP 3 has finished running"
	
load_current_weather_data_json: load_current_energy_data_json
	curl https://api-energiemonitor.eon.com/weather-data?regionCode=09188113 | jq . > raw_data/weatherdata_berg.json
	curl https://api-energiemonitor.eon.com/weather-data?regionCode=09375 | jq . > raw_data/weatherdata_regensburg.json
	curl https://api-energiemonitor.eon.com/weather-data?regionCode=09186 | jq . > raw_data/weatherdata_pfaffenhofen.json
	echo "STEP 2 has finished running"

load_current_energy_data_json:
	curl https://api-energiemonitor.eon.com/meter-data?regionCode=09188113 | jq . > raw_data/energydata_berg.json
	curl https://api-energiemonitor.eon.com/meter-data?regionCode=09375 | jq . > raw_data/energydata_regensburg.json 
	curl https://api-energiemonitor.eon.com/meter-data?regionCode=09186 | jq . > raw_data/energydata_pfaffenhofen.json
	echo "STEP 1 has finished running"
