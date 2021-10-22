# PipelineProject
Portfolio Project for Pipeline Academy

# Why?

## Possible use cases:

- wind turbines need to be switched off, in case of wind speed exceeding threshold set by wind turbines rotor limitations
- overproduction of energy could 
  - overload the network
  - or be distributed differently (e. g. energy storage) 
- overall, better energy management

# The Project

## Monitoring Pipeline

### Data Acquisition

hourly via GitHub Actions:

- wind speed data scraping from https://energiemonitor.bayernwerk.de/ for three different locations
- wind power data scraping from https://energiemonitor.bayernwerk.de/ for three different locations

daily via GitHub Actions:

- wind speed forecast data from tomorrow.io API acquisition 

### Data Transformation

.json to .csv with python

### Load

- with python to postgres hosted on PythonAnywhere

### Serve

- Streamlit backed by postgres data base

## Main Features

The project is currently set up for three different regions. The pipeline can be easily extended to additional regions of interest due to modular python script based on a lookup table

## Project Status

Automated ETL pipeline completed and deployed on streamlit.io

(https://share.streamlit.io/patrycjao/pipelineproject/main)

## Next Steps

- integrate sunshine forecast API
- implement github secrets
- integrate forecasting for solar and wind power
- Great Expectations
- Logging & Error Handling
- Migration to AWS + Prefect
- expand tool to more regions

For further information, see PipelineProject.pdf



