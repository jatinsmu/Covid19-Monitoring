#!/bin/bash

# Data Extraction From Web Source
#Confirmed with COVID19 Cases
wget -O time_series_covid19_confirmed_global.csv https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv
#Death by COVID19 Cases
wget -O time_series_covid19_deaths_global.csv https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv
#Recovered from COVID19 Cases
wget -O time_series_covid19_recovered_global.csv https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv

# Data Loading To Hadoop
hadoop fs -mkdir -p /covid/
hadoop fs -put -f time_series_covid19_confirmed_global.csv /covid/
hadoop fs -put -f time_series_covid19_deaths_global.csv /covid/
hadoop fs -put -f time_series_covid19_recovered_global.csv /covid/
hadoop fs -ls /covid/
