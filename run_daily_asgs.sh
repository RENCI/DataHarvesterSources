#!/bin/bash
#SBATCH -t 128:00:00
#SBATCH -p batch
#SBATCH -N 1
#SBATCH -n 1 
#SBATCH -J DailyHarvester
#SBATCH --mem-per-cpu 64000

## A quick script to process all preceding January days to now.

export PYTHONPATH=/projects/sequence_analysis/vol1/prediction_work/HARVESTOR/fetch_station_data
export RUNTIMEDIR=./DAILIES-TEST

# ADCIRC

for DAYS in 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31; do
    for HOURS in 00 06 12 18 ; do
        #python fetch_adcirc_data.py --url http://tds.renci.org:8080/thredds/dodsC/2022/nam/202201$DAYS$HOURS/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.61.nc --data_source 'ASGS'
        python fetch_adcirc_data.py --url http://tds.renci.org:8080/thredds/dodsC/2022/nam/202201$DAYS$HOURS/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast/fort.61.nc --data_source 'ASGS'
    done
done

