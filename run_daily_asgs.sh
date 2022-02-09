#!/bin/bash
#SBATCH -t 128:00:00
#SBATCH -p batch
#SBATCH -N 1
#SBATCH -n 1 
#SBATCH -J DailyHarvester
#SBATCH --mem-per-cpu 64000

## A quick script to process all preceding January days to now.

export PYTHONPATH=/projects/sequence_analysis/vol1/prediction_work/HARVESTOR/DataHarvesterSources
#export RUNTIMEDIR=./DAILIES-TEST
export RUNTIMEDIR=/projects/ees/TDS/DataHarvesting/DAILY_HARVESTING

# ADCIRC

for DAYS in 02 ; do
    for HOURS in 00 06 ; do
        python fetch_adcirc_data.py --url "http://tds.renci.org:8080/thredds/dodsC/2022/nam/202202$DAYS$HOURS/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.61.nc" --data_source 'ASGS'
        python fetch_adcirc_data.py --url "http://tds.renci.org:8080/thredds/dodsC/2022/nam/202202$DAYS$HOURS/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast/fort.61.nc" --data_source 'ASGS'
    done
done

