#!/bin/bash
#SBATCH -t 128:00:00
#SBATCH -p batch
#SBATCH -N 1
#SBATCH -n 1 
#SBATCH -J DailyHarvester
#SBATCH --mem-per-cpu 64000

export PYTHONPATH=/projects/sequence_analysis/vol1/prediction_work/HARVESTOR/DataHarvesterSources
export RUNTIMEDIR=/projects/ees/TDS/DataHarvesting/DAILY_HARVESTING

# ADCIRC

for DAYS in 14 15 ; do
    for HOURS in 00 06 12 18 ; do
        python fetch_adcirc_addNowcast.py --url "http://tds.renci.org/thredds/dodsC/2022/nam/202202$DAYS$HOURS/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast/fort.61.nc" --data_source 'ASGS'
    done
done

