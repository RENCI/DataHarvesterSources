#!/bin/bash
#SBATCH -t 128:00:00
#SBATCH -p batch
#SBATCH -N 1
#SBATCH -n 1 
#SBATCH -J DailyHarvester
#SBATCH --mem-per-cpu 64000

## A quick script to process all preceding January days to now.

export PYTHONPATH=/projects/sequence_analysis/vol1/prediction_work/HARVESTOR/DataHarvesterSources
export RUNTIMEDIR=./DAILIES

# Observations

#for DAYS in 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26; do
for DAYS in 27 28 29 30 31; do
    python fetch_data.py --data_source 'CONTRAILS' --data_product 'coastal_water_level' --stoptime '2022-01-'$DAYS' 00:00:00'
    python fetch_data.py --data_source 'CONTRAILS' --data_product 'river_water_level' --stoptime '2022-01-'$DAYS' 00:00:00'
    python fetch_data.py --data_source 'NOAA' --data_product 'water_level' --stoptime '2022-01-'$DAYS' 00:00:00'
done


