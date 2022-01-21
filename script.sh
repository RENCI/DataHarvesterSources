#!/bin/bash
#SBATCH -t 512:00:00
#SBATCH -p batch
#SBATCH -N 1
#SBATCH -n 1 
#SBATCH -J DataHarvester
#SBATCH --mem-per-cpu 256000

export PYTHONPATH=/projects/sequence_analysis/vol1/prediction_work/HARVESTOR/fetch_station_data
export RUNTIMEDIR=./NEWTEST5

# Run 10 days worth of runs with a 2 day look back
python update_newtest_inputs_2.py --gridname 'hsofs' --stoptime '2022-01-09 00:00:00'
python update_newtest_inputs_2.py --gridname 'hsofs' --stoptime '2022-01-10 00:00:00'
python update_newtest_inputs_2.py --gridname 'hsofs' --stoptime '2022-01-11 00:00:00'
python update_newtest_inputs_2.py --gridname 'hsofs' --stoptime '2022-01-12 00:00:00'
python update_newtest_inputs_2.py --gridname 'hsofs' --stoptime '2022-01-13 00:00:00'
python update_newtest_inputs_2.py --gridname 'hsofs' --stoptime '2022-01-14 00:00:00'
python update_newtest_inputs_2.py --gridname 'hsofs' --stoptime '2022-01-15 00:00:00'
python update_newtest_inputs_2.py --gridname 'hsofs' --stoptime '2022-01-16 00:00:00'
python update_newtest_inputs_2.py --gridname 'hsofs' --stoptime '2022-01-17 00:00:00'
python update_newtest_inputs_2.py --gridname 'hsofs' --stoptime '2022-01-18 00:00:00'
