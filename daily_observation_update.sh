#
# Setup the basic env values relative to /projects/sequence_analysis/vol1/prediction_work/HARVESTOR/fetch_station_data

export PYTHONPATH=/projects/sequence_analysis/vol1/prediction_work/HARVESTOR/fetch_station_data
#export RUNTIMEDIR=./DAILIES-TEST
export RUNTIMEDIR=/projects/ees/TDS/DataHarvesting/DAILY_HARVESTING

# Prepare invocation
cd /projects/sequence_analysis/vol1/prediction_work/HARVESTOR/fetch_station_data

# Dig out the DAY, MONTH, and YEAR values. Always set hour:min:sec to 00:00:00

day="$(date +'%d')"
month="$(date +'%m')"
year="$(date +'%Y')"
hour="$(date +'%H')"
#printf "Current date in dd/mm/yyyy format %s\n" "$day-$month-$year $hour:00:00"
printf "Current date in dd/mm/yyyy format %s\n" "$day-$month-$year 00:00:00"

stoptime="$year-$month-$day 00:00:00"

/home/jtilson/anaconda3/bin/python fetch_data.py --data_source 'CONTRAILS' --data_product 'coastal_water_level' --stoptime "$stoptime"
/home/jtilson/anaconda3/bin/python fetch_data.py --data_source 'CONTRAILS' --data_product 'river_water_level' --stoptime "$stoptime"
/home/jtilson/anaconda3/bin/python fetch_data.py --data_source 'NOAA' --data_product 'water_level' --stoptime "$stoptime"

echo "Finished"



