#
# Setup the basic env values relative to /projects/sequence_analysis/vol1/prediction_work/HARVESTOR/fetch_station_data
#
# This job is to be run every 6 hours to check for adcirc files
# Run the cron at 6 hours + 15 mins to help ensure the data actually got to the ASGS site
# We do not included $min:$sec values here

export PYTHONPATH=/projects/sequence_analysis/vol1/prediction_work/HARVESTOR/fetch_station_data
#export RUNTIMEDIR=./DAILIES
export RUNTIMEDIR=/projects/ees/TDS/DataHarvesting/DAILY_HARVESTING

# Prepare invocation
cd /projects/sequence_analysis/vol1/prediction_work/HARVESTOR/fetch_station_data

# Dig out the DAY, MONTH, and YEAR values. Always set hour:min:sec to 00:00:00

day="$(date +'%d')"
month="$(date +'%m')"
year="$(date +'%Y')"
hour="$(date +'%H')"
printf "Current rumtime in yyyy-mm-dd hh format %s\n" "$year-$month-$day $hour"

# Let's check to see if hour is either 00,06,12,or 18. Else do nothing and fix your cron launch parameters. *set to  

if [ "$hour" -eq "00" ] || [ "$hour" -eq "06" ] || [ "$hour" -eq "12" ] || [ "$hour" -eq "18" ]
    then
        urltime="$year$month$day$hour"
        /home/jtilson/anaconda3/bin/python fetch_adcirc_data.py --url "http://tds.renci.org:8080/thredds/dodsC/$year/nam/$urltime/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.61.nc" --data_source 'ASGS'
        /home/jtilson/anaconda3/bin/python fetch_adcirc_data.py --url "http://tds.renci.org:8080/thredds/dodsC/$year/nam/$urltime/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast/fort.61.nc" --data_source 'ASGS'
        echo "Requested ASGS data"
    else
    # Cron should have taken care of this but here we are...
        echo "A non-6hourly time was specified. Can't do anything with this but exit"
fi
echo "Finished"

