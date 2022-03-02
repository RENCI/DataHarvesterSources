#!/usr/bin/env python
#
# Here we are simulating having run a series of fetches from the Harvester and storing the data to csv files.
# These files will be used for building and testing out schema for the newq DB 
#
# The filenames are going to have the following nomenclature:
#
# Basic stations will be as before with a simple name such as
#      noaa_stationdata_times and noaa_stationdata_meta_time
#

import os,sys
import pandas as pd
import datetime as dt
import math

import fetch_data as fetch_data

#from fetch_station_data import adcirc_fetch_data, noaanos_fetch_data, contrails_fetch_data
from utilities.utilities import utilities as utilities

main_config = utilities.load_config()
rootdir=utilities.fetchBasedir(main_config['DEFAULT']['RDIR'], basedirExtra='')

##
## Some basic functions that will eventually be handled by the caller
##

# This is UGLY. Contrails forces us to split times by the day. So a simple stride seems hard to do.
# For all but the first day, we can simply assume each hour is 59 min, 59 sec long beyond the first point. So this is easy
# But for the first time, we need to compute how many min+sec to the ending break.
#
# TODO replace this with something clever

def closest_number(n, h=6, adctype='nowcast'):
    """
    minutes and secons are ignored in this function
    
    Input:
        n: hourly indicator of the desired time. time.hour (int)
        h: hourly demarcations. 6 = every 6 hours < 24 (00,06,12,18) 
        type: nowcast yields times that are constrained to be <= start_time and end_time ,respectively.
            however if not nowcast (namforecast), the end times can be in the future
        
    Nowcast urls:
    We wish to build a series of 6-hourly times for building ADCIRC urls
    We never want to go past the final time. For the start time, however, we may
    want to go ahead and include it. Return a LIST of ints (about the inpt time m) which we
    can pass to subsequent relativetime functions.
        
    This differs from the forecast numbers since, in that case, we DO want to go to a 
    future end time and expect the files to exist
    
    """
    # Need to NOT go beyond the time_start or time_end. So we include a lower bounds on n2
    # Two possibilities
    # First: input time can already be on a proper boundary - done
    # Ensure n = (0,23) not (1,24)
    if n>=24:
        print('Input hours must be in the range (0-23). Input was {}'.format(n))
        sys.exit(1)
    q = int(n / h)
    n1 = h * q     # 1st possible closest number
    # Second: find the nearest value +/- 2nd possible closest number
    if((n * h) > 0) :
        n2 = (h * (q + 1))
    else :
        n2 = (h * (q - 1))
    # Apply lower-bound to n2.
    
    if adctype=='nowcast':
        if n2 > n: 
            n2 -= 6 # The first line limits time to < 24 so this is okay
    # if true, then n1 is the required closest number
    if (abs(n - n1) < abs(n - n2)) :
        return n1
    # else n2 is the required closest number
    return n2


def returnListOfURLRanges(start_time, end_time, adctype='nowcast'):
    """
    Return a list of times on the 00,06,12,18 bounds. Note minutes and seconds are ignored.
    
    Input:
        start_time: (str) format %Y-%m-%d %H:%M:%S
        end_time: (str) format %Y-%m-%d %H:%M:%S
        type: nowcast yields times that are <= start_time and end_time ,respectively.
            however if namforecast, the end times can be in the future
        
    Return:  
    The list entries are valid TIMES to buld URLS
    for the HSOFS fetched from the RENCI server for the year 2021.
    Eg:
    http://tds.renci.org/thredds/dodsC/2021/nam/2021052318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.63.nc"
    """
    periods=list()
    dformat='%Y-%m-%d %H:%M:%S'
    print('URL Input: start time {}, end_time {}'.format(start_time, end_time))
    time_start = dt.datetime.strptime(start_time, dformat)
    time_end = dt.datetime.strptime(end_time, dformat) 
    if time_start > time_end:
        print('Swapping input times') # I want to be able to log this eventually
        time_start, time_end = time_end, time_start
    today = dt.datetime.today()
    if time_end > today:
          time_end = today
          print('ADCIRC Truncating list: new end time is {} '.format(dt.datetime.strftime(today, dformat)))             
    # Now find a list of times on the HH=00,06,12,18 <= time_end with the start time
    # nearest <= time_start
    h_final=closest_number(time_end.hour, 6, adctype='nowcast')
    h_start=closest_number(time_start.hour, 6, adctype='nowcast')
    end_time= time_end.replace(hour=0, minute=0, second=0) + dt.timedelta(hours=h_final)
    start_time= time_start.replace(hour=0, minute=0, second=0) - dt.timedelta(hours=h_start)
    # Loop over range and fill in the details
    print(h_start)
    sixHours=dt.timedelta(hours=6) # An update interval shift
    timelist=list()
    step_time = start_time
    while step_time <= end_time:
        timelist.append(step_time) # Format the same as a typical ADCIRC URL ()
        step_time+=sixHours
        print(step_time)
    return timelist

##
## End functions
##

##
## Globals
##

dformat='%Y-%m-%d %H:%M:%S'
GLOBAL_TIMEZONE='gmt' # Every source is set or presumed to return times in the zone
PRODUCT='water_level'

##
## Run stations
##

RESAMPLE=15
PRODUCT='water_level'

def main(args):
    """
    Generally we anticipate inputting a STOPTIME
    Then the STARTTIME is ndays on the past
    """

    # Setup times and ranges
    if args.stoptime is not None:
        time_stop=dt.datetime.strptime(args.stoptime,dformat)
    else:
        time_stop=dt.datetime.now()

    time_start=time_stop+dt.timedelta(days=args.ndays) # How many days BACK
    starttime=dt.datetime.strftime(time_start, dformat)
    endtime=dt.datetime.strftime(time_stop, dformat)
    #starttime='2021-12-08 12:00:00'
    #endtime='2021-12-10 00:00:00'

    print('Time range is {} to {}, ndays is {}'.format(starttime,endtime,args.ndays))

    ## Get default stations: Methods will quietly ignore superfluous stations
    noaa_stations=fetch_data.get_noaa_stations()

    # Build ranges for contrails ( and noaa/nos if you like)
    time_range=(starttime,endtime) # Can be directly used by NOAA 

    #NOAA/NOS
    noaa_metadata='_'+endtime.replace(' ','T') # +'_'+starttime.replace(' ','T')
    data, meta = fetch_data.process_noaa_stations(time_range, noaa_stations, noaa_metadata, PRODUCT)
    df_noaa_data = fetch_data.format_data_frames(data) # Melt the data :s Harvester default format
    # Output
    try:
        dataf=utilities.writeCsv(df_noaa_data, rootdir=rootdir,subdir='',fileroot='noaa_stationdata',iometadata=noaa_metadata)
        metaf=utilities.writeCsv(meta, rootdir=rootdir,subdir='',fileroot='noaa_stationdata_meta',iometadata=noaa_metadata)
        utilities.log.info('NOAA data has been stored {},{}'.format(dataf,metaf))
    except Exception as e:
        utilities.log.error('Error: NOAA: Failed Write {}'.format(e))
        sys.exit(1)

    print('Finished')

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('--ndays', action='store', dest='ndays', default=-2, type=int,
                        help='Number of look-back days from stoptime (or now)')
    parser.add_argument('--stoptime', action='store', dest='stoptime', default=None, type=str,
                        help='Desired stoptime YYYY-mm-dd HH:MM:SS. Default=now')

    args = parser.parse_args()
    sys.exit(main(args))

