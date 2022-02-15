#!/usr/bin/env python
#
# Here we are simulating having run a series of fetches from the Harvester and storing the data to csv files.
# These files will be used for building and testing out schema for the newq DB 
#
# We intentionally have time ranges that are overlapping
# We exclude adcirc for now
#
#

import os,sys
import pandas as pd
import datetime as dt
import math
from datetime import timedelta
import datetime as dt

from fetch_station_data import adcirc_fetch_data, noaanos_fetch_data, contrails_fetch_data
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

def return_list_of_daily_timeranges(time_tuple)-> list():
    """
    Input:
        A tuple consisting of:
        start_time: Time of format %Y-%m-%d %H:%M:%S (str)
        end_time: Time of format %Y-%m-%d %H:%M:%S (str)

    Output:
        periods: List of daily tuple ranges

    Take an arbitrary start and end time (inclusive) in the format of %Y-%m-%d %H:%M:%S. Break up into a list of tuples which 
    which are at most a day in length AND break alopng day boundaries. [ {day1,day1),(day2,day2)..]
    The first tuple and the last tuple can be partial days. All intervening tuples will be full days.

    Assume an HOURLY stepping even though non-zero minute offsets may be in effect.
    
    Return:  
    """
    start_time=time_tuple[0]
    end_time=time_tuple[1]
    print(start_time)
    print(end_time)
    periods=list()
    dformat='%Y-%m-%d %H:%M:%S'
    print('Input: start time {}, end_time {}'.format(start_time, end_time))
    
    time_start = dt.datetime.strptime(start_time, dformat)
    time_end = dt.datetime.strptime(end_time, dformat) 
    if time_start > time_end:
        print('Swapping input times') # I want to be able to log this eventually
        time_start, time_end = time_end, time_start
        
    today = dt.datetime.today()
    if time_end > today:
          time_end = today
          print('Truncating list: new end time is {} '.format(dt.datetime.strftime(today, dformat)))
                           
    #What hours/min/secs are we starting on - compute proper interval shifting
    
    init_hour = 24-math.floor(time_start.hour)-1
    init_min = 60-math.floor(time_start.minute)-1
    init_sec = 60-math.floor(time_start.second)-1
    
    oneSecond=timedelta(seconds=1) # An update interval shift

    subrange_start = time_start   
    while subrange_start < time_end:
        interval = timedelta(hours=init_hour, minutes=init_min, seconds=init_sec)
        subrange_end=min(subrange_start+interval,time_end) # Need a variable interval to prevent a day-span  
        periods.append( (dt.datetime.strftime(subrange_start,dformat),dt.datetime.strftime(subrange_end,dformat)) )
        subrange_start=subrange_end+oneSecond # onehourint
        init_hour, init_min, init_sec = 23,59,59
    return periods

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
          print('Truncating list: new end time is {} '.format(dt.datetime.strftime(today, dformat)))             
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

def get_noaa_stations(fname='/projects/sequence_analysis/vol1/prediction_work/HARVESTOR/fetch_station_data/config/noaa_stations.txt'):
    """
    Simply read a list of stations from a txt file.
    """
    # choose the HSOFS list that includes the islands

    noaa_stations=list()
    with open(fname) as f:
        for station in f:
            noaa_stations.append(station)
    noaa_stations=[word.rstrip() for word in noaa_stations[1:]] # Strip off comment line
    return noaa_stations

def get_contrails_stations(fname='/projects/sequence_analysis/vol1/prediction_work/HARVESTOR/fetch_station_data/config/contrails_stations.txt'):
    """
    A convenience method to fetch river guage lists. 
    Contrails data
    """
    contrails_stations=list()
    with open(fname) as f:
        for station in f:
            contrails_stations.append(station)
    contrails_stations=[word.rstrip() for word in contrails_stations[1:]] # Strip off comment line
    return contrails_stations

def format_data_frames(df, df_meta):
    """
    A Common formatting used by all sources
    """
    df.index = df.index.strftime('%Y-%m-%dT%H:%M:%S')
    df.reset_index(inplace=True)
    df_out=pd.melt(df, id_vars=['TIME'])
    df_out.columns=('TIME','STATION',PRODUCT.upper())
    df_out.set_index('TIME',inplace=True)
    df_meta.index.name='STATION'
    return df_out, df_meta

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
## Start the job
##

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

    time_start=time_stop-timedelta(days=args.ndays) # How many days BACK
    starttime=dt.datetime.strftime(time_start, dformat)
    endtime=dt.datetime.strftime(time_stop, dformat)

    #starttime='2021-12-08 12:00:00'
    #endtime='2021-12-10 00:00:00'

    print('Time range is {} to {}, ndays is {}'.format(starttime,endtime,args.ndays))

    ## Get default stations
    noaa_stations=get_noaa_stations()
    contrails_stations=get_contrails_stations()
    adcirc_stations=noaa_stations+contrails_stations

    ## Set up Contrails
    config = utilities.load_config('./secrets/contrails.yml')['DEFAULT']

    # Build ranges for contrails ( and noaa/nos if you like)
    time_range=[(starttime,endtime)] # Can be directly used by NOAA 
    periods=return_list_of_daily_timeranges(time_range[0])

    # Now build urls
    # NOWCASTS
    nowfrmt='http://tds.renci.org/thredds/dodsC/2021/nam/%s/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.61.nc'
    listtimes = returnListOfURLRanges(starttime, endtime, adctype='nowcast')
    urls=list()
    for time in listtimes:
        url=nowfrmt % (time.strftime('%Y%m%d%H'))
        urls.append(url) # This would replace PERIODS in the DH fetcher codes

    #FORECASTS
    forefrmt='http://tds.renci.org/thredds/dodsC/2021/nam/%s/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast/fort.61.nc'   
    listtimes = returnListOfURLRanges(starttime, endtime, adctype='forecast')
    urls_fc=list()
    for time in listtimes:
        url=forefrmt % (time.strftime('%Y%m%d%H'))
        urls_fc.append(url) # This would replace PERIODS in the DH fetcher codes

    #print(urls)
    #print(urls_fc)

    # Basic metadata for ALL data files
    metadata = '_'+starttime.replace(' ','T')+'_'+endtime.replace(' ','T')

    #NOAA/NOS
    noaanos = noaanos_fetch_data(noaa_stations, time_range, 'water_level')
    df_noaa_data = noaanos.aggregate_station_data()
    df_noaa_meta = noaanos.aggregate_station_metadata()
    df_noaa_data_out,df_noaa_meta = format_data_frames(df_noaa_data,df_noaa_meta)
    
    #Contrails - useperiods instead of timerange
    contrails = contrails_fetch_data(contrails_stations, periods, config, 'water_level', 'NCEM')
    df_contrails_data = contrails.aggregate_station_data()
    df_contrails_meta = contrails.aggregate_station_metadata()
    df_contrails_data_out,df_contrails_meta = format_data_frames(df_contrails_data,df_contrails_meta)
    
    # NOWCAST ADCIRC
    adcirc = adcirc_fetch_data(adcirc_stations, urls, 'water_level')
    df_adcirc_data = adcirc.aggregate_station_data()
    df_adcirc_meta = adcirc.aggregate_station_metadata()
    df_adcirc_data_out,df_adcirc_meta = format_data_frames(df_adcirc_data,df_adcirc_meta)
    
    # FORECAST ADCIRC
    adcirc_fc = adcirc_fetch_data(adcirc_stations, urls_fc, 'water_level')
    df_adcirc_fc_data = adcirc.aggregate_station_data()
    df_adcirc_fc_meta = adcirc.aggregate_station_metadata()
    df_adcirc_fc_data_out,df_adcirc_fc_meta = format_data_frames(df_adcirc_fc_data,df_adcirc_fc_meta)

    # Save the OBS files
    noaafile=utilities.writeCsv(df_noaa_data_out, rootdir=rootdir,subdir='',fileroot='noaa_stationdata',iometadata=metadata)
    noaametafile=utilities.writeCsv(df_noaa_meta, rootdir=rootdir,subdir='',fileroot='noaa_stationdata_meta',iometadata=metadata)
    # Contrails
    contrailsfile=utilities.writeCsv(df_contrails_data_out, rootdir=rootdir,subdir='',fileroot='contrails_stationdata',iometadata=metadata)
    contrailsmetafile=utilities.writeCsv(df_contrails_meta, rootdir=rootdir,subdir='',fileroot='contrails_stationdata_meta',iometadata=metadata)
    #Nowcasts
    adcircfile=utilities.writeCsv(df_adcirc_data_out, rootdir=rootdir,subdir='',fileroot='adcirc_stationdata_nowcast',iometadata=metadata)
    adcircmetafile=utilities.writeCsv(df_adcirc_meta, rootdir=rootdir,subdir='',fileroot='adcirc_stationdata_meta_nowcast',iometadata=metadata)
    #Forecasts
    adcircfile_fc=utilities.writeCsv(df_adcirc_fc_data_out, rootdir=rootdir,subdir='',fileroot='adcirc_stationdata_namforecast',iometadata=metadata)
    adcircmetafile_fc=utilities.writeCsv(df_adcirc_fc_meta, rootdir=rootdir,subdir='',fileroot='adcirc_stationdata_meta_namforecast',iometadata=metadata)
    print('Finished metadata is {}'.format(metadata))

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('--ndays', action='store', dest='ndays', default=2, type=int,
                        help='Number of look-back days from stoptime (or now)')
    parser.add_argument('--stoptime', action='store', dest='stoptime', default=None, type=str,
                        help='Desired stoptime YYYY-mm-dd HH:MM:SS. Default=now')
    args = parser.parse_args()
    sys.exit(main(args))

