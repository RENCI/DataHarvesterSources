## NOTE adcirc combined files may have duplicate times


#!/usr/bin/env python
#
# Here we are simulating having run a series of fetches from the Harvester and storing the data to csv files.
# These files will be used for building and testing out schema for the newq DB 
#
# We intentionally have time ranges that are overlapping
#
# The filenames are going to have the following nomenclature:
#
# ensemble: An arbitrary string. According to a cursory look at tds, this has values such as:
#    nowcast, nhc0fcl, veerright, etc. So we will set the following defaults:
# grid: hsofs,ec95d,etc
#
#

import os,sys
import pandas as pd
import datetime as dt
import math
from datetime import timedelta
import datetime as dt

from fetch_station_data import noaanos_fetch_data, contrails_fetch_data
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

# Currently supported sources
SOURCES = ['NOAA','CONTRAILS']

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
          print('Contrails: Truncating list: new end time is {} '.format(dt.datetime.strftime(today, dformat)))
                           
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
## Run stations
##

def process_noaa_stations(time_range, noaa_stations, metadata, data_product='water_level'):
    # Fetch the data
    try:
        if data_product != 'water_level':
            utilities.log.error('NOAA data product can only be: water_level')
            sys.exit(1)
        noaanos = noaanos_fetch_data(noaa_stations, time_range, data_product)
        df_noaa_data = noaanos.aggregate_station_data()
        df_noaa_meta = noaanos.aggregate_station_metadata()
        df_noaa_data_out,df_noaa_meta = format_data_frames(df_noaa_data,df_noaa_meta)
        # Save data
        noaafile=utilities.writeCsv(df_noaa_data_out, rootdir=rootdir,subdir='',fileroot='noaa_stationdata',iometadata=metadata)
        noaametafile=utilities.writeCsv(df_noaa_meta, rootdir=rootdir,subdir='',fileroot='noaa_stationdata_meta',iometadata=metadata)
        utilities.log.info('NOAA data has been stored {},{}'.format(noaafile,noaametafile))
    except Exception as e:
        utilities.log.error('Error: NOAA: {}'.format(e))
    return noaafile, noaametafile

def process_contrails_stations(periods, contrails_stations, metadata, data_product='river_water_level'):
    # Fetch the data
    dproduct=['river_water_level','coastal_water_level']
    if data_product not in dproduct:
        utilities.log.error('Contrails data product can only be: {}'.format(dproduct))
        sys.exit(1)
    try:
        contrails = contrails_fetch_data(contrails_stations, periods, config, product=data_product, owner='NCEM')
        df_contrails_data = contrails.aggregate_station_data()
        df_contrails_meta = contrails.aggregate_station_metadata()
        df_contrails_data_out,df_contrails_meta = format_data_frames(df_contrails_data,df_contrails_meta)
        # Save data
        contrailsfile=utilities.writeCsv(df_contrails_data_out, rootdir=rootdir,subdir='',fileroot='contrails_stationdata',iometadata=metadata)
        contrailsmetafile=utilities.writeCsv(df_contrails_meta, rootdir=rootdir,subdir='',fileroot='contrails_stationdata_meta',iometadata=metadata)
        utilities.log.info('CONTRAILS data has been stored {},{}'.format(contrailsfile,contrailsmetafile))
    except Exception as e:
        utilities.log.error('Error: CONTRAILS: {}'.format(e))
    return contrailsfile, contrailsfile

## Set up Contrails
domain='http://contrail.nc.gov:8080/OneRain/DataAPI'
systemkey = '20cebc91-5838-49b1-ab01-701324161aa8'
config={'domain':'http://contrail.nc.gov:8080/OneRain/DataAPI',
    'systemkey':'20cebc91-5838-49b1-ab01-701324161aa8'}

def main(args):
    """
    Generally we anticipate inputting a STOPTIME
    Then the STARTTIME is ndays on the past
    """

    if args.sources:
         print('Return list of sources')
         return SOURCES
         sys.exit(0)
    data_source = args.data_source
    data_product = args.data_product

    if data_source.upper() in SOURCES:
        utilities.log.info('Found selected data source {}'.format(data_source))
    else:
        utilities.log.error('Invalid data source {}'.format(data_source))
        sys.exit(1)

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

    utilities.log.info('Selected time range is {} to {}, ndays is {}'.format(starttime,endtime,args.ndays))

    # metadata are used to augment filename
    #NOAA/NOS
    if data_source.upper()=='NOAA':
        excludedStations=list()
        time_range=[(starttime,endtime)] # Can be directly used by NOAA 
        # Use default station list
        noaa_stations=get_noaa_stations()
        noaa_metadata='_'+starttime.replace(' ','T')+'_'+endtime.replace(' ','T')
        dataf, metaf = process_noaa_stations(time_range, noaa_stations, noaa_metadata, data_product)

    #Contrails
    if data_source.upper()=='CONTRAILS':
        template = "An exception of type {0} occurred."
        excludedStations=list()
        if data_product=='river_water_level':
            fname='/projects/sequence_analysis/vol1/prediction_work/HARVESTOR/fetch_station_data/config/contrails_stations_rivers.txt'
            meta='_RIVERS'
        else:
            fname='/projects/sequence_analysis/vol1/prediction_work/HARVESTOR/fetch_station_data/config/contrails_stations_coastal.txt'
            meta='_COASTAL'
        try:
            # Build ranges for contrails ( and noaa/nos if you like)
            time_range=[(starttime,endtime)] 
            periods=return_list_of_daily_timeranges(time_range[0]) # Must be broken up into days
            # Get default station list
            contrails_stations=get_contrails_stations(fname)
            contrails_metadata=meta+'_'+starttime.replace(' ','T')+'_'+endtime.replace(' ','T')
            dataf, metaf = process_contrails_stations(periods, contrails_stations, contrails_metadata, data_product)
        except Exception as ex:
            utilities.log.warn('CONTRAILS error'.format(template.format(type(ex).__name__, ex.args)))
            sys.exit(1)

    utilities.log.info('Finished with data source {}'.format(data_source))
    utilities.log.info('Data file {}, meta file {}'.format(dataf,metaf))
    utilities.log.info('Finished')

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('--ndays', action='store', dest='ndays', default=2, type=int,
                        help='Number of look-back days from stoptime (or now)')
    parser.add_argument('--stoptime', action='store', dest='stoptime', default=None, type=str,
                        help='Desired stoptime YYYY-mm-dd HH:MM:SS. Default=now')
    parser.add_argument('--sources', action='store_true',
                        help='List currently supported data sources')
    parser.add_argument('--data_source', action='store', dest='data_source', default=None, type=str,
                        help='choose supported data source (case independant) eg NOAA or CONTRAILS')
    parser.add_argument('--data_product', action='store', dest='data_product', default=None, type=str,
                        help='choose supported data product eg river_water_level: Only required for Contrails')

    args = parser.parse_args()
    sys.exit(main(args))
