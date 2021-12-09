#!/usr/bin/env python
#
# A simple program to launch the NOAA/NOS fetch code
# This is expected to be cron'd
#
# It takes either now() or the input time string
# and build a start time of n-days behind
#
#
# Can launch a job like:
#    python contrails_updates.py --stoptime '2021-09-04 00:30:00' --ndays 3 --stations '8410140' '8761724'

import os,sys
import pandas as pd
import math
import datetime as dt
from datetime import timedelta
from datetime import datetime as dt

from fetch_station_data import contrails_fetch_data, contrails_fetch_data
from utilities.utilities import utilities as utilities

#
# Setup authorization codes and setup the dict
#
domain='http://contrail.nc.gov:8080/OneRain/DataAPI'
#method='GetSensorData'
systemkey = '20cebc91-5838-49b1-ab01-701324161aa8'

config={'domain':'http://contrail.nc.gov:8080/OneRain/DataAPI',
        'systemkey':'20cebc91-5838-49b1-ab01-701324161aa8'}

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

    time_start = dt.strptime(start_time, dformat)
    time_end = dt.strptime(end_time, dformat)
    if time_start > time_end:
        print('Swapping input times') # I want to be able to log this eventually
        time_start, time_end = time_end, time_start

    today = dt.today()
    if time_end > today:
          time_end = today
          print('Truncating list: new end time is {} '.format(dt.strftime(today, dformat)))

    #What hours/min/secs are we starting on - compute proper interval shifting

    init_hour = 24-math.floor(time_start.hour)-1
    init_min = 60-math.floor(time_start.minute)-1
    init_sec = 60-math.floor(time_start.second)-1

    oneSecond=timedelta(seconds=1) # An update interval shift

    subrange_start = time_start
    while subrange_start < time_end:
        interval = timedelta(hours=init_hour, minutes=init_min, seconds=init_sec)
        subrange_end=min(subrange_start+interval,time_end) # Need a variable interval to prevent a day-span  
        periods.append( (dt.strftime(subrange_start,dformat),dt.strftime(subrange_end,dformat)) )
        subrange_start=subrange_end+oneSecond # onehourint
        init_hour, init_min, init_sec = 23,59,59
    return periods

##
## Start the job
##

main_config = utilities.load_config()
rootdir=utilities.fetchBasedir(main_config['DEFAULT']['RDIR'], basedirExtra='')

# DEFAULT station list 
default_stations=['30069', '30054', '30055', '30039', '30006', '30033', '30065', '30029', '30052', '30012', 'WNRN7', '30003', '30050', 'EWPN7', '35370047744601', '30048', '30009', 'GTNN7', '30032', 'MINN7', '30011', 'EGHN7', '30016', 'WSNN7', 'HSWN7', '30008', '30030', 'HBKN7', 'JYCN7', '30010', 'WDVN7', '30031', '30001', 'ORLN7', 'VNCN7', '30060', 'JCKN7', 'ALIN7', '30062', 'CTIN7', 'OCAN7', 'EMWN7', 'BLHN7', 'PNGN7', '30059', 'ROFN7', 'COLN7', '30042', '30002', '30064', '30053', 'WHSN7', '30061', '30058', '30017', 'STYN7', 'STON7', 'GRMN7', 'RMBN7', 'TRTN7', '30015', '30007']

dformat='%Y-%m-%d %H:%M:%S'

def main(args):
    ndays=args.ndays
    product=args.product

    # Setup times and ranges
    if args.stoptime is not None:
        time_stop=dt.strptime(args.stoptime,dformat)
    else:
        time_stop=dt.now()
    time_start=time_stop-timedelta(days=ndays)
    starttime=dt.strftime(time_start, dformat)
    endtime=dt.strftime(time_stop, dformat)
    time_range=[(starttime,endtime)] # A list of tuples

    periods=return_list_of_daily_timeranges(time_range[0])
    print(periods)

    metadata = '_'+starttime.replace(' ','T')+'_'+endtime.replace(' ','T')+'_'+product

    # Choose stations or grab default list
    if args.stations is not None:
        stations=list(args.stations)
    else:
        stations=default_stations
    #print('{} '.format(stations))

    # Build metadata used for saving files
    metadata = '_'+starttime.replace(' ','T')+'_'+endtime.replace(' ','T')+'_'+product

    # Run the job
    contrails = contrails_fetch_data(stations, periods, config, product='water_level', owner='NCEM')
    df_contrails_data = contrails.aggregate_station_data()
    df_contrails_meta = contrails.aggregate_station_metadata()

    # Reformat the data for the database load
    df_contrails_data.index = df_contrails_data.index.strftime('%Y-%m-%dT%H:%M:%S')
    df_contrails_data.reset_index(inplace=True)
    df_contrails_data_out=pd.melt(df_contrails_data, id_vars=['TIME'])
    df_contrails_data_out.columns=('TIME','STATION',product.upper())
    df_contrails_data_out.set_index('TIME',inplace=True)
    df_contrails_meta.index.name='STATION'
    
    # Write out the data
    contrailsfile=utilities.writeCsv(df_contrails_data_out, rootdir=rootdir,subdir='',fileroot='contrails_stationdata',iometadata=metadata)
    contrailsmetafile=utilities.writeCsv(df_contrails_meta, rootdir=rootdir,subdir='',fileroot='contrails_stationdata_meta',iometadata=metadata)
    utilities.log.info('Finished: Wrote files data {} and metadata {}'.format(contrailsfile,contrailsmetafile))

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('--ndays', action='store', dest='ndays', default=2, type=int, 
                        help='Number of look-back days from stoptime (or now)')
    parser.add_argument('--stoptime', action='store', dest='stoptime', default=None, type=str, 
                        help='Desired stoptime YYYY-mm-dd HH:MM:SS. Default=now')
    parser.add_argument('--product', action='store', dest='product', default='water_level', type=str, 
                        help='Desired product')
    parser.add_argument('--stations', nargs="*", type=str, action='store', dest='stations', default=None,
                        help='List of str station ids')
    args = parser.parse_args()
    sys.exit(main(args))
