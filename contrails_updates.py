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

config = utilities.load_config('./secrets/contrails.yml')['DEFAULT']

##
## Start the job
##

main_config = utilities.load_config()
rootdir=utilities.fetchBasedir(main_config['DEFAULT']['RDIR'], basedirExtra='')

config = utilities.load_config('./secrets/contrails.yml')['DEFAULT']

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
    time_range=(starttime,endtime) # A time_range tuple

    ##periods=return_list_of_daily_timeranges(time_range[0])
    ##print(periods)

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
    contrails = contrails_fetch_data(stations, time_range, config, product=product, owner='NCEM')
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
    parser.add_argument('--product', action='store', dest='product', default='river_water_level', type=str, 
                        help='Desired product')
    parser.add_argument('--stations', nargs="*", type=str, action='store', dest='stations', default=None,
                        help='List of str station ids')
    args = parser.parse_args()
    sys.exit(main(args))
