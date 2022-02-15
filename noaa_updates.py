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
#    python noaa_updates.py --stoptime '2021-09-04 00:30:00' --ndays 3 --stations '8410140' '8761724'

import os,sys
import pandas as pd
import datetime as dt
from datetime import timedelta
from datetime import datetime as dt

from fetch_station_data import noaanos_fetch_data, contrails_fetch_data
from utilities.utilities import utilities as utilities

main_config = utilities.load_config()
rootdir=utilities.fetchBasedir(main_config['DEFAULT']['RDIR'], basedirExtra='')

# DEFAULT station list from CERA_NOAA_HSOFS_stations_V3.csv
default_stations=['2695540','8410140', '8411060', '8413320', '8418150', '8419317', '8423898', '8443970', '8447386', '8447435', '8447930', '8449130', '8452660', '8452944', '8454000', '8454049', '8461490', '8465705', '8467150', '8510560', '8516945', '8518750', '8518962', '8519483', '8531680', '8534720', '8536110', '8537121', '8539094', '8540433', '8545240', '8548989', '8551762', '8551910', '8555889', '8557380', '8570283', '8571421', '8571892', '8573364', '8573927', '8574680', '8575512', '8577330', '8594900', '8631044', '8632200', '8635027', '8635750', '8636580', '8637689', '8638610', '8638901', '8639348', '8651370', '8652587', '8654467', '8656483', '8658120', '8658163', '8661070', '8662245', '8665530', '8670870', '8720030', '8720218', '8720219', '8720226', '8720357', '8720625', '8721604', '8722670', '8722956', '8723214', '8723970', '8724580', '8725110', '8725520', '8726384', '8726520', '8726607', '8726667', '8726724', '8727520', '8728690', '8729108', '8729210', '8729840', '8732828', '8735180', '8735391', '8735523', '8736897', '8737048', '8737138', '8738043', '8739803', '8740166', '8741041', '8741533', '8747437', '8760721', '8760922', '8761305', '8761724', '8761927', '8761955', '8762075', '8762482', '8762483', '8764044', '8764227', '8764314', '8766072', '8767816', '8767961', '8768094', '8770570', '8770613', '8770822', '8771013', '8771450', '8772447', '8772471', '8773767', '8774770', '8775241', '8775870', '8779748', '8779770', '9751364', '9751381', '9751401', '9751639', '9752235', '9752695', '9755371', '9759110', '9759394', '9759938', '9761115']

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

    # Choose stations or grab default list
    if args.stations is not None:
        stations=list(args.stations)
    else:
        stations=default_stations
    print('{} '.format(stations))

    # Convert times to strings with T separators
    # Build metadata used for saving files
    metadata = '_'+starttime.replace(' ','T')+'_'+endtime.replace(' ','T')+'_'+product

    # Run the job
    noaanos = noaanos_fetch_data(stations, time_range, 'water_level')
    df_noaa_data = noaanos.aggregate_station_data()
    df_noaa_meta = noaanos.aggregate_station_metadata()

    # Reformat the data and for the database load
    df_noaa_data.index = df_noaa_data.index.strftime('%Y-%m-%dT%H:%M:%S')
    df_noaa_data.reset_index(inplace=True)
    df_noaa_data_out=pd.melt(df_noaa_data, id_vars=['TIME'])
    df_noaa_data_out.columns=('TIME','STATION',product.upper())
    df_noaa_data_out.set_index('TIME', inplace=True)
    df_noaa_meta.index.name='STATION'
    
    # Write out the data
    noaafile=utilities.writeCsv(df_noaa_data_out, rootdir=rootdir,subdir='',fileroot='noaa_stationdata',iometadata=metadata)
    noaametafile=utilities.writeCsv(df_noaa_meta, rootdir=rootdir,subdir='',fileroot='noaa_stationdata_meta',iometadata=metadata)
    utilities.log.info('Finished: Wrote files data {} and metadata {}'.format(noaafile,noaametafile))

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
