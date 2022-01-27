#!/usr/bin/env python
#
# We intentionally have time ranges that are overlapping
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

from fetch_station_data import adcirc_fetch_data
from utilities.utilities import utilities as utilities

main_config = utilities.load_config()
rootdir=utilities.fetchBasedir(main_config['DEFAULT']['RDIR'], basedirExtra='')

# Currently supported sources
SOURCES = ['ASGS']

def get_adcirc_stations(fname='/projects/sequence_analysis/vol1/prediction_work/HARVESTOR/fetch_station_data/config/adcirc_stations.txt'):
    """
    Simply read a list of stations from a txt file.
    Generally, we simply com,bine NOAA and COntrails into a single list. It is okay to include stations not likely to exist since
    the processing stage will simply remove them

    """
    adcirc_stations=list()
    with open(fname) as f:
        for station in f:
            adcirc_stations.append(station)
    adcirc_stations=[word.rstrip() for word in adcirc_stations[1:]] # Strip off comment line
    return adcirc_stations

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

# The hurricane methods are for the future
def checkAdvisory(value):
    """
    Try to ensure if an advisory number was passed
    """
    state_hurricane=False
    utilities.log.debug('Check advisory {}'.format(value))
    try:
        test=dt.datetime.strptime(value,'%Y%m%d%H')
        utilities.log.info('A timestamp data was found: Not a Hurricane URL ? {}'.format(test))
    except ValueError:
        try:
            outid = int(value)
            state_hurricane=True
        except ValueError:
            utilities.log.error('Expected an Advisory value but could not convert to int {}'.format(value))
            sys.exit(1)
    utilities.log.info('URL state_hurricane is {}'.format(state_hurricane))
    return state_hurricane

def checkIfHurricane(url):
    """
    Very simple procedure but requires using the ASGS nomenclature
    """
    words=url.split('/')
    state_hurricane = checkAdvisory(words[-6])
    return state_hurricane

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

def process_adcirc_stations(url, adcirc_stations, gridname, instance, metadata, data_product='water_level'):
    # Fetch the data
    try:
        if data_product != 'water_level':
            utilities.log.error('ADCIRC data product can only be: water_level')
            sys.exit(1)

        adcirc = adcirc_fetch_data(adcirc_stations, url, data_product, gridname=gridname, castType=instance.rstrip())
        df_adcirc_data = adcirc.aggregate_station_data()
        df_adcirc_meta = adcirc.aggregate_station_metadata()
        df_adcirc_data_out,df_adcirc_meta = format_data_frames(df_adcirc_data,df_adcirc_meta)
        # Save data
        adcircfile=utilities.writeCsv(df_adcirc_data_out, rootdir=rootdir,subdir='',fileroot='adcirc_stationdata',iometadata=metadata)
        adcircmetafile=utilities.writeCsv(df_adcirc_meta, rootdir=rootdir,subdir='',fileroot='adcirc_stationdata_meta',iometadata=metadata)
        utilities.log.info('ADCIRC data has been stored {},{}'.format(adcircfile,adcircmetafile))
    except Exception as e:
        utilities.log.error('Error: ADCIRC: {}'.format(e))
    return adcircfile, adcircmetafile

def stripTimeFromURL(url):
    """
    We mandate that the URLs input to this fetcher are those used to access the ASGS data. The "time" information will be in position .split('/')[-6]
    eg. 'http://tds.renci.org:8080/thredds/dodsC/2021/nam/2021052318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.63.nc'
    
    Return time string in either ASGS formatted '%Y%m%d%H' or possibly as a hurricane advisory string (to be checked later)
    """
    words = url.split('/')
    ttime=words[-6] # Always count from the back. NOTE if a hurrican this could be an advisory number.
    return ttime

def stripInstanceFromURL(url):
    """
    We mandate that the URLs input to this fetcher are those used to access the ASGS data. The "time" information will be in position .split('/')[-2]
    eg. 'http://tds.renci.org:8080/thredds/dodsC/2021/nam/2021052318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.63.nc'
    
    Return:
        Instance string
    """
    words = url.split('/')
    instance=words[-2] # Usually nowcast,forecast, etc 
    return instance

def grabGridnameFromURL(url):
    """
    We mandate that the URLs input to this fetcher are those used to access the ASGS data. The "time" information will be in position .split('/')[-2]
    eg. 'http://tds.renci.org:8080/thredds/dodsC/2021/nam/2021052318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.63.nc'
    
    Return:
        grid.upper() string
    """
    words = url.split('/')
    grid=words[-5] # Usually nowcast,forecast, etc 
    return grid.upper()

def main(args):
    """
    We require the provided URL are using the typical ASGS nomenclature and that the timestamp is in ('/') position -6
    Moreover, This time stamp behaves a little different if fetching a nowcast versus a forecast. For now, we will
    annotate final .csv files with _TIME1_TIME2_ corresponding to the reported total range.
    """

    if args.sources:
         print('Return list of sources')
         return SOURCES
         sys.exit(0)

    data_source = args.data_source

    if data_source.upper() in SOURCES:
        utilities.log.info('Found selected data source {}'.format(data_source))
    else:
        utilities.log.error('Invalid data source {}'.format(data_source))
        sys.exit(1)

    url = args.url
    if url==None:
        utilities.log.error('No URL was specified: Abort')
        sys.exit(1)

    data_product = args.data_product
    if data_product != 'water_level':
        utilities.log.error('ADCIRC: Only available data product is water_level: {}'.format(data_product))
        sys.exit(1)
    else:
        utilities.log.info('Chosen data source {}'.format(data_source))

    # If this is a hurricane then abort for now
    if checkIfHurricane(url):
        utilities.log.error('URL is a Hurricane. We do not process those yet')
        sys.exit(1)

    urltimeStr = stripTimeFromURL(url)
    urltime = dt.datetime.strptime(urltimeStr,'%Y%m%d%H')

    instance = stripInstanceFromURL(url) 

    gridname = grabGridnameFromURL(url)

    ##
    ## Start the processing
    ##

    runtime=dt.datetime.strftime(urltime, dformat)
    #starttime='2021-12-08 12:00:00'
    utilities.log.info('Selected run time range is {}'.format(runtime))

    # metadata are used to augment filename
    #ASGS
    if data_source.upper()=='ASGS':
        excludedStations=list()

        urls=[url] # Can be directly used by NOAA 

        # Use default station list
        adcirc_stations=get_adcirc_stations()
        adcirc_metadata='_'+instance+'_'+runtime.replace(' ','T')
        dataf, metaf = process_adcirc_stations(urls, adcirc_stations, gridname, instance, adcirc_metadata, data_product)

    utilities.log.info('Finished with data source {}'.format(data_source))
    utilities.log.info('Data file {}, meta file {}'.format(dataf,metaf))
    utilities.log.info('Finished')

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('--sources', action='store_true',
                        help='List currently supported data sources')
    parser.add_argument('--data_source', action='store', dest='data_source', default='ASGS', type=str,
                        help='choose supported data source: default = ASGS')
    parser.add_argument('--url', action='store', dest='url', default=None, type=str,
                        help='ASGS url to fetcb ADCIRC data')
    parser.add_argument('--data_product', action='store', dest='data_product', default='water_level', type=str,
                        help='choose supported data product: default is water_level')

    args = parser.parse_args()
    sys.exit(main(args))
