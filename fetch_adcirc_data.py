#!/usr/bin/env python
#
# A class suitable for use by ADDA,APSVIZ2,Reanalysis to fetch ADCIRC water levels from the ASGS
# The ASGS inputs to this class is a list of URLs. If you require running this by specifying TIMES, 
# then you must preprocess the data into a list of URLs.
#
# TODO Check into the case where ADCIRC returns completely empty stations. This filtering may have been 
# turned off n the Harvester codes.

import os,sys
import pandas as pd
import datetime as dt
import math

from fetch_station_data import adcirc_fetch_data
from utilities.utilities import utilities as utilities

main_config = utilities.load_config()
rootdir=utilities.fetchBasedir(main_config['DEFAULT']['RDIR'], basedirExtra='')

# Currently supported sources
SOURCES = ['ASGS']

def get_adcirc_stations_fort63_style(fname='./config/CERA_NOAA_HSOFS_stations_V3.1.csv'):
    """
    Simply read a list of stations from a csv file.
    This gets read into a DataFrame. File MUST contain at least Node and stationid columns
    """
    df = pd.read_csv(fname, index_col=0, header=0, skiprows=[1], sep=',')
    df["stationid"]=df["stationid"].astype(str)
    df["Node"]=df["Node"].astype(int)
    return df

def get_adcirc_stations_fort61_style(fname='./config/adcirc_stations.txt'):
    """
    Simply read a list of stations from a txt file.
    Generally, we simply combine NOAA and Contrails into a single list. It is okay to include stations not likely to exist since
    the processing stage will simply remove them

    """
    adcirc_stations=list()
    with open(fname) as f:
        for station in f:
            adcirc_stations.append(station)
    adcirc_stations=[word.rstrip() for word in adcirc_stations[1:]] # Strip off comment line
    return adcirc_stations

def format_data_frames(df):
    """
    A Common formatting used by all sources
    """
    df.index = df.index.strftime('%Y-%m-%dT%H:%M:%S')
    df.reset_index(inplace=True)
    df_out=pd.melt(df, id_vars=['TIME'])
    df_out.columns=('TIME','STATION',PRODUCT.upper())
    df_out.set_index('TIME',inplace=True)
    return df_out

# The hurricane methods are for the future
def check_advisory(value):
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

def check_if_hurricane(urls):
    """
    Very simple procedure but requires using the ASGS nomenclature
    Only need to check one valid url
    """
    if not isinstance(urls, list):
        utilities.log.error('time: URLs must be in list form')
        sys.exit(1)
    for url in urls:
        try:
            words=url.split('/')
            state_hurricane = check_advisory(words[-6])
            break
        except IndexError as e:
            utilities.log.error('check_if_hurricane Uexpected failure try next:{}'.format(e))
    return state_hurricane

def convert_input_url_to_nowcast(urls):
    """
    Though one could call this method using a nowcast url, occasionally we want to be able to
    only pass a forecast type url and, from that, figure out what the corresponding nowcast url might be.
    This assume a proper ASGS formatted url and makes no attempts to validate the usefullness of
    the constructed url. Either it exists or this methiod exits(1)

    To use this feature:
    We mandate that the url is used to access ASGS data. The "ensemble" information will be in position .split('/')[-2]
    """
    if not isinstance(urls, list):
        utilities.log.error('nowcast: URLs must be in list form')
        sys.exit(1)
    newurls=list()
    for url in urls:
        urlwords=url.split('/')
        urlwords[-2]='nowcast'
        newurls.append('/'.join(urlwords))
    utilities.log.info('Modified input URL to be a nowcast type')
    return newurls

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

def process_adcirc_stations(urls, adcirc_stations, gridname, ensemble, metadata, data_product='water_level', resample_mins=0, fort63_style=False):
    # Fetch the data
    try:
        if data_product != 'water_level':
            utilities.log.error('ADCIRC data product can only be: water_level')
            sys.exit(1)
        adcirc = adcirc_fetch_data(adcirc_stations, urls, data_product, gridname=gridname, castType=ensemble.rstrip(), resample_mins=resample_mins, fort63_style=fort63_style)
        df_adcirc_data = adcirc.aggregate_station_data()
        df_adcirc_meta = adcirc.aggregate_station_metadata()
    except Exception as e:
        utilities.log.error('Error: ADCIRC: {}'.format(e))
    return df_adcirc_data, df_adcirc_meta 

def first_true(iterable, default=False, pred=None):
    """
    itertools recipe found in the Python 3 docs
    Returns the first true value in the iterable.
    If no true value is found, returns *default*
    If *pred* is not None, returns the first item
    for which pred(item) is true.

    first_true([a,b,c], x) --> a or b or c or x
    first_true([a,b], x, f) --> a if f(a) else b if f(b) else x
    """
    return next(filter(pred, iterable), default)

def strip_time_from_url(urls):
    """
    We mandate that the URLs input to this fetcher are those used to access the ASGS data. The "time" information will be in position .split('/')[-6]
    eg. 'http://tds.renci.org/thredds/dodsC/2021/nam/2021052318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.63.nc'
    
    Return time string in either ASGS formatted '%Y%m%d%H' or possibly as a hurricane advisory string (to be checked later)
    """
    url = grab_first_url_from_urllist(urls)
    try:
        words = url.split('/')
        ttime=words[-6] # Always count from the back. NOTE if a hurrican this could be an advisory number.
    except IndexError as e:
        utilities.log.error('strip_time_from_url Uexpected failure try next:{}'.format(e))
    return ttime

def strip_ensemble_from_url(urls):
    """
    We mandate that the URLs input to this fetcher are those used to access the ASGS data. The "ensemble" information will be in position .split('/')[-2]
    eg. 'http://tds.renci.org/thredds/dodsC/2021/nam/2021052318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.63.nc'
    
    Return:
        Ensemble string
    """
    url = grab_first_url_from_urllist(urls)
    try:
        words = url.split('/')
        ensemble=words[-2] # Usually nowcast,forecast, etc 
    except IndexError as e:
        utilities.log.error('strip_ensemble_from_url Uexpected failure try next:{}'.format(e))
    return ensemble

def strip_instance_from_url(urls):
    """
    We mandate that the URLs input to this fetcher are those used to access the ASGS data. The "instance" information will be in position .split('/')[-3]
    eg. 'http://tds.renci.org/thredds/dodsC/2021/nam/2021052318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.63.nc'
    
    Return:
        Instance string
    """
    url = grab_first_url_from_urllist(urls)
    try:
        words = url.split('/')
        instance=words[-3] # Usually nowcast,forecast, etc 
    except IndexError as e:
        utilities.log.error('strip_instance_from_url Uexpected failure try next:{}'.format(e))
    return instance 

def grab_gridname_from_url(urls):
    """
    We mandate that the URLs input to this fetcher are those used to access the ASGS data. The "grid" information will be in position .split('/')[-2]
    eg. 'http://tds.renci.org/thredds/dodsC/2021/nam/2021052318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.63.nc'
    
    Return:
        grid.upper() string
    """
    url = grab_first_url_from_urllist(urls)
    try:
        words = url.split('/')
        grid=words[-5] # Usually nowcast,forecast, etc 
    except IndexError as e:
        utilities.log.error('strip_gridname_from_url Uexpected failure try next:{}'.format(e))
    return grid.upper()

def grab_first_url_from_urllist(urls):
    """
    eg. 'http://tds.renci.org/thredds/dodsC/2021/nam/2021052318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.63.nc'
    
    Return:
    """
    if not isinstance(urls, list):
        utilities.log.error('first url: URLs must be in list form')
        sys.exit(1)
    url = first_true(urls)
    return url

def main(args):
    """
    We require the provided URL are using the typical ASGS nomenclature and that the timestamp is in ('/') position -6
    Moreover, This time stamp behaves a little different if fetching a nowcast versus a forecast. For now, we will
    annotate final .csv files with _TIME_ corresponding to the reported url starttime.
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

    urls = args.urls
    if urls==None:
        utilities.log.error('No URL was specified: Abort')
        sys.exit(1)

    if not isinstance(urls, list):
        utilities.log.error('urls: URLs must be in list form: Converting')
        urls = [urls]

    if args.convertToNowcast:
        utilities.log.info('Requested conversion to Nowcast')
        urls = convert_input_url_to_nowcast(urls)

    data_product = args.data_product
    if data_product != 'water_level':
        utilities.log.error('ADCIRC: Only available data product is water_level: {}'.format(data_product))
        sys.exit(1)
    else:
        utilities.log.info('Chosen data source {}'.format(data_source))

    # Check if this is a Hurricane
    if not check_if_hurricane(urls):
        utilities.log.error('URL is not a Hurricane advisory')
        #sys.exit(1)
        urltimeStr = strip_time_from_url(urls)
        urltime = dt.datetime.strptime(urltimeStr,'%Y%m%d%H')
        runtime=dt.datetime.strftime(urltime, dformat)
    else:
        utilities.log.error('URL is a Hurricane')
        urladvisory = strip_time_from_url(urls)
        runtime=urladvisory

    if args.fort63_style:
        utilities.log.info('Fort_63 style station inputs specified')

    ensemble = strip_ensemble_from_url(urls)  # Only need to check on of them
    gridname = grab_gridname_from_url(urls)   # ditto

    ##
    ## Start the processing
    ##

    #starttime='2021-12-08 12:00:00'
    utilities.log.info('Selected run time/Advisory range is {}'.format(runtime))

    # metadata are used to augment filename
    #ASGS
    if data_source.upper()=='ASGS':
        excludedStations=list()
        # Use default station list
        if args.fort63_style:
            adcirc_stations=get_adcirc_stations_fort63_style()
        else:
            adcirc_stations=get_adcirc_stations_fort61_style()

        adcirc_metadata='_'+ensemble+'_'+gridname.upper()+'_'+runtime.replace(' ','T')
        data, meta = process_adcirc_stations(urls, adcirc_stations, gridname, ensemble, adcirc_metadata, data_product, resample_mins=0, fort63_style=args.fort63_style)
        df_adcirc_data = format_data_frames(data)
        # Output 
        try:
            dataf=utilities.writeCsv(df_adcirc_data, rootdir=rootdir,subdir='',fileroot='adcirc_stationdata',iometadata=adcirc_metadata)
            metaf=utilities.writeCsv(meta, rootdir=rootdir,subdir='',fileroot='adcirc_stationdata_meta',iometadata=adcirc_metadata)
            utilities.log.info('ADCIRC data has been stored {},{}'.format(dataf,metaf))
        except Exception as e:
            utilities.log.error('Error: ADCIRC: Failed Write {}'.format(e))
            sys.exit(1)

    utilities.log.info('Finished with data source {}'.format(data_source))
    utilities.log.info('Finished')

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('--sources', action='store_true',
                        help='List currently supported data sources')
    parser.add_argument('--data_source', action='store', dest='data_source', default='ASGS', type=str,
                        help='choose supported data source: default = ASGS')
    parser.add_argument('--urls', nargs='+', action='store', dest='urls', default=None, type=str,
                        help='ASGS url to fetcb ADCIRC data')
    parser.add_argument('--data_product', action='store', dest='data_product', default='water_level', type=str,
                        help='choose supported data product: default is water_level')
    parser.add_argument('--convertToNowcast', action='store_true',
                        help='Attempts to force input URL into a nowcast url assuming normal ASGS conventions')
    parser.add_argument('--fort63_style', action='store_true', 
                        help='Boolean: Will inform Harvester to use fort.63.methods to get station nodesids')
    args = parser.parse_args()
    sys.exit(main(args))
