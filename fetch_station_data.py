#!/usr/bin/env python
#
# For each source: define the functions:
# fetch_single_data(station)-> pd.DataFrame
# fetch_single_metadata(station,periods)-> pd.DataFrame
# Each subclass needs to overwritye definitions for these two functions.
#

# We want to upload data to the DB every 15mins. This will require smoothing the data
# But also, I can see the River data is a mess. We will need to excluded stations with insufficient data.
#
# An attempt at a simple first pass at a fetch method for product data (primarily water_level)
#
# Note the df_meta method can be a little tricky to implement a proper format. 
#
import os,sys
import numpy as np
import pandas as pd
import xarray as xr
import datetime as dt
from datetime import timedelta
from utilities.utilities import utilities


#Contrails
import urllib
import urllib.parse
import requests
from requests.exceptions import ConnectionError
from requests.exceptions import Timeout
from requests.exceptions import HTTPError
import xmltodict

# NOAA/NOS
import noaa_coops as coops

# THREDDS (ADCIRC model)
#from get_adcirc.GetADCIRC import Adcirc, writeToJSON, get_water_levels63
import netCDF4 as nc4
import numpy as np
import numpy.ma as ma
import pandas as pd
from siphon.catalog import TDSCatalog
from collections import OrderedDict

GLOBAL_TIMEZONE='gmt' # Every source is set or presumed to return times in the zone

GLOBAL_FILL_VALUE='-99999'
UNITS='meters' # Now the code only applies to WL

def replaceAndFill(df):
    """
    Replace all Nans ans 'None" valuesa with GLOBAL_FILL_VALUE
    """
    df=df.fillna(GLOBAL_FILL_VALUE)
    return df

def stations_resample(df, sample_mins=None)->pd.DataFrame:
    """
    Resample (all stations) on a 15min (or other) basis

    NOTE: Final aggregated data still have flanked nans for some stations because
    The reported times might have been different. 

    Input:
        df: A time series x stations data frame
        sample_min. A numerical value for th enumber of mins to resample

    Output:
        df_out. New time series every 15mins x stations
    """
    timesample='15min'
    if sample_mins is not None:
        timesample=f'{sample_mins}min'
    utilities.log.info('Resampling freq set to {}'.format(timesample))
    dx=df.groupby(pd.Grouper(freq=timesample)).first().reset_index()
    return dx.set_index('TIME')

def stations_interpolate(df)->pd.DataFrame:
    """

    NOTE: Final aggregated data still have flanked nans for some stations because
    The reported times might have been different. 

    Input:
        df: A time series x stations data frame

    Output:
        df_out. New time series every 15mins x stations
    """
    utilities.log.info('Interpolating station data' )
    df.interpolate(method='polynomial', order=1, limit=1)
    return df 

class fetch_station_data(object):
    """
    We expect upon entry to this class a LIST of station dataframes (TIME vs PRODUCT)
    with TIME as datetime timestamps and a column of data of the desired units and with a column
    name of the station

   Default return products wil be on the sampling_mins frequency
    """
    def __init__(self, stations, periods, resample_mins=15):
        """
        stations:  A list of stations (str) or tuples of (station,adcirc node) (str,int) 
        periods: A list of tuples. [(time1,time2)]
        """
        self._stations=stations
        self._periods=periods
        self._resampling_mins=resample_mins

    def aggregate_station_data(self)->pd.DataFrame:
        """
        Loop over the list of stations and fetch the products. Then concatenate them info single dataframe

        nans now get converted to the value in GLOBAL_FILL_VALUE
        """
        aggregateData = list()
        excludedStations=list()
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        for station in self._stations:
            utilities.log.info(station)    
            try:
                dx = self.fetch_single_product(station, self._periods)
                dx_int = stations_interpolate(dx)
                aggregateData.append(stations_resample(dx_int, sample_mins=self._resampling_mins))
                #aggregateData.append(resample_and_interpolate(dx))
                #tm.sleep(2) # sleep 2 secs
                #print('Iterate: Kept station is {}'.format(station))
            except Exception as ex:
                excludedStations.append(station)
                message = template.format(type(ex).__name__, ex.args)
                utilities.log.warn('Error Value: Probably the station simply had no data; Skip {}, msg {}'.format(station, message))

        if len(aggregateData)==0:
            utilities.log.warn('No site data was found for the given site_id list. Perhaps the server is down or file doesnt exist')
            ##return np.nan
            #sys.exit(1) # Keep processing the remaining list
        utilities.log.info('{} Stations were excluded'.format(len(excludedStations)))
        utilities.log.info('{} Stations included'.format(len(aggregateData)))
        try:
            df_data = pd.concat(aggregateData, axis=1)
            idx = df_data.index
            utilities.log.info('Check for time duplicates')
            if idx.duplicated().any():
                utilities.log.info("Duplicated data times found . will keep first value(s) only")
                df_data = df_data.loc[~df_data.index.duplicated(keep='first')]
            if len(idx) != len(df_data.index):
                utilities.log.warning('had duplicate times {} {}'.format(len(idx),len(df_data.index)))
            # I have seen lots of nans coming from ADCIRC
            #df_data.dropna(how='all', axis=1, inplace=True)
            df_data = replaceAndFill(df_data)
        except Exception as e:
            utilities.log.error('Aggregate: error: {}'.format(e))
            ##df_data=np.nan
        print(df_data)
        return df_data

# TODO Need to sync with df_data
    def aggregate_station_metadata(self)->pd.DataFrame:
        """
        Loop over the list of stations and fetch the metadata. Then concatenate info single dataframe
        Transpose final data to have stations as index

        nans now get converted to the value in GLOBAL_FILL_VALUE
        """
        aggregateMetaData = list()
        excludedStations = list()
        template = "A metadata exception of type {0} occurred. Arguments:\n{1!r}"
        for station in self._stations:
            try:
                dx = self.fetch_single_metadata(station)
                aggregateMetaData.append(dx)
                #tm.sleep(2) # sleep 2 secs
                utilities.log.info('Iterate: Kept station is {}'.format(station))
            except Exception as ex:
                excludedStations.append(station)
                message = template.format(type(ex).__name__, ex.args)
                utilities.log.warn('Error Value: Metadata: {}, msg {}'.format(station, message))
        if len(aggregateMetaData)==0:
            utilities.log.warn('Metadata: No site data was found for the given site_id list. Perhaps the server is down Exit')
            #sys.exit(1) # Process remaining list
        utilities.log.info('{} Metadata Stations were excluded'.format(len(excludedStations)))
        df_meta = pd.concat(aggregateMetaData, axis=1).T
        #df_meta.dropna(how='all', axis=1, inplace=True)
        df_meta = replaceAndFill(df_meta)
        return df_meta

#####################################################################################
##
## Fetching the ADCIRC NODE data from TDS
## In contrast to NOAA and Contrails,
## We will pass in NOAA/Contrails stationids but we must query the fort.61.nc file 
##

## The one node at a time approach may not be a big deal as 1: It better conforms to accessinmg the other sources
## 2: It is a NC4 file which is lazyloading

## The fort61 file contains INTERPOLATED values of the solution to a specific lon,lat point.  
## For the fort.63, we are extracting time series from specific node numbers.  
## Unless the lon,lat point is very close to the specified node, these will be slightly different.

class adcirc_fetch_data(fetch_station_data):
    """
    Input:
        station_id_list: list of station_ids to pass to ADCIRC
        periods: list of valid ADCIRC urls tuples (*63.nc,*.61.nc) for aggregation 
    """
    # dict( persistant tag: source speciific tag )
    products={ 'water_level':'water_level'  # 6 min
            }

    def _removeEmptyURLPointers(self, in_periods):
        """
        Loop through the entire list and remove any entries that thorw a File Not Found error
        """
        new_periods=list()
        for url in in_periods:
            try:
                nc = nc4.Dataset(url)
                new_periods.append(url)
            except OSError as e:
                utilities.log.info('URL not found: Remove url {}'.format(url))
        return new_periods

    def typeADCIRCCast(self, url, df):
        """
        Compute the URL starttime value to the time range in df. Ascertain if this
        job is a forecast or a nowcast type.
        This is tricky since, eg, an ADCIRC file _might_ include times from a prior (failed) ASGS run. 
        But it will never have pre-forecasted results. So this is how we will check. Simply compare
        the last timeseries value to the url starttime. if timeseries > starttime than it is a forecast

        url TIME must reside in url.split('/')[-6]
        Assume the times series are ordered.

        Return:
              Either NOWCAST or FORECAST
        """
        timeseries = pd.to_datetime(df.index.astype(str)) # Account for ctime/calender changes in pandas. Thx !.
        starttime=url.split('/')[-6]
        try:
            urltime = dt.datetime.strptime(starttime,'%Y%m%d%H')
            if timeseries[-1] > urltime:
                return 'FORECAST'
            else:
                return 'NOWCAST'
        except ValueError:
            utilities.log.error('Found a Hurricane Advisory value: Assumes forecast {}'.format(starttime))
            return 'FORECAST'
        except IndexError as e:
            utilities.log.error('Error: {}'.format(e))
            sys.exit(1)


#TODO change name periods to urls
    def __init__(self, station_id_list, periods=None, product='water_level',
                datum='MSL', gridname='None', castType='None', resample_mins=15):
        self._product=product
        #self._interval=interval 
        self._units='metric'
        self._datum=datum
        periods=periods
        if castType.upper()=='None':
            utilities.log.info('ADCIRC: castType not set. Will result in poor metadata NAME value')
        self._castType=castType 
        if gridname=='None':
            utilities.log.info('ADCIRC: gridname not specified. Will result in poor metadata NAME value') 
        self._gridname=gridname
        available_stations = self._fetch_adcirc_nodes_from_stations(station_id_list, periods)
        if available_stations==None:
            utilities.log.error('No valid fort.61 files were found: Abort')
            #sys.exit(1)
        utilities.log.info('List of ADCIRC generated stations {}'.format(available_stations))
        periods = self._removeEmptyURLPointers(periods)
        super().__init__(available_stations, periods, resample_mins=resample_mins) # Pass in the full dict

    def _fetch_adcirc_nodes_from_stations(self, stations, periods) -> OrderedDict():
        """
        periods contains all the possible urls. We do this because TDS may or may not actually
        have one or more of the requested urls. So we keep checking urls for stations until no more
        urls exist. If none, then die.

        Input:
            station <str>. A list of (eg NOAA/Contrails) station ids
            periods <list>. The list of url-61 values. 

        Return: list of tuples (stationid,nodeid). Superfluous stationids are ignored
        """
        utilities.log.info('Attempt to find ADCIRC stations')
        for url61 in periods:
            utilities.log.info('Fetch stations: {} '.format(url61))
            try:
                ds = xr.open_dataset(url61)
                ds = ds.transpose()
                ds.attrs = []
                sn = ds['station_name'].values
                snn = []
                for i in range(len(sn)): # This gets the stationids IN THE FILE not necc what we requested.
                    ts = str(sn[i].strip().decode("utf-8"))
                    #print(ts)
                    snn.append(str(ts.split(' ')[0])) # Strips off actual name leaving only the leading id
                # Get intersection of input station ids and available ids
                snn_pruned=[str(x) for x in stations if x in snn]
                idx = list() # Build a list of tuples (stationid,nodeid)
                for ss in stations: # Loop over stations to maintain order
                    s = ss # Supposed to pull out the first word
                    # Cnvert to a try clause
                    if s in snn_pruned:
                        utilities.log.debug("{} is in list.".format(s))
                        idx.append( (s,snn.index(s)) )
                    else:
                        utilities.log.info("{} not in fort.61.nc station_name list".format(s))
                        ##sys.exit(1)
                return idx # Need this to get around loop structure in aggregate etch 
            except OSError:
                utilities.log.warn("Could not open/read a specific fort.61 URL. Try next iteration {}".format(url61))
            except Exception:
                utilities.log.error('Could not find ANY fort.61 urls from which to get stations lists')
                utilities.log.info('Bottomed out in _fetch_adcirc_nodes_from_stations')
                raise
        #return np.nan

##
## Criky. When I initially inserted nodelat/lon into meta, then subsequently updated COUNTY as np.nan
## The inserted "nan" was somehow, not a real nan and thus, the fillna() step would do nothing.
## No idea why at this time. Clearly another SNAFU driven by duck-typing issues. Jan 2022. Even though
## the nodelat/lon was actually a single element masked array (which I corrected) why would that impact
## The value of meta['COUNTY'] ?
##
## A Series of ARCIRC urls _may_ point to a url that doesn't exist.l It should have but sometimes not.
## So we pre-filter the url periods lists so no empties show up here
##
    def fetch_single_product(self, station_tuple, periods) -> pd.DataFrame:
        """
        Input:
            station_tuple (str,int). A tuple that maps stationid to the current ADCIRC-grid nodeid
            periods <list>. A url-61 values. 

       Return: dataframe of time (timestamps) vs values for the requested stationid
        """
        # First up. Get the list of available station ids

        station=station_tuple[0]
        node=station_tuple[1]
        datalist=list()
        typeCast_status=list() # Check each period to see if this was a nowcast or forecast type fetch. If mixed then abort
        for url in periods: # If a period is SHORT no data may be found esp for Contrails
            try:
                nc = nc4.Dataset(url)
            except OSError as e:
                utilities.log.error('URL not found should never happen here. Should have been prefiltered')
                sys.exit(1)
            # we need to test access to the netCDF variables, due to infrequent issues with
            # netCDF files written with v1.8 of HDF5.
            if "zeta" not in nc.variables.keys():
                print("zeta not found in netCDF for {}.".format(url))
                # okay to have a missing one  do not exit # sys.exit(1)
            else:
                time_var = nc.variables['time']
                t = nc4.num2date(time_var[:], time_var.units)
                data = np.empty([len(t), 0])
                try:
                    data = nc['zeta'][:,node] # Not the same as when reading fort.63 -1]
                except IndexError as e:
                    utilities.log.error('Error: This is usually caused by accessing non-hsofs data but forgetting to specify the proper --grid {}'.format(e))
                    #sys.exit()
                np.place(data, data < -1000, np.nan)
                dx = pd.DataFrame(data, columns=[str(node)], index=t)
                dx.columns=[station]
                dx.index.name='TIME'
                typeCast_status.append(self.typeADCIRCCast(url, dx))
                # Now some fudging to account for Pandas timestamp capability changes
                dx.index = pd.to_datetime(dx.index.astype(str)) # New pandas can only do this to strings now
                datalist.append(dx)
        try:
            df_data = pd.concat(datalist)
        except Exception as e:
            print('ADCIRC concat error: {}'.format(e))
        # Check if ALL entries in typeCast_status are the same. If not fail hard.
        if len(set(typeCast_status)) != 1:
            utilities.log.error('Some mix up with typeCast_status {}'.format(typeCast_status))
            sys.exit(1)
        self._typeCast = list(set(typeCast_status))[0] 
        utilities.log.info('ADCIRC type determined to be {}'.format(self._typeCast))
        return df_data

##
## The nodelat/nodelon objects are masked arrays. For a single node (as used here)
## the ma.getdata() returns an ndarray of shape=() but with the a single value.
## Imposing a float() onto that value converts it to a real float
##
## Try to determine if this was a nowcast or a forecast type dataset

    def fetch_single_metadata(self, station_tuple) -> pd.DataFrame:
        """
        Input:
            station <str>. A valid station id

        Return:
            dataframe of time (timestamps) vs values for the requested nodes 
        """
        station=station_tuple[0]
        node=station_tuple[1]
        meta=dict()
        periods=self._periods

        for url in periods:
            nc = nc4.Dataset(url)
            # we need to test access to the netCDF variables, due to infrequent issues with
            # netCDF files written with v1.8 of HDF5.
            try:
                nodelon=nc.variables['x'][node]
                nodelat=nc.variables['y'][node]
                break; # If we found it no need to check other urls
            except IndexError as e:
                utilities.log.error('Meta Error:{}'.format(e))
                #sys.exit()
        lat = float(ma.getdata(nodelat))
        lon = float(ma.getdata(nodelon))
        meta['LAT'] = lat 
        meta['LON'] = lon 
        # meta['NAME']= nc.agrid # Long form of grid name description # Or possible use nc.version
        meta['NAME']='_'.join([self._gridname.upper(),self._castType.upper()]) # These values come from the calling routine and should be usually nowcast, forecast
        #meta['VERSION'] = nc.version
        meta['UNITS'] ='meters'
        meta['TZ'] = GLOBAL_TIMEZONE # Can look in nc.comments
        meta['OWNER'] = nc.source
        meta['STATE'] = np.nan 
        meta['COUNTY'] = np.nan 
        df_meta=pd.DataFrame.from_dict(meta, orient='index')
        df_meta.columns = [str(station)]
        return df_meta

#####################################################################################
##
## Fetching the Station data from NOAA/NOS
##

class noaanos_fetch_data(fetch_station_data):
    """
    Input:
        station_id_list: list of NOAA station_ids <str>
        a tuple of (time_start, time_end) <str>,<str> format %Y-%m-%d %H:%M:%S
        a valid PRODUCT id <str>: hourly_height, water_level,predictions (tidal predictions)
        interval <str> set to 'h' returned hourly data, else 6min data

        NOTE: Default to using imperial units. Because the metadata that gets returned only reports
        the units for how the data were stored not fetched. So it wouid be easay for the calling program to get confused.
        Let the caller choose to update units and modify the df_meta structure prior to DB uploads
    """
    # dict( persistant tag: source specific tag )
    # products defines current products (as keys) and uses the value as a column header in the returned data set
    products={ 'water_level':'water_level',  # 6 min
               'predictions': 'predicted_wl', # 6 min
               'air_pressure': 'air_press',
               'hourly_height':'hourly_height', # hourly
               'wind':'spd'}

    def __init__(self, station_id_list, periods, product='water_level', interval=None, units='metric', 
                datum='MSL', resample_mins=15):
        """
        An interval value of None default to 6 mins. If choosing Tidal or Hourhy Height specify interval as h
        """
        try:
            self._product=product # product
            utilities.log.info('NOAA Fetching product {}'.format(self._product))
        except KeyError:
            utilities.log.error('NOAA/NOS No such product key. Input {}, Available {}'.format(product, self.products.keys()))
            sys.exit(1)
        else:
            self._interval=interval
        self._units='metric' # Redundant cleanup TODO
        self._datum=datum
        super().__init__(station_id_list, periods, resample_mins=resample_mins)

    def check_duplicate_time_entries(self, station, stationdata):
        """
        Sometimes station data comes back with multiple entries for a single time.
        Here we search for such dups and keep the FIRST one (Same as for ADDA)
        Choosing first was based on a single station and looking at the noaa coops website

        Parameters:
            station: str an individual stationID to check
            stationData: dataframe. Current list of all station product levels (from detailedIDlist) 
        Results:
            New dataframe containing no duplicate values
            multivalue. bool: True if duplicates were found 
        """
        multivalue = False
        idx = stationdata.index
        if idx.duplicated().any():
            utilities.log.info("Duplicated Obs data found for station {} will keep first value(s) only".format(str(station)))
            stationdata = stationdata.loc[~stationdata.index.duplicated(keep='first')]
            multivalue = True
        return stationdata, multivalue

# The weirdness with tstart/tend. Prior work by us indicated noaa coops reqs time formats of %Y%m%d %H:%M')
# Even though their website says otherwise (as of Oct 2021)

    def fetch_single_product(self, station, periods) -> pd.DataFrame:
        """
        For a single NOAA NOS site_id, process all the tuples from the input periods list
        and aggregate them into a dataframe with index pd.timestamps and a single column
        containing the desired product values. Rename the column to station id
        
        NOAA COOPS does not have the same time range constraints as Contrails. So this tuple list
        can, in fact, simply be the start and end time.
        
        Input:
            station <str>. A valid station id
            periods <list>. A list of tuples (<str>,<str>) denoting time ranges

        Return:
            dataframe of time (timestamps) vs values for the requested station
        """
        datalist=list()
        for tstart,tend in periods:
            utilities.log.info('NOAA/NOS:Iterate: start time is {}, end time is {}, station is {}'.format(tstart,tend,station))
            timein =  pd.Timestamp(tstart).strftime('%Y%m%d %H:%M')
            timeout =  pd.Timestamp(tend).strftime('%Y%m%d %H:%M')
            try:
                stationdata = pd.DataFrame()
                location = coops.Station(station)
                dx = location.get_data(begin_date=timein,
                                                end_date=timeout,
                                                product=self._product,
                                                datum=self._datum,
                                                units=self._units,
                                                interval=self._interval, # If none defaults to 6min
                                                time_zone=GLOBAL_TIMEZONE)[self.products[self._product]].to_frame()
                dx, multivalue = self.check_duplicate_time_entries(station, dx)
                # Put checks in here in case we want to exclude these stations with multiple values
                dx.reset_index(inplace=True)
                dx.set_index(['date_time'], inplace=True)
                dx.columns=[station]
                dx.index.name='TIME'
                dx.index = pd.to_datetime(dx.index)
                datalist.append(dx)
            except ConnectionError:
                utilities.log.error('Hard fail: Could not connect to COOPS for products {}'.format(station))
            except HTTPError:
                utilities.log.error('Hard fail: HTTP error to COOPS for products')
            except Timeout:
                utilities.log.error('Hard fail: Timeout')
            except Exception as e:
                utilities.log.error('NOAA/NOS data error: {} was {}'.format(e, self._product))
        try:
            df_data = pd.concat(datalist)
            df_data=df_data.astype(float)
        except Exception as e:
            utilities.log.error('NOAA/NOS concat error: {}'.format(e))
            df_data = np.nan
        return df_data

# TODO The NOAA metadata scheme is Horrible for what we need. This example is very tentative 
    def fetch_single_metadata(self, station) -> pd.DataFrame:
        """
        For a single NOAA site_id fetch the associated metadata.
        The choice of data is highly subjective at this time.

        Input:
             A valid station id <str>
        Return:
             dataframe of preselected metadata for a single station in the (keys,values) orientation

             This orientation facilitates aggregation upstream. Upstream will transpose this eventually
             to our preferred orientation with stations as index

        NOTE: DO not store a column entry with the stationid. Simply name the column station id, then take transpose.
        """
        meta=dict()
        try:
            location = coops.Station(station)
        except Exception as e:
            utilities.log.error('NOAA/NOS meta error: {}'.format(e))
        meta['LAT'] = location.metadata['lat'] if location.metadata['lat']!='' else np.nan
        meta['LON'] = location.metadata['lng'] if location.metadata['lng']!='' else np.nan
        meta['NAME'] =  location.metadata['name'] if location.metadata['name']!='' else np.nan
        meta['UNITS'] = UNITS # Manual override bcs -> location.sensors['units'] # This can DIFFER from the actual data. For data you can specify a transform to metric.
        #meta['ELEVATION'] = location['elevation']
        meta['TZ'] = GLOBAL_TIMEZONE
        meta['OWNER'] = 'NOAA/NOS'
        meta['STATE'] = location.metadata['state'] if location.metadata['state']!='' else np.nan
        meta['COUNTY'] = np.nan # None
        #
        df_meta=pd.DataFrame.from_dict(meta, orient='index')
        df_meta.columns = [str(station)]
        return df_meta

#####################################################################################
##
## Fetching the Station data from Contrails managed by OneRain
##

## Must MANUALLY convert to meters here

class contrails_fetch_data(fetch_station_data):
    """
    Input:
        station_id_list: list of station_ids <str>
        a tuple of (time_start, time_end) <str>,<str> format %Y-%m-%d %H:%M:%S
        a valid OWNER for Contrails <str>: One of NCDOT,Lake Lure,Asheville,Carolina Beach,
            Town of Cary,NCEM Synthetic,Wake County,
            USFWS,Morrisville,NCEM,USGS,NOAA,Currituck County,Duke Energy
        config: a dict containing values for domain <str>, method <str>, systemkey <str>
        a valid PRODUCT id <str>: See CLASSDICT definitions for specifics
    """
    # dict( persistant tag: source speciific tag )

# See Tom's email Regarding coastal versus river class values

    products={ 'river_water_level':'Stage', 'coastal_water_level':'Water Elevation'
            }

    CLASSDICT = {
        'Rain Increment':10,
        'Rain Accumulation':11,
        'Stage':20,            # River guages
        'Water Elevation':94,  # Coastal guages  - not documented in contrails doc as of today
        'Flow Volume':25,
        'Air Temperature':30,
        'Fuel Temperature':38,
        'Wind Velocity':40,
        'Wind Velocity, maximum':41,
        'Wind Direction':44,
        'ALERT Wind':47,
        'Relative Humidity':50,
        'Soil Moisture':51,
        'Fuel Moisture':52,
        'Barometric Pressure':53,
        'Net Solar Radiation':60,
        'Evapotranspiration Rate':84,
        'Binary Status':197,
        'Repeater Status':198,
        'Battery':199,
        'Average Voltage':200,
        'Repeater Pass List':240,
        'Msg Count':246,
    }
    # Here call a pipeline to do the fetch and then super the main class
    # We expect the calling metyhod to have resolved the different MAP terms for a given source
    # Currently only tested with the NCEM owner

    def __init__(self, station_id_list, periods, config, product='river_water_level', owner='NCEM', resample_mins=15):
        self._owner=owner
        try:
            self._product=self.products[product] # product
            #self.product_class=str(CLASSDICT[self._product]) # For now should eval to only 20 or 94.
        except KeyError:
            utilities.log.error('Contrails No such product key. Input {}, Available {}'.format(product, self.products.keys()))
            sys.exit(1)
        print(self._product)
        utilities.log.info('CONTRAILS Fetching product {}'.format(self._product))
        self._systemkey=config['systemkey']
        self._domain=config['domain']
        super().__init__(station_id_list, periods, resample_mins=resample_mins)

    def build_url_for_contrails_station(self, domain,systemkey,indict)->str:
        """
        Build a simple query for a single gauge and the product level values
        Parameters:
        Results:
        """
        url=domain
        url_values=urllib.parse.urlencode(indict)
        full_url = url +'?' +url_values
        return full_url

# We do a station at a time because the max number of rows returned is 5,000
# And the data for some stations is 6min
#
# For each source: define the functions:
# fetch_single_data(station)-> pd.DataFrame
# fetch_single_metadata(station,periods)-> pd.DataFrame
#
    def fetch_single_product(self, station, periods) -> pd.DataFrame: 
        """
        For a single Contrails site_id, process all tuples from the input periods list
        and aggregate them into a dataframe with index pd.timestamps and a single column
        containing the desired CLASSDICT[...] values. Rename the column to station id
        
        Input:
            station <str>. A valid station id
            periods <list>. A list of tuples (<str>,<str>) denoting time ranges

        Return:
            dataframe of time (timestamps) vs values for the requested station
        """
        METHOD = 'GetSensorData'
        datalist=list()
        for tstart,tend in periods:
            utilities.log.info('Iterate: start time is {}, end time is {}, station is {}'.format(tstart,tend,station))
            indict = {'method': METHOD, 'class': self.CLASSDICT[self._product],
                 'system_key': self._systemkey ,'site_id': station,
                 'tz': GLOBAL_TIMEZONE,
                 'data_start': tstart,'data_end': tend }
            url = self.build_url_for_contrails_station(self._domain,self._systemkey,indict)
            try:
                response = requests.get(url)
                dict_data = xmltodict.parse(response.content)
                data = dict_data['onerain']['response']['general']
                dx = pd.DataFrame(data['row']) # Will  be <= 5000
                dx = dx[['data_time','data_value']]
                utilities.log.info('Contrails. Converting to meters')
                dx.columns = ['TIME',station]
                dx.set_index('TIME',inplace=True)
                dx.index = pd.to_datetime(dx.index)
                datalist.append(dx)
            except Exception as e:
                utilities.log.warn('Contrails response data error: Perhaps empty data contribution: {}'.format(e))
        try:
            # Manually convert all values to meters
            df_data = pd.concat(datalist)
            utilities.log.info('Contrails. Converting to meters')
            df_data=df_data.astype(float) * 0.3048 # Convert to meters
        except Exception as e:
            utilities.log.error('Contrails failed concat: error: {}'.format(e))
            df_data=np.nan
        return df_data

# Note it is possible to get all station metadata but only a subset of station data
# According to oneRain the current best way to access the meta data is using or_site_id
#
# river and coastal metadata return different kind of objects
    def fetch_single_metadata(self, station) -> pd.DataFrame:      
        """
        For a single Contrails site_id fetch the associated metadata.
        Need to perform multiple queries to get the desired set of data. This is optional
        The caller will check the DB to see if this a new station requiring metadata

        Input:
             A valid station id <str>
        Return:
             dataframe of preselected metadata for a single station in the (keys,values) orientation

             This orientation facilitates aggregation upstream. Upstream will transpose this eventually
             to our preferred orientation with stations as index
        """
        # Something wrong with GTNN7 the second dict is way too big as a LIST. The list doesn't include GTNN7
        # Sent bug report to contrails.Dec 8, 2021
        # The second dict for GTNN7 contains LOTS of station's data which corrupts the algorithm
        # Solution: Switch to using or_site_id
        #
        # Needed a more extensive SensorMetaData approach
        meta=dict() 
        # 1
        METHOD = 'GetSensorMetaData'
        indict = {'method': METHOD,'tz':GLOBAL_TIMEZONE, 'class': self.CLASSDICT[self._product],
             'system_key': self._systemkey ,'site_id': station }
        url = self.build_url_for_contrails_station(self._domain,self._systemkey,indict)
        response = requests.get(url)
        dict_data = xmltodict.parse(response.content)
        data = dict_data['onerain']['response']['general']['row']

        if isinstance(data, list):
            for entry in data:
                if entry['sensor_class']==self.CLASSDICT[self._product]:
                    list_sensor_id = entry['or_sensor_id'] # Yes a list of ordered dicts.
                    or_site_id = entry['or_site_id']
                    break
        else:
            or_site_id= data['or_site_id']
        utilities.log.info('Current or_site_id is {}'.format(or_site_id))
        # 2
        METHOD = 'GetSiteMetaData'
        #indict = {'method': METHOD,'tz':GLOBAL_TIMEZONE, 'class': self.CLASSDICT[self._product],
        #     'system_key': self._systemkey ,'site_id': station }
        indict = {'method': METHOD,'tz':GLOBAL_TIMEZONE, 'class': self.CLASSDICT[self._product],
             'system_key': self._systemkey ,'or_site_id': or_site_id }
        url = self.build_url_for_contrails_station(self._domain,self._systemkey,indict)
        try:
            response = requests.get(url)
        except Exception as e:
            utilities.log.error('Contrails response meta error: {}'.format(e))
        dict_data = xmltodict.parse(response.content)
        data2 = dict_data['onerain']['response']['general']['row']
        # Gets here but then fails hard and returns for GTNN7
        meta['LAT'] = data2['latitude_dec'] if data2['latitude_dec'] !='' else np.nan
        meta['LON'] = data2['longitude_dec'] if data2['longitude_dec'] !='' else np.nan
        meta['NAME'] = data['location'] if data2['location'] !='' else np.nan
        meta['UNITS'] = UNITS # Manual override bcs -> data['units'].replace('.','') # I have seen . in some labels
        meta['TZ'] = GLOBAL_TIMEZONE # data['utc_offset']
        ###meta['ELEVATION'] = data['elevation']
        meta['OWNER'] = self._owner # data2 always returns the value=DEPRECATED data2['owner']
        meta['STATE'] = np.nan # None # data2['state']  # DO these work ?
        meta['COUNTY'] = np.nan
        #
        df_meta=pd.DataFrame.from_dict(meta, orient='index')
        df_meta.columns = [str(station)] 
        return df_meta
