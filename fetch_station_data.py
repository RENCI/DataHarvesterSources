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
import pandas as pd
from siphon.catalog import TDSCatalog
from collections import OrderedDict

GLOBAL_TIMEZONE='gmt' # Every source is set or presumed to return times in the zone

def resample_and_interpolate(df, sample_mins=None)->pd.DataFrame:
    """
    Resample (all stations) on a 15min (or other) basis
    Interpolate nans. Leftover First nans in the series (if any)  
    get bfilled()  and pad()) to clean up leading/trailing nans

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
    dx.interpolate(method='polynomial', order=1)
    dx.fillna(method='bfill',inplace=True) # Any leading nans
    dx.fillna(method='pad',inplace=True) # any trailing nans
    return dx.set_index('TIME')

class fetch_station_data(object):
    """
    We expect upon entry to this class a LIST of station dataframes (TIME vs PRODUCT)
    with TIME as datetime timestamps and a column of data of the desired units and with a column
    name of the station
    """
    def __init__(self, stations, periods):
        """
        stations:  A list of stations (str) or tuples of (station,adcirc node) (str,int) 
        periods: A list of tuples. [(time1,time2)]
        """
        self._stations=stations
        self._periods=periods

    def aggregate_station_data(self)->pd.DataFrame:
        """
        Loop over the list of stations and fetch the products. Then concatenate them info single dataframe
        """
        aggregateData = list()
        excludedStations=list()
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        for station in self._stations:
            print(station)    
            try:
                dx = self.fetch_single_product(station, self._periods)
                aggregateData.append(resample_and_interpolate(dx))
                #tm.sleep(2) # sleep 2 secs
                #print('Iterate: Kept station is {}'.format(station))
            except Exception as ex:
                excludedStations.append(station)
                message = template.format(type(ex).__name__, ex.args)
                utilities.log.warn('Error Value: Probably the station simply had no data; Skip {}, msg {}'.format(station, message))
        if len(aggregateData)==0:
            utilities.log.warn('No site data was found for the given site_id list. Perhaps the server is down Exit')
            #sys.exit(1) # Keep processing the remaining list
        utilities.log.info('{} Stations were excluded'.format(len(excludedStations)))
        utilities.log.info('{} Stations included'.format(len(aggregateData)))
        df_data = pd.concat(aggregateData, axis=1)
        # I have seen loits of nans coming from ADCIRC
        #df_data.dropna(how='all', axis=1, inplace=True)
        return df_data

# TODO Need to sync with df_data
    def aggregate_station_metadata(self)->pd.DataFrame:
        """
        Loop over the list of stations and fetch the metadata. Then concatenate info single dataframe
        Transpose final data to have stations as index
        """
        aggregateMetaData = list()
        excludedStations = list()
        template = "A metadata exception of type {0} occurred. Arguments:\n{1!r}"
        for station in self._stations:
            try:
                dx = self.fetch_single_metadata(station)
                aggregateMetaData.append(dx)
                #print(dx)
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

#TODO change name periods to urls
    def __init__(self, station_id_list, periods=None, product='water_level',
                datum='MSL'):
        self._product=product
        #self._interval=interval 
        self._units='metric'
        self._datum=datum
        self._periods=periods
        available_stations = self._fetch_adcirc_nodes_from_stations(station_id_list, periods[0])
        utilities.log.info('List of generated stations {}'.format(available_stations))
        super().__init__(available_stations, periods) # Pass in the full dict

    def _fetch_adcirc_nodes_from_stations(self, stations, period) -> OrderedDict():
        """
        Input:
            station <str>. A list of NOAA/Contrails station ids
            periods <list>. A url-61 value. 

        Return: list of tuples (stationid,nodeid). Superfluous stationids are ignored
        """
        url61 = period # Only look at a single fort.61.nc file is needed for now
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
        idx = list() # Bukld a list of tuples (stationid,nodeid)
        for ss in stations: # Loop over stations to maintain order
            s = ss # Supposed to pull out the first word
            # Cnvert ot a try clause
            if s in snn_pruned:
                utilities.log.debug("{} is in list.".format(s))
                idx.append( (s,snn.index(s)) )
            else:
                utilities.log.info("{} not in fort.61.nc station_name list".format(s))
                #sys.exit(1)
        return idx # Need this to get around loop structure in aggregate etch 

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
        for url in periods:
            utilities.log.info('ADCIRC url {}'.format(url))
            nc = nc4.Dataset(url)
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
                df_data = pd.DataFrame(data, columns=[str(node)], index=t)
                df_data.columns=[station]
                df_data.index.name='TIME'
                datalist.append(df_data)
        try:
            df_data = pd.concat(datalist)
        except Exception as e:
            utilities.log.error('ADCIRC concat error: {}'.format(e))
        return df_data

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
            lons = nc.variables['x']
            lats=nc.variables['y']
            # we need to test access to the netCDF variables, due to infrequent issues with
            # netCDF files written with v1.8 of HDF5.
            try:
                data = nc['zeta'][:, node]
                nodelon=nc.variables['x'][node]
                nodelat=nc.variables['y'][node]
            except IndexError as e:
                utilities.log.error('Meta Error:{}'.format(e))
                #sys.exit()
        meta['LAT'] = nodelat
        meta['LON'] = nodelon
        meta['NAME']= nc.description # Or possible use nc.version
        meta['VERSION'] = nc.version
        meta['UNITS'] ='metric'
        meta['TZ'] = GLOBAL_TIMEZONE # Can look in nc.comments
        meta['OWNER'] = nc.source
        meta['STATE'] = None
        meta['COUNTY'] = None
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
    # dict( persistant tag: source speciific tag )
    products={ 'water_level':'water_level'  # 6 min
            }

    def __init__(self, station_id_list, periods, product='water_level', interval=None, # units='metric', 
                datum='MSL'):
        try:
            self._product=self.products[product] # product
        except KeyError:
            utilities.log.error('NOAA/NOS No such product key. Input {}, Available {}'.format(product, self.products.keys()))
            sys.exit(1)

        self._interval=interval
        #self._units=units # Do not set this becaseu subsequent metadata calls will only return the native units not what you set here.
        self._datum=datum
        super().__init__(station_id_list, periods)

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
                                                #units=self._units,
                                                interval=self._interval, # If none defaults to 6min
                                                time_zone=GLOBAL_TIMEZONE)[self._product].to_frame()
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
                utilities.log.error('NOAA/NOS data error: {}'.format(e))
        try:
            df_data = pd.concat(datalist)
        except Exception as e:
            utilities.log.error('NOAA/NOS concat error: {}'.format(e))
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
        meta['LAT'] = location.metadata['lat']
        meta['LON'] = location.metadata['lng']
        meta['UNITS'] = location.sensors['units'] # This can DIFFER from the actual data. For data you can specify a transform to metric.
        meta['NAME'] =  location.metadata['name']
        #meta['ELEVATION'] = location['elevation']
        meta['TZ'] = GLOBAL_TIMEZONE
        meta['OWNER'] = 'NOAA/NOS'
        meta['STATE'] = location.metadata['state']
        meta['COUNTY'] = None
        df_meta=pd.DataFrame.from_dict(meta, orient='index')
        df_meta.columns = [str(station)]
        return df_meta

#####################################################################################
##
## Fetching the Station data from Contrails managed by OneRain
##

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
    products={ 'water_level':'Stage'  # 6 min
            }

    CLASSDICT = {
        'Rain Increment':10,
        'Rain Accumulation':11,
        'Stage':20,
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

    def __init__(self, station_id_list, periods, config, product='water_level', owner='NCEM'):
        self._owner=owner
        try:
            self._product=self.products[product] # product
        except KeyError:
            utilities.log.error('Contrails No such product key. Input {}, Avaibale {}'.format(product, self.products.keys()))
            sys.exit(1)
        print(self._product)
        self._systemkey=config['systemkey']
        self._domain=config['domain']
        super().__init__(station_id_list, periods)

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
            except Exception as e:
                utilities.log.error('Contrails response data error: {}'.format(e))
            dict_data = xmltodict.parse(response.content)
            data = dict_data['onerain']['response']['general']
            dx = pd.DataFrame(data['row']) # Will  be <= 5000
            dx = dx[['data_time','data_value']]
            dx.columns = ['TIME',station]
            dx.set_index('TIME',inplace=True)
            dx.index = pd.to_datetime(dx.index)
            dx = dx.astype(float) # need to do this later if plotting. So do it now.
            datalist.append(dx)
        try:
            df_data = pd.concat(datalist)
        except Exception as e:
            utilities.log.error('Contrails concat error: {}'.format(e))
        return df_data

# Note it is possible to get all station metadata but only a subset of station data
# According to oneRain the current best way to access the meta data is using or_site_id
#
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
        # Switch to using or_site_id
        meta=dict() 
        # 1
        METHOD = 'GetSensorMetaData'
        indict = {'method': METHOD,'tz':GLOBAL_TIMEZONE, 'class': self.CLASSDICT[self._product],
             'system_key': self._systemkey ,'site_id': station }
        url = self.build_url_for_contrails_station(self._domain,self._systemkey,indict)
        response = requests.get(url)
        dict_data = xmltodict.parse(response.content)
        data = dict_data['onerain']['response']['general']['row']
        meta['NAME'] = data['location']
        ##meta['STATION'] = data['site_id'] # Do not add here will cause problems
        meta['SENSOR'] = data['sensor_id'] 
        meta['PRODUCT'] = data['description']
        meta['UNITS'] = data['units'].replace('.','') # I have seen . in some labels
        meta['TZ'] = GLOBAL_TIMEZONE # data['utc_offset']
        or_site_id= data['or_site_id'] 
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
        data = dict_data['onerain']['response']['general']['row']
        # Gets here but then fails hard and returns for GTNN7
        meta['LAT'] = data['latitude_dec']
        meta['LON'] = data['longitude_dec']
        ###meta['ELEVATION'] = data['elevation']
        meta['OWNER'] = data['owner']
        meta['COUNTY'] = None # data['county']
        meta['STATE'] = None # data['state']
        df_meta=pd.DataFrame.from_dict(meta, orient='index')
        df_meta.columns = [str(station)] 
        return df_meta
