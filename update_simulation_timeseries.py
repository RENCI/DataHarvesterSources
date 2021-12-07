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
from datetime import datetime as dt

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

# 
# A convenience function
#
def get_noaa_stations(grid=None):
    """
    A convenience method to fetch stations lists. These lisrs were derived form those used for APSVIZ2 work
    If an invalid grid is provided, a None is returned. This is okay since we may want to skip this and only get
    Contrails data
    """
    # choose the HSOFS list that includes the islands
    if grid=='hsofs':
        noaa_stations=['2695540', '8410140', '8411060', '8413320', '8418150', '8419317', '8423898', '8443970', '8447386', '8447435', '8447930', '8449130', '8452660', '8452944', '8454000', '8454049', '8461490', '8465705', '8467150', '8510560', '8516945', '8518750', '8518962', '8519483', '8531680', '8534720', '8536110', '8537121', '8539094', '8540433', '8545240', '8548989', '8551762', '8551910', '8555889', '8557380', '8570283', '8571421', '8571892', '8573364', '8573927', '8574680', '8575512', '8577330', '8594900', '8631044', '8632200', '8635027', '8635750', '8636580', '8637689', '8638610', '8638901', '8639348', '8651370', '8652587', '8654467', '8656483', '8658120', '8658163', '8661070', '8662245', '8665530', '8670870', '8720030', '8720218', '8720219', '8720226', '8720357', '8720625', '8721604', '8722670', '8722956', '8723214', '8723970', '8724580', '8725110', '8725520', '8726384', '8726520', '8726607', '8726667', '8726724', '8727520', '8728690', '8729108', '8729210', '8729840', '8732828', '8735180', '8735391', '8735523', '8736897', '8737048', '8737138', '8738043', '8739803', '8740166', '8741041', '8741533', '8747437', '8760721', '8760922', '8761305', '8761724', '8761927', '8761955', '8762075', '8762482', '8762483', '8764044', '8764227', '8764314', '8766072', '8767816', '8767961', '8768094', '8770570', '8770613', '8770822', '8771013', '8771450', '8772447', '8772471', '8773767', '8774770', '8775241', '8775870', '8779748', '8779770', '9751364', '9751381', '9751401', '9751639', '9752235', '9752695', '9755371', '9759110', '9759394', '9759938', '9761115']
    elif grid=='ec95d':
        noaa_stations=['8410140', '8447930', '8510560', '8516945', '8518750', '8531680', '8534720', '8638901', '8651370', '8658163', '8661070', '8723214', '8723970', '8724580', '8725110', '8726724', '8727520', '8729210', '8729840', '8747437', '8761724', '8764314', '8768094', '8770822', '8772471', '8775870']
    elif grid=='region3':
        noaa_stations=['8410140', '8413320', '8418150', '8419317', '8443970', '8447930', '8449130', '8461490', '8465705', '8467150', '8510560', '8516945', '8531680', '8534720', '8536110', '8555889', '8551910', '8574680', '8557380', '8570283', '8635750', '8638901', '8651370', '8656483', '8658163', '8661070', '8665530', '8670870']
    else:
        print('No NOAA/NOS grid/stations specified: skip')
        noaa_stations=None
    return noaa_stations

def get_contrails_stations(grid=None):
    """
    A convenience method to fetch river guage lists. 
    Contrails data
    """
#basedir='/projects/sequence_analysis/vol1/prediction_work/RIVERS_Contrails/PRELIMINARY_INFO'
#df_meta=pd.read_csv(f'{basedir}/FIMAN_NCEM_Coastal_Lonpruned_68.csv') # Toms original data
    contrails_stations=['30069', '30054', '30055', '30039', '30006', '30033', '30065', '30029', '30052', '30012', 'WNRN7', '30003', '30050', 'EWPN7', '35370047744601', '30048', '30009', 'GTNN7', '30032', 'MINN7', '30011', 'EGHN7', '30016', 'WSNN7', 'HSWN7', '30008', '30030', 'HBKN7', 'JYCN7', '30010', 'WDVN7', '30031', '30001', 'ORLN7', 'VNCN7', '30060', 'JCKN7', 'ALIN7', '30062', 'CTIN7', 'OCAN7', 'EMWN7', 'BLHN7', 'PNGN7', '30059', 'ROFN7', 'COLN7', '30042', '30002', '30064', '30053', 'WHSN7', '30061', '30058', '30017', 'STYN7', 'STON7', 'GRMN7', 'RMBN7', 'TRTN7', '30015', '30007']
    return contrails_stations

def get_adcirc_stations(grid=None):
    """
    Currently hard-coded set of valid nodes
    """
    adcirc_stations=['2695540', '8410140', '8411060', '8413320', '8418150', '8419317', '8423898', '8443970', '8447386', '8447435', '8447930', '8449130', '8452660', '8452944', '8454000', '8454049', '8461490', '8465705', '8467150', '8510560', '8516945', '8518750', '8518962', '8519483', '8531680', '8534720', '8536110', '8537121', '8539094', '8540433', '8545240', '8548989', '8551762', '8551910', '8555889', '8557380', '8570283', '8571421', '8571892', '8573364', '8573927', '8574680', '8575512', '8577330', '8594900', '8631044', '8632200', '8635027', '8635750', '8636580', '8637689', '8638610', '8638901', '8639348', '8651370', '8652587', '8654467', '8656483', '8658120', '8658163', '8661070', '8662245', '8665530', '8670870', '8720030', '8720218', '8720219', '8720226', '8720357', '8720625', '8721604', '8722670', '8722956', '8723214', '8723970', '8724580', '8725110', '8725520', '8726384', '8726520', '8726607', '8726667', '8726724', '8727520', '8728690', '8729108', '8729210', '8729840', '8732828', '8735180', '8735391', '8735523', '8736897', '8737048', '8737138', '8738043', '8739803', '8740166', '8741041', '8741533', '8747437', '8760721', '8760922', '8761305', '8761724', '8761927', '8761955', '8762075', '8762482', '8762483', '8764044', '8764227', '8764314', '8766072', '8767816', '8767961', '8768094', '8770570', '8770613', '8770822', '8771013', '8771450', '8772447', '8772471', '8773767', '8774770', '8775241', '8775870', '8779748', '8779770', '9751364', '9751381', '9751401', '9751639', '9752235', '9752695', '9755371', '9759110', '9759394', '9759938', '9761115']
    urls=[
         'http://tds.renci.org:8080/thredds/dodsC/2021/nam/2021061300/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.61.nc',
         'http://tds.renci.org:8080/thredds/dodsC/2021/nam/2021061306/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.61.nc',
         'http://tds.renci.org:8080/thredds/dodsC/2021/nam/2021061312/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.61.nc',
         'http://tds.renci.org:8080/thredds/dodsC/2021/nam/2021061318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.61.nc'
         ]
    return adcirc_stations, urls

def get_adcirc_forcast_stations(grid=None):
    """
    Currently hard-coded set of valid nodes
    """
    adcirc_stations=['2695540', '8410140', '8411060', '8413320', '8418150', '8419317', '8423898', '8443970', '8447386', '8447435', '8447930', '8449130', '8452660', '8452944', '8454000', '8454049', '8461490', '8465705', '8467150', '8510560', '8516945', '8518750', '8518962', '8519483', '8531680', '8534720', '8536110', '8537121', '8539094', '8540433', '8545240', '8548989', '8551762', '8551910', '8555889', '8557380', '8570283', '8571421', '8571892', '8573364', '8573927', '8574680', '8575512', '8577330', '8594900', '8631044', '8632200', '8635027', '8635750', '8636580', '8637689', '8638610', '8638901', '8639348', '8651370', '8652587', '8654467', '8656483', '8658120', '8658163', '8661070', '8662245', '8665530', '8670870', '8720030', '8720218', '8720219', '8720226', '8720357', '8720625', '8721604', '8722670', '8722956', '8723214', '8723970', '8724580', '8725110', '8725520', '8726384', '8726520', '8726607', '8726667', '8726724', '8727520', '8728690', '8729108', '8729210', '8729840', '8732828', '8735180', '8735391', '8735523', '8736897', '8737048', '8737138', '8738043', '8739803', '8740166', '8741041', '8741533', '8747437', '8760721', '8760922', '8761305', '8761724', '8761927', '8761955', '8762075', '8762482', '8762483', '8764044', '8764227', '8764314', '8766072', '8767816', '8767961', '8768094', '8770570', '8770613', '8770822', '8771013', '8771450', '8772447', '8772471', '8773767', '8774770', '8775241', '8775870', '8779748', '8779770', '9751364', '9751381', '9751401', '9751639', '9752235', '9752695', '9755371', '9759110', '9759394', '9759938', '9761115']
    urls=[
         'http://tds.renci.org:8080/thredds/dodsC/2021/nam/2021061300/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast/fort.61.nc',
         'http://tds.renci.org:8080/thredds/dodsC/2021/nam/2021061306/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast/fort.61.nc',
         'http://tds.renci.org:8080/thredds/dodsC/2021/nam/2021061312/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast/fort.61.nc',
         'http://tds.renci.org:8080/thredds/dodsC/2021/nam/2021061318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast/fort.61.nc'
         ]
    return adcirc_stations, urls


##
## End functions
##

GLOBAL_TIMEZONE='gmt' # Every source is set or presumed to return times in the zone
PRODUCT='water_level'

grid='hsofs'
print('Choosing NOAA/NOS, Contrails, and ADCIRC stations for the grid {}'.format(grid))

##
## Get default stations
##
noaa_stations=get_noaa_stations(grid)
contrails_stations=get_contrails_stations(grid)
adcirc_stations,urls=get_adcirc_stations(grid)
adcirc_stations_fc,urls_fc=get_adcirc_forecast_stations(grid)

#Contrails
domain='http://contrail.nc.gov:8080/OneRain/DataAPI'
systemkey = '20cebc91-5838-49b1-ab01-701324161aa8'
config={'domain':'http://contrail.nc.gov:8080/OneRain/DataAPI',
        'systemkey':'20cebc91-5838-49b1-ab01-701324161aa8'}

## Loop over times. Need these in datetime format to use timedelta. But then convert back to string for the methods.

numsteps=5 # Number of times to increment and repeat

for istep in range(numsteps):
    tstart = dt.fromisoformat('2021-06-13')+timedelta(days=istep*3)
    tstop = dt.fromisoformat('2021-06-17')+timedelta(days=istep*3)
    #time_start='2021-09-01 00:00:30'
    #time_end='2021-09-07 00:00:00'
    time_start = tstart.strftime('%Y-%m-%d %H:%M:%S')
    time_end = tstop.strftime('%Y-%m-%d %H:%M:%S')
    #
    time_range=[(time_start,time_end)] # Can be directly used by NOAA 
    periods=return_list_of_daily_timeranges(time_range[0])
    metadata = '_'+time_start.replace(' ','T')+'_'+time_end.replace(' ','T')
    #NOAA/NOS
    noaanos = noaanos_fetch_data(noaa_stations, time_range, 'water_level')
    df_noaa_data = noaanos.aggregate_station_data()
    df_noaa_meta = noaanos.aggregate_station_metadata()
    df_noaa_data.index = df_noaa_data.index.strftime('%Y-%m-%dT%H:%M:%S')
    df_noaa_data.reset_index(inplace=True)
    df_noaa_data_out=pd.melt(df_noaa_data, id_vars=['TIME'])
    df_noaa_data_out.columns=('TIME','STATION',product)
    print(df_noaa_data_out)
    df_noaa_meta.index.name='STATION'
    #Contrails
    contrails = contrails_fetch_data(contrails_stations, periods, config, 'water_level', 'NCEM')
    df_contrails_data = contrails.aggregate_station_data()
    df_contrails_meta = contrails.aggregate_station_metadata()
    df_contrails_data.index = df_contrails_data.index.strftime('%Y-%m-%dT%H:%M:%S')
    df_contrails_data.reset_index(inplace=True)
    df_contrails_data_out=pd.melt(df_contrails_data, id_vars=['TIME'])
    df_contrails_data_out.columns=('TIME','STATION',product)
    df_contrails_meta.index.name='STATION'
    df_contrails_meta.reset_index(inplace=True)

    # ADCIRC
    adcirc = adcirc_fetch_data(adcirc_stations, urls, 'water_level')
    df_adcirc_data = adcirc.aggregate_station_data()
    df_adcirc_meta = adcirc.aggregate_station_metadata()

    df_adcirc_data.index = df_adcirc_data.index.strftime('%Y-%m-%dT%H:%M:%S')
    df_adcirc_data.reset_index(inplace=True)
    df_adcirc_data_out=pd.melt(df_adcirc_data, id_vars=['TIME'])
    df_adcirc_data_out.columns=('TIME','STATION',product)
    df_adcirc_meta.index.name='STATION'
    df_adcirc_meta.reset_index(inplace=True)

    # ADCIRC
    adcirc_fc = get_adcirc_forcast_stations(adcirc_stations_fc, urls_fc, 'water_level')
    df_adcirc_fc_data = adcirc.aggregate_station_data()
    df_adcirc_frc_meta = adcirc.aggregate_station_metadata()

    df_adcirc_fc_data.index = df_fc_adcirc_data.index.strftime('%Y-%m-%dT%H:%M:%S')
    df_adcirc_fc_data.reset_index(inplace=True)
    df_adcirc_fc_data_out=pd.melt(df_fc_adcirc_data, id_vars=['TIME'])
    df_adcirc_fc_data_out.columns=('TIME','STATION',product)
    df_adcirc_fc_meta.index.name='STATION'
    df_adcirc_fc_meta.reset_index(inplace=True)


    # Save the files
    noaafile=utilities.writeCsv(df_noaa_data_out, rootdir=rootdir,subdir='',fileroot='noaa_stationdata',iometadata=metadata)
    noaametafile=utilities.writeCsv(df_noaa_meta, rootdir=rootdir,subdir='',fileroot='noaa_stationdata_meta',iometadata=metadata)
    #
    contrailsfile=utilities.writeCsv(df_contrails_data_out, rootdir=rootdir,subdir='',fileroot='contrails_stationdata',iometadata=metadata)
    contrailsmetafile=utilities.writeCsv(df_contrails_meta, rootdir=rootdir,subdir='',fileroot='contrails_stationdata_meta',iometadata=metadata)
    #
    adcircfile=utilities.writeCsv(df_adcirc_data_out, rootdir=rootdir,subdir='',fileroot='adcirc_stationdata_nowcast',iometadata=metadata)
    adcircmetafile=utilities.writeCsv(df_adcirc_meta, rootdir=rootdir,subdir='',fileroot='adcirc_stationdata_meta_nowcast',iometadata=metadata)
    #
    adcircfile_fc=utilities.writeCsv(df_adcirc_fc_data_out, rootdir=rootdir,subdir='',fileroot='adcirc_stationdata_namforecast',iometadata=metadata)
    adcircmetafile_fc=utilities.writeCsv(df_adcirc_fc_meta, rootdir=rootdir,subdir='',fileroot='adcirc_stationdata_meta_namforecast',iometadata=metadata)

print('Finished')
