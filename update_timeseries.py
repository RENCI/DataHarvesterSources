#!/usr/bin/env python
#
# An example code that could be run to periodically update noaa/nos and contrails data
# For now, we will exclude ADCIRC TDS updates
#
# We include here a method to break up the time range into a list of tuples of smaller ranges
# Mostly this is only required for the contrails data fetches
#
# An attempt at a simple first pass at a fetch method for product data (primarily water_level)
#
# THis code is simply to guide discussions of how to best coordinate getting these data into
# the DH datebase
#

import os,sys
import pandas as pd
import datetime as dt
import math
from datetime import timedelta
from fetch_station_data import noaanos_fetch_data, contrails_fetch_data

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
    
    oneSecond=dt.timedelta(seconds=1) # An update interval shift

    subrange_start = time_start   
    while subrange_start < time_end:
        interval = dt.timedelta(hours=init_hour, minutes=init_min, seconds=init_sec)
        subrange_end=min(subrange_start+interval,time_end) # Need a variable interval to prevent a day-span  
        periods.append( (dt.datetime.strftime(subrange_start,dformat),dt.datetime.strftime(subrange_end,dformat)) )
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
    if grid=='hsofs':
        adcirc_nodes=['1491931', '1034613', '1247724', '1162475', '1176249', '1182888', '1215810', '991751', '1118012', '1338907', '1268115', '1411509', '1274635', '1018562', '1023972', '1176624', '1052139', '824413', '730946', '1395009', '950489', '1266674', '220876', '1247182', '1449501', '1562085', '1545989', '1175222', '652061', '1046551', '785629', '586346', '1403854', '1382714', '1508886', '1540535', '1579004', '1741191', '1718135', '1651552', '1538961', '1643487', '1683429', '1747983', '1769108', '1601183', '1675414', '1761652', '1753697', '1734199', '1675751', '1658501', '1667208', '1629769', '1592136', '1573505', '1507409', '1416194', '1164661', '1405214', '1350289', '1350201', '1236589', '1165583', '1152522', '1212537', '1010215', '876106', '790929', '968360', '1251971', '1302999', '1213435', '1160390', '843056', '805177', '677402', '567622', '537236', '557139', '455411', '317586', '642196', '594808', '469899', '483347', '549873', '506871', '400012', '551721', '267278', '340841', '215102', '189089', '181930', '441352', '498715', '485841', '498791', '490189', '310024', '598673', '644552', '296041', '593088', '96279', '6135', '616002', '218438', '68258', '282070', '526365', '580635', '554724', '393400', '388984', '518411', '466673', '130181', '493081', '239845', '448841', '436049', '449717', '435844', '312041', '435338', '435421', '476631', '422458', '1161509', '1207819', '1141995', '1154898', '1194451', '1045264', '1303782', '1174500', '1240231', '1357922', '1214468']
    elif grid=='ec95d':
        adcirc_nodes=['30474', '28465', '27646', '30245', '29497', '28519', '27226', '28312', '26644', '24265', '25020', '10515', '6295', '4449', '6734', '6348', '8303', '6935', '10572', '10300', '4138', '3758', '3774', '3753', '2970', '3369']
    elif grid=='region3':
        adcirc_nodes=['1051378', '960216', '1012396', '1778634', '1783467', '908778', '871780', '975291', '1786035', '1136533', '922825', '1227099', '879383', '964479', '72477', '477738', '7231', '755769', '406904', '49664', '920362', '1861105', '876307', '917990', '927369', '913115', '908263', '989537']
    else:
        adcirc_nodes=None
    return adcirc_nodes

##
## End functions
##

##
## Specify basic global data parameters
##

GLOBAL_TIMEZONE='gmt' # Every source is set or presumed to return times in the zone
PRODUCT='water_level'

##
## Misc settings
## Maybe store the data off into iRODS for now.
##

##
## Choose the overall time range (inclusive).
## For NOAA we do need modify but for COntrails we must breakup into dailies.
##
time_start='2021-09-01 03:00:30'
time_end='2021-09-04 00:30:00'

time_range=[(time_start,time_end)] # Can be directly used by NOAA 
periods=return_list_of_daily_timeranges(time_range[0])

##
## Input requests. We specify a VALID list of stations (for coops or contrails) 
## Technically, they do not need to be VALID as the underlying methods will eventually
## Remove invalids BUT, lots of time (eg making url calls) could be wasted in doing so.
## or nodes for ADCIRC
## Generally these will be constants

grid='hsofs'
print('Choosing NOAA/NOS, Contrails, and ADCIRC stations for the grid {}'.format(grid))

noaa_stations=get_noaa_stations(grid)
contrails_stations=get_contrails_stations(grid)
adcirc_stations=get_adcirc_stations(grid)

##
## Start grinding out the data
##

# TODO refactor the time_range arguments

#NOAA/NOS
noaanos = noaanos_fetch_data(noaa_stations, time_range, 'water_level')
df_noaa_data = noaanos.aggregate_station_data()
df_noaa_meta = noaanos.aggregate_station_metadata()

#Contrails
config = utilities.load_config('./secrets/contrails.yml')['DEFAULT']

#
contrails = contrails_fetch_data(contrails_stations, periods, config, 'water_level', 'NCEM')
df_contrails_data = contrails.aggregate_station_data()
df_contrails_meta = contrails.aggregate_station_metadata()












