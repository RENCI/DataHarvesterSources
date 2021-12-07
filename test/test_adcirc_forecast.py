import os,sys
import pandas as pd
from fetch_station_data import adcirc_fetch_data
from utilities.utilities import utilities as utilities

main_config = utilities.load_config()

metadata = '_ADCIRC_TEST'
rootdir=utilities.fetchBasedir(main_config['DEFAULT']['RDIR'], basedirExtra='')


#
# Read the stationlist data NOTE: It is up to you to ensure the nodelist is compatible with the grid implied by the url
#
basedir='/projects/sequence_analysis/vol1/prediction_work/ADCIRCSupportTools/ADCIRCSupportTools/config'
#df_meta_temp=pd.read_csv(f'{basedir}/ec95d_stations.V1.csv',index_col=0, header=0, skiprows=[1], sep=',') # From AST
df_meta_temp=pd.read_csv(f'{basedir}/CERA_NOAA_HSOFS_stations_V2.csv',index_col=0, header=0, skiprows=[1], sep=',') # From AST
df_meta_temp.set_index('stationid',inplace=True)
df_meta_temp.index = df_meta_temp.index.map(str) # Prefer using ids only as strings
stations=df_meta_temp.index.to_list()
#stations=df_meta_temp['Node'].to_list()

print(stations)
#stations=[('8449130',1411509),('8413320',1162475),('8418150',1176249)]

#
# Set up a PERIODS list URLs
#

periods=[
     'http://tds.renci.org:8080/thredds/dodsC/2021/nam/2021061300/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast/fort.61.nc',
     'http://tds.renci.org:8080/thredds/dodsC/2021/nam/2021061306/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast/fort.61.nc',
     'http://tds.renci.org:8080/thredds/dodsC/2021/nam/2021061312/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast/fort.61.nc',
     'http://tds.renci.org:8080/thredds/dodsC/2021/nam/2021061318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast/fort.61.nc'
    ]

adcirc = adcirc_fetch_data(stations, periods, 'water_level')

df_data = adcirc.aggregate_station_data()
df_meta = adcirc.aggregate_station_metadata()


df_adcirc_data.index = df_adcirc_data.index.strftime('%Y-%m-%dT%H:%M:%S')
df_adcirc_data.reset_index(inplace=True)
df_adcirc_data_out=pd.melt(df_adcirc_data, id_vars=['TIME'])
df_adcirc_data_out.columns=('TIME','STATION',product)
#
df_adcirc_meta.index.name='STATION'
#df_adcirc_meta.reset_index(inplace=True)

print(df_adcirc_data)
print(df_adcirc_meta)

metadata='_2021061300_2021061318_namforecast'
outdata=utilities.writeCsv(df_adcirc_data_out, rootdir=rootdir,subdir='',fileroot='adcirc_stationdata',iometadata=metadata)
outdatameta=utilities.writeCsv(df_adcirc_meta, rootdir=rootdir,subdir='',fileroot='adcirc_stationdata_meta',iometadata=metadata)
utilities.log.info('Wrote pipeline data {} and metadata {}'.format(outdata, outdatameta))



