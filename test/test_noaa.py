import os,sys
import pandas as pd
from fetch_station_data import noaanos_fetch_data
from utilities.utilities import utilities as utilities

main_config = utilities.load_config()
rootdir=utilities.fetchBasedir(main_config['DEFAULT']['RDIR'], basedirExtra='')


#
# Read the stationlist data
# This meta data is not supposed to be a substitute for the real metadata 
basedir='/projects/sequence_analysis/vol1/prediction_work/ADCIRCSupportTools/ADCIRCSupportTools/config'
#df_meta_temp=pd.read_csv(f'{basedir}/CERA_NOAA_HSOFS_stations_V3.csv',index_col=0, header=0, skiprows=[1], sep=',') # From AST
df_meta_temp=pd.read_csv(f'{basedir}/Region3_Stations_V2.csv',index_col=0, header=0, skiprows=[1], sep=',') # From AST
df_meta_temp.set_index('stationid',inplace=True)
df_meta_temp.index = df_meta_temp.index.map(str) # Prefer using ids only as strings
stations=df_meta_temp.index.to_list()
print(stations)

#stations=['8410140','8447930','8510560','8516945']

#
# Set up a PERIODS list of tuples
#

#periods = [('2021-09-01 03:00:30', '2021-09-01 23:59:59'), ('2021-09-02 00:00:00', '2021-09-02 23:59:59'), ('2021-09-03 00:00:00', '2021-09-03 23:59:59'), ('2021-09-04 00:00:00', '2021-09-04 00:30:00')]

time_start='2021-09-01 03:00:30'
time_stop='2021-09-04 00:30:00'

metadata = '_'+time_start.replace(' ','T')+'_'+time_stop.replace(' ','T')


periods = [(time_start,time_stop)]

noaanos = noaanos_fetch_data(stations, periods, product='water_level') # only 'water_level' for now
df_data = noaanos.aggregate_station_data()
df_meta = noaanos.aggregate_station_metadata()

print(df_data)
print(df_meta)

metadata = '_'+time_start.replace(' ','T')+'_'+time_stop.replace(' ','T')
rootdir=utilities.fetchBasedir(main_config['DEFAULT']['RDIR'], basedirExtra='')

outdata=utilities.writeCsv(df_data, rootdir=rootdir,subdir='',fileroot='noaa_stationdata',iometadata=metadata)
outdatameta=utilities.writeCsv(df_meta, rootdir=rootdir,subdir='',fileroot='noaa_stationdata_meta',iometadata=metadata)
utilities.log.info('Wrote pipeline data {} and metadata {}'.format(outdata, outdatameta))


