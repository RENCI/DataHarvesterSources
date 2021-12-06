import os,sys
import pandas as pd
from fetch_station_data import contrails_fetch_data
from utilities.utilities import utilities as utilities

#
# Setup authorization codes and setup the dict
#
domain='http://contrail.nc.gov:8080/OneRain/DataAPI'
#method='GetSensorData'
systemkey = '20cebc91-5838-49b1-ab01-701324161aa8'

config={'domain':'http://contrail.nc.gov:8080/OneRain/DataAPI', 
        'systemkey':'20cebc91-5838-49b1-ab01-701324161aa8'}


main_config = utilities.load_config()
rootdir=utilities.fetchBasedir(main_config['DEFAULT']['RDIR'], basedirExtra='')

#
# Read the stationlist data
#

##basedir='/projects/sequence_analysis/vol1/prediction_work/RIVERS_Contrails/PRELIMINARY_INFO'
##df_meta=pd.read_csv(f'{basedir}/FIMAN_NCEM_Coastal_Lonpruned_68.csv') # Toms original data
##df_meta.set_index('SITE_ID',inplace=True)
#df_meta.set_index = df_meta.set_index.map(str)
##stations=df_meta.index.to_list()

stations=['30069', '30054', '30055', '30039', '30006', '30033', '30065', '30029', '30052', '30012', 'WNRN7', '30003', '30050', 'EWPN7', '35370047744601', '30048', '30009', 'GTNN7', '30032', 'MINN7', '30011', 'EGHN7', '30016', 'WSNN7', 'HSWN7', '30008', '30030', 'HBKN7', 'JYCN7', '30010', 'WDVN7', '30031', '30001', 'ORLN7', 'VNCN7', '30060', 'JCKN7', 'ALIN7', '30062', 'CTIN7', 'OCAN7', 'EMWN7', 'BLHN7', 'PNGN7', '30059', 'ROFN7', 'COLN7', '30042', '30002', '30064', '30053', 'WHSN7', '30061', '30058', '30017', 'STYN7', 'STON7', 'GRMN7', 'RMBN7', 'TRTN7', '30015', '30007']

#stations=['GTNN7','30032','BLHN7','30012']

#
# Set up a PERIODS list of tuples
#

periods = [('2021-09-01 03:00:30', '2021-09-01 23:59:59'), ('2021-09-02 00:00:00', '2021-09-02 23:59:59'), ('2021-09-03 00:00:00', '2021-09-03 23:59:59'), ('2021-09-04 00:00:00', '2021-09-04 00:30:00')]

contrails = contrails_fetch_data(stations, periods, config, 'water_level', 'NCEM')

df_data = contrails.aggregate_station_data()
df_meta = contrails.aggregate_station_metadata()

print('Finished')
print(df_data)
print(df_meta)

metadata = '_CONTRAILS_METADATA'

outdata=utilities.writeCsv(df_data, rootdir=rootdir,subdir='',fileroot='contrails_stationdata',iometadata=metadata)
outdatameta=utilities.writeCsv(df_meta, rootdir=rootdir,subdir='',fileroot='contrails_stationdata_meta',iometadata=metadata)
utilities.log.info('Wrote pipeline data {} and metadata {}'.format(outdata, outdatameta))



