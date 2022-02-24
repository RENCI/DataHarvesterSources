#!/usr/bin/env python
#
#

import os,sys
from argparse import ArgumentParser
from utilities.utilities import utilities as utilities

parser = ArgumentParser()
parser.add_argument('--sources', action='store_true',
                    help='List currently supported data sources')
parser.add_argument('--data_source', action='store', dest='data_source', default='ASGS', type=str,
                    help='choose supported data source: default = ASGS')
parser.add_argument('--urls', action='store', dest='urls', default=None, type=str,
                    help='ASGS url to fetcb ADCIRC data')
parser.add_argument('--data_product', action='store', dest='data_product', default='water_level', type=str,
                    help='choose supported data product: default is water_level')
parser.add_argument('--convertToNowcast', action='store_true',
                    help='Attempts to force input URL into a nowcast url assuming normal ASGS conventions')
parser.add_argument('--fort63_style', action='store_true',
                    help='Boolean: Will inform Harvester to use fort.63.methods to get station nodesids')
#print(sys.argv[1:])
args = parser.parse_args()
#argList=sys.argv[1:]

import fetch_adcirc_data

# NOTE we change args name because jobs can run concurrently

try:
    fetch_adcirc_data.main(args) 
except Exception as e:
    utilities.log.error('FORECAST Fail. {}'.format(e))

# Now ADD the --convertToNowcast value to the argList and rerun
#argList.append("--convertToNowcast") 

args2 = args
args2.convertToNowcast=True

try:
    fetch_adcirc_data.main(args2)
except Exception as e:
    utilities.log.error('NOWCAST Fail. {}'.format(e))
    sys.exit(1)

utilities.log.info('Finished')


