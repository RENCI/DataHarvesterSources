#!/usr/bin/env python

#############################################################
#
# RENCI 2020
#############################################################

import datetime as dt
import numpy as np
import pandas as pd
import sys,os
import yaml
import logging
import json
from argparse import ArgumentParser

LOGGER = None

class Utilities:
    """
    """
    def __init__(self, instanceid=None):
        """
        Initialize the Utilities class, set up logging
        """
        global LOGGER
        self.config = self.load_config()

        if LOGGER is None and self.config["DEFAULT"]["LOGGING"]:
            log = self.initialize_logging(instanceid)
            LOGGER = log
        self.log = LOGGER

#############################################################
# Logging

    def initialize_logging(self, instanceid=None):
        """
        Initialize project logging
        instanceid is a subdirectory to be created under LOG_PATH
        """
        # logger = logging.getLogger(__name__)
        logger = logging.getLogger("dataharvester_services") # We could simply add the instanceid here as well
        log_level = self.config["DEFAULT"].get('LOGLEVEL', 'DEBUG')
        # log_level = getattr(logging, self.config["DEFAULT"].get('LOGLEVEL', 'DEBUG'))
        logger.setLevel(log_level)

        # LogFile = self.config['LOG_FILE']
        # LogFile = '{}.{}.log'.format(thisDomain, currentdatecycle.cdc)
        #LogFile = 'log'
        #LogFile = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))
        if instanceid is not None:
            Logdir = '/'.join([os.getenv('LOG_PATH','.'),instanceid])
        else:
            Logdir = os.getenv('LOG_PATH','.') 
        #LogName =os.getenv('LOG_NAME','logs')
        LogName='DataHavester.log'
        LogFile='/'.join([Logdir,LogName])
        self.LogFile = LogFile

        # print('Use a log filename of '+LogFile)
        formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(funcName)s : %(module)s : %(name)s : %(message)s ')
        dirname = os.path.dirname(LogFile)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname)
        file_handler = logging.FileHandler(LogFile, mode='w')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # logging stream
        # formatter = logging.Formatter('%(asctime)s - %(process)d - %(name)s - %(module)s:%(lineno)d - %(levelname)s - %(message)s')
        # stream_handler = logging.StreamHandler()
        # stream_handler.setFormatter(formatter)
        # logger.addHandler(stream_handler)

        return logger

#############################################################
# YAML
    def load_config(self, yaml_file=os.path.join(os.path.dirname(__file__), '../config', 'main.yml')):
        #yaml_file = os.path.join(os.path.dirname(__file__), "../config/", "main.yml")
        if not os.path.exists(yaml_file):
            raise IOError("Failed to load yaml config file {}".format(yaml_file))
        with open(yaml_file, 'r') as stream:
            config = yaml.safe_load(stream)
            print('Opened yaml file {}'.format(yaml_file,))
        self.config = config
        return config

#############################################################
# IO uses the base YAML config to do its work

    def fetchBasedir(self, inconfig, basedirExtra='None'):
        try:
            rundir = os.environ[inconfig.replace('$', '')]  # Yaml call to be subsequently removed
        except:
            print('Chosen basedir invalid: '+str(inconfig['DEFAULT']['RDIR']))
            print('reset to CWD')
            rundir = os.getcwd()
        if basedirExtra is not None:
            rundir = rundir+'/'+basedirExtra
            if not os.path.exists(rundir):
                #print("Create high level Cycle dir space at "+rundir)
                try:
                    os.makedirs(rundir)
                except OSError:
                    sys.exit("Creation of the high level run directory %s failed" % rundir)
        return rundir

    def setBasedir(self, indir, basedirExtra=None):
        if basedirExtra is not None:
            indir = indir+'/'+basedirExtra
        if not os.path.exists(indir):
            #print("Create high level Cycle dir space at "+rundir)
            try:
                #os.mkdir(rundir)
                os.makedirs(indir)
            except OSError:
                sys.exit("Creation of the high level run directory %s failed" % indir)
        return indir

    def getSubdirectoryFileName(self, basedir, subdir, fname ):
        """Check and existance of and construct filenames for 
        storing the image data. basedir/subdir/filename 
        subdir is created as needed.
        """
        # print(basedir)
        if not os.path.exists(basedir):
            try:
                os.makedirs(basedir)
            except OSError:
                sys.exit("Creation of the basedir %s failed" % basedir)
        fulldir = os.path.join(basedir, subdir)
        if not os.path.exists(fulldir):
            #print("Create datastation dir space at "+fulldir)
            try:
                os.makedirs(fulldir)
            except OSError:
                #sys.exit("Creation of the directory %s failed" % fulldir)
                if not os.path.isdir(fulldir): 
                    sys.exit("Creation of the directory %s failed" % fulldir)
                    raise
                utilities.log.warn('mkdirs reports couldnt make directory. butmight be a race condition that can be ignored')
            #else:
            #    print("Successfully created the directory %s " % fulldir)
        return os.path.join(fulldir, fname)

    def writePickle(self, df, rootdir='.',subdir='obspkl',fileroot='filename',iometadata='Nometadata'):
        """ 
        Returns full filename for capture
        """
        newfilename=None
        try:
            mdir = rootdir
            newfilename = self.getSubdirectoryFileName(mdir, subdir, fileroot+iometadata+'.pkl')
            df.to_pickle(newfilename)
            print('Wrote pickle file {}'.format(newfilename))
        except IOError:
            raise IOerror("Failed to write PKL file %s" % (newfilename))
        return newfilename

    def writeCsv(self, df, rootdir='.',subdir='obspkl',fileroot='filename',iometadata='Nometadata'):
        """
        Write out current self.excludeList to disk as a csv
        output to rootdir/obspkl/.
        """
        newfilename=None
        try:
            mdir = rootdir
            newfilename = self.getSubdirectoryFileName(mdir, subdir, fileroot+iometadata+'.csv')
            df.to_csv(newfilename)
            print('Wrote CSV file {}'.format(newfilename))
        except IOError:
            raise IOerror("Failed to write file %s" % (newfilename))
        return newfilename

utilities = Utilities()

