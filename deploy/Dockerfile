##############
# Docker file for the creation of the DataHarvester tools.
#
# to create image: docker build -t data_harverster:latest .
# to push image:
#       docker tag data_harverster:latest renciorg/data_harverster:latest
#       docker push renciorg/data_harverster:latest
##############

FROM continuumio/miniconda3

# get some credit
LABEL maintainer="jtilson@renci.org"

# make sure the container is up-to-date
RUN apt-get update

# Start off getting Python 3.7.3
RUN conda install  -y python=3.8

# update conda and set the download channel
RUN conda update conda && \
    conda config --add channels conda-forge

# tell conda what the shell is
RUN conda init bash

# make d directory for the repos and go there
RUN mkdir /repo
WORKDIR /repo

# get the repos

RUN git clone https://github.com/RENCI/DataHarvesterSources.git
RUN git clone https://github.com/GClunies/noaa_coops.git

WORKDIR DataHarvesterSources/deploy

# install the conda requirements
RUN conda install --yes --file requirements.txt
                              
# change to the noaa directory
WORKDIR /repo/noaa_coops

# run the noaa seteup
RUN python setup.py install --user
                        
# change to the pipelines directory                        
WORKDIR /repo/DataHarvesterSources
                                   
# make a temporary directory for the output.
# this should eventually point a common PV
RUN mkdir temp

# set the python path
ENV PYTHONPATH=/repo/DataHarvesterSources

# set the location of the output directory
ENV RUNTIMEDIR=/data

##########
# at this point the container is ready to accept the launch command.
##########
