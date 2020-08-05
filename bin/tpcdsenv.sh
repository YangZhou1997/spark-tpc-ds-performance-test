#!/bin/bash
#
# tpcdsenv.sh - UNIX Environment Setup
#

#######################################################################
# This is a mandatory parameter. Please provide the location of
# spark installation.
#######################################################################
export SPARK_HOME=$SPARK_HOME

#######################################################################
# Script environment parameters. When they are not set the script
# defaults to paths relative from the script directory.
#######################################################################

export TPCDS_ROOT_DIR=
export TPCDS_LOG_DIR=
export TPCDS_DBNAME="TPCDS100"
export TPCDS_WORK_DIR=
export TPCDS_GENDATA_DIR="hdfs:///tmp/tpcds100_raw"
export TPCDS_FORMAT="parquet"

export ADDITION_SPARK_OPTIONS=
