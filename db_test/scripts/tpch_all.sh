#!/bin/bash

BASEDIR="`realpath "$(dirname "$0")"`"
cd $BASEDIR

./tpch_abtree.sh
./tpch_bernoulli.sh
./tpch_baseline.sh
