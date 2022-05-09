#!/bin/bash

BASEDIR="$(realpath "`dirname "$0"`")"
cd "$BASEDIR"

dropdb -h localhost test_abtree
dropdb -h localhost test_btree
createdb -h localhost test_abtree
createdb -h localhost test_btree
psql -h localhost test_abtree < create_table.sql
psql -h localhost test_btree < create_table_bt.sql
