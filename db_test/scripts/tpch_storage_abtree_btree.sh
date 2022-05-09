#!/bin/bash

BASEDIR="`realpath "$(dirname "$0")"`"
cd $BASEDIR

. ./switch_pg.sh release

initdb
cp /mnt/ssd1/zyzhao/loaded_data/abtree_1B/postgresql.conf $PGDATA/
echo "shared_buffers = 32GB" >> $PGDATA/postgresql.conf

pg_ctl start

echo "sleeping 5 seconds"
sleep 5

createdb tpch
echo "sleeping 1 second"
sleep 1


psql -f ../sql/tpch/createtable.sql tpch
psql -f ../sql/tpch/create_abtree.sql tpch
psql -f ../sql/tpch/create_btree.sql tpch
DBID="`psql -t --csv -c "select oid from pg_database where datname = 'tpch'" tpch`"
TABID="`psql -t --csv -c "select oid from pg_class where relname = 'lineitem'" tpch`"
ABTID="`psql -t --csv -c "select oid from pg_class where relname = 'lineitem_abtree'" tpch`"
BTID="`psql -t --csv -c "select oid from pg_class where relname = 'lineitem_btree'" tpch`"
pg_ctl stop

sleep 1

echo $DBID
echo $TABID
echo $ABTID
echo $BTID

cd "$PGDATA/base/$DBID"
for OID in $TABID $ABTID $BTID; do
	SZ=$(ls -al ${OID}* | awk 'BEGIN{ s = 0; }{s += $5;}END{ print s;}')
	echo "disk_usage $OID $SZ"
done

cd "$BASEDIR"

for ((i = 1; i <= 10; ++i)); do
	echo "Loading the $i th chunk of lineitem"
	pg_ctl start
	echo "sleeping 2 seconds"
	sleep 2
	
	psql -c "drop index if exists lineitem_abtree" tpch
	psql -c "drop index if exists lineitem_btree" tpch

	psql -c "COPY lineitem FROM '/home/zyzhao/tpch_data/tpch_2_17_0/dbgen/lineitem.tbl.$i' DELIMITER '|';" tpch

	sleep 1
	psql -f ../sql/tpch/create_abtree.sql tpch
	sleep 1
	psql -f ../sql/tpch/create_btree.sql tpch
	sleep 1

	ABTID="`psql -t --csv -c "select oid from pg_class where relname = 'lineitem_abtree'" tpch`"
	BTID="`psql -t --csv -c "select oid from pg_class where relname = 'lineitem_btree'" tpch`"

	pg_ctl stop
	sleep 1

	echo $DBID
	echo $TABID
	echo $ABTID
	echo $BTID

	cd "$PGDATA/base/$DBID"
	for OID in $TABID $ABTID $BTID; do
		SZ=$(ls -al ${OID}* | awk 'BEGIN{ s = 0; }{s += $5;}END{ print s;}')
		echo "disk_usage $OID $SZ"
	done

	cd "$BASEDIR"
done

rm -rf $PGDATA

