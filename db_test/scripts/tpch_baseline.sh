#!/bin/bash

BASEDIR="`realpath "$(dirname "$0")"`"
cd $BASEDIR

BUILD="baseline"
src_data=/mnt/ssd1/zyzhao/loaded_data/tpch_100g_abtree_baseline_build
src_pgwal=/mnt/ssd1/zyzhao/loaded_data/tpch_100g_abtree_baseline_build_pg_wal

. ./switch_pg.sh $BUILD

echo "copying data"
dst_data=$PGDATA
dst_pgwal=/mnt/ssd2/zyzhao/release_pg_wal

rm -rf $dst_data &
rm -rf $dst_pgwal &
wait
cp -r $src_data $dst_data &
cp -r $src_pgwal $dst_pgwal &
wait
echo "copy done"

BUFSIZE=128MB
echo "shared_buffers = $BUFSIZE" >> $dst_data/postgresql.conf
ln -s $dst_pgwal $dst_data/pg_wal

echo "starting postgresql"
numactl --cpubind=0 --membind=0 pg_ctl start

echo "wait 5 seconds"
sleep 5 
echo "warm up..."
tmpfile=`mktemp`
./run test.SWRTPCHQuery tpch_100g 300 10000 10 1234567 $tmpfile
rm -f $tmpfile

PREFIX="logs/tpch_abtree_baseline_${BUFSIZE}_warm"
./run test.MixedTPCHInsertAndSWRQuery -i tpch_100g 600000 10 13141234 20000 \
	500 10000 20 0 ${PREFIX}.meter | tee ${PREFIX}.log

echo "cleaning up..."
pg_ctl stop
sleep 2
rm -rf $dst_data &
rm -rf $dst_pgwal &

wait

