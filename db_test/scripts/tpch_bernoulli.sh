#!/bin/bash

BASEDIR="`realpath "$(dirname "$0")"`"
cd $BASEDIR

BUFSIZE=128MB
BUILD="baseline"
src_data=/mnt/ssd1/zyzhao/loaded_data/tpch_100g_btree_baseline_build
src_pgwal=/mnt/ssd1/zyzhao/loaded_data/tpch_100g_btree_baseline_build_pg_wal

. ./switch_pg.sh $BUILD

echo "copying data"
dst_data=$PGDATA
dst_pgwal=/mnt/ssd2/zyzhao/baseline_pg_wal

rm -rf $dst_data &
rm -rf $dst_pgwal &
wait
cp -r $src_data $dst_data &
cp -r $src_pgwal $dst_pgwal &
wait
echo "copy done"
echo "shared_buffers = $BUFSIZE" >> $dst_data/postgresql.conf
ln -s $dst_pgwal $dst_data/pg_wal

echo "starting postgresql"
numactl --cpubind=0 --membind=0 pg_ctl start

echo "wait 5 seconds"
sleep 5 
echo "warm up..."
tmpfile=`mktemp`
echo "y" | ./run test.BernoulliTPCHQuery tpch_100g 1 0.0055 1 1234567 $tmpfile
rm -f $tmpfile

PREFIX="logs/tpch_bernoulli_${BUFSIZE}_warm"
./run test.MixedTPCHInsertAndBernoulli tpch_100g 600000 10 13141234 75000 \
	3 0.011 20 0 ${PREFIX}.meter | tee ${PREFIX}.log

echo "cleaning up..."
sleep 5
pg_ctl stop
sleep 2
rm -rf $dst_data &
rm -rf $dst_pgwal &

wait

