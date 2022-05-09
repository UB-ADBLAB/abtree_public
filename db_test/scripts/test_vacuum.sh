#!/bin/bash

BASEDIR="`realpath "$(dirname "$0")"`"
cd $BASEDIR

if [ $# -lt 3 ]; then
	echo "usage: $0 <tree> <delete_seed> <nsample_threads>"
	exit 1
fi

TREE=$1
DELETE_SEED=$2
NSAMPLE_THREAD=$3
BUFSIZE=32GB

if [ "x$TREE" = "xabtree" ]; then
	BUILD="release"
	src_data=/mnt/ssd1/zyzhao/loaded_data/abtree_10M
	src_pgwal=/mnt/ssd1/zyzhao/loaded_data/abtree_pg_wal_10M
#elif [ x"$TREE" = "xbaseline" ]; then
#	BUILD="baseline"
#	src_data=/mnt/ssd1/zyzhao/loaded_data/baseline_1B
#	src_pgwal=/mnt/ssd1/zyzhao/loaded_data/baseline_pg_wal_1B
else
	echo "Invalid tree name: $TREE"
	exit 2
fi

. ./switch_pg.sh $BUILD

echo "copying data"
dst_data=$PGDATA
dst_pgwal=/mnt/ssd2/zyzhao/pg_wal

rm -rf $dst_data &
rm -rf $dst_pgwal &
wait
cp -r $src_data $dst_data &
cp -r $src_pgwal $dst_pgwal &
wait
echo "copy done"

echo "shared_buffers = $BUFSIZE" >> $dst_data/postgresql.conf
echo "autovacuum = off" >> $dst_data/postgresql.conf
ln -s $dst_pgwal $dst_data/pg_wal

#drop_cache

echo "starting postgresql"
pg_ctl start

echo "wait 5 seconds"
sleep 5 

echo "testing..."
PREFIX="logs/test_vacuum_sample_t${NSAMPLE_THREAD}"
./run test.MixedWorkload -i test_abtree 0 0 0 0 0 10000 ${NSAMPLE_THREAD}\
	0 0 0 120 ${PREFIX}.meter | tee ${PREFIX}.log &

echo "sleep 60 seconds for the DB to warm up"
sleep 60

echo "delete 5% of the DB"
PREFIX="logs/test_vacuum_delete_seed${DELETE_SEED}"
echo y | ./run test.DeleteManyRandom test_abtree 500000 1 $DELETE_SEED \
	${PREFIX}.meter | tee ${PREFIX}.log

echo "sleep another 5 seconds"
sleep 5

echo "vacuum verbose A"
echo "vacuum verbose A;" | psql test_abtree 2>&1 | tee -a ${PREFIX}.log

echo "wait for sampling to finish"
wait

echo "cleaning up..."
pg_ctl stop
sleep 2
#rm -rf $dst_data &
#rm -rf $dst_pgwal &

wait
