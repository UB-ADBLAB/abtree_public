#!/bin/bash

BASEDIR="`realpath "$(dirname "$0")"`"
cd $BASEDIR

if [ $# -lt 3 ]; then
	echo "usage: $0 <tree> <nquery_threads> <bufsize>"
	exit 1
fi

TREE=$1
N=$2
BUFSIZE=$3

if [ "x$TREE" = "xabtree" ]; then
	BUILD="release"
	src_data=/mnt/ssd1/zyzhao/loaded_data/abtree_1B
	src_pgwal=/mnt/ssd1/zyzhao/loaded_data/abtree_pg_wal_1B
elif [ x"$TREE" = "xbaseline" ]; then
	BUILD="baseline"
	src_data=/mnt/ssd1/zyzhao/loaded_data/baseline_1B
	src_pgwal=/mnt/ssd1/zyzhao/loaded_data/baseline_pg_wal_1B
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
ln -s $dst_pgwal $dst_data/pg_wal

#drop_cache

echo "starting postgresql"
pg_ctl start

echo "wait 5 seconds"
sleep 5 
#echo "warm up..."
#tmpfile=`mktemp`
#./run test.MixedWorkload test_abtree 0 0 0 0 0 10000 20 0 0 0 60 $tmpfile
#rm -f $tmpfile

echo "testing..."
PREFIX="logs/q_${TREE}_n${N}_S${BUFSIZE}"
./run test.MixedWorkload -i test_abtree 0 0 0 0 0 10000 $N \
	0 0 0 30 ${PREFIX}.meter | tee ${PREFIX}.log

echo "cleaning up..."
pg_ctl stop
sleep 2
rm -rf $dst_data &
rm -rf $dst_pgwal &

wait

