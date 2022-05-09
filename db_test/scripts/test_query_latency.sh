#!/bin/bash

N=500000

for tree in abtree baseline; do
	if [ "x$tree" = "xabtree" ]; then
		BUILD="release"
		src_data=/mnt/ssd1/zyzhao/loaded_data/abtree_1B
		src_pgwal=/mnt/ssd1/zyzhao/loaded_data/abtree_pg_wal_1B
	elif [ x"$tree" = "xbaseline" ]; then
		BUILD="baseline"
		src_data=/mnt/ssd1/zyzhao/loaded_data/baseline_1B
		src_pgwal=/mnt/ssd1/zyzhao/loaded_data/baseline_pg_wal_1B
	else
		echo "Invalid tree name: $tree"
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
	ln -s $dst_pgwal $dst_data/pg_wal
	echo "copy done"

	for ssize in 1000 5000 10000 50000 100000; do
		for shared_buffers in 32GB; do
			cp $src_data/postgresql.conf $dst_data/postgresql.conf
			echo "shared_buffers = $shared_buffers" >> $dst_data/postgresql.conf

			echo "starting postgresql"
			pg_ctl start

			echo "wait 5 seconds"
			sleep 5 

			echo "testing..."
			PREFIX="logs/q_${tree}_n1_S${shared_buffers}_ssize${ssize}"
			./run test.MixedWorkload -i test_abtree 0 0 0 0 0 $ssize 1 \
				0 0 0 30 ${PREFIX}.meter | tee ${PREFIX}.log

			echo "cleaning up..."
			pg_ctl stop
			sleep 2
		done
	done
	
	echo "deleting data..."	
	sleep 2
	rm -rf $dst_data &
	rm -rf $dst_pgwal &
	wait
done

