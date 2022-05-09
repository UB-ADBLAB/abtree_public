#!/bin/bash

. ./switch_pg.sh release
numactl --cpubind=0 --membind=0 pg_ctl start

tmpfile=`mktemp`
echo 1 | ./run test.BernoulliCountQuery test_abtree 2 0.001 1 $tmpfile
rm -f $tmpfile

for i in 1 2 3 4 5 6 7 8 9 10 13 15 17 20 23 25 30 33 36; do
	prefix=logs/bern_n10_s10000_t${i}
	echo "1" | ./run test.BernoulliCountQuery test_abtree 10 0.001 $i  ${prefix}.meter | tee ${prefix}.log
done

pg_ctl stop
