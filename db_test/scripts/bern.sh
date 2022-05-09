#!/bin/bash

for i in 0.01 0.005 0.001 0.0005 0.0001; do
	s=`echo $i | awk '{ print $i * 1000000000 / 100; }'`
	echo $n
	prefix=logs/bern_n10_s${s}_t1
	echo "1" | ./run test.BernoulliCountQuery test_abtree 10 $i 1 ${prefix}.meter | tee ${prefix}.log
done
