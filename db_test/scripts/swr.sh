#!/bin/bash

for i in 1000 5000 10000 50000 100000; do
	echo $n
	prefix=logs/swr_n100_s${i}_t1
	echo "1" | ./run test.SWRCountQuery test_abtree 100 $i 1 ${prefix}.meter | tee ${prefix}.log
done
