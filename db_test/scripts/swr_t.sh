#!/bin/bash

for i in 1 2 3 4 5 6 7 8 9 10 13 15 17 20 23 25 30 33 36; do
	echo $n
	prefix=logs/swr_n100_s10000_t${i}
	echo "1" | ./run test.SWRCountQuery test_abtree 100 10000 $i ${prefix}.meter | tee ${prefix}.log
done
