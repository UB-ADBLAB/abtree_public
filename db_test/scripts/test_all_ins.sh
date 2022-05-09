#!/bin/bash

N=500000

for t in 1 2 3 4 5 6 7 8 9 10 13 15 17 20 23 25 30 33 36; do
	for tree in abtree baseline btree; do
		for shared_buffers in 128MB 32GB; do
			./test_ins.sh $tree $N $t 20210504 $shared_buffers
		done
	done
done
