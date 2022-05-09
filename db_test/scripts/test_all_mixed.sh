#!/bin/bash

for t in 1 2 3 4 5 6 7 8 9 10; do
	for tree in abtree baseline; do
		./test_insert_with_mixed_query.sh $tree 10 $t 20210504
	done
done
