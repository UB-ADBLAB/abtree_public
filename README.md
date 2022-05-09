Source code for AB-Tree in VLDB '22
==========================================

This repository contains the source code for the experiments of the paper
"AB-Tree:h

abtree: the PostgreSQL with AB-tree
baseline: the PostgreSQL with the baseline aggregate B-tree

Both of the PG in our experiemnts were configured and compiled with:
    
    CFLAGS="-O3" ./configure --prefix=<path-to-installation-location>
    make
    make install

db_test: the scripts for running the experiments, see db_test/README for
details.

