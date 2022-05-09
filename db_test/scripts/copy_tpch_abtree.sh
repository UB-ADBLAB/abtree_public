#!/bin/bash

rm -rf /mnt/ssd3/zyzhao/pg_data/release
rm -rf /mnt/ssd2/zyzhao/release_pg_wal
cp -r /mnt/ssd1/zyzhao/loaded_data/tpch_100g_abtree_pg_wal /mnt/ssd2/zyzhao/release_pg_wal &
cp -r /mnt/ssd1/zyzhao/loaded_data/tpch_100g_abtree /mnt/ssd3/zyzhao/pg_data/release &
wait
ln -s /mnt/ssd2/zyzhao/release_pg_wal /mnt/ssd3/zyzhao/pg_data/release/pg_wal
