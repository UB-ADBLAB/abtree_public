#!/bin/bash

rm -rf /mnt/ssd3/zyzhao/pg_data/baseline
rm -rf /mnt/ssd2/zyzhao/baselinepg_wal
cp -r /mnt/ssd1/zyzhao/loaded_data/tpch_100g_btree_baseline_build_pg_wal /mnt/ssd2/zyzhao/baseline_pg_wal &
cp -r /mnt/ssd1/zyzhao/loaded_data/tpch_100g_btree_baseline_build /mnt/ssd3/zyzhao/pg_data/baseline &
wait
ln -s /mnt/ssd2/zyzhao/baseline_pg_wal /mnt/ssd3/zyzhao/pg_data/baseline/pg_wal
