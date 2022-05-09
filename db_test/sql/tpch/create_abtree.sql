create index lineitem_abtree on lineitem using abtree(l_shipdate) with (aggregation_type = int8, agg_support = abt_count_support);
