create table if not exists A (
	x int, y int);

create index on A using abtree(y) with (aggregation_type = int8, agg_support = abt_count_support);

