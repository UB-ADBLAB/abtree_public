create table if not exists A (
	x int, y int);

create index on A using btree(y) ;

