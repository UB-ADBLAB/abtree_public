DROP TABLE IF EXISTS lineitem;

-- we only load lineitem for these experiments
CREATE TABLE lineitem (
	l_orderkey INTEGER,
	l_partkey INTEGER,
	l_suppkey INTEGER,
	l_linenumber INTEGER,
	l_quantity DECIMAL(12, 2),
	l_extendedprice DECIMAL(12, 2),
	l_discount DECIMAL(12, 2),
	l_tax DECIMAL(12, 2),
	l_returnflag CHAR(1),
	l_linestatus CHAR(1),
	l_shipdate DATE,
	l_commitdate DATE,
	l_receiptdate DATE,
	l_shipinstruct CHAR(25),
	l_shipmode CHAR(10),
	l_comment VARCHAR(44)
);

-- start date: 1992-01-01 -- J2448623
-- end date: 1998-12-31 -- J2451179
-- last shipdate: 1998-12-01 - J2451149
