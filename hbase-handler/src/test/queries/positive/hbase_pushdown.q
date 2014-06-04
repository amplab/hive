CREATE TABLE hbase_pushdown(key int, value string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:string");

INSERT OVERWRITE TABLE hbase_pushdown 
SELECT *
FROM src;

-- with full pushdown
explain select * from hbase_pushdown where key=90;

select * from hbase_pushdown where key=90;

-- with partial pushdown

explain select * from hbase_pushdown where key=90 and value like '%90%';

select * from hbase_pushdown where key=90 and value like '%90%';

-- with two residuals

explain select * from hbase_pushdown
where key=90 and value like '%90%' and key=cast(value as int);

-- with contradictory pushdowns

explain select * from hbase_pushdown
where key=80 and key=90 and value like '%90%';

select * from hbase_pushdown
where key=80 and key=90 and value like '%90%';

-- with nothing to push down

explain select * from hbase_pushdown;

-- with a predicate which is not actually part of the filter, so
-- it should be ignored by pushdown

explain select * from hbase_pushdown
where (case when key=90 then 2 else 4 end) > 3;

-- with a predicate which is under an OR, so it should
-- be ignored by pushdown

explain select * from hbase_pushdown
where key=80 or value like '%90%';

set hive.optimize.ppd.storage=false;

-- with pushdown disabled

explain select * from hbase_pushdown where key=90;


--  Nonkey predicate push down tests

CREATE TABLE hbase_pushdown2(key string, v1 string, v2 string, v3 int, v4 string, v5 int)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:string,cf2:string,cf1:int,cf3:string2,cf3:int");

-- Only key predicate without any non-key predicates

explain extended select * from hbase_pushdown2 where key < "ab" and key >"cd"
explain extended select * from hbase_pushdown2 where key < "ab"

-- no pushdown
explain extended select * from hbase_pushdown2 where key < "ab" and key >"cd" and key="efg"

-- Only non key predicates without any key predicates

-- should push the left predicate and ignore the right one
explain extended select * from hbase_pushdown2 where (v1 > "ab" and v2 < "cd") and (v3 <5 or v4 <"ff")

explain extended select * from hbase_pushdown2 where (v1 > "ab" and v2 < "cd") and (v3 >5 and (v4<"abc" and v5=6))

-- Nested ANDs and ORs

-- pushes everything since everything is either =/string/binary
explain extended select * from hbase_pushdown2 where (v1 > "ab" and v2 < "cd") and (v3=5 and (v4<"abc" or v5=6))

-- doesnt push the OR predicate
explain extended select * from hbase_pushdown2 where (v1 > "ab" and v2 < "cd") and (v3=5 and (v4<"abc" or v5>6))

-- Both Key and Non-Key Predicates - Keys are used for ranges and Non-Keys for filters

explain extended select * from hbase_pushdown2 where (((key < "a5") and v1 > "ab") and v2 < "cd") and (v3 >5 and (v4<"abc" and v5=6))

-- No key based ranges are possible , only non-key predicates are pushed down
explain extended select * from hbase_pushdown2 where (((key < "a5") and v1 > "ab") and v2 < "cd") and ((key="c3" and v3 >5) and (v4<"abc" and v5=6))
