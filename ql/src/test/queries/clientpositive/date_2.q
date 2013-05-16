drop table date_2;

create table date_2 (d date);
alter table date_2 set serde 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe';

insert overwrite table date_2 
  select cast('2011-01-01' as date) from src limit 1;
select cast(d as boolean) from date_2 limit 1;
select cast(d as tinyint) from date_2 limit 1;
select cast(d as smallint) from date_2 limit 1;
select cast(d as int) from date_2 limit 1;
select cast(d as bigint) from date_2 limit 1;
select cast(d as float) from date_2 limit 1;
select cast(d as double) from date_2 limit 1;
select cast(d as string) from date_2 limit 1;

insert overwrite table date_2
  select '2011-01-01' from src limit 1;
select cast(d as boolean) from date_2 limit 1;
select cast(d as tinyint) from date_2 limit 1;
select cast(d as smallint) from date_2 limit 1;
select cast(d as int) from date_2 limit 1;
select cast(d as bigint) from date_2 limit 1;
select cast(d as float) from date_2 limit 1;
select cast(d as double) from date_2 limit 1;
select cast(d as string) from date_2 limit 1;

drop table date_2;
