drop table date_1;

create table date_1 (d date);
alter table date_1 set serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';

insert overwrite table date_1 
  select cast('2011-01-01' as date) from src limit 1;
select cast(d as boolean) from date_1 limit 1;
select cast(d as tinyint) from date_1 limit 1;
select cast(d as smallint) from date_1 limit 1;
select cast(d as int) from date_1 limit 1;
select cast(d as bigint) from date_1 limit 1;
select cast(d as float) from date_1 limit 1;
select cast(d as double) from date_1 limit 1;
select cast(d as string) from date_1 limit 1;

insert overwrite table date_1
  select '2011-01-01' from src limit 1;
select cast(d as boolean) from date_1 limit 1;
select cast(d as tinyint) from date_1 limit 1;
select cast(d as smallint) from date_1 limit 1;
select cast(d as int) from date_1 limit 1;
select cast(d as bigint) from date_1 limit 1;
select cast(d as float) from date_1 limit 1;
select cast(d as double) from date_1 limit 1;
select cast(d as string) from date_1 limit 1;

drop table date_1;
