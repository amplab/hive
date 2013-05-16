drop table date_3;

create table date_3 (d date);
alter table date_3 set serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';

insert overwrite table date_3 
  select cast(cast('1.3041352164485E9' as double) as date) from src limit 1;
select cast(d as boolean) from date_3 limit 1;
select cast(d as tinyint) from date_3 limit 1;
select cast(d as smallint) from date_3 limit 1;
select cast(d as int) from date_3 limit 1;
select cast(d as bigint) from date_3 limit 1;
select cast(d as float) from date_3 limit 1;
select cast(d as double) from date_3 limit 1;
select cast(d as string) from date_3 limit 1;
select cast(d as timestamp) from date_3 limit 1;

drop table date_3;
