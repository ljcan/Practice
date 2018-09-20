create database storm;

use storm;

create table wc(
count int(10),
word varchar(20)
)charset utf8;

create table location(
time bigint,
latitude double,
longitude double
)charset utf8;


select current_timestamp() from dual;

select unix_timestamp(current_timestamp - interval 10 minute) from dual;

# 当前十分钟之内（时间戳格式）
select unix_timestamp(date_sub(sysdate(),interval 10 minute))*1000 from dual;

select unix_timestamp(date_sub(current_timestamp(),interval 10 minute))*1000 from dual;

select longitude,latitude,count(1) from location where time>(select unix_timestamp(date_sub(current_timestamp(),interval 10 minute))*1000 from dual) group by longitude,latitude;

# 统计最近十分钟的数据
select longitude,latitude,count(*) from location where time>unix_timestamp(date_sub(current_timestamp(),interval 10 minute))*1000 group by longitude,latitude;

