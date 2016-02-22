/*step1 oppo 相关的uid, accountid, appid */

drop table if exists tmp.temp_cyp_oppo_1;
create table tmp.temp_cyp_oppo_1 
as 
select b.uid, b.accountid, b.appid from 
(select uid from warehouse.device_rolling where lower(brand) like '%oppo%') a 
join (select uid, accountid, appid from  warehouse.account_rolling) b on a.uid=b.uid


/*step2 oppo 相关的accountid, appid 反查其他uid (一个游戏帐号在不同机型上)*/
set hive.optimize.skewjoin = true;
set hive.exec.reducers.bytes.per.reducer=64000000;
drop table if exists tmp.temp_cyp2_oppo_2;
create table tmp.temp_cyp2_oppo_2 as 
select a.uid,a.appid, a.accountid from (select uid, accountid, appid from warehouse.account_rolling where appid<>'6F1B02148D77CCBFB6BBB51F938CCCDC' and accountid <>'OFFLINE' and accountid <> '-' and accountid <> '0' and accountid <> '1' and accountid <> '2' and accountid <> '3'  and accountid <> '4' and accountid <> 'default') a join (select * from tmp.temp_cyp_oppo_1 where appid<>'6F1B02148D77CCBFB6BBB51F938CCCDC' and accountid <>'OFFLINE' and accountid <> '-'  and accountid <> '0' and accountid <> '1' and accountid <> '2' and accountid <> '3'  and accountid <> '4' and accountid <> 'default') b on a.accountid=b.accountid and a.appid=b.appid group by a.uid, a.accountid, a.appid;


drop table if exists tmp.tmp_oppo_0910_1;
create table tmp.tmp_oppo_0910_1 as  
select d.uid from 
(select uid, count(distinct brand) as total from warehouse.device_rolling  where uid<>'528C8E6CD4A3C6598999A0E9DF15AD32' and uid<>'E86942BC9C9CC1891BB9637E182C63C5' group by uid) d where d.total=1

drop table if exists tmp.tmp_oppo_0910_2;
create table tmp.tmp_oppo_0910_2 as  
select b.uid,min(b.brand) as brand, min(b.createtime) as createtime from tmp.tmp_oppo_0910_1 a join 
(select uid, brand,createtime from warehouse.device_rolling where uid<>'528C8E6CD4A3C6598999A0E9DF15AD32' and uid<>'E86942BC9C9CC1891BB9637E182C63C5' 
group by uid, brand, createtime) b on a.uid=b.uid group by b.uid order by createtime;


set mapreduce.job.reduces=20;
set hive.groupby.skewindata=true;
drop table if exists tmp.tmp_oppo_0913_v1;
create table tmp.tmp_oppo_0913_v1 as  
select t2.appid, t2.accountid, concat_ws(',', collect_list(t2.uid)) as uids, 
concat_ws('_', collect_list(string(cast(t2.avg_duration as int)))) as avg_duration, concat_ws('_', 
    collect_list(string(cast(t2.sum_duration/(t2.diff_date/86400+1) as int)))) as new_avg_duration, 
concat_ws('_', collect_list(string(cast(t2.avg_logintimes as int)))) as avg_logintimes, concat_ws('_', 
    collect_list(string(cast(t2.sum_logintimes/(t2.diff_date/86400+1) as int)))) as new_avg_logintimes, 
concat_ws('_', collect_list(string(cast(t2.avg_paytimes as int)))) as avg_paytimes, concat_ws('_', 
    collect_list(string(cast(t2.sum_paytimes/(t2.diff_date/86400+1) as int)))) as new_avg_paytimes, 
concat_ws('_', collect_list(string(cast(t2.avg_payamount as int)))) as avg_payamount, concat_ws('_', 
    collect_list(string(cast(t2.sum_payamount/(t2.diff_date/86400+1) as int)))) as new_avg_payamount, 
concat_ws('_', collect_list(string(cast(t2.avg_duration_weekend  as int)))) as avg_duration_weekend, concat_ws('_', 
    collect_list(string(cast(t2.sum_duration_weekend/(t2.diff_date/86400+1)*2/7 as int)))) as new_avg_duration_weekend, 
concat_ws('_', collect_list(string(cast(t2.avg_duration_weekday as int)))) as avg_duration_weekday,concat_ws('_', 
    collect_list(string(cast(t2.sum_duration_weekday/(t2.diff_date/86400+1)*5/7 as int)))) as new_avg_duration_weekday,  
concat_ws('_', collect_list(string(cast(t2.diff_date/86400+1 as int)))) as diff_date, 
    concat_ws('_',collect_list(string(round(t2.count/((t2.diff_date/86400+1)), 2)))) as new_count, 
    concat_ws('@', collect_list(t2.brand)) as brands, 
collect_list(t2.province)[size(collect_list(t2.province))-1] as province from 
(select a.appid, a.accountid, c.*, b.brand,b.createtime from 
(select uid, `_c1` as brand, createtime from  tmp.tmp_oppo_0910_2) b 
join (select uid, accountid, appid from tmp.temp_cyp2_oppo_2 where uid<>'528C8E6CD4A3C6598999A0E9DF15AD32' and uid<>'E86942BC9C9CC1891BB9637E182C63C5' group by uid, accountid, appid) a 
on a.uid=b.uid join tmp.threesteptable3 c on a.uid=c.uid 
order by a.appid, a.accountid, b.createtime, c.uid) t2 group by t2.appid,t2.accountid;


drop table if exists tmp.tmp_oppo_0913_final_v2;
create table tmp.tmp_oppo_0913_final_v2 as 
select * from tmp.tmp_oppo_0913_v2 where size(split(uids,','))>=2 and size(split(uids,','))<=10 and lower(brands) like '%oppo%';



drop table tmp.tmp_oppo_0913_final_v3;
create table tmp.tmp_oppo_0913_final_v3 as 
select min(appid) as appid, min(accountid) as accountid,uids,min(avg_duration) as avg_duration,
min(new_avg_duration) as new_avg_duration, min(avg_logintimes) as avg_logintimes,min(new_avg_logintimes) as new_avg_logintimes,min(avg_paytimes) as avg_paytimes,
min(new_avg_paytimes) as new_avg_paytimes, min(avg_payamount) as avg_payamount,min(new_avg_payamount) as new_avg_payamount, min(avg_duration_weekend) as avg_duration_weekend,
min(new_avg_duration_weekend) as new_avg_duration_weekend,min(avg_duration_weekday) as avg_duration_weekday,min(new_avg_duration_weekday) as new_avg_duration_weekday, 
min(diff_date) as diff_date,min(new_count) as new_count,min(brands) as brands,min(province) as province from tmp.tmp_oppo_0913_final_v1 group by uids;


