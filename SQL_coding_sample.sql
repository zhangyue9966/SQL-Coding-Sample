
--hive
create table zhangyue_auto (uid string) row format DELIMITED FIELDS TERMINATED BY '\t';   
hadoop fs -put autouid.txt /user/zhangyue_auto ;

--sql 
select sum(x.orig_cnt) as orig_num,sum(x.tran_cnt) as tran_num from 
(select a.uid,count(case when b.is_transmit='0' then b.mid end) as orig_cnt,
count(case when b.is_transmit='1' then b.mid end) as tran_cnt from 
(select uid from zhangyue_auto)a 
join 
(select uid,mid,is_transmit from table1 where dt>='20170101' and dt<='20170701')b 
on a.uid=b.uid group by a.uid)x;


select a.uid,a.class,b.mid from 
(select uid,1 class from zhangyue_uid1
union all 
select uid,2 class from zhangyue_uid2
union all 
select uid,3 class from zhangyue_uid3
union all 
select uid,4 class from zhangyue_uid4
union all 
select uid,5 class from zhangyue_uid5
union all 
select uid,6 class from zhangyue_uid6
union all 
select uid,7 class from zhangyue_uid7
)a 
join 
(select uid,mid from mds_bhv_pubblog where dt>='20170101' and dt<='20170930' and has_video='1') b 
on a.uid=b.uid 
group by a.uid,a.class,b.mid;


-- use java package to extract specific type of words --
add jar /usr/IKAnalyzerHDFS_auto_ref.jar;
Create temporary function fenciauto as 'dw.udf.IKAnalyzer';

select d.keyword1,sum(e.query_cnt) as num from
(select keyword1 from 
(select fenciauto(concat(b.content),false,false)as keyword from 
(select uid,content from ods_tblog_content where dt>='20170701' and  dt<='20170930')b)c
LATERAL VIEW explode(split(c.keyword,'\\&'))adTable AS keyword1
group by  keyword1)d 
join 
(select keyword,query_cnt from mds_search_keyword_day where dt>='20170701' and dt<='20170930')e
on d.keyword1=e.keyword 
group by d.keyword1
order by num desc;

-- count how many times did a specific word shows in the articles during a specific period
select sum(case when a.keyword like 'winter' then a.query_cnt end) from 
(select keyword,query_cnt from search_keyword_tb where (dt>='20170701' and dt<='20170731'))a;4117


-- find 50 the most popular blogger 
select f.brand,f.uid,f.nick,f.nzhpz from
(select e.brand,e.uid,e.nick,e.nzhpz,row_number() over (partition by e.brand order by e.nzhpz desc) as rank from
(select brand,uid,nick,nzhpz from 
(select uid,nick from kol) a join 
(select brand,uid1,nzhpz from zhp)b
on a.uid=b.uid1
group by brand,uid,nick,nzhpz)e)f
where f.rank<='50';

