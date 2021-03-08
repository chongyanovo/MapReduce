

```sql
hive> create table data(
    > area string,
    > shop string,
    > good_Jan string,
    > sale_Jan int,
    > good_Feb string,
    > sale_Feb int)
    > row format delimited fields terminated by ','; 
OK
Time taken: 0.453 seconds
hive> load data inpath '/demo2/out1_2/part-r-00000' into table data;
Loading data to table demo2.data
OK
Time taken: 0.699 seconds
```



1 统计该表中有几个地区

 ```sql
hive> select count(distinct area) from data;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20210307172244_061cc491-774e-494f-8b89-bf3a49689514
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2021-03-07 17:22:46,200 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local484291990_0004
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 7908 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
3
Time taken: 2.007 seconds, Fetched: 1 row(s)
 ```

2、统计该表中有几个商家

```sql
hive> select  count(distinct shop) from data;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20210307172314_b4ab9561-7fa3-4ed7-bb15-db50ea4abf9d
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2021-03-07 17:23:16,043 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1040412579_0005
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 9032 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
4
Time taken: 2.038 seconds, Fetched: 1 row(s)

```

3、统计该表中有几种商品

 ```sql
hive> select count(distinct good) from 
    > (select distinct good_Jan good from data
    > union all
    > select distinct  good_Feb good from data) tmp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20210307173127_790d1535-b97c-4726-8141-e7c25016082f
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2021-03-07 17:31:30,534 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local329058301_0013
Launching Job 2 out of 3
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2021-03-07 17:31:32,298 Stage-3 map = 100%,  reduce = 100%
Ended Job = job_local1675041747_0014
Launching Job 3 out of 3
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2021-03-07 17:31:34,296 Stage-2 map = 100%,  reduce = 100%
Ended Job = job_local1368832268_0015
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 15776 HDFS Write: 0 SUCCESS
Stage-Stage-3:  HDFS Read: 16900 HDFS Write: 0 SUCCESS
Stage-Stage-2:  HDFS Read: 25350 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
9
Time taken: 7.171 seconds, Fetched: 1 row(s)
 ```

4、统计2月份比1月份销量增加幅度最大的是哪个商品

 ```sql
hive> select good_Jan,(sale_Jan - sale_Feb) sale from data sort by sale desc limit 1;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20210307204856_65455426-8cf6-4ddc-b620-4e68bccb464d
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2021-03-07 20:48:57,879 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local74025699_0003
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2021-03-07 20:48:59,112 Stage-2 map = 100%,  reduce = 100%
Ended Job = job_local966853108_0004
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 8992 HDFS Write: 0 SUCCESS
Stage-Stage-2:  HDFS Read: 8992 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
电视A	40
Time taken: 2.571 seconds, Fetched: 1 row(s)
 ```

5、统计每个地区1月份所有商品的销量总和，并按从高到低的顺序排序

 ```sql
hive> select area,sum(sale_Jan) sale from data group by area sort by sale desc;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20210308091845_6f6c4b09-9bdc-4819-8657-d9d25b36853a
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2021-03-08 09:18:47,610 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1531332413_0001
Launching Job 2 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2021-03-08 09:18:49,030 Stage-2 map = 100%,  reduce = 100%
Ended Job = job_local1266952398_0002
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 1124 HDFS Write: 0 SUCCESS
Stage-Stage-2:  HDFS Read: 1124 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
秦淮	115
江宁	85
鼓楼	35
Time taken: 3.932 seconds, Fetched: 3 row(s)
 ```

6、统计每个商家的每个产品的1月份销售数据和2月份销售数据的总和

 ```sql
hive> select shop,good_Jan,sum(sale_Jan + sale_Feb) sale from data group by shop,good_Jan;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20210308092054_8375eb31-6cc6-4e23-a81e-b342b35293f0
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2021-03-08 09:20:57,475 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local678129115_0003
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 2248 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
商家A	冰箱A	50
商家A	冰箱B	30
商家B	冰箱A	45
商家B	冰箱B	48
商家B	电视A	40
商家C	冰箱A	52
商家C	冰箱B	42
商家C	洗衣机A	13
商家C	热水器A	11
商家C	电视A	78
商家D	洗衣机A	23
商家D	空调A	85
商家D	空调B	25
Time taken: 3.367 seconds, Fetched: 13 row(s)
 ```

7、统计每个商家1月份销售的所有商品名称和对应的销售额

```sql
hive> select shop,good_Jan,sum(sale_Jan) from data group by shop,good_Jan;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20210307205043_c74889c3-d716-4d6c-bb7b-925dacfc711e
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2021-03-07 20:50:44,751 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1513732175_0005
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 10116 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
商家A	冰箱A	20
商家A	冰箱B	15
商家B	冰箱A	22
商家B	冰箱B	18
商家B	电视A	40
商家C	冰箱A	22
商家C	冰箱B	20
商家C	洗衣机A	5
商家C	热水器A	5
商家C	电视A	15
商家D	洗衣机A	8
商家D	空调A	30
商家D	空调B	15
Time taken: 1.368 seconds, Fetched: 13 row(s)
```

