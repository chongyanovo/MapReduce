建表导入数据

```sql
hive> show databases;
OK
default
mytest
test4
Time taken: 5.5 seconds, Fetched: 3 row(s)
hive> create database demo1;
OK
Time taken: 1.487 seconds
hive> create external table cars(
    > province string, --省份
    > month int, --月
    > city string, --市 
    > county string, --区县
    > year int, --年
    > cartype string,--车辆型号
    > productor string,--制造商
    > brand string, --品牌
    > mold string,--车辆类型
    > owner string,--所有权
    > nature string, --使用性质
    > number int,--数量
    > ftype string,--发动机型号
    > outv int,--排量
    > power double, --功率
    > fuel string,--燃料种类
    > length int,--车长
    > width int,--车宽
    > height int,--车高
    > xlength int,--厢长
    > xwidth int,--厢宽
    > xheight int,--厢高
    > count int,--轴数
    > base int,--轴距
    > front int,--前轮距
    > norm string,--轮胎规格
    > tnumber int,--轮胎数
    > total int,--总质量
    > curb int,--整备质量
    > hcurb int,--核定载质量
    > passenger string,--核定载客
    > zhcurb int,--准牵引质量
    > business string,--底盘企业
    > dtype string,--底盘品牌
    > fmold string,--底盘型号
    > fbusiness string,--发动机企业
    > name string,--车辆名称
    > age int,--年龄
    > sex string --性别
    > )
    > row format delimited
    > fields terminated by '\t';
OK
Time taken: 1.183 seconds
hive> dfs -mkdir /demo1;
hive> !ls;
aaa.txt
abc.txt
car.txt
derby.log
wordcount.jar
wordcount.txt
hive> dfs -put car.txt /demo1;
hive> load data inpath '/demo1/car.txt' into table cars;
Loading data to table default.cars
OK
Time taken: 2.13 seconds
hive> select * from cars limit 2;
OK
山西省	3	朔州市	朔城区	2013	LZW6450PF	上汽通用五菱汽车股份有限公司	五菱	小型普通客车	个人	非营运	1	L3C	8424	79.0	汽油	4490	1615	1900	NULL	NULL	NULL	2	3050	1386	175/70R14LT	4	2110	1275	NULL	7	NULL				上汽通用五菱汽车股份有限公司	客车	1913	男性
山西省	3	晋城市	城区	2013	EQ6450PF1	东风小康汽车有限公司	东风	小型普通客车	个人	非营运	1DK13-06	1587	74.0	汽油	4500	1680	1960	NULL	NULL	NULL	2	3050	1435	185R14LT 6PR	41970	1290	NULL	7	NULL	东风小康汽车有限公司		EQ6440KMF	重庆渝安淮海动力有限公司	客车	1929	男性
Time taken: 0.173 seconds, Fetched: 2 row(s)
```

统计车辆不同用途的数量分布

```sql
hive> select nature,count(1) from cars group by nature;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20210304095727_20b0548f-6af1-4f40-9340-0f6b08eceead
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
2021-03-04 09:57:29,921 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1399581449_0002
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 82671928 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
NULL	273
	5
中小学生校车	119
公交客运	1742
公路客运	1448
出租客运	2
初中生校车	2
小学生校车	111
工程救险	1
幼儿校车	17
救护	1
教练	26
旅游客运	219
消防	7
租赁	24
警用	165
非营运	66478
Time taken: 2.794 seconds, Fetched: 17 row(s)
```

统计山西省2013年每个月的汽车销售数量的比例

- not in (null)  	is not null 

```sql
hive> select month,round(s1/s2,2) per
    > from
    > (select month,count(1) s1 from cars where month is not null group by month) a,
    > (select count(1) s2 from cars) b;
Warning: Map Join MAPJOIN[25][bigTable=?] in task 'Stage-4:MAPRED' is a cross product
Warning: Map Join MAPJOIN[26][bigTable=?] in task 'Stage-5:MAPRED' is a cross product
Warning: Shuffle Join JOIN[17][tables = [$hdt$_0, $hdt$_1]] in Stage 'Stage-2:MAPRED' is a cross product
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20210304102057_2b4a9a55-c0c0-4a84-b234-1c175b2b21c7
Total jobs = 5
Launching Job 1 out of 5
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2021-03-04 10:20:59,325 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1634480780_0011
Launching Job 2 out of 5
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2021-03-04 10:21:00,519 Stage-3 map = 100%,  reduce = 100%
Ended Job = job_local539381102_0012
Stage-7 is selected by condition resolver.
Stage-8 is filtered out by condition resolver.
Stage-2 is filtered out by condition resolver.
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/hive/lib/log4j-slf4j-impl-2.6.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
2021-03-04 10:21:10	Starting to launch local task to process map join;	maximum memory = 518979584
2021-03-04 10:21:11	Dump the side-table for tag: 1 with group count: 1 into file: file:/tmp/root/7e16a63b-5b63-4456-98e1-ce391a4d1964/hive_2021-03-04_10-20-57_721_4289143085151534353-1/-local-10006/HashTable-Stage-4/MapJoin-mapfile61--.hashtable
2021-03-04 10:21:11	Uploaded 1 File to: file:/tmp/root/7e16a63b-5b63-4456-98e1-ce391a4d1964/hive_2021-03-04_10-20-57_721_4289143085151534353-1/-local-10006/HashTable-Stage-4/MapJoin-mapfile61--.hashtable (281 bytes)
2021-03-04 10:21:11	End of local task; Time Taken: 1.009 sec.
Execution completed successfully
MapredLocal task succeeded
Launching Job 4 out of 5
Number of reduce tasks is set to 0 since there's no reduce operator
Job running in-process (local Hadoop)
2021-03-04 10:21:14,645 Stage-4 map = 100%,  reduce = 0%
Ended Job = job_local2017488299_0013
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 330687712 HDFS Write: 0 SUCCESS
Stage-Stage-3:  HDFS Read: 372023676 HDFS Write: 0 SUCCESS
Stage-Stage-4:  HDFS Read: 186011838 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
1	0.15
2	0.06
3	0.09
4	0.07
5	0.07
6	0.06
7	0.06
8	0.06
9	0.07
10	0.1
11	0.1
12	0.1
Time taken: 16.94 seconds, Fetched: 12 row(s)
```

统计山西省2013年各市、区县的汽车销售的分布 

```sql
hive> select city,county,count(1) num from cars 
    > where city is not null and county is not null 
    > group by city,county; 
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20210304102727_f8e05aeb-7ef5-447e-a6df-129705d51e49
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
2021-03-04 10:27:28,821 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1535280329_0017
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 537367532 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
		48
	2600	5
1913	男性	3
1921	男性	3
1925	男性	5
1926	女性	3
1926	男性	3
1928	女性	3
1928	男性	6
1929	男性	4
1930	女性	3
1930	男性	2
1931	男性	3
1932	女性	1
1932	男性	1
1933	男性	1
1935	女性	3
1935	男性	1
1936	女性	1
1937	女性	1
1937	男性	2
1939	男性	1
1941	男性	1
临汾市	临汾市	653
临汾市	乡宁县	119
临汾市	侯马市	561
临汾市	古县	132
临汾市	吉县	221
临汾市	大宁县	93
临汾市	安泽县	263
临汾市	尧都区	1382
临汾市	市辖区	240
临汾市	曲沃县	274
临汾市	永和县	55
临汾市	汾西县	106
临汾市	洪洞县	1012
临汾市	浮山县	183
临汾市	翼城县	263
临汾市	蒲县	108
临汾市	襄汾县	663
临汾市	隰县	119
临汾市	霍州市	388
吕梁市	中阳县	38
吕梁市	临县	443
吕梁市	交口县	70
吕梁市	交城县	48
吕梁市	兴县	78
吕梁市	吕梁市	65
吕梁市	孝义市	510
吕梁市	岚县	37
吕梁市	市辖区	1772
吕梁市	文水县	346
吕梁市	方山县	158
吕梁市	柳林县	224
吕梁市	汾阳市	622
吕梁市	石楼县	84
吕梁市	离石区	351
大同市	南郊区	367
大同市	城区	636
大同市	大同县	601
大同市	大同城区	1052
大同市	大同市	1589
大同市	天镇县	166
大同市	左云县	320
大同市	市辖区	885
大同市	广灵县	144
大同市	新荣区	77
大同市	浑源县	415
大同市	灵丘县	195
大同市	矿区	270
大同市	阳高县	346
太原市	太原市	7309
太原市	市辖区	2709
太原市	杏花岭区	3766
太原市	阳泉市	136
忻州市	五台县	289
忻州市	五寨县	107
忻州市	代县	157
忻州市	保德县	195
忻州市	偏关县	105
忻州市	原平市	454
忻州市	宁武县	84
忻州市	定襄县	150
忻州市	岢岚县	76
忻州市	市辖区	1462
忻州市	忻州市	473
忻州市	忻府区	437
忻州市	河曲县	146
忻州市	神池县	74
忻州市	繁峙县	168
忻州市	静乐县	47
晋中市	介休市	579
晋中市	和顺县	412
晋中市	太谷县	518
晋中市	寿阳县	399
晋中市	左权县	255
晋中市	市辖区	343
晋中市	平遥县	729
晋中市	昔阳县	454
晋中市	晋中市	970
晋中市	榆次区	1382
晋中市	榆社县	266
晋中市	灵石县	327
晋中市	祁县	593
晋城市	城区	1008
晋城市	市辖区	113
晋城市	晋城城区	62
晋城市	晋城市	187
晋城市	沁水县	617
晋城市	泽州县	1129
晋城市	阳城县	977
晋城市	陵川县	643
晋城市	高平市	905
朔州市	右玉县	44
朔州市	山阴县	246
朔州市	市辖区	56
朔州市	平鲁区	177
朔州市	应县	140
朔州市	怀仁县	180
朔州市	朔城区	727
朔州市	朔州市	136
运城市	万荣县	881
运城市	临猗县	1040
运城市	垣曲县	234
运城市	夏县	484
运城市	市辖区	73
运城市	平陆县	319
运城市	新绛县	527
运城市	永济市	603
运城市	河津市	565
运城市	盐湖区	1648
运城市	稷山县	536
运城市	绛县	458
运城市	芮城县	465
运城市	运城市	136
运城市	闻喜县	385
长治市	城区	523
长治市	壶关县	412
长治市	屯留县	348
长治市	市辖区	2385
长治市	平顺县	214
长治市	武乡县	15
长治市	沁县	183
长治市	沁源县	140
长治市	潞城市	271
长治市	襄垣县	483
长治市	郊区	389
长治市	长子县	533
长治市	长治县	424
长治市	长治城区	342
长治市	长治市	1008
长治市	黎城县	37
阳泉市	城区	433
阳泉市	市辖区	355
阳泉市	平定县	633
阳泉市	盂县	618
阳泉市	矿区	169
阳泉市	郊区	276
阳泉市	阳泉城区	72
阳泉市	阳泉市	83
Time taken: 1.445 seconds, Fetched: 160 row(s)
```

统计买车的男女比例

```sql
hive> select sex,round(s1/s2,2) pre from 
    > (select sex,count(1) s1 from cars where sex !='' group by sex) a,
    > (select count(1) s2 from cars where sex !='') b ;
Warning: Map Join MAPJOIN[27][bigTable=?] in task 'Stage-4:MAPRED' is a cross product
Warning: Map Join MAPJOIN[28][bigTable=?] in task 'Stage-5:MAPRED' is a cross product
Warning: Shuffle Join JOIN[18][tables = [$hdt$_0, $hdt$_1]] in Stage 'Stage-2:MAPRED' is a cross product
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20210304103837_5a44846e-ddd2-4d3a-b653-ea9ee069cde5
Total jobs = 5
Launching Job 1 out of 5
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2021-03-04 10:38:39,504 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local552530692_0021
Launching Job 2 out of 5
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2021-03-04 10:38:40,685 Stage-3 map = 100%,  reduce = 100%
Ended Job = job_local1371104720_0022
Stage-7 is selected by condition resolver.
Stage-8 is filtered out by condition resolver.
Stage-2 is filtered out by condition resolver.
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/hive/lib/log4j-slf4j-impl-2.6.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
2021-03-04 10:38:52	Starting to launch local task to process map join;	maximum memory = 518979584
2021-03-04 10:38:53	Dump the side-table for tag: 1 with group count: 1 into file: file:/tmp/root/7e16a63b-5b63-4456-98e1-ce391a4d1964/hive_2021-03-04_10-38-37_687_7156311560742907427-1/-local-10006/HashTable-Stage-4/MapJoin-mapfile101--.hashtable
2021-03-04 10:38:53	Uploaded 1 File to: file:/tmp/root/7e16a63b-5b63-4456-98e1-ce391a4d1964/hive_2021-03-04_10-38-37_687_7156311560742907427-1/-local-10006/HashTable-Stage-4/MapJoin-mapfile101--.hashtable (280 bytes)
2021-03-04 10:38:53	End of local task; Time Taken: 0.926 sec.
Execution completed successfully
MapredLocal task succeeded
Launching Job 4 out of 5
Number of reduce tasks is set to 0 since there's no reduce operator
Job running in-process (local Hadoop)
2021-03-04 10:38:56,795 Stage-4 map = 100%,  reduce = 0%
Ended Job = job_local621779630_0023
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 661375424 HDFS Write: 0 SUCCESS
Stage-Stage-3:  HDFS Read: 702711388 HDFS Write: 0 SUCCESS
Stage-Stage-4:  HDFS Read: 351355694 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
女性	0.3
男性	0.7
Time taken: 19.12 seconds, Fetched: 2 row(s)
```

统计的车的所有权、 车辆类型和品牌的分布   

```sql
hive> select owner,mold,brand,count(1) from cars 
    > group by owner,mold,brand;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20210304104102_0f112db0-e85d-4290-8d8f-445f7496f3fb
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
2021-03-04 10:41:04,072 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1783201400_0024
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 744047352 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
NULL	NULL	NULL	273
			5
个人		五菱宏光	2743
个人		欧诺	300
个人	中型专用校车	宇通	1
个人	中型普通客车	东风	26
个人	中型普通客车	中通	1
个人	中型普通客车	依维柯	14
个人	中型普通客车	合客	2
个人	中型普通客车	大通	1
个人	中型普通客车	大马	10
个人	中型普通客车	宇通	10
个人	中型普通客车	少林	100
个人	中型普通客车	汇众	2
个人	中型普通客车	江淮	13
个人	中型普通客车	江铃全顺	21
个人	中型普通客车	福田	15
个人	中型普通客车	金旅	4
个人	中型普通客车	金杯	15
个人	中型普通客车	金龙	12
个人	中型越野客车	依维柯	1
个人	大型普通客车	同心	1
个人	大型普通客车	宇通	2
个人	大型普通客车	恒通客车	4
个人	大型普通客车	柯斯达	6
个人	大型普通客车	梅赛德斯-奔驰	1
个人	大型普通客车	金旅	2
个人	大型普通客车	金龙	2
个人	大型普通客车	长城	1
个人	大型普通客车	黄海	2
个人	小型普通客车		7
个人	小型普通客车	一汽佳星	3
个人	小型普通客车	东南	14
个人	小型普通客车	东风	4754
个人	小型普通客车	中誉	6
个人	小型普通客车	五菱	41403
个人	小型普通客车	五菱宏光	645
个人	小型普通客车	众泰	8
个人	小型普通客车	依维柯	89
个人	小型普通客车	俊风	5
个人	小型普通客车	力帆	111
个人	小型普通客车	北京	2577
个人	小型普通客车	吉奥	42
个人	小型普通客车	哈飞	11
个人	小型普通客车	大通	37
个人	小型普通客车	奥路卡	402
个人	小型普通客车	尼桑	4
个人	小型普通客车	开瑞	339
个人	小型普通客车	昌河	161
个人	小型普通客车	昌河铃木	6
个人	小型普通客车	松花江	111
个人	小型普通客车	欧诺	60
个人	小型普通客车	汇众	1
个人	小型普通客车	江淮	7
个人	小型普通客车	江铃全顺	263
个人	小型普通客车	海格	2
个人	小型普通客车	神剑	22
个人	小型普通客车	福田	51
个人	小型普通客车	航天	124
个人	小型普通客车	解放	349
个人	小型普通客车	通家福	24
个人	小型普通客车	野马	28
个人	小型普通客车	金旅	7
个人	小型普通客车	金杯	367
个人	小型普通客车	金龙	28
个人	小型普通客车	长城	19
个人	小型普通客车	长安	5341
个人	小型普通客车	飞碟	3
个人	微型普通客车	长安	2
单位		五菱宏光	200
单位		欧诺	28
单位	中型专用校车	五菱	1
单位	中型专用校车	大力	3
单位	中型专用校车	宇通	8
单位	中型专用校车	海格	16
单位	中型普通客车	东风	71
单位	中型普通客车	中通	140
单位	中型普通客车	五菱	88
单位	中型普通客车	依维柯	67
单位	中型普通客车	南骏	26
单位	中型普通客车	合客	2
单位	中型普通客车	大力	4
单位	中型普通客车	大通	10
单位	中型普通客车	大马	53
单位	中型普通客车	宇通	33
单位	中型普通客车	少林	142
单位	中型普通客车	春洲	1
单位	中型普通客车	梅赛德斯-奔驰	7
单位	中型普通客车	汇众	14
单位	中型普通客车	江淮	27
单位	中型普通客车	江铃	9
单位	中型普通客车	江铃全顺	166
单位	中型普通客车	海格	7
单位	中型普通客车	申龙	1
单位	中型普通客车	福田	19
单位	中型普通客车	舒驰	31
单位	中型普通客车	解放	2
单位	中型普通客车	金旅	35
单位	中型普通客车	金杯	66
单位	中型普通客车	金龙	98
单位	中型普通客车	长安	15
单位	中型普通客车	长鹿	5
单位	中型普通客车	骊山	10
单位	中型普通客车	黄河	3
单位	大型专用校车	东风	14
单位	大型专用校车	大力	5
单位	大型专用校车	大通	9
单位	大型专用校车	宇通	88
单位	大型专用校车	楚风	2
单位	大型专用校车	海格	77
单位	大型专用校车	金旅	2
单位	大型专用校车	金龙	23
单位	大型专用校车	长安	1
单位	大型双层客车	安凯	1
单位	大型普通客车		15
单位	大型普通客车	三一	6
单位	大型普通客车	东风	96
单位	大型普通客车	中通	309
单位	大型普通客车	亚星	25
单位	大型普通客车	依维柯	11
单位	大型普通客车	北方	16
单位	大型普通客车	南骏	124
单位	大型普通客车	友谊	14
单位	大型普通客车	合客	15
单位	大型普通客车	同心	1
单位	大型普通客车	大力	11
单位	大型普通客车	大汉	1
单位	大型普通客车	大马	2
单位	大型普通客车	宇通	827
单位	大型普通客车	安凯	33
单位	大型普通客车	少林	297
单位	大型普通客车	山西	2
单位	大型普通客车	广汽	9
单位	大型普通客车	恒通客车	85
单位	大型普通客车	扬子江	146
单位	大型普通客车	柯斯达	83
单位	大型普通客车	梅赛德斯-奔驰	10
单位	大型普通客车	楚风	2
单位	大型普通客车	欧曼	1
单位	大型普通客车	江淮	2
单位	大型普通客车	江铃	29
单位	大型普通客车	江铃全顺	27
单位	大型普通客车	海格	151
单位	大型普通客车	申龙	22
单位	大型普通客车	福田	27
单位	大型普通客车	舒驰	7
单位	大型普通客车	衡山	6
单位	大型普通客车	西沃	3
单位	大型普通客车	解放	10
单位	大型普通客车	邦乐	4
单位	大型普通客车	金旅	94
单位	大型普通客车	金龙	286
单位	大型普通客车	长安	68
单位	大型普通客车	长鹿	1
单位	大型普通客车	青年	45
单位	大型普通客车	骊山	57
单位	大型普通客车	黄河	133
单位	大型普通客车	黄海	141
单位	大型铰接客车	黄海	3
单位	小型专用客车	航天	4
单位	小型专用客车	雨花	1
单位	小型普通客车		1
单位	小型普通客车	东南	6
单位	小型普通客车	东风	376
单位	小型普通客车	中誉	2
单位	小型普通客车	五菱	2267
单位	小型普通客车	五菱宏光	102
单位	小型普通客车	依维柯	79
单位	小型普通客车	力帆	8
单位	小型普通客车	北京	57
单位	小型普通客车	吉奥	3
单位	小型普通客车	哈飞	1
单位	小型普通客车	大通	32
单位	小型普通客车	奥路卡	24
单位	小型普通客车	尼桑	8
单位	小型普通客车	开瑞	36
单位	小型普通客车	昌河	21
单位	小型普通客车	昌河铃木	7
单位	小型普通客车	松花江	12
单位	小型普通客车	梅赛德斯-奔驰	1
单位	小型普通客车	欧诺	14
单位	小型普通客车	汇众	5
单位	小型普通客车	江铃全顺	206
单位	小型普通客车	海格	1
单位	小型普通客车	神剑	37
单位	小型普通客车	福田	13
单位	小型普通客车	航天	113
单位	小型普通客车	解放	95
单位	小型普通客车	通家福	2
单位	小型普通客车	金旅	4
单位	小型普通客车	金杯	97
单位	小型普通客车	金龙	2
单位	小型普通客车	长城	5
单位	小型普通客车	长安	1085
单位	小型普通客车	飞碟	3
Time taken: 1.664 seconds, Fetched: 195 row(s)
```

统计不同品牌的车在每个月的销售量分布

```sql
hive> select brand,month,count(1) from cars 
    > group by brand,month;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20210304104330_7bee7331-ac6b-4a88-aa5f-7ebfb70b4380
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
2021-03-04 10:43:31,452 Stage-1 map = 100%,  reduce = 0%
2021-03-04 10:43:32,454 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1721699862_0025
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 785383316 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
NULL	NULL	273
	NULL	5
	11	23
一汽佳星	9	1
一汽佳星	10	2
三一	7	6
东南	1	7
东南	2	1
东南	3	2
东南	4	2
东南	5	1
东南	6	1
东南	7	3
东南	8	1
东南	10	1
东南	12	1
东风	1	1029
东风	2	347
东风	3	541
东风	4	385
东风	5	394
东风	6	286
东风	7	323
东风	8	272
东风	9	275
东风	10	460
东风	11	534
东风	12	491
中誉	1	1
中誉	2	2
中誉	5	1
中誉	10	2
中誉	11	1
中誉	12	1
中通	1	77
中通	2	14
中通	3	95
中通	4	2
中通	5	93
中通	6	21
中通	7	18
中通	8	8
中通	9	3
中通	10	25
中通	11	18
中通	12	76
五菱	1	5589
五菱	2	2226
五菱	3	3557
五菱	4	2389
五菱	5	3351
五菱	6	2302
五菱	7	2893
五菱	8	2980
五菱	9	3422
五菱	10	5278
五菱	11	4809
五菱	12	4963
五菱宏光	1	1526
五菱宏光	2	578
五菱宏光	3	839
五菱宏光	4	747
亚星	2	4
亚星	3	11
亚星	6	9
亚星	12	1
众泰	3	1
众泰	6	1
众泰	7	2
众泰	8	2
众泰	11	1
众泰	12	1
依维柯	1	12
依维柯	2	8
依维柯	3	28
依维柯	4	24
依维柯	5	34
依维柯	6	30
依维柯	7	14
依维柯	8	23
依维柯	9	6
依维柯	10	27
依维柯	11	25
依维柯	12	30
俊风	5	2
俊风	6	1
俊风	7	1
俊风	10	1
力帆	1	29
力帆	2	8
力帆	3	20
力帆	4	11
力帆	5	10
力帆	6	9
力帆	7	5
力帆	8	3
力帆	9	5
力帆	10	8
力帆	11	10
力帆	12	1
北京	1	162
北京	2	86
北京	3	179
北京	4	145
北京	5	147
北京	6	136
北京	7	175
北京	8	245
北京	9	251
北京	10	356
北京	11	353
北京	12	399
北方	3	9
北方	4	1
北方	5	4
北方	7	1
北方	11	1
南骏	1	6
南骏	3	10
南骏	4	20
南骏	5	14
南骏	6	29
南骏	7	2
南骏	8	4
南骏	10	23
南骏	11	5
南骏	12	37
友谊	6	14
合客	1	4
合客	2	5
合客	4	1
合客	6	3
合客	8	2
合客	11	1
合客	12	3
吉奥	2	3
吉奥	3	3
吉奥	4	4
吉奥	6	4
吉奥	7	4
吉奥	8	2
吉奥	9	2
吉奥	10	7
吉奥	11	8
吉奥	12	8
同心	3	1
同心	10	1
哈飞	1	2
哈飞	3	1
哈飞	4	1
哈飞	5	2
哈飞	6	1
哈飞	7	1
哈飞	8	1
哈飞	10	1
哈飞	11	1
哈飞	12	1
大力	1	11
大力	4	2
大力	5	8
大力	6	1
大力	12	1
大汉	1	1
大通	1	12
大通	2	2
大通	3	6
大通	4	6
大通	5	20
大通	6	6
大通	7	11
大通	8	4
大通	9	2
大通	10	4
大通	11	7
大通	12	9
大马	1	3
大马	2	1
大马	3	3
大马	4	15
大马	5	4
大马	6	4
大马	7	8
大马	8	6
大马	9	4
大马	10	6
大马	11	4
大马	12	7
奥路卡	1	84
奥路卡	2	28
奥路卡	3	50
奥路卡	4	40
奥路卡	5	43
奥路卡	6	27
奥路卡	7	36
奥路卡	8	33
奥路卡	9	24
奥路卡	10	32
奥路卡	11	17
奥路卡	12	12
宇通	1	133
宇通	2	65
宇通	3	101
宇通	4	55
宇通	5	71
宇通	6	116
宇通	7	35
宇通	8	39
宇通	9	33
宇通	10	105
宇通	11	74
宇通	12	142
安凯	2	10
安凯	3	1
安凯	4	2
安凯	6	3
安凯	7	5
安凯	9	1
安凯	10	10
安凯	11	1
安凯	12	1
少林	1	120
少林	2	20
少林	3	23
少林	4	10
少林	5	24
少林	6	82
少林	7	28
少林	8	64
少林	9	36
少林	10	78
少林	11	17
少林	12	37
尼桑	3	5
尼桑	4	7
山西	8	2
广汽	1	1
广汽	2	1
广汽	3	1
广汽	5	3
广汽	6	1
广汽	7	2
开瑞	1	65
开瑞	2	18
开瑞	3	23
开瑞	4	29
开瑞	5	36
开瑞	6	14
开瑞	7	17
开瑞	8	24
开瑞	9	27
开瑞	10	35
开瑞	11	44
开瑞	12	43
恒通客车	8	20
恒通客车	9	15
恒通客车	10	50
恒通客车	12	4
扬子江	5	23
扬子江	11	43
扬子江	12	80
昌河	1	48
昌河	2	13
昌河	3	33
昌河	4	23
昌河	5	24
昌河	6	13
昌河	7	5
昌河	8	1
昌河	9	6
昌河	10	5
昌河	11	7
昌河	12	4
昌河铃木	1	1
昌河铃木	4	2
昌河铃木	6	1
昌河铃木	7	5
昌河铃木	8	1
昌河铃木	10	3
春洲	7	1
松花江	1	38
松花江	2	15
松花江	3	20
松花江	4	14
松花江	5	5
松花江	6	2
松花江	7	2
松花江	8	22
松花江	10	1
松花江	11	3
松花江	12	1
柯斯达	1	12
柯斯达	2	4
柯斯达	3	5
柯斯达	4	3
柯斯达	5	14
柯斯达	6	2
柯斯达	7	12
柯斯达	8	9
柯斯达	9	2
柯斯达	10	5
柯斯达	11	7
柯斯达	12	14
梅赛德斯-奔驰	1	5
梅赛德斯-奔驰	2	1
梅赛德斯-奔驰	4	3
梅赛德斯-奔驰	6	1
梅赛德斯-奔驰	7	1
梅赛德斯-奔驰	8	4
梅赛德斯-奔驰	9	1
梅赛德斯-奔驰	11	2
梅赛德斯-奔驰	12	1
楚风	1	1
楚风	4	1
楚风	11	1
楚风	12	1
欧曼	1	1
欧诺	1	167
欧诺	2	57
欧诺	3	104
欧诺	4	74
汇众	1	2
汇众	2	1
汇众	4	2
汇众	5	5
汇众	6	3
汇众	7	1
汇众	10	3
汇众	11	2
汇众	12	3
江淮	1	6
江淮	2	6
江淮	3	11
江淮	4	5
江淮	5	1
江淮	6	5
江淮	7	2
江淮	8	1
江淮	9	4
江淮	10	4
江淮	12	4
江铃	1	10
江铃	3	5
江铃	5	2
江铃	6	6
江铃	8	1
江铃	9	2
江铃	10	3
江铃	11	3
江铃	12	6
江铃全顺	1	55
江铃全顺	2	29
江铃全顺	3	73
江铃全顺	4	59
江铃全顺	5	60
江铃全顺	6	92
江铃全顺	7	36
江铃全顺	8	50
江铃全顺	9	72
江铃全顺	10	57
江铃全顺	11	54
江铃全顺	12	46
海格	1	7
海格	2	46
海格	3	33
海格	4	4
海格	5	21
海格	6	12
海格	7	26
海格	8	11
海格	9	22
海格	10	21
海格	11	27
海格	12	24
申龙	1	4
申龙	5	1
申龙	6	12
申龙	7	1
申龙	10	3
申龙	11	1
申龙	12	1
神剑	7	1
神剑	8	5
神剑	9	6
神剑	10	10
神剑	11	26
神剑	12	11
福田	1	12
福田	2	4
福田	3	16
福田	4	12
福田	5	10
福田	6	18
福田	7	12
福田	8	8
福田	9	10
福田	10	7
福田	11	11
福田	12	5
舒驰	1	3
舒驰	3	6
舒驰	4	2
舒驰	6	11
舒驰	7	6
舒驰	8	5
舒驰	9	1
舒驰	10	1
舒驰	11	1
舒驰	12	2
航天	1	37
航天	2	22
航天	3	35
航天	4	21
航天	5	19
航天	6	21
航天	7	10
航天	8	14
航天	9	10
航天	10	12
航天	11	11
航天	12	29
衡山	2	1
衡山	3	1
衡山	6	4
西沃	1	2
西沃	10	1
解放	1	77
解放	2	27
解放	3	35
解放	4	33
解放	5	38
解放	6	50
解放	7	30
解放	8	28
解放	9	28
解放	10	27
解放	11	41
解放	12	42
通家福	1	8
通家福	2	4
通家福	3	3
通家福	4	9
通家福	6	2
邦乐	6	2
邦乐	9	1
邦乐	12	1
野马	8	28
金旅	1	14
金旅	2	4
金旅	3	8
金旅	4	9
金旅	5	17
金旅	6	24
金旅	7	8
金旅	8	5
金旅	9	6
金旅	10	12
金旅	11	11
金旅	12	30
金杯	1	26
金杯	2	17
金杯	3	29
金杯	4	17
金杯	5	64
金杯	6	35
金杯	7	44
金杯	8	60
金杯	9	55
金杯	10	67
金杯	11	74
金杯	12	57
金龙	1	50
金龙	2	15
金龙	3	46
金龙	4	36
金龙	5	52
金龙	6	66
金龙	7	35
金龙	8	32
金龙	9	34
金龙	10	31
金龙	11	34
金龙	12	20
长城	6	2
长城	7	23
长安	1	855
长安	2	392
长安	3	568
长安	4	397
长安	5	525
长安	6	371
长安	7	595
长安	8	466
长安	9	510
长安	10	541
长安	11	647
长安	12	645
长鹿	10	4
长鹿	12	2
雨花	5	1
青年	1	19
青年	2	1
青年	3	2
青年	4	9
青年	5	1
青年	6	3
青年	9	4
青年	10	4
青年	11	1
青年	12	1
飞碟	3	1
飞碟	9	1
飞碟	10	1
飞碟	11	1
飞碟	12	2
骊山	1	13
骊山	2	1
骊山	3	4
骊山	5	1
骊山	6	23
骊山	9	1
骊山	10	4
骊山	11	2
骊山	12	18
黄河	1	12
黄河	6	20
黄河	8	1
黄河	10	3
黄河	11	99
黄河	12	1
黄海	1	54
黄海	2	16
黄海	4	2
黄海	5	5
黄海	6	1
黄海	7	3
黄海	8	1
黄海	9	6
黄海	10	10
黄海	11	11
黄海	12	37
Time taken: 2.355 seconds, Fetched: 540 row(s)
```











