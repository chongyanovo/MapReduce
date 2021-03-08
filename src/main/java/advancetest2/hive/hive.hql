create table data(
area string,
shop string,
good_Jan string,
sale_Jan int,
good_Feb string,
sale_Feb int)
row format delimited fields terminated by ','; 

load data inpath '/demo2/out1_2/part-r-00000' into table data;

--1 统计该表中有几个地区
select count(distinct area) from data;

--2、统计该表中有几个商家
select count(distinct shop) from data;

--3、统计该表中有几种商品
select count(distinct good) from 
(select distinct good_Jan good from data
union all
select distinct  good_Feb good from data) tmp;

--4、统计2月份比1月份销量增加幅度最大的是哪个商品
销量幅度
select good_Jan,(sale_Jan - sale_Feb) sale from data sort by sale desc limit 1;

--5、统计每个地区1月份所有商品的销量总和，并按从高到低的顺序排序
select area,sum(sale_Jan) sale from data group by area sort by sale desc;

--6、统计每个商家的每个产品的1月份销售数据和2月份销售数据的总和
select shop,good_Jan,sum(sale_Jan + sale_Feb) sale from data group by shop,good_Jan;

--7、统计每个商家1月份销售的所有商品名称和对应的销售额
select shop,good_Jan,sum(sale_Jan) from data group by shop,good_Jan;

