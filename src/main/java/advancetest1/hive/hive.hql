--1、创建cars表，该表字段定义如表字段.txt：
create external table cars(
province string, --省份
month int, --月
city string, --市
county string, --区县
year int, --年
cartype string,--车辆型号
productor string,--制造商
brand string, --品牌
mold string,--车辆类型
owner string,--所有权
nature string, --使用性质
number int,--数量
ftype string,--发动机型号
outv int,--排量
power double, --功率
fuel string,--燃料种类
length int,--车长
width int,--车宽
height int,--车高
xlength int,--厢长
xwidth int,--厢宽
xheight int,--厢高
count int,--轴数
base int,--轴距
front int,--前轮距
norm string,--轮胎规格
tnumber int,--轮胎数
total int,--总质量
curb int,--整备质量
hcurb int,--核定载质量
passenger string,--核定载客
zhcurb int,--准牵引质量
business string,--底盘企业
dtype string,--底盘品牌
fmold string,--底盘型号
fbusiness string,--发动机企业
name string,--车辆名称
age int,--年龄
sex string --性别
)
row format delimited
fields terminated by '\t';

--2、统计车辆不同用途的数量分布
select nature,count(1) from cars group by nature;

--3、统计山西省2013年每个月的汽车销售数量的比例
select month,round(s1/s2,2) per from
(select month,count(1) s1 from cars where month is not null group by month) a,
(select count(1) s2 from cars) b;

--4 统计山西省2013年各市、区县的汽车销售的分布
select city,county,count(1) num from cars
where city is not null and county is not null
group by city,county;

--5 统计买车的男女比例
select sex,round(s1/s2,2) pre from
(select sex,count(1) s1 from cars where sex !='' group by sex) a,
(select count(1) s2 from cars where sex !='') b ;

--6 统计的车的所有权、 车辆类型和品牌的分布
select owner,mold,brand,count(1) from cars
group by owner,mold,brand;

--7 统计不同品牌的车在每个月的销售量分布
select brand,month,count(1) from cars
group by brand,month;