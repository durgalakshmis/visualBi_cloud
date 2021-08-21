use visualbi;

CREATE TABLE `regionmaster` (
  `RegionCode` int NOT NULL,
  `RegionName` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`RegionCode`)
);

insert into  `visualbi`.`region master`  values (1001,"Central");
insert into  `visualbi`.`region master`  values (2001,"Midwest");

CREATE TABLE ProductMaster (
ProductCode int,
ProductName	VARCHAR(100),
UnitPrice decimal,
ValidFromPeriod	char(7),
ValidToPeriod char(7)
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/productmaster-data1.csv' INTO TABLE ProductMaster
  FIELDS TERMINATED BY ','  
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;

CREATE TABLE Revenue (
RegionCode int,
Product int,
Year_ int,
Month_	int, 
Revenue decimal,
QuantitySold decimal
#constraint p_rc_fk FOREIGN KEY (RegionCode) REFERENCES RegionMaster(RegionCode) ON DELETE CASCADE
#constraint p_prd_fk FOREIGN KEY (Product) REFERENCES ProductMaster(ProductCode) ON DELETE CASCADE
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/revenues-data1.csv' INTO TABLE Revenue
  FIELDS TERMINATED BY ','  
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;
  
 # 1.	Query to display the Revenue, Cost to Product and Net Revenue for each region for year 2020 and 2021. 
 select  r.regioncode, m.regionname,  r.year_ , round(sum(revenue),2) as revenue,  
 sum(r.QuantitySold * p.UnitPrice) as cost_to_product ,
 round((sum(revenue)*1000000)- sum(r.QuantitySold * p.UnitPrice),2) as net_revenue from revenue r 
 left join regionmaster m on r.regioncode=m.regioncode
 left join productmaster p on r.product=p.productcode and r.year_+r.month_/100 between ValidFromPeriod and ValidToPeriod 
 group by r.regioncode, r.year_  order by r.year_;

 # 2.   Design a query to display the company’s YTD Revenue, YTD Cost to Product and YTD Net Revenue for each month for both the years.
with cte1 as
(
select  r.year_ , r.month_, round(sum(revenue*1000000),0) as revenue,  
round(sum(r.QuantitySold * p.UnitPrice),0) as cost_to_product ,
round((sum(revenue)*1000000)- sum(r.QuantitySold * p.UnitPrice),0) as net_revenue from revenue r 
left join regionmaster m on r.regioncode=m.regioncode
left join productmaster p on r.product=p.productcode and r.year_+r.month_/100 between ValidFromPeriod and ValidToPeriod 
group by r.year_, r.month_  order by r.year_, r.month_
)
select year_, 
month_, 
sum(revenue) over (partition by year_ order by year_, month_) as YTD_Revenue,
sum(cost_to_product) over (partition by year_ order by year_, month_) as YTD_Cost_to_Product,
sum(net_revenue) over (partition by year_ order by year_, month_) as YTD_Net_Revenue
from cte1;
  
  
  
# 3.   Design a query to display the Net Share of each product in Revenue for each region for year 2020.
  select e.regioncode, r.regionname , e.product,  e.year_, 
  (select sum(revenue) from revenue 
  where year_ =2020 
  and product=e.product and regioncode=e.regioncode) /
  (select sum(revenue) from revenue 
  where year_ =2020 
  and e.regioncode=regioncode) as net_share   from revenue e 
  join regionmaster r on e.regioncode = r.regioncode
  group by e.regioncode , e.year_ , e.product having e.year_=2020
 order by regioncode ;
  
  
# 4.   	Query to display the top product by Revenue in each region for both years.

  select regioncode, regionname, FIRST_VALUE(product) OVER (partition by regioncode,regionname  order by revenuesum desc ) as top_product_code,  round(revenuesum,2) as product_revenue from 
 ( select e.regioncode, r.regionname , e.product,  sum(e.revenue) as revenuesum  from revenue e 
  join regionmaster r on 
  e.regioncode = r.regioncode
  group by e.regioncode, r.regionname , e.product) as t group by  regioncode, regionname ;
  
  #5.	Query to display Average rolling previous 3 months Net Revenue for each month and region.
with cte1 as
(
select  r.regioncode, r.year_ , r.month_, 
round((sum(revenue)*1000000)- sum(r.QuantitySold * p.UnitPrice),0) as net_revenue from revenue r 
left join regionmaster m on r.regioncode=m.regioncode
left join productmaster p on r.product=p.productcode and r.year_+r.month_/100 between ValidFromPeriod and ValidToPeriod 
group by 1,2,3  order by 1,2,3
)
select regioncode, 
year_, 
month_,
round(avg(net_revenue) over (order by regioncode,year_,month_ rows between 3 preceding and 1 preceding),0) as prev_3_month_avg
from cte1; 

# 6.    Query to display the Average Selling Price for each month compared to the Average Selling Price in rolling previous 6 months.

with cte1 as
(
 select  r.year_ , r.month_,
 sum(r.QuantitySold) as qty_sold,
 round(sum(revenue*1000000),2) as revenue,
 round(sum(revenue*1000000)/sum(r.QuantitySold),2) as avg_sp
 from revenue r 
 left join regionmaster m on r.regioncode=m.regioncode
 left join productmaster p on r.product=p.productcode and r.year_+r.month_/100 between ValidFromPeriod and ValidToPeriod 
 group by 1,2 order by 1,2
 )
 select 
year_,
month_,
qty_sold,
round(revenue,0), 
round(avg_sp,0),
round(sum(revenue) over (order by year_, month_ rows between 6 preceding and 1 preceding)/
sum(qty_sold) over (order by year_, month_ rows between 6 preceding and 1 preceding),0) as last_6_avg_sp
from cte1;
