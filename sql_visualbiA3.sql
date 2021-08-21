use visual_assess3;

CREATE TABLE `visual_assess3`.`order_header_source_ day_1` (
  `Order_No` INT NOT NULL,
  `Order_Date` VARCHAR(10) NULL,
  `Order_Type` INT NULL,
  `Customer_ID` INT NULL,
  `Shipping_type` INT NULL,
  `Created_On` VARCHAR(10) NULL,
  `Updated_On` VARCHAR(10) NULL,
  PRIMARY KEY (`Order_No`));

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/HD1.csv' 
  INTO TABLE `visual_assess3`.`order_header_source_ day_1`   FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
 

CREATE TABLE `visual_assess3`.`order_header_source_ day_2` (
  `Order_No` INT NOT NULL,
  `Order_Date` VARCHAR(10) NULL,
  `Order_Type` INT NULL,
  `Customer_ID` INT NULL,
  `Shipping_type` INT NULL,
  `Created_On` VARCHAR(10) NULL,
  `Updated_On` VARCHAR(10) NULL,
  PRIMARY KEY (`Order_No`));
  
  LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/HD2.csv' 
  INTO TABLE `visual_assess3`.`order_header_source_ day_2`   FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

  
  CREATE TABLE `visual_assess3`.`order_header_source_ day_3` (
  `Order_No` INT NOT NULL,
  `Order_Date` VARCHAR(10) NULL,
  `Order_Type` INT NULL,
  `Customer_ID` INT NULL,
  `Shipping_type` INT NULL,
  `Created_On` VARCHAR(10) NULL,
  `Updated_On` VARCHAR(10) NULL,
  PRIMARY KEY (`Order_No`));

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/HD3.csv' 
  INTO TABLE `visual_assess3`.`order_header_source_ day_3`   FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;


CREATE TABLE `visual_assess3`.`order_item_source_day_1` (
  `Order_No` INT NOT NULL,
  `Item_No` INT NOT NULL,
  `Product_ID` INT NULL,
  `Quantity` INT NULL,
  `Price` INT NULL,
  `Created_On` VARCHAR(10) NULL,
  `Updated_On` VARCHAR(10) NULL);

 LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ID1.csv' 
  INTO TABLE `visual_assess3`.`order_item_source_day_1`   FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
  

CREATE TABLE `visual_assess3`.`order_item_source_day_2` (
  `Order_No` INT NOT NULL,
  `Item_No` INT NOT NULL,
  `Product_ID` INT NULL,
  `Quantity` INT NULL,
  `Price` INT NULL,
  `Created_On` VARCHAR(10) NULL,
  `Updated_On` VARCHAR(10) NULL);

 LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ID2.csv' 
  INTO TABLE `visual_assess3`.`order_item_source_day_2`   FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
  

CREATE TABLE `visual_assess3`.`order_item_source_day_3` (
  `Order_No` INT NOT NULL,
  `Item_No` INT NOT NULL,
  `Product_ID` INT NULL,
  `Quantity` INT NULL,
  `Price` INT NULL,
  `Created_On` VARCHAR(10) NULL,
  `Updated_On` VARCHAR(10) NULL );
  
  LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ID3.csv' 
  INTO TABLE `visual_assess3`.`order_item_source_day_3`   FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
  
  #1.	Write queries to perform a full load and eventually incremental/delta loads from the source tables to models
  #created for each of the tables individually. Full loads happen on Day 1, on Day 2 and 3 we need incremental data loads.
  
  #headers load
  WITH h_loads as 
    ((SELECT h1.order_no, h1.order_date,h1.order_type, h1.customer_id, h1.shipping_type, h1.created_on, h1.updated_on 
    from  `visual_assess3`.`order_header_source_ day_1` as h1  ) UNION ALL
    
  (SELECT h2.order_no, h2.order_date,h2.order_type, h2.customer_id, h2.shipping_type, h2.created_on, h2.updated_on 
    from  `visual_assess3`.`order_header_source_ day_2` as h2
    where h2.updated_on > (select max(updated_on) from `visual_assess3`.`order_header_source_ day_1`)) UNION ALL
    
    (SELECT h3.order_no, h3.order_date,h3.order_type, h3.customer_id, h3.shipping_type, h3.created_on, h3.updated_on 
    from  `visual_assess3`.`order_header_source_ day_3` as h3  
    where h3.updated_on > (select max(updated_on) from `visual_assess3`.`order_header_source_ day_2`)  )
    ) 
     select order_no, order_date,order_type, customer_id, shipping_type, created_on, max(updated_on)  as updated_on from h_loads
    group by order_no, created_on ;
  
  select count(*) from `visual_assess3`.`order_header_source_ day_3`;
  
  #item load 
  WITH i_loads as 
    ((SELECT i1.order_no, i1.item_no, i1.product_id, i1.quantity, i1.price, i1.created_on, i1.updated_on
    from  `visual_assess3`.`order_item_source_day_1` as i1  ) UNION ALL
    
  (SELECT i2.order_no, i2.item_no, i2.product_id, i2.quantity, i2.price, i2.created_on, i2.updated_on
    from  `visual_assess3`.`order_item_source_day_2` as i2 
    where i2.updated_on > (select max(updated_on) from `visual_assess3`.`order_item_source_day_1`)) UNION ALL
    
    (SELECT i3.order_no, i3.item_no, i3.product_id, i3.quantity, i3.price, i3.created_on, i3.updated_on
    from  `visual_assess3`.`order_item_source_day_3` as i3  
    where i3.updated_on > (select max(updated_on) from `visual_assess3`.`order_item_source_day_2`)  )
    ) 
    select order_no, item_no, product_id, quantity, price, created_on, max(updated_on) as updated_on from i_loads
    group by order_no, item_no,Product_ID ,created_on ;
  
  # 2.	Write queries to perform a full load and eventually incremental/delta loads from the source tables 
  #to a single model that is a combination/join of the 2 tables, Order_Header and Order_item.
  
  WITH join_load as (
	(SELECT h1.order_no, h1.order_date,h1.order_type, h1.customer_id, h1.shipping_type, h1.created_on as head_created, h1.updated_on as head_updated,
    i1.item_no, i1.product_id, i1.quantity, i1.price, i1.created_on, i1.updated_on
    from  `visual_assess3`.`order_header_source_ day_1` as h1 join `visual_assess3`.`order_item_source_day_1` as i1
    on h1.order_no = i1.order_no)
    UNION ALL
    (SELECT h2.order_no, h2.order_date,h2.order_type, h2.customer_id, h2.shipping_type, h2.created_on as head_created, h2.updated_on as head_updated,
    i2.item_no, i2.product_id, i2.quantity, i2.price, i2.created_on, i2.updated_on
    from  `visual_assess3`.`order_header_source_ day_2` as h2 join `visual_assess3`.`order_item_source_day_2` as i2
    on h2.order_no = i2.order_no where i2.updated_on > (select max(updated_on) from `visual_assess3`.`order_item_source_day_1`) or
    h2.updated_on > (select max(updated_on) from `visual_assess3`.`order_header_source_ day_1`)
    )
    UNION ALL
    (SELECT h3.order_no, h3.order_date,h3.order_type, h3.customer_id, h3.shipping_type, h3.created_on as head_created, h3.updated_on as head_updated,
    i3.item_no, i3.product_id, i3.quantity, i3.price, i3.created_on, i3.updated_on
    from  `visual_assess3`.`order_header_source_ day_3` as h3 join `visual_assess3`.`order_item_source_day_3` as i3
    on h3.order_no = i3.order_no
    where i3.updated_on > (select max(updated_on) from `visual_assess3`.`order_item_source_day_2`) or
    h3.updated_on > (select max(updated_on) from `visual_assess3`.`order_header_source_ day_2`)
    ) )
	select order_no, order_date,order_type, customer_id, shipping_type, created_on as head_created, updated_on as head_updated,
    order_no, item_no, product_id, quantity, price, created_on from join_load
    group by order_no, item_no,Product_ID ,created_on ;
  
  
  