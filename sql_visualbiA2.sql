
use visualbi_assess2;

CREATE TABLE `continent` (
  `continent_code` int NOT NULL,
  `continent_name` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`continent_code`)
) ;

CREATE TABLE `country` (
  `country_code` int NOT NULL,
  `country_name` varchar(45) DEFAULT NULL,
  `continent_code` int DEFAULT NULL,
  PRIMARY KEY (`country_code`),
  KEY `continent_code_idx` (`continent_code`),
  CONSTRAINT `continent_code` FOREIGN KEY (`continent_code`) REFERENCES `continent` (`continent_code`)
) ;

CREATE TABLE `region` (
  `region_code` int NOT NULL,
  `region_name` varchar(45) DEFAULT NULL,
  `country_code` int NOT NULL,
  PRIMARY KEY (`region_code`,`country_code`)
) ;

CREATE TABLE `city` (
  `city_code` int NOT NULL,
  `city_name` varchar(45) DEFAULT NULL,
  `region_code` int NOT NULL,
  `country_code` int NOT NULL,
  PRIMARY KEY (`city_code`,`region_code`,`country_code`),
  KEY `region_code_idx` (`region_code`),
  CONSTRAINT `region_code` FOREIGN KEY (`region_code`) REFERENCES `region` (`region_code`)
) ;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/continent-data2.csv' INTO TABLE `visualbi_assess2`.`continent`  
 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/country-data2.csv' INTO TABLE `visualbi_assess2`.`country`  
 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/regions-data2.csv' INTO TABLE `visualbi_assess2`.`region` 
  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cities-data2.csv' INTO TABLE `visualbi_assess2`.`city` 
  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;



# 1.	Write a SQL code to create a parent-child representation of geography hierarchy, flat representation of the hierarchy. 

WITH 
   allnodes as 
    ((SELECT Continent_code AS Node_code, Continent_name AS Node_name, 1 AS Node_level_no, null AS Parent_node_code FROM Continents) UNION ALL
     (SELECT Country_code AS Node_code, Country_name AS Node_name, 2 AS Node_level_no, Continent_code AS Parent_node_code FROM Countries) UNION ALL 
     (SELECT CONCAT(Country_code,"-",Region_code) AS Node_code, Region_name AS Node_name, 3 AS Node_level_no, Country_code AS Parent_node_code FROM Regions) UNION ALL
     (SELECT CONCAT(Country_code,"-",Region_code,"-", City_code) AS Node_code, City_name AS Node_name, 4 AS Node_level_no, CONCAT(Country_code,"-",Region_code) AS Parent_node_code FROM Cities))
   SELECT a.Node_code, a.Node_level_no, a.Parent_node_code, a.Node_level_no -1 AS Parent_node_level_no, a.Node_name, b.Node_name AS Parent_Node_name from allnodes a
   LEFT OUTER JOIN allnodes b ON a.Parent_node_code = b.Node_code AND a.Node_level_no = b.Node_level_no + 1;


# 2.	Now using the output of Query 1 as the input table, write SQL code to represent the flow of each node from the topmost parent node.

WITH 
   allnodes as 
    ((SELECT Continent_code AS Node_code, Continent_name AS Node_name, 1 AS Node_level_no, null AS Parent_node_code FROM Continent) UNION ALL
     (SELECT Country_code AS Node_code, Country_name AS Node_name, 2 AS Node_level_no, Continent_code AS Parent_node_code FROM Country) UNION ALL 
     (SELECT CONCAT(Country_code,"-",Region_code) AS Node_code, Region_name AS Node_name, 3 AS Node_level_no, Country_code AS Parent_node_code FROM Region) UNION ALL
     (SELECT CONCAT(Country_code,"-",Region_code,"-", City_code) AS Node_code, City_name AS Node_name, 4 AS Node_level_no, CONCAT(Country_code,"-",Region_code) AS Parent_node_code FROM City)),
   allnodes_with_pname AS (SELECT a.Node_code, a.Node_level_no, a.Parent_node_code, a.Node_level_no -1 AS Parent_node_level_no, a.Node_name, b.Node_name AS Parent_Node_name from allnodes a LEFT OUTER JOIN allnodes b ON a.Parent_node_code = b.Node_code AND a.Node_level_no = b.Node_level_no + 1),
   query2 AS ( WITH RECURSIVE Cte AS (
					SELECT *, CAST(Node_name AS Char(100)) AS Hierarchy_flow FROM allnodes_with_pname WHERE Parent_node_code IS NULL
                    UNION ALL 
                    SELECT a.*, CONCAT(b.Hierarchy_flow, " --> ", a.Node_name) AS Hierarchy_flow FROM allnodes_with_pname a INNER JOIN  Cte b ON a.Parent_node_code = b.Node_code AND a.Node_level_no = b.Node_level_no + 1   
					)
				SELECT * FROM Cte)
    SELECT * FROM query2;
   
   