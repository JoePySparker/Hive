CREATE DATABASE myhivebook;
CREATE DATABASE IF NOT EXISTS myhivebook;
CREATE DATABASE IF NOT EXISTS myhivebook
. . . . . . .> COMMENT 'hive database demo'
. . . . . . .> LOCATION '/hdfs/directory'
. . . . . . .> WITH DBPROPERTIES ('creator'='dayongd','date'='2015-01-01');
SHOW DATABASES;
DESCRIBE DATABASE default;
DROP DATABASE IF EXISTS myhivebook;
ALTER DATABASE myhivebook
 SET OWNER user dayongd;

When there is data already in HDFS, an external Hive table can be created to describe the data. 
It is called EXTERNAL because the data in the external table is specified in the LOCATION properties instead of the default warehouse directory.
When keeping data in the internal tables, Hive fully manages the life cycle of the table and data. 
This means the data is removed once the internal table is dropped. If the external table is dropped, 
the table metadata is deleted but the data is kept. Most of the time, 
an external table is preferred to avoid deleting data along with tables by mistake. 
The following are DDLs for Hive internal and external table examples:
CREATE TABLE IF NOT EXISTS employee_internal
. . . . . . .> (
. . . . . . .>  name string,
. . . . . . .>  work_place ARRAY<string>,
. . . . . . .>  sex_age STRUCT<sex:string,age:int>,
. . . . . . .>  skills_score MAP<string,int>,
. . . . . . .>  depart_title MAP<STRING,ARRAY<STRING>>
. . . . . . .> )
. . . . . . .> COMMENT 'This is an internal table'
. . . . . . .> ROW FORMAT DELIMITED
. . . . . . .> FIELDS TERMINATED BY '|'
. . . . . . .> COLLECTION ITEMS TERMINATED BY ','
. . . . . . .> MAP KEYS TERMINATED BY ':'
. . . . . . .> STORED AS TEXTFILE;
> LOAD DATA LOCAL INPATH '/home/hadoop/employee.txt'
. . . . . . .> OVERWRITE INTO TABLE employee_internal;

> CREATE EXTERNAL TABLE employee_external
. . . . . . .> (
. . . . . . .>  name string,
. . . . . . .>  work_place ARRAY<string>,
. . . . . . .>  sex_age STRUCT<sex:string,age:int>,
. . . . . . .>  skills_score MAP<string,int>,
. . . . . . .>  depart_title MAP<STRING,ARRAY<STRING>>
. . . . . . .> )
. . . . . . .> COMMENT 'This is an external table'
. . . . . . .> ROW FORMAT DELIMITED
. . . . . . .> FIELDS TERMINATED BY '|'
. . . . . . .> COLLECTION ITEMS TERMINATED BY ','
. . . . . . .> MAP KEYS TERMINATED BY ':'
. . . . . . .> STORED AS TEXTFILE
. . . . . . .> LOCATION '/user/dayongd/employee';
LOAD DATA LOCAL INPATH '/home/hadoop/employee.txt'. . . . . . .> OVERWRITE
 INTO TABLE employee_external;

CREATE TABLE ctas_employee 
. . . . . . .> AS SELECT * FROM employee_external;

> CREATE TABLE cte_employee AS
. . . . . . .> WITH r1 AS 
. . . . . . .> (SELECT name FROM r2 
. . . . . . .> WHERE name = 'Michael'),
. . . . . . .> r2 AS 
. . . . . . .> (SELECT name FROM employee 
. . . . . . .> WHERE sex_age.sex= 'Male'),
. . . . . . .> r3 AS 
. . . . . . .> (SELECT name FROM employee 
. . . . . . .> WHERE sex_age.sex= 'Female')
. . . . . . .> SELECT * FROM r1 UNION ALL select * FROM r3;
No rows affected (61.852 seconds)

> SELECT * FROM cte_employee;

CREATE TABLE empty_ctas_employee AS 
. . . . . . .> SELECT * FROM employee_internal WHERE 1=2;

CREATE TABLE empty_like_employee
. . . . . . .> LIKE employee_internal;

SELECT COUNT(*) AS row_cnt 
. . . . . . .> FROM empty_ctas_employee;

SELECT COUNT(*) AS row_cnt 
. . . . . . .> FROM empty_like_employee;


DROP TABLE IF EXISTS empty_ctas_employee;

DROP TABLE IF EXISTS empty_like_employee;

SELECT * FROM cte_employee;

TRUNCATE TABLE cte_employee;


SELECT * FROM cte_employee;

ALTER TABLE cte_employee RENAME TO c_employee;


ALTER TABLE c_employee 
. . . . . . .> SET TBLPROPERTIES ('comment'='New name, comments');


ALTER TABLE c_employee 
. . . . . . .> SET LOCATION
. . . . . . .> 'hdfs://localhost:8020/user/dayongd/employee'; 


ALTER TABLE c_employee ENABLE NO_DROP; 
jdbc:hive2://> ALTER TABLE c_employee DISABLE NO_DROP; 
jdbc:hive2://> ALTER TABLE c_employee ENABLE OFFLINE;
jdbc:hive2://> ALTER TABLE c_employee DISABLE OFFLINE;

DESC employee_internal;
ALTER TABLE employee_internal 
. . . . . . .> CHANGE name employee_name string AFTER sex_age;

DESC employee_internal;

ALTER TABLE employee_internal 
. . . . . . .> CHANGE employee_name name string FIRST;


ALTER TABLE c_employee ADD COLUMNS (work string);

ALTER TABLE c_employee 
. . . . . . .> REPLACE COLUMNS (name string);


CREATE TABLE employee_partitioned
. . . . . . .> (
. . . . . . .>   name string,
. . . . . . .>   work_place ARRAY<string>,
. . . . . . .>   sex_age STRUCT<sex:string,age:int>,
. . . . . . .>   skills_score MAP<string,int>,
. . . . . . .>   depart_title MAP<STRING,ARRAY<STRING>>
. . . . . . .> )
. . . . . . .> PARTITIONED BY (Year INT, Month INT)
. . . . . . .> ROW FORMAT DELIMITED
. . . . . . .> FIELDS TERMINATED BY '|'
. . . . . . .> COLLECTION ITEMS TERMINATED BY ','
. . . . . . .> MAP KEYS TERMINATED BY ':';


> SHOW PARTITIONS employee_partitioned;

--Add multiple partitions
jdbc:hive2://> ALTER TABLE employee_partitioned ADD 
. . . . . . .> PARTITION (year=2014, month=11)        
. . . . . . .> PARTITION (year=2014, month=12);
No rows affected (0.248 seconds)

jdbc:hive2://> SHOW PARTITIONS employee_partitioned;


--Drop the partition
jdbc:hive2://> ALTER TABLE employee_partitioned
. . . . . . .> DROP IF EXISTS PARTITION (year=2014, month=11); 

jdbc:hive2://> SHOW PARTITIONS employee_partitioned;


> LOAD DATA LOCAL INPATH
. . . . . . .> '/home/dayongd/Downloads/employee.txt' 
. . . . . . .> OVERWRITE INTO TABLE employee_partitioned
. . . . . . .> PARTITION (year=2014, month=12);


> SELECT name, year, month FROM employee_partitioned;


ALTER TABLE table_name PARTITION partition_spec SET FILEFORMAT file_format;
ALTER TABLE table_name PARTITION partition_spec SET LOCATION 'full URI';
ALTER TABLE table_name PARTITION partition_spec ENABLE NO_DROP;
ALTER TABLE table_name PARTITION partition_spec ENABLE OFFLINE;
ALTER TABLE table_name PARTITION partition_spec DISABLE NO_DROP;
ALTER TABLE table_name PARTITION partition_spec DISABLE OFFLINE;
ALTER TABLE table_name PARTITION partition_spec CONCATENATE;

--Prepare another dataset and table for bucket table
jdbc:hive2://> CREATE TABLE employee_id
. . . . . . .> (
. . . . . . .>   name string,
. . . . . . .>   employee_id int,
. . . . . . .>   work_place ARRAY<string>,
. . . . . . .>   sex_age STRUCT<sex:string,age:int>,
. . . . . . .>   skills_score MAP<string,int>,
. . . . . . .>   depart_title MAP<string,ARRAY<string>>
. . . . . . .> )
. . . . . . .> ROW FORMAT DELIMITED
. . . . . . .> FIELDS TERMINATED BY '|'
. . . . . . .> COLLECTION ITEMS TERMINATED BY ','
. . . . . . .> MAP KEYS TERMINATED BY ':';

> LOAD DATA LOCAL INPATH 
. . . . . . .> '/home/dayongd/Downloads/employee_id.txt' 
. . . . . . .> OVERWRITE INTO TABLE employee_id


> CREATE TABLE employee_id_buckets
. . . . . . .> (
. . . . . . .>   name string,
. . . . . . .>   employee_id int,
. . . . . . .>   work_place ARRAY<string>,
. . . . . . .>   sex_age STRUCT<sex:string,age:int>,
. . . . . . .>   skills_score MAP<string,int>,
. . . . . . .>   depart_title MAP<string,ARRAY<string >>
. . . . . . .> )
. . . . . . .> CLUSTERED BY (employee_id) INTO 2 BUCKETS
. . . . . . .> ROW FORMAT DELIMITED
. . . . . . .> FIELDS TERMINATED BY '|'
. . . . . . .> COLLECTION ITEMS TERMINATED BY ','
. . . . . . .> MAP KEYS TERMINATED BY ':';


INSERT OVERWRITE TABLE employee_id_buckets 
. . . . . . .> SELECT * FROM employee_id;


> CREATE VIEW employee_skills
. . . . . . .> AS
. . . . . . .> SELECT name, skills_score['DB'] AS DB,
. . . . . . .> skills_score['Perl'] AS Perl, 
. . . . . . .> skills_score['Python'] AS Python,
. . . . . . .> skills_score['Sales'] as Sales, 
. . . . . . .> skills_score['HR'] as HR 
. . . . . . .> FROM employee;


ALTER VIEW employee_skills 
. . . . . . .> SET TBLPROPERTIES ('comment' = 'This is a view');


> ALTER VIEW employee_skills AS 
. . . . . . .> SELECT * from employee ;


DROP VIEW employee_skills; 

> WITH t1 AS (
. . . . . . .> SELECT * FROM employee
. . . . . . .> WHERE sex_age.sex = 'Male')
. . . . . . .> SELECT name, sex_age.sex AS sex FROM t1;


SELECT name, sex_age.sex AS sex
. . . . . . .> FROM
. . . . . . .> (
. . . . . . .>   SELECT * FROM employee
. . . . . . .>   WHERE sex_age.sex = 'Male'
. . . . . . .> ) t1;


SELECT name, sex_age.sex AS sex
. . . . . . .> FROM employee a
. . . . . . .> WHERE a.name IN
. . . . . . .> (SELECT name FROM employee
. . . . . . .> WHERE sex_age.sex = 'Male'
. . . . . . .> );


SELECT name, sex_age.sex AS sex
. . . . . . .> FROM employee a
. . . . . . .> WHERE EXISTS
. . . . . . .> (SELECT * FROM employee b
. . . . . . .> WHERE a.sex_age.sex = b.sex_age.sex 
. . . . . . .> AND b.sex_age.sex = 'Male'
. . . . . . .> );


CREATE TABLE IF NOT EXISTS employee_hr
. . . . . . .> (
. . . . . . .>   name string,
. . . . . . .>   employee_id int,
. . . . . . .>   sin_number string,
. . . . . . .>   start_date date
. . . . . . .> )
. . . . . . .> ROW FORMAT DELIMITED
. . . . . . .> FIELDS TERMINATED BY '|'
. . . . . . .> STORED AS TEXTFILE;


> LOAD DATA LOCAL INPATH 
. . . . . . .> '/home/Dayongd/employee_hr.txt' 
. . . . . . .> OVERWRITE INTO TABLE employee_hr;


SELECT emp.name, emph.sin_number
. . . . . . .> FROM employee emp
. . . . . . .> JOIN employee_hr emph ON emp.name = emph.name;


SELECT emp.name, empi.employee_id, emph.sin_number
. . . . . . .> FROM employee emp
. . . . . . .> JOIN employee_hr emph ON emp.name = emph.name
. . . . . . .> JOIN employee_id empi ON emp.name = empi.name;


SELECT emp.name
. . . . . . .> FROM employee emp
. . . . . . .> JOIN employee emp_b
. . . . . . .> ON emp.name = emp_b.name;


> SELECT emp.name, emph.sin_number
. . . . . . .> FROM employee emp, employee_hr emph
. . . . . . .> WHERE emp.name = emph.name;


> SELECT emp.name, empi.employee_id, emph.sin_number
. . . . . . .> FROM employee emp
. . . . . . .> JOIN employee_hr emph ON emp.name = emph.name
. . . . . . .> JOIN employee_id empi ON emph.employee_id = empi.employee_id;


Load local data to the Hive table:
jdbc:hive2://> LOAD DATA LOCAL INPATH
. . . . . . .> '/home/dayongd/Downloads/employee_hr.txt' 
. . . . . . .> OVERWRITE INTO TABLE employee_hr;


Load local data to the Hive partition table:
jdbc:hive2://> LOAD DATA LOCAL INPATH 
. . . . . . .> '/home/dayongd/Downloads/employee.txt'
. . . . . . .> OVERWRITE INTO TABLE employee_partitioned
. . . . . . .> PARTITION (year=2014, month=12);


Load HDFS data to the Hive table using the default system path:
jdbc:hive2://> LOAD DATA INPATH 
. . . . . . .> '/user/dayongd/employee/employee.txt' 
. . . . . . .> OVERWRITE INTO TABLE employee;


Load HDFS data to the Hive table with full URI:
jdbc:hive2://> LOAD DATA INPATH 
. . . . . . .> 'hdfs://[dfs_host]:8020/user/dayongd/employee/employee.txt' 
. . . . . . .> OVERWRITE INTO TABLE employee;


--Check the target table, which is empty.
jdbc:hive2://> SELECT name, work_place, sex_age 
. . . . . . .> FROM employee;


--Populate data from SELECT
jdbc:hive2://> INSERT INTO TABLE employee
. . . . . . .> SELECT * FROM ctas_employee;


--Verify the data loaded
jdbc:hive2://> SELECT name, work_place, sex_age FROM employee;


> WITH a AS (SELECT * FROM ctas_employee )
. . . . . . .> FROM a
. . . . . . .> INSERT OVERWRITE TABLE employee
. . . . . . .> SELECT *;


> FROM ctas_employee
. . . . . . .> INSERT OVERWRITE TABLE employee
. . . . . . .> SELECT *
. . . . . . .> INSERT OVERWRITE TABLE employee_internal
. . . . . . .> SELECT * ;


> SET hive.exec.dynamic.partition=true;


jdbc:hive2://> SET hive.exec.dynamic.partition.mode=nonstrict;
No rows affected (0.002 seconds)

jdbc:hive2://> INSERT INTO TABLE employee_partitioned 
. . . . . . .> PARTITION(year, month)
. . . . . . .> SELECT name, array('Toronto') as work_place, 
. . . . . . .> named_struct("sex","Male","age",30) as sex_age, 
. . . . . . .> map("Python",90) as skills_score,
. . . . . . .> map("R&D",array('Developer')) as depart_title, 
. . . . . . .> year(start_date) as year, month(start_date) as month
. . . . . . .> FROM employee_hr eh
. . . . . . .> WHERE eh.employee_id = 102;


jdbc:hive2://> INSERT OVERWRITE LOCAL DIRECTORY '/tmp/output1' 
. . . . . . .> SELECT * FROM employee;

jdbc:hive2://> INSERT OVERWRITE LOCAL DIRECTORY '/tmp/output2' 
. . . . . . .> ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
. . . . . . .> SELECT * FROM employee;
No rows affected (31.937 seconds)


jdbc:hive2://> FROM employee
. . . . . . .> INSERT OVERWRITE DIRECTORY '/user/dayongd/output'
. . . . . . .> SELECT *
. . . . . . .> INSERT OVERWRITE DIRECTORY '/user/dayongd/output1'
. . . . . . .> SELECT * ;


jdbc:hive2://> EXPORT TABLE employee TO '/user/dayongd/output3';


jdbc:hive2://> IMPORT TABLE empolyee_imported FROM 
. . . . . . .> '/user/dayongd/output3';


jdbc:hive2://> IMPORT EXTERNAL TABLE empolyee_imported_external 
. . . . . . .> FROM '/user/dayongd/output3'
. . . . . . .> LOCATION '/user/dayongd/output4' ;


jdbc:hive2://> EXPORT TABLE employee_partitioned partition 
. . . . . . .> (year=2014, month=11) TO '/user/dayongd/output5';
No rows affected (0.247 seconds)

jdbc:hive2://> IMPORT TABLE employee_partitioned_imported 
. . . . . . .> FROM '/user/dayongd/output5';
No rows affected (0.14 seconds)


SHOW FUNCTIONS; --List all functions
DESCRIBE FUNCTION <function_name>; --Detail for specified function
DESCRIBE FUNCTION EXTENDED <function_name>; --Even more details 

jdbc:hive2://> SELECT SIZE(work_place) AS array_size, 
. . . . . . .> SIZE(skills_score) AS map_size, 
. . . . . . .> SIZE(depart_title) AS complex_size, 
. . . . . . .> SIZE(depart_title["Product"]) AS nest_size 
. . . . . . .> FROM employee;


jdbc:hive2://> SELECT ARRAY_CONTAINS(work_place, 'Toronto') 
. . . . . . .> AS is_Toronto,
. . . . . . .> SORT_ARRAY(work_place) AS sorted_array 
. . . . . . .> FROM employee;


jdbc:hive2://> SELECT 
. . . . . . .> FROM_UNIXTIME(UNIX_TIMESTAMP()) AS current_time 
. . . . . . .> FROM employee LIMIT 1;


--To compare the difference between two dates.
jdbc:hive2://> SELECT (UNIX_TIMESTAMP ('2015-01-21 18:00:00') 
. . . . . . .> - UNIX_TIMESTAMP('2015-01-10 11:00:00'))/60/60/24 
. . . . . . .> AS daydiff FROM employee LIMIT 1;



