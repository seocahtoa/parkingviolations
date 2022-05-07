--Hive Initialize code
beeline
!connect jdbc:hive2://hm-1.hpc.nyu.edu:10000/
use nc2325(use your netid)


--Create and Importing database from https://data.cityofnewyork.us/City-Government/Open-Parking-and-Camera-Violations/nc67-uf89
--Open Parking and Camera Violations - April 1st version
CREATE EXTERNAL TABLE camera(
    plate string, 
    state_code string, 
    license_type string, 
    summons_number string, 
    issue_date date, 
    summons_image string, 
    violation_time string, 
    violation string, 
    fine_amount float, 
    penalty_amount float, 
    interest_amount float, 
    reduction_amount float, 
    payment_amount float, 
    amount_due float, 
    precinct int, 
    county string, 
    issuing_agency string, 
    violation_status string, 
    judgment_entry_date date) 
    COMMENT 'Open parking external table' 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS TEXTFILE LOCATION '/user/nc2325/project_camera' 
    tblproperties ("skip.header.line.count"="1");