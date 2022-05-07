--Author Boss nc2325 and Minsu ms11108

--attemp to run the code by ctrlc ctrlv the code usually doesn't work due to quotation symbol (') issue
--try to retype every quotationmark inorder for stuff to runs smoothly

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

--Query only preferred field - this code also works as a cleaning as it drops NULL value in process
SELECT 
        county, count(*)
    FROM 
        camera
    WHERE 
        county IN ('NY','K','Q','R','QN','Qns','Bronx','ST','Kings','BK','BX','MN')
    GROUP BY
        County;

--Query the crime by borough 
SELECT 
    county, count(*)
FROM 
    camera
WHERE 
    county IN ('NY','K','Q','R','QN','Qns','Bronx','ST','Kings','BK','BX','MN')
GROUP BY
    County;
--Put the filtered data into csv file via Beeline

beeline -u jdbc:hive2://hm-1.hpc.nyu.edu:10000/nc2325 -n nc2325 -p PASSWORD --outputformat=csv2 -e 
    "SELECT 
        county, count(*)
    FROM 
        camera
    WHERE 
        county IN ('NY','K','Q','R','QN','Qns','Bronx','ST','Kings','BK','BX','MN')
    GROUP BY
        County;" > county_camera.csv


--Creating datatabse from https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i
-- NYPD Complaint Data Historic, April 21st version
create table crimes(
    ID string, 
    COMPLAINT_DATE date, 
    COMPLAINT_TIME string, 
    COMPLAINT_PROCESSED_DATE date, 
    COMPLAINT_PROCESSED_TIME string, 
    PRECINCT string, REPORTED_DATE date, 
    OFFENSE_CODE string, 
    OFFENSE_DESC string, 
    INTERNAL_CODE string, 
    PD_DESCRIPTION string, 
    CRIME_ATTEMPT string, 
    LEVEL_OF_OFFENSE string, 
    BOROUGH string, 
    LOCATION string, 
    PREMISE string, 
    JURISDICTION_DESC string, 
    JURISDICTION_CODE string, 
    PARKS string, 
    HADEVELOPT string, 
    HOUSING_PSA string, 
    X_COORD string, 
    Y_COORD string, 
    AGE_GROUP string, 
    SUSP_RACE string, 
    SUSP_SEX string, 
    TRANSIT_DISTRICT string, 
    LATITUDE string, 
    LONGITUDE string, 
    LAT_LONG string, 
    PATROL_BOROUGH string, 
    STATION_NAME string, 
    VIC_AGE string, 
    VIC_RACE string, 
    VIC_SEX string)
    COMMENT 'CRIMES'
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"="\"",
    "escapeChar"="\\"
    )
    tblproperties ("skip.header.line.count"="1");
    
"--Load the data into the table
LOAD DATA 
    INPATH '/user/nc2325/project_complaint/NYPD_Complaint_Data_Historic.csv' 
INTO TABLE crimes;

--Query to find crime count in each borough - this code also works as a cleaning as it drops NULL value in process
SELECT 
    BOROUGH, count(*)
FROM 
    crimes
WHERE 
    BOROUGH IN ('MANHATTAN','QUEENS', 'STATEN ISLAND', 'BRONX', 'BROOKLYN')
GROUP BY
    BOROUGH;

--importing 1st set of query into csv via beeline
beeline -u jdbc:hive2://hm-1.hpc.nyu.edu:10000/nc2325 -n nc2325 -p PASSWORD --outputformat=csv2 -e 
"SELECT 
  BOROUGH, count(*)
FROM 
  crimes
WHERE 
  BOROUGH IN ('MANHATTAN','QUEENS', 'STATEN ISLAND', 'BRONX', 'BROOKLYN')
GROUP BY
  BOROUGH;" > crimes.csv 

--Query of location of all crime related to fortune telling - this code also works as a cleaning as it drops NULL value in process
SELECT OFFENSE_DESC, LATITUDE, LONGITUDE 
FROM CRIMES 
WHERE OFFENSE_DESC = "FORTUNE TELLING";

--importing 2nd set of query into csv via beeline
beeline -u jdbc:hive2://hm-1.hpc.nyu.edu:10000/nc2325 -n nc2325 -p PASSWORD --outputformat=csv2 -e 
'SELECT OFFENSE_DESC, LATITUDE, LONGITUDE 
FROM CRIMES 
WHERE OFFENSE_DESC = "FORTUNE TELLING";' > fortune_telling.csv

--Query of location of all crime related to arson - this code also works as a cleaning as it drops NULL value in process
SELECT OFFENSE_DESC, LATITUDE, LONGITUDE 
FROM CRIMES 
WHERE OFFENSE_DESC = "ARSON";

--importing 3rd set of query into csv via beeline
beeline -u jdbc:hive2://hm-1.hpc.nyu.edu:10000/nc2325 -n nc2325 -p PASSWORD --outputformat=csv2 -e 
'SELECT OFFENSE_DESC, LATITUDE, LONGITUDE 
FROM CRIMES 
WHERE OFFENSE_DESC = "ARSON";'> arson.csv

--Query of location of all crime related to manslaughter - this code also works as a cleaning as it drops NULL value in process
SELECT OFFENSE_DESC, LATITUDE, LONGITUDE 
FROM CRIMES 
WHERE OFFENSE_DESC = "MURDER & NON-NEGL. MANSLAUGHTER";

--importing 4th set of query into csv via beeline
beeline -u jdbc:hive2://hm-1.hpc.nyu.edu:10000/nc2325 -n nc2325 -p PASSWORD --outputformat=csv2 -e 
'SELECT OFFENSE_DESC, LATITUDE, LONGITUDE 
FROM CRIMES 
WHERE OFFENSE_DESC = "MURDER & NON-NEGL. MANSLAUGHTER";' > manslaughter.csv

--Query of location of all crime related to kidnapping - this code also works as a cleaning as it drops NULL value in process
SELECT OFFENSE_DESC, LATITUDE, LONGITUDE, LAT_LONG 
FROM CRIMES 
WHERE OFFENSE_DESC = "KIDNAPPING & RELATED OFFENSES";

--importing 5th set of query into csv via beeline
beeline -u jdbc:hive2://hm-1.hpc.nyu.edu:10000/nc2325 -n nc2325 -p PASSWORD --outputformat=csv2 -e 
‘SELECT OFFENSE_DESC, LATITUDE, LONGITUDE, LAT_LONG 
FROM CRIMES 
WHERE OFFENSE_DESC = "KIDNAPPING & RELATED OFFENSES";’ > kidnapping.csv