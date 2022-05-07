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