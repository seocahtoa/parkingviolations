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