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