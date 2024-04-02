-- EXTRACT
DROP TABLE NEW_STAGEAREA;
CREATE TABLE NEW_STAGEAREA AS
SELECT bed_id, ward_id, care_centre_id, admission_date, bed_status, data_source, Occupied_Bed, Available_Bed 
FROM (SELECT ba.bed_id, w.ward_id, c.care_centre_id, r.admission_date, b.bed_status,'WYR' as data_source, CAST(NULL AS VARCHAR2(225)) AS Occupied_Bed, CAST(NULL AS VARCHAR2(225)) AS Available_Bed
FROM WYR_Reservation r
JOIN WYR_BedAssigned ba ON r.reservation_id = ba.reservation_id
JOIN WYR_Bed b ON ba.bed_id = b.bed_id
JOIN WYR_WARD w ON b.ward_id = w.ward_id
JOIN WYR_CARE_CENTRE c ON w.care_centre_id = c.care_centre_id

UNION

SELECT b.bed_id, w.ward_id, c.care_centre_id, a.admission_date, b.bed_status, 'NYR' as data_source, CAST(NULL AS VARCHAR2(225)) AS Occupied_Bed, CAST(NULL AS VARCHAR2(225)) AS Available_Bed
FROM NYR_ADMISSION a
JOIN NYR_BED b ON a.bed_id = b.bed_id
JOIN NYR_WARD w ON b.ward_id = w.ward_id
JOIN NYR_CARE_CENTRE c ON w.care_centre_id = c.care_centre_id
);
-- The EXTRACTION creates a new table 'NEW_STAGEAREA' that combines data from two different sources, WYR and NYR, 
-- into a unified structure. It uses the UNION operator to merge the results of two SELECT statements, 
-- each fetching data from a different source, and assigns a specific value to the data_source column to indicate the source of each row. 
-- Additionally, the Occupied_Bed and Available_Bed columns are populated with NULL values.

-- TRANSFORMATION
DROP TABLE ETL_LOGG CASCADE CONSTRAINTS;
CREATE TABLE ETL_LOGG
(issue_id NUMBER(5) NOT NULL, 
table_name VARCHAR2(20),
data_error_code NUMBER(5),
issue_desc VARCHAR2(50),
issue_date DATE, 
issue_status VARCHAR2(20),
status_update_date DATE);

DROP SEQUENCE EL_SEQQ;
CREATE SEQUENCE EL_SEQQ
START WITH 1
INCREMENT BY 1
MAXVALUE 10000
MINVALUE 1;

--Create Trigger
-- DROP TRIGGER trg_quality_chk cascade constraints;
CREATE or REPLACE trigger trg_quality_chk 
  before update on NEW_STAGEAREA
  for each row 
begin  
  INSERT INTO ETL_logg
  (issue_id,  table_name,  data_error_code,  issue_desc,  issue_date, issue_status, status_update_date)
   VALUES
  (EL_SEQQ.nextval, 'NEW_STAGEAREA', '0', 'Quality checks', SYSDATE, 'completed', SYSDATE);
end;
/

-- TRANSFORMATION
UPDATE NEW_STAGEAREA
SET bed_status = 'OCCUPIED'
WHERE bed_status = 'Occupied';
-- This updates the 'bed_status' column in the 'NEW_STAGEAREA' table
-- It sets the values of 'bed_status' to 'OCCUPIED' where it is currently 'Occupied'
-- This ensures consistency in the representation of bed statuses.

UPDATE NEW_STAGEAREA
SET bed_status = 'AVAILABLE'
WHERE bed_status IN ('Available', 'NOT OCCUPIED');
-- This updates the 'bed_status' column in the 'NEW_STAGEAREA' table
-- It sets the values of 'bed_status' to 'AVAILABLE' where it is currently 'Available' or 'NOT OCCUPIED'.
-- This ensures consistency in the representation of Available beds.

DELETE FROM NEW_STAGEAREA
WHERE admission_date IS NULL;
-- This removes rows from the 'NEW_STAGEAREA' table
-- where 'admission_date' column is NULL
-- Rows with missing admission dates are considered irrelevant and are removed.
-- These operations are designed to clean and standardize data in the 'NEW_STAGEAREA' table.

-- Populate the Time_Dim
DROP SEQUENCE Time_id_seq;
CREATE SEQUENCE Time_id_seq
START WITH 1
INCREMENT BY 1
MAXVALUE 1000
MINVALUE 1;

DROP VIEW Time_Dim_View;
CREATE VIEW Time_Dim_View AS
SELECT DISTINCT
   EXTRACT(MONTH FROM admission_date) AS month, 
   EXTRACT(YEAR FROM admission_date) AS year 
   FROM NEW_STAGEAREA
   WHERE admission_date IS NOT NULL and data_source = 'NYR' or data_source = 'WYR'
   ORDER BY month, year;

INSERT INTO Time_Dim (Time_id, month, year)
SELECT Time_id_seq.nextval, month, year
FROM Time_Dim_View;
-- Sequences are often used to generate unique identifiers for primary keys.
-- This SQL statement creates a new view named Time_Dim_View.
-- The view extracts distinct month and year values from the admission_date column in the NEW_STAGEAREA table.
-- It filters records where admission_date is not NULL and the data source is either 'NYR' or 'WYR'.
-- Views provide a way to run complex queries for easier use.
-- The insertion into the Time_Dim table is based on the data extracted and transformed by the Time_Dim_View.

-- LOAD
-- Extract and Load Ward_Dim data
DROP TABLE tmp_Ward_Dim;
CREATE TABLE tmp_Ward_Dim AS
SELECT w.ward_id, w.ward_name, CAST(NULL AS VARCHAR2(225)) AS current_ward_name, w.ward_capacity, w.ward_status, CURRENT_TIMESTAMP AS Effective_date
FROM WYR_WARD w
UNION
SELECT w.ward_id, w.ward_name, CAST(NULL AS VARCHAR2(225)) AS current_ward_name, w.ward_capacity, w.ward_status, CURRENT_TIMESTAMP AS Effective_date
FROM NYR_WARD w;

DROP SEQUENCE ward_id_surrogate_seq;
CREATE SEQUENCE ward_id_surrogate_seq
START WITH 1
INCREMENT BY 1
MAXVALUE 10000
MINVALUE 1;

INSERT INTO Ward_Dim (ward_id_surrogate, ward_id, ward_name, current_ward_name, ward_capacity, ward_status, Effective_date)
SELECT ward_id_surrogate_seq.nextval, ward_id, ward_name, current_ward_name, ward_capacity, ward_status, Effective_date
FROM tmp_Ward_Dim;
-- The sequence 'ward_id_surrogate_seq' is used to generate surrogate keys for the Ward_Dim table.
-- SCD TYPE 3 has been implemented in this dimention to keep a dated record of changes made to the 'ward name'.
-- The process involves extracting data from two different ward tables (WYR_WARD and NYR_WARD) and loading it into a temporary table.
-- The final step involves loading data from the temporary dim into the Ward_Dim using the generated surrogate keys.


-- Extract and Load Care_centre_Dim
DROP TABLE tmp_Care_centre_Dim;
CREATE TABLE tmp_Care_centre_Dim AS
SELECT  c.care_centre_id, c.care_centre_name, c.address, c.phone, 'WYR' as region
FROM WYR_CARE_CENTRE c
UNION
SELECT  c.care_centre_id, c.care_centre_name, c.address, c.phone, 'NYR' as region
FROM NYR_CARE_CENTRE c;

DROP SEQUENCE Care_Centre_id_seq;
CREATE SEQUENCE care_centre_id_seq
START WITH 1
INCREMENT BY 1
MAXVALUE 10000
MINVALUE 1;

INSERT INTO Care_centre_Dim (care_centre_id, care_centre_name, address, phone, region)
SELECT care_centre_id_seq.nextval, care_centre_name, address, phone, region
FROM tmp_Care_Centre_Dim;
-- The sequence care_centre_id_seq is used to generate unique keys for the Care_centre_Dim table.
-- The process involves extracting data from two different care centre tables (WYR_CARE_CENTRE and NYR_CARE_CENTRE) and loading it into a temporary table.
-- The final step involves loading data from the temporary dim into the Care_centre_Dim table using the generated keys.

-- Populate the Fact table from the cleaned data sets
DROP SEQUENCE Fact_id_seq;
CREATE SEQUENCE Fact_id_seq
START WITH 1
INCREMENT BY 1
MAXVALUE 10000
MINVALUE 1;

DROP TABLE tmp_Fact_Dim;
CREATE TABLE tmp_Fact_Dim AS
SELECT
    (select time_id from time_dim 
    where month = EXTRACT(month FROM admission_date)
    and year = EXTRACT(year FROM admission_date)) as time_id,
    care_centre_id,
    ward_id,
    bed_status,
    Occupied_Bed,
    Available_Bed
FROM NEW_STAGEAREA
WHERE bed_status IN ('OCCUPIED', 'AVAILABLE');

Drop Table Fact_Dim;
Create Table Fact_Dim AS
SELECT 
    Time_id,
    care_centre_id,
    ward_id,
    COUNT(CASE WHEN bed_status = 'OCCUPIED' THEN 1 END) AS Occupied_Bed,
    COUNT(CASE WHEN bed_status = 'AVAILABLE' THEN 1 END) AS Available_Bed
From tmp_Fact_Dim
GROUP BY Time_id, care_centre_id, ward_id
ORDER BY Time_id ASC;
-- The sequence Fact_id_seq is used to generate unique keys for the Fact_Dim table.
-- The process involves creating and populating a fact table (Fact_Dim) based on aggregated data from the temporary table (tmp_Fact_Dim).
-- Aggregation is performed on the temporary table to count the number of occupied and available beds for each time period, care centre, and ward.
-- The result is grouped by Time_id, care_centre_id, and ward_id, and the table is ordered by Time_id in ascending order.


SELECT * FROM Fact_Dim;
SELECT * FROM Time_Dim;
SELECT * FROM Ward_Dim;
SELECT * FROM Care_Centre_Dim;
-- These queries provide a glimpse into the contents of the respective tables in the database.


