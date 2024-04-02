-- Drop Table commands.
DROP TABLE Care_Centre_Dim CASCADE CONSTRAINTS;
DROP TABLE Time_Dim CASCADE CONSTRAINTS;
DROP TABLE Ward_Dim CASCADE CONSTRAINTS;
DROP TABLE FACT_DIM CASCADE CONSTRAINTS;

-- Create a Database table to represent the "Time_Dim" entity.
CREATE TABLE Time_Dim(
	Time_id	INTEGER NOT NULL,
	Month	VARCHAR2(225),
	Year	VARCHAR2(225),
	CONSTRAINT	pk_Time_Dim PRIMARY KEY (Time_id)
);

-- Create a Database table to represent the "Care_Centre_Dim" entity.
CREATE TABLE Care_Centre_Dim(
	Care_Centre_id	INTEGER NOT NULL,
	Care_Centre_Name	VARCHAR2(225),
    address VARCHAR2(225),
    phone VARCHAR2(225),
    region VARCHAR(225),
	CONSTRAINT	pk_Care_Centre_Dim PRIMARY KEY (Care_Centre_id)
);

-- Create a Database table to represent the "Fact_Dim" entity.
CREATE TABLE Fact_Dim(
	Fact_id	INTEGER NOT NULL,
    fk1_Time_id	INTEGER NOT NULL,
	fk2_Care_Centre_id	INTEGER NOT NULL,
	fk3_Ward_id_surrogate	INTEGER NOT NULL,
    bed_status VARCHAR2(225),
	Occupied_Bed INTEGER,
    Available_Bed INTEGER,
	CONSTRAINT	pk_Fact_Dim PRIMARY KEY (Fact_id)
);

-- Create a Database table to represent the "Ward_Dim" entity.
CREATE TABLE Ward_Dim(
	Ward_id_surrogate	INTEGER NOT NULL,
	Ward_id	INTEGER,
	Ward_Name	VARCHAR2(225),
	Current_Ward_Name	VARCHAR2(225),
    ward_capacity VARCHAR2(225),
    ward_status VARCHAR2(225),
	Effective_Date	VARCHAR2(225),
	CONSTRAINT	pk_Ward_Dim PRIMARY KEY (Ward_id_surrogate)
);

DROP SEQUENCE Time_id_seq;
DROP SEQUENCE Ward_id_surrogate_seq;
DROP SEQUENCE Fact_id_seq;
DROP SEQUENCE Care_Centre_id_seq;

CREATE SEQUENCE Time_id_seq;
CREATE SEQUENCE Ward_id_surrogate_seq; 
CREATE SEQUENCE Fact_id_seq;
CREATE SEQUENCE Care_Centre_id_seq;

-- Alter Tables to add fk constraints --

ALTER TABLE Fact_Dim 
ADD CONSTRAINT fk1_Fact_Dim_to_Time_Dim FOREIGN KEY(fk1_Time_id) REFERENCES Time_Dim(Time_id);

ALTER TABLE Fact_Dim 
ADD CONSTRAINT fk2_Fact_Dim_to_Care_Centre_Dim FOREIGN KEY(fk2_Care_Centre_id) REFERENCES Care_Centre_Dim(Care_Centre_id);

ALTER TABLE Fact_Dim 
ADD CONSTRAINT fk3_Fact_Dim_to_Ward_Dim FOREIGN KEY(fk3_Ward_id_surrogate) REFERENCES Ward_Dim(Ward_id_surrogate);


--------------------------------------------------------------
-- End of DDL file auto-generation
--------------------------------------------------------------

-- The script is a Data Definition Language (DDL) script written in SQL, and it is used for creating a database schema with related tables, sequences, and foreign key constraints.
-- It establishes relationships between dimensions (Time_Dim, Care_Centre_Dim, Ward_Dim) and the fact table (Fact_Dim) using foreign key constraints.
-- The schema models a healthcare case study with information about time, care centers, wards, and bed occupancy
