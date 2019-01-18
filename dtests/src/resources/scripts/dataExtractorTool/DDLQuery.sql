CREATE TABLE tempColTable(name String, age int, address String) using column;
CREATE TABLE tempRowTable(name String, age int, address String) using row;
ALTER TABLE testPatients ADD COUNTRY String;
TRUNCATE TABLE testPatients;
DROP TABLE testPatients;
CREATE POLICY p0 ON gemfire1.Patients FOR SELECT TO gemfire2 USING PASSPORT = 'X57928236X' AND CITY = 'Tewksbury';