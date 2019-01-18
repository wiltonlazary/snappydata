SELECT * FROM patients limit 10;
INSERT INTO patients VALUES('5f1cef17-7151dc2c7221','2000-04-06 00:00:00.00',NULL,'999-43-1147','S99990853',NULL,'Ms.','Holley125','Berge125',NULL,NULL,NULL,'white','italian','F','Lynn','667 Mitchell Dale Suite 96','Holyoke','Massachusetts',01040);
PUT INTO patients VALUES('5f1cef17-7151dc2c7221','2000-04-06 00:00:00.00',NULL,'999-43-1147','S99990853',NULL,'Ms.','Holley125','Berge125',NULL,NULL,NULL,'white','italian','F','Lynn','667 Mitchell Dale Suite 96','Holyoke','Massachusetts',01040);
UPDATE patients set DEATHDATE=CURRENT_DATE() where ID='d48138da-21dd-471d-96fb-0215311e179a';
DELETE FROM patients WHERE ID='d48138da-21dd-471d-96fb-0215311e179a';
SELECT * FROM careplans limit 10;