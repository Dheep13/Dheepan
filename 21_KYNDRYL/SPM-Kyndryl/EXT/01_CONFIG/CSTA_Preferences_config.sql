DELETE FROm TCMP.CSTA_PREFERENCES;

Insert into TCMP.CSTA_PREFERENCES   (TENANTID, NAME, VALUE) Values  ('1259', 'UseCompensationDates', 'TRUE');

Insert into TCMP.CSTA_PREFERENCES   (TENANTID, NAME, VALUE) Values   ('1259', 'PreserveImportedTA', 'TRUE');
Insert into TCMP.CSTA_PREFERENCES   (TENANTID, NAME, VALUE) Values   ('1259', 'genericParallelDegree', '10');
Insert into TCMP.CSTA_PREFERENCES   (TENANTID, NAME, VALUE) Values   ('1259', 'StatsOnCSAssignment', 'TRUE');
Insert into TCMP.CSTA_PREFERENCES   (TENANTID, NAME, VALUE) Values   ('1259', 'validateECA', 'FALSE');
Insert into TCMP.CSTA_PREFERENCES   (TENANTID, NAME, VALUE) Values   ('1259', 'AllowAssignmentDuplicates', 'TRUE');
COMMIT;
