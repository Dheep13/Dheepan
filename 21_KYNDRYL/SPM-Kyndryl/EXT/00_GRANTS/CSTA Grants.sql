-- CSTA tables
grant select,insert,delete,update on TCMP.CSTA_COPYFIELD to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_CUSTOMER to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_EXECUTIONHINT to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_EXPRESSION to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_GENERICCLASSIFIER to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_GENERICCLASSIFIER_EXC to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_JOINCONDITION to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_MASTERRULE to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_MASTERRULEHOOK to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_POSITIONSTORUN to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_PREFERENCES to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_PUEXCLUSION to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_PUSQL to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_READ to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_RPT_CES_SUMMARY to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_RULE to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_RUNLOG to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_RUNPARAMETERS to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_TARGETFIELD to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_TXNASSIGNMENTS to PROFSERVICES_ROLE;
grant select,insert,delete,update on TCMP.CSTA_TXNASSIGNMENTSCOPY to PROFSERVICES_ROLE;

-- Preferences
grant select,insert,delete,update on TCMP.CS_PREFERENCES to PROFSERVICES_ROLE;

-- procedure
grant execute on TCMP.CSTA_ASSIGNMENTLIB__RUN to PROFSERVICES_ROLE;
