CREATE LIBRARY "EXT"."PKG_REPORTING_EXTRACT_R2" LANGUAGE SQLSCRIPT AS
BEGIN
  PUBLIC VARIABLE cEndofTime CONSTANT TIMESTAMP = TO_DATE('01012200','mmddyyyy');
  PUBLIC VARIABLE cTenantID CONSTANT VARCHAR(4) = 'STEL';
  PUBLIC VARIABLE vProcName VARCHAR(60);
  PUBLIC VARIABLE vSQLerrm VARCHAR(1000);
  PUBLIC VARIABLE vCurYrStartDate TIMESTAMP;
  PUBLIC VARIABLE vCurYrEndDate TIMESTAMP;
  PUBLIC VARIABLE vPeriodtype VARCHAR(60);
  PUBLIC VARIABLE vSTSRoadShowCategory VARCHAR(255);
  PUBLIC VARIABLE veventtypeid_ccomobile VARCHAR(255);
  PUBLIC VARIABLE veventtypeid_ccotv VARCHAR(255);
  PUBLIC VARIABLE vcredittypeid_PayAdj VARCHAR(255);
  PUBLIC VARIABLE vcredittypeid_CEAdj VARCHAR(255);
  PUBLIC VARIABLE vcredittypeid_Mobile VARCHAR(255);
  PUBLIC VARIABLE vcredittypeid_TV VARCHAR(255);
  PUBLIC VARIABLE vcredittypeid_HandFee VARCHAR(255);
  PUBLIC VARIABLE vOperational_Compliance VARCHAR(255);
  PUBLIC VARIABLE vGSTRate DECIMAL(5,5);
  PUBLIC PROCEDURE init_session_global()
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE init_timestamp VARCHAR(50) := TO_VARCHAR(CURRENT_TIMESTAMP);
    DECLARE vPeriodRow ROW LIKE CS_PERIOD ;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_PERIOD' not found (for %ROWTYPE declaration) */
    DECLARE vCalendarRow ROW LIKE CS_CALENDAR;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_CALENDAR' not found (for %ROWTYPE declaration) */
    DECLARE vProcessingUnitRow ROW LIKE CS_PROCESSINGUNIT;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_PROCESSINGUNIT' not found (for %ROWTYPE declaration) */
    DECLARE cEndofTime CONSTANT TIMESTAMP = TO_DATE('01012200','mmddyyyy');   
    
        SET 'vProcName' = NULL;
        SET 'vSQLerrm' = NULL;
        SET 'vCurYrStartDate' = NULL;
        SET 'vCurYrEndDate' = NULL;
        SET 'vPeriodtype' = 'MONTHLY';
        SET 'vSTSRoadShowCategory' = 'Roadshow';
        SET 'veventtypeid_ccomobile' = 'Mobile Closed';
        SET 'veventtypeid_ccotv' = 'TV Closed';
        SET 'vcredittypeid_PayAdj' = 'Payment Adjustment';
        SET 'vcredittypeid_CEAdj' = 'Customer Experience';
        SET 'vcredittypeid_Mobile' = 'CCO Mobile VAS';
        SET 'vcredittypeid_TV' = 'CCO TV';
        SET 'vcredittypeid_HandFee' = 'TVReconHandlingFee - CCO';
        SET 'vOperational_Compliance' = 'Operational Compliance';
        SET 'vGSTRate' = 0.07;
        SET 'cEndofTime' = :cEndofTime;
    END;
  PUBLIC PROCEDURE prc_setVariables
(
    IN vPeriodSeq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'CS_PERIODCALENDAR.periodseq' not found (for %TYPE declaration) */
                                                      /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_setVariables.vPeriodSeq' */
                                                      /* ORIGSQL: vPeriodSeq IN BIGINT */
    IN vProcessingUnitSeq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'CS_PROCESSINGUNIT.processingunitseq' not found (for %TYPE declaration) */
                                                                      /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_setVariables.vProcessingUnitSeq' */
                                                                      /* ORIGSQL: vProcessingUnitSeq IN BIGINT */
    IN vCalendarSeq BIGINT      /* RESOLVE: Identifier not found: Table/Column 'CS_PERIODCALENDAR.calendarseq' not found (for %TYPE declaration) */
                                                            /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_setVariables.vCalendarSeq' */
                                                            /* ORIGSQL: vCalendarSeq IN BIGINT */
)
SQL SECURITY DEFINER
READS SQL DATA
AS
BEGIN
    /****************************************************************************************************************
        The purpose of this procedure is to initialize all the required variables.
    
        Date        Author     Description
        ------------------------------------------------------------------------------------------------------------
    --- 30 Nov 2017 Tharanikumar  Initial release
    ***************************************************************************************************************/
    -- Get Period.
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIOD' not found */
   DECLARE  vPeriodRow ROW LIKE CS_PERIOD;
   DECLARE  vProcessingUnitRow ROW LIKE CS_PROCESSINGUNIT;
   DECLARE  vCalendarRow ROW LIKE CS_CALENDAR;
   
    SELECT *
    INTO
        vPeriodRow
    FROM
        cs_period prd
    WHERE
        prd.removedate = :cEndofTime
        AND prd.periodseq = :vPeriodSeq;

    -- Get Processing Unit.
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PROCESSINGUNIT' not found */

    SELECT *
    INTO
        vProcessingUnitRow
    FROM
        cs_processingunit pu
    WHERE
        pu.processingunitseq = :vProcessingUnitSeq;

    -- Get Calendar.
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_CALENDAR' not found */

    SELECT *
    INTO
        vCalendarRow
    FROM
        cs_calendar cal
    WHERE
        cal.removedate = :cEndofTime
        AND cal.calendarseq = :vCalendarSeq;

    /*
      -- Get Current Year Start and End Dates
      SELECT per.startdate,  per.enddate - 1
      INTO vCurYrStartDate, vCurYrEndDate
      FROM   cs_period per
      WHERE per.periodSeq = (SELECT per1.periodseq
            FROM cs_period  per1, cs_periodtype pt1
            WHERE per1.PeriodTypeseq = pt1.PeriodTypeseq
         AND pt1.Name = 'year'
                START WITH per1.periodseq = vperiodseq
                CONNECT BY PRIOR per1.parentseq = per1.periodseq
            );
    */
    
    
    SET SESSION 'GvProcessingUnitSeq' = :vProcessingUnitRow.processingunitSeq;
    SET SESSION 'GvPeriodName' = :vPeriodRow.name;
    SET SESSION 'GvPeriodStartDate' = :vPeriodRow.startDate;
    SET SESSION 'GvPeriodEndDate' = :vPeriodRow.endDate;
    SET SESSION 'GvCalendarName' = :vCalendarRow.name;
    SET SESSION 'GvPeriodShortName' = :vPeriodRow.shortname;  
        
END;
  PUBLIC PROCEDURE prc_DeleteTable
(
    IN pTableName VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                 /* ORIGSQL: pTableName VARCHAR2 */
    IN pProcessingUnitSeq BIGINT      /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_DeleteTable.pProcessingUnitSeq' */
                                                                        /* ORIGSQL: pProcessingUnitSeq IN BIGINT */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN


    /****************************************************************************************************************
        The purpose of this procedure is to truncate the supplied table.
    
        Date        Author     Description
        ------------------------------------------------------------------------------------------------------------
    --- 30 Nov 2017 Tharanikumar  Initial release
    ****************************************************************************************************************/
    /* ORIGSQL: prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_DeleteTable','Delete started',''||pTableName,NULL) */
   --DECLARE vPeriodRow ROW LIKE CS_PERIOD;
   --DECLARE vProcessingUnitRow ROW LIKE CS_PROCESSINGUNIT;
   --DECLARE vCalendarRow ROW LIKE CS_CALENDAR;
   DECLARE vPeriodName VARCHAR(100);
   DECLARE vProcessingUnitSeq BIGINT;
   DECLARE vCalendarName VARCHAR(100);
   DECLARE vPeriodStartDate timestamp;
   DECLARE vPeriodEndDate timestamp;
    
    SELECT SESSION_CONTEXT('GvPeriodName') INTO vPeriodName FROM SYS.DUMMY ;
    SELECT SESSION_CONTEXT('GvCalendarName') INTO vCalendarName FROM SYS.DUMMY ;
    SELECT SESSION_CONTEXT('GvProcessingUnitSeq') INTO vProcessingUnitSeq FROM SYS.DUMMY ;
    SELECT SESSION_CONTEXT('GvPeriodStartDate') INTO vPeriodStartDate FROM SYS.DUMMY ;
    SELECT SESSION_CONTEXT('GvPeriodEndDate') INTO vPeriodEndDate FROM SYS.DUMMY ;
    
    CALL EXT.PRC_LOGEVENT(:vPeriodName, 'prc_DeleteTable', 'Delete started', ''||IFNULL(:pTableName,''), NULL);

    /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
    /* ORIGSQL: EXECUTE IMMEDIATE 'Delete from '||pTableName||' where processingunitseq = '||pProcessingUnitSeq; */
    EXECUTE IMMEDIATE 'Delete from '||IFNULL(:pTableName,'')||' where processingunitseq = '||IFNULL(TO_VARCHAR(:pProcessingUnitSeq),'');
    /* ORIGSQL: prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_DeleteTable','Delete End',''||pTableName,NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodName, 'prc_DeleteTable', 'Delete End', ''||IFNULL(:pTableName,''), NULL);

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_DeleteTable','START','Base Table Delete '||pTableName,NULL);
END;
  PUBLIC PROCEDURE prc_TruncateTablePartition /*Deepan : Proc not required*/
(
    IN pTableName VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                  /* ORIGSQL: pTableName VARCHAR2 */
    IN vPeriodSeq BIGINT,   /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_TruncateTablePartition.vPeriodSeq' */
                                                      /* ORIGSQL: vPeriodSeq IN BIGINT */
    IN vProcessingUnitSeq BIGINT,   /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_TruncateTablePartition.vProcessingUnitSeq' */
                                                                      /* ORIGSQL: vProcessingUnitSeq IN BIGINT */
    IN vCalendarSeq BIGINT      /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_TruncateTablePartition.vCalendarSeq' */
                                                            /* ORIGSQL: vCalendarSeq IN BIGINT */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /****************************************************************************************************************
        The purpose of this procedure is to truncate a set of Partitions of the supplied table.
    
        Date        Author     Description
        ------------------------------------------------------------------------------------------------------------
    
    ****************************************************************************************************************/
    /* ORIGSQL: prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_TruncateTablePartition','START','Truncating Table Partitions for : '||pTableName,NULL) */
   DECLARE vPeriodName VARCHAR(100);
   DECLARE vProcessingUnitSeq BIGINT;
   DECLARE vCalendarName VARCHAR(100);
   
   
    SELECT SESSION_CONTEXT('GvPeriodName') INTO vPeriodName FROM SYS.DUMMY ;
    SELECT SESSION_CONTEXT('GvCalendarName') INTO vCalendarName FROM SYS.DUMMY ;
    SELECT SESSION_CONTEXT('GvProcessingUnitSeq') INTO vProcessingUnitSeq FROM SYS.DUMMY ;
    
    
 
    -- CALL EXT.PRC_LOGEVENT(:vPeriodName, 'prc_TruncateTablePartition', 'START', 'Truncating Table Partitions for : '||IFNULL(:pTableName,''), NULL);
    -- Partition truncate code to be included here.
    -- Include a call to OD_GetPeriodSubPartitionName;
    -- Will require to raise a call for this one.
END;
  PUBLIC FUNCTION fnc_PipelineWasRun
(
    IN vPeriodSeq BIGINT,   /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'fnc_PipelineWasRun.vPeriodSeq' */
                                                      /* ORIGSQL: vPeriodSeq IN BIGINT */
    IN vProcessingUnitSeq BIGINT,   /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'fnc_PipelineWasRun.vProcessingUnitSeq' */
                                                                      /* ORIGSQL: vProcessingUnitSeq IN BIGINT */
    IN vCalendarSeq BIGINT      /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'fnc_PipelineWasRun.vCalendarSeq' */
                                                            /* ORIGSQL: vCalendarSeq IN BIGINT */
)
RETURNS dbmtk_function_result BOOLEAN   /* ORIGSQL: RETURN BOOLEAN */
SQL SECURITY DEFINER
AS
BEGIN
    /****************************************************************************************************************
        The purpose of this procedure is to check whether a pipeline was run since the last Report Generation.
        If no new Pipeline are found, then don't execute the Reporting Stored Procedures.
    
        Date        Author     Description
        ------------------------------------------------------------------------------------------------------------
    
    ****************************************************************************************************************/
    -- Include a call to odsutils.istorunodsprocs (pprocessingunitseq, pperiodseq);
    -- Will require to raise a call for this one. 
    dbmtk_function_result = TRUE;

END;
  PUBLIC PROCEDURE prc_AnalyzeTable/*Deepan : proc not required*/
(
    IN pTableName VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                 /* ORIGSQL: pTableName VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /****************************************************************************************************************
        The purpose of this procedure is to analyze reporting tables and to keep the statistics upto date for performance.
    
        Date        Author     Description
        ------------------------------------------------------------------------------------------------------------
    --- 30 Nov 2017 Tharanikumar  Initial release
    ****************************************************************************************************************/
    /* ORIGSQL: prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_AnalyzeTable','prc_AnalyzeTable started',''||pTableName,NULL) */
    -- CALL EXT.PRC_LOGEVENT(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name, 'prc_AnalyzeTable', 'prc_AnalyzeTable started', ''||IFNULL(:pTableName,''), NULL);/*Deepan : Partition not required*/

    /* ORIGSQL: DBMS_STATS.gather_table_stats(ownname => '',tabname => pTableName,estimate_percent => DBMS_STATS.auto_sample_size) */
    -- EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| ''|| '.'|| IFNULL(:pTableName,'');/*Deepan : Partition not required*/
    /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */
    /* ORIGSQL: prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_AnalyzeTable','prc_AnalyzeTable ended',''||pTableName,NULL) */
    -- CALL EXT.PRC_LOGEVENT(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name, 'prc_AnalyzeTable', 'prc_AnalyzeTable ended', ''||IFNULL(:pTableName,''), NULL);/*Deepan : Partition not required*/
END;
  PUBLIC PROCEDURE prc_AnalyzeTableSubpartition/*Deepan : Partition not required*/
(
    IN pExtUser VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                /* ORIGSQL: pExtUser VARCHAR2 */
    IN pRptTableName VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                     /* ORIGSQL: pRptTableName VARCHAR2 */
    IN pSubPartitionName VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                        /* ORIGSQL: pSubPartitionName VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /****************************************************************************************************************
        The purpose of this procedure is to analyze reporting tables and to keep the statistics upto date for performance.
    
        Date        Author     Description
        ------------------------------------------------------------------------------------------------------------
    --- 30 Nov 2017 Tharanikumar  Initial release
    ****************************************************************************************************************/
    /* ORIGSQL: prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_AnalyzeTableSubpartition','prc_AnalyzeTableSubpartition started',''||pRptTableName,NULL) */
    -- CALL EXT.PRC_LOGEVENT(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name, 'prc_AnalyzeTableSubpartition', 'prc_AnalyzeTableSubpartition started', ''||IFNULL(:pRptTableName,''), NULL);/*Deepan : Partition not required*/

    /* ORIGSQL: DBMS_STATS.gather_table_stats(ownname => pExtUser, tabname => pRptTableName, partname => pSubPartitionName, method_opt => 'FOR ALL INDEXED COLUMNS size AUTO', DEGREE => 1, CASCADE => TRUE, estimate_pe(...) */
    -- EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| IFNULL(:pExtUser,'') || '.'|| IFNULL(:pRptTableName,'');/*Deepan : Partition not required*/
    /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */
    /* ORIGSQL: prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_AnalyzeTableSubpartition','prc_AnalyzeTableSubpartition ended',''||pRptTableName,NULL) */
    -- CALL EXT.PRC_LOGEVENT(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name, 'prc_AnalyzeTableSubpartition', 'prc_AnalyzeTableSubpartition ended', ''||IFNULL(:pRptTableName,''), NULL);/*Deepan : Partition not required*/
END;
  PUBLIC PROCEDURE prc_AddTableSubpartition/*Deepan : Partition not required*/
(
    IN vExtUser VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                /* ORIGSQL: vExtUser VARCHAR2 */
    IN vTCTemplateTable VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                        /* ORIGSQL: vTCTemplateTable VARCHAR2 */
    IN vTCSchemaName VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                     /* ORIGSQL: vTCSchemaName VARCHAR2 */
    IN vTenantId VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                 /* ORIGSQL: vTenantId VARCHAR2 */
    IN vProcessingUnitSeq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
                                                                      /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_AddTableSubpartition.vProcessingUnitSeq' */
                                                                      /* ORIGSQL: vProcessingUnitSeq IN BIGINT */
    IN vPeriodSeq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
                                              /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_AddTableSubpartition.vPeriodSeq' */
                                              /* ORIGSQL: vPeriodSeq IN BIGINT */
    IN vRptTableName VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                    /* ORIGSQL: vRptTableName VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    /****************************************************************************************************************
        The purpose of this procedure is to create new subpartitions for the custom reporting tables as needed.
    
        Date        Author     Description
        ------------------------------------------------------------------------------------------------------------
    --- 30 Nov 2017 Tharanikumar  Initial release
    ****************************************************************************************************************/

    /* ORIGSQL: CURSOR c_partitioncheck(p_rpttable varchar2, p_tableowner varchar2, p_partition_name varchar2) IS SELECT 'X' FROM all_tab_partitions WHERE table_name = p_rpttable AND table_owner=p_tableowner AND part(...) */
   /*Deepan : Partition not required, hence commenting out both cursors dbmtk_cursor_968 and c_partitioncheck  */
   
    -- DECLARE CURSOR c_partitioncheck (p_rpttable VARCHAR(255), p_tableowner VARCHAR(255), p_partition_name VARCHAR(255))
    -- FOR
    --     SELECT   /* ORIGSQL: SELECT 'X' FROM all_tab_partitions WHERE table_name = p_rpttable AND table_owner=p_tableowner AND partition_name=p_partition_name; */
    --         'X'
    --     FROM
    --         SYS.TABLE_PARTITIONS  /* RESOLVE: Catalog reference(partly converted): Oracle catalog 'ALL_TAB_PARTITIONS': verify conversion */
    --                               /* ORIGSQL: all_tab_partitions (Oracle catalog) */
    --     WHERE
    --         TABLE_NAME = p_rpttable  /* ORIGSQL: table_name (column in Oracle catalog 'ALL_TAB_PARTITIONS') */
    --         AND SCHEMA_NAME = p_tableowner  /* ORIGSQL: table_owner (column in Oracle catalog 'ALL_TAB_PARTITIONS') */
    --         AND TO_NVARCHAR(PART_ID) = p_partition_name;  /* ORIGSQL: partition_name (column in Oracle catalog 'ALL_TAB_PARTITIONS') */

    -- DECLARE vLong NCLOB;  /* ORIGSQL: vLong long; */
    -- DECLARE vVarchar2 VARCHAR(2000);  /* ORIGSQL: vVarchar2 varchar2(2000); */
    -- DECLARE v_PartitionAvail VARCHAR(1) = NULL;  /* RESOLVE: Manual edits required: CHAR variable(no length): user-configured length=255; adjust as needed */
    --                                               /* RESOLVE: Datatype partly supported: Fixed-length CHAR datatype not supported; changed to VARCHAR for variable('v_PartitionAvail'); review all references */
    --                                               /* ORIGSQL: v_PartitionAvail CHAR := NULL; */
    -- DECLARE v_FirstCheck VARCHAR(1) = NULL;  /* RESOLVE: Manual edits required: CHAR variable(no length): user-configured length=255; adjust as needed */
    --                                           /* RESOLVE: Datatype partly supported: Fixed-length CHAR datatype not supported; changed to VARCHAR for variable('v_FirstCheck'); review all references */
    --                                           /* ORIGSQL: v_FirstCheck CHAR := NULL; */
    -- DECLARE v_partitionname VARCHAR(100) = NULL;  /* ORIGSQL: v_partitionname varchar2(100) := NULL; */

    -- /* ORIGSQL: FOR row IN (SELECT subpartition_name, high_value FROM all_tab_subpartitions WHERE table_name = vTCTemplateTable AND table_owner=vTCSchemaName AND subpartition_name IN ((SELECT subpartition_name FROM a(...) */
    -- DECLARE CURSOR dbmtk_cursor_968
    -- FOR
    --     SELECT   /* ORIGSQL: SELECT subpartition_name, high_value FROM all_tab_subpartitions WHERE table_name = vTCTemplateTable AND table_owner=vTCSchemaName AND subpartition_name IN (SELECT subpartition_name FROM SYS.TABLE_PART(...) */
    --         subpartition_name,
    --         high_value
    --     FROM
    --         SYS.TABLE_PARTITIONS  /* RESOLVE: Catalog reference(partly converted): Oracle catalog 'ALL_TAB_SUBPARTITIONS': verify conversion */
    --                               /* ORIGSQL: all_tab_subpartitions (Oracle catalog) */
    --     WHERE
    --         TABLE_NAME = :vTCTemplateTable  /* ORIGSQL: table_name (column in Oracle catalog 'ALL_TAB_SUBPARTITIONS') */
    --         AND SCHEMA_NAME = :vTCSchemaName  /* ORIGSQL: table_owner (column in Oracle catalog 'ALL_TAB_SUBPARTITIONS') */
    --         AND subpartition_name IN
    --         (
    --             SELECT   /* ORIGSQL: (SELECT subpartition_name FROM all_tab_subpartitions WHERE table_name = vTCTemplateTable AND table_owner=vTCSchemaName AND subpartition_name = (SELECT EXT.OD_GETPERIODSUBPARTITIONNAME(vTCSchemaName, v(...) */
    --                 subpartition_name
    --             FROM
    --                 SYS.TABLE_PARTITIONS  /* RESOLVE: Catalog reference(partly converted): Oracle catalog 'ALL_TAB_SUBPARTITIONS': verify conversion */
    --                                       /* ORIGSQL: all_tab_subpartitions (Oracle catalog) */
    --             WHERE
    --                 TABLE_NAME = :vTCTemplateTable  /* ORIGSQL: table_name (column in Oracle catalog 'ALL_TAB_SUBPARTITIONS') */
    --                 AND SCHEMA_NAME = :vTCSchemaName  /* ORIGSQL: table_owner (column in Oracle catalog 'ALL_TAB_SUBPARTITIONS') */
    --                 AND subpartition_name =
    --                 (
    --                     SELECT   /* ORIGSQL: (SELECT EXT.OD_GETPERIODSUBPARTITIONNAME(vTCSchemaName, vTenantId, vProcessingUnitSeq, vPeriodSeq, vTCTemplateTable) FROM SYS.DUMMY) FROM DUAL) */
    --                         EXT.OD_GETPERIODSUBPARTITIONNAME(:vTCSchemaName, :vTenantId, :vProcessingUnitSeq, :vPeriodSeq, :vTCTemplateTable)   /* ORIGSQL: OD_GetPeriodSubPartitionName(vTCSchemaName, vTenantId, vProcessingUnitSeq, vPeriodSeq, vTCTemplateTable) */
    --                     FROM
    --                         SYS.DUMMY   /* ORIGSQL: FROM DUAL) */
    --                 )
    --     EXCEPT

    --         /* ORIGSQL: MINUS */
    --         SELECT   /* ORIGSQL: SELECT subpartition_name FROM all_tab_subpartitions WHERE TABLE_NAME = vrpttablename AND SCHEMA_NAME=vTenantId||'EXT') ; */
    --             subpartition_name
    --         FROM
    --             SYS.TABLE_PARTITIONS  /* RESOLVE: Catalog reference(partly converted): Oracle catalog 'ALL_TAB_SUBPARTITIONS': verify conversion */
    --                                   /* ORIGSQL: all_tab_subpartitions (Oracle catalog) */
    --         WHERE
    --             TABLE_NAME = :vRptTableName  /* ORIGSQL: table_name (column in Oracle catalog 'ALL_TAB_SUBPARTITIONS') */
    --             AND SCHEMA_NAME = IFNULL(:vTenantId,'')||'EXT'   /* ORIGSQL: table_owner (column in Oracle catalog 'ALL_TAB_SUBPARTITIONS') */
    --     ); --/*Deepan : Partition not required */

    /* ORIGSQL: prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_AddTableSubpartition','START','Auto create subpartitions',NULL) */
    -- CALL EXT.PRC_LOGEVENT(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name, 'prc_AddTableSubpartition', 'START', 'Auto create subpartitions', NULL);/*Deepan : Partition not required*/

/*Deepan : Below partitioning not required*/
--   FOR row AS dbmtk_cursor_968
--     DO
--         v_partitionname = substring(:row.subpartition_name,0,12);  /* ORIGSQL: SUBSTR(row.subpartition_name, 0, 12) */

--         IF :v_FirstCheck IS NULL
--         THEN
--             v_FirstCheck = 'X';

--             /* ORIGSQL: prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_AddTableSubpartition','START','Auto create partition',v_partitionname) */
--             CALL EXT.PRC_LOGEVENT(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name, 'prc_AddTableSubpartition', 'START', 'Auto create partition', :v_partitionname);

--             /* ORIGSQL: OPEN c_partitioncheck(vRptTableName,vTenantId||'EXT',v_partitionname); */
--             OPEN c_partitioncheck(:vRptTableName, IFNULL(:vTenantId,'')||'EXT', :v_partitionname);

--             /* ORIGSQL: FETCH c_partitioncheck INTO v_PartitionAvail; */
--             FETCH c_partitioncheck INTO v_PartitionAvail;

--             /* ORIGSQL: CLOSE c_partitioncheck; */
--             CLOSE c_partitioncheck;

--             IF :v_PartitionAvail IS NULL
--             THEN
--                 SELECT
--                     high_value
--                 INTO
--                     vLong
--                 FROM
--                     SYS.TABLE_PARTITIONS  /* RESOLVE: Catalog reference(partly converted): Oracle catalog 'ALL_TAB_PARTITIONS': verify conversion */
--                                           /* ORIGSQL: all_tab_partitions (Oracle catalog) */
--                 WHERE
--                     TABLE_NAME = UPPER(:vTCTemplateTable)  /* ORIGSQL: table_name (column in Oracle catalog 'ALL_TAB_PARTITIONS') */
--                     AND SCHEMA_NAME = UPPER(:vTCSchemaName)  /* ORIGSQL: table_owner (column in Oracle catalog 'ALL_TAB_PARTITIONS') */
--                     AND TO_NVARCHAR(PART_ID) = :v_partitionname;  /* ORIGSQL: partition_name (column in Oracle catalog 'ALL_TAB_PARTITIONS') */

--                 vVarchar2 = :vLong;

--                 /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
--                 /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER TABLE ' ||vRptTableName|| ' ADD PARTITION '|| v_partitionname || ' VALUES LESS THAN(' ||vVarchar2||') TABLESPACE TALLYDATA' ; */
--                 EXECUTE IMMEDIATE 'ALTER TABLE ' ||IFNULL(:vRptTableName,'')||
--                 ' ADD PARTITION '|| IFNULL(:v_partitionname,'') ||
--                 ' VALUES LESS THAN(' ||IFNULL(:vVarchar2,'')||') TABLESPACE TALLYDATA';
--             END IF;
--         END IF;

--         /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
--         /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER TABLE ' ||vRptTableName|| ' MODIFY PARTITION '|| SUBSTR(row.subpartition_name, 0, 12) || ' ADD SUBPARTITION ' || row.subpartition_name || ' VALUES (' ||row.high_value||') TABL(...) */
--         EXECUTE IMMEDIATE 'ALTER TABLE ' ||IFNULL(:vRptTableName,'')||
--         ' MODIFY PARTITION '|| IFNULL(substring(:row.subpartition_name,0,12),'') ||  /* ORIGSQL: SUBSTR(row.subpartition_name, 0, 12) */
--         ' ADD SUBPARTITION ' || IFNULL(:row.subpartition_name,'') ||
--         ' VALUES (' ||IFNULL(:row.high_value,'')||') TABLESPACE TALLYDATA';
--     END FOR;  /* ORIGSQL: END LOOP; */
END;
  PUBLIC PROCEDURE prc_TruncateTableSubpartition /*Deepan :  partitioning not required*/
(
    IN vRptTableName VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                     /* ORIGSQL: vRptTableName VARCHAR2 */
    IN vSubpartitionName VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                        /* ORIGSQL: vSubpartitionName VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /****************************************************************************************************************
        The purpose of this procedure is to truncate a specified subpartition.
    
        Date        Author     Description
        ------------------------------------------------------------------------------------------------------------
    --- 30 Nov 2017 Tharanikumar  Initial release
    ****************************************************************************************************************/
    /* ORIGSQL: prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_TruncateTableSubpartition','START','Start truncate table '||vSubPartitionName,NULL) */
    -- CALL EXT.PRC_LOGEVENT(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name, 'prc_TruncateTableSubpartition', 'START', 'Start truncate table '||IFNULL(:vSubpartitionName,''), NULL);/*Deepan :  partitioning not required*/

    /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
    /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER TABLE ' ||vRptTableName || ' TRUNCATE SUBPARTITION ' || vSubPartitionName || ' DROP STORAGE' ; */
    -- EXECUTE IMMEDIATE 'ALTER TABLE ' ||IFNULL(:vRptTableName,'') || ' TRUNCATE SUBPARTITION ' || IFNULL(:vSubpartitionName,'') || ' DROP STORAGE';/*Deepan :  partitioning not required*/
END;
  PUBLIC PROCEDURE prc_buildreportingtables
(
    IN vPeriodSeq BIGINT,   /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_buildreportingtables.vPeriodSeq' */
                                                      /* ORIGSQL: vPeriodSeq IN BIGINT */
    IN vProcessingUnitSeq BIGINT,   /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_buildreportingtables.vProcessingUnitSeq' */
                                                                      /* ORIGSQL: vProcessingUnitSeq IN BIGINT */
    IN vCalendarSeq BIGINT,   /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_buildreportingtables.vCalendarSeq' */
                                                          /* ORIGSQL: vCalendarSeq IN BIGINT */
    IN preportgroup VARCHAR(255) DEFAULT NULL      /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                                  /* ORIGSQL: preportgroup IN VARCHAR2 DEFAULT NULL */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */ 
    DECLARE  v_procname VARCHAR(255) = 'prc_buildreportingtables';  /* ORIGSQL: v_procname varchar2(255):='prc_buildreportingtables'; */
    DECLARE vProcessingUnitSeqext BIGINT;/* NOT CONVERTED! */
    DECLARE vpipelinerundate  DATE ;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table/Column 'cs_credit.pipelinerundate' not found (for %TYPE declaration) */
    DECLARE vrunmode  VARCHAR(255);/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table/Column 'cs_pipelinerun.runmode' not found (for %TYPE declaration) */
    DECLARE vpipelinerunseq BIGINT;/* NOT CONVERTED! */
    /* RESOLVE: Identifier not found: Table/Column 'cs_pipelinerun.pipelinerunseq' not found (for %TYPE declaration) */
    DECLARE v_reportlist VARCHAR(30000);  /* ORIGSQL: v_reportlist VARCHAR2(30000); */

    /* ORIGSQL: cursor c_positiongrouplist is Select distinct pgp.name positiongroupname From CS_PipelineRun_Positions PlPos, CS_Position Pos, CS_PositionGroup Pgp, CS_Period Per Where PlPos.PipelineRunSeq = vpipelin(...) */
    DECLARE CURSOR c_positiongrouplist
    FOR
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PIPELINERUN_POSITIONS' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_POSITIONGROUP' not found */
        SELECT   /* ORIGSQL: SELECT distinct pgp.name positiongroupname From CS_PipelineRun_Positions PlPos, CS_Position Pos, CS_PositionGroup Pgp, CS_Period Per Where PlPos.PipelineRunSeq = vpipelinerunseq And PlPos.PositionSeq (...) */
            DISTINCT
            pgp.name AS positiongroupname
        FROM
            CS_PipelineRun_Positions PlPos,
            CS_Position Pos,
            CS_PositionGroup Pgp,
            CS_Period Per
        WHERE
            PlPos.PipelineRunSeq = :vpipelinerunseq
            AND PlPos.PositionSeq = Pos.RuleElementOwnerSeq
            AND Pos.RemoveDate = :cEndofTime
            --And Pos.PositionGroupSeq  = Pgp.PositionGroupSeq   --sudhir
            AND Pgp.RemoveDate = :cEndofTime
            AND per.periodseq = :vPeriodSeq
            AND per.calendarseq = :vCalendarSeq
            AND per.removedate = :cEndofTime
            AND pos.effectivestartdate < Per.enddate
            AND pos.effectiveenddate >= Per.startdate
            AND Pos.createdate <= :vpipelinerundate
            AND Pos.removedate > :vpipelinerundate
            AND Pos.effectivestartdate
            /* RESOLVE: Identifier not found: Table/view 'EXT.CS_POSITION' not found */
            =
            (
                /* ORIGSQL: SELECT / *+ index(pos cs_position_IND1) * / */
                SELECT   /* ORIGSQL: (SELECT MAX(p.effectivestartdate) FROM cs_position p, CS_Period per WHERE p.ruleelementownerseq = pos.ruleelementownerseq And per.periodseq = vPeriodSeq And per.calendarseq = vCalendarSeq And per.remo(...) */
                    MAX(p.effectivestartdate)
                FROM
                    cs_position p,
                    CS_Period per
                WHERE
                    p.ruleelementownerseq = pos.ruleelementownerseq
                    AND per.periodseq = :vPeriodSeq
                    AND per.calendarseq = :vCalendarSeq
                    AND per.removedate = :cEndofTime
                    AND :vpipelinerundate >= p.createdate
                    AND :vpipelinerundate < p.removedate
                    AND p.effectivestartdate < per.enddate
                    AND p.effectiveenddate > per.startdate
                    AND :vProcessingUnitSeq = pos.processingunitseq
            );

    /****************************************************************************************************************
        The purpose of this procedure is to populate reporting custom tables.
        Date        Author        Description
        ------------------------------------------------------------------------------------------------------------
    --- 30 Nov 2017 Tharanikumar  Initial release
    ****************************************************************************************************************/
  DECLARE vPeriodName VARCHAR(100);
  DECLARE vProcessingUnitSeq BIGINT;
  DECLARE vCalendarName VARCHAR(100);
  DECLARE vPeriodShortName VARCHAR(100);
   
    SELECT SESSION_CONTEXT('GvPeriodName') INTO vPeriodName FROM SYS.DUMMY ;
    SELECT SESSION_CONTEXT('GvCalendarName') INTO vCalendarName FROM SYS.DUMMY ;
    SELECT SESSION_CONTEXT('GvProcessingUnitSeq') INTO vProcessingUnitSeq FROM SYS.DUMMY ;
    SELECT SESSION_CONTEXT('GvPeriodShortName') INTO vPeriodShortName FROM SYS.DUMMY ;


    /* ORIGSQL: prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Params : Period:'||vPeriodSeq||',Processing: ' ||vProcessingUnitSeq||', CalendarSeq :' ||vCalendarSeq||', Report gro(...) */
    CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname, :vCalendarName, 'Params : Period:'||IFNULL(:vPeriodSeq,'')||',Processing: '||IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||', CalendarSeq :'||IFNULL(:vCalendarSeq,'')||', Report group : '|| IFNULL(:preportgroup,''), NULL /*NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ */--Deepan: Cannot be used here replacing with NULL
    );  /* ORIGSQL: sqlerrm */

    /* RESOLVE: Identifier not found: Table/view 'TCMP.CS_PIPELINERUN' not found */

    SELECT
        to_date(TO_VARCHAR(starttime,'YYYY-MON-DD HH24:MI:SS'),  /* ORIGSQL: TO_DATE(TO_CHAR (starttime, 'YYYY-MON-DD HH24:MI:SS'), 'YYYY-MON-DD HH24:MI:SS') */
                                                                                                      /* ORIGSQL: TO_CHAR(starttime, 'YYYY-MON-DD HH24:MI:SS') */
        'YYYY-MON-DD HH24:MI:SS'),
        runmode,
        pipelinerunseq
    INTO
        vpipelinerundate,
        vrunmode,
        vpipelinerunseq
    FROM
        tcmp.cs_pipelinerun
    WHERE
        periodseq = vPeriodSeq
        AND processingunitseq = vProcessingUnitSeq
        AND pipelinerunseq  
        =
        (
            SELECT   /* ORIGSQL: (SELECT MAX(pipelinerunseq) FROM tcmp.cs_pipelinerun WHERE periodseq = vperiodseq and processingunitseq = vprocessingunitseq) */
                MAX(pipelinerunseq)
            FROM
                tcmp.cs_pipelinerun
            WHERE
                periodseq = :vPeriodSeq
                AND processingunitseq = :vProcessingUnitSeq
        );

    --to be --
    /* ORIGSQL: prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,  :vCalendarName,'Params : vpipelinerundate:'||vpipelinerundate||',vrunmode: ' ||vrunmode||', vpipelinerunseq :' ||vpipelinerunseq,sql(...) */
    CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Params : vpipelinerundate:'||IFNULL(:vpipelinerundate,'')||',vrunmode: '||IFNULL(:vrunmode,'')||', vpipelinerunseq :'||IFNULL(:vpipelinerunseq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
    );  /* ORIGSQL: sqlerrm */

    --prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,  :vCalendarName,'Started Full Mode',sqlerrm);
    BEGIN 
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
            /* ORIGSQL: when others then */
            BEGIN
                v_reportlist = :preportgroup;
            END;


        /* ORIGSQL: prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,  :vCalendarName,'Enter Build Reporting Table BEGIN',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Enter Build Reporting Table BEGIN', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PIPELINERUN' not found */

        SELECT
            IFNULL(RTRIM(SUBSTRING_REGEXPR('\[odsReportList\]([^\[]+)' FLAG 'i' IN runparameters FROM 1 OCCURRENCE 1), ','),'') ||','   /* ORIGSQL: REGEXP_SUBSTR(runparameters, '\[odsReportList\]([^\[]+)', 1, 1, 'i', 1) */
        INTO
            v_reportlist
        FROM
            cs_pipelinerun
        WHERE
            command = 'PipelineRun'
            AND description LIKE '%ODS%'
            AND state <> 'Pending'  --Added by Gopi
            AND state <> 'Done'
            AND periodseq = :vPeriodSeq
            AND processingunitseq = :vProcessingUnitSeq;

        /* ORIGSQL: exception when others then */
    END;

    /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'v_reportlist :' ||v_reportlist,sqlerrm) */
    CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'v_reportlist :'||IFNULL(:v_reportlist,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
    );  /* ORIGSQL: sqlerrm */
    BEGIN
        /* ORIGSQL: execute immediate 'truncate table stel_classifier_Tab'; */
        /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_CLASSIFIER_TAB' not found */

        /* ORIGSQL: truncate table stel_classifier_Tab ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stel_classifier_Tab';

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Executing truncate table stel_classifier_Tab',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Executing truncate table stel_classifier_Tab', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: insert into stel_classifier_Tab select * from stel_classifier; */
        INSERT INTO ext.stel_classifier_Tab
            SELECT   /* ORIGSQL: select * from stel_classifier; */
                *
            FROM
                ext.stel_classifier;

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Inserting into stel_classifier.',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Inserting into stel_classifier.', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */
    END;
    --to be added
    /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'v_reportlist :' ||v_reportlist,sqlerrm) */
    CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'v_reportlist :'||IFNULL(:v_reportlist,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
    );  /* ORIGSQL: sqlerrm */

    -- BSC Procedures Started

    IF :v_reportlist LIKE '%_BSC_%' 
    THEN
        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'START-BSC Report Group',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'START-BSC Report Group', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Start-PRC_BSC_PAY_MTHQTR_ACHV -'||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||'(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Start-PRC_BSC_PAY_MTHQTR_ACHV -'||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_BSC_PAY_MTHQTR_ACHV('BSCPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
        CALL EXT.PRC_BSC_PAY_MTHQTR_ACHV('BSCPAYEEACHIVEMENT', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'End-PRC_BSC_PAY_MTHQTR_ACHV',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'End-PRC_BSC_PAY_MTHQTR_ACHV', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Start-PRC_BSC_ADV_COMM_PAY'||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCal(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Start-PRC_BSC_ADV_COMM_PAY'||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_BSC_ADV_COMM_PAY('BSCADVCOMMPAY',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
        CALL EXT.PRC_BSC_ADV_COMM_PAY('BSCADVCOMMPAY', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'End-PRC_BSC_ADV_COMM_PAY',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'End-PRC_BSC_ADV_COMM_PAY', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        --to be added
        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'vPeriodRow.Shortname : '||vPeriodRow.shortname,sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'vPeriodShortname : '||IFNULL(:vPeriodShortname,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        IF :vPeriodShortname IN ('Mar', 'Jun', 'Sep', 'Dec')
        THEN
            -- Quaterly Report Generation only

            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'START - PRC_BSC_QTR_PAY_SUM -QTR- Started'||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessing(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'START - PRC_BSC_QTR_PAY_SUM -QTR- Started'||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */

            /* ORIGSQL: PRC_BSC_QTR_PAY_SUM('BSCQTRPAYSUM',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
            CALL EXT.PRC_BSC_QTR_PAY_SUM('BSCQTRPAYSUM', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'END - PRC_BSC_QTR_PAY_SUM -QTR- Ended',sqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'END - PRC_BSC_QTR_PAY_SUM -QTR- Ended', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */

            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'START - PRC_BSC_QTR_HIGHLIGHT -QTR- started'||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessi(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'START - PRC_BSC_QTR_HIGHLIGHT -QTR- started'||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */

            /* ORIGSQL: PRC_BSC_QTR_HIGHLIGHT('PAYEEQTRPAYSUMMARY',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
            CALL EXT.PRC_BSC_QTR_HIGHLIGHT('PAYEEQTRPAYSUMMARY', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

            -- It should be always after PRC_BSC_PAY_MTHQTR_ACHV

            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'END - PRC_BSC_QTR_HIGHLIGHT -QTR- Ended',sqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'END - PRC_BSC_QTR_HIGHLIGHT -QTR- Ended', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */
        END IF;

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'End-BSC Report Group',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'End-BSC Report Group', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */
    END IF;

    --CCO Procedures

    IF :v_reportlist LIKE '%_CCO_%' 
    THEN
        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Start-CCO Report Group',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Start-CCO Report Group', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Started - PRC_CCO_MOBILE_PAYOUTSUMM - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUni(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Started - PRC_CCO_MOBILE_PAYOUTSUMM - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_CCO_MOBILE_PAYOUTSUMM('CCOMOBILEPAYOUTSUMM',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
        CALL EXT.PRC_CCO_MOBILE_PAYOUTSUMM('CCOMOBILEPAYOUTSUMM', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Ended - PRC_CCO_MOBILE_PAYOUTSUMM - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Ended - PRC_CCO_MOBILE_PAYOUTSUMM - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Started - PRC_CCO_MOBILE_RAWDATA - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSe(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Started - PRC_CCO_MOBILE_RAWDATA - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_CCO_MOBILE_RAWDATA(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
        CALL EXT.PRC_CCO_MOBILE_RAWDATA(:vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Ended - PRC_CCO_MOBILE_RAWDATA - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Ended - PRC_CCO_MOBILE_RAWDATA - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Started - PRC_CCO_SINGTELTV_DETAILAI - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUn(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Started - PRC_CCO_SINGTELTV_DETAILAI - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_CCO_SINGTELTV_DETAILAI(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
        CALL EXT.PRC_CCO_SINGTELTV_DETAILAI(:vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Ended - PRC_CCO_SINGTELTV_DETAILAI - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Ended - PRC_CCO_SINGTELTV_DETAILAI - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Started - PRC_CCO_TV_PAYOUTSUMMARY - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnit(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Started - PRC_CCO_TV_PAYOUTSUMMARY - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_CCO_TV_PAYOUTSUMMARY('CCOTVPAYMENTSUMMARY',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
        CALL EXT.PRC_CCO_TV_PAYOUTSUMMARY('CCOTVPAYMENTSUMMARY', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Ended - PRC_CCO_TV_PAYOUTSUMMARY - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSe(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Ended - PRC_CCO_TV_PAYOUTSUMMARY - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'END-CCO Report Group',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'END-CCO Report Group', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */
    END IF;

    --STS procedures

    IF :v_reportlist LIKE '%_STS_%' 
    THEN
        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Start-STS Report Group',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Start-STS Report Group', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Started - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessi(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Started - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_STS_RC_DS_PAYEE_INDIVIDUAL('STSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY') */
        CALL EXT.PRC_STS_RC_DS_PAYEE_INDIVIDUAL('STSPAYEEACHIVEMENT', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq, 'MONTHLY');

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Ended - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Ended - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'vPeriodShortname : '||vPeriodShortname,sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'vPeriodShortname : '||IFNULL(:vPeriodShortname,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        IF :vPeriodShortname IN ('Mar', 'Jun', 'Sep', 'Dec')
        THEN
            -- Quaterly Report Generation only
            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Started - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - Qrt -  '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vP(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Started - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - Qrt -  '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */

            /* ORIGSQL: PRC_STS_RC_DS_PAYEE_INDIVIDUAL('STSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'QUARTERLY') */
            CALL EXT.PRC_STS_RC_DS_PAYEE_INDIVIDUAL('STSPAYEEACHIVEMENT', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq, 'QUARTERLY');

            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Ended - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - Qrt -  ',sqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Ended - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - Qrt -  ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */
        END IF;

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED - PRC_STSRCSDS_PAYEE_SUMMARY - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUn(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED - PRC_STSRCSDS_PAYEE_SUMMARY - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_STSRCSDS_PAYEE_SUMMARY('STSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
        CALL EXT.PRC_STSRCSDS_PAYEE_SUMMARY('STSPAYEEACHIVEMENT', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

        -- It should be always after PRC_STS_PAYEE_INDIVIDUAL
        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED - PRC_STSRCSDS_PAYEE_SUMMARY - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED - PRC_STSRCSDS_PAYEE_SUMMARY - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Started - PRC_STS_ROADSHOW -  '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Started - PRC_STS_ROADSHOW -  '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_STS_ROADSHOW('STSROADSHOW',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
        CALL EXT.PRC_STS_ROADSHOW('STSROADSHOW', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

        -- It should be always after PRC_STS_PAYEE_INDIVIDUAL
        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Ended - PRC_STS_ROADSHOW -  ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Ended - PRC_STS_ROADSHOW -  ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Started - PRC_STS_COMM_HIGHLIGHT -  '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitS(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Started - PRC_STS_COMM_HIGHLIGHT -  '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_STS_COMM_HIGHLIGHT(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
        CALL EXT.PRC_STS_COMM_HIGHLIGHT(:vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

        -- It should be always after PRC_STS_PAYEE_INDIVIDUAL
        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Ended - PRC_STS_COMM_HIGHLIGHT -  ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Ended - PRC_STS_COMM_HIGHLIGHT -  ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'END-STS Report Group',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'END-STS Report Group', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */
    END IF;

    --RCS Procedures

    IF :v_reportlist LIKE '%_RCS_%' 
    THEN
        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'START-RCS report Group',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'START-RCS report Group', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Started - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessi(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Started - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_STS_RC_DS_PAYEE_INDIVIDUAL('RCSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY') */
        CALL EXT.PRC_STS_RC_DS_PAYEE_INDIVIDUAL('RCSPAYEEACHIVEMENT', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq, 'MONTHLY');

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Ended - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Ended - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'vPeriodShortname: '||vPeriodShortname,sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'vPeriodShortname: '||IFNULL(:vPeriodShortname,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        IF :vPeriodShortname IN ('Mar', 'Jun', 'Sep', 'Dec')
        THEN
            -- Quaterly Report Generation only
            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL -QTR - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vPro(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL -QTR - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */

            /* ORIGSQL: PRC_STS_RC_DS_PAYEE_INDIVIDUAL('RCSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'QUARTERLY') */
            CALL EXT.PRC_STS_RC_DS_PAYEE_INDIVIDUAL('RCSPAYEEACHIVEMENT', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq, 'QUARTERLY');

            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL -QTR - ',sqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL -QTR - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */
        END IF;

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED - PRC_STSRCSDS_PAYEE_SUMMARY - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUn(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED - PRC_STSRCSDS_PAYEE_SUMMARY - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_STSRCSDS_PAYEE_SUMMARY('RCSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
        CALL EXT.PRC_STSRCSDS_PAYEE_SUMMARY('RCSPAYEEACHIVEMENT', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED - PRC_STSRCSDS_PAYEE_SUMMARY - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED - PRC_STSRCSDS_PAYEE_SUMMARY - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED - PRC_RCS_MICHAEL_INDIVIDUAL - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUn(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED - PRC_RCS_MICHAEL_INDIVIDUAL - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_RCS_MICHAEL_INDIVIDUAL('MRCSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY') */
        CALL EXT.PRC_RCS_MICHAEL_INDIVIDUAL('MRCSPAYEEACHIVEMENT', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq, 'MONTHLY');

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED - PRC_RCS_MICHAEL_INDIVIDUAL - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED - PRC_RCS_MICHAEL_INDIVIDUAL - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'End-RCS report group.',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'End-RCS report group.', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */
    END IF;

    --Digital Sales Procedures

    IF :v_reportlist LIKE '%_DS_%' 
    THEN
        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'START-DS report group.',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'START-DS report group.', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessi(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_STS_RC_DS_PAYEE_INDIVIDUAL('DSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY') */
        CALL EXT.PRC_STS_RC_DS_PAYEE_INDIVIDUAL('DSPAYEEACHIVEMENT', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq, 'MONTHLY');

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'vPeriodShortname : '||vPeriodShortname,sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'vPeriodShortname : '||IFNULL(:vPeriodShortname,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        IF :vPeriodShortname IN ('Mar', 'Jun', 'Sep', 'Dec')
        THEN
            -- Quaterly Report Generation only
            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL -QTR- '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProc(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL -QTR- '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */

            /* ORIGSQL: PRC_STS_RC_DS_PAYEE_INDIVIDUAL('DSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'QUARTERLY') */
            CALL EXT.PRC_STS_RC_DS_PAYEE_INDIVIDUAL('DSPAYEEACHIVEMENT', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq, 'QUARTERLY');

            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL -QTR- ',sqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL -QTR- ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */
        END IF;

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED - PRC_STSRCSDS_PAYEE_SUMMARY - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUn(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED - PRC_STSRCSDS_PAYEE_SUMMARY - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_STSRCSDS_PAYEE_SUMMARY('DSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
        CALL EXT.PRC_STSRCSDS_PAYEE_SUMMARY('DSPAYEEACHIVEMENT', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED - PRC_STSRCSDS_PAYEE_SUMMARY - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED - PRC_STSRCSDS_PAYEE_SUMMARY - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'END -DS report group.',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'END -DS report group.', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */
    END IF;
    /*
    --TEPL Report only for Admin Group not for User Group
    if v_reportlist like '%_TEPL_%' THEN
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENTER _TEPL_ report group.',sqlerrm);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_AI_SUMMARY - STARTED',sqlerrm);
      PRC_TEPL_AI_SUMMARY('TEPLAISUMMARY',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_AI_SUMMARY - ENDED',sqlerrm);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_MSF_SUMMONTHLY - STARTED',sqlerrm);
      PRC_TEPL_MSF_SUMMONTHLY(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_MSF_SUMMONTHLY - ENDED',sqlerrm);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_AI_MSF_DETAIL - STARTED',sqlerrm);
      PRC_TEPL_AI_MSF_DETAIL('TEPLAIMSFDETAIL',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq); -- It should be always after PRC_TEPL_AI_SUMMARY
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_AI_MSF_DETAIL - ENDED',sqlerrm);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_MSF_SUMMDETAIL - STARTED',sqlerrm);
      PRC_TEPL_MSF_SUMMDETAIL(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_MSF_SUMMDETAIL - ENDED',sqlerrm);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_MOBILE_MBB_SPICE - STARTED',sqlerrm);
      PRC_TEPL_MOBILE_MBB_SPICE(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_MOBILE_MBB_SPICE - ENDED',sqlerrm);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_ACT_ACR - STARTED',sqlerrm);
      PRC_TEPL_ACT_ACR('TEPLACTUALACCRUAL',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_ACT_ACR - ENDED',sqlerrm);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_VOUCHER_COVERNOTE - STARTED',sqlerrm);
      PRC_TEPL_VOUCHER_COVERNOTE(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_VOUCHER_COVERNOTE - ENDED',sqlerrm);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_CONSUMER_COVERNOTEYKM - STARTED',sqlerrm);
      PRC_TEPL_CONSUMER_COVERNOTEYKM('TEPLCVRNOTEKYM',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_CONSUMER_COVERNOTEYKM - ENDED',sqlerrm);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_CONSUMER_REQMEMO - STARTED',sqlerrm);
      PRC_TEPL_CONSUMER_REQMEMO('TEPLCONSREQMEMO',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Procedure PRC_TEPL_CONSUMER_REQMEMO - ENDED',sqlerrm);
        prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Exit _TEPL_ report group.',sqlerrm);
    End if;
    */

    --CSTI

    IF :v_reportlist LIKE '%_CSTI_%' 
    THEN
        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'START - CSTI report group.',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'START - CSTI report group.', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED PRC_CSTI_TRANS_DETAIL - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED PRC_CSTI_TRANS_DETAIL - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_CSTI_TRANS_DETAIL(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
        CALL EXT.PRC_CSTI_TRANS_DETAIL(:vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED PRC_CSTI_TRANS_DETAIL ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED PRC_CSTI_TRANS_DETAIL ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'END - CSTI report group.',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'END - CSTI report group.', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */
    END IF;

    --COMMON

    IF :v_reportlist LIKE '%_Comm_%' 
    THEN
        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'START - COMM report group',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'START - COMM report group', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED PRC_COMM_PAYOUT_MONTHLY - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED PRC_COMM_PAYOUT_MONTHLY - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_COMM_PAYOUT_MONTHLY(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
        CALL EXT.PRC_COMM_PAYOUT_MONTHLY(:vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED PRC_COMM_PAYOUT_MONTHLY - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED PRC_COMM_PAYOUT_MONTHLY - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED PRC_COMM_SAA_SUMMARY - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||'(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED PRC_COMM_SAA_SUMMARY - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_COMM_SAA_SUMMARY('SAASUMMARY',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY') */
        CALL EXT.PRC_COMM_SAA_SUMMARY('SAASUMMARY', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq, 'MONTHLY');

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED PRC_COMM_SAA_SUMMARY - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED PRC_COMM_SAA_SUMMARY - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'vPeriodShortname : '||vPeriodShortname,sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'vPeriodShortname : '||IFNULL(:vPeriodShortname,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        IF :vPeriodShortname IN ('Mar', 'Jun', 'Sep', 'Dec')
        THEN
            -- Quaterly Report Generation only
            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED PRC_COMM_SAA_SUMMARY - QTR - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnit(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED PRC_COMM_SAA_SUMMARY - QTR - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */

            /* ORIGSQL: PRC_COMM_SAA_SUMMARY('SAASUMMARY',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'QUARTERLY') */
            CALL EXT.PRC_COMM_SAA_SUMMARY('SAASUMMARY', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq, 'QUARTERLY');

            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED PRC_COMM_SAA_SUMMARY - QTR - ',sqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED PRC_COMM_SAA_SUMMARY - QTR - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */
        END IF;

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'END - COMM report group',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'END - COMM report group', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */
    END IF;

    -- Direct Sales

    IF :v_reportlist LIKE '%_DirectSales_%' 
    THEN
        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'START DirectSales report group.',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'START DirectSales report group.', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED PRC_DIRECTSALES_INDIVIDUAL - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnit(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED PRC_DIRECTSALES_INDIVIDUAL - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_DIRECTSALES_INDIVIDUAL('DIRECTSALESACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY') */
        CALL EXT.PRC_DIRECTSALES_INDIVIDUAL('DIRECTSALESACHIVEMENT', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq, 'MONTHLY');

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED PRC_DIRECTSALES_INDIVIDUAL - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED PRC_DIRECTSALES_INDIVIDUAL - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        IF :vPeriodShortname IN ('Mar', 'Jun', 'Sep', 'Dec')
        THEN
            -- Quaterly Report Generation only
            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED PRC_DIRECTSALES_INDIVIDUAL -QTR- '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessing(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED PRC_DIRECTSALES_INDIVIDUAL -QTR- '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */

            /* ORIGSQL: PRC_DIRECTSALES_INDIVIDUAL('DIRECTSALESACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'QUARTERLY') */
            CALL EXT.PRC_DIRECTSALES_INDIVIDUAL('DIRECTSALESACHIVEMENT', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq, 'QUARTERLY');

            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED PRC_DIRECTSALES_INDIVIDUAL -QTR- ',sqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED PRC_DIRECTSALES_INDIVIDUAL -QTR- ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */
        END IF;

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'END DirectSales report group.',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'END DirectSales report group.', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */
    END IF;

    -- Internal Prepaid
    /* Commenting IP for implementing the quarterly code change for director
      if v_reportlist like '%_IP_Individual Payment Summary%' THEN
      PRC_INTPREPAID_INDIVIDUAL('INTMGR',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
      PRC_INTPREPAIDSNR_INDIVIDUAL('INTSNRBIZDEMGR',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
      End if;
      */
    -- Internal Prepaid

    IF :v_reportlist LIKE '%_IP_Individual Payment Summary%' 
    THEN
        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'START - IP_Individual Report Group.',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'START - IP_Individual Report Group.', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED - PRC_INTPREPAID_INDIVIDUAL - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUni(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED - PRC_INTPREPAID_INDIVIDUAL - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_INTPREPAID_INDIVIDUAL('INTMGR',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY') */
        CALL EXT.PRC_INTPREPAID_INDIVIDUAL('INTMGR', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq, 'MONTHLY');

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED - PRC_INTPREPAID_INDIVIDUAL - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED - PRC_INTPREPAID_INDIVIDUAL - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED - PRC_INTPREPAIDSNR_INDIVIDUAL - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessing(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED - PRC_INTPREPAIDSNR_INDIVIDUAL - '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: PRC_INTPREPAIDSNR_INDIVIDUAL('INTSNRBIZDEMGR',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY') */
        CALL EXT.PRC_INTPREPAIDSNR_INDIVIDUAL('INTSNRBIZDEMGR', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq, 'MONTHLY');

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED - PRC_INTPREPAIDSNR_INDIVIDUAL - ',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED - PRC_INTPREPAIDSNR_INDIVIDUAL - ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'vPeriodShortname : '||vPeriodShortname,sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, ':vPeriodShortname : '||IFNULL(:vPeriodShortname,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */

        IF :vPeriodShortname IN ('Mar', 'Jun', 'Sep', 'Dec')
        THEN
            -- Quaterly Report Generation only
            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'STARTED - PRC_INTPREPAIDSNR_INDIVIDUAL -QTR- '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProces(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'STARTED - PRC_INTPREPAIDSNR_INDIVIDUAL -QTR- '||'Parms:vPeriodSeq :'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq:'|| IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||',vCalendarSeq :'|| IFNULL(:vCalendarSeq,''), NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */

            /* ORIGSQL: PRC_INTPREPAID_INDIVIDUAL('INTMGR',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'QUARTERLY') */
            CALL EXT.PRC_INTPREPAID_INDIVIDUAL('INTMGR', :vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq, 'QUARTERLY');

            /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'ENDED - PRC_INTPREPAIDSNR_INDIVIDUAL -QTR- ',sqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'ENDED - PRC_INTPREPAIDSNR_INDIVIDUAL -QTR- ', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
            );  /* ORIGSQL: sqlerrm */
        END IF;
        /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'END IP_Individual Report Group.',sqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'END IP_Individual Report Group.', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
        );  /* ORIGSQL: sqlerrm */
    END IF;

    /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'Ended Full Mode',sqlerrm) */
    CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'Ended Full Mode', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
    );  /* ORIGSQL: sqlerrm */

    /* ORIGSQL: prc_logevent(:vPeriodName,v_procname,  :vCalendarName,'END Build Reporting Table',sqlerrm) */
    CALL EXT.PRC_LOGEVENT(:vPeriodName, :v_procname,   :vCalendarName, 'END Build Reporting Table', NULL /*::SQL_ERROR_MESSAGE*/ --/*Deepan: Cannot be used here, replacing with NULL*/ 
    );  /* ORIGSQL: sqlerrm */
END;
  PUBLIC PROCEDURE prc_buildbasetables
(
    IN vPeriodSeq BIGINT,   /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_buildbasetables.vPeriodSeq' */
                                                      /* ORIGSQL: vPeriodSeq IN BIGINT */
    IN vProcessingUnitSeq BIGINT,   /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_buildbasetables.vProcessingUnitSeq' */
                                                                      /* ORIGSQL: vProcessingUnitSeq IN BIGINT */
    IN vCalendarSeq BIGINT      /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_buildbasetables.vCalendarSeq' */
                                                            /* ORIGSQL: vCalendarSeq IN BIGINT */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN SEQUENTIAL EXECUTION
    -- DECLARE DBMTK_TMPVAR_STRING_1 VARCHAR(5000); /*sapdbmtk-generated help variable*/
    -- DECLARE DBMTK_CTV_PROCID INT := sapdbmtk.sp_f_dbmtk_ctv_procid(); /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    /****************************************************************************************************************
        The purpose of this procedure is to populate reporting base tables.
    
        Date        Author     Description
        ------------------------------------------------------------------------------------------------------------
    --- 30 Nov 2017 Tharanikumar  Initial release
    ****************************************************************************************************************/
 DECLARE vPeriodStartDate timestamp;
 DECLARE vPeriodEndDate timestamp;
 
 
    SELECT SESSION_CONTEXT('GvPeriodStartDate') INTO vPeriodStartDate FROM SYS.DUMMY ;
    SELECT SESSION_CONTEXT('GvPeriodEndDate') INTO vPeriodEndDate FROM SYS.DUMMY ;

    /* ORIGSQL: Cursor c_pareporting is SELECT par.*, SYSDATE FROM cs_pareporting par WHERE par.processingunitseq = vProcessingUnitSeq AND par.effectivestartdate < vPeriodRow.enddate AND par.effectiveenddate >= vPeri(...) */
   

    -- DECLARE CURSOR c_pareporting
    -- FOR 
    --     SELECT   /* ORIGSQL: SELECT par.*, SYSDATE FROM cs_pareporting par WHERE par.processingunitseq = vProcessingUnitSeq AND par.effectivestartdate < vPeriodRow.enddate AND par.effectiveenddate >= vPeriodRow.startdate AND effe(...) */
    --         par.*,
    --         CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
    --     FROM
    --         cs_pareporting par
    --     WHERE
    --         par.processingunitseq = :vProcessingUnitSeq
    --         AND par.effectivestartdate < vPeriodRow.enddate
    --         AND par.effectiveenddate >= vPeriodRow.startdate
    --         AND effectiveenddate
    --         /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PAREPORTING' not found */
    --         =
    --         (
    --             SELECT   /* ORIGSQL: (SELECT MAX(effectiveenddate) FROM cs_pareporting WHERE effectivestartdate < vPeriodRow.enddate AND effectiveenddate >= vPeriodRow.startdate AND removedate = cendoftime AND par.descendantpositionseq =(...) */
    --                 MAX(effectiveenddate)
    --             FROM
    --                 cs_pareporting
    --             WHERE
    --                 effectivestartdate < vPeriodRow.enddate
    --                 AND effectiveenddate >= vPeriodRow.startdate
    --                 AND removedate = :cEndofTime
    --                 AND par.descendantpositionseq = descendantpositionseq
    --                 AND par.descendantuserid = descendantuserid
    --                 AND par.Ancestorpositionseq = ancestorpositionseq
    --         ) -- Siva: Added this condition to form hierachy correctly
    --         AND par.removedate = :cEndofTime;

    /* ORIGSQL: prc_setVariables(vperiodseq, vprocessingunitseq, vcalendarseq) */
    CALL prc_setVariables(:vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

    /* ORIGSQL: prc_AnalyzeTable('rpt_base_padimension') */
    -- CALL prc_AnalyzeTable('rpt_base_padimension');/*Deepan : Partition Not required*/


    /* ORIGSQL: prc_base_padimension(vperiodseq, vprocessingunitseq, vcalendarseq) */
    CALL EXT.PRC_BASE_PADIMENSION(:vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);

    /* ORIGSQL: prc_AnalyzeTable('rpt_base_padimension') */
    -- CALL prc_AnalyzeTable('rpt_base_padimension');/*Deepan : Partition Not required*/


    /* ORIGSQL: prc_AnalyzeTable('rpt_base_salestransaction') */
    -- CALL prc_AnalyzeTable('rpt_base_salestransaction');/*Deepan : Partition Not required*/

-- 
    /* ORIGSQL: prc_base_salestransaction(vperiodseq, vprocessingunitseq, vcalendarseq) */
    CALL EXT.PRC_BASE_SALESTRANSACTION(:vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);


    /* ORIGSQL: prc_AnalyzeTable('rpt_base_salestransaction') */
    -- CALL prc_AnalyzeTable('rpt_base_salestransaction');/*Deepan : Partition Not required*/


    /* ORIGSQL: prc_AnalyzeTable('rpt_base_pareporting') */
    -- CALL prc_AnalyzeTable('rpt_base_pareporting');/*Deepan : Partition Not required*/



      /*Deepan : replacing the entire logic without using cursors*/
        INSERT INTO ext.rpt_base_pareporting (
        SELECT par.*, CURRENT_TIMESTAMP AS sysdate
        FROM cs_pareporting par
        WHERE par.processingunitseq = :vProcessingUnitSeq
            AND par.effectivestartdate < :vPeriodEndDate
            AND par.effectiveenddate >= :vPeriodStartDate
            AND effectiveenddate = (SELECT MAX(effectiveenddate)
                                    FROM cs_pareporting
                                    WHERE effectivestartdate < :vPeriodEndDate
                                        AND effectiveenddate >= :vPeriodStartDate
                                        AND removedate = :cEndofTime  
                                        AND par.descendantpositionseq = descendantpositionseq
                                        AND par.descendantuserid = descendantuserid
                                        AND par.Ancestorpositionseq = ancestorpositionseq)
            AND par.removedate =  :cEndofTime
    );
    /* ORIGSQL: OPEN c_pareporting; */
    -- OPEN c_pareporting;

    -- /* ORIGSQL: LOOP */
    -- LOOP 
    --     BEGIN
    --         /* RESOLVE: Collection datatype/operation, rewrite needed: Collection operation (FETCH..BULK COLLECT INTO) must be migrated manually. */
    --         /* ORIGSQL: FETCH c_pareporting BULK COLLECT INTO vPaReporting LIMIT 10000; */
    --         FETCH c_pareporting INTO vPaReporting LIMIT 10000;

    --         /* ORIGSQL: FORALL i IN 1..vPaReporting.COUNT INSERT / *+ APPEND PARALLEL * / */

    --         FOR i IN 1 .. sapdbmtk.sp_f_dbmtk_ctv_count('vPaReporting',:DBMTK_CTV_PROCID)  /* ORIGSQL: vPaReporting.COUNT */
    --         /* ORIGSQL: INSERT / *+ APPEND PARALLEL * / */
    --         DO
    --             /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_BASE_PAREPORTING' not found */

    --             /* ORIGSQL: INSERT INTO ext.rpt_base_pareporting NOLOGGING VALUES vPaReporting(i); */
    --             DBMTK_TMPVAR_STRING_1 = :vPaReporting.column_value[sapdbmtk.sp_f_dbmtk_ctvan_map(:i,'vPaReporting',:DBMTK_CTV_PROCID)];
    --             INSERT INTO ext.rpt_base_pareporting
    --             VALUES (:DBMTK_TMPVAR_STRING_1);  /* ORIGSQL: vPaReporting(i) */
    --         END FOR;  /* ORIGSQL: END LOOP; */

    --         /* ORIGSQL: COMMIT; */
    --         COMMIT;

    --         /* ORIGSQL: EXIT WHEN c_pareporting%NOTFOUND */
    --         IF c_pareporting::NOTFOUND  
    --         THEN
    --             BREAK;
    --         END IF;
    --     END;
    -- END LOOP;  /* ORIGSQL: END LOOP; */

    -- /* ORIGSQL: CLOSE c_pareporting; */
    -- CLOSE c_pareporting;

    /* ORIGSQL: prc_AnalyzeTable('rpt_base_deposit') */
    -- CALL prc_AnalyzeTable('rpt_base_deposit'); 

    /*INSERT INTO ext.rpt_base_deposit
    (SELECT d.*, SYSDATE*/
        --added by kyap, column values as there's an error on column mismatch 
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_STAGESUMMARY' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_STAGETYPE' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_DEPOSIT' not found */
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_BASE_DEPOSIT' not found */

        /* ORIGSQL: INSERT INTO ext.rpt_base_deposit (TENANTID, DEPOSITSEQ, NAME, PAYEESEQ, POSITIONSEQ, PERIODSEQ, PIPELINERUNSEQ, ORIGINTYPEID, PIPELINERUNDATE, BUSINESSUNITMAP, PREADJUSTEDVALUE, UNITTYPEFORPREADJU(...) */
        INSERT INTO ext.rpt_base_deposit
            (
                TENANTID,
                DEPOSITSEQ,
                NAME,
                PAYEESEQ,
                POSITIONSEQ,
                PERIODSEQ,
                PIPELINERUNSEQ,
                ORIGINTYPEID,
                PIPELINERUNDATE,
                BUSINESSUNITMAP,
                PREADJUSTEDVALUE,
                UNITTYPEFORPREADJUSTEDVALUE,
                VALUE,
                UNITTYPEFORVALUE,
                EARNINGGROUPID,
                EARNINGCODEID,
                RULESEQ,
                ISHELD,
                RELEASEDATE,
                DEPOSITDATE,
                REASONSEQ,
                COMMENTS,
                GENERICATTRIBUTE1,
                GENERICATTRIBUTE2,
                GENERICATTRIBUTE3,
                GENERICATTRIBUTE4,
                GENERICATTRIBUTE5,
                GENERICATTRIBUTE6,
                GENERICATTRIBUTE7,
                GENERICATTRIBUTE8,
                GENERICATTRIBUTE9,
                GENERICATTRIBUTE10,
                GENERICATTRIBUTE11,
                GENERICATTRIBUTE12,
                GENERICATTRIBUTE13,
                GENERICATTRIBUTE14,
                GENERICATTRIBUTE15,
                GENERICATTRIBUTE16,
                GENERICNUMBER1,
                UNITTYPEFORGENERICNUMBER1,
                GENERICNUMBER2,
                UNITTYPEFORGENERICNUMBER2,
                GENERICNUMBER3,
                UNITTYPEFORGENERICNUMBER3,
                GENERICNUMBER4,
                UNITTYPEFORGENERICNUMBER4,
                GENERICNUMBER5,
                UNITTYPEFORGENERICNUMBER5,
                GENERICNUMBER6,
                UNITTYPEFORGENERICNUMBER6,
                GENERICDATE1,
                GENERICDATE2,
                GENERICDATE3,
                GENERICDATE4,
                GENERICDATE5,
                GENERICDATE6,
                GENERICBOOLEAN1,
                GENERICBOOLEAN2,
                GENERICBOOLEAN3,
                GENERICBOOLEAN4,
                GENERICBOOLEAN5,
                GENERICBOOLEAN6,
                PROCESSINGUNITSEQ,
                LOADDTTM
            )
            
                SELECT   /* ORIGSQL: (select d.TENANTID, d.DEPOSITSEQ, d.NAME, d.PAYEESEQ, d.POSITIONSEQ, d.PERIODSEQ, d.PIPELINERUNSEQ, d.ORIGINTYPEID, d.PIPELINERUNDATE, d.BUSINESSUNITMAP, d.PREADJUSTEDVALUE, d.UNITTYPEFORPREADJUSTEDVA(...) */
                    d.TENANTID,
                    d.DEPOSITSEQ,
                    d.NAME,
                    d.PAYEESEQ,
                    d.POSITIONSEQ,
                    d.PERIODSEQ,
                    d.PIPELINERUNSEQ,
                    d.ORIGINTYPEID,
                    d.PIPELINERUNDATE,
                    d.BUSINESSUNITMAP,
                    d.PREADJUSTEDVALUE,
                    d.UNITTYPEFORPREADJUSTEDVALUE,
                    d.VALUE,
                    d.UNITTYPEFORVALUE,
                    d.EARNINGGROUPID,
                    d.EARNINGCODEID,
                    d.RULESEQ,
                    d.ISHELD,
                    d.RELEASEDATE,
                    d.DEPOSITDATE,
                    d.REASONSEQ,
                    d.COMMENTS,
                    d.GENERICATTRIBUTE1,
                    d.GENERICATTRIBUTE2,
                    d.GENERICATTRIBUTE3,
                    d.GENERICATTRIBUTE4,
                    d.GENERICATTRIBUTE5,
                    d.GENERICATTRIBUTE6,
                    d.GENERICATTRIBUTE7,
                    d.GENERICATTRIBUTE8,
                    d.GENERICATTRIBUTE9,
                    d.GENERICATTRIBUTE10,
                    d.GENERICATTRIBUTE11,
                    d.GENERICATTRIBUTE12,
                    d.GENERICATTRIBUTE13,
                    d.GENERICATTRIBUTE14,
                    d.GENERICATTRIBUTE15,
                    d.GENERICATTRIBUTE16,
                    d.GENERICNUMBER1,
                    d.UNITTYPEFORGENERICNUMBER1,
                    d.GENERICNUMBER2,
                    d.UNITTYPEFORGENERICNUMBER2,
                    d.GENERICNUMBER3,
                    d.UNITTYPEFORGENERICNUMBER3,
                    d.GENERICNUMBER4,
                    d.UNITTYPEFORGENERICNUMBER4,
                    d.GENERICNUMBER5,
                    d.UNITTYPEFORGENERICNUMBER5,
                    d.GENERICNUMBER6,
                    d.UNITTYPEFORGENERICNUMBER6,
                    d.GENERICDATE1,
                    d.GENERICDATE2,
                    d.GENERICDATE3,
                    d.GENERICDATE4,
                    d.GENERICDATE5,
                    d.GENERICDATE6,
                    d.GENERICBOOLEAN1,
                    d.GENERICBOOLEAN2,
                    d.GENERICBOOLEAN3,
                    d.GENERICBOOLEAN4,
                    d.GENERICBOOLEAN5,
                    d.GENERICBOOLEAN6,
                    d.PROCESSINGUNITSEQ,
                    CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                FROM
                    cs_deposit d,
                    cs_period per
                WHERE
                    d.periodseq = per.periodseq
                    AND d.processingunitseq = :vProcessingUnitSeq
                    --AND per.startdate >= vCurYrStartDate
                    --AND per.enddate <= vPeriodRow.enddate
                    AND per.removedate = :cEndofTime
                    AND (per.periodseq = :vPeriodSeq
                        OR per.shortname IN ('Mar', 'Jun', 'Sep', 'Dec'))
                    AND (d.originTypeId IN ('manual', 'imported')
                        OR (d.pipelinerunseq IN
                            (
                                SELECT   /* ORIGSQL: (SELECT pr.pipelineRunSeq FROM cs_pipelinerun pr, cs_stagesummary ss, cs_stagetype st WHERE pr.pipelineRunSeq = ss.pipelineRunSeq AND ss.stageTypeSeq = st.stageTypeSeq AND st.name = 'Reward' AND pr.pe(...) */
                                    pr.pipelineRunSeq
                                FROM
                                    cs_pipelinerun pr,
                                    cs_stagesummary ss,
                                    cs_stagetype st
                                WHERE
                                    pr.pipelineRunSeq = ss.pipelineRunSeq
                                    AND ss.stageTypeSeq = st.stageTypeSeq
                                    AND st.name = 'Reward'
                                    AND pr.periodSeq = :vPeriodSeq
                                    AND pr.processingunitseq = :vProcessingUnitSeq
                                    AND ss.isactive = 1
                            )
                    ))
            ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: prc_AnalyzeTable('rpt_base_deposit') */
    -- CALL prc_AnalyzeTable('rpt_base_deposit');/*Deepan : Parition not required*/

    /* ORIGSQL: prc_AnalyzeTable('rpt_base_incentive') */
    -- CALL prc_AnalyzeTable('rpt_base_incentive');/*Deepan : Parition not required*/

    /*INSERT INTO ext.rpt_base_incentive
    (SELECT i.*, SYSDATE*/
        --added by kyap, column values as there's an error on column mismatch   
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_INCENTIVE' not found */
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_BASE_INCENTIVE' not found */

        /* ORIGSQL: INSERT INTO ext.rpt_base_incentive(TENANTID, INCENTIVESEQ, NAME, PAYEESEQ, POSITIONSEQ, PERIODSEQ, PIPELINERUNSEQ, PIPELINERUNDATE, PLANSEQ, RULESEQ, VALUE, UNITTYPEFORVALUE, ISACTIVE, RELEASEDATE(...) */
        INSERT INTO ext.rpt_base_incentive
            (
                TENANTID,
                INCENTIVESEQ,
                NAME,
                PAYEESEQ,
                POSITIONSEQ,
                PERIODSEQ,
                PIPELINERUNSEQ,
                PIPELINERUNDATE,
                PLANSEQ,
                RULESEQ,
                VALUE,
                UNITTYPEFORVALUE,
                ISACTIVE,
                RELEASEDATE,
                QUOTA,
                UNITTYPEFORQUOTA,
                ATTAINMENT,
                UNITTYPEFORATTAINMENT,
                BUSINESSUNITMAP,
                GENERICATTRIBUTE1,
                GENERICATTRIBUTE2,
                GENERICATTRIBUTE3,
                GENERICATTRIBUTE4,
                GENERICATTRIBUTE5,
                GENERICATTRIBUTE6,
                GENERICATTRIBUTE7,
                GENERICATTRIBUTE8,
                GENERICATTRIBUTE9,
                GENERICATTRIBUTE10,
                GENERICATTRIBUTE11,
                GENERICATTRIBUTE12,
                GENERICATTRIBUTE13,
                GENERICATTRIBUTE14,
                GENERICATTRIBUTE15,
                GENERICATTRIBUTE16,
                GENERICNUMBER1,
                UNITTYPEFORGENERICNUMBER1,
                GENERICNUMBER2,
                UNITTYPEFORGENERICNUMBER2,
                GENERICNUMBER3,
                UNITTYPEFORGENERICNUMBER3,
                GENERICNUMBER4,
                UNITTYPEFORGENERICNUMBER4,
                GENERICNUMBER5,
                UNITTYPEFORGENERICNUMBER5,
                GENERICNUMBER6,
                UNITTYPEFORGENERICNUMBER6,
                GENERICDATE1,
                GENERICDATE2,
                GENERICDATE3,
                GENERICDATE4,
                GENERICDATE5,
                GENERICDATE6,
                GENERICBOOLEAN1,
                GENERICBOOLEAN2,
                GENERICBOOLEAN3,
                GENERICBOOLEAN4,
                GENERICBOOLEAN5,
                GENERICBOOLEAN6,
                PROCESSINGUNITSEQ,
                ISREPORTABLE,
                LOADDTTM
            )
            SELECT   /* ORIGSQL: (select i.TENANTID, i.INCENTIVESEQ, i.NAME, i.PAYEESEQ, i.POSITIONSEQ, i.PERIODSEQ, i.PIPELINERUNSEQ, i.PIPELINERUNDATE, i.PLANSEQ, i.RULESEQ, i.VALUE, i.UNITTYPEFORVALUE, i.ISACTIVE, i.RELEASEDATE, i(...) */
              i.TENANTID,
              i.INCENTIVESEQ,
              i.NAME,
              i.PAYEESEQ,
              i.POSITIONSEQ,
              i.PERIODSEQ,
              i.PIPELINERUNSEQ,
              i.PIPELINERUNDATE,
              i.PLANSEQ,
              i.RULESEQ,
              i.VALUE,
              i.UNITTYPEFORVALUE,
              i.ISACTIVE,
              i.RELEASEDATE,
              i.QUOTA,
              i.UNITTYPEFORQUOTA,
              i.ATTAINMENT,
              i.UNITTYPEFORATTAINMENT,
              i.BUSINESSUNITMAP,
              i.GENERICATTRIBUTE1,
              i.GENERICATTRIBUTE2,
              i.GENERICATTRIBUTE3,
              i.GENERICATTRIBUTE4,
              i.GENERICATTRIBUTE5,
              i.GENERICATTRIBUTE6,
              i.GENERICATTRIBUTE7,
              i.GENERICATTRIBUTE8,
              i.GENERICATTRIBUTE9,
              i.GENERICATTRIBUTE10,
              i.GENERICATTRIBUTE11,
              i.GENERICATTRIBUTE12,
              i.GENERICATTRIBUTE13,
              i.GENERICATTRIBUTE14,
              i.GENERICATTRIBUTE15,
              i.GENERICATTRIBUTE16,
              i.GENERICNUMBER1,
              i.UNITTYPEFORGENERICNUMBER1,
              i.GENERICNUMBER2,
              i.UNITTYPEFORGENERICNUMBER2,
              i.GENERICNUMBER3,
              i.UNITTYPEFORGENERICNUMBER3,
              i.GENERICNUMBER4,
              i.UNITTYPEFORGENERICNUMBER4,
              i.GENERICNUMBER5,
              i.UNITTYPEFORGENERICNUMBER5,
              i.GENERICNUMBER6,
              i.UNITTYPEFORGENERICNUMBER6,
              i.GENERICDATE1,
              i.GENERICDATE2,
              i.GENERICDATE3,
              i.GENERICDATE4,
              i.GENERICDATE5,
              i.GENERICDATE6,
              i.GENERICBOOLEAN1,
              i.GENERICBOOLEAN2,
              i.GENERICBOOLEAN3,
              i.GENERICBOOLEAN4,
              i.GENERICBOOLEAN5,
              i.GENERICBOOLEAN6,
              i.PROCESSINGUNITSEQ,
              i.ISREPORTABLE,
                CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            FROM
                cs_incentive i,
                cs_period per
            WHERE
              i.periodseq = per.periodseq
                AND i.processingunitseq = :vProcessingUnitSeq
                --AND per.startdate >= vCurYrStartDate
                --AND per.enddate <= vPeriodRow.enddate
                AND per.removedate = :cEndofTime
                AND (per.periodseq = :vPeriodSeq
                    OR per.shortname IN ('Mar', 'Jun', 'Sep', 'Dec'))
                AND (i.pipelinerunseq IN
                    (
                        SELECT   /* ORIGSQL: (SELECT pr.pipelineRunSeq FROM cs_pipelinerun pr, cs_stagesummary ss, cs_stagetype st WHERE pr.pipelineRunSeq = ss.pipelineRunSeq AND ss.stageTypeSeq = st.stageTypeSeq AND st.name = 'Reward' AND pr.pe(...) */
                            pr.pipelineRunSeq
                        FROM
                            cs_pipelinerun pr,
                            cs_stagesummary ss,
                            cs_stagetype st
                        WHERE
                            pr.pipelineRunSeq = ss.pipelineRunSeq
                            AND ss.stageTypeSeq = st.stageTypeSeq
                            AND st.name = 'Reward'
                            AND pr.periodSeq = :vPeriodSeq
                            AND pr.processingunitseq = :vProcessingUnitSeq
                            AND ss.isactive = 1
                    )
                )
        ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: prc_AnalyzeTable('rpt_base_incentive') */
    -- CALL prc_AnalyzeTable('rpt_base_incentive');/*Deepan : Parition not required*/

    /* ORIGSQL: prc_AnalyzeTable('rpt_base_measurement') */
    -- CALL prc_AnalyzeTable('rpt_base_measurement');/*Deepan : Parition not required*/

    /*INSERT INTO ext.rpt_base_measurement
    (SELECT m.*, SYSDATE*/
        --added by kyap, column values as there's an error on column mismatch   
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_MEASUREMENT' not found */
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_BASE_MEASUREMENT' not found */

        /* ORIGSQL: INSERT INTO ext.rpt_base_measurement(TENANTID, MEASUREMENTSEQ, NAME, PAYEESEQ, POSITIONSEQ, PERIODSEQ, PIPELINERUNSEQ, PIPELINERUNDATE, PLANSEQ, RULESEQ, VALUE, UNITTYPEFORVALUE, NUMBEROFCREDITS, (...) */
        INSERT INTO ext.rpt_base_measurement
            (
                TENANTID,
                MEASUREMENTSEQ,
                NAME,
                PAYEESEQ,
                POSITIONSEQ,
                PERIODSEQ,
                PIPELINERUNSEQ,
                PIPELINERUNDATE,
                PLANSEQ,
                RULESEQ,
                VALUE,
                UNITTYPEFORVALUE,
                NUMBEROFCREDITS,
                BUSINESSUNITMAP,
                GENERICATTRIBUTE1,
                GENERICATTRIBUTE2,
                GENERICATTRIBUTE3,
                GENERICATTRIBUTE4,
                GENERICATTRIBUTE5,
                GENERICATTRIBUTE6,
                GENERICATTRIBUTE7,
                GENERICATTRIBUTE8,
                GENERICATTRIBUTE9,
                GENERICATTRIBUTE10,
                GENERICATTRIBUTE11,
                GENERICATTRIBUTE12,
                GENERICATTRIBUTE13,
                GENERICATTRIBUTE14,
                GENERICATTRIBUTE15,
                GENERICATTRIBUTE16,
                GENERICNUMBER1,
                UNITTYPEFORGENERICNUMBER1,
                GENERICNUMBER2,
                UNITTYPEFORGENERICNUMBER2,
                GENERICNUMBER3,
                UNITTYPEFORGENERICNUMBER3,
                GENERICNUMBER4,
                UNITTYPEFORGENERICNUMBER4,
                GENERICNUMBER5,
                UNITTYPEFORGENERICNUMBER5,
                GENERICNUMBER6,
                UNITTYPEFORGENERICNUMBER6,
                GENERICDATE1,
                GENERICDATE2,
                GENERICDATE3,
                GENERICDATE4,
                GENERICDATE5,
                GENERICDATE6,
                GENERICBOOLEAN1,
                GENERICBOOLEAN2,
                GENERICBOOLEAN3,
                GENERICBOOLEAN4,
                GENERICBOOLEAN5,
                GENERICBOOLEAN6,
                PROCESSINGUNITSEQ,
                UNITTYPEFORNUMBEROFCREDITS,
                LOADDTTM
            )
            SELECT   /* ORIGSQL: (SELECT m.TENANTID, m.MEASUREMENTSEQ, m.NAME, m.PAYEESEQ, m.POSITIONSEQ, m.PERIODSEQ, m.PIPELINERUNSEQ, m.PIPELINERUNDATE, m.PLANSEQ, m.RULESEQ, m.VALUE, m.UNITTYPEFORVALUE, m.NUMBEROFCREDITS, m.BUSIN(...) */
                m.TENANTID,
                m.MEASUREMENTSEQ,
                m.NAME,
                m.PAYEESEQ,
                m.POSITIONSEQ,
                m.PERIODSEQ,
                m.PIPELINERUNSEQ,
                m.PIPELINERUNDATE,
                m.PLANSEQ,
                m.RULESEQ,
                m.VALUE,
                m.UNITTYPEFORVALUE,
                m.NUMBEROFCREDITS,
                m.BUSINESSUNITMAP,
                m.GENERICATTRIBUTE1,
                m.GENERICATTRIBUTE2,
                m.GENERICATTRIBUTE3,
                m.GENERICATTRIBUTE4,
                m.GENERICATTRIBUTE5,
                m.GENERICATTRIBUTE6,
                m.GENERICATTRIBUTE7,
                m.GENERICATTRIBUTE8,
                m.GENERICATTRIBUTE9,
                m.GENERICATTRIBUTE10,
                m.GENERICATTRIBUTE11,
                m.GENERICATTRIBUTE12,
                m.GENERICATTRIBUTE13,
                m.GENERICATTRIBUTE14,
                m.GENERICATTRIBUTE15,
                m.GENERICATTRIBUTE16,
                m.GENERICNUMBER1,
                m.UNITTYPEFORGENERICNUMBER1,
                m.GENERICNUMBER2,
                m.UNITTYPEFORGENERICNUMBER2,
                m.GENERICNUMBER3,
                m.UNITTYPEFORGENERICNUMBER3,
                m.GENERICNUMBER4,
                m.UNITTYPEFORGENERICNUMBER4,
                m.GENERICNUMBER5,
                m.UNITTYPEFORGENERICNUMBER5,
                m.GENERICNUMBER6,
                m.UNITTYPEFORGENERICNUMBER6,
                m.GENERICDATE1,
                m.GENERICDATE2,
                m.GENERICDATE3,
                m.GENERICDATE4,
                m.GENERICDATE5,
                m.GENERICDATE6,
                m.GENERICBOOLEAN1,
                m.GENERICBOOLEAN2,
                m.GENERICBOOLEAN3,
                m.GENERICBOOLEAN4,
                m.GENERICBOOLEAN5,
                m.GENERICBOOLEAN6,
                m.PROCESSINGUNITSEQ,
                m.UNITTYPEFORNUMBEROFCREDITS,
                CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            FROM
                cs_measurement m,
                cs_period per
            WHERE
                m.periodseq = per.periodseq
                AND m.processingunitseq = :vProcessingUnitSeq
                --AND per.startdate >= vCurYrStartDate
                --AND per.enddate <= vPeriodRow.enddate
                AND per.removedate = :cEndofTime
                AND (per.periodseq = :vPeriodSeq
                    OR per.shortname IN ('Mar', 'Jun', 'Sep', 'Dec'))
                AND m.pipelinerunseq IN
                (
                    SELECT   /* ORIGSQL: (SELECT pr.pipelineRunSeq FROM cs_pipelinerun pr, cs_stagesummary ss, cs_stagetype st WHERE pr.pipelineRunSeq = ss.pipelineRunSeq AND ss.stageTypeSeq = st.stageTypeSeq AND st.name IN ('Allocate', 'Rew(...) */
                        pr.pipelineRunSeq
                    FROM
                        cs_pipelinerun pr,
                        cs_stagesummary ss,
                        cs_stagetype st
                    WHERE
                        pr.pipelineRunSeq = ss.pipelineRunSeq
                        AND ss.stageTypeSeq = st.stageTypeSeq
                        AND st.name IN ('Allocate', 'Reward', 'CreateDefaultData')
                        AND pr.periodSeq = :vPeriodSeq
                        AND pr.processingunitseq = :vProcessingUnitSeq
                        AND ss.isactive = 1
                )
            ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: prc_AnalyzeTable('rpt_base_measurement') */
    -- CALL prc_AnalyzeTable('rpt_base_measurement');/*Deepan : Parition not required*/

    /* ORIGSQL: prc_AnalyzeTable('rpt_base_credit') */
    -- CALL prc_AnalyzeTable('rpt_base_credit');/*Deepan : Parition not required*/

    /*INSERT INTO ext.rpt_base_credit
    (SELECT cr.*, crt.credittypeid, SYSDATE*/
        --added by kyap, column values as there's an error on column mismatch   
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_CREDIT' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_CREDITTYPE' not found */
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_BASE_CREDIT' not found */

        /* ORIGSQL: INSERT INTO ext.rpt_base_credit(TENANTID, CREDITSEQ, PAYEESEQ, POSITIONSEQ, SALESORDERSEQ, SALESTRANSACTIONSEQ, PERIODSEQ, CREDITTYPESEQ, NAME, PIPELINERUNSEQ, ORIGINTYPEID, COMPENSATIONDATE, PIPE(...) */
        INSERT INTO ext.rpt_base_credit
            (
                TENANTID,
                CREDITSEQ,
                PAYEESEQ,
                POSITIONSEQ,
                SALESORDERSEQ,
                SALESTRANSACTIONSEQ,
                PERIODSEQ,
                CREDITTYPESEQ,
                NAME,
                PIPELINERUNSEQ,
                ORIGINTYPEID,
                COMPENSATIONDATE,
                PIPELINERUNDATE,
                BUSINESSUNITMAP,
                PREADJUSTEDVALUE,
                UNITTYPEFORPREADJUSTEDVALUE,
                VALUE,
                UNITTYPEFORVALUE,
                RELEASEDATE,
                RULESEQ,
                ISHELD,
                ISROLLABLE,
                ROLLDATE,
                REASONSEQ,
                COMMENTS,
                GENERICATTRIBUTE1,
                GENERICATTRIBUTE2,
                GENERICATTRIBUTE3,
                GENERICATTRIBUTE4,
                GENERICATTRIBUTE5,
                GENERICATTRIBUTE6,
                GENERICATTRIBUTE7,
                GENERICATTRIBUTE8,
                GENERICATTRIBUTE9,
                GENERICATTRIBUTE10,
                GENERICATTRIBUTE11,
                GENERICATTRIBUTE12,
                GENERICATTRIBUTE13,
                GENERICATTRIBUTE14,
                GENERICATTRIBUTE15,
                GENERICATTRIBUTE16,
                GENERICNUMBER1,
                UNITTYPEFORGENERICNUMBER1,
                GENERICNUMBER2,
                UNITTYPEFORGENERICNUMBER2,
                GENERICNUMBER3,
                UNITTYPEFORGENERICNUMBER3,
                GENERICNUMBER4,
                UNITTYPEFORGENERICNUMBER4,
                GENERICNUMBER5,
                UNITTYPEFORGENERICNUMBER5,
                GENERICNUMBER6,
                UNITTYPEFORGENERICNUMBER6,
                GENERICDATE1,
                GENERICDATE2,
                GENERICDATE3,
                GENERICDATE4,
                GENERICDATE5,
                GENERICDATE6,
                GENERICBOOLEAN1,
                GENERICBOOLEAN2,
                GENERICBOOLEAN3,
                GENERICBOOLEAN4,
                GENERICBOOLEAN5,
                GENERICBOOLEAN6,
                PROCESSINGUNITSEQ,
                CREDITTYPEID,
                LOADDTTM
            )
            SELECT   /* ORIGSQL: (select cr.TENANTID, cr.CREDITSEQ, cr.PAYEESEQ, cr.POSITIONSEQ, cr.SALESORDERSEQ, cr.SALESTRANSACTIONSEQ, cr.PERIODSEQ, cr.CREDITTYPESEQ, cr.NAME, cr.PIPELINERUNSEQ, cr.ORIGINTYPEID, cr.COMPENSATIONDA(...) */
                cr.TENANTID,
                cr.CREDITSEQ,
                cr.PAYEESEQ,
                cr.POSITIONSEQ,
                cr.SALESORDERSEQ,
                cr.SALESTRANSACTIONSEQ,
                cr.PERIODSEQ,
                cr.CREDITTYPESEQ,
                cr.NAME,
                cr.PIPELINERUNSEQ,
                cr.ORIGINTYPEID,
                cr.COMPENSATIONDATE,
                cr.PIPELINERUNDATE,
                cr.BUSINESSUNITMAP,
                cr.PREADJUSTEDVALUE,
                cr.UNITTYPEFORPREADJUSTEDVALUE,
                cr.VALUE,
                cr.UNITTYPEFORVALUE,
                cr.RELEASEDATE,
                cr.RULESEQ,
                cr.ISHELD,
                cr.ISROLLABLE,
                cr.ROLLDATE,
                cr.REASONSEQ,
                cr.COMMENTS,
                cr.GENERICATTRIBUTE1,
                cr.GENERICATTRIBUTE2,
                cr.GENERICATTRIBUTE3,
                cr.GENERICATTRIBUTE4,
                cr.GENERICATTRIBUTE5,
                cr.GENERICATTRIBUTE6,
                cr.GENERICATTRIBUTE7,
                cr.GENERICATTRIBUTE8,
                cr.GENERICATTRIBUTE9,
                cr.GENERICATTRIBUTE10,
                cr.GENERICATTRIBUTE11,
                cr.GENERICATTRIBUTE12,
                cr.GENERICATTRIBUTE13,
                cr.GENERICATTRIBUTE14,
                cr.GENERICATTRIBUTE15,
                cr.GENERICATTRIBUTE16,
                cr.GENERICNUMBER1,
                cr.UNITTYPEFORGENERICNUMBER1,
                cr.GENERICNUMBER2,
                cr.UNITTYPEFORGENERICNUMBER2,
                cr.GENERICNUMBER3,
                cr.UNITTYPEFORGENERICNUMBER3,
                cr.GENERICNUMBER4,
                cr.UNITTYPEFORGENERICNUMBER4,
                cr.GENERICNUMBER5,
                cr.UNITTYPEFORGENERICNUMBER5,
                cr.GENERICNUMBER6,
                cr.UNITTYPEFORGENERICNUMBER6,
                cr.GENERICDATE1,
                cr.GENERICDATE2,
                cr.GENERICDATE3,
                cr.GENERICDATE4,
                cr.GENERICDATE5,
                cr.GENERICDATE6,
                cr.GENERICBOOLEAN1,
                cr.GENERICBOOLEAN2,
                cr.GENERICBOOLEAN3,
                cr.GENERICBOOLEAN4,
                cr.GENERICBOOLEAN5,
                cr.GENERICBOOLEAN6,
                cr.PROCESSINGUNITSEQ,
                crt.CREDITTYPEID,
                CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
            FROM
                cs_credit cr,
                cs_credittype crt
            WHERE
                ((cr.periodseq = :vPeriodSeq
                        AND cr.processingunitseq = :vProcessingUnitSeq
                        AND cr.credittypeseq = crt.datatypeseq
                        AND crt.removedate = :cEndofTime
                        AND cr.isheld = 0
                        AND cr.pipelinerunseq IN
                        (
                            SELECT   /* ORIGSQL: (SELECT pr.pipelineRunSeq FROM cs_pipelinerun pr, cs_stagesummary ss, cs_stagetype st WHERE pr.pipelineRunSeq = ss.pipelineRunSeq AND ss.stageTypeSeq = st.stageTypeSeq AND st.name = 'Allocate' AND pr.(...) */
                                pr.pipelineRunSeq
                            FROM
                                cs_pipelinerun pr,
                                cs_stagesummary ss,
                                cs_stagetype st
                            WHERE
                                pr.pipelineRunSeq = ss.pipelineRunSeq
                                AND ss.stageTypeSeq = st.stageTypeSeq
                                AND st.name = 'Allocate'
                                AND pr.periodSeq = :vPeriodSeq
                                AND pr.processingunitseq = :vProcessingUnitSeq
                                AND ss.isactive = 1
                        )
                    )
                    OR (cr.releasedate = (TO_DATE(ADD_SECONDS(:vPeriodendDate,(86400*-1))))))  /* ORIGSQL: vPeriodRow.enddate-1 */
        ;
    /* or (cr.releasedate = (vPeriodRow.enddate-1) Condition
          Add on for the issue Add on up front issues because compensation date between salestransaction and credit are not matching
          However, some of credits are held so that add the condition
        */

        /* ORIGSQL: COMMIT; */
        COMMIT;

    /* ORIGSQL: prc_AnalyzeTable('rpt_base_credit') */
    -- CALL prc_AnalyzeTable('rpt_base_credit');/*Deepan : parition not required*/
END;
  PUBLIC PROCEDURE prc_driver
(
    IN vPeriodSeq BIGINT,   /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_driver.vPeriodSeq' */
                                                      /* ORIGSQL: vPeriodSeq IN BIGINT */
    IN vProcessingUnitSeq BIGINT,   /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_driver.vProcessingUnitSeq' */
                                                                      /* ORIGSQL: vProcessingUnitSeq IN BIGINT */
    IN vCalendarSeq BIGINT,   /* RESOLVE: Datatype unresolved: Datatype (BIGINT) not resolved for parameter 'prc_driver.vCalendarSeq' */
                                                          /* ORIGSQL: vCalendarSeq IN BIGINT */
    IN preportgroup VARCHAR(255) DEFAULT NULL      /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                                  /* ORIGSQL: preportgroup IN VARCHAR2 DEFAULT NULL */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /****************************************************************************************************************
        The purpose of this procedure is to create one entry point for
        running more than one report extract in the ODSReportsGenerationConfig.xml.
    
        Date        Author     Description
        ------------------------------------------------------------------------------------------------------------
    --- 30 Nov 2017 Tharanikumar  Initial release
    ****************************************************************************************************************/

  DECLARE vPeriodName VARCHAR(100);
  DECLARE vProcessingUnitSeq BIGINT;
  DECLARE vCalendarName VARCHAR(100);
  DECLARE vPeriodStartDate timestamp;
  DECLARE vPeriodEndDate timestamp;
  DECLARE vPeriodRow ROW LIKE CS_PERIOD;
    

  select * into vPeriodRow from cs_period where periodseq=:vPeriodSeq and removedate =:cEndofTime;

   /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','START-PRC-DRIVER','vPeriodSeq:'||vPeriodSeq||',vProcessingUnitSeq: ' ||vProcessingUnitSeq||', vCalendarSeq :' ||vCalendarSeq||',prep(...) */
   CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'START-PRC-DRIVER', 'vPeriodSeq:'||IFNULL(:vPeriodSeq,'')||',vProcessingUnitSeq: '||IFNULL(TO_VARCHAR(:vProcessingUnitSeq),'')||', vCalendarSeq :'||IFNULL(:vCalendarSeq,'')||',preportgroup :'||IFNULL(:preportgroup,''), NULL 
   );  /* ORIGSQL: sqlerrm */

   /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','START-prc_setVariables','Setting up variables...',NULL) */
   CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'START-prc_setVariables', 'Setting up variables...', NULL);

   /* ORIGSQL: prc_setVariables(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
   CALL prc_setVariables(:vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);/*Deepan: Uncomment this once the proc is ready*/

   SELECT SESSION_CONTEXT('GvPeriodName') INTO vPeriodName FROM SYS.DUMMY ;
   SELECT SESSION_CONTEXT('GvCalendarName') INTO vCalendarName FROM SYS.DUMMY ;
   SELECT SESSION_CONTEXT('GvProcessingUnitSeq') INTO vProcessingUnitSeq FROM SYS.DUMMY ;
   SELECT SESSION_CONTEXT('GvPeriodStartDate') INTO vPeriodStartDate FROM SYS.DUMMY ;
   SELECT SESSION_CONTEXT('GvPeriodEndDate') INTO vPeriodEndDate FROM SYS.DUMMY ;

   /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','END-prc_setVariables','Setting up variables...',NULL) */
   CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'END-prc_setVariables', 'Setting up variables...', NULL);
    
    
    --IF odsutils.istorunodsprocs (vprocessingunitseq, vperiodseq)
    --THEN
    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','START-DELETE -rpt_base_salestransaction','',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'START-DELETE -rpt_base_salestransaction', '', NULL);

    --Delete Base Tables here.

    -- prc_DeleteTable('rpt_base_salestransaction',vProcessingUnitSeq);

    --Since we're clearing the table, truncate is sufficient. There is only one PU being used.
    -- if this changes in the future, the parition name should have the PU in it, so the repsective parition can be truncated
    /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER TABLE rpt_base_salestransaction TRUNCATE PARTITION P_STEL_00001' ; */
    /* RESOLVE: Syntax not supported in target DBMS: ALTER TABLE..TRUNCATE PARTITION: convert manually */
    /* ORIGSQL: ALTER TABLE rpt_base_salestransaction TRUNCATE PARTITION P_STEL_00001 ; */
    
    /*Deepan : partitioning not required*/
    -- EXECUTE IMMEDIATE
    -- 'ALTER TABLE EXT.rpt_base_salestransaction
    --     TRUNCATE PARTITION P_STEL_00001';

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','END-DELETE -rpt_base_salestransaction','',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'END-DELETE -rpt_base_salestransaction', '', NULL);

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','START-DELETE -rpt_base_padimension','',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'START-DELETE -rpt_base_padimension', '', NULL);

    /* ORIGSQL: prc_DeleteTable('rpt_base_padimension',vProcessingUnitSeq) */
    CALL prc_DeleteTable('rpt_base_padimension', :vProcessingUnitSeq);/*Deepan: Uncomment this once the proc is ready*/

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','END-DELETE -rpt_base_padimension','',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'END-DELETE -rpt_base_padimension', '', NULL);

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','START-DELETE -rpt_base_pareporting','',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'START-DELETE -rpt_base_pareporting', '', NULL);

    /* ORIGSQL: prc_DeleteTable('rpt_base_pareporting',vProcessingUnitSeq) */
    CALL prc_DeleteTable('rpt_base_pareporting', :vProcessingUnitSeq);/*Deepan: Uncomment this once the proc is ready*/

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','END-DELETE -rpt_base_pareporting','',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'END-DELETE -rpt_base_pareporting', '', NULL);

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','START-DELETE -rpt_base_credit','',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'START-DELETE -rpt_base_credit', '', NULL);

    /* ORIGSQL: prc_DeleteTable('rpt_base_credit',vProcessingUnitSeq) */
    CALL prc_DeleteTable('rpt_base_credit', :vProcessingUnitSeq);/*Deepan: Uncomment this once the proc is ready*/

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','END-DELETE -rpt_base_credit','',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'END-DELETE -rpt_base_credit', '', NULL);

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','START-DELETE -rpt_base_measurement','',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'START-DELETE -rpt_base_measurement', '', NULL);

    /* ORIGSQL: prc_DeleteTable('rpt_base_measurement',vProcessingUnitSeq) */
    CALL prc_DeleteTable('rpt_base_measurement', :vProcessingUnitSeq);/*Deepan: Uncomment this once the proc is ready*/

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','END-DELETE -rpt_base_measurement','',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'END-DELETE -rpt_base_measurement', '', NULL);

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','START-DELETE -rpt_base_incentive','',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'START-DELETE -rpt_base_incentive', '', NULL);

    /* ORIGSQL: prc_DeleteTable('rpt_base_incentive',vProcessingUnitSeq) */
    CALL prc_DeleteTable('rpt_base_incentive', :vProcessingUnitSeq);/*Deepan: Uncomment this once the proc is ready*/

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','END-DELETE -rpt_base_incentive','',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'END-DELETE -rpt_base_incentive', '', NULL);

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','START-DELETE -rpt_base_deposit','',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'START-DELETE -rpt_base_deposit', '', NULL);

    /* ORIGSQL: prc_DeleteTable('rpt_base_deposit',vProcessingUnitSeq) */
    CALL prc_DeleteTable('rpt_base_deposit', :vProcessingUnitSeq);/*Deepan: Uncomment this once the proc is ready*/

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'PRC-DRIVER','END-DELETE -rpt_base_deposit','',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'PRC-DRIVER', 'END-DELETE -rpt_base_deposit', '', NULL);

    --prc_AddTableSubpartition('STELEXT','CS_CREDIT','TCMP','STEL',vProcessingUnitSeq,vPeriodSeq,'rpt_base_salestransaction');

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'prc_driver','START-prc_buildbasetables','Base Table Populations STARTED...',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'prc_driver', 'START-prc_buildbasetables', 'Base Table Populations STARTED...', NULL);

    /* ORIGSQL: prc_buildbasetables(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq) */
    CALL prc_buildbasetables(:vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq);/*Deepan: Uncomment this once the proc is ready*/

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'prc_driver','END-prc_buildbasetables','Base Table Populations COMPLETED...',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'prc_driver', 'END-prc_buildbasetables', 'Base Table Populations COMPLETED...', NULL);

    --END IF;

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'prc_driver','START-prc_buildreportingtables','START Reporting Table Populations...',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'prc_driver', 'START-prc_buildreportingtables', 'START Reporting Table Populations...', NULL);

    /* ORIGSQL: prc_buildreportingtables(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,preportgroup) */
    -- CALL prc_buildreportingtables(:vPeriodSeq, :vProcessingUnitSeq, :vCalendarSeq, :preportgroup);/*Deepan: Uncomment this once the proc is ready*/

    /* ORIGSQL: prc_logevent(:vPeriodRow.name,'prc_driver','END-prc_buildreportingtables','End Reporting table populations...',NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, 'prc_driver', 'END-prc_buildreportingtables', 'End Reporting table populations...', NULL);
END;
END