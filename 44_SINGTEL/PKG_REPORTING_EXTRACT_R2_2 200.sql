

CREATE LIBRARY "EXT"."PKG_REPORTING_EXTRACT_R2_2" LANGUAGE SQLSCRIPT AS
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
    /****************************************************************************************************************
        The purpose of this procedure is to truncate the supplied table.
    
        Date        Author     Description
        ------------------------------------------------------------------------------------------------------------
    --- 30 Nov 2017 Tharanikumar  Initial release
    ****************************************************************************************************************/
    /* ORIGSQL: prc_logevent(:vPeriodName,'prc_DeleteTable','Delete started',''||pTableName,NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodName, 'prc_DeleteTable', 'Delete started', ''||IFNULL(:pTableName,''), NULL);

    /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
    /* ORIGSQL: EXECUTE IMMEDIATE 'Delete from '||pTableName||' where processingunitseq = '||pProcessingUnitSeq; */
    EXECUTE IMMEDIATE 'Delete from '||IFNULL(:pTableName,'')||' where processingunitseq = '||IFNULL(TO_VARCHAR(:pProcessingUnitSeq),'');
    /* ORIGSQL: prc_logevent(:vPeriodName,'prc_DeleteTable','Delete End',''||pTableName,NULL) */
    CALL EXT.PRC_LOGEVENT(:vPeriodName, 'prc_DeleteTable', 'Delete End', ''||IFNULL(:pTableName,''), NULL);

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --prc_logevent(:vPeriodName,'prc_DeleteTable','START','Base Table Delete '||pTableName,NULL);
END;
  PUBLIC PROCEDURE prc_TruncateTablePartition
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
END