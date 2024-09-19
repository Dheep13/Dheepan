CREATE PROCEDURE EXT.RPT_OVERALL_DRIVER
(
    IN vperiodseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'CS_PERIODCALENDAR.periodseq' not found (for %TYPE declaration) */
                                                      /* RESOLVE: Datatype unresolved: Datatype (CS_PERIODCALENDAR.periodseq%type) not resolved for parameter 'RPT_OVERALL_DRIVER.vperiodseq' */
                                                      /* ORIGSQL: vperiodseq IN CS_PERIODCALENDAR.periodseq%type */
    IN vprocessingunitseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'CS_PROCESSINGUNIT.processingunitseq' not found (for %TYPE declaration) */
                                                                      /* RESOLVE: Datatype unresolved: Datatype (CS_PROCESSINGUNIT.processingunitseq%type) not resolved for parameter 'RPT_OVERALL_DRIVER.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN CS_PROCESSINGUNIT.processingunitseq%type */
    IN vcalendarseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'CS_PERIODCALENDAR.calendarseq' not found (for %TYPE declaration) */
                                               /* RESOLVE: Datatype unresolved: Datatype (CS_PERIODCALENDAR.calendarseq%type) not resolved for parameter 'RPT_OVERALL_DRIVER.vcalendarseq' */
                                                          /* ORIGSQL: vcalendarseq IN CS_PERIODCALENDAR.calendarseq%type */
    IN vtenantid VARCHAR(10)     /* RESOLVE: Identifier not found: Table/Column 'CS_PERIODCALENDAR.tenantid' not found (for %TYPE declaration) */
                                                      /* RESOLVE: Datatype unresolved: Datatype (CS_PERIODCALENDAR.tenantid%type) not resolved for parameter 'RPT_OVERALL_DRIVER.vtenantid' */
                                                      /* ORIGSQL: vtenantid IN CS_PERIODCALENDAR.tenantid%type */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE vprocname VARCHAR(50) = 'RPT_OVERALL_DRIVER';  /* ORIGSQL: vprocname VARCHAR2(50) := 'RPT_OVERALL_DRIVER'; */

    /* ORIGSQL: VARCHAR2(50 BYTE) := 'RPT_OVERALL_DRIVER'; */
    DECLARE v_Sql VARCHAR(30000);  /* ORIGSQL: v_Sql VARCHAR2(30000); */
    DECLARE v_reportlist VARCHAR(30000);  /* ORIGSQL: v_reportlist VARCHAR2(30000); */
    DECLARE vStartIdx DECIMAL(38,10);  /* ORIGSQL: vStartIdx NUMBER; */
    DECLARE vEndIdx DECIMAL(38,10);  /* ORIGSQL: vEndIdx NUMBER; */
    DECLARE vcurValue VARCHAR(5000);  /* ORIGSQL: vcurValue varchar2(5000); */
    DECLARE vLoop DECIMAL(38,10) = 0;  /* ORIGSQL: vLoop number:=0; */
    DECLARE vFlag DECIMAL(38,10) = 0;  /* ORIGSQL: vFlag NUMBER:=0; */
    DECLARE vPeriodRow ROW LIKE CS_PERIOD;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_PERIOD' not found (for %ROWTYPE declaration) */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: when others then */
        BEGIN
            /* ORIGSQL: EXT.prc_logevent (vPeriodRow.name,vProcName,'ERROR',NULL,SQLCODE ||' '||SQLE(...) */
            CALL ext.prc_logevent(:vPeriodRow.name, :vprocname, 'ERROR', NULL, ::SQL_ERROR_CODE ||' '||::SQL_ERROR_MESSAGE);  /* ORIGSQL: SQLERRM */

            /* ORIGSQL: raise_application_error(-20911, vprocname || ' failed: ' || SQLERRM) */
            -- sapdbmtk: mapped error code -20911 => 10911: (ABS(-20911)%10000)+10000
            SIGNAL SQL_ERROR_CODE 10911 SET MESSAGE_TEXT = IFNULL(:vprocname,'') || ' failed: ' || ::SQL_ERROR_MESSAGE;  /* ORIGSQL: SQLERRM */
        END;

        /* ORIGSQL: EXT.prc_logevent (vPeriodRow.name,vprocname,'Start-RPT_OVERALL_DRIVER','vPer(...) */
        CALL ext.prc_logevent(:vPeriodRow.name, :vprocname, 'Start-RPT_OVERALL_DRIVER', 'vPeriodSeq:'||IFNULL(:vperiodseq,'')||',vProcessingUnitSeq: '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||', vCalendarSeq :'||IFNULL(:vcalendarseq,'')||',vtenantid :'||IFNULL(:vtenantid,''), NULL);  /* RESOLVE: Identifier not found: Cannot resolve procedure call 'stelext.prc_logevent' */
                                                                                                                                                                                                                                                                                                                   /* ORIGSQL: sqlerrm */

        SELECT
            per.*
        INTO
            vPeriodRow
        FROM
            cs_period per
        WHERE
            per.removedate = '01-JAN-2200'
            AND per.periodseq = :vperiodseq;

        /* ORIGSQL: EXT.prc_logevent (vPeriodRow.name,vprocname,'Start-RPT_DRIVER_PROCEDURE','vP(...) */
        CALL EXT.prc_logevent(:vPeriodRow.name, :vprocname, 'Start-RPT_DRIVER_PROCEDURE', 'vPeriodSeq:'||IFNULL(:vperiodseq,'')||',vProcessingUnitSeq: '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||', vCalendarSeq :'||IFNULL(:vcalendarseq,'')||',vtenantid :'||IFNULL(:vtenantid,''), NULL);  /* ORIGSQL: sqlerrm */ /*Deepan: Error code not required here*/

        /* ORIGSQL: EXT.RPT_DRIVER_PROCEDURE(vperiodseq,vprocessingunitseq,vcalendarseq,vtenanti(...) */
        CALL EXT.RPT_DRIVER_PROCEDURE(:vperiodseq, :vprocessingunitseq, :vcalendarseq, :vtenantid);  /* RESOLVE: Identifier not found: Cannot resolve procedure call 'STELEXT.RPT_DRIVER_PROCEDURE' */

        /* ORIGSQL: EXT.prc_logevent (vPeriodRow.name,vprocname,'End-RPT_DRIVER_PROCEDURE','vPer(...) */
        CALL EXT.prc_logevent(:vPeriodRow.name, :vprocname, 'End-RPT_DRIVER_PROCEDURE', 'vPeriodSeq:'||IFNULL(:vperiodseq,'')||',vProcessingUnitSeq: '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||', vCalendarSeq :'||IFNULL(:vcalendarseq,'')||',vtenantid :'||IFNULL(:vtenantid,''), NULL);  /* ORIGSQL: sqlerrm */ /*Deepan: Error code not required here*/

        /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_RPT_RUNNINGREPORTS' not found */

        SELECT
            COUNT(*)
        INTO
            vFlag
        FROM
            EXT.STEL_RPT_RUNNINGREPORTS
        WHERE
            caldreportname NOT IN ('25-TeleSales - Individual Payment Summary','26-TeleSales-Payment Summary Report');

        --reports that don't use the base tables can be put in here.

        IF :vFlag = 0
        THEN
            /* ORIGSQL: EXT.prc_logevent (vPeriodRow.name,vprocname,'RPT_OVERALL_DRIVER-No need to t(...) */
            CALL EXT.prc_logevent(:vPeriodRow.name, :vprocname, 'RPT_OVERALL_DRIVER-No need to trigger PKG', 'vPeriodSeq:'||IFNULL(:vperiodseq,'')||',vProcessingUnitSeq: '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||', vCalendarSeq :'||IFNULL(:vcalendarseq,'')||',vtenantid :'||IFNULL(:vtenantid,''), NULL);  /* ORIGSQL: sqlerrm */ /*Deepan: Error code not required here*/

            /* ORIGSQL: EXT.prc_logevent (vPeriodRow.name,vprocname,'End-RPT_OVERALL_DRIVER','vPerio(...) */
            CALL EXT.prc_logevent(:vPeriodRow.name, :vprocname, 'End-RPT_OVERALL_DRIVER', 'vPeriodSeq:'||IFNULL(:vperiodseq,'')||',vProcessingUnitSeq: '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||', vCalendarSeq :'||IFNULL(:vcalendarseq,'')||',vtenantid :'||IFNULL(:vtenantid,''), NULL);  /* ORIGSQL: sqlerrm */

            RETURN;
        END IF;
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vprocname,'Start-PKG_REPORTING_EXTRACT_R2.PRC_DRIV(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vprocname, 'Start-PKG_REPORTING_EXTRACT_R2.PRC_DRIVER', 'vPeriodSeq:'||IFNULL(:vperiodseq,'')||',vProcessingUnitSeq: '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||', vCalendarSeq :'||IFNULL(:vcalendarseq,'')||',vtenantid :'||IFNULL(:vtenantid,''), NULL
        );  /* ORIGSQL: sqlerrm */

        /* ORIGSQL: EXT.PKG_REPORTING_EXTRACT_R2.PRC_DRIVER(vperiodseq,vprocessingunitseq,vcalen(...) */
        CALL EXT.PKG_REPORTING_EXTRACT_R2:PRC_DRIVER(:vperiodseq, :vprocessingunitseq, :vcalendarseq);  /* RESOLVE: Identifier not found: Cannot resolve procedure call 'STELEXT.PKG_REPORTING_EXTRACT_R2.PRC_DRIVER' */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vprocname,'End-PKG_REPORTING_EXTRACT_R2.PRC_DRIVER(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vprocname, 'End-PKG_REPORTING_EXTRACT_R2.PRC_DRIVER', 'vPeriodSeq:'||IFNULL(:vperiodseq,'')||',vProcessingUnitSeq: '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||', vCalendarSeq :'||IFNULL(:vcalendarseq,'')||',vtenantid :'||IFNULL(:vtenantid,''), NULL
        );  /* ORIGSQL: sqlerrm *//*Deepan: Error code not required here*/

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: EXT.prc_logevent (vPeriodRow.name,vprocname,'End-RPT_OVERALL_DRIVER','vPerio(...) */
        CALL EXT.prc_logevent(:vPeriodRow.name, :vprocname, 'End-RPT_OVERALL_DRIVER', 'vPeriodSeq:'||IFNULL(:vperiodseq,'')||',vProcessingUnitSeq: '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||', vCalendarSeq :'||IFNULL(:vcalendarseq,'')||',vtenantid :'||IFNULL(:vtenantid,''), NULL);  /* ORIGSQL: sqlerrm *//*Deepan: Error code not required here*/

        --stelext.prc_logevent (vPeriodRow.name,vprocname,'Report Procedures Completed',NULL,NULL);

        /* ORIGSQL: exception when others then */
END