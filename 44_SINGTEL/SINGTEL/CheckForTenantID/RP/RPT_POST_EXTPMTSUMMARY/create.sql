CREATE PROCEDURE EXT.RPT_POST_EXTPMTSUMMARY
(
    IN p_rpttype VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                /* ORIGSQL: p_rpttype VARCHAR2 */
    IN p_periodseq DECIMAL(38,10),   /* ORIGSQL: p_periodseq NUMBER */
    IN p_processingunitseq DECIMAL(38,10)   /* ORIGSQL: p_processingunitseq NUMBER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    --DECLARE v_calendar cs_calendar%ROWTYPE;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.cs_calendar' not found (for %ROWTYPE declaration) */
    DECLARE v_calendar ROW LIKE cs_calendar;
    DECLARE v_eot TIMESTAMP = to_date('01-jan-2200', 'dd-mon-yyyy');  /* ORIGSQL: v_eot DATE := TO_DATE('01-jan-2200', 'dd-mon-yyyy') ; */

    --v_period     cs_period%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.cs_period' not found (for %ROWTYPE declaration) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */
    Declare v_period ROW LIKE cs_period;

    SELECT *
    INTO
        v_period
    FROM
        cs_period
    WHERE
        periodseq = :p_periodseq
        AND removedate = :v_eot;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CALENDAR' not found */

    SELECT *
    INTO
        v_calendar
    FROM
        cs_calendar
    WHERE
        Calendarseq = :v_period.calendarseq
        AND removedate = :v_eot;

    /*  UPDATE STEL_RPT_DATA_EXTPAYSUMMARY tgt
        SET Calendarname = v_calendar.name,
            dataperiodname =
               (SELECT DISTINCT name
                      FROM cs_period
                 WHERE periodseq = tgt.dataperiodseq AND removedate = v_eot),
                 --startdate=v_period.startdate,enddate=v_period.enddate-1
      WHERE periodseq = p_periodseq;
    */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_DATA_EXTPAYSUMMARY' not found */
    /* ORIGSQL: update STEL_RPT_DATA_EXTPAYSUMMARY tgt SET (startdate, enddate) = (SELECT x.star(...) */
    UPDATE EXT.STEL_RPT_DATA_EXTPAYSUMMARY tgt 
        SET
        /* ORIGSQL: (startdate, enddate) = */
        (startdate, enddate) = (
            SELECT   /* ORIGSQL: (select x.startdate, x.enddate-1 from cs_period x where x.periodseq=tgt.payperio(...) */
                x.startdate,
                TO_DATE(ADD_SECONDS(x.enddate,(86400*-1)))   /* ORIGSQL: x.enddate-1 */
            FROM
                cs_period x
            WHERE
                x.periodseq = tgt.payperiodseq
                AND x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
        )
    WHERE
        periodseq = :p_periodseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;  

    /* ORIGSQL: update STEL_RPT_DATA_EXTPAYSUMMARY tgt SET (startdate, enddate) = (SELECT x.star(...) */
    UPDATE EXT.STEL_RPT_DATA_EXTPAYSUMMARY tgt 
        SET
        /* ORIGSQL: (startdate, enddate) = */
        (startdate, enddate) = (
            SELECT   /* ORIGSQL: (select x.startdate, x.enddate-1 from cs_period x where x.periodseq=tgt.dataperi(...) */
                x.startdate,
                TO_DATE(ADD_SECONDS(x.enddate,(86400*-1)))   /* ORIGSQL: x.enddate-1 */
            FROM
                cs_period x
            WHERE
                x.periodseq = tgt.dataperiodseq
                AND x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
        )
    FROM
        EXT.STEL_RPT_DATA_EXTPAYSUMMARY tgt
    WHERE
        periodseq = :p_periodseq
        AND (groupfield LIKE '%MMP%'
            OR UPPER(REPLACE(groupfield,CHAR(32),'')) LIKE '%ADJUSTMENT%FIXEDLINEVAS%');  /* ORIGSQL: chr(32) */

    --added by kyap, fixedline VAS adjustment is monthly report

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: delete from ST_EXT_PAY_SUMMARY where periodseq=p_periodseq and rpttype=p_rpttype(...) */
    DELETE
    FROM
        EXT.ST_EXT_PAY_SUMMARY
    WHERE
        periodseq = :p_periodseq
        AND rpttype = :p_rpttype;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.ST_EXT_PAY_SUMMARY' not found */

    /* ORIGSQL: INSERT INTO ST_EXT_PAY_SUMMARY (tenantid, VENDOR_NAME, AMOUNT, PERIODSEQ,PERIODN(...) */
    INSERT INTO EXT.ST_EXT_PAY_SUMMARY
        (
            tenantid, VENDOR_NAME, AMOUNT, PERIODSEQ, PERIODNAME, PAYEESEQ,
            POSITIONSEQ, PROCESSINGUNITSEQ, PROCESSINGUNITNAME, USERID, GROUPFIELD, GROUPFIELDLABEL,
            POSITIONNAME, LOADDATE, RPTTYPE, DATAPERIODSEQ, LASTNAME, DATAPERIODNAME,
            Calendarname, startdate, enddate
        )
        SELECT   /* ORIGSQL: SELECT TENANTID, VENDOR_NAME, AMOUNT, PERIODSEQ, PERIODNAME PERIODNAME, PAYEESEQ(...) */
            TENANTID,
            VENDOR_NAME,
            AMOUNT,
            PERIODSEQ,
            PERIODNAME AS PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            USERID,
            GROUPFIELD,
            GROUPFIELDLABEL,
            POSITIONNAME,
            LOADDATE,
            RPTTYPE,
            DATAPERIODSEQ,
            LASTNAME,
            DATAPERIODNAME AS DATAPERIODNAME,
            Calendarname,
            startdate,
            enddate
        FROM
            EXT.STEL_RPT_DATA_EXTPAYSUMMARY
        WHERE
            periodseq = :p_periodseq
            AND rpttype = :p_rpttype
            AND processingunitseq = :p_processingunitseq;

    /* ORIGSQL: commit; */
    COMMIT;

    -- Handling for DEL Adjustments
    /* UPDATE ST_EXT_PAY_SUMMARY tgt
    set groupfieldlabel2 = (select outputlabel2 from stel_rpt_cfg_lookup
    where rpttype=p_rpttype and inputname = tgt.groupfield)
     WHERE periodseq = p_periodseq; */
    --bugfix by kyap, additional join by rpttype    
    /* ORIGSQL: UPDATE ST_EXT_PAY_SUMMARY tgt SET groupfieldlabel2 = (SELECT outputlabel2 FROM s(...) */
    UPDATE EXT.ST_EXT_PAY_SUMMARY tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_CFG_LOOKUP' not found */
        SET
        /* ORIGSQL: groupfieldlabel2 = */
        groupfieldlabel2 = (
            SELECT   /* ORIGSQL: (select outputlabel2 from stel_rpt_cfg_lookup src where src.inputname = tgt.grou(...) */
                outputlabel2
            FROM
                ext.stel_rpt_cfg_lookup src
            WHERE
                src.inputname = tgt.groupfield
                AND tgt.rpttype = src.rpttype
        )
    WHERE
        periodseq = :p_periodseq
        AND tgt.rpttype = :p_rpttype;

    /* ORIGSQL: commit; */
    COMMIT;
END