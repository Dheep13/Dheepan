CREATE PROCEDURE EXT.RPT_POST_EXTPMTSUMMARY_MIDMON
(
    IN p_rpttype VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                /* ORIGSQL: p_rpttype varchar2 */
    IN p_periodseq DECIMAL(38,10),   /* ORIGSQL: p_periodseq number */
    IN p_processingunitseq DECIMAL(38,10)   /* ORIGSQL: p_processingunitseq number */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    --DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
   
    DECLARE v_calendar ROW LIKE cs_calendar;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.cs_calendar' not found (for %ROWTYPE declaration) */
    DECLARE v_eot TIMESTAMP = to_date('01-jan-2200', 'dd-mon-yyyy');  /* ORIGSQL: v_eot DATE := TO_DATE('01-jan-2200', 'dd-mon-yyyy') ; */

    DECLARE v_period ROW LIKE cs_period;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.cs_period' not found (for %ROWTYPE declaration) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */

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

    /* UPDATE STEL_RPT_EXTPAYSUMMARY_MIDMON tgt
      SET Calendarname = v_calendar.name,
          dataperiodname =
             (SELECT DISTINCT name
                    FROM cs_period
               WHERE periodseq = tgt.dataperiodseq AND removedate = v_eot),
               startdate=v_period.startdate,enddate=v_period.enddate-1
    WHERE periodseq = p_periodseq;
    */   

    /* ORIGSQL: update STEL_RPT_EXTPAYSUMMARY_MIDMON tgt SET (startdate, enddate) = (SELECT x.st(...) */
    UPDATE EXT.STEL_RPT_EXTPAYSUMMARY_MIDMON tgt 
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

    /*for the period A payment*/
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_EXTPAYSUMMARY_MIDMON' not found */

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_RPT_EXTPAYSUMMARY_MIDMON tgt using (SELECT dim0, value AS cutoff(...) */
    MERGE INTO EXT.STEL_RPT_EXTPAYSUMMARY_MIDMON AS tgt

        USING
        (
            SELECT   /* ORIGSQL: (SELECT dim0, value cutoff FROM stel_lookup WHERE name = 'LT_VirtualPartners_Rat(...) */
                dim0,
                value AS cutoff
            FROM
                EXT.stel_lookup
            WHERE
                name = 'LT_VirtualPartners_Rates'
                AND dim1 = 'Mid Month Cut Off'
                AND dim2 LIKE 'Top Up Revenue%'
                AND value <> 0
        ) AS src
        ON (src.dim0 = tgt.positionname AND tgt.periodseq = :p_periodseq)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.enddate = TO_DATE(ADD_SECONDS(TO_DATE(ADD_SECONDS(tgt.startdate,(86400*(cutoff)))),(86400*-1)))   /* ORIGSQL: tgt.startdate+ cutoff */
                                                                                                                  /* ORIGSQL: TO_DATE(ADD_SECONDS(tgt.startdate,(86400*(cutoff)))) -1 */
        ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: delete from ST_EXT_PAY_SUMMARY where periodseq=p_periodseq and rpttype=p_rpttype(...) */
    DELETE
    FROM
        ST_EXT_PAY_SUMMARY
    WHERE
        periodseq = :p_periodseq
        AND rpttype = :p_rpttype;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.ST_EXT_PAY_SUMMARY' not found */

    /* ORIGSQL: INSERT INTO ST_EXT_PAY_SUMMARY (tenantid, VENDOR_NAME, AMOUNT, PERIODSEQ,PERIODN(...) */
    INSERT INTO ST_EXT_PAY_SUMMARY
        (
            tenantid, VENDOR_NAME, AMOUNT, PERIODSEQ, PERIODNAME, PAYEESEQ,
            POSITIONSEQ, PROCESSINGUNITSEQ, PROCESSINGUNITNAME, USERID, GROUPFIELD, GROUPFIELDLABEL,
            POSITIONNAME, LOADDATE, RPTTYPE, DATAPERIODSEQ, LASTNAME, DATAPERIODNAME,
            calendarname, startdate, enddate
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
            IFNULL(DATAPERIODNAME,'')|| ' A' AS DATAPERIODNAME,
            Calendarname,
            startdate,
            enddate
        FROM
            EXT.STEL_RPT_EXTPAYSUMMARY_MIDMON
        WHERE
            periodseq = :p_periodseq
            AND rpttype = :p_rpttype
            AND processingunitseq = :p_processingunitseq;

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: NULL; */
    --DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
END