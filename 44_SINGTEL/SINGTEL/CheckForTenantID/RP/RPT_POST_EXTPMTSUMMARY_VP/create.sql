CREATE PROCEDURE EXT.RPT_POST_EXTPMTSUMMARY_VP
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

    --for adjustments
    -- the text 'Adjustment -' is set up in the cfg_lookup   

    /* ORIGSQL: UPDATE STEL_RPT_DATA_EXTPAYSUMMARY_VP tgt SET groupfieldlabel2 = (SELECT outputl(...) */
    UPDATE EXT.STEL_RPT_DATA_EXTPAYSUMMARY_VP tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_CFG_LOOKUP' not found */
        SET
        /* ORIGSQL: groupfieldlabel2 = */
        groupfieldlabel2 = (
            SELECT   /* ORIGSQL: (select outputlabel2 from stel_rpt_cfg_lookup where rpttype=p_rpttype and inputn(...) */
                outputlabel2
            FROM
                EXT.stel_rpt_cfg_lookup
            WHERE
                rpttype = :p_rpttype
                AND inputname = tgt.groupfield
        )
    WHERE
        periodseq = :p_periodseq;

    /*UPDATE STEL_RPT_DATA_EXTPAYSUMMARY_VP tgt
    SET Calendarname = v_calendar.name,
        dataperiodname =
           (SELECT DISTINCT name
                  FROM cs_period
             WHERE periodseq = tgt.dataperiodseq AND removedate = v_eot),
             startdate=v_period.startdate,enddate=v_period.enddate-1
     WHERE periodseq = p_periodseq;
     */   

    /* ORIGSQL: update STEL_RPT_DATA_EXTPAYSUMMARY_VP tgt SET (startdate, enddate) = (SELECT x.s(...) */
    UPDATE EXT.STEL_RPT_DATA_EXTPAYSUMMARY_VP tgt 
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

    --for adjustments that for quarterly components that are entered in the 3rd month of the qtr,
    --we set start date to 1st mth of qtr and
    --payperiodseq to qtr
    --mcash and dereg

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_DATA_EXTPAYSUMMARY_VP' not found */

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_RPT_DATA_EXTPAYSUMMARY_VP tgt using (SELECT periodseq, startdate(...) */
    MERGE INTO EXT.STEL_RPT_DATA_EXTPAYSUMMARY_VP AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_PERIODHIERARCHY_TBL' not found */
        USING
        (
            SELECT   /* ORIGSQL: (select periodseq, startdate, enddate, monthperiodseq from stel_periodhierarchy_(...) */
                periodseq,
                startdate,
                enddate,
                monthperiodseq
            FROM
                EXT.stel_periodhierarchy_tbl
            WHERE
                periodtypename = 'quarter'
                AND monthperiodseq = :p_periodseq
                AND monthenddate = enddate
        ) AS src
        ON (src.monthperiodseq = tgt.periodseq AND tgt.periodseq = :p_periodseq AND tgt.groupfield IN ('D_Dereg Adjustment','D_mCash Adjustment'))
    WHEN MATCHED THEN
        UPDATE SET payperiodseq = src.periodseq,startdate = src.startdate, enddate = src.enddate;  

    /* ORIGSQL: update STEL_RPT_DATA_EXTPAYSUMMARY_VP tgt SET (startdate, enddate) = (SELECT x.s(...) */
    UPDATE EXT.STEL_RPT_DATA_EXTPAYSUMMARY_VP tgt 
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

    /*for the period B payment*/ 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_RPT_DATA_EXTPAYSUMMARY_VP tgt using (SELECT dim0, value AS cutof(...) */
    MERGE INTO EXT.STEL_RPT_DATA_EXTPAYSUMMARY_VP AS tgt

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
            tgt.startdate = TO_DATE(ADD_SECONDS(tgt.startdate,(86400*(cutoff))))   /* ORIGSQL: tgt.startdate+ cutoff */
        ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: DELETE FROM st_ext_pay_summary WHERE periodseq = p_periodseq AND rpttype = p_rpt(...) */
    DELETE
    FROM
        EXT.st_ext_pay_summary
    WHERE
        periodseq = :p_periodseq
        AND rpttype = :p_rpttype;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.ST_EXT_PAY_SUMMARY' not found */

    /* ORIGSQL: INSERT INTO ST_EXT_PAY_SUMMARY (tenantid, VENDOR_NAME, AMOUNT, PERIODSEQ, PERIOD(...) */
    INSERT INTO EXT.ST_EXT_PAY_SUMMARY
        (
            tenantid, VENDOR_NAME, AMOUNT, PERIODSEQ, PERIODNAME, PAYEESEQ,
            POSITIONSEQ, PROCESSINGUNITSEQ, PROCESSINGUNITNAME, USERID, GROUPFIELD, GROUPFIELDLABEL,
            POSITIONNAME, LOADDATE, RPTTYPE, DATAPERIODSEQ, LASTNAME, DATAPERIODNAME,
            Calendarname, startdate, enddate, groupfieldlabel2
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
            DATAPERIODNAME,
            Calendarname,
            startdate,
            enddate,
            groupfieldlabel2
        FROM
            EXT.STEL_RPT_DATA_EXTPAYSUMMARY_VP
        WHERE
            periodseq = :p_periodseq
            AND rpttype = :p_rpttype
            AND processingunitseq = :p_processingunitseq
            AND positionname NOT IN
            (
                SELECT   /* ORIGSQL: (SELECT dim0 FROM stel_lookup WHERE name = 'LT_VirtualPartners_Rates' AND dim1 =(...) */
                    dim0
                FROM
                    EXT.stel_lookup
                WHERE
                    name = 'LT_VirtualPartners_Rates'
                    AND dim1 = 'Mid Month Cut Off'
                    AND dim2 LIKE 'Top Up Revenue%'
                    AND value <> 0
            );

    /* ORIGSQL: INSERT INTO ST_EXT_PAY_SUMMARY (tenantid, VENDOR_NAME, AMOUNT, PERIODSEQ, PERIOD(...) */
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
            IFNULL(DATAPERIODNAME,'') || ' B' AS DATAPERIODNAME,
            Calendarname,
            startdate,
            enddate
        FROM
            EXT.STEL_RPT_DATA_EXTPAYSUMMARY_VP
        WHERE
            periodseq = :p_periodseq
            AND rpttype = :p_rpttype
            AND processingunitseq = :p_processingunitseq
            AND positionname IN
            (
                SELECT   /* ORIGSQL: (SELECT dim0 FROM stel_lookup WHERE name = 'LT_VirtualPartners_Rates' AND dim1 =(...) */
                    dim0
                FROM
                    EXT.stel_lookup
                WHERE
                    name = 'LT_VirtualPartners_Rates'
                    AND dim1 = 'Mid Month Cut Off'
                    AND dim2 LIKE 'Top Up Revenue%'
                    AND value <> 0
            );

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: NULL; */
    --DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
END