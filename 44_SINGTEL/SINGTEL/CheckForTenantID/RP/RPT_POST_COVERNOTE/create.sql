CREATE PROCEDURE EXT.RPT_POST_COVERNOTE
(
    IN RPTTYPE VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                              /* ORIGSQL: RPTTYPE IN VARCHAR2 */
    IN in_PERIODSEQ DECIMAL(38,10),   /* ORIGSQL: in_PERIODSEQ IN NUMBER */
    IN in_PROCESSINGUNITSEQ DECIMAL(38,10)   /* ORIGSQL: in_PROCESSINGUNITSEQ IN NUMBER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* ORIGSQL: EXECUTE IMMEDIATE 'Truncate table EXT.STEL_CLASSIFIER_TAB'; */
    /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_CLASSIFIER_TAB' not found */

    /* ORIGSQL: Truncate table EXT.STEL_CLASSIFIER_TAB ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.STEL_CLASSIFIER_TAB';

    /* ORIGSQL: INSERT INTO EXT.STEL_CLASSIFIER_TAB (CATEGORYNAME, CATEGORYTREENAME, CLASSFIERNAME, (...) */
    INSERT INTO EXT.STEL_CLASSIFIER_TAB
        (
            CATEGORYNAME,
            CATEGORYTREENAME,
            CLASSFIERNAME,
            CLASSIFIERID,
            COST,
            DESCRIPTION,
            EFFECTIVEENDDATE,
            EFFECTIVESTARTDATE,
            GENERICATTRIBUTE1,
            GENERICATTRIBUTE10,
            GENERICATTRIBUTE11,
            GENERICATTRIBUTE12,
            GENERICATTRIBUTE13,
            GENERICATTRIBUTE14,
            GENERICATTRIBUTE15,
            GENERICATTRIBUTE16,
            GENERICATTRIBUTE2,
            GENERICATTRIBUTE3,
            GENERICATTRIBUTE4,
            GENERICATTRIBUTE5,
            GENERICATTRIBUTE6,
            GENERICATTRIBUTE7,
            GENERICATTRIBUTE8,
            GENERICATTRIBUTE9,
            GENERICBOOLEAN1,
            GENERICBOOLEAN2,
            GENERICBOOLEAN3,
            GENERICBOOLEAN4,
            GENERICBOOLEAN5,
            GENERICBOOLEAN6,
            GENERICDATE1,
            GENERICDATE2,
            GENERICDATE3,
            GENERICDATE4,
            GENERICDATE5,
            GENERICDATE6,
            GENERICNUMBER1,
            GENERICNUMBER2,
            GENERICNUMBER3,
            GENERICNUMBER4,
            GENERICNUMBER5,
            GENERICNUMBER6,
            PRICE
        )
        SELECT   /* ORIGSQL: SELECT CATEGORYNAME, CATEGORYTREENAME, CLASSFIERNAME, CLASSIFIERID, COST, DESCRI(...) */
            CATEGORYNAME,
            CATEGORYTREENAME,
            CLASSFIERNAME,
            CLASSIFIERID,
            COST,
            DESCRIPTION,
            EFFECTIVEENDDATE,
            EFFECTIVESTARTDATE,
            GENERICATTRIBUTE1,
            GENERICATTRIBUTE10,
            GENERICATTRIBUTE11,
            GENERICATTRIBUTE12,
            GENERICATTRIBUTE13,
            GENERICATTRIBUTE14,
            GENERICATTRIBUTE15,
            GENERICATTRIBUTE16,
            GENERICATTRIBUTE2,
            GENERICATTRIBUTE3,
            GENERICATTRIBUTE4,
            GENERICATTRIBUTE5,
            GENERICATTRIBUTE6,
            GENERICATTRIBUTE7,
            GENERICATTRIBUTE8,
            GENERICATTRIBUTE9,
            GENERICBOOLEAN1,
            GENERICBOOLEAN2,
            GENERICBOOLEAN3,
            GENERICBOOLEAN4,
            GENERICBOOLEAN5,
            GENERICBOOLEAN6,
            GENERICDATE1,
            GENERICDATE2,
            GENERICDATE3,
            GENERICDATE4,
            GENERICDATE5,
            GENERICDATE6,
            GENERICNUMBER1,
            GENERICNUMBER2,
            GENERICNUMBER3,
            GENERICNUMBER4,
            GENERICNUMBER5,
            GENERICNUMBER6,
            PRICE
        FROM
            EXT.STEL_CLASSIFIER;

    /* ORIGSQL: DELETE FROM ST_EXT_PAY_SUMMARY WHERE periodseq = in_periodseq AND rpttype = 'PGC(...) */
    DELETE
    FROM
        ST_EXT_PAY_SUMMARY
    WHERE
        periodseq = :in_PERIODSEQ
        AND RPTTYPE = 'PGCOVERNOTE';

    /* RESOLVE: Identifier not found: Table/view 'EXT.ST_EXT_PAY_SUMMARY' not found */

    /* ORIGSQL: INSERT INTO ST_EXT_PAY_SUMMARY (TENANTID, VENDOR_NAME, AMOUNT, PERIODSEQ, PERIOD(...) */
    /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_RPT_PICK_GO_PAYSUMMRY' not found */
    INSERT INTO ST_EXT_PAY_SUMMARY
        (
            TENANTID,
            VENDOR_NAME,
            AMOUNT,
            PERIODSEQ,
            PERIODNAME,
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
            CALENDARNAME,
            STARTDATE,
            ENDDATE
        )
        SELECT   /* ORIGSQL: SELECT 'STEL', AGENT, PAYOUT, a.PERIODSEQ, PERIODNAME, PAYEESEQ, POSITIONSEQ, PR(...) */
            'STEL',
            AGENT,
            /*,AMOUNT,PERIODSEQ,PERIODNAME,PAYEESEQ,POSITIONSEQ,PROCESSINGUNITSEQ
            ,PROCESSINGUNITNAME,USERID,GROUPFIELD,GROUPFIELDLABEL,POSITIONNAME
            ,LOADDATE,RPTTYPE,DATAPERIODSEQ,LASTNAME,DATAPERIODNAME,CALENDARNAME
            ,STARTDATE,ENDDATE*/
            PAYOUT,
            a.PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            DEALERCODE,
            CASE
                WHEN PRODUCTGROUP = 'MUSIC'
                THEN 'Pick & go music'
                WHEN PRODUCTGROUP LIKE 'SIM%'
                THEN 'Pick & go SIM Only'
                WHEN PRODUCTGROUP = 'DASH'
                THEN 'Pick & go dash'
                WHEN PRODUCTGROUP = 'PREPAID TOP UP'
                THEN 'Pick & go Prepaid Top Up'
                ELSE PRODUCTGROUP
            END,
            CASE
                WHEN PRODUCTGROUP = 'MUSIC'
                THEN 'Pick & go music'
                WHEN PRODUCTGROUP LIKE 'SIM%'
                THEN 'Pick & go SIM Only'
                WHEN PRODUCTGROUP = 'DASH'
                THEN 'Pick & go dash'
                WHEN PRODUCTGROUP = 'PREPAID TOP UP'
                THEN 'Pick & go Prepaid Top Up'
                ELSE PRODUCTGROUP
            END,
            DEALERCODE,
            CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
            'PGCOVERNOTE',
            a.PERIODSEQ,
            AGENT,
            PERIODNAME,
            CALENDARNAME,
            pd.startdate,
            TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1)))   /* ORIGSQL: pd.enddate-1 */
        FROM
            EXT.STEL_RPT_PICK_GO_PAYSUMMRY a
        INNER JOIN
            cs_period pd
            ON pd.periodseq = a.periodseq
            AND pd.removedate = TO_DATE('22000101', 'YYYYMMDD')
            AND a.dataperiodseq = :in_PERIODSEQ;

    /* ORIGSQL: DELETE FROM ST_EXT_PAY_SUMMARY WHERE periodseq = in_periodseq AND rpttype = 'EXT(...) */
    DELETE
    FROM
        ST_EXT_PAY_SUMMARY
    WHERE
        periodseq = :in_PERIODSEQ
        AND RPTTYPE = 'EXTPPCOVERNOTE';

    /* ORIGSQL: INSERT INTO ST_EXT_PAY_SUMMARY (TENANTID, VENDOR_NAME, AMOUNT, PERIODSEQ, PERIOD(...) */
    /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_RPT_EXTPREPAIDPAYSUMM' not found */
    INSERT INTO ST_EXT_PAY_SUMMARY
        (
            TENANTID,
            VENDOR_NAME,
            AMOUNT,
            PERIODSEQ,
            PERIODNAME,
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
            CALENDARNAME,
            STARTDATE,
            ENDDATE
        )
        SELECT   /* ORIGSQL: SELECT 'STEL', VENDORNAME, SUM(COMMISSION), a.PERIODSEQ, PERIODNAME, PAYEESEQ, P(...) */
            'STEL',
            VENDORNAME,
            /*,AMOUNT,PERIODSEQ,PERIODNAME,PAYEESEQ,POSITIONSEQ,PROCESSINGUNITSEQ
            ,PROCESSINGUNITNAME,USERID,GROUPFIELD,GROUPFIELDLABEL,POSITIONNAME
            ,LOADDATE,RPTTYPE,DATAPERIODSEQ,LASTNAME,DATAPERIODNAME,CALENDARNAME
            ,STARTDATE,ENDDATE*/
            SUM(COMMISSION),
            a.PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            VENDORCODE,
            'Prepaid' AS PRODUCTGROUP,
            'Prepaid' AS PRODUCTGROUP,
            VENDORCODE,
            CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
            'EXTPPCOVERNOTE',
            a.PERIODSEQ,
            VENDORNAME,
            PERIODNAME,
            CALENDARNAME,
            ext.trunc(pd.startdate,'QUARTER'),  /* ORIGSQL: trunc(pd.startdate,'Q') */--Deepan:Get first date of quarter
            TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1)))   /* ORIGSQL: pd.enddate-1 */
        FROM
            EXT.STEL_RPT_EXTPREPAIDPAYSUMM a
        INNER JOIN
            cs_period pd
            ON pd.periodseq = a.periodseq
            AND pd.removedate = TO_DATE('22000101', 'YYYYMMDD')
            AND a.periodseq = :in_PERIODSEQ
        GROUP BY
            'STEL', VENDORNAME, a.PERIODSEQ, PERIODNAME, PAYEESEQ,
            POSITIONSEQ, PROCESSINGUNITSEQ, PROCESSINGUNITNAME, VENDORCODE, VENDORCODE,
            CURRENT_TIMESTAMP, 'EXTPPCOVERNOTE', a.PERIODSEQ, VENDORNAME, PERIODNAME,  /* ORIGSQL: SYSDATE */
            CALENDARNAME,  ext.trunc(pd.startdate,'QUARTER'), TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))), 'Prepaid';  /* ORIGSQL: trunc(pd.startdate,'Q') */
                                                                                                                                            /* ORIGSQL: pd.enddate-1 */

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --added by kyap, to handle midmonth VP payees, to display date range of midmonth
    --below query is moved over from covernote report as CR does not allow with-clause in query statement
    --use of temp table, as direct update on ST_EXT_PAY_SUMMARY will impact payment summary report

    /* ORIGSQL: EXECUTE IMMEDIATE 'Truncate table EXT.STEL_RPT_COVERNOTE'; */
    /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_RPT_COVERNOTE' not found */

    /* ORIGSQL: Truncate table EXT.STEL_RPT_COVERNOTE ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.STEL_RPT_COVERNOTE';


-- select * from ext.STEL_RPT_COVERNOTE
    /* RESOLVE: Identifier not found: Table/view 'ext.STEL_LOOKUP' not found */
    /* RESOLVE: Identifier not found: Table/view 'ext.STEL_CLASSIFIER_TAB' not found */
    /* RESOLVE: Identifier not found: Table/view 'ext.ST_EXT_PAY_SUMMARY' not found */
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIOD' not found */
    /* ORIGSQL: insert into EXT.STEL_RPT_COVERNOTE (PERIODSEQ, PERIODNAME, STARTDATE, ENDDATE, VENDO(...) */
    INSERT INTO EXT.STEL_RPT_COVERNOTE
        (
            PERIODSEQ,
            PERIODNAME,
            STARTDATE,
            ENDDATE,
            VENDOR_NAME,
            GROUPFIELDLABEL,
            AMOUNT,
            GENERICNUMBER1,
            RPTTYPE,
            DATAPERIODNAME,
            SUBJECT1,
            PERIODLABEL,
            TO_DET,
            TO_DETAIL,
            FROM_DETAILS,
            FOOTER_DETAILS1,
            FOOTER_DETAILS2,
            FOOTER_DETAILS3,
            CALENDARNAME,
            PROCESSINGUNITNAME
        )
        WITH 
        refdata AS (
            SELECT   /* ORIGSQL: (select dim0, value from ext.stel_lookup where name = 'LT_VirtualPartners_Ra(...) */
                dim0,
                value
            FROM
                ext.stel_lookup
            WHERE
                name = 'LT_VirtualPartners_Rates'
                AND dim1 = 'Mid Month Cut Off'
                AND dim2 LIKE 'Top Up Revenue%'
        
        )
        SELECT   /* ORIGSQL: SELECT T1.PERIODSEQ, T1.PERIODNAME, T1.STARTDATE, T1.ENDDATE, CASE WHEN rf.value(...) */
            T1.PERIODSEQ,
            T1.PERIODNAME,
            T1.STARTDATE,
            T1.ENDDATE,
            CASE
                WHEN rf.value IS NOT NULL
                THEN
                CASE
                    WHEN T1.DATAPERIODNAME LIKE '%A'
                    THEN IFNULL(T1.VENDOR_NAME,'') || '('|| IFNULL(TO_VARCHAR(T1.startdate,'dd'),'') || '-'|| IFNULL(TO_VARCHAR(rf.value),'') || 'th '|| IFNULL(T1.periodname,'') || ')'   /* ORIGSQL: to_char(T1.startdate,'dd') */
                    ELSE IFNULL(T1.VENDOR_NAME,'') || '('|| IFNULL(TO_VARCHAR(rf.value+1),'') || '-'|| IFNULL(TO_VARCHAR(T1.enddate,'dd'),'') || 'th '|| IFNULL(T1.periodname,'') || ')'   /* ORIGSQL: to_char(T1.enddate,'dd') */
                END
                ELSE T1.VENDOR_NAME
            END
            AS VENDOR_NAME,
            T1.GROUPFIELDLABEL,
            T1.AMOUNT,
            T1.GENERICNUMBER1,
            T1.RPTTYPE,
            T1.DATAPERIODNAME,
            T1.SUBJECT1,
            T1.PERIODLABEL,
            T1.TO_DET,
            CASE
                WHEN T1.AMOUNT >= CF.GENERICNUMBER2
                AND T1.AMOUNT < CF.GENERICNUMBER3
                THEN CF.GENERICATTRIBUTE7
                ELSE
                CASE
                    WHEN T1.AMOUNT >= CF.GENERICNUMBER3
                    AND T1.AMOUNT < CF.GENERICNUMBER4
                    THEN CF.GENERICATTRIBUTE9
                    ELSE
                    CASE
                        WHEN T1.AMOUNT >= CF.GENERICNUMBER4
                        THEN CF.GENERICATTRIBUTE11
                    END
                END
            END
            AS to_detail,
            CASE
                WHEN T1.AMOUNT >= CF.GENERICNUMBER2
                AND T1.AMOUNT < CF.GENERICNUMBER3
                THEN CF.GENERICATTRIBUTE8
                ELSE
                CASE
                    WHEN T1.AMOUNT >= CF.GENERICNUMBER3
                    AND T1.AMOUNT < CF.GENERICNUMBER4
                    THEN CF.GENERICATTRIBUTE10
                    ELSE
                    CASE
                        WHEN T1.AMOUNT >= CF.GENERICNUMBER4
                        THEN CF.GENERICATTRIBUTE12
                    END
                END
            END
            AS from_Details,
            T1.FOOTER_DETAILS1,
            T1.FOOTER_DETAILS2,
            T1.FOOTER_DETAILS3,
            CALENDARNAME,
            PROCESSINGUNITNAME
        FROM
            /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
            EXT.STEL_CLASSIFIER_TAB AS CF
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select x.periodseq,x.periodname,x.startdate,x.enddate, x.vendor_name, nvl(x.gro(...) */
                    x.periodseq,
                    x.periodname,
                    x.startdate,
                    x.enddate,
                    x.vendor_name,
                    IFNULL(x.groupfieldlabel2,x.groupfieldlabel) AS groupfieldlabel,  /* ORIGSQL: nvl(x.groupfieldlabel2,x.groupfieldlabel) */
                    SUM(x.amount) AS AMOUNT,
                    c.genericnumber1,
                    'n.a.' AS rpttype,
                    CASE
                        WHEN IFNULL(r.periodtype,'month') = 'month'  /* ORIGSQL: nvl(r.periodtype,'month') */
                        THEN dataperiodname
                        ELSE periodname
                    END
                    AS dataperiodname,
                    'Commission Payment for '||IFNULL(x.groupfieldlabel2,x.groupfieldlabel) ||' Period  ' AS subject1,  /* ORIGSQL: nvl(x.groupfieldlabel2,x.groupfieldlabel) */
                    (
                        CASE
                            WHEN EXTRACT(MONTH FROM x.startdate) = EXTRACT(MONTH FROM x.enddate)
                            AND LAST_DAY(x.enddate) = x.enddate
                            AND EXTRACT(DAY FROM x.startdate) = 1
                            THEN x.periodname
                            WHEN EXTRACT(MONTH FROM x.startdate) = EXTRACT(MONTH FROM x.enddate)
                            AND LAST_DAY(x.enddate) = x.enddate
                            AND EXTRACT(DAY FROM x.startdate) <> 1
                            THEN x.dataperiodname
                            WHEN EXTRACT(MONTH FROM x.startdate) = EXTRACT(MONTH FROM x.enddate)
                            AND LAST_DAY(x.enddate) <> x.enddate
                            AND EXTRACT(DAY FROM x.startdate) = 1
                            THEN x.dataperiodname
                            -- ELSE IFNULL(TRIM(sapdbmtk.sp_f_dbmtk_format_datetime_to_string(x.startdate,'Month')),'') ||' '|| IFNULL(TRIM(TO_VARCHAR(x.startdate,'YYYY')),'') || ' to '|| IFNULL(TRIM(sapdbmtk.sp_f_dbmtk_format_datetime_to_string(x.enddate,'Month')),'') ||' '|| IFNULL(TRIM(TO_VARCHAR(x.enddate,'YYYY')),'')  /* ORIGSQL: to_char(x.startdate,'YYYY') */
                           else ext.fn_quarter_period_label (x.startdate) --Deepan : new function to get the quarter range                                                                                                                                                                                                                                                                                                                    /* ORIGSQL: to_char(x.startdate,'Month') */
                                                                                                                                                                                                                                                                                                                                                   /* ORIGSQL: to_char(x.enddate,'YYYY') */
                                                                                                                                                                                                                                                                                                                                                   /* ORIGSQL: to_char(x.enddate,'Month') */
                        END
                    ) AS periodlabel,
                    IFNULL(SUBSTRING_REGEXPR('[^|]+' IN c.genericattribute1),'') ||'' AS to_det,  /* ORIGSQL: regexp_substr(c.genericattribute1,'[^|]+') */
                    SUBSTRING_REGEXPR('[^|]+' IN c.genericattribute3) AS footer_Details1,  /* ORIGSQL: regexp_substr(c.genericattribute3,'[^|]+') */
                    SUBSTRING_REGEXPR('[^|]+' IN c.genericattribute3 FROM 1 OCCURRENCE 2) AS footer_Details2,  /* ORIGSQL: regexp_substr(c.genericattribute3,'[^|]+',1,2) */
                    TRIM(SUBSTRING_REGEXPR('[^|]+' IN c.genericattribute3 FROM 2 OCCURRENCE 3)) AS footer_details3,  /* ORIGSQL: regexp_substr(c.genericattribute3,'[^|]+',2,3) */
                    x.calendarname,
                    x.processingunitname
                FROM
                    (
                        SELECT   /* ORIGSQL: (select * from ext.STEL_CLASSIFIER_TAB where categorytreename='Reporting Con(...) */
                            *
                        FROM
                            ext.STEL_CLASSIFIER_TAB
                        WHERE
                            categorytreename = 'Reporting Config'
                            AND categoryname = 'Cover Note'
                    ) AS c,
                    ext.ST_EXT_PAY_SUMMARY x
                    --ext.TEMP1 x
                INNER JOIN
                    cs_period pd
                    ON pd.periodseq = x.periodseq
                    AND pd.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    /* RESOLVE: Identifier not found: Table/view 'ext.STEL_RPT_CFG_RPTTYPE' not found */
                LEFT OUTER JOIN
                    (
                        SELECT   /* ORIGSQL: (select y.rpttype, MAX(nvl(y.periodtype,'month')) periodtype from ext.stel_r(...) */
                            y.rpttype,
                            MAX(IFNULL(y.periodtype,'month')) AS periodtype  /* ORIGSQL: nvl(y.periodtype,'month') */
                        FROM
                            ext.stel_rpt_cfg_rpttype y
                        GROUP BY
                            y.rpttype
                    ) AS r
                    ON r.rpttype = x.rpttype
                WHERE
                    c.effectivestartdate < pd.enddate
                    AND c.effectiveenddate >= pd.enddate
                    AND x.rpttype IN('EXTPMTSUMMARY_VP','EXTPMTSUMMARY','EXTPMTSUMMARY_MIDMONTH')
                    --and x.periodname ='{?Period Name}'
                    --and x.calendarname='{?Calendar Name}'
                    --and x.processingunitname='{?ProcessingUnit Name}'
                    AND x.periodseq = :in_PERIODSEQ
                    AND x.processingunitseq = :in_PROCESSINGUNITSEQ
                    --and'{?AncestorUserIdTenantId}'='{?AncestorUserIdTenantId}'
                    -- and x.groupfieldlabel = c.genericattribute6 --bugfix by kyap, to include adjustments
                    AND IFNULL(x.groupfieldlabel2,x.groupfieldlabel) = c.genericattribute6  /* ORIGSQL: nvl(x.groupfieldlabel2,x.groupfieldlabel) */
                    -- and x.periodname ='April 2017'
                GROUP BY
                    x.periodseq,x.periodname,x.startdate,x.enddate,
                    x.vendor_name,
                    IFNULL(x.groupfieldlabel2,x.groupfieldlabel),c.genericnumber1,'n.a.',  /* ORIGSQL: nvl(x.groupfieldlabel2,x.groupfieldlabel) */
                    CASE
                        WHEN IFNULL(r.periodtype,'month') = 'month'  /* ORIGSQL: nvl(r.periodtype,'month') */
                        THEN dataperiodname
                        ELSE periodname
                    END,
                    'Commission Payment for '||IFNULL(x.groupfieldlabel2,x.groupfieldlabel) ||' Period ',  /* ORIGSQL: nvl(x.groupfieldlabel2,x.groupfieldlabel) */
                    (
                        CASE
                            WHEN EXTRACT(MONTH FROM x.startdate) = EXTRACT(MONTH FROM x.enddate)
                            AND LAST_DAY(x.enddate) = x.enddate
                            AND EXTRACT(DAY FROM x.startdate) = 1
                            THEN x.periodname
                            WHEN EXTRACT(MONTH FROM x.startdate) = EXTRACT(MONTH FROM x.enddate)
                            AND LAST_DAY(x.enddate) = x.enddate
                            AND EXTRACT(DAY FROM x.startdate) <> 1
                            THEN x.dataperiodname
                            WHEN EXTRACT(MONTH FROM x.startdate) = EXTRACT(MONTH FROM x.enddate)
                            AND LAST_DAY(x.enddate) <> x.enddate
                            AND EXTRACT(DAY FROM x.startdate) = 1
                            THEN x.dataperiodname
                            -- ELSE IFNULL(TRIM(sapdbmtk.sp_f_dbmtk_format_datetime_to_string(x.startdate,'Month')),'') ||' '|| IFNULL(TRIM(TO_VARCHAR(x.startdate,'YYYY')),'') || ' to '|| IFNULL(TRIM(sapdbmtk.sp_f_dbmtk_format_datetime_to_string(x.enddate,'Month')),'') ||' '|| IFNULL(TRIM(TO_VARCHAR(x.enddate,'YYYY')),'')  /* ORIGSQL: to_char(x.startdate,'YYYY') */
                            ELSE ext.fn_quarter_period_label(x.startdate)--Deepan : new function introduced                                                                                                                                                                                                                                                                                                                       /* ORIGSQL: to_char(x.startdate,'Month') */
                                                                                                                                                                                                                                                                                                                                                   /* ORIGSQL: to_char(x.enddate,'YYYY') */
                                                                                                                                                                                                                                                                                                                                                   /* ORIGSQL: to_char(x.enddate,'Month') */
                        END
                    ),
                    IFNULL(SUBSTRING_REGEXPR('[^|]+' IN c.genericattribute1),'') ||'',  /* ORIGSQL: regexp_substr(c.genericattribute1,'[^|]+') */
                    IFNULL(SUBSTRING_REGEXPR('[^|]+' IN c.genericattribute1),'') || IFNULL(SUBSTRING_REGEXPR('[^|]+' IN c.genericattribute1 FROM 1 OCCURRENCE 2),'') ||IFNULL(TRIM(SUBSTRING_REGEXPR('[^|]+' IN c.genericattribute1 FROM 2 OCCURRENCE 3)),''),  /* ORIGSQL: regexp_substr(c.genericattribute1,'[^|]+',2,3) */
                                                                                                                                                                                                                                                                                                                    /* ORIGSQL: regexp_substr(c.genericattribute1,'[^|]+',1,2) */
                                                                                                                                                                                                                                                                                                                    /* ORIGSQL: regexp_substr(c.genericattribute1,'[^|]+') */
                    IFNULL(SUBSTRING_REGEXPR('[^|]+' IN c.genericattribute2),'') || IFNULL(SUBSTRING_REGEXPR('[^|]+' IN c.genericattribute2 FROM 1 OCCURRENCE 2),'') ||IFNULL(SUBSTRING_REGEXPR('[^|]+' IN c.genericattribute2 FROM 2 OCCURRENCE 3),''),  /* ORIGSQL: regexp_substr(c.genericattribute2,'[^|]+',2,3) */
                                                                                                                                                                                                                                                                                                              /* ORIGSQL: regexp_substr(c.genericattribute2,'[^|]+',1,2) */
                                                                                                                                                                                                                                                                                                              /* ORIGSQL: regexp_substr(c.genericattribute2,'[^|]+') */
                    SUBSTRING_REGEXPR('[^|]+' IN c.genericattribute3),  /* ORIGSQL: regexp_substr(c.genericattribute3,'[^|]+') */
                    SUBSTRING_REGEXPR('[^|]+' IN c.genericattribute3 FROM 1 OCCURRENCE 2),  /* ORIGSQL: regexp_substr(c.genericattribute3,'[^|]+',1,2) */
                    TRIM(SUBSTRING_REGEXPR('[^|]+' IN c.genericattribute3 FROM 2 OCCURRENCE 3)),  /* ORIGSQL: regexp_substr(c.genericattribute3,'[^|]+',2,3) */
                    x.calendarname,
                    x.processingunitname
                HAVING
                    SUM(x.amount) > c.genericnumber1
            ) AS T1
            ON IFNULL(T1.GROUPFIELDLABEL,' ') = IFNULL(CF.GENERICATTRIBUTE6, ' ')   /* ORIGSQL: NVL(T1.GROUPFIELDLABEL,' ') */
                                                                                    /* ORIGSQL: NVL(CF.GENERICATTRIBUTE6,' ') */
            AND CF.categoryname = 'Cover Note' 
        LEFT OUTER JOIN
            refdata AS rf
            ON t1.vendor_name = rf.dim0;  /* ORIGSQL: t1.vendor_name = rf.dim0(+) */

    /* ORIGSQL: commit; */
    COMMIT;
END