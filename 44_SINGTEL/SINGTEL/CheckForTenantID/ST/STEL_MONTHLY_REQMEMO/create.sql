CREATE PROCEDURE EXT.STEL_MONTHLY_REQMEMO
(
    IN IN_RPTTYPE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                  /* ORIGSQL: IN_RPTTYPE IN varchar2 */
    IN IN_PERIODSEQ BIGINT,   /* ORIGSQL: IN_PERIODSEQ IN INTEGER */
    IN IN_PROCESSINGUNITSEQ BIGINT     /* ORIGSQL: IN_PROCESSINGUNITSEQ IN INTEGER */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_Tenant VARCHAR(10) = 'STEL';  /* ORIGSQL: v_Tenant VARCHAR2(10) := 'STEL'; */
    DECLARE v_StMsg VARCHAR(10) = 'START';  /* ORIGSQL: v_StMsg VARCHAR2(10) := 'START'; */
    DECLARE v_EdMsg VARCHAR(10) = 'END';  /* ORIGSQL: v_EdMsg VARCHAR2(10) := 'END'; */
    DECLARE v_ComponentName VARCHAR(100) = NULL;  /* ORIGSQL: v_ComponentName VARCHAR2(100) := NULL; */
    DECLARE v_CalendarName VARCHAR(100) = NULL;  /* ORIGSQL: v_CalendarName VARCHAR2(100) := NULL; */
    DECLARE v_PeriodName VARCHAR(100) = NULL;  /* ORIGSQL: v_PeriodName VARCHAR2(100) := NULL; */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    DECLARE v_periodenddate TIMESTAMP;  /* ORIGSQL: v_periodenddate DATE; */
    DECLARE v_periodstartdate TIMESTAMP;  /* ORIGSQL: v_periodstartdate DATE; */
    DECLARE v_companyname VARCHAR(100) = NULL;  /* ORIGSQL: v_companyname VARCHAR2(100) := NULL; */
    DECLARE v_regNo VARCHAR(100) = NULL;  /* ORIGSQL: v_regNo VARCHAR2(100) := NULL; */

    v_ComponentName = 'STEL_MONTHLY_REQMEMO';

    -- Add debug Log for Process START
    /* ORIGSQL: EXT.STEL_log (v_Tenant, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSeq || ' PU - ' || in_ProcessingUnitSeq) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIODCALENDAR' not found */

    SELECT
        CALENDARNAME,
        PERIODNAME
    INTO
        v_CalendarName,
        v_PeriodName
    FROM
        cs_periodcalendar
    WHERE
        PERIODSEQ = :IN_PERIODSEQ;

    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIOD' not found */

    SELECT
        startdate,
        enddate
    INTO
        v_periodstartdate,
        v_periodenddate
    FROM
        cs_period
    WHERE
        periodseq = :IN_PERIODSEQ
        AND removedate = :v_eot;

    --     CleanUp existing records for the Period 
    /* ORIGSQL: DELETE from EXT.STEL_RPT_REQMEMO WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUNITSEQ = IN_PROCESSINGUNITSEQ; */
    DELETE
    FROM
        ext.STEL_RPT_REQMEMO
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: EXT.STEL_PROC_RPT_partitions_pseq (IN_PERIODSEQ, 'STEL_RPT_REQMEMO') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS_PSEQ(:IN_PERIODSEQ, 'STEL_RPT_REQMEMO');

    -- managing table partitions

    --added by kyap, vpdata with-clause data to handle midmonth VP payees, to display date range of midmonth
    --update on vendor_name, as this will cater to reqmemo report separation

    /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_RPT_REQMEMO' not found */

    /* ORIGSQL: INSERT INTO EXT.STEL_RPT_REQMEMO (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERIODNAME, reportperiodname, PROCESSINGUNITNAME, CALENDARNAME, POSITIONSEQ, PAYEESEQ, DEALER_CODE, DEALER_NAME, VENDOR_CODE, VEND(...) */
    /* RESOLVE: Identifier not found: Table/view 'EXT.ST_EXT_PAY_DETAIL' not found */
    INSERT INTO EXT.STEL_RPT_REQMEMO
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            reportperiodname,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            DEALER_CODE,
            DEALER_NAME,
            VENDOR_CODE,
            VENDOR_NAME,
            PRODUCT_NAME,
            SCHEME_TYPE,
            QUANTITY,
            REVENUE,
            PAYOUT,
            RATE,
            RATEUNIT,
            COMPANYNAME,
            REGNO,
            CREATEDATE
        )
        WITH 
        refdata AS (
            SELECT   /* ORIGSQL: (SELECT genericattribute5 vendorcode, genericattribute6 product, genericattribute1 companyname, genericattribute2 regNo FROM EXT.STEL_classifier WHERE categorytreename = 'Reporting Config' AND categorynam(...) */
                genericattribute5 AS vendorcode,
                genericattribute6 AS product,
                genericattribute1 AS companyname,
                genericattribute2 AS regNo
            FROM
                EXT.STEL_classifier
            WHERE
                --            effectivestartdate < v_periodenddate
                -- AND effectiveenddate > v_periodstartdate AND
                categorytreename = 'Reporting Config'
                AND categoryname LIKE 'RequisitionMemo%'
        
        )
        ,
        vpdata AS
        (
            SELECT   /* ORIGSQL: (select dim0, value from EXT.STEL_lookup where name = 'LT_VirtualPartners_Rates' and dim1 = 'Mid Month Cut Off' and dim2 like 'Top Up Revenue%') */
                dim0,
                value
            FROM
                EXT.STEL_lookup
            WHERE
                name = 'LT_VirtualPartners_Rates'
                AND dim1 = 'Mid Month Cut Off'
                AND dim2 LIKE 'Top Up Revenue%'
        
        )
        SELECT   /* ORIGSQL: SELECT S1.TENANTID, S1.PERIODSEQ, S1.PROCESSINGUNITSEQ, v_periodname periodname, s1.reportperiodname REPORtPERIODNAME, S1.PROCESSINGUNITNAME, v_CalendarName, S1.POSITIONSEQ, S1.PAYEESEQ, s1.delindicat(...) */
            S1.TENANTID,
            S1.PERIODSEQ,
            S1.PROCESSINGUNITSEQ,
            :v_PeriodName AS periodname,
            s1.reportperiodname AS REPORtPERIODNAME,
            S1.PROCESSINGUNITNAME,
            :v_CalendarName,
            S1.POSITIONSEQ,
            S1.PAYEESEQ,
            /* --null DEARLER_CODE, */
            s1.delindicator AS DEARLER_CODE,
            NULL AS DEALER_NAME,
            IFNULL(S1.VENDOR_CODE,s1.dearler_Code) AS vendor_Code,  /* ORIGSQL: nvl(S1.VENDOR_CODE,s1.dearler_Code) */
            /* --nvl(S1.VENDOR_NAME,s1.dealer_name) dealer_name, */
            CASE
                WHEN vp.value IS NOT NULL
                AND s1.rpttype = 'EXTPMTDETAIL_VP'
                THEN
                CASE
                    WHEN TO_INT(TO_VARCHAR(s1.transaction_date,'dd'),'99') <= vp.value  /* ORIGSQL: to_number(to_char(s1.transaction_date,'dd'),'99') */
                    THEN IFNULL(S1.VENDOR_NAME,s1.dealer_name) || '('|| IFNULL(TO_VARCHAR(s1.startdate,'dd'),'') || '-'|| IFNULL(TO_VARCHAR(vp.value),'') || 'th '|| IFNULL(s1.reportperiodname,'') || ')'   /* ORIGSQL: to_char(s1.startdate,'dd') */
                                                                                                                                                                                                                              /* ORIGSQL: nvl(S1.VENDOR_NAME,s1.dealer_name) */
                    ELSE IFNULL(S1.VENDOR_NAME,s1.dealer_name) || '('|| IFNULL(TO_VARCHAR(vp.value+1),'') || '-'|| IFNULL(TO_VARCHAR(s1.enddate,'dd'),'') || 'th '|| IFNULL(s1.reportperiodname,'') || ')'   /* ORIGSQL: to_char(s1.enddate,'dd') */
                                                                                                                                                                                                                              /* ORIGSQL: nvl(S1.VENDOR_NAME,s1.dealer_name) */
                END
                ELSE IFNULL(S1.VENDOR_NAME,s1.dealer_name)  /* ORIGSQL: nvl(S1.VENDOR_NAME,s1.dealer_name) */
            END
            AS dealer_name,
            S1.PRODUCT_NAME,
            CASE
                WHEN s1.scheme_type LIKE 'Adjustment%'
                THEN s1.scheme_type
                ELSE S1.product_name
            END,
            SUM(S1.QUANTITY) AS QUANTITY,
            SUM(S1.AMOUNT) AS REVENUE,
            SUM(S1.COMMISSION) AS PAYOUT,
            (S1.RATE) AS RATE,
            S1.RATEUNIT AS RATEUNIT,
            rf.companyname,
            rf.REGNO,
            CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        FROM
            EXT.ST_EXT_PAY_DETAIL S1
        LEFT OUTER JOIN
            refdata rf
            ON TRIM(rf.product) = TRIM(s1.product_name)
            AND rf.vendorcode = 'ALL'
        LEFT OUTER JOIN
            vpdata vp
            ON s1.vendor_name = vp.dim0
        WHERE
            S1.PERIODSEQ = :IN_PERIODSEQ
            AND S1.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
            -- Commented below condition by AMK to show Dereg data in the report.  on 12.07.2018
            -- and (nvl(s1.rate,0)<>0 or s1.scheme_type like 'Adjustment%')
        GROUP BY
            S1.TENANTID,
            S1.PERIODSEQ,
            S1.PROCESSINGUNITSEQ,
            s1.reportperiodname,
            S1.PROCESSINGUNITNAME,
            :v_CalendarName,
            S1.POSITIONSEQ,
            S1.PAYEESEQ,
            s1.delindicator,
            --S1.DEARLER_CODE,
            --S1.DEALER_NAME,
            s1.rate,
            IFNULL(S1.VENDOR_CODE,s1.dearler_Code),  /* ORIGSQL: nvl(S1.VENDOR_CODE,s1.dearler_Code) */
            --nvl(S1.VENDOR_NAME,s1.dealer_name) ,
            CASE
                WHEN vp.value IS NOT NULL
                AND s1.rpttype = 'EXTPMTDETAIL_VP'
                THEN
                CASE
                    WHEN TO_INT(TO_VARCHAR(s1.transaction_date,'dd'),'99') <= vp.value  /* ORIGSQL: to_number(to_char(s1.transaction_date,'dd'),'99') */
                    THEN IFNULL(S1.VENDOR_NAME,s1.dealer_name) || '('|| IFNULL(TO_VARCHAR(s1.startdate,'dd'),'') || '-'|| IFNULL(TO_VARCHAR(vp.value),'') || 'th '|| IFNULL(s1.reportperiodname,'') || ')'   /* ORIGSQL: to_char(s1.startdate,'dd') */
                                                                                                                                                                                                                              /* ORIGSQL: nvl(S1.VENDOR_NAME,s1.dealer_name) */
                    ELSE IFNULL(S1.VENDOR_NAME,s1.dealer_name) || '('|| IFNULL(TO_VARCHAR(vp.value+1),'') || '-'|| IFNULL(TO_VARCHAR(s1.enddate,'dd'),'') || 'th '|| IFNULL(s1.reportperiodname,'') || ')'   /* ORIGSQL: to_char(s1.enddate,'dd') */
                                                                                                                                                                                                                              /* ORIGSQL: nvl(S1.VENDOR_NAME,s1.dealer_name) */
                END
                ELSE IFNULL(S1.VENDOR_NAME,s1.dealer_name)  /* ORIGSQL: nvl(S1.VENDOR_NAME,s1.dealer_name) */
            END,
            S1.PRODUCT_NAME,
            CASE
                WHEN s1.scheme_type LIKE 'Adjustment%'
                THEN s1.scheme_type
                ELSE S1.product_name
            END,
            S1.RATEUNIT,
            rf.companyname,
            rf.regno,
            CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        HAVING
            SUM(S1.COMMISSION) > 0;

    -- added by kyap, to not display zero payout

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: MERGE / *+ USE_HASH(rnk, res)* / */
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO EXT.STEL_RPT_REQMEMO rnk USING (SELECT genericattribute5 AS vendorcode, genericattribute6 AS product, genericattribute1 AS companyname, genericattribute2 AS regNo FROM EXT.STEL_classifier WHERE eff(...) */
    MERGE INTO EXT.STEL_RPT_REQMEMO AS rnk
        USING
        (
            SELECT   /* ORIGSQL: (SELECT genericattribute5 vendorcode, genericattribute6 product, genericattribute1 companyname, genericattribute2 regNo FROM EXT.STEL_classifier WHERE effectivestartdate < v_periodenddate AND effectiveend(...) */
                genericattribute5 AS vendorcode,
                genericattribute6 AS product,
                genericattribute1 AS companyname,
                genericattribute2 AS regNo
            FROM
                EXT.STEL_classifier
            WHERE
                effectivestartdate < :v_periodenddate
                AND effectiveenddate > :v_periodstartdate
                AND categorytreename = 'Reporting Config'
                AND categoryname LIKE 'RequisitionMemo%'
                AND genericattribute5 <> 'ALL'
        ) AS res
        ON (rnk.product_name = res.product
            AND IFNULL(rnk.vendor_code, rnk.dealer_code) = res.vendorcode  /* ORIGSQL: NVL(rnk.vendor_code, rnk.dealer_code) */
            AND rnk.periodSeq = :IN_PERIODSEQ
        AND rnk.processingUnitSeq = :IN_PROCESSINGUNITSEQ)
    WHEN MATCHED THEN
        UPDATE SET rnk.companyname = res.companyname, rnk.regno = res.regNo;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --added by kyap, to update companyname and regno for DELVAS, using the DEL-ALL in reporting config, since DEL VAS product name varies, e.g. DEL VAS Basket 3
    SELECT
        genericattribute1,
        genericattribute2
    INTO
        v_companyname,
        v_regNo
    FROM
        EXT.STEL_classifier c
    WHERE
        categorytreename = 'Reporting Config'
        AND categoryname LIKE 'RequisitionMemo%'
        AND genericattribute5 = 'ALL'
        AND classifierid = 'DEL-ALL';  

    /* ORIGSQL: update EXT.STEL_RPT_REQMEMO SET companyname = v_companyname, regno = v_regNo where PERIODSEQ = IN_PERIODSEQ and PROCESSINGUNITSEQ = IN_PROCESSINGUNITSEQ and UPPER(product_name) like 'DEL VAS%' and company(...) */
    UPDATE EXT.STEL_RPT_REQMEMO
        SET
        /* ORIGSQL: companyname = */
        companyname = :v_companyname,
        /* ORIGSQL: regno = */
        regno = :v_regNo
    FROM
        EXT.STEL_RPT_REQMEMO
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
        AND UPPER(product_name) LIKE 'DEL VAS%' 
        AND companyname IS NULL
        AND regno IS NULL;

    /* ORIGSQL: commit; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: EXT.STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq || ' PU - ' || in_ProcessingUnitSeq) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END