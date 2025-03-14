CREATE LIBRARY "EXT"."PK_AIA_CB_CALCULATION_FA" LANGUAGE SQLSCRIPT AS
BEGIN
  PUBLIC VARIABLE STR_CYCLEDATE_FILE_NAME CONSTANT VARCHAR(10) = 'GLOBAL';
  PUBLIC VARIABLE STR_CYCLEDATE_KEY CONSTANT VARCHAR(20) = 'OPER_CYCLE_DATE';
  PUBLIC VARIABLE STR_DATE_FORMAT CONSTANT VARCHAR(50) = 'yyyymmdd';
  PUBLIC VARIABLE STR_DATE_FORMAT_TYPE CONSTANT VARCHAR(50) = 'yyyy-mm-dd';
  PUBLIC VARIABLE STR_CALENDAR_TYPE CONSTANT VARCHAR(10) = 'month';
  PUBLIC VARIABLE STR_CALENDAR_TYPE_QTR CONSTANT VARCHAR(10) = 'quarter';
  PUBLIC VARIABLE STR_CALENDARNAME CONSTANT VARCHAR(50) = 'AIA Singapore Calendar';
  PUBLIC VARIABLE STR_AGENCY_SHORT_NAME CONSTANT VARCHAR(5) = 'AGY';
  PUBLIC VARIABLE DT_REMOVEDATE CONSTANT TIMESTAMP = TO_DATE('22000101', 'yyyymmdd');
  PUBLIC VARIABLE STR_BUNAME_FA CONSTANT VARCHAR(20) = 'SGPAFA';
  PUBLIC VARIABLE STR_PU CONSTANT VARCHAR(20) = 'AGY_PU';
  PUBLIC VARIABLE STR_LUMPSUM CONSTANT VARCHAR(20) = 'LUMPSUM';
  PUBLIC VARIABLE STR_ONGOING CONSTANT VARCHAR(20) = 'ONGOING';
  PUBLIC VARIABLE STR_GST_COMMISSION CONSTANT VARCHAR(20) = 'GST';
  PUBLIC VARIABLE STR_COMMISSION CONSTANT VARCHAR(20) = 'COMMISSION';
  PUBLIC VARIABLE STR_COMPENSATION CONSTANT VARCHAR(20) = 'COMPENSATION';
  PUBLIC VARIABLE STR_IDENTIFY CONSTANT VARCHAR(20) = 'IDENTIFY';
  PUBLIC VARIABLE INT_SVI_RATE DECIMAL(10,2);
  PUBLIC VARIABLE STR_STATUS_START CONSTANT VARCHAR(20) = 'start';
  PUBLIC VARIABLE STR_STATUS_PROCESSING CONSTANT VARCHAR(20) = 'processing';
  PUBLIC VARIABLE STR_STATUS_FAIL CONSTANT VARCHAR(20) = 'fail';
  PUBLIC VARIABLE STR_STATUS_COMPLETED_SP CONSTANT VARCHAR(20) = 'completed_sp';
  PUBLIC VARIABLE STR_STATUS_COMPLETED_SH CONSTANT VARCHAR(20) = 'completed_sh';
  PUBLIC VARIABLE V_PROCESSINGUNITSEQ BIGINT;
  PUBLIC VARIABLE V_CALENDARSEQ BIGINT;
  PUBLIC VARIABLE V_WEEKEND_DATE VARCHAR(20);
  PUBLIC VARIABLE V_WEEKEND_SEQ VARCHAR(20);
  PUBLIC VARIABLE V_CYCLE_DATE VARCHAR(20);
  PUBLIC VARIABLE V_periodtype_quarter_seq BIGINT;
  PUBLIC VARIABLE V_periodtype_month_seq BIGINT;
  PUBLIC VARIABLE V_period_seq2 BIGINT;
  PUBLIC VARIABLE V_First_QTR BIGINT;
  PUBLIC VARIABLE V_Second_QTR BIGINT;
  PUBLIC PROCEDURE init_session_global
()
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE init_timestamp VARCHAR(50) := TO_VARCHAR(CURRENT_TIMESTAMP);
    /* do not execute further if package already initialized */
    IF SESSION_CONTEXT('EXT.PK_EXT.AIA_CB_CALCULATION_FA.INIT_SESSION_GLOBAL') IS NOT NULL
    THEN
        RETURN;
    END IF;

    /* mark package as initialized */
    SET SESSION 'EXT.PK_EXT.AIA_CB_CALCULATION_FA.INIT_SESSION_GLOBAL' = :init_timestamp;
    
    BEGIN
        /* Next lines imported from package header (EXT.AIA_CB_CALCULATION_FA.PACKAGE.plsql): */

        --  STR_BUNAME              CONSTANT VARCHAR2(20) := 'SGPAGY';

        INT_SVI_RATE = 0.60;

        /*status desc
        --start:
        --processing:
        --fail:
        --completed_sp:
        --completed_sh:
        */

        V_PROCESSINGUNITSEQ = NULL;

        V_CALENDARSEQ = NULL;

        V_WEEKEND_DATE = NULL;

        V_WEEKEND_SEQ = NULL;

        V_CYCLE_DATE = NULL;

        -- --Version 13 add by Amanda
        -- :V_periodtype_quarter_seq cs_periodtype.periodtypeseq%type;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table/Column 'cs_periodtype.periodtypeseq' not found (for %TYPE declaration) */
        -- :V_periodtype_month_seq cs_periodtype.periodtypeseq%type;;/* NOT CONVERTED! */
        -- :V_period_seq2 cs_period.periodseq%type;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */

        V_First_QTR = NULL;

        V_Second_QTR = NULL;
    END;
END;
  PUBLIC PROCEDURE init
()
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
DECLARE V_PROCESSINGUNITSEQ BIGINT;
DECLARE V_CALENDARSEQ BIGINT;  /* ORIGSQL: V_CALENDARSEQ INTEGER ; */
DECLARE V_WEEKEND_DATE VARCHAR(20);  /* ORIGSQL: V_WEEKEND_DATE VARCHAR2(20) ; */
DECLARE V_WEEKEND_SEQ VARCHAR(20);  /* ORIGSQL: V_WEEKEND_SEQ VARCHAR2(20) ; */
DECLARE V_CYCLE_DATE VARCHAR(20);  /* ORIGSQL: V_CYCLE_DATE VARCHAR2(20) ; */
--Version 13 add by Amanda
DECLARE V_periodtype_quarter_seq BIGINT;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table/Column 'cs_periodtype.periodtypeseq' not found (for %TYPE declaration) */
DECLARE V_periodtype_month_seq BIGINT;/* NOT CONVERTED! */
DECLARE V_period_seq2 BIGINT;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
DECLARE V_First_QTR BIGINT;  /* ORIGSQL: V_First_QTR INTEGER ; */
DECLARE V_Second_QTR BIGINT;  /* ORIGSQL: V_Second_QTR INTEGER ; */

    /* initialize library variables, if not yet done */
    CALL init_session_global();

    --setup processing unit seq number
    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_PROCESSINGUNIT' not found */

    SELECT
        processingunitseq
    INTO
        V_PROCESSINGUNITSEQ
    FROM
        cs_processingunit
    WHERE
        name = :STR_PU;

    --setup calendar seq number
    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_CALENDAR' not found */

    SELECT
        CALENDARSEQ
    INTO
        V_CALENDARSEQ
    FROM
        cs_calendar
    WHERE
        name = :STR_CALENDARNAME;

    --get weekend date

    SELECT
        CTL.TXT_KEY_VALUE
    INTO
        V_WEEKEND_DATE
    FROM
        EXT.IN_ETL_CONTROL CTL
    WHERE
        CTL.TXT_KEY_STRING = 'PAYMENT_END_DATE_WEEKLY'
        AND CTL.TXT_FILE_NAME = :STR_PU;

    --get week sequence number

    SELECT
        CTL.TXT_KEY_VALUE
    INTO
        V_WEEKEND_SEQ
    FROM
        EXT.IN_ETL_CONTROL CTL
    WHERE
        CTL.TXT_KEY_STRING = 'PAYMENT_SEQ_WEEKLY'
        AND CTL.TXT_FILE_NAME = :STR_PU;

    --get current cycle date

    SELECT
        CTL.TXT_KEY_VALUE
    INTO
        V_CYCLE_DATE
    FROM
        EXT.IN_ETL_CONTROL CTL
    WHERE
        CTL.TXT_KEY_STRING = 'OPER_CYCLE_DATE';

    --Version 2 Amanda add begin
    --Get quarter period type
    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_PERIODTYPE' not found */

    SELECT
        periodtypeseq
    INTO
        V_periodtype_quarter_seq
    FROM
        cs_periodtype
    WHERE
        name = :STR_CALENDAR_TYPE_QTR;

    SELECT
        periodtypeseq
    INTO
        V_periodtype_month_seq
    FROM
        cs_periodtype
    WHERE
        name = :STR_CALENDAR_TYPE;

    --Version 2 end

    /* ORIGSQL: EXT.Log('END INT') */
    CALL EXT.Log('END INT');
END;
  PUBLIC FUNCTION fn_get_periodseq
(
    IN P_STR_CB_CYCLEDATE VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                         /* ORIGSQL: P_STR_CB_CYCLEDATE IN VARCHAR2 */
)
RETURNS dbmtk_function_result DECIMAL(38,10)   /* ORIGSQL: return number */
SQL SECURITY DEFINER
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_periodseq BIGINT;  /* ORIGSQL: v_periodseq integer; */

    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_PERIOD' not found */
    SELECT
        cbp.periodseq
    INTO
        v_periodseq
    FROM
        cs_period cbp
    INNER JOIN
        cs_calendar cd
        ON cbp.calendarseq = cd.calendarseq
    INNER JOIN
        cs_periodtype pt
        ON cbp.periodtypeseq = pt.periodtypeseq
    WHERE
        cd.name = :STR_CALENDARNAME
        AND cbp.removedate = to_date('2200-01-01','yyyy-mm-dd') --for Cosimo
        /* ORIGSQL: to_date('2200-01-01','yyyy-mm-dd') */
        AND TO_DATE(:P_STR_CB_CYCLEDATE, :STR_DATE_FORMAT_TYPE) BETWEEN cbp.startdate AND TO_DATE(ADD_SECONDS(cbp.enddate,(86400*-1)))   /* ORIGSQL: cbp.enddate - 1 */
        AND pt.name = :STR_CALENDAR_TYPE;

    dbmtk_function_result = :v_periodseq;
    RETURN;
END;
  PUBLIC FUNCTION fn_get_batch_no
(
    IN P_STR_CYCLEDATE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                       /* ORIGSQL: P_STR_CYCLEDATE IN VARCHAR2 */
    IN P_CB_NAME VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                 /* ORIGSQL: P_CB_NAME IN VARCHAR2 */
    IN P_CB_TYPE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                 /* ORIGSQL: P_CB_TYPE IN VARCHAR2 */
    IN P_STATUS VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                               /* ORIGSQL: P_STATUS IN VARCHAR2 */
)
RETURNS dbmtk_function_result DECIMAL(38,10)   /* ORIGSQL: return number */
SQL SECURITY DEFINER
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_batch_no BIGINT;  /* ORIGSQL: v_batch_no integer; */

    SELECT
        IFNULL(MAX(cbs.batchnum), 0)  /* ORIGSQL: nvl(max(cbs.batchnum), 0) */
    INTO
        v_batch_no
    FROM
        EXT.AIA_CB_BATCH_STATUS cbs
    WHERE
        TO_VARCHAR(cbs.cycledate,'yyyymm') =  /* ORIGSQL: to_char(cbs.cycledate, 'yyyymm') */
        TO_VARCHAR(TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE),'yyyymm')  /* ORIGSQL: to_char(to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE), 'yyyymm') */
        AND cbs.status = :P_STATUS
        AND cbs.clawbacktype = :P_CB_TYPE
        AND cbs.clawbackname = :P_CB_NAME
        AND cbs.islatest = 'Y'
        AND cbs.buname = :STR_BUNAME_FA;

    dbmtk_function_result = :v_batch_no;
    RETURN;
END;
  PRIVATE FUNCTION fn_get_batch_no_dbmtkoverloaded_1
()
RETURNS dbmtk_function_result DECIMAL(38,10)   /* ORIGSQL: return number */
SQL SECURITY DEFINER
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_batch_no BIGINT;  /* ORIGSQL: v_batch_no integer; */

    SELECT
        IFNULL(MAX(cbs.batchnum),0)  /* ORIGSQL: nvl(max(cbs.batchnum),0) */
    INTO
        v_batch_no
    FROM
        EXT.AIA_CB_BATCH_STATUS cbs
    WHERE
        cbs.buname = :STR_BUNAME_FA;

    dbmtk_function_result = :v_batch_no;
    RETURN;
END;
  PUBLIC FUNCTION fn_get_cb_type
(
    IN P_BATCH_NO BIGINT     /* ORIGSQL: P_BATCH_NO IN INTEGER */
)
RETURNS dbmtk_function_result VARCHAR(255)   /* ORIGSQL: return varchar2 */
/* RESOLVE: Manual edits required: VARCHAR2 function value(no length): user-configured length=255; adjust as needed */
SQL SECURITY DEFINER
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_cb_type VARCHAR(50);  /* ORIGSQL: v_cb_type varchar2(50); */

    SELECT
        cbs.clawbacktype
    INTO
        v_cb_type
    FROM
        EXT.AIA_CB_BATCH_STATUS cbs
    WHERE
        cbs.batchnum = :P_BATCH_NO;

    dbmtk_function_result = :v_cb_type;
    RETURN;
END;
  PUBLIC FUNCTION fn_get_cb_name
(
    IN P_BATCH_NO BIGINT     /* ORIGSQL: P_BATCH_NO IN INTEGER */
)
RETURNS dbmtk_function_result VARCHAR(255)   /* ORIGSQL: return varchar2 */
/* RESOLVE: Manual edits required: VARCHAR2 function value(no length): user-configured length=255; adjust as needed */
SQL SECURITY DEFINER
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_cb_name VARCHAR(50);  /* ORIGSQL: v_cb_name varchar2(50); */

    SELECT
        cbs.clawbackname
    INTO
        v_cb_name
    FROM
        EXT.AIA_CB_BATCH_STATUS cbs
    WHERE
        cbs.batchnum = :P_BATCH_NO;

    dbmtk_function_result = :v_cb_name;
    RETURN;
END;
  PUBLIC FUNCTION fn_get_cb_quarter
(
    IN P_BATCH_NO BIGINT     /* ORIGSQL: P_BATCH_NO IN INTEGER */
)
RETURNS dbmtk_function_result VARCHAR(255)   /* ORIGSQL: return varchar2 */
/* RESOLVE: Manual edits required: VARCHAR2 function value(no length): user-configured length=255; adjust as needed */
SQL SECURITY DEFINER
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_cb_quarter VARCHAR(50);  /* ORIGSQL: v_cb_quarter varchar2(50); */

    SELECT
        cbs.cb_quarter_name
    INTO
        v_cb_quarter
    FROM
        EXT.AIA_CB_BATCH_STATUS cbs
    WHERE
        cbs.batchnum = :P_BATCH_NO;

    dbmtk_function_result = :v_cb_quarter;
    RETURN;
END;
  PUBLIC PROCEDURE sp_get_batch_no_pre
(
    IN P_CB_NAME VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                 /* ORIGSQL: P_CB_NAME IN VARCHAR2 */
    IN P_CB_TYPE VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                /* ORIGSQL: P_CB_TYPE IN VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_cb_type VARCHAR(50);  /* ORIGSQL: v_cb_type varchar2(50); */
    DECLARE v_cb_name VARCHAR(50);  /* ORIGSQL: v_cb_name varchar2(50); */
    DECLARE v_cb_month VARCHAR(20);  /* ORIGSQL: v_cb_month VARCHAR2(20); */
    DECLARE v_pre_cb_batch_no BIGINT;  /* ORIGSQL: v_pre_cb_batch_no INTEGER; */
    DECLARE v_rec_count BIGINT;  /* ORIGSQL: v_rec_count INTEGER; */

    /* initialize library variables, if not yet done */
    CALL init_session_global();

    v_cb_month = TO_VARCHAR(TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE),'yyyymm');  /* ORIGSQL: to_char(to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE),'yyyymm') */

    /* ORIGSQL: delete from EXT.AIA_CB_BATCH_LIST cbl where cbl.clawbackname = P_CB_NAME and cbl.clawbacktype = P_CB_TYPE ; */
    DELETE
    FROM
        EXT.AIA_CB_BATCH_LIST
        cbl
    WHERE
        cbl.clawbackname = :P_CB_NAME
        AND cbl.clawbacktype = :P_CB_TYPE;

    --for commission lumpsum
    IF :P_CB_NAME = :STR_COMMISSION
    AND :P_CB_TYPE = :STR_LUMPSUM
    THEN
        /* ORIGSQL: insert into EXT.AIA_CB_BATCH_LIST select batchnum, cb_quarter_name, status, isactive, islatest, ispopulated, cycledate, clawbackname, clawbacktype from EXT.AIA_CB_BATCH_STATUS cbs where cbs.islatest = 'Y' and(...) */
        INSERT INTO EXT.AIA_CB_BATCH_LIST
            SELECT   /* ORIGSQL: select batchnum, cb_quarter_name, status, isactive, islatest, ispopulated, cycledate, clawbackname, clawbacktype from EXT.AIA_CB_BATCH_STATUS cbs where cbs.islatest = 'Y' and cbs.clawbackname = P_CB_NAME (...) */
                batchnum,
                cb_quarter_name,
                status,
                isactive,
                islatest,
                ispopulated,
                cycledate,
                clawbackname,
                clawbacktype
            FROM
                EXT.AIA_CB_BATCH_STATUS cbs
            WHERE
                cbs.islatest = 'Y'
                AND cbs.clawbackname = :P_CB_NAME
                AND cbs.clawbacktype = :P_CB_TYPE
                AND ((cbs.status = :STR_STATUS_COMPLETED_SP
                    AND cbs.isactive = 'Y')
                    OR (cbs.status = :STR_STATUS_COMPLETED_SH
                AND cbs.isactive = 'N'))
                AND TO_VARCHAR(cbs.cycledate,'yyyymm') = :v_cb_month;  /* ORIGSQL: to_char(cbs.cycledate, 'yyyymm') */

        /* ORIGSQL: commit; */
        COMMIT;

        --for commission on-going
    ELSEIF :P_CB_NAME = :STR_COMMISSION
    AND :P_CB_TYPE = :STR_ONGOING  /* ORIGSQL: elsif P_CB_NAME = STR_COMMISSION and P_CB_TYPE = STR_ONGOING then */
    THEN
        /* ORIGSQL: insert into EXT.AIA_CB_BATCH_LIST select batchnum, cb_quarter_name, status, isactive, islatest, ispopulated, cycledate, clawbackname, clawbacktype from EXT.AIA_CB_BATCH_STATUS cbs where cbs.islatest = 'Y' and(...) */
        INSERT INTO EXT.AIA_CB_BATCH_LIST
            SELECT   /* ORIGSQL: select batchnum, cb_quarter_name, status, isactive, islatest, ispopulated, cycledate, clawbackname, clawbacktype from EXT.AIA_CB_BATCH_STATUS cbs where cbs.islatest = 'Y' and cbs.clawbackname = P_CB_NAME (...) */
                batchnum,
                cb_quarter_name,
                status,
                isactive,
                islatest,
                ispopulated,
                cycledate,
                clawbackname,
                clawbacktype
            FROM
                EXT.AIA_CB_BATCH_STATUS cbs
            WHERE
                cbs.islatest = 'Y'
                AND cbs.clawbackname = :P_CB_NAME
                AND cbs.clawbacktype = :P_CB_TYPE
                AND TO_VARCHAR(cbs.cycledate,'yyyymm') = :v_cb_month  /* ORIGSQL: to_char(cbs.cycledate, 'yyyymm') */
                AND cbs.status = :STR_STATUS_COMPLETED_SP
                AND cbs.isactive = 'Y';

        /* ORIGSQL: commit; */
        COMMIT;

        --for compensation lumpsum
    ELSEIF :P_CB_NAME = :STR_COMPENSATION
    AND :P_CB_TYPE = :STR_LUMPSUM  /* ORIGSQL: elsif P_CB_NAME = STR_COMPENSATION and P_CB_TYPE = STR_LUMPSUM then */
    THEN
        /* ORIGSQL: insert into EXT.AIA_CB_BATCH_LIST select batchnum, cb_quarter_name, status, isactive, islatest, ispopulated, cycledate, clawbackname, clawbacktype from EXT.AIA_CB_BATCH_STATUS cbs where cbs.islatest = 'Y' and(...) */
        INSERT INTO EXT.AIA_CB_BATCH_LIST
            SELECT   /* ORIGSQL: select batchnum, cb_quarter_name, status, isactive, islatest, ispopulated, cycledate, clawbackname, clawbacktype from EXT.AIA_CB_BATCH_STATUS cbs where cbs.islatest = 'Y' and cbs.clawbackname = P_CB_NAME (...) */
                batchnum,
                cb_quarter_name,
                status,
                isactive,
                islatest,
                ispopulated,
                cycledate,
                clawbackname,
                clawbacktype
            FROM
                EXT.AIA_CB_BATCH_STATUS cbs
            WHERE
                cbs.islatest = 'Y'
                AND cbs.clawbackname = :P_CB_NAME
                AND cbs.clawbacktype = :P_CB_TYPE
                AND ((cbs.status = :STR_STATUS_COMPLETED_SP
                    AND cbs.isactive = 'Y')
                    OR (cbs.status = :STR_STATUS_COMPLETED_SH
                AND cbs.isactive = 'N'))
                AND TO_VARCHAR(cbs.cycledate,'yyyymm') = :v_cb_month;  /* ORIGSQL: to_char(cbs.cycledate, 'yyyymm') */

        /* ORIGSQL: commit; */
        COMMIT;

        --for compensation on-going
    ELSEIF :P_CB_NAME = :STR_COMPENSATION
    AND :P_CB_TYPE = :STR_ONGOING  /* ORIGSQL: elsif P_CB_NAME = STR_COMPENSATION and P_CB_TYPE = STR_ONGOING then */
    THEN
        /* ORIGSQL: insert into EXT.AIA_CB_BATCH_LIST select batchnum, cb_quarter_name, status, isactive, islatest, ispopulated, cycledate, clawbackname, clawbacktype from EXT.AIA_CB_BATCH_STATUS cbs where cbs.islatest = 'Y' and(...) */
        INSERT INTO EXT.AIA_CB_BATCH_LIST
            SELECT   /* ORIGSQL: select batchnum, cb_quarter_name, status, isactive, islatest, ispopulated, cycledate, clawbackname, clawbacktype from EXT.AIA_CB_BATCH_STATUS cbs where cbs.islatest = 'Y' and cbs.clawbackname = P_CB_NAME (...) */
                batchnum,
                cb_quarter_name,
                status,
                isactive,
                islatest,
                ispopulated,
                cycledate,
                clawbackname,
                clawbacktype
            FROM
                EXT.AIA_CB_BATCH_STATUS cbs
            WHERE
                cbs.islatest = 'Y'
                AND cbs.clawbackname = :P_CB_NAME
                AND cbs.clawbacktype = :P_CB_TYPE
                AND TO_VARCHAR(cbs.cycledate,'yyyymm') = :v_cb_month  /* ORIGSQL: to_char(cbs.cycledate, 'yyyymm') */
                AND cbs.status = :STR_STATUS_COMPLETED_SP
                AND cbs.isactive = 'Y';

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    --Log('previous clawback quarter is: ' || v_pre_cb_qtr);
END;
  PUBLIC PROCEDURE sp_update_batch_status
(
    IN P_BATCH_NO BIGINT,   /* ORIGSQL: P_BATCH_NO IN INTEGER */
    IN P_STR_STATUS VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                   /* ORIGSQL: P_STR_STATUS IN VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    --status: completed_sp

    IF :P_STR_STATUS = :STR_STATUS_COMPLETED_SP
    THEN 
        /* ORIGSQL: UPDATE EXT.AIA_CB_BATCH_STATUS ST SET ST.STATUS=P_STR_STATUS,ISACTIVE='Y',UPDATEDATE=SYSDATE WHERE ST.BATCHNUM=P_BATCH_NO; */
        UPDATE EXT.AIA_CB_BATCH_STATUS ST
            SET
            /* ORIGSQL: ST.STATUS = */
            STATUS = :P_STR_STATUS,
            /* ORIGSQL: ISACTIVE = */
            ISACTIVE = 'Y',
            /* ORIGSQL: UPDATEDATE = */
            UPDATEDATE = CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        WHERE
            ST.BATCHNUM = :P_BATCH_NO;

        --status: completed_sh
    ELSEIF :P_STR_STATUS = :STR_STATUS_COMPLETED_SH  /* ORIGSQL: ELSIF P_STR_STATUS = STR_STATUS_COMPLETED_SH THEN */
    THEN 
        /* ORIGSQL: UPDATE EXT.AIA_CB_BATCH_STATUS ST SET ST.STATUS=P_STR_STATUS,ISACTIVE='N',UPDATEDATE=SYSDATE WHERE ST.BATCHNUM=P_BATCH_NO; */
        UPDATE EXT.AIA_CB_BATCH_STATUS ST
            SET
            /* ORIGSQL: ST.STATUS = */
            STATUS = :P_STR_STATUS,
            /* ORIGSQL: ISACTIVE = */
            ISACTIVE = 'N',
            /* ORIGSQL: UPDATEDATE = */
            UPDATEDATE = CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        WHERE
            ST.BATCHNUM = :P_BATCH_NO;

        --status: fail
    ELSEIF :P_STR_STATUS = :STR_STATUS_FAIL  /* ORIGSQL: ELSIF P_STR_STATUS = STR_STATUS_FAIL THEN */
    THEN 
        /* ORIGSQL: UPDATE EXT.AIA_CB_BATCH_STATUS ST SET ST.STATUS=P_STR_STATUS,ISACTIVE='N',UPDATEDATE=SYSDATE WHERE ST.BATCHNUM=P_BATCH_NO; */
        UPDATE EXT.AIA_CB_BATCH_STATUS ST
            SET
            /* ORIGSQL: ST.STATUS = */
            STATUS = :P_STR_STATUS,
            /* ORIGSQL: ISACTIVE = */
            ISACTIVE = 'N',
            /* ORIGSQL: UPDATEDATE = */
            UPDATEDATE = CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        WHERE
            ST.BATCHNUM = :P_BATCH_NO;

        --status: processing
    ELSEIF :P_STR_STATUS = :STR_STATUS_PROCESSING  /* ORIGSQL: ELSIF P_STR_STATUS = STR_STATUS_PROCESSING THEN */
    THEN 
        /* ORIGSQL: UPDATE EXT.AIA_CB_BATCH_STATUS ST SET ST.STATUS=P_STR_STATUS,UPDATEDATE=SYSDATE WHERE ST.BATCHNUM=P_BATCH_NO; */
        UPDATE EXT.AIA_CB_BATCH_STATUS ST
            SET
            /* ORIGSQL: ST.STATUS = */
            STATUS = :P_STR_STATUS,
            /* ORIGSQL: UPDATEDATE = */
            UPDATEDATE = CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        WHERE
            ST.BATCHNUM = :P_BATCH_NO;
    END IF;

    /* ORIGSQL: commit; */
    COMMIT;
END;
  PUBLIC PROCEDURE sp_revert_by_batch
(
    IN P_BATCH_NO BIGINT     /* ORIGSQL: P_BATCH_NO IN INTEGER */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE V_REC_COUNT BIGINT;  /* ORIGSQL: V_REC_COUNT INTEGER; */

    /* ORIGSQL: EXT.Log('Revert clawback related tables for batch: ' || P_BATCH_NO) */
    CALL EXT.Log('Revert clawback related tables for batch: '|| IFNULL(TO_VARCHAR(:P_BATCH_NO),''));
    ----------------------------------------------------------------------------------
    --delete Credit records by batch number
    ----------------------------------------------------------------------------------
    --get records count from EXT.AIA_CB_CREDIT_STG
    SELECT
        COUNT(1)
    INTO
        V_REC_COUNT
    FROM
        EXT.AIA_CB_CREDIT_STG
    WHERE
        batch_no = :P_BATCH_NO;

    --delete the records in EXT.AIA_CB_CREDIT_STG if batch number is being reused.
    IF :V_REC_COUNT > 0
    THEN
        -- insert into EXT.AIA_cb_credit_Stg_reset   select sysdate, a.* from EXT.AIA_CB_CREDIT_STG a where batch_no = P_BATCH_NO;

        /* ORIGSQL: delete from EXT.AIA_CB_CREDIT_STG where batch_no = P_BATCH_NO; */
        DELETE
        FROM
            EXT.AIA_CB_CREDIT_STG
        WHERE
            batch_no = :P_BATCH_NO;

        /* ORIGSQL: EXT.Log('delete from EXT.AIA_CB_CREDIT_STG' || '; row count: ' || to_char(sql%rowcount)) */
        CALL EXT.Log('delete from EXT.AIA_CB_CREDIT_STG'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
            );  /* ORIGSQL: to_char(sql%rowcount) */

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    ----------------------------------------------------------------------------------
    --delete PM records by batch number
    ----------------------------------------------------------------------------------
    --get records count from EXT.AIA_CB_PM_STG
    SELECT
        COUNT(1)
    INTO
        V_REC_COUNT
    FROM
        EXT.AIA_CB_PM_STG
    WHERE
        batch_no = :P_BATCH_NO;

    --delete the records in EXT.AIA_CB_CREDIT_STG if batch number is being reused.
    IF :V_REC_COUNT > 0
    THEN
        --delete related records in EXT.AIA_CB_PM_STG

        /* ORIGSQL: delete from EXT.AIA_CB_PM_STG where batch_no = P_BATCH_NO; */
        DELETE
        FROM
            EXT.AIA_CB_PM_STG
        WHERE
            batch_no = :P_BATCH_NO;

        /* ORIGSQL: EXT.Log('delete from EXT.AIA_CB_PM_STG' || '; row count: ' || to_char(sql%rowcount)) */
        CALL EXT.Log('delete from EXT.AIA_CB_PM_STG'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
            );  /* ORIGSQL: to_char(sql%rowcount) */

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    ----------------------------------------------------------------------------------
    --delete PM Credit trace records by batch number
    ----------------------------------------------------------------------------------
    --get records count from EXT.AIA_CB_PM_STG
    SELECT
        COUNT(1)
    INTO
        V_REC_COUNT
    FROM
        EXT.AIA_CB_PMCRDTRACE_STG
    WHERE
        batch_no = :P_BATCH_NO;

    --delete the records in EXT.AIA_CB_PMCRDTRACE_STG if batch number is being reused.
    IF :V_REC_COUNT > 0
    THEN
        --delete related records in EXT.AIA_CB_PMCRDTRACE_STG

        /* ORIGSQL: delete from EXT.AIA_CB_PMCRDTRACE_STG where batch_no = P_BATCH_NO; */
        DELETE
        FROM
            EXT.AIA_CB_PMCRDTRACE_STG
        WHERE
            batch_no = :P_BATCH_NO;

        /* ORIGSQL: EXT.Log('delete from EXT.AIA_CB_PMCRDTRACE_STG' || '; row count: ' || to_char(sql%rowcount)) */
        CALL EXT.Log('delete from EXT.AIA_CB_PMCRDTRACE_STG'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
            );  /* ORIGSQL: to_char(sql%rowcount) */

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;
END;
  PUBLIC PROCEDURE sp_delete_policy
(
    IN P_POLICYIDSEQ BIGINT,   /* ORIGSQL: P_POLICYIDSEQ IN INTEGER */
    IN P_DELETEBY VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                 /* ORIGSQL: P_DELETEBY IN VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* ORIGSQL: insert into EXT.AIA_cb_identify_policy_log select BUNAME, YEAR, QUARTER, WRI_DIST_CODE, WRI_DIST_NAME, WRI_DM_CODE, WRI_DM_NAME, WRI_AGY_CODE, WRI_AGY_NAME, WRI_AGY_LDR_CODE, WRI_AGY_LDR_NAME, WRI_AGT_COD(...) */
    INSERT INTO EXT.AIA_cb_identify_policy_log
        SELECT   /* ORIGSQL: select BUNAME, YEAR, QUARTER, WRI_DIST_CODE, WRI_DIST_NAME, WRI_DM_CODE, WRI_DM_NAME, WRI_AGY_CODE, WRI_AGY_NAME, WRI_AGY_LDR_CODE, WRI_AGY_LDR_NAME, WRI_AGT_CODE, WRI_AGT_NAME, FSC_TYPE, RANK, CLASS,(...) */
            BUNAME,
            YEAR,
            QUARTER,
            WRI_DIST_CODE,
            WRI_DIST_NAME,
            WRI_DM_CODE,
            WRI_DM_NAME,
            WRI_AGY_CODE,
            WRI_AGY_NAME,
            WRI_AGY_LDR_CODE,
            WRI_AGY_LDR_NAME,
            WRI_AGT_CODE,
            WRI_AGT_NAME,
            FSC_TYPE,
            RANK,
            CLASS,
            FSC_BSC_GRADE,
            FSC_BSC_PERCENTAGE,
            PONUMBER,
            INSURED_NAME,
            CONTRACT_CAT,
            LIFE_NUMBER,
            COVERAGE_NUMBER,
            RIDER_NUMBER,
            COMPONENT_CODE,
            COMPONENT_NAME,
            ISSUE_DATE,
            INCEPTION_DATE,
            RISK_COMMENCEMENT_DATE,
            FHR_DATE,
            BASE_RIDER_IND,
            TRANSACTION_DATE,
            PAYMENT_MODE,
            POLICY_CURRENCY,
            PROCESSING_PERIOD,
            CREATED_DATE,
            POLICYIDSEQ,
            SUBMITDATE,
            :P_DELETEBY,
            FAOB_AGT_CODE
        FROM
            EXT.AIA_cb_identify_policy ip
        WHERE
            ip.policyidseq = :P_POLICYIDSEQ;

    /* ORIGSQL: delete from EXT.AIA_cb_identify_policy ip where ip.policyidseq = P_POLICYIDSEQ; */
    DELETE
    FROM
        EXT.AIA_cb_identify_policy
        ip
    WHERE
        ip.policyidseq = :P_POLICYIDSEQ;

    /* ORIGSQL: commit; */
    COMMIT;
END;
  PUBLIC PROCEDURE SP_POLICY_EXCL_FA
(
    IN P_STR_CYCLEDATE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                       /* ORIGSQL: P_STR_CYCLEDATE IN VARCHAR2 */
    IN P_CB_NAME VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                /* ORIGSQL: P_CB_NAME IN VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE V_CYCLEDATE TIMESTAMP;  /* ORIGSQL: V_CYCLEDATE DATE; */
    DECLARE V_CB_YEAR VARCHAR(20);  /* ORIGSQL: V_CB_YEAR VARCHAR2(20); */
    DECLARE V_CB_QUARTER VARCHAR(20);  /* ORIGSQL: V_CB_QUARTER VARCHAR2(20); */
    DECLARE V_INCEPTION_START_DT TIMESTAMP;  /* ORIGSQL: V_INCEPTION_START_DT DATE; */
    DECLARE V_INCEPTION_END_DT TIMESTAMP;  /* ORIGSQL: V_INCEPTION_END_DT DATE; */

    /* initialize library variables, if not yet done */
    CALL init_session_global();

    /* ORIGSQL: EXT.Log('SP_POLICY_EXCL_FA start') */
    CALL EXT.Log('SP_POLICY_EXCL_FA start');

    ------------------------get cycle date  'yyyy-mm-dd'--------------------------

    SELECT
        TO_DATE(IFNULL(:P_STR_CYCLEDATE, TXT_KEY_VALUE), :STR_DATE_FORMAT_TYPE)  /* ORIGSQL: NVL(P_STR_CYCLEDATE, TXT_KEY_VALUE) */
    INTO
        V_CYCLEDATE
    FROM
        EXT.IN_ETL_CONTROL
    WHERE
        TXT_FILE_NAME = :STR_CYCLEDATE_FILE_NAME
        AND TXT_KEY_STRING = :STR_CYCLEDATE_KEY;

    ------------------------get clawback year and quarter, inception period--------------------------

    SELECT
        CBP.YEAR,
        CBP.Quarter,
        CBP.Inception_Startdate,
        CBP.Inception_Enddate
    INTO
        V_CB_YEAR,
        V_CB_QUARTER,
        V_INCEPTION_START_DT,
        V_INCEPTION_END_DT
    FROM
        EXT.AIA_CB_PERIOD CBP
    WHERE
        CBP.CB_CYCLEDATE = TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
        AND CBP.BUNAME = :STR_BUNAME_FA
        AND CBP.Removedate = :DT_REMOVEDATE
        AND cbp.cb_name = :P_CB_NAME;

    --for trace indirect credit rule records
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into EXT.AIA_CB_POLICY_EXCL pol_ex using (SELECT hist.BUNAME, hist.PONUMBER FROM EXT.AIA_CB_POLICY_EXCL_HIST hist union select STR_BUNAME_FA, st.ponumber from cs_salestransaction st, EXT.AIA_CB_COMPONENT_EX(...) */
    MERGE INTO EXT.AIA_CB_POLICY_EXCL AS pol_ex
        USING
        (
            SELECT   /* ORIGSQL: (select hist.BUNAME, hist.PONUMBER from EXT.AIA_CB_POLICY_EXCL_HIST hist) */
                hist.BUNAME,
                hist.PONUMBER
            FROM
                EXT.AIA_CB_POLICY_EXCL_HIST hist
        UNION 
            SELECT   /* ORIGSQL: select STR_BUNAME_FA, st.ponumber from cs_salestransaction st, EXT.AIA_CB_COMPONENT_EXCL ex, cs_businessunit bu where st.tenantid='AIAS' and st.processingUnitseq=V_PROCESSINGUNITSEQ and st.genericattribut(...) */
                :STR_BUNAME_FA,
                st.ponumber
            FROM
                cs_salestransaction st,
                EXT.AIA_CB_COMPONENT_EXCL ex,
                cs_businessunit bu
            WHERE
                st.tenantid = 'AIAS'
                AND st.processingUnitseq = :V_PROCESSINGUNITSEQ
                AND st.genericattribute19 IN ('LF', 'LN', 'UL')
                AND st.productid = ex.component_name
                AND ex.removedate = TO_DATE('22000101', 'yyyymmdd')
                AND st.genericattribute6 = '1'
                AND st.ponumber IS NOT NULL
                AND st.businessunitmap = bu.mask
                AND bu.name = :STR_BUNAME_FA
                AND st.compensationdate BETWEEN V_INCEPTION_START_DT AND V_INCEPTION_END_DT
        ) AS t
        ON (pol_ex.buname = t.buname
        AND pol_ex.ponumber = t.ponumber)
    WHEN NOT MATCHED THEN
        INSERT
            (BUNAME, PONUMBER, CYCLE_DATE, CREATE_DATE)
        VALUES
            (t.BUNAME, t.PONUMBER, TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE), CURRENT_TIMESTAMP);  /* ORIGSQL: sysdate */

    /* ORIGSQL: EXT.Log('updated EXT.AIA_CB_POLICY_EXCL for FA Agents; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('updated EXT.AIA_CB_POLICY_EXCL for FA Agents; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    --version 6 add by Amanda begin
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into EXT.AIA_CB_POLICY_EXCL pol_ex using (SELECT pol.BUNAME, pol.PONUMBER, pol.component_code FROM ext.vw_CB_PROJECTED_POLICY_MASTER pol where pol.CB_EXCLUDE_FLG= 1 and pol.buname = STR_BUNAME_FA and po(...) */
    MERGE INTO EXT.AIA_CB_POLICY_EXCL AS pol_ex
        USING
        (
            SELECT   /* ORIGSQL: (select pol.BUNAME, pol.PONUMBER, pol.component_code from ext.vw_CB_PROJECTED_POLICY_MASTER pol where pol.CB_EXCLUDE_FLG= 1 and pol.buname = STR_BUNAME_FA and pol.year = V_CB_YEAR and pol.quarter = V_CB_Q(...) */
                pol.BUNAME,
                pol.PONUMBER,
                pol.component_code
            FROM
                ext.VW_CB_PROJECTED_POLICY_MASTER pol
            WHERE
                pol.CB_EXCLUDE_FLG = 1
                AND pol.buname = :STR_BUNAME_FA
                AND pol.year = :V_CB_YEAR
                AND pol.quarter = :V_CB_QUARTER
        ) AS t
        ON (pol_ex.buname = t.buname
            AND pol_ex.ponumber = t.ponumber
        AND pol_ex.component_cd = t.component_code)
    WHEN NOT MATCHED THEN
        INSERT
            (BUNAME, PONUMBER, CYCLE_DATE, CREATE_DATE, component_cd)
        VALUES
            (t.BUNAME, t.PONUMBER, TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE), CURRENT_TIMESTAMP, t.component_code);  /* ORIGSQL: sysdate */

    /* ORIGSQL: EXT.Log('updated EXT.AIA_CB_POLICY_EXCL from policy master; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('updated EXT.AIA_CB_POLICY_EXCL from policy master; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    --version 6 add by Amanda end

    /* ORIGSQL: EXT.Log('SP_POLICY_EXCL_FA end') */
    CALL EXT.Log('SP_POLICY_EXCL_FA end');
END;
  PUBLIC PROCEDURE SP_IDENTIFY_POLICY_FA
(
    IN P_STR_CB_CYCLEDATE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                          /* ORIGSQL: P_STR_CB_CYCLEDATE IN VARCHAR2 */
    IN P_CB_NAME VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                /* ORIGSQL: P_CB_NAME IN VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN SEQUENTIAL EXECUTION
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/
    DECLARE DBMTK_TMPVAR_STRING_1 VARCHAR(5000); /*sapdbmtk-generated help variable*/
    -- DECLARE DBMTK_CTV_PROCID INT := sapdbmtk.sp_f_dbmtk_ctv_procid(); /*sapdbmtk-generated help variable*/--Deepan: Not Requied

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */ 
    DECLARE t_periodseq BIGINT;  /* ORIGSQL: t_periodseq periodseq_type; */
    DECLARE dt_cb_cycledate TIMESTAMP;  /* ORIGSQL: dt_cb_cycledate date; */

    -- date format for input cb cycle date
    DECLARE v_cb_period_FIELD_BUNAME VARCHAR(20);  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_YEAR VARCHAR(20);  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_QUARTER VARCHAR(20);  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_CB_QUARTER_NAME VARCHAR(50);  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_CB_CYCLEDATE TIMESTAMP;  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_CB_STARTDATE TIMESTAMP;  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_CB_ENDDATE TIMESTAMP;  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_INCEPTION_STARTDATE TIMESTAMP;  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_INCEPTION_ENDDATE TIMESTAMP;  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_CREATEDATE TIMESTAMP;  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_REMOVEDATE TIMESTAMP;  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_CB_NAME VARCHAR(20);  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */

    -- month period type seq 
    DECLARE int_bu_unit_map_sgp BIGINT;  /* ORIGSQL: int_bu_unit_map_sgp int; */

    /* ORIGSQL: for i in 1..t_periodseq.count loop */
    DECLARE i INT;
    DECLARE int_sg_calendar_seq BIGINT;/* NOT CONVERTED! */-- SG calendar seq
    DECLARE int_periodtype_month_seq BIGINT;
    DECLARE v_cb_period  row like EXT.AIA_cb_period;/* 


    /* initialize library variables, if not yet done */
    CALL init_session_global();

    /*
    Clawback Policy/Component list is just for Lumpsum calculation,
    which for user to review the policy/component that will be used as base table.
    After the BSC agent list and policy list for exclusion are ready in system.
    System will base on the credit and transaction to build up the policy/component list.
    
    // @input P_STR_CB_CYCLEDATE: cycle date with format yyyymmdd
    
    ************************************************
    Version     Create By       Create Date   Change
    ************************************************
    1           Zeno Zhao        20160510    Initial
    */

    /* TODO enter package declarations (types, exceptions, methods etc) here */

    -- define period seq of each month

    /*--- TYPE definition moved to  scripts/dbmtk_create_types.sqlscript ---
    ----- Converted type 'periodseq_type' to 'EXT.AIA_CB_CALCULATION_FA__SP_IDENTIFY_POLICY_FA__periodseq_type'
    TYPE periodseq_type IS TABLE OF cs_period.periodseq%TYPE;
    ---end of TYPE definition commented out---*/ 
    --row variable of the CS period. For each procedure call, there is one cb_period_record
    /* ORIGSQL: init */
    CALL init();

    ---------------- initial variables
    --get the batch number from batch status table
    --select 1 into V_BATCH_NO from EXT.AIA_CB_BATCH_STATUS bs;
    --V_BATCH_NO := 1;

    -- get calendar seq and period type seq 
    SELECT
        calendarseq
    INTO
        int_sg_calendar_seq
    FROM
        cs_calendar
    WHERE
        name = :STR_CALENDARNAME;

    SELECT
        periodtypeseq
    INTO
        int_periodtype_month_seq
    FROM
        cs_periodtype
    WHERE
        name = :STR_CALENDAR_TYPE;

    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_BUSINESSUNIT' not found */

    SELECT
        mask
    INTO
        int_bu_unit_map_sgp
    FROM
        cs_businessunit
    WHERE
        name = :STR_BUNAME_FA;

    /* ORIGSQL: EXT.Log('calendar seq: ' || to_char(int_sg_calendar_seq) || '; month seq: ' || to_char(int_periodtype_month_seq) || '; BU mask: ' || to_char(int_bu_unit_map_sgp)) */
    CALL EXT.Log('calendar seq: '|| IFNULL(TO_VARCHAR(:int_sg_calendar_seq),'') || '; month seq: '|| IFNULL(TO_VARCHAR(:int_periodtype_month_seq,'DD-MON-YY HH12:MI:SS.FF AM'),'') || '; BU mask: '|| IFNULL(TO_VARCHAR(:int_bu_unit_map_sgp),'') 
        /* ORIGSQL: to_char(int_sg_calendar_seq) */
        /* ORIGSQL: to_char(int_periodtype_month_seq) */
        );  /* ORIGSQL: to_char(int_bu_unit_map_sgp) */

    -- get cs_period record

    dt_cb_cycledate = TO_DATE(:P_STR_CB_CYCLEDATE, :STR_DATE_FORMAT_TYPE);

    SELECT
        BUNAME,
        YEAR,
        QUARTER,
        CB_QUARTER_NAME,
        CB_CYCLEDATE,
        CB_STARTDATE,
        CB_ENDDATE,
        INCEPTION_STARTDATE,
        INCEPTION_ENDDATE,
        CREATEDATE,
        REMOVEDATE,
        CB_NAME
    INTO
        v_cb_period_FIELD_BUNAME,
        v_cb_period_FIELD_YEAR,
        v_cb_period_FIELD_QUARTER,
        v_cb_period_FIELD_CB_QUARTER_NAME,
        v_cb_period_FIELD_CB_CYCLEDATE,
        v_cb_period_FIELD_CB_STARTDATE,
        v_cb_period_FIELD_CB_ENDDATE,
        v_cb_period_FIELD_INCEPTION_STARTDATE,
        v_cb_period_FIELD_INCEPTION_ENDDATE,
        v_cb_period_FIELD_CREATEDATE,
        v_cb_period_FIELD_REMOVEDATE,
        v_cb_period_FIELD_CB_NAME
    FROM
        EXT.AIA_cb_period
    WHERE
        cb_cycledate = :dt_cb_cycledate
        AND BUNAME = :STR_BUNAME_FA
        AND CB_NAME = :P_CB_NAME
        AND removedate = :DT_REMOVEDATE
    LIMIT 1;  /* ORIGSQL: rownum = 1 */

    /* ORIGSQL: EXT.Log('quarter ' || v_cb_period.cb_quarter_name) */
    CALL EXT.Log('quarter '|| IFNULL(:v_cb_period_FIELD_CB_QUARTER_NAME,''));

    ---------------- empty temp table
    /* ORIGSQL: EXT.Log('Empty temp tables for FA') */
    CALL EXT.Log('Empty temp tables for FA');

    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_CB_CREDITFILTER_TMP'; */
    /* ORIGSQL: truncate table EXT.AIA_CB_CREDITFILTER_TMP ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_CB_CREDITFILTER_TMP';

    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_CB_POLICY_INC_TMP'; */
    /* ORIGSQL: truncate table EXT.AIA_CB_POLICY_INC_TMP ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_CB_POLICY_INC_TMP';

    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_CB_CREDITFILTER'; */
    /* ORIGSQL: truncate table EXT.AIA_CB_CREDITFILTER ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_CB_CREDITFILTER';

    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_CB_SALESTRANSACTION'; */
    /* ORIGSQL: truncate table EXT.AIA_CB_SALESTRANSACTION ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_CB_SALESTRANSACTION';

    -- delete old records for rerun
    /* ORIGSQL: EXT.Log('Remove old record for rerun for FA Agents') */
    CALL EXT.Log('Remove old record for rerun for FA Agents');

    /* ORIGSQL: delete from EXT.AIA_CB_IDENTIFY_POLICY where buname=STR_BUNAME_FA and year=v_cb_period.YEAR and quarter=v_cb_period.QUARTER; */
    DELETE
    FROM
        EXT.AIA_CB_IDENTIFY_POLICY
    WHERE
        buname = :STR_BUNAME_FA
        AND year = :v_cb_period_FIELD_YEAR
        AND quarter = :v_cb_period_FIELD_QUARTER;

    /* ORIGSQL: EXT.Log('Delete from  EXT.AIA_CB_IDENTIFY_POLICY for FA Agents' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('Delete from  EXT.AIA_CB_IDENTIFY_POLICY for FA Agents'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    ------------------

    -- get all month period seq  
    /* ORIGSQL: select p.periodseq BULK COLLECT into t_periodseq from cs_period a, cs_period p where a.calendarseq=int_sg_calendar_seq and p.calendarseq=int_sg_calendar_seq and a.name in (v_cb_period.CB_QUARTER_NAME,(...) */
    dbmtk_bulk_tabvar_22527 =   
    SELECT
        p.periodseq AS periodseq
        /* ORIGSQL: BULK COLLECT into t_periodseq */
    FROM
        cs_period a,
        cs_period p
    WHERE
        a.calendarseq = :int_sg_calendar_seq
        AND p.calendarseq = :int_sg_calendar_seq
        --Revised by Win Tan for version 12 begin
        --and a.name=v_cb_period.cb_quarter_name
        AND a.name IN (:v_cb_period_FIELD_CB_QUARTER_NAME,
            MAP(:v_cb_period_FIELD_CB_QUARTER_NAME, 'Q4 2017', 'Q1 2017S', ''))  /* ORIGSQL: decode(v_cb_period.cb_quarter_name,'Q4 2017','Q1 2017S','') */
        --version 12 end
        AND p.periodtypeseq = :int_periodtype_month_seq
        AND p.startdate >= a.startdate
        AND p.enddate <= a.enddate
        AND p.startdate >= :v_cb_period_FIELD_INCEPTION_STARTDATE
        AND p.enddate <= TO_DATE(ADD_SECONDS(:v_cb_period_FIELD_INCEPTION_ENDDATE,(86400*1)))   /* ORIGSQL: v_cb_period.INCEPTION_ENDDATE + 1 */
        AND a.removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
        /* ORIGSQL: to_date('2200-01-01','yyyy-mm-dd') */
        AND p.removedate = to_date('2200-01-01','yyyy-mm-dd');--Cosimo
    /* ORIGSQL: to_date('2200-01-01','yyyy-mm-dd') */

--Deepan : not required, generated by conversion tool. Used alternate approach to load data to 
    -- t_periodseq = SELECT periodseq AS column_value, TO_INT(ROW_NUMBER() OVER ()) AS dbmtk_ix_col FROM :dbmtk_bulk_tabvar_22527;  /* ORIGSQL: SELECT-INTO..BULK COLLECT INTO..t_periodseq */
    -- t_dbmtk_ix_col = UNNEST(ARRAY_AGG(:t_periodseq.dbmtk_ix_col)) AS (dbmtk_ix_col); CALL sapdbmtk.sp_dbmtk_ctv_pushix('t_periodseq',:DBMTK_CTV_PROCID,:t_dbmtk_ix_col);

/*Deepan : not required, generated by conversion tool*/
    --------- For each month (partition), filter the select credit into the temp table
    -- FOR i IN 1 .. sapdbmtk.sp_f_dbmtk_ctv_count('t_periodseq',:DBMTK_CTV_PROCID)  /* ORIGSQL: t_periodseq.count */
    -- DO
    --     /* ORIGSQL: EXT.Log('peroid seq for FA: [' || to_char(i) || ']= ' || to_char(t_periodseq(i))) */
    --     DBMTK_TMPVAR_STRING_1 = 'peroid seq for FA: ['|| IFNULL(TO_VARCHAR(:i),'') || ']= '|| IFNULL(TO_VARCHAR(:t_periodseq.column_value[:i],NULL),'')   
    --     ;
    --     /* ORIGSQL: to_char(t_periodseq(i)) */
    --     /* ORIGSQL: to_char(i) */
    --     CALL EXT.Log(:DBMTK_TMPVAR_STRING_1);

        -- log

        -- Get credit record by month with cretia: agent is in date scope

        /* ORIGSQL: insert into EXT.AIA_CB_CREDITFILTER_TMP select cr.CREDITSEQ,cr.PAYEESEQ,cr.POSITIONSEQ,cr.SALESORDERSEQ,cr.SALESTRANSACTIONSEQ,cr.PERIODSEQ,cr.CREDITTYPESEQ,cr.NAME,cr.PIPELINERUNSEQ,cr.ORIGINTYPEID,cr.CO(...) */
        /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_CREDIT' not found */
        -- DBMTK_TMPVAR_INT_1 = :t_periodseq.column_value[:i];
        
        
        
   /*Deepan : Commented this out and used a different method below this */     
        -- INSERT INTO EXT.AIA_CB_CREDITFILTER_TMP
        --     /* ORIGSQL: select / *+ parallel * / */
        --     SELECT   /* ORIGSQL: select cr.CREDITSEQ,cr.PAYEESEQ,cr.POSITIONSEQ,cr.SALESORDERSEQ,cr.SALESTRANSACTIONSEQ,cr.PERIODSEQ,cr.CREDITTYPESEQ,cr.NAME,cr.PIPELINERUNSEQ,cr.ORIGINTYPEID,cr.COMPENSATIONDATE,cr.PIPELINERUNDATE,cr(...) */
        --         cr.CREDITSEQ,
        --         cr.PAYEESEQ,
        --         cr.POSITIONSEQ,
        --         cr.SALESORDERSEQ,
        --         cr.SALESTRANSACTIONSEQ,
        --         cr.PERIODSEQ,
        --         cr.CREDITTYPESEQ,
        --         cr.NAME,
        --         cr.PIPELINERUNSEQ,
        --         cr.ORIGINTYPEID,
        --         cr.COMPENSATIONDATE,
        --         cr.PIPELINERUNDATE,
        --         cr.BUSINESSUNITMAP,
        --         cr.PREADJUSTEDVALUE,
        --         cr.UNITTYPEFORPREADJUSTEDVALUE,
        --         cr.VALUE,
        --         cr.UNITTYPEFORVALUE,
        --         cr.RELEASEDATE,
        --         cr.RULESEQ,
        --         cr.ISHELD,
        --         cr.ISROLLABLE,
        --         cr.ROLLDATE,
        --         cr.REASONSEQ,
        --         cr.COMMENTS,
        --         cr.GENERICATTRIBUTE1,
        --         cr.GENERICATTRIBUTE2,
        --         cr.GENERICATTRIBUTE3,
        --         cr.GENERICATTRIBUTE4,
        --         cr.GENERICATTRIBUTE5,
        --         cr.GENERICATTRIBUTE6,
        --         cr.GENERICATTRIBUTE7,
        --         cr.GENERICATTRIBUTE8,
        --         cr.GENERICATTRIBUTE9,
        --         cr.GENERICATTRIBUTE10,
        --         cr.GENERICATTRIBUTE11,
        --         cr.GENERICATTRIBUTE12,
        --         cr.GENERICATTRIBUTE13,
        --         cr.GENERICATTRIBUTE14,
        --         cr.GENERICATTRIBUTE15,
        --         cr.GENERICATTRIBUTE16,
        --         cr.GENERICNUMBER1,
        --         cr.UNITTYPEFORGENERICNUMBER1,
        --         cr.GENERICNUMBER2,
        --         cr.UNITTYPEFORGENERICNUMBER2,
        --         cr.GENERICNUMBER3,
        --         cr.UNITTYPEFORGENERICNUMBER3,
        --         cr.GENERICNUMBER4,
        --         cr.UNITTYPEFORGENERICNUMBER4,
        --         cr.GENERICNUMBER5,
        --         cr.UNITTYPEFORGENERICNUMBER5,
        --         cr.GENERICNUMBER6,
        --         cr.UNITTYPEFORGENERICNUMBER6,
        --         cr.GENERICDATE1,
        --         cr.GENERICDATE2,
        --         cr.GENERICDATE3,
        --         cr.GENERICDATE4,
        --         cr.GENERICDATE5,
        --         cr.GENERICDATE6,
        --         cr.GENERICBOOLEAN1,
        --         cr.GENERICBOOLEAN2,
        --         cr.GENERICBOOLEAN3,
        --         cr.GENERICBOOLEAN4,
        --         cr.GENERICBOOLEAN5,
        --         cr.GENERICBOOLEAN6,
        --         cr.PROCESSINGUNITSEQ
        --     FROM
        --         cs_credit cr
        --     INNER JOIN
        --         EXT.AIA_CB_BSC_AGENT agt
        --         ON cr.GENERICATTRIBUTE12 = agt.AGENTCODE
        --     INNER JOIN
        --         (
        --             SELECT   /* ORIGSQL: (select distinct SOURCE_RULE_OUTPUT from EXT.AIA_CB_RULES_LOOKUP where buname = STR_BUNAME_FA and rule_type = 'CREDIT' and SOURCE_RULE_OUTPUT like '%\_DIRECT\_%' ESCAPE '\') */
        --                 DISTINCT
        --                 SOURCE_RULE_OUTPUT
        --             FROM
        --                 EXT.AIA_CB_RULES_LOOKUP
        --             WHERE
        --                 buname = :STR_BUNAME_FA
        --                 AND rule_type = 'CREDIT'
        --                 AND SOURCE_RULE_OUTPUT LIKE '%\_DIRECT\_%' ESCAPE '\'
        --         ) AS rl
        --         ON cr.name = rl.SOURCE_RULE_OUTPUT
        --         --    inner join cs_position POS on 'SGT'||AGT.AGENTCODE=POS.NAME
        --     WHERE
        --         cr.tenantid = 'AIAS'
        --         AND cr.processingUnitseq = :V_PROCESSINGUNITSEQ
        --         AND cr.periodseq = :DBMTK_TMPVAR_INT_1  /* ORIGSQL: t_periodseq(i) */
        --         AND agt.ENTITLEMENTPERCENT <> 1 -- not equal 100%
        --         AND agt.year = :v_cb_period_FIELD_YEAR --change to period year and quator instead of startdate/enddate
        --         AND agt.quarter = :v_cb_period_FIELD_QUARTER
        --         AND cr.compensationdate BETWEEN :v_cb_period_FIELD_INCEPTION_STARTDATE AND :v_cb_period_FIELD_INCEPTION_ENDDATE
        --         AND CR.businessunitmap = :int_bu_unit_map_sgp;-- and POS.GENERICATTRIBUTE6='AFA'
        -- -- AND POS.removedate = DT_REMOVEDATE
        -- -- AND POS.effectivestartdate <= AGT.ENDDATE
        -- -- AND POS.effectiveenddate   >  AGT.ENDDATE
        
/*Deepan : Alternate approach for the above functionality. Needs to be tested thoroughly*/        
INSERT INTO EXT.AIA_CB_CREDITFILTER_TMP
WITH FilteredPeriods AS (
    SELECT p.periodseq
    FROM cs_period a
    JOIN cs_period p ON a.calendarseq = p.calendarseq
        AND a.name IN (:v_cb_period.cb_quarter_name, 
                       CASE WHEN :v_cb_period.cb_quarter_name = 'Q4 2017' THEN 'Q1 2017S' ELSE '' END)
        AND p.periodtypeseq = :int_periodtype_month_seq
        AND p.startdate >= a.startdate
        AND p.enddate <= a.enddate
        AND p.startdate >= :v_cb_period.inception_startdate
        AND p.enddate <= ADD_DAYS(:v_cb_period.inception_enddate, 1)
        AND a.removedate = TO_DATE('2200-01-01', 'YYYY-MM-DD')
        AND p.removedate = TO_DATE('2200-01-01', 'YYYY-MM-DD')
),
CreditsToInsert AS (
    SELECT     cr.CREDITSEQ,
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
                cr.PROCESSINGUNITSEQ
    FROM cs_credit cr
    JOIN EXT.AIA_CB_BSC_AGENT agt ON cr.GENERICATTRIBUTE12 = agt.AGENTCODE
    JOIN FilteredPeriods fp ON cr.periodseq = fp.periodseq
    JOIN EXT.AIA_CB_RULES_LOOKUP rl ON cr.name = rl.SOURCE_RULE_OUTPUT
        AND rl.buname = :STR_BUNAME_FA
        AND rl.rule_type = 'CREDIT'
        AND rl.SOURCE_RULE_OUTPUT LIKE '%\_DIRECT\_%' ESCAPE '\'
    WHERE cr.tenantid = 'AIAS' 
      AND cr.processingUnitseq = :V_PROCESSINGUNITSEQ
      AND agt.ENTITLEMENTPERCENT <> 1
      AND agt.year = :v_cb_period.year
      AND agt.quarter = :v_cb_period.quarter
      AND cr.compensationdate BETWEEN :v_cb_period.inception_startdate AND :v_cb_period.inception_enddate
      AND CR.businessunitmap = :int_bu_unit_map_sgp
)
SELECT * FROM CreditsToInsert;

        /* ORIGSQL: EXT.Log('moth peroid seq for FA: [' || to_char(i) || '] ' || '; row count: ' || to_char(sql%rowcount)) */
        CALL EXT.Log('moth peroid seq for FA: ['|| IFNULL(TO_VARCHAR(:i),'') || '] '|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
            /* ORIGSQL: to_char(i) */
            );  /* ORIGSQL: to_char(sql%rowcount) */

        /* ORIGSQL: commit; */
        COMMIT;  /* ORIGSQL: end loop; */

    ----------- get target policy list

    /* ORIGSQL: insert into EXT.AIA_CB_POLICY_INC_TMP (ponumber, create_date, fhr_date) select distinct st.ponumber, sysdate, fhr.fhr_date from cs_salestransaction st inner join EXT.AIA_CB_CREDITFILTER_TMP cr on st.salestran(...) */
    INSERT INTO EXT.AIA_CB_POLICY_INC_TMP
        (
            ponumber, create_date, fhr_date
        )
        /* ORIGSQL: select / *+ parallel * / */
        SELECT   /* ORIGSQL: select distinct st.ponumber, sysdate, fhr.fhr_date from cs_salestransaction st inner join EXT.AIA_CB_CREDITFILTER_TMP cr on st.salestransactionseq = cr.salestransactionseq left join EXT.AIA_CB_POLICY_EXCL ex (...) */
            DISTINCT
            st.ponumber,
            CURRENT_TIMESTAMP,  /* ORIGSQL: sysdate */
            fhr.fhr_date
        FROM
            cs_salestransaction st
        INNER JOIN
            EXT.AIA_CB_CREDITFILTER_TMP cr
            ON st.salestransactionseq = cr.salestransactionseq
        LEFT OUTER JOIN
            EXT.AIA_CB_POLICY_EXCL ex
            ON st.ponumber = ex.ponumber --and ex.ponumber IS NULL
            AND IFNULL(ex.component_cd, st.productid) = st.productid --version 6 add by Amanda Wei
            /* ORIGSQL: nvl(ex.component_cd, st.productid) */
            AND EX.BUNAME = :STR_BUNAME_FA
        LEFT OUTER JOIN
            EXT.AIA_CB_POLICY_FHR_DATE fhr
            ON st.ponumber = fhr.ponumber
            AND FHR.BUNAME = :STR_BUNAME_FA
        WHERE
            st.tenantid = 'AIAS'
            AND st.processingUnitseq = :V_PROCESSINGUNITSEQ
            AND st.GENERICATTRIBUTE19 IN ('LF', 'LN', 'UL')
            AND ex.ponumber IS NULL;

    -- and EX.BUNAME=STR_BUNAME_FA -- Modified by Gopi for unittesting
    -- and FHR.BUNAME=STR_BUNAME_FA;

    /* ORIGSQL: EXT.Log('CB policy include records for FA Agents; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('CB policy include records for FA Agents; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    --  end if;

    /* ORIGSQL: commit; */
    COMMIT;

    --execute immediate 'analyze table EXT.AIA_CB_POLICY_INC_TMP compute statistic';

    ------------- insert into CB credit filter

    /* ORIGSQL: insert into EXT.AIA_CB_CREDITFILTER select cr.genericattribute12, cr.genericattribute14, cr.genericattribute1, cr.compensationdate, cr.positionseq, cr.genericdate2, cr.salestransactionseq, inc.fhr_date fr(...) */
    INSERT INTO EXT.AIA_CB_CREDITFILTER
        SELECT   /* ORIGSQL: select cr.genericattribute12, cr.genericattribute14, cr.genericattribute1, cr.compensationdate, cr.positionseq, cr.genericdate2, cr.salestransactionseq, inc.fhr_date from EXT.AIA_CB_CREDITFILTER_TMP cr in(...) */
            cr.genericattribute12,
            cr.genericattribute14,
            cr.genericattribute1,
            cr.compensationdate,
            cr.positionseq,
            cr.genericdate2,
            cr.salestransactionseq,
            inc.fhr_date
        FROM
            EXT.AIA_CB_CREDITFILTER_TMP cr
        INNER JOIN
            EXT.AIA_CB_POLICY_INC_TMP inc
            ON cr.genericattribute6 = inc.ponumber;

    /* ORIGSQL: EXT.Log('CB credit filter for FA; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('CB credit filter for FA; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    ------------- insert into CB transaction

    /* ORIGSQL: insert into EXT.AIA_CB_SALESTRANSACTION select cr.genericattribute12 as WRI_AGT_CODE, cr.genericattribute14 as CLASS, st.ponumber as PONUMBER, st.genericattribute23 as INSURED_NAME, st.genericattribute19 (...) */
    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_GASALESTRANSACTION' not found */
    INSERT INTO EXT.AIA_CB_SALESTRANSACTION
        SELECT   /* ORIGSQL: select cr.genericattribute12 as WRI_AGT_CODE, cr.genericattribute14 as CLASS, st.ponumber as PONUMBER, st.genericattribute23 as INSURED_NAME, st.genericattribute19 as CONTRACT_CAT, st.GENERICATTRIBUTE(...) */
            cr.genericattribute12 AS WRI_AGT_CODE,
            cr.genericattribute14 AS CLASS,
            st.ponumber AS PONUMBER,
            st.genericattribute23 AS INSURED_NAME,
            st.genericattribute19 AS CONTRACT_CAT,
            st.GENERICATTRIBUTE29 AS LIFE_NUMBER,
            st.GENERICATTRIBUTE30 AS COVERAGE_NUMBER,
            st.GENERICATTRIBUTE31 AS RIDER_NUMBER,
            cr.genericattribute1 AS COMPONENT_CODE,
            st.genericattribute3 AS COMPONENT_NAME,
            st.genericdate3 AS ISSUE_DATE,
            st.genericdate6 AS INCEPTION_DATE,
            st.genericdate2 AS RISK_COMMENCEMENT_DATE,
            cr.fhr_date AS FHR_DATE,
            MAP(st.GENERICATTRIBUTE6, '1', 'Y', 'N') AS BASE_RIDER_IND,  /* ORIGSQL: decode(st.GENERICATTRIBUTE6, '1', 'Y', 'N') */
            cr.compensationdate AS TRANSACTION_DATE,
            st.genericattribute1 AS PAYMENT_MODE,
            st.genericattribute5 AS POLICY_CURRENCY,
            st.salestransactionseq,
            cr.positionseq,
            cr.genericdate2 AS POLICY_ISSUE_DATE,
            gast.genericdate8 AS submitdate
        FROM
            cs_salestransaction st
        INNER JOIN
            EXT.AIA_CB_CREDITFILTER cr
            ON st.salestransactionseq = cr.salestransactionseq
        INNER JOIN
            cs_gasalestransaction gast
            ON st.salestransactionseq = gast.salestransactionseq
            AND gast.pagenumber = 0
        WHERE
            st.tenantid = 'AIAS'
            AND st.processingunitseq = :V_PROCESSINGUNITSEQ
            AND GREATEST(IFNULL(st.genericdate3, TO_DATE('19000101', 'yyyymmdd')),  /* ORIGSQL: nvl(st.genericdate3, to_date('19000101', 'yyyymmdd')) */
                IFNULL(st.genericdate6, TO_DATE('19000101', 'yyyymmdd')),  /* ORIGSQL: nvl(st.genericdate6, to_date('19000101', 'yyyymmdd')) */
                IFNULL(st.genericdate2, TO_DATE('19000101', 'yyyymmdd'))) BETWEEN  /* ORIGSQL: nvl(st.genericdate2, to_date('19000101', 'yyyymmdd')) */
            :v_cb_period_FIELD_INCEPTION_STARTDATE AND :v_cb_period_FIELD_INCEPTION_ENDDATE;

    /* ORIGSQL: EXT.Log('CB transaction for FA; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('CB transaction for FA; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: EXT.Log('CB transaction for FA; v_cb_period.inception_startdate: ' || v_cb_period.inception_startdate) */
    CALL EXT.Log('CB transaction for FA; v_cb_period.inception_startdate: '|| IFNULL(TO_VARCHAR(:v_cb_period_FIELD_INCEPTION_STARTDATE),''));

    /* ORIGSQL: EXT.Log('CB transaction for FA; v_cb_period.inception_enddate: ' || v_cb_period.inception_enddate) */
    CALL EXT.Log('CB transaction for FA; v_cb_period.inception_enddate: '|| IFNULL(TO_VARCHAR(:v_cb_period_FIELD_INCEPTION_ENDDATE),''));

    /* ORIGSQL: EXT.Log('CB transaction for FA; V_PROCESSINGUNITSEQ: ' || V_PROCESSINGUNITSEQ) */
    CALL EXT.Log('CB transaction for FA; V_PROCESSINGUNITSEQ: '|| IFNULL(TO_VARCHAR(:V_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: EXT.Log('CB transaction for FA; v_cb_period.year: ' || to_char(v_cb_period.year)) */
    CALL EXT.Log('CB transaction for FA; v_cb_period.year: '|| IFNULL(TO_VARCHAR(:v_cb_period_FIELD_YEAR,NULL),'') 
        );  /* ORIGSQL: to_char(v_cb_period.year) */

    /* ORIGSQL: EXT.Log('CB transaction for FA; v_cb_period.quarter: ' || to_char(v_cb_period.quarter)) */
    CALL EXT.Log('CB transaction for FA; v_cb_period.quarter: '|| IFNULL(TO_VARCHAR(:v_cb_period_FIELD_QUARTER,NULL),'') 
        );  /* ORIGSQL: to_char(v_cb_period.quarter) */

    /* ORIGSQL: EXT.Log('CB transaction for FA; DT_REMOVEDATE: ' || to_char(DT_REMOVEDATE)) */
    CALL EXT.Log('CB transaction for FA; DT_REMOVEDATE: '|| IFNULL(TO_VARCHAR(:DT_REMOVEDATE,'DD-MON-YY'),'') 
        );  /* ORIGSQL: to_char(DT_REMOVEDATE) */

    /* ORIGSQL: EXT.Log('CB transaction for FA; STR_BUNAME_FA: ' || to_char(STR_BUNAME_FA)) */
    CALL EXT.Log('CB transaction for FA; STR_BUNAME_FA: '|| IFNULL(TO_VARCHAR(:STR_BUNAME_FA,NULL),'') 
        );  /* ORIGSQL: to_char(STR_BUNAME_FA) */

    ------------- final insert into target
    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_TITLE' not found */
    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_GAPARTICIPANT' not found */

    /* ORIGSQL: insert into EXT.AIA_CB_IDENTIFY_POLICY(BUNAME,YEAR,QUARTER,WRI_DIST_CODE,WRI_DIST_NAME,WRI_DM_CODE,WRI_DM_NAME,WRI_AGY_CODE,WRI_AGY_NAME,WRI_AGY_LDR_CODE,WRI_AGY_LDR_NAME,WRI_AGT_CODE,WRI_AGT_NAME,FSC_TYP(...) */
    INSERT INTO EXT.AIA_CB_IDENTIFY_POLICY
        (
            BUNAME
            , YEAR
            , QUARTER
            , WRI_DIST_CODE
            , WRI_DIST_NAME
            , WRI_DM_CODE
            , WRI_DM_NAME
            , WRI_AGY_CODE
            , WRI_AGY_NAME
            , WRI_AGY_LDR_CODE
            , WRI_AGY_LDR_NAME
            , WRI_AGT_CODE
            , WRI_AGT_NAME
            , FSC_TYPE
            , RANK
            , CLASS
            , FSC_BSC_GRADE
            , FSC_BSC_PERCENTAGE
            , PONUMBER
            , INSURED_NAME
            , CONTRACT_CAT
            , LIFE_NUMBER
            , COVERAGE_NUMBER
            , RIDER_NUMBER
            , COMPONENT_CODE
            , COMPONENT_NAME
            , ISSUE_DATE
            , INCEPTION_DATE
            , RISK_COMMENCEMENT_DATE
            , FHR_DATE
            , BASE_RIDER_IND
            , TRANSACTION_DATE
            , PAYMENT_MODE
            , POLICY_CURRENCY
            , PROCESSING_PERIOD
            /* --,BATCH_NO */
            , CREATED_DATE
            , POLICYIDSEQ
            , SUBMITDATE
            , FAOB_AGT_CODE
        )
        SELECT   /* ORIGSQL: select curr_ip.BUNAME,curr_ip.YEAR,curr_ip.QUARTER,curr_ip.WRI_DIST_CODE,curr_ip.WRI_DIST_NAME,curr_ip.WRI_DM_CODE,curr_ip.WRI_DM_NAME,curr_ip.WRI_AGY_CODE,curr_ip.WRI_AGY_NAME,curr_ip.WRI_AGY_LDR_COD(...) */
            curr_ip.BUNAME,
            curr_ip.YEAR,
            curr_ip.QUARTER,
            curr_ip.WRI_DIST_CODE,
            curr_ip.WRI_DIST_NAME,
            curr_ip.WRI_DM_CODE,
            curr_ip.WRI_DM_NAME,
            curr_ip.WRI_AGY_CODE,
            curr_ip.WRI_AGY_NAME,
            curr_ip.WRI_AGY_LDR_CODE,
            curr_ip.WRI_AGY_LDR_NAME,
            curr_ip.WRI_AGT_CODE,
            curr_ip.WRI_AGT_NAME,
            curr_ip.FSC_TYPE,
            curr_ip.RANK,
            curr_ip.CLASS,
            curr_ip.FSC_BSC_GRADE,
            curr_ip.FSC_BSC_PERCENTAGE,
            curr_ip.PONUMBER,
            curr_ip.INSURED_NAME,
            curr_ip.CONTRACT_CAT,
            curr_ip.LIFE_NUMBER,
            curr_ip.COVERAGE_NUMBER,
            curr_ip.RIDER_NUMBER,
            curr_ip.COMPONENT_CODE,
            curr_ip.COMPONENT_NAME,
            curr_ip.ISSUE_DATE,
            curr_ip.INCEPTION_DATE,
            curr_ip.RISK_COMMENCEMENT_DATE,
            curr_ip.FHR_DATE,
            curr_ip.BASE_RIDER_IND,
            curr_ip.TRANSACTION_DATE,
            curr_ip.PAYMENT_MODE,
            curr_ip.POLICY_CURRENCY,
            curr_ip.PROCESSING_PERIOD
            /* --,BATCH_NO */, curr_ip.CREATED_DATE
            /* -- add sequence for id */, EXT.SEQ_CB_IDENTIFY_POLICY.NEXTVAL AS POLICYIDSEQ,  /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'NEXTVAL' (=reserved word in HANA) renamed to '"NEXTVAL"'; ensure all other references are renamed accordingly */
                                                                                                    /* RESOLVE: Identifier not found: Sequence not found: */
                                                                                                    /* ORIGSQL: SEQ_CB_IDENTIFY_POLICY.NEXTVAL */
            curr_ip.submitdate,
            curr_ip.FAOB_AGT_CODE
        FROM
            (
                /* ORIGSQL: select / *+ INDEX(cr IDX_CB_CREDITFILTER_TMP_1)* / */
                SELECT   /* ORIGSQL: (select STR_BUNAME_FA as BUNAME,v_cb_period.YEAR as YEAR,v_cb_period.QUARTER as QUARTER,pos_dis.GENERICATTRIBUTE3 as WRI_DIST_CODE,TRIM(par_dis.firstname||' '||par_dis.lastname) as WRI_DIST_NAME,pos_d(...) */
                    :STR_BUNAME_FA AS BUNAME,
                    :v_cb_period_FIELD_YEAR AS YEAR,
                    :v_cb_period_FIELD_QUARTER AS QUARTER
                    /* --writing district info. */, pos_dis.GENERICATTRIBUTE3 AS WRI_DIST_CODE,
                    TRIM(IFNULL(par_dis.firstname,'')||' '||IFNULL(par_dis.lastname,'')) AS WRI_DIST_NAME
                    /* --writing district leader info. */, pos_dis.genericattribute2 AS WRI_DM_CODE,
                    pos_dis.genericattribute7 AS WRI_DM_NAME
                    /* --writing agency info. */, SUBSTRING(pos_agy.name,4) AS WRI_AGY_CODE,  /* ORIGSQL: substr(pos_agy.name, 4) */
                    TRIM(IFNULL(par_agy.firstname,'')||' '||IFNULL(par_agy.lastname,'')) AS WRI_AGY_NAME
                    /* --writing agency leader info. */, pos_agt.GENERICATTRIBUTE2 AS WRI_AGY_LDR_CODE,
                    pos_agt.genericattribute7 AS WRI_AGY_LDR_NAME
                    /* --writing agent info. */, st.WRI_AGT_CODE,
                    TRIM(IFNULL(par_agt.firstname,'')||' '||IFNULL(par_agt.lastname,'')) AS WRI_AGT_NAME
                    /* --,'Normal FSC' as FSC_TYPE */, MAP(par_agt.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') AS FSC_TYPE,  /* ORIGSQL: decode(par_agt.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') */
                    title_agt.name AS RANK,
                    st.CLASS,
                    agt.bsc_grade AS FSC_BSC_GRADE,
                    agt.entitlementpercent AS FSC_BSC_PERCENTAGE,
                    st.ponumber AS PONUMBER /* ---??? */, st.INSURED_NAME,
                    st.CONTRACT_CAT,
                    st.LIFE_NUMBER,
                    st.COVERAGE_NUMBER,
                    st.RIDER_NUMBER,
                    st.COMPONENT_CODE,
                    st.COMPONENT_NAME,
                    st.ISSUE_DATE,
                    st.INCEPTION_DATE,
                    st.RISK_COMMENCEMENT_DATE,
                    st.fhr_date AS FHR_DATE,
                    st.BASE_RIDER_IND,
                    st.TRANSACTION_DATE,
                    st.PAYMENT_MODE,
                    st.POLICY_CURRENCY,
                    :dt_cb_cycledate AS PROCESSING_PERIOD /* --:22:23:24 */
                    /* --,V_BATCH_NO           as BATCH_NO   ---EXT.AIA_CB_BATCH_STATUS, :25:26:27:28:29 when to insert data into this table30313233 */, CURRENT_TIMESTAMP AS CREATED_DATE  /* ORIGSQL: sysdate */
                    /* -- Rank by key: policy number, comonent code, writing agent */, ROW_NUMBER() OVER (PARTITION BY st.ponumber, st.COMPONENT_CODE, st.WRI_AGT_CODE,
                    st.LIFE_NUMBER, st.COVERAGE_NUMBER, st.RIDER_NUMBER ORDER BY st.TRANSACTION_DATE DESC) AS rk
                    /* --,row_number() over(partition by st.ponumber, cr.genericattribute1,cr.genericattribute12  order by cr.compensationdate desc) rk */
                    /* --,1 as rk */, st.submitdate,
                    GA_PARTICIPANT.GENERICATTRIBUTE4 AS FAOB_AGT_CODE
                FROM
                    EXT.AIA_CB_SALESTRANSACTION st
                INNER JOIN
                    EXT.AIA_CB_BSC_AGENT agt
                    ON st.WRI_AGT_CODE = agt.AGENTCODE
                    AND agt.year = :v_cb_period_FIELD_YEAR
                    AND agt.quarter = :v_cb_period_FIELD_QUARTER
                INNER JOIN
                    cs_position pos_agy
                    ON pos_agy.tenantid = 'AIAS'
                    AND pos_agy.ruleelementownerseq = st.positionseq
                    AND pos_agy.removedate = :DT_REMOVEDATE
                    AND pos_agy.effectivestartdate <= st.POLICY_ISSUE_DATE
                    AND pos_agy.effectiveenddate > st.POLICY_ISSUE_DATE
                    -- and pos_agy.GENERICATTRIBUTE6='AFA'
                INNER JOIN
                    cs_participant par_agy
                    ON par_agy.tenantid = 'AIAS'
                    AND par_agy.PAYEESEQ = pos_agy.PAYEESEQ
                    AND par_agy.effectivestartdate <= st.POLICY_ISSUE_DATE
                    AND par_agy.effectiveenddate > st.POLICY_ISSUE_DATE
                    AND par_agy.removedate = :DT_REMOVEDATE
                INNER JOIN
                    cs_position pos_dis
                    ON pos_dis.tenantid = 'AIAS'
                    AND pos_dis.name = 'SGY'|| IFNULL(pos_agy.genericattribute3,'')
                    AND pos_dis.effectivestartdate <= st.POLICY_ISSUE_DATE
                    AND pos_dis.effectiveenddate > st.POLICY_ISSUE_DATE
                    AND pos_dis.removedate = :DT_REMOVEDATE
                INNER JOIN
                    cs_participant par_dis
                    ON par_dis.tenantid = 'AIAS'
                    AND par_dis.PAYEESEQ = pos_dis.PAYEESEQ
                    AND par_dis.effectivestartdate <= st.POLICY_ISSUE_DATE
                    AND par_dis.effectiveenddate > st.POLICY_ISSUE_DATE
                    AND par_dis.removedate = :DT_REMOVEDATE
                INNER JOIN
                    cs_position pos_agt
                    ON pos_agt.tenantid = 'AIAS'
                    AND 'SGT'||IFNULL(st.WRI_AGT_CODE,'') = pos_agt.name
                    AND pos_agt.effectivestartdate <= st.POLICY_ISSUE_DATE
                    AND pos_agt.effectiveenddate > st.POLICY_ISSUE_DATE
                    AND pos_agt.removedate = :DT_REMOVEDATE
                    AND pos_agt.GENERICATTRIBUTE6 = 'AFA'
                INNER JOIN
                    cs_participant par_agt
                    ON par_agt.tenantid = 'AIAS'
                    AND par_agt.payeeseq = pos_agt.PAYEESEQ
                    AND par_agt.effectivestartdate <= st.POLICY_ISSUE_DATE
                    AND par_agt.effectiveenddate > st.POLICY_ISSUE_DATE
                    AND par_agt.removedate = :DT_REMOVEDATE
                INNER JOIN
                    cs_title title_agt
                    ON title_agt.tenantid = 'AIAS'
                    AND title_agt.RULEELEMENTOWNERSEQ = pos_agt.TITLESEQ
                    AND title_agt.effectivestartdate <= st.POLICY_ISSUE_DATE
                    AND title_agt.effectiveenddate > st.POLICY_ISSUE_DATE
                    AND title_agt.REMOVEDATE = :DT_REMOVEDATE
                INNER JOIN
                    cs_gaparticipant ga_participant
                    ON ga_participant.tenantid = 'AIAS'
                    AND ga_participant.PAYEESEQ = pos_agy.PAYEESEQ
                    AND ga_participant.effectivestartdate <= st.POLICY_ISSUE_DATE
                    AND ga_participant.effectiveenddate > st.POLICY_ISSUE_DATE
                    AND ga_participant.removedate = :DT_REMOVEDATE
            ) AS curr_ip
            --if the component is being capture in previous quarters, then ignore to capture in current quarter
        LEFT OUTER JOIN
            EXT.AIA_CB_IDENTIFY_POLICY pre_ip
            ON (IFNULL(pre_ip.year,'') || ' '|| IFNULL(pre_ip.quarter,'')) < (IFNULL(curr_ip.year,'') || ' '|| IFNULL(curr_ip.quarter,''))
            AND pre_ip.ponumber = curr_ip.ponumber
            AND pre_ip.wri_agt_code = curr_ip.wri_agt_code
            AND pre_ip.life_number = curr_ip.life_number
            AND pre_ip.coverage_number = curr_ip.coverage_number
            AND pre_ip.rider_number = curr_ip.rider_number
            AND pre_ip.component_code = curr_ip.component_code
        WHERE
            curr_ip.rk = 1
            AND pre_ip.BUNAME IS NULL;

    /* ORIGSQL: EXT.Log('Final EXT.AIA_CB_IDENTIFY_POLICY for FA; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('Final EXT.AIA_CB_IDENTIFY_POLICY for FA; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;
END;
  PUBLIC PROCEDURE SP_EXEC_IDENTIFY_POLICY_FA
(
    IN P_STR_CB_CYCLEDATE VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                         /* ORIGSQL: P_STR_CB_CYCLEDATE IN VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE V_STR_CB_TYPE VARCHAR(20);  /* ORIGSQL: V_STR_CB_TYPE VARCHAR2(20); */
    DECLARE V_BATCH_NO DECIMAL(38,10);  /* ORIGSQL: V_BATCH_NO NUMBER; */
    DECLARE V_ID_FLAG DECIMAL(38,10);  /* ORIGSQL: V_ID_FLAG NUMBER; */
    DECLARE V_MESSAGE VARCHAR(2000);  /* ORIGSQL: V_MESSAGE VARCHAR2(2000); */
    DECLARE V_CB_YEAR VARCHAR(20);  /* ORIGSQL: V_CB_YEAR VARCHAR2(20); */
    DECLARE V_CB_QUARTER VARCHAR(20);  /* ORIGSQL: V_CB_QUARTER VARCHAR2(20); */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            V_MESSAGE = SUBSTRING(::SQL_ERROR_MESSAGE,1,2000);  /* ORIGSQL: SUBSTR(SQLERRM,1,2000) */

            /* ORIGSQL: EXT.Log(v_message) */
            CALL EXT.Log(:V_MESSAGE);
        END;

        /* ORIGSQL: init */
        CALL init();

        --check if the cycle date is the date for run identify policy and report
        SELECT
            COUNT(1) 
        INTO
            V_ID_FLAG
        FROM
            EXT.AIA_CB_PERIOD
        WHERE
            CB_CYCLEDATE = TO_DATE(:P_STR_CB_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
            AND cb_name = :STR_IDENTIFY
            AND buname = :STR_BUNAME_FA;

        IF :V_ID_FLAG > 0
        THEN
            /* ORIGSQL: SP_POLICY_EXCL_FA(P_STR_CB_CYCLEDATE, STR_IDENTIFY) */
            CALL SP_POLICY_EXCL_FA(:P_STR_CB_CYCLEDATE, :STR_IDENTIFY);

            /* ORIGSQL: SP_IDENTIFY_POLICY_FA (P_STR_CB_CYCLEDATE, STR_IDENTIFY) */
            CALL SP_IDENTIFY_POLICY_FA(:P_STR_CB_CYCLEDATE, :STR_IDENTIFY);

            /*         --get clawback year and quarter from clawback period table
                     select cbp.year, cbp.quarter
                     into V_CB_YEAR, V_CB_QUARTER
                     from EXT.AIA_cb_period cbp where cbp.cb_cycledate = to_date(P_STR_CB_CYCLEDATE, STR_DATE_FORMAT_TYPE);
            
                     --run report for identify policy result
                     PK_EXT.AIA_CB_REPORTS.SP_RPT_CB_MASTER_POLICY (V_CB_YEAR , V_CB_QUARTER);*/
        ELSE 
            /* ORIGSQL: EXT.Log(P_STR_CB_CYCLEDATE || ' is not the avaiable identify policy cycle date for FA') */
            CALL EXT.Log(IFNULL(:P_STR_CB_CYCLEDATE,'') || ' is not the avaiable identify policy cycle date for FA');
        END IF;
        ---catch exception
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
    END;
  PUBLIC PROCEDURE sp_create_batch_no_FA
(
    IN P_STR_CB_CYCLEDATE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                          /* ORIGSQL: P_STR_CB_CYCLEDATE IN VARCHAR2 */
    IN P_STR_CB_TYPE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                     /* ORIGSQL: P_STR_CB_TYPE IN VARCHAR2 */
    IN P_STR_CB_NAME VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                    /* ORIGSQL: P_STR_CB_NAME IN VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE V_BATCH_NO BIGINT;  /* ORIGSQL: V_BATCH_NO INTEGER; */
    DECLARE V_CB_QUARTER_NAME VARCHAR(50);  /* ORIGSQL: V_CB_QUARTER_NAME varchar2(50); */
    DECLARE V_CB_CYCLE_TYPE VARCHAR(50);  /* ORIGSQL: V_CB_CYCLE_TYPE varchar2(50); */
    DECLARE V_PREVIOUS_BATCH_NO BIGINT;  /* ORIGSQL: V_PREVIOUS_BATCH_NO INTEGER; */
    DECLARE STR_WEEKLY_CYCLE_DATE VARCHAR(50);  /* ORIGSQL: STR_WEEKLY_CYCLE_DATE varchar2(50); */
    DECLARE V_MONTHEND_FLAG BIGINT;  /* ORIGSQL: V_MONTHEND_FLAG INTEGER; */
    DECLARE NUM_OF_CYCLE_IND BIGINT;  /* ORIGSQL: NUM_OF_CYCLE_IND integer; */
    DECLARE NUM_LAST_BATCH BIGINT;  /* ORIGSQL: NUM_LAST_BATCH integer; */

    IF :P_STR_CB_TYPE = :STR_LUMPSUM
    THEN
        --get measurement quarter name for lumpsum clawback
        SELECT
            /* --cbp.cb_quarter_name */
            IFNULL(substring(cbp.cb_quarter_name,LOCATE(cbp.cb_quarter_name,' ',1,1) + 1),'') || ' ' ||  /* ORIGSQL: substr(cbp.cb_quarter_name, instr(cbp.cb_quarter_name, ' ') + 1) */
            IFNULL(substring(cbp.cb_quarter_name,1,LOCATE(cbp.cb_quarter_name,' ',1,1) - 1),'')  /* ORIGSQL: substr(cbp.cb_quarter_name, 1, instr(cbp.cb_quarter_name, ' ') - 1) */
        INTO
            V_CB_QUARTER_NAME
        FROM
            EXT.AIA_CB_PERIOD cbp
        WHERE
            CB_CYCLEDATE = TO_DATE(:P_STR_CB_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
            AND cbp.buname = :STR_BUNAME_FA
            AND cbp.cb_name = :P_STR_CB_NAME;

        --need to be revised
        --get current quarter by P_STR_CB_CYCLEDATE
    ELSEIF :P_STR_CB_TYPE = :STR_ONGOING
    AND :P_STR_CB_NAME = :STR_COMMISSION  /* ORIGSQL: ELSIF P_STR_CB_TYPE = STR_ONGOING and P_STR_CB_NAME = STR_COMMISSION then */
    THEN
        SELECT
            TO_VARCHAR(TO_DATE(:P_STR_CB_CYCLEDATE,:STR_DATE_FORMAT_TYPE),'yyyymm')  /* ORIGSQL: to_char(to_date(P_STR_CB_CYCLEDATE,STR_DATE_FORMAT_TYPE),'yyyymm') */
        INTO
            V_CB_QUARTER_NAME
        FROM
            SYS.DUMMY;  /* ORIGSQL: FROM dual ; */
    ELSEIF :P_STR_CB_TYPE = :STR_ONGOING
    AND :P_STR_CB_NAME = :STR_COMPENSATION  /* ORIGSQL: ELSIF P_STR_CB_TYPE = STR_ONGOING and P_STR_CB_NAME = STR_COMPENSATION then */
    THEN
        SELECT
            TO_VARCHAR(TO_DATE(:P_STR_CB_CYCLEDATE,:STR_DATE_FORMAT_TYPE),'yyyymm')  /* ORIGSQL: to_char(to_date(P_STR_CB_CYCLEDATE,STR_DATE_FORMAT_TYPE),'yyyymm') */
        INTO
            V_CB_QUARTER_NAME
        FROM
            SYS.DUMMY;  /* ORIGSQL: FROM dual ; */
    END IF;

    --get last batch number of batch

    SELECT
        IFNULL(MAX(t.batchnum),0)  /* ORIGSQL: nvl(max(t.batchnum),0) */
    INTO
        V_PREVIOUS_BATCH_NO
    FROM
        EXT.AIA_CB_BATCH_STATUS t
    WHERE
        t.buname = :STR_BUNAME_FA
        AND t.clawbacktype = :P_STR_CB_TYPE
        AND t.clawbackname = :P_STR_CB_NAME
        AND t.cb_quarter_name = :V_CB_QUARTER_NAME;

    --and t.status in (STR_STATUS_FAIL, STR_STATUS_COMPLETED_SP, STR_STATUS_COMPLETED_SH);

    --Log('V_PREVIOUS_BATCH_NO: ' || V_PREVIOUS_BATCH_NO);

    --get batch number by max(batch number) + 1

    SELECT
        IFNULL(MAX(batchnum),0) + 1  /* ORIGSQL: nvl(max(batchnum),0) */
    INTO
        V_BATCH_NO
    FROM
        EXT.AIA_CB_BATCH_STATUS;

    --Log('V_BATCH_NO: ' || V_BATCH_NO);

    --update the column islatest for previous cycle
    IF :V_PREVIOUS_BATCH_NO > 0
    THEN 
        /* ORIGSQL: update EXT.AIA_CB_BATCH_STATUS cbs SET islatest = 'N' where cbs.batchnum = V_PREVIOUS_BATCH_NO; */
        UPDATE EXT.AIA_CB_BATCH_STATUS cbs
            SET
            /* ORIGSQL: islatest = */
            islatest = 'N' 
        WHERE
            cbs.batchnum = :V_PREVIOUS_BATCH_NO;

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    --insert new cycle record

    /* ORIGSQL: insert into EXT.AIA_CB_BATCH_STATUS (batchnum, BUNAME, cb_quarter_name, status, isactive, islatest, ispopulated, cycledate, clawbackname, clawbacktype, createdate, updatedate) values (V_BATCH_NO, STR_BUNA(...) */
    INSERT INTO EXT.AIA_CB_BATCH_STATUS
        (
            batchnum,
            BUNAME,
            cb_quarter_name,
            status,
            isactive,
            islatest,
            ispopulated,
            cycledate,
            clawbackname,
            clawbacktype,
            createdate,
            updatedate
        )
    VALUES
        (
                :V_BATCH_NO,
                :STR_BUNAME_FA,
                :V_CB_QUARTER_NAME,
                :STR_STATUS_START,
                'Y',
                'Y',
                'N',
                TO_DATE(:P_STR_CB_CYCLEDATE, :STR_DATE_FORMAT_TYPE),
                :P_STR_CB_NAME,
                :P_STR_CB_TYPE,
                CURRENT_TIMESTAMP,  /* ORIGSQL: sysdate */
                ''
        );

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: EXT.Log('V_BATCH_NO for FA: ' || V_BATCH_NO) */
    CALL EXT.Log('V_BATCH_NO for FA: '|| IFNULL(TO_VARCHAR(:V_BATCH_NO),''));

    /* ORIGSQL: EXT.Log('V_CB_QUARTER_NAME for FA: ' || V_CB_QUARTER_NAME) */
    CALL EXT.Log('V_CB_QUARTER_NAME for FA: '|| IFNULL(:V_CB_QUARTER_NAME,''));
END;
  PUBLIC FUNCTION fn_get_batch_no_fa
(
    IN P_STR_CYCLEDATE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                       /* ORIGSQL: P_STR_CYCLEDATE IN VARCHAR2 */
    IN P_CB_NAME VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                 /* ORIGSQL: P_CB_NAME IN VARCHAR2 */
    IN P_CB_TYPE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                 /* ORIGSQL: P_CB_TYPE IN VARCHAR2 */
    IN P_STATUS VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                               /* ORIGSQL: P_STATUS IN VARCHAR2 */
)
RETURNS dbmtk_function_result DECIMAL(38,10)   /* ORIGSQL: return number */
SQL SECURITY DEFINER
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_batch_no BIGINT;  /* ORIGSQL: v_batch_no integer; */

    SELECT
        IFNULL(MAX(cbs.batchnum), 0)  /* ORIGSQL: nvl(max(cbs.batchnum), 0) */
    INTO
        v_batch_no
    FROM
        EXT.AIA_CB_BATCH_STATUS cbs
    WHERE
        TO_VARCHAR(cbs.cycledate,'yyyymm') =  /* ORIGSQL: to_char(cbs.cycledate, 'yyyymm') */
        TO_VARCHAR(TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE),'yyyymm')  /* ORIGSQL: to_char(to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE), 'yyyymm') */
        AND cbs.status = :P_STATUS
        AND cbs.clawbacktype = :P_CB_TYPE
        AND cbs.clawbackname = :P_CB_NAME
        AND cbs.islatest = 'Y'
        AND cbs.buname = :STR_BUNAME_FA;

    dbmtk_function_result = :v_batch_no;
    RETURN;
END;
  PUBLIC PROCEDURE SP_TRACE_FORWARD_COMMISSION_FA
(
    IN P_STR_CYCLEDATE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                       /* ORIGSQL: P_STR_CYCLEDATE IN VARCHAR2 */
    IN P_STR_TYPE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                  /* ORIGSQL: P_STR_TYPE IN VARCHAR2 */
    IN P_BATCH_NO BIGINT     /* ORIGSQL: P_BATCH_NO IN INTEGER */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    -- DECLARE DBMTK_CTV_PROCID INT := sapdbmtk.sp_f_dbmtk_ctv_procid(); /*sapdbmtk-generated help variable*/--Deepan : not required

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE V_CAL_PERIOD VARCHAR(30);  /* ORIGSQL: V_CAL_PERIOD VARCHAR2(30); */

    --measurement quarter
    DECLARE DT_CB_START_DATE TIMESTAMP;  /* ORIGSQL: DT_CB_START_DATE DATE; */
    DECLARE DT_CB_END_DATE TIMESTAMP;  /* ORIGSQL: DT_CB_END_DATE DATE; */
    DECLARE DT_INCEPTION_START_DATE TIMESTAMP;  /* ORIGSQL: DT_INCEPTION_START_DATE DATE; */
    DECLARE DT_INCEPTION_END_DATE TIMESTAMP;  /* ORIGSQL: DT_INCEPTION_END_DATE DATE; */
    DECLARE DT_WEEKLY_START_DATE TIMESTAMP;  /* ORIGSQL: DT_WEEKLY_START_DATE DATE; */
    DECLARE DT_WEEKLY_END_DATE TIMESTAMP;  /* ORIGSQL: DT_WEEKLY_END_DATE DATE; */
    DECLARE DT_ONGOING_START_DATE TIMESTAMP;  /* ORIGSQL: DT_ONGOING_START_DATE DATE; */
    DECLARE DT_ONGOING_END_DATE TIMESTAMP;  /* ORIGSQL: DT_ONGOING_END_DATE DATE; */

    --NUM_OF_CYCLE_IND integer;
    DECLARE v_cb_period_FIELD_BUNAME VARCHAR(20);  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_YEAR VARCHAR(20);  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_QUARTER VARCHAR(20);  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_CB_QUARTER_NAME VARCHAR(50);  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_CB_CYCLEDATE TIMESTAMP;  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_CB_STARTDATE TIMESTAMP;  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_CB_ENDDATE TIMESTAMP;  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_INCEPTION_STARTDATE TIMESTAMP;  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_INCEPTION_ENDDATE TIMESTAMP;  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_CREATEDATE TIMESTAMP;  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_REMOVEDATE TIMESTAMP;  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE v_cb_period_FIELD_CB_NAME VARCHAR(20);  /* ORIGSQL: v_cb_period EXT.AIA_cb_period%rowtype; */
    DECLARE vSQL VARCHAR(4000);  /* ORIGSQL: vSQL varchar2(4000); */
    DECLARE vCalendarseq BIGINT;  /* ORIGSQL: vCalendarseq integer; */
    DECLARE vPertypeSeq BIGINT;  /* ORIGSQL: vPertypeSeq integer; */

    DECLARE t_periodseq BIGINT;  /* ORIGSQL: t_periodseq periodseq_type; */
    DECLARE vOngoingperiod DECIMAL(38,10);  /* ORIGSQL: vOngoingperiod number; */
    DECLARE vOngoingendperiod DECIMAL(38,10);  /* ORIGSQL: vOngoingendperiod number; */

    /* initialize library variables, if not yet done */
    CALL init_session_global();

    /*--- TYPE definition moved to  scripts/dbmtk_create_types.sqlscript ---
    ----- Converted type 'periodseq_type' to 'EXT.AIA_CB_CALCULATION_FA__SP_TRACE_FORWARD_COMMISSION_FA__periodseq_type'
    TYPE periodseq_type IS TABLE OF cs_period.periodseq%TYPE;
    ---end of TYPE definition commented out---*/ 

    /* ORIGSQL: init */
    CALL init();

    --update status
    /* ORIGSQL: sp_update_batch_status (P_BATCH_NO,'processing') */
    CALL sp_update_batch_status(:P_BATCH_NO, 'processing'); 

    SELECT
        calendarseq
    INTO
        vCalendarseq
    FROM
        cs_calendar
    WHERE
        removedate = :DT_REMOVEDATE
        AND name = 'AIA Singapore Calendar';

    SELECT
        periodtypeseq
    INTO
        vPertypeSeq
    FROM
        cs_periodtype
    WHERE
        removedate = :DT_REMOVEDATE
        AND name = 'month';

    /*
    --if the input parameter for cycledate is not exist in EXT.AIA_CB_PERIOD, the program will end.
    select count(1)
      into NUM_OF_CYCLE_IND
      from EXT.AIA_CB_PERIOD cbp
     where CB_CYCLEDATE = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE);
    
    if NUM_OF_CYCLE_IND = 0 then
      EXT.Log(P_STR_CYCLEDATE || ' is not the eligible cycle date.');
      return;
    END IF; */

    /* ORIGSQL: EXT.Log('SP_TRACE_FORWARD_COMMISSION start for FA') */
    CALL EXT.Log('SP_TRACE_FORWARD_COMMISSION start for FA');
    /*  select p.periodseq --BULK COLLECT into t_periodseq
      from cs_period a, cs_period p
      where a.calendarseq=2251799813685250
     and p.calendarseq=2251799813685250
     and a.name=V_CAL_PERIOD--'Q3 2016'
     and p.periodtypeseq = 2814749767106561
     and p.startdate>=a.startdate
     and p.enddate <=a.enddate;
    */

    --get cycle date for weekly payment
    --weekly payment start date

    SELECT
        TO_DATE(TXT_KEY_VALUE, :STR_DATE_FORMAT_TYPE)
    INTO
        DT_WEEKLY_START_DATE
    FROM
        EXT.IN_ETL_CONTROL
    WHERE
        txt_key_string = 'PAYMENT_START_DATE_WEEKLY';

    --weekly payment end date

    SELECT
        TO_DATE(TXT_KEY_VALUE, :STR_DATE_FORMAT_TYPE)
    INTO
        DT_WEEKLY_END_DATE
    FROM
        EXT.IN_ETL_CONTROL
    WHERE
        txt_key_string = 'PAYMENT_END_DATE_WEEKLY';

    /* ORIGSQL: EXT.Log('For FA DT_WEEKLY_START_DATE = ' || DT_WEEKLY_START_DATE) */
    CALL EXT.Log('For FA DT_WEEKLY_START_DATE = '|| IFNULL(TO_VARCHAR(:DT_WEEKLY_START_DATE),''));

    /* ORIGSQL: EXT.Log('For FA DT_WEEKLY_END_DATE = ' || DT_WEEKLY_END_DATE) */
    CALL EXT.Log('For FA DT_WEEKLY_END_DATE = '|| IFNULL(TO_VARCHAR(:DT_WEEKLY_END_DATE),''));
    IF :P_STR_TYPE = :STR_LUMPSUM
    THEN
        SELECT
            BUNAME,
            YEAR,
            QUARTER,
            CB_QUARTER_NAME,
            CB_CYCLEDATE,
            CB_STARTDATE,
            CB_ENDDATE,
            INCEPTION_STARTDATE,
            INCEPTION_ENDDATE,
            CREATEDATE,
            REMOVEDATE,
            CB_NAME
        INTO
            v_cb_period_FIELD_BUNAME,
            v_cb_period_FIELD_YEAR,
            v_cb_period_FIELD_QUARTER,
            v_cb_period_FIELD_CB_QUARTER_NAME,
            v_cb_period_FIELD_CB_CYCLEDATE,
            v_cb_period_FIELD_CB_STARTDATE,
            v_cb_period_FIELD_CB_ENDDATE,
            v_cb_period_FIELD_INCEPTION_STARTDATE,
            v_cb_period_FIELD_INCEPTION_ENDDATE,
            v_cb_period_FIELD_CREATEDATE,
            v_cb_period_FIELD_REMOVEDATE,
            v_cb_period_FIELD_CB_NAME
        FROM
            EXT.AIA_cb_period
        WHERE
            cb_cycledate = TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
            AND cb_name = :STR_COMMISSION
            AND BUNAME = :STR_BUNAME_FA;

        SELECT
            IFNULL(cbp.quarter,'') || ' ' || IFNULL(cbp.year,''),
            cbp.cb_startdate,
            cbp.cb_enddate,
            cbp.inception_startdate,
            cbp.inception_enddate
        INTO
            V_CAL_PERIOD,
            DT_CB_START_DATE,
            DT_CB_END_DATE,
            DT_INCEPTION_START_DATE,
            DT_INCEPTION_END_DATE
        FROM
            EXT.AIA_CB_PERIOD cbp
        WHERE
            CB_CYCLEDATE = TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
            AND cb_name = :STR_COMMISSION
            AND cbp.BUNAME = :STR_BUNAME_FA;

        /* ORIGSQL: EXT.Log('For FA DT_LUMPSUM_START_DATE = ' || DT_CB_START_DATE) */
        CALL EXT.Log('For FA DT_LUMPSUM_START_DATE = '|| IFNULL(TO_VARCHAR(:DT_CB_START_DATE),''));

        /* ORIGSQL: EXT.Log('For FA DT_LUMPSUM_END_DATE = ' || DT_CB_END_DATE) */
        CALL EXT.Log('For FA DT_LUMPSUM_END_DATE = '|| IFNULL(TO_VARCHAR(:DT_CB_END_DATE),''));
        -- Get the periodseqs for lumpsum period  
        /* ORIGSQL: select periodseq BULK COLLECT into t_periodseq from cs_period csp inner join cs_periodtype pt on csp.periodtypeseq = pt.periodtypeseq where csp.startdate >= DT_CB_START_DATE and csp.enddate <= DT_CB_E(...) */
        dbmtk_bulk_tabvar_22535 =   
        SELECT
            periodseq
            /* ORIGSQL: BULK COLLECT into t_periodseq */
        FROM
            cs_period csp
        INNER JOIN
            cs_periodtype pt
            ON csp.periodtypeseq = pt.periodtypeseq
        WHERE
            csp.startdate >= :DT_CB_START_DATE
            AND csp.enddate <= TO_DATE(ADD_SECONDS(:DT_CB_END_DATE,(86400*1)))   /* ORIGSQL: DT_CB_END_DATE + 1 */
            AND csp.removedate = :DT_REMOVEDATE
            AND csp.calendarseq = :V_CALENDARSEQ
            AND pt.name = :STR_CALENDAR_TYPE;

/*Deepan : t_periodseq and t_dbmtk_ix_col are not require*/
        -- t_periodseq = SELECT periodseq AS column_value, TO_INT(ROW_NUMBER() OVER ()) AS dbmtk_ix_col FROM :dbmtk_bulk_tabvar_22535;  /* ORIGSQL: SELECT-INTO..BULK COLLECT INTO..t_periodseq */
        -- t_dbmtk_ix_col = UNNEST(ARRAY_AGG(:t_periodseq.dbmtk_ix_col)) AS (dbmtk_ix_col); CALL sapdbmtk.sp_dbmtk_ctv_pushix('t_periodseq',:DBMTK_CTV_PROCID,:t_dbmtk_ix_col);

        /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_comls_period'; */
        /* ORIGSQL: truncate table EXT.AIA_tmp_comls_period ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_period';

        /* ORIGSQL: insert into EXT.AIA_tmp_comls_period select periodseq from cs_period csp inner join cs_periodtype pt on csp.periodtypeseq = pt.periodtypeseq where csp.startdate >= DT_CB_START_DATE and csp.enddate <= DT_C(...) */
        INSERT INTO EXT.AIA_tmp_comls_period
            SELECT   /* ORIGSQL: select periodseq from cs_period csp inner join cs_periodtype pt on csp.periodtypeseq = pt.periodtypeseq where csp.startdate >= DT_CB_START_DATE and csp.enddate <= DT_CB_END_DATE + 1 and csp.removedate(...) */
                periodseq
            FROM
                cs_period csp
            INNER JOIN
                cs_periodtype pt
                ON csp.periodtypeseq = pt.periodtypeseq
            WHERE
                csp.startdate >= :DT_CB_START_DATE
                AND csp.enddate <= TO_DATE(ADD_SECONDS(:DT_CB_END_DATE,(86400*1)))   /* ORIGSQL: DT_CB_END_DATE + 1 */
                AND csp.removedate = :DT_REMOVEDATE
                AND csp.calendarseq = :V_CALENDARSEQ
                AND pt.name = :STR_CALENDAR_TYPE;

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD for FA, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO) */
        CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD for FA, '|| 'clawback type = '|| IFNULL(:P_STR_TYPE,'') ||', batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO),''));
        /*
        for i in 1..t_periodseq.count loop
        
        --for lumpsum commission trace forward
        insert /*+ APPEND   into EXT.AIA_CB_TRACE_FORWARD
        select STR_BUNAME as BUNAME,
               ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
               ip.ponumber as POLICY_NUMBER,
               ip.policyidseq as POLICYIDSEQ,
               pm.positionseq as PAYEE_SEQ,
               substr(dep_pos.name, 4) as PAYEE_CODE,
               crd.genericattribute12 as PAYOR_CODE,
               ip.life_number as LIFE_NUMBER,
               ip.coverage_number as COVERAGE_NUMBER,
               ip.rider_number as RIDER_NUMBER,
               ip.component_code as COMPONENT_CODE,
               ip.component_name as COMPONENT_NAME,
               ip.base_rider_ind as BASE_RIDER_IND,
               crd.compensationdate as TRANSACTION_DATE,
               --to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE) as TRANSACTION_DATE,
               TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
               --clawback type = 'Lumpsum'
               STR_LUMPSUM as CLAWBACK_TYPE,
               --clawback name = 'Commission'
               --STR_CB_NAME            as CLAWBACK_NAME,
               rl.CLAWBACK_NAME       as CLAWBACK_NAME,
               ct.credittypeid        as CREDITTYPE,
               crd.creditseq          as CREDITSEQ,
               crd.name               as CREDIT_NAME,
               crd.value              as CREDIT_VALUE,
               pm.measurementseq      as PM_SEQ,
               pm.name                as PM_NAME,
               pct.contributionvalue  as PM_CONTRIBUTE_VALUE,
               1                      as PM_RATE,
               dep.depositseq         as DEPOSITSEQ,
               dep.name               as DEPOSIT_NAME,
               dep.value              as DEPOSIT_VALUE,
               crd.periodseq          as PERIODSEQ,
               st.salestransactionseq as SALESTRANSACTIONSEQ,
               crd.genericattribute2  as PRODUCT_NAME,
               crd.genericnumber1     as POLICY_YEAR,
               st.genericnumber2      as COMMISSION_RATE,
               st.genericdate4        as PAID_TO_DATE,
               P_BATCH_NO             as BATCH_NUMBER,
               sysdate                as CREATED_DATE
          FROM CS_SALESTRANSACTION st
         inner join cs_period p
            on      st.compensationdate>=p.startdate and st.compensationdate<p.enddate and p.removedate>sysdate and p.calendarseq=2251799813685250
         inner join CS_CREDIT crd
            on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
         and crd.periodseq=p.periodseq
         inner join CS_PMCREDITTRACE pct
            on crd.CREDITSEQ = pct.CREDITSEQ
         inner join CS_MEASUREMENT pm
            on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
         inner join cs_depositpmtrace dpt
            on pm.measurementseq = dpt.measurementseq
         inner join cs_deposit dep
            on dep.depositseq = dpt.depositseq
         inner join cs_position dep_pos
            on dep.positionseq = dep_pos.ruleelementownerseq
         and dep_pos.removedate = DT_REMOVEDATE
         and dep_pos.effectivestartdate <= crd.genericdate2
         and dep_pos.effectiveenddate > crd.genericdate2
         inner join CS_CREDITTYPE ct
            on crd.CREDITTYPESEQ = ct.DATATYPESEQ
         and ct.Removedate = DT_REMOVEDATE
         inner join EXT.AIA_CB_IDENTIFY_POLICY ip
            on ip.BUNAME = STR_BUNAME
         AND st.PONUMBER = ip.PONUMBER
         AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
         AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
         AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
         AND st.PRODUCTID = ip.COMPONENT_CODE
         and crd.genericattribute12 = ip.wri_agt_code
         and ip.quarter || ' ' || ip.year = V_CAL_PERIOD
           --check if the deposit position is same as writing agent
         and dep_pos.name = 'SGT' || ip.wri_agt_code
         inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
                           from EXT.AIA_CB_RULES_LOOKUP
                          where RULE_TYPE = 'PM'
             AND CLAWBACK_NAME in (STR_COMMISSION,STR_GST_COMMISSION)) rl
            on pm.NAME = rl.SOURCE_RULE_OUTPUT
         WHERE st.tenantid='AIAS' and crd.tenantid='AIAS' and pm.tenantid='AIAS'
         and pct.tenantid='AIAS' and dpt.tenantid='AIAS'
         and pct.PROCESSINGUNITSEQ= V_PROCESSINGUNITSEQ
         and pct.TARGETPERIODSEQ=pm.periodseq
         and dpt.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
         and st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
         and crd.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
         and pm.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
         and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
         AND st.BUSINESSUNITMAP = 1
         and crd.genericattribute16 not in ('RO', 'RNO')
         and  dep.periodseq =  t_periodseq(i)
             ;
        */

        /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_Comls_Step0'; */
        /* ORIGSQL: truncate table EXT.AIA_tmp_Comls_Step0 ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_Comls_Step0';

        /* ORIGSQL: insert into EXT.AIA_tmp_Comls_Step0 (SALESTRANSACTIONSEQ, WRI_AGT_CODE_ORIG, CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE, COMPONENT_NAME, BAS(...) */
        INSERT INTO EXT.AIA_tmp_Comls_Step0
            (
                SALESTRANSACTIONSEQ,
                WRI_AGT_CODE_ORIG,
                CALCULATION_PERIOD,
                POLICY_NUMBER,
                POLICYIDSEQ,
                LIFE_NUMBER,
                COVERAGE_NUMBER,
                RIDER_NUMBER,
                COMPONENT_CODE,
                COMPONENT_NAME,
                BASE_RIDER_IND,
                COMMISSION_RATE,
                PAID_TO_DATE,
                WRI_AGT_CODE,
                GENERICATTRIBUTE10 /* --version 6 update by Amanda Wei */,
                GA26_WRI_AGT2 /* --version 10 for share agent2 */,
                QTRYR,
                EXTENDEDDATE9,
                FAOB_AGT_CODE
            )
            --Added by Gopi, to consider the migrated policies
            /* ORIGSQL: select / *+ leading(ip,st) * / */
            SELECT   /* ORIGSQL: select st.salestransactionseq, ip.wri_agt_code as wri_agt_code_ORIG, IP.QUARTER || ' ' || IP.YEAR AS CALCULATION_PERIOD, IP.PONUMBER AS POLICY_NUMBER, IP.POLICYIDSEQ AS POLICYIDSEQ, IP.LIFE_NUMBER AS (...) */
                st.salestransactionseq,
                ip.wri_agt_code AS wri_agt_code_ORIG,
                IFNULL(IP.QUARTER,'')
                || ' '
                || IFNULL(IP.YEAR,'') AS CALCULATION_PERIOD,
                IP.PONUMBER AS POLICY_NUMBER,
                IP.POLICYIDSEQ AS POLICYIDSEQ,
                IP.LIFE_NUMBER AS LIFE_NUMBER,
                IP.COVERAGE_NUMBER AS COVERAGE_NUMBER,
                IP.RIDER_NUMBER AS RIDER_NUMBER,
                IP.COMPONENT_CODE AS COMPONENT_CODE,
                IP.COMPONENT_NAME AS COMPONENT_NAME,
                IP.BASE_RIDER_IND AS BASE_RIDER_IND,
                ST.GENERICNUMBER2 AS COMMISSION_RATE,
                ST.GENERICDATE4 AS PAID_TO_DATE,
                'SGT'||IFNULL(IP.WRI_AGT_CODE,'') AS WRI_AGT_CODE,
                'SGT'|| IFNULL(st.GENERICATTRIBUTE10,'') /* --version 6 update by Amanda Wei */, 'SGT'|| IFNULL(st.GENERICATTRIBUTE26,'') /* --version 10 for share agent2 */, IFNULL(IP.QUARTER,'')
                || ' '
                || IFNULL(IP.YEAR,'') AS QTRYR,
                GST.GENERICDATE9,
                IP.FAOB_AGT_CODE
            FROM
                cs_Salestransaction st
            INNER JOIN
                EXT.AIA_CB_IDENTIFY_POLICY IP
                ON 1 = 1
                AND IP.BUNAME = :STR_BUNAME_FA
                AND ST.PONUMBER = IP.PONUMBER
                AND ST.GENERICATTRIBUTE29 = IP.LIFE_NUMBER
                AND ST.GENERICATTRIBUTE30 = IP.COVERAGE_NUMBER
                AND ST.GENERICATTRIBUTE31 = IP.RIDER_NUMBER
                AND ST.PRODUCTID = IP.COMPONENT_CODE  --Added based on Endi request-Gopi -27062019
            INNER JOIN
                CS_GASALESTRANSACTION GST
                ---Added by Gopi, to consider the migrated policies --12092019
                ON gst.pagenumber = 0
                AND st.salestransactionseq = GST.salestransactionseq
            WHERE
                st.tenantid = 'AIAS'
                AND st.processingUnitseq = :V_PROCESSINGUNITSEQ
                --and st.compensationdate between '1-mar-2017' and '31-may-2017'
                AND st.compensationdate BETWEEN :DT_CB_START_DATE AND :DT_CB_END_DATE
                AND GST.tenantid = 'AIAS'
                AND gst.processingUnitseq = :V_PROCESSINGUNITSEQ
                AND gst.compensationdate BETWEEN :DT_CB_START_DATE AND :DT_CB_END_DATE;

        /* ORIGSQL: execute immediate 'TRUNCATE table EXT.AIA_tmp_comls_step1'; */
        /* ORIGSQL: TRUNCATE table EXT.AIA_tmp_comls_step1 ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_step1';

        /* ORIGSQL: insert into EXT.AIA_TMP_COMLS_step1 SELECT DISTINCT CRD.CREDITSEQ, CRD.SALESTRANSACTIONSEQ, ip.CALCULATION_PERIOD, ip.POLICY_NUMBER, ip.POLICYIDSEQ, ip.LIFE_NUMBER, ip.COVERAGE_NUMBER, ip.RIDER_NUMBER, IP(...) */
        INSERT INTO EXT.AIA_TMP_COMLS_step1
            /* ORIGSQL: SELECT / *+ leading(ip,crd) index(crd CS_CREDIT_TRANSACTIONSEQ) * / */
            SELECT   /* ORIGSQL: SELECT DISTINCT CRD.CREDITSEQ, CRD.SALESTRANSACTIONSEQ, ip.CALCULATION_PERIOD, ip.POLICY_NUMBER, ip.POLICYIDSEQ, ip.LIFE_NUMBER, ip.COVERAGE_NUMBER, ip.RIDER_NUMBER, IP.COMPONENT_CODE, IP.COMPONENT_NA(...) */
                DISTINCT
                CRD.CREDITSEQ,
                CRD.SALESTRANSACTIONSEQ,
                ip.CALCULATION_PERIOD,
                ip.POLICY_NUMBER,
                ip.POLICYIDSEQ,
                ip.LIFE_NUMBER,
                ip.COVERAGE_NUMBER,
                ip.RIDER_NUMBER,
                IP.COMPONENT_CODE,
                IP.COMPONENT_NAME,
                IP.BASE_RIDER_IND,
                CRD.COMPENSATIONDATE AS TRANSACTION_DATE,
                CRD.GENERICATTRIBUTE12 AS PAYOR_CODE,
                CT.CREDITTYPEID AS CREDITTYPE,
                CRD.NAME AS CREDIT_NAME,
                CRD.VALUE AS CREDIT_VALUE,
                CRD.PERIODSEQ AS PERIODSEQ,
                CRD.GENERICATTRIBUTE2 AS PRODUCT_NAME,
                CRD.GENERICNUMBER1 AS POLICY_YEAR,
                ip.COMMISSION_RATE AS COMMISSION_RATE,
                ip.PAID_TO_DATE AS PAID_TO_DATE,
                ip.WRI_AGT_CODE,
                ip.QTRYR,
                CRD.GENERICDATE2
            FROM
                CS_CREDIT CRD
            INNER JOIN
                EXT.AIA_TMP_COMLS_PERIOD P
                ON CRD.PERIODSEQ = P.PERIODSEQ
            INNER JOIN
                CS_CREDITTYPE CT
                ON CRD.CREDITTYPESEQ = CT.DATATYPESEQ
                AND CT.REMOVEDATE >CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
            INNER JOIN
                EXT.AIA_tmp_Comls_Step0 IP
                ON 1 = 1
                --AND IP.BUNAME                     = 'SGPAGY'
                AND crd.salestransactionseq = ip.salestransactionseq
                --version 6 update by Amanda Wei begin
                --AND (CRD.GENERICATTRIBUTE12 = IP.WRI_AGT_CODE_ORIG or CRD.GENERICATTRIBUTE12 = IP.FAOB_AGT_CODE)
                --AND (CRD.GENERICATTRIBUTE12 = IP.WRI_AGT_CODE_ORIG or CRD.GENERICATTRIBUTE12 = IP.FAOB_AGT_CODE or IP.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE
                    AND ((CRD.GENERICATTRIBUTE16 IN ('O')
                        AND CRD.GENERICATTRIBUTE12 = IP.WRI_AGT_CODE_ORIG)
                        OR (CRD.GENERICATTRIBUTE16 IN ('RO','RNO')
                        AND IP.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE)
                        OR (CRD.GENERICATTRIBUTE16 IN ('RO','RNO')
                    AND IP.GA26_WRI_AGT2 = IP.WRI_AGT_CODE)) --version 10 add for share case
                INNER JOIN
                    cs_participant PAR
                    ON PAR.USERID = IP.WRI_AGT_CODE
                    AND PAR.REMOVEDATE = :DT_REMOVEDATE
                INNER JOIN
                    cs_gaparticipant GA_PAR
                    ON PAR.PAYEESEQ = GA_PAR.PAYEESEQ
                    AND GA_PAR.REMOVEDATE = :DT_REMOVEDATE
                    --version 6 update by Amanda Wei end
                WHERE
                    1 = 1
                    --and CRD.GENERICATTRIBUTE16 NOT IN ('RO', 'RNO')
                    --and (CRD.GENERICATTRIBUTE16 NOT IN ('RO', 'RNO') OR (CRD.GENERICATTRIBUTE16 IN ('RO', 'RNO') AND ip.Extendeddate9 IS NOT NULL)) ---Added by Gopi, to consider the migrated policies --12092019
                    --version 10 start
                    --and (CRD.GENERICATTRIBUTE16 NOT IN ('RO', 'RNO') OR (CRD.GENERICATTRIBUTE16 IN ('RO', 'RNO') AND ip.Extendeddate9 IS NOT NULL) or (IP.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE AND CRD.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE4)) --version 6 update by Amanda Wei
                    --Harm_Phase4 start
                    --AND (CRD.GENERICATTRIBUTE16 IN ('O') or ((IP.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE or IP.GA26_WRI_AGT2 = IP.WRI_AGT_CODE) AND CRD.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE4))
                    --AND (CRD.GENERICATTRIBUTE16 IN ('O') or ((IP.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE or IP.GA26_WRI_AGT2 = IP.WRI_AGT_CODE) AND CRD.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE11))
                    AND (CRD.GENERICATTRIBUTE16 IN ('O')
                        OR ((IP.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE
                            OR IP.GA26_WRI_AGT2 = IP.WRI_AGT_CODE)
                            AND (CRD.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE4
                    OR CRD.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE11))) --version 14 Harm BSC SPI
                    --Harm_Phase4 end
                    --version 10 end
                    AND CRD.TENANTID = 'AIAS'
                    AND CRD.PROCESSINGUNITSEQ = 38280596832649218;

        /* 170807
        insert into EXT.AIA_tmp_comls_step1
        
        --drop table EXT.AIA_tmp_comls_step1;
        --create table EXT.AIA_tmp_comls_step1 as
        select crd.creditseq,
               crd.salestransactionseq ,
                ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
               ip.ponumber as POLICY_NUMBER,
               ip.policyidseq as POLICYIDSEQ,
                ip.life_number as LIFE_NUMBER,
               ip.coverage_number as COVERAGE_NUMBER,
               ip.rider_number as RIDER_NUMBER,
               ip.component_code as COMPONENT_CODE,
               ip.component_name as COMPONENT_NAME,
               ip.base_rider_ind as BASE_RIDER_IND,
                crd.compensationdate as TRANSACTION_DATE,
                 crd.genericattribute12 as PAYOR_CODE,
                 ct.credittypeid        as CREDITTYPE,
               crd.name               as CREDIT_NAME,
               crd.value              as CREDIT_VALUE,
                crd.periodseq          as PERIODSEQ,
                 crd.genericattribute2  as PRODUCT_NAME,
               crd.genericnumber1     as POLICY_YEAR,
               st.genericnumber2      as COMMISSION_RATE,
               st.genericdate4        as PAID_TO_DATE
               ,'SGT'||ip.wri_agt_code wri_agt_code
               ,ip.quarter || ' ' || ip.year qtrYr, crd.genericdate2
        
          from cs_Credit crd
          join EXT.AIA_tmp_comls_period p
          on crd.periodseq=p.periodseq
          join cs_Salestransaction st
          on st.salestransactionseq=crd.salestransactionseq
         and st.tenantid='AIAS' and st.processingunitseq=crd.processingunitseq
         -- and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
           inner join CS_CREDITTYPE ct
            on crd.CREDITTYPESEQ = ct.DATATYPESEQ
         and ct.Removedate >sysdate
          inner join EXT.AIA_CB_IDENTIFY_POLICY ip
            on 1=1
         and ip.BUNAME = STR_BUNAME
         AND st.PONUMBER = ip.PONUMBER
         AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
         AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
         AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
         AND st.PRODUCTID = ip.COMPONENT_CODE
         and crd.genericattribute12 = ip.wri_agt_code
           where crd.genericattribute16 not in ('RO', 'RNO')
         and crd.tenantid = 'AIAS'
         and crd.processingunitseq = V_PROCESSINGUNITSEQ
          --and st.compensationdate>='1-mar-2016' and st.compensationdate<='30-nov-2016'
        -- and periodseq = 2533274790398934
        --105 seconds. 9 mill rows for nov
        --9 secs, 1221 rows
        -- 240 secs 5000 rows
        --select count(*) from xtmp
        ;*/

        /* ORIGSQL: EXT.Log('insert 1 done for FA'||SQL%ROWCOUNT) */
        CALL EXT.Log('insert 1 done for FA'||::ROWCOUNT);  

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: delete from EXT.AIA_TMP_COMLS_STEP1 where transaction_Date <DT_CB_START_DATE or transaction_Date>DT_CB_END_DATE; */
        DELETE
        FROM
            EXT.AIA_TMP_COMLS_STEP1
        WHERE
            transaction_Date < :DT_CB_START_DATE
            OR transaction_Date > :DT_CB_END_DATE;

        /* ORIGSQL: EXT.Log('delete 1 done for FA '||SQL%ROWCOUNT) */
        CALL EXT.Log('delete 1 done for FA '||::ROWCOUNT);  

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: DBMS_STATS.GATHER_TABLE_STATS(ownname => '"AIASEXT"', tabname => '"EXT.AIA_TMP_COMLS_STEP1"', estimate_percent => 1) */
        EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| '"AIASEXT"'|| '.'|| '"EXT.AIA_TMP_COMLS_STEP1"';

        /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

        /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_comls_step2'; */
        /* ORIGSQL: truncate table EXT.AIA_tmp_comls_step2 ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_step2';

        /* ORIGSQL: insert into EXT.AIA_tmp_comls_step2 select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq, x.clawback_name from cs_measurement m join EXT.AIA_tmp_comls_period p on m.periodseq=p.periodseq(...) */
        INSERT INTO EXT.AIA_tmp_comls_step2
            --drop table EXT.AIA_tmp_comls_step2;
            --create table EXT.AIA_tmp_comls_step2  as
            SELECT   /* ORIGSQL: select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq, x.clawback_name from cs_measurement m join EXT.AIA_tmp_comls_period p on m.periodseq=p.periodseq join (SELECT distinct SOURCE_RU(...) */
                measurementseq,
                m.name,
                m.periodseq,
                payeeseq,
                ruleseq,
                positionseq,
                x.clawback_name
            FROM
                cs_measurement m
            INNER JOIN
                EXT.AIA_tmp_comls_period p
                ON m.periodseq = p.periodseq
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME from EXT.AIA_CB_RULES_LOOKUP where RULE_TYPE = 'PM' AND CLAWBACK_NAME in (STR_COMMISSION,STR_GST_COMMISSION) and BUNAME=STR_BUNAME_FA) */
                        DISTINCT
                        SOURCE_RULE_OUTPUT,
                        CLAWBACK_NAME
                    FROM
                        EXT.AIA_CB_RULES_LOOKUP
                    WHERE
                        RULE_TYPE = 'PM'
                        AND CLAWBACK_NAME IN (:STR_COMMISSION,:STR_GST_COMMISSION)
                        AND BUNAME = :STR_BUNAME_FA
                ) AS x
                ON x.SOURCE_RULE_OUTPUT = m.name
            WHERE
                m.processingunitseq = :V_PROCESSINGUNITSEQ
                AND m.tenantid = 'AIAS';

        /* ORIGSQL: EXT.Log('insert 2 done  for FA '||SQL%ROWCOUNT) */
        CALL EXT.Log('insert 2 done  for FA '||::ROWCOUNT);  

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: DBMS_STATS.GATHER_TABLE_STATS(ownname => '"AIASEXT"', tabname => '"EXT.AIA_TMP_COMLS_STEP2"', estimate_percent => 1) */
        EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| '"AIASEXT"'|| '.'|| '"EXT.AIA_TMP_COMLS_STEP2"';

        /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

        /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_comls_step3'; */
        /* ORIGSQL: truncate table EXT.AIA_tmp_comls_step3 ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_step3';

        /* ORIGSQL: insert into EXT.AIA_tmp_comls_step3 select pct.creditseq pctCreditSeq, pct.measurementseq, pct.contributionvalue PctContribValue, dct.depositseq, s1.CREDITSEQ,SALESTRANSACTIONSEQ,CALCULATION_PERIOD,POLICY(...) */
        /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_PMCREDITTRACE' not found */
        /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_DEPOSITPMTRACE' not found */
        INSERT INTO EXT.AIA_tmp_comls_step3
            -- drop table EXT.AIA_tmp_comls_step3
            -- create table EXT.AIA_tmp_comls_step3 as
            SELECT   /* ORIGSQL: select pct.creditseq pctCreditSeq, pct.measurementseq, pct.contributionvalue PctContribValue, dct.depositseq, s1.CREDITSEQ,SALESTRANSACTIONSEQ,CALCULATION_PERIOD,POLICY_NUMBER,POLICYIDSEQ,LIFE_NUMBER,(...) */
                pct.creditseq AS pctCreditSeq,
                pct.measurementseq,
                pct.contributionvalue AS PctContribValue,
                dct.depositseq,
                s1.CREDITSEQ,
                SALESTRANSACTIONSEQ,
                CALCULATION_PERIOD,
                POLICY_NUMBER,
                POLICYIDSEQ,
                LIFE_NUMBER,
                COVERAGE_NUMBER,
                RIDER_NUMBER,
                COMPONENT_CODE,
                COMPONENT_NAME,
                BASE_RIDER_IND,
                TRANSACTION_DATE,
                PAYOR_CODE,
                CREDITTYPE,
                CREDIT_NAME,
                CREDIT_VALUE,
                s1.PERIODSEQ,
                PRODUCT_NAME,
                POLICY_YEAR,
                COMMISSION_RATE,
                PAID_TO_DATE,
                s2.name AS mname,
                s2.periodseq AS mPeriodSeq,
                s2.payeeseq AS mPayeeSeq,
                s2.ruleseq AS mruleSeq,
                s2.positionseq AS mPositionSeq,
                s2.clawback_name,
                WRI_AGT_CODE,
                QTRYR,
                GD2
            FROM
                cs_pmcredittrace pct
            INNER JOIN
                EXT.AIA_tmp_comls_step1 s1
                ON pct.creditseq = s1.creditseq
            INNER JOIN
                EXT.AIA_tmp_comls_step2 s2
                ON s2.measurementseq = pct.measurementseq
                AND ((s2.ruleseq = pct.ruleseq
                    AND s2.name != 'PM_NADOR_CM')
                    OR (s2.name = 'PM_NADOR_CM')) -- Added condiftion to not check ruleseq for NADOR measurements-Gopi-25102019
                --and pct.targetperiodseq=s2.periodseq
            INNER JOIN
                cs_depositpmtrace dct
                ON 1 = 1
                AND dct.measurementseq = pct.measurementseq
                --and dct.targetperiodseq=s2.periodseq
                AND dct.tenantid = 'AIAS'
                AND pct.tenantid = 'AIAS'
                AND dct.processingUnitseq = :V_PROCESSINGUNITSEQ
                AND pct.processingUnitseq = :V_PROCESSINGUNITSEQ;

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: EXT.Log('insert 3 done for FA') */
        CALL EXT.Log('insert 3 done for FA');

        /* ORIGSQL: DBMS_STATS.GATHER_TABLE_STATS(ownname => '"AIASEXT"', tabname => '"EXT.AIA_TMP_COMLS_STEP3"', estimate_percent => 1) */
        EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| '"AIASEXT"'|| '.'|| '"EXT.AIA_TMP_COMLS_STEP3"';

        /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

        /* ORIGSQL: insert into EXT.AIA_CB_TRACE_FORWARD select DISTINCT STR_BUNAME_FA as BUNAME, QtrYr as CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, mPositionseq PAYEE_SEQ, substr(dep_pos.name, 4) as PAYEE_CODE, PAYOR_(...) */
        /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_DEPOSIT' not found */
        INSERT INTO EXT.AIA_CB_TRACE_FORWARD
            SELECT   /* ORIGSQL: select DISTINCT STR_BUNAME_FA as BUNAME, QtrYr as CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, mPositionseq PAYEE_SEQ, substr(dep_pos.name, 4) as PAYEE_CODE, PAYOR_CODE, LIFE_NUMBER, COVERAGE_NUMBE(...) */
                DISTINCT
                :STR_BUNAME_FA AS BUNAME,
                QtrYr AS CALCULATION_PERIOD,
                POLICY_NUMBER,
                POLICYIDSEQ,
                mPositionseq AS PAYEE_SEQ,
                SUBSTRING(dep_pos.name,4) AS PAYEE_CODE,  /* ORIGSQL: substr(dep_pos.name, 4) */
                PAYOR_CODE,
                LIFE_NUMBER,
                COVERAGE_NUMBER,
                RIDER_NUMBER,
                COMPONENT_CODE,
                COMPONENT_NAME,
                BASE_RIDER_IND,
                TRANSACTION_DATE,
                /* --to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE) as TRANSACTION_DATE, */
                TO_VARCHAR(:DT_CB_START_DATE,'MON-YYYY') AS PROCESSING_PERIOD,  /* ORIGSQL: TO_CHAR(DT_CB_START_DATE,'MON-YYYY') */
                /* --clawback type = 'Lumpsum' */
                :STR_LUMPSUM AS CLAWBACK_TYPE,
                /* --clawback name = 'Commission' */
                /* --STR_CB_NAME            as CLAWBACK_NAME, */
                CLAWBACK_NAME AS CLAWBACK_NAME,
                CREDITTYPE,
                CREDITSEQ,
                CREDIT_NAME,
                CREDIT_VALUE,
                measurementseq AS PM_SEQ,
                mname AS PM_NAME,
                pctcontribvalue AS PM_CONTRIBUTE_VALUE,
                1 AS PM_RATE,
                dep.depositseq AS DEPOSITSEQ,
                dep.name AS DEPOSIT_NAME,
                dep.value AS DEPOSIT_VALUE,
                x.periodseq AS PERIODSEQ,
                salestransactionseq AS SALESTRANSACTIONSEQ,
                PRODUCT_NAME,
                POLICY_YEAR,
                COMMISSION_RATE,
                PAID_TO_DATE,
                :P_BATCH_NO AS BATCH_NUMBER,
                CURRENT_TIMESTAMP AS CREATED_DATE,  /* ORIGSQL: sysdate */
                SUBSTRING(x.WRI_AGT_CODE,4) /* --version 10 */  /* ORIGSQL: substr(x.WRI_AGT_CODE,4) */
            FROM
                EXT.AIA_tmp_comls_step3 x
            INNER JOIN
                cs_deposit dep
                ON dep.depositseq = x.depositseq
            INNER JOIN
                cs_position dep_pos
                ON dep.positionseq = dep_pos.ruleelementownerseq
                AND dep_pos.removedate = :DT_REMOVEDATE
                -- and dep_pos.effectivestartdate <= x.GD2
                -- and dep_pos.effectiveenddate > x.GD2
                AND dep_pos.name = 'SGT'||IFNULL(x.payor_code,'') -- x.wri_agt_code  --Modified by to Get New agent code also 
            WHERE
                x.qtrYr = :V_CAL_PERIOD;

        /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD for FA' || '; row count: ' || to_char(sql%rowcount)) */
        CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD for FA'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
            );  /* ORIGSQL: to_char(sql%rowcount) */

        /* ORIGSQL: commit; */
        COMMIT;

        --end loop;
    ELSEIF :P_STR_TYPE = :STR_ONGOING  /* ORIGSQL: elsif P_STR_TYPE = STR_ONGOING then */
    THEN
        --setup the start date and end date for on-going period
        IF TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE) = :DT_WEEKLY_END_DATE
        THEN
            DT_ONGOING_START_DATE = ext.trunc(TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE),'MONTH');  /* ORIGSQL: trunc(to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE),'MONTH') */

            DT_ONGOING_END_DATE = :DT_WEEKLY_END_DATE;
        ELSE 
            DT_ONGOING_START_DATE = ext.trunc(TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE),'MONTH');  /* ORIGSQL: trunc(to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE),'MONTH') */

            SELECT
                TO_DATE(ADD_SECONDS(csp.enddate,(86400*-1)))   /* ORIGSQL: csp.enddate - 1 */
            INTO
                DT_ONGOING_END_DATE
            FROM
                cs_period csp
            INNER JOIN
                cs_periodtype pt
                ON csp.periodtypeseq = pt.periodtypeseq
            WHERE
                csp.enddate = TO_DATE(ADD_SECONDS(TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE),(86400*1)))   /* ORIGSQL: TO_DATE(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) + 1 */
                AND csp.removedate = :DT_REMOVEDATE
                AND calendarseq = :V_CALENDARSEQ
                AND pt.name = 'month';
        END IF;

        /* ORIGSQL: EXT.Log('FA DT_ONGOING_START_DATE = ' || DT_ONGOING_START_DATE) */
        CALL EXT.Log('FA DT_ONGOING_START_DATE = '|| IFNULL(TO_VARCHAR(:DT_ONGOING_START_DATE),''));

        /* ORIGSQL: EXT.Log('FA DT_ONGOING_END_DATE = ' || DT_ONGOING_END_DATE) */
        CALL EXT.Log('FA DT_ONGOING_END_DATE = '|| IFNULL(TO_VARCHAR(:DT_ONGOING_END_DATE),'')); 

        SELECT
            MIN(periodseq) 
        INTO
            vOngoingperiod
        FROM
            CS_period
        WHERE
            removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND startdate = ADD_MONTHS(TO_DATE(ADD_SECONDS(LAST_DAY(TO_DATE(:DT_ONGOING_START_DATE, 'DD')),(86400*1))),-1)  /* ORIGSQL: trunc(DT_ONGOING_START_DATE) */
            AND periodtypeseq = 2814749767106561
            AND calendarseq = :V_CALENDARSEQ
            AND removedate = to_date('2200-01-01','yyyy-mm-dd');--Cosimo
        /* ORIGSQL: to_date('2200-01-01','yyyy-mm-dd') */

        SELECT
            MIN(periodseq) 
        INTO
            vOngoingendperiod
        FROM
            CS_period
        WHERE
            removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND startdate = ADD_MONTHS(TO_DATE(ADD_SECONDS(LAST_DAY(TO_DATE(:DT_ONGOING_END_DATE, 'DD')),(86400*1))),-1)  /* ORIGSQL: trunc(DT_ONGOING_END_DATE) */
            AND periodtypeseq = 2814749767106561
            AND calendarseq = :V_CALENDARSEQ
            AND removedate = to_date('2200-01-01','yyyy-mm-dd');--Cosimo
        /* ORIGSQL: to_date('2200-01-01','yyyy-mm-dd') */

        /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD for FA, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO) */
        CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD for FA, '|| 'clawback type = '|| IFNULL(:P_STR_TYPE,'') ||', batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO),''));

        /* ORIGSQL: execute immediate 'truncate table EXT.AIA_CB_TRACE_FORWARD_TMP'; */
        /* ORIGSQL: truncate table EXT.AIA_CB_TRACE_FORWARD_TMP ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_CB_TRACE_FORWARD_TMP';

        /* ORIGSQL: EXT.Log('insert 1 started for FA') */
        CALL EXT.Log('insert 1 started for FA');
        ---temp table insert  EXT.AIA_CB_TRACE_FORWARD_TMP
        /* ORIGSQL: insert / *+ APPEND * / */

        /* ORIGSQL: insert into EXT.AIA_CB_TRACE_FORWARD_TMP select null as PAYEE_SEQ, null as PAYEE_CODE, crd.genericattribute12 as PAYOR_CODE, crd.compensationdate as TRANSACTION_DATE, ct.credittypeid as CREDITTYPE, crd.cr(...) */
        INSERT INTO EXT.AIA_CB_TRACE_FORWARD_TMP
            /* ORIGSQL: select / *+ PARALLEL leading(crd) * / */
            SELECT   /* ORIGSQL: select null as PAYEE_SEQ, null as PAYEE_CODE, crd.genericattribute12 as PAYOR_CODE, crd.compensationdate as TRANSACTION_DATE, ct.credittypeid as CREDITTYPE, crd.creditseq as CREDITSEQ, crd.name as CRE(...) */
                NULL AS PAYEE_SEQ,
                NULL AS PAYEE_CODE,
                crd.genericattribute12 AS PAYOR_CODE,
                crd.compensationdate AS TRANSACTION_DATE,
                ct.credittypeid AS CREDITTYPE,
                crd.creditseq AS CREDITSEQ,
                crd.name AS CREDIT_NAME,
                crd.value AS CREDIT_VALUE,
                NULL AS PM_SEQ,
                NULL AS PM_NAME,
                pct.CONTRIBUTIONVALUE AS PM_CONTRIBUTE_VALUE,
                crd.periodseq AS PERIODSEQ,
                st.salestransactionseq AS SALESTRANSACTIONSEQ,
                crd.genericattribute2 AS PRODUCT_NAME,
                crd.genericnumber1 AS POLICY_YEAR,
                st.genericnumber2 AS COMMISSION_RATE,
                st.genericdate4 AS PAID_TO_DATE,
                st.GENERICATTRIBUTE29,
                st.PONUMBER,
                st.GENERICATTRIBUTE30,
                st.GENERICATTRIBUTE31,
                st.PRODUCTID,
                crd.genericattribute12,
                NULL AS name,
                pct.measurementseq,
                NULL,
                crd.genericdate2,
                pct.targetperiodseq  /* -- v5 */, st.genericattribute10,
                crd.genericattribute16,
                GST.GENERICDATE9  /* --version 6 add by Amanda Wei */, st.genericattribute26  /* --version 10 */
            FROM
                CS_SALESTRANSACTION st
            INNER JOIN
                CS_CREDIT crd
                ON st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
                --and crd.genericdate2
            INNER JOIN
                CS_PMCREDITTRACE pct
                -- v4 added sourceperiodseq to hit index
                ON crd.CREDITSEQ = pct.CREDITSEQ
                AND crd.periodseq = pct.sourceperiodseq  --- Modified by Sundeep
                AND crd.pipelinerunseq = pct.pipelinerunseq  --Added by Sundeep
            INNER JOIN
                CS_CREDITTYPE ct
                ON crd.CREDITTYPESEQ = ct.DATATYPESEQ
                AND ct.Removedate = :DT_REMOVEDATE
                -- v5 start
            INNER JOIN
                EXT.AIA_CB_IDENTIFY_POLICY ip
                ON ip.BUNAME = :STR_BUNAME_FA
                AND st.PONUMBER = ip.PONUMBER
                AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
                AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
                AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
                AND st.PRODUCTID = ip.COMPONENT_CODE
                AND (crd.genericattribute12 = ip.wri_agt_code
                OR crd.genericattribute12 = IP.FAOB_AGT_CODE)
                -- v5 end
            INNER JOIN
                cs_businessunit bu
                ON st.businessunitmap = bu.mask
                -- v4 added pagenumber
            INNER JOIN
                cs_gasalestransaction GST
                ON GST.SALESTRANSACTIONSEQ = st.SALESTRANSACTIONSEQ
                AND gst.pagenumber = 0 ---Added by Gopi, to consider the migrated policies --12092019
            WHERE
                st.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ
                -- AND bu.name = STR_BUNAME_FA    --Added by Gopi, to consider the migrated policies --16012019
                AND st.compensationdate BETWEEN :DT_ONGOING_START_DATE AND :DT_ONGOING_END_DATE
                ---Added by Gopi, to consider the migrated policies --12092019
                -- v3 changed AND condition to OR
                -- and (crd.genericattribute16 not in ('RO', 'RNO') OR (crd.genericattribute16 in ('RO', 'RNO') and GST.GENERICDATE9 IS NOT NULL))  --version 6 remove by Amanda Wei
                AND crd.periodseq BETWEEN :vOngoingperiod AND :vOngoingendperiod
                AND crd.tenantid = 'AIAS'
                AND crd.processingunitseq = :V_PROCESSINGUNITSEQ
                AND st.tenantid = 'AIAS'
                AND st.processingunitseq = :V_PROCESSINGUNITSEQ
                AND pct.tenantid = 'AIAS'
                AND pct.processingunitseq = :V_PROCESSINGUNITSEQ
                --v4 added to hit partition
                AND gst.tenantid = 'AIAS'
                AND pct.processingunitseq = :V_PROCESSINGUNITSEQ
                AND gst.compensationdate BETWEEN :DT_ONGOING_START_DATE AND :DT_ONGOING_END_DATE
                -- v5
                AND st.compensationdate = crd.compensationdate;

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: EXT.Log('insert 1 ended for FA') */
        CALL EXT.Log('insert 1 ended for FA');

        /* ORIGSQL: EXT.Log('insert 2 started for FA') */
        CALL EXT.Log('insert 2 started for FA');
        ------Main table insert  EXT.AIA_CB_TRACE_FORWARD
        -- v12 add ORDERED hints
        /* ORIGSQL: insert / *+ APPEND * / */

        /* ORIGSQL: insert into EXT.AIA_CB_TRACE_FORWARD select DISTINCT STR_BUNAME_FA as BUNAME, ip.quarter || ' ' || ip.year as CALCULATION_PERIOD, ip.ponumber as POLICY_NUMBER, ip.policyidseq as POLICYIDSEQ, pm.positionse(...) */
        INSERT INTO EXT.AIA_CB_TRACE_FORWARD
            /* ORIGSQL: select / *+ ORDERED * / */
            SELECT   /* ORIGSQL: select DISTINCT STR_BUNAME_FA as BUNAME, ip.quarter || ' ' || ip.year as CALCULATION_PERIOD, ip.ponumber as POLICY_NUMBER, ip.policyidseq as POLICYIDSEQ, pm.positionseq as PAYEE_SEQ, substr(pm_pos.nam(...) */
                DISTINCT
                :STR_BUNAME_FA AS BUNAME,
                IFNULL(ip.quarter,'') || ' ' || IFNULL(ip.year,'') AS CALCULATION_PERIOD,
                ip.ponumber AS POLICY_NUMBER,
                ip.policyidseq AS POLICYIDSEQ,
                pm.positionseq AS PAYEE_SEQ,
                SUBSTRING(pm_pos.name,4) AS PAYEE_CODE,  /* ORIGSQL: substr(pm_pos.name, 4) */
                tmp.PAYOR_CODE,
                ip.life_number AS LIFE_NUMBER,
                ip.coverage_number AS COVERAGE_NUMBER,
                ip.rider_number AS RIDER_NUMBER,
                ip.component_code AS COMPONENT_CODE,
                ip.component_name AS COMPONENT_NAME,
                ip.base_rider_ind AS BASE_RIDER_IND,
                tmp.TRANSACTION_DATE,
                /* --TO_CHAR(DT_CB_START_DATE, 'MON-YYYY') as PROCESSING_PERIOD, */
                TO_VARCHAR(TO_DATE(:P_STR_CYCLEDATE,:STR_DATE_FORMAT_TYPE),'MON-YYYY') AS PROCESSING_PERIOD,  /* ORIGSQL: TO_CHAR(to_date(P_STR_CYCLEDATE,STR_DATE_FORMAT_TYPE), 'MON-YYYY') */
                /* --clawback type = 'Lumpsum' */
                :STR_ONGOING AS CLAWBACK_TYPE,
                /* --clawback name = 'Commission' */
                /* --STR_CB_NAME            as CLAWBACK_NAME, */
                rl.CLAWBACK_NAME AS CLAWBACK_NAME,
                tmp.CREDIT_TYPE,
                tmp.CREDITSEQ,
                tmp.CREDIT_NAME,
                tmp.CREDIT_VALUE,
                pm.measurementseq AS PM_SEQ,
                pm.name AS PM_NAME,
                tmp.PM_CONTRIBUTION_VALUE,
                1 AS PM_RATE,
                '' AS DEPOSITSEQ,
                '' AS DEPOSIT_NAME,
                '' AS DEPOSIT_VALUE,
                tmp.PERIODSEQ,
                tmp.SALESTRANSACTIONSEQ,
                tmp.PRODUCT_NAME,
                tmp.POLICY_YEAR,
                tmp.COMMISSION_RATE,
                tmp.PAID_TO_DATE,
                :P_BATCH_NO AS BATCH_NUMBER,
                CURRENT_TIMESTAMP AS CREATED_DATE,  /* ORIGSQL: sysdate */
                ip.wri_agt_code  /* --version 10 */
            FROM
                EXT.AIA_CB_TRACE_FORWARD_TMP tmp
            INNER JOIN
                CS_MEASUREMENT pm
                ON tmp.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
            INNER JOIN
                CS_POSITION pm_pos
                ON pm_pos.ruleelementownerseq = pm.positionseq
                AND pm_pos.removedate = :DT_REMOVEDATE
                AND pm_pos.islast = 1
                -- and pm_pos.effectivestartdate <= tmp.genericdate2
                -- and pm_pos.effectiveenddate > tmp.genericdate2   --Added by Gopi, to consider the migrated policies --16012019
            INNER JOIN
                EXT.AIA_CB_IDENTIFY_POLICY ip
                ON ip.BUNAME = :STR_BUNAME_FA
                AND tmp.PONUMBER = ip.PONUMBER
                AND tmp.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
                AND tmp.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
                AND tmp.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
                AND tmp.PRODUCTID = ip.COMPONENT_CODE
                -- and tmp.genericattribute12 = ip.wri_agt_code
                -- and (tmp.genericattribute12 = ip.wri_agt_code or tmp.genericattribute12=IP.FAOB_AGT_CODE) ---Added by Gopi, to consider the migrated policies --12092019
                --and (tmp.genericattribute12 = ip.wri_agt_code or tmp.genericattribute12=IP.FAOB_AGT_CODE or tmp.genericattribute10 = ip.wri_agt_code) --version 6 update by Amanda Wei
                AND((tmp.GENERICATTRIBUTE16 IN ('O')
                    AND tmp.genericattribute12 = ip.wri_agt_code)
                    OR (tmp.GENERICATTRIBUTE16 IN ('RO','RNO')
                    AND tmp.genericattribute10 = ip.wri_agt_code)
                    OR (tmp.GENERICATTRIBUTE16 IN ('RO','RNO')
                AND tmp.GA26_WRI_AGT2 = ip.wri_agt_code)) --version 10 add
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME from EXT.AIA_CB_RULES_LOOKUP where RULE_TYPE = 'PM' AND CLAWBACK_NAME in (STR_COMMISSION, STR_GST_COMMISSION) and BUNAME=STR_BUNAME_FA) */
                        DISTINCT
                        SOURCE_RULE_OUTPUT,
                        CLAWBACK_NAME
                    FROM
                        EXT.AIA_CB_RULES_LOOKUP
                    WHERE
                        RULE_TYPE = 'PM'
                        AND CLAWBACK_NAME IN (:STR_COMMISSION, :STR_GST_COMMISSION)
                        AND BUNAME = :STR_BUNAME_FA
                ) AS rl
                ON pm.NAME = rl.SOURCE_RULE_OUTPUT
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select distinct cb_quarter_name, cb_startdate, cb_enddate from EXT.AIA_cb_period where cb_name = STR_COMMISSION and BUNAME=STR_BUNAME_FA) */
                        DISTINCT
                        cb_quarter_name,
                        cb_startdate,
                        cb_enddate
                    FROM
                        EXT.AIA_cb_period
                    WHERE
                        cb_name = :STR_COMMISSION
                        AND BUNAME = :STR_BUNAME_FA
                ) AS cbp
                ON IFNULL(ip.quarter,'') || ' '|| IFNULL(ip.year,'') = cbp.cb_quarter_name
                --version 6 update by Amanda Wei begin
            INNER JOIN
                cs_participant PAR
                ON PAR.USERID = 'SGT'|| IFNULL(IP.WRI_AGT_CODE,'')
                AND PAR.REMOVEDATE = :DT_REMOVEDATE
            INNER JOIN
                cs_gaparticipant GA_PAR
                ON PAR.PAYEESEQ = GA_PAR.PAYEESEQ
                AND GA_PAR.REMOVEDATE = :DT_REMOVEDATE
            WHERE
                TO_DATE(:P_STR_CYCLEDATE,:STR_DATE_FORMAT_TYPE) > cbp.cb_enddate
                -- v5 start
                AND pm.tenantid = 'AIAS'
                AND pm.processingunitseq = :V_PROCESSINGUNITSEQ
                AND tmp.PM_TARGETPERIODSEQ = pm.periodseq
                -- v5 end
                AND PAR.TENANTID = 'AIAS'
                AND GA_PAR.TENANTID = 'AIAS'
                --AND (tmp.GENERICATTRIBUTE16 in ('O') or tmp.GENERICDATE9 IS NOT NULL or (tmp.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE AND tmp.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE4))
                --Harm_Phase4 start
                --AND (tmp.GENERICATTRIBUTE16 IN ('O') or ((tmp.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE or tmp.GA26_WRI_AGT2 = IP.WRI_AGT_CODE) AND tmp.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE4))
                --AND (tmp.GENERICATTRIBUTE16 IN ('O') or ((tmp.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE or tmp.GA26_WRI_AGT2 = IP.WRI_AGT_CODE) AND tmp.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE11))
                AND (tmp.GENERICATTRIBUTE16 IN ('O')
                    OR ((tmp.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE
                        OR tmp.GA26_WRI_AGT2 = IP.WRI_AGT_CODE)
                        AND (tmp.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE4
                OR tmp.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE11)));--version 14 Harm BSC SPI
        --Harm_Phase4 end
        --version 6 update by Amanda Wei end

        /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD for FA' || '; row count: ' || to_char(sql%rowcount)) */
        CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD for FA'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
            );  /* ORIGSQL: to_char(sql%rowcount) */

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: EXT.Log('SP_TRACE_FORWARD_COMMISSION_FA end') */
        CALL EXT.Log('SP_TRACE_FORWARD_COMMISSION_FA end');
    END IF;
END;
  PUBLIC PROCEDURE SP_CLAWBACK_COMMISSION_FA
(
    IN P_STR_CYCLEDATE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                       /* ORIGSQL: P_STR_CYCLEDATE IN VARCHAR2 */
    IN P_BATCH_NO BIGINT     /* ORIGSQL: P_BATCH_NO IN INTEGER */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE V_REC_COUNT BIGINT;  /* ORIGSQL: V_REC_COUNT INTEGER; */
    DECLARE V_BATCH_NO_PRE_QTR BIGINT;  /* ORIGSQL: V_BATCH_NO_PRE_QTR INTEGER; */
    DECLARE V_CB_TYPE VARCHAR(50);  /* ORIGSQL: V_CB_TYPE VARCHAR2(50); */
    DECLARE V_CB_NAME VARCHAR(50);  /* ORIGSQL: V_CB_NAME VARCHAR2(50); */
    DECLARE V_CB_QTR VARCHAR(50);  /* ORIGSQL: V_CB_QTR VARCHAR2(50); */

    /* initialize library variables, if not yet done */
    CALL init_session_global();

    /* ORIGSQL: EXT.Log('SP_CLAWBACK_COMMISSION_FA start') */
    CALL EXT.Log('SP_CLAWBACK_COMMISSION_FA start');

    /* ORIGSQL: init */
    CALL init();

    --get records count from EXT.AIA_CB_CLAWBACK_COMMISSION
    SELECT
        COUNT(1)
    INTO
        V_REC_COUNT
    FROM
        EXT.AIA_CB_CLAWBACK_COMMISSION
    WHERE
        batch_no = :P_BATCH_NO;

    --delete the records in EXT.AIA_CB_CLAWBACK_COMMISSION if batch number is being reused.
    IF :V_REC_COUNT > 0
    THEN
        /* ORIGSQL: delete from EXT.AIA_CB_CLAWBACK_COMMISSION where batch_no = P_BATCH_NO; */
        DELETE
        FROM
            EXT.AIA_CB_CLAWBACK_COMMISSION
        WHERE
            batch_no = :P_BATCH_NO;

        /* ORIGSQL: delete from EXT.AIA_CB_CLAWBACK_SVI_TMP where batch_no = P_BATCH_NO; */
        DELETE
        FROM
            EXT.AIA_CB_CLAWBACK_SVI_TMP
        WHERE
            batch_no = :P_BATCH_NO;

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_CLAWBACK_COMMISSION_FA,' ||' batch_no = ' || P_BATCH_NO) */
    CALL EXT.Log('insert into EXT.AIA_CB_CLAWBACK_COMMISSION_FA,'||' batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO),''));

    --insert data into EXT.AIA_CB_CLAWBACK_COMMISSION for commission
    /* ORIGSQL: insert into EXT.AIA_CB_CLAWBACK_COMMISSION select DISTINCT tf.calculation_period as MEASUREMENT_QUARTER, tf.clawback_type as CLAWBACK_TYPE, tf.clawback_name as CLAWBACK_NAME, TO_DATE(P_STR_CYCLEDATE, STR_(...) */
    INSERT INTO EXT.AIA_CB_CLAWBACK_COMMISSION
        /* ORIGSQL: select / *+ leading(tf,ip,ba,st,cr) use_nl(tf,ip,ba,st,cr) NO_PARALLEL index(ST EXT.AIA_CS_SALESTRANSACTION_SEQ) index(CR OD_CREDIT_CREDITSEQ) * / */
        SELECT   /* ORIGSQL: select DISTINCT tf.calculation_period as MEASUREMENT_QUARTER, tf.clawback_type as CLAWBACK_TYPE, tf.clawback_name as CLAWBACK_NAME, TO_DATE(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) as CALCULATION_DATE, (...) */
            -- RULE*/
            DISTINCT
            tf.calculation_period AS MEASUREMENT_QUARTER,
            tf.clawback_type AS CLAWBACK_TYPE,
            tf.clawback_name AS CLAWBACK_NAME,
            /* --tf.processing_period as CALCULATION_DATE, */
            TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE) AS CALCULATION_DATE,
            pos_dis.GENERICATTRIBUTE3 AS WRI_DIST_CODE,
            TRIM(IFNULL(par_dis.firstname,'')||' '||IFNULL(par_dis.lastname,'')) AS WRI_DIST_NAME,
            pos_dis.genericattribute2 AS WRI_DM_CODE,
            SUBSTRING(pos_agy.name,4) AS WRI_AGY_CODE,  /* ORIGSQL: substr(pos_agy.name, 4) */
            TRIM(IFNULL(par_agy.firstname,'')||' '||IFNULL(par_agy.lastname,'')) AS WRI_AGY_NAME,
            pos_agt.GENERICATTRIBUTE2 AS WRI_AGY_LDR_CODE,
            pos_agt.genericattribute7 AS WRI_AGY_LDR_NAME,
            tf.payor_code AS WRI_AGT_CODE,
            TRIM(IFNULL(par_agt.firstname,'')||' '||IFNULL(par_agt.lastname,'')) AS WRI_AGT_NAME,
            /* --'Normal FSC' as FSC_TYPE, */
            MAP(par_agt.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') AS FSC_TYPE,  /* ORIGSQL: decode(par_agt.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') */
            title_agt.name AS RANK,
            cr.genericattribute14 AS CLASS,
            pos_agt.genericattribute4 AS UM_CLASS,
            ba.bsc_grade AS FSC_BSC_GRADE,
            ba.entitlementpercent AS FSC_BSC_PERCENTAGE,
            tf.policy_number AS PONUMBER,
            tf.LIFE_NUMBER AS LIFE_NUMBER,
            tf.COVERAGE_NUMBER AS COVERAGE_NUMBER,
            tf.RIDER_NUMBER AS RIDER_NUMBER,
            tf.component_code AS COMPONENT_CODE,
            tf.product_name AS PRODUCT_NAME,
            tf.transaction_date AS TRANSACTION_DATE,
            tf.policy_year AS POLICY_YEAR,
            CASE
                WHEN tf.credit_type = 'FYC'
                THEN tf.credit_value
                ELSE 0
            END
            AS FYC,
            CASE
                WHEN tf.credit_type = 'API'
                THEN tf.credit_value
                ELSE 0
            END
            AS API,
            CASE
                WHEN tf.credit_type = 'SSCP'
                THEN tf.credit_value
                ELSE 0
            END
            AS SSC,
            CASE
                WHEN tf.credit_type = 'RYC'
                THEN tf.credit_value
                ELSE 0
            END
            AS RYC,
            /**
            --for Commission only
            --if SVI is a negative value, then check if this component exist in last quarter clawback result,
            --if exist and clawback value is negative, then continue, else skip(set figure=0).
            **/
            (tf.pm_contribution_value * :INT_SVI_RATE) AS SVI,
            (tf.pm_contribution_value * :INT_SVI_RATE) * ba.entitlementpercent AS ENTITLEMENT,
            /** SVI - ENTITLEMENT */
            /* --fix the rounding issue */
            ROUND(((tf.pm_contribution_value * :INT_SVI_RATE) -
                    (tf.pm_contribution_value * :INT_SVI_RATE) * ba.entitlementpercent) * (-1)
            ,2) AS CLAWBACK_VALUE,
            0 AS PROCESSED_CLAWBACK,
            /* --0 as BASIC_RIDER_IND, */
            tf.base_rider_ind AS BASE_RIDER_IND,
            tf.salestransactionseq,
            tf.creditseq,
            tf.pm_seq,
            :P_BATCH_NO,
            0 AS OFFSET_CLAWBACK,
            tf.wri_agt_code AS wri_agt_code_org /* --version 10 */
        FROM
            EXT.AIA_CB_TRACE_FORWARD tf
        INNER JOIN
            EXT.AIA_cb_identify_policy ip
            ON tf.policyidseq = ip.policyidseq
        INNER JOIN
            EXT.AIA_cb_bsc_agent ba
            ON tf.calculation_period = (IFNULL(ba.quarter,'') || ' ' || IFNULL(ba.year,''))
            AND tf.wri_agt_code = ba.agentcode --version 10
            -- and tf.payor_code = ba.agentcode
        INNER JOIN
            cs_salestransaction st
            ON tf.salestransactionseq = st.salestransactionseq
            --and (ip.wri_agt_code=ba.agentcode or ip.FAOB_AGT_CODE=ba.agentcode)-- Modified condition to get FA Agent data also-Gopi-11112019
            --and (ip.wri_agt_code=ba.agentcode or ip.FAOB_AGT_CODE=ba.agentcode or (tf.payor_code = st.GENERICATTRIBUTE12 and st.GENERICATTRIBUTE10 = ba.agentcode)) --Modified condition to get Forts Agent data also--version 6 by Amanda
            /* and (ip.wri_agt_code=ba.agentcode or ip.FAOB_AGT_CODE=ba.agentcode or (tf.payor_code = st.GENERICATTRIBUTE12 and st.GENERICATTRIBUTE10 = ba.agentcode)
                 or (tf.payor_code = st.GENERICATTRIBUTE12 and st.GENERICATTRIBUTE26 = ba.agentcode)) */--version 10 comment
        INNER JOIN
            CS_CREDIT cr
            ON tf.creditseq = cr.creditseq
            AND cr.processingUnitseq = :V_PROCESSINGUNITSEQ
            --for writing Agency postion info
        INNER JOIN
            cs_position pos_agy
            ON pos_agy.name = 'SGY' || IFNULL(ip.wri_agy_code,'')
            AND pos_agy.removedate = :DT_REMOVEDATE
            AND pos_agy.effectivestartdate <= cr.genericdate2
            AND pos_agy.effectiveenddate > cr.genericdate2
            --for writing Agency participant info
        INNER JOIN
            cs_participant par_agy
            ON par_agy.PAYEESEQ = pos_agy.PAYEESEQ
            AND par_agy.effectivestartdate <= cr.genericdate2
            AND par_agy.effectiveenddate > cr.genericdate2
            AND par_agy.removedate = :DT_REMOVEDATE
            --for writing District postion info
        INNER JOIN
            cs_position pos_dis
            ON pos_dis.name = 'SGY' || IFNULL(pos_agy.genericattribute3,'')
            AND pos_dis.effectivestartdate <= cr.genericdate2
            AND pos_dis.effectiveenddate > cr.genericdate2
            AND pos_dis.removedate = :DT_REMOVEDATE
            --for writing District participant info
        INNER JOIN
            cs_participant par_dis
            ON par_dis.PAYEESEQ = pos_dis.PAYEESEQ
            AND par_dis.effectivestartdate <= cr.genericdate2
            AND par_dis.effectiveenddate > cr.genericdate2
            AND par_dis.removedate = :DT_REMOVEDATE
            --for writing Agent postion info
        INNER JOIN
            cs_position pos_agt
            ON 'SGT'||IFNULL(ip.wri_agt_code,'') = pos_agt.name
            AND pos_agt.effectivestartdate <= cr.genericdate2
            AND pos_agt.effectiveenddate > cr.genericdate2
            AND pos_agt.removedate = :DT_REMOVEDATE
            -- and POS_AGT.GENERICATTRIBUTE6='AFA'
            --for writing Agent participant info
        INNER JOIN
            cs_participant par_agt
            ON par_agt.payeeseq = pos_agt.PAYEESEQ
            AND par_agt.effectivestartdate <= cr.genericdate2
            AND par_agt.effectiveenddate > cr.genericdate2
            AND par_agt.removedate = :DT_REMOVEDATE
            --for payor agent title info
        INNER JOIN
            cs_title title_agt
            ON title_agt.RULEELEMENTOWNERSEQ = pos_agt.TITLESEQ
            AND title_agt.effectivestartdate <= cr.genericdate2
            AND title_agt.effectiveenddate > cr.genericdate2
            AND title_agt.REMOVEDATE = :DT_REMOVEDATE
        WHERE
            tf.clawback_name IN (:STR_COMMISSION, :STR_GST_COMMISSION)
            AND tf.batch_number = :P_BATCH_NO
            -- and st.tenantid = 'AIAS'
            -- and cr.tenantid = 'AIAS'
            AND pos_agy.tenantid = 'AIAS'
            AND pos_dis.tenantid = 'AIAS'
            AND pos_agt.tenantid = 'AIAS'
            AND par_agt.tenantid = 'AIAS'
            AND title_agt.tenantid = 'AIAS'
            AND par_agy.tenantid = 'AIAS'
            AND par_dis.tenantid = 'AIAS'
            AND tf.BUNAME = :STR_BUNAME_FA
            AND IP.BUNAME = :STR_BUNAME_FA;

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_CLAWBACK_COMMISSION_FA' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('insert into EXT.AIA_CB_CLAWBACK_COMMISSION_FA'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    /**
    the below logic is to check the clawback policy has the negative SVI value in current measurement quarter.
    if yes, need to trace the same policy's clawback value of last quarter,
      if figure < 0, continue
      else if figure > 0, set current month clawback value = 0
    end
    **/

    --get clawback type and clawback name, only LUMPSUM case will apply this logic

    V_CB_TYPE = fn_get_cb_type(:P_BATCH_NO);

    --V_CB_NAME := fn_get_cb_name(P_BATCH_NO);

    V_CB_QTR = fn_get_cb_quarter(:P_BATCH_NO);

    IF :V_CB_TYPE = :STR_LUMPSUM
    THEN
        --get previous quarter batch number
        --V_BATCH_NO_PRE_QTR := fn_get_batch_no_pre_qtr(P_BATCH_NO);

        /* ORIGSQL: insert into EXT.AIA_CB_CLAWBACK_SVI_TMP select curr_cc.*, P_BATCH_NO from (SELECT wri_dist_code, wri_agy_code, wri_agt_code, ponumber, life_number, coverage_number, rider_number, component_code, product_n(...) */
        INSERT INTO EXT.AIA_CB_CLAWBACK_SVI_TMP
            SELECT   /* ORIGSQL: select curr_cc.*, P_BATCH_NO from (SELECT wri_dist_code, wri_agy_code, wri_agt_code, ponumber, life_number, coverage_number, rider_number, component_code, product_name, SUM(clawback)AS clawback FROM A(...) */
                curr_cc.*,
                :P_BATCH_NO
            FROM
                (
                    SELECT   /* ORIGSQL: (select wri_dist_code, wri_agy_code, wri_agt_code, ponumber, life_number, coverage_number, rider_number, component_code, product_name, SUM(clawback) as clawback from EXT.AIA_CB_CLAWBACK_COMMISSION where c(...) */
                        wri_dist_code,
                        wri_agy_code,
                        wri_agt_code,
                        ponumber,
                        life_number,
                        coverage_number,
                        rider_number,
                        component_code,
                        product_name,
                        SUM(clawback)AS clawback
                    FROM
                        EXT.AIA_CB_CLAWBACK_COMMISSION
                    WHERE
                        clawback_type = :STR_LUMPSUM
                        AND clawback_name = :STR_COMMISSION
                        AND batch_no = :P_BATCH_NO
                    GROUP BY
                        wri_dist_code,
                        wri_agy_code,
                        wri_agt_code,
                        ponumber,
                        life_number,
                        coverage_number,
                        rider_number,
                        component_code,
                        product_name
                    HAVING
                        SUM(clawback) > 0
                ) AS curr_cc
            LEFT OUTER JOIN
                (
                    SELECT   /* ORIGSQL: (select cc.wri_dist_code, cc.wri_agy_code, cc.wri_agt_code, cc.ponumber, cc.life_number, cc.coverage_number, cc.rider_number, cc.component_code, cc.product_name, SUM(cc.processed_clawback) as processe(...) */
                        cc.wri_dist_code,
                        cc.wri_agy_code,
                        cc.wri_agt_code,
                        cc.ponumber,
                        cc.life_number,
                        cc.coverage_number,
                        cc.rider_number,
                        cc.component_code,
                        cc.product_name,
                        /* --processed_clawback value should be updated after pipeline compeleted */
                        SUM(cc.processed_clawback) AS processed_clawback
                    FROM
                        EXT.AIA_CB_CLAWBACK_COMMISSION cc
                    INNER JOIN
                        (
                            SELECT   /* ORIGSQL: (select nvl(max(t.batchnum), 0) as batch_no from EXT.AIA_cb_batch_status t inner join (SELECT distinct quarter, year, cb_startdate, cb_enddate FROM EXT.AIA_cb_period where cb_name = STR_COMMISSION and BUNAME=(...) */
                                IFNULL(MAX(t.batchnum), 0) AS batch_no
                            FROM
                                EXT.AIA_cb_batch_status t
                            INNER JOIN
                                (
                                    SELECT   /* ORIGSQL: (select distinct quarter, year, cb_startdate, cb_enddate from EXT.AIA_cb_period where cb_name = STR_COMMISSION and BUNAME=STR_BUNAME_FA) */
                                        DISTINCT
                                        quarter,
                                        year,
                                        cb_startdate,
                                        cb_enddate
                                    FROM
                                        EXT.AIA_cb_period
                                    WHERE
                                        cb_name = :STR_COMMISSION
                                        AND BUNAME = :STR_BUNAME_FA
                                ) AS cbp
                                ON t.cb_quarter_name = IFNULL(cbp.year,'') || ' '|| IFNULL(cbp.quarter,'')
                            WHERE
                                t.islatest = 'Y'
                                AND t.BUNAME = :STR_BUNAME_FA
                                AND t.status = :STR_STATUS_COMPLETED_SH
                                AND t.clawbackname = :STR_COMMISSION
                                AND t.clawbacktype = :STR_LUMPSUM
                                AND t.cb_quarter_name <> :V_CB_QTR
                                AND TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE) >= cbp.cb_enddate
                            GROUP BY
                                t.cb_quarter_name, t.clawbackname, t.clawbacktype
                        ) AS pre_batch
                        ON cc.batch_no = pre_batch.batch_no
                    WHERE
                        cc.clawback_type = :STR_LUMPSUM
                        AND cc.clawback_name = :STR_COMMISSION
                    GROUP BY
                        cc.wri_dist_code,
                        cc.wri_agy_code,
                        cc.wri_agt_code,
                        cc.ponumber,
                        cc.life_number,
                        cc.coverage_number,
                        cc.rider_number,
                        cc.component_code,
                        cc.product_name
                    HAVING
                        SUM(cc.processed_clawback) < 0
                ) AS pre_cc
                ON curr_cc.wri_dist_code = pre_cc.wri_dist_code
                AND curr_cc.wri_agy_code = pre_cc.wri_agy_code
                AND curr_cc.wri_agt_code = pre_cc.wri_agt_code
                AND curr_cc.ponumber = pre_cc.ponumber
                AND curr_cc.life_number = pre_cc.life_number
                AND curr_cc.coverage_number = pre_cc.coverage_number
                AND curr_cc.rider_number = pre_cc.rider_number
                AND curr_cc.component_code = pre_cc.component_code
                AND curr_cc.product_name = pre_cc.product_name
            WHERE
                pre_cc.ponumber IS NULL;

        /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_CLAWBACK_SVI_TMP for FA' || '; row count: ' || to_char(sql%rowcount)) */
        CALL EXT.Log('insert into EXT.AIA_CB_CLAWBACK_SVI_TMP for FA'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
            );  /* ORIGSQL: to_char(sql%rowcount) */

        /* ORIGSQL: commit; */
        COMMIT;
    ELSEIF :V_CB_TYPE = :STR_ONGOING  /* ORIGSQL: elsif V_CB_TYPE = STR_ONGOING then */
    THEN
        /* ORIGSQL: insert into EXT.AIA_CB_CLAWBACK_SVI_TMP select curr_cc.*, P_BATCH_NO from (SELECT wri_dist_code, wri_agy_code, wri_agt_code, ponumber, life_number, coverage_number, rider_number, component_code, product_n(...) */
        INSERT INTO EXT.AIA_CB_CLAWBACK_SVI_TMP
            SELECT   /* ORIGSQL: select curr_cc.*, P_BATCH_NO from (SELECT wri_dist_code, wri_agy_code, wri_agt_code, ponumber, life_number, coverage_number, rider_number, component_code, product_name, SUM(clawback)AS clawback FROM A(...) */
                curr_cc.*,
                :P_BATCH_NO
            FROM
                (
                    SELECT   /* ORIGSQL: (select wri_dist_code, wri_agy_code, wri_agt_code, ponumber, life_number, coverage_number, rider_number, component_code, product_name, SUM(clawback) as clawback from EXT.AIA_CB_CLAWBACK_COMMISSION where c(...) */
                        wri_dist_code,
                        wri_agy_code,
                        wri_agt_code,
                        ponumber,
                        life_number,
                        coverage_number,
                        rider_number,
                        component_code,
                        product_name,
                        SUM(clawback)AS clawback
                    FROM
                        EXT.AIA_CB_CLAWBACK_COMMISSION
                    WHERE
                        clawback_type = :STR_ONGOING
                        AND clawback_name = :STR_COMMISSION
                        AND batch_no = :P_BATCH_NO
                    GROUP BY
                        wri_dist_code,
                        wri_agy_code,
                        wri_agt_code,
                        ponumber,
                        life_number,
                        coverage_number,
                        rider_number,
                        component_code,
                        product_name
                    HAVING
                        SUM(clawback) > 0
                ) AS curr_cc
            LEFT OUTER JOIN
                (
                    SELECT   /* ORIGSQL: (select cc.wri_dist_code, cc.wri_agy_code, cc.wri_agt_code, cc.ponumber, cc.life_number, cc.coverage_number, cc.rider_number, cc.component_code, cc.product_name, SUM(cc.processed_clawback) as processe(...) */
                        cc.wri_dist_code,
                        cc.wri_agy_code,
                        cc.wri_agt_code,
                        cc.ponumber,
                        cc.life_number,
                        cc.coverage_number,
                        cc.rider_number,
                        cc.component_code,
                        cc.product_name,
                        /* --processed_clawback value should be updated after pipeline compeleted */
                        SUM(cc.processed_clawback) AS processed_clawback
                    FROM
                        EXT.AIA_CB_CLAWBACK_COMMISSION cc
                    INNER JOIN
                        (
                            --lumpsum batch number

                            SELECT   /* ORIGSQL: (select nvl(max(t.batchnum), 0) as batch_no from EXT.AIA_cb_batch_status t inner join (SELECT distinct quarter, year, cb_startdate, cb_enddate FROM EXT.AIA_cb_period where cb_name = STR_COMMISSION and BUNAME=(...) */
                                IFNULL(MAX(t.batchnum), 0) AS batch_no
                            FROM
                                EXT.AIA_cb_batch_status t
                            INNER JOIN
                                (
                                    SELECT   /* ORIGSQL: (select distinct quarter, year, cb_startdate, cb_enddate from EXT.AIA_cb_period where cb_name = STR_COMMISSION and BUNAME=STR_BUNAME_FA) */
                                        DISTINCT
                                        quarter,
                                        year,
                                        cb_startdate,
                                        cb_enddate
                                    FROM
                                        EXT.AIA_cb_period
                                    WHERE
                                        cb_name = :STR_COMMISSION
                                        AND BUNAME = :STR_BUNAME_FA
                                ) AS cbp
                                ON t.cb_quarter_name = IFNULL(cbp.year,'') || ' '|| IFNULL(cbp.quarter,'')
                            WHERE
                                t.islatest = 'Y'
                                AND T.BUNAME = :STR_BUNAME_FA
                                AND t.status = :STR_STATUS_COMPLETED_SH
                                AND t.clawbackname = :STR_COMMISSION
                                AND t.clawbacktype = :STR_LUMPSUM
                                AND t.cb_quarter_name <> :V_CB_QTR
                                AND TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE) >= cbp.cb_enddate
                            GROUP BY
                                t.cb_quarter_name, t.clawbackname, t.clawbacktype
                    UNION
                        --on-going batch number

                        SELECT   /* ORIGSQL: select nvl(max(t.batchnum), 0) as batch_no from EXT.AIA_cb_batch_status t where t.islatest = 'Y' and T.BUNAME=STR_BUNAME_FA and t.status = STR_STATUS_COMPLETED_SH and t.clawbackname = STR_COMMISSION and t(...) */
                            IFNULL(MAX(t.batchnum), 0) AS batch_no
                        FROM
                            EXT.AIA_cb_batch_status t
                        WHERE
                            t.islatest = 'Y'
                            AND T.BUNAME = :STR_BUNAME_FA
                            AND t.status = :STR_STATUS_COMPLETED_SH --'completed_sh'
                            AND t.clawbackname = :STR_COMMISSION--'COMMISSION'
                            AND t.clawbacktype = :STR_ONGOING --'ONGOING'
                            AND TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE) > t.cycledate
                    ) AS pre_batch
                    ON cc.batch_no = pre_batch.batch_no
                WHERE
                    cc.clawback_name = :STR_COMMISSION
                GROUP BY
                    cc.wri_dist_code,
                    cc.wri_agy_code,
                    cc.wri_agt_code,
                    cc.ponumber,
                    cc.life_number,
                    cc.coverage_number,
                    cc.rider_number,
                    cc.component_code,
                    cc.product_name
                HAVING
                    SUM(cc.processed_clawback) < 0
            ) AS pre_cc
            ON curr_cc.wri_dist_code = pre_cc.wri_dist_code
            AND curr_cc.wri_agy_code = pre_cc.wri_agy_code
            AND curr_cc.wri_agt_code = pre_cc.wri_agt_code
            AND curr_cc.ponumber = pre_cc.ponumber
            AND curr_cc.life_number = pre_cc.life_number
            AND curr_cc.coverage_number = pre_cc.coverage_number
            AND curr_cc.rider_number = pre_cc.rider_number
            AND curr_cc.component_code = pre_cc.component_code
            AND curr_cc.product_name = pre_cc.product_name
        WHERE
            pre_cc.ponumber IS NULL;

        /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_CLAWBACK_SVI_TMP for FA' || '; row count: ' || to_char(sql%rowcount)) */
        CALL EXT.Log('insert into EXT.AIA_CB_CLAWBACK_SVI_TMP for FA'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
            );  /* ORIGSQL: to_char(sql%rowcount) */

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    --update the table EXT.AIA_CB_CLAWBACK_COMMISSION for special handling for positive clawback
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into EXT.AIA_CB_CLAWBACK_COMMISSION cc using EXT.AIA_CB_CLAWBACK_SVI_TMP st on (cc.wri_dist_code = st.wri_dist_code and cc.wri_agy_code = st.wri_agy_code and cc.wri_agt_code = st.wri_agt_code and cc.pon(...) */
    MERGE INTO EXT.AIA_CB_CLAWBACK_COMMISSION AS cc
        USING EXT.AIA_CB_CLAWBACK_SVI_TMP st
        ON (cc.wri_dist_code = st.wri_dist_code
            AND cc.wri_agy_code = st.wri_agy_code
            AND cc.wri_agt_code = st.wri_agt_code
            AND cc.ponumber = st.ponumber
            AND cc.life_number = st.life_number
            AND cc.coverage_number = st.coverage_number
            AND cc.rider_number = st.rider_number
            AND cc.component_code = st.component_code
            AND cc.product_name = st.product_name
            AND cc.batch_no = st.batch_no
            AND cc.batch_no = :P_BATCH_NO
        )
    WHEN MATCHED THEN
        UPDATE SET cc.clawback = 0;

    /* ORIGSQL: EXT.Log('merge into EXT.AIA_CB_CLAWBACK_COMMISSION_FA' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('merge into EXT.AIA_CB_CLAWBACK_COMMISSION_FA'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: EXT.Log('SP_CLAWBACK_COMMISSION_FA end') */
    CALL EXT.Log('SP_CLAWBACK_COMMISSION_FA end');
END;
  PUBLIC PROCEDURE SP_EXEC_COMMISSION_LUMPSUM_FA
(
    IN P_STR_CB_CYCLEDATE VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                         /* ORIGSQL: P_STR_CB_CYCLEDATE IN VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE V_LUMPSUM_FLAG DECIMAL(38,10);  /* ORIGSQL: V_LUMPSUM_FLAG NUMBER; */
    DECLARE V_BATCH_NO DECIMAL(38,10);  /* ORIGSQL: V_BATCH_NO NUMBER; */
    DECLARE v_message VARCHAR(2000);  /* ORIGSQL: v_message VARCHAR2(2000); */
    DECLARE V_CB_YEAR VARCHAR(20);  /* ORIGSQL: V_CB_YEAR VARCHAR2(20); */
    DECLARE V_CB_QUARTER VARCHAR(20);  /* ORIGSQL: V_CB_QUARTER VARCHAR2(20); */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            v_message = SUBSTRING(::SQL_ERROR_MESSAGE,1,2000);  /* ORIGSQL: SUBSTR(SQLERRM,1,2000) */

            /* ORIGSQL: EXT.Log(v_message) */
            CALL EXT.Log(:v_message);

            /* ORIGSQL: sp_update_batch_status(V_BATCH_NO, STR_STATUS_FAIL) */
            CALL sp_update_batch_status(:V_BATCH_NO, :STR_STATUS_FAIL);
        END;

        /* ORIGSQL: init */
        CALL init();

        SELECT
            COUNT(1) 
        INTO
            V_LUMPSUM_FLAG
        FROM
            EXT.AIA_CB_PERIOD
        WHERE
            CB_CYCLEDATE = TO_DATE(:P_STR_CB_CYCLEDATE,:STR_DATE_FORMAT_TYPE)
            AND cb_name = :STR_COMMISSION
            AND BUNAME = :STR_BUNAME_FA;

        IF :V_LUMPSUM_FLAG > 0
        THEN
            --LUMPSUM
            /* ORIGSQL: sp_create_batch_no_fa(P_STR_CB_CYCLEDATE,STR_LUMPSUM,STR_COMMISSION) */
            CALL sp_create_batch_no_FA(:P_STR_CB_CYCLEDATE, :STR_LUMPSUM, :STR_COMMISSION);

            V_BATCH_NO = fn_get_batch_no_fa(:P_STR_CB_CYCLEDATE, :STR_COMMISSION, :STR_LUMPSUM, :STR_STATUS_START);

            --SP_POLICY_EXCL(P_STR_CB_CYCLEDATE, STR_COMMISSION);
            --SP_IDENTIFY_POLICY (P_STR_CB_CYCLEDATE, STR_COMMISSION);
            /* ORIGSQL: sp_revert_by_batch(V_BATCH_NO) */
            CALL sp_revert_by_batch(:V_BATCH_NO);

            /*         --get clawback year and quarter from clawback period table
                     select cbp.year, cbp.quarter
                     into V_CB_YEAR, V_CB_QUARTER
                     from EXT.AIA_cb_period cbp where cbp.cb_cycledate = to_date(P_STR_CB_CYCLEDATE, STR_DATE_FORMAT_TYPE);
                     --run report for identify policy result
                     PK_EXT.AIA_CB_REPORTS.SP_RPT_CB_MASTER_POLICY (V_CB_YEAR , V_CB_QUARTER);  */

            /* ORIGSQL: SP_TRACE_FORWARD_COMMISSION_FA (P_STR_CB_CYCLEDATE,STR_LUMPSUM, V_BATCH_NO) */
            CALL SP_TRACE_FORWARD_COMMISSION_FA(:P_STR_CB_CYCLEDATE, :STR_LUMPSUM, :V_BATCH_NO);

            /* ORIGSQL: SP_CLAWBACK_COMMISSION_FA (P_STR_CB_CYCLEDATE, V_BATCH_NO) */
            CALL SP_CLAWBACK_COMMISSION_FA(:P_STR_CB_CYCLEDATE, :V_BATCH_NO);

            /* ORIGSQL: sp_update_batch_status (V_BATCH_NO, STR_STATUS_COMPLETED_SP) */
            CALL sp_update_batch_status(:V_BATCH_NO, :STR_STATUS_COMPLETED_SP);
        ELSE 
            /* ORIGSQL: EXT.Log(P_STR_CB_CYCLEDATE || ' is not the avaiable clawback cycle date for FA') */
            CALL EXT.Log(IFNULL(:P_STR_CB_CYCLEDATE,'') || ' is not the avaiable clawback cycle date for FA');
        END IF;
        ---catch exception
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
    END;
  PUBLIC PROCEDURE SP_EXEC_COMMISSION_ONGOING_FA
(
    IN P_STR_CB_CYCLEDATE VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                         /* ORIGSQL: P_STR_CB_CYCLEDATE IN VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE V_STR_CB_TYPE VARCHAR(20);  /* ORIGSQL: V_STR_CB_TYPE VARCHAR2(20); */
    DECLARE V_BATCH_NO DECIMAL(38,10);  /* ORIGSQL: V_BATCH_NO NUMBER; */
    DECLARE V_WEEKEND_FLAG DECIMAL(38,10);  /* ORIGSQL: V_WEEKEND_FLAG NUMBER; */
    DECLARE V_MONTHEND_FLAG DECIMAL(38,10);  /* ORIGSQL: V_MONTHEND_FLAG NUMBER; */
    DECLARE V_MESSAGE VARCHAR(2000);  /* ORIGSQL: V_MESSAGE VARCHAR2(2000); */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            V_MESSAGE = SUBSTRING(::SQL_ERROR_MESSAGE,1,2000);  /* ORIGSQL: SUBSTR(SQLERRM,1,2000) */

            /* ORIGSQL: EXT.Log(v_message) */
            CALL EXT.Log(:V_MESSAGE);

            /* ORIGSQL: sp_update_batch_status(V_BATCH_NO, STR_STATUS_FAIL) */
            CALL sp_update_batch_status(:V_BATCH_NO, :STR_STATUS_FAIL);
        END;

        /* initialize library variables, if not yet done */
        CALL init_session_global();

        /* ORIGSQL: init */
        CALL init();

        /* ORIGSQL: EXT.Log('Start') */
        CALL EXT.Log('Start');

        ---to define the run type

        SELECT
            COUNT(1)
        INTO
            V_WEEKEND_FLAG
        FROM
            EXT.IN_ETL_CONTROL CTL
        WHERE
            CTL.TXT_KEY_STRING = 'PAYMENT_END_DATE_WEEKLY'
            AND CTL.TXT_FILE_NAME = :STR_PU
            AND CTL.TXT_KEY_VALUE = :P_STR_CB_CYCLEDATE;

        SELECT
            COUNT(1) 
        INTO
            V_MONTHEND_FLAG
        FROM
            CS_PERIOD CSP
        WHERE
            TO_DATE(ADD_SECONDS(CSP.ENDDATE,(86400*-1))) = TO_DATE(:P_STR_CB_CYCLEDATE,:STR_DATE_FORMAT_TYPE)  /* ORIGSQL: CSP.ENDDATE - 1 */
            AND CSP.CALENDARSEQ = :V_CALENDARSEQ
            AND CSP.PERIODTYPESEQ 
            =
            (
                SELECT   /* ORIGSQL: (select periodtypeseq from cs_periodtype where name = STR_CALENDAR_TYPE) */
                    periodtypeseq
                FROM
                    cs_periodtype
                WHERE
                    name = :STR_CALENDAR_TYPE
            )
            AND CSP.removedate = to_date('2200-01-01','yyyy-mm-dd');--Cosimo
        /* ORIGSQL: to_date('2200-01-01','yyyy-mm-dd') */

        /* ORIGSQL: EXT.Log(V_WEEKEND_FLAG+V_MONTHEND_FLAG || ' Flag') */
        CALL EXT.Log(:V_WEEKEND_FLAG+IFNULL(TO_VARCHAR(:V_MONTHEND_FLAG),'') || ' Flag');

        IF :V_WEEKEND_FLAG+:V_MONTHEND_FLAG > 0
        THEN
            /* ORIGSQL: EXT.Log(V_WEEKEND_FLAG+V_MONTHEND_FLAG || ' Flag') */
            CALL EXT.Log(:V_WEEKEND_FLAG+IFNULL(TO_VARCHAR(:V_MONTHEND_FLAG),'') || ' Flag');

            --ONGOING
            /* ORIGSQL: sp_create_batch_no_fa(P_STR_CB_CYCLEDATE,STR_ONGOING,STR_COMMISSION) */
            CALL sp_create_batch_no_FA(:P_STR_CB_CYCLEDATE, :STR_ONGOING, :STR_COMMISSION);

            V_BATCH_NO = fn_get_batch_no_fa(:P_STR_CB_CYCLEDATE, :STR_COMMISSION, :STR_ONGOING, :STR_STATUS_START);

            /* ORIGSQL: sp_revert_by_batch(V_BATCH_NO) */
            CALL sp_revert_by_batch(:V_BATCH_NO);

            /* ORIGSQL: EXT.Log(V_BATCH_NO || ' Bacth No') */
            CALL EXT.Log(IFNULL(TO_VARCHAR(:V_BATCH_NO),'') || ' Bacth No');

            /* ORIGSQL: SP_TRACE_FORWARD_COMMISSION_FA (P_STR_CB_CYCLEDATE,STR_ONGOING, V_BATCH_NO) */
            CALL SP_TRACE_FORWARD_COMMISSION_FA(:P_STR_CB_CYCLEDATE, :STR_ONGOING, :V_BATCH_NO);

            /* ORIGSQL: SP_CLAWBACK_COMMISSION_FA (P_STR_CB_CYCLEDATE, V_BATCH_NO) */
            CALL SP_CLAWBACK_COMMISSION_FA(:P_STR_CB_CYCLEDATE, :V_BATCH_NO);

            /* ORIGSQL: sp_update_batch_status (V_BATCH_NO, STR_STATUS_COMPLETED_SP) */
            CALL sp_update_batch_status(:V_BATCH_NO, :STR_STATUS_COMPLETED_SP);
        END IF;
        ---catch exception
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
    END;
  PUBLIC PROCEDURE SP_TRACE_FORWARD_COMP_FA
(
    IN P_STR_CYCLEDATE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                       /* ORIGSQL: P_STR_CYCLEDATE IN VARCHAR2 */
    IN P_STR_TYPE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                  /* ORIGSQL: P_STR_TYPE IN VARCHAR2 */
    IN P_BATCH_NO BIGINT     /* ORIGSQL: P_BATCH_NO IN INTEGER */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/
    -- DECLARE DBMTK_CTV_PROCID INT := sapdbmtk.sp_f_dbmtk_ctv_procid(); /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE STR_CB_NAME CONSTANT VARCHAR(20) = 'COMPENSATION';  /* ORIGSQL: STR_CB_NAME CONSTANT VARCHAR2(20) := 'COMPENSATION'; */
    DECLARE V_CAL_PERIOD VARCHAR(30);  /* ORIGSQL: V_CAL_PERIOD VARCHAR2(30); */

    --measurement quarter
    DECLARE DT_CB_START_DATE TIMESTAMP;  /* ORIGSQL: DT_CB_START_DATE DATE; */
    DECLARE DT_CB_END_DATE TIMESTAMP;  /* ORIGSQL: DT_CB_END_DATE DATE; */
    DECLARE DT_INCEPTION_START_DATE TIMESTAMP;  /* ORIGSQL: DT_INCEPTION_START_DATE DATE; */
    DECLARE DT_INCEPTION_END_DATE TIMESTAMP;  /* ORIGSQL: DT_INCEPTION_END_DATE DATE; */
    DECLARE NUM_OF_CYCLE_IND BIGINT;  /* ORIGSQL: NUM_OF_CYCLE_IND integer; */
    DECLARE RECORD_CNT_ONGOING BIGINT;  /* ORIGSQL: RECORD_CNT_ONGOING integer; */
    DECLARE ts_periodseq BIGINT;  /* ORIGSQL: ts_periodseq integer; */
    DECLARE V_NADOR_RATE DECIMAL(10,2);  /* ORIGSQL: V_NADOR_RATE NUMBER(10,2); */
    DECLARE V_NLPI_RATE DECIMAL(10,2);  /* ORIGSQL: V_NLPI_RATE NUMBER(10,2); */

    DECLARE t_periodseq BIGINT;  /* ORIGSQL: t_periodseq periodseq_type; */
    DECLARE ONGOING_ST_DT TIMESTAMP;  /* ORIGSQL: ONGOING_ST_DT DATE; */
    DECLARE ONGOING_END_DT TIMESTAMP;  /* ORIGSQL: ONGOING_END_DT DATE; */
    DECLARE ONGOING_PERIOD VARCHAR(50);  /* ORIGSQL: ONGOING_PERIOD VARCHAR2(50); */
    DECLARE V_period_seq2 BIGINT;
    DECLARE V_First_QTR NVARCHAR(50);
    DECLARE V_Second_QTR NVARCHAR(50);

    /* ORIGSQL: for v_comls_period in (select periodseq from EXT.AIA_tmp_comls_period) loop insert into ext.aias_tx_temp2 select ip.tenantid, salestransactionseq, salesorderseq, linenumber, sublinenumber, eventtypeseq, ip.pi(...) */
    DECLARE CURSOR dbmtk_cursor_22755
    FOR
        SELECT   /* ORIGSQL: select periodseq from EXT.AIA_tmp_comls_period; */
            periodseq
        FROM
            EXT.AIA_tmp_comls_period;

    /* initialize library variables, if not yet done */
    CALL init_session_global();

    -- define period seq of each month

    /*--- TYPE definition moved to  scripts/dbmtk_create_types.sqlscript ---
    ----- Converted type 'periodseq_type' to 'EXT.AIA_CB_CALCULATION_FA__SP_TRACE_FORWARD_COMP_FA__periodseq_type'
    TYPE periodseq_type IS TABLE OF cs_period.periodseq%TYPE;
    ---end of TYPE definition commented out---*/ 

    /* ORIGSQL: init */
    CALL init();

    --update status
    /* ORIGSQL: sp_update_batch_status (P_BATCH_NO,'processing') */
    CALL sp_update_batch_status(:P_BATCH_NO, 'processing');

    /* ORIGSQL: EXT.Log('SP_TRACE_FORWARD_COMP_FA start') */
    CALL EXT.Log('SP_TRACE_FORWARD_COMP_FA start');

    --Get the periodseq for Ongoing period
    IF :P_STR_TYPE = :STR_ONGOING
    THEN  
        SELECT
            COUNT(1) 
        INTO
            RECORD_CNT_ONGOING
        FROM
            cs_period csp
        INNER JOIN
            cs_periodtype pt
            ON csp.periodtypeseq = pt.periodtypeseq
        WHERE
            csp.enddate = TO_DATE(ADD_SECONDS(TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE),(86400*1)))   /* ORIGSQL: TO_DATE(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) + 1 */
            AND csp.removedate = :DT_REMOVEDATE
            AND csp.calendarseq = :V_CALENDARSEQ
            AND pt.name = :STR_CALENDAR_TYPE;

        IF :RECORD_CNT_ONGOING = 0
        THEN
            RETURN;  /* ORIGSQL: goto ProcDone; */
        END IF;  

        SELECT
            csp.periodseq,
            csp.startdate,
            TO_DATE(ADD_SECONDS(csp.enddate,(86400*-1))),  /* ORIGSQL: csp.enddate-1 */
            csp.name
        INTO
            ts_periodseq,
            ONGOING_ST_DT,
            ONGOING_END_DT,
            ONGOING_PERIOD
        FROM
            cs_period csp
        INNER JOIN
            cs_periodtype pt
            ON csp.periodtypeseq = pt.periodtypeseq
        WHERE
            csp.enddate = TO_DATE(ADD_SECONDS(TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE),(86400*1)))   /* ORIGSQL: TO_DATE(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) + 1 */
            AND csp.removedate = :DT_REMOVEDATE
            AND csp.calendarseq = :V_CALENDARSEQ
            AND pt.name = :STR_CALENDAR_TYPE;

        /* ORIGSQL: EXT.Log('FA DT_ONGOING_START_DATE = ' || ONGOING_ST_DT) */
        CALL EXT.Log('FA DT_ONGOING_START_DATE = '|| IFNULL(TO_VARCHAR(:ONGOING_ST_DT),''));

        /* ORIGSQL: EXT.Log('FA DT_ONGOING_END_DATE = ' || ONGOING_END_DT) */
        CALL EXT.Log('FA DT_ONGOING_END_DATE = '|| IFNULL(TO_VARCHAR(:ONGOING_END_DT),''));

        --delete from EXT.AIA_CB_TRACE_FORWARD_COMP where CLAWBACK_TYPE= P_STR_TYPE and  transaction_date between ONGOING_ST_DT and ONGOING_END_DT;
        --and CLAWBACK_NAME not in (STR_COMMISSION, STR_GST_COMMISSION);

        --commit;
    ELSE 
        SELECT
            COUNT(1) 
        INTO
            NUM_OF_CYCLE_IND
        FROM
            EXT.AIA_CB_PERIOD cbp
        WHERE
            cbp.CB_CYCLEDATE = TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
            AND cb_name = :STR_CB_NAME
            AND cbp.buname = :STR_BUNAME_FA;
        -- add cb_name here

        IF :NUM_OF_CYCLE_IND = 0
        THEN
            RETURN;  /* ORIGSQL: goto ProcDone; */
        END IF;

        --get calculation period name, clawback start date and end date for lumpsum compensation
        SELECT
            IFNULL(cbp.quarter,'') || ' ' || IFNULL(cbp.year,''),
            cbp.cb_startdate,
            cbp.cb_enddate,
            cbp.inception_startdate,
            cbp.inception_enddate
        INTO
            V_CAL_PERIOD,
            DT_CB_START_DATE,
            DT_CB_END_DATE,
            DT_INCEPTION_START_DATE,
            DT_INCEPTION_END_DATE
        FROM
            EXT.AIA_CB_PERIOD cbp
        WHERE
            CB_CYCLEDATE = TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
            AND cb_name = :STR_CB_NAME
            AND cbp.buname = :STR_BUNAME_FA;

        /* ORIGSQL: EXT.Log('FA DT_LUMPSUM_START_DATE = ' || DT_CB_START_DATE) */
        CALL EXT.Log('FA DT_LUMPSUM_START_DATE = '|| IFNULL(TO_VARCHAR(:DT_CB_START_DATE),''));

        /* ORIGSQL: EXT.Log('FA DT_LUMPSUM_END_DATE = ' || DT_CB_END_DATE) */
        CALL EXT.Log('FA DT_LUMPSUM_END_DATE = '|| IFNULL(TO_VARCHAR(:DT_CB_END_DATE),''));

        -- Get the periodseqs for lumpsum period  
        /* ORIGSQL: select periodseq BULK COLLECT into t_periodseq from cs_period csp inner join cs_periodtype pt on csp.periodtypeseq = pt.periodtypeseq where csp.startdate >= DT_CB_START_DATE and csp.enddate <= DT_CB_E(...) */
        dbmtk_bulk_tabvar_22550 =   
        SELECT
            periodseq
            /* ORIGSQL: BULK COLLECT into t_periodseq */
        FROM
            cs_period csp
        INNER JOIN
            cs_periodtype pt
            ON csp.periodtypeseq = pt.periodtypeseq
        WHERE
            csp.startdate >= :DT_CB_START_DATE
            AND csp.enddate <= TO_DATE(ADD_SECONDS(:DT_CB_END_DATE,(86400*1)))   /* ORIGSQL: DT_CB_END_DATE + 1 */
            AND csp.removedate = :DT_REMOVEDATE
            AND csp.calendarseq = :V_CALENDARSEQ
            AND pt.name = :STR_CALENDAR_TYPE;
/*Deepan : Below code not not required. :tperiodSeq not used anywhere*/
        -- t_periodseq = SELECT periodseq AS column_value, TO_INT(ROW_NUMBER() OVER ()) AS dbmtk_ix_col FROM :dbmtk_bulk_tabvar_22550;  /* ORIGSQL: SELECT-INTO..BULK COLLECT INTO..t_periodseq */
        -- t_dbmtk_ix_col = UNNEST(ARRAY_AGG(:t_periodseq.dbmtk_ix_col)) AS (dbmtk_ix_col); CALL sapdbmtk.sp_dbmtk_ctv_pushix('t_periodseq',:DBMTK_CTV_PROCID,:t_dbmtk_ix_col);

        /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_comls_period'; */
        /* ORIGSQL: truncate table EXT.AIA_tmp_comls_period ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_period';

        /* ORIGSQL: insert into EXT.AIA_tmp_comls_period select periodseq from cs_period csp inner join cs_periodtype pt on csp.periodtypeseq = pt.periodtypeseq where csp.startdate >= DT_CB_START_DATE and csp.enddate <= DT_C(...) */
        INSERT INTO EXT.AIA_tmp_comls_period
            SELECT   /* ORIGSQL: select periodseq from cs_period csp inner join cs_periodtype pt on csp.periodtypeseq = pt.periodtypeseq where csp.startdate >= DT_CB_START_DATE and csp.enddate <= DT_CB_END_DATE + 1 and csp.removedate(...) */
                periodseq
            FROM
                cs_period csp
            INNER JOIN
                cs_periodtype pt
                ON csp.periodtypeseq = pt.periodtypeseq
            WHERE
                csp.startdate >= :DT_CB_START_DATE
                AND csp.enddate <= TO_DATE(ADD_SECONDS(:DT_CB_END_DATE,(86400*1)))   /* ORIGSQL: DT_CB_END_DATE + 1 */
                AND csp.removedate = :DT_REMOVEDATE
                AND csp.calendarseq = :V_CALENDARSEQ
                AND pt.name = :STR_CALENDAR_TYPE;

        /* ORIGSQL: commit; */
        COMMIT;

        --delete from EXT.AIA_CB_TRACE_FORWARD_COMP where CLAWBACK_TYPE= P_STR_TYPE and CALCULATION_PERIOD=V_CAL_PERIOD; --and CLAWBACK_NAME not in (STR_COMMISSION, STR_GST_COMMISSION);
        --commit;

        --Version 2 add by Amanda to get quarter end period begin
        /* ORIGSQL: execute immediate 'truncate table EXT.AIA_TMP_COMLS_PERIOD_SPI'; */
        /* ORIGSQL: truncate table EXT.AIA_TMP_COMLS_PERIOD_SPI ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_TMP_COMLS_PERIOD_SPI';

        --get 6 lumpsum months for SPI
        /* ORIGSQL: insert into EXT.AIA_TMP_COMLS_PERIOD_SPI select csp.periodseq, CASE WHEN csp_qtr.name IS NOT NULL THEN EXTRACT(YEAR FROM csp.startdate) || '0' || substr(csp_qtr.name,2,1) ELSE null END,csp.parentseq,'',0 (...) */
        INSERT INTO EXT.AIA_TMP_COMLS_PERIOD_SPI
            SELECT   /* ORIGSQL: select csp.periodseq, CASE WHEN csp_qtr.name IS NOT NULL THEN EXTRACT(YEAR FROM csp.startdate) || '0' || substr(csp_qtr.name,2,1) ELSE null END,csp.parentseq,'',0 from cs_period csp left join cs_perio(...) */
                csp.periodseq,
                CASE
                    WHEN csp_qtr.name IS NOT NULL
                    THEN IFNULL(TO_VARCHAR(EXTRACT(YEAR FROM csp.startdate)),'') || '0' || IFNULL(SUBSTRING(csp_qtr.name,2,1),'')  /* ORIGSQL: substr(csp_qtr.name,2,1) */
                    ELSE NULL
                END
                ,
                csp.parentseq,
                '',
                0
            FROM
                cs_period csp
            LEFT OUTER JOIN
                cs_period csp_qtr
                ON csp_qtr.periodtypeseq = :V_periodtype_quarter_seq
                AND csp_qtr.enddate = csp.enddate
                AND csp_qtr.removedate = :DT_REMOVEDATE
            WHERE
                csp.startdate >= :DT_CB_START_DATE
                AND csp.enddate <= TO_DATE(ADD_SECONDS(ADD_MONTHS(:DT_CB_END_DATE,-2),(86400*1)))   /* ORIGSQL: ADD_MONTHS(DT_CB_END_DATE,-2) + 1 */
                AND csp.removedate = :DT_REMOVEDATE
                AND csp.calendarseq = :V_CALENDARSEQ
                AND csp.periodtypeseq = :V_periodtype_month_seq;

        /* ORIGSQL: commit; */
        COMMIT;

        --update quarter end month periodseq for traceforward, can't delete!
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into EXT.AIA_TMP_COMLS_PERIOD_SPI tmp1 using (SELECT periodseq, year_qtr, qtr_periodseq, qtr_end_periodseq, ROW_NUMBER() OVER (ORDER BY year_qtr asc) AS qtr_order FROM EXT.AIA_TMP_COMLS_PERIOD_SPI where(...) */
        MERGE INTO EXT.AIA_TMP_COMLS_PERIOD_SPI AS tmp1
            USING
            (
                SELECT   /* ORIGSQL: (select periodseq,year_qtr,qtr_periodseq,qtr_end_periodseq,ROW_NUMBER() OVER (ORDER BY year_qtr asc) as qtr_order from EXT.AIA_TMP_COMLS_PERIOD_SPI where year_qtr IS NOT NULL) */
                    periodseq,
                    year_qtr,
                    qtr_periodseq,
                    qtr_end_periodseq,
                    ROW_NUMBER() OVER (ORDER BY year_qtr ASC) AS qtr_order
                FROM
                    EXT.AIA_TMP_COMLS_PERIOD_SPI
                WHERE
                    year_qtr IS NOT NULL
            ) AS tmp
            ON (tmp1.qtr_periodseq = tmp.qtr_periodseq)
        WHEN MATCHED THEN
            UPDATE
                SET tmp1.qtr_end_periodseq = tmp.periodseq,tmp1.qtr_order = tmp.qtr_order;

        /* ORIGSQL: commit; */
        COMMIT;

        SELECT
            qtr_end_periodseq
        INTO
            V_period_seq2
        FROM
            EXT.AIA_TMP_COMLS_PERIOD_SPI
        WHERE
            year_qtr IS NOT NULL
            AND qtr_order = 2;

        --Version 2 end
    END IF;

    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_FIXEDVALUE' not found */

    SELECT
        value
    INTO
        V_NADOR_RATE
    FROM
        CS_FIXEDVALUE fv
    WHERE
        name = 'FV_NADOR_Payout_Rate'
        AND Removedate = :DT_REMOVEDATE;

    SELECT
        value
    INTO
        V_NLPI_RATE
    FROM
        CS_FIXEDVALUE fv
    WHERE
        name = 'FV_NLPI_RATE'
        AND Removedate = :DT_REMOVEDATE;

    IF :P_STR_TYPE = :STR_LUMPSUM
    THEN
        /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for FYO,RYO,FSM_RYO,NLPI' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO) */
        CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for FYO,RYO,FSM_RYO,NLPI'|| 'clawback type = '|| IFNULL(:P_STR_TYPE,'') ||', batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO),''));

        --for i in 1..t_periodseq.count loop
        /* ORIGSQL: EXT.Log('EXT.AIA_CB_TRACE_FORWARD_COMP  for FA'|| ' '||V_CAL_PERIOD) */
        CALL EXT.Log('EXT.AIA_CB_TRACE_FORWARD_COMP  for FA'|| ' '||IFNULL(:V_CAL_PERIOD,''));

        --for lumpsum compensation trace forward for 'FYO','RYO','FSM_RYO','NLPI'
        /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_Comls_Step0_1'; */
        /* ORIGSQL: truncate table EXT.AIA_tmp_Comls_Step0_1 ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_Comls_Step0_1';

        /* ORIGSQL: insert into EXT.AIA_tmp_Comls_Step0_1 select st.salestransactionseq, ip.wri_agt_code as wri_agt_code_ORIG, ip.quarter || ' ' || ip.year as CALCULATION_PERIOD, ip.ponumber as POLICY_NUMBER, ip.policyidseq (...) */
        INSERT INTO EXT.AIA_tmp_Comls_Step0_1
            /* ORIGSQL: select / *+ leading(ip,st) * / */
            SELECT   /* ORIGSQL: select st.salestransactionseq, ip.wri_agt_code as wri_agt_code_ORIG, ip.quarter || ' ' || ip.year as CALCULATION_PERIOD, ip.ponumber as POLICY_NUMBER, ip.policyidseq as POLICYIDSEQ, ip.life_number as (...) */
                st.salestransactionseq,
                ip.wri_agt_code AS wri_agt_code_ORIG,
                IFNULL(ip.quarter,'') || ' ' || IFNULL(ip.year,'') AS CALCULATION_PERIOD,
                ip.ponumber AS POLICY_NUMBER,
                ip.policyidseq AS POLICYIDSEQ,
                ip.life_number AS LIFE_NUMBER,
                ip.coverage_number AS COVERAGE_NUMBER,
                ip.rider_number AS RIDER_NUMBER,
                ip.component_code AS COMPONENT_CODE,
                ip.component_name AS COMPONENT_NAME,
                ip.base_rider_ind AS BASE_RIDER_IND,
                st.genericnumber2 AS COMMISSION_RATE,
                st.genericdate4 AS PAID_TO_DATE,
                'SGT'||IFNULL(ip.wri_agt_code,'') AS wri_agt_code,
                IFNULL(ip.quarter,'') || ' ' || IFNULL(ip.year,'') AS qtrYr,
                '',
                ''
            FROM
                cs_Salestransaction st
            INNER JOIN
                EXT.AIA_CB_IDENTIFY_POLICY IP
                ON 1 = 1
                AND IP.BUNAME = :STR_BUNAME_FA
                AND ST.PONUMBER = IP.PONUMBER
                AND ST.GENERICATTRIBUTE29 = IP.LIFE_NUMBER
                AND ST.GENERICATTRIBUTE30 = IP.COVERAGE_NUMBER
                AND ST.GENERICATTRIBUTE31 = IP.RIDER_NUMBER
                AND st.productid = ip.component_CODE
            WHERE
                st.tenantid = 'AIAS'
                AND st.processingUnitseq = :V_PROCESSINGUNITSEQ
                AND st.eventtypeseq <> 16607023625933358;--and st.compensationdate between '1-mar-2017' and '31-may-2017'
        --and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE

        --AND ST.PRODUCTID                  = IP.COMPONENT_CODE

        /* ORIGSQL: commit; */
        COMMIT;

        --Add for AI transaction NL20180308
        /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_Comls_Step0_AMR'; */
        /* ORIGSQL: truncate table EXT.AIA_tmp_Comls_Step0_AMR ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_Comls_Step0_AMR';

        --version 11 define column name

        /* ORIGSQL: insert into EXT.AIA_tmp_Comls_Step0_AMR (RN,QUARTER,COMP_MONTH,COMP_YEAR,COMP_PERIOD,PAYEE_DISTRICT_UNIT,PAYEE_CURRENT_UNIT,PAYEE_CODE,PAYEE_NAME,AGENCY_NAME,PAYEE_CURRENT_CLASS_CODE,PAYEE_RANK,CONTRACT_D(...) */
        INSERT INTO EXT.AIA_tmp_Comls_Step0_AMR
            (
                RN
                , QUARTER
                , COMP_MONTH
                , COMP_YEAR
                , COMP_PERIOD
                , PAYEE_DISTRICT_UNIT
                , PAYEE_CURRENT_UNIT
                , PAYEE_CODE
                , PAYEE_NAME
                , AGENCY_NAME
                , PAYEE_CURRENT_CLASS_CODE
                , PAYEE_RANK
                , CONTRACT_DATE
                , CONTRACT_QUARTER
                , TERMINATIONDATE
                , PONUMBER
                , POLICY_SUBMISSION_DATE
                , RISK_COMMENCEMENT_DATE
                , POLICY_INCEPTION_DATE
                , COMPENSATIONDATE
                , CASE_COUNT
                , FYC
                , TOTAL_CASE_COUNT
                , CURR_QTR_VALIDATION_MET
                , POL_PREV_QTR_VALIDATION
                , AI_PAID
                , AI_CLAWBACK
                , AI_PAYMENT
                , FORTS
                , DEPOSIT_VALUE
                , AI_RATE
                , YTD
                , HELD_DEPOSIT_VALUE
                , COMPONENT_CODE
                , FREELOOK_INCEPTED_CURR_QTR
                , OLD_AGENT_CD
                , FA_ON_BRIDGING_FLG
                , NEW_AGENT_CD
                , OLD_DISTRICT_CODE
                , OLD_UNIT_CODE
                , NEW_HIRE_DATE
                , TRANSFERED_AGENT
            )
            SELECT   /* ORIGSQL: select ROW_NUMBER() OVER (PARTITION BY t1.PONUMBER, t1.AI_PAYMENT, t1.COMPENSATIONDATE, t1.PAYEE_CODE, t1.POLICY_INCEPTION_DATE ORDER BY t1.component_CODE) as rn,t1.QUARTER,t1.COMP_MONTH,t1.COMP_YEAR,(...) */
                ROW_NUMBER() OVER (PARTITION BY t1.PONUMBER, t1.AI_PAYMENT, t1.COMPENSATIONDATE, t1.PAYEE_CODE, t1.POLICY_INCEPTION_DATE ORDER BY t1.component_CODE) AS rn,
                t1.QUARTER,
                t1.COMP_MONTH,
                t1.COMP_YEAR,
                t1.COMP_PERIOD,
                t1.PAYEE_DISTRICT_UNIT,
                t1.PAYEE_CURRENT_UNIT,
                t1.PAYEE_CODE,
                t1.PAYEE_NAME,
                t1.AGENCY_NAME,
                t1.PAYEE_CURRENT_CLASS_CODE,
                t1.PAYEE_RANK,
                t1.CONTRACT_DATE,
                t1.CONTRACT_QUARTER,
                t1.TERMINATIONDATE,
                t1.PONUMBER,
                t1.POLICY_SUBMISSION_DATE,
                t1.RISK_COMMENCEMENT_DATE,
                t1.POLICY_INCEPTION_DATE,
                t1.COMPENSATIONDATE,
                t1.CASE_COUNT,
                t1.FYC,
                t1.TOTAL_CASE_COUNT,
                t1.CURR_QTR_VALIDATION_MET,
                t1.POL_PREV_QTR_VALIDATION,
                t1.AI_PAID,
                t1.AI_CLAWBACK,
                t1.AI_PAYMENT,
                t1.FORTS,
                t1.DEPOSIT_VALUE,
                t1.AI_RATE,
                t1.YTD,
                t1.HELD_DEPOSIT_VALUE,
                t1.COMPONENT_CODE,
                t1.FREELOOK_INCEPTED_CURR_QTR,
                t1.OLD_AGENT_CD,
                t1.FA_ON_BRIDGING_FLG,
                t1.NEW_AGENT_CD,
                t1.OLD_DISTRICT_CODE,
                t1.OLD_UNIT_CODE,
                t1.NEW_HIRE_DATE,
                t1.TRANSFERED_AGENT
            FROM
                EXT.AI_MONTHLY_REPORT t1
            WHERE
                t1.AI_PAYMENT <> 0;

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_Comls_Step0_TXN'; */
        /* ORIGSQL: truncate table EXT.AIA_tmp_Comls_Step0_TXN ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_Comls_Step0_TXN';

        /* ORIGSQL: insert into EXT.AIA_tmp_Comls_Step0_TXN select ROW_NUMBER() OVER (PARTITION BY t2.PONUMBER, t2.VALUE, t2.ACCOUNTINGDATE, t2.GENERICATTRIBUTE11, t2.GENERICDATE2 ORDER BY t2.PRODUCTID) as rn, t2.salestransa(...) */
        INSERT INTO EXT.AIA_tmp_Comls_Step0_TXN
            SELECT   /* ORIGSQL: select ROW_NUMBER() OVER (PARTITION BY t2.PONUMBER, t2.VALUE, t2.ACCOUNTINGDATE, t2.GENERICATTRIBUTE11, t2.GENERICDATE2 ORDER BY t2.PRODUCTID) as rn, t2.salestransactionseq, t2.PONUMBER, t2.VALUE, t2.(...) */
                ROW_NUMBER() OVER (PARTITION BY t2.PONUMBER, t2.VALUE, t2.ACCOUNTINGDATE, t2.GENERICATTRIBUTE11, t2.GENERICDATE2 ORDER BY t2.PRODUCTID) AS rn,
                t2.salestransactionseq,
                t2.PONUMBER,
                t2.VALUE,
                t2.ACCOUNTINGDATE,
                t2.GENERICATTRIBUTE11,
                t2.GENERICDATE2,
                t2.genericnumber2,
                t2.genericdate4
            FROM
                cs_Salestransaction t2
            WHERE
                t2.tenantid = 'AIAS'
                AND t2.processingUnitseq = :V_PROCESSINGUNITSEQ
                AND t2.eventtypeseq = 16607023625933358;

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: insert into EXT.AIA_tmp_Comls_Step0_1 WITH IP AS (SELECT ROW_NUMBER() OVER (PARTITION BY t3.PONUMBER,t3.WRI_AGT_CODE,t3.component_CODE,t3.inception_date,t3.risk_commencement_date ORDER BY t3.coverage_numb(...) */
        INSERT INTO EXT.AIA_tmp_Comls_Step0_1 
       /*with AMR as (select row_number() over(partition by t1.PONUMBER,t1.AI_PAYMENT,t1.COMPENSATIONDATE,t1.PAYEE_CODE ,t1.POLICY_INCEPTION_DATE order by t1.component_CODE) as rn,
                     t1.* from AI_MONTHLY_REPORT t1 where t1.AI_PAYMENT<> 0),
             st as (select row_number() over(partition by t2.PONUMBER,t2.VALUE,t2.ACCOUNTINGDATE,t2.GENERICATTRIBUTE11 ,t2.GENERICDATE2 order by t2.PRODUCTID) as rn,
                        t2.* from cs_Salestransaction t2,cs_businessunit bu  where t2.tenantid='AIAS' and T2.BUSINESSUNITMAP=BU.MASK
            -- and BU.NAME=STR_BUNAME_FA  --Changes done to fix not getting AGY AI records --Gopi-04072019
         and t2.processingUnitseq=V_PROCESSINGUNITSEQ and t2.eventtypeseq = 16607023625933358 ),*/

    WITH 
    IP AS (
        SELECT   /* ORIGSQL: (select ROW_NUMBER() OVER (PARTITION BY t3.PONUMBER,t3.WRI_AGT_CODE,t3.component_CODE,t3.inception_date,t3.risk_commencement_date ORDER BY t3.coverage_number) as rn, t3.* from EXT.AIA_CB_IDENTIFY_POLICY t(...) */
            ROW_NUMBER()OVER (PARTITION BY t3.PONUMBER,t3.WRI_AGT_CODE,t3.component_CODE,t3.inception_date,t3.risk_commencement_date ORDER BY t3.coverage_number) AS rn,
            t3.*
        FROM
            EXT.AIA_CB_IDENTIFY_POLICY t3
        WHERE
            t3.BUNAME = :STR_BUNAME_FA
    
    )
    SELECT   /* ORIGSQL: select st.salestransactionseq, ip.wri_agt_code as wri_agt_code_ORIG, ip.quarter || ' '|| ip.year as CALCULATION_PERIOD, ip.ponumber as POLICY_NUMBER, ip.policyidseq as POLICYIDSEQ, ip.life_number as L(...) */
        /* ORIGSQL: select / *+ PARALLEL * / */
        st.salestransactionseq,
        ip.wri_agt_code AS wri_agt_code_ORIG,
        /* -- (case when AMR.OLD_AGENT_CD IS NULL THEN ip.wri_agt_code ELSE AMR.OLD_AGENT_CD END) as wri_agt_code_ORIG, --Changes done to fix not getting AGY AI records --Gopi-04072019 */
        IFNULL(ip.quarter,'') || ' '|| IFNULL(ip.year,'') AS CALCULATION_PERIOD,
        ip.ponumber AS POLICY_NUMBER,
        ip.policyidseq AS POLICYIDSEQ,
        ip.life_number AS LIFE_NUMBER,
        ip.coverage_number AS COVERAGE_NUMBER,
        ip.rider_number AS RIDER_NUMBER,
        ip.component_code AS COMPONENT_CODE,
        ip.component_name AS COMPONENT_NAME,
        ip.base_rider_ind AS BASE_RIDER_IND,
        st.genericnumber2 AS COMMISSION_RATE,
        st.genericdate4 AS PAID_TO_DATE,
        'SGT'||IFNULL(ip.wri_agt_code,'') AS wri_agt_code
        /* --  ,'SGT'||(case when AMR.OLD_AGENT_CD IS NULL THEN ip.wri_agt_code ELSE AMR.OLD_AGENT_CD END)  wri_agt_code --Changes done to fix not getting AGY AI records --Gopi-04072019 */, IFNULL(ip.quarter,'') || ' '|| IFNULL(ip.year,'') AS qtrYr,
        AMR.OLD_AGENT_CD,
        AMR.NEW_AGENT_CD
    FROM
        EXT.AIA_tmp_Comls_Step0_TXN st
    INNER JOIN
        EXT.AIA_tmp_Comls_Step0_AMR AMR
        ON st.PONUMBER = AMR.PONUMBER
        AND st.VALUE = AMR.AI_PAYMENT
        AND st.ACCOUNTINGDATE = AMR.COMPENSATIONDATE
        AND (st.GENERICATTRIBUTE11 = AMR.NEW_AGENT_CD
        OR st.GENERICATTRIBUTE11 = AMR.OLD_AGENT_CD) ----Changes done to fix not getting AGY AI records --Gopi-04072019
        AND st.GENERICDATE2 = AMR.POLICY_INCEPTION_DATE
        --AND st.rn = AMR.rn
    INNER JOIN
        IP
        ON 1 = 1
        AND IP.BUNAME = :STR_BUNAME_FA
        AND AMR.PONUMBER = IP.PONUMBER
        AND (AMR.NEW_AGENT_CD = IP.WRI_AGT_CODE
        OR AMR.OLD_AGENT_CD = IP.WRI_AGT_CODE) --Changes done to fix not getting AGY AI records --Gopi-04072019
        AND AMR.component_CODE = ip.component_CODE
        AND AMR.policy_inception_date = ip.inception_date
        AND AMR.risk_commencement_date = ip.risk_commencement_date
        AND AMR.rn = IP.rn;

    /* ORIGSQL: EXT.Log('insert 0_1 done for FA '||SQL%ROWCOUNT) */
    CALL EXT.Log('insert 0_1 done for FA '||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: execute immediate 'TRUNCATE table EXT.AIA_tmp_comls_step1_1'; */
    /* ORIGSQL: TRUNCATE table EXT.AIA_tmp_comls_step1_1 ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_step1_1';

    /* ORIGSQL: insert into EXT.AIA_tmp_comls_step1_1 select crd.creditseq, crd.salestransactionseq, ip.CALCULATION_PERIOD, ip.POLICY_NUMBER, ip.policyidseq as POLICYIDSEQ, ip.life_number as LIFE_NUMBER, ip.coverage_numb(...) */
    INSERT INTO EXT.AIA_tmp_comls_step1_1
        /* ORIGSQL: select / *+ leading(ip,crd) index(crd CS_CREDIT_TRANSACTIONSEQ) * / */
        SELECT   /* ORIGSQL: select crd.creditseq, crd.salestransactionseq, ip.CALCULATION_PERIOD, ip.POLICY_NUMBER, ip.policyidseq as POLICYIDSEQ, ip.life_number as LIFE_NUMBER, ip.coverage_number as COVERAGE_NUMBER, ip.rider_nu(...) */
            crd.creditseq,
            crd.salestransactionseq,
            ip.CALCULATION_PERIOD,
            ip.POLICY_NUMBER,
            ip.policyidseq AS POLICYIDSEQ,
            ip.life_number AS LIFE_NUMBER,
            ip.coverage_number AS COVERAGE_NUMBER,
            ip.rider_number AS RIDER_NUMBER,
            ip.component_code AS COMPONENT_CODE,
            ip.component_name AS COMPONENT_NAME,
            ip.base_rider_ind AS BASE_RIDER_IND,
            crd.compensationdate AS TRANSACTION_DATE,
            crd.genericattribute12 AS PAYOR_CODE,
            ct.credittypeid AS CREDITTYPE,
            crd.name AS CREDIT_NAME,
            crd.value AS CREDIT_VALUE,
            crd.periodseq AS PERIODSEQ,
            crd.genericattribute2 AS PRODUCT_NAME,
            crd.genericnumber1 AS POLICY_YEAR,
            ip.COMMISSION_RATE,
            ip.PAID_TO_DATE,
            ip.wri_agt_code,
            ip.qtrYr,
            crd.genericdate2,
            crd.genericattribute13,
            crd.genericattribute14,
            crd.positionseq,
            crd.ruleseq,
            ip.old_agent_cd,
            ip.new_agent_cd
        FROM
            cs_Credit crd
        INNER JOIN
            EXT.AIA_tmp_comls_period p
            ON crd.periodseq = p.periodseq
        INNER JOIN
            CS_CREDITTYPE ct
            ON crd.CREDITTYPESEQ = ct.DATATYPESEQ
            AND ct.Removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
        INNER JOIN
            EXT.AIA_tmp_comls_step0_1 ip
            ON 1 = 1
            AND ip.salestransactionseq = crd.salestransactionseq
            AND (crd.genericattribute12 = ip.wri_agt_code_orig
                OR crd.genericattribute12 = ip.old_agent_cd
            OR crd.genericattribute12 = ip.new_agent_cd)
            AND ip.CALCULATION_PERIOD = :V_CAL_PERIOD
            --where crd.genericattribute16 not in ('RO', 'RNO')
        WHERE
            crd.tenantid = 'AIAS'
            AND crd.processingunitseq = :V_PROCESSINGUNITSEQ;

    /* ORIGSQL: EXT.Log('insert 1_1 done for FA'||SQL%ROWCOUNT) */
    CALL EXT.Log('insert 1_1 done for FA'||::ROWCOUNT);  

    --delete from EXT.AIA_TMP_COMLS_STEP1_1 where transaction_Date <DT_CB_START_DATE or transaction_Date>DT_CB_END_DATE;

    --Log('delete 1_1 done for FA'||SQL%ROWCOUNT);

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: DBMS_STATS.GATHER_TABLE_STATS(ownname => '"AIASEXT"', tabname => '"EXT.AIA_TMP_COMLS_STEP1_1"', estimate_percent => 1) */
    EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| '"AIASEXT"'|| '.'|| '"EXT.AIA_TMP_COMLS_STEP1_1"';

    /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_comls_step2_1'; */
    /* ORIGSQL: truncate table EXT.AIA_tmp_comls_step2_1 ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_step2_1';

    /* ORIGSQL: insert into EXT.AIA_tmp_comls_step2_1 select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq, null as clawback_name from cs_measurement m join EXT.AIA_tmp_comls_period p on m.periodseq=p.p(...) */
    INSERT INTO EXT.AIA_tmp_comls_step2_1
        SELECT   /* ORIGSQL: select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq, null as clawback_name from cs_measurement m join EXT.AIA_tmp_comls_period p on m.periodseq=p.periodseq inner join (SELECT distin(...) */
            measurementseq,
            m.name,
            m.periodseq,
            payeeseq,
            ruleseq,
            positionseq,
            NULL AS clawback_name
        FROM
            cs_measurement m
        INNER JOIN
            EXT.AIA_tmp_comls_period p
            ON m.periodseq = p.periodseq
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select distinct SOURCE_RULE_OUTPUT from EXT.AIA_CB_RULES_LOOKUP where RULE_TYPE = 'PM' AND CLAWBACK_NAME IN ('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION','FA_FYO_2.1','FA_RYO_2.1','FA_FYO_ONG(...) */
                    DISTINCT
                    SOURCE_RULE_OUTPUT
                FROM
                    EXT.AIA_CB_RULES_LOOKUP
                WHERE
                    RULE_TYPE = 'PM'
                    --added by suresh
                    --add AI NL20180308
                    AND CLAWBACK_NAME IN ('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION'
                        --verstion 13 Harm_Phase4 Start
                        ,'FA_FYO_2.1','FA_RYO_2.1','FA_FYO_ONG_2.1','FA_RYO_ONG_2.1','FA_AI_2.1'
                        --verstion 13 Harm_Phase4 End
                    )
            ) AS pmr
            --Added as part of UnitTesting-Gopi
            --end by Suresh
            ON pmr.SOURCE_RULE_OUTPUT = m.name
        WHERE
            m.processingunitseq = :V_PROCESSINGUNITSEQ
            AND m.tenantid = 'AIAS';

    /* ORIGSQL: DBMS_STATS.GATHER_TABLE_STATS(ownname => '"AIASEXT"', tabname => '"EXT.AIA_TMP_COMLS_STEP2_1"', estimate_percent => 1) */
    EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| '"AIASEXT"'|| '.'|| '"EXT.AIA_TMP_COMLS_STEP2_1"';

    /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

    /* ORIGSQL: EXT.Log('insert 2_1 done for FA'||SQL%ROWCOUNT) */
    CALL EXT.Log('insert 2_1 done for FA'||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    --  execute immediate 'truncate table EXT.AIA_tmp_comls_step3_1';

    /* ORIGSQL: delete from EXT.AIA_tmp_comls_step3_1; */
    DELETE
    FROM
        EXT.AIA_tmp_comls_step3_1;
    --Update by Amanda here for the issue object on longer exists
    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: insert into EXT.AIA_tmp_comls_step3_1 select pct.creditseq pctCreditSeq, pct.measurementseq, pct.contributionvalue PctContribValue, dct.depositseq, s1.CREDITSEQ,SALESTRANSACTIONSEQ,CALCULATION_PERIOD,POLI(...) */
    INSERT INTO EXT.AIA_tmp_comls_step3_1
        SELECT   /* ORIGSQL: select pct.creditseq pctCreditSeq, pct.measurementseq, pct.contributionvalue PctContribValue, dct.depositseq, s1.CREDITSEQ,SALESTRANSACTIONSEQ,CALCULATION_PERIOD,POLICY_NUMBER,POLICYIDSEQ,LIFE_NUMBER,(...) */
            pct.creditseq AS pctCreditSeq,
            pct.measurementseq,
            pct.contributionvalue AS PctContribValue,
            dct.depositseq,
            s1.CREDITSEQ,
            SALESTRANSACTIONSEQ,
            CALCULATION_PERIOD,
            POLICY_NUMBER,
            POLICYIDSEQ,
            LIFE_NUMBER,
            COVERAGE_NUMBER,
            RIDER_NUMBER,
            COMPONENT_CODE,
            COMPONENT_NAME,
            BASE_RIDER_IND,
            TRANSACTION_DATE,
            PAYOR_CODE,
            CREDITTYPE,
            CREDIT_NAME,
            CREDIT_VALUE,
            s1.PERIODSEQ,
            PRODUCT_NAME,
            POLICY_YEAR,
            COMMISSION_RATE,
            PAID_TO_DATE,
            s2.name AS mname,
            s2.periodseq AS mPeriodSeq,
            s2.payeeseq AS mPayeeSeq,
            s2.ruleseq AS mruleSeq,
            s2.positionseq AS mPositionSeq,
            s2.clawback_name,
            WRI_AGT_CODE,
            QTRYR,
            GD2,
            pct.contributionvalue AS CONTRIBUTIONVALUE,
            s1.genericattribute13,
            s1.genericattribute14,
            s1.crd_positionseq,
            s1.crd_ruleseq,
            s1.old_agent_cd,
            s1.new_agent_cd
        FROM
            cs_pmcredittrace pct
        INNER JOIN
            EXT.AIA_tmp_comls_step1_1 s1
            ON pct.creditseq = s1.creditseq
        INNER JOIN
            EXT.AIA_tmp_comls_step2_1 s2
            ON s2.measurementseq = pct.measurementseq
            AND s2.ruleseq = pct.ruleseq
            --and pct.targetperiodseq=s2.periodseq
        INNER JOIN
            CS_PMSELFTRACE pmslf
            ON s2.measurementseq = pmslf.sourcemeasurementseq
            -- and pmslf.targetperiodseq=s2.periodseq
        INNER JOIN
            CS_INCENTIVEPMTRACE inpm
            ON pmslf.TARGETMEASUREMENTSEQ = inpm.MEASUREMENTSEQ
            --and pmslf.targetperiodseq=s2.periodseq

            /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_DEPOSITINCENTIVETRACE' not found */
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select * from cs_depositincentivetrace) */
                    *
                FROM
                    cs_depositincentivetrace
        UNION
            SELECT   /* ORIGSQL: select * from ext.aias_depositincentivetrace) AS dct on inpm.incentiveseq = dct.incentiveseq and dct.targetperiodseq=s2.periodseq and dct.tenantid='AIAS' and pct.tenantid='AIAS' and pmslf.tenantid='AIAS' (...) */
                *
            FROM
                EXT.aias_depositincentivetrace
        ) AS dct
        ON inpm.incentiveseq = dct.incentiveseq
        --and dct.targetperiodseq=s2.periodseq
        AND dct.targetperiodseq = s2.periodseq
        AND dct.tenantid = 'AIAS'
        AND pct.tenantid = 'AIAS'
        AND pmslf.tenantid = 'AIAS'
        AND inpm.tenantid = 'AIAS'
        AND dct.processingUnitseq = :V_PROCESSINGUNITSEQ
        AND pct.processingUnitseq = :V_PROCESSINGUNITSEQ
        AND pmslf.processingUnitseq = :V_PROCESSINGUNITSEQ
        AND inpm.processingUnitseq = :V_PROCESSINGUNITSEQ;

    --add AI NL20180308

    /* ORIGSQL: insert into EXT.AIA_tmp_comls_step3_1 select pct.creditseq pctCreditSeq, pct.measurementseq, pct.contributionvalue PctContribValue, dct.depositseq, s1.CREDITSEQ,SALESTRANSACTIONSEQ,CALCULATION_PERIOD,POLI(...) */
    INSERT INTO EXT.AIA_tmp_comls_step3_1
        SELECT   /* ORIGSQL: select pct.creditseq pctCreditSeq, pct.measurementseq, pct.contributionvalue PctContribValue, dct.depositseq, s1.CREDITSEQ,SALESTRANSACTIONSEQ,CALCULATION_PERIOD,POLICY_NUMBER,POLICYIDSEQ,LIFE_NUMBER,(...) */
            pct.creditseq AS pctCreditSeq,
            pct.measurementseq,
            pct.contributionvalue AS PctContribValue,
            dct.depositseq,
            s1.CREDITSEQ,
            SALESTRANSACTIONSEQ,
            CALCULATION_PERIOD,
            POLICY_NUMBER,
            POLICYIDSEQ,
            LIFE_NUMBER,
            COVERAGE_NUMBER,
            RIDER_NUMBER,
            COMPONENT_CODE,
            COMPONENT_NAME,
            BASE_RIDER_IND,
            TRANSACTION_DATE,
            PAYOR_CODE,
            CREDITTYPE,
            CREDIT_NAME,
            CREDIT_VALUE,
            s1.PERIODSEQ,
            PRODUCT_NAME,
            POLICY_YEAR,
            COMMISSION_RATE,
            PAID_TO_DATE,
            s2.name AS mname,
            s2.periodseq AS mPeriodSeq,
            s2.payeeseq AS mPayeeSeq,
            s2.ruleseq AS mruleSeq,
            s2.positionseq AS mPositionSeq,
            s2.clawback_name,
            WRI_AGT_CODE,
            QTRYR,
            GD2,
            pct.contributionvalue AS CONTRIBUTIONVALUE,
            s1.genericattribute13,
            s1.genericattribute14,
            s1.crd_positionseq,
            s1.crd_ruleseq,
            /* --verstion 13 start comment the old_agent_cd, otherwise would produce duplicated data for AI */
            /* --,s1.old_agent_cd */
            /* --verstion 13 end */ '',
            s1.new_agent_cd
        FROM
            cs_pmcredittrace pct
        INNER JOIN
            EXT.AIA_tmp_comls_step1_1 s1
            ON pct.creditseq = s1.creditseq
        INNER JOIN
            EXT.AIA_tmp_comls_step2_1 s2
            ON s2.measurementseq = pct.measurementseq
            AND s2.ruleseq = pct.ruleseq
            --and pct.targetperiodseq=s2.periodseq
            -- and pmslf.targetperiodseq=s2.periodseq
        INNER JOIN
            CS_INCENTIVEPMTRACE inpm
            ON pct.measurementseq = inpm.MEASUREMENTSEQ
            --and pmslf.targetperiodseq=s2.periodseq 
        INNER JOIN
            /*CS_DEPOSITINCENTIVETRACE*/
            (
                SELECT   /* ORIGSQL: (select * from cs_depositincentivetrace) */
                    *
                FROM
                    cs_depositincentivetrace
        UNION
            SELECT   /* ORIGSQL: select * from ext.aias_depositincentivetrace) AS dct on inpm.incentiveseq = dct.incentiveseq and dct.targetperiodseq=s2.periodseq and dct.tenantid='AIAS' and pct.tenantid='AIAS' and inpm.tenantid='AIAS' a(...) */
                *
            FROM
                EXT.aias_depositincentivetrace
        ) AS dct
        ON inpm.incentiveseq = dct.incentiveseq
        --and dct.targetperiodseq=s2.periodseq
        AND dct.targetperiodseq = s2.periodseq
        AND dct.tenantid = 'AIAS'
        AND pct.tenantid = 'AIAS'
        AND inpm.tenantid = 'AIAS'
        AND dct.processingUnitseq = :V_PROCESSINGUNITSEQ
        AND pct.processingUnitseq = :V_PROCESSINGUNITSEQ
        AND inpm.processingUnitseq = :V_PROCESSINGUNITSEQ;

    /* ORIGSQL: EXT.Log('insert 3 done for FA'||SQL%ROWCOUNT) */
    CALL EXT.Log('insert 3 done for FA'||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: DBMS_STATS.GATHER_TABLE_STATS(ownname => '"AIASEXT"', tabname => '"EXT.AIA_TMP_COMLS_STEP3_1"', estimate_percent => 1) */
    EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| '"AIASEXT"'|| '.'|| '"EXT.AIA_TMP_COMLS_STEP3_1"';

    /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

    /* ORIGSQL: insert into EXT.AIA_CB_TRACE_FORWARD_COMP select STR_BUNAME_FA as BUNAME, QtrYr as CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, mPositionSeq PAYEE_SEQ, substr(dep_pos.name, 4) as PAYEE_CODE, PAYOR_CODE(...) */
    INSERT INTO EXT.AIA_CB_TRACE_FORWARD_COMP
        SELECT   /* ORIGSQL: select STR_BUNAME_FA as BUNAME, QtrYr as CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, mPositionSeq PAYEE_SEQ, substr(dep_pos.name, 4) as PAYEE_CODE, PAYOR_CODE, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_(...) */
            :STR_BUNAME_FA AS BUNAME,
            QtrYr AS CALCULATION_PERIOD,
            POLICY_NUMBER,
            POLICYIDSEQ,
            mPositionSeq AS PAYEE_SEQ,
            SUBSTRING(dep_pos.name,4) AS PAYEE_CODE,  /* ORIGSQL: substr(dep_pos.name, 4) */
            PAYOR_CODE,
            LIFE_NUMBER,
            COVERAGE_NUMBER,
            RIDER_NUMBER,
            COMPONENT_CODE,
            COMPONENT_NAME,
            BASE_RIDER_IND,
            TRANSACTION_DATE,
            TO_VARCHAR(:DT_CB_START_DATE,'MON-YYYY') AS PROCESSING_PERIOD,  /* ORIGSQL: TO_CHAR(DT_CB_START_DATE,'MON-YYYY') */
            :STR_LUMPSUM AS CLAWBACK_TYPE,
            rl.CLAWBACK_NAME,
            :STR_CB_NAME AS CLAWBACK_METHOD,
            CREDITTYPE,
            CREDITSEQ,
            CREDIT_NAME,
            CREDIT_VALUE,
            crd_positionseq AS crd_positionseq,
            GD2 AS crd_genericdate2,
            crd_ruleseq AS crd_ruleseq,
            measurementseq AS PM_SEQ,
            mname AS PM_NAME,
            CASE rl.CLAWBACK_NAME
                WHEN 'NLPI'
                THEN x.contributionvalue*:V_NLPI_RATE
                ELSE x.contributionvalue
            END
            AS PM_CONTRIBUTION_VALUE,
            CASE rl.CLAWBACK_NAME
                WHEN 'FYO'
                THEN fyo_rate.value
                WHEN 'NEW_FYO'
                THEN new_fyo_rate.value
                WHEN 'RYO'
                THEN ryo_rate.value
                WHEN 'NEW_RYO'
                THEN new_ryo_rate.value
                WHEN 'FSM_RYO'
                THEN ryo_rate.value
                WHEN 'NLPI'
                THEN :V_NLPI_RATE
                ELSE 1
            END
            AS PM_RATE,
            dep.depositseq AS DEPOSITSEQ,
            /*dep.name*/ REPLACE(dep.name,'_MANUAL','') AS DEPOSIT_NAME,
            dep.value AS DEPOSIT_VALUE,
            x.periodseq AS PERIODSEQ,
            x.salestransactionseq AS SALESTRANSACTIONSEQ,
            PRODUCT_NAME,
            POLICY_YEAR,
            COMMISSION_RATE,
            PAID_TO_DATE,
            :P_BATCH_NO AS BATCH_NUMBER,
            CURRENT_TIMESTAMP AS CREATED_DATE,  /* ORIGSQL: sysdate */
            x.old_agent_cd,
            x.new_agent_cd,
            NULL AS deposit_period /* --Version 13 add by Amanda */
        FROM
            EXT.AIA_tmp_comls_step3_1 x
        INNER JOIN
            cs_deposit dep
            ON dep.depositseq = x.depositseq
        INNER JOIN
            cs_position dep_pos
            ON dep.positionseq = dep_pos.ruleelementownerseq
            AND dep_pos.removedate = :DT_REMOVEDATE
            AND dep_pos.effectivestartdate <= x.GD2
            AND dep_pos.effectiveenddate > x.GD2
            --and dep_pos.name = x.wri_agt_code
        INNER JOIN
            cs_title dep_title
            ON dep_pos.titleseq = dep_title.ruleelementownerseq
            AND dep_title.removedate = :DT_REMOVEDATE
            AND dep_title.effectivestartdate <= GD2
            AND dep_title.effectiveenddate > GD2
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME from EXT.AIA_CB_RULES_LOOKUP where RULE_TYPE = 'DR' AND CLAWBACK_NAME IN ('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION','FA_FYO_2.1','FA_FYO_O(...) */
                    DISTINCT
                    SOURCE_RULE_OUTPUT,
                    CLAWBACK_NAME
                FROM
                    EXT.AIA_CB_RULES_LOOKUP
                WHERE
                    RULE_TYPE = 'DR'
                    --Changed by suresh
                    --Add for AI NL20180308
                    AND CLAWBACK_NAME IN ('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION'
                        --verstion 13 start
                    ,'FA_FYO_2.1','FA_FYO_ONG_2.1','FA_RYO_2.1','FA_RYO_ONG_2.1','FA_AI_2.1')
            ) AS rl
            --verstion 13 end
            --end by suresh
            ON /*dep.NAME*/ REPLACE(dep.name,'_MANUAL','') = rl.SOURCE_RULE_OUTPUT
        LEFT OUTER JOIN
            ext.vw_lt_fyo_rate fyo_rate
            ON fyo_rate.Contributor_Leader_title = x.genericattribute13 --payor agency leader title
            AND fyo_rate.PIB_TYPE = ext.fn_fyo_pib_type(x.genericattribute13, x.genericattribute14, x.credit_name)
            AND fyo_rate.Receiver_title = dep_title.name
            AND rl.CLAWBACK_NAME = 'FYO_FA'
            --for lookup PM rate for RYO
        LEFT OUTER JOIN
            ext.vw_lt_ryo_life_rate ryo_rate
            ON ryo_rate.Contributor_Leader_title = x.genericattribute13 --payor agency leader title
            AND ryo_rate.PIB_TYPE = ext.fn_fyo_pib_type(x.genericattribute13, x.genericattribute14, x.credit_name)
            AND ryo_rate.Receiver_title = dep_title.name
            AND rl.CLAWBACK_NAME IN ('RYO_FA','FSM_RYO_FA')
            --Added by Suresh
            --for lookup PM rate for New FYO
        LEFT OUTER JOIN
            ext.vw_lt_new_fyo_rate new_fyo_rate
            ON new_fyo_rate.Contributor_Leader_title = x.genericattribute13 --payor agency leader title
            AND new_fyo_rate.PIB_TYPE = ext.fn_fyo_pib_type(x.genericattribute13, x.genericattribute14, x.credit_name)
            AND new_fyo_rate.Receiver_title = dep_title.name
            AND rl.CLAWBACK_NAME = 'NEW_FYO_FA'
            --for lookup PM rate for New RYO
        LEFT OUTER JOIN
            ext.vw_LT_NEW_RYO_LIFE_RATE new_ryo_rate
            ON new_ryo_rate.Contributor_Leader_title = x.genericattribute13 --payor agency leader title
            AND new_ryo_rate.PIB_TYPE = ext.fn_fyo_pib_type(x.genericattribute13, x.genericattribute14, x.credit_name)
            AND new_ryo_rate.Receiver_title = dep_title.name
            AND rl.CLAWBACK_NAME = 'NEW_RYO_FA';--end by Suresh
    --  where x.qtrYr = V_CAL_PERIOD
    /*
    
    
    
    insert into EXT.AIA_CB_TRACE_FORWARD_COMP
    select STR_BUNAME as BUNAME,
           ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
           ip.ponumber as POLICY_NUMBER,
           ip.policyidseq as POLICYIDSEQ,
           pm.positionseq as PAYEE_SEQ,
           substr(dep_pos.name, 4) as PAYEE_CODE,
           crd.genericattribute12 as PAYOR_CODE,
           ip.life_number as LIFE_NUMBER,
           ip.coverage_number as COVERAGE_NUMBER,
           ip.rider_number as RIDER_NUMBER,
           ip.component_code as COMPONENT_CODE,
           ip.component_name as COMPONENT_NAME,
           ip.base_rider_ind as BASE_RIDER_IND,
           crd.compensationdate as TRANSACTION_DATE,
           TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
           STR_LUMPSUM as CLAWBACK_TYPE,
            rl.CLAWBACK_NAME       as CLAWBACK_NAME,
            STR_CB_NAME as CLAWBACK_METHOD,
           ct.credittypeid        as CREDIT_TYPE,
           crd.creditseq          as CREDITSEQ,
           crd.name               as CREDIT_NAME,
           crd.value              as CREDIT_VALUE,
           crd.positionseq as crd_positionseq,
           st.genericdate2 as crd_genericdate2,
           crd.ruleseq as crd_ruleseq,
           pm.measurementseq      as PM_SEQ,
           pm.name                as PM_NAME,
           case rl.CLAWBACK_NAME
           when 'NLPI' then pct.contributionvalue*V_NLPI_RATE
           else
           pct.contributionvalue
           end as PM_CONTRIBUTION_VALUE,
           case rl.CLAWBACK_NAME
             when 'FYO' then fyo_rate.value
             when 'RYO' then ryo_rate.value
             when 'FSM_RYO' then ryo_rate.value
             when 'NLPI' then V_NLPI_RATE
           else 1
             end as PM_RATE,
           dep.depositseq         as DEPOSITSEQ,
           dep.name               as DEPOSIT_NAME,
           dep.value              as DEPOSIT_VALUE,
           crd.periodseq          as PERIODSEQ,
           st.salestransactionseq as SALESTRANSACTIONSEQ,
           crd.genericattribute2  as PRODUCT_NAME,
           crd.genericnumber1     as POLICY_YEAR,
           st.genericnumber2      as COMMISSION_RATE,
           st.genericdate4        as PAID_TO_DATE,
           P_BATCH_NO             as BATCH_NUMBER,
           sysdate                as CREATED_DATE
      FROM CS_SALESTRANSACTION st
     inner join CS_CREDIT crd
        on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
     and crd.tenantid=st.tenantid and crd.processingunitseq=st.processingunitseq
    --and crd.genericdate2
     inner join CS_PMCREDITTRACE pct
        on crd.CREDITSEQ = pct.CREDITSEQ
     and pct.tenantid=crd.tenantid and pct.processingunitseq=crd.processingunitseq
     inner join CS_MEASUREMENT pm
        on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
     and pct.tenantid=pm.tenantid and pct.processingunitseq=pm.processingunitseq
     inner join (select distinct SOURCE_RULE_OUTPUT
                       from EXT.AIA_CB_RULES_LOOKUP
                      where RULE_TYPE = 'PM'
         AND CLAWBACK_NAME IN ('FYO','RYO','FSM_RYO','NLPI'))pmr
        on pmr.SOURCE_RULE_OUTPUT = pm.name
     inner join  CS_PMSELFTRACE pmslf
       on  pm.measurementseq = pmslf.sourcemeasurementseq
     and pm.tenantid=pmslf.tenantid and pm.processingunitseq=pmslf.processingunitseq
    inner join CS_INCENTIVEPMTRACE inpm
       on pmslf.TARGETMEASUREMENTSEQ = inpm.MEASUREMENTSEQ
     and inpm.tenantid=pmslf.tenantid and inpm.processingunitseq=pmslf.processingunitseq
    inner join CS_DEPOSITINCENTIVETRACE depin
       on inpm.incentiveseq = depin.incentiveseq
     and inpm.tenantid=depin.tenantid and inpm.processingunitseq=depin.processingunitseq
     inner join cs_deposit dep
        on depin.depositseq = dep.depositseq
     and dep.tenantid=depin.tenantid and dep.processingunitseq=depin.processingunitseq
     inner join cs_position dep_pos
        on dep.positionseq = dep_pos.ruleelementownerseq
     and dep_pos.tenantid='AIAS'
     and dep_pos.removedate = DT_REMOVEDATE
     and dep_pos.effectivestartdate <= crd.genericdate2
     and dep_pos.effectiveenddate > crd.genericdate2
     inner join CS_CREDITTYPE ct
        on crd.CREDITTYPESEQ = ct.DATATYPESEQ
     and ct.Removedate = DT_REMOVEDATE
     inner join EXT.AIA_CB_IDENTIFY_POLICY ip
        on st.PONUMBER = ip.PONUMBER
     AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
     AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
     AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
     AND st.PRODUCTID = ip.COMPONENT_CODE
     and crd.genericattribute12 = ip.wri_agt_code
     and ip.quarter || ' ' || ip.year = V_CAL_PERIOD
     --for lookup the compensation output name
     inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
                       from EXT.AIA_CB_RULES_LOOKUP
                      where RULE_TYPE = 'DR'
         AND CLAWBACK_NAME IN ('FYO','RYO','FSM_RYO','NLPI')) rl
        on dep.NAME = rl.SOURCE_RULE_OUTPUT
     --for lookup the receiver info.
     inner join cs_title dep_title
     on dep_pos.titleseq = dep_title.ruleelementownerseq
     and dep_title.removedate = DT_REMOVEDATE
     and dep_title.effectivestartdate <= crd.genericdate2
     and dep_title.effectiveenddate > crd.genericdate2
     --for lookup PM rate for FYO
     left join ext.vw_lt_fyo_rate fyo_rate
     on fyo_rate.Contributor_Leader_title = crd.genericattribute13 --payor agency leader title
     and fyo_rate.PIB_TYPE = ext.fn_fyo_pib_type(crd.genericattribute13, crd.genericattribute14, crd.name)
     and fyo_rate.Receiver_title = dep_title.name
     and rl.CLAWBACK_NAME = 'FYO'
     --for lookup PM rate for RYO
     left join ext.vw_lt_ryo_life_rate ryo_rate
     on ryo_rate.Contributor_Leader_title = crd.genericattribute13 --payor agency leader title
     and ryo_rate.PIB_TYPE = ext.fn_fyo_pib_type(crd.genericattribute13, crd.genericattribute14, crd.name)
     and ryo_rate.Receiver_title = dep_title.name
     and rl.CLAWBACK_NAME in ( 'RYO','FSM_RYO')
     WHERE st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ and st.tenantid='AIAS'
     AND st.BUSINESSUNITMAP = 1;
       --and dep.periodseq =  t_periodseq(i);
       --and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
     /# and greatest(nvl(st.GENERICDATE3, to_date('19000101', 'yyyymmdd')),
                        nvl(st.GENERICDATE2, to_date('19000101', 'yyyymmdd')),
                        nvl(st.GENERICDATE5, to_date('19000101', 'yyyymmdd'))) between
                    --to_date('20150801','yyyymmdd') and to_date('20160531','yyyymmdd');
           DT_INCEPTION_START_DATE and DT_INCEPTION_END_DATE; */
    -- and crd.genericattribute16 not in ('RO', 'RNO')

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for FYO,RYO,FSM_RYO,NLPI' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for FYO,RYO,FSM_RYO,NLPI'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    --end loop;

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for NADOR FA' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO) */
    CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for NADOR FA'|| 'clawback type = '|| IFNULL(:P_STR_TYPE,'') ||', batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO),''));
    --for lumpsum compensation trace forward for NADOR
    --for i in 1..t_periodseq.count
    --loop
    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for NADOR FA using periodseq V_CAL_PERIOD of ' || V_CAL_PERIOD) */
    CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for NADOR FA using periodseq V_CAL_PERIOD of '|| IFNULL(:V_CAL_PERIOD,''));

    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_Comls_Step0_2'; */
    /* ORIGSQL: truncate table EXT.AIA_tmp_Comls_Step0_2 ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_Comls_Step0_2';

    /* ORIGSQL: insert into EXT.AIA_tmp_Comls_Step0_2 select st.salestransactionseq, nvl(st.genericattribute12,ip.wri_agt_code) as wri_agt_code_ORIG, ip.quarter || ' '|| ip.year as CALCULATION_PERIOD, ip.ponumber as POLI(...) */
    INSERT INTO EXT.AIA_tmp_Comls_Step0_2
        /* ORIGSQL: select / *+ leading(ip,st) * / */
        SELECT   /* ORIGSQL: select st.salestransactionseq, nvl(st.genericattribute12,ip.wri_agt_code) as wri_agt_code_ORIG, ip.quarter || ' '|| ip.year as CALCULATION_PERIOD, ip.ponumber as POLICY_NUMBER, ip.policyidseq as POLIC(...) */
            st.salestransactionseq,
            /* --ip.wri_agt_code as wri_agt_code_ORIG, */
            /* --version 13 start */
            IFNULL(st.genericattribute12,ip.wri_agt_code) AS wri_agt_code_ORIG /* --commission agent code */, /* --version 13 end */  /* ORIGSQL: nvl(st.genericattribute12,ip.wri_agt_code) */
            IFNULL(ip.quarter,'') || ' '|| IFNULL(ip.year,'') AS CALCULATION_PERIOD,
            ip.ponumber AS POLICY_NUMBER,
            ip.policyidseq AS POLICYIDSEQ,
            ip.life_number AS LIFE_NUMBER,
            ip.coverage_number AS COVERAGE_NUMBER,
            ip.rider_number AS RIDER_NUMBER,
            ip.component_code AS COMPONENT_CODE,
            ip.component_name AS COMPONENT_NAME,
            ip.base_rider_ind AS BASE_RIDER_IND,
            st.genericnumber2 AS COMMISSION_RATE,
            st.genericdate4 AS PAID_TO_DATE,
            'SGT'||IFNULL(ip.wri_agt_code,'') AS wri_agt_code,
            IFNULL(ip.quarter,'') || ' '|| IFNULL(ip.year,'') AS qtrYr
        FROM
            cs_Salestransaction st
        INNER JOIN
            EXT.AIA_CB_IDENTIFY_POLICY IP
            ON 1 = 1
            AND IP.BUNAME = :STR_BUNAME_FA
            AND ST.PONUMBER = IP.PONUMBER
            AND ST.GENERICATTRIBUTE29 = IP.LIFE_NUMBER
            AND ST.GENERICATTRIBUTE30 = IP.COVERAGE_NUMBER
            AND ST.GENERICATTRIBUTE31 = IP.RIDER_NUMBER
            AND st.productid = ip.component_CODE
        WHERE
            st.tenantid = 'AIAS'
            AND st.processingUnitseq = :V_PROCESSINGUNITSEQ;--and st.compensationdate between '1-mar-2017' and '31-may-2017'
    --and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE

    --AND ST.PRODUCTID                  = IP.COMPONENT_CODE

    /* ORIGSQL: EXT.Log('insert 0_2 done for FA'||SQL%ROWCOUNT) */
    CALL EXT.Log('insert 0_2 done for FA'||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_comls_step1_2'; */
    /* ORIGSQL: truncate table EXT.AIA_tmp_comls_step1_2 ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_step1_2';

    /* ORIGSQL: insert into EXT.AIA_tmp_comls_step1_2 select crd.creditseq, crd.salestransactionseq, ip.CALCULATION_PERIOD, ip.POLICY_NUMBER as POLICY_NUMBER, ip.policyidseq as POLICYIDSEQ, ip.life_number as LIFE_NUMBER,(...) */
    INSERT INTO EXT.AIA_tmp_comls_step1_2
        /* ORIGSQL: select / *+ leading(ip,crd) index(crd CS_CREDIT_TRANSACTIONSEQ) * / */
        SELECT   /* ORIGSQL: select crd.creditseq, crd.salestransactionseq, ip.CALCULATION_PERIOD, ip.POLICY_NUMBER as POLICY_NUMBER, ip.policyidseq as POLICYIDSEQ, ip.life_number as LIFE_NUMBER, ip.coverage_number as COVERAGE_NU(...) */
            crd.creditseq,
            crd.salestransactionseq,
            ip.CALCULATION_PERIOD,
            ip.POLICY_NUMBER AS POLICY_NUMBER,
            ip.policyidseq AS POLICYIDSEQ,
            ip.life_number AS LIFE_NUMBER,
            ip.coverage_number AS COVERAGE_NUMBER,
            ip.rider_number AS RIDER_NUMBER,
            ip.component_code AS COMPONENT_CODE,
            ip.component_name AS COMPONENT_NAME,
            ip.base_rider_ind AS BASE_RIDER_IND,
            crd.compensationdate AS TRANSACTION_DATE,
            crd.genericattribute12 AS PAYOR_CODE,
            ct.credittypeid AS CREDITTYPE,
            crd.name AS CREDIT_NAME,
            crd.value AS CREDIT_VALUE,
            crd.periodseq AS PERIODSEQ,
            crd.genericattribute2 AS PRODUCT_NAME,
            crd.genericnumber1 AS POLICY_YEAR,
            ip.COMMISSION_RATE,
            ip.PAID_TO_DATE,
            ip.wri_agt_code AS wri_agt_code,
            ip.qtrYr,
            crd.genericdate2,
            crd.genericattribute13,
            crd.genericattribute14,
            crd.positionseq,
            crd.ruleseq
        FROM
            cs_Credit crd
        INNER JOIN
            EXT.AIA_tmp_comls_period p
            ON crd.periodseq = p.periodseq
        INNER JOIN
            cs_Salestransaction st
            ON st.salestransactionseq = crd.salestransactionseq
            AND st.tenantid = 'AIAS'
            AND st.processingunitseq = crd.processingunitseq
            -- and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
        INNER JOIN
            CS_CREDITTYPE ct
            ON crd.CREDITTYPESEQ = ct.DATATYPESEQ
            AND ct.Removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
        INNER JOIN
            EXT.AIA_tmp_comls_step0_2 ip
            ON 1 = 1
            AND ip.salestransactionseq = crd.salestransactionseq
            AND crd.genericattribute12 = ip.wri_agt_code_ORIG
            AND ip.CALCULATION_PERIOD = :V_CAL_PERIOD
        INNER JOIN
            cs_businessunit bu
            ON st.businessunitmap = bu.mask
        WHERE
            crd.tenantid = 'AIAS'
            AND crd.processingunitseq = :V_PROCESSINGUNITSEQ;-- and bu.name = STR_BUNAME_FA

    /* ORIGSQL: EXT.Log('insert 1_2 done for FA '||SQL%ROWCOUNT) */
    CALL EXT.Log('insert 1_2 done for FA '||::ROWCOUNT);  

    --delete from EXT.AIA_TMP_COMLS_STEP1_2 where transaction_Date <DT_CB_START_DATE or transaction_Date>DT_CB_END_DATE;

    --Log('delete 1_2 done for FA '||SQL%ROWCOUNT);

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: DBMS_STATS.GATHER_TABLE_STATS(ownname => '"AIASEXT"', tabname => '"EXT.AIA_TMP_COMLS_STEP1_2"', estimate_percent => 1) */
    EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| '"AIASEXT"'|| '.'|| '"EXT.AIA_TMP_COMLS_STEP1_2"';

    /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_comls_step2_2'; */
    /* ORIGSQL: truncate table EXT.AIA_tmp_comls_step2_2 ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_step2_2';

    /* ORIGSQL: insert into EXT.AIA_tmp_comls_step2_2 select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq, null as clawback_name from cs_measurement m join EXT.AIA_tmp_comls_period p on m.periodseq=p.p(...) */
    INSERT INTO EXT.AIA_tmp_comls_step2_2
        SELECT   /* ORIGSQL: select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq, null as clawback_name from cs_measurement m join EXT.AIA_tmp_comls_period p on m.periodseq=p.periodseq inner join (SELECT distin(...) */
            measurementseq,
            m.name,
            m.periodseq,
            payeeseq,
            ruleseq,
            positionseq,
            NULL AS clawback_name
        FROM
            cs_measurement m
        INNER JOIN
            EXT.AIA_tmp_comls_period p
            ON m.periodseq = p.periodseq
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select distinct SOURCE_RULE_OUTPUT from EXT.AIA_CB_RULES_LOOKUP where RULE_TYPE = 'PM' AND CLAWBACK_NAME ='NADOR') */
                    DISTINCT
                    SOURCE_RULE_OUTPUT
                FROM
                    EXT.AIA_CB_RULES_LOOKUP
                WHERE
                    RULE_TYPE = 'PM'
                    AND CLAWBACK_NAME = 'NADOR'
            ) AS pmr
            ON pmr.SOURCE_RULE_OUTPUT = m.name
        WHERE
            m.processingunitseq = :V_PROCESSINGUNITSEQ
            AND m.tenantid = 'AIAS';

    /* ORIGSQL: EXT.Log('insert 2_2 done for FA '||SQL%ROWCOUNT) */
    CALL EXT.Log('insert 2_2 done for FA '||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: DBMS_STATS.GATHER_TABLE_STATS(ownname => '"AIASEXT"', tabname => '"EXT.AIA_TMP_COMLS_STEP2_2"', estimate_percent => 1) */
    EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| '"AIASEXT"'|| '.'|| '"EXT.AIA_TMP_COMLS_STEP2_2"';

    /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_comls_step3_2'; */
    /* ORIGSQL: truncate table EXT.AIA_tmp_comls_step3_2 ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_step3_2';

    /* ORIGSQL: insert into EXT.AIA_tmp_comls_step3_2 select pct.creditseq pctCreditSeq, pct.measurementseq, pct.contributionvalue PctContribValue, depin.depositseq, s1.CREDITSEQ,SALESTRANSACTIONSEQ,CALCULATION_PERIOD,PO(...) */
    INSERT INTO EXT.AIA_tmp_comls_step3_2
        SELECT   /* ORIGSQL: select pct.creditseq pctCreditSeq, pct.measurementseq, pct.contributionvalue PctContribValue, depin.depositseq, s1.CREDITSEQ,SALESTRANSACTIONSEQ,CALCULATION_PERIOD,POLICY_NUMBER,POLICYIDSEQ,LIFE_NUMBE(...) */
            pct.creditseq AS pctCreditSeq,
            pct.measurementseq,
            pct.contributionvalue AS PctContribValue,
            depin.depositseq,
            s1.CREDITSEQ,
            SALESTRANSACTIONSEQ,
            CALCULATION_PERIOD,
            POLICY_NUMBER,
            POLICYIDSEQ,
            LIFE_NUMBER,
            COVERAGE_NUMBER,
            RIDER_NUMBER,
            COMPONENT_CODE,
            COMPONENT_NAME,
            BASE_RIDER_IND,
            TRANSACTION_DATE,
            PAYOR_CODE,
            CREDITTYPE,
            CREDIT_NAME,
            CREDIT_VALUE,
            s1.PERIODSEQ,
            PRODUCT_NAME,
            POLICY_YEAR,
            COMMISSION_RATE,
            PAID_TO_DATE,
            s2.name AS mname,
            s2.periodseq AS mPeriodSeq,
            s2.payeeseq AS mPayeeSeq,
            s2.ruleseq AS mruleSeq,
            s2.positionseq AS mPositionSeq,
            s2.clawback_name,
            WRI_AGT_CODE,
            QTRYR,
            GD2,
            pct.contributionvalue AS CONTRIBUTIONVALUE,
            s1.genericattribute13,
            s1.genericattribute14,
            s1.crd_positionseq,
            s1.crd_ruleseq
        FROM
            cs_pmcredittrace pct
        INNER JOIN
            EXT.AIA_tmp_comls_step1_2 s1
            ON pct.creditseq = s1.creditseq
        INNER JOIN
            EXT.AIA_tmp_comls_step2_2 s2
            ON s2.measurementseq = pct.measurementseq --and s2.ruleseq=pct.ruleseq
            --and pct.targetperiodseq=s2.periodseq
        INNER JOIN
            CS_INCENTIVEPMTRACE inpm
            ON s2.MEASUREMENTSEQ = inpm.MEASUREMENTSEQ
            AND inpm.tenantid = 'AIAS'
            AND inpm.processingunitseq = :V_PROCESSINGUNITSEQ
            --and inpm.targetperiodseq=s2.periodseq 
        INNER JOIN
            /*CS_DEPOSITINCENTIVETRACE*/
            (
                SELECT   /* ORIGSQL: (select * from cs_depositincentivetrace) */
                    *
                FROM
                    cs_depositincentivetrace
        UNION
            SELECT   /* ORIGSQL: select * from ext.aias_depositincentivetrace) AS depin on inpm.incentiveseq = depin.incentiveseq and inpm.tenantid=depin.tenantid where depin.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ and depin.tenantid='AI(...) */
                *
            FROM
                ext.aias_depositincentivetrace
        ) AS depin
        ON inpm.incentiveseq = depin.incentiveseq
        AND inpm.tenantid = depin.tenantid
        --and depin.targetperiodseq=s2.periodseq
    WHERE
        depin.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ
        AND depin.tenantid = 'AIAS';

    /* ORIGSQL: EXT.Log('insert 3_2 part a done for FA'||SQL%ROWCOUNT) */
    CALL EXT.Log('insert 3_2 part a done for FA'||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: DBMS_STATS.GATHER_TABLE_STATS(ownname => '"AIASEXT"', tabname => '"EXT.AIA_TMP_COMLS_STEP3_2"', estimate_percent => 1) */
    EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| '"AIASEXT"'|| '.'|| '"EXT.AIA_TMP_COMLS_STEP3_2"';

    /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

    --verstion 13 start

    /* ORIGSQL: insert into EXT.AIA_tmp_comls_step3_2 select pct.creditseq pctCreditSeq, pct.measurementseq, pct.contributionvalue PctContribValue, depin.depositseq, s1.CREDITSEQ,SALESTRANSACTIONSEQ,CALCULATION_PERIOD,PO(...) */
    INSERT INTO EXT.AIA_tmp_comls_step3_2
        SELECT   /* ORIGSQL: select pct.creditseq pctCreditSeq, pct.measurementseq, pct.contributionvalue PctContribValue, depin.depositseq, s1.CREDITSEQ,SALESTRANSACTIONSEQ,CALCULATION_PERIOD,POLICY_NUMBER,POLICYIDSEQ,LIFE_NUMBE(...) */
            pct.creditseq AS pctCreditSeq,
            pct.measurementseq,
            pct.contributionvalue AS PctContribValue,
            depin.depositseq,
            s1.CREDITSEQ,
            SALESTRANSACTIONSEQ,
            CALCULATION_PERIOD,
            POLICY_NUMBER,
            POLICYIDSEQ,
            LIFE_NUMBER,
            COVERAGE_NUMBER,
            RIDER_NUMBER,
            COMPONENT_CODE,
            COMPONENT_NAME,
            BASE_RIDER_IND,
            TRANSACTION_DATE,
            PAYOR_CODE,
            CREDITTYPE,
            CREDIT_NAME,
            CREDIT_VALUE,
            s1.PERIODSEQ,
            PRODUCT_NAME,
            POLICY_YEAR,
            COMMISSION_RATE,
            PAID_TO_DATE,
            s2.name AS mname,
            s2.periodseq AS mPeriodSeq,
            s2.payeeseq AS mPayeeSeq,
            s2.ruleseq AS mruleSeq,
            s2.positionseq AS mPositionSeq,
            s2.clawback_name,
            WRI_AGT_CODE,
            QTRYR,
            GD2,
            pct.contributionvalue AS CONTRIBUTIONVALUE,
            s1.genericattribute13,
            s1.genericattribute14,
            s1.crd_positionseq,
            s1.crd_ruleseq
        FROM
            cs_pmcredittrace pct
        INNER JOIN
            EXT.AIA_tmp_comls_step1_2 s1
            ON pct.creditseq = s1.creditseq
        INNER JOIN
            EXT.AIA_tmp_comls_step2_2 s2
            ON s2.measurementseq = pct.measurementseq
        INNER JOIN
            CS_PMSELFTRACE CPT
            ON s2.measurementseq = CPT.sourcemeasurementseq
        INNER JOIN
            CS_INCENTIVEPMTRACE inpm
            ON CPT.TARGETMEASUREMENTSEQ = inpm.MEASUREMENTSEQ
            AND inpm.tenantid = 'AIAS'
            AND inpm.processingunitseq = :V_PROCESSINGUNITSEQ 
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select * from cs_depositincentivetrace) */
                    *
                FROM
                    cs_depositincentivetrace
        UNION
            SELECT   /* ORIGSQL: select * from ext.aias_depositincentivetrace) AS depin on inpm.incentiveseq = depin.incentiveseq and inpm.tenantid=depin.tenantid where depin.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ and depin.tenantid='AI(...) */
                *
            FROM
                ext.aias_depositincentivetrace
        ) AS depin
        ON inpm.incentiveseq = depin.incentiveseq
        AND inpm.tenantid = depin.tenantid
    WHERE
        depin.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ
        AND depin.tenantid = 'AIAS';

    /* ORIGSQL: EXT.Log('insert 3_2 part b for NADOR 2.1 On-Bridge done for FA'||SQL%ROWCOUNT) */
    CALL EXT.Log('insert 3_2 part b for NADOR 2.1 On-Bridge done for FA'||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: DBMS_STATS.GATHER_TABLE_STATS(ownname => '"AIASEXT"', tabname => '"EXT.AIA_TMP_COMLS_STEP3_2"', estimate_percent => 1) */
    EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| '"AIASEXT"'|| '.'|| '"EXT.AIA_TMP_COMLS_STEP3_2"';

    /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */
    -- verstion 13 end

    /* ORIGSQL: insert / *+ APPEND * / */

    /* ORIGSQL: insert into EXT.AIA_CB_TRACE_FORWARD_COMP select STR_BUNAME_FA as BUNAME, QtrYr as CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, mPositionSeq PAYEE_SEQ, substr(dep_pos.name, 4) as PAYEE_CODE, PAYOR_CODE(...) */
    INSERT INTO EXT.AIA_CB_TRACE_FORWARD_COMP
        SELECT   /* ORIGSQL: select STR_BUNAME_FA as BUNAME, QtrYr as CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, mPositionSeq PAYEE_SEQ, substr(dep_pos.name, 4) as PAYEE_CODE, PAYOR_CODE, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_(...) */
            :STR_BUNAME_FA AS BUNAME,
            QtrYr AS CALCULATION_PERIOD,
            POLICY_NUMBER,
            POLICYIDSEQ,
            mPositionSeq AS PAYEE_SEQ,
            SUBSTRING(dep_pos.name,4) AS PAYEE_CODE,  /* ORIGSQL: substr(dep_pos.name, 4) */
            PAYOR_CODE,
            LIFE_NUMBER,
            COVERAGE_NUMBER,
            RIDER_NUMBER,
            COMPONENT_CODE,
            COMPONENT_NAME,
            BASE_RIDER_IND,
            TRANSACTION_DATE,
            TO_VARCHAR(:DT_CB_START_DATE,'MON-YYYY') AS PROCESSING_PERIOD,  /* ORIGSQL: TO_CHAR(DT_CB_START_DATE,'MON-YYYY') */
            :STR_LUMPSUM AS CLAWBACK_TYPE,
            rl.CLAWBACK_NAME AS CLAWBACK_NAME,
            :STR_CB_NAME AS CLAWBACK_METHOD,
            CREDITTYPE,
            CREDITSEQ,
            CREDIT_NAME,
            CREDIT_VALUE,
            crd_positionseq AS crd_positionseq,
            GD2 AS crd_genericdate2,
            crd_ruleseq AS crd_ruleseq,
            measurementseq AS PM_SEQ,
            mname AS PM_NAME,
            x.contributionvalue*:V_NADOR_RATE AS PM_CONTRIBUTION_VALUE,
            :V_NADOR_RATE AS PM_RATE,
            dep.depositseq AS DEPOSITSEQ,
            /*dep.name*/ REPLACE(dep.name,'_MANUAL','') AS DEPOSIT_NAME,
            dep.value AS DEPOSIT_VALUE,
            x.periodseq AS PERIODSEQ,
            x.salestransactionseq AS SALESTRANSACTIONSEQ,
            PRODUCT_NAME,
            POLICY_YEAR,
            COMMISSION_RATE,
            PAID_TO_DATE,
            :P_BATCH_NO AS BATCH_NUMBER,
            CURRENT_TIMESTAMP AS CREATED_DATE,  /* ORIGSQL: sysdate */
            '',
            '',
            NULL AS deposit_period
        FROM
            EXT.AIA_tmp_comls_step3_2 x
        INNER JOIN
            cs_deposit dep
            ON dep.depositseq = x.depositseq
        INNER JOIN
            cs_position dep_pos
            ON dep.positionseq = dep_pos.ruleelementownerseq
            AND dep_pos.removedate = :DT_REMOVEDATE
            AND dep_pos.effectivestartdate <= x.GD2
            AND dep_pos.effectiveenddate > x.GD2
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME from EXT.AIA_CB_RULES_LOOKUP where RULE_TYPE = 'DR' AND CLAWBACK_NAME IN ('NADOR_FA_2.1')) */
                    DISTINCT
                    SOURCE_RULE_OUTPUT,
                    CLAWBACK_NAME
                FROM
                    EXT.AIA_CB_RULES_LOOKUP
                WHERE
                    RULE_TYPE = 'DR'
                    AND CLAWBACK_NAME IN ('NADOR_FA_2.1')
            ) AS rl
            ON /*dep.NAME*/ REPLACE(Dep.name,'_MANUAL','') = rl.SOURCE_RULE_OUTPUT
        WHERE
            dep.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ;

    --and dep.periodseq =  x.periodseq 170807

    --insert /*+ APPEND */ into EXT.AIA_CB_TRACE_FORWARD_COMP
    /*select STR_BUNAME as BUNAME,
           ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
           ip.ponumber as POLICY_NUMBER,
           ip.policyidseq as POLICYIDSEQ,
           pm.positionseq as PAYEE_SEQ,
           substr(dep_pos.name, 4) as PAYEE_CODE,
           crd.genericattribute12 as PAYOR_CODE,
           ip.life_number as LIFE_NUMBER,
           ip.coverage_number as COVERAGE_NUMBER,
           ip.rider_number as RIDER_NUMBER,
           ip.component_code as COMPONENT_CODE,
           ip.component_name as COMPONENT_NAME,
           ip.base_rider_ind as BASE_RIDER_IND,
           crd.compensationdate as TRANSACTION_DATE,
           TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
           STR_LUMPSUM as CLAWBACK_TYPE,
            rl.CLAWBACK_NAME       as CLAWBACK_NAME,
             STR_CB_NAME as CLAWBACK_METHOD,
           ct.credittypeid        as CREDIT_TYPE,
           crd.creditseq          as CREDITSEQ,
           crd.name               as CREDIT_NAME,
           crd.value              as CREDIT_VALUE,
            crd.positionseq as crd_positionseq,
           st.genericdate2 as crd_genericdate2,
           crd.ruleseq as crd_ruleseq,
           pm.measurementseq      as PM_SEQ,
           pm.name                as PM_NAME,
           pct.contributionvalue*V_NADOR_RATE  as PM_CONTRIBUTION_VALUE,
           V_NADOR_RATE           as PM_RATE,
           dep.depositseq         as DEPOSITSEQ,
           dep.name               as DEPOSIT_NAME,
           dep.value              as DEPOSIT_VALUE,
           crd.periodseq          as PERIODSEQ,
           st.salestransactionseq as SALESTRANSACTIONSEQ,
           crd.genericattribute2  as PRODUCT_NAME,
           crd.genericnumber1     as POLICY_YEAR,
           st.genericnumber2      as COMMISSION_RATE,
           st.genericdate4        as PAID_TO_DATE,
           P_BATCH_NO             as BATCH_NUMBER,
           sysdate                as CREATED_DATE
      FROM CS_SALESTRANSACTION st
     inner join CS_CREDIT crd
        on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ and st.tenantid=crd.tenantid and st.processingunitseq=pm.processingunitseq
     inner join CS_PMCREDITTRACE pct
        on crd.CREDITSEQ = pct.CREDITSEQ and pct.tenantid=crd.tenantid and crd.processingunitseq=pm.processingunitseq
     and pct.sourceperiodseq=crd.periodseq
     inner join CS_MEASUREMENT pm
        on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ and pct.tenantid=pm.tenantid and pct.processingunitseq=pm.processingunitseq
     and pm.periodseq=pct.targetperiodseq
     inner join (select distinct SOURCE_RULE_OUTPUT
                       from EXT.AIA_CB_RULES_LOOKUP
                      where RULE_TYPE = 'PM'
     AND CLAWBACK_NAME ='NADOR')pmr
        on pmr.SOURCE_RULE_OUTPUT = pm.name
     inner join CS_INCENTIVEPMTRACE inpm
        on pm.MEASUREMENTSEQ = inpm.MEASUREMENTSEQ and inpm.tenantid=pm.tenantid and inpm.processingunitseq=pm.processingunitseq
     inner join CS_DEPOSITINCENTIVETRACE depin
        on inpm.incentiveseq = depin.incentiveseq and inpm.tenantid=depin.tenantid and inpm.processingunitseq=depin.processingunitseq
     inner join cs_deposit dep
        on depin.depositseq = dep.depositseq and dep.tenantid=depin.tenantid and dep.processingunitseq=depin.processingunitseq
     inner join cs_position dep_pos
        on dep.positionseq = dep_pos.ruleelementownerseq
     and dep_pos.removedate = DT_REMOVEDATE
     and dep_pos.effectivestartdate <= crd.genericdate2
     and dep_pos.effectiveenddate > crd.genericdate2
     inner join CS_CREDITTYPE ct
        on crd.CREDITTYPESEQ = ct.DATATYPESEQ
     and ct.Removedate = DT_REMOVEDATE
     inner join EXT.AIA_CB_IDENTIFY_POLICY ip
        on st.PONUMBER = ip.PONUMBER
     AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
     AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
     AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
     AND st.PRODUCTID = ip.COMPONENT_CODE
     and crd.genericattribute12 = ip.wri_agt_code
     and ip.quarter || ' ' || ip.year = V_CAL_PERIOD
     inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
                       from EXT.AIA_CB_RULES_LOOKUP
                      where RULE_TYPE = 'DR'
         AND CLAWBACK_NAME IN ('NADOR')) rl
        on dep.NAME = rl.SOURCE_RULE_OUTPUT
     WHERE st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
     AND st.BUSINESSUNITMAP = 1
     and dep.periodseq =  t_periodseq(i)
     and st.tenantid='AIAS' and st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
     and crd.tenantid='AIAS' and crd.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
     and pct.tenantid='AIAS' and pct.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
     and pm.tenantid='AIAS' and pm.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
     and inpm.tenantid='AIAS' and inpm.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
     and depin.tenantid='AIAS' and depin.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
     and dep.tenantid='AIAS' and dep.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
     and dep_pos.tenantid='AIAS' and dep_pos.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ;
    
    -- and crd.genericattribute16 not in ('RO', 'RNO');
    
    
    commit;
    --end loop;
    */
    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for NADOR' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for NADOR'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    --Version 2 add by Amanda for BSC SPI clawback begin
    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_comls_step2_2'; */
    /* ORIGSQL: truncate table EXT.AIA_tmp_comls_step2_2 ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_step2_2';

    SELECT
        year_qtr
    INTO
        V_First_QTR
    FROM
        EXT.AIA_TMP_COMLS_PERIOD_SPI
    WHERE
        year_qtr IS NOT NULL
        AND qtr_order = 1;

    SELECT
        year_qtr
    INTO
        V_Second_QTR
    FROM
        EXT.AIA_TMP_COMLS_PERIOD_SPI
    WHERE
        year_qtr IS NOT NULL
        AND qtr_order = 2;

    /* ORIGSQL: insert into EXT.AIA_tmp_comls_step2_2 select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq, null as clawback_name from cs_measurement m join EXT.AIA_TMP_COMLS_PERIOD_SPI p on m.periodseq(...) */
    INSERT INTO EXT.AIA_tmp_comls_step2_2
        SELECT   /* ORIGSQL: select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq, null as clawback_name from cs_measurement m join EXT.AIA_TMP_COMLS_PERIOD_SPI p on m.periodseq=p.periodseq inner join (SELECT di(...) */
            measurementseq,
            m.name,
            m.periodseq,
            payeeseq,
            ruleseq,
            positionseq,
            NULL AS clawback_name
        FROM
            cs_measurement m
        INNER JOIN
            EXT.AIA_TMP_COMLS_PERIOD_SPI p
            ON m.periodseq = p.periodseq
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select distinct SOURCE_RULE_OUTPUT from EXT.AIA_CB_RULES_LOOKUP where RULE_TYPE = 'PM' AND CLAWBACK_NAME ='SPI_FA') */
                    DISTINCT
                    SOURCE_RULE_OUTPUT
                FROM
                    EXT.AIA_CB_RULES_LOOKUP
                WHERE
                    RULE_TYPE = 'PM'
                    AND CLAWBACK_NAME = 'SPI_FA'
            ) AS pmr
            ON pmr.SOURCE_RULE_OUTPUT = m.name
        WHERE
            m.processingunitseq = :V_PROCESSINGUNITSEQ
            AND m.value <> 0
            AND m.tenantid = 'AIAS';

    /* ORIGSQL: EXT.Log('insert 2_2 done for SPI FA '||SQL%ROWCOUNT) */
    CALL EXT.Log('insert 2_2 done for SPI FA '||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_comls_step3_3'; */
    /* ORIGSQL: truncate table EXT.AIA_tmp_comls_step3_3 ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_step3_3';

    /* ORIGSQL: insert into EXT.AIA_tmp_comls_step3_3(PCTCREDITSEQ, MEASUREMENTSEQ, PCTCONTRIBVALUE, CREDITSEQ, SALESTRANSACTIONSEQ, CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUM(...) */
    INSERT INTO EXT.AIA_tmp_comls_step3_3
        (
            PCTCREDITSEQ, MEASUREMENTSEQ, PCTCONTRIBVALUE, CREDITSEQ, SALESTRANSACTIONSEQ, CALCULATION_PERIOD,
            POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,
            COMPONENT_NAME, BASE_RIDER_IND, TRANSACTION_DATE, PAYOR_CODE, CREDITTYPE, CREDIT_NAME,
            CREDIT_VALUE, PERIODSEQ, PRODUCT_NAME, POLICY_YEAR, COMMISSION_RATE, PAID_TO_DATE,
            MNAME, MPERIODSEQ, MPAYEESEQ, MRULESEQ, MPOSITIONSEQ, CLAWBACK_NAME,
            WRI_AGT_CODE, QTRYR, GD2, CONTRIBUTIONVALUE, GENERICATTRIBUTE13, GENERICATTRIBUTE14,
            CRD_POSITIONSEQ, CRD_RULESEQ, SPI_RATE, YTD_MEASUREMENTSEQ
        )
        SELECT   /* ORIGSQL: select pct.creditseq pctCreditSeq,pct.measurementseq, pct.contributionvalue PctContribValue, s1.CREDITSEQ,SALESTRANSACTIONSEQ, CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUM(...) */
            pct.creditseq AS pctCreditSeq,
            pct.measurementseq,
            pct.contributionvalue AS PctContribValue,
            s1.CREDITSEQ,
            SALESTRANSACTIONSEQ,
            CALCULATION_PERIOD,
            POLICY_NUMBER,
            POLICYIDSEQ,
            LIFE_NUMBER,
            COVERAGE_NUMBER,
            RIDER_NUMBER,
            COMPONENT_CODE,
            COMPONENT_NAME,
            BASE_RIDER_IND,
            TRANSACTION_DATE,
            PAYOR_CODE,
            CREDITTYPE,
            CREDIT_NAME,
            CREDIT_VALUE,
            s1.PERIODSEQ,
            PRODUCT_NAME,
            POLICY_YEAR,
            COMMISSION_RATE,
            PAID_TO_DATE,
            s2.name AS mname,
            sm2.periodseq AS mPeriodSeq/* --s2.periodseq mPeriodSeq, */, s2.payeeseq AS mPayeeSeq,
            s2.ruleseq AS mruleSeq,
            s2.positionseq AS mPositionSeq,
            s2.clawback_name,
            WRI_AGT_CODE,
            QTRYR,
            GD2,
            pct.contributionvalue AS CONTRIBUTIONVALUE/* --sm1.value as CONTRIBUTIONVALUE, */, s1.genericattribute13,
            s1.genericattribute14,
            s1.crd_positionseq,
            s1.crd_ruleseq
            /*       depin.depositseq deposit_seq,
                   sm4.genericnumber1 SPI_RATE --SPI_RATE */,
            0,
            sm2.measurementseq
        FROM
            cs_pmcredittrace pct
        INNER JOIN
            EXT.AIA_tmp_comls_step1_2 s1
            ON pct.creditseq = s1.creditseq
        INNER JOIN
            EXT.AIA_TMP_COMLS_PERIOD_SPI p_spi
            --only get 6 months period for SPI
            ON s1.periodseq = p_spi.periodseq
        INNER JOIN
            EXT.AIA_tmp_comls_step2_2 s2
            ON pct.MEASUREMENTSEQ = s2.MEASUREMENTSEQ
            --for SM level 1 (SM_PIB_SG_SPI_SGPAFA)
        INNER JOIN
            CS_PMSELFTRACE pm_sm1
            ON s2.measurementseq = pm_sm1.sourcemeasurementseq
        INNER JOIN
            CS_MEASUREMENT sm1
            ON sm1.measurementseq = pm_sm1.targetmeasurementseq
            AND sm1.name = 'SM_PIB_SG_SPI_SGPAFA'
            --for SM level 2 (SM_PIB_YTD_SG_SPI_SGPAFA)
        INNER JOIN
            CS_MEASUREMENT sm2
            ON sm1.payeeseq = sm2.payeeseq
            AND sm1.positionseq = sm2.positionseq
            AND sm2.periodseq = p_spi.qtr_end_periodseq
            AND sm2.name = 'SM_PIB_YTD_SG_SPI_SGPAFA';

    IF SUBSTRING(:V_First_QTR,1,4) = SUBSTRING(:V_Second_QTR,1,4)  /* ORIGSQL: substr(V_Second_QTR,1,4) */
                                                                   /* ORIGSQL: substr(V_First_QTR,1,4) */
    THEN
        --only contribute to second quarter when same year

        /* ORIGSQL: insert into EXT.AIA_tmp_comls_step3_3(PCTCREDITSEQ, MEASUREMENTSEQ, PCTCONTRIBVALUE, CREDITSEQ, SALESTRANSACTIONSEQ, CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUM(...) */
        INSERT INTO EXT.AIA_tmp_comls_step3_3
            (
                PCTCREDITSEQ, MEASUREMENTSEQ, PCTCONTRIBVALUE, CREDITSEQ, SALESTRANSACTIONSEQ, CALCULATION_PERIOD,
                POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,
                COMPONENT_NAME, BASE_RIDER_IND, TRANSACTION_DATE, PAYOR_CODE, CREDITTYPE, CREDIT_NAME,
                CREDIT_VALUE, PERIODSEQ, PRODUCT_NAME, POLICY_YEAR, COMMISSION_RATE, PAID_TO_DATE,
                MNAME, MPERIODSEQ, MPAYEESEQ, MRULESEQ, MPOSITIONSEQ, CLAWBACK_NAME,
                WRI_AGT_CODE, QTRYR, GD2, CONTRIBUTIONVALUE, GENERICATTRIBUTE13, GENERICATTRIBUTE14,
                CRD_POSITIONSEQ, CRD_RULESEQ, SPI_RATE, YTD_MEASUREMENTSEQ
            )
            SELECT   /* ORIGSQL: select pct.creditseq pctCreditSeq,pct.measurementseq, pct.contributionvalue PctContribValue, s1.CREDITSEQ,SALESTRANSACTIONSEQ, CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUM(...) */
                pct.creditseq AS pctCreditSeq,
                pct.measurementseq,
                pct.contributionvalue AS PctContribValue,
                s1.CREDITSEQ,
                SALESTRANSACTIONSEQ,
                CALCULATION_PERIOD,
                POLICY_NUMBER,
                POLICYIDSEQ,
                LIFE_NUMBER,
                COVERAGE_NUMBER,
                RIDER_NUMBER,
                COMPONENT_CODE,
                COMPONENT_NAME,
                BASE_RIDER_IND,
                TRANSACTION_DATE,
                PAYOR_CODE,
                CREDITTYPE,
                CREDIT_NAME,
                CREDIT_VALUE,
                s1.PERIODSEQ,
                PRODUCT_NAME,
                POLICY_YEAR,
                COMMISSION_RATE,
                PAID_TO_DATE,
                s2.name AS mname,
                sm2.periodseq AS mPeriodSeq/* --s2.periodseq mPeriodSeq, */, s2.payeeseq AS mPayeeSeq,
                s2.ruleseq AS mruleSeq,
                s2.positionseq AS mPositionSeq,
                s2.clawback_name,
                WRI_AGT_CODE,
                QTRYR,
                GD2,
                pct.contributionvalue AS CONTRIBUTIONVALUE/* --sm1.value as CONTRIBUTIONVALUE, */, s1.genericattribute13,
                s1.genericattribute14,
                s1.crd_positionseq,
                s1.crd_ruleseq,
                0,
                sm2.measurementseq
            FROM
                cs_pmcredittrace pct
            INNER JOIN
                EXT.AIA_tmp_comls_step1_2 s1
                ON pct.creditseq = s1.creditseq
            INNER JOIN
                EXT.AIA_TMP_COMLS_PERIOD_SPI p_spi
                --only get 6 months period for SPI
                ON s1.periodseq = p_spi.periodseq
            INNER JOIN
                EXT.AIA_tmp_comls_step2_2 s2
                ON pct.MEASUREMENTSEQ = s2.MEASUREMENTSEQ
                --for SM level 1 (SM_PIB_SG_SPI_SGPAFA)
            INNER JOIN
                CS_PMSELFTRACE pm_sm1
                ON s2.measurementseq = pm_sm1.sourcemeasurementseq
            INNER JOIN
                CS_MEASUREMENT sm1
                ON sm1.measurementseq = pm_sm1.targetmeasurementseq
                AND sm1.name = 'SM_PIB_SG_SPI_SGPAFA'
                --for SM level 2 (SM_PIB_YTD_SG_SPI_SGPAFA)
            INNER JOIN
                CS_MEASUREMENT sm2
                ON sm1.payeeseq = sm2.payeeseq
                AND sm1.positionseq = sm2.positionseq
                AND sm2.periodseq = :V_period_seq2
                AND sm2.name = 'SM_PIB_YTD_SG_SPI_SGPAFA'
            WHERE
                p_spi.qtr_order = 1;
    END IF;

    /* ORIGSQL: EXT.Log('insert 3_3 done for SPI FA'||SQL%ROWCOUNT) */
    CALL EXT.Log('insert 3_3 done for SPI FA'||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into EXT.AIA_tmp_comls_step3_3 temp using (SELECT sm2_sm3.sourcemeasurementseq, depin.depositseq, sm4.genericnumber1 FROM CS_PMSELFTRACE sm2_sm3 inner join CS_MEASUREMENT sm3 on sm3.measurementseq =(...) */
    MERGE INTO EXT.AIA_tmp_comls_step3_3 AS temp
        /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_PMSELFTRACE' not found */
        /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_MEASUREMENT' not found */
        /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_INCENTIVEPMTRACE' not found */
        /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_INCENTIVE' not found */
        USING
        (
            SELECT   /* ORIGSQL: (select sm2_sm3.sourcemeasurementseq, depin.depositseq, sm4.genericnumber1 from CS_PMSELFTRACE sm2_sm3 inner join CS_MEASUREMENT sm3 on sm3.measurementseq = sm2_sm3.targetmeasurementseq and sm3.name =(...) */
                sm2_sm3.sourcemeasurementseq,
                depin.depositseq,
                sm4.genericnumber1
            FROM
                CS_PMSELFTRACE sm2_sm3
                --on sm2.measurementseq = sm2_sm3.sourcemeasurementseq
            INNER JOIN
                CS_MEASUREMENT sm3
                ON sm3.measurementseq = sm2_sm3.targetmeasurementseq
                AND sm3.name = 'SM_SPI_CALCULATE_YTD_SGPAFA'
                --for SM level 4 (SM_SPI_PAYMENT_QTR_SGPAFA)
            INNER JOIN
                CS_PMSELFTRACE sm3_sm4
                ON sm3.measurementseq = sm3_sm4.sourcemeasurementseq
            INNER JOIN
                CS_MEASUREMENT sm4
                ON sm4.measurementseq = sm3_sm4.targetmeasurementseq
                --for Incentive (I_SPI_SG_SGPAFA)
            INNER JOIN
                CS_INCENTIVEPMTRACE inpm
                ON sm4.measurementseq = inpm.MEASUREMENTSEQ
                AND inpm.tenantid = 'AIAS'
                AND inpm.processingunitseq = :V_PROCESSINGUNITSEQ
            INNER JOIN
                cs_incentive inc
                ON inpm.incentiveseq = inc.incentiveseq
                --for deposit (D_SPI_SG_SGPAFA)
            INNER JOIN
                CS_DEPOSITINCENTIVETRACE depin
                ON inpm.incentiveseq = depin.incentiveseq
                AND depin.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ
                AND depin.tenantid = 'AIAS'
        ) AS temp1
        ON (temp.YTD_MEASUREMENTSEQ = temp1.sourcemeasurementseq)
    WHEN MATCHED THEN
        UPDATE
            SET temp.DEPOSITSEQ = temp1.depositseq,
            temp.SPI_RATE = temp1.genericnumber1;

    /* ORIGSQL: EXT.Log('update 3_3 done for SPI FA'||SQL%ROWCOUNT) */
    CALL EXT.Log('update 3_3 done for SPI FA'||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: DBMS_STATS.GATHER_TABLE_STATS(ownname => '"AIASEXT"', tabname => '"EXT.AIA_TMP_COMLS_STEP3_3"', estimate_percent => 1) */
    EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| '"AIASEXT"'|| '.'|| '"EXT.AIA_TMP_COMLS_STEP3_3"';

    /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

    /* ORIGSQL: insert / *+ APPEND * / */

    /* ORIGSQL: insert into EXT.AIA_CB_TRACE_FORWARD_COMP select STR_BUNAME_FA as BUNAME, QtrYr as CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, mPositionSeq PAYEE_SEQ, substr(dep_pos.name, 4) as PAYEE_CODE, PAYOR_CODE(...) */
    INSERT INTO EXT.AIA_CB_TRACE_FORWARD_COMP
        SELECT   /* ORIGSQL: select STR_BUNAME_FA as BUNAME, QtrYr as CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, mPositionSeq PAYEE_SEQ, substr(dep_pos.name, 4) as PAYEE_CODE, PAYOR_CODE, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_(...) */
            :STR_BUNAME_FA AS BUNAME,
            QtrYr AS CALCULATION_PERIOD,
            POLICY_NUMBER,
            POLICYIDSEQ,
            mPositionSeq AS PAYEE_SEQ,
            SUBSTRING(dep_pos.name,4) AS PAYEE_CODE,  /* ORIGSQL: substr(dep_pos.name, 4) */
            PAYOR_CODE,
            LIFE_NUMBER,
            COVERAGE_NUMBER,
            RIDER_NUMBER,
            COMPONENT_CODE,
            COMPONENT_NAME,
            BASE_RIDER_IND,
            TRANSACTION_DATE,
            TO_VARCHAR(:DT_CB_START_DATE,'MON-YYYY') AS PROCESSING_PERIOD,  /* ORIGSQL: TO_CHAR(DT_CB_START_DATE,'MON-YYYY') */
            :STR_LUMPSUM AS CLAWBACK_TYPE,
            IFNULL(rl.CLAWBACK_NAME,'SPI_FA') AS CLAWBACK_NAME,  /* ORIGSQL: nvl(rl.CLAWBACK_NAME,'SPI_FA') */
            :STR_CB_NAME AS CLAWBACK_METHOD,
            CREDITTYPE,
            CREDITSEQ,
            CREDIT_NAME,
            CREDIT_VALUE,
            crd_positionseq AS crd_positionseq,
            GD2 AS crd_genericdate2,
            crd_ruleseq AS crd_ruleseq,
            measurementseq AS PM_SEQ,
            mname AS PM_NAME,
            x.contributionvalue AS PM_CONTRIBUTION_VALUE,
            /* --x.contributionvalue*SPI_RATE  as PM_CONTRIBUTION_VALUE, */
            SPI_RATE AS PM_RATE,
            x.depositseq AS DEPOSITSEQ,
            /*dep.name*/ REPLACE(dep.name,'_MANUAL','') AS DEPOSIT_NAME,
            dep.value AS DEPOSIT_VALUE,
            x.periodseq AS PERIODSEQ,
            x.salestransactionseq AS SALESTRANSACTIONSEQ,
            PRODUCT_NAME,
            POLICY_YEAR,
            COMMISSION_RATE,
            PAID_TO_DATE,
            :P_BATCH_NO AS BATCH_NUMBER,
            CURRENT_TIMESTAMP AS CREATED_DATE,  /* ORIGSQL: sysdate */
            '',
            '',
            x.MPERIODSEQ  /* -- Quarter end periodseq */
        FROM
            EXT.AIA_tmp_comls_step3_3 x
        INNER JOIN
            cs_position dep_pos
            ON x.mPositionSeq = dep_pos.ruleelementownerseq
            AND dep_pos.removedate = :DT_REMOVEDATE
            AND dep_pos.effectivestartdate <= x.GD2
            AND dep_pos.effectiveenddate > x.GD2
        LEFT OUTER JOIN
            cs_deposit dep
            ON dep.depositseq = x.depositseq
            AND dep.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ
        LEFT OUTER JOIN
            (
                SELECT   /* ORIGSQL: (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME from EXT.AIA_CB_RULES_LOOKUP where RULE_TYPE = 'DR' AND CLAWBACK_NAME = 'SPI_FA') */
                    DISTINCT
                    SOURCE_RULE_OUTPUT,
                    CLAWBACK_NAME
                FROM
                    EXT.AIA_CB_RULES_LOOKUP
                WHERE
                    RULE_TYPE = 'DR'
                    AND CLAWBACK_NAME = 'SPI_FA'
            ) AS rl
            ON /*dep.NAME*/ REPLACE(Dep.name,'_MANUAL','') = rl.SOURCE_RULE_OUTPUT;

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP for SPI FA' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP for SPI FA'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    --Version 2 end

    --Version 17 add by Zero for BSC SPI FA 2.1 clawback begin
    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_comls_step2_2'; */
    /* ORIGSQL: truncate table EXT.AIA_tmp_comls_step2_2 ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_step2_2';

    --select year_qtr into V_First_QTR from EXT.AIA_TMP_COMLS_PERIOD_SPI where year_qtr IS NOT NULL and qtr_order = 1;
    --select year_qtr into V_Second_QTR from EXT.AIA_TMP_COMLS_PERIOD_SPI where year_qtr IS NOT NULL and qtr_order = 2;

    /* ORIGSQL: insert into EXT.AIA_tmp_comls_step2_2 select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq, null as clawback_name from cs_measurement m join EXT.AIA_TMP_COMLS_PERIOD_SPI p on m.periodseq(...) */
    INSERT INTO EXT.AIA_tmp_comls_step2_2
        SELECT   /* ORIGSQL: select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq, null as clawback_name from cs_measurement m join EXT.AIA_TMP_COMLS_PERIOD_SPI p on m.periodseq=p.periodseq inner join (SELECT di(...) */
            measurementseq,
            m.name,
            m.periodseq,
            payeeseq,
            ruleseq,
            positionseq,
            NULL AS clawback_name
        FROM
            cs_measurement m
        INNER JOIN
            EXT.AIA_TMP_COMLS_PERIOD_SPI p
            ON m.periodseq = p.periodseq
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select distinct SOURCE_RULE_OUTPUT from EXT.AIA_CB_RULES_LOOKUP where RULE_TYPE = 'PM' AND CLAWBACK_NAME ='SPI_FA_2.1') */
                    DISTINCT
                    SOURCE_RULE_OUTPUT
                FROM
                    EXT.AIA_CB_RULES_LOOKUP
                WHERE
                    RULE_TYPE = 'PM'
                    AND CLAWBACK_NAME = 'SPI_FA_2.1'
            ) AS pmr
            -- version 14 Harm_BSC_SPI
            ON pmr.SOURCE_RULE_OUTPUT = m.name
        WHERE
            m.processingunitseq = :V_PROCESSINGUNITSEQ
            AND m.value <> 0
            AND m.tenantid = 'AIAS';

    /* ORIGSQL: EXT.Log('insert 2_2 done for SPI FA 2.1 '||SQL%ROWCOUNT) */
    CALL EXT.Log('insert 2_2 done for SPI FA 2.1 '||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_comls_step3_3'; */
    /* ORIGSQL: truncate table EXT.AIA_tmp_comls_step3_3 ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_step3_3';

    /* ORIGSQL: insert into EXT.AIA_tmp_comls_step3_3(PCTCREDITSEQ, MEASUREMENTSEQ, PCTCONTRIBVALUE, CREDITSEQ, SALESTRANSACTIONSEQ, CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUM(...) */
    INSERT INTO EXT.AIA_tmp_comls_step3_3
        (
            PCTCREDITSEQ, MEASUREMENTSEQ, PCTCONTRIBVALUE, CREDITSEQ, SALESTRANSACTIONSEQ, CALCULATION_PERIOD,
            POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,
            COMPONENT_NAME, BASE_RIDER_IND, TRANSACTION_DATE, PAYOR_CODE, CREDITTYPE, CREDIT_NAME,
            CREDIT_VALUE, PERIODSEQ, PRODUCT_NAME, POLICY_YEAR, COMMISSION_RATE, PAID_TO_DATE,
            MNAME, MPERIODSEQ, MPAYEESEQ, MRULESEQ, MPOSITIONSEQ, CLAWBACK_NAME,
            WRI_AGT_CODE, QTRYR, GD2, CONTRIBUTIONVALUE, GENERICATTRIBUTE13, GENERICATTRIBUTE14,
            CRD_POSITIONSEQ, CRD_RULESEQ, SPI_RATE, YTD_MEASUREMENTSEQ
        )
        SELECT   /* ORIGSQL: select pct.creditseq pctCreditSeq,pct.measurementseq, pct.contributionvalue PctContribValue, s1.CREDITSEQ,SALESTRANSACTIONSEQ, CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUM(...) */
            pct.creditseq AS pctCreditSeq,
            pct.measurementseq,
            pct.contributionvalue AS PctContribValue,
            s1.CREDITSEQ,
            SALESTRANSACTIONSEQ,
            CALCULATION_PERIOD,
            POLICY_NUMBER,
            POLICYIDSEQ,
            LIFE_NUMBER,
            COVERAGE_NUMBER,
            RIDER_NUMBER,
            COMPONENT_CODE,
            COMPONENT_NAME,
            BASE_RIDER_IND,
            TRANSACTION_DATE,
            PAYOR_CODE,
            CREDITTYPE,
            CREDIT_NAME,
            CREDIT_VALUE,
            s1.PERIODSEQ,
            PRODUCT_NAME,
            POLICY_YEAR,
            COMMISSION_RATE,
            PAID_TO_DATE,
            s2.name AS mname,
            sm2.periodseq AS mPeriodSeq/* --s2.periodseq mPeriodSeq, */, s2.payeeseq AS mPayeeSeq,
            s2.ruleseq AS mruleSeq,
            s2.positionseq AS mPositionSeq,
            s2.clawback_name,
            WRI_AGT_CODE,
            QTRYR,
            GD2,
            pct.contributionvalue AS CONTRIBUTIONVALUE/* --sm1.value as CONTRIBUTIONVALUE, */, s1.genericattribute13,
            s1.genericattribute14,
            s1.crd_positionseq,
            s1.crd_ruleseq
            /*       depin.depositseq deposit_seq,
                   sm4.genericnumber1 SPI_RATE --SPI_RATE */,
            0,
            sm2.measurementseq
        FROM
            cs_pmcredittrace pct
        INNER JOIN
            EXT.AIA_tmp_comls_step1_2 s1
            ON pct.creditseq = s1.creditseq
        INNER JOIN
            EXT.AIA_TMP_COMLS_PERIOD_SPI p_spi
            --only get 6 months period for SPI
            ON s1.periodseq = p_spi.periodseq
        INNER JOIN
            EXT.AIA_tmp_comls_step2_2 s2
            ON pct.MEASUREMENTSEQ = s2.MEASUREMENTSEQ
            --for SM level 1 (SM_PIB_SG_SPI/SM_PIB_SG_SPI_FAOB)
        INNER JOIN
            CS_PMSELFTRACE pm_sm1
            ON s2.measurementseq = pm_sm1.sourcemeasurementseq
        INNER JOIN
            CS_MEASUREMENT sm1
            ON sm1.measurementseq = pm_sm1.targetmeasurementseq
            AND sm1.name IN ('SM_PIB_SG_SPI','SM_PIB_SG_SPI_FAOB')-- version 17 Harm_BSC_SPI
            --for SM level 2 (SM_PIB_YTD_SG_SPI/SM_PIB_YTD_SG_SPI_FAOB)
        INNER JOIN
            CS_MEASUREMENT sm2
            ON sm1.payeeseq = sm2.payeeseq
            AND sm1.positionseq = sm2.positionseq
            AND sm2.periodseq = p_spi.qtr_end_periodseq
            AND sm2.name IN ('SM_PIB_YTD_SG_SPI','SM_PIB_YTD_SG_SPI_FAOB');-- version 17 Harm_BSC_SPI

    IF SUBSTRING(:V_First_QTR,1,4) = SUBSTRING(:V_Second_QTR,1,4)  /* ORIGSQL: substr(V_Second_QTR,1,4) */
                                                                   /* ORIGSQL: substr(V_First_QTR,1,4) */
    THEN
        --only contribute to second quarter when same year

        /* ORIGSQL: insert into EXT.AIA_tmp_comls_step3_3(PCTCREDITSEQ, MEASUREMENTSEQ, PCTCONTRIBVALUE, CREDITSEQ, SALESTRANSACTIONSEQ, CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUM(...) */
        INSERT INTO EXT.AIA_tmp_comls_step3_3
            (
                PCTCREDITSEQ, MEASUREMENTSEQ, PCTCONTRIBVALUE, CREDITSEQ, SALESTRANSACTIONSEQ, CALCULATION_PERIOD,
                POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,
                COMPONENT_NAME, BASE_RIDER_IND, TRANSACTION_DATE, PAYOR_CODE, CREDITTYPE, CREDIT_NAME,
                CREDIT_VALUE, PERIODSEQ, PRODUCT_NAME, POLICY_YEAR, COMMISSION_RATE, PAID_TO_DATE,
                MNAME, MPERIODSEQ, MPAYEESEQ, MRULESEQ, MPOSITIONSEQ, CLAWBACK_NAME,
                WRI_AGT_CODE, QTRYR, GD2, CONTRIBUTIONVALUE, GENERICATTRIBUTE13, GENERICATTRIBUTE14,
                CRD_POSITIONSEQ, CRD_RULESEQ, SPI_RATE, YTD_MEASUREMENTSEQ
            )
            SELECT   /* ORIGSQL: select pct.creditseq pctCreditSeq,pct.measurementseq, pct.contributionvalue PctContribValue, s1.CREDITSEQ,SALESTRANSACTIONSEQ, CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUM(...) */
                pct.creditseq AS pctCreditSeq,
                pct.measurementseq,
                pct.contributionvalue AS PctContribValue,
                s1.CREDITSEQ,
                SALESTRANSACTIONSEQ,
                CALCULATION_PERIOD,
                POLICY_NUMBER,
                POLICYIDSEQ,
                LIFE_NUMBER,
                COVERAGE_NUMBER,
                RIDER_NUMBER,
                COMPONENT_CODE,
                COMPONENT_NAME,
                BASE_RIDER_IND,
                TRANSACTION_DATE,
                PAYOR_CODE,
                CREDITTYPE,
                CREDIT_NAME,
                CREDIT_VALUE,
                s1.PERIODSEQ,
                PRODUCT_NAME,
                POLICY_YEAR,
                COMMISSION_RATE,
                PAID_TO_DATE,
                s2.name AS mname,
                sm2.periodseq AS mPeriodSeq/* --s2.periodseq mPeriodSeq, */, s2.payeeseq AS mPayeeSeq,
                s2.ruleseq AS mruleSeq,
                s2.positionseq AS mPositionSeq,
                s2.clawback_name,
                WRI_AGT_CODE,
                QTRYR,
                GD2,
                pct.contributionvalue AS CONTRIBUTIONVALUE/* --sm1.value as CONTRIBUTIONVALUE, */, s1.genericattribute13,
                s1.genericattribute14,
                s1.crd_positionseq,
                s1.crd_ruleseq,
                0,
                sm2.measurementseq
            FROM
                cs_pmcredittrace pct
            INNER JOIN
                EXT.AIA_tmp_comls_step1_2 s1
                ON pct.creditseq = s1.creditseq
            INNER JOIN
                EXT.AIA_TMP_COMLS_PERIOD_SPI p_spi
                --only get 6 months period for SPI
                ON s1.periodseq = p_spi.periodseq
            INNER JOIN
                EXT.AIA_tmp_comls_step2_2 s2
                ON pct.MEASUREMENTSEQ = s2.MEASUREMENTSEQ
                --for SM level 1 (SM_PIB_SG_SPI/SM_PIB_SG_SPI_FAOB)
            INNER JOIN
                CS_PMSELFTRACE pm_sm1
                ON s2.measurementseq = pm_sm1.sourcemeasurementseq
            INNER JOIN
                CS_MEASUREMENT sm1
                ON sm1.measurementseq = pm_sm1.targetmeasurementseq
                AND sm1.name IN ('SM_PIB_SG_SPI','SM_PIB_SG_SPI_FAOB') -- version 17 Harm_BSC_SPI
                --for SM level 2 (SM_PIB_YTD_SG_SPI/SM_PIB_YTD_SG_SPI_FAOB)
            INNER JOIN
                CS_MEASUREMENT sm2
                ON sm1.payeeseq = sm2.payeeseq
                AND sm1.positionseq = sm2.positionseq
                AND sm2.periodseq = :V_period_seq2
                AND sm2.name IN ('SM_PIB_YTD_SG_SPI','SM_PIB_YTD_SG_SPI_FAOB') -- version 17 Harm_BSC_SPI
            WHERE
                p_spi.qtr_order = 1;
    END IF;

    /* ORIGSQL: EXT.Log('insert 3_3 done for SPI FA 2.1'||SQL%ROWCOUNT) */
    CALL EXT.Log('insert 3_3 done for SPI FA 2.1'||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    -- old bize pay to old code/ new bize pay to new code

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into EXT.AIA_tmp_comls_step3_3 temp using (SELECT sm2_sm3.sourcemeasurementseq, depin.depositseq, sm4.genericnumber1 FROM CS_PMSELFTRACE sm2_sm3 inner join CS_MEASUREMENT sm3 on sm3.measurementseq =(...) */
    MERGE INTO EXT.AIA_tmp_comls_step3_3 AS temp       
        USING
        (
            SELECT   /* ORIGSQL: (select sm2_sm3.sourcemeasurementseq, depin.depositseq, sm4.genericnumber1 from CS_PMSELFTRACE sm2_sm3 inner join CS_MEASUREMENT sm3 on sm3.measurementseq = sm2_sm3.targetmeasurementseq and sm3.name I(...) */
                sm2_sm3.sourcemeasurementseq,
                depin.depositseq,
                sm4.genericnumber1
            FROM
                CS_PMSELFTRACE sm2_sm3
                --on sm2.measurementseq = sm2_sm3.sourcemeasurementseq
            INNER JOIN
                CS_MEASUREMENT sm3
                ON sm3.measurementseq = sm2_sm3.targetmeasurementseq
                AND sm3.name IN ('SM_SPI_CALCULATE_YTD_FAOB','SM_SPI_CALCULATE_YTD')-- version 17 Harm_BSC_SPI
                --for SM level 4 (SM_SPI_PAYMENT_QTR_FAOB/SM_SPI_PAYMENT_QTR)
            INNER JOIN
                CS_PMSELFTRACE sm3_sm4
                ON sm3.measurementseq = sm3_sm4.sourcemeasurementseq
            INNER JOIN
                CS_MEASUREMENT sm4
                ON sm4.measurementseq = sm3_sm4.targetmeasurementseq
                --for Incentive (I_SPI_SG_FAOB/I_SPI_SG)
            INNER JOIN
                CS_INCENTIVEPMTRACE inpm
                ON sm4.measurementseq = inpm.MEASUREMENTSEQ
                AND inpm.tenantid = 'AIAS'
                AND inpm.processingunitseq = :V_PROCESSINGUNITSEQ
            INNER JOIN
                cs_incentive inc
                ON inpm.incentiveseq = inc.incentiveseq
                --for deposit (D_SPI_SG_FAOB/D_SPI_SG)
            INNER JOIN
                CS_DEPOSITINCENTIVETRACE depin
                ON inpm.incentiveseq = depin.incentiveseq
                AND depin.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ
                AND depin.tenantid = 'AIAS'
        ) AS temp1
        ON (temp.YTD_MEASUREMENTSEQ = temp1.sourcemeasurementseq)
    WHEN MATCHED THEN
        UPDATE
            SET temp.DEPOSITSEQ = temp1.depositseq,
            temp.SPI_RATE = temp1.genericnumber1;

    /* ORIGSQL: EXT.Log('update 3_3 done for SPI FA 2.1'||SQL%ROWCOUNT) */
    CALL EXT.Log('update 3_3 done for SPI FA 2.1'||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    -- old bize pay to new code

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into EXT.AIA_tmp_comls_step3_3 temp using (SELECT sm2_sm3.sourcemeasurementseq, depin.depositseq, sm5.genericnumber1 FROM CS_PMSELFTRACE sm2_sm3 inner join CS_MEASUREMENT sm3 on sm3.measurementseq =(...) */
    MERGE INTO EXT.AIA_tmp_comls_step3_3 AS temp         
        USING
        (
            SELECT   /* ORIGSQL: (select sm2_sm3.sourcemeasurementseq, depin.depositseq, sm5.genericnumber1 from CS_PMSELFTRACE sm2_sm3 inner join CS_MEASUREMENT sm3 on sm3.measurementseq = sm2_sm3.targetmeasurementseq and sm3.name =(...) */
                sm2_sm3.sourcemeasurementseq,
                depin.depositseq,
                sm5.genericnumber1
            FROM
                CS_PMSELFTRACE sm2_sm3
                --on sm2.measurementseq = sm2_sm3.sourcemeasurementseq
            INNER JOIN
                CS_MEASUREMENT sm3
                ON sm3.measurementseq = sm2_sm3.targetmeasurementseq
                AND sm3.name = 'SM_PIB_YTD_SG_SPI_FAOB'
                --for SM level 4 (SM_SPI_CALCULATE_YTD_FAOB)
            INNER JOIN
                CS_PMSELFTRACE sm3_sm4
                ON sm3.measurementseq = sm3_sm4.sourcemeasurementseq
            INNER JOIN
                CS_MEASUREMENT sm4
                ON sm4.measurementseq = sm3_sm4.targetmeasurementseq
                AND sm4.name = 'SM_SPI_CALCULATE_YTD_FAOB'
                --for SM level 5 (SM_SPI_PAYMENT_QTR_FAOB)
            INNER JOIN
                CS_PMSELFTRACE sm4_sm5
                ON sm4.measurementseq = sm4_sm5.sourcemeasurementseq
            INNER JOIN
                CS_MEASUREMENT sm5
                ON sm5.measurementseq = sm4_sm5.targetmeasurementseq
                --for Incentive (I_SPI_SG_FAOB)
            INNER JOIN
                CS_INCENTIVEPMTRACE inpm
                ON sm5.measurementseq = inpm.MEASUREMENTSEQ
                AND inpm.tenantid = 'AIAS'
                AND inpm.processingunitseq = :V_PROCESSINGUNITSEQ
            INNER JOIN
                cs_incentive inc
                ON inpm.incentiveseq = inc.incentiveseq
                --for deposit (D_SPI_SG_FAOB)
            INNER JOIN
                CS_DEPOSITINCENTIVETRACE depin
                ON inpm.incentiveseq = depin.incentiveseq
                AND depin.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ
                AND depin.tenantid = 'AIAS'
        ) AS temp1
        ON (temp.YTD_MEASUREMENTSEQ = temp1.sourcemeasurementseq)
    WHEN MATCHED THEN
        UPDATE
            SET temp.DEPOSITSEQ = temp1.depositseq,
            temp.SPI_RATE = temp1.genericnumber1;

    /* ORIGSQL: EXT.Log('update 3_3 done for SPI FA 2.1'||SQL%ROWCOUNT) */
    CALL EXT.Log('update 3_3 done for SPI FA 2.1'||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: DBMS_STATS.GATHER_TABLE_STATS(ownname => '"AIASEXT"', tabname => '"EXT.AIA_TMP_COMLS_STEP3_3"', estimate_percent => 1) */
    EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| '"AIASEXT"'|| '.'|| '"EXT.AIA_TMP_COMLS_STEP3_3"';

    /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

    /* ORIGSQL: insert / *+ APPEND * / */

    /* ORIGSQL: insert into EXT.AIA_CB_TRACE_FORWARD_COMP select STR_BUNAME_FA as BUNAME, QtrYr as CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, mPositionSeq PAYEE_SEQ, substr(dep_pos.name, 4) as PAYEE_CODE, PAYOR_CODE(...) */
    INSERT INTO EXT.AIA_CB_TRACE_FORWARD_COMP
        SELECT   /* ORIGSQL: select STR_BUNAME_FA as BUNAME, QtrYr as CALCULATION_PERIOD, POLICY_NUMBER, POLICYIDSEQ, mPositionSeq PAYEE_SEQ, substr(dep_pos.name, 4) as PAYEE_CODE, PAYOR_CODE, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_(...) */
            :STR_BUNAME_FA AS BUNAME,
            QtrYr AS CALCULATION_PERIOD,
            POLICY_NUMBER,
            POLICYIDSEQ,
            mPositionSeq AS PAYEE_SEQ,
            SUBSTRING(dep_pos.name,4) AS PAYEE_CODE,  /* ORIGSQL: substr(dep_pos.name, 4) */
            PAYOR_CODE,
            LIFE_NUMBER,
            COVERAGE_NUMBER,
            RIDER_NUMBER,
            COMPONENT_CODE,
            COMPONENT_NAME,
            BASE_RIDER_IND,
            TRANSACTION_DATE,
            TO_VARCHAR(:DT_CB_START_DATE,'MON-YYYY') AS PROCESSING_PERIOD,  /* ORIGSQL: TO_CHAR(DT_CB_START_DATE,'MON-YYYY') */
            :STR_LUMPSUM AS CLAWBACK_TYPE,
            IFNULL(rl.CLAWBACK_NAME,'SPI_FA_2.1') AS CLAWBACK_NAME/* -- version 17 Harm_BSC_SPI */, :STR_CB_NAME AS CLAWBACK_METHOD,  /* ORIGSQL: nvl(rl.CLAWBACK_NAME,'SPI_FA_2.1') */
            CREDITTYPE,
            CREDITSEQ,
            CREDIT_NAME,
            CREDIT_VALUE,
            crd_positionseq AS crd_positionseq,
            GD2 AS crd_genericdate2,
            crd_ruleseq AS crd_ruleseq,
            measurementseq AS PM_SEQ,
            mname AS PM_NAME,
            x.contributionvalue AS PM_CONTRIBUTION_VALUE,
            /* --x.contributionvalue*SPI_RATE  as PM_CONTRIBUTION_VALUE, */
            SPI_RATE AS PM_RATE,
            x.depositseq AS DEPOSITSEQ,
            /*dep.name*/ REPLACE(dep.name,'_MANUAL','') AS DEPOSIT_NAME,
            dep.value AS DEPOSIT_VALUE,
            x.periodseq AS PERIODSEQ,
            x.salestransactionseq AS SALESTRANSACTIONSEQ,
            PRODUCT_NAME,
            POLICY_YEAR,
            COMMISSION_RATE,
            PAID_TO_DATE,
            :P_BATCH_NO AS BATCH_NUMBER,
            CURRENT_TIMESTAMP AS CREATED_DATE,  /* ORIGSQL: sysdate */
            '',
            '',
            x.MPERIODSEQ  /* -- Quarter end periodseq */
        FROM
            EXT.AIA_tmp_comls_step3_3 x
        INNER JOIN
            cs_position dep_pos
            ON x.mPositionSeq = dep_pos.ruleelementownerseq
            AND dep_pos.removedate = :DT_REMOVEDATE
            AND dep_pos.effectivestartdate <= TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND dep_pos.effectiveenddate > TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
        LEFT OUTER JOIN
            cs_deposit dep
            ON dep.depositseq = x.depositseq
            AND dep.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ
        LEFT OUTER JOIN
            (
                SELECT   /* ORIGSQL: (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME from EXT.AIA_CB_RULES_LOOKUP where RULE_TYPE = 'DR' AND CLAWBACK_NAME = 'SPI_FA_2.1') */
                    DISTINCT
                    SOURCE_RULE_OUTPUT,
                    CLAWBACK_NAME
                FROM
                    EXT.AIA_CB_RULES_LOOKUP
                WHERE
                    RULE_TYPE = 'DR'
                    AND CLAWBACK_NAME = 'SPI_FA_2.1'
            ) AS rl
            ON /*dep.NAME*/ REPLACE(Dep.name,'_MANUAL','') = rl.SOURCE_RULE_OUTPUT;

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP for SPI FA 2.1' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP for SPI FA 2.1'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    -- update old agent code for old bize pay to new code
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into EXT.AIA_CB_TRACE_FORWARD_COMP tf using (SELECT distinct ip.WRI_AGT_CODE, ip.ponumber, ip.COMPONENT_CODE, ip.FAOB_AGT_CODE FROM EXT.AIA_CB_IDENTIFY_POLICY ip where ip.FAOB_AGT_CODE IS NOT NULL and i(...) */
    MERGE INTO EXT.AIA_CB_TRACE_FORWARD_COMP AS tf
        USING
        (
            SELECT   /* ORIGSQL: (SELECT distinct ip.WRI_AGT_CODE, ip.ponumber, ip.COMPONENT_CODE, ip.FAOB_AGT_CODE FROM EXT.AIA_CB_IDENTIFY_POLICY ip where ip.FAOB_AGT_CODE IS NOT NULL and ip.YEAR IN (substr(V_First_QTR,1,4),substr(V_Se(...) */
                DISTINCT
                ip.WRI_AGT_CODE,
                ip.ponumber,
                ip.COMPONENT_CODE,
                ip.FAOB_AGT_CODE
            FROM
                EXT.AIA_CB_IDENTIFY_POLICY ip
            WHERE
                ip.FAOB_AGT_CODE IS NOT NULL
                AND ip.YEAR IN (SUBSTRING(:V_First_QTR,1,4),SUBSTRING(:V_Second_QTR,1,4))  /* ORIGSQL: substr(V_Second_QTR,1,4) */
                                                                                           /* ORIGSQL: substr(V_First_QTR,1,4) */
                AND ip.QUARTER IN ('Q'|| IFNULL(SUBSTRING(:V_First_QTR,6,1),''),'Q'|| IFNULL(SUBSTRING(:V_Second_QTR,6,1),''))  /* ORIGSQL: substr(V_Second_QTR,6,1) */
                                                                                                                                /* ORIGSQL: substr(V_First_QTR,6,1) */
                AND ip.BUNAME = :STR_BUNAME_FA
        ) AS temp
        ON (tf.POLICY_NUMBER = temp.ponumber
            AND tf.COMPONENT_CODE = temp.COMPONENT_CODE
            AND tf.BUNAME = :STR_BUNAME_FA
        AND tf.BATCH_NUMBER = :P_BATCH_NO)
    WHEN MATCHED THEN
        UPDATE
            SET tf.OLD_AGENT_CD = temp.WRI_AGT_CODE;

    /* ORIGSQL: EXT.Log('update EXT.AIA_CB_TRACE_FORWARD_COMP old agent code for SPI FA 2.1' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('update EXT.AIA_CB_TRACE_FORWARD_COMP old agent code for SPI FA 2.1'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    --Version 17 end

    /*Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP for SPI' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);
    
    --for lumpsum compensation trace forward for SPI
    for i in 1..t_periodseq.count loop
    insert into EXT.AIA_CB_TRACE_FORWARD_COMP
    select STR_BUNAME as BUNAME,
           ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
           ip.ponumber as POLICY_NUMBER,
           ip.policyidseq as POLICYIDSEQ,
           pm.positionseq as PAYEE_SEQ,
           substr(dep_pos.name, 4) as PAYEE_CODE,
           crd.genericattribute12 as PAYOR_CODE,
           ip.life_number as LIFE_NUMBER,
           ip.coverage_number as COVERAGE_NUMBER,
           ip.rider_number as RIDER_NUMBER,
           ip.component_code as COMPONENT_CODE,
           ip.component_name as COMPONENT_NAME,
           ip.base_rider_ind as BASE_RIDER_IND,
           crd.compensationdate as TRANSACTION_DATE,
           TO_CHAR(DT_CB_START_DATE, 'MON-YYYY') as PROCESSING_PERIOD,
           STR_LUMPSUM as CLAWBACK_TYPE,
           rl.CLAWBACK_NAME as CLAWBACK_NAME,
           STR_COMPENSATION as CLAWBACK_METHOD,
           ct.credittypeid as CREDIT_TYPE,
           crd.creditseq as CREDITSEQ,
           crd.name as CREDIT_NAME,
           crd.value as CREDIT_VALUE,
           crd.positionseq as crd_positionseq,
           st.genericdate2 as crd_genericdate2,
           crd.ruleseq as crd_ruleseq,
           pm.measurementseq as PM_SEQ,
           pm.name as PM_NAME,
           pct.contributionvalue as PM_CONTRIBUTION_VALUE,
           case
             when sm3.name like 'SM_SPI_RATE%' then
              sm3.value
             else
              0
           end as PM_RATE,
           dep.depositseq as DEPOSITSEQ,
           dep.name as DEPOSIT_NAME,
           dep.value as DEPOSIT_VALUE,
           crd.periodseq as PERIODSEQ,
           st.salestransactionseq as SALESTRANSACTIONSEQ,
           crd.genericattribute2 as PRODUCT_NAME,
           crd.genericnumber1 as POLICY_YEAR,
           st.genericnumber2 as COMMISSION_RATE,
           st.genericdate4 as PAID_TO_DATE,
           P_BATCH_NO as BATCH_NUMBER,
           sysdate as CREATED_DATE
      FROM CS_SALESTRANSACTION st
     inner join CS_CREDIT crd
        on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
    --and crd.genericdate2
     inner join CS_PMCREDITTRACE pct
        on crd.CREDITSEQ = pct.CREDITSEQ
     inner join CS_MEASUREMENT pm
        on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
     inner join (select distinct SOURCE_RULE_OUTPUT
                       from EXT.AIA_CB_RULES_LOOKUP
                      where RULE_TYPE = 'PM'
     AND CLAWBACK_NAME = 'SPI') pmr
        on pmr.SOURCE_RULE_OUTPUT = pm.name
    --for SM level 1 (SM_PIB_SG_SPI)
     inner join CS_PMSELFTRACE pm_sm1
        on pm.measurementseq = pm_sm1.sourcemeasurementseq
     inner join CS_MEASUREMENT sm1
        on sm1.measurementseq = pm_sm1.targetmeasurementseq
    --for SM level 2 (SM_PIB_YTD_SG_SPI)
     inner join CS_PMSELFTRACE sm1_sm2
        on sm1.measurementseq = sm1_sm2.sourcemeasurementseq
     inner join CS_MEASUREMENT sm2
        on sm2.measurementseq = sm1_sm2.targetmeasurementseq
    --for SM level 3 (SM_SPI_RATE/SM_SPI_RATE_NEW_AGT)
     inner join CS_PMSELFTRACE sm2_sm3
        on sm2.measurementseq = sm2_sm3.sourcemeasurementseq
     inner join CS_MEASUREMENT sm3
        on sm3.measurementseq = sm2_sm3.targetmeasurementseq
    --for SM level 4 (SM_SPI_CALCULATE_YTD)
     inner join CS_PMSELFTRACE sm3_sm4
        on sm3.measurementseq = sm3_sm4.sourcemeasurementseq
     inner join CS_MEASUREMENT sm4
        on sm4.measurementseq = sm3_sm4.targetmeasurementseq
    --for SM level 5 (SM_SPI_PAYMENT_QTR)
     inner join CS_PMSELFTRACE sm4_sm5
        on sm4.measurementseq = sm4_sm5.sourcemeasurementseq
     inner join CS_MEASUREMENT sm5
        on sm5.measurementseq = sm4_sm5.targetmeasurementseq
    --for SM level 6 (SM_SPI_PAYMENT_YTD)
     inner join CS_PMSELFTRACE sm5_sm6
        on sm5.measurementseq = sm5_sm6.sourcemeasurementseq
     inner join CS_MEASUREMENT sm6
        on sm6.measurementseq = sm5_sm6.targetmeasurementseq
    --for Incentive (I_SPI_SG)
     inner join CS_INCENTIVEPMTRACE inpm
        on sm5.measurementseq = inpm.MEASUREMENTSEQ
     inner join cs_incentive inc
        on inpm.incentiveseq = inc.incentiveseq
    --for deposit (D_SPI_SG)
     inner join CS_DEPOSITINCENTIVETRACE depin
        on inpm.incentiveseq = depin.incentiveseq
     inner join cs_deposit dep
        on depin.depositseq = dep.depositseq
     inner join cs_position dep_pos
        on dep.positionseq = dep_pos.ruleelementownerseq
     and dep_pos.removedate = DT_REMOVEDATE
     and dep_pos.effectivestartdate <= crd.genericdate2
     and dep_pos.effectiveenddate > crd.genericdate2
     inner join CS_CREDITTYPE ct
        on crd.CREDITTYPESEQ = ct.DATATYPESEQ
     and ct.Removedate = DT_REMOVEDATE
     inner join EXT.AIA_CB_IDENTIFY_POLICY ip
        on st.PONUMBER = ip.PONUMBER
     AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
     AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
     AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
     AND st.PRODUCTID = ip.COMPONENT_CODE
     and crd.genericattribute12 = ip.wri_agt_code
      inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
                       from EXT.AIA_CB_RULES_LOOKUP
                      where RULE_TYPE = 'DR'
     AND CLAWBACK_NAME = 'SPI') rl
        on dep.NAME = rl.SOURCE_RULE_OUTPUT
     where dep.periodseq = t_periodseq(i)
     and st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
     AND st.BUSINESSUNITMAP = 1
     --and crd.genericattribute16 not in ('RO', 'RNO')
     ;
    
    end loop;
    
    EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP for SPI' || '; row count: ' || to_char(sql%rowcount));*/
ELSEIF :P_STR_TYPE = :STR_ONGOING  /* ORIGSQL: elsif P_STR_TYPE = STR_ONGOING then */
THEN 
    SELECT
        value
    INTO
        V_NADOR_RATE
    FROM
        CS_FIXEDVALUE fv
    WHERE
        name = 'FV_NADOR_Payout_Rate'
        AND Removedate = :DT_REMOVEDATE;

    SELECT
        value
    INTO
        V_NLPI_RATE
    FROM
        CS_FIXEDVALUE fv
    WHERE
        name = 'FV_NLPI_RATE'
        AND Removedate = :DT_REMOVEDATE;

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for Ongoing 1, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO) */
    CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for Ongoing 1, '|| 'clawback type = '|| IFNULL(:P_STR_TYPE,'') ||', batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO),''));
    /*
    
    create table ext.aias_tx_temp tablespace tallydata
    as
    select  *+ PARALLEL leading(ip,st) INDEX(st EXT.AIA_salestransaction_product) *st*,
    ip.BUNAME
    ,ip.YEAR
    ,ip.QUARTER
    ,ip.WRI_DIST_CODE
    ,ip.WRI_DIST_NAME
    ,ip.WRI_DM_CODE
    ,ip.WRI_DM_NAME
    ,ip.WRI_AGY_CODE
    ,ip.WRI_AGY_NAME
    ,ip.WRI_AGY_LDR_CODE
    ,ip.WRI_AGY_LDR_NAME
    ,ip.WRI_AGT_CODE
    ,ip.WRI_AGT_NAME
    ,ip.FSC_TYPE
    ,ip.RANK
    ,ip.CLASS
    ,ip.FSC_BSC_GRADE
    ,ip.FSC_BSC_PERCENTAGE
    ,ip.INSURED_NAME
    ,ip.CONTRACT_CAT
    ,ip.LIFE_NUMBER
    ,ip.COVERAGE_NUMBER
    ,ip.RIDER_NUMBER
    ,ip.COMPONENT_CODE
    ,ip.COMPONENT_NAME
    ,ip.ISSUE_DATE
    ,ip.INCEPTION_DATE
    ,ip.RISK_COMMENCEMENT_DATE
    ,ip.FHR_DATE
    ,ip.BASE_RIDER_IND
    ,ip.TRANSACTION_DATE
    ,ip.PAYMENT_MODE
    ,ip.POLICY_CURRENCY
    ,ip.PROCESSING_PERIOD
    ,ip.CREATED_DATE
    ,ip.POLICYIDSEQ
    ,ip.SUBMITDATE
    ,p.periodseq from CS_SALESTRANSACTION st
      inner join EXT.AIA_CB_IDENTIFY_POLICY ip
         on st.PONUMBER = ip.PONUMBER
     AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
     AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
     AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
     AND st.PRODUCTID = ip.COMPONENT_CODE
      inner join CS_PERIOD p
         on st.compensationdate>=p.startdate and st.compensationdate<p.enddate and p.removedate>sysdate
     and p.calendarseq=2251799813685250
           where 1=0;
    */
    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for Ongoing 1 part1, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO) */
    CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for Ongoing 1 part1, '|| 'clawback type = '|| IFNULL(:P_STR_TYPE,'') ||', batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO),''));

    /* ORIGSQL: execute immediate 'truncate table ext.aias_tx_temp'; */
    /* ORIGSQL: truncate table ext.aias_tx_temp ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ext.aias_tx_temp';

    /* ORIGSQL: insert into ext.aias_tx_temp select st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, st.BIL(...) */
    INSERT INTO ext.aias_tx_temp
        /* ORIGSQL: select / *+ PARALLEL leading(ip,st,p) INDEX(st EXT.AIA_salestransaction_product) * / */
        SELECT   /* ORIGSQL: select st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, st.BILLTOADDRESSSEQ, st.SHIPTOA(...) */
            st.TENANTID,
            st.SALESTRANSACTIONSEQ,
            st.SALESORDERSEQ,
            st.LINENUMBER,
            st.SUBLINENUMBER,
            st.EVENTTYPESEQ,
            st.PIPELINERUNSEQ,
            st.ORIGINTYPEID,
            st.COMPENSATIONDATE,
            st.BILLTOADDRESSSEQ,
            st.SHIPTOADDRESSSEQ,
            st.OTHERTOADDRESSSEQ,
            st.ISRUNNABLE,
            st.BUSINESSUNITMAP,
            st.ACCOUNTINGDATE,
            st.PRODUCTID,
            st.PRODUCTNAME,
            st.PRODUCTDESCRIPTION,
            st.NUMBEROFUNITS,
            st.UNITVALUE,
            st.UNITTYPEFORUNITVALUE,
            st.PREADJUSTEDVALUE,
            st.UNITTYPEFORPREADJUSTEDVALUE,
            st.VALUE,
            st.UNITTYPEFORVALUE,
            st.NATIVECURRENCY,
            st.NATIVECURRENCYAMOUNT,
            st.DISCOUNTPERCENT,
            st.DISCOUNTTYPE,
            st.PAYMENTTERMS,
            st.PONUMBER,
            st.CHANNEL,
            st.ALTERNATEORDERNUMBER,
            st.DATASOURCE,
            st.REASONSEQ,
            st.COMMENTS,
            st.GENERICATTRIBUTE1,
            st.GENERICATTRIBUTE2,
            st.GENERICATTRIBUTE3,
            st.GENERICATTRIBUTE4,
            st.GENERICATTRIBUTE5,
            st.GENERICATTRIBUTE6,
            st.GENERICATTRIBUTE7,
            st.GENERICATTRIBUTE8,
            st.GENERICATTRIBUTE9,
            st.GENERICATTRIBUTE10,
            st.GENERICATTRIBUTE11,
            st.GENERICATTRIBUTE12,
            st.GENERICATTRIBUTE13,
            st.GENERICATTRIBUTE14,
            st.GENERICATTRIBUTE15,
            st.GENERICATTRIBUTE16,
            st.GENERICATTRIBUTE17,
            st.GENERICATTRIBUTE18,
            st.GENERICATTRIBUTE19,
            st.GENERICATTRIBUTE20,
            st.GENERICATTRIBUTE21,
            st.GENERICATTRIBUTE22,
            st.GENERICATTRIBUTE23,
            st.GENERICATTRIBUTE24,
            st.GENERICATTRIBUTE25,
            st.GENERICATTRIBUTE26,
            st.GENERICATTRIBUTE27,
            st.GENERICATTRIBUTE28,
            st.GENERICATTRIBUTE29,
            st.GENERICATTRIBUTE30,
            st.GENERICATTRIBUTE31,
            st.GENERICATTRIBUTE32,
            st.GENERICNUMBER1,
            st.UNITTYPEFORGENERICNUMBER1,
            st.GENERICNUMBER2,
            st.UNITTYPEFORGENERICNUMBER2,
            st.GENERICNUMBER3,
            st.UNITTYPEFORGENERICNUMBER3,
            st.GENERICNUMBER4,
            st.UNITTYPEFORGENERICNUMBER4,
            st.GENERICNUMBER5,
            st.UNITTYPEFORGENERICNUMBER5,
            st.GENERICNUMBER6,
            st.UNITTYPEFORGENERICNUMBER6,
            st.GENERICDATE1,
            st.GENERICDATE2,
            st.GENERICDATE3,
            st.GENERICDATE4,
            st.GENERICDATE5,
            st.GENERICDATE6,
            st.GENERICBOOLEAN1,
            st.GENERICBOOLEAN2,
            st.GENERICBOOLEAN3,
            st.GENERICBOOLEAN4,
            st.GENERICBOOLEAN5,
            st.GENERICBOOLEAN6,
            st.PROCESSINGUNITSEQ,
            st.MODIFICATIONDATE,
            st.UNITTYPEFORLINENUMBER,
            st.UNITTYPEFORSUBLINENUMBER,
            st.UNITTYPEFORNUMBEROFUNITS,
            st.UNITTYPEFORDISCOUNTPERCENT,
            st.UNITTYPEFORNATIVECURRENCYAMT,
            st.MODELSEQ,
            ip.BUNAME,
            ip.YEAR,
            ip.QUARTER,
            ip.WRI_DIST_CODE,
            ip.WRI_DIST_NAME,
            ip.WRI_DM_CODE,
            ip.WRI_DM_NAME,
            ip.WRI_AGY_CODE,
            ip.WRI_AGY_NAME,
            ip.WRI_AGY_LDR_CODE,
            ip.WRI_AGY_LDR_NAME,
            ip.WRI_AGT_CODE,
            ip.WRI_AGT_NAME,
            ip.FSC_TYPE,
            ip.RANK,
            ip.CLASS,
            ip.FSC_BSC_GRADE,
            ip.FSC_BSC_PERCENTAGE,
            ip.INSURED_NAME,
            ip.CONTRACT_CAT,
            ip.LIFE_NUMBER,
            ip.COVERAGE_NUMBER,
            ip.RIDER_NUMBER,
            ip.COMPONENT_CODE,
            ip.COMPONENT_NAME,
            ip.ISSUE_DATE,
            ip.INCEPTION_DATE,
            ip.RISK_COMMENCEMENT_DATE,
            ip.FHR_DATE,
            ip.BASE_RIDER_IND,
            ip.TRANSACTION_DATE,
            ip.PAYMENT_MODE,
            ip.POLICY_CURRENCY,
            ip.PROCESSING_PERIOD,
            ip.CREATED_DATE,
            ip.POLICYIDSEQ,
            ip.SUBMITDATE,
            p.periodseq,
            ip.FAOB_AGT_CODE,
            '',
            ''
        FROM
            CS_SALESTRANSACTION st
        INNER JOIN
            EXT.AIA_CB_IDENTIFY_POLICY ip
            ON st.PONUMBER = ip.PONUMBER
            AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
            AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
            AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
            AND st.PRODUCTID = ip.COMPONENT_CODE
        INNER JOIN
            CS_PERIOD p
            ON st.compensationdate >= p.startdate
            AND st.compensationdate < p.enddate
            AND p.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND p.calendarseq = :V_CALENDARSEQ
            AND p.periodtypeseq = 2814749767106561
        INNER JOIN
            cs_businessunit bu
            ON st.businessunitmap = bu.mask
        WHERE
            st.tenantid = 'AIAS'
            AND st.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ
            AND ip.buname = :STR_BUNAME_FA
            AND st.eventtypeseq <> 16607023625933358
            AND p.removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
            /* ORIGSQL: to_date('2200-01-01','yyyy-mm-dd') */
            --v8 20200928
            AND st.compensationdate BETWEEN :ONGOING_ST_DT AND :ONGOING_END_DT;

    --For AI clawback NL20180308
    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_SALESTRANSACTION' not found */

    /* ORIGSQL: insert into ext.aias_tx_temp WITH AMR AS (SELECT ROW_NUMBER() OVER (PARTITION BY t1.PONUMBER,t1.AI_PAYMENT,t1.COMPENSATIONDATE,t1.PAYEE_CODE,t1.POLICY_INCEPTION_DATE ORDER BY t1.component_CODE) AS rn, t1.(...) */
    INSERT INTO ext.aias_tx_temp
        WITH 
        AMR AS (
            SELECT   /* ORIGSQL: (select ROW_NUMBER() OVER (PARTITION BY t1.PONUMBER,t1.AI_PAYMENT,t1.COMPENSATIONDATE,t1.PAYEE_CODE,t1.POLICY_INCEPTION_DATE ORDER BY t1.component_CODE) as rn, t1.* from AI_MONTHLY_REPORT t1 where t1.(...) */
                ROW_NUMBER()OVER (PARTITION BY t1.PONUMBER,t1.AI_PAYMENT,t1.COMPENSATIONDATE,t1.PAYEE_CODE,t1.POLICY_INCEPTION_DATE ORDER BY t1.component_CODE) AS rn,
                t1.*
            FROM
                ext.AI_MONTHLY_REPORT t1
            WHERE
                t1.AI_PAYMENT <> 0
        
        )
        ,
        st AS
        (
            SELECT   /* ORIGSQL: (select ROW_NUMBER() OVER (PARTITION BY t2.PONUMBER,t2.VALUE,t2.ACCOUNTINGDATE,t2.GENERICATTRIBUTE11,t2.GENERICDATE2 ORDER BY t2.PRODUCTID) as rn, t2.* from cs_Salestransaction t2,cs_businessunit bu w(...) */
                ROW_NUMBER() OVER (PARTITION BY t2.PONUMBER,t2.VALUE,t2.ACCOUNTINGDATE,t2.GENERICATTRIBUTE11,t2.GENERICDATE2 ORDER BY t2.PRODUCTID) AS rn,
                t2.*
            FROM
                cs_Salestransaction t2,
                cs_businessunit bu
            WHERE
                t2.tenantid = 'AIAS'
                AND t2.businessunitmap = bu.mask
                -- and bu.name = STR_BUNAME_FA   --Changes done to fix not getting AGY AI records --Gopi-04072019
                AND t2.eventtypeseq = 16607023625933358
                AND t2.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ
                --v8 20200928
                AND t2.compensationdate BETWEEN :ONGOING_ST_DT AND :ONGOING_END_DT
        
        )
        ,
        IP AS
        (
            SELECT   /* ORIGSQL: (select ROW_NUMBER() OVER (PARTITION BY t3.PONUMBER,t3.WRI_AGT_CODE,t3.component_CODE,t3.inception_date,t3.risk_commencement_date ORDER BY t3.coverage_number) as rn, t3.* from EXT.AIA_CB_IDENTIFY_POLICY t(...) */
                ROW_NUMBER() OVER (PARTITION BY t3.PONUMBER,t3.WRI_AGT_CODE,t3.component_CODE,t3.inception_date,t3.risk_commencement_date ORDER BY t3.coverage_number) AS rn,
                t3.*
            FROM
                EXT.AIA_CB_IDENTIFY_POLICY t3
            WHERE
                t3.BUNAME = :STR_BUNAME_FA
        
        )
        SELECT   /* ORIGSQL: select st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, st.BILLTOADDRESSSEQ, st.SHIPTOA(...) */
            /* ORIGSQL: select / *+ PARALLEL * / */
            st.TENANTID,
            st.SALESTRANSACTIONSEQ,
            st.SALESORDERSEQ,
            st.LINENUMBER,
            st.SUBLINENUMBER,
            st.EVENTTYPESEQ,
            st.PIPELINERUNSEQ,
            st.ORIGINTYPEID,
            st.COMPENSATIONDATE,
            st.BILLTOADDRESSSEQ,
            st.SHIPTOADDRESSSEQ,
            st.OTHERTOADDRESSSEQ,
            st.ISRUNNABLE,
            st.BUSINESSUNITMAP,
            st.ACCOUNTINGDATE,
            st.PRODUCTID,
            st.PRODUCTNAME,
            st.PRODUCTDESCRIPTION,
            st.NUMBEROFUNITS,
            st.UNITVALUE,
            st.UNITTYPEFORUNITVALUE,
            st.PREADJUSTEDVALUE,
            st.UNITTYPEFORPREADJUSTEDVALUE,
            st.VALUE,
            st.UNITTYPEFORVALUE,
            st.NATIVECURRENCY,
            st.NATIVECURRENCYAMOUNT,
            st.DISCOUNTPERCENT,
            st.DISCOUNTTYPE,
            st.PAYMENTTERMS,
            st.PONUMBER,
            st.CHANNEL,
            st.ALTERNATEORDERNUMBER,
            st.DATASOURCE,
            st.REASONSEQ,
            st.COMMENTS,
            st.GENERICATTRIBUTE1,
            st.GENERICATTRIBUTE2,
            st.GENERICATTRIBUTE3,
            st.GENERICATTRIBUTE4,
            st.GENERICATTRIBUTE5,
            st.GENERICATTRIBUTE6,
            st.GENERICATTRIBUTE7,
            st.GENERICATTRIBUTE8,
            st.GENERICATTRIBUTE9,
            st.GENERICATTRIBUTE10,
            st.GENERICATTRIBUTE11,
            st.GENERICATTRIBUTE12,
            st.GENERICATTRIBUTE13,
            st.GENERICATTRIBUTE14,
            st.GENERICATTRIBUTE15,
            st.GENERICATTRIBUTE16,
            st.GENERICATTRIBUTE17,
            st.GENERICATTRIBUTE18,
            st.GENERICATTRIBUTE19,
            st.GENERICATTRIBUTE20,
            st.GENERICATTRIBUTE21,
            st.GENERICATTRIBUTE22,
            st.GENERICATTRIBUTE23,
            st.GENERICATTRIBUTE24,
            st.GENERICATTRIBUTE25,
            st.GENERICATTRIBUTE26,
            st.GENERICATTRIBUTE27,
            st.GENERICATTRIBUTE28,
            st.GENERICATTRIBUTE29,
            st.GENERICATTRIBUTE30,
            st.GENERICATTRIBUTE31,
            st.GENERICATTRIBUTE32,
            st.GENERICNUMBER1,
            st.UNITTYPEFORGENERICNUMBER1,
            st.GENERICNUMBER2,
            st.UNITTYPEFORGENERICNUMBER2,
            st.GENERICNUMBER3,
            st.UNITTYPEFORGENERICNUMBER3,
            st.GENERICNUMBER4,
            st.UNITTYPEFORGENERICNUMBER4,
            st.GENERICNUMBER5,
            st.UNITTYPEFORGENERICNUMBER5,
            st.GENERICNUMBER6,
            st.UNITTYPEFORGENERICNUMBER6,
            st.GENERICDATE1,
            st.GENERICDATE2,
            st.GENERICDATE3,
            st.GENERICDATE4,
            st.GENERICDATE5,
            st.GENERICDATE6,
            st.GENERICBOOLEAN1,
            st.GENERICBOOLEAN2,
            st.GENERICBOOLEAN3,
            st.GENERICBOOLEAN4,
            st.GENERICBOOLEAN5,
            st.GENERICBOOLEAN6,
            st.PROCESSINGUNITSEQ,
            st.MODIFICATIONDATE,
            st.UNITTYPEFORLINENUMBER,
            st.UNITTYPEFORSUBLINENUMBER,
            st.UNITTYPEFORNUMBEROFUNITS,
            st.UNITTYPEFORDISCOUNTPERCENT,
            st.UNITTYPEFORNATIVECURRENCYAMT,
            st.MODELSEQ,
            ip.BUNAME,
            ip.YEAR,
            ip.QUARTER,
            ip.WRI_DIST_CODE,
            ip.WRI_DIST_NAME,
            ip.WRI_DM_CODE,
            ip.WRI_DM_NAME,
            ip.WRI_AGY_CODE,
            ip.WRI_AGY_NAME,
            ip.WRI_AGY_LDR_CODE,
            ip.WRI_AGY_LDR_NAME,
            ip.WRI_AGT_CODE,
            ip.WRI_AGT_NAME,
            ip.FSC_TYPE,
            ip.RANK,
            ip.CLASS,
            ip.FSC_BSC_GRADE,
            ip.FSC_BSC_PERCENTAGE,
            ip.INSURED_NAME,
            ip.CONTRACT_CAT,
            ip.LIFE_NUMBER,
            ip.COVERAGE_NUMBER,
            ip.RIDER_NUMBER,
            ip.COMPONENT_CODE,
            ip.COMPONENT_NAME,
            ip.ISSUE_DATE,
            ip.INCEPTION_DATE,
            ip.RISK_COMMENCEMENT_DATE,
            ip.FHR_DATE,
            ip.BASE_RIDER_IND,
            ip.TRANSACTION_DATE,
            ip.PAYMENT_MODE,
            ip.POLICY_CURRENCY,
            ip.PROCESSING_PERIOD,
            ip.CREATED_DATE,
            ip.POLICYIDSEQ,
            ip.SUBMITDATE,
            p.periodseq,
            ip.FAOB_AGT_CODE,
            '',
            ''
        FROM
            st
        INNER JOIN
            AMR
            ON st.PONUMBER = AMR.PONUMBER
            AND st.VALUE = AMR.AI_PAYMENT
            AND st.ACCOUNTINGDATE = AMR.COMPENSATIONDATE
            --AND st.GENERICATTRIBUTE11 = AMR.PAYEE_CODE
            AND (st.GENERICATTRIBUTE11 = AMR.NEW_AGENT_CD
            OR st.GENERICATTRIBUTE11 = AMR.OLD_AGENT_CD) ----Changes done to fix not getting AGY AI records --Gopi-04072019
            AND st.GENERICDATE2 = AMR.POLICY_INCEPTION_DATE
            AND st.rn = AMR.rn
        INNER JOIN
            ip
            ON IP.BUNAME = :STR_BUNAME_FA
            AND AMR.PONUMBER = IP.PONUMBER
            /*AND ST.GENERICATTRIBUTE29         = IP.LIFE_NUMBER
            AND ST.GENERICATTRIBUTE30         = IP.COVERAGE_NUMBER
            AND ST.GENERICATTRIBUTE31         = IP.RIDER_NUMBER*/
            -- and AMR.PAYEE_CODE = IP.WRI_AGT_CODE
            AND (AMR.NEW_AGENT_CD = IP.WRI_AGT_CODE
            OR AMR.OLD_AGENT_CD = IP.WRI_AGT_CODE) --Changes done to fix not getting AGY AI records --Gopi-04072019
            AND AMR.component_CODE = ip.component_CODE
            AND AMR.policy_inception_date = ip.inception_date
            AND AMR.risk_commencement_date = ip.risk_commencement_date
            AND AMR.rn = IP.rn
        INNER JOIN
            CS_PERIOD p
            ON st.compensationdate >= p.startdate
            AND st.compensationdate < p.enddate
            AND p.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND p.calendarseq = :V_CALENDARSEQ
            AND p.periodtypeseq = 2814749767106561
        WHERE
            p.removedate = to_date('2200-01-01','yyyy-mm-dd');--Cosimo
    /* ORIGSQL: to_date('2200-01-01','yyyy-mm-dd') */

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for Ongoing 1 part 1b, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO || ' ' ||SQL%ROWCOUNT) */
    CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for Ongoing 1 part 1b, '|| 'clawback type = '|| IFNULL(:P_STR_TYPE,'') ||', batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO),'') || ' '||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: insert into ext.aias_tx_temp select * from EXT.AIA_CB_COMP_ONG_STGPAST_TX_FA; */
    INSERT INTO ext.aias_tx_temp
        SELECT   /* ORIGSQL: select * from EXT.AIA_CB_COMP_ONG_STGPAST_TX_FA; */
            *
        FROM
            EXT.AIA_CB_COMP_ONG_STGPAST_TX_FA;

    /* ORIGSQL: EXT.Log('added EXT.AIA_CB_COMP_ONG_STGPAST_TX_FA into ext.aias_tx_temp for Ongoing 1 part 1b, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO || ' ' ||SQL%ROWCOUNT) */
    CALL EXT.Log('added EXT.AIA_CB_COMP_ONG_STGPAST_TX_FA into ext.aias_tx_temp for Ongoing 1 part 1b, '|| 'clawback type = '|| IFNULL(:P_STR_TYPE,'') ||', batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO),'') || ' '||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: execute immediate 'truncate table ext.aias_tx_temp15'; */
    /* ORIGSQL: truncate table ext.aias_tx_temp15 ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ext.aias_tx_temp15';

    --drop table ext.aias_tx_temp2

    /* ORIGSQL: insert into ext.aias_tx_temp15 select ip.TENANTID, ip.SALESTRANSACTIONSEQ, ip.SALESORDERSEQ, ip.LINENUMBER, ip.SUBLINENUMBER, ip.EVENTTYPESEQ, ip.PIPELINERUNSEQ, ip.ORIGINTYPEID, ip.COMPENSATIONDATE, ip.B(...) */
    INSERT INTO ext.aias_tx_temp15
        /* ORIGSQL: select / *+ INDEX(crd CS_CREDIT_TRANSACTIONSEQ) * / */
        SELECT   /* ORIGSQL: select ip.TENANTID, ip.SALESTRANSACTIONSEQ, ip.SALESORDERSEQ, ip.LINENUMBER, ip.SUBLINENUMBER, ip.EVENTTYPESEQ, ip.PIPELINERUNSEQ, ip.ORIGINTYPEID, ip.COMPENSATIONDATE, ip.BILLTOADDRESSSEQ, ip.SHIPTOA(...) */
            ip.TENANTID,
            ip.SALESTRANSACTIONSEQ,
            ip.SALESORDERSEQ,
            ip.LINENUMBER,
            ip.SUBLINENUMBER,
            ip.EVENTTYPESEQ,
            ip.PIPELINERUNSEQ,
            ip.ORIGINTYPEID,
            ip.COMPENSATIONDATE,
            ip.BILLTOADDRESSSEQ,
            ip.SHIPTOADDRESSSEQ,
            ip.OTHERTOADDRESSSEQ,
            ip.ISRUNNABLE,
            ip.BUSINESSUNITMAP,
            ip.ACCOUNTINGDATE,
            ip.PRODUCTID,
            ip.PRODUCTNAME,
            ip.PRODUCTDESCRIPTION,
            ip.NUMBEROFUNITS,
            ip.UNITVALUE,
            ip.UNITTYPEFORUNITVALUE,
            ip.PREADJUSTEDVALUE,
            ip.UNITTYPEFORPREADJUSTEDVALUE,
            ip.VALUE,
            ip.UNITTYPEFORVALUE,
            ip.NATIVECURRENCY,
            ip.NATIVECURRENCYAMOUNT,
            ip.DISCOUNTPERCENT,
            ip.DISCOUNTTYPE,
            ip.PAYMENTTERMS,
            ip.PONUMBER,
            ip.CHANNEL,
            ip.ALTERNATEORDERNUMBER,
            ip.DATASOURCE,
            ip.REASONSEQ,
            ip.COMMENTS,
            ip.GENERICATTRIBUTE1,
            ip.GENERICATTRIBUTE2,
            ip.GENERICATTRIBUTE3,
            ip.GENERICATTRIBUTE4,
            ip.GENERICATTRIBUTE5,
            ip.GENERICATTRIBUTE6,
            ip.GENERICATTRIBUTE7,
            ip.GENERICATTRIBUTE8,
            ip.GENERICATTRIBUTE9,
            ip.GENERICATTRIBUTE10,
            ip.GENERICATTRIBUTE11,
            ip.GENERICATTRIBUTE12,
            ip.GENERICATTRIBUTE13,
            ip.GENERICATTRIBUTE14,
            ip.GENERICATTRIBUTE15,
            ip.GENERICATTRIBUTE16,
            ip.GENERICATTRIBUTE17,
            ip.GENERICATTRIBUTE18,
            ip.GENERICATTRIBUTE19,
            ip.GENERICATTRIBUTE20,
            ip.GENERICATTRIBUTE21,
            ip.GENERICATTRIBUTE22,
            ip.GENERICATTRIBUTE23,
            ip.GENERICATTRIBUTE24,
            ip.GENERICATTRIBUTE25,
            ip.GENERICATTRIBUTE26,
            ip.GENERICATTRIBUTE27,
            ip.GENERICATTRIBUTE28,
            ip.GENERICATTRIBUTE29,
            ip.GENERICATTRIBUTE30,
            ip.GENERICATTRIBUTE31,
            ip.GENERICATTRIBUTE32,
            ip.GENERICNUMBER1,
            ip.UNITTYPEFORGENERICNUMBER1,
            ip.GENERICNUMBER2,
            ip.UNITTYPEFORGENERICNUMBER2,
            ip.GENERICNUMBER3,
            ip.UNITTYPEFORGENERICNUMBER3,
            ip.GENERICNUMBER4,
            ip.UNITTYPEFORGENERICNUMBER4,
            ip.GENERICNUMBER5,
            ip.UNITTYPEFORGENERICNUMBER5,
            ip.GENERICNUMBER6,
            ip.UNITTYPEFORGENERICNUMBER6,
            ip.GENERICDATE1,
            ip.GENERICDATE2,
            ip.GENERICDATE3,
            ip.GENERICDATE4,
            ip.GENERICDATE5,
            ip.GENERICDATE6,
            ip.GENERICBOOLEAN1,
            ip.GENERICBOOLEAN2,
            ip.GENERICBOOLEAN3,
            ip.GENERICBOOLEAN4,
            ip.GENERICBOOLEAN5,
            ip.GENERICBOOLEAN6,
            ip.PROCESSINGUNITSEQ,
            ip.MODIFICATIONDATE,
            ip.UNITTYPEFORLINENUMBER,
            ip.UNITTYPEFORSUBLINENUMBER,
            ip.UNITTYPEFORNUMBEROFUNITS,
            ip.UNITTYPEFORDISCOUNTPERCENT,
            ip.UNITTYPEFORNATIVECURRENCYAMT,
            ip.MODELSEQ,
            ip.BUNAME,
            ip.YEAR,
            ip.QUARTER,
            ip.WRI_DIST_CODE,
            ip.WRI_DIST_NAME,
            ip.WRI_DM_CODE,
            ip.WRI_DM_NAME,
            ip.WRI_AGY_CODE,
            ip.WRI_AGY_NAME,
            ip.WRI_AGY_LDR_CODE,
            ip.WRI_AGY_LDR_NAME,
            ip.WRI_AGT_CODE,
            ip.WRI_AGT_NAME,
            ip.FSC_TYPE,
            ip.RANK,
            ip.CLASS,
            ip.FSC_BSC_GRADE,
            ip.FSC_BSC_PERCENTAGE,
            ip.INSURED_NAME,
            ip.CONTRACT_CAT,
            ip.LIFE_NUMBER,
            ip.COVERAGE_NUMBER,
            ip.RIDER_NUMBER,
            ip.COMPONENT_CODE,
            ip.COMPONENT_NAME,
            ip.ISSUE_DATE,
            ip.INCEPTION_DATE,
            ip.RISK_COMMENCEMENT_DATE,
            ip.FHR_DATE,
            ip.BASE_RIDER_IND,
            ip.TRANSACTION_DATE,
            ip.PAYMENT_MODE,
            ip.POLICY_CURRENCY,
            ip.PROCESSING_PERIOD,
            ip.CREATED_DATE,
            ip.POLICYIDSEQ,
            ip.SUBMITDATE,
            ip.PERIODSEQ,
            crd.name,
            crd.creditseq,
            crd.genericdate2 AS crdGd2,
            crd.genericattribute13 AS crdga13,
            crd.genericattribute14 AS crdga14,
            crd.name AS crdName,
            NULL AS measurementseq,
            ct.credittypeid,
            cbp.cb_enddate AS cbpenddate,
            crd.genericattribute12 AS CRDGA12,
            crd.compensationdate AS CRDCOMPDate,
            crd.value AS crdvalue,
            crd.positionseq AS crdpositionseq,
            crd.ruleseq AS crdRuleSeq,
            crd.periodseq AS Crdperiodseq,
            crd.genericattribute2 AS crdGA2,
            crd.genericnumber1 AS crdgn1/* --, null as contributionvalue */,
            '',
            ''
        FROM
            ext.aias_tx_temp ip
        INNER JOIN
            CS_CREDIT crd
            ON ip.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
            AND (crd.genericattribute12 = ip.wri_agt_code
            OR ip.FAOB_AGT_CODE = crd.genericattribute12)
            AND crd.periodseq = ip.periodseq
            --inner join CS_PMCREDITTRACE pct
            -- on crd.CREDITSEQ = pct.CREDITSEQ
            -- and pct.sourceperiodseq=2533274790398934
        INNER JOIN
            CS_CREDITTYPE ct
            ON crd.CREDITTYPESEQ = ct.DATATYPESEQ
            AND ct.tenantid = 'AIAS'
            AND ct.Removedate = '1-jan-2200'
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select distinct cb_quarter_name, cb_startdate, cb_enddate from EXT.AIA_cb_period where cb_name = STR_CB_NAME AND buname = STR_BUNAME_FA) */
                    DISTINCT
                    cb_quarter_name,
                    cb_startdate,
                    cb_enddate
                FROM
                    EXT.AIA_cb_period
                WHERE
                    cb_name = :STR_CB_NAME
                    AND buname = :STR_BUNAME_FA
            ) AS cbp
            ON IFNULL(ip.quarter,'') || ' '|| IFNULL(ip.year,'') = cbp.cb_quarter_name;--  where    crd.tenantid='AIAS' and   crd.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
    -- AND pct.tenantid='AIAS' and   pct.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ 
    /* ORIGSQL: EXT.Log('insert into ext.aias_tx_temp15 FA for Ongoing 1 part 1c, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO || ' ' ||SQL%ROWCOUNT) */
    CALL EXT.Log('insert into ext.aias_tx_temp15 FA for Ongoing 1 part 1c, '|| 'clawback type = '|| IFNULL(:P_STR_TYPE,'') ||', batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO),'') || ' '||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    --v9 tuning

    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_comls_period'; */
    /* ORIGSQL: truncate table EXT.AIA_tmp_comls_period ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_period';

    /* ORIGSQL: insert into EXT.AIA_tmp_comls_period select periodseq from cs_period where startdate >= trunc(ONGOING_END_DT,'YYYY') and enddate <= ONGOING_END_DT + 1 and periodtypeseq = V_periodtype_month_seq and remove(...) */
    INSERT INTO EXT.AIA_tmp_comls_period
        SELECT   /* ORIGSQL: select periodseq from cs_period where startdate >= trunc(ONGOING_END_DT,'YYYY') and enddate <= ONGOING_END_DT + 1 and periodtypeseq = V_periodtype_month_seq and removedate = DT_REMOVEDATE; */
            periodseq
        FROM
            cs_period
        WHERE
            startdate >= to_date(:ONGOING_END_DT,'YYYY')  /* ORIGSQL: trunc(ONGOING_END_DT,'YYYY') */
            -- and add_months((ONGOING_END_DT+1),-9)
            AND enddate <= TO_DATE(ADD_SECONDS(:ONGOING_END_DT,(86400*1)))   /* ORIGSQL: ONGOING_END_DT + 1 */
            AND periodtypeseq = :V_periodtype_month_seq
            AND removedate = :DT_REMOVEDATE;

    --month

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_tmp_comls_period for Ongoing 1 part 1c - comls_period, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO || ' ' ||SQL%ROWCOUNT) */
    CALL EXT.Log('insert into EXT.AIA_tmp_comls_period for Ongoing 1 part 1c - comls_period, '|| 'clawback type = '|| IFNULL(:P_STR_TYPE,'') ||', batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO),'') || ' '||::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: execute immediate 'truncate table ext.aias_tx_temp2'; */
    /* ORIGSQL: truncate table ext.aias_tx_temp2 ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ext.aias_tx_temp2';

    FOR v_comls_period AS dbmtk_cursor_22755
    DO
        /* ORIGSQL: insert into ext.aias_tx_temp2 select ip.tenantid, salestransactionseq, salesorderseq, linenumber, sublinenumber, eventtypeseq, ip.pipelinerunseq, origintypeid, compensationdate, billtoaddressseq, shiptoad(...) */
        INSERT INTO ext.aias_tx_temp2
            --create table ext.aias_tx_temp2 as
            SELECT   /* ORIGSQL: select ip.tenantid, salestransactionseq, salesorderseq, linenumber, sublinenumber, eventtypeseq, ip.pipelinerunseq, origintypeid, compensationdate, billtoaddressseq, shiptoaddressseq, othertoaddressse(...) */
                ip.tenantid,
                salestransactionseq,
                salesorderseq,
                linenumber,
                sublinenumber,
                eventtypeseq,
                ip.pipelinerunseq,
                origintypeid,
                compensationdate,
                billtoaddressseq,
                shiptoaddressseq,
                othertoaddressseq,
                isrunnable,
                ip.businessunitmap,
                accountingdate,
                productid,
                productname,
                productdescription,
                numberofunits,
                unitvalue,
                unittypeforunitvalue,
                preadjustedvalue,
                unittypeforpreadjustedvalue,
                value,
                unittypeforvalue,
                nativecurrency,
                nativecurrencyamount,
                discountpercent,
                discounttype,
                paymentterms,
                ponumber,
                channel,
                alternateordernumber,
                datasource,
                reasonseq,
                comments,
                genericattribute1,
                genericattribute2,
                genericattribute3,
                genericattribute4,
                genericattribute5,
                genericattribute6,
                genericattribute7,
                genericattribute8,
                genericattribute9,
                genericattribute10,
                genericattribute11,
                genericattribute12,
                genericattribute13,
                genericattribute14,
                genericattribute15,
                genericattribute16,
                genericattribute17,
                genericattribute18,
                genericattribute19,
                genericattribute20,
                genericattribute21,
                genericattribute22,
                genericattribute23,
                genericattribute24,
                genericattribute25,
                genericattribute26,
                genericattribute27,
                genericattribute28,
                genericattribute29,
                genericattribute30,
                genericattribute31,
                genericattribute32,
                genericnumber1,
                unittypeforgenericnumber1,
                genericnumber2,
                unittypeforgenericnumber2,
                genericnumber3,
                unittypeforgenericnumber3,
                genericnumber4,
                unittypeforgenericnumber4,
                genericnumber5,
                unittypeforgenericnumber5,
                genericnumber6,
                unittypeforgenericnumber6,
                genericdate1,
                genericdate2,
                genericdate3,
                genericdate4,
                genericdate5,
                genericdate6,
                genericboolean1,
                genericboolean2,
                genericboolean3,
                genericboolean4,
                genericboolean5,
                genericboolean6,
                ip.processingunitseq,
                modificationdate,
                unittypeforlinenumber,
                unittypeforsublinenumber,
                unittypefornumberofunits,
                unittypefordiscountpercent,
                unittypefornativecurrencyamt,
                ip.modelseq,
                buname,
                year,
                quarter,
                wri_dist_code,
                wri_dist_name,
                wri_dm_code,
                wri_dm_name,
                wri_agy_code,
                wri_agy_name,
                wri_agy_ldr_code,
                wri_agy_ldr_name,
                wri_agt_code,
                wri_agt_name,
                fsc_type,
                rank,
                class,
                fsc_bsc_grade,
                fsc_bsc_percentage,
                insured_name,
                contract_cat,
                life_number,
                coverage_number,
                rider_number,
                component_code,
                component_name,
                issue_date,
                inception_date,
                risk_commencement_date,
                fhr_date,
                base_rider_ind,
                transaction_date,
                payment_mode,
                policy_currency,
                processing_period,
                created_date,
                policyidseq,
                submitdate,
                periodseq,
                name,
                ip.creditseq,
                crdgd2,
                crdga13,
                crdga14,
                crdname,
                pct.measurementseq,
                credittypeid,
                cbpenddate,
                crdga12,
                crdcompdate,
                crdvalue,
                crdpositionseq,
                crdruleseq,
                crdperiodseq,
                crdga2,
                crdgn1,
                pct.contributionvalue,
                '',
                ''
            FROM
                ext.aias_tx_temp15 ip
            INNER JOIN
                CS_PMCREDITTRACE pct
                ON ip.CREDITSEQ = pct.CREDITSEQ
            WHERE
                pct.tenantid = 'AIAS'
                AND pct.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ
                AND pct.targetperiodseq = :v_comls_period.periodseq;

        /* ORIGSQL: commit; */
        COMMIT;
    END FOR;  /* ORIGSQL: end loop; */

    -- v9 tuning end

    --for on-going compensation trace forward
    --insert /*+ APPEND */ into EXT.AIA_CB_TRACE_FORWARD_COMP
    -- select  /*+ LEADING(aias_tx_temp,crd,pct,pm) PARALLEL */ STR_BUNAME as BUNAME,
    /*      ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
        ip.ponumber as POLICY_NUMBER,
        ip.policyidseq as POLICYIDSEQ,
        pm.positionseq as PAYEE_SEQ,
        substr(pm_pos.name, 4) as PAYEE_CODE,
        crd.genericattribute12 as PAYOR_CODE,
        ip.life_number as LIFE_NUMBER,
        ip.coverage_number as COVERAGE_NUMBER,
        ip.rider_number as RIDER_NUMBER,
        ip.component_code as COMPONENT_CODE,
        ip.component_name as COMPONENT_NAME,
        ip.base_rider_ind as BASE_RIDER_IND,
        crd.compensationdate as TRANSACTION_DATE,
        substr(ONGOING_PERIOD,1,3)||'-'||substr(ONGOING_PERIOD,-4) as PROCESSING_PERIOD,
        STR_ONGOING as CLAWBACK_TYPE,
         rl.CLAWBACK_NAME       as CLAWBACK_NAME,
          STR_CB_NAME as CLAWBACK_METHOD,
        ct.credittypeid as CREDITTYPE,
        crd.creditseq as CREDITSEQ,
        crd.name as CREDIT_NAME,
        crd.value as CREDIT_VALUE,
         crd.positionseq as crd_positionseq,
        ip.genericdate2 as crd_genericdate2,
        crd.ruleseq as crd_ruleseq,
        pm.measurementseq as PM_SEQ,
        pm.name as PM_NAME,
        case rl.CLAWBACK_NAME
         when 'NLPI_ONG' then pct.contributionvalue*V_NLPI_RATE
         when 'NADOR' then pct.contributionvalue*V_NADOR_RATE
         else
          pct.contributionvalue
        end as PM_CONTRIBUTION_VALUE,
        case rl.CLAWBACK_NAME
         when 'FYO_ONG' then fyo_rate.value
         when 'RYO_ONG' then ryo_rate.value
         when 'FSM_RYO_ONG' then ryo_rate.value
         when 'NLPI_ONG' then V_NLPI_RATE
         when 'NADOR' then V_NADOR_RATE
       else 1
         end as PM_RATE,
      --1 as PM_RATE,
        '' as DEPOSITSEQ,
        '' as DEPOSIT_NAME,
        '' as DEPOSIT_VALUE,
        crd.periodseq as PERIODSEQ,
        ip.salestransactionseq as SALESTRANSACTIONSEQ,
        crd.genericattribute2 as PRODUCT_NAME,
        crd.genericnumber1 as POLICY_YEAR,
        ip.genericnumber2      as COMMISSION_RATE,
        ip.genericdate4 as PAID_TO_DATE,
        P_BATCH_NO as BATCH_NUMBER,
        sysdate as CREATED_DATE
     FROM ext.aias_tx_temp ip
    inner join CS_CREDIT crd
     on ip.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
    and crd.genericattribute12 = ip.wri_agt_code
    and crd.periodseq = ip.periodseq
    inner join CS_PMCREDITTRACE pct
     on crd.CREDITSEQ = pct.CREDITSEQ
    -- and pct.targetperiodseq= ip.periodseq
    inner join CS_MEASUREMENT pm
     on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
      -- and pct.targetperiodseq= pm.periodseq
    inner join CS_POSITION pm_pos
     on pm.positionseq = pm_pos.ruleelementownerseq
    and pm_pos.removedate = DT_REMOVEDATE
    and pm_pos.effectivestartdate <= crd.genericdate2
    and pm_pos.effectiveenddate > crd.genericdate2
    inner join CS_CREDITTYPE ct
     on crd.CREDITTYPESEQ = ct.DATATYPESEQ
    and ct.Removedate = DT_REMOVEDATE
    inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
                    from EXT.AIA_CB_RULES_LOOKUP
                   where RULE_TYPE = 'PM'
         AND CLAWBACK_NAME IN ('FYO_ONG','RYO_ONG','FSM_RYO_ONG','NLPI_ONG','NADOR')) rl
     on pm.NAME = rl.SOURCE_RULE_OUTPUT
     and pm.periodseq = ts_periodseq
    inner join  (select distinct
                          cb_quarter_name,
                          cb_startdate,
                          cb_enddate
                     from EXT.AIA_cb_period
                where cb_name = STR_CB_NAME) cbp
     on ip.quarter || ' ' || ip.year = cbp.cb_quarter_name
    inner join cs_position dep_pos
     on pm.positionseq = dep_pos.ruleelementownerseq
    and dep_pos.removedate = DT_REMOVEDATE
    and dep_pos.effectivestartdate <= crd.genericdate2
    and dep_pos.effectiveenddate > crd.genericdate2
      --for lookup the receiver info.
    inner join cs_title dep_title
     on dep_pos.titleseq = dep_title.ruleelementownerseq
    and dep_title.removedate = DT_REMOVEDATE
    and dep_title.effectivestartdate <= crd.genericdate2
    and dep_title.effectiveenddate > crd.genericdate2
     left join ext.vw_lt_fyo_rate fyo_rate
     on fyo_rate.Contributor_Leader_title = crd.genericattribute13 --payor agency leader title
     and fyo_rate.PIB_TYPE = ext.fn_fyo_pib_type(crd.genericattribute13, crd.genericattribute14, crd.name)
     and fyo_rate.Receiver_title = dep_title.name
     and rl.CLAWBACK_NAME = 'FYO'
     left join ext.vw_lt_ryo_life_rate ryo_rate
     on ryo_rate.Contributor_Leader_title = crd.genericattribute13 --payor agency leader title
     and ryo_rate.PIB_TYPE = ext.fn_fyo_pib_type(crd.genericattribute13, crd.genericattribute14, crd.name)
     and ryo_rate.Receiver_title = dep_title.name
     and rl.CLAWBACK_NAME in ( 'RYO','FSM_RYO')
    WHERE to_date(P_STR_CYCLEDATE,STR_DATE_FORMAT_TYPE) > cbp.cb_enddate
    AND crd.tenantid='AIAS' and   crd.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
    AND crd.tenantid='AIAS' and   crd.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
    AND pm.tenantid='AIAS' and   pm.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
    AND pct.tenantid='AIAS' and   pct.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ    ;
    */
    /* ORIGSQL: insert / *+ APPEND * / */

    /* ORIGSQL: insert into EXT.AIA_CB_TRACE_FORWARD_COMP select STR_BUNAME_FA as BUNAME, ip.quarter || ' '|| ip.year as CALCULATION_PERIOD, ip.ponumber as POLICY_NUMBER, ip.policyidseq as POLICYIDSEQ, pm.positionseq as (...) */
    INSERT INTO EXT.AIA_CB_TRACE_FORWARD_COMP
        /* ORIGSQL: select / *+ LEADING(aias_tx_temp2,pm) PARALLEL * / */
        SELECT   /* ORIGSQL: select STR_BUNAME_FA as BUNAME, ip.quarter || ' '|| ip.year as CALCULATION_PERIOD, ip.ponumber as POLICY_NUMBER, ip.policyidseq as POLICYIDSEQ, pm.positionseq as PAYEE_SEQ, substr(pm_pos.name, 4) as P(...) */
            :STR_BUNAME_FA AS BUNAME,
            IFNULL(ip.quarter,'') || ' '|| IFNULL(ip.year,'') AS CALCULATION_PERIOD,
            ip.ponumber AS POLICY_NUMBER,
            ip.policyidseq AS POLICYIDSEQ,
            pm.positionseq AS PAYEE_SEQ,
            SUBSTRING(pm_pos.name,4) AS PAYEE_CODE,  /* ORIGSQL: substr(pm_pos.name, 4) */
            /* --        crdga12 as PAYOR_CODE, --Moified to get NADOR old agent data -Gopinath 10122019 */
            (
                CASE rl.CLAWBACK_NAME
                    WHEN 'NADOR_FA_2.1'
                    THEN IP.WRI_AGT_CODE
                    ELSE crdga12
                END
            ) AS PAYOR_CODE,
            ip.life_number AS LIFE_NUMBER,
            ip.coverage_number AS COVERAGE_NUMBER,
            ip.rider_number AS RIDER_NUMBER,
            ip.component_code AS COMPONENT_CODE,
            ip.component_name AS COMPONENT_NAME,
            ip.base_rider_ind AS BASE_RIDER_IND,
            crdcompdate AS TRANSACTION_DATE,
            IFNULL(SUBSTRING(:ONGOING_PERIOD,1,3),'') ||'-'||IFNULL(substring(:ONGOING_PERIOD,-4),'') AS PROCESSING_PERIOD,  /* ORIGSQL: substr(ONGOING_PERIOD,1,3) */
                                                                                                                                                 /* ORIGSQL: substr(ONGOING_PERIOD,-4) */
            :STR_ONGOING AS CLAWBACK_TYPE,
            rl.CLAWBACK_NAME AS CLAWBACK_NAME,
            :STR_CB_NAME AS CLAWBACK_METHOD,
            credittypeid AS CREDITTYPE,
            creditseq AS CREDITSEQ,
            crdname AS CREDIT_NAME,
            crdvalue AS CREDIT_VALUE,
            crdpositionseq AS crd_positionseq,
            crdgd2 AS crd_genericdate2,
            crdRuleSeq AS crd_ruleseq,
            pm.measurementseq AS PM_SEQ,
            pm.name AS PM_NAME,
            CASE rl.CLAWBACK_NAME
                WHEN 'NLPI_ONG'
                THEN ip.contributionvalue*:V_NLPI_RATE
                WHEN 'NADOR_FA_2.1'
                THEN ip.contributionvalue*:V_NADOR_RATE
                ELSE ip.contributionvalue
            END
            AS PM_CONTRIBUTION_VALUE,
            CASE rl.CLAWBACK_NAME
                WHEN 'FYO_FA'
                THEN fyo_rate.value
                /* --Added by Suresh */
                WHEN 'FYO_FA_ONG'
                THEN new_fyo_rate.value
                WHEN 'RYO_FA_ONG'
                THEN ryo_rate.value
                WHEN 'RYO_FA'
                THEN new_ryo_rate.value
                /* --         when 'FSM_RYO_ONG' then ryo_rate.value */
                /* --         when 'NLPI_ONG' then V_NLPI_RATE */
                WHEN 'NADOR_FA_2.1'
                THEN :V_NADOR_RATE
                /* --added by Suresh */
                ELSE 1
            END
            AS PM_RATE,
            /* --1 as PM_RATE, */
            '' AS DEPOSITSEQ,
            '' AS DEPOSIT_NAME,
            '' AS DEPOSIT_VALUE,
            crdperiodseq AS PERIODSEQ,
            ip.salestransactionseq AS SALESTRANSACTIONSEQ,
            crdga2 AS PRODUCT_NAME,
            crdgn1 AS POLICY_YEAR,
            ip.genericnumber2 AS COMMISSION_RATE,
            ip.genericdate4 AS PAID_TO_DATE,
            :P_BATCH_NO AS BATCH_NUMBER,
            CURRENT_TIMESTAMP AS CREATED_DATE,  /* ORIGSQL: sysdate */
            '',
            '',
            NULL AS deposit_period
        FROM
            ext.aias_tx_temp2 ip
        INNER JOIN
            CS_MEASUREMENT pm
            ON pm.MEASUREMENTSEQ = ip.MEASUREMENTSEQ
            AND pm.tenantid = 'AIAS'
            AND pm.processingunitseq = :V_PROCESSINGUNITSEQ
        INNER JOIN
            CS_POSITION pm_pos
            ON pm.positionseq = pm_pos.ruleelementownerseq
            AND pm_pos.removedate = '1-jan-2200'
            AND pm_pos.effectivestartdate <= ip.crdGD2
            AND pm_pos.effectiveenddate > ip.crdGD2
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME from EXT.AIA_CB_RULES_LOOKUP where RULE_TYPE = 'PM' AND CLAWBACK_NAME IN ('FYO_FA_ONG','RYO_FA_ONG','NLPI_ONG','FA_FYO_ONG_2.1','FA_RYO_ONG_2.1','NADOR_F(...) */
                    DISTINCT
                    SOURCE_RULE_OUTPUT,
                    CLAWBACK_NAME
                FROM
                    EXT.AIA_CB_RULES_LOOKUP
                WHERE
                    RULE_TYPE = 'PM'
                    --Changed by Suresh
                    --Add AI NL20180308
                    -- AND CLAWBACK_NAME IN ('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION')) rl
                AND CLAWBACK_NAME IN ('FYO_FA_ONG','RYO_FA_ONG','NLPI_ONG'
                    --verstion 13 Harm_Phase4 Start
                    ,'FA_FYO_ONG_2.1'
                    ,'FA_RYO_ONG_2.1'
                    ,'NADOR_FA_2.1'
                    ,'FA_AI_ONG_2.1'
                    --verstion 13 Harm_Phase4 End
                )
        ) AS rl
        --('FYO_ONG','NEW_FYO_ONG','RYO_ONG','NEW_RYO_ONG','FSM_RYO_ONG','NLPI_ONG','NADOR','AI_ONG')) rl
    --end by Suresh
    ON pm.NAME = rl.SOURCE_RULE_OUTPUT
    AND pm.periodseq = :ts_periodseq
INNER JOIN
    cs_position dep_pos
    ON pm.positionseq = dep_pos.ruleelementownerseq
    AND dep_pos.removedate = '1-jan-2200'
    AND dep_pos.effectivestartdate <= crdGD2
    AND dep_pos.effectiveenddate > crdGD2
    --for lookup the receiver info.
INNER JOIN
    cs_title dep_title
    ON dep_pos.titleseq = dep_title.ruleelementownerseq
    AND dep_title.removedate = '1-jan-2200'
    AND dep_title.effectivestartdate <= crdGD2
    AND dep_title.effectiveenddate > crdGD2
LEFT OUTER JOIN
    ext.vw_lt_fyo_rate fyo_rate
    ON fyo_rate.Contributor_Leader_title = crdGA13 --payor agency leader title
    AND fyo_rate.PIB_TYPE = ext.fn_fyo_pib_type(crdGA13, crdGA14, crdname)
    AND fyo_rate.Receiver_title = dep_title.name
    --Changed by Suresh
    AND rl.CLAWBACK_NAME = 'FYO_FA_ONG'
    --end by Suresh
LEFT OUTER JOIN
    ext.vw_lt_ryo_life_rate ryo_rate
    ON ryo_rate.Contributor_Leader_title = crdGA13 --payor agency leader title
    AND ryo_rate.PIB_TYPE = ext.fn_fyo_pib_type(crdGA13, crdGA14, crdname)
    AND ryo_rate.Receiver_title = dep_title.name
    --Added by Suresh
    AND rl.CLAWBACK_NAME IN ('RYO_FA_ONG','FSM_RYO_ONG_FA') --version 11
    --for lookup PM rate for New FYO
LEFT OUTER JOIN
    ext.vw_lt_new_fyo_rate new_fyo_rate
    ON new_fyo_rate.Contributor_Leader_title = crdGA13 --payor agency leader title
    AND new_fyo_rate.PIB_TYPE = ext.fn_fyo_pib_type(crdGA13, crdGA14, crdname)
    AND new_fyo_rate.Receiver_title = dep_title.name
    AND rl.CLAWBACK_NAME = 'NEW_FYO_ONG_FA'
    --for lookup PM rate for New RYO
LEFT OUTER JOIN
    ext.vw_LT_NEW_RYO_LIFE_RATE new_ryo_rate
    ON new_ryo_rate.Contributor_Leader_title = crdGA13 --payor agency leader title
    AND new_ryo_rate.PIB_TYPE = ext.fn_fyo_pib_type(crdGA13, crdGA14, crdname)
    AND new_ryo_rate.Receiver_title = dep_title.name
    AND rl.CLAWBACK_NAME = 'NEW_RYO_ONG_FA'
    --End by Suresh
WHERE
    TO_DATE(:P_STR_CYCLEDATE,:STR_DATE_FORMAT_TYPE) > cbpenddate
    AND pm.tenantid = 'AIAS'
    AND pm.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ;

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for Ongoing 2' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP FA for Ongoing 2'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    --Version 2 added by Amanda for SPI FA ONGOING begin
    RECORD_CNT_ONGOING = 0;

    --Check quarter end 
    SELECT
        COUNT(1)
    INTO
        RECORD_CNT_ONGOING
    FROM
        cs_period csp
    WHERE
        csp.enddate = TO_DATE(ADD_SECONDS(:ONGOING_END_DT,(86400*1)))   /* ORIGSQL: ONGOING_END_DT + 1 */
        AND csp.removedate = :DT_REMOVEDATE
        AND csp.calendarseq = :V_CALENDARSEQ
        AND csp.periodtypeseq = :V_periodtype_quarter_seq;

    IF :RECORD_CNT_ONGOING > 0
    THEN
        /* ORIGSQL: execute immediate 'truncate table EXT.AIA_tmp_comls_period'; */
        /* ORIGSQL: truncate table EXT.AIA_tmp_comls_period ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_tmp_comls_period';

        --Get clawback period for ongoing, from Jan-1 to clawback end date
        /* ORIGSQL: insert into EXT.AIA_tmp_comls_period select periodseq from cs_period where startdate >= trunc(ONGOING_END_DT,'YYYY') and enddate <= ONGOING_END_DT + 1 and removedate = DT_REMOVEDATE and periodtypeseq = V_(...) */
        INSERT INTO EXT.AIA_tmp_comls_period
            SELECT   /* ORIGSQL: select periodseq from cs_period where startdate >= trunc(ONGOING_END_DT,'YYYY') and enddate <= ONGOING_END_DT + 1 and removedate = DT_REMOVEDATE and periodtypeseq = V_periodtype_month_seq; */
                periodseq
            FROM
                cs_period
            WHERE
                startdate >= to_date(:ONGOING_END_DT,'YYYY')  /* ORIGSQL: trunc(ONGOING_END_DT,'YYYY') */
                -- and add_months((ONGOING_END_DT+1),-9)
                AND enddate <= TO_DATE(ADD_SECONDS(:ONGOING_END_DT,(86400*1)))   /* ORIGSQL: ONGOING_END_DT + 1 */
                AND removedate = :DT_REMOVEDATE  -- v9 added removecondition
                AND periodtypeseq = :V_periodtype_month_seq;

        --month

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: insert / *+ APPEND * / */

        /* ORIGSQL: insert into EXT.AIA_CB_TRACE_FORWARD_COMP select STR_BUNAME_FA as BUNAME, ip.quarter || ' ' || ip.year as CALCULATION_PERIOD, ip.ponumber as POLICY_NUMBER, ip.policyidseq as POLICYIDSEQ, pm.positionseq as(...) */
        INSERT INTO EXT.AIA_CB_TRACE_FORWARD_COMP
            /* ORIGSQL: select / *+ LEADING(aias_tx_temp2,pm) PARALLEL * / */
            SELECT   /* ORIGSQL: select STR_BUNAME_FA as BUNAME, ip.quarter || ' ' || ip.year as CALCULATION_PERIOD, ip.ponumber as POLICY_NUMBER, ip.policyidseq as POLICYIDSEQ, pm.positionseq as PAYEE_SEQ, substr(pm_pos.name, 4) as (...) */
                :STR_BUNAME_FA AS BUNAME,
                IFNULL(ip.quarter,'') || ' ' || IFNULL(ip.year,'') AS CALCULATION_PERIOD,
                ip.ponumber AS POLICY_NUMBER,
                ip.policyidseq AS POLICYIDSEQ,
                pm.positionseq AS PAYEE_SEQ,
                SUBSTRING(pm_pos.name,4) AS PAYEE_CODE,  /* ORIGSQL: substr(pm_pos.name, 4) */
                crdga12 AS PAYOR_CODE,
                ip.life_number AS LIFE_NUMBER,
                ip.coverage_number AS COVERAGE_NUMBER,
                ip.rider_number AS RIDER_NUMBER,
                ip.component_code AS COMPONENT_CODE,
                ip.component_name AS COMPONENT_NAME,
                ip.base_rider_ind AS BASE_RIDER_IND,
                crdcompdate AS TRANSACTION_DATE,
                IFNULL(SUBSTRING(:ONGOING_PERIOD,1,3),'') ||'-'||IFNULL(substring(:ONGOING_PERIOD,-4),'') AS PROCESSING_PERIOD,  /* ORIGSQL: substr(ONGOING_PERIOD,1,3) */
                                                                                                                                                     /* ORIGSQL: substr(ONGOING_PERIOD,-4) */
                :STR_ONGOING AS CLAWBACK_TYPE,
                rl.CLAWBACK_NAME AS CLAWBACK_NAME,
                :STR_CB_NAME AS CLAWBACK_METHOD,
                credittypeid AS CREDITTYPE,
                creditseq AS CREDITSEQ,
                crdname AS CREDIT_NAME,
                crdvalue AS CREDIT_VALUE,
                crdpositionseq AS crd_positionseq,
                crdgd2 AS crd_genericdate2,
                crdRuleSeq AS crd_ruleseq,
                pm.measurementseq AS PM_SEQ,
                pm.name AS PM_NAME,
                ip.contributionvalue AS PM_CONTRIBUTION_VALUE,
                1 AS PM_RATE,
                '' AS DEPOSITSEQ,
                '' AS DEPOSIT_NAME,
                '' AS DEPOSIT_VALUE,
                crdperiodseq AS PERIODSEQ,
                ip.salestransactionseq AS SALESTRANSACTIONSEQ,
                crdga2 AS PRODUCT_NAME,
                crdgn1 AS POLICY_YEAR,
                ip.genericnumber2 AS COMMISSION_RATE,
                ip.genericdate4 AS PAID_TO_DATE,
                :P_BATCH_NO AS BATCH_NUMBER,
                CURRENT_TIMESTAMP AS CREATED_DATE,  /* ORIGSQL: sysdate */
                '',
                '',
                :ts_periodseq
            FROM
                ext.aias_tx_temp2 ip
            INNER JOIN
                CS_MEASUREMENT pm
                ON pm.MEASUREMENTSEQ = ip.MEASUREMENTSEQ
                AND pm.tenantid = 'AIAS'
                AND pm.processingunitseq = :V_PROCESSINGUNITSEQ
            INNER JOIN
                CS_POSITION pm_pos
                ON pm.positionseq = pm_pos.ruleelementownerseq
                AND pm_pos.removedate = '1-jan-2200'
                AND pm_pos.effectivestartdate <= TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE) -- version 17 Harm_BSC_SPI 
                AND pm_pos.effectiveenddate > TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME from EXT.AIA_CB_RULES_LOOKUP where RULE_TYPE = 'PM' AND CLAWBACK_NAME in ('SPI_FA_ONG','SPI_FA_ONG_2.1')) */
                        DISTINCT
                        SOURCE_RULE_OUTPUT,
                        CLAWBACK_NAME
                    FROM
                        EXT.AIA_CB_RULES_LOOKUP
                    WHERE
                        RULE_TYPE = 'PM'
                        AND CLAWBACK_NAME IN ('SPI_FA_ONG','SPI_FA_ONG_2.1')
                ) AS rl
                -- version 17 Harm_BSC_SPI
                ON pm.NAME = rl.SOURCE_RULE_OUTPUT
            INNER JOIN
                EXT.AIA_tmp_comls_period period
                ON pm.periodseq = period.periodseq
            INNER JOIN
                cs_position dep_pos
                ON pm.positionseq = dep_pos.ruleelementownerseq
                AND dep_pos.removedate = '1-jan-2200'
                AND dep_pos.effectivestartdate <= TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE) -- version 17 Harm_BSC_SPI 
                AND dep_pos.effectiveenddate > TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE) -- version 17 Harm_BSC_SPI 
            INNER JOIN
                cs_title dep_title
                ON dep_pos.titleseq = dep_title.ruleelementownerseq
                AND dep_title.removedate = '1-jan-2200'
                AND dep_title.effectivestartdate <= TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE) -- version 17 Harm_BSC_SPI 
                AND dep_title.effectiveenddate > TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE) -- version 17 Harm_BSC_SPI 
            WHERE
                TO_DATE(:P_STR_CYCLEDATE,:STR_DATE_FORMAT_TYPE) > cbpenddate
                AND pm.tenantid = 'AIAS'
                AND pm.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ;

        /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP for SPI FA Ongoing ' || '; row count: ' || to_char(sql%rowcount)) */
        CALL EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP for SPI FA Ongoing '|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
            );  /* ORIGSQL: to_char(sql%rowcount) */

        /* ORIGSQL: commit; */
        COMMIT;

        -- version 17 start Harm_BSC_SPI
        -- delete SPI FA 2.2 Ongoing records but agent is FA 2.1

        /* ORIGSQL: delete from EXT.AIA_CB_TRACE_FORWARD_COMP cb where exists (SELECT 1 FROM EXT.AIA_CB_TRACE_FORWARD_COMP tf inner join cs_position cp on cp.ruleelementownerseq = tf.PAYEE_SEQ and cp.tenantid='AIAS' AND cp.effec(...) */
        DELETE
        FROM
            EXT.AIA_CB_TRACE_FORWARD_COMP
            cb
        WHERE  
            EXISTS
            (
                SELECT   /* ORIGSQL: (select 1 from EXT.AIA_CB_TRACE_FORWARD_COMP tf inner join cs_position cp on cp.ruleelementownerseq = tf.PAYEE_SEQ and cp.tenantid='AIAS' AND cp.effectivestartdate <= TO_DATE(V_CYCLE_DATE,STR_DATE_FORMAT_(...) */
                    1
                FROM
                    EXT.AIA_CB_TRACE_FORWARD_COMP tf
                INNER JOIN
                    cs_position cp
                    ON cp.ruleelementownerseq = tf.PAYEE_SEQ
                    AND cp.tenantid = 'AIAS' -- Added by Sundeep
                    AND cp.effectivestartdate <= TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
                    AND cp.effectiveenddate > TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
                    AND cp.removedate = :DT_REMOVEDATE
                INNER JOIN
                    cs_gaparticipant pa
                    ON cp.payeeseq = pa.payeeseq
                    AND pa.tenantid = 'AIAS' -- Added by Sundeep
                    AND pa.effectivestartdate <= TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
                    AND pa.effectiveenddate > TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
                    AND pa.removedate = :DT_REMOVEDATE
                    AND pa.genericboolean2 = 1
                WHERE
                    tf.BUNAME = cb.BUNAME
                    AND tf.CLAWBACK_TYPE = cb.CLAWBACK_TYPE
                    AND tf.PAYOR_CODE = cb.PAYOR_CODE
                    AND tf.POLICY_NUMBER = cb.POLICY_NUMBER
                    AND tf.LIFE_NUMBER = cb.LIFE_NUMBER
                    AND tf.COVERAGE_NUMBER = cb.COVERAGE_NUMBER
                    AND tf.RIDER_NUMBER = cb.RIDER_NUMBER
                    AND tf.COMPONENT_CODE = cb.COMPONENT_CODE
                    AND tf.batch_number = :P_BATCH_NO
                    AND cb.batch_number = :P_BATCH_NO
                    AND tf.CLAWBACK_NAME = 'SPI_FA_ONG'
                    AND cb.CLAWBACK_NAME = 'SPI_FA_ONG'
            );

        /* ORIGSQL: EXT.Log('delete EXT.AIA_CB_TRACE_FORWARD_COMP  duplicate records for SPI FA Ongoing 2.1' || '; row count: ' || to_char(sql%rowcount)) */
        CALL EXT.Log('delete EXT.AIA_CB_TRACE_FORWARD_COMP  duplicate records for SPI FA Ongoing 2.1'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
            );  /* ORIGSQL: to_char(sql%rowcount) */

        /* ORIGSQL: commit; */
        COMMIT;

        -- update old agent code for old bize pay to new code
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into EXT.AIA_CB_TRACE_FORWARD_COMP tf using (SELECT distinct ip.WRI_AGT_CODE, ip.ponumber, ip.COMPONENT_CODE, ip.FAOB_AGT_CODE, ip.POLICYIDSEQ FROM EXT.AIA_CB_IDENTIFY_POLICY ip inner join EXT.AIA_CB_TRACE_(...) */
        MERGE INTO EXT.AIA_CB_TRACE_FORWARD_COMP AS tf
            USING
            (
                SELECT   /* ORIGSQL: (SELECT distinct ip.WRI_AGT_CODE, ip.ponumber, ip.COMPONENT_CODE, ip.FAOB_AGT_CODE, ip.POLICYIDSEQ FROM EXT.AIA_CB_IDENTIFY_POLICY ip inner join EXT.AIA_CB_TRACE_FORWARD_COMP ctfc on ip.POLICYIDSEQ = ctfc.POL(...) */
                    DISTINCT
                    ip.WRI_AGT_CODE,
                    ip.ponumber,
                    ip.COMPONENT_CODE,
                    ip.FAOB_AGT_CODE,
                    ip.POLICYIDSEQ
                FROM
                    EXT.AIA_CB_IDENTIFY_POLICY ip
                INNER JOIN
                    EXT.AIA_CB_TRACE_FORWARD_COMP ctfc
                    ON ip.POLICYIDSEQ = ctfc.POLICYIDSEQ
                WHERE
                    ip.FAOB_AGT_CODE IS NOT NULL
                    AND ip.BUNAME = :STR_BUNAME_FA
                    AND ctfc.BATCH_NUMBER = :P_BATCH_NO
                    AND IFNULL(ip.quarter,'') || ' '|| IFNULL(ip.year,'') = ctfc.CALCULATION_PERIOD
            ) AS temp
            ON (tf.POLICY_NUMBER = temp.ponumber
                AND tf.COMPONENT_CODE = temp.COMPONENT_CODE
                AND tf.BUNAME = :STR_BUNAME_FA
                AND tf.BATCH_NUMBER = :P_BATCH_NO
            AND tf.POLICYIDSEQ = temp.POLICYIDSEQ)
        WHEN MATCHED THEN
            UPDATE
                SET tf.OLD_AGENT_CD = temp.WRI_AGT_CODE;

        /* ORIGSQL: EXT.Log('update EXT.AIA_CB_TRACE_FORWARD_COMP old agent code for SPI FA Ongoing 2.1' || '; row count: ' || to_char(sql%rowcount)) */
        CALL EXT.Log('update EXT.AIA_CB_TRACE_FORWARD_COMP old agent code for SPI FA Ongoing 2.1'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
            );  /* ORIGSQL: to_char(sql%rowcount) */

        /* ORIGSQL: commit; */
        COMMIT;

        -- version 17 end Harm_BSC_SPI
    END IF;
    --Version 2 end

    /*
    EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP for SPI Ongoing, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);
    
    --for SPI on-going
    --for on-going compensation trace forward
    insert into EXT.AIA_CB_TRACE_FORWARD_COMP
      with cb_period as
       (select p.periodseq, p.name
              from cs_period a
             inner join cs_period p
                on p.startdate >= a.startdate
         and p.enddate <= a.enddate
             inner join cs_periodtype cpt_qtr
                on a.periodtypeseq = cpt_qtr.periodtypeseq
         and cpt_qtr.name = 'quarter'
             inner join cs_periodtype cpt_mon
                on p.periodtypeseq = cpt_mon.periodtypeseq
         and cpt_mon.name = 'month'
             where a.calendarseq = V_CALENDARSEQ
         and p.calendarseq = V_CALENDARSEQ
         and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) between a.startdate and
                   (a.enddate - 1)
         and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) >= p.startdate)
      select STR_BUNAME as BUNAME,
             ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
             ip.ponumber as POLICY_NUMBER,
             ip.policyidseq as POLICYIDSEQ,
             pm.positionseq as PAYEE_SEQ,
             substr(pm_pos.name, 4) as PAYEE_CODE,
             crd.genericattribute12 as PAYOR_CODE,
             ip.life_number as LIFE_NUMBER,
             ip.coverage_number as COVERAGE_NUMBER,
             ip.rider_number as RIDER_NUMBER,
             ip.component_code as COMPONENT_CODE,
             ip.component_name as COMPONENT_NAME,
             ip.base_rider_ind as BASE_RIDER_IND,
             crd.compensationdate as TRANSACTION_DATE,
             substr(ONGOING_PERIOD, 1, 3) || '-' || substr(ONGOING_PERIOD, -4) as PROCESSING_PERIOD,
             STR_ONGOING as CLAWBACK_TYPE,
             rl.CLAWBACK_NAME as CLAWBACK_NAME,
             STR_CB_NAME as CLAWBACK_METHOD,
             ct.credittypeid as CREDITTYPE,
             crd.creditseq as CREDITSEQ,
             crd.name as CREDIT_NAME,
             crd.value as CREDIT_VALUE,
             crd.positionseq as crd_positionseq,
             st.genericdate2 as crd_genericdate2,
             crd.ruleseq as crd_ruleseq,
             pm.measurementseq as PM_SEQ,
             pm.name as PM_NAME,
             pct.contributionvalue as PM_CONTRIBUTION_VALUE,
             1 as PM_RATE,
             '' as DEPOSITSEQ,
             '' as DEPOSIT_NAME,
             '' as DEPOSIT_VALUE,
             crd.periodseq as PERIODSEQ,
             st.salestransactionseq as SALESTRANSACTIONSEQ,
             crd.genericattribute2 as PRODUCT_NAME,
             crd.genericnumber1 as POLICY_YEAR,
             st.genericnumber2 as COMMISSION_RATE,
             st.genericdate4 as PAID_TO_DATE,
             P_BATCH_NO as BATCH_NUMBER,
             sysdate as CREATED_DATE
        FROM CS_SALESTRANSACTION st
       inner join CS_CREDIT crd
          on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
       inner join CS_PMCREDITTRACE pct
          on crd.CREDITSEQ = pct.CREDITSEQ
       inner join CS_MEASUREMENT pm
          on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
       inner join CS_POSITION pm_pos
          on pm.positionseq = pm_pos.ruleelementownerseq
     and pm_pos.removedate = DT_REMOVEDATE
     and pm_pos.effectivestartdate <= crd.genericdate2
     and pm_pos.effectiveenddate > crd.genericdate2
       inner join CS_CREDITTYPE ct
          on crd.CREDITTYPESEQ = ct.DATATYPESEQ
     and ct.Removedate = DT_REMOVEDATE
       inner join cb_period
          on pm.periodseq = cb_period.periodseq
     and crd.periodseq = cb_period.periodseq
       inner join EXT.AIA_CB_IDENTIFY_POLICY ip
          on st.PONUMBER = ip.PONUMBER
     AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
     AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
     AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
     AND st.PRODUCTID = ip.COMPONENT_CODE
     and crd.genericattribute12 = ip.wri_agt_code
       inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
                         from EXT.AIA_CB_RULES_LOOKUP
                        where RULE_TYPE = 'PM'
     AND CLAWBACK_NAME = 'SPI_ONG') rl
          on pm.NAME = rl.SOURCE_RULE_OUTPUT
       inner join cs_position dep_pos
          on pm.positionseq = dep_pos.ruleelementownerseq
     and dep_pos.removedate = DT_REMOVEDATE
     and dep_pos.effectivestartdate <= crd.genericdate2
     and dep_pos.effectiveenddate > crd.genericdate2
      --for lookup the receiver info.
       inner join cs_title dep_title
          on dep_pos.titleseq = dep_title.ruleelementownerseq
     and dep_title.removedate = DT_REMOVEDATE
     and dep_title.effectivestartdate <= crd.genericdate2
     and dep_title.effectiveenddate > crd.genericdate2
       inner join (select distinct
                              cb_quarter_name,
                              cb_startdate,
                              cb_enddate
                         from EXT.AIA_cb_period
                    where cb_name = STR_COMPENSATION) cbp
          on ip.quarter || ' ' || ip.year = cbp.cb_quarter_name
       WHERE st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
     AND st.BUSINESSUNITMAP = 1
         --to avoid fetching the transactions which not being processed by lumpsum procedure
     and to_date(P_STR_CYCLEDATE,STR_DATE_FORMAT_TYPE) > cbp.cb_enddate
         ;
    
    EXT.Log('insert into EXT.AIA_CB_TRACE_FORWARD_COMP for SPI Ongoing' || '; row count: ' || to_char(sql%rowcount));
    
    commit;*/
END IF;

-- LABEL ProcDone: BEGIN/*dummy block for label*/END;  /* ORIGSQL: <<ProcDone>> */

    /* ORIGSQL: NULL; */
    -- DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
END;
  PUBLIC PROCEDURE SP_CLAWBACK_COMP_FA
(
    IN P_STR_CYCLEDATE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                       /* ORIGSQL: P_STR_CYCLEDATE IN VARCHAR2 */
    IN P_BATCH_NO BIGINT     /* ORIGSQL: P_BATCH_NO IN INTEGER */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE STR_LUMPSUM CONSTANT VARCHAR(20) = 'LUMPSUM';  /* ORIGSQL: STR_LUMPSUM CONSTANT VARCHAR2(20) := 'LUMPSUM'; */
    DECLARE STR_ONGOING CONSTANT VARCHAR(20) = 'ONGOING';  /* ORIGSQL: STR_ONGOING CONSTANT VARCHAR2(20) := 'ONGOING'; */
    DECLARE STR_BUNAME_FA CONSTANT VARCHAR(20) = 'SGPAFA';  /* ORIGSQL: STR_BUNAME_FA CONSTANT VARCHAR2(20) := 'SGPAFA'; */
    DECLARE STR_DATE_FORMAT CONSTANT VARCHAR(50) = 'yyyymmdd';  /* ORIGSQL: STR_DATE_FORMAT CONSTANT VARCHAR2(50) := 'yyyymmdd'; */
    DECLARE STR_CB_NAME CONSTANT VARCHAR(20) = 'COMPENSATION';  /* ORIGSQL: STR_CB_NAME CONSTANT VARCHAR2(20) := 'COMPENSATION'; */
    DECLARE STR_CALENDARNAME CONSTANT VARCHAR(50) = 'AIA Singapore Calendar';  /* ORIGSQL: STR_CALENDARNAME CONSTANT VARCHAR2(50) := 'AIA Singapore Calendar'; */
    DECLARE V_CAL_PERIOD VARCHAR(30);  /* ORIGSQL: V_CAL_PERIOD VARCHAR2(30); */

    --measurement quarter
    DECLARE DT_REMOVEDATE CONSTANT TIMESTAMP = TO_DATE('22000101', 'yyyymmdd');  /* ORIGSQL: DT_REMOVEDATE CONSTANT DATE := TO_DATE('22000101', 'yyyymmdd') ; */
    DECLARE DT_CB_START_DATE TIMESTAMP;  /* ORIGSQL: DT_CB_START_DATE DATE; */
    DECLARE DT_CB_END_DATE TIMESTAMP;  /* ORIGSQL: DT_CB_END_DATE DATE; */
    DECLARE DT_INCEPTION_START_DATE TIMESTAMP;  /* ORIGSQL: DT_INCEPTION_START_DATE DATE; */
    DECLARE DT_INCEPTION_END_DATE TIMESTAMP;  /* ORIGSQL: DT_INCEPTION_END_DATE DATE; */
    DECLARE DT_WEEKLY_START_DATE TIMESTAMP;  /* ORIGSQL: DT_WEEKLY_START_DATE DATE; */
    DECLARE DT_WEEKLY_END_DATE TIMESTAMP;  /* ORIGSQL: DT_WEEKLY_END_DATE DATE; */
    DECLARE DT_ONGOING_START_DATE TIMESTAMP;  /* ORIGSQL: DT_ONGOING_START_DATE DATE; */
    DECLARE DT_ONGOING_END_DATE TIMESTAMP;  /* ORIGSQL: DT_ONGOING_END_DATE DATE; */
    DECLARE NUM_OF_CYCLE_IND BIGINT;  /* ORIGSQL: NUM_OF_CYCLE_IND integer; */
    DECLARE STR_DATE_FORMAT_TYPE CONSTANT VARCHAR(50) = 'yyyy-mm-dd';  /* ORIGSQL: STR_DATE_FORMAT_TYPE CONSTANT VARCHAR2(50) := 'yyyy-mm-dd'; */
    DECLARE V_REC_COUNT BIGINT;  /* ORIGSQL: V_REC_COUNT INTEGER; */
    DECLARE V_NLPI_RATE DECIMAL(10,2);  /* ORIGSQL: V_NLPI_RATE NUMBER(10,2); */
    DECLARE INT_SVI_RATE DECIMAL(10,2) = 0.60;  /* ORIGSQL: INT_SVI_RATE NUMBER(10,2) := 0.60; */
    DECLARE V_BATCH_NO_PRE_QTR BIGINT;  /* ORIGSQL: V_BATCH_NO_PRE_QTR INTEGER; */
    DECLARE V_CB_TYPE VARCHAR(50);  /* ORIGSQL: V_CB_TYPE VARCHAR2(50); */
    DECLARE V_CB_NAME VARCHAR(50);  /* ORIGSQL: V_CB_NAME VARCHAR2(50); */
    DECLARE STR_STATUS_COMPLETED_SH CONSTANT VARCHAR(20) = 'completed_sh';  /* ORIGSQL: STR_STATUS_COMPLETED_SH CONSTANT VARCHAR2(20) := 'completed_sh'; */
    DECLARE V_CB_QTR VARCHAR(50);  /* ORIGSQL: V_CB_QTR VARCHAR2(50); */

    --Version 2 add by Amanda begin
    DECLARE V_Curr_QTR VARCHAR(30);  /* ORIGSQL: V_Curr_QTR VARCHAR2(30); */
    DECLARE V_Previous_QTR VARCHAR(50);  /* ORIGSQL: V_Previous_QTR VARCHAR2(30); */
    DECLARE V_First_QTR VARCHAR(50);
    DECLARE V_Second_QTR VARCHAR(50);

    /* initialize library variables, if not yet done */
    CALL init_session_global();

    --Version 2 end

    /* ORIGSQL: EXT.Log('SP_CLAWBACK_COMP for FA start') */
    CALL EXT.Log('SP_CLAWBACK_COMP for FA start');

    /* ORIGSQL: init */
    CALL init();

    --version 7 start

    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_CB_BSC_LEADER_TMP'; */
    /* ORIGSQL: truncate table EXT.AIA_CB_BSC_LEADER_TMP ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_CB_BSC_LEADER_TMP';

    /* ORIGSQL: EXT.Log('P_STR_CYCLEDATE:  '||P_STR_CYCLEDATE) */
    CALL EXT.Log('P_STR_CYCLEDATE:  '||IFNULL(:P_STR_CYCLEDATE,''));

    /* ORIGSQL: EXT.Log('P_BATCH_NO:  '||P_BATCH_NO) */
    CALL EXT.Log('P_BATCH_NO:  '||IFNULL(TO_VARCHAR(:P_BATCH_NO),''));

    --update leader agency for ongoing only
    /* ORIGSQL: insert into EXT.AIA_CB_BSC_LEADER_TMP (YEAR, QUARTER, FSC_CODE, LEADER_CODE, ENTITLEMENT, LEADER_AGENCY) SELECT ldr.YEAR, ldr.QUARTER, ldr.FSC_CODE, ldr.LEADER_CODE, ldr.ENTITLEMENT, pos.GENERICATTRIBUTE1(...) */
    INSERT INTO EXT.AIA_CB_BSC_LEADER_TMP
        (
            YEAR,
            QUARTER,
            FSC_CODE,
            LEADER_CODE,
            ENTITLEMENT,
            LEADER_AGENCY
        )
        SELECT   /* ORIGSQL: SELECT ldr.YEAR, ldr.QUARTER, ldr.FSC_CODE, ldr.LEADER_CODE, ldr.ENTITLEMENT, pos.GENERICATTRIBUTE1 AS LEADER_AGENCY FROM EXT.AIA_CB_BSC_LEADER ldr, cs_position pos where pos.effectivestartdate <= TO_DATE(...) */
            ldr.YEAR,
            ldr.QUARTER,
            ldr.FSC_CODE,
            ldr.LEADER_CODE,
            ldr.ENTITLEMENT,
            pos.GENERICATTRIBUTE1 AS LEADER_AGENCY
        FROM
            EXT.AIA_CB_BSC_LEADER ldr,
            cs_position pos
        WHERE
            pos.effectivestartdate <= TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
            AND pos.effectiveenddate > TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
            AND pos.removedate = :DT_REMOVEDATE
            AND 'SGT'||IFNULL(ldr.LEADER_CODE,'') = pos.NAME;

    /* ORIGSQL: EXT.Log('Update leader agency code for special rate ' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('Update leader agency code for special rate '|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    --version 7 end

    --get records count from EXT.AIA_CB_CLAWBACK_COMP
    SELECT
        COUNT(1)
    INTO
        V_REC_COUNT
    FROM
        EXT.AIA_CB_CLAWBACK_COMP
    WHERE
        batch_no = :P_BATCH_NO;

    --delete the records in EXT.AIA_CB_CLAWBACK_COMP if batch number is being reused.
    IF :V_REC_COUNT > 0
    THEN
        /* ORIGSQL: delete from EXT.AIA_CB_CLAWBACK_COMP where batch_no = P_BATCH_NO; */
        DELETE
        FROM
            EXT.AIA_CB_CLAWBACK_COMP
        WHERE
            batch_no = :P_BATCH_NO;

        /* ORIGSQL: delete from EXT.AIA_CB_CLAWBACK_SVI_COMP_TMP where batch_no = P_BATCH_NO; */
        DELETE
        FROM
            EXT.AIA_CB_CLAWBACK_SVI_COMP_TMP
        WHERE
            batch_no = :P_BATCH_NO;

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    --Version 2 add by Amanda for SPI FA CB begin
    /* ORIGSQL: delete from EXT.AIA_CB_SPI_CLAWBACK cb where exists (SELECT 1 FROM EXT.AIA_CB_TRACE_FORWARD_COMP tf inner join cs_period csp on csp.periodseq = tf.YTD_MPERIOD and csp.removedate = DT_REMOVEDATE and csp.period(...) */
    DELETE
    FROM
        EXT.AIA_CB_SPI_CLAWBACK
        cb
    WHERE  
        EXISTS
        (
            SELECT   /* ORIGSQL: (select 1 from EXT.AIA_CB_TRACE_FORWARD_COMP tf inner join cs_period csp on csp.periodseq = tf.YTD_MPERIOD and csp.removedate = DT_REMOVEDATE and csp.periodtypeseq = V_periodtype_month_seq and csp.calenda(...) */
                1
            FROM
                EXT.AIA_CB_TRACE_FORWARD_COMP tf
            INNER JOIN
                cs_period csp
                ON csp.periodseq = tf.YTD_MPERIOD
                AND csp.removedate = :DT_REMOVEDATE
                AND csp.periodtypeseq = :V_periodtype_month_seq --month
                AND csp.calendarseq = :V_CALENDARSEQ --2251799813685250
            INNER JOIN
                cs_period qtr
                ON csp.parentseq = qtr.periodseq
                AND qtr.removedate = :DT_REMOVEDATE
                AND qtr.calendarseq = :V_CALENDARSEQ --2251799813685250
                AND qtr.periodtypeseq = :V_periodtype_quarter_seq --quarter
            WHERE
                tf.BUNAME = cb.BUNAME
                AND tf.CLAWBACK_TYPE = cb.CLAWBACK_TYPE
                AND tf.PAYOR_CODE = cb.WRI_AGT_CODE
                AND tf.POLICY_NUMBER = cb.PONUMBER
                AND tf.LIFE_NUMBER = cb.LIFE_NUMBER
                AND tf.COVERAGE_NUMBER = cb.COVERAGE_NUMBER
                AND tf.RIDER_NUMBER = cb.RIDER_NUMBER
                AND tf.COMPONENT_CODE = cb.COMPONENT_CODE
                AND qtr.name = (IFNULL(cb.quarter,'') || ' ' || IFNULL(cb.YEAR,'')) --get current quarter to delete 
                AND tf.clawback_name IN ('SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1') -- version 17 Harm_BSC_SPI
                AND tf.batch_number = :P_BATCH_NO
        )
        AND cb.BUNAME = :STR_BUNAME_FA;

    /* ORIGSQL: EXT.Log('delete from EXT.AIA_CB_SPI_CLAWBACK for FA ' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('delete from EXT.AIA_CB_SPI_CLAWBACK for FA '|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    SELECT
        MIN(year_qtr)
    INTO
        V_First_QTR
    FROM
        EXT.AIA_TMP_COMLS_PERIOD_SPI
    WHERE
        year_qtr IS NOT NULL;

    SELECT
        MAX(year_qtr)
    INTO
        V_Second_QTR
    FROM
        EXT.AIA_TMP_COMLS_PERIOD_SPI
    WHERE
        year_qtr IS NOT NULL;

    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_RELATIONALMDLT' not found */
    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_MDLTINDEX' not found */

    /* ORIGSQL: insert into EXT.AIA_CB_SPI_CLAWBACK(YEAR, QUARTER, BUNAME, PONUMBER, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE, WRI_AGT_CODE, CLAWBACK_TYPE, PROCESSING_PERIOD, SPI_RATE, YTD_PIB, YTD_SPI_(...) */
    INSERT INTO EXT.AIA_CB_SPI_CLAWBACK
        (
            YEAR, QUARTER, BUNAME, PONUMBER, LIFE_NUMBER, COVERAGE_NUMBER,
            RIDER_NUMBER, COMPONENT_CODE, WRI_AGT_CODE, CLAWBACK_TYPE, PROCESSING_PERIOD, SPI_RATE,
            YTD_PIB, YTD_SPI_CB, SPI_CB
        )
        SELECT   /* ORIGSQL: select substr(qtr.name,4,4) as year, substr(qtr.name,1,2) as qtr, tf.buname, tf.policy_number, tf.LIFE_NUMBER, tf.COVERAGE_NUMBER, tf.RIDER_NUMBER, tf.COMPONENT_CODE, tf.payor_code WRI_AGT_CODE, tf.CL(...) */
            SUBSTRING(qtr.name,4,4) AS year,
            SUBSTRING(qtr.name,1,2) AS qtr,  /* ORIGSQL: substr(qtr.name,1,2) */
            tf.buname,
            tf.policy_number,
            tf.LIFE_NUMBER,
            tf.COVERAGE_NUMBER,
            tf.RIDER_NUMBER,
            tf.COMPONENT_CODE,
            tf.payor_code AS WRI_AGT_CODE,
            tf.CLAWBACK_TYPE,
            TO_VARCHAR(TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE),'YYYYMM') AS PROCESSING_PERIOD,  /* ORIGSQL: TO_CHAR(to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE),'YYYYMM') */
            MAX(tf.PM_RATE) /* --SPI Rate */, SUM((
                    CASE 
                        WHEN pm_name IN ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl')
                        THEN -1 * tf.pm_contribution_value
                        ELSE tf.pm_contribution_value
                    END
                    * 0.60 -
                    CASE
                        WHEN pm_name IN ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl')
                        THEN -1 * tf.pm_contribution_value
                        ELSE tf.pm_contribution_value
                    END
                    * 0.60 * ba.entitlementpercent) * (-1)) /* --YTD PIB */, SUM((
                    CASE 
                        WHEN pm_name IN ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl')
                        THEN -1 * tf.pm_contribution_value
                        ELSE tf.pm_contribution_value
                    END
                    * 0.60 -
                    CASE
                        WHEN pm_name IN ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl')
                        THEN -1 * tf.pm_contribution_value
                        ELSE tf.pm_contribution_value
                    END
                    * 0.60 * ba.entitlementpercent) * (-1) * tf.PM_RATE) /* --YTD SPI CB = current QTR YTD SPI CB */,
            CASE
                WHEN tf.clawback_name = 'SPI_FA'
                THEN SUM((
                        CASE
                            WHEN pm_name IN ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl')
                            THEN -1 * tf.pm_contribution_value
                            ELSE tf.pm_contribution_value
                        END
                        * 0.60 -
                        CASE
                            WHEN pm_name IN ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl')
                            THEN -1 * tf.pm_contribution_value
                            ELSE tf.pm_contribution_value
                        END
                        * 0.60 * ba.entitlementpercent) * (-1) * tf.PM_RATE)
                WHEN tf.clawback_name = 'SPI_FA_ONG'
                THEN SUM((
                        CASE
                            WHEN pm_name IN ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl')
                            THEN -1 * tf.pm_contribution_value
                            ELSE tf.pm_contribution_value
                        END
                        * 0.60 -
                        CASE
                            WHEN pm_name IN ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl')
                            THEN -1 * tf.pm_contribution_value
                            ELSE tf.pm_contribution_value
                        END
                        * 0.60 * ba.entitlementpercent) * (-1))
            END
            AS SPI_CB /* --SPI CB */
        FROM
            EXT.AIA_CB_TRACE_FORWARD_COMP tf
        INNER JOIN
            EXT.AIA_cb_bsc_agent ba
            ON tf.calculation_period = (IFNULL(ba.quarter,'') || ' '|| IFNULL(ba.year,''))
            AND tf.payor_code = ba.agentcode
        INNER JOIN
            cs_period csp
            ON csp.periodseq = tf.YTD_MPERIOD
            AND csp.removedate = :DT_REMOVEDATE
            AND csp.periodtypeseq = :V_periodtype_month_seq --month
            AND csp.calendarseq = :V_CALENDARSEQ
        INNER JOIN
            cs_period qtr
            ON csp.parentseq = qtr.periodseq
            AND qtr.removedate = :DT_REMOVEDATE
            AND qtr.calendarseq = :V_CALENDARSEQ
            AND qtr.periodtypeseq = :V_periodtype_quarter_seq --quarter
        WHERE
            tf.clawback_name IN ('SPI_FA','SPI_FA_ONG')
            AND tf.batch_number = :P_BATCH_NO
            --Version 13 update by Amanda for AGY SPI exclusion begin
            AND tf.component_code NOT IN
            (
                SELECT   /* ORIGSQL: (select b.MINSTRING from CS_RELATIONALMDLT a inner join CS_MDLTIndex b on a.ruleelementseq = b.ruleelementseq and a.removedate = DT_REMOVEDATE and b.removedate = DT_REMOVEDATE and a.name = 'LT_SPI_Bon(...) */
                    b.MINSTRING
                FROM
                    CS_RELATIONALMDLT a
                INNER JOIN
                    CS_MDLTIndex b
                    ON a.ruleelementseq = b.ruleelementseq
                    AND a.removedate = :DT_REMOVEDATE
                    AND b.removedate = :DT_REMOVEDATE
                    AND a.name = 'LT_SPI_Bonus_Component_Excl'
            ) --Version 13 end
        GROUP BY
            qtr.name,
            tf.buname,
            tf.policy_number,
            tf.LIFE_NUMBER,
            tf.COVERAGE_NUMBER,
            tf.RIDER_NUMBER,
            tf.COMPONENT_CODE,
            tf.payor_code,
            tf.CLAWBACK_TYPE,
            tf.clawback_name;

    -- version 17 Harm_BSC_SPI start  

    /* ORIGSQL: insert into EXT.AIA_CB_SPI_CLAWBACK(YEAR, QUARTER, BUNAME, PONUMBER, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE, WRI_AGT_CODE, CLAWBACK_TYPE, PROCESSING_PERIOD, SPI_RATE, YTD_PIB, YTD_SPI_(...) */
    INSERT INTO EXT.AIA_CB_SPI_CLAWBACK
        (
            YEAR, QUARTER, BUNAME, PONUMBER, LIFE_NUMBER, COVERAGE_NUMBER,
            RIDER_NUMBER, COMPONENT_CODE, WRI_AGT_CODE, CLAWBACK_TYPE, PROCESSING_PERIOD, SPI_RATE,
            YTD_PIB, YTD_SPI_CB, SPI_CB
        )
        SELECT   /* ORIGSQL: select substr(qtr.name,4,4) as year, substr(qtr.name,1,2) as qtr, tf.buname, tf.policy_number, tf.LIFE_NUMBER, tf.COVERAGE_NUMBER, tf.RIDER_NUMBER, tf.COMPONENT_CODE, tf.payor_code WRI_AGT_CODE, tf.CL(...) */
            SUBSTRING(qtr.name,4,4) AS year,
            SUBSTRING(qtr.name,1,2) AS qtr,  /* ORIGSQL: substr(qtr.name,1,2) */
            tf.buname,
            tf.policy_number,
            tf.LIFE_NUMBER,
            tf.COVERAGE_NUMBER,
            tf.RIDER_NUMBER,
            tf.COMPONENT_CODE,
            tf.payor_code AS WRI_AGT_CODE,
            tf.CLAWBACK_TYPE,
            TO_VARCHAR(TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE),'YYYYMM') AS PROCESSING_PERIOD,  /* ORIGSQL: TO_CHAR(to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE),'YYYYMM') */
            MAX(tf.PM_RATE),
            SUM((
                    CASE 
                        WHEN pm_name = 'PM_FYC_SPI_Bonus_Excl'
                        THEN -1 * tf.pm_contribution_value
                        ELSE tf.pm_contribution_value
                    END
                    * 0.60 -
                    CASE
                        WHEN pm_name = 'PM_FYC_SPI_Bonus_Excl'
                        THEN -1 * tf.pm_contribution_value
                        ELSE tf.pm_contribution_value
                    END
                    * 0.60 * ba.entitlementpercent) * (-1)) /* --YTD PIB */, SUM((
                    CASE 
                        WHEN pm_name = 'PM_FYC_SPI_Bonus_Excl'
                        THEN -1 * tf.pm_contribution_value
                        ELSE tf.pm_contribution_value
                    END
                    * 0.60 -
                    CASE
                        WHEN pm_name = 'PM_FYC_SPI_Bonus_Excl'
                        THEN -1 * tf.pm_contribution_value
                        ELSE tf.pm_contribution_value
                    END
                    * 0.60 * ba.entitlementpercent) * (-1) * tf.PM_RATE) /* --YTD SPI CB = current QTR YTD SPI CB */,
            CASE
                WHEN tf.clawback_name = 'SPI_FA_2.1'
                THEN SUM((
                        CASE
                            WHEN pm_name = 'PM_FYC_SPI_Bonus_Excl'
                            THEN -1 * tf.pm_contribution_value
                            ELSE tf.pm_contribution_value
                        END
                        * 0.60 -
                        CASE
                            WHEN pm_name = 'PM_FYC_SPI_Bonus_Excl'
                            THEN -1 * tf.pm_contribution_value
                            ELSE tf.pm_contribution_value
                        END
                        * 0.60 * ba.entitlementpercent) * (-1) * tf.PM_RATE)
                WHEN tf.clawback_name = 'SPI_FA_ONG_2.1'
                THEN SUM((
                        CASE
                            WHEN pm_name = 'PM_FYC_SPI_Bonus_Excl'
                            THEN -1 * tf.pm_contribution_value
                            ELSE tf.pm_contribution_value
                        END
                        * 0.60 -
                        CASE
                            WHEN pm_name = 'PM_FYC_SPI_Bonus_Excl'
                            THEN -1 * tf.pm_contribution_value
                            ELSE tf.pm_contribution_value
                        END
                        * 0.60 * ba.entitlementpercent) * (-1))
            END
            AS SPI_CB /* --SPI CB */
            /* --Version 13 end */
        FROM
            EXT.AIA_CB_TRACE_FORWARD_COMP tf
        INNER JOIN
            EXT.AIA_cb_bsc_agent ba
            ON tf.calculation_period = (IFNULL(ba.quarter,'') || ' '|| IFNULL(ba.year,''))
            AND ba.agentcode = (
                CASE
                    WHEN tf.OLD_AGENT_CD IS NOT NULL
                    THEN tf.OLD_AGENT_CD
                    ELSE tf.payor_code
                END
            )
        INNER JOIN
            cs_period csp
            ON csp.periodseq = tf.YTD_MPERIOD
            AND csp.removedate = :DT_REMOVEDATE
            AND csp.periodtypeseq = :V_periodtype_month_seq --month
            AND csp.calendarseq = :V_CALENDARSEQ
        INNER JOIN
            cs_period qtr
            ON csp.parentseq = qtr.periodseq
            AND qtr.removedate = :DT_REMOVEDATE
            AND qtr.calendarseq = :V_CALENDARSEQ
            AND qtr.periodtypeseq = :V_periodtype_quarter_seq --quarter
        WHERE
            tf.clawback_name IN ('SPI_FA_2.1','SPI_FA_ONG_2.1')
            AND tf.batch_number = :P_BATCH_NO
            AND tf.component_code NOT IN
            (
                SELECT   /* ORIGSQL: (select b.MINSTRING from CS_RELATIONALMDLT a inner join CS_MDLTIndex b on a.ruleelementseq = b.ruleelementseq and a.removedate = DT_REMOVEDATE and b.removedate = DT_REMOVEDATE and a.name = 'LT_SPI_Bon(...) */
                    b.MINSTRING
                FROM
                    CS_RELATIONALMDLT a
                INNER JOIN
                    CS_MDLTIndex b
                    ON a.ruleelementseq = b.ruleelementseq
                    AND a.removedate = :DT_REMOVEDATE
                    AND b.removedate = :DT_REMOVEDATE
                    AND a.name = 'LT_SPI_Bonus_Component_Excl'
            )
        GROUP BY
            qtr.name,
            tf.buname,
            tf.policy_number,
            tf.LIFE_NUMBER,
            tf.COVERAGE_NUMBER,
            tf.RIDER_NUMBER,
            tf.COMPONENT_CODE,
            tf.payor_code,
            tf.CLAWBACK_TYPE,
            tf.clawback_name;

    -- version 17 Harm_BSC_SPI  end

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_SPI_CLAWBACK FA 1st QTR' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('insert into EXT.AIA_CB_SPI_CLAWBACK FA 1st QTR'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    --Version 2 end

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_CLAWBACK_COMP FA for FYO,RYO, NADOR' ||' batch_no = ' || P_BATCH_NO) */
    CALL EXT.Log('insert into EXT.AIA_CB_CLAWBACK_COMP FA for FYO,RYO, NADOR'||' batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO),''));

    --insert data into EXT.AIA_CB_CLAWBACK_COMP for compensation for FYO, RYO and NADOR
    /* ORIGSQL: insert into EXT.AIA_CB_CLAWBACK_COMP select tf.calculation_period as MEASUREMENT_QUARTER, tf.clawback_type as CLAWBACK_TYPE, tf.clawback_name as CLAWBACK_NAME, STR_CB_NAME as CLAWBACK_METHOD, TO_DATE(P_ST(...) */
    INSERT INTO EXT.AIA_CB_CLAWBACK_COMP
        SELECT   /* ORIGSQL: select tf.calculation_period as MEASUREMENT_QUARTER, tf.clawback_type as CLAWBACK_TYPE, tf.clawback_name as CLAWBACK_NAME, STR_CB_NAME as CLAWBACK_METHOD, TO_DATE(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE(...) */
            tf.calculation_period AS MEASUREMENT_QUARTER,
            tf.clawback_type AS CLAWBACK_TYPE,
            tf.clawback_name AS CLAWBACK_NAME,
            :STR_CB_NAME AS CLAWBACK_METHOD,
            TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE) AS CALCULATION_DATE,
            Agency_code.genericattribute3 AS WRI_DIST_CODE,
            TRIM(IFNULL(District_name.firstname,'')||' '||IFNULL(District_name.lastname,'')) AS WRI_DIST_NAME,
            DM_code.genericattribute2 AS WRI_DM_CODE,
            /* --substr(pos_agy.name, 4) as WRI_AGY_CODE, */
            /* --pos_agy.genericattribute1 as WRI_AGY_CODE, */
            agent.genericattribute1 AS WRI_AGY_CODE,
            /* --         trim(par_agy.firstname||' '||par_agy.lastname) as WRI_AGY_NAME, */
            TRIM(IFNULL(Agency_name.firstname,'')||' '||IFNULL(Agency_name.lastname,'')) AS WRI_AGY_NAME,
            agent.genericattribute2 AS wri_agy_ldr_code,
            agent.genericattribute7 AS wri_agy_ldr_name,
            tf.payor_code AS WRI_AGT_CODE,
            TRIM(IFNULL(Agent_name.firstname,'')||' '||IFNULL(Agent_name.lastname,'')) AS wri_agt_name,
            MAP(Agent_name.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') AS FSC_TYPE,  /* ORIGSQL: decode(Agent_name.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') */
            title_agt.name AS RANK,
            agent.genericattribute4 AS UM_CLASS,
            agent.genericattribute11 AS UM_RANK /* -- Check cr.genericattribute14 as CLASS, */, ba.bsc_grade AS FSC_BSC_GRADE,
            /* --ba.entitlementpercent as FSC_BSC_PERCENTAGE, */
            IFNULL(ldr.ENTITLEMENT,ba.entitlementpercent) AS FSC_BSC_PERCENTAGE /* --version 7 add */, tf.policy_number AS PONUMBER,  /* ORIGSQL: nvl(ldr.ENTITLEMENT,ba.entitlementpercent) */
            tf.life_number AS LIFE_NUMBER,
            tf.coverage_number AS COVERAGE_NUMBER,
            tf.RIDER_NUMBER AS RIDER_NUMBER,
            tf.COMPONENT_NAME AS COMPONENT_NAME,
            tf.component_code AS COMPONENT_CODE,
            tf.PRODUCT_NAME AS PRODUCT_NAME,
            tf.transaction_date AS TRANSACTION_DATE,
            tf.policy_year AS POLICY_YEAR,
            CASE
                WHEN tf.credit_type IN ('FYC','FYC_W','FYC_W_DUPLICATE')
                THEN tf.credit_value
                ELSE 0
            END
            AS FYC,
            CASE
                WHEN tf.credit_type IN ('API','API_W','API_W_DUPLICATE')
                THEN tf.credit_value
                ELSE 0
            END
            AS API,
            CASE
                WHEN tf.credit_type IN ('SSCP','SSCP_W','SSCP_W_DUPLICATE')
                THEN tf.credit_value
                ELSE 0
            END
            AS SSC,
            CASE
                WHEN tf.credit_type IN ('RYC','RYC_W','RYC_W_DUPLICATE','ORYC_W','ORYC_W_DUPLICATE')
                THEN tf.credit_value
                ELSE 0
            END
            AS RYC,
            CASE
                WHEN tf.clawback_name IN ('FYO_FA','FYO_FA_ONG'
                    /* --verstion 13 Harm_Phase4 Strat */
                    ,'FA_FYO_2.1','FA_FYO_ONG_2.1'
                    /* --verstion 13 Harm_Phase4 End */
                )
                THEN tf.pm_contribution_value
                ELSE 0
            END
            AS FYO,
            CASE
                WHEN tf.clawback_name IN ('RYO_FA','RYO_FA_ONG'
                    /* --verstion 13 Harm_Phase4 Start */
                    ,'FA_RYO_2.1','FA_RYO_ONG_2.1'
                    /* --verstion 13 Harm_Phase4 End */
                )
                THEN /* --version 11 */
                tf.pm_contribution_value
                ELSE 0
            END
            AS RYO,
            CASE
                WHEN tf.clawback_name IN ('FSM_RYO_FA','FSM_RYO_ONG_FA')
                AND pm_name = 'PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY'
                THEN -1 *tf.pm_contribution_value
                WHEN tf.clawback_name IN ('FSM_RYO_FA','FSM_RYO_ONG_FA')
                THEN tf.pm_contribution_value
                ELSE 0
            END
            AS FSM_RYO,
            CASE
                WHEN tf.clawback_name = 'NADOR_FA_2.1'
                THEN tf.pm_contribution_value
                ELSE 0
            END
            AS NADOR,
            CASE
                WHEN tf.clawback_name IN ('NLPI_FA','NLPI_ONG_FA')
                AND pm_name IN ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW')
                THEN -1*tf.pm_contribution_value
                WHEN tf.clawback_name IN ('NLPI','NLPI_ONG')
                THEN tf.pm_contribution_value
                ELSE 0
            END
            AS NLPI,
            0 AS SPI,
            CASE
                WHEN tf.clawback_name IN ('NLPI_FA','NLPI_ONG_FA')
                AND pm_name IN ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW')
                THEN -1*tf.pm_contribution_value
                WHEN tf.clawback_name IN ('FSM_RYO_FA','FSM_RYO_ONG_FA')
                AND pm_name = 'PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY'
                THEN -1 *tf.pm_contribution_value
                ELSE tf.pm_contribution_value
            END
            *0.60 AS SVI,
            CASE
                WHEN tf.clawback_name IN ('NLPI_FA','NLPI_ONG_FA')
                AND pm_name IN ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW')
                THEN -1*tf.pm_contribution_value
                WHEN tf.clawback_name IN ('FSM_RYO_FA','FSM_RYO_ONG_FA')
                AND pm_name = 'PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY'
                THEN -1 *tf.pm_contribution_value
                ELSE tf.pm_contribution_value
            END
            *0.60* /*ba.entitlementpercent version 7*/ IFNULL(ldr.ENTITLEMENT,ba.entitlementpercent) AS ENTITLEMENT,  /* ORIGSQL: nvl(ldr.ENTITLEMENT,ba.entitlementpercent) */
            ROUND(((
                        CASE   /* ORIGSQL: round(((CASE WHEN tf.clawback_name in ('NLPI_FA','NLPI_ONG_FA') and pm_name in ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW') THEN -1*tf.pm_contribution_value WHEN tf.clawback_name in ('FSM_RYO(...) */
                            WHEN tf.clawback_name IN ('NLPI_FA','NLPI_ONG_FA')
                            AND pm_name IN ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW')
                            THEN -1*tf.pm_contribution_value
                            WHEN tf.clawback_name IN ('FSM_RYO_FA','FSM_RYO_ONG_FA')
                            AND pm_name = 'PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY'
                            THEN -1 *tf.pm_contribution_value
                            ELSE tf.pm_contribution_value
                        END
                    *0.60) -
                    (
                        CASE
                            WHEN tf.clawback_name IN ('NLPI_FA','NLPI_ONG_FA')
                            AND pm_name IN ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW')
                            THEN -1*tf.pm_contribution_value
                            WHEN tf.clawback_name IN ('FSM_RYO_FA','FSM_RYO_ONG_FA')
                            AND pm_name = 'PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY'
                            THEN -1 *tf.pm_contribution_value
                            ELSE tf.pm_contribution_value
                        END
                        *0.60* /*ba.entitlementpercent version 7*/ IFNULL(ldr.ENTITLEMENT,ba.entitlementpercent)))* (-1),2) AS CLAWBACK_VALUE,  /* ORIGSQL: nvl(ldr.ENTITLEMENT,ba.entitlementpercent) */
            0 AS PROCESSED_CLAWBACK,
            tf.base_rider_ind AS BASIC_RIDER_IND,
            tf.salestransactionseq,
            tf.creditseq,
            tf.pm_seq,
            :P_BATCH_NO,
            pos_agy_rcr.GENERICATTRIBUTE2 AS RCVR_AGY_LDR_CODE,
            pos_agy_rcr.genericattribute11 AS RCVR_AGY_LDR_RANK,
            CASE rul.EXPRESSIONTYPEFORTYPE
                WHEN 256
                THEN 'DIRECT'
                WHEN 1024
                THEN 'INDIRECT'
                ELSE '0'
            END
            AS REPORT_TYPE,
            /* --Added by Suresh */
            0 AS OFFSET_CLAWBACK,
            CASE
                WHEN tf.clawback_name IN ('NEW_FYO_FA','NEW_FYO_ONG_FA')
                THEN tf.pm_contribution_value
                ELSE 0
            END
            AS NEW_FYO,
            CASE
                WHEN tf.clawback_name IN ('NEW_RYO_FA','NEW_RYO_ONG_FA')
                THEN tf.pm_contribution_value
                ELSE 0
            END
            AS NEW_RYO,
            /* --End by Suresh */
            /* --add AI NL20180308 */
            CASE
                /* --verstion 13 start, changed from AI_ONG to FA_AI_ONG_2.1 as the AI_ONG is useless */
                WHEN tf.clawback_name IN ('FA_AI_2.1','FA_AI_ONG_2.1')
                THEN /* --verstion 13 end */
                tf.credit_value
                ELSE 0
            END
            AS AI,
            NULL AS YTD_PERIOD /* --Vesion 13 add by Amanda */
        FROM
            EXT.AIA_CB_TRACE_FORWARD_COMP tf
        INNER JOIN
            EXT.AIA_cb_bsc_agent ba
            ON tf.calculation_period = (IFNULL(ba.quarter,'') || ' '|| IFNULL(ba.year,''))
            AND (tf.payor_code = ba.agentcode
                OR ba.agentcode = tf.new_agent_cd
            OR ba.agentcode = tf.old_agent_cd)
        INNER JOIN
            CS_CREDIT cr
            ON tf.creditseq = cr.creditseq
            AND tf.periodseq = cr.periodseq -- Added by Sundeep
        INNER JOIN
            cs_rule rul
            ON cr.ruleseq = rul.ruleseq
            AND rul.REMOVEDATE = :DT_REMOVEDATE
            AND rul.islast = 1
        INNER JOIN
            cs_position pos_agy_rcr
            ON pos_agy_rcr.ruleelementownerseq = cr.positionseq
            AND pos_agy_rcr.tenantid = 'AIAS' -- Added by Sundeep
            /*AND pos_agy_rcr.removedate = DT_REMOVEDATE
            and pos_agy_rcr.islast = 1 */
            AND pos_agy_rcr.effectivestartdate <= tf.CRD_GENERICDATE2
            AND pos_agy_rcr.effectiveenddate > tf.CRD_GENERICDATE2
            AND pos_agy_rcr.removedate = :DT_REMOVEDATE
        INNER JOIN
            cs_position Agent
            ON Agent.name = 'SGT'||IFNULL(tf.payor_code,'')
            AND Agent.tenantid = 'AIAS' -- Added by Sundeep
            AND Agent.effectivestartdate <= tf.CRD_GENERICDATE2
            AND Agent.effectiveenddate > tf.CRD_GENERICDATE2
            AND Agent.removedate = :DT_REMOVEDATE
            -- and AGENT.GENERICATTRIBUTE6='AFA'
        INNER JOIN
            cs_participant Agent_name
            ON Agent.payeeseq = Agent_name.payeeseq
            AND Agent_name.tenantid = 'AIAS' -- Added by Sundeep
            AND Agent_name.effectivestartdate <= tf.CRD_GENERICDATE2
            AND Agent_name.effectiveenddate > tf.CRD_GENERICDATE2
            AND Agent_name.removedate = :DT_REMOVEDATE
        INNER JOIN
            cs_position Agency_code
            ON 'SGY'||IFNULL(agent.genericattribute1,'') = Agency_code.name
            AND Agency_code.tenantid = 'AIAS' -- Added by Sundeep
            AND Agency_code.effectivestartdate <= tf.CRD_GENERICDATE2
            AND Agency_code.effectiveenddate > tf.CRD_GENERICDATE2
            AND Agency_code.removedate = :DT_REMOVEDATE
        INNER JOIN
            cs_participant Agency_name
            ON Agency_code.payeeseq = Agency_name.payeeseq
            AND Agency_name.effectivestartdate <= tf.CRD_GENERICDATE2
            AND Agency_name.effectiveenddate > tf.CRD_GENERICDATE2
            AND Agency_name.removedate = :DT_REMOVEDATE
        INNER JOIN
            cs_position DM_code
            --on 'SGY'||agent.genericattribute3 = DM_code.name
            ON 'SGY'||IFNULL(Agency_code.genericattribute3,'') = DM_code.name
            AND DM_code.tenantid = 'AIAS' -- Added by Sundeep
            AND DM_code.effectivestartdate <= tf.CRD_GENERICDATE2
            AND DM_code.effectiveenddate > tf.CRD_GENERICDATE2
            AND DM_code.removedate = :DT_REMOVEDATE
        INNER JOIN
            cs_participant District_name
            ON dm_code.payeeseq = district_name.payeeseq
            AND District_name.tenantid = 'AIAS' -- Added by Sundeep
            AND District_name.effectivestartdate <= tf.CRD_GENERICDATE2
            AND District_name.effectiveenddate > tf.CRD_GENERICDATE2
            AND District_name.removedate = :DT_REMOVEDATE
        INNER JOIN
            cs_title title_agt
            ON title_agt.RULEELEMENTOWNERSEQ = Agent.TITLESEQ
            AND title_agt.effectivestartdate <= tf.CRD_GENERICDATE2
            AND title_agt.effectiveenddate > tf.CRD_GENERICDATE2
            AND title_agt.REMOVEDATE = :DT_REMOVEDATE
            --version 7 add
        LEFT OUTER JOIN
            EXT.AIA_CB_BSC_LEADER_TMP ldr
            ON tf.calculation_period = (IFNULL(ldr.quarter,'') || ' '|| IFNULL(ldr.year,''))
            AND (tf.payee_code = ldr.LEADER_CODE
            OR tf.payee_code = ldr.LEADER_AGENCY) --for ongoing, payee code is agency code get from measurement
            AND tf.payor_code = ldr.FSC_CODE
            --version 7 end
            --chaned by Suresh
            --add AI NL20180308
        WHERE
            tf.clawback_name IN ('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION'
                --version13 Harm_Phase4 Start
                ,'FA_FYO_2.1'
                ,'FA_FYO_ONG_2.1'
                ,'FA_RYO_2.1'
                ,'FA_RYO_ONG_2.1'
                ,'NADOR_FA_2.1'
                ,'FA_AI_2.1'
                ,'FA_AI_ONG_2.1'
                --version13  Harm Phase4 End
            )
            --End by Suresh
            AND tf.batch_number = :P_BATCH_NO;

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_CLAWBACK_COMP FA for FYO, RYO, NADOR' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('insert into EXT.AIA_CB_CLAWBACK_COMP FA for FYO, RYO, NADOR'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    --get clawback type and clawback name, only LUMPSUM case will apply this logic

    V_CB_TYPE = fn_get_cb_type(:P_BATCH_NO);

    --V_CB_NAME := fn_get_cb_name(P_BATCH_NO);

    V_CB_QTR = fn_get_cb_quarter(:P_BATCH_NO);

    IF :V_CB_TYPE = :STR_LUMPSUM
    THEN
        --get previous quarter batch number
        --V_BATCH_NO_PRE_QTR := fn_get_batch_no_pre_qtr(P_BATCH_NO);

        --Version 2 add by Amanda begin
        --only same year need to update SPI CB for second QTR
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into EXT.AIA_CB_SPI_CLAWBACK cb2 using (SELECT cb.BUNAME, cb.PONUMBER, cb.WRI_AGT_CODE, cb.YTD_PIB, cb.YTD_SPI_CB, cb.YEAR, cb.quarter, cb.CLAWBACK_TYPE, cb.LIFE_NUMBER, cb.COVERAGE_NUMBER, cb.RIDER(...) */
        MERGE INTO EXT.AIA_CB_SPI_CLAWBACK AS cb2
            USING
            (
                SELECT   /* ORIGSQL: (select cb.BUNAME, cb.PONUMBER, cb.WRI_AGT_CODE, cb.YTD_PIB, cb.YTD_SPI_CB, cb.YEAR, cb.quarter, cb.CLAWBACK_TYPE, cb.LIFE_NUMBER, cb.COVERAGE_NUMBER, cb.RIDER_NUMBER, cb.COMPONENT_CODE from EXT.AIA_CB_SP(...) */
                    cb.BUNAME,
                    cb.PONUMBER,
                    cb.WRI_AGT_CODE,
                    cb.YTD_PIB,
                    cb.YTD_SPI_CB,
                    cb.YEAR,
                    cb.quarter,
                    cb.CLAWBACK_TYPE,
                    cb.LIFE_NUMBER,
                    cb.COVERAGE_NUMBER,
                    cb.RIDER_NUMBER,
                    cb.COMPONENT_CODE
                FROM
                    EXT.AIA_CB_SPI_CLAWBACK cb
                WHERE
                    cb.BUNAME = :STR_BUNAME_FA
                    AND cb.CLAWBACK_TYPE = 'LUMPSUM'
                    AND cb.year = SUBSTRING(:V_First_QTR,1,4)  /* ORIGSQL: substr(V_First_QTR,1,4) */
                    AND cb.quarter = 'Q'||IFNULL(SUBSTRING(:V_First_QTR,6,1),'')  /* ORIGSQL: substr(V_First_QTR,6,1) */
            ) AS cb1
            ON (cb2.BUNAME = cb1.BUNAME
                AND cb1.WRI_AGT_CODE = cb2.WRI_AGT_CODE
                AND cb1.PONUMBER = cb2.PONUMBER
                AND cb1.CLAWBACK_TYPE = cb2.CLAWBACK_TYPE
                AND cb1.LIFE_NUMBER = cb2.LIFE_NUMBER
                AND cb1.COVERAGE_NUMBER = cb2.COVERAGE_NUMBER
                AND cb1.RIDER_NUMBER = cb2.RIDER_NUMBER
                AND cb1.COMPONENT_CODE = cb2.COMPONENT_CODE
            AND cb1.year = cb2.year
            AND cb2.BUNAME = :STR_BUNAME_FA
            AND cb2.CLAWBACK_TYPE = 'LUMPSUM'
            AND cb2.year = SUBSTRING(:V_Second_QTR,1,4)  /* ORIGSQL: substr(V_Second_QTR,1,4) */
            AND cb2.quarter = 'Q'||IFNULL(SUBSTRING(:V_Second_QTR,6,1),'')  /* ORIGSQL: substr(V_Second_QTR,6,1) */
            )
        WHEN MATCHED THEN
            UPDATE
                SET --cb2.YTD_PIB = cb2.YTD_PIB + cb1.YTD_PIB,
                cb2.YTD_SPI_CB =
                CASE
                    WHEN cb2.SPI_RATE = 0
                    THEN cb1.YTD_SPI_CB
                    ELSE cb2.YTD_SPI_CB
                END
                ,--special handle for new agent in Q1, not new agent in Q2
                cb2.SPI_CB = (
                    CASE
                        WHEN cb2.SPI_RATE = 0
                        THEN cb1.YTD_SPI_CB
                        ELSE cb2.YTD_SPI_CB
                    END
                ) - cb1.YTD_SPI_CB;
               

        /* ORIGSQL: EXT.Log('Merge into EXT.AIA_CB_SPI_CLAWBACK FA LUMPSUM 2nd QTR ' || V_Second_QTR || '; row count: ' || to_char(sql%rowcount)) */
        CALL EXT.Log('Merge into EXT.AIA_CB_SPI_CLAWBACK FA LUMPSUM 2nd QTR '|| IFNULL(TO_VARCHAR(:V_Second_QTR),'') || '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
            );  /* ORIGSQL: to_char(sql%rowcount) */

        /* ORIGSQL: commit; */
        COMMIT;

        --Version 2 end

        /* ORIGSQL: insert into EXT.AIA_CB_CLAWBACK_SVI_COMP_TMP select curr_cc.*, P_BATCH_NO from (SELECT wri_dist_code, wri_agy_code, wri_agt_code, ponumber, life_number, coverage_number, rider_number, component_code, prod(...) */
        INSERT INTO EXT.AIA_CB_CLAWBACK_SVI_COMP_TMP
            SELECT   /* ORIGSQL: select curr_cc.*, P_BATCH_NO from (SELECT wri_dist_code, wri_agy_code, wri_agt_code, ponumber, life_number, coverage_number, rider_number, component_code, product_name, clawback_name, SUM(clawback)AS (...) */
                curr_cc.*,
                :P_BATCH_NO
            FROM
                (
                    SELECT   /* ORIGSQL: (select wri_dist_code, wri_agy_code, wri_agt_code, ponumber, life_number, coverage_number, rider_number, component_code, product_name, clawback_name, SUM(clawback) as clawback from EXT.AIA_CB_CLAWBACK_COM(...) */
                        wri_dist_code,
                        wri_agy_code,
                        wri_agt_code,
                        ponumber,
                        life_number,
                        coverage_number,
                        rider_number,
                        component_code,
                        product_name,
                        clawback_name,
                        SUM(clawback)AS clawback
                    FROM
                        EXT.AIA_CB_CLAWBACK_COMP
                    WHERE
                        clawback_type = :STR_LUMPSUM
                        AND clawback_method = :STR_CB_NAME
                        AND batch_no = :P_BATCH_NO
                    GROUP BY
                        wri_dist_code,
                        wri_agy_code,
                        wri_agt_code,
                        ponumber,
                        life_number,
                        coverage_number,
                        rider_number,
                        component_code,
                        product_name,
                        clawback_name
                    HAVING
                        SUM(clawback) > 0
                ) AS curr_cc
            LEFT OUTER JOIN
                (
                    SELECT   /* ORIGSQL: (select cc.wri_dist_code, cc.wri_agy_code, cc.wri_agt_code, cc.ponumber, cc.life_number, cc.coverage_number, cc.rider_number, cc.component_code, cc.product_name, cc.clawback_name, SUM(cc.processed_cla(...) */
                        cc.wri_dist_code,
                        cc.wri_agy_code,
                        cc.wri_agt_code,
                        cc.ponumber,
                        cc.life_number,
                        cc.coverage_number,
                        cc.rider_number,
                        cc.component_code,
                        cc.product_name,
                        cc.clawback_name,
                        /* --processed_clawback value should be updated after pipeline compeleted */
                        SUM(cc.processed_clawback) AS processed_clawback
                    FROM
                        EXT.AIA_CB_CLAWBACK_COMP cc
                    INNER JOIN
                        (
                            SELECT   /* ORIGSQL: (select nvl(max(t.batchnum), 0) as batch_no from EXT.AIA_cb_batch_status t inner join (SELECT distinct quarter, year, cb_startdate, cb_enddate FROM EXT.AIA_cb_period where cb_name = STR_CB_NAME and BUNAME=STR(...) */
                                IFNULL(MAX(t.batchnum), 0) AS batch_no
                            FROM
                                EXT.AIA_cb_batch_status t
                            INNER JOIN
                                (
                                    SELECT   /* ORIGSQL: (select distinct quarter, year, cb_startdate, cb_enddate from EXT.AIA_cb_period where cb_name = STR_CB_NAME and BUNAME=STR_BUNAME_FA) */
                                        DISTINCT
                                        quarter,
                                        year,
                                        cb_startdate,
                                        cb_enddate
                                    FROM
                                        EXT.AIA_cb_period
                                    WHERE
                                        cb_name = :STR_CB_NAME
                                        AND BUNAME = :STR_BUNAME_FA
                                ) AS cbp
                                ON t.cb_quarter_name = IFNULL(cbp.year,'') || ' '|| IFNULL(cbp.quarter,'')
                            WHERE
                                t.islatest = 'Y'
                                AND t.status = :STR_STATUS_COMPLETED_SH
                                AND t.clawbackname = :STR_CB_NAME
                                AND t.clawbacktype = :STR_LUMPSUM
                                AND t.cb_quarter_name <> :V_CB_QTR
                                AND TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE) >= cbp.cb_enddate
                            GROUP BY
                                t.cb_quarter_name, t.clawbackname, t.clawbacktype
                        ) AS pre_batch
                        ON cc.batch_no = pre_batch.batch_no
                    WHERE
                        cc.clawback_type = :STR_LUMPSUM
                        AND cc.clawback_method = :STR_CB_NAME
                    GROUP BY
                        cc.wri_dist_code,
                        cc.wri_agy_code,
                        cc.wri_agt_code,
                        cc.ponumber,
                        cc.life_number,
                        cc.coverage_number,
                        cc.rider_number,
                        cc.component_code,
                        cc.product_name,
                        cc.clawback_name
                    HAVING
                        SUM(cc.processed_clawback) < 0
                ) AS pre_cc
                ON curr_cc.wri_dist_code = pre_cc.wri_dist_code
                AND curr_cc.wri_agy_code = pre_cc.wri_agy_code
                AND curr_cc.wri_agt_code = pre_cc.wri_agt_code
                AND curr_cc.ponumber = pre_cc.ponumber
                AND curr_cc.life_number = pre_cc.life_number
                AND curr_cc.coverage_number = pre_cc.coverage_number
                AND curr_cc.rider_number = pre_cc.rider_number
                AND curr_cc.component_code = pre_cc.component_code
                AND curr_cc.product_name = pre_cc.product_name
                AND curr_cc.clawback_name = pre_cc.clawback_name
            WHERE
                pre_cc.ponumber IS NULL;

        /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_CLAWBACK_SVI_COMP_TMP FA for Compensation Lumpsum' || '; row count: ' || to_char(sql%rowcount)) */
        CALL EXT.Log('insert into EXT.AIA_CB_CLAWBACK_SVI_COMP_TMP FA for Compensation Lumpsum'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
            );  /* ORIGSQL: to_char(sql%rowcount) */

        /* ORIGSQL: commit; */
        COMMIT;
    ELSEIF :V_CB_TYPE = :STR_ONGOING  /* ORIGSQL: elsif V_CB_TYPE = STR_ONGOING then */
    THEN
        /* ORIGSQL: insert into EXT.AIA_CB_CLAWBACK_SVI_COMP_TMP select curr_cc.*, P_BATCH_NO from (SELECT wri_dist_code, wri_agy_code, wri_agt_code, ponumber, life_number, coverage_number, rider_number, component_code, prod(...) */
        INSERT INTO EXT.AIA_CB_CLAWBACK_SVI_COMP_TMP
            SELECT   /* ORIGSQL: select curr_cc.*, P_BATCH_NO from (SELECT wri_dist_code, wri_agy_code, wri_agt_code, ponumber, life_number, coverage_number, rider_number, component_code, product_name, clawback_name, SUM(clawback)AS (...) */
                curr_cc.*,
                :P_BATCH_NO
            FROM
                (
                    SELECT   /* ORIGSQL: (select wri_dist_code, wri_agy_code, wri_agt_code, ponumber, life_number, coverage_number, rider_number, component_code, product_name, clawback_name, SUM(clawback) as clawback from EXT.AIA_CB_CLAWBACK_COM(...) */
                        wri_dist_code,
                        wri_agy_code,
                        wri_agt_code,
                        ponumber,
                        life_number,
                        coverage_number,
                        rider_number,
                        component_code,
                        product_name,
                        clawback_name,
                        SUM(clawback)AS clawback
                    FROM
                        EXT.AIA_CB_CLAWBACK_COMP
                    WHERE
                        clawback_type = :STR_ONGOING
                        AND clawback_method = :STR_CB_NAME
                        AND batch_no = :P_BATCH_NO
                    GROUP BY
                        wri_dist_code,
                        wri_agy_code,
                        wri_agt_code,
                        ponumber,
                        life_number,
                        coverage_number,
                        rider_number,
                        component_code,
                        product_name,
                        clawback_name
                    HAVING
                        SUM(clawback) > 0
                ) AS curr_cc
            LEFT OUTER JOIN
                (
                    SELECT   /* ORIGSQL: (select cc.wri_dist_code, cc.wri_agy_code, cc.wri_agt_code, cc.ponumber, cc.life_number, cc.coverage_number, cc.rider_number, cc.component_code, cc.product_name, cc.clawback_name, SUM(cc.processed_cla(...) */
                        cc.wri_dist_code,
                        cc.wri_agy_code,
                        cc.wri_agt_code,
                        cc.ponumber,
                        cc.life_number,
                        cc.coverage_number,
                        cc.rider_number,
                        cc.component_code,
                        cc.product_name,
                        cc.clawback_name,
                        /* --processed_clawback value should be updated after pipeline compeleted */
                        SUM(cc.processed_clawback) AS processed_clawback
                    FROM
                        EXT.AIA_CB_CLAWBACK_COMP cc
                    INNER JOIN
                        (
                            --lumpsum batch number

                            SELECT   /* ORIGSQL: (select nvl(max(t.batchnum), 0) as batch_no from EXT.AIA_cb_batch_status t inner join (SELECT distinct quarter, year, cb_startdate, cb_enddate FROM EXT.AIA_cb_period where cb_name = STR_CB_NAME and BUNAME=STR(...) */
                                IFNULL(MAX(t.batchnum), 0) AS batch_no
                            FROM
                                EXT.AIA_cb_batch_status t
                            INNER JOIN
                                (
                                    SELECT   /* ORIGSQL: (select distinct quarter, year, cb_startdate, cb_enddate from EXT.AIA_cb_period where cb_name = STR_CB_NAME and BUNAME=STR_BUNAME_FA) */
                                        DISTINCT
                                        quarter,
                                        year,
                                        cb_startdate,
                                        cb_enddate
                                    FROM
                                        EXT.AIA_cb_period
                                    WHERE
                                        cb_name = :STR_CB_NAME
                                        AND BUNAME = :STR_BUNAME_FA
                                ) AS cbp
                                ON t.cb_quarter_name = IFNULL(cbp.year,'') || ' '|| IFNULL(cbp.quarter,'')
                            WHERE
                                t.islatest = 'Y'
                                AND t.BUNAME = :STR_BUNAME_FA
                                AND t.status = :STR_STATUS_COMPLETED_SH
                                AND t.clawbackname = :STR_CB_NAME
                                AND t.clawbacktype = :STR_LUMPSUM
                                AND t.cb_quarter_name <> :V_CB_QTR
                                AND TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE) >= cbp.cb_enddate
                            GROUP BY
                                t.cb_quarter_name, t.clawbackname, t.clawbacktype
                    UNION
                        --on-going batch number

                        SELECT   /* ORIGSQL: select nvl(max(t.batchnum), 0) as batch_no from EXT.AIA_cb_batch_status t where t.islatest = 'Y' and t.BUNAME=STR_BUNAME_FA and t.status = STR_STATUS_COMPLETED_SH and t.clawbackname = STR_CB_NAME and t.cl(...) */
                            IFNULL(MAX(t.batchnum), 0) AS batch_no
                        FROM
                            EXT.AIA_cb_batch_status t
                        WHERE
                            t.islatest = 'Y'
                            AND t.BUNAME = :STR_BUNAME_FA
                            AND t.status = :STR_STATUS_COMPLETED_SH --'completed_sh'
                            AND t.clawbackname = :STR_CB_NAME--'COMMISSION'
                            AND t.clawbacktype = :STR_ONGOING --'ONGOING'
                            AND TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE) > t.cycledate
                    ) AS pre_batch
                    ON cc.batch_no = pre_batch.batch_no
                WHERE
                    cc.clawback_method = :STR_CB_NAME
                GROUP BY
                    cc.wri_dist_code,
                    cc.wri_agy_code,
                    cc.wri_agt_code,
                    cc.ponumber,
                    cc.life_number,
                    cc.coverage_number,
                    cc.rider_number,
                    cc.component_code,
                    cc.product_name,
                    cc.clawback_name
                HAVING
                    SUM(cc.processed_clawback) < 0
            ) AS pre_cc
            ON curr_cc.wri_dist_code = pre_cc.wri_dist_code
            AND curr_cc.wri_agy_code = pre_cc.wri_agy_code
            AND curr_cc.wri_agt_code = pre_cc.wri_agt_code
            AND curr_cc.ponumber = pre_cc.ponumber
            AND curr_cc.life_number = pre_cc.life_number
            AND curr_cc.coverage_number = pre_cc.coverage_number
            AND curr_cc.rider_number = pre_cc.rider_number
            AND curr_cc.component_code = pre_cc.component_code
            AND curr_cc.product_name = pre_cc.product_name
            AND curr_cc.clawback_name = pre_cc.clawback_name
        WHERE
            pre_cc.ponumber IS NULL;

        /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_CLAWBACK_SVI_COMP_TMP FA for Compensation Ongoing' || '; row count: ' || to_char(sql%rowcount)) */
        CALL EXT.Log('insert into EXT.AIA_CB_CLAWBACK_SVI_COMP_TMP FA for Compensation Ongoing'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
            );  /* ORIGSQL: to_char(sql%rowcount) */

        /* ORIGSQL: commit; */
        COMMIT;

        --Version 2 added by Amanda begin
        --Fix no data found issue
        /*V_REC_COUNT := 0;
        
        --Check quarter end
        select count(1)
        into V_REC_COUNT
        from cs_period csp
         inner join cs_period csp_qtr on csp_qtr.periodtypeseq = V_periodtype_quarter_seq
         and csp_qtr.enddate = csp.enddate
         and csp_qtr.removedate =  DT_REMOVEDATE
         where csp.enddate = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) + 1
         and csp.removedate =  DT_REMOVEDATE
         and csp.calendarseq = V_CALENDARSEQ
         and csp.periodtypeseq = V_periodtype_month_seq;
        
        if V_REC_COUNT > 0 then
          --get current quarter name
          select csp_qtr.name
          into V_Curr_QTR
          from cs_period csp
           inner join cs_period csp_qtr on csp_qtr.periodtypeseq = V_periodtype_quarter_seq
         and csp_qtr.enddate = csp.enddate
         and csp_qtr.removedate =  DT_REMOVEDATE
           where csp.enddate = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) + 1
         and csp.removedate =  DT_REMOVEDATE
         and csp.calendarseq = V_CALENDARSEQ
         and csp.periodtypeseq = V_periodtype_month_seq;
        
          --get previous quarter name
          select csp_qtr.name
          into V_Previous_QTR
          from cs_period csp
           inner join cs_period csp_qtr on csp_qtr.periodtypeseq = V_periodtype_quarter_seq
         and csp_qtr.enddate = csp.enddate
         and csp_qtr.removedate =  DT_REMOVEDATE
           where csp.enddate = add_months(to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) + 1,-3)
         and csp.removedate =  DT_REMOVEDATE
         and csp.calendarseq = V_CALENDARSEQ
         and csp.periodtypeseq = V_periodtype_month_seq;
        
          EXT.Log('Current quarter for FA SPI CB: ' || V_Curr_QTR || 'Previous quarter for FA SPI CB: ' || V_Previous_QTR);
        
          --update YTD SPI
          merge into EXT.AIA_CB_SPI_CLAWBACK cb2
          using(select cb.BUNAME, cb.PONUMBER, cb.WRI_AGT_CODE, cb.YTD_PIB, cb.YEAR, cb.quarter,
                        cb.LIFE_NUMBER, cb.COVERAGE_NUMBER, cb.RIDER_NUMBER, cb.COMPONENT_CODE
                      from EXT.AIA_CB_SPI_CLAWBACK cb
                     where cb.BUNAME = STR_BUNAME_FA
             and (cb.quarter || ' ' || cb.year) = V_Previous_QTR
                   ) cb1
          on (  cb2.BUNAME = cb1.BUNAME
             and cb1.WRI_AGT_CODE = cb2.WRI_AGT_CODE
             and cb1.PONUMBER = cb2.PONUMBER
             and cb1.LIFE_NUMBER = cb2.LIFE_NUMBER
             and cb1.COVERAGE_NUMBER = cb2.COVERAGE_NUMBER
             and cb1.RIDER_NUMBER = cb2.RIDER_NUMBER
             and cb1.COMPONENT_CODE = cb2.COMPONENT_CODE
         and cb1.year = cb2.year) --only same year to update YTD PIB
          when matched then update
          set cb2.YTD_PIB = cb2.YTD_PIB + cb1.YTD_PIB,
              cb2.SPI_CB = cb2.YTD_PIB + cb1.YTD_PIB --update here for value of PM_SPI_ONG_PIB_CB
          where cb2.BUNAME = STR_BUNAME_FA
         and cb2.CLAWBACK_TYPE = 'ONGOING'
         and (cb2.quarter || ' ' || cb2.year) = V_Curr_QTR;
        
          EXT.Log('Merge into EXT.AIA_CB_SPI_CLAWBACK ONGOING 2nd QTR ' || V_Curr_QTR || '; row count: ' || to_char(sql%rowcount));
          commit;
        
        else
          EXT.Log('Not quarter end for SPI FA CB:' || P_STR_CYCLEDATE);
        end if;*/
        --Version 2 end
    END IF;

    --Version 2 update EXT.AIA_CB_CLAWBACK_COMP clawback value begin
    --insert data into EXT.AIA_CB_CLAWBACK_COMP for compensation for SPI FA  
    /* ORIGSQL: insert into EXT.AIA_CB_CLAWBACK_COMP select tf.calculation_period as MEASUREMENT_QUARTER, tf.clawback_type as CLAWBACK_TYPE, tf.clawback_name as CLAWBACK_NAME, STR_CB_NAME as CLAWBACK_METHOD, TO_DATE(P_ST(...) */
    INSERT INTO EXT.AIA_CB_CLAWBACK_COMP
        SELECT   /* ORIGSQL: select tf.calculation_period as MEASUREMENT_QUARTER, tf.clawback_type as CLAWBACK_TYPE, tf.clawback_name as CLAWBACK_NAME, STR_CB_NAME as CLAWBACK_METHOD, TO_DATE(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE(...) */
            tf.calculation_period AS MEASUREMENT_QUARTER,
            tf.clawback_type AS CLAWBACK_TYPE,
            tf.clawback_name AS CLAWBACK_NAME,
            :STR_CB_NAME AS CLAWBACK_METHOD,
            TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE) AS CALCULATION_DATE,
            Agency_code.genericattribute3 AS WRI_DIST_CODE,
            TRIM(IFNULL(District_name.firstname,'')||' '||IFNULL(District_name.lastname,'')) AS WRI_DIST_NAME,
            DM_code.genericattribute2 AS WRI_DM_CODE,
            agent.genericattribute1 AS WRI_AGY_CODE,
            TRIM(IFNULL(Agency_name.firstname,'')||' '||IFNULL(Agency_name.lastname,'')) AS WRI_AGY_NAME,
            agent.genericattribute2 AS wri_agy_ldr_code,
            agent.genericattribute7 AS wri_agy_ldr_name,
            tf.payor_code AS WRI_AGT_CODE,
            TRIM(IFNULL(Agent_name.firstname,'')||' '||IFNULL(Agent_name.lastname,'')) AS wri_agt_name,
            MAP(Agent_name.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') AS FSC_TYPE,  /* ORIGSQL: decode(Agent_name.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') */
            title_agt.name AS RANK,
            agent.genericattribute4 AS UM_CLASS,
            agent.genericattribute11 AS UM_RANK,
            ba.bsc_grade AS FSC_BSC_GRADE,
            ba.entitlementpercent AS FSC_BSC_PERCENTAGE,
            tf.policy_number AS PONUMBER,
            tf.life_number AS LIFE_NUMBER,
            tf.coverage_number AS COVERAGE_NUMBER,
            tf.RIDER_NUMBER AS RIDER_NUMBER,
            tf.COMPONENT_NAME AS COMPONENT_NAME,
            tf.component_code AS COMPONENT_CODE,
            tf.PRODUCT_NAME AS PRODUCT_NAME,
            MAX(tf.transaction_date) AS TRANSACTION_DATE,
            MAX(tf.policy_year) AS POLICY_YEAR,
            SUM(
                CASE 
                    WHEN tf.credit_type IN ('FYC','FYC_W','FYC_W_DUPLICATE')
                    AND pm_name IN ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl')
                    THEN -1 * tf.credit_value
                    WHEN tf.credit_type IN ('FYC','FYC_W','FYC_W_DUPLICATE')
                    THEN tf.credit_value
                    ELSE 0
                END
            ) AS FYC,
            SUM(
                CASE 
                    WHEN tf.credit_type IN ('API','API_W','API_W_DUPLICATE')
                    THEN tf.credit_value
                    ELSE 0
                END
            ) AS API,
            SUM(
                CASE 
                    WHEN tf.credit_type IN ('SSCP','SSCP_W','SSCP_W_DUPLICATE')
                    THEN tf.credit_value
                    ELSE 0
                END
            ) AS SSC,
            0 AS RYC,
            0 AS FYO,
            0 AS RYO,
            0 AS FSM_RYO,
            0 AS NADOR,
            0 AS NLPI,
            SUM(
                CASE 
                    WHEN pm_name IN ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl')
                    THEN -1 * tf.pm_contribution_value
                    ELSE tf.pm_contribution_value
                END
            ) AS SPI,
            SUM(
                CASE 
                    WHEN pm_name IN ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl')
                    THEN -1 * tf.pm_contribution_value
                    ELSE tf.pm_contribution_value
                END
            * 0.6) AS SVI,
            SUM(
                CASE 
                    WHEN pm_name IN ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl')
                    THEN -1 * tf.pm_contribution_value
                    ELSE tf.pm_contribution_value
                END
            * 0.6 * ba.entitlementpercent) AS ENTITLEMENT,
            ROUND(SUM((
                        CASE
                            WHEN pm_name IN ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl')
                            THEN -1 * tf.pm_contribution_value
                            ELSE tf.pm_contribution_value
                        END
                        * 0.6 -
                        CASE
                            WHEN pm_name IN ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl')
                            THEN -1 * tf.pm_contribution_value
                            ELSE tf.pm_contribution_value
                        END
                        * 0.60 * ba.entitlementpercent) * (-1)),2) AS CLAWBACK_VALUE,
            0 AS PROCESSED_CLAWBACK,
            tf.base_rider_ind AS BASIC_RIDER_IND,
            MAX(tf.salestransactionseq),
            MAX(tf.creditseq),
            MAX(tf.pm_seq),
            :P_BATCH_NO,
            pos_agy_rcr.GENERICATTRIBUTE2 AS RCVR_AGY_LDR_CODE,
            pos_agy_rcr.genericattribute11 AS RCVR_AGY_LDR_RANK,
            CASE rul.EXPRESSIONTYPEFORTYPE
                WHEN 256
                THEN 'DIRECT'
                WHEN 1024
                THEN 'INDIRECT'
                ELSE '0'
            END
            AS REPORT_TYPE,
            0 AS OFFSET_CLAWBACK,
            0 AS NEW_FYO,
            0 AS NEW_RYO,
            0 AS AI,
            tf.YTD_MPERIOD  /* --Vesion 13 add by Amanda */
        FROM
            EXT.AIA_CB_TRACE_FORWARD_COMP tf
        INNER JOIN
            EXT.AIA_cb_bsc_agent ba
            ON tf.calculation_period = (IFNULL(ba.quarter,'') || ' '|| IFNULL(ba.year,''))
            AND ba.agentcode = (
                CASE
                    WHEN tf.OLD_AGENT_CD IS NOT NULL
                    THEN tf.OLD_AGENT_CD
                    ELSE tf.payor_code
                END
            ) -- version 17 Harm_BSC_SPI
        INNER JOIN
            cs_period csp
            ON csp.periodseq = tf.YTD_MPERIOD
            AND csp.removedate = :DT_REMOVEDATE
            AND csp.periodtypeseq = :V_periodtype_month_seq --month
            AND csp.calendarseq = :V_CALENDARSEQ
        INNER JOIN
            cs_period qtr
            ON csp.parentseq = qtr.periodseq
            AND qtr.removedate = :DT_REMOVEDATE
            AND qtr.calendarseq = :V_CALENDARSEQ
            AND qtr.periodtypeseq = :V_periodtype_quarter_seq --quarter
        INNER JOIN
            CS_CREDIT cr
            ON tf.creditseq = cr.creditseq
            AND tf.periodseq = cr.periodseq --Added by Sundeep
            AND cr.tenantid = 'AIAS' --Added by Sundeep
            AND cr.processingunitseq = 38280596832649218 --Added by Sundeep
        INNER JOIN
            cs_rule rul
            ON cr.ruleseq = rul.ruleseq
            AND rul.REMOVEDATE = :DT_REMOVEDATE
            AND rul.islast = 1
        INNER JOIN
            cs_position pos_agy_rcr
            ON pos_agy_rcr.ruleelementownerseq = cr.positionseq
            AND pos_agy_rcr.tenantid = 'AIAS' -- Added by Sundeep
            AND pos_agy_rcr.effectivestartdate <= TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND pos_agy_rcr.effectiveenddate > TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND pos_agy_rcr.removedate = :DT_REMOVEDATE
        INNER JOIN
            cs_position Agent
            ON Agent.name = 'SGT'||IFNULL(tf.payor_code,'')
            AND Agent.tenantid = 'AIAS' -- Added by Sundeep
            AND Agent.effectivestartdate <= TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND Agent.effectiveenddate > TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND Agent.removedate = :DT_REMOVEDATE
        INNER JOIN
            cs_participant Agent_name
            ON Agent.payeeseq = Agent_name.payeeseq
            AND Agent_name.tenantid = 'AIAS' -- Added by Sundeep
            AND Agent_name.effectivestartdate <= TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND Agent_name.effectiveenddate > TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND Agent_name.removedate = :DT_REMOVEDATE
        INNER JOIN
            cs_position Agency_code
            ON 'SGY'||IFNULL(agent.genericattribute1,'') = Agency_code.name
            AND Agency_code.tenantid = 'AIAS' -- Added by Sundeep
            AND Agency_code.effectivestartdate <= TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND Agency_code.effectiveenddate > TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND Agency_code.removedate = :DT_REMOVEDATE
        INNER JOIN
            cs_participant Agency_name
            ON Agency_code.payeeseq = Agency_name.payeeseq
            AND Agency_name.tenantid = 'AIAS' -- Added by Sundeep
            AND Agency_name.effectivestartdate <= TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND Agency_name.effectiveenddate > TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND Agency_name.removedate = :DT_REMOVEDATE
        INNER JOIN
            cs_position DM_code
            ON 'SGY'||IFNULL(Agency_code.genericattribute3,'') = DM_code.name
            AND DM_code.tenantid = 'AIAS' -- Added by Sundeep
            AND DM_code.effectivestartdate <= TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND DM_code.effectiveenddate > TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND DM_code.removedate = :DT_REMOVEDATE
        INNER JOIN
            cs_participant District_name
            ON dm_code.payeeseq = district_name.payeeseq
            AND District_name.tenantid = 'AIAS' -- Added by Sundeep
            AND District_name.effectivestartdate <= TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND District_name.effectiveenddate > TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND District_name.removedate = :DT_REMOVEDATE
        INNER JOIN
            cs_title title_agt
            ON title_agt.RULEELEMENTOWNERSEQ = Agent.TITLESEQ
            AND title_agt.effectivestartdate <= TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND title_agt.effectiveenddate > TO_DATE(:V_CYCLE_DATE,:STR_DATE_FORMAT_TYPE)
            AND title_agt.REMOVEDATE = :DT_REMOVEDATE
        WHERE
            tf.clawback_name IN ('SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1') -- version 17 Harm_BSC_SPI
            AND tf.batch_number = :P_BATCH_NO
            --Version 13 update by Amanda for AGY SPI exclusion begin
            AND tf.component_code NOT IN
            (
                SELECT   /* ORIGSQL: (select b.MINSTRING from CS_RELATIONALMDLT a inner join CS_MDLTIndex b on a.ruleelementseq = b.ruleelementseq and a.removedate = DT_REMOVEDATE and b.removedate = DT_REMOVEDATE and a.name = 'LT_SPI_Bon(...) */
                    b.MINSTRING
                FROM
                    CS_RELATIONALMDLT a
                INNER JOIN
                    CS_MDLTIndex b
                    ON a.ruleelementseq = b.ruleelementseq
                    AND a.removedate = :DT_REMOVEDATE
                    AND b.removedate = :DT_REMOVEDATE
                    AND a.name = 'LT_SPI_Bonus_Component_Excl'
            ) --Version 13 end
        GROUP BY
            tf.calculation_period,
            tf.clawback_type,
            tf.clawback_name,
            Agency_code.genericattribute3,
            TRIM(IFNULL(District_name.firstname,'')||' '||IFNULL(District_name.lastname,'')),
            DM_code.genericattribute2,
            agent.genericattribute1,
            TRIM(IFNULL(Agency_name.firstname,'')||' '||IFNULL(Agency_name.lastname,'')),
            agent.genericattribute2,
            agent.genericattribute7,
            tf.payor_code,
            TRIM(IFNULL(Agent_name.firstname,'')||' '||IFNULL(Agent_name.lastname,'')),
            MAP(Agent_name.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC'),  /* ORIGSQL: decode(Agent_name.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') */
            title_agt.name,
            agent.genericattribute4,
            agent.genericattribute11,
            ba.bsc_grade,
            ba.entitlementpercent,
            tf.policy_number,
            tf.life_number,
            tf.coverage_number,
            tf.RIDER_NUMBER,
            tf.COMPONENT_NAME,
            tf.component_code,
            qtr.name,
            tf.PRODUCT_NAME,
            tf.base_rider_ind,
            tf.YTD_MPERIOD,
            pos_agy_rcr.GENERICATTRIBUTE2,
            pos_agy_rcr.genericattribute11,
            rul.EXPRESSIONTYPEFORTYPE;

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_CLAWBACK_COMP FA for SPI FA' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('insert into EXT.AIA_CB_CLAWBACK_COMP FA for SPI FA'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into EXT.AIA_CB_CLAWBACK_COMP cc using (SELECT ROW_NUMBER() OVER (PARTITION BY cb.YEAR, cb.quarter, cb.PONUMBER, cb.WRI_AGT_CODE, cb.LIFE_NUMBER, cb.RIDER_NUMBER, cb.COVERAGE_NUMBER, cb.COMPONENT_CO(...) */
    MERGE INTO EXT.AIA_CB_CLAWBACK_COMP AS cc  
        USING
        (
            SELECT   /* ORIGSQL: (select ROW_NUMBER() OVER (PARTITION BY cb.YEAR, cb.quarter, cb.PONUMBER, cb.WRI_AGT_CODE, cb.LIFE_NUMBER, cb.RIDER_NUMBER, cb.COVERAGE_NUMBER, cb.COMPONENT_CODE ORDER BY cb.WRI_AGT_CODE desc) rk, cb.(...) */
                ROW_NUMBER() OVER (PARTITION BY
                    cb.YEAR, cb.quarter, cb.PONUMBER, cb.WRI_AGT_CODE,
                    cb.LIFE_NUMBER, cb.RIDER_NUMBER, cb.COVERAGE_NUMBER,
                cb.COMPONENT_CODE ORDER BY cb.WRI_AGT_CODE DESC) AS rk,
                cb.PONUMBER,
                cb.WRI_AGT_CODE,
                cb.SPI_CB,
                cb.YEAR,
                cb.quarter,
                cb.CLAWBACK_TYPE,
                cb.LIFE_NUMBER,
                cb.RIDER_NUMBER,
                cb.COVERAGE_NUMBER,
                cb.COMPONENT_CODE,
                csp.PERIODSEQ
            FROM
                EXT.AIA_CB_SPI_CLAWBACK cb
            LEFT OUTER JOIN
                cs_period qtr
                ON qtr.name = (IFNULL(cb.quarter,'') || ' '|| IFNULL(cb.year,''))
            LEFT OUTER JOIN
                cs_period csp
                ON csp.parentseq = qtr.periodseq
                AND csp.calendarseq = :V_CALENDARSEQ
                AND csp.periodtypeseq = :V_periodtype_month_seq
                AND csp.enddate = qtr.enddate
            WHERE
                cb.BUNAME = :STR_BUNAME_FA
                AND qtr.removedate = :DT_REMOVEDATE
                AND qtr.calendarseq = :V_CALENDARSEQ
                AND qtr.periodtypeseq = :V_periodtype_quarter_seq --quarter
        ) AS src
        ON (cc.WRI_AGT_CODE = src.WRI_AGT_CODE
            AND cc.PONUMBER = src.PONUMBER
            AND cc.CLAWBACK_TYPE = src.CLAWBACK_TYPE
            AND cc.LIFE_NUMBER = src.LIFE_NUMBER
            AND cc.COVERAGE_NUMBER = src.COVERAGE_NUMBER
            AND cc.RIDER_NUMBER = src.RIDER_NUMBER
            AND cc.COMPONENT_CODE = src.COMPONENT_CODE
        AND cc.YTD_PERIOD = src.PERIODSEQ
        AND src.rk = 1
        AND cc.CLAWBACK_NAME IN ('SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1') -- version 17 Harm_BSC_SPI
        AND cc.BATCH_NO = :P_BATCH_NO
        )
    WHEN MATCHED THEN
        UPDATE
            SET cc.CLAWBACK = src.SPI_CB
        ;

    /* ORIGSQL: EXT.Log('Merge into EXT.AIA_CB_CLAWBACK_COMP FA clawback value ' || V_Second_QTR || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('Merge into EXT.AIA_CB_CLAWBACK_COMP FA clawback value '|| IFNULL(TO_VARCHAR(:V_Second_QTR),'') || '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    --Version 2

    --update the table EXT.AIA_CB_CLAWBACK_COMMISSION for special handling for positive clawback
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into EXT.AIA_CB_CLAWBACK_COMP cc using EXT.AIA_CB_CLAWBACK_SVI_COMP_TMP st on (cc.wri_dist_code = st.wri_dist_code and cc.wri_agy_code = st.wri_agy_code and cc.wri_agt_code = st.wri_agt_code and cc.ponu(...) */
    MERGE INTO EXT.AIA_CB_CLAWBACK_COMP AS cc
        USING EXT.AIA_CB_CLAWBACK_SVI_COMP_TMP st
        ON (cc.wri_dist_code = st.wri_dist_code
            AND cc.wri_agy_code = st.wri_agy_code
            AND cc.wri_agt_code = st.wri_agt_code
            AND cc.ponumber = st.ponumber
            AND cc.life_number = st.life_number
            AND cc.coverage_number = st.coverage_number
            AND cc.rider_number = st.rider_number
            AND cc.component_code = st.component_code
            AND cc.product_name = st.product_name
            AND cc.clawback_name = st.clawback_name
            AND cc.batch_no = st.batch_no
            AND cc.batch_no = :P_BATCH_NO
        )
    WHEN MATCHED THEN
        UPDATE SET cc.clawback = 0;

    /* ORIGSQL: EXT.Log('merge into EXT.AIA_CB_CLAWBACK_COMP FA to handle positive clawback' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('merge into EXT.AIA_CB_CLAWBACK_COMP FA to handle positive clawback'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    -- version 17 Harm_BSC_SPI start
    -- update wri_agt_code when  wri_agt_code and comm_agt_code is old code but need pay to new code in migration quarter

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into EXT.AIA_CB_CLAWBACK_COMP cc using (SELECT distinct gapar.genericattribute4, par.userid, cc1.YTD_PERIOD, cc1.PONUMBER, cc1.TRANSACTION_DATE FROM cs_participant par inner join EXT.AIA_CB_CLAWBACK_COM(...) */
    MERGE INTO EXT.AIA_CB_CLAWBACK_COMP AS cc    
        USING
        (
            SELECT   /* ORIGSQL: (select distinct gapar.genericattribute4,par.userid,cc1.YTD_PERIOD,cc1.PONUMBER,cc1.TRANSACTION_DATE from cs_participant par inner join EXT.AIA_CB_CLAWBACK_COMP cc1 on par.userid = 'SGT'|| cc1.WRI_AGT_COD(...) */
                DISTINCT
                gapar.genericattribute4,
                par.userid,
                cc1.YTD_PERIOD,
                cc1.PONUMBER,
                cc1.TRANSACTION_DATE /* --,cp.enddate,par2.hiredate */
            FROM
                cs_participant par
            INNER JOIN
                EXT.AIA_CB_CLAWBACK_COMP cc1
                ON par.userid = 'SGT'|| IFNULL(cc1.WRI_AGT_CODE,'')
            INNER JOIN
                cs_gaparticipant gapar
                ON gapar.payeeseq = par.payeeseq
            INNER JOIN
                cs_period cp
                ON cp.periodseq = cc1.YTD_PERIOD
            INNER JOIN
                cs_participant par2
                ON par2.userid = 'SGT'|| IFNULL(gapar.genericattribute4,'')
            WHERE
                par.islast = 1
                AND par.removedate = :DT_REMOVEDATE
                AND gapar.effectiveenddate = :DT_REMOVEDATE
                AND gapar.removedate = :DT_REMOVEDATE
                AND gapar.genericattribute4 IS NOT NULL
                AND cp.removedate = :DT_REMOVEDATE
                AND par2.hiredate <= TO_DATE(ADD_SECONDS(cp.enddate,(86400*-1)))   /* ORIGSQL: cp.enddate -1 */
                AND cc1.clawback_name IN ('SPI_FA_2.1','SPI_FA_ONG_2.1')
                AND cc1.BATCH_NO = :P_BATCH_NO
                AND par2.removedate = :DT_REMOVEDATE
                AND par2.islast = 1
        ) AS tempcode
        ON (
            tempcode.YTD_PERIOD = cc.YTD_PERIOD
            AND tempcode.PONUMBER = cc.PONUMBER
            AND tempcode.TRANSACTION_DATE = cc.TRANSACTION_DATE
            AND cc.BATCH_NO = :P_BATCH_NO
        )
    WHEN MATCHED THEN
        UPDATE
            SET cc.WRI_AGT_CODE = tempcode.genericattribute4;

    /* ORIGSQL: EXT.Log('update EXT.AIA_CB_CLAWBACK_COMP payor code for SPI FA  2.1' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('update EXT.AIA_CB_CLAWBACK_COMP payor code for SPI FA  2.1'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: EXT.Log('SP_CLAWBACK_COMP_FA end') */
    CALL EXT.Log('SP_CLAWBACK_COMP_FA end');

    -- version 17 Harm_BSC_SPI end

    /* ORIGSQL: commit; */
    COMMIT;
END;
  PUBLIC PROCEDURE SP_EXEC_COMP_LUMPSUM_FA
(
    IN P_STR_CB_CYCLEDATE VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                         /* ORIGSQL: P_STR_CB_CYCLEDATE IN VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE V_LUMPSUM_FLAG DECIMAL(38,10);  /* ORIGSQL: V_LUMPSUM_FLAG NUMBER; */
    DECLARE V_BATCH_NO DECIMAL(38,10);  /* ORIGSQL: V_BATCH_NO NUMBER; */
    DECLARE v_message VARCHAR(2000);  /* ORIGSQL: v_message VARCHAR2(2000); */
    DECLARE V_CB_YEAR VARCHAR(20);  /* ORIGSQL: V_CB_YEAR VARCHAR2(20); */
    DECLARE V_CB_QUARTER VARCHAR(20);  /* ORIGSQL: V_CB_QUARTER VARCHAR2(20); */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            v_message = SUBSTRING(::SQL_ERROR_MESSAGE,1,2000);  /* ORIGSQL: SUBSTR(SQLERRM,1,2000) */

            /* ORIGSQL: EXT.Log(v_message) */
            CALL EXT.Log(:v_message);

            /* ORIGSQL: sp_update_batch_status(V_BATCH_NO, STR_STATUS_FAIL) */
            CALL sp_update_batch_status(:V_BATCH_NO, :STR_STATUS_FAIL);
        END;

        /* ORIGSQL: init */
        CALL init();

        SELECT
            COUNT(1) 
        INTO
            V_LUMPSUM_FLAG
        FROM
            EXT.AIA_CB_PERIOD
        WHERE
            CB_CYCLEDATE = TO_DATE(:P_STR_CB_CYCLEDATE,:STR_DATE_FORMAT_TYPE)
            AND cb_name = :STR_COMPENSATION
            AND buname = :STR_BUNAME_FA;

        /* ORIGSQL: EXT.Log('V_LUMPSUM_FLAG'||V_LUMPSUM_FLAG) */
        CALL EXT.Log('V_LUMPSUM_FLAG'||IFNULL(TO_VARCHAR(:V_LUMPSUM_FLAG),''));

        IF :V_LUMPSUM_FLAG > 0
        THEN
            --LUMPSUM
            /* ORIGSQL: sp_create_batch_no_fa(P_STR_CB_CYCLEDATE,STR_LUMPSUM,STR_COMPENSATION) */
            CALL sp_create_batch_no_FA(:P_STR_CB_CYCLEDATE, :STR_LUMPSUM, :STR_COMPENSATION);

            V_BATCH_NO = fn_get_batch_no_fa(:P_STR_CB_CYCLEDATE, :STR_COMPENSATION, :STR_LUMPSUM, :STR_STATUS_START);

            /* ORIGSQL: EXT.Log('V_BATCH_NO'||V_BATCH_NO) */
            CALL EXT.Log('V_BATCH_NO'||IFNULL(TO_VARCHAR(:V_BATCH_NO),''));

            /* ORIGSQL: SP_TRACE_FORWARD_COMP_FA (P_STR_CB_CYCLEDATE,STR_LUMPSUM, V_BATCH_NO) */
            CALL SP_TRACE_FORWARD_COMP_FA(:P_STR_CB_CYCLEDATE, :STR_LUMPSUM, :V_BATCH_NO);

            /* ORIGSQL: SP_CLAWBACK_COMP_FA (P_STR_CB_CYCLEDATE, V_BATCH_NO) */
            CALL SP_CLAWBACK_COMP_FA(:P_STR_CB_CYCLEDATE, :V_BATCH_NO);

            /* ORIGSQL: sp_update_batch_status (V_BATCH_NO, STR_STATUS_COMPLETED_SP) */
            CALL sp_update_batch_status(:V_BATCH_NO, :STR_STATUS_COMPLETED_SP);
        ELSE 
            /* ORIGSQL: EXT.Log(P_STR_CB_CYCLEDATE || ' is not the avaiable clawback cycle date for FA') */
            CALL EXT.Log(IFNULL(:P_STR_CB_CYCLEDATE,'') || ' is not the avaiable clawback cycle date for FA');
        END IF;
        ---catch exception
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
    END;
  PUBLIC PROCEDURE SP_EXEC_COMP_ONGOING_FA
(
    IN P_STR_CB_CYCLEDATE VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                         /* ORIGSQL: P_STR_CB_CYCLEDATE IN VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE V_STR_CB_TYPE VARCHAR(20);  /* ORIGSQL: V_STR_CB_TYPE VARCHAR2(20); */
    DECLARE V_BATCH_NO DECIMAL(38,10);  /* ORIGSQL: V_BATCH_NO NUMBER; */
    DECLARE V_WEEKEND_FLAG DECIMAL(38,10);  /* ORIGSQL: V_WEEKEND_FLAG NUMBER; */
    DECLARE V_MONTHEND_FLAG DECIMAL(38,10);  /* ORIGSQL: V_MONTHEND_FLAG NUMBER; */
    DECLARE V_MESSAGE VARCHAR(2000);  /* ORIGSQL: V_MESSAGE VARCHAR2(2000); */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            V_MESSAGE = SUBSTRING(::SQL_ERROR_MESSAGE,1,2000);  /* ORIGSQL: SUBSTR(SQLERRM,1,2000) */

            /* ORIGSQL: EXT.Log(v_message) */
            CALL EXT.Log(:V_MESSAGE);

            /* ORIGSQL: sp_update_batch_status(V_BATCH_NO, STR_STATUS_FAIL) */
            CALL sp_update_batch_status(:V_BATCH_NO, :STR_STATUS_FAIL);
        END;

        /* initialize library variables, if not yet done */
        CALL init_session_global();

        /* ORIGSQL: init */
        CALL init();

        ---to define the run type
        /*SELECT COUNT(1) INTO V_WEEKEND_FLAG FROM IN_ETL_CONTROL CTL
        WHERE CTL.TXT_KEY_STRING='PAYMENT_END_DATE_WEEKLY' AND CTL.TXT_FILE_NAME= STR_PU AND CTL.TXT_KEY_VALUE=P_STR_CB_CYCLEDATE;*/ 
        SELECT
            COUNT(1) 
        INTO
            V_MONTHEND_FLAG
        FROM
            CS_PERIOD CSP
        WHERE
            TO_DATE(ADD_SECONDS(CSP.ENDDATE,(86400*-1))) = TO_DATE(:P_STR_CB_CYCLEDATE,:STR_DATE_FORMAT_TYPE)  /* ORIGSQL: CSP.ENDDATE - 1 */
            AND CSP.CALENDARSEQ = :V_CALENDARSEQ
            AND CSP.PERIODTYPESEQ 
            =
            (
                SELECT   /* ORIGSQL: (select periodtypeseq from cs_periodtype where name = STR_CALENDAR_TYPE) */
                    periodtypeseq
                FROM
                    cs_periodtype
                WHERE
                    name = :STR_CALENDAR_TYPE
            )
            AND CSP.removedate = to_date('2200-01-01','yyyy-mm-dd');--Cosimo
        /* ORIGSQL: to_date('2200-01-01','yyyy-mm-dd') */

        IF :V_MONTHEND_FLAG > 0
        THEN
            --ONGOING
            /* ORIGSQL: sp_create_batch_no_fa(P_STR_CB_CYCLEDATE,STR_ONGOING,STR_COMPENSATION) */
            CALL sp_create_batch_no_FA(:P_STR_CB_CYCLEDATE, :STR_ONGOING, :STR_COMPENSATION);

            V_BATCH_NO = fn_get_batch_no_fa(:P_STR_CB_CYCLEDATE, :STR_COMPENSATION, :STR_ONGOING, :STR_STATUS_START);

            /* ORIGSQL: SP_TRACE_FORWARD_COMP_FA (P_STR_CB_CYCLEDATE,STR_ONGOING, V_BATCH_NO) */
            CALL SP_TRACE_FORWARD_COMP_FA(:P_STR_CB_CYCLEDATE, :STR_ONGOING, :V_BATCH_NO);

            /* ORIGSQL: SP_CLAWBACK_COMP_FA (P_STR_CB_CYCLEDATE, V_BATCH_NO) */
            CALL SP_CLAWBACK_COMP_FA(:P_STR_CB_CYCLEDATE, :V_BATCH_NO);

            /* ORIGSQL: sp_update_batch_status (V_BATCH_NO, STR_STATUS_COMPLETED_SP) */
            CALL sp_update_batch_status(:V_BATCH_NO, :STR_STATUS_COMPLETED_SP);
        END IF;
        ---catch exception
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
    END;
  PUBLIC PROCEDURE SP_CREDIT_COMMISSION_FA
(
    IN P_STR_CYCLEDATE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                       /* ORIGSQL: P_STR_CYCLEDATE IN VARCHAR2 */
    IN P_BATCH_NO_1 BIGINT,   /* ORIGSQL: P_BATCH_NO_1 IN INTEGER */
    IN P_BATCH_NO_2 BIGINT     /* ORIGSQL: P_BATCH_NO_2 IN INTEGER */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    --V_BATCH_NO INTEGER;
    DECLARE V_REC_COUNT BIGINT;  /* ORIGSQL: V_REC_COUNT INTEGER; */
    DECLARE V_GST_RATE DECIMAL(10,2);  /* ORIGSQL: V_GST_RATE NUMBER(10,2); */
    DECLARE vCreditOffset BIGINT;  /* ORIGSQL: vCreditOffset INTEGER; */
    DECLARE v_STR_CYCLEDATE_PERIODSEQ BIGINT := fn_get_periodseq(:P_STR_CYCLEDATE); --Deepan : added this since using function in select clause doesnt work
    DECLARE v_sql VARCHAR(5000);
    /* ORIGSQL: EXT.Log('SP_CREDIT_COMMISSION_FA start') */
    CALL EXT.Log('SP_CREDIT_COMMISSION_FA start');

    --get batch number
    --V_BATCH_NO := PK_EXT.AIA_CB_CALCULATION.fn_get_batch_no(P_STR_CYCLEDATE);

    --get the GST rate from TrueComp rate schedule table
    /*
    --v14
    
    select cell.value
      into V_GST_RATE
      from CS_RELATIONALMDLT RM
     inner join CS_MDLTCell cell
        on cell.mdltseq = RM.ruleelementseq
     where RM.name = 'LT_SG_GST'
     and RM.removedate = DT_REMOVEDATE
     and cell.removedate = DT_REMOVEDATE;
    */

    SELECT
        value
    INTO
        V_GST_RATE
    FROM
        ext.vw_lt_gst_rate
    WHERE
        TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE) >= effectivestartdate
        AND TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE) < effectiveenddate;

    --insert data into credit stage table

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_CREDIT_STG,' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2) */
    CALL EXT.Log('insert into EXT.AIA_CB_CREDIT_STG,'||' batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_1),'') || ' and '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_2),''));

    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_CREDITTYPE' not found */

    /* ORIGSQL: insert into EXT.AIA_CB_CREDIT_STG (new_creditSeq, src_creditSeq, payeeSeq, positionSeq, salesOrderSeq, salesTransactionSeq, creditTypeSeq, isHeld, releaseDate, pipelineRunSeq, originTypeId, periodSeq, com(...) */
    INSERT INTO EXT.AIA_CB_CREDIT_STG
        (
            new_creditSeq, src_creditSeq, payeeSeq, positionSeq, salesOrderSeq, salesTransactionSeq,
            creditTypeSeq, isHeld, releaseDate, pipelineRunSeq, originTypeId, periodSeq,
            compensationDate, value, unitTypeForValue, preAdjustedValue, unitTypeForPreAdjustedValue, isRollable,
            rollDate, reasonSeq, ruleSeq, pipelineRunDate, businessUnitMap, name,
            comments, genericAttribute1, genericAttribute2, genericAttribute3, genericAttribute4, genericAttribute5,
            genericAttribute6, genericAttribute7, genericAttribute8, genericAttribute9, genericAttribute10, genericAttribute11,
            genericAttribute12, genericAttribute13, genericAttribute14, genericAttribute15, genericAttribute16, genericNumber1,
            unitTypeForGenericNumber1, genericNumber2, unitTypeForGenericNumber2, genericNumber3, unitTypeForGenericNumber3, genericNumber4,
            unitTypeForGenericNumber4, genericNumber5, unitTypeForGenericNumber5, genericNumber6, unitTypeForGenericNumber6, genericDate1,
            genericDate2, genericDate3, genericDate4, genericDate5, genericDate6, genericBoolean1,
            genericBoolean2, genericBoolean3, genericBoolean4, genericBoolean5, genericBoolean6, processingUnitSeq,
            BATCH_NO
        )
        SELECT   /* ORIGSQL: select ROW_NUMBER() OVER (ORDER BY 0*0) as new_creditSeq, cb.creditseq as src_creditSeq, crd.payeeSeq, crd.positionSeq, st.salesOrderSeq, st.salesTransactionSeq, (SELECT dataTypeSeq FROM CS_CreditType(...) */
            --will get credit seq in stagehook
            ROW_NUMBER() OVER (ORDER BY 0*0) AS new_creditSeq,  /* ORIGSQL: rownum */
            cb.creditseq AS src_creditSeq,
            crd.payeeSeq,
            crd.positionSeq,
            st.salesOrderSeq,
            st.salesTransactionSeq,
            (
                SELECT   /* ORIGSQL: (select dataTypeSeq from CS_CreditType where LOWER(creditTypeId) = LOWER(rl_type.TARGET_CREDIT_TYPE) and removeDate = TO_DATE('01012200','mmddyyyy')) */
                    dataTypeSeq
                FROM
                    CS_CreditType
                WHERE
                    LOWER(creditTypeId) = LOWER(rl_type.TARGET_CREDIT_TYPE)
                    AND removeDate = TO_DATE('01012200','mmddyyyy')
            ),
            crd.isHeld,
            crd.releaseDate,
            /* --will get the pipeline run seq in stagehook */
            0 AS pipelinerunseq,
            'calculated',
            :v_STR_CYCLEDATE_PERIODSEQ, --Deepan : added this since using function in select clause doesnt work,
            CASE cb.clawback_type
                WHEN :STR_LUMPSUM
                THEN cb.calculation_date
                WHEN :STR_ONGOING
                THEN crd.compensationdate
                ELSE TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
            END
            AS compensationDate,
            ROUND(IFNULL(cb.clawback,0),2) AS crd_value/* --round(cb.clawback,2) as crd_value, */,  /* ORIGSQL: nvl(cb.clawback,0) */
            crd.unittypeforvalue,
            ROUND(IFNULL(cb.clawback,0),2) AS preAdjustedValue/* --round(cb.clawback,2) as preAdjustedValue, */,  /* ORIGSQL: nvl(cb.clawback,0) */
            crd.unitTypeForPreAdjustedValue,
            crd.isRollable,
            crd.rollDate,
            crd.reasonSeq,
            crd_rule.ruleseq AS ruleSeq,
            '' AS pipelinerundate,
            crd.businessUnitMap,
            rl_out.target_rule_output AS name,
            crd.comments,
            crd.genericAttribute1,
            crd.genericAttribute2,
            crd.genericAttribute3,
            crd.genericAttribute4,
            crd.genericAttribute5,
            crd.genericAttribute6,
            /* --expense account */
            /* --ac_lk.target_ac_code as genericAttribute7, */
            crd.genericAttribute7 AS genericAttribute7,
            /* --balance account */
            /* --crd.genericAttribute8 as genericAttribute8, */
            ac_lk.target_ac_code AS genericAttribute8,
            crd.genericAttribute9,
            crd.genericAttribute10,
            crd.genericAttribute11,
            crd.genericAttribute12,
            crd.genericAttribute13,
            crd.genericAttribute14,
            crd.genericAttribute15,
            crd.genericAttribute16,
            crd.genericNumber1,
            crd.unitTypeForGenericNumber1,
            crd.genericNumber2,
            crd.unitTypeForGenericNumber2,
            /* --crd.genericNumber3, */
            CASE crd.value
                WHEN 0
                THEN 0
                ELSE ROUND((crd.genericNumber3 / crd.value) * cb.clawback, 2)
            END
            AS genericNumber3,
            crd.unitTypeForGenericNumber3,
            /* --crd.genericNumber4, */
            CASE crd.value
                WHEN 0
                THEN 0
                ELSE ROUND(ROUND((crd.genericNumber3 / crd.value) * cb.clawback, 2) * :V_GST_RATE, 2)
            END
            AS genericNumber4,
            crd.unitTypeForGenericNumber4,
            crd.genericNumber5,
            crd.unitTypeForGenericNumber5,
            /* --GST amount */
            ROUND(ROUND(cb.clawback, 2) * :V_GST_RATE,2) AS genericNumber6,
            crd.unitTypeForGenericNumber6,
            crd.genericDate1,
            crd.genericDate2,
            crd.genericDate3,
            crd.genericDate4,
            crd.genericDate5,
            crd.genericDate6,
            crd.genericBoolean1,
            crd.genericBoolean2,
            crd.genericBoolean3,
            crd.genericBoolean4,
            crd.genericBoolean5,
            crd.genericBoolean6,
            crd.processingUnitSeq,
            cb.batch_no
        FROM
            EXT.AIA_CB_CLAWBACK_COMMISSION cb
        INNER JOIN
            CS_SalesTransaction st
            ON cb.salestransactionseq = st.salestransactionseq
        INNER JOIN
            cs_credit crd
            ON cb.creditseq = crd.creditseq
        INNER JOIN
            CS_CREDITTYPE ct
            ON crd.CREDITTYPESEQ = ct.DATATYPESEQ
            AND ct.Removedate = :DT_REMOVEDATE
            --for lookup new credit type
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select distinct source_credit_type, target_credit_type from EXT.AIA_cb_rules_lookup where BUNAME = STR_BUNAME_FA and RULE_TYPE = 'CREDIT' and CLAWBACK_NAME = STR_COMMISSION) */
                    DISTINCT
                    source_credit_type,
                    target_credit_type
                FROM
                    EXT.AIA_cb_rules_lookup
                WHERE
                    BUNAME = :STR_BUNAME_FA
                    AND RULE_TYPE = 'CREDIT'
                    AND CLAWBACK_NAME = :STR_COMMISSION
            ) AS rl_type
            ON ct.credittypeid = rl_type.source_credit_type
            --for lookup new credit output name
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select distinct source_rule_name, source_rule_output, target_rule_name,target_rule_output from EXT.AIA_cb_rules_lookup where BUNAME = STR_BUNAME_FA and RULE_TYPE = 'CREDIT' and CLAWBACK_NAME = STR_COMMIS(...) */
                    DISTINCT
                    source_rule_name,
                    source_rule_output,
                    target_rule_name,
                    target_rule_output
                FROM
                    EXT.AIA_cb_rules_lookup
                WHERE
                    BUNAME = :STR_BUNAME_FA
                    AND RULE_TYPE = 'CREDIT'
                    AND CLAWBACK_NAME = :STR_COMMISSION
            ) AS rl_out
            ON UPPER(crd.name) = UPPER(rl_out.source_rule_output)
            --for lookup new credit sequence number
        INNER JOIN
            cs_rule crd_rule
            ON crd_rule.removedate = :DT_REMOVEDATE
            AND crd_rule.name = rl_out.target_rule_name
            --v15
            AND crd_rule.effectivestartdate <= TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
            AND crd_rule.effectiveenddate > TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
            --for lookup new account code
        INNER JOIN
            EXT.AIA_cb_ac_lookup ac_lk
            ON crd.name = ac_lk.source_credit_name
            AND crd.genericattribute4 = ac_lk.premium_type
            AND crd.genericattribute3 = ac_lk.fund_type
            AND crd.genericattribute8 = ac_lk.source_ac_code
        WHERE
            cb.clawback_name = :STR_COMMISSION
            AND cb.batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2);

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_CREDIT_STG' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('insert into EXT.AIA_CB_CREDIT_STG'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    --get latest records count from EXT.AIA_CB_CREDIT_STG
    SELECT
        COUNT(1)
    INTO
        V_REC_COUNT
    FROM
        EXT.AIA_CB_CREDIT_STG
    WHERE
        batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2);

    --get credit sequence number from TrueComp

    IF :V_REC_COUNT > 0
    THEN
        -- vCreditOffset = SequenceGenPkg.GetNextFullSeq('creditSeq', 56, :V_REC_COUNT) - 1; --Deepan : hardcoding value for now
		-- vCreditOffset =100;
		v_sql := 'CALL TCMP.SEQUENCEGENLIB:GetNextSeq(''creditSeq'', ?, ?)';
		EXECUTE IMMEDIATE :v_sql INTO vCreditOffset USING (:V_REC_COUNT-1);

        /* ORIGSQL: update EXT.AIA_CB_CREDIT_STG SET new_creditSeq = new_creditSeq + vCreditOffset where batch_no in (P_BATCH_NO_1, P_BATCH_NO_2); */
        UPDATE EXT.AIA_CB_CREDIT_STG
            SET
            /* ORIGSQL: new_creditSeq = */
            new_creditSeq = new_creditSeq + :vCreditOffset
        FROM
            EXT.AIA_CB_CREDIT_STG
        WHERE
            batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2);

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    /* ORIGSQL: EXT.Log('SP_CREDIT_COMMISSION_FA end') */
    CALL EXT.Log('SP_CREDIT_COMMISSION_FA end');
END;
  PUBLIC PROCEDURE SP_PM_COMMISSION_FA
(
    IN P_STR_CYCLEDATE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                       /* ORIGSQL: P_STR_CYCLEDATE IN VARCHAR2 */
    IN P_BATCH_NO_1 BIGINT,   /* ORIGSQL: P_BATCH_NO_1 IN INTEGER */
    IN P_BATCH_NO_2 BIGINT     /* ORIGSQL: P_BATCH_NO_2 IN INTEGER */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    --V_BATCH_NO INTEGER;
    DECLARE V_REC_COUNT BIGINT;  /* ORIGSQL: V_REC_COUNT INTEGER; */
    DECLARE vPMOffset BIGINT;  /* ORIGSQL: vPMOffset INTEGER; */
    DECLARE v_STR_CYCLEDATE_PERIODSEQ BIGINT := fn_get_periodseq(:P_STR_CYCLEDATE); --Deepan : added this since using function in select clause doesnt work
    DECLARE v_sql varchar(5000);
    /* ORIGSQL: EXT.Log('SP_PM_COMMISSION_FA start') */
    CALL EXT.Log('SP_PM_COMMISSION_FA start');

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_PM_STG,' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2) */
    CALL EXT.Log('insert into EXT.AIA_CB_PM_STG,'||' batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_1),'') || ' and '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_2),''));

    --insert data into EXT.AIA_CB_PM_STG 
    /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_RULE' not found */

    /* ORIGSQL: insert into EXT.AIA_CB_PM_STG (new_measurementseq, src_measurementseq, name, payeeseq, positionseq, periodseq, pipelinerunseq, pipelinerundate, ruleseq, value, unittypeforvalue, numberofcredits, businessu(...) */
    INSERT INTO EXT.AIA_CB_PM_STG
        (
            new_measurementseq,
            src_measurementseq,
            name,
            payeeseq,
            positionseq,
            periodseq,
            pipelinerunseq,
            pipelinerundate,
            ruleseq,
            value,
            unittypeforvalue,
            numberofcredits,
            businessunitmap,
            genericnumber1,
            unittypeforgenericnumber1,
            processingunitseq,
            unittypefornumberofcredits,
            batch_no
        )
        SELECT   /* ORIGSQL: select DENSE_RANK() OVER (ORDER BY rs.NAME, rs.PAYEESEQ, rs.POSITIONSEQ, rs.PERIODSEQ, rs.PIPELINERUNSEQ) as new_measurementseq, rs.* from (SELECT cb.pmseq AS src_measurementseq, rl_out.target_rule_ou(...) */
            DENSE_RANK() OVER (ORDER BY rs.NAME, rs.PAYEESEQ, rs.POSITIONSEQ, rs.PERIODSEQ, rs.PIPELINERUNSEQ) AS new_measurementseq,
            rs.*
        FROM
            --for commission clawback records
            (
                SELECT   /* ORIGSQL: (select cb.pmseq as src_measurementseq, rl_out.target_rule_output as name, pm.payeeseq as payeeseq, pm.positionseq as positionseq, fn_get_periodseq(P_STR_CYCLEDATE) as periodSeq, 0 as pipelinerunseq, (...) */
                    cb.pmseq AS src_measurementseq,
                    rl_out.target_rule_output AS name,
                    pm.payeeseq AS payeeseq,
                    pm.positionseq AS positionseq,
                    -- fn_get_periodseq(:P_STR_CYCLEDATE) AS periodSeq,
                    :v_STR_CYCLEDATE_PERIODSEQ as periodSeq,
                    0 AS pipelinerunseq,
                    '' AS pipelinerundate,
                    pm_rule.ruleseq AS ruleseq,
                    cb.clawback AS value,
                    pm.unittypeforvalue AS unittypeforvalue,
                    cb.numberofcredits AS numberofcredits,
                    pm.businessunitmap AS businessunitmap,
                    cb.FSC_BSC_PERCENTAGE AS genericnumber1,
                    /* --unit type for percent */
                    1970324836974598 AS unittypeforgenericnumber1,
                    pm.processingunitseq,
                    pm.unittypefornumberofcredits,
                    cb.batch_no
                FROM
                    (
                        SELECT   /* ORIGSQL: (select batch_no, measurement_quarter, clawback_type, pmseq, FSC_BSC_PERCENTAGE, SUM(clawback) as clawback, COUNT(DISTINCT creditseq) as numberofcredits from EXT.AIA_CB_CLAWBACK_COMMISSION where CLAWBACK_(...) */
                            batch_no,
                            measurement_quarter,
                            clawback_type,
                            pmseq,
                            FSC_BSC_PERCENTAGE,
                            SUM(clawback) AS clawback,
                            COUNT(DISTINCT creditseq) AS numberofcredits
                        FROM
                            EXT.AIA_CB_CLAWBACK_COMMISSION
                        WHERE
                            CLAWBACK_NAME = :STR_COMMISSION
                            AND EXT.AIA_CB_CLAWBACK_COMMISSION.batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2)
                        GROUP BY
                            batch_no,
                            measurement_quarter,
                            clawback_type,
                            pmseq,
                            FSC_BSC_PERCENTAGE
                    ) AS cb
                INNER JOIN
                    cs_measurement pm
                    ON cb.pmseq = pm.measurementseq
                    --for lookup new pm output name
                INNER JOIN
                    (
                        SELECT   /* ORIGSQL: (select distinct source_rule_name, source_rule_output, target_rule_name, target_rule_output from EXT.AIA_cb_rules_lookup where BUNAME = STR_BUNAME_FA and RULE_TYPE = 'PM' and CLAWBACK_NAME = STR_COMMISSIO(...) */
                            DISTINCT
                            source_rule_name,
                            source_rule_output,
                            target_rule_name,
                            target_rule_output
                        FROM
                            EXT.AIA_cb_rules_lookup
                        WHERE
                            BUNAME = :STR_BUNAME_FA
                            AND RULE_TYPE = 'PM'
                            AND CLAWBACK_NAME = :STR_COMMISSION
                    ) AS rl_out
                    ON UPPER(pm.name) = UPPER(rl_out.source_rule_output)
                    --for lookup new pm rules
                INNER JOIN
                    cs_rule pm_rule
                    ON pm_rule.removedate = :DT_REMOVEDATE
                    AND rl_out.target_rule_name = pm_rule.name
                    --v15
                    AND pm_rule.effectivestartdate <= TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
                    AND pm_rule.effectiveenddate > TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
        UNION ALL
            --for GST clawback records
            SELECT   /* ORIGSQL: select cb.pmseq as src_measurementseq, rl_out.target_rule_output as name, pm.payeeseq as payeeseq, pm.positionseq as positionseq, fn_get_periodseq(P_STR_CYCLEDATE) as periodSeq, 0 as pipelinerunseq, '(...) */
                cb.pmseq AS src_measurementseq,
                rl_out.target_rule_output AS name,
                pm.payeeseq AS payeeseq,
                pm.positionseq AS positionseq,
                :v_STR_CYCLEDATE_PERIODSEQ,
                0 AS pipelinerunseq,
                '' AS pipelinerundate,
                pm_rule.ruleseq AS ruleseq,
                /* --GST value */
                cb.clawback AS value,
                pm.unittypeforvalue AS unittypeforvalue,
                cb.numberofcredits AS numberofcredits,
                pm.businessunitmap AS businessunitmap,
                pm.genericnumber1,
                pm.unittypeforgenericnumber1,
                pm.processingunitseq,
                pm.unittypefornumberofcredits,
                cb.batch_no
            FROM
                (
                    SELECT   /* ORIGSQL: (select batch_no, measurement_quarter, clawback_type, pmseq, FSC_BSC_PERCENTAGE, SUM(clawback) as clawback, COUNT(DISTINCT creditseq) as numberofcredits from EXT.AIA_CB_CLAWBACK_COMMISSION where CLAWBACK_(...) */
                        batch_no,
                        measurement_quarter,
                        clawback_type,
                        pmseq,
                        FSC_BSC_PERCENTAGE,
                        SUM(clawback) AS clawback,
                        COUNT(DISTINCT creditseq) AS numberofcredits
                    FROM
                        EXT.AIA_CB_CLAWBACK_COMMISSION
                    WHERE
                        CLAWBACK_NAME = :STR_GST_COMMISSION
                        AND EXT.AIA_CB_CLAWBACK_COMMISSION.batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2)
                    GROUP BY
                        batch_no,
                        measurement_quarter,
                        clawback_type,
                        pmseq,
                        FSC_BSC_PERCENTAGE
                ) AS cb
            INNER JOIN
                cs_measurement pm
                ON cb.pmseq = pm.measurementseq
                --for lookup new pm output name
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select distinct source_rule_name, source_rule_output, target_rule_name, target_rule_output from EXT.AIA_cb_rules_lookup where BUNAME = STR_BUNAME_FA and RULE_TYPE = 'PM' and CLAWBACK_NAME = STR_GST_COMMI(...) */
                        DISTINCT
                        source_rule_name,
                        source_rule_output,
                        target_rule_name,
                        target_rule_output
                    FROM
                        EXT.AIA_cb_rules_lookup
                    WHERE
                        BUNAME = :STR_BUNAME_FA
                        AND RULE_TYPE = 'PM'
                        AND CLAWBACK_NAME = :STR_GST_COMMISSION
                ) AS rl_out
                ON UPPER(pm.name) = UPPER(rl_out.source_rule_output)
                --for lookup new pm rules
            INNER JOIN
                cs_rule pm_rule
                ON pm_rule.removedate = :DT_REMOVEDATE
                AND rl_out.target_rule_name = pm_rule.name
                --v15
                AND pm_rule.effectivestartdate <= TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
                AND pm_rule.effectiveenddate > TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
        UNION ALL
            --for supplement the PM records with figure 0 for deposit rules
            SELECT   /* ORIGSQL: select distinct 0 as src_measurementseq, rl_supl_pm.target_rule_output as name, pm.payeeseq as payeeseq, pm.positionseq as positionseq, fn_get_periodseq(P_STR_CYCLEDATE) as periodSeq, 0 as pipelinerun(...) */
                DISTINCT
                0 AS src_measurementseq,
                rl_supl_pm.target_rule_output AS name,
                pm.payeeseq AS payeeseq,
                pm.positionseq AS positionseq,
                :v_STR_CYCLEDATE_PERIODSEQ AS periodSeq,
                0 AS pipelinerunseq,
                '' AS pipelinerundate,
                pm_rule.ruleseq AS ruleseq,
                0 AS value,
                pm.unittypeforvalue AS unittypeforvalue,
                0 AS numberofcredits,
                pm.businessunitmap AS businessunitmap,
                cb.FSC_BSC_PERCENTAGE AS genericnumber1,
                1970324836974598 AS unittypeforgenericnumber1,
                pm.processingunitseq,
                pm.unittypefornumberofcredits,
                cb.batch_no AS BATCH_NO
            FROM
                (
                    SELECT   /* ORIGSQL: (select batch_no, measurement_quarter, clawback_type, pmseq, FSC_BSC_PERCENTAGE, SUM(clawback) as clawback, COUNT(DISTINCT creditseq) as numberofcredits from EXT.AIA_CB_CLAWBACK_COMMISSION where CLAWBACK_(...) */
                        batch_no,
                        measurement_quarter,
                        clawback_type,
                        pmseq,
                        FSC_BSC_PERCENTAGE,
                        SUM(clawback) AS clawback,
                        COUNT(DISTINCT creditseq) AS numberofcredits
                    FROM
                        EXT.AIA_CB_CLAWBACK_COMMISSION
                    WHERE
                        CLAWBACK_NAME = :STR_COMMISSION
                        AND EXT.AIA_CB_CLAWBACK_COMMISSION.batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2)
                    GROUP BY
                        batch_no,
                        measurement_quarter,
                        clawback_type,
                        pmseq,
                        FSC_BSC_PERCENTAGE
                ) AS cb
            INNER JOIN
                cs_measurement pm
                ON cb.pmseq = pm.measurementseq
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select distinct source_rule_output, source_rule_name, target_rule_output, target_rule_name from EXT.AIA_cb_rules_lookup where BUNAME = STR_BUNAME_FA and RULE_TYPE = 'PM' and CLAWBACK_NAME = STR_COMMISSIO(...) */
                        DISTINCT
                        source_rule_output,
                        source_rule_name,
                        target_rule_output,
                        target_rule_name
                    FROM
                        EXT.AIA_cb_rules_lookup
                    WHERE
                        BUNAME = :STR_BUNAME_FA
                        AND RULE_TYPE = 'PM'
                        AND CLAWBACK_NAME = :STR_COMMISSION
                        AND source_rule_output IN
                        ('PM_FYC_Initial_LF_RP',
                            'PM_FYC_Initial_LF_SP',
                            'PM_FYC_Non_Initial_LF_RP',
                        'PM_FYC_Non_Initial_LF_SP')
                ) AS rl_supl_pm
                ON 1 = 1 --pm.name = rl_supl_pm.source_rule_output
                AND pm.name IN ('PM_FYC_Initial_LF_RP',
                    'PM_FYC_Initial_LF_SP',
                    'PM_FYC_Non_Initial_LF_RP',
                'PM_FYC_Non_Initial_LF_SP')
                --for lookup new pm rules
            INNER JOIN
                cs_rule pm_rule
                ON pm_rule.removedate = :DT_REMOVEDATE
                AND rl_supl_pm.target_rule_name = pm_rule.name
                --v15
                AND pm_rule.effectivestartdate <= TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
                AND pm_rule.effectiveenddate > TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
        ) AS rs;

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_PM_STG' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('insert into EXT.AIA_CB_PM_STG'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    --get latest records count from EXT.AIA_CB_CREDIT_STG
    SELECT
        COUNT(DISTINCT New_Measurementseq)
    INTO
        V_REC_COUNT
    FROM
        EXT.AIA_CB_PM_STG
    WHERE
        batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2);

    --get credit sequence number from TrueComp

    IF :V_REC_COUNT > 0
    THEN
        -- vPMOffset = SequenceGenPkg.GetNextFullSeq('measurementSeq', 64, :V_REC_COUNT) - 1; --Deepan : hard coding value for now
        v_sql := 'CALL TCMP.SEQUENCEGENLIB:GetNextSeq(''measurementSeq'', ?, ?)';
		EXECUTE IMMEDIATE :v_sql INTO vPMOffset USING (:V_REC_COUNT-1);
        -- vPMOffset = 100; --Deepan : hard coding value for now
        /* ORIGSQL: update EXT.AIA_CB_PM_STG t SET new_measurementseq = new_measurementseq + vPMOffset where batch_no in (P_BATCH_NO_1, P_BATCH_NO_2); */
        UPDATE EXT.AIA_CB_PM_STG t
            SET
            /* ORIGSQL: new_measurementseq = */
            new_measurementseq = new_measurementseq + :vPMOffset
        FROM
            EXT.AIA_CB_PM_STG t
        WHERE
            batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2);

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    /* ORIGSQL: EXT.Log('SP_PM_COMMISSION_FA end') */
    CALL EXT.Log('SP_PM_COMMISSION_FA end');
END;
  PUBLIC PROCEDURE SP_PMCRDTRACE_COMMISSION_FA
(
    IN P_STR_CYCLEDATE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                       /* ORIGSQL: P_STR_CYCLEDATE IN VARCHAR2 */
    IN P_BATCH_NO_1 BIGINT,   /* ORIGSQL: P_BATCH_NO_1 IN INTEGER */
    IN P_BATCH_NO_2 BIGINT     /* ORIGSQL: P_BATCH_NO_2 IN INTEGER */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    -- V_BATCH_NO INTEGER;
    DECLARE v_puseq BIGINT;  /* ORIGSQL: v_puseq integer; */

    /* initialize library variables, if not yet done */
    CALL init_session_global();

    /* ORIGSQL: EXT.Log('SP_PMCRDTRACE_COMMISSION_FA start') */
    CALL EXT.Log('SP_PMCRDTRACE_COMMISSION_FA start');

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_PMCRDTRACE_STG,' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2) */
    CALL EXT.Log('insert into EXT.AIA_CB_PMCRDTRACE_STG,'||' batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_1),'') || ' and '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_2),''));

    --insert date into EXT.AIA_CB_PMCRDTRACE_STG

    /* ORIGSQL: insert into EXT.AIA_CB_PMCRDTRACE_STG select distinct crd_stg.new_creditseq as creditseq, pm_stg.new_measurementseq as measurementseq, pm_stg.ruleseq as ruleseq, 0 as pipelinerunseq, crd_stg.periodseq as (...) */
    INSERT INTO EXT.AIA_CB_PMCRDTRACE_STG
        SELECT   /* ORIGSQL: select distinct crd_stg.new_creditseq as creditseq, pm_stg.new_measurementseq as measurementseq, pm_stg.ruleseq as ruleseq, 0 as pipelinerunseq, crd_stg.periodseq as sourceperiodseq, pm_stg.periodseq (...) */
            DISTINCT
            crd_stg.new_creditseq AS creditseq,
            pm_stg.new_measurementseq AS measurementseq,
            pm_stg.ruleseq AS ruleseq,
            0 AS pipelinerunseq,
            crd_stg.periodseq AS sourceperiodseq,
            pm_stg.periodseq AS targetperiodseq,
            'calculated' AS sourceoringintypeid,
            MAP(rl.clawback_name, :STR_COMMISSION, crd_stg.value, :STR_GST_COMMISSION, crd_stg.genericnumber6, 0) AS contributionvalue,  /* ORIGSQL: decode(rl.clawback_name, STR_COMMISSION, crd_stg.value, STR_GST_COMMISSION, crd_stg.genericnumber6, 0) */
            crd_stg.unittypeforvalue AS unittypeforcontributionvalue,
            64 AS businessunitmap,
            :V_PROCESSINGUNITSEQ AS processingunitseq,
            cb.batch_no
        FROM
            EXT.AIA_CB_CLAWBACK_COMMISSION cb
        INNER JOIN
            EXT.AIA_CB_CREDIT_STG crd_stg
            ON cb.creditseq = crd_stg.src_creditseq
        INNER JOIN
            EXT.AIA_CB_PM_STG pm_stg
            ON cb.pmseq = pm_stg.src_measurementseq
        LEFT OUTER JOIN
            EXT.AIA_cb_rules_lookup rl
            ON pm_stg.name = rl.target_rule_output
            AND rl.clawback_name IN (:STR_COMMISSION, :STR_GST_COMMISSION)
            AND rule_type = 'PM'
        WHERE
            cb.batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2)
            AND crd_stg.batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2)
            AND pm_stg.batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2);

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_PMCRDTRACE_STG' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('insert into EXT.AIA_CB_PMCRDTRACE_STG'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: EXT.Log('SP_PMCRDTRACE_COMMISSION_FA end') */
    CALL EXT.Log('SP_PMCRDTRACE_COMMISSION_FA end');
END;
  PUBLIC PROCEDURE SP_CREDIT_COMP_FA
(
    IN P_STR_CYCLEDATE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                       /* ORIGSQL: P_STR_CYCLEDATE IN VARCHAR2 */
    IN P_BATCH_NO_1 BIGINT,   /* ORIGSQL: P_BATCH_NO_1 IN INTEGER */
    IN P_BATCH_NO_2 BIGINT     /* ORIGSQL: P_BATCH_NO_2 IN INTEGER */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE V_REC_COUNT BIGINT;  /* ORIGSQL: V_REC_COUNT INTEGER; */
    DECLARE vCreditOffset BIGINT;  /* ORIGSQL: vCreditOffset INTEGER; */
    DECLARE V_GST_RATE DECIMAL(10,2);  /* ORIGSQL: V_GST_RATE NUMBER(10,2); */
    DECLARE v_STR_CYCLEDATE_PERIODSEQ BIGINT;
    DECLARE v_sql varchar(5000);
    /* ORIGSQL: EXT.Log('SP_CREDIT_COMP_FA start') */
    CALL EXT.Log('SP_CREDIT_COMP_FA start');

    /* ORIGSQL: delete from EXT.AIA_CB_CREDIT_STG where batch_no in (P_BATCH_NO_1,P_BATCH_NO_2); */
    DELETE
    FROM
        EXT.AIA_CB_CREDIT_STG
    WHERE
        batch_no IN (:P_BATCH_NO_1,:P_BATCH_NO_2);

    /* ORIGSQL: commit; */
    COMMIT;

    --get batch number
    --V_BATCH_NO := PK_EXT.AIA_CB_CALCULATION.fn_get_batch_no(P_STR_CYCLEDATE);

    --get the GST rate from TrueComp rate schedule table
    --v14
    /*
    select cell.value
      into V_GST_RATE
      from CS_RELATIONALMDLT RM
     inner join CS_MDLTCell cell
        on cell.mdltseq = RM.ruleelementseq
     where RM.name = 'LT_SG_GST'
     and RM.removedate = DT_REMOVEDATE
     and cell.removedate = DT_REMOVEDATE;
    */

    SELECT
        value
    INTO
        V_GST_RATE
    FROM
        ext.vw_lt_gst_rate
    WHERE
        TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE) >= effectivestartdate
        AND TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE) < effectiveenddate;

    --insert data into credit stage table
    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_CREDIT_STG,' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2) */
    CALL EXT.Log('insert into EXT.AIA_CB_CREDIT_STG,'||' batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_1),'') || ' and '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_2),'')); 

    /* ORIGSQL: insert into EXT.AIA_CB_CREDIT_STG (new_creditSeq, src_creditSeq, payeeSeq, positionSeq, salesOrderSeq, salesTransactionSeq, creditTypeSeq, isHeld, releaseDate, pipelineRunSeq, originTypeId, periodSeq, com(...) */
    INSERT INTO EXT.AIA_CB_CREDIT_STG
        (
            new_creditSeq, src_creditSeq, payeeSeq, positionSeq, salesOrderSeq, salesTransactionSeq,
            creditTypeSeq, isHeld, releaseDate, pipelineRunSeq, originTypeId, periodSeq,
            compensationDate, value, unitTypeForValue, preAdjustedValue, unitTypeForPreAdjustedValue, isRollable,
            rollDate, reasonSeq, ruleSeq, pipelineRunDate, businessUnitMap, name,
            comments, genericAttribute1, genericAttribute2, genericAttribute3, genericAttribute4, genericAttribute5,
            genericAttribute6, genericAttribute7, genericAttribute8, genericAttribute9, genericAttribute10, genericAttribute11,
            genericAttribute12, genericAttribute13, genericAttribute14, genericAttribute15, genericAttribute16, genericNumber1,
            unitTypeForGenericNumber1, genericNumber2, unitTypeForGenericNumber2, genericNumber3, unitTypeForGenericNumber3, genericNumber4,
            unitTypeForGenericNumber4, genericNumber5, unitTypeForGenericNumber5, genericNumber6, unitTypeForGenericNumber6, genericDate1,
            genericDate2, genericDate3, genericDate4, genericDate5, genericDate6, genericBoolean1,
            genericBoolean2, genericBoolean3, genericBoolean4, genericBoolean5, genericBoolean6, processingUnitSeq,
            BATCH_NO
        )
        SELECT   /* ORIGSQL: select ROW_NUMBER() OVER (ORDER BY 0*0) as new_creditSeq, cb.creditseq as src_creditSeq, crd.payeeSeq, crd.positionSeq, crd.salesOrderSeq, crd.salesTransactionSeq, (SELECT dataTypeSeq FROM CS_CreditTy(...) */
            --will get credit seq in stagehook
            ROW_NUMBER() OVER (ORDER BY 0*0) AS new_creditSeq,  /* ORIGSQL: rownum */
            cb.creditseq AS src_creditSeq,
            crd.payeeSeq,
            crd.positionSeq,
            crd.salesOrderSeq,
            crd.salesTransactionSeq,
            (
                SELECT   /* ORIGSQL: (select dataTypeSeq from CS_CreditType where LOWER(creditTypeId) = LOWER(rl_type.TARGET_CREDIT_TYPE) and removeDate = TO_DATE('01012200','mmddyyyy')) */
                    dataTypeSeq
                FROM
                    CS_CreditType
                WHERE
                    LOWER(creditTypeId) = LOWER(rl_type.TARGET_CREDIT_TYPE)
                    AND removeDate = TO_DATE('01012200','mmddyyyy')
            ),
            crd.isHeld,
            crd.releaseDate,
            /* --will get the pipeline run seq in stagehook */
            0 AS pipelinerunseq,
            'calculated',
            :v_STR_CYCLEDATE_PERIODSEQ AS periodSeq /* -- crd.periodseq as periodSeq, */,
            /* --crd.compensationDate, */
            /* --to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE) as compensationDate, */
            CASE cb.clawback_type
                WHEN :STR_LUMPSUM
                THEN cb.calculation_date
                /* --when STR_ONGOING then crd.compensationdate */
                ELSE TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
            END
            AS compensationDate,
            IFNULL(cb.clawback,0) AS crd_value/* --cb.clawback as crd_value, */, crd.unittypeforvalue,  /* ORIGSQL: nvl(cb.clawback,0) */
            IFNULL(cb.clawback,0) AS preAdjustedValue/* --cb.clawback as preAdjustedValue, */, crd.unitTypeForPreAdjustedValue,  /* ORIGSQL: nvl(cb.clawback,0) */
            crd.isRollable,
            crd.rollDate,
            crd.reasonSeq,
            crd_rule.ruleseq AS ruleSeq/* -- 1 as ruleseq,-- Change it when merged with package */, '' AS pipelinerundate,
            crd.businessUnitMap,
            rl_out.target_rule_output AS name,
            crd.comments,
            crd.genericAttribute1,
            crd.genericAttribute2,
            crd.genericAttribute3,
            crd.genericAttribute4,
            crd.genericAttribute5,
            crd.genericAttribute6,
            cb.clawback_name,
            /* --ac code */
            crd.genericAttribute8  /* --Not required for compensation, Use same code as that of credit -- ac_lk.target_ac_code as genericAttribute8, */, crd.genericAttribute9,
            crd.genericAttribute10,
            crd.genericAttribute11,
            crd.genericAttribute12,
            crd.genericAttribute13,
            crd.genericAttribute14,
            crd.genericAttribute15,
            crd.genericAttribute16,
            crd.genericNumber1,
            crd.unitTypeForGenericNumber1,
            crd.genericNumber2,
            crd.unitTypeForGenericNumber2,
            /* --crd.genericNumber3, */
            CASE crd.value
                WHEN 0
                THEN 0
                ELSE (crd.genericNumber3 / crd.value) * cb.clawback
            END
            AS genericNumber3,
            crd.unitTypeForGenericNumber3,
            /* --crd.genericNumber4, */
            CASE crd.value
                WHEN 0
                THEN 0
                ELSE (crd.genericNumber3 / crd.value) * cb.clawback * :V_GST_RATE
            END
            AS genericNumber4,
            crd.unitTypeForGenericNumber4,
            crd.genericNumber5,
            crd.unitTypeForGenericNumber5,
            /* --GST amount */
            cb.clawback * :V_GST_RATE AS genericNumber6,
            crd.unitTypeForGenericNumber6,
            crd.genericDate1,
            crd.genericDate2,
            crd.genericDate3,
            crd.genericDate4,
            crd.genericDate5,
            crd.genericDate6,
            crd.genericBoolean1,
            crd.genericBoolean2,
            crd.genericBoolean3,
            crd.genericBoolean4,
            crd.genericBoolean5,
            crd.genericBoolean6,
            crd.processingUnitSeq,
            cb.batch_no
        FROM
            EXT.AIA_CB_CLAWBACK_COMP cb
        INNER JOIN
            cs_credit crd
            ON cb.creditseq = crd.creditseq
            AND cb.salestransactionseq = crd.salestransactionseq
            --for lookup new credit output name
        INNER JOIN
            cs_rule crd_rule
            ON crd_rule.removedate = :DT_REMOVEDATE
            AND crd.compensationdate BETWEEN crd_rule.effectivestartdate AND TO_DATE(ADD_SECONDS(crd_rule.effectiveenddate,(86400*-1)))   /* ORIGSQL: crd_rule.effectiveenddate -1 */
            AND crd_rule.ruleseq = crd.ruleseq--rl_out.source_rule_name  */
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select distinct clawback_name,source_rule_name, source_rule_output, target_rule_name,target_rule_output from EXT.AIA_cb_rules_lookup where BUNAME = STR_BUNAME_FA and RULE_TYPE = 'CREDIT' and CLAWBACK_NAM(...) */
                    DISTINCT
                    clawback_name,
                    source_rule_name,
                    source_rule_output,
                    target_rule_name,
                    target_rule_output
                FROM
                    EXT.AIA_cb_rules_lookup
                WHERE
                    BUNAME = :STR_BUNAME_FA
                    AND RULE_TYPE = 'CREDIT'
                    --Added by Suresh
                    --Add AI NL20180308
                    AND CLAWBACK_NAME IN ('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION','SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1' -- version 14 Harm_BSC_SPI
                        --version13  Harm_Phase4 Start
                        ,'FA_FYO_2.1'
                        ,'FA_FYO_ONG_2.1'
                        ,'FA_RYO_2.1'
                        ,'FA_RYO_ONG_2.1'
                        ,'NADOR_FA_2.1'
                        ,'FA_AI_2.1'
                        ,'FA_AI_ONG_2.1'
                        --version13  Harm_Phase4 End
                    )
            ) AS rl_out
            ON UPPER(crd_rule.name) = UPPER(rl_out.source_rule_name)
        INNER JOIN
            CS_CREDITTYPE ct
            ON crd.CREDITTYPESEQ = ct.DATATYPESEQ
            AND ct.Removedate = :DT_REMOVEDATE
            --for lookup new credit type
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select distinct clawback_name,source_rule_name,source_credit_type, target_credit_type from EXT.AIA_cb_rules_lookup where BUNAME = STR_BUNAME_FA and RULE_TYPE = 'CREDIT' and CLAWBACK_NAME in('FYO_FA','FYO(...) */
                    DISTINCT
                    clawback_name,
                    source_rule_name,
                    source_credit_type,
                    target_credit_type
                FROM
                    EXT.AIA_cb_rules_lookup
                WHERE
                    BUNAME = :STR_BUNAME_FA
                    AND RULE_TYPE = 'CREDIT'
                    AND CLAWBACK_NAME IN('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION','SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1' -- version 17 Harm_BSC_SPI
                        --version13 Harm_Phase4 Start
                        ,'FA_FYO_2.1'
                        ,'FA_FYO_ONG_2.1'
                        ,'FA_RYO_2.1'
                        ,'FA_RYO_ONG_2.1'
                        ,'NADOR_FA_2.1'
                        ,'FA_AI_2.1'
                        ,'FA_AI_ONG_2.1'
                        --version13 Harm_Phase4 End
                    )
            ) AS rl_type
            ON ct.credittypeid = rl_type.source_credit_type
            AND cb.clawback_name = rl_out.clawback_name
            AND cb.clawback_name = rl_type.clawback_name
            AND UPPER(rl_type.SOURCE_RULE_NAME) = UPPER(crd_rule.name)
            --for lookup new credit sequence number
        WHERE
            cb.clawback_name IN ('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION','SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1' -- version 17 Harm_BSC_SPI
                --version13 Harm_Phase4 Start
                ,'FA_FYO_2.1'
                ,'FA_FYO_ONG_2.1'
                ,'FA_RYO_2.1'
                ,'FA_RYO_ONG_2.1'
                ,'NADOR_FA_2.1'
                ,'FA_AI_2.1'
                ,'FA_AI_ONG_2.1'
                --version13 Harm_Phase4 End
            )
            --End by Suresh
            AND cb.batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2);

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_CREDIT_STG' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('insert into EXT.AIA_CB_CREDIT_STG'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: commit; */
    COMMIT;

    --get latest records count from EXT.AIA_CB_CREDIT_STG
    SELECT
        COUNT(1)
    INTO
        V_REC_COUNT
    FROM
        EXT.AIA_CB_CREDIT_STG
    WHERE
        batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2);

    --get credit sequence number from TrueComp

    IF :V_REC_COUNT > 0
    THEN
        -- vCreditOffset = SequenceGenPkg.GetNextFullSeq('creditSeq', 56, :V_REC_COUNT) - 1; --Deepan: Hard coding value for now.
        v_sql := 'CALL TCMP.SEQUENCEGENLIB:GetNextSeq(''creditSeq'', ?, ?)';
		EXECUTE IMMEDIATE :v_sql INTO vCreditOffset USING (:V_REC_COUNT-1);
        
           --vCreditOffset = 100; 
        /* ORIGSQL: update EXT.AIA_CB_CREDIT_STG SET new_creditSeq = new_creditSeq + vCreditOffset where batch_no in (P_BATCH_NO_1, P_BATCH_NO_2); */
        UPDATE EXT.AIA_CB_CREDIT_STG
            SET
            /* ORIGSQL: new_creditSeq = */
            new_creditSeq = new_creditSeq + :vCreditOffset
        FROM
            EXT.AIA_CB_CREDIT_STG
        WHERE
            batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2);

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    /* ORIGSQL: EXT.Log('SP_CREDIT_COMP_FA end') */
    CALL EXT.Log('SP_CREDIT_COMP_FA end');
END;
  PUBLIC PROCEDURE SP_PM_COMP_FA
(
    IN P_STR_CYCLEDATE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                       /* ORIGSQL: P_STR_CYCLEDATE IN VARCHAR2 */
    IN P_BATCH_NO_1 BIGINT,   /* ORIGSQL: P_BATCH_NO_1 IN INTEGER */
    IN P_BATCH_NO_2 BIGINT     /* ORIGSQL: P_BATCH_NO_2 IN INTEGER */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE STR_CB_NAME CONSTANT VARCHAR(20) = 'COMPENSATION';  /* ORIGSQL: STR_CB_NAME CONSTANT VARCHAR2(20) := 'COMPENSATION'; */
    DECLARE V_REC_COUNT BIGINT;  /* ORIGSQL: V_REC_COUNT INTEGER; */
    DECLARE vPMOffset BIGINT;  /* ORIGSQL: vPMOffset INTEGER; */
    DECLARE v_STR_CYCLEDATE_PERIODSEQ BIGINT := fn_get_periodseq(:P_STR_CYCLEDATE) ; /* ORIGSQL: v_periodseq_new integer; */
    DECLARE v_sql varchar(5000);
    -----POST Aggregate Fine Tune  202311-15 Tina

    /* ORIGSQL: EXT.Log('SP_PM_COMP_FA start') */
    CALL EXT.Log('SP_PM_COMP_FA start');

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_PM_STG,' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2) */
    CALL EXT.Log('insert into EXT.AIA_CB_PM_STG,'||' batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_1),'') || ' and '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_2),''));

    /* ORIGSQL: delete from EXT.AIA_CB_PM_STG where batch_no in (P_BATCH_NO_1,P_BATCH_NO_2); */
    DELETE
    FROM
        EXT.AIA_CB_PM_STG
    WHERE
        batch_no IN (:P_BATCH_NO_1,:P_BATCH_NO_2);

    /* ORIGSQL: commit; */
    COMMIT;

    --v_periodseq_new = fn_get_periodseq(:P_STR_CYCLEDATE);
    -----POST Aggregate Fine Tune  202311-15 Tina

    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_CB_MEASUREMENT_TEMP'; */
    /* ORIGSQL: truncate table EXT.AIA_CB_MEASUREMENT_TEMP ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_CB_MEASUREMENT_TEMP';

    -----POST Aggregate Fine Tune  202311-15 Tina

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: EXT.Log('start insert into  into EXT.AIA_CB_MEASUREMENT_TEMP, ' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2) */
    CALL EXT.Log('start insert into  into EXT.AIA_CB_MEASUREMENT_TEMP, '||' batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_1),'') || ' and '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_2),''));   

    /* ORIGSQL: insert into EXT.AIA_CB_MEASUREMENT_TEMP (MEASUREMENTSEQ, NAME,PAYEESEQ,POSITIONSEQ,PERIODSEQ,VALUE,UNITTYPEFORVALUE,NUMBEROFCREDITS, BUSINESSUNITMAP,PROCESSINGUNITSEQ,UNITTYPEFORNUMBEROFCREDITS) select pm(...) */
    INSERT INTO EXT.AIA_CB_MEASUREMENT_TEMP
        (
            MEASUREMENTSEQ, NAME, PAYEESEQ, POSITIONSEQ, PERIODSEQ, VALUE,
            UNITTYPEFORVALUE, NUMBEROFCREDITS, BUSINESSUNITMAP, PROCESSINGUNITSEQ, UNITTYPEFORNUMBEROFCREDITS
        )
        SELECT   /* ORIGSQL: select pm.MEASUREMENTSEQ, pm.NAME, pm.PAYEESEQ, pm.POSITIONSEQ, pm.PERIODSEQ, pm.VALUE, pm.UNITTYPEFORVALUE, pm.NUMBEROFCREDITS, pm.BUSINESSUNITMAP, pm.PROCESSINGUNITSEQ, pm.UNITTYPEFORNUMBEROFCREDITS(...) */
            pm.MEASUREMENTSEQ,
            pm.NAME,
            pm.PAYEESEQ,
            pm.POSITIONSEQ,
            pm.PERIODSEQ,
            pm.VALUE,
            pm.UNITTYPEFORVALUE,
            pm.NUMBEROFCREDITS,
            pm.BUSINESSUNITMAP,
            pm.PROCESSINGUNITSEQ,
            pm.UNITTYPEFORNUMBEROFCREDITS
        FROM
            cs_measurement pm
        WHERE
            pm.tenantid = 'AIAS'
            AND pm.name IN
            (
                SELECT   /* ORIGSQL: (select distinct source_rule_output from EXT.AIA_cb_rules_lookup where BUNAME = STR_BUNAME_FA and RULE_TYPE = 'PM') */
                    DISTINCT
                    source_rule_output
                FROM
                    EXT.AIA_cb_rules_lookup
                WHERE
                    BUNAME = :STR_BUNAME_FA
                    AND RULE_TYPE = 'PM'
            )
            AND pm.PERIODSEQ IN
            (
                SELECT   /* ORIGSQL: (select cbp.PERIODSEQ from cs_period cbp inner join cs_calendar cd on cbp.calendarseq = cd.calendarseq inner join cs_periodtype pt on cbp.periodtypeseq = pt.periodtypeseq where cd.name = 'AIA Singapor(...) */
                    cbp.PERIODSEQ
                FROM
                    cs_period cbp
                INNER JOIN
                    cs_calendar cd
                    ON cbp.calendarseq = cd.calendarseq
                INNER JOIN
                    cs_periodtype pt
                    ON cbp.periodtypeseq = pt.periodtypeseq
                WHERE
                    cd.name = 'AIA Singapore Calendar'
                    -- and cbp.removedate = to_date('2200-01-01','yyyy-mm-dd') --for Cosimo
                    AND cbp.removedate = :DT_REMOVEDATE
                    AND cbp.startdate BETWEEN ADD_MONTHS(to_date(:P_STR_CYCLEDATE, 'yyyy-mm-dd'),-12) AND to_date(:P_STR_CYCLEDATE, 'yyyy-mm-dd')  /* ORIGSQL: to_date(P_STR_CYCLEDATE, 'yyyy-mm-dd') */
                    AND pt.name = 'month'
            );

    /* ORIGSQL: DBMS_STATS.GATHER_TABLE_STATS(ownname => 'AIASEXT', tabname => 'EXT.AIA_CB_MEASUREMENT_TEMP',estimate_percent => dbms_stats.AUTO_SAMPLE_SIZE) */
    EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| 'AIASEXT'|| '.'|| 'EXT.AIA_CB_MEASUREMENT_TEMP';

    /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: EXT.Log('end insert into  into EXT.AIA_CB_MEASUREMENT_TEMP, ' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2) */
    CALL EXT.Log('end insert into  into EXT.AIA_CB_MEASUREMENT_TEMP, '||' batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_1),'') || ' and '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_2),''));

    --insert data into EXT.AIA_CB_PM_STG   

    /* ORIGSQL: insert into EXT.AIA_CB_PM_STG (new_measurementseq, src_measurementseq, name, payeeseq, positionseq, periodseq, pipelinerunseq, pipelinerundate, ruleseq, value, unittypeforvalue, numberofcredits, businessu(...) */
    INSERT INTO EXT.AIA_CB_PM_STG
        (
            new_measurementseq,
            src_measurementseq,
            name,
            payeeseq,
            positionseq,
            periodseq,
            pipelinerunseq,
            pipelinerundate,
            ruleseq,
            value,
            unittypeforvalue,
            numberofcredits,
            businessunitmap,
            genericnumber1,
            unittypeforgenericnumber1,
            processingunitseq,
            unittypefornumberofcredits,
            batch_no,
            clawback_name
        )
        SELECT   /* ORIGSQL: select DENSE_RANK() OVER (ORDER BY rs.NAME, rs.PAYEESEQ, rs.POSITIONSEQ, rs.PERIODSEQ, rs.PIPELINERUNSEQ) as new_measurementseq, rs.* from (SELECT DISTINCT cb.pmseq AS src_measurementseq, rl_out.targe(...) */
            DENSE_RANK() OVER (ORDER BY rs.NAME, rs.PAYEESEQ, rs.POSITIONSEQ, rs.PERIODSEQ, rs.PIPELINERUNSEQ) AS new_measurementseq,
            rs.*
        FROM
            --for comp clawback records
            (
                SELECT   /* ORIGSQL: (select DISTINCT cb.pmseq as src_measurementseq, rl_out.target_rule_output as name, CASE WHEN rl_out.clawback_name in ('SPI_FA_2.1','SPI_FA_ONG_2.1') THEN cb.payeeseq ELSE pm.payeeseq END as payeeseq,(...) */
                    DISTINCT
                    cb.pmseq AS src_measurementseq,
                    rl_out.target_rule_output AS name,
                    /* -- version 17 Harm_BSC_SPI start */
                    CASE
                        WHEN rl_out.clawback_name IN ('SPI_FA_2.1','SPI_FA_ONG_2.1')
                        THEN cb.payeeseq
                        ELSE pm.payeeseq
                    END
                    AS payeeseq,
                    CASE
                        WHEN rl_out.clawback_name IN ('SPI_FA_2.1','SPI_FA_ONG_2.1')
                        THEN cb.positionseq
                        ELSE pm.positionseq
                    END
                    AS positionseq,
                    /* -- version 17 Harm_BSC_SPI end */
                    /* --fn_get_periodseq(P_STR_CYCLEDATE) as periodSeq, -- pm.periodSeq as periodSeq, ----POST Aggregate Fine Tune  202311-15 Tina */
                    :v_STR_CYCLEDATE_PERIODSEQ AS periodSeq,
                    0 AS pipelinerunseq,
                    '' AS pipelinerundate,
                    pm_rule.ruleseq AS ruleseq,
                    IFNULL(cb.clawback,0) AS value,  /* ORIGSQL: nvl(cb.clawback,0) */
                    pm.unittypeforvalue AS unittypeforvalue,
                    cb.numberofcredits AS numberofcredits,
                    pm.businessunitmap AS businessunitmap,
                    cb.FSC_BSC_PERCENTAGE AS genericnumber1,
                    /* --unit type for percent */
                    1970324836974598 AS unittypeforgenericnumber1,
                    pm.processingunitseq,
                    pm.unittypefornumberofcredits,
                    cb.batch_no,
                    rl_out.clawback_name
                    /* -- version 17 Harm_BSC_SPI start */
                FROM
                    (
                        SELECT   /* ORIGSQL: (select comp.clawback_name, comp.batch_no, comp.measurement_quarter, comp.clawback_type, comp.pmseq, comp.FSC_BSC_PERCENTAGE, SUM(comp.clawback) as clawback, COUNT(DISTINCT comp.creditseq) as numberof(...) */
                            comp.clawback_name,
                            comp.batch_no,
                            comp.measurement_quarter,
                            comp.clawback_type,
                            comp.pmseq,
                            comp.FSC_BSC_PERCENTAGE,
                            SUM(comp.clawback) AS clawback,
                            COUNT(DISTINCT comp.creditseq) AS numberofcredits,
                            MAX(cp.ruleelementownerseq) AS positionseq,
                            MAX(cp.payeeseq) AS payeeseq
                        FROM
                            EXT.AIA_CB_CLAWBACK_COMP comp
                        INNER JOIN
                            CS_POSITION cp
                            ON 'SGT'|| IFNULL(comp.wri_agt_code,'') = cp.name
                            AND cp.islast = 1
                            AND cp.removedate = :DT_REMOVEDATE
                        WHERE
                            comp.CLAWBACK_METHOD = :STR_CB_NAME
                            AND comp.batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2)
                        GROUP BY
                            comp.clawback_name,
                            comp.batch_no,
                            comp.measurement_quarter,
                            comp.ytd_period,
                            comp.clawback_type,
                            comp.pmseq,
                            comp.FSC_BSC_PERCENTAGE
                    ) AS cb
                    -- version 17 Harm_BSC_SPI end
                    -- inner join cs_measurement pm  ----POST Aggregate Fine Tune  202311-15 Tina
                INNER JOIN
                    EXT.AIA_CB_MEASUREMENT_TEMP pm
                    ON cb.pmseq = pm.measurementseq
                    --for lookup new pm output name
                INNER JOIN
                    (
                        SELECT   /* ORIGSQL: (select distinct clawback_name,source_rule_name, source_rule_output, target_rule_name, target_rule_output from EXT.AIA_cb_rules_lookup where BUNAME = STR_BUNAME_FA and RULE_TYPE = 'PM' and CLAWBACK_NAME i(...) */
                            DISTINCT
                            clawback_name,
                            source_rule_name,
                            source_rule_output,
                            target_rule_name,
                            target_rule_output
                        FROM
                            EXT.AIA_cb_rules_lookup
                        WHERE
                            BUNAME = :STR_BUNAME_FA
                            AND RULE_TYPE = 'PM'
                            --Added by Suresh
                            AND CLAWBACK_NAME IN ('FYO_FA_ONG','FYO_FA','RYO_FA','RYO_FA_ONG','NLPI','NLPI_ONG','SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1' -- version 17 Harm_BSC_SPI
                                --version13 Harm_Phase4 Start
                                ,'FA_FYO_2.1'
                                ,'FA_FYO_ONG_2.1'
                                ,'FA_RYO_2.1'
                                ,'FA_RYO_ONG_2.1'
                                ,'NADOR_FA_2.1'
                                ,'FA_AI_2.1'
                                ,'FA_AI_ONG_2.1'
                                --version13 Harm_Phase4 End
                            )
                    ) AS rl_out
                    --end by Suresh
                    ON UPPER(pm.name) = UPPER(rl_out.source_rule_output)
                    --for lookup new pm rules
                INNER JOIN
                    cs_rule pm_rule
                    ON pm_rule.removedate = :DT_REMOVEDATE
                    AND rl_out.target_rule_name = pm_rule.name
                    AND rl_out.clawback_name = cb.clawback_name
                    --v15
                    AND pm_rule.effectivestartdate <= TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
                    AND pm_rule.effectiveenddate > TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
        UNION ALL
            --for supplement the PM records with figure 0 for deposit rules
            SELECT   /* ORIGSQL: select distinct 0 as src_measurementseq, rl_supl_pm.target_rule_output as name, CASE WHEN rl_supl_pm.clawback_name in ('SPI_FA_2.1','SPI_FA_ONG_2.1') THEN cb.payeeseq ELSE pm.payeeseq END as payeeseq,(...) */
                DISTINCT
                0 AS src_measurementseq,
                rl_supl_pm.target_rule_output AS name,
                /* -- version 17 Harm_BSC_SPI start */
                CASE
                    WHEN rl_supl_pm.clawback_name IN ('SPI_FA_2.1','SPI_FA_ONG_2.1')
                    THEN cb.payeeseq
                    ELSE pm.payeeseq
                END
                AS payeeseq,
                CASE
                    WHEN rl_supl_pm.clawback_name IN ('SPI_FA_2.1','SPI_FA_ONG_2.1')
                    THEN cb.positionseq
                    ELSE pm.positionseq
                END
                AS positionseq,
                /* -- version 17 Harm_BSC_SPI end */
                /* --fn_get_periodseq(P_STR_CYCLEDATE) as periodSeq, -- pm.periodSeq as periodSeq, ----POST Aggregate Fine Tune  202311-15 Tina */
                :v_STR_CYCLEDATE_PERIODSEQ AS periodSeq,
                0 AS pipelinerunseq,
                '' AS pipelinerundate,
                pm_rule.ruleseq AS ruleseq,
                0 AS value,
                pm.unittypeforvalue AS unittypeforvalue,
                0 AS numberofcredits,
                pm.businessunitmap AS businessunitmap,
                cb.FSC_BSC_PERCENTAGE AS genericnumber1,
                1970324836974598 AS unittypeforgenericnumber1,
                pm.processingunitseq,
                pm.unittypefornumberofcredits,
                cb.batch_no AS BATCH_NO,
                rl_supl_pm.clawback_name
                /* -- version 17 Harm_BSC_SPI start */
            FROM
                (
                    SELECT   /* ORIGSQL: (select comp.clawback_name,comp.batch_no, comp.measurement_quarter, comp.clawback_type, comp.pmseq, comp.FSC_BSC_PERCENTAGE, SUM(comp.clawback) as clawback, COUNT(DISTINCT comp.creditseq) as numberofc(...) */
                        comp.clawback_name,
                        comp.batch_no,
                        comp.measurement_quarter,
                        comp.clawback_type,
                        comp.pmseq,
                        comp.FSC_BSC_PERCENTAGE,
                        SUM(comp.clawback) AS clawback,
                        COUNT(DISTINCT comp.creditseq) AS numberofcredits,
                        MAX(cp.ruleelementownerseq) AS positionseq,
                        MAX(cp.payeeseq) AS payeeseq
                    FROM
                        EXT.AIA_CB_CLAWBACK_COMP comp
                    INNER JOIN
                        CS_POSITION cp
                        ON 'SGT'|| IFNULL(comp.wri_agt_code,'') = cp.name
                        AND cp.islast = 1
                        AND cp.removedate = :DT_REMOVEDATE
                    WHERE
                        comp.CLAWBACK_METHOD = :STR_CB_NAME
                        AND comp.batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2)
                    GROUP BY
                        comp.clawback_name,comp.batch_no,
                        comp.measurement_quarter,
                        comp.ytd_period,
                        comp.clawback_type,
                        comp.pmseq,
                        comp.FSC_BSC_PERCENTAGE
                ) AS cb
                -- version 17 Harm_BSC_SPI end
                -- inner join cs_measurement pm  ----POST Aggregate Fine Tune  202311-15 Tina
            INNER JOIN
                EXT.AIA_CB_MEASUREMENT_TEMP pm
                ON cb.pmseq = pm.measurementseq
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select distinct clawback_name,source_rule_output, source_rule_name, target_rule_output, target_rule_name from EXT.AIA_cb_rules_lookup where BUNAME = STR_BUNAME_FA and RULE_TYPE = 'PM' and CLAWBACK_NAME i(...) */
                        DISTINCT
                        clawback_name,
                        source_rule_output,
                        source_rule_name,
                        target_rule_output,
                        target_rule_name
                    FROM
                        EXT.AIA_cb_rules_lookup
                    WHERE
                        BUNAME = :STR_BUNAME_FA
                        AND RULE_TYPE = 'PM'
                        --Added by Suresh
                        AND CLAWBACK_NAME IN ('FYO_FA_ONG','FYO_FA','RYO_FA','RYO_FA_ONG','NLPI','NLPI_ONG','SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1' -- version 17 Harm_BSC_SPI
                            --version13 Harm_Phase4 Start
                            ,'FA_FYO_2.1'
                            ,'FA_FYO_ONG_2.1'
                            ,'FA_RYO_2.1'
                            ,'FA_RYO_ONG_2.1'
                            ,'NADOR_FA_2.1'
                            ,'FA_AI_2.1'
                            ,'FA_AI_ONG_2.1'
                            --version13 Harm_Phase4 End
                        )
                ) AS rl_supl_pm
                ON --pm.name = rl_supl_pm.source_rule_output
                1 = 1
                AND pm.name IN
                (
                    SELECT   /* ORIGSQL: (select distinct source_rule_output from EXT.AIA_cb_rules_lookup where BUNAME = STR_BUNAME_FA and RULE_TYPE = 'PM' and CLAWBACK_NAME in ('FYO_FA_ONG','FYO_FA','RYO_FA','RYO_FA_ONG','NLPI','NLPI_ONG','SPI_(...) */
                        DISTINCT
                        source_rule_output
                    FROM
                        EXT.AIA_cb_rules_lookup
                    WHERE
                        BUNAME = :STR_BUNAME_FA
                        AND RULE_TYPE = 'PM'
                        AND CLAWBACK_NAME IN ('FYO_FA_ONG','FYO_FA','RYO_FA','RYO_FA_ONG','NLPI','NLPI_ONG','SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1' -- version 17 Harm_BSC_SPI
                            --version13 Harm_Phase4 Start
                            ,'FA_FYO_2.1'
                            ,'FA_FYO_ONG_2.1'
                            ,'FA_RYO_2.1'
                            ,'FA_RYO_ONG_2.1'
                            ,'NADOR_FA_2.1'
                            ,'FA_AI_2.1'
                            ,'FA_AI_ONG_2.1'
                            --version13 Harm_Phase4 End
                        )
                ) --End by Suresh
                --for lookup new pm rules
            INNER JOIN
                cs_rule pm_rule
                ON pm_rule.removedate = :DT_REMOVEDATE
                AND rl_supl_pm.target_rule_name = pm_rule.name
                --v15
                AND pm_rule.effectivestartdate <= TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
                AND pm_rule.effectiveenddate > TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
        ) AS rs;

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_PM_STG' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('insert into EXT.AIA_CB_PM_STG'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    --get latest records count from EXT.AIA_CB_CREDIT_STG
    SELECT
        COUNT(DISTINCT New_Measurementseq)
    INTO
        V_REC_COUNT
    FROM
        EXT.AIA_CB_PM_STG
    WHERE
        batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2);

    --get credit sequence number from TrueComp

    IF :V_REC_COUNT > 0
    THEN
        -- vPMOffset = SequenceGenPkg.GetNextFullSeq('measurementSeq', 64, :V_REC_COUNT) - 1; --Deepan : Hard Coding value for now
        -- vPMOffset = 100;
        v_sql := 'CALL TCMP.SEQUENCEGENLIB:GetNextSeq(''measurementSeq'', ?, ?)';
		EXECUTE IMMEDIATE :v_sql INTO vPMOffset USING (:V_REC_COUNT-1);
        /* ORIGSQL: update EXT.AIA_CB_PM_STG t SET new_measurementseq = new_measurementseq + vPMOffset where batch_no in (P_BATCH_NO_1, P_BATCH_NO_2); */
        UPDATE EXT.AIA_CB_PM_STG t
            SET
            /* ORIGSQL: new_measurementseq = */
            new_measurementseq = new_measurementseq + :vPMOffset
        FROM
            EXT.AIA_CB_PM_STG t
        WHERE
            batch_no IN (:P_BATCH_NO_1, :P_BATCH_NO_2);

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    /* ORIGSQL: EXT.Log('SP_PM_COMP_FA end') */
    CALL EXT.Log('SP_PM_COMP_FA end');
END;
  PUBLIC PROCEDURE SP_PMCRDTRACE_COMP_FA
(
    IN P_STR_CYCLEDATE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                       /* ORIGSQL: P_STR_CYCLEDATE IN VARCHAR2 */
    IN P_BATCH_NO_1 BIGINT,   /* ORIGSQL: P_BATCH_NO_1 IN INTEGER */
    IN P_BATCH_NO_2 BIGINT     /* ORIGSQL: P_BATCH_NO_2 IN INTEGER */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_puseq BIGINT;  /* ORIGSQL: v_puseq integer; */

    /* initialize library variables, if not yet done */
    CALL init_session_global();

    /* ORIGSQL: EXT.Log('SP_PMCRDTRACE_COMP_FA start') */
    CALL EXT.Log('SP_PMCRDTRACE_COMP_FA start');

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_PMCRDTRACE_STG,' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2) */
    CALL EXT.Log('insert into EXT.AIA_CB_PMCRDTRACE_STG,'||' batch_no = '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_1),'') || ' and '|| IFNULL(TO_VARCHAR(:P_BATCH_NO_2),''));

    /* ORIGSQL: delete from EXT.AIA_CB_PMCRDTRACE_STG where batch_no in (P_BATCH_NO_1,P_BATCH_NO_2); */
    DELETE
    FROM
        EXT.AIA_CB_PMCRDTRACE_STG
    WHERE
        batch_no IN (:P_BATCH_NO_1,:P_BATCH_NO_2);

    /* ORIGSQL: commit; */
    COMMIT;

    --insert date into EXT.AIA_CB_PMCRDTRACE_STG

    /* ORIGSQL: insert into EXT.AIA_CB_PMCRDTRACE_STG select distinct crd_stg.new_creditseq as creditseq, pm_stg.new_measurementseq as measurementseq, pm_stg.ruleseq as ruleseq, 0 as pipelinerunseq, crd_stg.periodseq as (...) */
    INSERT INTO EXT.AIA_CB_PMCRDTRACE_STG
        SELECT   /* ORIGSQL: select distinct crd_stg.new_creditseq as creditseq, pm_stg.new_measurementseq as measurementseq, pm_stg.ruleseq as ruleseq, 0 as pipelinerunseq, crd_stg.periodseq as sourceperiodseq, pm_stg.periodseq (...) */
            DISTINCT
            crd_stg.new_creditseq AS creditseq,
            pm_stg.new_measurementseq AS measurementseq,
            pm_stg.ruleseq AS ruleseq,
            0 AS pipelinerunseq,
            crd_stg.periodseq AS sourceperiodseq,
            pm_stg.periodseq AS targetperiodseq,
            'calculated' AS sourceoringintypeid,
            IFNULL(crd_stg.value,0) AS contributionvalue,  /* ORIGSQL: nvl(crd_stg.value,0) */
            crd_stg.unittypeforvalue AS unittypeforcontributionvalue,
            64 AS businessunitmap,
            /* --1 as processingunitseq, -- */
            :V_PROCESSINGUNITSEQ AS processingunitseq,
            cb.batch_no
        FROM
            EXT.AIA_CB_CLAWBACK_COMP cb
        INNER JOIN
            EXT.AIA_CB_CREDIT_STG crd_stg
            ON cb.creditseq = crd_stg.src_creditseq
            AND cb.clawback_name = crd_stg.genericattribute7
        INNER JOIN
            EXT.AIA_CB_PM_STG pm_stg
            ON cb.pmseq = pm_stg.src_measurementseq
            AND pm_stg.clawback_name = cb.clawback_name
        WHERE
            cb.batch_no IN (:P_BATCH_NO_1,:P_BATCH_NO_2)
            AND crd_stg.batch_no IN (:P_BATCH_NO_1,:P_BATCH_NO_2)
            AND pm_stg.batch_no IN (:P_BATCH_NO_1,:P_BATCH_NO_2);

    /* ORIGSQL: EXT.Log('insert into EXT.AIA_CB_PMCRDTRACE_STG' || '; row count: ' || to_char(sql%rowcount)) */
    CALL EXT.Log('insert into EXT.AIA_CB_PMCRDTRACE_STG'|| '; row count: '|| IFNULL(TO_VARCHAR(::ROWCOUNT),'')   
        );  /* ORIGSQL: to_char(sql%rowcount) */

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: EXT.Log('SP_PMCRDTRACE_COMP_FA end') */
    CALL EXT.Log('SP_PMCRDTRACE_COMP_FA end');
END;
  PUBLIC PROCEDURE SP_STAGE_COMP_ONG_PASTTX_FA
(
    IN P_STR_CB_ONG_STARTDATE VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                             /* ORIGSQL: P_STR_CB_ONG_STARTDATE IN VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* initialize library variables, if not yet done */
    CALL init_session_global();

    /* ORIGSQL: init */
    CALL init();

    /* ORIGSQL: EXT.Log('SP_STAGE_COMP_ONG_PASTTX_FA started with param ' || P_STR_CB_ONG_STARTDATE) */
    CALL EXT.Log('SP_STAGE_COMP_ONG_PASTTX_FA started with param '|| IFNULL(:P_STR_CB_ONG_STARTDATE,''));

    /* ORIGSQL: execute immediate 'truncate table EXT.AIA_CB_COMP_ONG_STGPAST_TX_FA'; */
    /* ORIGSQL: truncate table EXT.AIA_CB_COMP_ONG_STGPAST_TX_FA ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.AIA_CB_COMP_ONG_STGPAST_TX_FA';

    /* ORIGSQL: insert into EXT.AIA_CB_COMP_ONG_STGPAST_TX_FA select st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENS(...) */
    INSERT INTO EXT.AIA_CB_COMP_ONG_STGPAST_TX_FA
        /* ORIGSQL: select / *+ PARALLEL leading(ip,st,p) INDEX(st EXT.AIA_salestransaction_product) * / */
        SELECT   /* ORIGSQL: select st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, st.BILLTOADDRESSSEQ, st.SHIPTOA(...) */
            st.TENANTID,
            st.SALESTRANSACTIONSEQ,
            st.SALESORDERSEQ,
            st.LINENUMBER,
            st.SUBLINENUMBER,
            st.EVENTTYPESEQ,
            st.PIPELINERUNSEQ,
            st.ORIGINTYPEID,
            st.COMPENSATIONDATE,
            st.BILLTOADDRESSSEQ,
            st.SHIPTOADDRESSSEQ,
            st.OTHERTOADDRESSSEQ,
            st.ISRUNNABLE,
            st.BUSINESSUNITMAP,
            st.ACCOUNTINGDATE,
            st.PRODUCTID,
            st.PRODUCTNAME,
            st.PRODUCTDESCRIPTION,
            st.NUMBEROFUNITS,
            st.UNITVALUE,
            st.UNITTYPEFORUNITVALUE,
            st.PREADJUSTEDVALUE,
            st.UNITTYPEFORPREADJUSTEDVALUE,
            st.VALUE,
            st.UNITTYPEFORVALUE,
            st.NATIVECURRENCY,
            st.NATIVECURRENCYAMOUNT,
            st.DISCOUNTPERCENT,
            st.DISCOUNTTYPE,
            st.PAYMENTTERMS,
            st.PONUMBER,
            st.CHANNEL,
            st.ALTERNATEORDERNUMBER,
            st.DATASOURCE,
            st.REASONSEQ,
            st.COMMENTS,
            st.GENERICATTRIBUTE1,
            st.GENERICATTRIBUTE2,
            st.GENERICATTRIBUTE3,
            st.GENERICATTRIBUTE4,
            st.GENERICATTRIBUTE5,
            st.GENERICATTRIBUTE6,
            st.GENERICATTRIBUTE7,
            st.GENERICATTRIBUTE8,
            st.GENERICATTRIBUTE9,
            st.GENERICATTRIBUTE10,
            st.GENERICATTRIBUTE11,
            st.GENERICATTRIBUTE12,
            st.GENERICATTRIBUTE13,
            st.GENERICATTRIBUTE14,
            st.GENERICATTRIBUTE15,
            st.GENERICATTRIBUTE16,
            st.GENERICATTRIBUTE17,
            st.GENERICATTRIBUTE18,
            st.GENERICATTRIBUTE19,
            st.GENERICATTRIBUTE20,
            st.GENERICATTRIBUTE21,
            st.GENERICATTRIBUTE22,
            st.GENERICATTRIBUTE23,
            st.GENERICATTRIBUTE24,
            st.GENERICATTRIBUTE25,
            st.GENERICATTRIBUTE26,
            st.GENERICATTRIBUTE27,
            st.GENERICATTRIBUTE28,
            st.GENERICATTRIBUTE29,
            st.GENERICATTRIBUTE30,
            st.GENERICATTRIBUTE31,
            st.GENERICATTRIBUTE32,
            st.GENERICNUMBER1,
            st.UNITTYPEFORGENERICNUMBER1,
            st.GENERICNUMBER2,
            st.UNITTYPEFORGENERICNUMBER2,
            st.GENERICNUMBER3,
            st.UNITTYPEFORGENERICNUMBER3,
            st.GENERICNUMBER4,
            st.UNITTYPEFORGENERICNUMBER4,
            st.GENERICNUMBER5,
            st.UNITTYPEFORGENERICNUMBER5,
            st.GENERICNUMBER6,
            st.UNITTYPEFORGENERICNUMBER6,
            st.GENERICDATE1,
            st.GENERICDATE2,
            st.GENERICDATE3,
            st.GENERICDATE4,
            st.GENERICDATE5,
            st.GENERICDATE6,
            st.GENERICBOOLEAN1,
            st.GENERICBOOLEAN2,
            st.GENERICBOOLEAN3,
            st.GENERICBOOLEAN4,
            st.GENERICBOOLEAN5,
            st.GENERICBOOLEAN6,
            st.PROCESSINGUNITSEQ,
            st.MODIFICATIONDATE,
            st.UNITTYPEFORLINENUMBER,
            st.UNITTYPEFORSUBLINENUMBER,
            st.UNITTYPEFORNUMBEROFUNITS,
            st.UNITTYPEFORDISCOUNTPERCENT,
            st.UNITTYPEFORNATIVECURRENCYAMT,
            st.MODELSEQ,
            ip.BUNAME,
            ip.YEAR,
            ip.QUARTER,
            ip.WRI_DIST_CODE,
            ip.WRI_DIST_NAME,
            ip.WRI_DM_CODE,
            ip.WRI_DM_NAME,
            ip.WRI_AGY_CODE,
            ip.WRI_AGY_NAME,
            ip.WRI_AGY_LDR_CODE,
            ip.WRI_AGY_LDR_NAME,
            ip.WRI_AGT_CODE,
            ip.WRI_AGT_NAME,
            ip.FSC_TYPE,
            ip.RANK,
            ip.CLASS,
            ip.FSC_BSC_GRADE,
            ip.FSC_BSC_PERCENTAGE,
            ip.INSURED_NAME,
            ip.CONTRACT_CAT,
            ip.LIFE_NUMBER,
            ip.COVERAGE_NUMBER,
            ip.RIDER_NUMBER,
            ip.COMPONENT_CODE,
            ip.COMPONENT_NAME,
            ip.ISSUE_DATE,
            ip.INCEPTION_DATE,
            ip.RISK_COMMENCEMENT_DATE,
            ip.FHR_DATE,
            ip.BASE_RIDER_IND,
            ip.TRANSACTION_DATE,
            ip.PAYMENT_MODE,
            ip.POLICY_CURRENCY,
            ip.PROCESSING_PERIOD,
            ip.CREATED_DATE,
            ip.POLICYIDSEQ,
            ip.SUBMITDATE,
            p.periodseq,
            ip.FAOB_AGT_CODE,
            '',
            ''
        FROM
            CS_SALESTRANSACTION st
        INNER JOIN
            EXT.AIA_CB_IDENTIFY_POLICY ip
            ON st.PONUMBER = ip.PONUMBER
            AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
            AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
            AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
            AND st.PRODUCTID = ip.COMPONENT_CODE
        INNER JOIN
            CS_PERIOD p
            ON st.compensationdate >= p.startdate
            AND st.compensationdate < p.enddate
            AND p.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND p.calendarseq = :V_CALENDARSEQ
            AND p.periodtypeseq = 2814749767106561
        INNER JOIN
            cs_businessunit bu
            ON st.businessunitmap = bu.mask
        WHERE
            st.tenantid = 'AIAS'
            AND st.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ
            AND ip.buname = :STR_BUNAME_FA
            AND st.eventtypeseq <> 16607023625933358
            AND p.removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
            /* ORIGSQL: to_date('2200-01-01','yyyy-mm-dd') */
            --v8 20200928
            AND st.compensationdate <= (TO_DATE(ADD_SECONDS(TO_DATE(:P_STR_CB_ONG_STARTDATE, :STR_DATE_FORMAT_TYPE),(86400*-1))));  /* ORIGSQL: TO_DATE(P_STR_CB_ONG_STARTDATE, STR_DATE_FORMAT_TYPE) -1 */

    /* ORIGSQL: EXT.Log('SP_STAGE_COMP_ONG_PASTTX_FA compensation, tx up to ' || (TO_DATE(P_STR_CB_ONG_STARTDATE, STR_DATE_FORMAT_TYPE) -1) || ' with count = ' || SQL%ROWCOUNT) */
    CALL EXT.Log('SP_STAGE_COMP_ONG_PASTTX_FA compensation, tx up to '|| (TO_DATE(ADD_SECONDS(TO_DATE(:P_STR_CB_ONG_STARTDATE, :STR_DATE_FORMAT_TYPE),(86400*-1)))) || ' with count = '|| ::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;

    --For AI clawback NL20180308  
    /* ORIGSQL: insert into EXT.AIA_CB_COMP_ONG_STGPAST_TX_FA WITH AMR AS (SELECT ROW_NUMBER() OVER (PARTITION BY t1.PONUMBER,t1.AI_PAYMENT,t1.COMPENSATIONDATE,t1.PAYEE_CODE,t1.POLICY_INCEPTION_DATE ORDER BY t1.component(...) */
    INSERT INTO EXT.AIA_CB_COMP_ONG_STGPAST_TX_FA
        WITH 
        AMR AS (
            SELECT   /* ORIGSQL: (select ROW_NUMBER() OVER (PARTITION BY t1.PONUMBER,t1.AI_PAYMENT,t1.COMPENSATIONDATE,t1.PAYEE_CODE,t1.POLICY_INCEPTION_DATE ORDER BY t1.component_CODE) as rn, t1.* from AI_MONTHLY_REPORT t1 where t1.(...) */
                ROW_NUMBER()OVER (PARTITION BY t1.PONUMBER,t1.AI_PAYMENT,t1.COMPENSATIONDATE,t1.PAYEE_CODE,t1.POLICY_INCEPTION_DATE ORDER BY t1.component_CODE) AS rn,
                t1.*
            FROM
                EXT.AI_MONTHLY_REPORT t1
            WHERE
                t1.AI_PAYMENT <> 0
        
        )
        ,
        st AS
        (
            SELECT   /* ORIGSQL: (select ROW_NUMBER() OVER (PARTITION BY t2.PONUMBER,t2.VALUE,t2.ACCOUNTINGDATE,t2.GENERICATTRIBUTE11,t2.GENERICDATE2 ORDER BY t2.PRODUCTID) as rn, t2.* from cs_Salestransaction t2,cs_businessunit bu w(...) */
                ROW_NUMBER() OVER (PARTITION BY t2.PONUMBER,t2.VALUE,t2.ACCOUNTINGDATE,t2.GENERICATTRIBUTE11,t2.GENERICDATE2 ORDER BY t2.PRODUCTID) AS rn,
                t2.*
            FROM
                cs_Salestransaction t2,
                cs_businessunit bu
            WHERE
                t2.tenantid = 'AIAS'
                AND t2.businessunitmap = bu.mask
                -- and bu.name = STR_BUNAME_FA   --Changes done to fix not getting AGY AI records --Gopi-04072019
                AND t2.eventtypeseq = 16607023625933358
                AND t2.PROCESSINGUNITSEQ = :V_PROCESSINGUNITSEQ
                --v8 20200928
                AND t2.compensationdate <= (TO_DATE(ADD_SECONDS(TO_DATE(:P_STR_CB_ONG_STARTDATE, :STR_DATE_FORMAT_TYPE),(86400*-1))))  /* ORIGSQL: TO_DATE(P_STR_CB_ONG_STARTDATE, STR_DATE_FORMAT_TYPE) -1 */
        
        )
        ,
        IP AS
        (
            SELECT   /* ORIGSQL: (select ROW_NUMBER() OVER (PARTITION BY t3.PONUMBER,t3.WRI_AGT_CODE,t3.component_CODE,t3.inception_date,t3.risk_commencement_date ORDER BY t3.coverage_number) as rn, t3.* from EXT.AIA_CB_IDENTIFY_POLICY t(...) */
                ROW_NUMBER() OVER (PARTITION BY t3.PONUMBER,t3.WRI_AGT_CODE,t3.component_CODE,t3.inception_date,t3.risk_commencement_date ORDER BY t3.coverage_number) AS rn,
                t3.*
            FROM
                EXT.AIA_CB_IDENTIFY_POLICY t3
            WHERE
                t3.BUNAME = :STR_BUNAME_FA
        
        )
        SELECT   /* ORIGSQL: select st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, st.BILLTOADDRESSSEQ, st.SHIPTOA(...) */
            /* ORIGSQL: select / *+ PARALLEL * / */
            st.TENANTID,
            st.SALESTRANSACTIONSEQ,
            st.SALESORDERSEQ,
            st.LINENUMBER,
            st.SUBLINENUMBER,
            st.EVENTTYPESEQ,
            st.PIPELINERUNSEQ,
            st.ORIGINTYPEID,
            st.COMPENSATIONDATE,
            st.BILLTOADDRESSSEQ,
            st.SHIPTOADDRESSSEQ,
            st.OTHERTOADDRESSSEQ,
            st.ISRUNNABLE,
            st.BUSINESSUNITMAP,
            st.ACCOUNTINGDATE,
            st.PRODUCTID,
            st.PRODUCTNAME,
            st.PRODUCTDESCRIPTION,
            st.NUMBEROFUNITS,
            st.UNITVALUE,
            st.UNITTYPEFORUNITVALUE,
            st.PREADJUSTEDVALUE,
            st.UNITTYPEFORPREADJUSTEDVALUE,
            st.VALUE,
            st.UNITTYPEFORVALUE,
            st.NATIVECURRENCY,
            st.NATIVECURRENCYAMOUNT,
            st.DISCOUNTPERCENT,
            st.DISCOUNTTYPE,
            st.PAYMENTTERMS,
            st.PONUMBER,
            st.CHANNEL,
            st.ALTERNATEORDERNUMBER,
            st.DATASOURCE,
            st.REASONSEQ,
            st.COMMENTS,
            st.GENERICATTRIBUTE1,
            st.GENERICATTRIBUTE2,
            st.GENERICATTRIBUTE3,
            st.GENERICATTRIBUTE4,
            st.GENERICATTRIBUTE5,
            st.GENERICATTRIBUTE6,
            st.GENERICATTRIBUTE7,
            st.GENERICATTRIBUTE8,
            st.GENERICATTRIBUTE9,
            st.GENERICATTRIBUTE10,
            st.GENERICATTRIBUTE11,
            st.GENERICATTRIBUTE12,
            st.GENERICATTRIBUTE13,
            st.GENERICATTRIBUTE14,
            st.GENERICATTRIBUTE15,
            st.GENERICATTRIBUTE16,
            st.GENERICATTRIBUTE17,
            st.GENERICATTRIBUTE18,
            st.GENERICATTRIBUTE19,
            st.GENERICATTRIBUTE20,
            st.GENERICATTRIBUTE21,
            st.GENERICATTRIBUTE22,
            st.GENERICATTRIBUTE23,
            st.GENERICATTRIBUTE24,
            st.GENERICATTRIBUTE25,
            st.GENERICATTRIBUTE26,
            st.GENERICATTRIBUTE27,
            st.GENERICATTRIBUTE28,
            st.GENERICATTRIBUTE29,
            st.GENERICATTRIBUTE30,
            st.GENERICATTRIBUTE31,
            st.GENERICATTRIBUTE32,
            st.GENERICNUMBER1,
            st.UNITTYPEFORGENERICNUMBER1,
            st.GENERICNUMBER2,
            st.UNITTYPEFORGENERICNUMBER2,
            st.GENERICNUMBER3,
            st.UNITTYPEFORGENERICNUMBER3,
            st.GENERICNUMBER4,
            st.UNITTYPEFORGENERICNUMBER4,
            st.GENERICNUMBER5,
            st.UNITTYPEFORGENERICNUMBER5,
            st.GENERICNUMBER6,
            st.UNITTYPEFORGENERICNUMBER6,
            st.GENERICDATE1,
            st.GENERICDATE2,
            st.GENERICDATE3,
            st.GENERICDATE4,
            st.GENERICDATE5,
            st.GENERICDATE6,
            st.GENERICBOOLEAN1,
            st.GENERICBOOLEAN2,
            st.GENERICBOOLEAN3,
            st.GENERICBOOLEAN4,
            st.GENERICBOOLEAN5,
            st.GENERICBOOLEAN6,
            st.PROCESSINGUNITSEQ,
            st.MODIFICATIONDATE,
            st.UNITTYPEFORLINENUMBER,
            st.UNITTYPEFORSUBLINENUMBER,
            st.UNITTYPEFORNUMBEROFUNITS,
            st.UNITTYPEFORDISCOUNTPERCENT,
            st.UNITTYPEFORNATIVECURRENCYAMT,
            st.MODELSEQ,
            ip.BUNAME,
            ip.YEAR,
            ip.QUARTER,
            ip.WRI_DIST_CODE,
            ip.WRI_DIST_NAME,
            ip.WRI_DM_CODE,
            ip.WRI_DM_NAME,
            ip.WRI_AGY_CODE,
            ip.WRI_AGY_NAME,
            ip.WRI_AGY_LDR_CODE,
            ip.WRI_AGY_LDR_NAME,
            ip.WRI_AGT_CODE,
            ip.WRI_AGT_NAME,
            ip.FSC_TYPE,
            ip.RANK,
            ip.CLASS,
            ip.FSC_BSC_GRADE,
            ip.FSC_BSC_PERCENTAGE,
            ip.INSURED_NAME,
            ip.CONTRACT_CAT,
            ip.LIFE_NUMBER,
            ip.COVERAGE_NUMBER,
            ip.RIDER_NUMBER,
            ip.COMPONENT_CODE,
            ip.COMPONENT_NAME,
            ip.ISSUE_DATE,
            ip.INCEPTION_DATE,
            ip.RISK_COMMENCEMENT_DATE,
            ip.FHR_DATE,
            ip.BASE_RIDER_IND,
            ip.TRANSACTION_DATE,
            ip.PAYMENT_MODE,
            ip.POLICY_CURRENCY,
            ip.PROCESSING_PERIOD,
            ip.CREATED_DATE,
            ip.POLICYIDSEQ,
            ip.SUBMITDATE,
            p.periodseq,
            ip.FAOB_AGT_CODE,
            '',
            ''
        FROM
            st
        INNER JOIN
            AMR
            ON st.PONUMBER = AMR.PONUMBER
            AND st.VALUE = AMR.AI_PAYMENT
            AND st.ACCOUNTINGDATE = AMR.COMPENSATIONDATE
            --AND st.GENERICATTRIBUTE11 = AMR.PAYEE_CODE
            AND (st.GENERICATTRIBUTE11 = AMR.NEW_AGENT_CD
            OR st.GENERICATTRIBUTE11 = AMR.OLD_AGENT_CD) ----Changes done to fix not getting AGY AI records --Gopi-04072019
            AND st.GENERICDATE2 = AMR.POLICY_INCEPTION_DATE
            AND st.rn = AMR.rn
        INNER JOIN
            ip
            ON IP.BUNAME = :STR_BUNAME_FA
            AND AMR.PONUMBER = IP.PONUMBER
            /*AND ST.GENERICATTRIBUTE29         = IP.LIFE_NUMBER
            AND ST.GENERICATTRIBUTE30         = IP.COVERAGE_NUMBER
            AND ST.GENERICATTRIBUTE31         = IP.RIDER_NUMBER*/
            -- and AMR.PAYEE_CODE = IP.WRI_AGT_CODE
            AND (AMR.NEW_AGENT_CD = IP.WRI_AGT_CODE
            OR AMR.OLD_AGENT_CD = IP.WRI_AGT_CODE) --Changes done to fix not getting AGY AI records --Gopi-04072019
            AND AMR.component_CODE = ip.component_CODE
            AND AMR.policy_inception_date = ip.inception_date
            AND AMR.risk_commencement_date = ip.risk_commencement_date
            AND AMR.rn = IP.rn
        INNER JOIN
            CS_PERIOD p
            ON st.compensationdate >= p.startdate
            AND st.compensationdate < p.enddate
            AND p.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND p.calendarseq = :V_CALENDARSEQ
            AND p.periodtypeseq = 2814749767106561
        WHERE
            p.removedate = to_date('2200-01-01','yyyy-mm-dd');--Cosimo
    /* ORIGSQL: to_date('2200-01-01','yyyy-mm-dd') */

    /* ORIGSQL: EXT.Log('SP_STAGE_COMP_ONG_PASTTX_FA AI ended, tx up to ' || (TO_DATE(P_STR_CB_ONG_STARTDATE, STR_DATE_FORMAT_TYPE) -1) || ' with count = ' || SQL%ROWCOUNT) */
    CALL EXT.Log('SP_STAGE_COMP_ONG_PASTTX_FA AI ended, tx up to '|| (TO_DATE(ADD_SECONDS(TO_DATE(:P_STR_CB_ONG_STARTDATE, :STR_DATE_FORMAT_TYPE),(86400*-1)))) || ' with count = '|| ::ROWCOUNT);  

    /* ORIGSQL: commit; */
    COMMIT;
END;
END