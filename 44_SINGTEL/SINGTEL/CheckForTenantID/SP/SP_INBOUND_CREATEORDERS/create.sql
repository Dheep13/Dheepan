CREATE PROCEDURE EXT.SP_INBOUND_CREATEORDERS
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_maxseq DECIMAL(38,10);  /* ORIGSQL: v_maxseq number; */
    DECLARE v_startdate TIMESTAMP;  /* ORIGSQL: v_startdate date; */
    DECLARE v_Eot TIMESTAMP = TO_DATE('21991231','YYYYMMDD');  /* ORIGSQL: v_Eot date:=TO_DATE('21991231','YYYYMMDD') ; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_CREATEORDERS';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_CREATEORDERS'; */
    DECLARE v_parameter ROW LIKE Inbound_cfg_Parameter; --%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.Inbound_cfg_Parameter' not found (for %ROWTYPE declaration) */
    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount integer; */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT
        DISTINCT
        *
    INTO
        v_parameter
    FROM
        EXT.inbound_cfg_Parameter;

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Start :' || v_parameter.file_type || '-FileNam(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Start   :'|| IFNULL(:v_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_date,''),1,255) 
        , 'Starting Proc', 0, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Start   :' || v_parameter.file_type || '-FileName:' || v_(...) */
    BEGIN 
        DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
            /* ORIGSQL: WHEN NO_DATA_FOUND THEN */
            BEGIN
                /* ORIGSQL: NULL; */
                DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
            END;


        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */

        SELECT
            TO_DATE(MAX(field1),'YYYYMMDD')
        INTO
            v_startdate
        FROM
            EXT.inbound_Data_Staging;

        /* ORIGSQL: EXCEPTION WHEN NO_DATA_FOUND THEN */
    END;
    BEGIN 
        DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
            /* ORIGSQL: WHEN NO_DATA_FOUND THEN */
            BEGIN
                /* ORIGSQL: NULL; */
                DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
            END;



        SELECT
            ext.trunc(CURRENT_TIMESTAMP, 'DD')  /* ORIGSQL: trunc(Sysdate) */
        INTO
            v_startdate
        FROM
            SYS.DUMMY   /* ORIGSQL: FROM dual where v_Startdate IS NULL ; */
        WHERE
            :v_startdate IS NULL;

        /* ORIGSQL: EXCEPTION WHEN NO_DATA_FOUND THEN */
    END;

    v_maxseq = 1;

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Variables V_maxseq and v_Startdate :' || v_par(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Variables V_maxseq and v_Startdate   :'|| IFNULL(:v_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_date,''),1,255) 
        , 'v_maxseq: '||IFNULL(TO_VARCHAR(:v_maxseq),'')||' v_Startdate: '||IFNULL(TO_VARCHAR(:v_startdate,'YYYYMMDD'),'')   /* ORIGSQL: SUBSTR(v_proc_name || 'Variables V_maxseq and v_Startdate   :' || v_parameter.fi(...) */
        , 0, NULL, NULL);  /* ORIGSQL: to_char(v_startdate,'YYYYMMDD') */

    --, 'ConnCount'||ta.positionname as orderid,  to_Char(last_day(st.compensationdate),'YYYYMMDD') as LineNumber 

    /* ORIGSQL: insert into inbound_Data_staging (filetype, filename, filedate, seq, field1, fie(...) */
    INSERT INTO EXT.inbound_Data_staging
        (
            filetype, filename, filedate, seq, field1, field2,
            field3, field4, field5, field6
        )
        SELECT   /* ORIGSQL: select p.file_Type, p.file_name, p.file_Date, v_maxseq+ROW_NUMBER() OVER (ORDER (...) */
            p.file_Type,
            p.file_name,
            p.file_Date,
            :v_maxseq+ROW_NUMBER() OVER (ORDER BY 0*0),  /* ORIGSQL: rownum */
            SUBSTRING(x.orderid,1,40),  /* ORIGSQL: substr(x.orderid,1,40) */
            x.linenum,
            ROW_NUMBER() OVER (PARTITION BY   x.orderid, x.linenum ORDER BY x.product) AS subline,
            x.product,/* --field4 */  x.position,/* --field5 */   'DATA' AS field6
        FROM
            (
                SELECT   /* ORIGSQL: (select distinct to_char('ConnCnt-'||pos.name||'-'||y.product) orderid, to_Char((...) */
                    DISTINCT
                    TO_VARCHAR('ConnCnt-'||IFNULL(pos.name,'')||'-'||IFNULL(y.product,''),NULL) AS orderid,
                    TO_VARCHAR(TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))),'YYYYMMDD') AS linenum,  /* ORIGSQL: to_Char(pd.enddate-1,'YYYYMMDD') */
                    pos.name AS position,
                    y.product
                FROM
                   cs_position pos
                INNER JOIN
                    cs_title t
                    ON pos.titleseq = t.ruleelementownerseq
                    AND t.removedate > :v_Eot
                    AND t.effectiveenddate > :v_Eot
                INNER JOIN
                    cs_period pd
                    ON TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))) >= :v_startdate  /* ORIGSQL: pd.enddate-1 */
                    AND pd.startdate <= :v_startdate
                    AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))) BETWEEN pos.effectivestartdate AND pos.effectiveenddate  /* ORIGSQL: pd.enddate-1 */
                INNER JOIN
                    cs_Calendar c
                    ON pd.calendarseq = c.calendarseq
                    AND c.removedate > :v_Eot
                INNER JOIN
                    cs_periodtype pt
                    ON pt.periodtypeseq = pd.periodtypeseq
                    AND pt.removedate > :v_Eot
                INNER JOIN
                    (
                        SELECT   /* ORIGSQL: (select distinct REPLACE(Dim0, ' ','') product from stel_lookup@stelext where na(...) */
                            DISTINCT
                            REPLACE(Dim0, ' ','') AS product
                        FROM
                            EXT.stel_lookup
                            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.stel_lookup@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.stel_lookup_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        WHERE
                            name LIKE 'LT_Internal_Product_Target_Indv'
                    ) AS y
                    ON 1 = 1
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_title@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_title_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_periodtype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_periodtype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_period@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_period_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_Calendar@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_Calendar_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                WHERE
                    pos.removedate > :v_Eot
                    AND c.name LIKE '%Singtel%Mon%'
                    AND pt.name = 'month'
                    AND (t.name LIKE 'Digital%'
                        OR t.name LIKE 'Retail%'
                        OR t.name LIKE 'Singtel Shop%'
                    OR t.name LIKE 'STS%')
            ) AS x
        INNER JOIN
            EXT.inbound_cfg_parameter p
            ON 1 = 1
        WHERE
            (x.orderid,x.linenum) NOT IN
            (
                SELECT   /* ORIGSQL: (select orderid, linenumber from vw_Salestransaction@stelext where eventtypeid='(...) */
                    orderid,
                    linenumber
                FROM
                    EXT.vw_Salestransaction
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.vw_Salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.vw_Salestransaction_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                WHERE
                    eventtypeid = 'Aggregated Connection Count'
                    AND compensationdate >= :v_startdate
            );

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert into INBOUND_DATA_STAGING :' || v_param(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert into INBOUND_DATA_STAGING   :'|| IFNULL(:v_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_date,''),1,255) 
        , 'INSERT into INBOUND_DATA_STAGING  Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert into INBOUND_DATA_STAGING   :' || v_parameter.file(...) */

    /* ORIGSQL: commit; */
    COMMIT;
END