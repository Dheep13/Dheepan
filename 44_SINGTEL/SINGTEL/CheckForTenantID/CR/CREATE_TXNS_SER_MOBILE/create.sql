CREATE PROCEDURE EXT.CREATE_TXNS_SER_MOBILE
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_default_date TIMESTAMP = to_date('01-JAN-1900', 'DD-MON-YYYY');  /* ORIGSQL: v_default_date DATE := TO_DATE('01-JAN-1900', 'DD-MON-YYYY') ; */

    DECLARE v_eot TIMESTAMP = to_date('01-JAN-2200', 'DD-MON-YYYY');  /* ORIGSQL: v_eot DATE := TO_DATE('01-JAN-2200', 'DD-MON-YYYY') ; */

    DECLARE v_tenant VARCHAR(4) = 'STEL';  /* ORIGSQL: v_tenant VARCHAR(4) := 'STEL'; */
    DECLARE v_max_mod_date TIMESTAMP;  /* ORIGSQL: v_max_mod_date DATE; */
    DECLARE v_running_num DECIMAL(38,10);  /* ORIGSQL: v_running_num NUMBER; */
    DECLARE v_param ROW LIKE inbound_cfg_parameter;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */
    DECLARE v_eventtype VARCHAR(30);  /* ORIGSQL: v_eventtype varchar(30); */
    DECLARE v_Period VARCHAR(6);  /* ORIGSQL: v_Period varchar2(6); */

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_DATA_STAGING' not found */

    SELECT
        CASE
            WHEN LOWER(TRIM(field1)) = 'r1' 
            THEN 'Mobile SER R1'
            WHEN LOWER(TRIM(field1)) = 'r2' 
            THEN 'Mobile SER R2'
            ELSE 'XYZ'
        END
    INTO
        v_eventtype
    FROM
        ext.inbound_Data_staging
    LIMIT 1;  /* ORIGSQL: rownum =1 */

    SELECT
        TRIM(field2)
    INTO
        v_Period
    FROM
        ext.inbound_Data_staging
    LIMIT 1;  /* ORIGSQL: rownum =1 */

    /* ORIGSQL: execute immediate 'Truncate table inbound_Data_staging'; */
    /* ORIGSQL: Truncate table inbound_Data_staging ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ext.inbound_Data_staging';

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_param
    FROM
        ext.Inbound_cfg_Parameter
    WHERE
        object_name = 'SP_INBOUND_TXN_MAP';

    /* RESOLVE: Identifier not found: Table/view 'EXT.TXNS_TO_BE_CREATED' not found */

    SELECT
        IFNULL(MAX(maxmodificationdate), :v_default_date),  /* ORIGSQL: NVL(MAX (maxmodificationdate), v_default_date) */
        IFNULL(MAX(running_number), 0)  /* ORIGSQL: NVL(MAX (running_number), 0) */
    INTO
        v_max_mod_date,
        v_running_num
    FROM
        txns_to_be_created
    WHERE
        IFNULL(txns_created_flag, 0) = 1;  /* ORIGSQL: NVL(txns_created_flag, 0) */

    /* ORIGSQL: INSERT INTO txns_to_be_created (eventtypeid, linenumber, numberofunits, generica(...) */
    INSERT INTO ext.txns_to_be_created
        (
            eventtypeid,
            linenumber,
            numberofunits,
            genericattribute3,
            genericattribute4,
            genericattribute5,
            genericattribute7,
            genericattribute9,
            genericattribute14,
            genericattribute18,
            genericattribute19,
            genericattribute20,
            maxmodificationdate,
            filename,
            filedate
        )
        SELECT   /* ORIGSQL: SELECT evt.eventtypeid, TO_NUMBER(TO_CHAR (st.compensationdate, 'YYYYMM')) LineN(...) */
            evt.eventtypeid,
            TO_DECIMAL(TO_VARCHAR(st.compensationdate,'YYYYMM'),38,18) AS LineNumber,  /* ORIGSQL: TO_NUMBER(TO_CHAR (st.compensationdate, 'YYYYMM')) */
                                                                                       /* ORIGSQL: TO_CHAR(st.compensationdate, 'YYYYMM') */
            IFNULL(st.numberofunits, 0) AS numberofunits,  /* ORIGSQL: NVL(st.numberofunits, 0) */
            IFNULL(st.genericattribute3, 'x') AS genericattribute3,  /* ORIGSQL: NVL(st.genericattribute3, 'x') */
            IFNULL(st.genericattribute4, 'x') AS genericattribute4,  /* ORIGSQL: NVL(st.genericattribute4, 'x') */
            IFNULL(st.genericattribute5, 'x') AS genericattribute5,  /* ORIGSQL: NVL(st.genericattribute5, 'x') */
            IFNULL(st.genericattribute7, 'x') AS genericattribute7,/* --commented Arun/Nag 26/4 */ IFNULL(st.genericattribute9, 'x') AS genericattribute9,  /* ORIGSQL: NVL(st.genericattribute9, 'x') */
                                                                                                                                                            /* ORIGSQL: NVL(st.genericattribute7, 'x') */
            IFNULL(st.genericattribute14, 'x') AS genericattribute14,  /* ORIGSQL: NVL(st.genericattribute14, 'x') */
            IFNULL(st.genericattribute18, 'x') AS genericattribute18,  /* ORIGSQL: NVL(st.genericattribute18, 'x') */
            IFNULL(st.genericattribute19, 'x') AS genericattribute19,  /* ORIGSQL: NVL(st.genericattribute19, 'x') */
            IFNULL(st.genericattribute20, 'x') AS genericattribute20,  /* ORIGSQL: NVL(st.genericattribute20, 'x') */
            st.modificationdate,
            :v_param.file_name,
            :v_param.file_date
        FROM
            cs_eventtype evt
        INNER JOIN
          cs_salestransaction st
            ON (evt.datatypeseq = st.eventtypeseq)
        LEFT OUTER JOIN
            EXT.stel_lookup mdlt
            ON (mdlt.name = 'LT_Mobile_SER/TEPL_ComboPlan_Mapping'
                AND st.genericattribute7 = mdlt.dim0
                AND st.compensationdate >= mdlt.effectivestartdate
            AND st.compensationdate < mdlt.effectiveenddate)
            /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_lookup@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.stel_lookup'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'EXT.cs_salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.cs_salestransaction'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'EXT.cs_eventtype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.cs_eventtype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        WHERE
            evt.eventtypeid = :v_eventtype --IN ('Mobile Closed', 'Mobile Submitted')
            AND evt.removedate = :v_eot
            AND evt.tenantid = :v_tenant
            AND st.tenantid = :v_tenant
            AND IFNULL(st.genericboolean1, 0) = 0  /* ORIGSQL: NVL(st.genericboolean1, 0) */
            AND TO_DECIMAL(TO_VARCHAR(st.compensationdate,'YYYYMM'),38,18) = TO_DECIMAL(:v_Period,38,18)  /* ORIGSQL: TO_NUMBER(v_Period) */
                                                                                                          /* ORIGSQL: TO_NUMBER(TO_CHAR (st.compensationdate, 'YYYYMM')) */
                                                                                                          /* ORIGSQL: TO_CHAR(st.compensationdate, 'YYYYMM') */
            -- AND st.modificationdate > v_max_mod_date
            AND NOT EXISTS
            (
                SELECT   /* ORIGSQL: (select 1 from cs_salestransaction@stelext st1,cs_Eventtype@stelext et1 where et(...) */
                    1
                FROM
                    cs_salestransaction st1,
                    cs_Eventtype et1
                    /* RESOLVE: Oracle Database link: Remote table/view 'EXT.cs_salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.cs_salestransaction'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'EXT.cs_Eventtype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.cs_Eventtype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                WHERE
                    et1.eventtypeid IN('Mobile SER R1 Aggregated','Mobile SER R2 Aggregated')
                    AND et1.datatypeseq = st.eventtypeseq
                    AND et1.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND TO_DECIMAL(TO_VARCHAR(st1.compensationdate,'YYYYMM'),38,18) = TO_DECIMAL(:v_Period,38,18)  /* ORIGSQL: TO_NUMBER(v_Period) */
                                                                                                                   /* ORIGSQL: TO_NUMBER(TO_CHAR (st1.compensationdate, 'YYYYMM')) */
                                                                                                                   /* ORIGSQL: TO_CHAR(st1.compensationdate, 'YYYYMM') */
                    AND TO_DECIMAL(TO_VARCHAR(st1.compensationdate,'YYYYMM'),38,18) =TO_DECIMAL(TO_VARCHAR(st.compensationdate,'YYYYMM'),38,18)  /* ORIGSQL: TO_NUMBER(TO_CHAR (st1.compensationdate, 'YYYYMM')) */
                                                                                                                                                 /* ORIGSQL: TO_NUMBER(TO_CHAR (st.compensationdate, 'YYYYMM')) */
                                                                                                                                                 /* ORIGSQL: TO_CHAR(st1.compensationdate, 'YYYYMM') */
                                                                                                                                                 /* ORIGSQL: TO_CHAR(st.compensationdate, 'YYYYMM') */
                    AND IFNULL(st1.genericattribute3, 'x') =IFNULL(st.genericattribute3, 'x')  /* ORIGSQL: NVL(st1.genericattribute3, 'x') */
                                                                                               /* ORIGSQL: NVL(st.genericattribute3, 'x') */
                    AND IFNULL(st1.genericattribute4, 'x') =IFNULL(st.genericattribute4, 'x')  /* ORIGSQL: NVL(st1.genericattribute4, 'x') */
                                                                                               /* ORIGSQL: NVL(st.genericattribute4, 'x') */
                    AND IFNULL(st1.genericattribute5, 'x') =IFNULL(st.genericattribute5, 'x')  /* ORIGSQL: NVL(st1.genericattribute5, 'x') */
                                                                                               /* ORIGSQL: NVL(st.genericattribute5, 'x') */
                    AND IFNULL(st1.genericattribute7, 'x') =IFNULL(st.genericattribute7, 'x')  /* ORIGSQL: NVL(st1.genericattribute7, 'x') */
                                                                                               /* ORIGSQL: NVL(st.genericattribute7, 'x') */
                    AND IFNULL(st1.genericattribute9, 'x') =IFNULL(st.genericattribute9, 'x')  /* ORIGSQL: NVL(st1.genericattribute9, 'x') */
                                                                                               /* ORIGSQL: NVL(st.genericattribute9, 'x') */
                    AND IFNULL(st1.genericattribute14, 'x') =IFNULL(st.genericattribute14, 'x')  /* ORIGSQL: NVL(st1.genericattribute14, 'x') */
                                                                                                 /* ORIGSQL: NVL(st.genericattribute14, 'x') */
                    AND IFNULL(st1.genericattribute18, 'x') =IFNULL(st.genericattribute18, 'x')  /* ORIGSQL: NVL(st1.genericattribute18, 'x') */
                                                                                                 /* ORIGSQL: NVL(st.genericattribute18, 'x') */
                    AND IFNULL(st1.genericattribute19, 'x') =IFNULL(st.genericattribute19, 'x')  /* ORIGSQL: NVL(st1.genericattribute19, 'x') */
                                                                                                 /* ORIGSQL: NVL(st.genericattribute19, 'x') */
                    AND IFNULL(st1.genericattribute20, 'x') =IFNULL(st.genericattribute20, 'x')  /* ORIGSQL: NVL(st1.genericattribute20, 'x') */
                                                                                                 /* ORIGSQL: NVL(st.genericattribute20, 'x') */
            );

    --                      GROUP BY evt.eventtypeid,
    --               TO_NUMBER (TO_CHAR (st.compensationdate, 'YYYYMM')),
    --               NVL (st.numberofunits, 0),
    --               NVL (st.genericattribute3, 'x'),
    --               NVL (st.genericattribute4, 'x'),
    ----               NVL (st.genericattribute5, 'x'),  --Changed by Nagarjun 29/4, to get full count
    --               NVL (st.genericattribute7, 'x'),
    --               NVL (st.genericattribute9, 'x'),
    --               NVL (st.genericattribute14, 'x'),
    --               NVL (st.genericattribute18, 'x'),
    --               NVL (st.genericattribute19, 'x'),
    --               NVL (st.genericattribute20, 'x');

    /* ORIGSQL: COMMIT; */
    COMMIT;  

    /* ORIGSQL: UPDATE txns_to_be_created tgt SET tgt.running_number = v_running_num + ROWNUM WH(...) */
    
    /*Deepan : New update statement used*/
    UPDATE ext.txns_to_be_created tgt SET tgt.running_number = :v_running_num + (
    SELECT ROWNUM
    FROM (
        SELECT 
            ROW_NUMBER() OVER () AS ROWNUM,
            eventtypeid,
            linenumber,
            numberofunits,
            genericattribute3,
            genericattribute4,
            genericattribute5,
            genericattribute7,
            genericattribute9,
            genericattribute14,
            genericattribute18,
            genericattribute19,
            genericattribute20,
            maxmodificationdate,
            txns_created_flag,
            filename,
            filedate
        FROM ext.txns_to_be_created
        WHERE running_number IS NULL
          AND IFNULL(txns_created_flag, 0) = 0
    ) sub
    WHERE sub.eventtypeid = tgt.eventtypeid
      AND sub.linenumber = tgt.linenumber
      AND sub.numberofunits = tgt.numberofunits
      AND IFNULL(sub.genericattribute3, '') = IFNULL(tgt.genericattribute3, '')
      AND IFNULL(sub.genericattribute4, '') = IFNULL(tgt.genericattribute4, '')
      AND IFNULL(sub.genericattribute5, '') = IFNULL(tgt.genericattribute5, '')
      AND IFNULL(sub.genericattribute7, '') = IFNULL(tgt.genericattribute7, '')
      AND IFNULL(sub.genericattribute9, '') = IFNULL(tgt.genericattribute9, '')
      AND IFNULL(sub.genericattribute14, '') = IFNULL(tgt.genericattribute14, '')
      AND IFNULL(sub.genericattribute18, '') = IFNULL(tgt.genericattribute18, '')
      AND IFNULL(sub.genericattribute19, '') = IFNULL(tgt.genericattribute19, '')
      AND IFNULL(sub.genericattribute20, '') = IFNULL(tgt.genericattribute20, '')
      AND sub.maxmodificationdate = tgt.maxmodificationdate
      AND sub.txns_created_flag = tgt.txns_created_flag
      AND sub.filename = tgt.filename
      AND sub.filedate = tgt.filedate
)
WHERE tgt.running_number IS NULL 
  AND IFNULL(tgt.txns_created_flag, 0) = 0;
    -- UPDATE txns_to_be_created tgt
    --     SET
    --     /* ORIGSQL: tgt.running_number = */
    --     running_number = :v_running_num + ROWNUM   /* RESOLVE: ROWNUM pseudo-column(not converted): Rewrite 'ROWNUM' as TOP/LIMIT or 'ROW_NUMBER() OVER...' with subquery */
    -- FROM
    --     txns_to_be_created tgt
    -- WHERE
    --     tgt.running_number IS NULL
    --     AND IFNULL(tgt.txns_created_flag, 0) = 0;  /* ORIGSQL: NVL(tgt.txns_created_flag, 0) */

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Create data in Staging Table 

    /* ORIGSQL: INSERT INTO inbound_Data_staging (filetype, filename, filedate, seq, field1, fie(...) */
    INSERT INTO inbound_Data_staging
        (
            filetype,
            filename,
            filedate,
            seq,
            field1,
            field2,
            field3,
            field4,
            field5,
            field6,
            field7,
            field8,
            field9,
            field10,
            field11,
            field12,
            field13,
            field14,
            field15
        )
        SELECT   /* ORIGSQL: SELECT v_param.file_type, v_param.file_name, v_param.file_date, ROW_NUMBER() OVE(...) */
            :v_param.file_type,
            :v_param.file_name,
            :v_param.file_date,
            ROW_NUMBER() OVER (ORDER BY 0*0),  /* ORIGSQL: ROWNUM */
            eventtypeid,
            linenumber,
            numberofunits,
            genericattribute3,
            genericattribute4,
            genericattribute5,
            genericattribute7,
            genericattribute9,
            genericattribute14,
            genericattribute18,
            genericattribute19,
            genericattribute20,
            maxmodificationdate,
            txns_created_flag,
            running_number
        FROM
            ext.txns_to_be_created
        WHERE
            IFNULL(txns_created_flag, 0) = 0;  /* ORIGSQL: NVL(txns_created_flag, 0) */

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Update creation flag   

    /* ORIGSQL: UPDATE txns_to_be_created tgt SET tgt.txns_created_flag = 1 WHERE NVL(txns_creat(...) */
    UPDATE ext.txns_to_be_created tgt
        SET
        /* ORIGSQL: tgt.txns_created_flag = */
        txns_created_flag = 1
    FROM
        ext.txns_to_be_created tgt
    WHERE
        IFNULL(txns_created_flag, 0) = 0;  /* ORIGSQL: NVL(txns_created_flag, 0) */

    /* ORIGSQL: COMMIT; */
    COMMIT;
END