CREATE PROCEDURE EXT.SP_INBOUND_POST_RMT0081
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_POST_RMT0081';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_POST_RMT0081'; */
    DECLARE v_Sql VARCHAR(20000);  /* ORIGSQL: v_Sql VARCHAR2(20000); */
    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */

    /* ORIGSQL: EXECUTE immediate 'alter session set nls_date_format = ''DD-MON-YYYY'' ' ; */
    /* ORIGSQL: alter session set nls_date_format = 'DD-MON-YYYY' ; */
    --CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        EXT.INBOUND_CFG_PARAMETER;

    /* field3 - Dealer code. field13 mm/dd/yyyy comp date*/ 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_Data_assignment tgt using (SELECT MAX(mgr.name) AS mgrname, t(...) */
    MERGE INTO ext.inbound_Data_assignment AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_ASSIGNMENT' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_TXN' not found */
        USING
        (
            SELECT   /* ORIGSQL: (select MAX(mgr.name) mgrname, ta.orderid, ta.linenumber, ta.sublinenumber from (...) */
                MAX(mgr.name) AS mgrname,
                ta.orderid,
                ta.linenumber,
                ta.sublinenumber
            FROM
                inbound_Data_assignment ta
            INNER JOIN
                inbound_data_txn st
                ON st.orderid = ta.orderid
                AND st.linenumber = ta.linenumber
                AND st.sublinenumber = ta.sublinenumber
                AND st.recordstatus = 0
                AND st.filename = :v_inbound_cfg_parameter.file_name
                AND st.filedate = :v_inbound_cfg_parameter.file_Date
            INNER JOIN
               cs_position pos
                ON st.genericattribute4 = pos.name
            INNER JOIN
                cs_position mgr
                ON pos.managerseq = mgr.ruleelementownerseq
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                pos.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND mgr.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND st.compensationdate BETWEEN pos.effectivestartdate AND Add_Days(pos.effectiveenddate,-1)
                AND st.compensationdate BETWEEN mgr.effectivestartdate AND Add_Days(mgr.effectiveenddate,-1)
            GROUP BY
                ta.orderid, ta.linenumber, ta.sublinenumber
        ) AS src
        ON (src.orderid = tgt.orderid
            AND src.linenumber = tgt.linenumber
            AND src.sublinenumber = tgt.sublinenumber
        	AND filename = :v_inbound_cfg_parameter.file_name
            AND filedate = :v_inbound_cfg_parameter.file_Date
            AND recordstatus = 0
        )
    WHEN MATCHED THEN
        UPDATE SET tgt.genericattribute1 = src.mgrname
        --WHERE
            --filename = :v_inbound_cfg_parameter.file_name
            --AND filedate = :v_inbound_cfg_parameter.file_Date
            --AND recordstatus = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'ManagerName Update on GA1 :' || v_inbound_cfg_(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'ManagerName Update on GA1 :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'ManagerName Update on GA1 Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'ManagerName Update on GA1 :' || v_inbound_cfg_parameter.f(...) */

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_Data_assignment tgt using (SELECT MAX(mgr.name) AS oldmgrname(...) */
    MERGE INTO ext.inbound_Data_assignment AS tgt  
        USING
        (
            SELECT   /* ORIGSQL: (select MAX(mgr.name) oldmgrname, ta.orderid, ta.linenumber, ta.sublinenumber fr(...) */
                MAX(mgr.name) AS oldmgrname,
                ta.orderid,
                ta.linenumber,
                ta.sublinenumber
            FROM
                inbound_Data_assignment ta
            INNER JOIN
                inbound_data_txn st
                ON st.orderid = ta.orderid
                AND st.linenumber = ta.linenumber
                AND st.sublinenumber = ta.sublinenumber
                AND st.recordstatus = 0
                AND st.filename = :v_inbound_cfg_parameter.file_name
                AND st.filedate = :v_inbound_cfg_parameter.file_Date
            INNER JOIN
                cs_position pos
                ON st.genericattribute4 = pos.name
            INNER JOIN
                cs_position mgr
                ON pos.managerseq = mgr.ruleelementownerseq
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                pos.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND mgr.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND TO_DATE(ADD_SECONDS(st.compensationdate,(86400*(-1*IFNULL(pos.genericnumber1,0)))))
                BETWEEN pos.effectivestartdate AND Add_Days(pos.effectiveenddate,-1)  /* ORIGSQL: st.compensationdate-IFNULL(pos.genericnumber1,0) */
                                                                           /* ORIGSQL: nvl(pos.genericnumber1,0) */
                AND TO_DATE(ADD_SECONDS(st.compensationdate,(86400*(-1*IFNULL(pos.genericnumber1,0)))))
                BETWEEN mgr.effectivestartdate AND Add_Days(mgr.effectiveenddate,-1)  /* ORIGSQL: st.compensationdate-IFNULL(pos.genericnumber1,0) */
                                                                           /* ORIGSQL: nvl(pos.genericnumber1,0) */
            GROUP BY
                ta.orderid, ta.linenumber, ta.sublinenumber
        ) AS src
        ON (src.orderid = tgt.orderid
            AND src.linenumber = tgt.linenumber
            AND src.sublinenumber = tgt.sublinenumber
        	AND filename = :v_inbound_cfg_parameter.file_name
            AND filedate = :v_inbound_cfg_parameter.file_Date
            AND recordstatus = 0
        )
    WHEN MATCHED THEN
        UPDATE SET tgt.genericattribute2 = src.oldmgrname
        --WHERE
            --filename = :v_inbound_cfg_parameter.file_name
            --AND filedate = :v_inbound_cfg_parameter.file_Date
            --AND recordstatus = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Old ManagerName Update on GA2 :' || v_inbound_(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Old ManagerName Update on GA2 :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Old ManagerName Update on GA2 Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Old ManagerName Update on GA2 :' || v_inbound_cfg_paramet(...) */

    /* ORIGSQL: commit; */
    COMMIT;
END