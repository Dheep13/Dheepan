CREATE PROCEDURE EXT.SP_INBOUND_WORKINGDAYS
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_table VARCHAR(255) = 'LT_Working_Days';  /* ORIGSQL: v_table varchar2(255):= 'LT_Working_Days'; */
    DECLARE v_startdate TIMESTAMP = TO_DATE('20150401','YYYYMMDD');  /* ORIGSQL: v_startdate date:= TO_DATE('20150401','YYYYMMDD') ; */
    DECLARE v_enddate TIMESTAMP = TO_DATE('22000101','YYYYMMDD');  /* ORIGSQL: v_enddate date:= TO_DATE('22000101','YYYYMMDD') ; */
    DECLARE v_unit VARCHAR(50) = 'quantity';  /* ORIGSQL: v_unit varchar2(50):='quantity'; */
    DECLARE v_inbound_cfg_parameter ROW LIKE inbound_cfg_parameter;--%rowtype;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_WORKINGDAYS';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_WORKINGDAYS'; */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        inbound_cfg_parameter;

    /*populate Working days LT based on configured leave days*/

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_PLMC' not found */

    /* ORIGSQL: INSERT INTO INBOUND_DATA_PLMC (FILEDATE, FILENAME, RECORDSTATUS, DOWNLOADED, MDL(...) */
    INSERT INTO INBOUND_DATA_PLMC
        (
            FILEDATE,
            FILENAME,
            RECORDSTATUS,
            DOWNLOADED,
            MDLTNAME,
            EFFECTIVESTARTDATE,
            EFFECTIVEENDDATE,
            VALUE,
            UNITTYPEFORVALUE,
            DIM0LOW,
            DIM0HIGH,
            DIM0NAME,
            DIM1LOW,
            DIM1HIGH,
            DIM1NAME,
            DIM2LOW,
            DIM2HIGH,
            DIM2NAME
        )
        SELECT   /* ORIGSQL: select trunc(sysdate), v_table, '0' recstatus,'0' downloaded, v_table, v_Startda(...) */
            trunc(CURRENT_TIMESTAMP, 'DD'),  
            :v_table,
            '0' AS recstatus,
            '0' AS downloaded,
            :v_table,
            :v_startdate,
            :v_enddate,
            WorkingdaysInEndMonth,
            :v_unit,
            channel AS dim0,
            channel,
            'dummy',
            TO_VARCHAR(caldate,'YYYY') AS dim1,  /* ORIGSQL: to_Char(caldate,'YYYY') */
            TO_VARCHAR(caldate,'YYYY'),  /* ORIGSQL: to_Char(caldate,'YYYY') */
            'dummy',
            TO_VARCHAR(caldate,'MM') AS dim2,  /* ORIGSQL: to_Char(caldate,'MM') */
            TO_VARCHAR(caldate,'MM'),  /* ORIGSQL: to_Char(caldate,'MM') */
            'dummy'
        FROM
            STEL_WORKINGDAYS;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Working Days Load into INBOUND_DATA_PLMC :' ||(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Working Days Load into INBOUND_DATA_PLMC :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Working Days Load into INBOUND_DATA_PLMC Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Working Days Load into INBOUND_DATA_PLMC :' || v_inbound_(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: SP_MDLTDIMENSION_MERGER(v_table, trunc(sysdate)) */
    CALL ext.SP_MDLTDIMENSION_MERGER(:v_table, trunc(CURRENT_TIMESTAMP, 'DD') 
        /* ORIGSQL: trunc(sysdate) */
    );  
END