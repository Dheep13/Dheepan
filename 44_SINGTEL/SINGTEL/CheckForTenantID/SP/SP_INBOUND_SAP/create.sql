CREATE PROCEDURE EXT.SP_INBOUND_SAP
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_SAP';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_SAP'; */

    DECLARE v_inb_param  ROW LIKE INBOUND_CFG_PARAMETER;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inb_param
    FROM
        INBOUND_CFG_PARAMETER;

    /* ORIGSQL: sp_inbound_Equipmentprice() */
    CALL EXT.SP_INBOUND_EQUIPMENTPRICE();

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Completed SP_INB_EQP PRICE - EXIT' || v_inb_pa(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Completed SP_INB_EQP PRICE - EXIT'|| IFNULL(:v_inb_param.file_type,'') || '-FileName:'|| IFNULL(:v_inb_param.file_name,'') || '-Date:'|| IFNULL(:v_inb_param.file_date,''),1,255) 
        , 'Completed SP_INB_EQP PRICE - EXIT', 0, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Completed SP_INB_EQP PRICE - EXIT' || :v_inb_param.file_ty(...) */

    /* ORIGSQL: update STEL_DATA_TransferCost sap SET sap.enddate = (SELECT to_date(st.field6,'D(...) */
    UPDATE STEL_DATA_TransferCost sap
        /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_DATA_STAGING' not found */
        SET
        /* ORIGSQL: sap.enddate = */
        enddate = (
            SELECT   /* ORIGSQL: (select to_date(st.field6,'DD/MM/YYYY') -1 from inbound_data_staging st where st(...) */
                -- TO_DATE(ADD_SECONDS(to_date(st.field6,'DD/MM/YYYY'),(86400*-1))) /*Deepan : simpler code provided below*/
                ADD_DAYS(TO_DATE(st.field6, 'DD/MM/YYYY'), -1)
            FROM
                inbound_data_staging st
            WHERE
                st.field1 = sap.Customer
                AND st.field4 = sap.stockcode
                AND st.field2 = sap.salesorg
                AND st.field3 = sap.distchannel
                AND to_date(st.field6,'DD/MM/YYYY') <> sap.startdate  /* ORIGSQL: to_date(st.field6,'DD/MM/YYYY') */
        )
    WHERE
        sap.startdate
        /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_TRANSFERCOST' not found */
        =
        (
            SELECT   /* ORIGSQL: (select MAX(startdate) from STEL_DATA_TransferCost tc,inbound_data_staging st wh(...) */
                MAX(startdate)
            FROM
                STEL_DATA_TransferCost tc,
                inbound_data_staging st
            WHERE
                --st.field1=sap.Customer and st.field4=sap.stockcode and st.field2=sap.salesorg and st.field3=sap.distchannel
                st.field1 = tc.Customer
                AND st.field4 = tc.stockcode
                AND st.field2 = tc.salesorg
                AND st.field3 = tc.distchannel
        );

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update STEL_DATA_TransferCost EndDate :' || v_(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update STEL_DATA_TransferCost EndDate :'|| IFNULL(:v_inb_param.file_type,'') || '-FileName:'|| IFNULL(:v_inb_param.file_name,'') || '-Date:'|| IFNULL(:v_inb_param.file_date,''),1,255) 
        , 'Update STEL_DATA_TransferCost EndDate Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update STEL_DATA_TransferCost EndDate :' || :v_inb_param.f(...) */

    /* ORIGSQL: insert into STEL_DATA_TransferCost select st.FILEDATE, st.FILENAME, st.FIELD1, s(...) */
    INSERT INTO STEL_DATA_TransferCost
        SELECT   /* ORIGSQL: select st.FILEDATE, st.FILENAME, st.FIELD1, st.FIELD2, st.FIELD3, st.FIELD4, st.(...) */
            st.FILEDATE,
            st.FILENAME,
            st.FIELD1,
            st.FIELD2,
            st.FIELD3,
            st.FIELD4,
            st.FIELD5,
            to_date(st.FIELD6,'DD/MM/YYYY'),  /* ORIGSQL: TO_DATE(st.FIELD6,'DD/MM/YYYY') */
            to_date(st.FIELD7,'DD/MM/YYYY'),  /* ORIGSQL: TO_DATE(st.FIELD7,'DD/MM/YYYY') */
            TO_DECIMAL(st.FIELD8,38,18)  /* ORIGSQL: TO_NUMBER(st.FIELD8) */
        FROM
            inbound_data_staging st,
            STEL_DATA_TransferCost sap
        WHERE
            st.FILENAME = :v_inb_param.FILE_NAME
            AND st.FILETYPE = :v_inb_param.FILE_TYPE
            AND st.FILEDATE = :v_inb_param.FILE_DATE
            AND st.field1 = sap.Customer
            AND st.field4 = sap.stockcode
            AND st.field2 = sap.salesorg
            AND st.field3 = sap.distchannel
            AND to_date(st.field6,'DD/MM/YYYY') <> sap.startdate;  /* ORIGSQL: to_date(st.field6,'DD/MM/YYYY') */

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '1. Insert into STEL_DATA_TransferCost :' || v_(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '1. Insert into STEL_DATA_TransferCost :'|| IFNULL(:v_inb_param.file_type,'') || '-FileName:'|| IFNULL(:v_inb_param.file_name,'') || '-Date:'|| IFNULL(:v_inb_param.file_date,''),1,255) 
        , '1. Insert into STEL_DATA_TransferCost Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '1. Insert into STEL_DATA_TransferCost :' || :v_inb_param.f(...) */

    /* ORIGSQL: insert into STEL_DATA_TransferCost select st.FILEDATE, st.FILENAME, st.FIELD1, s(...) */
    INSERT INTO STEL_DATA_TransferCost
        SELECT   /* ORIGSQL: select st.FILEDATE, st.FILENAME, st.FIELD1, st.FIELD2, st.FIELD3, st.FIELD4, st.(...) */
            st.FILEDATE,
            st.FILENAME,
            st.FIELD1,
            st.FIELD2,
            st.FIELD3,
            st.FIELD4,
            st.FIELD5,
            to_date(st.FIELD6,'DD/MM/YYYY'),  /* ORIGSQL: TO_DATE(st.FIELD6,'DD/MM/YYYY') */
            to_date(st.FIELD7,'DD/MM/YYYY'),  /* ORIGSQL: TO_DATE(st.FIELD7,'DD/MM/YYYY') */
            TO_DECIMAL(st.FIELD8,38,18)  /* ORIGSQL: TO_NUMBER(st.FIELD8) */
        FROM
            inbound_data_staging st
        WHERE
            st.FILENAME = :v_inb_param.FILE_NAME
            AND st.FILETYPE = :v_inb_param.FILE_TYPE
            AND st.FILEDATE = :v_inb_param.FILE_DATE
            AND NOT EXISTS
            (
                SELECT   /* ORIGSQL: (select 1 FROM inbound_data_staging st,STEL_DATA_TransferCost sap WHERE st.field(...) */
                    1
                FROM
                    inbound_data_staging st,
                    STEL_DATA_TransferCost sap
                WHERE
                    st.field1 = sap.Customer
                    AND st.field4 = sap.stockcode
                    AND st.field2 = sap.salesorg
                    AND st.field3 = sap.distchannel
            );

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '2. Insert into STEL_DATA_TransferCost :' || v_(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '2. Insert into STEL_DATA_TransferCost :'|| IFNULL(:v_inb_param.file_type,'') || '-FileName:'|| IFNULL(:v_inb_param.file_name,'') || '-Date:'|| IFNULL(:v_inb_param.file_date,''),1,255) 
        , '2. Insert into STEL_DATA_TransferCost Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '2. Insert into STEL_DATA_TransferCost :' || :v_inb_param.f(...) */
END