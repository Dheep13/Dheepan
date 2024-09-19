CREATE PROCEDURE EXT.SP_INBOUND_POST_BCCARPU
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_cutoffday DECIMAL(38,10);  /* ORIGSQL: v_cutoffday NUMBER; */
    DECLARE v_oppr ROW LIKE inbound_cfg_BCC_Txn;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_BCC_Txn' not found (for %ROWTYPE declaration) */
    DECLARE v_maxseq DECIMAL(38,10);  /* ORIGSQL: v_maxseq number; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_POST_BCCARPU';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_POST_BCCARPU'; */
    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount integer; */

    DECLARE v_prmtr ROW LIKE inbound_cfg_parameter;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */

    /* ORIGSQL: dbms_output.put_line ('*****************'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('*****************');

    /* ORIGSQL: dbms_output.put_line ('Start Post BCC ARPU'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Start Post BCC ARPU');

    /* ORIGSQL: dbms_output.put_line ('*****************'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('*****************');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_prmtr
    FROM
        ext.inbound_cfg_parameter
    WHERE
        object_name = 'SP_INBOUND_TXN_MAP';

    /* ORIGSQL: dbms_output.put_line ('51 TV ARPU Start'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('51 TV ARPU Start');

    -- ODS -TV  File

    IF :v_prmtr.file_type LIKE 'BCC%edTV%' 
    THEN
        /* ORIGSQL: execute immediate 'Truncate table Inbound_temp_txn drop storage' ; */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_TEMP_TXN' not found */

        /* ORIGSQL: Truncate table Inbound_temp_txn drop storage ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE ext.inbound_temp_txn';

        /* ORIGSQL: insert into Inbound_temp_txn select * from Inbound_data_txn where filedate=v_prm(...) */
        INSERT INTO ext.inbound_temp_txn
            SELECT   /* ORIGSQL: select * from Inbound_data_txn where filedate=v_prmtr.file_date and filename=v_p(...) */
                *
            FROM
                ext.inbound_data_txn
            WHERE
                filedate = :v_prmtr.file_date
                AND filename = :v_prmtr.file_name
                AND recordstatus = 0;

        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

        /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'ODS -TV insert into Inbound_temp_txn :' || v_p(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'ODS -TV insert into  Inbound_temp_txn :'|| IFNULL(:v_prmtr.file_type,'') || '-FileName:'|| IFNULL(:v_prmtr.file_name,'') || '-Date:'|| IFNULL(:v_prmtr.file_date,''),1,255) 
            , 'ODS -TV insert into  Inbound_temp_txn Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'ODS -TV insert into  Inbound_temp_txn :' || v_prmtr.file_(...) */

        /* ORIGSQL: dbms_output.put_line ('51 TV Insert '||v_rowcount); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('51 TV Insert '||IFNULL(TO_VARCHAR(:v_rowcount),''));

        /* ORIGSQL: Commit; */
        COMMIT;

        -- Lookup For Pricelist in Classifiers
        /*
        update Inbound_temp_txn tgt
         set genericnumber4= (select case when nvl(genericattribute6,'0')='1' then nvl(genericnumber1,0) else  nvl(cost,0) end
              from stel_classifier@stelext where categorytreename='Singtel'
            and nvl(genericboolean3,0)=1
        and classfiername=tgt.productid)
           where
           filedate=v_prmtr.file_date
         and filename=v_prmtr.file_name
         and recordstatus=0;
        
        
                   v_rowcount := SQL%ROWCOUNT;
        
              SP_LOGGER (
                     SUBSTR (
                               v_proc_name
                            || 'Pricelist Update in Inbound_temp_txn :'
                            || v_prmtr.file_type
                            || '-FileName:'
                            || v_prmtr.file_name
                            || '-Date:'
                            || v_prmtr.file_date,
                            1,
                        255),
                     'Pricelist Update in Inbound_temp_txn Execution Completed',
                     v_rowcount,
                     NULL,
                 null);
        
           dbms_output.put_line ('52 TV merge'||v_rowcount);
           Commit;
        
        -- Update the ARPU Value
        
        update Inbound_temp_txn tgt
        set (genericnumber2,genericnumber3) =
         (select sum(nvl(genericnumber4,0)
                 * case when upper(genericattribute22 ) = 'C' then -1 else 1 end
             ) ARPU,
             sum(nvl(genericnumber4,0)
                 * case when upper(genericattribute22 ) = 'C' then -1 else 1 end) -
             sum(case when upper(genericattribute31)='P' then nvl(genericnumber4,0) else 0 end)  as Delta_ARPU -- new_ARPU minus Old_ARPU
             from Inbound_temp_txn
               where filedate=v_prmtr.file_date
             and filename=v_prmtr.file_name
             and recordstatus=0
         and billtocity =tgt.billtocity)
         where
         upper(genericattribute9) = 'M' -- main Line
         and filedate=v_prmtr.file_date
         and filename=v_prmtr.file_name
         and recordstatus=0;
        
                      v_rowcount := SQL%ROWCOUNT;
        
              SP_LOGGER (
                     SUBSTR (
                               v_proc_name
                            || 'ARPU Update in Inbound_temp_txn :'
                            || v_prmtr.file_type
                            || '-FileName:'
                            || v_prmtr.file_name
                            || '-Date:'
                            || v_prmtr.file_date,
                            1,
                        255),
                     'ARPU Update in Inbound_temp_txn Execution Completed',
                     v_rowcount,
                     NULL,
                 null);
        
           dbms_output.put_line ('53 TV Update 1 '||v_rowcount);
        Commit;
        
        
        --update the VAS lines with the individual List price
        update Inbound_temp_txn tgt
        set  genericnumber2=genericnumber4, genericnumber3=genericnumber4
         where
         upper(genericattribute9) = 'S'
         and filedate=v_prmtr.file_date
         and filename=v_prmtr.file_name
         and recordstatus=0;
        
                         v_rowcount := SQL%ROWCOUNT;
        
              SP_LOGGER (
                     SUBSTR (
                               v_proc_name
                            || 'VAS Lines Update in Inbound_temp_txn :'
                            || v_prmtr.file_type
                            || '-FileName:'
                            || v_prmtr.file_name
                            || '-Date:'
                            || v_prmtr.file_date,
                            1,
                        255),
                     'VAS Lines Update in Inbound_temp_txn Execution Completed',
                     v_rowcount,
                     NULL,
                 null);
        
        dbms_output.put_line ('54 TV Update 2 '||v_rowcount);
        commit;
        
        -- Update the all fields on main table
        
        merge into Inbound_data_txn tgt
        using Inbound_temp_txn src
        on (tgt.filedate=src.filedate and tgt.filename=src.filename
        and tgt.orderid=src.orderid and tgt.linenumber=src.linenumber and tgt.sublinenumber=src.sublinenumber)
        when matched then update set
        tgt.genericnumber2=src.genericnumber2,
        tgt.genericnumber3=src.genericnumber3,
        tgt.genericnumber4=src.genericnumber4;
        
        v_rowcount := SQL%ROWCOUNT;
        
              SP_LOGGER (
                     SUBSTR (
                               v_proc_name
                            || 'Update all fields in Inbound_data_txn :'
                            || v_prmtr.file_type
                            || '-FileName:'
                            || v_prmtr.file_name
                            || '-Date:'
                            || v_prmtr.file_date,
                            1,
                        255),
                     'Update all fields in Inbound_data_txn Execution Completed',
                     v_rowcount,
                     NULL,
                 null);
        
        dbms_output.put_line ('55 TV merge  '||v_rowcount);
        Commit;
        
        update Inbound_data_txn tgt
        set UNITTYPEFORGENERICNUMBER2 = case when GENERICNUMBER2 IS NOT NULL then 'SGD' else null end,
        UNITTYPEFORGENERICNUMBER3=case when GENERICNUMBER3 IS NOT NULL then 'SGD' else null end,
        UNITTYPEFORGENERICNUMBER4=case when GENERICNUMBER4 IS NOT NULL then 'SGD' else null end
         where filedate=v_prmtr.file_date
         and filename=v_prmtr.file_name
         and recordstatus=0;
        
        */   

        /* ORIGSQL: update Inbound_data_txn tgt SET GENERICNUMBER2=0, UNITTYPEFORGENERICNUMBER2 = 'S(...) */
        UPDATE ext.inbound_data_txn tgt
            SET
            /* ORIGSQL: GENERICNUMBER2 = */
            GENERICNUMBER2 = 0,
            /* ORIGSQL: UNITTYPEFORGENERICNUMBER2 = */
            UNITTYPEFORGENERICNUMBER2 = 'SGD',
            /* ORIGSQL: GENERICNUMBER3 = */
            GENERICNUMBER3 = 0,
            /* ORIGSQL: UNITTYPEFORGENERICNUMBER3 = */
            UNITTYPEFORGENERICNUMBER3 = 'SGD',
            /* ORIGSQL: GENERICNUMBER4 = */
            GENERICNUMBER4 = 0,
            /* ORIGSQL: UNITTYPEFORGENERICNUMBER4 = */
            UNITTYPEFORGENERICNUMBER4 = 'SGD' 
        WHERE
            filedate = :v_prmtr.file_date
            AND filename = :v_prmtr.file_name
            AND recordstatus = 0;

        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

        /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update UnitTypes for GN Inbound_data_txn :' ||(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update UnitTypes for GN Inbound_data_txn :'|| IFNULL(:v_prmtr.file_type,'') || '-FileName:'|| IFNULL(:v_prmtr.file_name,'') || '-Date:'|| IFNULL(:v_prmtr.file_date,''),1,255) 
            , 'Update UnitTypes for GN Inbound_data_txn Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update UnitTypes for GN Inbound_data_txn :' || v_prmtr.fi(...) */

        /* ORIGSQL: dbms_output.put_line ('56 TV last update  '||v_rowcount); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('56 TV last update  '||IFNULL(TO_VARCHAR(:v_rowcount),''));

        /* ORIGSQL: Commit; */
        COMMIT;

        /********************/
        /********Platform Migration************/
        /********************/

        /*
        
        5.    Identify Migration Orders:
        \x95    Txn type=\x94Change\x94  GA5
        \x95    Order type = \x93CH\x94 ga10
        \x95    Sub txn type= "Change main plan" ga 11
        \x95    Previous data indicator=\x94N\x94 or \x93P\x94 ga31
        Order line typ = \x93Main\x94 GA9
        
        6.    Filter for Orders with ARPU = 0 for the \x93N\x94 record
        7.    Filter for Orders where the ADSL FTTH No field starts with \x916\x92, for the \x93N\x94 record (bill to country field)
        8.    Filter these for Records where the \x93P\x94 record\x92s ADSL No. starts with \x911\x92
        9.    The remaining records are updated with GENERICBOOLEAN 3 = TRUE
        */ 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into inbound_data_txn tgt using (SELECT orderid, linenumber, sublinenumber(...) */
        MERGE INTO inbound_data_txn AS tgt
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_TXN' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select orderid, linenumber, sublinenumber, eventtypeid from inbound_data_txn t (...) */
                    orderid,
                    linenumber,
                    sublinenumber,
                    eventtypeid
                FROM
                    inbound_data_txn t
                WHERE
                    t.filedate = :v_prmtr.file_date
                    AND t.filename = :v_prmtr.file_name
                    AND t.recordstatus = 0
                    AND t.genericattribute9 = 'M'
                    AND t.genericattribute5 IN ('CH','Change')
                    AND UPPER(t.genericattribute11) = UPPER('Change Main Plan')
                    AND (
                        (t.genericattribute15 = 'A'
                        AND t.genericattribute31 = 'P')
                        OR (t.genericattribute15 = 'F'
                        AND t.genericattribute31 = 'N')
                    )
                GROUP BY
                    orderid, linenumber, sublinenumber, eventtypeid
                HAVING
                    COUNT(DISTINCT genericattribute31) > 1
            ) AS src
            ON (tgt.orderid = src.orderid
                AND tgt.linenumber = src.linenumber
                AND tgt.sublinenumber = src.sublinenumber
                AND tgt.eventtypeid = src.eventtypeid
            	AND tgt.filedate = :v_prmtr.file_date
                AND tgt.filename = :v_prmtr.file_name
                AND tgt.recordstatus = 0
                AND tgt.genericattribute9 = 'M'
                AND tgt.genericattribute5 IN ('CH','Change')
                AND tgt.genericattribute31 = 'N'
            )
        WHEN MATCHED THEN
            UPDATE SET tgt.genericboolean3 = 1
            /*WHERE
                tgt.filedate = v_prmtr.file_date
                AND tgt.filename = v_prmtr.file_name
                AND tgt.recordstatus = 0
                AND tgt.genericattribute9 = 'M'
                AND tgt.genericattribute5 IN ('CH','Change')
                AND tgt.genericattribute31 = 'N'*/
                ;

        /* ORIGSQL: commit; */
        COMMIT;

        /*
        update inbound_Data_Txn
        set recordstatus=-2
        where genericattribute31='P'
        and  filedate=v_prmtr.file_date
         and filename=v_prmtr.file_name
         and recordstatus=0;
        
        
        update inbound_Data_assignment
        set recordstatus=-2
        where (orderid, linenumber,sublinenumber) in
        (Select orderid, linenumber,sublinenumber from inbound_Data_Txn where
            genericattribute31='P'
            and  filedate=v_prmtr.file_date
             and filename=v_prmtr.file_name
           )
         and filedate=v_prmtr.file_date
         and filename=v_prmtr.file_name
         and recordstatus=0;
        */
    END IF;

    /* ORIGSQL: commit; */
    COMMIT;
END