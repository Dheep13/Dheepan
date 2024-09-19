CREATE PROCEDURE EXT.SP_INBOUND_POST_EPC
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_POST_EPC';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_POST_EPC'; */

    DECLARE v_param ROW LIKE INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_param
    FROM
        EXT.INBOUND_CFG_PARAMETER;  

    /* ORIGSQL: update inbound_Data_clpr t SET recordstatus=-9 where t.filename = v_param.file_n(...) */
    UPDATE ext.inbound_Data_clpr t
        SET
        /* ORIGSQL: recordstatus = */
        recordstatus = -9
    WHERE
        t.filename = :v_param.file_name
        AND t.filedate = :v_param.file_date
        AND t.recordstatus = 0;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_CLPR' not found */

    /* ORIGSQL: insert into inbound_Data_clpr SELECT FILEDATE, FILENAME, '0' RECORDSTATUS, DOWNL(...) */
    INSERT INTO ext.inbound_Data_clpr
        SELECT   /* ORIGSQL: SELECT FILEDATE, FILENAME, '0' RECORDSTATUS, DOWNLOADED, b.classfiername ||' - '(...) */
            FILEDATE,
            FILENAME,
            '0' AS RECORDSTATUS,
            DOWNLOADED,
            IFNULL(b.classfiername,'') ||' - ' || IFNULL(TO_VARCHAR(PRODUCTID),''),/* --note */  EFFECTIVESTARTDATE,
            EFFECTIVEENDDATE,
            PRODUCTID AS NAME,/* --note */  PRICE,
            UNITTYPEFORPRICE,
            COST,
            UNITTYPEFORCOST,
            b.CATEGORYTREENAME,
            b.CATEGORYNAME,
            CASE
                WHEN classfiername = 'DS - Internal'
                THEN 'ConSales_Internal'
                WHEN classfiername = 'DH - External'
                THEN 'DigitalHome_External'
                WHEN classfiername = 'SER'
                THEN 'ConSales_External'
                WHEN classfiername = 'TEPL'
                THEN 'ConSales_External'
                WHEN classfiername = 'Digital Sales - Telesales'
                THEN 'ConSales_Internal'
                WHEN classfiername = 'Singtel Shop'
                THEN 'ConSales_Internal'
                WHEN classfiername = 'DS - External'
                THEN 'ConSales_External'
                WHEN classfiername = 'DH - Internal'
                THEN 'DigitalHome_Internal'
                WHEN classfiername = 'RCM'
                THEN 'ConSales_Internal'
                WHEN classfiername = 'CCO - Solution Plus'
                THEN 'ConOpsSPlus'
                ELSE 'ConSales_External'
            END
            AS BUSINESSUNITNAME,
            /* -- BUSINESSUNITNAME, */
            name AS DESCRIPTION,/* --note */  GENERICATTRIBUTE1,
            GENERICATTRIBUTE2,
            GENERICATTRIBUTE3,
            GENERICATTRIBUTE4,
            GENERICATTRIBUTE5,
            GENERICATTRIBUTE6,
            GENERICATTRIBUTE7,
            GENERICATTRIBUTE8,
            GENERICATTRIBUTE9,
            GENERICATTRIBUTE10,
            GENERICATTRIBUTE11,
            GENERICATTRIBUTE12,
            GENERICATTRIBUTE13,
            GENERICATTRIBUTE14,
            GENERICATTRIBUTE15,
            GENERICATTRIBUTE16,
            GENERICNUMBER1,
            UNITTYPEFORGENERICNUMBER1,
            GENERICNUMBER2,
            UNITTYPEFORGENERICNUMBER2,
            GENERICNUMBER3,
            UNITTYPEFORGENERICNUMBER3,
            GENERICNUMBER4,
            UNITTYPEFORGENERICNUMBER4,
            GENERICNUMBER5,
            UNITTYPEFORGENERICNUMBER5,
            GENERICNUMBER6,
            UNITTYPEFORGENERICNUMBER6,
            GENERICDATE1,
            GENERICDATE2,
            GENERICDATE3,
            GENERICDATE4,
            GENERICDATE5,
            GENERICDATE6,
            GENERICBOOLEAN1,
            GENERICBOOLEAN2,
            GENERICBOOLEAN3,
            GENERICBOOLEAN4,
            GENERICBOOLEAN5,
            GENERICBOOLEAN6
        FROM
            ext.inbound_Data_clpr t
        CROSS JOIN
            (
                SELECT   /* ORIGSQL: (Select distinct classfiername, genericattribute3 categorytreename, CASE WHEN ge(...) */
                    DISTINCT
                    classfiername,
                    genericattribute3 AS categorytreename,
                    CASE
                        WHEN genericattribute3 LIKE '%Internal%'
                        THEN 'PRODUCTS-INT'
                        ELSE 'PRODUCTS'
                    END
                    AS categoryname
                FROM
                    ext.stel_Classifier
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.stel_Classifier@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.stel_Classifier_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                WHERE
                    categorytreename = 'Channel List'
                    AND categoryname = 'Channel List'
                    AND CURRENT_TIMESTAMP BETWEEN effectivestartdate AND Add_Days(effectiveenddate,-1)
                     /* ORIGSQL: sysdate */
            ) AS b
        WHERE
            t.filename = :v_param.file_name
            AND t.filedate = :v_param.file_date
            AND t.recordstatus = -9;

    /* Arjun 20190523 removing this. added ct into classifier.ga3 for channel list
        insert into inbound_Data_clpr
        SELECT FILEDATE,
      FILENAME,
      '0' RECORDSTATUS,
      DOWNLOADED,
      b.classfiername ||' - ' || PRODUCTID, --note
      EFFECTIVESTARTDATE,
      EFFECTIVEENDDATE,
      PRODUCTID as NAME, --note
      PRICE,
      UNITTYPEFORPRICE,
      COST,
      UNITTYPEFORCOST,
      'Singtel-Internal-Products' CATEGORYTREENAME,
      'PRODUCTS-INT' CATEGORYNAME,
      BUSINESSUNITNAME,
      name as DESCRIPTION, --note
      GENERICATTRIBUTE1,
      GENERICATTRIBUTE2,
      GENERICATTRIBUTE3,
      GENERICATTRIBUTE4,
      GENERICATTRIBUTE5,
      GENERICATTRIBUTE6,
      GENERICATTRIBUTE7,
      GENERICATTRIBUTE8,
      GENERICATTRIBUTE9,
      GENERICATTRIBUTE10,
      GENERICATTRIBUTE11,
      GENERICATTRIBUTE12,
      GENERICATTRIBUTE13,
      GENERICATTRIBUTE14,
      GENERICATTRIBUTE15,
      GENERICATTRIBUTE16,
      GENERICNUMBER1,
      UNITTYPEFORGENERICNUMBER1,
      GENERICNUMBER2,
      UNITTYPEFORGENERICNUMBER2,
      GENERICNUMBER3,
      UNITTYPEFORGENERICNUMBER3,
      GENERICNUMBER4,
      UNITTYPEFORGENERICNUMBER4,
      GENERICNUMBER5,
      UNITTYPEFORGENERICNUMBER5,
      GENERICNUMBER6,
      UNITTYPEFORGENERICNUMBER6,
      GENERICDATE1,
      GENERICDATE2,
      GENERICDATE3,
      GENERICDATE4,
      GENERICDATE5,
      GENERICDATE6,
      GENERICBOOLEAN1,
      GENERICBOOLEAN2,
      GENERICBOOLEAN3,
      GENERICBOOLEAN4,
      GENERICBOOLEAN5,
      GENERICBOOLEAN6
    
        from inbound_Data_clpr t
        cross join (Select distinct classfiername, genericattribute3, case when genericattribute3 like '%Internal%' then 'PRODUCTS-INT' else 'PRODUCTS' end as genericattribute4
                  from stel_Classifier@stelext where categorytreename='Channel List' and categoryname='Channel List'
         and sysdate between effectivestartdate and effectiveenddate-1
              ) b
    
        where t.filename = v_param.file_name and t.filedate=v_param.file_date and t.recordstatus=-9;
    */
    -- we still want these loaded in- as product master data   
    /* ORIGSQL: update inbound_Data_clpr t SET recordstatus=0 where t.filename = v_param.file_na(...) */
    UPDATE ext.inbound_Data_clpr t
        SET
        /* ORIGSQL: recordstatus = */
        recordstatus = 0
    WHERE
        t.filename = :v_param.file_name
        AND t.filedate = :v_param.file_date
        AND t.recordstatus = -9;

    -- delete data that hasn;t changed compared to what's in TC   

    /* ORIGSQL: update inbound_Data_clpr t SET recordstatus=-2 where t.filename = v_param.file_n(...) */
    UPDATE ext.inbound_Data_clpr t
        SET
        /* ORIGSQL: recordstatus = */
        recordstatus = -2
    FROM
        ext.inbound_Data_clpr t
    WHERE
        t.filename = :v_param.file_name
        AND t.filedate = :v_param.file_date
        AND t.recordstatus = 0
        AND (productid, name, cost, genericattribute1
            , genericattribute10, genericattribute11, genericattribute13
        , genericattribute14, genericattribute15) IN

        (
            SELECT   /* ORIGSQL: (Select classifierid, classfiername, cost, genericattribute1, genericattribute10(...) */
                classifierid,
                classfiername,
                cost,
                genericattribute1,
                genericattribute10 AS FVSvc,
                genericattribute11,
                genericattribute13,
                genericattribute14,
                genericattribute15
            FROM
                ext.stel_Classifier
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.stel_Classifier@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.stel_Classifier_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                categorytreename = t.categorytreename/*'Singtel'*/
                AND Categoryname = t.categoryname/*'PRODUCTS'*/
                AND CURRENT_TIMESTAMP BETWEEN effectivestartdate AND effectiveenddate  /* ORIGSQL: sysdate */
        );

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update RecordStatus=-2 in inbound_Data_clpr :'(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update RecordStatus=-2 in inbound_Data_clpr :'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Update RecordStatus=-2 in inbound_Data_clpr Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update RecordStatus=-2 in inbound_Data_clpr :' || v_param(...) */

    -- for data that has changed, update the effectivedate
    --merge the data with what was uploaded by the user 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_Data_clpr tgt using (SELECT * FROM DBMTK_USER_NAME.stel_Class(...) */
    MERGE INTO inbound_Data_clpr AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (Select * from stel_Classifier@stelext where ((categorytreename='Singtel' and Ca(...) */
                *
            FROM
                ext.stel_Classifier
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.stel_Classifier@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.stel_Classifier_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                ((categorytreename = 'Singtel'
                    AND Categoryname = 'PRODUCTS')
                    OR (categorytreename = 'Singtel-Internal-Products'
                    AND Categoryname = 'PRODUCTS-INT')
                    OR (categorytreename = 'StockCode'
                    AND categoryname = 'PRODUCTS') --[Arun 28th Mar 2019 - Adding Stock codes to the list as well - As user don't want the configured MSF to be gone after every reload.
                )
                AND CURRENT_TIMESTAMP BETWEEN effectivestartdate AND effectiveenddate  /* ORIGSQL: sysdate */
        ) AS src
        ON (tgt.productid = src.classifierid
            --arjun 20190522 adding the below join due to Mobile VAS being set up without category
            AND tgt.categorytreename = src.categorytreename
            AND tgt.categoryname = src.categoryname
            AND tgt.filename = :v_param.file_name
            AND tgt.filedate = :v_param.file_date
            AND tgt.recordstatus = 0
        )
    WHEN MATCHED THEN
        UPDATE
            SET
            tgt.price = src.price, tgt.cost = src.cost
            , tgt.genericattribute1 = src.genericattribute1
            , tgt.genericattribute2 = src.genericattribute2
            , tgt.genericattribute3 = src.genericattribute3
            , tgt.genericattribute4 = src.genericattribute4
            , tgt.genericattribute5 = src.genericattribute5
            , tgt.genericattribute6 = src.genericattribute6
            , tgt.genericattribute7 = src.genericattribute7
            , tgt.genericattribute8 = src.genericattribute8
            , tgt.genericattribute9 = src.genericattribute9
            , tgt.genericattribute12 = src.genericattribute12
            , tgt.genericattribute15 = src.genericattribute15
            , tgt.genericattribute16 = src.genericattribute16
            , tgt.genericnumber1 = src.genericnumber1
            , tgt.genericnumber2 = src.genericnumber2
            , tgt.genericnumber3 = src.genericnumber3
            , tgt.genericnumber4 = src.genericnumber4
            , tgt.genericnumber5 = src.genericnumber5
            , tgt.genericnumber6 = src.genericnumber6
            , tgt.unittypeforgenericnumber1 =
            CASE
                WHEN src.genericnumber1 IS NULL
                THEN NULL
                ELSE 'SGD'
            END
            , tgt.unittypeforgenericnumber2 =
            CASE
                WHEN src.genericnumber2 IS NULL
                THEN NULL
                ELSE 'SGD'
            END
            , tgt.unittypeforgenericnumber3 =
            CASE
                WHEN src.genericnumber3 IS NULL
                THEN NULL
                ELSE 'SGD'
            END
            , tgt.unittypeforgenericnumber4 =
            CASE
                WHEN src.genericnumber4 IS NULL
                THEN NULL
                ELSE 'SGD'
            END
            , tgt.unittypeforgenericnumber5 =
            CASE
                WHEN src.genericnumber5 IS NULL
                THEN NULL
                ELSE 'SGD'
            END
            , tgt.unittypeforgenericnumber6 =
            CASE
                WHEN src.genericnumber6 IS NULL
                THEN NULL
                ELSE 'SGD'
            END

            , tgt.genericdate1 = src.genericdate1
            , tgt.genericdate2 = src.genericdate2
            , tgt.genericdate3 = src.genericdate3
            , tgt.genericdate4 = src.genericdate4
            , tgt.genericdate5 = src.genericdate5
            , tgt.genericdate6 = src.genericdate6
            , tgt.genericboolean1 = src.genericboolean1
            , tgt.genericboolean2 = src.genericboolean2
            , tgt.genericboolean3 = src.genericboolean3
            , tgt.genericboolean4 = src.genericboolean4
            , tgt.genericboolean5 = src.genericboolean5
            , tgt.genericboolean6 = src.genericboolean6
        --WHERE
            --tgt.filename = v_param.file_name
            --AND tgt.filedate = v_param.file_date
            --AND tgt.recordstatus = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update Users data ininbound_Data_clpr:' || v_p(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update Users data ininbound_Data_clpr:'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Update Users data ininbound_Data_clpr Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update Users data ininbound_Data_clpr:' || v_param.file_t(...) */

    /* ORIGSQL: update inbound_Data_clpr tgt SET unittypeforprice= CASE WHEN price IS NOT NULL T(...) */
    UPDATE ext.inbound_Data_clpr tgt
        SET
        /* ORIGSQL: unittypeforprice = */
        unittypeforprice =
        CASE 
            WHEN price IS NOT NULL
            THEN 'SGD'
            ELSE NULL
        END,
        /* ORIGSQL: unittypeforcost = */
        unittypeforcost =
        CASE 
            WHEN cost IS NOT NULL
            THEN 'SGD'
            ELSE NULL
        END,
        /* ORIGSQL: cost = */
        cost =
        CASE 
            WHEN unittypeforcost = 'SGD'
            THEN IFNULL(cost,0)  /* ORIGSQL: nvl(cost,0) */
            ELSE NULL
        END
    WHERE
        tgt.filename = :v_param.file_name
        AND tgt.filedate = :v_param.file_date
        AND tgt.recordstatus = 0;

    /* ORIGSQL: commit; */
    COMMIT;
END