
    update inbound_Data_clpr AS tgt
    set tgt.price = src.price, tgt.cost = src.cost
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
    from 
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
        join inbound_Data_clpr AS tgt
        ON (tgt.productid = src.classifierid
            --arjun 20190522 adding the below join due to Mobile VAS being set up without category
            AND tgt.categorytreename = src.categorytreename
            AND tgt.categoryname = src.categoryname
            AND tgt.filename = :v_param.file_name
            AND tgt.filedate = :v_param.file_date
            AND tgt.recordstatus = 0
        );


