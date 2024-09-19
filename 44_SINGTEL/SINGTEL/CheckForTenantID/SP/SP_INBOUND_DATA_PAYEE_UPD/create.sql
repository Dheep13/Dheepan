CREATE PROCEDURE EXT.SP_INBOUND_DATA_PAYEE_UPD
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_defaultStartdate TIMESTAMP = to_date('01-Apr-2015', 'dd-mon-yyyy');  /* ORIGSQL: v_defaultStartdate DATE := TO_DATE('01-Apr-2015', 'dd-mon-yyyy') ; */

    DECLARE v_defaultHRCDate TIMESTAMP = to_date('02-Feb-2020', 'dd-mon-yyyy');  /* ORIGSQL: v_defaultHRCDate DATE := TO_DATE('02-Feb-2020', 'dd-mon-yyyy') ; */

    -- HRC sets the last modified in PROD to this date for all records, so basically we're only concerned with records updated after this date
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_DATA_PAYEE_UPD';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_DATA_PAYEE_UPD'; */
    --DECLARE v_parameter Inbound_cfg_Parameter%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.Inbound_cfg_Parameter' not found (for %ROWTYPE declaration) */
   DECLARE v_parameter ROW LIKE Inbound_cfg_Parameter;
    
    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount integer; */

    DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
        /* ORIGSQL: WHEN NO_DATA_FOUND THEN */
        BEGIN
            /* ORIGSQL: NULL; */
            DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */

            /* ORIGSQL: COMMIT; */
            COMMIT;

            --END sp_inbound_data_payee_upd;
        END;

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

        SELECT
            DISTINCT
            *
        INTO
            v_parameter
        FROM
            Inbound_cfg_Parameter;

        IF 1 = 1
        THEN
            --added by kyap, set the BU based on cs_title   
            /* ORIGSQL: update inbound_data_ogpo ogpo SET ogpo.businessunitname = (SELECT distinct bu.na(...) */
            UPDATE inbound_data_ogpo ogpo
                SET
                /* ORIGSQL: ogpo.businessunitname = */
                businessunitname = (
                    SELECT   /* ORIGSQL: (select distinct bu.name from cs_title@stelext t, cs_ruleelementowner@stelext r,(...) */
                        DISTINCT
                        bu.name
                    FROM
                        cs_title t,
                        cs_ruleelementowner r,
                        cs_businessunit bu
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_title@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_title_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_ruleelementowner@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_ruleelementowner_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_businessunit@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_businessunit_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        t.ruleelementownerseq = r.ruleelementownerseq
                        AND r.businessunitmap = bu.mask
                        AND t.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND t.effectiveenddate > CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND r.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND r.effectiveenddate > CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND t.name = ogpo.titlename
                )
            WHERE
                ogpo.filename = :v_parameter.file_name
                AND ogpo.filedate = :v_parameter.file_Date
                AND ogpo.recordstatus = '0';

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '0. inbound_data_ogpo businessunit update :' ||(...) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '0. inbound_data_ogpo businessunit update  :'|| IFNULL(:v_parameter.file_Date,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_date,''),1,255) 
                , '0. inbound_data_ogpo businessunit update Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '0. inbound_data_ogpo businessunit update  :' || v_paramet(...) */

            /* ORIGSQL: commit; */
            COMMIT;

            --Added by gopi for Telesales hire date   
            /* ORIGSQL: update inbound_data_ogpt a SET a.HIREDATE =TO_DATE('01-July-2015', 'dd-mon-yyyy'(...) */
            UPDATE inbound_data_ogpt a
                SET
                /* ORIGSQL: a.HIREDATE = */
                HIREDATE = to_date('01-July-2015', 'dd-mon-yyyy')  /* ORIGSQL: TO_DATE('01-July-2015', 'dd-mon-yyyy') */
            FROM
                inbound_data_ogpt a
            WHERE
                a.HIREDATE <= to_date('01-APR-2015', 'dd-mon-yyyy')  /* ORIGSQL: TO_DATE('01-APR-2015', 'dd-mon-yyyy') */
                AND BUSINESSUNITNAME = 'DigitalHome_Internal'
                AND TERMINATIONDATE IS NULL;

            /* ORIGSQL: commit; */
            COMMIT;  

            /* ORIGSQL: update inbound_data_ogpt ogpt SET ogpt.businessunitname = (SELECT distinct ogpo.(...) */
            UPDATE inbound_data_ogpt ogpt
                /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_OGPO' not found */
                SET
                /* ORIGSQL: ogpt.businessunitname = */
                businessunitname = (
                    SELECT   /* ORIGSQL: (select distinct ogpo.businessunitname from inbound_data_ogpo ogpo where ogpt.pa(...) */
                        DISTINCT
                        ogpo.businessunitname
                    FROM
                        inbound_data_ogpo ogpo
                    WHERE
                        ogpt.payeeid = ogpo.positionname
                        AND ogpt.filename = ogpo.filename
                        AND ogpt.filedate = ogpo.filedate
                        AND ogpo.recordstatus = 0
                )
            WHERE
                ogpt.filename = :v_parameter.file_name
                AND ogpt.filedate = :v_parameter.file_Date
                AND ogpt.recordstatus = '0';

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '0. inbound_data_ogpt businessunit update :' ||(...) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '0. inbound_data_ogpt businessunit update  :'|| IFNULL(:v_parameter.file_Date,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_Date,''),1,255) 
                , '0. inbound_data_ogpt businessunit update Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '0. inbound_data_ogpt businessunit update  :' || v_paramet(...) */

            /* ORIGSQL: commit; */
            COMMIT;

            --end of added by kyap, set the BU based on cs_title

            --added by kyap 20190223, use temp table to improve performance

            /* ORIGSQL: EXECUTE IMMEDIATE 'TRUNCATE TABLE inbound_data_payee_upd_temp'; */
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_PAYEE_UPD_TEMP' not found */

            /* ORIGSQL: TRUNCATE TABLE inbound_data_payee_upd_temp ; */
            EXECUTE IMMEDIATE 'TRUNCATE TABLE inbound_data_payee_upd_temp';

            /* ORIGSQL: insert into inbound_data_payee_upd_temp (SELECT v_parameter.file_name, v_paramet(...) */
            INSERT INTO inbound_data_payee_upd_temp
                SELECT   /* ORIGSQL: (SELECT v_parameter.file_name, v_parameter.file_date, ogpo.effectivestartdate, o(...) */
                    :v_parameter.file_name,
                    :v_parameter.file_Date,
                    ogpo.effectivestartdate,
                    ogpo.effectiveenddate,
                    cspy.payeeid,
                    NULL AS payeetype,
                    NULL AS planname,
                    ogpo.name AS positionname,
                    csti.name AS titlename,
                    ogpg.name AS positiongroupname,
                    ogpo.targetcompensation,
                    ogpo.unittypefortargetcompensation,
                    /* --csbu.name */
                    'null' AS businessunitname,
                    NULL AS description,
                    ogpo.genericattribute1,
                    ogpo.genericattribute2,
                    ogpo.genericattribute3,
                    ogpo.genericattribute4,
                    ogpo.genericattribute5,
                    ogpo.genericattribute6,
                    ogpo.genericattribute7,
                    ogpo.genericattribute8,
                    ogpo.genericattribute9,
                    ogpo.genericattribute10,
                    ogpo.genericattribute11,
                    ogpo.genericattribute12,
                    ogpo.genericattribute13,
                    ogpo.genericattribute14,
                    ogpo.genericattribute15,
                    ogpo.genericattribute16,
                    ogpo.genericnumber1,
                    ogpo.unittypeforgenericnumber1,
                    ogpo.genericnumber2,
                    ogpo.unittypeforgenericnumber2,
                    ogpo.genericnumber3,
                    ogpo.unittypeforgenericnumber3,
                    ogpo.genericnumber4,
                    ogpo.unittypeforgenericnumber4,
                    ogpo.genericnumber5,
                    ogpo.unittypeforgenericnumber5,
                    ogpo.genericnumber6,
                    ogpo.unittypeforgenericnumber6,
                    ogpo.genericdate1,
                    ogpo.genericdate2,
                    ogpo.genericdate3,
                    ogpo.genericdate4,
                    ogpo.genericdate5,
                    ogpo.genericdate6,
                    ogpo.genericboolean1,
                    ogpo.genericboolean2,
                    ogpo.genericboolean3,
                    ogpo.genericboolean4,
                    ogpo.genericboolean5,
                    ogpo.genericboolean6,
                    ogpo.creditstartdate,
                    ogpo.creditenddate,
                    ogpo.processingstartdate,
                    ogpo.processingenddate,
                    ogpo_mgr.name AS managername
                FROM
                    --inbound_data_ogpo stgpo,
                    --inbound_cfg_parameter p,
                    cs_position ogpo
                LEFT OUTER JOIN
                    cs_position ogpo_mgr
                    ON ogpo.managerseq = ogpo_mgr.ruleelementownerseq
                    AND ogpo_mgr.removedate = '1-Jan-2200'
                    AND ogpo_mgr.effectiveenddate = '1-Jan-2200'
                    --on ogpo.managerseq=ogpo_mgr.ruleelementownerseq and ogpo_mgr.removedate=to_Date('2200101','YYYYMMDD') and ogpo_mgr.effectiveenddate=to_Date('2200101','YYYYMMDD') --[Arun 28/6/2019 - Commented as manager value is not returned and changed the date format
                INNER JOIN
                    cs_payee cspy
                    ON ogpo.payeeseq = cspy.payeeseq
                    AND cspy.removedate = TO_DATE('22000101','YYYYMMDD')
                    AND cspy.effectiveenddate = TO_DATE('22000101','YYYYMMDD')
                INNER JOIN
                    cs_title csti
                    ON ogpo.titleseq = csti.ruleelementownerseq
                    AND csti.removedate = TO_DATE('22000101','YYYYMMDD')
                    AND csti.effectiveenddate = TO_DATE('22000101','YYYYMMDD')
                LEFT OUTER JOIN
                    cs_positiongroup ogpg
                    ON ogpo.positiongroupseq = ogpg.positiongroupseq
                    AND ogpg.removedate = TO_DATE('22000101','YYYYMMDD')

                    --  cs_businessunit@stelext csbu,
                INNER JOIN
                    cs_ruleelementowner r
                    --added by kyap 20181015
                    ON ogpo.ruleelementownerseq = r.ruleelementownerseq
                    AND r.removedate = TO_DATE('22000101','YYYYMMDD')
                    AND r.effectiveenddate = TO_DATE('22000101','YYYYMMDD')

                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_title@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_title_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_ruleelementowner@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_ruleelementowner_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_positiongroup@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_positiongroup_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_payee@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_payee_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                WHERE
                    1 = 1-- stgpo.positionname = ogpo.name
                    AND ogpo.removedate = TO_DATE('22000101','YYYYMMDD')
                    AND ogpo.effectiveenddate = TO_DATE('22000101','YYYYMMDD')
                    -- AND stgpo.filename = p.file_name
                    -- AND stgpo.filedate = p.file_date
                    --AND stgpo.recordstatus='0'
                    --AND p.object_name = 'SP_INBOUND_TXN_MAP'
                    AND ogpo.tenantid = 'STEL'

                    AND cspy.tenantid = 'STEL'
                    AND r.tenantid = 'STEL'
            ;

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '1. inbound_data_ogpo Attribute insert 1 :' || (...) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '1. inbound_data_ogpo Attribute insert 1  :'|| IFNULL(:v_parameter.file_Date,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_Date,''),1,255) 
                , '1. inbound_data_ogpo insert  Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '1. inbound_data_ogpo Attribute insert 1  :' || v_paramete(...) */

            /* ORIGSQL: commit; */
            COMMIT;

            /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
            /* ORIGSQL: MERGE INTO inbound_data_ogpo a USING (SELECT * FROM inbound_data_payee_upd_temp)(...) */
            MERGE INTO inbound_data_ogpo AS a 
                USING
                (
                    SELECT   /* ORIGSQL: (select * from inbound_data_payee_upd_temp) */
                        *
                    FROM
                        inbound_data_payee_upd_temp
                        --added by kyap 20190223, remove and insert into temp table first
                        /*SELECT p.file_name,
                        p.file_date,
                           ogpo.effectivestartdate,
                              ogpo.effectiveenddate,
                              cspy.payeeid,
                              NULL AS payeetype,
                              NULL planname,
                              ogpo.name positionname,
                              csti.name titlename,
                              ogpg.name positiongroupname,
                              ogpo.targetcompensation,
                              ogpo.unittypefortargetcompensation,
                              csbu.name businessunitname,
                              NULL description,
                              ogpo.genericattribute1,
                              ogpo.genericattribute2,
                              ogpo.genericattribute3,
                              ogpo.genericattribute4,
                              ogpo.genericattribute5,
                              ogpo.genericattribute6,
                              ogpo.genericattribute7,
                              ogpo.genericattribute8,
                              ogpo.genericattribute9,
                              ogpo.genericattribute10,
                              ogpo.genericattribute11,
                              ogpo.genericattribute12,
                              ogpo.genericattribute13,
                              ogpo.genericattribute14,
                              ogpo.genericattribute15,
                              ogpo.genericattribute16,
                              ogpo.genericnumber1,
                              ogpo.unittypeforgenericnumber1,
                              ogpo.genericnumber2,
                              ogpo.unittypeforgenericnumber2,
                              ogpo.genericnumber3,
                              ogpo.unittypeforgenericnumber3,
                              ogpo.genericnumber4,
                              ogpo.unittypeforgenericnumber4,
                              ogpo.genericnumber5,
                              ogpo.unittypeforgenericnumber5,
                              ogpo.genericnumber6,
                              ogpo.unittypeforgenericnumber6,
                              ogpo.genericdate1,
                              ogpo.genericdate2,
                              ogpo.genericdate3,
                              ogpo.genericdate4,
                              ogpo.genericdate5,
                              ogpo.genericdate6,
                              ogpo.genericboolean1,
                              ogpo.genericboolean2,
                              ogpo.genericboolean3,
                              ogpo.genericboolean4,
                              ogpo.genericboolean5,
                              ogpo.genericboolean6,
                              ogpo.creditstartdate,
                              ogpo.creditenddate,
                              ogpo.processingstartdate,
                              ogpo.processingenddate, ogpo_mgr.name managername
                         FROM inbound_data_ogpo stgpo,
                              inbound_cfg_parameter p,
                              cs_position@stelext ogpo,
                              cs_position@stelext ogpo_mgr,
                              cs_payee@stelext cspy,
                              cs_title@stelext csti,
                              cs_positiongroup@stelext ogpg,
                              cs_businessunit@stelext csbu,
                              cs_ruleelementowner@stelext r --added by kyap 20181015
                        
                        
                        WHERE     stgpo.positionname = ogpo.name
                         AND ogpo.removedate > SYSDATE
                         AND ogpo.effectiveenddate > SYSDATE
                         AND stgpo.filename = p.file_name
                         AND stgpo.filedate = p.file_date
                         AND stgpo.recordstatus='0'
                         AND p.object_name = 'SP_INBOUND_TXN_MAP'
                         AND ogpo.islast = 1
                         AND ogpo.managerseq = ogpo_mgr.ruleelementownerseq(+)
                         AND ogpo_mgr.removedate(+) > SYSDATE
                         AND ogpo_mgr.effectiveenddate(+) > SYSDATE
                         AND ogpo_mgr.islast(+) = 1
                         AND ogpo.payeeseq = cspy.payeeseq
                         AND cspy.removedate > SYSDATE
                         AND cspy.effectiveenddate > SYSDATE
                         AND cspy.islast = 1
                         AND ogpo.titleseq = csti.ruleelementownerseq(+)
                         AND csti.removedate(+) > SYSDATE
                         AND csti.effectiveenddate(+) > SYSDATE
                         AND csti.islast(+) = 1
                         AND ogpo.positiongroupseq = ogpg.positiongroupseq(+)
                         AND ogpg.removedate(+) > SYSDATE
                              --AND cspy.businessunitmap = csbu.mask --comment out by kyap 20181015
                              --added by kyap 20181015, bugfix for BU map as cs_participant and cs_position BU can be different
                         and ogpo.ruleelementownerseq = r.ruleelementownerseq
                         and r.removedate > sysdate
                         and r.effectiveenddate > sysdate
                         and r.businessunitmap = csbu.mask
                         and r.islast=1*/
                ) AS b
                ON (a.positionname = b.positionname
                    AND a.filename = b.file_name
                    AND a.filedate = b.file_date
                )
            WHEN MATCHED THEN
                UPDATE SET
                    a.effectivestartdate =
                    CASE
                        WHEN a.effectivestartdate <= :v_defaultHRCDate
                        THEN IFNULL(b.effectivestartdate, a.effectivestartdate)  /* ORIGSQL: NVL(b.effectivestartdate, a.effectivestartdate) */
                        ELSE IFNULL(a.effectivestartdate, b.effectivestartdate)  /* ORIGSQL: NVL(a.effectivestartdate, b.effectivestartdate) */
                    END,
                    a.effectiveenddate = IFNULL(a.effectiveenddate, b.effectiveenddate),  /* ORIGSQL: NVL(a.effectiveenddate, b.effectiveenddate) */
                    a.payeeid = IFNULL(a.payeeid, b.payeeid),  /* ORIGSQL: NVL(a.payeeid, b.payeeid) */
                    a.managername = IFNULL(b.managername, a.managername),  /* ORIGSQL: NVL(b.managername, a.managername) */
                    --a.titlename = case when b.titlename like '%BSC%' then b.titlename else NVL (a.titlename, b.titlename) end, --[Arun-Doesn't meet Siti's req - To retain Removed Title/Being replace with OGPO because of the filter at the end of the proc - so changed to below]
                    a.titlename =
                    CASE
                        WHEN (b.titlename LIKE '%BSC%'
                        OR b.titlename LIKE '%Removed%')
                        THEN b.titlename
                        ELSE IFNULL(a.titlename, b.titlename)  /* ORIGSQL: NVL(a.titlename, b.titlename) */
                    END,
                    a.positiongroupname = IFNULL(a.positiongroupname, b.positiongroupname),  /* ORIGSQL: NVL(a.positiongroupname, b.positiongroupname) */
                    a.targetcompensation =
                    IFNULL(a.targetcompensation, b.targetcompensation),  /* ORIGSQL: NVL(a.targetcompensation, b.targetcompensation) */
                    a.unittypefortargetcompensation =
                    IFNULL(a.unittypefortargetcompensation,  /* ORIGSQL: NVL(a.unittypefortargetcompensation, b.unittypefortargetcompensation) */
                    b.unittypefortargetcompensation),
                    a.businessunitname = IFNULL(a.businessunitname, b.businessunitname),  /* ORIGSQL: NVL(a.businessunitname, b.businessunitname) */
                    a.genericattribute1 = IFNULL(b.genericattribute1, a.genericattribute1), --[arun commented on 9th June to avoid updating GA1 during file load
                    /* ORIGSQL: NVL(b.genericattribute1, a.genericattribute1) */
                    a.genericattribute2 = IFNULL(b.genericattribute2, a.genericattribute2),  /* ORIGSQL: NVL(b.genericattribute2, a.genericattribute2) */
                    a.genericattribute3 = IFNULL(a.genericattribute3, b.genericattribute3),  /* ORIGSQL: NVL(a.genericattribute3, b.genericattribute3) */
                    a.genericattribute4 = IFNULL(a.genericattribute4, b.genericattribute4),  /* ORIGSQL: NVL(a.genericattribute4, b.genericattribute4) */
                    a.genericattribute5 = IFNULL(a.genericattribute5, b.genericattribute5),  /* ORIGSQL: NVL(a.genericattribute5, b.genericattribute5) */
                    a.genericattribute6 = IFNULL(a.genericattribute6, b.genericattribute6),  /* ORIGSQL: NVL(a.genericattribute6, b.genericattribute6) */
                    a.genericattribute7 = IFNULL(a.genericattribute7, b.genericattribute7),  /* ORIGSQL: NVL(a.genericattribute7, b.genericattribute7) */
                    a.genericattribute8 = IFNULL(a.genericattribute8, b.genericattribute8),  /* ORIGSQL: NVL(a.genericattribute8, b.genericattribute8) */
                    a.genericattribute9 = IFNULL(a.genericattribute9, b.genericattribute9),  /* ORIGSQL: NVL(a.genericattribute9, b.genericattribute9) */
                    a.genericattribute10 =
                    IFNULL(a.genericattribute10, b.genericattribute10),  /* ORIGSQL: NVL(a.genericattribute10, b.genericattribute10) */
                    a.genericattribute11 =
                    IFNULL(a.genericattribute11, b.genericattribute11),  /* ORIGSQL: NVL(a.genericattribute11, b.genericattribute11) */
                    a.genericattribute12 =
                    IFNULL(a.genericattribute12, b.genericattribute12),  /* ORIGSQL: NVL(a.genericattribute12, b.genericattribute12) */
                    a.genericattribute13 =
                    IFNULL(a.genericattribute13, b.genericattribute13),  /* ORIGSQL: NVL(a.genericattribute13, b.genericattribute13) */
                    a.genericattribute14 =
                    IFNULL(a.genericattribute14, b.genericattribute14),  /* ORIGSQL: NVL(a.genericattribute14, b.genericattribute14) */
                    a.genericattribute15 =
                    IFNULL(a.genericattribute15, b.genericattribute15),  /* ORIGSQL: NVL(a.genericattribute15, b.genericattribute15) */
                    a.genericattribute16 =
                    IFNULL(a.genericattribute16, b.genericattribute16),  /* ORIGSQL: NVL(a.genericattribute16, b.genericattribute16) */
                    a.genericnumber1 = IFNULL(a.genericnumber1, b.genericnumber1),  /* ORIGSQL: NVL(a.genericnumber1, b.genericnumber1) */
                    a.unittypeforgenericnumber1 =
                    IFNULL(a.unittypeforgenericnumber1, b.unittypeforgenericnumber1),  /* ORIGSQL: NVL(a.unittypeforgenericnumber1, b.unittypeforgenericnumber1) */
                    a.genericnumber2 = IFNULL(a.genericnumber2, b.genericnumber2),  /* ORIGSQL: NVL(a.genericnumber2, b.genericnumber2) */
                    a.unittypeforgenericnumber2 =
                    IFNULL(a.unittypeforgenericnumber2, b.unittypeforgenericnumber2),  /* ORIGSQL: NVL(a.unittypeforgenericnumber2, b.unittypeforgenericnumber2) */
                    a.genericnumber3 = IFNULL(a.genericnumber3, b.genericnumber3),  /* ORIGSQL: NVL(a.genericnumber3, b.genericnumber3) */
                    a.unittypeforgenericnumber3 =
                    IFNULL(a.unittypeforgenericnumber3, b.unittypeforgenericnumber3),  /* ORIGSQL: NVL(a.unittypeforgenericnumber3, b.unittypeforgenericnumber3) */
                    a.genericnumber4 = IFNULL(a.genericnumber4, b.genericnumber4),  /* ORIGSQL: NVL(a.genericnumber4, b.genericnumber4) */
                    a.unittypeforgenericnumber4 =
                    IFNULL(a.unittypeforgenericnumber4, b.unittypeforgenericnumber4),  /* ORIGSQL: NVL(a.unittypeforgenericnumber4, b.unittypeforgenericnumber4) */
                    a.genericnumber5 = IFNULL(a.genericnumber5, b.genericnumber5),  /* ORIGSQL: NVL(a.genericnumber5, b.genericnumber5) */
                    a.unittypeforgenericnumber5 =
                    IFNULL(a.unittypeforgenericnumber5, b.unittypeforgenericnumber5),  /* ORIGSQL: NVL(a.unittypeforgenericnumber5, b.unittypeforgenericnumber5) */
                    a.genericnumber6 = IFNULL(a.genericnumber6, b.genericnumber6),  /* ORIGSQL: NVL(a.genericnumber6, b.genericnumber6) */
                    a.unittypeforgenericnumber6 =
                    IFNULL(a.unittypeforgenericnumber6, b.unittypeforgenericnumber6),  /* ORIGSQL: NVL(a.unittypeforgenericnumber6, b.unittypeforgenericnumber6) */

                    a.genericdate1 = IFNULL(a.genericdate1, b.genericdate1),  /* ORIGSQL: NVL(a.genericdate1, b.genericdate1) */
                    a.genericdate2 = IFNULL(a.genericdate2, b.genericdate2),  /* ORIGSQL: NVL(a.genericdate2, b.genericdate2) */
                    a.genericdate3 = IFNULL(a.genericdate3, b.genericdate3),  /* ORIGSQL: NVL(a.genericdate3, b.genericdate3) */
                    a.genericdate4 = IFNULL(b.genericdate4, a.genericdate4), --[arun commented on 9th June to avoid updating GA1 during file load
                    /* ORIGSQL: NVL(b.genericdate4, a.genericdate4) */
                    a.genericdate5 = IFNULL(a.genericdate5, b.genericdate5),  /* ORIGSQL: NVL(a.genericdate5, b.genericdate5) */
                    a.genericdate6 = IFNULL(a.genericdate6, b.genericdate6),  /* ORIGSQL: NVL(a.genericdate6, b.genericdate6) */
                    a.genericboolean1 = IFNULL(a.genericboolean1, b.genericboolean1),  /* ORIGSQL: NVL(a.genericboolean1, b.genericboolean1) */
                    a.genericboolean2 = IFNULL(a.genericboolean2, b.genericboolean2),  /* ORIGSQL: NVL(a.genericboolean2, b.genericboolean2) */
                    a.genericboolean3 = IFNULL(a.genericboolean3, b.genericboolean3),  /* ORIGSQL: NVL(a.genericboolean3, b.genericboolean3) */
                    a.genericboolean4 = IFNULL(a.genericboolean4, b.genericboolean4),  /* ORIGSQL: NVL(a.genericboolean4, b.genericboolean4) */
                    a.genericboolean5 = IFNULL(a.genericboolean5, b.genericboolean5),  /* ORIGSQL: NVL(a.genericboolean5, b.genericboolean5) */
                    a.genericboolean6 = IFNULL(a.genericboolean6, b.genericboolean6),  /* ORIGSQL: NVL(a.genericboolean6, b.genericboolean6) */
                    a.creditstartdate =
                    CASE
                        WHEN a.effectivestartdate <= :v_defaultHRCDate
                        THEN IFNULL(b.creditstartdate, a.creditstartdate)  /* ORIGSQL: NVL(b.creditstartdate, a.creditstartdate) */
                        ELSE IFNULL(a.creditstartdate, b.creditstartdate)  /* ORIGSQL: NVL(a.creditstartdate, b.creditstartdate) */
                    END,
                    a.creditenddate = IFNULL(a.creditenddate, b.creditenddate),  /* ORIGSQL: NVL(a.creditenddate, b.creditenddate) */
                    a.processingstartdate =
                    CASE
                        WHEN a.effectivestartdate <= :v_defaultHRCDate
                        THEN IFNULL(b.processingstartdate, a.processingstartdate)  /* ORIGSQL: NVL(b.processingstartdate, a.processingstartdate) */
                        ELSE IFNULL(a.processingstartdate, b.processingstartdate)  /* ORIGSQL: NVL(a.processingstartdate, b.processingstartdate) */
                    END,
                    a.processingenddate = IFNULL(a.processingenddate, b.processingenddate);-- WHERE b.titlename <>'Removed' --[Arun-20/3/2020]
            /* ORIGSQL: NVL(a.processingenddate, b.processingenddate) */

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '1. inbound_data_ogpo Attribute update :' || v_(...) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '1. inbound_data_ogpo Attribute update  :'|| IFNULL(:v_parameter.file_Date,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_Date,''),1,255) 
                , '1. inbound_data_ogpo Attribute update Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '1. inbound_data_ogpo Attribute update  :' || v_parameter.(...) */

            /* ORIGSQL: update inbound_Data_ogpo tgt SET unittypefortargetcompensation = (SELECT x.name (...) */
            UPDATE inbound_Data_ogpo tgt
                SET
                /* ORIGSQL: unittypefortargetcompensation = */
                unittypefortargetcompensation = (
                    SELECT   /* ORIGSQL: (select x.name from cs_unittype@stelext x where x.removedate>sysdate and x.unitt(...) */
                        x.name
                    FROM
                        cs_unittype x
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.unittypeseq = tgt.unittypefortargetcompensation
                ),
                /* ORIGSQL: unittypeforgenericnumber1 = */
                unittypeforgenericnumber1 = (
                    SELECT   /* ORIGSQL: (select x.name from cs_unittype@stelext x where x.removedate>sysdate and x.unitt(...) */
                        x.name
                    FROM
                        cs_unittype x
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.unittypeseq = tgt.unittypeforgenericnumber1
                ),
                /* ORIGSQL: unittypeforgenericnumber2 = */
                unittypeforgenericnumber2 = (
                    SELECT   /* ORIGSQL: (select x.name from cs_unittype@stelext x where x.removedate>sysdate and x.unitt(...) */
                        x.name
                    FROM
                        cs_unittype x
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.unittypeseq = tgt.unittypeforgenericnumber2
                ),
                /* ORIGSQL: unittypeforgenericnumber3 = */
                unittypeforgenericnumber3 = (
                    SELECT   /* ORIGSQL: (select x.name from cs_unittype@stelext x where x.removedate>sysdate and x.unitt(...) */
                        x.name
                    FROM
                        cs_unittype x
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.unittypeseq = tgt.unittypeforgenericnumber3
                ),
                /* ORIGSQL: unittypeforgenericnumber4 = */
                unittypeforgenericnumber4 = (
                    SELECT   /* ORIGSQL: (select x.name from cs_unittype@stelext x where x.removedate>sysdate and x.unitt(...) */
                        x.name
                    FROM
                        cs_unittype x
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.unittypeseq = tgt.unittypeforgenericnumber4
                ),
                /* ORIGSQL: unittypeforgenericnumber5 = */
                unittypeforgenericnumber5 = (
                    SELECT   /* ORIGSQL: (select x.name from cs_unittype@stelext x where x.removedate>sysdate and x.unitt(...) */
                        x.name
                    FROM
                        cs_unittype x
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.unittypeseq = tgt.unittypeforgenericnumber5
                ),
                /* ORIGSQL: unittypeforgenericnumber6 = */
                unittypeforgenericnumber6 = (
                    SELECT   /* ORIGSQL: (select x.name from cs_unittype@stelext x where x.removedate>sysdate and x.unitt(...) */
                        x.name
                    FROM
                        cs_unittype x
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.unittypeseq = tgt.unittypeforgenericnumber6
                )
            WHERE
                filename = :v_parameter.file_name
                AND filedate = :v_parameter.file_Date
                AND recordstatus = 0
                AND IFNULL(unittypefortargetcompensation,1) != 1;  /* ORIGSQL: is_number(unittypefortargetcompensation) */

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '1. inbound_data_ogpo Unit Type update :' || v_(...) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '1. inbound_data_ogpo Unit Type update  :'|| IFNULL(:v_parameter.file_Date,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_Date,''),1,255) 
                , '1. inbound_data_ogpo Attribute update Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '1. inbound_data_ogpo Unit Type update  :' || v_parameter.(...) */

            /* ORIGSQL: COMMIT; */
            COMMIT;

            -- Update Effective start date to v_defaultStartdate if atleast single version of position is not available in system   
            /* ORIGSQL: UPDATE inbound_data_ogpo tgt SET effectivestartdate = v_defaultStartdate, credit(...) */
            UPDATE inbound_data_ogpo tgt
                SET
                /* ORIGSQL: effectivestartdate = */
                effectivestartdate = :v_defaultStartdate,
                /* ORIGSQL: creditstartdate = */
                creditstartdate = :v_defaultStartdate,
                /* ORIGSQL: processingstartdate = */
                processingstartdate = :v_defaultStartdate
            FROM
                inbound_data_ogpo tgt
            WHERE
                (filename, filedate)  
                IN
                (
                    SELECT   /* ORIGSQL: (SELECT file_name, file_date FROM inbound_cfg_parameter WHERE object_name = 'SP_(...) */
                        file_name,
                        file_date
                    FROM
                        inbound_cfg_parameter
                    WHERE
                        object_name = 'SP_INBOUND_TXN_MAP'
                )
                AND NOT EXISTS
                (
                    SELECT   /* ORIGSQL: (SELECT 1 FROM cs_position@stelext WHERE name = tgt.positionname) */
                        1
                    FROM
                        cs_position
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        name = tgt.positionname
                );

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '2. Update inbound_data_ogpo EffectivStart date(...) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '2. Update inbound_data_ogpo EffectivStart date to DefaultStartdate  :'|| IFNULL(:v_parameter.file_Date,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_Date,''),1,255) 
                , '2. Update inbound_data_ogpo EffectivStart date to DefaultStartdateExecution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '2. Update inbound_data_ogpo EffectivStart date to Default(...) */

            /* ORIGSQL: COMMIT; */
            COMMIT;

            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_OGPT' not found */

            /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
            /* ORIGSQL: MERGE INTO inbound_data_ogpt a USING (SELECT v_parameter.file_date, v_parameter.(...) */
            MERGE INTO inbound_data_ogpt AS a
                USING
                (
                    SELECT   /* ORIGSQL: (SELECT v_parameter.file_date, v_parameter.file_name, cspy.payeeid, ogpt.effecti(...) */
                        :v_parameter.file_Date,
                        :v_parameter.file_name,
                        cspy.payeeid,
                        ogpt.effectivestartdate,
                        ogpt.effectiveenddate,
                        ogpt.prefix,
                        ogpt.firstname,
                        ogpt.middlename,
                        ogpt.lastname,
                        ogpt.suffix,
                        ogpt.taxid,
                        ogpt.salary,
                        ogpt.unittypeforsalary,
                        ogpt.hiredate,
                        ogpt.terminationdate,
                        NULL AS bu,
                        NULL AS description,
                        ogpt.genericattribute1,
                        ogpt.genericattribute2,
                        ogpt.genericattribute3,
                        ogpt.genericattribute4,
                        ogpt.genericattribute5,
                        ogpt.genericattribute6,
                        ogpt.genericattribute7,
                        ogpt.genericattribute8,
                        ogpt.genericattribute9,
                        ogpt.genericattribute10,
                        ogpt.genericattribute11,
                        ogpt.genericattribute12,
                        ogpt.genericattribute13,
                        ogpt.genericattribute14,
                        ogpt.genericattribute15,
                        ogpt.genericattribute16,
                        ogpt.genericnumber1,
                        ogpt.unittypeforgenericnumber1,
                        ogpt.genericnumber2,
                        ogpt.unittypeforgenericnumber2,
                        ogpt.genericnumber3,
                        ogpt.unittypeforgenericnumber3,
                        ogpt.genericnumber4,
                        ogpt.unittypeforgenericnumber4,
                        ogpt.genericnumber5,
                        ogpt.unittypeforgenericnumber5,
                        ogpt.genericnumber6,
                        ogpt.unittypeforgenericnumber6,
                        ogpt.genericdate1,
                        ogpt.genericdate2,
                        ogpt.genericdate3,
                        ogpt.genericdate4,
                        ogpt.genericdate5,
                        ogpt.genericdate6,
                        ogpt.genericboolean1,
                        ogpt.genericboolean2,
                        ogpt.genericboolean3,
                        ogpt.genericboolean4,
                        ogpt.genericboolean5,
                        ogpt.genericboolean6,
                        ogpt.userid,
                        ogpt.participantemail
                    FROM
                        cs_participant ogpt,
                        cs_payee cspy

                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_payee@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_payee_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_participant@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_participant_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        1 = 1
                        AND ogpt.payeeseq = cspy.payeeseq
                        AND ogpt.effectiveenddate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND ogpt.effectivestartdate <= CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                        AND ogpt.removedate = TO_DATE('22000101','YYYYMMDD')
                        AND cspy.effectiveenddate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                        AND cspy.effectivestartdate <= CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                        AND cspy.removedate = TO_DATE('22000101','YYYYMMDD')
                ) AS b
                ON (a.payeeid = b.payeeid

                )
            WHEN MATCHED THEN
                UPDATE SET
                    a.effectivestartdate =
                    CASE
                        WHEN a.effectivestartdate <= :v_defaultHRCDate
                        THEN IFNULL(b.effectivestartdate, a.effectivestartdate)  /* ORIGSQL: NVL(b.effectivestartdate, a.effectivestartdate) */
                        ELSE IFNULL(a.effectivestartdate, b.effectivestartdate)  /* ORIGSQL: NVL(a.effectivestartdate, b.effectivestartdate) */
                    END,
                    a.effectiveenddate = IFNULL(a.effectiveenddate, b.effectiveenddate),  /* ORIGSQL: NVL(a.effectiveenddate, b.effectiveenddate) */
                    a.prefix = IFNULL(a.prefix, b.prefix),  /* ORIGSQL: NVL(a.prefix, b.prefix) */
                    a.firstname = IFNULL(a.firstname, b.firstname),  /* ORIGSQL: NVL(a.firstname, b.firstname) */
                    a.middlename = IFNULL(a.middlename, b.middlename),  /* ORIGSQL: NVL(a.middlename, b.middlename) */
                    a.lastname = IFNULL(a.lastname, b.lastname),  /* ORIGSQL: NVL(a.lastname, b.lastname) */
                    a.suffix = IFNULL(a.suffix, b.suffix),  /* ORIGSQL: NVL(a.suffix, b.suffix) */
                    a.taxid = IFNULL(a.taxid, b.taxid),  /* ORIGSQL: NVL(a.taxid, b.taxid) */
                    a.salary = IFNULL(a.salary, b.salary),  /* ORIGSQL: NVL(a.salary, b.salary) */
                    a.unittypeforsalary = IFNULL(a.unittypeforsalary, b.unittypeforsalary),  /* ORIGSQL: NVL(a.unittypeforsalary, b.unittypeforsalary) */
                    a.hiredate = IFNULL(a.hiredate, b.hiredate),  /* ORIGSQL: NVL(a.hiredate, b.hiredate) */
                    a.terminationdate = IFNULL(a.terminationdate, b.terminationdate),  /* ORIGSQL: NVL(a.terminationdate, b.terminationdate) */
                    a.businessunitname = IFNULL(a.businessunitname, b.bu),  /* ORIGSQL: NVL(a.businessunitname, b.bu) */
                    a.description = IFNULL(a.description, b.description),  /* ORIGSQL: NVL(a.description, b.description) */
                    a.genericattribute1 = IFNULL(b.genericattribute1, a.genericattribute1), --[arun added on 18th June to avoid updating Participant GA1 during file load
                    /* ORIGSQL: NVL(b.genericattribute1, a.genericattribute1) */
                    a.genericattribute2 = IFNULL(a.genericattribute2, b.genericattribute2),  /* ORIGSQL: NVL(a.genericattribute2, b.genericattribute2) */
                    a.genericattribute3 = IFNULL(a.genericattribute3, b.genericattribute3),  /* ORIGSQL: NVL(a.genericattribute3, b.genericattribute3) */
                    a.genericattribute4 = IFNULL(a.genericattribute4, b.genericattribute4),  /* ORIGSQL: NVL(a.genericattribute4, b.genericattribute4) */
                    a.genericattribute5 = IFNULL(a.genericattribute5, b.genericattribute5),  /* ORIGSQL: NVL(a.genericattribute5, b.genericattribute5) */
                    a.genericattribute6 = IFNULL(a.genericattribute6, b.genericattribute6),  /* ORIGSQL: NVL(a.genericattribute6, b.genericattribute6) */
                    a.genericattribute7 = IFNULL(a.genericattribute7, b.genericattribute7),  /* ORIGSQL: NVL(a.genericattribute7, b.genericattribute7) */
                    a.genericattribute8 = IFNULL(a.genericattribute8, b.genericattribute8),  /* ORIGSQL: NVL(a.genericattribute8, b.genericattribute8) */
                    a.genericattribute9 = IFNULL(a.genericattribute9, b.genericattribute9),  /* ORIGSQL: NVL(a.genericattribute9, b.genericattribute9) */
                    a.genericattribute10 =
                    IFNULL(a.genericattribute10, b.genericattribute10),  /* ORIGSQL: NVL(a.genericattribute10, b.genericattribute10) */
                    a.genericattribute11 =
                    IFNULL(a.genericattribute11, b.genericattribute11),  /* ORIGSQL: NVL(a.genericattribute11, b.genericattribute11) */
                    a.genericattribute12 =
                    IFNULL(a.genericattribute12, b.genericattribute12),  /* ORIGSQL: NVL(a.genericattribute12, b.genericattribute12) */
                    a.genericattribute13 =
                    IFNULL(a.genericattribute13, b.genericattribute13),  /* ORIGSQL: NVL(a.genericattribute13, b.genericattribute13) */
                    a.genericattribute14 =
                    IFNULL(a.genericattribute14, b.genericattribute14),  /* ORIGSQL: NVL(a.genericattribute14, b.genericattribute14) */
                    a.genericattribute15 =
                    IFNULL(a.genericattribute15, b.genericattribute15),  /* ORIGSQL: NVL(a.genericattribute15, b.genericattribute15) */
                    a.genericattribute16 =
                    IFNULL(a.genericattribute16, b.genericattribute16),  /* ORIGSQL: NVL(a.genericattribute16, b.genericattribute16) */
                    a.genericnumber1 = IFNULL(a.genericnumber1, b.genericnumber1),  /* ORIGSQL: NVL(a.genericnumber1, b.genericnumber1) */
                    a.unittypeforgenericnumber1 =
                    IFNULL(a.unittypeforgenericnumber1, b.unittypeforgenericnumber1),  /* ORIGSQL: NVL(a.unittypeforgenericnumber1, b.unittypeforgenericnumber1) */
                    a.genericnumber2 = IFNULL(a.genericnumber2, b.genericnumber2),  /* ORIGSQL: NVL(a.genericnumber2, b.genericnumber2) */
                    a.unittypeforgenericnumber2 =
                    IFNULL(a.unittypeforgenericnumber2, b.unittypeforgenericnumber2),  /* ORIGSQL: NVL(a.unittypeforgenericnumber2, b.unittypeforgenericnumber2) */
                    a.genericnumber3 = IFNULL(a.genericnumber3, b.genericnumber3),  /* ORIGSQL: NVL(a.genericnumber3, b.genericnumber3) */
                    a.unittypeforgenericnumber3 =
                    IFNULL(a.unittypeforgenericnumber3, b.unittypeforgenericnumber3),  /* ORIGSQL: NVL(a.unittypeforgenericnumber3, b.unittypeforgenericnumber3) */
                    a.genericnumber4 = IFNULL(a.genericnumber4, b.genericnumber4),  /* ORIGSQL: NVL(a.genericnumber4, b.genericnumber4) */
                    a.unittypeforgenericnumber4 =
                    IFNULL(a.unittypeforgenericnumber4, b.unittypeforgenericnumber4),  /* ORIGSQL: NVL(a.unittypeforgenericnumber4, b.unittypeforgenericnumber4) */
                    a.genericnumber5 = IFNULL(a.genericnumber5, b.genericnumber5),  /* ORIGSQL: NVL(a.genericnumber5, b.genericnumber5) */
                    a.unittypeforgenericnumber5 =
                    IFNULL(a.unittypeforgenericnumber5, b.unittypeforgenericnumber5),  /* ORIGSQL: NVL(a.unittypeforgenericnumber5, b.unittypeforgenericnumber5) */
                    a.genericnumber6 = IFNULL(a.genericnumber6, b.genericnumber6),  /* ORIGSQL: NVL(a.genericnumber6, b.genericnumber6) */
                    a.unittypeforgenericnumber6 =
                    IFNULL(a.unittypeforgenericnumber6, b.unittypeforgenericnumber6),  /* ORIGSQL: NVL(a.unittypeforgenericnumber6, b.unittypeforgenericnumber6) */
                    a.genericdate1 = IFNULL(a.genericdate1, b.genericdate1),  /* ORIGSQL: NVL(a.genericdate1, b.genericdate1) */
                    a.genericdate2 = IFNULL(a.genericdate2, b.genericdate2),  /* ORIGSQL: NVL(a.genericdate2, b.genericdate2) */
                    a.genericdate3 = IFNULL(a.genericdate3, b.genericdate3),  /* ORIGSQL: NVL(a.genericdate3, b.genericdate3) */
                    a.genericdate4 = IFNULL(a.genericdate4, b.genericdate4), --[arun commented on 9th June to avoid updating GA1 during file load
                    /* ORIGSQL: NVL(a.genericdate4, b.genericdate4) */
                    a.genericdate5 = IFNULL(a.genericdate5, b.genericdate5),  /* ORIGSQL: NVL(a.genericdate5, b.genericdate5) */
                    a.genericdate6 = IFNULL(a.genericdate6, b.genericdate6),  /* ORIGSQL: NVL(a.genericdate6, b.genericdate6) */
                    a.genericboolean1 = IFNULL(a.genericboolean1, b.genericboolean1),  /* ORIGSQL: NVL(a.genericboolean1, b.genericboolean1) */
                    a.genericboolean2 = IFNULL(a.genericboolean2, b.genericboolean2),  /* ORIGSQL: NVL(a.genericboolean2, b.genericboolean2) */
                    a.genericboolean3 = IFNULL(a.genericboolean3, b.genericboolean3),  /* ORIGSQL: NVL(a.genericboolean3, b.genericboolean3) */
                    a.genericboolean4 = IFNULL(a.genericboolean4, b.genericboolean4),  /* ORIGSQL: NVL(a.genericboolean4, b.genericboolean4) */
                    a.genericboolean5 = IFNULL(a.genericboolean5, b.genericboolean5),  /* ORIGSQL: NVL(a.genericboolean5, b.genericboolean5) */
                    a.genericboolean6 = IFNULL(a.genericboolean6, b.genericboolean6),  /* ORIGSQL: NVL(a.genericboolean6, b.genericboolean6) */
                    a.userid = IFNULL(a.userid, b.userid),  /* ORIGSQL: NVL(a.userid, b.userid) */
                    a.participantemail = IFNULL(a.participantemail, b.participantemail);  /* ORIGSQL: NVL(a.participantemail, b.participantemail) */

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '3. Update inbound_data_ogpt attribute update :(...) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '3. Update inbound_data_ogpt attribute update  :'|| IFNULL(:v_parameter.file_Date,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_Date,''),1,255) 
                , '3. Update inbound_data_ogpt attribute update Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '3. Update inbound_data_ogpt attribute update  :' || v_par(...) */

            /* ORIGSQL: update inbound_Data_ogpt tgt SET unittypeforsalary = (SELECT x.name FROM DBMTK_U(...) */
            UPDATE inbound_Data_ogpt tgt
                SET
                /* ORIGSQL: unittypeforsalary = */
                unittypeforsalary = (
                    SELECT   /* ORIGSQL: (select x.name from cs_unittype@stelext x where x.removedate>sysdate and x.unitt(...) */
                        x.name
                    FROM
                        cs_unittype x
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.unittypeseq = tgt.unittypeforsalary
                ),
                /* ORIGSQL: unittypeforgenericnumber1 = */
                unittypeforgenericnumber1 = (
                    SELECT   /* ORIGSQL: (select x.name from cs_unittype@stelext x where x.removedate>sysdate and x.unitt(...) */
                        x.name
                    FROM
                        cs_unittype x
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.unittypeseq = tgt.unittypeforgenericnumber1
                ),
                /* ORIGSQL: unittypeforgenericnumber2 = */
                unittypeforgenericnumber2 = (
                    SELECT   /* ORIGSQL: (select x.name from cs_unittype@stelext x where x.removedate>sysdate and x.unitt(...) */
                        x.name
                    FROM
                        cs_unittype x
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.unittypeseq = tgt.unittypeforgenericnumber2
                ),
                /* ORIGSQL: unittypeforgenericnumber3 = */
                unittypeforgenericnumber3 = (
                    SELECT   /* ORIGSQL: (select x.name from cs_unittype@stelext x where x.removedate>sysdate and x.unitt(...) */
                        x.name
                    FROM
                        cs_unittype x
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.unittypeseq = tgt.unittypeforgenericnumber3
                ),
                /* ORIGSQL: unittypeforgenericnumber4 = */
                unittypeforgenericnumber4 = (
                    SELECT   /* ORIGSQL: (select x.name from cs_unittype@stelext x where x.removedate>sysdate and x.unitt(...) */
                        x.name
                    FROM
                        cs_unittype x
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.unittypeseq = tgt.unittypeforgenericnumber4
                ),
                /* ORIGSQL: unittypeforgenericnumber5 = */
                unittypeforgenericnumber5 = (
                    SELECT   /* ORIGSQL: (select x.name from cs_unittype@stelext x where x.removedate>sysdate and x.unitt(...) */
                        x.name
                    FROM
                        cs_unittype x
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.unittypeseq = tgt.unittypeforgenericnumber5
                ),
                /* ORIGSQL: unittypeforgenericnumber6 = */
                unittypeforgenericnumber6 = (
                    SELECT   /* ORIGSQL: (select x.name from cs_unittype@stelext x where x.removedate>sysdate and x.unitt(...) */
                        x.name
                    FROM
                        cs_unittype x
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.unittypeseq = tgt.unittypeforgenericnumber6
                )
            WHERE
                filename = :v_parameter.file_name
                AND filedate = :v_parameter.file_Date
                AND recordstatus = 0
                AND IFNULL(tgt.unittypeforsalary, 1) != 1;  /* ORIGSQL: is_number(tgt.unittypeforsalary) */

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '3. Update inbound_data_ogpt Unit Type update :(...) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '3. Update inbound_data_ogpt Unit Type update  :'|| IFNULL(:v_parameter.file_Date,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_Date,''),1,255) 
                , '3. Update inbound_data_ogpt attribute update Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '3. Update inbound_data_ogpt Unit Type update  :' || v_par(...) */

            /* ORIGSQL: COMMIT; */
            COMMIT;

            -- Update Effective start date to v_defaultStartdate if atleast single version of Payee is not available in system   
            /* ORIGSQL: UPDATE inbound_data_ogpt tgt SET effectivestartdate = v_defaultStartdate WHERE ((...) */
            UPDATE inbound_data_ogpt tgt
                SET
                /* ORIGSQL: effectivestartdate = */
                effectivestartdate = :v_defaultStartdate
            FROM
                inbound_data_ogpt tgt
            WHERE
                (filename, filedate)  
                IN
                (
                    SELECT   /* ORIGSQL: (SELECT file_name, file_date FROM inbound_cfg_parameter WHERE object_name = 'SP_(...) */
                        file_name,
                        file_date
                    FROM
                        inbound_cfg_parameter
                    WHERE
                        object_name = 'SP_INBOUND_TXN_MAP'
                )
                AND NOT EXISTS
                (
                    SELECT   /* ORIGSQL: (SELECT 1 FROM cs_payee@stelext WHERE payeeid = tgt.payeeid) */
                        1
                    FROM
                        cs_payee
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_payee@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_payee_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        payeeid = tgt.payeeid
                );

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '4. Update inbound_data_ogpt EffectivStart date(...) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '4. Update inbound_data_ogpt EffectivStart date to DefaultStartdate  :'|| IFNULL(:v_parameter.file_Date,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_Date,''),1,255) 
                , '4. Update inbound_data_ogpt EffectivStart date to DefaultStartdate Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '4. Update inbound_data_ogpt EffectivStart date to Default(...) */

            /* ORIGSQL: COMMIT; */
            COMMIT;  

            /* ORIGSQL: update inbound_data_ogpo tgt SET newbatchnumber = 1, batchnumber=1 where (tgt.fi(...) */
            UPDATE inbound_data_ogpo tgt
                SET
                /* ORIGSQL: newbatchnumber = */
                newbatchnumber = 1,
                /* ORIGSQL: batchnumber = */
                batchnumber = 1
            FROM
                inbound_data_ogpo tgt
            WHERE
                (tgt.filename,tgt.filedate) 
                =
                (
                    SELECT   /* ORIGSQL: (select file_name,file_date from inbound_cfg_Parameter where object_name = 'SP_I(...) */
                        file_name,
                        file_date
                    FROM
                        inbound_cfg_Parameter
                    WHERE
                        object_name = 'SP_INBOUND_TXN_MAP'
                );

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '5. Update inbound_data_ogpo NewBatchNo and Bat(...) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '5. Update inbound_data_ogpo NewBatchNo and BatchNo  :'|| IFNULL(:v_parameter.file_Date,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_Date,''),1,255) 
                , '5. Update inbound_data_ogpo NewBatchNo and BatchNo Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '5. Update inbound_data_ogpo NewBatchNo and BatchNo  :' ||(...) */

            /* ORIGSQL: Commit; */
            COMMIT;  

            /* ORIGSQL: update inbound_data_ogpt tgt SET newbatchnumber = 1, batchnumber=1 where (tgt.fi(...) */
            UPDATE inbound_data_ogpt tgt
                SET
                /* ORIGSQL: newbatchnumber = */
                newbatchnumber = 1,
                /* ORIGSQL: batchnumber = */
                batchnumber = 1
            FROM
                inbound_data_ogpt tgt
            WHERE
                (tgt.filename,tgt.filedate) 
                =
                (
                    SELECT   /* ORIGSQL: (select file_name,file_date from inbound_cfg_Parameter where object_name = 'SP_I(...) */
                        file_name,
                        file_date
                    FROM
                        inbound_cfg_Parameter
                    WHERE
                        object_name = 'SP_INBOUND_TXN_MAP'
                );

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '5. Update inbound_data_ogpt NewBatchNo and Bat(...) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '5. Update inbound_data_ogpt NewBatchNo and BatchNo  :'|| IFNULL(:v_parameter.file_Date,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_Date,''),1,255) 
                , '5. Update inbound_data_ogpt NewBatchNo and BatchNo Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '5. Update inbound_data_ogpt NewBatchNo and BatchNo  :' ||(...) */

            /* ORIGSQL: Commit; */
            COMMIT;

            -- Update SUDONG Job Grade   
            /* ORIGSQL: UPDATE inbound_data_ogpo tgt SET genericattribute4 = genericattribute4||'-Sudong(...) */
            UPDATE inbound_data_ogpo tgt
                SET
                /* ORIGSQL: genericattribute4 = */
                genericattribute4 = IFNULL(TO_VARCHAR(genericattribute4),'')||'-Sudong' 
            FROM
                inbound_data_ogpo tgt
            WHERE
                (filename, filedate)  
                IN
                (
                    SELECT   /* ORIGSQL: (SELECT file_name, file_date FROM inbound_cfg_parameter WHERE object_name = 'SP_(...) */
                        file_name,
                        file_date
                    FROM
                        inbound_cfg_parameter
                    WHERE
                        object_name = 'SP_INBOUND_TXN_MAP'
                )
                AND UPPER(titlename) LIKE '%SUDONG%';

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '20. Update inbound_data_ogpo Sudong Job grade:(...) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '20. Update inbound_data_ogpo Sudong Job grade:'|| IFNULL(:v_parameter.file_Date,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_Date,''),1,255) 
                , '20. Update inbound_data_ogpo Sudong Job grade Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '20. Update inbound_data_ogpo Sudong Job grade:' || v_para(...) */

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END IF;

        /* ORIGSQL: EXCEPTION WHEN NO_DATA_FOUND THEN */
END