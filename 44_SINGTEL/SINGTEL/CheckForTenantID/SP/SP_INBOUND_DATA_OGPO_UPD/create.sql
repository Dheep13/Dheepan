CREATE PROCEDURE EXT.SP_INBOUND_DATA_OGPO_UPD
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_DATA_OGPO_UPD';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_DATA_OGPO_UPD'; */
    DECLARE v_parameter ROW LIKE Inbound_cfg_Parameter; --%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.Inbound_cfg_Parameter' not found (for %ROWTYPE declaration) */
    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount integer; */

    DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
        /* ORIGSQL: WHEN NO_DATA_FOUND THEN */
        BEGIN
            /* ORIGSQL: NULL; */
            DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
        END;

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

        SELECT
            DISTINCT
            *
        INTO
            v_parameter
        FROM
            EXT.Inbound_cfg_Parameter;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO inbound_data_ogpo a USING (SELECT ogpo.effectivestartdate, ogpo.effec(...) */
        MERGE INTO EXT.Inbound_data_ogpo AS a
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_OGPO' not found */
            USING
            (
                SELECT   /* ORIGSQL: (SELECT ogpo.effectivestartdate, ogpo.effectiveenddate, cspy.payeeid, NULL AS pa(...) */
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
                    csbu.name AS businessunitname,
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
                    ogpo.processingenddate
                FROM
                    /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                    cs_businessunit AS csbu
                INNER JOIN
                    cs_payee AS cspy
                    ON cspy.businessunitmap = csbu.mask
                    AND cspy.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                    AND cspy.effectiveenddate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                    AND cspy.islast = 1
                INNER JOIN
                    cs_position AS ogpo
                    ON ogpo.payeeseq = cspy.payeeseq
                    AND ogpo.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                    AND ogpo.effectiveenddate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                    AND ogpo.islast = 1
                LEFT OUTER JOIN
                   cs_title AS csti
                    ON ogpo.titleseq = csti.ruleelementownerseq  /* ORIGSQL: ogpo.titleseq = csti.ruleelementownerseq(+) */
                    AND csti.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: csti.removedate(+) > SYSDATE */
                    AND csti.effectiveenddate > CURRENT_TIMESTAMP   /* ORIGSQL: csti.effectiveenddate(+) > SYSDATE */
                    AND csti.islast = 1  /* ORIGSQL: csti.islast(+) = 1 */
                LEFT OUTER JOIN
                    cs_positiongroup AS ogpg
                    ON ogpo.positiongroupseq = ogpg.positiongroupseq  /* ORIGSQL: ogpo.positiongroupseq = ogpg.positiongroupseq(+) */
                    AND ogpg.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: ogpg.removedate(+) > SYSDATE */
                LEFT OUTER JOIN
                    cs_position AS ogpo_mgr
                    ON ogpo.managerseq = ogpo_mgr.ruleelementownerseq  /* ORIGSQL: ogpo.managerseq = ogpo_mgr.ruleelementownerseq(+) */
                    AND ogpo_mgr.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: ogpo_mgr.removedate(+) > SYSDATE */
                    AND ogpo_mgr.effectiveenddate > CURRENT_TIMESTAMP   /* ORIGSQL: ogpo_mgr.effectiveenddate(+) > SYSDATE */
                    AND ogpo_mgr.islast = 1  /* ORIGSQL: ogpo_mgr.islast(+) = 1 */
                INNER JOIN
                    EXT.inbound_data_ogpo AS stgpo
                    ON stgpo.positionname = ogpo.name
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_title@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_title_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_positiongroup@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_positiongroup_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_payee@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_payee_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_businessunit@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_businessunit_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            ) AS b
            ON (a.positionname = b.positionname)
        WHEN MATCHED THEN
            UPDATE SET
                a.effectivestartdate =
                IFNULL(b.effectivestartdate, a.effectivestartdate),  /* ORIGSQL: NVL(b.effectivestartdate, a.effectivestartdate) */
                a.effectiveenddate = IFNULL(b.effectiveenddate, a.effectiveenddate),  /* ORIGSQL: NVL(b.effectiveenddate, a.effectiveenddate) */
                a.payeeid = IFNULL(a.payeeid, b.payeeid),  /* ORIGSQL: NVL(a.payeeid, b.payeeid) */
                a.managername = IFNULL(a.managername, b.positionname),  /* ORIGSQL: NVL(a.managername, b.positionname) */
                a.titlename = IFNULL(a.titlename, b.titlename),  /* ORIGSQL: NVL(a.titlename, b.titlename) */
                a.positiongroupname = IFNULL(a.positiongroupname, b.positiongroupname),  /* ORIGSQL: NVL(a.positiongroupname, b.positiongroupname) */
                a.targetcompensation =
                IFNULL(a.targetcompensation, b.targetcompensation),  /* ORIGSQL: NVL(a.targetcompensation, b.targetcompensation) */
                a.unittypefortargetcompensation =
                IFNULL(a.unittypefortargetcompensation,  /* ORIGSQL: NVL(a.unittypefortargetcompensation, b.unittypefortargetcompensation) */
                b.unittypefortargetcompensation),
                a.businessunitname = IFNULL(a.businessunitname, b.businessunitname),  /* ORIGSQL: NVL(a.businessunitname, b.businessunitname) */
                a.genericattribute1 = IFNULL(b.genericattribute1, a.genericattribute1), --[arun commented on 9th June to avoid updating GA1 during file load
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
                a.creditstartdate = IFNULL(a.creditstartdate, b.creditstartdate),  /* ORIGSQL: NVL(a.creditstartdate, b.creditstartdate) */
                a.creditenddate = IFNULL(a.creditenddate, b.creditenddate),  /* ORIGSQL: NVL(a.creditenddate, b.creditenddate) */
                a.processingstartdate =
                IFNULL(a.processingstartdate, b.processingstartdate),  /* ORIGSQL: NVL(a.processingstartdate, b.processingstartdate) */
                a.processingenddate = IFNULL(a.processingenddate, b.processingenddate);  /* ORIGSQL: NVL(a.processingenddate, b.processingenddate) */

        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

        /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'inbound_data_ogpo update with all attribute :'(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'inbound_data_ogpo update with all attribute  :'|| IFNULL(:v_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_date,''),1,255) 
            , 'inbound_data_ogpo update with all attribute Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'inbound_data_ogpo update with all attribute  :' || v_para(...) */

        /* ORIGSQL: EXCEPTION WHEN NO_DATA_FOUND THEN */
END