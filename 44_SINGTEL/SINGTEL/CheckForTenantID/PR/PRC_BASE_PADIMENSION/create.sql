CREATE PROCEDURE EXT.PRC_BASE_PADIMENSION
(
    IN vperiodseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_BASE_PADIMENSION.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    IN vprocessingunitseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_BASE_PADIMENSION.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    IN vcalendarseq BIGINT      /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
                                                    /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_BASE_PADIMENSION.vcalendarseq' */
                                                    /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    -------------------------------------------------------------------------------------------------------------------
    -- Purpose:
    --
    -- Design objectives:
    --
    -------------------------------------------------------------------------------------------------------------------
    -- Modification Log:
    -- Date             Author        Description
    -------------------------------------------------------------------------------------------------------------------
    -- 30-Nov-2017      Tharanikumar  Initial release
    --
    --
    -------------------------------------------------------------------------------------------------------------------
    DECLARE vprocname VARCHAR(30) = UPPER('PRC_BASE_PADIMENSION');  /* ORIGSQL: vprocname VARCHAR2(30) := UPPER('PRC_BASE_PADIMENSION') ; */
    DECLARE vsqlerrm VARCHAR(3900);  /* ORIGSQL: vsqlerrm VARCHAR2(3900); */
    DECLARE vprocessingunitrow ROW LIKE cs_processingunit;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.cs_processingunit' not found (for %ROWTYPE declaration) */
    DECLARE vperiodcalendarrow ROW LIKE cs_period;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.cs_period' not found (for %ROWTYPE declaration) */

    DECLARE vpipelinerundate TIMESTAMP;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table/Column 'cs_credit.pipelinerundate' not found (for %TYPE declaration) */
    DECLARE vrunmode VARCHAR(50);/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table/Column 'cs_pipelinerun.runmode' not found (for %TYPE declaration) */
    DECLARE vpipelinerunseq BIGINT  ;/* NOT CONVERTED! */

    /* RESOLVE: Identifier not found: Table/Column 'cs_pipelinerun.pipelinerunseq' not found (for %TYPE declaration) */
    DECLARE vcalendarrow ROW LIKE cs_calendar;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.cs_calendar' not found (for %ROWTYPE declaration) */

    /* ORIGSQL: Cursor C_Padimension is SELECT 'STEL', SYSDATE AS loaddttm, vpipelinerundate pip(...) */
    DECLARE CURSOR C_Padimension
    FOR    
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_REPORTGROUP' not found */

        SELECT   /* ORIGSQL: SELECT 'STEL', SYSDATE AS loaddttm, vpipelinerundate pipelinerundate, vprocessin(...) */
            'STEL',
            CURRENT_TIMESTAMP AS loaddttm,  /* ORIGSQL: SYSDATE */
            :vpipelinerundate AS pipelinerundate,
            :vprocessingunitrow.processingunitseq,
            :vprocessingunitrow.NAME AS processingunitname,
            :vperiodcalendarrow.calendarseq,
            :vcalendarrow.name AS calendarname,
            :vperiodcalendarrow.periodseq,
            :vperiodcalendarrow.name AS PERIODNAME,
            :vperiodcalendarrow.startdate AS periodstartdate,
            :vperiodcalendarrow.enddate AS periodenddate,
            cs_participant.payeeseq,
            cs_position.ruleelementownerseq AS positionseq,
            cs_position.managerseq,
            cs_payee.payeeid AS participantid,
            cs_position.NAME AS positionname,
            cs_title.NAME AS positiontitle,
            cs_participant.prefix,
            cs_participant.firstname,
            cs_participant.middlename,
            cs_participant.lastname,
            cs_participant.suffix,
            TRIM(IFNULL(cs_participant.firstname, ' ')  /* ORIGSQL: NVL(cs_participant.firstname, ' ') */
                || IFNULL(cs_participant.middlename, ' ')  /* ORIGSQL: NVL(cs_participant.middlename, ' ') */
                || IFNULL(cs_participant.lastname, ' ')) AS fullname,  /* ORIGSQL: NVL(cs_participant.lastname, ' ') */
            cs_participant.userid,
            cs_participant.taxid,
            cs_participant.salary,
            cs_participant.unittypeforsalary,
            cs_position.targetcompensation,
            cs_position.unittypefortargetcompensation,
            cs_participant.effectivestartdate AS participanteffectivestartdate,
            cs_participant.effectiveenddate AS participanteffectiveenddate,
            cs_participant.createdate AS participantcreatedate,
            cs_participant.removedate AS participantremovedate,
            cs_participant.hiredate,
            cs_participant.terminationdate,
            cs_position.creditstartdate AS positioncreditstartdate,
            cs_position.creditenddate AS positioncreditenddate,
            cs_position.processingstartdate AS positionprocessingstartdate,
            cs_position.processingenddate AS positionprocessingenddate,
            cs_position.effectivestartdate AS positioneffectivestartdate,
            cs_position.effectiveenddate AS positioneffectiveenddate,
            cs_position.createdate AS positioncreatedate,
            cs_position.removedate AS positionremovedate,
            cs_title.effectivestartdate AS titleeffectivestartdate,
            cs_title.effectiveenddate AS titleeffectiveenddate,
            cs_title.createdate AS titlecreatedate,
            cs_title.removedate AS titleremovedate,
            cs_participant.genericattribute1 AS participantga1,
            cs_participant.genericattribute2 AS participantga2,
            cs_participant.genericattribute3 AS participantga3,
            cs_participant.genericattribute4 AS participantga4,
            cs_participant.genericattribute5 AS participantga5,
            cs_participant.genericattribute6 AS participantga6,
            cs_participant.genericattribute7 AS participantga7,
            cs_participant.genericattribute8 AS participantga8,
            cs_participant.genericattribute9 AS participantga9,
            cs_participant.genericattribute10 AS participantga10,
            cs_participant.genericattribute11 AS participantga11,
            cs_participant.genericattribute12 AS participantga12,
            cs_participant.genericattribute13 AS participantga13,
            cs_participant.genericattribute14 AS participantga14,
            cs_participant.genericattribute15 AS participantga15,
            cs_participant.genericattribute16 AS participantga16,
            cs_participant.genericnumber1 AS participantgn1,
            cs_participant.unittypeforgenericnumber1 AS participantunittypeforgn1,
            cs_participant.genericnumber2 AS participantgn2,
            cs_participant.unittypeforgenericnumber2 AS participantunittypeforgn2,
            cs_participant.genericnumber3 AS participantgn3,
            cs_participant.unittypeforgenericnumber3 AS participantunittypeforgn3,
            cs_participant.genericnumber4 AS participantgn4,
            cs_participant.unittypeforgenericnumber4 AS participantunittypeforgn4,
            cs_participant.genericnumber5 AS participantgn5,
            cs_participant.unittypeforgenericnumber5 AS participantunittypeforgn5,
            cs_participant.genericnumber6 AS participantgn6,
            cs_participant.unittypeforgenericnumber6 AS participantunittypeforgn6,
            cs_participant.genericdate1 AS participantgd1,
            cs_participant.genericdate2 AS participantgd2,
            cs_participant.genericdate3 AS participantgd3,
            cs_participant.genericdate4 AS participantgd4,
            cs_participant.genericdate5 AS participantgd5,
            cs_participant.genericdate6 AS participantgd6,
            cs_participant.genericboolean1 AS participantgb1,
            cs_participant.genericboolean2 AS participantgb2,
            cs_participant.genericboolean3 AS participantgb3,
            cs_participant.genericboolean4 AS participantgb4,
            cs_participant.genericboolean5 AS participantgb5,
            cs_participant.genericboolean6 AS participantgb6,
            cs_participant.participantemail AS participantemail,
            cs_position.genericattribute1 AS positionga1,
            cs_position.genericattribute2 AS positionga2,
            cs_position.genericattribute3 AS positionga3,
            cs_position.genericattribute4 AS positionga4,
            cs_position.genericattribute5 AS positionga5,
            cs_position.genericattribute6 AS positionga6,
            cs_position.genericattribute7 AS positionga7,
            cs_position.genericattribute8 AS positionga8,
            cs_position.genericattribute9 AS positionga9,
            cs_position.genericattribute10 AS positionga10,
            cs_position.genericattribute11 AS positionga11,
            cs_position.genericattribute12 AS positionga12,
            cs_position.genericattribute13 AS positionga13,
            cs_position.genericattribute14 AS positionga14,
            cs_position.genericattribute15 AS positionga15,
            cs_position.genericattribute16 AS positionga16,
            cs_position.genericnumber1 AS positiongn1,
            cs_position.unittypeforgenericnumber1 AS positionunittypeforgn1,
            cs_position.genericnumber2 AS positiongn2,
            cs_position.unittypeforgenericnumber2 AS positionunittypeforgn2,
            cs_position.genericnumber3 AS positiongn3,
            cs_position.unittypeforgenericnumber3 AS positionunittypeforgn3,
            cs_position.genericnumber4 AS positiongn4,
            cs_position.unittypeforgenericnumber4 AS positionunittypeforgn4,
            cs_position.genericnumber5 AS positiongn5,
            cs_position.unittypeforgenericnumber5 AS positionunittypeforgn5,
            cs_position.genericnumber6 AS positiongn6,
            cs_position.unittypeforgenericnumber6 AS positionunittypeforgn6,
            cs_position.genericdate1 AS positiongd1,
            cs_position.genericdate2 AS positiongd2,
            cs_position.genericdate3 AS positiongd3,
            cs_position.genericdate4 AS positiongd4,
            cs_position.genericdate5 AS positiongd5,
            cs_position.genericdate6 AS positiongd6,
            cs_position.genericboolean1 AS positiongb1,
            cs_position.genericboolean2 AS positiongb2,
            cs_position.genericboolean3 AS positiongb3,
            cs_position.genericboolean4 AS positiongb4,
            cs_position.genericboolean5 AS positiongb5,
            cs_position.genericboolean6 AS positiongb6,
            cs_title.genericattribute1 AS titlega1,
            cs_title.genericattribute2 AS titlega2,
            cs_title.genericattribute3 AS titlega3,
            cs_title.genericattribute4 AS titlega4,
            cs_title.genericattribute5 AS titlega5,
            cs_title.genericattribute6 AS titlega6,
            cs_title.genericattribute7 AS titlega7,
            cs_title.genericattribute8 AS titlega8,
            cs_title.genericattribute9 AS titlega9,
            cs_title.genericattribute10 AS titlega10,
            cs_title.genericattribute11 AS titlega11,
            cs_title.genericattribute12 AS titlega12,
            cs_title.genericattribute13 AS titlega13,
            cs_title.genericattribute14 AS titlega14,
            cs_title.genericattribute15 AS titlega15,
            cs_title.genericattribute16 AS titlega16,
            cs_title.genericnumber1 AS titlegn1,
            cs_title.unittypeforgenericnumber1 AS titleunittypeforgn1,
            cs_title.genericnumber2 AS titlegn2,
            cs_title.unittypeforgenericnumber2 AS titleunittypeforgn2,
            cs_title.genericnumber3 AS titlegn3,
            cs_title.unittypeforgenericnumber3 AS titleunittypeforgn3,
            cs_title.genericnumber4 AS titlegn4,
            cs_title.unittypeforgenericnumber4 AS titleunittypeforgn4,
            cs_title.genericnumber5 AS titlegn5,
            cs_title.unittypeforgenericnumber5 AS titleunittypeforgn5,
            cs_title.genericnumber6 AS titlegn6,
            cs_title.unittypeforgenericnumber6 AS titleunittypeforgn6,
            cs_title.genericdate1 AS titlegd1,
            cs_title.genericdate2 AS titlegd2,
            cs_title.genericdate3 AS titlegd3,
            cs_title.genericdate4 AS titlegd4,
            cs_title.genericdate5 AS titlegd5,
            cs_title.genericdate6 AS titlegd6,
            cs_title.genericboolean1 AS titlegb1,
            cs_title.genericboolean2 AS titlegb2,
            cs_title.genericboolean3 AS titlegb3,
            cs_title.genericboolean4 AS titlegb4,
            cs_title.genericboolean5 AS titlegb5,
            cs_title.genericboolean6 AS titlegb6,
            rpt.reportgroup,
            rpt.reporttitle,
            NULL,/* --cs_businessunit.name, */ NULL,/* --cs_businessunit.mask, */  rpt.frequency,
            NULL AS positiongroupname
        FROM
            cs_participant,
            cs_payee,
            cs_position,
            cs_title,
            --cs_businessunit,
            ext.rpt_reportgroup rpt
        WHERE
            cs_participant.effectivestartdate < :vperiodcalendarrow.enddate
            AND cs_participant.effectiveenddate >= :vperiodcalendarrow.startdate
            AND :vpipelinerundate >= cs_participant.createdate
            AND :vpipelinerundate < cs_participant.removedate
            AND cs_participant.effectivestartdate
            /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PARTICIPANT' not found */
            =
            (
                /* ORIGSQL: SELECT / *+ index(par cs_participant_IND1) * / */
                SELECT   /* ORIGSQL: (SELECT MAX(effectivestartdate) FROM cs_participant par WHERE par.payeeseq = cs_(...) */
                    MAX(effectivestartdate) 
                FROM
                    cs_participant par
                WHERE
                    par.payeeseq = cs_participant.payeeseq
                    AND :vpipelinerundate >= par.createdate
                    AND :vpipelinerundate < par.removedate
                    AND par.effectivestartdate < :vperiodcalendarrow.enddate
                    AND par.effectiveenddate > :vperiodcalendarrow.startdate
            )
            AND cs_payee.payeeseq = cs_participant.payeeseq
            AND cs_payee.effectivestartdate < :vperiodcalendarrow.enddate
            AND cs_payee.effectiveenddate >= :vperiodcalendarrow.startdate
            AND :vpipelinerundate >= cs_payee.createdate
            AND :vpipelinerundate < cs_payee.removedate
            AND cs_payee.effectivestartdate
            /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PAYEE' not found */
            =
            (
                /* ORIGSQL: SELECT / *+ index(par cs_payee_IND1) * / */
                SELECT   /* ORIGSQL: (SELECT MAX(effectivestartdate) FROM cs_payee par WHERE par.payeeseq = cs_payee.(...) */
                    MAX(effectivestartdate)
                FROM
                    cs_payee par
                WHERE
                    par.payeeseq = cs_payee.payeeseq
                    AND :vpipelinerundate >= par.createdate
                    AND :vpipelinerundate < par.removedate
                    AND par.effectivestartdate < :vperiodcalendarrow.enddate
                    AND par.effectiveenddate > :vperiodcalendarrow.startdate
            )
            AND cs_position.payeeseq = cs_participant.payeeseq
            --AND cs_position.processingunitseq = cs_businessunit.processingunitseq  -- Tharani 4-03
            --AND cs_payee.businessunitmap = cs_businessunit.mask                    -- Tharani 4-03                                                                               -- Tharani 4-03
            AND :vprocessingunitseq = cs_position.processingunitseq
            AND cs_position.effectivestartdate < :vperiodcalendarrow.enddate
            AND cs_position.effectiveenddate > :vperiodcalendarrow.startdate
            AND cs_position.createdate <= :vpipelinerundate
            AND cs_position.removedate > :vpipelinerundate
            AND cs_position.effectivestartdate
            /* RESOLVE: Identifier not found: Table/view 'EXT.CS_POSITION' not found */
            =
            (
                /* ORIGSQL: SELECT / *+ index(pos cs_position_IND1) * / */
                SELECT   /* ORIGSQL: (SELECT MAX(effectivestartdate) FROM cs_position pos WHERE pos.ruleelementowners(...) */
                    MAX(effectivestartdate)
                FROM
                    cs_position pos
                WHERE
                    pos.ruleelementownerseq = cs_position.ruleelementownerseq
                    AND :vpipelinerundate >= pos.createdate
                    AND :vpipelinerundate < pos.removedate
                    AND pos.effectivestartdate < :vperiodcalendarrow.enddate
                    AND pos.effectiveenddate > :vperiodcalendarrow.startdate
                    AND :vprocessingunitseq = pos.processingunitseq
            )
            AND cs_title.ruleelementownerseq = cs_position.titleseq
            AND cs_title.effectivestartdate < :vperiodcalendarrow.enddate
            AND cs_title.effectiveenddate > :vperiodcalendarrow.startdate
            AND cs_title.removedate > :vpipelinerundate
            AND cs_title.effectivestartdate
            /* RESOLVE: Identifier not found: Table/view 'EXT.CS_TITLE' not found */
            =
            (
                /* ORIGSQL: SELECT / *+ index(pos cs_title_IND1) * / */
                SELECT   /* ORIGSQL: (SELECT MAX(effectivestartdate) FROM cs_title tit WHERE tit.ruleelementownerseq (...) */
                    MAX(effectivestartdate)
                FROM
                    cs_title tit
                WHERE
                    tit.ruleelementownerseq = cs_title.ruleelementownerseq
                    AND :vpipelinerundate < tit.removedate
                    AND tit.effectivestartdate < :vperiodcalendarrow.enddate
                    AND tit.effectiveenddate > :vperiodcalendarrow.startdate
            )
            AND cs_title.removedate  
            =
            (
                /* ORIGSQL: SELECT / *+ index(pos cs_title_IND1) * / */
                SELECT   /* ORIGSQL: (SELECT MAX(removedate) FROM cs_title tit WHERE tit.ruleelementownerseq = cs_tit(...) */
                    MAX(removedate)
                FROM
                    cs_title tit
                WHERE
                    tit.ruleelementownerseq =
                    cs_title.ruleelementownerseq
                    AND :vpipelinerundate < tit.removedate
                    AND tit.effectivestartdate < :vperiodcalendarrow.enddate
                    AND tit.effectiveenddate > :vperiodcalendarrow.startdate
            )
            AND UPPER(rpt.positiontitle) = UPPER(cs_title.name);

    /* ORIGSQL: Cursor C_Padimension_position is SELECT 'STEL', SYSDATE AS loaddttm, vpipelineru(...) */
    DECLARE CURSOR C_Padimension_position
    FOR    
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_BUSINESSUNIT' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_REPORTGROUP' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PIPELINERUN_POSITIONS' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_POSITIONGROUP' not found */

        SELECT   /* ORIGSQL: SELECT 'STEL', SYSDATE AS loaddttm, vpipelinerundate pipelinerundate, vprocessin(...) */
            'STEL',
            CURRENT_TIMESTAMP AS loaddttm,  /* ORIGSQL: SYSDATE */
            :vpipelinerundate AS pipelinerundate,
            :vprocessingunitrow.processingunitseq,
            :vprocessingunitrow.NAME AS processingunitname,
            :vperiodcalendarrow.calendarseq,
            :vcalendarrow.name AS calendarname,
            :vperiodcalendarrow.periodseq,
            :vperiodcalendarrow.name AS PERIODNAME,
            :vperiodcalendarrow.startdate AS periodstartdate,
            :vperiodcalendarrow.enddate AS periodenddate,
            cs_participant.payeeseq,
            cs_position.ruleelementownerseq AS positionseq,
            cs_position.managerseq,
            cs_payee.payeeid AS participantid,
            cs_position.NAME AS positionname,
            cs_title.NAME AS positiontitle,
            cs_participant.prefix,
            cs_participant.firstname,
            cs_participant.middlename,
            cs_participant.lastname,
            cs_participant.suffix,
            TRIM(IFNULL(cs_participant.firstname, ' ')  /* ORIGSQL: NVL(cs_participant.firstname, ' ') */
                || IFNULL(cs_participant.middlename, ' ')  /* ORIGSQL: NVL(cs_participant.middlename, ' ') */
                || IFNULL(cs_participant.lastname, ' ')) AS fullname,  /* ORIGSQL: NVL(cs_participant.lastname, ' ') */
            cs_participant.userid,
            cs_participant.taxid,
            cs_participant.salary,
            cs_participant.unittypeforsalary,
            cs_position.targetcompensation,
            cs_position.unittypefortargetcompensation,
            cs_participant.effectivestartdate AS participanteffectivestartdate,
            cs_participant.effectiveenddate AS participanteffectiveenddate,
            cs_participant.createdate AS participantcreatedate,
            cs_participant.removedate AS participantremovedate,
            cs_participant.hiredate,
            cs_participant.terminationdate,
            cs_position.creditstartdate AS positioncreditstartdate,
            cs_position.creditenddate AS positioncreditenddate,
            cs_position.processingstartdate AS positionprocessingstartdate,
            cs_position.processingenddate AS positionprocessingenddate,
            cs_position.effectivestartdate AS positioneffectivestartdate,
            cs_position.effectiveenddate AS positioneffectiveenddate,
            cs_position.createdate AS positioncreatedate,
            cs_position.removedate AS positionremovedate,
            cs_title.effectivestartdate AS titleeffectivestartdate,
            cs_title.effectiveenddate AS titleeffectiveenddate,
            cs_title.createdate AS titlecreatedate,
            cs_title.removedate AS titleremovedate,
            cs_participant.genericattribute1 AS participantga1,
            cs_participant.genericattribute2 AS participantga2,
            cs_participant.genericattribute3 AS participantga3,
            cs_participant.genericattribute4 AS participantga4,
            cs_participant.genericattribute5 AS participantga5,
            cs_participant.genericattribute6 AS participantga6,
            cs_participant.genericattribute7 AS participantga7,
            cs_participant.genericattribute8 AS participantga8,
            cs_participant.genericattribute9 AS participantga9,
            cs_participant.genericattribute10 AS participantga10,
            cs_participant.genericattribute11 AS participantga11,
            cs_participant.genericattribute12 AS participantga12,
            cs_participant.genericattribute13 AS participantga13,
            cs_participant.genericattribute14 AS participantga14,
            cs_participant.genericattribute15 AS participantga15,
            cs_participant.genericattribute16 AS participantga16,
            cs_participant.genericnumber1 AS participantgn1,
            cs_participant.unittypeforgenericnumber1 AS participantunittypeforgn1,
            cs_participant.genericnumber2 AS participantgn2,
            cs_participant.unittypeforgenericnumber2 AS participantunittypeforgn2,
            cs_participant.genericnumber3 AS participantgn3,
            cs_participant.unittypeforgenericnumber3 AS participantunittypeforgn3,
            cs_participant.genericnumber4 AS participantgn4,
            cs_participant.unittypeforgenericnumber4 AS participantunittypeforgn4,
            cs_participant.genericnumber5 AS participantgn5,
            cs_participant.unittypeforgenericnumber5 AS participantunittypeforgn5,
            cs_participant.genericnumber6 AS participantgn6,
            cs_participant.unittypeforgenericnumber6 AS participantunittypeforgn6,
            cs_participant.genericdate1 AS participantgd1,
            cs_participant.genericdate2 AS participantgd2,
            cs_participant.genericdate3 AS participantgd3,
            cs_participant.genericdate4 AS participantgd4,
            cs_participant.genericdate5 AS participantgd5,
            cs_participant.genericdate6 AS participantgd6,
            cs_participant.genericboolean1 AS participantgb1,
            cs_participant.genericboolean2 AS participantgb2,
            cs_participant.genericboolean3 AS participantgb3,
            cs_participant.genericboolean4 AS participantgb4,
            cs_participant.genericboolean5 AS participantgb5,
            cs_participant.genericboolean6 AS participantgb6,
            cs_participant.participantemail AS participantemail,
            cs_position.genericattribute1 AS positionga1,
            cs_position.genericattribute2 AS positionga2,
            cs_position.genericattribute3 AS positionga3,
            cs_position.genericattribute4 AS positionga4,
            cs_position.genericattribute5 AS positionga5,
            cs_position.genericattribute6 AS positionga6,
            cs_position.genericattribute7 AS positionga7,
            cs_position.genericattribute8 AS positionga8,
            cs_position.genericattribute9 AS positionga9,
            cs_position.genericattribute10 AS positionga10,
            cs_position.genericattribute11 AS positionga11,
            cs_position.genericattribute12 AS positionga12,
            cs_position.genericattribute13 AS positionga13,
            cs_position.genericattribute14 AS positionga14,
            cs_position.genericattribute15 AS positionga15,
            cs_position.genericattribute16 AS positionga16,
            cs_position.genericnumber1 AS positiongn1,
            cs_position.unittypeforgenericnumber1 AS positionunittypeforgn1,
            cs_position.genericnumber2 AS positiongn2,
            cs_position.unittypeforgenericnumber2 AS positionunittypeforgn2,
            cs_position.genericnumber3 AS positiongn3,
            cs_position.unittypeforgenericnumber3 AS positionunittypeforgn3,
            cs_position.genericnumber4 AS positiongn4,
            cs_position.unittypeforgenericnumber4 AS positionunittypeforgn4,
            cs_position.genericnumber5 AS positiongn5,
            cs_position.unittypeforgenericnumber5 AS positionunittypeforgn5,
            cs_position.genericnumber6 AS positiongn6,
            cs_position.unittypeforgenericnumber6 AS positionunittypeforgn6,
            cs_position.genericdate1 AS positiongd1,
            cs_position.genericdate2 AS positiongd2,
            cs_position.genericdate3 AS positiongd3,
            cs_position.genericdate4 AS positiongd4,
            cs_position.genericdate5 AS positiongd5,
            cs_position.genericdate6 AS positiongd6,
            cs_position.genericboolean1 AS positiongb1,
            cs_position.genericboolean2 AS positiongb2,
            cs_position.genericboolean3 AS positiongb3,
            cs_position.genericboolean4 AS positiongb4,
            cs_position.genericboolean5 AS positiongb5,
            cs_position.genericboolean6 AS positiongb6,
            cs_title.genericattribute1 AS titlega1,
            cs_title.genericattribute2 AS titlega2,
            cs_title.genericattribute3 AS titlega3,
            cs_title.genericattribute4 AS titlega4,
            cs_title.genericattribute5 AS titlega5,
            cs_title.genericattribute6 AS titlega6,
            cs_title.genericattribute7 AS titlega7,
            cs_title.genericattribute8 AS titlega8,
            cs_title.genericattribute9 AS titlega9,
            cs_title.genericattribute10 AS titlega10,
            cs_title.genericattribute11 AS titlega11,
            cs_title.genericattribute12 AS titlega12,
            cs_title.genericattribute13 AS titlega13,
            cs_title.genericattribute14 AS titlega14,
            cs_title.genericattribute15 AS titlega15,
            cs_title.genericattribute16 AS titlega16,
            cs_title.genericnumber1 AS titlegn1,
            cs_title.unittypeforgenericnumber1 AS titleunittypeforgn1,
            cs_title.genericnumber2 AS titlegn2,
            cs_title.unittypeforgenericnumber2 AS titleunittypeforgn2,
            cs_title.genericnumber3 AS titlegn3,
            cs_title.unittypeforgenericnumber3 AS titleunittypeforgn3,
            cs_title.genericnumber4 AS titlegn4,
            cs_title.unittypeforgenericnumber4 AS titleunittypeforgn4,
            cs_title.genericnumber5 AS titlegn5,
            cs_title.unittypeforgenericnumber5 AS titleunittypeforgn5,
            cs_title.genericnumber6 AS titlegn6,
            cs_title.unittypeforgenericnumber6 AS titleunittypeforgn6,
            cs_title.genericdate1 AS titlegd1,
            cs_title.genericdate2 AS titlegd2,
            cs_title.genericdate3 AS titlegd3,
            cs_title.genericdate4 AS titlegd4,
            cs_title.genericdate5 AS titlegd5,
            cs_title.genericdate6 AS titlegd6,
            cs_title.genericboolean1 AS titlegb1,
            cs_title.genericboolean2 AS titlegb2,
            cs_title.genericboolean3 AS titlegb3,
            cs_title.genericboolean4 AS titlegb4,
            cs_title.genericboolean5 AS titlegb5,
            cs_title.genericboolean6 AS titlegb6,
            rpt.reportgroup,
            rpt.reporttitle,
            cs_businessunit.name,
            cs_businessunit.mask,
            rpt.frequency,
            CS_PositionGroup.name AS positiongroupname
        FROM
            cs_participant,
            cs_payee,
            cs_position,
            cs_title,
            cs_businessunit,
            rpt_reportgroup rpt,
            CS_PipelineRun_Positions,
            CS_PositionGroup
        WHERE
            cs_participant.effectivestartdate < :vperiodcalendarrow.enddate
            AND cs_participant.effectiveenddate >= :vperiodcalendarrow.startdate
            AND :vpipelinerundate >= cs_participant.createdate
            AND :vpipelinerundate < cs_participant.removedate
            AND cs_participant.effectivestartdate  
            =
            (
                /* ORIGSQL: SELECT / *+ index(par cs_participant_IND1) * / */
                SELECT   /* ORIGSQL: (SELECT MAX(effectivestartdate) FROM cs_participant par WHERE par.payeeseq = cs_(...) */
                    MAX(effectivestartdate)
                FROM
                    cs_participant par
                WHERE
                    par.payeeseq = cs_participant.payeeseq
                    AND :vpipelinerundate >= par.createdate
                    AND :vpipelinerundate < par.removedate
                    AND par.effectivestartdate < :vperiodcalendarrow.enddate
                    AND par.effectiveenddate > :vperiodcalendarrow.startdate
            )
            AND cs_payee.payeeseq = cs_participant.payeeseq
            AND cs_payee.effectivestartdate < :vperiodcalendarrow.enddate
            AND cs_payee.effectiveenddate >= :vperiodcalendarrow.startdate
            AND :vpipelinerundate >= cs_payee.createdate
            AND :vpipelinerundate < cs_payee.removedate
            AND cs_payee.effectivestartdate  
            =
            (
                /* ORIGSQL: SELECT / *+ index(par cs_payee_IND1) * / */
                SELECT   /* ORIGSQL: (SELECT MAX(effectivestartdate) FROM cs_payee par WHERE par.payeeseq = cs_payee.(...) */
                    MAX(effectivestartdate)
                FROM
                    cs_payee par
                WHERE
                    par.payeeseq = cs_payee.payeeseq
                    AND :vpipelinerundate >= par.createdate
                    AND :vpipelinerundate < par.removedate
                    AND par.effectivestartdate < :vperiodcalendarrow.enddate
                    AND par.effectiveenddate > :vperiodcalendarrow.startdate
            )
            AND cs_position.payeeseq = cs_participant.payeeseq
            --AND cs_position.RuleElementOwnerSeq = CS_PipelineRun_Positions.PositionSeq  -- Tharani 07-20
            AND CS_PipelineRun_Positions.PipelineRunSeq = :vpipelinerunseq -- Tharani 07-20
            --AND CS_PositionGroup.PositionGroupSeq  = cs_position.PositionGroupSeq       -- Tharani 07-20
            AND :vpipelinerundate < CS_PositionGroup.RemoveDate -- Tharani 07-20
            AND CS_PositionGroup.businessunitmap = cs_businessunit.mask -- Tharani 07-20
            AND cs_position.processingunitseq = cs_businessunit.processingunitseq -- Tharani 4-03
            AND cs_payee.businessunitmap = cs_businessunit.mask -- Tharani 4-03                                                                               -- Tharani 4-03
            AND :vprocessingunitseq = cs_position.processingunitseq
            AND cs_position.effectivestartdate < :vperiodcalendarrow.enddate
            AND cs_position.effectiveenddate > :vperiodcalendarrow.startdate
            AND cs_position.createdate <= :vpipelinerundate
            AND cs_position.removedate > :vpipelinerundate
            AND cs_position.effectivestartdate  
            =
            (
                /* ORIGSQL: SELECT / *+ index(pos cs_position_IND1) * / */
                SELECT   /* ORIGSQL: (SELECT MAX(effectivestartdate) FROM cs_position pos WHERE pos.ruleelementowners(...) */
                    MAX(effectivestartdate)
                FROM
                    cs_position pos
                WHERE
                    pos.ruleelementownerseq = cs_position.ruleelementownerseq
                    AND :vpipelinerundate >= pos.createdate
                    AND :vpipelinerundate < pos.removedate
                    AND pos.effectivestartdate < :vperiodcalendarrow.enddate
                    AND pos.effectiveenddate > :vperiodcalendarrow.startdate
                    AND :vprocessingunitseq = pos.processingunitseq
            )
            AND cs_title.ruleelementownerseq = cs_position.titleseq
            AND cs_title.effectivestartdate < :vperiodcalendarrow.enddate
            AND cs_title.effectiveenddate > :vperiodcalendarrow.startdate
            AND cs_title.removedate > :vpipelinerundate
            AND cs_title.effectivestartdate  
            =
            (
                /* ORIGSQL: SELECT / *+ index(pos cs_title_IND1) * / */
                SELECT   /* ORIGSQL: (SELECT MAX(effectivestartdate) FROM cs_title tit WHERE tit.ruleelementownerseq (...) */
                    MAX(effectivestartdate)
                FROM
                    cs_title tit
                WHERE
                    tit.ruleelementownerseq = cs_title.ruleelementownerseq
                    AND :vpipelinerundate < tit.removedate
                    AND tit.effectivestartdate < :vperiodcalendarrow.enddate
                    AND tit.effectiveenddate > :vperiodcalendarrow.startdate
            )
            AND cs_title.removedate  
            =
            (
                /* ORIGSQL: SELECT / *+ index(pos cs_title_IND1) * / */
                SELECT   /* ORIGSQL: (SELECT MAX(removedate) FROM cs_title tit WHERE tit.ruleelementownerseq = cs_tit(...) */
                    MAX(removedate)
                FROM
                    cs_title tit
                WHERE
                    tit.ruleelementownerseq =
                    cs_title.ruleelementownerseq
                    AND :vpipelinerundate < tit.removedate
                    AND tit.effectivestartdate < :vperiodcalendarrow.enddate
                    AND tit.effectiveenddate > :vperiodcalendarrow.startdate
            );

    -- DECLARE vPaDimension EXT.PRC_BASE_PADIMENSION__padimType;  /* ORIGSQL: vPaDimension padimType; *//*Deepan: Not required , since bulk collect cursor not used*/

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            vsqlerrm = SUBSTRING(::SQL_ERROR_MESSAGE,1,3900);  /* ORIGSQL: SUBSTR(SQLERRM, 1, 3900) */

            /* ORIGSQL: prc_logevent(:vperiodcalendarrow.name,vprocname,'ERROR',NULL,vsqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vperiodcalendarrow.name, :vprocname, 'ERROR', NULL, :vsqlerrm);

            /* ORIGSQL: raise_application_error(-20911,'Error raised: '||vprocname||' Failed: '|| DBMS_U(...) */
            -- sapdbmtk: mapped error code -20911 => 10911: (ABS(-20911)%10000)+10000
            SIGNAL SQL_ERROR_CODE 10911 SET MESSAGE_TEXT = 'Error raised: '||IFNULL(:vprocname,'')||' Failed: '
            -- ||

            -- DBMS_UTILITY.FORMAT_ERROR_BACKTRACE

            || ' - '||IFNULL(:vsqlerrm,'');  /* RESOLVE: Standard Package call(not converted): 'DBMS_UTILITY.FORMAT_ERROR_BACKTRACE' not supported, manual conversion required */
        END;

        --AND UPPER(rpt.positiontitle) = UPPER(cs_title.name);

        /*--- TYPE definition moved to  scripts/dbmtk_create_types.sqlscript ---
        ----- Converted type 'padimType' to 'EXT.PRC_BASE_PADIMENSION__padimType'
        TYPE padimType IS TABLE OF EXT.RPT_BASE_PADIMENSION%ROWTYPE INDEX BY PLS_INTEGER;
        ---end of TYPE definition commented out---*/ 
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PROCESSINGUNIT' not found */

        SELECT *
        INTO
            vProcessingUnitRow
        FROM
            CS_PROCESSINGUNIT
        WHERE
            cs_processingunit.processingunitseq = :vprocessingunitseq;

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIOD' not found */

        SELECT *
        INTO
            vperiodcalendarrow
        FROM
            cs_period
        WHERE
            cs_period.calendarseq = :vcalendarseq
            AND cs_period.periodseq = :vperiodseq
        LIMIT 1;  /* ORIGSQL: ROWNUM = 1 */

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_CALENDAR' not found */

        SELECT *
        INTO
            vCalendarRow
        FROM
            CS_CALENDAR
        WHERE
            CS_CALENDAR.calendarseq = :vcalendarseq
        LIMIT 1;  /* ORIGSQL: ROWNUM = 1 */

        /*
        SELECT TO_DATE (TO_CHAR (MAX(pr.starttime), 'YYYY-MON-DD HH24:MI:SS'),
                        'YYYY-MON-DD HH24:MI:SS'
                   ) AS pipelinerundate
          INTO vpipelinerundate
          FROM cs_pipelinerun pr, cs_stagesummary ss, cs_stagetype st
         WHERE
           pr.pipelineRunSeq = ss.pipelineRunSeq
         AND ss.stagetypeseq = st.stagetypeseq
         AND pr.processingunitseq = vprocessingunitseq
         AND pr.periodseq = vperiodseq
         AND st.NAME = 'Allocate'
         AND ss.isactive = 1;
         */
        /* Changed for Position Group */ -- Tharani 07-20
        /*SELECT pipelinerundate, runmode, pipelinerunseq
          INTO vpipelinerundate, vrunmode, vpipelinerunseq
        FROM
            (SELECT TO_DATE (TO_CHAR (pr.starttime, 'YYYY-MON-DD HH24:MI:SS'),
                          'YYYY-MON-DD HH24:MI:SS'
                     ) AS pipelinerundate, runmode, pr.pipelinerunseq
             FROM cs_pipelinerun pr, cs_stagesummary ss, cs_stagetype st
            WHERE
              pr.pipelineRunSeq = ss.pipelineRunSeq
             AND ss.stagetypeseq = st.stagetypeseq
             AND pr.processingunitseq = vprocessingunitseq
             AND pr.periodseq = vperiodseq
             AND st.NAME = 'Allocate'
             AND ss.isactive = 1
            order by pr.pipelinerunseq desc
        )
        where rownum = 1;
        */

        /* ORIGSQL: Execute IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION ENABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION ENABLE PARALLEL DML ; */
        /* ORIGSQL: prc_logevent (:vperiodcalendarrow.name, vProcName, 'Padimesion table insert start(...) */
        CALL EXT.PRC_LOGEVENT(:vperiodcalendarrow.name, :vprocname, 'Padimesion table insert start', NULL, :vsqlerrm);

        --IF vrunmode = 'full' then
        vpipelinerundate = CURRENT_TIMESTAMP;  /* ORIGSQL: sysdate */
        /*
              OPEN C_Padimension;
              LOOP
                BEGIN
                  FETCH C_Padimension BULK COLLECT INTO vPaDimension limit 10000;
                  FORALL i in 1..vPaDimension.count
                  INSERT INTO RPT_BASE_PADIMENSION NOLOGGING VALUES vPaDimension(i);
                  COMMIT;
                  EXIT WHEN C_Padimension%NOTFOUND;
                END;
              END LOOP;
              CLOSE C_Padimension;*/

        /* ORIGSQL: INSERT / *+ APPEND PARALLEL * / */
        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_BASE_PADIMENSION' not found */

        /* ORIGSQL: INSERT INTO RPT_BASE_PADIMENSION NOLOGGING SELECT 'STEL', SYSDATE AS loaddttm, v(...) */
        INSERT INTO RPT_BASE_PADIMENSION
            -- NOLOGGING
            /* ORIGSQL: SELECT / *+ leading(cs_position) * / */
            SELECT   /* ORIGSQL: SELECT 'STEL', SYSDATE AS loaddttm, vpipelinerundate pipelinerundate, vprocessin(...) */
                'STEL',
                CURRENT_TIMESTAMP AS loaddttm,  /* ORIGSQL: SYSDATE */
                :vpipelinerundate AS pipelinerundate,
                :vprocessingunitrow.processingunitseq,
                :vprocessingunitrow.NAME AS processingunitname,
                :vperiodcalendarrow.calendarseq,
                :vcalendarrow.name AS calendarname,
                :vperiodcalendarrow.periodseq,
                :vperiodcalendarrow.name AS PERIODNAME,
                :vperiodcalendarrow.startdate AS periodstartdate,
                :vperiodcalendarrow.enddate AS periodenddate,
                cs_participant.payeeseq,
                cs_position.ruleelementownerseq AS positionseq,
                cs_position.managerseq,
                cs_payee.payeeid AS participantid,
                cs_position.NAME AS positionname,
                cs_title.NAME AS positiontitle,
                cs_participant.prefix,
                cs_participant.firstname,
                cs_participant.middlename,
                cs_participant.lastname,
                cs_participant.suffix,
                TRIM(IFNULL(cs_participant.firstname, ' ')  /* ORIGSQL: NVL(cs_participant.firstname, ' ') */
                    || IFNULL(cs_participant.middlename, ' ')  /* ORIGSQL: NVL(cs_participant.middlename, ' ') */
                    || IFNULL(cs_participant.lastname, ' ')) AS fullname,  /* ORIGSQL: NVL(cs_participant.lastname, ' ') */
                cs_participant.userid,
                cs_participant.taxid,
                cs_participant.salary,
                cs_participant.unittypeforsalary,
                cs_position.targetcompensation,
                cs_position.unittypefortargetcompensation,
                cs_participant.effectivestartdate AS participanteffectivestartdate,
                cs_participant.effectiveenddate AS participanteffectiveenddate,
                cs_participant.createdate AS participantcreatedate,
                cs_participant.removedate AS participantremovedate,
                cs_participant.hiredate,
                cs_participant.terminationdate,
                cs_position.creditstartdate AS positioncreditstartdate,
                cs_position.creditenddate AS positioncreditenddate,
                cs_position.processingstartdate AS positionprocessingstartdate,
                cs_position.processingenddate AS positionprocessingenddate,
                cs_position.effectivestartdate AS positioneffectivestartdate,
                cs_position.effectiveenddate AS positioneffectiveenddate,
                cs_position.createdate AS positioncreatedate,
                cs_position.removedate AS positionremovedate,
                cs_title.effectivestartdate AS titleeffectivestartdate,
                cs_title.effectiveenddate AS titleeffectiveenddate,
                cs_title.createdate AS titlecreatedate,
                cs_title.removedate AS titleremovedate,
                cs_participant.genericattribute1 AS participantga1,
                cs_participant.genericattribute2 AS participantga2,
                cs_participant.genericattribute3 AS participantga3,
                cs_participant.genericattribute4 AS participantga4,
                cs_participant.genericattribute5 AS participantga5,
                cs_participant.genericattribute6 AS participantga6,
                cs_participant.genericattribute7 AS participantga7,
                cs_participant.genericattribute8 AS participantga8,
                cs_participant.genericattribute9 AS participantga9,
                cs_participant.genericattribute10 AS participantga10,
                cs_participant.genericattribute11 AS participantga11,
                cs_participant.genericattribute12 AS participantga12,
                cs_participant.genericattribute13 AS participantga13,
                cs_participant.genericattribute14 AS participantga14,
                cs_participant.genericattribute15 AS participantga15,
                cs_participant.genericattribute16 AS participantga16,
                cs_participant.genericnumber1 AS participantgn1,
                cs_participant.unittypeforgenericnumber1 AS participantunittypeforgn1,
                cs_participant.genericnumber2 AS participantgn2,
                cs_participant.unittypeforgenericnumber2 AS participantunittypeforgn2,
                cs_participant.genericnumber3 AS participantgn3,
                cs_participant.unittypeforgenericnumber3 AS participantunittypeforgn3,
                cs_participant.genericnumber4 AS participantgn4,
                cs_participant.unittypeforgenericnumber4 AS participantunittypeforgn4,
                cs_participant.genericnumber5 AS participantgn5,
                cs_participant.unittypeforgenericnumber5 AS participantunittypeforgn5,
                cs_participant.genericnumber6 AS participantgn6,
                cs_participant.unittypeforgenericnumber6 AS participantunittypeforgn6,
                cs_participant.genericdate1 AS participantgd1,
                cs_participant.genericdate2 AS participantgd2,
                cs_participant.genericdate3 AS participantgd3,
                cs_participant.genericdate4 AS participantgd4,
                cs_participant.genericdate5 AS participantgd5,
                cs_participant.genericdate6 AS participantgd6,
                cs_participant.genericboolean1 AS participantgb1,
                cs_participant.genericboolean2 AS participantgb2,
                cs_participant.genericboolean3 AS participantgb3,
                cs_participant.genericboolean4 AS participantgb4,
                cs_participant.genericboolean5 AS participantgb5,
                cs_participant.genericboolean6 AS participantgb6,
                cs_participant.participantemail AS participantemail,
                cs_position.genericattribute1 AS positionga1,
                cs_position.genericattribute2 AS positionga2,
                cs_position.genericattribute3 AS positionga3,
                cs_position.genericattribute4 AS positionga4,
                cs_position.genericattribute5 AS positionga5,
                cs_position.genericattribute6 AS positionga6,
                cs_position.genericattribute7 AS positionga7,
                cs_position.genericattribute8 AS positionga8,
                cs_position.genericattribute9 AS positionga9,
                cs_position.genericattribute10 AS positionga10,
                cs_position.genericattribute11 AS positionga11,
                cs_position.genericattribute12 AS positionga12,
                cs_position.genericattribute13 AS positionga13,
                cs_position.genericattribute14 AS positionga14,
                cs_position.genericattribute15 AS positionga15,
                cs_position.genericattribute16 AS positionga16,
                cs_position.genericnumber1 AS positiongn1,
                cs_position.unittypeforgenericnumber1 AS positionunittypeforgn1,
                cs_position.genericnumber2 AS positiongn2,
                cs_position.unittypeforgenericnumber2 AS positionunittypeforgn2,
                cs_position.genericnumber3 AS positiongn3,
                cs_position.unittypeforgenericnumber3 AS positionunittypeforgn3,
                cs_position.genericnumber4 AS positiongn4,
                cs_position.unittypeforgenericnumber4 AS positionunittypeforgn4,
                cs_position.genericnumber5 AS positiongn5,
                cs_position.unittypeforgenericnumber5 AS positionunittypeforgn5,
                cs_position.genericnumber6 AS positiongn6,
                cs_position.unittypeforgenericnumber6 AS positionunittypeforgn6,
                cs_position.genericdate1 AS positiongd1,
                cs_position.genericdate2 AS positiongd2,
                cs_position.genericdate3 AS positiongd3,
                cs_position.genericdate4 AS positiongd4,
                cs_position.genericdate5 AS positiongd5,
                cs_position.genericdate6 AS positiongd6,
                cs_position.genericboolean1 AS positiongb1,
                cs_position.genericboolean2 AS positiongb2,
                cs_position.genericboolean3 AS positiongb3,
                cs_position.genericboolean4 AS positiongb4,
                cs_position.genericboolean5 AS positiongb5,
                cs_position.genericboolean6 AS positiongb6,
                cs_title.genericattribute1 AS titlega1,
                cs_title.genericattribute2 AS titlega2,
                cs_title.genericattribute3 AS titlega3,
                cs_title.genericattribute4 AS titlega4,
                cs_title.genericattribute5 AS titlega5,
                cs_title.genericattribute6 AS titlega6,
                cs_title.genericattribute7 AS titlega7,
                cs_title.genericattribute8 AS titlega8,
                cs_title.genericattribute9 AS titlega9,
                cs_title.genericattribute10 AS titlega10,
                cs_title.genericattribute11 AS titlega11,
                cs_title.genericattribute12 AS titlega12,
                cs_title.genericattribute13 AS titlega13,
                cs_title.genericattribute14 AS titlega14,
                cs_title.genericattribute15 AS titlega15,
                cs_title.genericattribute16 AS titlega16,
                cs_title.genericnumber1 AS titlegn1,
                cs_title.unittypeforgenericnumber1 AS titleunittypeforgn1,
                cs_title.genericnumber2 AS titlegn2,
                cs_title.unittypeforgenericnumber2 AS titleunittypeforgn2,
                cs_title.genericnumber3 AS titlegn3,
                cs_title.unittypeforgenericnumber3 AS titleunittypeforgn3,
                cs_title.genericnumber4 AS titlegn4,
                cs_title.unittypeforgenericnumber4 AS titleunittypeforgn4,
                cs_title.genericnumber5 AS titlegn5,
                cs_title.unittypeforgenericnumber5 AS titleunittypeforgn5,
                cs_title.genericnumber6 AS titlegn6,
                cs_title.unittypeforgenericnumber6 AS titleunittypeforgn6,
                cs_title.genericdate1 AS titlegd1,
                cs_title.genericdate2 AS titlegd2,
                cs_title.genericdate3 AS titlegd3,
                cs_title.genericdate4 AS titlegd4,
                cs_title.genericdate5 AS titlegd5,
                cs_title.genericdate6 AS titlegd6,
                cs_title.genericboolean1 AS titlegb1,
                cs_title.genericboolean2 AS titlegb2,
                cs_title.genericboolean3 AS titlegb3,
                cs_title.genericboolean4 AS titlegb4,
                cs_title.genericboolean5 AS titlegb5,
                cs_title.genericboolean6 AS titlegb6,
                rpt.reportgroup,
                rpt.reporttitle,
                NULL,/* --cs_businessunit.name, */ NULL,/* --cs_businessunit.mask, */  rpt.frequency,
                NULL AS positiongroupname
            FROM
                cs_participant,
                cs_payee,
                cs_position,
                cs_title,
                --cs_businessunit,
                ext.rpt_reportgroup rpt
            WHERE
                cs_participant.effectivestartdate < :vperiodcalendarrow.enddate
                AND cs_participant.effectiveenddate >= :vperiodcalendarrow.startdate
                AND :vpipelinerundate >= cs_participant.createdate
                AND :vpipelinerundate < cs_participant.removedate
                AND cs_participant.effectivestartdate =
                (
                    /* ORIGSQL: SELECT / *+ index(par cs_participant_IND1) * / */
                    SELECT   /* ORIGSQL: (SELECT MAX(effectivestartdate) FROM cs_participant par WHERE par.payeeseq = cs_(...) */
                        MAX(effectivestartdate) 
                    FROM
                        cs_participant par
                    WHERE
                        par.payeeseq = cs_participant.payeeseq
                        AND :vpipelinerundate >= par.createdate
                        AND :vpipelinerundate < par.removedate
                        AND par.effectivestartdate < :vperiodcalendarrow.enddate
                        AND par.effectiveenddate > :vperiodcalendarrow.startdate
                )
                AND cs_payee.payeeseq = cs_participant.payeeseq
                AND cs_payee.effectivestartdate < :vperiodcalendarrow.enddate
                AND cs_payee.effectiveenddate >= :vperiodcalendarrow.startdate
                AND :vpipelinerundate >= cs_payee.createdate
                AND :vpipelinerundate < cs_payee.removedate
                AND cs_payee.effectivestartdate =
                (
                    /* ORIGSQL: SELECT / *+ index(par cs_payee_IND1) * / */
                    SELECT   /* ORIGSQL: (SELECT MAX(effectivestartdate) FROM cs_payee par WHERE par.payeeseq = cs_payee.(...) */
                        MAX(effectivestartdate)
                    FROM
                        cs_payee par
                    WHERE
                        par.payeeseq = cs_payee.payeeseq
                        AND :vpipelinerundate >= par.createdate
                        AND :vpipelinerundate < par.removedate
                        AND par.effectivestartdate < :vperiodcalendarrow.enddate
                        AND par.effectiveenddate > :vperiodcalendarrow.startdate
                )
                AND cs_position.payeeseq = cs_participant.payeeseq
                --AND cs_position.processingunitseq = cs_businessunit.processingunitseq  -- Tharani 4-03
                --AND cs_payee.businessunitmap = cs_businessunit.mask                    -- Tharani 4-03                                                                               -- Tharani 4-03
                AND :vprocessingunitseq = cs_position.processingunitseq
                AND cs_position.effectivestartdate < :vperiodcalendarrow.enddate
                AND cs_position.effectiveenddate > :vperiodcalendarrow.startdate
                AND cs_position.createdate <= :vpipelinerundate
                AND cs_position.removedate > :vpipelinerundate
                AND cs_position.effectivestartdate =
                (
                    /* ORIGSQL: SELECT / *+ index(pos cs_position_IND1) * / */
                    SELECT   /* ORIGSQL: (SELECT MAX(effectivestartdate) FROM cs_position pos WHERE pos.ruleelementowners(...) */
                        MAX(effectivestartdate)
                    FROM
                        cs_position pos
                    WHERE
                        pos.ruleelementownerseq = cs_position.ruleelementownerseq
                        AND :vpipelinerundate >= pos.createdate
                        AND :vpipelinerundate < pos.removedate
                        AND pos.effectivestartdate < :vperiodcalendarrow.enddate
                        AND pos.effectiveenddate > :vperiodcalendarrow.startdate
                        AND :vprocessingunitseq = pos.processingunitseq
                )
                AND cs_title.ruleelementownerseq = cs_position.titleseq
                AND cs_title.effectivestartdate < :vperiodcalendarrow.enddate
                AND cs_title.effectiveenddate > :vperiodcalendarrow.startdate
                AND cs_title.removedate > :vpipelinerundate
                AND cs_title.effectivestartdate =
                (
                    /* ORIGSQL: SELECT / *+ index(pos cs_title_IND1) * / */
                    SELECT   /* ORIGSQL: (SELECT MAX(effectivestartdate) FROM cs_title tit WHERE tit.ruleelementownerseq (...) */
                        MAX(effectivestartdate)
                    FROM
                        cs_title tit
                    WHERE
                        tit.ruleelementownerseq = cs_title.ruleelementownerseq
                        AND :vpipelinerundate < tit.removedate
                        AND tit.effectivestartdate < :vperiodcalendarrow.enddate
                        AND tit.effectiveenddate > :vperiodcalendarrow.startdate
                )
                AND cs_title.removedate =
                (
                    /* ORIGSQL: SELECT / *+ index(pos cs_title_IND1) * / */
                    SELECT   /* ORIGSQL: (SELECT MAX(removedate) FROM cs_title tit WHERE tit.ruleelementownerseq = cs_tit(...) */
                        MAX(removedate)
                    FROM
                        cs_title tit
                    WHERE
                        tit.ruleelementownerseq =
                        cs_title.ruleelementownerseq
                        AND :vpipelinerundate < tit.removedate
                        AND tit.effectivestartdate < :vperiodcalendarrow.enddate
                        AND tit.effectiveenddate > :vperiodcalendarrow.startdate
                )
                AND UPPER(rpt.positiontitle) = UPPER(cs_title.name);

        /* ORIGSQL: prc_logevent (:vperiodcalendarrow.name, vProcName, 'Padimesion table insert end',(...) */
        CALL EXT.PRC_LOGEVENT(:vperiodcalendarrow.name, :vprocname, 'Padimesion table insert end', NULL, :vsqlerrm);

        /*
          Elsif vrunmode = 'positions' then
            OPEN C_Padimension_position;
            LOOP
              BEGIN
                FETCH C_Padimension_position BULK COLLECT INTO vPaDimension limit 10000;
                FORALL i in 1..vPaDimension.count
                INSERT /*+ APPEND PARALLEL */ /*INTO RPT_BASE_PADIMENSION NOLOGGING VALUES vPaDimension(i);
                  COMMIT;
                  EXIT WHEN C_Padimension_position%NOTFOUND;
                END;
              END LOOP;
              CLOSE C_Padimension_position;
            End If;
            */

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END