--------------------------------------------------------
--  DDL for Procedure PRC_BASE_PADIMENSION
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "PRC_BASE_PADIMENSION" (
    vperiodseq           IN   cs_period.periodseq%TYPE,
    vprocessingunitseq   IN   cs_processingunit.processingunitseq%TYPE,
    vcalendarseq         IN   cs_period.calendarseq%TYPE
 )
 IS
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
    vprocname              VARCHAR2 (30) := UPPER ('PRC_BASE_PADIMENSION');
    vsqlerrm               VARCHAR2 (3900);
    vprocessingunitrow     cs_processingunit%ROWTYPE;
    vperiodcalendarrow     cs_period%ROWTYPE;
    vpipelinerundate       cs_credit.pipelinerundate%TYPE;
    vrunmode               cs_pipelinerun.runmode%TYPE;
    vpipelinerunseq        cs_pipelinerun.pipelinerunseq%TYPE;
    vcalendarrow           cs_calendar%ROWTYPE;
    Cursor C_Padimension is
          SELECT
               'STEL',
               SYSDATE AS loaddttm,
               vpipelinerundate pipelinerundate,
               vprocessingunitrow.processingunitseq,
               vprocessingunitrow.NAME processingunitname,
               vperiodcalendarrow.calendarseq,
               vcalendarrow.name calendarname,
               vperiodcalendarrow.periodseq, vperiodcalendarrow.name PERIODNAME,
               vperiodcalendarrow.startdate periodstartdate,
               vperiodcalendarrow.enddate periodenddate,
               cs_participant.payeeseq,
               cs_position.ruleelementownerseq positionseq,
               cs_position.managerseq, cs_payee.payeeid participantid,
               cs_position.NAME positionname, cs_title.NAME positiontitle,
               cs_participant.prefix, cs_participant.firstname,
               cs_participant.middlename, cs_participant.lastname,
               cs_participant.suffix,
               TRIM (   NVL (cs_participant.firstname, ' ')
                     || NVL (cs_participant.middlename, ' ')
                     || NVL (cs_participant.lastname, ' ')
                    ) fullname,
               cs_participant.userid, cs_participant.taxid,
               cs_participant.salary, cs_participant.unittypeforsalary,
               cs_position.targetcompensation,
               cs_position.unittypefortargetcompensation,
               cs_participant.effectivestartdate participanteffectivestartdate,
               cs_participant.effectiveenddate participanteffectiveenddate,
               cs_participant.createdate participantcreatedate,
               cs_participant.removedate participantremovedate,
               cs_participant.hiredate, cs_participant.terminationdate,
               cs_position.creditstartdate positioncreditstartdate,
               cs_position.creditenddate positioncreditenddate,
               cs_position.processingstartdate positionprocessingstartdate,
               cs_position.processingenddate positionprocessingenddate,
               cs_position.effectivestartdate positioneffectivestartdate,
               cs_position.effectiveenddate positioneffectiveenddate,
               cs_position.createdate positioncreatedate,
               cs_position.removedate positionremovedate,
               cs_title.effectivestartdate titleeffectivestartdate,
               cs_title.effectiveenddate titleeffectiveenddate,
               cs_title.createdate titlecreatedate,
               cs_title.removedate titleremovedate,
               cs_participant.genericattribute1 participantga1,
               cs_participant.genericattribute2 participantga2,
               cs_participant.genericattribute3 participantga3,
               cs_participant.genericattribute4 participantga4,
               cs_participant.genericattribute5 participantga5,
               cs_participant.genericattribute6 participantga6,
               cs_participant.genericattribute7 participantga7,
               cs_participant.genericattribute8 participantga8,
               cs_participant.genericattribute9 participantga9,
               cs_participant.genericattribute10 participantga10,
               cs_participant.genericattribute11 participantga11,
               cs_participant.genericattribute12 participantga12,
               cs_participant.genericattribute13 participantga13,
               cs_participant.genericattribute14 participantga14,
               cs_participant.genericattribute15 participantga15,
               cs_participant.genericattribute16 participantga16,
               cs_participant.genericnumber1 participantgn1,
               cs_participant.unittypeforgenericnumber1
                                                     participantunittypeforgn1,
               cs_participant.genericnumber2 participantgn2,
               cs_participant.unittypeforgenericnumber2
                                                     participantunittypeforgn2,
               cs_participant.genericnumber3 participantgn3,
               cs_participant.unittypeforgenericnumber3
                                                     participantunittypeforgn3,
               cs_participant.genericnumber4 participantgn4,
               cs_participant.unittypeforgenericnumber4
                                                     participantunittypeforgn4,
               cs_participant.genericnumber5 participantgn5,
               cs_participant.unittypeforgenericnumber5
                                                     participantunittypeforgn5,
               cs_participant.genericnumber6 participantgn6,
               cs_participant.unittypeforgenericnumber6
                                                     participantunittypeforgn6,
               cs_participant.genericdate1 participantgd1,
               cs_participant.genericdate2 participantgd2,
               cs_participant.genericdate3 participantgd3,
               cs_participant.genericdate4 participantgd4,
               cs_participant.genericdate5 participantgd5,
               cs_participant.genericdate6 participantgd6,
               cs_participant.genericboolean1 participantgb1,
               cs_participant.genericboolean2 participantgb2,
               cs_participant.genericboolean3 participantgb3,
               cs_participant.genericboolean4 participantgb4,
               cs_participant.genericboolean5 participantgb5,
               cs_participant.genericboolean6 participantgb6,
               cs_participant.participantemail participantemail,
               cs_position.genericattribute1 positionga1,
               cs_position.genericattribute2 positionga2,
               cs_position.genericattribute3 positionga3,
               cs_position.genericattribute4 positionga4,
               cs_position.genericattribute5 positionga5,
               cs_position.genericattribute6 positionga6,
               cs_position.genericattribute7 positionga7,
               cs_position.genericattribute8 positionga8,
               cs_position.genericattribute9 positionga9,
               cs_position.genericattribute10 positionga10,
               cs_position.genericattribute11 positionga11,
               cs_position.genericattribute12 positionga12,
               cs_position.genericattribute13 positionga13,
               cs_position.genericattribute14 positionga14,
               cs_position.genericattribute15 positionga15,
               cs_position.genericattribute16 positionga16,
               cs_position.genericnumber1 positiongn1,
               cs_position.unittypeforgenericnumber1 positionunittypeforgn1,
               cs_position.genericnumber2 positiongn2,
               cs_position.unittypeforgenericnumber2 positionunittypeforgn2,
               cs_position.genericnumber3 positiongn3,
               cs_position.unittypeforgenericnumber3 positionunittypeforgn3,
               cs_position.genericnumber4 positiongn4,
               cs_position.unittypeforgenericnumber4 positionunittypeforgn4,
               cs_position.genericnumber5 positiongn5,
               cs_position.unittypeforgenericnumber5 positionunittypeforgn5,
               cs_position.genericnumber6 positiongn6,
               cs_position.unittypeforgenericnumber6 positionunittypeforgn6,
               cs_position.genericdate1 positiongd1,
               cs_position.genericdate2 positiongd2,
               cs_position.genericdate3 positiongd3,
               cs_position.genericdate4 positiongd4,
               cs_position.genericdate5 positiongd5,
               cs_position.genericdate6 positiongd6,
               cs_position.genericboolean1 positiongb1,
               cs_position.genericboolean2 positiongb2,
               cs_position.genericboolean3 positiongb3,
               cs_position.genericboolean4 positiongb4,
               cs_position.genericboolean5 positiongb5,
               cs_position.genericboolean6 positiongb6,
               cs_title.genericattribute1 titlega1,
               cs_title.genericattribute2 titlega2,
               cs_title.genericattribute3 titlega3,
               cs_title.genericattribute4 titlega4,
               cs_title.genericattribute5 titlega5,
               cs_title.genericattribute6 titlega6,
               cs_title.genericattribute7 titlega7,
               cs_title.genericattribute8 titlega8,
               cs_title.genericattribute9 titlega9,
               cs_title.genericattribute10 titlega10,
               cs_title.genericattribute11 titlega11,
               cs_title.genericattribute12 titlega12,
               cs_title.genericattribute13 titlega13,
               cs_title.genericattribute14 titlega14,
               cs_title.genericattribute15 titlega15,
               cs_title.genericattribute16 titlega16,
               cs_title.genericnumber1 titlegn1,
               cs_title.unittypeforgenericnumber1 titleunittypeforgn1,
               cs_title.genericnumber2 titlegn2,
               cs_title.unittypeforgenericnumber2 titleunittypeforgn2,
               cs_title.genericnumber3 titlegn3,
               cs_title.unittypeforgenericnumber3 titleunittypeforgn3,
               cs_title.genericnumber4 titlegn4,
               cs_title.unittypeforgenericnumber4 titleunittypeforgn4,
               cs_title.genericnumber5 titlegn5,
               cs_title.unittypeforgenericnumber5 titleunittypeforgn5,
               cs_title.genericnumber6 titlegn6,
               cs_title.unittypeforgenericnumber6 titleunittypeforgn6,
               cs_title.genericdate1 titlegd1, cs_title.genericdate2 titlegd2,
               cs_title.genericdate3 titlegd3, cs_title.genericdate4 titlegd4,
               cs_title.genericdate5 titlegd5, cs_title.genericdate6 titlegd6,
               cs_title.genericboolean1 titlegb1,
               cs_title.genericboolean2 titlegb2,
               cs_title.genericboolean3 titlegb3,
               cs_title.genericboolean4 titlegb4,
               cs_title.genericboolean5 titlegb5,
               cs_title.genericboolean6 titlegb6,
               rpt.reportgroup,
               rpt.reporttitle,
               null, --cs_businessunit.name,
               null, --cs_businessunit.mask,
               rpt.frequency,
               NULL positiongroupname
          FROM cs_participant,
               cs_payee,
               cs_position,
               cs_title,
               --cs_businessunit,
               stelext.rpt_reportgroup rpt
         WHERE
           cs_participant.effectivestartdate < vperiodcalendarrow.enddate
           AND cs_participant.effectiveenddate >= vperiodcalendarrow.startdate
           AND vpipelinerundate >= cs_participant.createdate
           AND vpipelinerundate < cs_participant.removedate
           AND cs_participant.effectivestartdate =
                  (SELECT /*+ index(par cs_participant_IND1) */
                          MAX (effectivestartdate)
                     FROM cs_participant par
                    WHERE par.payeeseq = cs_participant.payeeseq
                      AND vpipelinerundate >= par.createdate
                      AND vpipelinerundate < par.removedate
                      AND par.effectivestartdate < vperiodcalendarrow.enddate
                      AND par.effectiveenddate > vperiodcalendarrow.startdate)
           AND cs_payee.payeeseq = cs_participant.payeeseq
           AND cs_payee.effectivestartdate < vperiodcalendarrow.enddate
           AND cs_payee.effectiveenddate >= vperiodcalendarrow.startdate
           AND vpipelinerundate >= cs_payee.createdate
           AND vpipelinerundate < cs_payee.removedate
           AND cs_payee.effectivestartdate =
                  (SELECT /*+ index(par cs_payee_IND1) */
                          MAX (effectivestartdate)
                     FROM cs_payee par
                    WHERE par.payeeseq = cs_payee.payeeseq
                      AND vpipelinerundate >= par.createdate
                      AND vpipelinerundate < par.removedate
                      AND par.effectivestartdate < vperiodcalendarrow.enddate
                      AND par.effectiveenddate > vperiodcalendarrow.startdate)
           AND cs_position.payeeseq = cs_participant.payeeseq
           --AND cs_position.processingunitseq = cs_businessunit.processingunitseq  -- Tharani 4-03
           --AND cs_payee.businessunitmap = cs_businessunit.mask                    -- Tharani 4-03                                                                               -- Tharani 4-03
           AND vprocessingunitseq = cs_position.processingunitseq
           AND cs_position.effectivestartdate < vperiodcalendarrow.enddate
           AND cs_position.effectiveenddate > vperiodcalendarrow.startdate
           AND cs_position.createdate <= vpipelinerundate
           AND cs_position.removedate > vpipelinerundate
           AND cs_position.effectivestartdate =
                  (SELECT /*+ index(pos cs_position_IND1) */
                          MAX (effectivestartdate)
                     FROM cs_position pos
                    WHERE pos.ruleelementownerseq = cs_position.ruleelementownerseq
                      AND vpipelinerundate >= pos.createdate
                      AND vpipelinerundate < pos.removedate
                      AND pos.effectivestartdate < vperiodcalendarrow.enddate
                      AND pos.effectiveenddate > vperiodcalendarrow.startdate
                      AND vprocessingunitseq = pos.processingunitseq)
           AND cs_title.ruleelementownerseq = cs_position.titleseq
           AND cs_title.effectivestartdate < vperiodcalendarrow.enddate
           AND cs_title.effectiveenddate > vperiodcalendarrow.startdate
           AND cs_title.removedate > vpipelinerundate
           AND cs_title.effectivestartdate =
                  (SELECT /*+ index(pos cs_title_IND1) */
                          MAX (effectivestartdate)
                     FROM cs_title tit
                    WHERE tit.ruleelementownerseq = cs_title.ruleelementownerseq
                      AND vpipelinerundate < tit.removedate
                      AND tit.effectivestartdate < vperiodcalendarrow.enddate
                      AND tit.effectiveenddate > vperiodcalendarrow.startdate)
            AND cs_title.removedate =
                 (SELECT /*+ index(pos cs_title_IND1) */
                          MAX (removedate)
                     FROM cs_title tit
                    WHERE tit.ruleelementownerseq =
                                                   cs_title.ruleelementownerseq
                      AND vpipelinerundate < tit.removedate
                      AND tit.effectivestartdate < vperiodcalendarrow.enddate
                      AND tit.effectiveenddate > vperiodcalendarrow.startdate)
            AND UPPER(rpt.positiontitle) = UPPER(cs_title.name);
    Cursor C_Padimension_position is
          SELECT
               'STEL',
               SYSDATE AS loaddttm,
               vpipelinerundate pipelinerundate,
               vprocessingunitrow.processingunitseq,
               vprocessingunitrow.NAME processingunitname,
               vperiodcalendarrow.calendarseq,
               vcalendarrow.name calendarname,
               vperiodcalendarrow.periodseq, vperiodcalendarrow.name PERIODNAME,
               vperiodcalendarrow.startdate periodstartdate,
               vperiodcalendarrow.enddate periodenddate,
               cs_participant.payeeseq,
               cs_position.ruleelementownerseq positionseq,
               cs_position.managerseq, cs_payee.payeeid participantid,
               cs_position.NAME positionname, cs_title.NAME positiontitle,
               cs_participant.prefix, cs_participant.firstname,
               cs_participant.middlename, cs_participant.lastname,
               cs_participant.suffix,
               TRIM (   NVL (cs_participant.firstname, ' ')
                     || NVL (cs_participant.middlename, ' ')
                     || NVL (cs_participant.lastname, ' ')
                    ) fullname,
               cs_participant.userid, cs_participant.taxid,
               cs_participant.salary, cs_participant.unittypeforsalary,
               cs_position.targetcompensation,
               cs_position.unittypefortargetcompensation,
               cs_participant.effectivestartdate participanteffectivestartdate,
               cs_participant.effectiveenddate participanteffectiveenddate,
               cs_participant.createdate participantcreatedate,
               cs_participant.removedate participantremovedate,
               cs_participant.hiredate, cs_participant.terminationdate,
               cs_position.creditstartdate positioncreditstartdate,
               cs_position.creditenddate positioncreditenddate,
               cs_position.processingstartdate positionprocessingstartdate,
               cs_position.processingenddate positionprocessingenddate,
               cs_position.effectivestartdate positioneffectivestartdate,
               cs_position.effectiveenddate positioneffectiveenddate,
               cs_position.createdate positioncreatedate,
               cs_position.removedate positionremovedate,
               cs_title.effectivestartdate titleeffectivestartdate,
               cs_title.effectiveenddate titleeffectiveenddate,
               cs_title.createdate titlecreatedate,
               cs_title.removedate titleremovedate,
               cs_participant.genericattribute1 participantga1,
               cs_participant.genericattribute2 participantga2,
               cs_participant.genericattribute3 participantga3,
               cs_participant.genericattribute4 participantga4,
               cs_participant.genericattribute5 participantga5,
               cs_participant.genericattribute6 participantga6,
               cs_participant.genericattribute7 participantga7,
               cs_participant.genericattribute8 participantga8,
               cs_participant.genericattribute9 participantga9,
               cs_participant.genericattribute10 participantga10,
               cs_participant.genericattribute11 participantga11,
               cs_participant.genericattribute12 participantga12,
               cs_participant.genericattribute13 participantga13,
               cs_participant.genericattribute14 participantga14,
               cs_participant.genericattribute15 participantga15,
               cs_participant.genericattribute16 participantga16,
               cs_participant.genericnumber1 participantgn1,
               cs_participant.unittypeforgenericnumber1
                                                     participantunittypeforgn1,
               cs_participant.genericnumber2 participantgn2,
               cs_participant.unittypeforgenericnumber2
                                                     participantunittypeforgn2,
               cs_participant.genericnumber3 participantgn3,
               cs_participant.unittypeforgenericnumber3
                                                     participantunittypeforgn3,
               cs_participant.genericnumber4 participantgn4,
               cs_participant.unittypeforgenericnumber4
                                                     participantunittypeforgn4,
               cs_participant.genericnumber5 participantgn5,
               cs_participant.unittypeforgenericnumber5
                                                     participantunittypeforgn5,
               cs_participant.genericnumber6 participantgn6,
               cs_participant.unittypeforgenericnumber6
                                                     participantunittypeforgn6,
               cs_participant.genericdate1 participantgd1,
               cs_participant.genericdate2 participantgd2,
               cs_participant.genericdate3 participantgd3,
               cs_participant.genericdate4 participantgd4,
               cs_participant.genericdate5 participantgd5,
               cs_participant.genericdate6 participantgd6,
               cs_participant.genericboolean1 participantgb1,
               cs_participant.genericboolean2 participantgb2,
               cs_participant.genericboolean3 participantgb3,
               cs_participant.genericboolean4 participantgb4,
               cs_participant.genericboolean5 participantgb5,
               cs_participant.genericboolean6 participantgb6,
               cs_participant.participantemail participantemail,
               cs_position.genericattribute1 positionga1,
               cs_position.genericattribute2 positionga2,
               cs_position.genericattribute3 positionga3,
               cs_position.genericattribute4 positionga4,
               cs_position.genericattribute5 positionga5,
               cs_position.genericattribute6 positionga6,
               cs_position.genericattribute7 positionga7,
               cs_position.genericattribute8 positionga8,
               cs_position.genericattribute9 positionga9,
               cs_position.genericattribute10 positionga10,
               cs_position.genericattribute11 positionga11,
               cs_position.genericattribute12 positionga12,
               cs_position.genericattribute13 positionga13,
               cs_position.genericattribute14 positionga14,
               cs_position.genericattribute15 positionga15,
               cs_position.genericattribute16 positionga16,
               cs_position.genericnumber1 positiongn1,
               cs_position.unittypeforgenericnumber1 positionunittypeforgn1,
               cs_position.genericnumber2 positiongn2,
               cs_position.unittypeforgenericnumber2 positionunittypeforgn2,
               cs_position.genericnumber3 positiongn3,
               cs_position.unittypeforgenericnumber3 positionunittypeforgn3,
               cs_position.genericnumber4 positiongn4,
               cs_position.unittypeforgenericnumber4 positionunittypeforgn4,
               cs_position.genericnumber5 positiongn5,
               cs_position.unittypeforgenericnumber5 positionunittypeforgn5,
               cs_position.genericnumber6 positiongn6,
               cs_position.unittypeforgenericnumber6 positionunittypeforgn6,
               cs_position.genericdate1 positiongd1,
               cs_position.genericdate2 positiongd2,
               cs_position.genericdate3 positiongd3,
               cs_position.genericdate4 positiongd4,
               cs_position.genericdate5 positiongd5,
               cs_position.genericdate6 positiongd6,
               cs_position.genericboolean1 positiongb1,
               cs_position.genericboolean2 positiongb2,
               cs_position.genericboolean3 positiongb3,
               cs_position.genericboolean4 positiongb4,
               cs_position.genericboolean5 positiongb5,
               cs_position.genericboolean6 positiongb6,
               cs_title.genericattribute1 titlega1,
               cs_title.genericattribute2 titlega2,
               cs_title.genericattribute3 titlega3,
               cs_title.genericattribute4 titlega4,
               cs_title.genericattribute5 titlega5,
               cs_title.genericattribute6 titlega6,
               cs_title.genericattribute7 titlega7,
               cs_title.genericattribute8 titlega8,
               cs_title.genericattribute9 titlega9,
               cs_title.genericattribute10 titlega10,
               cs_title.genericattribute11 titlega11,
               cs_title.genericattribute12 titlega12,
               cs_title.genericattribute13 titlega13,
               cs_title.genericattribute14 titlega14,
               cs_title.genericattribute15 titlega15,
               cs_title.genericattribute16 titlega16,
               cs_title.genericnumber1 titlegn1,
               cs_title.unittypeforgenericnumber1 titleunittypeforgn1,
               cs_title.genericnumber2 titlegn2,
               cs_title.unittypeforgenericnumber2 titleunittypeforgn2,
               cs_title.genericnumber3 titlegn3,
               cs_title.unittypeforgenericnumber3 titleunittypeforgn3,
               cs_title.genericnumber4 titlegn4,
               cs_title.unittypeforgenericnumber4 titleunittypeforgn4,
               cs_title.genericnumber5 titlegn5,
               cs_title.unittypeforgenericnumber5 titleunittypeforgn5,
               cs_title.genericnumber6 titlegn6,
               cs_title.unittypeforgenericnumber6 titleunittypeforgn6,
               cs_title.genericdate1 titlegd1, cs_title.genericdate2 titlegd2,
               cs_title.genericdate3 titlegd3, cs_title.genericdate4 titlegd4,
               cs_title.genericdate5 titlegd5, cs_title.genericdate6 titlegd6,
               cs_title.genericboolean1 titlegb1,
               cs_title.genericboolean2 titlegb2,
               cs_title.genericboolean3 titlegb3,
               cs_title.genericboolean4 titlegb4,
               cs_title.genericboolean5 titlegb5,
               cs_title.genericboolean6 titlegb6,
               rpt.reportgroup,
               rpt.reporttitle,
               cs_businessunit.name,
               cs_businessunit.mask,
               rpt.frequency,
               CS_PositionGroup.name positiongroupname
          FROM cs_participant,
               cs_payee,
               cs_position,
               cs_title,
               cs_businessunit,
               rpt_reportgroup rpt,
               CS_PipelineRun_Positions,
               CS_PositionGroup
         WHERE
           cs_participant.effectivestartdate < vperiodcalendarrow.enddate
           AND cs_participant.effectiveenddate >= vperiodcalendarrow.startdate
           AND vpipelinerundate >= cs_participant.createdate
           AND vpipelinerundate < cs_participant.removedate
           AND cs_participant.effectivestartdate =
                  (SELECT /*+ index(par cs_participant_IND1) */
                          MAX (effectivestartdate)
                     FROM cs_participant par
                    WHERE par.payeeseq = cs_participant.payeeseq
                      AND vpipelinerundate >= par.createdate
                      AND vpipelinerundate < par.removedate
                      AND par.effectivestartdate < vperiodcalendarrow.enddate
                      AND par.effectiveenddate > vperiodcalendarrow.startdate)
           AND cs_payee.payeeseq = cs_participant.payeeseq
           AND cs_payee.effectivestartdate < vperiodcalendarrow.enddate
           AND cs_payee.effectiveenddate >= vperiodcalendarrow.startdate
           AND vpipelinerundate >= cs_payee.createdate
           AND vpipelinerundate < cs_payee.removedate
           AND cs_payee.effectivestartdate =
                  (SELECT /*+ index(par cs_payee_IND1) */
                          MAX (effectivestartdate)
                     FROM cs_payee par
                    WHERE par.payeeseq = cs_payee.payeeseq
                      AND vpipelinerundate >= par.createdate
                      AND vpipelinerundate < par.removedate
                      AND par.effectivestartdate < vperiodcalendarrow.enddate
                      AND par.effectiveenddate > vperiodcalendarrow.startdate)
           AND cs_position.payeeseq = cs_participant.payeeseq
           --AND cs_position.RuleElementOwnerSeq = CS_PipelineRun_Positions.PositionSeq  -- Tharani 07-20
           AND CS_PipelineRun_Positions.PipelineRunSeq  = vpipelinerunseq              -- Tharani 07-20
           --AND CS_PositionGroup.PositionGroupSeq  = cs_position.PositionGroupSeq       -- Tharani 07-20
           AND vpipelinerundate < CS_PositionGroup.RemoveDate                          -- Tharani 07-20
           AND CS_PositionGroup.businessunitmap   = cs_businessunit.mask               -- Tharani 07-20
           AND cs_position.processingunitseq = cs_businessunit.processingunitseq  -- Tharani 4-03
           AND cs_payee.businessunitmap = cs_businessunit.mask                    -- Tharani 4-03                                                                               -- Tharani 4-03
           AND vprocessingunitseq = cs_position.processingunitseq
           AND cs_position.effectivestartdate < vperiodcalendarrow.enddate
           AND cs_position.effectiveenddate > vperiodcalendarrow.startdate
           AND cs_position.createdate <= vpipelinerundate
           AND cs_position.removedate > vpipelinerundate
           AND cs_position.effectivestartdate =
                  (SELECT /*+ index(pos cs_position_IND1) */
                          MAX (effectivestartdate)
                     FROM cs_position pos
                    WHERE pos.ruleelementownerseq = cs_position.ruleelementownerseq
                      AND vpipelinerundate >= pos.createdate
                      AND vpipelinerundate < pos.removedate
                      AND pos.effectivestartdate < vperiodcalendarrow.enddate
                      AND pos.effectiveenddate > vperiodcalendarrow.startdate
                      AND vprocessingunitseq = pos.processingunitseq)
           AND cs_title.ruleelementownerseq = cs_position.titleseq
           AND cs_title.effectivestartdate < vperiodcalendarrow.enddate
           AND cs_title.effectiveenddate > vperiodcalendarrow.startdate
           AND cs_title.removedate > vpipelinerundate
           AND cs_title.effectivestartdate =
                  (SELECT /*+ index(pos cs_title_IND1) */
                          MAX (effectivestartdate)
                     FROM cs_title tit
                    WHERE tit.ruleelementownerseq = cs_title.ruleelementownerseq
                      AND vpipelinerundate < tit.removedate
                      AND tit.effectivestartdate < vperiodcalendarrow.enddate
                      AND tit.effectiveenddate > vperiodcalendarrow.startdate)
            AND cs_title.removedate =
                 (SELECT /*+ index(pos cs_title_IND1) */
                          MAX (removedate)
                     FROM cs_title tit
                    WHERE tit.ruleelementownerseq =
                                                   cs_title.ruleelementownerseq
                      AND vpipelinerundate < tit.removedate
                      AND tit.effectivestartdate < vperiodcalendarrow.enddate
                      AND tit.effectiveenddate > vperiodcalendarrow.startdate);
            --AND UPPER(rpt.positiontitle) = UPPER(cs_title.name);
    TYPE padimType IS TABLE OF RPT_BASE_PADIMENSION%ROWTYPE INDEX BY PLS_INTEGER;
    vPaDimension padimType;
 BEGIN
    SELECT *
      INTO vProcessingUnitRow
      FROM CS_PROCESSINGUNIT
     WHERE cs_processingunit.processingunitseq = vprocessingunitseq;
    SELECT *
      INTO vperiodcalendarrow
      FROM cs_period
     WHERE cs_period.calendarseq = vcalendarseq
       AND cs_period.periodseq = vperiodseq
       AND ROWNUM = 1;
    SELECT *
      INTO vCalendarRow
      FROM CS_CALENDAR
     WHERE CS_CALENDAR.calendarseq = vcalendarseq
       AND ROWNUM = 1;
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
     /* Changed for Position Group */   -- Tharani 07-20
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

    Execute IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
    prc_logevent (vperiodcalendarrow.name,
                  vProcName,
                  'Padimesion table insert start',
                  NULL,
                  vsqlerrm);
    --IF vrunmode = 'full' then
      vpipelinerundate:=sysdate;/*
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


     INSERT /*+ APPEND PARALLEL */ INTO RPT_BASE_PADIMENSION NOLOGGING 
     SELECT /*+ leading(cs_position) */
               'STEL',
               SYSDATE AS loaddttm,
               vpipelinerundate pipelinerundate,
               vprocessingunitrow.processingunitseq,
               vprocessingunitrow.NAME processingunitname,
               vperiodcalendarrow.calendarseq,
               vcalendarrow.name calendarname,
               vperiodcalendarrow.periodseq, vperiodcalendarrow.name PERIODNAME,
               vperiodcalendarrow.startdate periodstartdate,
               vperiodcalendarrow.enddate periodenddate,
               cs_participant.payeeseq,
               cs_position.ruleelementownerseq positionseq,
               cs_position.managerseq, cs_payee.payeeid participantid,
               cs_position.NAME positionname, cs_title.NAME positiontitle,
               cs_participant.prefix, cs_participant.firstname,
               cs_participant.middlename, cs_participant.lastname,
               cs_participant.suffix,
               TRIM (   NVL (cs_participant.firstname, ' ')
                     || NVL (cs_participant.middlename, ' ')
                     || NVL (cs_participant.lastname, ' ')
                    ) fullname,
               cs_participant.userid, cs_participant.taxid,
               cs_participant.salary, cs_participant.unittypeforsalary,
               cs_position.targetcompensation,
               cs_position.unittypefortargetcompensation,
               cs_participant.effectivestartdate participanteffectivestartdate,
               cs_participant.effectiveenddate participanteffectiveenddate,
               cs_participant.createdate participantcreatedate,
               cs_participant.removedate participantremovedate,
               cs_participant.hiredate, cs_participant.terminationdate,
               cs_position.creditstartdate positioncreditstartdate,
               cs_position.creditenddate positioncreditenddate,
               cs_position.processingstartdate positionprocessingstartdate,
               cs_position.processingenddate positionprocessingenddate,
               cs_position.effectivestartdate positioneffectivestartdate,
               cs_position.effectiveenddate positioneffectiveenddate,
               cs_position.createdate positioncreatedate,
               cs_position.removedate positionremovedate,
               cs_title.effectivestartdate titleeffectivestartdate,
               cs_title.effectiveenddate titleeffectiveenddate,
               cs_title.createdate titlecreatedate,
               cs_title.removedate titleremovedate,
               cs_participant.genericattribute1 participantga1,
               cs_participant.genericattribute2 participantga2,
               cs_participant.genericattribute3 participantga3,
               cs_participant.genericattribute4 participantga4,
               cs_participant.genericattribute5 participantga5,
               cs_participant.genericattribute6 participantga6,
               cs_participant.genericattribute7 participantga7,
               cs_participant.genericattribute8 participantga8,
               cs_participant.genericattribute9 participantga9,
               cs_participant.genericattribute10 participantga10,
               cs_participant.genericattribute11 participantga11,
               cs_participant.genericattribute12 participantga12,
               cs_participant.genericattribute13 participantga13,
               cs_participant.genericattribute14 participantga14,
               cs_participant.genericattribute15 participantga15,
               cs_participant.genericattribute16 participantga16,
               cs_participant.genericnumber1 participantgn1,
               cs_participant.unittypeforgenericnumber1
                                                     participantunittypeforgn1,
               cs_participant.genericnumber2 participantgn2,
               cs_participant.unittypeforgenericnumber2
                                                     participantunittypeforgn2,
               cs_participant.genericnumber3 participantgn3,
               cs_participant.unittypeforgenericnumber3
                                                     participantunittypeforgn3,
               cs_participant.genericnumber4 participantgn4,
               cs_participant.unittypeforgenericnumber4
                                                     participantunittypeforgn4,
               cs_participant.genericnumber5 participantgn5,
               cs_participant.unittypeforgenericnumber5
                                                     participantunittypeforgn5,
               cs_participant.genericnumber6 participantgn6,
               cs_participant.unittypeforgenericnumber6
                                                     participantunittypeforgn6,
               cs_participant.genericdate1 participantgd1,
               cs_participant.genericdate2 participantgd2,
               cs_participant.genericdate3 participantgd3,
               cs_participant.genericdate4 participantgd4,
               cs_participant.genericdate5 participantgd5,
               cs_participant.genericdate6 participantgd6,
               cs_participant.genericboolean1 participantgb1,
               cs_participant.genericboolean2 participantgb2,
               cs_participant.genericboolean3 participantgb3,
               cs_participant.genericboolean4 participantgb4,
               cs_participant.genericboolean5 participantgb5,
               cs_participant.genericboolean6 participantgb6,
               cs_participant.participantemail participantemail,
               cs_position.genericattribute1 positionga1,
               cs_position.genericattribute2 positionga2,
               cs_position.genericattribute3 positionga3,
               cs_position.genericattribute4 positionga4,
               cs_position.genericattribute5 positionga5,
               cs_position.genericattribute6 positionga6,
               cs_position.genericattribute7 positionga7,
               cs_position.genericattribute8 positionga8,
               cs_position.genericattribute9 positionga9,
               cs_position.genericattribute10 positionga10,
               cs_position.genericattribute11 positionga11,
               cs_position.genericattribute12 positionga12,
               cs_position.genericattribute13 positionga13,
               cs_position.genericattribute14 positionga14,
               cs_position.genericattribute15 positionga15,
               cs_position.genericattribute16 positionga16,
               cs_position.genericnumber1 positiongn1,
               cs_position.unittypeforgenericnumber1 positionunittypeforgn1,
               cs_position.genericnumber2 positiongn2,
               cs_position.unittypeforgenericnumber2 positionunittypeforgn2,
               cs_position.genericnumber3 positiongn3,
               cs_position.unittypeforgenericnumber3 positionunittypeforgn3,
               cs_position.genericnumber4 positiongn4,
               cs_position.unittypeforgenericnumber4 positionunittypeforgn4,
               cs_position.genericnumber5 positiongn5,
               cs_position.unittypeforgenericnumber5 positionunittypeforgn5,
               cs_position.genericnumber6 positiongn6,
               cs_position.unittypeforgenericnumber6 positionunittypeforgn6,
               cs_position.genericdate1 positiongd1,
               cs_position.genericdate2 positiongd2,
               cs_position.genericdate3 positiongd3,
               cs_position.genericdate4 positiongd4,
               cs_position.genericdate5 positiongd5,
               cs_position.genericdate6 positiongd6,
               cs_position.genericboolean1 positiongb1,
               cs_position.genericboolean2 positiongb2,
               cs_position.genericboolean3 positiongb3,
               cs_position.genericboolean4 positiongb4,
               cs_position.genericboolean5 positiongb5,
               cs_position.genericboolean6 positiongb6,
               cs_title.genericattribute1 titlega1,
               cs_title.genericattribute2 titlega2,
               cs_title.genericattribute3 titlega3,
               cs_title.genericattribute4 titlega4,
               cs_title.genericattribute5 titlega5,
               cs_title.genericattribute6 titlega6,
               cs_title.genericattribute7 titlega7,
               cs_title.genericattribute8 titlega8,
               cs_title.genericattribute9 titlega9,
               cs_title.genericattribute10 titlega10,
               cs_title.genericattribute11 titlega11,
               cs_title.genericattribute12 titlega12,
               cs_title.genericattribute13 titlega13,
               cs_title.genericattribute14 titlega14,
               cs_title.genericattribute15 titlega15,
               cs_title.genericattribute16 titlega16,
               cs_title.genericnumber1 titlegn1,
               cs_title.unittypeforgenericnumber1 titleunittypeforgn1,
               cs_title.genericnumber2 titlegn2,
               cs_title.unittypeforgenericnumber2 titleunittypeforgn2,
               cs_title.genericnumber3 titlegn3,
               cs_title.unittypeforgenericnumber3 titleunittypeforgn3,
               cs_title.genericnumber4 titlegn4,
               cs_title.unittypeforgenericnumber4 titleunittypeforgn4,
               cs_title.genericnumber5 titlegn5,
               cs_title.unittypeforgenericnumber5 titleunittypeforgn5,
               cs_title.genericnumber6 titlegn6,
               cs_title.unittypeforgenericnumber6 titleunittypeforgn6,
               cs_title.genericdate1 titlegd1, cs_title.genericdate2 titlegd2,
               cs_title.genericdate3 titlegd3, cs_title.genericdate4 titlegd4,
               cs_title.genericdate5 titlegd5, cs_title.genericdate6 titlegd6,
               cs_title.genericboolean1 titlegb1,
               cs_title.genericboolean2 titlegb2,
               cs_title.genericboolean3 titlegb3,
               cs_title.genericboolean4 titlegb4,
               cs_title.genericboolean5 titlegb5,
               cs_title.genericboolean6 titlegb6,
               rpt.reportgroup,
               rpt.reporttitle,
               null, --cs_businessunit.name,
               null, --cs_businessunit.mask,
               rpt.frequency,
               NULL positiongroupname
          FROM cs_participant,
               cs_payee,
               cs_position,
               cs_title,
               --cs_businessunit,
               stelext.rpt_reportgroup rpt
         WHERE
           cs_participant.effectivestartdate < vperiodcalendarrow.enddate
           AND cs_participant.effectiveenddate >= vperiodcalendarrow.startdate
           AND vpipelinerundate >= cs_participant.createdate
           AND vpipelinerundate < cs_participant.removedate
           AND cs_participant.effectivestartdate =
                  (SELECT /*+ index(par cs_participant_IND1) */
                          MAX (effectivestartdate)
                     FROM cs_participant par
                    WHERE par.payeeseq = cs_participant.payeeseq
                      AND vpipelinerundate >= par.createdate
                      AND vpipelinerundate < par.removedate
                      AND par.effectivestartdate < vperiodcalendarrow.enddate
                      AND par.effectiveenddate > vperiodcalendarrow.startdate)
           AND cs_payee.payeeseq = cs_participant.payeeseq
           AND cs_payee.effectivestartdate < vperiodcalendarrow.enddate
           AND cs_payee.effectiveenddate >= vperiodcalendarrow.startdate
           AND vpipelinerundate >= cs_payee.createdate
           AND vpipelinerundate < cs_payee.removedate
           AND cs_payee.effectivestartdate =
                  (SELECT /*+ index(par cs_payee_IND1) */
                          MAX (effectivestartdate)
                     FROM cs_payee par
                    WHERE par.payeeseq = cs_payee.payeeseq
                      AND vpipelinerundate >= par.createdate
                      AND vpipelinerundate < par.removedate
                      AND par.effectivestartdate < vperiodcalendarrow.enddate
                      AND par.effectiveenddate > vperiodcalendarrow.startdate)
           AND cs_position.payeeseq = cs_participant.payeeseq
           --AND cs_position.processingunitseq = cs_businessunit.processingunitseq  -- Tharani 4-03
           --AND cs_payee.businessunitmap = cs_businessunit.mask                    -- Tharani 4-03                                                                               -- Tharani 4-03
           AND vprocessingunitseq = cs_position.processingunitseq
           AND cs_position.effectivestartdate < vperiodcalendarrow.enddate
           AND cs_position.effectiveenddate > vperiodcalendarrow.startdate
           AND cs_position.createdate <= vpipelinerundate
           AND cs_position.removedate > vpipelinerundate
           AND cs_position.effectivestartdate =
                  (SELECT /*+ index(pos cs_position_IND1) */
                          MAX (effectivestartdate)
                     FROM cs_position pos
                    WHERE pos.ruleelementownerseq = cs_position.ruleelementownerseq
                      AND vpipelinerundate >= pos.createdate
                      AND vpipelinerundate < pos.removedate
                      AND pos.effectivestartdate < vperiodcalendarrow.enddate
                      AND pos.effectiveenddate > vperiodcalendarrow.startdate
                      AND vprocessingunitseq = pos.processingunitseq)
           AND cs_title.ruleelementownerseq = cs_position.titleseq
           AND cs_title.effectivestartdate < vperiodcalendarrow.enddate
           AND cs_title.effectiveenddate > vperiodcalendarrow.startdate
           AND cs_title.removedate > vpipelinerundate
           AND cs_title.effectivestartdate =
                  (SELECT /*+ index(pos cs_title_IND1) */
                          MAX (effectivestartdate)
                     FROM cs_title tit
                    WHERE tit.ruleelementownerseq = cs_title.ruleelementownerseq
                      AND vpipelinerundate < tit.removedate
                      AND tit.effectivestartdate < vperiodcalendarrow.enddate
                      AND tit.effectiveenddate > vperiodcalendarrow.startdate)
            AND cs_title.removedate =
                 (SELECT /*+ index(pos cs_title_IND1) */
                          MAX (removedate)
                     FROM cs_title tit
                    WHERE tit.ruleelementownerseq =
                                                   cs_title.ruleelementownerseq
                      AND vpipelinerundate < tit.removedate
                      AND tit.effectivestartdate < vperiodcalendarrow.enddate
                      AND tit.effectiveenddate > vperiodcalendarrow.startdate)
            AND UPPER(rpt.positiontitle) = UPPER(cs_title.name);

      prc_logevent (vperiodcalendarrow.name,
                  vProcName,
                  'Padimesion table insert end',
                  NULL,
                  vsqlerrm);
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
    COMMIT;
    EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML';
 EXCEPTION
    WHEN OTHERS THEN
             vsqlerrm := SUBSTR (SQLERRM, 1, 3900);
             prc_logevent(vperiodcalendarrow.name,vprocname,'ERROR',NULL,vsqlerrm);
             raise_application_error( -20911,'Error raised: '||vprocname||' Failed: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE || ' - '||vsqlerrm);
     END PRC_BASE_PADIMENSION;
