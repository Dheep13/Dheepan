CREATE PROCEDURE EXT.SP_CLAWBACK_CALCULATION_FA(IN i_periodSeq INT)
LANGUAGE SQLSCRIPT
AS
BEGIN
    DECLARE v_sql NVARCHAR(5000);
    DECLARE v_tempcount INT;
    DECLARE v_pipelinerunseq INT;
    DECLARE v_pipelinerundate DATE;
    DECLARE v_batch_no_lumpsum_comm INT;
    DECLARE v_batch_no_lumpsum_comp INT;
    DECLARE v_batch_no_ongoing_comm INT;
    DECLARE v_batch_no_ongoing_comp INT;
    DECLARE v_processingunitseq INT;
    DECLARE v_str_cycledate VARCHAR(20);
    DECLARE v_period_start DATE;
    DECLARE v_period_end DATE;
    DECLARE STR_CYCLEDATE_FILE_NAME CONSTANT VARCHAR(10) := 'GLOBAL';
    DECLARE STR_CYCLEDATE_KEY CONSTANT VARCHAR(20) := 'OPER_CYCLE_DATE';
    DECLARE STR_LUMPSUM CONSTANT VARCHAR(20) := 'LUMPSUM';
    DECLARE STR_ONGOING CONSTANT VARCHAR(20) := 'ONGOING';
    DECLARE STR_COMMISSION CONSTANT VARCHAR(20) := 'COMMISSION';
    DECLARE STR_COMPENSATION CONSTANT VARCHAR(20) := 'COMPENSATION';
    DECLARE STR_STATUS_START CONSTANT VARCHAR(20) := 'start';
    DECLARE STR_STATUS_PROCESSING CONSTANT VARCHAR(20) := 'processing';
    DECLARE STR_STATUS_FAIL CONSTANT VARCHAR(20) := 'fail';
    DECLARE STR_STATUS_COMPLETED_SP CONSTANT VARCHAR(20) := 'completed_sp';
    DECLARE STR_STATUS_COMPLETED_SH CONSTANT VARCHAR(20) := 'completed_sh';
    DECLARE V_MESSAGE VARCHAR(2000);
    DECLARE v_batch_no_special INT; -- added for version 6
    DECLARE v_periodStartDate DATE;
    DECLARE v_previous_qtr VARCHAR(10); -- added for version 14
    DECLARE v_eot DATE := TO_DATE('01/01/2200','mm/dd/yyyy');
    DECLARE v_tenantId varchar(5);
    DECLARE v_maxCreditSeq BIGINT;
    DECLARE GV_CALENDARSEQ BIGINT;
    DECLARE GV_PERIODSEQ BIGINT;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            V_MESSAGE = SUBSTRING(::SQL_ERROR_MESSAGE,1,2000);  /* ORIGSQL: SUBSTR(SQLERRM,1,2000) */

            /* ORIGSQL: Log('[CB] ' || v_message) */
            CALL ext.Log('[CB] '|| IFNULL(:V_MESSAGE,''));

            --update the batch status to fail
            /* ORIGSQL: PK_AIA_CB_CALCULATION:sp_update_batch_status(v_batch_no_lumpsum_comm, STR_STATUS_FAIL) */
            CALL  EXT.PK_AIA_CB_CALCULATION_FA:sp_update_batch_status(:v_batch_no_lumpsum_comm, :STR_STATUS_FAIL);

            /* ORIGSQL: PK_AIA_CB_CALCULATION:sp_update_batch_status(v_batch_no_ongoing_comm, STR_STATUS_FAIL) */
            CALL  EXT.PK_AIA_CB_CALCULATION_FA:sp_update_batch_status(:v_batch_no_ongoing_comm, :STR_STATUS_FAIL);

            /* ORIGSQL: PK_AIA_CB_CALCULATION:sp_update_batch_status(v_batch_no_lumpsum_comp, STR_STATUS_FAIL) */
            CALL  EXT.PK_AIA_CB_CALCULATION_FA:sp_update_batch_status(:v_batch_no_lumpsum_comp, :STR_STATUS_FAIL);

            /* ORIGSQL: PK_AIA_CB_CALCULATION:sp_update_batch_status(v_batch_no_ongoing_comp, STR_STATUS_FAIL) */
            CALL  EXT.PK_AIA_CB_CALCULATION_FA:sp_update_batch_status(:v_batch_no_ongoing_comp, :STR_STATUS_FAIL);
        END;


    --Get tenantid
   select tenantid into v_tenantId from cs_tenant;

    -- Get cycle date
    SELECT TXT_KEY_VALUE INTO v_str_cycledate
    FROM ext.IN_ETL_CONTROL
    WHERE TXT_FILE_NAME = :STR_CYCLEDATE_FILE_NAME
      AND TXT_KEY_STRING = :STR_CYCLEDATE_KEY;

    -- Log cycle date
    CALL EXT.Log('[CB] Oper cycle date is: ' || :v_str_cycledate);

    -- Get period start date and period end date
    SELECT cp.startdate, add_days(cp.enddate, - 1)
    INTO v_period_start, v_period_end
    FROM cs_period cp
    WHERE cp.periodseq = :i_periodSeq;

    -- If current cycle date not between pipeline period, then exit
    IF TO_DATE(:v_str_cycledate, 'yyyy-mm-dd') BETWEEN :v_period_start AND :v_period_end THEN
        -- Execute commission ongoing
        CALL EXT.Log('[CB] SP_EXEC_COMMISSION_ONGOING_FA starting ' || :v_str_cycledate);
        CALL EXT.PK_AIA_CB_CALCULATION_FA:SP_EXEC_COMMISSION_ONGOING_FA(:v_str_cycledate);
        CALL EXT.Log('[CB] SP_EXEC_COMMISSION_ONGOING_FA ending');

        -- Execute compensation ongoing
        CALL EXT.Log('[CB] SP_EXEC_COMP_ONGOING_FA starting');
        CALL  EXT.PK_AIA_CB_CALCULATION_FA:SP_EXEC_COMP_ONGOING_FA(:v_str_cycledate);
        CALL EXT.Log('[CB] SP_EXEC_COMP_ONGOING_FA ended');

        -- Get pipeline run sequence number and date
        SELECT MAX(y.pipelineRunSeq), MAX(y.startTime)
        INTO v_pipelinerunseq, v_pipelinerundate
        FROM CS_PipelineRun y
        WHERE y.command = 'PipelineRun' AND y.stopTime IS NULL AND y.startTime = 
        (SELECT MAX(x.startTime) FROM CS_PipelineRun x WHERE x.command = 'PipelineRun' AND x.stopTime IS NULL);

        IF :v_pipelinerundate IS NULL THEN
            SELECT MAX(pipelineRunSeq), CURRENT_DATE
            INTO v_pipelinerunseq, v_pipelinerundate
            FROM CS_PipelineRun y
            WHERE y.command = 'PipelineRun';
        END IF;

        -- Get the batch number for special handling
        SELECT COALESCE(MAX(batchnum), 0)
        INTO v_batch_no_special
        FROM ext.aia_cb_batch_special
        WHERE ISACTIVE = 'Y' AND SPEC_RUN_DATE = TO_DATE(:v_str_cycledate, 'yyyy-mm-dd') AND BUNAME = 'SGPAFA';

        CALL EXT.Log('[CB] Special run batch number is: ' || :v_batch_no_special);

        -- Get active batch number for lumpsum commission
        v_batch_no_lumpsum_comm :=  EXT.PK_AIA_CB_CALCULATION_FA:fn_get_batch_no(:v_str_cycledate, :STR_COMMISSION, :STR_LUMPSUM, :STR_STATUS_COMPLETED_SP);

        IF :v_batch_no_lumpsum_comm = 0 THEN
            v_batch_no_lumpsum_comm :=  EXT.PK_AIA_CB_CALCULATION_FA:fn_get_batch_no(:v_str_cycledate, :STR_COMMISSION, :STR_LUMPSUM, :STR_STATUS_COMPLETED_SH);
        END IF;

        CALL EXT.Log('[CB] Clawback batch for commission LUMPSUM: ' || :v_batch_no_lumpsum_comm);
        CALL EXT.PK_AIA_CB_CALCULATION_FA:sp_revert_by_batch(:v_batch_no_lumpsum_comm);

        -- Get active batch number for on-going commission
        v_batch_no_ongoing_comm :=  EXT.PK_AIA_CB_CALCULATION_FA:fn_get_batch_no(:v_str_cycledate, :STR_COMMISSION, :STR_ONGOING, :STR_STATUS_COMPLETED_SP);
        CALL EXT.Log('[CB] Clawback batch for commission ONGOING: ' || :v_batch_no_ongoing_comm);
        CALL EXT.PK_AIA_CB_CALCULATION_FA:sp_revert_by_batch(:v_batch_no_ongoing_comm);

        -- Get active batch number for lumpsum compensation
        v_batch_no_lumpsum_comp :=  EXT.PK_AIA_CB_CALCULATION_FA:fn_get_batch_no(:v_str_cycledate, :STR_COMPENSATION, :STR_LUMPSUM, :STR_STATUS_COMPLETED_SP);
        IF :v_batch_no_lumpsum_comp = 0 THEN
            v_batch_no_lumpsum_comp :=  EXT.PK_AIA_CB_CALCULATION_FA:fn_get_batch_no(:v_str_cycledate, :STR_COMPENSATION, :STR_LUMPSUM, :STR_STATUS_COMPLETED_SH);
        END IF;
        CALL EXT.Log('[CB] Clawback batch for compensation LUMPSUM: ' || :v_batch_no_lumpsum_comp);
        CALL EXT.PK_AIA_CB_CALCULATION_FA:sp_revert_by_batch(:v_batch_no_lumpsum_comp);

        -- Get active batch number for on-going compensation
        v_batch_no_ongoing_comp := EXT.PK_AIA_CB_CALCULATION_FA:fn_get_batch_no(:v_str_cycledate, :STR_COMPENSATION, :STR_ONGOING, :STR_STATUS_COMPLETED_SP);
        CALL EXT.Log('[CB] Clawback batch for compensation ONGOING: ' || :v_batch_no_ongoing_comp);
        CALL EXT.PK_AIA_CB_CALCULATION_FA:sp_revert_by_batch(:v_batch_no_ongoing_comp);

        -- Prepare the commission clawback data for insert to TrueComp build in tables
        -- Credit
        CALL EXT.PK_AIA_CB_CALCULATION_FA:SP_CREDIT_COMMISSION_FA(:v_str_cycledate, :v_batch_no_lumpsum_comm, :v_batch_no_ongoing_comm);
        -- Primary measurement
        CALL EXT.PK_AIA_CB_CALCULATION_FA:SP_PM_COMMISSION_FA(:v_str_cycledate, :v_batch_no_lumpsum_comm, :v_batch_no_ongoing_comm);
        -- PM credit trace
        CALL EXT.PK_AIA_CB_CALCULATION_FA:SP_PMCRDTRACE_COMMISSION_FA(:v_str_cycledate, :v_batch_no_lumpsum_comm, :v_batch_no_ongoing_comm);

        -- Prepare the compensation clawback data for insert to TrueComp build in tables
        -- Credit
        CALL EXT.PK_AIA_CB_CALCULATION_FA:SP_CREDIT_COMP_FA(:v_str_cycledate, :v_batch_no_lumpsum_comp, :v_batch_no_ongoing_comp);
        -- Primary measurement
        CALL EXT.PK_AIA_CB_CALCULATION_FA:SP_PM_COMP_FA(:v_str_cycledate, :v_batch_no_lumpsum_comp, :v_batch_no_ongoing_comp);
        -- PM credit trace
        CALL EXT.PK_AIA_CB_CALCULATION_FA:SP_PMCRDTRACE_COMP_FA(:v_str_cycledate, :v_batch_no_lumpsum_comp, :v_batch_no_ongoing_comp);

        -- Insert records into cs_credit
        CALL EXT.Log('[CB] Insert COMMISSION records into cs_credit for batch-' || :v_batch_no_lumpsum_comm || ' and batch-' || :v_batch_no_ongoing_comm);
        CALL EXT.Log('[CB] Insert COMPENSATION records into cs_credit for batch-' || :v_batch_no_lumpsum_comp || ' and batch-' || :v_batch_no_ongoing_comp);

		select max(creditseq) into v_maxCreditSeq from cs_credit; --Deepan : Getting max of creditseq from cs_credit;
        
      --Deepan : Commenting this out and changing it to dynamic sql  
        
        /*
        INSERT INTO cs_credit
        SELECT :v_tenantId AS tenantid,
               ROW_NUMBER() OVER () + :v_maxCreditSeq as creditseq,
               --NEWID() AS creditseq, -- Use NEWID() to generate unique identifier in HANA
               payeeseq,
               positionseq,
               salesorderseq,
               salestransactionseq,
               periodseq,
               credittypeseq,
               name,
               :v_pipelinerunseq AS pipelinerunseq,
               origintypeid,
               compensationdate,
               :v_pipelinerundate AS pipelinerundate,
               businessunitmap,
               preadjustedvalue,
               unittypeforpreadjustedvalue,
               value,
               unittypeforvalue,
               releasedate,
               ruleseq,
               isheld,
               isrollable,
               rolldate,
               reasonseq,
               comments,
               genericattribute1,
               genericattribute2,
               genericattribute3,
               genericattribute4,
               genericattribute5,
               genericattribute6,
               genericattribute7,
               genericattribute8,
               genericattribute9,
               genericattribute10,
               genericattribute11,
               genericattribute12,
               genericattribute13,
               genericattribute14,
               genericattribute15,
               genericattribute16,
               genericnumber1,
               unittypeforgenericnumber1,
               genericnumber2,
               unittypeforgenericnumber2,
               genericnumber3,
               unittypeforgenericnumber3,
               genericnumber4,
               unittypeforgenericnumber4,
               genericnumber5,
               unittypeforgenericnumber5,
               genericnumber6,
               unittypeforgenericnumber6,
               genericdate1,
               genericdate2,
               genericdate3,
               genericdate4,
               genericdate5,
               genericdate6,
               genericboolean1,
               genericboolean2,
               genericboolean3,
               genericboolean4,
               genericboolean5,
               genericboolean6,
               processingunitseq,
               1 ---Deepan : Additional ISPRIVATE column in HANA
        FROM EXT.AIA_CB_CREDIT_STG crd
        WHERE crd.batch_no IN (:v_batch_no_lumpsum_comm,
                               :v_batch_no_ongoing_comm,
                               :v_batch_no_lumpsum_comp,
                               :v_batch_no_ongoing_comp,
                               :v_batch_no_special);
*/


v_sql := 'INSERT INTO cs_credit ' ||
         'SELECT :v_tenantId AS tenantid, ' ||
         'ROW_NUMBER() OVER () + :v_maxCreditSeq AS creditseq, ' ||
         'payeeseq, positionseq, salesorderseq, salestransactionseq, periodseq, credittypeseq, name, ' ||
         ':v_pipelinerunseq AS pipelinerunseq, origintypeid, compensationdate, :v_pipelinerundate AS pipelinerundate, ' ||
         'businessunitmap, preadjustedvalue, unittypeforpreadjustedvalue, value, unittypeforvalue, releasedate, ' ||
         'ruleseq, isheld, isrollable, rolldate, reasonseq, comments, ' ||
         'genericattribute1, genericattribute2, genericattribute3, genericattribute4, genericattribute5, ' ||
         'genericattribute6, genericattribute7, genericattribute8, genericattribute9, genericattribute10, ' ||
         'genericattribute11, genericattribute12, genericattribute13, genericattribute14, genericattribute15, ' ||
         'genericattribute16, genericnumber1, unittypeforgenericnumber1, genericnumber2, unittypeforgenericnumber2, ' ||
         'genericnumber3, unittypeforgenericnumber3, genericnumber4, unittypeforgenericnumber4, genericnumber5, ' ||
         'unittypeforgenericnumber5, genericnumber6, unittypeforgenericnumber6, genericdate1, genericdate2, ' ||
         'genericdate3, genericdate4, genericdate5, genericdate6, genericboolean1, genericboolean2, ' ||
         'genericboolean3, genericboolean4, genericboolean5, genericboolean6, processingunitseq, 1 ' ||
         'FROM EXT.AIA_CB_CREDIT_STG crd ' ||
         'WHERE crd.batch_no IN (:v_batch_no_lumpsum_comm, :v_batch_no_ongoing_comm, ' ||
         ':v_batch_no_lumpsum_comp, :v_batch_no_ongoing_comp, :v_batch_no_special);';

EXECUTE IMMEDIATE v_sql USING v_tenantId, v_maxCreditSeq, v_pipelinerunseq, v_pipelinerundate, v_batch_no_lumpsum_comm, v_batch_no_ongoing_comm,
                  v_batch_no_lumpsum_comp, v_batch_no_ongoing_comp, v_batch_no_special;

CALL EXT.Log(:v_sql);


        CALL EXT.Log('[CB] Insert records into cs_credit' || '; row count: ' || TO_NVARCHAR(::ROWCOUNT));

        COMMIT;


--insert records into cs_measurement
CALL EXT.Log('[CB] Insert COMMISSION records into cs_measurement for batch-' || :v_batch_no_lumpsum_comm || ' and batch-' || :v_batch_no_ongoing_comm);
CALL EXT.Log('[CB] Insert COMPENSATION records into cs_measurement for batch-' || :v_batch_no_lumpsum_comp || ' and batch-' || :v_batch_no_ongoing_comp);
CALL EXT.Log(to_varchar(:v_processingunitseq));
CALL EXT.Log(:v_batch_no_lumpsum_comm||' '|| :v_batch_no_ongoing_comm||' '|| :v_batch_no_lumpsum_comp||' '|| :v_batch_no_ongoing_comp);


SELECT COUNT(*)
    INTO v_tempcount
    FROM ext.AIA_CB_PM_STG pms
    JOIN cs_period pd ON pd.periodseq = pms.periodseq AND pd.removedate = :v_eot
    JOIN cs_position p ON p.ruleelementownerseq = pms.positionseq AND p.removedate = :v_eot
      AND pd.startdate BETWEEN p.effectivestartdate AND add_days(p.effectiveenddate ,- 1)
    LEFT JOIN cs_planassignable pa ON pa.ruleelementownerseq = p.titleseq AND pa.removedate = :v_eot
      AND pd.startdate BETWEEN pa.effectivestartdate AND add_days(pa.effectiveenddate, -1)
    WHERE pms.batch_no IN (:v_batch_no_lumpsum_comm, :v_batch_no_ongoing_comm, :v_batch_no_lumpsum_comp,
                           :v_batch_no_ongoing_comp, :v_batch_no_special);
   -- group by new_measurementseq, pms.name, pms.payeeseq, pms.positionseq, pms.periodseq;

  CALL  EXT.Log('v_tempcount '||:v_tempcount);


--Deepan : Commenting this out and changing it to dynamic sql
/*merge into cs_measurement tgt
using (
select   --revised by Win for version 6
         --new_measurementseq as measurementseq,
         max(new_measurementseq) as measurementseq,
         max(pa.planseq) planseq,
         :v_tenantId tenantid,
         pms.name,
         pms.payeeseq,
         pms.positionseq,
         pms.periodseq,
         --v_pipelinerunseq as pipelinerunseq,
         --v_pipelinerundate as pipelinerundate,
         max(pms.ruleseq) as ruleseq,
         sum(pms.value) as value,
         max(pms.unittypeforvalue) as unittypeforvalue,
         sum(pms.numberofcredits) as numberofcredits,
         max(pms.businessunitmap) as businessunitmap,
         max(pms.genericnumber1) as genericnumber1,
         max(pms.unittypeforgenericnumber1) as unittypeforgenericnumber1,
         max(pms.processingunitseq) processingunitseq,
         max(pms.unittypefornumberofcredits) unittypefornumberofcredits
    from ext.AIA_CB_PM_STG pms
    join cs_period pd
on pd.periodseq=pms.periodseq and pd.removedate=:v_eot
join  cs_position p
on p.ruleelementownerseq=pms.positionseq and p.removedate=:v_eot
and pd.startdate between p.effectivestartdate and add_days(p.effectiveenddate,-1)
left join  cs_planassignable  pa
on (pa.ruleelementownerseq=p.titleseq) and pa.removedate=:v_eot
and pd.startdate between pa.effectivestartdate and add_days(pa.effectiveenddate,-1)
    where pms.batch_no in (:v_batch_no_lumpsum_comm,
                          :v_batch_no_ongoing_comm,
                          :v_batch_no_lumpsum_comp,
                          :v_batch_no_ongoing_comp,
                          :v_batch_no_special --revised by Win Tan for version 6
                          )
    group by --new_measurementseq,  --revised by Win for version 6
    pms.name, pms.payeeseq, pms.positionseq, pms.periodseq) src
on ( src.positionseq=tgt.positionseq and src.payeeseq=tgt.payeeseq
and src.periodseq=tgt.periodseq and src.tenantid=tgt.tenantid
 and src.processingunitseq=tgt.processingunitseq
 and src.name=tgt.name
 and tgt.tenantid=:v_tenantId)
when matched then update
set tgt.planseq=src.planseq,
   -- tgt.pipelinerunseq=src.pipelinerunseq,
   --tgt.pipelinerundate=src.pipelinerundate,
   tgt.ruleseq=src.ruleseq,
   tgt.value=src.value,
   tgt.unittypeforvalue=src.unittypeforvalue,
   tgt.numberofcredits=src.numberofcredits,
   tgt.businessunitmap=src.businessunitmap,
   tgt.genericnumber1=src.genericnumber1,
   tgt.unittypeforgenericnumber1=src.unittypeforgenericnumber1,
   tgt.unittypefornumberofcredits=src.unittypefornumberofcredits; --and tgt.processingunitseq=v_processingunitseq;

*/

v_sql := 'MERGE INTO cs_measurement AS tgt USING (' ||
         'SELECT ' ||
         'MAX(new_measurementseq) AS measurementseq, ' ||
         'MAX(pa.planseq) AS planseq, ' ||
         ':v_tenantId AS tenantid, ' ||
         'pms.name, ' ||
         'pms.payeeseq, ' ||
         'pms.positionseq, ' ||
         'pms.periodseq, ' ||
         'MAX(pms.ruleseq) AS ruleseq, ' ||
         'SUM(pms.value) AS value, ' ||
         'MAX(pms.unittypeforvalue) AS unittypeforvalue, ' ||
         'SUM(pms.numberofcredits) AS numberofcredits, ' ||
         'MAX(pms.businessunitmap) AS businessunitmap, ' ||
         'MAX(pms.genericnumber1) AS genericnumber1, ' ||
         'MAX(pms.unittypeforgenericnumber1) AS unittypeforgenericnumber1, ' ||
         'MAX(pms.processingunitseq) AS processingunitseq, ' ||
         'MAX(pms.unittypefornumberofcredits) AS unittypefornumberofcredits ' ||
         'FROM ext.AIA_CB_PM_STG AS pms ' ||
         'JOIN cs_period AS pd ON pd.periodseq = pms.periodseq AND pd.removedate = :v_eot ' ||
         'JOIN cs_position AS p ON p.ruleelementownerseq = pms.positionseq AND p.removedate = :v_eot ' ||
         'AND pd.startdate BETWEEN p.effectivestartdate AND ADD_DAYS(p.effectiveenddate, -1) ' ||
         'LEFT JOIN cs_planassignable AS pa ON pa.ruleelementownerseq = p.titleseq AND pa.removedate = :v_eot ' ||
         'AND pd.startdate BETWEEN pa.effectivestartdate AND ADD_DAYS(pa.effectiveenddate, -1) ' ||
         'WHERE pms.batch_no IN (:v_batch_no_lumpsum_comm, :v_batch_no_ongoing_comm, :v_batch_no_lumpsum_comp, :v_batch_no_ongoing_comp, :v_batch_no_special) ' ||
         'GROUP BY pms.name, pms.payeeseq, pms.positionseq, pms.periodseq) AS src ' ||
         'ON (src.positionseq = tgt.positionseq AND src.payeeseq = tgt.payeeseq AND src.periodseq = tgt.periodseq AND src.tenantid = tgt.tenantid ' ||
         'AND src.processingunitseq = tgt.processingunitseq AND src.name = tgt.name AND tgt.tenantid = :v_tenantId) ' ||
         'WHEN MATCHED THEN UPDATE SET tgt.planseq = src.planseq, tgt.ruleseq = src.ruleseq, tgt.value = tgt.value+src.value, ' ||
         'tgt.unittypeforvalue = src.unittypeforvalue, tgt.numberofcredits = src.numberofcredits, tgt.businessunitmap = src.businessunitmap, ' ||
         'tgt.genericnumber1 = src.genericnumber1, tgt.unittypeforgenericnumber1 = src.unittypeforgenericnumber1, tgt.unittypefornumberofcredits = src.unittypefornumberofcredits;';

CALL EXT.Log(:v_sql);
EXECUTE IMMEDIATE v_sql USING v_tenantId, v_eot, v_tenantId, v_batch_no_lumpsum_comm, v_batch_no_ongoing_comm, v_batch_no_lumpsum_comp, v_batch_no_ongoing_comp, v_batch_no_special, v_tenantId;




/*This query only works once  - since the orig measurement seq is not backed up*/
update  ext.AIA_CB_PMCRDTRACE_STG tgt
set measurementseq= (select
--Version 14 add by Amanda begin
--max(m.measurementseq)
case when max(m.measurementseq) is null then tgt.measurementseq --for SPI CB, no need to update credit trace stage
     else max(m.measurementseq) end
--Version 14 add by Amanda end
from cs_measurement m
join ext.AIA_CB_PM_STG pms
on m.positionseq=pms.positionseq and  m.payeeseq=pms.payeeseq
and m.periodseq=pms.periodseq --and src.tenantid=pms.tenantid
 and m.processingunitseq=pms.processingunitseq
 and m.name=pms.name and m.tenantid=:v_tenantId
where pms.batch_no in (:v_batch_no_lumpsum_comm,
                          :v_batch_no_ongoing_comm,
                          :v_batch_no_lumpsum_comp,
                          :v_batch_no_ongoing_comp,
                          :v_batch_no_special --revised by Win Tan for version 6
                          )
and  pms.new_measurementseq =tgt.measurementseq
and pms.batch_no=tgt.batch_no
)
where tgt.batch_no in  (:v_batch_no_lumpsum_comm,
                          :v_batch_no_ongoing_comm,
                          v_batch_no_lumpsum_comp,
                          :v_batch_no_ongoing_comp,
                          :v_batch_no_special --revised by Win Tan for version 6
                          )
;

CALL EXT.Log('[CB] Merge records into cs_measurement' || '; row count: ' || to_varchar(::ROWCOUNT));

    commit;

    --insert records into cs_pmcredittrace
CALL EXT.LOG('[CB] Insert COMMISSION records into cs_pmcredittrace for batch-' || :v_batch_no_lumpsum_comm || ' and batch-' || :v_batch_no_ongoing_comm);
CALL EXT.LOG('[CB] Insert COMPENSATION records into cs_pmcredittrace for batch-' || :v_batch_no_lumpsum_comp || ' and batch-' || :v_batch_no_ongoing_comp);
--v12
--Deepan : Commenting this out and changing it to dynamic sql
/*
insert into cs_pmcredittrace
 (Tenantid,
       creditSeq,
       measurementSeq,
       ruleSeq,
       pipelineRunSeq,
       sourcePeriodSeq,
       targetPeriodSeq,
       sourceorigintypeid,
       contributionValue,
       unittypeforContributionValue,
       businessunitMap,
       processingUnitSeq)
  select :v_tenantId as Tenantid,
          creditseq,
         measurementseq,
         ruleseq,
         :v_pipelinerunseq as pipelinerunseq,
         sourceperiodseq,
         targetperiodseq,
         sourceorigintypeid,
         contributionvalue,
         unittypeforcontributionvalue,
         businessunitmap,
         processingunitseq
    from ext.AIA_CB_PMCRDTRACE_STG pcs
   where pcs.batch_no in (:v_batch_no_lumpsum_comm,
                          :v_batch_no_ongoing_comm,
                          :v_batch_no_lumpsum_comp,
                          :v_batch_no_ongoing_comp,
                          :v_batch_no_special --revised by Win Tan for version 6
                          )
;
*/

v_sql := 'INSERT INTO cs_pmcredittrace (' ||
         'Tenantid, ' ||
         'creditSeq, ' ||
         'measurementSeq, ' ||
         'ruleSeq, ' ||
         'pipelineRunSeq, ' ||
         'sourcePeriodSeq, ' ||
         'targetPeriodSeq, ' ||
         'sourceorigintypeid, ' ||
         'contributionValue, ' ||
         'unittypeforContributionValue, ' ||
         'businessunitMap, ' ||
         'processingUnitSeq) ' ||
         'SELECT :v_tenantId AS Tenantid, ' ||
         'creditseq, ' ||
         'measurementseq, ' ||
         'ruleseq, ' ||
         ':v_pipelinerunseq AS pipelinerunseq, ' ||
         'sourceperiodseq, ' ||
         'targetperiodseq, ' ||
         'sourceorigintypeid, ' ||
         'contributionvalue, ' ||
         'unittypeforcontributionvalue, ' ||
         'businessunitmap, ' ||
         'processingunitseq ' ||
         'FROM ext.AIA_CB_PMCRDTRACE_STG pcs ' ||
         'WHERE pcs.batch_no IN (:v_batch_no_lumpsum_comm, ' ||
         ':v_batch_no_ongoing_comm, ' ||
         ':v_batch_no_lumpsum_comp, ' ||
         ':v_batch_no_ongoing_comp, ' ||
         ':v_batch_no_special);';
         
CALL EXT.Log (:v_sql );
EXECUTE IMMEDIATE v_sql USING v_tenantId, v_pipelinerunseq, v_batch_no_lumpsum_comm, v_batch_no_ongoing_comm, v_batch_no_lumpsum_comp, v_batch_no_ongoing_comp, v_batch_no_special;

CALL EXT.LOG('[CB] Insert records into cs_pmcredittrace' || '; row count: ' || to_varchar(::ROWCOUNT));

    commit;

--Version 12 add by Amanda begin
select startDate
      into v_periodStartDate
      from cs_period
     where periodSeq = i_periodSeq;

--get previous quarter for YTD clawback
select qtr.name
  into v_previous_qtr
  from cs_period csp
 inner join cs_period qtr
    on csp.parentseq = qtr.periodseq
   and qtr.removedate = :v_eot
   and qtr.calendarseq = :gv_calendarSeq
   and qtr.periodtypeseq = 2814749767106563 --quarter
 where csp.startdate = add_months(:v_periodStartDate, -3)
   and csp.removedate = :v_eot
   and csp.periodtypeseq = 2814749767106561 --month
   and csp.calendarseq = :gv_calendarSeq;

CALL EXT.Log('[CB] Get previous quarter for SPI FA CB' || '; row count: ' || to_varchar(::ROWCOUNT));
--sum up YTD SPI CB if one agent has more than one policy

--Deepan : Commenting this out and changing it to dynamic sql
/*merge into cs_measurement tgt
using (
select :v_tenantId as tenantid,
       cb.buname,
       cb.year,
       cb.quarter,
       cb.WRI_AGT_CODE,
       pos.ruleelementownerseq,
       pos.payeeseq,
       sum(case when cb.year = extract (year from :v_periodStartDate ) then YTD_SPI_CB --ONGONING YTD FYC CB
         else 0
        end) YTD_SPI_CB
    from EXT.AIA_CB_SPI_CLAWBACK cb
    inner join cs_position pos
           on  pos.name = 'SGT'||cb.WRI_AGT_CODE
           and pos.tenantid=:v_tenantId
           and pos.removedate=:v_eot
           and :v_periodStartDate between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)
     where (cb.quarter || ' ' || cb.year) = :v_previous_qtr
      and cb.buname = 'SGPAFA'
      group by
       cb.buname,
       cb.year,
       cb.quarter,
       cb.WRI_AGT_CODE,
       pos.ruleelementownerseq,
       pos.payeeseq
) src
on (   src.ruleelementownerseq=tgt.positionseq
   and src.payeeseq=tgt.payeeseq
   and i_periodSeq=tgt.periodseq
   and src.tenantid=tgt.tenantid
   and tgt.tenantid = :v_tenantId
     and tgt.processingunitseq = :v_processingunitseq
     and tgt.name = 'PM_SPI_ONG_YTD_CB'
   )
when matched then update
set tgt.value = YTD_SPI_CB; */
--round(YTD_SPI_CB,2) fix round issue;

v_sql := 'MERGE INTO cs_measurement AS tgt USING (' ||
         'SELECT :v_tenantId AS tenantid, ' ||
         'cb.buname, ' ||
         'cb.year, ' ||
         'cb.quarter, ' ||
         'cb.WRI_AGT_CODE, ' ||
         'pos.ruleelementownerseq, ' ||
         'pos.payeeseq, ' ||
         'SUM(CASE WHEN cb.year = EXTRACT(YEAR FROM :v_periodStartDate) THEN YTD_SPI_CB ELSE 0 END) AS YTD_SPI_CB ' ||
         'FROM EXT.AIA_CB_SPI_CLAWBACK AS cb ' ||
         'INNER JOIN cs_position AS pos ON pos.name = ''SGT'' || cb.WRI_AGT_CODE ' ||
         'AND pos.tenantid = :v_tenantId ' ||
         'AND pos.removedate = :v_eot ' ||
         'AND :v_periodStartDate BETWEEN pos.effectivestartdate AND ADD_DAYS(pos.effectiveenddate, -1) ' ||
         'WHERE (cb.quarter || '' '' || cb.year) = :v_previous_qtr ' ||
         'AND cb.buname = ''SGPAFA'' ' ||
         'GROUP BY cb.buname, cb.year, cb.quarter, cb.WRI_AGT_CODE, pos.ruleelementownerseq, pos.payeeseq' ||
         ') AS src ' ||
         'ON (src.ruleelementownerseq = tgt.positionseq ' ||
         'AND src.payeeseq = tgt.payeeseq ' ||
         'AND i_periodSeq = tgt.periodseq ' ||
         'AND src.tenantid = tgt.tenantid ' ||
         'AND tgt.tenantid = :v_tenantId ' ||
         'AND tgt.processingunitseq = :v_processingunitseq ' ||
         'AND tgt.name = ''PM_SPI_ONG_YTD_CB'') ' ||
         'WHEN MATCHED THEN UPDATE SET tgt.value = src.YTD_SPI_CB;';

CALL EXT.Log (:v_sql );
EXECUTE IMMEDIATE v_sql USING v_tenantId, v_periodStartDate, v_tenantId, v_eot, v_periodStartDate, v_previous_qtr, v_tenantId, v_processingunitseq;


CALL EXT.Log('[CB] Merge records into cs_measurement for SPI FA ONGOING YTD CB' || '; row count: ' || to_varchar(::ROWCOUNT));
--Version 12 end

--update the batch status to complete
CALL EXT.PK_AIA_CB_CALCULATION_FA:sp_update_batch_status(:v_batch_no_lumpsum_comm, :STR_STATUS_COMPLETED_SH);
CALL EXT.PK_AIA_CB_CALCULATION_FA:sp_update_batch_status(:v_batch_no_ongoing_comm, :STR_STATUS_COMPLETED_SH);

    --Added by Win Tan for version 4 **Begin**
    if to_date(:v_str_cycledate,'yyyy-mm-dd') = :v_period_end then
    CALL EXT.PK_AIA_CB_CALCULATION_FA:sp_update_batch_status(:v_batch_no_lumpsum_comp, :STR_STATUS_COMPLETED_SH);
    CALL EXT.PK_AIA_CB_CALCULATION_FA:sp_update_batch_status(:v_batch_no_ongoing_comp, :STR_STATUS_COMPLETED_SH);
    end if;
    --Added by Win Tan for version 4 **End**

   --added by Win for version 6
   --update the batch status for special handling
    update ext.aia_cb_batch_special
       set ISACTIVE = 'N'
     where batchnum = :v_batch_no_special
       and ISACTIVE = 'Y'
       and SPEC_RUN_DATE = to_date(:v_str_cycledate, 'yyyy-mm-dd')
       and BUNAME = 'SGPAFA'; --v19 add BUNAME

else

CALL EXT.LOG('[CB] ' || :v_str_cycledate || ' not exit in pipeline run period: ' || :Gv_Periodseq);

end if;

      ---catch exception
     /* EXCEPTION WHEN OTHERS
        THEN    v_message := SUBSTR(SQLERRM,1,2000);
                EXT.Log('[CB] ' || :V_MESSAGE);
                --update the batch status to fail
                EXT.PK_AIA_CB_CALCULATION:sp_update_batch_status(:v_batch_no_lumpsum_comm, :STR_STATUS_FAIL);
                EXT.PK_AIA_CB_CALCULATION:sp_update_batch_status(:v_batch_no_ongoing_comm, :STR_STATUS_FAIL);
                EXT.PK_AIA_CB_CALCULATION:sp_update_batch_status(:v_batch_no_lumpsum_comp, :STR_STATUS_FAIL);
                EXT.PK_AIA_CB_CALCULATION:sp_update_batch_status(:v_batch_no_ongoing_comp, :STR_STATUS_FAIL);

*/--Deepan : Handled at the top of the proc in exit handler
end