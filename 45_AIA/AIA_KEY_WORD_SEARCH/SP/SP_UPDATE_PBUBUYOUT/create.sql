CREATE procedure SP_UPDATE_PBUBUYOUT(in i_periodSeq bigint) 
sql security invoker 
AS
BEGIN
	DECLARE gv_error VARCHAR(1000); /* package/session variable */
    DECLARE cdt_EndOfTime date = to_date('2200-01-01','yyyy-mm-dd');
    DECLARE gv_isMonthEnd BIGINT; /* package/session variable */
    DECLARE Gv_Processingunitseq BIGINT; /* package/session variable */
    DECLARE Gv_Periodseq BIGINT; /* package/session variable */

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_periodseq BIGINT = :i_periodSeq;  /* ORIGSQL: v_periodseq int := i_periodSeq; */
    DECLARE v_periodStartDate TIMESTAMP;  /* ORIGSQL: v_periodStartDate date; */
    DECLARE v_periodEndDate TIMESTAMP;  /* ORIGSQL: v_periodEndDate date; */
    DECLARE v_pirorYearStartDate TIMESTAMP;  /* ORIGSQL: v_pirorYearStartDate Date; */
    DECLARE v_pirorYearEndDate TIMESTAMP;  /* ORIGSQL: v_pirorYearEndDate date; */
    DECLARE v_calendarSeq BIGINT;  /* ORIGSQL: v_calendarSeq int; */
    DECLARE v_periodTypeSeq BIGINT;  /* ORIGSQL: v_periodTypeSeq int; */
    DECLARE v_startPeriodSeq BIGINT;  /* ORIGSQL: v_startPeriodSeq int; */
    DECLARE V_Temp BIGINT;  /* ORIGSQL: V_Temp Int; */
    DECLARE V_Defaultdate date = to_date('1/1/1900', 'mm/dd/yyyy');  /* ORIGSQL: V_Defaultdate Date := To_Date('1/1/1900', 'mm/dd/yyyy') ; */

    DECLARE v_componentValue VARCHAR(30) = 'PBU_PM';  /* ORIGSQL: v_componentValue VARCHAR2(30) := 'PBU_PM'; */
    DECLARE V_Credittypeseq_FYC DECIMAL(38,10);  /* ORIGSQL: V_Credittypeseq_FYC number; */
    DECLARE V_Credittypeseq_API DECIMAL(38,10);  /* ORIGSQL: V_Credittypeseq_API number; */
    DECLARE V_Credittypeseq_SSCP DECIMAL(38,10);  /* ORIGSQL: V_Credittypeseq_SSCP number; */
    DECLARE V_Credittypeseq_H_PIB DECIMAL(38,10);  /* ORIGSQL: V_Credittypeseq_H_PIB number; */

    -- for revamp begin
    DECLARE V_Credittypeseq_RPI DECIMAL(38,10);  /* ORIGSQL: V_Credittypeseq_RPI number; */
    DECLARE V_Credittypeseq_FYC_W DECIMAL(38,10);  /* ORIGSQL: V_Credittypeseq_FYC_W number; */
    DECLARE V_Credittypeseq_FYC_W_DUP DECIMAL(38,10);  /* ORIGSQL: V_Credittypeseq_FYC_W_DUP number; */
    DECLARE V_Credittypeseq_FYC_WC_DUP DECIMAL(38,10);  /* ORIGSQL: V_Credittypeseq_FYC_WC_DUP number; */
    DECLARE V_Credittypeseq_API_W DECIMAL(38,10);  /* ORIGSQL: V_Credittypeseq_API_W number; */
    DECLARE V_Credittypeseq_API_W_DUP DECIMAL(38,10);  /* ORIGSQL: V_Credittypeseq_API_W_DUP number; */
    DECLARE V_Credittypeseq_API_WC_DUP DECIMAL(38,10);  /* ORIGSQL: V_Credittypeseq_API_WC_DUP number; */
    DECLARE V_Credittypeseq_SSCP_W DECIMAL(38,10);  /* ORIGSQL: V_Credittypeseq_SSCP_W number; */
    DECLARE V_Credittypeseq_SSCP_W_DUP DECIMAL(38,10);  /* ORIGSQL: V_Credittypeseq_SSCP_W_DUP number; */
    DECLARE V_Credittypeseq_SSCP_WC_DUP DECIMAL(38,10);  /* ORIGSQL: V_Credittypeseq_SSCP_WC_DUP number; */

    --for revamp end
    --v23 MAS86 PBU Lumpsum start
    DECLARE V_Credittypeseq_FYC_TPGI DECIMAL(38,10);  /* ORIGSQL: V_Credittypeseq_FYC_TPGI number; */

    --v23 MAS86 PBU Lumpsum end
    DECLARE v_PromoteeCount DECIMAL(38,10) = 0;  /* ORIGSQL: v_PromoteeCount number := 0; */
	declare v_tenantid varchar(25);
DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            gv_error = 'Error [SP_UPDATE_PBUBUYOUT]: ' || ::SQL_ERROR_MESSAGE;  /* ORIGSQL: sqlerrm */

            /* RESOLVE: Standard Package call(not converted): 'dbms_utility.format_error_backtrace' not supported, manual conversion required */
            /*dbms_utility.format_error_backtrace;*/
            /* Saving modified package/session variable 'gv_error': */ 
            SET SESSION 'GV_ERROR' = :gv_error;
            ROLLBACK;
        END;

		SELECT SESSION_CONTEXT('GV_ERROR') INTO gv_error FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_ISMONTHEND') AS BIGINT) INTO gv_isMonthEnd FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_PROCESSINGUNITSEQ') AS BIGINT) INTO Gv_Processingunitseq FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_PERIODSEQ') AS BIGINT) INTO Gv_Periodseq FROM SYS.DUMMY ;
   

select tenantid into v_tenantid from cs_tenant;
	  
    --comDebugger('SP_UPDATE_PBUBUYOUT','StageHook start for period ['||i_periodSeq||']'||'at ['||to_timestamp(sysdate)||']');

    /* TODO implementation required */

    ---clean up temp table
    /*delete from sh_query_result
    where component='PBU_PM';*/

/*    comInitialPartition('PBU', v_componentValue, i_periodSeq);*/

    execute immediate 'truncate table sh_sequence';

    --DBMS_OUTPUT.put_line('start pbu------------');

    ----get period info.

    select startDate,
           endDate,
           add_months(startDate, -12) as priorYearStartDate,
           add_months(endDate, -12) as priorYearEtartDate,
           periodTypeSeq,
           calendarSeq
      into v_periodStartDate,
           v_periodEndDate,
           v_pirorYearStartDate,
           v_pirorYearEndDate,
           v_periodTypeSeq,
           v_calendarSeq
      from cs_period
     where tenantid=v_tenantid and periodseq = v_periodseq
       and removeDate = cdt_EndOfTime;

    /*DBMS_OUTPUT.put_line('start pbu: periodSeq'||v_pirorYearStartDate||'--start period:'||v_pirorYearEndDate||'--type'
    ||v_periodTypeSeq||'--clendar'||v_calendarSeq);*/

    ---get start periodSeq
    select periodSeq
      into v_startPeriodSeq
      from cs_period
     where tenantid=v_tenantid and startDate = v_pirorYearStartDate
       and endDate = v_pirorYearEndDate
       and periodtypeSeq = v_periodTypeSeq
       and calendarSeq = v_calendarSeq
       and removeDate = cdt_EndOfTime;

    -- DBMS_OUTPUT.put_line('start pbu: periodSeq'||v_periodSeq||'--start period:'||v_startPeriodSeq);

    --Maintenance.Enablepdml;

    comDebugger('SQL Performance','Stagehook[SP_UPDATE_PBUBUYOUT]-SQL1 START:' || current_date);

    V_Credittypeseq_FYC   := Comgetcredittypeseq('FYC');
    V_Credittypeseq_API   := Comgetcredittypeseq('API');
    V_Credittypeseq_SSCP  := Comgetcredittypeseq('SSCP');
    V_Credittypeseq_H_PIB := Comgetcredittypeseq('H_PIB');
    --for revamp begin
    V_Credittypeseq_RPI         := Comgetcredittypeseq('RPI');
    V_Credittypeseq_FYC_W       := Comgetcredittypeseq('FYC_W');
    V_Credittypeseq_FYC_W_DUP   := Comgetcredittypeseq('FYC_W_DUPLICATE');
    V_Credittypeseq_FYC_WC_DUP  := Comgetcredittypeseq('FYC_WC_DUPLICATE');
    V_Credittypeseq_API_W       := Comgetcredittypeseq('API_W');
    V_Credittypeseq_API_W_DUP   := Comgetcredittypeseq('API_W_DUPLICATE');
    V_Credittypeseq_API_WC_DUP  := Comgetcredittypeseq('API_WC_DUPLICATE');
    V_Credittypeseq_SSCP_W      := Comgetcredittypeseq('SSCP_W');
    V_Credittypeseq_SSCP_W_DUP  := Comgetcredittypeseq('SSCP_W_DUPLICATE');
    V_Credittypeseq_SSCP_WC_DUP := Comgetcredittypeseq('SSCP_WC_DUPLICATE');
    --for revamp end
    --V23 For MAS86 PBU
    V_Credittypeseq_FYC_TPGI    := Comgetcredittypeseq('FYC_TPGI_DIRECT');    

    -- not going to change
    --vParName := segmentationutils.segmentname('CS_SalesTransaction', pProcessingUnitSeq, vPeriod.endDate);

    select value into gv_isMonthEnd from cs_fixedValue
       where name = 'FV_Mo_End'
         And Removedate = to_date('22000101','YYYYMMDD')
         And Effectivestartdate <= v_periodStartDate
         and effectiveEndDate > v_periodStartDate;
        
SET SESSION 'ISMONTHEND' = CAST(:gv_isMonthEnd AS VARCHAR(512));

    if gv_isMonthEnd = 1 then
        Log('12');
        ----get eligible credit

        --v17 start
        execute immediate 'truncate table SH_PBU_TMP_DATAPARTICIPANT';

        insert into SH_PBU_TMP_DATAPARTICIPANT
        select pa.userid, pa.payeeseq, gpa.GENERICBOOLEAN2, gpa.genericAttribute6 from cs_participant pa, cs_gaparticipant gpa
        where pa.tenantid = v_tenantid
                  and gpa.tenantid = v_tenantid
                  and pa.removeDate = cdt_EndOfTime
                  and v_periodEndDate >= pa.effectiveStartDate
                  and v_periodEndDate < pa.effectiveEndDate
                  and gpa.removeDate = cdt_EndOfTime
                  and pa.effectiveStartDate = gpa.effectiveStartDate
                  and pa.payeeseq = gpa.payeeseq
                  and gpa.pagenumber = 0
                  ;

        execute immediate 'truncate table SH_PBU_TMP_DATAPOSITION';

        insert into SH_PBU_TMP_DATAPOSITION
        select
                  cp.ruleElementOwnerSeq,
                  cp.Name,
                  cp.Genericattribute2 as ga2, --leader code
                  cp.GENERICATTRIBUTE3 as ga3, --disctrict code
                  cp.effectiveStartDate,
                  cp.effectiveEndDate,
                  cp.removeDate,
                  cgp.genericDate5 as ed5,  --leader's promotion date
                  cgp.genericDate11 as ed11, --leader's Prev_Demotion_date
                  tt.name as title_name,
                  pa.genericAttribute6 as pa_ea6  --AIAS unit code for migrated unit
                  from cs_position cp 
                  join cs_title tt on (cp.titleseq = tt.ruleelementownerseq)
                  join sh_pbu_tmp_dataparticipant pa on ('SGT' || cp.Genericattribute2 = pa.USERID)
                  left join cs_gaposition cgp on ( cgp.tenantid = v_tenantid and cgp.pageNumber = 0
                  and cgp.ruleElementOwnerSeq = cp.ruleElementOwnerSeq
                  and cgp.removeDate = cdt_EndOfTime
                  and cgp.effectiveStartDate = cp.effectivestartdate)
                  where
                  cp.tenantid = v_tenantid
                  and cp.name like 'SGY%'
                  and v_periodEndDate >= cp.effectiveStartDate
                  and v_periodEndDate < cp.effectiveEndDate
                  and cp.removeDate = cdt_EndOfTime
                  and tt.removeDate = cdt_EndOfTime
                  and v_periodEndDate >= tt.effectiveStartDate
                  and v_periodEndDate < tt.effectiveEndDate
                  and 'SGT' || cp.Genericattribute2 = pa.USERID
                  and ((cp.Genericattribute6 = 'AGY')   --AIAS
                       or (cp.Genericattribute6 = 'AFA' and pa.GENERICBOOLEAN2 =1)) --FA 2.1
                       ;

        execute immediate 'truncate table SH_PBU_TMP_ALLUNIT';

        insert into SH_PBU_TMP_ALLUNIT
        select    ed5,
                       ed11,
                       districtName,
                       unitName,
                       pa_ea6,
                       ruleElementOwnerSeq
                  from (Select newFSD.ed5                     as ed5, --New FSD's promotion date
                                newFSD.ed11                    as ed11, --New FSD's Prev_Demotion_Date
                                newFSD.Name                    as districtName, --District position name
                                newFSDUnit.NAME                as unitName, --Unit position name
                                newFSDUnit.pa_ea6              as pa_ea6, --Unit old AIAS's code
                                newFSDUnit.ruleElementOwnerSeq as ruleElementOwnerSeq
                           from SH_PBU_TMP_DATAPOSITION newFSDUnit -- direct and indirect units for FSD/ED that promoted in current month,
                          inner join SH_PBU_TMP_DATAPOSITION newFSD
                             on newFSD.name = 'SGY' || newFSDUnit.ga3 --direact unit
                          where newFSD.ed5 >= v_periodStartDate
                            and newFSD.ed5 <  v_periodEndDate
                            and newFSD.title_name in ('DISTRICT', 'ORGANISATION')) newUnit
                 UNION ALL
                 select ed5,
                        ed11,
                        districtName,
                        unitName,
                        pa_ea6,
                        ruleElementOwnerSeq
                   from (Select newFSD.ed5                  as ed5, --New FSD's promotion date
                               newFSD.ed11                 as ed11, --New FSD's Prev_Demotion_Date
                                 newFSD.Name                 as districtName, --District position name
                                 oldUnit.NAME                as unitName, --Unit position name
                                 oldUnit.pa_ea6              as pa_ea6, --Unit old AIAS's code
                               oldUnit.ruleElementOwnerSeq as ruleElementOwnerSeq
                          from SH_PBU_TMP_DATAPOSITION oldUnit -- direct/indirect units before migrated for ED promoted in current month.
                         inner join SH_PBU_TMP_DATAPOSITION newUnit
                            on oldUnit.name = 'SGY' || newUnit.pa_ea6
                         inner join SH_PBU_TMP_DATAPOSITION newFSD
                            on newFSD.name = 'SGY' || newUnit.ga3
                         where newFSD.ed5 >= v_periodStartDate
                           and newFSD.ed5 < v_periodEndDate
                           and newFSD.title_name in ('DISTRICT', 'ORGANISATION')) migratedUnit
                        ;

            commit;

            select count(*) into v_PromoteeCount from SH_PBU_TMP_ALLUNIT;

            if v_PromoteeCount > 0 then

        -- credittypeseq index
        Insert 
        Into Sh_Query_Result
          (Component,
           Objoutputname,
           Payeeseq,
           Positionseq,
           Periodseq,
           value,
           genericSequence1,
           genericSequence2,
           Genericdate1,  --New FSD's promotion date
           Genericdate2,  --New FSD's Prev Demotion_Date
           Genericdate3,  --Agenct's transfer date

         --version 15 begin
         GENERICATTRIBUTE1,  --Nev FSD's position name
         GENERICATTRIBUTE2,   --Unit agency code
         GENERICATTRIBUTE3   --Unit old agency code
         --version 15 end
         )
         --version 15 begin
         /*with data_position as
         (select
            cp.ruleElementOwnerSeq,
          cp.Name,
          cp.Genericattribute2 as ga2, --leader code
          cp.GENERICATTRIBUTE3 as ga3, --disctrict code
          cp.effectiveStartDate,
          cp.effectiveEndDate,
          cp.removeDate,
          cgp.genericDate5 as ed5,  --leader's promotion date
          cgp.genericDate11 as ed11, --leader's Prev_Demotion_date
          tt.name as title_name,
          gpa.genericAttribute6 as pa_ea6  --AIAS unit code for migrated unit
          from cs_position cp, cs_gaposition cgp, cs_title tt, cs_participant pa, cs_gaparticipant gpa
          where
          cp.tenantid = v_tenantid
          and cgp.tenantid = v_tenantid
          and pa.tenantid = v_tenantid
          and gpa.tenantid = v_tenantid
          and cp.name like 'SGY%'
          and v_periodEndDate >= cp.effectiveStartDate
          and v_periodEndDate < cp.effectiveEndDate
          and cp.removeDate = cdt_EndOfTime
          and cgp.pageNumber(+) = 0
          and cgp.ruleElementOwnerSeq(+) = cp.ruleElementOwnerSeq
          and cgp.removeDate(+) = cdt_EndOfTime
          and cgp.effectiveStartDate(+) <= v_periodEndDate
          and cgp.effectiveEndDate(+) > v_periodEndDate
          and cp.titleseq = tt.ruleelementownerseq
          and tt.removeDate = cdt_EndOfTime
          and v_periodEndDate >= tt.effectiveStartDate
          and v_periodEndDate < tt.effectiveEndDate
          and pa.removeDate = cdt_EndOfTime
          and v_periodEndDate >= pa.effectiveStartDate
          and v_periodEndDate < pa.effectiveEndDate
          and 'SGT' || cp.Genericattribute2 = pa.USERID
          and gpa.removeDate = cdt_EndOfTime
          and v_periodEndDate >= gpa.effectiveStartDate
          and v_periodEndDate < gpa.effectiveEndDate
          and pa.payeeseq = gpa.payeeseq
          and ((cp.Genericattribute6 = 'AGY')   --AIAS
               or (cp.Genericattribute6 = 'AFA'and gpa.GENERICBOOLEAN2 =1)) --FA 2.1

         ),
         allUnit as (
         select    ed5,
                       ed11,
                       districtName,
                       unitName,
                       pa_ea6,
                       ruleElementOwnerSeq
                  from (Select newFSD.ed5                     as ed5, --New FSD's promotion date
                                newFSD.ed11                    as ed11, --New FSD's Prev_Demotion_Date
                                newFSD.Name                    as districtName, --District position name
                                newFSDUnit.NAME                as unitName, --Unit position name
                                newFSDUnit.pa_ea6              as pa_ea6, --Unit old AIAS's code
                                newFSDUnit.ruleElementOwnerSeq as ruleElementOwnerSeq
                           from data_position newFSDUnit -- direct and indirect units for FSD/ED that promoted in current month,
                          inner join data_position newFSD
                             on newFSD.name = 'SGY' || newFSDUnit.ga3 --direact unit
                          where newFSD.ed5 >= v_periodStartDate
                            and newFSD.ed5 <  v_periodEndDate
                            and newFSD.title_name in ('DISTRICT', 'ORGANISATION')) newUnit
                 UNION ALL
                 select ed5,
                        ed11,
                        districtName,
                        unitName,
                        pa_ea6,
                        ruleElementOwnerSeq
                   from (Select newFSD.ed5                  as ed5, --New FSD's promotion date
                               newFSD.ed11                 as ed11, --New FSD's Prev_Demotion_Date
                                 newFSD.Name                 as districtName, --District position name
                                 oldUnit.NAME                as unitName, --Unit position name
                                 oldUnit.pa_ea6              as pa_ea6, --Unit old AIAS's code
                               oldUnit.ruleElementOwnerSeq as ruleElementOwnerSeq
                          from data_position oldUnit -- direct/indirect units before migrated for ED promoted in current month.
                         inner join data_position newUnit
                            on oldUnit.name = 'SGY' || newUnit.pa_ea6
                         inner join data_position newFSD
                            on newFSD.name = 'SGY' || newUnit.ga3
                         where newFSD.ed5 >= v_periodStartDate
                           and newFSD.ed5 < v_periodEndDate
                           and newFSD.title_name in ('DISTRICT', 'ORGANISATION')) migratedUnit

         ) */
         --version 15 end
          Select 
                 v_componentValue,
                 'PM_PBU_Buyout',
                 cc.payeeSeq,
                 cc.positionSeq,
                 v_periodSeq,
                 cc.value,
                 cc.creditSeq,
                 cc.periodSeq,
           --version 15 begin
                 --cgp.genericDate5,
                 --cgp.genericDate11,
           allUnit.ed5, --New FSD's promotion date
           allUnit.ed11, --New FSD's Prev_Demotion_Date
           cc.genericDate4, --Agent's transfer date
           allUnit.districtName, --District code
           allUnit.unitName, --Unit code
           allUnit.pa_ea6 --Unit old code
           --version 15 end
            from cs_credit cc,
        --version 15 begin
          --cs_position cp,
          --cs_gaposition cgp
            SH_PBU_TMP_ALLUNIT allUnit
          --version 15 end
           Where cc.tenantid=v_tenantid
         --version 15 begin
             --and cp.tenantid=v_tenantid
             --and cgp.tenantid=v_tenantid
         --And Cp.Name Like 'SGY%'
         --and cp.removeDate = cdt_EndOfTime
         --and v_periodEndDate >= cp.effectiveStartDate
             --and v_periodEndDate < cp.effectiveEndDate
         --and cgp.genericDate5 >= v_periodStartDate
             --and cgp.genericDate5 < v_periodEndDate
         --and cp.ruleElementOwnerSeq = cc.positionSeq

         --version 15 end

             and cc.processingUnitSeq = Gv_Processingunitseq
             and cc.periodseq >= v_startPeriodSeq
             and cc.periodSeq < v_periodSeq
             and cc.origintypeid in ('calculated', 'imported')
             and cc.genericAttribute14 <> '12'
             and ifnull(cc.genericDate4, v_defaultDate) <         --version 15 begin
                 --add_months(cgp.genericDate5, -36)
           add_months(allUnit.ed5, -36)
           --version 15 end
             And ((cc.credittypeseq = V_Credittypeseq_FYC And -- old policy issued before 12/1/2015, follow the old logic
                 Cc.Genericattribute2 In ('PA', 'CS', 'CL', 'HS', 'LF') And
                 cc.genericdate2 < to_date('12/1/2015', 'mm/dd/yyyy')) or
                 (cc.credittypeseq = V_Credittypeseq_FYC And -- new policy issued after 12/1/2015, CS credit will follow commission agent/agency, gb5 = false
                 Cc.Genericattribute2 In ('CS', 'CL') And
                 cc.genericdate2 >= to_date('12/1/2015', 'mm/dd/yyyy')) or
                 (cc.credittypeseq IN
                 (V_Credittypeseq_FYC_W,
                    V_Credittypeseq_FYC_W_DUP,
                    V_Credittypeseq_FYC_WC_DUP) And -- new policy issued after 12/1/2015, Life/PA credit will follow writing agent/agency, gb5 = true
                 Cc.Genericattribute2 In ('PA', 'HS', 'LF') And
                 cc.genericdate2 >= to_date('12/1/2015', 'mm/dd/yyyy')) or --V23 MAS86 PBU lumpsum
     (cc.credittypeseq = V_Credittypeseq_FYC_TPGI And Cc.Genericattribute1 In ('TPAH', 'TPCS')) or 
                 (cc.creditTypeseq in
                 (V_Credittypeseq_API,
                    V_Credittypeseq_SSCP,
                    V_Credittypeseq_H_PIB,
                    V_Credittypeseq_API_W,
                    V_Credittypeseq_API_W_DUP,
                    V_Credittypeseq_API_WC_DUP,
                    V_Credittypeseq_SSCP_W,
                    V_Credittypeseq_SSCP_W_DUP,
                    V_Credittypeseq_SSCP_WC_DUP))
                 )
        --version 15 begin
             --and cgp.pageNumber(+) = 0
             --and cgp.ruleElementOwnerSeq(+) = cc.positionseq
             --and cgp.removeDate(+) = cdt_EndofTime
             --and cgp.effectiveStartDate(+) <= v_periodEndDate
             --and cgp.effectiveEndDate(+) > v_periodEndDate
             --and ((cgp.genericDate11) is null or
                 --cgp.genericDate11 <= add_months(cgp.genericDate5, -60));
        and allUnit.ruleElementOwnerSeq = cc.positionseq
        and ((allUnit.ed11) is null or
        allUnit.ed11 <= add_months(allUnit.ed5, -60));
        --version 15 end

        commit;

        Log('12');
        ----------for PBU special handling of Nov 27th issue.
        delete from Sh_Query_Result t
         where exists(with x as (select sqr.periodseq,
                                  sqr.genericsequence1,
                                  sqr.genericsequence2
                             from Sh_Query_Result sqr
                            inner join cs_credit c
                               on sqr.genericsequence1 = c.creditseq
                              and sqr.genericsequence2 = c.periodseq
                            inner join PBU_Buyout_TxnExcl PBT
                               on c.salestransactionseq =
                                  PBT.salestransactionseq
                              and c.periodseq = PBT.periodseq
                              and c.tenantid = v_tenantid
                              and c.processingUnitSeq = gv_processingUnitSeq
                            where sqr.component = 'PBU_PM'  and sqr.periodseq = v_periodSeq
                                 )
           select 1
             from x
            where t.periodseq = x.periodseq
              and t.genericsequence1 = x.genericsequence1
              and t.genericsequence2 = x.genericsequence2) 
              and component = 'PBU_PM'  and t.periodseq = v_periodSeq;

        commit;
        Log('13');

        insert into sh_sequence
        select genericSequence1,
                 'CREDITSEQ',
                 payeeSeq,
                 positionSeq,
                 periodSeq
            from sh_query_result
           Where Component = V_Componentvalue
             and periodSeq = gv_periodseq;

        commit;
        Log('13');

        Log('14');

        execute immediate 'Update Cs_Credit t
           Set Genericboolean4 = 1
         Where tenantid='''||v_tenantid||''' and Processingunitseq = '||Gv_Processingunitseq||'
           and T.Creditseq in (Select Sh.Businessseq
                                 From Sh_Sequence Sh
                                Where Sh.Seqtype = ''CREDITSEQ'')';

        commit;
        Log('14');

        Log('15');

        insert into sh_sequence
          select distinct cm.measurementSeq,
                          'PMSEQ',
                          cm.payeeSeq,
                          cm.positionSeq,
                          cm.periodSeq
            from cs_measurement cm, sh_query_result r
           where cm.tenantid=v_tenantid and R.Component = V_Componentvalue
             and r.periodseq = gv_periodseq
             and cm.name = r.objOutputName
             and cm.payeeSeq = r.payeeSeq
             and cm.positionSeq = r.positionSeq
             And Cm.Periodseq = R.Periodseq
             and cm.processingunitseq = gv_processingunitseq;

        commit;
        Log('15');

        --DBMS_OUTPUT.put_line('save pmseq');

        -----update pm

        Log('16');

       execute immediate 'merge
        into cs_measurement cm
        using (select objOutputName,
                      payeeSeq,
                      positionSeq,
                      periodSeq,
                      sum(value) aggValue,
                      count(*) numberOfCredits
                 from sh_query_result
                where component = '''||v_componentValue||'''
                  and periodSeq = '||gv_periodseq||'
                group by objOutputName, payeeSeq, positionSeq, periodSeq) r
        on (cm.tenantid='''||v_tenantid||''' and cm.payeeseq = r.payeeseq
            and cm.positionseq = r.positionseq
            and cm.periodSeq = r.periodSeq
            and cm.name = r.objOutputName
            and exists (select 1 from sh_sequence where businessSeq = cm.measurementSeq and seqType = ''PMSEQ''))
        when matched then
          update
             set cm.value = r.aggValue, cm.numberOfCredits = r.numberOfCredits';

        commit;
        Log('16');


        Log('17');
        comUpdPMCreditTrace('SP_UPDATE_PBUBUYOUT');
        Log('17');
        else
            Log('12 - no promotee found');
        end if;    else
        Log('12 - Not month end');
    end if;

   END --SP_UPDATE_PBUBUYOUT;