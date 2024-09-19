/*
 * This file was extracted from 'C:/HANAMigrations/AIASG/OracleObjects/ext_DDL.sql' 
 * at 05-Jun-2024 11:41:58 with the 'extract_offline' command of SAP Advanced SQL Migration v.3.5.3 (64791)
 * User config setting for 'extract_offline' (id=132) was '0'.
 */


CREATE OR REPLACE  PACKAGE BODY AIASEXT.PK_PIAOR_CALCULATION is

  /*
************************************************
Version     Create By       Create Date   Change
************************************************
1           Callidus         20150407    Production version
2           Endi             20160229    Added query hint to SP_TXA_PIAOR procedure - see Log('40')
5           Jeff             20170217    PIAOR enhancement
8           Sammi            20180824    Rewrite PIAOR,logic copy from callidus rules and pakeage stage hook
9           Sammi            20181206    add breakdown by Line of business for PI and AOR detail calculation
10          Sammi            20190102    SP_TXA_PIAOR long run tuning
11          Sammi            20200110    Fix per_limra dupicate data
*/
  gv_error                  varchar2(1000);
  Gv_Processingunitseq      Int := 38280596832649218;
  Gv_Periodseq              Int := 2533274790398900; --last period  November 2014
  gv_calendarSeq            int := 2251799813685250;
  gv_plStartTime            timestamp;
  gv_isYearEnd              int := 0;
  Gv_Pipelinerunseq         Int := 0;
  gv_CrossoverEffectiveDate date;
  gv_CYCLE_DATE             VARCHAR2(10);

  --for revamp end
  Gv_Setnumberpi    Int := 3;
  gv_setnumberaor   Int := 4;


  --for revamp end

  gv_hryc int := 16607023625930577; --- must be defined here for testing, otherwise, HRYC will be impact

  procedure init as

  begin

  --setup processing unit seq number
  select processingunitseq into Gv_Processingunitseq from cs_processingunit where name = STR_PU;

  --setup calendar seq number
  select CALENDARSEQ into gv_calendarSeq from cs_calendar where name = STR_CALENDARNAME;

  --get current cycle date
  SELECT CTL.TXT_KEY_VALUE INTO gv_CYCLE_DATE FROM IN_ETL_CONTROL CTL WHERE CTL.TXT_KEY_STRING='OPER_CYCLE_DATE';



  end init;

  PROCEDURE init(P_STR_CYCLEDATE in VARCHAR2) as

  begin

  --setup processing unit seq number
  select processingunitseq into Gv_Processingunitseq from cs_processingunit where name = STR_PU;

  --setup calendar seq number
  select CALENDARSEQ into gv_calendarSeq from cs_calendar where name = STR_CALENDARNAME;

  --get current cycle date
  gv_CYCLE_DATE := P_STR_CYCLEDATE;



  end init;


  procedure Log(inText varchar2) is
    pragma autonomous_transaction;
    vText varchar2(4000);
  begin
    vText := substr(inText, 1, 4000);

    insert into CS_PIAOR_DEBUG (text, value) values ('PIAOR_' || vText, 1);
    commit;
    dbms_output.put_line('PIAOR_' || vText);
  exception
    when others then
      rollback;
      raise;
  end Log;

  procedure comDebugger(i_objName in varchar2, i_objContent in varchar2) is

  pragma autonomous_transaction;

  begin

  insert into sh_debugger values (i_objName, sysdate, i_objContent);
  commit;

  end comDebugger;


  procedure SP_TXA_PIAOR as
     v_periodSeq int;
     v_periodStartDate date;
     v_periodEndDate date;
     v_cutOverDate date;
     v_positionName varchar2(100);
     V_Crossoverflag Int:=0;
     v_ConstantCrossoverDate date;
     v_AgyDistTrxn R_AgyDistTrxn;
     v_ga13 varchar2(30);
     V_Componentvalue_Pi Varchar2(10):='PI';
     v_componentValue_aor varchar2(10):='AOR';
     V_HRYCSEQ int;
     vSQL varchar2(4000);
  begin

  log('SP_TXA_PIAOR: start');

  --get period startDate, endDate

  log('gv_CYCLE_DATE: '||gv_CYCLE_DATE);
  log('gv_calendarSeq: '||gv_calendarSeq);

  select cp.PERIODSEQ,cp.startDate,cp.endDate
    into v_periodSeq,v_periodStartDate,v_periodEndDate
    from CS_PERIOD cp,
       cs_periodtype pt
   where cp.tenantid='AIAS'
     and cp.REMOVEDATE=cdt_EndOfTime
     and cp.CALENDARSEQ=gv_calendarSeq
     and cp.startdate<=to_date(gv_CYCLE_DATE,'yyyy-mm-dd')
     and cp.enddate>to_date(gv_CYCLE_DATE,'yyyy-mm-dd')
     and pt.name = 'month'
     and pt.periodtypeseq=cp.periodtypeseq
;

log('v_periodSeq: ' ||v_periodSeq);
log('v_periodStartDate: '||v_periodStartDate);
log('v_periodEndDate: '||v_periodEndDate);



    Gv_Periodseq := v_periodSeq;


--version 8 init piaor assignment
Log('1 Init PIAOR assignment');
  AssignmentInitialpartition(v_periodSeq);

execute immediate ('alter index PIAOR_ASSIGNMENT_PK rebuild parallel nologging' );

log('1 rebuild index PIAOR_ASSIGNMENT_PK done');


Log('2 Pre Call init partition PI');
    comInitialPartition('PI',v_componentValue_pi,v_periodSeq);
Log('3 Pre Call init partition AOR');
    comInitialPartition('AOR',v_componentValue_aor,v_periodSeq);
commit;

/*Arjun adding this below*/

Log('4 Pre Build index Start');
    execute immediate ('alter index SH_QUERY_RESULT_IDX2 rebuild parallel nologging' );
Log('5 Pre Build index Done');
    --cut over date to determine what position table need to be used
    --before cutoverdate, use AIA tbl_agent_move
    --after/on cutoverdate, use cs_position
    begin
      select NVL(refDateValue,to_date('1/1/2000','mm/dd/yyyy'))
      into v_cutOverDate --12/31/2014
      From Sh_Reference
      Where Refid='CUTOVERDATE';
    Exception WHEN NO_DATA_FOUND Then
      v_cutOverDate:=to_date('11/30/2013','mm/dd/yyyy');
    end;

    begin
      select NVL(refDateValue,to_date('1/1/2000','mm/dd/yyyy'))
      into v_ConstantCrossoverDate--4/1/2006
      From Sh_Reference
      Where Refid='CUTOVERISSUEDATE';
    Exception WHEN NO_DATA_FOUND Then
      v_ConstantCrossoverDate:=to_date('1/1/2005','mm/dd/yyyy');
    end;




  --version 8 get FA AGY relation
  execute immediate 'truncate table AIA_FA_AGY_RELA_TMP';

  INSERT INTO AIA_FA_AGY_RELA_TMP
    select ce.payeeid as FA_agent, --has prefix example SGT /SGY
           cg.genericattribute5 as AGY_agent, --no prefix
           cg.genericattribute6 as AGY_agency --on prefix
      from CS_PAYEE ce,
           Cs_Gaparticipant cg
    where  ce.payeeseq=cg.payeeseq
       and ce.islast=1
       and cg.pagenumber=0
       and cg.effectivestartdate>=ce.effectivestartdate
       and cg.effectiveenddate<=ce.effectiveenddate
       and ce.removedate=cdt_endoftime
       and cg.removedate=cdt_endoftime
       and cg.genericattribute5 is not null
       and ce.effectivestartdate<=v_periodEndDate-1
       and ce.effectiveenddate>=v_periodEndDate-1
   ;


log('6 Get FA corresponding AGY old code: ' ||SQL%ROWCOUNT);

COMMIT;





      /*
    the piaor store proc is seperated into 2 portion,
    1st for policy issue before cutover date, need to look into aia custom table
    2nd for policy issue on/after cutover date, need look into ODS position tables
    */
Log('7 Pre Call comConvertAgentRole');

    --look into aia customer table begin
   -- comConvertAgentRole(i_periodSeq);

    comDebugger('SQL Performance','PIAOR_Calculation[SP_TXA_PIAOR]-SQL1 START:' ||SYSDATE);

--step1, look into aia custom table with txn.GA12+13+issuedate




--    Log('30');

---------------------------------genericAttribute17 equal O series and Genericdate2 before V_Cutoverdate----------------------------

   --if genericAttribute17=o,mean the writing code equal the commision code?
   --policy issue before cutover date, need to look into aia custom table
   --transaction store the current agent/agency information
   --look the old version from sh_agent_role or cs_position,use the condition for example issue data or compensation date between effstartdate and effenddate

    execute immediate 'truncate table aia_x';
    insert /*+ APPEND */
    into aia_x
        select /*+    */ st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, 
            st.BILLTOADDRESSSEQ, st.SHIPTOADDRESSSEQ, st.OTHERTOADDRESSSEQ, st.ISRUNNABLE, st.BUSINESSUNITMAP, st.ACCOUNTINGDATE, st.PRODUCTID, st.PRODUCTNAME, st.PRODUCTDESCRIPTION, 
            st.NUMBEROFUNITS, st.UNITVALUE, st.UNITTYPEFORUNITVALUE, st.PREADJUSTEDVALUE, st.UNITTYPEFORPREADJUSTEDVALUE, st.VALUE, st.UNITTYPEFORVALUE, st.NATIVECURRENCY, st.NATIVECURRENCYAMOUNT, 
            st.DISCOUNTPERCENT, st.DISCOUNTTYPE, st.PAYMENTTERMS, st.PONUMBER, st.CHANNEL, st.ALTERNATEORDERNUMBER, st.DATASOURCE, st.REASONSEQ, st.COMMENTS, st.GENERICATTRIBUTE1, 
            st.GENERICATTRIBUTE2, st.GENERICATTRIBUTE3, st.GENERICATTRIBUTE4, st.GENERICATTRIBUTE5, st.GENERICATTRIBUTE6, st.GENERICATTRIBUTE7, st.GENERICATTRIBUTE8, st.GENERICATTRIBUTE9, 
            st.GENERICATTRIBUTE10, st.GENERICATTRIBUTE11, st.GENERICATTRIBUTE12, st.GENERICATTRIBUTE13, st.GENERICATTRIBUTE14, st.GENERICATTRIBUTE15, st.GENERICATTRIBUTE16, st.GENERICATTRIBUTE17, 
            st.GENERICATTRIBUTE18, st.GENERICATTRIBUTE19, st.GENERICATTRIBUTE20, st.GENERICATTRIBUTE21, st.GENERICATTRIBUTE22, st.GENERICATTRIBUTE23, st.GENERICATTRIBUTE24, st.GENERICATTRIBUTE25, 
            st.GENERICATTRIBUTE26, st.GENERICATTRIBUTE27, st.GENERICATTRIBUTE28, st.GENERICATTRIBUTE29, st.GENERICATTRIBUTE30, st.GENERICATTRIBUTE31, st.GENERICATTRIBUTE32, st.GENERICNUMBER1, 
            st.UNITTYPEFORGENERICNUMBER1, st.GENERICNUMBER2, st.UNITTYPEFORGENERICNUMBER2, st.GENERICNUMBER3, st.UNITTYPEFORGENERICNUMBER3, st.GENERICNUMBER4, st.UNITTYPEFORGENERICNUMBER4, 
            st.GENERICNUMBER5, st.UNITTYPEFORGENERICNUMBER5, st.GENERICNUMBER6, st.UNITTYPEFORGENERICNUMBER6, st.GENERICDATE1, st.GENERICDATE2, st.GENERICDATE3, st.GENERICDATE4, st.GENERICDATE5, 
            st.GENERICDATE6, st.GENERICBOOLEAN1, st.GENERICBOOLEAN2, st.GENERICBOOLEAN3, st.GENERICBOOLEAN4, st.GENERICBOOLEAN5, st.GENERICBOOLEAN6, st.PROCESSINGUNITSEQ, st.MODIFICATIONDATE, 
            st.UNITTYPEFORLINENUMBER, st.UNITTYPEFORSUBLINENUMBER, st.UNITTYPEFORNUMBEROFUNITS, st.UNITTYPEFORDISCOUNTPERCENT, st.UNITTYPEFORNATIVECURRENCYAMT, st.MODELSEQ, 
            et.eventtypeid,
            'SGY'||agy.agencyCode wAgency,
            'SGY'||ldr.agencyCode wAgencyLeader,
            ldr.agentRole wAgyLdrTitle,
            Ldr.District As Wagyldrdistrict,
            agy.Classcode As Wagtclass, --add by nelson
            'SGT'||ldr.AGENTCODE wAgyLdrCde,
            --add version 8
            afy.AGY_agent,
            afy.AGY_agency
          from cs_salestransaction st,
               cs_eventtype et,
               sh_agent_role agy,
               Sh_Agent_Role Ldr,
         AIA_FA_AGY_RELA_TMP afy  --version 8
         where et.tenantid='AIAS' and ST.PROCESSINGUNITSEQ=GV_PROCESSINGUNITSEQ
            AND st.compensationDate>=v_periodStartDate
            AND st.COMPENSATIONDATE < v_periodEndDate
            AND st.tenantid='AIAS'
            and st.businessunitmap in (1,16) --add by nelson
            and et.datatypeSeq=st.eventTypeSeq
            And Et.Removedate= Cdt_Endoftime
            and et.eventTypeId in ('RYC','API','IFYC','FYC', 'SSCP')
      --and st.genericattribute12=agy.agentCode
      and 'SGT'||st.genericattribute12=afy.FA_agent(+) --version 8 for transfer agent
            and decode(afy.AGY_agent,null,st.genericattribute12,afy.AGY_agent)=agy.agentCode --version 8
            and agy.effectiveStartDate<=st.genericdate2
            and agy.effectiveEndDate>st.genericdate2
            and agy.agencyLeader=ldr.AGENTCODE
            and ldr.effectiveStartDate<=st.genericdate2
            and ldr.effectiveEndDate>st.genericdate2
            and st.genericAttribute17 ='O'  -- non reassignment transaction
        And St.Genericdate2<=V_Cutoverdate;

log('8-1 just for mark : '||sql%rowcount);
    commit;


    For C_Txn In (

        with tmp_pos as( select *
                       from cs_position
              where removeDate=cdt_EndofTime
                        and effectiveStartDate<=v_periodEndDate -1
                        and effectiveEndDate > v_periodEndDate-1
                 ) --tmp table
        select /*+   leading(x) */
        x.genericAttribute2,  --add by nelson txn code
        x.genericAttribute14,
        x.Genericattribute17,
        x.Businessunitmap,
        x.Productname,
        --x.Genericattribute13,
        decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) as Genericattribute13, --version 8
        x.Compensationdate,
        x.SALESORDERSEQ,
        x.Salestransactionseq,
        x.genericDate2,
        x.eventtypeid,
        x.wAgency,
        x.wAgencyLeader,
        x.wAgyLdrTitle,
        'SGY'||x.Wagyldrdistrict As Wagyldrdistrict,
        x.Wagtclass,
        'SGY'||curDis.Genericattribute3 as CurDistrict,
        tt.name as LdrCurRole,
        x.wAgyLdrCde ,
         nvl((select stp.txtagt from  In_Pi_Aor_Setup stp
         where 'SGT'||to_number(stp.txtagt) =  x.wAgyLdrCde
         and stp.dtecycle = v_periodEndDate-1
         and stp.txttype in ('C','D')
         and rownum =1),'X') as setup
        from  aia_x  x,
              tmp_pos agt,
              tmp_pos curDis,
              tmp_pos LdrCurRole ,
              cs_title tt
        where curDis.tenantid='AIAS'
        and LdrCurRole.tenantid='AIAS'
        and tt.tenantid='AIAS'
        and decode(x.AGY_agent,null,'SGT'||x.genericattribute12,'SGT'||x.AGY_agent)=agt.name
        and 'SGY'||agt.genericattribute1 = curDis.Name
        and x.wAgyLdrCde = LdrCurRole.name
        and LdrCurRole.titleseq = tt.ruleelementownerseq
        and tt.removedate = cdt_EndofTime
        and tt.effectiveenddate  = cdt_EndofTime
        --version 8 tunning end
    )
    loop
      V_Agydisttrxn.Wagency:=C_Txn.wAgency;
      V_Agydisttrxn.Wagencyleader:=C_Txn.wAgencyLeader;
      V_Agydisttrxn.wAgyLdrTitle:=c_txn.wAgyLdrTitle;
      V_Agydisttrxn.Wagyldrdistrict:=C_Txn.Wagyldrdistrict;
      V_Agydisttrxn.Wagtclass:=C_Txn.Wagtclass;
      V_Agydisttrxn.CurDistrict:=c_txn.CurDistrict;  -- add by Nelson
      V_Agydisttrxn.LdrCurRole:=c_txn.LdrCurRole;  -- add by Nelson
      V_Agydisttrxn.wAgyLdrCde:=c_txn.wAgyLdrCde;  -- add by Nelson
      V_Agydisttrxn.setup:=c_txn.setup;  -- add by Nelson
      V_Agydisttrxn.txnCode:=c_txn.genericAttribute2; -- add by Nelson
      V_Agydisttrxn.policyIssueDate:=c_txn.genericDate2;

      V_Agydisttrxn.Salestransactionseq:=C_Txn.Salestransactionseq;
      V_Agydisttrxn.SALESORDERSEQ:=C_Txn.SALESORDERSEQ;
      V_Agydisttrxn.Compensationdate:=C_Txn.Compensationdate;
      V_Agydisttrxn.Commissionagy:='SGY'||C_Txn.Genericattribute13;
      V_Agydisttrxn.Runningtype:='Before Cutover - GA17=O';
      V_Agydisttrxn.eventtypeid:=C_Txn.eventtypeid;
      V_Agydisttrxn.Productname:=C_Txn.Productname;
      V_Agydisttrxn.Businessunitmap:=C_Txn.Businessunitmap;
      V_Agydisttrxn.Orphanpolicy:=C_Txn.Genericattribute17;
      V_Agydisttrxn.Periodseq:=v_periodSeq;
      V_Agydisttrxn.TxnClassCode:=c_txn.genericAttribute14;



      if C_Txn.Salestransactionseq=14636699154312497  then
      Comtransferpiaor_debug(V_Agydisttrxn);
      else
      comTransferPIAOR(V_Agydisttrxn) ;
      end if;



    end loop; -- end c_txn

    Log('8 Before Cutover GA17 equal O');

    commit;



--leader who retire or leave company can also get the PIAOR??

  execute immediate 'truncate table aia_x1';
  insert /*+ append */ into aia_x1
      select /*+   materialize */ st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, 
        st.BILLTOADDRESSSEQ, st.SHIPTOADDRESSSEQ, st.OTHERTOADDRESSSEQ, st.ISRUNNABLE, st.BUSINESSUNITMAP, st.ACCOUNTINGDATE, st.PRODUCTID, st.PRODUCTNAME, st.PRODUCTDESCRIPTION, 
        st.NUMBEROFUNITS, st.UNITVALUE, st.UNITTYPEFORUNITVALUE, st.PREADJUSTEDVALUE, st.UNITTYPEFORPREADJUSTEDVALUE, st.VALUE, st.UNITTYPEFORVALUE, st.NATIVECURRENCY, st.NATIVECURRENCYAMOUNT, 
        st.DISCOUNTPERCENT, st.DISCOUNTTYPE, st.PAYMENTTERMS, st.PONUMBER, st.CHANNEL, st.ALTERNATEORDERNUMBER, st.DATASOURCE, st.REASONSEQ, st.COMMENTS, st.GENERICATTRIBUTE1, 
        st.GENERICATTRIBUTE2, st.GENERICATTRIBUTE3, st.GENERICATTRIBUTE4, st.GENERICATTRIBUTE5, st.GENERICATTRIBUTE6, st.GENERICATTRIBUTE7, st.GENERICATTRIBUTE8, st.GENERICATTRIBUTE9, 
        st.GENERICATTRIBUTE10, st.GENERICATTRIBUTE11, st.GENERICATTRIBUTE12, st.GENERICATTRIBUTE13, st.GENERICATTRIBUTE14, st.GENERICATTRIBUTE15, st.GENERICATTRIBUTE16, st.GENERICATTRIBUTE17, 
        st.GENERICATTRIBUTE18, st.GENERICATTRIBUTE19, st.GENERICATTRIBUTE20, st.GENERICATTRIBUTE21, st.GENERICATTRIBUTE22, st.GENERICATTRIBUTE23, st.GENERICATTRIBUTE24, st.GENERICATTRIBUTE25, 
        st.GENERICATTRIBUTE26, st.GENERICATTRIBUTE27, st.GENERICATTRIBUTE28, st.GENERICATTRIBUTE29, st.GENERICATTRIBUTE30, st.GENERICATTRIBUTE31, st.GENERICATTRIBUTE32, st.GENERICNUMBER1, 
        st.UNITTYPEFORGENERICNUMBER1, st.GENERICNUMBER2, st.UNITTYPEFORGENERICNUMBER2, st.GENERICNUMBER3, st.UNITTYPEFORGENERICNUMBER3, st.GENERICNUMBER4, st.UNITTYPEFORGENERICNUMBER4, 
        st.GENERICNUMBER5, st.UNITTYPEFORGENERICNUMBER5, st.GENERICNUMBER6, st.UNITTYPEFORGENERICNUMBER6, st.GENERICDATE1, st.GENERICDATE2, st.GENERICDATE3, st.GENERICDATE4, st.GENERICDATE5, 
        st.GENERICDATE6, st.GENERICBOOLEAN1, st.GENERICBOOLEAN2, st.GENERICBOOLEAN3, st.GENERICBOOLEAN4, st.GENERICBOOLEAN5, st.GENERICBOOLEAN6, st.PROCESSINGUNITSEQ, st.MODIFICATIONDATE, 
        st.UNITTYPEFORLINENUMBER, st.UNITTYPEFORSUBLINENUMBER, st.UNITTYPEFORNUMBEROFUNITS, st.UNITTYPEFORDISCOUNTPERCENT, st.UNITTYPEFORNATIVECURRENCYAMT, st.MODELSEQ,et.eventtypeid,
          'SGY'||agy.agencyCode wAgency,
          'SGY'||ldr.agencyCode wAgencyLeader,
          ldr.agentRole wAgyLdrTitle,
          Ldr.District As Wagyldrdistrict,
          agy.Classcode As Wagtclass,--  add by Nelson
          'SGT'||ldr.Agentcode wAgyLdrCde,  -- add by Nelson
      --add version 8
      afy.AGY_agent,
      afy.AGY_agency
        from cs_salestransaction st,
             cs_eventtype et,
             Sh_Agent_Role Agy,
             Sh_Agent_Role Ldr,
       AIA_FA_AGY_RELA_TMP afy  --version 8
       where ST.PROCESSINGUNITSEQ=GV_PROCESSINGUNITSEQ and st.tenantid='AIAS'
          AND st.compensationDate>=v_periodStartDate
          AND st.COMPENSATIONDATE < v_periodEndDate
          and st.businessunitmap in (1,16) --add by nelson
          and et.datatypeSeq=st.eventTypeSeq
          And Et.Removedate= Cdt_Endoftime
          and et.eventTypeId in ('RYC','API','IFYC','FYC', 'SSCP')
          --and st.genericattribute12=agy.agentCode
      and 'SGT'||st.genericattribute12=afy.FA_agent(+) --version 8 for transfer agent
          and decode(afy.AGY_agent,null,st.genericattribute12,afy.AGY_agent)=agy.agentCode --version 8
          and agy.effectiveStartDate<=st.genericdate2
          and agy.effectiveEndDate>st.genericdate2
          And Agy.Agencyleader=Ldr.Agentcode
          and agy.agencycode=ldr.agencycode
          And Ldr.Effectiveenddate<=St.Genericdate2 -- need to consider the version end date is same as issue date
          And Ldr.EffectiveStartdate = (Select Max(Effectivestartdate)
          From Sh_Agent_Role T Where Ldr.Agentcode=T.Agentcode And Ldr.Agencycode=T.Agencycode
          And T.Effectiveenddate<=St.Genericdate2
          )
          and st.genericAttribute17 ='O'  -- non reassignment transaction
          And St.Genericdate2<=V_Cutoverdate
          And Not Exists (Select 1 From SH_QUERY_RESULT R
                           Where Component In ('PI','AOR') And Periodseq=v_periodSeq And St.Salestransactionseq=R.Genericsequence1);



  Log('9-1');
commit;




  For C_Txn In (
   select /*+   leading(x1) */
        x.genericAttribute2,  --add by nelson txn code
        x.genericAttribute14,
        x.Genericattribute17,
        x.Businessunitmap,
        x.Productname,
        --x.Genericattribute13,
    decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) as Genericattribute13, --version 8
        x.Compensationdate,
        x.SALESORDERSEQ,
        x.Salestransactionseq,
        x.genericDate2,
        x.eventtypeid,
        x.wAgency,
        x.wAgencyLeader,
        x.wAgyLdrTitle,
        'SGY'||x.Wagyldrdistrict As Wagyldrdistrict,
        x.Wagtclass,
        'SGY'||curDis.Genericattribute3 as CurDistrict,
        tt.name as LdrCurRole,
        x.wAgyLdrCde ,
         nvl((select stp.txtagt from  In_Pi_Aor_Setup stp
         where 'SGT'||to_number(stp.txtagt) =  x.wAgyLdrCde
         and stp.dtecycle = v_periodEndDate-1
         and stp.txttype in ('C','D')
         and rownum =1),'X') as setup
     from aia_x1 x,
     ---version 5 fix incorrect DMcode
     cs_position agt,
     cs_position curDis,
     cs_position LdrCurRole ,
     cs_title tt
     where  --'SGT'||x.genericattribute12=agt.name
        decode(x.AGY_agent,null,'SGT'||x.genericattribute12,'SGT'||x.AGY_agent)=agt.name --add version 8
        and agt.removeDate=cdt_EndofTime
        AND agt.effectiveStartDate<=v_periodEndDate -1
        AND agt.effectiveEndDate > v_periodEndDate-1
        and 'SGY'||agt.genericattribute1 = curDis.Name
        and curDis.removeDate=cdt_EndofTime
        AND curDis.effectiveStartDate<= v_periodEndDate-1
        AND curDis.effectiveEndDate> v_periodEndDate-1
        and x.wAgyLdrCde = LdrCurRole.name
        and LdrCurRole.removeDate=cdt_EndofTime
        AND LdrCurRole.effectiveStartDate<=v_periodEndDate -1
        AND LdrCurRole.effectiveEndDate > v_periodEndDate-1
        and LdrCurRole.titleseq = tt.ruleelementownerseq
        and tt.removedate = cdt_EndofTime
        and tt.effectiveenddate  = cdt_EndofTime

     --add by nelson end
    )

    loop

      V_Agydisttrxn.Wagency:=C_Txn.wAgency;
      V_Agydisttrxn.Wagencyleader:=C_Txn.wAgencyLeader;
      V_Agydisttrxn.wAgyLdrTitle:=c_txn.wAgyLdrTitle;
      V_Agydisttrxn.Wagyldrdistrict:=C_Txn.Wagyldrdistrict;
      V_Agydisttrxn.Wagtclass:=C_Txn.Wagtclass;
      V_Agydisttrxn.policyIssueDate:=c_txn.genericDate2;
      V_Agydisttrxn.CurDistrict:=c_txn.CurDistrict;  -- add by Nelson
      V_Agydisttrxn.LdrCurRole:=c_txn.LdrCurRole;  -- add by Nelson
      V_Agydisttrxn.wAgyLdrCde:=c_txn.wAgyLdrCde;  -- add by Nelson
      V_Agydisttrxn.setup:=c_txn.setup;  -- add by Nelson
      V_Agydisttrxn.txnCode:=c_txn.genericAttribute2; -- add by Nelson

      V_Agydisttrxn.Salestransactionseq:=C_Txn.Salestransactionseq;
      V_Agydisttrxn.SALESORDERSEQ:=C_Txn.SALESORDERSEQ;
      V_Agydisttrxn.Compensationdate:=C_Txn.Compensationdate;
      V_Agydisttrxn.Commissionagy:='SGY'||C_Txn.Genericattribute13;
      V_Agydisttrxn.Runningtype:='Before Cutover - GA17=O - Ealier district';
      V_Agydisttrxn.eventtypeid:=C_Txn.eventtypeid;
      V_Agydisttrxn.Productname:=C_Txn.Productname;
      V_Agydisttrxn.Businessunitmap:=C_Txn.Businessunitmap;
      V_Agydisttrxn.Orphanpolicy:=C_Txn.Genericattribute17;
      V_Agydisttrxn.Periodseq:=v_periodSeq;
      V_Agydisttrxn.TxnClassCode:=c_txn.genericAttribute14;


      ----DBMS_OUTPUT.put_line('start loop'||v_maxSetNumber);

      if C_Txn.Salestransactionseq=14636699154312497  then
      Comtransferpiaor_debug(V_Agydisttrxn);
      else
      comTransferPIAOR(V_Agydisttrxn) ;
      end if;


    end loop; -- end c_txn

    commit;



    Log('9-2 Before Cutover ealier district ,GA17 equal O' );

  --look into aia customer table ga12+13+ lastest version end


  --            vParName := segmentationutils.segmentname('CS_SalesTransaction', pProcessingUnitSeq, v_periodEndDate);

----------------------------------------------------Before cutover date end line-----------------------------------------------------





----------------------------------------------------After cutover date start line----------------------------------------------------

  --look into ods table start
  --version 10 long run tuning
  -- for c_txn in (
  -- with x as (
  --  select /*+ parallel(8) materialize */ st.*,et.eventtypeid,
  --    'SGY'||Agy.Genericattribute1 Wagency, -- add by nelson
  --    'SGY'||Ldr.Genericattribute1 Wagencyleader, --add by nelson
  --    Ldr.genericAttribute11 wAgyLdrTitle, --add by nelson
  --    Ldr.Genericattribute3 As Wagyldrdistrict, --add by nelson
  --    Agy.Genericattribute4 As Wagtclass, --add by nelson
  --    Ldr.Genericdate5 As Spinstartdate, --add by nelson
  --    Ldr.genericDate6 as spinEndDate, --add by nelson
  --    Ldr.name wAgyLdrCde, -- add by Nelson
  --  --add version 8
  --  afy.AGY_agent,
  --  afy.AGY_agency
  --    from cs_salestransaction st,
  --         cs_eventtype et,
  --         Cs_Position Agy,
  --         Cs_Position Ldr, -- add by Nelson
  --     AIA_FA_AGY_RELA_TMP afy  --version 8
  --   Where ST.tenantid='AIAS'
  --      and et.tenantid='AIAS'
  --      and Agy.tenantid='AIAS'
  --      and Ldr.tenantid='AIAS'
  --      and ST.PROCESSINGUNITSEQ=GV_PROCESSINGUNITSEQ
  --      AND st.compensationDate>=v_periodStartDate
  --      AND st.COMPENSATIONDATE < v_periodEndDate
  --      and st.businessunitmap in (1,16) --add by nelson
  --      and et.datatypeSeq=st.eventTypeSeq
  --      And Et.Removedate= Cdt_Endoftime
  --      and et.eventTypeId in ('RYC','API','IFYC','FYC', 'SSCP')
  --      --and 'SGT'||st.genericattribute12=agy.name -- add by Nelson
  --  and 'SGT'||st.genericattribute12=afy.FA_agent(+) --version 8 for transfer agent
  --      and decode(afy.AGY_agent,null,'SGT'||st.genericattribute12,afy.AGY_agent)=agy.name --version 8
  --      and agy.removeDate=cdt_EndofTime
  --      and agy.effectiveStartDate<=st.genericDate2
  --      and agy.effectiveEndDate>st.genericDate2
  --      And Ldr.Genericattribute11 In ('FSD','FSAD','AM','FSM') -- add by nelson
  --      and 'SGT'||Agy.Genericattribute2 = Ldr.name  --add by nelson
  --      and Ldr.removeDate=cdt_EndofTime --add by nelson
  --      and Ldr.effectiveStartDate<=st.genericDate2 --add by nelson
  --      and Ldr.effectiveEndDate>st.genericDate2 --add by nelson
  --      And (St.Genericattribute17='O'
  --      or (st.genericAttribute17<>'O' and ST.Genericattribute14 in ('10','48'))
  --      )
  --      And St.Genericdate2>V_Cutoverdate
  --      and st.genericdate2 <  to_date('12/01/2015', 'mm/dd/yyyy')
  --   )


     execute immediate 'truncate table tmp_x';

      insert /*+ APPEND */ into tmp_x
     (
      TENANTID,
      SALESTRANSACTIONSEQ,
      SALESORDERSEQ,
      COMPENSATIONDATE,
      BUSINESSUNITMAP,
      PRODUCTNAME,
      GENERICATTRIBUTE2,
      GENERICATTRIBUTE12,
      GENERICATTRIBUTE13,
      GENERICATTRIBUTE14,
      GENERICATTRIBUTE17,
      GENERICDATE2,
      eventtypeid,
      Wagency,
      WagtLeader,
      Wagtclass,
      AGY_agent,
      AGY_agency
      )
       select /*+   materialize */
          st.TENANTID,
          st.SALESTRANSACTIONSEQ,
          st.SALESORDERSEQ,
          st.COMPENSATIONDATE,
          st.BUSINESSUNITMAP,
          st.PRODUCTNAME,
          st.GENERICATTRIBUTE2,
          st.GENERICATTRIBUTE12,
          st.GENERICATTRIBUTE13,
          st.GENERICATTRIBUTE14,
          st.GENERICATTRIBUTE17,
          st.GENERICDATE2,
          et.eventtypeid,
          'SGY'||Agy.Genericattribute1 Wagency,
          'SGT'||Agy.Genericattribute2 WagtLeader,
          Agy.Genericattribute4 As Wagtclass,
          afy.AGY_agent,
          afy.AGY_agency
     from cs_salestransaction st,
          cs_eventtype et,
          Cs_Position Agy,
          AIA_FA_AGY_RELA_TMP afy
    Where ST.tenantid='AIAS'
      and et.tenantid='AIAS'
      and Agy.tenantid='AIAS'
      and ST.PROCESSINGUNITSEQ=GV_PROCESSINGUNITSEQ
      AND st.compensationDate>=v_periodStartDate
      AND st.COMPENSATIONDATE < v_periodEndDate
      and st.businessunitmap in (1,16)
      and et.datatypeSeq=st.eventTypeSeq
      And Et.Removedate= Cdt_Endoftime
      and et.eventTypeId in ('RYC','API','IFYC','FYC', 'SSCP')
      and 'SGT'||st.genericattribute12=afy.FA_agent(+)
      and decode(afy.AGY_agent,null,'SGT'||st.genericattribute12,afy.AGY_agent)=agy.name
      and agy.removeDate=cdt_EndofTime
      and agy.effectiveStartDate<=st.genericDate2
      and agy.effectiveEndDate>st.genericDate2
      And (St.Genericattribute17='O'
      or (st.genericAttribute17<>'O' and ST.Genericattribute14 in ('10','48'))
      )
      And St.Genericdate2>V_Cutoverdate
      and st.genericdate2 <  to_date('12/01/2015', 'mm/dd/yyyy')
    ;

    log('10-1 get transaction afer cutover into tmp_x done');

    commit;

    --get agency learder position version at policy issue date
      merge into tmp_x x
      using(select *
              from cs_position
             where tenantid='AIAS'
               and removedate=cdt_EndofTime
           ) Ldr
      on (Ldr.name = x.WagtLeader
          and Ldr.effectiveStartDate<=x.genericDate2
          and Ldr.effectiveEndDate>x.genericDate2
          and Ldr.Genericattribute11 In ('FSD','FSAD','AM','FSM')
        )
    when matched then update
         set x.Wagencyleader='SGY'||Ldr.Genericattribute1,
             x.wAgyLdrTitle=Ldr.genericAttribute11,
             x.Wagyldrdistrict=Ldr.Genericattribute3,
         x.Spinstartdate=Ldr.Genericdate5,
             x.spinEndDate=Ldr.genericDate6,
             x.wAgyLdrCde=Ldr.name
         ;
      log('10-2 merge agent leader version into tmp_x done');

    commit;

  --version 10 end


     --add by nelson start
     for c_txn in (
     select /*+   */
     x.genericAttribute2,  --add by nelson txn code
     x.genericAttribute14,
     x.Genericattribute17,
     x.Businessunitmap,
     x.Productname,
     --x.Genericattribute13,
   decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) as Genericattribute13, --version 8
     x.Compensationdate,
     x.SALESORDERSEQ,
     x.Salestransactionseq,
     x.genericDate2,
     x.eventtypeid,
     x.wAgency ,
     x.wAgencyLeader,
     x.wAgyLdrTitle ,
     'SGY'||x.Wagyldrdistrict as Wagyldrdistrict,
     x.Wagtclass ,
     x.wAgyLdrCde ,
     x.Spinstartdate,
     x.spinEndDate,
     'SGY'||curDis.Genericattribute3 as CurDistrict,
     tt.name as LdrCurRole,
         nvl((select stp.txtagt from  In_Pi_Aor_Setup stp
         where 'SGT'||to_number(stp.txtagt) =  x.wAgyLdrCde
         and stp.dtecycle = v_periodEndDate-1
         and stp.txttype in ('C','D')
         and rownum =1),'X') as setup
     --from x,
     from tmp_x x, --version 10 add
     ---version 5 fix incorrect DMcode
     cs_position agt,
     cs_position curDis,
     cs_position LdrCurRole ,
     cs_title tt
     where  --'SGT'||x.genericattribute12=agt.name
      decode(x.AGY_agent,null,'SGT'||x.genericattribute12,'SGT'||x.AGY_agent)=agt.name --add version 8
        and agt.removeDate=cdt_EndofTime
        AND agt.effectiveStartDate<=v_periodEndDate -1
        AND agt.effectiveEndDate > v_periodEndDate-1
        and 'SGY'||agt.genericattribute1 = curDis.Name
        and curDis.removeDate=cdt_EndofTime
        AND curDis.effectiveStartDate<=v_periodEndDate -1
        AND curDis.effectiveEndDate > v_periodEndDate-1
        and x.wAgyLdrCde = LdrCurRole.name
        and LdrCurRole.removeDate=cdt_EndofTime
        AND LdrCurRole.effectiveStartDate<=v_periodEndDate -1
        AND LdrCurRole.effectiveEndDate > v_periodEndDate-1
        and LdrCurRole.titleseq = tt.ruleelementownerseq
        and tt.removedate = cdt_EndofTime
        and tt.effectiveenddate  = cdt_EndofTime
        and curDis.tenantid='AIAS'
        and LdrCurRole.tenantid='AIAS'
        and tt.tenantid='AIAS'

     --add by nelson end
    )

    loop


    ----DBMS_OUTPUT.put_line('start loop'||v_maxSetNumber);

      V_Agydisttrxn.Wagency:=C_Txn.wAgency;
      V_Agydisttrxn.Wagencyleader:=C_Txn.wAgencyLeader;
      V_Agydisttrxn.wAgyLdrTitle:=c_txn.wAgyLdrTitle;
      V_Agydisttrxn.Wagyldrdistrict:=C_Txn.Wagyldrdistrict;
      V_Agydisttrxn.Wagtclass:=C_Txn.Wagtclass;
      V_Agydisttrxn.policyIssueDate:=c_txn.genericDate2;
      V_Agydisttrxn.CurDistrict:=c_txn.CurDistrict;  -- add by Nelson
      V_Agydisttrxn.LdrCurRole:=c_txn.LdrCurRole;  -- add by Nelson
      V_Agydisttrxn.wAgyLdrCde:=c_txn.wAgyLdrCde;  -- add by Nelson
      V_Agydisttrxn.setup:=c_txn.setup;  -- add by Nelson
      V_Agydisttrxn.txnCode:=c_txn.genericAttribute2; -- add by Nelson

      V_Agydisttrxn.Salestransactionseq:=C_Txn.Salestransactionseq;
      V_Agydisttrxn.SALESORDERSEQ:=C_Txn.SALESORDERSEQ;
      V_Agydisttrxn.Compensationdate:=C_Txn.Compensationdate;
      V_Agydisttrxn.Commissionagy:='SGY'||C_Txn.Genericattribute13;
      V_Agydisttrxn.Runningtype:='After Cutover - GA17='||c_txn.genericattribute17;
      V_Agydisttrxn.eventtypeid:=C_Txn.eventtypeid;
      V_Agydisttrxn.Productname:=C_Txn.Productname;
      V_Agydisttrxn.Businessunitmap:=C_Txn.Businessunitmap;
      V_Agydisttrxn.Orphanpolicy:='O';
      V_Agydisttrxn.ActualOrphanPolicy:=c_txn.genericAttribute17;
      V_Agydisttrxn.Periodseq:=v_periodSeq;
      V_Agydisttrxn.Spinstartdate:=C_Txn.Spinstartdate;
      V_Agydisttrxn.SpinEnddate:=C_Txn.SpinEnddate;
      V_Agydisttrxn.Spindaterange:=Ceil(Months_Between(c_txn.genericDate2,C_Txn.Spinstartdate)/12);
      V_Agydisttrxn.Txnclasscode:=C_Txn.Genericattribute14;

      if C_Txn.Salestransactionseq=14636699154312497  then
      Comtransferpiaor_debug(V_Agydisttrxn);
      else
      comTransferPIAOR(V_Agydisttrxn) ;
      end if;


    end loop; -- end c_txn



  commit;
  Log('10-3 after cutover befor 2015-12-1 GA17 equal O or GA17 not equal O and GA14 in 10,48' );

  --look into ods table end

    comDebugger('SQL Performance','PIAOR_Calculation[SP_TXA_PIAOR]-SQL4 START:' ||SYSDATE);

 commit;




  execute immediate 'truncate table tmp_x_St';

  Log('11 Truncate tmp_x_St done');

 insert /*+ APPEND */ into tmp_x_St
 select st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, 
st.BILLTOADDRESSSEQ, st.SHIPTOADDRESSSEQ, st.OTHERTOADDRESSSEQ, st.ISRUNNABLE, st.BUSINESSUNITMAP, st.ACCOUNTINGDATE, st.PRODUCTID, st.PRODUCTNAME, st.PRODUCTDESCRIPTION, 
st.NUMBEROFUNITS, st.UNITVALUE, st.UNITTYPEFORUNITVALUE, st.PREADJUSTEDVALUE, st.UNITTYPEFORPREADJUSTEDVALUE, st.VALUE, st.UNITTYPEFORVALUE, st.NATIVECURRENCY, st.NATIVECURRENCYAMOUNT, 
st.DISCOUNTPERCENT, st.DISCOUNTTYPE, st.PAYMENTTERMS, st.PONUMBER, st.CHANNEL, st.ALTERNATEORDERNUMBER, st.DATASOURCE, st.REASONSEQ, st.COMMENTS, st.GENERICATTRIBUTE1, 
st.GENERICATTRIBUTE2, st.GENERICATTRIBUTE3, st.GENERICATTRIBUTE4, st.GENERICATTRIBUTE5, st.GENERICATTRIBUTE6, st.GENERICATTRIBUTE7, st.GENERICATTRIBUTE8, st.GENERICATTRIBUTE9, 
st.GENERICATTRIBUTE10, st.GENERICATTRIBUTE11, st.GENERICATTRIBUTE12, st.GENERICATTRIBUTE13, st.GENERICATTRIBUTE14, st.GENERICATTRIBUTE15, st.GENERICATTRIBUTE16, st.GENERICATTRIBUTE17, 
st.GENERICATTRIBUTE18, st.GENERICATTRIBUTE19, st.GENERICATTRIBUTE20, st.GENERICATTRIBUTE21, st.GENERICATTRIBUTE22, st.GENERICATTRIBUTE23, st.GENERICATTRIBUTE24, st.GENERICATTRIBUTE25, 
st.GENERICATTRIBUTE26, st.GENERICATTRIBUTE27, st.GENERICATTRIBUTE28, st.GENERICATTRIBUTE29, st.GENERICATTRIBUTE30, st.GENERICATTRIBUTE31, st.GENERICATTRIBUTE32, st.GENERICNUMBER1, 
st.UNITTYPEFORGENERICNUMBER1, st.GENERICNUMBER2, st.UNITTYPEFORGENERICNUMBER2, st.GENERICNUMBER3, st.UNITTYPEFORGENERICNUMBER3, st.GENERICNUMBER4, st.UNITTYPEFORGENERICNUMBER4, 
st.GENERICNUMBER5, st.UNITTYPEFORGENERICNUMBER5, st.GENERICNUMBER6, st.UNITTYPEFORGENERICNUMBER6, st.GENERICDATE1, st.GENERICDATE2, st.GENERICDATE3, st.GENERICDATE4, st.GENERICDATE5, 
st.GENERICDATE6, st.GENERICBOOLEAN1, st.GENERICBOOLEAN2, st.GENERICBOOLEAN3, st.GENERICBOOLEAN4, st.GENERICBOOLEAN5, st.GENERICBOOLEAN6, st.PROCESSINGUNITSEQ, st.MODIFICATIONDATE, 
st.UNITTYPEFORLINENUMBER, st.UNITTYPEFORSUBLINENUMBER, st.UNITTYPEFORNUMBEROFUNITS, st.UNITTYPEFORDISCOUNTPERCENT, st.UNITTYPEFORNATIVECURRENCYAMT, st.MODELSEQ,
'SGT'||st.genericattribute12
 ,null,null,null,null,null,null,null,null ,null, et.eventtypeid, null,
 --add version 8
afy.AGY_agent,
afy.AGY_agency
 from cs_Salestransaction  st
 join cs_eventtype et
 on et.datatypeseq=st.eventtypeseq and et.removedate=cdt_EndofTime
 left join AIA_FA_AGY_RELA_TMP afy on 'SGT'||st.genericattribute12=afy.FA_agent--version 8
 where st.tenantid = 'AIAS'  and et.tenantid = 'AIAS'
              and ST.PROCESSINGUNITSEQ = GV_PROCESSINGUNITSEQ
              AND st.compensationDate >= v_periodstartdate
              AND st.COMPENSATIONDATE < v_periodenddate
              and st.businessunitmap in (1,16)
              and eventtypeid in   ('RYC',
                  'API',
                  'IFYC',
                  'FYC',
                  'SSCP',
                  'ORYC')
            And not Exists (Select 1
                                From SH_QUERY_RESULT R
                               Where Component In ('PI',
                                     'AOR')
                                 And Periodseq = v_periodSeq
                                 And St.Salestransactionseq = R.Genericsequence1)
              and st.genericdate2 < to_date('12/01/2015','mm/dd/yyyy');


Log('12 Insert tmp_x_St Done');


            DBMS_STATS.GATHER_TABLE_STATS (
            ownname          => 'AIASEXT',
            tabname          => 'tmp_x_St',
            method_opt       => 'FOR ALL INDEXED COLUMNS SIZE AUTO',
            estimate_percent => dbms_stats.auto_sample_size,
            degree           => dbms_stats.default_degree,
            cascade          => true
          );


Log('13 gather tmp_x_St Stats Done');

--search transaction commission agent information,during compensatindate

merge into tmp_x_St tgt
using(
  select *
    from cs_position
   where tenantid='AIAS'
     and removedate=cdt_EndofTime
) src
--on (src.name = tgt.sgtga12 and   --st.genericattribute12
on (src.name = decode(tgt.AGY_agent,null,tgt.sgtga12,'SGT'||AGY_agent)  --add version 8
    and tgt.compensationdate between src.effectivestartdate and src.effectiveenddate-1 and
    src.tenantid=tgt.tenantid)
when matched then update
set tgt.wagency='SGY' || src.Genericattribute1
    ,tgt.wagtclass=src.Genericattribute4
    , tgt.sgtga2='SGT'||src.genericattribute2;--commission agent leader code

  Log('14 Merge tmp_x_St 1 Done');



merge into   tmp_x_St tgt
using(
   select   *
     from cs_position p
    where tenantid='AIAS' and  removedate=cdt_EndofTime
      and Genericattribute11 In ('FSD',
                  'FSAD',
                  'AM',
                  'FSM')
      ) src
     on (src.name = tgt.sgtga2
         and tgt.compensationdate between src.effectivestartdate and src.effectiveenddate-1 )
when matched then update
   set tgt.wagencyleader='SGY' || src.Genericattribute1
       ,tgt.wAgyLdrTitle=src.genericAttribute11
       ,tgt.Wagyldrdistrict=src.Genericattribute3
       ,tgt.Spinstartdate=src.genericdate5
       ,tgt.SpinEnddate=src.genericdate6
       ,tgt.wAgyLdrCode = src.name;

  Log('15 Merge  tmp_x_St 2 Done');



update tmp_x_St x
set  setup = nvl((select stp.txtagt
               from In_Pi_Aor_Setup stp
              where 'SGT' || to_number(stp.txtagt) = x.wAgyLdrCode
                and stp.dtecycle = v_periodenddate-1
                and stp.txttype in('C', 'D')
                and rownum = 1),
       'X') ;

 Log('16 Update tmp_x_St Done');


 --search the commission agent current information ,during period end date

  for c_txn in (
--version 8 comment for tunning
--with x as (select  * from tmp_x_St)
    /*select \*+ parallel(8)  leading(curdis,x,ldr,LDRCURROLE,tt)  index(curdis CS_POSITION_AK1) index(ldrcurrole CS_POSITION_AK1) *\
       x.genericAttribute2,
       x.genericAttribute14,
       x.Genericattribute17,
       x.Businessunitmap,
       x.Productname,
       --x.Genericattribute13,
   decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) as Genericattribute13, --version 8
       x.Compensationdate,
       x.SALESORDERSEQ,
       x.Salestransactionseq,
       x.genericDate2,
       x.eventtypeid,
       x.wAgency,
       x.wAgencyLeader,
       x.wAgyLdrTitle,
       'SGY' || x.Wagyldrdistrict as Wagyldrdistrict,
       x.Wagtclass,
       x.wAgyLdrCode as wAgyLdrCde ,
       x.Spinstartdate,
       x.spinEndDate,
       'SGY' || curDis.Genericattribute3 as CurDistrict,
       tt.name as LdrCurRole
       ,x.setup
  from tmp_x_St x,
 ---version 5 fix incorrect DMcode
       cs_position agt,
       cs_position curDis,
       cs_position LdrCurRole,
       cs_title tt
 where --x.sgtga12 = agt.name
 decode(x.AGY_agent,null,x.sgtga12,'SGT'||x.AGY_agent)=agt.name --add version 8
   and agt.removeDate=cdt_EndofTime
   AND agt.effectiveStartDate<=v_periodEndDate -1
   AND agt.effectiveEndDate > v_periodEndDate-1
   and 'SGY'||agt.genericattribute1 = curDis.Name
   and curDis.tenantid = 'AIAS'
   and curDis.removeDate = cdt_EndofTime
   AND curDis.effectiveStartDate <= v_periodenddate-1
   AND curDis.effectiveEndDate > v_periodenddate-1
   and x.wAgyLdrCode = LdrCurRole.name
   and LdrCurRole.tenantid = 'AIAS'
   and LdrCurRole.removeDate = cdt_EndofTime
   AND LdrCurRole.effectiveStartDate <= v_periodenddate-1
   AND LdrCurRole.effectiveEndDate > v_periodenddate-1
   and LdrCurRole.titleseq = tt.ruleelementownerseq
   and tt.removedate = cdt_EndofTime
   and tt.effectiveenddate = cdt_EndofTime
   and tt.tenantid='AIAS')*/
    with tmp_pos as( select *
                       from cs_position
                      where removeDate=cdt_EndofTime
                        and effectiveStartDate<=v_periodEndDate -1
                        and effectiveEndDate > v_periodEndDate-1
                 ) --tmp table
      select /*+    leading(x)*/
           x.genericAttribute2,
           x.genericAttribute14,
           x.Genericattribute17,
           x.Businessunitmap,
           x.Productname,
           --x.Genericattribute13,
           decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) as Genericattribute13, --version 8
           x.Compensationdate,
           x.SALESORDERSEQ,
           x.Salestransactionseq,
           x.genericDate2,
           x.eventtypeid,
           x.wAgency,
           x.wAgencyLeader,
           x.wAgyLdrTitle,
           'SGY' || x.Wagyldrdistrict as Wagyldrdistrict,
           x.Wagtclass,
           x.wAgyLdrCode as wAgyLdrCde ,
           x.Spinstartdate,
           x.spinEndDate,
           'SGY' || curDis.Genericattribute3 as CurDistrict,
           tt.name as LdrCurRole
           ,x.setup
      from tmp_x_St x,
     ---version 5 fix incorrect DMcode
           tmp_pos agt,
           tmp_pos curDis,
           tmp_pos LdrCurRole,
           cs_title tt
     where --x.sgtga12 = agt.name
       decode(x.AGY_agent,null,x.sgtga12,'SGT'||x.AGY_agent)=agt.name --add version 8
       and 'SGY'||agt.genericattribute1 = curDis.Name
       and curDis.tenantid = 'AIAS'
       and x.wAgyLdrCode = LdrCurRole.name
       and LdrCurRole.tenantid = 'AIAS'
       and LdrCurRole.titleseq = tt.ruleelementownerseq
       and tt.removedate = cdt_EndofTime
       and tt.effectiveenddate = cdt_EndofTime
       and tt.tenantid='AIAS')
    loop

      V_Agydisttrxn.Wagency:=C_Txn.wAgency;
      V_Agydisttrxn.Wagencyleader:=C_Txn.wAgencyLeader;
      V_Agydisttrxn.wAgyLdrTitle:=c_txn.wAgyLdrTitle;
      V_Agydisttrxn.Wagyldrdistrict:=C_Txn.Wagyldrdistrict;
      V_Agydisttrxn.Wagtclass:=C_Txn.Wagtclass;
      V_Agydisttrxn.policyIssueDate:=c_txn.genericDate2;
      V_Agydisttrxn.CurDistrict:=c_txn.CurDistrict;  -- add by Nelson
      V_Agydisttrxn.LdrCurRole:=c_txn.LdrCurRole;  -- add by Nelson
      V_Agydisttrxn.wAgyLdrCde:=c_txn.wAgyLdrCde;  -- add by Nelson
      V_Agydisttrxn.setup:=c_txn.setup;  -- add by Nelson
      V_Agydisttrxn.txnCode:=c_txn.genericAttribute2; -- add by Nelson

      V_Agydisttrxn.Salestransactionseq:=C_Txn.Salestransactionseq;
      V_Agydisttrxn.SALESORDERSEQ:=C_Txn.SALESORDERSEQ;
      V_Agydisttrxn.Compensationdate:=C_Txn.Compensationdate;
      V_Agydisttrxn.Commissionagy:='SGY'||C_Txn.Genericattribute13;
      V_Agydisttrxn.Runningtype:='After Cutover - GA17<>O';
      V_Agydisttrxn.eventtypeid:=C_Txn.eventtypeid;
      V_Agydisttrxn.Productname:=C_Txn.Productname;
      V_Agydisttrxn.Businessunitmap:=C_Txn.Businessunitmap;
      V_Agydisttrxn.Orphanpolicy:= 'X'|| C_Txn.Genericattribute17 ;
      ---those ga17=o, but not able get version by policy issue date, will trade as ga17<>O
      V_Agydisttrxn.ActualOrphanPolicy:=c_txn.genericAttribute17;
      V_Agydisttrxn.Periodseq:=v_periodSeq;
      V_Agydisttrxn.Spinstartdate:=C_Txn.Spinstartdate;
      V_Agydisttrxn.SpinEnddate:=C_Txn.SpinEnddate;
      V_Agydisttrxn.Spindaterange:=Ceil(Months_Between(c_txn.genericDate2,C_Txn.Spinstartdate)/12);
      V_Agydisttrxn.Txnclasscode:=C_Txn.Genericattribute14;

      --comTransferPIAOR(V_Agydisttrxn) ;
      if C_Txn.Salestransactionseq=14636699154312497  then
      Comtransferpiaor_debug(V_Agydisttrxn);
      else
      comTransferPIAOR(V_Agydisttrxn) ;
      end if;
    end loop; -- end c_txn

    commit;
    Log('17 befor 2015-12-1 but not exists befor step');



  comDebugger('SQL Performance','PIAOR_Calculation[SP_TXA_PIAOR]-SQL5 START:' ||SYSDATE);




    Log('18');
  --REMOVE NA district result
  Delete /*+   */ From SH_QUERY_RESULT
  where component in (V_Componentvalue_Pi,V_Componentvalue_Aor)
  And Periodseq=v_periodSeq
  and genericAttribute4='NA';

  commit;
  Log('18');




--Added by Suresh 20180129

  --genericDate1,    --policyIssueDate
  --genericDate2,     --compensationDate

EXECUTE IMMEDIATE 'truncate table SH_QUERY_RESULT_TMP35';
Insert /*+ APPEND */ into SH_QUERY_RESULT_TMP35
    Select /*+ index(R SH_QUERY_RESULT_IDX) index(CP CS_POSITION_AK1) index(CGP CS_GAPOSITION_PK)*/ R.Component,r.periodseq,R.Genericsequence1,
           Cgp.Genericdate13 As Crossoverstartdate,
           Cgp.Genericdate14 As Crossoverenddate,
           Cgp.Genericdate15 As Demotionstartdate,
           Cgp.Genericdate16 As DemotionEndDate,
            Case
              ----crossover date chking
              --case#1
                When Cgp.Genericdate13 Is Not Null
                And Cgp.Genericdate14 Is Null
                And Cgp.Genericdate15 Is Null
                And Cgp.Genericdate16 Is Null Then
                  Case When R.Genericdate1<Cgp.Genericdate13 And R.genericDate2>=Cgp.Genericdate13
                     Then Replace(R.Genericattribute5,'Direct Team','Crossover')
                  Else R.Genericattribute5
                  End
                --case#2
                When Cgp.Genericdate13 Is Not Null
                And Cgp.Genericdate14 Is not Null
                And Cgp.Genericdate15 Is Null
                And Cgp.Genericdate16 Is Null Then
                 Case When R.Genericdate1<Cgp.Genericdate13
                      and R.genericDate2 >=Cgp.Genericdate13
                      And R.Genericdate2 <Cgp.Genericdate14
                         Then Replace(R.Genericattribute5,'Direct Team','Crossover')
                     Else R.Genericattribute5
                     End
                --demotion date chking
                --case#3
                When Cgp.Genericdate13 Is Null
                And Cgp.Genericdate14 Is Null
                And Cgp.Genericdate15 Is not Null
                And Cgp.Genericdate16 Is Null Then
                 Case When R.Genericdate1<Cgp.Genericdate15
                   And R.Genericdate2>=Cgp.Genericdate15
                     Then Replace(R.Genericattribute5,'AOR - Direct Team','AOR - Crossover')
                   else R.Genericattribute5
                   End
                --case#4
                When Cgp.Genericdate13 Is Null
                And Cgp.Genericdate14 Is Null
                And Cgp.Genericdate15 Is Not Null
                And Cgp.Genericdate16 Is not null Then
                 Case When R.Genericdate1<Cgp.Genericdate15
                   And R.Genericdate2>=Cgp.Genericdate15
                   And R.Genericdate2<Cgp.Genericdate16
                     Then Replace(R.Genericattribute5,'AOR - Direct Team','AOR - Crossover')
                   else R.Genericattribute5
                   End
                --case#5
                When Cgp.Genericdate13 Is not Null
                And Cgp.Genericdate14 Is Null
                And Cgp.Genericdate15 Is Not Null
                And Cgp.Genericdate16 Is Null
                and Cgp.Genericdate13<Cgp.Genericdate15
                Then
                 Case When R.Genericdate1<Cgp.Genericdate13
                   And R.Genericdate2>=Cgp.Genericdate15
                       Then Replace(R.Genericattribute5,'Direct Team','Crossover')
                   When R.Genericdate1>=Cgp.Genericdate13 And R.Genericdate1<Cgp.Genericdate15
                       And R.Genericdate2>=Cgp.Genericdate15
                       then Replace(R.Genericattribute5,'AOR - Direct Team','AOR - Crossover')
                   else R.Genericattribute5
                   End
                --case#6
                When Cgp.Genericdate13 Is not Null
                And Cgp.Genericdate14 Is Null
                And Cgp.Genericdate15 Is not Null
                And Cgp.Genericdate16 Is Null
                and Cgp.Genericdate13>Cgp.Genericdate15
                Then
                 Case When R.Genericdate1<Cgp.Genericdate15
                   And R.Genericdate2>=Cgp.Genericdate15 and  R.Genericdate2<Cgp.Genericdate13
                       Then Replace(R.Genericattribute5,'AOR - Direct Team','AOR - Crossover')
                   When R.Genericdate1<Cgp.Genericdate15
                   And R.Genericdate2>=Cgp.Genericdate13
                       Then Replace(R.Genericattribute5,'Direct Team','Crossover')
                   When R.Genericdate1>=Cgp.Genericdate15 And R.Genericdate1<Cgp.Genericdate13
                   And R.Genericdate2>=Cgp.Genericdate13
                       Then Replace(R.Genericattribute5,'Direct Team','Crossover')
                   Else R.Genericattribute5 End
                  --case#7
                  When Cgp.Genericdate13 Is not Null
                    And Cgp.Genericdate14 Is Null
                    And Cgp.Genericdate15 Is not Null
                    And Cgp.Genericdate16 Is Null
                    and Cgp.Genericdate13=Cgp.Genericdate15
                    Then
                   Replace(R.Genericattribute5,'Direct Team','Crossover')
            else R.Genericattribute5
            end as rule
    From SH_QUERY_RESULT R, cs_position cp, cs_gaposition cgp
    Where cp.tenantid='AIAS' and cgp.tenantid='AIAS' and R.Component In ('PI','AOR')
    and r.periodseq=v_periodSeq
    and r.genericAttribute5 in ('PI - Direct Team', 'AOR - Direct Team')
    And R.Genericattribute1=Cp.Name
    And Cp.Removedate=Cdt_Endoftime
    And Cp.Effectivestartdate<=R.Genericdate2
    And Cp.Effectiveenddate>R.Genericdate2
    And Cp.Ruleelementownerseq=Cgp.Ruleelementownerseq
    And Cgp.Removedate=Cdt_Endoftime
    And Cgp.Effectivestartdate<=R.Genericdate2
    And Cgp.Effectiveenddate>R.Genericdate2
    And Cgp.Pagenumber=0
    And (Cgp.Genericdate13 Is Not Null Or Cgp.Genericdate14 Is Not Null
    Or Cgp.Genericdate15 Is Not Null Or Cgp.Genericdate16 Is Not Null)
    AND r.genericAttribute11 <>'XO' --if xo, means the GA17=0 trxn cant find a matched dirstrict by policy issue date in both aia and tc table
  ;
  COMMIT;
  Log('19 match Crossover data');


    merge into SH_QUERY_RESULT m
    Using (select /*+   */ * from SH_QUERY_RESULT_TMP35
    ) T
    On (T.Genericsequence1=M.Genericsequence1  --salestransactionSeq
    And T.Component=M.Component
    and t.periodseq=m.periodseq
    )
    When Matched Then Update
    Set M.Genericattribute5=T.Rule,
        M.Genericdate3=T.Crossoverstartdate,
        M.Genericdate4=T.Crossoverenddate,
        M.Genericdate5=T.Demotionstartdate,
        m.Genericdate6=T.DemotionEnddate
        ;
--end by Suresh 20180129
    commit;
    Log('20 match Crossover data');

   -- comDebugger('piaor','merge1 done!!'||i_periodSeq);


  --when completed data gathering from kinds of scenario, then insert the assignment data to cs_txnassignment
  --delete pi/aor assignment data which not same as standard agency
 /*** the deletion is deined here, because assignment will be clean up by comCleanAssignment()
  delete /-+parallel(Ta,8)-/ from
   cs_transactionAssignment ta
   where 1=1
   And (Ta.Genericattribute4 Like '%PI%' Or Ta.Genericattribute4 Like '%AOR%')
   And Ta.Genericattribute4 Not Like 'NADOR%'
   and ta.setnumber>2
   AND ta.genericAttribute9 is null
   and ta.compensationDate>=v_periodStartDate
   And Ta.Compensationdate<V_Periodenddate
   --ensure only delete those trxn is in current pu
   And Exists (
   Select 1 From Cs_Salestransaction
   Where Salestransactionseq=Ta.Salestransactionseq
   and processingUnitSeq=gv_processingUnitSeq
   )
   And Not Exists (Select 1 From Cs_Salestransaction
   Where Salestransactionseq=Ta.Salestransactionseq
   and processingUnitSeq=gv_processingUnitSeq
   AND EVENTTYPESEQ=GV_HRYC
   );

   */

   -- commit;

  --version 8  comment,beause table PIAOR_ASSIGNMENT will truncate when the procedure start
    --reset ga4 of standard agency assignment which is shared for pi or aor

    --Log('36');
    --Update /* parallel(Ta,8)*/Cs_Transactionassignment Ta
    /*
  set ta.genericattribute4=decode(substrc(ta.genericattribute4,1,5),'NADOR','NADOR',''),
ta.genericAttribute5=null,
ta.genericAttribute6=null,
ta.genericAttribute7=null,
ta.genericAttribute8=null,
ta.genericAttribute10=null
Where  (Ta.Genericattribute4 Like '%PI%' Or Ta.Genericattribute4 Like '%AOR%')
and ta.positionname like 'SGY%'
and ta.genericAttribute9 is null
and ta.compensationDate>=v_periodStartDate
and ta.compensationDate<v_periodEndDate
And Exists (
Select 1 From Cs_Salestransaction
Where Salestransactionseq=Ta.Salestransactionseq
And Processingunitseq=Gv_Processingunitseq
)
And Not Exists (Select 1 From Cs_Salestransaction
Where Salestransactionseq=Ta.Salestransactionseq
And Eventtypeseq=GV_HRYC
);


commit;
Log('36');
  */



    /*version 8 comment
Log('37');
--delete from sh_sequence where seqtype in ('AOR_TRXNSEQ', 'PI_TRXNSEQ');
execute immediate 'truncate table sh_sequence';  --add by nelson for performance tune

COMMIT;
Log('37');
  */

  --different PI agency

 --  Log('38');

 --insert ALL
 --WHEN standardAgency<>positionName THEN
 --INTO Cs_Transactionassignment
 --  (Tenantid,salesTransactionSeq,salesOrderSeq,setNumber,compensationDate,positionName,payeeId,genericAttribute4,genericAttribute5,genericAttribute6,
 --  genericAttribute7,genericAttribute8,processingunitseq)
 --  VALUES ('AIAS',salesTransactionSeq, salesOrderSeq, setNumber,compensationDate, positionName,null,assignmentType,ruleIndicator,
 --  wAgency,wAgyLdrTitle,wAgyLdrDistrict,processingunitseq)
 --  WHEN standardAgency=positionName THEN
 --  INTO sh_sequence (businessSeq,seqType) values (salesTransactionSeq,'PI_TRXNSEQ')
 --  select /*+ INDEX(ta.AIAS_TXNASSIGN_PNAME) parallel(8) */ r.genericSequence1 as salesTransactionSeq,r.genericSequence2 as salesOrderSeq,r.genericNumber1 as setNumber,
 --         r.genericDate1 as policyIssuedDate,r.genericDate2 as compensationDate,
 --         r.genericAttribute1 as positionName ,r.genericAttribute2 as wAgyLdrTitle,
 --         r.genericAttribute3 as wAgency, r.genericAttribute4 as wAgyLdrDistrict,
 --         r.genericAttribute5 as ruleIndicator, r.genericAttribute6 as businessUnitMap,
 --         r.component as assignmentType,nvl(ta.positionName,'#') standardAgency,ta.processingunitseq as processingunitseq
 --    from sh_query_result r,Cs_Transactionassignment  ta
 --   Where ta.tenantid='AIAS' and R.Component ='PI'
 --     and r.periodseq=gv_periodseq
 --     and ta.salestransactionseq=r.genericSequence1
 --     and Ta.Setnumber=1
 --     aND r.genericAttribute11 <>'XO';
 -- -- and ta.positionName <> r.genericAttribute1
 --  commit;




 --  version 8 select PI data from SH_QUERY_RESULT

 insert INTO PIAOR_ASSIGNMENT
          (Tenantid,
      salesTransactionSeq,
      salesOrderSeq,
      setNumber,
      compensationDate,
      positionName,
      payeeId,
      genericAttribute4,
      genericAttribute5,
      genericAttribute6,
          genericAttribute7,
      genericAttribute8,
      processingunitseq)
   select 'AIAS',
          r.genericSequence1 as salesTransactionSeq,
      r.genericSequence2 as salesOrderSeq,
      r.genericNumber1 as setNumber,
      r.genericDate2 as compensationDate,
          r.genericAttribute1 as positionName ,
      null,
      r.component as assignmentType,
      r.genericAttribute5 as ruleIndicator,
          r.genericAttribute3 as wAgency,
      r.genericAttribute2 as wAgyLdrTitle,
      r.genericAttribute4 as wAgyLdrDistrict,
      GV_PROCESSINGUNITSEQ as processingunitseq
     from SH_QUERY_RESULT r
    Where R.Component ='PI'
      and r.periodseq=v_periodSeq
      and r.genericAttribute11 <>'XO';
   commit;



    Log('21 update PI assignment');

  /* version 8 comment,due to assiment rewirt and insert into table PIAOR_ASSIGNMENT
      DBMS_STATS.GATHER_TABLE_STATS (
          ownname          => 'AIASEXT',
          tabname          => 'SH_SEQUENCE',
          method_opt       => 'FOR ALL INDEXED COLUMNS SIZE AUTO',
          estimate_percent => dbms_stats.auto_sample_size,
          degree           => dbms_stats.default_degree,
          cascade          => true
        );

  Log('39');
*/

  --version 8 comment,due to assiment rewirt and insert into table PIAOR_ASSIGNMENT
 -- Merge /*+ INDEX(ta AIAS_TXNASSIGN_PNAME) */ Into Cs_Transactionassignment Ta
 -- Using
 -- (Select Salestransactionseq, Salesorderseq, Setnumber,Policyissueddate,
 -- Compensationdate,Positionname,Wagyldrtitle,Wagency,Wagyldrdistrict,Ruleindicator,Businessunitmap
 -- from (SELECT /*+ LEADING(r,s) index(R SH_QUERY_RESULT_IDX2) */s.businessSeq businessSeq,
 --              r.genericSequence1 as salesTransactionSeq,r.genericSequence2 as salesOrderSeq,
 --              r.genericNumber1 as setNumber,
 --              r.genericDate1 as policyIssuedDate,r.genericDate2 as compensationDate,
 --              r.genericAttribute1 as positionName ,r.genericAttribute2 as wAgyLdrTitle,
 --              r.genericAttribute3 as wAgency, r.genericAttribute4 as wAgyLdrDistrict,
 --              R.Genericattribute5 As Ruleindicator, R.Genericattribute6 As Businessunitmap
 --       from sh_query_result r left join sh_sequence s
 --         on s.businessSeq=r.genericSequence1
 --        and s.Seqtype='PI_TRXNSEQ'
 --      Where R.Component ='PI'
 --        And R.Periodseq=Gv_Periodseq
 --        And R.Genericattribute11 <>'XO'
 --      )R
 --      where r.businessSeq is not null
 -- ) t
 -- on
 -- ( t.salestransactionSeq=ta.salestransactionseq
 --   and t.positionName=ta.positionName
 --   and ta.setnumber=1
 -- )
 -- When Matched Then Update Set
 -- ta.genericAttribute4=decode( nvl(ta.genericAttribute4,'Standard'),'Standard', 'Standard_PI', ta.genericAttribute4||'_PI'),
 -- ta.genericAttribute5=t.ruleIndicator,
 -- ta.genericAttribute6=t.wAgency,
 -- ta.genericAttribute7=t.wAgyLdrTitle,
 -- ta.genericAttribute8=t.wAgyLdrDistrict
 -- ;
 --
 --
 -- commit;
 --
 -- Log('39');


  --  Log('40');

  --deal with AOR data ,and if exists PI at the same time

--version 8 comment ,due to assiment rewirt and insert into table PIAOR_ASSIGNMENT
 -- insert /*+ append */ ALL
 -- WHEN standardAgency<>positionName and PIPositionName<>positionName THEN
 -- INTO Cs_Transactionassignment
 --   (tenantid,salesTransactionSeq,salesOrderSeq,setNumber,compensationDate,positionName,payeeId,genericAttribute4,genericAttribute10,genericAttribute6,
 --   genericAttribute7,genericAttribute8,PROCESSINGUNITSEQ)
 --   VALUES ('AIAS',salesTransactionSeq, salesOrderSeq, setNumber,compensationDate, positionName,null,assignmentType,ruleIndicator,
 --   wAgency,wAgyLdrTitle,wAgyLdrDistrict,PROCESSINGUNITSEQ)
 --   WHEN standardAgency=positionName or PIPositionName=positionName THEN
 --   INTO sh_sequence (businessSeq,seqType) values (salesTransactionSeq,'AOR_TRXNSEQ')
 --   Select /*+  INDEX(ta AIAS_TXNASSIGN_PNAME)  PARALLEL(r,8) */
 --   R.Genericsequence1 As Salestransactionseq,R.Genericsequence2 As Salesorderseq,
 --    r.genericnumber1 as setNumber,
 --   r.genericDate1 as policyIssuedDate,r.genericDate2 as compensationDate,
 --  r.genericAttribute1 as positionName ,r.genericAttribute2 as wAgyLdrTitle,
 --  r.genericAttribute3 as wAgency, r.genericAttribute4 as wAgyLdrDistrict,
 --  r.genericAttribute5 as ruleIndicator, r.genericAttribute6 as businessUnitMap,
 --  r.component as assignmentType,nvl(ta.positionName,'#') standardAgency, nvl(rpi.genericattribute1,'#') as PIPositionName,TA.PROCESSINGUNITSEQ as PROCESSINGUNITSEQ
 --   from sh_query_result r,Cs_Transactionassignment ta, sh_query_result rpi
 --   Where ta.tenantid='AIAS' and ta.PROCESSINGUNITSEQ=GV_PROCESSINGUNITSEQ and R.Component ='AOR'
 --   And R.Periodseq=Gv_Periodseq
 --   AND r.genericAttribute11 <>'XO'
 --   and ta.salestransactionseq=r.genericSequence1
 --   and ta.setnumber=1
 --   and rpi.component(+)='PI'
 --   And R.Genericsequence1=Rpi.Genericsequence1(+)
 --   and rpi.periodseq(+)=gv_periodseq ;

 --FOR RERUN
DELETE FROM SH_SEQUENCE WHERE seqType='AOR_TRXNSEQ';

  insert /*+ append */ ALL
  WHEN  PIPositionName<>positionName THEN
  INTO PIAOR_ASSIGNMENT
    (tenantid,salesTransactionSeq,salesOrderSeq,setNumber,compensationDate,positionName,payeeId,genericAttribute4,genericAttribute10,genericAttribute6,
    genericAttribute7,genericAttribute8,PROCESSINGUNITSEQ)
    VALUES ('AIAS',salesTransactionSeq, salesOrderSeq, setNumber,compensationDate, positionName,null,assignmentType,ruleIndicator,
    wAgency,wAgyLdrTitle,wAgyLdrDistrict,PROCESSINGUNITSEQ)
    WHEN PIPositionName=positionName THEN
    INTO SH_SEQUENCE (businessSeq,seqType) values (salesTransactionSeq,'AOR_TRXNSEQ')
    Select 'AIAS' as tenantid,
         R.Genericsequence1 As Salestransactionseq,
       R.Genericsequence2 As Salesorderseq,
           r.genericnumber1 as setNumber,
           r.genericDate1 as policyIssuedDate,
       r.genericDate2 as compensationDate,
           r.genericAttribute1 as positionName ,
       r.genericAttribute2 as wAgyLdrTitle,
           r.genericAttribute3 as wAgency,
       r.genericAttribute4 as wAgyLdrDistrict,
           r.genericAttribute5 as ruleIndicator,
       r.genericAttribute6 as businessUnitMap,
           r.component as assignmentType,
       nvl(rpi.genericattribute1,'#') as PIPositionName,
       GV_PROCESSINGUNITSEQ as PROCESSINGUNITSEQ
    from SH_QUERY_RESULT r, SH_QUERY_RESULT rpi
    Where R.Component ='AOR'
    And R.Periodseq=v_periodSeq
    AND R.genericAttribute11 <>'XO'
    and rpi.component(+)='PI'
    And R.Genericsequence1=rpi.Genericsequence1(+)
    and rpi.periodseq(+)=v_periodSeq
  ;

    commit;
    Log('22-1 updae AOR assignment');

     begin
        DBMS_STATS.GATHER_TABLE_STATS (
            ownname          => 'AIASEXT',
            tabname          => 'SH_SEQUENCE',
            method_opt       => 'FOR ALL INDEXED COLUMNS SIZE AUTO',
            estimate_percent => dbms_stats.auto_sample_size,
            degree           => dbms_stats.default_degree,
            cascade          => true
          );

    end;

--comDebugger('piaor','merge2 start piaor'||i_periodSeq);
     --update standard_pi_aor
    Log('22-2 gather sequence status information');

  --version 8 comment
  --  merge /*+ INDEX(ta AIAS_TXNASSIGN_PNAME) */ into Cs_Transactionassignment ta
  --  using
  --  (Select Salestransactionseq, Salesorderseq, Setnumber,Policyissueddate,
  --          Compensationdate,Positionname,Wagyldrtitle,Wagency,Wagyldrdistrict,Ruleindicator,Businessunitmap
  --     from (SELECT /*+ LEADING(r,s) index(R SH_QUERY_RESULT_IDX2) */s.businessSeq businessSeq,
  --                r.genericSequence1 as salesTransactionSeq,r.genericSequence2 as salesOrderSeq,
  --                r.genericNumber1 as setNumber,
  --                r.genericDate1 as policyIssuedDate,r.genericDate2 as compensationDate,
  --                r.genericAttribute1 as positionName ,r.genericAttribute2 as wAgyLdrTitle,
  --                r.genericAttribute3 as wAgency, r.genericAttribute4 as wAgyLdrDistrict,
  --                R.Genericattribute5 As Ruleindicator, R.Genericattribute6 As Businessunitmap
  --             from sh_query_result r left join sh_sequence s
  --               on s.businessSeq=r.genericSequence1
  --              and S.Seqtype='AOR_TRXNSEQ'
  --            Where R.Component ='AOR'
  --              And R.Periodseq=Gv_Periodseq
  --              And r.Genericattribute11 <>'XO'
  --          ) R
  --          where r.businessSeq is not null
  --  ) t
  --  on
  --  ( t.salestransactionSeq=ta.salestransactionseq
  --    And Ta.Positionname=T.Positionname
  --   -- and ta.tenantid='AIAS'
  --  )
  --  when matched then update set
  --    ta.genericAttribute4=decode( nvl(ta.genericAttribute4,'Standard'),'Standard', 'Standard_AOR', ta.genericAttribute4||'_AOR'),
  --    ta.genericAttribute10=t.ruleIndicator,
  --    ta.genericAttribute6=t.wAgency,
  --    ta.genericAttribute7=t.wAgyLdrTitle,
  --    ta.genericAttribute8=t.wAgyLdrDistrict;


    merge into PIAOR_ASSIGNMENT ta
    using
    (Select Salestransactionseq, Salesorderseq, Setnumber,Policyissueddate,
            Compensationdate,Positionname,Wagyldrtitle,Wagency,Wagyldrdistrict,Ruleindicator,Businessunitmap
       from (SELECT /*+ LEADING(r,s) index(R SH_QUERY_RESULT_IDX2) */s.businessSeq businessSeq,
                  r.genericSequence1 as salesTransactionSeq,r.genericSequence2 as salesOrderSeq,
                  r.genericNumber1 as setNumber,
                  r.genericDate1 as policyIssuedDate,r.genericDate2 as compensationDate,
                  r.genericAttribute1 as positionName ,r.genericAttribute2 as wAgyLdrTitle,
                  r.genericAttribute3 as wAgency, r.genericAttribute4 as wAgyLdrDistrict,
                  R.Genericattribute5 As Ruleindicator, R.Genericattribute6 As Businessunitmap
               from SH_QUERY_RESULT r left join SH_SEQUENCE s
                 on s.businessSeq=r.genericSequence1
                and S.Seqtype='AOR_TRXNSEQ'
              Where R.Component ='AOR'
                And R.Periodseq=v_periodSeq
                And r.Genericattribute11 <>'XO'
            ) R
            where r.businessSeq is not null
    ) t
    on
    ( t.salestransactionSeq=ta.salestransactionseq
      And Ta.Positionname=T.Positionname
     -- and ta.tenantid='AIAS'
    )
    when matched then update set
      ta.genericAttribute4=decode( nvl(ta.genericAttribute4,'Standard'),'Standard', 'Standard_AOR', ta.genericAttribute4||'_AOR'),
      ta.genericAttribute10=t.ruleIndicator,
      ta.genericAttribute6=t.wAgency,
      ta.genericAttribute7=t.wAgyLdrTitle,
      ta.genericAttribute8=t.wAgyLdrDistrict;



    Commit;
    Log('22-3 updae AOR flag to PI assignment record');


log('SP_TXA_PIAOR: end');

    --UPDATE XO result with to trxn.eb4
/*Arjun 0520 - temporary patch*/
/*update cs_transactionassignment ta
set genericattribute4 = 'NADOR_Standard_AOR'
where setnumber=1 and processingunitseq=gv_processingunitseq and tenantid='AIAS'
and   ta.compensationDate>=v_periodStartDate
    and ta.compensationDate<v_periodEndDate
and genericattribute4 = 'NADOR_AOR'
; */
commit;

/*genericBoolean4 comment beacuse aiasadmin has not insufficient privileges*/
 --  Log('42');
 --  Merge /*+ INDEX(gst AIA_Cs_gaSalestransaction_SEQ) */ Into Cs_Gasalestransaction gst
 --  Using (
 --    Select Distinct Genericsequence1 As Salestransactionseq, --due to pi-aor might share one trxn seq, so need distinct here.
 --    0 as pagenumber
 --    From SH_QUERY_RESULT
 --    where Component in ('AOR','PI')
 --    And Periodseq=i_periodSeq
 --    AND genericAttribute11 ='XO'
 --  ) T
 --  On (T.Salestransactionseq=Gst.Salestransactionseq
 --      and t.pagenumber=gst.pagenumber
 --      and gst.tenantid='AIAS'
 --  )
 --  When Matched Then Update Set
 --       genericBoolean4=1;
 --
 --  commit;
 --  Log('42');

    exception

    when others then
    rollback;
    gv_error:='Error [SP_TXA_PIAOR]: ' ||sqlerrm||' - '||dbms_utility.format_error_backtrace;


    raise_application_error(-20000,gv_error);



  end SP_TXA_PIAOR;




 --version 8 add monthly procedure
   procedure SP_MONTHLY_AGGREGATE as
  v_periodSeq int;
  v_periodStartDate date;
  v_Periodenddate date;
  v_PeriodName VARCHAR2(50);
  v_piaor_year varchar2(50);

  begin

  LOG('SP_MONTHLY_AGGREGATE: start');

  log('gv_CYCLE_DATE: '||gv_CYCLE_DATE);
  log('gv_calendarSeq: '||gv_calendarSeq);

    --get period startDate, endDate

  select cp.PERIODSEQ,cp.name,cp.startDate,cp.endDate
    into v_periodSeq,v_PeriodName,v_periodStartDate,v_periodEndDate
    from CS_PERIOD cp,
       cs_periodtype pt
   where cp.tenantid='AIAS'
     and cp.REMOVEDATE=cdt_EndOfTime
     and cp.CALENDARSEQ=gv_calendarSeq
     and cp.startdate<=to_date(gv_CYCLE_DATE,'yyyy-mm-dd')
     and cp.enddate>to_date(gv_CYCLE_DATE,'yyyy-mm-dd')
     and pt.name = 'month'
     and pt.periodtypeseq=cp.periodtypeseq
;

 if v_periodStartDate>=date'2016-12-01' and v_periodEndDate <= date'2018-01-01'   --for 2017
  then

    SELECT SUBSTR(C.NAME,1,4)
       INTO V_PIAOR_YEAR
       FROM CS_PERIOD A,
            CS_PERIOD B,
            CS_PERIOD C
        WHERE A.tenantid = 'AIAS'
      AND B.tenantid = 'AIAS'
      AND C.tenantid = 'AIAS'
      AND A.removeDate= cdt_EndOfTime
      AND B.removeDate= cdt_EndOfTime
      AND C.removeDate= cdt_EndOfTime
      AND A.PERIODSEQ = v_periodSeq
      AND A.calendarSeq = B.calendarSeq
          AND A.PARENTSEQ = B.PERIODSEQ
      AND B.calendarSeq = C.calendarSeq
          AND B.PARENTSEQ = C.PERIODSEQ;

  else

  select extract(year from startdate)
    into V_PIAOR_YEAR
    from cs_period
   where tenantid = 'AIAS'
     and removeDate = cdt_EndOfTime
     and PERIODSEQ = v_periodSeq;

  end if;

log('v_periodSeq: ' ||v_periodSeq);
log('v_PeriodName: '||v_PeriodName);
log('v_periodStartDate: '||v_periodStartDate);
log('v_periodEndDate: '||v_periodEndDate);
log('v_piaor_year: '||v_piaor_year);


commit;

  --init target table
  log('23 init table');

  execute immediate 'truncate table AIA_PIAOR_TRAN_TMP';
  delete from PIAOR_DETAIL where period=v_periodSeq;


  INSERT INTO AIA_PIAOR_TRAN_TMP
  select /* + parallel(18) */
       v_periodSeq,
       'PI',
       v.genericAttribute3 wAgency,
       s.genericattribute12 wAgent,
       v.genericAttribute1 payee,
       v.genericAttribute5 rule ,
       --'' PIB,--AOR only
     --sum(s.value) RYC
     s.PRODUCTNAME, --version 9 add
       sum(s.value) value
    from SH_QUERY_RESULT v,
         cs_salestransaction s
   where v.component = 'PI'
    and  s.salestransactionseq = v.genericSequence1
    and  v.periodseq = v_periodSeq
    and  ((v.genericattribute11 = 'XO' and s.genericattribute17 <> 'O') or v.genericattribute11 <> 'XO')
    and  s.compensationdate  >= v_periodStartDate
    and  s.compensationdate < v_Periodenddate
    and  v.genericAttribute6  in (1,16) -- correct
    and ((s.productname  in ('LF','HS') and s.genericattribute2  in ('PAY2','PAY3','PAY4','PAY5','PAY6')))
    group by v_periodSeq,v.genericAttribute3,s.genericattribute12,v.genericAttribute1,v.genericAttribute5,s.PRODUCTNAME;


  log('24 Sum PI RYC : '||SQL%ROWCOUNT);

commit;


    insert into AIA_PIAOR_TRAN_TMP
    select /* + parallel(12) */
         v_periodSeq,
         'AOR',
         v.genericAttribute3 wAgency,
         s.genericattribute12 wAgent,
         v.genericAttribute1 payee,
         v.genericAttribute5 rule ,
     --version 9 add aor only
         --sum(case when v.genericattribute7 <> 'RYC' then s.value+ nvl(gs.genericnumber3,0) else 0 end ) PIB ,
         --sum(case when v.genericattribute7 = 'RYC' then s.value else 0 end ) RYC,
     s.PRODUCTNAME||case when v.genericattribute7 = 'RYC' then '_RYC' ELSE '_PIB' end PRODUCTNAME ,
     SUM(case when v.genericattribute7 = 'RYC' then s.value else s.value+ nvl(gs.genericnumber3,0) end) value
    from SH_QUERY_RESULT v,
       cs_salestransaction s ,
     cs_gasalestransaction gs
    where v.component = 'AOR'
      and s.salestransactionseq = v.genericSequence1
      and s.salestransactionseq = gs.salestransactionseq
      and v.periodseq = v_periodSeq
    and s.compensationdate >= v_periodStartDate
      and s.compensationdate < v_Periodenddate
      and v.genericAttribute6  in (1,16) -- correct
      and ((v.genericattribute11 = 'XO' and s.genericattribute17 <> 'O') or v.genericattribute11 <> 'XO')
      and ( v.genericattribute7 in ('API','IFYC','FYC', 'SSCP')
          or ( v.genericattribute7 = 'RYC'
          and ((s.productname in ('LF','HS') and s.genericattribute2 in ('PAY2','PAY3','PAY4','PAY5','PAY6'))
                    or s.productname in ('PA')
            )
         )
          )
      group by v_periodSeq,v.genericAttribute3,s.genericattribute12,v.genericAttribute1,v.genericAttribute5,
      s.PRODUCTNAME,v.genericattribute7;


  log('25 Sum AOR RYC and PIB : '||SQL%ROWCOUNT);

    commit;


--insert into target table

    INSERT INTO PIAOR_DETAIL(
           Period,
           PIAOR_Year,
           component,
           Wri_Agency,
           Wri_Agent,
           Payee_Agency,
           Rule,
           --PIB, version 9 comment
       PA_RYC,
       LF_RYC,
           RYC
        )
    select /*leading(f) */
         v_periodSeq,
         v_piaor_year,
         f.component,
         f.wagency ,
         f.wagent,
         f.payee,
         f.rule,
     --version 9
         --sum(PIB),
         --sum(ryc)
     sum(case when f.PRODUCTNAME='PA' then f.value else 0 end) PA_RYC,
     sum(case when f.PRODUCTNAME<>'PA' then f.value else 0 end) LF_RYC,
         sum(value)
     --version 9 end
    from AIA_PIAOR_TRAN_TMP f
  WHERE COMPONENT='PI'  --version 9 add just for PI calculate
    group by f.component,f.wagency,f.wagent,f.payee,f.rule
  ;

  log('26 classify PI : '||SQL%ROWCOUNT);

  commit;


--version 9 add for AOR
     INSERT INTO PIAOR_DETAIL(
           Period,
           PIAOR_Year,
           component,
           Wri_Agency,
           Wri_Agent,
           Payee_Agency,
           Rule,
       PA_FYC,
       CS_FYC,
       LF_FYC,
           PIB,
       PA_RYC,
       LF_RYC,
           RYC
        )
    select /*leading(f) */
         v_periodSeq,
         v_piaor_year,
         f.component,
         f.wagency ,
         f.wagent,
         f.payee,
         f.rule,
     sum(case when f.PRODUCTNAME='PA_PIB' then f.value else 0 end) PA_FYC,
     sum(case when f.PRODUCTNAME='CS_PIB' then f.value else 0 end) CS_FYC,
     sum(case when f.PRODUCTNAME like '%PIB' and PRODUCTNAME not in ('PA_PIB','CS_PIB')  then f.value else 0 end) LF_FYC,
         sum(case when f.PRODUCTNAME like '%PIB' then f.value else 0 end) PIB,
     sum(case when f.PRODUCTNAME='PA_RYC' then f.value else 0 end) PA_RYC,
     sum(case when f.PRODUCTNAME like '%RYC' and f.PRODUCTNAME<>'PA_RYC' then f.value else 0 end) LF_RYC,
         sum(case when f.PRODUCTNAME like '%RYC' then f.value else 0 end) RYC
    from AIA_PIAOR_TRAN_TMP f
  WHERE COMPONENT='AOR'
    group by f.component,f.wagency,f.wagent,f.payee,f.rule
  ;

  log('26-1 classify AOR : '||SQL%ROWCOUNT);

  commit;
--version 9 end

     LOG('SP_MONTHLY_AGGREGATE: end');

    Exception
    When Others Then

      COMDEBUGGER('SP_MONTHLY_AGGREGATE error: ', sqlerrm);

    return;

  end SP_MONTHLY_AGGREGATE;





  procedure SP_TXNTXA_YREND_PI as
    v_periodSeq         int;
    v_yrStartDate       date;
    v_yrEndDate         date;
    v_yrEndEventTypeSeq int := 0;
    v_modificationTime  timestamp := gv_plstartTime - interval '1' second;
    v_periodStartDate   date;
    v_periodEndDate     date;
    v_compDate          date;
    Vseq                Number;
    v_txnSeq            number;
    v_piaor_year        varchar2(50);

    NO_YRENDFIXEDVALUE_FOUND EXCEPTION;
    NO_YRENDEVENTTYPE_FOUND  EXCEPTION;
    INVALID_PERIODDATE       EXCEPTION;

    v1 number;
    v2 number;
    v3 number;
  v_rtn           int := 0;

  begin
    --year end process

    --dbms_output.put_line('get fixed value');
   log('SP_TXNTXA_YREND_PI: start');

    --get period startDate, endDate

  select cp.PERIODSEQ,cp.startDate,cp.endDate
    into v_periodSeq,v_periodStartDate,v_periodEndDate
    from CS_PERIOD cp,
       cs_periodtype pt
   where cp.tenantid='AIAS'
     and cp.REMOVEDATE=cdt_EndOfTime
     and cp.CALENDARSEQ=gv_calendarSeq
     and cp.startdate<=to_date(gv_CYCLE_DATE,'yyyy-mm-dd')
     and cp.enddate>to_date(gv_CYCLE_DATE,'yyyy-mm-dd')
     and pt.name = 'month'
     and pt.periodtypeseq=cp.periodtypeseq
;

commit;

    v_rtn := comGetYrLastMonth(v_periodSeq);

    if v_rtn < 1 then
    log('SP_TXNTXA_YREND_PI: not year end month');
      return;
    end if;


    Gv_Periodseq := v_periodSeq;


  -- init

  log('27 init table ');
  execute immediate 'truncate table AIA_YrEnd_Tran_rela';





    begin

      select nvl(dataTypeSeq, 0)
        into v_yrEndEventTypeSeq
        from cs_eventType
       where tenantid='AIAS' and  eventTypeId = 'PI_Year_End'
         and removeDate = cdt_endoftime;

      --dbms_output.put_line('get event date');

    exception
      when no_data_found then
        raise NO_YRENDEVENTTYPE_FOUND;
    end;

    -- dbms_output.put_line('get start date');
  --version 8

    --begin
    --  select y.startDate, y.endDate
    --    into v_yrStartDate, v_yrEndDate
    --    from cs_periodtype pt, cs_period y, cs_period p
    --   where pt.tenantid='AIAS'
    --     and y.tenantid='AIAS'
    --     and p.tenantid='AIAS'
    --     and pt.name = 'year'
    --     and pt.removeDate = cdt_EndOfTime
    --     and p.removeDate = cdt_EndOfTime
    --     and y.removeDate = cdt_EndOfTime
    --     and p.periodSeq = v_periodSeq
    --     and y.startDate <= p.startDate
    --     and y.endDate > p.startDate
    --     and y.calendarSeq = p.calendarSeq
    --     and y.periodTypeSeq = pt.periodTypeSeq;
    --exception
    --  when no_data_found then
    --    raise INVALID_PERIODDATE;
    --end;


  if v_periodStartDate>=date'2016-12-01' and v_periodEndDate <= date'2018-01-01'   --for 2017
  then

         SELECT add_months(v_periodStartDate,-12)
             into v_yrStartDate
             FROM DUAL
           ;

    SELECT SUBSTR(C.NAME,1,4),C.ENDDATE,C.ENDDATE-1
       INTO V_PIAOR_YEAR,
      V_Yrenddate,
      V_Compdate
       FROM CS_PERIOD A,
            CS_PERIOD B,
            CS_PERIOD C
        WHERE A.tenantid = 'AIAS'
      AND B.tenantid = 'AIAS'
      AND C.tenantid = 'AIAS'
      AND A.removeDate= cdt_EndOfTime
      AND B.removeDate= cdt_EndOfTime
      AND C.removeDate= cdt_EndOfTime
      AND A.PERIODSEQ = v_periodSeq
      AND A.calendarSeq = B.calendarSeq
          AND A.PARENTSEQ = B.PERIODSEQ
      AND B.calendarSeq = C.calendarSeq
          AND B.PARENTSEQ = C.PERIODSEQ;

  else

   select extract(year from startdate),
          trunc(startdate,'YYYY'),
          add_months(trunc(startdate,'yyyy'),12),
          add_months(trunc(startdate,'yyyy'),12)-1
     into V_PIAOR_YEAR,
          v_yrStartDate,
          V_Yrenddate,
          V_Compdate
     from cs_period
    where tenantid = 'AIAS'
      and removeDate = cdt_EndOfTime
      and PERIODSEQ = v_periodSeq;


  end if;

  --version 8 end

    Log('28');

    delete /*+   FULL(PIAOR_Assignment)  */
      from PIAOR_Assignment
     where
          tenantid='AIAS'
       and genericAttribute4 like '%PI%'
       and processingunitseq=GV_PROCESSINGUNITSEQ
       and Genericattribute9 = 'YE REASSIGN TO DISTRICT'
       AND COMPENSATIONDATE = v_compDate;

    commit;
    Log('29 delete ye reassign for rerun');

  --version 8 comment
   --Log('51');
   --
   --delete from cs_gasalestransaction ga
   -- Where tenantid='AIAS' and Exists
   --       (Select 1
   --          From Cs_Salestransaction st
   --         where
   --               st.tenantid='AIAS'
   --           And Processingunitseq = Gv_Processingunitseq
   --           And Compensationdate = V_Compdate
   --           And Ga.Salestransactionseq = St.Salestransactionseq
   --           and Eventtypeseq = V_Yrendeventtypeseq
   --           );
   --
   --commit;
   --Log('51');

   --version 8 comment
    --Log('52');
    --delete /*+ parallel(8) FULL(cs_salestransaction)  */ from cs_salestransaction
    -- Where
    -- tenantid='AIAS'
    -- and processingUnitSeq = gv_processingUnitSeq
    -- and COMPENSATIONDATE = v_compDate
    -- and Eventtypeseq = V_Yrendeventtypeseq;
    --
    --
    --
    --commit;
    --Log('52');

    --Vseq:=Sequencegenpkg.Getnextfullseq('auditLogSeq', Classid.Cidauditlog);
    --v_txnSeq := Sequencegenpkg.Getnextfullseq('salesTransactionSeq',
    --                                          Classid.Cidsalestransaction);


   /*Arjun 20170509

The issue is that there are many salestransactionseqs that were deleted from cs_salestransaction,
but not from cs_Gasalestransaction and Cs_transactionassignment.

The logic the SH uses is to get the max STSEQ from CS_Salestransaction, and then adds to that
before inserting the new Year End records. These clash with the existing records.
I can change the logic to get the maximum seq from the SalesTransaction, GA and Assignment tables
and use that as a base, but ideally, if we?re deleting transactions, they should be deleted from all the tables.

*/
   -- select /*+ INDEX(CS_salestransaction,AIA_CS_SALESTRANSACTION_SEQ) */  MAX(salesTransactionSeq)+1 into v_txnSeq
    --  from CS_salestransaction





       select max(salestransactionseq)+1
   into v_txnseq
   from (select /*+ INDEX(CS_salestransaction,AIA_CS_SALESTRANSACTION_SEQ) */  max(salesTransactionSeq) salestransactionseq
      from CS_salestransaction
      union all
      select max(salestransactionseq ) from  PIAOR_ASSIGNMENT
      union all
      select max(salestransactionseq )  from cs_gasalestransaction
      )
       ;


    --CREATE NEW TXN

    v1  := Comgeteventtypeseq('RYC');
    v2  := Comgeteventtypeseq('H_RYC');
    v3  := Comgeteventtypeseq('ORYC');

-- et.eventTypeId in ('RYC', 'H_RYC', 'ORYC')

   /**
 create table AIA_MAX_sublinenumber tablespace tallydata
 as
 select *+ index(cs_salestransaction AIA_salestransaction_orderline) * salesorderseq,max(sublinenumber) as maxsublinenumber
             from cs_salestransaction
            where salesorderseq = st.salesorderseq and st.tenantid='AIAS' and 1=0 group by salesorderseq

            create index AIA_MAX_sublinenumber_idx on AIA_MAX_sublinenumber(salesorderseq,maxsublinenumber) tablespace tallyindex
  */
    execute immediate 'truncate table  AIA_MAX_sublinenumber';
    insert into AIA_MAX_sublinenumber
    select /*+ leading(ta,st) index(cs_salestransaction AIA_salestransaction_orderline)  index(ta     AIA_CS_TRANSACTIONASSIGN_IDX2) */ ta.salesorderseq,max(sublinenumber) as maxsublinenumber
                from cs_salestransaction st,  PIAOR_ASSIGNMENT ta
                where ta.compensationDate >= v_yrStartDate
                  And ta.Compensationdate < V_Yrenddate
                 and ta.tenantid='AIAS'
                 and ta.processingunitseq = Gv_Processingunitseq
                 AND Ta.Genericattribute5 = 'PI - Direct Team' -- changed check ga5, and only create trxn for pi-direct team
                 and ta.genericAttribute7 = 'FSAD'
                 and st.salestransactionseq=ta.salestransactionseq
                 and ta.processingunitseq=st.processingunitseq
                 and ta.Compensationdate = st.Compensationdate
                group by ta.salesorderseq;
    commit;

    Log('30 '||v_yrStartDate ||' ' ||v_yrEndDate || ' ' || v_compdate);



 --version 8 comment ,no sufficient privileges
   --insert all when salestransactionSeq > 0 then into cs_salestransaction
   --  (tenantid,salestransactionseq,
   --   salesOrderSeq,
   --   linenumber,
   --   sublinenumber,
   --   eventtypeseq,
   --   compensationDate,
   --   value,
   --   unittypeforvalue,
   --   modificationDate,
   --   isRunnable,
   --   ORIGINTYPEID,
   --   PREADJUSTEDVALUE,
   --   UNITTYPEFORPREADJUSTEDVALUE,
   --   PROCESSINGUNITSEQ,
   --   genericDate6,
   --   pipelineRunSeq,
   --   unittypeForLineNumber,
   --   unitTypeForSubLineNumber,
   --   businessUnitMap,
   --   --GENERIC FIELDS
   --   productId,
   --   productName,
   --   productDescription,
   --   dataSource,
   --   genericAttribute1,
   --   genericAttribute2,
   --   genericAttribute3,
   --   genericAttribute4,
   --   genericAttribute5,
   --   genericAttribute6,
   --   genericAttribute7,
   --   genericAttribute8,
   --   genericAttribute9,
   --   genericAttribute10,
   --   genericAttribute11,
   --   genericAttribute12,
   --   genericAttribute13,
   --   genericAttribute14,
   --   genericAttribute15,
   --   genericAttribute16,
   --   genericAttribute17,
   --   genericAttribute18,
   --   genericAttribute19,
   --   genericAttribute20,
   --   genericAttribute21,
   --   genericAttribute22,
   --   genericAttribute23,
   --   genericAttribute24,
   --   genericAttribute25,
   --   genericAttribute26,
   --   genericAttribute27,
   --   genericAttribute28,
   --   genericAttribute29,
   --   genericAttribute30,
   --   genericAttribute31,
   --   genericAttribute32,
   --   genericNumber1,
   --   genericNumber2,
   --   genericNumber3,
   --   genericNumber4,
   --   genericNumber5,
   --   genericNumber6,
   --   unitTypeForGenericNumber1,
   --   unitTypeForGenericNumber2,
   --   unitTypeForGenericNumber3,
   --   unitTypeForGenericNumber4,
   --   unitTypeForGenericNumber5,
   --   unitTypeForGenericNumber6,
   --   genericDate1,
   --   genericDate2,
   --   genericDate3,
   --   genericDate4,
   --   genericDate5,
   --   genericBoolean1,
   --   genericBoolean2,
   --   genericBoolean3,
   --   genericBoolean4,
   --   genericBoolean5,
   --   genericBoolean6
   --   ---
   --   )
   --values
   --  ('AIAS',salesTransactionSeq,
   --   salesOrderSeq,
   --   linenumber,
   --   sublinenumber,
   --   v_yrEndEventTypeSeq,
   --   v_compDate,
   --   value,
   --   unittypeforvalue,
   --   modificationDate,
   --   isRunnable,
   --   ORIGINTYPEID,
   --   PREADJUSTEDVALUE,
   --   UNITTYPEFORPREADJUSTEDVALUE,
   --   PROCESSINGUNITSEQ,
   --   compensationDate,
   --   gv_pipelineRunSeq,
   --   unittypeForLineNumber,
   --   unitTypeForSubLineNumber,
   --   businessUnitMap,
   --   --GENERIC FIELDS
   --   productId,
   --   productName,
   --   productDescription,
   --   dataSource,
   --   genericAttribute1,
   --   genericAttribute2,
   --   genericAttribute3,
   --   genericAttribute4,
   --   genericAttribute5,
   --   genericAttribute6,
   --   genericAttribute7,
   --   genericAttribute8,
   --   genericAttribute9,
   --   genericAttribute10,
   --   genericAttribute11,
   --   genericAttribute12,
   --   genericAttribute13,
   --   genericAttribute14,
   --   genericAttribute15,
   --   genericAttribute16,
   --   genericAttribute17,
   --   genericAttribute18,
   --   genericAttribute19,
   --   genericAttribute20,
   --   genericAttribute21,
   --   genericAttribute22,
   --   genericAttribute23,
   --   genericAttribute24,
   --   genericAttribute25,
   --   genericAttribute26,
   --   genericAttribute27,
   --   genericAttribute28,
   --   genericAttribute29,
   --   genericAttribute30,
   --   genericAttribute31,
   --   genericAttribute32,
   --   genericNumber1,
   --   genericNumber2,
   --   genericNumber3,
   --   genericNumber4,
   --   genericNumber5,
   --   genericNumber6,
   --   unitTypeForGenericNumber1,
   --   unitTypeForGenericNumber2,
   --   unitTypeForGenericNumber3,
   --   unitTypeForGenericNumber4,
   --   unitTypeForGenericNumber5,
   --   unitTypeForGenericNumber6,
   --   genericDate1,
   --   genericDate2,
   --   genericDate3,
   --   genericDate4,
   --   genericDate5,
   --   genericBoolean1,
   --   genericBoolean2,
   --   genericBoolean3,
   --   genericBoolean4,
   --   genericBoolean5,
   --   GENERICBOOLEAN6
   --   --
   --   )
   ----gaSalestransaction
   --WHEN salestransactionSeq > 0 then into cs_gasalestransaction
   --  (tenantid, salestransactionSeq,
   --   pagenumber,
   -- --Added by Suresh 10292017
   --   PROCESSINGUNITSEQ,
   --   compensationDate,
   --   --End by Suresh 10292017
   --   GENERICATTRIBUTE1,
   --   GENERICATTRIBUTE2,
   --   GENERICATTRIBUTE3,
   --   GENERICATTRIBUTE4,
   --   GENERICATTRIBUTE5,
   --   GENERICATTRIBUTE6,
   --   GENERICATTRIBUTE7,
   --   GENERICATTRIBUTE8,
   --   GENERICATTRIBUTE9,
   --   GENERICATTRIBUTE10,
   --   GENERICATTRIBUTE11,
   --   GENERICATTRIBUTE12,
   --   GENERICATTRIBUTE13,
   --   GENERICATTRIBUTE14,
   --   GENERICATTRIBUTE15,
   --   GENERICATTRIBUTE16,
   --   GENERICATTRIBUTE17,
   --   GENERICATTRIBUTE18,
   --   GENERICATTRIBUTE19,
   --   GENERICATTRIBUTE20,
   --   GENERICDATE1,
   --   GENERICDATE2,
   --   GENERICDATE3,
   --   GENERICDATE4,
   --   GENERICDATE5,
   --   GENERICDATE6,
   --   GENERICDATE7,
   --   GENERICDATE8,
   --   GENERICDATE9,
   --   GENERICDATE10,
   --   GENERICDATE11,
   --   GENERICDATE12,
   --   GENERICDATE13,
   --   GENERICDATE14,
   --   GENERICDATE15,
   --   GENERICDATE16,
   --   GENERICDATE17,
   --   GENERICDATE18,
   --   GENERICDATE19,
   --   GENERICDATE20,
   --   GENERICBOOLEAN1,
   --   GENERICBOOLEAN2,
   --   GENERICBOOLEAN3,
   --   GENERICBOOLEAN4,
   --   GENERICBOOLEAN5,
   --   GENERICBOOLEAN6,
   --   GENERICBOOLEAN7,
   --   GENERICBOOLEAN8,
   --   GENERICBOOLEAN9,
   --   GENERICBOOLEAN10,
   --   GENERICBOOLEAN11,
   --   GENERICBOOLEAN12,
   --   GENERICBOOLEAN13,
   --   GENERICBOOLEAN14,
   --   GENERICBOOLEAN15,
   --   GENERICBOOLEAN16,
   --   GENERICBOOLEAN17,
   --   GENERICBOOLEAN18,
   --   GENERICBOOLEAN19,
   --   GENERICBOOLEAN20,
   --   GENERICNUMBER1,
   --   UNITTYPEFORGENERICNUMBER1,
   --   GENERICNUMBER2,
   --   UNITTYPEFORGENERICNUMBER2,
   --   GENERICNUMBER3,
   --   UNITTYPEFORGENERICNUMBER3,
   --   GENERICNUMBER4,
   --   UNITTYPEFORGENERICNUMBER4,
   --   GENERICNUMBER5,
   --   UNITTYPEFORGENERICNUMBER5,
   --   GENERICNUMBER6,
   --   UNITTYPEFORGENERICNUMBER6,
   --   GENERICNUMBER7,
   --   UNITTYPEFORGENERICNUMBER7,
   --   GENERICNUMBER8,
   --   UNITTYPEFORGENERICNUMBER8,
   --   GENERICNUMBER9,
   --   UNITTYPEFORGENERICNUMBER9,
   --   GENERICNUMBER10,
   --   UNITTYPEFORGENERICNUMBER10,
   --   GENERICNUMBER11,
   --   UNITTYPEFORGENERICNUMBER11,
   --   GENERICNUMBER12,
   --   UNITTYPEFORGENERICNUMBER12,
   --   GENERICNUMBER13,
   --   UNITTYPEFORGENERICNUMBER13,
   --   GENERICNUMBER14,
   --   UNITTYPEFORGENERICNUMBER14,
   --   GENERICNUMBER15,
   --   UNITTYPEFORGENERICNUMBER15,
   --   GENERICNUMBER16,
   --   UNITTYPEFORGENERICNUMBER16,
   --   GENERICNUMBER17,
   --   UNITTYPEFORGENERICNUMBER17,
   --   GENERICNUMBER18,
   --   UNITTYPEFORGENERICNUMBER18,
   --   GENERICNUMBER19,
   --   UNITTYPEFORGENERICNUMBER19,
   --   GENERICNUMBER20,
   --   UNITTYPEFORGENERICNUMBER20
   --   )
   --values
   --  ('AIAS',salestransactionseq,
   --   0,
   -- --Added by Suresh 10292017
   --   PROCESSINGUNITSEQ,
   --   compensationDate,
   --   --End by Suresh 10292017
   --   GA1,
   --   GA2,
   --   GA3,
   --   GA4,
   --   GA5,
   --   GA6,
   --   GA7,
   --   GA8,
   --   GA9,
   --   GA10,
   --   GA11,
   --   GA12,
   --   GA13,
   --   GA14,
   --   GA15,
   --   GA16,
   --   GA17,
   --   GA18,
   --   GA19,
   --   GA20,
   --   GD1,
   --   GD2,
   --   GD3,
   --   GD4,
   --   GD5,
   --   GD6,
   --   GD7,
   --   GD8,
   --   GD9,
   --   GD10,
   --   GD11,
   --   GD12,
   --   GD13,
   --   GD14,
   --   GD15,
   --   GD16,
   --   GD17,
   --   GD18,
   --   GD19,
   --   GD20,
   --   GB1,
   --   GB2,
   --   GB3,
   --   GB4,
   --   GB5,
   --   GB6,
   --   GB7,
   --   GB8,
   --   GB9,
   --   GB10,
   --   GB11,
   --   GB12,
   --   GB13,
   --   GB14,
   --   GB15,
   --   GB16,
   --   GB17,
   --   GB18,
   --   GB19,
   --   GB20,
   --   GN1,
   --   UNITTYPEFORGN1,
   --   GN2,
   --   UNITTYPEFORGN2,
   --   GN3,
   --   UNITTYPEFORGN3,
   --   GN4,
   --   UNITTYPEFORGN4,
   --   GN5,
   --   UNITTYPEFORGN5,
   --   GN6,
   --   UNITTYPEFORGN6,
   --   GN7,
   --   UNITTYPEFORGN7,
   --   GN8,
   --   UNITTYPEFORGN8,
   --   GN9,
   --   UNITTYPEFORGN9,
   --   GN10,
   --   UNITTYPEFORGN10,
   --   GN11,
   --   UNITTYPEFORGN11,
   --   GN12,
   --   UNITTYPEFORGN12,
   --   GN13,
   --   UNITTYPEFORGN13,
   --   GN14,
   --   UNITTYPEFORGN14,
   --   GN15,
   --   UNITTYPEFORGN15,
   --   GN16,
   --   UNITTYPEFORGN16,
   --   GN17,
   --   UNITTYPEFORGN17,
   --   GN18,
   --   UNITTYPEFORGN18,
   --   GN19,
   --   UNITTYPEFORGN19,
   --   GN20,
   --   UNITTYPEFORGN20)
    --create new writing agency txta

  --version 8 only insert into piaor_assignment
    insert all when salestransactionSeq > 0 then into PIAOR_Assignment
      (tenantid,salestransactionSeq,
       salesOrderSeq,
       setNumber,
       positionName,
       compensationDate,
       Genericattribute4,
       Genericattribute5,
       Genericattribute6,
       Genericattribute7,
       Genericattribute8,
       Genericattribute9,
       Genericdate6,
       PROCESSINGUNITSEQ)
    Values
      ('AIAS',Salestransactionseq,
       Salesorderseq,
       1,
       Positionname,
       V_Compdate,
       'PI',
       'PI - Direct Team',
       Ga6_Wagency,
       Ga7_Incepttitle,
       Ga8_Wdistrict,
       Ga9_Yrend,
       Compensationdate,
       PROCESSINGUNITSEQ)
    --create new writing distrcit txta
    when salestransactionSeq > 0 then into PIAOR_Assignment--cs_transactionAssignment
      (tenantid, salestransactionSeq,
       salesOrderSeq,
       setNumber,
       positionName,
       compensationDate,
       Genericattribute4,
       Genericattribute5,
       Genericattribute6,
       Genericattribute7,
       Genericattribute8,
       Genericattribute9,
       Genericdate6,PROCESSINGUNITSEQ)
    Values
      ('AIAS' , Salestransactionseq,
       Salesorderseq,
       2,
       Ga8_Wdistrict,
       V_Compdate,
       'PI',
       'PI - Indirect Team',
       Ga6_Wagency,
       Ga7_Incepttitle,
       Ga8_Wdistrict,
       Ga9_Yrend,
       Compensationdate, processingunitseq)
  when salestransactionSeq > 0 then into AIA_YrEnd_Tran_rela
  VALUES
     (Salestransactionseq,
      oldtrxnseq
     )
      Select   /*+ LEADING(ta,st,gata)  USE_NL(ta,st) USE_NL(ta,cp) INDEX(ta,aia_cs_transactionassign_idx2) no_expand  */ rownum as rn,
             v_txnSeq + rownum As Salestransactionseq,
             st.salestransactionseq as oldtrxnseq,
             ta.salesorderseq,
             ta.compensationDate,
             nvl(v_modificationTime, st.modificationDate) as modificationDate,
             ta.PositionName,
             Ta.Genericattribute4 As Ga4_Piaor,
             Ta.Genericattribute6 As Ga6_Wagency,
             'FSAD' as GA7_inceptTitle, --only assign as FSAD title
             ta.genericAttribute8 as GA8_WDistrict,
             'YE REASSIGN TO DISTRICT' as GA9_yrEnd,
             cp.genericAttribute11 curTitle, -- new title
             cpa.genericAttribute1 status,
             st.eventtypeSeq, st.linenumber,
maxsublinenumber + rownum as SUBLINENUMBER,
             st.value,
             st.unittypeforvalue,
             1 as isRunnable,
             st.ORIGINTYPEID,
             st.PREADJUSTEDVALUE,
             st.UNITTYPEFORPREADJUSTEDVALUE,
             st.PROCESSINGUNITSEQ,
             st.BusinessUnitMap,
             gv_pipelinerunseq as pipelineRunSeq,
             st.unittypeForLineNumber,
             st.unitTypeForSubLineNumber,
             --genericFields
             st.productId,
             st.productName,
             st.productDescription,
             st.dataSource,
             st.genericAttribute1,
             st.genericAttribute2,
             st.genericAttribute3,
             st.genericAttribute4,
             st.genericAttribute5,
             st.genericAttribute6,
             st.genericAttribute7,
             st.genericAttribute8,
             st.genericAttribute9,
             st.genericAttribute10,
             st.genericAttribute11,
             st.genericAttribute12,
             st.genericAttribute13,
             st.genericAttribute14,
             st.genericAttribute15,
             st.genericAttribute16,
             st.genericAttribute17,
             st.genericAttribute18,
             st.genericAttribute19,
             st.genericAttribute20,
             st.genericAttribute21,
             st.genericAttribute22,
             st.genericAttribute23,
             st.genericAttribute24,
             st.genericAttribute25,
             st.genericAttribute26,
             st.genericAttribute27,
             st.genericAttribute28,
             st.genericAttribute29,
             st.genericAttribute30,
             st.genericAttribute31,
             st.genericAttribute32,
             st.genericNumber1,
             st.genericNumber2,
             st.genericNumber3,
             st.genericNumber4,
             st.genericNumber5,
             st.genericNumber6,
             st.unitTypeForGenericNumber1,
             st.unitTypeForGenericNumber2,
             st.unitTypeForGenericNumber3,
             st.unitTypeForGenericNumber4,
             st.unitTypeForGenericNumber5,
             st.unitTypeForGenericNumber6,
             st.genericDate1,
             st.genericDate2,
             st.genericDate3,
             st.genericDate4,
             st.genericDate5,
             st.genericBoolean1,
             st.genericBoolean2,
             st.genericBoolean3,
             st.genericBoolean4,
             st.genericBoolean5,
             st.GENERICBOOLEAN6,
             ----extend generic fields
             gata.GENERICATTRIBUTE1          as GA1,
             gata.GENERICATTRIBUTE2          as GA2,
             gata.GENERICATTRIBUTE3          as GA3,
             gata.GENERICATTRIBUTE4          as GA4,
             gata.GENERICATTRIBUTE5          as GA5,
             gata.GENERICATTRIBUTE6          as GA6,
             gata.GENERICATTRIBUTE7          as GA7,
             gata.GENERICATTRIBUTE8          as GA8,
             gata.GENERICATTRIBUTE9          as GA9,
             gata.GENERICATTRIBUTE10         as GA10,
             gata.GENERICATTRIBUTE11         as GA11,
             gata.GENERICATTRIBUTE12         as GA12,
             gata.GENERICATTRIBUTE13         as GA13,
             gata.GENERICATTRIBUTE14         as GA14,
             gata.GENERICATTRIBUTE15         as GA15,
             gata.GENERICATTRIBUTE16         as GA16,
             gata.GENERICATTRIBUTE17         as GA17,
             gata.GENERICATTRIBUTE18         as GA18,
             gata.GENERICATTRIBUTE19         as GA19,
             gata.GENERICATTRIBUTE20         as GA20,
             gata.GENERICDATE1               as GD1,
             gata.GENERICDATE2               as GD2,
             gata.GENERICDATE3               as GD3,
             gata.GENERICDATE4               as GD4,
             gata.GENERICDATE5               as GD5,
             gata.GENERICDATE6               as GD6,
             gata.GENERICDATE7               as GD7,
             gata.GENERICDATE8               as GD8,
             gata.GENERICDATE9               as GD9,
             gata.GENERICDATE10              as GD10,
             gata.GENERICDATE11              as GD11,
             gata.GENERICDATE12              as GD12,
             gata.GENERICDATE13              as GD13,
             gata.GENERICDATE14              as GD14,
             gata.GENERICDATE15              as GD15,
             gata.GENERICDATE16              as GD16,
             gata.GENERICDATE17              as GD17,
             gata.GENERICDATE18              as GD18,
             gata.GENERICDATE19              as GD19,
             gata.GENERICDATE20              as GD20,
             gata.GENERICBOOLEAN1            as GB1,
             gata.GENERICBOOLEAN2            as GB2,
             gata.GENERICBOOLEAN3            as GB3,
             gata.GENERICBOOLEAN4            as GB4,
             gata.GENERICBOOLEAN5            as GB5,
             gata.GENERICBOOLEAN6            as GB6,
             gata.GENERICBOOLEAN7            as GB7,
             gata.GENERICBOOLEAN8            as GB8,
             gata.GENERICBOOLEAN9            as GB9,
             gata.GENERICBOOLEAN10           as GB10,
             gata.GENERICBOOLEAN11           as GB11,
             gata.GENERICBOOLEAN12           as GB12,
             gata.GENERICBOOLEAN13           as GB13,
             gata.GENERICBOOLEAN14           as GB14,
             gata.GENERICBOOLEAN15           as GB15,
             gata.GENERICBOOLEAN16           as GB16,
             gata.GENERICBOOLEAN17           as GB17,
             gata.GENERICBOOLEAN18           as GB18,
             gata.GENERICBOOLEAN19           as GB19,
             gata.GENERICBOOLEAN20           as GB20,
             gata.GENERICNUMBER1             as GN1,
             gata.UNITTYPEFORGENERICNUMBER1  aS UNITTYPEFORGN1,
             gata.GENERICNUMBER2             as GN2,
             gata.UNITTYPEFORGENERICNUMBER2  as UNITTYPEFORGN2,
             gata.GENERICNUMBER3             as GN3,
             gata.UNITTYPEFORGENERICNUMBER3  as UNITTYPEFORGN3,
             gata.GENERICNUMBER4             as GN4,
             gata.UNITTYPEFORGENERICNUMBER4  as UNITTYPEFORGN4,
             gata.GENERICNUMBER5             as GN5,
             gata.UNITTYPEFORGENERICNUMBER5  as UNITTYPEFORGN5,
             gata.GENERICNUMBER6             as GN6,
             gata.UNITTYPEFORGENERICNUMBER6  as UNITTYPEFORGN6,
             gata.GENERICNUMBER7             as GN7,
             gata.UNITTYPEFORGENERICNUMBER7  as UNITTYPEFORGN7,
             gata.GENERICNUMBER8             as GN8,
             gata.UNITTYPEFORGENERICNUMBER8  as UNITTYPEFORGN8,
             gata.GENERICNUMBER9             as GN9,
             gata.UNITTYPEFORGENERICNUMBER9  as UNITTYPEFORGN9,
             gata.GENERICNUMBER10            as GN10,
             gata.UNITTYPEFORGENERICNUMBER10 as UNITTYPEFORGN10,
             gata.GENERICNUMBER11            as GN11,
             gata.UNITTYPEFORGENERICNUMBER11 as UNITTYPEFORGN11,
             gata.GENERICNUMBER12            as GN12,
             gata.UNITTYPEFORGENERICNUMBER12 as UNITTYPEFORGN12,
             gata.GENERICNUMBER13            as GN13,
             gata.UNITTYPEFORGENERICNUMBER13 as UNITTYPEFORGN13,
             gata.GENERICNUMBER14            as GN14,
             gata.UNITTYPEFORGENERICNUMBER14 as UNITTYPEFORGN14,
             gata.GENERICNUMBER15            as GN15,
             gata.UNITTYPEFORGENERICNUMBER15 as UNITTYPEFORGN15,
             gata.GENERICNUMBER16            as GN16,
             gata.UNITTYPEFORGENERICNUMBER16 as UNITTYPEFORGN16,
             gata.GENERICNUMBER17            as GN17,
             gata.UNITTYPEFORGENERICNUMBER17 as UNITTYPEFORGN17,
             gata.GENERICNUMBER18            as GN18,
             gata.UNITTYPEFORGENERICNUMBER18 as UNITTYPEFORGN18,
             gata.GENERICNUMBER19            as GN19,
             gata.UNITTYPEFORGENERICNUMBER19 as UNITTYPEFORGN19,
             gata.GENERICNUMBER20            as GN20,
             gata.UNITTYPEFORGENERICNUMBER20 as UNITTYPEFORGN20
        from PIAOR_ASSIGNMENT ta,
             cs_salestransaction      st,
--             cs_eventType             et,
             cs_position              cp,
             cs_participant           cpa,
             Cs_Gasalestransaction    Gata, AIA_MAX_sublinenumber
       Where
       -- et.tenantid='AIAS'
       -- and et.eventTypeId in ('RYC', 'H_RYC', 'ORYC')
       -- AND et.removeDate = cdt_EndOfTime
       -- and et.tenantid=st.tenantid
       -- And Et.Datatypeseq = St.Eventtypeseq
       -- AND
       AIA_MAX_sublinenumber.salesorderseq=st.salesorderseq and
        st.tenantid='AIAS' and St.Eventtypeseq IN (v1,v2,v3)
        and st.processingunitseq=Gv_Processingunitseq
        and st.compensationDate >= v_yrStartDate
        And St.Compensationdate < V_Yrenddate
        and ta.compensationDate >= v_yrStartDate
        And ta.Compensationdate < V_Yrenddate
        and st.genericdate2 < to_date('12/1/2015', 'mm/dd/yyyy')
        and ta.tenantid='AIAS'
        and ta.processingunitseq = Gv_Processingunitseq
        and st.compensationdate = ta.compensationdate
        and st.salesTransactionSeq = ta.salesTransactionSeq
        AND Ta.Genericattribute5 = 'PI - Direct Team' -- changed check ga5, and only create trxn for pi-direct team
        and ta.genericAttribute7 = 'FSAD'
        and cp.tenantid ='AIAS'
        and ta.positionName = cp.name
        And Cp.Effectivestartdate <= V_Compdate
        and cp.effectiveEndDate > V_Compdate
        and cp.removeDate = cdt_EndOfTime
        and cpa.tenantid='AIAS'
        and Cp.Payeeseq = Cpa.Payeeseq
        And Cpa.Effectivestartdate <= V_Compdate
        And Cpa.Effectiveenddate > V_Compdate
        and cpa.removeDate = cdt_EndOfTime
        And ((Cp.Genericattribute11 In ('FSAD', 'FSD') And
             Cpa.Genericattribute1 Not In ('00', '0')) OR
             (CP.Genericattribute11 In ('AM', 'FSM', 'FSC') And
             CPa.Genericattribute1 In ('00', '0')))
        and st.tenantid = gata.tenantid(+)
        And St.Salestransactionseq = Gata.Salestransactionseq(+)
        and gata.pagenumber(+) = 0
        -- and st.salestransactionseq=14636698977976073
         ;

  Log('31 get year end reassign data : '||SQL%ROWCOUNT);
    commit;



   --add value for new payee which contribute from old payee

--   execute immediate 'delete from PIAOR_DETAIL where Component =''PI REASSIGN''';

 --version 9 add
  delete from PIAOR_DETAIL where period=v_periodSeq and Component ='PI REASSIGN' ;

  commit;


    insert into PIAOR_DETAIL(
         Period,
         PIAOR_Year,
         Component,
         Wri_Agency,
         Wri_Agent,
         Payee_Agency,
         Rule,
     PA_RYC,--version 9
     LF_RYC,--version 9
         RYC,
         Yearend_old_Payee
  )
  SELECT v_periodSeq,
         v_piaor_year,
         'PI REASSIGN',
         f.Genericattribute6,  --Wri_Agency
         s.genericAttribute12, --Wri_Agent
         f.positionName,
         f.genericAttribute5,
     --version 9 add production breakdown value
     sum(case when s.PRODUCTNAME='PA'  and f.setnumber = 1 then 0-s.value --old payee PA value
              when s.PRODUCTNAME='PA'  and f.setnumber <>1 then s.value   --new payee PA value
          else 0 end) PA_RYC,
     sum(case when s.PRODUCTNAME<>'PA' and f.setnumber = 1 then 0-s.value --old payee LF value
              when s.PRODUCTNAME<>'PA' and f.setnumber <>1 then s.value   --new payee LF value
          else 0 end) LF_RYC,
     sum(case when f.setnumber = 1 then 0-s.value else s.value end),
         case when f.setnumber =2 then f2.positionName else null end
    from PIAOR_ASSIGNMENT f,
         cs_salestransaction s,
         PIAOR_ASSIGNMENT f2,
         AIA_YrEnd_Tran_rela rela
   where f.genericAttribute4 like '%PI%'
       and f.Genericattribute9 = 'YE REASSIGN TO DISTRICT'
       and f.COMPENSATIONDATE = v_compDate
       --and f.salestransactionseq = s.salestransactionseq
       and f.salestransactionseq = rela.salestransactionseq
       and rela.oldtrxnseq = s.salestransactionseq
       and f2.genericAttribute4 like '%PI%'
       and f2.Genericattribute9 = 'YE REASSIGN TO DISTRICT'
       and f2.COMPENSATIONDATE = v_compDate
       and f2.salestransactionseq = f.salestransactionseq
     and f2.setnumber=1
  group by f.Genericattribute6,
       s.genericAttribute12,
       f.positionName,
       f.genericAttribute5,
       case when f.setnumber =2 then f2.positionName else null end
           ;

   log('32 update year end new payee data : '||SQL%ROWCOUNT);

     commit;

--UPDATE PIAOR PAYEE AGENT

  Merge into PIAOR_DETAIL pd
  using (select d.name,
                d.genericattribute2 as Payee_agent
           from cs_position d
          where d.removedate = Cdt_Endoftime
            and d.effectivestartdate <= V_Compdate
            and d.effectiveenddate > V_Compdate
          ) t
  on (pd.Payee_Agency = t.name and pd.piaor_year = v_piaor_year)
  when matched then
       update set pd.PAYEE_AGENT = t.Payee_agent;

   log('33 update payee agent : '||SQL%ROWCOUNT);

Commit;


--UPDATE PIAOR STATUS

  Merge into PIAOR_DETAIL pd
  using (select d.name,
                p.genericattribute1
           from cs_position d, cs_participant p
          where d.removedate = Cdt_Endoftime
            and d.effectivestartdate <= V_Compdate
            and d.effectiveenddate > V_Compdate
            and p.payeeseq = d.payeeseq
            and p.removedate = Cdt_Endoftime
            and p.effectivestartdate <= V_Compdate
            and p.effectiveenddate > V_Compdate) t
  on ('SGT'||pd.Payee_Agent = t.name and pd.piaor_year = v_piaor_year)
  when matched then
       update set pd.status = t.genericattribute1;

   log('34 update year end AOR status : '||SQL%ROWCOUNT);

Commit;

--update all year PI data


  Merge into PIAOR_DETAIL pd
  using (select d.name,
                g.genericboolean2,
                g.genericboolean3,
                d.genericattribute2,
                p.genericattribute1,
                d.genericattribute11
           from cs_position d, cs_gaposition g, cs_participant p
          where d.removedate = Cdt_Endoftime
            and d.effectivestartdate <= V_Compdate
            and d.effectiveenddate > V_Compdate
            and d.ruleelementownerseq = g.ruleelementownerseq
            and g.removedate = Cdt_Endoftime
            and g.effectivestartdate <= V_Compdate
            and g.effectiveenddate > V_Compdate
            and p.payeeseq = d.payeeseq
            and p.removedate = Cdt_Endoftime
            and p.effectivestartdate <= V_Compdate
            and p.effectiveenddate > V_Compdate) t
  on ('SGT'||pd.Payee_Agent = t.name and pd.piaor_year = v_piaor_year and pd.component like '%PI%')
  when matched then
    update set pd.RI = case when t.genericboolean3 = 1 and pd.rule = 'PI - Direct Team' then 1 else 0 end,
               pd.CB = case when t.genericboolean2 = 1 and t.genericattribute2 = pd.Wri_Agent then 1 else 0 end,
               pd.FSAD_Exclude_Indirect = case when t.genericattribute11 = 'FSAD' and pd.rule = 'PI - Indirect Team' then 1 else 0 end
       ;

   log('35 update year end PI and AOR data : '||SQL%ROWCOUNT);

Commit;

--update aor status




 log('SP_TXNTXA_YREND_PI: end');


    --     SequenceGenPkg.UpdateSeq('auditLogSeq');

  exception
    when NO_YRENDEVENTTYPE_FOUND then
      gv_error := 'Error [SP_TXNTXA_YREND_PI]: the PI_Year_End event type is not found - '  ||
                  dbms_utility.format_error_backtrace;
      raise_application_error(-20000, gv_error);

    when INVALID_PERIODDATE then
      gv_error := 'Error [SP_TXNTXA_YREND_PI]: the year start date  date are invalid - '  ||
                  dbms_utility.format_error_backtrace;
      raise_application_error(-20000, gv_error);
    when others then
      rollback;
      gv_error := 'Error [SP_TXNTXA_YREND_PI]: '  || sqlerrm || ' - ' ||
                  dbms_utility.format_error_backtrace;

      comDebugger('PIAOR YR DEBUGGER', 'ERROR' || gv_error);

      raise_application_error(-20000, gv_error);

  end SP_TXNTXA_YREND_PI;


 --version 8 add year end procedure

  PROCEDURE SP_YEAR_END_CALCULATION As
      v_Periodseq       int;
      v_periodStartDate date;
      v_Periodenddate   date;
      v_piaor_year      varchar2(100);
      v_rtn             int := 0;


  Begin

  log('SP_YEAR_END_CALCULATION: start');
  log('gv_CYCLE_DATE: '||gv_CYCLE_DATE);
  log('gv_calendarSeq: '||gv_calendarSeq);
    --get period startDate, endDate

  select cp.PERIODSEQ,cp.startDate,cp.endDate
    into v_periodSeq,v_periodStartDate,v_periodEndDate
    from CS_PERIOD cp,
       cs_periodtype pt
   where cp.tenantid='AIAS'
     and cp.REMOVEDATE=cdt_EndOfTime
     and cp.CALENDARSEQ=gv_calendarSeq
     and cp.startdate<=to_date(gv_CYCLE_DATE,'yyyy-mm-dd')
     and cp.enddate>to_date(gv_CYCLE_DATE,'yyyy-mm-dd')
     and pt.name = 'month'
     and pt.periodtypeseq=cp.periodtypeseq
;


    v_rtn := comGetYrLastMonth(v_periodSeq);

    if v_rtn < 1 then
    log('SP_YEAR_END_CALCULATION: not year end month');
      return;
    end if;

  if v_periodStartDate>=date'2016-12-01' and v_periodEndDate <= date'2018-01-01'   --for 2017
  then

    SELECT SUBSTR(C.NAME,1,4)
       INTO V_PIAOR_YEAR
       FROM CS_PERIOD A,
            CS_PERIOD B,
            CS_PERIOD C
        WHERE A.tenantid = 'AIAS'
      AND B.tenantid = 'AIAS'
      AND C.tenantid = 'AIAS'
      AND A.removeDate= cdt_EndOfTime
      AND B.removeDate= cdt_EndOfTime
      AND C.removeDate= cdt_EndOfTime
      AND A.PERIODSEQ = v_periodSeq
      AND A.calendarSeq = B.calendarSeq
          AND A.PARENTSEQ = B.PERIODSEQ
      AND B.calendarSeq = C.calendarSeq
          AND B.PARENTSEQ = C.PERIODSEQ;

  else

    select extract(year from startdate)
    into V_PIAOR_YEAR
    from cs_period
   where tenantid = 'AIAS'
     and removeDate = cdt_EndOfTime
     and PERIODSEQ = v_periodSeq;


  end if;



log('v_periodSeq: ' ||v_periodSeq);
log('v_periodStartDate: '||v_periodStartDate);
log('v_periodEndDate: '||v_periodEndDate);
log('v_piaor_year: '||v_piaor_year);

commit;


  --init table



    delete from PIAOR_Payment where Year =v_piaor_year;

  log('init table: '||sql%rowcount);

    commit;

  --AOR

   insert into PIAOR_Payment(
    Year,
      District,
      Agency,
      Payee_Name,
      Payee_Code,
      Title,
      Inforce,
      Annual_PIB,
      Annual_RYC,
      type,
      PAYMENT_YEAR
  )
    with tmp_pos as
    (select *
       from cs_position
    where removeDate=cdt_EndofTime
        and effectiveStartDate<=v_periodEndDate -1
        and effectiveEndDate > v_periodEndDate-1
     )
   select pd.PIAOR_YEAR as Year,
          tp.genericattribute3 as District,
          pd.PAYEE_AGENCY as Agency,
          tp.genericattribute7 as Payee_Name,
          pd.PAYEE_AGENT as Payee_Code,
          tp.genericattribute11 as Title,
          case when pd.status in ('00','0') then 'Y' else 'N' end as Inforce,
          --version 9
          --sum(pd.PIB) as Annual_PIB,
          --sum(pd.RYC) as Annual_RYC,
          sum(nvl(pd.PIB,0)) as Annual_PIB,
          sum(nvl(pd.RYC,0)) as Annual_RYC,
          pd.component as type,
           to_char(cast(pd.PIAOR_YEAR as integer)+1) as PAYMENT_YEAR
   from piaor_detail pd,
        tmp_pos tp
  where pd.PIAOR_YEAR=v_piaor_year
    and 'SGT'||pd.payee_agent=tp.name
    and pd.component='AOR'
    group by pd.PIAOR_YEAR,
               tp.genericattribute3,
               pd.PAYEE_AGENCY,
               tp.genericattribute7,
               pd.PAYEE_AGENT,
               tp.genericattribute11,
               pd.status,
               pd.component,
                cast(pd.PIAOR_YEAR as integer)+1
    ;

    log('sum AOR detail: '||sql%rowcount);

    commit;


  --PI

    insert into PIAOR_Payment(
    Year,
      District,
      Agency,
      Payee_Name,
      Payee_Code,
      Title,
      Inforce,
      RI,
      CB,
      Annual_RYC,
      Persistency,
      type,
    PAYMENT_YEAR
  )
    with tmp_pos as
    (select *
       from cs_position
    where removeDate=cdt_EndofTime
        and effectiveStartDate<=v_periodEndDate -1
        and effectiveEndDate > v_periodEndDate-1
     )
   select pd.PIAOR_YEAR as Year,
          tp.genericattribute3 as District,
          pd.PAYEE_AGENCY as Agency,
          tp.genericattribute7 as Payee_Name,
          pd.PAYEE_AGENT as Payee_Code,
          tp.genericattribute11 as Title,
          case when pd.status in ('00','0') then 'Y' else 'N' end as Inforce,
          pd.RI as RI,
          pd.CB as CB,
          --version 9
          --sum(pd.RYC) as Annual_RYC,
          sum(nvl(pd.RYC,0)) as Annual_RYC,
          case when tp.genericattribute11 = 'FSD' then tl1.PER_CC_P12
               else tl2.PER_CC_P12 end as Persistency,
          'PI' as type,
           to_char(cast(pd.PIAOR_YEAR as integer)+1) as PAYMENT_YEAR
   from piaor_detail pd,
        tmp_pos tp,
      (select agy,agt,per_cc_p12,agent_type
           from per_limra
          where cycle_mth = v_periodEndDate-1
            and LIMRA_TYPE = 'LIMPI'
            and CMCD = 'SG'
            and agent_type='03'
            and agy not like 'A%'  --version 11
         ) tl1,
        (select agy,agt,per_cc_p12,agent_type
           from per_limra
          where cycle_mth = v_periodEndDate-1
            and LIMRA_TYPE = 'LIMPI'
            and CMCD = 'SG'
          and agent_type='02'
          and agy not like 'A%'   --version 11
         ) tl2
  where pd.PIAOR_YEAR=v_piaor_year
    and 'SGT'||pd.payee_agent=tp.name
    and pd.component in ('PI','PI REASSIGN')
    and pd.payee_agent=tl1.agt(+)
    and pd.payee_agent=tl2.agt(+)
    and pd.RI<>1
    and pd.CB<>1
    and pd.FSAD_Exclude_Indirect<>1
    group by pd.PIAOR_YEAR,
               tp.genericattribute3,
               pd.PAYEE_AGENCY,
               tp.genericattribute7,
               pd.PAYEE_AGENT,
               tp.genericattribute11,
               pd.status,
               pd.RI,
               pd.CB,
               case when tp.genericattribute11 = 'FSD' then tl1.PER_CC_P12
                    else tl2.PER_CC_P12 end,
               cast(pd.PIAOR_YEAR as integer)+1
    ;

    log('sum PI detail: '||sql%rowcount);

    commit;


   --update PI_rate

   update PIAOR_Payment pp
      set PI_rate =case when pp.Persistency >= 0.9  then 0.3
                          when pp.Persistency >= 0.85 then 0.25
                          when pp.Persistency >= 0.8  then 0.15
                          else 0 end
   where pp.Year=v_piaor_year
     and pp.type='PI'
   and pp.Inforce='Y'
   ;

   log('update pi rate');

   commit;

   --update payment

   update PIAOR_Payment pp
   set pp.Payment=case when pp.type ='PI' then (Annual_RYC*PI_rate)/12
                       else Annual_PIB*AOR_PIB_Rate+Annual_RYC*AOR_RYC_Rate
                       end
     where pp.year = v_piaor_year
   and pp.Inforce='Y'
   ;

     log('update AOR and PI payment') ;

   commit;




  log('SP_YEAR_END_CALCULATION: end');

    Exception
    When Others Then

      COMDEBUGGER('SP_YEAR_END_CALCULATION error: ', sqlerrm);

    return;


  end SP_YEAR_END_CALCULATION;






  PROCEDURE SP_PIAOR_CALCULATION_ALL IS BEGIN

    Log('SP_PIAOR_CALCULATION_ALL Started');

    INIT;


    SP_TXA_PIAOR;

    SP_MONTHLY_AGGREGATE;

    SP_TXNTXA_YREND_PI;

    SP_YEAR_END_CALCULATION;


    Log('SP_PIAOR_CALCULATION_ALL Ended');
    COMMIT;

  END SP_PIAOR_CALCULATION_ALL;







  function comGetCrossoverAgy (i_comp in varchar2,I_wAgyLdr in varchar2, i_policyIssueDate in date) return string --add by nelson
is
  v_oldDM varchar2(30);
  v_countsetup number(10);
  v_odm varchar2(30);
  v_effdate date;
Begin

    --Log('70');


--add by nelson start

    if i_comp = 'PI' then

      ---add version 5 (If Payee only include one setup , it will assign to old DM)
      --version 8 comment
     -- select count(1), max(TXTOLDDMCODE), max(Dteeffectivedate)
     --into v_countsetup, v_odm, v_effdate
     --   from (select max(ST.TXTOLDDMCODE) TXTOLDDMCODE,
     select count(1), max(Txtolddistrict), max(Dteeffectivedate)
        into v_countsetup, v_odm, v_effdate
        from (select max(ST.Txtolddistrict) Txtolddistrict,
                     ST.Dteeffectivedate,
                     ST.Txtagt
                from In_Pi_Aor_Setup ST, Cs_Period PT
               where 'SGT' || to_number(ST.Txtagt) = I_wAgyLdr
                 and I_Policyissuedate <= ST.Dteeffectivedate
                 and ST.Dtecycle = PT.Enddate - 1
                 and PT.Periodseq = Gv_Periodseq
                 And ST.Txttype in ('C')
                 and ST.Decstatus = 0
               group by ST.Dteeffectivedate, ST.Txtagt)
       group by Txtagt;

      if v_countsetup = 1 then
        V_oldDM := v_odm;
        Gv_Crossovereffectivedate := v_effdate;
      else

      select ST.Txtolddistrict, ST.Dteeffectivedate
      Into v_oldDM, Gv_Crossovereffectivedate
      from  In_Pi_Aor_Setup ST,Cs_Period PT
      where 'SGT'||to_number(ST.Txtagt)=I_wAgyLdr
      and   ST.Dteeffectivedate = (
         Select MIN(S.Dteeffectivedate)
          From In_Pi_Aor_Setup S,Cs_Period P
          Where 'SGT'||to_number(S.Txtagt)=I_wAgyLdr
          And I_Policyissuedate <= S.Dteeffectivedate
          And S.Dtecycle = P.Enddate - 1
          And P.Periodseq=Gv_Periodseq
          And S.Txttype in ('C')
          and S.Decstatus = 0)
      and ST.Dtecycle = PT.Enddate -1
      and PT.Periodseq=Gv_Periodseq
      and I_Policyissuedate >= (select max(g.effectiveenddate) from sh_agent_role g
                                   where g.agentcode = to_number(ST.Txtagt)
                                   and   g.effectiveenddate < ST.dteeffectivedate);

    end if;
    elsif   i_comp = 'AOR' then

      ---add version 5 (If Payee only include one setup , it will assign to old DM)
      select count(1), max(Txtolddistrict), max(Dteeffectivedate)
        into v_countsetup, v_odm, v_effdate
        from (select max(ST.Txtolddistrict) Txtolddistrict,
                     ST.Dteeffectivedate,
                     ST.Txtagt
                from In_Pi_Aor_Setup ST, Cs_Period PT
               where 'SGT' || to_number(ST.Txtagt) = I_wAgyLdr
                 and I_Policyissuedate <= ST.Dteeffectivedate
                 and ST.Dtecycle = PT.Enddate - 1
                 and PT.Periodseq = Gv_Periodseq
                 And ST.Txttype in ('C','D')
                 and ST.Decstatus = 0
               group by ST.Dteeffectivedate, ST.Txtagt)
       group by Txtagt;

       if v_countsetup = 1 then
        V_oldDM := v_odm;
        Gv_Crossovereffectivedate := v_effdate;

      else


      select ST.Txtolddistrict, ST.Dteeffectivedate
      Into v_oldDM, Gv_Crossovereffectivedate
      from  In_Pi_Aor_Setup ST,Cs_Period PT
      where PT.tenantid='AIAS' and 'SGT'||to_number(ST.Txtagt)=I_wAgyLdr
      and   ST.Dteeffectivedate = (
         Select MIN(S.Dteeffectivedate)
          From In_Pi_Aor_Setup S,Cs_Period P
          Where 'SGT'||to_number(S.Txtagt)=I_wAgyLdr
          And I_Policyissuedate <= S.Dteeffectivedate
          And S.Dtecycle = p.enddate-1
          And P.Periodseq=Gv_Periodseq
          And S.Txttype in ('C','D')
          and S.Decstatus = 0)
      and ST.Dtecycle = PT.Enddate - 1
      and PT.Periodseq=Gv_Periodseq
      and I_Policyissuedate >= (select max(g.effectiveenddate) from sh_agent_role g
                                   where g.agentcode = to_number(ST.Txtagt)
                                   and   g.effectiveenddate < ST.dteeffectivedate);

    end if;

    end if;
    --Log('70');

    return v_oldDM;
--add by nelson end

    exception when no_data_found then
     --Log('70');
     return null;

end comGetCrossoverAgy;


Procedure Comtransferpiaor(I_R_Agydisttrxn In R_Agydisttrxn)
as

  v_policyIssueDate date;
  V_Compensationdate Date;
  v_maxSetNumber int; --removed
  v_rule varchar2(100);
  v_writingAgyLdr varchar2(30);
  v_wAgency varchar2(30);
  v_wAgencyLeader varchar2(30);
  v_wAgyLdrTitle varchar2(30);
  v_wAgyLdrDistrict varchar2(30);
  v_Wagtclass varchar2(10);
  vNewWritingAgy varchar2(10);
  vAorNewWritingAgy varchar2(10);
  v_AorRule  varchar2(100);
  V_Aorwagyldr Varchar2(30);
  v_commissionAgy varchar2(30);
  v_CurDistrict Varchar2(30); --add by nelson
  v_LdrCurRole Varchar2(30); --add by nelson
  v_wAgyLdrCde Varchar2(30); --add by nelson
  v_setup  Varchar2(30);--add by nelson
  v_wAgyLdrCurClass varchar2(30);
  v_standalone varchar2(30);--add by jeff
  v_standalone2 number(30);--add by jeff
  v_periodenddate date;--add by jeff

  V_Crossoverflag Int:=0;
  v_ConstantCrossoverDate date:=to_date('1/1/2005','mm/dd/yyyy');



  V_Manageragy Varchar2(30);
  v_RunningType varchar2(255);

  v_OrphanPolicy varchar2(30);

  Invalid_Manager exception;

begin

  /*
begin
    select nvl(max(setNumber),0)
    into v_maxSetNumber
    from cs_transactionAssignment
    where I_R_Agydisttrxn.salestransactionSeq=salestransactionSeq
    And Genericattribute4 Is Null;

  exception when no_data_found then
    v_maxSetNumber:=0;
  end;

  */



    --DBMS_OUTPUT.put_line(c_txn.salestransactionSeq||'   start c_txn  issue date  '||to_char(v_policyIssueDate,'mm/dd/yyyy'));
    --DBMS_OUTPUT.put_line(v_policyIssueDate||' ---- '||v_cutoverdate);

      v_wAgency:=I_R_Agydisttrxn.wAgency;
      v_wAgencyLeader:=I_R_Agydisttrxn.wAgencyLeader;
      v_wAgyLdrTitle:=I_R_Agydisttrxn.wAgyLdrTitle;
      v_wAgyLdrDistrict:=I_R_Agydisttrxn.wAgyLdrDistrict;
      v_Wagtclass:=I_R_Agydisttrxn.Wagtclass;
      V_Policyissuedate:=I_R_Agydisttrxn.Policyissuedate;
      V_Commissionagy:=I_R_Agydisttrxn.Commissionagy;
      V_Runningtype:=I_R_Agydisttrxn.Runningtype;
      v_OrphanPolicy:=I_R_Agydisttrxn.Orphanpolicy;
      v_CurDistrict:=I_R_Agydisttrxn.CurDistrict; --add by nelson
      v_LdrCurRole:=I_R_Agydisttrxn.LdrCurRole; --add by nelson
      v_wAgyLdrCde:=I_R_Agydisttrxn.wAgyLdrCde; --add by nelson
      v_setup:=I_R_Agydisttrxn.setup; --add by nelson

      select enddate into v_periodenddate from cs_period where periodseq=Gv_Periodseq;
      --comDebugger('PIAOR DEBUGGER','WAGENCY['||v_wAgency||']||');


   if I_R_Agydisttrxn.wAgency is not null then

    --comDebugger('PIAOR DEBUGGER','vNewWritingAgy: '||vNewWritingAgy ||' -- '||v_wAgencyLeader);
       -- pi
      If (I_R_Agydisttrxn.Eventtypeid = 'RYC' Or (I_R_Agydisttrxn.Eventtypeid = 'ORYC'
      AND substr(I_R_Agydisttrxn.OrphanPolicy,1,1) ='X'))
      and I_R_Agydisttrxn.productname in ('LF','HS')
      and I_R_Agydisttrxn.txnCode in ('PAY2','PAY3','PAY4','PAY5','PAY6') then
        if v_wAgyLdrTitle = 'FSD' then

           if v_wAgyLdrDistrict=v_commissionAgy then
                v_rule:='PI - Direct Team';
                v_writingAgyLdr:=v_wAgyLdrDistrict;
 --               Log('C2');
          else
            v_rule:='PI - Indirect Team';
            v_writingAgyLdr:=v_wAgyLdrDistrict;
   --         Log('C3');
          end if;

        Elsif  V_Wagyldrtitle='FSAD' Then

           -- If I_R_Agydisttrxn.Agyspinoffindicator='Y' And I_R_Agydisttrxn.Agyspinoffflag=1 Then
           --above checking was disabled
            if I_R_Agydisttrxn.Spinstartdate is not null then
            --spin off case
              V_Writingagyldr:=V_Wagencyleader;
              V_Rule:='PI - Direct Team';
              v_RunningType:=v_RunningType||'_SpinOff';
     --         Log('C4');
            Else
            --non spin off case
                if I_R_Agydisttrxn.Wagtclass <>'12' then -- FSAD no need compare ga13 with district, as long as class=10, writingAgy always get PI

                  select p.genericattribute4
                  into v_wAgyLdrCurClass
                  from cs_position p,cs_period t
                  where p.name = v_wAgyLdrCde
                  and   p.removeDate=Cdt_Endoftime
                  and   t.periodseq = Gv_Periodseq
                  and   p.effectiveStartDate<= t.enddate -1
                  and   p.effectiveEndDate > t.enddate-1 ;

--Log('C5');
                  if v_wAgyLdrCurClass <> '12' Then
                    v_rule:='PI - Direct Team';
                    v_writingAgyLdr:=v_wAgencyLeader  ;
  --                  Log('C6');
                  Else
                    v_rule:='PI - Indirect Team';
                    v_writingAgyLdr:=v_wAgyLdrDistrict;
    --                Log('C7');
                  end if;
                Else
                  v_rule:='PI - Indirect Team';
                  v_writingAgyLdr:=v_wAgyLdrDistrict;
      --            Log('C8');
                End If;
            end if;--spin off chking

        elsif v_wAgyLdrTitle in ('FSM','AM') then
          --
        --  Log('C9');
          If I_R_Agydisttrxn.Agyspinoffindicator='N' And I_R_Agydisttrxn.Agyspinoffflag=1 Then
            --spin off case
              begin
              Select Name
                Into v_managerAgy
                From Cs_Position
               Where Ruleelementownerseq=I_R_Agydisttrxn.Managerseq
                 And Removedate=Cdt_Endoftime
                 And Effectivestartdate<=I_R_Agydisttrxn.Versioningdate
                 and effectiveEndDate>I_R_Agydisttrxn.versioningdate;

              Exception When No_Data_Found Then
                raise Invalid_Manager;
              End;
--Log('C10');
              V_Rule:='PI - Indirect Team';
              V_Writingagyldr:=V_Manageragy;
              v_RunningType:=v_RunningType||'_SpinOff';

            Else
              --non spin off
            if v_wAgyLdrDistrict=v_commissionAgy then
              V_Rule:='PI - Indirect Team';
              V_Writingagyldr:=V_Wagyldrdistrict;
  --            Log('C11');
            else
              V_Rule:='PI - Indirect Team';
              V_Writingagyldr:=V_Wagyldrdistrict;
    --          Log('C12');
            end if;


           End If; --spin off chking for um
       end if;
--Log('C13');


       --add version 5 (if payee is ever a standalone AM, and current title is FSAD, then it will set all in direct team)
       if V_Writingagyldr is not null then
       SELECT po.genericattribute11
         into v_standalone
         FROM cs_position po
        where po.name = V_Writingagyldr
          AND PO.REMOVEDATE = cdt_EndOfTime
          AND po.effectivestartdate <= v_periodenddate - 1
          and po.effectiveenddate > v_periodenddate - 1;

        SELECT count(1)
          into v_standalone2
          FROM TBL_PIAOR_STANDALONE
         where agyname = V_Writingagyldr;

       if v_standalone = 'FSAD' and v_standalone2 >= 1
       then
         V_Rule := 'PI - Direct Team';
       end if;

      end if;
       --end add


       --check crossover set up
       if v_setup <> 'X' and v_wAgyLdrTitle in ('FSD','FSAD','FSM','AM') then
            vNewWritingAgy:=comGetCrossoverAgy('PI',v_wAgyLdrCde,v_policyIssueDate); --add by nelson
--Log('C14');
            IF vNewWritingAgy IS NOT NULL THEN -- add by nelson
               V_Writingagyldr:='SGY'||Vnewwritingagy;  -- add by nelson
               V_Rule:='PI - Direct Team'; -- add by nelson
  --             Log('C15');
            END IF; -- add by nelson
       end if;
      end if; --eventtype check FOR PI

        --comDebugger('PIAOR DEBUGGER','v_rule1: '||v_rule);
--Log('C16');
          --aor
      if I_R_Agydisttrxn.eventtypeid  in ('API','IFYC','FYC', 'SSCP')
        or (I_R_Agydisttrxn.eventtypeid = 'RYC' and I_R_Agydisttrxn.productname in ('LF','HS','PA','CS'))
        then
--Log('C17');
          --add by nelson start
          if v_wAgyLdrTitle = 'FSD' and v_wAgyLdrDistrict=v_commissionAgy and v_wAgyLdrDistrict=v_CurDistrict and v_LdrCurRole ='FSD' then
                      v_AorRule:='AOR - Direct Team';
                      v_AorWAgyLdr:=v_CurDistrict;
                    --  Log('C18');
          Elsif v_wAgyLdrTitle = 'FSAD' and v_wAgyLdrDistrict<>v_CurDistrict and v_LdrCurRole ='FSD'  then
                      v_AorRule:='AOR - Indirect Team';
                      v_AorWAgyLdr:=v_wAgyLdrDistrict;
                      --Log('C19');
          Elsif v_wAgyLdrTitle = 'FSAD' and (I_R_Agydisttrxn.Spinstartdate Is Not Null  or I_R_Agydisttrxn.SpinEndDate is not null) then
                      --spin off case
                      V_Aorrule:='AOR - Direct Team';
                      V_Aorwagyldr:=V_Wagyldrdistrict;
    --                  Log('C20');
                      If I_R_Agydisttrxn.Spindaterange>8 Or ( I_R_Agydisttrxn.Actualorphanpolicy<>'O'
                      AND I_R_Agydisttrxn.compensationDate>I_R_Agydisttrxn.spinEndDate )  Then
                          v_OrphanPolicy:='XO'; --- set the flag as 'XO', the transaction will not get PI or AOR, but stamp 1 to EB4 of trxn
  --                    Log('C21');
                      end if;
          else
                      v_AorRule:='AOR - Indirect Team';
                      v_AorWAgyLdr:=v_CurDistrict;
      --                Log('C22');
          end if;
          --add by nelson end


          --add version 5 (if payee is ever a standalone AM, it will reassign to current DM.)
          if V_Aorwagyldr is not null then
          SELECT count(1)
          into v_standalone2
          FROM TBL_PIAOR_STANDALONE
          where agyname=V_Aorwagyldr;


          if  v_standalone2 >=1 then
            V_Aorwagyldr := v_CurDistrict;
          end if;

          end if;
          --end add


          if v_setup <> 'X' then
               vAorNewWritingAgy:=comGetCrossoverAgy('AOR',v_wAgyLdrCde,v_policyIssueDate);
--Log('C23');
                IF vAorNewWritingAgy IS NOT NULL THEN
                 --Vnewwritingagy:='SGY'||Vnewwritingagy; commented by nelson
                 V_Aorwagyldr:='SGY'||vAorNewWritingAgy;  -- add by nelson
                 V_Aorrule:='AOR - Direct Team';
  --               Log('C24');
                END IF;
          end if;

        END IF; --EVENTTYPE CHECK FOR AOR


     --comDebugger('PIAOR DEBUGGER','v_rule2'||v_rule);
--Log('C25');

     If V_Rule Is Not Null And I_R_Agydisttrxn.Eventtypeid IN   ('RYC','ORYC') Then
     -- v_maxSetNumber:=v_maxSetNumber+1;
  ---   Log('C26');
      insert  into SH_QUERY_RESULT (component,periodseq,
          genericSequence1, --txnseq
          genericSequence2,  --orderSeq
          genericAttribute1, --wAgyLdr
          genericAttribute2,  --wAgyLdrTitle
          genericAttribute3,   --wAgy
          genericAttribute4,   --wAgyDistrict
          genericAttribute5,  --rule
          genericDate1,    --policyIssueDate
          genericDate2,     --compensationDate
          genericNumber1,  --setNumber
          genericAttribute6,  --BUMap
          GENERICATTRIBUTE7,
          Genericattribute8,
          Genericattribute9,
          Genericattribute10, --ga10 is the rule before redirect
          Genericattribute11,
          genericAttribute12
          ) Values (
          'PI',gv_periodseq,
          I_R_Agydisttrxn.salestransactionSeq,
          I_R_Agydisttrxn.salesOrderSeq,
          v_writingAgyLdr,
          v_wAgyLdrTitle,
          v_wAgency,
          v_wAgyLdrDistrict,
          v_rule,
          v_policyIssueDate,
          I_R_Agydisttrxn.Compensationdate,
          gV_Setnumberpi,
          I_R_Agydisttrxn.businessUnitMap,
          I_R_Agydisttrxn.eventtypeid,
          I_R_Agydisttrxn.Productname,
          'PI '||v_RunningType,
          V_Rule,
          I_R_Agydisttrxn.Orphanpolicy,  --REAL GA17
          I_R_Agydisttrxn.TxnclassCode
      );
     end if;


      if v_AorRule is not null
      And I_R_Agydisttrxn.Eventtypeid  In ('RYC','API','IFYC','FYC', 'SSCP') Then
      --v_maxSetNumber:=v_maxSetNumber+1;
     -- Log('C27');

      insert into SH_QUERY_RESULT (component,periodseq,
          genericSequence1, --txnseq
          genericSequence2,  --orderSeq
          genericAttribute1, --wAgyLdr
          genericAttribute2,  --wAgyLdrTitle
          genericAttribute3,   --wAgy
          genericAttribute4,   --wAgyDistrict
          genericAttribute5,  --rule
          genericDate1,    --policyIssueDate
          genericDate2,     --compensationDate
          genericNumber1,  --setNumber
          genericAttribute6,  --BUMap
          GENERICATTRIBUTE7, --eventtype
          GENERICATTRIBUTE8, --productname
          Genericattribute9, --running type
          Genericattribute10, --rule before redicrect, because, after the SP, stagehook will update GA5
          Genericattribute11,  --REAL GA17
          Genericdate3, --spin off start date
          Genericnumber2, --spin off range
          genericAttribute12 --txn classcode
          ) Values (
          'AOR',gv_periodSeq,
          I_R_Agydisttrxn.salestransactionSeq,
          I_R_Agydisttrxn.salesOrderSeq,
          v_AorWAgyLdr,
          v_wAgyLdrTitle,
          v_wAgency,
          v_wAgyLdrDistrict,
          v_AorRule,
          v_policyIssueDate,
          I_R_Agydisttrxn.Compensationdate,
          gV_Setnumberaor,
          I_R_Agydisttrxn.businessUnitMap,
          I_R_Agydisttrxn.eventtypeid,
          I_R_Agydisttrxn.productname,
          'AOR '||v_RunningType,
          V_Aorrule,
          v_OrphanPolicy,
          I_R_Agydisttrxn.Spinstartdate,
          I_R_Agydisttrxn.Spindaterange,
          I_R_Agydisttrxn.TxnClassCode

      );

      end if;

      end if; -- wAgency is not null

      V_Rule:=Null;
      v_AorRule:=null;
      --Log('C29');

      Exception When Invalid_Manager Then

        Comdebugger('ComTransferPIAOR','Stagehook is not able get any spin off manager'||I_R_Agydisttrxn.salestransactionSeq);

      when others then

        comDebugger('ComTransferPIAOR','Error'||sqlerrm);


end Comtransferpiaor;



Procedure Comtransferpiaor_debug(I_R_Agydisttrxn In R_Agydisttrxn)
as

  v_policyIssueDate date;
  V_Compensationdate Date;
  v_maxSetNumber int; --removed
  v_rule varchar2(100);
  v_writingAgyLdr varchar2(30);
  v_wAgency varchar2(30);
  v_wAgencyLeader varchar2(30);
  v_wAgyLdrTitle varchar2(30);
  v_wAgyLdrDistrict varchar2(30);
  v_Wagtclass varchar2(10);
  vNewWritingAgy varchar2(10);
  vAorNewWritingAgy varchar2(10);
  v_AorRule  varchar2(100);
  V_Aorwagyldr Varchar2(30);
  v_commissionAgy varchar2(30);
  v_CurDistrict Varchar2(30); --add by nelson
  v_LdrCurRole Varchar2(30); --add by nelson
  v_wAgyLdrCde Varchar2(30); --add by nelson
  v_setup  Varchar2(30);--add by nelson
  v_wAgyLdrCurClass varchar2(30);

  V_Crossoverflag Int:=0;
  v_ConstantCrossoverDate date:=to_date('1/1/2005','mm/dd/yyyy');



  V_Manageragy Varchar2(30);
  v_RunningType varchar2(255);

  v_OrphanPolicy varchar2(30);

  Invalid_Manager exception;

begin

  /*
begin
    select nvl(max(setNumber),0)
    into v_maxSetNumber
    from cs_transactionAssignment
    where I_R_Agydisttrxn.salestransactionSeq=salestransactionSeq
    And Genericattribute4 Is Null;

  exception when no_data_found then
    v_maxSetNumber:=0;
  end;

  */



    --DBMS_OUTPUT.put_line(c_txn.salestransactionSeq||'   start c_txn  issue date  '||to_char(v_policyIssueDate,'mm/dd/yyyy'));
    --DBMS_OUTPUT.put_line(v_policyIssueDate||' ---- '||v_cutoverdate);

      v_wAgency:=I_R_Agydisttrxn.wAgency;
      v_wAgencyLeader:=I_R_Agydisttrxn.wAgencyLeader;
      v_wAgyLdrTitle:=I_R_Agydisttrxn.wAgyLdrTitle;
      v_wAgyLdrDistrict:=I_R_Agydisttrxn.wAgyLdrDistrict;
      v_Wagtclass:=I_R_Agydisttrxn.Wagtclass;
      V_Policyissuedate:=I_R_Agydisttrxn.Policyissuedate;
      V_Commissionagy:=I_R_Agydisttrxn.Commissionagy;
      V_Runningtype:=I_R_Agydisttrxn.Runningtype;
      v_OrphanPolicy:=I_R_Agydisttrxn.Orphanpolicy;
      v_CurDistrict:=I_R_Agydisttrxn.CurDistrict; --add by nelson
      v_LdrCurRole:=I_R_Agydisttrxn.LdrCurRole; --add by nelson
      v_wAgyLdrCde:=I_R_Agydisttrxn.wAgyLdrCde; --add by nelson
      v_setup:=I_R_Agydisttrxn.setup; --add by nelson


      --comDebugger('PIAOR DEBUGGER','WAGENCY['||v_wAgency||']||');

 Log('Comtransferpiaor wAgency '|| I_R_Agydisttrxn.wAgency);
 Log('Comtransferpiaor Eventtypeid '|| I_R_Agydisttrxn.Eventtypeid);
 Log('Comtransferpiaor OrphanPolicy '|| I_R_Agydisttrxn.OrphanPolicy);
 Log('Comtransferpiaor productname '|| I_R_Agydisttrxn.productname);
  Log('Comtransferpiaor txnCode '|| I_R_Agydisttrxn.txnCode);

 Log('Comtransferpiaor CurDistrict '|| I_R_Agydisttrxn.CurDistrict);
 Log('Comtransferpiaor LdrCurRole '|| I_R_Agydisttrxn.LdrCurRole);
  Log('Comtransferpiaor wAgyLdrCde '|| I_R_Agydisttrxn.wAgyLdrCde);

 Log('Comtransferpiaor wAgencyLeader '|| I_R_Agydisttrxn.wAgencyLeader);
 Log('Comtransferpiaor wAgyLdrTitle '|| I_R_Agydisttrxn.wAgyLdrTitle);
  Log('Comtransferpiaor wAgyLdrDistrict '|| I_R_Agydisttrxn.wAgyLdrDistrict);

Log('Comtransferpiaor Spinstartdate '|| I_R_Agydisttrxn.Spinstartdate);
 Log('Comtransferpiaor SpinEnddate '|| I_R_Agydisttrxn.SpinEnddate);
  Log('Comtransferpiaor Txnclasscode '|| I_R_Agydisttrxn.Txnclasscode);


Log('Comtransferpiaor Salestransactionseq '|| I_R_Agydisttrxn.Salestransactionseq);
 Log('Comtransferpiaor Wagency '|| I_R_Agydisttrxn.Wagency);
  Log('Comtransferpiaor Commissionagy '|| I_R_Agydisttrxn.Commissionagy);

   if I_R_Agydisttrxn.wAgency is not null then

    --comDebugger('PIAOR DEBUGGER','vNewWritingAgy: '||vNewWritingAgy ||' -- '||v_wAgencyLeader);
Log('C1');
      -- pi
      If (I_R_Agydisttrxn.Eventtypeid = 'RYC' Or (I_R_Agydisttrxn.Eventtypeid = 'ORYC'
      AND substr(I_R_Agydisttrxn.OrphanPolicy,1,1) ='X'))
      and I_R_Agydisttrxn.productname in ('LF','HS')
      and I_R_Agydisttrxn.txnCode in ('PAY2','PAY3','PAY4','PAY5','PAY6') then
        if v_wAgyLdrTitle = 'FSD' then

           if v_wAgyLdrDistrict=v_commissionAgy then
                v_rule:='PI - Direct Team';
                v_writingAgyLdr:=v_wAgyLdrDistrict;
                Log('C2');
          else
            v_rule:='PI - Indirect Team';
            v_writingAgyLdr:=v_wAgyLdrDistrict;
            Log('C3');
          end if;

        Elsif  V_Wagyldrtitle='FSAD' Then

           -- If I_R_Agydisttrxn.Agyspinoffindicator='Y' And I_R_Agydisttrxn.Agyspinoffflag=1 Then
           --above checking was disabled
            if I_R_Agydisttrxn.Spinstartdate is not null then
            --spin off case
              V_Writingagyldr:=V_Wagencyleader;
              V_Rule:='PI - Direct Team';
              v_RunningType:=v_RunningType||'_SpinOff';
              Log('C4');
            Else
            --non spin off case
                if I_R_Agydisttrxn.Wagtclass <>'12' then -- FSAD no need compare ga13 with district, as long as class=10, writingAgy always get PI

                  select p.genericattribute4
                  into v_wAgyLdrCurClass
                  from cs_position p,cs_period t
                  where p.name = v_wAgyLdrCde
                  and   p.removeDate=Cdt_Endoftime
                  and   t.periodseq = Gv_Periodseq
                  and   p.effectiveStartDate<= t.enddate -1
                  and   p.effectiveEndDate > t.enddate-1 ;

Log('C5');
                  if v_wAgyLdrCurClass <> '12' Then
                    v_rule:='PI - Direct Team';
                    v_writingAgyLdr:=v_wAgencyLeader  ;
                    Log('C6');
                  Else
                    v_rule:='PI - Indirect Team';
                    v_writingAgyLdr:=v_wAgyLdrDistrict;
                    Log('C7');
                  end if;
                Else
                  v_rule:='PI - Indirect Team';
                  v_writingAgyLdr:=v_wAgyLdrDistrict;
                  Log('C8');
                End If;
            end if;--spin off chking

        elsif v_wAgyLdrTitle in ('FSM','AM') then
          --
          Log('C9');
          If I_R_Agydisttrxn.Agyspinoffindicator='N' And I_R_Agydisttrxn.Agyspinoffflag=1 Then
            --spin off case
              begin
              Select Name
                Into v_managerAgy
                From Cs_Position
               Where Ruleelementownerseq=I_R_Agydisttrxn.Managerseq
                 And Removedate=Cdt_Endoftime
                 And Effectivestartdate<=I_R_Agydisttrxn.Versioningdate
                 and effectiveEndDate>I_R_Agydisttrxn.versioningdate;

              Exception When No_Data_Found Then
                raise Invalid_Manager;
              End;
Log('C10');
              V_Rule:='PI - Indirect Team';
              V_Writingagyldr:=V_Manageragy;
              v_RunningType:=v_RunningType||'_SpinOff';

            Else
              --non spin off
            if v_wAgyLdrDistrict=v_commissionAgy then
              V_Rule:='PI - Indirect Team';
              V_Writingagyldr:=V_Wagyldrdistrict;
              Log('C11');
            else
              V_Rule:='PI - Indirect Team';
              V_Writingagyldr:=V_Wagyldrdistrict;
              Log('C12');
            end if;


           End If; --spin off chking for um
       end if;
Log('C13');
       --check crossover set up
       if v_setup <> 'X' and v_wAgyLdrTitle in ('FSD','FSAD','FSM','AM') then
            vNewWritingAgy:=comGetCrossoverAgy('PI',v_wAgyLdrCde,v_policyIssueDate); --add by nelson
Log('C14');
            IF vNewWritingAgy IS NOT NULL THEN -- add by nelson
               V_Writingagyldr:='SGY'||Vnewwritingagy;  -- add by nelson
               V_Rule:='PI - Direct Team'; -- add by nelson
               Log('C15');
            END IF; -- add by nelson
       end if;
      end if; --eventtype check FOR PI

        --comDebugger('PIAOR DEBUGGER','v_rule1: '||v_rule);
Log('C16');
          --aor
      if I_R_Agydisttrxn.eventtypeid  in ('API','IFYC','FYC', 'SSCP')
        or (I_R_Agydisttrxn.eventtypeid = 'RYC' and I_R_Agydisttrxn.productname in ('LF','HS','PA','CS'))
        then
Log('C17');
          --add by nelson start
          if v_wAgyLdrTitle = 'FSD' and v_wAgyLdrDistrict=v_commissionAgy and v_wAgyLdrDistrict=v_CurDistrict and v_LdrCurRole ='FSD' then
                      v_AorRule:='AOR - Direct Team';
                      v_AorWAgyLdr:=v_CurDistrict;
                      Log('C18');
          Elsif v_wAgyLdrTitle = 'FSAD' and v_wAgyLdrDistrict<>v_CurDistrict and v_LdrCurRole ='FSD'  then
                      v_AorRule:='AOR - Indirect Team';
                      v_AorWAgyLdr:=v_wAgyLdrDistrict;
                      Log('C19');
          Elsif v_wAgyLdrTitle = 'FSAD' and (I_R_Agydisttrxn.Spinstartdate Is Not Null  or I_R_Agydisttrxn.SpinEndDate is not null) then
                      --spin off case
                      V_Aorrule:='AOR - Direct Team';
                      V_Aorwagyldr:=V_Wagyldrdistrict;
                      Log('C20');
                      If I_R_Agydisttrxn.Spindaterange>8 Or ( I_R_Agydisttrxn.Actualorphanpolicy<>'O'
                      AND I_R_Agydisttrxn.compensationDate>I_R_Agydisttrxn.spinEndDate )  Then
                          v_OrphanPolicy:='XO'; --- set the flag as 'XO', the transaction will not get PI or AOR, but stamp 1 to EB4 of trxn
                      Log('C21');
                      end if;
          else
                      v_AorRule:='AOR - Indirect Team';
                      v_AorWAgyLdr:=v_CurDistrict;
                      Log('C22');
          end if;
          --add by nelson end

          if v_setup <> 'X' then
               vAorNewWritingAgy:=comGetCrossoverAgy('AOR',v_wAgyLdrCde,v_policyIssueDate);
Log('C23');
                IF vAorNewWritingAgy IS NOT NULL THEN
                 --Vnewwritingagy:='SGY'||Vnewwritingagy; commented by nelson
                 V_Aorwagyldr:='SGY'||vAorNewWritingAgy;  -- add by nelson
                 V_Aorrule:='AOR - Direct Team';
                 Log('C24');
                END IF;
          end if;

        END IF; --EVENTTYPE CHECK FOR AOR


     --comDebugger('PIAOR DEBUGGER','v_rule2'||v_rule);
Log('C25 '|| V_Rule);
log('C25-1 '||V_Aorrule);

     If V_Rule Is Not Null And I_R_Agydisttrxn.Eventtypeid IN   ('RYC','ORYC') Then
     -- v_maxSetNumber:=v_maxSetNumber+1;
     Log('C26');
      insert  into SH_QUERY_RESULT (component,periodseq,
          genericSequence1, --txnseq
          genericSequence2,  --orderSeq
          genericAttribute1, --wAgyLdr
          genericAttribute2,  --wAgyLdrTitle
          genericAttribute3,   --wAgy
          genericAttribute4,   --wAgyDistrict
          genericAttribute5,  --rule
          genericDate1,    --policyIssueDate
          genericDate2,     --compensationDate
          genericNumber1,  --setNumber
          genericAttribute6,  --BUMap
          GENERICATTRIBUTE7,
          Genericattribute8,
          Genericattribute9,
          Genericattribute10, --ga10 is the rule before redirect
          Genericattribute11,
          genericAttribute12
          ) Values (
          'PI',gv_periodseq,
          I_R_Agydisttrxn.salestransactionSeq,
          I_R_Agydisttrxn.salesOrderSeq,
          v_writingAgyLdr,
          v_wAgyLdrTitle,
          v_wAgency,
          v_wAgyLdrDistrict,
          v_rule,
          v_policyIssueDate,
          I_R_Agydisttrxn.Compensationdate,
          gV_Setnumberpi,
          I_R_Agydisttrxn.businessUnitMap,
          I_R_Agydisttrxn.eventtypeid,
          I_R_Agydisttrxn.Productname,
          'PI '||v_RunningType,
          V_Rule,
          I_R_Agydisttrxn.Orphanpolicy,  --REAL GA17
          I_R_Agydisttrxn.TxnclassCode
      );
     end if;


      if v_AorRule is not null
      And I_R_Agydisttrxn.Eventtypeid  In ('RYC','API','IFYC','FYC', 'SSCP') Then
      --v_maxSetNumber:=v_maxSetNumber+1;
      Log('C27');
      insert into SH_QUERY_RESULT (component,periodseq,
          genericSequence1, --txnseq
          genericSequence2,  --orderSeq
          genericAttribute1, --wAgyLdr
          genericAttribute2,  --wAgyLdrTitle
          genericAttribute3,   --wAgy
          genericAttribute4,   --wAgyDistrict
          genericAttribute5,  --rule
          genericDate1,    --policyIssueDate
          genericDate2,     --compensationDate
          genericNumber1,  --setNumber
          genericAttribute6,  --BUMap
          GENERICATTRIBUTE7, --eventtype
          GENERICATTRIBUTE8, --productname
          Genericattribute9, --running type
          Genericattribute10, --rule before redicrect, because, after the SP, stagehook will update GA5
          Genericattribute11,  --REAL GA17
          Genericdate3, --spin off start date
          Genericnumber2, --spin off range
          genericAttribute12 --txn classcode
          ) Values (
          'AOR',gv_periodSeq,
          I_R_Agydisttrxn.salestransactionSeq,
          I_R_Agydisttrxn.salesOrderSeq,
          v_AorWAgyLdr,
          v_wAgyLdrTitle,
          v_wAgency,
          v_wAgyLdrDistrict,
          v_AorRule,
          v_policyIssueDate,
          I_R_Agydisttrxn.Compensationdate,
          gV_Setnumberaor,
          I_R_Agydisttrxn.businessUnitMap,
          I_R_Agydisttrxn.eventtypeid,
          I_R_Agydisttrxn.productname,
          'AOR '||v_RunningType,
          V_Aorrule,
          v_OrphanPolicy,
          I_R_Agydisttrxn.Spinstartdate,
          I_R_Agydisttrxn.Spindaterange,
          I_R_Agydisttrxn.TxnClassCode

      );

      end if;

      end if; -- wAgency is not null

      V_Rule:=Null;
      v_AorRule:=null;
      Log('C29');

      Exception When Invalid_Manager Then

        Comdebugger('ComTransferPIAOR','Stagehook is not able get any spin off manager'||I_R_Agydisttrxn.salestransactionSeq);

      when others then

        comDebugger('ComTransferPIAOR','Error'||sqlerrm);


end Comtransferpiaor_debug;





Function Comgeteventtypeseq(I_Eventtypeid In Varchar2) Return Int as
    v_eventtypeseq int;
  Begin

    Select datatypeseq
      Into V_Eventtypeseq
      From Cs_Eventtype
     Where Eventtypeid = I_Eventtypeid
       and removedate = cdt_EndofTime;

    return v_eventtypeseq;

  exception
    when others then
      return 0;
end Comgeteventtypeseq;



Procedure ComInitialpartition(I_Component      In Varchar2,
                                i_componentValue in varchar2,
                                I_Periodseq      In Int) As
    V_str              Varchar2(1000);
    V_Cnt              Int;
    v_partitionname    varchar2(100) := 'SH_INITIAL_' || I_Component;
    V_subPartitionname Varchar2(100) := Upper('Sh_' || I_Component || '_' ||
                                              I_Periodseq);

  Begin

    DBMS_OUTPUT.PUT_LINE('CLEAN UP PARTTION' || V_subPartitionname ||
                         '---'  || V_Cnt);

    Select Count(*)
      Into V_Cnt
      From User_Tab_SUBPartitions
     Where Upper(Table_Name) = 'SH_QUERY_RESULT'
       and upper(subpartition_name) = V_subPartitionname;

    --Comdebugger('CLEAN UP PARTTION',V_subPartitionname||'---'||V_Cnt);

    If V_Cnt = 0 Then
      V_str := 'alter table SH_QUERY_RESULT modify partition '  ||
               v_partitionname || ' add subpartition ' ||
               V_Subpartitionname || ' values ('  || I_Periodseq || ')' ;
      --dbms_output.put_line(v_Str);

      -- Comdebugger('CLEAN UP PARTTION',V_Str);
      execute immediate v_str;
    Else
      V_Str := 'ALTER TABLE SH_QUERY_RESULT truncate subpartition '  ||
               V_Subpartitionname;
      --Comdebugger('CLEAN UP PARTTION',V_Str);
      execute immediate v_str;
    end if;

    RETURN;

  Exception
    When Others Then
      --dbms_output.put_line(sqlerrm);

      COMDEBUGGER('CLEAN UP PARTTION ERROR', sqlerrm);

      return;

  end comInitialPartition;



--version 8 add piaor_assignment initial procedure

  Procedure AssignmentInitialpartition(I_Periodseq In Int) As
    V_str              Varchar2(1000);
    V_Cnt              Int;
    v_partitionname    varchar2(100) := 'P_AIAS_';
    v_Periodenddate    varchar2(100);

  Begin

  log('init start---' );

  --get Period startdate enddate rate
   select to_char(cs.enddate,'yyyymmdd')
     into v_Periodenddate
     from cs_period cs
    where cs.tenantid='AIAS'
    and cs.periodSeq = i_periodSeq
      and cs.Removedate = Cdt_Endoftime
    and cs.CALENDARSEQ = GV_CALENDARSEQ
    ;
  log('v_Periodenddate: ' ||v_Periodenddate);

  v_partitionname := v_partitionname||v_Periodenddate;

  Select Count(*)
      Into V_Cnt
      From User_Tab_Partitions
     Where Upper(Table_Name) = 'PIAOR_ASSIGNMENT'
     and Upper(PARTITION_NAME) = v_partitionname;

  log('v_partitionname: ' ||v_partitionname);
  log('V_Cnt: ' ||V_Cnt);

    DBMS_OUTPUT.PUT_LINE('CLEAN UP PARTTION' || v_partitionname ||
                         '---'  || V_Cnt);


    If V_Cnt = 0 Then
      V_str := 'alter table PIAOR_ASSIGNMENT add partition '  ||
               v_partitionname || ' values less than (''AIAS'',TO_DATE('''  || v_Periodenddate
         || ' 00:00:00'', ''YYYYMMDD HH24:MI:SS''))' ;

    Else
      V_Str := 'ALTER TABLE PIAOR_ASSIGNMENT truncate partition '  ||
               v_partitionname;

    end if;

    log('V_str: ' ||V_str);

    execute immediate v_str;

    RETURN;

  Exception
    When Others Then

      COMDEBUGGER('CLEAN UP PARTTION ERROR', sqlerrm);

      return;

  end AssignmentInitialpartition;



function comGetYrLastMonth(i_periodSeq in int) return int

   is

    NOT_YEAR_END EXCEPTION;
    v_periodTypeSeq int := 0;
    v_periodStartDate date;

  begin

    select startdate
      into v_periodStartDate
      from cs_period
     where tenantid = 'AIAS'
       and periodSeq = i_periodSeq
       And Removedate = Cdt_Endoftime
       AND CALENDARSEQ = GV_CALENDARSEQ;

    begin

      if v_periodStartDate<=date'2016-12-01'
      then
       select periodTypeSeq
         into v_periodTypeSeq
         from cs_period
        where tenantid = 'AIAS'
          and periodSeq = i_periodSeq
          and shortName = 'Nov'
          And Removedate = Cdt_Endoftime
          AND CALENDARSEQ = GV_CALENDARSEQ;

      else

      select periodTypeSeq
        into v_periodTypeSeq
        from cs_period
       where tenantid = 'AIAS'
         and periodSeq = i_periodSeq
         and shortName = 'Dec'
         And Removedate = Cdt_Endoftime
         AND CALENDARSEQ = GV_CALENDARSEQ;

       end if;

    exception
      when no_data_found then
        return - 1;
    end;

    if v_periodTypeSeq = null then
       raise NOT_YEAR_END;

    end if;

    return 1;

  exception
    WHEN NOT_YEAR_END then
      gv_error := 'Info [PIAOR_Calculation]: The PIAOR Calculation will be skip in current period.' ;
      return - 1;

  end comGetYrLastMonth;


end PK_PIAOR_CALCULATION;
 
