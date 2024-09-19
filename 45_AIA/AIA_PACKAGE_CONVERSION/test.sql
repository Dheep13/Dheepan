/*
 * This file was extracted from 'C:/HANAMigrations/AIASG/OracleObjects/ext_DDL.sql' 
 * at 05-Jun-2024 11:42:16 with the 'extract_offline' command of SAP Advanced SQL Migration v.3.5.3 (64791)
 * User config setting for 'extract_offline' (id=132) was '0'.
 */


CREATE OR REPLACE  PACKAGE BODY AIASEXT.PK_RPT_PRODUCTION 
AS
 /*
 this pakage is created for Agent Statement report.
 ************************************************
 Version     Create By       Create Date   Change
 ************************************************
 1           Callidus          20171030      Intial
 2           Jeff              20180417      add entry to run all report with report cycledate.
 8           Sammi             20181128      add on bridging flag for production commission report
 9           Sammi             20190621      Project Alpha add GI logic
 */
V_PROCNAME           VARCHAR2(256);
v_eot                date := to_date('01/01/2200', 'DD/MM/YYYY');
V_CYCLEDATE            DATE;
V_PERIODSEQ            NUMBER;
V_PERIODSTARTDATE    DATE;
V_PERIODENDDATE      DATE;
V_PROCESSINGUNITSEQ    INTEGER;
V_PROCESSINGUNITNAME   VARCHAR2(256);
V_PERIODNAME           VARCHAR2(50);
V_PERIODTYPESEQ        NUMBER;
V_CALENDARNAME         VARCHAR2(256);
V_CALENDARSEQ          NUMBER;
V_STARTDATE            DATE;
V_ENDDATE              DATE;
v_BUSINESSUNITMAP1     INTEGER;
v_BUSINESSUNITMAP2     INTEGER;
V_SYSDATE        DATE;
V_Partition_Name varchar2(250);
V_Report_SEQ           INTEGER;


     --Added by Suresh
  procedure Log(inText varchar2) is
    pragma autonomous_transaction;
    vText RPT_SGP_AGY_PKG_DEBUG.text%type;
    vBatch_No integer;
  begin
    vText := substr(inText, 1, 4000);
    insert into RPT_SGP_AGY_PKG_DEBUG (datetime, text) values (systimestamp, 'LOG: ' || vText);
    commit;
    dbms_output.put_line( to_char(systimestamp,'yyyy-mm-dd hh24:mi:ssxff') || '  ' || vText);
  exception
    when others then
      rollback;
      raise;
  end Log;
 -- End by Suresh

PROCEDURE REP_RUN_INIT(P_STR_CYCLEDATE IN VARCHAR2) as

Begin

  ------------------------get cycle date  'yyyy-mm-dd'--------------------------
  SELECT TO_DATE(NVL(P_STR_CYCLEDATE,TXT_KEY_VALUE),STR_DATE_FORMAT_TYPE)
      INTO V_CYCLEDATE
  FROM IN_ETL_CONTROL
  WHERE TXT_FILE_NAME = STR_CYCLEDATE_FILE_NAME
  AND TXT_KEY_STRING  = STR_CYCLEDATE_KEY;

 -----------get specified periodseq,enddate,periodtypeseq,calendarseq----------
    pk_aia_rpt_comm_fn.sp_period_getbydate( V_CYCLEDATE,
                                            V_PERIODSEQ,
                                            V_STARTDATE,
                                            V_ENDDATE,
                                            V_PERIODTYPESEQ,
                                            V_CALENDARSEQ,
                                            V_PERIODNAME);

SELECT PROCESSINGUNITSEQ INTO V_PROCESSINGUNITSEQ  FROM cs_processingunit WHERE NAME = 'AGY_PU';
Log('V_PROCESSINGUNITSEQ '||V_PROCESSINGUNITSEQ);
SELECT MASK INTO v_BUSINESSUNITMAP1 FROM cs_businessunit WHERE name IN ('SGPAFA');
Log('v_BUSINESSUNITMAP1 '||v_BUSINESSUNITMAP1);
SELECT MASK INTO v_BUSINESSUNITMAP2 FROM cs_businessunit WHERE name IN ('BRUAGY');
Log('v_BUSINESSUNITMAP2 '||v_BUSINESSUNITMAP2);

/*EXECUTE IMMEDIATE 'ALTER INDEX INDX_PRODUCTION_CF_PAKEY REBUILD';
EXECUTE IMMEDIATE 'ALTER INDEX INDX_PRODUCTION_CDIM_CREDITTYPE REBUILD';
EXECUTE IMMEDIATE 'ALTER INDEX INDX_PRODUCTION_TDIM_PRODTNAME REBUILD';
EXECUTE IMMEDIATE 'ALTER INDEX INDX_PRODUCTION_CD_GA4 REBUILD';
EXECUTE IMMEDIATE 'ALTER INDEX INDX_PRODUCTION_CD_GA2 REBUILD';
EXECUTE IMMEDIATE 'ALTER INDEX INDX_PRODUCTION_TDIM_EVENTTYPE REBUILD';*/

/*insert into AIA_Report_Proc_Logs
VALUES
('AIA_RPT_PRC_PRODUCTION_TEMP',sysdate,'AGY_PU',v_periodname,'Starting The PRODUCTION District and Policy Report Temp Values Procedure');

commit;

insert into AIA_Report_Proc_Logs
VALUES
('AIA_RPT_PRC_PRODUCTION_TEMP',sysdate,'AGY_PU',v_periodname,'Starting The PRODUCTION District and Policy Report Temp Values in to AIA_RPT_PRODUCTION_TEMP_VALUES');

commit;*/

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PIB_REPORT.REP_RUN_INIT' , 'AIA_RPT_PRD_TEMP_CREDITS'  , 'Processing', 'insert into AIA_RPT_PRD_TEMP_CREDITS');

EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_TEMP_CREDITS';

insert into AIA_RPT_PRD_TEMP_CREDITS NOLOGGING
  select V_PERIODSEQ            as periodseq,
         ct.credittypeid        as cd_credittype,
         crd.genericattribute13 as cd_ga13,
          crd.genericattribute1 as cd_ga1,
         crd.genericattribute2  as cd_ga2,
         crd.genericattribute4  as cd_ga4,
         crd.genericattribute6  as cd_ga6,
         crd.genericnumber1     as cd_gn1,
         crd.value              as cd_value,
         pu.name                as processingunit,
         crd.positionseq        as cd_positionseq,
         pos.name               as participantid,
         ttl.name               as positiontitle,
         crd.genericdate2       as cd_gd2,
         crd.genericattribute12 as cd_ga12,
         crd.genericattribute14 as cd_ga14,
         crd.name               as cd_name,
         crd.salestransactionseq as cd_txn_seq,
         crd.businessunitmap as businessunitmap,
     case
     --for BN
     when ttl.name like 'BR%'
       then '0'
     --for the records with policy issue date < 12/1/2015
     when crd.genericdate2 < to_date('12/1/2015', 'mm/dd/yyyy') then '0'
     --for all CS/PL RYC
     --when ct.credittypeid like '%RYC%' and crd.genericattribute2 in ('CS','CL','PL')
     when ct.credittypeid like '%RYC%' and crd.genericattribute2 in ('CS','CL','PL','GI')  --version 9 add GI product
       then '0'
     --for all FYP and Case Count
     when ct.credittypeid in ('FYP','Case_Count')
       then '0'
     --for Life/PA/CS FYC and policy issue date >= 12/1/2015
     when crd.businessunitmap = v_BUSINESSUNITMAP1
       and (ct.credittypeid like 'FYC%' or ct.credittypeid like 'API%' or ct.credittypeid like 'SSCP%' or ct.credittypeid like 'APB%')
       and crd.genericdate2 >= to_date('12/1/2015', 'mm/dd/yyyy')
       then '1'
     --for Life/PA RYC and policy issue date >= 12/1/2015
     when crd.businessunitmap = v_BUSINESSUNITMAP1
       and ct.credittypeid like '%RYC%' and crd.genericattribute2 in ('LF','PA','HS','VL')
       and crd.genericdate2 >= to_date('12/1/2015', 'mm/dd/yyyy')
       then '1'
     else '0'
     end as NEW_PRD_IND
    from cs_credit crd
   inner join cs_credittype ct
      on crd.credittypeseq = ct.datatypeseq
      and ct.removedate = DT_REMOVEDATE
   inner join cs_processingunit pu
      on crd.processingunitseq = pu.processingunitseq
   inner join cs_position pos
      on crd.positionseq = pos.ruleelementownerseq
     and pos.removedate = DT_REMOVEDATE
     and pos.effectivestartdate < V_ENDDATE
     AND pos.effectiveenddate > V_ENDDATE - 1
   inner join cs_title ttl
      on ttl.RULEELEMENTOWNERSEQ = pos.TITLESEQ
     AND ttl.effectivestartdate < V_ENDDATE
     AND ttl.effectiveenddate > V_ENDDATE - 1
     AND ttl.REMOVEDATE = DT_REMOVEDATE
   where crd.periodseq = V_PERIODSEQ --2533274790398962
     AND crd.GENERICATTRIBUTE2 IN
         --('LF', 'HS', 'PA', 'PL', 'VL', 'CS', 'CL','PAYT')
         ('LF', 'HS', 'PA', 'PL', 'VL', 'CS', 'CL','PAYT','GI') --version 9 add GI product
     and ct.credittypeid in ('FYP',
                             'FYC',
                             'RYC',
                             'API',
                             'SSCP',
                             'APB',
                             'ORYC',
                             'Case_Count',
                             'API_W',
                             'API_W_DUPLICATE',
                             'API_WC_DUPLICATE',
                             'SSCP_W',
                             'SSCP_W_DUPLICATE',
                             'SSCP_WC_DUPLICATE',
                             'FYC_W',
                             'FYC_W_DUPLICATE',
                             'FYC_WC_DUPLICATE',
                             'RYC_W',
                             'RYC_W_DUPLICATE',
                             'RYC_WC_DUPLICATE',
                             'ORYC_W',
                             'ORYC_W_DUPLICATE',
                             'ORYC_WC_DUPLICATE');
Log('Records inserted into AIA_RPT_PRD_TEMP_CREDITS are '||SQL%ROWCOUNT);

commit;

BEGIN
        DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME => '"AIASEXT"' ,
                                        TABNAME => '"AIA_RPT_PRD_TEMP_CREDITS"' ,
                                        ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE);
END;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_INIT' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_INIT' , 'AIA_RPT_PRD_TEMP_VALUES'  , 'Processing', 'insert into AIA_RPT_PRD_TEMP_VALUES');


EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_TEMP_VALUES';

/** Remark for report requirement
1.The commissionable PRD reports (old report) will show LIFE FYC/RYC,PA FYC/ RYC,CS FYC of issue date < 12/1/2015 only,
and CS RYC, PL RYC, FYP and CASE of all policies.

2.The writing PIB reports (new report) will show LIFE FYC/RYC,PA FYC/ RYC,CS FYC policies of issue date>=12/1/2015,
the CS RYC, PL RYC, FYP and CASE will be removed from the reports.
*/

INSERT /*+ APPEND */
INTO AIA_RPT_PRD_TEMP_VALUES NOLOGGING
  SELECT CF.PERIODSEQ,
         CF.Cd_Positionseq as CD_POSITIONSEQ,
         CF.POSITIONTITLE  as CD_TITLE,
         CF.cd_credittype  as CREDITTYPE,
         CF.CD_GA12,
         --for writing/commission agency
         case
           when CF.NEW_PIB_IND = '0' then
            CF.CD_GA13
           when CF.NEW_PIB_IND = '1'
               --and FN_FYO_PRD_TYPE(CF.CD_GA13, CF.CD_GA14, CF.CD_NAME)='DIRECT'
                and CF.CD_NAME not like '%INDIRECT%' and
                CF.CD_NAME like '%DIRECT%' then
            to_char(substr(pos.name, 4))
           else
            CF.CD_GA13
         end as CD_GA13,
         CF.CD_GA1,
         CF.CD_GA2,
         CF.CD_GA4,
         CF.CD_GA6,
         CF.CD_GN1,
         CF.CD_VALUE,
         CF.CD_GD2,
         CF.businessunitmap,
         et.eventtypeid as EVENTTYPE,
          ST.DATASOURCE as SOURCE_SYSTEM,
         st.productname as PRODUCTNAME,
         st.genericattribute3 as TXN_GA3,
         st.genericattribute10 as TXN_GA10,
         st.genericattribute11 as TXN_GA11,
         st.genericnumber3 as TXN_GN3,
         st.genericnumber5 as TXN_GN5,
         st.genericnumber6 as TXN_GN6,
         st.genericdate6 as TXN_GD6,
         st.genericattribute14 as TXN_GA14,
         st.genericattribute17 as TXN_GA17,
         --PIB type is just for new policy
         case CF.NEW_PIB_IND
           when '1' then
            case
              when CF.CD_NAME not like '%INDIRECT%' and CF.CD_NAME like '%DIRECT%'
                then 'DIRECT'
              when CF.CD_NAME like '%INDIRECT%'
                then 'INDIRECT'
              ELSE
               'PERSONAL'
            end
           else ''
         end as PIB_TYPE,
         CF.NEW_PIB_IND,
        agn.genericnumber1/100 as Assign_GN1  ---add version 3
         --add in version 5
         ,  gast.genericdate8 submitdate
         -- end to add
         ,st.genericattribute19 as TXN_GA19
    FROM AIA_RPT_PRD_TEMP_CREDITS CF
   inner join cs_salestransaction st
      on CF.CD_TXN_SEQ = st.salestransactionseq
   ---add version 3
   --version 8 fix duplicate data
   --left join cs_transactionassignment agn
   --   on agn.salestransactionseq=st.salestransactionseq and (agn.positionname like 'BRT%' OR  agn.positionname like 'SGY%')
   left join (select salestransactionseq,max(genericnumber1) genericnumber1
                from cs_transactionassignment
               where (positionname like 'BRT%' OR positionname like 'SGY%')
                 and COMPENSATIONDATE < V_ENDDATE
                 and COMPENSATIONDATE>=V_STARTDATE
                 group by salestransactionseq
              ) agn
         on agn.salestransactionseq=st.salestransactionseq
   --end
   inner join cs_eventtype et
      on st.eventtypeseq = et.datatypeseq
     and et.removedate = DT_REMOVEDATE
  --add in version 5
   left join cs_gasalestransaction gast
     on st.salestransactionseq = gast.salestransactionseq
     and gast.pagenumber =0
  --end to add in version 5
   inner join Cs_Position pos
      on CF.Cd_Positionseq = pos.ruleelementownerseq
     AND pos.effectivestartdate < V_ENDDATE
     AND pos.effectiveenddate > V_ENDDATE - 1
     AND pos.removedate = DT_REMOVEDATE
   WHERE CF.PROCESSINGUNIT = 'AGY_PU'
     --AND st.PRODUCTNAME IN ('LF', 'HS', 'PA', 'PL', 'VL', 'CS', 'CL','PAYT')
     AND st.PRODUCTNAME IN ('LF', 'HS', 'PA', 'PL', 'VL', 'CS', 'CL','PAYT','GI')  --version 9 add GI product
      and st.COMPENSATIONDATE <  V_ENDDATE
and st.COMPENSATIONDATE>= V_STARTDATE
-- and agn.COMPENSATIONDATE < V_ENDDATE version 8
--and agn.COMPENSATIONDATE>=V_STARTDATE version 8
 and gast.COMPENSATIONDATE <  V_ENDDATE
and gast.COMPENSATIONDATE>=V_STARTDATE;

Log('Records inserted into AIA_RPT_PRD_TEMP_VALUES are '||SQL%ROWCOUNT);
COMMIT;



BEGIN
        DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME => '"AIASEXT"' ,
                                        tabname => '"AIA_RPT_PRD_TEMP_VALUES"' ,
                                        ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE);

END;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PIB_REPORT.REP_RUN_INIT' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PIB_REPORT.REP_RUN_INIT' , 'AIA_RPT_PRD_TEMP_PADIM'  , 'Processing', 'insert into AIA_RPT_PRD_TEMP_PADIM');



/* commented by Suresh

insert into AIA_RPT_PRD_TEMP_PADIM
select
       pn.ruleelementownerseq as POSITIONSEQ,
       pn.managerseq,
       pn.name,
       pt.firstname,
       pt.lastname,
       pn.genericattribute1  as POS_GA1,
       pn.genericattribute2  as POS_GA2,
       pn.genericattribute3  as POS_GA3,
       pn.genericattribute7  as POS_GA7,
       pn.genericattribute9  as POS_GA9,
       pn.genericattribute11 as POS_GA11,
       pn.genericattribute4  as POS_GA4,
       ttl.name           POSITIONTITLE,
       pt.terminationdate     AGY_TERMINATIONDATE,
       pn.genericdate4        ASSIGNED_DATE,
       pn.genericdate1        AGY_APPOINTMENT_DATE,
       pt.hiredate,
       pt.genericattribute1  as PT_GA1,
       pn.effectivestartdate,
       pn.effectiveenddate,
       pe.businessunitmap
  from cs_position pn
      inner join cs_participant pt
      on pt.payeeseq = pn.payeeseq
      and pt.removedate = DT_REMOVEDATE
      AND pt.effectivestartdate < V_ENDDATE
      AND pt.effectiveenddate   > V_ENDDATE-1
      inner join cs_title ttl
      on pn.titleseq = ttl.ruleelementownerseq
      and ttl.removedate = DT_REMOVEDATE
      AND ttl.effectivestartdate < V_ENDDATE
      AND ttl.effectiveenddate   > V_ENDDATE-1
      inner join cs_payee pe
      ON  pn.payeeseq = pe.payeeseq
      AND pe.businessunitmap IN (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
      AND pe.removedate = DT_REMOVEDATE
      AND pe.effectivestartdate < V_ENDDATE
      AND pe.effectiveenddate   > V_ENDDATE-1
where pn.removedate = DT_REMOVEDATE
   AND pn.effectivestartdate < V_ENDDATE
   AND pn.effectiveenddate   > V_ENDDATE-1
;
*/

--Added by Suresh
EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_TEMP_PADIM_HELP';

insert /*+ APPEND */ into AIA_RPT_PRD_TEMP_PADIM_HELP
select
       pn.ruleelementownerseq as POSITIONSEQ,
       pn.managerseq,
       pn.name,
       pt.firstname,
       pt.lastname,
       pn.genericattribute1  as POS_GA1,
       pn.genericattribute2  as POS_GA2,
       pn.genericattribute3  as POS_GA3,
       pn.genericattribute7  as POS_GA7,
       pn.genericattribute9  as POS_GA9,
       pn.genericattribute11 as POS_GA11,
       pn.genericattribute4  as POS_GA4,
       ttl.name           POSITIONTITLE,
       pt.terminationdate     AGY_TERMINATIONDATE,
       pn.genericdate4        ASSIGNED_DATE,
       pn.genericdate1        AGY_APPOINTMENT_DATE,
       pt.hiredate,
       pt.genericattribute1  as PT_GA1,
       pn.effectivestartdate,
       pn.effectiveenddate,
       pn.payeeseq
       --pe.businessunitmap
  from cs_position pn
      inner join cs_participant pt
      on pt.payeeseq = pn.payeeseq
      and pt.removedate = DT_REMOVEDATE
      AND pt.effectivestartdate < V_ENDDATE
      AND pt.effectiveenddate   > V_ENDDATE-1
      inner join cs_title ttl
      on pn.titleseq = ttl.ruleelementownerseq
      and ttl.removedate = DT_REMOVEDATE
      AND ttl.effectivestartdate < V_ENDDATE
      AND ttl.effectiveenddate   > V_ENDDATE-1
where pn.removedate = DT_REMOVEDATE
   AND pn.effectivestartdate < V_ENDDATE
   AND pn.effectiveenddate   > V_ENDDATE-1;

    dbms_output.put_line('rows inserted in AIA_RPT_PRD_TEMP_PADIM_HELP are '||SQL%ROWCOUNT);
    Log('Records inserted into AIA_RPT_PRD_TEMP_PADIM_HELP are '||SQL%ROWCOUNT);
    commit;

   BEGIN
        DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME => '"AIASEXT"' ,
                                        TABNAME => '"AIA_RPT_PRD_TEMP_PADIM_HELP"' ,
                                        ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE);
END;

   EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_TEMP_PADIM';

         insert /*+ append */ into AIA_RPT_PRD_TEMP_PADIM
      select
      help.POSITIONSEQ,
      help.managerseq,
       help.name,
       help.firstname,
       help.lastname,
       help.POS_GA1,
       help.POS_GA2,
       help.POS_GA3,
       help.POS_GA7,
       help.POS_GA9,
       help.POS_GA11,
       help.POS_GA4,
       help.POSITIONTITLE,
       help.AGY_TERMINATIONDATE,
       help.ASSIGNED_DATE,
       help.AGY_APPOINTMENT_DATE,
       help.hiredate,
       help.PT_GA1,
       help.effectivestartdate,
       help.effectiveenddate,
       pe.businessunitmap
      from
      AIA_RPT_PRD_TEMP_PADIM_HELP help
      inner join cs_payee pe
      ON  help.payeeseq = pe.payeeseq
      AND pe.businessunitmap IN (V_BUSINESSUNITMAP1)
      AND pe.removedate = DT_REMOVEDATE
      AND pe.effectivestartdate < V_ENDDATE
      AND pe.effectiveenddate   > V_ENDDATE-1;

      dbms_output.put_line('rows inserted in AIA_RPT_PRD_TEMP_PADIM_HELP are '||SQL%ROWCOUNT);
      Log('Records inserted into AIA_RPT_PRD_TEMP_PADIM are '||SQL%ROWCOUNT);

COMMIT;

BEGIN
        DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME => '"AIASEXT"' ,
                                        TABNAME => '"AIA_RPT_PRD_TEMP_PADIM"' ,
                                        ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE);
END;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_INIT' , 'Finish', '');

END REP_RUN_INIT;

--PROCEDURE REP_RUN_INIT_BRU(P_STR_CYCLEDATE IN VARCHAR2) as
--
--Begin
--
--  ------------------------get cycle date  'yyyy-mm-dd'--------------------------
--  SELECT TO_DATE(NVL(P_STR_CYCLEDATE,TXT_KEY_VALUE),STR_DATE_FORMAT_TYPE)
--      INTO V_CYCLEDATE
--  FROM IN_ETL_CONTROL
--  WHERE TXT_FILE_NAME = STR_CYCLEDATE_FILE_NAME
--  AND TXT_KEY_STRING  = STR_CYCLEDATE_KEY;
--
-- -----------get specified periodseq,enddate,periodtypeseq,calendarseq----------
--    pk_aia_rpt_comm_fn.sp_period_getbydate( V_CYCLEDATE,
--                                            V_PERIODSEQ,
--                                            V_STARTDATE,
--                                            V_ENDDATE,
--                                            V_PERIODTYPESEQ,
--                                            V_CALENDARSEQ,
--                                            V_PERIODNAME);
--
--SELECT PROCESSINGUNITSEQ INTO V_PROCESSINGUNITSEQ  FROM cs_processingunit WHERE NAME = 'AGY_PU';
--Log('V_PROCESSINGUNITSEQ '||V_PROCESSINGUNITSEQ);
--SELECT MASK INTO v_BUSINESSUNITMAP1 FROM cs_businessunit WHERE name IN ('SGPAFA');
--Log('v_BUSINESSUNITMAP1 '||V_PROCESSINGUNITSEQ);
--SELECT MASK INTO v_BUSINESSUNITMAP2 FROM cs_businessunit WHERE name IN ('BRUAGY');
--Log('v_BUSINESSUNITMAP1 '||V_PROCESSINGUNITSEQ);
--
--/*EXECUTE IMMEDIATE 'ALTER INDEX INDX_PRD_CF_PAKEY REBUILD';
--EXECUTE IMMEDIATE 'ALTER INDEX INDX_PRD_CDIM_CREDITTYPE REBUILD';
--EXECUTE IMMEDIATE 'ALTER INDEX INDX_PRD_TDIM_PRODTNAME REBUILD';
--EXECUTE IMMEDIATE 'ALTER INDEX INDX_PRD_CD_GA4 REBUILD';
--EXECUTE IMMEDIATE 'ALTER INDEX INDX_PRD_CD_GA2 REBUILD';
--EXECUTE IMMEDIATE 'ALTER INDEX INDX_PRD_TDIM_EVENTTYPE REBUILD';*/
--
--/*insert into AIA_Report_Proc_Logs
--VALUES
--('AIA_RPT_PRC_PRD_TEMP',sysdate,'AGY_PU',v_periodname,'Starting The PRD District and Policy Report Temp Values Procedure');
--
--commit;
--
--insert into AIA_Report_Proc_Logs
--VALUES
--('AIA_RPT_PRC_PRD_TEMP',sysdate,'AGY_PU',v_periodname,'Starting The PRD District and Policy Report Temp Values in to AIA_RPT_PRD_TEMP_VALUES');
--
--commit;*/
--
----get report log sequence number
--SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
----job start log
--pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_INIT_BRU' , 'AIA_RPT_PRD_TEMP_CREDITS'  , 'Processing', 'insert into AIA_RPT_PRD_TEMP_CREDITS');
--
--EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_TEMP_CREDITS';
--
--insert into AIA_RPT_PRD_TEMP_CREDITS NOLOGGING
--  select V_PERIODSEQ            as periodseq,
--         ct.credittypeid        as cd_credittype,
--         crd.genericattribute13 as cd_ga13,
--          crd.genericattribute1 as cd_ga1,
--         crd.genericattribute2  as cd_ga2,
--         crd.genericattribute4  as cd_ga4,
--         crd.genericattribute6  as cd_ga6,
--         crd.genericnumber1     as cd_gn1,
--         crd.value              as cd_value,
--         pu.name                as processingunit,
--         crd.positionseq        as cd_positionseq,
--         pos.name               as participantid,
--         ttl.name               as positiontitle,
--         crd.genericdate2       as cd_gd2,
--         crd.genericattribute12 as cd_ga12,
--         crd.genericattribute14 as cd_ga14,
--         crd.name               as cd_name,
--         crd.salestransactionseq as cd_txn_seq,
--         crd.businessunitmap as businessunitmap,
--     case
--     --for BN
--     when ttl.name like 'BR%'
--       then '0'
--     --for the records with policy issue date < 12/1/2015
--     when crd.genericdate2 < to_date('12/1/2015', 'mm/dd/yyyy') then '0'
--     --for all CS/PL RYC
--     when ct.credittypeid like '%RYC%' and crd.genericattribute2 in ('CS','CL','PL')
--       then '0'
--     --for all FYP and Case Count
--     when ct.credittypeid in ('FYP','Case_Count')
--       then '0'
--     --for Life/PA/CS FYC and policy issue date >= 12/1/2015
--     when crd.businessunitmap = v_BUSINESSUNITMAP1
--       and (ct.credittypeid like 'FYC%' or ct.credittypeid like 'API%' or ct.credittypeid like 'SSCP%' or ct.credittypeid like 'APB%')
--       and crd.genericdate2 >= to_date('12/1/2015', 'mm/dd/yyyy')
--       then '1'
--     --for Life/PA RYC and policy issue date >= 12/1/2015
--     when crd.businessunitmap = v_BUSINESSUNITMAP1
--       and ct.credittypeid like '%RYC%' and crd.genericattribute2 in ('LF','PA','HS','VL')
--       and crd.genericdate2 >= to_date('12/1/2015', 'mm/dd/yyyy')
--       then '1'
--     else '0'
--     end as NEW_PRD_IND
--    from cs_credit crd
--   inner join cs_credittype ct
--      on crd.credittypeseq = ct.datatypeseq
--      and ct.removedate = DT_REMOVEDATE
--   inner join cs_processingunit pu
--      on crd.processingunitseq = pu.processingunitseq
--   inner join cs_position pos
--      on crd.positionseq = pos.ruleelementownerseq
--     and pos.removedate = DT_REMOVEDATE
--     and pos.effectivestartdate < V_ENDDATE
--     AND pos.effectiveenddate > V_ENDDATE - 1
--   inner join cs_title ttl
--      on ttl.RULEELEMENTOWNERSEQ = pos.TITLESEQ
--     AND ttl.effectivestartdate < V_ENDDATE
--     AND ttl.effectiveenddate > V_ENDDATE - 1
--     AND ttl.REMOVEDATE = DT_REMOVEDATE
--   where crd.periodseq = V_PERIODSEQ --2533274790398962
--     AND crd.businessunitmap = v_BUSINESSUNITMAP2 -----ADD BY JEFF FOR VERSION 4 BRU ONLY
--     AND crd.GENERICATTRIBUTE2 IN
--         ('LF', 'HS', 'PA', 'PL', 'VL', 'CS', 'CL')
--     and ct.credittypeid in ('FYP',
--                             'FYC',
--                             'RYC',
--                             'API',
--                             'SSCP',
--                             'APB',
--                             'ORYC',
--                             'Case_Count',
--                             'API_W',
--                             'API_W_DUPLICATE',
--                             'API_WC_DUPLICATE',
--                             'SSCP_W',
--                             'SSCP_W_DUPLICATE',
--                             'SSCP_WC_DUPLICATE',
--                             'FYC_W',
--                             'FYC_W_DUPLICATE',
--                             'FYC_WC_DUPLICATE',
--                             'RYC_W',
--                             'RYC_W_DUPLICATE',
--                             'RYC_WC_DUPLICATE',
--                             'ORYC_W',
--                             'ORYC_W_DUPLICATE',
--                             'ORYC_WC_DUPLICATE');
--Log('Records inserted into AIA_RPT_PRD_TEMP_CREDITS are '||SQL%ROWCOUNT);
--
--commit;
--
--BEGIN
--        DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME => '"AIASEXT"',
--                                        TABNAME => '"AIA_RPT_PRD_TEMP_CREDITS"',
--                                        ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE);
--END;
--
----job end log
--pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_INIT_BRU' , 'Finish', '');
--
----get report log sequence number
--SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
----job start log
--pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_INIT_BRU' , 'AIA_RPT_PRD_TEMP_VALUES'  , 'Processing', 'insert into AIA_RPT_PRD_TEMP_VALUES');
--
--
--EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_TEMP_VALUES';
--
--/** Remark for report requirement
--1.The commissionable PRD reports (old report) will show LIFE FYC/RYC,PA FYC/ RYC,CS FYC of issue date < 12/1/2015 only,
--and CS RYC, PL RYC, FYP and CASE of all policies.
--
--2.The writing PRD reports (new report) will show LIFE FYC/RYC,PA FYC/ RYC,CS FYC policies of issue date>=12/1/2015,
--the CS RYC, PL RYC, FYP and CASE will be removed from the reports.
--*/
--
--INSERT
--/*+ APPEND */
--INTO AIA_RPT_PRD_TEMP_VALUES NOLOGGING
--  SELECT CF.PERIODSEQ,
--         CF.Cd_Positionseq as CD_POSITIONSEQ,
--         CF.POSITIONTITLE  as CD_TITLE,
--         CF.cd_credittype  as CREDITTYPE,
--         CF.CD_GA12,
--         --for writing/commission agency
--         case
--           when CF.NEW_PIB_IND = '0' then
--            CF.CD_GA13
--           when CF.NEW_PIB_IND = '1'
--               --and FN_FYO_PRD_TYPE(CF.CD_GA13, CF.CD_GA14, CF.CD_NAME)='DIRECT'
--                and CF.CD_NAME not like '%INDIRECT%' and
--                CF.CD_NAME like '%DIRECT%' then
--            to_char(substr(pos.name, 4))
--           else
--            CF.CD_GA13
--         end as CD_GA13,
--          CF.CD_GA1,
--         CF.CD_GA2,
--         CF.CD_GA4,
--         CF.CD_GA6,
--         CF.CD_GN1,
--         CF.CD_VALUE,
--         CF.CD_GD2,
--         CF.businessunitmap,
--         et.eventtypeid as EVENTTYPE,
-- ST.DATASOURCE as SOURCE_SYSTEM,
--         st.productname as PRODUCTNAME,
--         st.genericattribute3 as TXN_GA3,
--         st.genericattribute10 as TXN_GA10,
--         st.genericattribute11 as TXN_GA11,
--         st.genericnumber3 as TXN_GN3,
--         st.genericnumber5 as TXN_GN5,
--         st.genericnumber6 as TXN_GN6,
--         st.genericdate6 as TXN_GD6,
--         st.genericattribute14 as TXN_GA14,
--         st.genericattribute17 as TXN_GA17,
--         --PRD type is just for new policy
--         case CF.NEW_PIB_IND
--           when '1' then
--            case
--              when CF.CD_NAME not like '%INDIRECT%' and CF.CD_NAME like '%DIRECT%'
--                then 'DIRECT'
--              when CF.CD_NAME like '%INDIRECT%'
--                then 'INDIRECT'
--              ELSE
--               'PERSONAL'
--            end
--           else ''
--         end as PRD_TYPE,
--         CF.NEW_PIB_IND,
--         agn.genericnumber1/100 as Assign_GN1  ---add version 3
--         --add in version 5
--          ,gast.genericdate8 submitdate
--      -- end to add
--    FROM AIA_RPT_PRD_TEMP_CREDITS CF
--   inner join cs_salestransaction st
--      on CF.CD_TXN_SEQ = st.salestransactionseq
--   ---add version 3
--   left join cs_transactionassignment agn
--      on agn.salestransactionseq=st.salestransactionseq and (agn.positionname like 'BRT%')
--   --end
--   inner join cs_eventtype et
--      on st.eventtypeseq = et.datatypeseq
--     and et.removedate = DT_REMOVEDATE
--     ---add version 5
--      left join cs_gasalestransaction gast
--     on st.salestransactionseq = gast.salestransactionseq
--     and gast.pagenumber=0
--    --end to add
--   inner join Cs_Position pos
--      on CF.Cd_Positionseq = pos.ruleelementownerseq
--     AND pos.effectivestartdate < V_ENDDATE
--     AND pos.effectiveenddate > V_ENDDATE - 1
--     AND pos.removedate = DT_REMOVEDATE
--   WHERE CF.PROCESSINGUNIT = 'AGY_PU'
--     AND st.PRODUCTNAME IN ('LF', 'HS', 'PA', 'PL', 'VL', 'CS', 'CL');
--
--Log('Records inserted into AIA_RPT_PRD_TEMP_VALUES are '||SQL%ROWCOUNT);
--COMMIT;
--
--
--BEGIN
--        DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME => '"AIASEXT"',
--                                        tabname => '"AIA_RPT_PRD_TEMP_VALUES"',
--                                        ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE);
--
--END;
--
----job end log
--pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_INIT_BRU' , 'Finish', '');
--
----get report log sequence number
--SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
----job start log
--pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_INIT_BRU' , 'AIA_RPT_PRD_TEMP_PADIM'  , 'Processing', 'insert into AIA_RPT_PRD_TEMP_PADIM');
--
--
--
--EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_TEMP_PADIM';
--
--/*Commented by Suresh
--insert into AIA_RPT_PRD_TEMP_PADIM
--select
--       pn.ruleelementownerseq as POSITIONSEQ,
--       pn.managerseq,
--       pn.name,
--       pt.firstname,
--       pt.lastname,
--       pn.genericattribute1  as POS_GA1,
--       pn.genericattribute2  as POS_GA2,
--       pn.genericattribute3  as POS_GA3,
--       pn.genericattribute7  as POS_GA7,
--       pn.genericattribute9  as POS_GA9,
--       pn.genericattribute11 as POS_GA11,
--       pn.genericattribute4  as POS_GA4,
--       ttl.name           POSITIONTITLE,
--       pt.terminationdate     AGY_TERMINATIONDATE,
--       pn.genericdate4        ASSIGNED_DATE,
--       pn.genericdate1        AGY_APPOINTMENT_DATE,
--       pt.hiredate,
--       pt.genericattribute1  as PT_GA1,
--       pn.effectivestartdate,
--       pn.effectiveenddate,
--       pe.businessunitmap
--  from cs_position pn
--      inner join cs_participant pt
--      on pt.payeeseq = pn.payeeseq
--      and pt.removedate = DT_REMOVEDATE
--      AND pt.effectivestartdate < V_ENDDATE
--      AND pt.effectiveenddate   > V_ENDDATE-1
--      inner join cs_title ttl
--      on pn.titleseq = ttl.ruleelementownerseq
--      and ttl.removedate = DT_REMOVEDATE
--      AND ttl.effectivestartdate < V_ENDDATE
--      AND ttl.effectiveenddate   > V_ENDDATE-1
--      inner join cs_payee pe
--      ON  pn.payeeseq = pe.payeeseq
--      -----version 4 BRU only begin
--      --AND pe.businessunitmap IN (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--      AND pe.businessunitmap = V_BUSINESSUNITMAP2
--      ---end
--      AND pe.removedate = DT_REMOVEDATE
--      AND pe.effectivestartdate < V_ENDDATE
--      AND pe.effectiveenddate   > V_ENDDATE-1
--where pn.removedate = DT_REMOVEDATE
--   AND pn.effectivestartdate < V_ENDDATE
--   AND pn.effectiveenddate   > V_ENDDATE-1
--;
--*/
--
--        insert /*+ append */ into AIA_RPT_PRD_TEMP_PADIM
--      select
--      help.POSITIONSEQ,
--      help.managerseq,
--       help.name,
--       help.firstname,
--       help.lastname,
--       help.POS_GA1,
--       help.POS_GA2,
--       help.POS_GA3,
--       help.POS_GA7,
--       help.POS_GA9,
--       help.POS_GA11,
--       help.POS_GA4,
--       help.POSITIONTITLE,
--       help.AGY_TERMINATIONDATE,
--       help.ASSIGNED_DATE,
--       help.AGY_APPOINTMENT_DATE,
--       help.hiredate,
--       help.PT_GA1,
--       help.effectivestartdate,
--       help.effectiveenddate,
--       pe.businessunitmap
--      from
--      AIA_RPT_PRD_TEMP_PADIM_HELP help
--     inner join cs_payee pe
--      ON  help.payeeseq = pe.payeeseq
--      -----version 4 BRU only begin
--      --AND pe.businessunitmap IN (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--      AND pe.businessunitmap = V_BUSINESSUNITMAP2
--      ---end
--      AND pe.removedate = DT_REMOVEDATE
--      AND pe.effectivestartdate < V_ENDDATE
--      AND pe.effectiveenddate   > V_ENDDATE-1;
--
--      Log('Records inserted into AIA_RPT_PRD_TEMP_PADIM for businessunit 2 are '||SQL%ROWCOUNT);
--      dbms_output.put_line('rows inserted in AIA_RPT_PRD_TEMP_PADIM_HELP are for businessunit 2 '||SQL%ROWCOUNT);
--
--
--COMMIT;
--
--BEGIN
--        DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME => '"AIASEXT"',
--                                        TABNAME => '"AIA_RPT_PRD_TEMP_PADIM"',
--                                        ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE);
--END;
--
----job end log
--pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_INIT_BRU' , 'Finish', '');
--
--END REP_RUN_INIT_BRU;


PROCEDURE REP_RUN_PRD_WRI as

  V_EOT                DATE := TO_DATE('01/01/2200','DD/MM/YYYY');
  V_PERIODNAME         VARCHAR2(255 BYTE);
  V_YTD_PRD_START_DATE DATE;
  V_YTD_PRD_END_DATE   DATE;
  V_CALENDARNAME       VARCHAR2(255);
  V_PROCESSINGUNITSEQ  INTEGER;
  V_PROCESSINGUNITNAME VARCHAR2(256);
  V_PERIODSTARTDATE    DATE;
  V_PERIODENDDATE      DATE;
  V_CALENDARSEQ       INTEGER;
  V_PROCNAME          VARCHAR2(256);
  V_SYSDATE           DATE;
  V_PERIODTYPESEQ     CS_PERIOD.PERIODTYPESEQ%TYPE;

Begin
  --V_PROCNAME := 'PROC_RPT_PRD_DISTRICT';
V_SYSDATE := SYSDATE;

BEGIN
SELECT P.STARTDATE,P.ENDDATE,C.DESCRIPTION,P.NAME,P.CALENDARSEQ
INTO V_PERIODSTARTDATE,V_PERIODENDDATE,V_CALENDARNAME,V_PERIODNAME,V_CALENDARSEQ
FROM  CS_PERIOD P INNER JOIN  CS_CALENDAR C ON P.CALENDARSEQ=C.CALENDARSEQ
WHERE PERIODSEQ = V_PERIODSEQ
;
SELECT PERIODTYPESEQ INTO V_PERIODTYPESEQ FROM  CS_PERIODTYPE WHERE NAME ='month';

end;

-----------=======LOG
  SELECT PROCESSINGUNITSEQ,NAME  INTO V_PROCESSINGUNITSEQ,V_PROCESSINGUNITNAME
  FROM CS_PROCESSINGUNIT  WHERE NAME = 'AGY_PU';

  SELECT MASK INTO v_BUSINESSUNITMAP1 FROM CS_BUSINESSUNIT
   WHERE NAME        IN ('SGPAFA');
  SELECT MASK INTO v_BUSINESSUNITMAP2 FROM CS_BUSINESSUNIT
   WHERE NAME        IN ('BRUAGY');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_WRI' , 'AIA_RPT_PRD_DIST_WRI_NEW 1'  , 'Processing', 'insert into AIA_RPT_PRD_DIST_WRI_NEW');

  EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_DIST_WRI_NEW_HELP1';

insert into AIA_RPT_PRD_DIST_WRI_NEW_HELP1
SELECT
        pos_dis.name as name,
        pos_dis.effectivestartdate as effstartdt,
        pos_dis.effectiveenddate as effenddt,
        par_dis.firstname as firstname,
        par_dis.lastname as lastname,
        pos_dis.genericattribute2 ,
        pos_dis.genericattribute7  ,
        pos_dis.genericattribute11 ,
        pos_dis.genericattribute4
        FROM cs_position pos_dis
        --on pos_dis.name=trim('SGY'||pos_agy.genericattribute3)
        --AND pos_dis.effectivestartdate <= PD.CD_GD2 --policy issue date
           --AND pos_dis.effectiveenddate   > PD.CD_GD2  --policy issue date
           --for writing district participant info
           inner join cs_participant par_dis
           on par_dis.PAYEESEQ = pos_dis.PAYEESEQ
           AND par_dis.effectivestartdate < V_ENDDATE
           AND par_dis.effectiveenddate   >  V_ENDDATE-1
           AND par_dis.removedate = DT_REMOVEDATE
           WHERE pos_dis.removedate =DT_REMOVEDATE;

 Log('0 Records inserted into AIA_RPT_PRD_DIST_WRI_NEW_HELP1 are '||SQL%ROWCOUNT);
COMMIT;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_DIST_WRI_NEW_HELP';

insert /*+APPEND*/ into AIA_RPT_PRD_DIST_WRI_NEW_HELP NOLOGGING
(select /*+   */
       decode(PD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') BUNAME,
      --decode(PD.businessunitmap,1,'SGPAFA',2,'BRUAGY') BUNAME,
        PD.businessunitmap BUMAP,
        PD.CD_GA13 UNIT_CODE,
        CASE
          WHEN (PD.CD_GA2 IN ('LF','HS')
          --AND PD.CD_CREDITTYPE         IN ('FYC','API','SSCP','APB')
          AND PD.CD_CREDITTYPE          IN ('FYC', 'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                                            'API', 'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                                            'SSCP', 'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                                            'APB')
          )
          THEN PD.CD_VALUE
          ELSE 0
        END LIFE_FYC_API_SSC,
        CASE
 WHEN (PD.CD_GA2 IN ('LF','HS')
 --AND PD.CD_CREDITTYPE IN ('FYC','API','SSCP','APB')
 AND PD.CD_GA4 IN ('PAY1','PAY0','PAYE','PAYF')
 )
 THEN PD.CD_VALUE
 ELSE 0
 END LIFE_FYC_RP,

 CASE
 WHEN (PD.CD_GA2 IN ('LF','HS')
 --AND PD.CD_CREDITTYPE IN ('FYC','API','SSCP','APB')
 AND PD.CD_GA4 IN ('PAYS','PTAF')
 AND PD.CD_GA1 NOT IN ('GFBA','GFBC','IACB','IAGB','IAP4','IARB','IAS1','IAS2','IAS3','IBOB','IBSB','IFRB','IFYP','IFZP','IGCB','IPGA','IPOA','IPOB','IPOC','IPOD','ISAC','ISBO','ISFS','ISGR','IWBO','IWBS','IWCB','IWRB')
 )
 THEN PD.CD_VALUE
 ELSE 0
 END LIFE_FYC_NONILP,

  CASE
 WHEN (PD.CD_GA2 IN ('LF','HS')
 --AND PD.CD_CREDITTYPE IN ('FYC','API','SSCP','APB')
 AND PD.CD_GA4 IN ('PAYS')
 AND PD.CD_GA1 IN ('GFBA','GFBC','IACB','IAGB','IAP4','IARB','IAS1','IAS2','IAS3','IBOB','IBSB','IFRB','IFYP','IFZP','IGCB','IPGA','IPOA','IPOB','IPOC','IPOD','ISAC','ISBO','ISFS','ISGR','IWBO','IWBS','IWCB','IWRB')
 )
 THEN PD.CD_VALUE
 ELSE 0
 END LIFE_FYC_ILP,

 CASE
 WHEN (PD.CD_GA2 IN ('LF','HS','PA','CS')
 --AND PD.CD_CREDITTYPE IN ('FYC','API','SSCP','APB')
 AND PD.CD_GA4 IN ('PAYT')
 )
 THEN PD.CD_VALUE
 ELSE 0
 END LIFE_FYC_TOPUP,
        CASE
          WHEN (PD.CD_GA2 IN ('PA') --,'VL') as asked by Donny
          --AND PD.CD_CREDITTYPE         IN ('FYC','API','SSCP','APB')
          AND PD.CD_CREDITTYPE          IN ('FYC', 'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                                            'API', 'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                                            'SSCP', 'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                                            'APB')
          )
          THEN PD.CD_VALUE
          ELSE 0
        END PA_FYC,
        CASE
          WHEN ( PD.CD_GA2 IN ('CS','CL')
          --AND PD.CD_CREDITTYPE           IN ('FYC','API','SSCP','APB')
          AND PD.CD_CREDITTYPE          IN ('FYC', 'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                                            'API', 'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                                            'SSCP', 'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                                            'APB')
          )
          THEN PD.CD_VALUE
          ELSE 0
        END CS_FYC,
        CASE
          WHEN (PD.CD_GA4 = 'PAY2'
          AND PD.CD_CREDITTYPE          IN ('RYC','RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
          AND PD.CD_GA2  IN ('LF','HS') )
          THEN PD.CD_VALUE
          ELSE 0
        END LIFE_2YEAR,
        CASE
          WHEN (PD.CD_GA4 = 'PAY3'
          AND PD.CD_CREDITTYPE          IN ('RYC','RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
          AND PD.CD_GA2  IN ('LF','HS') )
          THEN PD.CD_VALUE
          ELSE 0
        END LIFE_3YEAR,
        CASE
          WHEN ( PD.CD_GA4 IN ('PAY4','PAY5','PAY6')
          AND PD.CD_CREDITTYPE           IN ('RYC','RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
          AND PD.CD_GA2    IN ('LF','HS') )
          THEN PD.CD_VALUE
          ELSE 0
        END LIFE_4YEAR,
        CASE
          WHEN (PD.CD_GA2 IN ('PA')--,'VL')
          AND PD.CD_CREDITTYPE          IN ('RYC','RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') )
          THEN PD.CD_VALUE
          ELSE 0
        END PA_RC,
        SUBSTR(help1.NAME,4) DISTRICT_CODE,
        help1.firstname || ' ' || help1.lastname DM_NAME,
        help1.genericattribute2 DIST_LEADER_CODE,
        help1.genericattribute7 DIST_LEADER_NAME,
        help1.genericattribute11 DIST_LEAER_TITLE,
        help1.genericattribute4 DIST_LEADER_CLASS,
        PD.businessunitmap as businessunitmap,
        PD.CD_GA12 as CD_GA12,
        PD.CD_POSITIONSEQ as CD_POSITIONSEQ,
    '' CD_GA13,
    0 LIFE_CASE,
    0 PA_CASE,
    0 UNASSIGNED_LIFE_2YEAR,
    0 UNASSIGNED_LIFE_3YEAR,
    0 UNASSIGNED_LIFE_4YEAR,
    0 UNASSIGNED_PA_RC
        from
        AIA_RPT_PRD_TEMP_VALUES PD
           --for writing agency postion info
           inner join cs_position pos_agy
           on pos_agy.name = trim('SGY'||PD.CD_GA13)
           AND pos_agy.effectivestartdate <= PD.CD_GD2 --policy issue date
           AND pos_agy.effectiveenddate   > PD.CD_GD2  --policy issue date
           AND pos_agy.removedate =DT_REMOVEDATE
           --for writing district postion info
           inner join AIA_RPT_PRD_DIST_WRI_NEW_HELP1 help1
           ON help1.name=trim('SGY'||pos_agy.genericattribute3)
           AND help1.effstartdt <= PD.CD_GD2 --policy issue date
           AND help1.effenddt   > PD.CD_GD2  --policy issue date
        WHERE PD.NEW_PIB_IND = 1 AND PD.PIB_TYPE = 'DIRECT'
        --add in version 5
        --AND ( PD.CD_GA2  IN ('PA','HS','CS','PL','CL','VL','LF') or nvl(PD.submitdate,TO_DATE('2016/01/01','yyyy/mm/dd')) < TO_DATE('2017/01/01','yyyy/mm/dd'))
        --version 9 add GI product
        AND ( PD.CD_GA2  IN ('PA','HS','CS','PL','CL','VL','LF','GI') or nvl(PD.submitdate,TO_DATE('2016/01/01','yyyy/mm/dd')) < TO_DATE('2017/01/01','yyyy/mm/dd'))
        AND PD.CD_CREDITTYPE IN ('FYP', 'FYC', 'RYC', 'API', 'SSCP', 'APB',
             'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
             'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
             'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
             'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
        --AND PD.CD_GA2  IN ('LF','PA','HS','CS','PL','CL','VL')
        AND PD.CD_GA2  IN ('LF','PA','HS','CS','PL','CL','VL','GI')  --version 9 add GI product
 );

 Log('0 Records inserted into AIA_RPT_PRD_DIST_WRI_NEW_HELP are '||SQL%ROWCOUNT);
COMMIT;

   BEGIN
        DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME => '"AIASEXT"' ,
                                        TABNAME => '"AIA_RPT_PRD_DIST_WRI_NEW_HELP"' ,
                                        ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE);
END;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_DIST_WRI_NEW';

INSERT /*+ Append  */ INTO AIA_RPT_PRD_DIST_WRI_NEW NOLOGGING
(SELECT /*+  */
        V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
        V_PROCESSINGUNITNAME PUNAME,
        PD.BUNAME,
        PD.BUMAP,
        V_CALENDARSEQ CALENDARSEQ,
        V_CALENDARNAME CALENDARNAME,
        V_PERIODSEQ PERIODKEY,
        V_PERIODNAME periodname,
        --PAD.POSITIONSEQ POSITIONSEQ,
        PAD.MANAGERSEQ,
        PAD.POSITIONSEQ POSITIONSEQ,
        PAD.NAME,
        PD.DISTRICT_CODE,
        PD.DM_NAME,
        PD.DIST_LEADER_CODE,
        PD.DIST_LEADER_NAME,
        PD.DIST_LEAER_TITLE,
        PD.DIST_LEADER_CLASS,
        PD.UNIT_CODE,
        agy.firstname|| ' '|| agy.lastname AGENCY,
        agy.POS_GA2 UNIT_LEADER_CODE,
        agy.POS_GA7 UNIT_LEADER_NAME,
        DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
        agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
        (
        CASE
          WHEN AGY.POSITIONTITLE IN ('ORGANISATION','UNIT')
          THEN AGY.AGY_TERMINATIONDATE
        END)DISSOLVED_DATE,
        SUBSTR(PAD.NAME,4) AGT_CODE,
        (PAD.firstname
        || PAD.lastname) NAME,
        DECODE(PAD.positiontitle, 'FSC_NON_PROCESS', 'FSC', PAD.positiontitle) ROLE,
        Pad.POS_GA4 CLASS,
        PAD.HIREDATE CONTRACT_DATE,
        PAD.AGY_APPOINTMENT_DATE APPOINTMENT_DATE,
        PAD.AGY_TERMINATIONDATE TERMINATION_DATE,
        (
        CASE
          WHEN pad.PT_GA1 = '00'
          THEN 'INFORCE'
          WHEN pad.PT_GA1 IN ('50','51','52','55','56')
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 = '13'
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 IN ('60','61')
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 = '70'
          THEN 'TERMINATED'
        END) AGENT_STATUS,

        --removed from report
        0 as LIFE_FYP,
        0 as PA_FYP,
        0 as LIFE_CASE,
        0 as PA_CASE,

        PD.LIFE_FYC_API_SSC,
        PD.LIFE_FYC_RP,
        PD.LIFE_FYC_NONILP,
        PD.LIFE_FYC_ILP,
        PD.LIFE_FYC_TOPUP,
        PD.PA_FYC,
        PD.CS_FYC,
        PD.LIFE_2YEAR,
        PD.LIFE_3YEAR,
        PD.LIFE_4YEAR,
        PD.PA_RC,

        --removed from report
        0 as CS_RC,
        0 as CS_PL,

        0 UNASSIGNED_LIFE_2YEAR,
        0 UNASSIGNED_LIFE_3YEAR,
        0 UNASSIGNED_LIFE_4YEAR,
        0 UNASSIGNED_PA_RC,
        0 LIFE_FYP_YTD,
        0 PA_FYP_YTD,
        0 LIFE_CASE_YTD,
        0 PA_CASE_YTD,
        0 LIFE_FYC_API_SSC_YTD,
        0 LIFE_FYC_RP_YTD,
        0 LIFE_FYC_NONILP_YTD,
        0 LIFE_FYC_ILP_YTD,
        0 LIFE_FYC_TOPUP_YTD,
        0 PA_FYC_YTD,
        0 CS_FYC_YTD,
        0 LIFE_2YEAR_YTD,
        0 LIFE_3YEAR_YTD,
        0 LIFE_4YEAR_YTD,
        0 PA_RC_YTD,
        0 CS_RC_YTD,
        0 CS_PL_YTD,
        0 UNASSIGNED_LIFE_2YEAR_YTD,
        0 UNASSIGNED_LIFE_3YEAR_YTD,
        0 UNASSIGNED_LIFE_4YEAR_YTD,
        0 UNASSIGNED_PA_RC_YTD
      FROM AIA_RPT_PRD_TEMP_PADIM pad,
           AIA_RPT_PRD_TEMP_PADIM agy,
           AIA_RPT_PRD_TEMP_PADIM agy_ldr,
           --AIA_RPT_PRD_TEMP_PADIM DISTRICT,
           AIA_RPT_PRD_DIST_WRI_NEW_HELP PD
      WHERE PAD.POSITIONTITLE IN ('AD','DIR','ED','FC','SD')
      AND PAD.POS_GA4 NOT IN ('45','48')
      AND (
      (PD.businessunitmap = 64 and PD.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (PD.businessunitmap <> 64 and PD.CD_POSITIONSEQ = PAD.POSITIONSEQ)
      )
      --AND PD.CD_CREDITTYPE IN ('FYP', 'RYC', 'FYC','API','SSCP','APB')

      AND AGY.POS_GA1 = PD.UNIT_CODE
      AND AGY.POSITIONTITLE IN ('ORGANISATION','UNIT')
      AND ((agy_ldr.NAME = 'SGT'
        || agy.POS_GA2
      AND agy.NAME LIKE 'SGY%')   )
);


    --Ended By suresh

      Log('1 Records inserted into AIA_RPT_PRD_DIST_WRI_NEW are '||SQL%ROWCOUNT);
     COMMIT;

   BEGIN
        DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME => '"AIASEXT"' ,
                                        TABNAME => '"AIA_RPT_PRD_DIST_WRI_NEW"' ,
                                        ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE);
END;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_WRI' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_WRI' , 'AIA_RPT_PRD_DIST_WRI_NEW 2'  , 'Processing', 'insert into AIA_RPT_PRD_DIST_WRI_NEW');


-----------=======LOG
/* Commented by Suresh
INSERT
    /*+ Append */
/*  INTO AIA_RPT_PRD_DIST_WRI_NEW NOLOGGING
      (SELECT
        V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
        V_PROCESSINGUNITNAME PUNAME,
        decode(PD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') BUNAME,
        PD.businessunitmap BUMAP,
        V_CALENDARSEQ CALENDARSEQ,
        V_CALENDARNAME CALENDARNAME,
        V_PERIODSEQ PERIODKEY,
        V_PERIODNAME periodname,
        --PAD.POSITIONSEQ POSITIONSEQ,
        PAD.MANAGERSEQ,
        PAD.POSITIONSEQ POSITIONSEQ,
        PAD.NAME,
/*      SUBSTR(DISTRICT.NAME,4) DISTRICT_CODE,
        district.firstname
        || ' '
        || district.lastname DM_NAme,
        district.POS_GA2 DIST_LEADER_CODE,
        district.POS_GA7 DIST_LEADER_NAME,
        district.POS_GA11 DIST_LEAER_TITLE,
        DISTRICT.POS_GA4 DIST_LEADER_CLASS,*/
  /*      SUBSTR(pos_dis.NAME,4) DISTRICT_CODE,
      par_dis.firstname || ' ' || par_dis.lastname DM_NAME,
      pos_dis.genericattribute2 DIST_LEADER_CODE,
      pos_dis.genericattribute7 DIST_LEADER_NAME,
      pos_dis.genericattribute11 DIST_LEAER_TITLE,
      pos_dis.genericattribute4 DIST_LEADER_CLASS,
      PD.TRANS_GA11 UNIT_CODE,
      agy.firstname
      || ' '
      || agy.lastname AGENCY,
      agy.POS_GA2 UNIT_LEADER_CODE,
      agy.POS_GA7 UNIT_LEADER_NAME,
      DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
      agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
      (
      CASE
        WHEN AGY.POSITIONTITLE IN ('AGENCY','DISTRICT','BR_AGENCY','BR_DISTRICT')
        THEN AGY.AGY_TERMINATIONDATE
      END)DISSOLVED_DATE,
      SUBSTR(PAD.NAME,4) AGT_CODE,
      (PAD.firstname
      || PAD.lastname) NAME,
      DECODE(PAD.positiontitle, 'FSC_NON_PROCESS', 'FSC', PAD.positiontitle) ROLE,
      Pad.POS_GA4 CLASS,
      PAD.HIREDATE CONTRACT_DATE,
      PAD.AGY_APPOINTMENT_DATE APPOINTMENT_DATE,
      PAD.AGY_TERMINATIONDATE TERMINATION_DATE,
      (
      CASE
        WHEN pad.PT_GA1 = '00'
        THEN 'INFORCE'
        WHEN pad.PT_GA1 IN ('50','51','52','55','56')
        THEN 'TERMINATED'
        WHEN pad.PT_GA1 = '13'
        THEN 'TERMINATED'
        WHEN pad.PT_GA1 IN ('60','61')
        THEN 'TERMINATED'
        WHEN pad.PT_GA1 = '70'
        THEN 'TERMINATED'
      END) AGENT_STATUS,
      0 LIFE_FYP,
      0 PA_FYP,
      CASE
        WHEN PD.CD_GA2 IN ('LF','HS')
        AND PD.CD_CREDITTYPE ='Case_Count'
        THEN PD.TRANS_GN5
        ELSE 0
      END LIFE_CASE,
      CASE
        WHEN PD.CD_GA2 = 'PA'
        AND PD.CD_CREDITTYPE ='Case_Count'
        THEN PD.TRANS_GN5
        ELSE 0
      END PA_CASE,
      0 LIFE_FYC_API_SSC,
      0 PA_FYC,
      0 CS_FYC,
      0 LIFE_2YEAR,
      0 LIFE_3YEAR,
      0 LIFE_4YEAR,
      0 PA_RC,
      0 CS_RC,
      0 CS_PL,
      0 UNASSIGNED_LIFE_2YEAR,
      0 UNASSIGNED_LIFE_3YEAR,
      0 UNASSIGNED_LIFE_4YEAR,
      0 UNASSIGNED_PA_RC,
      0 LIFE_FYP_YTD,
      0 PA_FYP_YTD,
      0 LIFE_CASE_YTD,
      0 PA_CASE_YTD,
      0 LIFE_FYC_API_SSC_YTD,
      0 PA_FYC_YTD,
      0 CS_FYC_YTD,
      0 LIFE_2YEAR_YTD,
      0 LIFE_3YEAR_YTD,
      0 LIFE_4YEAR_YTD,
      0 PA_RC_YTD,
      0 CS_RC_YTD,
      0 CS_PL_YTD,
      0 UNASSIGNED_LIFE_2YEAR_YTD,
      0 UNASSIGNED_LIFE_3YEAR_YTD,
      0 UNASSIGNED_LIFE_4YEAR_YTD,
      0 UNASSIGNED_PA_RC_YTD
    FROM AIA_RPT_PRD_TEMP_PADIM pad,
         AIA_RPT_PRD_TEMP_PADIM agy,
         AIA_RPT_PRD_TEMP_PADIM agy_ldr,
         --AIA_RPT_PRD_TEMP_PADIM DISTRICT,
         AIA_RPT_PRD_TEMP_VALUES PD
         --for writing agency postion info
         inner join cs_position pos_agy
         on pos_agy.name = trim('SGY'||PD.CD_GA13)
         AND pos_agy.effectivestartdate <= PD.CD_GD2 --policy issue date
         AND pos_agy.effectiveenddate   > PD.CD_GD2  --policy issue date
         AND pos_agy.removedate =DT_REMOVEDATE
         --for writing district postion info
         inner join cs_position pos_dis
         on pos_dis.name=trim('SGY'||pos_agy.genericattribute3)
         AND pos_dis.effectivestartdate <= PD.CD_GD2 --policy issue date
         AND pos_dis.effectiveenddate   > PD.CD_GD2  --policy issue date
         AND pos_dis.removedate =DT_REMOVEDATE
         --for writing district participant info
         inner join cs_participant par_dis
         on par_dis.PAYEESEQ = pos_dis.PAYEESEQ
         AND par_dis.effectivestartdate < V_ENDDATE
         AND par_dis.effectiveenddate   >  V_ENDDATE-1
         AND par_dis.removedate = DT_REMOVEDATE
    WHERE PD.NEW_PRD_IND=1 AND PD.PRD_TYPE = 'DIRECT'
    --add in version 5
     AND ( PD.CD_GA2  IN ('PA','HS','CS','PL','CL','VL') or nvl(PD.submitdate,TO_DATE('2016/01/01','yyyy/mm/dd')) < TO_DATE('2017/01/01','yyyy/mm/dd'))
    --end to add
    AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD','AM','FSD','FSM','BR_DM','BR_FSC','BR_UM')
    AND PAD.POS_GA4 NOT IN ('45','48')
    --AND PD.CD_POSITIONSEQ = PAD.POSITIONSEQ
    AND (
    (PD.businessunitmap = 1 and PD.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (PD.businessunitmap <> 1 and PD.CD_POSITIONSEQ = PAD.POSITIONSEQ)
    )
    --AND PD.CD_CREDITTYPE IN ('FYP', 'RYC', 'FYC','API','SSCP','APB','Case_Count')
    AND PD.CD_CREDITTYPE IN ('FYP', 'FYC', 'RYC', 'API', 'SSCP', 'APB', 'Case_Count',
                           'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                           'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                           'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                           'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
    --AND AGY.POS_GA1 = PD.TRANS_GA11
    AND AGY.POS_GA1 = PD.CD_GA13
    AND AGY.POSITIONTITLE      IN ('AGENCY','DISTRICT','BR_AGENCY','BR_DISTRICT')
    AND ((agy_ldr.NAME = 'SGT'
      || agy.POS_GA2
    AND agy.NAME LIKE 'SGY%')
    OR (agy_ldr.NAME = 'BRT'
      || agy.POS_GA2
    AND agy.NAME LIKE 'BRY%'))
    --AND DISTRICT.POS_GA3 = AGY.POS_GA3
    --AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
    --AND district.positiontitle     IN ('DISTRICT','BR_DISTRICT')
    AND PD.CD_GA2  IN ('LF','HS','PA'));

    */

  --Added by Suresh
        EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_DIST_WRI_NEW_HELP';

insert /*+APPEND*/ into AIA_RPT_PRD_DIST_WRI_NEW_HELP NOLOGGING
(select /*+   */
        decode(PD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') BUNAME,
        PD.businessunitmap BUMAP,
        PD.TRANS_GA11 UNIT_CODE,
        0 LIFE_FYC_API_SSC,
        0 LIFE_FYC_RP,
        0 LIFE_FYC_NONILP,
        0 LIFE_FYC_ILP,
        0 LIFE_FYC_TOPUP,
        0 PA_FYC,
        0 CS_FYC,
        0 LIFE_2YEAR,
        0 LIFE_3YEAR,
        0 LIFE_4YEAR,
        0 PA_RC,
        SUBSTR(help1.NAME,4) DISTRICT_CODE,
        help1.firstname || ' ' || help1.lastname DM_NAME,
        help1.genericattribute2 DIST_LEADER_CODE,
        help1.genericattribute7 DIST_LEADER_NAME,
        help1.genericattribute11 DIST_LEAER_TITLE,
        help1.genericattribute4 DIST_LEADER_CLASS,
        PD.businessunitmap as businessunitmap,
        PD.CD_GA12 as CD_GA12,
        PD.CD_POSITIONSEQ as CD_POSITIONSEQ,

        PD.CD_GA13 as CD_GA13,
        CASE
          WHEN PD.CD_GA2 IN ('LF','HS')
          AND PD.CD_CREDITTYPE ='Case_Count'
          THEN PD.TRANS_GN5
          ELSE 0
        END LIFE_CASE,
        CASE
          WHEN PD.CD_GA2 = 'PA'
          AND PD.CD_CREDITTYPE ='Case_Count'
          THEN PD.TRANS_GN5
          ELSE 0
        END PA_CASE,
            0 UNASSIGNED_LIFE_2YEAR,
    0 UNASSIGNED_LIFE_3YEAR,
    0 UNASSIGNED_LIFE_4YEAR,
    0 UNASSIGNED_PA_RC

        from
        AIA_RPT_PRD_TEMP_VALUES PD
           --for writing agency postion info
           inner join cs_position pos_agy
           on pos_agy.name = trim('SGY'||PD.CD_GA13)
           AND pos_agy.effectivestartdate <= PD.CD_GD2 --policy issue date
           AND pos_agy.effectiveenddate   > PD.CD_GD2  --policy issue date
           AND pos_agy.removedate =DT_REMOVEDATE
           --for writing district postion info
           inner join AIA_RPT_PRD_DIST_WRI_NEW_HELP1 help1
           ON help1.name=trim('SGY'||pos_agy.genericattribute3)
           AND help1.effstartdt <= PD.CD_GD2 --policy issue date
           AND help1.effenddt   > PD.CD_GD2  --policy issue date
        WHERE PD.NEW_PIB_IND=1 AND PD.PIB_TYPE = 'DIRECT'
        --add in version 5
       --AND ( PD.CD_GA2  IN ('PA','HS','CS','PL','CL','VL') or nvl(PD.submitdate,TO_DATE('2016/01/01','yyyy/mm/dd')) < TO_DATE('2017/01/01','yyyy/mm/dd'))
       --version 9 add GI product
       AND ( PD.CD_GA2  IN ('PA','HS','CS','PL','CL','VL','GI') or nvl(PD.submitdate,TO_DATE('2016/01/01','yyyy/mm/dd')) < TO_DATE('2017/01/01','yyyy/mm/dd'))
      --end to add
      AND PD.CD_CREDITTYPE IN ('FYP', 'FYC', 'RYC', 'API', 'SSCP', 'APB', 'Case_Count',
                             'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                             'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                             'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                             'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
         AND PD.CD_GA2  IN ('LF','HS','PA'));

      Log('2 Records inserted into AIA_RPT_PRD_DIST_WRI_NEW_HELP 2 are '||SQL%ROWCOUNT);

COMMIT;

   BEGIN
        DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME => '"AIASEXT"' ,
                                        TABNAME => '"AIA_RPT_PRD_DIST_WRI_NEW_HELP"' ,
                                        ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE);
END;


  INSERT /*+ Append  */ INTO AIA_RPT_PRD_DIST_WRI_NEW NOLOGGING
(SELECT /*+  */
        V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
        V_PROCESSINGUNITNAME PUNAME,
        PD.BUNAME,
        PD.BUMAP,
        V_CALENDARSEQ CALENDARSEQ,
        V_CALENDARNAME CALENDARNAME,
        V_PERIODSEQ PERIODKEY,
        V_PERIODNAME periodname,
        --PAD.POSITIONSEQ POSITIONSEQ,
        PAD.MANAGERSEQ,
        PAD.POSITIONSEQ POSITIONSEQ,
        PAD.NAME,
        PD.DISTRICT_CODE,
        PD.DM_NAME,
        PD.DIST_LEADER_CODE,
        PD.DIST_LEADER_NAME,
        PD.DIST_LEAER_TITLE,
        PD.DIST_LEADER_CLASS,
        PD.UNIT_CODE,
        agy.firstname|| ' '|| agy.lastname AGENCY,
        agy.POS_GA2 UNIT_LEADER_CODE,
        agy.POS_GA7 UNIT_LEADER_NAME,
        DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
        agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
        (
        CASE
          WHEN AGY.POSITIONTITLE IN ('ORGANISATION','UNIT')
          THEN AGY.AGY_TERMINATIONDATE
        END)DISSOLVED_DATE,
        SUBSTR(PAD.NAME,4) AGT_CODE,
        (PAD.firstname
        || PAD.lastname) NAME,
        DECODE(PAD.positiontitle, 'FSC_NON_PROCESS', 'FSC', PAD.positiontitle) ROLE,
        Pad.POS_GA4 CLASS,
        PAD.HIREDATE CONTRACT_DATE,
        PAD.AGY_APPOINTMENT_DATE APPOINTMENT_DATE,
        PAD.AGY_TERMINATIONDATE TERMINATION_DATE,
        (
        CASE
          WHEN pad.PT_GA1 = '00'
          THEN 'INFORCE'
          WHEN pad.PT_GA1 IN ('50','51','52','55','56')
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 = '13'
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 IN ('60','61')
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 = '70'
          THEN 'TERMINATED'
        END) AGENT_STATUS,

        --removed from report
        0 as LIFE_FYP,
        0 as PA_FYP,
        PD.LIFE_CASE,
        PD.PA_CASE,

        PD.LIFE_FYC_API_SSC,
        PD.LIFE_FYC_RP,
        PD.LIFE_FYC_NONILP,
        PD.LIFE_FYC_ILP,
        PD.LIFE_FYC_TOPUP,
        PD.PA_FYC,
        PD.CS_FYC,
        PD.LIFE_2YEAR,
        PD.LIFE_3YEAR,
        PD.LIFE_4YEAR,
        PD.PA_RC,

        --removed from report
        0 as CS_RC,
        0 as CS_PL,

        0 UNASSIGNED_LIFE_2YEAR,
        0 UNASSIGNED_LIFE_3YEAR,
        0 UNASSIGNED_LIFE_4YEAR,
        0 UNASSIGNED_PA_RC,
        0 LIFE_FYP_YTD,
        0 PA_FYP_YTD,
        0 LIFE_CASE_YTD,
        0 PA_CASE_YTD,
        0 LIFE_FYC_API_SSC_YTD,
        0 LIFE_FYC_RP_YTD,
        0 LIFE_FYC_NONILP_YTD,
        0 LIFE_FYC_ILP_YTD,
        0 LIFE_FYC_TOPUP_YTD ,
        0 PA_FYC_YTD,
        0 CS_FYC_YTD,
        0 LIFE_2YEAR_YTD,
        0 LIFE_3YEAR_YTD,
        0 LIFE_4YEAR_YTD,
        0 PA_RC_YTD,
        0 CS_RC_YTD,
        0 CS_PL_YTD,
        0 UNASSIGNED_LIFE_2YEAR_YTD,
        0 UNASSIGNED_LIFE_3YEAR_YTD,
        0 UNASSIGNED_LIFE_4YEAR_YTD,
        0 UNASSIGNED_PA_RC_YTD
      FROM AIA_RPT_PRD_TEMP_PADIM pad,
           AIA_RPT_PRD_TEMP_PADIM agy,
           AIA_RPT_PRD_TEMP_PADIM agy_ldr,
           --AIA_RPT_PRD_TEMP_PADIM DISTRICT,
           AIA_RPT_PRD_DIST_WRI_NEW_HELP PD
      WHERE  PAD.POSITIONTITLE IN ('AD','DIR','ED','FC','SD')
      AND PAD.POS_GA4 NOT IN ('45','48')
      --AND PD.CD_POSITIONSEQ = PAD.POSITIONSEQ
      AND (
      (PD.businessunitmap = 64 and PD.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (PD.businessunitmap <> 64 and PD.CD_POSITIONSEQ = PAD.POSITIONSEQ)
      )
      AND AGY.POS_GA1 = PD.CD_GA13
      AND AGY.POSITIONTITLE  IN ('ORGANISATION','UNIT')
      AND ((agy_ldr.NAME = 'SGT'
        || agy.POS_GA2
      AND agy.NAME LIKE 'SGY%')
      )
      --AND DISTRICT.POS_GA3 = AGY.POS_GA3
      --AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
      --AND district.positiontitle     IN ('DISTRICT','BR_DISTRICT')
);

       /* End of above Insert to capture CASE COUNT Of Brunei Records which doesn't CreditType = 'FYP' coming from 'Case_Count */
       Log('2 Records inserted into AIA_RPT_PRD_DIST_WRI_NEW 2 are '||SQL%ROWCOUNT);
      COMMIT;

----End by Suresh
--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_WRI' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_WRI' , 'AIA_RPT_PRD_DIST_WRI_NEW 3'  , 'Processing', 'insert into AIA_RPT_PRD_DIST_WRI_NEW');


------=======ADD LOG

--- Adding Unassigned RC Logic below.
/*Commented by Suresh
INSERT
    /*+ Append */
/*  INTO AIA_RPT_PRD_DIST_WRI_NEW NOLOGGING
( SELECT
        V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
        V_PROCESSINGUNITNAME PUNAME,
        decode(PD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') BUNAME,
        PD.businessunitmap BUMAP,
        V_CALENDARSEQ CALENDARSEQ,
        V_CALENDARNAME CALENDARNAME,
        V_PERIODSEQ PERIODKEY,
        V_PERIODNAME periodname,
        --PAD_AGT.POSITIONSEQ POSITIONSEQ,
        PAD_AGT.MANAGERSEQ,
        PAD_AGT.POSITIONSEQ POSITIONSEQ,
        pad_agt.NAME,
/*      SUBSTR(DISTRICT.NAME,4) DISTRICT_CODE,
        district.firstname
        || ' '
        || district.lastname DM_NAme,
        district.POS_GA2 DIST_LEADER_CODE,
        district.POS_GA7 DIST_LEADER_NAME,
        district.POS_GA11 DIST_LEAER_TITLE,
        DISTRICT.POS_GA4 DIST_LEADER_CLASS,*/
  /*      SUBSTR(pos_dis.NAME,4) DISTRICT_CODE,
      par_dis.firstname || ' ' || par_dis.lastname DM_NAME,
      pos_dis.genericattribute2 DIST_LEADER_CODE,
      pos_dis.genericattribute7 DIST_LEADER_NAME,
      pos_dis.genericattribute11 DIST_LEAER_TITLE,
      pos_dis.genericattribute4 DIST_LEADER_CLASS,
      PD.CD_GA13 UNIT_CODE,
      agy.firstname
      || ' '
      || agy.lastname AGENCY,
      agy_ldr.POS_GA2 UNIT_LEADER_CODE,
      agy_ldr.POS_GA7 UNIT_LEADER_NAME,
      DECODE(agy_ldr.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy_ldr.POS_GA11) UNIT_LEAER_TITLE,
      agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
      (
      CASE
        WHEN agy.POSITIONTITLE IN ('AGENCY','DISTRICT','BR_AGENCY','BR_DISTRICT')
        THEN agy.AGY_TERMINATIONDATE
      END)DISSOLVED_DATE,
      SUBSTR(pad_agt.NAME,4) AGT_CODE,
      (pad_agt.firstname
      || pad_agt.lastname) NAME,
      DECODE(pad_agt.positiontitle, 'FSC_NON_PROCESS', 'FSC', pad_agt.positiontitle) ROLE,
      pad_agt.POS_GA4 CLASS,
      pad_agt.HIREDATE CONTRACT_DATE,
      PAD.AGY_APPOINTMENT_DATE APPOINTMENT_DATE,
      pad_agt.AGY_TERMINATIONDATE TERMINATION_DATE,
      (
      CASE
      WHEN pad_agt.PT_GA1 = '00'
        THEN 'INFORCE'
        WHEN pad_agt.PT_GA1 IN ('50','51','52','55','56')
        THEN 'TERMINATED'
        WHEN pad_agt.PT_GA1 = '13'
        THEN 'TERMINATED'
        WHEN pad_agt.PT_GA1 IN ('60','61')
        THEN 'TERMINATED'
        WHEN pad_agt.PT_GA1 = '70'
        THEN 'TERMINATED'
      END) AGENT_STATUS,
      0 LIFE_FYP,
      0 PA_FYP,
      0 LIFE_CASE,
      0 PA_CASE,
      0 LIFE_FYC_API_SSC,
      0 PA_FYC,
      0 CS_FYC,
      0 LIFE_2YEAR,
      0 LIFE_3YEAR,
      0 LIFE_4YEAR,
      0 PA_RC,
      0 CS_RC,
      0 CS_PL,
      CASE
        WHEN PD.CD_GA4 = 'PAY2'
        AND PD.CD_GA2  IN ('LF','HS')
        THEN PD.CD_VALUE
        ELSE 0
      END UNASSIGNED_LIFE_2YEAR,
      CASE
        WHEN PD.CD_GA4 = 'PAY3'
        AND PD.CD_GA2   IN ('LF','HS')
        THEN PD.CD_VALUE
        ELSE 0
      END UNASSIGNED_LIFE_3YEAR,
      CASE
        WHEN PD.CD_GA4 IN ('PAY4','PAY5','PAY6')
        AND PD.CD_GA2   IN ('LF','HS')
        THEN PD.CD_VALUE
        ELSE 0
      END UNASSIGNED_LIFE_4YEAR,
      CASE
        WHEN PD.CD_GA2 IN ('PA')--,'VL')
        THEN PD.CD_VALUE
        ELSE 0
      END UNASSIGNED_PA_RC,
      0 LIFE_FYP_YTD,
      0 PA_FYP_YTD,
      0 LIFE_CASE_YTD,
      0 PA_CASE_YTD,
      0 LIFE_FYC_API_SSC_YTD,
      0 PA_FYC_YTD,
      0 CS_FYC_YTD,
      0 LIFE_2YEAR_YTD,
      0 LIFE_3YEAR_YTD,
      0 LIFE_4YEAR_YTD,
      0 PA_RC_YTD,
      0 CS_RC_YTD,
      0 CS_PL_YTD,
      0 UNASSIGNED_LIFE_2YEAR_YTD,
      0 UNASSIGNED_LIFE_3YEAR_YTD,
      0 UNASSIGNED_LIFE_4YEAR_YTD,
      0 UNASSIGNED_PA_RC_YTD
      FROM
      AIA_RPT_PRD_TEMP_PADIM PAD,
      AIA_RPT_PRD_TEMP_PADIM pad_agt,
      AIA_RPT_PRD_TEMP_PADIM agy,
      AIA_RPT_PRD_TEMP_PADIM AGY_LDR,
      --AIA_RPT_PRD_TEMP_PADIM DISTRICT,
      AIA_RPT_PRD_TEMP_VALUES PD
      --for writing agency postion info
      inner join cs_position pos_agy
      on pos_agy.name = trim('SGY'||PD.CD_GA12)
      AND pos_agy.effectivestartdate <= PD.CD_GD2 --policy issue date
      AND pos_agy.effectiveenddate   > PD.CD_GD2  --policy issue date
      AND pos_agy.removedate =DT_REMOVEDATE
      --for writing district postion info
      inner join cs_position pos_dis
      on pos_dis.name=trim('SGY'||pos_agy.genericattribute3)
      AND pos_dis.effectivestartdate <= PD.CD_GD2 --policy issue date
      AND pos_dis.effectiveenddate   > PD.CD_GD2  --policy issue date
      AND pos_dis.removedate =DT_REMOVEDATE
      --for writing district participant info
      inner join cs_participant par_dis
      on par_dis.PAYEESEQ = pos_dis.PAYEESEQ
      AND par_dis.effectivestartdate < V_ENDDATE
      AND par_dis.effectiveenddate   >  V_ENDDATE-1
      AND par_dis.removedate = DT_REMOVEDATE
 WHERE PD.NEW_PRD_IND=1 AND PD.PRD_TYPE = 'DIRECT'
   --add in version 5
     AND ( PD.CD_GA2  IN ('PA','HS','CS','PL','CL','VL') or nvl(PD.submitdate,TO_DATE('2016/01/01','yyyy/mm/dd')) < TO_DATE('2017/01/01','yyyy/mm/dd'))
    --end to add
    --AND PD.CD_POSITIONSEQ = PAD.POSITIONSEQ
    AND (
    (PD.businessunitmap = 1 and PD.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (PD.businessunitmap <> 1 and PD.CD_POSITIONSEQ = PAD.POSITIONSEQ)
    )
    --AND PD.CD_CREDITTYPE  IN ('ORYC', 'RYC','Case_Count')
    AND PD.CD_CREDITTYPE IN ('RYC', 'ORYC',
                           'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE',
                           'ORYC_W', 'ORYC_W_DUPLICATE', 'ORYC_WC_DUPLICATE')
    AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD','AM','FSD','FSM','BR_DM','BR_FSC','BR_UM')
    AND PAD.POS_GA4 NOT IN ('45')--,'48')
    AND AGY.POS_GA1 = PD.CD_GA13
    --AND SUBSTR(PAD.NAME,4)  = PD.TRANS_GA11
    --AND SUBSTR(PAD_AGT.NAME,4) = PD.TRANS_GA10
    AND SUBSTR(PAD.NAME,4)  = PD.CD_GA13
    AND SUBSTR(PAD_AGT.NAME,4) = PD.CD_GA12 and SUBSTR(PAD_AGT.NAME,3,1)='T'
    AND AGY.POSITIONTITLE      IN ('AGENCY','DISTRICT','BR_AGENCY','BR_DISTRICT')
    AND ((agy_ldr.NAME = 'SGT'
      || agy.POS_GA2
    AND agy.NAME LIKE 'SGY%')
    OR (agy_ldr.NAME = 'BRT'
      || agy.POS_GA2
    AND agy.NAME LIKE 'BRY%'))
    --AND DISTRICT.POS_GA3 = AGY.POS_GA3
    --AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
    --AND district.positiontitle     IN ('DISTRICT','BR_DISTRICT')
    AND PD.CD_GA2   IN ('LF','PA','HS','VL')
   -- AND PD.CD_GA4     IN ('PAY2','PAY3','PAY4','PAY5','PAY6')
);*/

--Added by Suresh
  EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_DIST_WRI_NEW_HELP';

insert /*+APPEND*/ into AIA_RPT_PRD_DIST_WRI_NEW_HELP NOLOGGING
(select /*+   */
        decode(PD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') BUNAME,
        PD.businessunitmap BUMAP,
        PD.CD_GA13 UNIT_CODE,
        0 LIFE_FYC_API_SSC,
        0 LIFE_FYC_RP,
        0 LIFE_FYC_NONILP,
        0 LIFE_FYC_ILP,
        0 LIFE_FYC_TOPUP,
        0 PA_FYC,
        0 CS_FYC,
        0 LIFE_2YEAR,
        0 LIFE_3YEAR,
        0 LIFE_4YEAR,
        0 PA_RC,
        SUBSTR(help1.NAME,4) DISTRICT_CODE,
        help1.firstname || ' ' || help1.lastname DM_NAME,
        help1.genericattribute2 DIST_LEADER_CODE,
        help1.genericattribute7 DIST_LEADER_NAME,
        help1.genericattribute11 DIST_LEAER_TITLE,
        help1.genericattribute4 DIST_LEADER_CLASS,
        PD.businessunitmap as businessunitmap,
        PD.CD_GA12 as CD_GA12,
        PD.CD_POSITIONSEQ as CD_POSITIONSEQ,

        PD.CD_GA13 as CD_GA13,
        0 LIFE_CASE,
        0 PA_CASE,

        CASE
          WHEN PD.CD_GA4 = 'PAY2'
          AND PD.CD_GA2  IN ('LF','HS')
          THEN PD.CD_VALUE
          ELSE 0
        END UNASSIGNED_LIFE_2YEAR,
        CASE
          WHEN PD.CD_GA4 = 'PAY3'
          AND PD.CD_GA2   IN ('LF','HS')
          THEN PD.CD_VALUE
          ELSE 0
        END UNASSIGNED_LIFE_3YEAR,
        CASE
          WHEN PD.CD_GA4 IN ('PAY4','PAY5','PAY6')
          AND PD.CD_GA2   IN ('LF','HS')
          THEN PD.CD_VALUE
          ELSE 0
        END UNASSIGNED_LIFE_4YEAR,
        CASE
          WHEN PD.CD_GA2 IN ('PA')--,'VL')
          THEN PD.CD_VALUE
          ELSE 0
        END UNASSIGNED_PA_RC

        from
        AIA_RPT_PRD_TEMP_VALUES PD
        --for writing agency postion info
        inner join cs_position pos_agy
        on pos_agy.name = trim('SGY'||PD.CD_GA12)
        AND pos_agy.effectivestartdate <= PD.CD_GD2 --policy issue date
        AND pos_agy.effectiveenddate   > PD.CD_GD2  --policy issue date
        AND pos_agy.removedate =DT_REMOVEDATE
        --for writing district postion info
        inner join AIA_RPT_PRD_DIST_WRI_NEW_HELP1 help1
           ON help1.name=trim('SGY'||pos_agy.genericattribute3)
           AND help1.effstartdt <= PD.CD_GD2 --policy issue date
           AND help1.effenddt   > PD.CD_GD2  --policy issue date
 WHERE PD.NEW_PIB_IND=1 AND PD.PIB_TYPE = 'DIRECT'
     --add in version 5
       --AND ( PD.CD_GA2  IN ('PA','HS','CS','PL','CL','VL') or nvl(PD.submitdate,TO_DATE('2016/01/01','yyyy/mm/dd')) < TO_DATE('2017/01/01','yyyy/mm/dd'))
       --version 9 add GI product
       AND ( PD.CD_GA2  IN ('PA','HS','CS','PL','CL','VL','GI') or nvl(PD.submitdate,TO_DATE('2016/01/01','yyyy/mm/dd')) < TO_DATE('2017/01/01','yyyy/mm/dd'))
      --end to add
      --AND PD.CD_POSITIONSEQ = PAD.POSITIONSEQ
       AND PD.CD_CREDITTYPE IN ('RYC', 'ORYC',
                             'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE',
                             'ORYC_W', 'ORYC_W_DUPLICATE', 'ORYC_WC_DUPLICATE')
         AND PD.CD_GA2   IN ('LF','PA','HS','VL')
);

Log('3 Records inserted into f_NEW_HELP 3 are '||SQL%ROWCOUNT);
COMMIT;

   BEGIN
        DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME => '"AIASEXT"' ,
                                        TABNAME => '"AIA_RPT_PRD_DIST_WRI_NEW_HELP"' ,
                                        ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE);
END;

--- Adding Unassigned RC Logic below.
INSERT
    /*+ Append */
  INTO AIA_RPT_PRD_DIST_WRI_NEW NOLOGGING
( SELECT
        V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
        V_PROCESSINGUNITNAME PUNAME,
        PD.BUNAME,
        PD.BUMAP,
        V_CALENDARSEQ CALENDARSEQ,
        V_CALENDARNAME CALENDARNAME,
        V_PERIODSEQ PERIODKEY,
        V_PERIODNAME periodname,
        --PAD_AGT.POSITIONSEQ POSITIONSEQ,
        PAD_AGT.MANAGERSEQ,
        PAD_AGT.POSITIONSEQ POSITIONSEQ,
        pad_agt.NAME,
/*      SUBSTR(DISTRICT.NAME,4) DISTRICT_CODE,
        district.firstname
        || ' '
        || district.lastname DM_NAme,
        district.POS_GA2 DIST_LEADER_CODE,
        district.POS_GA7 DIST_LEADER_NAME,
        district.POS_GA11 DIST_LEAER_TITLE,
        DISTRICT.POS_GA4 DIST_LEADER_CLASS,*/
        PD.DISTRICT_CODE,
        PD.DM_NAME,
        PD.DIST_LEADER_CODE,
        PD.DIST_LEADER_NAME,
        PD.DIST_LEAER_TITLE,
        PD.DIST_LEADER_CLASS,
        PD.UNIT_CODE,
        agy.firstname || ' ' || agy.lastname AGENCY,
        agy_ldr.POS_GA2 UNIT_LEADER_CODE,
        agy_ldr.POS_GA7 UNIT_LEADER_NAME,
        DECODE(agy_ldr.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy_ldr.POS_GA11) UNIT_LEAER_TITLE,
        agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
        (
        CASE
          WHEN agy.POSITIONTITLE IN ('ORGANISATION','UNIT')
          THEN agy.AGY_TERMINATIONDATE
        END)DISSOLVED_DATE,
        SUBSTR(pad_agt.NAME,4) AGT_CODE,
        (pad_agt.firstname
        || pad_agt.lastname) NAME,
        DECODE(pad_agt.positiontitle, 'FSC_NON_PROCESS', 'FSC', pad_agt.positiontitle) ROLE,
        pad_agt.POS_GA4 CLASS,
        pad_agt.HIREDATE CONTRACT_DATE,
        PAD.AGY_APPOINTMENT_DATE APPOINTMENT_DATE,
        pad_agt.AGY_TERMINATIONDATE TERMINATION_DATE,
        (
        CASE
        WHEN pad_agt.PT_GA1 = '00'
          THEN 'INFORCE'
          WHEN pad_agt.PT_GA1 IN ('50','51','52','55','56')
          THEN 'TERMINATED'
          WHEN pad_agt.PT_GA1 = '13'
          THEN 'TERMINATED'
          WHEN pad_agt.PT_GA1 IN ('60','61')
          THEN 'TERMINATED'
          WHEN pad_agt.PT_GA1 = '70'
          THEN 'TERMINATED'
        END) AGENT_STATUS,
        0 LIFE_FYP,
        0 PA_FYP,
        0 LIFE_CASE,
        0 PA_CASE,
        0 LIFE_FYC_API_SSC,
        0 LIFE_FYC_RP,
        0 LIFE_FYC_NONILP,
        0 LIFE_FYC_ILP,
        0 LIFE_FYC_TOPUP,
        0 PA_FYC,
        0 CS_FYC,
        0 LIFE_2YEAR,
        0 LIFE_3YEAR,
        0 LIFE_4YEAR,
        0 PA_RC,
        0 CS_RC,
        0 CS_PL,
        PD.UNASSIGNED_LIFE_2YEAR,
        PD.UNASSIGNED_LIFE_3YEAR,
        PD.UNASSIGNED_LIFE_4YEAR,
        PD.UNASSIGNED_PA_RC,
        0 LIFE_FYP_YTD,
        0 PA_FYP_YTD,
        0 LIFE_CASE_YTD,
        0 PA_CASE_YTD,
        0 LIFE_FYC_API_SSC_YTD,
        0 LIFE_FYC_RP_YTD,
        0 LIFE_FYC_NONILP_YTD,
        0 LIFE_FYC_ILP_YTD,
        0 LIFE_FYC_TOPUP_YTD,
        0 PA_FYC_YTD,
        0 CS_FYC_YTD,
        0 LIFE_2YEAR_YTD,
        0 LIFE_3YEAR_YTD,
        0 LIFE_4YEAR_YTD,
        0 PA_RC_YTD,
        0 CS_RC_YTD,
        0 CS_PL_YTD,
        0 UNASSIGNED_LIFE_2YEAR_YTD,
        0 UNASSIGNED_LIFE_3YEAR_YTD,
        0 UNASSIGNED_LIFE_4YEAR_YTD,
        0 UNASSIGNED_PA_RC_YTD
        FROM
        AIA_RPT_PRD_TEMP_PADIM PAD,
        AIA_RPT_PRD_TEMP_PADIM pad_agt,
        AIA_RPT_PRD_TEMP_PADIM agy,
        AIA_RPT_PRD_TEMP_PADIM AGY_LDR,
        --AIA_RPT_PRD_TEMP_PADIM DISTRICT,
        AIA_RPT_PRD_DIST_WRI_NEW_HELP PD
WHERE (
      (PD.businessunitmap = 1 and PD.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (PD.businessunitmap <> 1 and PD.CD_POSITIONSEQ = PAD.POSITIONSEQ)
      )
      AND PAD.POSITIONTITLE IN ('AD','DIR','ED','FC','SD')
      AND PAD.POS_GA4 NOT IN ('45')--,'48')
      AND AGY.POS_GA1 = PD.CD_GA13
      --AND SUBSTR(PAD.NAME,4)  = PD.TRANS_GA11
      --AND SUBSTR(PAD_AGT.NAME,4) = PD.TRANS_GA10
      AND SUBSTR(PAD.NAME,4)  = PD.CD_GA13
      AND SUBSTR(PAD_AGT.NAME,4) = PD.CD_GA12 and SUBSTR(PAD_AGT.NAME,3,1)='T'
      AND AGY.POSITIONTITLE      IN ('ORGANISATION','UNIT')
      AND ((agy_ldr.NAME = 'SGT'
        || agy.POS_GA2
      AND agy.NAME LIKE 'SGY%'))
);
--End by Suresh

       Log('3 Records inserted into AIA_RPT_PRD_DIST_WRI_NEW 3 are '||SQL%ROWCOUNT);
COMMIT;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_WRI' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_WRI' , 'AIA_RPT_PRD_DIST_WRI_TMP'  , 'Processing', 'insert into AIA_RPT_PRD_DIST_WRI_TMP');


------=======ADD LOG
----------------line 622
DELETE
  FROM AIA_RPT_PRD_DIST_WRI_TMP WHERE PERIODNAME = V_PERIODNAME ;

  COMMIT;


INSERT /*+ Append */
  INTO AIA_RPT_PRD_DIST_WRI_TMP NOLOGGING
 SELECT MAX(PROCESSINGUNITSEQ) PROCESSINGUNITSEQ,
    MAX(PUNAME) PUNAME,
    BUNAME,
    BUMAP,
    MAX(CALENDARSEQ) CALENDARSEQ,
    MAX(CALENDARNAME) CALENDARNAME,
    MAX(PERIODKEY) PERIODKEY,
    MAX(PERIODNAME) PERIODNAME,
    --MAX(POSITIONSEQ) POSITIONSEQ,
    MAX(MANAGERSEQ) MANAGERSEQ,
    MAX(POSITIONSEQ) POSITIONSEQ,
    MAX(POSITIONNAME),
    DISTRICT_CODE,
    MAX(DM_NAME) DM_NAME,
    MAX(DIST_LEADER_CODE) DIST_LEADER_CODE,
    MAX(DIST_LEADER_NAME) DIST_LEADER_NAME,
    MAX(DIST_LEAER_TITLE) DIST_LEAER_TITLE,
    MAX(DIST_LEADER_CLASS) DIST_LEADER_CLASS,
    UNIT_CODE UNIT_CODE,
    MAX(AGENCY) AGENCY,
    MAX(UNIT_LEADER_CODE) UNIT_LEADER_CODE ,
    MAX(UNIT_LEADER_NAME) UNIT_LEADER_NAME,
    MAX(UNIT_LEAER_TITLE) UNIT_LEAER_TITLE,
    MAX(UNIT_LEADER_CLASS) UNIT_LEADER_CLASS,
    MAX(DISSOLVED_DATE) DISSOLVED_DATE,
    AGT_CODE AGT_CODE,
    MAX(NAME) NAME,
    MAX(ROLE) ROLE ,
    MAX(CLASS) CLASS,
    MAX(CONTRACT_DATE) CONTRACT_DATE,
    MAX(APPOINTMENT_DATE) APPOINTMENT_DATE,
    MAX(TERMINATION_DATE) TERMINATION_DATE,
    MAX(AGENT_STATUS) AGENT_STATUS,
    SUM(LIFE_FYP) LIFE_FYP,
    SUM(PA_FYP) PA_FYP,
    SUM(LIFE_CASE) LIFE_CASE,
    SUM(PA_CASE) PA_CASE,
    SUM(LIFE_FYC_API_SSC) LIFE_FYC_API_SSC,
    SUM(LIFE_FYC_RP) LIFE_FYC_RP,
    SUM(LIFE_FYC_NONILP) LIFE_FYC_NONILP,
    SUM(LIFE_FYC_ILP) LIFE_FYC_ILP,
    SUM(LIFE_FYC_TOPUP) LIFE_FYC_TOPUP,
    SUM(PA_FYC) PA_FYC,
    SUM(CS_FYC) CS_FYC,
    SUM(LIFE_2YEAR) LIFE_2YEAR,
    SUM(LIFE_3YEAR) LIFE_3YEAR,
    SUM(LIFE_4YEAR) LIFE_4YEAR,
    SUM(PA_RC) PA_RC,
    SUM(CS_RC) CS_RC,
    SUM(CS_PL) CS_PL,
    SUM(UNASSIGNED_LIFE_2YEAR) UNASSIGNED_LIFE_2YEAR,
    SUM(UNASSIGNED_LIFE_3YEAR) UNASSIGNED_LIFE_3YEAR,
    SUM(UNASSIGNED_LIFE_4YEAR) UNASSIGNED_LIFE_4YEAR,
    SUM(UNASSIGNED_PA_RC) UNASSIGNED_PA_RC,
   SUM(LIFE_FYP+LIFE_FYP_YTD) LIFE_FYP_YTD,
    SUM(PA_FYP+PA_FYP_YTD) PA_FYP_YTD,
    SUM(LIFE_CASE+LIFE_CASE_YTD) LIFE_CASE_YTD,
    SUM(PA_CASE+PA_CASE_YTD) PA_CASE_YTD,
    SUM(LIFE_FYC_API_SSC+LIFE_FYC_API_SSC_YTD) LIFE_FYC_API_SSC_YTD,
    SUM(LIFE_FYC_RP+LIFE_FYC_RP_YTD) LIFE_FYC_RP_YTD,
    SUM(LIFE_FYC_NONILP+LIFE_FYC_NONILP_YTD) LIFE_FYC_NONILP_YTD,
    SUM(LIFE_FYC_ILP+LIFE_FYC_ILP_YTD) LIFE_FYC_ILP_YTD,
    SUM(LIFE_FYC_TOPUP+LIFE_FYC_TOPUP_YTD) LIFE_FYC_TOPUP_YTD,
    SUM(PA_FYC+PA_FYC_YTD) PA_FYC_YTD,
    SUM(CS_FYC+CS_FYC_YTD) CS_FYC_YTD,
    SUM(LIFE_2YEAR+LIFE_2YEAR_YTD) LIFE_2YEAR_YTD,
    SUM(LIFE_3YEAR+LIFE_3YEAR_YTD) LIFE_3YEAR_YTD,
    SUM(LIFE_4YEAR+LIFE_4YEAR_YTD) LIFE_4YEAR_YTD,
    SUM(PA_RC+PA_RC_YTD) PA_RC_YTD,
    SUM(CS_RC+CS_RC_YTD) CS_RC_YTD,
    SUM(CS_PL+CS_PL_YTD) CS_PL_YTD,
    SUM(UNASSIGNED_LIFE_2YEAR+UNASSIGNED_LIFE_2YEAR_YTD) UNASSIGNED_LIFE_2YEAR_YTD,
    SUM(UNASSIGNED_LIFE_3YEAR+UNASSIGNED_LIFE_3YEAR_YTD) UNASSIGNED_LIFE_3YEAR_YTD,
    SUM(UNASSIGNED_LIFE_4YEAR+UNASSIGNED_LIFE_4YEAR_YTD) UNASSIGNED_LIFE_4YEAR_YTD,
    SUM(UNASSIGNED_PA_RC+UNASSIGNED_PA_RC_YTD) UNASSIGNED_PA_RC_YTD
  FROM
    (SELECT PROCESSINGUNITSEQ,
      PUNAME,
      BUNAME,
      BUMAP,
      CALENDARSEQ,
      CALENDARNAME,
      PERIODKEY,
      periodname,
      --POSITIONSEQ,
      MANAGERSEQ,
      POSITIONSEQ,
      POSITIONNAME,
      DISTRICT_CODE,
      DM_NAme,
      dIST_LEADER_CODE,
      DIST_LEADER_NAME,
      DIST_LEAER_TITLE,
      DIST_LEADER_CLASS,
      UNIT_CODE,
      AGENCY,
      UNIT_LEADER_CODE,
      UNIT_LEADER_NAME,
      UNIT_LEAER_TITLE,
      UNIT_LEADER_CLASS,
      DISSOLVED_DATE,
      AGT_CODE,
      NAME,
      ROLE,
      CLASS,
      CONTRACT_DATE,
      APPOINTMENT_DATE,
      TERMINATION_DATE,
      AGENT_STATUS,
      LIFE_FYP,
      PA_FYP,
       LIFE_CASE,
      PA_CASE,
      LIFE_FYC_API_SSC,
      LIFE_FYC_RP,
 LIFE_FYC_NONILP,
LIFE_FYC_ILP,
 LIFE_FYC_TOPUP,
      PA_FYC,
      CS_FYC,
      LIFE_2YEAR,
      LIFE_3YEAR,
      LIFE_4YEAR,
      PA_RC,
       CS_RC,
      CS_PL,
      UNASSIGNED_LIFE_2YEAR,
       UNASSIGNED_LIFE_3YEAR,
      UNASSIGNED_LIFE_4YEAR,
      UNASSIGNED_PA_RC ,
      LIFE_FYP_YTD ,
      PA_FYP_YTD ,
       LIFE_CASE_YTD ,
      PA_CASE_YTD ,
      LIFE_FYC_API_SSC_YTD ,
      LIFE_FYC_RP_YTD,
 LIFE_FYC_NONILP_YTD,
 LIFE_FYC_ILP_YTD,
 LIFE_FYC_TOPUP_YTD ,
      PA_FYC_YTD ,
      CS_FYC_YTD ,
      LIFE_2YEAR_YTD ,
       LIFE_3YEAR_YTD ,
      LIFE_4YEAR_YTD ,
      PA_RC_YTD ,
      CS_RC_YTD ,
      CS_PL_YTD ,
      UNASSIGNED_LIFE_2YEAR_YTD ,
     UNASSIGNED_LIFE_3YEAR_YTD ,
      UNASSIGNED_LIFE_4YEAR_YTD ,
      UNASSIGNED_PA_RC_YTD
    FROM AIA_RPT_PRD_DIST_WRI_NEW
    UNION ALL
    SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
      PUNAME,
      BUNAME,
      BUMAP,
      CALENDARSEQ,
      CALENDARNAME,
        V_PERIODSEQ PERIODKEY,
    V_PERIODNAME periodname,
      --POSITIONSEQ,
      MANAGERSEQ,
      POSITIONSEQ,
      POSITIONNAME,
      DISTRICT_CODE,
      DM_NAme,
      dIST_LEADER_CODE,
      DIST_LEADER_NAME,
      DIST_LEAER_TITLE,
      DIST_LEADER_CLASS,
      UNIT_CODE,
      AGENCY,
      UNIT_LEADER_CODE,
      UNIT_LEADER_NAME,
      UNIT_LEAER_TITLE,
      UNIT_LEADER_CLASS,
      to_date(DISSOLVED_DATE, 'mm/dd/yyyy') DISSOLVED_DATE,
      AGT_CODE,
      NAME,
      ROLE,
      CLASS,
      CONTRACT_DATE,
      APPOINTMENT_DATE,
      TERMINATION_DATE,
      AGENT_STATUS,
      0 LIFE_FYP,
      0 PA_FYP,
      0 LIFE_CASE,
      0 PA_CASE,
      0 LIFE_FYC_API_SSC,
      0 LIFE_FYC_RP,
      0 LIFE_FYC_NONILP,
      0 LIFE_FYC_ILP,
      0 LIFE_FYC_TOPUP,
      0 PA_FYC,
      0 CS_FYC,
      0 LIFE_2YEAR,
      0 LIFE_3YEAR,
      0 LIFE_4YEAR,
      0 PA_RC,
      0 CS_RC,
      0 CS_PL,
      0 UNASSIGNED_LIFE_2YEAR,
      0 UNASSIGNED_LIFE_3YEAR,
      0 UNASSIGNED_LIFE_4YEAR,
      0 UNASSIGNED_PA_RC,
      LIFE_FYP LIFE_FYP_YTD,
      PA_FYP PA_FYP_YTD,
      LIFE_CASE LIFE_CASE_YTD,
      PA_CASE PA_CASE_YTD,
      LIFE_FYC_API_SSC LIFE_FYC_API_SSC_YTD,
      LIFE_FYC_RP LIFE_FYC_RP_YTD,
    LIFE_FYC_NONILP LIFE_FYC_NONILP_YTD,
    LIFE_FYC_ILP LIFE_FYC_ILP_YTD,
    LIFE_FYC_TOPUP LIFE_FYC_TOPUP_YTD ,
      PA_FYC PA_FYC_YTD,
      CS_FYC CS_FYC_YTD,
      LIFE_2YEAR LIFE_2YEAR_YTD,
      LIFE_3YEAR LIFE_3YEAR_YTD,
      LIFE_4YEAR LIFE_4YEAR_YTD,
      PA_RC PA_RC_YTD,
      CS_RC CS_RC_YTD,
      CS_PL CS_PL_YTD,
      UNASSIGNED_LIFE_2YEAR UNASSIGNED_LIFE_2YEAR_YTD,
      UNASSIGNED_LIFE_3YEAR UNASSIGNED_LIFE_3YEAR_YTD,
      UNASSIGNED_LIFE_4YEAR UNASSIGNED_LIFE_4YEAR_YTD,
      UNASSIGNED_PA_RC UNASSIGNED_PA_RC_YTD
    FROM AIA_RPT_PRD_DIST_WRI_NEW
    WHERE periodname IN
    (SELECT name
       FROM CS_PERIOD
      WHERE periodtypeseq = V_PERIODTYPESEQ
      AND calendarseq = V_CALENDARSEQ
      AND V_PERIODNAME  LIKE 'Jan%'
      AND REMOVEDATE=to_date('01/01/2200','mm/dd/yyyy')
      AND STARTDATE >=trunc(V_STARTDATE,'year')
      and ENDDATE<= V_ENDDATE
      )

    UNION ALL
    --- Adding YTD Logic.
    SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
      PUNAME,
      BUNAME,
      BUMAP,
      CALENDARSEQ,
      CALENDARNAME,
       V_PERIODSEQ PERIODKEY,
    V_PERIODNAME periodname,
      --POSITIONSEQ,
      MANAGERSEQ,
      POSITIONSEQ,
      POSITIONNAME,
      DISTRICT_CODE,
      DM_NAme,
      dIST_LEADER_CODE,
      DIST_LEADER_NAME,
      DIST_LEAER_TITLE,
      DIST_LEADER_CLASS,
      UNIT_CODE,
      AGENCY,
      UNIT_LEADER_CODE,
      UNIT_LEADER_NAME,
      UNIT_LEAER_TITLE,
      UNIT_LEADER_CLASS,
      to_date(DISSOLVED_DATE, 'mm/dd/yyyy') DISSOLVED_DATE,
      AGT_CODE,
      NAME,
      ROLE,
      CLASS,
      CONTRACT_DATE,
      APPOINTMENT_DATE,
      TERMINATION_DATE,
      AGENT_STATUS,
      0 LIFE_FYP,
      0 PA_FYP,
      0 LIFE_CASE,
      0 PA_CASE,
      0 LIFE_FYC_API_SSC,
      0 LIFE_FYC_RP,
      0 LIFE_FYC_NONILP,
      0 LIFE_FYC_ILP,
      0 LIFE_FYC_TOPUP,
      0 PA_FYC,
      0 CS_FYC,
      0 LIFE_2YEAR,
      0 LIFE_3YEAR,
      0 LIFE_4YEAR,
      0 PA_RC,
      0 CS_RC,
      0 CS_PL,
      0 UNASSIGNED_LIFE_2YEAR,
      0 UNASSIGNED_LIFE_3YEAR,
      0 UNASSIGNED_LIFE_4YEAR,
      0 UNASSIGNED_PA_RC,
      LIFE_FYP_YTD,
      PA_FYP_YTD,
      LIFE_CASE_YTD,
      PA_CASE_YTD,
      LIFE_FYC_API_SSC_YTD,
      LIFE_FYC_RP_YTD,
    LIFE_FYC_NONILP_YTD,
    LIFE_FYC_ILP_YTD,
     LIFE_FYC_TOPUP_YTD ,
      PA_FYC_YTD,
      CS_FYC_YTD,
      LIFE_2YEAR_YTD,
      LIFE_3YEAR_YTD,
      LIFE_4YEAR_YTD,
      PA_RC_YTD,
      CS_RC_YTD,
      CS_PL_YTD,
      UNASSIGNED_LIFE_2YEAR_YTD,
      UNASSIGNED_LIFE_3YEAR_YTD,
      UNASSIGNED_LIFE_4YEAR_YTD,
      UNASSIGNED_PA_RC_YTD
    FROM AIA_RPT_PRD_DIST_WRI
    WHERE
    periodname IN
    (SELECT name
     FROM CS_PERIOD
      WHERE periodtypeseq = V_PERIODTYPESEQ
      AND calendarseq = V_CALENDARSEQ
      AND V_PERIODNAME NOT LIKE 'Jan%'
      AND REMOVEDATE=to_date('01/01/2200','mm/dd/yyyy')
      AND STARTDATE >=trunc(V_STARTDATE,'year')
      and ENDDATE<= V_ENDDATE
      )
      )
  GROUP BY DISTRICT_CODE,
  BUNAME,
  BUMAP,
  POSITIONNAME,
  UNIT_CODE,
  AGT_CODE   ;

--   SELECT MAX(PROCESSINGUNITSEQ) PROCESSINGUNITSEQ,
--    MAX(PUNAME) PUNAME,
--    BUNAME,
--    BUMAP,
--    MAX(CALENDARSEQ) CALENDARSEQ,
--    MAX(CALENDARNAME) CALENDARNAME,
--    MAX(PERIODKEY) PERIODKEY,
--    MAX(PERIODNAME) PERIODNAME,
--    --MAX(POSITIONSEQ) POSITIONSEQ,
--    MAX(MANAGERSEQ) MANAGERSEQ,
--    MAX(POSITIONSEQ) POSITIONSEQ,
--    POSITIONNAME,
--    DISTRICT_CODE,
--    MAX(DM_NAME) DM_NAME,
--    MAX(DIST_LEADER_CODE) DIST_LEADER_CODE,
--    MAX(DIST_LEADER_NAME) DIST_LEADER_NAME,
--    MAX(DIST_LEAER_TITLE) DIST_LEAER_TITLE,
--    MAX(DIST_LEADER_CLASS) DIST_LEADER_CLASS,
--    UNIT_CODE UNIT_CODE,
--    MAX(AGENCY) AGENCY,
--    MAX(UNIT_LEADER_CODE) UNIT_LEADER_CODE ,
--    MAX(UNIT_LEADER_NAME) UNIT_LEADER_NAME,
--    MAX(UNIT_LEAER_TITLE) UNIT_LEAER_TITLE,
--    MAX(UNIT_LEADER_CLASS) UNIT_LEADER_CLASS,
--    MAX(DISSOLVED_DATE) DISSOLVED_DATE,
--    AGT_CODE AGT_CODE,
--    MAX(NAME) NAME,
--    MAX(ROLE) ROLE ,
--    MAX(CLASS) CLASS,
--    MAX(CONTRACT_DATE) CONTRACT_DATE,
--    MAX(APPOINTMENT_DATE) APPOINTMENT_DATE,
--    MAX(TERMINATION_DATE) TERMINATION_DATE,
--    MAX(AGENT_STATUS) AGENT_STATUS,
--    SUM(LIFE_FYP) LIFE_FYP,
--    SUM(PA_FYP) PA_FYP,
--    SUM(LIFE_CASE) LIFE_CASE,
--    SUM(PA_CASE) PA_CASE,
--    SUM(LIFE_FYC_API_SSC) LIFE_FYC_API_SSC,
--    SUM(LIFE_FYC_RP) LIFE_FYC_RP,
--    SUM(LIFE_FYC_NONILP) LIFE_FYC_NONILP,
--    SUM(LIFE_FYC_ILP) LIFE_FYC_ILP,
--    SUM(LIFE_FYC_TOPUP) LIFE_FYC_TOPUP,
--    SUM(PA_FYC) PA_FYC,
--    SUM(CS_FYC) CS_FYC,
--    SUM(LIFE_2YEAR) LIFE_2YEAR,
--    SUM(LIFE_3YEAR) LIFE_3YEAR,
--    SUM(LIFE_4YEAR) LIFE_4YEAR,
--    SUM(PA_RC) PA_RC,
--    SUM(CS_RC) CS_RC,
--    SUM(CS_PL) CS_PL,
--    SUM(UNASSIGNED_LIFE_2YEAR) UNASSIGNED_LIFE_2YEAR,
--    SUM(UNASSIGNED_LIFE_3YEAR) UNASSIGNED_LIFE_3YEAR,
--    SUM(UNASSIGNED_LIFE_4YEAR) UNASSIGNED_LIFE_4YEAR,
--    SUM(UNASSIGNED_PA_RC) UNASSIGNED_PA_RC,
--    SUM(LIFE_FYP_YTD) LIFE_FYP_YTD,
--    SUM(PA_FYP_YTD) PA_FYP_YTD,
--    SUM(LIFE_CASE_YTD) LIFE_CASE_YTD,
--    SUM(PA_CASE_YTD) PA_CASE_YTD,
--    SUM(LIFE_FYC_API_SSC_YTD) LIFE_FYC_API_SSC_YTD,
--    SUM(LIFE_FYC_RP_YTD) LIFE_FYC_RP_YTD,
--    SUM(LIFE_FYC_NONILP_YTD) LIFE_FYC_NONILP_YTD,
--    SUM(LIFE_FYC_ILP_YTD) LIFE_FYC_ILP_YTD,
--    SUM(LIFE_FYC_TOPUP_YTD) LIFE_FYC_TOPUP_YTD,
--    SUM(PA_FYC_YTD) PA_FYC_YTD,
--    SUM(CS_FYC_YTD) CS_FYC_YTD,
--    SUM(LIFE_2YEAR_YTD) LIFE_2YEAR_YTD,
--    SUM(LIFE_3YEAR_YTD) LIFE_3YEAR_YTD,
--    SUM(LIFE_4YEAR_YTD) LIFE_4YEAR_YTD,
--    SUM(PA_RC_YTD) PA_RC_YTD,
--    SUM(CS_RC_YTD) CS_RC_YTD,
--    SUM(CS_PL_YTD) CS_PL_YTD,
--    SUM(UNASSIGNED_LIFE_2YEAR_YTD) UNASSIGNED_LIFE_2YEAR_YTD,
--    SUM(UNASSIGNED_LIFE_3YEAR_YTD) UNASSIGNED_LIFE_3YEAR_YTD,
--    SUM(UNASSIGNED_LIFE_4YEAR_YTD) UNASSIGNED_LIFE_4YEAR_YTD,
--    SUM(UNASSIGNED_PA_RC_YTD) UNASSIGNED_PA_RC_YTD
--  FROM
--    (SELECT PROCESSINGUNITSEQ,
--      PUNAME,
--      BUNAME,
--      BUMAP,
--      CALENDARSEQ,
--      CALENDARNAME,
--      PERIODKEY,
--      periodname,
--      --POSITIONSEQ,
--      MANAGERSEQ,
--      POSITIONSEQ,
--      POSITIONNAME,
--      DISTRICT_CODE,
--      DM_NAme,
--      dIST_LEADER_CODE,
--      DIST_LEADER_NAME,
--      DIST_LEAER_TITLE,
--      DIST_LEADER_CLASS,
--      UNIT_CODE,
--      AGENCY,
--      UNIT_LEADER_CODE,
--      UNIT_LEADER_NAME,
--      UNIT_LEAER_TITLE,
--      UNIT_LEADER_CLASS,
--      DISSOLVED_DATE,
--      AGT_CODE,
--      NAME,
--      ROLE,
--      CLASS,
--      CONTRACT_DATE,
--      APPOINTMENT_DATE,
--      TERMINATION_DATE,
--      AGENT_STATUS,
--      LIFE_FYP,
--      PA_FYP,
--       LIFE_CASE,
--      PA_CASE,
--      LIFE_FYC_API_SSC,
--      LIFE_FYC_RP,
-- LIFE_FYC_NONILP,
--LIFE_FYC_ILP,
-- LIFE_FYC_TOPUP,
--      PA_FYC,
--      CS_FYC,
--      LIFE_2YEAR,
--      LIFE_3YEAR,
--      LIFE_4YEAR,
--      PA_RC,
--       CS_RC,
--      CS_PL,
--      UNASSIGNED_LIFE_2YEAR,
--       UNASSIGNED_LIFE_3YEAR,
--      UNASSIGNED_LIFE_4YEAR,
--      UNASSIGNED_PA_RC ,
--      LIFE_FYP_YTD ,
--      PA_FYP_YTD ,
--       LIFE_CASE_YTD ,
--      PA_CASE_YTD ,
--      LIFE_FYC_API_SSC_YTD ,
--      LIFE_FYC_RP_YTD,
-- LIFE_FYC_NONILP_YTD,
-- LIFE_FYC_ILP_YTD,
-- LIFE_FYC_TOPUP_YTD ,
--      PA_FYC_YTD ,
--      CS_FYC_YTD ,
--      LIFE_2YEAR_YTD ,
--       LIFE_3YEAR_YTD ,
--      LIFE_4YEAR_YTD ,
--      PA_RC_YTD ,
--      CS_RC_YTD ,
--      CS_PL_YTD ,
--      UNASSIGNED_LIFE_2YEAR_YTD ,
--     UNASSIGNED_LIFE_3YEAR_YTD ,
--      UNASSIGNED_LIFE_4YEAR_YTD ,
--      UNASSIGNED_PA_RC_YTD
--    FROM AIA_RPT_PRD_DIST_WRI_NEW
--
--    UNION ALL
--    SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
--      PUNAME,
--      BUNAME,
--      BUMAP,
--      CALENDARSEQ,
--      CALENDARNAME,
--      V_PERIODSEQ PERIODKEY,
--      V_PERIODNAME periodname,
--      --POSITIONSEQ,
--      MANAGERSEQ,
--      POSITIONSEQ,
--      POSITIONNAME,
--      DISTRICT_CODE,
--      DM_NAme,
--      dIST_LEADER_CODE,
--      DIST_LEADER_NAME,
--      DIST_LEAER_TITLE,
--      DIST_LEADER_CLASS,
--      UNIT_CODE,
--      AGENCY,
--      UNIT_LEADER_CODE,
--      UNIT_LEADER_NAME,
--      UNIT_LEAER_TITLE,
--      UNIT_LEADER_CLASS,
--      to_date(DISSOLVED_DATE, 'mm/dd/yyyy') DISSOLVED_DATE,
--      AGT_CODE,
--      NAME,
--      ROLE,
--      CLASS,
--      CONTRACT_DATE,
--      APPOINTMENT_DATE,
--      TERMINATION_DATE,
--      AGENT_STATUS,
--      0 LIFE_FYP,
--      0 PA_FYP,
--      0 LIFE_CASE,
--      0 PA_CASE,
--      0 LIFE_FYC_API_SSC,
--      0 LIFE_FYC_RP,
--      0 LIFE_FYC_NONILP,
--      0 LIFE_FYC_ILP,
--      0 LIFE_FYC_TOPUP,
--      0 PA_FYC,
--      0 CS_FYC,
--      0 LIFE_2YEAR,
--      0 LIFE_3YEAR,
--      0 LIFE_4YEAR,
--      0 PA_RC,
--      0 CS_RC,
--      0 CS_PL,
--      0 UNASSIGNED_LIFE_2YEAR,
--      0 UNASSIGNED_LIFE_3YEAR,
--      0 UNASSIGNED_LIFE_4YEAR,
--      0 UNASSIGNED_PA_RC,
--      LIFE_FYP LIFE_FYP_YTD,
--      PA_FYP PA_FYP_YTD,
--      LIFE_CASE LIFE_CASE_YTD,
--      PA_CASE PA_CASE_YTD,
--      LIFE_FYC_API_SSC LIFE_FYC_API_SSC_YTD,
--      LIFE_FYC_RP LIFE_FYC_RP_YTD,
--    LIFE_FYC_NONILP LIFE_FYC_NONILP_YTD,
--    LIFE_FYC_ILP LIFE_FYC_ILP_YTD,
--    LIFE_FYC_TOPUP LIFE_FYC_TOPUP_YTD ,
--      PA_FYC PA_FYC_YTD,
--      CS_FYC CS_FYC_YTD,
--      LIFE_2YEAR LIFE_2YEAR_YTD,
--      LIFE_3YEAR LIFE_3YEAR_YTD,
--      LIFE_4YEAR LIFE_4YEAR_YTD,
--      PA_RC PA_RC_YTD,
--      CS_RC CS_RC_YTD,
--      CS_PL CS_PL_YTD,
--      UNASSIGNED_LIFE_2YEAR UNASSIGNED_LIFE_2YEAR_YTD,
--      UNASSIGNED_LIFE_3YEAR UNASSIGNED_LIFE_3YEAR_YTD,
--      UNASSIGNED_LIFE_4YEAR UNASSIGNED_LIFE_4YEAR_YTD,
--      UNASSIGNED_PA_RC UNASSIGNED_PA_RC_YTD
--    FROM AIA_RPT_PRD_DIST_WRI_NEW
--    WHERE periodname IN
--    (SELECT name
--      FROM CS_PERIOD
--      WHERE periodtypeseq = V_PERIODTYPESEQ
--      AND calendarseq = V_CALENDARSEQ
--      AND V_PERIODNAME LIKE 'Jan%'
--      AND REMOVEDATE=to_date('01/01/2200','mm/dd/yyyy')
--      AND STARTDATE >=trunc(V_STARTDATE,'year')
--      and ENDDATE<= V_ENDDATE
--      )
--
--    UNION ALL
--    --- Adding YTD Logic.
--    SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
--      PUNAME,
--      BUNAME,
--      BUMAP,
--      CALENDARSEQ,
--      CALENDARNAME,
--      V_PERIODSEQ PERIODKEY,
--      V_PERIODNAME periodname,
--      --POSITIONSEQ,
--      MANAGERSEQ,
--      POSITIONSEQ,
--      POSITIONNAME,
--      DISTRICT_CODE,
--      DM_NAme,
--      dIST_LEADER_CODE,
--      DIST_LEADER_NAME,
--      DIST_LEAER_TITLE,
--      DIST_LEADER_CLASS,
--      UNIT_CODE,
--      AGENCY,
--      UNIT_LEADER_CODE,
--      UNIT_LEADER_NAME,
--      UNIT_LEAER_TITLE,
--      UNIT_LEADER_CLASS,
--      to_date(DISSOLVED_DATE, 'mm/dd/yyyy') DISSOLVED_DATE,
--      AGT_CODE,
--      NAME,
--      ROLE,
--      CLASS,
--      CONTRACT_DATE,
--      APPOINTMENT_DATE,
--      TERMINATION_DATE,
--      AGENT_STATUS,
--      0 LIFE_FYP,
--      0 PA_FYP,
--      0 LIFE_CASE,
--      0 PA_CASE,
--      0 LIFE_FYC_API_SSC,
--      0 LIFE_FYC_RP,
--      0 LIFE_FYC_NONILP,
--      0 LIFE_FYC_ILP,
--      0 LIFE_FYC_TOPUP,
--      0 PA_FYC,
--      0 CS_FYC,
--      0 LIFE_2YEAR,
--      0 LIFE_3YEAR,
--      0 LIFE_4YEAR,
--      0 PA_RC,
--      0 CS_RC,
--      0 CS_PL,
--      0 UNASSIGNED_LIFE_2YEAR,
--      0 UNASSIGNED_LIFE_3YEAR,
--      0 UNASSIGNED_LIFE_4YEAR,
--      0 UNASSIGNED_PA_RC,
--      LIFE_FYP_YTD,
--      PA_FYP_YTD,
--      LIFE_CASE_YTD,
--      PA_CASE_YTD,
--      LIFE_FYC_API_SSC_YTD,
--      LIFE_FYC_RP_YTD,
--    LIFE_FYC_NONILP_YTD,
--    LIFE_FYC_ILP_YTD,
--     LIFE_FYC_TOPUP_YTD ,
--      PA_FYC_YTD,
--      CS_FYC_YTD,
--      LIFE_2YEAR_YTD,
--      LIFE_3YEAR_YTD,
--      LIFE_4YEAR_YTD,
--      PA_RC_YTD,
--      CS_RC_YTD,
--      CS_PL_YTD,
--      UNASSIGNED_LIFE_2YEAR_YTD,
--      UNASSIGNED_LIFE_3YEAR_YTD,
--      UNASSIGNED_LIFE_4YEAR_YTD,
--      UNASSIGNED_PA_RC_YTD
--    FROM AIA_RPT_PRD_DIST_WRI
--    WHERE periodname IN
--    (SELECT name
--      FROM CS_PERIOD
--      WHERE periodtypeseq = V_PERIODTYPESEQ
--      AND calendarseq = V_CALENDARSEQ
--      AND V_PERIODNAME NOT LIKE 'Jan%'
--      AND REMOVEDATE=to_date('01/01/2200','mm/dd/yyyy')
--      AND STARTDATE >=trunc(V_STARTDATE,'year')
--      and ENDDATE<= V_ENDDATE
--      )
--    )
--  GROUP BY DISTRICT_CODE,
--  BUNAME,
--  BUMAP,
--  POSITIONNAME,
--  UNIT_CODE,
--  AGT_CODE   ;

       Log('4 Records inserted into AIA_RPT_PRD_DIST_WRI_TMP TEST 2 are '||SQL%ROWCOUNT);
COMMIT;

------=======ADD LOG

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_WRI' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_WRI' , 'BUILD HIERARCHY LIST'  , 'Processing', '');


-----------start from line 880
EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_AGENCY_LIST';

INSERT /*+ Append  */
INTO AIA_RPT_AGENCY_LIST NOLOGGING
  (AGY_CODE,
   AGY_PARTICIPANTID,
   AGY_NAME,
   AGY_POSITIONTITLE,
   AGY_POS_GA4,
   AGY_POS_GA2,
   AGY_POS_GA3,
   AGY_TERMINATIONDATE,
   AGY_APPOINTMENT_DATE,
   AGY_POS_GA9,
   NEW_DISTRICT_CODE,
   AGY_BUNAME)
  (select CHILDPAD.POS_GA1 AGY_CODE,
          CHILDPAD.name AGY_PARTICIPANTID,
          CHILDPAD.FIRSTNAME || ' ' || CHILDPAD.LASTNAME AGY_NAME,
          DECODE(CHILDPAD.POSITIONTITLE,
                 'FSC_NON_PROCESS',
                 'FSC',
                 'BR_FSC_NON_PROCESS',
                 'FSC',
                 'AM_FSC_NON_PROCESS',
                 'FSC',
                 'AM_NON_PROCESS',
                 'AM',
                 'FSD_NON_PROCESS',
                 'FSD',
                 'FSAD_NON_PROCESS',
                 'FSAD',
                 'BR_Staff',
                 'Staff',
                 'BR_DM',
                 'DM',
                 'BR_DM_NON_PROCESS',
                 'DM',
                 'BR_FSC',
                 'FSC',
                 'BR_Staff_NON_PROCESS',
                 'Staff',
                 'BR_UM',
                 'UM',
                 'BR_UM_NON_PROCESS',
                 'UM',
                 'Staff_NON_PROCESS',
                 'Staff',
                 CHILDPAD.POSITIONTITLE) AGY_POSITIONTITLE,
          CHILDPAD.POS_GA4 AGY_POS_GA4,
          CHILDPAD.POS_GA2 AGY_POS_GA2,
          CHILDPAD.POS_GA3 AGY_POS_GA3,
          CHILDPAD.AGY_TERMINATIONDATE AGY_TERMINATIONDATE,
          CHILDPAD.AGY_APPOINTMENT_DATE AGY_APPOINTMENT_DATE,
          CHILDPAD.POS_GA9 AGY_POS_GA9,
          PARENTPAD.POS_GA3 NEW_DISTRICT_CODE,
          decode(CHILDPAD.businessunitmap,
                 v_BUSINESSUNITMAP1,
                 'SGPAFA',
                 v_BUSINESSUNITMAP2,
                 'BRUAGY') AGY_BUNAME
     from AIA_RPT_PRD_TEMP_PADIM CHILDPAD
     INNER JOIN cs_positionrelation PR
     ON CHILDPAD.positionseq = PR.childpositionseq
     AND PR.positionrelationtypeseq =
          (select datatypeseq
             from cs_positionrelationtype
            where name = 'PBA_Roll'
              and removedate = V_EOT)
     INNER JOIN AIA_RPT_PRD_TEMP_PADIM PARENTPAD
     ON PARENTPAD.positionseq = PR.parentpositionseq
     WHERE CHILDPAD.POSITIONTITLE IN ('ORGANISATION','UNIT')
     AND CHILDPAD.POS_GA9 = 'Y'

     UNION

     select PAD.POS_GA1 AGY_CODE,
          PAD.name AGY_PARTICIPANTID,
          PAD.FIRSTNAME || ' ' || PAD.LASTNAME AGY_NAME,
          DECODE(PAD.POSITIONTITLE,
                 'FSC_NON_PROCESS',
                 'FSC',
                 'BR_FSC_NON_PROCESS',
                 'FSC',
                 'AM_FSC_NON_PROCESS',
                 'FSC',
                 'AM_NON_PROCESS',
                 'AM',
                 'FSD_NON_PROCESS',
                 'FSD',
                 'FSAD_NON_PROCESS',
                 'FSAD',
                 'BR_Staff',
                 'Staff',
                 'BR_DM',
                 'DM',
                 'BR_DM_NON_PROCESS',
                 'DM',
                 'BR_FSC',
                 'FSC',
                 'BR_Staff_NON_PROCESS',
                 'Staff',
                 'BR_UM',
                 'UM',
                 'BR_UM_NON_PROCESS',
                 'UM',
                 'Staff_NON_PROCESS',
                 'Staff',
                 PAD.POSITIONTITLE) AGY_POSITIONTITLE,
          PAD.POS_GA4 AGY_POS_GA4,
          PAD.POS_GA2 AGY_POS_GA2,
          PAD.POS_GA3 AGY_POS_GA3,
          PAD.AGY_TERMINATIONDATE AGY_TERMINATIONDATE,
          PAD.AGY_APPOINTMENT_DATE AGY_APPOINTMENT_DATE,
          PAD.POS_GA9 AGY_POS_GA9,
          PAD.POS_GA3 NEW_DISTRICT_CODE,
          decode(PAD.businessunitmap,
                 v_BUSINESSUNITMAP1,
                 'SGPAFA',
                 v_BUSINESSUNITMAP2,
                 'BRUAGY') AGY_BUNAME

     from AIA_RPT_PRD_TEMP_PADIM PAD
     WHERE (PAD.POS_GA9 = 'N'
       OR PAD.POS_GA9  IS NULL)
     AND PAD.POSITIONTITLE  IN ('ORGANISATION','UNIT')
);
Log('5 Records inserted into AIA_RPT_AGENCY_LIST are '||SQL%ROWCOUNT);
COMMIT;



------=======ADD LOG1

EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_AGENT_LIST';
INSERT  /*+ Append  */
INTO AIA_RPT_AGENT_LIST NOLOGGING
(fsc_id ,
FSC_NAME ,
FSC_TITLE,
FSC_CLASS ,
FSC_HIREDATE ,
FSC_TERMINATION_DATE ,
FSC_STATUS ,
FSC_ASSIGNED_DATE ,
FSC_APPOINTMENT_DATE ,
FSC_BUNAME
)
(SELECT SUBSTR(NAME, 4)fsc_id ,
PAD.firstname || ' ' || PAD.LASTNAME FSC_NAME ,
DECODE(PAD.POSITIONTITLE,
    'FSC_NON_PROCESS', 'FSC', 'BR_FSC_NON_PROCESS',
    'FSC', 'AM_FSC_NON_PROCESS', 'FSC', 'AM_NON_PROCESS', 'AM' ,
    'FSD_NON_PROCESS', 'FSD',
    'FSAD_NON_PROCESS', 'FSAD',
    'BR_Staff' , 'Staff',
    'BR_DM', 'DM',
    'BR_DM_NON_PROCESS', 'DM',
    'BR_FSC' , 'FSC',
    'BR_Staff_NON_PROCESS', 'Staff',
    'BR_UM','UM',
    'BR_UM_NON_PROCESS', 'UM',
    'Staff_NON_PROCESS', 'Staff',
     PAD.POSITIONTITLE) FSC_TITLE,
PAD.POS_GA4 FSC_CLASS ,
PAD.hiredate FSC_HIREDATE ,
PAD.AGY_TERMINATIONDATE FSC_TERMINATION_DATE ,
(CASE WHEN PAD.PT_GA1 = '00' THEN 'INFORCE'
        WHEN PAD.PT_GA1 IN ('50','51','52','55','56') THEN 'TERMINATED'
          WHEN PAD.PT_GA1 = '13' then  'TERMINATED'
          WHEN PAD.PT_GA1 IN ('60','61') then  'TERMINATED'
          WHEN PAD.PT_GA1 = '70' THEN  'TERMINATED'
        END) FSC_STATUS ,
PAD.ASSIGNED_DATE FSC_ASSIGNED_DATE ,
PAD.AGY_APPOINTMENT_DATE FSC_APPOINTMENT_DATE ,
decode(PAD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') FSC_BUNAME
FROM AIA_RPT_PRD_TEMP_PADIM PAD
WHERE PAD.POSITIONTITLE NOT IN ('ORGANISATION','UNIT')

);

Log('6 Records inserted into AIA_RPT_AGENCY_LIST 2 are '||SQL%ROWCOUNT);
COMMIT;

------=======ADD LOG

EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_AGENCY_LEADER';

INSERT  /*+ Append  */
INTO AIA_RPT_AGENCY_LEADER NOLOGGING
(AGY_LDR_CODE ,
AGY_LDR_PARTICIPANTID,
AGY_LDR_NAME ,
AGY_LDR_TITLE,
AGY_LDR_POS_GA2 ,
AGY_LDR_POS_GA4,
AGY_LDR_POS_GA3,
AGY_LDR_BUNAME
)
(SELECT SUBSTR(NAME, 4)AGY_LDR_CODE ,
NAME AGY_LDR_PARTICIPANTID,
PAD.firstname || ' ' || PAD.LASTNAME AGY_LDR_NAME ,
DECODE(PAD.POSITIONTITLE,
    'FSC_NON_PROCESS', 'FSC', 'BR_FSC_NON_PROCESS',
    'FSC', 'AM_FSC_NON_PROCESS', 'FSC', 'AM_NON_PROCESS', 'AM' ,
    'FSD_NON_PROCESS', 'FSD',
    'FSAD_NON_PROCESS', 'FSAD',
    'BR_Staff' , 'Staff',
    'BR_DM', 'DM',
    'BR_DM_NON_PROCESS', 'DM',
    'BR_FSC' , 'FSC',
    'BR_Staff_NON_PROCESS', 'Staff',
    'BR_UM','UM',
    'BR_UM_NON_PROCESS', 'UM',
    'Staff_NON_PROCESS', 'Staff',
     PAD.POSITIONTITLE) AGY_LDR_TITLE,
     PAD.POS_GA2 AGY_LDR_POS_GA2,
     PAD.POS_GA4 AGY_LDR_POS_GA4 ,
     PAD.POS_GA3 AGY_LDR_POS_GA3,
decode(PAD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') AGY_LDR_BUNAME
FROM  AIA_RPT_PRD_TEMP_PADIM PAD

);

Log('7 Records inserted into AIA_RPT_AGENCY_LEADER 1 are '||SQL%ROWCOUNT);

COMMIT;

------=======ADD LOG



EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_DISTRICT_LIST';

INSERT  /*+ Append  */
INTO AIA_RPT_DISTRICT_LIST NOLOGGING
(DIST_CODE ,
DIST_PARTICIPANTID,
DIST_NAME ,
DIST_TITLE,
DIST_POS_GA2 ,
DIST_POS_GA4,
DIST_POS_GA3,
DIST_BUNAME
)
(SELECT SUBSTR(NAME, 4)DIST_CODE ,
        NAME DIST_PARTICIPANTID,
        PAD.firstname || ' ' || PAD.LASTNAME DIST_NAME ,
        PAD.POSITIONTITLE DIST_TITLE,
        PAD.POS_GA2 DIST_POS_GA2,
        PAD.POS_GA4 DIST_POS_GA4 ,
        PAD.POS_GA3 DIST_POS_GA3,
        decode(PAD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') DIST_BUNAME
FROM AIA_RPT_PRD_TEMP_PADIM PAD
        WHERE PAD.POSITIONTITLE NOT LIKE '%FSC%'
);

Log('8 Records inserted into AIA_RPT_DISTRICT_LIST are '||SQL%ROWCOUNT);
COMMIT;

------=======ADD LOG

EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_DISTRICT_LEADER';

INSERT  /*+ Append  */
INTO AIA_RPT_DISTRICT_LEADER NOLOGGING
(DIST_LDR_CODE ,
DIST_LDR_PARTICIPANTID,
DIST_LDR_NAME ,
DIST_LDR_TITLE,
DIST_LDR_POS_GA2 ,
DIST_LDR_POS_GA4,
DIST_LDR_POS_GA3,
DIST_LDR_BUNAME
)
   (SELECT SUBSTR(NAME, 4)DIST_LDR_CODE ,
           NAME DIST_LDR_PARTICIPANTID,
           PAD.firstname || ' ' || PAD.LASTNAME DIST_LDR_NAME ,
           DECODE(PAD.POSITIONTITLE,
    'FSC_NON_PROCESS', 'FSC', 'BR_FSC_NON_PROCESS',
    'FSC', 'AM_FSC_NON_PROCESS', 'FSC', 'AM_NON_PROCESS', 'AM' ,
    'FSD_NON_PROCESS', 'FSD',
    'FSAD_NON_PROCESS', 'FSAD',
    'BR_Staff' , 'Staff',
    'BR_DM', 'DM',
    'BR_DM_NON_PROCESS', 'DM',
    'BR_FSC' , 'FSC',
    'BR_Staff_NON_PROCESS', 'Staff',
    'BR_UM','UM',
    'BR_UM_NON_PROCESS', 'UM',
    'Staff_NON_PROCESS', 'Staff',
     PAD.POSITIONTITLE) DIST_LDR_TITLE,
     PAD.POS_GA2 DIST_LDR_POS_GA2,
     PAD.POS_GA4 DIST_LDR_POS_GA4 ,
     PAD.POS_GA3 DIST_LDR_POS_GA3,
     decode(PAD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') DIST_LDR_BUNAME
     FROM  AIA_RPT_PRD_TEMP_PADIM PAD
     WHERE PAD.POSITIONTITLE NOT LIKE '%FSC%'
);

Log('9 Records inserted into AIA_RPT_DISTRICT_LEADER are '||SQL%ROWCOUNT);
COMMIT;

pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_WRI' , 'Finish', '');
--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_WRI' , 'AIA_RPT_PRD_DIST_WRI'  , 'Processing', 'insert into AIA_RPT_PRD_DIST_WRI');


DELETE FROM AIA_RPT_PRD_DIST_WRI WHERE PERIODNAME = V_PERIODNAME ;
COMMIT;


   INSERT
    /*+Append   */
  INTO Aia_Rpt_PRD_Dist_WRI NOLOGGING
    (
      PROCESSINGUNITSEQ,
      PUNAME,
      BUNAME,
      BUMAP,
      calendarseq,
      calendarname,
      PERIODKEY,
      Periodname,
      --POSITIONSEQ,
      MANAGERSEQ,
      POSITIONSEQ,
      POSITIONNAME,
      DISTRICT_CODE,
      DM_NAme,
      dIST_LEADER_CODE,
      DIST_LEADER_NAME,
      DIST_LEAER_TITLE,
      DIST_LEADER_CLASS,
      UNIT_CODE,
      AGENCY,
      UNIT_LEADER_CODE,
      UNIT_LEADER_NAME,
      UNIT_LEAER_TITLE,
      UNIT_LEADER_CLASS,
      DISSOLVED_DATE,
      AGT_CODE,
      NAME,
      ROLE,
      CLASS,
      CONTRACT_DATE,
      APPOINTMENT_DATE,
      TERMINATION_DATE,
      AGENT_STATUS,
      LIFE_FYP,
      PA_FYP,
      LIFE_CASE,
      PA_CASE,
      LIFE_FYC_API_SSC,
      LIFE_FYC_RP,
 LIFE_FYC_NONILP,
 LIFE_FYC_ILP,
 LIFE_FYC_TOPUP ,
      PA_FYC,
      CS_FYC,
      LIFE_2YEAR,
      LIFE_3YEAR,
      LIFE_4YEAR,
      Pa_Rc,
      CS_RC,
      CS_PL,
      UNASSIGNED_LIFE_2YEAR,
      UNASSIGNED_LIFE_3YEAR,
      UNASSIGNED_LIFE_4YEAR,
      UNASSIGNED_PA_RC,
      LIFE_FYP_YTD,
      PA_FYP_YTD,
      LIFE_CASE_YTD,
      PA_CASE_YTD,
      LIFE_FYC_API_SSC_YTD,
      LIFE_FYC_RP_YTD,
 LIFE_FYC_NONILP_YTD,
 LIFE_FYC_ILP_YTD,
 LIFE_FYC_TOPUP_YTD ,
      PA_FYC_YTD,
      CS_FYC_YTD,
      Life_2year_Ytd,
      LIFE_3YEAR_YTD,
      LIFE_4YEAR_YTD,
      PA_RC_YTD,
      CS_RC_YTD,
      CS_PL_YTD,
      UNASSIGNED_LIFE_2YEAR_YTD,
      UNASSIGNED_LIFE_3YEAR_YTD,
      Unassigned_Life_4year_Ytd,
      UNASSIGNED_PA_RC_YTD
    )
SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
  V_PROCESSINGUNITNAME PUNAME,
  --AGT.FSC_BUNAME BUNAME,
  tmp.buname,
  TMP.BUMAP,
  V_CALENDARSEQ calendarseq,
  V_CALENDARNAME calendarname,
  V_PERIODSEQ PERIODKEY,
  V_PERIODNAME Periodname,
  --NULL POSITIONSEQ,
  TMP.MANAGERSEQ MANAGERSEQ,
  TMP.POSITIONSEQ POSITIONSEQ,
  TMP.POSITIONNAME POSITIONNAME,
/*  DISTRICT.DIST_CODE,
  DISTRICT.DIST_NAME DM_NAME,
  district_ldr.DIST_LDR_CODE DIST_LEADER_CODE,
  district_ldr.DIST_LDR_NAME DIST_LEADER_NAME,
  district_ldr.DIST_LDR_TITLE DIST_LEAER_TITLE,
  DISTRICT_LDR.DIST_LDR_POS_GA4 DIST_LEADER_CLASS,*/
  tmp.district_code as DIST_CODE,
  tmp.dm_name as DM_NAME,
  tmp.dist_leader_code as DIST_LEADER_CODE,
  tmp.dist_leader_name as DIST_LEADER_NAME,
  tmp.dist_leaer_title as DIST_LEAER_TITLE,
  tmp.dist_leader_class as DIST_LEADER_CLASS,

/*  AGY.AGY_CODE UNIT_CODE,
  AGY_NAME AGENCY,
  AGY_LDR.AGY_LDR_CODE UNIT_LEADER_CODE,
  AGY_LDR.AGY_LDR_NAME  UNIT_LEADER_NAME,
  AGY_LDR.AGY_LDR_TITLE  UNIT_LEAER_TITLE,
  AGY_LDR.AGY_LDR_POS_GA4  UNIT_LEADER_CLASS,
  TO_CHAR(AGY.AGY_TERMINATIONDATE, 'MM/DD/YYYY') DISSOLVED_DATE,*/
  tmp.unit_code,
  tmp.agency,
  tmp.unit_leader_code,
  tmp.unit_leader_name,
  tmp.unit_leaer_title,
  tmp.unit_leader_class,
  TO_CHAR(tmp.dissolved_date,'MM/DD/YYYY'),

  TMP.AGT_CODE,
/*  AGT.FSC_NAME NAME,
  AGT.FSC_TITLE ROLE,
  AGT.FSC_CLASS CLASS,
  AGT.FSC_HIREDATE CONTRACT_DATE,
  AGT.FSC_APPOINTMENT_DATE APPOINTMENT_DATE,
  AGT.FSC_TERMINATION_DATE TERMINATION_DATE,
  AGT.FSC_STATUS AGENT_STATUS,*/
  tmp.name,
  tmp.role,
  tmp.class,
  tmp.contract_date,
  tmp.appointment_date,
  tmp.termination_date,
  tmp.agent_status,

  SUM(LIFE_FYP),
  SUM(PA_FYP),
  SUM( LIFE_CASE),
  SUM(PA_CASE),
  SUM(LIFE_FYC_API_SSC),
  SUM(LIFE_FYC_RP),
 SUM(LIFE_FYC_NONILP),
 SUM(LIFE_FYC_ILP),
 SUM(LIFE_FYC_TOPUP),
  SUM( PA_FYC),
  SUM( CS_FYC),
  SUM(LIFE_2YEAR),
  SUM( LIFE_3YEAR),
  SUM(LIFE_4YEAR),
  SUM( PA_RC),
  SUM( CS_RC),
  SUM( CS_PL),
  SUM(UNASSIGNED_LIFE_2YEAR),
  SUM( UNASSIGNED_LIFE_3YEAR),
  SUM(UNASSIGNED_LIFE_4YEAR),
  SUM(UNASSIGNED_PA_RC),
  SUM( LIFE_FYP_YTD),
  SUM( PA_FYP_YTD),
  SUM( LIFE_CASE_YTD),
  SUM( PA_CASE_YTD),
  SUM( LIFE_FYC_API_SSC_YTD),
  SUM(LIFE_FYC_RP_YTD),
 SUM(LIFE_FYC_NONILP_YTD),
 SUM(LIFE_FYC_ILP_YTD),
 SUM(LIFE_FYC_TOPUP_YTD),
 SUM( PA_FYC_YTD),
 SUM( CS_FYC_YTD),
 SUM( LIFE_2YEAR_YTD),
 SUM( LIFE_3YEAR_YTD),
 SUM( LIFE_4YEAR_YTD),
 SUM( PA_RC_YTD),
 SUM( CS_RC_YTD),
 SUM( CS_PL_YTD),
 SUM( UNASSIGNED_LIFE_2YEAR_YTD),
 SUM( UNASSIGNED_LIFE_3YEAR_YTD),
 SUM( UNASSIGNED_LIFE_4YEAR_YTD),
 SUM( UNASSIGNED_PA_RC_YTD)
--
--  SUM(LIFE_FYP)               + SUM( LIFE_FYP_YTD),
--  SUM(PA_FYP)                 + SUM( PA_FYP_YTD),
--  SUM( LIFE_CASE)             + SUM( LIFE_CASE_YTD),
--  SUM(PA_CASE)                + SUM( PA_CASE_YTD),
--  SUM(LIFE_FYC_API_SSC)       + SUM( LIFE_FYC_API_SSC_YTD),
--  SUM(LIFE_FYC_RP)  + SUM(LIFE_FYC_RP_YTD),
-- SUM(LIFE_FYC_NONILP) + SUM(LIFE_FYC_NONILP_YTD),
-- SUM(LIFE_FYC_ILP) + SUM(LIFE_FYC_ILP_YTD),
-- SUM(LIFE_FYC_TOPUP) + SUM(LIFE_FYC_TOPUP_YTD),
--  SUM( PA_FYC)                + SUM( PA_FYC_YTD),
--  SUM( CS_FYC)                + SUM( CS_FYC_YTD),
--  SUM(LIFE_2YEAR)             + SUM( LIFE_2YEAR_YTD),
--  SUM( LIFE_3YEAR)            + SUM( LIFE_3YEAR_YTD),
--  SUM(LIFE_4YEAR)             + SUM( LIFE_4YEAR_YTD),
--  SUM( PA_RC)                 + SUM( PA_RC_YTD),
--  SUM( CS_RC)                 + SUM( CS_RC_YTD),
--  SUM( CS_PL)                 + SUM( CS_PL_YTD),
--  SUM(UNASSIGNED_LIFE_2YEAR)  + SUM( UNASSIGNED_LIFE_2YEAR_YTD),
--  SUM( UNASSIGNED_LIFE_3YEAR) + SUM( UNASSIGNED_LIFE_3YEAR_YTD),
--  SUM(UNASSIGNED_LIFE_4YEAR)  + SUM( UNASSIGNED_LIFE_4YEAR_YTD),
--  SUM(UNASSIGNED_PA_RC)       + SUM( UNASSIGNED_PA_RC_YTD)
FROM AIA_RPT_PRD_DIST_WRI_TMP tmp
/*  inner join AIA_RPT_AGENT_LIST agt
  on TMP.AGT_CODE = AGT.FSC_ID
  and TMP.BUNAME = AGT.FSC_BUNAME
  inner join AIA_RPT_AGENCY_LIST agy
  on TMP.UNIT_CODE = AGY.AGY_CODE
  and AGY.AGY_BUNAME = AGT.FSC_BUNAME
  inner join AIA_RPT_AGENCY_LEADER agy_ldr
  on (AGY_LDR.AGY_LDR_PARTICIPANTID = 'SGT' || AGY.AGY_POS_GA2 AND AGY.AGY_PARTICIPANTID LIKE 'SGY%')
  and AGY_LDR.AGY_LDR_CODE      = AGY.AGY_POS_GA2
  inner join AIA_RPT_DISTRICT_LIST district
  on DISTRICT.DIST_BUNAME = AGY.AGY_BUNAME
  and DISTRICT.DIST_CODE = agy.NEW_DISTRICT_CODE
  left join AIA_RPT_DISTRICT_LEADER DISTRICT_LDR
  on AGT.FSC_BUNAME = DISTRICT_LDR.DIST_LDR_BUNAME
  AND DISTRICT_LDR.DIST_LDR_CODE = DISTRICT.DIST_POS_GA2*/
WHERE TMP.PERIODKEY          = V_PERIODSEQ
AND TMP.BUNAME = 'SGPAFA'
GROUP BY
  --AGT.FSC_BUNAME,
  tmp.buname,
  TMP.BUMAP,
/*  DISTRICT.DIST_CODE,
  DISTRICT.DIST_NAME ,
  district_ldr.DIST_LDR_CODE ,
  district_ldr.DIST_LDR_NAME ,
  district_ldr.DIST_LDR_TITLE ,
  DISTRICT_LDR.DIST_LDR_POS_GA4 ,*/
    TMP.MANAGERSEQ,
  TMP.POSITIONSEQ,
  TMP.POSITIONNAME,
  tmp.district_code,
  tmp.dm_name,
  tmp.dist_leader_code,
  tmp.dist_leader_name,
  tmp.dist_leaer_title,
  tmp.dist_leader_class,
/*  AGY.AGY_CODE ,
  AGY.AGY_NAME ,
  AGY_LDR.AGY_LDR_CODE ,
  AGY_LDR.AGY_LDR_NAME  ,
  AGY_LDR.AGY_LDR_TITLE  ,
  AGY_LDR.AGY_LDR_POS_GA4  ,
  TO_CHAR(AGY.AGY_TERMINATIONDATE, 'MM/DD/YYYY') ,*/
  tmp.unit_code,
  tmp.agency,
  tmp.unit_leader_code,
  tmp.unit_leader_name,
  tmp.unit_leaer_title,
  tmp.unit_leader_class,
  tmp.dissolved_date,

  TMP.AGT_CODE,
/*  AGT.FSC_NAME ,
  AGT.FSC_TITLE ,
  AGT.FSC_CLASS ,
  agt.FSC_HIREDATE ,
  AGT.FSC_APPOINTMENT_DATE ,
  agt.FSC_TERMINATION_DATE ,
  agt.FSC_STATUS*/
  tmp.name,
  tmp.role,
  tmp.class,
  tmp.contract_date,
  tmp.appointment_date,
  tmp.termination_date,
  tmp.agent_status
  ;
COMMIT;

Log('10 Records inserted into AIA_RPT_PRD_DIST_WRI are '||SQL%ROWCOUNT);
  ------=======ADD LOG




  INSERT
    /*+ Append  */
  INTO Aia_Rpt_PRD_Dist_WRI NOLOGGING
    (
      PROCESSINGUNITSEQ,
      PUNAME,
      BUNAME,
      BUMAP,
      calendarseq,
      calendarname,
      PERIODKEY,
      Periodname,
      --POSITIONSEQ,
      MANAGERSEQ,
      POSITIONSEQ,
      POSITIONNAME,
      DISTRICT_CODE,
      DM_NAme,
      dIST_LEADER_CODE,
      DIST_LEADER_NAME,
      DIST_LEAER_TITLE,
      DIST_LEADER_CLASS,
      UNIT_CODE,
      AGENCY,
      UNIT_LEADER_CODE,
      UNIT_LEADER_NAME,
      UNIT_LEAER_TITLE,
      UNIT_LEADER_CLASS,
      DISSOLVED_DATE,
      AGT_CODE,
      NAME,
      ROLE,
      CLASS,
      CONTRACT_DATE,
      APPOINTMENT_DATE,
      TERMINATION_DATE,
      AGENT_STATUS,
      LIFE_FYP,
      PA_FYP,
      LIFE_CASE,
      PA_CASE,
      LIFE_FYC_API_SSC,
      LIFE_FYC_RP,
 LIFE_FYC_NONILP,
 LIFE_FYC_ILP,
 LIFE_FYC_TOPUP,
      PA_FYC,
      CS_FYC,
      LIFE_2YEAR,
      LIFE_3YEAR,
      LIFE_4YEAR,
      Pa_Rc,
      CS_RC,
      CS_PL,
      UNASSIGNED_LIFE_2YEAR,
      UNASSIGNED_LIFE_3YEAR,
      UNASSIGNED_LIFE_4YEAR,
      UNASSIGNED_PA_RC,
      LIFE_FYP_YTD,
      PA_FYP_YTD,
      LIFE_CASE_YTD,
      PA_CASE_YTD,
      LIFE_FYC_API_SSC_YTD,
      LIFE_FYC_RP_YTD,
 LIFE_FYC_NONILP_YTD,
 LIFE_FYC_ILP_YTD,
 LIFE_FYC_TOPUP_YTD ,
      PA_FYC_YTD,
      CS_FYC_YTD,
      Life_2year_Ytd,
      LIFE_3YEAR_YTD,
      LIFE_4YEAR_YTD,
      PA_RC_YTD,
      CS_RC_YTD,
      CS_PL_YTD,
      UNASSIGNED_LIFE_2YEAR_YTD,
      UNASSIGNED_LIFE_3YEAR_YTD,
      Unassigned_Life_4year_Ytd,
      UNASSIGNED_PA_RC_YTD
    )
SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
  V_PROCESSINGUNITNAME PUNAME,
  AGT.FSC_BUNAME BUNAME,
  TMP.BUMAP BUMAP,
  V_CALENDARSEQ calendarseq,
  V_CALENDARNAME calendarname,
  V_PERIODSEQ PERIODKEY,
  V_PERIODNAME Periodname,
  --NULL POSITIONSEQ,
  TMP.MANAGERSEQ MANAGERSEQ,
  TMP.POSITIONSEQ POSITIONSEQ,
  TMP.POSITIONNAME POSITIONNAME,
  DISTRICT.DIST_CODE,
  DISTRICT.DIST_NAME DM_NAME,
  district_ldr.DIST_LDR_CODE DIST_LEADER_CODE,
  district_ldr.DIST_LDR_NAME DIST_LEADER_NAME,
  district_ldr.DIST_LDR_TITLE DIST_LEAER_TITLE,
  DISTRICT_LDR.DIST_LDR_POS_GA4 DIST_LEADER_CLASS,
  AGY.AGY_CODE UNIT_CODE,
  AGY_NAME AGENCY,
  AGY_LDR.AGY_LDR_CODE UNIT_LEADER_CODE,
  AGY_LDR.AGY_LDR_NAME  UNIT_LEADER_NAME,
  AGY_LDR.AGY_LDR_TITLE  UNIT_LEAER_TITLE,
  AGY_LDR.AGY_LDR_POS_GA4  UNIT_LEADER_CLASS,
  TO_CHAR(AGY.AGY_TERMINATIONDATE, 'MM/DD/YYYY') DISSOLVED_DATE,
  TMP.AGT_CODE,
  AGT.FSC_NAME NAME,
  AGT.FSC_TITLE ROLE,
  AGT.FSC_CLASS CLASS,
  AGT.FSC_HIREDATE CONTRACT_DATE,
  AGT.FSC_APPOINTMENT_DATE APPOINTMENT_DATE,
  AGT.FSC_TERMINATION_DATE TERMINATION_DATE,
  AGT.FSC_STATUS AGENT_STATUS,
  SUM(LIFE_FYP),
  SUM(PA_FYP),
  SUM( LIFE_CASE),
  SUM(PA_CASE),
  SUM(LIFE_FYC_API_SSC),
  SUM(LIFE_FYC_RP),
 SUM(LIFE_FYC_NONILP),
 SUM(LIFE_FYC_ILP),
 SUM(LIFE_FYC_TOPUP),
  SUM( PA_FYC),
  SUM( CS_FYC),
  SUM(LIFE_2YEAR),
  SUM( LIFE_3YEAR),
  SUM(LIFE_4YEAR),
  SUM( PA_RC),
  SUM( CS_RC),
  SUM( CS_PL),
  SUM(UNASSIGNED_LIFE_2YEAR),
  SUM( UNASSIGNED_LIFE_3YEAR),
  SUM(UNASSIGNED_LIFE_4YEAR),
  SUM(UNASSIGNED_PA_RC),
  SUM( LIFE_FYP_YTD),
  SUM( PA_FYP_YTD),
  SUM( LIFE_CASE_YTD),
  SUM( PA_CASE_YTD),
  SUM( LIFE_FYC_API_SSC_YTD),
 SUM(LIFE_FYC_RP_YTD),
 SUM(LIFE_FYC_NONILP_YTD),
 SUM(LIFE_FYC_ILP_YTD),
 SUM(LIFE_FYC_TOPUP_YTD),
  SUM( PA_FYC_YTD),
  SUM( CS_FYC_YTD),
  SUM( LIFE_2YEAR_YTD),
  SUM( LIFE_3YEAR_YTD),
  SUM( LIFE_4YEAR_YTD),
  SUM( PA_RC_YTD),
  SUM( CS_RC_YTD),
  SUM( CS_PL_YTD),
  SUM( UNASSIGNED_LIFE_2YEAR_YTD),
  SUM( UNASSIGNED_LIFE_3YEAR_YTD),
  SUM( UNASSIGNED_LIFE_4YEAR_YTD),
  SUM( UNASSIGNED_PA_RC_YTD)
--
--   SUM(LIFE_FYP)               + SUM( LIFE_FYP_YTD),
--  SUM(PA_FYP)                 + SUM( PA_FYP_YTD),
--  SUM( LIFE_CASE)             + SUM( LIFE_CASE_YTD),
--  SUM(PA_CASE)                + SUM( PA_CASE_YTD),
--  SUM(LIFE_FYC_API_SSC)       + SUM( LIFE_FYC_API_SSC_YTD),
--    SUM(LIFE_FYC_RP)  + SUM(LIFE_FYC_RP_YTD),
-- SUM(LIFE_FYC_NONILP) + SUM(LIFE_FYC_NONILP_YTD),
-- SUM(LIFE_FYC_ILP) + SUM(LIFE_FYC_ILP_YTD),
-- SUM(LIFE_FYC_TOPUP) + SUM(LIFE_FYC_TOPUP_YTD),
--  SUM( PA_FYC)                + SUM( PA_FYC_YTD),
--  SUM( CS_FYC)                + SUM( CS_FYC_YTD),
--  SUM(LIFE_2YEAR)             + SUM( LIFE_2YEAR_YTD),
--  SUM( LIFE_3YEAR)            + SUM( LIFE_3YEAR_YTD),
--  SUM(LIFE_4YEAR)             + SUM( LIFE_4YEAR_YTD),
--  SUM( PA_RC)                 + SUM( PA_RC_YTD),
--  SUM( CS_RC)                 + SUM( CS_RC_YTD),
--  SUM( CS_PL)                 + SUM( CS_PL_YTD),
--  SUM(UNASSIGNED_LIFE_2YEAR)  + SUM( UNASSIGNED_LIFE_2YEAR_YTD),
--  SUM( UNASSIGNED_LIFE_3YEAR) + SUM( UNASSIGNED_LIFE_3YEAR_YTD),
--  SUM(UNASSIGNED_LIFE_4YEAR)  + SUM( UNASSIGNED_LIFE_4YEAR_YTD),
--  SUM(UNASSIGNED_PA_RC)       + SUM( UNASSIGNED_PA_RC_YTD)
FROM AIA_RPT_PRD_DIST_WRI_TMP tmp,
  AIA_RPT_AGENT_LIST agt,
  AIA_RPT_AGENCY_LIST agy,
  AIA_RPT_AGENCY_LEADER agy_ldr,
  AIA_RPT_DISTRICT_LIST district,
  AIA_RPT_DISTRICT_LEADER DISTRICT_LDR
WHERE TMP.PERIODKEY          = V_PERIODSEQ
AND TMP.BUNAME = 'SGPAFA'

AND AGT.FSC_ID           = TMP.AGT_CODE
AND AGY.AGY_CODE        = TMP.UNIT_CODE
AND agy.NEW_DISTRICT_CODE = TMP.DISTRICT_CODE
AND TMP.BUNAME = AGT.FSC_BUNAME
AND AGY.AGY_BUNAME = AGT.FSC_BUNAME
AND AGT.FSC_BUNAME = DISTRICT_LDR.DIST_LDR_BUNAME
AND (agy_ldr.AGY_LDR_PARTICIPANTID = 'SRT' || agy.AGY_POS_GA2 AND agy.AGY_PARTICIPANTID like 'SRY%')
AND AGY_LDR.AGY_LDR_CODE      = AGY.AGY_POS_GA2
AND DISTRICT.DIST_BUNAME = AGY.AGY_BUNAME
AND DISTRICT.DIST_CODE = agy.NEW_DISTRICT_CODE
AND DISTRICT_LDR.DIST_LDR_CODE = DISTRICT.DIST_POS_GA2
GROUP BY
  AGT.FSC_BUNAME,
  TMP.BUMAP,
  TMP.MANAGERSEQ,
  TMP.POSITIONSEQ,
  TMP.POSITIONNAME,
  DISTRICT.DIST_CODE,
  DISTRICT.DIST_NAME ,
  district_ldr.DIST_LDR_CODE ,
  district_ldr.DIST_LDR_NAME ,
  district_ldr.DIST_LDR_TITLE ,
  DISTRICT_LDR.DIST_LDR_POS_GA4 ,
  AGY.AGY_CODE ,
  AGY.AGY_NAME ,
  AGY_LDR.AGY_LDR_CODE ,
  AGY_LDR.AGY_LDR_NAME  ,
  AGY_LDR.AGY_LDR_TITLE  ,
  AGY_LDR.AGY_LDR_POS_GA4  ,
  TO_CHAR(AGY.AGY_TERMINATIONDATE, 'MM/DD/YYYY') ,
  TMP.AGT_CODE,
  AGT.FSC_NAME ,
  AGT.FSC_TITLE ,
  AGT.FSC_CLASS ,
  agt.FSC_HIREDATE ,
  AGT.FSC_APPOINTMENT_DATE ,
  agt.FSC_TERMINATION_DATE ,
  agt.FSC_STATUS
  ;

Log('11 Records inserted into AIA_RPT_PRD_DIST_WRI are '||SQL%ROWCOUNT);
COMMIT;

  ------=======ADD LOG


-- DISTRICT TOTALS
UPDATE Aia_Rpt_PRD_Dist_WRI P1
SET
  (
    Life_Fyp_Dt ,
    Pa_Fyp_DT ,
    Life_Case_Dt ,
    Pa_Case_Dt ,
    Life_Fyc_Api_Ssc_Dt ,
    LIFE_FYC_RP_DT,
 LIFE_FYC_NONILP_DT,
 LIFE_FYC_ILP_DT,
 LIFE_FYC_TOPUP_DT,
    Pa_Fyc_Dt ,
    Cs_Fyc_Dt ,
    Life_2year_Dt ,
    Life_3year_Dt ,
    Life_4year_Dt ,
    Pa_Rc_Dt ,
    Cs_Rc_Dt ,
    Cs_Pl_Dt ,
    Unassigned_Life_2year_Dt ,
    Unassigned_Life_3year_Dt ,
    Unassigned_Life_4year_Dt ,
    Unassigned_Pa_Rc_Dt ,
    Life_Fyp_Ytd_Dt ,
    Pa_Fyp_Ytd_Dt ,
    Life_Case_Ytd_Dt ,
    Pa_Case_Ytd_Dt ,
    Life_Fyc_Api_Ssc_Ytd_Dt ,
    LIFE_FYC_RP_YTD_DT,
 LIFE_FYC_NONILP_YTD_DT,
 LIFE_FYC_ILP_YTD_DT,
 LIFE_FYC_TOPUP_YTD_DT ,
    Pa_Fyc_Ytd_Dt ,
    Cs_Fyc_Ytd_Dt ,
    Life_2year_Ytd_Dt ,
    Life_3year_Ytd_Dt ,
    Life_4year_Ytd_Dt ,
    Pa_Rc_Ytd_Dt ,
    Cs_Rc_Ytd_Dt ,
    Cs_Pl_Ytd_Dt ,
    Unassigned_Life_2year_Ytd_Dt ,
    Unassigned_Life_3year_Ytd_Dt ,
    Unassigned_Life_4year_Ytd_Dt ,
    Unassigned_Pa_Rc_Ytd_Dt
  )
  =
  (SELECT SUM(Life_Fyp) ,
    SUM(Pa_Fyp) ,
    SUM(Life_Case) ,
    SUM(Pa_Case) ,
    SUM(Life_Fyc_Api_Ssc) ,
    SUM(LIFE_FYC_RP),
 SUM(LIFE_FYC_NONILP),
 SUM(LIFE_FYC_ILP),
 SUM(LIFE_FYC_TOPUP),
    SUM(Pa_Fyc) ,
    SUM(Cs_Fyc) ,
    SUM(Life_2year) ,
    SUM(Life_3year) ,
    SUM(Life_4year) ,
    SUM(Pa_Rc) ,
    SUM(Cs_Rc) ,
    SUM(Cs_Pl) ,
    SUM(Unassigned_Life_2year) ,
    SUM(Unassigned_Life_3year) ,
    SUM(Unassigned_Life_4year) ,
    SUM(Unassigned_Pa_Rc) ,
    SUM(Life_Fyp_Ytd) ,
    SUM(Pa_Fyp_Ytd) ,
    SUM(Life_Case_Ytd) ,
    SUM(Pa_Case_Ytd) ,
    SUM(Life_Fyc_Api_Ssc_Ytd) ,
    SUM(LIFE_FYC_RP_YTD),
 SUM(LIFE_FYC_NONILP_YTD),
 SUM(LIFE_FYC_ILP_YTD),
 SUM(LIFE_FYC_TOPUP_YTD) ,
    SUM(Pa_Fyc_Ytd) ,
    SUM(Cs_Fyc_Ytd) ,
    SUM(Life_2year_Ytd) ,
    SUM(Life_3year_Ytd) ,
    SUM(Life_4year_Ytd) ,
    SUM(Pa_Rc_Ytd) ,
    SUM(Cs_Rc_Ytd) ,
    SUM(Cs_Pl_Ytd) ,
    SUM(Unassigned_Life_2year_Ytd) ,
    SUM(Unassigned_Life_3year_Ytd) ,
    SUM(Unassigned_Life_4year_Ytd) ,
    SUM(Unassigned_Pa_Rc_Ytd)
  FROM AIA_RPT_PRD_DIST_WRI P2
  WHERE P1.DISTRICT_CODE = P2.DISTRICT_CODE
  AND P1.BUNAME          = P2.BUNAME
    --and p2.periodkey = p1.periodkey
  AND p2.periodkey = V_PERIODSEQ
  )
WHERE p1.periodkey = V_PERIODSEQ;
Log('12 Records updated into AIA_RPT_PRD_DIST_WRI are '||SQL%ROWCOUNT);
COMMIT;
    ------=======ADD LOG

    UPDATE Aia_Rpt_PRD_Dist_WRI P1
SET
  (
    Life_Fyp_Ct ,
    Pa_Fyp_Ct ,
    Life_Case_ct ,
    Pa_Case_Ct ,
    Life_Fyc_Api_Ssc_ct ,
    LIFE_FYC_RP_CT,
 LIFE_FYC_NONILP_CT,
 LIFE_FYC_ILP_CT,
 LIFE_FYC_TOPUP_CT,
    Pa_Fyc_ct ,
    Cs_Fyc_Ct ,
    Life_2year_ct ,
    Life_3year_ct ,
    Life_4year_ct ,
    Pa_Rc_ct ,
    Cs_Rc_ct ,
    Cs_Pl_Ct ,
    Unassigned_Life_2year_ct ,
    Unassigned_Life_3year_ct ,
    Unassigned_Life_4year_ct ,
    Unassigned_Pa_Rc_ct ,
    Life_Fyp_Ytd_ct ,
    Pa_Fyp_Ytd_ct ,
    Life_Case_Ytd_ct ,
    Pa_Case_Ytd_Ct ,
    Life_Fyc_Api_Ssc_Ytd_ct ,
    LIFE_FYC_RP_YTD_CT,
 LIFE_FYC_NONILP_YTD_CT,
 LIFE_FYC_ILP_YTD_CT,
 LIFE_FYC_TOPUP_YTD_CT,
    Pa_Fyc_Ytd_ct ,
    Cs_Fyc_Ytd_ct ,
    Life_2year_Ytd_Ct ,
    Life_3year_Ytd_Ct ,
    Life_4year_Ytd_Ct ,
    Pa_Rc_Ytd_Ct ,
    Cs_Rc_Ytd_ct ,
    Cs_Pl_Ytd_ct ,
    Unassigned_Life_2year_Ytd_ct ,
    Unassigned_Life_3year_Ytd_ct ,
    Unassigned_Life_4year_Ytd_ct ,
    Unassigned_Pa_Rc_Ytd_ct
  )
  =
  (SELECT SUM(Life_Fyp) ,
    SUM(Pa_Fyp) ,
    SUM(Life_Case) ,
    SUM(Pa_Case) ,
    SUM(Life_Fyc_Api_Ssc) ,
    SUM(LIFE_FYC_RP),
 SUM(LIFE_FYC_NONILP),
 SUM(LIFE_FYC_ILP),
 SUM(LIFE_FYC_TOPUP),
    SUM(Pa_Fyc) ,
    SUM(Cs_Fyc) ,
    SUM(Life_2year) ,
    SUM(Life_3year) ,
    SUM(Life_4year) ,
    SUM(Pa_Rc) ,
    SUM(Cs_Rc) ,
    SUM(Cs_Pl) ,
    SUM(Unassigned_Life_2year) ,
    SUM(Unassigned_Life_3year) ,
    SUM(Unassigned_Life_4year) ,
    SUM(Unassigned_Pa_Rc) ,
    SUM(Life_Fyp_Ytd) ,
    SUM(Pa_Fyp_Ytd) ,
    SUM(Life_Case_Ytd) ,
    SUM(Pa_Case_Ytd) ,
    SUM(Life_Fyc_Api_Ssc_Ytd) ,
    SUM(LIFE_FYC_RP_YTD),
 SUM(LIFE_FYC_NONILP_YTD),
 SUM(LIFE_FYC_ILP_YTD),
 SUM(LIFE_FYC_TOPUP_YTD),
    SUM(Pa_Fyc_Ytd) ,
    SUM(Cs_Fyc_Ytd) ,
    SUM(Life_2year_Ytd) ,
    SUM(Life_3year_Ytd) ,
    SUM(Life_4year_Ytd) ,
    SUM(Pa_Rc_Ytd) ,
    SUM(Cs_Rc_Ytd) ,
    SUM(Cs_Pl_Ytd) ,
    SUM(Unassigned_Life_2year_Ytd) ,
    SUM(Unassigned_Life_3year_Ytd) ,
    SUM(Unassigned_Life_4year_Ytd) ,
    SUM(Unassigned_Pa_Rc_Ytd)
  FROM AIA_RPT_PRD_DIST_WRI P2
  WHERE P1.BUNAME = P2.BUNAME
    --and p2.periodkey = p1.periodkey
  AND p2.periodkey = V_PERIODSEQ
  )
WHERE p1.periodkey = V_PERIODSEQ;
Log('13 Records updated into AIA_RPT_PRD_DIST_WRI are '||SQL%ROWCOUNT);
COMMIT;

      ------=======ADD LOG

/* Start of deleting the Terminated Agents with 0 Value. Added by Saurabh Trehan*/
delete from Aia_Rpt_PRD_Dist_WRI
where upper(Agent_Status) != 'INFORCE'
and   termination_date is not null
and
periodkey = V_PERIODSEQ
and  nvl(LIFE_FYP,0) = 0 and
nvl(PA_FYP,0) = 0 and
nvl(LIFE_CASE,0) = 0 and
nvl(PA_CASE,0) = 0 and
nvl(LIFE_FYC_API_SSC,0) = 0 and
nvl(LIFE_FYC_RP,0)=0 and
nvl(LIFE_FYC_NONILP,0)=0 and
nvl(LIFE_FYC_ILP,0)=0 and
 nvl(LIFE_FYC_TOPUP,0)=0 and
nvl(PA_FYC,0) = 0 and
nvl(CS_FYC,0) = 0 and
nvl(LIFE_2YEAR,0) = 0 and
nvl(LIFE_3YEAR,0) = 0 and
NVL(LIFE_4YEAR,0) = 0 AND
nvl(PA_RC,0) = 0 and
nvl(CS_RC,0) = 0 and
NVL(CS_PL,0) = 0 AND
--------------------------------------
NVL(UNASSIGNED_LIFE_2YEAR,0) = 0 AND
NVL(UNASSIGNED_LIFE_3YEAR,0) = 0 AND
NVL(UNASSIGNED_LIFE_4YEAR,0) = 0 AND
NVL(UNASSIGNED_PA_RC,0) = 0 AND
---------------------------------------
nvl(LIFE_FYP_YTD,0)  = 0 and
nvl(PA_FYP_YTD,0)  = 0 and
nvl(LIFE_CASE_YTD,0)  = 0 and
nvl(PA_CASE_YTD,0)  = 0 and
nvl(LIFE_FYC_API_SSC_YTD,0)  = 0 and
nvl(LIFE_FYC_RP_YTD,0)=0 and
 nvl(LIFE_FYC_NONILP_YTD,0)=0 and
 nvl(LIFE_FYC_ILP_YTD,0)=0 and
 nvl(LIFE_FYC_TOPUP_YTD,0)=0 and
nvl(PA_FYC_YTD,0)  = 0 and
nvl(CS_FYC_YTD,0)  = 0 and
nvl(LIFE_2YEAR_YTD,0)  = 0 and
nvl(LIFE_3YEAR_YTD,0)  = 0 and
nvl(LIFE_4YEAR_YTD,0)  = 0 and
NVL(PA_RC_YTD,0)  = 0 AND
NVL(CS_RC_YTD,0)  = 0 AND
NVL(CS_PL_YTD,0)  = 0 AND
NVL(UNASSIGNED_LIFE_2YEAR_YTD,0)  = 0 AND
NVL(UNASSIGNED_LIFE_3YEAR_YTD,0)  = 0 AND
NVL(UNASSIGNED_LIFE_4YEAR_YTD,0)  = 0 AND
NVL(Unassigned_Pa_Rc_Ytd,0)  = 0
--AND nvl(PA_FYC_DT,0) = 0 and
--nvl(CS_FYC_DT,0) = 0 and
--nvl(PA_RC_DT,0) = 0 and
--nvl(CS_RC_DT,0) = 0 and
--NVL(CS_PL_DT,0) = 0
;
/* End of deleting the Terminated Agents with 0 Value. Added by Saurabh Trehan*/
Log('14 Records DELETED into AIA_RPT_PRD_DIST_WRI are '||SQL%ROWCOUNT);
commit;

      ------=======ADD LOG JEFF1
--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_WRI' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_WRI' , 'AIA_RPT_PRD_DIST_WRI_PARAM'  , 'Processing', 'insert into AIA_RPT_PRD_DIST_WRI_PARAM');



-- Parameter table updation
EXECUTE IMMEDIATE 'Truncate table AIA_RPT_PRD_DIST_WRI_PARAM drop storage' ;

      ------=======ADD LOG

--1
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  ) ;
Log('15 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
COMMIT;

--ADDED 2
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  ) ;
Log('16 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
COMMIT;

--ADDED 3
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  ) ;
Log('17 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
COMMIT;

--ADDED 4
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  ) ;
Log('18 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
COMMIT;
--ADDED 5
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  ) ;
Log('19 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
COMMIT;
--ADDED 6
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
Log('20 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
COMMIT;
--ADDED 7
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
Log('21 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
COMMIT;
--added new
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
Log('22 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
COMMIT;
--added new
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
Log('23 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
COMMIT;
--ADDED 8
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
Log('24 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
COMMIT;
--ADDED 9
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
Log('25 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
COMMIT;
--ADDED 10
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
Log('26 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
COMMIT;
--ADDED 11
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
Log('27 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
COMMIT;
--ADDED 12
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
  Log('28 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
COMMIT;
--ADDED 13
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
Log('29 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
COMMIT;
--ADDED 14
INSERT
INTO AIA_RPT_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM aia_rpt_PRD_dist_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );

Log('30 Records Inserted into AIA_RPT_PRD_DIST_WRI_PARAM are '||SQL%ROWCOUNT);
commit;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_WRI' , 'Finish', '');

End;

PROCEDURE REP_RUN_NEW_PRD_WRI as

  V_EOT                DATE := TO_DATE('01/01/2200','DD/MM/YYYY');
  V_submissionDate     DATE := TO_DATE('01/01/2017','DD/MM/YYYY');
  V_PERIODNAME         VARCHAR2(255 BYTE);
  V_CALENDARNAME       VARCHAR2(255);
  V_PROCESSINGUNITSEQ  INTEGER;
  V_PROCESSINGUNITNAME VARCHAR2(256);
  V_PERIODSTARTDATE    DATE;
  V_PERIODENDDATE      DATE;
  V_CALENDARSEQ       INTEGER;
  V_PERIODTYPESEQ     CS_PERIOD.PERIODTYPESEQ%TYPE;

Begin
  --V_PROCNAME := 'PROC_RPT_PRD_DISTRICT';

BEGIN
SELECT P.STARTDATE,P.ENDDATE,C.DESCRIPTION,P.NAME,P.CALENDARSEQ
INTO V_PERIODSTARTDATE,V_PERIODENDDATE,V_CALENDARNAME,V_PERIODNAME,V_CALENDARSEQ
FROM  CS_PERIOD P INNER JOIN  CS_CALENDAR C ON P.CALENDARSEQ=C.CALENDARSEQ
WHERE PERIODSEQ = V_PERIODSEQ
;
SELECT PERIODTYPESEQ INTO V_PERIODTYPESEQ FROM  CS_PERIODTYPE WHERE NAME ='month';

end;

-----------=======LOG
  SELECT PROCESSINGUNITSEQ,NAME  INTO V_PROCESSINGUNITSEQ,V_PROCESSINGUNITNAME
  FROM CS_PROCESSINGUNIT  WHERE NAME = 'AGY_PU';

  SELECT MASK INTO v_BUSINESSUNITMAP1 FROM CS_BUSINESSUNIT
   WHERE NAME        IN ('SGPAFA');
  SELECT MASK INTO v_BUSINESSUNITMAP2 FROM CS_BUSINESSUNIT
   WHERE NAME        IN ('BRUAGY');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_NEW_PRD_WRI' , 'AIA_RPT_PRD_DIST_WRI_NEW 1'  , 'Processing', 'insert into AIA_RPT_PRD_DIST_WRI_NEW');


-----------=======LOG
  EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_DIST_WRI_NEW';


-- Date insert in reporting table,
/*Commented by Suresh  INSERT
    /*+ Append  */
/*  INTO AIA_RPT_PRD_DIST_WRI_NEW NOLOGGING
    (SELECT
        V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
        V_PROCESSINGUNITNAME PUNAME,
        decode(PD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') BUNAME,
        PD.businessunitmap BUMAP,
        V_CALENDARSEQ CALENDARSEQ,
        V_CALENDARNAME CALENDARNAME,
        V_PERIODSEQ PERIODKEY,
        V_PERIODNAME periodname,
        --PAD.POSITIONSEQ POSITIONSEQ,
        PAD.MANAGERSEQ,
        PAD.POSITIONSEQ POSITIONSEQ,
        PAD.NAME,
/*      SUBSTR(DISTRICT.NAME,4) DISTRICT_CODE,
        district.firstname
        || ' '
        || district.lastname DM_NAme,
        district.POS_GA2 DIST_LEADER_CODE,
        district.POS_GA7 DIST_LEADER_NAME,
        district.POS_GA11 DIST_LEAER_TITLE,
        DISTRICT.POS_GA4 DIST_LEADER_CLASS,*/
 /*       SUBSTR(pos_dis.NAME,4) DISTRICT_CODE,
       par_dis.firstname || ' ' || par_dis.lastname DM_NAME,
       pos_dis.genericattribute2 DIST_LEADER_CODE,
       pos_dis.genericattribute7 DIST_LEADER_NAME,
       pos_dis.genericattribute11 DIST_LEAER_TITLE,
       pos_dis.genericattribute4 DIST_LEADER_CLASS,
       PD.CD_GA13 UNIT_CODE,
       agy.firstname
       || ' '
       || agy.lastname AGENCY,
       agy.POS_GA2 UNIT_LEADER_CODE,
       agy.POS_GA7 UNIT_LEADER_NAME,
       DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
       agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
       (
       CASE
         WHEN AGY.POSITIONTITLE IN ('AGENCY','DISTRICT','BR_AGENCY','BR_DISTRICT')
         THEN AGY.AGY_TERMINATIONDATE
       END)DISSOLVED_DATE,
       SUBSTR(PAD.NAME,4) AGT_CODE,
       (PAD.firstname
       || PAD.lastname) NAME,
       DECODE(PAD.positiontitle, 'FSC_NON_PROCESS', 'FSC', PAD.positiontitle) ROLE,
       Pad.POS_GA4 CLASS,
       PAD.HIREDATE CONTRACT_DATE,
       PAD.AGY_APPOINTMENT_DATE APPOINTMENT_DATE,
       PAD.AGY_TERMINATIONDATE TERMINATION_DATE,
       (
       CASE
         WHEN pad.PT_GA1 = '00'
         THEN 'INFORCE'
         WHEN pad.PT_GA1 IN ('50','51','52','55','56')
         THEN 'TERMINATED'
         WHEN pad.PT_GA1 = '13'
         THEN 'TERMINATED'
         WHEN pad.PT_GA1 IN ('60','61')
         THEN 'TERMINATED'
         WHEN pad.PT_GA1 = '70'
         THEN 'TERMINATED'
       END) AGENT_STATUS,

       --removed from report
       0 as LIFE_FYP,
       0 as PA_FYP,
       0 as LIFE_CASE,
       0 as PA_CASE,

       CASE
         WHEN (PD.CD_GA2 IN ('LF','HS')
         --AND PD.CD_CREDITTYPE         IN ('FYC','API','SSCP','APB')
         AND PD.CD_CREDITTYPE          IN ('FYC', 'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                                           'API', 'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                                           'SSCP', 'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                                           'APB')
         )
         THEN PD.CD_VALUE
         ELSE 0
       END LIFE_FYC_API_SSC,
       CASE
         WHEN (PD.CD_GA2 IN ('PA') --,'VL') as asked by Donny
         --AND PD.CD_CREDITTYPE         IN ('FYC','API','SSCP','APB')
         AND PD.CD_CREDITTYPE          IN ('FYC', 'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                                           'API', 'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                                           'SSCP', 'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                                           'APB')
         )
         THEN PD.CD_VALUE
         ELSE 0
       END PA_FYC,
       CASE
         WHEN ( PD.CD_GA2 IN ('CS','CL')
         --AND PD.CD_CREDITTYPE           IN ('FYC','API','SSCP','APB')
         AND PD.CD_CREDITTYPE          IN ('FYC', 'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                                           'API', 'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                                           'SSCP', 'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                                           'APB')
         )
         THEN PD.CD_VALUE
         ELSE 0
       END CS_FYC,
       CASE
         WHEN (PD.CD_GA4 = 'PAY2'
         AND PD.CD_CREDITTYPE         IN ('RYC','RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
         AND PD.CD_GA2  IN ('LF','HS') )
         THEN PD.CD_VALUE
         ELSE 0
       END LIFE_2YEAR,
       CASE
         WHEN (PD.CD_GA4 = 'PAY3'
         AND PD.CD_CREDITTYPE         IN ('RYC','RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
         AND PD.CD_GA2  IN ('LF','HS') )
         THEN PD.CD_VALUE
         ELSE 0
       END LIFE_3YEAR,
       CASE
         WHEN ( PD.CD_GA4 IN ('PAY4','PAY5','PAY6')
         AND PD.CD_CREDITTYPE            IN ('RYC','RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
         AND PD.CD_GA2    IN ('LF','HS') )
         THEN PD.CD_VALUE
         ELSE 0
       END LIFE_4YEAR,
       CASE
         WHEN (PD.CD_GA2 IN ('PA')--,'VL')
         AND PD.CD_CREDITTYPE         IN ('RYC','RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') )
         THEN PD.CD_VALUE
         ELSE 0
       END PA_RC,

       --removed from report
       0 as CS_RC,
       0 as CS_PL,

       0 UNASSIGNED_LIFE_2YEAR,
       0 UNASSIGNED_LIFE_3YEAR,
       0 UNASSIGNED_LIFE_4YEAR,
       0 UNASSIGNED_PA_RC,
       0 LIFE_FYP_YTD,
       0 PA_FYP_YTD,
       0 LIFE_CASE_YTD,
       0 PA_CASE_YTD,
       0 LIFE_FYC_API_SSC_YTD,
       0 PA_FYC_YTD,
       0 CS_FYC_YTD,
       0 LIFE_2YEAR_YTD,
       0 LIFE_3YEAR_YTD,
       0 LIFE_4YEAR_YTD,
       0 PA_RC_YTD,
       0 CS_RC_YTD,
       0 CS_PL_YTD,
       0 UNASSIGNED_LIFE_2YEAR_YTD,
       0 UNASSIGNED_LIFE_3YEAR_YTD,
       0 UNASSIGNED_LIFE_4YEAR_YTD,
       0 UNASSIGNED_PA_RC_YTD
     FROM AIA_RPT_PRD_TEMP_PADIM pad,
          AIA_RPT_PRD_TEMP_PADIM agy,
          AIA_RPT_PRD_TEMP_PADIM agy_ldr,
          --AIA_RPT_PRD_TEMP_PADIM DISTRICT,
          AIA_RPT_PRD_TEMP_VALUES PD
          --for writing agency postion info
          inner join cs_position pos_agy
          on pos_agy.name = trim('SGY'||PD.CD_GA13)
          AND pos_agy.effectivestartdate <= PD.CD_GD2 --policy issue date
          AND pos_agy.effectiveenddate   > PD.CD_GD2  --policy issue date
          AND pos_agy.removedate =DT_REMOVEDATE
          --for writing district postion info
          inner join cs_position pos_dis
          on pos_dis.name=trim('SGY'||pos_agy.genericattribute3)
          AND pos_dis.effectivestartdate <= PD.CD_GD2 --policy issue date
          AND pos_dis.effectiveenddate   > PD.CD_GD2  --policy issue date
          AND pos_dis.removedate =DT_REMOVEDATE
          --for writing district participant info
          inner join cs_participant par_dis
          on par_dis.PAYEESEQ = pos_dis.PAYEESEQ
          AND par_dis.effectivestartdate < V_ENDDATE
          AND par_dis.effectiveenddate   >  V_ENDDATE-1
          AND par_dis.removedate = DT_REMOVEDATE
     WHERE PD.NEW_PRD_IND = 1 AND PD.PRD_TYPE = 'DIRECT'
     AND  PD.SUBMITDATE >= V_submissionDate
     AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD','AM','FSD','FSM','BR_DM','BR_FSC','BR_UM')
     AND PAD.POS_GA4 NOT IN ('45','48')
     --AND PD.CD_POSITIONSEQ = PAD.POSITIONSEQ
     AND (
     (PD.businessunitmap = 1 and PD.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (PD.businessunitmap <> 1 and PD.CD_POSITIONSEQ = PAD.POSITIONSEQ)
     )
     --AND PD.CD_CREDITTYPE IN ('FYP', 'RYC', 'FYC','API','SSCP','APB')
     AND PD.CD_CREDITTYPE IN ('FYP', 'FYC', 'RYC', 'API', 'SSCP', 'APB',
                            'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                            'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                            'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                            'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
     AND AGY.POS_GA1 = PD.CD_GA13
     AND AGY.POSITIONTITLE      IN ('AGENCY','DISTRICT','BR_AGENCY','BR_DISTRICT')
     AND ((agy_ldr.NAME = 'SGT'
       || agy.POS_GA2
     AND agy.NAME LIKE 'SGY%')
     OR (agy_ldr.NAME = 'BRT'
       || agy.POS_GA2
     AND agy.NAME LIKE 'BRY%'))
     --AND DISTRICT.POS_GA3 = AGY.POS_GA3
     --AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
     --AND district.POSITIONTITLE     IN ('DISTRICT','BR_DISTRICT')
     AND PD.CD_GA2  IN ('LF','PA','HS','CS','PL','CL','VL'));
     */

           --Added by Suresh
  EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_DIST_WRI_NEW_HELP';

insert /*+APPEND*/ into AIA_RPT_PRD_DIST_WRI_NEW_HELP NOLOGGING
(select /*+   */
       decode(PD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') BUNAME,
      --decode(PD.businessunitmap,1,'SGPAFA',2,'BRUAGY') BUNAME,
        PD.businessunitmap BUMAP,
        PD.CD_GA13 UNIT_CODE,
        CASE
          WHEN (PD.CD_GA2 IN ('LF','HS')
          --AND PD.CD_CREDITTYPE         IN ('FYC','API','SSCP','APB')
          AND PD.CD_CREDITTYPE          IN ('FYC', 'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                                            'API', 'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                                            'SSCP', 'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                                            'APB')
          )
          THEN PD.CD_VALUE
          ELSE 0
        END LIFE_FYC_API_SSC,
        CASE
 WHEN (PD.CD_GA2 IN ('LF','HS')
 --AND PD.CD_CREDITTYPE IN ('FYC','API','SSCP','APB')
 AND PD.CD_GA4 IN ('PAY1','PAY0','PAYE','PAYF')
 )
 THEN PD.CD_VALUE
 ELSE 0
 END LIFE_FYC_RP,

 CASE
 WHEN (PD.CD_GA2 IN ('LF','HS')
 --AND PD.CD_CREDITTYPE IN ('FYC','API','SSCP','APB')
 AND PD.CD_GA4 IN ('PAYS','PTAF')
 )
 THEN PD.CD_VALUE
 ELSE 0
 END LIFE_FYC_NONILP,

   CASE
 WHEN (PD.CD_GA2 IN ('LF','HS')
 --AND PD.CD_CREDITTYPE IN ('FYC','API','SSCP','APB')
 AND PD.CD_GA4 IN ('PAYS')
 AND PD.CD_GA1 NOT IN ('GFBA','GFBC','IACB','IAGB','IAP4','IARB','IAS1','IAS2','IAS3','IBOB','IBSB','IFRB','IFYP','IFZP','IGCB','IPGA','IPOA','IPOB','IPOC','IPOD','ISAC','ISBO','ISFS','ISGR','IWBO','IWBS','IWCB','IWRB')
 )
 THEN PD.CD_VALUE
 ELSE 0
 END LIFE_FYC_ILP,

 CASE
 WHEN (PD.CD_GA2 IN ('LF','HS')
 --AND PD.CD_CREDITTYPE IN ('FYC','API','SSCP','APB')
 AND PD.CD_GA4 IN ('PAYT')
 )
 THEN PD.CD_VALUE
 ELSE 0
 END LIFE_FYC_TOPUP,

        CASE
          WHEN (PD.CD_GA2 IN ('PA') --,'VL') as asked by Donny
          --AND PD.CD_CREDITTYPE         IN ('FYC','API','SSCP','APB')
          AND PD.CD_CREDITTYPE          IN ('FYC', 'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                                            'API', 'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                                            'SSCP', 'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                                            'APB')
          )
          THEN PD.CD_VALUE
          ELSE 0
        END PA_FYC,
        CASE
          WHEN ( PD.CD_GA2 IN ('CS','CL')
          --AND PD.CD_CREDITTYPE           IN ('FYC','API','SSCP','APB')
          AND PD.CD_CREDITTYPE          IN ('FYC', 'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                                            'API', 'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                                            'SSCP', 'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                                            'APB')
          )
          THEN PD.CD_VALUE
          ELSE 0
        END CS_FYC,
        CASE
          WHEN (PD.CD_GA4 = 'PAY2'
          AND PD.CD_CREDITTYPE         IN ('RYC','RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
          AND PD.CD_GA2  IN ('LF','HS') )
          THEN PD.CD_VALUE
          ELSE 0
        END LIFE_2YEAR,
        CASE
          WHEN (PD.CD_GA4 = 'PAY3'
          AND PD.CD_CREDITTYPE         IN ('RYC','RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
          AND PD.CD_GA2  IN ('LF','HS') )
          THEN PD.CD_VALUE
          ELSE 0
        END LIFE_3YEAR,
        CASE
          WHEN ( PD.CD_GA4 IN ('PAY4','PAY5','PAY6')
          AND PD.CD_CREDITTYPE            IN ('RYC','RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
          AND PD.CD_GA2    IN ('LF','HS') )
          THEN PD.CD_VALUE
          ELSE 0
        END LIFE_4YEAR,
        CASE
          WHEN (PD.CD_GA2 IN ('PA')--,'VL')
          AND PD.CD_CREDITTYPE         IN ('RYC','RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') )
          THEN PD.CD_VALUE
          ELSE 0
        END PA_RC,

        SUBSTR(help1.NAME,4) DISTRICT_CODE,
        help1.firstname || ' ' || help1.lastname DM_NAME,
        help1.genericattribute2 DIST_LEADER_CODE,
        help1.genericattribute7 DIST_LEADER_NAME,
        help1.genericattribute11 DIST_LEAER_TITLE,
        help1.genericattribute4 DIST_LEADER_CLASS,
        PD.businessunitmap as businessunitmap,
        PD.CD_GA12 as CD_GA12,
        PD.CD_POSITIONSEQ as CD_POSITIONSEQ,
    PD.CD_GA13 CD_GA13,
    0 LIFE_CASE,
    0 PA_CASE,
    0 UNASSIGNED_LIFE_2YEAR,
    0 UNASSIGNED_LIFE_3YEAR,
    0 UNASSIGNED_LIFE_4YEAR,
    0 UNASSIGNED_PA_RC
        from
        AIA_RPT_PRD_TEMP_VALUES PD
           --for writing agency postion info
           inner join cs_position pos_agy
           on pos_agy.name = trim('SGY'||PD.CD_GA13)
           AND pos_agy.effectivestartdate <= PD.CD_GD2 --policy issue date
           AND pos_agy.effectiveenddate   > PD.CD_GD2  --policy issue date
           AND pos_agy.removedate =DT_REMOVEDATE
           --for writing district postion info
           inner join AIA_RPT_PRD_DIST_WRI_NEW_HELP1 help1
          ON help1.name=trim('SGY'||pos_agy.genericattribute3)
          AND help1.effstartdt <= PD.CD_GD2 --policy issue date
           AND help1.effenddt   > PD.CD_GD2  --policy issue date
    WHERE PD.NEW_PIB_IND = 1 AND PD.PIB_TYPE = 'DIRECT'
            AND  PD.SUBMITDATE >= V_submissionDate
        AND PD.CD_CREDITTYPE IN ('FYP', 'FYC', 'RYC', 'API', 'SSCP', 'APB',
                             'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                             'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                             'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                             'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
        --AND PD.CD_GA2  IN ('LF','PA','HS','CS','PL','CL','VL')
        AND PD.CD_GA2  IN ('LF','PA','HS','CS','PL','CL','VL','GI')   --version 9 add GI product
 );

 Log('Records inserted into AIA_RPT_PRD_DIST_WRI_NEW_HELP are '||SQL%ROWCOUNT);
COMMIT;


   BEGIN
        DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME => '"AIASEXT"' ,
                                        TABNAME => '"AIA_RPT_PRD_DIST_WRI_NEW_HELP"' ,
                                        ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE);
END;


-- Date insert in reporting table,
  INSERT
    /*+ Append  */
  INTO AIA_RPT_PRD_DIST_WRI_NEW NOLOGGING
    (SELECT
        V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
        V_PROCESSINGUNITNAME PUNAME,
        PD.BUNAME,
        PD.BUMAP,
        V_CALENDARSEQ CALENDARSEQ,
        V_CALENDARNAME CALENDARNAME,
        V_PERIODSEQ PERIODKEY,
        V_PERIODNAME periodname,
        --PAD.POSITIONSEQ POSITIONSEQ,
        PAD.MANAGERSEQ,
        PAD.POSITIONSEQ POSITIONSEQ,
        PAD.NAME,
/*      SUBSTR(DISTRICT.NAME,4) DISTRICT_CODE,
        district.firstname
        || ' '
        || district.lastname DM_NAme,
        district.POS_GA2 DIST_LEADER_CODE,
        district.POS_GA7 DIST_LEADER_NAME,
        district.POS_GA11 DIST_LEAER_TITLE,
        DISTRICT.POS_GA4 DIST_LEADER_CLASS,*/
        PD.DISTRICT_CODE,
        PD.DM_NAME,
        PD.DIST_LEADER_CODE,
        PD.DIST_LEADER_NAME,
        PD.DIST_LEAER_TITLE,
        PD.DIST_LEADER_CLASS,
        PD.UNIT_CODE,
        agy.firstname        || ' '        || agy.lastname AGENCY,
        agy.POS_GA2 UNIT_LEADER_CODE,
        agy.POS_GA7 UNIT_LEADER_NAME,
        DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
        agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
        (
        CASE
          WHEN AGY.POSITIONTITLE IN ('ORGANISATION','UNIT')
          THEN AGY.AGY_TERMINATIONDATE
        END)DISSOLVED_DATE,
        SUBSTR(PAD.NAME,4) AGT_CODE,
        (PAD.firstname
        || PAD.lastname) NAME,
        DECODE(PAD.positiontitle, 'FSC_NON_PROCESS', 'FSC', PAD.positiontitle) ROLE,
        Pad.POS_GA4 CLASS,
        PAD.HIREDATE CONTRACT_DATE,
        PAD.AGY_APPOINTMENT_DATE APPOINTMENT_DATE,
        PAD.AGY_TERMINATIONDATE TERMINATION_DATE,
        (
        CASE
          WHEN pad.PT_GA1 = '00'
          THEN 'INFORCE'
          WHEN pad.PT_GA1 IN ('50','51','52','55','56')
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 = '13'
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 IN ('60','61')
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 = '70'
          THEN 'TERMINATED'
        END) AGENT_STATUS,

        --removed from report
        0 as LIFE_FYP,
        0 as PA_FYP,
        0 as LIFE_CASE,
        0 as PA_CASE,

        PD.LIFE_FYC_API_SSC,
        PD.LIFE_FYC_RP,
 PD.LIFE_FYC_NONILP,
 PD.LIFE_FYC_ILP,
 PD.LIFE_FYC_TOPUP,
        PD.PA_FYC,
        PD.CS_FYC,
        PD.LIFE_2YEAR,
        PD.LIFE_3YEAR,
        PD.LIFE_4YEAR,
        PD.PA_RC,

        --removed from report
        0 as CS_RC,
        0 as CS_PL,

        0 UNASSIGNED_LIFE_2YEAR,
        0 UNASSIGNED_LIFE_3YEAR,
        0 UNASSIGNED_LIFE_4YEAR,
        0 UNASSIGNED_PA_RC,
        0 LIFE_FYP_YTD,
        0 PA_FYP_YTD,
        0 LIFE_CASE_YTD,
        0 PA_CASE_YTD,
        0 LIFE_FYC_API_SSC_YTD,
        0 LIFE_FYC_RP_YTD,
0 LIFE_FYC_NONILP_YTD,
0 LIFE_FYC_ILP_YTD,
 0 LIFE_FYC_TOPUP_YTD ,
        0 PA_FYC_YTD,
        0 CS_FYC_YTD,
        0 LIFE_2YEAR_YTD,
        0 LIFE_3YEAR_YTD,
        0 LIFE_4YEAR_YTD,
        0 PA_RC_YTD,
        0 CS_RC_YTD,
        0 CS_PL_YTD,
        0 UNASSIGNED_LIFE_2YEAR_YTD,
        0 UNASSIGNED_LIFE_3YEAR_YTD,
        0 UNASSIGNED_LIFE_4YEAR_YTD,
        0 UNASSIGNED_PA_RC_YTD
      FROM AIA_RPT_PRD_TEMP_PADIM pad,
           AIA_RPT_PRD_TEMP_PADIM agy,
           AIA_RPT_PRD_TEMP_PADIM agy_ldr,
           --AIA_RPT_PRD_TEMP_PADIM DISTRICT,
           AIA_RPT_PRD_DIST_WRI_NEW_HELP PD
      WHERE PAD.POSITIONTITLE IN ('AD','DIR','ED','FC','SD')
      AND PAD.POS_GA4 NOT IN ('45','48')
      --AND PD.CD_POSITIONSEQ = PAD.POSITIONSEQ
      AND (
      (PD.businessunitmap = 64 and PD.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (PD.businessunitmap <> 64 and PD.CD_POSITIONSEQ = PAD.POSITIONSEQ)
      )
      AND AGY.POS_GA1 = PD.CD_GA13
      AND AGY.POSITIONTITLE      IN ('ORGANISATION','UNIT')
      AND ((agy_ldr.NAME = 'SGT'
        || agy.POS_GA2
      AND agy.NAME LIKE 'SGY%')
      OR (agy_ldr.NAME = 'BRT'
        || agy.POS_GA2
      AND agy.NAME LIKE 'BRY%'))
      --AND DISTRICT.POS_GA3 = AGY.POS_GA3
      --AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
      --AND district.POSITIONTITLE     IN ('DISTRICT','BR_DISTRICT')
     );

--End By Suresh
      Log('31 Records inserted into AIA_RPT_PRD_DIST_WRI_NEW are ' || SQL%ROWCOUNT);
     COMMIT;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_NEW_PRD_WRI' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_NEW_PRD_WRI' , 'AIA_RPT_PRD_DIST_WRI_NEW 2'  , 'Processing', 'insert into AIA_RPT_PRD_DIST_WRI_NEW');


-----------=======LOG
/*commented by Suresh
INSERT
    /*+ Append */
/*  INTO AIA_RPT_PRD_DIST_WRI_NEW NOLOGGING
      (SELECT
        V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
        V_PROCESSINGUNITNAME PUNAME,
        decode(PD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') BUNAME,
        PD.businessunitmap BUMAP,
        V_CALENDARSEQ CALENDARSEQ,
        V_CALENDARNAME CALENDARNAME,
        V_PERIODSEQ PERIODKEY,
        V_PERIODNAME periodname,
        --PAD.POSITIONSEQ POSITIONSEQ,
        PAD.MANAGERSEQ,
        PAD.POSITIONSEQ POSITIONSEQ,
        PAD.NAME,
/*      SUBSTR(DISTRICT.NAME,4) DISTRICT_CODE,
        district.firstname
        || ' '
        || district.lastname DM_NAme,
        district.POS_GA2 DIST_LEADER_CODE,
        district.POS_GA7 DIST_LEADER_NAME,
        district.POS_GA11 DIST_LEAER_TITLE,
        DISTRICT.POS_GA4 DIST_LEADER_CLASS,*/
  /*      SUBSTR(pos_dis.NAME,4) DISTRICT_CODE,
      par_dis.firstname || ' ' || par_dis.lastname DM_NAME,
      pos_dis.genericattribute2 DIST_LEADER_CODE,
      pos_dis.genericattribute7 DIST_LEADER_NAME,
      pos_dis.genericattribute11 DIST_LEAER_TITLE,
      pos_dis.genericattribute4 DIST_LEADER_CLASS,
      PD.TRANS_GA11 UNIT_CODE,
      agy.firstname
      || ' '
      || agy.lastname AGENCY,
      agy.POS_GA2 UNIT_LEADER_CODE,
      agy.POS_GA7 UNIT_LEADER_NAME,
      DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
      agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
      (
      CASE
        WHEN AGY.POSITIONTITLE IN ('AGENCY','DISTRICT','BR_AGENCY','BR_DISTRICT')
        THEN AGY.AGY_TERMINATIONDATE
      END)DISSOLVED_DATE,
      SUBSTR(PAD.NAME,4) AGT_CODE,
      (PAD.firstname
      || PAD.lastname) NAME,
      DECODE(PAD.positiontitle, 'FSC_NON_PROCESS', 'FSC', PAD.positiontitle) ROLE,
      Pad.POS_GA4 CLASS,
      PAD.HIREDATE CONTRACT_DATE,
      PAD.AGY_APPOINTMENT_DATE APPOINTMENT_DATE,
      PAD.AGY_TERMINATIONDATE TERMINATION_DATE,
      (
      CASE
        WHEN pad.PT_GA1 = '00'
        THEN 'INFORCE'
        WHEN pad.PT_GA1 IN ('50','51','52','55','56')
        THEN 'TERMINATED'
        WHEN pad.PT_GA1 = '13'
        THEN 'TERMINATED'
        WHEN pad.PT_GA1 IN ('60','61')
        THEN 'TERMINATED'
        WHEN pad.PT_GA1 = '70'
        THEN 'TERMINATED'
      END) AGENT_STATUS,
      0 LIFE_FYP,
      0 PA_FYP,
      CASE
        WHEN PD.CD_GA2 IN ('LF','HS')
        AND PD.CD_CREDITTYPE ='Case_Count'
        THEN PD.TRANS_GN5
        ELSE 0
      END LIFE_CASE,
      CASE
        WHEN PD.CD_GA2 = 'PA'
        AND PD.CD_CREDITTYPE ='Case_Count'
        THEN PD.TRANS_GN5
        ELSE 0
      END PA_CASE,
      0 LIFE_FYC_API_SSC,
      0 PA_FYC,
      0 CS_FYC,
      0 LIFE_2YEAR,
      0 LIFE_3YEAR,
      0 LIFE_4YEAR,
      0 PA_RC,
      0 CS_RC,
      0 CS_PL,
      0 UNASSIGNED_LIFE_2YEAR,
      0 UNASSIGNED_LIFE_3YEAR,
      0 UNASSIGNED_LIFE_4YEAR,
      0 UNASSIGNED_PA_RC,
      0 LIFE_FYP_YTD,
      0 PA_FYP_YTD,
      0 LIFE_CASE_YTD,
      0 PA_CASE_YTD,
      0 LIFE_FYC_API_SSC_YTD,
      0 PA_FYC_YTD,
      0 CS_FYC_YTD,
      0 LIFE_2YEAR_YTD,
      0 LIFE_3YEAR_YTD,
      0 LIFE_4YEAR_YTD,
      0 PA_RC_YTD,
      0 CS_RC_YTD,
      0 CS_PL_YTD,
      0 UNASSIGNED_LIFE_2YEAR_YTD,
      0 UNASSIGNED_LIFE_3YEAR_YTD,
      0 UNASSIGNED_LIFE_4YEAR_YTD,
      0 UNASSIGNED_PA_RC_YTD
    FROM AIA_RPT_PRD_TEMP_PADIM pad,
         AIA_RPT_PRD_TEMP_PADIM agy,
         AIA_RPT_PRD_TEMP_PADIM agy_ldr,
         --AIA_RPT_PRD_TEMP_PADIM DISTRICT,
         AIA_RPT_PRD_TEMP_VALUES PD
         --for writing agency postion info
         inner join cs_position pos_agy
         on pos_agy.name = trim('SGY'||PD.CD_GA13)
         AND pos_agy.effectivestartdate <= PD.CD_GD2 --policy issue date
         AND pos_agy.effectiveenddate   > PD.CD_GD2  --policy issue date
         AND pos_agy.removedate =DT_REMOVEDATE
         --for writing district postion info
         inner join cs_position pos_dis
         on pos_dis.name=trim('SGY'||pos_agy.genericattribute3)
         AND pos_dis.effectivestartdate <= PD.CD_GD2 --policy issue date
         AND pos_dis.effectiveenddate   > PD.CD_GD2  --policy issue date
         AND pos_dis.removedate =DT_REMOVEDATE
         --for writing district participant info
         inner join cs_participant par_dis
         on par_dis.PAYEESEQ = pos_dis.PAYEESEQ
         AND par_dis.effectivestartdate < V_ENDDATE
         AND par_dis.effectiveenddate   >  V_ENDDATE-1
         AND par_dis.removedate = DT_REMOVEDATE
    WHERE PD.NEW_PRD_IND=1 AND PD.PRD_TYPE = 'DIRECT'
    AND  PD.SUBMITDATE >= V_submissionDate
    AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD','AM','FSD','FSM','BR_DM','BR_FSC','BR_UM')
    AND PAD.POS_GA4 NOT IN ('45','48')
    --AND PD.CD_POSITIONSEQ = PAD.POSITIONSEQ
    AND (
    (PD.businessunitmap = 1 and PD.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (PD.businessunitmap <> 1 and PD.CD_POSITIONSEQ = PAD.POSITIONSEQ)
    )
    --AND PD.CD_CREDITTYPE IN ('FYP', 'RYC', 'FYC','API','SSCP','APB','Case_Count')
    AND PD.CD_CREDITTYPE IN ('FYP', 'FYC', 'RYC', 'API', 'SSCP', 'APB', 'Case_Count',
                           'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                           'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                           'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                           'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
    --AND AGY.POS_GA1 = PD.TRANS_GA11
    AND AGY.POS_GA1 = PD.CD_GA13
    AND AGY.POSITIONTITLE      IN ('AGENCY','DISTRICT','BR_AGENCY','BR_DISTRICT')
    AND ((agy_ldr.NAME = 'SGT'
      || agy.POS_GA2
    AND agy.NAME LIKE 'SGY%')
    OR (agy_ldr.NAME = 'BRT'
      || agy.POS_GA2
    AND agy.NAME LIKE 'BRY%'))
    --AND DISTRICT.POS_GA3 = AGY.POS_GA3
    --AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
    --AND district.positiontitle     IN ('DISTRICT','BR_DISTRICT')
    AND PD.CD_GA2  IN ('LF','HS','PA'));
    */

        --Added by Suresh
        EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_DIST_WRI_NEW_HELP';

insert /*+APPEND*/ into AIA_RPT_PRD_DIST_WRI_NEW_HELP NOLOGGING
(select /*+   */
        decode(PD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') BUNAME,
        PD.businessunitmap BUMAP,
        PD.TRANS_GA11 UNIT_CODE,
        0 LIFE_FYC_API_SSC,
        0 LIFE_FYC_RP,
0 LIFE_FYC_NONILP,
0 LIFE_FYC_ILP,
0 LIFE_FYC_TOPUP,
        0 PA_FYC,
        0 CS_FYC,
        0 LIFE_2YEAR,
        0 LIFE_3YEAR,
        0 LIFE_4YEAR,
        0 PA_RC,
        SUBSTR(help1.NAME,4) DISTRICT_CODE,
        help1.firstname || ' ' || help1.lastname DM_NAME,
        help1.genericattribute2 DIST_LEADER_CODE,
        help1.genericattribute7 DIST_LEADER_NAME,
        help1.genericattribute11 DIST_LEAER_TITLE,
        help1.genericattribute4 DIST_LEADER_CLASS,
        PD.businessunitmap as businessunitmap,
        PD.CD_GA12 as CD_GA12,
        PD.CD_POSITIONSEQ as CD_POSITIONSEQ,

        PD.CD_GA13 as CD_GA13,
        CASE
          WHEN PD.CD_GA2 IN ('LF','HS')
          AND PD.CD_CREDITTYPE ='Case_Count'
          THEN PD.TRANS_GN5
          ELSE 0
        END LIFE_CASE,
        CASE
          WHEN PD.CD_GA2 = 'PA'
          AND PD.CD_CREDITTYPE ='Case_Count'
          THEN PD.TRANS_GN5
          ELSE 0
        END PA_CASE,
            0 UNASSIGNED_LIFE_2YEAR,
    0 UNASSIGNED_LIFE_3YEAR,
    0 UNASSIGNED_LIFE_4YEAR,
    0 UNASSIGNED_PA_RC

        from
        AIA_RPT_PRD_TEMP_VALUES PD
           --for writing agency postion info
           inner join cs_position pos_agy
           on pos_agy.name = trim('SGY'||PD.CD_GA13)
           AND pos_agy.effectivestartdate <= PD.CD_GD2 --policy issue date
           AND pos_agy.effectiveenddate   > PD.CD_GD2  --policy issue date
           AND pos_agy.removedate =DT_REMOVEDATE
           --for writing district postion info
           inner join AIA_RPT_PRD_DIST_WRI_NEW_HELP1 help1
           ON help1.name=trim('SGY'||pos_agy.genericattribute3)
           AND help1.effstartdt <= PD.CD_GD2 --policy issue date
           AND help1.effenddt   > PD.CD_GD2  --policy issue date
        WHERE PD.NEW_PIB_IND=1 AND PD.PIB_TYPE = 'DIRECT'
      AND  PD.SUBMITDATE >= V_submissionDate
      AND PD.CD_CREDITTYPE IN ('FYP', 'FYC', 'RYC', 'API', 'SSCP', 'APB', 'Case_Count',
                             'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                             'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                             'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                             'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
         AND PD.CD_GA2  IN ('LF','HS','PA'));

      Log('32 Records inserted into AIA_RPT_PRD_DIST_WRI_NEW_HELP 2 are '||SQL%ROWCOUNT);

COMMIT;

   BEGIN
        DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME => '"AIASEXT"' ,
                                        TABNAME => '"AIA_RPT_PRD_DIST_WRI_NEW_HELP"' ,
                                        ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE);
END;



  INSERT /*+ Append  */ INTO AIA_RPT_PRD_DIST_WRI_NEW NOLOGGING
(SELECT /*+  */
        V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
        V_PROCESSINGUNITNAME PUNAME,
        PD.BUNAME,
        PD.BUMAP,
        V_CALENDARSEQ CALENDARSEQ,
        V_CALENDARNAME CALENDARNAME,
        V_PERIODSEQ PERIODKEY,
        V_PERIODNAME periodname,
        --PAD.POSITIONSEQ POSITIONSEQ,
        PAD.MANAGERSEQ,
        PAD.POSITIONSEQ POSITIONSEQ,
        PAD.NAME,
        PD.DISTRICT_CODE,
        PD.DM_NAME,
        PD.DIST_LEADER_CODE,
        PD.DIST_LEADER_NAME,
        PD.DIST_LEAER_TITLE,
        PD.DIST_LEADER_CLASS,
        PD.UNIT_CODE,
        agy.firstname|| ' '|| agy.lastname AGENCY,
        agy.POS_GA2 UNIT_LEADER_CODE,
        agy.POS_GA7 UNIT_LEADER_NAME,
        DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
        agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
        (
        CASE
          WHEN AGY.POSITIONTITLE IN ('ORGANISATION','UNIT')
          THEN AGY.AGY_TERMINATIONDATE
        END)DISSOLVED_DATE,
        SUBSTR(PAD.NAME,4) AGT_CODE,
        (PAD.firstname
        || PAD.lastname) NAME,
        DECODE(PAD.positiontitle, 'FSC_NON_PROCESS', 'FSC', PAD.positiontitle) ROLE,
        Pad.POS_GA4 CLASS,
        PAD.HIREDATE CONTRACT_DATE,
        PAD.AGY_APPOINTMENT_DATE APPOINTMENT_DATE,
        PAD.AGY_TERMINATIONDATE TERMINATION_DATE,
        (
        CASE
          WHEN pad.PT_GA1 = '00'
          THEN 'INFORCE'
          WHEN pad.PT_GA1 IN ('50','51','52','55','56')
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 = '13'
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 IN ('60','61')
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 = '70'
          THEN 'TERMINATED'
        END) AGENT_STATUS,

        --removed from report
        0 as LIFE_FYP,
        0 as PA_FYP,
        PD.LIFE_CASE,
        PD.PA_CASE,

        PD.LIFE_FYC_API_SSC,
        PD.LIFE_FYC_RP,
 PD.LIFE_FYC_NONILP,
 PD.LIFE_FYC_ILP,
 PD.LIFE_FYC_TOPUP,

        PD.PA_FYC,
        PD.CS_FYC,
        PD.LIFE_2YEAR,
        PD.LIFE_3YEAR,
        PD.LIFE_4YEAR,
        PD.PA_RC,

        --removed from report
        0 as CS_RC,
        0 as CS_PL,

        0 UNASSIGNED_LIFE_2YEAR,
        0 UNASSIGNED_LIFE_3YEAR,
        0 UNASSIGNED_LIFE_4YEAR,
        0 UNASSIGNED_PA_RC,
        0 LIFE_FYP_YTD,
        0 PA_FYP_YTD,
        0 LIFE_CASE_YTD,
        0 PA_CASE_YTD,
        0 LIFE_FYC_API_SSC_YTD,
        0 LIFE_FYC_RP_YTD,
 0 LIFE_FYC_NONILP_YTD,
 0 LIFE_FYC_ILP_YTD,
 0 LIFE_FYC_TOPUP_YTD ,
        0 PA_FYC_YTD,
        0 CS_FYC_YTD,
        0 LIFE_2YEAR_YTD,
        0 LIFE_3YEAR_YTD,
        0 LIFE_4YEAR_YTD,
        0 PA_RC_YTD,
        0 CS_RC_YTD,
        0 CS_PL_YTD,
        0 UNASSIGNED_LIFE_2YEAR_YTD,
        0 UNASSIGNED_LIFE_3YEAR_YTD,
        0 UNASSIGNED_LIFE_4YEAR_YTD,
        0 UNASSIGNED_PA_RC_YTD
      FROM AIA_RPT_PRD_TEMP_PADIM pad,
           AIA_RPT_PRD_TEMP_PADIM agy,
           AIA_RPT_PRD_TEMP_PADIM agy_ldr,
           --AIA_RPT_PRD_TEMP_PADIM DISTRICT,
           AIA_RPT_PRD_DIST_WRI_NEW_HELP PD
      WHERE  PAD.POSITIONTITLE IN ('AD','DIR','ED','FC','SD')
      AND PAD.POS_GA4 NOT IN ('45','48')
      --AND PD.CD_POSITIONSEQ = PAD.POSITIONSEQ
      AND (
      (PD.businessunitmap = 64 and PD.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (PD.businessunitmap <> 64 and PD.CD_POSITIONSEQ = PAD.POSITIONSEQ)
      )
      AND AGY.POS_GA1 = PD.CD_GA13
      AND AGY.POSITIONTITLE      IN ('ORGANISATION','UNIT')
      AND ((agy_ldr.NAME = 'SGT'
        || agy.POS_GA2
      AND agy.NAME LIKE 'SGY%')
     )
      --AND DISTRICT.POS_GA3 = AGY.POS_GA3
      --AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
      --AND district.positiontitle     IN ('DISTRICT','BR_DISTRICT')
);
     --End by Suresh

 Log('32 Records inserted into AIA_RPT_PRD_DIST_WRI_NEW are ' || SQL%ROWCOUNT);
       /* End of above Insert to capture CASE COUNT Of Brunei Records which doesn't CreditType = 'FYP' coming from 'Case_Count */
      COMMIT;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_NEW_PRD_WRI' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_NEW_PRD_WRI' , 'AIA_RPT_PRD_DIST_WRI_NEW 3'  , 'Processing', 'insert into AIA_RPT_PRD_DIST_WRI_NEW');


------=======ADD LOG

--- Adding Unassigned RC Logic below.
/*Commented by Suresh
INSERT
    /*+ Append */
  /*INTO AIA_RPT_PRD_DIST_WRI_NEW NOLOGGING
( SELECT
      V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
      V_PROCESSINGUNITNAME PUNAME,
      decode(PD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') BUNAME,
      PD.businessunitmap BUMAP,
      V_CALENDARSEQ CALENDARSEQ,
      V_CALENDARNAME CALENDARNAME,
      V_PERIODSEQ PERIODKEY,
      V_PERIODNAME periodname,
      --PAD_AGT.POSITIONSEQ POSITIONSEQ,
      PAD_AGT.MANAGERSEQ,
      PAD_AGT.POSITIONSEQ POSITIONSEQ,
      pad_agt.NAME,
/*      SUBSTR(DISTRICT.NAME,4) DISTRICT_CODE,
      district.firstname
      || ' '
      || district.lastname DM_NAme,
      district.POS_GA2 DIST_LEADER_CODE,
      district.POS_GA7 DIST_LEADER_NAME,
      district.POS_GA11 DIST_LEAER_TITLE,
      DISTRICT.POS_GA4 DIST_LEADER_CLASS,*/
   /*     SUBSTR(pos_dis.NAME,4) DISTRICT_CODE,
     par_dis.firstname || ' ' || par_dis.lastname DM_NAME,
     pos_dis.genericattribute2 DIST_LEADER_CODE,
     pos_dis.genericattribute7 DIST_LEADER_NAME,
     pos_dis.genericattribute11 DIST_LEAER_TITLE,
     pos_dis.genericattribute4 DIST_LEADER_CLASS,
     PD.CD_GA13 UNIT_CODE,
     agy.firstname
     || ' '
     || agy.lastname AGENCY,
     agy_ldr.POS_GA2 UNIT_LEADER_CODE,
     agy_ldr.POS_GA7 UNIT_LEADER_NAME,
     DECODE(agy_ldr.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy_ldr.POS_GA11) UNIT_LEAER_TITLE,
     agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
     (
     CASE
       WHEN agy.POSITIONTITLE IN ('AGENCY','DISTRICT','BR_AGENCY','BR_DISTRICT')
       THEN agy.AGY_TERMINATIONDATE
     END)DISSOLVED_DATE,
     SUBSTR(pad_agt.NAME,4) AGT_CODE,
     (pad_agt.firstname
     || pad_agt.lastname) NAME,
     DECODE(pad_agt.positiontitle, 'FSC_NON_PROCESS', 'FSC', pad_agt.positiontitle) ROLE,
     pad_agt.POS_GA4 CLASS,
     pad_agt.HIREDATE CONTRACT_DATE,
     PAD.AGY_APPOINTMENT_DATE APPOINTMENT_DATE,
     pad_agt.AGY_TERMINATIONDATE TERMINATION_DATE,
     (
     CASE
     WHEN pad_agt.PT_GA1 = '00'
       THEN 'INFORCE'
       WHEN pad_agt.PT_GA1 IN ('50','51','52','55','56')
       THEN 'TERMINATED'
       WHEN pad_agt.PT_GA1 = '13'
       THEN 'TERMINATED'
       WHEN pad_agt.PT_GA1 IN ('60','61')
       THEN 'TERMINATED'
       WHEN pad_agt.PT_GA1 = '70'
       THEN 'TERMINATED'
     END) AGENT_STATUS,
     0 LIFE_FYP,
     0 PA_FYP,
     0 LIFE_CASE,
     0 PA_CASE,
     0 LIFE_FYC_API_SSC,
     0 PA_FYC,
     0 CS_FYC,
     0 LIFE_2YEAR,
     0 LIFE_3YEAR,
     0 LIFE_4YEAR,
     0 PA_RC,
     0 CS_RC,
     0 CS_PL,
     CASE
       WHEN PD.CD_GA4 = 'PAY2'
       AND PD.CD_GA2  IN ('LF','HS')
       THEN PD.CD_VALUE
       ELSE 0
     END UNASSIGNED_LIFE_2YEAR,
     CASE
       WHEN PD.CD_GA4 = 'PAY3'
       AND PD.CD_GA2   IN ('LF','HS')
       THEN PD.CD_VALUE
       ELSE 0
     END UNASSIGNED_LIFE_3YEAR,
     CASE
       WHEN PD.CD_GA4 IN ('PAY4','PAY5','PAY6')
       AND PD.CD_GA2   IN ('LF','HS')
       THEN PD.CD_VALUE
       ELSE 0
     END UNASSIGNED_LIFE_4YEAR,
     CASE
       WHEN PD.CD_GA2 IN ('PA')--,'VL')
       THEN PD.CD_VALUE
       ELSE 0
     END UNASSIGNED_PA_RC,
     0 LIFE_FYP_YTD,
     0 PA_FYP_YTD,
     0 LIFE_CASE_YTD,
     0 PA_CASE_YTD,
     0 LIFE_FYC_API_SSC_YTD,
     0 PA_FYC_YTD,
     0 CS_FYC_YTD,
     0 LIFE_2YEAR_YTD,
     0 LIFE_3YEAR_YTD,
     0 LIFE_4YEAR_YTD,
     0 PA_RC_YTD,
     0 CS_RC_YTD,
     0 CS_PL_YTD,
     0 UNASSIGNED_LIFE_2YEAR_YTD,
     0 UNASSIGNED_LIFE_3YEAR_YTD,
     0 UNASSIGNED_LIFE_4YEAR_YTD,
     0 UNASSIGNED_PA_RC_YTD
     FROM
     AIA_RPT_PRD_TEMP_PADIM PAD,
     AIA_RPT_PRD_TEMP_PADIM pad_agt,
     AIA_RPT_PRD_TEMP_PADIM agy,
     AIA_RPT_PRD_TEMP_PADIM AGY_LDR,
     --AIA_RPT_PRD_TEMP_PADIM DISTRICT,
     AIA_RPT_PRD_TEMP_VALUES PD
     --for writing agency postion info
     inner join cs_position pos_agy
     on pos_agy.name = trim('SGY'||PD.CD_GA12)
     AND pos_agy.effectivestartdate <= PD.CD_GD2 --policy issue date
     AND pos_agy.effectiveenddate   > PD.CD_GD2  --policy issue date
     AND pos_agy.removedate =DT_REMOVEDATE
     --for writing district postion info
     inner join cs_position pos_dis
     on pos_dis.name=trim('SGY'||pos_agy.genericattribute3)
     AND pos_dis.effectivestartdate <= PD.CD_GD2 --policy issue date
     AND pos_dis.effectiveenddate   > PD.CD_GD2  --policy issue date
     AND pos_dis.removedate =DT_REMOVEDATE
     --for writing district participant info
     inner join cs_participant par_dis
     on par_dis.PAYEESEQ = pos_dis.PAYEESEQ
     AND par_dis.effectivestartdate < V_ENDDATE
     AND par_dis.effectiveenddate   >  V_ENDDATE-1
     AND par_dis.removedate = DT_REMOVEDATE
 WHERE PD.NEW_PRD_IND=1 AND PD.PRD_TYPE = 'DIRECT'
   AND  PD.SUBMITDATE >= V_submissionDate
   --AND PD.CD_POSITIONSEQ = PAD.POSITIONSEQ
   AND (
   (PD.businessunitmap = 1 and PD.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (PD.businessunitmap <> 1 and PD.CD_POSITIONSEQ = PAD.POSITIONSEQ)
   )
   --AND PD.CD_CREDITTYPE  IN ('ORYC', 'RYC','Case_Count')
   AND PD.CD_CREDITTYPE IN ('RYC', 'ORYC',
                          'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE',
                          'ORYC_W', 'ORYC_W_DUPLICATE', 'ORYC_WC_DUPLICATE')
   AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD','AM','FSD','FSM','BR_DM','BR_FSC','BR_UM')
   AND PAD.POS_GA4 NOT IN ('45')--,'48')
   AND AGY.POS_GA1 = PD.CD_GA13
   --AND SUBSTR(PAD.NAME,4)  = PD.TRANS_GA11
   --AND SUBSTR(PAD_AGT.NAME,4) = PD.TRANS_GA10
   AND SUBSTR(PAD.NAME,4)  = PD.CD_GA13
   AND SUBSTR(PAD_AGT.NAME,4) = PD.CD_GA12 and SUBSTR(PAD_AGT.NAME,3,1)='T'
   AND AGY.POSITIONTITLE      IN ('AGENCY','DISTRICT','BR_AGENCY','BR_DISTRICT')
   AND ((agy_ldr.NAME = 'SGT'
     || agy.POS_GA2
   AND agy.NAME LIKE 'SGY%')
   OR (agy_ldr.NAME = 'BRT'
     || agy.POS_GA2
   AND agy.NAME LIKE 'BRY%'))
   --AND DISTRICT.POS_GA3 = AGY.POS_GA3
   --AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
   --AND district.positiontitle     IN ('DISTRICT','BR_DISTRICT')
   AND PD.CD_GA2   IN ('LF','PA','HS','VL')
  -- AND PD.CD_GA4     IN ('PAY2','PAY3','PAY4','PAY5','PAY6')
);
*/

--Added by Suresh
  EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_DIST_WRI_NEW_HELP';

insert /*+APPEND*/ into AIA_RPT_PRD_DIST_WRI_NEW_HELP NOLOGGING
(select /*+   */
        decode(PD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') BUNAME,
        PD.businessunitmap BUMAP,
        PD.CD_GA13 UNIT_CODE,
        0 LIFE_FYC_API_SSC,
        0 LIFE_FYC_RP,
 0 LIFE_FYC_NONILP,
 0 LIFE_FYC_ILP,
 0 LIFE_FYC_TOPUP,
        0 PA_FYC,
        0 CS_FYC,
        0 LIFE_2YEAR,
        0 LIFE_3YEAR,
        0 LIFE_4YEAR,
        0 PA_RC,
        SUBSTR(help1.NAME,4) DISTRICT_CODE,
        help1.firstname || ' ' || help1.lastname DM_NAME,
        help1.genericattribute2 DIST_LEADER_CODE,
        help1.genericattribute7 DIST_LEADER_NAME,
        help1.genericattribute11 DIST_LEAER_TITLE,
        help1.genericattribute4 DIST_LEADER_CLASS,
        PD.businessunitmap as businessunitmap,
        PD.CD_GA12 as CD_GA12,
        PD.CD_POSITIONSEQ as CD_POSITIONSEQ,

        PD.CD_GA13 as CD_GA13,
        0 LIFE_CASE,
        0 PA_CASE,

        CASE
          WHEN PD.CD_GA4 = 'PAY2'
          AND PD.CD_GA2  IN ('LF','HS')
          THEN PD.CD_VALUE
          ELSE 0
        END UNASSIGNED_LIFE_2YEAR,
        CASE
          WHEN PD.CD_GA4 = 'PAY3'
          AND PD.CD_GA2   IN ('LF','HS')
          THEN PD.CD_VALUE
          ELSE 0
        END UNASSIGNED_LIFE_3YEAR,
        CASE
          WHEN PD.CD_GA4 IN ('PAY4','PAY5','PAY6')
          AND PD.CD_GA2   IN ('LF','HS')
          THEN PD.CD_VALUE
          ELSE 0
        END UNASSIGNED_LIFE_4YEAR,
        CASE
          WHEN PD.CD_GA2 IN ('PA')--,'VL')
          THEN PD.CD_VALUE
          ELSE 0
        END UNASSIGNED_PA_RC

        from
        AIA_RPT_PRD_TEMP_VALUES PD
        --for writing agency postion info
        inner join cs_position pos_agy
        on pos_agy.name = trim('SGY'||PD.CD_GA12)
        AND pos_agy.effectivestartdate <= PD.CD_GD2 --policy issue date
        AND pos_agy.effectiveenddate   > PD.CD_GD2  --policy issue date
        AND pos_agy.removedate =DT_REMOVEDATE
        --for writing district postion info
        inner join AIA_RPT_PRD_DIST_WRI_NEW_HELP1 help1
           ON help1.name=trim('SGY'||pos_agy.genericattribute3)
           AND help1.effstartdt <= PD.CD_GD2 --policy issue date
           AND help1.effenddt   > PD.CD_GD2  --policy issue date
 WHERE PD.NEW_PIB_IND=1 AND PD.PIB_TYPE = 'DIRECT'
      AND  PD.SUBMITDATE >= V_submissionDate
      --AND PD.CD_POSITIONSEQ = PAD.POSITIONSEQ
      AND PD.CD_CREDITTYPE IN ('RYC', 'ORYC',
                             'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE',
                             'ORYC_W', 'ORYC_W_DUPLICATE', 'ORYC_WC_DUPLICATE')
      AND PD.CD_GA2   IN ('LF','PA','HS','VL')
);

Log('33 Records inserted into AIA_RPT_PRD_DIST_WRI_NEW_HELP 3 are '||SQL%ROWCOUNT);
COMMIT;

   BEGIN
        DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME => '"AIASEXT"' ,
                                        TABNAME => '"AIA_RPT_PRD_DIST_WRI_NEW_HELP"' ,
                                        ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE);
END;

--- Adding Unassigned RC Logic below.
INSERT
    /*+ Append */
  INTO AIA_RPT_PRD_DIST_WRI_NEW NOLOGGING
( SELECT
        V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
        V_PROCESSINGUNITNAME PUNAME,
        PD.BUNAME,
        PD.BUMAP,
        V_CALENDARSEQ CALENDARSEQ,
        V_CALENDARNAME CALENDARNAME,
        V_PERIODSEQ PERIODKEY,
        V_PERIODNAME periodname,
        --PAD_AGT.POSITIONSEQ POSITIONSEQ,
        PAD_AGT.MANAGERSEQ,
        PAD_AGT.POSITIONSEQ POSITIONSEQ,
        pad_agt.NAME,
/*      SUBSTR(DISTRICT.NAME,4) DISTRICT_CODE,
        district.firstname
        || ' '
        || district.lastname DM_NAme,
        district.POS_GA2 DIST_LEADER_CODE,
        district.POS_GA7 DIST_LEADER_NAME,
        district.POS_GA11 DIST_LEAER_TITLE,
        DISTRICT.POS_GA4 DIST_LEADER_CLASS,*/
        PD.DISTRICT_CODE,
        PD.DM_NAME,
        PD.DIST_LEADER_CODE,
        PD.DIST_LEADER_NAME,
        PD.DIST_LEAER_TITLE,
        PD.DIST_LEADER_CLASS,
        PD.UNIT_CODE,
        agy.firstname || ' ' || agy.lastname AGENCY,
        agy_ldr.POS_GA2 UNIT_LEADER_CODE,
        agy_ldr.POS_GA7 UNIT_LEADER_NAME,
        DECODE(agy_ldr.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy_ldr.POS_GA11) UNIT_LEAER_TITLE,
        agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
        (
        CASE
          WHEN agy.POSITIONTITLE IN ('ORGANISATION','UNIT')
          THEN agy.AGY_TERMINATIONDATE
        END)DISSOLVED_DATE,
        SUBSTR(pad_agt.NAME,4) AGT_CODE,
        (pad_agt.firstname
        || pad_agt.lastname) NAME,
        DECODE(pad_agt.positiontitle, 'FSC_NON_PROCESS', 'FSC', pad_agt.positiontitle) ROLE,
        pad_agt.POS_GA4 CLASS,
        pad_agt.HIREDATE CONTRACT_DATE,
        PAD.AGY_APPOINTMENT_DATE APPOINTMENT_DATE,
        pad_agt.AGY_TERMINATIONDATE TERMINATION_DATE,
        (
        CASE
        WHEN pad_agt.PT_GA1 = '00'
          THEN 'INFORCE'
          WHEN pad_agt.PT_GA1 IN ('50','51','52','55','56')
          THEN 'TERMINATED'
          WHEN pad_agt.PT_GA1 = '13'
          THEN 'TERMINATED'
          WHEN pad_agt.PT_GA1 IN ('60','61')
          THEN 'TERMINATED'
          WHEN pad_agt.PT_GA1 = '70'
          THEN 'TERMINATED'
        END) AGENT_STATUS,
        0 LIFE_FYP,
        0 PA_FYP,
        0 LIFE_CASE,
        0 PA_CASE,
        0 LIFE_FYC_API_SSC,
        0 LIFE_FYC_RP,
0 LIFE_FYC_NONILP,
0 LIFE_FYC_ILP,
0 LIFE_FYC_TOPUP,
        0 PA_FYC,
        0 CS_FYC,
        0 LIFE_2YEAR,
        0 LIFE_3YEAR,
        0 LIFE_4YEAR,
        0 PA_RC,
        0 CS_RC,
        0 CS_PL,
        PD.UNASSIGNED_LIFE_2YEAR,
        PD.UNASSIGNED_LIFE_3YEAR,
        PD.UNASSIGNED_LIFE_4YEAR,
        PD.UNASSIGNED_PA_RC,
        0 LIFE_FYP_YTD,
        0 PA_FYP_YTD,
        0 LIFE_CASE_YTD,
        0 PA_CASE_YTD,
        0 LIFE_FYC_API_SSC_YTD,
        0 LIFE_FYC_RP_YTD,
 0 LIFE_FYC_NONILP_YTD,
 0 LIFE_FYC_ILP_YTD,
 0 LIFE_FYC_TOPUP_YTD ,
        0 PA_FYC_YTD,
        0 CS_FYC_YTD,
        0 LIFE_2YEAR_YTD,
        0 LIFE_3YEAR_YTD,
        0 LIFE_4YEAR_YTD,
        0 PA_RC_YTD,
        0 CS_RC_YTD,
        0 CS_PL_YTD,
        0 UNASSIGNED_LIFE_2YEAR_YTD,
        0 UNASSIGNED_LIFE_3YEAR_YTD,
        0 UNASSIGNED_LIFE_4YEAR_YTD,
        0 UNASSIGNED_PA_RC_YTD
        FROM
        AIA_RPT_PRD_TEMP_PADIM PAD,
        AIA_RPT_PRD_TEMP_PADIM pad_agt,
        AIA_RPT_PRD_TEMP_PADIM agy,
        AIA_RPT_PRD_TEMP_PADIM AGY_LDR,
        --AIA_RPT_PRD_TEMP_PADIM DISTRICT,
        AIA_RPT_PRD_DIST_WRI_NEW_HELP PD
WHERE (
      (PD.businessunitmap = 1 and PD.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (PD.businessunitmap <> 1 and PD.CD_POSITIONSEQ = PAD.POSITIONSEQ)
      )
      AND PAD.POSITIONTITLE IN ('AD','DIR','ED','FC','SD')
      AND PAD.POS_GA4 NOT IN ('45')--,'48')
      AND AGY.POS_GA1 = PD.CD_GA13
      --AND SUBSTR(PAD.NAME,4)  = PD.TRANS_GA11
      --AND SUBSTR(PAD_AGT.NAME,4) = PD.TRANS_GA10
      AND SUBSTR(PAD.NAME,4)  = PD.CD_GA13
      AND SUBSTR(PAD_AGT.NAME,4) = PD.CD_GA12 and SUBSTR(PAD_AGT.NAME,3,1)='T'
      AND AGY.POSITIONTITLE      IN ('ORGANISATION','UNIT')
      AND ((agy_ldr.NAME = 'SGT'
        || agy.POS_GA2
      AND agy.NAME LIKE 'SGY%')
    )
);
Log('33 Records inserted into AIA_RPT_PRD_DIST_WRI_NEW 3 are '||SQL%ROWCOUNT);
COMMIT;


--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_NEW_PRD_WRI' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_NEW_PRD_WRI' , 'AIA_RPT_NEW_PRD_DIST_WRI_TMP'  , 'Processing', 'insert into AIA_RPT_NEW_PRD_DIST_WRI_TMP');


------=======ADD LOG
----------------line 622
DELETE
  FROM AIA_RPT_NEW_PRD_DIST_WRI_TMP WHERE PERIODNAME = V_PERIODNAME ;

  COMMIT;


INSERT /*+ Append */
  INTO AIA_RPT_NEW_PRD_DIST_WRI_TMP NOLOGGING
  SELECT MAX(PROCESSINGUNITSEQ) PROCESSINGUNITSEQ,
    MAX(PUNAME) PUNAME,
    BUNAME,
    BUMAP,
    MAX(CALENDARSEQ) CALENDARSEQ,
    MAX(CALENDARNAME) CALENDARNAME,
    MAX(PERIODKEY) PERIODKEY,
    MAX(PERIODNAME) PERIODNAME,
    --MAX(POSITIONSEQ) POSITIONSEQ,
    MAX(MANAGERSEQ) MANAGERSEQ,
    MAX(POSITIONSEQ) POSITIONSEQ,
    POSITIONNAME,
    DISTRICT_CODE,
    MAX(DM_NAME) DM_NAME,
    MAX(DIST_LEADER_CODE) DIST_LEADER_CODE,
    MAX(DIST_LEADER_NAME) DIST_LEADER_NAME,
    MAX(DIST_LEAER_TITLE) DIST_LEAER_TITLE,
    MAX(DIST_LEADER_CLASS) DIST_LEADER_CLASS,
    UNIT_CODE UNIT_CODE,
    MAX(AGENCY) AGENCY,
    MAX(UNIT_LEADER_CODE) UNIT_LEADER_CODE ,
    MAX(UNIT_LEADER_NAME) UNIT_LEADER_NAME,
    MAX(UNIT_LEAER_TITLE) UNIT_LEAER_TITLE,
    MAX(UNIT_LEADER_CLASS) UNIT_LEADER_CLASS,
    MAX(DISSOLVED_DATE) DISSOLVED_DATE,
    AGT_CODE AGT_CODE,
    MAX(NAME) NAME,
    MAX(ROLE) ROLE ,
    MAX(CLASS) CLASS,
    MAX(CONTRACT_DATE) CONTRACT_DATE,
    MAX(APPOINTMENT_DATE) APPOINTMENT_DATE,
    MAX(TERMINATION_DATE) TERMINATION_DATE,
    MAX(AGENT_STATUS) AGENT_STATUS,
    SUM(LIFE_FYC_API_SSC) LIFE_FYC_API_SSC,
    SUM(LIFE_2YEAR) LIFE_2YEAR,
    SUM(LIFE_3YEAR) LIFE_3YEAR,
    SUM(LIFE_4YEAR) LIFE_4YEAR,
    SUM(UNASSIGNED_LIFE_2YEAR) UNASSIGNED_LIFE_2YEAR,
    SUM(UNASSIGNED_LIFE_3YEAR) UNASSIGNED_LIFE_3YEAR,
    SUM(UNASSIGNED_LIFE_4YEAR) UNASSIGNED_LIFE_4YEAR,
    SUM(LIFE_FYC_API_SSC_YTD) LIFE_FYC_API_SSC_YTD,
    SUM(LIFE_2YEAR_YTD) LIFE_2YEAR_YTD,
    SUM(LIFE_3YEAR_YTD) LIFE_3YEAR_YTD,
    SUM(LIFE_4YEAR_YTD) LIFE_4YEAR_YTD,
    SUM(UNASSIGNED_LIFE_2YEAR_YTD) UNASSIGNED_LIFE_2YEAR_YTD,
    SUM(UNASSIGNED_LIFE_3YEAR_YTD) UNASSIGNED_LIFE_3YEAR_YTD,
    SUM(UNASSIGNED_LIFE_4YEAR_YTD) UNASSIGNED_LIFE_4YEAR_YTD
  FROM
    (SELECT PROCESSINGUNITSEQ,
      PUNAME,
      BUNAME,
      BUMAP,
      CALENDARSEQ,
      CALENDARNAME,
      PERIODKEY,
      periodname,
      --POSITIONSEQ,
      MANAGERSEQ,
      POSITIONSEQ,
      POSITIONNAME,
      DISTRICT_CODE,
      DM_NAme,
      dIST_LEADER_CODE,
      DIST_LEADER_NAME,
      DIST_LEAER_TITLE,
      DIST_LEADER_CLASS,
      UNIT_CODE,
      AGENCY,
      UNIT_LEADER_CODE,
      UNIT_LEADER_NAME,
      UNIT_LEAER_TITLE,
      UNIT_LEADER_CLASS,
      DISSOLVED_DATE,
      AGT_CODE,
      NAME,
      ROLE,
      CLASS,
      CONTRACT_DATE,
      APPOINTMENT_DATE,
      TERMINATION_DATE,
      AGENT_STATUS,
      LIFE_FYC_API_SSC,
      LIFE_2YEAR,
      LIFE_3YEAR,
      LIFE_4YEAR,
      UNASSIGNED_LIFE_2YEAR,
       UNASSIGNED_LIFE_3YEAR,
      UNASSIGNED_LIFE_4YEAR,
      LIFE_FYC_API_SSC_YTD ,
      LIFE_2YEAR_YTD ,
       LIFE_3YEAR_YTD ,
      LIFE_4YEAR_YTD ,
      UNASSIGNED_LIFE_2YEAR_YTD ,
     UNASSIGNED_LIFE_3YEAR_YTD ,
      UNASSIGNED_LIFE_4YEAR_YTD
    FROM AIA_RPT_PRD_DIST_WRI_NEW

    UNION ALL
    --- Adding YTD Logic.
    SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
      PUNAME,
      BUNAME,
      BUMAP,
      CALENDARSEQ,
      CALENDARNAME,
      V_PERIODSEQ PERIODKEY,
      V_PERIODNAME periodname,
      --POSITIONSEQ,
      MANAGERSEQ,
      POSITIONSEQ,
      POSITIONNAME,
      DISTRICT_CODE,
      DM_NAme,
      dIST_LEADER_CODE,
      DIST_LEADER_NAME,
      DIST_LEAER_TITLE,
      DIST_LEADER_CLASS,
      UNIT_CODE,
      AGENCY,
      UNIT_LEADER_CODE,
      UNIT_LEADER_NAME,
      UNIT_LEAER_TITLE,
      UNIT_LEADER_CLASS,
      to_date(DISSOLVED_DATE, 'mm/dd/yyyy') DISSOLVED_DATE,
      AGT_CODE,
      NAME,
      ROLE,
      CLASS,
      CONTRACT_DATE,
      APPOINTMENT_DATE,
      TERMINATION_DATE,
      AGENT_STATUS,
      0 LIFE_FYC_API_SSC,
      0 LIFE_2YEAR,
      0 LIFE_3YEAR,
      0 LIFE_4YEAR,
      0 UNASSIGNED_LIFE_2YEAR,
      0 UNASSIGNED_LIFE_3YEAR,
      0 UNASSIGNED_LIFE_4YEAR,
      LIFE_FYC_API_SSC_YTD,
      LIFE_2YEAR_YTD,
      LIFE_3YEAR_YTD,
      LIFE_4YEAR_YTD,
      UNASSIGNED_LIFE_2YEAR_YTD,
      UNASSIGNED_LIFE_3YEAR_YTD,
      UNASSIGNED_LIFE_4YEAR_YTD
    FROM Aia_Rpt_NEW_PRD_DIST_WRI
    WHERE periodname =
      (SELECT name
      FROM CS_PERIOD
      WHERE periodtypeseq = V_PERIODTYPESEQ
      AND calendarseq = V_CALENDARSEQ
      AND V_PERIODNAME NOT LIKE 'Dec%'
      AND ENDDATE =
        (SELECT STARTDATE
        FROM CS_PERIOD
        WHERE NAME = V_PERIODNAME
        AND CALENDARSEQ = V_CALENDARSEQ
        )
      )
    )
  GROUP BY DISTRICT_CODE,
  BUNAME,
  BUMAP,
  POSITIONNAME,
  UNIT_CODE,
  AGT_CODE   ;


COMMIT;

------=======ADD LOG

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_NEW_PRD_WRI' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_NEW_PRD_WRI' , 'BUILD HIERARCHY LIST'  , 'Processing', '');


-----------start from line 880
EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_AGENCY_LIST';

INSERT /*+ Append  */
INTO AIA_RPT_AGENCY_LIST NOLOGGING
  (AGY_CODE,
   AGY_PARTICIPANTID,
   AGY_NAME,
   AGY_POSITIONTITLE,
   AGY_POS_GA4,
   AGY_POS_GA2,
   AGY_POS_GA3,
   AGY_TERMINATIONDATE,
   AGY_APPOINTMENT_DATE,
   AGY_POS_GA9,
   NEW_DISTRICT_CODE,
   AGY_BUNAME)
  (select CHILDPAD.POS_GA1 AGY_CODE,
          CHILDPAD.name AGY_PARTICIPANTID,
          CHILDPAD.FIRSTNAME || ' ' || CHILDPAD.LASTNAME AGY_NAME,
          DECODE(CHILDPAD.POSITIONTITLE,
                 'FSC_NON_PROCESS',
                 'FSC',
                 'BR_FSC_NON_PROCESS',
                 'FSC',
                 'AM_FSC_NON_PROCESS',
                 'FSC',
                 'AM_NON_PROCESS',
                 'AM',
                 'FSD_NON_PROCESS',
                 'FSD',
                 'FSAD_NON_PROCESS',
                 'FSAD',
                 'BR_Staff',
                 'Staff',
                 'BR_DM',
                 'DM',
                 'BR_DM_NON_PROCESS',
                 'DM',
                 'BR_FSC',
                 'FSC',
                 'BR_Staff_NON_PROCESS',
                 'Staff',
                 'BR_UM',
                 'UM',
                 'BR_UM_NON_PROCESS',
                 'UM',
                 'Staff_NON_PROCESS',
                 'Staff',
                 CHILDPAD.POSITIONTITLE) AGY_POSITIONTITLE,
          CHILDPAD.POS_GA4 AGY_POS_GA4,
          CHILDPAD.POS_GA2 AGY_POS_GA2,
          CHILDPAD.POS_GA3 AGY_POS_GA3,
          CHILDPAD.AGY_TERMINATIONDATE AGY_TERMINATIONDATE,
          CHILDPAD.AGY_APPOINTMENT_DATE AGY_APPOINTMENT_DATE,
          CHILDPAD.POS_GA9 AGY_POS_GA9,
          PARENTPAD.POS_GA3 NEW_DISTRICT_CODE,
          decode(CHILDPAD.businessunitmap,
                 v_BUSINESSUNITMAP1,
                 'SGPAFA',
                 v_BUSINESSUNITMAP2,
                 'BRUAGY') AGY_BUNAME
     from AIA_RPT_PRD_TEMP_PADIM CHILDPAD
     INNER JOIN cs_positionrelation PR
     ON CHILDPAD.positionseq = PR.childpositionseq
     AND PR.positionrelationtypeseq =
          (select datatypeseq
             from cs_positionrelationtype
            where name = 'PBA_Roll'
              and removedate = V_EOT)
     INNER JOIN AIA_RPT_PRD_TEMP_PADIM PARENTPAD
     ON PARENTPAD.positionseq = PR.parentpositionseq
     WHERE CHILDPAD.POSITIONTITLE IN ('ORGANISATION','UNIT')
     AND CHILDPAD.POS_GA9 = 'Y'

     UNION

     select PAD.POS_GA1 AGY_CODE,
          PAD.name AGY_PARTICIPANTID,
          PAD.FIRSTNAME || ' ' || PAD.LASTNAME AGY_NAME,
          DECODE(PAD.POSITIONTITLE,
                 'FSC_NON_PROCESS',
                 'FSC',
                 'BR_FSC_NON_PROCESS',
                 'FSC',
                 'AM_FSC_NON_PROCESS',
                 'FSC',
                 'AM_NON_PROCESS',
                 'AM',
                 'FSD_NON_PROCESS',
                 'FSD',
                 'FSAD_NON_PROCESS',
                 'FSAD',
                 'BR_Staff',
                 'Staff',
                 'BR_DM',
                 'DM',
                 'BR_DM_NON_PROCESS',
                 'DM',
                 'BR_FSC',
                 'FSC',
                 'BR_Staff_NON_PROCESS',
                 'Staff',
                 'BR_UM',
                 'UM',
                 'BR_UM_NON_PROCESS',
                 'UM',
                 'Staff_NON_PROCESS',
                 'Staff',
                 PAD.POSITIONTITLE) AGY_POSITIONTITLE,
          PAD.POS_GA4 AGY_POS_GA4,
          PAD.POS_GA2 AGY_POS_GA2,
          PAD.POS_GA3 AGY_POS_GA3,
          PAD.AGY_TERMINATIONDATE AGY_TERMINATIONDATE,
          PAD.AGY_APPOINTMENT_DATE AGY_APPOINTMENT_DATE,
          PAD.POS_GA9 AGY_POS_GA9,
          PAD.POS_GA3 NEW_DISTRICT_CODE,
          decode(PAD.businessunitmap,
                 v_BUSINESSUNITMAP1,
                 'SGPAFA',
                 v_BUSINESSUNITMAP2,
                 'BRUAGY') AGY_BUNAME

     from AIA_RPT_PRD_TEMP_PADIM PAD
     WHERE (PAD.POS_GA9 = 'N'
       OR PAD.POS_GA9  IS NULL)
     AND PAD.POSITIONTITLE  IN ('ORGANISATION','UNIT')
);
COMMIT;



------=======ADD LOG1

EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_AGENT_LIST';
INSERT  /*+ Append  */
INTO AIA_RPT_AGENT_LIST NOLOGGING
(fsc_id ,
FSC_NAME ,
FSC_TITLE,
FSC_CLASS ,
FSC_HIREDATE ,
FSC_TERMINATION_DATE ,
FSC_STATUS ,
FSC_ASSIGNED_DATE ,
FSC_APPOINTMENT_DATE ,
FSC_BUNAME
)
(SELECT SUBSTR(NAME, 4)fsc_id ,
PAD.firstname || ' ' || PAD.LASTNAME FSC_NAME ,
DECODE(PAD.POSITIONTITLE,
    'FSC_NON_PROCESS', 'FSC', 'BR_FSC_NON_PROCESS',
    'FSC', 'AM_FSC_NON_PROCESS', 'FSC', 'AM_NON_PROCESS', 'AM' ,
    'FSD_NON_PROCESS', 'FSD',
    'FSAD_NON_PROCESS', 'FSAD',
    'BR_Staff' , 'Staff',
    'BR_DM', 'DM',
    'BR_DM_NON_PROCESS', 'DM',
    'BR_FSC' , 'FSC',
    'BR_Staff_NON_PROCESS', 'Staff',
    'BR_UM','UM',
    'BR_UM_NON_PROCESS', 'UM',
    'Staff_NON_PROCESS', 'Staff',
     PAD.POSITIONTITLE) FSC_TITLE,
PAD.POS_GA4 FSC_CLASS ,
PAD.hiredate FSC_HIREDATE ,
PAD.AGY_TERMINATIONDATE FSC_TERMINATION_DATE ,
(CASE WHEN PAD.PT_GA1 = '00' THEN 'INFORCE'
        WHEN PAD.PT_GA1 IN ('50','51','52','55','56') THEN 'TERMINATED'
          WHEN PAD.PT_GA1 = '13' then  'TERMINATED'
          WHEN PAD.PT_GA1 IN ('60','61') then  'TERMINATED'
          WHEN PAD.PT_GA1 = '70' THEN  'TERMINATED'
        END) FSC_STATUS ,
PAD.ASSIGNED_DATE FSC_ASSIGNED_DATE ,
PAD.AGY_APPOINTMENT_DATE FSC_APPOINTMENT_DATE ,
decode(PAD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') FSC_BUNAME
FROM AIA_RPT_PRD_TEMP_PADIM PAD
WHERE PAD.POSITIONTITLE NOT IN ('ORGANISATION','UNIT')

);

COMMIT;

------=======ADD LOG

EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_AGENCY_LEADER';

INSERT  /*+ Append  */
INTO AIA_RPT_AGENCY_LEADER NOLOGGING
(AGY_LDR_CODE ,
AGY_LDR_PARTICIPANTID,
AGY_LDR_NAME ,
AGY_LDR_TITLE,
AGY_LDR_POS_GA2 ,
AGY_LDR_POS_GA4,
AGY_LDR_POS_GA3,
AGY_LDR_BUNAME
)
(SELECT SUBSTR(NAME, 4)AGY_LDR_CODE ,
NAME AGY_LDR_PARTICIPANTID,
PAD.firstname || ' ' || PAD.LASTNAME AGY_LDR_NAME ,
DECODE(PAD.POSITIONTITLE,
    'FSC_NON_PROCESS', 'FSC', 'BR_FSC_NON_PROCESS',
    'FSC', 'AM_FSC_NON_PROCESS', 'FSC', 'AM_NON_PROCESS', 'AM' ,
    'FSD_NON_PROCESS', 'FSD',
    'FSAD_NON_PROCESS', 'FSAD',
    'BR_Staff' , 'Staff',
    'BR_DM', 'DM',
    'BR_DM_NON_PROCESS', 'DM',
    'BR_FSC' , 'FSC',
    'BR_Staff_NON_PROCESS', 'Staff',
    'BR_UM','UM',
    'BR_UM_NON_PROCESS', 'UM',
    'Staff_NON_PROCESS', 'Staff',
     PAD.POSITIONTITLE) AGY_LDR_TITLE,
     PAD.POS_GA2 AGY_LDR_POS_GA2,
     PAD.POS_GA4 AGY_LDR_POS_GA4 ,
     PAD.POS_GA3 AGY_LDR_POS_GA3,
decode(PAD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') AGY_LDR_BUNAME
FROM  AIA_RPT_PRD_TEMP_PADIM PAD

);

COMMIT;

------=======ADD LOG



EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_DISTRICT_LIST';

INSERT  /*+ Append  */
INTO AIA_RPT_DISTRICT_LIST NOLOGGING
(DIST_CODE ,
DIST_PARTICIPANTID,
DIST_NAME ,
DIST_TITLE,
DIST_POS_GA2 ,
DIST_POS_GA4,
DIST_POS_GA3,
DIST_BUNAME
)
(SELECT SUBSTR(NAME, 4)DIST_CODE ,
        NAME DIST_PARTICIPANTID,
        PAD.firstname || ' ' || PAD.LASTNAME DIST_NAME ,
        PAD.POSITIONTITLE DIST_TITLE,
        PAD.POS_GA2 DIST_POS_GA2,
        PAD.POS_GA4 DIST_POS_GA4 ,
        PAD.POS_GA3 DIST_POS_GA3,
        decode(PAD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') DIST_BUNAME
FROM AIA_RPT_PRD_TEMP_PADIM PAD
        WHERE PAD.POSITIONTITLE NOT LIKE '%FSC%'
);

COMMIT;

------=======ADD LOG

EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_DISTRICT_LEADER';

INSERT  /*+ Append  */
INTO AIA_RPT_DISTRICT_LEADER NOLOGGING
(DIST_LDR_CODE ,
DIST_LDR_PARTICIPANTID,
DIST_LDR_NAME ,
DIST_LDR_TITLE,
DIST_LDR_POS_GA2 ,
DIST_LDR_POS_GA4,
DIST_LDR_POS_GA3,
DIST_LDR_BUNAME
)
   (SELECT SUBSTR(NAME, 4)DIST_LDR_CODE ,
           NAME DIST_LDR_PARTICIPANTID,
           PAD.firstname || ' ' || PAD.LASTNAME DIST_LDR_NAME ,
           DECODE(PAD.POSITIONTITLE,
    'FSC_NON_PROCESS', 'FSC', 'BR_FSC_NON_PROCESS',
    'FSC', 'AM_FSC_NON_PROCESS', 'FSC', 'AM_NON_PROCESS', 'AM' ,
    'FSD_NON_PROCESS', 'FSD',
    'FSAD_NON_PROCESS', 'FSAD',
    'BR_Staff' , 'Staff',
    'BR_DM', 'DM',
    'BR_DM_NON_PROCESS', 'DM',
    'BR_FSC' , 'FSC',
    'BR_Staff_NON_PROCESS', 'Staff',
    'BR_UM','UM',
    'BR_UM_NON_PROCESS', 'UM',
    'Staff_NON_PROCESS', 'Staff',
     PAD.POSITIONTITLE) DIST_LDR_TITLE,
     PAD.POS_GA2 DIST_LDR_POS_GA2,
     PAD.POS_GA4 DIST_LDR_POS_GA4 ,
     PAD.POS_GA3 DIST_LDR_POS_GA3,
     decode(PAD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') DIST_LDR_BUNAME
     FROM  AIA_RPT_PRD_TEMP_PADIM PAD
     WHERE PAD.POSITIONTITLE NOT LIKE '%FSC%'
);

COMMIT;

pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_NEW_PRD_WRI' , 'Finish', '');
--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_NEW_PRD_WRI' , 'Aia_Rpt_NEW_PRD_DIST_WRI'  , 'Processing', 'insert into Aia_Rpt_NEW_PRD_DIST_WRI');


DELETE FROM Aia_Rpt_NEW_PRD_DIST_WRI WHERE PERIODNAME = V_PERIODNAME ;
COMMIT;


   INSERT
    /*+Append   */
  INTO Aia_Rpt_NEW_PRD_DIST_WRI NOLOGGING
    (
      PROCESSINGUNITSEQ,
      PUNAME,
      BUNAME,
      calendarseq,
      calendarname,
      PERIODKEY,
      Periodname,
      --POSITIONSEQ,
      MANAGERSEQ,
      POSITIONSEQ,
      POSITIONNAME,
      DISTRICT_CODE,
      DM_NAme,
      dIST_LEADER_CODE,
      DIST_LEADER_NAME,
      DIST_LEAER_TITLE,
      DIST_LEADER_CLASS,
      UNIT_CODE,
      AGENCY,
      UNIT_LEADER_CODE,
      UNIT_LEADER_NAME,
      UNIT_LEAER_TITLE,
      UNIT_LEADER_CLASS,
      DISSOLVED_DATE,
      AGT_CODE,
      NAME,
      ROLE,
      CLASS,
      CONTRACT_DATE,
      APPOINTMENT_DATE,
      TERMINATION_DATE,
      AGENT_STATUS,
      LIFE_FYC_API_SSC,
      LIFE_2YEAR,
      LIFE_3YEAR,
      LIFE_4YEAR,
      UNASSIGNED_LIFE_2YEAR,
      UNASSIGNED_LIFE_3YEAR,
      UNASSIGNED_LIFE_4YEAR,
      LIFE_FYC_API_SSC_YTD,
      Life_2year_Ytd,
      LIFE_3YEAR_YTD,
      LIFE_4YEAR_YTD,
      UNASSIGNED_LIFE_2YEAR_YTD,
      UNASSIGNED_LIFE_3YEAR_YTD,
      Unassigned_Life_4year_Ytd
    )
SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
  V_PROCESSINGUNITNAME PUNAME,
  --AGT.FSC_BUNAME BUNAME,
  tmp.buname,
  V_CALENDARSEQ calendarseq,
  V_CALENDARNAME calendarname,
  V_PERIODSEQ PERIODKEY,
  V_PERIODNAME Periodname,
  --NULL POSITIONSEQ,
  NULL MANAGERSEQ,
  NULL POSITIONSEQ,
  NULL POSITIONNAME,
/*  DISTRICT.DIST_CODE,
  DISTRICT.DIST_NAME DM_NAME,
  district_ldr.DIST_LDR_CODE DIST_LEADER_CODE,
  district_ldr.DIST_LDR_NAME DIST_LEADER_NAME,
  district_ldr.DIST_LDR_TITLE DIST_LEAER_TITLE,
  DISTRICT_LDR.DIST_LDR_POS_GA4 DIST_LEADER_CLASS,*/
  tmp.district_code as DIST_CODE,
  tmp.dm_name as DM_NAME,
  tmp.dist_leader_code as DIST_LEADER_CODE,
  tmp.dist_leader_name as DIST_LEADER_NAME,
  tmp.dist_leaer_title as DIST_LEAER_TITLE,
  tmp.dist_leader_class as DIST_LEADER_CLASS,

/*  AGY.AGY_CODE UNIT_CODE,
  AGY_NAME AGENCY,
  AGY_LDR.AGY_LDR_CODE UNIT_LEADER_CODE,
  AGY_LDR.AGY_LDR_NAME  UNIT_LEADER_NAME,
  AGY_LDR.AGY_LDR_TITLE  UNIT_LEAER_TITLE,
  AGY_LDR.AGY_LDR_POS_GA4  UNIT_LEADER_CLASS,
  TO_CHAR(AGY.AGY_TERMINATIONDATE, 'MM/DD/YYYY') DISSOLVED_DATE,*/
  tmp.unit_code,
  tmp.agency,
  tmp.unit_leader_code,
  tmp.unit_leader_name,
  tmp.unit_leaer_title,
  tmp.unit_leader_class,
  TO_CHAR(tmp.dissolved_date,'MM/DD/YYYY'),

  TMP.AGT_CODE,
/*  AGT.FSC_NAME NAME,
  AGT.FSC_TITLE ROLE,
  AGT.FSC_CLASS CLASS,
  AGT.FSC_HIREDATE CONTRACT_DATE,
  AGT.FSC_APPOINTMENT_DATE APPOINTMENT_DATE,
  AGT.FSC_TERMINATION_DATE TERMINATION_DATE,
  AGT.FSC_STATUS AGENT_STATUS,*/
  tmp.name,
  tmp.role,
  tmp.class,
  tmp.contract_date,
  tmp.appointment_date,
  tmp.termination_date,
  tmp.agent_status,
  SUM(LIFE_FYC_API_SSC),
  SUM(LIFE_2YEAR),
  SUM( LIFE_3YEAR),
  SUM(LIFE_4YEAR),
  SUM(UNASSIGNED_LIFE_2YEAR),
  SUM( UNASSIGNED_LIFE_3YEAR),
  SUM(UNASSIGNED_LIFE_4YEAR),
  SUM(LIFE_FYC_API_SSC)       + SUM( LIFE_FYC_API_SSC_YTD),
  SUM(LIFE_2YEAR)             + SUM( LIFE_2YEAR_YTD),
  SUM( LIFE_3YEAR)            + SUM( LIFE_3YEAR_YTD),
  SUM(LIFE_4YEAR)             + SUM( LIFE_4YEAR_YTD),
  SUM(UNASSIGNED_LIFE_2YEAR)  + SUM( UNASSIGNED_LIFE_2YEAR_YTD),
  SUM( UNASSIGNED_LIFE_3YEAR) + SUM( UNASSIGNED_LIFE_3YEAR_YTD),
  SUM(UNASSIGNED_LIFE_4YEAR)  + SUM( UNASSIGNED_LIFE_4YEAR_YTD)
FROM AIA_RPT_NEW_PRD_DIST_WRI_TMP tmp
/*  inner join AIA_RPT_AGENT_LIST agt
  on TMP.AGT_CODE = AGT.FSC_ID
  and TMP.BUNAME = AGT.FSC_BUNAME
  inner join AIA_RPT_AGENCY_LIST agy
  on TMP.UNIT_CODE = AGY.AGY_CODE
  and AGY.AGY_BUNAME = AGT.FSC_BUNAME
  inner join AIA_RPT_AGENCY_LEADER agy_ldr
  on (AGY_LDR.AGY_LDR_PARTICIPANTID = 'SGT' || AGY.AGY_POS_GA2 AND AGY.AGY_PARTICIPANTID LIKE 'SGY%')
  and AGY_LDR.AGY_LDR_CODE      = AGY.AGY_POS_GA2
  inner join AIA_RPT_DISTRICT_LIST district
  on DISTRICT.DIST_BUNAME = AGY.AGY_BUNAME
  and DISTRICT.DIST_CODE = agy.NEW_DISTRICT_CODE
  left join AIA_RPT_DISTRICT_LEADER DISTRICT_LDR
  on AGT.FSC_BUNAME = DISTRICT_LDR.DIST_LDR_BUNAME
  AND DISTRICT_LDR.DIST_LDR_CODE = DISTRICT.DIST_POS_GA2*/
WHERE TMP.PERIODKEY          = V_PERIODSEQ
AND TMP.BUNAME = 'SGPAFA'
GROUP BY
  --AGT.FSC_BUNAME,
  tmp.buname,
/*  DISTRICT.DIST_CODE,
  DISTRICT.DIST_NAME ,
  district_ldr.DIST_LDR_CODE ,
  district_ldr.DIST_LDR_NAME ,
  district_ldr.DIST_LDR_TITLE ,
  DISTRICT_LDR.DIST_LDR_POS_GA4 ,*/
  tmp.district_code,
  tmp.dm_name,
  tmp.dist_leader_code,
  tmp.dist_leader_name,
  tmp.dist_leaer_title,
  tmp.dist_leader_class,
/*  AGY.AGY_CODE ,
  AGY.AGY_NAME ,
  AGY_LDR.AGY_LDR_CODE ,
  AGY_LDR.AGY_LDR_NAME  ,
  AGY_LDR.AGY_LDR_TITLE  ,
  AGY_LDR.AGY_LDR_POS_GA4  ,
  TO_CHAR(AGY.AGY_TERMINATIONDATE, 'MM/DD/YYYY') ,*/
  tmp.unit_code,
  tmp.agency,
  tmp.unit_leader_code,
  tmp.unit_leader_name,
  tmp.unit_leaer_title,
  tmp.unit_leader_class,
  tmp.dissolved_date,

  TMP.AGT_CODE,
/*  AGT.FSC_NAME ,
  AGT.FSC_TITLE ,
  AGT.FSC_CLASS ,
  agt.FSC_HIREDATE ,
  AGT.FSC_APPOINTMENT_DATE ,
  agt.FSC_TERMINATION_DATE ,
  agt.FSC_STATUS*/
  tmp.name,
  tmp.role,
  tmp.class,
  tmp.contract_date,
  tmp.appointment_date,
  tmp.termination_date,
  tmp.agent_status
  ;
COMMIT;


  ------=======ADD LOG




  INSERT
    /*+ Append  */
  INTO Aia_Rpt_NEW_PRD_DIST_WRI NOLOGGING
    (
      PROCESSINGUNITSEQ,
      PUNAME,
      BUNAME,
      calendarseq,
      calendarname,
      PERIODKEY,
      Periodname,
      --POSITIONSEQ,
      MANAGERSEQ,
      POSITIONSEQ,
      POSITIONNAME,
      DISTRICT_CODE,
      DM_NAme,
      dIST_LEADER_CODE,
      DIST_LEADER_NAME,
      DIST_LEAER_TITLE,
      DIST_LEADER_CLASS,
      UNIT_CODE,
      AGENCY,
      UNIT_LEADER_CODE,
      UNIT_LEADER_NAME,
      UNIT_LEAER_TITLE,
      UNIT_LEADER_CLASS,
      DISSOLVED_DATE,
      AGT_CODE,
      NAME,
      ROLE,
      CLASS,
      CONTRACT_DATE,
      APPOINTMENT_DATE,
      TERMINATION_DATE,
      AGENT_STATUS,
      LIFE_FYC_API_SSC,
      LIFE_2YEAR,
      LIFE_3YEAR,
      LIFE_4YEAR,
      UNASSIGNED_LIFE_2YEAR,
      UNASSIGNED_LIFE_3YEAR,
      UNASSIGNED_LIFE_4YEAR,
      LIFE_FYC_API_SSC_YTD,
      Life_2year_Ytd,
      LIFE_3YEAR_YTD,
      LIFE_4YEAR_YTD,
      UNASSIGNED_LIFE_2YEAR_YTD,
      UNASSIGNED_LIFE_3YEAR_YTD,
      Unassigned_Life_4year_Ytd
    )
SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
  V_PROCESSINGUNITNAME PUNAME,
  AGT.FSC_BUNAME BUNAME,
  V_CALENDARSEQ calendarseq,
  V_CALENDARNAME calendarname,
  V_PERIODSEQ PERIODKEY,
  V_PERIODNAME Periodname,
  --NULL POSITIONSEQ,
  NULL MANAGERSEQ,
  NULL POSITIONSEQ,
  NULL POSITIONNAME,
  DISTRICT.DIST_CODE,
  DISTRICT.DIST_NAME DM_NAME,
  district_ldr.DIST_LDR_CODE DIST_LEADER_CODE,
  district_ldr.DIST_LDR_NAME DIST_LEADER_NAME,
  district_ldr.DIST_LDR_TITLE DIST_LEAER_TITLE,
  DISTRICT_LDR.DIST_LDR_POS_GA4 DIST_LEADER_CLASS,
  AGY.AGY_CODE UNIT_CODE,
  AGY_NAME AGENCY,
  AGY_LDR.AGY_LDR_CODE UNIT_LEADER_CODE,
  AGY_LDR.AGY_LDR_NAME  UNIT_LEADER_NAME,
  AGY_LDR.AGY_LDR_TITLE  UNIT_LEAER_TITLE,
  AGY_LDR.AGY_LDR_POS_GA4  UNIT_LEADER_CLASS,
  TO_CHAR(AGY.AGY_TERMINATIONDATE, 'MM/DD/YYYY') DISSOLVED_DATE,
  TMP.AGT_CODE,
  AGT.FSC_NAME NAME,
  AGT.FSC_TITLE ROLE,
  AGT.FSC_CLASS CLASS,
  AGT.FSC_HIREDATE CONTRACT_DATE,
  AGT.FSC_APPOINTMENT_DATE APPOINTMENT_DATE,
  AGT.FSC_TERMINATION_DATE TERMINATION_DATE,
  AGT.FSC_STATUS AGENT_STATUS,
  SUM(LIFE_FYC_API_SSC),
  SUM(LIFE_2YEAR),
  SUM( LIFE_3YEAR),
  SUM(LIFE_4YEAR),
  SUM(UNASSIGNED_LIFE_2YEAR),
  SUM( UNASSIGNED_LIFE_3YEAR),
  SUM(UNASSIGNED_LIFE_4YEAR),
  SUM(LIFE_FYC_API_SSC)       + SUM( LIFE_FYC_API_SSC_YTD),
  SUM(LIFE_2YEAR)             + SUM( LIFE_2YEAR_YTD),
  SUM( LIFE_3YEAR)            + SUM( LIFE_3YEAR_YTD),
  SUM(LIFE_4YEAR)             + SUM( LIFE_4YEAR_YTD),
  SUM(UNASSIGNED_LIFE_2YEAR)  + SUM( UNASSIGNED_LIFE_2YEAR_YTD),
  SUM( UNASSIGNED_LIFE_3YEAR) + SUM( UNASSIGNED_LIFE_3YEAR_YTD),
  SUM(UNASSIGNED_LIFE_4YEAR)  + SUM( UNASSIGNED_LIFE_4YEAR_YTD)
FROM AIA_RPT_NEW_PRD_DIST_WRI_TMP tmp,
  AIA_RPT_AGENT_LIST agt,
  AIA_RPT_AGENCY_LIST agy,
  AIA_RPT_AGENCY_LEADER agy_ldr,
  AIA_RPT_DISTRICT_LIST district,
  AIA_RPT_DISTRICT_LEADER DISTRICT_LDR
WHERE TMP.PERIODKEY          = V_PERIODSEQ
AND TMP.BUNAME = 'BRUAGY'

AND AGT.FSC_ID           = TMP.AGT_CODE
AND AGY.AGY_CODE        = TMP.UNIT_CODE
AND agy.NEW_DISTRICT_CODE = TMP.DISTRICT_CODE
AND TMP.BUNAME = AGT.FSC_BUNAME
AND AGY.AGY_BUNAME = AGT.FSC_BUNAME
AND AGT.FSC_BUNAME = DISTRICT_LDR.DIST_LDR_BUNAME
AND (agy_ldr.AGY_LDR_PARTICIPANTID = 'BRT' || agy.AGY_POS_GA2 AND agy.AGY_PARTICIPANTID like 'BRY%')
AND AGY_LDR.AGY_LDR_CODE      = AGY.AGY_POS_GA2
AND DISTRICT.DIST_BUNAME = AGY.AGY_BUNAME
AND DISTRICT.DIST_CODE = agy.NEW_DISTRICT_CODE
AND DISTRICT_LDR.DIST_LDR_CODE = DISTRICT.DIST_POS_GA2
GROUP BY
  AGT.FSC_BUNAME,
  DISTRICT.DIST_CODE,
  DISTRICT.DIST_NAME ,
  district_ldr.DIST_LDR_CODE ,
  district_ldr.DIST_LDR_NAME ,
  district_ldr.DIST_LDR_TITLE ,
  DISTRICT_LDR.DIST_LDR_POS_GA4 ,
  AGY.AGY_CODE ,
  AGY.AGY_NAME ,
  AGY_LDR.AGY_LDR_CODE ,
  AGY_LDR.AGY_LDR_NAME  ,
  AGY_LDR.AGY_LDR_TITLE  ,
  AGY_LDR.AGY_LDR_POS_GA4  ,
  TO_CHAR(AGY.AGY_TERMINATIONDATE, 'MM/DD/YYYY') ,
  TMP.AGT_CODE,
  AGT.FSC_NAME ,
  AGT.FSC_TITLE ,
  AGT.FSC_CLASS ,
  agt.FSC_HIREDATE ,
  AGT.FSC_APPOINTMENT_DATE ,
  agt.FSC_TERMINATION_DATE ,
  agt.FSC_STATUS
  ;
COMMIT;

  ------=======ADD LOG


-- DISTRICT TOTALS
UPDATE Aia_Rpt_NEW_PRD_DIST_WRI P1
SET
  (
    Life_Fyc_Api_Ssc_Dt ,
    Life_2year_Dt ,
    Life_3year_Dt ,
    Life_4year_Dt ,
    Unassigned_Life_2year_Dt ,
    Unassigned_Life_3year_Dt ,
    Unassigned_Life_4year_Dt ,
    Life_Fyc_Api_Ssc_Ytd_Dt ,
    Life_2year_Ytd_Dt ,
    Life_3year_Ytd_Dt ,
    Life_4year_Ytd_Dt ,
    Unassigned_Life_2year_Ytd_Dt ,
    Unassigned_Life_3year_Ytd_Dt ,
    Unassigned_Life_4year_Ytd_Dt
  )
  =
  (SELECT
    SUM(Life_Fyc_Api_Ssc) ,
    SUM(Life_2year) ,
    SUM(Life_3year) ,
    SUM(Life_4year) ,
    SUM(Unassigned_Life_2year) ,
    SUM(Unassigned_Life_3year) ,
    SUM(Unassigned_Life_4year) ,
    SUM(Life_Fyc_Api_Ssc_Ytd) ,
    SUM(Life_2year_Ytd) ,
    SUM(Life_3year_Ytd) ,
    SUM(Life_4year_Ytd) ,
    SUM(Unassigned_Life_2year_Ytd) ,
    SUM(Unassigned_Life_3year_Ytd) ,
    SUM(Unassigned_Life_4year_Ytd)
  FROM Aia_Rpt_NEW_PRD_DIST_WRI P2
  WHERE P1.DISTRICT_CODE = P2.DISTRICT_CODE
  AND P1.BUNAME          = P2.BUNAME
    --and p2.periodkey = p1.periodkey
  AND p2.periodkey = V_PERIODSEQ
  )
WHERE p1.periodkey = V_PERIODSEQ;
COMMIT;
    ------=======ADD LOG

    UPDATE Aia_Rpt_NEW_PRD_DIST_WRI P1
SET
  (
    Life_Fyc_Api_Ssc_ct ,
    Life_2year_ct ,
    Life_3year_ct ,
    Life_4year_ct ,
    Unassigned_Life_2year_ct ,
    Unassigned_Life_3year_ct ,
    Unassigned_Life_4year_ct ,
    Life_Fyc_Api_Ssc_Ytd_ct ,
    Life_2year_Ytd_Ct ,
    Life_3year_Ytd_Ct ,
    Life_4year_Ytd_Ct ,
    Unassigned_Life_2year_Ytd_ct ,
    Unassigned_Life_3year_Ytd_ct ,
    Unassigned_Life_4year_Ytd_ct
  )
  =
  (SELECT
    SUM(Life_Fyc_Api_Ssc) ,
    SUM(Life_2year) ,
    SUM(Life_3year) ,
    SUM(Life_4year) ,
    SUM(Unassigned_Life_2year) ,
    SUM(Unassigned_Life_3year) ,
    SUM(Unassigned_Life_4year) ,
    SUM(Life_Fyc_Api_Ssc_Ytd) ,
    SUM(Life_2year_Ytd) ,
    SUM(Life_3year_Ytd) ,
    SUM(Life_4year_Ytd) ,
    SUM(Unassigned_Life_2year_Ytd) ,
    SUM(Unassigned_Life_3year_Ytd) ,
    SUM(Unassigned_Life_4year_Ytd)
  FROM Aia_Rpt_NEW_PRD_DIST_WRI P2
  WHERE P1.BUNAME = P2.BUNAME
    --and p2.periodkey = p1.periodkey
  AND p2.periodkey = V_PERIODSEQ
  )
WHERE p1.periodkey = V_PERIODSEQ;
COMMIT;

      ------=======ADD LOG

/* Start of deleting the Terminated Agents with 0 Value. Added by Saurabh Trehan*/
delete from Aia_Rpt_NEW_PRD_DIST_WRI
where upper(Agent_Status) != 'INFORCE'
and   termination_date is not null
and
periodkey = V_PERIODSEQ and
nvl(LIFE_FYC_API_SSC,0) = 0 and
nvl(LIFE_2YEAR,0) = 0 and
nvl(LIFE_3YEAR,0) = 0 and
NVL(LIFE_4YEAR,0) = 0 AND
--------------------------------------
NVL(UNASSIGNED_LIFE_2YEAR,0) = 0 AND
NVL(UNASSIGNED_LIFE_3YEAR,0) = 0 AND
NVL(UNASSIGNED_LIFE_4YEAR,0) = 0 AND
---------------------------------------
nvl(LIFE_FYC_API_SSC_YTD,0)  = 0 and
nvl(LIFE_2YEAR_YTD,0)  = 0 and
nvl(LIFE_3YEAR_YTD,0)  = 0 and
nvl(LIFE_4YEAR_YTD,0)  = 0 and
NVL(UNASSIGNED_LIFE_2YEAR_YTD,0)  = 0 AND
NVL(UNASSIGNED_LIFE_3YEAR_YTD,0)  = 0 AND
NVL(UNASSIGNED_LIFE_4YEAR_YTD,0)  = 0
--AND nvl(PA_FYC_DT,0) = 0 and
--nvl(CS_FYC_DT,0) = 0 and
--nvl(PA_RC_DT,0) = 0 and
--nvl(CS_RC_DT,0) = 0 and
--NVL(CS_PL_DT,0) = 0
;
/* End of deleting the Terminated Agents with 0 Value. Added by Saurabh Trehan*/

commit;

      ------=======ADD LOG JEFF1
--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_NEW_PRD_WRI' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_NEW_PRD_WRI' , 'AIA_RPT_NEW_PRD_DIST_WRI_PARAM'  , 'Processing', 'insert into AIA_RPT_NEW_PRD_DIST_WRI_PARAM');



-- Parameter table updation
EXECUTE IMMEDIATE 'Truncate table AIA_RPT_NEW_PRD_DIST_WRI_PARAM drop storage' ;

      ------=======ADD LOG

--1
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  ) ;
COMMIT;

--ADDED 2
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  ) ;
COMMIT;

--ADDED 3
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  ) ;
COMMIT;

--ADDED 4
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  ) ;
COMMIT;
--ADDED 5
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  ) ;
COMMIT;
--ADDED 6
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 7
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--added new
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--added new
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 8
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 9
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 10
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 11
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 12
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 13
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 14
INSERT
INTO AIA_RPT_NEW_PRD_DIST_WRI_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM Aia_Rpt_NEW_PRD_DIST_WRI rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );


commit;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_NEW_PRD_WRI' , 'Finish', '');

End;

PROCEDURE REP_RUN_PRD_COMM
AS
  V_EOT                DATE := TO_DATE('01/01/2200','DD/MM/YYYY');
  V_PERIODNAME         VARCHAR2(255 BYTE);
  V_YTD_PRD_START_DATE DATE;
  V_YTD_PRD_END_DATE   DATE;
  V_CALENDARNAME       VARCHAR2(255);
  V_PROCESSINGUNITSEQ  INTEGER;
  V_PROCESSINGUNITNAME VARCHAR2(256);
  V_PERIODSTARTDATE    DATE;
  V_PERIODENDDATE      DATE;
  V_CALENDARSEQ       INTEGER;
  V_PROCNAME          VARCHAR2(256);
  V_SYSDATE           DATE;
  V_PERIODTYPESEQ     CS_PERIOD.PERIODTYPESEQ%TYPE;
  --v_PERIODDIMENSION   PERIODDIMENSION%ROWTYPE;
BEGIN

--V_PROCNAME := 'PROC_RPT_PRD_DISTRICT';
V_SYSDATE := SYSDATE;

BEGIN
SELECT P.STARTDATE,P.ENDDATE,C.DESCRIPTION,P.NAME,P.CALENDARSEQ
INTO V_PERIODSTARTDATE,V_PERIODENDDATE,V_CALENDARNAME,V_PERIODNAME,V_CALENDARSEQ
FROM  CS_PERIOD P INNER JOIN  CS_CALENDAR C ON P.CALENDARSEQ=C.CALENDARSEQ
WHERE PERIODSEQ = V_PERIODSEQ
;
SELECT PERIODTYPESEQ INTO V_PERIODTYPESEQ FROM  CS_PERIODTYPE WHERE NAME ='month';

/*exception
  when no_data_found then
    insert into AIA_Report_Proc_Logs
  Values
  ('PROC_RPT_PRD_DISTRICT',sysdate,'AGY_PU','','Unable to get period data. Invalid Period');

  commit;

  RAISE;*/
end;

-----------=======LOG
  SELECT PROCESSINGUNITSEQ,NAME  INTO V_PROCESSINGUNITSEQ,V_PROCESSINGUNITNAME
  FROM CS_PROCESSINGUNIT  WHERE NAME = 'AGY_PU';

  SELECT MASK INTO v_BUSINESSUNITMAP1 FROM CS_BUSINESSUNIT
   WHERE NAME        IN ('SGPAFA');
  SELECT MASK INTO v_BUSINESSUNITMAP2 FROM CS_BUSINESSUNIT
   WHERE NAME        IN ('BRUAGY');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_COMM' , 'AIA_RPT_PRD_DIST_COMM_NEW 1'  , 'Processing', 'insert into AIA_RPT_PRD_DIST_COMM_NEW');


-----------=======LOG
  EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_DIST_COMM_NEW';


-- Date insert in reporting table,
  INSERT
    /*+ Append  */
  INTO AIA_RPT_PRD_DIST_COMM_NEW NOLOGGING
    (SELECT
        V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
        V_PROCESSINGUNITNAME PUNAME,
        decode(PD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') BUNAME,
        PD.businessunitmap BUMAP,
        V_CALENDARSEQ CALENDARSEQ,
        V_CALENDARNAME CALENDARNAME,
        V_PERIODSEQ PERIODKEY,
        V_PERIODNAME periodname,
        --PAD.POSITIONSEQ POSITIONSEQ,
        PAD.MANAGERSEQ,
        PAD.POSITIONSEQ POSITIONSEQ,
        PAD.NAME,
        SUBSTR(DISTRICT.NAME,4) DISTRICT_CODE,
        district.firstname
        || ' '
        || district.lastname DM_NAme,
        district.POS_GA2 DIST_LEADER_CODE,
        district.POS_GA7 DIST_LEADER_NAME,
        district.POS_GA11 DIST_LEAER_TITLE,
        DISTRICT.POS_GA4 DIST_LEADER_CLASS,
        PD.CD_GA13 UNIT_CODE,
        agy.firstname
        || ' '
        || agy.lastname AGENCY,
        agy.POS_GA2 UNIT_LEADER_CODE,
        agy.POS_GA7 UNIT_LEADER_NAME,
        DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
        agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
        (
        CASE
          WHEN AGY.POSITIONTITLE IN ('ORGANISATION','UNIT')
          THEN AGY.AGY_TERMINATIONDATE
        END)DISSOLVED_DATE,
        SUBSTR(PAD.NAME,4) AGT_CODE,
        (PAD.firstname
        || PAD.lastname) NAME,
        DECODE(PAD.positiontitle, 'FSC_NON_PROCESS', 'FSC', PAD.positiontitle) ROLE,
        Pad.POS_GA4 CLASS,
        PAD.HIREDATE CONTRACT_DATE,
        PAD.AGY_APPOINTMENT_DATE APPOINTMENT_DATE,
        PAD.AGY_TERMINATIONDATE TERMINATION_DATE,
        (
        CASE
          WHEN pad.PT_GA1 = '00'
          THEN 'INFORCE'
          WHEN pad.PT_GA1 IN ('50','51','52','55','56')
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 = '13'
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 IN ('60','61')
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 = '70'
          THEN 'TERMINATED'
        END) AGENT_STATUS,
        CASE
          WHEN (PD.CD_GA2 IN ('LF','HS')
          AND PD.CD_CREDITTYPE          IN ('FYC','SSCP')
          AND PD.CD_VALUE <> 0 )
          THEN PD.TRANS_GN6
          ELSE 0
        END LIFE_FYP,
        CASE
          WHEN (PD.CD_GA2 IN ('PA')--,'VL')
          AND PD.CD_CREDITTYPE         IN ('FYC','SSCP')
          AND PD.CD_VALUE <> 0)
          THEN PD.TRANS_GN6
          ELSE 0
        END PA_FYP,
        CASE
          WHEN PD.CD_GA2 IN ('LF','HS')
          AND PD.TRANS_DIM_EVENTTYPE          = 'FYP'
          THEN PD.TRANS_GN5
          ELSE 0
        END LIFE_CASE,
        CASE
          WHEN PD.CD_GA2 = 'PA'
          AND PD.TRANS_DIM_EVENTTYPE         = 'FYP'
          THEN PD.TRANS_GN5
          ELSE 0
        END PA_CASE,
        CASE
          WHEN (PD.CD_GA2 IN ('LF','HS')
          AND PD.CD_CREDITTYPE          IN ('FYC','API','SSCP','APB') )
          THEN PD.CD_VALUE
          ELSE 0
        END LIFE_FYC_API_SSC,
        CASE
 WHEN (PD.CD_GA2 IN ('LF','HS')
 --AND PD.CD_CREDITTYPE IN ('FYC','API','SSCP','APB')
 AND PD.CD_GA4 IN ('PAY1','PAY0','PAYE','PAYF')
 )
 THEN PD.CD_VALUE
 ELSE 0
 END LIFE_FYC_RP,
  CASE
 WHEN (PD.CD_GA2 IN ('LF','HS')
 --AND PD.CD_CREDITTYPE IN ('FYC','API','SSCP','APB')
 AND PD.CD_GA4 IN ('PAYS','PTAF')
 AND PD.CD_GA1 NOT IN ('GFBA','GFBC','IACB','IAGB','IAP4','IARB','IAS1','IAS2','IAS3','IBOB','IBSB','IFRB','IFYP','IFZP','IGCB','IPGA','IPOA','IPOB','IPOC','IPOD','ISAC','ISBO','ISFS','ISGR','IWBO','IWBS','IWCB','IWRB')
 )
 THEN PD.CD_VALUE
 ELSE 0
 END LIFE_FYC_NONILP,

  CASE
 WHEN (PD.CD_GA2 IN ('LF','HS')
 --AND PD.CD_CREDITTYPE IN ('FYC','API','SSCP','APB')
 AND PD.CD_GA4 IN ('PAYS')
 AND PD.CD_GA1 IN ('GFBA','GFBC','IACB','IAGB','IAP4','IARB','IAS1','IAS2','IAS3','IBOB','IBSB','IFRB','IFYP','IFZP','IGCB','IPGA','IPOA','IPOB','IPOC','IPOD','ISAC','ISBO','ISFS','ISGR','IWBO','IWBS','IWCB','IWRB')
 )
 THEN PD.CD_VALUE
 ELSE 0
 END LIFE_FYC_ILP,
 CASE
 WHEN (PD.CD_GA2 IN ('LF','HS','PA','CS')
 --AND PD.CD_CREDITTYPE IN ('FYC','API','SSCP','APB')
 AND PD.CD_GA4 IN ('PAYT')
 )
 THEN PD.CD_VALUE
 ELSE 0
 END LIFE_FYC_TOPUP,
        CASE
          WHEN (PD.CD_GA2 IN ('PA') --,'VL') as asked by Donny
          AND PD.CD_CREDITTYPE         IN ('FYC','API','SSCP','APB') )
          THEN PD.CD_VALUE
          ELSE 0
        END PA_FYC,
        CASE
          WHEN ( PD.CD_GA2 IN ('CS','CL')
          AND PD.CD_CREDITTYPE           IN ('FYC','API','SSCP','APB') )
          THEN PD.CD_VALUE
          ELSE 0
        END CS_FYC,
        CASE
 --WHEN (PD.CD_GA1 Like ('ATWP%') version 9 comment
 WHEN (  (
         PD.CD_GA1 Like ('ATWP%')   --old logic before alpha
         OR (PD.CD_GA2='GI' AND PD.SOURCE_SYSTEM = 'INLF') --alpha logic
        )
AND PD.CD_CREDITTYPE IN ('FYC','API','SSCP','APB')
AND PD.CD_GA4 IN ('PAYS')
)
THEN PD.CD_VALUE
ELSE 0
END GI_SP_FYC,
 CASE
 WHEN (PD.CD_GA2 IN ('VL')
 )
 THEN PD.CD_VALUE
 ELSE 0
 END GI_VL_FYC,
 --version 9 comment start
 /*
CASE
WHEN ( (PD.CD_GA1  Like ('ATWP%') OR PD.CD_GA1  Like ('%03389%'))
--AND PD.CD_CREDITTYPE IN ('FYC','API','SSCP','APB')
AND PD.CD_GA4 NOT IN ('PAYS')
AND PD.CD_GA2 NOT IN ('VL')
AND PD.SOURCE_SYSTEM in ('HIAS','PLAS')
)
*/

 CASE
WHEN  (
         ( (PD.CD_GA1  Like 'ATWP%' OR PD.CD_GA1  Like '%03389%')
            AND PD.CD_GA2 NOT IN ('VL')
            AND PD.SOURCE_SYSTEM in ('HIAS','PLAS')
          )                                              --old logic before alpha
       OR (PD.CD_GA2='GI' AND PD.SOURCE_SYSTEM = 'INLF') --alpha logic
        )
   AND PD.CD_GA4 NOT IN ('PAYS')
   AND PD.CD_CREDITTYPE IN ('FYC','API','SSCP','APB')
THEN PD.CD_VALUE
ELSE 0
END GI_RP_FYC,
 --version 9 end
        CASE
          WHEN (PD.CD_GA4 = 'PAY2'
          AND PD.CD_CREDITTYPE          = 'RYC'
          AND PD.CD_GA2  IN ('LF','HS') )
          THEN PD.CD_VALUE
          ELSE 0
        END LIFE_2YEAR,
        CASE
          WHEN (PD.CD_GA4 = 'PAY3'
          AND PD.CD_CREDITTYPE          = 'RYC'
          AND PD.CD_GA2  IN ('LF','HS') )
          THEN PD.CD_VALUE
          ELSE 0
        END LIFE_3YEAR,
        CASE
          WHEN ( PD.CD_GA4 IN ('PAY4','PAY5','PAY6')
          AND PD.CD_CREDITTYPE            = 'RYC'
          AND PD.CD_GA2    IN ('LF','HS') )
          THEN PD.CD_VALUE
          ELSE 0
        END LIFE_4YEAR,
        CASE
          WHEN (PD.CD_GA2 IN ('PA')--,'VL')
          AND PD.CD_CREDITTYPE          = 'RYC' )
          THEN PD.CD_VALUE
          ELSE 0
        END PA_RC,
        CASE
          WHEN (PD.CD_GA2 = 'CS'
          AND PD.CD_CREDITTYPE          = 'RYC' )
          THEN PD.CD_VALUE
          ELSE 0
        END CS_RC,
        CASE
          --WHEN (PD.CD_GA2 = 'PL'
          WHEN (PD.CD_GA2 in ('PL','GI')   --version 9 add GI product
          AND PD.CD_CREDITTYPE          = 'RYC' )
          THEN PD.CD_VALUE
          ELSE 0
        END CS_PL,
        0 UNASSIGNED_LIFE_2YEAR,
        0 UNASSIGNED_LIFE_3YEAR,
        0 UNASSIGNED_LIFE_4YEAR,
        0 UNASSIGNED_PA_RC,
        0 LIFE_FYP_YTD,
        0 PA_FYP_YTD,
        0 LIFE_CASE_YTD,
        0 PA_CASE_YTD,
        0 LIFE_FYC_API_SSC_YTD,
        0 LIFE_FYC_RP_YTD,
 0 LIFE_FYC_NONILP_YTD,
 0 LIFE_FYC_ILP_YTD,
 0 LIFE_FYC_TOPUP_YTD ,
        0 PA_FYC_YTD,
        0 CS_FYC_YTD,
         0 GI_SP_FYC_YTD,
 0 GI_VL_FYC_YTD,
 0 GI_RP_FYC_YTD,
        0 LIFE_2YEAR_YTD,
        0 LIFE_3YEAR_YTD,
        0 LIFE_4YEAR_YTD,
        0 PA_RC_YTD,
        0 CS_RC_YTD,
        0 CS_PL_YTD,
        0 UNASSIGNED_LIFE_2YEAR_YTD,
        0 UNASSIGNED_LIFE_3YEAR_YTD,
        0 UNASSIGNED_LIFE_4YEAR_YTD,
        0 UNASSIGNED_PA_RC_YTD,
        --add in version 2
        PD.CD_GA6  as POLICYNO,
        PD.CD_GN1 AS POLICYYEAR,
        --add in version 2
        ---add version 3
        PD.Assign_GN1
      FROM AIA_RPT_PRD_TEMP_PADIM pad,
           AIA_RPT_PRD_TEMP_PADIM agy,
           AIA_RPT_PRD_TEMP_PADIM agy_ldr,
           AIA_RPT_PRD_TEMP_PADIM DISTRICT,
           AIA_RPT_PRD_TEMP_VALUES PD
      WHERE
--      PD.NEW_PIB_IND=0  AND
      PAD.POSITIONTITLE IN ('AD','DIR','ED','FC','SD') --mod by drs 20160901
      --AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD','AM','FSD','FSM')                         --mod by drs 20160901
      AND PAD.POS_GA4 NOT IN ('45','48')
      AND PD.CD_POSITIONSEQ = PAD.POSITIONSEQ
      AND PD.CD_CREDITTYPE IN ('FYP', 'RYC', 'FYC','API','SSCP','APB')
      AND AGY.POS_GA1 = PD.CD_GA13
      AND AGY.POSITIONTITLE      IN ('ORGANISATION','UNIT')                        --mod by drs20160901
      --AND AGY.POSITIONTITLE      IN ('AGENCY','DISTRICT')                                                --mod by drs20160901
      AND ((agy_ldr.NAME = 'SGT'
        || agy.POS_GA2
      AND agy.NAME LIKE 'SGY%'))
      AND DISTRICT.POS_GA3 = AGY.POS_GA3
      AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
      AND district.POSITIONTITLE     IN ('ORGANISATION')        --mod by drs 20160901
      --AND district.POSITIONTITLE = 'DISTRICT'                           --mod by drs 20160901
      --AND PD.CD_GA2  IN ('LF','PA','HS','CS','PL','CL','VL')
      AND PD.CD_GA2  IN ('LF','PA','HS','CS','PL','CL','VL','GI')   --version 9 add GI product
      --fix in version 2 for solving DISSOLVED agency or terminated agent more 7 years
      and (PD.businessunitmap=v_BUSINESSUNITMAP1
      or (nvl(AGY.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84)
      AND nvl(PAD.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84))
      )
     --end fix
      );
     COMMIT;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_COMM' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_COMM' , 'AIA_RPT_PRD_DIST_COMM_NEW 2'  , 'Processing', 'insert into AIA_RPT_PRD_DIST_COMM_NEW');


-----------=======LOG
INSERT
    /*+ Append */
  INTO AIA_RPT_PRD_DIST_COMM_NEW NOLOGGING
      (SELECT
        V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
        V_PROCESSINGUNITNAME PUNAME,
        decode(PD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') BUNAME,
        PD.businessunitmap BUMAP,
        V_CALENDARSEQ CALENDARSEQ,
        V_CALENDARNAME CALENDARNAME,
        V_PERIODSEQ PERIODKEY,
        V_PERIODNAME periodname,
        --PAD.POSITIONSEQ POSITIONSEQ,
        PAD.MANAGERSEQ,
        PAD.POSITIONSEQ POSITIONSEQ,
        PAD.NAME,
        SUBSTR(DISTRICT.NAME,4) DISTRICT_CODE,
        district.firstname
        || ' '
        || district.lastname DM_NAme,
        district.POS_GA2 DIST_LEADER_CODE,
        district.POS_GA7 DIST_LEADER_NAME,
        district.POS_GA11 DIST_LEAER_TITLE,
        DISTRICT.POS_GA4 DIST_LEADER_CLASS,
        PD.TRANS_GA11 UNIT_CODE,
        agy.firstname
        || ' '
        || agy.lastname AGENCY,
        agy.POS_GA2 UNIT_LEADER_CODE,
        agy.POS_GA7 UNIT_LEADER_NAME,
        DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
        agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
        (
        CASE
          WHEN AGY.POSITIONTITLE IN ('ORGANISATION','UNIT')
          THEN AGY.AGY_TERMINATIONDATE
        END)DISSOLVED_DATE,
        SUBSTR(PAD.NAME,4) AGT_CODE,
        (PAD.firstname
        || PAD.lastname) NAME,
        DECODE(PAD.positiontitle, 'FSC_NON_PROCESS', 'FSC', PAD.positiontitle) ROLE,
        Pad.POS_GA4 CLASS,
        PAD.HIREDATE CONTRACT_DATE,
        PAD.AGY_APPOINTMENT_DATE APPOINTMENT_DATE,
        PAD.AGY_TERMINATIONDATE TERMINATION_DATE,
        (
        CASE
          WHEN pad.PT_GA1 = '00'
          THEN 'INFORCE'
          WHEN pad.PT_GA1 IN ('50','51','52','55','56')
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 = '13'
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 IN ('60','61')
          THEN 'TERMINATED'
          WHEN pad.PT_GA1 = '70'
          THEN 'TERMINATED'
        END) AGENT_STATUS,
        0 LIFE_FYP,
        0 PA_FYP,
        CASE
          WHEN PD.CD_GA2 IN ('LF','HS')
          AND PD.CD_CREDITTYPE ='Case_Count'
          THEN PD.TRANS_GN5
          ELSE 0
        END LIFE_CASE,
        CASE
          WHEN PD.CD_GA2 = 'PA'
          AND PD.CD_CREDITTYPE ='Case_Count'
          THEN PD.TRANS_GN5
          ELSE 0
        END PA_CASE,
        0 LIFE_FYC_API_SSC,
        0 LIFE_FYC_RP,
 0 LIFE_FYC_NONILP,
 0 LIFE_FYC_ILP,
 0 LIFE_FYC_TOPUP,
        0 PA_FYC,
        0 CS_FYC,
        0 GI_SP_FYC,
0 GI_VL_FYC,
0 GI_RP_FYC,
        0 LIFE_2YEAR,
        0 LIFE_3YEAR,
        0 LIFE_4YEAR,
        0 PA_RC,
        0 CS_RC,
        0 CS_PL,
        0 UNASSIGNED_LIFE_2YEAR,
        0 UNASSIGNED_LIFE_3YEAR,
        0 UNASSIGNED_LIFE_4YEAR,
        0 UNASSIGNED_PA_RC,
        0 LIFE_FYP_YTD,
        0 PA_FYP_YTD,
        0 LIFE_CASE_YTD,
        0 PA_CASE_YTD,
        0 LIFE_FYC_API_SSC_YTD,
        0 LIFE_FYC_RP_YTD,
0 LIFE_FYC_NONILP_YTD,
0 LIFE_FYC_ILP_YTD,
 0 LIFE_FYC_TOPUP_YTD ,
        0 PA_FYC_YTD,
        0 CS_FYC_YTD,
         0 GI_SP_FYC_YTD,
 0 GI_VL_FYC_YTD,
 0 GI_RP_FYC_YTD,
        0 LIFE_2YEAR_YTD,
        0 LIFE_3YEAR_YTD,
        0 LIFE_4YEAR_YTD,
        0 PA_RC_YTD,
        0 CS_RC_YTD,
        0 CS_PL_YTD,
        0 UNASSIGNED_LIFE_2YEAR_YTD,
        0 UNASSIGNED_LIFE_3YEAR_YTD,
        0 UNASSIGNED_LIFE_4YEAR_YTD,
        0 UNASSIGNED_PA_RC_YTD,
        --add in version 2
        PD.CD_GA6  as POLICYNO,
        PD.CD_GN1 AS POLICYYEAR,
        --end to add in version 2
        ---add version 3
        PD.Assign_GN1
      FROM AIA_RPT_PRD_TEMP_PADIM pad,
           AIA_RPT_PRD_TEMP_PADIM agy,
           AIA_RPT_PRD_TEMP_PADIM agy_ldr,
           AIA_RPT_PRD_TEMP_PADIM DISTRICT,
           AIA_RPT_PRD_TEMP_VALUES PD
      WHERE PD.NEW_PIB_IND=0
      AND PAD.POSITIONTITLE IN ('AD','DIR','ED','FC','SD') --mod by drs 20160901
      --AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD','AM','FSD','FSM')                        --mod by drs 20160901
      AND PAD.POS_GA4 NOT IN ('45','48')
      AND PD.CD_POSITIONSEQ = PAD.POSITIONSEQ
      AND PD.CD_CREDITTYPE IN ('FYP', 'RYC', 'FYC','API','SSCP','APB','Case_Count')
      AND AGY.POS_GA1 = PD.TRANS_GA11
      AND AGY.POSITIONTITLE      IN ('ORGANISATION','UNIT')       --mod by drs 20160901
      --AND AGY.POSITIONTITLE      IN ('AGENCY','DISTRICT')                             --mod by drs 20160901
      AND ((agy_ldr.NAME = 'SGT'
        || agy.POS_GA2
      AND agy.NAME LIKE 'SGY%'))
      AND DISTRICT.POS_GA3 = AGY.POS_GA3
            AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
      AND district.positiontitle     IN ('ORGANISATION')          --mod by drs 20160901
      --AND district.positiontitle = 'DISTRICT'                           --mod by drs 20160901
      AND PD.CD_GA2  IN ('LF','HS','PA')
       --fix in version 2 for solving DISSOLVED agency or terminated agent more 7 years
     and (PD.businessunitmap=v_BUSINESSUNITMAP1
      or (nvl(AGY.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84)
      AND nvl(PAD.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84))
      )
     --end fix
      );

       /* End of above Insert to capture CASE COUNT Of Brunei Records which doesn't CreditType = 'FYP' coming from 'Case_Count */
      COMMIT;

------=======ADD LOG

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_COMM' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_COMM' , 'AIA_RPT_PRD_DIST_COMM_NEW 3'  , 'Processing', 'insert into AIA_RPT_PRD_DIST_COMM_NEW');


--- Adding Unassigned RC Logic below.
INSERT
    /*+ Append */
  INTO AIA_RPT_PRD_DIST_COMM_NEW NOLOGGING
( SELECT /*+ leading(agy,agy_ldr,pd,PAD) use_hash(agy,agy_ldr,PD,pad)*/
        V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
        V_PROCESSINGUNITNAME PUNAME,
        decode(PD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') BUNAME,
        PD.businessunitmap BUMAP,
        V_CALENDARSEQ CALENDARSEQ,
        V_CALENDARNAME CALENDARNAME,
        V_PERIODSEQ PERIODKEY,
        V_PERIODNAME periodname,
        --PAD_AGT.POSITIONSEQ POSITIONSEQ,
        PAD_AGT.MANAGERSEQ,
        PAD_AGT.POSITIONSEQ POSITIONSEQ,
        pad_agt.NAME,
        SUBSTR(DISTRICT.NAME,4) DISTRICT_CODE,
        district.firstname
        || ' '
        || district.lastname DM_NAme,
        district.POS_GA2 DIST_LEADER_CODE,
        district.POS_GA7 DIST_LEADER_NAME,
        district.POS_GA11 DIST_LEAER_TITLE,
        DISTRICT.POS_GA4 DIST_LEADER_CLASS,
        PD.CD_GA13 UNIT_CODE,
        agy.firstname
        || ' '
        || agy.lastname AGENCY,
        agy_ldr.POS_GA2 UNIT_LEADER_CODE,
        agy_ldr.POS_GA7 UNIT_LEADER_NAME,
        DECODE(agy_ldr.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy_ldr.POS_GA11) UNIT_LEAER_TITLE,
        agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
        (
        CASE
          WHEN agy.POSITIONTITLE IN ('ORGANISATION','UNIT')
          THEN agy.AGY_TERMINATIONDATE
        END)DISSOLVED_DATE,
        SUBSTR(pad_agt.NAME,4) AGT_CODE,
        (pad_agt.firstname
        || pad_agt.lastname) NAME,
        DECODE(pad_agt.positiontitle, 'FSC_NON_PROCESS', 'FSC', pad_agt.positiontitle) ROLE,
        pad_agt.POS_GA4 CLASS,
        pad_agt.HIREDATE CONTRACT_DATE,
        PAD.AGY_APPOINTMENT_DATE APPOINTMENT_DATE,
        pad_agt.AGY_TERMINATIONDATE TERMINATION_DATE,
        (
        CASE
        WHEN pad_agt.PT_GA1 = '00'
          THEN 'INFORCE'
          WHEN pad_agt.PT_GA1 IN ('50','51','52','55','56')
          THEN 'TERMINATED'
          WHEN pad_agt.PT_GA1 = '13'
          THEN 'TERMINATED'
          WHEN pad_agt.PT_GA1 IN ('60','61')
          THEN 'TERMINATED'
          WHEN pad_agt.PT_GA1 = '70'
          THEN 'TERMINATED'
        END) AGENT_STATUS,
        0 LIFE_FYP,
        0 PA_FYP,
        0 LIFE_CASE,
        0 PA_CASE,
        0 LIFE_FYC_API_SSC,
        0 LIFE_FYC_RP,
 0 LIFE_FYC_NONILP,
 0 LIFE_FYC_ILP,
 0 LIFE_FYC_TOPUP,
        0 PA_FYC,
        0 CS_FYC,
        0 GI_SP_FYC,
0 GI_VL_FYC,
0 GI_RP_FYC,
        0 LIFE_2YEAR,
        0 LIFE_3YEAR,
        0 LIFE_4YEAR,
        0 PA_RC,
        0 CS_RC,
        0 CS_PL,
        CASE
          WHEN PD.CD_GA4 = 'PAY2'
          AND PD.CD_GA2  IN ('LF','HS')
          THEN PD.CD_VALUE
          ELSE 0
        END UNASSIGNED_LIFE_2YEAR,
        CASE
          WHEN PD.CD_GA4 = 'PAY3'
          AND PD.CD_GA2   IN ('LF','HS')
          THEN PD.CD_VALUE
          ELSE 0
        END UNASSIGNED_LIFE_3YEAR,
        CASE
          WHEN PD.CD_GA4 IN ('PAY4','PAY5','PAY6')
          AND PD.CD_GA2   IN ('LF','HS')
          THEN PD.CD_VALUE
          ELSE 0
        END UNASSIGNED_LIFE_4YEAR,
        CASE
          WHEN PD.CD_GA2 IN ('PA')--,'VL')
          THEN PD.CD_VALUE
          ELSE 0
        END UNASSIGNED_PA_RC,
        0 LIFE_FYP_YTD,
        0 PA_FYP_YTD,
        0 LIFE_CASE_YTD,
        0 PA_CASE_YTD,
        0 LIFE_FYC_API_SSC_YTD,
        0 LIFE_FYC_RP_YTD,
 0 LIFE_FYC_NONILP_YTD,
 0 LIFE_FYC_ILP_YTD,
 0 LIFE_FYC_TOPUP_YTD ,
        0 PA_FYC_YTD,
        0 CS_FYC_YTD,
         0 GI_SP_FYC_YTD,
 0 GI_VL_FYC_YTD,
 0 GI_RP_FYC_YTD,
        0 LIFE_2YEAR_YTD,
        0 LIFE_3YEAR_YTD,
        0 LIFE_4YEAR_YTD,
        0 PA_RC_YTD,
        0 CS_RC_YTD,
        0 CS_PL_YTD,
        0 UNASSIGNED_LIFE_2YEAR_YTD,
        0 UNASSIGNED_LIFE_3YEAR_YTD,
        0 UNASSIGNED_LIFE_4YEAR_YTD,
        0 UNASSIGNED_PA_RC_YTD,
        --add in version 2
        PD.CD_GA6  as POLICYNO,
        PD.CD_GN1 AS POLICYYEAR,
        --end to add in version 2
        ---add version 3
        PD.Assign_GN1
        FROM
        AIA_RPT_PRD_TEMP_PADIM PAD,
        AIA_RPT_PRD_TEMP_PADIM pad_agt,
        AIA_RPT_PRD_TEMP_PADIM agy,
        AIA_RPT_PRD_TEMP_PADIM AGY_LDR,
        AIA_RPT_PRD_TEMP_PADIM DISTRICT,
        AIA_RPT_PRD_TEMP_VALUES PD
 WHERE PD.NEW_PIB_IND=0
      AND PD.CD_POSITIONSEQ = PAD.POSITIONSEQ
      AND PD.CD_CREDITTYPE  IN ('ORYC', 'RYC','Case_Count')
      AND PAD.POSITIONTITLE IN ('AD','DIR','ED','FC','SD')  --mod by drs 20160901
      --AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD','AM','FSD','FSM')                         --mod by drs 20160901
      AND PAD.POS_GA4 NOT IN ('45')--,'48')
      AND AGY.POS_GA1 = PD.CD_GA13
      AND SUBSTR(PAD.NAME,4)  = PD.TRANS_GA11
      AND SUBSTR(PAD_AGT.NAME,4) = PD.TRANS_GA10 AND SUBSTR(PAD_AGT.NAME,3,1)='T'
      AND AGY.POSITIONTITLE      IN ('ORGANISATION','UNIT')          --mod by drs 20160901
      --AND AGY.POSITIONTITLE      IN ('AGENCY','DISTRICT')                                --mod by drs 20160901
      AND ((agy_ldr.NAME = 'SGT'
        || agy.POS_GA2
      AND agy.NAME LIKE 'SGY%')
      OR (agy_ldr.NAME = 'BRT'
        || agy.POS_GA2
      AND agy.NAME LIKE 'BRY%'))
      AND DISTRICT.POS_GA3 = AGY.POS_GA3
      AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
      AND district.positiontitle     IN ('ORGANISATION')                       --mod by drs 20160901
      --AND district.positiontitle = 'DISTRICT'                                          --mod by drs 20160901
      AND PD.CD_GA2   IN ('LF','PA','HS','VL')
     -- AND PD.CD_GA4     IN ('PAY2','PAY3','PAY4','PAY5','PAY6')
      and PD.businessunitmap = pad_agt.Businessunitmap
      and PD.businessunitmap = agy.Businessunitmap
      and PD.businessunitmap = AGY_LDR.Businessunitmap
      and PD.businessunitmap = DISTRICT.Businessunitmap
      --fix in version 2 for solving DISSOLVED agency or terminated agent more 7 years
      and (PD.businessunitmap=v_BUSINESSUNITMAP1
      or (nvl(AGY.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84)
      AND nvl(PAD.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84))
      )
     --end fix
);
COMMIT;

------=======ADD LOG

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_COMM' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_COMM' , 'AIA_RPT_PRD_DIST_COMM_TMP'  , 'Processing', 'insert into AIA_RPT_PRD_DIST_COMM_TMP');



----------------line 622
DELETE
  FROM AIA_RPT_PRD_DIST_COMM_TMP WHERE PERIODNAME = V_PERIODNAME ;

  COMMIT;


INSERT /*+ Append */
  INTO AIA_RPT_PRD_DIST_COMM_TMP NOLOGGING
  SELECT MAX(PROCESSINGUNITSEQ) PROCESSINGUNITSEQ,
    MAX(PUNAME) PUNAME,
    BUNAME,
    BUMAP,
    MAX(CALENDARSEQ) CALENDARSEQ,
    MAX(CALENDARNAME) CALENDARNAME,
    MAX(PERIODKEY) PERIODKEY,
    MAX(PERIODNAME) PERIODNAME,
    --MAX(POSITIONSEQ) POSITIONSEQ,
    MAX(MANAGERSEQ) MANAGERSEQ,
    MAX(POSITIONSEQ) POSITIONSEQ,
    POSITIONNAME,
    DISTRICT_CODE,
    MAX(DM_NAME) DM_NAME,
    MAX(DIST_LEADER_CODE) DIST_LEADER_CODE,
    MAX(DIST_LEADER_NAME) DIST_LEADER_NAME,
    MAX(DIST_LEAER_TITLE) DIST_LEAER_TITLE,
    MAX(DIST_LEADER_CLASS) DIST_LEADER_CLASS,
    UNIT_CODE UNIT_CODE,
    MAX(AGENCY) AGENCY,
    MAX(UNIT_LEADER_CODE) UNIT_LEADER_CODE ,
    MAX(UNIT_LEADER_NAME) UNIT_LEADER_NAME,
    MAX(UNIT_LEAER_TITLE) UNIT_LEAER_TITLE,
    MAX(UNIT_LEADER_CLASS) UNIT_LEADER_CLASS,
    MAX(DISSOLVED_DATE) DISSOLVED_DATE,
    AGT_CODE AGT_CODE,
    MAX(NAME) NAME,
    MAX(ROLE) ROLE ,
    MAX(CLASS) CLASS,
    MAX(CONTRACT_DATE) CONTRACT_DATE,
    MAX(APPOINTMENT_DATE) APPOINTMENT_DATE,
    MAX(TERMINATION_DATE) TERMINATION_DATE,
    MAX(AGENT_STATUS) AGENT_STATUS,
    SUM(LIFE_FYP) LIFE_FYP,
    SUM(PA_FYP) PA_FYP,
    SUM(LIFE_CASE) LIFE_CASE,
    SUM(PA_CASE) PA_CASE,
    SUM(LIFE_FYC_API_SSC) LIFE_FYC_API_SSC,
    SUM(LIFE_FYC_RP) LIFE_FYC_RP,
 SUM(LIFE_FYC_NONILP) LIFE_FYC_NONILP,
 SUM(LIFE_FYC_ILP) LIFE_FYC_ILP,
 SUM(LIFE_FYC_TOPUP) LIFE_FYC_TOPUP,
    SUM(PA_FYC) PA_FYC,
    SUM(CS_FYC) CS_FYC,
    SUM(GI_SP_FYC) GI_SP_FYC,
SUM(GI_VL_FYC) GI_VL_FYC,
SUM(GI_RP_FYC) GI_RP_FYC,
    SUM(LIFE_2YEAR) LIFE_2YEAR,
    SUM(LIFE_3YEAR) LIFE_3YEAR,
    SUM(LIFE_4YEAR) LIFE_4YEAR,
    SUM(PA_RC) PA_RC,
    SUM(CS_RC) CS_RC,
    SUM(CS_PL) CS_PL,
    SUM(UNASSIGNED_LIFE_2YEAR) UNASSIGNED_LIFE_2YEAR,
    SUM(UNASSIGNED_LIFE_3YEAR) UNASSIGNED_LIFE_3YEAR,
    SUM(UNASSIGNED_LIFE_4YEAR) UNASSIGNED_LIFE_4YEAR,
    SUM(UNASSIGNED_PA_RC) UNASSIGNED_PA_RC,
    SUM(LIFE_FYP_YTD) LIFE_FYP_YTD,
    SUM(PA_FYP_YTD) PA_FYP_YTD,
    SUM(LIFE_CASE_YTD) LIFE_CASE_YTD,
    SUM(PA_CASE_YTD) PA_CASE_YTD,
    SUM(LIFE_FYC_API_SSC_YTD) LIFE_FYC_API_SSC_YTD,
    SUM(LIFE_FYC_RP_YTD) LIFE_FYC_RP_YTD,
  SUM(LIFE_FYC_NONILP_YTD) LIFE_FYC_NONILP_YTD,
  SUM(LIFE_FYC_ILP_YTD) LIFE_FYC_ILP_YTD,
 SUM(LIFE_FYC_TOPUP_YTD) LIFE_FYC_TOPUP_YTD,
    SUM(PA_FYC_YTD) PA_FYC_YTD,
    SUM(CS_FYC_YTD) CS_FYC_YTD,
    SUM(GI_SP_FYC_YTD) GI_SP_FYC_YTD,
SUM(GI_VL_FYC_YTD) GI_VL_FYC_YTD,
SUM(GI_RP_FYC_YTD) GI_RP_FYC_YTD,
    SUM(LIFE_2YEAR_YTD) LIFE_2YEAR_YTD,
    SUM(LIFE_3YEAR_YTD) LIFE_3YEAR_YTD,
    SUM(LIFE_4YEAR_YTD) LIFE_4YEAR_YTD,
    SUM(PA_RC_YTD) PA_RC_YTD,
    SUM(CS_RC_YTD) CS_RC_YTD,
    SUM(CS_PL_YTD) CS_PL_YTD,
    SUM(UNASSIGNED_LIFE_2YEAR_YTD) UNASSIGNED_LIFE_2YEAR_YTD,
    SUM(UNASSIGNED_LIFE_3YEAR_YTD) UNASSIGNED_LIFE_3YEAR_YTD,
    SUM(UNASSIGNED_LIFE_4YEAR_YTD) UNASSIGNED_LIFE_4YEAR_YTD,
    SUM(UNASSIGNED_PA_RC_YTD) UNASSIGNED_PA_RC_YTD
  FROM
    (      --change in version 2
                  SELECT MAX(PROCESSINGUNITSEQ) PROCESSINGUNITSEQ,
                    MAX(PUNAME) PUNAME,
                    BUNAME,
                    BUMAP,
                    MAX(CALENDARSEQ) CALENDARSEQ,
                    MAX(CALENDARNAME) CALENDARNAME,
                    MAX(PERIODKEY) PERIODKEY,
                    MAX(PERIODNAME) PERIODNAME,
                  --  MAX(PAKEY) PAKEY,
                    MAX(MANAGERSEQ) MANAGERSEQ,
                    MAX(POSITIONSEQ) POSITIONSEQ,
                    POSITIONNAME,
                    DISTRICT_CODE,
                    MAX(DM_NAME) DM_NAME,
                    MAX(DIST_LEADER_CODE) DIST_LEADER_CODE,
                    MAX(DIST_LEADER_NAME) DIST_LEADER_NAME,
                    MAX(DIST_LEAER_TITLE) DIST_LEAER_TITLE,
                  MAX(DIST_LEADER_CLASS) DIST_LEADER_CLASS,
                  UNIT_CODE UNIT_CODE,
                  max(AGENCY) AGENCY,
                  MAX(UNIT_LEADER_CODE) UNIT_LEADER_CODE ,
                  MAX(UNIT_LEADER_NAME) UNIT_LEADER_NAME,
                  MAX(UNIT_LEAER_TITLE) UNIT_LEAER_TITLE,
                  MAX(UNIT_LEADER_CLASS) UNIT_LEADER_CLASS,
                  MAX(DISSOLVED_DATE) DISSOLVED_DATE,
                  AGT_CODE AGT_CODE,
                  MAX(NAME) NAME,
                  MAX(ROLE) ROLE ,
                  MAX(CLASS) CLASS,
                  MAX(CONTRACT_DATE) CONTRACT_DATE,
                  MAX(APPOINTMENT_DATE) APPOINTMENT_DATE,
                  MAX(TERMINATION_DATE) TERMINATION_DATE,
                  MAX(AGENT_STATUS) AGENT_STATUS,
                  SUM(LIFE_FYP*nvl(Assign_GN1,0)) LIFE_FYP,---add version 3
                  SUM(PA_FYP*nvl(Assign_GN1,0)) PA_FYP,    ---add version 3
                  SUM(LIFE_CASE) LIFE_CASE,
                  SUM(PA_CASE) PA_CASE,
                  SUM(LIFE_FYC_API_SSC) LIFE_FYC_API_SSC,
                  SUM(LIFE_FYC_RP) LIFE_FYC_RP,
 SUM(LIFE_FYC_NONILP) LIFE_FYC_NONILP,
 SUM(LIFE_FYC_ILP*nvl(Assign_GN1,0)) LIFE_FYC_ILP,
 SUM(LIFE_FYC_TOPUP*nvl(Assign_GN1,0)) LIFE_FYC_TOPUP,
                  SUM(PA_FYC) PA_FYC,
                  SUM(CS_FYC) CS_FYC,
                  SUM(GI_SP_FYC) GI_SP_FYC,
SUM(GI_VL_FYC) GI_VL_FYC,
SUM(GI_RP_FYC) GI_RP_FYC,
                  SUM(LIFE_2YEAR) LIFE_2YEAR,
                  SUM(LIFE_3YEAR) LIFE_3YEAR,
                  SUM(LIFE_4YEAR) LIFE_4YEAR,
                  SUM(PA_RC) PA_RC,
                  SUM(CS_RC) CS_RC,
                  SUM(CS_PL) CS_PL,
                  SUM(UNASSIGNED_LIFE_2YEAR) UNASSIGNED_LIFE_2YEAR,
                  SUM(UNASSIGNED_LIFE_3YEAR) UNASSIGNED_LIFE_3YEAR,
                  SUM(UNASSIGNED_LIFE_4YEAR) UNASSIGNED_LIFE_4YEAR,
                  SUM(UNASSIGNED_PA_RC) UNASSIGNED_PA_RC,
                  SUM(LIFE_FYP_YTD*nvl(Assign_GN1,0)) LIFE_FYP_YTD,  ---add version 3
                  SUM(PA_FYP_YTD*nvl(Assign_GN1,0)) PA_FYP_YTD,       ---add version 3
                  SUM(LIFE_CASE_YTD) LIFE_CASE_YTD,
                  SUM(PA_CASE_YTD) PA_CASE_YTD,
                  SUM(LIFE_FYC_API_SSC_YTD) LIFE_FYC_API_SSC_YTD,
                  SUM(LIFE_FYC_RP_YTD) LIFE_FYC_RP_YTD,
                  SUM(LIFE_FYC_NONILP_YTD) LIFE_FYC_NONILP_YTD,
                  SUM(LIFE_FYC_ILP_YTD) LIFE_FYC_ILP_YTD,
                  SUM(LIFE_FYC_TOPUP_YTD) LIFE_FYC_TOPUP_YTD,
                  SUM(PA_FYC_YTD) PA_FYC_YTD,
                  SUM(CS_FYC_YTD) CS_FYC_YTD,
                  SUM(GI_SP_FYC_YTD) GI_SP_FYC_YTD,
 SUM(GI_VL_FYC_YTD) GI_VL_FYC_YTD,
 SUM(GI_RP_FYC_YTD) GI_RP_FYC_YTD,
                  SUM(LIFE_2YEAR_YTD) LIFE_2YEAR_YTD,
                  SUM(LIFE_3YEAR_YTD) LIFE_3YEAR_YTD,
                  SUM(LIFE_4YEAR_YTD) LIFE_4YEAR_YTD,
                  SUM(PA_RC_YTD) PA_RC_YTD,
                  SUM(CS_RC_YTD) CS_RC_YTD,
                  SUM(CS_PL_YTD) CS_PL_YTD,
                  SUM(UNASSIGNED_LIFE_2YEAR_YTD) UNASSIGNED_LIFE_2YEAR_YTD,
                  SUM(UNASSIGNED_LIFE_3YEAR_YTD) UNASSIGNED_LIFE_3YEAR_YTD,
                  SUM(UNASSIGNED_LIFE_4YEAR_YTD) UNASSIGNED_LIFE_4YEAR_YTD,
                  SUM(UNASSIGNED_PA_RC_YTD) UNASSIGNED_PA_RC_YTD
                 FROM AIA_RPT_PRD_DIST_COMM_NEW
                 where BUNAME='SGPAFA'
                  GROUP BY DISTRICT_CODE,
              BUNAME,
              BUMAP,
              POSITIONNAME,
              UNIT_CODE,
              AGT_CODE,
              POLICYNO,
              POLICYYEAR
      UNION ALL
              --end to change in version 2
--     SELECT PROCESSINGUNITSEQ,
--          PUNAME,
--          BUNAME,
--          BUMAP,
--          CALENDARSEQ,
--          CALENDARNAME,
--          PERIODKEY,
--          periodname,
--          --POSITIONSEQ,
--          MANAGERSEQ,
--          POSITIONSEQ,
--          POSITIONNAME,
--          DISTRICT_CODE,
--          DM_NAme,
--          dIST_LEADER_CODE,
--          DIST_LEADER_NAME,
--          DIST_LEAER_TITLE,
--          DIST_LEADER_CLASS,
--          UNIT_CODE,
--          AGENCY,
--          UNIT_LEADER_CODE,
--          UNIT_LEADER_NAME,
--          UNIT_LEAER_TITLE,
--          UNIT_LEADER_CLASS,
--          DISSOLVED_DATE,
--          AGT_CODE,
--          NAME,
--          ROLE,
--          CLASS,
--          CONTRACT_DATE,
--          APPOINTMENT_DATE,
--          TERMINATION_DATE,
--          AGENT_STATUS,
--          LIFE_FYP,
--          PA_FYP,
--           LIFE_CASE,
--          PA_CASE,
--          LIFE_FYC_API_SSC,
--          LIFE_FYC_RP,
-- LIFE_FYC_NONILP,
-- LIFE_FYC_TOPUP,
--          PA_FYC,
--          CS_FYC,
--           GI_SP_FYC,
--GI_VL_FYC,
--GI_RP_FYC,
--          LIFE_2YEAR,
--          LIFE_3YEAR,
--          LIFE_4YEAR,
--          PA_RC,
--           CS_RC,
--          CS_PL,
--          UNASSIGNED_LIFE_2YEAR,
--           UNASSIGNED_LIFE_3YEAR,
--          UNASSIGNED_LIFE_4YEAR,
--          UNASSIGNED_PA_RC ,
--          LIFE_FYP_YTD ,
--          PA_FYP_YTD ,
--           LIFE_CASE_YTD ,
--          PA_CASE_YTD ,
--          LIFE_FYC_API_SSC_YTD ,
--          LIFE_FYC_RP_YTD,
-- LIFE_FYC_NONILP_YTD,
-- LIFE_FYC_TOPUP_YTD ,
--          PA_FYC_YTD ,
--          CS_FYC_YTD ,
-- GI_SP_FYC,
--GI_VL_FYC,
--GI_RP_FYC,
--          LIFE_2YEAR_YTD ,
--           LIFE_3YEAR_YTD ,
--          LIFE_4YEAR_YTD ,
--          PA_RC_YTD ,
--          CS_RC_YTD ,
--          CS_PL_YTD ,
--          UNASSIGNED_LIFE_2YEAR_YTD ,
--         UNASSIGNED_LIFE_3YEAR_YTD ,
--          UNASSIGNED_LIFE_4YEAR_YTD ,
--          UNASSIGNED_PA_RC_YTD
--        FROM AIA_RPT_PRD_DIST_COMM_NEW
--        --change in version 2
--        where BUNAME='SGPAFA'
--    --end change
--     UNION ALL
    --- Adding YTD Logic.
    SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
      PUNAME,
      BUNAME,
      BUMAP,
      CALENDARSEQ,
      CALENDARNAME,
      V_PERIODSEQ PERIODKEY,
      V_PERIODNAME periodname,
      --POSITIONSEQ,
      MANAGERSEQ,
      POSITIONSEQ,
      POSITIONNAME,
      DISTRICT_CODE,
      DM_NAme,
      dIST_LEADER_CODE,
      DIST_LEADER_NAME,
      DIST_LEAER_TITLE,
      DIST_LEADER_CLASS,
      UNIT_CODE,
      AGENCY,
      UNIT_LEADER_CODE,
      UNIT_LEADER_NAME,
      UNIT_LEAER_TITLE,
      UNIT_LEADER_CLASS,
      to_date(DISSOLVED_DATE, 'mm/dd/yyyy') DISSOLVED_DATE,
      AGT_CODE,
      NAME,
      ROLE,
      CLASS,
      CONTRACT_DATE,
      APPOINTMENT_DATE,
      TERMINATION_DATE,
      AGENT_STATUS,
      0 LIFE_FYP,
      0 PA_FYP,
      0 LIFE_CASE,
      0 PA_CASE,
      0 LIFE_FYC_API_SSC,
      0 LIFE_FYC_RP,
 0 LIFE_FYC_NONILP,
 0 LIFE_FYC_ILP,
 0 LIFE_FYC_TOPUP,
      0 PA_FYC,
      0 CS_FYC,
              0 GI_SP_FYC,
0 GI_VL_FYC,
0 GI_RP_FYC,
      0 LIFE_2YEAR,
      0 LIFE_3YEAR,
      0 LIFE_4YEAR,
      0 PA_RC,
      0 CS_RC,
      0 CS_PL,
      0 UNASSIGNED_LIFE_2YEAR,
      0 UNASSIGNED_LIFE_3YEAR,
      0 UNASSIGNED_LIFE_4YEAR,
      0 UNASSIGNED_PA_RC,
      LIFE_FYP   LIFE_FYP_YTD,
      PA_FYP PA_FYP_YTD,
      LIFE_CASE LIFE_CASE_YTD,
      PA_CASE PA_CASE_YTD,
     LIFE_FYC_API_SSC LIFE_FYC_API_SSC_YTD,
      LIFE_FYC_RP LIFE_FYC_RP_YTD,
LIFE_FYC_NONILP LIFE_FYC_NONILP_YTD,
LIFE_FYC_ILP LIFE_FYC_ILP_YTD,
LIFE_FYC_TOPUP LIFE_FYC_TOPUP_YTD ,
PA_FYC PA_FYC_YTD,
CS_FYC CS_FYC_YTD,
GI_SP_FYC GI_SP_FYC_YTD,
GI_VL_FYC GI_VL_FYC_YTD,
GI_RP_FYC GI_RP_FYC_YTD,
LIFE_2YEAR LIFE_2YEAR_YTD,
LIFE_3YEAR LIFE_3YEAR_YTD,
LIFE_4YEAR LIFE_4YEAR_YTD,
PA_RC PA_RC_YTD,
CS_RC CS_RC_YTD,
CS_PL CS_PL_YTD,
UNASSIGNED_LIFE_2YEAR UNASSIGNED_LIFE_2YEAR_YTD,
UNASSIGNED_LIFE_3YEAR UNASSIGNED_LIFE_3YEAR_YTD,
UNASSIGNED_LIFE_4YEAR UNASSIGNED_LIFE_4YEAR_YTD,
UNASSIGNED_PA_RC UNASSIGNED_PA_RC_YTD
--     (LIFE_FYP+LIFE_FYP_YTD) LIFE_FYP_YTD,
--      (PA_FYP+PA_FYP_YTD) PA_FYP_YTD,
--      (LIFE_CASE+LIFE_CASE_YTD) LIFE_CASE_YTD,
--      (PA_CASE+PA_CASE_YTD) PA_CASE_YTD,
--      (LIFE_FYC_API_SSC+LIFE_FYC_API_SSC_YTD) LIFE_FYC_API_SSC_YTD,
--      (LIFE_FYC_RP+LIFE_FYC_RP_YTD) LIFE_FYC_RP_YTD,
--(LIFE_FYC_NONILP+LIFE_FYC_NONILP_YTD)  LIFE_FYC_NONILP_YTD,
-- (LIFE_FYC_ILP+LIFE_FYC_ILP_YTD) LIFE_FYC_ILP_YTD,
--(LIFE_FYC_TOPUP+LIFE_FYC_TOPUP_YTD) LIFE_FYC_TOPUP_YTD ,
--     (PA_FYC+PA_FYC_YTD) PA_FYC_YTD,
--       (CS_FYC+CS_FYC_YTD) CS_FYC_YTD,
--      (GI_SP_FYC+GI_SP_FYC_YTD) GI_SP_FYC_YTD,
--(GI_VL_FYC+GI_VL_FYC_YTD) GI_VL_FYC_YTD,
-- (GI_RP_FYC+GI_RP_FYC_YTD) GI_RP_FYC_YTD,
--    (LIFE_2YEAR+LIFE_2YEAR_YTD)  LIFE_2YEAR_YTD,
--     (LIFE_3YEAR+LIFE_3YEAR_YTD) LIFE_3YEAR_YTD,
--     (LIFE_4YEAR+LIFE_4YEAR_YTD) LIFE_4YEAR_YTD,
--      (PA_RC+PA_RC_YTD) PA_RC_YTD,
--       (CS_RC+CS_RC_YTD) CS_RC_YTD,
--     (CS_PL+CS_PL_YTD) CS_PL_YTD,
--      (UNASSIGNED_LIFE_2YEAR+UNASSIGNED_LIFE_2YEAR_YTD) UNASSIGNED_LIFE_2YEAR_YTD,
--(UNASSIGNED_LIFE_3YEAR+UNASSIGNED_LIFE_3YEAR_YTD)  UNASSIGNED_LIFE_3YEAR_YTD,
--(UNASSIGNED_LIFE_4YEAR+UNASSIGNED_LIFE_4YEAR_YTD)  UNASSIGNED_LIFE_4YEAR_YTD,
--(UNASSIGNED_PA_RC+UNASSIGNED_PA_RC_YTD)   UNASSIGNED_PA_RC_YTD
    FROM AIA_RPT_PRD_DIST_COMM
    WHERE periodname IN
    (SELECT name
      FROM CS_PERIOD
      WHERE periodtypeseq = V_PERIODTYPESEQ
      AND calendarseq = V_CALENDARSEQ
      AND V_PERIODNAME NOT LIKE 'Dec%'
      AND REMOVEDATE=to_date('01/01/2200','mm/dd/yyyy')
      AND STARTDATE >=trunc(V_STARTDATE,'year')
      and ENDDATE<= V_ENDDATE
      )
      -- change it in version 2
      and (BUNAME='SGPAFA'
      or
      (nvl( to_date(DISSOLVED_DATE, 'mm/dd/yyyy') ,v_eot) > add_months(V_PERIODSTARTDATE,-84)
      and nvl( TERMINATION_DATE,v_eot) > add_months(V_PERIODSTARTDATE,-84))
      )
      -- end to change it in version 2
    )
  GROUP BY DISTRICT_CODE,
  BUNAME,
  BUMAP,
  POSITIONNAME,
  UNIT_CODE,
  AGT_CODE   ;


COMMIT;

------=======ADD LOG


--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_COMM' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_COMM' , 'BUILD HIERARCHY LIST'  , 'Processing', '');


-----------start from line 880
EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_AGENCY_LIST';

INSERT /*+ Append  */
INTO AIA_RPT_AGENCY_LIST NOLOGGING
  (AGY_CODE,
   AGY_PARTICIPANTID,
   AGY_NAME,
   AGY_POSITIONTITLE,
   AGY_POS_GA4,
   AGY_POS_GA2,
   AGY_POS_GA3,
   AGY_TERMINATIONDATE,
   AGY_APPOINTMENT_DATE,
   AGY_POS_GA9,
   NEW_DISTRICT_CODE,
   AGY_BUNAME)
  (select CHILDPAD.POS_GA1 AGY_CODE,
          CHILDPAD.name AGY_PARTICIPANTID,
          CHILDPAD.FIRSTNAME || ' ' || CHILDPAD.LASTNAME AGY_NAME,
          DECODE(CHILDPAD.POSITIONTITLE,
                 'FSC_NON_PROCESS',
                 'FSC',
                 'BR_FSC_NON_PROCESS',
                 'FSC',
                 'AM_FSC_NON_PROCESS',
                 'FSC',
                 'AM_NON_PROCESS',
                 'AM',
                 'FSD_NON_PROCESS',
                 'FSD',
                 'FSAD_NON_PROCESS',
                 'FSAD',
                 'BR_Staff',
                 'Staff',
                 'BR_DM',
                 'DM',
                 'BR_DM_NON_PROCESS',
                 'DM',
                 'BR_FSC',
                 'FSC',
                 'BR_Staff_NON_PROCESS',
                 'Staff',
                 'BR_UM',
                 'UM',
                 'BR_UM_NON_PROCESS',
                 'UM',
                 'Staff_NON_PROCESS',
                 'Staff',
                 CHILDPAD.POSITIONTITLE) AGY_POSITIONTITLE,
          CHILDPAD.POS_GA4 AGY_POS_GA4,
          CHILDPAD.POS_GA2 AGY_POS_GA2,
          CHILDPAD.POS_GA3 AGY_POS_GA3,
          CHILDPAD.AGY_TERMINATIONDATE AGY_TERMINATIONDATE,
          CHILDPAD.AGY_APPOINTMENT_DATE AGY_APPOINTMENT_DATE,
          CHILDPAD.POS_GA9 AGY_POS_GA9,
          PARENTPAD.POS_GA3 NEW_DISTRICT_CODE,
          decode(CHILDPAD.businessunitmap,
                 v_BUSINESSUNITMAP1,
                 'SGPAFA',
                 v_BUSINESSUNITMAP2,
                 'BRUAGY') AGY_BUNAME
     from AIA_RPT_PRD_TEMP_PADIM CHILDPAD
     INNER JOIN cs_positionrelation PR
     ON CHILDPAD.positionseq = PR.childpositionseq
     AND PR.positionrelationtypeseq =
          (select datatypeseq
             from cs_positionrelationtype
            where name = 'PBA_Roll'
              and removedate = V_EOT)
     INNER JOIN AIA_RPT_PRD_TEMP_PADIM PARENTPAD
     ON PARENTPAD.positionseq = PR.parentpositionseq
     WHERE CHILDPAD.POSITIONTITLE IN ('ORGANISATION','UNIT')
     AND CHILDPAD.POS_GA9 = 'Y'

     UNION

     select PAD.POS_GA1 AGY_CODE,
          PAD.name AGY_PARTICIPANTID,
          PAD.FIRSTNAME || ' ' || PAD.LASTNAME AGY_NAME,
          DECODE(PAD.POSITIONTITLE,
                 'FSC_NON_PROCESS',
                 'FSC',
                 'BR_FSC_NON_PROCESS',
                 'FSC',
                 'AM_FSC_NON_PROCESS',
                 'FSC',
                 'AM_NON_PROCESS',
                 'AM',
                 'FSD_NON_PROCESS',
                 'FSD',
                 'FSAD_NON_PROCESS',
                 'FSAD',
                 'BR_Staff',
                 'Staff',
                 'BR_DM',
                 'DM',
                 'BR_DM_NON_PROCESS',
                 'DM',
                 'BR_FSC',
                 'FSC',
                 'BR_Staff_NON_PROCESS',
                 'Staff',
                 'BR_UM',
                 'UM',
                 'BR_UM_NON_PROCESS',
                 'UM',
                 'Staff_NON_PROCESS',
                 'Staff',
                 PAD.POSITIONTITLE) AGY_POSITIONTITLE,
          PAD.POS_GA4 AGY_POS_GA4,
          PAD.POS_GA2 AGY_POS_GA2,
          PAD.POS_GA3 AGY_POS_GA3,
          PAD.AGY_TERMINATIONDATE AGY_TERMINATIONDATE,
          PAD.AGY_APPOINTMENT_DATE AGY_APPOINTMENT_DATE,
          PAD.POS_GA9 AGY_POS_GA9,
          PAD.POS_GA3 NEW_DISTRICT_CODE,
          decode(PAD.businessunitmap,
                 v_BUSINESSUNITMAP1,
                 'SGPAFA',
                 v_BUSINESSUNITMAP2,
                 'BRUAGY') AGY_BUNAME

     from AIA_RPT_PRD_TEMP_PADIM PAD
     WHERE (PAD.POS_GA9 = 'N'
       OR PAD.POS_GA9  IS NULL)
     AND PAD.POSITIONTITLE  IN ('ORGANISATION','UNIT')
);
COMMIT;



------=======ADD LOG1

EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_AGENT_LIST';
INSERT  /*+ Append  */
INTO AIA_RPT_AGENT_LIST NOLOGGING
(fsc_id ,
FSC_NAME ,
FSC_TITLE,
FSC_CLASS ,
FSC_HIREDATE ,
FSC_TERMINATION_DATE ,
FSC_STATUS ,
FSC_ASSIGNED_DATE ,
FSC_APPOINTMENT_DATE ,
FSC_BUNAME
)
(SELECT SUBSTR(NAME, 4)fsc_id ,
PAD.firstname || ' ' || PAD.LASTNAME FSC_NAME ,
DECODE(PAD.POSITIONTITLE,
    'FSC_NON_PROCESS', 'FSC', 'BR_FSC_NON_PROCESS',
    'FSC', 'AM_FSC_NON_PROCESS', 'FSC', 'AM_NON_PROCESS', 'AM' ,
    'FSD_NON_PROCESS', 'FSD',
    'FSAD_NON_PROCESS', 'FSAD',
    'BR_Staff' , 'Staff',
    'BR_DM', 'DM',
    'BR_DM_NON_PROCESS', 'DM',
    'BR_FSC' , 'FSC',
    'BR_Staff_NON_PROCESS', 'Staff',
    'BR_UM','UM',
    'BR_UM_NON_PROCESS', 'UM',
    'Staff_NON_PROCESS', 'Staff',
     PAD.POSITIONTITLE) FSC_TITLE,
PAD.POS_GA4 FSC_CLASS ,
PAD.hiredate FSC_HIREDATE ,
PAD.AGY_TERMINATIONDATE FSC_TERMINATION_DATE ,
(CASE WHEN PAD.PT_GA1 = '00' THEN 'INFORCE'
        WHEN PAD.PT_GA1 IN ('50','51','52','55','56') THEN 'TERMINATED'
          WHEN PAD.PT_GA1 = '13' then  'TERMINATED'
          WHEN PAD.PT_GA1 IN ('60','61') then  'TERMINATED'
          WHEN PAD.PT_GA1 = '70' THEN  'TERMINATED'
        END) FSC_STATUS ,
PAD.ASSIGNED_DATE FSC_ASSIGNED_DATE ,
PAD.AGY_APPOINTMENT_DATE FSC_APPOINTMENT_DATE ,
decode(PAD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') FSC_BUNAME
FROM AIA_RPT_PRD_TEMP_PADIM PAD
WHERE PAD.POSITIONTITLE NOT IN ('ORGANISATION','UNIT')

);

COMMIT;

------=======ADD LOG

EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_AGENCY_LEADER';

INSERT  /*+ Append  */
INTO AIA_RPT_AGENCY_LEADER NOLOGGING
(AGY_LDR_CODE ,
AGY_LDR_PARTICIPANTID,
AGY_LDR_NAME ,
AGY_LDR_TITLE,
AGY_LDR_POS_GA2 ,
AGY_LDR_POS_GA4,
AGY_LDR_POS_GA3,
AGY_LDR_BUNAME
)
(SELECT SUBSTR(NAME, 4)AGY_LDR_CODE ,
NAME AGY_LDR_PARTICIPANTID,
PAD.firstname || ' ' || PAD.LASTNAME AGY_LDR_NAME ,
DECODE(PAD.POSITIONTITLE,
    'FSC_NON_PROCESS', 'FSC', 'BR_FSC_NON_PROCESS',
    'FSC', 'AM_FSC_NON_PROCESS', 'FSC', 'AM_NON_PROCESS', 'AM' ,
    'FSD_NON_PROCESS', 'FSD',
    'FSAD_NON_PROCESS', 'FSAD',
    'BR_Staff' , 'Staff',
    'BR_DM', 'DM',
    'BR_DM_NON_PROCESS', 'DM',
    'BR_FSC' , 'FSC',
    'BR_Staff_NON_PROCESS', 'Staff',
    'BR_UM','UM',
    'BR_UM_NON_PROCESS', 'UM',
    'Staff_NON_PROCESS', 'Staff',
     PAD.POSITIONTITLE) AGY_LDR_TITLE,
     PAD.POS_GA2 AGY_LDR_POS_GA2,
     PAD.POS_GA4 AGY_LDR_POS_GA4 ,
     PAD.POS_GA3 AGY_LDR_POS_GA3,
decode(PAD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') AGY_LDR_BUNAME
FROM  AIA_RPT_PRD_TEMP_PADIM PAD

);

COMMIT;

------=======ADD LOG



EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_DISTRICT_LIST';

INSERT  /*+ Append  */
INTO AIA_RPT_DISTRICT_LIST NOLOGGING
(DIST_CODE ,
DIST_PARTICIPANTID,
DIST_NAME ,
DIST_TITLE,
DIST_POS_GA2 ,
DIST_POS_GA4,
DIST_POS_GA3,
DIST_BUNAME
)
(SELECT SUBSTR(NAME, 4)DIST_CODE ,
        NAME DIST_PARTICIPANTID,
        PAD.firstname || ' ' || PAD.LASTNAME DIST_NAME ,
        PAD.POSITIONTITLE DIST_TITLE,
        PAD.POS_GA2 DIST_POS_GA2,
        PAD.POS_GA4 DIST_POS_GA4 ,
        PAD.POS_GA3 DIST_POS_GA3,
        decode(PAD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') DIST_BUNAME
FROM AIA_RPT_PRD_TEMP_PADIM PAD
        WHERE PAD.POSITIONTITLE NOT LIKE '%FSC%'
);

COMMIT;

------=======ADD LO

EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_DISTRICT_LEADER';

INSERT  /*+ Append  */
INTO AIA_RPT_DISTRICT_LEADER NOLOGGING
(DIST_LDR_CODE ,
DIST_LDR_PARTICIPANTID,
DIST_LDR_NAME ,
DIST_LDR_TITLE,
DIST_LDR_POS_GA2 ,
DIST_LDR_POS_GA4,
DIST_LDR_POS_GA3,
DIST_LDR_BUNAME
)
   (SELECT SUBSTR(NAME, 4)DIST_LDR_CODE ,
           NAME DIST_LDR_PARTICIPANTID,
           PAD.firstname || ' ' || PAD.LASTNAME DIST_LDR_NAME ,
           DECODE(PAD.POSITIONTITLE,
    'FSC_NON_PROCESS', 'FSC', 'BR_FSC_NON_PROCESS',
    'FSC', 'AM_FSC_NON_PROCESS', 'FSC', 'AM_NON_PROCESS', 'AM' ,
    'FSD_NON_PROCESS', 'FSD',
    'FSAD_NON_PROCESS', 'FSAD',
    'BR_Staff' , 'Staff',
    'BR_DM', 'DM',
    'BR_DM_NON_PROCESS', 'DM',
    'BR_FSC' , 'FSC',
    'BR_Staff_NON_PROCESS', 'Staff',
    'BR_UM','UM',
    'BR_UM_NON_PROCESS', 'UM',
    'Staff_NON_PROCESS', 'Staff',
     PAD.POSITIONTITLE) DIST_LDR_TITLE,
     PAD.POS_GA2 DIST_LDR_POS_GA2,
     PAD.POS_GA4 DIST_LDR_POS_GA4 ,
     PAD.POS_GA3 DIST_LDR_POS_GA3,
     decode(PAD.businessunitmap,v_BUSINESSUNITMAP1,'SGPAFA',v_BUSINESSUNITMAP2,'BRUAGY') DIST_LDR_BUNAME
     FROM  AIA_RPT_PRD_TEMP_PADIM PAD
     WHERE PAD.POSITIONTITLE NOT LIKE '%FSC%'
);

COMMIT;





------=======ADD LOG
--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_COMM' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_COMM' , 'AIA_RPT_PRD_DIST_COMM'  , 'Processing', 'insert into AIA_RPT_PRD_DIST_COMM');


 DELETE FROM AIA_RPT_PRD_DIST_COMM WHERE PERIODNAME = V_PERIODNAME ;
  COMMIT;
  ------=======ADD LOG


   INSERT
    /*+ Append  */
  INTO AIA_RPT_PRD_DIST_COMM NOLOGGING
    (
      PROCESSINGUNITSEQ,
      PUNAME,
      BUNAME,
      calendarseq,
      calendarname,
      PERIODKEY,
      Periodname,
      --POSITIONSEQ,
      MANAGERSEQ,
      POSITIONSEQ,
      POSITIONNAME,
      DISTRICT_CODE,
      DM_NAme,
      dIST_LEADER_CODE,
      DIST_LEADER_NAME,
      DIST_LEAER_TITLE,
      DIST_LEADER_CLASS,
      UNIT_CODE,
      AGENCY,
      UNIT_LEADER_CODE,
      UNIT_LEADER_NAME,
      UNIT_LEAER_TITLE,
      UNIT_LEADER_CLASS,
      DISSOLVED_DATE,
      AGT_CODE,
      NAME,
      ROLE,
      CLASS,
      CONTRACT_DATE,
      APPOINTMENT_DATE,
      TERMINATION_DATE,
      AGENT_STATUS,
      LIFE_FYP,
      PA_FYP,
      LIFE_CASE,
      PA_CASE,
      LIFE_FYC_API_SSC,
      LIFE_FYC_RP,
 LIFE_FYC_NONILP,
 LIFE_FYC_ILP,
 LIFE_FYC_TOPUP,
      PA_FYC,
      CS_FYC,
       GI_SP_FYC,
GI_VL_FYC,
GI_RP_FYC,
      LIFE_2YEAR,
      LIFE_3YEAR,
      LIFE_4YEAR,
      Pa_Rc,
      CS_RC,
      CS_PL,
      UNASSIGNED_LIFE_2YEAR,
      UNASSIGNED_LIFE_3YEAR,
      UNASSIGNED_LIFE_4YEAR,
      UNASSIGNED_PA_RC,
      LIFE_FYP_YTD,
      PA_FYP_YTD,
      LIFE_CASE_YTD,
      PA_CASE_YTD,
      LIFE_FYC_API_SSC_YTD,
      LIFE_FYC_RP_YTD,
 LIFE_FYC_NONILP_YTD,
 LIFE_FYC_ILP_YTD,
 LIFE_FYC_TOPUP_YTD ,
      PA_FYC_YTD,
      CS_FYC_YTD,
             GI_SP_FYC_YTD,
 GI_VL_FYC_YTD,
 GI_RP_FYC_YTD,
      Life_2year_Ytd,
      LIFE_3YEAR_YTD,
      LIFE_4YEAR_YTD,
      PA_RC_YTD,
      CS_RC_YTD,
      CS_PL_YTD,
      UNASSIGNED_LIFE_2YEAR_YTD,
      UNASSIGNED_LIFE_3YEAR_YTD,
      Unassigned_Life_4year_Ytd,
      UNASSIGNED_PA_RC_YTD
    )
SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
  V_PROCESSINGUNITNAME PUNAME,
  AGT.FSC_BUNAME BUNAME,
  V_CALENDARSEQ calendarseq,
  V_CALENDARNAME calendarname,
  V_PERIODSEQ PERIODKEY,
  V_PERIODNAME Periodname,
  --NULL POSITIONSEQ,
  NULL MANAGERSEQ,
  NULL POSITIONSEQ,
  NULL POSITIONNAME,
  DISTRICT.DIST_CODE,
  DISTRICT.DIST_NAME DM_NAME,
  district_ldr.DIST_LDR_CODE DIST_LEADER_CODE,
  district_ldr.DIST_LDR_NAME DIST_LEADER_NAME,
  district_ldr.DIST_LDR_TITLE DIST_LEAER_TITLE,
  DISTRICT_LDR.DIST_LDR_POS_GA4 DIST_LEADER_CLASS,
  AGY.AGY_CODE UNIT_CODE,
  AGY_NAME AGENCY,
  AGY_LDR.AGY_LDR_CODE UNIT_LEADER_CODE,
  AGY_LDR.AGY_LDR_NAME  UNIT_LEADER_NAME,
  AGY_LDR.AGY_LDR_TITLE  UNIT_LEAER_TITLE,
  AGY_LDR.AGY_LDR_POS_GA4  UNIT_LEADER_CLASS,
  TO_CHAR(AGY.AGY_TERMINATIONDATE, 'MM/DD/YYYY') DISSOLVED_DATE,
  TMP.AGT_CODE,
  AGT.FSC_NAME NAME,
  AGT.FSC_TITLE ROLE,
  AGT.FSC_CLASS CLASS,
  AGT.FSC_HIREDATE CONTRACT_DATE,
  AGT.FSC_APPOINTMENT_DATE APPOINTMENT_DATE,
  AGT.FSC_TERMINATION_DATE TERMINATION_DATE,
  AGT.FSC_STATUS AGENT_STATUS,
  SUM(LIFE_FYP),
  SUM(PA_FYP),
  SUM( LIFE_CASE),
  SUM(PA_CASE),
  SUM(LIFE_FYC_API_SSC),
  SUM(LIFE_FYC_RP),
 SUM(LIFE_FYC_NONILP),
 SUM(LIFE_FYC_ILP),
 SUM(LIFE_FYC_TOPUP),
  SUM( PA_FYC),
  SUM( CS_FYC),
  SUM(GI_SP_FYC),
SUM(GI_VL_FYC),
SUM(GI_RP_FYC),
  SUM(LIFE_2YEAR),
  SUM( LIFE_3YEAR),
  SUM(LIFE_4YEAR),
  SUM( PA_RC),
  SUM( CS_RC),
  SUM( CS_PL),
  SUM(UNASSIGNED_LIFE_2YEAR),
  SUM( UNASSIGNED_LIFE_3YEAR),
  SUM(UNASSIGNED_LIFE_4YEAR),
  SUM(UNASSIGNED_PA_RC),
   SUM( LIFE_FYP_YTD),
  SUM( PA_FYP_YTD),
  SUM( LIFE_CASE_YTD),
  SUM( PA_CASE_YTD),
  SUM( LIFE_FYC_API_SSC_YTD),
  SUM(LIFE_FYC_RP_YTD),
 SUM(LIFE_FYC_NONILP_YTD),
 SUM(LIFE_FYC_ILP_YTD),
 SUM(LIFE_FYC_TOPUP_YTD),
 SUM( PA_FYC_YTD),
 SUM( CS_FYC_YTD),
 SUM( GI_SP_FYC_YTD),
SUM(GI_VL_FYC_YTD),
SUM(GI_RP_FYC_YTD),
SUM( LIFE_2YEAR_YTD),
SUM( LIFE_3YEAR_YTD),
SUM( LIFE_4YEAR_YTD),
SUM( PA_RC_YTD),
SUM( CS_RC_YTD),
SUM( CS_PL_YTD),
SUM( UNASSIGNED_LIFE_2YEAR_YTD),
SUM( UNASSIGNED_LIFE_3YEAR_YTD),
SUM( UNASSIGNED_LIFE_4YEAR_YTD),
SUM( UNASSIGNED_PA_RC_YTD)
--  SUM(LIFE_FYP)               + SUM( LIFE_FYP_YTD),
--  SUM(PA_FYP)                 + SUM( PA_FYP_YTD),
--  SUM( LIFE_CASE)             + SUM( LIFE_CASE_YTD),
--  SUM(PA_CASE)                + SUM( PA_CASE_YTD),
--  SUM(LIFE_FYC_API_SSC)       + SUM( LIFE_FYC_API_SSC_YTD),
--    SUM(LIFE_FYC_RP)  + SUM(LIFE_FYC_RP_YTD),
-- SUM(LIFE_FYC_NONILP) + SUM(LIFE_FYC_NONILP_YTD),
-- SUM(LIFE_FYC_ILP) + SUM(LIFE_FYC_ILP_YTD),
-- SUM(LIFE_FYC_TOPUP) + SUM(LIFE_FYC_TOPUP_YTD),
--  SUM( PA_FYC)                + SUM( PA_FYC_YTD),
--  SUM( CS_FYC)                + SUM( CS_FYC_YTD),
--  SUM( GI_SP_FYC) +SUM( GI_SP_FYC_YTD),
--SUM(GI_VL_FYC) + SUM(GI_VL_FYC_YTD),
--SUM(GI_RP_FYC) + SUM(GI_RP_FYC_YTD),
--  SUM(LIFE_2YEAR)             + SUM( LIFE_2YEAR_YTD),
--  SUM( LIFE_3YEAR)            + SUM( LIFE_3YEAR_YTD),
--  SUM(LIFE_4YEAR)             + SUM( LIFE_4YEAR_YTD),
--  SUM( PA_RC)                 + SUM( PA_RC_YTD),
--  SUM( CS_RC)                 + SUM( CS_RC_YTD),
--  SUM( CS_PL)                 + SUM( CS_PL_YTD),
--  SUM(UNASSIGNED_LIFE_2YEAR)  + SUM( UNASSIGNED_LIFE_2YEAR_YTD),
--  SUM( UNASSIGNED_LIFE_3YEAR) + SUM( UNASSIGNED_LIFE_3YEAR_YTD),
--  SUM(UNASSIGNED_LIFE_4YEAR)  + SUM( UNASSIGNED_LIFE_4YEAR_YTD),
--  SUM(UNASSIGNED_PA_RC)       + SUM( UNASSIGNED_PA_RC_YTD)
FROM AIA_RPT_PRD_DIST_COMM_TMP tmp,
  AIA_RPT_AGENT_LIST agt,
  AIA_RPT_AGENCY_LIST agy,
  AIA_RPT_AGENCY_LEADER agy_ldr,
  AIA_RPT_DISTRICT_LIST district,
  AIA_RPT_DISTRICT_LEADER DISTRICT_LDR
WHERE TMP.PERIODKEY          = V_PERIODSEQ
AND TMP.BUNAME = 'SGPAFA'
AND AGT.FSC_ID           = TMP.AGT_CODE
AND AGY.AGY_CODE        = TMP.UNIT_CODE
AND AGY.AGY_POSITIONTITLE IN  ('ORGANISATION','UNIT')
AND AGT.FSC_BUNAME = 'SGPAFA'--AND TMP.BUNAME = AGT.FSC_BUNAME
AND AGY.AGY_BUNAME = 'SGPAFA'--AND AGY.AGY_BUNAME = AGT.FSC_BUNAME
AND  DISTRICT_LDR.DIST_LDR_BUNAME = 'SGPAFA'--AND AGT.FSC_BUNAME = DISTRICT_LDR.DIST_LDR_BUNAME
AND (AGY_LDR.AGY_LDR_PARTICIPANTID = 'SGT' || AGY_LDR.AGY_LDR_CODE AND AGY.AGY_PARTICIPANTID LIKE 'SGY%')--AND (AGY_LDR.AGY_LDR_PARTICIPANTID = 'SGT' || AGY.AGY_POS_GA2 AND AGY.AGY_PARTICIPANTID LIKE 'SGY%')
AND AGY_LDR.AGY_LDR_CODE       = AGY.AGY_POS_GA2
AND DISTRICT.DIST_BUNAME ='SGPAFA'--AND DISTRICT.DIST_BUNAME = AGY.AGY_BUNAME
AND DISTRICT.DIST_CODE = agy.NEW_DISTRICT_CODE
AND DISTRICT_LDR.DIST_LDR_CODE  = DISTRICT.DIST_POS_GA2
and DISTRICT.DIST_TITLE IN ('ORGANISATION')
GROUP BY
  AGT.FSC_BUNAME,
  DISTRICT.DIST_CODE,
  DISTRICT.DIST_NAME ,
  district_ldr.DIST_LDR_CODE ,
  district_ldr.DIST_LDR_NAME ,
  district_ldr.DIST_LDR_TITLE ,
  DISTRICT_LDR.DIST_LDR_POS_GA4 ,
  AGY.AGY_CODE ,
  AGY.AGY_NAME ,
  AGY_LDR.AGY_LDR_CODE ,
  AGY_LDR.AGY_LDR_NAME  ,
  AGY_LDR.AGY_LDR_TITLE  ,
  AGY_LDR.AGY_LDR_POS_GA4  ,
  TO_CHAR(AGY.AGY_TERMINATIONDATE, 'MM/DD/YYYY') ,
  TMP.AGT_CODE,
  AGT.FSC_NAME ,
  AGT.FSC_TITLE ,
  AGT.FSC_CLASS ,
  agt.FSC_HIREDATE ,
  AGT.FSC_APPOINTMENT_DATE ,
  agt.FSC_TERMINATION_DATE ,
  agt.FSC_STATUS
  ;
COMMIT;


  ------=======ADD LOG




  INSERT
    /*+ Append  */
  INTO AIA_RPT_PRD_DIST_COMM NOLOGGING
    (
      PROCESSINGUNITSEQ,
      PUNAME,
      BUNAME,
      calendarseq,
      calendarname,
      PERIODKEY,
      Periodname,
      --POSITIONSEQ,
      MANAGERSEQ,
      POSITIONSEQ,
      POSITIONNAME,
      DISTRICT_CODE,
      DM_NAme,
      dIST_LEADER_CODE,
      DIST_LEADER_NAME,
      DIST_LEAER_TITLE,
      DIST_LEADER_CLASS,
      UNIT_CODE,
      AGENCY,
      UNIT_LEADER_CODE,
      UNIT_LEADER_NAME,
      UNIT_LEAER_TITLE,
      UNIT_LEADER_CLASS,
      DISSOLVED_DATE,
      AGT_CODE,
      NAME,
      ROLE,
      CLASS,
      CONTRACT_DATE,
      APPOINTMENT_DATE,
      TERMINATION_DATE,
      AGENT_STATUS,
      LIFE_FYP,
      PA_FYP,
      LIFE_CASE,
      PA_CASE,
      LIFE_FYC_API_SSC,
      LIFE_FYC_RP,
 LIFE_FYC_NONILP,
 LIFE_FYC_ILP,
 LIFE_FYC_TOPUP,
      PA_FYC,
      CS_FYC,
       GI_SP_FYC,
GI_VL_FYC,
GI_RP_FYC,
      LIFE_2YEAR,
      LIFE_3YEAR,
      LIFE_4YEAR,
      Pa_Rc,
      CS_RC,
      CS_PL,
      UNASSIGNED_LIFE_2YEAR,
      UNASSIGNED_LIFE_3YEAR,
      UNASSIGNED_LIFE_4YEAR,
      UNASSIGNED_PA_RC,
      LIFE_FYP_YTD,
      PA_FYP_YTD,
      LIFE_CASE_YTD,
      PA_CASE_YTD,
      LIFE_FYC_API_SSC_YTD,
      LIFE_FYC_RP_YTD,
 LIFE_FYC_NONILP_YTD,
 LIFE_FYC_ILP_YTD,
 LIFE_FYC_TOPUP_YTD ,
      PA_FYC_YTD,
      CS_FYC_YTD,
             GI_SP_FYC_YTD,
 GI_VL_FYC_YTD,
 GI_RP_FYC_YTD,
      Life_2year_Ytd,
      LIFE_3YEAR_YTD,
      LIFE_4YEAR_YTD,
      PA_RC_YTD,
      CS_RC_YTD,
      CS_PL_YTD,
      UNASSIGNED_LIFE_2YEAR_YTD,
      UNASSIGNED_LIFE_3YEAR_YTD,
      Unassigned_Life_4year_Ytd,
      UNASSIGNED_PA_RC_YTD
    )
SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
  V_PROCESSINGUNITNAME PUNAME,
  AGT.FSC_BUNAME BUNAME,
  V_CALENDARSEQ calendarseq,
  V_CALENDARNAME calendarname,
  V_PERIODSEQ PERIODKEY,
  V_PERIODNAME Periodname,
  --NULL POSITIONSEQ,
  NULL MANAGERSEQ,
  NULL POSITIONSEQ,
  NULL POSITIONNAME,
  DISTRICT.DIST_CODE,
  DISTRICT.DIST_NAME DM_NAME,
  district_ldr.DIST_LDR_CODE DIST_LEADER_CODE,
  district_ldr.DIST_LDR_NAME DIST_LEADER_NAME,
  district_ldr.DIST_LDR_TITLE DIST_LEAER_TITLE,
  DISTRICT_LDR.DIST_LDR_POS_GA4 DIST_LEADER_CLASS,
  AGY.AGY_CODE UNIT_CODE,
  AGY_NAME AGENCY,
  AGY_LDR.AGY_LDR_CODE UNIT_LEADER_CODE,
  AGY_LDR.AGY_LDR_NAME  UNIT_LEADER_NAME,
  AGY_LDR.AGY_LDR_TITLE  UNIT_LEAER_TITLE,
  AGY_LDR.AGY_LDR_POS_GA4  UNIT_LEADER_CLASS,
  TO_CHAR(AGY.AGY_TERMINATIONDATE, 'MM/DD/YYYY') DISSOLVED_DATE,
  TMP.AGT_CODE,
  AGT.FSC_NAME NAME,
  AGT.FSC_TITLE ROLE,
  AGT.FSC_CLASS CLASS,
  AGT.FSC_HIREDATE CONTRACT_DATE,
  AGT.FSC_APPOINTMENT_DATE APPOINTMENT_DATE,
  AGT.FSC_TERMINATION_DATE TERMINATION_DATE,
  AGT.FSC_STATUS AGENT_STATUS,
  SUM(LIFE_FYP),
  SUM(PA_FYP),
  SUM( LIFE_CASE),
  SUM(PA_CASE),
  SUM(LIFE_FYC_API_SSC),
  SUM(LIFE_FYC_RP),
 SUM(LIFE_FYC_NONILP),
 SUM(LIFE_FYC_ILP),
 SUM(LIFE_FYC_TOPUP),
  SUM( PA_FYC),
  SUM( CS_FYC),
  SUM(GI_SP_FYC),
SUM(GI_VL_FYC),
SUM(GI_RP_FYC),
  SUM(LIFE_2YEAR),
  SUM( LIFE_3YEAR),
  SUM(LIFE_4YEAR),
  SUM( PA_RC),
  SUM( CS_RC),
  SUM( CS_PL),
  SUM(UNASSIGNED_LIFE_2YEAR),
  SUM( UNASSIGNED_LIFE_3YEAR),
  SUM(UNASSIGNED_LIFE_4YEAR),
  SUM(UNASSIGNED_PA_RC),
  SUM( LIFE_FYP_YTD),
  SUM( PA_FYP_YTD),
  SUM( LIFE_CASE_YTD),
  SUM( PA_CASE_YTD),
  SUM( LIFE_FYC_API_SSC_YTD),
    SUM(LIFE_FYC_RP_YTD),
 SUM(LIFE_FYC_NONILP_YTD),
 SUM(LIFE_FYC_ILP_YTD),
 SUM(LIFE_FYC_TOPUP_YTD),
 SUM( PA_FYC_YTD),
 SUM( CS_FYC_YTD),
 SUM( GI_SP_FYC_YTD),
SUM(GI_VL_FYC_YTD),
SUM(GI_RP_FYC_YTD),
  SUM( LIFE_2YEAR_YTD),
  SUM( LIFE_3YEAR_YTD),
  SUM( LIFE_4YEAR_YTD),
  SUM( PA_RC_YTD),
  SUM( CS_RC_YTD),
  SUM( CS_PL_YTD),
  SUM( UNASSIGNED_LIFE_2YEAR_YTD),
  SUM( UNASSIGNED_LIFE_3YEAR_YTD),
  SUM( UNASSIGNED_LIFE_4YEAR_YTD),
  SUM( UNASSIGNED_PA_RC_YTD)
--  SUM(LIFE_FYP)               + SUM( LIFE_FYP_YTD),
--  SUM(PA_FYP)                 + SUM( PA_FYP_YTD),
--  SUM( LIFE_CASE)             + SUM( LIFE_CASE_YTD),
--  SUM(PA_CASE)                + SUM( PA_CASE_YTD),
--  SUM(LIFE_FYC_API_SSC)       + SUM( LIFE_FYC_API_SSC_YTD),
--    SUM(LIFE_FYC_RP)  + SUM(LIFE_FYC_RP_YTD),
-- SUM(LIFE_FYC_NONILP) + SUM(LIFE_FYC_NONILP_YTD),
-- SUM(LIFE_FYC_NONILP) + SUM(LIFE_FYC_ILP_YTD),
-- SUM(LIFE_FYC_TOPUP) + SUM(LIFE_FYC_TOPUP_YTD),
--  SUM( PA_FYC)                + SUM( PA_FYC_YTD),
--  SUM( CS_FYC)                + SUM( CS_FYC_YTD),
--    SUM( GI_SP_FYC) +SUM( GI_SP_FYC_YTD),
--SUM(GI_VL_FYC) + SUM(GI_VL_FYC_YTD),
--SUM(GI_RP_FYC) + SUM(GI_RP_FYC_YTD),
--  SUM(LIFE_2YEAR)             + SUM( LIFE_2YEAR_YTD),
--  SUM( LIFE_3YEAR)            + SUM( LIFE_3YEAR_YTD),
--  SUM(LIFE_4YEAR)             + SUM( LIFE_4YEAR_YTD),
--  SUM( PA_RC)                 + SUM( PA_RC_YTD),
--  SUM( CS_RC)                 + SUM( CS_RC_YTD),
--  SUM( CS_PL)                 + SUM( CS_PL_YTD),
--  SUM(UNASSIGNED_LIFE_2YEAR)  + SUM( UNASSIGNED_LIFE_2YEAR_YTD),
--  SUM( UNASSIGNED_LIFE_3YEAR) + SUM( UNASSIGNED_LIFE_3YEAR_YTD),
--  SUM(UNASSIGNED_LIFE_4YEAR)  + SUM( UNASSIGNED_LIFE_4YEAR_YTD),
--  SUM(UNASSIGNED_PA_RC)       + SUM( UNASSIGNED_PA_RC_YTD)
FROM AIA_RPT_PRD_DIST_COMM_TMP tmp,
  AIA_RPT_AGENT_LIST agt,
  AIA_RPT_AGENCY_LIST agy,
  AIA_RPT_AGENCY_LEADER agy_ldr,
  AIA_RPT_DISTRICT_LIST district,
  AIA_RPT_DISTRICT_LEADER DISTRICT_LDR
WHERE TMP.PERIODKEY          = V_PERIODSEQ
AND TMP.BUNAME = 'BRUAGY'
AND AGT.FSC_ID           = TMP.AGT_CODE
AND AGY.AGY_CODE        = TMP.UNIT_CODE
AND agy.NEW_DISTRICT_CODE = TMP.DISTRICT_CODE
AND AGT.FSC_BUNAME = 'BRUAGY'--AND TMP.BUNAME = AGT.FSC_BUNAME
AND AGY.AGY_BUNAME = 'BRUAGY'--AND AGY.AGY_BUNAME = AGT.FSC_BUNAME
AND DISTRICT_LDR.DIST_LDR_BUNAME = 'BRUAGY'--AND AGT.FSC_BUNAME = DISTRICT_LDR.DIST_LDR_BUNAME
AND (agy_ldr.AGY_LDR_PARTICIPANTID = 'BRT' || AGY_LDR.AGY_LDR_CODE AND agy.AGY_PARTICIPANTID like 'BRY%')--AND (agy_ldr.AGY_LDR_PARTICIPANTID = 'BRT' || agy.AGY_POS_GA2 AND agy.AGY_PARTICIPANTID like 'BRY%')
AND AGY_LDR.AGY_LDR_CODE       = AGY.AGY_POS_GA2
AND DISTRICT.DIST_BUNAME = 'BRUAGY'--AND DISTRICT.DIST_BUNAME = AGY.AGY_BUNAME
AND DISTRICT.DIST_CODE = agy.NEW_DISTRICT_CODE
AND DISTRICT_LDR.DIST_LDR_CODE  = DISTRICT.DIST_POS_GA2
GROUP BY
  AGT.FSC_BUNAME,
  DISTRICT.DIST_CODE,
  DISTRICT.DIST_NAME ,
  district_ldr.DIST_LDR_CODE ,
  district_ldr.DIST_LDR_NAME ,
  district_ldr.DIST_LDR_TITLE ,
  DISTRICT_LDR.DIST_LDR_POS_GA4 ,
  AGY.AGY_CODE ,
  AGY.AGY_NAME ,
  AGY_LDR.AGY_LDR_CODE ,
  AGY_LDR.AGY_LDR_NAME  ,
  AGY_LDR.AGY_LDR_TITLE  ,
  AGY_LDR.AGY_LDR_POS_GA4  ,
  TO_CHAR(AGY.AGY_TERMINATIONDATE, 'MM/DD/YYYY') ,
  TMP.AGT_CODE,
  AGT.FSC_NAME ,
  AGT.FSC_TITLE ,
  AGT.FSC_CLASS ,
  agt.FSC_HIREDATE ,
  AGT.FSC_APPOINTMENT_DATE ,
  agt.FSC_TERMINATION_DATE ,
  agt.FSC_STATUS
  ;
COMMIT;

  ------=======ADD LOG


-- DISTRICT TOTALS
UPDATE AIA_RPT_PRD_DIST_COMM P1
SET
  (
    Life_Fyp_Dt ,
    Pa_Fyp_DT ,
    Life_Case_Dt ,
    Pa_Case_Dt ,
    Life_Fyc_Api_Ssc_Dt ,
    LIFE_FYC_RP_DT,
 LIFE_FYC_NONILP_DT,
 LIFE_FYC_ILP_DT,
 LIFE_FYC_TOPUP_DT,
    Pa_Fyc_Dt ,
    Cs_Fyc_Dt ,
     GI_SP_FYC_DT,
GI_VL_FYC_DT,
GI_RP_FYC_DT,
    Life_2year_Dt ,
    Life_3year_Dt ,
    Life_4year_Dt ,
    Pa_Rc_Dt ,
    Cs_Rc_Dt ,
    Cs_Pl_Dt ,
    Unassigned_Life_2year_Dt ,
    Unassigned_Life_3year_Dt ,
    Unassigned_Life_4year_Dt ,
    Unassigned_Pa_Rc_Dt ,
    Life_Fyp_Ytd_Dt ,
    Pa_Fyp_Ytd_Dt ,
    Life_Case_Ytd_Dt ,
    Pa_Case_Ytd_Dt ,
    Life_Fyc_Api_Ssc_Ytd_Dt ,
    LIFE_FYC_RP_YTD_Dt,
 LIFE_FYC_NONILP_YTD_DT,
 LIFE_FYC_ILP_YTD_DT,
 LIFE_FYC_TOPUP_YTD_DT,
    Pa_Fyc_Ytd_Dt ,
    Cs_Fyc_Ytd_Dt ,
           GI_SP_FYC_YTD_DT,
 GI_VL_FYC_YTD_DT,
 GI_RP_FYC_YTD_DT,
    Life_2year_Ytd_Dt ,
    Life_3year_Ytd_Dt ,
    Life_4year_Ytd_Dt ,
    Pa_Rc_Ytd_Dt ,
    Cs_Rc_Ytd_Dt ,
    Cs_Pl_Ytd_Dt ,
    Unassigned_Life_2year_Ytd_Dt ,
    Unassigned_Life_3year_Ytd_Dt ,
    Unassigned_Life_4year_Ytd_Dt ,
    Unassigned_Pa_Rc_Ytd_Dt
  )
  =
  (SELECT SUM(Life_Fyp) ,
    SUM(Pa_Fyp) ,
    SUM(Life_Case) ,
    SUM(Pa_Case) ,
    SUM(Life_Fyc_Api_Ssc) ,
    SUM(LIFE_FYC_RP),
 SUM(LIFE_FYC_NONILP),
 SUM(LIFE_FYC_ILP),
 SUM(LIFE_FYC_TOPUP),
    SUM(Pa_Fyc) ,
    SUM(Cs_Fyc) ,
    SUM(GI_SP_FYC),
SUM(GI_VL_FYC),
SUM(GI_RP_FYC),
    SUM(Life_2year) ,
    SUM(Life_3year) ,
    SUM(Life_4year) ,
    SUM(Pa_Rc) ,
    SUM(Cs_Rc) ,
    SUM(Cs_Pl) ,
    SUM(Unassigned_Life_2year) ,
    SUM(Unassigned_Life_3year) ,
    SUM(Unassigned_Life_4year) ,
    SUM(Unassigned_Pa_Rc) ,
    SUM(Life_Fyp_Ytd) ,
    SUM(Pa_Fyp_Ytd) ,
    SUM(Life_Case_Ytd) ,
    SUM(Pa_Case_Ytd) ,
    SUM(Life_Fyc_Api_Ssc_Ytd) ,
    SUM(LIFE_FYC_RP_YTD),
 SUM(LIFE_FYC_NONILP_YTD),
 SUM(LIFE_FYC_ILP_YTD),
 SUM(LIFE_FYC_TOPUP_YTD),
    SUM(Pa_Fyc_Ytd) ,
    SUM(Cs_Fyc_Ytd) ,
    SUM(GI_SP_FYC_YTD),
 SUM(GI_VL_FYC_YTD),
 SUM(GI_RP_FYC_YTD),
    SUM(Life_2year_Ytd) ,
    SUM(Life_3year_Ytd) ,
    SUM(Life_4year_Ytd) ,
    SUM(Pa_Rc_Ytd) ,
    SUM(Cs_Rc_Ytd) ,
    SUM(Cs_Pl_Ytd) ,
    SUM(Unassigned_Life_2year_Ytd) ,
    SUM(Unassigned_Life_3year_Ytd) ,
    SUM(Unassigned_Life_4year_Ytd) ,
    SUM(Unassigned_Pa_Rc_Ytd)
  FROM AIA_RPT_PRD_DIST_COMM P2
  WHERE P1.DISTRICT_CODE = P2.DISTRICT_CODE
  AND P1.BUNAME          = P2.BUNAME
    --and p2.periodkey = p1.periodkey
  AND p2.periodkey = V_PERIODSEQ
  )
WHERE p1.periodkey = V_PERIODSEQ;
COMMIT;
    ------=======ADD LOG

    UPDATE AIA_RPT_PRD_DIST_COMM P1
SET
  (
    Life_Fyp_Ct ,
    Pa_Fyp_Ct ,
    Life_Case_ct ,
    Pa_Case_Ct ,
    Life_Fyc_Api_Ssc_ct ,
    LIFE_FYC_RP_CT,
 LIFE_FYC_NONILP_CT,
 LIFE_FYC_ILP_CT,
 LIFE_FYC_TOPUP_CT,
    Pa_Fyc_ct ,
    Cs_Fyc_Ct ,
     GI_SP_FYC_CT,
GI_VL_FYC_CT,
GI_RP_FYC_CT,
    Life_2year_ct ,
    Life_3year_ct ,
    Life_4year_ct ,
    Pa_Rc_ct ,
    Cs_Rc_ct ,
    Cs_Pl_Ct ,
    Unassigned_Life_2year_ct ,
    Unassigned_Life_3year_ct ,
    Unassigned_Life_4year_ct ,
    Unassigned_Pa_Rc_ct ,
    Life_Fyp_Ytd_ct ,
    Pa_Fyp_Ytd_ct ,
    Life_Case_Ytd_ct ,
    Pa_Case_Ytd_Ct ,
    Life_Fyc_Api_Ssc_Ytd_ct ,
    LIFE_FYC_RP_YTD_CT,
 LIFE_FYC_NONILP_YTD_CT,
 LIFE_FYC_ILP_YTD_CT,
 LIFE_FYC_TOPUP_YTD_CT,
    Pa_Fyc_Ytd_ct ,
    Cs_Fyc_Ytd_ct ,
           GI_SP_FYC_YTD_CT,
 GI_VL_FYC_YTD_CT,
 GI_RP_FYC_YTD_CT,
    Life_2year_Ytd_Ct ,
    Life_3year_Ytd_Ct ,
    Life_4year_Ytd_Ct ,
    Pa_Rc_Ytd_Ct ,
    Cs_Rc_Ytd_ct ,
    Cs_Pl_Ytd_ct ,
    Unassigned_Life_2year_Ytd_ct ,
    Unassigned_Life_3year_Ytd_ct ,
    Unassigned_Life_4year_Ytd_ct ,
    Unassigned_Pa_Rc_Ytd_ct
  )
  =
  (SELECT SUM(Life_Fyp) ,
    SUM(Pa_Fyp) ,
    SUM(Life_Case) ,
    SUM(Pa_Case) ,
    SUM(Life_Fyc_Api_Ssc) ,
    SUM(LIFE_FYC_RP),
 SUM(LIFE_FYC_NONILP),
 SUM(LIFE_FYC_ILP),
 SUM(LIFE_FYC_TOPUP),
    SUM(Pa_Fyc) ,
    SUM(Cs_Fyc) ,
    SUM( GI_SP_FYC),
SUM(GI_VL_FYC),
SUM(GI_RP_FYC),
    SUM(Life_2year) ,
    SUM(Life_3year) ,
    SUM(Life_4year) ,
    SUM(Pa_Rc) ,
    SUM(Cs_Rc) ,
    SUM(Cs_Pl) ,
    SUM(Unassigned_Life_2year) ,
    SUM(Unassigned_Life_3year) ,
    SUM(Unassigned_Life_4year) ,
    SUM(Unassigned_Pa_Rc) ,
    SUM(Life_Fyp_Ytd) ,
    SUM(Pa_Fyp_Ytd) ,
    SUM(Life_Case_Ytd) ,
    SUM(Pa_Case_Ytd) ,
    SUM(Life_Fyc_Api_Ssc_Ytd) ,
    SUM(LIFE_FYC_RP_YTD),
 SUM(LIFE_FYC_NONILP_YTD),
 SUM(LIFE_FYC_ILP_YTD),
 SUM(LIFE_FYC_TOPUP_YTD),
    SUM(Pa_Fyc_Ytd) ,
    SUM(Cs_Fyc_Ytd) ,
    SUM(GI_SP_FYC_YTD),
 SUM(GI_VL_FYC_YTD),
 SUM(GI_RP_FYC_YTD),
    SUM(Life_2year_Ytd) ,
    SUM(Life_3year_Ytd) ,
    SUM(Life_4year_Ytd) ,
    SUM(Pa_Rc_Ytd) ,
    SUM(Cs_Rc_Ytd) ,
    SUM(Cs_Pl_Ytd) ,
    SUM(Unassigned_Life_2year_Ytd) ,
    SUM(Unassigned_Life_3year_Ytd) ,
    SUM(Unassigned_Life_4year_Ytd) ,
    SUM(Unassigned_Pa_Rc_Ytd)
  FROM AIA_RPT_PRD_DIST_COMM P2
  WHERE P1.BUNAME = P2.BUNAME
    --and p2.periodkey = p1.periodkey
  AND p2.periodkey = V_PERIODSEQ
  )
WHERE p1.periodkey = V_PERIODSEQ;
COMMIT;

      ------=======ADD LOG

/* Start of deleting the Terminated Agents with 0 Value. Added by Saurabh Trehan*/
delete from AIA_RPT_PRD_DIST_COMM
where upper(Agent_Status) != 'INFORCE'
and   termination_date is not null
and
periodkey = V_PERIODSEQ
and  nvl(LIFE_FYP,0) = 0 and
nvl(PA_FYP,0) = 0 and
nvl(LIFE_CASE,0) = 0 and
nvl(PA_CASE,0) = 0 and
nvl(LIFE_FYC_API_SSC,0) = 0 and
nvl(LIFE_FYC_RP,0)=0 and
 nvl(LIFE_FYC_NONILP,0)=0 and
 nvl(LIFE_FYC_ILP,0)=0 and
 nvl(LIFE_FYC_TOPUP,0)=0 and
nvl(PA_FYC,0) = 0 and
nvl(CS_FYC,0) = 0 and
nvl( GI_SP_FYC,0)=0 and
nvl(GI_VL_FYC,0)=0 and
nvl(GI_RP_FYC,0)=0 and
nvl(LIFE_2YEAR,0) = 0 and
nvl(LIFE_3YEAR,0) = 0 and
NVL(LIFE_4YEAR,0) = 0 AND
nvl(PA_RC,0) = 0 and
nvl(CS_RC,0) = 0 and
NVL(CS_PL,0) = 0 AND
--------------------------------------
NVL(UNASSIGNED_LIFE_2YEAR,0) = 0 AND
NVL(UNASSIGNED_LIFE_3YEAR,0) = 0 AND
NVL(UNASSIGNED_LIFE_4YEAR,0) = 0 AND
NVL(UNASSIGNED_PA_RC,0) = 0 AND
---------------------------------------
nvl(LIFE_FYP_YTD,0)  = 0 and
nvl(PA_FYP_YTD,0)  = 0 and
nvl(LIFE_CASE_YTD,0)  = 0 and
nvl(PA_CASE_YTD,0)  = 0 and
nvl(LIFE_FYC_API_SSC_YTD,0)  = 0 and
nvl(LIFE_FYC_RP_YTD,0)=0 and
 nvl(LIFE_FYC_NONILP_YTD,0)=0 and
 nvl(LIFE_FYC_ILP_YTD,0)=0 and
 nvl(LIFE_FYC_TOPUP_YTD ,0)=0 and
nvl(PA_FYC_YTD,0)  = 0 and
nvl(CS_FYC_YTD,0)  = 0 and
nvl(GI_SP_FYC_YTD,0)=0 and
 nvl(GI_VL_FYC_YTD,0)=0 and
 nvl(GI_RP_FYC_YTD,0)=0 and
nvl(LIFE_2YEAR_YTD,0)  = 0 and
nvl(LIFE_3YEAR_YTD,0)  = 0 and
nvl(LIFE_4YEAR_YTD,0)  = 0 and
NVL(PA_RC_YTD,0)  = 0 AND
NVL(CS_RC_YTD,0)  = 0 AND
NVL(CS_PL_YTD,0)  = 0 AND
NVL(UNASSIGNED_LIFE_2YEAR_YTD,0)  = 0 AND
NVL(UNASSIGNED_LIFE_3YEAR_YTD,0)  = 0 AND
NVL(UNASSIGNED_LIFE_4YEAR_YTD,0)  = 0 AND
NVL(Unassigned_Pa_Rc_Ytd,0)  = 0
--AND nvl(PA_FYC_DT,0) = 0 and
--nvl(CS_FYC_DT,0) = 0 and
--nvl(PA_RC_DT,0) = 0 and
--nvl(CS_RC_DT,0) = 0 and
--NVL(CS_PL_DT,0) = 0
;
/* End of deleting the Terminated Agents with 0 Value. Added by Saurabh Trehan*/

commit;

--version 8 add on bridging flag

 execute immediate 'truncate table AIA_RPT_PRD_ONBRIDGING';

 insert into AIA_RPT_PRD_ONBRIDGING
   SELECT PAR.USERID              AS AGENT_CODE, --contains SGT as prefix
          GAPAR.GENERICATTRIBUTE4 AS AGY_AGENT,
          GAPAR.GENERICATTRIBUTE5 AS FA_AGENT,
          GAPAR.GENERICATTRIBUTE6 AS FA_AGENCY,
          GAPAR.GENERICBOOLEAN2   AS ON_BRIDGING_FLAG
     FROM CS_PARTICIPANT PAR, CS_GAPARTICIPANT GAPAR
    WHERE PAR.TENANTID = 'AIAS'
      AND PAR.REMOVEDATE = DT_REMOVEDATE
      AND PAR.EFFECTIVESTARTDATE <= V_PERIODENDDATE-1
      AND PAR.EFFECTIVEENDDATE > V_PERIODENDDATE-1
      AND GAPAR.TENANTID = 'AIAS'
      AND GAPAR.REMOVEDATE = DT_REMOVEDATE
      AND GAPAR.EFFECTIVESTARTDATE <= V_PERIODENDDATE-1
      AND GAPAR.EFFECTIVEENDDATE > V_PERIODENDDATE-1
      AND GAPAR.PAGENUMBER = 0
      AND PAR.PAYEESEQ = GAPAR.PAYEESEQ
      AND PAR.USERID LIKE '%T%';

      COMMIT;


 merge into AIA_RPT_PRD_DIST_COMM s
 using (SELECT AGENT_CODE,
               ON_BRIDGING_FLAG
          FROM AIA_RPT_PRD_ONBRIDGING) t
 on ('SGT' || s.agt_code = t.AGENT_CODE and s.PERIODNAME = V_PERIODNAME)
 when matched then
   update set s.on_bridging_flag = case
 when t.ON_BRIDGING_FLAG = 1 then 1 else 0 end;

  commit;

--version 8 end

      ------=======ADD LOG JEFF1

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_COMM' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_COMM' , 'AIA_RPT_PRD_DIST_COMM_PARAM'  , 'Processing', 'insert into AIA_RPT_PRD_DIST_COMM_PARAM');


-- Parameter table updation
EXECUTE IMMEDIATE 'Truncate table AIA_RPT_PRD_DIST_COMM_PARAM drop storage' ;

      ------=======ADD LOG

--1
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  ) ;
COMMIT;

--ADDED 2
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  ) ;
COMMIT;

--ADDED 3
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  ) ;
COMMIT;

--ADDED 4
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  ) ;
COMMIT;
--ADDED 5
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  ) ;
COMMIT;
--ADDED 6
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 7
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--added new
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--added new
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 8
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 9
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 10
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      RT.AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 11
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      RT.UNIT_CODE
      || ' - '
      || RT.AGENCY UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 12
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      RT.DISTRICT_CODE
      || ' - '
      || RT.DM_NAME DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 13
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      RT.AGT_CODE
      || ' - '
      || RT.NAME AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;
--ADDED 14
INSERT
INTO AIA_RPT_PRD_DIST_COMM_PARAM
  (SELECT DISTINCT rt.periodkey,
      TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
      SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
      SUBSTR('0'
      || MOD(EXTRACT(MONTH FROM PD.STARTDATE), 13), -2) MONTHNO,
      ' ALL' DISTRICT_CODE_DESC,
      ' ALL' UNIT_CODE_DESC,
      ' ALL' AGENTCODE,
      ' ALL' AGENT_STATUS,
      RT.BUNAME BUNAME
    FROM AIA_RPT_PRD_DIST_COMM rt,
      cs_period pd
    WHERE rt.periodkey  = pd.PERIODSEQ
    AND pd.removedate   = V_EOT
    AND pd.calendarseq = V_CALENDARSEQ
      -- COndition to ignore history YTD information
    AND rt.PROCESSINGUNITSEQ       IS NOT NULL
    AND rt.calendarseq IS NOT NULL
  );
COMMIT;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_COMM' , 'Finish', '');

END ;

PROCEDURE REP_RUN_PRD_POLICY_WRI as

  V_PROCNAME           VARCHAR2(256);
  v_eot                date := to_date('01/01/2200', 'DD/MM/YYYY');
  V_CALENDARNAME       VARCHAR2(256);
  V_CALENDARSEQ        INTEGER;
  V_PROCESSINGUNITSEQ  INTEGER;
  V_PROCESSINGUNITNAME VARCHAR2(256);
  V_PERIODSTARTDATE    DATE;
  V_PERIODENDDATE      DATE;
  V_PERIODNAME         VARCHAR2(256);
  v_BUSINESSUNITMAP1   INTEGER;
  V_BUSINESSUNITMAP2   INTEGER;
  V_SYSDATE        DATE;
  V_Partition_Name varchar2(250);
  V_PERIODTYPESEQ  NUMBER;

BEGIN

  V_PROCNAME := 'PROC_RPT_PRD_POLICY_WRI';
  V_SYSDATE  := SYSDATE;


  SELECT P.STARTDATE, P.ENDDATE, C.DESCRIPTION, P.NAME, P.CALENDARSEQ
    INTO V_PERIODSTARTDATE,
         V_PERIODENDDATE,
         V_CALENDARNAME,
         V_PERIODNAME,
         V_CALENDARSEQ
    FROM CS_PERIOD P
   INNER JOIN CS_CALENDAR C
      ON P.CALENDARSEQ = C.CALENDARSEQ
   WHERE PERIODSEQ = V_PERIODSEQ;
  SELECT PERIODTYPESEQ
    INTO V_PERIODTYPESEQ
    FROM CS_PERIODTYPE
   WHERE NAME = 'month';

---------log


  SELECT PROCESSINGUNITSEQ, NAME
    INTO V_PROCESSINGUNITSEQ, V_PROCESSINGUNITNAME
    FROM CS_PROCESSINGUNIT
   WHERE NAME = 'AGY_PU';

  SELECT MASK
    INTO v_BUSINESSUNITMAP1
    FROM CS_BUSINESSUNIT
   WHERE NAME IN ('SGPAFA');
  SELECT MASK
    INTO v_BUSINESSUNITMAP2
    FROM CS_BUSINESSUNIT
   WHERE NAME IN ('BRUAGY');

  V_Partition_Name := 'P_' || replace(V_PeriodName, ' ', '_');
  Begin
        Execute Immediate 'Alter Table AIA_RPT_PRD_POLICY_WRI Truncate Partition '  ||
                      V_Partition_Name;
  Exception
    when others then
      null;
  End;
  Begin
    Execute Immediate 'Alter Table AIA_RPT_PRD_POLICY_WRI Add Partition '  ||
                      V_Partition_Name || ' Values('''  || V_PeriodName ||
                      ''') ' ;
  Exception
    when others then
      NULL;
  End;

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;

--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_WRI' , 'AIA_RPT_PRD_POLICY_WRI 1'  , 'Processing', 'insert into AIA_RPT_PRD_POLICY_WRI');


 /*commented by suresh
INSERT
 /*+ Append */
/*  INTO AIA_RPT_PRD_POLICY_WRI
    (PROCESSINGUNITSEQ, --PUKEY,
     PUNAME,
     BUNAME,
     BUMAP,
     CALENDARSEQ,
     CALNAME,
     PERIODSEQ,--PERIODKEY,
     PERIODNAME,
     POSITIONSEQ, --Pakey,
     MANAGERSEQ,
     POSITIONNAME,
     DISTRICT_CODE,
     DM_NAME,
     DIST_LEADER_CODE,
     DIST_LEADER_NAME,
     DIST_LEAER_TITLE,
     DIST_LEADER_CLASS,
     UNIT_CODE,
     AGENCY,
     UNIT_LEADER_CODE,
     UNIT_LEADER_NAME,
     UNIT_LEAER_TITLE,
     UNIT_LEADER_CLASS,
     DISOLVED_DATE,
     Agent_code,
     NAME,
     POLICYNO,
     ---add by jeff for 2 columns
     PLAN_DESCRIPTION,
     RISK_COMMENCEMENT_DATE,
     INCEPTION_DATE,
     LOB,
     POLICYYEAR,
     TYPE_COMMISSION,
     ---add by jeff for 2 columns
     API_AMT,
     SSCP_AMT,
     COMMISSION_AMT,
     FYP,
     CASECOUNT
     --ADD In version 5
     ,SUBMIT_DATE
     --end to add
     )
    (SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
            V_PROCESSINGUNITNAME PUNAME,
            decode(pad.BUSINESSUNITMAP,
                   V_BUSINESSUNITMAP1,
                   'SGPAFA',
                   V_BUSINESSUNITMAP2,
                   'BRUAGY') BUNAME,
            pad.BUSINESSUNITMAP BUMAP,
            V_CALENDARSEQ CALENDARSEQ,
            V_CALENDARNAME CALENDARNAME,
            V_PERIODSEQ PERIODSEQ,
            V_PERIODNAME periodname,
            PAD.POSITIONSEQ POSITIONSEQ,
            PAD.MANAGERSEQ,
            PAD.NAME,
/*            SUBSTR(DISTRICT.NAME, 4) DISTRICT_CODE,
            district.firstname || ' ' || district.lastname DM_NAme,
            district.POS_GA2 DIST_LEADER_CODE,
            district.POS_GA7 DIST_LEADER_NAME,
            district.POS_GA11 DIST_LEAER_TITLE,
            DISTRICT.POS_GA4 DIST_LEADER_CLASS,*/
      /*      SUBSTR(pos_dis.NAME,4) DISTRICT_CODE,
      par_dis.firstname || ' ' || par_dis.lastname DM_NAME,
      pos_dis.genericattribute2 DIST_LEADER_CODE,
      pos_dis.genericattribute7 DIST_LEADER_NAME,
      pos_dis.genericattribute11 DIST_LEAER_TITLE,
      pos_dis.genericattribute4 DIST_LEADER_CLASS,
      TMP.CD_GA13 UNIT_CODE,
      agy.firstname || ' ' || agy.lastname AGENCY,
      agy.POS_GA2 UNIT_LEADER_CODE,
      agy.POS_GA7 UNIT_LEADER_NAME,
      DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
      agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
      (CASE
        WHEN AGY.POSITIONTITLE IN
             ('AGENCY', 'DISTRICT', 'BR_AGENCY', 'BR_DISTRICT') THEN
         AGY.AGY_TERMINATIONDATE
      END) DISSOLVED_DATE,
      SUBSTR(PAD.NAME, 4) AGT_CODE,
      (PAD.firstname || PAD.LASTNAME) NAME,
      TMP.CD_GA6 POLICYNO,
      ---add by jeff for 2 columns
      TMP.TRANS_GA3,
      TMP.CD_GD2,
      CASE
        WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
             --TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
             TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB',
                       'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                       'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                       'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                       'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
         TO_CHAR(TMP.TRANS_GD6, 'MM/DD/YYYY')
      END INCEPTION_DATE,
      TMP.CD_GA2 LOB,
      TMP.CD_GN1 POLICYYEAR,
      CASE
        WHEN TMP.TRANS_GA17 = 'O' AND
             TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
             --TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
             TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB',
                       'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                       'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                       'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                       'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
         'OWN'
        WHEN TMP.TRANS_GA17 = 'RNO' OR
             TMP.TRANS_GA17 = 'RO' AND
             --TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
             TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB',
                       'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                       'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                       'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                       'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
         'ASSIGN'
      END TYPE_COMMISSION,
      -----add by jeff for 2 columns
      CASE
        WHEN
             --TMP.CD_CREDITTYPE = 'API' and TMP.CD_GN1 = 1
             TMP.CD_CREDITTYPE in ('API','API_W','API_W_DUPLICATE', 'API_WC_DUPLICATE') and TMP.CD_GN1 = 1
              THEN TMP.CD_VALUE
        ELSE
         0
      END API_AMT,
      CASE
        WHEN
             --TMP.CD_CREDITTYPE = 'SSCP' and TMP.CD_GN1 = 2
             TMP.CD_CREDITTYPE in ('SSCP','SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE') and TMP.CD_GN1 = 2
              THEN TMP.CD_VALUE
        ELSE
         0
      END SSCP_AMT,
      CASE
        WHEN (TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
             TMP.CD_CREDITTYPE IN
             --('FYC', 'API', 'SSCP', 'RYC', 'APB')
             ('FYC', 'RYC', 'API', 'SSCP', 'APB',
              'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
              'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
              'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
              'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
             ) THEN
         TMP.CD_VALUE
        ELSE
         0
      END COMMISSION_AMT,
      CASE
        WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
             --TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') AND
              TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB',
              'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
              'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
              'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
              'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') AND
             TMP.TRANS_GN3 <> 0 THEN
         TMP.TRANS_GN6
        ELSE
         0
      END FYP,
      CASE
        WHEN TMP.TRANS_DIM_PRODUCTNAME IN
             ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
             TMP.TRANS_DIM_EVENTTYPE = 'FYP' THEN
         TMP.TRANS_GN5
        ELSE
         0
      END CASECOUNT
     --ADD In version 5
      ,TMP.submitdate
     --end to add
 FROM AIA_RPT_PRD_TEMP_PADIM  pad,
      AIA_RPT_PRD_TEMP_PADIM  agy,
      AIA_RPT_PRD_TEMP_PADIM  agy_ldr,
      --AIA_RPT_PRD_TEMP_PADIM  DISTRICT,
      AIA_RPT_PRD_TEMP_VALUES TMP
     inner join cs_position pos_agy
     on pos_agy.name = trim('SGY'||TMP.CD_GA13)
     AND pos_agy.effectivestartdate <= TMP.CD_GD2 --policy issue date
     AND pos_agy.effectiveenddate   > TMP.CD_GD2  --policy issue date
     AND pos_agy.removedate =DT_REMOVEDATE
     --for writing district postion info
     inner join cs_position pos_dis
     on pos_dis.name=trim('SGY'||pos_agy.genericattribute3)
     AND pos_dis.effectivestartdate <= TMP.CD_GD2 --policy issue date
     AND pos_dis.effectiveenddate   > TMP.CD_GD2  --policy issue date
     AND pos_dis.removedate =DT_REMOVEDATE
     --for writing district participant info
     inner join cs_participant par_dis
     on par_dis.PAYEESEQ = pos_dis.PAYEESEQ
     AND par_dis.effectivestartdate < V_ENDDATE
     AND par_dis.effectiveenddate   >  V_ENDDATE-1
     AND par_dis.removedate = DT_REMOVEDATE
WHERE TMP.NEW_PRD_IND=1 AND TMP.PRD_TYPE = 'DIRECT'
  AND pad.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
  and agy.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
  and agy_ldr.BUSINESSUNITMAP in
      (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
/*        AND DISTRICT.BUSINESSUNITMAP IN
      (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)*/
  /*      AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD','AM','FSD','FSM','BR_DM','BR_FSC','BR_UM')
      AND PAD.POS_GA4 NOT IN ('45', '48')
      --AND TMP.CD_CREDITTYPE IN ('FYP', 'RYC', 'FYC', 'API', 'SSCP', 'APB', 'Case_Count')
      AND TMP.CD_CREDITTYPE IN ('FYP', 'FYC', 'RYC', 'API', 'SSCP', 'APB', 'Case_Count',
                           'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                           'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                           'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                           'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
      --AND TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ
      AND (
      (TMP.businessunitmap = 1 and TMP.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (TMP.businessunitmap <> 1 and TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ)
      )
      AND agy.POS_GA1 = TMP.CD_GA13
      AND AGY.POSITIONTITLE IN
          ('AGENCY', 'DISTRICT', 'BR_AGENCY', 'BR_DISTRICT')
      AND ((agy_ldr.NAME = 'SGT' || agy.POS_GA2 AND agy.NAME LIKE 'SGY%') OR
          (agy_ldr.NAME = 'BRT' || agy.POS_GA2 AND agy.NAME LIKE 'BRY%'))
      --AND DISTRICT.POS_GA3 = AGY.POS_GA3
      --AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
      --AND district.positiontitle IN ('DISTRICT', 'BR_DISTRICT')
      AND TMP.CD_GA2 IN ('LF', 'PA', 'HS', 'CS', 'PL', 'CL', 'VL'));
      */
 --Added by Suresh

    EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_POLICY_WRI_HELP';

insert /*+APPEND*/ into AIA_RPT_PRD_POLICY_WRI_HELP NOLOGGING
(select /*+   */
       ' ' BUNAME,
      --decode(PD.businessunitmap,1,'SGPAFA',2,'BRUAGY') BUNAME,
        TMP.businessunitmap BUMAP,
        TMP.CD_GA13 UNIT_CODE,
            TMP.CD_GA6 POLICYNO,
            ---add by jeff for 2 columns
            TMP.TRANS_GA3,
            TMP.CD_GD2,
            CASE
              --WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
              WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL','GI') AND    --version 9 add GI product
                   --TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
                   TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB',
                             'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                             'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                             'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                             'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
               TO_CHAR(TMP.TRANS_GD6, 'MM/DD/YYYY')
            END INCEPTION_DATE,
            TMP.CD_GA2 LOB,
            TMP.CD_GN1 POLICYYEAR,
            CASE
              WHEN TMP.TRANS_GA17 = 'O' AND
                   --TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
                   TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL','GI') AND  --version 9 add GI product
                   --TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
                   TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB',
                             'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                             'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                             'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                             'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
               'OWN'
              WHEN TMP.TRANS_GA17 = 'RNO' OR
                   TMP.TRANS_GA17 = 'RO' AND
                   --TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
                   TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB',
                             'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                             'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                             'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                             'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
               'ASSIGN'
            END TYPE_COMMISSION,
            -----add by jeff for 2 columns
            CASE
              WHEN
                   --TMP.CD_CREDITTYPE = 'API' and TMP.CD_GN1 = 1
                   TMP.CD_CREDITTYPE in ('API','API_W','API_W_DUPLICATE', 'API_WC_DUPLICATE') and TMP.CD_GN1 = 1
                    THEN TMP.CD_VALUE
              ELSE
               0
            END API_AMT,
            CASE
              WHEN
                   --TMP.CD_CREDITTYPE = 'SSCP' and TMP.CD_GN1 = 2
                   TMP.CD_CREDITTYPE in ('SSCP','SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE') and TMP.CD_GN1 = 2
                    THEN TMP.CD_VALUE
              ELSE
               0
            END SSCP_AMT,
            CASE
              --WHEN (TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
              WHEN (TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL','GI') AND   --version 9 add GI product
                   TMP.CD_CREDITTYPE IN
                   --('FYC', 'API', 'SSCP', 'RYC', 'APB')
                   ('FYC', 'RYC', 'API', 'SSCP', 'APB',
                    'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                    'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                    'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                    'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
                   ) THEN
               TMP.CD_VALUE
              ELSE
               0
            END COMMISSION_AMT,
            CASE
              --WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
              WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL','GI') AND   --version 9 add GI product
                   --TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') AND
                    TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB',
                    'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                    'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                    'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                    'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') AND
                   TMP.TRANS_GN3 <> 0 THEN
               TMP.TRANS_GN6
              ELSE
               0
            END FYP,
            CASE
              WHEN TMP.TRANS_DIM_PRODUCTNAME IN
                   --('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
                   ('LF', 'HS', 'PA', 'PL', 'CS', 'VL','GI') AND    --version 9 add GI product
                   TMP.TRANS_DIM_EVENTTYPE = 'FYP' THEN
               TMP.TRANS_GN5
              ELSE
               0
            END CASECOUNT
           --ADD In version 5
            ,TMP.submitdate,
     --end to add
     SUBSTR(help1.NAME,4) DISTRICT_CODE,
            help1.firstname || ' ' || help1.lastname DM_NAME,
            help1.genericattribute2 DIST_LEADER_CODE,
            help1.genericattribute7 DIST_LEADER_NAME,
            help1.genericattribute11 DIST_LEAER_TITLE,
            help1.genericattribute4 DIST_LEADER_CLASS,
            TMP.CD_GA12,
            TMP.CD_POSITIONSEQ,
            CASE WHEN TMP.TRANS_DIM_EVENTTYPE ='FYC'
            THEN TMP.CD_VALUE
            ELSE
            0
            END FYC,
           CASE WHEN TMP.TRANS_DIM_EVENTTYPE='FYC' and TMP.CD_GA4='PAYT'
            THEN TMP.CD_VALUE
            ELSE
            0
            END TOPUP_FYC,
            CASE WHEN TMP.TRANS_DIM_EVENTTYPE='RYC'
            THEN TMP.CD_VALUE
            ELSE
            0
            END RYC,
            TMP.TRANS_GN6,
            TMP.TRANS_GN5
        from
        AIA_RPT_PRD_TEMP_VALUES TMP
           inner join cs_position pos_agy
           on pos_agy.name = trim('SGY'||TMP.CD_GA13)
           AND pos_agy.effectivestartdate <= TMP.CD_GD2 --policy issue date
           AND pos_agy.effectiveenddate   > TMP.CD_GD2  --policy issue date
           AND pos_agy.removedate =DT_REMOVEDATE
           --for writing district postion info
           inner join AIA_RPT_PRD_DIST_WRI_NEW_HELP1 help1
           ON help1.name=trim('SGY'||pos_agy.genericattribute3)
           AND help1.effstartdt <= TMP.CD_GD2 --policy issue date
           AND help1.effenddt   > TMP.CD_GD2  --policy issue date
        WHERE TMP.NEW_PIB_IND=1 AND TMP.PIB_TYPE = 'PERSONAL'
         AND TMP.CD_CREDITTYPE IN ('FYP', 'FYC', 'RYC', 'API', 'SSCP', 'APB', 'Case_Count',
                             'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
                             'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
                             'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
                             'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
--        AND TMP.CD_GA2 IN ('LF', 'PA', 'HS', 'CS', 'PL', 'CL', 'VL')
AND TMP.CD_GA2 IN ('LF', 'PA')
 );

  Log('Records insert into AIA_RPT_PRD_POLICY_WRI_HELP 1 are '||SQL%ROWCOUNT);
 COMMIT;

     INSERT
  /*+ Append */
  INTO AIA_RPT_PRD_POLICY_WRI
    (PROCESSINGUNITSEQ, --PUKEY,
     PUNAME,
     BUNAME,
     BUMAP,
     CALENDARSEQ,
     CALNAME,
     PERIODSEQ,--PERIODKEY,
     PERIODNAME,
     POSITIONSEQ, --Pakey,
     MANAGERSEQ,
     POSITIONNAME,
     DISTRICT_CODE,
     DM_NAME,
     DIST_LEADER_CODE,
     DIST_LEADER_NAME,
     DIST_LEAER_TITLE,
     DIST_LEADER_CLASS,
     UNIT_CODE,
     AGENCY,
     UNIT_LEADER_CODE,
     UNIT_LEADER_NAME,
     UNIT_LEAER_TITLE,
     UNIT_LEADER_CLASS,
     DISOLVED_DATE,
     Agent_code,
     NAME,
     POLICYNO,
     ---add by jeff for 2 columns
     PLAN_DESCRIPTION,
     RISK_COMMENCEMENT_DATE,
     INCEPTION_DATE,
     LOB,
     POLICYYEAR,
     TYPE_COMMISSION,
     ---add by jeff for 2 columns
     API_AMT,
     SSCP_AMT,
     COMMISSION_AMT,
     FYP,
     CASECOUNT
     --ADD In version 5
     ,SUBMIT_DATE,
     FYC,
  TOPUP_FYC,
  RYC,
  PREMIUM,
  CASE_COUNT
     --end to add
     )
    (SELECT /*+  */ V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
            V_PROCESSINGUNITNAME PUNAME,
            decode(pad.BUSINESSUNITMAP,
                   V_BUSINESSUNITMAP1,
                   'SGPAFA',
                   V_BUSINESSUNITMAP2,
                   'BRUAGY') BUNAME,
            pad.BUSINESSUNITMAP BUMAP,
            V_CALENDARSEQ CALENDARSEQ,
            V_CALENDARNAME CALENDARNAME,
            V_PERIODSEQ PERIODSEQ,
            V_PERIODNAME periodname,
            PAD.POSITIONSEQ POSITIONSEQ,
            PAD.MANAGERSEQ,
            PAD.NAME,
/*            SUBSTR(DISTRICT.NAME, 4) DISTRICT_CODE,
            district.firstname || ' ' || district.lastname DM_NAme,
            district.POS_GA2 DIST_LEADER_CODE,
            district.POS_GA7 DIST_LEADER_NAME,
            district.POS_GA11 DIST_LEAER_TITLE,
            DISTRICT.POS_GA4 DIST_LEADER_CLASS,*/
            TMP.DISTRICT_CODE,
            TMP.DM_NAME,
            TMP.DIST_LEADER_CODE,
            TMP.DIST_LEADER_NAME,
            TMP.DIST_LEAER_TITLE,
            TMP.DIST_LEADER_CLASS,
            TMP.UNIT_CODE UNIT_CODE,
            agy.firstname || ' ' || agy.lastname AGENCY,
            agy.POS_GA2 UNIT_LEADER_CODE,
            agy.POS_GA7 UNIT_LEADER_NAME,
            DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
            agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
            (CASE
              WHEN AGY.POSITIONTITLE IN
                  ('ORGANISATION','UNIT')   THEN
               AGY.AGY_TERMINATIONDATE
            END) DISSOLVED_DATE,
            SUBSTR(PAD.NAME, 4) AGT_CODE,
            (PAD.firstname || PAD.LASTNAME) NAME,
            TMP.POLICYNO POLICYNO,
            ---add by jeff for 2 columns
            TMP.TRANS_GA3,
            TMP.CD_GD2,
            TMP.INCEPTION_DATE,
            TMP.LOB,
            TMP.POLICYYEAR,
            TMP.TYPE_COMMISSION,
            -----add by jeff for 2 columns
            TMP.API_AMT,
            TMP.SSCP_AMT,
            TMP.COMMISSION_AMT,
            TMP.FYP,
            TMP.CASECOUNT
           --ADD In version 5
            ,TMP.submitdate
            ,TMP.FYC
            ,TMP.TOPUP_FYC
  ,TMP.RYC,
  TMP.PREMIUM
  ,TMP.CASE_COUNT
     --end to add
       FROM AIA_RPT_PRD_TEMP_PADIM  pad,
            AIA_RPT_PRD_TEMP_PADIM  agy,
            AIA_RPT_PRD_TEMP_PADIM  agy_ldr,
            --AIA_RPT_PRD_TEMP_PADIM  DISTRICT,
            AIA_RPT_PRD_POLICY_WRI_HELP TMP
      WHERE pad.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
        and agy.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
        and agy_ldr.BUSINESSUNITMAP in
            (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
/*        AND DISTRICT.BUSINESSUNITMAP IN
            (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)*/
        AND PAD.POSITIONTITLE IN ('AD','DIR','ED','FC','SD')
        AND PAD.POS_GA4 NOT IN ('45', '48')
        AND (
        (TMP.BUMAP = 1 and TMP.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (TMP.BUMAP <> 1 and TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ)
        )
        AND agy.POS_GA1 = TMP.UNIT_CODE
        AND AGY.POSITIONTITLE IN
           ('ORGANISATION','UNIT')
        AND ((agy_ldr.NAME = 'SGT' || agy.POS_GA2 AND agy.NAME LIKE 'SGY%') OR
            (agy_ldr.NAME = 'BRT' || agy.POS_GA2 AND agy.NAME LIKE 'BRY%'))
    );

Log('Records inserted into AIA_RPT_PRD_POLICY_WRI 1 are '||SQL%ROWCOUNT);
COMMIT;
--End by Suresh

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_WRI' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;

--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_WRI' , 'AIA_RPT_PRD_POLICY_WRI 2'  , 'Processing', 'insert into AIA_RPT_PRD_POLICY_WRI');

/*commented by Suresh
  INSERT
  /*+ Append  */
  /*INTO AIA_RPT_PRD_POLICY_WRI
  (PROCESSINGUNITSEQ, --PUKEY,
   PUNAME,
   BUNAME,
   BUMAP,
   CALENDARSEQ,
   CALNAME,
   PERIODSEQ,--PERIODKEY
   PERIODNAME,
   POSITIONSEQ, --Pakey,
   MANAGERSEQ,
   POSITIONNAME,
   DISTRICT_CODE,
   DM_NAME,
   DIST_LEADER_CODE,
   DIST_LEADER_NAME,
   DIST_LEAER_TITLE,
   DIST_LEADER_CLASS,
   UNIT_CODE,
   AGENCY,
   UNIT_LEADER_CODE,
   UNIT_LEADER_NAME,
   UNIT_LEAER_TITLE,
   UNIT_LEADER_CLASS,
   DISOLVED_DATE,
   Agent_code,
   NAME,
   POLICYNO,
   ---add by jeff for 2 columns
   PLAN_DESCRIPTION,
   RISK_COMMENCEMENT_DATE,
   INCEPTION_DATE,
   LOB,
   POLICYYEAR,
   TYPE_COMMISSION,
   ---add by jeff for 2 columns
   API_AMT,
   SSCP_AMT,
   COMMISSION_AMT,
   FYP,
   CASECOUNT
   --ADD In version 5
   ,SUBMIT_DATE
   --end to add
   )
  (SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
          V_PROCESSINGUNITNAME PUNAME,
          decode(pad.BUSINESSUNITMAP,
                 V_BUSINESSUNITMAP1,
                 'SGPAFA',
                 V_BUSINESSUNITMAP2,
                 'BRUAGY') BUNAME,
          pad.BUSINESSUNITMAP BUMAP,
          V_CALENDARSEQ CALENDARSEQ,
          V_CALENDARNAME CALENDARNAME,
          V_PERIODSEQ PERIODSEQ,
          V_PERIODNAME periodname,
          PAD.POSITIONSEQ POSITIONSEQ,
          PAD.MANAGERSEQ,
          -- PAD.POSITIONSEQ POSITIONSEQ,
          PAD.NAME,
/*            SUBSTR(DISTRICT.NAME, 4) DISTRICT_CODE,
          district.firstname || ' ' || district.lastname DM_NAme,
          district.POS_GA2 DIST_LEADER_CODE,
          district.POS_GA7 DIST_LEADER_NAME,
          district.POS_GA11 DIST_LEAER_TITLE,
          DISTRICT.POS_GA4 DIST_LEADER_CLASS,*/
      /*      SUBSTR(pos_dis.NAME,4) DISTRICT_CODE,
      par_dis.firstname || ' ' || par_dis.lastname DM_NAME,
      pos_dis.genericattribute2 DIST_LEADER_CODE,
      pos_dis.genericattribute7 DIST_LEADER_NAME,
      pos_dis.genericattribute11 DIST_LEAER_TITLE,
      pos_dis.genericattribute4 DIST_LEADER_CLASS,
      TMP.TRANS_GA11 UNIT_CODE,
      --        PAD.PositionGenericAttribute1 UNIT_CODE,
      agy.firstname || ' ' || agy.lastname AGENCY,
      agy.POS_GA2 UNIT_LEADER_CODE,
      agy.POS_GA7 UNIT_LEADER_NAME,
      DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
      agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
      (CASE
        WHEN AGY.POSITIONTITLE IN
             ('AGENCY', 'DISTRICT', 'BR_AGENCY', 'BR_DISTRICT') THEN
         AGY.AGY_TERMINATIONDATE
      END) DISSOLVED_DATE,
      SUBSTR(PAD.NAME, 4) AGT_CODE,
      (PAD.firstname || PAD.LASTNAME) NAME,
      TMP.CD_GA6 POLICYNO,
      ---add by jeff for 2 columns
      TMP.TRANS_GA3,
      TMP.CD_GD2,
      CASE
        WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
             --TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
             TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB',
              'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
              'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
              'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
              'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
         TO_CHAR(TMP.TRANS_GD6, 'MM/DD/YYYY')
      END INCEPTION_DATE,
      TMP.CD_GA2 LOB,
      TMP.CD_GN1 POLICYYEAR,
      CASE
        WHEN TMP.TRANS_GA17 = 'O' AND
             TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
             --TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
             TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB',
              'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
              'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
              'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
              'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
         'OWN'
        WHEN TMP.TRANS_GA17 = 'RNO' OR
             TMP.TRANS_GA17 = 'RO' AND
             --TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
             TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB',
              'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
              'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
              'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
              'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
         'ASSIGN'
      END TYPE_COMMISSION,
      -----add by jeff for 2 columns
      0 API_AMT,
      0 SSCP_AMT,
      0 COMMISSION_AMT,
      0 FYP,
      CASE
        WHEN TMP.TRANS_DIM_PRODUCTNAME IN
             ('LF', 'HS', 'PA', 'PL', 'CS', 'VL')
            --  AND TMP.CF_VALUE <> 0
             AND TMP.CD_CREDITTYPE = 'Case_Count' THEN
         TMP.TRANS_GN5
        ELSE
         0
      END CASECOUNT
    --ADD In version 5
     ,TMP.submitdate
     --end to add
 FROM AIA_RPT_PRD_TEMP_PADIM  pad,
      AIA_RPT_PRD_TEMP_PADIM  agy,
      AIA_RPT_PRD_TEMP_PADIM  agy_ldr,
      --AIA_RPT_PRD_TEMP_PADIM  DISTRICT,
      AIA_RPT_PRD_TEMP_VALUES TMP
     inner join cs_position pos_agy
     on pos_agy.name = trim('SGY'||TMP.CD_GA13)
     AND pos_agy.effectivestartdate <= TMP.CD_GD2 --policy issue date
     AND pos_agy.effectiveenddate   > TMP.CD_GD2  --policy issue date
     AND pos_agy.removedate =DT_REMOVEDATE
     --for writing district postion info
     inner join cs_position pos_dis
     on pos_dis.name=trim('SGY'||pos_agy.genericattribute3)
     AND pos_dis.effectivestartdate <= TMP.CD_GD2 --policy issue date
     AND pos_dis.effectiveenddate   > TMP.CD_GD2  --policy issue date
     AND pos_dis.removedate =DT_REMOVEDATE
     --for writing district participant info
     inner join cs_participant par_dis
     on par_dis.PAYEESEQ = pos_dis.PAYEESEQ
     AND par_dis.effectivestartdate < V_ENDDATE
     AND par_dis.effectiveenddate   >  V_ENDDATE-1
     AND par_dis.removedate = DT_REMOVEDATE
WHERE TMP.NEW_PRD_IND=1 AND TMP.PRD_TYPE = 'DIRECT'
  AND pad.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
  and agy.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
  and agy_ldr.BUSINESSUNITMAP in
      (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
  --AND DISTRICT.BUSINESSUNITMAP IN (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
  AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD','AM','FSD','FSM','BR_DM','BR_FSC','BR_UM')
  AND PAD.POS_GA4 NOT IN ('45', '48')
  --AND TMP.CD_CREDITTYPE IN ('FYP', 'RYC', 'FYC', 'API', 'SSCP', 'APB', 'Case_Count')
  AND TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB', 'Case_Count',
              'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
              'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
              'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
              'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
  --AND TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ
  AND (
  (TMP.businessunitmap = 1 and TMP.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (TMP.businessunitmap <> 1 and TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ)
  )
  AND agy.POS_GA1 = TMP.TRANS_GA11
  AND AGY.POSITIONTITLE IN
      ('AGENCY', 'DISTRICT', 'BR_AGENCY', 'BR_DISTRICT')
  AND ((agy_ldr.NAME = 'SGT' || agy.POS_GA2 AND agy.NAME LIKE 'SGY%') OR
      (agy_ldr.NAME = 'BRT' || agy.POS_GA2 AND agy.NAME LIKE 'BRY%'))
  --AND DISTRICT.POS_GA3 = AGY.POS_GA3
  --AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
  --AND district.positiontitle IN ('DISTRICT', 'BR_DISTRICT')
  AND TMP.CD_GA2 IN ('LF', 'PA', 'HS', 'CS', 'PL', 'CL', 'VL'));
  */

        --Added by Suresh
--          EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_POLICY_WRI_HELP';
--
--insert /*+APPEND*/ into AIA_RPT_PRD_POLICY_WRI_HELP NOLOGGING
--(select /*+ PARALLEL(30) */
--       ' ' BUNAME,
--      --decode(PD.businessunitmap,1,'SGPAFA',2,'BRUAGY') BUNAME,
--        TMP.businessunitmap BUMAP,
--        TMP.TRANS_GA11 UNIT_CODE,
--            TMP.CD_GA6 POLICYNO,
--            ---add by jeff for 2 columns
--            TMP.TRANS_GA3,
--            TMP.CD_GD2,
--            CASE
--              WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
--                   --TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
--                   TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB',
--                    'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
--                    'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
--                    'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
--                    'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
--               TO_CHAR(TMP.TRANS_GD6, 'MM/DD/YYYY')
--            END INCEPTION_DATE,
--            TMP.CD_GA2 LOB,
--            TMP.CD_GN1 POLICYYEAR,
--            CASE
--              WHEN TMP.TRANS_GA17 = 'O' AND
--                   TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
--                   --TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
--                   TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB',
--                    'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
--                    'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
--                    'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
--                    'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
--               'OWN'
--              WHEN TMP.TRANS_GA17 = 'RNO' OR
--                   TMP.TRANS_GA17 = 'RO' AND
--                   --TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
--                   TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB',
--                    'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
--                    'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
--                    'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
--                    'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
--               'ASSIGN'
--            END TYPE_COMMISSION,
--            -----add by jeff for 2 columns
--            0 API_AMT,
--            0 SSCP_AMT,
--            0 COMMISSION_AMT,
--            0 FYP,
--            CASE
--              WHEN TMP.TRANS_DIM_PRODUCTNAME IN
--                   ('LF', 'HS', 'PA', 'PL', 'CS', 'VL')
--                  --  AND TMP.CF_VALUE <> 0
--                   AND TMP.CD_CREDITTYPE = 'Case_Count' THEN
--               TMP.TRANS_GN5
--              ELSE
--               0
--            END CASECOUNT
--           --ADD In version 5
--            ,TMP.submitdate,
--     --end to add
--     SUBSTR(help1.NAME,4) DISTRICT_CODE,
--            help1.firstname || ' ' || help1.lastname DM_NAME,
--            help1.genericattribute2 DIST_LEADER_CODE,
--            help1.genericattribute7 DIST_LEADER_NAME,
--            help1.genericattribute11 DIST_LEAER_TITLE,
--            help1.genericattribute4 DIST_LEADER_CLASS,
--            TMP.CD_GA12,
--            TMP.CD_POSITIONSEQ,
--            CASE WHEN TMP.CD_CREDITTYPE='FYC' and TMP.CD_GA2<>'PAYT'
--            THEN TMP.CD_VALUE
--            ELSE
--            0
--            END FYC,
--           CASE WHEN TMP.CD_CREDITTYPE='FYC' and TMP.CD_GA2='PAYT'
--            THEN TMP.CD_VALUE
--            ELSE
--            0
--            END TOPUP_FYC,
--            CASE WHEN TMP.CD_CREDITTYPE='RYC'
--            THEN TMP.CD_VALUE
--            ELSE
--            0
--            END RYC
--        from
--        AIA_RPT_PRD_TEMP_VALUES TMP
--           inner join cs_position pos_agy
--           on pos_agy.name = trim('SGY'||TMP.CD_GA13)
--           AND pos_agy.effectivestartdate <= TMP.CD_GD2 --policy issue date
--           AND pos_agy.effectiveenddate   > TMP.CD_GD2  --policy issue date
--           AND pos_agy.removedate =DT_REMOVEDATE
--           --for writing district postion info
--           inner join AIA_RPT_PRD_DIST_WRI_NEW_HELP1 help1
--           ON help1.name=trim('SGY'||pos_agy.genericattribute3)
--           AND help1.effstartdt <= TMP.CD_GD2 --policy issue date
--           AND help1.effenddt   > TMP.CD_GD2  --policy issue date
--        WHERE TMP.NEW_PIB_IND=1 AND TMP.PIB_TYPE = 'DIRECT'
--         AND TMP.CD_CREDITTYPE IN ('FYC', 'RYC', 'API', 'SSCP', 'APB', 'Case_Count',
--                    'API_W', 'API_W_DUPLICATE', 'API_WC_DUPLICATE',
--                    'SSCP_W', 'SSCP_W_DUPLICATE', 'SSCP_WC_DUPLICATE',
--                    'FYC_W', 'FYC_W_DUPLICATE', 'FYC_WC_DUPLICATE',
--                    'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
--        AND TMP.CD_GA2 IN ('LF', 'PA', 'HS', 'CS', 'PL', 'CL', 'VL')
-- );
--
--
-- Log('Records insert into AIA_RPT_PRD_POLICY_WRI_HELP 2 are '||SQL%ROWCOUNT);
-- COMMIT;
--
--
--
--   INSERT
--  /*+ Append  */
--  INTO AIA_RPT_PRD_POLICY_WRI
--    (PROCESSINGUNITSEQ, --PUKEY,
--     PUNAME,
--     BUNAME,
--     BUMAP,
--     CALENDARSEQ,
--     CALNAME,
--     PERIODSEQ,--PERIODKEY
--     PERIODNAME,
--     POSITIONSEQ, --Pakey,
--     MANAGERSEQ,
--     POSITIONNAME,
--     DISTRICT_CODE,
--     DM_NAME,
--     DIST_LEADER_CODE,
--     DIST_LEADER_NAME,
--     DIST_LEAER_TITLE,
--     DIST_LEADER_CLASS,
--     UNIT_CODE,
--     AGENCY,
--     UNIT_LEADER_CODE,
--     UNIT_LEADER_NAME,
--     UNIT_LEAER_TITLE,
--     UNIT_LEADER_CLASS,
--     DISOLVED_DATE,
--     Agent_code,
--     NAME,
--     POLICYNO,
--     ---add by jeff for 2 columns
--     PLAN_DESCRIPTION,
--     RISK_COMMENCEMENT_DATE,
--     INCEPTION_DATE,
--     LOB,
--     POLICYYEAR,
--     TYPE_COMMISSION,
--     ---add by jeff for 2 columns
--     API_AMT,
--     SSCP_AMT,
--     COMMISSION_AMT,
--     FYP,
--     CASECOUNT
--     --ADD In version 5
--     ,SUBMIT_DATE
--     ,FYC
--  ,TOPUP_FYC
--  ,RYC
--     --end to add
--     )
--    (SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
--            V_PROCESSINGUNITNAME PUNAME,
--            decode(pad.BUSINESSUNITMAP,
--                   V_BUSINESSUNITMAP1,
--                   'SGPAFA',
--                   V_BUSINESSUNITMAP2,
--                   'BRUAGY') BUNAME,
--            pad.BUSINESSUNITMAP BUMAP,
--            V_CALENDARSEQ CALENDARSEQ,
--            V_CALENDARNAME CALENDARNAME,
--            V_PERIODSEQ PERIODSEQ,
--            V_PERIODNAME periodname,
--            PAD.POSITIONSEQ POSITIONSEQ,
--            PAD.MANAGERSEQ,
--            -- PAD.POSITIONSEQ POSITIONSEQ,
--            PAD.NAME,
--/*            SUBSTR(DISTRICT.NAME, 4) DISTRICT_CODE,
--            district.firstname || ' ' || district.lastname DM_NAme,
--            district.POS_GA2 DIST_LEADER_CODE,
--            district.POS_GA7 DIST_LEADER_NAME,
--            district.POS_GA11 DIST_LEAER_TITLE,
--            DISTRICT.POS_GA4 DIST_LEADER_CLASS,*/
--            TMP.DISTRICT_CODE,
--            TMP.DM_NAME,
--            TMP.DIST_LEADER_CODE,
--            TMP.DIST_LEADER_NAME,
--            TMP.DIST_LEAER_TITLE,
--            TMP.DIST_LEADER_CLASS,
--            TMP.UNIT_CODE,
--            --        PAD.PositionGenericAttribute1 UNIT_CODE,
--            agy.firstname || ' ' || agy.lastname AGENCY,
--            agy.POS_GA2 UNIT_LEADER_CODE,
--            agy.POS_GA7 UNIT_LEADER_NAME,
--            DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
--            agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
--            (CASE
--              WHEN AGY.POSITIONTITLE IN
--                  ('ORGANISATION','UNIT')   THEN
--               AGY.AGY_TERMINATIONDATE
--            END) DISSOLVED_DATE,
--            SUBSTR(PAD.NAME, 4) AGT_CODE,
--            (PAD.firstname || PAD.LASTNAME) NAME,
--            TMP.POLICYNO,
--            ---add by jeff for 2 columns
--            TMP.TRANS_GA3,
--            TMP.CD_GD2,
--            TMP.INCEPTION_DATE,
--            TMP.LOB,
--            TMP.POLICYYEAR,
--            TMP.TYPE_COMMISSION,
--            -----add by jeff for 2 columns
--            0 API_AMT,
--            0 SSCP_AMT,
--            0 COMMISSION_AMT,
--            0 FYP,
--            TMP.CASECOUNT
--          --ADD In version 5
--           ,TMP.submitdate
--           ,TMP.FYC
--  ,TMP.TOPUP_FYC
--  ,TMP.RYC
--           --end to add
--       FROM AIA_RPT_PRD_TEMP_PADIM  pad,
--            AIA_RPT_PRD_TEMP_PADIM  agy,
--            AIA_RPT_PRD_TEMP_PADIM  agy_ldr,
--            --AIA_RPT_PRD_TEMP_PADIM  DISTRICT,
--            AIA_RPT_PRD_POLICY_WRI_HELP TMP
--      WHERE pad.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        and agy.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        and agy_ldr.BUSINESSUNITMAP in
--            (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        --AND DISTRICT.BUSINESSUNITMAP IN (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        AND PAD.POSITIONTITLE IN ('AD','DIR','ED','FC','SD')
--        AND PAD.POS_GA4 NOT IN ('45', '48')
--        --AND TMP.CD_CREDITTYPE IN ('FYP', 'RYC', 'FYC', 'API', 'SSCP', 'APB', 'Case_Count')
--        --AND TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ
--        AND (
--        (TMP.BUMAP = 64 and TMP.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (TMP.BUMAP <> 64 and TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ)
--        )
--        AND agy.POS_GA1 = TMP.UNIT_CODE
--        AND AGY.POSITIONTITLE IN
--           ('ORGANISATION','UNIT')
--        AND ((agy_ldr.NAME = 'SGT' || agy.POS_GA2 AND agy.NAME LIKE 'SGY%'))
--        --AND DISTRICT.POS_GA3 = AGY.POS_GA3
--        --AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
--        --AND district.positiontitle IN ('DISTRICT', 'BR_DISTRICT')
--       );
--
--        Log('Records insert into AIA_RPT_PRD_POLICY_WRI 2 are '||SQL%ROWCOUNT);
--  COMMIT;
 --Ended by Suresh

  /* End of above Insert to capture CASE COUNT Of Brunei Records which doesn't CreditType = 'FYP' coming from 'Case_Count */
  COMMIT;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_WRI' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;

--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_WRI' , 'AIA_RPT_PRD_POLICY_WRI 3'  , 'Processing', 'insert into AIA_RPT_PRD_POLICY_WRI');


/*commented by Suresh
INSERT
  /*+ Append  */
/*  INTO AIA_RPT_PRD_POLICY_WRI
    (PROCESSINGUNITSEQ, --PUKEY,
     PUNAME,
     BUNAME,
     BUMAP,
     CALENDARSEQ,
     CALNAME,
     PERIODSEQ,--PERIODKEY
     PERIODNAME,
     POSITIONSEQ, --POSITIONSEQ,
     MANAGERSEQ,
     POSITIONNAME,
     DISTRICT_CODE,
     DM_NAME,
     DIST_LEADER_CODE,
     DIST_LEADER_NAME,
     DIST_LEAER_TITLE,
     DIST_LEADER_CLASS,
     UNIT_CODE,
     AGENCY,
     UNIT_LEADER_CODE,
     UNIT_LEADER_NAME,
     UNIT_LEAER_TITLE,
     UNIT_LEADER_CLASS,
     DISOLVED_DATE,
     Agent_code,
     NAME,
     POLICYNO,
     ---add by jeff for 2 columns
     PLAN_DESCRIPTION,
     RISK_COMMENCEMENT_DATE,
     INCEPTION_DATE,
     LOB,
     POLICYYEAR,
     TYPE_COMMISSION,
     ---add by jeff for 2 columns
     API_AMT,
     SSCP_AMT,
     COMMISSION_AMT,
     FYP,
     CASECOUNT
      --ADD In version 5
     ,SUBMIT_DATE
     --end to add
     )
    (SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
            V_PROCESSINGUNITNAME PUNAME,
            decode(pad.BUSINESSUNITMAP,
                   V_BUSINESSUNITMAP1,
                   'SGPAFA',
                   V_BUSINESSUNITMAP2,
                   'BRUAGY') BUNAME,
            pad.BUSINESSUNITMAP BUMAP,
            V_CALENDARSEQ CALENDARSEQ,
            V_CALENDARNAME CALENDARNAME,
            V_PERIODSEQ PERIODSEQ,
            V_PERIODNAME periodname,
            PAD_AGT.POSITIONSEQ POSITIONSEQ,
            PAD_AGT.MANAGERSEQ,
            pad_agt.NAME,
/*            SUBSTR(DISTRICT.NAME, 4) DISTRICT_CODE,
            -- district.POSITIONGENERICATTRIBUTE3 DISTRICT_CODE,
            district.firstname || ' ' || district.lastname DM_NAme,
            district.POS_GA2 DIST_LEADER_CODE,
            district.POS_GA7 DIST_LEADER_NAME,
            district.POS_GA11 DIST_LEAER_TITLE,
            DISTRICT.POS_GA4 DIST_LEADER_CLASS,*/
    /*        SUBSTR(pos_dis.NAME,4) DISTRICT_CODE,
        par_dis.firstname || ' ' || par_dis.lastname DM_NAME,
        pos_dis.genericattribute2 DIST_LEADER_CODE,
        pos_dis.genericattribute7 DIST_LEADER_NAME,
        pos_dis.genericattribute11 DIST_LEAER_TITLE,
        pos_dis.genericattribute4 DIST_LEADER_CLASS,
        --        SUBSTR(PAD.NAME,4) UNIT_CODE,
        TMP.CD_GA13 UNIT_CODE,
        agy.firstname || ' ' || agy.lastname AGENCY,
        agy_ldr.POS_GA2 UNIT_LEADER_CODE,
        agy_ldr.POS_GA7 UNIT_LEADER_NAME,
        DECODE(agy_ldr.POS_GA11,
               'FSC_NON_PROCESS',
               'FSC',
               agy_ldr.POS_GA11) UNIT_LEAER_TITLE,
        agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
        (CASE
          WHEN agy.POSITIONTITLE IN
               ('AGENCY', 'DISTRICT', 'BR_AGENCY', 'BR_DISTRICT') THEN
           agy.AGY_TERMINATIONDATE
        END) DISSOLVED_DATE,
        SUBSTR(pad_agt.NAME, 4) AGT_CODE,
        (pad_agt.firstname || PAD_AGT.LASTNAME) NAME,
        TMP.CD_GA6 POLICYNO,
        ---add by jeff for 2 columns
        TMP.TRANS_GA3,
        TMP.CD_GD2,
        CASE
          WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
               --TMP.CD_CREDITTYPE IN ('ORYC', 'RYC') THEN
               TMP.CD_CREDITTYPE IN ('ORYC', 'RYC', 'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
           TO_CHAR(TMP.TRANS_GD6, 'MM/DD/YYYY')
        END INCEPTION_DATE,
        TMP.CD_GA2 LOB,
        TMP.CD_GN1 POLICYYEAR,
        CASE
          WHEN TMP.TRANS_GA17 = 'O' AND
               TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
               --TMP.CD_CREDITTYPE IN ('RYC') THEN
               TMP.CD_CREDITTYPE IN ('RYC', 'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
           'UNASSIGN'
          WHEN TMP.TRANS_GA17 = 'RNO' OR
               TMP.TRANS_GA17 = 'RO'
               --AND TMP.CD_CREDITTYPE IN ('RYC') THEN
               AND TMP.CD_CREDITTYPE IN ('RYC', 'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
           'UNASSIGN'
          WHEN TMP.TRANS_GA14 = '48' AND
               TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
               TMP.CD_CREDITTYPE = 'ORYC' then
           'UNASSIGN'
        END TYPE_COMMISSION,
        -----add by jeff for 2 columns
        0 API_AMT,
        0 SSCP_AMT,
        (CASE
          WHEN (TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
               --TMP.CD_CREDITTYPE IN ('ORYC', 'RYC')
               TMP.CD_CREDITTYPE IN ('ORYC', 'RYC', 'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
               ) THEN
           TMP.CD_VALUE
          ELSE
           0
        END) AS COMMISSION_AMT,
        (CASE
          WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
               --TMP.CD_CREDITTYPE IN ('ORYC', 'RYC') AND
               TMP.CD_CREDITTYPE IN ('ORYC', 'RYC', 'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') AND
               TMP.TRANS_GN3 <> 0 THEN
           TMP.TRANS_GN6
          ELSE
           0
        END) AS FYP,
        (CASE
          WHEN TMP.TRANS_DIM_PRODUCTNAME IN ('LF', 'HS', 'PA', 'VL') AND
               TMP.TRANS_DIM_EVENTTYPE = 'FYP' THEN
           TMP.TRANS_GN5
          ELSE
           0
        END) AS CASECOUNT
       --ADD In version 5
       ,TMP.submitdate
       --end to add
   FROM AIA_RPT_PRD_TEMP_PADIM  PAD,
        AIA_RPT_PRD_TEMP_PADIM  pad_agt,
        AIA_RPT_PRD_TEMP_PADIM  agy,
        AIA_RPT_PRD_TEMP_PADIM  AGY_LDR,
        --AIA_RPT_PRD_TEMP_PADIM  DISTRICT,
        AIA_RPT_PRD_TEMP_VALUES TMP
       inner join cs_position pos_agy
       on pos_agy.name = trim('SGY'||TMP.CD_GA13)
       AND pos_agy.effectivestartdate <= TMP.CD_GD2 --policy issue date
       AND pos_agy.effectiveenddate   > TMP.CD_GD2  --policy issue date
       AND pos_agy.removedate =DT_REMOVEDATE
       --for writing district postion info
       inner join cs_position pos_dis
       on pos_dis.name=trim('SGY'||pos_agy.genericattribute3)
       AND pos_dis.effectivestartdate <= TMP.CD_GD2 --policy issue date
       AND pos_dis.effectiveenddate   > TMP.CD_GD2  --policy issue date
       AND pos_dis.removedate =DT_REMOVEDATE
       --for writing district participant info
       inner join cs_participant par_dis
       on par_dis.PAYEESEQ = pos_dis.PAYEESEQ
       AND par_dis.effectivestartdate < V_ENDDATE
       AND par_dis.effectiveenddate   >  V_ENDDATE-1
       AND par_dis.removedate = DT_REMOVEDATE
  WHERE TMP.NEW_PRD_IND=1 AND TMP.PRD_TYPE = 'DIRECT'
    AND pad.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
    and agy.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
    and agy_ldr.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
    --AND DISTRICT.BUSINESSUNITMAP IN (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
    AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD','AM','FSD','FSM','BR_DM','BR_FSC','BR_UM')
    AND PAD.POS_GA4 NOT IN ('45') --,'48')
    --AND TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ
    AND (
    (TMP.businessunitmap = 1 and TMP.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (TMP.businessunitmap <> 1 and TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ)
    )
    --AND TMP.CD_CREDITTYPE IN ('ORYC', 'RYC', 'Case_Count')
    AND TMP.CD_CREDITTYPE IN ('ORYC', 'RYC','Case_Count', 'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
    AND AGY.POS_GA1 = TMP.CD_GA13
    --AND SUBSTR(PAD.NAME, 4) = TMP.TRANS_GA11
    --AND SUBSTR(PAD_AGT.NAME, 4) = TMP.TRANS_GA10
    AND SUBSTR(PAD.NAME,4)  = TMP.CD_GA13
    AND SUBSTR(PAD_AGT.NAME,4) = TMP.CD_GA12 and SUBSTR(PAD_AGT.NAME,3,1)='T'
    AND AGY.POSITIONTITLE IN
        ('AGENCY', 'DISTRICT', 'BR_AGENCY', 'BR_DISTRICT')
    AND ((agy_ldr.NAME = 'SGT' || agy.POS_GA2 AND agy.NAME LIKE 'SGY%') OR
        (agy_ldr.NAME = 'BRT' || agy.POS_GA2 AND agy.NAME LIKE 'BRY%'))
    --AND DISTRICT.POS_GA3 = AGY.POS_GA3
    --AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
    --AND district.positiontitle IN ('DISTRICT', 'BR_DISTRICT')
    AND TMP.CD_GA2 IN ('LF', 'PA', 'HS', 'VL'));
*/

--Added by suresh
          EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_POLICY_WRI_HELP';
          Log('TRUNCATE DONE AIA_RPT_PRD_POLICY_WRI_HELP ');

insert /*+APPEND*/ into AIA_RPT_PRD_POLICY_WRI_HELP NOLOGGING
(select /*+   */
       ' ' BUNAME,
      --decode(PD.businessunitmap,1,'SGPAFA',2,'BRUAGY') BUNAME,
        TMP.businessunitmap BUMAP,
        TMP.CD_GA13 UNIT_CODE,
        TMP.CD_GA6 POLICYNO,
            ---add by jeff for 2 columns
            TMP.TRANS_GA3,
            TMP.CD_GD2,
            CASE
              WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
                   --TMP.CD_CREDITTYPE IN ('ORYC', 'RYC') THEN
                   TMP.CD_CREDITTYPE IN ('ORYC', 'RYC', 'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
               TO_CHAR(TMP.TRANS_GD6, 'MM/DD/YYYY')
            END INCEPTION_DATE,
            TMP.CD_GA2 LOB,
            TMP.CD_GN1 POLICYYEAR,
            CASE
              WHEN TMP.TRANS_GA17 = 'O' AND
                   TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
                   --TMP.CD_CREDITTYPE IN ('RYC') THEN
                   TMP.CD_CREDITTYPE IN ('RYC', 'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
               'UNASSIGN'
              WHEN TMP.TRANS_GA17 = 'RNO' OR
                   TMP.TRANS_GA17 = 'RO'
                   --AND TMP.CD_CREDITTYPE IN ('RYC') THEN
                   AND TMP.CD_CREDITTYPE IN ('RYC', 'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') THEN
               'UNASSIGN'
              WHEN TMP.TRANS_GA14 = '48' AND
                   TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
                   TMP.CD_CREDITTYPE = 'ORYC' then
               'UNASSIGN'
            END TYPE_COMMISSION,
            -----add by jeff for 2 columns
            0 API_AMT,
            0 SSCP_AMT,
            (CASE
              WHEN (TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
                   --TMP.CD_CREDITTYPE IN ('ORYC', 'RYC')
                   TMP.CD_CREDITTYPE IN ('ORYC', 'RYC', 'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
                   ) THEN
               TMP.CD_VALUE
              ELSE
               0
            END) AS COMMISSION_AMT,
            (CASE
              WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
                   --TMP.CD_CREDITTYPE IN ('ORYC', 'RYC') AND
                   TMP.CD_CREDITTYPE IN ('ORYC', 'RYC', 'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE') AND
                   TMP.TRANS_GN3 <> 0 THEN
               TMP.TRANS_GN6
              ELSE
               0
            END) AS FYP,
            (CASE
              WHEN TMP.TRANS_DIM_PRODUCTNAME IN ('LF', 'HS', 'PA', 'VL') AND
                   TMP.TRANS_DIM_EVENTTYPE = 'FYP' THEN
               TMP.TRANS_GN5
              ELSE
               0
            END) AS CASECOUNT
           --ADD In version 5
           ,TMP.submitdate,

     --end to add
     SUBSTR(help1.NAME,4) DISTRICT_CODE,
            help1.firstname || ' ' || help1.lastname DM_NAME,
            help1.genericattribute2 DIST_LEADER_CODE,
            help1.genericattribute7 DIST_LEADER_NAME,
            help1.genericattribute11 DIST_LEAER_TITLE,
            help1.genericattribute4 DIST_LEADER_CLASS,
            TMP.CD_GA12,
            TMP.CD_POSITIONSEQ,
              CASE WHEN TMP.CD_CREDITTYPE='FYC'
            THEN TMP.CD_VALUE
            ELSE
            0
            END FYC,
           CASE WHEN TMP.CD_CREDITTYPE='FYC' and TMP.CD_GA2='PAYT'
            THEN TMP.CD_VALUE
            ELSE
            0
            END TOPUP_FYC,
            CASE WHEN TMP.CD_CREDITTYPE='RYC'
            THEN TMP.CD_VALUE
            ELSE
            0
            END RYC,
            TMP.TRANS_GN6 PREMIUM,
            TMP.TRANS_GN5 CASE_COUNT
        from
        AIA_RPT_PRD_TEMP_VALUES TMP
           inner join cs_position pos_agy
           on pos_agy.name = trim('SGY'||TMP.CD_GA13)
           AND pos_agy.effectivestartdate <= TMP.CD_GD2 --policy issue date
           AND pos_agy.effectiveenddate   > TMP.CD_GD2  --policy issue date
           AND pos_agy.removedate =DT_REMOVEDATE
           --for writing district postion info
           inner join AIA_RPT_PRD_DIST_WRI_NEW_HELP1 help1
           ON help1.name=trim('SGY'||pos_agy.genericattribute3)
           AND help1.effstartdt <= TMP.CD_GD2 --policy issue date
           AND help1.effenddt   > TMP.CD_GD2  --policy issue date
        WHERE TMP.NEW_PIB_IND=1 AND TMP.PIB_TYPE = 'DIRECT'
         AND TMP.CD_CREDITTYPE IN ('ORYC', 'RYC','Case_Count', 'RYC_W', 'RYC_W_DUPLICATE', 'RYC_WC_DUPLICATE')
--        AND TMP.CD_GA2 IN ('LF', 'PA', 'HS', 'VL')
 AND TMP.CD_GA2 IN ('LF', 'PA')
 );


  Log('Records insert in AIA_RPT_PRD_POLICY_WRI_HELP3 are'||SQL%ROWCOUNT);
 COMMIT;




   INSERT
  /*+ Append  */
  INTO AIA_RPT_PRD_POLICY_WRI
    (PROCESSINGUNITSEQ, --PUKEY,
     PUNAME,
     BUNAME,
     BUMAP,
     CALENDARSEQ,
     CALNAME,
     PERIODSEQ,--PERIODKEY
     PERIODNAME,
     POSITIONSEQ, --POSITIONSEQ,
     MANAGERSEQ,
     POSITIONNAME,
     DISTRICT_CODE,
     DM_NAME,
     DIST_LEADER_CODE,
     DIST_LEADER_NAME,
     DIST_LEAER_TITLE,
     DIST_LEADER_CLASS,
     UNIT_CODE,
     AGENCY,
     UNIT_LEADER_CODE,
     UNIT_LEADER_NAME,
     UNIT_LEAER_TITLE,
     UNIT_LEADER_CLASS,
     DISOLVED_DATE,
     Agent_code,
     NAME,
     POLICYNO,
     ---add by jeff for 2 columns
     PLAN_DESCRIPTION,
     RISK_COMMENCEMENT_DATE,
     INCEPTION_DATE,
     LOB,
     POLICYYEAR,
     TYPE_COMMISSION,
     ---add by jeff for 2 columns
     API_AMT,
     SSCP_AMT,
     COMMISSION_AMT,
     FYP,
     CASECOUNT
      --ADD In version 5
     ,SUBMIT_DATE
          ,FYC
  ,TOPUP_FYC
  ,RYC,
  PREMIUM,
  CASE_COUNT
     --end to add
     )
    (SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
            V_PROCESSINGUNITNAME PUNAME,
            decode(pad.BUSINESSUNITMAP,
                   V_BUSINESSUNITMAP1,
                   'SGPAFA',
                   V_BUSINESSUNITMAP2,
                   'BRUAGY') BUNAME,
            pad.BUSINESSUNITMAP BUMAP,
            V_CALENDARSEQ CALENDARSEQ,
            V_CALENDARNAME CALENDARNAME,
            V_PERIODSEQ PERIODSEQ,
            V_PERIODNAME periodname,
            PAD_AGT.POSITIONSEQ POSITIONSEQ,
            PAD_AGT.MANAGERSEQ,
            pad_agt.NAME,
/*            SUBSTR(DISTRICT.NAME, 4) DISTRICT_CODE,
            -- district.POSITIONGENERICATTRIBUTE3 DISTRICT_CODE,
            district.firstname || ' ' || district.lastname DM_NAme,
            district.POS_GA2 DIST_LEADER_CODE,
            district.POS_GA7 DIST_LEADER_NAME,
            district.POS_GA11 DIST_LEAER_TITLE,
            DISTRICT.POS_GA4 DIST_LEADER_CLASS,*/
            TMP.DISTRICT_CODE,
            TMP.DM_NAME,
            TMP.DIST_LEADER_CODE,
            TMP.DIST_LEADER_NAME,
            TMP.DIST_LEAER_TITLE,
            TMP.DIST_LEADER_CLASS,
            --        SUBSTR(PAD.NAME,4) UNIT_CODE,
            TMP.UNIT_CODE,
            agy.firstname || ' ' || agy.lastname AGENCY,
            agy_ldr.POS_GA2 UNIT_LEADER_CODE,
            agy_ldr.POS_GA7 UNIT_LEADER_NAME,
            DECODE(agy_ldr.POS_GA11,
                   'FSC_NON_PROCESS',
                   'FSC',
                   agy_ldr.POS_GA11) UNIT_LEAER_TITLE,
            agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
            (CASE
              WHEN agy.POSITIONTITLE IN
                  ('ORGANISATION','UNIT')   THEN
               agy.AGY_TERMINATIONDATE
            END) DISSOLVED_DATE,
            SUBSTR(pad_agt.NAME, 4) AGT_CODE,
            (pad_agt.firstname || PAD_AGT.LASTNAME) NAME,
            TMP.POLICYNO,
            ---add by jeff for 2 columns
            TMP.TRANS_GA3,
            TMP.CD_GD2,
            TMP.INCEPTION_DATE,
            TMP.LOB,
            TMP.POLICYYEAR,
            TMP.TYPE_COMMISSION,
            -----add by jeff for 2 columns
            0 API_AMT,
            0 SSCP_AMT,
            TMP.COMMISSION_AMT,
            TMP.FYP,
            TMP.CASECOUNT
           --ADD In version 5
           ,TMP.submitdate
            ,TMP.FYC
  ,TMP.TOPUP_FYC
  ,TMP.RYC
  ,TMP.PREMIUM
  ,TMP.CASE_COUNT
           --end to add
       FROM AIA_RPT_PRD_TEMP_PADIM  PAD,
            AIA_RPT_PRD_TEMP_PADIM  pad_agt,
            AIA_RPT_PRD_TEMP_PADIM  agy,
            AIA_RPT_PRD_TEMP_PADIM  AGY_LDR,
            --AIA_RPT_PRD_TEMP_PADIM  DISTRICT,
            AIA_RPT_PRD_POLICY_WRI_HELP TMP
      WHERE pad.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
        and agy.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
        and agy_ldr.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
        --AND DISTRICT.BUSINESSUNITMAP IN (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
        AND PAD.POSITIONTITLE IN ('AD','DIR','ED','FC','SD')
        AND PAD.POS_GA4 NOT IN ('45') --,'48')
        --AND TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ
        AND (
        (TMP.BUMAP = 64 and TMP.CD_GA12 = substr(PAD.name,4) and substr(PAD.name,1,3) = 'SGT' ) or (TMP.BUMAP <> 64 and TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ)
        )
       AND AGY.POS_GA1 = TMP.UNIT_CODE
        --AND SUBSTR(PAD.NAME, 4) = TMP.TRANS_GA11
        --AND SUBSTR(PAD_AGT.NAME, 4) = TMP.TRANS_GA10
        AND SUBSTR(PAD.NAME,4)  = TMP.UNIT_CODE
        AND SUBSTR(PAD_AGT.NAME,4) = TMP.CD_GA12 and SUBSTR(PAD_AGT.NAME,3,1)='T'
        AND AGY.POSITIONTITLE IN
           ('ORGANISATION','UNIT')
        AND ((agy_ldr.NAME = 'SGT' || agy.POS_GA2 AND agy.NAME LIKE 'SGY%') )
        );

  Log('Records insert into AIA_RPT_PRD_POLICY_WRI 3 are '||SQL%ROWCOUNT);
 COMMIT;

--End by Suresh


--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_WRI' , 'Finish', '');


  EXECUTE IMMEDIATE 'truncate table AIA_RPT_PRD_POLICY_WRI_TEMP';

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;

--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_WRI' , 'AIA_RPT_PRD_POLICY_WRI_TEMP'  , 'Processing', 'insert into AIA_RPT_PRD_POLICY_WRI_TEMP');


  insert /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_WRI_TEMP
    select BUNAME,
           DISTRICT_CODE,
           SUM(COMMISSION_AMT) as SUMCA,
           sum(FYP) as SUMFYP
      from AIA_RPT_PRD_POLICY_WRI
     WHERE PERIODSEQ = V_PERIODSEQ
     GROUP BY BUNAME, DISTRICT_CODE;
  commit;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_WRI' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;

--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_WRI' , 'AIA_RPT_PRD_POLICY_WRI'  , 'Processing', 'update AIA_RPT_PRD_POLICY_WRI');



----ADD BY JEFF FOR SUM C_API_AMT D_API_AMT C_SSCP_AMT D_SSCP_AMT
UPDATE /*+   */ AIA_RPT_PRD_POLICY_WRI P1
     SET (D_API_AMT) = NVL((SELECT API_AMT
                                    FROM AIA_RPT_PRD_POLICY_WRI_TEMP P2
                                   WHERE P1.DISTRICT_CODE = P2.DISTRICT_CODE
                                     AND P1.BUNAME = P2.BUNAME),
                                  0),
         (D_SSCP_AMT) = NVL((SELECT SSCP_AMT
                         FROM AIA_RPT_PRD_POLICY_WRI_TEMP P2
                        WHERE P1.DISTRICT_CODE = P2.DISTRICT_CODE
                          AND P1.BUNAME = P2.BUNAME),
                       0)
   where p1.PERIODSEQ = V_PERIODSEQ;

  commit;
  update AIA_RPT_PRD_POLICY_WRI p1
     set (C_API_AMT, C_SSCP_AMT) =
         (select sum(API_AMT), sum(SSCP_AMT)
            from AIA_RPT_PRD_POLICY_WRI p2
           where P2.PERIODSEQ = P1.PERIODSEQ
             AND P1.BUNAME = P2.BUNAME)
   where p1.PERIODSEQ = V_PERIODSEQ;

  commit;


----END ADD BY JEFF

  UPDATE /*+   */ AIA_RPT_PRD_POLICY_WRI P1
     SET (D_COMMISSION_AMT) = NVL((SELECT COMMISSION_AMT
                                    FROM AIA_RPT_PRD_POLICY_WRI_TEMP P2
                                   WHERE P1.DISTRICT_CODE = P2.DISTRICT_CODE
                                     AND P1.BUNAME = P2.BUNAME),
                                  0),
         (D_FYP) = NVL((SELECT FYP
                         FROM AIA_RPT_PRD_POLICY_WRI_TEMP P2
                        WHERE P1.DISTRICT_CODE = P2.DISTRICT_CODE
                          AND P1.BUNAME = P2.BUNAME),
                       0)
   where p1.PERIODSEQ = V_PERIODSEQ;

  commit;

---------log

  update AIA_RPT_PRD_POLICY_WRI p1
     set (C_COMMISSION_AMT, C_FYP) =
         (select sum(COMMISSION_AMT), sum(FYP)
            from AIA_RPT_PRD_POLICY_WRI p2
           where P2.PERIODSEQ = P1.PERIODSEQ
             AND P1.BUNAME = P2.BUNAME)
   where p1.PERIODSEQ = V_PERIODSEQ;

  commit;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_WRI' , 'Finish', '');


  EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_POLICY_PARAM_WRI DROP STORAGE' ;

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;

--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_WRI' , 'AIA_RPT_PRD_POLICY_PARAM_WRI'  , 'Processing', 'insert into AIA_RPT_PRD_POLICY_PARAM_WRI');



  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_WRI
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     RT.DISTRICT_CODE || ' - ' || RT.DM_NAME DISTRICTCODE,
                     RT.UNIT_CODE || ' - ' || RT.AGENCY UNITCODE,
                     RT.AGENT_CODE || ' - ' || RT.NAME AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_WRI rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);
  COMMIT;

  --added
  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_WRI
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     RT.DISTRICT_CODE || ' - ' || RT.DM_NAME DISTRICTCODE,
                     ' ALL' UNITCODE,
                     RT.AGENT_CODE || ' - ' || RT.NAME AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_WRI rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);
  COMMIT;

  --ADDED
  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_WRI
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     RT.DISTRICT_CODE || ' - ' || RT.DM_NAME DISTRICTCODE,
                     RT.UNIT_CODE || ' - ' || RT.AGENCY UNITCODE,
                     ' ALL' AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_WRI rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);
  COMMIT;

  --ADDED
  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_WRI
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     RT.DISTRICT_CODE || ' - ' || RT.DM_NAME DISTRICTCODE,
                     ' ALL' UNITCODE,
                     ' ALL' AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_WRI rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);
  COMMIT;

  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_WRI
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     ' ALL' DISTRICTCODE,
                     RT.UNIT_CODE || ' - ' || RT.AGENCY UNITCODE,
                     RT.AGENT_CODE || ' - ' || RT.NAME AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_WRI rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);
  COMMIT;

  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_WRI
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     ' ALL' DISTRICTCODE,
                     ' ALL' UNITCODE,
                     RT.AGENT_CODE || ' - ' || RT.NAME AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_WRI rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);
  COMMIT;
  --ADDED
  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_WRI
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     ' ALL' DISTRICTCODE,
                     RT.UNIT_CODE || ' - ' || RT.AGENCY UNITCODE,
                     ' ALL' AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_WRI rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);
  COMMIT;

  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_WRI
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     ' ALL' DISTRICTCODE,
                     ' ALL' UNITCODE,
                     ' ALL' AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_WRI rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);

 COMMIT;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_WRI' , 'Finish', '');

 ----exception

  begin
        DBMS_STATS.GATHER_TABLE_STATS (
            OWNNAME => '"AIASEXT"' ,
              TABNAME => '"AIA_RPT_PRD_POLICY_WRI"' ,
              ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE
            --  ,METHOD_OPT=>'FOR ALL INDEXED COLUMNS SIZE AUTO'
           );
            end;
END;


PROCEDURE REP_RUN_PRD_POLICY_COMM as

  V_PROCNAME           VARCHAR2(256);
  v_eot                date := to_date('01/01/2200', 'DD/MM/YYYY');
  V_CALENDARNAME       VARCHAR2(256);
  V_CALENDARSEQ        INTEGER;
  V_PROCESSINGUNITSEQ  INTEGER;
  V_PROCESSINGUNITNAME VARCHAR2(256);
  V_PERIODSTARTDATE    DATE;
  V_PERIODENDDATE      DATE;
  V_PERIODNAME         VARCHAR2(256);
  v_BUSINESSUNITMAP1   INTEGER;
  V_BUSINESSUNITMAP2   INTEGER;
  V_SYSDATE        DATE;
  V_Partition_Name varchar2(250);
  V_PERIODTYPESEQ  NUMBER;

BEGIN

  V_PROCNAME := 'PROC_RPT_PRD_POLICY_COMM';
  V_SYSDATE  := SYSDATE;


  SELECT P.STARTDATE, P.ENDDATE, C.DESCRIPTION, P.NAME, P.CALENDARSEQ
    INTO V_PERIODSTARTDATE,
         V_PERIODENDDATE,
         V_CALENDARNAME,
         V_PERIODNAME,
         V_CALENDARSEQ
    FROM CS_PERIOD P
   INNER JOIN CS_CALENDAR C
      ON P.CALENDARSEQ = C.CALENDARSEQ
   WHERE PERIODSEQ = V_PERIODSEQ;
  SELECT PERIODTYPESEQ
    INTO V_PERIODTYPESEQ
    FROM CS_PERIODTYPE
   WHERE NAME = 'month';

---------log

  SELECT PROCESSINGUNITSEQ, NAME
    INTO V_PROCESSINGUNITSEQ, V_PROCESSINGUNITNAME
    FROM CS_PROCESSINGUNIT
   WHERE NAME = 'AGY_PU';

  SELECT MASK
    INTO v_BUSINESSUNITMAP1
    FROM CS_BUSINESSUNIT
   WHERE NAME IN ('SGPAFA');
  SELECT MASK
    INTO v_BUSINESSUNITMAP2
    FROM CS_BUSINESSUNIT
   WHERE NAME IN ('BRUAGY');

  V_Partition_Name := 'P_' || replace(V_PeriodName, ' ', '_');
  Begin
    Execute Immediate 'Alter Table AIA_RPT_PRD_POLICY_COMM Truncate Partition '  ||
                      V_Partition_Name;
  Exception
    when others then
      null;
  End;
  Begin
    Execute Immediate 'Alter Table AIA_RPT_PRD_POLICY_COMM Add Partition '  ||
                      V_Partition_Name || ' Values('''  || V_PeriodName ||
                      ''') ' ;
  Exception
    when others then
      NULL;
  End;

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;

--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM' , 'AIA_RPT_PRD_POLICY_COMM 1'  , 'Processing', 'insert into AIA_RPT_PRD_POLICY_COMM');


  INSERT
  /*+ Append  */
  INTO AIA_RPT_PRD_POLICY_COMM
    (PROCESSINGUNITSEQ, --PUKEY,
     PUNAME,
     BUNAME,
     BUMAP,
     CALENDARSEQ,
     CALNAME,
     PERIODSEQ,--PERIODKEY,
     PERIODNAME,
     POSITIONSEQ, --Pakey,
     MANAGERSEQ,
     POSITIONNAME,
     DISTRICT_CODE,
     DM_NAME,
     DIST_LEADER_CODE,
     DIST_LEADER_NAME,
     DIST_LEAER_TITLE,
     DIST_LEADER_CLASS,
     UNIT_CODE,
     AGENCY,
     UNIT_LEADER_CODE,
     UNIT_LEADER_NAME,
     UNIT_LEAER_TITLE,
     UNIT_LEADER_CLASS,
     DISOLVED_DATE,
     Agent_code,
     NAME,
     POLICYNO,
     ---add by jeff for 2 columns
     PLAN_DESCRIPTION,
     RISK_COMMENCEMENT_DATE,
     INCEPTION_DATE,
     LOB,
     POLICYYEAR,
     TYPE_COMMISSION,
     ---add by jeff for 2 columns
     API_AMT,
     SSCP_AMT,
     COMMISSION_AMT,
     FYP,
     CASECOUNT
          ,FYC
  ,TOPUP_FYC
  ,RYC
  ,PREMIUM
  ,CASE_COUNT )
    (SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
            V_PROCESSINGUNITNAME PUNAME,
            decode(pad.BUSINESSUNITMAP,
                   V_BUSINESSUNITMAP1,
                   'SGPAFA',
                   V_BUSINESSUNITMAP2,
                   'BRUAGY') BUNAME,
            pad.BUSINESSUNITMAP BUMAP,
            V_CALENDARSEQ CALENDARSEQ,
            V_CALENDARNAME CALENDARNAME,
            V_PERIODSEQ PERIODSEQ,
            V_PERIODNAME periodname,
            PAD.POSITIONSEQ POSITIONSEQ,
            PAD.MANAGERSEQ,
            PAD.NAME,
            SUBSTR(DISTRICT.NAME, 4) DISTRICT_CODE,
            district.firstname || ' ' || district.lastname DM_NAme,
            district.POS_GA2 DIST_LEADER_CODE,
            district.POS_GA7 DIST_LEADER_NAME,
            district.POS_GA11 DIST_LEAER_TITLE,
            DISTRICT.POS_GA4 DIST_LEADER_CLASS,
            TMP.CD_GA13 UNIT_CODE,
            agy.firstname || ' ' || agy.lastname AGENCY,
            agy.POS_GA2 UNIT_LEADER_CODE,
            agy.POS_GA7 UNIT_LEADER_NAME,
            DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
            agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
            (CASE
              WHEN AGY.POSITIONTITLE IN
                  ('ORGANISATION','UNIT')  THEN
               AGY.AGY_TERMINATIONDATE
            END) DISSOLVED_DATE,
            SUBSTR(PAD.NAME, 4) AGT_CODE,
            (PAD.firstname || PAD.LASTNAME) NAME,
            TMP.CD_GA6 POLICYNO,
            ---add by jeff for 2 columns
            TMP.TRANS_GA3,
            TMP.CD_GD2,
            CASE
              --WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
              WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL','GI') AND   --version 9 add GI product
                   TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
               TO_CHAR(TMP.TRANS_GD6, 'MM/DD/YYYY')
            END INCEPTION_DATE,
            TMP.CD_GA2 LOB,
            TMP.CD_GN1 POLICYYEAR,
            CASE
              WHEN TMP.TRANS_GA17 = 'O' AND
                   --TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
                   TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL','GI') AND  --version 9 add GI product
                   TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
               'OWN'
              WHEN TMP.TRANS_GA17 = 'RNO' OR
                   TMP.TRANS_GA17 = 'RO' AND
                   TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
               'ASSIGN'
            END TYPE_COMMISSION,
            -----add by jeff for 2 columns
            CASE
              WHEN
                   TMP.CD_CREDITTYPE = 'API' and TMP.CD_GN1 = 1
                    THEN TMP.CD_VALUE
              ELSE
               0
            END API_AMT,
            CASE
              WHEN
                   TMP.CD_CREDITTYPE = 'SSCP' and TMP.CD_GN1 = 2
                    THEN TMP.CD_VALUE
              ELSE
               0
            END SSCP_AMT,
            CASE
              --WHEN (TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
              WHEN (TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL','GI') AND    --version 9 add GI product
                   TMP.CD_CREDITTYPE IN
                   ('FYC', 'API', 'SSCP', 'RYC', 'APB')) THEN
               TMP.CD_VALUE
              ELSE
               0
            END COMMISSION_AMT,
            CASE
              --WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
              WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL','GI') AND   --version 9 add GI product
                   TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') AND
                   TMP.TRANS_GN3 <> 0 THEN
               ----version 3 begin
               --TMP.TRANS_GN6
               CASE WHEN pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP2 ---IF 'BRUAGY' THEN fyp*rate. if 'SGPAFA' then fyp
                 THEN
                    TMP.TRANS_GN6*nvl(TMP.ASSIGN_GN1,0)
                 ELSE
                    TMP.TRANS_GN6
               ---END
                 END
              ELSE
               0
            END FYP,
            CASE
              WHEN TMP.TRANS_DIM_PRODUCTNAME IN
                   --('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
                   ('LF', 'HS', 'PA', 'PL', 'CS', 'VL','GI') AND    --version 9 add GI product
                   TMP.TRANS_DIM_EVENTTYPE = 'FYP' THEN
               TMP.TRANS_GN5
              ELSE
               0
            END CASECOUNT,
            CASE WHEN TMP.TRANS_DIM_EVENTTYPE ='FYC'
            THEN TMP.CD_VALUE
            ELSE
            0
            END FYC,
           CASE WHEN TMP.TRANS_DIM_EVENTTYPE='FYC' and TMP.CD_GA4='PAYT'
            THEN TMP.CD_VALUE
            ELSE
            0
            END TOPUP_FYC,
            CASE WHEN TMP.TRANS_DIM_EVENTTYPE='RYC'
            THEN TMP.CD_VALUE
            ELSE
            0
            END RYC,
              TMP.TRANS_GN6,
            TMP.TRANS_GN5
       FROM AIA_RPT_PRD_TEMP_PADIM  pad,
            AIA_RPT_PRD_TEMP_PADIM  agy,
            AIA_RPT_PRD_TEMP_PADIM  agy_ldr,
            AIA_RPT_PRD_TEMP_PADIM  DISTRICT,
            AIA_RPT_PRD_TEMP_VALUES TMP
      WHERE
--       TMP.NEW_PIB_IND=0 and
--mod start by drs 20160901
        pad.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
        and agy.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
        and agy_ldr.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
        AND DISTRICT.BUSINESSUNITMAP IN (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
/*        AND pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
        and agy.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
        and agy_ldr.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
        AND DISTRICT.BUSINESSUNITMAP = V_BUSINESSUNITMAP1 */
        AND PAD.POSITIONTITLE IN ('AD','DIR','ED','FC','SD')
        --AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD','AM','FSD','FSM')
--mod end by drs 20160901
        AND PAD.POS_GA4 NOT IN ('45', '48')
        AND TMP.CD_CREDITTYPE IN
            ('FYP', 'RYC', 'FYC', 'API', 'SSCP', 'APB', 'Case_Count')
        AND TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ
        AND agy.POS_GA1 = TMP.CD_GA13
        AND AGY.POSITIONTITLE IN ('ORGANISATION','UNIT')     --mod by drs 20160901
        --AND AGY.POSITIONTITLE IN ('AGENCY', 'DISTRICT')                             --mod by drs 20160901
        AND ((agy_ldr.NAME = 'SGT' || agy.POS_GA2 AND agy.NAME LIKE 'SGY%' and pad.businessunitmap = V_BUSINESSUNITMAP1)) ---version 4 fix SGP agency in BRU BU
        AND DISTRICT.POS_GA3 = AGY.POS_GA3
        AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
        AND district.positiontitle IN ('ORGANISATION')                 --mod by drs 20160901
        --AND district.positiontitle = 'DISTRICT'                                 --mod by drs 20160901
        --AND TMP.CD_GA2 IN ('LF', 'PA', 'HS', 'CS', 'PL', 'CL', 'VL','PAYT')
        AND TMP.CD_GA2 IN ('LF', 'PA', 'HS', 'CS', 'PL', 'CL', 'VL','PAYT','GI')    --version 9 add GI product
        --fix in version 2 for solving DISSOLVED agency or terminated agent more 7 years
        AND
        (pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
        OR( nvl(AGY.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84)
        AND nvl(PAD.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84))
        )
        --end fix
        );
  COMMIT;

----job end log
--pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM' , 'Finish', '');
----get report log sequence number
--SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--
----job start log
--pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM' , 'AIA_RPT_PRD_POLICY_COMM 2'  , 'Processing', 'insert into AIA_RPT_PRD_POLICY_COMM');
--
--
--  INSERT
--  /*+ Append  */
--  INTO AIA_RPT_PRD_POLICY_COMM
--    (PROCESSINGUNITSEQ, --PUKEY,
--     PUNAME,
--     BUNAME,
--     BUMAP,
--     CALENDARSEQ,
--     CALNAME,
--     PERIODSEQ,--PERIODKEY
--     PERIODNAME,
--     POSITIONSEQ, --Pakey,
--     MANAGERSEQ,
--     POSITIONNAME,
--     DISTRICT_CODE,
--     DM_NAME,
--     DIST_LEADER_CODE,
--     DIST_LEADER_NAME,
--     DIST_LEAER_TITLE,
--     DIST_LEADER_CLASS,
--     UNIT_CODE,
--     AGENCY,
--     UNIT_LEADER_CODE,
--     UNIT_LEADER_NAME,
--     UNIT_LEAER_TITLE,
--     UNIT_LEADER_CLASS,
--     DISOLVED_DATE,
--     Agent_code,
--     NAME,
--     POLICYNO,
--     ---add by jeff for 2 columns
--     PLAN_DESCRIPTION,
--     RISK_COMMENCEMENT_DATE,
--     INCEPTION_DATE,
--     LOB,
--     POLICYYEAR,
--     TYPE_COMMISSION,
--     ---add by jeff for 2 columns
--     API_AMT,
--     SSCP_AMT,
--     COMMISSION_AMT,
--     FYP,
--     CASECOUNT     ,FYC
--  ,TOPUP_FYC
--  ,RYC )
--    (SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
--            V_PROCESSINGUNITNAME PUNAME,
--            decode(pad.BUSINESSUNITMAP,
--                   V_BUSINESSUNITMAP1,
--                   'SGPAFA',
--                   V_BUSINESSUNITMAP2,
--                   'BRUAGY') BUNAME,
--            pad.BUSINESSUNITMAP BUMAP,
--            V_CALENDARSEQ CALENDARSEQ,
--            V_CALENDARNAME CALENDARNAME,
--            V_PERIODSEQ PERIODSEQ,
--            V_PERIODNAME periodname,
--            PAD.POSITIONSEQ POSITIONSEQ,
--            PAD.MANAGERSEQ,
--            -- PAD.POSITIONSEQ POSITIONSEQ,
--            PAD.NAME,
--            SUBSTR(DISTRICT.NAME, 4) DISTRICT_CODE,
--            district.firstname || ' ' || district.lastname DM_NAme,
--            district.POS_GA2 DIST_LEADER_CODE,
--            district.POS_GA7 DIST_LEADER_NAME,
--            district.POS_GA11 DIST_LEAER_TITLE,
--            DISTRICT.POS_GA4 DIST_LEADER_CLASS,
--            TMP.TRANS_GA11 UNIT_CODE,
--            --        PAD.PositionGenericAttribute1 UNIT_CODE,
--            agy.firstname || ' ' || agy.lastname AGENCY,
--            agy.POS_GA2 UNIT_LEADER_CODE,
--            agy.POS_GA7 UNIT_LEADER_NAME,
--            DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
--            agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
--            (CASE
--              WHEN AGY.POSITIONTITLE IN
--                   ('ORGANISATION','UNIT')   THEN
--               AGY.AGY_TERMINATIONDATE
--            END) DISSOLVED_DATE,
--            SUBSTR(PAD.NAME, 4) AGT_CODE,
--            (PAD.firstname || PAD.LASTNAME) NAME,
--            TMP.CD_GA6 POLICYNO,
--            ---add by jeff for 2 columns
--            TMP.TRANS_GA3,
--            TMP.CD_GD2,
--            CASE
--              WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
--                   TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
--               TO_CHAR(TMP.TRANS_GD6, 'MM/DD/YYYY')
--            END INCEPTION_DATE,
--            TMP.CD_GA2 LOB,
--            TMP.CD_GN1 POLICYYEAR,
--            CASE
--              WHEN TMP.TRANS_GA17 = 'O' AND
--                   TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
--                   TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
--               'OWN'
--              WHEN TMP.TRANS_GA17 = 'RNO' OR
--                   TMP.TRANS_GA17 = 'RO' AND
--                   TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
--               'ASSIGN'
--            END TYPE_COMMISSION,
--            -----add by jeff for 2 columns
--            0 API_AMT,
--            0 SSCP_AMT,
--            0 COMMISSION_AMT,
--            0 FYP,
--            CASE
--              WHEN TMP.TRANS_DIM_PRODUCTNAME IN
--                   ('LF', 'HS', 'PA', 'PL', 'CS', 'VL')
--                  --  AND TMP.CF_VALUE <> 0
--                   AND TMP.CD_CREDITTYPE = 'Case_Count' THEN
--               TMP.TRANS_GN5
--              ELSE
--               0
--            END CASECOUNT,
--             CASE WHEN TMP.CD_CREDITTYPE='FYC'
--            THEN TMP.CD_VALUE
--            ELSE
--            0
--            END FYC,
--           CASE WHEN TMP.CD_CREDITTYPE='FYC' and TMP.CD_GA2='PAYT'
--            THEN TMP.CD_VALUE
--            ELSE
--            0
--            END TOPUP_FYC,
--            CASE WHEN TMP.CD_CREDITTYPE='RYC'
--            THEN TMP.CD_VALUE
--            ELSE
--            0
--            END RYC
--       FROM AIA_RPT_PRD_TEMP_PADIM  pad,
--            AIA_RPT_PRD_TEMP_PADIM  agy,
--            AIA_RPT_PRD_TEMP_PADIM  agy_ldr,
--            AIA_RPT_PRD_TEMP_PADIM  DISTRICT,
--            AIA_RPT_PRD_TEMP_VALUES TMP
--      WHERE TMP.NEW_PIB_IND=0
----mod start by drs 20160901
--        AND pad.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        and agy.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        and agy_ldr.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        AND DISTRICT.BUSINESSUNITMAP IN (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--/*        AND pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        and agy.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        and agy_ldr.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        AND DISTRICT.BUSINESSUNITMAP = V_BUSINESSUNITMAP1  */
--        AND PAD.POSITIONTITLE IN  ('AD','DIR','ED','FC','SD')
--        --AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD', 'AM','FSD','FSM')
----mod end by drs 20160901
--        AND PAD.POS_GA4 NOT IN ('45', '48')
--        AND TMP.CD_CREDITTYPE IN
--            ('FYP', 'RYC', 'FYC', 'API', 'SSCP', 'APB', 'Case_Count')
--        AND TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ
--        AND agy.POS_GA1 = TMP.TRANS_GA11
--        AND AGY.POSITIONTITLE IN ('ORGANISATION','UNIT')      --mod by drs 20160901
--        --AND AGY.POSITIONTITLE IN ('AGENCY', 'DISTRICT')                              --mod by drs 20160901
--        AND ((agy_ldr.NAME = 'SGT' || agy.POS_GA2 AND agy.NAME LIKE 'SGY%' and pad.businessunitmap = V_BUSINESSUNITMAP1) )
--        AND DISTRICT.POS_GA3 = AGY.POS_GA3
--        AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
--        AND district.positiontitle IN ('ORGANISATION')                  --mod by drs 20160901
--        --AND district.positiontitle = 'DISTRICT'                                  --mod by drs 20160901
--        AND TMP.CD_GA2 IN ('LF', 'PA', 'HS', 'CS', 'PL', 'CL', 'VL','PAYT')
--        --fix in version 2 for solving DISSOLVED agency or terminated agent more 7 years
--        AND
--        (pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        OR( nvl(AGY.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84)
--        AND nvl(PAD.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84))
--        )
--        --end fix
--        );
--  /* End of above Insert to capture CASE COUNT Of Brunei Records which doesn't CreditType = 'FYP' coming from 'Case_Count */
--  COMMIT;
--
--
----job end log
--pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM' , 'Finish', '');
--
----get report log sequence number
--SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--
----job start log
--pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM' , 'AIA_RPT_PRD_POLICY_COMM 3'  , 'Processing', 'insert into AIA_RPT_PRD_POLICY_COMM');
--
--  INSERT
--  /*+ Append  */
--  INTO AIA_RPT_PRD_POLICY_COMM
--    (PROCESSINGUNITSEQ, --PUKEY,
--     PUNAME,
--     BUNAME,
--     BUMAP,
--     CALENDARSEQ,
--     CALNAME,
--     PERIODSEQ,--PERIODKEY
--     PERIODNAME,
--     POSITIONSEQ, --POSITIONSEQ,
--     MANAGERSEQ,
--     POSITIONNAME,
--     DISTRICT_CODE,
--     DM_NAME,
--     DIST_LEADER_CODE,
--     DIST_LEADER_NAME,
--     DIST_LEAER_TITLE,
--     DIST_LEADER_CLASS,
--     UNIT_CODE,
--     AGENCY,
--     UNIT_LEADER_CODE,
--     UNIT_LEADER_NAME,
--     UNIT_LEAER_TITLE,
--     UNIT_LEADER_CLASS,
--     DISOLVED_DATE,
--     Agent_code,
--     NAME,
--     POLICYNO,
--     ---add by jeff for 2 columns
--     PLAN_DESCRIPTION,
--     RISK_COMMENCEMENT_DATE,
--     INCEPTION_DATE,
--     LOB,
--     POLICYYEAR,
--     TYPE_COMMISSION,
--     ---add by jeff for 2 columns
--     API_AMT,
--     SSCP_AMT,
--     COMMISSION_AMT,
--     FYP,
--     CASECOUNT     ,FYC
--  ,TOPUP_FYC
--  ,RYC )
--    (SELECT /*+ leading(agy,agy_ldr,TMP,PAD) use_hash(agy,agy_ldr,TMP,pad)*/
--            V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
--            V_PROCESSINGUNITNAME PUNAME,
--            decode(pad.BUSINESSUNITMAP,
--                   V_BUSINESSUNITMAP1,
--                   'SGPAFA',
--                   V_BUSINESSUNITMAP2,
--                   'BRUAGY') BUNAME,
--            pad.BUSINESSUNITMAP BUMAP,
--            V_CALENDARSEQ CALENDARSEQ,
--            V_CALENDARNAME CALENDARNAME,
--            V_PERIODSEQ PERIODSEQ,
--            V_PERIODNAME periodname,
--            PAD_AGT.POSITIONSEQ POSITIONSEQ,
--            PAD_AGT.MANAGERSEQ,
--            pad_agt.NAME,
--            SUBSTR(DISTRICT.NAME, 4) DISTRICT_CODE,
--            -- district.POSITIONGENERICATTRIBUTE3 DISTRICT_CODE,
--            district.firstname || ' ' || district.lastname DM_NAme,
--            district.POS_GA2 DIST_LEADER_CODE,
--            district.POS_GA7 DIST_LEADER_NAME,
--            district.POS_GA11 DIST_LEAER_TITLE,
--            DISTRICT.POS_GA4 DIST_LEADER_CLASS,
--            --        SUBSTR(PAD.NAME,4) UNIT_CODE,
--            TMP.CD_GA13 UNIT_CODE,
--            agy.firstname || ' ' || agy.lastname AGENCY,
--            agy_ldr.POS_GA2 UNIT_LEADER_CODE,
--            agy_ldr.POS_GA7 UNIT_LEADER_NAME,
--            DECODE(agy_ldr.POS_GA11,
--                   'FSC_NON_PROCESS',
--                   'FSC',
--                   agy_ldr.POS_GA11) UNIT_LEAER_TITLE,
--            agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
--            (CASE
--              WHEN agy.POSITIONTITLE IN
--                  ('ORGANISATION','UNIT')   THEN
--               agy.AGY_TERMINATIONDATE
--            END) DISSOLVED_DATE,
--            SUBSTR(pad_agt.NAME, 4) AGT_CODE,
--            (pad_agt.firstname || PAD_AGT.LASTNAME) NAME,
--            TMP.CD_GA6 POLICYNO,
--            ---add by jeff for 2 columns
--            TMP.TRANS_GA3,
--            TMP.CD_GD2,
--            CASE
--              WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
--                   TMP.CD_CREDITTYPE IN ('ORYC', 'RYC') THEN
--               TO_CHAR(TMP.TRANS_GD6, 'MM/DD/YYYY')
--            END INCEPTION_DATE,
--            TMP.CD_GA2 LOB,
--            TMP.CD_GN1 POLICYYEAR,
--            CASE
--              WHEN TMP.TRANS_GA17 = 'O' AND
--                   TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
--                   TMP.CD_CREDITTYPE IN ('RYC') THEN
--               'UNASSIGN'
--              WHEN TMP.TRANS_GA17 = 'RNO' OR
--                   TMP.TRANS_GA17 = 'RO' AND TMP.CD_CREDITTYPE IN ('RYC') THEN
--               'UNASSIGN'
--              WHEN TMP.TRANS_GA14 = '48' AND
--                   TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
--                   TMP.CD_CREDITTYPE = 'ORYC' then
--               'UNASSIGN'
--            END TYPE_COMMISSION,
--            -----add by jeff for 2 columns
--            0 API_AMT,
--            0 SSCP_AMT,
--            (CASE
--              WHEN (TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
--                   TMP.CD_CREDITTYPE IN ('ORYC', 'RYC')) THEN
--               TMP.CD_VALUE
--              ELSE
--               0
--            END) AS COMMISSION_AMT,
--            (CASE
--              WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
--                   TMP.CD_CREDITTYPE IN ('ORYC', 'RYC') AND
--                   TMP.TRANS_GN3 <> 0 THEN
--               ----version 3 begin
--               --TMP.TRANS_GN6
--               CASE WHEN pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP2 ---IF 'BRUAGY' THEN fyp*rate. if 'SGPAFA' then fyp
--                 THEN
--                    TMP.TRANS_GN6*nvl(TMP.ASSIGN_GN1,0)
--                 ELSE
--                    TMP.TRANS_GN6
--               ---END
--                 END
--              ELSE
--               0
--            END) AS FYP,
--            (CASE
--              WHEN TMP.TRANS_DIM_PRODUCTNAME IN ('LF', 'HS', 'PA', 'VL') AND
--                   TMP.TRANS_DIM_EVENTTYPE = 'FYP' THEN
--               TMP.TRANS_GN5
--              ELSE
--               0
--            END) AS CASECOUNT,
--            CASE WHEN TMP.CD_CREDITTYPE='FYC'
--            THEN TMP.CD_VALUE
--            ELSE
--            0
--            END FYC,
--           CASE WHEN TMP.CD_CREDITTYPE='FYC' and TMP.CD_GA2='PAYT'
--            THEN TMP.CD_VALUE
--            ELSE
--            0
--            END TOPUP_FYC,
--            CASE WHEN TMP.CD_CREDITTYPE='RYC'
--            THEN TMP.CD_VALUE
--            ELSE
--            0
--            END RYC
--       FROM AIA_RPT_PRD_TEMP_PADIM  PAD,
--            AIA_RPT_PRD_TEMP_PADIM  pad_agt,
--            AIA_RPT_PRD_TEMP_PADIM  agy,
--            AIA_RPT_PRD_TEMP_PADIM  AGY_LDR,
--            AIA_RPT_PRD_TEMP_PADIM  DISTRICT,
--            AIA_RPT_PRD_TEMP_VALUES TMP
--      WHERE TMP.NEW_PIB_IND=0
----mod start by drs 20160901
--        AND pad.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        and agy.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        and agy_ldr.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        AND DISTRICT.BUSINESSUNITMAP IN (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--/*        AND pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        and agy.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        and agy_ldr.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        AND DISTRICT.BUSINESSUNITMAP = V_BUSINESSUNITMAP1 */
--        AND PAD.POSITIONTITLE IN  ('AD','DIR','ED','FC','SD')
--        --AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD', 'AM','FSD','FSM')
----mod end by drs 20160901
--        AND PAD.POS_GA4 NOT IN ('45') --,'48')
--        AND TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ
--        AND TMP.CD_CREDITTYPE IN ('ORYC', 'RYC', 'Case_Count')
--        AND AGY.POS_GA1 = TMP.CD_GA13
--        AND SUBSTR(PAD.NAME, 4) = TMP.TRANS_GA11
--        AND SUBSTR(PAD_AGT.NAME, 4) = TMP.TRANS_GA10 and SUBSTR(PAD_AGT.NAME,3,1)='T'
--        AND AGY.POSITIONTITLE IN ('ORGANISATION','UNIT')             --mod by drs 20160901
--        --AND AGY.POSITIONTITLE IN ('AGENCY', 'DISTRICT')                                     --mod by drs 20160901
--        AND ((agy_ldr.NAME = 'SGT' || agy.POS_GA2 AND agy.NAME LIKE 'SGY%' and pad.businessunitmap = V_BUSINESSUNITMAP1) )
--        AND DISTRICT.POS_GA3 = AGY.POS_GA3
--        AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
--        AND district.positiontitle IN ('ORGANISATION')                    --mod by drs 20160901
--        --AND district.positiontitle = 'DISTRICT'                                    --mod by drs 20160901
--        AND TMP.CD_GA2 IN ('LF', 'PA', 'HS', 'VL','PAYT')
--        AND TMP.BUSINESSUNITMAP = PAD_AGT.BUSINESSUNITMAP
--        AND TMP.BUSINESSUNITMAP = AGY.BUSINESSUNITMAP
--        AND TMP.BUSINESSUNITMAP = AGY_LDR.BUSINESSUNITMAP
--        AND TMP.BUSINESSUNITMAP = DISTRICT.BUSINESSUNITMAP
--        --fix in version 2 for solving DISSOLVED agency or terminated agent more 7 years
--        AND
--        (pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        OR( nvl(AGY.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84)
--        AND nvl(PAD.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84))
--        )
--        --end fix
--        );

--  COMMIT;



--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM' , 'Finish', '');

  EXECUTE IMMEDIATE 'truncate table AIA_RPT_PRD_POLICY_COMM_TEMP';

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;

--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM' , 'AIA_RPT_PRD_POLICY_COMM_TEMP'  , 'Processing', 'insert into AIA_RPT_PRD_POLICY_COMM_TEMP');


  insert /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_COMM_TEMP
    select BUNAME,
           DISTRICT_CODE,
           SUM(COMMISSION_AMT) as SUMCA,
           sum(FYP) as SUMFYP
      from AIA_RPT_PRD_POLICY_COMM
     WHERE PERIODSEQ = V_PERIODSEQ
     GROUP BY BUNAME, DISTRICT_CODE;
  commit;

--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM' , 'Finish', '');

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;

--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM' , 'AIA_RPT_PRD_POLICY_COMM'  , 'Processing', 'update AIA_RPT_PRD_POLICY_COMM');



----ADD BY JEFF FOR SUM C_API_AMT D_API_AMT C_SSCP_AMT D_SSCP_AMT
UPDATE /*+   */ AIA_RPT_PRD_POLICY_COMM P1
     SET (D_API_AMT) = NVL((SELECT API_AMT
                                    FROM AIA_RPT_PRD_POLICY_COMM_TEMP P2
                                   WHERE P1.DISTRICT_CODE = P2.DISTRICT_CODE
                                     AND P1.BUNAME = P2.BUNAME),
                                  0),
         (D_SSCP_AMT) = NVL((SELECT SSCP_AMT
                         FROM AIA_RPT_PRD_POLICY_COMM_TEMP P2
                        WHERE P1.DISTRICT_CODE = P2.DISTRICT_CODE
                          AND P1.BUNAME = P2.BUNAME),
                       0)
   where p1.PERIODSEQ = V_PERIODSEQ;

  commit;
  update AIA_RPT_PRD_POLICY_COMM p1
     set (C_API_AMT, C_SSCP_AMT) =
         (select sum(API_AMT), sum(SSCP_AMT)
            from AIA_RPT_PRD_POLICY_COMM p2
           where P2.PERIODSEQ = P1.PERIODSEQ
             AND P1.BUNAME = P2.BUNAME)
   where p1.PERIODSEQ = V_PERIODSEQ;

  commit;


----END ADD BY JEFF

  UPDATE /*+   */ AIA_RPT_PRD_POLICY_COMM P1
     SET (D_COMMISSION_AMT) = NVL((SELECT COMMISSION_AMT
                                    FROM AIA_RPT_PRD_POLICY_COMM_TEMP P2
                                   WHERE P1.DISTRICT_CODE = P2.DISTRICT_CODE
                                     AND P1.BUNAME = P2.BUNAME),
                                  0),
         (D_FYP) = NVL((SELECT FYP
                         FROM AIA_RPT_PRD_POLICY_COMM_TEMP P2
                        WHERE P1.DISTRICT_CODE = P2.DISTRICT_CODE
                          AND P1.BUNAME = P2.BUNAME),
                       0)
   where p1.PERIODSEQ = V_PERIODSEQ;

  commit;

---------log

  update AIA_RPT_PRD_POLICY_COMM p1
     set (C_COMMISSION_AMT, C_FYP) =
         (select sum(COMMISSION_AMT), sum(FYP)
            from AIA_RPT_PRD_POLICY_COMM p2
           where P2.PERIODSEQ = P1.PERIODSEQ
             AND P1.BUNAME = P2.BUNAME)
   where p1.PERIODSEQ = V_PERIODSEQ;
   commit;

--version 8 add on bridging flag

 execute immediate 'truncate table AIA_RPT_PRD_ONBRIDGING';

 insert into AIA_RPT_PRD_ONBRIDGING
   SELECT PAR.USERID              AS AGENT_CODE, --contains SGT as prefix
          GAPAR.GENERICATTRIBUTE4 AS AGY_AGENT,
          GAPAR.GENERICATTRIBUTE5 AS FA_AGENT,
          GAPAR.GENERICATTRIBUTE6 AS FA_AGENCY,
          GAPAR.GENERICBOOLEAN2   AS ON_BRIDGING_FLAG
     FROM CS_PARTICIPANT PAR, CS_GAPARTICIPANT GAPAR
    WHERE PAR.TENANTID = 'AIAS'
      AND PAR.REMOVEDATE = DT_REMOVEDATE
      AND PAR.EFFECTIVESTARTDATE <= V_PERIODENDDATE-1
      AND PAR.EFFECTIVEENDDATE > V_PERIODENDDATE-1
      AND GAPAR.TENANTID = 'AIAS'
      AND GAPAR.REMOVEDATE = DT_REMOVEDATE
      AND GAPAR.EFFECTIVESTARTDATE <= V_PERIODENDDATE-1
      AND GAPAR.EFFECTIVEENDDATE > V_PERIODENDDATE-1
      AND GAPAR.PAGENUMBER = 0
      AND PAR.PAYEESEQ = GAPAR.PAYEESEQ
      AND PAR.USERID LIKE '%T%';

      COMMIT;


 merge into AIA_RPT_PRD_POLICY_COMM s
 using (SELECT AGENT_CODE,
               ON_BRIDGING_FLAG
          FROM AIA_RPT_PRD_ONBRIDGING) t
 on ('SGT' || s.agent_code = t.AGENT_CODE and s.PERIODNAME = V_PERIODNAME)
 when matched then
   update set s.on_bridging_flag = case
 when t.ON_BRIDGING_FLAG = 1 then 1 else 0 end;

  commit;

--version 8 end



--job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM' , 'Finish', '');

--Revised by Win Tan for version 7
  --EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_POLICY_PARAM_COMM DROP STORAGE';
  DELETE FROM AIA_RPT_PRD_POLICY_PARAM_COMM where PERIODSEQ = V_PERIODSEQ;
  commit;

--get report log sequence number
SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;

--job start log
pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM' , 'AIA_RPT_PRD_POLICY_PARAM_COMM'  , 'Processing', 'insert into AIA_RPT_PRD_POLICY_PARAM_COMM');


  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     RT.DISTRICT_CODE || ' - ' || RT.DM_NAME DISTRICTCODE,
                     RT.UNIT_CODE || ' - ' || RT.AGENCY UNITCODE,
                     RT.AGENT_CODE || ' - ' || RT.NAME AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        --Added by Win Tan for version 7
        --begin
        and pd.periodseq = V_PERIODSEQ
        and rt.periodname = pd.name
        --end
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);
  COMMIT;

  --added
  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     RT.DISTRICT_CODE || ' - ' || RT.DM_NAME DISTRICTCODE,
                     ' ALL' UNITCODE,
                     RT.AGENT_CODE || ' - ' || RT.NAME AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        --Added by Win Tan for version 7
        --begin
        and pd.periodseq = V_PERIODSEQ
        and rt.periodname = pd.name
        --end
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);
  COMMIT;

  --ADDED
  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     RT.DISTRICT_CODE || ' - ' || RT.DM_NAME DISTRICTCODE,
                     RT.UNIT_CODE || ' - ' || RT.AGENCY UNITCODE,
                     ' ALL' AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        --Added by Win Tan for version 7
        --begin
        and pd.periodseq = V_PERIODSEQ
        and rt.periodname = pd.name
        --end
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);
  COMMIT;

  --ADDED
  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     RT.DISTRICT_CODE || ' - ' || RT.DM_NAME DISTRICTCODE,
                     ' ALL' UNITCODE,
                     ' ALL' AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        --Added by Win Tan for version 7
        --begin
        and pd.periodseq = V_PERIODSEQ
        and rt.periodname = pd.name
        --end
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);
  COMMIT;

  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     ' ALL' DISTRICTCODE,
                     RT.UNIT_CODE || ' - ' || RT.AGENCY UNITCODE,
                     RT.AGENT_CODE || ' - ' || RT.NAME AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        --Added by Win Tan for version 7
        --begin
        and pd.periodseq = V_PERIODSEQ
        and rt.periodname = pd.name
        --end
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);
  COMMIT;

  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     ' ALL' DISTRICTCODE,
                     ' ALL' UNITCODE,
                     RT.AGENT_CODE || ' - ' || RT.NAME AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        --Added by Win Tan for version 7
        --begin
        and pd.periodseq = V_PERIODSEQ
        and rt.periodname = pd.name
        --end
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);
  COMMIT;
  --ADDED
  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     ' ALL' DISTRICTCODE,
                     RT.UNIT_CODE || ' - ' || RT.AGENCY UNITCODE,
                     ' ALL' AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        --Added by Win Tan for version 7
        --begin
        and pd.periodseq = V_PERIODSEQ
        and rt.periodname = pd.name
        --end
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);
  COMMIT;

  INSERT /*+ APPEND */
  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
    (SELECT DISTINCT PD.PERIODSEQ,
                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
                     SUBSTR('0' ||
                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
                            -2) MONTHNO,
                     ' ALL' DISTRICTCODE,
                     ' ALL' UNITCODE,
                     ' ALL' AGENTCODE,
                     RT.BUNAME BUNAME
       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
      where rt.PERIODSEQ = pd.periodseq
        --Added by Win Tan for version 7
        --begin
        and pd.periodseq = V_PERIODSEQ
        and rt.periodname = pd.name
        --end
        and pd.removedate = V_EOT
        and pd.calendarseq = V_CALENDARSEQ);

 COMMIT;

 --job end log
pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM' , 'Finish', '');
 ----exception

  begin
        DBMS_STATS.GATHER_TABLE_STATS (
            OWNNAME => '"AIASEXT"' ,
              TABNAME => '"AIA_RPT_PRD_POLICY_COMM"' ,
              ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE
            --  ,METHOD_OPT=>'FOR ALL INDEXED COLUMNS SIZE AUTO'
           );
            end;
END;

--PROCEDURE REP_RUN_PRD_POLICY_COMM_BRU as
--
--  V_PROCNAME           VARCHAR2(256);
--  v_eot                date := to_date('01/01/2200', 'DD/MM/YYYY');
--  V_CALENDARNAME       VARCHAR2(256);
--  V_CALENDARSEQ        INTEGER;
--  V_PROCESSINGUNITSEQ  INTEGER;
--  V_PROCESSINGUNITNAME VARCHAR2(256);
--  V_PERIODSTARTDATE    DATE;
--  V_PERIODENDDATE      DATE;
--  V_PERIODNAME         VARCHAR2(256);
--  v_BUSINESSUNITMAP1   INTEGER;
--  V_BUSINESSUNITMAP2   INTEGER;
--  V_SYSDATE        DATE;
--  V_Partition_Name varchar2(250);
--  V_PERIODTYPESEQ  NUMBER;
--
--BEGIN
--
--  V_PROCNAME := 'PROC_RPT_PRD_POLICY_COMM';
--  V_SYSDATE  := SYSDATE;
--
--
--  SELECT P.STARTDATE, P.ENDDATE, C.DESCRIPTION, P.NAME, P.CALENDARSEQ
--    INTO V_PERIODSTARTDATE,
--         V_PERIODENDDATE,
--         V_CALENDARNAME,
--         V_PERIODNAME,
--         V_CALENDARSEQ
--    FROM CS_PERIOD P
--   INNER JOIN CS_CALENDAR C
--      ON P.CALENDARSEQ = C.CALENDARSEQ
--   WHERE PERIODSEQ = V_PERIODSEQ;
--  SELECT PERIODTYPESEQ
--    INTO V_PERIODTYPESEQ
--    FROM CS_PERIODTYPE
--   WHERE NAME = 'month';
--
-----------log
--
--  SELECT PROCESSINGUNITSEQ, NAME
--    INTO V_PROCESSINGUNITSEQ, V_PROCESSINGUNITNAME
--    FROM CS_PROCESSINGUNIT
--   WHERE NAME = 'AGY_PU';
--
--  SELECT MASK
--    INTO v_BUSINESSUNITMAP1
--    FROM CS_BUSINESSUNIT
--   WHERE NAME IN ('SGPAFA');
--  SELECT MASK
--    INTO v_BUSINESSUNITMAP2
--    FROM CS_BUSINESSUNIT
--   WHERE NAME IN ('BRUAGY');
--
--  V_Partition_Name := 'P_' || replace(V_PeriodName, ' ', '_');
--  ----VERSION 4 BRU ONLY BEGIN
--  /*Begin
--    Execute Immediate 'Alter Table AIA_RPT_PRD_POLICY_COMM Truncate Partition ' ||
--                      V_Partition_Name;
--  Exception
--    when others then
--      null;
--  End;
--  Begin
--    Execute Immediate 'Alter Table AIA_RPT_PRD_POLICY_COMM Add Partition ' ||
--                      V_Partition_Name || ' Values(''' || V_PeriodName ||
--                      ''') ';
--  Exception
--    when others then
--      NULL;
--  End;*/
--  DELETE FROM AIA_RPT_PRD_POLICY_COMM WHERE BUNAME = 'BRUAGY' AND PERIODSEQ=V_PERIODSEQ;
--  COMMIT;
--  ---END;
--
----get report log sequence number
--SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--
----job start log
--pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM_BRU' , 'AIA_RPT_PRD_POLICY_COMM 1'  , 'Processing', 'insert into AIA_RPT_PRD_POLICY_COMM');
--
--
--  INSERT
--  /*+ Append  */
--  INTO AIA_RPT_PRD_POLICY_COMM
--    (PROCESSINGUNITSEQ, --PUKEY,
--     PUNAME,
--     BUNAME,
--     BUMAP,
--     CALENDARSEQ,
--     CALNAME,
--     PERIODSEQ,--PERIODKEY,
--     PERIODNAME,
--     POSITIONSEQ, --Pakey,
--     MANAGERSEQ,
--     POSITIONNAME,
--     DISTRICT_CODE,
--     DM_NAME,
--     DIST_LEADER_CODE,
--     DIST_LEADER_NAME,
--     DIST_LEAER_TITLE,
--     DIST_LEADER_CLASS,
--     UNIT_CODE,
--     AGENCY,
--     UNIT_LEADER_CODE,
--     UNIT_LEADER_NAME,
--     UNIT_LEAER_TITLE,
--     UNIT_LEADER_CLASS,
--     DISOLVED_DATE,
--     Agent_code,
--     NAME,
--     POLICYNO,
--     ---add by jeff for 2 columns
--     PLAN_DESCRIPTION,
--     RISK_COMMENCEMENT_DATE,
--     INCEPTION_DATE,
--     LOB,
--     POLICYYEAR,
--     TYPE_COMMISSION,
--     ---add by jeff for 2 columns
--     API_AMT,
--     SSCP_AMT,
--     COMMISSION_AMT,
--     FYP,
--     CASECOUNT)
--    (SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
--            V_PROCESSINGUNITNAME PUNAME,
--            decode(pad.BUSINESSUNITMAP,
--                   V_BUSINESSUNITMAP1,
--                   'SGPAFA',
--                   V_BUSINESSUNITMAP2,
--                   'BRUAGY') BUNAME,
--            pad.BUSINESSUNITMAP BUMAP,
--            V_CALENDARSEQ CALENDARSEQ,
--            V_CALENDARNAME CALENDARNAME,
--            V_PERIODSEQ PERIODSEQ,
--            V_PERIODNAME periodname,
--            PAD.POSITIONSEQ POSITIONSEQ,
--            PAD.MANAGERSEQ,
--            PAD.NAME,
--            SUBSTR(DISTRICT.NAME, 4) DISTRICT_CODE,
--            district.firstname || ' ' || district.lastname DM_NAme,
--            district.POS_GA2 DIST_LEADER_CODE,
--            district.POS_GA7 DIST_LEADER_NAME,
--            district.POS_GA11 DIST_LEAER_TITLE,
--            DISTRICT.POS_GA4 DIST_LEADER_CLASS,
--            TMP.CD_GA13 UNIT_CODE,
--            agy.firstname || ' ' || agy.lastname AGENCY,
--            agy.POS_GA2 UNIT_LEADER_CODE,
--            agy.POS_GA7 UNIT_LEADER_NAME,
--            DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
--            agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
--            (CASE
--              WHEN AGY.POSITIONTITLE IN
--                   ('AGENCY', 'DISTRICT', 'BR_AGENCY', 'BR_DISTRICT') THEN
--               AGY.AGY_TERMINATIONDATE
--            END) DISSOLVED_DATE,
--            SUBSTR(PAD.NAME, 4) AGT_CODE,
--            (PAD.firstname || PAD.LASTNAME) NAME,
--            TMP.CD_GA6 POLICYNO,
--            ---add by jeff for 2 columns
--            TMP.TRANS_GA3,
--            TMP.CD_GD2,
--            CASE
--              WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
--                   TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
--               TO_CHAR(TMP.TRANS_GD6, 'MM/DD/YYYY')
--            END INCEPTION_DATE,
--            TMP.CD_GA2 LOB,
--            TMP.CD_GN1 POLICYYEAR,
--            CASE
--              WHEN TMP.TRANS_GA17 = 'O' AND
--                   TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
--                   TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
--               'OWN'
--              WHEN TMP.TRANS_GA17 = 'RNO' OR
--                   TMP.TRANS_GA17 = 'RO' AND
--                   TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
--               'ASSIGN'
--            END TYPE_COMMISSION,
--            -----add by jeff for 2 columns
--            CASE
--              WHEN
--                   TMP.CD_CREDITTYPE = 'API' and TMP.CD_GN1 = 1
--                    THEN TMP.CD_VALUE
--              ELSE
--               0
--            END API_AMT,
--            CASE
--              WHEN
--                   TMP.CD_CREDITTYPE = 'SSCP' and TMP.CD_GN1 = 2
--                    THEN TMP.CD_VALUE
--              ELSE
--               0
--            END SSCP_AMT,
--            CASE
--              WHEN (TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
--                   TMP.CD_CREDITTYPE IN
--                   ('FYC', 'API', 'SSCP', 'RYC', 'APB')) THEN
--               TMP.CD_VALUE
--              ELSE
--               0
--            END COMMISSION_AMT,
--            CASE
--              WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
--                   TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') AND
--                   TMP.TRANS_GN3 <> 0 THEN
--               ----version 3 begin
--               --TMP.TRANS_GN6
--               CASE WHEN pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP2 ---IF 'BRUAGY' THEN fyp*rate. if 'SGPAFA' then fyp
--                 THEN
--                    TMP.TRANS_GN6*nvl(TMP.ASSIGN_GN1,0)
--                 ELSE
--                    TMP.TRANS_GN6
--               ---END
--                 END
--              ELSE
--               0
--            END FYP,
--            CASE
--              WHEN TMP.TRANS_DIM_PRODUCTNAME IN
--                   ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
--                   TMP.TRANS_DIM_EVENTTYPE = 'FYP' THEN
--               TMP.TRANS_GN5
--              ELSE
--               0
--            END CASECOUNT
--       FROM AIA_RPT_PRD_TEMP_PADIM  pad,
--            AIA_RPT_PRD_TEMP_PADIM  agy,
--            AIA_RPT_PRD_TEMP_PADIM  agy_ldr,
--            AIA_RPT_PRD_TEMP_PADIM  DISTRICT,
--            AIA_RPT_PRD_TEMP_VALUES TMP
--      WHERE TMP.NEW_PIB_IND=0
----mod start by drs 20160901
--        AND pad.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        and agy.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        and agy_ldr.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        AND DISTRICT.BUSINESSUNITMAP IN (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--/*        AND pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        and agy.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        and agy_ldr.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        AND DISTRICT.BUSINESSUNITMAP = V_BUSINESSUNITMAP1 */
--        AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD','AM','FSD','FSM','BR_DM','BR_FSC','BR_UM')
--        --AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD','AM','FSD','FSM')
----mod end by drs 20160901
--        AND PAD.POS_GA4 NOT IN ('45', '48')
--        AND TMP.CD_CREDITTYPE IN
--            ('FYP', 'RYC', 'FYC', 'API', 'SSCP', 'APB', 'Case_Count')
--        AND TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ
--        AND agy.POS_GA1 = TMP.CD_GA13
--        AND AGY.POSITIONTITLE IN ('AGENCY', 'DISTRICT', 'BR_AGENCY', 'BR_DISTRICT')   --mod by drs 20160901
--        --AND AGY.POSITIONTITLE IN ('AGENCY', 'DISTRICT')                             --mod by drs 20160901
--        ----VERSION 4 BRU ONLY BEGIN
--        /*AND ((agy_ldr.NAME = 'SGT' || agy.POS_GA2 AND agy.NAME LIKE 'SGY%') OR
--            (agy_ldr.NAME = 'BRT' || agy.POS_GA2 AND agy.NAME LIKE 'BRY%'))*/
--        AND (agy_ldr.NAME = 'BRT' || agy.POS_GA2 AND agy.NAME LIKE 'BRY%' and pad.businessunitmap = V_BUSINESSUNITMAP2)---version 4 fix SGP agency in BRU BU
--        --END
--        AND DISTRICT.POS_GA3 = AGY.POS_GA3
--        AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
--        AND district.positiontitle IN ('DISTRICT', 'BR_DISTRICT')                 --mod by drs 20160901
--        --AND district.positiontitle = 'DISTRICT'                                 --mod by drs 20160901
--        AND TMP.CD_GA2 IN ('LF', 'PA', 'HS', 'CS', 'PL', 'CL', 'VL')
--        --fix in version 2 for solving DISSOLVED agency or terminated agent more 7 years
--        AND
--        ----VERSION 4 BRU ONLY BEGIN
--        /*(pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        OR( nvl(AGY.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84)
--        AND nvl(PAD.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84))
--        )*/
--        (pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP2
--        AND( nvl(AGY.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84)
--        AND nvl(PAD.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84))
--        )
--        ---END
--        --end fix
--        );
--  COMMIT;
--
----job end log
--pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM_BRU' , 'Finish', '');
----get report log sequence number
--SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--
----job start log
--pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM_BRU' , 'AIA_RPT_PRD_POLICY_COMM 2'  , 'Processing', 'insert into AIA_RPT_PRD_POLICY_COMM');
--
--
--  INSERT
--  /*+ Append  */
--  INTO AIA_RPT_PRD_POLICY_COMM
--    (PROCESSINGUNITSEQ, --PUKEY,
--     PUNAME,
--     BUNAME,
--     BUMAP,
--     CALENDARSEQ,
--     CALNAME,
--     PERIODSEQ,--PERIODKEY
--     PERIODNAME,
--     POSITIONSEQ, --Pakey,
--     MANAGERSEQ,
--     POSITIONNAME,
--     DISTRICT_CODE,
--     DM_NAME,
--     DIST_LEADER_CODE,
--     DIST_LEADER_NAME,
--     DIST_LEAER_TITLE,
--     DIST_LEADER_CLASS,
--     UNIT_CODE,
--     AGENCY,
--     UNIT_LEADER_CODE,
--     UNIT_LEADER_NAME,
--     UNIT_LEAER_TITLE,
--     UNIT_LEADER_CLASS,
--     DISOLVED_DATE,
--     Agent_code,
--     NAME,
--     POLICYNO,
--     ---add by jeff for 2 columns
--     PLAN_DESCRIPTION,
--     RISK_COMMENCEMENT_DATE,
--     INCEPTION_DATE,
--     LOB,
--     POLICYYEAR,
--     TYPE_COMMISSION,
--     ---add by jeff for 2 columns
--     API_AMT,
--     SSCP_AMT,
--     COMMISSION_AMT,
--     FYP,
--     CASECOUNT)
--    (SELECT V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
--            V_PROCESSINGUNITNAME PUNAME,
--            decode(pad.BUSINESSUNITMAP,
--                   V_BUSINESSUNITMAP1,
--                   'SGPAFA',
--                   V_BUSINESSUNITMAP2,
--                   'BRUAGY') BUNAME,
--            pad.BUSINESSUNITMAP BUMAP,
--            V_CALENDARSEQ CALENDARSEQ,
--            V_CALENDARNAME CALENDARNAME,
--            V_PERIODSEQ PERIODSEQ,
--            V_PERIODNAME periodname,
--            PAD.POSITIONSEQ POSITIONSEQ,
--            PAD.MANAGERSEQ,
--            -- PAD.POSITIONSEQ POSITIONSEQ,
--            PAD.NAME,
--            SUBSTR(DISTRICT.NAME, 4) DISTRICT_CODE,
--            district.firstname || ' ' || district.lastname DM_NAme,
--            district.POS_GA2 DIST_LEADER_CODE,
--            district.POS_GA7 DIST_LEADER_NAME,
--            district.POS_GA11 DIST_LEAER_TITLE,
--            DISTRICT.POS_GA4 DIST_LEADER_CLASS,
--            TMP.TRANS_GA11 UNIT_CODE,
--            --        PAD.PositionGenericAttribute1 UNIT_CODE,
--            agy.firstname || ' ' || agy.lastname AGENCY,
--            agy.POS_GA2 UNIT_LEADER_CODE,
--            agy.POS_GA7 UNIT_LEADER_NAME,
--            DECODE(agy.POS_GA11, 'FSC_NON_PROCESS', 'FSC', agy.POS_GA11) UNIT_LEAER_TITLE,
--            agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
--            (CASE
--              WHEN AGY.POSITIONTITLE IN
--                   ('AGENCY', 'DISTRICT', 'BR_AGENCY', 'BR_DISTRICT') THEN
--               AGY.AGY_TERMINATIONDATE
--            END) DISSOLVED_DATE,
--            SUBSTR(PAD.NAME, 4) AGT_CODE,
--            (PAD.firstname || PAD.LASTNAME) NAME,
--            TMP.CD_GA6 POLICYNO,
--            ---add by jeff for 2 columns
--            TMP.TRANS_GA3,
--            TMP.CD_GD2,
--            CASE
--              WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
--                   TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
--               TO_CHAR(TMP.TRANS_GD6, 'MM/DD/YYYY')
--            END INCEPTION_DATE,
--            TMP.CD_GA2 LOB,
--            TMP.CD_GN1 POLICYYEAR,
--            CASE
--              WHEN TMP.TRANS_GA17 = 'O' AND
--                   TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'PL', 'CS', 'VL') AND
--                   TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
--               'OWN'
--              WHEN TMP.TRANS_GA17 = 'RNO' OR
--                   TMP.TRANS_GA17 = 'RO' AND
--                   TMP.CD_CREDITTYPE IN ('FYC', 'API', 'SSCP', 'RYC', 'APB') THEN
--               'ASSIGN'
--            END TYPE_COMMISSION,
--            -----add by jeff for 2 columns
--            0 API_AMT,
--            0 SSCP_AMT,
--            0 COMMISSION_AMT,
--            0 FYP,
--            CASE
--              WHEN TMP.TRANS_DIM_PRODUCTNAME IN
--                   ('LF', 'HS', 'PA', 'PL', 'CS', 'VL')
--                  --  AND TMP.CF_VALUE <> 0
--                   AND TMP.CD_CREDITTYPE = 'Case_Count' THEN
--               TMP.TRANS_GN5
--              ELSE
--               0
--            END CASECOUNT
--       FROM AIA_RPT_PRD_TEMP_PADIM  pad,
--            AIA_RPT_PRD_TEMP_PADIM  agy,
--            AIA_RPT_PRD_TEMP_PADIM  agy_ldr,
--            AIA_RPT_PRD_TEMP_PADIM  DISTRICT,
--            AIA_RPT_PRD_TEMP_VALUES TMP
--      WHERE TMP.NEW_PIB_IND=0
----mod start by drs 20160901
--        AND pad.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        and agy.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        and agy_ldr.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        AND DISTRICT.BUSINESSUNITMAP IN (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--/*        AND pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        and agy.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        and agy_ldr.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        AND DISTRICT.BUSINESSUNITMAP = V_BUSINESSUNITMAP1  */
--        AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD', 'AM','FSD','FSM','BR_DM','BR_FSC','BR_UM')
--        --AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD', 'AM','FSD','FSM')
----mod end by drs 20160901
--        AND PAD.POS_GA4 NOT IN ('45', '48')
--        AND TMP.CD_CREDITTYPE IN
--            ('FYP', 'RYC', 'FYC', 'API', 'SSCP', 'APB', 'Case_Count')
--        AND TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ
--        AND agy.POS_GA1 = TMP.TRANS_GA11
--        AND AGY.POSITIONTITLE IN ('AGENCY', 'DISTRICT', 'BR_AGENCY', 'BR_DISTRICT')    --mod by drs 20160901
--        --AND AGY.POSITIONTITLE IN ('AGENCY', 'DISTRICT')                              --mod by drs 20160901
--        ----VERSION 4 BRU ONLY BEGIN
--        /*AND ((agy_ldr.NAME = 'SGT' || agy.POS_GA2 AND agy.NAME LIKE 'SGY%') OR
--            (agy_ldr.NAME = 'BRT' || agy.POS_GA2 AND agy.NAME LIKE 'BRY%'))*/
--        AND (agy_ldr.NAME = 'BRT' || agy.POS_GA2 AND agy.NAME LIKE 'BRY%' and pad.businessunitmap = V_BUSINESSUNITMAP2)---version 4 fix SGP agency in BRU BU
--        --END
--        AND DISTRICT.POS_GA3 = AGY.POS_GA3
--        AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
--        AND district.positiontitle IN ('DISTRICT', 'BR_DISTRICT')                  --mod by drs 20160901
--        --AND district.positiontitle = 'DISTRICT'                                  --mod by drs 20160901
--        AND TMP.CD_GA2 IN ('LF', 'PA', 'HS', 'CS', 'PL', 'CL', 'VL')
--        --fix in version 2 for solving DISSOLVED agency or terminated agent more 7 years
--        AND
--        ----VERSION 4 BRU ONLY BEGIN
--        /*(pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        OR( nvl(AGY.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84)
--        AND nvl(PAD.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84))
--        )*/
--        (pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP2
--        AND( nvl(AGY.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84)
--        AND nvl(PAD.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84))
--        )
--        ---END
--        --end fix
--        );
--  /* End of above Insert to capture CASE COUNT Of Brunei Records which doesn't CreditType = 'FYP' coming from 'Case_Count */
--  COMMIT;
--
--
----job end log
--pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM_BRU' , 'Finish', '');
--
----get report log sequence number
--SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--
----job start log
--pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM_BRU' , 'AIA_RPT_PRD_POLICY_COMM 3'  , 'Processing', 'insert into AIA_RPT_PRD_POLICY_COMM');
--
--  INSERT
--  /*+ Append  */
--  INTO AIA_RPT_PRD_POLICY_COMM
--    (PROCESSINGUNITSEQ, --PUKEY,
--     PUNAME,
--     BUNAME,
--     BUMAP,
--     CALENDARSEQ,
--     CALNAME,
--     PERIODSEQ,--PERIODKEY
--     PERIODNAME,
--     POSITIONSEQ, --POSITIONSEQ,
--     MANAGERSEQ,
--     POSITIONNAME,
--     DISTRICT_CODE,
--     DM_NAME,
--     DIST_LEADER_CODE,
--     DIST_LEADER_NAME,
--     DIST_LEAER_TITLE,
--     DIST_LEADER_CLASS,
--     UNIT_CODE,
--     AGENCY,
--     UNIT_LEADER_CODE,
--     UNIT_LEADER_NAME,
--     UNIT_LEAER_TITLE,
--     UNIT_LEADER_CLASS,
--     DISOLVED_DATE,
--     Agent_code,
--     NAME,
--     POLICYNO,
--     ---add by jeff for 2 columns
--     PLAN_DESCRIPTION,
--     RISK_COMMENCEMENT_DATE,
--     INCEPTION_DATE,
--     LOB,
--     POLICYYEAR,
--     TYPE_COMMISSION,
--     ---add by jeff for 2 columns
--     API_AMT,
--     SSCP_AMT,
--     COMMISSION_AMT,
--     FYP,
--     CASECOUNT)
--    (SELECT /*+ leading(agy,agy_ldr,TMP,PAD) use_hash(agy,agy_ldr,TMP,pad)*/
--            V_PROCESSINGUNITSEQ PROCESSINGUNITSEQ,
--            V_PROCESSINGUNITNAME PUNAME,
--            decode(pad.BUSINESSUNITMAP,
--                   V_BUSINESSUNITMAP1,
--                   'SGPAFA',
--                   V_BUSINESSUNITMAP2,
--                   'BRUAGY') BUNAME,
--            pad.BUSINESSUNITMAP BUMAP,
--            V_CALENDARSEQ CALENDARSEQ,
--            V_CALENDARNAME CALENDARNAME,
--            V_PERIODSEQ PERIODSEQ,
--            V_PERIODNAME periodname,
--            PAD_AGT.POSITIONSEQ POSITIONSEQ,
--            PAD_AGT.MANAGERSEQ,
--            pad_agt.NAME,
--            SUBSTR(DISTRICT.NAME, 4) DISTRICT_CODE,
--            -- district.POSITIONGENERICATTRIBUTE3 DISTRICT_CODE,
--            district.firstname || ' ' || district.lastname DM_NAme,
--            district.POS_GA2 DIST_LEADER_CODE,
--            district.POS_GA7 DIST_LEADER_NAME,
--            district.POS_GA11 DIST_LEAER_TITLE,
--            DISTRICT.POS_GA4 DIST_LEADER_CLASS,
--            --        SUBSTR(PAD.NAME,4) UNIT_CODE,
--            TMP.CD_GA13 UNIT_CODE,
--            agy.firstname || ' ' || agy.lastname AGENCY,
--            agy_ldr.POS_GA2 UNIT_LEADER_CODE,
--            agy_ldr.POS_GA7 UNIT_LEADER_NAME,
--            DECODE(agy_ldr.POS_GA11,
--                   'FSC_NON_PROCESS',
--                   'FSC',
--                   agy_ldr.POS_GA11) UNIT_LEAER_TITLE,
--            agy_ldr.POS_GA4 UNIT_LEADER_CLASS,
--            (CASE
--              WHEN agy.POSITIONTITLE IN
--                   ('AGENCY', 'DISTRICT', 'BR_AGENCY', 'BR_DISTRICT') THEN
--               agy.AGY_TERMINATIONDATE
--            END) DISSOLVED_DATE,
--            SUBSTR(pad_agt.NAME, 4) AGT_CODE,
--            (pad_agt.firstname || PAD_AGT.LASTNAME) NAME,
--            TMP.CD_GA6 POLICYNO,
--            ---add by jeff for 2 columns
--            TMP.TRANS_GA3,
--            TMP.CD_GD2,
--            CASE
--              WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
--                   TMP.CD_CREDITTYPE IN ('ORYC', 'RYC') THEN
--               TO_CHAR(TMP.TRANS_GD6, 'MM/DD/YYYY')
--            END INCEPTION_DATE,
--            TMP.CD_GA2 LOB,
--            TMP.CD_GN1 POLICYYEAR,
--            CASE
--              WHEN TMP.TRANS_GA17 = 'O' AND
--                   TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
--                   TMP.CD_CREDITTYPE IN ('RYC') THEN
--               'UNASSIGN'
--              WHEN TMP.TRANS_GA17 = 'RNO' OR
--                   TMP.TRANS_GA17 = 'RO' AND TMP.CD_CREDITTYPE IN ('RYC') THEN
--               'UNASSIGN'
--              WHEN TMP.TRANS_GA14 = '48' AND
--                   TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
--                   TMP.CD_CREDITTYPE = 'ORYC' then
--               'UNASSIGN'
--            END TYPE_COMMISSION,
--            -----add by jeff for 2 columns
--            0 API_AMT,
--            0 SSCP_AMT,
--            (CASE
--              WHEN (TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
--                   TMP.CD_CREDITTYPE IN ('ORYC', 'RYC')) THEN
--               TMP.CD_VALUE
--              ELSE
--               0
--            END) AS COMMISSION_AMT,
--            (CASE
--              WHEN TMP.CD_GA2 IN ('LF', 'HS', 'PA', 'VL') AND
--                   TMP.CD_CREDITTYPE IN ('ORYC', 'RYC') AND
--                   TMP.TRANS_GN3 <> 0 THEN
--               ----version 3 begin
--               --TMP.TRANS_GN6
--               CASE WHEN pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP2 ---IF 'BRUAGY' THEN fyp*rate. if 'SGPAFA' then fyp
--                 THEN
--                    TMP.TRANS_GN6*nvl(TMP.ASSIGN_GN1,0)
--                 ELSE
--                    TMP.TRANS_GN6
--               ---END
--                 END
--              ELSE
--               0
--            END) AS FYP,
--            (CASE
--              WHEN TMP.TRANS_DIM_PRODUCTNAME IN ('LF', 'HS', 'PA', 'VL') AND
--                   TMP.TRANS_DIM_EVENTTYPE = 'FYP' THEN
--               TMP.TRANS_GN5
--              ELSE
--               0
--            END) AS CASECOUNT
--       FROM AIA_RPT_PRD_TEMP_PADIM  PAD,
--            AIA_RPT_PRD_TEMP_PADIM  pad_agt,
--            AIA_RPT_PRD_TEMP_PADIM  agy,
--            AIA_RPT_PRD_TEMP_PADIM  AGY_LDR,
--            AIA_RPT_PRD_TEMP_PADIM  DISTRICT,
--            AIA_RPT_PRD_TEMP_VALUES TMP
--      WHERE TMP.NEW_PIB_IND=0
----mod start by drs 20160901
--        AND pad.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        and agy.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        and agy_ldr.BUSINESSUNITMAP in (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--        AND DISTRICT.BUSINESSUNITMAP IN (V_BUSINESSUNITMAP1, V_BUSINESSUNITMAP2)
--/*        AND pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        and agy.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        and agy_ldr.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        AND DISTRICT.BUSINESSUNITMAP = V_BUSINESSUNITMAP1 */
--        AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD', 'AM','FSD','FSM','BR_DM','BR_FSC','BR_UM')
--        --AND PAD.POSITIONTITLE IN ('FSC','FSC_NON_PROCESS','FSAD', 'AM','FSD','FSM')
----mod end by drs 20160901
--        AND PAD.POS_GA4 NOT IN ('45') --,'48')
--        AND TMP.CD_POSITIONSEQ = PAD.POSITIONSEQ
--        AND TMP.CD_CREDITTYPE IN ('ORYC', 'RYC', 'Case_Count')
--        AND AGY.POS_GA1 = TMP.CD_GA13
--        AND SUBSTR(PAD.NAME, 4) = TMP.TRANS_GA11
--        AND SUBSTR(PAD_AGT.NAME, 4) = TMP.TRANS_GA10 and SUBSTR(PAD_AGT.NAME,3,1)='T'
--        AND AGY.POSITIONTITLE IN ('AGENCY', 'DISTRICT', 'BR_AGENCY', 'BR_DISTRICT')           --mod by drs 20160901
--        --AND AGY.POSITIONTITLE IN ('AGENCY', 'DISTRICT')                                     --mod by drs 20160901
--        ----VERSION 4 BRU ONLY BEGIN
--        /*AND ((agy_ldr.NAME = 'SGT' || agy.POS_GA2 AND agy.NAME LIKE 'SGY%') OR
--            (agy_ldr.NAME = 'BRT' || agy.POS_GA2 AND agy.NAME LIKE 'BRY%'))*/
--        AND (agy_ldr.NAME = 'BRT' || agy.POS_GA2 AND agy.NAME LIKE 'BRY%' and pad.businessunitmap = V_BUSINESSUNITMAP2)---version 4 fix SGP agency in BRU BU
--        --END
--        AND DISTRICT.POS_GA3 = AGY.POS_GA3
--        AND district.NAME LIKE '%Y%' --UNCOMMETED BY RAVI ON 11/18
--        AND district.positiontitle IN ('DISTRICT', 'BR_DISTRICT')                    --mod by drs 20160901
--        --AND district.positiontitle = 'DISTRICT'                                    --mod by drs 20160901
--        AND TMP.CD_GA2 IN ('LF', 'PA', 'HS', 'VL')
--        AND TMP.BUSINESSUNITMAP = PAD_AGT.BUSINESSUNITMAP
--        AND TMP.BUSINESSUNITMAP = AGY.BUSINESSUNITMAP
--        AND TMP.BUSINESSUNITMAP = AGY_LDR.BUSINESSUNITMAP
--        AND TMP.BUSINESSUNITMAP = DISTRICT.BUSINESSUNITMAP
--        --fix in version 2 for solving DISSOLVED agency or terminated agent more 7 years
--        AND
--        ----VERSION 4 BRU ONLY BEGIN
--        /*(pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP1
--        OR( nvl(AGY.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84)
--        AND nvl(PAD.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84))
--        )*/
--        (pad.BUSINESSUNITMAP = V_BUSINESSUNITMAP2
--        AND( nvl(AGY.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84)
--        AND nvl(PAD.AGY_TERMINATIONDATE,v_eot) > add_months(V_PERIODSTARTDATE,-84))
--        )
--        ---END
--        --end fix
--        );
--
--  COMMIT;
--
--
--
----job end log
--pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM_BRU' , 'Finish', '');
--
--  EXECUTE IMMEDIATE 'truncate table AIA_RPT_PRD_POLICY_COMM_TEMP';
--
----get report log sequence number
--SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--
----job start log
--pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM_BRU' , 'AIA_RPT_PRD_POLICY_COMM_TEMP'  , 'Processing', 'insert into AIA_RPT_PRD_POLICY_COMM_TEMP');
--
--
--  insert /*+ APPEND */
--  INTO AIA_RPT_PRD_POLICY_COMM_TEMP
--    select BUNAME,
--           DISTRICT_CODE,
--           SUM(COMMISSION_AMT) as SUMCA,
--           sum(FYP) as SUMFYP
--      from AIA_RPT_PRD_POLICY_COMM
--     WHERE PERIODSEQ = V_PERIODSEQ
--     GROUP BY BUNAME, DISTRICT_CODE;
--  commit;
--
----job end log
--pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM_BRU' , 'Finish', '');
--
----get report log sequence number
--SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--
----job start log
--pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM_BRU' , 'AIA_RPT_PRD_POLICY_COMM'  , 'Processing', 'update AIA_RPT_PRD_POLICY_COMM');
--
--
--
------ADD BY JEFF FOR SUM C_API_AMT D_API_AMT C_SSCP_AMT D_SSCP_AMT
--UPDATE /*+ PARALLEL(p1,8) */ AIA_RPT_PRD_POLICY_COMM P1
--     SET (D_API_AMT) = NVL((SELECT API_AMT
--                                    FROM AIA_RPT_PRD_POLICY_COMM_TEMP P2
--                                   WHERE P1.DISTRICT_CODE = P2.DISTRICT_CODE
--                                     AND P1.BUNAME = P2.BUNAME),
--                                  0),
--         (D_SSCP_AMT) = NVL((SELECT SSCP_AMT
--                         FROM AIA_RPT_PRD_POLICY_COMM_TEMP P2
--                        WHERE P1.DISTRICT_CODE = P2.DISTRICT_CODE
--                          AND P1.BUNAME = P2.BUNAME),
--                       0)
--   where p1.PERIODSEQ = V_PERIODSEQ
--   AND BUNAME = 'BRUAGY' ---VERSION 4 FOR BRU ONLY
--   ;
--
--  commit;
--  update AIA_RPT_PRD_POLICY_COMM p1
--     set (C_API_AMT, C_SSCP_AMT) =
--         (select sum(API_AMT), sum(SSCP_AMT)
--            from AIA_RPT_PRD_POLICY_COMM p2
--           where P2.PERIODSEQ = P1.PERIODSEQ
--             AND P1.BUNAME = P2.BUNAME)
--   where p1.PERIODSEQ = V_PERIODSEQ
--   AND BUNAME = 'BRUAGY'---VERSION 4 FOR BRU ONLY
--   ;
--
--  commit;
--
--
------END ADD BY JEFF
--
--  UPDATE /*+ PARALLEL(p1,8) */ AIA_RPT_PRD_POLICY_COMM P1
--     SET (D_COMMISSION_AMT) = NVL((SELECT COMMISSION_AMT
--                                    FROM AIA_RPT_PRD_POLICY_COMM_TEMP P2
--                                   WHERE P1.DISTRICT_CODE = P2.DISTRICT_CODE
--                                     AND P1.BUNAME = P2.BUNAME),
--                                  0),
--         (D_FYP) = NVL((SELECT FYP
--                         FROM AIA_RPT_PRD_POLICY_COMM_TEMP P2
--                        WHERE P1.DISTRICT_CODE = P2.DISTRICT_CODE
--                          AND P1.BUNAME = P2.BUNAME),
--                       0)
--   where p1.PERIODSEQ = V_PERIODSEQ
--   AND BUNAME = 'BRUAGY'---VERSION 4 FOR BRU ONLY
--   ;
--
--  commit;
--
-----------log
--
--  update AIA_RPT_PRD_POLICY_COMM p1
--     set (C_COMMISSION_AMT, C_FYP) =
--         (select sum(COMMISSION_AMT), sum(FYP)
--            from AIA_RPT_PRD_POLICY_COMM p2
--           where P2.PERIODSEQ = P1.PERIODSEQ
--             AND P1.BUNAME = P2.BUNAME)
--   where p1.PERIODSEQ = V_PERIODSEQ
--   AND BUNAME = 'BRUAGY'---VERSION 4 FOR BRU ONLY
--   ;
--   commit;
--
--
----job end log
--pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM_BRU' , 'Finish', '');
--
--  EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_RPT_PRD_POLICY_PARAM_COMM DROP STORAGE';
--
----get report log sequence number
--SELECT pk_aia_rpt_comm_fn.fn_get_seq INTO V_Report_SEQ from dual;
--
----job start log
--pk_aia_rpt_comm_fn.sp_job_start_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM_BRU' , 'AIA_RPT_PRD_POLICY_PARAM_COMM'  , 'Processing', 'insert into AIA_RPT_PRD_POLICY_PARAM_COMM');
--
--
--  INSERT /*+ APPEND */
--  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
--    (SELECT DISTINCT PD.PERIODSEQ,
--                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
--                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
--                     SUBSTR('0' ||
--                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
--                            -2) MONTHNO,
--                     RT.DISTRICT_CODE || ' - ' || RT.DM_NAME DISTRICTCODE,
--                     RT.UNIT_CODE || ' - ' || RT.AGENCY UNITCODE,
--                     RT.AGENT_CODE || ' - ' || RT.NAME AGENTCODE,
--                     RT.BUNAME BUNAME
--       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
--      where rt.PERIODSEQ = pd.periodseq
--        and pd.removedate = V_EOT
--        and pd.calendarseq = V_CALENDARSEQ);
--  COMMIT;
--
--  --added
--  INSERT /*+ APPEND */
--  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
--    (SELECT DISTINCT PD.PERIODSEQ,
--                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
--                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
--                     SUBSTR('0' ||
--                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
--                            -2) MONTHNO,
--                     RT.DISTRICT_CODE || ' - ' || RT.DM_NAME DISTRICTCODE,
--                     ' ALL' UNITCODE,
--                     RT.AGENT_CODE || ' - ' || RT.NAME AGENTCODE,
--                     RT.BUNAME BUNAME
--       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
--      where rt.PERIODSEQ = pd.periodseq
--        and pd.removedate = V_EOT
--        and pd.calendarseq = V_CALENDARSEQ);
--  COMMIT;
--
--  --ADDED
--  INSERT /*+ APPEND */
--  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
--    (SELECT DISTINCT PD.PERIODSEQ,
--                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
--                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
--                     SUBSTR('0' ||
--                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
--                            -2) MONTHNO,
--                     RT.DISTRICT_CODE || ' - ' || RT.DM_NAME DISTRICTCODE,
--                     RT.UNIT_CODE || ' - ' || RT.AGENCY UNITCODE,
--                     ' ALL' AGENTCODE,
--                     RT.BUNAME BUNAME
--       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
--      where rt.PERIODSEQ = pd.periodseq
--        and pd.removedate = V_EOT
--        and pd.calendarseq = V_CALENDARSEQ);
--  COMMIT;
--
--  --ADDED
--  INSERT /*+ APPEND */
--  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
--    (SELECT DISTINCT PD.PERIODSEQ,
--                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
--                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
--                     SUBSTR('0' ||
--                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
--                            -2) MONTHNO,
--                     RT.DISTRICT_CODE || ' - ' || RT.DM_NAME DISTRICTCODE,
--                     ' ALL' UNITCODE,
--                     ' ALL' AGENTCODE,
--                     RT.BUNAME BUNAME
--       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
--      where rt.PERIODSEQ = pd.periodseq
--        and pd.removedate = V_EOT
--        and pd.calendarseq = V_CALENDARSEQ);
--  COMMIT;
--
--  INSERT /*+ APPEND */
--  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
--    (SELECT DISTINCT PD.PERIODSEQ,
--                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
--                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
--                     SUBSTR('0' ||
--                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
--                            -2) MONTHNO,
--                     ' ALL' DISTRICTCODE,
--                     RT.UNIT_CODE || ' - ' || RT.AGENCY UNITCODE,
--                     RT.AGENT_CODE || ' - ' || RT.NAME AGENTCODE,
--                     RT.BUNAME BUNAME
--       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
--      where rt.PERIODSEQ = pd.periodseq
--        and pd.removedate = V_EOT
--        and pd.calendarseq = V_CALENDARSEQ);
--  COMMIT;
--
--  INSERT /*+ APPEND */
--  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
--    (SELECT DISTINCT PD.PERIODSEQ,
--                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
--                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
--                     SUBSTR('0' ||
--                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
--                            -2) MONTHNO,
--                     ' ALL' DISTRICTCODE,
--                     ' ALL' UNITCODE,
--                     RT.AGENT_CODE || ' - ' || RT.NAME AGENTCODE,
--                     RT.BUNAME BUNAME
--       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
--      where rt.PERIODSEQ = pd.periodseq
--        and pd.removedate = V_EOT
--        and pd.calendarseq = V_CALENDARSEQ);
--  COMMIT;
--  --ADDED
--  INSERT /*+ APPEND */
--  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
--    (SELECT DISTINCT PD.PERIODSEQ,
--                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
--                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
--                     SUBSTR('0' ||
--                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
--                            -2) MONTHNO,
--                     ' ALL' DISTRICTCODE,
--                     RT.UNIT_CODE || ' - ' || RT.AGENCY UNITCODE,
--                     ' ALL' AGENTCODE,
--                     RT.BUNAME BUNAME
--       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
--      where rt.PERIODSEQ = pd.periodseq
--        and pd.removedate = V_EOT
--        and pd.calendarseq = V_CALENDARSEQ);
--  COMMIT;
--
--  INSERT /*+ APPEND */
--  INTO AIA_RPT_PRD_POLICY_PARAM_COMM
--    (SELECT DISTINCT PD.PERIODSEQ,
--                     TO_CHAR(EXTRACT(YEAR FROM pd.STARTDATE)) YEAR,
--                     SUBSTR(PD.NAME, 0, LENGTH(PD.NAME) - 5) MONTH,
--                     SUBSTR('0' ||
--                            MOD(EXTRACT(MONTH FROM pd.STARTDATE), 13),
--                            -2) MONTHNO,
--                     ' ALL' DISTRICTCODE,
--                     ' ALL' UNITCODE,
--                     ' ALL' AGENTCODE,
--                     RT.BUNAME BUNAME
--       from AIA_RPT_PRD_POLICY_COMM rt, cs_period pd
--      where rt.PERIODSEQ = pd.periodseq
--        and pd.removedate = V_EOT
--        and pd.calendarseq = V_CALENDARSEQ);
--
-- COMMIT;
--
-- --job end log
--pk_aia_rpt_comm_fn.sp_job_end_log(V_Report_SEQ , 'PRD_REPORT.REP_RUN_PRD_POLICY_COMM_BRU' , 'Finish', '');
-- ----exception
--
--  begin
--        DBMS_STATS.GATHER_TABLE_STATS (
--            OWNNAME => '"AIASEXT"',
--              TABNAME => '"AIA_RPT_PRD_POLICY_COMM"',
--              ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE
--            --  ,METHOD_OPT=>'FOR ALL INDEXED COLUMNS SIZE AUTO'
--           );
--            end;
--END;


PROCEDURE REP_RUN_PRD_ALL(P_STR_CYCLEDATE IN VARCHAR2) as
  /*
this procedure is the entry to populate PRD report data for Writing and Commission.
*/
Begin
Log('Start PK_RPT_PRODUCTION.REP_RUN_PRD_ALL Package');
Log('Start PK_RPT_PRODUCTION.REP_RUN_INIT Package');
  REP_RUN_INIT (P_STR_CYCLEDATE);
  Log('Start REP_RUN_PRD_WRI Package');
  REP_RUN_PRD_WRI;
--  Log('Start REP_RUN_NEW_PRD_WRI Package');
--  REP_RUN_NEW_PRD_WRI;
  Log('Start REP_RUN_PRD_COMM Package');
  REP_RUN_PRD_COMM;
  Log('Start REP_RUN_PRD_POLICY_WRI Package');
  REP_RUN_PRD_POLICY_WRI;
  Log('Start REP_RUN_PRD_POLICY_COMM Package');
  REP_RUN_PRD_POLICY_COMM;


  COMMIT;

  EXCEPTION WHEN OTHERS THEN NULL;

END;

PROCEDURE REP_RUN_NEW_PRD_WRI(P_STR_CYCLEDATE IN VARCHAR2) as
  /*
this procedure is the entry to populate PRD report data for Writing.
*/
Begin

  REP_RUN_INIT (P_STR_CYCLEDATE);
  REP_RUN_NEW_PRD_WRI;

  COMMIT;

  EXCEPTION WHEN OTHERS THEN NULL;

END;


PROCEDURE REP_RUN_PRD_WRI(P_STR_CYCLEDATE IN VARCHAR2) as
  /*
this procedure is the entry to populate PRD report data for Writing.
*/
Begin

  REP_RUN_INIT (P_STR_CYCLEDATE);
  REP_RUN_PRD_WRI;

  COMMIT;

  EXCEPTION WHEN OTHERS THEN NULL;

END;

PROCEDURE REP_RUN_PRD_COMM(P_STR_CYCLEDATE IN VARCHAR2) as
  /*
this procedure is the entry to populate PRD report data for Commission.
*/
Begin

  REP_RUN_INIT (P_STR_CYCLEDATE);
  REP_RUN_PRD_COMM;

  COMMIT;

  EXCEPTION WHEN OTHERS THEN NULL;

END;

PROCEDURE REP_RUN_PRD_POLICY_WRI(P_STR_CYCLEDATE IN VARCHAR2) as
  /*
this procedure is the entry to populate PRD report data for Commission.
*/
Begin

  REP_RUN_INIT (P_STR_CYCLEDATE);
  REP_RUN_PRD_POLICY_WRI;

  COMMIT;

  EXCEPTION WHEN OTHERS THEN NULL;

END;

PROCEDURE REP_RUN_PRD_POLICY_COMM(P_STR_CYCLEDATE IN VARCHAR2) as
  /*
this procedure is the entry to populate PRD report data for Commission.
*/
Begin

REP_RUN_INIT (P_STR_CYCLEDATE);
  REP_RUN_PRD_POLICY_COMM;

  COMMIT;

  EXCEPTION WHEN OTHERS THEN NULL;

END;

-----version 2
PROCEDURE RUN_PROD_ALL as
  V_REPORTCYCLDATE  VARCHAR2(20);
  BEGIN
    SELECT CTL.TXT_KEY_VALUE INTO V_REPORTCYCLDATE FROM IN_ETL_CONTROL CTL
    WHERE CTL.TXT_KEY_STRING = 'REPORT_CYCLE_DATE';
    Log('report cycle date is '||V_REPORTCYCLDATE);
    REP_RUN_PRD_ALL(V_REPORTCYCLDATE);
    commit;
    EXCEPTION WHEN OTHERS THEN NULL;
  end;

END PK_RPT_PRODUCTION;
 
