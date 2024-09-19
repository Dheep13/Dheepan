CREATE PROCEDURE EXT.PRC_INTPREPAID_INDIVIDUAL
(
    IN vrptname NVARCHAR(4000),   /* RESOLVE: Identifier not found: Table/Column 'rpt_directsales_mapping.reportname' not found (for %TYPE declaration) */
                                                           /* RESOLVE: Datatype unresolved: Datatype (rpt_directsales_mapping.reportname%TYPE) not resolved for parameter 'PRC_INTPREPAID_INDIVIDUAL.vrptname' */
                                                           /* ORIGSQL: vrptname IN ext.rpt_directsales_mapping.reportname%TYPE */
    IN vperiodseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_INTPREPAID_INDIVIDUAL.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    IN vprocessingunitseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_INTPREPAID_INDIVIDUAL.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    IN vcalendarseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
                                                  /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_INTPREPAID_INDIVIDUAL.vcalendarseq' */
                                                  /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
    IN vPeriodtype NVARCHAR(50)      /* RESOLVE: Identifier not found: Table/Column 'rpt_directsales_mapping.REPORT_FREQUENCY' not found (for %TYPE declaration) */
                                                                      /* RESOLVE: Datatype unresolved: Datatype (rpt_directsales_mapping.REPORT_FREQUENCY%TYPE) not resolved for parameter 'PRC_INTPREPAID_INDIVIDUAL.vPeriodtype' */
                                                                      /* ORIGSQL: vPeriodtype IN ext.rpt_directsales_mapping.REPORT_FREQUENCY%TYPE */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    -------------------------------------------------------------------------------------------------------------------
    -- Purpose:
    --
    -- Design objectives:
    -- Data for Dealer Statement Report
    -------------------------------------------------------------------------------------------------------------------
    -- Modification Log:
    -- Date             Author        Description
    -------------------------------------------------------------------------------------------------------------------
    -- 11-Jan-2017      Maria Monisha  Initial release
    -------------------------------------------------------------------------------------------------------------------
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_INTPREPAID_INDIVIDUAL');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_INTPREPAID_INDIVIDUAL') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'RPT_INTPREPAID_INDIVIDUAL';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_INTPREPAID_INDIVIDUAL'; */
    DECLARE vTenantId VARCHAR(4) = SUBSTRING(SESSION_USER,1,4);  /* ORIGSQL: vTenantId VARCHAR2(4) := SUBSTR(USER, 1, 4) ; */
    DECLARE vExtUser VARCHAR(7) = IFNULL(:vTenantId,'') || 'EXT';  /* ORIGSQL: vExtUser VARCHAR2(7) := vTenantId || 'EXT'; */
    DECLARE vSubPartitionPrefix VARCHAR(30) = 'P_';  /* ORIGSQL: vSubPartitionPrefix VARCHAR2(30) := 'P_'; */
    DECLARE vSubPartitionName VARCHAR(30);  /* ORIGSQL: vSubPartitionName VARCHAR2(30); */
    DECLARE vPeriodRow ROW LIKE CS_PERIOD;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_PERIOD' not found (for %ROWTYPE declaration) */
    DECLARE vCalendarRow ROW LIKE CS_CALENDAR;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_CALENDAR' not found (for %ROWTYPE declaration) */
    DECLARE vProcessingUnitRow ROW LIKE CS_PROCESSINGUNIT;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_PROCESSINGUNIT' not found (for %ROWTYPE declaration) */
    DECLARE vCurYrStartDate TIMESTAMP;  /* ORIGSQL: vCurYrStartDate DATE; */
    DECLARE vCurYrEndDate TIMESTAMP;  /* ORIGSQL: vCurYrEndDate DATE; */
    DECLARE v_envdtl VARCHAR(1000);  /* ORIGSQL: v_envdtl VARCHAR2(1000); */
    DECLARE v_sql NCLOB;  /* ORIGSQL: v_sql long; */
    DECLARE v_payable SMALLINT;  /* ORIGSQL: v_payable NUMBER(1); */
    DECLARE v_UserGroup VARCHAR(1) = 'N';  /* ORIGSQL: v_UserGroup VARCHAR2(1) := 'N'; */
    DECLARE v_payableflag SMALLINT;  /* ORIGSQL: v_payableflag NUMBER(1); */
    DECLARE v_reportgroup VARCHAR(127);  /* ORIGSQL: v_reportgroup VARCHAR2(127); */
    DECLARE v_classifierid NVARCHAR(127);  /* ORIGSQL: v_classifierid NVARCHAR2(127); */
    DECLARE cEndofTime CONSTANT date := to_date('2200-01-01','yyyy-mm-dd');
    DECLARE vcredittypeid_PayAdj VARCHAR(255); 
    ----------INDIVIDUAL PRODUCT-MEASUREMENT ... INDIVIDUAL

    /* ORIGSQL: for i in (select distinct product from ext.rpt_directsales_mapping where reportname = vrptname and allgroups = 'INDIVIDUAL PRODUCTS' AND PRODUCT IS NOT NULL AND PRODUCT <> 'IndividualAch') Loop CALL EXT.P(...) */
    DECLARE CURSOR dbmtk_cursor_7972
    FOR 
        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_DIRECTSALES_MAPPING' not found */

        SELECT   /* ORIGSQL: select distinct product from ext.rpt_directsales_mapping where reportname = vrptname and allgroups = 'INDIVIDUAL PRODUCTS' AND PRODUCT IS NOT NULL AND PRODUCT <> 'IndividualAch'; */
            DISTINCT
            product
        FROM
            ext.rpt_directsales_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'INDIVIDUAL PRODUCTS'
            AND PRODUCT IS NOT NULL
            AND PRODUCT <> 'IndividualAch';

    ----TEAM PRODUCTS MEASUREMENT

    /* ORIGSQL: for i in (select distinct product from ext.rpt_directsales_mapping where reportname = vrptname and allgroups = 'TEAM PRODUCTS' AND PRODUCT IS NOT NULL) Loop CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, vProcNam(...) */
    DECLARE CURSOR dbmtk_cursor_7975
    FOR 
        SELECT   /* ORIGSQL: select distinct product from ext.rpt_directsales_mapping where reportname = vrptname and allgroups = 'TEAM PRODUCTS' AND PRODUCT IS NOT NULL; */
            DISTINCT
            product
        FROM
            ext.rpt_directsales_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'TEAM PRODUCTS'
            AND PRODUCT IS NOT NULL;

    /* changed overall commission  section
      ---ADJUST COMMISSION PRODUCTS-Advance Protected Commission, Payment Adjustment  -Deposit
    
    
    for i in (select distinct product from ext.rpt_directsales_mapping where reportname = vrptname
     and allgroups = 'ADJUST COMMISSION' and product not in 'CE Adjustment')
    Loop
    
     prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION PRODUCTS Deposit',NULL,i.product);
     v_sql :=
     'INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
                                               (tenantid,
                                                    positionseq,
                                                    payeeseq,
                                                    processingunitseq,
                                                    periodseq,
                                                    periodname,
                                                    processingunitname,
                                                    calendarname,
                                                    reportcode,
                                                    sectionid,
                                                    sectionname,
                                                    subsectionname,
                                                    sortorder,
                                                    titlename,
                                                    loaddttm,
                                                    allgroups,
                                                    geid,
                                                    name,
                                                    products,
                                                    BASICSAMT,
                                                    shopname,
                                                    teamvisible,
                                                    payable_flag,
                                                    reportgroup
                                                )
        SELECT   '''||vTenantID||''',
                 pad.positionseq,
                 pad.payeeseq,
                 '||vProcessingUnitRow.processingunitseq||',
                 '||vperiodseq||',
                 '''||vPeriodRow.name||''',
                 '''||vProcessingUnitRow.name||''',
                 '''||vCalendarRow.name||''',
                 ''87'' reportcode,
                 ''04'' sectionid,
                 ''ADJUST COMMISSION'' sectionname,
                 ''ADJUST COMMISSION'' subsectionname,
                 ''01'' sortorder,
                 pad.reporttitle titlename,
                 SYSDATE,
                 ''ADJUST COMMISSION'' allgroups,
                 pad.PARTICIPANTID, --geid
                 pad.FULLNAME, --name
                 product,
                 BASICSAMT,
                 pad.POSITIONGA1 shopname,
                 nvl(pad.TITLEGB1,1) teamvisible,
                 '||v_payableflag||' payable_flag,
                 '''||v_reportgroup||''' reportgroup
          FROM   ext.rpt_base_padimension pad,
                 (
                          SELECT
                            CM.positionseq,
                            CM.payeeseq,
                            CM.processingunitseq,
                            CM.periodseq,
                            rmap.allgroups allgroups,
                            '''||i.product||''' product,
                            MAX(case when rmap.rptcolumnname = ''BASICSAMT'' then '||fun_directsales_mapping(vrptname,'BASICSAMT',i.product)||' end) AS BASICSAMT
                          FROM ext.rpt_base_deposit CM,rpt_directsales_mapping rmap
                            WHERE CM.name in rmap.rulename
         AND rmap.reportname= '''||vrptname||'''
         and CM.periodseq = '||vperiodseq||'
         and CM.processingunitseq = '||vprocessingunitseq||'
         and rmap.product = '''||i.product||'''
         and rmap.allgroups = ''ADJUST COMMISSION''
                          GROUP BY CM.positionseq,
                            CM.payeeseq,
                            CM.processingunitseq,
                            CM.periodseq,
                            rmap.allgroups
                  )mes
         WHERE       pad.payeeseq = mes.payeeseq
     AND pad.positionseq = mes.positionseq
     AND pad.processingunitseq = mes.processingunitseq
     and pad.periodseq = mes.periodseq
     and pad.reportgroup = '''||v_reportgroup||'''';          --need to change
    
     prc_logevent (vPeriodRow.name,vProcName,'Completed ADJUST COMMISSION PRODUCTS Deposit',NULL,v_sql);
    
     EXECUTE IMMEDIATE v_sql;
    
     COMMIT;
    
    end loop;
    
    -- Adjustment Commission : CE Adjustment
      prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION MEASUREMENT',NULL,'CE Adjustment');
    v_sql :=
     'INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
                 (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                      processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
                      sortorder,titlename,loaddttm,allgroups,geid,name,
                      products,CECOMM,shopname,teamvisible,payable_flag,reportgroup
                  )
           SELECT   '''||vTenantID||''', pad.positionseq, pad.payeeseq, '||vProcessingUnitRow.processingunitseq||',
                       '||vperiodseq||','''||vPeriodRow.name||''',             '''||vProcessingUnitRow.name||''','''||vCalendarRow.name||''',''59'' reportcode,
                    04 sectionid,''ADJUST COMMISSION'' sectionname,''ADJUST COMMISSION'' subsectionname,
                    02 sortorder, pad.reporttitle titlename, SYSDATE,
                    ''ADJUST COMMISSION'' allgroups,pad.PARTICIPANTID,pad.FULLNAME,''CE Adjustment'' products, CECOMM,
                    pad.POSITIONGA1 shopname,nvl(pad.TITLEGB1,1) teamvisible,'||v_payableflag||' payable_flag,'''||v_reportgroup||''' reportgroup
           FROM   ext.rpt_base_padimension pad,
             (   select mes.positionseq,
                                 mes.payeeseq,
                                 mes.processingunitseq,
                                 mes.periodseq,
                                 MAX(case when rmap.rptcolumnname = ''CECOMM'' then '||fun_sts_mapping(vrptname,'CECOMM','CE Adjustment')||' end) AS CECOMM
                                 from ext.rpt_BASE_MEASUREMENT mes,rpt_directsales_mapping rmap
                              where  mes.name in rmap.rulename
         AND rmap.reportname= '''||vrptname||'''
         and mes.periodseq = '||vperiodseq||'
         and mes.processingunitseq = '||vprocessingunitseq||'
         and rmap.product = ''CE Adjustment''
         and rmap.allgroups = ''ADJUST COMMISSION''
                              group by mes.positionseq,
                                   mes.payeeseq,
                                   mes.processingunitseq,
                                   mes.periodseq
              )mes
           WHERE pad.payeeseq = mes.payeeseq
     AND pad.positionseq = mes.positionseq
     AND pad.processingunitseq = mes.processingunitseq
     and pad.periodseq = mes.periodseq
     and pad.reportgroup = '''||v_reportgroup||'''';
    
        EXECUTE IMMEDIATE v_sql;
         prc_logevent (vPeriodRow.name,vProcName,'END ADJUST COMMISSION MEASUREMENT',NULL,v_sql);
    
     COMMIT;
    
    --CE Adjustment merge
    prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION Incentive for Individual',NULL,'Individual Payout');
     MERGE INTO ext.rpt_INTPREPAID_INDIVIDUAL rpt using
                      (select cre.positionseq,
                                 cre.payeeseq,
                                 cre.processingunitseq,
                                 cre.periodseq,
                                 max(cre.genericattribute4) REMARKS,
                                 sum(cre.genericnumber1) CEACTUAL,
                                 sum(cre.value) CEADJ
                          from ext.rpt_base_credit cre
                          where cre.processingunitseq = vprocessingunitseq
         and cre.periodseq = vperiodseq
         and cre.credittypeid = pkg_reporting_extract_r2.vcredittypeid_CEAdj
                          group by cre.positionseq,
                                   cre.payeeseq,
                                   cre.processingunitseq,
                               cre.periodseq)qtr
       on (rpt.processingunitseq = qtr.processingunitseq
         and rpt.periodseq = qtr.periodseq
         and rpt.positionseq = qtr.positionseq
         and rpt.payeeseq = qtr.payeeseq
         and rpt.allgroups='ADJUST COMMISSION'
         and rpt.products='CE Adjustment'
        )
       when matched then update set rpt.REMARKS =qtr.REMARKS,rpt.CEACTUAL=qtr.CEACTUAL,rpt.CEADJ=qtr.CEADJ;
    
      prc_logevent (vPeriodRow.name,vProcName,'Begin Adjust commssion  credit for CE Adjustment start',NULL,'CE Adjustment Start');
      COMMIT;
      prc_logevent (vPeriodRow.name,vProcName,'Begin Adjust commssion  credit for CE Adjustment complete',NULL,'CE Adjustment complete');
    
    --ADJUST COMMISSION CREDIT..Remarks
    
    prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Credit REMARKS',NULL,'REMARKS');
    INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
                 (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                      processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
                      sortorder,titlename,loaddttm,allgroups,geid,name,
                      products, remarks,shopname,teamvisible,payable_flag,reportgroup
                  )
           SELECT   vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processingunitseq,
                    vperiodseq,vPeriodRow.name,vProcessingUnitRow.name,:vCalendarRow.name,'59' reportcode,
                    '04' sectionid,'ADJUST COMMISSION' sectionname,'ADJUST COMMISSION' subsectionname,
                    '03' sortorder, pad.reporttitle titlename, SYSDATE,
                    'ADJUST REMARKS' allgroups,pad.PARTICIPANTID,pad.FULLNAME,'ADJUST REMARKS' products, REMARKS,pad.POSITIONGA1 shopname,
                    nvl(pad.TITLEGB1,1) teamvisible,v_payableflag,v_reportgroup reportgroup
           FROM   ext.rpt_base_padimension pad,
             (    select mes.positionseq,
                                 mes.payeeseq,
                                 mes.processingunitseq,
                                 mes.periodseq,
                                 max(mes.genericattribute3) Remarks
        
                          from ext.rpt_base_credit mes
                          where mes.processingunitseq = vprocessingunitseq
         and mes.periodseq = vperiodseq
         and mes.credittypeid = pkg_reporting_extract_r2.vcredittypeid_PayAdj
                          group by mes.positionseq,
                                   mes.payeeseq,
                                   mes.processingunitseq,
                                   mes.periodseq
              )mes
           WHERE pad.payeeseq = mes.payeeseq
     AND pad.positionseq = mes.positionseq
     AND pad.processingunitseq = mes.processingunitseq
     and pad.periodseq = mes.periodseq
     and pad.reportgroup = v_reportgroup;
    
           COMMIT;
    prc_logevent (vPeriodRow.name,vProcName,'ADJUST COMMISSION Credit REMARKS Completed',NULL,'REMARKS');
    
      --ADJUST commission CREDIT..Operational Compliance Adjustment
    prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Credit Operational Compliance Adjustment',NULL,'Operational Compliance Adjustment');
    
    INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
                 (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                      processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
                      sortorder,titlename,loaddttm,allgroups,geid,name,
                      products, BASICSAMT,shopname,teamvisible,payable_flag,reportgroup
                  )
           SELECT   vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processingunitseq,
                    vperiodseq,vPeriodRow.name,vProcessingUnitRow.name,:vCalendarRow.name,'59' reportcode,
                    '04' sectionid,'ADJUST COMMISSION' sectionname,'ADJUST COMMISSION' subsectionname,
                    '04' sortorder, pad.reporttitle titlename, SYSDATE,
                    'ADJUST COMMISSION' allgroups,pad.PARTICIPANTID,pad.FULLNAME,'Operational Compliance Adjustment' products, BASICSAMT,
                    pad.POSITIONGA1 shopname, nvl(pad.TITLEGB1,1) teamvisible,v_payableflag payable_flag,v_reportgroup reportgroup
           FROM   ext.rpt_base_padimension pad,
             (    select mes.positionseq,
                                 mes.payeeseq,
                                 mes.processingunitseq,
                                 mes.periodseq,
                                 sum(mes.value) BASICSAMT
                          from ext.rpt_base_credit mes
                          where mes.processingunitseq = vprocessingunitseq
         and mes.periodseq = vperiodseq
         and mes.credittypeid = pkg_reporting_extract_r2.vcredittypeid_PayAdj
         and mes.genericattribute1 = pkg_reporting_extract_r2.vOperational_Compliance
                          group by mes.positionseq,
                                   mes.payeeseq,
                                   mes.processingunitseq,
                                   mes.periodseq
              )mes
           WHERE pad.payeeseq = mes.payeeseq
     AND pad.positionseq = mes.positionseq
     AND pad.processingunitseq = mes.processingunitseq
     and pad.periodseq = mes.periodseq
     and pad.reportgroup = v_reportgroup;
    
           COMMIT;
          prc_logevent (vPeriodRow.name,vProcName,'ADJUST COMMISSION Credit Operational Compliance Adjustment Completed',NULL,'Operational Compliance Adjustment');
    
     --ADJUST commission BALANCE ..Prior Balance Adjustment
    prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Balance Prior Balance Adjustment',NULL,'Balance Prior Balance Adjustment');
    
      INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
         (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
              processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
              sortorder,titlename,loaddttm,allgroups,geid,name,
              products, BASICSAMT,shopname,teamvisible,payable_flag,reportgroup
          )
      SELECT    vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processingunitseq,
                    vperiodseq,vPeriodRow.name,vProcessingUnitRow.name,:vCalendarRow.name,'59' reportcode,
                    '04' sectionid,'ADJUST COMMISSION' sectionname,'ADJUST COMMISSION' subsectionname,
                    '05' sortorder, pad.reporttitle titlename, SYSDATE,
                    'ADJUST COMMISSION' allgroups,pad.PARTICIPANTID,pad.FULLNAME,'Prior Balance Adjustment' products, BASICSAMT,
                    pad.POSITIONGA1 shopname, nvl(pad.TITLEGB1,1) teamvisible,v_payableflag payable_flag,v_reportgroup reportgroup
      FROM   ext.rpt_base_padimension pad,
      (     select pay.positionseq,
                       pay.payeeseq,
                       pay.processingunitseq,
                       pay.periodseq,
                       sum(bal.value) BASICSAMT
                  from cs_balance bal,
                    cs_balancepaymenttrace baltrace,
                    cs_payment pay
                  where
                    bal.periodseq=baltrace.sourceperiodseq
         and baltrace.targetperiodseq = pay.periodseq
         and bal.balanceseq = baltrace.balanceseq
         and bal.processingunitseq = pay.processingunitseq
         and bal.processingunitseq = baltrace.processingunitseq
         and pay.periodseq = vperiodseq
         and pay.processingunitseq = vprocessingunitseq
                  group by pay.positionseq,
                         pay.payeeseq,
                         pay.processingunitseq,
                         pay.periodseq
          )mes
           WHERE       pad.payeeseq = mes.payeeseq
     AND pad.positionseq = mes.positionseq
     AND pad.processingunitseq = mes.processingunitseq
     and pad.periodseq = mes.periodseq
     and pad.reportgroup = v_reportgroup;
    
       COMMIT;
    prc_logevent (vPeriodRow.name,vProcName,'ADJUST COMMISSION Balance Prior Balance Adjustment Completed',NULL,'Balance Prior Balance Adjustment');
    
    
    ---ADJUST commission-- Total Adjustment
    prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST commission Total Adjustment',NULL,'Total Adjustment');
    
    INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
             (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                  processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
                  sortorder,titlename,loaddttm,allgroups,geid,name,
                  products, SECTION_COMMISSION,shopname,teamvisible,payable_flag,reportgroup
              )
           SELECT   vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processingunitseq,
                    vperiodseq,vPeriodRow.name,vProcessingUnitRow.name,:vCalendarRow.name,'00' reportcode,
                    '04' sectionid,'ADJUST COMMISSION' sectionname,'ADJUST COMMISSION' subsectionname,
                    '06' sortorder, pad.reporttitle titlename, SYSDATE,
                    'ADJUST REMARKS' allgroups,pad.PARTICIPANTID,pad.FULLNAME,'Total Adjustment' products, SECTION_COMMISSION,
                    pad.POSITIONGA1 shopname, nvl(pad.TITLEGB1,1) teamvisible,v_payableflag payable_flag,v_reportgroup reportgroup
           FROM   ext.rpt_base_padimension pad,
             (    select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq,
                        sum(mes.basicsamt) SECTION_COMMISSION
                      from ext.rpt_INTPREPAID_INDIVIDUAL mes
                        where mes.allgroups='ADJUST COMMISSION'
         and mes.periodseq = vperiodseq
         and mes.processingunitseq = vprocessingunitseq
                      group by mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq
              )mes
           WHERE pad.payeeseq = mes.payeeseq
     AND pad.positionseq = mes.positionseq
     AND pad.processingunitseq = mes.processingunitseq
     and pad.periodseq = mes.periodseq
     and pad.reportgroup = v_reportgroup;
    Commit;
    prc_logevent (vPeriodRow.name,vProcName,'ADJUST commission Total Adjustment Complete',NULL,'Total Adjustment');
    
    --Total Commission Payout
    
    prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Total Commission Payout',NULL,'Total Commission Payout');
    INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
             (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                  processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
                  sortorder,titlename,loaddttm,allgroups,geid,name,
                  products, TOTALCOMMISSION,shopname,teamvisible,payable_flag,reportgroup
              )
           SELECT   vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processingunitseq,
                    vperiodseq,vPeriodRow.name,vProcessingUnitRow.name,:vCalendarRow.name,'59' reportcode,
                    '05' sectionid,'TOTAL COMMISSION' sectionname,'OVERALL COMMISSION' subsectionname,
                    '99' sortorder, pad.reporttitle titlename, SYSDATE,
                    'ADJUST REMARKS' allgroups,pad.PARTICIPANTID,pad.FULLNAME,'Total Commission Payout' products, TOTALCOMMISSION,
                    pad.POSITIONGA1 shopname,nvl(pad.TITLEGB1,1) teamvisible,v_payableflag payable_flag,v_reportgroup reportgroup
           FROM   ext.rpt_base_padimension pad,
             (    select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq,
                        sum(mes.SECTION_COMMISSION) TOTALCOMMISSION
                      from ext.rpt_INTPREPAID_INDIVIDUAL mes
                        where mes.allgroups in ('ADJUST REMARKS','EARNED COMMISSION')
         and mes.periodseq = vperiodseq
         and mes.processingunitseq = vprocessingunitseq
                      group by mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq
              )mes
           WHERE pad.payeeseq = mes.payeeseq
     AND pad.positionseq = mes.positionseq
     AND pad.processingunitseq = mes.processingunitseq
     and pad.periodseq = mes.periodseq
     and pad.reportgroup = v_reportgroup;
    
    Commit;
    prc_logevent (vPeriodRow.name,vProcName,'OVERALL COMMISSION Total Commission Payout Completed',NULL,'Total Commission Payout');
    
    --Update the null OTC,GEID,NAME
    
       MERGE INTO ext.rpt_INTPREPAID_INDIVIDUAL rpt using
                      (select distinct mes.positionseq,
                                 mes.payeeseq,
                                 mes.processingunitseq,
                                 mes.periodseq,
                                 mes.OTC,
                                 mes.GEID,
                                 mes.NAME,
                                 mes.shopname
                          from ext.rpt_INTPREPAID_INDIVIDUAL mes
                          where mes.processingunitseq = vprocessingunitseq
         and mes.periodseq = vperiodseq
         and mes.sectionname='INDIVIDUAL ACHIEVEMENT'
         and mes.subsectionname='INDIVIDUAL ACHIEVEMENT'
                       )qtr
       on (rpt.processingunitseq = qtr.processingunitseq
         and rpt.periodseq = qtr.periodseq
         and rpt.positionseq = qtr.positionseq
     and rpt.payeeseq = qtr.payeeseq)
      when matched then update set rpt.OTC = qtr.OTC,rpt.GEID = qtr.GEID,rpt.NAME = qtr.NAME,rpt.shopname=qtr.shopname;
    COMMIT;*/

    ----------##########begin adjustment insert
    ---ADJUST COMMISSION PRODUCTS- Payment Adjustment  -Deposit

    /* ORIGSQL: for i in (select distinct product from ext.rpt_directsales_mapping where reportname = vrptname and allgroups = 'ADJUST COMMISSION' and product not in 'CE Adjustment') Loop CALL EXT.PRC_LOGEVENT(:vPeriodRow(...) */
    DECLARE CURSOR dbmtk_cursor_7978
    FOR 
        SELECT   /* ORIGSQL: select distinct product from ext.rpt_directsales_mapping where reportname = vrptname and allgroups = 'ADJUST COMMISSION' and product not in 'CE Adjustment'; */
            DISTINCT
            product
        FROM
            ext.rpt_directsales_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'ADJUST COMMISSION'
            AND product <> 'CE Adjustment';

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            vSQLERRM = SUBSTRING(::SQL_ERROR_MESSAGE,1,3900);  /* ORIGSQL: SUBSTR(SQLERRM, 1, 3900) */

            /* ORIGSQL: prc_logevent (vPeriodRow.name, vProcName, 'ERROR', NULL, vsqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ERROR', NULL, :vSQLERRM);

            /* ORIGSQL: raise_application_error(-20911,'Error raised: '||vprocname||' Failed: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE || ' - '||vsqlerrm) */
            -- sapdbmtk: mapped error code -20911 => 10911: (ABS(-20911)%10000)+10000
            SIGNAL SQL_ERROR_CODE 10911 SET MESSAGE_TEXT = 'Error raised: '||IFNULL(:vProcName,'')||' Failed: '
            || ' - '||IFNULL(:vSQLERRM,'');  /* RESOLVE: Standard Package call(not converted): 'DBMS_UTILITY.FORMAT_ERROR_BACKTRACE' not supported, manual conversion required */
        END;

        /* initialize library variables, if not yet done */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2:init_session_global();/*Deepan : replacing with session variable*/
        
        SET  'vProcName' = NULL;
        SET  'vSQLerrm' = NULL;
        SET  'vCurYrStartDate' = NULL;
        SET  'vCurYrEndDate' = NULL;
        SET  'vPeriodtype' = 'MONTHLY';
        SET  'vSTSRoadShowCategory' = 'Roadshow';
        SET  'veventtypeid_ccomobile' = 'Mobile Closed';
        SET  'veventtypeid_ccotv' = 'TV Closed';
        SET  'vcredittypeid_PayAdj' = 'Payment Adjustment';
        SET  'vcredittypeid_CEAdj' = 'Customer Experience';
        SET  'vcredittypeid_Mobile' = 'CCO Mobile VAS';
        SET  'vcredittypeid_TV' = 'CCO TV';
        SET  'vcredittypeid_HandFee' = 'TVReconHandlingFee - CCO';
        SET  'vOperational_Compliance' = 'Operational Compliance';
        SET  'vGSTRate' = 0.07;

        --!!!!!!The below truncate and variable initialization will be executed in ext.rpt_reporting_extract.prc_driver. Will remove unnecessary code after initial testing.
        /* ORIGSQL: prc_logevent (NULL, vProcName, 'Begin procedure', NULL, vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(NULL, :vProcName, 'Begin procedure', NULL, :vSQLERRM);
        --------Add subpartitions to report table if needed----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AddTableSubpartition (vExtUser, vTCTemplateTable, vTCSchemaName, vTenantId, vProcessingUnitSeq, vPeriodSeq, vRptTableName) */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2:prc_AddTableSubpartition(
        --         :vExtUser,
        --         :vTCTemplateTable,
        --         :vTCSchemaName,
        --         :vTenantId,
        --         :vprocessingunitseq,
        --         :vperiodseq,
        --         :vRptTableName
        --     );

        --------Find subpartition name------------------------------------------------------------------------------------

        -- vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName);  /* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vProcessingUnitSeq, vPeriodSeq, vRptTableName) */

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, vSubPartitionName) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2:prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);

        --------Truncate report table subpartition------------------------------------------------------------------------
        --pkg_reporting_extract_r2.prc_TruncateTableSubpartition (vRptTableName,
        --                                                   vSubpartitionName);
        --Since Deleting the records using reportgroup

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, vSubPartitionName) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2:prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);

        --------Turn on Parallel DML---------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION ENABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION ENABLE PARALLEL DML ; */

        --------Initialize variables---------------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (NULL, vProcName, 'Setting up variables', NULL, vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(NULL, :vProcName, 'Setting up variables', NULL, :vSQLERRM);

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIOD' not found */

        SELECT
            per.*
        INTO
            vPeriodRow
        FROM
            cs_period per
        WHERE
            per.removedate = :cEndofTime  /* ORIGSQL: pkg_reporting_extract_r2.cEndofTime */
            AND per.periodseq = :vperiodseq;

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PROCESSINGUNIT' not found */

        SELECT
            pu.*
        INTO
            vProcessingUnitRow
        FROM
            cs_processingunit pu
        WHERE
            pu.processingunitseq = :vprocessingunitseq;

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_CALENDAR' not found */

        SELECT
            cal.*
        INTO
            vCalendarRow
        FROM
            cs_calendar cal
        WHERE
            cal.removedate = :cEndofTime  /* ORIGSQL: pkg_reporting_extract_r2.cEndofTime */
            AND cal.calendarseq = :vcalendarseq;

        /*
           SELECT   per.startdate, per.enddate - 1
             INTO   vCurYrStartDate, vCurYrEndDate
             FROM   cs_period per
            WHERE   per.periodSeq =
                       (    SELECT   per1.periodseq
                                  FROM   cs_period per1, cs_periodtype pt1
                                 WHERE   per1.PeriodTypeseq = pt1.PeriodTypeseq
             AND pt1.Name = 'year'
                            START WITH   per1.periodseq = vperiodseq
                        CONNECT BY   PRIOR per1.parentseq = per1.periodseq);
        */

        --------Begin Insert-------------------------------------------------------------------------------
        v_UserGroup = 'N';

        v_reportgroup = 'InternalPrepaid';

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin insert',NULL,vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin insert', NULL, :vSQLERRM);

        /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_CLASSIFIER' not found */

        SELECT
            MAX(GENERICBOOLEAN1)
        INTO
            v_payable
        FROM
            EXT.STEL_CLASSIFIER
        WHERE
            CATEGORYTREENAME = 'Reporting Config'
            AND Categoryname = 'Payable'
            AND Classifierid = :v_classifierid
            AND effectiveenddate > :vPeriodRow.startdate
            AND effectivestartdate < :vPeriodRow.enddate;

        SELECT
            MAX('Y') 
        INTO
            v_UserGroup
            /* RESOLVE: Identifier not found: Table/view 'TCMP.CS_PIPELINERUN' not found */
        FROM
            (
                SELECT   /* ORIGSQL: (select RTRIM(REGEXP_SUBSTR(runparameters, '\[boGroupsList\]([^\[]+)', 1, 1, 'i', 1), ',') ||',' GroupList from tcmp.cs_pipelinerun where command ='PipelineRun' and description like '%ODS%' and state<(...) */
                    IFNULL(RTRIM(SUBSTRING_REGEXPR('\[boGroupsList\]([^\[]+)' FLAG 'i' IN runparameters FROM 1 OCCURRENCE 1), ','),'') ||',' AS GroupList  
                FROM
                    tcmp.cs_pipelinerun
                WHERE
                    command = 'PipelineRun'
                    AND description LIKE '%ODS%'
                    AND state <> 'Done'
                    AND periodseq = :vperiodseq
                    AND processingunitseq = :vprocessingunitseq
            ) AS dbmtk_corrname_8001
        WHERE
            Grouplist LIKE '%User Group%';

        IF :v_UserGroup = 'Y' 
        THEN
            v_payableflag = :v_payable;
        ELSE 
            v_payableflag = 1;
        END IF;

        -----DELETE EXISTING RECORDS BASED ON REPORT GROUP 

        /* ORIGSQL: DELETE FROM ext.rpt_INTPREPAID_INDIVIDUAL WHERE reportgroup=v_reportgroup and periodseq=vperiodseq and processingunitseq=vprocessingunitseq and FREQUENCY=vPeriodtype; */
        DELETE
        FROM
            ext.rpt_INTPREPAID_INDIVIDUAL
        WHERE
            reportgroup = :v_reportgroup
            AND periodseq = :vperiodseq
            AND processingunitseq = :vprocessingunitseq
            AND FREQUENCY = :vPeriodtype;

        /* ORIGSQL: commit; */
        COMMIT;

        FOR i AS dbmtk_cursor_7972
        DO
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual PRODUCTS MEASUREMENTcompleted',NULL,i.product) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS MEASUREMENTcompleted', NULL, :i.product);
            v_sql = 'INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
            (tenantid, positionseq,payeeseq,processingunitseq,periodseq,periodname,processingunitname,calendarname,
                reportcode,sectionid,sectionname,subsectionname,sortorder,titlename,loaddttm,allgroups,geid,name,products,
            CONNTARGET,CONNACTUALS,shopname,teamvisible,payable_flag,reportgroup,FREQUENCY)
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''87'' reportcode,
            ''01'' sectionid,
            ''INDIVIDUAL ACHIEVEMENT'' sectionname,
            ''INDIVIDUAL ACHIEVEMENT'' subsectionname,
            ''01'' sortorder,
            pad.reporttitle titlename,
            SYSDATE,
            ''INDIVIDUAL PRODUCTS'' allgroups,
            pad.PARTICIPANTID, --geid
            pad.FULLNAME, --name
            product,
            CONNTARGET,
            CONNACTUALS,
            pad.POSITIONGA1 shopname,
            nvl(pad.TITLEGB1,1) teamvisible,
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
            ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
            ''' ||IFNULL(:vPeriodtype,'')||''' FREQUENCY
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups allgroups,
                ''' ||IFNULL(:i.product,'')||''' product,
                titlemap.titlename,
                SUM(case when rmap.rptcolumnname = ''CONNTARGET'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CONNTARGET', :i.product, :vPeriodtype),'') ||' end) AS CONNTARGET,
                SUM(case when rmap.rptcolumnname = ''CONNACTUALS'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CONNACTUALS', :i.product, :vPeriodtype),'') ||' end) AS CONNACTUALS
                FROM ext.rpt_BASE_MEASUREMENT CM,rpt_directsales_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
                WHERE CM.GENERICATTRIBUTE1=rmap.SUBRPTCLMN1
                and CM.GENERICATTRIBUTE2=rmap.SUBRPTCLMN2
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                and CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                and CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and rmap.product = '''||IFNULL(:i.product,'')||'''
                and rmap.allgroups = ''INDIVIDUAL PRODUCTS''
                and rmap.product=titlemap.product
                and rmap.allgroups=titlemap.allgroups
                and rmap.report_frequency=titlemap.report_frequency
                and rmap.reportname=titlemap.reportname
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups,titlemap.titlename
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.POSITIONTITLE=mes.titlename
            and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
            and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_directsales_mapping(vrptname,'CONNTARGET',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'CONNACTUALS',i.product,vPeriodtype) */
            --need to change

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual PRODUCTS MEASUREMENT start',NULL,v_sql) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS MEASUREMENT start', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual PRODUCTS MEASUREMENT completed',NULL,v_sql) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS MEASUREMENT completed', NULL, :v_sql);

            /* ORIGSQL: COMMIT; */
            COMMIT;

            --MERGE for Conn Weight
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual PRODUCTS MEASUREMENT for Conn Weight',NULL,'POINTSPAYOUT') */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS MEASUREMENT for Conn Weight', NULL, 'POINTSPAYOUT');
            v_sql = 'MERGE INTO ext.rpt_INTPREPAID_INDIVIDUAL rpt using
            (select CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                SUM(case when rmap.rptcolumnname = ''CONNWT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CONNWT', :i.product, :vPeriodtype),'') ||' end) AS CONNWT,
                SUM(case when rmap.rptcolumnname = ''CONNACTUALTARGET'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CONNACTUALTARGET', :i.product, :vPeriodtype),'') ||' end ) AS CONNACTUALTARGET
                FROM ext.rpt_BASE_MEASUREMENT CM,rpt_directsales_mapping rmap
                WHERE CM.GENERICATTRIBUTE1=rmap.SUBRPTCLMN1
                and CM.GENERICATTRIBUTE3 <> rmap.SUBRPTCLMN2
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                and CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                and CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and rmap.product = '''||IFNULL(:i.product,'')||'''
                and rmap.report_frequency ='''||IFNULL(:vPeriodtype,'')||'''
                and rmap.allgroups = ''INDIVIDUAL PRODUCTS''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq
            )qtr
            on (rpt.processingunitseq = qtr.processingunitseq
                and rpt.periodseq = qtr.periodseq
                and rpt.positionseq = qtr.positionseq
                and rpt.payeeseq = qtr.payeeseq
                and rpt.PRODUCTS= ''' ||IFNULL(:i.product,'')||'''
                and rpt.allgroups = ''INDIVIDUAL PRODUCTS''
                and rpt.frequency='''||IFNULL(:vPeriodtype,'')||''')
            when matched then update set rpt.CONNWT=(qtr.CONNWT*100),rpt.CONNACTUALTARGET=(qtr.CONNACTUALTARGET*100)';  /* ORIGSQL: fun_directsales_mapping(vrptname,'CONNWT',i.product,vPeriodtype) */
                                                                                                                                                                                                                        /* ORIGSQL: fun_directsales_mapping(vrptname,'CONNACTUALTARGET',i.product,vPeriodtype) */

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Individual PRODUCTS MEASUREMENT for Conn Weight start',NULL,v_sql) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Individual PRODUCTS MEASUREMENT for Conn Weight start', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Individual PRODUCTS MEASUREMENT for Conn Weight completed',NULL,v_sql) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Individual PRODUCTS MEASUREMENT for Conn Weight completed', NULL, :v_sql);

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

        ----------INDIVIDUAL AVERAGE-MEASUREMENT ... INDIVIDUAL

        v_sql = 'INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
        (tenantid,
            positionseq,
            payeeseq,
            processingunitseq,
            periodseq,
            periodname,
            processingunitname,
            calendarname,
            reportcode,
            sectionid,
            sectionname,
            subsectionname,
            sortorder,
            titlename,
            loaddttm,
            allgroups,
            geid,
            name,
            products,
            GAPER,
            shopname,
            teamvisible,
            payable_flag,
            reportgroup,
            frequency
        )
        SELECT   ''' ||IFNULL(:vTenantId,'')||''',
        pad.positionseq,
        pad.payeeseq,
        ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
        ' ||IFNULL(:vperiodseq,'')||',
        ''' ||IFNULL(:vPeriodRow.name,'')||''',
        ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
        ''' ||IFNULL(:vCalendarRow.name,'')||''',
        ''87'' reportcode,
        ''01'' sectionid,
        ''INDIVIDUAL ACHIEVEMENT'' sectionname,
        ''AVERAGE ACHIEVEMENT'' subsectionname,
        ''99'' sortorder,
        pad.reporttitle titlename,
        SYSDATE,
        ''POINTS PAYOUT'' allgroups,
        pad.PARTICIPANTID, --geid
        pad.FULLNAME, --name
        ''ACHIEVEMENT'' product,
        (GAPER*100) GAPER,
        pad.POSITIONGA1 shopname,
        nvl(pad.TITLEGB1,1) teamvisible,
        ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
        ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
        ''' ||IFNULL(:vPeriodtype,'')||''' frequency
        FROM   ext.rpt_base_padimension pad,
        (
            SELECT
            INC.positionseq,
            INC.payeeseq,
            INC.processingunitseq,
            INC.periodseq,
            rmap.allgroups allgroups,
            titlemap.titlename,
            MAX(case when rmap.rptcolumnname = ''GAPER'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'GAPER', 'ACHIEVEMENT', :vPeriodtype),'') ||' end) AS GAPER
            FROM ext.rpt_BASE_INCENTIVE INC,rpt_directsales_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
            WHERE INC.GENERICATTRIBUTE1 in rmap.rulename
            and rmap.product=titlemap.product
            and rmap.allgroups=titlemap.allgroups
            and rmap.report_frequency=titlemap.report_frequency
            and rmap.reportname=titlemap.reportname
            AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
            and INC.periodseq = '||IFNULL(:vperiodseq,'')||'
            and INC.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and rmap.product = ''ACHIEVEMENT''
            and rmap.allgroups = ''INDIVIDUAL AVERAGE''
            GROUP BY INC.positionseq,
            INC.payeeseq,
            INC.processingunitseq,
            INC.periodseq,
            rmap.allgroups,titlemap.titlename
        )mes
        WHERE       pad.payeeseq = mes.payeeseq
        AND pad.positionseq = mes.positionseq
        AND pad.processingunitseq = mes.processingunitseq
        and pad.periodseq = mes.periodseq
        and pad.POSITIONTITLE=mes.titlename
        and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
        and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_directsales_mapping(vrptname,'GAPER','ACHIEVEMENT',vPeriodtype) */
        --need to change

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual AVERAGE MEASUREMENT start',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual AVERAGE MEASUREMENT start', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual AVERAGE MEASUREMENT completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual AVERAGE MEASUREMENT completed', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        FOR i AS dbmtk_cursor_7975
        DO
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TEAM PRODUCTS MEASUREMENT start',NULL,i.product) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM PRODUCTS MEASUREMENT start', NULL, :i.product);
            v_sql = 'INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
            (tenantid, positionseq,payeeseq,processingunitseq,periodseq,periodname,processingunitname,calendarname,
                reportcode,sectionid,sectionname,subsectionname,sortorder,titlename,loaddttm,allgroups,geid,name,products,
            CONNTARGET,CONNACTUALS,shopname,teamvisible,payable_flag,reportgroup,frequency)
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''87'' reportcode,
            ''02'' sectionid,
            ''TEAM ACHIEVEMENT'' sectionname,
            ''TEAM ACHIEVEMENT'' subsectionname,
            ''01'' sortorder,
            pad.reporttitle titlename,
            SYSDATE,
            ''TEAM PRODUCTS'' allgroups,
            pad.PARTICIPANTID, --geid
            pad.FULLNAME, --name
            product,
            CONNTARGET,
            CONNACTUALS,
            pad.POSITIONGA1 shopname,
            nvl(pad.TITLEGB1,1) teamvisible,
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
            ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
            ''' ||IFNULL(:vPeriodtype,'')||''' frequency
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups allgroups,
                titlemap.titlename,
                ''' ||IFNULL(:i.product,'')||''' product,
                SUM(case when rmap.rptcolumnname = ''TEAMCONNTARGET'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMCONNTARGET', :i.product, :vPeriodtype),'') ||' end) AS CONNTARGET,
                SUM(case when rmap.rptcolumnname = ''TEAMCONNACTUALS'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMCONNACTUALS', :i.product, :vPeriodtype),'') ||' end) AS CONNACTUALS
                FROM ext.rpt_BASE_MEASUREMENT CM,rpt_directsales_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
                WHERE CM.GENERICATTRIBUTE1=rmap.SUBRPTCLMN1
                and CM.GENERICATTRIBUTE3=rmap.SUBRPTCLMN2
                and CM.GENERICATTRIBUTE2=rmap.SUBRPTCLMN3
                and rmap.product=titlemap.product
                and rmap.report_frequency=titlemap.report_frequency
                and rmap.allgroups=titlemap.allgroups
                and rmap.reportname=titlemap.reportname
                and rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                and CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                and CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and rmap.product = '''||IFNULL(:i.product,'')||'''
                and rmap.allgroups = ''TEAM PRODUCTS''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups  ,titlemap.titlename
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.POSITIONTITLE=mes.titlename
            and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
            and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMCONNTARGET',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMCONNACTUALS',i.product,vPeriodtype) */
            --need to change

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TEam PRODUCTS MEASUREMENT Start',NULL,v_sql) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEam PRODUCTS MEASUREMENT Start', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TEam PRODUCTS MEASUREMENT Start',NULL,v_sql) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEam PRODUCTS MEASUREMENT Start', NULL, :v_sql);

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual PRODUCTS MEASUREMENT for Conn Weight',NULL,'POINTSPAYOUT') */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS MEASUREMENT for Conn Weight', NULL, 'POINTSPAYOUT');
            v_sql = 'MERGE INTO ext.rpt_INTPREPAID_INDIVIDUAL rpt using
            (select mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
                mes.periodseq,
                MAX(case when rmap.rptcolumnname = ''TEAMCONNWT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMCONNWT', :i.product, :vPeriodtype),'') ||' end) AS CONNWT,
                SUM(case when rmap.rptcolumnname = ''TEAMCONNACTUALTARGET'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMCONNACTUALTARGET', :i.product, :vPeriodtype),'') ||' end ) AS CONNACTUALTARGET
                FROM ext.rpt_BASE_MEASUREMENT mes,rpt_directsales_mapping rmap
                WHERE mes.GENERICATTRIBUTE1=rmap.SUBRPTCLMN1
                and mes.GENERICATTRIBUTE3=rmap.SUBRPTCLMN2
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
                and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and rmap.product = '''||IFNULL(:i.product,'')||'''
                and rmap.allgroups = ''TEAM PRODUCTS''
                and rmap.report_frequency ='''||IFNULL(:vPeriodtype,'')||'''
                GROUP BY mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
                mes.periodseq,
                rmap.allgroups
            )qtr
            on (rpt.processingunitseq = qtr.processingunitseq
                and rpt.periodseq = qtr.periodseq
                and rpt.positionseq = qtr.positionseq
                and rpt.payeeseq = qtr.payeeseq
                and rpt.PRODUCTS = ''' ||IFNULL(:i.product,'')||'''
                and rpt.allgroups = ''TEAM PRODUCTS''
                and rpt.frequency='''||IFNULL(:vPeriodtype,'')||''')
            when matched then update set rpt.CONNWT=(qtr.CONNWT*100),rpt.CONNACTUALTARGET=(qtr.CONNACTUALTARGET*100)';  /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMCONNWT',i.product,vPeriodtype) */
                                                                                                                                                                                                                        /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMCONNACTUALTARGET',i.product,vPeriodtype) */

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Individual PRODUCTS MEASUREMENT for Conn Weight start',NULL,v_sql) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Individual PRODUCTS MEASUREMENT for Conn Weight start', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Individual PRODUCTS MEASUREMENT for Conn Weight completed',NULL,v_sql) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Individual PRODUCTS MEASUREMENT for Conn Weight completed', NULL, :v_sql);

            /* ORIGSQL: COMMIT; */
            COMMIT;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

        ----------TEAM AVERAGE-MEASUREMENT ... INDIVIDUAL

        v_sql = 'INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
        (tenantid,
            positionseq,
            payeeseq,
            processingunitseq,
            periodseq,
            periodname,
            processingunitname,
            calendarname,
            reportcode,
            sectionid,
            sectionname,
            subsectionname,
            sortorder,
            titlename,
            loaddttm,
            allgroups,
            geid,
            name,
            products,
            GAPER,
            shopname,
            teamvisible,
            payable_flag,
            reportgroup,
            frequency
        )
        SELECT   ''' ||IFNULL(:vTenantId,'')||''',
        pad.positionseq,
        pad.payeeseq,
        ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
        ' ||IFNULL(:vperiodseq,'')||',
        ''' ||IFNULL(:vPeriodRow.name,'')||''',
        ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
        ''' ||IFNULL(:vCalendarRow.name,'')||''',
        ''87'' reportcode,
        ''02'' sectionid,
        ''TEAM ACHIEVEMENT'' sectionname,
        ''AVERAGE ACHIEVEMENT'' subsectionname,
        ''99'' sortorder,
        pad.reporttitle titlename,
        SYSDATE,
        ''TEAM PAYOUT'' allgroups,
        pad.PARTICIPANTID, --geid
        pad.FULLNAME, --name
        ''ACHIEVEMENT'' product,
        (GAPER*100) GAPER,
        pad.POSITIONGA1 shopname,
        nvl(pad.TITLEGB1,1) teamvisible,
        ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
        ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
        ''' ||IFNULL(:vPeriodtype,'')||''' FREQUENCY
        FROM   ext.rpt_base_padimension pad,
        (
            SELECT
            INC.positionseq,
            INC.payeeseq,
            INC.processingunitseq,
            INC.periodseq,
            rmap.allgroups allgroups,
            titlemap.titlename,
            MAX(case when rmap.rptcolumnname = ''TEAMGAPER'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMGAPER', 'ACHIEVEMENT', :vPeriodtype),'') ||' end) AS GAPER
            FROM ext.rpt_BASE_INCENTIVE INC,rpt_directsales_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
            WHERE INC.GENERICATTRIBUTE1 in rmap.rulename
            and rmap.product=titlemap.product
            and rmap.allgroups=titlemap.allgroups
            and rmap.report_frequency=titlemap.report_frequency
            and rmap.reportname=titlemap.reportname
            AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
            and INC.periodseq = '||IFNULL(:vperiodseq,'')||'
            and INC.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and rmap.product = ''ACHIEVEMENT''
            and rmap.allgroups = ''TEAM AVERAGE''
            GROUP BY INC.positionseq,
            INC.payeeseq,
            INC.processingunitseq,
            INC.periodseq,
            rmap.allgroups ,titlemap.titlename
        )mes
        WHERE       pad.payeeseq = mes.payeeseq
        AND pad.positionseq = mes.positionseq
        AND pad.processingunitseq = mes.processingunitseq
        and pad.periodseq = mes.periodseq
        and pad.POSITIONTITLE=mes.titlename
        and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
        and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMGAPER','ACHIEVEMENT',vPeriodtype) */
        --need to change

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual AVERAGE MEASUREMENT start',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual AVERAGE MEASUREMENT start', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual AVERAGE MEASUREMENT completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual AVERAGE MEASUREMENT completed', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --Incentive individual   ...Individual

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual PRODUCTS INCENTIVE',NULL,NULL) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS INCENTIVE', NULL, NULL);
        v_sql = 'MERGE INTO ext.rpt_INTPREPAID_INDIVIDUAL rpt using
        (select inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
            MAX(case when rmap.rptcolumnname = ''OTC'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'OTC', 'IndividualAch', :vPeriodtype),'') ||' end) AS OTC,
            MAX(case when rmap.rptcolumnname = ''ACHIEVEMENTPER'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'ACHIEVEMENTPER', 'IndividualAch', :vPeriodtype),'') ||' end) AS ACHIEVEMENTPER
            from ext.rpt_base_incentive inc, ext.rpt_directsales_mapping rmap
            where inc.processingunitseq = ' ||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and inc.name in rmap.rulename
            and rmap.reportname = '''||IFNULL(:vrptname,'')||'''
            and inc.periodseq = '||IFNULL(:vperiodseq,'')||'
            and rmap.product =''IndividualAch''
            and rmap.allgroups=''INDIVIDUAL PRODUCTS''
            and rmap.report_frequency ='''||IFNULL(:vPeriodtype,'')||'''
            GROUP BY inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq
        )qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
            and rpt.payeeseq = qtr.payeeseq
            and rpt.allgroups = ''INDIVIDUAL PRODUCTS''
            and rpt.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
        )
        when matched then update set rpt.OTC = qtr.OTC, rpt.ACHIEVEMENTPER=(qtr.ACHIEVEMENTPER*100)';  /* ORIGSQL: fun_directsales_mapping(vrptname,'OTC','IndividualAch',vPeriodtype) */
                                                                                                                                                                                                       /* ORIGSQL: fun_directsales_mapping(vrptname,'ACHIEVEMENTPER','IndividualAch',vPeriodtype) */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual PRODUCTS INCENTIVE completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS INCENTIVE completed', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --Incentive Team   ...Team

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual PRODUCTS INCENTIVE',NULL,NULL) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS INCENTIVE', NULL, NULL);
        v_sql = 'MERGE INTO ext.rpt_INTPREPAID_INDIVIDUAL rpt using
        (select inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
            MAX(case when rmap.rptcolumnname = ''TEAMACHIEVEMENTPER'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMACHIEVEMENTPER', 'TeamAch', :vPeriodtype),'') ||' end) AS ACHIEVEMENTPER
            from ext.rpt_base_incentive inc, ext.rpt_directsales_mapping rmap
            where inc.processingunitseq = ' ||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and inc.name in rmap.rulename
            and rmap.reportname = '''||IFNULL(:vrptname,'')||'''
            and inc.periodseq = '||IFNULL(:vperiodseq,'')||'
            and rmap.product =''TeamAch''
            and rmap.allgroups=''TEAM PRODUCTS''
            and rmap.report_frequency ='''||IFNULL(:vPeriodtype,'')||'''
            GROUP BY inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq
        )qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
            and rpt.payeeseq = qtr.payeeseq
            and rpt.allgroups = ''TEAM PRODUCTS''
            and rpt.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
        )
        when matched then update set rpt.ACHIEVEMENTPER=(qtr.ACHIEVEMENTPER*100)';  /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMACHIEVEMENTPER','TeamAch',vPeriodtype) */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Team PRODUCTS INCENTIVE completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Team PRODUCTS INCENTIVE completed', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --EARNED commission --
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin team Commission incentive',NULL,'Individual') */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin team Commission incentive', NULL, 'Individual');
        v_sql = 'INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
        (tenantid,
            positionseq,
            payeeseq,
            processingunitseq,
            periodseq,
            periodname,
            processingunitname,
            calendarname,
            reportcode,
            sectionid,
            sectionname,
            subsectionname,
            sortorder,
            titlename,
            loaddttm,
            allgroups,
            geid,
            name,
            products,
            OVERALLPER,
            BASICSAMT,
            MULTIPLIERAMT,
            TOTAL,
            shopname,
            teamvisible,
            payable_flag,
            reportgroup,
            frequency
        )
        SELECT   ''' ||IFNULL(:vTenantId,'')||''',
        pad.positionseq,
        pad.payeeseq,
        ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
        ' ||IFNULL(:vperiodseq,'')||',
        ''' ||IFNULL(:vPeriodRow.name,'')||''',
        ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
        ''' ||IFNULL(:vCalendarRow.name,'')||''',
        ''87'' reportcode,
        ''03'' sectionid,
        ''OVERALL COMMISSION'' sectionname,
        ''EARNED COMMISSION'' subsectionname,
        ''01'' sortorder,
        pad.reporttitle titlename,
        SYSDATE,
        ''EARNED COMMISSION'' allgroups,
        pad.PARTICIPANTID, --geid
        pad.FULLNAME, --name
        ''Individual'' product,
        (OVERALLPER*100) OVERALLPER,
        BASICSAMT,
        MULTIPLIERAMT,
        TOTAL,
        pad.POSITIONGA1 shopname,
        nvl(pad.TITLEGB1,1) teamvisible,
        ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
        ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
        ''' ||IFNULL(:vPeriodtype,'')||''' FREQUENCY
        FROM   ext.rpt_base_padimension pad,
        (
            SELECT
            inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
            rmap.allgroups allgroups,
            titlemap.titlename,
            SUM(case when rmap.rptcolumnname = ''OVERALLPER'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'OVERALLPER', 'Individual', :vPeriodtype),'') ||' end) AS OVERALLPER,
            SUM(case when rmap.rptcolumnname = ''BASICSAMT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'BASICSAMT', 'Individual', :vPeriodtype),'') ||' end) AS BASICSAMT,
            SUM(case when rmap.rptcolumnname = ''MULTIPLIERAMT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'MULTIPLIERAMT', 'Individual', :vPeriodtype),'') ||' end) AS MULTIPLIERAMT,
            SUM(case when rmap.rptcolumnname = ''TOTAL'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TOTAL', 'Individual', :vPeriodtype),'') ||' end) AS TOTAL
            FROM ext.rpt_base_incentive inc,rpt_directsales_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
            WHERE inc.GENERICATTRIBUTE1=rmap.SUBRPTCLMN1
            and rmap.product=titlemap.product
            and rmap.allgroups=titlemap.allgroups
            and rmap.report_frequency=titlemap.report_frequency
            and rmap.reportname=titlemap.reportname
            AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
            and inc.periodseq = '||IFNULL(:vperiodseq,'')||'
            and inc.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and rmap.product = ''Individual''
            and rmap.allgroups = ''EARNED COMMISSION''
            GROUP BY inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
            rmap.allgroups  ,titlemap.titlename
        )qtr
        WHERE       pad.payeeseq = qtr.payeeseq
        AND pad.positionseq = qtr.positionseq
        AND pad.processingunitseq = qtr.processingunitseq
        and pad.periodseq = qtr.periodseq
        and pad.POSITIONTITLE=qtr.titlename
        and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
        and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_directsales_mapping(vrptname,'TOTAL','Individual',vPeriodtype) */
                                                                     /* ORIGSQL: fun_directsales_mapping(vrptname,'OVERALLPER','Individual',vPeriodtype) */
                                                                     /* ORIGSQL: fun_directsales_mapping(vrptname,'MULTIPLIERAMT','Individual',vPeriodtype) */
                                                                     /* ORIGSQL: fun_directsales_mapping(vrptname,'BASICSAMT','Individual',vPeriodtype) */
        --need to change

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Overall Commission incentive Individual start',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Overall Commission incentive Individual start', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Overall Commission incentive Individual completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Overall Commission incentive Individual completed', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --Earned Commission Individual First column %
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual Earned Commission %',NULL,'Percentage') */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual Earned Commission %', NULL, 'Percentage');
        v_sql = 'MERGE INTO ext.rpt_INTPREPAID_INDIVIDUAL rpt using
        (select inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
            MAX(case when rmap.rptcolumnname = ''ACHIEVEMENTPER'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'ACHIEVEMENTPER', 'Individual', :vPeriodtype),'') ||' end) AS ACHIEVEMENTPER
            FROM ext.rpt_base_incentive inc,rpt_directsales_mapping rmap
            WHERE inc.name in rmap.rulename
            AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
            and inc.periodseq = '||IFNULL(:vperiodseq,'')||'
            and inc.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and rmap.product = ''Individual''
            and rmap.allgroups = ''EARNED COMMISSION''
            and rmap.report_frequency ='''||IFNULL(:vPeriodtype,'')||'''
            GROUP BY inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq
        )qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
            and rpt.payeeseq = qtr.payeeseq
            and rpt.allgroups = ''EARNED COMMISSION''
            and rpt.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
        and rpt.products = ''Individual'')
        when matched then update set rpt.ACHIEVEMENTPER=(qtr.ACHIEVEMENTPER*100)';  /* ORIGSQL: fun_directsales_mapping(vrptname,'ACHIEVEMENTPER','Individual',vPeriodtype) */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Individual Earned Commission % start',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Individual Earned Commission % start', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Individual Earned Commission % completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Individual Earned Commission % completed', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Overall Commission incentive',NULL,'Team') */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Overall Commission incentive', NULL, 'Team');
        v_sql = 'INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
        (tenantid,
            positionseq,
            payeeseq,
            processingunitseq,
            periodseq,
            periodname,
            processingunitname,
            calendarname,
            reportcode,
            sectionid,
            sectionname,
            subsectionname,
            sortorder,
            titlename,
            loaddttm,
            allgroups,
            geid,
            name,
            products,
            OVERALLPER,
            BASICSAMT,
            MULTIPLIERAMT,
            TOTAL,
            shopname,
            teamvisible,
            payable_flag,
            reportgroup,
            frequency
        )
        SELECT   ''' ||IFNULL(:vTenantId,'')||''',
        pad.positionseq,
        pad.payeeseq,
        ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
        ' ||IFNULL(:vperiodseq,'')||',
        ''' ||IFNULL(:vPeriodRow.name,'')||''',
        ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
        ''' ||IFNULL(:vCalendarRow.name,'')||''',
        ''87'' reportcode,
        ''03'' sectionid,
        ''OVERALL COMMISSION'' sectionname,
        ''EARNED COMMISSION'' subsectionname,
        ''01'' sortorder,
        pad.reporttitle titlename,
        SYSDATE,
        ''EARNED COMMISSION'' allgroups,
        pad.PARTICIPANTID, --geid
        pad.FULLNAME, --name
        ''Team'' product,
        (OVERALLPER*100) OVERALLPER,
        BASICSAMT,
        MULTIPLIERAMT,
        TOTAL,
        pad.POSITIONGA1 shopname,
        nvl(pad.TITLEGB1,1) teamvisible,
        ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
        ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
        ''' ||IFNULL(:vPeriodtype,'')||''' frequency
        FROM   ext.rpt_base_padimension pad,
        (
            SELECT
            inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
            rmap.allgroups allgroups,
            titlemap.titlename,
            SUM(case when rmap.rptcolumnname = ''TEAMOVERALLPER'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMOVERALLPER', 'Team', :vPeriodtype),'') ||' end) AS OVERALLPER,
            SUM(case when rmap.rptcolumnname = ''TEAMBASICSAMT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMBASICSAMT', 'Team', :vPeriodtype),'') ||' end) AS BASICSAMT,
            SUM(case when rmap.rptcolumnname = ''TEAMMULTIPLIERAMT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMMULTIPLIERAMT', 'Team', :vPeriodtype),'') ||' end) AS MULTIPLIERAMT,
            SUM(case when rmap.rptcolumnname = ''TEAMTOTAL'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMTOTAL', 'Team', :vPeriodtype),'') ||' end) AS TOTAL
            FROM ext.rpt_base_incentive inc,rpt_directsales_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
            WHERE inc.GENERICATTRIBUTE1=rmap.SUBRPTCLMN1
            and rmap.product=titlemap.product
            and rmap.allgroups=titlemap.allgroups
            and rmap.report_frequency=titlemap.report_frequency
            and rmap.reportname=titlemap.reportname
            AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
            and inc.periodseq = '||IFNULL(:vperiodseq,'')||'
            and inc.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and rmap.product = ''Team''
            and rmap.allgroups = ''EARNED COMMISSION''
            GROUP BY inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
            rmap.allgroups  ,titlemap.titlename
        )qtr
        WHERE       pad.payeeseq = qtr.payeeseq
        AND pad.positionseq = qtr.positionseq
        AND pad.processingunitseq = qtr.processingunitseq
        and pad.periodseq = qtr.periodseq
        and pad.POSITIONTITLE=qtr.titlename
        and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
        and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMTOTAL','Team',vPeriodtype) */
                                                                     /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMOVERALLPER','Team',vPeriodtype) */
                                                                     /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMMULTIPLIERAMT','Team',vPeriodtype) */
                                                                     /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMBASICSAMT','Team',vPeriodtype) */
        --need to change

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Overall Commission incentive Team start',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Overall Commission incentive Team start', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Overall Commission incentive Team completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Overall Commission incentive Team completed', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --Earned Commission Team First column %
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual Earned Commission %',NULL,'Percentage') */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual Earned Commission %', NULL, 'Percentage');
        v_sql = 'MERGE INTO ext.rpt_INTPREPAID_INDIVIDUAL rpt using
        (select inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
            MAX(case when rmap.rptcolumnname = ''TEAMACHIEVEMENTPER'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMACHIEVEMENTPER', 'Team', :vPeriodtype),'') ||' end) AS ACHIEVEMENTPER
            FROM ext.rpt_base_incentive inc,rpt_directsales_mapping rmap
            WHERE inc.name in rmap.rulename
            AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
            and inc.periodseq = '||IFNULL(:vperiodseq,'')||'
            and inc.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and rmap.product = ''Team''
            and rmap.allgroups = ''EARNED COMMISSION''
            and rmap.report_frequency ='''||IFNULL(:vPeriodtype,'')||'''
            GROUP BY inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq
        )qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
            and rpt.payeeseq = qtr.payeeseq
            and rpt.allgroups = ''EARNED COMMISSION''
            and rpt.products = ''Team''
            and rpt.frequency=''' ||IFNULL(:vPeriodtype,'')||''')
        when matched then update set rpt.ACHIEVEMENTPER=(qtr.ACHIEVEMENTPER*100)';  /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMACHIEVEMENTPER','Team',vPeriodtype) */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Individual Earned Commission % start',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Individual Earned Commission % start', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Individual Earned Commission % completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Individual Earned Commission % completed', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --Earned Commission.. Overall Commission
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL SECTION EARNED COMISSION',NULL,'Earned Commision') */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL SECTION EARNED COMISSION', NULL, 'Earned Commision');
        v_sql = 'INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
        (tenantid,
            positionseq,
            payeeseq,
            processingunitseq,
            periodseq,
            periodname,
            processingunitname,
            calendarname,
            reportcode,
            sectionid,
            sectionname,
            subsectionname,
            sortorder,
            titlename,
            loaddttm,
            allgroups,
            geid,
            name,
            products,
            shopname,
            teamvisible,
            payable_flag,
            SECTION_COMMISSION,
            reportgroup,
            frequency
        )
        SELECT   ''' ||IFNULL(:vTenantId,'')||''',
        pad.positionseq,
        pad.payeeseq,
        ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
        ' ||IFNULL(:vperiodseq,'')||',
        ''' ||IFNULL(:vPeriodRow.name,'')||''',
        ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
        ''' ||IFNULL(:vCalendarRow.name,'')||''',
        ''87'' reportcode,
        ''03'' sectionid,
        ''OVERALL COMMISSION'' sectionname,
        ''EARNED COMMISSION'' subsectionname,
        ''99'' sortorder,
        pad.reporttitle titlename,
        SYSDATE,
        ''EARNED COMMISSION'' allgroups,
        pad.PARTICIPANTID, --geid
        pad.FULLNAME, --name
        ''EARNED COMMISSION'' product,
        pad.POSITIONGA1 shopname,
        nvl(pad.TITLEGB1,1) teamvisible,
        ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
        (nvl(EARNEDCOMM1,0)+nvl(EARNEDCOMM2,0)) SECTION_COMMISSION,
        ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
        ''' ||IFNULL(:vPeriodtype,'')||''' frequency
        FROM   ext.rpt_base_padimension pad,
        (
            SELECT
            INC.positionseq,
            INC.payeeseq,
            INC.processingunitseq,
            INC.periodseq,
            rmap.allgroups allgroups,
            titlemap.titlename,
            MAX(case when rmap.rptcolumnname = ''EARNEDCOMM1'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'EARNEDCOMM1', 'Earned Commission', :vPeriodtype),'') ||' end) AS EARNEDCOMM1,
            MAX(case when rmap.rptcolumnname = ''EARNEDCOMM2'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'EARNEDCOMM2', 'Earned Commission', :vPeriodtype),'') ||' end) AS EARNEDCOMM2
            FROM ext.rpt_BASE_INCENTIVE INC,rpt_directsales_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
            WHERE INC.GENERICATTRIBUTE1 in rmap.rulename
            and rmap.product=titlemap.product
            and rmap.allgroups=titlemap.allgroups
            and rmap.report_frequency=titlemap.report_frequency
            and rmap.reportname=titlemap.reportname
            AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
            and INC.periodseq = '||IFNULL(:vperiodseq,'')||'
            and INC.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and rmap.product = ''Earned Commission''
            and rmap.allgroups = ''EARNED COMMISSION''
            GROUP BY INC.positionseq,
            INC.payeeseq,
            INC.processingunitseq,
            INC.periodseq,
            rmap.allgroups ,titlemap.titlename
        )mes
        WHERE       pad.payeeseq = mes.payeeseq
        AND pad.positionseq = mes.positionseq
        AND pad.processingunitseq = mes.processingunitseq
        and pad.periodseq = mes.periodseq
        and pad.POSITIONTITLE=mes.titlename
        and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
        and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_directsales_mapping(vrptname,'EARNEDCOMM2','Earned Commission',vPeriodtype) */
                                                                     /* ORIGSQL: fun_directsales_mapping(vrptname,'EARNEDCOMM1','Earned Commission',vPeriodtype) */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'OVERALL SECTION EARNED COMISSION TOTAL Start',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'OVERALL SECTION EARNED COMISSION TOTAL Start', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'OVERALL SECTION EARNED COMISSION TOTAL Completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'OVERALL SECTION EARNED COMISSION TOTAL Completed', NULL, :v_sql);

        FOR i AS dbmtk_cursor_7978
        DO
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION PRODUCTS Deposit',NULL,i.product) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION PRODUCTS Deposit', NULL, :i.product);
            v_sql = 'INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
            (tenantid,
                positionseq,
                payeeseq,
                processingunitseq,
                periodseq,
                periodname,
                processingunitname,
                calendarname,
                reportcode,
                sectionid,
                sectionname,
                subsectionname,
                sortorder,
                titlename,
                loaddttm,
                allgroups,
                geid,
                name,
                products,
                CECOMM,
                shopname,
                teamvisible,
                payable_flag,
                reportgroup,
                frequency
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''85'' reportcode,
            ''04'' sectionid,
            ''ADJUST COMMISSION'' sectionname,
            ''ADJUST COMMISSION'' subsectionname,
            ''01'' sortorder,
            pad.reporttitle titlename,
            SYSDATE,
            ''ADJUST COMMISSION'' allgroups,
            pad.PARTICIPANTID, --geid
            pad.FULLNAME, --name
            product,
            CECOMM,
            pad.POSITIONGA1 shopname,
            nvl(pad.TITLEGB1,1) teamvisible,
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
            ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
            ''' ||IFNULL(:vPeriodtype,'')||''' frequency
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                titlemap.titlename,
                rmap.allgroups allgroups,
                ''' ||IFNULL(:i.product,'')||''' product,
                MAX(case when rmap.rptcolumnname = ''CECOMM'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CECOMM', :i.product, :vPeriodtype),'') ||' end) AS CECOMM
                FROM ext.rpt_base_deposit CM,rpt_directsales_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
                WHERE CM.name in rmap.rulename
                
                and rmap.product=titlemap.product
                and rmap.allgroups=titlemap.allgroups
                and rmap.reportname=titlemap.reportname
                and rmap.report_frequency=titlemap.report_frequency
                
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                and CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                and CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and rmap.product = '''||IFNULL(:i.product,'')||'''
                and rmap.allgroups = ''ADJUST COMMISSION''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups ,titlemap.titlename
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.POSITIONTITLE=mes.titlename
            and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
            and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_directsales_mapping(vrptname,'CECOMM',i.product,vPeriodtype) */
            --need to change

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Completed ADJUST COMMISSION PRODUCTS Deposit',NULL,v_sql) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Completed ADJUST COMMISSION PRODUCTS Deposit', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

        --ADJUST commission BALANCE ..Prior Balance Adjustment
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Balance Prior Balance Adjustment',NULL,'Balance Prior Balance Adjustment') */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION Balance Prior Balance Adjustment', NULL, 'Balance Prior Balance Adjustment');

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_BALANCE' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_BALANCEPAYMENTTRACE' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PAYMENT' not found */
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_INTPREPAID_INDIVIDUAL' not found */

        /* ORIGSQL: INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname, processingunitname, calendarname, reportcode, sectionid, sectionname,subsectio(...) */
        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_BASE_PADIMENSION' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_TITLE_PRODUCT_MAPPING' not found */
        INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, subsectionname,
                sortorder, titlename, loaddttm, allgroups, geid, name,
                products, CECOMM, shopname, teamvisible, payable_flag, reportgroup,
                frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processingunitseq, vperiodseq,vPeriodRow.name,vProcessingUnitRow.name,:vCalendarRow.name,'85' reportcode, '04' sectionid,'ADJUST COMMI(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '85' AS reportcode,
                '04' AS sectionid,
                'ADJUST COMMISSION' AS sectionname,
                'ADJUST COMMISSION' AS subsectionname,
                '05' AS sortorder,
                pad.reporttitle AS titlename,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                'ADJUST COMMISSION' AS allgroups,
                pad.PARTICIPANTID,
                pad.FULLNAME,
                'Prior Balance Adjustment' AS products,
                CECOMM,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1,1) AS teamvisible,  /* ORIGSQL: nvl(pad.TITLEGB1,1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                ext.rpt_base_padimension pad,
                ext.rpt_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select pay.positionseq, pay.payeeseq, pay.processingunitseq, pay.periodseq, SUM(bal.value) CECOMM from cs_balance bal, cs_balancepaymenttrace baltrace, cs_payment pay where bal.periodseq=baltrace.sou(...) */
                        pay.positionseq,
                        pay.payeeseq,
                        pay.processingunitseq,
                        pay.periodseq,
                        SUM(bal.value) AS CECOMM
                    FROM
                        cs_balance bal,
                        cs_balancepaymenttrace baltrace,
                        cs_payment pay
                    WHERE
                        bal.periodseq = baltrace.sourceperiodseq
                        AND baltrace.targetperiodseq = pay.periodseq
                        AND bal.payeeseq = pay.payeeseq
                        AND bal.balanceseq = baltrace.balanceseq
                        AND bal.processingunitseq = pay.processingunitseq
                        AND bal.processingunitseq = baltrace.processingunitseq
                        AND pay.periodseq = :vperiodseq
                        AND pay.processingunitseq = :vprocessingunitseq
                    GROUP BY
                        pay.positionseq,
                        pay.payeeseq,
                        pay.processingunitseq,
                        pay.periodseq
                ) AS mes
            WHERE
                pad.payeeseq = mes.payeeseq
                AND pad.positionseq = mes.positionseq
                AND pad.processingunitseq = mes.processingunitseq
                AND pad.periodseq = mes.periodseq
                AND pad.POSITIONTITLE = titlemap.titlename
                AND titlemap.product = 'Prior Balance Adjustment'
                AND titlemap.allgroups = 'ADJUST COMMISSION'
                AND titlemap.reportname = :vrptname
                AND pad.frequency = titlemap.report_frequency
                AND pad.frequency = :vPeriodtype
                AND pad.reportgroup = :v_reportgroup;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'ADJUST COMMISSION Balance Prior Balance Adjustment Completed',NULL,'Balance Prior Balance Adjustment') */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ADJUST COMMISSION Balance Prior Balance Adjustment Completed', NULL, 'Balance Prior Balance Adjustment');

        /*
        -- Adjustment Commission : CE Adjustment
        prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION MEASUREMENT',NULL,'CE Adjustment');
        v_sql :=
         'INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
                   (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                        processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
                        sortorder,titlename,loaddttm,allgroups,geid,name,
                        products,CECOMM,shopname,teamvisible,payable_flag,reportgroup
                    )
             SELECT   '''||vTenantID||''', pad.positionseq, pad.payeeseq, '||vProcessingUnitRow.processingunitseq||',
                         '||vperiodseq||','''||vPeriodRow.name||''',             '''||vProcessingUnitRow.name||''','''||vCalendarRow.name||''',''59'' reportcode,
                      04 sectionid,''ADJUST COMMISSION'' sectionname,''ADJUST COMMISSION'' subsectionname,
                      02 sortorder, pad.reporttitle titlename, SYSDATE,
                      ''ADJUST COMMISSION'' allgroups,pad.PARTICIPANTID,pad.FULLNAME,''CE Adjustment'' products, CECOMM,
                      pad.POSITIONGA1 shopname,nvl(pad.TITLEGB1,1) teamvisible,'||v_payableflag||' payable_flag,'''||v_reportgroup||''' reportgroup
             FROM   ext.rpt_base_padimension pad,
               (   select mes.positionseq,
                                   mes.payeeseq,
                                   mes.processingunitseq,
                                   mes.periodseq,
                                          titlemap.titlename,
                                   MAX(case when rmap.rptcolumnname = ''CECOMM'' then '||fun_sts_mapping(vrptname,'CECOMM','CE Adjustment')||' end) AS CECOMM
                                   from ext.rpt_BASE_MEASUREMENT mes,rpt_directsales_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
                              WHERE mes.name in rmap.rulename
             and rmap.product=titlemap.product
             and rmap.allgroups=titlemap.allgroups
             and rmap.report_frequency=titlemap.report_frequency
             and rmap.reportname=titlemap.reportname
             AND rmap.reportname= '''||vrptname||'''
             and mes.periodseq = '||vperiodseq||'
             and mes.processingunitseq = '||vprocessingunitseq||'
             and rmap.product = ''CE Adjustment''
             and rmap.allgroups = ''ADJUST COMMISSION''
                                group by mes.positionseq,
                                     mes.payeeseq,
                                     mes.processingunitseq,
                                     mes.periodseq  ,titlemap.titlename
                )mes
             WHERE pad.payeeseq = mes.payeeseq
         AND pad.positionseq = mes.positionseq
         AND pad.processingunitseq = mes.processingunitseq
         and pad.periodseq = mes.periodseq
         and pad.POSITIONTITLE=mes.titlename
         and pad.reportgroup = '''||v_reportgroup||'''';
        
          EXECUTE IMMEDIATE v_sql;
           prc_logevent (vPeriodRow.name,vProcName,'END ADJUST COMMISSION MEASUREMENT',NULL,v_sql);
        
         COMMIT;
        
        --CE Adjustment merge
        prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION Incentive for Individual',NULL,'Individual Payout');
         MERGE INTO ext.rpt_INTPREPAID_INDIVIDUAL rpt using
                        (select cre.positionseq,
                                   cre.payeeseq,
                                   cre.processingunitseq,
                                   cre.periodseq,
                                   max(cre.genericattribute4) REMARKS,
                                   sum(cre.genericnumber1) CEACTUAL,
                                   sum(cre.value) CEADJ
                            from ext.rpt_base_credit cre
                            where cre.processingunitseq = vprocessingunitseq
             and cre.periodseq = vperiodseq
             and cre.credittypeid = pkg_reporting_extract_r2.vcredittypeid_CEAdj
                            group by cre.positionseq,
                                     cre.payeeseq,
                                     cre.processingunitseq,
                                 cre.periodseq)qtr
         on (rpt.processingunitseq = qtr.processingunitseq
             and rpt.periodseq = qtr.periodseq
             and rpt.positionseq = qtr.positionseq
             and rpt.payeeseq = qtr.payeeseq
             and rpt.allgroups='ADJUST COMMISSION'
             and rpt.products='CE Adjustment'
          )
         when matched then update set rpt.REMARKS =qtr.REMARKS,rpt.CEACTUAL=qtr.CEACTUAL,rpt.CEADJ=qtr.CEADJ;
        
        prc_logevent (vPeriodRow.name,vProcName,'Begin Adjust commssion  credit for CE Adjustment start',NULL,'CE Adjustment Start');
        COMMIT;
        prc_logevent (vPeriodRow.name,vProcName,'Begin Adjust commssion  credit for CE Adjustment complete',NULL,'CE Adjustment complete');
        */

        ---ADJUST commission-- Total Adjustment
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST commission Total Adjustment',NULL,'Total Adjustment') */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST commission Total Adjustment', NULL, 'Total Adjustment');

        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_INTPREPAID_INDIVIDUAL' not found */
        /* ORIGSQL: INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname, processingunitname, calendarname, reportcode, sectionid, sectionname,subsectio(...) */
        INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, subsectionname,
                sortorder, titlename, loaddttm, geid, name, allgroups,
                products, SECTION_COMMISSION, shopname, teamvisible, payable_flag, reportgroup,
                frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processingunitseq, vperiodseq,vPeriodRow.name,:vProcessingUnitRow.name,:vCalendarRow.name,'85' reportcode, '04' sectionid,'ADJUST COMMI(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '85' AS reportcode,
                '04' AS sectionid,
                'ADJUST COMMISSION' AS sectionname,
                'ADJUST COMMISSION' AS subsectionname,
                '08' AS sortorder,
                pad.reporttitle AS titlename,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                pad.PARTICIPANTID,
                pad.FULLNAME,
                'ADJUST REMARKS' AS allgroups,
                'Total Adjustment' AS products,
                SECTION_COMMISSION,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1,1) AS teamvisible,  /* ORIGSQL: nvl(pad.TITLEGB1,1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                ext.rpt_base_padimension pad,
                ext.rpt_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, SUM(mes.CECOMM) SECTION_COMMISSION from ext.rpt_INTPREPAID_INDIVIDUAL mes where mes.allgroups='ADJUST COMMISSION' and mes.perio(...) */
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq,
                        SUM(mes.CECOMM) AS SECTION_COMMISSION
                    FROM
                        ext.rpt_INTPREPAID_INDIVIDUAL mes
                    WHERE
                        mes.allgroups = 'ADJUST COMMISSION'
                        AND mes.periodseq = :vperiodseq
                        AND mes.processingunitseq = :vprocessingunitseq
                    GROUP BY
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq
                ) AS mes
            WHERE
                pad.payeeseq = mes.payeeseq
                AND pad.positionseq = mes.positionseq
                AND pad.processingunitseq = mes.processingunitseq
                AND pad.periodseq = mes.periodseq
                AND pad.POSITIONTITLE = titlemap.titlename
                AND pad.frequency = :vPeriodtype
                AND titlemap.product = 'Total Adjustment'
                AND titlemap.allgroups = 'ADJUST REMARKS'
                AND titlemap.reportname = :vrptname
                AND pad.frequency = titlemap.report_frequency
                AND pad.reportgroup = :v_reportgroup;

        /* ORIGSQL: Commit; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'ADJUST commission Total Adjustment Complete',NULL,'Total Adjustment') */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ADJUST commission Total Adjustment Complete', NULL, 'Total Adjustment');

        --ADJUST COMMISSION CREDIT..Remarks
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Credit REMARKS',NULL,'REMARKS') */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION Credit REMARKS', NULL, 'REMARKS');

        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_BASE_CREDIT' not found */
        /* ORIGSQL: INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname, processingunitname, calendarname, reportcode, sectionid, sectionname,subsectio(...) */
        INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, subsectionname,
                sortorder, titlename, loaddttm, allgroups, geid, name,
                products, remarks, shopname, teamvisible, payable_flag, reportgroup,
                frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq,:vProcessingUnitRow.processingunitseq, vperiodseq,vPeriodRow.name,vProcessingUnitRow.name,:vCalendarRow.name,'85' reportcode, '04' sectionid,'ADJUST COMMI(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '85' AS reportcode,
                '04' AS sectionid,
                'ADJUST COMMISSION' AS sectionname,
                'ADJUST COMMISSION' AS subsectionname,
                '09' AS sortorder,
                pad.reporttitle AS titlename,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                'ADJUST REMARKS' AS allgroups,
                pad.PARTICIPANTID,
                pad.FULLNAME,
                'ADJUST REMARKS' AS products,
                REMARKS,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1,1) AS teamvisible,  /* ORIGSQL: nvl(pad.TITLEGB1,1) */
                :v_payableflag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                ext.rpt_base_padimension pad,
                ext.rpt_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, MAX(mes.genericattribute3) Remarks from ext.rpt_base_credit mes where mes.processingunitseq = vprocessingunitseq and mes.period(...) */
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq,
                        MAX(mes.genericattribute3) AS Remarks
                    FROM
                        ext.rpt_base_credit mes
                    WHERE
                        mes.processingunitseq = :vprocessingunitseq
                        AND mes.periodseq = :vperiodseq
                        AND mes.credittypeid = :vcredittypeid_PayAdj  /* ORIGSQL: pkg_reporting_extract_r2.vcredittypeid_PayAdj */
                    GROUP BY
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq
                ) AS mes
            WHERE
                pad.payeeseq = mes.payeeseq
                AND pad.positionseq = mes.positionseq
                AND pad.processingunitseq = mes.processingunitseq
                AND pad.periodseq = mes.periodseq

                AND pad.POSITIONTITLE = titlemap.titlename
                AND pad.frequency = :vPeriodtype
                AND titlemap.product = 'ADJUST REMARKS'
                AND titlemap.allgroups = 'ADJUST REMARKS'
                AND titlemap.reportname = :vrptname
                AND pad.frequency = titlemap.report_frequency
                AND pad.reportgroup = :v_reportgroup;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'ADJUST COMMISSION Credit REMARKS Completed',NULL,'REMARKS') */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ADJUST COMMISSION Credit REMARKS Completed', NULL, 'REMARKS');

        --Total Commission Payout
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Total Commission Payout',NULL,'Total Commission Payout') */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION Total Commission Payout', NULL, 'Total Commission Payout');  

        /* ORIGSQL: INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname, processingunitname, calendarname, reportcode, sectionid, sectionname,subsectio(...) */
        INSERT INTO EXT.RPT_INTPREPAID_INDIVIDUAL
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, subsectionname,
                sortorder, titlename, loaddttm, allgroups, geid, name,
                products, TOTALCOMMISSION, shopname, teamvisible, payable_flag, reportgroup,
                frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq,:vProcessingUnitRow.processingunitseq, vperiodseq,vPeriodRow.name,vProcessingUnitRow.name,:vCalendarRow.name,'85' reportcode, '05' sectionid,'TOTAL COMMIS(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '85' AS reportcode,
                '05' AS sectionid,
                'TOTAL COMMISSION' AS sectionname,
                'OVERALL COMMISSION' AS subsectionname,
                '99' AS sortorder,
                pad.reporttitle AS titlename,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                'ADJUST REMARKS' AS allgroups,
                pad.PARTICIPANTID,
                pad.FULLNAME,
                'Total Commission Payout' AS products,
                TOTALCOMMISSION,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1,1) AS teamvisible,  /* ORIGSQL: nvl(pad.TITLEGB1,1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                ext.rpt_base_padimension pad,
                ext.rpt_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, SUM(mes.SECTION_COMMISSION) TOTALCOMMISSION from ext.rpt_INTPREPAID_INDIVIDUAL mes where mes.allgroups in ('ADJUST REMARKS','EA(...) */
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq,
                        SUM(mes.SECTION_COMMISSION) AS TOTALCOMMISSION
                    FROM
                        ext.rpt_INTPREPAID_INDIVIDUAL mes
                    WHERE
                        mes.allgroups IN ('ADJUST REMARKS','EARNED COMMISSION')
                        AND mes.periodseq = :vperiodseq
                        AND mes.processingunitseq = :vprocessingunitseq
                    GROUP BY
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq
                ) AS mes
            WHERE
                pad.payeeseq = mes.payeeseq
                AND pad.positionseq = mes.positionseq
                AND pad.processingunitseq = mes.processingunitseq
                AND pad.periodseq = mes.periodseq
                AND pad.POSITIONTITLE = titlemap.titlename
                AND pad.frequency = :vPeriodtype
                AND titlemap.product = 'Total Commission Payout'
                AND titlemap.allgroups = 'ADJUST REMARKS'
                AND titlemap.reportname = :vrptname
                AND pad.frequency = titlemap.report_frequency
                AND pad.reportgroup = :v_reportgroup;

        /* ORIGSQL: Commit; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'OVERALL COMMISSION Total Commission Payout Completed',NULL,'Total Commission Payout') */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'OVERALL COMMISSION Total Commission Payout Completed', NULL, 'Total Commission Payout');

        --Update the null OTC,GEID,NAME 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO ext.rpt_INTPREPAID_INDIVIDUAL rpt using (SELECT distinct mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, mes.OTC, mes.GEID, mes.NAME, mes.shopname FROM ext.rpt_INTPREPAID_INDIV(...) */
        MERGE INTO ext.rpt_INTPREPAID_INDIVIDUAL AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select distinct mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, mes.OTC, mes.GEID, mes.NAME, mes.shopname from ext.rpt_INTPREPAID_INDIVIDUAL mes where mes.processingunitseq = vproces(...) */
                    DISTINCT
                    mes.positionseq,
                    mes.payeeseq,
                    mes.processingunitseq,
                    mes.periodseq,
                    mes.OTC,
                    mes.GEID,
                    mes.NAME,
                    mes.shopname
                FROM
                    ext.rpt_INTPREPAID_INDIVIDUAL mes
                WHERE
                    mes.processingunitseq = :vprocessingunitseq
                    AND mes.periodseq = :vperiodseq
                    AND mes.sectionname = 'INDIVIDUAL ACHIEVEMENT'
                    AND mes.subsectionname = 'INDIVIDUAL ACHIEVEMENT'
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.frequency = :vPeriodtype)
        WHEN MATCHED THEN
            UPDATE SET rpt.OTC = qtr.OTC,rpt.GEID = qtr.GEID,rpt.NAME = qtr.NAME,rpt.shopname = qtr.shopname;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION ENABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION ENABLE PARALLEL DML ; */

        --------End Insert---------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (vPeriodRow.name, vProcName, 'Report table insert complete', NULL, vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Report table insert complete', NULL, :vSQLERRM);

        --------Gather stats-------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (vPeriodRow.name, vProcName, 'Start DBMS_STATS', NULL, vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Start DBMS_STATS', NULL, :vSQLERRM);

        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTable (vrpttablename) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2:prc_AnalyzeTable(:vRptTableName);

        --------Turn off Parallel DML-------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END