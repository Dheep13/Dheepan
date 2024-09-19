CREATE VIEW "EXT"."CTAS_VW_TRXNPAYMENTTRACE" ( "SALESTRANSACTIONSEQ", "POSITIONNAME", "TRXVALUE", "PERIODNAME", "CREDITSEQ", "CREDITNAME", "CREDITVALUE", "PM_CONTRIBUTIONVALUE", "PM_MEASUREMENTSEQ", "PM_NAME", "PM_VALUE", "PMST_CONTRIBUTIONVALUE", "SM_MEASUREMENTSEQ", "SM_NAME", "SM_VALUE", "IPMT_CONTRIBUTIONVALUE", "INCENTIVESEQ", "INC_NAME", "INC_VALUE", "DIT_CONTRIBUTIONVALUE", "DEPOSITSEQ", "DEP_NAME", "DEP_VALUE", "DADT_CONTRIBUTIONVALUE", "ADPT_CONTRIBUTIONVALUE", "EARNINGGROUPID", "EARNINGCODEID", "PAY_VALUE", "PERIOD_STARTDATE", "PERIOD_ENDDATE" ) AS select st.salestransactionseq, pos.name positionname, st.value TRXVALUE, pe.name periodname,
       cr.creditseq, cr.name CREDITNAME, cr.value CREDITVALUE,
       pmct.contributionvalue PM_CONTRIBUTIONVALUE,
       mes_pm.measurementseq PM_MEASUREMENTSEQ, mes_pm.name PM_NAME, mes_pm.value PM_VALUE,
       pmst.contributionvalue PMST_CONTRIBUTIONVALUE,
       mes_sm.measurementseq SM_MEASUREMENTSEQ, mes_sm.name SM_NAME, mes_sm.value SM_VALUE,
       ipmt.contributionvalue IPMT_CONTRIBUTIONVALUE,
       inc.incentiveseq INCENTIVESEQ, inc.name INC_NAME, inc.value INC_VALUE,
       dit.contributionvalue DIT_CONTRIBUTIONVALUE,
       dep.depositseq DEPOSITSEQ, dep.name DEP_NAME, dep.value DEP_VALUE,
       dadt.contributionvalue DADT_CONTRIBUTIONVALUE,
       adpt.contributionvalue ADPT_CONTRIBUTIONVALUE,
       pay.earninggroupid, pay.earningcodeid, pay.value PAY_VALUE,
       pe.startdate period_startdate, pe.enddate period_enddate
from   cs_salestransaction st, 
       cs_credit cr,
       cs_position pos, 
       cs_pmcredittrace pmct, 
       cs_measurement mes_pm,
       cs_pmselftrace pmst,
       cs_measurement mes_sm,
       cs_incentivepmtrace ipmt, 
       cs_incentive inc, 
       cs_depositincentivetrace dit, 
       cs_deposit dep, 
       cs_depositappldeposittrace dadt, 
       cs_appldepositpaymenttrace adpt,
       cs_payment pay,
       cs_period pe,
       cs_calendar cal,
       cs_periodtype pt
where  st.salestransactionseq = cr.salestransactionseq
  and  cr.creditseq = pmct.creditseq
  and  cr.positionseq = pos.ruleelementownerseq
  and  pos.removedate = to_date('1/1/2200','mm/dd/yyyy')
  and  pos.effectivestartdate <= st.compensationdate
  and  pos.effectiveenddate > st.compensationdate
  and  pmct.measurementseq = mes_pm.measurementseq
  and  pmct.measurementseq = pmst.sourcemeasurementseq
  and  pmst.targetmeasurementseq = mes_sm.measurementseq
  and  pmst.targetmeasurementseq = ipmt.measurementseq
  and  ipmt.incentiveseq = inc.incentiveseq
  and  ipmt.incentiveseq = dit.incentiveseq
  and  dit.depositseq = dep.depositseq
  and  dit.depositseq = dadt.depositseq
  and  dadt.applieddepositseq = adpt.applieddepositseq
  and  adpt.paymentseq = pay.paymentseq
  and  pe.removedate = TO_DATE('01/01/2200','MM/DD/YYYY')
  and  pt.removedate = TO_DATE('01/01/2200','MM/DD/YYYY')
  and  cal.removedate = TO_DATE('01/01/2200','MM/DD/YYYY')
  and  cal.calendarseq=pe.calendarseq
  --and  pe.shortname = 'Week'
  and  cal.name='Cintas Hybrid Calendar'
  and  pt.name='week'
  and  pt.periodtypeseq=pe.periodtypeseq
  and  st.compensationdate >= pe.startdate 
  and  st.compensationdate < pe.enddate WITH READ ONLY