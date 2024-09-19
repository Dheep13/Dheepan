--------------------------------------------------------
--  DDL for Procedure RPT_ADVT_IND_PS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "RPT_ADVT_IND_PS" 
(
in_rpttype varchar2, 
    IN_PERIODSEQ        IN INTEGER,
    IN_PROCESSINGUNITSEQ    IN INTEGER
)
AS

    v_Tenantid        VARCHAR2(255) := 'STEL';
    v_Calendarname  VARCHAR2(255) := NULL;
    v_calendarseq    INTEGER;
    V_FV5 number;
 v_period cs_period%rowtype;

BEGIN
        
        SELECT NAME,CALENDARSEQ INTO v_Calendarname, v_calendarseq
                FROM CS_CALENDAR
                WHERE NAME = 'Singtel Monthly Calendar';


        SELECT (FV.VALUE*100) INTO V_FV5
    FROM CS_FIXEDVALUE FV, CS_PERIOD PER, CS_CALENDAR C
    WHERE
         FV.EFFECTIVESTARTDATE < PER.ENDDATE
         AND PER.CALENDARSEQ = C.CALENDARSEQ
        AND FV.EFFECTIVEENDDATE >= PER.ENDDATE
        AND FV.REMOVEDATE = to_date('01/01/2200','dd/mm/yyyy')
        AND PER.REMOVEDATE = to_date('01/01/2200','dd/mm/yyyy')
        AND PER.PERIODSEQ = IN_PERIODSEQ
        AND FV.NAME ='FV_Rev Acc Achv Cap'
        AND FV.PERIODTYPESEQ IN(SELECT PERIODTYPESEQ FROM CS_PERIODTYPE WHERE NAME ='year' )
        AND FV.MODELSEQ = 0
        AND C.CALENDARSEQ = V_CALENDARSEQ;


select * into v_period    from cs_period where periodseq=IN_PERIODSEQ
--and calendarseq=2251799813685251
and removedate>sysdate ;


        DELETE FROM STELEXT.STEL_RPT_ADV_IND_PS
        WHERE PERIODSEQ = IN_PERIODSEQ 
        AND PROCESSINGUNITSEQ = IN_PROCESSINGUNITSEQ;

        COMMIT;
        
   STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_ADV_IND_PS'); -- managing table partitions     

---insertion of Incentive values for section -1
------------------------------------------------

INSERT INTO STEL_RPT_ADV_IND_PS
 (
TENANTID,
  PAYEESEQ ,
  POSITIONSEQ,
  PERIODSEQ,
  CALENDARSEQ ,
  PROCESSINGUNITSEQ,
  PROCESSINGUNIT,
  PAYEEID,
  CALENDARNAME,
  SECTION ,
  HIREDATE,
  FIRSTNAME,
  MIDDLENAME,
  LASTNAME ,
  POSITIONNAME ,
  COMMISSION,
  INDIVIUALTARGET_M1,
  TEAMTARGET_M2,
  AR_M3       ,
  ---ANNUAL_TARGET ,
  PERIODNAME  ,
  M1_WEIGHTAGE,
  M2_WEIGHTAGE ,
  M3_WEIGHTAGE,
  YEARLY_CAP ,
  TITLESEQ,
 TITLENAME,
CREATEDATE
)
(
SELECT
 v_Tenantid,
 P.PAYEESEQ,
 P.POSITIONSEQ,
 P.PERIODSEQ,
 v_calendarseq,
 IN_PROCESSINGUNITSEQ,
 P.PROCESSINGUNITNAME,
 P.PAYEEID,
v_Calendarname AS CALENDARNAME,
'1',
P.HIREDATE,
P.FIRSTNAME,
P.MIDDLENAME,
P.LASTNAME,
P.POSITIONNAME,
sum (case when (I.GENERICATTRIBUTE1 ='Advt_M1_Indv' and P.periodname not like 'March%') then I.GENERICNUMBER1 
    when (I.GENERICATTRIBUTE1 in('Advt_M2_Team/SA','Advt_M2_SA') and P.periodname   like 'March%') then I.GENERICNUMBER1 
else 0 end) commission_OTI,
sum (case when (I.GENERICATTRIBUTE1 ='Advt_M1_Indv' and P.periodname not like 'March%') then I.GENERICNUMBER1 
    when (I.GENERICATTRIBUTE1 in('Advt_M2_Team/SA','Advt_M2_SA') and P.periodname   like 'March%') then I.GENERICNUMBER1 
else 0 end) * sum(CASE WHEN  I.GENERICATTRIBUTE1 ='Advt_M1_Indv' THEN I.GENERICNUMBER2 ELSE 0  END)   AS INDIVIDUALTARGET_M1,
sum (case when (I.GENERICATTRIBUTE1 ='Advt_M1_Indv' and P.periodname not like 'March%') then 0
    when (I.GENERICATTRIBUTE1 in('Advt_M2_Team/SA','Advt_M2_SA') and P.periodname   like 'March%') then I.GENERICNUMBER1 
else 0 end) * sum(CASE WHEN  I.GENERICATTRIBUTE1 IN('Advt_M2_Team/SA','Advt_M2_SA') THEN  I.GENERICNUMBER2 ELSE 0    END) as TEAMTARGET_M2,
sum (case when (I.GENERICATTRIBUTE1 ='Advt_M1_Indv' and P.periodname not like 'March%') then 0
    when (I.GENERICATTRIBUTE1 in('Advt_M2_Team/SA','Advt_M2_SA') and P.periodname   like 'March%') then I.GENERICNUMBER1 
else 0 end) * sum(CASE WHEN  I.GENERICATTRIBUTE1 IN('Advt_M3_AR','Advt_M3_PR') THEN  I.GENERICNUMBER2 ELSE 0   END) as AR_M3,
--P.SALARY,
--SUM((CASE WHEN  I.GENERICATTRIBUTE1 ='Advt_M1_Indv' THEN (I.GENERICNUMBER1 * I.GENERICNUMBER2) ELSE 0 END)) AS INDIVIDDUALTARGET_M1,
--SUM((CASE WHEN  I.GENERICATTRIBUTE1 IN('Advt_M2_Team/SA','Advt_M2_SA') THEN (I.GENERICNUMBER1 * I.GENERICNUMBER2) ELSE 0 END)) AS TEAMTARGET_M2,
--SUM((CASE WHEN  I.GENERICATTRIBUTE1 IN('Advt_M3_AR','Advt_M3_PR') THEN  (I.GENERICNUMBER1 * I.GENERICNUMBER2) ELSE 0 END)) AS AR_M3,
---SUM((CASE WHEN I.GENERICATTRIBUTE1 = 'Advt_Yearly_Accelerator' THEN I.GENERICNUMBER6 ELSE 0 END)) AS ANNUAL_TARGET,
P.PERIODNAME AS PERIODNAME,
SUM(((CASE WHEN  I.GENERICATTRIBUTE1 ='Advt_M1_Indv' THEN I.GENERICNUMBER2 ELSE 0  END)*100)) || '%' AS M1_WEIGHTAGE,
SUM(((CASE WHEN  I.GENERICATTRIBUTE1 IN('Advt_M2_Team/SA','Advt_M2_SA') THEN  I.GENERICNUMBER2 ELSE 0    END)*100)) || '%' AS M2_WEIGHTAGE,
SUM(((CASE WHEN  I.GENERICATTRIBUTE1 IN('Advt_M3_AR','Advt_M3_PR') THEN  I.GENERICNUMBER2 ELSE 0   END)*100)) || '%' AS M3_WEIGHTAGE,
--SUM((CASE WHEN  I.GENERICATTRIBUTE1 ='Advt_Yearly_Accelerator' THEN I.GENERICNUMBER3 ELSE 0  END)*100)|| '%' AS YEARLY_CAP,
V_FV5||'%' as YEARLY_CAP,
P.TITLESEQ AS TITLESEQ,
P.TITLE AS TITLENAME,
SYSDATE AS CREATEDATE
FROM  CS_INCENTIVE I, STEL_POSPART_MASTER P
WHERE 
I.PAYEESEQ = P.PAYEESEQ
AND I.POSITIONSEQ = P.POSITIONSEQ
AND I.PERIODSEQ = P.PERIODSEQ
AND P.PERIODSEQ = IN_PERIODSEQ
AND P.PROCESSINGUNIT = IN_PROCESSINGUNITSEQ  
AND I.GENERICATTRIBUTE1 IN('Advt_M1_Indv','Advt_M2_Team/SA','Advt_M3_AR','Advt_Yearly_Accelerator','Advt_M2_SA','Advt_M3_PR')
GROUP BY
P.PAYEESEQ,
 P.POSITIONSEQ,
 P.PERIODSEQ,
P.PROCESSINGUNITNAME,
 P.PAYEEID,
'1',
P.HIREDATE,
P.FIRSTNAME,
P.MIDDLENAME,
P.LASTNAME,
P.POSITIONNAME,
P.PERIODNAME,
V_FV5||'%',
P.TITLESEQ ,
P.TITLE
);
COMMIT;

---updation of measurement values in section- 1
------------------------------------------------


        UPDATE STELEXT.STEL_RPT_ADV_IND_PS T SET 
            MIN_THRESHOLD_PERCENT =(SELECT M.GENERICATTRIBUTE7 FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'),    
            MIN_THRESHOLD_EXTRA_PERCENT = (SELECT M.GENERICATTRIBUTE4 FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'),
            QUARTERLY_CAP = (SELECT M.GENERICATTRIBUTE6 FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'),
            THRESHOLD_PAYMENT = (SELECT M.GENERICATTRIBUTE5 FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'),
        ANNUAL_TARGET = (SELECT M.GENERICNUMBER2 FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt YTD Achv')
            WHERE T.SECTION = 1;
                    

         COMMIT;

---insertion of measurement for section -2
-------------------------------------------
INSERT INTO STEL_RPT_ADV_IND_PS
 (
TENANTID,
  PAYEESEQ ,
  POSITIONSEQ,
  PERIODSEQ,
  CALENDARSEQ ,
  PROCESSINGUNITSEQ,
  PROCESSINGUNIT,
  PAYEEID,
  CALENDARNAME,
  SECTION ,
  FIRSTNAME,
  MIDDLENAME,
  LASTNAME ,
  POSITIONNAME ,
  PERIODNAME  ,
  QUARTERS  ,
  QUARTERACHIEVEMENTS,
  QUARTERTARGETS ,
  QUARTERACHIEVEDPERCENT,
  TITLESEQ,
  TITLENAME,
CREATEDATE ,
sourceperiodseq,sourceperiodname
) 
(SELECT 
V_TENANTID,
       P.PAYEESEQ,
       P.POSITIONSEQ,
       v_period.periodseq,
--       qtdata.PERIODSEQ,
V_CALENDARSEQ,
   IN_PROCESSINGUNITSEQ,
       P.PROCESSINGUNITNAME,
       P.PAYEEID,
    V_CALENDARNAME AS CALENDARNAME,
       '2',
 P.FIRSTNAME,
P.MIDDLENAME,
P.LASTNAME,
P.POSITIONNAME,
       -- QTDATA.MONTHNAME AS PERIODNAME,
       v_period.name as periodname,
           QTDATA.QTR AS QUARTERS,  
    SUM(QTDATA.QUARTERACHIEVEMENTS) AS QUARTERACHIEVEMENTS ,
     SUM( QTDATA.QUARTERTARGETS) AS QUARTERTARGETS ,
  SUM((QTDATA.QUARTERACHIEVEDPERCENT)*100) AS QUARTERACHIEVEDPERCENT,
         P.TITLESEQ AS TITLESEQ,
P.TITLE AS TITLENAME,
SYSDATE AS CREATEDATE,
QTDATA.PERIODSEQ ,QTDATA.MONTHNAME 
  FROM STEL_POSPART_MASTER P,
                                             (SELECT POSITIONSEQ, PERIODSEQ, QTRNAME, PAYEESEQ, SUM(QUARTERACHIEVEMENTS) AS QUARTERACHIEVEMENTS,
                                             SUM(QUARTERTARGETS) AS QUARTERTARGETS,SUM(QUARTERACHIEVEDPERCENT) AS QUARTERACHIEVEDPERCENT,
                                             QTR,ENDDATE, MONTHNAME,GENERICATTRIBUTE1
                                                FROM
                                               (SELECT MES.POSITIONSEQ, QTR.ENDDATE AS ENDDATE, QTR.NAME AS QTRNAME, MES.PAYEESEQ,PRD.NAME AS MONTHNAME,GENERICATTRIBUTE1,
                                                MES.GENERICNUMBER2 AS QUARTERACHIEVEMENTS, MES.GENERICNUMBER3 AS QUARTERTARGETS, MES.GENERICNUMBER4 AS QUARTERACHIEVEDPERCENT, MES.PERIODSEQ, SUBSTR(QTR.NAME,0,2) AS QTR
                                                 FROM CS_MEASUREMENT MES, CS_PERIOD PRD, CS_PERIOD QTR
                                                              WHERE MES.PERIODSEQ = PRD.PERIODSEQ
                                                              AND PRD.PARENTSEQ = QTR.PERIODSEQ
                                                              and prd.periodseq in (SELECT periodseq
   FROM (    SELECT periodseq,
                    parentseq,
                    startdate,
                    enddate,
                    name,
                    periodtype
               FROM (SELECT pd.*, pt.name periodtype, cal.name calendarname
                       FROM cs_period pd, cs_periodtype pt, cs_calendar cal
                      WHERE     pd.periodtypeseq = pt.periodtypeseq
                            AND pt.removedate > SYSDATE
                            AND pd.calendarseq = cal.calendarseq
                            AND cal.removedate > SYSDATE
                            AND pd.removedate > SYSDATE) src
         START WITH periodseq = (SELECT periodseq
                                   FROM (    SELECT periodseq,
                                                    parentseq,
                                                    startdate,
                                                    enddate,
                                                    name,
                                                    periodtype
                                               FROM (SELECT pd.*,
                                                            pt.name periodtype,
                                                            cal.name calendarname
                                                       FROM cs_period pd,
                                                            cs_periodtype pt,
                                                            cs_calendar cal
                                                      WHERE pd.periodtypeseq =
                                                               pt.periodtypeseq
                                                            AND pt.removedate >
                                                                  SYSDATE
                                                            AND pd.calendarseq =
                                                                  cal.calendarseq
                                                            AND cal.removedate >
                                                                  SYSDATE
                                                            AND pd.removedate >
                                                                  SYSDATE) src
                                         START WITH periodseq =
                                                       v_period.periodseq -- Parameter 1
                                         CONNECT BY PRIOR parentseq =
                                                       periodseq)
                                  WHERE periodtype = 'year')
         CONNECT BY PRIOR periodseq = parentseq)
  WHERE periodtype = 'month' 
  AND enddate <= v_period.enddate   -- Parameter 2
)                                                                  AND PRD.REMOVEDATE = TO_DATE ('01/01/2200', 'MM/DD/YYYY')
                                                                    AND QTR.REMOVEDATE = TO_DATE ('01/01/2200', 'MM/DD/YYYY')
                                                                     AND MES.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'
                                                                             )
                                                                 GROUP BY POSITIONSEQ, PERIODSEQ, PAYEESEQ, QTR,ENDDATE, QTRNAME, MONTHNAME,GENERICATTRIBUTE1 ) QTDATA
        WHERE         QTDATA.PAYEESEQ = P.PAYEESEQ
AND QTDATA.POSITIONSEQ = P.POSITIONSEQ
AND P.PERIODSEQ = IN_PERIODSEQ
--AND QTDATA.PERIODSEQ IN (SELECT YTDPERIODSEQ FROM cs_periodcalendarytd WHERE periodseq = IN_PERIODSEQ AND calendarname =V_CALENDARNAME)
--AND QTDATA.PERIODSEQ = IN_PERIODSEQ 
AND P.PROCESSINGUNIT = IN_PROCESSINGUNITSEQ
AND QTDATA.GENERICATTRIBUTE1 ='Advt_Qtrly Rev Achv_Payable'
GROUP BY P.PAYEESEQ,
       P.POSITIONSEQ,
       qtdata.PERIODSEQ,
        P.PROCESSINGUNITNAME,
       P.PAYEEID,
        P.FIRSTNAME,
P.MIDDLENAME,
P.LASTNAME,
P.POSITIONNAME,
        '2',
         QTDATA.MONTHNAME,
         QTDATA.QTR,
           P.TITLESEQ ,
           P.TITLE ,
SYSDATE 
);

COMMIT;

--updation of payouts for section-2
-----------------------------------

UPDATE STELEXT.STEL_RPT_ADV_IND_PS T SET PAYOUTS =nvl( (SELECT nvl(i.value,0) FROM CS_INCENTIVE I WHERE 
            I.PERIODSEQ = T.sourceperiodseq AND T.PAYEESEQ = I.PAYEESEQ AND I.GENERICATTRIBUTE1 = 'Advt_M1_Indv' ),0)
                    WHERE T.SECTION = 2
                    and t.periodseq=v_period.periodseq
                    ; 

COMMIT;


----insertion of incentive values for section-3
-------------------------------------------------

INSERT INTO STEL_RPT_ADV_IND_PS
 (
TENANTID,
  PAYEESEQ ,
  POSITIONSEQ,
  PERIODSEQ,
  CALENDARSEQ ,
  PROCESSINGUNITSEQ,
  PROCESSINGUNIT,
  PAYEEID,
  CALENDARNAME,
  SECTION ,
  FIRSTNAME,
  MIDDLENAME,
  LASTNAME ,
  POSITIONNAME ,
  PERIODNAME  ,
  --OVERALL_FY_ACHIEVEMENT,
  ANNUAL_OTI ,
  --YEARLY_ACHIEVEMENTS,
 -- YEARLY_ACHV_PERCENT ,
  PAYOUT ,
  QUARTERS  ,
  M2_IS_ACHIEVED,
  M2_ACHIVED_INC,
  M3_IS_ACHIEVED,
  M3_ACHIEVED_INC,
  TITLESEQ,
TITLENAME,
CREATEDATE,
ANNUAL_ACC_PERCENT,
M1_PAYOUT
)
(
SELECT
v_Tenantid, 
P.PAYEESEQ, 
P.POSITIONSEQ,
 P.PERIODSEQ,
v_calendarseq, 
  IN_PROCESSINGUNITSEQ, 
  P.PROCESSINGUNITNAME,
   P.PAYEEID,
   v_Calendarname AS CALENDARNAME,
   '3',
   P.FIRSTNAME,
P.MIDDLENAME,
P.LASTNAME,
P.POSITIONNAME,
P.PERIODNAME AS  PERIODNAME,
--SUM((CASE WHEN I.GENERICATTRIBUTE1 = 'Advt_Yearly_Accelerator' THEN I.GENERICNUMBER6 ELSE 0 END)) AS OVERALL_FY_ACHIEVEMENTS,
SUM((CASE WHEN I.GENERICATTRIBUTE1  IN( 'Advt_M3_AR','Advt_M3_PR') THEN I.GENERICNUMBER1 ELSE 0 END)) AS ANNUAL_OTI,
--SUM((CASE WHEN I.GENERICATTRIBUTE1 = 'Advt_Yearly_Accelerator' THEN I.GENERICNUMBER5 ELSE 0 END)) AS YEARLY_ACHIEVEMENT,
--SUM((CASE WHEN I.GENERICATTRIBUTE1 = 'Advt_Yearly_Accelerator' THEN I.GENERICNUMBER4 ELSE 0 END)*100) AS YEARLY_ACHIEVED_PERCENT,
SUM((CASE WHEN I.GENERICATTRIBUTE1 = 'Advt_Yearly_Accelerator' THEN I.VALUE ELSE 0 END))  AS PAYOUT,
NULL AS QUARTERS,
MAX(CASE WHEN (I.GENERICATTRIBUTE1 IN('Advt_M2_SA', 'Advt_M2_Team/SA') AND I.GENERICBOOLEAN1 = 1) THEN 'YES' ELSE 'NO' END) AS M2_IS_ACHIEVED,
SUM((CASE WHEN I.GENERICATTRIBUTE1 IN('Advt_M2_SA', 'Advt_M2_Team/SA') THEN I.VALUE ELSE 0 END))AS M2_ACHIEVED_INC,
MAX(CASE WHEN (I.GENERICATTRIBUTE1 IN( 'Advt_M3_AR','Advt_M3_PR') AND I.GENERICBOOLEAN1 = 1) THEN 'YES' ELSE 'NO' END) AS M3_IS_ACHIEVED,
SUM((CASE WHEN I.GENERICATTRIBUTE1  IN( 'Advt_M3_AR','Advt_M3_PR')THEN I.VALUE ELSE 0 END)) AS M3_ACHIEVED_INC,
P.TITLESEQ AS TITLESEQ,
P.TITLE AS TITLENAME ,
SYSDATE AS CREATEDATE,
--(ADVT_IND_PAYMENT.ANNUAL_OTI * 1.2) +(ADVT_IND_PAYMENT.ANNUAL_OTI * 1.6)+(ADVT_IND_PAYMENT.ANNUAL_OTI * 2)+(ADVT_IND_PAYMENT.ANNUAL_OTI * 2.5)+(ADVT_IND_PAYMENT.ANNUAL_OTI * 3) 
MAX((CASE WHEN I.GENERICATTRIBUTE1='Advt_Yearly_Accelerator' THEN I. GENERICNUMBER2 ELSE 0 END )*100) AS ANNUAL_ACC_PERCENT,
null AS M1_PAYOUT
FROM CS_INCENTIVE I, STEL_POSPART_MASTER P
WHERE 
I.PAYEESEQ = P.PAYEESEQ
AND I.POSITIONSEQ = P.POSITIONSEQ
AND I.PERIODSEQ = IN_PERIODSEQ
AND P.PERIODSEQ =IN_PERIODSEQ
AND P.PROCESSINGUNIT = IN_PROCESSINGUNITSEQ  
AND I.GENERICATTRIBUTE1 IN('Advt_M1_Indv','Advt_M2_Team/SA','Advt_M3_AR','Advt_Yearly_Accelerator','Advt_M3_PR','Advt_M2_SA')
GROUP BY 
v_Tenantid, 
P.PAYEESEQ,
P.POSITIONSEQ,
 P.PERIODSEQ,
v_calendarseq, 
 IN_PROCESSINGUNITSEQ, 
 P.PROCESSINGUNITNAME,
P.PAYEEID,
 P.FIRSTNAME,
P.MIDDLENAME,
P.LASTNAME,
P.POSITIONNAME,
  v_Calendarname,
'3',
P.PERIODNAME ,
--(CASE WHEN (I.GENERICATTRIBUTE1 IN('Advt_M2_SA', 'Advt_M2_Team/SA') AND I.GENERICBOOLEAN1 = 1) THEN 'YES' ELSE 'NO' END),
--(CASE WHEN (I.GENERICATTRIBUTE1 IN( 'Advt_M3_AR','Advt_M3_PR') AND I.GENERICBOOLEAN1 = 1) THEN 'YES' ELSE 'NO' END),
P.TITLESEQ ,
P.TITLE ,
SYSDATE
 );

COMMIT;


UPDATE STELEXT.STEL_RPT_ADV_IND_PS T SET   M1_PAYOUT = (select sum(PAYOUTS)  from STEL_RPT_ADV_IND_PS  x where t.periodseq=x.periodseq and t.positionseq=x.positionseq and section=2)
                WHERE T.SECTION = 3 and t.periodseq=v_period.periodseq; 


UPDATE STEL_RPT_ADV_IND_PS  T SET
                OVERALL_FY_ACHIEVEMENT = (SELECT M.GENERICNUMBER2 FROM CS_MEASUREMENT M  WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt YTD Achv'),
                YEARLY_ACHIEVEMENTS = (SELECT M.GENERICNUMBER1 FROM CS_MEASUREMENT M  WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt YTD Achv'),
                YEARLY_ACHV_PERCENT = (SELECT (M.VALUE*100) FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt YTD Achv')
        WHERE T.SECTION = 3;

COMMIT;

--insertion of PR table section-4(sub-report)
---------------------------------------------
INSERT INTO STEL_RPT_ADV_IND_PS
 (
TENANTID,
  PAYEESEQ ,
  POSITIONSEQ,
  PERIODSEQ,
  CALENDARSEQ ,
  PROCESSINGUNITSEQ,
  PROCESSINGUNIT,
  PAYEEID,
  CALENDARNAME,
  SECTION ,
  FIRSTNAME,
  MIDDLENAME,
  LASTNAME ,
  POSITIONNAME ,
  PERIODNAME,
  PR_RATINGS,
  PR_VALUE ,
  TITLESEQ,
  TITLENAME,
CREATEDATE
)
(
SELECT
 v_Tenantid,
       P.PAYEESEQ,
       P.POSITIONSEQ,
       P.PERIODSEQ,
v_calendarseq,
    IN_PROCESSINGUNITSEQ,
       P.PROCESSINGUNITNAME,
       P.PAYEEID,
v_Calendarname AS CALENDARNAME,
       '4',
       P.FIRSTNAME,
P.MIDDLENAME,
P.LASTNAME,
P.POSITIONNAME,
v_period.name as periodname,
('PR' || LKP.DIM0) AS PR_RATINGS,
(LKP.VALUE*100)||'%' AS  PR_VALUE,
P.TITLESEQ AS TITLESEQ,
P.TITLE AS TITLENAME,
SYSDATE AS CREATEDATE 
FROM 
STEL_LOOKUP LKP, STEL_POSPART_MASTER P, CS_PERIOD PER
WHERE
P.PERIODSEQ = PER.PERIODSEQ
AND   lkp.NAME ='LT_Advt_TL/Direc_PR Rank'
AND LKP.EFFECTIVESTARTDATE < PER.ENDDATE
AND LKP.EFFECTIVEENDDATE > PER.STARTDATE
AND PER.REMOVEDATE = TO_DATE('01/01/2200','MM/DD/YYYY')
AND PER.PERIODSEQ = IN_PERIODSEQ
AND P.PROCESSINGUNIT =IN_PROCESSINGUNITSEQ
);

COMMIT;

END;
