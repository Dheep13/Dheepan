--------------------------------------------------------
--  DDL for Procedure RPT_POST_COVERNOTE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "RPT_POST_COVERNOTE" (
   RPTTYPE                IN VARCHAR2,
   in_PERIODSEQ           IN NUMBER,
   in_PROCESSINGUNITSEQ   IN NUMBER)
AS
BEGIN
   EXECUTE IMMEDIATE 'Truncate table STEL_CLASSIFIER_TAB';

   INSERT INTO STEL_CLASSIFIER_TAB (CATEGORYNAME,
                                    CATEGORYTREENAME,
                                    CLASSFIERNAME,
                                    CLASSIFIERID,
                                    COST,
                                    DESCRIPTION,
                                    EFFECTIVEENDDATE,
                                    EFFECTIVESTARTDATE,
                                    GENERICATTRIBUTE1,
                                    GENERICATTRIBUTE10,
                                    GENERICATTRIBUTE11,
                                    GENERICATTRIBUTE12,
                                    GENERICATTRIBUTE13,
                                    GENERICATTRIBUTE14,
                                    GENERICATTRIBUTE15,
                                    GENERICATTRIBUTE16,
                                    GENERICATTRIBUTE2,
                                    GENERICATTRIBUTE3,
                                    GENERICATTRIBUTE4,
                                    GENERICATTRIBUTE5,
                                    GENERICATTRIBUTE6,
                                    GENERICATTRIBUTE7,
                                    GENERICATTRIBUTE8,
                                    GENERICATTRIBUTE9,
                                    GENERICBOOLEAN1,
                                    GENERICBOOLEAN2,
                                    GENERICBOOLEAN3,
                                    GENERICBOOLEAN4,
                                    GENERICBOOLEAN5,
                                    GENERICBOOLEAN6,
                                    GENERICDATE1,
                                    GENERICDATE2,
                                    GENERICDATE3,
                                    GENERICDATE4,
                                    GENERICDATE5,
                                    GENERICDATE6,
                                    GENERICNUMBER1,
                                    GENERICNUMBER2,
                                    GENERICNUMBER3,
                                    GENERICNUMBER4,
                                    GENERICNUMBER5,
                                    GENERICNUMBER6,
                                    PRICE)
      SELECT CATEGORYNAME,
             CATEGORYTREENAME,
             CLASSFIERNAME,
             CLASSIFIERID,
             COST,
             DESCRIPTION,
             EFFECTIVEENDDATE,
             EFFECTIVESTARTDATE,
             GENERICATTRIBUTE1,
             GENERICATTRIBUTE10,
             GENERICATTRIBUTE11,
             GENERICATTRIBUTE12,
             GENERICATTRIBUTE13,
             GENERICATTRIBUTE14,
             GENERICATTRIBUTE15,
             GENERICATTRIBUTE16,
             GENERICATTRIBUTE2,
             GENERICATTRIBUTE3,
             GENERICATTRIBUTE4,
             GENERICATTRIBUTE5,
             GENERICATTRIBUTE6,
             GENERICATTRIBUTE7,
             GENERICATTRIBUTE8,
             GENERICATTRIBUTE9,
             GENERICBOOLEAN1,
             GENERICBOOLEAN2,
             GENERICBOOLEAN3,
             GENERICBOOLEAN4,
             GENERICBOOLEAN5,
             GENERICBOOLEAN6,
             GENERICDATE1,
             GENERICDATE2,
             GENERICDATE3,
             GENERICDATE4,
             GENERICDATE5,
             GENERICDATE6,
             GENERICNUMBER1,
             GENERICNUMBER2,
             GENERICNUMBER3,
             GENERICNUMBER4,
             GENERICNUMBER5,
             GENERICNUMBER6,
             PRICE
        FROM STEL_CLASSIFIER;



   DELETE FROM ST_EXT_PAY_SUMMARY
         WHERE periodseq = in_periodseq AND rpttype = 'PGCOVERNOTE';

   INSERT INTO ST_EXT_PAY_SUMMARY (TENANTID,
                                   VENDOR_NAME,
                                   AMOUNT,
                                   PERIODSEQ,
                                   PERIODNAME,
                                   PAYEESEQ,
                                   POSITIONSEQ,
                                   PROCESSINGUNITSEQ,
                                   PROCESSINGUNITNAME,
                                   USERID,
                                   GROUPFIELD,
                                   GROUPFIELDLABEL,
                                   POSITIONNAME,
                                   LOADDATE,
                                   RPTTYPE,
                                   DATAPERIODSEQ,
                                   LASTNAME,
                                   DATAPERIODNAME,
                                   CALENDARNAME,
                                   STARTDATE,
                                   ENDDATE)
      SELECT 'STEL',
             AGENT,
             /*,AMOUNT,PERIODSEQ,PERIODNAME,PAYEESEQ,POSITIONSEQ,PROCESSINGUNITSEQ
             ,PROCESSINGUNITNAME,USERID,GROUPFIELD,GROUPFIELDLABEL,POSITIONNAME
             ,LOADDATE,RPTTYPE,DATAPERIODSEQ,LASTNAME,DATAPERIODNAME,CALENDARNAME
             ,STARTDATE,ENDDATE*/
             PAYOUT,
             a.PERIODSEQ,
             PERIODNAME,
             PAYEESEQ,
             POSITIONSEQ,
             PROCESSINGUNITSEQ,
             PROCESSINGUNITNAME,
             DEALERCODE,
             CASE
                WHEN PRODUCTGROUP = 'MUSIC' THEN 'Pick & go music'
                WHEN PRODUCTGROUP like 'SIM%' THEN 'Pick & go SIM Only'
                WHEN PRODUCTGROUP = 'DASH' THEN 'Pick & go dash'
                WHEN PRODUCTGROUP = 'PREPAID TOP UP' THEN 'Pick & go Prepaid Top Up'
                ELSE PRODUCTGROUP
             END,
             CASE
                WHEN PRODUCTGROUP = 'MUSIC' THEN 'Pick & go music'
                WHEN PRODUCTGROUP like 'SIM%'  THEN 'Pick & go SIM Only'
                WHEN PRODUCTGROUP = 'DASH' THEN 'Pick & go dash'
                WHEN PRODUCTGROUP = 'PREPAID TOP UP' THEN 'Pick & go Prepaid Top Up'
                ELSE PRODUCTGROUP
             END,
             DEALERCODE,
             SYSDATE,
             'PGCOVERNOTE',
             a.PERIODSEQ,
             AGENT,
             PERIODNAME,
             CALENDARNAME,
             pd.startdate,
             pd.enddate-1
        FROM    STEL_RPT_PICK_GO_PAYSUMMRY a
             JOIN
                cs_period pd
             ON     pd.periodseq = a.periodseq
                AND pd.removedate = TO_DATE ('22000101', 'YYYYMMDD')
                AND a.dataperiodseq = in_periodseq;







   DELETE FROM ST_EXT_PAY_SUMMARY
         WHERE periodseq = in_periodseq AND rpttype = 'EXTPPCOVERNOTE';

   INSERT INTO ST_EXT_PAY_SUMMARY (TENANTID,
                                   VENDOR_NAME,
                                   AMOUNT,
                                   PERIODSEQ,
                                   PERIODNAME,
                                   PAYEESEQ,
                                   POSITIONSEQ,
                                   PROCESSINGUNITSEQ,
                                   PROCESSINGUNITNAME,
                                   USERID,
                                   GROUPFIELD,
                                   GROUPFIELDLABEL,
                                   POSITIONNAME,
                                   LOADDATE,
                                   RPTTYPE,
                                   DATAPERIODSEQ,
                                   LASTNAME,
                                   DATAPERIODNAME,
                                   CALENDARNAME,
                                   STARTDATE,
                                   ENDDATE)
      SELECT 'STEL',
             VENDORNAME,
             /*,AMOUNT,PERIODSEQ,PERIODNAME,PAYEESEQ,POSITIONSEQ,PROCESSINGUNITSEQ
             ,PROCESSINGUNITNAME,USERID,GROUPFIELD,GROUPFIELDLABEL,POSITIONNAME
             ,LOADDATE,RPTTYPE,DATAPERIODSEQ,LASTNAME,DATAPERIODNAME,CALENDARNAME
             ,STARTDATE,ENDDATE*/
             sum(COMMISSION),
             a.PERIODSEQ,
             PERIODNAME,
             PAYEESEQ,
             POSITIONSEQ,
             PROCESSINGUNITSEQ,
             PROCESSINGUNITNAME,
             VENDORCODE,
             'Prepaid' PRODUCTGROUP,
             'Prepaid'  PRODUCTGROUP ,
             VENDORCODE,
             SYSDATE,
             'EXTPPCOVERNOTE',
             a.PERIODSEQ,
             VENDORNAME,
             PERIODNAME,
             CALENDARNAME,
             trunc(pd.startdate,'Q'),
             pd.enddate-1
        FROM    STEL_RPT_EXTPREPAIDPAYSUMM a
             JOIN
                cs_period pd
             ON     pd.periodseq = a.periodseq
                AND pd.removedate = TO_DATE ('22000101', 'YYYYMMDD')
                AND a.periodseq = in_periodseq
        group by 'STEL', VENDORNAME, a.PERIODSEQ, PERIODNAME, PAYEESEQ, 
POSITIONSEQ, PROCESSINGUNITSEQ, PROCESSINGUNITNAME, VENDORCODE, VENDORCODE, 
SYSDATE, 'EXTPPCOVERNOTE', a.PERIODSEQ, VENDORNAME, PERIODNAME, 
CALENDARNAME, trunc(pd.startdate,'Q'), pd.enddate-1, 'Prepaid'
                ;



   COMMIT;
   
   --added by kyap, to handle midmonth VP payees, to display date range of midmonth
   --below query is moved over from covernote report as CR does not allow with-clause in query statement
   --use of temp table, as direct update on ST_EXT_PAY_SUMMARY will impact payment summary report
   EXECUTE IMMEDIATE 'Truncate table STEL_RPT_COVERNOTE';
   
    insert into STEL_RPT_COVERNOTE (PERIODSEQ,
                                    PERIODNAME,
                                    STARTDATE,
                                    ENDDATE,
                                    VENDOR_NAME,
                                    GROUPFIELDLABEL,
                                    AMOUNT,
                                    GENERICNUMBER1,
                                    RPTTYPE,
                                    DATAPERIODNAME,
                                    SUBJECT1,
                                    PERIODLABEL,
                                    TO_DET,
                                    TO_DETAIL,
                                    FROM_DETAILS,
                                    FOOTER_DETAILS1,
                                    FOOTER_DETAILS2,
                                    FOOTER_DETAILS3,
                                    CALENDARNAME,
                                    PROCESSINGUNITNAME)
    with refdata as (select dim0, value
    from stelext.stel_lookup
    where name = 'LT_VirtualPartners_Rates'
    and dim1 = 'Mid Month Cut Off'
    and dim2 like 'Top Up Revenue%')
    SELECT T1.PERIODSEQ,
           T1.PERIODNAME,
           T1.STARTDATE,
           T1.ENDDATE,
           case when rf.value is not null then
              case when T1.DATAPERIODNAME like '%A' then 
                T1.VENDOR_NAME || '(' || to_char(T1.startdate,'dd') || '-' || rf.value || 'th ' || T1.periodname || ')'
              else
                T1.VENDOR_NAME || '(' || (rf.value+1) || '-' || to_char(T1.enddate,'dd') || 'th ' || T1.periodname || ')'
              end
           else
              T1.VENDOR_NAME
           end VENDOR_NAME,
           T1.GROUPFIELDLABEL,
           T1.AMOUNT,
           T1.GENERICNUMBER1,
           T1.RPTTYPE,
           T1.DATAPERIODNAME,
           T1.SUBJECT1,
           T1.PERIODLABEL,
           T1.TO_DET,
           CASE
              WHEN T1.AMOUNT >= CF.GENERICNUMBER2
                   AND T1.AMOUNT < CF.GENERICNUMBER3
              THEN
                 CF.GENERICATTRIBUTE7
              ELSE
                 CASE
                    WHEN T1.AMOUNT >= CF.GENERICNUMBER3
                         AND T1.AMOUNT < CF.GENERICNUMBER4
                    THEN
                       CF.GENERICATTRIBUTE9
                    ELSE
                       CASE
                          WHEN T1.AMOUNT >= CF.GENERICNUMBER4
                          THEN
                             CF.GENERICATTRIBUTE11
                       END
                 END
           END
              to_detail,
           CASE
              WHEN T1.AMOUNT >= CF.GENERICNUMBER2
                   AND T1.AMOUNT < CF.GENERICNUMBER3
              THEN
                 CF.GENERICATTRIBUTE8
              ELSE
                 CASE
                    WHEN T1.AMOUNT >= CF.GENERICNUMBER3
                         AND T1.AMOUNT < CF.GENERICNUMBER4
                    THEN
                       CF.GENERICATTRIBUTE10
                    ELSE
                       CASE
                          WHEN T1.AMOUNT >= CF.GENERICNUMBER4
                          THEN
                             CF.GENERICATTRIBUTE12
                       END
                 END
           END
              from_Details,
           T1.FOOTER_DETAILS1,
           T1.FOOTER_DETAILS2,
           T1.FOOTER_DETAILS3,
           CALENDARNAME,
           PROCESSINGUNITNAME
      FROM  (
    select x.periodseq,x.periodname,x.startdate,x.enddate,
    x.vendor_name, 
    nvl(x.groupfieldlabel2,x.groupfieldlabel) as groupfieldlabel
    ,sum(x.amount) AMOUNT,c.genericnumber1, 'n.a.' rpttype,
    case when nvl(r.periodtype,'month') = 'month' then dataperiodname else periodname end as dataperiodname,
    'Commission Payment for '||nvl(x.groupfieldlabel2,x.groupfieldlabel)||' Period  ' subject1,
    (case when extract (month from x.startdate)=extract (month from x.enddate) and  last_day(x.enddate)=x.enddate  and  extract (day from x.startdate) =1
            then  x.periodname  
    when extract (month from x.startdate)=extract (month from x.enddate)    and  last_day(x.enddate)=x.enddate   and  extract (day from x.startdate) <>1
            then   x.dataperiodname
    when extract (month from x.startdate)=extract (month from x.enddate)    and  last_day(x.enddate)<>x.enddate   and  extract (day from x.startdate) =1
            then  x.dataperiodname
    else   trim(to_char(x.startdate,'Month')) ||' '|| trim(to_char(x.startdate,'YYYY')) || ' to '|| trim(to_char(x.enddate,'Month')) ||' '|| trim(to_char(x.enddate,'YYYY'))
    end ) periodlabel,
    regexp_substr(c.genericattribute1,'[^|]+')||'' as to_det,
    regexp_substr(c.genericattribute3,'[^|]+') as footer_Details1,
    regexp_substr(c.genericattribute3,'[^|]+',1,2) as footer_Details2,
    trim(regexp_substr(c.genericattribute3,'[^|]+',2,3)) as footer_details3,
    x.calendarname,
    x.processingunitname
    from (select * from stelext.STEL_CLASSIFIER_TAB
    where categorytreename='Reporting Config'
    and categoryname='Cover Note') c ,
    stelext.ST_EXT_PAY_SUMMARY x
    --stelext.TEMP1 x
    join cs_period pd
    on pd.periodseq=x.periodseq
    and pd.removedate>sysdate
    left join (select   y.rpttype, max(nvl(y.periodtype,'month'))
    periodtype from stelext.stel_rpt_cfg_rpttype y group by y.rpttype ) r
    on r.rpttype=x.rpttype
    where
      c.effectivestartdate<pd.enddate
    and c.effectiveenddate>=pd.enddate
    and x.rpttype in('EXTPMTSUMMARY_VP','EXTPMTSUMMARY','EXTPMTSUMMARY_MIDMONTH')
    --and x.periodname ='{?Period Name}'
    --and x.calendarname='{?Calendar Name}'
    --and x.processingunitname='{?ProcessingUnit Name}'
    and x.periodseq = in_PERIODSEQ
    and x.processingunitseq = in_PROCESSINGUNITSEQ
    --and'{?AncestorUserIdTenantId}'='{?AncestorUserIdTenantId}'
    -- and x.groupfieldlabel = c.genericattribute6 --bugfix by kyap, to include adjustments
    and nvl(x.groupfieldlabel2,x.groupfieldlabel) = c.genericattribute6
    -- and x.periodname ='April 2017'
     group by x.periodseq,x.periodname,x.startdate,x.enddate,
    x.vendor_name, 
    nvl(x.groupfieldlabel2,x.groupfieldlabel),c.genericnumber1,'n.a.' ,
    case when nvl(r.periodtype,'month') = 'month' then dataperiodname else periodname end ,
    'Commission Payment for '||nvl(x.groupfieldlabel2,x.groupfieldlabel)||' Period ' ,
    (case when extract (month from x.startdate)=extract (month from x.enddate) and  last_day(x.enddate)=x.enddate  and  extract (day from x.startdate) =1
            then  x.periodname  
    when extract (month from x.startdate)=extract (month from x.enddate)    and  last_day(x.enddate)=x.enddate   and  extract (day from x.startdate) <>1
            then   x.dataperiodname
    when extract (month from x.startdate)=extract (month from x.enddate)    and  last_day(x.enddate)<>x.enddate   and  extract (day from x.startdate) =1
            then  x.dataperiodname
    else   trim(to_char(x.startdate,'Month')) ||' '|| trim(to_char(x.startdate,'YYYY')) || ' to '|| trim(to_char(x.enddate,'Month')) ||' '|| trim(to_char(x.enddate,'YYYY'))
    end ),
    regexp_substr(c.genericattribute1,'[^|]+')||'' ,
    regexp_substr(c.genericattribute1,'[^|]+') || regexp_substr(c.genericattribute1,'[^|]+',1,2) ||trim(regexp_substr(c.genericattribute1,'[^|]+',2,3)),
    regexp_substr(c.genericattribute2,'[^|]+') || regexp_substr(c.genericattribute2,'[^|]+',1,2) ||regexp_substr(c.genericattribute2,'[^|]+',2,3),
    regexp_substr(c.genericattribute3,'[^|]+') ,
    regexp_substr(c.genericattribute3,'[^|]+',1,2) ,
    trim(regexp_substr(c.genericattribute3,'[^|]+',2,3)),
    x.calendarname,
    x.processingunitname
    having sum(x.amount) > c.genericnumber1
    ) T1, STELEXT.STEL_CLASSIFIER_TAB CF, refdata rf
    WHERE NVL(T1.GROUPFIELDLABEL,' ') = NVL(CF.GENERICATTRIBUTE6,' ')
    AND CF.categoryname = 'Cover Note' 
    and t1.vendor_name = rf.dim0(+);
    
    commit;
    
END RPT_POST_COVERNOTE;
