select CREDITSEQ,
CLAWBACKTYPE,
PERIODSEQ,
SALESTRANSACTIONSEQ,
NAME,
PREV_POSITIONNAME,
CREDIT_VALUE,
CREDIT_COMPDATE,
PREV_SUBLINENUMBER,
PREV_ALTERNATEORDERNUMBER,
PREV_COMPENSATIONDATE,
PREV_LATESTPREMIUM,
PREV_SALESTRANSACTIONSEQ,
PREV_POLICY_SDATE,
PREV_POLICY_EDATE,
CANTXNS_ALTERNATEORDERNUMBER,
CANTXNS_SALESTRANSACTIONSEQ,
CANTXNS_SALESORDERSEQ,
CANTXNS_LINENUMBER,
CANTXNS_SUBLINENUMBER,
CANTXNS_COMPDATE,
CANTXNS_OLD_PREMIUM,
CANTXNS_NEW_PREMIUM,
CANTXNS_POLICY_SDATE,
CANTXNS_POLICY_EDATE,
CANTXNS_POLICY_CDATE,
CANTXNS_POSITIONNAME,
CANTXNS_POSITIONSEQ,
SUM_CREDIT_VALUE,
NEW_SETNUMBER
 from (
select B.*,ROW_NUMBER() OVER (PARTITION BY B.cantxns_salestransactionseq,B.prev_positionname) as rownum,
ROW_NUMBER() OVER (PARTITION BY B.cantxns_salestransactionseq) +ta.setnumber as new_setnumber from (
select distinct clc.*,
A.sum_credit_value 
from (select cantxns_alternateordernumber, cantxns_salestransactionseq, prev_latestpremium,cantxns_positionname,
sum(credit_value) as sum_credit_value from ext.tryg_clawback_credits clc
where clawbacktype='CANC' 
-- and cantxns_alternateordernumber=6520000545616
and cantxns_alternateordernumber=8804002276593
group by  cantxns_alternateordernumber,cantxns_salestransactionseq,prev_latestpremium,cantxns_positionname) A
inner join ext.tryg_clawback_credits clc on
clc.cantxns_salestransactionseq = A.cantxns_salestransactionseq
inner join cs_credit cr on
clc.creditseq=cr.creditseq
)B 
inner join cs_transactionassignment ta on
ta.salestransactionseq= B.cantxns_salestransactionseq
and ta.salestransactionseq=B.cantxns_salestransactionseq) C
where rownum=1;


merge into ext.tryg_clawback_credits cc
USING(
SELECT cc_in.cantxns_alternateordernumber, cc_in.CANTXNS_SALESTRANSACTIONSEQ, cc_in.CANTXNS_COMPDATE, 
cc_in.CANTXNS_SUBLINENUMBER,cc_in.CREDIT_VALUE,
IFNULL(LAG(cc_in.CREDIT_VALUE) OVER (PARTITION BY cc_in.cantxns_alternateordernumber
ORDER BY cc_in.CANTXNS_COMPDATE,cc_in.CANTXNS_SUBLINENUMBER) + cc_in.CREDIT_VALUE,CREDIT_VALUE) AS CREDIT_TD
from ext.tryg_clawback_credits cc_in where cc_in.cantxns_alternateordernumber in (8200013669886,6200016379506,6200004431464)
) sub
on cc.CANTXNS_salestransactionseq = sub.cantxns_salestransactionseq
and cc.cantxns_alternateordernumber=sub.cantxns_alternateordernumber
WHEN MATCHED THEN
    UPDATE SET cc.CREDIT_SUM = sub.CREDIT_TD;

UPDATE ext.tryg_clawback_credits set CREDIT_SUM=0.0 where CREDIT_VALUE=0.0 and CREDIT_SUM <> 0.0;
