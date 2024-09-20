CREATE PROCEDURE EXT.TRYG_SH_POLICYPAY ( in_PeriodSeq BIGINT,in_ProcessingUnitSeq BIGINT) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER 
DEFAULT SCHEMA EXT AS 
/*---------------------------------------------------------------------
    | Author: Sharath K
    | Project Title: Consultant
    | Company: SAP Callidus
    | Initial Version Date: 01-May-2022
    |----------------------------------------------------------------------
    | Procedure Purpose: 
    | Version: 0.1	01-May-2022	Intial Version
    -----------------------------------------------------------------------
    */
BEGIN
	--Row type variables declarations
	DECLARE v_periodRow ROW LIKE TCMP.CS_PERIOD;
	DECLARE v_puRow ROW LIKE TCMP.CS_PROCESSINGUNIT;
	DECLARE v_ctRow ROW LIKE TCMP.CS_UNITTYPE;
	DECLARE v_ctDKKRow ROW LIKE TCMP.CS_UNITTYPE;
	DECLARE v_etSumm ROW LIKE TCMP.CS_EVENTTYPE;
	DECLARE v_etpay ROW LIKE TCMP.CS_EVENTTYPE;


	--Variable declarations
	DECLARE v_tenantid VARCHAR(50);
	DECLARE v_procedureName VARCHAR(50);
	DECLARE v_slqerrm VARCHAR(4000);
	DECLARE v_policyPay_ET VARCHAR(50);
	DECLARE v_policySales_ET VARCHAR(50);
	DECLARE v_policySalesSummary_ET VARCHAR(50);


	DECLARE v_sqlCount INT;

	DECLARE v_removeDate DATE;
	DECLARE v_changeDate DATE;


	-- Exeception Handling
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN v_slqerrm := ::SQL_ERROR_MESSAGE;
		CALL EXT.TRYG_LOG(v_procedureName,'ERROR = '||IFNULL(:v_slqerrm,'') ,NULL);
	END;

	--------------------------------------------------------------------------- 
	v_procedureName = 'TRYG_SH_POLICYPAY';
	v_removeDate = TO_DATE('01/01/2200','mm/dd/yyyy');
	v_policySales_ET = 'SC-DK-001-001';
	v_policyPay_ET = 'SC-DK-001-002';
 	v_policySalesSummary_ET = 'SC-DK-001-001-SUMMARY';
 	v_changeDate = TO_DATE('01/01/2023','mm/dd/yyyy');


	SELECT * INTO v_puRow FROM TCMP.CS_PROCESSINGUNIT cp WHERE cp.PROCESSINGUNITSEQ = in_ProcessingUnitSeq;
	SELECT * INTO v_periodRow FROM TCMP.CS_PERIOD cp WHERE cp.PERIODSEQ = in_PeriodSeq AND cp.REMOVEDATE = v_removeDate;
    select * into v_ctRow from cs_unittype where name='quantity' and REMOVEDATE = :v_removeDate;
    select * into v_etSumm from cs_eventtype where eventtypeid='SC-DK-001-001-SUMMARY' and REMOVEDATE = :v_removeDate;
    select * into v_etpay from cs_eventtype where eventtypeid='SC-DK-001-002' and REMOVEDATE = :v_removeDate;
    select * into v_ctDKKRow from cs_unittype where name='DKK' and REMOVEDATE = :v_removeDate;
	v_tenantid = :v_puRow.TENANTID;

	EXT.TRYG_LOG(v_procedureName,'####   BEGIN   #### '||:v_periodRow.Name,NULL);
    
UPDATE 
st
set st.genericboolean5=0, genericattribute21 = NULL
from cs_salestransaction st
inner join ext.TRYG_SH_SALESTXNS_POLICYCRDS cc on
st.alternateordernumber = cc.alternateordernumber
inner join cs_transactionassignment ta
on cc.salestransactionseq=ta.salestransactionseq
WHERE cc.periodseq = :v_periodRow.periodseq
AND st.compensationdate < cc.compensationdate
and st.compensationdate >= cc.LASTPAYMENTDATE
and cc.compensationdate=ta.compensationdate
-- and ifnull(ta.genericboolean1,0)=1
and st.eventtypeseq=:v_etSumm.datatypeseq
and ifnull(cc.creditvalue,0.0)> 0.0;
-- and ifnull(cc.creditvalue,9999999999) <> 9999999999;


	CALL EXT.TRYG_LOG(v_procedureName,'Reset paid flags on summary transactions ',::ROWCOUNT);
	COMMIT;	
	
	DELETE FROM ext.TRYG_SH_SALESTXNS_POLICYCRDS WHERE periodseq = :v_periodRow.periodseq;
	v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Deleting Previously inserted Policy Payment transactions for the month from temp table',v_sqlCount);
	COMMIT;
    
    --Reset flags for SC-DK-001-002
    UPDATE
		cs_transactionassignment ta SET (ta.genericnumber1,ta.unittypeforgenericnumber1,ta.genericboolean1) =(0,null,0)
	    where exists (
		select 1 from cs_salestransaction st
		INNER JOIN cs_transactionassignment sta	ON
			st.salestransactionseq = sta.salestransactionseq
			AND st.compensationdate = sta.compensationdate
		INNER JOIN cs_eventtype et	ON
			et.datatypeseq = st.eventtypeseq
		WHERE
		    ta.salestransactionseq = sta.salestransactionseq
		    and st.salestransactionseq=sta.salestransactionseq
		    and st.compensationdate = sta.compensationdate
		    and ta.compensationdate = sta.compensationdate
			AND et.removedate = v_removeDate
			AND et.eventtypeid = v_policyPay_ET
			AND st.compensationdate >= :v_periodRow.startdate
			AND st.compensationdate < :v_periodRow.enddate)
			AND ta.compensationdate >= :v_periodRow.startdate
			AND ta.compensationdate < :v_periodRow.enddate;

    
	-- v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Reset flags on TA for policy payment TXNS',::ROWCOUNT);
	COMMIT;		


	INSERT INTO	ext.TRYG_SH_SALESTXNS_POLICYCRDS
	(
		SELECT
			so.orderid,
			st.linenumber,
			st.sublinenumber,
			et.eventtypeid,
			st.compensationdate,
			st.salestransactionseq,
			st.alternateordernumber,
			:v_periodRow.periodseq AS periodseq,
			sta.positionname,
			0 AS positioinseq,
			9999999999 AS creditvalue,
			0 AS unittypeforcreditvalue,
			st.genericdate6 AS lastpaymentdate
		FROM
			cs_salesorder so
		INNER JOIN cs_salestransaction st ON
			st.salesorderseq = so.salesorderseq
		INNER JOIN cs_transactionassignment sta	ON
			st.salestransactionseq = sta.salestransactionseq
			AND st.compensationdate = sta.compensationdate
		INNER JOIN cs_eventtype et	ON
			et.datatypeseq = st.eventtypeseq
		WHERE
			so.removedate = v_removeDate
			AND et.removedate = v_removeDate
			AND et.eventtypeid = v_policyPay_ET
			AND st.compensationdate >= :v_periodRow.startdate
			AND st.compensationdate < :v_periodRow.enddate
	);
	v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Inserting Policy Payment transactions for the month into temp table',v_sqlCount);
	COMMIT;


	UPDATE
		ext.TRYG_SH_SALESTXNS_POLICYCRDS pc
	SET
		positionseq = (
		SELECT
			ruleelementownerseq
		FROM
			cs_position pos
		WHERE
			pos.removedate = :v_removedate
			AND pos.name = pc.positionname
			AND pos.effectivestartdate <= :v_periodRow.startdate
			AND pos.effectiveenddate > :v_periodRow.startdate)
	WHERE
		periodseq = :v_periodRow.periodseq
		AND positionseq = 0;
		
	v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Update of positionseq using positionname on assignments',v_sqlCount);
	COMMIT;
	
	/*
	---------------------------------------------------------------------------------------------------------------------------
	Fix for Position Change 
	
	Defect TBVS2-1230
	
	A M Robinson 23/03/2023
	---------------------------------------------------------------------------------------------------------------------------
	*/

	UPDATE
		pc
	SET
		pc.positionseq=pos2.ruleelementownerseq,
		pc.positionname=pos2.name
	from 
		ext.TRYG_SH_SALESTXNS_POLICYCRDS pc
		join cs_period per on per.periodseq=pc.periodseq and per.removedate=v_removedate
		join cs_position pos on pos.ruleelementownerseq=pc.positionseq and pos.removedate=v_removedate AND pos.EFFECTIVEENDDATE >= per.STARTDATE AND pos.EFFECTIVESTARTDATE < per.endDATE
		join cs_payee pay on pay.payeeseq=pos.payeeseq and pay.removedate=v_removedate AND pay.EFFECTIVEENDDATE >= per.STARTDATE AND pay.EFFECTIVESTARTDATE < per.endDATE
		join cs_participant part on part.payeeseq=pay.payeeseq and part.removedate=v_removedate AND part.EFFECTIVEENDDATE >= per.STARTDATE AND part.EFFECTIVESTARTDATE < per.endDATE
		join cs_position pos2 on pos2.payeeseq=pay.payeeseq and pos2.removedate=v_removedate AND pos2.EFFECTIVEENDDATE > per.STARTDATE AND pos2.EFFECTIVESTARTDATE < per.endDATE and pos2.effectivestartdate>=v_changeDate
	where pos.name <> pos2.name
	and pc.periodseq = :v_periodRow.periodseq;
		
	v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Update of Positions post 01/01/2023 Position Name Change',v_sqlCount);
	COMMIT;
	
	/* Clean up Duplicates */
	
	delete
    FROM ext.TRYG_SH_SALESTXNS_POLICYCRDS x
    WHERE "$rowid$"  NOT IN
    (
        SELECT MAX("$rowid$" )
        FROM ext.TRYG_SH_SALESTXNS_POLICYCRDS
        GROUP BY salestransactionseq, positionname
    );
		
	/*	
	---------------------------------------------------------------------------------------------------------------------------
	*/

/*
	UPDATE ext.TRYG_SH_SALESTXNS_POLICYCRDS pc
	SET
		(creditvalue,unittypeforcreditvalue) = 
		(
			SELECT
				sum(IFNULL(cc.value,0)),
				cc.unittypeforvalue
			FROM
				cs_credit cc
			INNER JOIN cs_salestransaction st ON
				st.salestransactionseq = cc.salestransactionseq
			INNER JOIN cs_eventtype et ON 
				et.datatypeseq = st.eventtypeseq
			WHERE
				st.compensationdate <  pc.compensationdate
				and ifnull(st.genericboolean5,0)=0
				and st.genericattribute21 is null
				AND st.compensationdate >= pc.LASTPAYMENTDATE
				AND et.removedate = :v_removedate
				AND et.eventtypeid IN (
					-- v_policySales_ET	,
				v_policySalesSummary_ET) --summary transactions have been included
				AND st.alternateordernumber = pc.alternateordernumber
				AND cc.positionseq in
					(	select distinct posall.ruleelementownerseq
						from cs_position poscr
						join cs_period per on poscr.EFFECTIVEENDDATE >= per.STARTDATE AND poscr.EFFECTIVESTARTDATE < per.endDATE and per.removedate=v_removedate
						join cs_payee pay on pay.payeeseq=poscr.payeeseq and pay.removedate=v_removedate AND pay.EFFECTIVEENDDATE >= per.STARTDATE AND pay.EFFECTIVESTARTDATE < per.endDATE
						join cs_participant part on part.payeeseq=pay.payeeseq and part.removedate=v_removedate AND part.EFFECTIVEENDDATE >= per.STARTDATE AND part.EFFECTIVESTARTDATE < per.endDATE
						join cs_position posall on posall.payeeseq=pay.payeeseq and posall.removedate=v_removedate --AND pos2.EFFECTIVEENDDATE >= per.STARTDATE AND pos2.EFFECTIVESTARTDATE < per.endDATE and pos2.effectivestartdate>=to_date('01012023','ddmmyyyy')
						where poscr.ruleelementownerseq=pc.positionseq and per.periodseq=cc.periodseq
					) -- = pc.positionseq
			GROUP BY 
				cc.unittypeforvalue --cc.unittypeforvalue
		)
	WHERE
		periodseq = :v_periodRow.periodseq
		AND creditvalue = 9999999999;

*/

--For every payment transaction identify the corresponding summary transactions and stamp them as paid(gb5) so
--that they are not accounted for the next payment transaction
UPDATE 
st
set genericattribute21 = cc.alternateordernumber || '-' || cc.sublinenumber || '-' || cc.linenumber
from cs_salestransaction st
inner join ext.TRYG_SH_SALESTXNS_POLICYCRDS cc on
st.alternateordernumber = cc.alternateordernumber
inner join cs_transactionassignment ta
on cc.salestransactionseq=ta.salestransactionseq
WHERE cc.periodseq = :v_periodRow.periodseq
AND st.compensationdate <= cc.compensationdate
and st.compensationdate >= cc.LASTPAYMENTDATE
and cc.compensationdate=ta.compensationdate
and ifnull(ta.genericboolean1,0)=0
and st.eventtypeseq=:v_etSumm.datatypeseq
-- and ifnull(cc.creditvalue,0.0)> 0.0
-- and ifnull(cc.creditvalue,9999999999) <> 9999999999
AND ifnull(st.genericboolean5,0) = 0
AND genericattribute21 IS NULL;

 CALL EXT.TRYG_LOG(v_procedureName,'Update summary transactions with payment txn info ',::ROWCOUNT);


	merge into ext.TRYG_SH_SALESTXNS_POLICYCRDS pc using (
			SELECT
			    pc.alternateordernumber,
			    pc.sublinenumber,
			    pc.linenumber,
			     :v_ctRow.unittypeseq,
				sum(IFNULL(cc.value,0)) as credit_value
			FROM
				cs_credit cc
			INNER JOIN cs_salestransaction st ON
				st.salestransactionseq = cc.salestransactionseq
			INNER JOIN cs_eventtype et ON 
				et.datatypeseq = st.eventtypeseq
				INNER JOIN ext.TRYG_SH_SALESTXNS_POLICYCRDS pc
				on
				 st.compensationdate <=  pc.compensationdate
				 and st.compensationdate >= pc.LASTPAYMENTDATE
				 and st.genericattribute21=pc.alternateordernumber || '-' || pc.sublinenumber || '-' || pc.linenumber
			WHERE
				 et.removedate = :v_removeDate
				AND et.eventtypeid IN (:v_policySalesSummary_ET) --summary transactions have been included
				AND st.alternateordernumber = pc.alternateordernumber
				and ifnull(st.genericboolean5,0)=0
				and st.genericattribute21 is not null
				AND cc.positionseq in
					(	select distinct posall.ruleelementownerseq
						from cs_position poscr
						join cs_period per on poscr.EFFECTIVEENDDATE >= per.STARTDATE AND poscr.EFFECTIVESTARTDATE < per.endDATE and per.removedate=:v_removeDate
						join cs_payee pay on pay.payeeseq=poscr.payeeseq and pay.removedate=:v_removeDate AND pay.EFFECTIVEENDDATE >= per.STARTDATE AND pay.EFFECTIVESTARTDATE < per.endDATE
						join cs_participant part on part.payeeseq=pay.payeeseq and part.removedate=:v_removeDate AND part.EFFECTIVEENDDATE >= per.STARTDATE AND part.EFFECTIVESTARTDATE < per.endDATE
						join cs_position posall on posall.payeeseq=pay.payeeseq and posall.removedate=:v_removeDate --AND pos2.EFFECTIVEENDDATE >= per.STARTDATE AND pos2.EFFECTIVESTARTDATE < per.endDATE and pos2.effectivestartdate>=to_date('01012023','ddmmyyyy')
						where poscr.ruleelementownerseq=pc.positionseq and per.periodseq=cc.periodseq
					) -- = pc.positionseq
			and	pc.periodseq = :v_periodRow.periodseq
			and pc.creditvalue = 9999999999
			group by
				pc.alternateordernumber,
			    pc.sublinenumber,
			    pc.linenumber,
			    :v_ctRow.unittypeseq
		) sub on
		        pc.alternateordernumber=sub.alternateordernumber
		        and pc.sublinenumber= sub.sublinenumber
		        and pc.linenumber= sub.linenumber
		        when matched then
		        update set creditvalue = credit_value , unittypeforcreditvalue = :v_ctRow.unittypeseq;
			
	v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Update of credit value for Policy Pay TXNS using ALTERNATEORDER number on policy sales TXNS',v_sqlCount);
	COMMIT;
	
--For every payment transaction identify the corresponding summary transactions and stamp them as paid(gb5) so
--that they are not accounted for the next payment transaction
UPDATE 
st
set st.genericboolean5=1
from cs_salestransaction st
inner join ext.TRYG_SH_SALESTXNS_POLICYCRDS cc on
st.alternateordernumber = cc.alternateordernumber
inner join cs_transactionassignment ta
on cc.salestransactionseq=ta.salestransactionseq
WHERE cc.periodseq = :v_periodRow.periodseq
AND st.compensationdate <= cc.compensationdate
and st.compensationdate >= cc.LASTPAYMENTDATE
and cc.compensationdate=ta.compensationdate
and ifnull(ta.genericboolean1,0)=0
and st.eventtypeseq=:v_etSumm.datatypeseq
and ifnull(cc.creditvalue,0.0)> 0.0
and ifnull(cc.creditvalue,9999999999) <> 9999999999
AND ifnull(st.genericboolean5,0) = 0
AND genericattribute21 IS NOT NULL;

CALL EXT.TRYG_LOG(v_procedureName,'Update summary transactions with paid flag ',::ROWCOUNT);
	
	
	UPDATE
		cs_transactionassignment sta
	SET
		(genericnumber1,unittypeforgenericnumber1,genericboolean1) = (
			SELECT
				cc.creditvalue,
				CASE WHEN SUBSTR_AFTER (ti.name,'-') in ('GA','PA','DB') THEN :v_ctDKKRow.unittypeseq 
				else :v_ctRow.unittypeseq end ,
				1
			FROM
				TRYG_SH_SALESTXNS_POLICYCRDS cc
			INNER JOIN cs_position pos
			on pos.ruleelementownerseq=cc.positionseq
			inner join cs_title ti
			on ti.ruleelementownerseq=pos.titleseq
			WHERE 
				cc.periodseq = :v_periodRow.periodseq
				and ti.removedate = :v_removedate
				and pos.removedate =:v_removedate
				AND cc.SALESTRANSACTIONSEQ = sta.SALESTRANSACTIONSEQ
				AND cc.positionname = sta.positionname
				group by cc.creditvalue, ti.name, 1
		)
	WHERE
		EXISTS 
		(
			SELECT
				1
			FROM
				ext.TRYG_SH_SALESTXNS_POLICYCRDS cc
			WHERE
				cc.periodseq = :v_periodRow.periodseq
				AND cc.SALESTRANSACTIONSEQ = sta.SALESTRANSACTIONSEQ
				AND ifnull(cc.creditvalue,9999999999) <> 9999999999
		);

	
	-- v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Set paid flag for summary transactions',::ROWCOUNT);
	COMMIT;		

---Release CANC/DECR Immediately without the need to wait for payment transaction.


update cs_transactionassignment ta set ta.genericboolean1=0 where 
EXISTS(select 1
from ext.tryg_cancel_txns ct where clawbacktype in ('CANC','DECR')
and ta.salestransactionseq=ct.cantxns_salestransactionseq)
and ta.compensationDate >= :v_periodRow.startdate
AND ta.compensationDate < :v_periodRow.startdate
AND ta.genericboolean1=1;

merge into cs_transactionassignment ta using
(
select distinct ct.cantxns_alternateordernumber, ct.cantxns_salestransactionseq,ct.cantxns_positionname
from ext.tryg_cancel_txns ct where clawbacktype in ('CANC','DECR')
and EXISTS(select 1 from TRYG_SH_SALESTXNS_POLICYCRDS cc
inner join cs_position pos on 
pos.name=cc.positionname
where cc.alternateordernumber=ct.cantxns_alternateordernumber
and pos.name =ct.cantxns_positionname 
and cc.positionname=ct.cantxns_positionname
and cc.compensationDate <= ct.cantxns_compdate
and cc.compensationDate > add_months(ct.cantxns_compdate,-12)
and pos.removedate =:v_removedate
AND ifnull(cc.creditvalue,9999999999) <> 9999999999
-- and ifnull(cc.creditvalue,0) <> 0.0
)
) sub on ta.salestransactionseq= sub.cantxns_salestransactionseq
and sub.cantxns_positionname=ta.positionname
WHEN MATCHED THEN UPDATE
set ta.genericboolean1 = 1;

CALL EXT.TRYG_LOG(v_procedureName,'Set immediate release flag for canc/decr summary transactions',::ROWCOUNT);
COMMIT;		

EXT.TRYG_LOG(v_procedureName,'####   END   ####',NULL);

	
END