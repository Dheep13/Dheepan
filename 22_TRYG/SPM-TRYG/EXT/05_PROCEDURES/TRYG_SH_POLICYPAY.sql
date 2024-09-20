CREATE OR REPLACE PROCEDURE EXT.TRYG_SH_POLICYPAY ( in_PeriodSeq BIGINT,in_ProcessingUnitSeq BIGINT) 
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

	EXT.TRYG_LOG(v_procedureName,'####   BEGIN   #### '||:v_periodRow.Name || ' Processingunitseq = ' || :in_ProcessingUnitSeq,NULL);
    
	DELETE FROM ext.TRYG_SH_SALESTXNS_POLICYCRDS WHERE periodseq = :v_periodRow.periodseq;
	v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Deleting Previously inserted Policy Payment transactions for the month from temp table',v_sqlCount);
	COMMIT;
    
    --Reset flags for SC-DK-001-002
    UPDATE
		cs_transactionassignment ta SET (ta.genericnumber1,ta.unittypeforgenericnumber1,ta.genericboolean1) =(0,null,0)
	    where exists (
		select 1 from cs_salestransaction st
		INNER JOIN cs_transactionassignment sta	ON st.salestransactionseq = sta.salestransactionseq AND st.compensationdate = sta.compensationdate
		INNER JOIN cs_eventtype et	ON et.datatypeseq = st.eventtypeseq
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
	CALL EXT.TRYG_LOG(v_procedureName,'Reset release flags on TA for policy payment TXNS',::ROWCOUNT);
	COMMIT;		

    --Reset flags for SC-DK-001-001 Summary
    UPDATE
		cs_transactionassignment ta SET (ta.genericnumber1,ta.unittypeforgenericnumber1,ta.genericboolean1) =(0,null,0)
	    where exists 
	    (
			select 1 from cs_salestransaction st
			INNER JOIN cs_transactionassignment sta	ON st.salestransactionseq = sta.salestransactionseq AND st.compensationdate = sta.compensationdate
			-- INNER JOIN cs_eventtype et	ON et.datatypeseq = st.eventtypeseq
			WHERE ta.salestransactionseq = sta.salestransactionseq
			    and st.salestransactionseq=sta.salestransactionseq
			    and st.compensationdate = sta.compensationdate
			    and ta.compensationdate = sta.compensationdate
				and st.eventtypeseq = :v_etSumm.datatypeseq
				AND st.compensationdate >= :v_periodRow.startdate
				AND st.compensationdate < :v_periodRow.enddate
				--AMR 20231021 clear all Summary Txns
				-- and st.genericnumber1 < st.genericnumber2 
	    )
			AND ta.compensationdate >= :v_periodRow.startdate
			AND ta.compensationdate < :v_periodRow.enddate;

	-- v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Reset release flags on TA for Sales Summary TXNS',::ROWCOUNT);
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
	
	
	-- ---------------------------------------------------------------------------------------------------------------------------
	-- Fix for Position Change 
	
	-- Defect TBVS2-1230
	
	-- A M Robinson 23/03/2023
	-- ---------------------------------------------------------------------------------------------------------------------------
	

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
	
	-- Clean up Duplicates 
	
	delete
    FROM ext.TRYG_SH_SALESTXNS_POLICYCRDS x
    WHERE "$rowid$"  NOT IN
    (
        SELECT MAX("$rowid$" )
        FROM ext.TRYG_SH_SALESTXNS_POLICYCRDS
        GROUP BY salestransactionseq, positionname
    );
    commit;
    
		
	merge into ext.TRYG_SH_SALESTXNS_POLICYCRDS pc using (
			SELECT
			    pc.alternateordernumber,
			    pc.sublinenumber,
			    pc.linenumber,
			    pc.positionname,
			     :v_ctRow.unittypeseq,
				sum(IFNULL(cc.value,0)) as credit_value
			FROM
				cs_credit cc
			INNER JOIN cs_salestransaction st ON st.salestransactionseq = cc.salestransactionseq
			INNER JOIN cs_eventtype et ON et.datatypeseq = st.eventtypeseq
				INNER JOIN ext.TRYG_SH_SALESTXNS_POLICYCRDS pc on st.compensationdate <=  pc.compensationdate and st.compensationdate >= pc.LASTPAYMENTDATE
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
						where poscr.ruleelementownerseq=pc.positionseq 
						--Removed as restricts the changes in position just to the current period TBVS2-2149 AMR 20230814
						--and per.periodseq=cc.periodseq
					) -- = pc.positionseq
			and	pc.periodseq = :v_periodRow.periodseq
			and pc.creditvalue = 9999999999
			group by
				pc.alternateordernumber,
			    pc.sublinenumber,
			    pc.linenumber,
			    pc.positionname, --defect 1799
			    :v_ctRow.unittypeseq
		) sub on
		        pc.alternateordernumber=sub.alternateordernumber
		        and pc.sublinenumber= sub.sublinenumber
		        and pc.linenumber= sub.linenumber
		        and pc.positionname = sub.positionname
		        when matched then
		        update set creditvalue = credit_value , unittypeforcreditvalue = :v_ctRow.unittypeseq;
			
	v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Update of credit value for Policy Pay TXNS using ALTERNATEORDER number on policy sales TXNS',v_sqlCount);
	COMMIT;		
		

	-- UPDATE ext.TRYG_SH_SALESTXNS_POLICYCRDS pc
	-- SET
	-- 	(creditvalue,unittypeforcreditvalue) = 
	-- 	(
	-- 		SELECT
	-- 			sum(IFNULL(cc.value,0)),
	-- 			cc.unittypeforvalue
	-- 		FROM
	-- 			cs_credit cc
	-- 		INNER JOIN cs_salestransaction st ON
	-- 			st.salestransactionseq = cc.salestransactionseq
	-- 		INNER JOIN cs_eventtype et ON 
	-- 			et.datatypeseq = st.eventtypeseq
	-- 		WHERE
	-- 			st.compensationdate <  pc.compensationdate
	-- 			and ifnull(st.genericboolean5,0)=0
	-- 			and st.genericattribute21 is null
	-- 			AND st.compensationdate >= pc.LASTPAYMENTDATE
	-- 			AND et.removedate = :v_removedate
	-- 			AND et.eventtypeid IN (
	-- 				-- v_policySales_ET	,
	-- 			v_policySalesSummary_ET) --summary transactions have been included
	-- 			AND st.alternateordernumber = pc.alternateordernumber
	-- 			AND cc.positionseq in
	-- 				(	select distinct posall.ruleelementownerseq
	-- 					from cs_position poscr
	-- 					join cs_period per on poscr.EFFECTIVEENDDATE >= per.STARTDATE AND poscr.EFFECTIVESTARTDATE < per.endDATE and per.removedate=v_removedate
	-- 					join cs_payee pay on pay.payeeseq=poscr.payeeseq and pay.removedate=v_removedate AND pay.EFFECTIVEENDDATE >= per.STARTDATE AND pay.EFFECTIVESTARTDATE < per.endDATE
	-- 					join cs_participant part on part.payeeseq=pay.payeeseq and part.removedate=v_removedate AND part.EFFECTIVEENDDATE >= per.STARTDATE AND part.EFFECTIVESTARTDATE < per.endDATE
	-- 					join cs_position posall on posall.payeeseq=pay.payeeseq and posall.removedate=v_removedate --AND pos2.EFFECTIVEENDDATE >= per.STARTDATE AND pos2.EFFECTIVESTARTDATE < per.endDATE and pos2.effectivestartdate>=to_date('01012023','ddmmyyyy')
	-- 					where poscr.ruleelementownerseq=pc.positionseq and per.periodseq=cc.periodseq
	-- 				) -- = pc.positionseq
	-- 		GROUP BY 
	-- 			cc.unittypeforvalue --cc.unittypeforvalue
	-- 	)
	-- WHERE
	-- 	periodseq = :v_periodRow.periodseq
	-- 	AND creditvalue = 9999999999;

------------------------------------------------------------------------------------------------------------------------------------
--lOGIC
-- 1) PAAYME IN PERIOD SUMMARY TXNS WHERE IN PERIOD pAYMENT TXN
-- 2) Pay Cancellation txns with prior payment

-- 3) Pay Payment txns
------------------------------------------------------------------------------------------------------------------------------------

	UPDATE 
	st
	set st.genericboolean5=0, genericattribute21 = NULL
	from cs_salestransaction st
	inner join ext.TRYG_SH_SALESTXNS_POLICYCRDS cc on st.alternateordernumber = cc.alternateordernumber
	inner join cs_transactionassignment ta on cc.salestransactionseq=ta.salestransactionseq
	WHERE cc.periodseq = :v_periodRow.periodseq
	AND ((st.compensationdate < cc.compensationdate and st.genericnumber2>st.genericnumber1) or (st.genericnumber2 < st.genericnumber1))
	-- AND st.compensationdate < cc.compensationdate
	and st.compensationdate >= cc.LASTPAYMENTDATE
	and cc.compensationdate=ta.compensationdate
	-- and ifnull(ta.genericboolean1,0)=1
	and st.eventtypeseq=:v_etSumm.datatypeseq
	and ifnull(cc.creditvalue,0.0)> 0.0;
	-- and ifnull(cc.creditvalue,9999999999) <> 9999999999;


	CALL EXT.TRYG_LOG(v_procedureName,'Reset paid flags on summary transactions ',::ROWCOUNT);
	COMMIT;	
	
	UPDATE 
	st
	set st.genericboolean5=1
	from cs_salestransaction st
	inner join ext.TRYG_SH_SALESTXNS_POLICYCRDS cc on st.alternateordernumber = cc.alternateordernumber
	inner join cs_transactionassignment ta on cc.salestransactionseq=ta.salestransactionseq
	-- AMR Clawback Paid Flag TBVS2-2180 TBVS2-2149
	WHERE ((cc.periodseq = :v_periodRow.periodseq) OR (st.genericdate3 IS NOT NULL AND st.genericattribute1 = 'AFGA' and cc.periodseq < :v_periodRow.periodseq ))
	--WHERE cc.periodseq = :v_periodRow.periodseq
	-- AMR Clawback Paid Flag TBVS2-2180
	and ((ifnull(ta.genericboolean1,0)=0) or (st.genericdate3 IS NOT NULL AND st.genericattribute1 = 'AFGA') or (st.genericnumber2 < st.genericnumber1))
	-- AND ((st.compensationdate <= cc.compensationdate ) OR (st.genericdate3 IS NOT NULL AND st.genericattribute1 = 'AFGA' ))
	--AND st.compensationdate <= cc.compensationdate
	and st.compensationdate >= cc.LASTPAYMENTDATE
	and cc.compensationdate=ta.compensationdate
	-- AMR Clawback Paid Flag TBVS2-2180
	and ((ifnull(ta.genericboolean1,0)=0) or (st.genericdate3 IS NOT NULL AND st.genericattribute1 = 'AFGA')) -- and ifnull(ta.genericboolean1,0)=1))
	--and ifnull(ta.genericboolean1,0)=0
	and st.eventtypeseq=:v_etSumm.datatypeseq
	and ifnull(cc.creditvalue,0.0)> 0.0
	and ifnull(cc.creditvalue,9999999999) <> 9999999999
	AND ifnull(st.genericboolean5,0) = 0
	AND genericattribute21 IS NOT NULL;
	
	CALL EXT.TRYG_LOG(v_procedureName,'Update summary transactions with paid flag ',::ROWCOUNT);
	commit;



------------------------------------------------------------------------------------------------------------------------------------
--SUMMARY TXNS
------------------------------------------------------------------------------------------------------------------------------------

-- Realease Summary txns in period with payment
merge into cs_transactionassignment ta
using
(
	select distinct tad.salestransactionseq, tad.positionname from cs_salestransaction st
	join cs_transactionassignment tad on tad.salestransactionseq=st.salestransactionseq
	where st.eventtypeseq=:v_etSumm.datatypeseq
	and (st.genericnumber1 < st.genericnumber2 ) or  (st.genericattribute1 in ('GENI','GESK'))
	--2316 AMR 20231026
	-- and st.genericnumber1 < st.genericnumber2
	-- and exists
	-- (
	-- select * from ext.TRYG_SH_SALESTXNS_POLICYCRDS cc
	-- 			WHERE cc.periodseq = :v_periodRow.periodseq
	-- 				AND cc.alternateordernumber = st.alternateordernumber
	-- 				AND ifnull(cc.creditvalue,9999999999) <> 9999999999
	-- 				-- and cc.alternateordernumber='6059994304506'
	-- )
	-- and not exists
	-- --Check if first
	-- (
	-- 	select * from ext.TRYG_SH_SALESTXNS_POLICYCRDS cc
	-- 			WHERE cc.periodseq < :v_periodRow.periodseq
	-- 				AND cc.alternateordernumber = st.alternateordernumber
	-- 				AND ifnull(cc.creditvalue,9999999999) <> 9999999999
	-- 				-- and cc.alternateordernumber='6059994304506'
	-- )
	and exists
	(
		select 1 from cs_salestransaction st_pay
		join cs_transactionassignment tad on tad.salestransactionseq=st_pay.salestransactionseq
		where st_pay.eventtypeseq = :v_etPay.datatypeseq
		AND st_pay.alternateordernumber = st.alternateordernumber			
		and st_pay.compensationdate >= :v_periodRow.startDate 
		and st_pay.compensationdate < :v_periodRow.endDate
		and st_pay.compensationdate>=st.compensationdate
		-- and st_pay.sublinenumber > st.sublinenumber
		-- and st_pay.alternateordernumber='6200003311848'
		)
	-- and not exists
	-- --Check if first
	-- (
	-- 	select 1 from cs_salestransaction st_pay
	-- 	join cs_transactionassignment tad on tad.salestransactionseq=st_pay.salestransactionseq
	-- 	where st_pay.eventtypeseq = :v_etPay.datatypeseq
	-- 	AND st_pay.alternateordernumber = st.alternateordernumber			
	-- 	and st_pay.compensationdate < :v_periodRow.startDate 
	-- 	-- and st_pay.alternateordernumber='6200003311848'
	-- )
	and ifnull(tad.genericboolean1,0)=0
	and tad.compensationdate >= :v_periodRow.startDate --to_date('01022022','ddmmyyyy')
	and tad.compensationdate < :v_periodRow.endDate --to_date('01032022','ddmmyyyy')
) sub on ta.salestransactionseq=sub.salestransactionseq and sub.positionname = ta.positionname
when matched then
update set ta.genericboolean1 =1;

CALL EXT.TRYG_LOG(v_procedureName,'Set immediate release flag for sales summary transactions',::ROWCOUNT);
COMMIT;	


--------------------------------------------------------------------------------------------------------------
---Release CANC/DECR Immediately without the need to wait for payment transaction.
--------------------------------------------------------------------------------------------------------------

update cs_transactionassignment ta set ta.genericboolean1=0 where 
EXISTS(select 1
from ext.tryg_cancel_txns ct where clawbacktype in ('CANC','DECR')
and ta.salestransactionseq=ct.cantxns_salestransactionseq)
and ta.compensationDate >= :v_periodRow.startdate
AND ta.compensationDate < :v_periodRow.enddate
AND ta.genericboolean1=1;

CALL EXT.TRYG_LOG(v_procedureName,'Release CANC/DECR Immediately without the need to wait for payment transaction',::ROWCOUNT);
commit;

UPDATE ta
set ta.genericboolean1 = 1
from cs_transactionassignment ta
join cs_salestransaction st on st.salestransactionseq=ta.salestransactionseq
inner join ext.TRYG_SH_SALESTXNS_POLICYCRDS cc on st.alternateordernumber = cc.alternateordernumber
where st.compensationDate >= :v_periodRow.startdate
AND st.compensationDate < :v_periodRow.enddate
AND cc.periodseq < :v_periodRow.periodseq 
and st.genericnumber1 > st.genericnumber2
-- and st.alternateordernumber ='6055004167025'
and st.eventtypeseq= :v_etSumm.datatypeseq
and st.compensationdate >= cc.LASTPAYMENTDATE
and ifnull(ta.genericboolean1,0)=0
and ifnull(cc.creditvalue,0.0)> 0.0;
-- AND ifnull(st.genericboolean5,0) = 0;
-- AND genericattribute21 IS NULL;

CALL EXT.TRYG_LOG(v_procedureName,'Release clawbacks Immediately without the need to wait for payment transaction',::ROWCOUNT);
commit;

--2439 AMR 17/10/2023
UPDATE st
set st.genericattribute21 = null
from cs_salestransaction st
inner join ext.TRYG_SH_SALESTXNS_POLICYCRDS cc on st.alternateordernumber = cc.alternateordernumber
join cs_transactionassignment ta on st.salestransactionseq=ta.salestransactionseq
where st.compensationDate >= :v_periodRow.startdate
AND st.compensationDate < :v_periodRow.enddate
AND cc.periodseq < :v_periodRow.periodseq 
and st.genericnumber1 > st.genericnumber2
-- and st.alternateordernumber ='6055004167025'
and st.eventtypeseq= :v_etSumm.datatypeseq
and st.compensationdate >= cc.LASTPAYMENTDATE
and ifnull(ta.genericboolean1,0)=0
and ifnull(cc.creditvalue,0.0)> 0.0;
-- AND ifnull(st.genericboolean5,0) = 0;
-- AND genericattribute21 IS NULL;

CALL EXT.TRYG_LOG(v_procedureName,'reset GA21 on Clawback txns',::ROWCOUNT);
commit;

--2439 AMR 17/10/2023
UPDATE st
set st.genericattribute21 = null
from cs_salestransaction st
-- inner join ext.TRYG_SH_SALESTXNS_POLICYCRDS cc on st.alternateordernumber = cc.alternateordernumber
join cs_transactionassignment ta on st.salestransactionseq=ta.salestransactionseq
where st.compensationDate >= :v_periodRow.startdate
AND st.compensationDate < :v_periodRow.enddate
-- AND cc.periodseq < :v_periodRow.periodseq 
-- and st.genericnumber1 > st.genericnumber2
-- and st.alternateordernumber ='6055004167025'
and st.eventtypeseq= :v_etSumm.datatypeseq
-- and st.compensationdate >= cc.LASTPAYMENTDATE
-- and ifnull(ta.genericboolean1,0)=0
and ta.positionname='99999999-OTHER';
-- and ifnull(cc.creditvalue,0.0)> 0.0;
-- AND ifnull(st.genericboolean5,0) = 0;
-- AND genericattribute21 IS NULL;

CALL EXT.TRYG_LOG(v_procedureName,'reset GA21 on 99999999-OTHER txns',::ROWCOUNT);
commit;

merge into cs_transactionassignment ta using
(
select distinct ct.cantxns_alternateordernumber, ct.cantxns_salestransactionseq,ct.cantxns_positionname
from ext.tryg_cancel_txns ct where clawbacktype in ('CANC','DECR')
and EXISTS(select 1 from TRYG_SH_SALESTXNS_POLICYCRDS cc
inner join cs_position pos on 
pos.name=cc.positionname
where cc.alternateordernumber=ct.cantxns_alternateordernumber
--------------------------
--Temp Fix
-- and pos.name =ct.cantxns_positionname 
-- and cc.positionname=ct.cantxns_positionname
and left(pos.name,instr(pos.name,'-')-1) = left(ct.cantxns_positionname ,instr(ct.cantxns_positionname ,'-')-1) 
and left(cc.positionname,instr(cc.positionname,'-')-1) = left(ct.cantxns_positionname ,instr(ct.cantxns_positionname ,'-')-1) 
-----------------------
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

commit;
CALL EXT.TRYG_LOG(v_procedureName,'reset GA21 on Clawback txns',::ROWCOUNT);


merge into cs_transactionassignment ta using
(
select st.alternateordernumber, st.salestransactionseq, tad.positionname
from cs_salestransaction st  
join cs_transactionassignment tad on tad.salestransactionseq=st.salestransactionseq
where st.genericattribute10  in ('CANC','DECR')
and st.compensationDate >= :v_periodRow.startdate
AND st.compensationDate < :v_periodRow.enddate
AND ifnull(tad.genericboolean1,0) = 0
--2439 Fix in period release
--2448 AMR 20231021 Fix so onlhy pays if prior payment
		and (
			exists
				(
					--payment in prior period
					select * from ext.TRYG_SH_SALESTXNS_POLICYCRDS cc
							WHERE cc.periodseq < :v_periodRow.periodseq
								AND cc.alternateordernumber = st.alternateordernumber
				)
				or exists
				(
					--Payment in period
					select 1 from cs_salestransaction st_pay
					join cs_transactionassignment tad on tad.salestransactionseq=st_pay.salestransactionseq
					where st_pay.eventtypeseq = :v_etPay.datatypeseq
					AND st_pay.alternateordernumber = st.alternateordernumber			
					and st_pay.compensationdate >= :v_periodRow.startDate 
					and st_pay.compensationdate < :v_periodRow.endDate
					-- and st_pay.alternateordernumber='6200003311848'
				)
			)
	
	-- and not exists
	-- (
	-- 	select * from ext.TRYG_SH_SALESTXNS_POLICYCRDS cc
	-- 			WHERE cc.periodseq < :v_periodRow.periodseq
	-- 				AND cc.alternateordernumber = st.alternateordernumber
	-- )
	-- and ifnull(tad.genericboolean1,0)=0
) sub on ta.salestransactionseq= sub.salestransactionseq and sub.positionname=ta.positionname
WHEN MATCHED THEN UPDATE
set ta.genericboolean1 = 1;

-- update ta 
-- set ta.genericboolean1 = 1
-- from cs_transactionassignment ta 
-- join cs_salestransaction st on ta.salestransactionseq=st.salestransactionseq
-- where st.genericattribute10  in ('CANC','DECR')
-- and st.eventtypeseq=:v_etSumm.datatypeseq
-- and st.compensationDate >= :v_periodRow.startdate
-- AND st.compensationDate < :v_periodRow.enddate
-- AND ifnull(ta.genericboolean1,0) = 0;
-- and alternateordernumber='6559990792565'

CALL EXT.TRYG_LOG(v_procedureName,'Set immediate release flag for canc/decr summary transactions ' || :v_etSumm.datatypeseq,::ROWCOUNT);
COMMIT;		


------------------------------------------------------------------------------------------------------------------------------------
--PAYMENT TXNS
------------------------------------------------------------------------------------------------------------------------------------

--For every payment transaction identify the corresponding summary transactions and stamp them as paid(gb5) so
--that they are not accounted for the next payment transaction
UPDATE 
st
set genericattribute21 = cc.alternateordernumber || '-' || cc.sublinenumber || '-' || cc.linenumber
from cs_salestransaction st
inner join ext.TRYG_SH_SALESTXNS_POLICYCRDS cc on st.alternateordernumber = cc.alternateordernumber
--2429 AMR 20231024 ----------------------------------------------------
-- inner join cs_transactionassignment ta on cc.salestransactionseq=ta.salestransactionseq
-------------------------------------------------------------------------
inner join cs_transactionassignment ta on st.salestransactionseq=ta.salestransactionseq and cc.positionname=ta.positionname
-- AMR Clawback Paid Flag TBVS2-2180 TBVS2-2149
WHERE ((cc.periodseq = :v_periodRow.periodseq) OR (st.genericdate3 IS NOT NULL AND st.genericattribute1 = 'AFGA' and cc.periodseq < :v_periodRow.periodseq ))
--WHERE cc.periodseq = :v_periodRow.periodseq
--------------AMR 20230821 2150
		AND (
				(st.compensationdate <= cc.compensationdate ) 
				OR (st.compensationdate < :v_periodRow.enddate and st.genericdate3 IS NOT NULL AND st.genericattribute1 = 'AFGA' )
				or (st.compensationdate >= :v_periodRow.startdate
					and st.compensationdate < :v_periodRow.enddate
					and st.genericdate3 IS NULL AND st.genericnumber1 > st.genericnumber2 )
			)
-- AND ((st.compensationdate <= cc.compensationdate ) OR (st.genericdate3 IS NOT NULL AND st.genericattribute1 = 'AFGA' ))
-----------------
--AND st.compensationdate <= cc.compensationdate
------------------------------------------------
and st.compensationdate >= cc.LASTPAYMENTDATE
--2429 AMR 20231024 --------------------------
-- and cc.compensationdate=ta.compensationdate
----------------------------------------------
AND ((ifnull(ta.genericboolean1,0)=0) OR (st.genericdate3 IS NOT NULL AND st.genericattribute1 = 'AFGA' ))
----2429 change amr 20231020
and ifnull(ta.genericboolean1,0)=0
and st.eventtypeseq=:v_etSumm.datatypeseq
---2448 amr 20231020 fix exclude decreases that release immeadiately
and st.genericnumber1 < st.genericnumber2
-- and ifnull(cc.creditvalue,0.0)> 0.0
-- and ifnull(cc.creditvalue,9999999999) <> 9999999999
AND ifnull(st.genericboolean5,0) = 0
--AMR TBVS-2429 stop Clawbacks released from being included
and ifnull(st.genericattribute10,'') not in ('CANC','DECR')
AND genericattribute21 IS NULL;

CALL EXT.TRYG_LOG(v_procedureName,'Update summary transactions with payment txn info ',::ROWCOUNT);
commit;  


	
--For every payment transaction identify the corresponding summary transactions and stamp them as paid(gb5) so
--that they are not accounted for the next payment transaction

	

	UPDATE
		cs_transactionassignment sta
	SET
		(genericnumber1,unittypeforgenericnumber1,genericboolean1) = (
			SELECT
				case when cc.creditvalue = 9999999999 then 0 else cc.creditvalue end,
				CASE WHEN SUBSTR_AFTER (ti.name,'-') in ('GA','PA','DB') THEN :v_ctDKKRow.unittypeseq 
				else :v_ctRow.unittypeseq end ,
				1
			FROM
				TRYG_SH_SALESTXNS_POLICYCRDS cc
			INNER JOIN cs_position pos on pos.ruleelementownerseq=cc.positionseq
			inner join cs_title ti on ti.ruleelementownerseq=pos.titleseq
			--2429 Duplicate restirtion where payment exists in period where dummy agent is no tpayable AMR 20231023
			-- join cs_salestransaction st on cc.alternateordernumber||'-'||cc.sublinenumber||'-'||cc.linenumber = st.genericattribute21 and st.alternateordernumber=st.alternateordernumber
			-- 	and st.compensationdate <  :v_periodRow.startDate
			-- join cs_transactionassignment ta on ta.salestransactionseq=st.salestransactionseq 
			-------------------------------------------------------------------------------------------
			WHERE cc.periodseq = :v_periodRow.periodseq
				--------------2429 Duplicate fix AMR 20231023
				-- and st.salestransactionseq is null
				---------------------------------------------
				and cc.compensationdate between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)--Deepan: added to address multiple versions of positions
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
				and not exists
					( 
						select 1 from cs_salestransaction st
						join cs_transactionassignment ta on ta.salestransactionseq=st.salestransactionseq 
						where cc.alternateordernumber||'-'||cc.sublinenumber||'-'||cc.linenumber = st.genericattribute21 and cc.alternateordernumber=st.alternateordernumber
						and st.compensationdate <  :v_periodRow.startDate
						and ifnull(ta.genericboolean1,0)=1
					)
		);

	
	-- v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Set paid flag for summary transactions',::ROWCOUNT);
	COMMIT;		




-- clear payment flag when no prior payment and is clawback.
-- Depends on Clawback stagehook running first to set cs_Transactionassignment.Genericnumber2

-- merge into cs_transactionassignment ta 
-- using 
-- (
-- select distinct tad.salestransactionseq -- st.alternateordernumber, tad.genericnumber2, tad2.genericnumber2, tad2.* 
-- from cs_salestransaction st
-- join ext.TRYG_SH_SALESTXNS_POLICYCRDS cc on cc.salestransactionseq=st.salestransactionseq
-- join cs_transactionassignment tad on tad.salestransactionseq=st.salestransactionseq
-- join cs_salestransaction st_002 on st.alternateordernumber||'-'||st.sublinenumber||'-'||st.linenumber = st_002.genericattribute21 and st.alternateordernumber=st_002.alternateordernumber
-- 		and st_002.compensationdate < :v_periodRow.endDate
-- join cs_transactionassignment tad2 on tad2.salestransactionseq=st_002.salestransactionseq
-- where cc.periodseq = :v_periodRow.periodseq
-- and tad.genericboolean1=1 and st_002.genericnumber1>st_002.genericnumber2 
-- and tad2.genericnumber2 = 0
-- -- and EXISTS
-- -- (
-- -- select 1
-- -- from ext.tryg_cancel_txns ct where clawbacktype in ('CANC','DECR')
-- -- and tad2.salestransactionseq=ct.cantxns_salestransactionseq
-- -- )
-- ) sub on ta.salestransactionseq=sub.salestransactionseq
-- when matched then update
-- set ta.genericboolean1=0;


-- CALL EXT.TRYG_LOG(v_procedureName,'Clear Payment Flag for Clawback payment Where no prior positive payment is made',:ROWCOUNT);
-- COMMIT;

/*

*/

EXT.TRYG_LOG(v_procedureName,'####   END   ####',NULL);

	
END