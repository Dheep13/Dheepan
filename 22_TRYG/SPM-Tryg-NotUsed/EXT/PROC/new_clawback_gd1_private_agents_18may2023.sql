CREATE OR REPLACE PROCEDURE EXT.TRYG_SH_CLAWBACK ( in_PeriodSeq BIGINT,in_ProcessingUnitSeq BIGINT) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER 
DEFAULT SCHEMA EXT AS 
/*---------------------------------------------------------------------
    | Authors: Sharath K, Deepan
    | Project Title: Consultant
    | Company: SAP Callidus
    | Initial Version Date: 19-April-2022
    |----------------------------------------------------------------------
    | Procedure Purpose: 
    | Version: 0.1	19-April-2022	Intial Version
      Version: 0.2	27-March-2023	Intial Version
      Version: 0.3	06-May-2023	    Clawback changes
      Version: 0.4	15-May-2023	    Clawback changes-Sprint 19
    -----------------------------------------------------------------------
    */
BEGIN
	--Row type variables declarations
	DECLARE v_periodRow ROW LIKE TCMP.CS_PERIOD;
	DECLARE v_puRow ROW LIKE TCMP.CS_PROCESSINGUNIT;
	DECLARE v_unitTypeRow ROW LIKE TCMP.CS_UNITTYPE;
	DECLARE v_unitTypeDKKRow ROW LIKE TCMP.CS_UNITTYPE;
	

	--Variable declarations
	DECLARE v_procedureName VARCHAR(50);
	DECLARE v_slqerrm VARCHAR(4000);
	DECLARE v_eventType VARCHAR(50);

	DECLARE v_removeDate DATE;
	DECLARE v_executionDate TIMESTAMP;
	DECLARE v_lastrunDate TIMESTAMP;

	DECLARE v_Count INT;
	DECLARE v_sqlCount INT;
	DECLARE v_eot date := '2200-01-01';
	DECLARE v_newPositionDate date := '2023-01-01';
    DECLARE stmt_clawback NVARCHAR(100);
    DECLARE stmt_canc_txn NVARCHAR(100);
	-- Exeception Handling
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN v_slqerrm := ::SQL_ERROR_MESSAGE;
		CALL EXT.TRYG_LOG(v_procedureName,'ERROR = '||IFNULL(:v_slqerrm,'') ,NULL);
	END;

	--------------------------------------------------------------------------- 
	v_procedureName = 'TRYG_SH_CLAWBACK';
	v_eventType = 'SC-DK-001-001-SUMMARY';
	v_removeDate = TO_DATE('01/01/2200','mm/dd/yyyy');
	v_executionDate	= current_timestamp;
	v_sqlCount = 0;
	v_Count = 0;

	SELECT * INTO v_puRow FROM TCMP.CS_PROCESSINGUNIT cp WHERE cp.PROCESSINGUNITSEQ = in_ProcessingUnitSeq;
	SELECT * INTO v_periodRow FROM TCMP.CS_PERIOD cp WHERE cp.PERIODSEQ = in_PeriodSeq AND cp.REMOVEDATE = v_removeDate;
	SELECT * INTO v_unitTypeRow FROM TCMP.CS_UNITTYPE cu WHERE cu.REMOVEDATE = v_removeDate AND cu.name = 'quantity';
    SELECT * INTO v_unitTypeDKKRow FROM TCMP.CS_UNITTYPE cu WHERE cu.REMOVEDATE = v_removeDate AND cu.name = 'DKK';

	EXT.TRYG_LOG(v_procedureName,'####   BEGIN   #### '||:v_periodRow.Name,NULL);

	SELECT
		ifnull(max(executionDate), to_timestamp('01/01/1900 00:00:00', 'dd/mm/yyyy HH24:MI:SS'))
		INTO
		v_lastrundate
	FROM
		ext.TRYG_SH_CLAWBACK_LKTB;

	CALL EXT.TRYG_LOG(v_procedureName,'last execution date for lookup table  = '|| v_lastrundate,NULL);
	COMMIT;

	SELECT count(*) INTO v_count
	FROM cs_relationalmdlt mdlt
	WHERE
		mdlt.name = 'LT_Agent_Type_Eligible_Clawback'
		AND mdlt.removedate = v_removedate
		AND mdlt.createdate > v_lastrundate;

	CALL EXT.TRYG_LOG(v_procedureName,'count check for if LT is changed = '|| v_count,NULL);
	COMMIT;

	IF v_count > 0
	THEN
		DELETE FROM ext.tryg_sh_clawback_lktb;
		v_sqlCount = ::ROWCOUNT;
		CALL EXT.TRYG_LOG(v_procedureName,'Deleting existing values to insert modified cell value Complete',v_sqlCount);
		COMMIT;
		
		INSERT INTO ext.tryg_sh_clawback_lktb
		(SELECT
				:v_puRow.TENANTID,
				mdlt.name,
				dim1.name AS dim_Name,
				ind1.minstring dim_indices,
				cell.VALUE,
				mdlt.createdate,
				v_executionDate AS executiondate
			FROM
				cs_relationalmdlt mdlt
			INNER JOIN cs_mdltdimension dim1 ON
				dim1.ruleelementseq = mdlt.ruleelementseq
				AND dim1.dimensionseq = 1
				AND dim1.removedate = v_removedate
				AND dim1.modelseq = 0
			INNER JOIN cs_mdltindex ind1 ON
				ind1.ruleelementseq = mdlt.ruleelementseq
				AND ind1.dimensionseq = dim1.dimensionseq
				AND ind1.removedate = v_removedate
				AND ind1.modelseq = 0
			LEFT JOIN cs_mdltcell cell ON
				cell.mdltseq = MDLT.RULEELEMENTSEQ
				AND cell.removedate = v_removedate
				AND cell.modelseq = 0
				AND dim0index = ind1.ordinal
			WHERE
				mdlt.removedate = v_removedate
				AND mdlt.modelseq = 0 
				AND mdlt.name LIKE 'LT_Agent_Type_Eligible_Clawback'
		);
		v_sqlCount = ::ROWCOUNT;
		CALL EXT.TRYG_LOG(v_procedureName,'Inserting eligible title lookup table values into tryg_sh_clawback_lktb Complete',v_sqlCount);
		COMMIT;
		
	END IF;

	SELECT ifnull(max(executionDate), to_timestamp('01/01/1900 00:00:00', 'dd/mm/yyyy HH24:MI:SS')) INTO v_lastrundate
	FROM ext.tryg_sh_clawback_Txns
	WHERE
		tenantid = :v_puRow.TENANTID
		AND processingunitseq = in_processingunitseq;

	CALL EXT.TRYG_LOG(v_procedureName,'last execution date for transaction table  = '|| v_lastrundate,NULL);
	COMMIT;

------------------------------------------------------------------------------------------------------------------------------------------
	UPDATE cs_salestransaction st
	SET genericattribute10 = 'DECR'
	WHERE
		st.compensationdate >= :v_periodRow.startDate
		AND st.compensationdate < :v_periodRow.enddate
		AND st.genericdate3 IS NULL
		AND st.genericnumber1 > st.genericnumber2
		AND st.eventtypeseq IN (SELECT DATAtypeseq FROM cs_eventtype WHERE removedate = v_removedate AND eventtypeid = v_eventType)
		AND st.genericattribute10 IS NULL 
		AND EXISTS (
		SELECT
			st_in.salestransactionseq AS cantxns_salestransactionseq,
			st_in.linenumber AS cantxns_linenumber,
			st_in.sublinenumber AS cantxns_sublinenumber,
			st_in.alternateordernumber AS cantxns_alternateordernumber,
			st_in.genericnumber1 AS cantxns_Old_premium,
			st_in.genericnumber2 AS cantxns_new_premium,
			st_in.genericdate1 AS cantxns_policy_sDate,
			st_in.genericdate2 AS cantxns_policy_eDate,
			st_in.genericdate3 AS cantxns_policy_cDate,
			st_in.*
		FROM
			cs_salestransaction st_in
		WHERE
			st_in.compensationdate >= :v_periodRow.startDate
			AND st_in.compensationdate < :v_periodRow.enddate
			AND st_in.genericdate3 IS NULL
			AND st_in.genericnumber1 < st_in.genericnumber2
			AND st_in.eventtypeseq IN (SELECT DATAtypeseq FROM cs_eventtype WHERE removedate = v_removedate AND eventtypeid = v_eventType)
			AND st_in.alternateordernumber = st.alternateordernumber 
			AND IFNULL(st_in.genericdate1,to_date('01/01/2000','mm/dd/yyyy')) = IFNULL(st.genericdate1,to_date('01/01/2000','mm/dd/yyyy'))
			AND IFNULL(st_in.genericdate2,to_date('01/01/2000','mm/dd/yyyy')) = IFNULL(st.genericdate2,to_date('01/01/2000','mm/dd/yyyy'))

		)
		AND NOT EXISTS (select * from cs_credit cc 
			where cc.genericattribute3 = to_varchar(st.alternateordernumber)
			-- and IFNULL(cc.periodseq, :v_periodRow.periodseq) <= :v_periodRow.periodseq
		    AND cc.compensationdate >= (st.genericdate1)
			AND cc.compensationdate < add_months(st.genericdate1,12)
		)
		AND st.genericattribute1 not in ('GENI','GESK');

	v_sqlCount = ::ROWCOUNT;	
	CALL EXT.TRYG_LOG(v_procedureName,'Updating the genericattribute10 for DECR txns having new txns in same month',v_sqlCount);	
	COMMIT;

	UPDATE cs_salestransaction st
	SET genericattribute10 = 'CANC'
	WHERE
		st.compensationdate >= :v_periodRow.startDate
		AND st.compensationdate < :v_periodRow.enddate
		AND st.genericdate3 IS NULL
		AND st.genericnumber1 > st.genericnumber2
		AND st.eventtypeseq IN (SELECT DATAtypeseq FROM cs_eventtype WHERE removedate = v_removedate AND eventtypeid = v_eventType)
		AND st.genericattribute10 IS NULL 
		AND EXISTS (
		SELECT
			st_in.salestransactionseq AS cantxns_salestransactionseq,
			st_in.linenumber AS cantxns_linenumber,
			st_in.sublinenumber AS cantxns_sublinenumber,
			st_in.alternateordernumber AS cantxns_alternateordernumber,
			st_in.genericnumber1 AS cantxns_Old_premium,
			st_in.genericnumber2 AS cantxns_new_premium,
			st_in.genericdate1 AS cantxns_policy_sDate,
			st_in.genericdate2 AS cantxns_policy_eDate,
			st_in.genericdate3 AS cantxns_policy_cDate,
			st_in.*
		FROM
			cs_salestransaction st_in
		WHERE
			st_in.compensationdate >= :v_periodRow.startDate
			AND st_in.compensationdate < :v_periodRow.enddate
			AND st_in.genericdate3 IS NOT NULL
			AND st_in.genericnumber1 < st_in.genericnumber2
			AND st_in.eventtypeseq IN (SELECT DATAtypeseq FROM cs_eventtype WHERE removedate = v_removedate AND eventtypeid = v_eventType)
			AND st_in.alternateordernumber = st.alternateordernumber 
			AND IFNULL(st_in.genericdate1,to_date('01/01/2000','mm/dd/yyyy')) = IFNULL(st.genericdate1,to_date('01/01/2000','mm/dd/yyyy'))
			AND IFNULL(st_in.genericdate2,to_date('01/01/2000','mm/dd/yyyy')) = IFNULL(st.genericdate2,to_date('01/01/2000','mm/dd/yyyy'))

		)
		AND NOT EXISTS (select * from cs_credit cc 
			where cc.genericattribute3 = to_varchar(st.alternateordernumber)
		    AND cc.compensationdate >= (st.genericdate1)
			AND cc.compensationdate < add_months(st.genericdate1,12)
		)
		AND genericattribute1 not in ('GENI','GESK');

	v_sqlCount = ::ROWCOUNT;	
	CALL EXT.TRYG_LOG(v_procedureName,'Updating the genericattribute10 for CANC txns having new txns in same month',v_sqlCount);	
	COMMIT;

----Reset cancel, decrease and gennikraft changes
delete from cs_transactionassignment ta where exists
(select * from cs_transactionassignment ta_in
inner join cs_salestransaction st on
ta_in.salestransactionseq =st.salestransactionseq
where ta.salestransactionseq=st.salestransactionseq
and ta.salestransactionseq=ta_in.salestransactionseq
and st.compensationdate >= :v_periodRow.startDate
and st.compensationdate < :v_periodRow.endDate
-- add_days(:v_periodRow.endDate,-1)
and ta.genericattribute4 in ('Gennikraft Insert', 'Cancel Insert', 'Decrease Insert'))
and ta.genericattribute4 in ('Gennikraft Insert', 'Cancel Insert', 'Decrease Insert');

commit;

update cs_transactionassignment ta set genericnumber2=0, genericnumber3=0, genericattribute4=NULL
where exists (select * from cs_transactionassignment ta_in
inner join cs_salestransaction st on
ta_in.salestransactionseq =st.salestransactionseq
where ta.salestransactionseq=st.salestransactionseq
and ta.salestransactionseq=ta_in.salestransactionseq
and st.compensationdate >= :v_periodRow.startDate
and st.compensationdate < :v_periodRow.endDate
--  :v_periodRow.startDate
-- and add_days(:v_periodRow.endDate,-1)
and ta.genericattribute4 in ('Gennikraft Update', 'Cancel Update', 'Decrease Update')
)
and ta.genericattribute4 in ('Gennikraft Update', 'Cancel Update', 'Decrease Update');

commit;

stmt_clawback := 'TRUNCATE TABLE ext.tryg_clawback_credits';
stmt_canc_txn := 'TRUNCATE TABLE ext.tryg_cancel_txns';
EXECUTE IMMEDIATE :stmt_clawback;
EXECUTE IMMEDIATE :stmt_canc_txn;

insert into ext.tryg_cancel_txns  (
                                        SELECT DISTINCT 
                                        'CANC' as clawbacktype,
                                        CASE WHEN SUBSTR_AFTER (ti.name,'-') in ('GA','PA','DB') THEN True else false end as isprivateagent,
                                        st_in.alternateordernumber as cantxns_alternateordernumber,
                                        sta_in.positionname as canc_positionname,
                                        st_in.salestransactionseq AS cantxns_salestransactionseq,
					                    st_in.salesorderseq AS cantxns_salesorderseq,
					                    st_in.eventtypeseq AS cantxns_eventtypeseq,
					                    st_in.linenumber AS cantxns_linenumber,
                                        st_in.sublinenumber AS cantxns_sublinenumber,
                                        st_in.compensationdate AS cantxns_compdate,
                                        st_in.genericnumber1 AS cantxns_Old_premium,
                                        st_in.genericnumber2 AS cantxns_new_premium,
                                        st_in.genericdate1 AS cantxns_policy_sDate,
                                        st_in.genericdate2 AS cantxns_policy_eDate,
                                        st_in.genericdate3 AS cantxns_policy_cDate,
                                        sta_in.positionname AS cantxns_positionname,
                                        pos.ruleelementownerseq as cantxns_positionseq
                                        
                                        FROM cs_salestransaction st_in
                                        INNER JOIN cs_transactionassignment sta_in ON 
                                        sta_in.salestransactionseq = st_in.salestransactionseq				 
                                        AND sta_in.compensationdate = st_in.compensationdate
                                        INNER JOIN cs_eventtype et ON 
                                        et.datatypeseq = st_in.eventtypeseq
                                        AND et.removedate =:v_removeDate
                                        INNER JOIN cs_position pos ON 
                                        pos.name=sta_in.positionname
                                        AND pos.removedate =:v_removeDate
                                        INNER JOIN cs_title ti on
                                        pos.titleseq=ti.ruleelementownerseq
                                        -- and SUBSTR_AFTER (ti.name,'-') not in ('GA','PA','DB')
                                        and ti.removedate=:v_removeDate
                                        WHERE st_in.genericdate3 IS NOT NULL
                                        AND st_in.genericattribute1 = 'AFGA'
                                        AND sta_in.processingunitseq =:v_puRow.processingunitseq
                                        AND st_in.genericnumber1 > st_in.genericnumber2
                                        AND et.eventtypeid =  :v_eventType
                                        AND st_in.compensationdate >=:v_periodRow.startDate
                                        AND st_in.compensationdate < :v_periodRow.endDate
                                        
                                        union
                                        
                                        
                                       SELECT DISTINCT 
                                       'DECR' as clawbacktype,
                                       CASE WHEN SUBSTR_AFTER (ti.name,'-') in ('GA','PA','DB') THEN True else false end as isprivateagent,
                                       st_in.alternateordernumber as cantxns_alternateordernumber,
                                        sta_in.positionname as canc_positionname,
                                        st_in.salestransactionseq AS cantxns_salestransactionseq,
					                    st_in.salesorderseq AS cantxns_salesorderseq,
					                    st_in.eventtypeseq AS cantxns_eventtypeseq,
					                    st_in.linenumber AS cantxns_linenumber,
                                        st_in.sublinenumber AS cantxns_sublinenumber,
                                        st_in.compensationdate AS cantxns_compdate,
                                        st_in.genericnumber1 AS cantxns_Old_premium,
                                        st_in.genericnumber2 AS cantxns_new_premium,
                                        st_in.genericdate1 AS cantxns_policy_sDate,
                                        st_in.genericdate2 AS cantxns_policy_eDate,
                                        st_in.genericdate3 AS cantxns_policy_cDate,
                                        sta_in.positionname AS cantxns_positionname,
                                        -- sta_in.salesorderseq AS cantxns_salesorderseq,
                                        
                                        pos.ruleelementownerseq as cantxns_positionseq
                                        
                                        FROM cs_salestransaction st_in
                                        INNER JOIN cs_transactionassignment sta_in ON 
                                        sta_in.salestransactionseq = st_in.salestransactionseq				 
                                        AND sta_in.compensationdate = st_in.compensationdate
                                        INNER JOIN cs_eventtype et ON 
                                        et.datatypeseq = st_in.eventtypeseq
                                        AND et.removedate =:v_removeDate
                                        INNER JOIN cs_position pos ON 
                                        pos.name=sta_in.positionname
                                        AND pos.removedate =:v_removeDate
                                        INNER JOIN cs_title ti on
                                        pos.titleseq=ti.ruleelementownerseq
                                        -- and SUBSTR_AFTER (ti.name,'-') not in ('GA','PA','DB')
                                        and ti.removedate=:v_removeDate
                                        WHERE st_in.genericdate3 IS NULL
                                        AND st_in.genericattribute1 <> 'AFGA'
                                        and st_in.genericattribute1 not in ('GENI', 'GESK')
                                        AND sta_in.processingunitseq =:v_puRow.processingunitseq
                                        AND st_in.genericnumber1 > st_in.genericnumber2
                                        AND et.eventtypeid =  :v_eventType
                                        AND st_in.compensationdate >= :v_periodRow.startDate
                                        AND st_in.compensationdate < :v_periodRow.endDate
                                
                                        union

                                        SELECT DISTINCT 
                                        'GENI' as clawbacktype,
                                        CASE WHEN SUBSTR_AFTER (ti.name,'-') in ('GA','PA','DB') THEN True else false end as isprivateagent,
                                        st_in.alternateordernumber as geni_alternateordernumber,
                                        sta_in.positionname as canc_positionname,
                                        st_in.salestransactionseq AS geni_salestransactionseq,
					                    st_in.salesorderseq AS geni_salesorderseq,
					                    st_in.eventtypeseq AS geni_eventtypeseq,
					                    st_in.linenumber AS geni_linenumber,
                                        st_in.sublinenumber AS geni_sublinenumber,
                                        st_in.compensationdate AS geni_compdate,
                                        st_in.genericnumber1 AS geni_Old_premium,
                                        st_in.genericnumber2 AS geni_new_premium,
                                        st_in.genericdate1 AS geni_policy_sDate,
                                        st_in.genericdate2 AS geni_policy_eDate,
                                        st_in.genericdate3 AS geni_policy_cDate,
                                        sta_in.positionname AS geni_positionname,
                                        -- sta_in.salesorderseq AS geni_salesorderseq,
                                        pos.ruleelementownerseq as geni_positionseq
                                        FROM cs_salestransaction st_in
                                        INNER JOIN cs_transactionassignment sta_in ON 
                                        sta_in.salestransactionseq = st_in.salestransactionseq				 
                                        AND sta_in.compensationdate = st_in.compensationdate
                                        INNER JOIN cs_eventtype et ON 
                                        et.datatypeseq = st_in.eventtypeseq
                                        AND et.removedate =:v_removeDate
                                        INNER JOIN cs_position pos ON 
                                        pos.name=sta_in.positionname
                                        AND pos.removedate = :v_removeDate
                                        INNER JOIN cs_title ti on
                                        pos.titleseq=ti.ruleelementownerseq
                                        and ti.removedate=:v_removeDate
                                        WHERE st_in.genericdate3 IS NULL
                                        AND st_in.genericattribute1 in ('GENI','GESK')
                                        AND sta_in.processingunitseq =:v_puRow.processingunitseq
                                        -- AND st_in.genericnumber1 > st_in.genericnumber2
                                        AND et.eventtypeid = :v_eventType
                                        AND st_in.compensationdate >= :v_periodRow.startDate
                                        AND st_in.compensationdate <  :v_periodRow.endDate);

CALL EXT.TRYG_LOG(v_procedureName,'Inserting canc/decr/geni transactions for current period Complete',::ROWCOUNT);
COMMIT;
insert into ext.tryg_clawback_credits (
	                        select c.creditseq,
                            'CANC' as clawbacktype,
	                        c.periodseq,
	                        c.salestransactionseq,
			                c.name,
                            pos.name as prev_positionname,
			                ifnull(c.value,0) as credit_value,
			                c.compensationdate as credit_compdate,
			                prev_sublinenumber,
                            prev_alternateordernumber,
                            prev_compensationdate,
                            ifnull(prev_latestpremium,0) as prev_latestpremium,
                            prev_salestransactionseq,
                            prev_policy_sDate,
                            prev_policy_eDate,
                            cantxns_alternateordernumber ,
                            cantxns_salestransactionseq,
                            cantxns_salesorderseq,
                            cantxns_linenumber,
                            cantxns_sublinenumber,
                            cantxns_compdate,
                            cantxns_Old_premium,
                            cantxns_new_premium,
                            cantxns_policy_sDate,
                            cantxns_policy_eDate,
                            cantxns_policy_cDate,
                            cantxns_positionname,
                            cantxns_positionseq from (SELECT ROW_NUMBER() OVER (
                                    PARTITION BY st_prev.alternateordernumber
                                    ORDER BY st_prev.compensationdate DESC, st_prev.sublinenumber desc
                                ) row_num,
                                 st_prev.eventtypeseq as prev_eventtypeseq,
                                st_prev.sublinenumber as prev_sublinenumber,
                                st_prev.alternateordernumber as prev_alternateordernumber,
                                st_prev.compensationdate as prev_compensationdate,
                                st_prev.genericnumber2 as prev_latestpremium,
                                st_prev.salestransactionseq as prev_salestransactionseq,
                                st_prev.genericdate1 as prev_policy_sDate,
                                st_prev.genericdate2 as prev_policy_eDate,
                                decr_txn.cantxns_alternateordernumber as cantxns_alternateordernumber,
                                decr_txn.cantxns_salesorderseq as cantxns_salesorderseq,
                                decr_txn.cantxns_salestransactionseq as cantxns_salestransactionseq,
                               
                                decr_txn.cantxns_eventtypeseq as cantxns_eventtypeseq,
                                decr_txn.cantxns_linenumber  as cantxns_linenumber,
                                decr_txn.cantxns_sublinenumber  as cantxns_sublinenumber,
                                decr_txn.cantxns_compdate  as cantxns_compdate,
                                decr_txn.cantxns_Old_premium  as cantxns_Old_premium,
                                decr_txn.cantxns_new_premium  as cantxns_new_premium,
                                decr_txn.cantxns_policy_sDate as cantxns_policy_sDate,
                                decr_txn.cantxns_policy_eDate as cantxns_policy_eDate,
                                decr_txn.cantxns_policy_cDate as cantxns_policy_cDate,
                                decr_txn.cantxns_positionname as cantxns_positionname,
                                decr_txn.cantxns_positionseq as cantxns_positionseq,
                                decr_txn.isprivateagent as isprivateagent

                            FROM cs_salestransaction st_prev,
                                ext.tryg_cancel_txns decr_txn, ---current period identify the decrease transactions,
                                cs_transactionassignment ta_prev
                                where decr_txn.clawbacktype='CANC'
                                and st_prev.compensationdate < decr_txn.cantxns_compdate
                                -- and st_prev.alternateordernumber =6200014176670
                                and st_prev.compensationdate >= (decr_txn.cantxns_policy_sDate)
                                and st_prev.compensationdate < add_months(decr_txn.cantxns_policy_sDate,12)
                                and ((st_prev.genericdate1=decr_txn.cantxns_policy_sDate and decr_txn.isprivateagent=False)
                                or (decr_txn.isprivateagent=True))
                                and st_prev.alternateordernumber = decr_txn.cantxns_alternateordernumber
                                -- and ta_prev.positionname = decr_txn.canc_positionname
                                and ta_prev.salestransactionseq = st_prev.salestransactionseq
                                and st_prev.eventtypeseq =decr_txn.cantxns_eventtypeseq
                                and st_prev.genericdate3 is null
                                and st_prev.genericattribute1 <> 'AFGA'
                                and st_prev.genericattribute1 not in ('GENI', 'GESK')
                                and exists(select 1 from ext.TRYG_SH_SALESTXNS_POLICYCRDS pc --only txns which have payments
                                			where pc.compensationdate < decr_txn.cantxns_compdate
                            				 -- and st_prev.alternateordernumber =6200014176670
                                			and pc.compensationdate >= (decr_txn.cantxns_policy_sDate)
                                			and pc.compensationdate < add_months(decr_txn.cantxns_policy_sDate,12)
                                			and ifnull(pc.creditvalue,0.0)> 0.0
                                			and ifnull(pc.creditvalue,9999999999) <> 9999999999
                                			and pc.alternateordernumber=decr_txn.cantxns_alternateordernumber
                                			-- and decr_txn.canc_positionname=pc.positionname
                                			and pc.eventtypeid='SC-DK-001-002'
                                )
                                ) final left join
                               cs_credit c on
                               final.cantxns_alternateordernumber=c.genericattribute3
                               and ((c.genericdate1=final.cantxns_policy_sDate and final.isprivateagent = False)
                               or(final.isprivateagent = True))
                               and c.compensationdate <= final.cantxns_compdate
                               and c.genericattribute7 not in ('Afgang','Ikraft nedsæt')
                               and c.compensationdate >= (final.cantxns_policy_sDate)
                               and c.compensationdate < add_months(final.cantxns_policy_sDate,12)
                               inner join cs_position pos on
                               pos.ruleelementownerseq=c.positionseq
                            --    and c.positionseq=cantxns_positionseq
                               --where final.prev_alternateordernumber=6200014176670
                               where pos.removedate=:v_removeDate
                               and final.row_num=1);

CALL EXT.TRYG_LOG(v_procedureName,'Inserting sum of previous credits for current period canc txns complete',::ROWCOUNT);
COMMIT;
insert into ext.tryg_clawback_credits (
	                        select distinct c.creditseq,
	                        'DECR' as clawbacktype,
	                        c.periodseq,
	                        c.salestransactionseq,
			                c.name,
                            pos.name as prev_positionname,
			                ifnull(c.value,0) as credit_value,
			                c.compensationdate as credit_compdate,
			                prev_sublinenumber,
                            prev_alternateordernumber,
                            prev_compensationdate,
                            ifnull(prev_latestpremium,0) as prev_latestpremium,
                            prev_salestransactionseq,
                            prev_policy_sDate,
                            prev_policy_eDate,
                            cantxns_alternateordernumber ,
                            cantxns_salestransactionseq,
                            cantxns_salesorderseq,
                            cantxns_linenumber,
                            cantxns_sublinenumber,
                            cantxns_compdate,
                            cantxns_Old_premium,
                            cantxns_new_premium,
                            cantxns_policy_sDate,
                            cantxns_policy_eDate,
                            cantxns_policy_cDate,
                            cantxns_positionname,
                            cantxns_positionseq from (SELECT ROW_NUMBER() OVER (
                                    PARTITION BY st_prev.alternateordernumber, decr_txn.cantxns_positionname
                                    ORDER BY st_prev.compensationdate DESC, st_prev.sublinenumber desc
                                ) row_num,
                                 st_prev.eventtypeseq as prev_eventtypeseq,
                                st_prev.sublinenumber as prev_sublinenumber,
                                st_prev.alternateordernumber as prev_alternateordernumber,
                                st_prev.compensationdate as prev_compensationdate,
                                st_prev.genericnumber2 as prev_latestpremium,
                                st_prev.salestransactionseq as prev_salestransactionseq,
                                st_prev.genericdate1 as prev_policy_sDate,
                                st_prev.genericdate2 as prev_policy_eDate,
                                decr_txn.cantxns_alternateordernumber as cantxns_alternateordernumber ,
                                decr_txn.cantxns_salesorderseq as cantxns_salesorderseq,
                                -- decr_txn.cantxns_positionname as cantxns_positionname,
                                decr_txn.cantxns_salestransactionseq as cantxns_salestransactionseq,
                               
                                decr_txn.cantxns_eventtypeseq as cantxns_eventtypeseq,
                                decr_txn.cantxns_linenumber  as cantxns_linenumber,
                                decr_txn.cantxns_sublinenumber  as cantxns_sublinenumber,
                                decr_txn.cantxns_compdate  as cantxns_compdate,
                                decr_txn.cantxns_Old_premium  as cantxns_Old_premium,
                                decr_txn.cantxns_new_premium  as cantxns_new_premium,
                                decr_txn.cantxns_policy_sDate as cantxns_policy_sDate,
                                decr_txn.cantxns_policy_eDate as cantxns_policy_eDate,
                                decr_txn.cantxns_policy_cDate as cantxns_policy_cDate,
                                decr_txn.cantxns_positionname as cantxns_positionname,
                                decr_txn.cantxns_positionseq as cantxns_positionseq,
                                decr_txn.isprivateagent as isprivateagent

                            FROM cs_salestransaction st_prev,
                                ext.tryg_cancel_txns decr_txn,---current period identify the decrease transactions,
                                cs_transactionassignment ta_prev
                                where decr_txn.clawbacktype='DECR'
                                and st_prev.compensationdate < decr_txn.cantxns_compdate
                                -- and st_prev.alternateordernumber =6200014176670
                                and st_prev.compensationdate >= (decr_txn.cantxns_policy_sDate)
                                and st_prev.compensationdate < add_months(decr_txn.cantxns_policy_sDate,12)
                                and ((st_prev.genericdate1=decr_txn.cantxns_policy_sDate and decr_txn.isprivateagent=false)
                                or(decr_txn.isprivateagent=True))
                                and st_prev.alternateordernumber = decr_txn.cantxns_alternateordernumber
                                -- and ta_prev.positionname = decr_txn.canc_positionname
                                and ta_prev.salestransactionseq = st_prev.salestransactionseq
                                and st_prev.eventtypeseq =decr_txn.cantxns_eventtypeseq
                                and st_prev.genericdate3 is null
                                and st_prev.genericattribute1 <> 'AFGA'
                                and exists(select 1 from ext.TRYG_SH_SALESTXNS_POLICYCRDS pc --only txns which have payments
                                			where pc.compensationdate < decr_txn.cantxns_compdate
                            				 -- and st_prev.alternateordernumber =6200014176670
                                			and pc.compensationdate >= (decr_txn.cantxns_policy_sDate)
                                			and pc.compensationdate < add_months(decr_txn.cantxns_policy_sDate,12)
                                			and ifnull(pc.creditvalue,0.0)> 0.0
                                			and ifnull(pc.creditvalue,9999999999) <> 9999999999
                                			and pc.alternateordernumber=decr_txn.cantxns_alternateordernumber
                                			-- and decr_txn.canc_positionname=pc.positionname
                                			and pc.eventtypeid='SC-DK-001-002'
                                )
                                ) final left join
                               cs_credit c on
                               final.cantxns_alternateordernumber=c.genericattribute3
                               and ((c.genericdate1=final.cantxns_policy_sDate and final.isprivateagent = False) 
                               or(final.isprivateagent=True))
                               and c.compensationdate <= final.cantxns_compdate
                               and c.genericattribute7 not in ('Afgang','Ikraft nedsæt')
                               and c.compensationdate >= (final.cantxns_policy_sDate)
                               and c.compensationdate < add_months(final.cantxns_policy_sDate,12)
                               inner join cs_position pos on
                               pos.ruleelementownerseq=c.positionseq
                            --    and c.positionseq=cantxns_positionseq
                               --where final.prev_alternateordernumber=6200014176670
                               where pos.removedate=:v_removeDate
                               and final.row_num=1);  

CALL EXT.TRYG_LOG(v_procedureName,'Inserting sum of previous credits for current period decr txns complete',::ROWCOUNT);
COMMIT;
insert into ext.tryg_clawback_credits (
	                        select c.creditseq,
	                        'GENI' as clawbacktype,
	                        c.periodseq,
	                        c.salestransactionseq,
			                c.name,
			                pos.name as prev_positionname,
			                ifnull(c.value,0) as credit_value,
			                c.compensationdate as credit_compdate,
			                prev_sublinenumber,
                            prev_alternateordernumber,
                            prev_compensationdate,
                            ifnull(prev_latestpremium,0) as prev_latestpremium,
                            prev_salestransactionseq,
                            prev_policy_sDate,
                            prev_policy_eDate,
                            geni_alternateordernumber ,
                            geni_salestransactionseq,
                            geni_salesorderseq,
                            geni_linenumber,
                            geni_sublinenumber,
                            geni_compdate,
                            geni_Old_premium,
                            geni_new_premium,
                            geni_policy_sDate,
                            geni_policy_eDate,
                            geni_policy_cDate,
                            geni_positionname,
                            geni_positionseq from (SELECT ROW_NUMBER() OVER (
                                    PARTITION BY st_prev.alternateordernumber
                                    ORDER BY st_prev.compensationdate DESC, st_prev.sublinenumber desc
                                ) row_num,
                                 st_prev.eventtypeseq as prev_eventtypeseq,
                                st_prev.sublinenumber as prev_sublinenumber,
                                st_prev.alternateordernumber as prev_alternateordernumber,
                                st_prev.compensationdate as prev_compensationdate,
                                st_prev.genericnumber1 as prev_latestpremium,
                                st_prev.salestransactionseq as prev_salestransactionseq,
                                st_prev.genericdate1 as prev_policy_sDate,
                                st_prev.genericdate2 as prev_policy_eDate,
                                geni_txn.cantxns_alternateordernumber as geni_alternateordernumber ,
                                geni_txn.cantxns_salesorderseq as geni_salesorderseq,
                                -- geni_txn.geni_positionname as geni_positionname,
                                geni_txn.cantxns_salestransactionseq as geni_salestransactionseq,
                               
                                geni_txn.cantxns_eventtypeseq as geni_eventtypeseq,
                                geni_txn.cantxns_linenumber  as geni_linenumber,
                                geni_txn.cantxns_sublinenumber  as geni_sublinenumber,
                                geni_txn.cantxns_compdate  as geni_compdate,
                                geni_txn.cantxns_Old_premium  as geni_Old_premium,
                                geni_txn.cantxns_new_premium  as geni_new_premium,
                                geni_txn.cantxns_policy_sDate as geni_policy_sDate,
                                geni_txn.cantxns_policy_eDate as geni_policy_eDate,
                                geni_txn.cantxns_policy_cDate as geni_policy_cDate,
                                geni_txn.cantxns_positionname as geni_positionname,
                                geni_txn.cantxns_positionseq as geni_positionseq

                            FROM cs_salestransaction st_prev,
                                 ext.tryg_cancel_txns geni_txn,---current period identify the decrease transactions,
                                cs_transactionassignment ta_prev
                                where geni_txn.clawbacktype='GENI'---current period identify the geni transactions,
                                and st_prev.compensationdate < geni_txn.cantxns_compdate
                                and st_prev.genericdate3 <=geni_txn.cantxns_compdate
                                and st_prev.genericdate3 > add_months(geni_txn.cantxns_compdate,-12)
                                and st_prev.alternateordernumber = geni_txn.cantxns_alternateordernumber
                                and ta_prev.salestransactionseq = st_prev.salestransactionseq
                                and st_prev.eventtypeseq =geni_txn.cantxns_eventtypeseq
                                and st_prev.genericdate3 is not null
                                and st_prev.genericattribute1 = 'AFGA'
                                and exists(select 1 from ext.TRYG_SH_SALESTXNS_POLICYCRDS pc --only transactions which have payments 
                                			where pc.compensationdate < geni_txn.cantxns_compdate
                            				 -- and st_prev.alternateordernumber =6200014176670
                                			and st_prev.genericdate3 <= pc.compensationdate
                            				and st_prev.genericdate3 > add_months(pc.compensationdate,-12)
                                			and ifnull(pc.creditvalue,0.0)> 0.0
                                			and ifnull(pc.creditvalue,9999999999) <> 9999999999
                                			and pc.alternateordernumber=geni_txn.cantxns_alternateordernumber
                                			-- and geni_txn.canc_positionname=pc.positionname
                                			and pc.eventtypeid='SC-DK-001-002'
                                )
                                ) final left join
                               cs_credit c on
                               final.geni_alternateordernumber=c.genericattribute3
                            --    and c.genericdate1=final.geni_policy_sDate
                               and c.salestransactionseq=final.prev_salestransactionseq
                               and c.compensationdate <= final.geni_compdate
                               and c.compensationdate > add_months(final.geni_compdate,-12)
                               and c.genericattribute7='Afgang'
                               inner join cs_position pos on
                               pos.ruleelementownerseq=c.positionseq
                            --    and c.positionseq=final.geni_positionseq
                               where pos.removedate=:v_removeDate
                               and final.row_num=1); 

CALL EXT.TRYG_LOG(v_procedureName,'Inserting sum of previous geni credits for current period geni txns complete',::ROWCOUNT);
COMMIT;

----position name change after 2023-01-01
merge into ext.tryg_clawback_credits clc
using
(
select pos_canc.name as cantxns_positionname, clc.cantxns_positionseq as cantxns_positionseq, 
clc.creditseq as creditseq, pos_prev.name as prev_positionname, clc.cantxns_salestransactionseq from ext.tryg_clawback_credits clc
inner join cs_position pos_canc
on pos_canc.ruleelementownerseq=clc.cantxns_positionseq
and pos_canc.removedate =:v_removeDate
and pos_canc.effectivestartdate = :v_newPositionDate
and pos_canc.effectiveenddate = :v_removeDate

inner join cs_position pos_prev
on pos_prev.name =clc.prev_positionname
and pos_prev.removedate =:v_removeDate
and pos_prev.effectivestartdate < :v_newPositionDate
and pos_prev.effectiveenddate = :v_removeDate
and pos_prev.payeeseq=pos_canc.payeeseq
where clc.clawbacktype='CANC'
) sub on 
clc.cantxns_positionname=sub.cantxns_positionname
and clc.prev_positionname=sub.prev_positionname
and clc.cantxns_salestransactionseq = sub.cantxns_salestransactionseq
and clc.creditseq=sub.creditseq
when matched then 
update set clc.prev_positionname=clc.cantxns_positionname;



------Begin update/insert on assignments for cancel, decrease and gennikraft 
merge into cs_transactionassignment ta
using
(select distinct cantxns_alternateordernumber, cantxns_salestransactionseq, prev_latestpremium, prev_positionname,
sum(credit_value) as credit_value from ext.tryg_clawback_credits clc
where clawbacktype='CANC'
group by  cantxns_alternateordernumber,cantxns_salestransactionseq,prev_latestpremium,prev_positionname) sub
on ta.salestransactionseq =  sub.cantxns_salestransactionseq
and ta.positionname=sub.prev_positionname
WHEN MATCHED THEN
    UPDATE SET ta.genericnumber2 = ifnull(credit_value,0), ta.genericnumber3 = sub.prev_latestpremium,
    ta.unittypeforgenericnumber2 =:v_unitTypeRow.unittypeseq  ,ta.unittypeforgenericnumber3 = :v_unitTypeRow.unittypeseq,
    ta.genericattribute4='Cancel Update';

CALL EXT.TRYG_LOG(v_procedureName,'Updating the txnassign genericnumber2 for cancel txns with credit value',::ROWCOUNT);	



merge into cs_transactionassignment ta
using
(select clc.*,
ROW_NUMBER() OVER (PARTITION BY A.cantxns_salestransactionseq) +ta.setnumber as new_setnumber,
A.sum_credit_value 
from (select cantxns_alternateordernumber, cantxns_salestransactionseq, prev_latestpremium,cantxns_positionname,
sum(credit_value) as sum_credit_value from ext.tryg_clawback_credits clc
where clawbacktype='CANC'
group by  cantxns_alternateordernumber,cantxns_salestransactionseq,prev_latestpremium,cantxns_positionname) A
inner join ext.tryg_clawback_credits clc on
clc.cantxns_salestransactionseq = A.cantxns_salestransactionseq
inner join cs_credit cr on
clc.creditseq=cr.creditseq
inner join cs_transactionassignment ta on
ta.salestransactionseq= clc.cantxns_salestransactionseq
and ta.salestransactionseq=A.cantxns_salestransactionseq
) sub
on ta.salestransactionseq = sub.cantxns_salestransactionseq
and ta.positionname = sub.prev_positionname

WHEN NOT MATCHED THEN
    INSERT (tenantid,salestransactionseq,setnumber,positionname,compensationdate,salesorderseq,processingunitseq,
    genericnumber2,unittypeforgenericnumber2,genericnumber3,unittypeforgenericnumber3,genericattribute4) 
    values ( :v_puRow.tenantid, sub.cantxns_salestransactionseq ,sub.new_setnumber,
    sub.prev_positionname, sub.cantxns_compdate, sub.cantxns_salesorderseq, :v_puRow.processingunitseq , 
    ifnull(sub.credit_value,0),:v_unitTypeRow.unittypeseq, ifnull(sub.prev_latestpremium,0), :v_unitTypeRow.unittypeseq, 'Cancel Insert');


CALL EXT.TRYG_LOG(v_procedureName,'Updating the txnassign genericnumber2 for cancel txns where position names are different with credit value',::ROWCOUNT);	

----position name change after 2023-01-01
merge into ext.tryg_clawback_credits clc
using
(
select pos_canc.name as cantxns_positionname, clc.cantxns_positionseq as cantxns_positionseq, 
clc.creditseq as creditseq, pos_prev.name as prev_positionname, clc.cantxns_salestransactionseq from ext.tryg_clawback_credits clc
inner join cs_position pos_canc
on pos_canc.ruleelementownerseq=clc.cantxns_positionseq
and pos_canc.removedate =:v_removeDate
and pos_canc.effectivestartdate = :v_newPositionDate
and pos_canc.effectiveenddate = :v_removeDate

inner join cs_position pos_prev
on pos_prev.name =clc.prev_positionname
and pos_prev.removedate =:v_removeDate
and pos_prev.effectivestartdate < :v_newPositionDate
and pos_prev.effectiveenddate = :v_removeDate
and pos_prev.payeeseq=pos_canc.payeeseq
where clc.clawbacktype='DECR'
) sub on 
clc.cantxns_positionname=sub.cantxns_positionname
and clc.prev_positionname=sub.prev_positionname
and clc.cantxns_salestransactionseq = sub.cantxns_salestransactionseq
and clc.creditseq=sub.creditseq
when matched then 
update set clc.prev_positionname=clc.cantxns_positionname;


merge into cs_transactionassignment ta
using
(select cantxns_salestransactionseq,cantxns_alternateordernumber, prev_positionname,prev_latestpremium,
sum(credit_value) as sum_credit_value from ext.tryg_clawback_credits 
where clawbacktype='DECR'
group by  cantxns_salestransactionseq,cantxns_alternateordernumber, prev_positionname,prev_latestpremium
) sub
on ta.salestransactionseq = sub.cantxns_salestransactionseq
and ta.positionname=sub.prev_positionname
WHEN MATCHED THEN
    UPDATE SET ta.genericnumber2 = ifnull(sum_credit_value,0), ta.genericnumber3 = sub.prev_latestpremium,
    ta.unittypeforgenericnumber2 =:v_unitTypeRow.unittypeseq  ,ta.unittypeforgenericnumber3 = :v_unitTypeRow.unittypeseq,
    ta.genericattribute4='Decrease Update';

CALL EXT.TRYG_LOG(v_procedureName,'Updating the txnassign genericnumber2 for Decrease txns with credit value',::ROWCOUNT);	


merge into cs_transactionassignment ta
using
(select clc.*,
ROW_NUMBER() OVER (PARTITION BY A.cantxns_salestransactionseq) +ta.setnumber as new_setnumber,
A.sum_credit_value 
from (select cantxns_alternateordernumber, cantxns_salestransactionseq, prev_latestpremium,cantxns_positionname,
sum(credit_value) as sum_credit_value from ext.tryg_clawback_credits clc
where clawbacktype='DECR'
group by  cantxns_alternateordernumber,cantxns_salestransactionseq,prev_latestpremium,cantxns_positionname) A
inner join ext.tryg_clawback_credits clc on
clc.cantxns_salestransactionseq = A.cantxns_salestransactionseq
inner join cs_credit cr on
clc.creditseq=cr.creditseq
inner join cs_transactionassignment ta on
ta.salestransactionseq= clc.cantxns_salestransactionseq
and ta.salestransactionseq=A.cantxns_salestransactionseq
) sub
on ta.salestransactionseq = sub.cantxns_salestransactionseq
and ta.positionname = sub.prev_positionname

WHEN NOT MATCHED THEN
    INSERT (tenantid,salestransactionseq,setnumber,positionname,compensationdate,salesorderseq,processingunitseq,
    genericnumber2,unittypeforgenericnumber2,genericnumber3,unittypeforgenericnumber3,genericattribute4) 
    values ( :v_puRow.tenantid, sub.cantxns_salestransactionseq ,sub.new_setnumber,
    sub.prev_positionname, sub.cantxns_compdate, sub.cantxns_salesorderseq, :v_puRow.processingunitseq , 
    ifnull(sub.credit_value,0),:v_unitTypeRow.unittypeseq, ifnull(sub.prev_latestpremium,0), :v_unitTypeRow.unittypeseq, 'Decrease Insert');


CALL EXT.TRYG_LOG(v_procedureName,'Updating the txnassign genericnumber2 for Decrease txns where position names are different with credit value',::ROWCOUNT);	

COMMIT;

---Gennikraft logic
----position name change after 2023-01-01
merge into ext.tryg_clawback_credits clc
using
(
select pos_canc.name as cantxns_positionname, clc.cantxns_positionseq as cantxns_positionseq, 
clc.creditseq as creditseq, pos_prev.name as prev_positionname, clc.cantxns_salestransactionseq from ext.tryg_clawback_credits clc
inner join cs_position pos_canc
on pos_canc.ruleelementownerseq=clc.cantxns_positionseq
and pos_canc.removedate =:v_removeDate
and pos_canc.effectivestartdate = :v_newPositionDate
and pos_canc.effectiveenddate = :v_removeDate

inner join cs_position pos_prev
on pos_prev.name =clc.prev_positionname
and pos_prev.removedate =:v_removeDate
and pos_prev.effectivestartdate < :v_newPositionDate
and pos_prev.effectiveenddate = :v_removeDate
and pos_prev.payeeseq=pos_canc.payeeseq
where clc.clawbacktype='GENI'
) sub on 
clc.cantxns_positionname=sub.cantxns_positionname
and clc.prev_positionname=sub.prev_positionname
and clc.cantxns_salestransactionseq = sub.cantxns_salestransactionseq
and clc.creditseq=sub.creditseq
when matched then 
update set clc.prev_positionname=clc.cantxns_positionname;


merge into cs_transactionassignment ta
using
(select cantxns_salestransactionseq,credit_value, prev_latestpremium, cantxns_positionname from ext.tryg_clawback_credits clc
where clc.clawbacktype='GENI'
) sub
on ta.salestransactionseq = sub.cantxns_salestransactionseq
and ta.positionname = sub.cantxns_positionname
WHEN MATCHED THEN
    UPDATE SET ta.genericnumber2 = ifnull(sub.credit_value,0), ta.genericnumber3 = sub.prev_latestpremium,
    ta.unittypeforgenericnumber2 =:v_unitTypeDKKRow.unittypeseq  ,ta.unittypeforgenericnumber3 = :v_unitTypeDKKRow.unittypeseq,
    ta.genericattribute4='Gennikraft Update';

CALL EXT.TRYG_LOG(v_procedureName,'Updating the txnassign genericnumber2 for genikraft txns with credit value',::ROWCOUNT);


merge into cs_transactionassignment ta
using
(select cantxns_salestransactionseq, cantxns_compdate,
ROW_NUMBER() OVER (PARTITION BY cantxns_salestransactionseq) +ta.setnumber as new_setnumber,
 cantxns_alternateordernumber ,cantxns_positionname, cantxns_salesorderseq,
 pos.name as prev_positionname, credit_value, prev_latestpremium from ext.tryg_clawback_credits clc
inner join cs_credit c on
c.creditseq = clc.creditseq
inner join cs_position pos on
c.positionseq=pos.ruleelementownerseq
inner join cs_transactionassignment ta on
ta.salestransactionseq=cantxns_salestransactionseq
where clc.clawbacktype='GENI'
and pos.removedate=:v_removeDate
) sub
on ta.salestransactionseq = sub.cantxns_salestransactionseq
and ta.positionname = sub.prev_positionname

WHEN NOT MATCHED THEN
    INSERT (tenantid,salestransactionseq,setnumber,positionname,compensationdate,salesorderseq,processingunitseq,
    genericnumber2,unittypeforgenericnumber2,genericnumber3,unittypeforgenericnumber3,genericattribute4) 
    values ( :v_puRow.tenantid, sub.cantxns_salestransactionseq ,sub.new_setnumber,
    sub.prev_positionname, sub.cantxns_compdate, sub.cantxns_salesorderseq, :v_puRow.processingunitseq , 
    ifnull(sub.credit_value,0),:v_unitTypeDKKRow.unittypeseq, ifnull(sub.prev_latestpremium,0), :v_unitTypeDKKRow.unittypeseq, 'Gennikraft Insert');


CALL EXT.TRYG_LOG(v_procedureName,'Updating the txnassign genericnumber2 for genikraft txns where position names are different with credit value',::ROWCOUNT);	

merge into cs_transactionassignment tas 
using
(select ta.salestransactionseq as ta_salestransactionseq, ta.setnumber as ta_setnumber,
CASE WHEN ta.genericnumber2 is NULL THEN 0 ELSE ta.genericnumber2 END as ta_genericnumber2,
CASE WHEN ta.genericnumber3 is NULL THEN 0 ELSE ta.genericnumber3 END as ta_genericnumber3,
CASE WHEN st.genericattribute1 in ('GENI','GESK') THEN :v_unitTypeDKKRow.unittypeseq else :v_unitTypeRow.unittypeseq
END as ta_unittypeforgenericnumber
from cs_transactionassignment ta, cs_salestransaction st 
where st.compensationdate >= :v_periodRow.startDate
and st.compensationdate < :v_periodRow.endDate
and st.eventtypeseq=(select datatypeseq from cs_eventtype where eventtypeid= :v_eventType and removedate = :v_removeDate)
and st.salestransactionseq=ta.salestransactionseq
and st.processingunitseq=:v_puRow.processingunitseq
and ta.genericattribute4 is NULL and (ta.genericnumber2 is null or ta.genericnumber3 is null)
and ta.compensationdate >= :v_periodRow.startDate
AND ta.compensationdate < :v_periodRow.endDate) subq on 
tas.salestransactionseq = subq.ta_salestransactionseq
and tas.setnumber=ta_setnumber
and tas.compensationdate >= :v_periodRow.startDate
and tas.compensationdate < :v_periodRow.endDate
WHEN MATCHED THEN
    UPDATE SET tas.genericnumber2 = ta_genericnumber2, tas.genericnumber3 = ta_genericnumber3,
    tas.unittypeforgenericnumber2 =ta_unittypeforgenericnumber ,tas.unittypeforgenericnumber3 = ta_unittypeforgenericnumber;


CALL EXT.TRYG_LOG(v_procedureName,'Updating the txnassignments gn2 and gn3 with zeros for genikraft,canc,decr txns where no prior credits/txns exists',::ROWCOUNT);	

commit;

--update latest transaction's ta.gd6 with previous increase transaction's gd2(policy enddate) 
merge into cs_transactionassignment ta 
using (
select st.alternateordernumber,st.genericdate2 as policyenddate,tas.salestransactionseq as latest_salestransactionseq, tas.setnumber as latest_setnumber
,tas.compensationdate as latest_compensationdate
from
cs_transactionassignment tas 
inner join cs_salestransaction st on
st.salestransactionseq=tas.salestransactionseq
inner join (select A.row_num, alternateordernumber, prev_salestransactionseq, latest_salestransactionseq, latest_policyenddate, latest_sublinenumber from (
SELECT ROW_NUMBER() OVER ( PARTITION BY st.alternateordernumber ORDER BY st.compensationdate desc, st.genericdate2 DESC,
st.sublinenumber desc ) as row_num, st.alternateordernumber as alternateordernumber, st.salestransactionseq as prev_salestransactionseq, 
st_new.salestransactionseq as latest_salestransactionseq, st.genericdate2 as latest_policyenddate, st.sublinenumber as latest_sublinenumber
from ext.tryg_clawback_credits clc
inner join cs_salestransaction st
on
st.compensationdate <= clc.cantxns_compdate
and st.compensationdate >= (clc.cantxns_policy_sDate)
and st.compensationdate < add_months(clc.cantxns_policy_sDate,12)
-- and st.genericdate1= clc.cantxns_policy_sDate
and st.alternateordernumber = clc.cantxns_alternateordernumber
and st.eventtypeseq =(select  datatypeseq from cs_eventtype where eventtypeid= :v_eventType and removedate=:v_eot)
inner join cs_transactionassignment ta
on ta.salestransactionseq = st.salestransactionseq
inner join cs_salestransaction st_new
on st_new.alternateordernumber=clc.cantxns_alternateordernumber
and st_new.salestransactionseq=clc.cantxns_salestransactionseq

where st.genericdate3 is null
and st.genericattribute1 <> 'AFGA'
and st.genericnumber2 > st.genericnumber1
-- and st_new.salestransactionseq = tas.salestransactionseq
and st.genericattribute1 not in ('GENI', 'GESK')) A
where A.row_num=1) latest_enddate

on latest_enddate.latest_salestransactionseq=tas.salestransactionseq
and latest_enddate.latest_salestransactionseq=st.salestransactionseq

where exists  (select clc_sub.cantxns_salestransactionseq from ext.tryg_clawback_credits clc_sub
	-- where  clc_sub.cantxns_compdate between '2023-01-01' and '2023-01-31'
	where clc_sub.cantxns_compdate >= :v_periodRow.startDate
	and clc_sub.cantxns_compdate < :v_periodRow.endDate
	and clc_sub.cantxns_salestransactionseq=tas.salestransactionseq
-- 	and clc_sub.cantxns_alternateordernumber in (6200004199488,
-- 6200004284502,
-- 6000000106302)
	
)) sub 
on ta.salestransactionseq=sub.latest_salestransactionseq
and ta.setnumber=sub.latest_setnumber
and ta.compensationdate = sub.latest_compensationdate

when matched then 
update set ta.genericdate6 = sub.policyenddate;

CALL EXT.TRYG_LOG(v_procedureName,'Updated txnassignments gd6 of decr/cancel txn with gd1(policyEndDate) of previous increase txn',::ROWCOUNT);	


	COMMIT;
	EXT.TRYG_LOG(v_procedureName,'####   END   ####',NULL);

	
END