CREATE PROCEDURE EXT.TRYG_SH_CLAWBACK ( in_PeriodSeq BIGINT,in_ProcessingUnitSeq BIGINT) 
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
    -----------------------------------------------------------------------
    */
BEGIN
	--Row type variables declarations
	DECLARE v_periodRow ROW LIKE TCMP.CS_PERIOD;
	DECLARE v_puRow ROW LIKE TCMP.CS_PROCESSINGUNIT;
	DECLARE v_unitTypeRow ROW LIKE TCMP.CS_UNITTYPE;

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
    DECLARE stmt NVARCHAR(500);


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


stmt := 'TRUNCATE TABLE ext.tryg_clawback_credits';
EXECUTE IMMEDIATE :stmt;
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
                                decr_txn.cantxns_alternateordernumber as cantxns_alternateordernumber ,
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
                                decr_txn.cantxns_positionseq as cantxns_positionseq

                            FROM cs_salestransaction st_prev,
                                (
                                    SELECT DISTINCT st_in.alternateordernumber as cantxns_alternateordernumber,
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
                                        AND et.removedate ='2200-01-01'
                                        INNER JOIN cs_position pos ON 
                                        pos.name=sta_in.positionname
                                        AND pos.removedate = '2200-01-01'
                                        WHERE st_in.genericdate3 IS NOT NULL
                                        AND st_in.genericattribute1 = 'AFGA'
                                        AND sta_in.processingunitseq =38280596832649318
                                        AND st_in.genericnumber1 > st_in.genericnumber2
                                        AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
                                        AND st_in.compensationdate >= '2022-12-01'
                                        AND st_in.compensationdate < '2023-12-31'
                                ) decr_txn,---current period identify the decrease transactions,
                                cs_transactionassignment ta_prev
                                where st_prev.compensationdate < decr_txn.cantxns_compdate
                                -- and st_prev.alternateordernumber =6200014176670
                                and st_prev.compensationdate >= (decr_txn.cantxns_policy_sDate)
                                and st_prev.compensationdate < add_months(decr_txn.cantxns_policy_sDate,12)
                                and st_prev.genericdate1= decr_txn.cantxns_policy_sDate
                                and st_prev.alternateordernumber = decr_txn.cantxns_alternateordernumber
                                and ta_prev.positionname = decr_txn.canc_positionname
                                and ta_prev.salestransactionseq = st_prev.salestransactionseq
                                and st_prev.eventtypeseq =decr_txn.cantxns_eventtypeseq
                                and st_prev.genericdate3 is null
                                and st_prev.genericattribute1 <> 'AFGA'
                                and st_prev.genericattribute1 not in ('GENI', 'GESK')
                                ) final left join
                               cs_credit c on
                               final.cantxns_alternateordernumber=c.genericattribute3
                               and c.genericdate1=final.cantxns_policy_sDate
                               and c.compensationdate <= final.cantxns_compdate
                               and c.genericattribute7 not in ('Afgang','Ikraft nedsæt')
                               and c.compensationdate >= (final.cantxns_policy_sDate)
                               and c.compensationdate < add_months(final.cantxns_policy_sDate,12)
                               inner join cs_position pos on
                               pos.ruleelementownerseq=c.positionseq
                            --    and c.positionseq=cantxns_positionseq
                               --where final.prev_alternateordernumber=6200014176670
                               where pos.removedate='2200-01-01'
                               and final.row_num=1);

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
                                decr_txn.cantxns_positionseq as cantxns_positionseq

                            FROM cs_salestransaction st_prev,
                                (
                                    SELECT DISTINCT st_in.alternateordernumber as cantxns_alternateordernumber,
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
                                        AND et.removedate ='2200-01-01'
                                        INNER JOIN cs_position pos ON 
                                        pos.name=sta_in.positionname
                                        AND pos.removedate = '2200-01-01'
                                        WHERE st_in.genericdate3 IS NULL
                                        AND st_in.genericattribute1 <> 'AFGA'
                                        and st_in.genericattribute1 not in ('GENI', 'GESK')
                                        AND sta_in.processingunitseq =38280596832649318
                                        AND st_in.genericnumber1 > st_in.genericnumber2
                                        AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
                                        AND st_in.compensationdate >= '2022-12-01'
                                        AND st_in.compensationdate < '2023-12-31'
                                ) decr_txn,---current period identify the decrease transactions,
                                cs_transactionassignment ta_prev
                                where st_prev.compensationdate < decr_txn.cantxns_compdate
                                -- and st_prev.alternateordernumber =6200014176670
                                and st_prev.compensationdate >= (decr_txn.cantxns_policy_sDate)
                                and st_prev.compensationdate < add_months(decr_txn.cantxns_policy_sDate,12)
                                and st_prev.genericdate1=decr_txn.cantxns_policy_sDate
                                and st_prev.alternateordernumber = decr_txn.cantxns_alternateordernumber
                                -- and ta_prev.positionname = decr_txn.canc_positionname
                                and ta_prev.salestransactionseq = st_prev.salestransactionseq
                                and st_prev.eventtypeseq =decr_txn.cantxns_eventtypeseq
                                and st_prev.genericdate3 is null
                                and st_prev.genericattribute1 <> 'AFGA'
                                ) final left join
                               cs_credit c on
                               final.cantxns_alternateordernumber=c.genericattribute3
                               and c.genericdate1=final.cantxns_policy_sDate
                               and c.compensationdate <= final.cantxns_compdate
                               and c.genericattribute7 not in ('Afgang','Ikraft nedsæt')
                               and c.compensationdate >= (final.cantxns_policy_sDate)
                               and c.compensationdate < add_months(final.cantxns_policy_sDate,12)
                               inner join cs_position pos on
                               pos.ruleelementownerseq=c.positionseq
                            --    and c.positionseq=cantxns_positionseq
                               --where final.prev_alternateordernumber=6200014176670
                               where pos.removedate='2200-01-01'
                               and final.row_num=1);  




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
                                st_prev.genericnumber2 as prev_latestpremium,
                                st_prev.salestransactionseq as prev_salestransactionseq,
                                geni_txn.geni_alternateordernumber as geni_alternateordernumber ,
                                geni_txn.geni_salesorderseq as geni_salesorderseq,
                                -- geni_txn.geni_positionname as geni_positionname,
                                geni_txn.geni_salestransactionseq as geni_salestransactionseq,
                               
                                geni_txn.geni_eventtypeseq as geni_eventtypeseq,
                                geni_txn.geni_linenumber  as geni_linenumber,
                                geni_txn.geni_sublinenumber  as geni_sublinenumber,
                                geni_txn.geni_compdate  as geni_compdate,
                                geni_txn.geni_Old_premium  as geni_Old_premium,
                                geni_txn.geni_new_premium  as geni_new_premium,
                                geni_txn.geni_policy_sDate as geni_policy_sDate,
                                geni_txn.geni_policy_eDate as geni_policy_eDate,
                                geni_txn.geni_policy_cDate as geni_policy_cDate,
                                geni_txn.geni_positionname as geni_positionname,
                                geni_txn.geni_positionseq as geni_positionseq

                            FROM cs_salestransaction st_prev,
                                (
                                    SELECT DISTINCT st_in.alternateordernumber as geni_alternateordernumber,
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
                                        AND et.removedate ='2200-01-01'
                                        INNER JOIN cs_position pos ON 
                                        pos.name=sta_in.positionname
                                        AND pos.removedate = '2200-01-01'
                                        WHERE st_in.genericdate3 IS NULL
                                        AND st_in.genericattribute1 in ('GENI','GESK')
                                        AND sta_in.processingunitseq =38280596832649318
                                        -- AND st_in.genericnumber1 > st_in.genericnumber2
                                        AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
                                        AND st_in.compensationdate >= '2022-12-01'
                                        AND st_in.compensationdate < '2023-12-31'
                                ) geni_txn,---current period identify the geni transactions,
                                cs_transactionassignment ta_prev
                                where st_prev.compensationdate < geni_txn.geni_compdate
                                -- and st_prev.alternateordernumber =6200014176670
                                and st_prev.compensationdate >= (geni_txn.geni_policy_sDate)
                                and st_prev.compensationdate < add_months(geni_txn.geni_policy_sDate,12)
                                and st_prev.genericdate1=geni_txn.geni_policy_sDate
                                and st_prev.alternateordernumber = geni_txn.geni_alternateordernumber
                                and ta_prev.positionname = geni_txn.canc_positionname
                                and ta_prev.salestransactionseq = st_prev.salestransactionseq
                                and st_prev.eventtypeseq =geni_txn.geni_eventtypeseq
                                and st_prev.genericdate3 is not null
                                and st_prev.genericattribute1 = 'AFGA'
                                ) final left join
                               cs_credit c on
                               final.geni_alternateordernumber=c.genericattribute3
                            --    and c.genericdate1=final.geni_policy_sDate
                               and c.salestransactionseq=final.prev_salestransactionseq
                               and c.compensationdate <= final.geni_compdate
                               and c.genericattribute7='Afgang'
                               and c.compensationdate >= (final.geni_policy_sDate)
                               and c.compensationdate < add_months(final.geni_policy_sDate,12)
                               inner join cs_position pos on
                               pos.ruleelementownerseq=c.positionseq
                            --    and c.positionseq=final.geni_positionseq
                               where pos.removedate='2200-01-01'
                               and final.row_num=1); 


----Reset cancel, decrease and gennikraft changes
delete from cs_transactionassignment ta where exists
(select * from cs_transactionassignment ta_in
inner join cs_salestransaction st on
ta_in.salestransactionseq =st.salestransactionseq
where ta.salestransactionseq=st.salestransactionseq
and ta.salestransactionseq=ta_in.salestransactionseq
and st.compensationdate between '2022-12-01'
and '2022-12-31'
-- add_days(:v_periodRow.endDate,-1)
and ta.genericattribute4 in ('Gennikraft Insert', 'Cancel Insert', 'Decrease Insert'))
and ta.genericattribute4 in ('Gennikraft Insert', 'Cancel Insert', 'Decrease Insert');

commit;

update cs_transactionassignment ta set genericnumber2=0, genericnumber3=0
where exists (select * from cs_transactionassignment ta_in
inner join cs_salestransaction st on
ta_in.salestransactionseq =st.salestransactionseq
where ta.salestransactionseq=st.salestransactionseq
and ta.salestransactionseq=ta_in.salestransactionseq
and st.compensationdate between '2022-12-01'
and '2022-12-31'
--  :v_periodRow.startDate
-- and add_days(:v_periodRow.endDate,-1)
and ta.genericattribute4 in ('Gennikraft Update', 'Cancel Update', 'Decrease Update')
)
and ta.genericattribute4 in ('Gennikraft Update', 'Cancel Update', 'Decrease Update');

commit;

------Being uodate/insert on assignments for cancel, decrease and gennikraft 
merge into cs_transactionassignment ta
using
(select cantxns_alternateordernumber, cantxns_salestransactionseq, prev_latestpremium,cantxns_positionname,
sum(credit_value) as credit_value from ext.tryg_clawback_credits clc
where clawbacktype='CANC'
group by  cantxns_alternateordernumber,cantxns_salestransactionseq,prev_latestpremium,cantxns_positionname) sub
on ta.salestransactionseq =  sub.cantxns_salestransactionseq
and ta.positionname=sub.cantxns_positionname
WHEN MATCHED THEN
    UPDATE SET ta.genericnumber2 = ifnull(credit_value,0), ta.genericnumber3 = sub.prev_latestpremium,
    ta.unittypeforgenericnumber2 =1970324836974600  ,ta.unittypeforgenericnumber3 = 1970324836974600,
    ta.genericattribute4='Cancel Update';


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
    values ( '1951', sub.cantxns_salestransactionseq ,sub.new_setnumber,
    sub.prev_positionname, sub.cantxns_compdate, sub.cantxns_salesorderseq, 38280596832649318 , 
    ifnull(sub.credit_value,0),1970324836974601, ifnull(sub.prev_latestpremium,0), 1970324836974601, 'Cancel Insert');


CALL EXT.TRYG_LOG(v_procedureName,'Updating the txnassign genericnumber2 for cancel txns with credit value',::ROWCOUNT);	


merge into cs_transactionassignment ta
using
(select cantxns_salestransactionseq,cantxns_alternateordernumber, cantxns_positionname,prev_latestpremium,
sum(credit_value) as sum_credit_value from ext.tryg_clawback_credits 
where clawbacktype='DECR'
and cantxns_salestransactionseq =14636698833188273
group by  cantxns_salestransactionseq,cantxns_alternateordernumber, cantxns_positionname,prev_latestpremium
) sub
on ta.salestransactionseq = sub.cantxns_salestransactionseq
and ta.positionname=sub.cantxns_positionname
WHEN MATCHED THEN
    UPDATE SET ta.genericnumber2 = ifnull(sum_credit_value,0), ta.genericnumber3 = sub.prev_latestpremium,
    ta.unittypeforgenericnumber2 =1970324836974600  ,ta.unittypeforgenericnumber3 = 1970324836974600,
    ta.genericattribute4='Decrease Update';


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
    values ( '1951', sub.cantxns_salestransactionseq ,sub.new_setnumber,
    sub.prev_positionname, sub.cantxns_compdate, sub.cantxns_salesorderseq, 38280596832649318 , 
    ifnull(sub.credit_value,0),1970324836974601, ifnull(sub.prev_latestpremium,0), 1970324836974601, 'Decrease Insert');


CALL EXT.TRYG_LOG(v_procedureName,'Updating the txnassign genericnumber2 for Decrease txns with credit value',::ROWCOUNT);	
COMMIT;


---Gennikraft logic

merge into cs_transactionassignment ta
using
(select cantxns_salestransactionseq,credit_value, prev_latestpremium, cantxns_positionname from ext.tryg_clawback_credits clc
where clc.clawbacktype='GENI'
) sub
on ta.salestransactionseq = sub.cantxns_salestransactionseq
and ta.positionname = sub.cantxns_positionname
WHEN MATCHED THEN
    UPDATE SET ta.genericnumber2 = ifnull(sub.credit_value,0), ta.genericnumber3 = sub.prev_latestpremium,
    ta.unittypeforgenericnumber2 =1970324836974600  ,ta.unittypeforgenericnumber3 = 1970324836974600,
    ta.genericattribute4='Gennikraft Update'

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
and pos.removedate='2200-01-01'
) sub
on ta.salestransactionseq = sub.cantxns_salestransactionseq
and ta.positionname = sub.prev_positionname

WHEN NOT MATCHED THEN
    INSERT (tenantid,salestransactionseq,setnumber,positionname,compensationdate,salesorderseq,processingunitseq,
    genericnumber2,unittypeforgenericnumber2,genericnumber3,unittypeforgenericnumber3,genericattribute4) 
    values ( '1951', sub.cantxns_salestransactionseq ,sub.new_setnumber,
    sub.prev_positionname, sub.cantxns_compdate, sub.cantxns_salesorderseq, 38280596832649318 , 
    ifnull(sub.credit_value,0),1970324836974601, ifnull(sub.prev_latestpremium,0), 1970324836974601, 'Gennikraft Insert');

CALL EXT.TRYG_LOG(v_procedureName,'Updating the txnassign genericnumber2 for GENI txns with credit value',::ROWCOUNT);

/*

merge into cs_transactionassignment ta 
using 
(
SELECT rn, geni_salestransactionseq, geni_compensationdate, ROW_NUMBER() OVER (ORDER BY  geni_setnumber) + geni_setnumber as geni_setnumber_rn, geni_salesorderseq, canc_positionname, alternateordernumber, canc_sublinenumber, 
canc_compensationdate, cancellationdate, canc_oldpremium, canc_credit
from (
SELECT DISTINCT 
ROW_NUMBER() OVER (PARTITION BY st_in.alternateordernumber ORDER by st_in.compensationdate desc , st_in.sublinenumber desc ,st_in.genericdate3 desc) AS rn,
st_in.alternateordernumber,
st_in.sublinenumber as canc_sublinenumber,
st_in.compensationdate as canc_compensationdate,
geni_txn.salestransactionseq as geni_salestransactionseq,
geni_txn.salesorderseq as geni_salesorderseq,
geni_txn.compensationdate as geni_compensationdate,
geni_txn.positionname as geni_positionname,
geni_txn.setnumber as geni_setnumber,
sta_in.positionname as canc_positionname,
st_in.genericdate3 as cancellationdate,
st_in.genericnumber1 as canc_oldpremium,
--st_in.genericnumber2 as newpremium,
sum(IFNULL(c.value, 0)) as canc_credit
FROM cs_salestransaction st_in
INNER JOIN cs_transactionassignment sta_in ON sta_in.salestransactionseq = st_in.salestransactionseq
AND sta_in.compensationdate = st_in.compensationdate
INNER JOIN cs_position pos
on pos.name=sta_in.positionname
and pos.removedate = :v_eot
LEFT join cs_credit c on 
st_in.salestransactionseq=c.salestransactionseq
and sta_in.compensationdate=c.compensationdate
and st_in.compensationdate=c.compensationdate
and st_in.alternateordernumber=c.genericattribute3
and pos.ruleelementownerseq=c.positionseq
INNER JOIN cs_eventtype et ON et.datatypeseq = st_in.eventtypeseq
AND et.removedate = :v_eot
INNER JOIN (select st.alternateordernumber, st.salestransactionseq, st.salesorderseq, ta.positionname,ta.setnumber,st.compensationdate 
from cs_salestransaction st, cs_transactionassignment ta
where st.compensationdate between :v_periodRow.startDate
and add_days(:v_periodRow.endDate,-1) 
and st.genericattribute1 in ('GENI','GESK')
and ta.salestransactionseq=st.salestransactionseq
and ta.compensationdate = st.compensationdate
and st.eventtypeseq=(select datatypeseq from cs_eventtype where eventtypeid=:v_eventType
and removedate=:v_eot
)) geni_txn
on st_in.alternateordernumber=geni_txn.alternateordernumber
WHERE st_in.genericdate3 IS NOT NULL
AND st_in.genericattribute4='AFGA'
AND sta_in.processingunitseq = :v_puRow.processingunitseq
AND st_in.genericnumber1 > st_in.genericnumber2
AND geni_txn.compensationdate > st_in.compensationdate
AND et.eventtypeid = :v_eventType
AND geni_txn.compensationdate >= (st_in.genericdate3)
AND geni_txn.compensationdate < add_months(st_in.genericdate3,12)
group by st_in.alternateordernumber,
st_in.sublinenumber,
st_in.compensationdate,
st_in.genericdate3,
geni_txn.compensationdate,
geni_txn.salestransactionseq,
sta_in.positionname,
st_in.genericnumber1,
geni_txn.positionname,
geni_txn.salesorderseq ,
geni_txn.compensationdate,
geni_txn.setnumber
order by st_in.compensationdate desc , st_in.sublinenumber desc ,st_in.genericdate3 desc) A
where rn=1
) subq
on ta.salestransactionseq=subq.geni_salestransactionseq
and ta.positionname=subq.canc_positionname
WHEN MATCHED THEN
    UPDATE SET ta.genericnumber2 = ifnull(subq.canc_credit,0), ta.genericnumber3 = subq.canc_oldpremium,
    ta.unittypeforgenericnumber2 = :v_unitTypeRow.unittypeseq ,ta.unittypeforgenericnumber3 = :v_unitTypeRow.unittypeseq ,
    ta.genericattribute4 ='Gennikraft Update'
WHEN NOT MATCHED THEN
    INSERT (tenantid,salestransactionseq,setnumber,positionname,compensationdate,salesorderseq,processingunitseq,
    genericnumber2,unittypeforgenericnumber2,genericnumber3,unittypeforgenericnumber3,genericattribute4) 
    values ( :v_puRow.tenantid, subq.geni_salestransactionseq ,subq.geni_setnumber_rn,
    subq.canc_positionname, subq.geni_compensationdate, subq.geni_salesorderseq, :v_puRow.processingunitseq , 
    ifnull(subq.canc_credit,0),:v_unitTypeRow.unittypeseq, ifnull(subq.canc_oldpremium,0), :v_unitTypeRow.unittypeseq, 'Gennikraft');

	v_sqlCount = ::ROWCOUNT;	
	CALL EXT.TRYG_LOG(v_procedureName,'Updating the cancelled old premium and cancelled credits for genikraft txns ',v_sqlCount);	
*/
	COMMIT;
	EXT.TRYG_LOG(v_procedureName,'####   END   ####',NULL);

	
END

----queries
---should we check if position between the cancelled and the previous(credit) match?