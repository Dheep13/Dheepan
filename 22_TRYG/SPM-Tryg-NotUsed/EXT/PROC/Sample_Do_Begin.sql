do begin

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
    DECLARE stmt NVARCHAR(500);

	--------------------------------------------------------------------------- 
	v_procedureName = 'TRYG_SH_CLAWBACK';
	v_eventType = 'SC-DK-001-001-SUMMARY';
	v_removeDate = TO_DATE('01/01/2200','mm/dd/yyyy');
	v_executionDate	= current_timestamp;
	v_sqlCount = 0;
	v_Count = 0;

	SELECT * INTO v_puRow FROM TCMP.CS_PROCESSINGUNIT cp WHERE cp.name = 'DK';
	SELECT * INTO v_periodRow FROM TCMP.CS_PERIOD cp WHERE cp.name = 'January 2023' AND cp.REMOVEDATE = v_removeDate;
	SELECT * INTO v_unitTypeRow FROM TCMP.CS_UNITTYPE cu WHERE cu.REMOVEDATE = v_removeDate AND cu.name = 'quantity';
    SELECT * INTO v_unitTypeDKKRow FROM TCMP.CS_UNITTYPE cu WHERE cu.REMOVEDATE = v_removeDate AND cu.name = 'DKK';
drop table ext.tryg_cancel_txns

drop table ext.tryg_cancel_txns
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
                                        AND et.removedate =:v_eot
                                        INNER JOIN cs_position pos ON 
                                        pos.name=sta_in.positionname
                                        AND pos.removedate =:v_eot
                                        INNER JOIN cs_title ti on
                                        pos.titleseq=ti.ruleelementownerseq
                                        -- and SUBSTR_AFTER (ti.name,'-') not in ('GA','PA','DB')
                                        and ti.removedate=:v_eot
                                        WHERE st_in.genericdate3 IS NOT NULL
                                        AND st_in.genericattribute1 = 'AFGA'
                                        AND sta_in.processingunitseq =:v_puRow.processingunitseq
                                        AND st_in.genericnumber1 > st_in.genericnumber2
                                        AND et.eventtypeid = :v_eventType
                                        AND st_in.compensationdate >=v_periodRow.startDate
                                        AND st_in.compensationdate < v_periodRow.endDate
                                        
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
                                        AND et.removedate =:v_eot
                                        INNER JOIN cs_position pos ON 
                                        pos.name=sta_in.positionname
                                        AND pos.removedate =:v_eot
                                        INNER JOIN cs_title ti on
                                        pos.titleseq=ti.ruleelementownerseq
                                        -- and SUBSTR_AFTER (ti.name,'-') not in ('GA','PA','DB')
                                        and ti.removedate=:v_eot
                                        WHERE st_in.genericdate3 IS NULL
                                        AND st_in.genericattribute1 <> 'AFGA'
                                        and st_in.genericattribute1 not in ('GENI', 'GESK')
                                        AND sta_in.processingunitseq =:v_puRow.processingunitseq
                                        AND st_in.genericnumber1 > st_in.genericnumber2
                                        AND et.eventtypeid = :v_eventType
                                        AND st_in.compensationdate >= v_periodRow.startDate
                                        AND st_in.compensationdate < v_periodRow.endDate
                                
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
                                        AND et.removedate =:v_eot
                                        INNER JOIN cs_position pos ON 
                                        pos.name=sta_in.positionname
                                        AND pos.removedate = :v_eot
                                        INNER JOIN cs_title ti on
                                        pos.titleseq=ti.ruleelementownerseq
                                        and ti.removedate=:v_eot
                                        WHERE st_in.genericdate3 IS NULL
                                        AND st_in.genericattribute1 in ('GENI','GESK')
                                        AND sta_in.processingunitseq =:v_puRow.processingunitseq
                                        -- AND st_in.genericnumber1 > st_in.genericnumber2
                                        AND et.eventtypeid =:v_eventType
                                        AND st_in.compensationdate >= v_periodRow.startDate
                                        AND st_in.compensationdate <  v_periodRow.endDate);
      end                          