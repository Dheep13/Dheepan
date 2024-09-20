
				SELECT distinct
					st.salestransactionseq AS cantxns_salestransactionseq,
					st.salesorderseq AS cantxns_salesorderseq,
					st.linenumber AS cantxns_linenumber,
					st.sublinenumber AS cantxns_sublinenumber,
					st.alternateordernumber AS cantxns_alternateordernumber,
					st.compensationdate AS cantxns_compdate,
					st.genericnumber1 AS cantxns_Old_premium,
					st.genericnumber2 AS cantxns_new_premium,
					st.genericdate1 AS cantxns_policy_sDate,
					st.genericdate2 AS cantxns_policy_eDate,
					st.genericdate3 AS cantxns_policy_cDate,
					sta.positionname AS cantxns_positionname,
					(
                    SELECT DISTINCT st_sub.genericnumber2
                    FROM (
                            SELECT ROW_NUMBER() OVER (
                                    PARTITION BY st_l1.alternateordernumber
                                    ORDER BY st_l1.compensationdate DESC
                                ) row_num,
                                st_l1.sublinenumber,
                                st_l1.alternateordernumber,
                                st_l1.compensationdate
                            FROM cs_salestransaction st_l1,
                                (
                                    SELECT DISTINCT st_in.alternateordernumber,
                                        st_in.compensationdate,
                                        sta_in.positionname
                                    FROM cs_salestransaction st_in
                                        INNER JOIN cs_transactionassignment sta_in
                                        ON sta_in.salestransactionseq = st_in.salestransactionseq
                                        AND sta_in.compensationdate = st_in.compensationdate
                                        INNER JOIN cs_eventtype et
                                        ON et.datatypeseq = st_in.eventtypeseq
                                        AND et.removedate = '2200-01-01'
                                        WHERE st_in.genericdate3 IS NOT NULL
                                        AND sta_in.processingunitseq = 38280596832649318
                                        AND st_in.genericnumber1 > st_in.genericnumber2
                                        AND et.eventtypeid ='SC-DK-001-001-SUMMARY'
                                        AND st_in.compensationdate >= '2022-09-01' 
                                        AND st_in.compensationdate <'2022-09-30'
                                        AND st_in.alternateordernumber=8804001913144
                                ) decr_txn,
                                cs_transactionassignment ta_l1,
                                cs_eventtype et_l1
                            where st_l1.compensationdate < decr_txn.compensationdate
                                and st_l1.alternateordernumber = decr_txn.alternateordernumber
                                and ta_l1.positionname = decr_txn.positionname
                                and ta_l1.salestransactionseq = st_l1.salestransactionseq
                                and st_l1.eventtypeseq = et_l1.datatypeseq
                                and et_l1.eventtypeid = 'SC-DK-001-001-SUMMARY'
                                and et_l1.removedate = '2200-01-01'
                                and st_l1.genericdate3 is null
                                -- and st_l1.alternateordernumber = st.alternateordernumber
                            group by st_l1.alternateordernumber,
                                st_l1.sublinenumber,
                                st_l1.compensationdate
                        ) max_sub
                        inner join cs_salestransaction st_sub 
                        on st_sub.compensationdate = max_sub.compensationdate
                        AND max_sub.alternateordernumber = st_sub.alternateordernumber
                        AND max_sub.sublinenumber = st_sub.sublinenumber
                        INNER JOIN cs_eventtype et_sub 
                        ON et_sub.datatypeseq = st_sub.eventtypeseq
                        AND et_sub.removedate = '2200-01-01'
                    	WHERE et_sub.eventtypeid = 'SC-DK-001-001-SUMMARY'
                		AND max_sub.row_num = 1
                )  lastest_premium
				FROM cs_salestransaction st
				INNER JOIN cs_transactionassignment sta ON
					sta.salestransactionseq = st.salestransactionseq
					AND sta.compensationdate = st.compensationdate
				INNER JOIN cs_eventtype et ON
					et.datatypeseq = st.eventtypeseq
					AND et.removedate ='2200-01-01'
				WHERE
					st.genericdate3 IS NOT NULL
                    AND sta.processingunitseq= 38280596832649318
					AND st.genericattribute1 = 'AFGA'
					AND st.genericnumber1 > st.genericnumber2  -- Old Premium Less than NEW Premium FOR NEW AND Increase txns
					AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
					AND st.compensationdate >= '2022-09-01'
					AND st.compensationdate <= '2022-09-30'
					AND st.alternateordernumber='8804001913144';
					
					
					
					SELECT st.salestransactionseq, st.alternateordernumber AS newtxns_alternateordernumber,
					sta.positionname AS newtxns_positionname,
					st.genericdate1 AS newtxns_policy_sDate,
					st.genericdate2 AS newtxns_policy_eDate,
					sum(cc.value) AS newtxns_crdvalue
				FROM
					cs_salestransaction st
				INNER JOIN cs_transactionassignment sta ON
					sta.salestransactionseq = st.salestransactionseq
					AND sta.compensationdate = st.compensationdate
				INNER JOIN cs_eventtype et ON
					et.datatypeseq = st.eventtypeseq
					AND et.removedate = '2200-01-01'
				LEFT JOIN cs_credit cc ON
					cc.genericattribute3 = st.alternateordernumber
					AND st.genericdate1 = cc.genericdate1
				LEFT JOIN cs_position pos ON	
					pos.name = sta.positionname
					AND pos.ruleelementownerseq = cc.positionseq
				WHERE
					-- st.genericdate3 IS NOT NULL
                     sta.processingunitseq= 38280596832649318
					AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
					AND pos.removedate = '2200-01-01'
					-- AND st.compensationdate between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)
					AND cc.compensationdate <= st.compensationdate
				    -- AND MONTHS_BETWEEN(st.genericdate3, ifnull(cc.genericdate1,st.genericdate3)) <=12
				    -- AND MONTHS_BETWEEN(st.genericdate3, ifnull(cc.genericdate1,st.genericdate3)) >= 0
					-- AND cc.compensationdate > add_months(st.genericdate3,-12)
					AND st.compensationdate between '2022-09-01' and '2022-09-30'
					And st.alternateordernumber=8804001913144
				    GROUP BY 
					st.alternateordernumber ,
					sta.positionname ,
					st.genericdate1 ,
					st.genericdate2,
					st.salestransactionseq
				
			
			