-- drop table ext.tryg_clawback_credits;
create column table ext.tryg_clawback_credits as (
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
                            genicantxns_alternateordernumber ,
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
                                st_prev.genericnumber1 as prev_latestpremium,
                                st_prev.salestransactionseq as prev_salestransactionseq,
                                st_prev.genericdate1 as prev_policy_sDate,
                                st_prev.genericdate2 as prev_policy_eDate,
                                geni_txn.cantxns_alternateordernumber as cantxns_alternateordernumber ,
                                geni_txn.cantxns_salesorderseq as cantxns_salesorderseq,
                                -- geni_txn.geni_positionname as geni_positionname,
                                geni_txn.cantxns_salestransactionseq as cantxns_salestransactionseq,
                               
                                geni_txn.cantxns_eventtypeseq as cantxns_eventtypeseq,
                                geni_txn.cantxns_linenumber  as cantxns_linenumber,
                                geni_txn.cantxns_sublinenumber  as cantxns_sublinenumber,
                                geni_txn.cantxns_compdate  as cantxns_compdate,
                                geni_txn.cantxns_Old_premium  as cantxns_Old_premium,
                                geni_txn.cantxns_new_premium  as cantxns_new_premium,
                                geni_txn.cantxns_policy_sDate as cantxns_policy_sDate,
                                geni_txn.cantxns_policy_eDate as cantxns_policy_eDate,
                                geni_txn.cantxns_policy_cDate as cantxns_policy_cDate,
                                geni_txn.cantxns_positionname as cantxns_positionname,
                                geni_txn.cantxns_positionseq as cantxns_positionseq

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
                               where pos.removedate='2200-01-01'
                               and final.row_num=1); 
