merge into cs_salestransaction st 
using
     (select 
		cust_id,
		location_id,
		invoice_date,
		estinguisher_count, 
		elight_count
	from
		ext.ctas_sh_prdct_count) src,
        	(select  txn.salestransactionseq
		tadd.cust_id as cust_id,
		txn.genericattribute19 as location_id,
		txn.compensationdate as invoice_date
	from
    	tcmp.cs_salestransaction txn, 
		tcmp.cs_transactionassignment txna,
		tcmp.cs_transactionaddress tadd,
		tcmp.cs_addresstype addt
	where 	txn.genericboolean1 = 1
		and tx.genericattribute22 = 'Invoiced'
		and txn.salestransactionseq = txna.salestransactionseq
		and tadd.addresstypeseq = addt.addresstypeseq
		and addt.addresstypeid = 'SHIPTO'
		and tx.salestransactionseq = tadd.salestransactionseq
		and txn.eventtypeseq = :v_datatypeseq
		and txn.compensationdate >= v_periodstartdate
		and txn.compensationdate < v_periodenddate
		and upper (tgt.productid) 
			in (select classifierid
				from ext.ctas_product_classifiers
				where category_name in ('EXTINGUISHER','ELIGHT')
	)) tgt
    )
	on
		(st.salestransactionseq = src.cust_id
		-- and tgt.location_id = src.location_id
		-- and tgt.invoice_date = src.invoice_date
        )
when matched
then
update set
    tgt.genericattribute26 =
            case
            when estinguisher_count >= :v_FV_Product_Count then 'TRUE'
            when elight_count >= :v_FV_Product_Count then 'FALSE'
            when elight_count >= 1 and estinguisher_count + elight_count >= :v_FV_Product_Count then 'TRUE'
            end;