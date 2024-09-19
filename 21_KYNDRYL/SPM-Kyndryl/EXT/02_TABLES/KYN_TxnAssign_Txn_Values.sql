--liquibase formatted sql

--changeset jcadby:KYN_TxnAssign_Txn_Values splitStatements:false stripComments:false
--comment: Create table
create column table EXT.KYN_TxnAssign_Txn_Values(
	processingunitseq bigint not null,
	periodseq bigint not null,
	eventtypeseq bigint not null,
  eventtypeid varchar(40) not null,
	productid varchar(127) not null,
	genericattribute3 varchar(255) not null
);
