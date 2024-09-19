--liquibase formatted sql

--changeset jcadby:KYN_TxnAssign_Territory splitStatements:false stripComments:false
--comment: Create table
create column table ext.KYN_TxnAssign_Territory(
  processingunitseq bigint not null,
	territoryseq    bigint not null,
	territory_name  varchar(127) not null,
	categoryseq     bigint,
	category_name   varchar(127),
	classifierseq   bigint,
	classifierid    nvarchar(127)
);
