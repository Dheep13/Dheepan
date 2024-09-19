--liquibase formatted sql

--changeset jcadby:KYN_TQ2COM_Filter1 splitStatements:false stripComments:false
--comment: Create table
create column table EXT.KYN_TQ2COM_Filter(
	run_key bigint,
	filter_column varchar(255),
	filter_value nvarchar(255)
);

--changeset jcadby:KYN_TQ2COM_Filter2 splitStatements:false stripComments:false
--comment: Comment table
comment on table EXT.KYN_TQ2COM_FILTER is 'Filter conditions for sync process from TnQ to Commissions';