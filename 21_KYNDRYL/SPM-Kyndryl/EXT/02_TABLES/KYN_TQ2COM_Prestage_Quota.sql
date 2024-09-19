--liquibase formatted sql

--changeset jcadby:KYN_TQ2COM_Prestage_Quota1 splitStatements:false stripComments:false
--comment: Create table
create column table EXT.KYN_TQ2COM_Prestage_Quota(
	run_key bigint not null,
  territoryprogram_periodseq bigint not null,
	semiannual_periodseq bigint not null,
	semiannual_name varchar(50),
	active_flag tinyint not null default 0,
	active_start timestamp,
	active_end timestamp,
	effectivestartdate timestamp not null,
	effectiveenddate timestamp not null,
	quotaname varchar(42) not null,
	value decimal(28, 10),
	unittypeforvalue varchar(40) not null,
	periodtypename varchar(50) not null,
	businessunitmap varchar(1),
	positionseq bigint not null,
	positionname nvarchar(127) not null,
	batchname varchar(90)
);

--changeset jcadby:KYN_TQ2COM_Prestage_Quota2 splitStatements:false stripComments:false
--comment: Comment table
comment on table EXT.KYN_TQ2COM_PRESTAGE_QUOTA is 'Quota Prestage table for sync process from TnQ to Commissions';