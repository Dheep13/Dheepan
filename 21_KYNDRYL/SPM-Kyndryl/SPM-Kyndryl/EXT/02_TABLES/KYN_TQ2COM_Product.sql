--liquibase formatted sql

--changeset jcadby:KYN_TQ2COM_Product1 splitStatements:false stripComments:false
--comment: Create table
create column table EXT.KYN_TQ2COM_Product(
	run_key bigint not null,
	semiannual_periodseq bigint,
	semiannual_name varchar(50),
	active_flag tinyint not null,
	active_start timestamp,
	active_end timestamp,
	territory varchar(255) not null,
	t_esd date,
	t_eed date,
	positionseq bigint not null,
	position nvarchar(127) not null,
	pos_esd date,
	pos_eed date,
	tprd_esd date,
	tprd_edd date,
	category varchar(127),
	classifierid nvarchar(127) not null,
	source nvarchar(127),
	process_flag tinyint not null default 0
);

--changeset jcadby:KYN_TQ2COM_Product2 splitStatements:false stripComments:false
--comment: Comment table
comment on table EXT.KYN_TQ2COM_PRODUCT is 'Staging area of Products for sync process from TnQ to Commissions';