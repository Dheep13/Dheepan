--liquibase formatted sql

--changeset jcadby:KYN_TQ2COM_Account1 splitStatements:false stripComments:false
--comment: Create table
create column table ext.kyn_tq2com_account(
	run_key bigint not null,
	semiannual_periodseq bigint,
	semiannual_name varchar(50),
	active_flag tinyint not null default 0,
	active_start timestamp,
	active_end timestamp,
	territory varchar(255) not null,
	t_esd date,
	t_eed date,
	positionseq bigint not null,
	position nvarchar(127) not null,
	pos_esd date,
	pos_eed date,
	tacc_esd date,
	tacc_edd date,
  accountseq bigint not null,
	accountid nvarchar(255) not null,
  account_esd date,
  account_eed date,
  account_createdate timestamp,
	isaddedduetoparent smallint not null
);

--changeset jcadby:KYN_TQ2COM_Account2 splitStatements:false stripComments:false
--comment: Comment table
comment on table EXT.KYN_TQ2COM_ACCOUNT is 'Staging area of Accounts for sync process from TnQ to Commissions';