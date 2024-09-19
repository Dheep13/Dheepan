--liquibase formatted sql

--changeset jcadby:KYN_TQ2COM_Report_Run1 splitStatements:false stripComments:false
--comment: Create table
create column table EXT.KYN_TQ2COM_Report_Run(
	run_key bigint not null,
	periodseq bigint not null,
	period_name varchar(50) not null,	
	start_time timestamp,
	stop_time timestamp,
	process_flag tinyint default 0 not null,
  message varchar(4000)
);

--changeset jcadby:KYN_TQ2COM_Report_Run2 splitStatements:false stripComments:false
--comment: Comment table
comment on table EXT.KYN_TQ2COM_Report_Run is 'Table used for automatic report refresh';