--liquibase formatted sql

--changeset jcadby:KYN_TQ2COM_IPL_Trace1 splitStatements:false stripComments:false
--comment: Create table
create table EXT.KYN_TQ2COM_IPL_Trace (
	documentprocessseq bigint not null,
	generatedate timestamp not null,
	name varchar(200),
	batchname varchar(150),
	startdate timestamp not null,
	enddate timestamp not null,
	positionseq bigint not null,
	position nvarchar(127) not null,
	status varchar(50) not null,
	acceptdate timestamp,
	semiannual_periodseq bigint not null,
	semiannual_name varchar(50) not null,
	run_key bigint,
  year_run_key bigint,
	process_flag tinyint not null default 0,
  message varchar(4000)
);

--changeset jcadby:KYN_TQ2COM_IPL_Trace2 splitStatements:false stripComments:false
--comment: Comment table
comment on table EXT.KYN_TQ2COM_IPL_Trace is 'Traces IPL to snapshot data for sync process from TnQ to Commissions';