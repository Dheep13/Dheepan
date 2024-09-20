call ext.kyn_lib_tq2com:create_lock('KYN_LIB_TQ2COM', ?);

-- prestage_quota
rename table EXT.KYN_TQ2COM_Prestage_Quota to EXT.KYN_TQ2COM_Prestage_Quota_old;

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

comment on table EXT.KYN_TQ2COM_PRESTAGE_QUOTA is 'Quota Prestage table for sync process from TnQ to Commissions';

alter table EXT.KYN_TQ2COM_Prestage_Quota_old add (territoryprogram_periodseq bigint);

update EXT.KYN_TQ2COM_Prestage_Quota_old x set territoryprogram_periodseq= (select y.territoryprogram_periodseq from  EXT.KYN_TQ2COM_sync y 
where x.run_key = y.run_key);

call ext.kyn_lib_utils:copy_data('ext','KYN_TQ2COM_Prestage_Quota_old','KYN_TQ2COM_Prestage_Quota');

drop table EXT.KYN_TQ2COM_Prestage_Quota_old;


-- IPL trace
rename table EXT.KYN_TQ2COM_IPL_Trace to EXT.KYN_TQ2COM_IPL_Trace_old;

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

comment on table EXT.KYN_TQ2COM_IPL_Trace is 'Traces IPL to snapshot data for sync process from TnQ to Commissions';

call ext.kyn_lib_utils:copy_data('ext','KYN_TQ2COM_IPL_Trace_old','KYN_TQ2COM_IPL_Trace');


-- TQ_Quota

rename table EXT.KYN_TQ2COM_TQ_Quota to EXT.KYN_TQ2COM_TQ_Quota_old;

create column table EXT.KYN_TQ2COM_TQ_Quota(
	run_key bigint not null,
  territoryprogram_periodseq bigint not null,
  semiannual_periodseq bigint not null,
  territoryseq bigint not null,
	territory_name varchar(255) not null,
	territory_esd date,
	territory_eed date,
	targettypeid varchar(40) not null,
	quota_esd date,
	quota_eed date,
	quotavalue decimal(25, 10),
	unittype varchar(40) not null,
	finalquotavalue decimal(25, 10),
	quota_casestatus varchar(127),
	tpos_esd date,
	tpos_eed date,
	split decimal(5, 2),
	positionseq bigint not null,
	position nvarchar(127) not null,
	payeeseq bigint not null,
	payeeid varchar(40) not null,
  titleseq bigint,
  title varchar(127),
  planseq bigint,
  plan varchar(127),
  element varchar(255),
  country_code varchar(255),
  period_cycle tinyint,
  lt_min_quota decimal(25, 10),
  min_quota decimal(25, 10),
  subordinate_count integer,
  revenue_percent decimal(25,10),
  final_quota decimal(25,10),
  --
  year_finalquotavalue decimal(25,10),
  year_lt_min_quota decimal(25, 10),
  year_min_quota decimal(25, 10),
  year_subordinate_count integer,
  year_revenue_percent decimal(25,10),
  year_final_quota decimal(25,10)
);

comment on table EXT.KYN_TQ2COM_TQ_QUOTA is 'Working table to store TnQ quotas';

call ext.kyn_lib_utils:copy_data('ext','KYN_TQ2COM_TQ_QUOTA_old','KYN_TQ2COM_TQ_QUOTA');


-- report run
create column table EXT.KYN_TQ2COM_Report_Run(
	run_key bigint not null,
	periodseq bigint not null,
	period_name varchar(50) not null,	
	start_time timestamp,
	stop_time timestamp,
	process_flag tinyint default 0 not null,
  message varchar(4000)
);

comment on table EXT.KYN_TQ2COM_Report_Run is 'Table used for automatic report refresh';

-- view
--liquibase formatted sql

--changeset jcadby:KYN_V_TQ2COM_WF_TP splitStatements:false stripComments:false
--comment: Create view
create or replace view EXT.KYN_V_TQ2COM_WF_TP as
select 
per.periodseq,
per.startdate as period_startdate,
per.name      as period_name,
--
tp.territoryprogramseq, 
tp.name               as territoryprogram_name,
tp.casestatus         as territoryprogram_casestatus,
tp.effectivestartdate as territoryprogram_esd, 
tp.effectiveenddate   as territoryprogram_eed,
--
tp2.territoryprogramseq as linked_territoryprogramseq,
tp2.name                as linked_territoryprogram_name,
tp2.casestatus          as linked_territoryprogram_casestatus,
tp2.effectivestartdate  as linked_territoryprogram_esd, 
tp2.effectiveenddate    as linked_territoryprogram_eed
from csq_territoryprogram tp
join cs_period per on tp.periodseq = per.periodseq and per.removedate = '2200-01-01'
left outer join csq_territoryprogram tp2 on 
  tp.periodseq = tp2.periodseq 
  and tp2.removedate = '2200-01-01' 
  and tp2.name != tp.name
  and tp2.name like 'FY__H2$_Seller$_%' escape '$'
  and REPLACE_REGEXPR('(FY[0-9][0-9])(H[1-2])(.*)' FLAG 'i' IN tp2.name WITH '\1H$\3' OCCURRENCE 1) = 
  REPLACE_REGEXPR('(FY[0-9][0-9])(H[1-2])(.*)' FLAG 'i' IN tp.name WITH '\1H$\3' OCCURRENCE 1)
where tp.removedate= '2200-01-01' and tp.name like 'FY__H_$_Seller$_%' escape '$'
order by per.startdate, tp.name;

-- release all functions

