--liquibase formatted sql

--changeset jcadby:KYN_TQ2COM_TQ_Quota1 splitStatements:false stripComments:false
--comment: rename table
rename table EXT.KYN_TQ2COM_TQ_Quota to EXT.KYN_TQ2COM_TQ_Quota_old;

--changeset jcadby:KYN_TQ2COM_TQ_Quota2 splitStatements:false stripComments:false
--comment: Create table
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
  title_gn3 decimal(25,10),
  planseq bigint,
  plan varchar(127),
  element varchar(255),
  country_code varchar(255),
  period_cycle tinyint,
  --
  lt_min_quota decimal(25, 10),
  lt_min_quota_override decimal(25, 10),
  subordinate_count integer,
  min_quota decimal(25, 10),
  revenue_percent decimal(25,10),
  final_quota decimal(25,10),
  --
  year_finalquotavalue decimal(25,10),
  year_lt_min_quota decimal(25, 10),
  year_lt_min_quota_override decimal(25, 10),
  year_subordinate_count integer,
  year_min_quota decimal(25, 10),
  year_revenue_percent decimal(25,10),
  year_final_quota decimal(25,10)
);

--changeset jcadby:KYN_TQ2COM_TQ_Quota3 splitStatements:false stripComments:false
--comment: Comment table
comment on table EXT.KYN_TQ2COM_TQ_Quota is 'Working table to store TnQ quotas';

--changeset jcadby:KYN_TQ2COM_TQ_Quota4 splitStatements:false stripComments:false
--comment: Copy data
call ext.kyn_lib_utils:copy_data('ext', 'KYN_TQ2COM_TQ_QUOTA_old','KYN_TQ2COM_TQ_QUOTA');

--changeset jcadby:KYN_TQ2COM_TQ_Quota5 splitStatements:false stripComments:false
--comment: drop old
drop table EXT.KYN_TQ2COM_TQ_Quota_old;