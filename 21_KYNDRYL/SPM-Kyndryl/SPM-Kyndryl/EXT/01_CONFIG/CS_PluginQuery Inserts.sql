--liquibase formatted sql

--changeset jcadby:CS_PluginQuery_Inserts1 splitStatements:false stripComments:false
--comment: IPL_Quota delete
delete from cs_pluginquery where name = 'IPL_Quota';

--changeset jcadby:CS_PluginQuery_Inserts2 splitStatements:false stripComments:false
--comment: IPL_Quota insert
insert into cs_pluginquery (
select 
tenantid, 
'IPL_Quota' as name, 
'select EXT.KYN_Fnc_TQ2Com_IPL_Quota(positionSeq, periodSeq, quotaName, periodType) as value, unittypeseq from (select $positionSeq as positionSeq, $periodSeq as periodSeq, $1 as quotaName, $2 as periodType, unittypeseq from cs_unittype where name=''USD'')' as query,
'Query to get quota values in IPL' as description
from cs_tenant
);

--changeset jcadby:CS_PluginQuery_Inserts3 splitStatements:false stripComments:false
--comment: IPL_Account_Summary delete
delete from cs_pluginquery where name = 'IPL_Account_Summary';

--changeset jcadby:CS_PluginQuery_Inserts4 splitStatements:false stripComments:false
--comment: IPL_Account_Summary insert
insert into cs_pluginquery (
select 
tenantid, 
'IPL_Account_Summary' as name, 
'select EXT.KYN_Fnc_TQ2Com_IPL_Account_Summary(positionSeq, periodSeq, periodType, to_number(startRow), to_number(endRow)) from (select $positionSeq as positionSeq, $periodSeq as periodSeq, $1 as periodType, $3 as startRow, $4 as endRow from dummy)' as query,
'Query to get account summary value in IPL' as description
from cs_tenant
);


--changeset jcadby:CS_PluginQuery_Inserts5 splitStatements:false stripComments:false
--comment: IPL_Account delete
delete from cs_pluginquery where name = 'IPL_Account';

--changeset jcadby:CS_PluginQuery_Inserts6 splitStatements:false stripComments:false
--comment: IPL_Account insert
insert into cs_pluginquery (
select 
tenantid, 
'IPL_Account' as name, 
'select EXT.KYN_Fnc_TQ2Com_IPL_Account(positionSeq, periodSeq, periodType, to_number(startRow), to_number(endRow)) from (select $positionSeq as positionSeq, $periodSeq as periodSeq, $1 as periodType, $3 as startRow, $4 as endRow from dummy)' as query,
'Query to get accounts in IPL' as description
from cs_tenant
);

--changeset jcadby:CS_PluginQuery_Inserts7 splitStatements:false stripComments:false
--comment: commit
commit;