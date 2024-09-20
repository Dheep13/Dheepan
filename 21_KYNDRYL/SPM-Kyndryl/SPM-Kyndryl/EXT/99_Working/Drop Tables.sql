create table ext.KYN_TQ2COM_Sync_bkp as (select * from ext.KYN_TQ2COM_Sync);

drop table ext.KYN_Config;
drop table ext.KYN_Debug;
drop table ext.KYN_Lock;
drop table ext.KYN_TQ2COM_Account;
drop table ext.KYN_TQ2COM_Filter;
drop table ext.KYN_TQ2COM_IPL_Trace;
drop table ext.KYN_TQ2COM_Prestage_Quota;
drop table ext.KYN_TQ2COM_Product;
drop table ext.KYN_TQ2COM_Sync;
drop table ext.KYN_TQ2COM_TQ_Quota;