#!/bin/bash
PATH=/opt/someApp/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
# Environments file path
. /home/callidus/.bash_profile
. /apps/Callidus/aias/integrator/aias_utilitysecretfuns.sh
tenantid="aias"
tenantid_uc="AIAS"
custinst="PRD"


# Database Details
#"LND-AIAS-DEV" Connection string
dbconnstring="aiasprd"
dbusername="aiasadmin"
dbpwd="aiasadmin"
truecomp_username="SPCALS01"
truecomp_password="IYrh46Kv@2max3"
extconnstring="aiasextprd"
extusername="AIASEXT"
funpasswd e698bbe51073f48106311995a9953aa4EXT
extpwd=$outpasswd

# Informatica Details
#service="Integration_service_aias_prd"
service="IS_lndaiasprd"
domain="Domain_lndaiasprd"
#infa_foldername="AIAS"
infa_foldername="AIA"
username="Administrator"
password="tVu4mzOEDS"
infatgtdir="/apps/Informatica/PowerCenter/server/infa_shared/TgtFiles"
infasrcdir="/apps/Informatica/PowerCenter/server/infa_shared/SrcFiles"
infabadfiledir="/apps/Informatica/PowerCenter/server/infa_shared/BadFiles"
infacachedir="/apps/Informatica/PowerCenter/server/infa_shared/Cache"
infasessiondir="/apps/Informatica/PowerCenter/server/infa_shared/SessLogs"
infaworkflowdir="/apps/Informatica/PowerCenter/server/infa_shared/WorkflowLogs"
infatgtoutbound="/apps/Informatica/PowerCenter/server/infa_shared/TgtFiles/outbound"
infatgtinbound="/apps/Informatica/PowerCenter/server/infa_shared/TgtFiles/inbound"



# Data Files Folders
basedir=/apps/Callidus
tntdir=$basedir/$tenantid
workdir=$tntdir/workarea
tntscriptdir=$tntdir/integrator
logfolder=$tntdir/logs
datafile=$tntdir/datafiles
tempdir=$datafile/temp
archivefolder=$datafile/archive
badfilesfolder=$datafile/badfiles
inboundfolder=$datafile/inbound
outboundfolder=$datafile/toapp
#outboundfolder=$datafile/aiastmp
lndoutboundfolder=$datafile/outbound
BulkAdjustments=$tntscriptdir/bulk_adjustments
outboundtempaias=$datafile/aiastmp
dataextract=$datafile/dataextract

#ODI Script Directories
odiscriptsdir="/apps/Callidus/ondemand/integrator"

#Tenant Script Directories
tntsetenvname="${tenantid}_setenv_variable.sh"
tntscriptsdir="/apps/Callidus/$tenantid/integrator"
datasummary="${tenantid}_datasummary.sh"
executeworkflow="${tenantid}_executeworkflow.sh"
inboundfilerun_start="${tenantid}_inboundfilerun_start.sh"   
inboundfilerun_end="${tenantid}_inboundfilerun_end.sh"
datafilesummary_start="${tenantid}_datafilesummary_start.sh"
datafilesummary_end="${tenantid}_datafilesummary_end.sh"
concatenate="${tenantid}_concatenate_all.sh"
statusmail="${tenantid}_statusmail.sh"
typemap="${tenantid}_typemap.conf"
codemap="${tenantid}_codemap.conf"
email="${tenantid}_email.conf"
MAemail_list="MAaias_email.conf"
bulkupload="${tenantid}_bulkupload_status.sh"
privatekeyid=0BA6E122
pubkey=DF7F91D0
aia_pubkey=3C4E99E0

# File Processing
timestamp=`date +%Y%m%d_%H%M%S`
starttime=`date +%m/%d/%Y-%H:%M:%S`
yyyymm=`date +%Y%m`
YMD=`date +%Y%m%d`

# pid=`ps|grep "bash"|awk '{print$1}'`
# instancename=$tenantid_uc"_"$timestamp"_"$pid

#INFA_HOME="/apps/Informatica/PowerCenter"
#PATH="/apps/Informatica/PowerCenter/server/bin"

#export INFA_HOME
#export PATH

#export dbconnstring
#export dbusername
#export dbpwd
#export truecomp_username
#export truecomp_password

