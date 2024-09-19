#!/bin/bash
#<<aias_utilityfunctions.sh>>
#-------------------------------------------------------------
# Date               Author                  Version
#-------------------------------------------------------------
# 12/09/2015        Callidus        		 1.0	
#
# Description : 
# Invoked by any script in the landing pad
#
# Command line: aias_utilityfunctions.sh <arguments as required by definition>
#################################################################################################################

PATH=/opt/someApp/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
# Import Environment Variables.
. /home/callidus/.bash_profile
. /apps/Callidus/aias/integrator/aias_setenv_variable.sh


#######################################################################################################
# Function : Count the No.of Data files received in Inbound folder for corresponding filetype supplied
#

DatafileCount () {

filetype="$1"
cd $inboundfolder

datafilename=`find $inboundfolder -name "*$filetype*" ! -name "*.aud" ! -name "*.GZIP" -type f -exec basename \{} \;| sort -n |head -1|tail -1 `

case "$datafilename" in
*.txt.gz)
		echo "[FileCount ] Data file is GZipped file." 
		dcount=`ls -ltr $inboundfolder/*$filetype*.txt.gz | wc -l | awk '{print $1}' |tail -1 |head -1`;;
*.txt)  
		echo "[FileCount ] Data file is Text file." 
		dcount=`ls -ltr $inboundfolder/*$filetype*.txt| wc -l | awk '{print $1}' |tail -1|head -1`;;
esac

return $dcount
}

#######################################################################################################
# Function : Check Outbound extract file size and move non-empty files to AppServer.
#
MoveFilestoAppServer ()
{
tgtfolder=$1
ob_filename=$2
filereccount=0

cd $tgtfolder
chmod 777 $tgtfolder/$ob_filename

filereccount=`wc -lwc $tgtfolder/$ob_filename | awk '{print$1}' |tail -1|head -1`
echo "[MoveFiles] Number of records in $ob_filename : $filereccount"

if [ $tgtfolder = $infatgtinbound ]; then
     Appfolder=$outboundfolder
     targetsystem="APPSERVER"
elif [ $tgtfolder = $infatgtoutbound ]; then 
     Appfolder=$lndoutboundfolder
     targetsystem="LNDDROPBOX"
fi

if [ $filereccount -ne 0 ]; then
     gzoutputFile=$ob_filename".gz"
	 echo "[MoveFiles] Zipping File : $ob_filename"
	 gzip $ob_filename
		if [ $targetsystem = "LNDDROPBOX" ]; then
			 auditFile=$gzoutputFile".aud"		
			 echo "[MoveFiles] Generating Audit file : $auditFile "
			 cksum $gzoutputFile >> $tgtfolder/$auditFile
			 cp $tgtfolder/$auditFile $Appfolder
			 cpaud=$?
				if [ $cpaud != 0 ] ; then
						echo "[MoveFiles] Error copying Audit file."
						echo "wfret=$cpaud" > $tempdir/wfreturncode.txt
						rm -f $tgtfolder/$ob_filename*
						exit 1
				fi
			 cp $tgtfolder/$auditFile $archivefolder
		fi
	 echo "[MoveFiles] Moving all Zipped files to AppServer folder and Archive folder."
	 cp $tgtfolder/$gzoutputFile $Appfolder
	 cp $tgtfolder/$gzoutputFile $archivefolder
	 #cp $tgtfolder/$auditFile $archivefolder
 else
	 echo "[MoveFiles] $ob_filename file is empty. Not transferring empty file"
  fi

echo "[MoveFiles] Removing All files from $tgtfolder folder."
rm -f $tgtfolder/$ob_filename*
}


##################################################################################################
# Function : set the variables by reading configure files and export them.
#
ReadParameters()
{
filetype=$1

chmod 777 $tntscriptsdir/$typemap
echo "[Autoloader] Reading Typemap data for $filetype"
executescript=`cat $tntscriptsdir/$typemap|grep "^$filetype|"|cut -d "|" -f2`
inboundwfname=`cat $tntscriptsdir/$typemap|grep "^$filetype|"|cut -d "|" -f3`
outboundwfname=`cat $tntscriptsdir/$typemap|grep "^$filetype|"|cut -d "|" -f4`
stagetablename=`cat $tntscriptsdir/$typemap|grep "^$filetype|"|cut -d "|" -f5`
wftype=`cat $tntscriptsdir/$typemap|grep "^$filetype|"|cut -d "|" -f6`
wftype1=`cat $tntscriptsdir/$typemap|grep "^$filetype|"|cut -d "|" -f7`
Dependency=`cat $tntscriptsdir/$typemap|grep "^$filetype|"|cut -d "|" -f8`
flag=`cat $tntscriptsdir/$typemap|grep "^$filetype|"|cut -d "|" -f9`
NumPartitions=`cat $tntscriptsdir/$typemap|grep "^$filetype|"|cut -d "|" -f10`
srcfiles=`cat $tntscriptsdir/$typemap|grep "^$filetype|"|cut -d "|" -f11`
paramfl=`cat $tntscriptsdir/$typemap|grep "^$filetype|"|cut -d "|" -f12`
Sequencejobs=`cat $tntscriptsdir/$typemap|grep "^$filetype|"|cut -d "|" -f13`
outputfils=`cat $tntscriptsdir/$typemap|grep "^$filetype|"|cut -d "|" -f14`


echo "[ReadParameters] Received filetype      : $filetype" 
echo "[ReadParameters] Script to be Executed  : $executescript"
echo "[ReadParameters] Inbound Workflow name  : $inboundwfname"
echo "[ReadParameters] Outbound Workflow name : $outboundwfname"
echo "[ReadParameters] Stageable name         : $stagetablename"
echo "[ReadParameters] Parameters to SP       : $wftype"
echo "[ReadParameters] Pipeline Stages        : $wftype1"
echo "[ReadParameters] File Dependency        : $Dependency"
echo "[ReadParameters] Concatenation flag(Y/N): $flag"
echo "[ReadParameters] File Partitions(16/5/0): $NumPartitions"
echo "[ReadParameters] Source files           : $srcfiles"
echo "[ReadParameters] Parameter file         : $paramfl"
echo "[ReadParameters] Sequence of jobs       : $Sequencejobs"
echo "[ReadParameters] Output files           : $outputfils"
echo "[ReadParameters] "

#Export all the above variables so that they are accessible in other scripts.
export filetype
export executescript
export inboundwfname
export outboundwfname
export stagetablename
export wftype
export wftype1
export Dependency
export flag
export NumPartitions
export srcfiles
export paramfl
export Sequencejobs
export outputfils
}

###################################################################################################
# Function : Execute Workflow, Return Success/Error code (and also update datafilesummary table)
#
ExecuteWorkflow()
{
foldername=$1
workflowname=$2
filehandler=$3
filename=$4
stagetablename=$5
filereccount=$6
runparamfile="$parameterfile"

pid=`ps|grep "bash"|awk '{print$1}'|tail -1|head -1`
case "$filename" in
*CMTX*|*PMTX*)	
instancename="INST_"$filename;;
*)
instancename=$tenantid"_"$timestamp"_"$pid;;
esac

echo "[ExecuteWorkflow] Workflow [$workflowname] execution started at $timestamp."
#case "$filetype" in
#OGPT|OGPR|CMTX|PMTX|RTTX|EXCH|APD|PAQPB|OGPO|APD|MA|RPILP|RPILR)
	pmcmd startworkflow -sv $service -d $domain -u $username -p $password -f $foldername -paramfile $runparamfile -wait -rin $instancename $workflowname
#;;
#*)
#	pmcmd startworkflow -sv $service -d $domain -u $username -p $password -f $foldername -wait -rin $instancename $workflowname;;
#esac

v_pmcmdreturncode=$?

 if [ $v_pmcmdreturncode -ne 0 ]; then
      echo "[ExecuteWorkflow] $v_pmcmdreturncode - Error in [$workflowname] Load, check Workflow Log."
	  sh $tntscriptsdir/$inboundfilerun_end $filehandler $filename $v_pmcmdreturncode
      echo "wfret=$v_pmcmdreturncode" > $tempdir/wfreturncode.txt
	  sh $tntscriptsdir/$datafilesummary_end $filehandler $filename $stagetablename $filename $v_pmcmdreturncode $filereccount
	  return $v_pmcmdreturncode
 else
      echo "wfret=0" > $tempdir/wfreturncode.txt
	  echo "[ExecuteWorkflow]"
	  echo "[ExecuteWorkflow] Workflow $workflowname with run instance name [$instancename] Completed with return code [$v_pmcmdreturncode]."
	  echo "[ExecuteWorkflow]"
	  sh $tntscriptsdir/$inboundfilerun_end $filehandler $filename $v_pmcmdreturncode
	  #commented this status check 20180402
	  #ibrun=$?
	  sh $tntscriptsdir/$datafilesummary_end $filehandler $filename $stagetablename $filename $v_pmcmdreturncode $filereccount
	  #commented this status check 20180402
	  #sumrum=$?
      	#  if [ "$ibrun" = 0 -a "$sumrum" = 0 ] ; then
	     #    result=0
	     # else
          #   result=1
	      #fi
 return $v_pmcmdreturncode
 fi
}

##################################################################################################
# Function : Count and Return the number of records in the input file
#
CountofRecords ()
{
	filename=$1
	filerecords=0
	filesize=0
	case "$filename" in
	*POLMOVE_SG*)
	filetype=POLMOVESG;;
	*POLMOVE_BN)
	filetype=POLMOVEBN;;
	*)
	filetype=`echo $filename | cut -d "_" -f2`
	esac
	#filetype=`echo $filename | cut -d "_" -f2`
#add TFTX by sammi
#add CBPOLPYR,CBSPCMP,CBCFPOL by Simon
#Add third party(TPTX) by Sammi 20220421
case "$filetype" in
OGPT|OGPR|CMTX|PMTX|RTTX|EXCH|APD|PAQPB|OGPO|APD|MA|RPILP|RPILR|TFTX|CBPOLPYR|CBSPCMP|CBCFPOL|TPTX)
	dirpath=$infasrcdir;;
*)
	if [ "$srcfiles" = "NA" ]; then
		dirpath=$inboundfolder
	else
		dirpath=$infasrcdir
	fi;;
esac	
	
	cd $dirpath
	datafilename=`find $dirpath -name "*$filename*" ! -name "*.aud" ! -name "*.GZIP" -type f -exec basename \{} \;| sort -n |head -1|tail -1 `
	chmod 777 $dirpath/$datafilename

		
if [ "$datafilename" != "" ]; then
	 filerecords=`cat $dirpath/$datafilename | wc -lwc |awk '{print $1}'|tail -1|head -1`
	 filesize=`cat $dirpath/$datafilename | wc -lwc  |awk '{print $3}'|tail -1|head -1`
	 
	 #File size in KB and MB
	 #It has to be enabled to get file size in KB
	 #filesizeKB=`echo "scale=2; $filesize/1024" |bc -l`
	 #filesizeKB="$filesizeKB"" K"
	 
	 if [ "$filetype" = "MA" ]; then
	 filerecords=`expr $filerecords \- 2`
	 fi
	 filesizeKB=`echo "scale=2; $filesize/1024/1024" |bc -l`
	 filesizeKB="$filesizeKB"" M"
fi	 
	echo "[CountofRecords] File Records   : $filerecords" 
	echo "[CountofRecords] File Size      : $filesizeKB"
	
	export filerecords
	export filesizeKB
}

################################################################################################
# Function : Log the name of the inbound/outbound file being processed.
#
LoggingProcess ()
{
filehandler=$1
filename=$2
stagetablename=$3
batchname=$4

sh $tntscriptsdir/$inboundfilerun_start $filehandler $filename
processretcode=$?
if [ $processretcode != 0 ]; then
	 echo "[LoggingProcess] $processretcode - Error logging to ${tenantid}_InboundFileRun"
	 echo "wfret=1" > $tempdir/wfreturncode.txt
fi

sh $tntscriptsdir/$datafilesummary_start $filehandler $filename $stagetablename $batchname
summaryretcode=$?
 if [ $summaryretcode != 0 ]; then
     echo "[LoggingProcess] $summaryretcode - Error logging to ${tenantid}_DataFileSummary"
	 echo "wfret=1" > $tempdir/wfreturncode.txt
 fi
 
 if [ "$processretcode" = 0 -a "$summaryretcode" = 0 ] ; then
	 lgresult=0
 else
	 lgresult=1
 fi
 
 return $lgresult
}

############################################################################################################
# Function : Check for any Dependencies based on filetype 
#
DependencyChecker()
{
filename=$1
case "$filename" in
*POLMOVE_SG*)
filetype=POLMOVESG;;
*POLMOVE_BN)
filetype=POLMOVEBN;;
*)
filetype=`echo $filename | cut -d "_" -f2`
esac
#filetype=`echo $filename | cut -d "_" -f2`
#basefilename=`echo $filename|awk -F\. '{print $1}' `
cat /dev/null > $tempdir/dependency_list.txt


if [ "$Dependency" != "none" ]; then
	 # echo $Dependency | cut -d "," -f1 >> $tempdir/dependency_list.txt
	 # echo $Dependency | cut -d "," -f2 >> $tempdir/dependency_list.txt
	 echo $Dependency | tr , "\n" >> $tempdir/dependency_list.txt
	    while read type
	    do
			datafilename1=`find $archivefolder -name "*$type*$filedate*" -type f -exec basename \{} \;| sort -n |head -1|tail -1 `
			if [ "$datafilename1" != "" ]; then
				 echo "[DependencyChecker] $filetype : $datafilename1 dependent file processed Successfully. "
				 echo "[DependencyChecker]" $datafilename1
				 echo "dpcode=0" > $tempdir/dependency_code.txt
			else
				 echo "[DependencyChecker] $filetype : $type dependent file has to be processed "
				 echo "dpcode=1" > $tempdir/dependency_code.txt
				 break
		    fi
        done < $tempdir/dependency_list.txt
else 
	 echo "dpcode=2" > $tempdir/dependency_code.txt
fi
}


#########################################################################################################
# Function : Check Outbound extract file and write error count.
#
ErrorCount ()
{

filename=$1
#tgtfilereccount1=0
#tgtfilereccount2=0
errfilereccount=0
#tgtfilename1=""
#tgtfilename2=""
errfilename=""

#### Archving Target File,Error count

l_insbatchname="'""$filename""'"

#tgtfilereccount1=`sqlplus -s $dbusername/$dbpwd <<! 
#set heading off feedback off verify off 
#set serveroutput on size 100000
#declare
#l_batchname   varchar2(255);
#l_errorcount number;
#begin
#l_batchname:=$l_insbatchname;
#SELECT nvl(targetrecordcount_1,0) into l_errorcount  from AIAS_Datasummary where sourcefilename=$l_insbatchname;
#dbms_output.put_line(l_errorcount);
#end;
#/
#!` 

#tgtfilereccount2=`sqlplus -s $dbusername/$dbpwd <<! 
#set heading off feedback off verify off 
#set serveroutput on size 100000
#declare

#l_batchname   varchar2(255);
#l_errorcount number;
#begin
#l_batchname:=$l_insbatchname;
#SELECT nvl(targetrecordcount_2,0) into l_errorcount  from AIAS_Datasummary where sourcefilename=$l_insbatchname;
#dbms_output.put_line(l_errorcount);
#end;
#/
#!` 

errfilereccount=`sqlplus -s $dbusername/$dbpwd <<! 
set heading off feedback off verify off 
set serveroutput on size 100000
declare
l_batchname   varchar2(255);
l_errorcount number;
begin
l_batchname:=$l_insbatchname;
SELECT nvl(ERRORCOUNT,0) into l_errorcount  from AIAS_Datasummary where sourcefilename=$l_insbatchname;
dbms_output.put_line(l_errorcount);
exception
when no_data_found then
dbms_output.put_line ('no error records found');
end;
/
!` 

errfilename=`sqlplus -s $dbusername/$dbpwd <<! 
set heading off feedback off verify off 
set serveroutput on size 100000
declare

l_batchname   varchar2(255);
l_errorfilename varchar2(255);
begin
l_batchname:=$l_insbatchname;
SELECT nvl(ERRORFILENAME,'NULL') into l_errorfilename  from AIAS_Datasummary where sourcefilename=$l_insbatchname;
dbms_output.put_line(l_errorfilename);
exception
when no_data_found then
dbms_output.put_line ('no error');
end;
/
!` 


#tgtfilename1=`sqlplus -s $dbusername/$dbpwd <<! 
#set heading off feedback off verify off 
#set serveroutput on size 100000
#declare

#l_batchname   varchar2(50);
#l_targetfilename varchar2(100);
#begin
#l_batchname:=$l_insbatchname;
#SELECT nvl(TARGETFILENAME_1,'NULL') into l_targetfilename  from AIAS_Datasummary where sourcefilename=$l_insbatchname;
#dbms_output.put_line(l_targetfilename);
#end;
#/
#!`

#tgtfilename2=`sqlplus -s $dbusername/$dbpwd <<! 
#set heading off feedback off verify off 
#set serveroutput on size 100000
#declare

#l_batchname   varchar2(50);
#l_targetfilename varchar2(100);
#begin
#l_batchname:=$l_insbatchname;

#SELECT nvl(TARGETFILENAME_2,'NULL') into l_targetfilename  from AIAS_Datasummary where sourcefilename=$l_insbatchname;
#dbms_output.put_line(l_targetfilename);
#end;
#/
#!`

#export tgtfilereccount1
#export tgtfilereccount2
export errfilereccount
export errfilename
#export tgtfilename1
#export tgtfilename2

}
##########################################################################################################

PsError()

{
i_filename=$1

pserr1=`sqlplus -s $dbusername/$dbpwd << EOF
set serveroutput on size 100000;
set feedback off;
set heading off;
set colsep '|';
set trimspool on;
set linesize 80;
set lines 9999; 
set pages 999;
spool $infasrcdir/prestageerror.txt
  SELECT FILENAME,FIELDNAME,ERRORMESSAGE,FILERUNDATE FROM AIAS_PRESTAGEERROR WHERE FILENAME='$i_filename' AND ROWNUM<=5;
spool off		 
EXIT;
EOF`

#pserr=`cat prestageerror.txt|tail -1|head -1`
#export pserr
#cat $infasrcdir/temp.txt | tr -d ' ' | grep -v '^$' > $infasrcdir/prestageerror.txt
}


#cat $infasrcdir/temp.txt | tr -d ' ' | grep -v '^$' > $infasrcdir/prestageerror.txt

##########################################################################################################
# Function : Send mail in HTML format : Header
#
MailHeader ()
{
filename="$1"
ProcessingResult="$2"
echo "<!DOCTYPE html>"
echo "<html>"
echo "<head>"
echo "<title>sample HTML Mail Document</title>"
echo "<style>p {color:Royalblue} body {background-color:white}</style>"
echo "</head>"
echo "<body>"
echo "<hr width="100%" align=left>"
echo "<span style=font-size:10.0pt;font-family:"Arial"><b>"$tenantid_uc"-LND Data Processing Results</b></span>"
echo "<hr width="100%" align=left>"
echo "<table border = 0 cellpadding = 1>"
echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> Processing data for: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial"><b>"$tenantid_uc"</b></span></td> </tr>"
echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> Instance: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial">"$custinst"</span></td> </tr>"
echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> DATAFILE: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial"><b>"$filename"</b></span></td></tr>"
echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> Start Time: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial">"$starttime"</span></td></tr>"
if [ "$ProcessingResult" = "SUCCESS" -o "$ProcessingResult" = "ERROR" -o "$ProcessingResult" = "EXIT" -a  $filetype != "ADJUSTMENT"  ] ; then
	echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> End Time: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial">`date +%m/%d/%Y-%H:%M:%S`</span></td></tr>"
	echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> ODI Filename: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial">"$ODIbatchname"</span></td></tr>"
	echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> ODI GA Filename: </span></td><td><span style=font-size:8.0pt;font-family:"Arial">"$GAODIbatchname"</span></td></tr>"
fi
	echo "</table>"
	echo "<hr width="100%" align=left>"
	echo "<table border=1;style="width: 100%">"
	echo "<tr>"
if [ "$errfilereccount" = 0 ] && [ "$ProcessingResult" -eq "SUCCESS" ]; then
	echo "<td><span style=font-size:10.0pt;font-family:"Arial";"color:black"><b>Processing Results</b></span></td><td><span style=font-size:10.0pt;font-family:"Arial";"color:darkgreen"><b>"$ProcessingResult"</b></span></td></tr></table>"
	echo "<table border=1;style="width: 100%">"
	echo "<tr bgcolor="DEB887"><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>No Error Records Found</b></span></tr>"
	echo "</table>"
elif [ "$errfilereccount" != 0 ] && [ "$ProcessingResult" = "SUCCESS" ]; then
	echo "<td><span style=font-size:10.0pt;font-family:"Arial";"color:black"><b>Processing Results</b></span></td><td><span style=font-size:10.0pt;font-family:"Arial";"color:darkgreen"><b>"$ProcessingResult"</b></span></td></tr></table>"
	echo "<hr width="100%" align=left>"
	echo "<table border=1;style="width: 100%">"
	echo "<tr bgcolor="DEB887"><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>File Processing Summary</b></span></tr>"
	echo "</table>"
	echo "<table border=1;style="width: 100%">"
	echo "<tr bgcolor="cyan">"
	echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>Batch ID</b></span></td>"
	echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>Error File Name</b></span></td>"
	echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>Errors</b></span></td>"
	echo "</tr>"
	echo "<tr>"
	echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black">$filename</span></td>"
	echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black">$errfilename</span></td>"
	echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:red"><b>$errfilereccount</span></td>"
	echo "</table>"
	echo "<table border=1;style="width: 100%">"
	echo "<tr bgcolor="red"><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>Error Details</b></span></tr>"
	echo "</table>"
	echo "<table border=1;style="width: 100%">"
	echo "<tr bgcolor="cyan">"
	echo "<td ><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>File Name</td>"
	echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>Field Name</td>"
	echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>Error Description</td>"
	echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>File Run Date</td>"
	echo "</tr>"
while read filename
do
	FILENAME=`echo $filename|awk -F"|" '{print $1}'`
	FIELDNAME=`echo $filename|awk -F"|" '{print $2}'`
	ERRORMESSAGE=`echo $filename|awk -F"|" '{print $3}'`
	FILERUNDATE=`echo $filename|awk -F"|" '{print $4}'`
	echo "<tr>"
	echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black">$FILENAME</td>"
	echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black">$FIELDNAME</td>"
	echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black">$ERRORMESSAGE</td>"
	echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black">$FILERUNDATE</td>"
	echo "</tr>"
done < $infasrcdir/prestageerror.txt
	echo "</table>"
else 
	echo "<td><span style=font-size:10.0pt;font-family:"Arial";"color:black"><b>Processing Results</b></span></td>"
	echo "<td><span style=font-size:10.0pt;font-family:"Arial";"color:red"><b>$ProcessingResult</b></span></td>"	
fi
	echo "</tr>"
	echo "</table>"
	echo "<hr width="100%" align=left>"
	echo "<table border=1 cellpadding=1>"
	echo "<tr>"
	echo "<td><span style='font-size:8.0pt;font-family:"Arial";color:#7777cc'>"
	echo "<hr width="100%" align=left>"
	echo "<span style=font-size:10.0pt;font-family:"Arial"><b>"Output from this autoloader utility:"</b></span>"
	echo "<hr width="100%" align=left>"
	echo "<pre>"
}

##########################################################################################################
# Function : Send mail in normal format : Header
#
MailHeader1 ()
{
filename="$1"
ProcessingResult="$2"
echo "$tenantid_uc -LND Data Processing Results"
echo "Processing data for: $tenantid_uc"
echo "Instance: $custinst"
echo "DATAFILE: $filename"
echo "Start Time: $starttime"
if [ "$ProcessingResult" = "SUCCESS" -o "$ProcessingResult" = "ERROR" -o "$ProcessingResult" = "EXIT" -a  $filetype != "ADJUSTMENT"  ] ; then
	echo "End Time: `date +%m/%d/%Y-%H:%M:%S`"
	echo "SIZE: $filesizeMB"
	echo "RECORDS: $filereccount"
fi
echo ""
if [ "$errfilereccount" = 0 ] && [ "$ProcessingResult" -eq "SUCCESS" ]; then
	echo "Processing Results $ProcessingResult"
	echo "No Error Records Found"
elif [ "$errfilereccount" = 0 ] && [ "$ProcessingResult" -eq "SUCCESS" ]; then
	echo "Processing Results: $ProcessingResult"
	echo ""
	echo "File Processing Summary:"
	echo "Batch ID: $filename"
	echo "error File Name: $errfilename"
	echo "Errors: $errfilereccount"
	echo ""
	
	echo "Error Details:"
	echo ""
while read filename
do
	FILENAME=`echo $filename|awk -F"|" '{print $1}'`
	FIELDNAME=`echo $filename|awk -F"|" '{print $2}'`
	ERRORMESSAGE=`echo $filename|awk -F"|" '{print $3}'`
	FILERUNDATE=`echo $filename|awk -F"|" '{print $4}'`
	
	echo "File Name: $FILENAME"
	echo "Field Name: $FIELDNAME"
	echo "Error Description: $ERRORMESSAGE"
	echo "File Run Date: $FILERUNDATE"
	echo ""
done < $infasrcdir/prestageerror.txt
	echo ""
else 
	echo "Processing Results"
	echo "$ProcessingResult"	
fi
	
	echo "Output from this autoloader utility:"

}


#########################################################################################################
# Function : Send mail in HTML format : Footer
#
MailFooter ()
 {
echo "</pre>"
echo "</td>"
echo "</tr>"
echo "</table>"
echo "<hr width="100%" align=left>"
echo "</body>"
echo "</html>"
 }
 
#########################################################################################################
# Function : Send mail in HTML format : Footer
#
MailFooter1 ()
 {
echo ""
echo ""
echo "Regards"
echo "Callidus Operations team"
 }

 ########################################################################################################
# Function : Send mail in HTML format : Main function
#
SendMail ()
{
 templogfile="$1"
 mailtype="$2"
 filename="$3"
 description="$4"
 
Mailbody="$tempdir/Mailbody_Autoloader.txt"

subject="LND-$mailtype: $tenantid_uc [$custinst] --> $description"

cat /dev/null > $Mailbody

MailHeader $filename $mailtype >> $Mailbody
cat $templogfile >> $Mailbody
MailFooter >> $Mailbody

chmod 777 $Mailbody

#mailx -s "$subject" `cat $tntscriptsdir/$email` <$Mailbody

mutt -e 'set content_type=text/html' -s "$subject" ` cat $tntscriptsdir/$email ` <$Mailbody

}

######################################################################################################
# Function : To get filetype and other parameters based file-naming convention.
#
filenameProperties ()
{
filename=$1
case "$filename" in 
# *TRIG*)
	# basefilename=`echo $filename | awk -F\. '{print $1}' `
	# filetype=`echo $basefilename | cut -d "_" -f2`
	# month=`echo $basefilename | cut -d "_" -f3 | tr '[a-z]' '[A-Z]'`
	# year=`echo $basefilename | cut -d "_" -f4`
	# periodname="$month $year"
	# PERIOD1=`sqlplus -s $dbusername/$dbpwd << EOF
	# set serveroutput on size 100000;
	# set heading off;
	# select trim(to_char(STARTDATE,'YYYYMM')) as pd from cs_period@aias where UPPER(name) = UPPER('$periodname') and removedate = AIAS_CONSTANTS_PKG.GETENDOFTIME AND calendarseq =(SELECT calendarseq FROM cs_calendar@aias WHERE removedate = AIAS_CONSTANTS_PKG.GETENDOFTIME AND name = 'Shaklee Fiscal Calendar' );
	# EXIT SQL.SQLCODE;
	# EOF`
	
	# period=`echo $PERIOD1 |tail -1|head -1`
	# filedate=$period
	# region=`echo $basefilename | cut -d "_" -f5`
	# echo "[Autoloader] Extract filetype: $filetype"
	# echo "[Autoloader] Extract Period  : $filedate"
	# echo "[Autoloader] Extract Region  : $region"
	# export region;;
*.H.TXT|*B.TXT|*H.txt|*B.txt)
	basefilename=`echo $filename | awk -F\. '{print $1}' `
	#filetype=`echo $basefilename | cut -d "_" -f2`
	filedate=`echo $basefilename | rev | cut -d"_" -f1 | rev`
	#echo "[Autoloader] filetype  : $filetype"
	echo "[Autoloader] filedate  : $filedate"
	;;
*)
	basefilename=`echo $filename | awk -F\. '{print $1}' `
	#filetype=`echo $basefilename | cut -d "_" -f2`
	filedate=`echo $basefilename | rev | cut -d"_" -f1 | rev`
	#echo "[Autoloader] filetype  : $filetype"
	echo "[Autoloader] filedate  : $filedate"
	;;
esac

export filedate
}

######################################################################################################
# Function : During the execution upon SUCCESS or FAILURE at any stage, copy and remove Inbound files.
#
CleanInboundfolder ()
{

status=$1
file=$2

cd $inboundfolder

#Identify where to move files
 if [ $status = 0 ]; then
	  Destinationfolder=$archivefolder	
 else
	  Destinationfolder=$badfilesfolder
 fi

echo "[CleanInboundfolder] Copying files to $Destinationfolder."

#Move files
#add TFTX by sammi
#add CBPOLPYR,CBSPCMP,CBCFPOL by Simon
case "$file" in
*OGPT*|*OGPR*|*CMTX*|*PMTX*|*RTTX*|*EXCH*|*APD*|*PAQPB*|*OGPO*|*APD*|*POLMOVE*|*RPILP*|*RPILR*|*TFTX*|*CBCFPOL*|*CBPOLPYR*|*CBSPCMP*)
	bfile=`find $inboundfolder -name "*" ! -name "*.aud" ! -name "*.GZIP" -type f -exec basename \{} \; | grep "$file" | sort -n | head -1 | tail -1`
	hfile=`echo $bfile | sed "s/\.B\./\.H\./"`
	sh $tntscriptdir/JOB_ARCHIVE_SRC_FILS.sh $inboundfolder/$bfile $status
	sh $tntscriptdir/JOB_ARCHIVE_SRC_FILS.sh $inboundfolder/$hfile $status
	#if [[ "$file" == *"PMTX"* ]]; then
	#mv $infasrcdir/$hfile $datafile/DATA
	#mv $infasrcdir/$bfile $datafile/DATA
	#else	
	rm -f $infasrcdir/$hfile
	rm -f $infasrcdir/$bfile
	#fi
	rm -f $inboundfolder/$bfile
	rm -f $inboundfolder/$hfile;;
#Add third party TPTX by sammi 20220421	
*MA_*|*TPTX_*)
	bfile=`find $inboundfolder -name "*" ! -name "*.aud" ! -name "*.GZIP" -type f -exec basename \{} \; | grep "$file" | sort -n | head -1 | tail -1`
	sh $tntscriptdir/JOB_ARCHIVE_SRC_FILS.sh $inboundfolder/$bfile $status
	rm -f $inboundfolder/$bfile;;
*)

	if [ "$srcfiles" = "NA" -o "$srcfiles" = "" ]; then
		if [ $status -eq 0 ]; then
		cp $inboundfolder/*$file* $archivefolder/COMMON
		else
		cp $inboundfolder/*$file* $badfilesfolder/COMMON
		fi
		bfile=`find $inboundfolder -name "*" ! -name "*.aud" ! -name "*.GZIP" -type f -exec basename \{} \; | grep "$file" | sort -n | head -1 | tail -1`
		rm -f $inboundfolder/$bfile
	else
		cat /dev/null > $tempdir/srcfl_list.txt
		echo $srcfiles | tr , "\n" >> $tempdir/srcfl_list.txt
	    while read linefile
	    do			
			if [ $status -eq 0 ]; then
				mv $inboundfolder/*$linefile* $archivefolder/COMMON
				cp $infasrcdir/*$linefile* $archivefolder/COMMON
			else
				mv $inboundfolder/*$linefile* $badfilesfolder/COMMON
				cp $infasrcdir/*$linefile* $badfilesfolder/COMMON
			fi
			#bfile=`find $inboundfolder -name "*" -type f -exec basename \{} \; | grep "$linefile" | sort -n | head -1 | tail -1`
			#if [ "$bfile" = "" ]; then
			bfile=`find $infasrcdir -name "*" ! -name "*.aud" ! -name "*.GZIP" -type f -exec basename \{} \; | grep "$linefile" | sort -n | head -1 | tail -1`
			#fi
			rm -f $inboundfolder/$bfile
			rm -f $infasrcdir/$bfile
        done < $tempdir/srcfl_list.txt
	fi;;	
esac
	

echo "[CleanInboundfolder] Removing Temporary files."   
	rm -f $workdir/$lock*
	rm -f $tempdir/returncode.txt		 
	rm -f $tempdir/wfreturncode.txt
	rm -f $tempdir/dependency*
	rm -f $tempdir/seq_list.txt
	rm -f $tempdir/srcfl_list
	rm -f $tempdir/opfl_list
	rm -f $tempdir/spparam_list.txt
	rm -f $tempdir/pipeparam_list.txt
	bfile=`find $infasrcdir -name "*" ! -name "*.aud" ! -name "*.GZIP" -type f -exec basename \{} \; | grep "$file" | sort -n | head -1 | tail -1`
	if [ "$bfile" != "" ]; then
	rm -f $infasrcdir/$bfile
	fi
	
}

#####################################################################################################
# Function : check if file completely loaded or not.
#
CheckFileGrowth ()
{
  inputfile="$1"
  cd $inboundfolder

  cksumold=`cksum $inboundfolder/$inputfile |awk '{print $2}' |tail -1|head -1`
  sleep 5
  cksumnew=`cksum $inboundfolder/$inputfile | awk '{print $2}' |tail -1|head -1`

  if [ $cksumold = $cksumnew ] ; then 
     fileLoadstatus=0
  else
     fileLoadstatus=1
  fi  

return $fileLoadstatus
}

####################################################################################################
# Function : To verify Audit file and data file .
#
CheckCksum ()
{
 
auditfile="$1"
datafile="$2"
cksumDatafile=`cksum $inboundfolder/$datafile |awk '{print $2}' |tail -1|head -1`
#cksumAuditfile=`cat $inboundfolder/$auditfile |awk '{print $2}' |tail -1|head -1`
cksumAuditfile=`cat $inboundfolder/$auditfile | cut -d'!' -f2`
cksumAuditfile=`echo $cksumAuditfile | sed 's/^0*//'`
if [ "$cksumAuditfile" = "" ]; then
cksumAuditfile=0
fi

#echo "[CheckCksum] Data file size: $cksumDatafile"
#echo "[CheckCksum] File size in auditfile: $cksumAuditfile"

  if [ $cksumDatafile = $cksumAuditfile ] ; then 
     filestatus=0
  else
     filestatus=1
  fi  

return $filestatus
}


####################################################################################################
# Function : To retrieve details of ODI files generated by execute script for Inbound jobs only.
#
ODIfiledetails ()
{
	filename=$1
	#filetype=`echo $filename | cut -d "_" -f2`
#add TFTX by sammi
#add CBPOLPYR,CBSPCMP,CBCFPOL by Simon
#Add third party TPTX by Sammi 20220421
	case "$filename" in
	*OGPT*|*OGPR*|*CMTX*|*PMTX*|*RTTX*|*EXCH*|*APD*|*PAQPB*|*OGPO*|*APD*|*MA_*|*LIVECOUNT*|*LIMRA*|*PAPER*|*RELEASE_REGULAR_PAY*|*Mo_End*|*MONTHEND*|AI_*|*RPILP*|*RPILR*|*TFTX*|*CBCFPOL*|*CBPOLPYR|*CBSPCMP*|*TPTX_*)   
		
ODIfiles=`sqlplus -s $dbusername/$dbpwd << EOF
set heading off feedback off verify off
select ltrim(rtrim(NVL(ODIBATCHNAME,'NA')))||';'||ltrim(rtrim(NVL(ODIGABATCHNAME,'NA')))||';'||ltrim(rtrim(NVL(ODIASSGNBATCHNAME,'NA'))) from AIAS_INBOUNDFILE 
WHERE INBOUNDFILERUNKEY = (Select MAX(InboundFileRunKey) 
                                 From AIAS_INBOUNDFILE WHERE FILENAME = '$filename');
EOF`
		if [ $? -ne 0 ] ; then 
		echo "[Autoloader] Error - Error in retrieving ODI batchname"
		exit 1
		fi  
		ODIfiles=$(echo "${ODIfiles//[[:space:]]/}" | sed 's/~//g')
		if [ "$ODIfiles" == "" ]; then
		ODIfiles="NA;NA;NA"
		fi
		ODIbatchname=`echo $ODIfiles | cut -d ";" -f1`
		GAODIbatchname=`echo $ODIfiles | cut -d ";" -f2`
		ASSGNODIbatchname=`echo $ODIfiles | cut -d ";" -f3`
		
		echo "[Autoloader] ODI Batchname for $filename is   : $ODIbatchname" 
		echo "[Autoloader] ODI GABatchname for $filename is : $GAODIbatchname"
		echo "[Autoloader] ODI ASSIGNBatchname for $filename is : $ASSGNODIbatchname"
		
		export ODIbatchname
		export GAODIbatchname
		export ASSGNODIbatchname;;
	*)
		ODIbatchname=NA
		GAODIbatchname=NA
		ASSGNODIbatchname=NA
		
		echo "[Autoloader] ODI Batchname for $filename is   : $ODIbatchname" 
		echo "[Autoloader] ODI GABatchname for $filename is : $GAODIbatchname"
		echo "[Autoloader] ODI ASSIGNBatchname for $filename is : $ASSGNODIbatchname"
		
		export ODIbatchname
		export GAODIbatchname
		export ASSGNODIbatchname;;
	esac
}


####################################################################################################
# Function : To retrieve files from inbound check the run status AIAS_AUTOLOADER_STATS table and pull only the new file from inbound
#			 Ultimately, this is to achieve parallel execution of Autoloader.
#
Inboundfilecheck ()
{
	ls -tr $inboundfolder | grep -v ".aud" > $tempdir/inbound_list.txt
	if [ ! -s $tempdir/inbound_list.txt ]; then
		echo "[Autoloader] There are no files in inbound folder"
		exit
	fi
	while read fileline
	do
		if [ "$fileline" = "" ]; then
			current_filetype=""
			break
		fi
		echo "[Autoloader] file name is $fileline"
		cur_fltype=`echo $fileline |  cut -d "_" -f2`
		current_filetype=""
		echo "[Autoloader] Check if this file type $cur_fltype is already running or not by checking AIAS_AUTOLOADER_STATS table"

case "$fileline" in
*TBLBANK-ACCOUNT*|*AGENT-DECEASED-BENEF*)
output=`sqlplus -s $dbusername/$dbpwd <<EOF
set heading off feedback off verify off
select count(*) from AIAS_AUTOLOADER_STATS where (filetype like '%TBLBANK-ACCOUNT%' OR filetype like '%AGENT-DECEASED-BENEF%') and ENDTIME is null;
exit;
EOF`
;;
*TBLPARIS-DM-ASSIGNMENT*|*TBLPARIS-DM-ASSIGNMENT-HIST*)
output=`sqlplus -s $dbusername/$dbpwd <<EOF
set heading off feedback off verify off
select count(*) from AIAS_AUTOLOADER_STATS where (filetype like '%TBLPARIS-DM-ASSIGNMENT%' OR filetype like '%TBLPARIS-DM-ASSIGNMENT-HIST%') and ENDTIME is null;
exit;
EOF`
;;
*TBLNADOR-PAYEE-SETUP*|*TBLNADOR-PAYEE-SETUP-HIST*)
output=`sqlplus -s $dbusername/$dbpwd <<EOF
set heading off feedback off verify off
select count(*) from AIAS_AUTOLOADER_STATS where (filetype like '%TBLNADOR-PAYEE-SETUP%' OR filetype like '%TBLNADOR-PAYEE-SETUP-HIST%') and ENDTIME is null;
exit;
EOF`
;;
*)
output=`sqlplus -s $dbusername/$dbpwd <<EOF
set heading off feedback off verify off
select count(*) from AIAS_AUTOLOADER_STATS where filetype like '%$cur_fltype%' and ENDTIME is null;
exit;
EOF`
;;
esac

		if [ $? -ne 0 ] ; then 
			echo "[Autoloader] failed to retrive count from AIAS_AUTOLOADER_STATS table"
			exit 20
		fi
		output=$(echo $output | sed 's/~//g')
		
		if [ $output -eq 0 ]; then
			echo "[Autoloader] There are no instances of $cur_fltype type running currently, so autoloader will take $cur_fltype files for processing now"
			if [ "$cur_fltype" = "MA" ]; then
				current_filetype=`echo $cur_fltype | sed 's/MA/MA_/'`
			else
				current_filetype=`echo $cur_fltype | cut -d "." -f1`
			fi
			
			break
		else
			echo "[Autoloader] this $cur_fltype file type is running currently. Repeat check for next file"
		fi	
	done < $tempdir/inbound_list.txt
	
	if [ "$current_filetype" = "" ]; then
		echo "[Autoloader] There are no files in inbound folder"
		exit
	else
		export current_filetype
	fi
}


Autoloader_entry ()
{
fltp=$1
flnm=`echo $2 | cut -d '.' -f1`
currentstate=$3
filtp="'""$fltp""'"
flname="'""$flnm""'"
procstate="$currentstate"

if [ $currentstate -eq 0 ]; then
	echo "[Autoloader] Stage proccess flag is $currentstate, so new entry with $fltp and $flnm will in inserted in Autoloader stats table"
	OUTPUT=`sqlplus -s $dbusername/$dbpwd <<! 
set heading off feedback off verify off 
set serveroutput on size 100000
DECLARE
l_filetype varchar2(255) ;
l_filename        varchar2(255) ;
l_process_stage  varchar2(255) ;
BEGIN
l_filetype :=$filtp;
l_filename        :=$flname;
l_process_stage  :=$procstate;
insert into aias_autoloader_stats (FILENAME,FILETYPE,STARTTIME,ENDTIME,STAGEPROCESS)
values (l_filename,l_filetype,sysdate,null,l_process_stage);
if sql%Found then
dbms_output.put_line('Recordinserted');
end if ;
commit;
exception 
when others then
dbms_output.put_line(sqlerrm);
END;
/
!`

	valcnt=`echo $OUTPUT | awk '{ print $1 }'`
	
	if [ "$valcnt" != "Recordinserted" ]; then
	echo "[Autoloader]  1 - Error in aias_autoloader_stats Insertion"
	echo $OUTPUT
	exit 1
	fi
	echo "[Autoloader] Entry Inserted in aias_autoloader_stats table"

else
	echo "[Autoloader] Stage proccess flag is $currentstate, so entry for $fltp and $flnm will be updated in Autoloader stats table"
	OUTPUT=`sqlplus -s $dbusername/$dbpwd <<! 
set heading off feedback off verify off 
set serveroutput on size 100000
DECLARE
l_filename        varchar2(255) ;
l_process_stage  varchar2(255) ;
BEGIN
l_filename        :=$flname;
l_process_stage  :=$procstate;
update aias_autoloader_stats set ENDTIME=sysdate,STAGEPROCESS=l_process_stage
where AUTO_ID=(Select MAX(AUTO_ID) 
                                 From aias_autoloader_stats 
	   		                     WHERE FILENAME = l_filename AND 
			                           ENDTIME  is null);
if sql%Found then
dbms_output.put_line('Recordupdated');
end if ;
commit;
exception 
when others then
dbms_output.put_line(sqlerrm);
END;
/
!`

	valcnt=`echo $OUTPUT | awk '{ print $1 }'`
	
	if [ "$valcnt" != "Recordupdated" ]; then
	echo $valcnt
	echo "[Autoloader]1 - Error in aias_DataFileSummary Updation"
	echo $OUTPUT
	exit 1
	fi
	
	echo "[Autoloader] Entry Updated in aias_autoloader_stats table"
fi

}


#####################################################################################################
# Function : check if file exists or not.
#
filecheck ()
{
inputfile="$1"
cd $inboundfolder
filecheckstatus=1
waittime=0
while [ $filecheckstatus -ne 0 ]
do
	if [ -f $inboundfolder/$inputfile ]; then
		filecheckstatus=0
		echo "[Autoloader] $inputfile is avaiable in inbound"
		break
	else
		echo "[Autoloader] $inputfile not yet received"
		sleep 60
		waittime=`expr $waittime + 1`
		if [ $waittime -eq 15 ]
		then
			echo "Timed out and exiting"
			break
		else
			echo "Repeating the file check for $waittime iteration"
		fi	
	fi
done

return $filecheckstatus
}

IBfilecheck ()
{
inputfile="$1"
cd $inboundfolder
#ibfilestatus=1

if [ -f $inboundfolder/$inputfile ]; then
	ibfilestatus=0
	echo "[Autoloader] $inputfile is avaiable in inbound"
	echo "[Autoloader] moving $inputfile waiting folder"
	mv $inboundfolder/$inputfile $datafile/waiting
elif [ -f $datafile/waiting/$inputfile ]; then
	ibfilestatus=0
	echo "[Autoloader] $inputfile is avaiable in waiting folder"
else
	echo "[Autoloader] $inputfile not yet received"
	ibfilestatus=1
fi

export ibfilestatus
}



##################################################################################################
# Function : set lock at wrapper script (continuous execution) level
#
wrapper_entry ()
{
fltp=$1
flnm=`echo $2 | cut -d '.' -f1`
currentstate=$3
filtp="'""$fltp""'"
flname="'""$flnm""'"
procstate="$currentstate"

if [ $currentstate -eq 0 ]; then
	echo "[Autoloader] Stage proccess flag is $currentstate, so new entry with $fltp and $flnm will in inserted in Autoloader stats table"
	OUTPUT=`sqlplus -s $dbusername/$dbpwd <<! 
set heading off feedback off verify off 
set serveroutput on size 100000
DECLARE
l_filetype varchar2(255) ;
l_filename        varchar2(255) ;
BEGIN
l_filetype :=$filtp;
l_filename        :=$flname;
insert into aias_wrapper_lock (FILENAME,FILETYPE)
values (l_filename,l_filetype);
if sql%Found then
dbms_output.put_line('Recordinserted');
end if ;
commit;
exception 
when others then
dbms_output.put_line(sqlerrm);
END;
/
!`

	valcnt=`echo $OUTPUT | awk '{ print $1 }'`
	
	if [ "$valcnt" != "Recordinserted" ]; then
	echo "[Autoloader]  1 - Error in aias_wrapper_lock Insertion"
	echo $OUTPUT
	exit 1
	fi
	echo "[Autoloader] Entry Inserted in aias_wrapper_lock table"

else
	echo "[Autoloader] Stage proccess flag is $currentstate, so entry for $fltp and $flnm will be updated in Autoloader stats table"
	OUTPUT=`sqlplus -s $dbusername/$dbpwd <<! 
set heading off feedback off verify off 
set serveroutput on size 100000
BEGIN
delete from aias_wrapper_lock;
commit;
exception 
when others then
dbms_output.put_line(sqlerrm);
END;
/
!`

	echo "[Autoloader] Entry deleted in aias_wrapper_lock table"
fi

}


####################################################################################################
# Function : To retrieve files from inbound check the run status aias_wrapper_lock table and pull only the new file from inbound
#			 Ultimately, this is to achieve parallel and continuous execution of Autoloader.
#
wrapperfilecheck ()
{
	ls -tr $inboundfolder | grep -v ".aud" > $tempdir/inbound_list.txt
	if [ ! -s $tempdir/inbound_list.txt ]; then
		echo "[Autoloader] There are no files in inbound folder"
		exit
	fi
	while read fileline
	do
		if [ "$fileline" = "" ]; then
			wrpcurrent_filetype=""
			break
		fi
		echo "[Autoloader] file name is $fileline"
		cur_fltype=`echo $fileline |  cut -d "_" -f2`
		wrpcurrent_filetype=""
		echo "[Autoloader] Check if this file type $cur_fltype is already running or not by checking aias_wrapper_lock table"

case "$fileline" in
*TBLBANK-ACCOUNT*|*AGENT-DECEASED-BENEF*)
output=`sqlplus -s $dbusername/$dbpwd <<EOF
set heading off feedback off verify off
select count(*) from aias_wrapper_lock where (filetype like '%TBLBANK-ACCOUNT%' OR filetype like '%AGENT-DECEASED-BENEF%');
exit;
EOF`
;;
*TBLPARIS-DM-ASSIGNMENT*|*TBLPARIS-DM-ASSIGNMENT-HIST*)
output=`sqlplus -s $dbusername/$dbpwd <<EOF
set heading off feedback off verify off
select count(*) from aias_wrapper_lock where (filetype like '%TBLPARIS-DM-ASSIGNMENT%' OR filetype like '%TBLPARIS-DM-ASSIGNMENT-HIST%');
exit;
EOF`
;;
*TBLNADOR-PAYEE-SETUP*|*TBLNADOR-PAYEE-SETUP-HIST*)
output=`sqlplus -s $dbusername/$dbpwd <<EOF
set heading off feedback off verify off
select count(*) from aias_wrapper_lock where (filetype like '%TBLNADOR-PAYEE-SETUP%' OR filetype like '%TBLNADOR-PAYEE-SETUP-HIST%');
exit;
EOF`
;;
*)
output=`sqlplus -s $dbusername/$dbpwd <<EOF
set heading off feedback off verify off
select count(*) from aias_wrapper_lock where filetype like '%$cur_fltype%';
exit;
EOF`
;;
esac

		if [ $? -ne 0 ] ; then 
			echo "[Autoloader] failed to retrive count from aias_wrapper_lock table"
			exit 20
		fi
		output=$(echo $output | sed 's/~//g')
		
		if [ $output -eq 0 ]; then
			echo "[Autoloader] There are no instances of $cur_fltype type running currently, so autoloader will take $cur_fltype files for processing now"
			if [ "$cur_fltype" = "MA" ]; then
				wrpcurrent_filetype=`echo $cur_fltype | sed 's/MA/MA_/'`
			else
				wrpcurrent_filetype=`echo $cur_fltype | cut -d "." -f1`
			fi
			
			break
		else
			echo "[Autoloader] this $cur_fltype file type is running currently. Repeat check for next file"
		fi	
	done < $tempdir/inbound_list.txt
	
	if [ "$wrpcurrent_filetype" = "" ]; then
		echo "[Autoloader] There are no files in inbound folder"
		exit
	else
		export wrpcurrent_filetype
	fi
}


####################################################################################################
# Function : To find, zip, gpg and move the output files to outbound
# Usage: encryptnmv <filename> <sourcepath>
#
encryptnmv ()
{
	obfilenm=$1
	srcpath=$2
	
	echo "[CallScript] Checking for the output file with pattern $obfilenm in Informatica target folder $srcpath"
	opfl=`ls -t $srcpath | grep $obfilenm | head -n1`
	if [ "$opfl" = "" ]; then
		echo "[CallScript] Output file $obfilenm is not available in target folder $srcpath. So, exiting.."
		exit
	fi
	echo "[CallScript] Output file $opfl is available in target folder $srcpath"
	cd $srcpath
	echo "[CallScript] Zipping output file $obfilenm"
	sed 's/$/\r/' "$opfl" > "$opfl".tmp
	cat "$opfl".tmp > "$opfl"
	rm -f "$opfl".tmp
	zip -r "$opfl".GZIP "$opfl"
	if [ $? -ne 0 ]; then			
		echo "CallScript] Failed to ZIP output file $opfl to outbound folder"
		echo "wfret=1" > $tempdir/wfreturncode.txt
		exit 1
	fi
	echo "[CallScript] Encrypting output file $opfl"
	gpg -r $aia_pubkey -e --output "$opfl".GZIP.PGP "$opfl".GZIP
	if [ $? -ne 0 ]; then			
		echo "CallScript] Failed to encrypt output file $opfl to outbound folder"
		echo "wfret=1" > $tempdir/wfreturncode.txt
		exit 1
	fi
	cp "$srcpath/$opfl" $archivefolder/OUTPUT
	cp "$srcpath/$opfl".GZIP.PGP $lndoutboundfolder
	if [ $? -ne 0 ]; then			
		echo "CallScript] Failed in moving output file $opfl to outbound folder"
		echo "wfret=1" > $tempdir/wfreturncode.txt
		exit 1
	fi
	echo "[CallScript] Output file $opfl.GZIP.PGP is moved to Outboud folder successfully"
	mv "$srcpath/$opfl" $archivefolder/OUTPUT
	mv "$srcpath/$opfl".GZIP.PGP $archivefolder/OUTPUT
	rm -f "$srcpath/$opfl".GZIP
	
}


####################################################################################################
# Function : To send pipeline performance report mails
# Usage: PLreportmail <Processing Unit> <Period> <Pipelinename> <Stagename - optional> <starttime - optional>
#
PLreportmail ()
{
Processingunit=$1
Period=$2
stg=$3
stgnm=$4
strttme=$5
strtfmt=`echo -n "$5" | wc -c`
echo "<!DOCTYPE html>"
echo "<html>"
echo "<head>"
echo "<title>sample HTML Mail Document</title>"
echo "<style>p {color:Royalblue} body {background-color:white}</style>"
echo "</head>"
echo "<body>"
echo "<hr width="100%" align=left>"
if [ "$stgnm" != "" ]; then
	if [ "$strtfmt" -ne 1 ]; then
		echo "<span style=font-size:10.0pt;font-family:"Arial";"color:red"><b>ALERT: "$tenantid_uc"-Pipeline stage $stgnm is running for a long time.</b></span>"
	else 
		echo "<span style=font-size:10.0pt;font-family:"Arial";"color:green"><b>"$tenantid_uc"-Pipeline stage $stgnm is completed</b></span>"
	fi
else
	echo "<span style=font-size:10.0pt;font-family:"Arial"><b>"$tenantid_uc"-Pipeline Performance Report</b></span>"
fi
echo "<hr width="100%" align=left>"
echo "<table border=0 cellpadding=1>"
	echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> Processing data for: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial"><b>"$tenantid_uc"</b></span></td> </tr>"
	echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> Instance: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial">"$custinst"</span></td> </tr>"
	echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> Processing Unit: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial"><b>"$Processingunit"</b></span></td></tr>"
	echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> Period: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial"><b>"$Period"</b></span></td></tr>"
	echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> Pipeline: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial"><b>"$stg"</b></span></td></tr>"
	if [ "$stgnm" != "" ]; then
	if [ "$strtfmt" -ne 1 ]; then
		echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> Pipeline Stage: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial"><b>"$stgnm"</b></span></td></tr>"
		echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> Start Time: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial">"$strttme"</span></td></tr>"
		echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> End Time: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial">In progress</span></td></tr>"
	else 
		echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> Pipeline Stage: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial"><b>"$stgnm"</b></span></td></tr>"
		echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> End Time: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial">Completed</span></td></tr>"
	fi
	else
		echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> Start Time: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial">"$starttime"</span></td></tr>"
		echo "<tr><td><span style=font-size:8.0pt;font-family:"Arial"> End Time: </span></td> <td><span style=font-size:8.0pt;font-family:"Arial">`date +%m/%d/%Y-%H:%M:%S`</span></td></tr>"
	fi
echo "</table>"
echo "<hr width="100%" align=left>"
if [ "$stgnm" != "" ]; then
if [ "$strtfmt" -ne 1 ]; then
	echo "<span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>Pipeline Performance Comparison against Previous "$Processingunit" PL run</b></span>"
	echo "<hr width="100%" align=left>"
#else
	#echo "<span style=font-size:10.0pt;font-family:"Arial";"color:black"><b>Pipeline stage "$stgnm" is Completed.</b></span>"
	#echo "<hr width="100%" align=left>"
fi
else
	echo "<span style=font-size:10.0pt;font-family:"Arial";"color:black"><b>Pipeline stage "$stgnm" is running for a long time. Please login to Portal and do the necessary</b></span>"
	echo "<hr width="100%" align=left>"
	echo "<span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>Pipeline Performance Comparison for "$stgnm" stage against Previous "$Processingunit" PL average runs</b></span>"
fi
if [ "$strtfmt" -ne 1 ]; then
	echo "<table border=1 cellpadding=1;style="width: 100%">"
	echo "<tr bgcolor="cyan">"
		echo "<td ><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>PL Stage Name</td>"
		echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>Average Run Time (in mins)</td>"
		echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>Buffer Time (in mins)</td>"
		echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>Current Run Time (in mins)</td>"
		echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>Difference (Negative/RED means overrun)</td>"
		
	echo "</tr>"
	if [ "$stgnm" == "" ]; then
	outputfilenm=$tempdir/plfullreport.out 
	else
	outputfilenm=$tempdir/plstgrpt.out 
	fi
	while read filename
	do
		stagenm=`echo $filename|awk -F"|" '{print $1}'`
		avgrun=`echo $filename|awk -F"|" '{print $2}'`
		bufftm=`echo $filename|awk -F"|" '{print $3}'`
		currun=`echo $filename|awk -F"|" '{print $4}'`
		differ=`echo $filename|awk -F"|" '{print $5}'`
		echo "<tr>"
		if [ $(echo "$differ < 0"|bc -l) -eq 1 ];then 
			echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:red"><b>$stagenm</td>"
			echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>$avgrun</td>"
			echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>$bufftm</td>"
			echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:red"><b>$currun</td>"
			echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:red"><b>$differ</td>"
		else
			echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black">$stagenm</td>"
			echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black">$avgrun</td>"
			echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black"><b>$bufftm</td>"
			echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black">$currun</td>"
			echo "<td><span style=font-size:8.0pt;font-family:"Arial";"color:black">$differ</td>"
		fi
		echo "</tr>"
	done < $outputfilenm
	echo "</table>"
echo "<p style=font-size:8.0pt;font-family:"Arial">Note: the PL Stages in <span style=font-size:8.0pt;font-family:Arial;color:red>RED</span> font took more time than pervious run</p>"
echo "<hr width="100%" align=left>"
fi
echo "</body>"
echo "</html>"	
}


####################################################################################################
# Function : To check autoloader is running duplicate instances			 
#
wrapperdups ()
{

output=`sqlplus -s $dbusername/$dbpwd <<EOF
set heading off feedback off verify off
select count(*) as cont from (select filetype,count(*) as cnt from aias_wrapper_lock group by filetype) where cnt>1;
exit;
EOF`

output=$(echo $output | sed 's/~//g')
if [ $output -eq 0 ]; then
	wrapstatus=0
else
	wrapstatus=1
fi
export wrapstatus
}

####################################################################################################
# Function : To check any inbound is running for CMTX/PMTX			 
#
commissioninbound ()
{

output=`sqlplus -s $dbusername/$dbpwd <<EOF
set heading off feedback off verify off
select count(*) as cont from aias_autoloader_stats where filetype in ('CMTX','PMTX','TFTX') and endtime is null;
exit;
EOF`

output=$(echo $output | sed 's/~//g')
if [ $output -eq 0 ]; then
	commstatus=0
else
	commstatus=1
	sleep 120
fi
export commstatus
}

####################################################################################################
# Function : To check to update oper_cycle_date			 
#
updatecontroltb ()
{
cdate="'""$1""'"

OUTPUT=`sqlplus -s $dbusername/$dbpwd <<! 
set heading off feedback off verify off 
set serveroutput on size 100000
DECLARE
l_date        varchar2(255) ;
BEGIN
l_date        :=$cdate;
UPDATE in_etl_control SET txt_key_value=l_date,dte_update_date=sysdate
WHERE txt_key_string='OPER_CYCLE_DATE' and TXT_FILE_NAME='GLOBAL';
if sql%Found then
dbms_output.put_line('Recordupdated');
end if ;
commit;
exception 
when others then
dbms_output.put_line(sqlerrm);
END;
/
!`

	valcnt=`echo $OUTPUT | awk '{ print $1 }'`
	
	if [ "$valcnt" != "Recordupdated" ]; then
	echo $valcnt
	echo "[Autoloader]1 - Error in in_etl_control Updation"
	echo $OUTPUT
	exit 1
	fi

}

replaceNonPrintable()
{
        oriFile=$1

        sed -i "s/[^[:print:]\t]//g" "$oriFile"
        # sed -i "s/\n/\r\n/g" "$oriFile"
        # sed -i "/^$/d" "$oriFile"
        #cp $oriFile ~/data
}
