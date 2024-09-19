#!/bin/bash
#<<aias_misc.sh>>
#-------------------------------------------------------------
# Date               Author                  Version
#-------------------------------------------------------------
# 04/20/2017        Suresh Musham        		 1.0	
#
# Description : 
# Filehandler script invoked by custom Transaction file
#
# Command line: aias_misc.sh <arguments as required by definition>
#################################################################################################################
PATH=/opt/someApp/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
. /apps/Callidus/aias/integrator/aias_setenv_variable.sh
. /apps/Callidus/aias/integrator/aias_utilityfunctions.sh

#filename=`echo $1 | sed "s/_//"`
filename=$1
filehandlername=$0
#filehandler=`echo $filehandlername | cut -d "/" -f6`
filehandler=`basename $filehandlername`

# outbound file varibales
if [[ $filename == *"_"* ]]; then 
if [[ "$filename" =~ '_'$ ]]; then
filename=`echo $filename | sed "s/_//"`
filetype=$filename
else
filetype=`echo $filename | cut -d "_" -f2`
fi
else
filetype=$filename
fi
hname=`hostname | tr '[a-z]' '[A-Z]'`
vhname=` echo $hname | cut -d "." -f1 `
vhname=` echo $vhname | cut -d "-" -f3 `

vdate=`date +%Y%m%d`
cycledt=`sed -n 1p $infasrcdir/CYCLE_DATE.DAT`
cdate=$(date -d "`echo $cycledt`" +%Y%m%d)

#Get variables from typemap configuration file
ReadParameters $filetype
if [ $? -ne 0 ]; then
	 echo "wfret=1" > $tempdir/wfreturncode.txt
	 exit 1
fi

echo "[CallScript]"
echo "[CallScript]  === [`date +%Y%m%d_%H%M%S`]:: START:: $filehandler ]===" 
echo "[CallScript]"

#Handle special case for Fixed Value Regular pay trigger

if [ "$filetype" = "FVREGULARPAY" ]; then
	informaticsrcfl="AIAS_"$filetype"_"$vhname".txt"
	echo "[CallScript] Creating source file based the Fixed value regular pay trigger: $informaticsrcfl in $infasrcdir"
	touch $infasrcdir/$informaticsrcfl
	fv_ind=`echo $filename | cut -d'_' -f3`
	fv_dt=`echo $filename | cut -d'_' -f5 | cut -d'.' -f1`
	fv_dtYYYY=`echo $fv_dt | cut -c1-4`
	fv_dtMM=`echo $fv_dt | cut -c5-6`
	fv_dtDD=`echo $fv_dt | cut -c7-8`
	echo "FV_RELEASE_REGULAR_PAY,"$fv_ind","$fv_dtYYYY"-"$fv_dtMM"-"$fv_dtDD > $infasrcdir/$informaticsrcfl
	echo "[CallScript] Successfully created source file in informatica source path with `cat $infasrcdir/$informaticsrcfl | head -1`"
	#FV_RELEASE_REGULAR_PAY,0,2016-11-30
fi

#Get the count of records from source file
if [ "$srcfiles" = "NA" ]; then
	echo "[CallScript] No source files required for the trigger file $filename"
else
	cat /dev/null > $tempdir/srcfl_list.txt
	echo $srcfiles | tr , "\n" >> $tempdir/srcfl_list.txt
	echo "[CheckDataFile] Wait for 10 seconds and Check if the source file is avaibale in inbound path"
	sleep 10
	while read linefile
	do
		srfl=`find $inboundfolder -name "*$linefile*" -type f -exec basename \{} \; | sort -n | head -1 | tail -1`
		if [ "$srfl" = "" ]; then
			echo "[CheckDataFile] Source file with pattern $linefile is not available in inbound path. So exiting ..." 
			echo "wfret=1" > $tempdir/wfreturncode.txt
			exit 1
		else
			echo "[CheckDataFile] Source file $srfl is available in inbound path"
			echo "[CheckDatafile] Moving source file to informatica source path"
			informaticsrcfl="AIAS_"$linefile"_"$vhname".TXT"
			cp $inboundfolder/$srfl $infasrcdir
			if [ $? -ne 0 ]; then
				echo "[Callscript] failed to move source file $linefile to informatica folder"
				echo "wfret=1" > $tempdir/wfreturncode.txt
				exit 1
			fi
			echo "[CheckDatafile] Renaming source file name to standard informatica source file format"
			mv $infasrcdir/$srfl $infasrcdir/$informaticsrcfl
			if [ $? -ne 0 ]; then
				echo "[Callscript] failed to move source file $linefile to informatica source filepath"
				echo "wfret=1" > $tempdir/wfreturncode.txt
				exit 1
			fi
			echo "[CallScript] Checking Record count of source files $linefile"
			CountofRecords $linefile
			if [ $? -ne 0 ]; then
				echo "wfret=1" > $tempdir/wfreturncode.txt
				exit 1
			fi
			echo "[CallScript] [$linefile] received from $tenantid_uc, and it has [ $filerecords ] records."
		fi	

	done < $tempdir/srcfl_list.txt
fi

echo "[CallScript] Current FileHanler path:" `dirname $filehandlername`
echo "[CallScript] Current FileHanler:" $filehandler
echo "[CallScript] $tenantid_uc Supplied Inbound file name :" $filename
echo "[CallScript] $tenantid_uc INBOUND filetype :" $filetype 
echo "[CallScript] Outbound Workflow that will be trigger is :" $inboundwfname


###################################################################################################
# Call the Utility Function to log the start of an inbound file process
LoggingProcess $filehandler $filename $stagetablename $filename
processretcode=$?
  if [ $processretcode -ne 0 ]; then
	 echo "wfret=1" > $tempdir/wfreturncode.txt
	 exit 1
  fi

###############################################################################################
# Pull Informatica parameter file and check for its existence in informatica source path  
echo "[CheckParamfile] Check for the parameter file $paramfl existence in informatica source dir."
cd $infasrcdir
if [ ! -f "$infasrcdir/$paramfl" ]; then
	echo "[CheckParamfile] No Param file $paramfl available at informatica source path, Exiting..."
	echo "wfret=1" > $tempdir/wfreturncode.txt
	exit 1
fi
#echo "[CheckParamfile] Parameter file $paramfl is available at informatica source dir"

parameterfile=$infasrcdir/$paramfl

export parameterfile
echo "[CallScript] Parameter file $parameterfile available in $infasrcdir. "

##############################################################################################
#Enforce Flag handling
case "$filetype" in
SCAL751M|SCAL600M|SCAL752M|FVREGULARPAY|AGENT-DECEASED-BENEF|TBLBANK-ACCOUNT|JCAL411M|TBLNADOR-PAYEE-SETUP|TBLNADOR-PAYEE-SETUP-HIST|TBLPARIS-DM-ASSIGNMENT|TBLPARIS-DM-ASSIGNMENT-HIST|TBLAM-MOVE-EXTRA|TBLAM-MOVE-EXTRA-HIST)
	echo ",FALSE" > $infasrcdir/ENFORCE_RUN.DAT
	echo "[CallScript] Enforce flag is updated in ENFORCE_RUN.DAT";;
esac

###############################################################################################
#Executing Workflow

#ExecuteWorkflow $infa_foldername $inboundwfname $filehandler $filename $stagetablename $filereccount
ExecuteWorkflow $infa_foldername $inboundwfname $filehandler $filename $stagetablename $filerecords
wfretcode=$?
  if [ $wfretcode -ne 0 ]; then
	 echo "wfret=1" > $tempdir/wfreturncode.txt
	 exit 1
  fi
  
#Handle special case for SCAL874W and SCAL874M	
if [ "$filetype" = "SCAL874W" ]; then
cycledt=`sed -n 1p $infatgtdir/weekly_cdate.out`
cdate=$(date -d "`echo $cycledt`" +%Y%m%d)
elif [ "$filetype" = "SCAL874M" ]; then
cycledt=`sed -n 1p $infatgtdir/monthly_cdate.out`
cdate=$(date -d "`echo $cycledt`" +%Y%m%d)
else
cycledt=`sed -n 1p $infasrcdir/CYCLE_DATE.DAT`
cdate=$(date -d "`echo $cycledt`" +%Y%m%d)
fi
###############################################################################################
# Move output files if any to outbound
###############################################################################################
if [ "$outputfils" = "NA" ]; then
	echo "[CallScript] There will be no output files for the trigger file $filename"
else
	cat /dev/null > $tempdir/op_list.txt
	echo $outputfils | tr , "\n" >> $tempdir/op_list.txt
	while read linefile
	do
	#check for the output files in informatica target folder
	echo "[CallScript] Checking for the output file with pattern $linefile in Informatica target folder $infatgtdir"
	case "$linefile" in
	*"+"*)		
		flname1=`echo $linefile | cut -d'+' -f1`
		flname2=`echo $linefile | cut -d'+' -f2`
		cnt=`ls $infatgtdir | grep $flname2 | wc -l`
		if [ "$cnt" -gt 0 ]; then
			linefile=$flname2
		else
			linefile=$flname1
		fi
		;;
	esac
	#opfl=`find $infatgtdir -maxdepth 1 -name "*$linefile*" -type f -exec basename \{} \; | sort -n | tail -1 | head -1`
	opfl=`ls -t $infatgtdir | grep $linefile | head -n1`
	if [ "$opfl" = "" ]; then
		echo "[CallScript] Output file $linefile is not available in target folder $infatgtdir. So, exiting.."
		echo "wfret=1" > $tempdir/wfreturncode.txt
		exit 1
	else
		echo "[CallScript] Output file $opfl is available in target folder $infatgtdir"
		sybopfl=`echo $opfl | cut -d'.' -f1`"_"$cdate".TXT"
		sybopfl=`echo $sybopfl | sed 's/[()]//g'`
		echo "[CallScript] Output file $opfl is converted to $sybopfl"
		mv $infatgtdir/$opfl $infatgtdir/$sybopfl
		echo "[CallScript] Output file $opfl is converted to $sybopfl"
		cd $infatgtdir
		echo "[CallScript] Zipping output file $sybopfl"
		sed 's/$/\r/' "$opfl" > "$opfl".tmp
		cat "$opfl".tmp > "$opfl"
		rm -f "$opfl".tmp
		zip -r "$sybopfl".GZIP "$sybopfl"
		if [ $? -ne 0 ]; then			
			echo "CallScript] Failed to ZIP output file $sybopfl to outbound folder"
			echo "wfret=1" > $tempdir/wfreturncode.txt
			exit 1
		fi
		echo "[CallScript] Encrypting output file $sybopfl"
		gpg -r $aia_pubkey -e --output "$sybopfl".GZIP.PGP "$sybopfl".GZIP
		if [ $? -ne 0 ]; then			
			echo "CallScript] Failed to encrypt output file $sybopfl to outbound folder"
			echo "wfret=1" > $tempdir/wfreturncode.txt
			exit 1
		fi
		cp "$infatgtdir/$sybopfl" $archivefolder/OUTPUT
		cp "$infatgtdir/$sybopfl".GZIP.PGP $lndoutboundfolder
		if [ $? -ne 0 ]; then			
			echo "CallScript] Failed in moving output file $opfl to outbound folder"
			echo "wfret=1" > $tempdir/wfreturncode.txt
			exit 1
		fi
		echo "[CallScript] Output file $opfl.GZIP.PGP is moved to Outboud folder successfully"
		mv "$infatgtdir/$sybopfl" $archivefolder/OUTPUT
		mv "$infatgtdir/$sybopfl".GZIP.PGP $archivefolder/OUTPUT
		rm -f "$infatgtdir/$sybopfl".GZIP
	fi		
	done < $tempdir/op_list.txt
fi
  
echo "[CallScript]"
echo "[CallScript]  === [`date +%Y%m%d_%H%M%S`]::END:: $filehandler ] ==="
exit $wfretcode

##################################################################################################