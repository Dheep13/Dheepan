#!/bin/bash
#<<aias_outbound.sh>>
#-------------------------------------------------------------
# Date               Author                  Version
#-------------------------------------------------------------
# 04/12/2017        Suresh Musham        		 1.0	
#
# Description : 
# Filehandler script invoked by custom Transaction file
#
# Command line: aias_outbound.sh <arguments as required by definition>
#################################################################################################################
PATH=/opt/someApp/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
. /apps/Callidus/aias/integrator/aias_setenv_variable.sh
. /apps/Callidus/aias/integrator/aias_utilityfunctions.sh

filename=$1
filehandlername=$0
#filehandler=`echo $filehandlername | cut -d "/" -f6`
filehandler=`basename $filehandlername`

# outbound file varibales
if [[ $filename == *"_"* ]]; then 
filetype=`echo $filename | cut -d "_" -f2`
else
filetype=$filename
fi
hname=`hostname | tr '[a-z]' '[A-Z]'`
vhname=` echo $hname | cut -d "." -f1 `
vhname=` echo $vhname | cut -d "-" -f3 `

vdate=`date +%Y%m%d`

#Get variables from typemap configuration file
ReadParameters $filetype
if [ $? -ne 0 ]; then
	 echo "wfret=1" > $tempdir/wfreturncode.txt
	 exit 1
fi

#Get the count of records from source file
#CountofRecords $filename
# if [ $? -ne 0 ]; then
	 # echo "wfret=1" > $tempdir/wfreturncode.txt
	 # exit 1
# fi

#filereccount=$filerecords
#filesizeMB=$filesizeKB


#OutputFileName1=`echo $tenantid_uc"_"$filetype"_ERRORFILE_"$vhname"_"$vdate".txt"`
#batchname=`echo $tenantid_uc"_"$filetype"_"$vhname"_"`date +%Y%m%d_%H%M%S`_`date '+%B%y'`.txt`

echo "[CallScript]"
echo "[CallScript]  === [`date +%Y%m%d_%H%M%S`]:: START:: $filehandler ]===" 
echo "[CallScript]"
echo "[CallScript] Current FileHanler path:" `dirname $filehandlername`
echo "[CallScript] Current FileHanler:" $filehandler
echo "[CallScript] $tenantid_uc Supplied trigger file name :" $filename
echo "[CallScript] $tenantid_uc trigger filetype :" $filetype 
#echo "[CallScript] [$filename] received from $tenantid_uc, and it has [ $filerecords ] records."
echo "[CallScript] Outbound Workflow that will be trigger is :" $outboundwfname


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
case "$outboundwfname" in
*"PD"*)
	F_N="PD_PU"
	case "$filetype" in
	*"RERUN"*)
		echo $F_N",TRUE" > $infasrcdir/ENFORCE_RUN.DAT;;
	*"ADHOC-DELTA"*)
		echo $F_N",ADHOC" > $infasrcdir/ENFORCE_RUN.DAT;;
	*)	
		echo $F_N",FALSE" > $infasrcdir/ENFORCE_RUN.DAT;;
	esac
	echo "[CallScript] Enforce flag is updated in ENFORCE_RUN.DAT";;
*"AGY"*)
	F_N="AGY_PU"
	case "$filetype" in
	*"RERUN"*)
		echo $F_N",TRUE" > $infasrcdir/ENFORCE_RUN.DAT;;
	*"ADHOC-DELTA"*)
		echo $F_N",ADHOC" > $infasrcdir/ENFORCE_RUN.DAT;;
	*)	
		echo $F_N",FALSE" > $infasrcdir/ENFORCE_RUN.DAT;;
	esac	
	#echo $F_N",FALSE" > $infasrcdir/ENFORCE_RUN.DAT
	echo "[CallScript] Enforce flag is updated in ENFORCE_RUN.DAT";;
*)
	echo ",FALSE" > $infasrcdir/ENFORCE_RUN.DAT
	echo "[CallScript] Enforce flag is updated in ENFORCE_RUN.DAT";;
esac


###############################################################################################
#Executing Workflow

#ExecuteWorkflow $infa_foldername $outboundwfname $filehandler $filename $stagetablename $filereccount
ExecuteWorkflow $infa_foldername $outboundwfname $filehandler $filename $stagetablename $filerecords
wfretcode=$?
  if [ $wfretcode -ne 0 ]; then
	 echo "wfret=1" > $tempdir/wfreturncode.txt
	 exit 1
  fi
  
	
###############################################################################################
# Move output files to outbound
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
		case "$linefile" in
		"ACCT_ERROR_DATA_")
			echo "[CallScript] Zipping output file $opfl"
			cd $infatgtdir
			errorcnth=`cat $opfl | grep -v ^$ | wc -lwc |awk '{print $1}'`
			errorcnt=`expr $errorcnth - 1`
			emailst=`sed -n '/STR_EMAIL_USER_NAME=/p' $infasrcdir/AIA_OUTBOUND.par | cut -d '=' -f2`
			cat /dev/null > $tempdir/errmail.txt
			echo "Hi Team," >> $tempdir/errmail.txt
			echo "" >> $tempdir/errmail.txt
			echo "There is a mismatch found between Payment and GL file as on todays run. We are investigating on the discripancy and will get back to you on our findings." >> $tempdir/errmail.txt
			echo "" >> $tempdir/errmail.txt
			echo "Total No of rejected records: $errorcnt" >> $tempdir/errmail.txt
			echo "Error output file is available at $badfilesfolder/OUTPUT" >> $tempdir/errmail.txt
			echo "" >> $tempdir/errmail.txt
			echo "Thanks" >> $tempdir/errmail.txt
			echo "Callidus team" >> $tempdir/errmail.txt
			mail -s "Notification: Mismatch between Payment and GL" $emailst < $tempdir/errmail.txt

			zip -r "$opfl".ZIP "$opfl"
			if [ $? -ne 0 ]; then			
				echo "CallScript] Failed to ZIP output file $opfl to outbound folder"
				echo "wfret=1" > $tempdir/wfreturncode.txt
				exit 1
			fi
			echo "[CallScript] Moving Error file Badfiles path $opfl"
			mv "$infatgtdir/$opfl".ZIP $badfilesfolder/OUTPUT
			if [ $? -ne 0 ]; then			
				echo "CallScript] Failed in moving output file $opfl to outbound folder"
				echo "wfret=1" > $tempdir/wfreturncode.txt
				exit 1
			fi
			mv "$infatgtdir/$opfl" $badfilesfolder/OUTPUT
			;;
		*)
		echo "[CallScript] Zipping output file $opfl"
		#obopfl=`echo $opfl | cut -d'.' -f1`"_"$vdate".txt"
		#mv $infatgtdir/$opfl $infatgtdir/$obopfl
		cd $infatgtdir
		case "$outputfils" in
		*"+"*)
			echo "[CallScript] This filetype has dynamic output file. So, change the file name if its empty file"
			fle1=`echo $outputfils | cut -d'+' -f1`
			fle2=`echo $outputfils | cut -d'+' -f2`
			extn=`echo $outputfils | cut -d'+' -f3`
			case "$opfl" in
			*"$fle1"*)
				mv $infatgtdir/$opfl $infatgtdir/$fle2"."$extn
				opfl=$fle2"."$extn
				echo "[CallScript] Output file is $opfl";;
			esac
		;;
		esac
		if [[ $opfl == *[\(\)]* ]]; then
		opflwobr=`echo $opfl | sed 's/[()]//g'`
		mv $infatgtdir/"$opfl" $infatgtdir/$opflwobr
		opfl=$opflwobr
		fi
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
		cp "$infatgtdir/$opfl" $archivefolder/OUTPUT
		cp "$infatgtdir/$opfl".GZIP.PGP $lndoutboundfolder
		if [ $? -ne 0 ]; then			
			echo "CallScript] Failed in moving output file $opfl to outbound folder"
			echo "wfret=1" > $tempdir/wfreturncode.txt
			exit 1
		fi
		echo "[CallScript] Output file $opfl.GZIP.PGP is moved to Outboud folder successfully"
		mv "$infatgtdir/$opfl" $archivefolder/OUTPUT
		mv "$infatgtdir/$opfl".GZIP.PGP $archivefolder/OUTPUT
		rm -f "$infatgtdir/$opfl".GZIP
		;;
		esac
	fi		
	done < $tempdir/op_list.txt
fi

echo "[CallScript]"
echo "[CallScript]  === [`date +%Y%m%d_%H%M%S`]::END:: $filehandler ] ==="
exit $wfretcode

##################################################################################################