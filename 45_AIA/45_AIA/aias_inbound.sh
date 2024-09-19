#!/bin/bash
#<<aias_inbound.sh>>
#-------------------------------------------------------------
# Date               Author                  Version
#-------------------------------------------------------------
# 04/08/2017        Suresh Musham        		 1.0	
# 10/01/2018	    Suresh Musham			2.0
#
# Description : 
# Filehandler script invoked by custom Transaction file
#
# Command line: aias_inbound.sh <arguments as required by definition>
#################################################################################################################
PATH=/opt/someApp/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
. /apps/Callidus/aias/integrator/aias_setenv_variable.sh
. /apps/Callidus/aias/integrator/aias_utilityfunctions.sh

filename=$1
filehandlername=$0
#filehandler=`echo $filehandlername | cut -d "/" -f6`
filehandler=`basename $filehandlername`

# outbound output file varibales
filetype=`echo $filename | cut -d "_" -f2`
hname=`hostname | tr '[a-z]' '[A-Z]'`
vhname=` echo $hname | cut -d "." -f1 `
vhname=` echo $vhname | cut -d "-" -f3 `

vdate=`date +%Y%m%d`
#vts=`date +%Y%m%d.%H%M%S`
curpath=$datafile/PER_BATCHJOB/DATA/ARCH

#Get variables from typemap configuration file
ReadParameters $filetype
if [ $? -ne 0 ]; then
	 echo "wfret=1" > $tempdir/wfreturncode.txt
	 exit 1
fi

if [[ "$filename" = *"PMTX_SG_IL"* ]] || [[ "$filename" = *"PMTX_BN_IL"* ]] || [[ "$filename" = *"PAQPB_SG_IN"* ]]; then
	headfile=`echo $filename | sed "s/\.B\./\.H\./"`
	#dt_curptath=$curpath/$vdate
	#mkdir -p $dt_curptath
	echo "[CallScript] Copying PMTX IL files to $curpath"

	#Added by Suresh to handle weekly PAQPB job SCAL441W 20180110
	case "$filename" in
	*"PAQPB_SG_IN"*)
		echo "[CallScript] As this is PAQPB file, checking if the file is of quarterly file"
		PAQPB_dt=`echo $filename | cut -d '_' -f5 | cut -d '.' -f1`
		echo "[CallScript] PAQPB file date is $PAQPB_dt"
		#nextmthfirst=$(date -d "$PAQPB_dt +1 month" +%Y%m01)
		firstday=$(date -d "`echo $PAQPB_dt`" +%Y%m01)
		nextmth=$(date -d "$firstday +1 month" +%Y%m%d)
		lastdy=$(date -d "$nextmth-1 day" +%Y%m%d)
		mm=${lastdy:4:2}
		if [ ${lastdy} -eq ${PAQPB_dt} ];then
			if [ "$mm" = "03" -o "$mm" = "06" -o "$mm" = "09" -o "$mm" = "12" ]; then
				echo "[CallScript] its the quarterly file, so copying to persistency path $curpath"
				echo "cp $inboundfolder/$filename $inboundfolder/$headfile $curpath"
				cp $inboundfolder/$filename $inboundfolder/$headfile $curpath
				if [ $? -ne 0 ]; then
					echo "wfret=1" > $tempdir/wfreturncode.txt
					exit 1
				fi
			else
				echo "[CallScript] its a weekly file, so moving to persistency path $curpath and exiting..."
				cp $inboundfolder/$filename $inboundfolder/$headfile $archivefolder/COMMON
				mv $inboundfolder/$filename $inboundfolder/$headfile $curpath
				echo "wfret=0" > $tempdir/wfreturncode.txt
				exit 0
			fi
		else
			echo "[CallScript] its a weekly file, so moving to persistency path $curpath and exiting..."
			cp $inboundfolder/$filename $inboundfolder/$headfile $archivefolder/COMMON
			mv $inboundfolder/$filename $inboundfolder/$headfile $curpath
			echo "wfret=0" > $tempdir/wfreturncode.txt
			exit 0
		fi
		
		;;
	*)
#Added by Suresh 20180723
		case "$filename" in
		*RERUN*)
			echo ""
			;;
		*)		
			cp $inboundfolder/$filename $inboundfolder/$headfile $curpath
			if [ $? -ne 0 ]; then
				echo "wfret=1" > $tempdir/wfreturncode.txt
				exit 1
			fi
			;;
		esac
#Ended by Suresh 20180723
		;;
	esac
	#Ended by Suresh to handle weekly PAQPB job SCAL441W 20180110

	cp $inboundfolder/$filename $inboundfolder/$headfile $curpath
		if [ $? -ne 0 ]; then
			echo "wfret=1" > $tempdir/wfreturncode.txt
			exit 1
		fi
	#cp $inboundfolder/$filename $inboundfolder/$headfile $datafile/PER_BATCHJOB/DATA	
fi

case "$filetype" in
#OGPT|OGPR|CMTX|PMTX|RTTX|EXCH|APD|PAQPB|OGPO|APD|RPILP|RPILR)
#Add TFTX by sammi
#Add CBPOLPYR,CBSPCMP,CBCFPOL by Simon
OGPT|OGPR|CMTX|PMTX|RTTX|EXCH|APD|PAQPB|OGPO|APD|RPILP|RPILR|RPILT|TFTX|CBPOLPYR|CBSPCMP|CBCFPOL)
	headerfile=`echo $filename | sed "s/\.B\./\.H\./"`
	echo "[CheckDataFile] Wait for 10 seconds and check for source files"
	sleep 10
	#`find $inboundfolder -name "*" -type f -exec basename \{} \; | grep "\.H\." | sort -n | head -2 | tail -1`
	if [[ -f "$inboundfolder/$filename" && -f "$inboundfolder/$headerfile" ]]; then
		echo "[CheckDataFile] Source files found in inbound folder"
		echo "[MoveDataFile] Moving data files [$filename and $headerfile] to Informatica SrcFiles dir:"
		echo "[MoveDataFile] $infasrcdir"
		cp $inboundfolder/$filename $inboundfolder/$headerfile $infasrcdir
		if [ $? -ne 0 ]; then
			echo "wfret=1" > $tempdir/wfreturncode.txt
			exit 1
		fi
		echo "[MoveDataFile] Looking for [$filename and $headerfile] in Informatica source file dir"
		cd $infasrcdir
		if [[ ! -f $infasrcdir/$filename || ! -f $infasrcdir/$headerfile ]]; then
			 echo "[MoveDataFile] Error - One of the source files not Found in Informatica Source Directory"
			 echo "wfret=1" > $tempdir/wfreturncode.txt
			 exit 1
		fi
		echo "[MoveDataFile] Found moved data files in $infasrcdir"
	else	
		echo "[CheckDataFile] No files found in inbound folder, exiting..."
		echo "wfret=1" > $tempdir/wfreturncode.txt
		exit 1
	fi;;
#Add Third party file type by sammi 20220421
#MA)
MA|TPTX)
	if [ -f "$inboundfolder/$filename" ]; then
		echo "[CheckDataFile] Source file found in inbound folder"
		echo "[MoveDataFile] Moving data file [$filename] to Informatica SrcFiles dir:"
		echo "[MoveDataFile] $infasrcdir"
		cp $inboundfolder/$filename $infasrcdir
		if [ $? -ne 0 ]; then
			echo "wfret=1" > $tempdir/wfreturncode.txt
			exit 1
		fi
		echo "[MoveDataFile] Looking for [$filename] in Informatica source file dir"
		cd $infasrcdir
		if [ ! -f $infasrcdir/$filename ]; then
			 echo "[MoveDataFile] Error - One of the source files not Found in Informatica Source Directory"
			 echo "wfret=1" > $tempdir/wfreturncode.txt
			 exit 1
		fi
		echo "[MoveDataFile] Found moved data file in $infasrcdir"
	else	
		echo "[CheckDataFile] No file found in inbound folder, exiting..."
		echo "wfret=1" > $tempdir/wfreturncode.txt
		exit 1
	fi;;
*)
	echo "[CheckDataFile] this not a valid file, exiting..."
	echo "wfret=1" > $tempdir/wfreturncode.txt
	exit 1;;
esac
	

#Get the count of records from source file
CountofRecords $filename
if [ $? -ne 0 ]; then
	 echo "wfret=1" > $tempdir/wfreturncode.txt
	 exit 1
fi

filereccount=$filerecords
filesizeMB=$filesizeKB


#OutputFileName1=`echo $tenantid_uc"_"$filetype"_ERRORFILE_"$vhname"_"$vdate".txt"`
#batchname=`echo $tenantid_uc"_"$filetype"_"$vhname"_"`date +%Y%m%d_%H%M%S`_`date '+%B%y'`.txt`

echo "[CallScript]"
echo "[CallScript]  === [`date +%Y%m%d_%H%M%S`]:: START:: $filehandler ]===" 
echo "[CallScript]"
echo "[CallScript] Current FileHanler path:" `dirname $filehandlername`
echo "[CallScript] Current FileHanler:" $filehandler
echo "[CallScript] $tenantid_uc Supplied Inbound file name :" $filename
echo "[CallScript] $tenantid_uc INBOUND filetype :" $filetype 
echo "[CallScript] [$filename] received from $tenantid_uc, and it has [ $filerecords ] records."
echo "[CallScript] Stage table Name from $tenantid_uc :" $stagetablename


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
echo "[CallScript] Parameter file $parameterfile available in $infasrcdir."

###############################################################################################
#Enforce Flag handling in all other inbounds
###############################################################################################
case "$filetype" in
OGPT|OGPR|EXCH|PAQPB|OGPO)
	F_N=`echo $filename | cut -d "_" -f2,3,4`
	echo $F_N",FALSE" > $infasrcdir/ENFORCE_RUN.DAT
	echo "[CallScript] Enforce flag is updated in ENFORCE_RUN.DAT";;
MA)
	echo ",FALSE" > $infasrcdir/ENFORCE_RUN.DAT
	echo "[CallScript] Enforce flag is updated in ENFORCE_RUN.DAT"
	echo "[CallScript] Special handling to create $filename_LOAD_IND file in infa source path"
	case "$filename" in
	CAL_MA_FIN_*)
		echo "[CallScript] this is MA_FIN file so, creating indicator file"
		touch $infasrcdir/"MA_LOAD_IND";;
	esac
	;;
esac

###############################################################################################
#Executing Workflow

case "$filetype" in
CMTX|PMTX|APD|RTTX)
	constr=`echo $filename | cut -d'_' -f2,3,4`
	strwf=`echo $inboundwfname| sed 's/_CO//g'`
case "$filename" in
*RERUN*)
	echo $constr",TRUE" > $infasrcdir/"ENFORCE_RUN_"$strwf"_"$constr".DAT"
	echo "[CallScript] Enforce flag is updated in ENFORCE_RUN_"$strwf"_"$constr".DAT";;
*)
	echo $constr",FALSE" > $infasrcdir/"ENFORCE_RUN_"$strwf"_"$constr".DAT"
	echo "[CallScript] Enforce flag is updated in ENFORCE_RUN_"$strwf"_"$constr".DAT";;
esac
	nxtprmfl="AIA_INST_"$constr".par"
	echo "[CheckParamfile] Check for the parameter file $nxtprmfl existence in informatica source dir."
	if [ ! -f "$infasrcdir/$nxtprmfl" ]; then
		echo "[CheckParamfile] No Param file $nxtprmfl available at informatica source path, Exiting..."
		echo "wfret=1" > $tempdir/wfreturncode.txt
		exit 1
	fi
	parameterfile=$infasrcdir/$nxtprmfl
	export parameterfile
	echo "[CallScript] Parameter file $parameterfile available in $infasrcdir."
	echo "[CallScript] Executing workflow $inboundwfname."
	ExecuteWorkflow $infa_foldername $inboundwfname $filehandler $constr $stagetablename $filerecords
	wfretcode=$?
	if [ $wfretcode -ne 0 ]; then
		echo "wfret=1" > $tempdir/wfreturncode.txt
		exit 1
	fi
	;;
#add TFTX by sammi start
TFTX)
	constr=`echo $filename | cut -d'_' -f2,3,4`
	strwf=`echo $inboundwfname| sed 's/_CO//g'`
case "$filename" in
*RERUN*)
	echo $constr",TRUE" > $infasrcdir/"ENFORCE_RUN_"$strwf"_"$constr".DAT"
	echo "[CallScript] Enforce flag is updated in ENFORCE_RUN_"$strwf"_"$constr".DAT";;
*)
	echo $constr",FALSE" > $infasrcdir/"ENFORCE_RUN_"$strwf"_"$constr".DAT"
	echo "[CallScript] Enforce flag is updated in ENFORCE_RUN_"$strwf"_"$constr".DAT";;
esac
	nxtprmfl="AIA_"$constr".par"
	echo "[CheckParamfile] Check for the parameter file $nxtprmfl existence in informatica source dir."
	if [ ! -f "$infasrcdir/$nxtprmfl" ]; then
		echo "[CheckParamfile] No Param file $nxtprmfl available at informatica source path, Exiting..."
		echo "wfret=1" > $tempdir/wfreturncode.txt
		exit 1
	fi
	parameterfile=$infasrcdir/$nxtprmfl
	export parameterfile
	echo "[CallScript] Parameter file $parameterfile available in $infasrcdir."
	echo "[CallScript] Executing workflow $inboundwfname."
	ExecuteWorkflow $infa_foldername $inboundwfname $filehandler $constr $stagetablename $filerecords
	wfretcode=$?
	if [ $wfretcode -ne 0 ]; then
		echo "wfret=1" > $tempdir/wfreturncode.txt
		exit 1
	fi
	;;
#add TFTX by sammi end
#add Third party control by sammi 20220421
TPTX)
	constr=`echo $filename | cut -d'_' -f2,3,4`
	strwf=`echo $inboundwfname| sed 's/_CO//g'`
case "$filename" in
*RERUN*)
	echo $constr",TRUE" > $infasrcdir/"ENFORCE_RUN_"$strwf"_"$constr".DAT"
	echo "[CallScript] Enforce flag is updated in ENFORCE_RUN_"$strwf"_"$constr".DAT";;
*)
	echo $constr",FALSE" > $infasrcdir/"ENFORCE_RUN_"$strwf"_"$constr".DAT"
	echo "[CallScript] Enforce flag is updated in ENFORCE_RUN_"$strwf"_"$constr".DAT";;
esac
	nxtprmfl="AIA_"$constr".par"
	echo "[CheckParamfile] Check for the parameter file $nxtprmfl existence in informatica source dir."
	if [ ! -f "$infasrcdir/$nxtprmfl" ]; then
		echo "[CheckParamfile] No Param file $nxtprmfl available at informatica source path, Exiting..."
		echo "wfret=1" > $tempdir/wfreturncode.txt
		exit 1
	fi
	parameterfile=$infasrcdir/$nxtprmfl
	export parameterfile
	echo "[CallScript] Parameter file $parameterfile available in $infasrcdir."
	echo "[CallScript] Executing workflow $inboundwfname."
	ExecuteWorkflow $infa_foldername $inboundwfname $filehandler $constr $stagetablename $filerecords
	wfretcode=$?
	if [ $wfretcode -ne 0 ]; then
		echo "wfret=1" > $tempdir/wfreturncode.txt
		exit 1
	fi
	;;


#add Third party control end 20220421
*)
	echo "[CallScript] Executing workflow $inboundwfname."
	#ExecuteWorkflow $infa_foldername $inboundwfname $filehandler $filename $stagetablename $filereccount
	ExecuteWorkflow $infa_foldername $inboundwfname $filehandler $filename $stagetablename $filerecords
	wfretcode=$?
	rm -f $infasrcdir/"MA_LOAD_IND" 
	if [ $wfretcode -ne 0 ]; then
		echo "wfret=1" > $tempdir/wfreturncode.txt
		exit 1
	fi
	;;
esac

# case "$filetype" in
# CMTX|PMTX|APD|RTTX)
# constr=`echo $filename | cut -d'_' -f2,3,4`
# nxtprmfl="AIA_INST_"$constr".par"

# echo "[CallScript] Run the concurrent job for the file $filename"
# echo "[CheckParamfile] Check for the parameter file $nxtprmfl existence in informatica source dir."

# if [ ! -f "$infasrcdir/$nxtprmfl" ]; then
	# echo "[CheckParamfile] No Param file $nxtprmfl available at informatica source path, Exiting..."
	# echo "wfret=1" > $tempdir/wfreturncode.txt
	# exit 1
# fi

# parameterfile=$infasrcdir/$nxtprmfl
# inboundwfnameco=$inboundwfname"_CO"

# export parameterfile
# echo "[CallScript] Parameter file $parameterfile available in $infasrcdir."
# echo "[CallScript] Executing workflow $inboundwfnameco."
# ExecuteWorkflow $infa_foldername $inboundwfnameco $filehandler $filename $stagetablename $filerecords
# wfretcode=$?
  # if [ $wfretcode -ne 0 ]; then
	 # echo "wfret=1" > $tempdir/wfreturncode.txt
	 # exit 1
  # fi
# ;;

# *)
	# nxtprmfl="";;
# esac


echo "[CallScript]"
echo "[CallScript]  === [`date +%Y%m%d_%H%M%S`]::END:: $filehandler ] ==="
exit $wfretcode

##################################################################################################