#!/bin/bash
#-------------------------------------------------------------
# Date               Author                  Version
#-------------------------------------------------------------
# 04/04/2017        Suresh Musham        		 1.0	
# 08/15/2021        Endi                         2.0
# 03/29/2022        Dimas                        3.0
# 
#
# Description : 
# Invoked every 2 minutes to process the files from inbound directory
#
# Command line: aias_autoloader.sh <arguments as required by definition>
#################################################################################################################

PATH=/opt/someApp/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
. /home/callidus/.bash_profile

# Import Environment Variables.
. /apps/Callidus/aias/integrator/aias_setenv_variable.sh
. /apps/Callidus/aias/integrator/aias_utilityfunctions.sh


#FunctionCall: to check if there any files in inbound and if the file is a new file or currently running file
Inboundfilecheck
if [ "$current_filetype" = "" ]; then
	echo "[Autoloader] There are no files in inbound folder"
	exit
fi

fname=`find $inboundfolder -name "*$current_filetype*" -type f -exec basename \{} \; | sort -n | head -1 | tail -1`
if [ "$fname" = "" ]; then
	echo "There are no files in inbound to run, so exiting..."
	exit
fi

filetype=`echo $fname | cut -d "_" -f2`;
region=`echo $fname | cut -d "_" -f3`;
wrapper_entry "$filetype" 'filename' 0

#autoloaderlog=$logfolder/Autoloader_log.txt
autoloaderlog=$logfolder/Autoloader"_"${yyyymm}.log
log=${tenantid_uc}"_"${filetype}"_"${timestamp}.log
logfile=$logfolder/$log
PGP_temp="PGP_list_$$.txt"

echo "=== [$timestamp::START::"$0" ] ===" | tee -a $autoloaderlog
echo "Current file type that Autoloader checks is $current_filetype $region" | tee -a $autoloaderlog $logfile
#Decrypting the PGP files if any and unzipping them
echo "PGP list created as ${PGP_temp}." | tee -a $autoloaderlog $logfile
cat /dev/null > $tempdir/${PGP_temp}
cd $inboundfolder
#version 3.0
echo "Checking specific file $current_filetype $region." | tee -a $autoloaderlog $logfile
if [ "$region" = "SG" ]; then
	if [ "$current_filetype" = "${filetype}" ]; then
		echo "$region Finished. $current_filetype" = "${filetype}" | tee -a $autoloaderlog $logfile
		ls $inboundfolder | grep "${filetype}" | grep "${region}" | grep ".PGP$" >> $tempdir/${PGP_temp}
	elif [ "$current_filetype" = "MA_" ]; then
		echo "$fname Finished" | tee -a $autoloaderlog $logfile
		ls $inboundfolder | grep "${filetype}" | grep ".PGP$" >> $tempdir/${PGP_temp}
	else
		echo "$current_filetype Finished." | tee -a $autoloaderlog $logfile
		ls $inboundfolder | grep ".PGP$" >> $tempdir/${PGP_temp}
	fi
elif [ "$region" = "BN" ]; then
	if [ "$current_filetype" = "${filetype}" ]; then
		echo "$region Finished. $current_filetype" = "${filetype}" | tee -a $autoloaderlog $logfile
		ls $inboundfolder | grep "${filetype}" | grep "${region}" | grep ".PGP$" >> $tempdir/${PGP_temp}
	elif [ "$current_filetype" = "MA_" ]; then
		echo "$fname Finished" | tee -a $autoloaderlog $logfile
		ls $inboundfolder | grep "${filetype}" | grep ".PGP$" >> $tempdir/${PGP_temp}
	else
		echo "$current_filetype Finished." | tee -a $autoloaderlog $logfile
		ls $inboundfolder | grep ".PGP$" >> $tempdir/${PGP_temp}
	fi
else
	if [ "$current_filetype" = "DATE" ]; then
		echo "Finished 3" | tee -a $autoloaderlog $logfile
		ls $inboundfolder | grep "${current_filetype}" | grep ".PGP$" >> $tempdir/${PGP_temp}
	else
		echo "Finished 4" | tee -a $autoloaderlog $logfile
		ls $inboundfolder | grep ".PGP$" >> $tempdir/${PGP_temp}
	fi
fi
cat $tempdir/${PGP_temp} | tee -a $autoloaderlog $logfile

while read PGPlst
do
	encyptfl=$PGPlst
	ackfl=`echo $encyptfl | sed "s/\.GZIP\.PGP//"`
	echo "[Autoloader] Found PGP file $encyptfl" | tee -a $autoloaderlog $logfile
	srchstrng=`echo $encyptfl | rev | cut -d'.' -f2- | rev`
	testfl=`find $inboundfolder -name "*$ackfl*" ! -name "*.aud" ! -name "*.PGP" -type f -exec basename \{} \; | sort -n | head -1 | tail -1`
	if [[ "$testfl" != "" ]]; then
	 rm -f $inboundfolder/$testfl
	fi
	gpg -r $privatekeyid -d --output $inboundfolder/$srchstrng $inboundfolder/$encyptfl
	wfretcode=$?
	if [ $wfretcode -ne 0 ]; then
		echo "[Autoloader] Failed to decrypt the file $encyptfl. So exiting..." | tee -a $autoloaderlog $logfile
		mv $inboundfolder/$encyptfl $badfilesfolder/COMMON
		SendMail $logfile "ERROR" $encyptfl "$encyptfl : Failed to decrypt PGP file"
		touch $lndoutboundfolder/$ackfl"_FAIL"
		exit 1
	fi
	if [[ "$srchstrng" == *.GZIP ]]; then
	testfl=`find $inboundfolder -name "*$ackfl*" ! -name "*.aud" ! -name "*.PGP" ! -name "*.GZIP" -type f -exec basename \{} \; | sort -n | head -1 | tail -1`
	if [[ "$testfl" != "" ]]; then
	 rm -f $inboundfolder/$testfl
	fi
	gzsrchstrng=`echo $srchstrng | sed "s/GZIP/gz/"`
	mv $inboundfolder/$srchstrng $inboundfolder/$gzsrchstrng
	gunzip $inboundfolder/$gzsrchstrng
	wfretcode=$?
	if [ $wfretcode -ne 0 ]; then
		echo "[Autoloader] Failed to unzip the file $srchstrng. So exiting..." | tee -a $autoloaderlog $logfile
		mv $inboundfolder/$gzsrchstrng $badfilesfolder/COMMON
		SendMail $logfile "ERROR" $srchstrng "$srchstrng : Failed to unzip the zip file"
		touch $lndoutboundfolder/$ackfl"_FAIL"
		exit 1
	fi
	rm -f $inboundfolder/$gzsrchstrng
	fi
	rm $inboundfolder/$PGPlst
	wfretcode=$?
	if [ $wfretcode -ne 0 ]; then
		echo "[Autoloader] Failed to remove $PGPlst file. So exiting..." | tee -a $autoloaderlog $logfile
		exit
	fi
	echo "[Autoloader] Decrypting $encyptfl completed successfully" | tee -a $autoloaderlog $logfile
done < $tempdir/${PGP_temp}
rm $tempdir/${PGP_temp}
echo "[Autoloader] Deleting $tempdir/${PGP_temp} successfully" | tee -a $autoloaderlog $logfile


fname=`find $inboundfolder -name "*$current_filetype*" ! -name "*.aud" ! -name "*.GZIP" -type f -exec basename \{} \; | sort -n | head -1 | tail -1`
afname=`find $inboundfolder -name "*$fname*" -name "*.aud" ! -name "*.GZIP" -type f -exec basename \{} \; | sort -n | head -1 | tail -1`
if [ "$afname" != "" ]; then
	datafl=`echo $afname | sed 's/.aud//'`
	if [ ! -f $inboundfolder/$datafl ]; then
		echo "Waiting for the data file for the audit file $afname..."
		exit
	fi
fi


case "$fname" in 
*.H.TXT|*B.TXT|*H.txt|*B.txt)
	basefilename=`echo $fname | awk -F\. '{print $1}' | tail -1| head -1`
	bodyfile=$basefilename".B.TXT"
	headfile=$basefilename".H.TXT"
	IBfilecheck $bodyfile | tee -a $autoloaderlog
	IBfilecheck $bodyfile
	retrunbody=$ibfilestatus
	IBfilecheck $headfile | tee -a $autoloaderlog
	IBfilecheck $headfile
	retrunhead=$ibfilestatus
	if [ $retrunbody -eq 1 -o $retrunhead -eq 1 ]; then
		echo "One of the data file is not sent cloud. So, exiting..." | tee -a $autoloaderlog
		exit
	else
		mv $datafile/waiting/$bodyfile $inboundfolder
		mv $datafile/waiting/$headfile $inboundfolder
	fi

	filename=`find $inboundfolder -name "*$basefilename*" ! -name "*.aud" ! -name "*.GZIP" -type f -exec basename \{} \; | grep "\.B\." | sort -n | head -1 | tail -1`
	if [ "$filename" = "" ]; then
		echo "Waiting for body file for the file header file $fname, so exiting..." | tee -a $autoloaderlog
		exit
	fi
	basefilename=$filename	
	;;
*)
	filename=$fname
	basefilename=`echo $filename | awk -F\. '{print $1}' | tail -1| head -1`
	;;
esac


hname=`hostname | tr '[a-z]' '[A-Z]'`
vhname=` echo $hname | cut -d "." -f1 `
vhname=` echo $vhname | cut -d "-" -f3 `


#basefilename=`echo $filename | awk -F\. '{print $1}' | tail -1| head -1`
case "$filename" in
*POLMOVE_SG*)
filetype=POLMOVESG;;
*POLMOVE_BN*)
filetype=POLMOVEBN;;
*)
filetype=`echo $filename | cut -d "_" -f2`;;
esac

echo "=== [$timestamp::START::"$0" ] ===" | tee -a $autoloaderlog $logfile
echo "[Autoloader] Autoloader invoked."   | tee -a $autoloaderlog $logfile
echo "[Autoloader] File name :"$filename  | tee -a $autoloaderlog $logfile
echo "[Autoloader] Didn't detect another datafile being processed - OK to continue..." | tee -a $autoloaderlog $logfile
echo "[Autoloader] Adding entry in table Autoloader Stats table to Lock on $filetype" | tee -a $autoloaderlog $logfile

#FunctionCall: To check if autolaoder is duplicated
wrapperdups
echo "test1" | tee -a $autoloaderlog $logfile
if [ $wrapstatus != 0 ] ; then
	echo "[Autoloader] Autoloader duplicated for the same file, so exiting..."
	exit
fi

#echo "test1" | tee -a $autoloaderlog $logfile
#FunctionCall: To check if commissioninbound is running
#echo "test2" | tee -a $autoloaderlog $logfile
#commissioninbound
#echo "test2" | tee -a $autoloaderlog $logfile
#if [ $commstatus!= 0 ] ; then
#	echo "[Inbound] Commission Inbound is running, so exiting..."
#	exit
#fi
#FunctionCall: To add entry in Autoloader_stats table
#<Parameters>: Pass current filetype, actual filename, process status
Autoloader_entry "$filetype" "$filename" 0 | tee -a $autoloaderlog $logfile

#Special case to handle CYCLE_DATE file
if [ "$filename" = "CYCLE_DATE.DAT" ]; then
	echo "[Autoloader] check for the audit file"
	filecheck "CYCLE_DATE.DAT.aud"
	if [ $filecheckstatus -ne 0 ]; then
		echo "[Autoloader] audit file CYCLE_DATE.DAT.aud is not avaiable. So exiting"
		Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
		CleanInboundfolder 1 $basefilename | tee -a $autoloaderlog $logfile
		touch $lndoutboundfolder/$ackfl"_FAIL"
		SendMail $logfile "ERROR" "$filename" "Audit file CYCLE_DATE.DAT.aud is missing"
		exit
	fi
	auditfl=`ls $inboundfolder | grep "CYCLE_DATE.DAT.aud"`
	if [ "$auditfl" = "" ]; then
	bodyfl=$filename
	else
	bodyfl=`echo $auditfl | sed "s/\.aud//"`
	CheckCksum $auditfl $bodyfl
	sizematch=$?
	if [ $sizematch != 0 ] ; then
		echo "[Autoloader] Data file size does not match with Audit file, exiting." | tee -a $autoloaderlog $logfile
		rm -f $inboundfolder/$auditfl
		#FunctionCall: To update entry in Autoloader_stats table
		#<Parameters>: Pass current filetype, actual filename, process status
		Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
		CleanInboundfolder $sizematch $basefilename | tee -a $autoloaderlog $logfile
		touch $lndoutboundfolder/$ackfl"_FAIL"
		SendMail $logfile "ERROR" "$filename" "Data file size does not match with Audit file received"
		exit
	fi
	echo "[Autoloader] Audit file check completed" | tee -a $autoloaderlog $logfile
	rm -f $inboundfolder/$auditfl
	fi
	echo "[Autoloader] This is Cycle date file, so simply moving this file to Informatica source path" | tee -a $autoloaderlog $logfile
	mv $inboundfolder/$filename $infasrcdir
	wfretcode=$?
	if [ $wfretcode -ne 0 ]; then
		echo "[Autoloader] Failed moving Cycle data file to informatica source directory. So exiting..." | tee -a $autoloaderlog $logfile
		#FunctionCall: To update entry in Autoloader_stats table
		#<Parameters>: Pass current filetype, actual filename, process status
		Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
		touch $lndoutboundfolder/$filename"_FAIL"
		mv $inboundfolder/$filename $badfilesfolder/COMMON
		SendMail $logfile "ERROR" $filename "$filename : Failed to process Cycledate file"
		exit 1
	fi
#added by suresh to handle line breaks 04022018	
	tr -d '\r' < $infasrcdir/CYCLE_DATE.DAT > $tempdir/cycle_tmp
	cat $tempdir/cycle_tmp > $infasrcdir/CYCLE_DATE.DAT
#end by suresh to handle line breaks  04022018
#added by Suresh 20180314 to change oper cycle date
	cycldtformat=`cat $infasrcdir/CYCLE_DATE.DAT | head -1`
	yy=`echo $cycldtformat | cut -d'/' -f3 | cut -c1-4`
	dd=`echo $cycldtformat | cut -d'/' -f2`
	mm=`echo $cycldtformat | cut -d'/' -f1`
	YMD=$yy"-"$mm"-"$dd
	updatecontroltb "$YMD"
	echo "[Autoloader] Updated oper cycle date in control table to $YMD" | tee -a $autoloaderlog $logfile
#ended by Suresh 20180314
	echo "[Autoloader] Successfully moved the Cycle date file to informatica source directory." | tee -a $autoloaderlog $logfile
	touch $lndoutboundfolder/$filename"_SUCCESS"
	#FunctionCall: To update entry in Autoloader_stats table
	#<Parameters>: Pass current filetype, actual filename, process status
	Autoloader_entry "$filetype" "$filename" 3 | tee -a $autoloaderlog $logfile
	chmod 777 $infasrcdir/$filename
	echo "===[$timestamp::END::"$0" ]===" | tee -a $autoloaderlog $logfile
	SendMail $logfile "SUCCESS" $filename "$filename"
	rm -f $workdir/lock*
	exit
fi


#FunctionCall: To get filename parameters like filedate
filenameProperties $filename 
#| tee -a $autoloaderlog $logfile
fldt=$filedate

#Pre-checking the Typemap file, to make sure Valid file received
filetype_config=`cat $tntscriptsdir/$typemap|grep "^$filetype|"|cut -d "|" -f1`
if [ "$filetype_config" != "$filetype" ]; then
	echo "[CheckTypemap] Processing file: $filename"  | tee -a $autoloaderlog $logfile
	echo "[CheckTypemap] Invalid file, please check file naming convention and reload file." | tee -a $autoloaderlog $logfile
	#FunctionCall: To update entry in Autoloader_stats table
	#<Parameters>: Pass current filetype, actual filename, process status
	Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
	CleanInboundfolder 1 $basefilename | tee -a $autoloaderlog $logfile
	SendMail $logfile "ERROR" $filename "Invalid Filename: $filename "
	touch $lndoutboundfolder/$filename"_FAIL"
	exit
fi

#FunctionCall: Read Typemap file and get required parameters
ReadParameters $filetype 

if [ "$stagetablename" = "NA" ]; then
	ackfl=$filename
else
	ackfl=$tenantid_uc"_"$stagetablename"_"$vhname"_"$fldt".txt"
fi

if [ "$filename" = "" ]; then
	echo "[CheckDataFile] Oops! $inboundfolder folder is empty, exiting Autoloader..."
	#FunctionCall: To update entry in Autoloader_stats table
	#<Parameters>: Pass current filetype, actual filename, process status
	Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
	touch $lndoutboundfolder/$ackfl"_FAIL"
	exit
fi

#FunctionCall: to check file growth
CheckFileGrowth $filename | tee -a $autoloaderlog $logfile
inputfilestatus=$?
if [ $inputfilestatus != 0 ] ; then
	echo "[Autoloader] File is not Stable, still loading..." | tee -a $autoloaderlog $logfile
	#FunctionCall: To update entry in Autoloader_stats table
	#<Parameters>: Pass current filetype, actual filename, process status
	Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
	#touch $lndoutboundfolder/$ackfl"_FAIL"	 
	exit
fi

#FunctionCall: check for any dependencies
DependencyChecker $filename | tee -a $autoloaderlog $logfile
Depreturn=`cat $tempdir/dependency_code.txt|cut -d "=" -f2`
    if [ $Depreturn = 0 ] ; then
         echo "[Autoloader] ($Depreturn) : All dependent file for $filetype were processed" | tee -a $autoloaderlog $logfile
		 rm -f $tempdir/dependency*
    elif [ $Depreturn = 2 ] ; then
		  echo "[Autoloader] ($Depreturn) : $filetype : Don't have any dependencies" | tee -a $autoloaderlog $logfile
		  rm -f $tempdir/dependency*
	else
          echo "[Autoloader] ($Depreturn) : Dependent files for $filetype yet to be processed,so exiting" | tee -a $autoloaderlog $logfile
		  #FunctionCall: To update entry in Autoloader_stats table
		  #<Parameters>: Pass current filetype, actual filename, process status
		  Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
		  CleanInboundfolder $Depreturn $basefilename | tee -a $autoloaderlog $logfile
		  SendMail $logfile "ERROR" $filename "Dependent files for $filetype yet to be processed,so exiting"
		  touch $lndoutboundfolder/$ackfl"_FAIL"
		  exit
    fi

#The main processing of the Autoloader, starts from here.	
autoloader () 
{
#Add TFTX by sammi
#Add CBPOLPYR,CBSPCMP,CBCFPOL by Simon
case "$filetype" in
OGPT|OGPR|CMTX|PMTX|RTTX|EXCH|APD|PAQPB|OGPO|APD|POLMOVESG|POLMOVEBN|RPILR|RPILP|TFTX|CBPOLPYR|CBSPCMP|CBCFPOL)
	inboundfilename1=`find $inboundfolder -name "*$filename*" ! -name "*.aud" ! -name "*.GZIP" -type f -exec basename \{} \;|  grep "\.B\." | sort -n | head -1 |tail -1`
	if [ "$inboundfilename1" = "" ]; then
		echo "[CheckDataFile] No files found in inbound folder, exiting..."
		badfl=`find $inboundfolder -name "*$filename*" -type f -exec basename \{} \;`
		CleanInboundfolder 1 $badfl | tee -a $autoloaderlog $logfile
		#FunctionCall: To update entry in Autoloader_stats table
		#<Parameters>: Pass current filetype, actual filename, process status
		Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
		SendMail $logfile "ERROR" $filename "No $filetype files found in Inbound folder"
		touch $lndoutboundfolder/$ackfl"_FAIL"
		exit
	fi
	basefilename=$filename
	bodyfl=$filename
	headfl=`echo $bodyfl | sed "s/\.B\./\.H\./"`
	auditbodyfl="$bodyfl".aud
	auditheadfl="$headfl".aud
	#auditbodyfl=`ls -t $inboundfolder | grep "$bodyfl".aud | head -n1`
	#auditheadfl=`ls -t $inboundfolder | grep "$headfl".aud | head -n1`

	echo "[Autoloader] check for the audit file" | tee -a $autoloaderlog $logfile
	filecheck $auditbodyfl
	echo "[Autoloader] Audit file check status is $filecheckstatus" | tee -a $autoloaderlog $logfile
	if [ $filecheckstatus -ne 0 ]; then
		echo "[Autoloader] audit file $auditbodyfl is not avaiable. So exiting" | tee -a $autoloaderlog $logfile
		Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
		CleanInboundfolder 1 $basefilename | tee -a $autoloaderlog $logfile
		touch $lndoutboundfolder/$ackfl"_FAIL"
		SendMail $logfile "ERROR" "$filename" "Audit file $auditbodyfl is missing"
		exit
	fi
	filecheck $auditheadfl
	echo "[Autoloader] Audit file check status is $filecheckstatus" | tee -a $autoloaderlog $logfile
	if [ $filecheckstatus -ne 0 ]; then
		echo "[Autoloader] audit file $auditheadfl is not avaiable. So exiting" | tee -a $autoloaderlog $logfile
		Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
		CleanInboundfolder 1 $basefilename | tee -a $autoloaderlog $logfile
		touch $lndoutboundfolder/$ackfl"_FAIL"
		SendMail $logfile "ERROR" "$filename" "Audit file $auditheadfl is missing"
		exit
	fi
	
	if [ -f $inboundfolder/$auditbodyfl ]; then
		#FunctionCall: to check file growth
		sleep 3
		CheckFileGrowth $auditbodyfl | tee -a $autoloaderlog $logfile
		inputfilestatus=$?
		if [ $inputfilestatus != 0 ] ; then
			echo "[Autoloader] File is not Stable, still loading..." | tee -a $autoloaderlog $logfile
			Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
			exit
		fi
		echo "[CheckDataFile] Audit File Found, and is of '$filetype' type." | tee -a $autoloaderlog $logfile
		echo "[CheckDataFile] Audit File: $auditbodyfl" | tee -a $autoloaderlog $logfile
		CheckCksum $auditbodyfl $bodyfl
		sizematch=$?
		if [ $sizematch != 0 ] ; then
			echo "[Autoloader] Data file size does not match with Audit file, exiting." | tee -a $autoloaderlog $logfile
			rm -f $inboundfolder/$auditbodyfl
			#FunctionCall: To update entry in Autoloader_stats table
			#<Parameters>: Pass current filetype, actual filename, process status
			Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
			CleanInboundfolder $sizematch $basefilename | tee -a $autoloaderlog $logfile
			SendMail $logfile "ERROR" "$filename" "Data file size does not match with Audit file received"
			touch $lndoutboundfolder/$ackfl"_FAIL"
			exit
		fi		
	fi
	rm -f $inboundfolder/$auditbodyfl $inboundfolder/$auditheadfl
	;;
#Add third party(TPTX) file type by sammi 20220421
#MA)
MA|TPTX)
	inboundfilename1=`find $inboundfolder -name "*$filename*" ! -name "*.aud" ! -name "*.GZIP" -type f -exec basename \{} \;| sort -n | head -1 |tail -1`
	if [ "$inboundfilename1" = "" ]; then
		echo "[CheckDataFile] No files found in inbound folder, exiting..."
		badfl=`find $inboundfolder -name "*$filename*" -type f -exec basename \{} \;`
		#FunctionCall: To update entry in Autoloader_stats table
		#<Parameters>: Pass current filetype, actual filename, process status
		Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
		CleanInboundfolder 1 $badfl | tee -a $autoloaderlog $logfile
		SendMail $logfile "ERROR" $filename "No $filetype files found in Inbound folder"
		touch $lndoutboundfolder/$ackfl"_FAIL"
		exit
	fi
	basefilename=$filename
	auditfl="$basefilename".aud
	
	echo "[Autoloader] check for the audit file" | tee -a $autoloaderlog $logfile
	filecheck $auditfl
	echo "[Autoloader] Audit file check status is $filecheckstatus" | tee -a $autoloaderlog $logfile
	if [ $filecheckstatus -ne 0 ]; then
		echo "[Autoloader] audit file $auditfl is not avaiable. So exiting" | tee -a $autoloaderlog $logfile
		Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
		CleanInboundfolder 1 $basefilename | tee -a $autoloaderlog $logfile
		touch $lndoutboundfolder/$ackfl"_FAIL"
		SendMail $logfile "ERROR" "$filename" "Audit file $auditfl is missing"
		exit
	fi
	
	if [ -f $inboundfolder/$auditfl ]; then
		#FunctionCall: to check file growth
		sleep 3
		CheckFileGrowth $auditfl | tee -a $autoloaderlog $logfile
		inputfilestatus=$?
		if [ $inputfilestatus != 0 ] ; then
			echo "[Autoloader] File is not Stable, still loading..." | tee -a $autoloaderlog $logfile
			Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
			exit
		fi
		echo "[CheckDataFile] Audit File Found, and is of '$filetype' type." | tee -a $autoloaderlog $logfile
		echo "[CheckDataFile] Audit File: $auditfl" | tee -a $autoloaderlog $logfile
		CheckCksum $auditfl $filename
		sizematch=$?
		if [ $sizematch != 0 ] ; then
			echo "[Autoloader] Data file size does not match with Audit file, exiting." | tee -a $autoloaderlog $logfile
			rm -f $inboundfolder/$auditfl
			#FunctionCall: To update entry in Autoloader_stats table
			#<Parameters>: Pass current filetype, actual filename, process status
			Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
			CleanInboundfolder $sizematch $basefilename | tee -a $autoloaderlog $logfile
			touch $lndoutboundfolder/$ackfl"_FAIL"
			SendMail $logfile "ERROR" "$filename" "Data file size does not match with Audit file received"
			exit
		fi		
	fi
	rm -f $inboundfolder/$auditfl
	;;
*)
	inboundfilename1=$srcfiles
	if [ "$inboundfilename1" = "NA" ]; then
		echo "[CheckDatatype] No source files required for this filetype $filetype"
		inboundfilename1=$filename
	else
		inboundfilename1=`echo $srcfiles | cut -d"," -f1`
		cat /dev/null > $tempdir/srcfl_list.txt
		echo $srcfiles | tr , "\n" >> $tempdir/srcfl_list.txt
		while read srccheck
		do
#Modified by suresh 04022018 to handle multiple source files
			dtfile=`ls $inboundfolder | grep "$srccheck" | grep -v ".aud" | head -1`
			if [ "$dtfile" = "" ]; then
				echo "[Autoloader] audit file of $srccheck type is missing. So exiting" | tee -a $autoloaderlog $logfile
				Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile 
				exit
			fi
			audfile=`ls $inboundfolder | grep "$srccheck" | grep ".aud" | head -1`
			if [ "$audfile" = "" ]; then
				echo "[Autoloader] Source file of $srccheck type is missing. So exiting" | tee -a $autoloaderlog $logfile
				Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile 
				exit
			fi
#end by suresh 04022018 to handle multiple source files
		done < $tempdir/srcfl_list.txt	

		while read audline
		do
			audfile=`ls $inboundfolder | grep "$audline" | grep -v ".aud" | head -1`
			#auditfl=`ls $inboundfolder | grep "$audline" | grep ".aud"`
			auditfl=$audfile".aud"

			echo "[Autoloader] check for the audit file" | tee -a $autoloaderlog $logfile
			filecheck $auditfl
			echo "[Autoloader] Audit file check status is $filecheckstatus" | tee -a $autoloaderlog $logfile
			if [ $filecheckstatus -ne 0 ]; then
				echo "[Autoloader] audit file $auditfl is not avaiable. So exiting" | tee -a $autoloaderlog $logfile
				Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile 
				CleanInboundfolder 1 $basefilename | tee -a $autoloaderlog $logfile
				touch $lndoutboundfolder/$ackfl"_FAIL"
				SendMail $logfile "ERROR" "$filename" "Audit file $auditfl is missing"
				exit
			fi		
			
			bodyfl=`echo $auditfl | sed "s/\.aud//"`
			a=0
			while [ $a -lt 10 ]
			do				
				bfl=`ls $inboundfolder | grep "$audline" | grep -v ".aud"`
				if [[ "$bfl" != "" ]]; then
					echo "$bfl is available in inbound path" | tee -a $autoloaderlog $logfile
				case "$bfl" in
				*PGP*|*GZIP*)
					echo "second source file is not decrypted. So, exiting"
					Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
					exit;;
				esac
					break			
				fi
				echo "Waiting for Body file with file type $audline" | tee -a $autoloaderlog $logfile
				sleep 30
				a=`expr $a + 1`				
			done	
			if [ -f $inboundfolder/$auditfl ]; then
			#FunctionCall: to check file growth
			sleep 3
			CheckFileGrowth $auditfl | tee -a $autoloaderlog $logfile
			inputfilestatus=$?
			if [ $inputfilestatus != 0 ] ; then
				echo "[Autoloader] File is not Stable, still loading..." | tee -a $autoloaderlog $logfile
				Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
				exit
			fi
			echo "[CheckDataFile] Audit File Found for the source file $bodyfl." | tee -a $autoloaderlog $logfile
			echo "[CheckDataFile] Audit File: $auditfl" | tee -a $autoloaderlog $logfile
			CheckCksum $auditfl $bodyfl
			sizematch=$?
			if [ $sizematch != 0 ] ; then
				echo "[Autoloader] Data file size does not match with Audit file, exiting." | tee -a $autoloaderlog $logfile
				rm -f $inboundfolder/$auditfl
				#FunctionCall: To update entry in Autoloader_stats table
				#<Parameters>: Pass current filetype, actual filename, process status
				Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
				CleanInboundfolder $sizematch $basefilename | tee -a $autoloaderlog $logfile
				touch $lndoutboundfolder/$ackfl"_FAIL"
				SendMail $logfile "ERROR" "$filename" "Data file size does not match with Audit file received"
				exit
			fi		
			fi
			rm -f $inboundfolder/$auditfl
			#v2.0 to replace all non printable char except TAB from inbound data file
                        echo "[CheckDataFile] Replacing non printable except TAB for: $bodyfl" | tee -a $autoloaderlog $logfile
                        replaceNonPrintable $bodyfl

		done < $tempdir/srcfl_list.txt
	fi
	basefilename=`echo $inboundfilename1 | awk -F\. '{print $1}' | tail -1 | head -1`	
	;;
esac


#basefilename=`echo $inboundfilename1 | awk -F\. '{print $1}' | tail -1 | head -1`	
  
#Process File according to file extension. 
#case "$inboundfilename1" in  
#*.txt|*.TXT|*.csv|*.CSV)   
         echo "[CheckDataFile] Text File Found" | tee -a $autoloaderlog $logfile
		 inboundfilename=$inboundfilename1
	     echo "[CheckDataFile] Found Data File: $inboundfilename" | tee -a $autoloaderlog $logfile
#		 ;;
#*)
#esac

if [ "$inboundfilename" = "" ]; then
    echo "[CallScript] No data file found in inbound folder, exiting..." | tee -a $autoloaderlog $logfile
	#FunctionCall: To update entry in Autoloader_stats table
	#<Parameters>: Pass current filetype, actual filename, process status
	Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
	SendMail $logfile "ERROR" "$filename" "$filetype : Data file not found"
	CleanInboundfolder 1 $basefilename | tee -a $autoloaderlog $logfile
	touch $lndoutboundfolder/$ackfl"_FAIL"
    exit
fi

#Send an alert mail, file received
SendMail $logfile "ALERT" "$filename" "$inboundfilename Received"

echo "[CheckDataFile] $timestamp - Started processing file: [$inboundfilename]" | tee -a $autoloaderlog $logfile
echo "[CheckDataFile] Processing: $inboundfilename" | tee -a $autoloaderlog $logfile

#chmod 777 $inboundfolder/$inboundfilename $inboundfolder/$inboundfilename1 
echo "[CheckDataType] File Type = [$filetype] Action = [$executescript] Dependencies = [none]" | tee -a $autoloaderlog $logfile
echo "[CheckDataType] "
echo "[CheckDataType] filename      = $inboundfilename"
echo "[CheckDataType] filetype      = $filetype"
echo "[CheckDataType] Executescript = $executescript"
echo "[CheckDataType] "

if [ "$executescript" = "" ]; then
     echo "[CheckDataType] Script not found for filetype=$filetype, exiting..." | tee -a $autoloaderlog $logfile
	 CleanInboundfolder 1 $basefilename | tee -a $autoloaderlog $logfile
	 (echo -e "ERROR: Cannot find Execute script for the file type[ $filetype ] in config file.\nDatafile = $inboundfilename" ) | mailx -s "LND-ERROR: $tenantid_uc [$custinst] --> Cannot find Execute script for the file type[] in config file" `cat $tntscriptsdir/$email `
	#FunctionCall: To update entry in Autoloader_stats table
	#<Parameters>: Pass current filetype, actual filename, process status
	Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
	 touch $lndoutboundfolder/$ackfl"_FAIL"
     exit
else
     echo "[CheckDataType] Script Identified: ($tntscriptsdir/$executescript)" | tee -a $autoloaderlog $logfile
 	 cd $tntscriptsdir
	 echo "[Autoloader] Executing script: $executescript"
	 echo "[Autoloader] Executing: $executescript for $inboundfilename" | tee -a $autoloaderlog $logfile
	 
	 #Executing identified script;
	 chmod 777 $tntscriptsdir/$executescript
	 if [ "$Sequencejobs" != "none" ]; then	 
		cat /dev/null > $tempdir/seq_list.txt
		echo $Sequencejobs | tr , "\n" >> $tempdir/seq_list.txt
	    while read linejob
	    do
			ReadParameters $linejob
			$tntscriptsdir/$executescript $linejob | tee -a $autoloaderlog $logfile
			retcode=$?
			#Capture Script execution and Workflow return code
			if [ -f $tempdir/wfreturncode.txt ]; then
				return=` cat $tempdir/wfreturncode.txt|cut -d "=" -f2 `
			else 
#added by suresh to handle lock issue wfreturncode.txt file 20180521
				sleep 2
				if [ -f $tempdir/wfreturncode.txt ]; then
					return=` cat $tempdir/wfreturncode.txt|cut -d "=" -f2 `
				else
					return=0
				fi
#end by Suresh 20180521
			fi
			if [ "$retcode" = 0 -a "$return" = 0 ] ; then
				touch $archivefolder/COMMON/$tenantid_uc"_"$linejob"_"$vhname"_"$fldt".txt"
				case "$linejob" in
				*POLMOV*)
					interackfl=$filename
					touch $lndoutboundfolder/$filename"_SUCCESS";;
				*)
					interackfl=$linejob
					touch $lndoutboundfolder/$tenantid_uc"_"$interackfl"_"$vhname"_"$fldt".TXT_SUCCESS";;
				esac
			else
				case "$linejob" in
				*POLMOV*)
					interackfl=$filename
					touch $lndoutboundfolder/$filename"_FAIL";;
				*)
					interackfl=$linejob
					touch $lndoutboundfolder/$tenantid_uc"_"$interackfl"_"$vhname"_"$fldt".TXT_FAIL";;
				esac
				touch $badfilesfolder/COMMON/$tenantid_uc"_"$linejob"_"$vhname"_"$fldt".txt"
				break					
			fi
        done < $tempdir/seq_list.txt
	 else		
		$tntscriptsdir/$executescript $inboundfilename | tee -a $autoloaderlog $logfile
		retcode=$?
		#Capture Script execution and Workflow return code
		if [ -f $tempdir/wfreturncode.txt ]; then
			 return=` cat $tempdir/wfreturncode.txt|cut -d "=" -f2 `
		else 
#added by suresh to handle lock issue wfreturncode.txt file 20180521
			sleep 2
			if [ -f $tempdir/wfreturncode.txt ]; then
				return=` cat $tempdir/wfreturncode.txt|cut -d "=" -f2 `
			else
				return=0
			fi
		fi
#end by Suresh 20180521
	 fi
	 #Getting details of ODI files generated by Workflow
	 ODIfiledetails $filename
	 echo "[Autoloader] " | tee -a $autoloaderlog $logfile
	 echo "[Autoloader] Execute Command return code [$retcode]" | tee -a $autoloaderlog $logfile
     echo "[Autoloader] Script execution return code [$return]" | tee -a $autoloaderlog $logfile
     echo "[Autoloader] " | tee -a $autoloaderlog $logfile
		if [ "$retcode" = 0 -a "$return" = 0 ] ; then 
			 echo "[Autoloader] $return - Script '$executescript' execution completed succefully." | tee -a $autoloaderlog $logfile
			 echo "[Autoloader] Archiving the files after successful execution." | tee -a $autoloaderlog $logfile
			 CleanInboundfolder $return $basefilename | tee -a $autoloaderlog $logfile
			 #ErrorCount $inboundfilename
			 echo "[Autoloader] Autoloader Process SUCCESS : [$inboundfilename]" | tee -a $autoloaderlog $logfile
			 #FunctionCall: To update entry in Autoloader_stats table
			 #<Parameters>: Pass current filetype, actual filename, process status
			 Autoloader_entry "$filetype" "$filename" 3 | tee -a $autoloaderlog $logfile
			 echo "[SuccessMail] Sending Autoloader SUCCESS mail, with log." | tee -a $autoloaderlog $logfile
			 SendMail $logfile "SUCCESS" "$filename" "$inboundfilename"
			 if [ "$Sequencejobs" == "none" ]; then
			 	acfl=$ackfl"_SUCCESS"
			 	echo "$acfl is created" | tee -a $autoloaderlog $logfile
			 	touch $lndoutboundfolder/$ackfl"_SUCCESS"
			 	if [ $? -ne 0 ]; then
					echo "$acfl is not sent to outbound" | tee -a $autoloaderlog $logfile
				fi
			 	echo "$acfl is sent to outbound" | tee -a $autoloaderlog $logfile								 
			 fi
		else
			 echo "[Autoloader] $return - Error in executing script '$executescript', check Log Files for more information." | tee -a $autoloaderlog $logfile
			 echo "[Autoloader] Execution failed, Moving files to badfiles folder..." | tee -a $autoloaderlog $logfile
			 CleanInboundfolder $return $basefilename | tee -a $autoloaderlog $logfile
			 echo "[Autoloader] Autoloader Process FAILED : [$inboundfilename]" | tee -a $autoloaderlog $logfile
			 #FunctionCall: To update entry in Autoloader_stats table
			 #<Parameters>: Pass current filetype, actual filename, process status
			 Autoloader_entry "$filetype" "$filename" 1 | tee -a $autoloaderlog $logfile
			 echo "[FailureMail] Sending Autoloader ERROR mail, with log." | tee -a $autoloaderlog $logfile
			 SendMail $logfile "ERROR" "$filename" "$inboundfilename"
			 if [ "$Sequencejobs" == "none" ]; then
			 touch $lndoutboundfolder/$ackfl"_FAIL"
			 fi
			 exit $return
		fi
   fi
 echo "===[$timestamp::END::"$0" ]===" | tee -a $autoloaderlog $logfile
 exit $return
}   


#Call Autoloader function
autoloader
