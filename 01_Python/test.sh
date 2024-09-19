echo "${SFTPPrivateKey}" | base64 --decode >> id_rsa
chmod 600 id_rsa

echo "!echo "Successful Connection"" >> CMD
echo "cd ${SFTPInboundDirectory}" >> CMD
echo "!echo "Inbound Directory Exists"" >> CMD
echo "cd ${SFTPTargetDirectory}" >> CMD
echo "!echo "Target Directory Exists"" >> CMD
echo "bye" >> CMD

# Initial setup and variable declarations
processedFileName=""

#This is an initial call - the purpose is to verify if the sftp connection can be established and inbound and target directory really exists
# if not a proper error message will be presented to the end user
sftp -P ${SFTPPort} -i id_rsa -o StrictHostKeyChecking=no -b CMD ${SFTPUsername}@${SFTPHost}>checkOut

connSuccessful=$(cat checkOut|grep -w  "^Successful Connection"|wc -l)

if [ $connSuccessful = 0 ]; then
  echo "ERROR: SFTP Connection failed."
  rm id_rsa
  return
else 
  echo "INFO: SFTP Connection successful."  
fi

inboundDirExists=$(cat checkOut|grep -w  "^Inbound Directory Exists"|wc -l)

if [ $inboundDirExists = 0 ]; then
  echo "ERROR: Directory  ${SFTPInboundDirectory} does not exist."
  rm id_rsa
  return
else
  echo "INFO: SFTP Inbound Directory exists."  
fi

targerDirExists=$(cat checkOut|grep -w  "^Target Directory Exists"|wc -l)

if [ $targerDirExists = 0 ]; then
  echo "ERROR: Child Directory ${SFTPTargetDirectory} does not exist in directory ${SFTPInboundDirectory}."
  rm id_rsa 
  return
else
  echo "INFO: SFTP Target Directory exists."
fi


#This is a second call - it will list all files & directories inside the Inbound directory
#We couldn't filter the matching files only in the SFTP call, the matching files are postprocessed

echo "ls -l" >> listCMD
echo "bye" >> listCMD

sftp -P ${SFTPPort} -i id_rsa -o StrictHostKeyChecking=no -b listCMD ${SFTPUsername}@${SFTPHost}:/${SFTPInboundDirectory}>listOut

#grepping only the files but not the subdirectories
cat listOut |grep '^-' | grep -oE '[^ ]+$' >> fileslist

#grepping only the files which are matching the inputs for prefix and suffix
cat fileslist| grep ^${FileNamePrefix} | grep ${FileNameSuffix}$ >>matchingFiles

matchingFilesFound=$(cat matchingFiles|wc -l)

if [ $matchingFilesFound = 0 ]; then
  echo "INFO: No matching files."
  rm id_rsa 
  return
else 
  echo "INFO: $matchingFilesFound matching files found!"
fi

#This is a third call - we create a renameCMD - which will move files from Inbound directory to target directory
#if the valud of the fileNameAfterMove is same then all files will be transferred with the same names
#if the fileNameAfterMove is a static value then only the first matching file will be moved and renamed ti fileNameAfterMove

if [ ${fileNameAfterMove} = 'same' ]; then
  while read p; do
    echo "rename $p ./${SFTPTargetDirectory}/$p" >> renameCMD
     processedFileName="$processedFileName $p"
  done <matchingFiles
else
  read -r p<matchingFiles
  echo "rename $p ./${SFTPTargetDirectory}/${fileNameAfterMove}" >> renameCMD
  processedFileName=${fileNameAfterMove}
  echo "INFO: will rename the first matching file to ${fileNameAfterMove}" 
  matchingFilesFound=1
fi

sftp -P ${SFTPPort} -i id_rsa -o StrictHostKeyChecking=no -b renameCMD ${SFTPUsername}@${SFTPHost}:/${SFTPInboundDirectory}

echo "Successfully moved $matchingFilesFound file(s) from ${SFTPInboundDirectory} to ${SFTPTargetDirectory} subdirectory. Renamed to: $processedFileName"
echo "$processedFileName"

echo "Modifying files in target directory..."

# Full path to the target directory where the file was moved
echo "$pwd"
fullTargetPath="${SFTPTargetDirectory}"

# Check if the target file exists in the target directory
targetFile="${fullTargetPath}/$(basename "${processedFileName}")"

if [ -f "${targetFile}" ]; then
    # Temporary file to hold the modified content
    tempFile="${targetFile}.tmp"

    # Add the filename as the first column
    awk -v filename="$(basename "${processedFileName}")" 'BEGIN{FS=OFS=","} {print filename, $0}' "${targetFile}" > "${tempFile}"

    # Replace the original file with the modified one
    mv "${tempFile}" "${targetFile}"
    echo "Modified file: ${targetFile}"
else
    echo "ERROR: File ${targetFile} not found in target directory."
fi

echo "File modification process completed."


# Clean up
rm id_rsa