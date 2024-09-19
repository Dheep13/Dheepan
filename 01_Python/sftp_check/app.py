import paramiko

def check_file_on_sftp_with_key(hostname, port, username, private_key_path, sftp_directory, file_to_check):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key (assuming RSA; adjust if using a different type)
        pkey = paramiko.RSAKey.from_private_key_file(private_key_path)

        client.connect(hostname, port=port, username=username, pkey=pkey)
        sftp = client.open_sftp()
        sftp.chdir(sftp_directory)
        files = sftp.listdir()

        if file_to_check in files:
            print(f"File '{file_to_check}' exists in the directory '{sftp_directory}'.")
        else:
            print(f"File '{file_to_check}' does NOT exist in the directory '{sftp_directory}'.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        sftp.close()
        client.close()

# SFTP server details
hostname = 'tsla.app.dev.commissions.dev.sap'
port = 22  # Default SFTP port, adjust if needed
username = 'tnta_570394907'
private_key_path = 'C:\\Users\\I520292\\Documents\\HCSC\\idrsa.ppk'  # Update this path to your private key
sftp_directory ='/integration'
file_to_check = '2024-02-26_MRDB_MemberFile.txt'

# Check if the file exists on the SFTP server
check_file_on_sftp_with_key(hostname, port, username, private_key_path, sftp_directory, file_to_check)
