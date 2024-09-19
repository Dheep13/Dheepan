import requests
import os
import zipfile
from requests.auth import HTTPBasicAuth
from config import username, password

url = "https://1951.callidusondemand.com/TrueComp-SaaS/services/rest/touchlessdeployment/status"
headers = {
    "Content-Type": "application/json",
}

try:
    response = requests.get(url, headers=headers, auth=HTTPBasicAuth(username, password))
    if response.status_code == 200:
        print("GET request successful!")
        print("Response Content:")
        print(response.text)
    else:
        print(f"GET request failed with status code: {response.status_code}")
except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")

deployment_url = "https://1951.callidusondemand.com/TrueComp-SaaS/services/rest/touchlessdeployment/update?changeMasterFile=master.xml"
changelog_zip_path = "1951_test.zip"
xml_file_name = "master.xml"

with zipfile.ZipFile(changelog_zip_path, 'r') as zip_ref:
    if xml_file_name in zip_ref.namelist():
        print(f"The file {xml_file_name} exists in the ZIP archive.")
    else:
        print(f"The file {xml_file_name} does not exist in the ZIP archive.")

# Create a dictionary for the parameters to be sent in the POST request
data = {
    "changeLogZip": (os.path.basename(changelog_zip_path), open(changelog_zip_path, 'rb'),'application/zip')
}
# print(data)
try:
    response = requests.post(
        deployment_url,
        auth=HTTPBasicAuth(username, password),
        files=data
    )
    if response.status_code == 202:
        print("Status 202 : Deployment request accepted. Schema changes will be scheduled.")
    elif response.status_code == 406:
        print("Status 406 : Deployment request failed. Changelog Zip is not valid or changeMasterFile doesn't exist.")
    else:
        print(f"Deployment request failed with status code: {response.status_code}")
except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")