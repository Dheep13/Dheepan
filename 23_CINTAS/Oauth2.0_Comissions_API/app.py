
#######################

# Go to IAS Administration → Applications and Create Commissions OIDC application.


# OpenID Connect (often abbreviated as OIDC) is a modern authentication protocol built on top of the OAuth 2.0 authorization 
# framework. It allows third-party applications to verify the identity of the end-user based on the authentication performed 
# by an authorization server and to obtain basic profile information about the end-user in an interoperable and REST-like manner.


# In essence, while OAuth 2.0 focuses on granting permissions, OpenID Connect focuses on user authentication,
#  providing apps with a reliable way to know who the user is and some basic information about them.


# In essence, the IAS server centralizes and streamlines identity and access management across cloud environments, 
# ensuring that the right users have the right access to the right resources, while also enhancing security and user experience
######################


import requests
from config import CLIENT_ID, CLIENT_SECRET

# OAuth2 Token Service Details
TOKEN_URL = "https://<ias>.accounts.ondemand.com/oauth2/token"  # Replace <ias> with the actual value
CLIENT_ID = "YOUR_CLIENT_ID"  # Replace with your client ID
CLIENT_SECRET = "YOUR_CLIENT_SECRET"  # Replace with your client secret
USERNAME = "YOUR_USERNAME"  # Replace with your username
PASSWORD = "YOUR_PASSWORD"  # Replace with your password
SCOPE = "YOUR_SCOPE"  # Any value as mentioned in the blog

def get_oauth2_token():
    """Fetch OAuth2 Token from IAS using client credentials."""
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {requests.auth._basic_auth_str(CLIENT_ID, CLIENT_SECRET)}"
    }
    data = {
        "grant_type": "client_credentials",
        "scope": SCOPE
    }
    
    response = requests.post(TOKEN_URL, headers=headers, data=data)
    response.raise_for_status()  # Raise exception for HTTP errors
    
    return response.json().get("access_token")

def authorize_service_account_with_password():
    """Authorize the Service Account using password grant type."""
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {requests.auth._basic_auth_str(CLIENT_ID, CLIENT_SECRET)}"
    }
    data = {
        "grant_type": "password",
        "client_id": CLIENT_ID,
        "username": USERNAME,
        "password": PASSWORD
    }
    
    response = requests.post(TOKEN_URL, headers=headers, data=data)
    response.raise_for_status()  # Ensure the authorization was successful

def get_sap_commissions_data(token, api_url):
    """Use the provided OAuth2 token to access SAP Commissions API."""
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    response = requests.get(api_url, headers=headers)
    response.raise_for_status()  # Raise exception for HTTP errors
    
    return response.json()

if __name__ == "__main__":


    # Fetch the OAuth2 token using client credentials
    token = get_oauth2_token()

    # Authorize the Service Account
    authorize_service_account_with_password()

    # Use the token to access a specific SAP Commissions API endpoint
    # (Replace the URL below with the appropriate SAP Commissions API endpoint)
    api_endpoint = "YOUR_SAP_COMMISSIONS_API_ENDPOINT"
    data = get_sap_commissions_data(token, api_endpoint)

    print(data)

