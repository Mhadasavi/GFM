import os
import pickle
from typing import List

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow


class GoogleAuthenticator:
    def __init__(self, credentials_path: str, scopes: List[str], port: int = 8000):
        self.credentials_path = credentials_path
        self.scopes = scopes
        self.port = port

    def authenticate(self) -> Credentials:
        creds = None
        token_path = "F:\\GFM Data\\token.pickle"  # Change this path as needed

        # Load existing credentials
        if os.path.exists(token_path):
            with open(token_path, "rb") as token:
                creds = pickle.load(token)

        # Refresh or obtain new credentials if needed
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, self.scopes
                )
                creds = flow.run_local_server(port=self.port)

            # Save the new credentials
            with open(token_path, "wb") as token:
                pickle.dump(creds, token)

        return creds
