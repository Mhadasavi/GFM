from typing import List

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow


class GoogleAuthenticator:
    def __init__(self, credentials_path: str, scopes: List[str], port: int = 8000):
        self.credentials_path = credentials_path
        self.scopes = scopes
        self.port = port

    def authenticate(self) -> Credentials:
        flow = InstalledAppFlow.from_client_secrets_file(
            self.credentials_path, self.scopes
        )
        creds = flow.run_local_server(port=self.port)
        return creds
