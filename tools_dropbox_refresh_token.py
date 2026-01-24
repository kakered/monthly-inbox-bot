import os
from dropbox import DropboxOAuth2FlowNoRedirect

APP_KEY = os.environ["DROPBOX_APP_KEY"]

auth_flow = DropboxOAuth2FlowNoRedirect(
    consumer_key=APP_KEY,
    consumer_secret=None,
    token_access_type="offline",
    use_pkce=True,
)

url = auth_flow.start()
print("\n[STEP 1] Open this URL in your browser and click 'Allow':\n")
print(url)

code = input("\n[STEP 2] Paste the authorization code here and press Enter:\n> ").strip()

res = auth_flow.finish(code)

print("\n[SUCCESS] REFRESH TOKEN:\n")
print(res.refresh_token)
