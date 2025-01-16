# GA-RepGen - Google Analytics and Sheets Report Generator

## Overview
GA-RepGen is a tool designed to automate the process of generating reports by integrating data from Google Analytics and dynamically update Google Sheets. This tool utilizes Google APIs to fetch analytics data and update a specified Google Sheet in real-time, streamlining data analysis and reporting workflows.

With GA-RepGen, you can:
- Access and retrieve data from Google Analytics using the Google Analytics API.
- Update and manipulate data within a Google Sheet using the Google Sheets API.
- Automate the generation of reports to save time and improve accuracy.

This README provides instructions for setting up the necessary API credentials and integrating them with GA-RepGen.

---

## Prerequisites
- A Google Cloud Platform (GCP) account.
- The necessary APIs (Google Analytics API and Google Sheets API) enabled in your project.
- Access to the [Google Sheet being updated](https://docs.google.com/spreadsheets/d/1faXFb6yEYzHssBFU-LH5b4YIRhBxttfBuyaHnl09fuA/edit?usp=sharing).

---

## Steps to Generate API Key JSON

### 1. Log in to Google Cloud Platform
1. Go to the [Google Cloud Console](https://console.cloud.google.com/).
2. Log in with your Google account.

### 2. Create a New Project (Optional)
1. In the top navigation bar, click the project dropdown menu.
2. Click `New Project`.
3. Name your project and click `Create`.

### 3. Enable APIs
#### Google Analytics API
1. In the left-hand menu, navigate to **APIs & Services > Library**.
2. Search for `Google Analytics API`.
3. Click `Enable`.

#### Google Sheets API
1. Return to the **Library**.
2. Search for `Google Sheets API`.
3. Click `Enable`.

### 4. Create Service Account Credentials
1. In the left-hand menu, navigate to **APIs & Services > Credentials**.
2. Click `Create Credentials` and select `Service Account`.
3. Fill in the details for the service account (e.g., name, role).
   - **Role**: Assign `Editor` or `Owner` role as needed for your use case.
4. Click `Done`.

### 5. Generate Key JSON File
1. Go to the **Service Accounts** page.
2. Find the service account you just created.
3. Click the **Actions** menu (three dots) next to it and select `Manage keys`.
4. Click `Add Key > Create New Key`.
5. Select `JSON` and click `Create`.
6. Save the downloaded JSON file securely.

### 6. Share the Google Sheet with the Service Account
1. Open the [Google Sheet](https://docs.google.com/spreadsheets/d/1faXFb6yEYzHssBFU-LH5b4YIRhBxttfBuyaHnl09fuA/edit?usp=sharing).
2. Click the **Share** button in the top-right corner.
3. Add the email address of the service account (found in the JSON file under `client_email`).
4. Assign `Editor` permissions and click `Send`.

---

## Testing the Setup
Use the downloaded JSON file in your application to authenticate requests to the Google Analytics API and Google Sheets API.

For example:
```python
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Load the JSON key file
credentials = Credentials.from_service_account_file(
    'path_to_your_key.json',
    scopes=[
        'https://www.googleapis.com/auth/analytics.readonly',
        'https://www.googleapis.com/auth/spreadsheets'
    ]
)

# Access Google Sheets
sheet_service = build('sheets', 'v4', credentials=credentials)
spreadsheet_id = '1faXFb6yEYzHssBFU-LH5b4YIRhBxttfBuyaHnl09fuA'
result = sheet_service.spreadsheets().values().get(
    spreadsheetId=spreadsheet_id,
    range='Sheet1!A1:A10'
).execute()
print(result)
```

---

## Notes
- Keep your JSON file secure; do not expose it publicly.
- Ensure the service account has sufficient permissions to access the APIs and Google Sheet.
- Refer to the [Google Cloud Documentation](https://cloud.google.com/docs) for additional details.
