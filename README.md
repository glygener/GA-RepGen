Target Google Sheet
===================
    https://docs.google.com/spreadsheets/d/1faXFb6yEYzHssBFU-LH5b4YIRhBxttfBuyaHnl09fuA/edit?gid=1246614802#gid=1246614802


Sheets and update scripts
=========================
	- Updated_AllDomains_Data, AllDomains_Top10Referrals, AllDomains_Bottom10Pages
			$ python3 update-alldomainsdata-sheet.py -d $domain
		where $domain can be glygen/argosdb/...

   
	- Top10Countries, Top10Countries_Monthly
			$ python3 update-alldomainstop10countries-sheet.py -d $domain
      where $domain can be glygen/argosdb/...

	
	- Subdomains_Overview
			$ python3 update-subdomainsoverview-sheet.py -d $domain
      where $domain can be glygen/argosdb/...

 
	- Improved_Top20Pages
			$ python3 update-improvedtop20pages-sheet.py -d $domain
		where $domain can be glygen/argosdb/...

	
	- GlyGen_Portal/Wiki/Data/Beta/API_Overview
			$ python3 update-overview-sheet.py -d $domain -m $mod
		where $domain can be glygen/argosdb/..., and $mod can be portal/wiki/data/beta/api

    

Google Cloud Platform (GCP) Setup
=================================
	a. Create a Google Cloud Project
		Go to the Google Cloud Console.
		Click on the project dropdown and select "New Project".
		Enter a project name (e.g., ga-repgen-project) and click "Create".

	b. Enable the Google Analytics Data and Google Sheets APIs
		In the API Library, search for "Google Analytics Data API".
		Select it and click "Enable".
		In the API Library, search for "Google Sheets API"
		Select it and click "Enable".

	c. Create a Service Account
		Navigate to IAM & Admin > Service Accounts.
		Click "Create Service Account".
		Provide a name (e.g., ga-repgen-sa) and description.
		Click "Create and Continue".
		Assign the role Viewer.
		Click "Done".

	d. Generate and Download JSON Credentials
		In the Service Accounts list, find your newly created service account.
		Click on it, then go to the "Keys" tab.
		Click "Add Key" > "Create New Key".
		Select "JSON" and click "Create".
		Save the downloaded JSON file securely as ~/.ssh/ga-repgen-sa-credentials.json

	e. Grant Access to the Service Account in GA4
		Open Google Analytics and navigate to your GA4 property.
		Go to Admin > Property Access Management.
		Click the "+" button and select "Add Users".
		Enter the service account's email address (found in the JSON file).
		Assign the role Viewer.
		Click "Add".

	f. Grant Access to the Service Account in Google Sheets
		Open the Google Sheet.
		https://docs.google.com/spreadsheets/d/1faXFb6yEYzHssBFU-LH5b4YIRhBxttfBuyaHnl09fuA/edit?gid=1246614802#gid=1246614802
		Click the Share button in the top-right corner.
		Add the email address of the service account (found in ~/.ssh/ga-repgen-sa-credentials.json under client_email).
		Assign Editor permissions and click Send.

