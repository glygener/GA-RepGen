### Step-1: Installation
After you clone this repository, use the following commands to install required python packages
```
$ sudo pip3 install google-analytics-data
$ sudo pip3 install google-api-python-client
$ sudo pip3 install numpy
$ sudo pip3 install pandas
$ sudo pip3 install gspread
```


### Step-2: Files under the "conf/" folder
Follow the instructions given in step-5 to generate Google credentials JSON file. Once done, place 
your Google credentials JSON file under the conf/ folder using keyword representing your domain. 
For example, for the GlyGen domain, the file should be named as "conf/credentials.glygen.json" 

To make a config file for your domain (e.g biomarkerdb domain) use the following command to make a copy.
```
$ cp conf/config.glygen.json conf/config.biomarkerkb.json
```
Edit your new config file to make sure the your parameters are correct. This file also contains values
for "property_id" which is the property ID from your Google Analytics account and "sheet_id" which is the
ID for your Google spreadsheet created in Step-4.


### Step-3: Running scripts to update sheets
Use the commands below to update your Google sheet tabs which are created following instructions in step-4. The 
scripts take one or two arguments (-d $domain -m $module) 
```
$ python3 update-alldomainsdata-sheet.py -d $domain      		# updated sheets: Updated_AllDomains_Data, AllDomains_Top10Referrals, AllDomains_Bottom10Pages
$ python3 update-alldomainstop10countries-sheet.py -d $domain		# updated sheets: Top10Countries, Top10Countries_Monthly
$ python3 update-subdomainsoverview-sheet.py -d $domain			# updated sheets: Subdomains_Overview
$ python3 update-improvedtop20pages-sheet.py -d $domain			# updated sheets: Improved_Top20Pages
$ python3 update-overview-sheet.py -d $domain -m $module		# updated sheets:  $domainlable_$modulelabel_Overview
$ python3 update-top20pages-sheet.py -d $domain -m $module		# updated sheets:  $domainlable_$modulelabel_Top20Pages
$ python3 update-top10referrals-sheet.py -d $domain -m $module		# updated sheets:  $domainlable_$modulelabel_Top10Referrals
$ python3 update-top10countries-sheet.py -d $domain -m $module		# updated sheets:  $domainlable_$modulelabel_Top10Countries_Monthly


$domain can be glygen/argosdb/biomarkerkb/... 
$domainlabel can be GlyGen/ArgosDB/BioMarkerKB/...
$module can be portal/beta/data/wiki/api
$modulelabel can be Portal/Beta/Data/Wiki/API
```


### Step-4 Creating Google Spreadsheet
Create Google spreadsheet and make sure the the sheet/tab names are in agreement with the names you have
used in "conf/config.$domain.json" file. The Google spreadsheet given below is for GlyGen and the value
for the "sheet_id" parameter is "1faXFb6yEYzHssBFU-LH5b4YIRhBxttfBuyaHnl09fuA"
```
https://docs.google.com/spreadsheets/d/1faXFb6yEYzHssBFU-LH5b4YIRhBxttfBuyaHnl09fuA/edit?gid=1246614802#gid=1246614802
```
The expected sheet names are:
```
Updated_AllDomains_Data, AllDomains_Top10Referrals, AllDomains_Bottom10Pages
Top10Countries, Top10Countries_Monthly
Subdomains_Overview
Improved_Top20Pages
$domainlable_$modulelabel_Overview
$domainlable_$modulelabel_Top20Pages
$domainlable_$modulelabel_Top10Referrals
$domainlable_$modulelabel_Top10Countries_Monthly

$domainlabel can be GlyGen/ArgosDB/BioMarkerKB/...
$modulelabel can be Portal/Beta/Data/Wiki/API
```

  

### Step-5: Google Cloud Platform (GCP) Setup

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
		Save the downloaded JSON file under the conf/ folder (e.g conf/credentials.glygen.json)

	e. Grant Access to the Service Account in GA4
		Open Google Analytics and navigate to your GA4 property.
		Go to Admin > Property Access Management.
		Click the "+" button and select "Add Users".
		Enter the service account's email address (found in the JSON file).
		Assign the role Viewer.
		Click "Add".

	f. Grant Access to the Service Account in Google Sheets
		Open the Google Sheet.
		Click the Share button in the top-right corner.
		Add the email address of the service account (found in  conf/credentials.glygen.json under client_email).
		Assign Editor permissions and click Send.

