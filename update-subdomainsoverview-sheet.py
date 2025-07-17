import os, sys
import json
import numpy as np
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, OrderBy, MetricType
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread
import json_lib
from optparse import OptionParser



def explore_subdomains():

    # First get all hostnames/subdomains
    hostname_request = RunReportRequest(
        property='properties/' + config_obj["property_id"],
        dimensions=[
            Dimension(name="hostname")
        ],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="engagedSessions"),
            Metric(name="totalUsers")
        ],
        order_bys=[
            OrderBy(metric={"metric_name": "screenPageViews"}, desc=True)
        ],
        date_ranges=[DateRange(start_date="2023-04-01", end_date="today")]
    )

    hostname_response = client.run_report(hostname_request)

    # Process into DataFrame
    data = []
    for row in hostname_response.rows:
        hostname = row.dimension_values[0].value
        pageviews = int(row.metric_values[0].value)
        sessions = int(row.metric_values[1].value)
        users = int(row.metric_values[2].value)
        data.append([hostname, pageviews, sessions, users])

    df = pd.DataFrame(data, columns=["Hostname", "Pageviews", "Engaged Sessions", "Users"])

    gc = gspread.authorize(creds)
    

    # Create or get worksheet
    try:
        worksheet = gc.open_by_key(config_obj["sheet_id"]).worksheet(SHEET_TITLE)
        worksheet.clear()
    except:
        worksheet = gc.open_by_key(config_obj["sheet_id"]).add_worksheet(SHEET_TITLE, rows=100, cols=20)

    # Update values
    values = [df.columns.tolist()] + df.values.tolist()
    worksheet.update(values=values, range_name='A1')

    # Format header
    format_requests = [{
        'repeatCell': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 0,
                'endRowIndex': 1
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
                    'textFormat': {'bold': True},
                    'horizontalAlignment': 'CENTER'
                }
            },
            'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
        }
    }]

    # Add conditional formatting for numeric columns
    for col_idx in range(1, 4):  # Columns B, C, D
        format_requests.append({
            'addConditionalFormatRule': {
                'rule': {
                    'ranges': [{
                        'sheetId': worksheet.id,
                        'startRowIndex': 1,
                        'startColumnIndex': col_idx,
                        'endColumnIndex': col_idx + 1
                    }],
                    'gradientRule': {
                        'minpoint': {
                            'color': {'red': 0.839, 'green': 0.404, 'blue': 0.404},
                            'type': 'MIN'
                        },
                        'midpoint': {
                            'color': {'red': 1, 'green': 1, 'blue': 1},
                            'type': 'PERCENTILE',
                            'value': '50'
                        },
                        'maxpoint': {
                            'color': {'red': 0.420, 'green': 0.655, 'blue': 0.420},
                            'type': 'MAX'
                        }
                    }
                }
            }
        })

    service.spreadsheets().batchUpdate(
        spreadsheetId=config_obj["sheet_id"],
        body={'requests': format_requests}
    ).execute()

    return df  # Return DataFrame for further analysis





def main():

    
    usage = "\n%prog  [options]"
    parser = OptionParser(usage,version=" ")
    parser.add_option("-d","--domain",action="store",dest="domain",help="glygen/argosdb")

    (options,args) = parser.parse_args()
    for file in ([options.domain]):
        if not (file):
            parser.print_help()
            sys.exit(0)

    domain = options.domain


    global config_obj
    global client
    global creds
    global service
    global SHEET_TITLE
    

    config_obj = json.load(open("conf/config.%s.json" % (domain)))
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "conf/credentials.%s.json" % (domain)
    credentials_file = "conf/credentials.%s.json" % (domain)
    # Initialize the GA4 client
    client = BetaAnalyticsDataClient()
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(credentials_file, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)

    SHEET_TITLE = 'Subdomains_Overview'
    subdomains_df = explore_subdomains()
    print("\n ... FINISHED UPDATING sheet=%s" % (SHEET_TITLE))

    #print(subdomains_df[['Hostname', 'Pageviews']].to_string())




    return



if __name__ == '__main__':
    main()



