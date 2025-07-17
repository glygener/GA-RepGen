import os,sys
import json
import numpy as np
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, OrderBy
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread
from optparse import OptionParser




def create_glygen_ga4_report():
    # Main GA4 metrics request
    main_request = RunReportRequest(
        property='properties/' + config_obj["property_id"],
        dimensions=[Dimension(name="year"), Dimension(name="month")],
        metrics=[
            Metric(name="totalUsers"),
            Metric(name="activeUsers"),
            Metric(name="newUsers"),
            Metric(name="eventCount"),
            Metric(name="sessions")
        ],
        order_bys=[OrderBy(dimension={'dimension_name': 'month'})],
        date_ranges=[DateRange(start_date="2020-01-01", end_date="today")],
        dimension_filter={
            'filter': {
                'field_name': 'hostname',
                'string_filter': {
                    'value': DOMAIN_LIST[0],
                    'match_type': 'EXACT'
                },
                'in_list_filter': {
                    'values': DOMAIN_LIST
                }
            }
        }
    )

    # Traffic sources request
    traffic_source_request = RunReportRequest(
        property='properties/' + config_obj["property_id"],
        dimensions=[
            Dimension(name="year"),
            Dimension(name="month"),
            Dimension(name="sessionSource")
        ],
        metrics=[Metric(name="sessions")],
        order_bys=[
            OrderBy(dimension={'dimension_name': 'year'}, desc=True),
            OrderBy(dimension={'dimension_name': 'month'}, desc=True)
        ],
        date_ranges=[DateRange(start_date="2020-01-01", end_date="today")],
        dimension_filter={
            'filter': {
                'field_name': 'hostname',
                'string_filter': {
                    'value': DOMAIN_LIST[0],
                    'match_type': 'EXACT'
                },
                'in_list_filter': {
                    'values': DOMAIN_LIST
                }
            }
        }
    )

    # Send requests
    main_response = client.run_report(main_request)
    traffic_source_response = client.run_report(traffic_source_request)

    # Process main metrics
    def process_glygen_metrics(response):
        row_headers = [row.dimension_values for row in response.rows]
        metric_values = [row.metric_values for row in response.rows]

        data = []
        
        for i in range(len(row_headers)):
            year = int(row_headers[i][0].value)
            month = int(row_headers[i][1].value)
            total_users = float(metric_values[i][0].value)
            active_users = float(metric_values[i][1].value)
            new_users = float(metric_values[i][2].value)
            returning_users = total_users - new_users
            hits_events = float(metric_values[i][3].value)
            sessions = float(metric_values[i][4].value)

            data.append([year, month, total_users, active_users, returning_users, new_users, hits_events, sessions])

        df = pd.DataFrame(data, columns=[
            "Year", "Month", "Total Users", "Users/Active Users", "Returning Users", "New Users", "Hits/Events", "Sessions"
        ])

        return df

    # Process traffic sources
    def process_glygen_traffic_sources(response):
        data = {}
        for row in response.rows:
            year = int(row.dimension_values[0].value)
            month = int(row.dimension_values[1].value)
            source = row.dimension_values[2].value
            sessions = float(row.metric_values[0].value)
            
            key = f"{month:02}, {year}"
            if key not in data:
                data[key] = {"Organic Search": 0, "Direct": 0, "Referral": 0}
            
            if source.lower() == "google":
                data[key]["Organic Search"] += sessions
            elif source.lower() == "(direct)":
                data[key]["Direct"] += sessions
            elif source.lower() not in ["google", "(direct)"]:
                data[key]["Referral"] += sessions

        df = pd.DataFrame.from_dict(data, orient='index', columns=["Organic Search", "Direct", "Referral"])
        df.index.name = "Month-Year"
        df = df.reset_index()
        
        return df

    # Process both datasets
    main_df = process_glygen_metrics(main_response)
    traffic_sources_df = process_glygen_traffic_sources(traffic_source_response)

    # Combine datasets
    def combine_datasets(main_df, traffic_sources_df):
        # Create Month-Year column for both dataframes
        main_df['Month-Year'] = main_df['Month'].apply(lambda x: f'{x:02}') + ', ' + main_df['Year'].astype(str)
        
        # Merge dataframes
        combined_df = pd.merge(main_df, traffic_sources_df, on='Month-Year', how='left')
        
        # Reorder and select columns
        columns_order = [
            'Month-Year', 'Total Users', 'Users/Active Users', 'Returning Users', 
            'New Users', 'Hits/Events', 'Sessions', 
            'Organic Search', 'Direct', 'Referral'
        ]
        combined_df = combined_df[columns_order]
        
        # Create a datetime column for sorting
        combined_df['Sort_Date'] = pd.to_datetime(combined_df['Month-Year'], format='%m, %Y')
        
        # Sort in descending order (latest first)
        combined_df = combined_df.sort_values('Sort_Date', ascending=False)
        
        # Drop the sorting column
        combined_df = combined_df.drop(columns=['Sort_Date'])
        
        return combined_df

    return combine_datasets(main_df, traffic_sources_df)

def add_color_formatting(df):
    """
    Add color formatting based on trends and outliers
    Color coding:
    - Green: Above average (positive trend)
    - Red: Below average (negative trend)
    - Yellow: Slightly different from average
    """
    def get_color_class(column):
        # Calculate mean and standard deviation
        mean = df[column].mean()
        std = df[column].std()
        
        def color_mapper(value):
            # More than 1 std dev above mean
            if value > mean + std:
                return 'positive-high-outlier'
            # Between 0.5 and 1 std dev above mean
            elif value > mean + (std/2):
                return 'positive-mild-outlier'
            # More than 1 std dev below mean
            elif value < mean - std:
                return 'negative-high-outlier'
            # Between 0.5 and 1 std dev below mean
            elif value < mean - (std/2):
                return 'negative-mild-outlier'
            # Close to average
            else:
                return 'average'
        
        return df[column].apply(color_mapper)
    
    # Columns to analyze (excluding Month-Year)
    numeric_columns = df.columns.drop('Month-Year').tolist()
    
    # Create color mapping for each column
    color_mapping = {col: get_color_class(col) for col in numeric_columns}
    
    return df, color_mapping

# Google Sheets API setup and export
def export_to_google_sheets(df, color_mapping):

    gc = gspread.authorize(creds)

    # Check if sheet exists, if not create it
    try:
        sheet = gc.open_by_key(config_obj["sheet_id"]).worksheet(SHEET_TITLE)
    except gspread.exceptions.WorksheetNotFound:
        sheet = gc.open_by_key(config_obj["sheet_id"]).add_worksheet(title=SHEET_TITLE, rows="100", cols="20")

    # Convert DataFrame to list of lists for Google Sheets
    values = [df.columns.tolist()] + df.values.tolist()

    # Update the sheet
    sheet.clear()  # Clear existing content
    sheet.update('A1', values, value_input_option='RAW')

    # Update the sheet
    #request = service.spreadsheets().values().update(
    #    spreadsheetId=config_obj["sheet_id"],
    #    range= SHEET_TITLE + '!A1',
    #    valueInputOption='RAW',
    #    body={'values': values}
    #)
    #response = request.execute()

    # Formatting colors
    batch_update_requests = [{
            'addConditionalFormatRule': {
                'rule': {
                    'ranges': [{
                        'sheetId': sheet.id,
                        'startRowIndex': 1,  # Skip header row
                        'startColumnIndex': col_idx - 1,
                        'endColumnIndex': col_idx
                    }],
                    'gradientRule': {
                        'minpoint': {
                            'color': {'red': 0.839, 'green': 0.404, 'blue': 0.404},  # Red
                            'type': 'MIN'
                        },
                        'midpoint': {
                            'color': {'red': 1, 'green': 1, 'blue': 1},  # White
                            'type': 'PERCENTILE',
                            'value': '50'
                        },
                        'maxpoint': {
                            'color': {'red': 0.420, 'green': 0.655, 'blue': 0.420},  # Green
                            'type': 'MAX'
                        }
                    }
                }
            }
        } for col_idx, col_name in enumerate(df.columns[1:], start=2)]  # Skip first column

    # Execute batch update
    if batch_update_requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=config_obj["sheet_id"],
            body={'requests': batch_update_requests}
        ).execute()

    print(f"Report updated successfully in sheet: {SHEET_TITLE}")





def main():

    

    usage = "\n%prog  [options]"
    parser = OptionParser(usage,version=" ")
    parser.add_option("-d","--domain",action="store",dest="domain",help="glygen/argosdb")
    parser.add_option("-m","--modudle",action="store",dest="module",help="portal/wiki/beta/data/api")


    (options,args) = parser.parse_args()
    for file in ([options.domain, options.module]):
        if not (file):
            parser.print_help()
            sys.exit(0)

    domain = options.domain
    module = options.module


    global config_obj
    global client
    global creds
    global service
    global SHEET_TITLE
    global DOMAIN_LIST
 

    config_obj = json.load(open("conf/config.%s.json" % (domain)))
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "conf/credentials.%s.json" % (domain)
    credentials_file = "conf/credentials.%s.json" % (domain)
    # Initialize the GA4 client
    client = BetaAnalyticsDataClient()
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(credentials_file, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)

    SHEET_TITLE = config_obj["tabs"]["overview"][module]["sheet_title"]
    DOMAIN_LIST = config_obj["tabs"]["overview"][module]["domain_list"]


    #print (SHEET_TITLE)
    #print (DOMAIN_LIST)
    #exit()

    df = create_glygen_ga4_report()
    df_with_colors, color_mapping = add_color_formatting(df)
    export_to_google_sheets(df_with_colors, color_mapping)

    # Optional: Print the first few rows and color mapping
    print(df_with_colors.head())
    print("\nColor Mapping Legend:")
    print("- Green shades: Performance above average (light to dark intensity)")
    print("- Red shades: Performance below average (light to dark intensity)")
    print("- White: Performance close to average")


    return





if __name__ == '__main__':
    main()


