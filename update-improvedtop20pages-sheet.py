import os,sys
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





def get_top_pages_overview():

    # First get top 20 pages overall with extended date range
    top_pages_request = RunReportRequest(
        property='properties/' + config_obj["property_id"],
        dimensions=[
            Dimension(name="pagePath"),
            Dimension(name="pageTitle")
        ],
        metrics=[
            Metric(name="screenPageViews")
        ],
        order_bys=[
            OrderBy(metric={"metric_name": "screenPageViews"}, desc=True)
        ],
        limit=100000,  # Increased to maximum limit
        date_ranges=[DateRange(start_date="2023-04-01", end_date="today")]
    )

    top_pages_response = client.run_report(top_pages_request)
    
    # In the first path mapping section, replace:
    path_mapping = {}
    for row in top_pages_response.rows:
        path = row.dimension_values[0].value
        normalized_path = path.rstrip('/')
        
        # Handle special cases and duplicates
        if path == "/" or path == "/home" or path == "/home/":
            normalized_path = "/"
        elif path.startswith("/glycan-search"):
            normalized_path = "/glycan-search/"
        elif path.startswith("/protein-search"):
            normalized_path = "/protein-search/"
            
        # Add views to get proper top pages
        views = int(row.metric_values[0].value)
        if normalized_path not in path_mapping:
            path_mapping[normalized_path] = {'path': path, 'views': views}
        else:
            path_mapping[normalized_path]['views'] += views

    # Sort by total views and get top 20
    consolidated_paths = [info['path'] for info in sorted(path_mapping.values(), 
                     key=lambda x: x['views'], reverse=True)][:20]


    # Then get monthly data with increased limits
    monthly_request = RunReportRequest(
        property='properties/' + config_obj["property_id"],
        dimensions=[
            Dimension(name="year"),
            Dimension(name="month"),
            Dimension(name="pagePath")
        ],
        metrics=[
            Metric(name="screenPageViews")
        ],
        order_bys=[
            OrderBy(dimension={"dimension_name": "year"}, desc=True),
            OrderBy(dimension={"dimension_name": "month"}, desc=True)
        ],
        date_ranges=[DateRange(start_date="2023-04-01", end_date="today")],
        limit=100000,  # Maximum limit to avoid data sampling
        offset=0  # Start from beginning
    )

    # Get all data by handling pagination
    all_rows = []
    while True:
        response = client.run_report(monthly_request)
        all_rows.extend(response.rows)
        
        if len(response.rows) < 100000:  # No more data to fetch
            break
            
        monthly_request.offset = len(all_rows)  # Update offset for next batch

    # Process into a dictionary with consolidated paths
    monthly_data = {}
    total_monthly_views = {}

    # In the monthly data processing section, modify:
    for row in all_rows:
        year = int(row.dimension_values[0].value)
        month = int(row.dimension_values[1].value)
        page_path = row.dimension_values[2].value
        views = int(row.metric_values[0].value)
        
        month_key = f"{month:02d}, {year}"
        
        if month_key not in monthly_data:
            monthly_data[month_key] = {path: 0 for path in consolidated_paths}
            total_monthly_views[month_key] = 0
        
        # Normalize path and add views
        normalized_path = page_path.rstrip('/')
        if normalized_path == "/" or normalized_path == "/home":
            normalized_path = "/"
            monthly_data[month_key]["/"] += views
        elif normalized_path.startswith("/glycan-search"):
            monthly_data[month_key]["/glycan-search/"] += views
        elif normalized_path.startswith("/protein-search"):
            monthly_data[month_key]["/protein-search/"] += views
        elif page_path in consolidated_paths:
            monthly_data[month_key][page_path] += views
        
        total_monthly_views[month_key] += views


    # Create DataFrame and continue with existing code...
    df = pd.DataFrame.from_dict(monthly_data, orient='index')
    df['Total Pageviews'] = pd.Series(total_monthly_views)
    
    cols = ['Total Pageviews'] + [col for col in df.columns if col != 'Total Pageviews']
    df = df[cols]
    
    df.index.name = 'Month-Year'
    df = df.reset_index()

    # Sort by date (latest first)
    df['Sort_Date'] = pd.to_datetime(df['Month-Year'], format='%m, %Y')
    df = df.sort_values('Sort_Date', ascending=False)
    df = df.drop('Sort_Date', axis=1)

    gc = gspread.authorize(creds)
    
    # Convert DataFrame to values
    values = [df.columns.tolist()] + df.values.tolist()
    
    # Update sheet
    sheet = gc.open_by_key(config_obj["sheet_id"]).worksheet(SHEET_TITLE)
    sheet.clear()
    sheet.update(values, "A1")

    # Apply conditional formatting to all numeric columns
    format_request = {
        'requests': [{
            'addConditionalFormatRule': {
                'rule': {
                    'ranges': [{
                        'sheetId': sheet.id,
                        'startRowIndex': 1,
                        'startColumnIndex': col_idx,
                        'endColumnIndex': col_idx + 1
                    }],
                    'gradientRule': {
                        'minpoint': {'color': {'red': 0.839, 'green': 0.404, 'blue': 0.404}, 'type': 'MIN'},
                        'midpoint': {'color': {'red': 1, 'green': 1, 'blue': 1}, 'type': 'PERCENTILE', 'value': '50'},
                        'maxpoint': {'color': {'red': 0.420, 'green': 0.655, 'blue': 0.420}, 'type': 'MAX'}
                    }
                }
            }
        } for col_idx in range(1, len(df.columns))]
    }
    
    service.spreadsheets().batchUpdate(
        spreadsheetId=config_obj["sheet_id"],
        body=format_request
    ).execute()


    return




def add_top_pages_chart():
    
    
    # Get sheet ID
    spreadsheet = service.spreadsheets().get(spreadsheetId=config_obj["sheet_id"]).execute()
    sheet_id = None
    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == SHEET_TITLE:
            sheet_id = sheet['properties']['sheetId']
            break
            
    if sheet_id is None:
        raise ValueError(f"Sheet '{SHEET_TITLE}' not found")

    # Get the data to determine dimensions
    gc = gspread.authorize(creds)
    worksheet = gc.open_by_key(config_obj["sheet_id"]).worksheet(SHEET_TITLE)
    data = worksheet.get_all_values()
    num_rows = len(data)
    num_columns = len(data[0])

    padding_request = {
        'requests': [{
            'updateDimensionProperties': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': num_columns,
                    'endIndex': num_columns + 5
                },
                'properties': {
                    'pixelSize': 75
                },
                'fields': 'pixelSize'
            }
        }]
    }

    # Define colors for the lines
    colors = [
        {'red': 0.4, 'green': 0.4, 'blue': 1.0},  # Blue
        {'red': 1.0, 'green': 0.4, 'blue': 0.4},  # Red
        {'red': 0.4, 'green': 1.0, 'blue': 0.4},  # Green
        {'red': 1.0, 'green': 0.8, 'blue': 0.2},  # Yellow
        {'red': 0.8, 'green': 0.4, 'blue': 0.8},  # Purple
        {'red': 0.4, 'green': 0.8, 'blue': 1.0},  # Light Blue
        {'red': 1.0, 'green': 0.6, 'blue': 0.4},  # Orange
        {'red': 0.6, 'green': 0.4, 'blue': 0.2},  # Brown
        {'red': 0.8, 'green': 0.8, 'blue': 0.4},  # Light Yellow
        {'red': 0.4, 'green': 0.8, 'blue': 0.6}   # Teal
    ]

    # Create chart specification
    chart_obj = json_lib.get_top_pages_chart_json(sheet_id, num_rows, num_columns)

    # Add series for top 10 pages (columns 1 to 11, including Total Pageviews)
    for idx in range(1, 11):
        series = {
            'series': {
                'sourceRange': {
                    'sources': [{
                        'sheetId': sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': num_rows,
                        'startColumnIndex': idx,
                        'endColumnIndex': idx + 1
                    }]
                }
            },
            'targetAxis': 'LEFT_AXIS',
            'color': colors[idx - 1] if idx - 1 < len(colors) else {'red': 0.5, 'green': 0.5, 'blue': 0.5},
            'lineStyle': {'type': 'SOLID', 'width': 2}
        }
        chart_obj['spec']['basicChart']['series'].append(series)

    # Create the chart request
    chart_request = {
        'requests': [{
            'addChart': {
                'chart': chart_obj
            }
        }]
    }

    # Execute the request
    try:
        service.spreadsheets().batchUpdate(
            spreadsheetId=config_obj["sheet_id"],
            body=chart_request
        ).execute()
    except Exception as e:
        print(f"Error creating chart: {str(e)}")

    return






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
    SHEET_TITLE = 'Improved_Top20Pages'


    get_top_pages_overview()
    add_top_pages_chart()
    
    print(" ... FINISHED UPDATING sheet=%s" % (SHEET_TITLE))


    return



if __name__ == '__main__':
    main()



