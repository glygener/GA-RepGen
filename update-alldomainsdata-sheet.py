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




def create_combined_ga4_report():
    # Main GA4 metrics request
    main_request = RunReportRequest(
        property='properties/' + config_obj["property_id"],
        dimensions=[Dimension(name="year"), Dimension(name="month"), Dimension(name="newVsReturning")],
        metrics=[
            Metric(name="totalUsers"),
            Metric(name="activeUsers"),
            Metric(name="eventCount"),
            Metric(name="sessions")
        ],
        order_bys=[OrderBy(dimension={'dimension_name': 'month'})],
        date_ranges=[DateRange(start_date="2020-01-01", end_date="today")]
    )

    # Traffic sources request (unchanged)
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
        date_ranges=[DateRange(start_date="2020-01-01", end_date="today")]
    )

    # Send requests
    main_response = client.run_report(main_request)
    traffic_source_response = client.run_report(traffic_source_request)

    # Process main metrics (updated)
    def process_main_metrics(response):
        data_dict = {}
        
        for row in response.rows:
            year = int(row.dimension_values[0].value)
            month = int(row.dimension_values[1].value)
            user_type = row.dimension_values[2].value
            
            key = (year, month)
            if key not in data_dict:
                data_dict[key] = {
                    'total_users': 0,
                    'active_users': 0,
                    'new_users': 0,
                    'returning_users': 0,
                    'event_count': 0,
                    'sessions': 0
                }
            
            # Handle user types
            if user_type == "new":
                data_dict[key]['new_users'] += float(row.metric_values[0].value)
            elif user_type == "returning":
                data_dict[key]['returning_users'] += float(row.metric_values[0].value)
            
            # Aggregate metrics
            data_dict[key]['active_users'] += float(row.metric_values[1].value)
            data_dict[key]['event_count'] += float(row.metric_values[2].value)
            data_dict[key]['sessions'] += float(row.metric_values[3].value)
            
            # Calculate total users
            data_dict[key]['total_users'] = (
                data_dict[key]['new_users'] + 
                data_dict[key]['returning_users']
            )

        # Convert to DataFrame
        data = []
        for (year, month), metrics in data_dict.items():
            data.append([
                year,
                month,
                metrics['total_users'],
                metrics['active_users'],
                metrics['new_users'],
                metrics['returning_users'],
                metrics['event_count'],
                metrics['sessions']
            ])
    
        return pd.DataFrame(data, columns=[
            "Year", "Month", "Total Users", "Users/Active Users", 
            "New Users", "Returning Users", "Hits/Events", "Sessions"
        ])

    # Process traffic sources
    def process_traffic_sources(response):
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
    main_df = process_main_metrics(main_response)
    traffic_sources_df = process_traffic_sources(traffic_source_response)

    # Combine datasets (updated)
    def combine_datasets(main_df, traffic_sources_df):
        # Create Month-Year column for both dataframes
        main_df['Month-Year'] = main_df['Month'].apply(lambda x: f'{x:02}') + ', ' + main_df['Year'].astype(str)
        
        # Merge dataframes
        combined_df = pd.merge(main_df, traffic_sources_df, on='Month-Year', how='left')
        
        # Reorder and select columns
        columns_order = [
            'Month-Year', 'Total Users', 'Users/Active Users', 
            'New Users', 'Returning Users', 'Hits/Events', 'Sessions',
            'Organic Search', 'Direct', 'Referral'
        ]
        combined_df = combined_df[columns_order]
        
        # Create datetime column for sorting
        combined_df['Sort_Date'] = pd.to_datetime(combined_df['Month-Year'], format='%m, %Y')
        
        # Sort in descending order
        combined_df = combined_df.sort_values('Sort_Date', ascending=False)
        
        # Drop sorting column
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
    # Convert DataFrame to list of lists for Google Sheets
    values = [df.columns.tolist()] + df.values.tolist()

    # Update the sheet
    request = service.spreadsheets().values().update(
        spreadsheetId=config_obj["sheet_id"],
        range=SHEET_TITLE + '!A1',
        valueInputOption='RAW',
        body={'values': values}
    )
    response = request.execute()

    # Formatting colors
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(config_obj["sheet_id"]).worksheet(SHEET_TITLE)

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
    #print(f"{response.get('updatedCells')} cells updated.")

    return




def update_charts(df):
    
    spreadsheet = service.spreadsheets().get(spreadsheetId=config_obj["sheet_id"]).execute()
    sheet_id = None
    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == SHEET_TITLE:
            sheet_id = sheet['properties']['sheetId']
            break
    if sheet_id is None:
        raise ValueError("Sheet '%s' not found" % (SHEET_TITLE))

    # Calculate the y-axis range
    max_value = df[['Total Users', 'Users/Active Users', 'Returning Users', 'New Users']].max().max()
    min_value = df[['Total Users', 'Users/Active Users', 'Returning Users', 'New Users']].min().min()
    y_axis_max = max_value * 1.1
    y_axis_min = max(0, min_value * 0.9)

    # First, delete any existing charts
    delete_charts_request = {
        'requests': [{
            'deleteEmbeddedObject': {
                'objectId': chart['chartId']
            }
        } for chart in spreadsheet.get('sheets', [])[0].get('charts', [])]
    }

    if delete_charts_request['requests']:
        service.spreadsheets().batchUpdate(
            spreadsheetId=config_obj["sheet_id"],
            body=delete_charts_request
        ).execute()

    # Define the new chart
    start_row_index, end_row_index = 0, df.shape[0] + 1 
    metric_chart_obj = json_lib.get_user_metrics_chart_json(sheet_id, y_axis_min, y_axis_max, start_row_index, end_row_index)
    chart_request = {
        'requests': [{
            'addChart': {
                'chart': metric_chart_obj
            }
        }]
    }
    response = service.spreadsheets().batchUpdate(
        spreadsheetId=config_obj["sheet_id"],
        body=chart_request
    ).execute()
    #print("User metrics chart updated successfully in %s sheet." % (SHEET_TITLE))


    start_row_index, end_row_index = 0, df.shape[0] + 1
    traffic_chart_obj = json_lib.get_traffic_chart_json(sheet_id, start_row_index, end_row_index)
    traffic_chart_request = {
        'requests': [{
            'addChart': {
                'chart': traffic_chart_obj
            }
        }]
    }

    response = service.spreadsheets().batchUpdate(
        spreadsheetId=config_obj["sheet_id"],
        body=traffic_chart_request
    ).execute()
    #print("Traffic chart updated successfully in %s sheet." % (SHEET_TITLE))




    return





def create_bottom_pages_trend_report():

    # First request: Get overall bottom 10 pages
    bottom_pages_request = RunReportRequest(
        property='properties/' + config_obj["property_id"],
        dimensions=[
            Dimension(name="pagePath"),
            Dimension(name="pageTitle")
        ],
        metrics=[
            Metric(name="screenPageViews")
        ],
        order_bys=[
            OrderBy(metric={"metric_name": "screenPageViews"}, desc=False)  # Changed to False for bottom pages
        ],
        limit=10,
        date_ranges=[DateRange(start_date="2023-12-01", end_date="today")]
    )

    bottom_pages_response = client.run_report(bottom_pages_request)
    bottom_pages = [(row.dimension_values[0].value, row.dimension_values[1].value) 
                   for row in bottom_pages_response.rows]

    # Second request: Get monthly data with explicit date range
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
        date_ranges=[DateRange(start_date="2023-12-01", end_date="today")],
        limit=50000
    )

    monthly_response = client.run_report(monthly_request)

    # Process monthly data with explicit date range handling
    monthly_data = {}
    
    # Create all month-year combinations from 2020 to today
    current_date = pd.Timestamp.now()
    start_date = pd.Timestamp('2023-12-01')
    date_range = pd.date_range(start=start_date, end=current_date, freq='M')
    
    # Initialize the dictionary with all possible months
    for date in date_range:
        month_key = f"{date.month:02d}, {date.year}"
        monthly_data[month_key] = {page[0]: 0 for page in bottom_pages}

    # Fill in the actual data
    for row in monthly_response.rows:
        year = int(row.dimension_values[0].value)
        month = int(row.dimension_values[1].value)
        page_path = row.dimension_values[2].value
        views = int(row.metric_values[0].value)
        
        month_key = f"{month:02d}, {year}"
        if month_key in monthly_data and page_path in monthly_data[month_key]:
            monthly_data[month_key][page_path] = views

    # Create DataFrame with all months
    df = pd.DataFrame.from_dict(monthly_data, orient='index')
    df.index.name = 'Month-Year'
    df = df.reset_index()

    # Sort by date (latest first)
    df['Sort_Date'] = pd.to_datetime(df['Month-Year'], format='%m, %Y')
    df = df.sort_values('Sort_Date', ascending=False)
    df = df.drop('Sort_Date', axis=1)

    # Format column headers with page titles
    header_mapping = {page[0]: f"{page[1]}\n({page[0]})" for page in bottom_pages}
    df = df.rename(columns=header_mapping)


    # Convert DataFrame to values
    values = [df.columns.tolist()] + df.values.tolist()

    # Update the sheet
    request = service.spreadsheets().values().update(
        spreadsheetId=config_obj["sheet_id"],
        range=f'{SHEET_TITLE}!A1',
        valueInputOption='RAW',
        body={'values': values}
    )
    response = request.execute()

    # Format header
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(config_obj["sheet_id"]).worksheet(SHEET_TITLE)
    
    format_requests = [{
        'repeatCell': {
            'range': {
                'sheetId': sheet.id,
                'startRowIndex': 0,
                'endRowIndex': 1
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
                    'textFormat': {'bold': True},
                    'wrapStrategy': 'WRAP',
                    'verticalAlignment': 'MIDDLE',
                    'horizontalAlignment': 'CENTER'
                }
            },
            'fields': 'userEnteredFormat(backgroundColor,textFormat,wrapStrategy,verticalAlignment,horizontalAlignment)'
        }
    }]

    service.spreadsheets().batchUpdate(
        spreadsheetId=config_obj["sheet_id"],
        body={'requests': format_requests}
    ).execute()


    return







def create_top_referrals_trend_report():

    # First request: Get overall top 10 referral sources
    top_referrals_request = RunReportRequest(
        property='properties/' + config_obj["property_id"],
        dimensions=[
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium")
        ],
        metrics=[
            Metric(name="sessions")
        ],
        order_bys=[
            OrderBy(metric={"metric_name": "sessions"}, desc=True)
        ],
        limit=10,
        date_ranges=[DateRange(start_date="2020-01-01", end_date="today")],
        dimension_filter={
            'filter': {
                'field_name': 'sessionMedium',
                'string_filter': {
                    'value': 'referral',
                    'match_type': 'EXACT'
                }
            }
        }
    )

    top_referrals_response = client.run_report(top_referrals_request)
    top_referrals = [(row.dimension_values[0].value, row.dimension_values[1].value) 
                     for row in top_referrals_response.rows]

    # Second request: Get monthly data for these referral sources
    monthly_request = RunReportRequest(
        property='properties/' + config_obj["property_id"],
        dimensions=[
            Dimension(name="year"),
            Dimension(name="month"),
            Dimension(name="sessionSource")
        ],
        metrics=[
            Metric(name="sessions")
        ],
        order_bys=[
            OrderBy(dimension={"dimension_name": "year"}, desc=True),
            OrderBy(dimension={"dimension_name": "month"}, desc=True)
        ],
        date_ranges=[DateRange(start_date="2020-01-01", end_date="today")],
        limit=50000
    )

    monthly_response = client.run_report(monthly_request)

    # Process monthly data
    monthly_data = {}
    
    # Create all month-year combinations
    current_date = pd.Timestamp.now()
    start_date = pd.Timestamp('2020-01-01')
    date_range = pd.date_range(start=start_date, end=current_date, freq='M')
    
    # Initialize the dictionary with all possible months
    for date in date_range:
        month_key = f"{date.month:02d}, {date.year}"
        monthly_data[month_key] = {referral[0]: 0 for referral in top_referrals}

    # Fill in the actual data
    for row in monthly_response.rows:
        year = int(row.dimension_values[0].value)
        month = int(row.dimension_values[1].value)
        source = row.dimension_values[2].value
        sessions = int(row.metric_values[0].value)
        
        month_key = f"{month:02d}, {year}"
        if month_key in monthly_data and source in monthly_data[month_key]:
            monthly_data[month_key][source] = sessions

    # Create DataFrame
    df = pd.DataFrame.from_dict(monthly_data, orient='index')
    df.index.name = 'Month-Year'
    df = df.reset_index()

    # Sort by date (latest first)
    df['Sort_Date'] = pd.to_datetime(df['Month-Year'], format='%m, %Y')
    df = df.sort_values('Sort_Date', ascending=False)
    df = df.drop('Sort_Date', axis=1)

    # Filter out rows where all numeric columns are 0
    numeric_columns = df.columns.drop('Month-Year')
    df = df[~(df[numeric_columns] == 0).all(axis=1)]


    # Convert DataFrame to values
    values = [df.columns.tolist()] + df.values.tolist()

    # Update the sheet
    request = service.spreadsheets().values().update(
        spreadsheetId=config_obj["sheet_id"],
        range=f'{SHEET_TITLE}!A1',
        valueInputOption='RAW',
        body={'values': values}
    )
    response = request.execute()

    # Format header
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(config_obj["sheet_id"]).worksheet(SHEET_TITLE)
    
    format_requests = [{
        'repeatCell': {
            'range': {
                'sheetId': sheet.id,
                'startRowIndex': 0,
                'endRowIndex': 1
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
                    'textFormat': {'bold': True},
                    'wrapStrategy': 'WRAP',
                    'verticalAlignment': 'MIDDLE',
                    'horizontalAlignment': 'CENTER'
                }
            },
            'fields': 'userEnteredFormat(backgroundColor,textFormat,wrapStrategy,verticalAlignment,horizontalAlignment)'
        }
    }]

    service.spreadsheets().batchUpdate(
        spreadsheetId=config_obj["sheet_id"],
        body={'requests': format_requests}
    ).execute()



    return df




def add_top_referrals_chart(df):
    
    # Get sheet ID
    spreadsheet = service.spreadsheets().get(spreadsheetId=config_obj["sheet_id"]).execute()
    sheet_id = None
    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == SHEET_TITLE:
            sheet_id = sheet['properties']['sheetId']
            break
            
    if sheet_id is None:
        raise ValueError(f"Sheet '{SHEET_TITLE}' not found")

    # Get the number of columns
    num_columns = len(df.columns)

    # Create the chart
    chart = {
        'spec': {
            'title': 'Top Referral Sources Over Time',
            'basicChart': {
                'chartType': 'LINE',
                'legendPosition': 'RIGHT_LEGEND',
                'headerCount': 1,
                'axis': [
                    {'position': 'BOTTOM_AXIS', 'title': 'Month-Year'},
                    {
                        'position': 'LEFT_AXIS',
                        'title': 'Sessions'
                    }
                ],
                'domains': [{
                    'domain': {
                        'sourceRange': {
                            'sources': [{
                                'sheetId': sheet_id,
                                'startRowIndex': 0,
                                'endRowIndex': len(df) + 1,
                                'startColumnIndex': 0,
                                'endColumnIndex': 1
                            }]
                        }
                    },
                    'reversed': True
                }],
                'series': []
            }
        },
        'position': {
            'overlayPosition': {
                'anchorCell': {
                    'sheetId': sheet_id,
                    'rowIndex': 0,
                    'columnIndex': num_columns + 2
                },
                'widthPixels': 1200,
                'heightPixels': 600
            }
        }
    }

    # Colors for different referral sources
    colors = [
        {'red': 0.4, 'green': 0.4, 'blue': 1.0},  # Blue
        {'red': 1.0, 'green': 0.4, 'blue': 0.4},  # Red
        {'red': 0.4, 'green': 0.8, 'blue': 0.4},  # Green
        {'red': 1.0, 'green': 0.8, 'blue': 0.2},  # Yellow
        {'red': 0.8, 'green': 0.4, 'blue': 0.8},  # Purple
        {'red': 0.4, 'green': 0.8, 'blue': 1.0},  # Light Blue
        {'red': 1.0, 'green': 0.6, 'blue': 0.4},  # Orange
        {'red': 0.6, 'green': 0.4, 'blue': 0.2},  # Brown
        {'red': 0.8, 'green': 0.8, 'blue': 0.4},  # Light Yellow
        {'red': 0.4, 'green': 0.8, 'blue': 0.6}   # Teal
    ]

    # Add series for each referral source
    for idx, col in enumerate(df.columns[1:], start=1):
        series = {
            'series': {
                'sourceRange': {
                    'sources': [{
                        'sheetId': sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': len(df) + 1,
                        'startColumnIndex': idx,
                        'endColumnIndex': idx + 1
                    }]
                }
            },
            'targetAxis': 'LEFT_AXIS',
            'color': colors[idx - 1] if idx - 1 < len(colors) else colors[-1],
            'lineStyle': {'type': 'SOLID', 'width': 2}
        }
        chart['spec']['basicChart']['series'].append(series)

    # Add the chart
    chart_request = {
        'requests': [{
            'addChart': {
                'chart': chart
            }
        }]
    }

    service.spreadsheets().batchUpdate(
        spreadsheetId=config_obj["sheet_id"],
        body=chart_request
    ).execute()



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
    SHEET_TITLE = "Updated_AllDomains_Data"



    # Main execution
    df = create_combined_ga4_report()

    df_with_colors, color_mapping = add_color_formatting(df)
    export_to_google_sheets(df_with_colors, color_mapping)

    # Optional: Print the first few rows and color mapping
    #print(df_with_colors.head())
    #print("\nColor Mapping Legend:")
    #print("- Green shades: Performance above average (light to dark intensity)")
    #print("- Red shades: Performance below average (light to dark intensity)")
    #print("- White: Performance close to average")

    update_charts(df)
    print("\n ... FINISHED UPDATING sheet=%s" % (SHEET_TITLE))

    SHEET_TITLE = 'AllDomains_Top10Referrals'
    df = create_top_referrals_trend_report()
    print(" ... FINISHED UPDATING sheet=%s" % (SHEET_TITLE))

    add_top_referrals_chart(df)
    print(" ... FINISHED UPDATING CHART IN sheet=%s" % (SHEET_TITLE))


    SHEET_TITLE = 'AllDomains_Bottom10Pages'  # Changed sheet title
    create_bottom_pages_trend_report()
    print(" ... FINISHED UPDATING sheet=%s" % (SHEET_TITLE))



    return



if __name__ == '__main__':
    main()



