import os,sys
import json

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest, OrderBy, Filter, FilterExpression, FilterExpressionList
)
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread
from optparse import OptionParser



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


def get_subdomain_filter():

    subdomain_filter = FilterExpression(
        or_group=FilterExpressionList(
            expressions=[
                FilterExpression(
                    filter=Filter(
                        field_name="hostname",
                        string_filter=Filter.StringFilter(
                            value="glygen.org",
                            match_type=Filter.StringFilter.MatchType.EXACT
                        )
                    )
                ),
                FilterExpression(
                    filter=Filter(
                        field_name="hostname",
                        string_filter=Filter.StringFilter(
                            value="www.glygen.org",
                            match_type=Filter.StringFilter.MatchType.EXACT
                        )
                    )
                )
            ]
        )
    )
    
    if module != "portal":
        subdomain_filter = FilterExpression(
            filter=Filter(
                field_name="hostname",
                string_filter=Filter.StringFilter(
                    value="%s.glygen.org" % (module),
                    match_type=Filter.StringFilter.MatchType.EXACT
                )
            )
        )

    return subdomain_filter





def get_glygen_top_pages_overview():

    subdomain_filter = get_subdomain_filter()

    top_pages_request = RunReportRequest(
        property=f'properties/' + config_obj["property_id"],
        dimensions=[
            Dimension(name="pagePath"),
            Dimension(name="pageTitle")
        ],
        metrics=[Metric(name="screenPageViews")],
        order_bys=[OrderBy(metric={"metric_name": "screenPageViews"}, desc=True)],
        limit=100000,
        date_ranges=[DateRange(start_date="2023-04-01", end_date="today")],
        dimension_filter=subdomain_filter  # Apply hostname filter
    )

    top_pages_response = client.run_report(top_pages_request)

    # Process path mapping
    path_mapping = {}
    for row in top_pages_response.rows:
        path = row.dimension_values[0].value
        normalized_path = path.rstrip('/')

        # Handle special cases and duplicates
        if path in ["/", "/home", "/home/"]:
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
    consolidated_paths = [info['path'] for info in sorted(path_mapping.values(), key=lambda x: x['views'], reverse=True)][:20]

    # Second request: Get monthly data for these top pages
    monthly_request = RunReportRequest(
        property=f'properties/' + config_obj["property_id"],
        dimensions=[
            Dimension(name="year"),
            Dimension(name="month"),
            Dimension(name="pagePath")
        ],
        metrics=[Metric(name="screenPageViews")],
        order_bys=[
            OrderBy(dimension={"dimension_name": "year"}, desc=True),
            OrderBy(dimension={"dimension_name": "month"}, desc=True)
        ],
        date_ranges=[DateRange(start_date="2023-04-01", end_date="today")],
        limit=100000,
        offset=0,
        dimension_filter=subdomain_filter  # Apply hostname filter
    )

    # Get all data by handling pagination
    all_rows = []
    while True:
        response = client.run_report(monthly_request)
        all_rows.extend(response.rows)
        if len(response.rows) < 100000:
            break
        monthly_request.offset = len(all_rows)

    # Process monthly data
    monthly_data = {}
    total_monthly_views = {}

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
        if normalized_path in ["/", "/home"]:
            normalized_path = "/"
            monthly_data[month_key]["/"] += views
        elif normalized_path.startswith("/glycan-search"):
            monthly_data[month_key]["/glycan-search/"] += views
        elif normalized_path.startswith("/protein-search"):
            monthly_data[month_key]["/protein-search/"] += views
        elif page_path in consolidated_paths:
            monthly_data[month_key][page_path] += views

        total_monthly_views[month_key] += views

    # Create DataFrame
    df = pd.DataFrame.from_dict(monthly_data, orient='index')
    df['Total Pageviews'] = pd.Series(total_monthly_views)
    df.index.name = 'Month-Year'
    df = df.reset_index()

    # Sort by date
    df['Sort_Date'] = pd.to_datetime(df['Month-Year'], format='%m, %Y')
    df = df.sort_values('Sort_Date', ascending=False)
    df = df.drop('Sort_Date', axis=1)

    # Apply conditional formatting
    df_with_colors, color_mapping = add_color_formatting(df)  # Reuse function

    # Export to Google Sheets
    gc = gspread.authorize(creds)


    try:
        sheet = gc.open_by_key(config_obj["sheet_id"]).worksheet(SHEET_TITLE)
    except gspread.exceptions.WorksheetNotFound:
        sheet = gc.open_by_key(config_obj["sheet_id"]).add_worksheet(title=SHEET_TITLE, rows="100", cols="20")

    # Convert DataFrame to values
    values = [df_with_colors.columns.tolist()] + df_with_colors.values.tolist()

    # Update sheet
    sheet.clear()
    sheet.update(values, "A1")

    # Apply color formatting in Google Sheets
    batch_update_requests = []
    for col_idx, col_name in enumerate(df_with_colors.columns[1:], start=2):  # Skip first column
        if col_name in color_mapping:
            batch_update_requests.append({
                'addConditionalFormatRule': {
                    'rule': {
                        'ranges': [{
                            'sheetId': sheet.id,
                            'startRowIndex': 1,
                            'startColumnIndex': col_idx - 1,
                            'endColumnIndex': col_idx
                        }],
                        'gradientRule': {
                            'minpoint': {'color': {'red': 0.839, 'green': 0.404, 'blue': 0.404}, 'type': 'MIN'},
                            'midpoint': {'color': {'red': 1, 'green': 1, 'blue': 1}, 'type': 'PERCENTILE', 'value': '50'},
                            'maxpoint': {'color': {'red': 0.420, 'green': 0.655, 'blue': 0.420}, 'type': 'MAX'}
                        }
                    }
                }
            })

    if batch_update_requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=config_obj["sheet_id"],
            body={'requests': batch_update_requests}
        ).execute()



def add_top_glygen_pages_chart():

    
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

    # Adjust column width for better visualization
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

    # Define colors for the top pages
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
    chart = {
        'spec': {
            'title': 'Top Pages Views Over Time',  # Updated chart title
            'basicChart': {
                'chartType': 'LINE',
                'legendPosition': 'RIGHT_LEGEND',
                'headerCount': 1,
                'axis': [
                    {
                        'position': 'BOTTOM_AXIS',
                        'title': 'Month-Year'
                    },
                    {
                        'position': 'LEFT_AXIS',
                        'title': 'Page Views'
                    }
                ],
                'domains': [{
                    'domain': {
                        'sourceRange': {
                            'sources': [{
                                'sheetId': sheet_id,
                                'startRowIndex': 0,
                                'endRowIndex': num_rows,
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
                    'rowIndex': num_rows + 1,  # Adjusted row index to place the chart below the table
                    'columnIndex': 0
                },
                'widthPixels': 900,
                'heightPixels': 520
            }
        }
    }

    # Add series for the top 10 pages (columns 1 to 11, including Total Pageviews)
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
        chart['spec']['basicChart']['series'].append(series)

    # Create the chart request
    chart_request = {
        'requests': [{
            'addChart': {
                'chart': chart
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
    parser.add_option("-m","--modudle",action="store",dest="module",help="portal/wiki/beta/data/api")


    (options,args) = parser.parse_args()
    for file in ([options.domain, options.module]):
        if not (file):
            parser.print_help()
            sys.exit(0)

    global config_obj
    global client
    global creds
    global service
    global SHEET_TITLE
    global domain
    global module


    domain = options.domain
    module = options.module
    

    config_obj = json.load(open("conf/config.%s.json" % (domain)))
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "conf/credentials.%s.json" % (domain)
    credentials_file = "conf/credentials.%s.json" % (domain)
    # Initialize the GA4 client
    client = BetaAnalyticsDataClient()
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(credentials_file, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)

    SHEET_TITLE = config_obj["tabs"]["top20pages"][module]["sheet_title"]


    get_glygen_top_pages_overview()
    print("\n ... FINISHED UPDATING sheet=%s" % (SHEET_TITLE))

    add_top_glygen_pages_chart()
    print(" ... FINISHED UPDATING CHART sheet=%s" % (SHEET_TITLE))

    return



if __name__ == '__main__':
    main()

