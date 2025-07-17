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





def create_top_countries_report():

    # Get top 10 countries data
    request = RunReportRequest(
        property='properties/' + config_obj["property_id"],
        dimensions=[
            Dimension(name="country")
        ],
        metrics=[
            Metric(name="screenPageViews")
        ],
        order_bys=[
            OrderBy(metric={"metric_name": "screenPageViews"}, desc=True)
        ],
        limit=10,
        date_ranges=[DateRange(start_date="2023-04-01", end_date="today")]
    )

    response = client.run_report(request)

    # Process data into DataFrame
    data = []
    for row in response.rows:
        country = row.dimension_values[0].value
        pageviews = int(row.metric_values[0].value)
        data.append([country, pageviews])

    df = pd.DataFrame(data, columns=["Country", "Pageviews"])

    gc = gspread.authorize(creds)
    
    
    # Create new sheet if it doesn't exist
    try:
        worksheet = gc.open_by_key(config_obj["sheet_id"]).worksheet(SHEET_TITLE)
        worksheet.clear()
    except:
        worksheet = gc.open_by_key(config_obj["sheet_id"]).add_worksheet(SHEET_TITLE, rows=100, cols=20)

    # Convert DataFrame to values
    values = [df.columns.tolist()] + df.values.tolist()
    
    # Update sheet
    worksheet.update(values, "A1")

    # Apply conditional formatting to pageviews column
    format_request = {
        'requests': [{
            'addConditionalFormatRule': {
                'rule': {
                    'ranges': [{
                        'sheetId': worksheet.id,
                        'startRowIndex': 1,
                        'endRowIndex': len(values),
                        'startColumnIndex': 1,
                        'endColumnIndex': 2
                    }],
                    'gradientRule': {
                        'minpoint': {'color': {'red': 0.839, 'green': 0.404, 'blue': 0.404}, 'type': 'MIN'},
                        'midpoint': {'color': {'red': 1, 'green': 1, 'blue': 1}, 'type': 'PERCENTILE', 'value': '50'},
                        'maxpoint': {'color': {'red': 0.420, 'green': 0.655, 'blue': 0.420}, 'type': 'MAX'}
                    }
                }
            }
        }]
    }
    
    service.spreadsheets().batchUpdate(
        spreadsheetId=config_obj["sheet_id"],
        body=format_request
    ).execute()

    return






def create_top_countries_report_monthly():
    try:
        # First get top 10 countries overall
        top_countries_request = RunReportRequest(
            property='properties/' + config_obj["property_id"],
            dimensions=[
                Dimension(name="country")
            ],
            metrics=[
                Metric(name="engagedSessions")
            ],
            order_bys=[
                OrderBy(metric={"metric_name": "engagedSessions"}, desc=True)
            ],
            limit=10,
            date_ranges=[DateRange(start_date="2023-04-01", end_date="today")]
        )

        top_countries_response = client.run_report(top_countries_request)
        top_countries = [row.dimension_values[0].value for row in top_countries_response.rows]

        # Then get monthly data for these countries
        monthly_request = RunReportRequest(
            property='properties/' + config_obj["property_id"],
            dimensions=[
                Dimension(name="year"),
                Dimension(name="month"),
                Dimension(name="country")
            ],
            metrics=[
                Metric(name="engagedSessions")
            ],
            order_bys=[
                OrderBy(dimension={"dimension_name": "year"}, desc=True),
                OrderBy(dimension={"dimension_name": "month"}, desc=True)
            ],
            date_ranges=[DateRange(start_date="2023-04-01", end_date="today")]
        )

        monthly_response = client.run_report(monthly_request)

        # Process into a dictionary with flattened structure
        monthly_data = {}
        total_monthly_sessions = {}
        
        for row in monthly_response.rows:
            year = int(row.dimension_values[0].value)
            month = int(row.dimension_values[1].value)
            country = row.dimension_values[2].value
            
            if country in top_countries:
                month_key = f"{month:02d}, {year}"
                if month_key not in monthly_data:
                    monthly_data[month_key] = {country: 0 for country in top_countries}
                    total_monthly_sessions[month_key] = 0
                
                sessions = int(row.metric_values[0].value)
                monthly_data[month_key][country] = sessions
                total_monthly_sessions[month_key] += sessions

        # Create DataFrame
        df = pd.DataFrame.from_dict(monthly_data, orient='index')
        df['Total Engaged Sessions'] = pd.Series(total_monthly_sessions)
        df.index.name = 'Month-Year'
        df = df.reset_index()

        # Sort by date
        df['Sort_Date'] = pd.to_datetime(df['Month-Year'], format='%m, %Y')
        df = df.sort_values('Sort_Date', ascending=False)
        df = df.drop('Sort_Date', axis=1)

        # Reorder columns to put Total first
        cols = ['Month-Year', 'Total Engaged Sessions'] + [col for col in df.columns if col not in ['Month-Year', 'Total Engaged Sessions']]
        df = df[cols]

        # Export to Google Sheets
        gc = gspread.authorize(creds)

        # Create or get worksheet
        try:
            worksheet = gc.open_by_key(config_obj["sheet_id"]).worksheet(SHEET_TITLE)
        except:
            worksheet = gc.open_by_key(config_obj["sheet_id"]).add_worksheet(SHEET_TITLE, rows=100, cols=20)

        # Clear existing content
        worksheet.clear()

        # Update values
        values = [df.columns.tolist()] + df.values.tolist()
        worksheet.update(values, "A1",value_input_option='RAW')

        # Apply formatting
        format_requests = []
        
        # Add header formatting
        format_requests.append({
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
        })

        # Add gradient conditional formatting for numeric columns
        for col_idx in range(1, len(df.columns)):
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

        # Execute formatting requests
        format_request = {'requests': format_requests}
        service.spreadsheets().batchUpdate(
            spreadsheetId=config_obj["sheet_id"],
            body=format_request
        ).execute()

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

        # Add chart
        chart_request = {
            'requests': [{
                'addChart': {
                    'chart': {
                        'spec': {
                            'title': 'Top Countries Engagement Over Time',
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
                                        'title': 'Engaged Sessions'
                                    }
                                ],
                                'domains': [{
                                    'domain': {
                                        'sourceRange': {
                                            'sources': [{
                                                'sheetId': worksheet.id,
                                                'startRowIndex': 0,
                                                'endRowIndex': len(values),
                                                'startColumnIndex': 0,
                                                'endColumnIndex': 1
                                            }]
                                        }
                                    },
                                    'reversed': True  # This will show old to new dates
                                }],
                                'series': [
                                    {
                                        'series': {
                                            'sourceRange': {
                                                'sources': [{
                                                    'sheetId': worksheet.id,
                                                    'startRowIndex': 0,
                                                    'endRowIndex': len(values),
                                                    'startColumnIndex': idx + 1,
                                                    'endColumnIndex': idx + 2
                                                }]
                                            }
                                        },
                                        'targetAxis': 'LEFT_AXIS',
                                        'lineStyle': {'width': 2},
                                        'color': colors[idx % len(colors)]  # Add different colors for each line
                                    } for idx in range(len(top_countries))
                                ]
                            }
                        },
                        'position': {
                            'overlayPosition': {
                                'anchorCell': {
                                    'sheetId': worksheet.id,
                                    'rowIndex': 0,
                                    'columnIndex': len(df.columns) + 2  # Position chart after the table with padding
                                },
                                'widthPixels': 1200,
                                'heightPixels': 600
                            }
                        }
                    }
                }
            }]
        }

        # Execute chart request
        service.spreadsheets().batchUpdate(
            spreadsheetId=config_obj["sheet_id"],
            body=chart_request
        ).execute()

    except Exception as e:
        print(f"Error creating report: {str(e)}")



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
    SHEET_TITLE = 'Top10Countries'

    create_top_countries_report()
    print("\n ... FINISHED UPDATING sheet=%s" % (SHEET_TITLE))


    SHEET_TITLE = "Top10Countries_Monthly"
    create_top_countries_report_monthly()
    print(" ... FINISHED UPDATING sheet=%s" % (SHEET_TITLE))




    return



if __name__ == '__main__':
    main()



