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
                        string_filter=Filter.StringFilter(value="glygen.org",
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
                    value="wiki.glygen.org",
                    match_type=Filter.StringFilter.MatchType.EXACT
                )
            )
        )

    return subdomain_filter


def create_glygen_top_referrals_trend_report():
 
    subdomain_filter = get_subdomain_filter()

    top_referrals_request = RunReportRequest(
        property=f'properties/{config_obj["property_id"]}',
        dimensions=[
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium")
        ],
        metrics=[Metric(name="sessions")],
        order_bys=[OrderBy(metric={"metric_name": "sessions"}, desc=True)],
        limit=10,
        date_ranges=[DateRange(start_date="2023-04-01", end_date="today")],
        dimension_filter=FilterExpression(
            and_group=FilterExpressionList(
                expressions=[
                    FilterExpression(
                        filter=Filter(
                            field_name="sessionMedium",
                            string_filter=Filter.StringFilter(value="referral", match_type=Filter.StringFilter.MatchType.EXACT)
                        )
                    ),
                    subdomain_filter  # Apply hostname filter
                ]
            )
        )
    )

    top_referrals_response = client.run_report(top_referrals_request)
    top_referrals = [(row.dimension_values[0].value, row.dimension_values[1].value) 
                     for row in top_referrals_response.rows]

    # Second request: Get monthly data for these referral sources
    monthly_request = RunReportRequest(
        property=f'properties/{config_obj["property_id"]}',
        dimensions=[
            Dimension(name="year"),
            Dimension(name="month"),
            Dimension(name="sessionSource")
        ],
        metrics=[Metric(name="sessions")],
        order_bys=[
            OrderBy(dimension={"dimension_name": "year"}, desc=True),
            OrderBy(dimension={"dimension_name": "month"}, desc=True)
        ],
        date_ranges=[DateRange(start_date="2023-04-01", end_date="today")],
        limit=50000,
        dimension_filter=subdomain_filter  # Apply hostname filter
    )

    monthly_response = client.run_report(monthly_request)

    # Process monthly data
    monthly_data = {}
    
    # Create all month-year combinations
    current_date = pd.Timestamp.now()
    start_date = pd.Timestamp('2023-04-01')
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

    # Apply conditional formatting
    df_with_colors, color_mapping = add_color_formatting(df)  # Reuse the existing function


    # Check if sheet exists, if not, create it
    gc = gspread.authorize(creds)
    try:
        sheet = gc.open_by_key(config_obj["sheet_id"]).worksheet(SHEET_TITLE)
    except gspread.exceptions.WorksheetNotFound:
        sheet = gc.open_by_key(config_obj["sheet_id"]).add_worksheet(title=SHEET_TITLE, rows="100", cols="20")

    # Convert DataFrame to values for Google Sheets
    values = [df_with_colors.columns.tolist()] + df_with_colors.values.tolist()

    # Update the sheet
    sheet.clear()
    sheet.update(values, 'A1')

    # Apply color formatting in Google Sheets
    batch_update_requests = []
    for col_idx, col_name in enumerate(df_with_colors.columns[1:], start=2):  # Skip first column
        if col_name in color_mapping:
            batch_update_requests.append({
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
            })

    # Execute batch update for conditional formatting
    if batch_update_requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=config_obj["sheet_id"],
            body={'requests': batch_update_requests}
        ).execute()

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

    SHEET_TITLE = config_obj["tabs"]["top10referrals"][module]["sheet_title"]



    create_glygen_top_referrals_trend_report()
    print("\n ... FINISHED UPDATING sheet=%s" % (SHEET_TITLE))


    return



if __name__ == '__main__':
    main()

