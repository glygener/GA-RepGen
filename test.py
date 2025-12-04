import os
import json
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, Dimension, Metric, DateRange,OrderBy


def main():

    domain = "glygen"
    config_obj = json.load(open("conf/config.%s.json" % (domain)))
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "conf/credentials.%s.json" % (domain)
    
    client = BetaAnalyticsDataClient()
    dim_list = [Dimension(name="hostname")]
    met_list = [Metric(name="screenPageViews"), Metric(name="engagedSessions"),Metric(name="totalUsers")]
    ordr_list = [OrderBy(metric={"metric_name": "screenPageViews"}, desc=True)]
    date_list = [DateRange(start_date="2025-12-01", end_date="today")]
    request = RunReportRequest(
        property= "properties/" + config_obj["property_id"],dimensions=dim_list,
        metrics=met_list,order_bys=ordr_list,date_ranges=date_list
    )
    response = client.run_report(request)
    for row in response.rows:
        hostname = row.dimension_values[0].value
        pageviews = int(row.metric_values[0].value)
        sessions = int(row.metric_values[1].value)
        users = int(row.metric_values[2].value)
        newrow = [hostname, pageviews, sessions, users]
        print (newrow)
    
    return



if __name__ == "__main__":
    main()

