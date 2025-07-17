def get_user_metrics_chart_json(sheet_id, y_axis_min, y_axis_max, start_row_index, end_row_index):

    return {
    'spec': {
        'title': 'User Metrics Over Time for All Domains (GA4 Data)',
        'basicChart': {
            'chartType': 'LINE',
            'legendPosition': 'RIGHT_LEGEND',
            'headerCount': 1,
            'axis': [
                {'position': 'BOTTOM_AXIS', 'title': 'Month-Year'},
                {
                    'position': 'LEFT_AXIS',
                    'title': 'Count',
                    'viewWindowOptions': {
                        'viewWindowMin': y_axis_min,
                        'viewWindowMax': y_axis_max
                    }
                }
            ],
            'domains': [{
                'domain': {
                    'sourceRange': {
                        'sources': [{
                            'sheetId': sheet_id,
                            'startRowIndex': start_row_index,
                            'endRowIndex': end_row_index,
                            'startColumnIndex': 0,
                            'endColumnIndex': 1
                        }]
                    }
                },
                'reversed': True
            }],
            'series': [
                {
                    'series': {
                        'sourceRange': {
                            'sources': [{
                                'sheetId': sheet_id,
                                'startRowIndex': start_row_index,
                                'endRowIndex': end_row_index,
                                'startColumnIndex': 1,
                                'endColumnIndex': 2
                            }]
                        }
                    },
                    'targetAxis': 'LEFT_AXIS',
                    'color': {'red': 0.4, 'green': 0.4, 'blue': 1.0},
                    'lineStyle': {'type': 'SOLID'}
                },
                {
                    'series': {
                        'sourceRange': {
                            'sources': [{
                                'sheetId': sheet_id,
                                'startRowIndex': start_row_index,
                                'endRowIndex': end_row_index,
                                'startColumnIndex': 2,
                                'endColumnIndex': 3
                            }]
                        }
                    },
                    'targetAxis': 'LEFT_AXIS',
                    'color': {'red': 1.0, 'green': 0.4, 'blue': 0.4},
                    'lineStyle': {'type': 'SOLID'}
                },
                {
                    'series': {
                        'sourceRange': {
                            'sources': [{
                                'sheetId': sheet_id,
                                'startRowIndex': start_row_index,
                                'endRowIndex': end_row_index,
                                'startColumnIndex': 3,
                                'endColumnIndex': 4
                            }]
                        }
                    },
                    'targetAxis': 'LEFT_AXIS',
                    'color': {'red': 1.0, 'green': 0.8, 'blue': 0.2},
                    'lineStyle': {'type': 'SOLID'}
                },
                {
                    'series': {
                        'sourceRange': {
                            'sources': [{
                                'sheetId': sheet_id,
                                'startRowIndex': start_row_index,
                                'endRowIndex': end_row_index,
                                'startColumnIndex': 4,
                                'endColumnIndex': 5
                            }]
                        }
                    },
                    'targetAxis': 'LEFT_AXIS',
                    'color': {'red': 0.2, 'green': 0.8, 'blue': 0.2},
                    'lineStyle': {'type': 'SOLID'}
                }
            ]
        }
    },
    'position': {
        'overlayPosition': {
            'anchorCell': {'sheetId': sheet_id, 'rowIndex': 0, 'columnIndex': 11},
            'widthPixels': 1200,
            'heightPixels': 600
        }
    }
    }


def get_traffic_chart_json(sheet_id, start_row_index, end_row_index):

    return{
    'spec': {
        'title': 'Traffic Sources Distribution Over Time',
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
                            'startRowIndex': start_row_index,
                            'endRowIndex': end_row_index,
                            'startColumnIndex': 0,
                            'endColumnIndex': 1
                        }]
                    }
                },
                'reversed': True
            }],
            'series': [
                {
                    'series': {
                        'sourceRange': {
                            'sources': [{
                                'sheetId': sheet_id,
                                'startRowIndex': start_row_index,
                                'endRowIndex': end_row_index,
                                'startColumnIndex': 7,
                                'endColumnIndex': 8
                            }]
                        }
                    },
                    'targetAxis': 'LEFT_AXIS',
                    'color': {'red': 0.2, 'green': 0.6, 'blue': 1.0},
                    'lineStyle': {'type': 'SOLID'}
                },
                {
                    'series': {
                        'sourceRange': {
                            'sources': [{
                                'sheetId': sheet_id,
                                'startRowIndex': start_row_index,
                                'endRowIndex': end_row_index,
                                'startColumnIndex': 8,
                                'endColumnIndex': 9
                            }]
                        }
                    },
                    'targetAxis': 'LEFT_AXIS',
                    'color': {'red': 0.8, 'green': 0.4, 'blue': 0.0},
                    'lineStyle': {'type': 'SOLID'}
                },
                {
                    'series': {
                        'sourceRange': {
                            'sources': [{
                                'sheetId': sheet_id,
                                'startRowIndex': start_row_index,
                                'endRowIndex': end_row_index,
                                'startColumnIndex': 9,
                                'endColumnIndex': 10
                            }]
                        }
                    },
                    'targetAxis': 'LEFT_AXIS',
                    'color': {'red': 0.6, 'green': 0.4, 'blue': 0.8},
                    'lineStyle': {'type': 'SOLID'}
                }
            ]
        }
    },
    'position': {
        'overlayPosition': {
            'anchorCell': {'sheetId': sheet_id, 'rowIndex': 30, 'columnIndex': 11},
            'widthPixels': 1200,
            'heightPixels': 600
        }
    }
    }




def get_top_pages_chart_json(sheet_id, num_rows, num_columns):

    return {
        'spec': {
            'title': 'Top Pages Views Over Time',
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
                    'rowIndex': 0,
                    'columnIndex': num_columns + 3
                },
                'widthPixels': 900,
                'heightPixels': 520
            }
        }
    }

