import unittest
from datetime import datetime
import pandas as pd
from Controller import WebpageNode, DataExtractor, extract_revenue_data_from_tsv, lambda_handler
from pandas.testing import assert_frame_equal
from moto import mock_s3

@mock_s3
class TestRevenue(unittest.TestCase):
    '''Test cases'''
    extractor= DataExtractor()

    # Test case to search domain and search key
    def test_extract_domain_key(self):
        extractor= DataExtractor()
        referer = "http://www.google.com/search?hl=en&client=firefox-a&rls=org.mozilla%3Aen-US%3Aofficial&hs=Zk5&q=ipod&aq=f&oq=&aqi=g-p1g9"
        domain, search_key = extractor.extract_domain_key(referer)
        self.assertEqual(domain, "google.com")
        self.assertEqual(search_key, "ipod")

        referer = "http://www.bing.com/search?q=zune&go=&form=QBLH&qs=n"
        domain, search_key = extractor.extract_domain_key(referer)
        self.assertEqual(domain, "bing.com")
        self.assertEqual(search_key, "zune")

        referer = "http://search.yahoo.com/search?p=cd+player&toggle=1&cop=mss&ei=UTF-8&fr=yfp-t-701"
        domain, search_key = extractor.extract_domain_key(referer)
        self.assertEqual(domain, "yahoo.com")
        self.assertEqual(search_key, "cd player")

    # Test case to search revenue
    def test_extract_revenue(self):
        extractor = DataExtractor()
        expected_revenue = 10.0
        row = {'product_list': 'product;category;event;10', 'event_list': '1,12,11'}
        actual_revenue = extractor.extract_revenue(row)
        self.assertEqual(actual_revenue, expected_revenue)

    def test_extract_revenue(self):
        extractor = DataExtractor()
        expected_revenue = 0.0
        row = {'product_list': 'product;category;event;0', 'event_list': '1,12,11'}
        actual_revenue = extractor.extract_revenue(row)
        self.assertEqual(actual_revenue, expected_revenue)

    def test_extract_revenue(self):
        extractor = DataExtractor()
        expected_revenue = 0.0
        row = {'product_list': 'product;category;event;10', 'event_list': 1}
        actual_revenue = extractor.extract_revenue(row)
        self.assertEqual(actual_revenue, expected_revenue)

    # Test case to extract date
    def test_timestamp_value(self):
        extractor = DataExtractor()
        expected_date = '2022-10-22'
        date_time = '10/22/22 6:33'
        actual_date = extractor.timestamp_value(date_time)
        self.assertEqual(actual_date, expected_date)

    # Test case to generate report based on dates
    def setUp(self):
        data = {'date_time':'01/01/22 6:33',  'ip': '121.12.12.12', 'event_list' : ['1'], 'product_list': '1;2;3;10;5', 'page_url': 'www.googl.com', 'referrer': 'http://www.google.com/search?hl=en&client=firefox-a&rls=org.mozilla%3Aen-US%3Aofficial&hs=ZzP&q=ipod&aq=f&oq=&aqi='}
        self.data = pd.DataFrame(data)

    def test_extract_revenue_data_from_tsv(self):

        expected_output = pd.DataFrame({'Search Engine Domain': ["google.com"],
                                        'Search Keyword': ['ipod'],
                                        'Revenue': [0.0]},
                                       index=[0])

        expected_dates = {'2022-01-01'}

        actual_output, dates = extract_revenue_data_from_tsv(self.data)
        for formatted_date in dates:
            date_to_filter = pd.to_datetime(formatted_date).strftime('%Y-%m-%d')
            actual_output = actual_output[actual_output['date'] == date_to_filter]
            # Calculate total revenue generated based on search engine and keyword
            actual_output = actual_output.groupby(['Search Engine Domain', 'Search Keyword'])['Revenue'].sum()
            actual_output = actual_output.reset_index()

        assert_frame_equal(actual_output, expected_output)
        self.assertEqual(dates, expected_dates)


if __name__ == '__main__':
    unittest.main()
