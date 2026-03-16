import unittest
from unittest.mock import patch, mock_open, MagicMock
from datetime import datetime as dt, timedelta
import search
import json

class TestSearchCommands(unittest.TestCase):
    
    @patch('search.requests.get')
    def test_fetch_gfz_forecast_csv(self, mock_get):
        csv_data = (
            "Time (UTC),minimum,0.25-quantile,median,0.75-quantile\n"
            "13-03-2026 09:00,x,x,5.33,x\n"
            "15-03-2026 23:45,x,x,2.33,x\n"
            "16-03-2026 01:20,x,x,1.33,x\n"
            "\n"
        )
        mock_response = MagicMock()
        mock_response.text = csv_data
        mock_get.return_value = mock_response
        
        res = search.fetch_gfz_forecast_csv()
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0]['datetime'], '2026-03-13T09:00:00Z')
        self.assertEqual(res[0]['kp'], 5.33)
        
        # 23:45 nearest is 23. If closest == 23 -> becomes 0 next day
        self.assertEqual(res[1]['datetime'], '2026-03-16T00:00:00Z')
        self.assertEqual(res[1]['kp'], 2.33)
        
        # 01:20 nearest is 3, but wait! date_time_obj.hour == 1 -> closest becomes 0
        self.assertEqual(res[2]['datetime'], '2026-03-16T00:00:00Z')
        self.assertEqual(res[2]['kp'], 1.33)
        
    @patch('search.requests.get')
    def test_fetch_gfz_realtime_json(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "datetime": ["2026-03-15T00:00:00Z", "2026-03-15T03:00:00Z"],
            "Kp": [4.0, 3.0]
        }
        mock_get.return_value = mock_response
        
        res = search.fetch_gfz_realtime_json()
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0], {'datetime': '2026-03-15T00:00:00Z', 'kp': 4.0})
        self.assertEqual(res[1], {'datetime': '2026-03-15T03:00:00Z', 'kp': 3.0})
        
    @patch('search.requests.get')
    def test_fetch_noaa_27day_outlook(self, mock_get):
        text_data = (
            ":Issued: 2026 Mar 15 1200 UTC\n"
            "2026 Mar 16  3  3  3\n"
            "ignore this line\n"
        )
        mock_response = MagicMock()
        mock_response.text = text_data
        mock_get.return_value = mock_response
        
        res = search.fetch_noaa_27day_outlook()
        self.assertEqual(len(res), 8)
        self.assertEqual(res[0]['kp'], 3)
        self.assertEqual(res[0]['datetime'], '2026-03-16T00:00:00Z')
        self.assertEqual(res[7]['datetime'], '2026-03-16T21:00:00Z')
        
    def test_merge_kp_data(self):
        # Empty inputs
        res_empty = search.merge_kp_data([], [])
        self.assertEqual(res_empty, [])
        
        hour_data = [{"datetime": "2026-03-15T00:00:00Z", "dt_obj": dt.fromisoformat("2026-03-15T00:00:00")}]
        daily_data = [
            {"datetime": "2026-03-14T00:00:00Z"}, # Too early
            {"datetime": "2026-03-16T00:00:00Z", "val": 3.0}, # Kept
            {"datetime": "2026-03-25T00:00:00Z"}  # Too late
        ]
        
        res = search.merge_kp_data([{"datetime": "2026-03-15T00:00:00Z", "kp": 1.0}], daily_data)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]['kp'], 1.0)
        self.assertEqual(res[1]['val'], 3.0)
        
    @patch('builtins.open', new_callable=mock_open)
    @patch('search.fetch_gfz_forecast_csv')
    @patch('search.fetch_gfz_realtime_json')
    @patch('search.fetch_noaa_27day_outlook')
    @patch('search.merge_kp_data')
    def test_get_kp_pipeline(self, mock_merge, mock_noaa, mock_realtime, mock_forecast, mock_open_file):
        mock_forecast.return_value = [{"datetime": "2026-03-15T00:00:00Z", "kp": 1.0}, {"datetime": "2026-03-15T06:00:00Z", "kp": 1.5}]
        mock_realtime.return_value = [{"datetime": "2026-03-15T00:00:00Z", "kp": 2.0}, {"datetime": "2026-03-15T03:00:00Z", "kp": 3.0}]
        mock_noaa.return_value = []
        
        mock_merge.return_value = [{"datetime": "2026-03-15T00:00:00Z", "kp": 2.0}]
        
        search.get_kp_pipeline()
        
        mock_open_file.assert_called_with('new_kp.json', 'w')
        mock_merge.assert_called_once()
        
        site1_modified_arg = mock_merge.call_args[0][0]
        self.assertEqual(len(site1_modified_arg), 3)
        self.assertEqual(site1_modified_arg[0]['kp'], 2.0)
        self.assertEqual(site1_modified_arg[1]['kp'], 3.0)
        self.assertEqual(site1_modified_arg[2]['kp'], 1.5)

if __name__ == '__main__':
    unittest.main()
