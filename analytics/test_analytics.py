import unittest

import pandas.testing
from pandas import Timestamp

import analytics
from analytics import *


class TestDataProcessing(unittest.TestCase):

    def test_group_data_by_device_id_and_hour(self):
        df = pd.DataFrame({'device_id': ['device_1', 'device_1', 'device_1'],
                           'time': [Timestamp('2023-06-18 03:00:00'), Timestamp('2023-06-18 03:40:00'),
                                    Timestamp('2023-06-18 04:00:00')]})
        df_grouped = group_data_by_device_id_and_hour(df)
        self.assertIsInstance(df_grouped, DataFrameGroupBy)
        self.assertEqual(len(df_grouped), 2)

    def test_get_max_temperature_per_group(self):
        df_grouped = pd.DataFrame({'device_id': ['device_1', 'device_1', 'device_1', 'device_2'],
                                   'temperature': [13, 14, 22, 13]}).groupby(['device_id'])
        max_temperatures = get_max_temperature_per_group(df_grouped).to_dict()
        self.assertEqual(len(max_temperatures), 2)
        self.assertEqual(max_temperatures['device_1'], 22)
        self.assertEqual(max_temperatures['device_2'], 13)

    def test_get_hourly_data_points_per_device(self):
        df_grouped = pd.DataFrame({'device_id': ['device_1', 'device_1', 'device_1', 'device_2']}).groupby(
            ['device_id'])
        data_point_counts = get_hourly_data_points_per_device(df_grouped)
        self.assertEqual(len(data_point_counts), 2)
        self.assertEqual(data_point_counts['device_1'], 3)
        self.assertEqual(data_point_counts['device_2'], 1)

    def test_calculate_distance_between_two_points(self):
        row = pd.DataFrame({
            'latitude': [18.5196, 17.2894, 16.7050],
            'longitude': [73.8554, 74.1818, 74.2433]
        })
        total_distance = calculate_distance(row)
        self.assertEqual(total_distance, 205.487)

    def test_calculate_distance_if_single_point(self):
        row = pd.DataFrame({
            'latitude': [18.5196],
            'longitude': [73.8554]
        })
        total_distance = calculate_distance(row)
        self.assertEqual(total_distance, 0)

    def test_format_column_types(self):
        df = pd.DataFrame({'device_id': ['device_1'],
                           'temperature': [42],
                           'location': ['{"latitude": "-23.382787", "longitude": "74.578627"}'],
                           'time': ['1687060684']})
        formatted_df = format_column_types(df)

        self.assertEqual(formatted_df['time'].dtype, 'datetime64[ns]')
        self.assertEqual(formatted_df['latitude'].dtype, float)
        self.assertEqual(formatted_df['longitude'].dtype, float)

        result_row = formatted_df.iloc[0]
        self.assertEqual(result_row['latitude'], -23.382787)
        self.assertEqual(result_row['longitude'], 74.578627)
        self.assertEqual(result_row['time'], Timestamp('2023-06-18 03:58:04'))

    def test_process_data(self):
        devices_test_data = r'''
        {
          "columns": [
            "device_id",
            "temperature",
            "location",
            "time"
          ],
          "data": [
            [
              "device_1",
              42,
              "{\"latitude\": \"-23.3827865\", \"longitude\": \"74.578627\"}",
              "1687060684"
            ],
            [
              "device_1",
              38,
              "{\"latitude\": \"-23.3827873\", \"longitude\": \"74.578628\"}",
              "1687061284"
            ],
            [
              "device_2",
              35,
              "{\"latitude\": \"18.5204\", \"longitude\": \"73.8567\"}",
              "1687060684"
            ],
            [
              "device_2",
              40,
              "{\"latitude\": \"19.0760\", \"longitude\": \"72.8777\"}",
              "1687060690"
            ]
          ]
        }
        '''
        test_data = pd.DataFrame(json.loads(devices_test_data)['data'],
                                 columns=['device_id', 'temperature', 'location', 'time'])
        expected_df = pd.DataFrame({'device_id': ['device_1', 'device_1', 'device_2'],
                                    'time': [Timestamp('2023-06-18 03:00:00'), Timestamp('2023-06-18 04:00:00'),
                                             Timestamp('2023-06-18 03:00:00')],
                                    'Max Temperature': [42, 38, 40],
                                    'Data Point Count': [1, 1, 2],
                                    'Total Distance (km)': [0, 0, 120.138]})

        result = analytics.process_data(test_data)
        pandas.testing.assert_frame_equal(result, expected_df)


if __name__ == '__main__':
    unittest.main()
