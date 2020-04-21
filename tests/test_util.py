import unittest


class TestUtil(unittest.TestCase):
    """
    util tests to implement:
    - for any that search by project or model name: case where doesn't exist
    - create_project():
      - no existing project
      - calls existing_project.delete()
      - POSTs '{conn.host}/api/projects/'
      - returns a Project()
    - delete_forecast():
      - no existing project
      - no existing model
      - calls existing_forecast.delete()
    - delete_model():
      - no existing project
      - no existing model
      - calls model.delete()
    - ! upload_forecast():
      - no existing project
      - no existing model
      - calls model.upload_forecast()
      - calls busy_poll_upload_file_job()
    - ! upload_forecast_batch():
      - no existing project
      - no existing model
      - 'batch args had different lengths'
      - 'no forecasts to upload'
      - can take filename string or json_io_dict
      - calls model.upload_forecast() on each
      - returns the last upload_file_job
    - download_forecast():
      - no existing project
      - no existing model
      = 'forecast not found'
      - calls existing_forecast.data()
    - dataframe_from_json_io_dict():
      - input examples/docs-predictions.json
      - test output df
    """


    def test_utils(self):
        self.fail("todo")


if __name__ == '__main__':
    unittest.main()
