import unittest


class TestUtil(unittest.TestCase):
    """
    - create_project():
      - calls existing_project.delete()
      - POSTs '{conn.host}/api/projects/'
      - returns a Project()
    - delete_forecast():
      - calls existing_forecast.delete()
    - delete_model():
        - calls model.delete()
    - ! upload_forecast():
      - calls model.upload_forecast()
      - calls busy_poll_upload_file_job()
    - ! upload_forecast_batch():
      - validates list args (counts)
      - can take filename string or json_io_dict
      - calls model.upload_forecast() on each
      - returns the last upload_file_job
    - dataframe_from_json_io_dict():
      - input examples/docs-predictions.json
      - test output df
    """


    def test_xx(self):
        self.fail()  # todo xx


if __name__ == '__main__':
    unittest.main()
