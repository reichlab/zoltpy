from abc import ABC

import requests


class ZoltarConnection:

    # notes:
    # - incomplete - ZoltarResource children only have a few properties implemented
    # - authentication approach is quick-and-dirty
    # - caches resource json, but doesn't automatically handle becoming stale, refreshing, etc.
    # - no back-pointers are stored, e.g., Model -> owning Project

    def __init__(self, host='http://zoltardata.com'):
        self.host = host
        self.username, self.password = None, None
        self.session = None

    def authenticate(self, username, password):
        self.username, self.password = username, password
        self.session = ZoltarSession(self)

    @property
    def projects(self):  # entry point into ZoltarResources. NB: hits API
        # NB: here we are throwing away each project's json, which is here because the API returns json objects for
        # projects rather than just URIs
        return [Project(self, project_json['url']) for project_json in
                self._json_for_uri(self.host + '/api/projects/')]

    def _json_for_uri(self, uri):
        if not self.session:
            raise RuntimeError('_validate_authentication(): no session')

        response = requests.get(uri, headers={'Accept': 'application/json; indent=4',
                                              'Authorization': 'JWT {}'.format(self.session.token)})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError('get_token(): status code was not 200: {}. {}'
                               .format(response.status_code, response.text))

        return response.json()


class ZoltarSession:  # internal use

    def __init__(self, zoltar_connection):
        super().__init__()
        self.zoltar_connection = zoltar_connection
        self.token = self._get_token()

    def _get_token(self):
        response = requests.post(self.zoltar_connection.host + '/api-token-auth/',
                                 {'username': self.zoltar_connection.username, 'password': self.zoltar_connection.password})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError('get_token(): status code was not 200: {}. {}'
                               .format(response.status_code, response.text))

        return response.json()['token']


class ZoltarResource(ABC):
    """
    An abstract proxy for a Zoltar object at a particular URI including its JSON. NB: subclasses not meant to be
    directly instantiated by users.
    """

    def __init__(self, zoltar_connection, uri):  # NB: hits API
        self.zoltar_connection = zoltar_connection
        self.uri = uri
        self.json = None  # cached -> can become stale!

        self.refresh()

    @property
    def id(self):
        return self.json['id']

    def refresh(self):
        self.json = self.zoltar_connection._json_for_uri(self.uri)

    def delete(self):
        response = requests.delete(self.uri, headers={'Accept': 'application/json; indent=4',
                                                      'Authorization': 'JWT {}'
                                                      .format(self.zoltar_connection.session.token)})
        if response.status_code != 204:  # HTTP_204_NO_CONTENT
            raise RuntimeError('delete_resource(): status code was not 204: {}. {}'
                               .format(response.status_code, response.text))


class Project(ZoltarResource):

    def __init__(self, zoltar_connection, uri):
        super().__init__(zoltar_connection, uri)

    def __repr__(self):
        return str((self.__class__.__name__, self.uri, self.id, self.name))

    @property
    def name(self):
        return self.json['name']

    @property
    def models(self):
        return [Model(self.zoltar_connection, model_uri) for model_uri in self.json['models']]


class Model(ZoltarResource):

    def __init__(self, zoltar_connection, uri):
        super().__init__(zoltar_connection, uri)

    def __repr__(self):
        return str((self.__class__.__name__, self.uri, self.id, self.name))

    @property
    def name(self):
        return self.json['name']

    @property
    def forecasts(self):
        # unlike other resources that are a list of URIs, model each model forecast is a dict with three keys:
        #   'timezero_date', 'data_version_date', 'forecast'
        #
        # for example:
        # [{'timezero_date': '20170117', 'data_version_date': None, 'forecast': 'http://127.0.0.1:8000/api/forecast/35/'},
        #  {'timezero_date': '20170124', 'data_version_date': None, 'forecast': None}]}
        #
        # note that 'data_version_date' and 'forecast' might be None. in this method we only return Forecast objects
        # that are not None. (recall that a model's TimeZeros might not have associated forecast data yet.)
        return [Forecast(self.zoltar_connection, forecast_dict['forecast']) for forecast_dict in self.json['forecasts']
                if forecast_dict['forecast']]

    def forecast_for_pk(self, forecast_pk):
        forecast_uri = self.zoltar_connection.host + \
            '/api/forecast/{}/'.format(forecast_pk)
        return Forecast(self.zoltar_connection, forecast_uri)

    # YYYYMMDD_DATE_FORMAT
    def upload_forecast(self, forecast_csv_file, timezero_date, data_version_date=None):
        data = {'timezero_date': timezero_date}
        if data_version_date:
            data['data_version_date'] = data_version_date
        response = requests.post(self.uri + 'forecasts/',
                                 headers={'Authorization': 'JWT {}'.format(
                                     self.zoltar_connection.session.token)},
                                 data=data,
                                 files={'data_file': open(forecast_csv_file, 'rb')})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError(
                'upload_forecast(): status code was not 200: {}'.format(response.text))

        upload_file_job_json = response.json()
        return UploadFileJob(self.zoltar_connection, upload_file_job_json['url'])


class Forecast(ZoltarResource):

    def __init__(self, zoltar_client, uri):
        super().__init__(zoltar_client, uri)

    def __repr__(self):
        return str((self.__class__.__name__, self.uri, self.id, self.timezero_date, self.csv_filename))

    @property
    def timezero_date(self):
        return self.json['time_zero']['timezero_date']

    @property
    def csv_filename(self):
        return self.json['csv_filename']

    def data(self, is_json=True):
        """
        :return: this forecast's data as either JSON or CSV
        :param is_json: True for JSON format, false for CSV
        """
        data_uri = self.json['forecast_data']
        if is_json:  # default API format
            return self.zoltar_connection._json_for_uri(data_uri)
        else:
            # todo fix api_views.forecast_data() to use proper accept type rather than 'format' query parameter
            response = requests.get(data_uri,
                                    headers={'Authorization': 'JWT {}'.format(
                                        self.zoltar_connection.session.token)},
                                    params={'format': 'csv'})
            if response.status_code != 200:  # HTTP_200_OK
                raise RuntimeError('data(): status code was not 200: {}. {}'
                                   .format(response.status_code, response.text))

            return response.content


class UploadFileJob(ZoltarResource):
    STATUS_ID_TO_STR = {
        0: 'PENDING',
        1: 'CLOUD_FILE_UPLOADED',
        2: 'QUEUED',
        3: 'CLOUD_FILE_DOWNLOADED',
        4: 'SUCCESS',
        5: 'FAILED',
    }

    def __init__(self, zoltar_client, uri):
        super().__init__(zoltar_client, uri)

    def __repr__(self):
        return str((self.__class__.__name__, self.uri, self.id, self.status_as_str))

    @property
    def output_json(self):
        return self.json['output_json']

    @property
    def status_as_str(self):
        status_int = self.json['status']
        return UploadFileJob.STATUS_ID_TO_STR[status_int]
