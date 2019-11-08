import json
import logging
from abc import ABC

import requests


logger = logging.getLogger(__name__)


class ZoltarConnection:
    """Represents a connection to a Zoltar server. This is an object-oriented
    interface that may be best suited to zoltpy developers. See the `util`
    module for a name-based non-OOP interface.

    Notes:
    - This implementation uses the simple approach of caching the JSON response for resource URLs, but doesn't
      automatically handle their becoming stale, hence the need to call ZoltarResource.refresh().
    """


    def __init__(self, host='https://zoltardata.com'):
        """
        :param host: URL of the Zoltar host. should *not* have a trailing '/'
        """
        self.host = host
        self.username, self.password = None, None
        self.session = None


    def authenticate(self, username, password):
        self.username, self.password = username, password
        self.session = ZoltarSession(self)


    def re_authenticate_if_necessary(self):
        if self.session.is_token_expired():
            logger.info(f"re_authenticate_if_necessary(): re-authenticating expired token. host={self.host}")
            self.authenticate(self.username, self.password)


    @property
    def projects(self):
        """The entry point into ZoltarResources.

        Returns a list of Projects. NB: A property, but hits the API.
        """
        # NB: here we are throwing away each project's json, which is here because the API returns json objects for
        # projects rather than just URIs
        return [Project(self, project_json['url']) for project_json in self.json_for_uri(self.host + '/api/projects/')]


    def json_for_uri(self, uri):
        if not self.session:
            raise RuntimeError("json_for_uri(): no session")

        response = requests.get(uri, headers={'Accept': 'application/json; indent=4',
                                              'Authorization': 'JWT {}'.format(self.session.token)})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError(f"json_for_uri(): status code was not 200. status_code={response.status_code}. "
                               f"text={response.text}")

        return response.json()


class ZoltarSession:  # internal use

    def __init__(self, zoltar_connection):
        super().__init__()
        self.zoltar_connection = zoltar_connection
        self.token = self._get_token()


    def _get_token(self):
        response = requests.post(self.zoltar_connection.host + '/api-token-auth/',
                                 {'username': self.zoltar_connection.username,
                                  'password': self.zoltar_connection.password})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError(f"get_token(): status code was not 200. status_code={response.status_code}. "
                               f"text={response.text}")

        return response.json()['token']


    def is_token_expired(self):
        """
        :return: True if my token is expired, and False o/w
        """
        # see zoltr: is_token_expired(), token_expiration_date()
        return True  # todo xx fix!



class ZoltarResource(ABC):
    """An abstract proxy for a Zoltar object at a particular URI including its
    JSON. All it does is cache JSON from a URI. Notes:

    - This class and its subclasses are not meant to be directly instantiated by users. Instead the user enters through
      ZoltarConnection.projects and then drills down.
    - Because the JSON is cached, it will become stale after the source object in the server changes, such as when a
    """


    def __init__(self, zoltar_connection, uri):  # NB: hits API
        self.zoltar_connection = zoltar_connection
        self.uri = uri  # *does* include trailing slash
        self.json = None  # cached -> can become stale!
        self.refresh()


    @property
    def id(self):
        return self.json['id']


    def refresh(self):
        self.json = self.zoltar_connection.json_for_uri(self.uri)


    def delete(self):
        response = requests.delete(self.uri, headers={'Accept': 'application/json; indent=4',
                                                      'Authorization': f'JWT {self.zoltar_connection.session.token}'})
        if response.status_code != 204:  # HTTP_204_NO_CONTENT
            raise RuntimeError(f'delete_resource(): status code was not 204: {response.status_code}. {response.text}')



class Project(ZoltarResource):
    """Represents a Zoltar project, and is the entry point for getting its list
    of Models."""


    def __init__(self, zoltar_connection, uri):
        super().__init__(zoltar_connection, uri)


    def __repr__(self):
        return str((self.__class__.__name__, self.uri, self.id, self.name))


    @property
    def name(self):
        return self.json['name']


    @property
    def models(self):
        """
        :return: a list of the Project's Models
        """
        return [Model(self.zoltar_connection, model_uri) for model_uri in self.json['models']]


    def create_model(self, model_config):
        """Creates a forecast Model with the passed configuration.

        :param model_config: a dict used to initialize the new model. it must contain these fields: ['name'], and can
            optionally contain: ['abbreviation', 'team_name', 'description', 'home_url', 'aux_data_url']
        :return: a Model
        """
        # validate model_config
        actual_keys = set(model_config.keys())
        expected_keys = {'name', 'abbreviation', 'team_name', 'description', 'home_url', 'aux_data_url'}
        if actual_keys != expected_keys:
            raise RuntimeError(f"Wrong keys in 'model_config'. expected={expected_keys}, actual={actual_keys}")

        # POST. note that we throw away the new model's JSON that's returned once we extract its pk b/c it will be
        # cached by the Model() call
        response = requests.post(f'{self.uri}models/',
                                 headers={'Authorization': f'JWT {self.zoltar_connection.session.token}'},
                                 json={'model_config': model_config})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError(f"status_code was not 200. status_code={response.status_code}, text={response.text}")

        new_model_json = response.json()
        new_model_pk = new_model_json['id']
        new_model_url = f'{self.zoltar_connection.host}/api/model/{new_model_pk}'
        new_model = Model(self.zoltar_connection, new_model_url)


class Model(ZoltarResource):
    """Represents a Zoltar forecast model, and is the entry point for getting
    its Forecasts as well as uploading them."""


    def __init__(self, zoltar_connection, uri):
        super().__init__(zoltar_connection, uri)


    def __repr__(self):
        return str((self.__class__.__name__, self.uri, self.id, self.name))


    @property
    def name(self):
        return self.json['name']


    @property
    def forecasts(self):
        """
        :return: a list of this Model's Forecasts
        """
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


    def upload_forecast(self, forecast_json_fp, source, timezero_date, data_version_date=None):
        """Uploads a forecast file.

        :param forecast_csv_file: a JSON file in the "JSON IO dict" format accepted by
            utils.forecast.load_predictions_from_json_io_dict()
        :param forecast_csv_file: a JSON file in the "JSON IO dict" format accepted by
            utils.forecast.load_predictions_from_json_io_dict()
        :param timezero_date: YYYYMMDD_DATE_FORMAT
        :param data_version_date: YYYYMMDD_DATE_FORMAT
        :return: an UploadFileJob
        """
        data = {'timezero_date': timezero_date}
        if data_version_date:
            data['data_version_date'] = data_version_date
        response = requests.post(self.uri + 'forecasts/',
                                 headers={'Authorization': f'JWT {self.zoltar_connection.session.token}'},
                                 data=data,
                                 files={'data_file': (source, forecast_json_fp, 'application/json')})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError(f"upload_forecast(): status code was not 200. status_code={response.status_code}. "
                               f"text={response.text}")

        upload_file_job_json = response.json()
        return UploadFileJob(self.zoltar_connection, upload_file_job_json['url'])


class Forecast(ZoltarResource):

    def __init__(self, zoltar_connection, uri):
        super().__init__(zoltar_connection, uri)


    def __repr__(self):
        return str((self.__class__.__name__, self.uri, self.id, self.timezero_date, self.source))


    @property
    def timezero_date(self):
        return self.json['time_zero']['timezero_date']


    @property
    def source(self):
        return self.json['source']


    def data(self):
        """
        :return: this forecast's data as a dict in the "JSON IO dict" format accepted by
            utils.forecast.load_predictions_from_json_io_dict()
        """
        data_uri = self.json['forecast_data']
        response = requests.get(data_uri,
                                headers={'Authorization': 'JWT {}'.format(self.zoltar_connection.session.token)})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError(f"data(): status code was not 200. status_code={response.status_code}. "
                               f"text={response.text}")

        return json.loads(response.content.decode('utf-8'))


class UploadFileJob(ZoltarResource):
    STATUS_ID_TO_STR = {
        0: 'PENDING',
        1: 'CLOUD_FILE_UPLOADED',
        2: 'QUEUED',
        3: 'CLOUD_FILE_DOWNLOADED',
        4: 'SUCCESS',
        5: 'FAILED',
    }


    def __init__(self, zoltar_connection, uri):
        super().__init__(zoltar_connection, uri)


    def __repr__(self):
        return str((self.__class__.__name__, self.uri, self.id, self.status_as_str))


    @property
    def output_json(self):
        return self.json['output_json']


    @property
    def status_as_str(self):
        status_int = self.json['status']
        return UploadFileJob.STATUS_ID_TO_STR[status_int]
