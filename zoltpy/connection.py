import base64
import csv
import datetime
import enum
import json
import logging
import tempfile
from abc import ABC

import dateutil
import requests

from zoltpy.cdc_io import YYYY_MM_DD_DATE_FORMAT


logger = logging.getLogger(__name__)


def _basic_str(obj):
    """
    Handy for writing quick and dirty __str__() implementations.
    """
    return obj.__class__.__name__ + ': ' + obj.__repr__()


class ZoltarConnection:
    """
    Represents a connection to a Zoltar server. This is an object-oriented interface that may be best suited to zoltpy
    developers. See the `util` module for a name-based non-OOP interface.

    A note on URLs: We require a trailing slash ('/') on all URLs. The only exception is the host arg passed to this
    class's constructor. This convention matches Django REST framework one, which is what Zoltar is written in.

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


    def __repr__(self):
        return str((self.host, self.session))


    def __str__(self):  # todo
        return _basic_str(self)


    def authenticate(self, username, password):
        self.username, self.password = username, password
        self.session = ZoltarSession(self)


    def re_authenticate_if_necessary(self):
        if self.session.is_token_expired():
            logger.debug(f"re_authenticate_if_necessary(): re-authenticating expired token. host={self.host}")
            self.authenticate(self.username, self.password)


    @property
    def projects(self):
        """
        The entry point into ZoltarResources.

        Returns a list of Projects. NB: A property, but hits the API.
        """
        projects_json_list = self.json_for_uri(self.host + '/api/projects/')
        return [Project(self, project_json['url'], project_json) for project_json in projects_json_list]


    def json_for_uri(self, uri, is_return_json=True, accept='application/json; indent=4'):
        logger.debug(f"json_for_uri(): {uri!r}")
        if not self.session:
            raise RuntimeError("json_for_uri(): no session. uri={uri}")

        self.re_authenticate_if_necessary()
        response = requests.get(uri, headers={'Accept': accept,
                                              'Authorization': 'JWT {}'.format(self.session.token)})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError(f"json_for_uri(): status code was not 200. uri={uri},"
                               f"status_code={response.status_code}. text={response.text}")

        return response.json() if is_return_json else response


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
        Details: based on how Zoltar implements JWT, we determine expiration by comparing the current datetime to the
        token's payload's "exp" field. its value is a POSIX timestamp of a UTC date and time as returned by
        datetime.utcnow().timestamp() - https://docs.python.org/3.6/library/datetime.html#datetime.datetime.utcnow

        :return: True if my token is expired, and False if still valid
        """
        return self.token_expiration_date().timestamp() <= datetime.datetime.utcnow().timestamp()


    def token_expiration_date(self):
        # # returns a POSIXct for the zoltar_session's token. see notes in is_token_expired() for details on extracting the date

        token_split = self.token.split('.')  # 3 parts: header, payload, signature
        payload_encoded = token_split[1]

        # per https://stackoverflow.com/questions/2941995/python-ignore-incorrect-padding-error-when-base64-decoding/49459036
        missing_padding = len(payload_encoded) % 4
        if missing_padding:
            payload_encoded += '=' * (4 - missing_padding)

        payload_decoded = base64.b64decode(payload_encoded)
        payload = json.loads(payload_decoded)
        timestamp_utc = payload['exp']
        exp_timestamp_date = datetime.datetime.utcfromtimestamp(timestamp_utc)
        return exp_timestamp_date


class ZoltarResource(ABC):
    """
    An abstract proxy for a Zoltar object at a particular URI including its JSON. All it does is cache JSON from a URI.
    Notes:

    - This class and its subclasses are not meant to be directly instantiated by users. Instead the user enters through
      ZoltarConnection.projects and then drills down.
    - Because the JSON is cached, it will become stale after the source object in the server changes, such as when a new
      model is created or a forecast uploaded. This it's the user's responsibility to call `refresh()` as needed.
    - Newly-created instances do *not* refresh by default, for efficiency.
    """


    def __init__(self, zoltar_connection, uri, initial_json=None):
        """
        :param zoltar_connection:
        :param uri:
        :param initial_json: optional param that's passed if caller already has JSON from server
        """
        self.zoltar_connection = zoltar_connection
        self.uri = uri  # *does* include trailing slash
        self._json = initial_json  # cached JSON is None if not yet touched. can become stale
        # NB: no self.refresh() call!


    def __repr__(self):
        """
        A default __repr__() that does not hit the API unless my _json has been cached, in which case my _repr_keys
        class var is used to determine which properties to return.
        """
        repr_keys = getattr(self, '_repr_keys', None)
        repr_list = [self.__class__.__name__, self.uri, self.id]
        if repr_keys and self._json:
            repr_list.extend([self._json[repr_key] for repr_key in repr_keys
                              if repr_key in self._json and self._json[repr_key]])
        return str(tuple(repr_list))


    @property
    def id(self):  # todo rename to not conflict with `id` builtin
        return ZoltarResource.id_for_uri(self.uri)


    @classmethod
    def id_for_uri(cls, uri):
        """
        :return: the trailing integer id from a url structured like: "http://example.com/api/forecast/71/" -> 71L
        """
        url_split = [split for split in uri.split('/') if split]  # drop any empty components, mainly from end
        return int(url_split[-1])


    @property
    def json(self):
        """
        :return: my json as a dict, refreshing if none cached yet
        """
        return self._json if self._json else self.refresh()


    def refresh(self):
        self._json = self.zoltar_connection.json_for_uri(self.uri)
        return self._json


    def delete(self):
        self.zoltar_connection.re_authenticate_if_necessary()
        response = requests.delete(self.uri, headers={'Accept': 'application/json; indent=4',
                                                      'Authorization': f'JWT {self.zoltar_connection.session.token}'})
        if (response.status_code != 200) and (response.status_code != 204):  # HTTP_200_OK, HTTP_204_NO_CONTENT
            raise RuntimeError(f'delete_resource(): status code was not 204: {response.status_code}. {response.text}')

        return response


class QueryType(enum.Enum):
    """
    Types of queries that `submit_query()` can handle.
    """
    FORECASTS = enum.auto()
    SCORES = enum.auto()
    TRUTH = enum.auto()


class Project(ZoltarResource):
    """
    Represents a Zoltar project, and is the entry point for getting its list of Models.
    """

    _repr_keys = ('name', 'is_public')


    def __init__(self, zoltar_connection, uri, initial_json=None):
        super().__init__(zoltar_connection, uri, initial_json)


    @property
    def name(self):
        return self.json['name']


    @property
    def models(self):
        """
        :return: a list of the Project's Models
        """
        models_json_list = self.zoltar_connection.json_for_uri(self.uri + 'models/')
        return [Model(self.zoltar_connection, model_json['url'], model_json) for model_json in models_json_list]


    @property
    def units(self):
        """
        :return: a list of the Project's Units
        """
        units_json_list = self.zoltar_connection.json_for_uri(self.uri + 'units/')
        return [Unit(self.zoltar_connection, unit_json['url'], unit_json) for unit_json in units_json_list]


    @property
    def targets(self):
        """
        :return: a list of the Project's Targets
        """
        targets_json_list = self.zoltar_connection.json_for_uri(self.uri + 'targets/')
        return [Target(self.zoltar_connection, target_json['url'], target_json) for target_json in targets_json_list]


    @property
    def timezeros(self):
        """
        :return: a list of the Project's TimeZeros
        """
        timezeros_json_list = self.zoltar_connection.json_for_uri(self.uri + 'timezeros/')
        return [TimeZero(self.zoltar_connection, timezero_json['url'], timezero_json)
                for timezero_json in timezeros_json_list]


    @property
    def truth_csv_filename(self):
        """
        :return: the Project's truth_csv_filename
        """
        # recall the json contains these keys: 'id', 'url', 'project', 'truth_csv_filename', 'truth_updated_at,
        # 'truth_data'
        return self.zoltar_connection.json_for_uri(self.uri + 'truth/')['truth_csv_filename']


    @property
    def truth_updated_at(self):
        """
        :return: the Project's truth_updated_at, a datetime.datetime
        """
        # recall the json contains these keys: 'id', 'url', 'project', 'truth_csv_filename', 'truth_updated_at,
        # 'truth_data'
        return dateutil.parser.parse(self.zoltar_connection.json_for_uri(self.uri + 'truth/')['truth_updated_at'])


    def upload_truth_data(self, truth_csv_fp):
        """
        Uploads truth data to this project, deleting existing truth if any.

        :param truth_csv_fp: an open truth csv file-like object. the truth CSV file format is documented at
            https://docs.zoltardata.com/
        :return: a Job to use to track the upload
        """
        self.zoltar_connection.re_authenticate_if_necessary()
        response = requests.post(self.uri + 'truth/',
                                 headers={'Authorization': f'JWT {self.zoltar_connection.session.token}'},
                                 files={'data_file': truth_csv_fp})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError(f"upload_truth_data(): status code was not 200. status_code={response.status_code}. "
                               f"text={response.text}")

        job_json = response.json()
        return Job(self.zoltar_connection, job_json['url'])


    def create_model(self, model_config):
        """
        Creates a forecast Model with the passed configuration.

        :param model_config: a dict used to initialize the new model. it must contain these fields: ['name',
            'abbreviation', 'team_name', 'description', 'contributors', 'license', 'notes', 'citation', 'methods',
            'home_url', 'aux_data_url']
        :return: a Model
        """
        self.zoltar_connection.re_authenticate_if_necessary()
        response = requests.post(f'{self.uri}models/',
                                 headers={'Authorization': f'JWT {self.zoltar_connection.session.token}'},
                                 json={'model_config': model_config})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError(f"status_code was not 200. status_code={response.status_code}, text={response.text}")

        new_model_json = response.json()
        return Model(self.zoltar_connection, new_model_json['url'], new_model_json)


    def create_timezero(self, timezero_date, data_version_date=None, is_season_start=False, season_name=''):
        """
        Creates a timezero in me with the passed parameters.

        :param timezero_date: YYYY-MM-DD DATE FORMAT, e.g., '2018-12-03'
        :param data_version_date: optional. same format as timezero_date
        :param is_season_start: optional boolean indicating season start
        :param season_name: optional season name. required if is_season_start
        :return: the new TimeZero
        """
        # validate args
        # POST. 'timezero_config' args:
        # - required: 'timezero_date', 'data_version_date', 'is_season_start'
        # - optional: 'season_name'
        timezero_config = {'timezero_date': timezero_date,
                           'data_version_date': data_version_date,
                           'is_season_start': is_season_start}
        if is_season_start:
            timezero_config['season_name'] = season_name
        self.zoltar_connection.re_authenticate_if_necessary()
        response = requests.post(f'{self.uri}timezeros/',
                                 headers={'Authorization': f'JWT {self.zoltar_connection.session.token}'},
                                 json={'timezero_config': timezero_config})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError(f"status_code was not 200. status_code={response.status_code}, text={response.text}")

        new_timezero_json = response.json()
        return TimeZero(self.zoltar_connection, new_timezero_json['url'], new_timezero_json)


    def submit_query(self, query_type, query):
        """
        Submits a request for the execution of a query of forecasts in this Project.

        :param query_type: a QueryType enum value indicating the type of query to run
        :param query: a dict that constrains the queried data. It is the analog of the JSON object documented at
            https://docs.zoltardata.com/ . Briefly, query is a dict whose keys vary depending on query_type. References
            to models, units, targets, and timezeros are strings that name the objects, and not IDs. Following are some
            examples of the three types of queries:

        Forecasts:
            {"models": ["60-contact", "CovidIL_100"],
             "units": ["US"],
             "targets": ["0 day ahead cum death", "1 day ahead cum death"],
             "timezeros": ["2020-05-14", "2020-05-09"],
             "types": ["point", "quantile"]}

         Scores:
            {"models": ["60-contact", "CovidIL_100"],
             "units": ["US"],
             "targets": ["0 day ahead cum death", "1 day ahead cum death"],
             "timezeros": ["2020-05-14", "2020-05-09"],
             "scores": ["log_single_bin", "interval_100"]}

        Truth:
            {"units": ["US"],
             "targets": ["0 day ahead cum death", "1 day ahead cum death"],
             "timezeros": ["2020-05-14", "2020-05-09"]}

        :return: a Job for the query
        """
        if not isinstance(query_type, QueryType):
            raise RuntimeError(f"invalid query_type: {query_type!r} ({type(query_type)})")

        query_url = {QueryType.FORECASTS: 'forecast_queries/',
                     QueryType.SCORES: 'scores_queries/',
                     QueryType.TRUTH: 'truth_queries/'}[query_type]
        self.zoltar_connection.re_authenticate_if_necessary()
        response = requests.post(self.uri + query_url,
                                 headers={'Authorization': f'JWT {self.zoltar_connection.session.token}'},
                                 json={'query': query})
        job_json = response.json()
        if response.status_code != 200:
            raise RuntimeError(f"error submitting query: {job_json['error']}")

        return Job(self.zoltar_connection, job_json['url'])


class Model(ZoltarResource):
    """
    Represents a Zoltar forecast model, and is the entry point for getting its Forecasts as well as uploading them.
    """

    _repr_keys = ('name',)


    def __init__(self, zoltar_connection, uri, initial_json=None):
        super().__init__(zoltar_connection, uri, initial_json)


    @property
    def name(self):
        return self.json['name']


    @property
    def abbreviation(self):
        return self.json['abbreviation']


    @property
    def team_name(self):
        return self.json['team_name']


    @property
    def description(self):
        return self.json['description']


    @property
    def contributors(self):
        return self.json['contributors']


    @property
    def license(self):
        return self.json['license']


    @property
    def notes(self):
        return self.json['notes']


    @property
    def citation(self):
        return self.json['citation']


    @property
    def methods(self):
        return self.json['methods']


    @property
    def home_url(self):
        return self.json['home_url']


    @property
    def aux_data_url(self):
        return self.json['aux_data_url']


    @property
    def forecasts(self):
        """
        :return: a list of this Model's Forecasts
        """
        forecasts_json_list = self.zoltar_connection.json_for_uri(self.uri + 'forecasts/')
        return [Forecast(self.zoltar_connection, forecast_json['url'], forecast_json)
                for forecast_json in forecasts_json_list]


    @property
    def latest_forecast(self):
        """
        :return: the forecast in my forecasts that has the latest timezero date, or None if no forecasts
        """
        the_forecast = None
        for forecast in self.forecasts:
            if not the_forecast or (forecast.timezero.timezero_date > the_forecast.timezero.timezero_date):
                the_forecast = forecast
        return the_forecast


    def edit(self, model_config):
        """
        Edits this model to have the passed values

        :param model_config: a dict used to edit this model. it must contain these fields: ['name',
            'abbreviation', 'team_name', 'description', 'contributors', 'license', 'notes', 'citation', 'methods',
            'home_url', 'aux_data_url']
        """
        response = requests.put(self.uri,
                                headers={'Authorization': f'JWT {self.zoltar_connection.session.token}'},
                                json={'model_config': model_config})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError(f"edit(): status code was not 200. status_code={response.status_code}. "
                               f"text={response.text}")


    def upload_forecast(self, forecast_json, source, timezero_date, notes=''):
        """
        Uploads forecast data to this connection.

        :param forecast_json: "JSON IO dict" to upload. format as documented at https://docs.zoltardata.com/
        :param timezero_date: timezero to upload to YYYY-MM-DD DATE FORMAT
        :param source: source to associate with the uploaded data
        :param notes: optional user notes for the new forecast
        :return: a Job to use to track the upload
        """
        self.zoltar_connection.re_authenticate_if_necessary()
        with tempfile.TemporaryFile("r+") as forecast_json_fp:
            json.dump(forecast_json, forecast_json_fp)
            forecast_json_fp.seek(0)
            response = requests.post(self.uri + 'forecasts/',
                                     headers={'Authorization': f'JWT {self.zoltar_connection.session.token}'},
                                     data={'timezero_date': timezero_date, 'notes': notes},
                                     files={'data_file': (source, forecast_json_fp, 'application/json')})
            if response.status_code != 200:  # HTTP_200_OK
                raise RuntimeError(f"upload_forecast(): status code was not 200. status_code={response.status_code}. "
                                   f"text={response.text}", response)

            job_json = response.json()
            return Job(self.zoltar_connection, job_json['url'])


class Forecast(ZoltarResource):
    _repr_keys = ('source', 'issue_date')


    def __init__(self, zoltar_connection, uri, initial_json=None):
        super().__init__(zoltar_connection, uri, initial_json)


    def delete(self):
        """
        Does the usual delete, but returns a Job for it. (Deleting a forecasts is an enqueued operation.)
        """
        response = super().delete()
        job_json = response.json()
        return Job(self.zoltar_connection, job_json['url'], job_json)


    @property
    def timezero(self):
        return TimeZero(self.zoltar_connection, self.json['time_zero']['url'], self.json['time_zero'])


    @property
    def source(self):
        return self.json['source']


    @source.setter
    def source(self, source):
        """
        Sets my source to `source`. NB: does *not* call `self.refresh()`, for efficiency
        """
        response = requests.patch(self.uri,
                                  headers={'Authorization': f'JWT {self.zoltar_connection.session.token}'},
                                  json={'source': source})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError(f"set source(): status code was not 200. status_code={response.status_code}. "
                               f"text={response.text}")


    @property
    def notes(self):
        return self.json['notes']


    @notes.setter
    def notes(self, notes):
        """
        Sets my notes to `notes`. NB: does *not* call `self.refresh()`, for efficiency
        """
        response = requests.patch(self.uri,
                                  headers={'Authorization': f'JWT {self.zoltar_connection.session.token}'},
                                  json={'notes': notes})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError(f"set notes(): status code was not 200. status_code={response.status_code}. "
                               f"text={response.text}")


    @property
    def issue_date(self):
        return self.json['issue_date']


    @issue_date.setter
    def issue_date(self, issue_date):
        """
        Sets my issue_date to `issue_date`. NB: does *not* call `self.refresh()`, for efficiency

        :param issue_date: new issue_date. must be a string in YYYY_MM_DD_DATE_FORMAT, e.g., '2017-01-17'
        """
        response = requests.patch(self.uri,
                                  headers={'Authorization': f'JWT {self.zoltar_connection.session.token}'},
                                  json={'issue_date': issue_date})
        if response.status_code != 200:  # HTTP_200_OK
            raise RuntimeError(f"set issue_date(): status code was not 200. status_code={response.status_code}. "
                               f"text={response.text}")


    @property
    def created_at(self):
        return self.json['created_at']


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


class Unit(ZoltarResource):
    _repr_keys = ('name',)


    def __init__(self, zoltar_connection, uri, initial_json=None):
        super().__init__(zoltar_connection, uri, initial_json)


    @property
    def name(self):
        return self.json['name']


class Target(ZoltarResource):
    _repr_keys = ('name', 'type', 'is_step_ahead', 'step_ahead_increment', 'unit')


    def __init__(self, zoltar_connection, uri, initial_json=None):
        super().__init__(zoltar_connection, uri, initial_json)


    @property
    def name(self):
        return self.json['name']


    @property
    def type(self):
        return self.json['type']


    @property
    def is_step_ahead(self):
        return self.json['is_step_ahead']


    @property
    def step_ahead_increment(self):
        return self.json['step_ahead_increment']


    @property
    def unit(self):
        return self.json['unit']


class TimeZero(ZoltarResource):
    _repr_keys = ('timezero_date', 'data_version_date', 'is_season_start', 'season_name')


    def __init__(self, zoltar_connection, uri, initial_json=None):
        super().__init__(zoltar_connection, uri, initial_json)


    @property
    def timezero_date(self):
        """
        :return: my timezero_date, a datetime.date
        """
        return datetime.datetime.strptime(self.json['timezero_date'], YYYY_MM_DD_DATE_FORMAT).date()  # never None


    @property
    def data_version_date(self):
        """
        :return: my data_version_date, a datetime.date
        """
        return datetime.datetime.strptime(self.json['data_version_date'], YYYY_MM_DD_DATE_FORMAT).date() \
            if self.json['data_version_date'] else None


    @property
    def is_season_start(self):
        return self.json['is_season_start']


    @property
    def season_name(self):
        return self.json['season_name']


class Job(ZoltarResource):
    STATUS_ID_TO_STR = {
        0: 'PENDING',
        1: 'CLOUD_FILE_UPLOADED',
        2: 'QUEUED',
        3: 'CLOUD_FILE_DOWNLOADED',
        4: 'SUCCESS',
        5: 'FAILED',
        6: 'TIMEOUT',
    }


    def __init__(self, zoltar_connection, uri, initial_json=None):
        super().__init__(zoltar_connection, uri, initial_json)


    def __repr__(self):
        return str((self.__class__.__name__, self.uri, self.id, self.status_as_str)) if self._json \
            else super().__repr__()


    @property
    def input_json(self):
        return self.json['input_json']


    @property
    def output_json(self):
        return self.json['output_json']


    @property
    def status_as_str(self):
        status_int = self.json['status']
        return Job.STATUS_ID_TO_STR[status_int]


    def created_forecast(self):
        """
        A helper function that returns the newly-uploaded Forecast. Should only be called on Jobs that are the results
        of an uploaded forecast via `Model.upload_forecast()`.

        :return: the new Forecast that this uploaded created, or None if the Job was for a non-forecast
            upload.
        """
        if 'forecast_pk' not in self.output_json:
            return None

        forecast_pk = self.output_json['forecast_pk']
        forecast_uri = self.zoltar_connection.host + f'/api/forecast/{forecast_pk}/'
        return Forecast(self.zoltar_connection, forecast_uri)


    def download_data(self):
        """
        Downloads the data for jobs that have an associated file, such as a query's results. Called on Jobs
        that are the results of a project forecast or score queries via `submit_query()`. NB: It is a 404 Not Found
        error if this is called on a Job that has no underlying S3 data file, which can happen b/c: 1) 24 hours has
        passed (the expiration time) or 2) the Job is not complete and therefore has not saved the data file. For
        the latter you may use `busy_poll_job()` to ensure the job is done.

        See docs at https://docs.zoltardata.com/ .

        :return: list of CSV rows. The columns depend on the originating query. Full documentation at
            https://docs.zoltardata.com/
        """
        job_data_url = f"{self.uri}data/"
        response_json = self.zoltar_connection.json_for_uri(job_data_url, False, 'text/csv')
        decoded_content = response_json.content.decode('utf-8')
        csv_reader = csv.reader(decoded_content.splitlines(), delimiter=',')
        return list(csv_reader)
