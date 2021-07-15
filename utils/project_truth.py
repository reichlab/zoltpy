import logging


logger = logging.getLogger(__name__)


#
# oracle model functions
#
# these functions help manage the single (for now) oracle model in a project. there is either zero or one
# of them. this business rule is managed via load_truth_data()
#

def oracle_model_for_project(project):
    """
    :param project: a Project
    :return: the single oracle ForecastModel in project, or None if none exists yet
    :raises RuntimeError: >1 oracle models found
    """
    oracle_models = project.models.filter(is_oracle=True)
    if len(oracle_models) > 1:
        raise RuntimeError(f"more than one oracle model found. oracle_models={oracle_models}")

    return oracle_models.first()


#
# truth data access functions
#

def truth_data_qs(project):
    """
    :return: A QuerySet of project's truth data - PredictionElement instances.
    """
    from forecast_app.models import PredictionElement  # avoid circular imports


    oracle_model = oracle_model_for_project(project)
    return PredictionElement.objects.none() if not oracle_model else \
        PredictionElement.objects.filter(forecast__forecast_model=oracle_model)


def is_truth_data_loaded(project):
    """
    :return: True if `project` has truth data loaded via load_truth_data(). Actually, returns the count, which acts as a
        boolean.
    """
    return truth_data_qs(project).exists()


def get_truth_data_preview(project):
    """
    :return: view helper function that returns a preview of my truth data in the form of a table that's
        represented as a nested list of rows. each row: [timezero_date, unit_name, target_name, truth_value]
    """
    from forecast_app.models import PredictionData  # avoid circular imports


    oracle_model = oracle_model_for_project(project)
    if not oracle_model:
        return PredictionData.objects.none()

    # note: https://code.djangoproject.com/ticket/32483 sqlite3 json query bug -> we manually access field instead of
    # using 'data__value'
    pred_data_qs = PredictionData.objects \
                       .filter(pred_ele__forecast__forecast_model=oracle_model) \
                       .values_list('pred_ele__forecast__time_zero__timezero_date', 'pred_ele__unit__name',
                                    'pred_ele__target__name',
                                    'data')[:10]
    return [(tz_date, unit__name, target__name, data['value'])
            for tz_date, unit__name, target__name, data in pred_data_qs]


#
# load_truth_data()
#

POSTGRES_NULL_VALUE = 'NULL'  # used for Postgres-specific loading of rows from csv data files

TRUTH_CSV_HEADER = ['timezero', 'unit', 'target', 'value']
