import logging

from django.db import models

from utils.utilities import basic_str


logger = logging.getLogger(__name__)


#
# ---- Project class ----
#

class Project(models.Model):
    is_public = models.BooleanField(default=True,
                                    help_text="Controls project visibility. False means the project is private and "
                                              "can only be accessed by the project's owner or any of its model_owners. "
                                              "True means it is publicly accessible.")
    name = models.TextField()
    WEEK_TIME_INTERVAL_TYPE = 'w'
    BIWEEK_TIME_INTERVAL_TYPE = 'b'
    MONTH_TIME_INTERVAL_TYPE = 'm'
    TIME_INTERVAL_TYPE_CHOICES = ((WEEK_TIME_INTERVAL_TYPE, 'Week'),
                                  (BIWEEK_TIME_INTERVAL_TYPE, 'Biweek'),
                                  (MONTH_TIME_INTERVAL_TYPE, 'Month'))
    time_interval_type = models.CharField(max_length=1,
                                          choices=TIME_INTERVAL_TYPE_CHOICES, default=WEEK_TIME_INTERVAL_TYPE,
                                          help_text="Used when visualizing the x axis label.")
    visualization_y_label = models.TextField(help_text="Used when visualizing the Y axis label.")
    description = models.TextField(help_text="A few paragraphs describing the project. Please see documentation for"
                                             "what should be included here - 'real-time-ness', time_zeros, etc.")
    home_url = models.URLField(help_text="The project's home site.")
    logo_url = models.URLField(blank=True, null=True, help_text="The project's optional logo image.")
    core_data = models.URLField(
        help_text="Directory or Zip file containing data files (e.g., CSV files) made made available to everyone in "
                  "the challenge, including supplemental data like Google queries or weather.")


    def __repr__(self):
        return str((self.pk, self.name))


    def __str__(self):  # todo
        return basic_str(self)


    def timezero_to_season_name(self):
        """
        :return: a dict mapping each of my timezeros -> containing season name
        """
        _timezero_to_season_name = {}
        containing_season_name = None
        for timezero in self.timezeros.order_by('timezero_date'):
            if timezero.is_season_start:
                containing_season_name = timezero.season_name
            _timezero_to_season_name[timezero] = containing_season_name
        return _timezero_to_season_name


    #
    # count-related functions
    #

    def num_models_forecasts(self):
        """
        :return: a 2-tuple: (num_models, num_forecasts)
        """
        from .forecast import Forecast  # avoid circular imports


        num_models = self.models.filter(project=self, is_oracle=False).count()
        num_forecasts = Forecast.objects.filter(forecast_model__project=self, forecast_model__is_oracle=False).count()
        return num_models, num_forecasts


    def num_pred_ele_rows_all_models(self, is_oracle=True):
        """
        :return: the total number of PredictionElements across all my models' forecasts, for all types of Predictions.
            can be very slow for large databases
        """
        from forecast_app.models import PredictionElement  # avoid circular imports


        return PredictionElement.objects.filter(forecast__forecast_model__project=self,
                                                forecast__forecast_model__is_oracle=is_oracle).count()


#
# ---- Unit class ----
#

class Unit(models.Model):
    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['project', 'abbreviation'], name='unique_unit_abbreviation'),
        ]


    project = models.ForeignKey(Project, related_name='units', on_delete=models.CASCADE)
    name = models.TextField()
    abbreviation = models.TextField()


    def __repr__(self):
        return str((self.pk, self.abbreviation, self.name))


    def __str__(self):  # todo
        return basic_str(self)


#
# ---- TimeZero class ----
#

class TimeZero(models.Model):
    project = models.ForeignKey(Project, related_name='timezeros', on_delete=models.CASCADE)
    timezero_date = models.DateField(help_text="A date that a target is relative to.")
    data_version_date = models.DateField(
        null=True, blank=True,
        help_text="The optional database date at which models should work with for the timezero_date.")  # nullable
    is_season_start = models.BooleanField(
        default=False,
        help_text="True if this TimeZero starts a season.")
    season_name = models.TextField(
        null=True, blank=True,
        max_length=50, help_text="The name of the season this TimeZero starts, if is_season_start.")  # nullable


    def __repr__(self):
        return str((self.pk, str(self.timezero_date), str(self.data_version_date),
                    self.is_season_start, self.season_name))


    def __str__(self):  # todo
        return basic_str(self)
