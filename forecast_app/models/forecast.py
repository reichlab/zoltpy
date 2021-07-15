from django.db import models

from forecast_app.models.forecast_model import ForecastModel
from forecast_app.models.project import TimeZero
from utils.utilities import basic_str


class Forecast(models.Model):
    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['forecast_model', 'time_zero', 'issued_at'], name='unique_version'),
        ]


    forecast_model = models.ForeignKey(ForecastModel, related_name='forecasts', on_delete=models.CASCADE)
    source = models.TextField(help_text="file name of the source of this forecast's prediction data")
    time_zero = models.ForeignKey(TimeZero, on_delete=models.CASCADE,
                                  help_text="TimeZero that this forecast is in relation to.")
    created_at = models.DateTimeField(auto_now_add=True)
    issued_at = models.DateTimeField(db_index=True, null=False)
    notes = models.TextField(null=True, blank=True,
                             help_text="Text describing anything slightly different about a given forecast, e.g., a "
                                       "changed set of assumptions or a comment about when the forecast was created. "
                                       "Notes should be brief, typically less than 50 words.")


    def __repr__(self):
        return str((self.pk, self.time_zero, self.issued_at, self.source, self.created_at))


    def __str__(self):  # todo
        return basic_str(self)
