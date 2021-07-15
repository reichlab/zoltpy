import datetime

from django.db import models
from django.db.models import BooleanField, IntegerField

from forecast_app.models import Project
from utils.utilities import basic_str


#
# ---- Target ----
#

class Target(models.Model):
    BOOLEAN_DATA_TYPE = bool
    DATE_DATA_TYPE = datetime.date
    FLOAT_DATA_TYPE = float
    INTEGER_DATA_TYPE = int
    TEXT_DATA_TYPE = str
    DATE_UNITS = ['month', 'week', 'biweek', 'day']
    project = models.ForeignKey(Project, related_name='targets', on_delete=models.CASCADE)
    CONTINUOUS_TARGET_TYPE = 0
    DISCRETE_TARGET_TYPE = 1
    NOMINAL_TARGET_TYPE = 2
    BINARY_TARGET_TYPE = 3
    DATE_TARGET_TYPE = 4
    TARGET_TYPE_CHOICES = (
        (CONTINUOUS_TARGET_TYPE, 'continuous'),
        (DISCRETE_TARGET_TYPE, 'discrete'),
        (NOMINAL_TARGET_TYPE, 'nominal'),
        (BINARY_TARGET_TYPE, 'binary'),
        (DATE_TARGET_TYPE, 'date'),
    )
    type = models.IntegerField(choices=TARGET_TYPE_CHOICES,
                               help_text="The Target's type. The choices are 'continuous', 'discrete', 'nominal', "
                                         "'binary', and 'date'.")
    name = models.TextField(help_text="A brief name for the target.")
    description = models.TextField(help_text="A verbose description of what the target is.")
    is_step_ahead = BooleanField(help_text="True if the target is one of a sequence of targets that predict values at "
                                           "different points in the future.")
    step_ahead_increment = IntegerField(help_text="An integer, indicating the forecast horizon represented by this "
                                                  "target. It is required if `is_step_ahead` is True.",
                                        null=True, default=None)
    unit = models.TextField(help_text="This target's units, e.g., 'percentage', 'week', 'cases', etc.", null=True)


    def __repr__(self):
        return str((self.pk, self.name, Target.str_for_target_type(self.type),
                    self.is_step_ahead, self.step_ahead_increment, self.unit))


    def __str__(self):  # todo
        return basic_str(self)


    @classmethod
    def str_for_target_type(cls, the_type_int):
        for type_int, type_name in cls.TARGET_TYPE_CHOICES:
            if type_int == the_type_int:
                return type_name

        return '!?'
