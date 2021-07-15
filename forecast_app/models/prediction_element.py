from django.db import models

from utils.utilities import basic_str


#
# PredictionElement
#

class PredictionElement(models.Model):
    BIN_CLASS = 0
    NAMED_CLASS = 1
    POINT_CLASS = 2
    SAMPLE_CLASS = 3
    QUANTILE_CLASS = 4
    PRED_CLASS_CHOICES = (
        (BIN_CLASS, 'bin'),
        (NAMED_CLASS, 'named'),
        (POINT_CLASS, 'point'),
        (SAMPLE_CLASS, 'sample'),
        (QUANTILE_CLASS, 'quantile'),
    )
    forecast = models.ForeignKey('Forecast', related_name='pred_eles', on_delete=models.CASCADE)
    pred_class = models.IntegerField(choices=PRED_CLASS_CHOICES)
    unit = models.ForeignKey('Unit', on_delete=models.CASCADE)
    target = models.ForeignKey('Target', on_delete=models.CASCADE)
    is_retract = models.BooleanField(default=False)
    data_hash = models.CharField(max_length=32)  # length based on output from hashlib.md5(s).hexdigest()


    def __repr__(self):
        return str((self.pk, self.forecast.pk, self.prediction_class_as_str(), self.unit.pk, self.target.pk,
                    self.is_retract, self.data_hash))


    def __str__(self):  # todo
        return basic_str(self)


    def prediction_class_as_str(self):
        return PredictionElement.prediction_class_int_as_str(self.pred_class)


    @classmethod
    def prediction_class_int_as_str(cls, prediction_class_int):
        return PRED_CLASS_INT_TO_NAME.get(prediction_class_int, '!?')


#
# some bidirectional accessors for PredictionElement.PRED_CLASS_CHOICES
#

PRED_CLASS_INT_TO_NAME = {class_int: class_name for class_int, class_name in PredictionElement.PRED_CLASS_CHOICES}
PRED_CLASS_NAME_TO_INT = {class_name: class_int for class_int, class_name in PredictionElement.PRED_CLASS_CHOICES}
