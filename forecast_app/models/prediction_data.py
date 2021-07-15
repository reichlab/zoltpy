from django.db import models

from utils.utilities import basic_str


#
# ---- PredictionData ----
#

class PredictionData(models.Model):
    pred_ele = models.ForeignKey('PredictionElement', related_name='pred_data', on_delete=models.CASCADE,
                                 primary_key=True)
    data = models.JSONField()


    def __repr__(self):
        return str((self.pk, list(self.data.keys())))


    def __str__(self):  # todo
        return basic_str(self)
