import datetime
from collections import namedtuple

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

    # target_type choices
    CONTINUOUS_TARGET_TYPE = 0
    DISCRETE_TARGET_TYPE = 1
    NOMINAL_TARGET_TYPE = 2
    BINARY_TARGET_TYPE = 3
    DATE_TARGET_TYPE = 4
    TYPE_CHOICES = (
        (CONTINUOUS_TARGET_TYPE, 'continuous'),
        (DISCRETE_TARGET_TYPE, 'discrete'),
        (NOMINAL_TARGET_TYPE, 'nominal'),
        (BINARY_TARGET_TYPE, 'binary'),
        (DATE_TARGET_TYPE, 'date'),
    )

    # reference_date_type ("RDT") choices. see _TARGET_REFERENCE_DATE_TYPES below for the master list of them
    DAY_RDT = 0
    MMWR_WEEK_LAST_TIMEZERO_MONDAY_RDT = 1
    MMWR_WEEK_LAST_TIMEZERO_TUESDAY_RDT = 2
    BIWEEK_RDT = 3
    REF_DATE_TYPE_CHOICES = (
        (DAY_RDT, 'DAY'),
        (MMWR_WEEK_LAST_TIMEZERO_MONDAY_RDT, 'MMWR_WEEK_LAST_TIMEZERO_MONDAY'),
        (MMWR_WEEK_LAST_TIMEZERO_TUESDAY_RDT, 'MMWR_WEEK_LAST_TIMEZERO_TUESDAY'),
        (BIWEEK_RDT, 'BIWEEK'),
    )

    project = models.ForeignKey(Project, related_name='targets', on_delete=models.CASCADE)

    # required fields for all types
    name = models.TextField(help_text="A brief name for the target.")
    type = models.IntegerField(choices=TYPE_CHOICES,
                               help_text="The Target's type. The choices are 'continuous', 'discrete', 'nominal', "
                                         "'binary', and 'date'.")
    description = models.TextField(help_text="A verbose description of what the target is.")
    outcome_variable = models.TextField(help_text="Human-readable string naming the target variable, e.g. 'Incident "
                                                  "cases'.")
    is_step_ahead = BooleanField(help_text="True if the target is one of a sequence of targets that predict values at "
                                           "different points in the future.")
    numeric_horizon = IntegerField(help_text="An integer, indicating the forecast horizon represented by this target. "
                                             "It is required if `is_step_ahead` is True.",
                                   null=True, default=None)
    reference_date_type = models.IntegerField(choices=REF_DATE_TYPE_CHOICES,
                                              help_text="Indicates how the Target calculates reference_date and "
                                                        "target_end_date from a TimeZero. It is required if "
                                                        "`is_step_ahead` is True.", null=True)


    # type-specific fields
    # NB: 'list' type-specific fields: see TargetLwr.lwrs, TargetCat.cats, and TargetDate.range


    def __repr__(self):
        return str((self.pk, self.name, Target.str_for_target_type(self.type),
                    self.outcome_variable, self.is_step_ahead, self.numeric_horizon,
                    reference_date_type_for_id(self.reference_date_type).name if self.reference_date_type is not None
                    else None))


    def __str__(self):  # todo
        return basic_str(self)


    @classmethod
    def str_for_target_type(cls, the_type_int):
        for type_int, type_name in cls.TYPE_CHOICES:
            if type_int == the_type_int:
                return type_name

        return '!?'


#
# This tuple class contains information associated with each RDT in Target.reference_date_types. Instances are saved in
# the master list _TARGET_REFERENCE_DATE_TYPES below.
#
# Fields:
# - `id`: id (int) used in Target.reference_date_type DB field: 0, 1, ... NB: IDs should never be changed or deleted b/c
#         they may have been stored in the database
# - `name`: long name (str) used for the `reference_date_type` field in project config JSON files. taken from
#           REF_DATE_TYPE_CHOICES
# - `abbreviation`: short name (str) used for plot y axis
# - `calc_fcn`: function that computes a datetime.date. the signature is:
#                 f(target, timezero) -> (reference_date, target_end_date) . where:
#   = input: A Target and TimeZero. Only target's numeric_horizon and reference_date_type fields are used
#   = output: a 2-tuple: (reference_date, target_end_date)
#
ReferenceDateType = namedtuple('ReferenceDateType', ['id', 'name', 'abbreviation', 'calc_fcn'])

#
# master list of all possible Target.reference_date_types
#

# _TARGET_REFERENCE_DATE_TYPES helper var
_RDT_ID_TO_ABBREV_AND_CALC_FCN = {
    Target.DAY_RDT: ('day', None),
    Target.MMWR_WEEK_LAST_TIMEZERO_MONDAY_RDT: ('week', None),
    Target.MMWR_WEEK_LAST_TIMEZERO_TUESDAY_RDT: ('week', None),
    Target.BIWEEK_RDT: ('biweek', None),
}

_TARGET_REFERENCE_DATE_TYPES = tuple(ReferenceDateType(rdt_id, rdt_name,
                                                       _RDT_ID_TO_ABBREV_AND_CALC_FCN[rdt_id][0],
                                                       _RDT_ID_TO_ABBREV_AND_CALC_FCN[rdt_id][1])
                                     for rdt_id, rdt_name in Target.REF_DATE_TYPE_CHOICES)


def reference_date_type_for_id(rdt_id):
    """
    :param rdt_id: ReferenceDateType.id to find
    :return: ReferenceDateType for `ref_id`, or None if not found
    """
    rdt = [ref_date_type for ref_date_type in _TARGET_REFERENCE_DATE_TYPES if ref_date_type.id == rdt_id]
    if not rdt:
        raise RuntimeError(f"could not find ReferenceDateType for rdt_id={rdt_id!r}")

    return rdt[0]
