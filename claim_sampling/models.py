from django.db import models
from claim.models import Claim
from core.models import HistoryModel


class ClaimSamplingBatch(HistoryModel):
    is_completed = models.BooleanField()
    is_applied = models.BooleanField()
    computed_value = models.JSONField(db_column="ComputedValue", blank=True, null=True)
    assigned_value = models.JSONField(db_column="AssignedValue", blank=True, null=True)


class ClaimSamplingBatchAssignment(HistoryModel):
    class Status(models.TextChoices):
        SKIPPED = "S"  # Claims Which Validation is based on sampling
        IDLE = "I"  # Part of the sample

    claim = models.ForeignKey(Claim, models.DO_NOTHING, db_column='ClaimID', related_name="ClaimID")
    claim_batch = models.ForeignKey(ClaimSamplingBatch, models.DO_NOTHING, db_column='ClaimSamplingBatchID',
                                    related_name="ClaimSamplingBatchID")
    status = models.CharField(max_length=2, choices=Status.choices, default=Status.IDLE)
