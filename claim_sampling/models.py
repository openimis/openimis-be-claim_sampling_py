from django.db import models
from claim.models import Claim
from core.models import HistoryModel


class ClaimSamplingBatch(HistoryModel):
    is_completed = models.BooleanField()
    is_applied = models.BooleanField()
    computed_value = models.JSONField(db_column="ComputedValue", blank=True, null=True)
    assigned_value = models.JSONField(db_column="AssignedValue", blank=True, null=True)

    def __str__(self):
        return f"Claim Sampling - {self.date_created}"


class ClaimSamplingBatchAssignmentStatus(models.TextChoices):
    SKIPPED = "S"  # Claims Which Validation is based on sampling
    IDLE = "I"  # Part of the sample


class ClaimSamplingBatchAssignment(HistoryModel):

    claim = models.ForeignKey(Claim, models.DO_NOTHING, db_column='ClaimID', related_name="assignments")
    claim_batch = models.ForeignKey(ClaimSamplingBatch, models.DO_NOTHING, db_column='ClaimSamplingBatchID',
                                    related_name="assignments")
    status = models.CharField(
        max_length=2,
        choices=ClaimSamplingBatchAssignmentStatus.choices,
        default=ClaimSamplingBatchAssignmentStatus.IDLE
    )
