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

    @classmethod
    def get_rights(cls, action):
        """
        The rights governing an action on this entity, for GraphQL and REST.

        Redeclares nothing: the rights table is `claim_sampling.apps.DJANGO_PERMS`, by
        entity then by action, and `configured_perms` reads the *configured* value
        there - the one ModuleConfiguration may have overridden - and not the declared
        default. The read happens here at call time and not at import time, because the
        `_perms` attributes only hold their value after `ready()`.
        """
        from claim_sampling.apps import configured_perms

        return configured_perms("claimSamplingBatch", action)


class ClaimSamplingBatchAssignmentStatus(models.TextChoices):
    SKIPPED = "S"  # Claims Which Validation is based on sampling
    IDLE = "I"  # Part of the sample


class ClaimSamplingBatchAssignment(HistoryModel):
    # An assignment is a row of the sampling batch: nobody holds a right of their own
    # on it, and modifying it means modifying the batch. The parent is declared and not
    # inferred - `claim` is just as much a foreign key, but a claim does not govern who
    # may sample it; the batch does.
    scope_parent = "claim_batch"

    claim = models.ForeignKey(Claim, models.DO_NOTHING, db_column='ClaimID', related_name="assignments")
    claim_batch = models.ForeignKey(ClaimSamplingBatch, models.DO_NOTHING, db_column='ClaimSamplingBatchID',
                                    related_name="assignments")
    status = models.CharField(
        max_length=2,
        choices=ClaimSamplingBatchAssignmentStatus.choices,
        default=ClaimSamplingBatchAssignmentStatus.IDLE
    )
