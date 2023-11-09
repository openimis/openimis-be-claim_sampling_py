import uuid

from core import models as core_models
from django.db import models
from claim.models import Claim, ClaimAdmin


class ClaimSamplingBatch(core_models.VersionedModel):
    id = models.AutoField(db_column='ClaimSamplingBatchID', primary_key=True)
    uuid = models.CharField(db_column='ClaimSamplingBatchUUID', max_length=36, default=uuid.uuid4, unique=True)
    assigned_to = models.ForeignKey(ClaimAdmin, models.DO_NOTHING, db_column='AssignedTo', related_name="AssignedTo")
    created_by = models.ForeignKey(ClaimAdmin, models.DO_NOTHING, db_column='CreatedBy', related_name="CreatedBy")
    is_completed = models.BooleanField()
    is_applied = models.BooleanField()
    computed_value = models.JSONField(db_column="ComputedValue", blank=True, null=True)
    assigned_value = models.JSONField(db_column="AssignedValue", blank=True, null=True)

    class Meta:
        managed = True
        db_table = "claim_ClaimSamplingBatch"


class ClaimSamplingBatchAssignment(core_models.VersionedModel):
    class Status(models.TextChoices):
        REVIEWED = "R"
        SKIPPED = "S"
        IDLE = "I"

    id = models.AutoField(db_column='ClaimSamplingBatchAssignmentID', primary_key=True)
    uuid = models.CharField(db_column='ClaimSamplingBatchAssignmentUUID',
                            max_length=36, default=uuid.uuid4, unique=True)
    claim_id = models.ForeignKey(Claim, models.DO_NOTHING, db_column='ClaimID', related_name="ClaimID")
    claim_batch_id = models.ForeignKey(ClaimSamplingBatch, models.DO_NOTHING, db_column='ClaimSamplingBatchID',
                                       related_name="ClaimSamplingBatchID")
    status = models.CharField(max_length=2, choices=Status.choices, default=Status.IDLE)

    class Meta:
        managed = True
        db_table = "claim_ClaimBatchAssignment"

    def set_claim_as_reviewed(self):
        self.status = self.Status.REVIEWED
