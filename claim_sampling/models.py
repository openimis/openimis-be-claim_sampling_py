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
        Les droits regissant une action sur cette entite, pour GraphQL et REST.

        Ne redeclare rien : la table des droits est `claim_sampling.apps.DJANGO_PERMS`,
        par entite puis par action, et `configured_perms` y lit la valeur *configuree* -
        celle que ModuleConfiguration a pu surcharger - et non le defaut declare. La
        lecture se fait ici a l'appel et non a l'import, car les attributs `_perms` ne
        valent leur valeur qu'apres `ready()`.
        """
        from claim_sampling.apps import configured_perms

        return configured_perms("claimSamplingBatch", action)


class ClaimSamplingBatchAssignmentStatus(models.TextChoices):
    SKIPPED = "S"  # Claims Which Validation is based on sampling
    IDLE = "I"  # Part of the sample


class ClaimSamplingBatchAssignment(HistoryModel):
    # Une assignation est une ligne du lot d'echantillonnage : personne n'y detient de
    # droit propre, la modifier c'est modifier le lot. Le parent est declare et non
    # deduit - `claim` est une cle etrangere tout autant, mais une reclamation ne
    # gouverne pas qui peut l'echantillonner ; c'est le lot qui le fait.
    scope_parent = "claim_batch"

    claim = models.ForeignKey(Claim, models.DO_NOTHING, db_column='ClaimID', related_name="assignments")
    claim_batch = models.ForeignKey(ClaimSamplingBatch, models.DO_NOTHING, db_column='ClaimSamplingBatchID',
                                    related_name="assignments")
    status = models.CharField(
        max_length=2,
        choices=ClaimSamplingBatchAssignmentStatus.choices,
        default=ClaimSamplingBatchAssignmentStatus.IDLE
    )
