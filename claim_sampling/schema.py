from enum import Enum

from django.db.models import OuterRef, Subquery, Avg, Q
import graphene_django_optimizer as gql_optimizer
from core.schema import OrderedDjangoFilterConnectionField
from core import filter_validity
from django.conf import settings
from claim_sampling.gql_queries import ClaimSamplingBatchGQLType, ClaimSamplingBatchAssignmentGQLType
from django.utils.translation import gettext as _
from claim_sampling.gql_mutations import *  # lgtm [py/polluting-import]

from claim_sampling.models import ClaimSamplingBatch, ClaimSamplingBatchAssignment
from claim.models import Claim


class Query(graphene.ObjectType):
    claim_sampling_batch = OrderedDjangoFilterConnectionField(
        ClaimSamplingBatchGQLType,
        id=graphene.Int(required=True)
    )

    claim_sampling_batch_assignment = graphene.Field(
        ClaimSamplingBatchAssignmentGQLType,
        id=graphene.Int(required=True)
    )

    sampling_batch_claims = graphene.Field(
        ClaimSamplingBatchGQLType,
        id=graphene.Int(required=True)
    )

    def resolve_claim_sampling_batch(self, info, **kwargs):
        if (
            not info.context.user.has_perms(ClaimSamplingConfig.gql_query_claim_batch_samplings_perms)
            and settings.ROW_SECURITY
        ):
            raise PermissionDenied(_("unauthorized"))

        claim_sampling_batch_id = kwargs.get("id", None)

        return ClaimSamplingBatch.objects.get(id=claim_sampling_batch_id, validity_to__isnull=True)

    def resolve_claim_sampling_batch_assignment(self, info, **kwargs):
        if (
            not info.context.user.has_perms(ClaimSamplingConfig.gql_query_claim_batch_samplings_perms)
            and settings.ROW_SECURITY
        ):
            raise PermissionDenied(_("unauthorized"))

        claim_sampling_batch_assignment_id = kwargs.get("id", None)

        return ClaimSamplingBatchAssignment.objects.get(id=claim_sampling_batch_assignment_id, validity_to__isnull=True)

    def resolve_sampling_batch_claims(self, info, **kwargs):
        if (
            not info.context.user.has_perms(ClaimSamplingConfig.gql_query_claim_batch_samplings_perms)
            and settings.ROW_SECURITY
        ):
            raise PermissionDenied(_("unauthorized"))

        current_batch = None
        if id is not None:
            current_batch = ClaimSamplingBatch.objects.get(id=id)
        if uuid is not None:
            current_batch = ClaimSamplingBatch.objects.get(uuid=uuid)
        # TODO move claim query resolver to a separate method and import it here?
        # TODO this will likely fail
        return Claim.objects.filter(id=current_batch.claim_id, validity_to__isnull=True)


class Mutation(graphene.ObjectType):
    create_claim_sampling_batch = CreateClaimSamplingBatchMutation.Field()
    update_claim_sampling_batch = UpdateClaimSamplingBatchMutation.Field()
    approve_claim_sampling_batch = ApproveClaimSamplingBatchMutation.Field()
