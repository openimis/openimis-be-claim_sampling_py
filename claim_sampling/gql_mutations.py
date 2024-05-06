import logging
import uuid
import pathlib
import base64
from typing import Callable, Dict
import random
import graphene
import json

from .apps import ClaimSamplingConfig
from core.schema import TinyInt, OpenIMISMutation
from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import ValidationError, PermissionDenied
from django.utils.translation import gettext as _

from claim.gql_mutations import ClaimCodeInputType, ClaimGuaranteeIdInputType, FeedbackInputType
from claim.models import ClaimAdmin

from django.db import transaction

from .models import ClaimSamplingBatch, ClaimSamplingBatchAssignment

logger = logging.getLogger(__name__)


class ClaimSamplingBatchInputType(OpenIMISMutation.Input):
    percentage = graphene.Int(required=True)
    claimAdminUuid = graphene.String(required=True)

    status = TinyInt(required=False)
    id = graphene.Int(required=False, read_only=True)
    uuid = graphene.String(required=False)
    autogenerate = graphene.Boolean(required=False)
    insuree_id = graphene.Int(required=False)
    date_from = graphene.Date(required=False)
    date_to = graphene.Date(required=False)
    icd_id = graphene.Int(required=False)
    icd_1_id = graphene.Int(required=False)
    icd_2_id = graphene.Int(required=False)
    icd_3_id = graphene.Int(required=False)
    icd_4_id = graphene.Int(required=False)
    review_status = TinyInt(required=False)
    date_claimed = graphene.Date(required=False)
    date_processed = graphene.Date(required=False)
    health_facility_id = graphene.Int(required=False)
    refer_from_id = graphene.Int(required=False)
    refer_to_id = graphene.Int(required=False)
    batch_run_id = graphene.Int(required=False)
    category = graphene.String(max_length=1, required=False)
    visit_type = graphene.String(max_length=1, required=False)
    admin_id = graphene.Int(required=False)
    explanation = graphene.String(required=False)
    adjustment = graphene.String(required=False)
    json_ext = graphene.types.json.JSONString(required=False)
    restore = graphene.UUID(required=False)
    feedback_available = graphene.Boolean(default=False)
    feedback_status = TinyInt(required=False)
    care_type = graphene.String(required=False)
    filters = graphene.JSONString(required=False)

    # code = graphene.Field(ClaimCodeInputType, required=True)
    # feedback = graphene.Field(FeedbackInputType, required=False)
    # guarantee_id = ClaimGuaranteeIdInputType(required=False)


@transaction.atomic
def update_or_create_claim_sampling_batch(data, user):
    claim_sampling_batch_uuid = data.pop("uuid", None)

    claim_sampling_batch_data = {'assigned_to': ClaimAdmin.objects.get(uuid=data.get("claimAdminUuid")),
                                 'created_by': ClaimAdmin.objects.get(uuid=data.pop("claimAdminUuid")),#user.id
                                 'is_completed': False,
                                 'is_applied': False,
                                 'computed_value':  {},
                                 'assigned_value':  {},
                                 }

    if claim_sampling_batch_uuid is not None:
        claim_sampling_batch = ClaimSamplingBatch.objects.get(uuid=claim_sampling_batch_uuid)
        claim_sampling_batch.save_history()
        # reset the non required fields
        # (each update is 'complete', necessary to be able to set 'null')
        [setattr(claim_sampling_batch, key, claim_sampling_batch_data[key]) for key in claim_sampling_batch_data]
    else:
        claim_sampling_batch = ClaimSamplingBatch.objects.create(**claim_sampling_batch_data)
    claim_sampling_batch.save()

    claim_sampling_assignment = create_claim_sampling_batch_assignment(data=data, claim_sampling_batch=claim_sampling_batch)

    assignment_as_json = [[x.claim_id.code, x.claim_batch_id.id] for x in claim_sampling_assignment]

    from claim_sampling.services import create_review_task
    create_review_task(user=user, claims=json.dumps(assignment_as_json), batch=claim_sampling_batch)

    return claim_sampling_batch


def create_claim_sampling_batch_assignment(data, claim_sampling_batch):
    from claim_sampling.services import get_claims_from_data_helper
    percentage = data.pop('percentage')
    claim_batch_ids = get_claims_from_data_helper(data)#[claim.id for claim in get_claims_from_data_helper(data)]
    data['claim_batch_id'] = claim_sampling_batch.id
    data['claim_id'] = claim_batch_ids
    status_options = [ClaimSamplingBatchAssignment.Status.IDLE, ClaimSamplingBatchAssignment.Status.SKIPPED]
    status_list = random.choices(status_options, cum_weights=[percentage, 100], k=len(claim_batch_ids))

    # claim_sampling_batch_assignments = [ClaimSamplingBatchAssignment(claim_batch_id=claim_sampling_batch.id,
    #                                                                  claim_id=status_list[])]
    claim_sampling_batch_assignments = []

    for idx, claim_id in enumerate(claim_batch_ids):
        claim_sampling_batch_assignments.append(ClaimSamplingBatchAssignment(claim_batch_id=claim_sampling_batch,
                                                                             claim_id=claim_id,
                                                                             status=status_list[idx]))

    assignments = ClaimSamplingBatchAssignment.objects.bulk_create(claim_sampling_batch_assignments)

    # claim_sampling_batch_assignment.save()
    return assignments #claim_sampling_batch_assignment


class CreateClaimSamplingBatchMutation(OpenIMISMutation):
    """
    Create a new claim sampling batch.
    """
    _mutation_module = "claim_sampling"
    _mutation_class = "CreateClaimSamplingBatchMutation"

    class Input(ClaimSamplingBatchInputType):
        pass

    @classmethod
    def async_mutate(cls, user, **data):
        try:
            if type(user) is AnonymousUser or not user.id:
                raise ValidationError(_("mutation.authentication_required"))
            # if not user.has_perms(ClaimSamplingConfig.gql_mutation_create_claim_batch_samplings_perms):
            #     raise PermissionDenied(_("unauthorized"))
            if "client_mutation_id" in data:
                data.pop('client_mutation_id')
            if "client_mutation_label" in data:
                data.pop('client_mutation_label')
            # data['audit_user_id'] = user.id_for_audit
            from core.utils import TimeUtils
            # data['validity_from'] = TimeUtils.now()
            claim_sampling_batch = update_or_create_claim_sampling_batch(data, user)
            return None
        except Exception as exc:
            return [{
                'message': _("claim.mutation.failed_to_create_claim_sampling_batch") % {'code': data['code']},
                'detail': str(exc)}]


class UpdateClaimSamplingBatchMutation(OpenIMISMutation):
    """
    Update a claim. The claim items and services can all be updated with this call
    """
    _mutation_module = "claim_sampling"
    _mutation_class = "UpdateClaimSamplingBatchMutation"

    class Input(ClaimSamplingBatchInputType):
        pass

    @classmethod
    def async_mutate(cls, user, **data):
        try:
            if type(user) is AnonymousUser or not user.id:
                raise ValidationError(
                    _("mutation.authentication_required"))
            if not user.has_perms(ClaimSamplingConfig.gql_mutation_update_claim_batch_samplings_perms):
                raise PermissionDenied(_("unauthorized"))
            data['audit_user_id'] = user.id_for_audit
            update_or_create_claim_sampling_batch(data, user)
            return None
        except Exception as exc:
            return [{
                'message': _("claim.mutation.failed_to_update_claim_sampling_batch") % {'code': data['code']},
                'detail': str(exc)}]


class ApproveClaimSamplingBatchMutation(OpenIMISMutation):
    """
    Approve given claim batch and apply deduction rate or other parameters across all claims in given batch.
    """
    _mutation_module = "claim_sampling"
    _mutation_class = "ApproveClaimSamplingBatchMutation"

    class Input(ClaimSamplingBatchInputType):
        pass

    @classmethod
    def async_mutate(cls, user, **data):
        if not user.has_perms(ClaimSamplingConfig.gql_mutation_approve_claim_batch_samplings_perms):
            raise PermissionDenied(_("unauthorized"))
        errors = []

        return errors
