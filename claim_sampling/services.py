import random
import uuid
from typing import List

from claim.apps import ClaimConfig
from claim.models import Claim, ClaimItem, ClaimService
from enum import Enum
from django.db.models import OuterRef, Subquery, Avg, Q, Sum, F, ExpressionWrapper, DecimalField,  Subquery, OuterRef, Case, Value, When
from django.db.models.functions import Coalesce
from django.db import transaction
from django.utils.translation import gettext as _

from claim.services import set_claims_status, update_claims_dedrems, validate_and_process_dedrem_claim
from claim_sampling.models import (
    ClaimSamplingBatch,
    ClaimSamplingBatchAssignment,
    ClaimSamplingBatchAssignmentStatus
)
from core.services import BaseService
from core.signals import register_service_signal
from core.validation import BaseModelValidation
from core import filter_validity
from tasks_management.apps import TasksManagementConfig
from tasks_management.models import Task, TaskGroup
from tasks_management.services import TaskService, _get_std_task_data_payload


class IndividualDataSourceValidation(BaseModelValidation):
    OBJECT_TYPE = ClaimSamplingBatch


class ClaimSamplingService(BaseService):
    OBJECT_TYPE = ClaimSamplingBatch

    @transaction.atomic
    @register_service_signal('claim_sampling_service.create')
    def create(self, obj_data, task_group: TaskGroup = None):
        """
        Creates a new sampling batch and assigns claims to it based on the specified sampling percentage.

        This method registers a service signal 'claim_sampling_service.create' and performs the following steps:
        1. Creates a new `ClaimSamplingBatch` instance.
        2. Determines which claims should be selected for review based on the given percentage.
        3. Creates assignments for each claim in the batch, tagging them with the appropriate review status.
        4. Saves all assignments in bulk to the database.

        Parameters:
            obj_data (dict): A dictionary containing:
                - 'percentage': The percentage of claims that should be selected for review (int).
                - 'uuids': A QuerySet of claim UUIDs that should be considered for sampling (QuerySet).
            task_group (TaskGroup): Task Group to which newly created task will be assigned.
        Usage:
            >>> claim_data = {'percentage': 20, 'uuids': Claim.objects.all()}
            >>> service = ClaimSamplingService(user)
            >>> service.create(claim_data)

        """
        percentage = int(obj_data.pop('percentage'))
        claim_batch_ids = obj_data.pop('uuids')  # UUIDS QuerySet

        if len(claim_batch_ids) == 0:
            raise ValueError(_("Claim List cannot be empty"))

        claim_batch_ids = self.__filter_already_assigned(claim_batch_ids=claim_batch_ids)

        if len(claim_batch_ids) == 0:
            raise ValueError(_("All claims already assigned"))

        if percentage < 1 or percentage > 100:
            raise ValueError(_("Percentage not in range (0, 100)"))

        sampling_batch_data = super().create({
            'is_completed': False,
            'is_applied': False,
            'computed_value': {},
            'assigned_value': {}
        })
        sampling_batch = ClaimSamplingBatch.objects.get(uuid=sampling_batch_data['data']['uuid'])

        is_selected_for_review = self.__choose_random_claims_for_review(claim_batch_ids.count(), percentage)

        batches = []
        for next_claim in claim_batch_ids:
            claim = Claim.objects.get(uuid=next_claim)
            should_be_reviewed = is_selected_for_review.pop()
            batches.append(ClaimSamplingBatchAssignment(
             uuid=uuid.uuid4(),
             claim=claim,
             claim_batch=sampling_batch,
             status=should_be_reviewed,
             user_created=self.user,
             user_updated=self.user
            ))
            if claim.review_status in [Claim.REVIEW_IDLE, Claim.REVIEW_NOT_SELECTED] \
                    and should_be_reviewed == ClaimSamplingBatchAssignmentStatus.IDLE:
                claim.review_status = Claim.REVIEW_SELECTED
                claim.save_history()
                claim.save()

        ClaimSamplingBatchAssignment.objects.bulk_create(batches)
        task = self._create_sampling_task(sampling_batch_data, sampling_batch, task_group)
        return sampling_batch

    @register_service_signal('claim_sampling_service.update')
    def update(self, obj_data):
        return super().update(obj_data)

    @register_service_signal('claim_sampling_service.delete')
    def delete(self, obj_data):
        return super().delete(obj_data)

    def __filter_already_assigned(self, claim_batch_ids):
        filtered_claim_batch_ids = claim_batch_ids.exclude(id__in=ClaimSamplingBatchAssignment.objects.filter(claim__uuid__in=claim_batch_ids).values("claim"))
        return filtered_claim_batch_ids

    @transaction.atomic
    def extrapolate_results(self, claim_sampling_id):
        claim_sampling = ClaimSamplingBatch.objects.get(id=claim_sampling_id)

        qs = Claim.objects.filter(assignments__claim_batch=claim_sampling, *filter_validity())
        # Subquery for total_itm_adjusted
        total_itm_adjusted_subquery = Claim.objects.filter(id=OuterRef('id')).annotate(
            total_itm_adjusted=Sum(
                F("items__qty_provided") * Coalesce("items__price_adjusted", "items__price_asked")
            )
        ).values('total_itm_adjusted')[:1]

        # Subquery for total_srv_adjusted
        total_srv_adjusted_subquery = Claim.objects.filter(id=OuterRef('id')).annotate(
            total_srv_adjusted=Sum(
                F("services__qty_provided") * Coalesce("services__price_adjusted", "services__price_asked")
            )
        ).values('total_srv_adjusted')[:1]

        # Subquery for total_itm_approved
        total_itm_approved_subquery = Claim.objects.filter(id=OuterRef('id')).annotate(
            total_itm_approved=Sum(
                Case(
                    When(status=Claim.STATUS_REJECTED, then=Value(0)),
                    default=Coalesce("items__qty_approved", "items__qty_provided", 0) * 
                        Coalesce("items__price_approved", "services__price_adjusted", "items__price_asked"),
                    output_field=DecimalField()
                )
            )
        ).values('total_itm_approved')[:1]

        # Subquery for total_srv_approved
        total_srv_approved_subquery = Claim.objects.filter(id=OuterRef('id')).annotate(
            total_srv_approved=Sum(
                Case(
                    When(status=Claim.STATUS_REJECTED, then=Value(0)),
                    default=Coalesce("services__qty_approved", "services__qty_provided", 0) *
                    Coalesce("services__price_approved", "services__price_adjusted", "services__price_asked"),
                    output_field=DecimalField()
                )
            )
        ).values('total_srv_approved')[:1]

        deductible = qs.filter(review_status=Claim.REVIEW_DELIVERED)\
            .filter(Q(services__rejection_reason=-1) | Q(services__rejection_reason__isnull=True))\
            .annotate(total_srv_adjusted=(total_srv_adjusted_subquery))\
            .annotate(total_itm_adjusted=(total_itm_adjusted_subquery))\
            .annotate(total_srv_approved=(total_srv_approved_subquery))\
            .annotate(total_itm_approved=(total_itm_approved_subquery))\
            .aggregate(value=ExpressionWrapper(
                (Sum("total_srv_approved") + Sum("total_itm_approved")) /
                ( Sum("total_srv_adjusted") + Sum("total_itm_adjusted")),
                output_field=DecimalField()
            ))["value"]
                   
        
        qs_extrapolated = qs.filter(assignments__status=ClaimSamplingBatchAssignmentStatus.SKIPPED, review_status=Claim.REVIEW_SELECTED)
        
        # Subquery for total_itm_adjusted
        total_itm_adjusted_subquery = Claim.objects.filter(id=OuterRef('id')).annotate(
            total_itm_adjusted=Sum(
                F("items__qty_provided") * Coalesce("items__price_adjusted", "items__price_asked")
            )
        ).values('total_itm_adjusted')[:1]

        # Subquery for total_srv_adjusted
        total_srv_adjusted_subquery = Claim.objects.filter(id=OuterRef('id')).annotate(
            total_srv_adjusted=Sum(
                F("services__qty_provided") * Coalesce("services__price_adjusted", "services__price_asked")
            )
        ).values('total_srv_adjusted')[:1]

        # Filter claims for extrapolation
        qs_extrapolated = qs.filter(assignments__status=ClaimSamplingBatchAssignmentStatus.SKIPPED, review_status=Claim.REVIEW_IDLE)

        # Update the claims using subqueries
        qs_extrapolated.update(
            review_status=Claim.REVIEW_BYPASSED,
            approved=ExpressionWrapper(
                deductible * (
                    Coalesce(Subquery(total_itm_adjusted_subquery), 0) +
                    Coalesce(Subquery(total_srv_adjusted_subquery), 0)
                ),
                output_field=DecimalField()
            )
        )
        # update service and item
        ClaimItem.objects.filter(claim__in=qs_extrapolated).update(price_approved=deductible * F("price_adjusted"))
        ClaimService.objects.filter(claim__in=qs_extrapolated).update(price_approved=deductible * F("price_adjusted"))

        errors = []
        for claim in qs:
            errors += validate_and_process_dedrem_claim(claim, self.user, True)

        return errors

    def prepare_sampling_summary(self, claim_sampling_id):
        relevant_claims = self._get_sampling_claims(claim_sampling_id)
        total = relevant_claims.count()
        reviewed_delivered = relevant_claims.filter(review_status=Claim.REVIEW_DELIVERED)
        rejected_from_review = reviewed_delivered.filter(status=Claim.STATUS_REJECTED)
        return rejected_from_review, reviewed_delivered, total

    def apply_claim_item_service_deduction(self, claim, deduction_rate):
        claim_items = claim.items.all()

        for item in claim_items:
            new_claim_item = item
            if new_claim_item.price_approved:
                new_claim_item.price_approved *= (100-deduction_rate)/100
                new_claim_item.save()

        claim_services = claim.services.all()

        for service in claim_services:
            new_claim_service = service
            if new_claim_service.price_approved:
                new_claim_service.price_approved *= (100-deduction_rate)/100
                new_claim_service.save()

    def _get_sampling_claims(self, claim_sampling_id, include_skip=False):
        assigned_claims = ClaimSamplingBatchAssignment.objects.filter(claim_batch_id=claim_sampling_id)
        filters = [
            ClaimSamplingBatchAssignmentStatus.IDLE
        ]

        if include_skip:
            filters += ClaimSamplingBatchAssignmentStatus.SKIPPED

        claim_assignments = assigned_claims.filter(status__in=filters)
        relevant_claims = Claim.objects \
            .filter(id__in=claim_assignments.values_list('claim_id', flat=True).distinct())
        return relevant_claims

    def __init__(self, user, validation_class=IndividualDataSourceValidation):
        super().__init__(user, validation_class)

    def __choose_random_claims_for_review(self, total_elements: int, percentage: int):
        selected_for_review = int((percentage/100.0) * total_elements)
        not_selected = total_elements - selected_for_review

        # Ensure at least one claim is selected for review
        if selected_for_review == 0 and not_selected > 0:
            selected_for_review += 1
            not_selected -= 1

        # Create the matching number of claims
        result_list = [ClaimSamplingBatchAssignmentStatus.IDLE] * selected_for_review + \
                      [ClaimSamplingBatchAssignmentStatus.SKIPPED] * not_selected

        # Shuffle the list to randomize the order
        random.shuffle(result_list)
        return result_list

    def _create_sampling_task(self, sampling_batch_data, sampling_batch, task_group):
        return TaskService(self.user).create({
            'source': 'claim_sampling',
            'entity': sampling_batch,
            'status': Task.Status.ACCEPTED if task_group else Task.Status.RECEIVED,
            'executor_action_event': TasksManagementConfig.default_executor_event,
            'business_event': 'claim_sample_extrapolation',
            'data': _get_std_task_data_payload(sampling_batch_data),
            'task_group': task_group
        })

    def _update_not_reviewed(self, claims_list: List[Claim]):
        uuids = [claim.uuid for claim in claims_list]

        set_claims_status(uuids, 'review_status', Claim.REVIEW_BYPASSED)
