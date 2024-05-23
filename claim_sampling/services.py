import random
import uuid
from typing import List

from claim.apps import ClaimConfig
from claim.models import Claim
from enum import Enum
from django.db.models import OuterRef, Subquery, Avg, Q
from django.db import transaction
from django.utils.translation import gettext as _

from claim.services import set_claims_status, update_claims_dedrems
from claim_sampling.models import ClaimSamplingBatch, ClaimSamplingBatchAssignment
from core.services import BaseService
from core.signals import register_service_signal
from core.validation import BaseModelValidation
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
            batches.append(ClaimSamplingBatchAssignment(
             uuid=uuid.uuid4(),
             claim=claim,
             claim_batch=sampling_batch,
             status=is_selected_for_review.pop(),
             user_created=self.user,
             user_updated=self.user
            ))
            if claim.review_status in [Claim.REVIEW_NOT_SELECTED, Claim.REVIEW_BYPASSED]:
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

    @transaction.atomic
    def extrapolate_results(self, claim_sampling_id):
        claim_sampling = ClaimSamplingBatch.objects.get(id=claim_sampling_id)
        sample_claim_assignments = self._get_sampling_claims(claim_sampling_id, False)

        selected_for_review_not_delivered = list(
            sample_claim_assignments
            .filter(review_status=Claim.REVIEW_SELECTED)
        )

        claims_awaiting_validation = ClaimSamplingBatchAssignment\
            .objects.filter(claim_batch_id=claim_sampling_id)\
            .filter(status=ClaimSamplingBatchAssignment.Status.SKIPPED)\
            .select_related('claim')

        reviewed_delivered = sample_claim_assignments.filter(review_status=Claim.REVIEW_DELIVERED)
        rejected_from_review = reviewed_delivered.filter(status=Claim.STATUS_REJECTED)

        # Change review status to bypass
        self._update_not_reviewed([c.claim for c in selected_for_review_not_delivered])

        deductible = round(rejected_from_review.count() / reviewed_delivered.count(), 2) * 100
        # SHOULD WE RUN ENGINE BEFORE THIS?
        split = self.__choose_random_claims_for_deductible(
            claims_awaiting_validation.count(), deductible
        )

        result = {
            'approved': [],
            'rejected': []
        }

        for approved, assignment in zip(split, claims_awaiting_validation):
            if approved == Claim.STATUS_VALUATED:
                result['approved'].append(assignment.claim.uuid)
            else:
                result['rejected'].append(assignment.claim.uuid)

        set_claims_status(result['rejected'], 'status', Claim.STATUS_REJECTED)
        errors = update_claims_dedrems(result['approved'], self.user)
        errors += update_claims_dedrems(
            sample_claim_assignments.filter(status=Claim.STATUS_CHECKED),
            self.user
        )
        return errors

    def prepare_sampling_summary(self, claim_sampling_id):
        relevant_claims = self._get_sampling_claims(claim_sampling_id)
        total = relevant_claims.count()
        reviewed_delivered = relevant_claims.filter(review_status=Claim.REVIEW_DELIVERED)
        rejected_from_review = reviewed_delivered.filter(status=Claim.STATUS_REJECTED)
        return rejected_from_review, reviewed_delivered, total

    def _get_sampling_claims(self, claim_sampling_id, include_skip=False):
        assigned_claims = ClaimSamplingBatchAssignment.objects.filter(claim_batch_id=claim_sampling_id)
        filters = [
            ClaimSamplingBatchAssignment.Status.IDLE
        ]

        if include_skip:
            filters += ClaimSamplingBatchAssignment.Status.SKIPPED

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
        result_list = [ClaimSamplingBatchAssignment.Status.IDLE] * selected_for_review + \
                      [ClaimSamplingBatchAssignment.Status.SKIPPED] * not_selected

        # Shuffle the list to randomize the order
        random.shuffle(result_list)
        return result_list

    def __choose_random_claims_for_deductible(self, total_elements: int, percentage: int):
        valid = int((percentage/100.0) * total_elements)
        rejected = total_elements - valid

        # Create the matching number of claims
        result_list = [Claim.STATUS_VALUATED] * valid + \
                      [Claim.STATUS_REJECTED] * rejected

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
