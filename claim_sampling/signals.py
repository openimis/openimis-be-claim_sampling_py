import logging

from core.forms import User
from core.service_signals import ServiceSignalBindType
from core.signals import bind_service_signal
from tasks_management.models import Task


logger = logging.getLogger(__name__)

def bind_service_signals():
    bind_service_signal(
        'task_service.resolve_task',
        on_task_resolve,
        bind_type=ServiceSignalBindType.AFTER
    )

    bind_service_signal(
        'task_service.complete_task',
        on_task_complete,
        bind_type=ServiceSignalBindType.AFTER
    )

def on_task_resolve(**kwargs):
    try:
        result = kwargs.get('result', None)
        if result and result['success'] \
                and result['data']['task']['status'] == Task.Status.ACCEPTED \
                and result['data']['task']['executor_action_event'] == 'claim_sampling_resolve':
            data = kwargs.get("result").get("data")
            task = Task.objects.select_related('task_group').prefetch_related('task_group__taskexecutor_set').get(
                id=data["task"]["id"])
            user = User.objects.get(id=data["user"]["id"])

            # dopisać review konkretnego taska do business_status

            #jak wszystkie taski zreviewowane, odpalić complete
            #TaskService(_user).complete_task({"id": _task.id})
    except Exception as e:
        logger.error("Error while executing on_task_resolve", exc_info=e)
        return [str(e)]

def on_task_complete(**kwargs):
    try:
        result = kwargs.get('result', None)
        if result and result['success'] \
                and result['data']['task']['status'] == Task.Status.ACCEPTED \
                and result['data']['task']['business_event'] == 'claim_sampling_complete':
            data = kwargs.get("result").get("data")
            task = Task.objects.select_related('task_group').prefetch_related('task_group__taskexecutor_set').get(
                id=data["task"]["id"])
            user = User.objects.get(id=data["user"]["id"])

            # Wyloczyć jakiś współczynnik
            # Stwotrzyć taska dla heada
    except Exception as e:
        logger.error("Error while executing on_task_resolve", exc_info=e)
        return [str(e)]