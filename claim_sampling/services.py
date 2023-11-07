from claim.apps import ClaimConfig
from claim.models import Claim
from enum import Enum
from django.db.models import OuterRef, Subquery, Avg, Q


def get_claims_from_data_helper(kwargs):
    '''
    This function is copy of resolver from claim method, ideally it should be rewritten, so both endpoints use the same
    code.
    :return:
    '''

    query = Claim.objects.all()
    code_is_not = kwargs.get("code_is_not", None)
    if code_is_not:
        query = query.exclude(code=code_is_not)
    variance = kwargs.get("diagnosisVariance", None)

    show_restored = kwargs.get("show_restored", None)
    if show_restored:
        query = query.filter(restore__isnull=False)

    items = kwargs.get("items", None)
    services = kwargs.get("services", None)

    if items:
        query = query.filter(items__item__code__in=items)

    if services:
        query = query.filter(services__service__code__in=services)

    attachment_status = kwargs.get("attachment_status", 0)

    class AttachmentStatusEnum(Enum):
        NONE = 0
        WITH = 1
        WITHOUT = 2

    if attachment_status == AttachmentStatusEnum.WITH.value:
        query = query.filter(attachments__isnull=False)
    elif attachment_status == AttachmentStatusEnum.WITHOUT.value:
        query = query.filter(attachments__isnull=True)

    care_type = kwargs.get("care_type", None)

    if care_type:
        query = query.filter(care_type=care_type)

    json_ext = kwargs.get("json_ext", None)

    if json_ext:
        query = query.filter(json_ext__jsoncontains=json_ext)

    if variance:
        from core import datetime, datetimedelta

        last_year = datetime.date.today() + datetimedelta(years=-1)
        diag_avg = (
            Claim.objects.filter(validity_to__isnull=True)
            .filter(date_claimed__gt=last_year)
            .values("icd__code")
            .filter(icd__code=OuterRef("icd__code"))
            .annotate(diag_avg=Avg("approved"))
            .values("diag_avg")
        )
        variance_filter = Q(claimed__gt=(
            1 + variance / 100) * Subquery(diag_avg))
        if not ClaimConfig.gql_query_claim_diagnosis_variance_only_on_existing:
            diags = (
                Claim.objects.filter(validity_to__isnull=True)
                .filter(date_claimed__gt=last_year)
                .values("icd__code")
                .distinct()
            )
            variance_filter = variance_filter | ~Q(icd__code__in=diags)
        query = query.filter(variance_filter)

    # TODO check if replacement is valid in every instance
    # return gql_optimizer.query(query.all(), info)

    # qs = super(DjangoFilterConnectionField, cls).resolve_queryset(
    #     connection, iterable, info, args
    # )
    # filter_kwargs = {k: v for k, v in args.items() if k in filtering_args}
    # qs = filterset_class(data=filter_kwargs, queryset=qs, request=info.context).qs
    #
    # return OrderedDjangoFilterConnectionField.orderBy(qs, args)
    # if 'status' in kwargs:
    #     query = query.filter(status=kwargs['status'])

    if kwargs is not None:
        query = query.filter(**kwargs)

    return query.filter(validity_to__isnull=True)
