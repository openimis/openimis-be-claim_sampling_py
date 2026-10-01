from django.apps import AppConfig

from core.rights_declaration import RightsDeclaration

MODULE_NAME = 'claim_sampling'


# Rights, by entity then by action.
#
# The entity is named after the model that carries it (`ClaimSamplingBatch`) and not
# after the config keys, which say "claim_batch_samplings" - an inherited word order,
# denoting no object of this module.
#
# `approve` is a business action, not an `update`: approving a sampling campaign
# applies the deduction rate to every claim in the batch (see
# ApproveClaimSamplingBatchMutation), which is not the same authorisation as correcting
# the batch itself. It therefore keeps its own identifier, and its django name stays
# declarative as long as `Meta.permissions` does not declare it.
DJANGO_PERMS = {
    "claimSamplingBatch": {
        "query": ("claim_sampling.view_claimsamplingbatch", 126001),
        "create": ("claim_sampling.add_claimsamplingbatch", 126002),
        "update": ("claim_sampling.change_claimsamplingbatch", 126003),
        "approve": ("claim_sampling.approve_claimsamplingbatch", 126004),
    },
}

_PERM_CFG = {
    "gql_query_claim_batch_samplings_perms": ("claimSamplingBatch", "query"),
    "gql_mutation_create_claim_batch_samplings_perms": ("claimSamplingBatch", "create"),
    "gql_mutation_update_claim_batch_samplings_perms": ("claimSamplingBatch", "update"),
    "gql_mutation_approve_claim_batch_samplings_perms": ("claimSamplingBatch", "approve"),
}

RIGHTS = RightsDeclaration(MODULE_NAME, DJANGO_PERMS, _PERM_CFG)

perms = RIGHTS.perms
django_perms = RIGHTS.django_perm_names
configured_perms = RIGHTS.configured
require = RIGHTS.require


DEFAULT_CFG = {
}


class ClaimSamplingConfig(AppConfig):
    name = MODULE_NAME

    # Rights: constants, no longer overridable. They go neither through DEFAULT_CFG
    # nor through ready(): `ModuleConfiguration.get_or_default` now ignores any
    # `_perms` key stored in the database.
    gql_query_claim_batch_samplings_perms = RIGHTS.perms("claimSamplingBatch", "query")
    gql_mutation_create_claim_batch_samplings_perms = RIGHTS.perms("claimSamplingBatch", "create")
    gql_mutation_update_claim_batch_samplings_perms = RIGHTS.perms("claimSamplingBatch", "update")
    gql_mutation_approve_claim_batch_samplings_perms = RIGHTS.perms("claimSamplingBatch", "approve")

    def __load_config(self, cfg):
        for field in cfg:
            if hasattr(ClaimSamplingConfig, field):
                setattr(ClaimSamplingConfig, field, cfg[field])

    def ready(self):
        from core.models import ModuleConfiguration
        cfg = ModuleConfiguration.get_or_default(MODULE_NAME, DEFAULT_CFG)
        self.__load_config(cfg)
