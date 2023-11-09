from django.apps import AppConfig

MODULE_NAME = 'claim_sampling'

DEFAULT_CFG = {
    "gql_query_claim_batch_samplings_perms": ["126001"],
    "gql_mutation_create_claim_batch_samplings_perms": ["126002"],
    "gql_mutation_update_claim_batch_samplings_perms": ["126003"],
    "gql_mutation_approve_claim_batch_samplings_perms": ["126004"],
}


class ClaimSamplingConfig(AppConfig):
    name = MODULE_NAME

    gql_query_claim_batch_samplings_perms = None
    gql_mutation_create_claim_batch_samplings_perms = None
    gql_mutation_update_claim_batch_samplings_perms = None
    gql_mutation_approve_claim_batch_samplings_perms = None

    def __load_config(self, cfg):
        for field in cfg:
            if hasattr(ClaimSamplingConfig, field):
                setattr(ClaimSamplingConfig, field, cfg[field])

    def ready(self):
        from core.models import ModuleConfiguration
        cfg = ModuleConfiguration.get_or_default(MODULE_NAME, DEFAULT_CFG)
        self.__load_config(cfg)
