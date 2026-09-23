"""
Guard rails on claim_sampling's rights declaration.

Same structure as `claim`: `DJANGO_PERMS` by entity then by action, `_PERM_CFG`
deriving the config keys from it, and `ClaimSamplingBatch.get_rights` which is only an
access point.

What is locked down here is the entity/action pair, not only the values:
  * an identifier in one place only (DJANGO_PERMS), hence no drift between the
    DEFAULT_CFG and the check;
  * a config key with no class attribute is never loaded by `__load_config` and
    reading it raises AttributeError - the right becomes unenforceable;
  * `has_perms([])` returns True, so an empty list grants to everybody;
  * an identifier missing from `permissions_map.json` is grantable to no role, so the
    feature it guards becomes unreachable instead of protected.

The file sits flat rather than in a `tests/` package: the module already has a
`tests.py`, which a package of the same name would shadow.
"""

import json
from pathlib import Path

from django.conf import settings
from django.test import TestCase

from claim_sampling.apps import (
    DJANGO_PERMS,
    ClaimSamplingConfig,
    _PERM_CFG,
    configured_perms,
    django_perms,
    perms,
)
from claim_sampling.models import ClaimSamplingBatch, ClaimSamplingBatchAssignment

# The identifiers as deployed. Changing one is incompatible with the existing roles:
# this test has to be updated *and* the new right granted.
EXPECTED_RIGHTS = {
    "gql_query_claim_batch_samplings_perms": ["126001"],
    "gql_mutation_create_claim_batch_samplings_perms": ["126002"],
    "gql_mutation_update_claim_batch_samplings_perms": ["126003"],
    "gql_mutation_approve_claim_batch_samplings_perms": ["126004"],
}

# The assembly's catalogue, not the package's: the modules are installed from a
# separate tree, so it is resolved from BASE_DIR.
PERMISSIONS_MAP = Path(settings.BASE_DIR) / "permissions_map.json"


class ClaimSamplingPermissionDeclarationTestCase(TestCase):
    def test_right_ids_unchanged(self):
        self.assertEqual(
            {key: getattr(ClaimSamplingConfig, key) for key in EXPECTED_RIGHTS},
            EXPECTED_RIGHTS,
        )

    def test_perm_cfg_covers_every_declared_action(self):
        declared = {
            (entity, action)
            for entity, actions in DJANGO_PERMS.items()
            for action in actions
        }
        self.assertEqual(set(_PERM_CFG.values()), declared)

    def test_perm_cfg_matches_config_attributes(self):
        """`__load_config` ignores the keys with no class attribute."""
        missing = [key for key in _PERM_CFG if not hasattr(ClaimSamplingConfig, key)]
        self.assertEqual(missing, [])

    def test_no_right_list_is_empty(self):
        empty = [key for key in _PERM_CFG if not getattr(ClaimSamplingConfig, key)]
        self.assertEqual(empty, [])

    def test_attributes_carry_the_declared_right(self):
        """
        The rights are constants set from DJANGO_PERMS: the attribute must equal the
        declaration, without going through the config.
        """
        for key, (entity, action) in _PERM_CFG.items():
            with self.subTest(key=key):
                self.assertEqual(getattr(ClaimSamplingConfig, key), perms(entity, action))

    def test_no_right_id_is_shared(self):
        """
        The module's four rights are distinct. Sharing is possible in itself -
        `RightPermission.right_id` is not unique - but it has to be a written decision,
        not the result of a copy-paste.
        """
        seen = {}
        for entity, actions in DJANGO_PERMS.items():
            for action, (_, right_id) in actions.items():
                seen.setdefault(right_id, []).append((entity, action))
        shared = {rid: who for rid, who in seen.items() if len(who) > 1}
        self.assertEqual(shared, {})

    def test_django_permission_names_are_unique(self):
        seen = {}
        for entity, actions in DJANGO_PERMS.items():
            for action, (name, _) in actions.items():
                seen.setdefault(name, []).append(f"{entity}.{action}")
        shared = {name: who for name, who in seen.items() if len(who) > 1}
        self.assertEqual(shared, {})

    def test_every_right_id_is_in_the_permissions_map(self):
        """
        The map is what the solution builder seeds the roles from: an identifier that
        is not in it can be granted to nobody.
        """
        catalog = set(json.loads(PERMISSIONS_MAP.read_text(encoding="utf-8")).values())
        missing = sorted(
            rid
            for entity, actions in DJANGO_PERMS.items()
            for _, (_, right_id) in actions.items()
            for rid in [str(right_id)]
            if rid not in catalog
        )
        self.assertEqual(missing, [])

    def test_unknown_entity_or_action_raises(self):
        with self.assertRaises(KeyError):
            perms("nosuchentity", "query")
        with self.assertRaises(KeyError):
            perms("claimSamplingBatch", "nosuchaction")
        with self.assertRaises(KeyError):
            django_perms("claimSamplingBatch", "nosuchaction")

    # --- the access point through the model -------------------------------
    def test_model_exposes_every_action_of_its_entity(self):
        for action in DJANGO_PERMS["claimSamplingBatch"]:
            with self.subTest(action=action):
                self.assertEqual(
                    ClaimSamplingBatch.get_rights(action),
                    configured_perms("claimSamplingBatch", action),
                )
                self.assertTrue(ClaimSamplingBatch.get_rights(action))

    def test_model_returns_none_for_an_undeclared_action(self):
        """None means "no rule": the caller must fail closed."""
        self.assertIsNone(ClaimSamplingBatch.get_rights("nosuchaction"))

    def test_model_reads_the_configured_value_not_the_declared_default(self):
        """
        ModuleConfiguration may override a right; the check must read the configured
        value, where `perms()` returns the declared default.
        """
        original = ClaimSamplingConfig.gql_query_claim_batch_samplings_perms
        try:
            ClaimSamplingConfig.gql_query_claim_batch_samplings_perms = ["999999"]
            self.assertEqual(ClaimSamplingBatch.get_rights("query"), ["999999"])
            self.assertEqual(perms("claimSamplingBatch", "query"), ["126001"])
        finally:
            ClaimSamplingConfig.gql_query_claim_batch_samplings_perms = original

    def test_assignment_declares_the_batch_as_its_scope_parent(self):
        """
        An assignment has no right of its own: it borrows the batch's. The field has to
        denote a real relation, otherwise `core.rights_scope` walks up to nothing and
        refuses everything.
        """
        self.assertEqual(ClaimSamplingBatchAssignment.scope_parent, "claim_batch")
        field = ClaimSamplingBatchAssignment._meta.get_field("claim_batch")
        self.assertEqual(field.related_model, ClaimSamplingBatch)
