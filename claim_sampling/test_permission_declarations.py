"""
Garde-fous sur la declaration des droits de claim_sampling.

Meme structure que `claim` : `DJANGO_PERMS` par entite puis par action, `_PERM_CFG` qui
en derive les cles de config, et `ClaimSamplingBatch.get_rights` qui n'est qu'un point
d'acces.

Ce qui est verrouille ici, c'est le couple entite/action, pas seulement les valeurs :
  * un identifiant a un seul endroit (DJANGO_PERMS), donc pas de derive entre le
    DEFAULT_CFG et le controle ;
  * une cle de config sans attribut de classe n'est jamais chargee par `__load_config`
    et sa lecture leve AttributeError - le droit devient inapplicable ;
  * `has_perms([])` renvoie True, donc une liste vide accorde a tous ;
  * un identifiant absent de `permissions_map.json` n'est accordable a aucun role, donc
    la fonctionnelle qu'il garde devient injoignable au lieu d'etre protegee.

Le fichier est pose a plat plutot que dans un paquet `tests/` : le module a deja un
`tests.py`, qu'un paquet du meme nom masquerait.
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

# Les identifiants tels que deployes. En changer un est incompatible avec les roles
# existants : il faut mettre ce test a jour *et* accorder le nouveau droit.
EXPECTED_RIGHTS = {
    "gql_query_claim_batch_samplings_perms": ["126001"],
    "gql_mutation_create_claim_batch_samplings_perms": ["126002"],
    "gql_mutation_update_claim_batch_samplings_perms": ["126003"],
    "gql_mutation_approve_claim_batch_samplings_perms": ["126004"],
}

# Le catalogue de l'assemblage, pas celui du paquet : les modules sont installes depuis
# un arbre separe, donc on le resout depuis BASE_DIR.
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
        """`__load_config` ignore les cles sans attribut de classe."""
        missing = [key for key in _PERM_CFG if not hasattr(ClaimSamplingConfig, key)]
        self.assertEqual(missing, [])

    def test_no_right_list_is_empty(self):
        empty = [key for key in _PERM_CFG if not getattr(ClaimSamplingConfig, key)]
        self.assertEqual(empty, [])

    def test_attributes_carry_the_declared_right(self):
        """
        Les droits sont des constantes posees depuis DJANGO_PERMS : l'attribut doit
        valoir la declaration, sans passer par la config.
        """
        for key, (entity, action) in _PERM_CFG.items():
            with self.subTest(key=key):
                self.assertEqual(getattr(ClaimSamplingConfig, key), perms(entity, action))

    def test_no_right_id_is_shared(self):
        """
        Les quatre droits du module sont distincts. Un partage est possible en soi -
        `RightPermission.right_id` n'est pas unique - mais il doit etre une decision
        ecrite, pas le resultat d'un copier-coller.
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
        La carte est ce depuis quoi le solution builder seme les roles : un identifiant
        qui n'y est pas ne peut etre accorde a personne.
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

    # --- le point d'acces par le modele -----------------------------------
    def test_model_exposes_every_action_of_its_entity(self):
        for action in DJANGO_PERMS["claimSamplingBatch"]:
            with self.subTest(action=action):
                self.assertEqual(
                    ClaimSamplingBatch.get_rights(action),
                    configured_perms("claimSamplingBatch", action),
                )
                self.assertTrue(ClaimSamplingBatch.get_rights(action))

    def test_model_returns_none_for_an_undeclared_action(self):
        """None signifie "aucune regle" : l'appelant doit echouer ferme."""
        self.assertIsNone(ClaimSamplingBatch.get_rights("nosuchaction"))

    def test_model_reads_the_configured_value_not_the_declared_default(self):
        """
        ModuleConfiguration peut surcharger un droit ; le controle doit lire la valeur
        configuree, la ou `perms()` renvoie le defaut declare.
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
        Une assignation n'a pas de droit propre : elle emprunte celui du lot. Le champ
        doit designer une relation reelle, sinon `core.rights_scope` ne remonte rien et
        refuse tout.
        """
        self.assertEqual(ClaimSamplingBatchAssignment.scope_parent, "claim_batch")
        field = ClaimSamplingBatchAssignment._meta.get_field("claim_batch")
        self.assertEqual(field.related_model, ClaimSamplingBatch)
