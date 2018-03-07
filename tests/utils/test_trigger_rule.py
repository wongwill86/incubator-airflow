import unittest
from airflow.utils.trigger_rule import TriggerRule

class TestTriggerRule(unittest.TestCase):

    def test_valid_trigger_rules(self):
        self.assertTrue(TriggerRule.is_valid(TriggerRule.ALL_SUCCESS))
        self.assertTrue(TriggerRule.is_valid(TriggerRule.ALL_FAILED))
        self.assertTrue(TriggerRule.is_valid(TriggerRule.ALL_DONE))
        self.assertTrue(TriggerRule.is_valid(TriggerRule.ONE_SUCCESS))
        self.assertTrue(TriggerRule.is_valid(TriggerRule.ONE_FAILED))
        self.assertTrue(TriggerRule.is_valid(TriggerRule.DUMMY))
        self.assertEqual(len(TriggerRule.all_triggers()), 6)
