import unittest
from airflow.utils.weight_rule import WeightRule

class TestWeightRule(unittest.TestCase):

    def test_valid_weight_rules(self):
        self.assertTrue(WeightRule.is_valid(WeightRule.DOWNSTREAM))
        self.assertTrue(WeightRule.is_valid(WeightRule.UPSTREAM))
        self.assertTrue(WeightRule.is_valid(WeightRule.ABSOLUTE))
        self.assertEqual(len(WeightRule.all_weight_rules()), 3)
