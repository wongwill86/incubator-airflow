# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from datetime import datetime

import math
import airflow
from airflow import settings
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import BaseOperator, TaskInstance, DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils.state import State
from airflow.utils.weight_rule import WeightRule

DEFAULT_DATE = datetime(2016, 1, 1)

class TriggerRuleDepTest(unittest.TestCase):

    def _get_task_instance(self, trigger_rule=TriggerRule.ALL_SUCCESS,
                           state=None, upstream_task_ids=None):
        task = BaseOperator(task_id='test_task', trigger_rule=trigger_rule,
                            start_date=datetime(2015, 1, 1))
        if upstream_task_ids:
            task._upstream_task_ids.update(upstream_task_ids)
        return TaskInstance(task=task, state=state, execution_date=None)

    def test_no_upstream_tasks(self):
        """
        If the TI has no upstream TIs then there is nothing to check and the dep is passed
        """
        ti = self._get_task_instance(TriggerRule.ALL_DONE, State.UP_FOR_RETRY)
        self.assertTrue(TriggerRuleDep().is_met(ti=ti))

    def test_dummy_tr(self):
        """
        The dummy trigger rule should always pass this dep
        """
        ti = self._get_task_instance(TriggerRule.DUMMY, State.UP_FOR_RETRY)
        self.assertTrue(TriggerRuleDep().is_met(ti=ti))

    def test_one_success_tr_success(self):
        """
        One-success trigger rule success
        """
        ti = self._get_task_instance(TriggerRule.ONE_SUCCESS, State.UP_FOR_RETRY)
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=1,
            skipped=2,
            failed=2,
            upstream_failed=2,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 0)

    def test_one_success_tr_failure(self):
        """
        One-success trigger rule failure
        """
        ti = self._get_task_instance(TriggerRule.ONE_SUCCESS)
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=0,
            skipped=2,
            failed=2,
            upstream_failed=2,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 1)
        self.assertFalse(dep_statuses[0].passed)

    def test_one_failure_tr_failure(self):
        """
        One-failure trigger rule failure
        """
        ti = self._get_task_instance(TriggerRule.ONE_FAILED)
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=2,
            skipped=0,
            failed=0,
            upstream_failed=0,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 1)
        self.assertFalse(dep_statuses[0].passed)

    def test_one_failure_tr_success(self):
        """
        One-failure trigger rule success
        """
        ti = self._get_task_instance(TriggerRule.ONE_FAILED)
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=0,
            skipped=2,
            failed=2,
            upstream_failed=0,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 0)

        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=0,
            skipped=2,
            failed=0,
            upstream_failed=2,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 0)

    def test_all_success_tr_success(self):
        """
        All-success trigger rule success
        """
        ti = self._get_task_instance(TriggerRule.ALL_SUCCESS,
                                     upstream_task_ids=["FakeTaskID"])
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=1,
            skipped=0,
            failed=0,
            upstream_failed=0,
            done=1,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 0)

    def test_all_success_tr_failure(self):
        """
        All-success trigger rule failure
        """
        ti = self._get_task_instance(TriggerRule.ALL_SUCCESS,
                                     upstream_task_ids=["FakeTaskID",
                                                        "OtherFakeTaskID"])
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=1,
            skipped=0,
            failed=1,
            upstream_failed=0,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 1)
        self.assertFalse(dep_statuses[0].passed)

    def test_all_failed_tr_success(self):
        """
        All-failed trigger rule success
        """
        ti = self._get_task_instance(TriggerRule.ALL_FAILED,
                                     upstream_task_ids=["FakeTaskID",
                                                        "OtherFakeTaskID"])
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=0,
            skipped=0,
            failed=2,
            upstream_failed=0,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 0)

    def test_all_failed_tr_failure(self):
        """
        All-failed trigger rule failure
        """
        ti = self._get_task_instance(TriggerRule.ALL_FAILED,
                                     upstream_task_ids=["FakeTaskID",
                                                        "OtherFakeTaskID"])
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=2,
            skipped=0,
            failed=0,
            upstream_failed=0,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 1)
        self.assertFalse(dep_statuses[0].passed)

    def test_all_done_tr_success(self):
        """
        All-done trigger rule success
        """
        ti = self._get_task_instance(TriggerRule.ALL_DONE,
                                     upstream_task_ids=["FakeTaskID",
                                                        "OtherFakeTaskID"])
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=2,
            skipped=0,
            failed=0,
            upstream_failed=0,
            done=2,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 0)

    def test_all_done_tr_failure(self):
        """
        All-done trigger rule failure
        """
        ti = self._get_task_instance(TriggerRule.ALL_DONE,
                                     upstream_task_ids=["FakeTaskID",
                                                        "OtherFakeTaskID"])
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=1,
            skipped=0,
            failed=0,
            upstream_failed=0,
            done=1,
            flag_upstream_failed=False,
            session="Fake Session"))
        self.assertEqual(len(dep_statuses), 1)
        self.assertFalse(dep_statuses[0].passed)

    def test_unknown_tr(self):
        """
        Unknown trigger rules should cause this dep to fail
        """
        ti = self._get_task_instance()
        ti.task.trigger_rule = "Unknown Trigger Rule"
        dep_statuses = tuple(TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=1,
            skipped=0,
            failed=0,
            upstream_failed=0,
            done=1,
            flag_upstream_failed=False,
            session="Fake Session"))

        self.assertEqual(len(dep_statuses), 1)
        self.assertFalse(dep_statuses[0].passed)

    def test_query_upstream_stats(self):
        """
        Test returns that all upstream are ready to be processed
        """
        import datetime
        start = datetime.datetime.now()
        dag_id = 'TriggerRuleDepTest.test_upstream_ready'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)

        session = settings.Session()
        __import__('pdb').set_trace()
        upstream_tasks = []
        misc_tasks = []
        width = 50
        with dag:
            end = DummyOperator(task_id='downstream_task')
            for task_id in range(0, width):
                upstream = DummyOperator(task_id='upstream_task_%s' % task_id,
                                         weight_rule=WeightRule.ABSOLUTE)
                upstream.set_downstream(end)
                upstream_tasks.append(upstream)

            for task_id in range(0, 2 * width):
                misc = DummyOperator(task_id='misc_task_%s' % task_id)
                misc_tasks.append(misc)


        dag.clear()
        dr = dag.create_dagrun(run_id="test",
                               state=State.SUCCESS,
                               execution_date=DEFAULT_DATE,
                               start_date=DEFAULT_DATE,
                               session=session)
        TI = airflow.models.TaskInstance

        print('create elapsed %s' % (datetime.datetime.now() - start))
        for index in range(0, len(upstream_tasks)):
            ti = TI(upstream_tasks[index], dr.execution_date)
            next_state = State.NONE
            if index < 100:
                next_state = State.SUCCESS
            elif index < 201:
                next_state = State.SKIPPED
            elif index < 303:
                next_state = State.FAILED
            elif index < 406:
                next_state = State.UPSTREAM_FAILED
            elif index < 510:
                next_state = State.UP_FOR_RETRY
            elif index < 615:
                next_state = State.RUNNING
            if next_state:
                ti.set_state(next_state, session)

        ti = dr.get_task_instance(task_id=end.task_id)
        ti.task = end
        import cProfile
        import io
        import pstats
        import contextlib

        @contextlib.contextmanager
        def profiled():
            pr = cProfile.Profile()
            pr.enable()
            yield
            pr.disable()
            s = io.StringIO()
            ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
            ps.print_stats()
            # uncomment this to see who's calling what
            # ps.print_callers()
            print(s.getvalue())

        start = datetime.datetime.now()
        # with profiled():
        for i in range(0, 10):
            stats = TriggerRuleDep()._query_upstream_stats_old(ti, session)
        print('trigger eval time old %s' % (datetime.datetime.now() - start))

        start = datetime.datetime.now()
        # with profiled():
        for i in range(0, 10):
            stats = TriggerRuleDep()._query_upstream_stats(ti, session)
        print('trigger eval time new %s' % (datetime.datetime.now() - start))

        # start = datetime.datetime.now()
        # for i in range(0, 10):
            # stats = TriggerRuleDep()._query_upstream_stats_raw_mysql(ti, session)
        # print('trigger eval time raw %s' % (datetime.datetime.now() - start))

        self.assertEqual(stats.successes, 100)
        self.assertEqual(stats.skipped, 101)
        self.assertEqual(stats.failed, 102)
        self.assertEqual(stats.upstream_failed, 103)
        self.assertEqual(stats.done, 100 + 101 + 102 + 103)
        self.assertTrue(False)
