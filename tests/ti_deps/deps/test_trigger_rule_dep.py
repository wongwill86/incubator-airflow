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
from datetime import datetime, timedelta

import math
import airflow
from airflow import settings
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import BaseOperator, DagModel, DagRun, TaskInstance, DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils.state import State
from airflow.utils.weight_rule import WeightRule

DEFAULT_DATE = datetime(2016, 1, 1)

class TriggerRuleDepTest(unittest.TestCase):
    def setUp(self):
        session = settings.Session()
        session.query(DagModel).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()
        session.commit()

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
        dag_id = 'TriggerRuleDepTest.test_upstream_ready'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        # extra dag to make sure we're selecting from the correct dag tasks
        misc_dag = DAG(dag_id='misc_dag', start_date=DEFAULT_DATE)

        session = settings.Session()
        upstream_tasks = []
        misc_tasks = []
        upstream_parents = 2000
        with dag:
            end = DummyOperator(task_id='downstream_task')
            for task_id in range(0, upstream_parents):
                upstream = DummyOperator(task_id='upstream_task_%s' % task_id,
                                         weight_rule=WeightRule.ABSOLUTE)
                upstream.set_downstream(end)
                upstream_tasks.append(upstream)

            # tasks added to ensure that we are selecting the correct task ids
            for task_id in range(0, 2 * upstream_parents):
                misc_tasks.append(DummyOperator(task_id='misc_task_%s' % task_id,
                                                weight_rule=WeightRule.ABSOLUTE))
                misc_tasks.append(DummyOperator(
                    task_id='misc_dag_misc_task_%s' % task_id,
                    dag=misc_dag,
                    weight_rule=WeightRule.ABSOLUTE
                ))


        dag.clear()
        dagrun = dag.create_dagrun(run_id="test",
                               state=State.SUCCESS,
                               execution_date=DEFAULT_DATE,
                               start_date=DEFAULT_DATE,
                               session=session)

        # extra dag run to make sure we're selecting the first dagrun only
        dagrun_other = dag.create_dagrun(run_id="test_other",
                               state=State.SUCCESS,
                               execution_date=DEFAULT_DATE + timedelta(hours=1),
                               start_date=DEFAULT_DATE,
                               session=session)
        misc_dagrun = misc_dag.create_dagrun(run_id="misc_test",
                               state=State.SUCCESS,
                               execution_date=DEFAULT_DATE,
                               start_date=DEFAULT_DATE,
                               session=session)

        for index in range(0, int(len(upstream_tasks)/2)):
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
                task_instance = TaskInstance(upstream_tasks[index], dagrun.execution_date)
                task_instance.set_state(next_state, session)
                task_instance_other = TaskInstance(upstream_tasks[index],
                                                   dagrun_other.execution_date)
                task_instance_other.set_state(next_state, session)

            misc_dag_task_instance = TaskInstance(misc_tasks[index*2],
                                                  dagrun.execution_date)

        end_task_instance = dagrun.get_task_instance(task_id=end.task_id)
        end_task_instance.task = end

        stats = TriggerRuleDep()._query_upstream_stats(end_task_instance, session)

        self.assertEqual(stats.successes, 100)
        self.assertEqual(stats.skipped, 101)
        self.assertEqual(stats.failed, 102)
        self.assertEqual(stats.upstream_failed, 103)
        self.assertEqual(stats.done, 100 + 101 + 102 + 103)
