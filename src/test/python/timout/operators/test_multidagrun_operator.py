from unittest import TestCase, mock

import datetime
from timout.operators.multidagrun_operator import TriggerMultipleDagRunOperator, TriggerDag


class TriggerMultipleDagRunOperatorTests(TestCase):

    @mock.patch('airflow.utils.timezone.utcnow', return_value=datetime.datetime(2000, 1, 1))
    def test_run_id(self, mock_utcnow):
        op = TriggerMultipleDagRunOperator(python_callable=None, task_id='t')
        self.assertEqual(op._get_run_id("t", "a"), "trig__t__a")
        self.assertEqual(op._get_run_id("t", None), "trig__t__2000-01-01T00:00:00")

    def test_get_execution_date(self):
        op = TriggerMultipleDagRunOperator(python_callable=None, task_id='t')
        self.assertFalse(op._get_execution_date(None))
        self.assertEqual(op._get_execution_date("test"), "test")
        self.assertEqual(op._get_execution_date(datetime.datetime(2000, 1, 1)), "2000-01-01T00:00:00")
        self.assertRaises(TypeError, op._get_execution_date, [])

    def test_convert_to_dag_empty(self):
        op = TriggerMultipleDagRunOperator(python_callable=None, task_id='t')
        self.assertFalse(op._convert_to_dag(None))
        self.assertFalse(op._convert_to_dag([]))
        self.assertFalse(op._convert_to_dag({}))
        self.assertFalse(op._convert_to_dag({'test':123}))

    @mock.patch('airflow.utils.timezone.utcnow', return_value=datetime.datetime(2000, 1, 1))
    def test_convert_to_dag_no_ex_date(self, mock_utcnow):
        op = TriggerMultipleDagRunOperator(python_callable=None, task_id='t')
        self.assertEqual(op._convert_to_dag({'trigger_dag_id': 't'}), TriggerDag(
            trigger_dag_id="t",
            run_id="trig__t__2000-01-01T00:00:00",
            execution_date=None,
            payload=None
        ))

    def test_convert_to_dag_ex_date(self):
        op = TriggerMultipleDagRunOperator(python_callable=None, task_id='t')
        self.assertEqual(op._convert_to_dag({'trigger_dag_id': 't', 'execution_date': '2000-01-01T00:00:00'}),
                         TriggerDag(
                             trigger_dag_id="t",
                             run_id="trig__t__2000-01-01T00:00:00",
                             execution_date=datetime.datetime(2000, 1, 1),
                             payload=None
                         ))

    def test_convert_to_dag_payload(self):
        op = TriggerMultipleDagRunOperator(python_callable=None, task_id='t')
        self.assertEqual(op._convert_to_dag({'trigger_dag_id': 't',
                                             'execution_date': '2000-01-01T00:00:00',
                                             'payload': {'test': '1'}}),
                         TriggerDag(
                             trigger_dag_id="t",
                             run_id="trig__t__2000-01-01T00:00:00",
                             execution_date=datetime.datetime(2000, 1, 1),
                             payload={'test': '1'}
                         ))

    def test_call_none(self):
        op = TriggerMultipleDagRunOperator(python_callable=None, task_id='t')
        self.assertFalse(op._call({}))

    def test_call_not_list(self):
        def fn(context):
            return {'trigger_dag_id': '123'}
        op = TriggerMultipleDagRunOperator(python_callable=fn, task_id='t')
        self.assertFalse(op._call({}))

    def test_call_empty_list(self):
        def fn(context):
            return []
        op = TriggerMultipleDagRunOperator(python_callable=fn, task_id='t',)
        self.assertFalse(op._call({}))

    def test_call_2_trigger_dags(self):
        def fn(context):
            return [{'trigger_dag_id':'t1', 'test1':'tt1'}, {'trigger_dag_id':'t2', 'test1':'tt2'}]
        op = TriggerMultipleDagRunOperator(python_callable=fn, task_id='t')
        self.assertEqual(op._call({}),
                         [{'trigger_dag_id':'t1', 'test1':'tt1'}, {'trigger_dag_id':'t2', 'test1':'tt2'}]
                         )


    @mock.patch('airflow.utils.timezone.utcnow', return_value=datetime.datetime(2000, 1, 1))
    @mock.patch('airflow.api.common.experimental.trigger_dag.trigger_dag', return_value=[])
    def test_execute(self, mock_trigger, mock_utcnow):
        def fn(context):
            return [
                {'trigger_dag_id': 't1', 'test1': 'tt1'},
                {'trigger_dag_id': 't2', 'execution_date': '2000-01-01T00:00:00'},
                {'dag_id': 't3', 'test1': 'tt2'}]
        op = TriggerMultipleDagRunOperator(python_callable=fn, task_id='t')
        op.execute({})
        self.assertEqual(mock_trigger.call_count, 2)
