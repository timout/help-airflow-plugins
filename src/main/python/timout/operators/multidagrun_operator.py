import datetime
from typing import Callable, Union, Optional, Dict, List, NamedTuple, Any

from airflow.models import BaseOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults
from airflow.api.common.experimental import trigger_dag

import json


class TriggerDag(NamedTuple):
    trigger_dag_id: str
    run_id: str
    execution_date: Optional[datetime.datetime]
    payload: Any


class TriggerMultipleDagRunOperator(BaseOperator):
    """
    Triggers DAG run-s for multiple dag ``dag_id``-s

    :param python_callable: a reference to a python function that will be
        called while passing it the ``context`` object.
        It should return List of Dict-s with the following fields: and a placeholder
        trigger_dag_id - dag to trigger
        execution_date: Execution date for the trigger_dag_id
        payload: a picklable object that will be made available
        to your tasks while executing that DAG run.
        Your function header should look like ``def foo(context: Dict) -> List[Dict]:``
    """
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(self, python_callable: Optional[Callable[[Dict], List[Dict]]], *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.python_callable = python_callable

    def execute(self, context):
        callable_res = self._call(context)
        dags_to_trigger = filter(lambda d: d, map(self._convert_to_dag, callable_res))
        for d in dags_to_trigger:
            trigger_dag.trigger_dag(dag_id=d.trigger_dag_id,
                                    run_id=d.run_id,
                                    conf=json.dumps(d.payload),
                                    execution_date=d.execution_date,
                                    replace_microseconds=False)

    def _call(self, context) -> List[Dict]:
        if self.python_callable is None:
            self.log.info("Python callable is None")
        else:
            res = self.python_callable(context)
            if type(res) is list:
                return res
            else:
                self.log.info(f"Python callable return is not a list: {res}")
        return []

    def _convert_to_dag(self, d: Optional[Dict]) -> Optional[TriggerDag]:
        if type(d) is dict and d.get('trigger_dag_id'):
            d_id = d.get('trigger_dag_id')
            ex_date = self._get_execution_date(d.get('execution_date'))
            run_id = self._get_run_id(d_id, ex_date)
            return TriggerDag(
                trigger_dag_id=d_id,
                run_id=run_id,
                execution_date=timezone.parse(ex_date) if ex_date else None,
                payload=d.get('payload')
            )
        else:
            return None

    def _get_execution_date(self, execution_date: Optional[Union[str, datetime.datetime]]) -> Optional[str]:
        if isinstance(execution_date, datetime.datetime):
            return execution_date.isoformat()
        elif isinstance(execution_date, str):
            return execution_date
        elif execution_date is None:
            return None
        else:
            raise TypeError(f'Expected str or datetime.datetime type for execution_date. Got {execution_date}')

    def _get_run_id(self, dag_id: str, execution_date: Optional[str]) -> str:
        if execution_date is not None:
            return f'trig__{dag_id}__{execution_date}'
        else:
            return f'trig__{dag_id}__{timezone.utcnow().isoformat()}'
