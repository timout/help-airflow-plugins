import datetime
import six
import json
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.configuration import conf


class CompletionOperator(BaseOperator):
    """
    Send message to Dag Trigger Service to trigger dependent dags

    :param python_callable: a reference to a python function that will be
        called while passing it the ``context`` object for your callable
        to fill and return payload for dependent dags.
        The payload has to be a picklable object that will be made available
        to your tasks while executing that DAG run. Your function header
        should look like ``def foo(context):``
    :type python_callable: python callable
    :param execution_date: Execution date for the dag (templated)
    :type execution_date: str or datetime.datetime
    :param endpoint: Completion Service Endpoint (templated)
    :type endpoint: str
    """
    template_fields = ('execution_date', 'endpoint')
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            python_callable=None,
            execution_date=None,
            endpoint=None,
            *args,
            **kwargs):
        super(CompletionOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.execution_date = self._get_execution_date(execution_date)
        self.endpoint = self._get_endpoint(endpoint)

    def execute(self, context):
        payload = self._get_call_conf(context)
        self._call_service(payload)

    def _call_service(self, payload):
        import requests
        self.log.info(f"Calling {self.endpoint} for {self.dag_id}")
        r = requests.post(self.endpoint, json={"dag": self.dag_id,
                                          "execution_time": self.execution_date,
                                          "conf": json.dumps(payload)})
        self.log.info(f"Called {self.endpoint} for {self.dag_id} with response code {r.status_code}")

    def _get_call_conf(self, context):
        if self.python_callable is not None:
            return self.python_callable(context)
        return None

    def _get_execution_date(self, execution_date):
        if isinstance(execution_date, datetime.datetime):
            return execution_date.isoformat()
        elif isinstance(execution_date, six.string_types):
            return execution_date
        elif execution_date is None:
            return execution_date
        else:
            raise TypeError('Expected str or datetime.datetime type for execution_date. Got {}'.format(
                type(execution_date)))

    def _get_endpoint(self, endpoint):
        if endpoint:
            return endpoint
        endpoint = conf.get('operators', 'completion_service_endpoint')  # type: str
        if not endpoint:
            raise TypeError('Parameter operators.completion_service_endpoint is missing')
        return endpoint
