
from airflow.plugins_manager import AirflowPlugin
from timout.operators.completion_operator import CompletionOperator
from timout.operators.multidagrun_operator import TriggerMultipleDagRunOperator


class TriggerOperatorPlugin(AirflowPlugin):
    name = "TriggerOperatorPlugin"
    operators = [CompletionOperator, TriggerMultipleDagRunOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []