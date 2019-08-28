
from airflow.plugins_manager import AirflowPlugin
from timout.operators.completion_operator import CompletionOperator

class CompletionOperatorPlugin(AirflowPlugin):
    name = "CompletionOperatorPlugin"
    operators = [CompletionOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []