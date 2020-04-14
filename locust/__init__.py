from .core import WebLocust, Locust, TaskSet, TaskSequence, task, seq_task, mod_context
from .exception import InterruptTaskSet, ResponseError, RescheduleTaskImmediately
from .config import configure, locust_config, register_config

__version__ = "0.8dr6"
