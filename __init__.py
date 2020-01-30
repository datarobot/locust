from locust.core import WebLocust, Locust, TaskSet, task, mod_context
from locust.exception import InterruptTaskSet, ResponseError, RescheduleTaskImmediately
from locust.config import configure, locust_config, register_config

__version__ = "0.8dr6"
