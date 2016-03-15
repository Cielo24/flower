from __future__ import absolute_import

from collections import defaultdict

from tornado import web
from tornado import gen
from celery import states

from ..views import BaseHandler
from ..utils.broker import Broker
from ..api.control import ControlHandler


def avg(total, count):
    return (total / count) if count > 0 else 0.0


class Monitor(BaseHandler):
    @web.authenticated
    def get(self):
        self.render("monitor.html")


class SucceededTaskMonitor(BaseHandler):
    @web.authenticated
    def get(self):
        timestamp = self.get_argument('lastquery', type=float)
        state = self.application.events.state

        data = defaultdict(int)
        for _, task in state.itertasks():
            if (timestamp < task.timestamp and task.state == states.SUCCESS):
                data[task.worker.hostname] += 1
        for worker in state.workers:
            if worker not in data:
                data[worker] = 0

        self.write(data)


class TimeToCompletionMonitor(BaseHandler):
    @web.authenticated
    def get(self):
        timestamp = self.get_argument('lastquery', type=float)
        state = self.application.events.state

        execute_time = 0
        queue_time = 0
        num_tasks = 0
        for _, task in state.itertasks():
            if (timestamp < task.timestamp and task.state == states.SUCCESS):
                # eta can make "time in queue" look really scary.
                if task.eta is not None:
                    continue

                if task.started is None or task.received is None or\
                        task.succeeded is None:
                    continue

                queue_time += task.started - task.received
                execute_time += task.succeeded - task.started
                num_tasks += 1

        avg_queue_time = (queue_time / num_tasks) if num_tasks > 0 else 0
        avg_execution_time = (execute_time / num_tasks) if num_tasks > 0 else 0

        result = {
            "Time in a queue": avg_queue_time,
            "Execution time": avg_execution_time,
        }
        self.write(result)


class TaskBreakdownMonitor(BaseHandler):
    @web.authenticated
    def get(self):
        timestamp = self.get_argument('lastquery', type=float)
        state = self.application.events.state

        task_results = {}
        total_average_queue_time = 0
        total_average_execution_time = 0
        total_num_tasks = 0

        for _, task in state.itertasks():

            if timestamp < task.timestamp and task.state == states.SUCCESS:
                # eta can make "time in queue" look really scary.
                if task.eta is not None:
                    continue

                if task.sent is None or task.started is None or task.succeeded is None:
                    continue

                queue_time = task.started - task.sent
                execution_time = task.succeeded - task.started

                entry = task_results.get(task.name, {'queue_time': 0.0, 'execute_time': 0.0, 'num_tasks': 0.0})
                entry['queue_time'] += queue_time
                entry['execute_time'] += execution_time
                entry['num_tasks'] += 1
                task_results[task.name] = entry

                total_average_queue_time += queue_time
                total_average_execution_time += execution_time
                total_num_tasks += 1

        results = {
            'Total average time in queue': avg(total_average_queue_time, total_num_tasks),
            'Total average execution time': avg(total_average_execution_time, total_num_tasks)
        }

        for name, entry in task_results.iteritems():
            queue_time_title = '{} average time in queue'.format(name)
            execution_time_title = '{} average execution time'.format(name)
            results[queue_time_title] = avg(entry['queue_time'], entry['num_tasks'])
            results[execution_time_title] = avg(entry['execute_time'], entry['num_tasks'])

        self.write(results)


class FailedTaskMonitor(BaseHandler):
    @web.authenticated
    def get(self):
        timestamp = self.get_argument('lastquery', type=float)
        state = self.application.events.state

        data = defaultdict(int)
        for _, task in state.itertasks():
            if (timestamp < task.timestamp and task.state == states.FAILURE):
                data[task.worker.hostname] += 1
        for worker in state.workers:
            if worker not in data:
                data[worker] = 0

        self.write(data)


class BrokerMonitor(BaseHandler):
    @web.authenticated
    @gen.coroutine
    def get(self):
        app = self.application
        capp = app.capp

        try:
            broker = Broker(capp.connection().as_uri(include_password=True),
                            http_api=app.options.broker_api)
        except NotImplementedError:
            self.write({})
            return

        queue_names = ControlHandler.get_active_queue_names()
        queues = yield broker.queues(queue_names)

        data = defaultdict(int)
        for queue in queues:
            data[queue['name']] = queue.get('messages', 0)

        self.write(data)
