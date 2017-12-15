import os
import json
from tasks import add, minus, mult
from kombu import Queue
from celery import Celery, chain

CONFIG = json.load(open('./config.json'))
BROKER = CONFIG["BROKER"]
BACKEND = CONFIG["BACKEND"]
TASK_CREATE_MISSING_QUEUE = CONFIG["TASK_CREATE_MISSING_QUEUE"]

WORKERS = {
    "WORKER1": "worker1@localhost",
    "WORKER2": "worker2@localhost",
    "WORKER3": "worker3@localhost",
}

HASHES = {
    "HASH1": "feature_hash1",
    "HASH2": "feature_hash2",
    "HASH3": "feature_hash3"
}

class CeleryApp:

    @staticmethod
    def connect_workers():
        '''Setup celery object and connect to broker.

        Returns:
            Celery instance
        '''
        return Celery(
            'worker.tasks',
            broker=BROKER,
            backend=BACKEND,
            task_create_missing_queues=TASK_CREATE_MISSING_QUEUE,
            task_queues=()
        )
    
class MainApp:

    def __init__(self, celery_app):
        '''
        this class acccept a celery_app object as a decorate pattern,
        which will be mainly used to handle jobs

        @param {CeleryApp} celery_app - the CeleryApp object decorating this class
        '''
        self._celery_app = celery_app

    def do_job1(self, feature_hash):
        '''
        function that mocks doing a specific job, it will pick
        up a worker based on some policy, here just hardcoding
        the policy

        this job is doing (2 + 2) * 3 - 6 == 6, in the first queue via WORKER1

        @param {string} feature_hash - the feature hash that representing a series of tasks
        '''
        self._configure_routing(feature_hash, WORKERS["WORKER1"])

        job = chain(
            add.s(2, 2).set(queue=feature_hash, routing_key=feature_hash),
            mult.s(3).set(queue=feature_hash, routing_key=feature_hash),
            minus.s(6).set(queue=feature_hash, routing_key=feature_hash)
        )
        
        job.delay()

    def do_job2(self, feature_hash):
        '''
        function that mocks doing a specific job, it will pick
        up a worker based on some policy, here just hardcoding
        the policy

        this job is doing 2 * 5 * 3 - 10 + 18 == 38, in the second queue via WORKER2

        @param {string} feature_hash - the feature hash that representing a series of tasks
        '''
        self._configure_routing(feature_hash, WORKERS["WORKER2"])

        job = chain(
            mult.s(2, 5).set(queue=feature_hash, routing_key=feature_hash),
            mult.s(3).set(queue=feature_hash, routing_key=feature_hash),
            minus.s(10).set(queue=feature_hash, routing_key=feature_hash),
            add.s(18).set(queue=feature_hash, routing_key=feature_hash)
        )
        
        job.delay()

    def do_job3(self, feature_hash):
        '''
        function that mocks doing a specific job, it will pick
        up a worker based on some policy, here just hardcoding
        the policy

        this job is doing (2 * 4 - 10) * 7 == -14, in the third queue via WORKER3

        @param {string} feature_hash - the feature hash that representing a series of tasks
        '''
        self._configure_routing(feature_hash, WORKERS["WORKER3"])

        job = chain(
            mult.s(2, 4).set(queue=feature_hash, routing_key=feature_hash), # cpu 10 mins
        )
        
        job.delay()

    def do_job4(self, feature_hash):
        '''
        function that mocks doing a specific job, it will pick
        up a worker based on some policy, here just hardcoding
        the policy

        this job is doing (2 * 4 - 10) * 7 == -14, but in the first queue via WORKER2

        @param {string} feature_hash - the feature hash that representing a series of tasks
        '''
        self._configure_routing(feature_hash, WORKERS["WORKER2"])

        job = chain(
            mult.s(2, 4).set(queue=feature_hash, routing_key=feature_hash),
            minus.s(10).set(queue=feature_hash, routing_key=feature_hash),
            mult.s(7).set(queue=feature_hash, routing_key=feature_hash)
        )
        
        job.delay()


    def _configure_routing(self, feature_hash, worker):
        '''
        Configures routing at runtime, basically setting up new queues
        and assign a worker to that queue

        @param {string} feature_hash - the feature hash that representing a series of tasks
        @param {string} worker - name (host) of worker that will be consuming the queue created
        '''

        if self._celery_app.conf.task_queues is None:
            self._celery_app.conf.task_queues = (
                Queue(feature_hash, routing_key=feature_hash),
            )
        else:
            self._celery_app.conf.task_queues += (
                Queue(feature_hash, routing_key=feature_hash),
            )

        self._celery_app.control.add_consumer(
            feature_hash,
            reply=True,
            routing_key=feature_hash,
            destination=[worker]
        )

celery_app = CeleryApp.connect_workers()

main_app = MainApp(celery_app)

main_app.do_job1(HASHES["HASH1"])
main_app.do_job2(HASHES["HASH2"])
main_app.do_job3(HASHES["HASH3"])
main_app.do_job4(HASHES["HASH1"])