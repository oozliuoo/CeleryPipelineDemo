from celery import Celery

app = Celery('worker.tasks', backend='redis://localhost:6379/1', broker='redis://localhost:6379/0')

@app.task
def add(x, y):
    return x + y

@app.task
def mult(x, y):
    return x * y

@app.task
def minus(x, y):
    return x - y