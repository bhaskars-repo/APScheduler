#
# @Author: Bhaskar S
# @Blog:   https://www.polarsparc.com
# @Date:   04 Sep 2022
#

import logging
import time
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

logging.basicConfig(format='%(levelname)s %(asctime)s - %(message)s', level=logging.INFO)

logger = logging.getLogger('sample-3')

jobstores = {
    'default': SQLAlchemyJobStore(url='sqlite:////home/bswamina/MyProjects/Python/APScheduler/data/jobs.db')
}


def task():
    logger.info('Started sample-3 task...')
    time.sleep(3)
    logger.info('Completed sample-3 task !!!')


def job_status_listener(event):
    if event.exception:
        logger.error('The job [%s] encountered exception ...' % event.job_id)
    else:
        logger.info('The job [%s] succeed !!!' % event.job_id)


def main():
    job_id = 'sample-3'
    scheduler = BackgroundScheduler(jobstores=jobstores, daemon=True)
    scheduler.add_job(task, id=job_id, trigger='interval', seconds=60, misfire_grace_time=5 * 60)
    scheduler.add_listener(job_status_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    scheduler.start()
    try:
        while True:
            scheduler.print_jobs(jobstore="default")
            time.sleep(30)
    except KeyboardInterrupt:
        scheduler.remove_job(job_id)
        scheduler.shutdown()


if __name__ == '__main__':
    main()
