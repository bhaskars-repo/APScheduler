#
# @Author: Bhaskar S
# @Blog:   https://www.polarsparc.com
# @Date:   04 Sep 2022
#

import logging
import time
import os
import zoneinfo
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

logging.basicConfig(format='%(levelname)s %(asctime)s - %(message)s', level=logging.INFO)

logger = logging.getLogger('sample-4')

jobstores = {
    'default': SQLAlchemyJobStore(url='sqlite:////home/bswamina/MyProjects/Python/APScheduler/data/jobs.db')
}

tz_NYC = zoneinfo.ZoneInfo('America/New_York')
scheduler = BackgroundScheduler(jobstores=jobstores, misfire_grace_time=5*60, daemon=True)


def task(jid, root, file):
    logger.info('Started [%s] task...' % jid)
    time.sleep(2)
    if not os.path.exists(os.path.join(root, file)):
        raise FileNotFoundError
    logger.info('Completed [%s] task !!!' % jid)


def job_status_listener(event):
    if event.exception:
        logger.error('*** The job [%s] encountered exception !!!' % event.job_id)
        # Failure - reschedule for sooner
        scheduler.reschedule_job(event.job_id, trigger='interval', seconds=15)
    else:
        logger.info('The job [%s] succeed.' % event.job_id)
        # Success - Back to default
        scheduler.reschedule_job(event.job_id, trigger='cron', day_of_week='mon-fri', minute='*/1', timezone=tz_NYC)


def main():
    job_id = 'sample-4'
    root = '/tmp'
    file = 'dummy.dat'
    scheduler.add_job(task, id=job_id, args=[job_id, root, file], trigger='cron', day_of_week='mon-fri',
                      minute='*/1', timezone=tz_NYC)
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
