#
# @Author: Bhaskar S
# @Blog:   https://www.polarsparc.com
# @Date:   04 Sep 2022
#

import logging
import time
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(format='%(levelname)s %(asctime)s - %(message)s', level=logging.INFO)

logger = logging.getLogger('sample-2')

jobstores = {
    'default': SQLAlchemyJobStore(url='sqlite:////home/bswamina/MyProjects/Python/APScheduler/data/jobs.db')
}

def task():
    logger.info('Started sample-2 task...')
    time.sleep(2)
    logger.info('Completed sample-2 task !!!')


def main():
    job_id = 'sample-2'
    scheduler = BackgroundScheduler(jobstores=jobstores, daemon=True)
    scheduler.add_job(task, id=job_id, trigger='interval', seconds=60, misfire_grace_time=5*60)
    scheduler.start()
    try:
        while True:
            scheduler.print_jobs(jobstore="default")
            time.sleep(15)
    except KeyboardInterrupt:
        scheduler.remove_job(job_id)
        scheduler.shutdown()


if __name__ == '__main__':
    main()
