#
# @Author: Bhaskar S
# @Blog:   https://www.polarsparc.com
# @Date:   04 Sep 2022
#

import logging
import time
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(format='%(levelname)s %(asctime)s - %(message)s', level=logging.INFO)

logger = logging.getLogger('sample-1')


def task():
    logger.info('Started sample-1 task...')
    time.sleep(1)
    logger.info('Completed sample-1 task !!!')


def main():
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(task, trigger='interval', seconds=60, misfire_grace_time=5*60)
    scheduler.start()
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        scheduler.shutdown()


if __name__ == '__main__':
    main()
