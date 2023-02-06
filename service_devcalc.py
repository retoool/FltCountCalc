# coding=utf-8
import traceback,datetime,time
begintime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
begintimeint = int(time.time())
print('begin',begintime)
from apscheduler.schedulers.blocking import BlockingScheduler
from devCalc import DevCalc
mainHandler = DevCalc()
from getkairosdata import KairosData
kairosdata = KairosData()
import logging

log = logging.getLogger('apschedulepytr.executors.default')
log.setLevel(logging.INFO)  # DEBUG

fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
h = logging.StreamHandler()
h.setFormatter(fmt)
log.addHandler(h)
def main():
    try:
        scheduler = BlockingScheduler()

        scheduler.add_job(mainHandler.dev_status_calc_base, 'cron', max_instances=1, second='0/10')

        scheduler.add_job(mainHandler.output_his_data, 'cron', max_instances=1, minute='*/5')

        scheduler.add_job(mainHandler.resettime, 'cron', max_instances=1, second='0', minute='0', hour='0', day='*')

        scheduler.add_job(mainHandler.dev_calc_consistency, 'cron', max_instances=1, second='0', minute='30', hour='0', day='*')

        scheduler.add_job(mainHandler.dev_calc_hismonth, 'cron', max_instances=1, second='0', minute='0', hour='1', day='1', month="*")

        scheduler.start()
    except:
        print ('Execution of Real-Time Computing failed!')
        print (traceback.format_exc())
        
if __name__ == '__main__':
    main()