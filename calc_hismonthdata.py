import datetime,time
import pandas as pd
from devCalc import DevCalc
mainHandler = DevCalc()

def split_time_ranges(from_time, to_time, frequency):
        from_time, to_time = pd.to_datetime(from_time), pd.to_datetime(to_time)
        time_range = list(pd.date_range(from_time, to_time, freq='%sS' % frequency))
        if to_time not in time_range:
            time_range.append(to_time)
        time_range = [item.strftime("%Y-%m-%d %H:%M:%S") for item in time_range]
        time_ranges = []
        for item in time_range:
            f_time = item
            t_time = (datetime.datetime.strptime(item, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(seconds=frequency))
            if t_time >= to_time:
                t_time = to_time.strftime("%Y-%m-%d %H:%M:%S")
                time_ranges.append([f_time, t_time])
                break
            time_ranges.append([f_time, t_time.strftime("%Y-%m-%d %H:%M:%S")])
        print(time_range)
        return time_ranges

if __name__ == '__main__':
    day_time_str = "2023-1-1 00:00:00"
    today_time_str = "2023-2-20 00:00:00"
    
    day_time = time.strptime(day_time_str,"%Y-%m-%d %H:%M:%S")
    today_time = time.strptime(today_time_str,"%Y-%m-%d %H:%M:%S")
    frequency = 60 * 60 * 24
    time_ranges = split_time_ranges(day_time_str,today_time_str,frequency)

    for times in time_ranges:
        time1 = times[0]
        time2 = times[1]
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        print(time1 + ' to ' + time2)
        day_time = int(time.mktime(time.strptime(time1,"%Y-%m-%d %H:%M:%S")))
        today_time = int(time.mktime(time.strptime(time2,"%Y-%m-%d %H:%M:%S")))
        mainHandler.dev_calc_10min(time1,time2)
    
