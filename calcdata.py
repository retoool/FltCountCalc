import time
# import numpy as np
import pandas as pd
from devCalc import DevCalc
calcmain = DevCalc()
from getkairosdata import KairosData
getdata = KairosData()
class Main:
    def __init__(self):
        self.devsCalcDict = calcmain.devsCalcDict
        day_time_str = "2023-1-1 00:00:00"
        today_time_str = "2023-2-1 00:00:00"
        self.today_time = int(time.mktime(time.strptime(today_time_str,"%Y-%m-%d %H:%M:%S")))
        self.day_time = int(time.mktime(time.strptime(day_time_str,"%Y-%m-%d %H:%M:%S")))
    def getresult(self):
        result = []
        ActPWR_Filter_AVG_10m = getdata.getdata('ActPWR_Filter_AVG_10m', 'sum', self.day_time, self.today_time, 'none', None, None, samplingValue='1', samplingUnit='years')
        Theory_PWR_Inter_Filter = getdata.getdata('Theory_PWR_Inter_Filter', 'sum', self.day_time, self.today_time, 'none', None, None, samplingValue='1', samplingUnit='years')
        WNAC_WdSpd_AVG_10m = getdata.getdata('WNAC_WdSpd_AVG_10m', 'avg', self.day_time, self.today_time, 'none', None, None, samplingValue='1', samplingUnit='years')
        for HashKey, attrs in self.devsCalcDict.items():
            if HashKey in ActPWR_Filter_AVG_10m and HashKey in Theory_PWR_Inter_Filter:
                a = ActPWR_Filter_AVG_10m[HashKey][0][1]
                b = Theory_PWR_Inter_Filter[HashKey][0][1]
                c = WNAC_WdSpd_AVG_10m[HashKey][0][1]
                result.append([HashKey,float(format(a/b, '.4f')),float(format(c, '.4f'))])
            else:
                result.append([HashKey,0,0])
        result.sort()
        # data = pd.DataFrame(result)
        # writer = pd.ExcelWriter('1month.xlsx')
        # data.to_excel(writer, 'page_1', float_format='%.5f')
        # writer.save()
        # writer.close()
        filename = "out.txt"
        with open(filename,'w') as file:
            for out in result:
                print(out)
                result = [str(num) for num in out]
                string = ','.join(result) + "\n"
                file.write(string)
        
if __name__ == "__main__":
    main = Main()
    main.getresult()
