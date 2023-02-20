# coding=utf-8
import sys,logging,traceback,time,copy,zlib,datetime,json,os,gzip,math
import numpy as np
curPath = os.path.realpath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import Queue
import mylib.xy_publicmodule as pubmod
import mylib.xy_redisdb as red
import mylib.xy_KairosDBnew as kai
import mylib.xy_mysql as mysql
import getkairosdata as kairosdata
import pandas as pd
from dateutil.relativedelta import relativedelta
from windtypeParaUpdate import WndTypParaUpdate
getdata = kairosdata.KairosData()
from getSQL2XML import GetMySQL2XML
getMySQL2XML = GetMySQL2XML()
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
# 七种状态
devStatus = pubmod.enum('stop', 'run', 'ready', 'service', 'fault', 'offline', 'limitPower')
#九种状态、新增技术待机7、电网故障8
devStatus9 = pubmod.enum('stop', 'run', 'ready', 'service', 'fault', 'offline', 'limitPower', 'readyTech', 'faultGrid') 
ValueMap={}
StMap={}
diffTimer = 60 * 30
# debug模式
debugmode1 = 0
debugmode2 = 1
#log
logger = logging.getLogger('logger')
logger.setLevel(logging.DEBUG)
forma = logging.Formatter(fmt='%(asctime)s | [%(levelname)s] | [%(message)s]', datefmt='%Y/%m/%d/%X')
fh = logging.FileHandler('./my.log', encoding='UTF-8')
fh.setFormatter(forma)
logger.addHandler(fh)
channel_main = 'main@scada_calcData'
#kairosdb查询json文件
dstDir = os.path.join(rootPath, 'stateCalcDataCache')
if not os.path.exists(dstDir):
    os.mkdir(dstDir)

class DevCalc:
    # 初始化
    def __init__(self):
        #读取xy_devStCalc.xml文件
        try:
            self.xmlTree = ET.parse('xy_devStCalc.xml')
        except Exception:
            print ('Error:cannot parse file:xy_devStCalc.xml.')
            sys.exit(1)#异常终止程序
        wndTypeParasTemp = WndTypParaUpdate()
        self.wndTypeParas = self.gpcParasInit(wndTypeParasTemp.jsonContect)
        
        self.devCalcRuleXML = {}
        self.devsCalcDict = {}
        self.lineMetricCount = {}
        self.termMetricCount = {}
        self.farmMetricCount = {}
        self.rootMetricCount = {}
        self.rootAllMetricCount = {}
        self.sqlHandler = None
        #redis
        self.redis = red.RedisDB()
        self.redis.connectionForLocal()
        self.pip = self.redis.conn.pipeline(False)
        #kairosdb
        self.kairos = kai.KairosDB()
        #mysql
        self.my = mysql.MySQL()
        self.NumMap = {}
        self.StMap = {}
        self.tags = []
        self.LongStopStMap = {}
        self.CountMapInit = {}
        self.CountMap = {}
        self.ValueMap = {}
        self.ValuesMap = {}
        self.TimeMap = {}
        self.SumList = []
        self.SumList_10m = []
        self.AvgList = []
        self.AvgList_10m = []
        self.MaxList_10m = []
        self.SumMapInit = {}
        self.SumMapInit_10m = {}
        self.SumMap = {}
        self.SumMap_10m = {}
        self.MsgList = []
        self.hisdata_queue = Queue.Queue()
        self.first_into_limitDelay_dict = dict()#字典
        self.get_dev_is_calc()
        self.nowtime = int(time.time())
        print('init end',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        return
    
    # 风机功率曲线区间计算参数
    @staticmethod
    def gpcParasInit(gpcDict):
        gpcdicttemp = {}
        for devtype, para in gpcDict.items():
            dicttemp = {}
            try:
                power_curve = []
                if 'powerCurve' in para:
                    list_windspeed = [float(x) for x in para["powerCurve"].keys()]
                    list_power = para["powerCurve"].values()
                    if len(list_windspeed) == len(list_power):
                        for i in range(0, len(list_windspeed)):
                            power_curve.append((list_windspeed[i], list_power[i]))
                capacity = 0
                if 'capacity' in para:
                    capacity = float(para["capacity"])
                    dicttemp['capacity'] = capacity
                windspeed_cutin = 0
                if 'windspeed_cutin' in para:
                    windspeed_cutin = float(para["windspeed_cutin"])
                    dicttemp['windspeed_cutin'] = windspeed_cutin
                windspeed_cutout = 0
                if 'windspeed_cutout' in para:
                    windspeed_cutout = float(para["windspeed_cutout"])
                    dicttemp['windspeed_cutout'] = windspeed_cutout
                if 'windspeed_life' in para:
                    windspeed_life = float(para["windspeed_life"])
                    dicttemp['windspeed_life'] = windspeed_life
                if len(power_curve) > 0:
                    if power_curve[0][0] > windspeed_cutin:
                        power_curve.insert(0, (windspeed_cutin, 0))
                    if power_curve[-1][0] < windspeed_cutout:
                        power_curve.append((windspeed_cutout, capacity))
                    dicttemp['power_curve'] = power_curve
                else:
                    power_curve.insert(0, (windspeed_cutin, 0))
                    power_curve.append((windspeed_cutout, capacity))
                    dicttemp['power_curve'] = power_curve
            except Exception as e:
                print("风机功率预测参数json文件数据缺失或错误！")
            gpcdicttemp[devtype] = dicttemp
        return gpcdicttemp
   
    # 在初始化函数内调用，获取所有设备及需要计算的指标
    def get_dev_is_calc(self):
        try:
            tag_list = list()
            tag_list.append('NewCalcRT_StndSt')
            tag_list.append('NewCalcRT_PrevStndSt')
            tag_list.append('CalcRT_FltFilterCnt')
            tag_list.append('CalcRT_FltFilterCntD')
            tag_list.append('CalcRT_FltFilterCntM')
            tag_list.append('CalcRT_FltFilterCntY')
            tag_list.append('CalcRT_FltChngTmbegin')
            tag_list.append('CalcRT_FltChngTmend')
            # 风速
            tag_list.append('WNAC_WdSpd')
            # 环境温度
            tag_list.append('WNAC_IntlTmp')
            self.SumList.append('CalcRT_FltFilterCnt')
            self.SumList.append('CalcRT_FltFilterCntD')
            self.SumList.append('CalcRT_FltFilterCntM')
            self.SumList.append('CalcRT_FltFilterCntY')
            self.SumList.append('CalcRT_FltFilterTime')
            self.SumList.append('CalcRT_FltFilterTimeD')
            self.SumList.append('CalcRT_FltFilterTimeM')
            self.SumList.append('CalcRT_FltFilterTimeY')
            
            self.SumList_10m.append('ActPWR_Filter_AVG_10m')
            self.SumList_10m.append('Theory_PWR_Inter')
            self.SumList_10m.append('Theory_PWR_Inter_his')
            self.SumList_10m.append('Theory_PWR_Inter_Filter')
            self.SumList_10m.append('Theory_PWR_Inter_Filter_his')

            self.AvgList_10m.append('CalcRT_density_AVG_10m')
            self.AvgList_10m.append('WNAC_WdSpd_FilterAVG_10m')
            self.AvgList_10m.append('WNAC_WdSpd_AVG_10m')

            self.MaxList_10m.append('WNAC_WdSpd_MAX_10m')

            project = self.xmlTree.find('./project')
            project_id = project.attrib['ID']
            self.CountMapInit[(project_id, 'all', devStatus.stop)] = 0
            self.CountMapInit[(project_id, 'all', devStatus.run)] = 0
            self.CountMapInit[(project_id, 'all', devStatus.ready)] = 0
            self.CountMapInit[(project_id, 'all', devStatus.service)] = 0
            self.CountMapInit[(project_id, 'all', devStatus.fault)] = 0
            self.CountMapInit[(project_id, 'all', devStatus.offline)] = 0
            self.CountMapInit[(project_id, 'all', devStatus.limitPower)] = 0
            # NumMap字典
            self.NumMap[project_id] = 0
            #project公司级、farm场站级
            listFarmStation = self.xmlTree.findall('./project/farmStation[@enrgType="%s"]' % 'FD')
            for farmStation in listFarmStation:
                farm_station_id = farmStation.attrib['ID']
                energy_type = farmStation.attrib['enrgType']
                farm_code = project_id + ':' + farm_station_id
                self.CountMapInit[(project_id, energy_type, devStatus.stop)] = 0
                self.CountMapInit[(project_id, energy_type, devStatus.run)] = 0
                self.CountMapInit[(project_id, energy_type, devStatus.ready)] = 0
                self.CountMapInit[(project_id, energy_type, devStatus.service)] = 0
                self.CountMapInit[(project_id, energy_type, devStatus.fault)] = 0
                self.CountMapInit[(project_id, energy_type, devStatus.offline)] = 0
                self.CountMapInit[(project_id, energy_type, devStatus.limitPower)] = 0
                self.CountMapInit[(farm_code, energy_type, devStatus.stop)] = 0
                self.CountMapInit[(farm_code, energy_type, devStatus.run)] = 0
                self.CountMapInit[(farm_code, energy_type, devStatus.ready)] = 0
                self.CountMapInit[(farm_code, energy_type, devStatus.service)] = 0
                self.CountMapInit[(farm_code, energy_type, devStatus.fault)] = 0
                self.CountMapInit[(farm_code, energy_type, devStatus.offline)] = 0
                self.CountMapInit[(farm_code, energy_type, devStatus.limitPower)] = 0
                self.NumMap[farm_code] = 0
                for para in self.SumList + self.AvgList:
                    self.SumMapInit[(farm_code, para)] = 0
                for para in self.SumList_10m + self.AvgList_10m + self.MaxList_10m:
                    self.SumMapInit_10m[(farm_code, para)] = [0,0]
                # term期数级
                listDevTerm = self.xmlTree.findall('.//farmStation[@ID="%s"]/term[@Calc="True"]' % farmStation.attrib['ID'])
                for devTerm in listDevTerm:
                    dev_term_id = devTerm.attrib['ID']
                    term_code = farm_code + ':' + dev_term_id
                    self.CountMapInit[(term_code, energy_type, devStatus.stop)] = 0
                    self.CountMapInit[(term_code, energy_type, devStatus.run)] = 0
                    self.CountMapInit[(term_code, energy_type, devStatus.ready)] = 0
                    self.CountMapInit[(term_code, energy_type, devStatus.service)] = 0
                    self.CountMapInit[(term_code, energy_type, devStatus.fault)] = 0
                    self.CountMapInit[(term_code, energy_type, devStatus.offline)] = 0
                    self.CountMapInit[(term_code, energy_type, devStatus.limitPower)] = 0
                    self.NumMap[term_code] = 0
                    for para in self.SumList + self.AvgList:
                        self.SumMapInit[(term_code, para)] = 0 
                    for para in self.SumList_10m + self.AvgList_10m + self.MaxList_10m:
                        self.SumMapInit_10m[(term_code, para)] = [0,0]
                    # line设备级
                    listLine = self.xmlTree.findall('.//farmStation[@ID="%s"]/term[@ID="%s"][@Calc="True"]/line' % (
                        farmStation.attrib['ID'], devTerm.attrib['ID']))
                    for line in listLine:
                        line_id = str(line.attrib['ID'])
                        line_code = term_code + ':' + line_id
                        self.CountMapInit[(line_code, energy_type, devStatus.stop)] = 0
                        self.CountMapInit[(line_code, energy_type, devStatus.run)] = 0
                        self.CountMapInit[(line_code, energy_type, devStatus.ready)] = 0
                        self.CountMapInit[(line_code, energy_type, devStatus.service)] = 0
                        self.CountMapInit[(line_code, energy_type, devStatus.fault)] = 0
                        self.CountMapInit[(line_code, energy_type, devStatus.offline)] = 0
                        self.CountMapInit[(line_code, energy_type, devStatus.limitPower)] = 0
                        self.NumMap[line_code] = 0
                        listDev = self.xmlTree.findall('.//farmStation[@ID="%s"]/term[@ID="%s"][@Calc="True"]/line[@ID="%s"]/dev' % (
                            farm_station_id, dev_term_id, line_id))
                        for dev in listDev:
                            self.NumMap[project_id] += 1
                            self.NumMap[farm_code] += 1
                            self.NumMap[term_code] += 1
                            self.NumMap[line_code] += 1
                            # 风机号
                            devNO = dev.attrib['devNO']
                            # 风机型号
                            devType = dev.attrib['devType']
                            altitude = dev.attrib['altitude']
                            hubHeight = dev.attrib['hubHeight']
                            HashKey = term_code + ':' + devNO
                            self.tags.append(HashKey)
                            self.first_into_limitDelay_dict[HashKey] = 0
                            if HashKey not in self.ValueMap:
                                self.ValueMap[HashKey] = dict()
                                self.TimeMap[HashKey] = dict()
                                self.ValueMap[HashKey]['CONN'] = 0
                            self.devsCalcDict[HashKey] = {
                                'devType': devType,# 设备型号
                                'line': line_id, # 设备号
                                'enrgType': energy_type, # 能源类型
                                'altitude': altitude, # 海拔高度
                                'hubHeight': hubHeight} # 轮毂高度
                            self.StMap[HashKey] = [-1, -1]
                            self.LongStopStMap[HashKey] = [-1, -1]
                            # 从redis中读取tag_list的值
                            ValueInit = self.redis.getDatasByTagList(HashKey, tag_list)
                            #tag点名、value数值
                            for tag, value in ValueInit.items():
                                if value:
                                    values = str(value).split(':')
                                    self.ValueMap[HashKey][tag] = float(values[1])
                                    self.TimeMap[HashKey][tag] = int(values[2])
                                    #CalcRT_StndSt当前状态
                                    if tag == 'NewCalcRT_StndSt':
                                        self.StMap[HashKey][1] = int(values[1])
                                    elif tag == 'NewCalcRT_PrevStndSt':
                                        self.StMap[HashKey][0] = int(values[1])
        except Exception as e:
            if debugmode2:
                traceback.print_exc()
            print ('get_dev_is_calc(): %s' % repr(e))
    
    # 设备计算基础
    def dev_status_calc_base(self):
        self.nowtime = int(time.time())
        self.CountMap = copy.deepcopy(self.CountMapInit)
        self.SumMap = copy.deepcopy(self.SumMapInit)
        for HashKey, _ in self.devsCalcDict.items():
            tag_list = list()
            tag_list.append('NewCalcRT_StndSt')
            tag_list.append('NewCalcRT_PrevStndSt')
            tag_list.append('WNAC_IntlTmp')
            tag_list.append('WNAC_WdSpd')
            ValueInit = self.redis.getDatasByTagList(HashKey, tag_list)
            #tag点名、value数值
            for tag, value in ValueInit.items():
                if value:
                    values = str(value).split(':')
                    self.ValueMap[HashKey][tag] = float(values[1])
                    self.TimeMap[HashKey][tag] = int(values[2])
                    #CalcRT_StndSt当前状态
                    if tag == 'NewCalcRT_StndSt':
                        self.StMap[HashKey][1] = int(values[1])
                    elif tag == 'NewCalcRT_PrevStndSt':
                        self.StMap[HashKey][0] = int(values[1])
            publish_ch_list = str(HashKey).split(':')
            project = publish_ch_list[0]
            farm = publish_ch_list[1]
            term = publish_ch_list[2]
            term_full = (':').join([project, farm, term])
            farm_full = (':').join([project, farm])
            #检查状态
            self._check_st(HashKey)
            #计算总和
            self.calcsum(HashKey, term_full, farm_full)
        
        for (rediskey, redistag), sumvalue in self.SumMap.items():
            if redistag in self.SumList:
                self._set_value(rediskey, redistag, sumvalue)
        self.pip.execute()
        if self.MsgList:
            if self.redis.conn.publish(channel_main, zlib.compress((',').join(self.MsgList))) <= 0:
                self.hisdata_queue.put(self.MsgList)
            self.MsgList = []
    
    # 功率一致性系数计算
    def dev_calc_consistency(self):
        today_time = int(time.mktime((datetime.date.today() + datetime.timedelta(days=0)).timetuple()))
        day_time=int(time.mktime((datetime.date.today() + datetime.timedelta(days=-1)).timetuple()))
        today_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(today_time))
        day_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(day_time))
        
        # day_time_str = "2023-2-13 00:00:00"
        # today_time_str = "2023-2-14 00:00:00"
        
        print(day_time_str + " to " + today_time_str)
        self.dev_calc_10min(day_time_str,today_time_str)
    
    # 10分钟数据计算
    def dev_calc_10min(self,day_time_str,today_time_str):
        self.SumMap_10m = copy.deepcopy(self.SumMapInit_10m)
        today_time = int(time.mktime(time.strptime(today_time_str,"%Y-%m-%d %H:%M:%S")))
        day_time = int(time.mktime(time.strptime(day_time_str,"%Y-%m-%d %H:%M:%S")))
        WNAC_WdSpd_filter = [["gte","50"],["lte","0"]]
        WNAC_ExTmp_filter = [["gte","60"],["lte","-60"]]
        WNAC_WdSpd_AVG_10m = getdata.getdata('WNAC_WdSpd',self.tags, 'avg', day_time, today_time, 'end', None, None, samplingValue='10', samplingUnit='minutes',filter=WNAC_WdSpd_filter)
        WNAC_WdSpd_MAX_10m = getdata.getdata('WNAC_WdSpd',self.tags, 'max', day_time, today_time, 'end', None, None, samplingValue='10', samplingUnit='minutes',filter=WNAC_WdSpd_filter)
        WNAC_ExTmp_AVG_10m = getdata.getdata('WNAC_ExTmp',self.tags, 'avg', day_time, today_time, 'end', None, None, samplingValue='10', samplingUnit='minutes',filter=WNAC_ExTmp_filter)
        ActPWR_AVG_10m = getdata.getdata('ActPWR',self.tags, 'avg', day_time, today_time, 'end', None, None, samplingValue='10', samplingUnit='minutes')
        NewCalcRT_StndSt_AVG_10m = getdata.getdata('NewCalcRT_StndSt',self.tags, 'avg', day_time, today_time, 'end', None, None, samplingValue='10', samplingUnit='minutes')
        WNAC_WdSpd_DEV_10m = getdata.getdata('WNAC_WdSpd',self.tags, 'dev', day_time, today_time, 'end', None, None, samplingValue='10', samplingUnit='minutes')
        data = WNAC_ExTmp_AVG_10m
        for HashKey in data:
            for i in range(len(data[HashKey])):
                value = float(format(data[HashKey][i][1], '.6f'))
                timeint = int(data[HashKey][i][0]/1000)
                self._set_value(HashKey,'WNAC_ExTmp_AVG_10m',value,time=timeint)
        data = ActPWR_AVG_10m
        for HashKey in data:
            for i in range(len(data[HashKey])):
                value = float(format(data[HashKey][i][1], '.6f'))
                timeint = int(data[HashKey][i][0]/1000)
                self._set_value(HashKey,'ActPWR_AVG_10m',value,time=timeint)
        data = NewCalcRT_StndSt_AVG_10m
        for HashKey in data:
            for i in range(len(data[HashKey])):
                value = float(format(data[HashKey][i][1], '.6f'))
                timeint = int(data[HashKey][i][0]/1000)
                self._set_value(HashKey,'NewCalcRT_StndSt_AVG_10m',value,time=timeint)
        data = WNAC_WdSpd_DEV_10m
        for HashKey in data:
            for i in range(len(data[HashKey])):
                value = float(format(data[HashKey][i][1], '.6f'))
                timeint = int(data[HashKey][i][0]/1000)
                self._set_value(HashKey,'WNAC_WdSpd_DEV_10m',value,time=timeint)
        data = WNAC_WdSpd_MAX_10m
        for HashKey in data:
            for i in range(len(data[HashKey])):
                value = float(format(data[HashKey][i][1], '.6f'))
                timeint = int(data[HashKey][i][0]/1000)
                NewCalcRT_StndSt_AVG_10mi = self.getvalue(HashKey,'NewCalcRT_StndSt_AVG_10m',timeint) # 状态
                if NewCalcRT_StndSt_AVG_10mi and NewCalcRT_StndSt_AVG_10mi != 5:
                    self._set_value(HashKey,'WNAC_WdSpd_MAX_10m',value,time=timeint)
        data = WNAC_WdSpd_AVG_10m
        for HashKey in data:
            for i in range(len(data[HashKey])):
                value = float(format(data[HashKey][i][1], '.6f'))
                timeint = int(data[HashKey][i][0]/1000)
                WNAC_ExTmpi = self.getvalue(HashKey,'WNAC_ExTmp_AVG_10m',timeint) # 温度
                WNAC_WdSpd_DEV_10mi = self.getvalue(HashKey,'WNAC_WdSpd_DEV_10m',timeint) # 标准差
                NewCalcRT_StndSt_AVG_10mi = self.getvalue(HashKey,'NewCalcRT_StndSt_AVG_10m',timeint) # 状态
                if value and WNAC_ExTmpi and WNAC_WdSpd_DEV_10mi and WNAC_WdSpd_DEV_10mi >= 0.001 or WNAC_ExTmpi >= 6:
                    self._set_value(HashKey,'WNAC_WdSpd_AVG_10m',value,time=timeint)
                    if NewCalcRT_StndSt_AVG_10mi and NewCalcRT_StndSt_AVG_10mi != 5:
                        self._set_value(HashKey,'WNAC_WdSpd_FilterAVG_10m',value,time=timeint)
            else:
                for i in range(len(data[HashKey])):
                    value = float(format(data[HashKey][i][1], '.6f'))
                    timeint = int(data[HashKey][i][0]/1000)
                    self._set_value(HashKey,'WNAC_WdSpd_AVG_10m',value,time=timeint)
                    if NewCalcRT_StndSt_AVG_10m != 5:
                        self._set_value(HashKey,'WNAC_WdSpd_FilterAVG_10m',value,time=timeint)
        
        print('getvalue end',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        self.pwrcalc(day_time_str, today_time_str)
        print('pwrcalc end',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        self.calc_lost_power(day_time_str, today_time_str)
        print('calc_lost_power end',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        self.pip.execute()
        if self.MsgList:
            if self.redis.conn.publish(channel_main, zlib.compress((',').join(self.MsgList))) <= 0:
                self.hisdata_queue.put(self.MsgList)
            self.MsgList = []
        self.output_his_data()

    # 功率计算
    def pwrcalc(self,day_time_str, today_time_str):
        frequency = 60 * 10
        from_time, to_time = pd.to_datetime(day_time_str), pd.to_datetime(today_time_str)
        time_range = list(pd.date_range(from_time, to_time, freq='%sS' % frequency))
        if to_time not in time_range:
            time_range.append(to_time)
        time_range = [item.strftime("%Y-%m-%d %H:%M:%S") for item in time_range]
        power_curve_dict = self.get_power_curve_his()                 # 理论功率曲线(历史)
        for timestr in time_range[1:len(time_range)]:
            timestamp = int(time.mktime(time.strptime(timestr, "%Y-%m-%d %H:%M:%S")))
            for HashKey, attrs in self.devsCalcDict.items():
                try:
                    WNAC_WdSpd_AVG_10m = self.getvalue(HashKey,'WNAC_WdSpd_AVG_10m',timestamp)    # 10分钟风速平均值
                    NewCalcRT_StndSt_AVG_10m = self.getvalue(HashKey,'NewCalcRT_StndSt_AVG_10m',timestamp)    # 10分钟状态平均值
                    WNAC_ExTmp_AVG_10m = self.getvalue(HashKey,'WNAC_ExTmp_AVG_10m',timestamp)    # 10分钟舱外温度平均值
                    ActPWR_AVG_10m = self.getvalue(HashKey,'ActPWR_AVG_10m',timestamp)            # 10分钟有功功率平均值
                    if WNAC_WdSpd_AVG_10m and NewCalcRT_StndSt_AVG_10m and WNAC_ExTmp_AVG_10m and ActPWR_AVG_10m:
                        publish_ch_list = str(HashKey).split(':')
                        project = publish_ch_list[0]
                        farm = publish_ch_list[1]
                        term = publish_ch_list[2]
                        term_full = (':').join([project, farm, term])
                        farm_full = (':').join([project, farm])
                        devtype = attrs['devType']      # 设备型号
                        altitude = float(attrs['altitude'])    # 海拔高度
                        hubHeight = float(attrs['hubHeight'])  # 轮毂高度
                        windspeed_cutin = self.wndTypeParas[devtype]['windspeed_cutin']     # 切入风速
                        windspeed_cutout = self.wndTypeParas[devtype]['windspeed_cutout']   # 切出风速
                        capacity = self.wndTypeParas[devtype]['capacity']                   # 额定功率
                        power_curve = self.wndTypeParas[devtype]['power_curve']             # 理论功率曲线(合同)
                        power_curve.sort()
                        P_10m = 101325 * math.exp(-(altitude + hubHeight) * 9.8/(287.05 * (273.15 + WNAC_ExTmp_AVG_10m)))   # 10分钟大气压强
                        Pw = 0.0000205 * math.exp(0.0613846 * (273.15 + WNAC_ExTmp_AVG_10m))                                # 10分钟特定温度大气压
                        density_10m = (P_10m/287.05 - 0.5 * Pw * (1/287.05 - 1/461.5))/(273.15 + WNAC_ExTmp_AVG_10m)        # 10分钟空气密度
                        windspd_stnd = (WNAC_WdSpd_AVG_10m**3 * density_10m/1.225)**(0.33333333)                            # 标准空气密度风速
                        Interval_array = [0+x*(0.5) for x in range(100)]
                        Interval_array = np.asarray(Interval_array)
                        idx = (np.abs(Interval_array - windspd_stnd)).argmin()
                        WNAC_WdSpd_Interval_10m = float(format(Interval_array[idx], '.1f'))   # 10分钟风速区间对应值
                        self._set_value(HashKey,'WNAC_WdSpd_Interval_10m',WNAC_WdSpd_Interval_10m,timestamp)
                        density_10m_value = float(format(density_10m, '.6f'))
                        windspd_stnd_value = float(format(windspd_stnd, '.6f'))
                        self._set_value(HashKey,'CalcRT_density_AVG_10m',density_10m_value,timestamp)
                        self._set_value(HashKey,'CalcRT_WdSpdStnd_AVG_10m',windspd_stnd_value,timestamp)
                        minWindSpd = windspeed_cutin - 1  # 区间最小风速
                        maxWindSpd = 50                   # 区间最大风速
                        entertag = 0
                        Theory_PWR_Inter = 0
                        Theory_PWR_Inter_his = 0
                        for i in range(0,len(power_curve)):
                            if power_curve[i][0] > 0:
                                windspd = power_curve[i][0]
                                power = power_curve[i][1]
                                if i == 0:
                                    prewindspd = 0
                                    prepower = 0
                                else:
                                    prewindspd = power_curve[i - 1][0]
                                    prepower = power_curve[i - 1][1]
                                # 合同功率曲线区间值
                                if WNAC_WdSpd_Interval_10m == windspd:
                                    Theory_PWR_Interval = float(power)
                                    self._set_value(HashKey,'Theory_PWR_Interval',Theory_PWR_Interval,timestamp)
                                # 计算额定功率85%对应风速的1.5倍
                                if power >= capacity * 0.85 and entertag == 0:
                                    entertag = 1
                                    if i == 0:
                                        if windspd == 0:
                                            maxWindSpd = 0
                                        else:
                                            maxWindSpd = ((windspd - 0) * (capacity * 0.85 - 0))/(windspd - 0) + prewindspd
                                    else:
                                        maxWindSpd =  ((windspd - prewindspd) * (capacity * 0.85 - prepower))/(power - prepower) + prewindspd
                                # 线性插值法计算理论功率
                                if prewindspd <= windspd_stnd <= windspd:
                                    theory_pwr = ((power - prepower) * (windspd_stnd - prewindspd))/(windspd - prewindspd) + prepower
                                    Theory_PWR_Inter = float(format(theory_pwr, '.6f'))
                                    self._set_value(HashKey,'Theory_PWR_Inter',Theory_PWR_Inter,timestamp)
                        if HashKey in power_curve_dict:
                            power_curve_his = power_curve_dict[HashKey]   # 理论功率曲线(历史)
                            power_curve_his.sort()
                            for i in range(0,len(power_curve_his)):
                                if power_curve_his[i][0] > 0:
                                    windspd = power_curve_his[i][0]
                                    power = power_curve_his[i][1]
                                    if i == 0:
                                        prewindspd = 0
                                        prepower = 0
                                    else:
                                        prewindspd = power_curve_his[i - 1][0]
                                        prepower = power_curve_his[i - 1][1]
                                    # 合同功率曲线区间值
                                    if WNAC_WdSpd_Interval_10m == windspd:
                                        Theory_PWR_Interval_his = float(power)
                                        self._set_value(HashKey,'Theory_PWR_Interval_his',Theory_PWR_Interval_his,timestamp)
                                    # 线性插值法计算理论功率
                                    if prewindspd <= windspd_stnd <= windspd:
                                        theory_pwr_his = ((power - prepower) * (windspd_stnd - prewindspd))/(windspd - prewindspd) + prepower
                                        Theory_PWR_Inter_his = float(format(theory_pwr_his, '.6f'))
                                        self._set_value(HashKey,'Theory_PWR_Inter_his',Theory_PWR_Inter_his,timestamp)
                        ActPWR_Filter_Tag = 0   # 有功功率过滤标签
                        if ActPWR_AVG_10m and NewCalcRT_StndSt_AVG_10m == 1 and 0 < ActPWR_AVG_10m < 2 * capacity:
                            ActPWR_Filter_Tag = 0
                        else:
                            ActPWR_Filter_Tag = 1
                        self._set_value(HashKey,'ActPWR_Filter_Tag', ActPWR_Filter_Tag, timestamp)
                        if ActPWR_Filter_Tag == 0 and minWindSpd <= windspd_stnd <= maxWindSpd:
                            self._set_value(HashKey,'ActPWR_Filter_AVG_10m', ActPWR_AVG_10m, timestamp)
                            self._set_value(HashKey,'Theory_PWR_Inter_Filter', Theory_PWR_Inter, timestamp)
                            self._set_value(HashKey,'Theory_PWR_Inter_Filter_his', Theory_PWR_Inter_his, timestamp)
                        if ActPWR_Filter_Tag == 0 and 0 <= windspd_stnd <= windspeed_cutout:
                            self._set_value(HashKey,'ActPWR_Fitting_AVG_10m', ActPWR_AVG_10m, timestamp)
                            self._set_value(HashKey,'Theory_PWR_Inter_Fitting', Theory_PWR_Inter, timestamp)
                            self._set_value(HashKey,'Theory_PWR_Inter_Fitting_his', Theory_PWR_Inter_his, timestamp)
                        for point in self.SumList_10m:
                            pointvalue = self.getvalue(HashKey,point,timestamp)
                            if pointvalue is not None:
                                self.SumMap_10m[(term_full, point)][0] += pointvalue
                                self.SumMap_10m[(farm_full, point)][0] += pointvalue
                        for point in self.AvgList_10m:
                            pointvalue = self.getvalue(HashKey,point,timestamp)
                            if pointvalue is not None:
                                self.SumMap_10m[(term_full, point)][0] += pointvalue
                                self.SumMap_10m[(farm_full, point)][0] += pointvalue        
                                self.SumMap_10m[(term_full, point)][1] += 1
                                self.SumMap_10m[(farm_full, point)][1] += 1
                        for point in self.MaxList_10m:
                            pointvalue = self.getvalue(HashKey,point,timestamp)
                            if pointvalue is not None:
                                prevalue = self.SumMap_10m[(term_full, point)][0]
                                self.SumMap_10m[(farm_full, point)][0] = max(pointvalue,prevalue)        
                        
                except Exception as e:
                    print(e)
            for (rediskey, redistag), sumvalue in self.SumMap_10m.items():
                sumnum = sumvalue[1] if sumvalue[1] != 0 else 1
                self._set_value(rediskey, redistag, sumvalue[0]/sumnum,timestamp)
            self.SumMap_10m = copy.deepcopy(self.SumMapInit_10m)

    # 历史功率曲线计算（历史三个月实际有功功率拟合）
    def dev_calc_hismonth(self):
        self.my.connectionForLocal()
        now = datetime.datetime.now()
        thismonth = datetime.datetime(now.year,now.month,1)
        begin_time = thismonth + relativedelta(months=-3)
        end_time = thismonth
        begin_time = int(time.mktime(begin_time.timetuple()))
        end_time = int(time.mktime(end_time.timetuple()))
        pwr_all = getdata.getdata('ActPWR_Fitting_AVG_10m', 'sum', begin_time, end_time, None, None, None, samplingValue='1', samplingUnit='milliseconds')
        windspd_all = getdata.getdata('WNAC_WdSpd_Interval_10m', 'sum', begin_time, end_time, None, None, None, samplingValue='1', samplingUnit='milliseconds')
        pwrdict = {}
        spddict = {}
        sumdict = {}
        for Hashkey in windspd_all:
            if Hashkey not in spddict:
                spddict[Hashkey] = {}
            for i in range(len(windspd_all[Hashkey])):
                spdvalue = windspd_all[Hashkey][i][1]
                spdtime = int(windspd_all[Hashkey][i][0]/1000)
                spddict[Hashkey][spdtime] = spdvalue
        for Hashkey in pwr_all:
            if Hashkey not in pwrdict:
                pwrdict[Hashkey] = {}
            for i in range(len(pwr_all[Hashkey])):
                pwrvalue = pwr_all[Hashkey][i][1]
                pwrtime = int(pwr_all[Hashkey][i][0]/1000)
                pwrdict[Hashkey][pwrtime] = pwrvalue 
                if Hashkey in spddict and pwrtime in spddict[Hashkey]:
                    windspd = spddict[Hashkey][pwrtime]
                    if Hashkey not in sumdict:
                        sumdict[Hashkey] = {}
                    if windspd not in sumdict[Hashkey]:
                        sumdict[Hashkey][windspd] = [0,0]
                    sumdict[Hashkey][windspd][0] += pwrvalue
                    sumdict[Hashkey][windspd][1] += 1
        Hashkeydict = sorted(sumdict)
        for Hashkey in Hashkeydict:
            array = []
            dict = {}
            for windspd in sumdict[Hashkey]:
                sumdict[Hashkey][windspd][0] = sumdict[Hashkey][windspd][0] / sumdict[Hashkey][windspd][1]
                sumdict[Hashkey][windspd] = format(sumdict[Hashkey][windspd][0],'.6f')
                array.append([windspd,sumdict[Hashkey][windspd]])
                dict[windspd] = sumdict[Hashkey][windspd]
            array.sort()
            sumdict[Hashkey] = array
            wind_code = Hashkey
            wind_type = self.devsCalcDict[Hashkey]['devType']
            curve_date = thismonth.strftime('%Y-%m-01')
            power_curve_his = json.dumps(dict)
            selectsql = "SELECT id FROM scada_wind_power_curve_his WHERE wind_code = '{0}' AND curve_date = '{1}'" \
                .format(wind_code, curve_date)
            self.my.execu(selectsql)
            self.my.commit()
            result = self.my.fetchall()
            if result:
                recid = result[0][0]
                updatesql = "UPDATE scada_wind_power_curve_his SET power_curve_his = '{0}' WHERE id = '{1}'" \
                    .format(power_curve_his, recid)
                self.my.execu(updatesql)
                self.my.commit()
            else:
                insertlist = list()
                insertlist.append('UUID()')
                insertlist.append("'" + wind_code + "'")
                insertlist.append("'" + wind_type + "'")
                insertlist.append("'" + curve_date + "'")
                insertlist.append("'" + power_curve_his + "'")
                insertSQL = "insert into `scada_wind_power_curve_his` value (" + ','.join(insertlist) + ')'
                self.my.execu(insertSQL)
                self.my.commit()
        self.my.disconnection()

    # 获取历史功率曲线
    def get_power_curve_his(self):
        self.my.connectionForLocal()
        now = datetime.datetime.now()
        thismonth = datetime.datetime(now.year,now.month,1)
        thismonthstr = thismonth.strftime('%Y-%m-01')
        sql = "select wind_code, power_curve_his from `scada_wind_power_curve_his` where curve_date = '{0}'".format(thismonthstr)
        self.my.execu(sql)
        self.my.commit()
        result = self.my.fetchall()
        power_curve_dict = {}
        for i in range(len(result)):
            Hashkey = result[i][0]
            power_curve = result[i][1]
            array = []
            datadict = json.loads(power_curve)
            for spd in datadict:
                pwr = float(datadict[spd])
                spd = float(spd)
                array.append([spd,pwr])
            power_curve_dict[Hashkey] = array
        return power_curve_dict

    # 计算损失电量
    def calc_lost_power(self,day_time_str, today_time_str):
        day_time = time.mktime(time.strptime(day_time_str, "%Y-%m-%d %H:%M:%S"))
        today_time = time.mktime(time.strptime(today_time_str, "%Y-%m-%d %H:%M:%S"))
        NewCalc_Stndst = getdata.getdata('NewCalcRT_StndSt',self.tags, 'sum', day_time, today_time, 'end', None, None, samplingValue='1', samplingUnit='milliseconds')
        frequency = 10 * 60
        time_ranges = self.split_time_ranges(day_time_str, today_time_str, frequency)
        listing = self.getlistingdata(day_time_str, today_time_str) # 挂牌记录
        for timearray in time_ranges:
            fromtime = int(time.mktime(time.strptime(timearray[0], "%Y-%m-%d %H:%M:%S")))
            totime = int(time.mktime(time.strptime(timearray[1], "%Y-%m-%d %H:%M:%S")))
            lostpwr_sumdict = {}
            for HashKey, _ in self.devsCalcDict.items():
                publish_ch_list = str(HashKey).split(':')
                project = publish_ch_list[0]
                farm = publish_ch_list[1]
                term = publish_ch_list[2]
                term_full = (':').join([project, farm, term])
                farm_full = (':').join([project, farm])
                if term_full not in lostpwr_sumdict:
                    lostpwr_sumdict[term_full] = {}
                if farm_full not in lostpwr_sumdict:
                    lostpwr_sumdict[farm_full] = {}
                ActPWR_AVG_10m = self.getvalue(HashKey,'ActPWR_AVG_10m',totime)
                Theory_PWR_Inter_his = self.getvalue(HashKey,'Theory_PWR_Inter_his',totime)
                if Theory_PWR_Inter_his and ActPWR_AVG_10m:
                    lostpwr = Theory_PWR_Inter_his - ActPWR_AVG_10m if Theory_PWR_Inter_his > ActPWR_AVG_10m else 0 # 10分钟总损失电量
                    self._set_value(HashKey,'CalcRT_LostPwr_All',lostpwr/6,totime)
                    if 'All' in lostpwr_sumdict[farm_full]:
                        lostpwr_sumdict[farm_full]['All'] += lostpwr/6
                    else:
                        lostpwr_sumdict[farm_full]['All'] = lostpwr/6
                    if 'All' in lostpwr_sumdict[term_full]:
                        lostpwr_sumdict[term_full]['All'] += lostpwr/6
                    else:
                        lostpwr_sumdict[term_full]['All'] = lostpwr/6
                    lostpwr_dict = {}
                    if HashKey in NewCalc_Stndst:
                        NewCalc_Stndst_HashKey = NewCalc_Stndst[HashKey] 
                        NewCalc_Stndst_HashKey.sort()
                        values = []
                        for Stndst in NewCalc_Stndst_HashKey:
                            timei = Stndst[0]/1000
                            valuei = Stndst[1]
                            code = self.transfmt(valuei,"st")
                            if fromtime < timei < totime:
                                values.append([timei,code])
                        values.sort()
                        values.append([fromtime,values[0][1]])
                        values.append([totime,values[len(values) - 1][1]])
                        values.sort()
                        overlaparr = self.findoverlap(HashKey, listing, fromtime, totime)
                        if overlaparr:
                            code           = overlaparr[0]
                            overlap_start  = overlaparr[1]
                            overlap_end    = overlaparr[2]
                            code2 = 0
                            entryi = 0
                            for value in values:
                                timei = value[0]
                                codei = value[1]
                                if overlap_start < timei < overlap_end:
                                    if entryi == 0:
                                        code2 = codei
                                        entryi += 1
                            values.append([overlap_start,code2])
                            values.append([overlap_end,code])
                            values = [i for i in values if i <= overlap_start and  i >= overlap_end]
                            values.sort()
                        for i in range(1,len(values)):
                            timei = values[i][0] - values[i - 1][0]
                            valuei = values[i][1]
                            lostpwri = timei/3600.0*lostpwr
                            if valuei in lostpwr_dict:
                                lostpwr_dict[valuei] += lostpwri
                            else:
                                lostpwr_dict[valuei] = lostpwri
                            if valuei in lostpwr_sumdict[term_full]:
                                lostpwr_sumdict[term_full][valuei] += lostpwri
                            else:
                                lostpwr_sumdict[term_full][valuei] = lostpwri
                            if valuei in lostpwr_sumdict[farm_full]:
                                lostpwr_sumdict[farm_full][valuei] += lostpwri
                            else:
                                lostpwr_sumdict[farm_full][valuei] = lostpwri
                        
                        for i in range(1,13):
                            self._set_value(HashKey,'CalcRT_LostPwr_' + str(i), 0, totime) 
                        for key, attrs in lostpwr_dict.items():
                            self._set_value(HashKey,'CalcRT_LostPwr_' + str(key), attrs, totime)
            for key in lostpwr_sumdict:
                for i in range(1,13):
                    self._set_value(key,'CalcRT_LostPwr_' + str(i), 0, totime)
                for num in lostpwr_sumdict[key]:
                    self._set_value(key,'CalcRT_LostPwr_' + str(num), lostpwr_sumdict[key][num], totime)

    # 获取挂牌记录
    def getlistingdata(self,day_time_str,today_time_str):
        try:
            self.my.connectionForLocal()
            sql = "SELECT t.device,t.listingNo,t.realBgnTm,t.realEndTm from scada_listing_result_his t where realBgnTm >= '{0}' and realEndTm <= '{1}'" \
                .format(day_time_str, today_time_str)
            self.my.execu(sql)
            self.my.commit()
            results = self.my.fetchall()
            if results:
                results_dict = {}
                for result in results:
                    HashKey = result[0]
                    code = result[1]
                    begintime = result[2]
                    endtime = result[3]
                    if HashKey in results_dict:
                        results_dict[HashKey].append([code, begintime, endtime])
                    else:
                        results_dict[HashKey] = [[code, begintime, endtime]]
                return results_dict
            else:
                return None
        except Exception as e:
            print(e)
        self.my.disconnection()

    # 寻找时间交集
    def findoverlap(self,HashKey, listing, fromtime, totime):
        if listing:
            if HashKey in listing:
                for i in range(len(listing[HashKey])):
                    code = listing[HashKey][i][0]
                    recode = self.transfmt(code, "guapai")
                    begintime = listing[HashKey][i][1]
                    endtime = listing[HashKey][i][2]
                    begintime = int(time.mktime(begintime.timetuple()))
                    endtime = int(time.mktime(endtime.timetuple()))
                    overlap_start = max(begintime, fromtime)
                    overlap_end = min(endtime, totime)
                    if overlap_start < overlap_end:
                        return (recode, overlap_start, overlap_end)

    # 格式转换
    def transfmt(self,code,fmt):
        if fmt == "st":
            if code == 0:       # 手动停机
                return 3
            elif code == 1:     # 正常发电
                return 1
            elif code == 2:     # 环境待命
                return 2
            elif code == 3:     # 维护状态
                return 3
            elif code == 4:     # 故障停机
                return 4
            elif code == 5:     # 未知状态
                return 5
            elif code == 6:     # 降出力运行
                return 6
            elif code == 7:     # 技术待命
                return 7
            elif code == 8:     # 电网故障
                return 8
        elif fmt == "guapai":
            if code == 1:       # 覆冰停机
                return 2
            elif code == 2:     # 调度限电
                return 9
            elif code == 3:     # 输变电计划停运
                return 11
            elif code == 4:     # 输变电非计划停运
                return 10
            elif code == 5:     # 暴风停机
                return 2
            elif code == 6:     # 环境超温
                return 2
            elif code == 7:     # 故障维护
                return 4
            elif code == 8:     # 定检维护
                return 3
            elif code == 9:     # 计划检修
                return 3
            elif code == 10:    # 机组故障
                return 4
            elif code == 11:    # 自降容
                return 6
            elif code == 12:    # 电网检修
                return 12
            elif code == 13:    # 电网故障
                return 8

    # 时间切片
    def split_time_ranges(self, from_time, to_time, frequency):
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
        return time_ranges

    # 检查状态
    def _check_st(self, devname):
        paraname = 'lastStndSt1'
        CalcRT_StndSt = self.StMap[devname][1]
        CalcRT_PrevStndSt = self.StMap[devname][0]
        if devname in self.ValueMap:
            if paraname in self.ValueMap[devname]:
                lastStndSt = self.ValueMap[devname][paraname]
            else:  
                lastStndSt = 9
        
        if CalcRT_StndSt != lastStndSt:
            print(devname,'last:',lastStndSt,' ,now:',CalcRT_StndSt)
            self.fltfilter(devname,CalcRT_StndSt,lastStndSt)
        else:
            for calctype in ('', 'D', 'M', 'Y'):
                fltTime, _ = self.getvalue(devname, 'CalcRT_FltFilterTime' + calctype)
                self._set_value(devname, 'CalcRT_FltFilterTime' + calctype, fltTime)
        self.ValueMap[devname][paraname] = CalcRT_StndSt

    # 故障次数条件过滤计算
    def fltfilter(self,devname,standst,oldstandst):
        #上次故障开始时间
        _, last_flt_timebegin = self.getvalue(devname, 'CalcRT_FltChngTmbegin')
        #上次故障结束时间
        _, last_flt_timeend = self.getvalue(devname, 'CalcRT_FltChngTmend')
        if last_flt_timebegin == 0:
            last_flt_timebegin = self.nowtime - diffTimer
            self._set_value(devname, 'CalcRT_FltChngTmbegin', True, last_flt_timebegin)
        if last_flt_timeend == 0:
            last_flt_timeend = self.nowtime - diffTimer
            self._set_value(devname, 'CalcRT_FltChngTmend', True, last_flt_timeend)
        #如果这次状态为故障
        if standst == 4:
            # 故障状态开始时间
            self._set_value(devname, 'CalcRT_FltChngTmbegin', True)
            # print(devname,standst)
            # 两次故障时间间隔
            fltTimeInterval1 = self.nowtime - last_flt_timeend
            # print('fltTimeInterval1',fltTimeInterval1)
            # 如果两次时间间隔大于等于15分钟，则增加一次故障次数
            if fltTimeInterval1 >= diffTimer:
                # print('FltFilterTime + 1.........................................')
                for calctype in ('', 'D', 'M', 'Y'):
                    fltcnt, _ = self.getvalue(devname, 'CalcRT_FltFilterCnt' + calctype)
                    self._set_value(devname, 'CalcRT_FltFilterCnt' + calctype, fltcnt + 1)
            else:
                # 如果两次时间间隔小于15分钟，两次故障合并，之前15分钟内的时间算作故障时间
                for calctype in ('', 'D', 'M', 'Y'):
                    fltTime, _ = self.getvalue(devname, 'CalcRT_FltFilterTime' + calctype)
                    self._set_value(devname, 'CalcRT_FltFilterTime' + calctype, fltTime + fltTimeInterval1)

                for calctype in ('', 'D', 'M', 'Y'):
                    fltcnt, _ = self.getvalue(devname, 'CalcRT_FltFilterCnt' + calctype)
                    self._set_value(devname, 'CalcRT_FltFilterCnt' + calctype, fltcnt) 
        else:
            for calctype in ('', 'D', 'M', 'Y'):
                fltcnt, _ = self.getvalue(devname, 'CalcRT_FltFilterCnt' + calctype)
                self._set_value(devname, 'CalcRT_FltFilterCnt' + calctype, fltcnt)
        
        # 如果上次状态为故障，
        if oldstandst == 4:
            # 上次故障结束时间
            self._set_value(devname, 'CalcRT_FltChngTmend', True)
            # 故障时长
            FltTime = self.nowtime - last_flt_timebegin
            # 两次故障时间间隔
            fltTimeInterval2 = last_flt_timebegin - last_flt_timeend
            # print('故障结束')
            # print('故障时长为',FltTime)
            # print('前两次故障时间间隔',fltTimeInterval2)
            # 如果上次故障时长小于300s且两次故障时间间隔大于等于15分钟，减少一次故障次数
            if FltTime < 300 and fltTimeInterval2 >= diffTimer:
                for calctype in ('', 'D', 'M', 'Y'):
                    fltcnt, _ = self.getvalue(devname, 'CalcRT_FltFilterCnt' + calctype)
                    self._set_value(devname, 'CalcRT_FltFilterCnt' + calctype, fltcnt - 1)
                    # print('上次状态为故障，两次故障时间间隔为',fltTimeInterval2,', 故障次数减1')
            else:
                for calctype in ('', 'D', 'M', 'Y'):
                    fltcnt, _ = self.getvalue(devname, 'CalcRT_FltFilterCnt' + calctype)
                    self._set_value(devname, 'CalcRT_FltFilterCnt' + calctype, fltcnt)
            if FltTime >= 300:
                for calctype in ('', 'D', 'M', 'Y'):
                    flttime, _ = self.getvalue(devname, 'CalcRT_FltFilterTime' + calctype)
                    self._set_value(devname, 'CalcRT_FltFilterTime' + calctype, flttime + FltTime)

    # 设置某设备的指标值
    def _set_value(self, dev_name, para_name, value, time=None):
    # Args:
    #     dev_name: 设备名
    #     para_name: 指标名
    #     value: 指标值
        if dev_name not in self.ValueMap:
            self.ValueMap[dev_name] = dict()
            self.TimeMap[dev_name] = dict()
        self.ValueMap[dev_name][para_name] = value
        self.TimeMap[dev_name][para_name] = self.nowtime

        if time == None:
            time = self.nowtime
        if dev_name not in self.ValuesMap:
            self.ValuesMap[dev_name] = dict()
        if para_name not in self.ValuesMap[dev_name]:
            self.ValuesMap[dev_name][para_name] = dict()
        self.ValuesMap[dev_name][para_name][time] = value
        self.TimeMap[dev_name][para_name] = time
        datatype = 'S'
        if type(value) == int:
            datatype = 'I'
        elif type(value) == float:
            datatype = 'F'
        elif type(value) == bool:
            datatype = 'B'
            if value:
                value = 1
            else:
                value = 0
        self.MsgList.append(dev_name + ':' + para_name + '@' + datatype + ':' + str(value) + ':' + str(time))

    # 获取缓存值 Value, Time
    def getvalue(self, devname, paraname, time=None):
        if time == None:
            if devname in self.ValueMap:
                if paraname in self.ValueMap[devname]:
                    return (self.ValueMap[devname][paraname], self.TimeMap[devname][paraname])
            return (0, 0)
        else:
            if devname in self.ValuesMap:
                if paraname in self.ValuesMap[devname]:
                    if time in self.ValuesMap[devname][paraname]:
                        return self.ValuesMap[devname][paraname][time]
            return None
    
    # 总和计算
    def calcsum(self, devname, term, farm):
        for para in self.SumList:
            value, _ = self.getvalue(devname, para)
            self.SumMap[(term, para)] += value
            self.SumMap[(farm, para)] += value
            
    # 存入历史数据
    def output_his_data(self):
        qsize = self.hisdata_queue.qsize()
        if not self.hisdata_queue.empty():
            dataslist = []
            dataslistOK = []
            for i in range(0, qsize):
                dataslist.extend(self.hisdata_queue.get(False))

            for fullpoint_value in dataslist:
                fullpoint_value = fullpoint_value.split('@')
                fullpointlist = fullpoint_value[0].rsplit(':', 1)
                strProjectDev = fullpointlist[0]
                strTagBasic = fullpointlist[1]
                valuelist = fullpoint_value[1].split(':')
                value = valuelist[1]
                intBeginDate = int(valuelist[2])
                onerecord = self.kairos.composeData(strProjectDev, strTagBasic, intBeginDate, 'F', value)
                if onerecord:
                    dataslistOK.append(onerecord)

            if len(dataslistOK) > 0:
                jsonRows = json.dumps(dataslistOK)
                filename = str(int(time.time()))
                dataFile = os.path.join(dstDir, filename)
                with gzip.GzipFile(filename=dataFile + '.dat', mode='wb', compresslevel=9) as (f):
                    f.write(jsonRows)
        print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())," output_his_data success")
    
    # 时间重置
    def resettime(self):
        nowtime = int(time.time())
        timeparas = dict()
        zerostr = 'F:0:' + str(nowtime)
        timeparas['CalcRT_FltFilterCntD'] = zerostr
        timeparas['CalcRT_FltFilterTimeD'] = zerostr
        nowdate = datetime.datetime.now()
        if nowdate.day == 1:
            timeparas['CalcRT_FltFilterCntM'] = zerostr
            timeparas['CalcRT_FltFilterTimeM'] = zerostr
            if nowdate.month == 1:
                timeparas['CalcRT_FltFilterCntY'] = zerostr
                timeparas['CalcRT_FltFilterTimeY'] = zerostr
        msglist = list()
        for HashKey in self.devsCalcDict.keys():
            for paraname in timeparas.keys():
                msglist.append(HashKey + ':' + paraname + '@' + zerostr)
                self.ValueMap[HashKey][paraname] = 0
                self.TimeMap[HashKey][paraname] = nowtime

        self.redis.conn.publish(channel_main, zlib.compress((',').join(msglist)))

if __name__ == '__main__':
    devCalc = DevCalc()
    # # devCalc.dev_calc_hismonth()
    # day_time_str = "2023-2-13 00:00:00"
    # today_time_str = "2023-2-14 00:00:00"
    # devCalc.getlistingdata(day_time_str,today_time_str)
    devCalc.dev_calc_consistency()