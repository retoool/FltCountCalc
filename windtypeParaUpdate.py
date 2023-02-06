#!/usr/bin/env python
# coding:utf-8
from imp import reload
import sys
import os
import json
import collections
from collections import OrderedDict

# import importlib         #python3.4以上
# importlib.reload(sys)    #python3.4以上

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
reload(sys)
sys.setdefaultencoding("utf8")
import mylib.xy_mysql as mysql

class WndTypParaUpdate:
    """查询数据库，并生成风机类型参数JSON文件
    """

    def __init__(self):
        # 数据库的连接初始化及文件读取等操作
        self.my = mysql.MySQL()
        self.my.connectionForLocal()

        # 打开json文件,如果失败就创建json文件
        self.file = "windtypePara.json"
        # self.jsonContect = collections.OrderedDict()
        try:
            jsonfile = open(self.file, "r+")
            self.jsonContect = json.load(jsonfile, object_pairs_hook=OrderedDict)
            jsonfile.close()
            print("'windtypePara.json' already exists")
        except Exception as e:
            print(repr(e))
            self.jsonContect = self.sqldoing()
            jsonfile = open(self.file, "w")
            jsonfile.write(json.dumps(self.jsonContect, ensure_ascii=False))
            jsonfile.close()
        self.my.disconnection()

        # 具体实现数据库的结果查询

    def sqldoing(self):
        strSQL = "SELECT a.id,a.wind_type,b.name,a.capacity,a.windspeed_cutin,a.windspeed_cutout FROM `scada_wind_type` as a JOIN (SELECT id,name FROM scada_wind_factory)as b on a.wind_factory=b.id ORDER BY b.name,a.capacity"
        self.my.execu(strSQL)
        self.my.commit()
        dictWndType = collections.OrderedDict()  # {"wind_type":{"name":"","capacity":"","windspeed_cutin":"","windspeed_cutout":""}}
        dictWndPwr = collections.OrderedDict()
        sqlResult1 = self.my.fetchall()
        if sqlResult1:
            for i in range(0, len(sqlResult1)):
                id = sqlResult1[i][0]
                strSQL = "SELECT wind_type,speed,power FROM `scada_theory_power_curves` WHERE wind_type='{0}' ORDER BY speed".format(
                    id)
                self.my.execute(strSQL)
                self.my.commit()
                sqlResult2 = self.my.fetchall()
                dicttemp = collections.OrderedDict()
                id_wind_type = "None"
                if sqlResult2:
                    id_wind_type = sqlResult2[0][0]
                    for wndPwr in sqlResult2:
                        speed = str(wndPwr[1])
                        power = float(wndPwr[2])
                        dicttemp.setdefault(speed, power)
                if id_wind_type != "None":
                    dictWndPwr.setdefault(id_wind_type, dicttemp)
        else:
            return None
        for windtype in sqlResult1:
            id = str(windtype[0])
            wind_type = str(windtype[1])
            wind_factory = str(windtype[2])
            capacity = float(windtype[3])
            windspeed_cutin = float(windtype[4])
            windspeed_cutout = float(windtype[5])
            dictWndTypeTmep = collections.OrderedDict()
            dicttemp = collections.OrderedDict()
            # dicttemp.setdefault("wind_type",wind_type)
            dicttemp.setdefault("wind_factory", wind_factory)
            dicttemp.setdefault("capacity", capacity)
            dicttemp.setdefault("windspeed_cutin", windspeed_cutin)
            dicttemp.setdefault("windspeed_cutout", windspeed_cutout)
            dicttemp.setdefault("windspeed_rate", 10.0)  # 因数据库缺少该字段的设计，后续修改
            dicttemp.setdefault("windspeed_life", 50.0)  # 因数据库缺少该字段的设计，后续修改
            dicttemp.setdefault("sweptArea", 1.0)  # 因数据库缺少该字段的设计，后续修改
            dictWndTypeTmep.update(dicttemp)
            if id in dictWndPwr:
                dicttempSpdPwr = dictWndPwr[id]
                dictWndTypeTmep.setdefault("powerCurve", dicttempSpdPwr)
                # dictWndTypeTmep.update(dicttempSpdPwr)
            dictWndType.setdefault(wind_type, dictWndTypeTmep)
        return dictWndType


if __name__ == '__main__':
    windtypeupdate = WndTypParaUpdate()