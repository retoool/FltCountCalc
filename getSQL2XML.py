#!/usr/bin/env python
# coding:utf-8
# author: wutiezhong
# company: www.dtxytech.com
# datatime: 2017-11-12
# version: 1.0
# function describe: created dev standard status calculation xml configure file from mysql

from imp import reload
import sys
import os
from string import maketrans

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from lxml import etree
import mylib.xy_mysql as mysql

# from string import maketrans    #python2

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

# try:
#     import xml.etree.cElementTree as etree
# except ImportError:
#     import xml.etree.ElementTree as etree

# import importlib         #python3.4以上
# importlib.reload(sys)    #python3.4以上

reload(sys)
sys.setdefaultencoding("utf8")


class GetMySQL2XML:
    """ # 数据库的连接初始化及文件读取等操作

    """
    def __init__(self):
        self.my = mysql.MySQL()
        self.my.connectionForLocal()

        # 打开xy_devStCalc.xml文件,如果失败就创建xml文件
        self.file = "xy_devStCalc.xml"
        try:
            self.xmlTree = ET.parse('xy_devStCalc.xml')
            print("'xy_devStCalc.xml' already exists")
        except Exception as e:
            result = self.sqlDoingCreatXML()
            if result:
                print("success")
            else:
                print("failture")
        self.my.disconnection()

    def sqlDoingCreatXML(self):
        fullcodedict = self.getwindhigh()
        strSQL = "SELECT CODE, MachineTypeCode, MachineTypeName as enrgType, line_code, SalveName from v_scada_machine_group ORDER BY CODE"
        self.my.execu(strSQL)
        self.my.commit()
        sqlResult1 = self.my.fetchall()
        if sqlResult1:
            root = etree.Element("data", nsmap={'xsi': 'http://www.dtxytech.com/'})
            for i in range(0, len(sqlResult1)):
                try:
                    CODE = str(sqlResult1[i][0])  # 设备全点名
                    MachineTypeCode = sqlResult1[i][1]  # 设备型号
                    enrgType = sqlResult1[i][2]  # 能源类型
                    line_code = sqlResult1[i][3]  # 所属集电线
                    descrTeam = sqlResult1[i][4]  # 场站建设期中文描述
                    CODEList = CODE.split(":")
                    NOproject = CODEList[0]  # 工程项目编号
                    NOfarmStation = CODEList[1]  # 场站编号
                    NOterm = CODEList[2]  # 建设期编号
                    NOdev = CODEList[3]  # 设备编号

                    altitude = fullcodedict[CODE]["altitude"]  # 海拔高度
                    hubHeight = fullcodedict[CODE]["hubHeight"] # 轮毂高度

                    if altitude == None or altitude == '':
                        altitude = '1500'
                    if hubHeight ==None or hubHeight == '':
                        hubHeight = '80'
                    # 项目
                    if root.find('./project[@ID="%s"]' % NOproject) is None:
                        project = etree.SubElement(root, "project")
                        project.set("ID", NOproject)
                    # 场站
                    if root.find('./project[@ID="%s"]/farmStation[@ID="%s"]' % (NOproject, NOfarmStation)) is None:
                        farmStation = etree.SubElement(project, "farmStation")
                        farmStation.set("ID", NOfarmStation)
                        if enrgType == "风电":
                            farmStation.set("enrgType", "FD")
                        elif enrgType == "光伏":
                            farmStation.set("enrgType", "GF")
                    # 建设期
                    if root.find('./project[@ID="%s"]/farmStation[@ID="%s"]/term[@ID="%s"]' % (
                            NOproject, NOfarmStation, NOterm)) is None:
                        term = etree.SubElement(farmStation, "term")
                        term.set("ID", NOterm)
                        term.set("Calc", "True")
                        # term.text = descrTeam+"\n"
                    # 集电线
                    if root.find('./project[@ID="%s"]/farmStation[@ID="%s"]/term[@ID="%s"]/line[@ID="%s"]' % (
                            NOproject, NOfarmStation, NOterm, line_code)) is None:
                        line = etree.SubElement(term, "line")
                        if line_code is not None:
                            line.set("ID", line_code)
                        else:
                            line.set("ID", "L001")
                    # 风机设备
                    if root.find(
                            './project[@ID="%s"]/farmStation[@ID="%s"]/term[@ID="%s"]/line[@ID="%s"]/dev[@devNO="%s"]'
                            % (NOproject, NOfarmStation, NOterm, line_code, NOdev)) is None:
                        dev = etree.SubElement(line, "dev")
                        dev.text = enrgType
                        dev.set("devType", MachineTypeCode)
                        dev.set("devNO", NOdev)
                        table = maketrans("%&^$@!|\\?*<\":>+[]/'", '_' * 19) ##pyhon2
                        # table = str(MachineTypeCode).maketrans("%&^$@!|\\?*<\":>+[]/'", '_' * 19)  ##pyhon3
                        devType = str(MachineTypeCode).translate(table)
                        dev.set("CalcRuleFile", devType)
                        if enrgType == "风电":
                            dev.set("altitude", altitude)
                            dev.set("hubHeight", hubHeight)
                except Exception as e:
                    pass
            tree = etree.ElementTree(root)
            tree.write('xy_devStCalc.xml', pretty_print=True, xml_declaration=True, encoding='utf-8')
            return True
        else:
            return False
 
    def getwindhigh(self):
        SQL1 = "select t1.id, t1.code, t1.PARENT_ID from security_organization as t1 where t1.nature is not null and t1.enabled = 1"
        self.my.execu(SQL1)
        self.my.commit()
        sqlResult1 = self.my.fetchall()
        orgcodedict = {}
        if sqlResult1:
            for i in range(0, len(sqlResult1)):
                id = sqlResult1[i][0]
                code = sqlResult1[i][1]
                parentid = sqlResult1[i][2]
                orgcode = self.getorgcode(parentid, sqlResult1, code)
                orgcodedict[id] = orgcode
        SQL2 = "SELECT id, org_id, machine_code, altitude, hubHeight FROM `scada_wind_machine`"
        self.my.execu(SQL2)
        self.my.commit()
        sqlResult2 = self.my.fetchall()
        fullcodedict = {}
        if sqlResult2:
            for i in range(0,len(sqlResult2)):
                id = sqlResult2[i][0]
                orgId = sqlResult2[i][1]
                machineCode = sqlResult2[i][2]
                altitude = sqlResult2[i][3]
                hubHeight = sqlResult2[i][4]
                fullcode = orgcodedict[orgId] + ':' + machineCode
                # if machineCode[0] == 'W':
                fullcodedict[fullcode] = dict()
                fullcodedict[fullcode]['altitude'] = altitude
                fullcodedict[fullcode]['hubHeight'] = hubHeight
        return fullcodedict

    def getorgcode(self, parentid, sqlResult, code):
        for i in range(0,len(sqlResult)):
            id = sqlResult[i][0]
            newparentid = sqlResult[i][2]
            if id == parentid and sqlResult[i][1] != 'root':
                newcode = sqlResult[i][1] + ':' + code
                code = self.getorgcode(newparentid, sqlResult, newcode)
        return code

if __name__ == '__main__':
    getMySQL2XML = GetMySQL2XML()
