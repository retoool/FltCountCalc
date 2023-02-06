#!/usr/bin/python
# coding=utf-8

# from imp import reload
from imp import reload
import sys,os,csv
import pandas as pd
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from xml import etree
import mylib.xy_mysql as mysql

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
    
reload(sys)
sys.setdefaultencoding("utf8")

class GetEXCELData:
    def main(self):
        self.my = mysql.MySQL()
        self.my.connectionForLocal()
        self.sqlDoingCreatXML()
        self.my.disconnection()

    def sqlDoingCreatXML(self):
        SQL1 = "select id, code, PARENT_ID from security_organization where nature is not null and enabled = 1"
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
        SQL2 = "SELECT id, org_id, machine_code, altitude, attr1 FROM `scada_wind_machine`"
        self.my.execu(SQL2)
        self.my.commit()
        exceldata = self.getExceldata()
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
                if machineCode[0] == 'W':
                    fullcodedict[fullcode] = dict()
                    fullcodedict[fullcode]['id'] = id
                    fullcodedict[fullcode]['altitude'] = altitude
                    fullcodedict[fullcode]['hubHeight'] = hubHeight
        for fullcode in fullcodedict:
            if exceldata.has_key(fullcode):
                altitude = exceldata[fullcode][0]
                hubHeight = exceldata[fullcode][1]
                id = fullcodedict[fullcode]['id']
                updatesql = "UPDATE scada_wind_machine SET altitude = '{0}', hubHeight = '{1}' WHERE id = '{2}'".format(altitude, hubHeight, id)
                print(updatesql)
                self.my.execu(updatesql)
                self.my.commit()

    def getorgcode(self, parentid, sqlResult, code):
        for i in range(0,len(sqlResult)):
            id = sqlResult[i][0]
            newparentid = sqlResult[i][2]
            if id == parentid and sqlResult[i][1] != 'root':
                newcode = sqlResult[i][1] + ':' + code
                code = self.getorgcode(newparentid, sqlResult, newcode)
        return code

    def getExceldata(self):
        dataarray = {}
        exceldata = pd.read_excel('excel2mysql.xls',sheet_name = 0)
        exceldata.to_csv('csvdata.dat', encoding = 'utf-8')
        with open('csvdata.dat','r') as csvdata:
            readcsv = csv.reader(csvdata)
            for i, row in enumerate(readcsv):
                if i >= 1:
                    id = row[0]
                    fullcode = row[1]
                    altitude = row[2]
                    hubHeight = row[3]
                    dataarray[fullcode] = [altitude, hubHeight]
        return dataarray

if __name__ == '__main__':
    getdata = GetEXCELData()
    getdata.main()
