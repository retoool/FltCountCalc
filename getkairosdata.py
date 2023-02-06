#!/usr/bin/python
# -*-coding:utf-8-*-

import sys
import os
import datetime
import time

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

import requests
import json
import mylib.xy_configuration as config


JSIntegralH = 'function fx(timestamps,values)' \
              '{var i = values.length - 1,sum = 0,result = {};' \
              'while( i-- ){sum += values[i] * (timestamps[i+1] - timestamps[i]) / 3600000;}' \
              'result.timestamp=timestamps[0];result.value=sum;' \
              'return result;}'

JSIntegralS = 'function fx(timestamps,values)' \
              '{var i = values.length - 1,sum = 0,result = {};' \
              'while( i-- ){sum += values[i] * (timestamps[i+1] - timestamps[i]) / 1000;}' \
              'result.timestamp=timestamps[0];result.value=sum;' \
              'return result;}'


class KairosData(object):
    def __init__(self):
        try:
            strserver = config.read('Kairos', 'Server')
            strport = str(int(config.read('Kairos', 'Port')))
            self.Url = 'http://%s:%s/api/v1/datapoints/query' % (strserver, strport)
            self.DelUrl = 'http://%s:%s/api/v1/metric/' % (strserver, strport)
            self.headers = {'content-type': 'application/json'}
            self.session = requests.session()
        except Exception as e:
            print('--init kairosdb is error:' + e.message)

    def getdata(self, pointname, aggr, starttime, endtime, aligntime, minvalue, maxvalue, saveas=None, tagMatch=None, samplingValue=None, samplingUnit=None, filter=None):
        try:
            if not self.session:
                print('--init session is null')
                return
            bodytext = {'start_absolute': starttime * 1000,
                        'end_absolute': endtime * 1000,
                        'metrics': [{}]
                        }
            pos = pointname.rfind(':')
            if pos >= 0:
                devname = pointname[0:pos]
                keyname = pointname[pos + 1:]
                bodytext['metrics'][0]['name'] = keyname
                bodytext['metrics'][0]['tags'] = {'project': [devname]}
            else:
                keyname = pointname
                bodytext['metrics'][0]['name'] = keyname
                bodytext['metrics'][0]['group_by'] = [{'name': 'tag', 'tags': ['project']}]
            bodytext['metrics'][0]['aggregators'] = []
            if samplingValue == None and samplingUnit == None:
                samplingValue = '10'
                samplingUnit = 'years'
            if minvalue is not None:
                bodytext['metrics'][0]['aggregators'].append(
                    {
                        'name': 'filter',
                        'filter_op': 'lt',
                        'threshold': str(minvalue)
                    }
                )
            if maxvalue is not None:
                bodytext['metrics'][0]['aggregators'].append(
                    {
                        'name': 'filter',
                        'filter_op': 'gt',
                        'threshold': str(maxvalue)
                    }
                )
            if aggr.lower() == 'integral_h':
                bodytext['metrics'][0]['aggregators'].append(
                    {
                        'name': 'jsrange',
                        'script': JSIntegralH,
                        'sampling': {'value': '10', 'unit': 'years'}
                    }
                )
                bodytext['metrics'][0]['aggregators'].append(
                    {
                        'name': 'first',
                        'sampling': {'value': '10', 'unit': 'years'}
                    }
                )
            elif aggr.lower() == 'integral_s':
                bodytext['metrics'][0]['aggregators'].append(
                    {
                        'name': 'jsrange',
                        'script': JSIntegralS,
                        'sampling': {'value': '10', 'unit': 'years'}
                    }
                )
                bodytext['metrics'][0]['aggregators'].append(
                    {
                        'name': 'first',
                        'sampling': {'value': '10', 'unit': 'years'}
                    }
                )
            elif aggr.lower() == 'sum':
                bodytext['metrics'][0]['aggregators'].append(
                    {
                        'name': aggr.lower(),   
                        'sampling': {'value': samplingValue, 'unit': samplingUnit}
                    }
                )
            elif aggr.lower() == 'lastmax':
                bodytext['metrics'][0]['aggregators'].append(
                    {
                        'name': 'max',
                        'sampling': {'value': '61', 'unit': 'minutes'}
                    }
                )
                bodytext['metrics'][0]['aggregators'].append(
                    {
                        'name': 'last',
                        'sampling': {'value': '10', 'unit': 'years'}
                    }
                )
            elif aggr:
                if samplingValue == None and samplingUnit == None:
                    samplingValue = '10'
                    samplingUnit = 'years'
                bodytext['metrics'][0]['aggregators'].append(
                    {
                        'name': aggr.lower(),   
                        'sampling': {'value': samplingValue, 'unit': samplingUnit}
                    }
                )
            if aligntime == 'end' and endtime > starttime:
                aligndict = {'align_end_time': True}
            else:
                aligndict = {'align_start_time': True}
            if aggr:
                bodytext['metrics'][0]['aggregators'][-1].update(aligndict)
            if saveas:
                bodytext['metrics'][0]['aggregators'].append(
                    {
                        'name': 'save_as',
                        'add_saved_from': False,
                        'metric_name': saveas
                    }
                )
            if filter:
                for i in range(len(filter)):
                    bodytext['metrics'][0]['aggregators'].append(
                        {
                            'name': 'filter',
                            'filter_op': filter[i][0],
                            'threshold': filter[i][1]
                        }
                    )
            response = self.session.post(self.Url, json.dumps(bodytext), headers=self.headers)
            if response.status_code == 200:
                buf = json.loads(response.text)['queries'][0]['results']
                # doc = open('out.txt','w')
                # print >>doc, (buf)
                # doc.close()
                resultLen = len(buf)
                if resultLen == 0:
                    print ("Error:no data found for point " + pointname)
                results = {}
                for i in range(resultLen):
                    if 'project' in buf[i]['tags']:
                        if len(buf[i]['tags']['project']) > 0:
                            dev = buf[i]['tags']['project'][0]
                            if tagMatch == None:
                                if len(buf[i]['values']) > 0:
                                    results[dev] = buf[i]['values']
                            else:
                                if dev.find(tagMatch) >= 0:
                                    if len(buf[i]['values']) > 0:
                                        results[dev] = buf[i]['values'][0]
                return results
            else:
                print('--request error: ' + pointname + ' - ' + response.text)
                return {}
        except Exception as e:
            print('--write kairosdb is error: ' + pointname + ' - ' + str(e.message))
            return

    def deletemetric(self, metric):
        self.session.delete(self.DelUrl + metric)
        return

if __name__ == '__main__':
    deletepoint = [
        'WNAC_WdSpd_AVG_10m',
        'WNAC_WdSpd_InterAVG_10m',
        'WNAC_WdSpd_DEV_10m',
        'WNAC_WdSpd_Interval_10m',
        'NewCalcRT_StndSt_AVG_10m',
        'WNAC_ExTmp_AVG_10m',
        'ActPWR_AVG_10m',
        'WROT_Pt1Ang_AVG_10m',
        'WROT_Pt1Ang_MAX_10m',
        'CalcRT_density_AVG_10m',
        'CalcRT_WdSpdStnd_AVG_10m',
        'ActPWR_Filter_Tag',
        'ActPWR_Filter_AVG_10m',
        'Theory_PWR_Inter',
        'Theory_PWR_Inter_Filter',
        'Theory_PWR_Interval',
        'ActPWR_Fitting_AVG_10m',
        'Theory_PWR_Inter_Fitting',
        'Theory_PWR_Inter_Fitting_his',
        'Theory_PWR_Inter_Filter_his',
        'Theory_PWR_Inter_his',
        'Theory_PWR_Inter_Filter_his',
        'WNAC_WdSpd_FilterAVG_10m',
        'WNAC_WdSpd_FilterStndSt_10m'
        ]
    kairosdata = KairosData()
    for point in deletepoint:
        kairosdata.deletemetric(point)