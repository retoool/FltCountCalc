WNAC_WdSpd_AVG_10m					10分钟风速平均值

WNAC_WdSpd_InterAVG_10m		        10分钟风速插补值

WNAC_WdSpd_DEV_10m					10分钟风速标准差

WNAC_WdSpd_Interval_10m			    10分钟风速区间值

WNAC_WdSpd_FilterAVG_10m		    10分钟风速过滤值

NewCalcRT_StndSt_AVG_10m			10分钟状态平均值

WNAC_ExTmp_AVG_10m					10分钟环境温度平均值

ActPWR_AVG_10m								 10分钟有功功率平均值

WROT_Pt1Ang_AVG_10m					  10分钟桨叶角度平均值

WROT_Pt1Ang_MAX_10m					 10分钟桨叶角度最大值

CalcRT_density_AVG_10m					 10分钟空气密度平均值

CalcRT_WdSpdStnd_AVG_10m			 10分钟标准空气密度风速平均值

ActPWR_Filter_Tag								  10分钟有功功率过滤标签

ActPWR_Filter_AVG_10m					   10分钟有功功率**过滤值**（*）

Theory_PWR_Inter								  10分钟理论功率线性插值

Theory_PWR_Inter_Filter					   10分钟理论功率线性插值**过滤值**（*）

Theory_PWR_Interval							 10分钟合同理论功率区间值

ActPWR_Fitting_AVG_10m					  10分钟有功功率**拟合值**

Theory_PWR_Inter_Fitting					  10分钟理论功率线性插值**拟合值**

Theory_PWR_Inter_Fitting_his			   10分钟理论功率历史线性插值**拟合值**

Theory_PWR_Interval_his					   10分钟合同理论**历史**功率区间值

Theory_PWR_Inter_his						    10分钟理论功率**历史**线性插值

Theory_PWR_Inter_Filter_his				 10分钟理论功率**历史**线性插值**过滤值**（*）


功率特性一致性（合同） = sum(ActPWR_Filter_AVG_10m) / sum(Theory_PWR_Inter_Filter)

功率特性一致性（历史） = sum(ActPWR_Filter_AVG_10m) / sum(Theory_PWR_Inter_Filter_his)