CREATE TABLE `scada_wind_power_curve_his` (
  `id` varchar(40) NOT NULL,
  `wind_code` varchar(40) DEFAULT NULL COMMENT '风机id',
  `wind_type` varchar(40) DEFAULT NULL COMMENT '风机型号',
  `curve_date` date NOT NULL COMMENT '曲线存储日期',
  `power_curve_his` text,
  PRIMARY KEY (`id`,`curve_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='历史功率曲线（历史三个月实际有功功率拟合）'