package com.lizhen.controller;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * 聚合出的能耗分项值
 *
 * @author chen qinping
 * @version 1.0.0
 * @date 2023/9/12 13:46
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EnergyItemAggregate {
    private String buildingCode;
    private String energyItemCode;
    /**
     * 时间类型,对应EnumTimeType
     */
    private int timeType;
    /**
     * 时间戳
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date time;
    /**
     * 本时间段数值，如果是差值（用量值）,则值为本时段的使用量=下一个时间段开始时间的读数值-本时间段开始时间的读数值
     */
    private double value;
}
