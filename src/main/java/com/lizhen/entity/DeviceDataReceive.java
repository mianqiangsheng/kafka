package com.lizhen.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

@Data
public class DeviceDataReceive implements Serializable {

    private static final long serialVersionUID = -6384871967268653799L;

    private String buildingId;

    private List<MeterDto> meters;

    @Data
    public static class MeterDto implements Serializable {

        private String meterId;

        @JsonFormat(timezone = "GMT+8",pattern = "yyyy-MM-dd HH:mm:ss")
        private Date dateTime;

        private List<ParameterDto> parameters;

    }

    @Data
    public static class ParameterDto implements Serializable {

        private String parameter;

        private BigDecimal value;

    }

}
