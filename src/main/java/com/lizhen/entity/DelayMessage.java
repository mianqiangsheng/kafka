package com.lizhen.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.format.annotation.DateTimeFormat;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @author ：勉强生
 * @description:
 * @date ：2021/10/11 10:49
 */
public class DelayMessage<T extends DelayMessage> implements Delayed {

    /**
     * 事件唯一ID,用于去重检查
     */
    private String eventId = UUID.randomUUID().toString();

    /**
     * 事件时间
     */
    @JsonFormat(timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    @JsonProperty(value = "eventTime")
    private Date eventTime = new Date();

    /**
     * 真实事件时间
     */
    @JsonFormat(timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    @JsonProperty(value = "actualTime")
    private Date actualTime;

    /**
     * 真实Topic
     */
    @JsonProperty(value = "actualTopic")
    private String actualTopic;

    public Date getActualTime() {
        return actualTime;
    }

    public T setActualTime(Date actualTime) {
        this.actualTime = actualTime;
        return (T) this;
    }

    public String getActualTopic() {
        return actualTopic;
    }

    public T setActualTopic(String actualTopic) {
        this.actualTopic = actualTopic;
        return (T) this;
    }

    public Date getEventTime() {
        return eventTime;
    }

    public T setEventTime(Date eventTime) {
        this.eventTime = eventTime;
        return (T) this;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    /**
     * 是否该消息需要延迟发送
     * @param message
     * @return
     */
    public Boolean isDelay(T message){
        Date eventTime = message.getEventTime(); //消息预计发送时间
        Date actualTime = message.getActualTime(); //消息真实发送时间

        return eventTime.compareTo(actualTime) > 0 ? true : false;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return this.getEventTime().getTime() - System.currentTimeMillis();
    }

    @Override
    public int compareTo(Delayed o) {
        DelayMessage delayMessage = (DelayMessage) o;
        return this.getEventTime().compareTo(delayMessage.getEventTime());
    }

    @Override
    public String toString() {
        return "DelayMessage{" +
                "eventId='" + eventId + '\'' +
                ", eventTime=" + eventTime +
                ", actualTime=" + actualTime +
                ", actualTopic='" + actualTopic + '\'' +
                '}';
    }
}
