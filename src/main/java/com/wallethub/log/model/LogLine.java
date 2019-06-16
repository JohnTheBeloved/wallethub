package com.wallethub.log.model;

import java.util.Date;

public class LogLine {
    private Date date;
    private String IP;
    private String request;
    private Integer status;
    private String userAgent;

    public LogLine(Date date, String IP, String request, Integer status, String userAgent) {
        this.date = date;
        this.IP = IP;
        this.request = request;
        this.status = status;
        this.userAgent = userAgent;
    }

    public Date getDate() {
        return date;
    }

    public String getIP() {
        return IP;
    }

    public String getRequest() {
        return request;
    }

    public Integer getStatus() {
        return status;
    }

    public String getUserAgent() {
        return userAgent;
    }
}
