package com.nyist.gmall.gmall_publisher.bean;

public class DauHour {
    private String logHour;     //小时
    private long ct;            //活跃用户数


    public String getLogHour() {
        return logHour;
    }

    public void setLogHour(String logHour) {
        this.logHour = logHour;
    }

    public long getCt() {
        return ct;
    }

    public void setCt(long ct) {
        this.ct = ct;
    }
}
