package com.joseph.flink.bean;

public class ActivityBean {

    private String uid;
    private String aid;
    private String time;
    private Integer type;
    private String province;

    public ActivityBean() {}

    public ActivityBean(String uid, String aid, String time, Integer type, String province) {
        this.uid = uid;
        this.aid = aid;
        this.time = time;
        this.type = type;
        this.province = province;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getAid() {
        return aid;
    }

    public void setAid(String aid) {
        this.aid = aid;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public static ActivityBean of(String uid, String aid, String time, Integer type, String province) {
        return new ActivityBean(uid, aid, time, type, province);
    }

    @Override
    public String toString() {
        return "ActivityBean{" +
                "uid='" + uid + '\'' +
                ", aid='" + aid + '\'' +
                ", time='" + time + '\'' +
                ", type=" + type +
                ", province='" + province + '\'' +
                '}';
    }
}
