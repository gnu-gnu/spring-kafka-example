package com.gnu.spring.kafka.springkafkaexample.dto;

import java.io.Serializable;

public class PojoMessage implements Serializable {
    private long id;
    private String msg;
    private boolean result;

    public PojoMessage(long id, String msg, boolean result) {
        this.id = id;
        this.msg = msg;
        this.result = result;
    }

    public PojoMessage() { }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public boolean isResult() {
        return result;
    }

    public void setResult(boolean result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "PojoMessage{" +
                "id=" + id +
                ", msg='" + msg + '\'' +
                ", result=" + result +
                '}';
    }
}
