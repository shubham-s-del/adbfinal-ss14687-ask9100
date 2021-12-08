package com.nyu.adb.transaction;

/**
 * @author shubham.srivastava
 * netId: ss14687
 */
public enum OperationType {

    BEGIN("begin"),
    END("end"),
    BEGINRO("beginRO"),
    READ("R"),
    WRITE("W"),
    DUMP("dump"),
    FAIL("fail"),
    RECOVER("recover");

    private String value;

    OperationType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
