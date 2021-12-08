package com.nyu.adb.driver;


import java.util.TreeMap;

/**
 * @author shubham.srivastava
 * netId: ss14687
 */
public class VersionedValues {
    private Integer currentValue;
    private TreeMap<Long, Integer> versionedCommittedValues;

    public VersionedValues(Integer currentValue, long timestamp, Integer committedValue) {
        this.currentValue = currentValue;
        this.versionedCommittedValues = new TreeMap<>();
        versionedCommittedValues.put(timestamp, committedValue);
    }

    public void insertNewCommittedValue(long timestamp, Integer committedValue) {
        if (versionedCommittedValues == null) versionedCommittedValues = new TreeMap<>();
        versionedCommittedValues.put(timestamp, committedValue);
    }

    public Integer getCurrentValue() {
        return currentValue;
    }

    public void setCurrentValue(Integer currentValue) {
        this.currentValue = currentValue;
    }

    public TreeMap<Long, Integer> getVersionedCommittedValues() {
        return versionedCommittedValues;
    }
}
