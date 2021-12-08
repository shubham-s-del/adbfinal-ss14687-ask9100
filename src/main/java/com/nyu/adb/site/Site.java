package com.nyu.adb.site;

import com.nyu.adb.driver.*;
import com.nyu.adb.transaction.Transaction;
import com.nyu.adb.transaction.TransactionStatus;
import com.nyu.adb.util.FileUtils;

import java.util.*;

/**
 * @author shubham.srivastava
 * netId: ss14687
 */
public class Site {

    private TreeMap<Integer, VersionedValues> data;
    private Map<Integer, Boolean> availableForRead;

    private Integer siteId;
    private boolean failed;
    private LockManager lockManager;

    public Site(int id) {
        data = new TreeMap<>();
        availableForRead = new HashMap<>();
        this.siteId = id;
        failed = false;
        lockManager = new LockManager();

    }

    public void addValue(Integer variable, long timestamp, Integer value) {
        insertData(variable, timestamp, value);
    }

    public void failSite() {
        failed = true;
        Set<String> abortTransactions = new HashSet<>();
        for (Map.Entry<Integer, List<Transaction>> entry : lockManager.getReadLocks().entrySet()) {
            Integer key = entry.getKey();
            List<Transaction> transactions = entry.getValue();
            for (Transaction transaction : transactions) {
                moveValueBackToCommittedValueAtTime(key, transaction.getTimestamp(), siteId);
                transaction.setTransactionStatus(TransactionStatus.ABORT);
                abortTransactions.add("T" + transaction.getTransactionId());
            }
        }
        lockManager.getWriteLocks().forEach((variable, transaction) -> {
                    moveValueBackToCommittedValueAtTime(variable, transaction.getTimestamp(), siteId);
                    transaction.setTransactionStatus(TransactionStatus.ABORT);
                    abortTransactions.add("T" + transaction.getTransactionId());
                }
        );
        lockManager.clearAllLocks();
        for (Integer variable : availableForRead.keySet()) {
            availableForRead.put(variable, false);
        }
        FileUtils.log("Site" + siteId + " failed! All locks released!");
        if (!abortTransactions.isEmpty()) FileUtils.log(abortTransactions.toString() + " will abort when they end");
    }

    public void recoverSite() {
        failed = false;
        for (Integer variable : availableForRead.keySet()) {
            if (variable % 2 == 1) availableForRead.put(variable, true);
        }
        FileUtils.log("Site" + siteId + " recovered!");
    }

    public Integer readValue(Transaction transaction, Integer variable) {
        // Multi-version read consistency orchestrator
        if (!transaction.isReadOnly()) {
            lockManager.acquireReadLock(transaction, variable);
        }

        if (!data.containsKey(variable)) {
            throw new DatabaseException("Invalid variable accessed in readValue!");
        }
        VersionedValues versionedValues = data.get(variable);
        return transaction.isReadOnly() ? versionedValues.getVersionedCommittedValues().floorEntry(transaction.getTimestamp()).getValue() : versionedValues.getCurrentValue();
    }

    public void writeValue(Transaction transaction, Integer variable, Integer writeValue) {
        lockManager.acquireWriteLock(transaction, variable);

        if (!data.containsKey(variable)) {
            throw new DatabaseException("Invalid variable accessed in readValue!");
        }
        VersionedValues values = data.get(variable);
        values.setCurrentValue(writeValue);
    }

    public void commitValues(Transaction transaction, long currentTimestamp) {
        Set<Integer> writeVariables = lockManager.getWriteVariablesHeldByTransaction(transaction);
        for (Integer writeVariable : writeVariables) {
            commitValueForVariable(writeVariable, currentTimestamp, siteId);
        }
    }

    public void moveCurrentValuesBackToCommitted(Transaction transaction, long currentTimestamp) {
        Set<Integer> writeVariables = lockManager.getWriteVariablesHeldByTransaction(transaction);
        for (Integer writeVariable : writeVariables) {
            moveValueBackToCommittedValueAtTime(writeVariable, currentTimestamp, siteId);
        }
    }

    public boolean isReadAllowed(Integer variable) {
        return !failed && availableForRead.getOrDefault(variable, false);
    }

    public Integer getSiteId() {
        return siteId;
    }

    public boolean checkStatus() {
        return !failed;
    }

    public LockManager getLockManager() {
        return lockManager;
    }


    protected void insertData(Integer variable, long timestamp, Integer value) {
        data.put(variable, new VersionedValues(value, timestamp, value));
        availableForRead.put(variable, true);
    }

    protected void commitValueForVariable(Integer variable, long currentTimestamp, Integer siteId) {
        if (!data.containsKey(variable)) {
            throw new DatabaseException("Invalid variable accessed in moveValueBackToCommittedValueAtTime!");
        }
        VersionedValues values = data.get(variable);
        values.insertNewCommittedValue(currentTimestamp, values.getCurrentValue());
        availableForRead.put(variable, true);
        FileUtils.log(currentTimestamp + ": x" + variable + "=" + values.getCurrentValue() + " at site" + siteId);

    }

    protected void moveValueBackToCommittedValueAtTime(Integer variable, long timestamp, Integer siteId) {
        if (!data.containsKey(variable)) {
            throw new DatabaseException("Invalid variable accessed in moveValueBackToCommittedValueAtTime!");
        }
        VersionedValues values = data.get(variable);
        Integer committedValue = values.getVersionedCommittedValues().floorEntry(timestamp).getValue();
        FileUtils.log("Rollback: x" + variable + "=" + values.getCurrentValue() + " to " + committedValue + " at site" + siteId);
        values.setCurrentValue(committedValue);
    }

    public Map<Integer, VersionedValues> getSiteState() {
        return data;
    }
}