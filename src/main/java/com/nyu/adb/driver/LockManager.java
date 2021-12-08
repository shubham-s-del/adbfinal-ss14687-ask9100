package com.nyu.adb.driver;


import com.nyu.adb.transaction.Transaction;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author shubham.srivastava
 * netId: ss14687
 */
public class LockManager {

    private Map<Integer, List<Transaction>> readLocks;
    private Map<Integer, Transaction> writeLocks;

    public LockManager() {
        readLocks = new HashMap<>();
        writeLocks = new HashMap<>();
    }

    public boolean isReadAllowedForVariable(Transaction transaction, Integer variable) {
        return !writeLocks.containsKey(variable)
                || writeLocks.get(variable).equals(transaction);
    }

    public boolean isWriteAllowedForVariable(Transaction transaction, Integer variable) {
        return (!writeLocks.containsKey(variable) || writeLocks.get(variable).equals(transaction))
                && (!readLocks.containsKey(variable) ||
                (readLocks.containsKey(variable) && readLocks.get(variable).size() == 1 && readLocks.get(variable).contains(transaction)));
    }

    public void acquireReadLock(Transaction transaction, Integer variable) {
        List<Transaction> transactions = readLocks.getOrDefault(variable, new ArrayList<>());
        transactions.add(transaction);
        readLocks.put(variable, transactions);
    }

    public void acquireWriteLock(Transaction transaction, Integer variable) {
        writeLocks.put(variable, transaction);
    }

    public Optional<Transaction> waitsForWriteTransaction(Integer variable) {
        return Optional.ofNullable(writeLocks.get(variable));
    }

    public List<Transaction> waitsForReadTransaction(Integer variable) {
        return readLocks.getOrDefault(variable, new ArrayList<>());
    }

    public void clearAllLocks() {
        readLocks.clear();
        writeLocks.clear();
    }

    public void releaseLocksForTransaction(Transaction transaction) {
        for (Map.Entry<Integer, List<Transaction>> entry : readLocks.entrySet()) {
            List<Transaction> transactions = entry.getValue();
            transactions.remove(transaction);
        }
        readLocks.entrySet().removeIf(readLock -> readLock.getValue().isEmpty());
        writeLocks.entrySet().removeIf(writeLock -> writeLock.getValue().equals(transaction));
    }

    public Set<Integer> getAllVariablesHeldByTransaction(Transaction transaction) {
        Set<Integer> variables = new LinkedHashSet<>();
        variables.addAll(readLocks.entrySet().stream().filter(entry -> entry.getValue().contains(transaction)).map(Map.Entry::getKey).collect(Collectors.toSet()));
        variables.addAll(getWriteVariablesHeldByTransaction(transaction));
        return variables;
    }


    public Map<Integer, List<Transaction>> getReadLocks() {
        return readLocks;
    }

    public Map<Integer, Transaction> getWriteLocks() {
        return writeLocks;
    }
}