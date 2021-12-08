package com.nyu.adb.driver;

import com.nyu.adb.transaction.Transaction;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author shubham.srivastava
 * netId: ss14687
 */
public interface LockManagerInterface {

    public boolean isReadAllowedForVariable(Transaction transaction, Integer variable);

    public boolean isWriteAllowedForVariable(Transaction transaction, Integer variable);

    public void acquireReadLock(Transaction transaction, Integer variable);

    public void acquireWriteLock(Transaction transaction, Integer variable);

    public Optional<Transaction> waitsForWriteTransaction(Integer variable);

    public List<Transaction> waitsForReadTransaction(Integer variable);

    public void clearAllLocks();

    public void releaseLocksForTransaction(Transaction transaction);

    public Set<Integer> getAllVariablesHeldByTransaction(Transaction transaction);

    public Set<Integer> getWriteVariablesHeldByTransaction(Transaction transaction);

    public Map<Integer, List<Transaction>> getReadLocks();

    public Map<Integer, Transaction> getWriteLocks();

}
