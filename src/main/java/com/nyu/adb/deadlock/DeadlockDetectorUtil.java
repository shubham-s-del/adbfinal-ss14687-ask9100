package com.nyu.adb.deadlock;

import com.nyu.adb.transaction.Transaction;
import com.nyu.adb.transaction.Transactions;

import java.util.*;

/**
 * @author shubham.srivastava
 * netId: ss14687
 */
public class DeadlockDetectorUtil {

    public static boolean detectCycle(Set<Integer> visited, Transaction currentTransaction, Transaction startingTransaction, Map<Integer, List<Integer>> waitsForGraph, Transactions transactions) {
        visited.add(currentTransaction.getTransactionId());
        List<Integer> connectedTransactions = waitsForGraph.getOrDefault(currentTransaction.getTransactionId(),
                new LinkedList<>());
        for (Integer nextTransactionId : connectedTransactions) {
            if (nextTransactionId == startingTransaction.getTransactionId())
                return true;
            if (!visited.contains(nextTransactionId)) {
                if (detectCycle(visited, transactions.get(nextTransactionId).get(), startingTransaction, waitsForGraph, transactions)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static Optional<Transaction> findYoungestDeadlockedTransaction(Map<Integer, List<Integer>> waitsForGraph, Transactions transactions) {
        Transaction youngestTransaction = null;
        for (Integer currentTransactionId : waitsForGraph.keySet()) {
            Transaction currentTransaction = transactions.getTransactionOrThrowException(currentTransactionId);
            Set<Integer> visited = new HashSet<>();
            if (detectCycle(visited, currentTransaction, currentTransaction, waitsForGraph, transactions)) {
                if (youngestTransaction == null) {
                    youngestTransaction = currentTransaction;
                } else if (currentTransaction.getTimestamp() > youngestTransaction.getTimestamp()) {
                    youngestTransaction = currentTransaction;
                }
            }
        }
        return Optional.ofNullable(youngestTransaction);
    }
}
