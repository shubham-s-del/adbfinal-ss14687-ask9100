package com.nyu.adb.transaction;

import com.nyu.adb.driver.DatabaseException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author shubham.srivastava
 * netId: ss14687
 */
public class Transactions {

    private Map<Integer, Transaction> transactions;

    public Transactions() {
        transactions = new HashMap<>();
    }

    protected void put(Transaction transaction) {
        transactions.put(transaction.getTransactionId(), transaction);
    }

    public Optional<Transaction> get(Integer transactionId) {
        return Optional.ofNullable(transactions.get(transactionId));
    }

    public Transaction getTransactionOrThrowException(Integer transactionId) {
        return getTransaction(transactionId).orElseThrow(() -> new DatabaseException("Transaction with id:" + transactionId + " not found"));
    }

    protected Optional<Transaction> getTransaction(Integer transactionId) {
        return get(transactionId);
    }
}
