package com.nyu.adb.transaction;

/**
 * @author shubham.srivastava
 * netId: ss14687
 */
public class Transaction {
    private Integer transactionId;
    private Long timestamp;
    private TransactionOperation currentTransactionOperation;
    private boolean isReadOnly;
    private TransactionStatus transactionStatus;

    public Transaction(Integer transactionId, Long timestamp, TransactionOperation currentTransactionOperation, boolean isReadOnly, TransactionStatus transactionStatus) {
        this.transactionId = transactionId;
        this.timestamp = timestamp;
        this.currentTransactionOperation = currentTransactionOperation;
        this.isReadOnly = isReadOnly;
        this.transactionStatus = transactionStatus;
    }

    public Integer getTransactionId() {
        return transactionId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public TransactionOperation getCurrentOperation() {
        return currentTransactionOperation;
    }

    public void setCurrentOperation(TransactionOperation currentTransactionOperation) {
        this.currentTransactionOperation = currentTransactionOperation;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public TransactionStatus getTransactionStatus() {
        return transactionStatus;
    }

    public void setTransactionStatus(TransactionStatus transactionStatus) {
        this.transactionStatus = transactionStatus;
    }
}
