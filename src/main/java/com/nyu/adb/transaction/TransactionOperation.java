package com.nyu.adb.transaction;

/**
 * @author shubham.srivastava
 * netId: ss14687
 */
public class TransactionOperation {
    private OperationType operationType;
    private Integer transactionId;
    private Integer variable;
    private Integer siteId;
    private Integer writeValue;

    public TransactionOperation(OperationType operationType) {
        this.operationType = operationType;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public Integer getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(Integer transactionId) {
        this.transactionId = transactionId;
    }

    public Integer getVariable() {
        return variable;
    }

    public void setVariable(Integer variable) {
        this.variable = variable;
    }

    public Integer getSiteId() {
        return siteId;
    }

    public void setSiteId(Integer siteId) {
        this.siteId = siteId;
    }

    public Integer getWriteValue() {
        return writeValue;
    }

    public void setWriteValue(Integer writeValue) {
        this.writeValue = writeValue;
    }
}
