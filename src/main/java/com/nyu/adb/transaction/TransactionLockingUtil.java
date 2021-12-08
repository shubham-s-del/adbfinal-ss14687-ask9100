package com.nyu.adb.transaction;

import com.nyu.adb.site.Site;

import java.util.List;
import java.util.Optional;

/**
 * @author shubham.srivastava
 * netId: ss14687
 */
public class TransactionLockingUtil {

    public static void transactionWaitsForWrite(Transaction transaction, Site site, Integer variable,
                                                TransactionManager transactionManager) {
        Optional<Transaction> waitsFor = site.getLockManager().waitsForWriteTransaction(variable);
        waitsFor.ifPresent(waitsForTransaction -> transactionManager.addToWaitingTransactions(waitsForTransaction,
                transaction, site, variable));
    }

    public static void transactionWaitsForRead(Transaction transaction, Site site, Integer variable,
                                               TransactionManager transactionManager) {
        List<Transaction> waitsFor = site.getLockManager().waitsForReadTransaction(variable);
        for (Transaction waitsForTransaction : waitsFor) {
            transactionManager.addToWaitingTransactions(waitsForTransaction, transaction, site, variable);
        }
    }

}
