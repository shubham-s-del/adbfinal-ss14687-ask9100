package com.nyu.adb.transaction;

import com.nyu.adb.deadlock.DeadlockDetectorUtil;
import com.nyu.adb.driver.DatabaseException;
import com.nyu.adb.driver.OutputWriter;
import com.nyu.adb.driver.VersionedValues;
import com.nyu.adb.site.Site;

import java.util.*;

import static com.nyu.adb.bookkeeper.Bookkeeper.getSiteIdForOddVariable;
import static com.nyu.adb.bookkeeper.Bookkeeper.isOddVariable;
import static com.nyu.adb.bookkeeper.DataInitHelper.addVariableAtAllSites;
import static com.nyu.adb.bookkeeper.DataInitHelper.addVariableAtSite;
import static com.nyu.adb.deadlock.DeadlockDetectorUtil.detectCycle;
import static com.nyu.adb.transaction.OperationType.READ;
import static com.nyu.adb.transaction.OperationType.WRITE;
import static com.nyu.adb.transaction.TransactionStatus.*;

/**
 * @author shubham.srivastava
 * netId: ss14687
 */
public class TransactionManager {

    private long currentTimestamp;
    private Transactions transactions;
    private List<Site> sites;
    //transactionId -> List<transactionIds>
    private Map<Integer, List<Integer>> waitsForGraph;
    //transactionId -> List<Site>
    private Map<Integer, List<Site>> waitingSites;
    //variable -> List<Operation>
    private Map<Integer, LinkedList<TransactionOperation>> waitingOperations;

    private static final Integer SITES_START = 1;
    private static final Integer SITES_END = 10;

    public TransactionManager() {
        currentTimestamp = 0;
        transactions = new Transactions();
        sites = new ArrayList<>();
        waitsForGraph = new HashMap<>();
        waitingSites = new HashMap<>();
        waitingOperations = new HashMap<>();
        initSitesAndVariables();
    }

    private void initSitesAndVariables() {
        //Create 11 new sites, first one being the dummy site
        for (int i = 0; i <= SITES_END; i++) {
            sites.add(new Site(i));
        }

        //create new variables and place them at the correct site
        for (int variable = 1; variable <= 20; variable++) {
            if (isOddVariable(variable)) {
                addVariableAtSite(variable, getSiteIdForOddVariable(variable), sites, currentTimestamp);
            } else {
                addVariableAtAllSites(variable, sites, currentTimestamp);
            }
        }
    }

    

    public void executeOperation(TransactionOperation transactionOperation) {
        currentTimestamp++;
        switch (transactionOperation.getOperationType()) {
            case BEGIN:
                beginTransaction(transactionOperation, false);
                break;
            case BEGINRO:
                beginTransaction(transactionOperation, true);
                break;
            case END:
                endTransaction(transactionOperation.getTransactionId());
                break;
            case READ:
                read(transactionOperation, true);
                break;
            case WRITE:
                write(transactionOperation, true);
                break;
            case DUMP:
                dump();
                break;
            case FAIL:
                sites.get(transactionOperation.getSiteId()).failSite();
                break;
            case RECOVER:
                Site site = sites.get(transactionOperation.getSiteId());
                site.recoverSite();
                wakeupTransactionsWaitingForSite(site);
                break;
        }
    }

    protected void beginTransaction(TransactionOperation transactionOperation, boolean isReadOnly) {
        if (transactions.getTransaction(transactionOperation.getTransactionId()).isPresent()) {
            throw new DatabaseException("Transaction with id: " + transactionOperation.getTransactionId() + " already exists!");
        }
        OutputWriter.getInstance().printMessageToConsoleAndLogFile("Begin " + (isReadOnly ? "RO " : "") + "T" + transactionOperation.getTransactionId() + " at time: " + currentTimestamp);
        transactions.put(new Transaction(transactionOperation.getTransactionId(), currentTimestamp, transactionOperation, isReadOnly, ACTIVE));
    }

    protected void endTransaction(Integer transactionId) {
        Transaction transaction = transactions.getTransactionOrThrowException(transactionId);
        Set<Integer> variablesHeldByTransaction = new LinkedHashSet<>();

        if (COMMITTED.equals(transaction.getTransactionStatus()) || ABORTED.equals(transaction.getTransactionStatus())) {
            OutputWriter.getInstance().printMessageToConsoleAndLogFile(String.format("T%d is already %s", transactionId, transaction.getTransactionStatus().name().toLowerCase()));
            return;
        }

        transaction.setTransactionStatus(ACTIVE.equals(transaction.getTransactionStatus()) ? COMMITTED : ABORTED);
        OutputWriter.getInstance().printMessageToConsoleAndLogFile("T" + transactionId + " " + transaction.getTransactionStatus().name().toLowerCase());
        for (Integer siteId = SITES_START; siteId <= SITES_END; siteId++) {
            Site site = sites.get(siteId);
            variablesHeldByTransaction.addAll(site.getLockManager().getAllVariablesHeldByTransaction(transaction));
            if (COMMITTED.equals(transaction.getTransactionStatus())) {
                site.commitValues(transaction, currentTimestamp);
            } else {
                site.rollBackCurrentValueToLastCommitted(transaction, transaction.getTimestamp());
            }
            site.getLockManager().releaseLocksForTransaction(transaction);
        }
        removeFromWaitingTransactions(transaction);
        removeFromWaitingOperations(transaction);
        long toSkip = SITES_START;
        for (Site site : sites) {
            if (toSkip > 0) {
                toSkip--;
                continue;
            }
            wakeupTransactionsWaitingForSite(site);
        }
        resumeTransactionsWaitingForVariables(variablesHeldByTransaction);
    }

    private void removeFromWaitingTransactions(Transaction transaction) {
        for (Map.Entry<Integer, List<Integer>> e : waitsForGraph.entrySet()) {
            List<Integer> waiting = e.getValue();
            waiting.remove(transaction);
        }
        waitsForGraph.entrySet().removeIf(entry -> {
            return transaction.getTransactionId().equals(entry.getKey()) || entry.getValue().isEmpty();
        });
    }

    private void removeFromWaitingOperations(Transaction transaction) {
        waitingOperations.forEach((variable, operations) -> operations.removeIf(operation -> operation.getTransactionId().equals(transaction.getTransactionId())));
        waitingOperations.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    }



    protected void read(TransactionOperation transactionOperation, boolean isNewRead) {
        Transaction transaction = transactions.getTransactionOrThrowException(transactionOperation.getTransactionId());
        if (COMMITTED.equals(transaction.getTransactionStatus()) || ABORTED.equals(transaction.getTransactionStatus())) {
            OutputWriter.getInstance().printMessageToConsoleAndLogFile("Finished T" + transaction.getTransactionId() + " trying to read. Ignoring.");
            return;
        }
        Integer variable = transactionOperation.getVariable();
        transaction.setCurrentOperation(transactionOperation);
        if (transaction.isReadOnly()) {
            readForReadOnlyTransaction(transaction, variable);
        } else {
            read(transaction, variable, transactionOperation, isNewRead);
        }
    }

    protected void readForReadOnlyTransaction(Transaction transaction, Integer variable) {
        if (isOddVariable(variable)) {
            readOnlyTransactionOddVariable(transaction, variable);
        } else {
            readOnlyTransactionEvenVariable(transaction, variable);
        }
    }

    private void read(Transaction transaction, Integer variable, TransactionOperation transactionOperation, boolean isNewRead) {
        if (isOddVariable(variable)) {
            readOddVariable(transaction, variable, transactionOperation, isNewRead);
        } else {
            readEvenVariable(transaction, variable, transactionOperation, isNewRead);
        }
    }

    private void readOnlyTransactionOddVariable(Transaction transaction, Integer variable) {
        Site site = sites.get(getSiteIdForOddVariable(variable));
        if (site.isReadAllowed(variable)) {
            readVariable(transaction, variable, site);
        } else {
            addWaitingSite(transaction, site);
        }
    }

    private void readOnlyTransactionEvenVariable(Transaction transaction, Integer variable) {
        for (int i = SITES_START; i <= SITES_END; i++) {
            Site site = sites.get(i);
            if (site.isReadAllowed(variable)) {
                readVariable(transaction, variable, site);
                return;
            }
        }
        sites.stream().skip(SITES_START).forEach(site -> addWaitingSite(transaction, site));
    }

    private boolean canRead(Transaction transaction, Integer variable) {
        return isOddVariable(variable) ? canReadOddVariable(transaction, variable) : canReadEvenVariable(transaction, variable);
    }

    private boolean canReadOddVariable(Transaction transaction, Integer variable) {
        Site site = sites.get(getSiteIdForOddVariable(variable));
        return site.isReadAllowed(variable) && site.getLockManager().isReadAllowedForVariable(transaction, variable);
    }

    private boolean canReadEvenVariable(Transaction transaction, Integer variable) {
        for (int i = SITES_START; i <= SITES_END; i++) {
            Site site = sites.get(i);
            if (site.isReadAllowed(variable) && site.getLockManager().isReadAllowedForVariable(transaction, variable)) {
                return true;
            }
        }
        return false;
    }

    private void readOddVariable(Transaction transaction, Integer variable, TransactionOperation transactionOperation, boolean isNewRead) {
        Site site = sites.get(getSiteIdForOddVariable(variable));
        if (canReadOddVariable(transaction, variable)) {
            if (checkWriteStarvation(transaction, variable, transactionOperation, isNewRead)) {
                readVariable(transaction, variable, site);
            }
        } else {
            block(transaction, site, variable, transactionOperation);
        }
    }

    private void readEvenVariable(Transaction transaction, Integer variable, TransactionOperation transactionOperation, boolean isNewRead) {
        if (!canReadEvenVariable(transaction, variable)) {
            sites.stream().skip(SITES_START).forEach(site -> block(transaction, site, variable, transactionOperation));
            return;
        }
        for (int i = SITES_START; i <= SITES_END; i++) {
            Site site = sites.get(i);
            if (site.isReadAllowed(variable) && site.getLockManager().isReadAllowedForVariable(transaction, variable)) {
                if (checkWriteStarvation(transaction, variable, transactionOperation, isNewRead)) {
                    readVariable(transaction, variable, site);
                }
                return;
            }
        }
    }

    protected boolean checkWriteStarvation(Transaction transaction, Integer variable, TransactionOperation transactionOperation, boolean isNewReadOrWrite) {
        if (!isNewReadOrWrite || !hasWriteWaiting(transactionOperation, variable)) return true;
        addWaitsForWaitingWrite(transaction, transactionOperation, variable);
        addWaitingOperation(variable, transactionOperation);
        detectDeadlock();
        return false;
    }

    private boolean hasWriteWaiting(TransactionOperation transactionOperation, Integer variable) {
        LinkedList<TransactionOperation> operationsInLine = waitingOperations.getOrDefault(variable, new LinkedList<>());
        return operationsInLine.stream().anyMatch(waitingOperation -> !waitingOperation.equals(transactionOperation) && WRITE.equals(waitingOperation.getOperationType()));
    }

    private void addWaitsForWaitingWrite(Transaction transaction, TransactionOperation transactionOperation, Integer variable) {
        LinkedList<TransactionOperation> operationsInLine = waitingOperations.getOrDefault(variable, new LinkedList<>());
        Optional<TransactionOperation> waitsForOperation = operationsInLine.stream().filter(waitingOperation -> !waitingOperation.equals(transactionOperation) && WRITE.equals(waitingOperation.getOperationType())).findFirst();
        waitsForOperation.ifPresent(waitsFor -> addToWaitingTransactions(transactions.getTransactionOrThrowException(waitsFor.getTransactionId()), transaction, null, variable));
    }

    private void readVariable(Transaction transaction, Integer variable, Site site) {
        Integer value = site.executeRead(transaction, variable);
        OutputWriter.getInstance().printMessageToConsoleAndLogFile("T" + transaction.getTransactionId() + ": read(x" + variable + ")=" + value + " at site" + site.getSiteId());
    }

    protected void write(TransactionOperation transactionOperation, boolean isNewWrite) {
        Transaction transaction = transactions.getTransactionOrThrowException(transactionOperation.getTransactionId());
        if (COMMITTED.equals(transaction.getTransactionStatus()) || ABORTED.equals(transaction.getTransactionStatus())) {
            OutputWriter.getInstance().printMessageToConsoleAndLogFile("Finished T " + transaction.getTransactionId() + " trying to write. Ignoring.");
            return;
        }
        Integer variable = transactionOperation.getVariable();
        Integer writeValue = transactionOperation.getWriteValue();

        transaction.setCurrentOperation(transactionOperation);
        if (isOddVariable(variable))
            writeOddVariable(transaction, variable, writeValue, transactionOperation, isNewWrite);
        else writeEvenVariable(transaction, variable, writeValue, transactionOperation, isNewWrite);
    }

    private void writeOddVariable(Transaction transaction, Integer variable, Integer writeValue, TransactionOperation transactionOperation, boolean isNewWrite) {
        Site site = sites.get(getSiteIdForOddVariable(variable));
        if (canWriteOddVariable(transaction, variable)) {
            if (checkWriteStarvation(transaction, variable, transactionOperation, isNewWrite)) {
                writeVariable(transaction, variable, writeValue, site);
            }
        } else {
            block(transaction, site, variable, transactionOperation);
        }
    }

    private void writeEvenVariable(Transaction transaction, Integer variable, Integer writeValue, TransactionOperation transactionOperation, boolean isNewWrite) {
        if (canWriteEvenVariable(transaction, variable)) {
            if (checkWriteStarvation(transaction, variable, transactionOperation, isNewWrite)) {
                sites.stream().skip(SITES_START).filter(Site::checkStatus).forEach(site -> writeVariable(transaction, variable, writeValue, site));
            }
        } else {
            if (allSitesDown()) {
                sites.stream().skip(SITES_START).forEach(site -> addWaitingSite(transaction, site));
            } else {
                for (int i = SITES_START; i <= SITES_END; i++) {
                    Site site = sites.get(i);
                    if (site.checkStatus() && !site.getLockManager().isWriteAllowedForVariable(transaction, variable)) {
                        block(transaction, site, variable, transactionOperation);
                        return;
                    }
                }
            }
        }
    }

    private boolean canWrite(Transaction transaction, Integer variable) {
        return isOddVariable(variable) ? canWriteOddVariable(transaction, variable) : canWriteEvenVariable(transaction, variable);
    }

    private boolean canWriteOddVariable(Transaction transaction, Integer variable) {
        Site site = sites.get(getSiteIdForOddVariable(variable));
        return site.checkStatus() && site.getLockManager().isWriteAllowedForVariable(transaction, variable);
    }

    private boolean canWriteEvenVariable(Transaction transaction, Integer variable) {
        if (allSitesDown()) return false;
        for (int i = SITES_START; i <= SITES_END; i++) {
            Site site = sites.get(i);
            if (site.checkStatus() && !site.getLockManager().isWriteAllowedForVariable(transaction, variable)) {
                return false;
            }
        }
        return true;
    }

    private boolean allSitesDown() {
        return sites.stream().skip(SITES_START).noneMatch(Site::checkStatus);
    }

    private void writeVariable(Transaction transaction, Integer variable, Integer writeValue, Site site) {
        site.executeWrite(transaction, variable, writeValue);
        OutputWriter.getInstance().printMessageToConsoleAndLogFile("T" + transaction.getTransactionId() + ": write(x" + variable + ")=" + writeValue + " at site" + site.getSiteId());
    }

    protected void block(Transaction transaction, Site site, Integer variable, TransactionOperation transactionOperation) {
        if (!site.checkStatus() || (transactionOperation.getOperationType().equals(READ) && !site.isReadAllowed(variable))) {
            addWaitingSite(transaction, site);
            return;
        }
        if (transactionOperation.getOperationType().equals(READ)) {
            blockRead(transaction, site, variable, transactionOperation);
        } else if (transactionOperation.getOperationType().equals(WRITE)) {
            blockWrite(transaction, site, variable, transactionOperation);
        }
        detectDeadlock();
    }

    private void detectDeadlock() {
        Optional<Transaction> abortTransaction = DeadlockDetectorUtil.findYoungestDeadlockedTransaction(waitsForGraph, transactions);
        while (abortTransaction.isPresent()) {
            Transaction transaction = abortTransaction.get();
            transaction.setTransactionStatus(ABORT);
            endTransaction(transaction.getTransactionId());
            abortTransaction = DeadlockDetectorUtil.findYoungestDeadlockedTransaction(waitsForGraph, transactions);
        }
    }


    protected void addWaitingSite(Transaction transaction, Site site) {
        if (Objects.nonNull(site)) {
            List<Site> waitingSitesForTransaction = waitingSites.getOrDefault(transaction.getTransactionId(), new ArrayList<>());
            waitingSitesForTransaction.add(site);
            waitingSites.put(transaction.getTransactionId(), waitingSitesForTransaction);
            OutputWriter.getInstance().printMessageToConsoleAndLogFile("T" + transaction.getTransactionId() + " waits for Site" + site.getSiteId());
        }
    }

    private void blockRead(Transaction transaction, Site site, Integer variable, TransactionOperation transactionOperation) {
        addWaitsForWriteLock(transaction, site, variable);
        addWaitingOperation(variable, transactionOperation);
    }

    private void blockWrite(Transaction transaction, Site site, Integer variable, TransactionOperation transactionOperation) {
        addWaitsForWriteLock(transaction, site, variable);
        addWaitsForReadLock(transaction, site, variable);
        addWaitingOperation(variable, transactionOperation);
    }

    private void addWaitsForWriteLock(Transaction transaction, Site site, Integer variable) {
        Optional<Transaction> waitsFor = site.getLockManager().waitsForWriteTransaction(variable);
        waitsFor.ifPresent(waitsForTransaction -> addToWaitingTransactions(waitsForTransaction, transaction, site, variable));
    }

    private void addWaitsForReadLock(Transaction transaction, Site site, Integer variable) {
        List<Transaction> waitsFor = site.getLockManager().waitsForReadTransaction(variable);
        waitsFor.forEach(waitsForTransaction -> addToWaitingTransactions(waitsForTransaction, transaction, site, variable));
    }

    private void addToWaitingTransactions(Transaction waitsFor, Transaction transaction, Site site, Integer
            variable) {
        if (waitsFor.getTransactionId().equals(transaction.getTransactionId())) return;
        List<Integer> waitingList = waitsForGraph.getOrDefault(waitsFor.getTransactionId(), new ArrayList<>());
        if (!waitingList.contains(transaction.getTransactionId())) {
            waitingList.add(transaction.getTransactionId());
            waitsForGraph.put(waitsFor.getTransactionId(), waitingList);
            OutputWriter.getInstance().printMessageToConsoleAndLogFile("T" + transaction.getTransactionId() + " waits for T" + waitsFor.getTransactionId() + " for x" + variable
                    + (Objects.isNull(site) ? " because of write waiting" : " at site" + site.getSiteId()));
        }
    }

    private void addWaitingOperation(Integer variable, TransactionOperation transactionOperation) {
        if (Objects.nonNull(transactionOperation)) {
            LinkedList<TransactionOperation> transactionOperations = waitingOperations.getOrDefault(variable, new LinkedList<>());
            transactionOperations.add(transactionOperation);
            waitingOperations.put(variable, transactionOperations);
        }
    }

    protected void wakeupTransactionsWaitingForSite(Site site) {
        waitingSites.forEach((transactionId, sites) -> {
            Transaction transaction = transactions.getTransactionOrThrowException(transactionId);
            TransactionOperation currentTransactionOperation = transaction.getCurrentOperation();
            if (READ.equals(currentTransactionOperation.getOperationType())) {
                if (sites.contains(site) && site.isReadAllowed(currentTransactionOperation.getVariable())) {
                    OutputWriter.getInstance().printMessageToConsoleAndLogFile("T" + transactionId + " woken up since site" + site.getSiteId() + " is up!");
                    read(currentTransactionOperation, true);
                    sites.clear();
                }
            } else if (WRITE.equals(currentTransactionOperation.getOperationType())) {
                if (sites.contains(site)) {
                    OutputWriter.getInstance().printMessageToConsoleAndLogFile("T" + transactionId + " woken up since site" + site.getSiteId() + " is up!");
                    write(currentTransactionOperation, true);
                    sites.clear();
                }
            } else throw new DatabaseException("T" + transactionId + " should not be waiting for sites!");
        });
        waitingSites.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    }

    protected void resumeTransactionsWaitingForVariables(Set<Integer> variables) {
        for (Integer variable : variables) {
            if (waitingOperations.containsKey(variable)) {
                LinkedList<TransactionOperation> transactionOperations = waitingOperations.get(variable);
                boolean canReadOrWrite = true;
                while (!transactionOperations.isEmpty() && canReadOrWrite) {
                    TransactionOperation transactionOperation = transactionOperations.getFirst();
                    Transaction transaction = transactions.getTransactionOrThrowException(transactionOperation.getTransactionId());
                    if (READ.equals(transactionOperation.getOperationType())) {
                        if (canRead(transaction, variable)) {
                            read(transactionOperation, false);
                            transactionOperations.removeFirst();
                        } else canReadOrWrite = false;
                    } else if (WRITE.equals(transactionOperation.getOperationType())) {
                        if (canWrite(transaction, variable)) {
                            write(transactionOperation, false);
                            transactionOperations.removeFirst();
                        } else canReadOrWrite = false;
                    } else {
                        throw new DatabaseException(transactionOperation.getOperationType() + " for T" + transactionOperation.getTransactionId() + " should not be in the waiting operations!");
                    }
                }
                waitingOperations.entrySet().removeIf(entry -> entry.getValue().isEmpty());
            }
        }
    }

    protected void dump() {
        OutputWriter.getInstance().printMessageToConsoleAndLogFile("Dump--------------------------------------->");
        long toSkip = SITES_START;
        for (Site site : sites) {
            if (toSkip > 0) {
                toSkip--;
                continue;
            }
            Map<Integer, VersionedValues> dataTreeMap = site.getSiteState();
            StringBuilder sb = new StringBuilder(String.format("site %d -", site.getSiteId()));
            dataTreeMap.keySet().forEach(variableId -> {
                VersionedValues versionedValues = dataTreeMap.get(variableId);
                sb.append(String.format(" x%d: %d,", variableId, versionedValues.getVersionedCommittedValues().lastEntry().getValue()));
            });
            // Removing last comma
            sb.setLength(sb.length() - 1);
            OutputWriter.getInstance().printMessageToConsoleAndLogFile(sb.toString());
        }
        OutputWriter.getInstance().printMessageToConsoleAndLogFile("\n");
    }


}