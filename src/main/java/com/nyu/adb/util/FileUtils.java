package com.nyu.adb.util;

import com.nyu.adb.driver.OutputWriter;
import com.nyu.adb.transaction.TransactionOperation;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static com.nyu.adb.transaction.OperationType.*;

public class FileUtils {

    public static List<TransactionOperation> parseFile(String inputFile) throws IOException {
        if (inputFile == null || inputFile.isEmpty()) {
            throw new IOException("Input path not provided");
        }

        List<String> allLines = Files.readAllLines(Paths.get(inputFile));
        return allLines.stream().filter(line -> !line.isEmpty() && !line.startsWith(Constants.COMMENT))
                .map(FileUtils::getOperation).collect(Collectors.toList());
    }

    private static TransactionOperation getOperation(String operationString) {
        String[] components = getTrimmedComponents(operationString.trim().split("[(),]"));
        String operationType = components[0];
        TransactionOperation transactionOperation;
        if (BEGIN.getValue().equalsIgnoreCase(operationType) && components.length == 2) {
            transactionOperation = new TransactionOperation(BEGIN);
            transactionOperation.setTransactionId(getId(components[1]));
        } else if (END.getValue().equalsIgnoreCase(operationType) && components.length == 2) {
            transactionOperation = new TransactionOperation(END);
            transactionOperation.setTransactionId(getId(components[1]));
        } else if (BEGINRO.getValue().equalsIgnoreCase(operationType) && components.length == 2) {
            transactionOperation = new TransactionOperation(BEGINRO);
            transactionOperation.setTransactionId(getId(components[1]));
        } else if (DUMP.getValue().equalsIgnoreCase(operationType) && components.length == 1) {
            transactionOperation = new TransactionOperation(DUMP);
        } else if (RECOVER.getValue().equalsIgnoreCase(operationType) && components.length == 2) {
            transactionOperation = new TransactionOperation(RECOVER);
            transactionOperation.setSiteId(getIntegerFromString(components[1]));
        } else if (FAIL.getValue().equalsIgnoreCase(operationType) && components.length == 2) {
            transactionOperation = new TransactionOperation(FAIL);
            transactionOperation.setSiteId(getIntegerFromString(components[1]));
        } else if (READ.getValue().equalsIgnoreCase(operationType) && components.length == 3) {
            transactionOperation = new TransactionOperation(READ);
            transactionOperation.setTransactionId(getId(components[1]));
            transactionOperation.setVariable(getId(components[2]));
        } else if (WRITE.getValue().equalsIgnoreCase(operationType) && components.length == 4) {
            transactionOperation = new TransactionOperation(WRITE);
            transactionOperation.setTransactionId(getId(components[1]));
            transactionOperation.setVariable(getId(components[2]));
            transactionOperation.setWriteValue(getIntegerFromString(components[3]));
        } else {
            OutputWriter.getInstance().log("Unsupported Operation: " + operationType);
            throw new UnsupportedOperationException("This operation is not supported");
        }
        return transactionOperation;
    }

    private static String[] getTrimmedComponents(String[] components) {
        for (int i = 0; i < components.length; i++) {
            components[i] = components[i].trim();
        }
        return components;
    }

    private static Integer getId(String transactionString) {
        return getIntegerFromString(transactionString.substring(1));
    }

    private static Integer getIntegerFromString(String value) {
        return Integer.valueOf(value);
    }

}
