package com.nyu.adb;

import com.nyu.adb.driver.OutputWriter;
import com.nyu.adb.transaction.TransactionOperation;
import com.nyu.adb.transaction.TransactionManager;
import com.nyu.adb.util.Constants;
import com.nyu.adb.util.InputUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Database {

    public static void main(String[] args) {
        try {
            //Defaults to input resource directory if directory not provided
            if (args.length == 0) {
                args = new String[]{Constants.RESOURCE_DIR_PATH + "/input"};
            }
            final File folder = new File(args[0]);

            if (!folder.isDirectory()) {
                executeInputFile(folder);
            } else {
                for (final File fileEntry : Objects.requireNonNull(folder.listFiles())) {
                    executeInputFile(fileEntry);
                }
            }
        } catch (Exception exception) {
            OutputWriter.getInstance().printMessageToConsoleAndLogFile(exception.getMessage());
            System.err.println("Exception occurred in execution. Exception: ");
            exception.printStackTrace();
        }
    }

    public static void executeInputFile(File fileEntry) throws IOException {
        OutputWriter.getInstance().createOutputFile(Constants.RESOURCE_DIR_PATH + "/output", fileEntry.getName() + "-output");
        OutputWriter.getInstance().printMessageToConsoleAndLogFile(Constants.INPUT_FILE);
        List<TransactionOperation> transactionOperations = InputUtils.parseFile(fileEntry.getPath());
        executeOperations(transactionOperations);
        OutputWriter.getInstance().printMessageToConsoleAndLogFile(
                "\n"
        );
    }

    private static void executeOperations(List<TransactionOperation> transactionOperations) {
        TransactionManager transactionManager = new TransactionManager();
        transactionOperations.forEach(transactionManager::executeOperation);
    }
}
