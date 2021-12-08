package com.nyu.adb.driver;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author shubham.srivastava
 * netId: ss14687
 */
public class OutputWriter {

    private static OutputWriter instance = new OutputWriter();
    private static String outputFile = "src/main/resources/output/output";

    public OutputWriter() {
    }

    public static OutputWriter getInstance() {
        return instance;
    }

    public void printDebugLine(String s) {
        System.out.println(s);
    }

    public void printErrorLine(String s) {
        System.err.println(s);
    }

    public void createOutputFile(String outputDirectoryPath, String outputFileName) throws IOException {
        if (outputDirectoryPath == null || outputDirectoryPath.isEmpty()) {
            throw new IOException("Output path not provided");
        }
        if (outputFileName == null || outputFileName.isEmpty()) {
            throw new IOException("Output file name not provided");
        }
        File directory = new File(outputDirectoryPath);

        if (!directory.exists()) {
            directory.mkdirs();
        }

        File file = new File(outputDirectoryPath + "/" + outputFileName);
        outputFile = file.getPath();
        if (Files.exists(Paths.get(outputFile))) file.delete();
    }

    public void log(String message) {
        instance.printDebugLine(message);
        try (FileWriter fw = new FileWriter(outputFile, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.println(message);
        } catch (IOException ioException) {
            System.err.println("Exception while writing into file: " + outputFile + "Exception: ");
            ioException.printStackTrace();
        }
    }

}
