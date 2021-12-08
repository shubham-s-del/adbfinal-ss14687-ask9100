package com.nyu.adb.bookkeeper;

/**
 * @author shubham.srivastava
 * netId: ss14687
 */
public class Bookkeeper {

    public static boolean isOddVariable(Integer variable) {
        return variable % 2 == 1;
    }

    public static Integer getSiteIdForOddVariable(Integer variable) {
        return 1 + (variable % 10);
    }
}
