package com.nyu.adb.bookkeeper;

import com.nyu.adb.site.Site;

import java.util.List;

/**
 * @author shubham.srivastava
 * netId: ss14687
 */
public class DataInitHelper {

    public static void addVariableAtAllSites(int variable, List<Site> sites, long currentTimestamp) {
        for (Site site : sites) {
            site.addValue(variable, currentTimestamp, 10 * variable);
        }
    }

    public static void addVariableAtSite(int variable, int siteid, List<Site> sites, long currentTimestamp) {
        sites.get(siteid).addValue(variable, currentTimestamp, 10 * variable);
    }

}
