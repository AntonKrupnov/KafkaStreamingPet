package com.lohika.akrupnov.kafka.dataobjects;

import java.io.Serializable;

public class CountTotalCost implements Serializable {
    private final String brandYear;
    private final int count;
    private final double totalCost;

    public CountTotalCost(String brandYear, int count, double totalCost) {
        this.brandYear = brandYear;
        this.count = count;
        this.totalCost = totalCost;
    }

    public double getTotalCost() {
        return totalCost;
    }

    public int getCount() {
        return count;
    }

    public String getBrandYear() {
        return brandYear;
    }
}
