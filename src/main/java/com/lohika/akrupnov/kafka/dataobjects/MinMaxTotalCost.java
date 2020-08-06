package com.lohika.akrupnov.kafka.dataobjects;

import java.io.Serializable;

public class MinMaxTotalCost implements Serializable {
    private final String brandModelYear;
    private final int count;
    private final double totalCost;
    private final double min;
    private final double max;

    public MinMaxTotalCost(String brandModelYear, int count, double totalCost, double min, double max) {
        this.brandModelYear = brandModelYear;
        this.count = count;
        this.totalCost = totalCost;
        this.min = min;
        this.max = max;
    }

    public String getBrandModelYear() {
        return brandModelYear;
    }

    public int getCount() {
        return count;
    }

    public double getTotalCost() {
        return totalCost;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }
}
