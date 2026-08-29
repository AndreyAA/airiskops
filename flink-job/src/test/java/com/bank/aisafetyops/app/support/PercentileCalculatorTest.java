package com.bank.aisafetyops.app.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.junit.jupiter.api.Test;

class PercentileCalculatorTest {
    @Test
    void returnsNullForEmptyInput() {
        assertNull(PercentileCalculator.nearestRank(List.of(), 50));
    }

    @Test
    void returnsOnlyValueForSingleElementInput() {
        assertEquals(0.91d, PercentileCalculator.nearestRank(List.of(0.91d), 50));
        assertEquals(0.91d, PercentileCalculator.nearestRank(List.of(0.91d), 95));
    }

    @Test
    void computesNearestRankPercentilesFromUnsortedValues() {
        List<Double> values = List.of(0.91d, 0.73d, 0.85d, 0.66d);

        assertEquals(0.73d, PercentileCalculator.nearestRank(values, 50));
        assertEquals(0.91d, PercentileCalculator.nearestRank(values, 95));
    }
}
