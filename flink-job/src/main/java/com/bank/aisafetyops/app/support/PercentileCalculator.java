package com.bank.aisafetyops.app.support;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Computes deterministic exact percentiles for small-to-medium window batches.
 *
 * <p>The current AISafetyOps MVP uses nearest-rank percentiles so the business
 * meaning stays easy to explain and reproduce during investigations.
 */
public final class PercentileCalculator {
    private PercentileCalculator() {
    }

    public static Double nearestRank(List<Double> values, int percentile) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        List<Double> sortedValues = new ArrayList<>(values);
        sortedValues.sort(Comparator.naturalOrder());

        int rank = (int) Math.ceil((percentile / 100.0d) * sortedValues.size());
        int index = Math.max(0, Math.min(sortedValues.size() - 1, rank - 1));
        return sortedValues.get(index);
    }
}
