package com.bank.airiskops.app.config;

import java.nio.file.Path;

/**
 * Bootstrap policy configuration for the local incident layer.
 *
 * <p>Increment 3.2a loads one YAML file during job startup and uses it as the
 * authoritative source for incident thresholds until the next restart.
 */
public record PolicyConfig(
        boolean enabled,
        Path bootstrapFile,
        boolean requireBootstrapPolicy,
        String updatesTopic,
        boolean rejectOlderVersions
) {
}
