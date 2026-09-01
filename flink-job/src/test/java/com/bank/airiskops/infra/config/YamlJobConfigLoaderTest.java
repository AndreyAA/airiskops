package com.bank.airiskops.infra.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class YamlJobConfigLoaderTest {
    @TempDir
    Path tempDir;

    @Test
    void loadRequiredReturnsYamlMap() throws Exception {
        Path configFile = tempDir.resolve("job.yaml");
        Files.writeString(configFile, """
                windowType: tumbling-event-time
                aggregateWindowMinutes:
                  - 1
                  - 5
                """);

        Map<String, Object> loaded = YamlJobConfigLoader.loadRequired(configFile);

        assertEquals("tumbling-event-time", loaded.get("windowType"));
        assertInstanceOf(java.util.List.class, loaded.get("aggregateWindowMinutes"));
    }

    @Test
    void loadRequiredRejectsMissingFile() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> YamlJobConfigLoader.loadRequired(tempDir.resolve("missing.yaml"))
        );

        assertTrue(error.getMessage().contains("Job config file not found"));
    }

    @Test
    void loadIfExistsReturnsEmptyMapWhenFileIsAbsent() {
        assertTrue(YamlJobConfigLoader.loadIfExists(tempDir.resolve("missing.yaml")).isEmpty());
    }

    @Test
    void loadFromStringReturnsEmptyMapForEmptyContent() {
        assertTrue(YamlJobConfigLoader.loadFromString("").isEmpty());
    }

    @Test
    void loadFromStringRejectsNonMapRoot() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> YamlJobConfigLoader.loadFromString("""
                        - one
                        - two
                        """, "list-root.yaml")
        );

        assertTrue(error.getMessage().contains("YAML root must be a map"));
    }

    @Test
    void loadRequiredWrapsIoErrors() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> YamlJobConfigLoader.loadRequired(tempDir)
        );

        assertTrue(error.getMessage().contains("Failed to read job config file"));
    }
}
