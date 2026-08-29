package com.bank.aisafetyops.infra.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

/**
 * Loads job configuration from YAML files into a generic key-value structure.
 *
 * <p>The loader intentionally stays infrastructure-only so application config
 * assembly can reuse it without depending on YAML-specific APIs elsewhere.
 */
public final class YamlJobConfigLoader {
    private YamlJobConfigLoader() {
    }

    public static Map<String, Object> loadRequired(Path path) {
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("Job config file not found: " + path);
        }
        return load(path);
    }

    public static Map<String, Object> loadIfExists(Path path) {
        return Files.exists(path) ? load(path) : Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> load(Path path) {
        LoadSettings settings = LoadSettings.builder().build();
        Load loader = new Load(settings);

        try (InputStream inputStream = Files.newInputStream(path)) {
            Object loaded = loader.loadFromInputStream(inputStream);
            if (loaded == null) {
                return Collections.emptyMap();
            }
            if (!(loaded instanceof Map<?, ?> rawMap)) {
                throw new IllegalArgumentException("Job config root must be a YAML map: " + path);
            }
            return new LinkedHashMap<>((Map<String, Object>) rawMap);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read job config file: " + path, e);
        }
    }
}
