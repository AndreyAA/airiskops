package com.bank.aisafetyops.infra.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IncidentPolicyLoaderTest {
    @TempDir
    Path tempDir;

    @Test
    void loadsDefaultsAndAgentOverrides() throws Exception {
        Path policyFile = tempDir.resolve("policy.yaml");
        Files.writeString(policyFile, """
                version: "policy-v1"
                updatedBy: "unit-test"
                updatedAt: "2026-08-30T00:00:00Z"
                defaults:
                  promptInjection:
                    medium: 0.55
                    high: 0.75
                    critical: 0.90
                  toxicity:
                    medium: 0.70
                    high: 0.90
                    critical: 0.95
                  looping:
                    severity: "MEDIUM"
                  systemPromptLeakage:
                    severity: "CRITICAL"
                agents:
                  agent-risk-01:
                    promptInjection:
                      high: 0.72
                      critical: 0.88
                """);

        var policy = IncidentPolicyLoader.loadRequired(policyFile);
        var resolvedPolicy = policy.resolveForAgent("agent-risk-01");

        assertEquals("policy-v1", policy.version());
        assertEquals(0.72d, resolvedPolicy.promptInjection().high());
        assertEquals(0.88d, resolvedPolicy.promptInjection().critical());
        assertEquals(0.90d, resolvedPolicy.toxicity().high());
    }
}
