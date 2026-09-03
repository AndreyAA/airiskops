package com.bank.airiskops.model;

import java.io.Serializable;

/** A bounded prompt-injection finding plus its normalized local embedding. */
public record EmbeddedGuardrailFinding(
        String tenantId, String agentId, String sessionId, String requestId, long eventTimeMillis,
        String guardrailName, String embeddingModelVersion, float[] embedding, String evidenceSnippet
) implements Serializable {
}
