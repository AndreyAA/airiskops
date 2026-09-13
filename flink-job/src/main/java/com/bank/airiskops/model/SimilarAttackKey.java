package com.bank.airiskops.model;

import java.io.Serializable;

/** State key which deliberately excludes session so campaigns can span sessions. */
public record SimilarAttackKey(String tenantId, String agentId, String guardrailName, String embeddingModelVersion)
        implements Serializable {
}
