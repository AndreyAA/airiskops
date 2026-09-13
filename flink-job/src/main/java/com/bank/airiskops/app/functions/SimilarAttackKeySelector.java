package com.bank.airiskops.app.functions;

import com.bank.airiskops.model.EmbeddedGuardrailFinding;
import com.bank.airiskops.model.SimilarAttackKey;
import org.apache.flink.api.java.functions.KeySelector;

/** Selects the tenant/agent/model scoped key for cross-session semantic correlation. */
public final class SimilarAttackKeySelector implements KeySelector<EmbeddedGuardrailFinding, SimilarAttackKey> {
    @Override public SimilarAttackKey getKey(EmbeddedGuardrailFinding value) {
        return new SimilarAttackKey(value.tenantId(), value.agentId(), value.guardrailName(), value.embeddingModelVersion());
    }
}
