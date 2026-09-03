package com.bank.airiskops.app.functions;

import com.bank.airiskops.app.config.SimilarPromptInjectionRuleConfig;
import com.bank.airiskops.infra.embedding.DeterministicEvidenceEmbedder;
import com.bank.airiskops.infra.embedding.EvidenceEmbedder;
import com.bank.airiskops.model.EmbeddedGuardrailFinding;
import com.bank.airiskops.model.SafetyEvent;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/** Creates one local normalized embedding per already-filtered evidence-bearing finding. */
public final class EmbedPromptInjectionEvidenceFunction extends RichMapFunction<SafetyEvent, EmbeddedGuardrailFinding> {
    private final SimilarPromptInjectionRuleConfig config;
    private final EvidenceEmbedder suppliedEmbedder;
    private transient EvidenceEmbedder embedder;
    public EmbedPromptInjectionEvidenceFunction(SimilarPromptInjectionRuleConfig config) { this(config, null); }
    public EmbedPromptInjectionEvidenceFunction(SimilarPromptInjectionRuleConfig config, EvidenceEmbedder suppliedEmbedder) {
        this.config = config; this.suppliedEmbedder = suppliedEmbedder;
    }
    @Override public void open(Configuration parameters) { embedder = suppliedEmbedder == null
            ? new DeterministicEvidenceEmbedder(config.embeddingModelVersion(), config.embeddingInputPrefix()) : suppliedEmbedder; }
    @Override public EmbeddedGuardrailFinding map(SafetyEvent event) {
        String evidence = event.evidenceSnippet().trim();
        if (evidence.length() > config.maxEvidenceLength()) evidence = evidence.substring(0, config.maxEvidenceLength());
        return new EmbeddedGuardrailFinding(event.tenantId(), event.agentId(), event.sessionId(), event.requestId(),
                event.eventTimeMillis(), event.guardrailName(), embedder.modelVersion(), embedder.embed(evidence), evidence);
    }
    @Override public void close() { if (embedder != null) embedder.close(); }
}
