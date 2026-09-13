package com.bank.airiskops.app.functions;

import com.bank.airiskops.app.config.SimilarPromptInjectionRuleConfig;
import com.bank.airiskops.app.config.EmbeddingProvider;
import com.bank.airiskops.infra.embedding.DeterministicEvidenceEmbedder;
import com.bank.airiskops.infra.embedding.EvidenceEmbedder;
import com.bank.airiskops.infra.embedding.LangChain4jOnnxEvidenceEmbedder;
import com.bank.airiskops.model.EmbeddedGuardrailFinding;
import com.bank.airiskops.model.SafetyEvent;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

/** Creates one local normalized embedding per already-filtered evidence-bearing finding. */
public final class EmbedPromptInjectionEvidenceFunction extends RichMapFunction<SafetyEvent, EmbeddedGuardrailFinding> {
    private final SimilarPromptInjectionRuleConfig config;
    private final EvidenceEmbedder suppliedEmbedder;
    private transient EvidenceEmbedder embedder;
    private transient Counter generatedCounter;
    public EmbedPromptInjectionEvidenceFunction(SimilarPromptInjectionRuleConfig config) { this(config, null); }
    public EmbedPromptInjectionEvidenceFunction(SimilarPromptInjectionRuleConfig config, EvidenceEmbedder suppliedEmbedder) {
        this.config = config; this.suppliedEmbedder = suppliedEmbedder;
    }
    @Override public void open(Configuration parameters) {
        generatedCounter = getRuntimeContext().getMetricGroup().addGroup("airiskops").addGroup("similarity_embedding")
                .counter("embeddings_generated_total");
        embedder = suppliedEmbedder == null ? createConfiguredEmbedder() : suppliedEmbedder;
    }
    @Override public EmbeddedGuardrailFinding map(SafetyEvent event) {
        String evidence = event.evidenceSnippet().trim();
        if (evidence.length() > config.maxEvidenceLength()) evidence = evidence.substring(0, config.maxEvidenceLength());
        EmbeddedGuardrailFinding finding = new EmbeddedGuardrailFinding(event.tenantId(), event.agentId(), event.sessionId(), event.requestId(),
                event.eventTimeMillis(), event.guardrailName(), embedder.modelVersion(), embedder.embed(evidence), evidence);
        generatedCounter.inc();
        return finding;
    }
    @Override public void close() { if (embedder != null) embedder.close(); }

    private EvidenceEmbedder createConfiguredEmbedder() {
        return config.embeddingProvider() == EmbeddingProvider.LANGCHAIN4J_ONNX
                ? new LangChain4jOnnxEvidenceEmbedder(config)
                : new DeterministicEvidenceEmbedder(config.embeddingModelVersion(), config.embeddingInputPrefix());
    }
}
