package com.bank.airiskops.infra.embedding;

/** Boundary for local evidence embedding implementations; no implementation may call a remote service. */
public interface EvidenceEmbedder extends AutoCloseable {
    float[] embed(String evidenceSnippet);
    String modelVersion();
    @Override default void close() { }
}
