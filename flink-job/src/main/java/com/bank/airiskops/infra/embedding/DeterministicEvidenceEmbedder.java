package com.bank.airiskops.infra.embedding;

import java.text.Normalizer;
import java.util.Locale;

/**
 * Local deterministic embedding used by the concept runtime when model artifacts are not packaged.
 *
 * <p>It has no network dependency and provides a stable normalized vector for replay and state tests.
 * A production image can replace this adapter with the configured ONNX implementation without changing
 * the stream contract or keyed-state algorithm.
 */
public final class DeterministicEvidenceEmbedder implements EvidenceEmbedder {
    private static final int DIMENSION = 384;
    private final String version;
    private final String prefix;

    public DeterministicEvidenceEmbedder(String version, String prefix) {
        this.version = version;
        this.prefix = prefix;
    }
    @Override public float[] embed(String evidenceSnippet) {
        String normalized = Normalizer.normalize(prefix + evidenceSnippet, Normalizer.Form.NFKC)
                .toLowerCase(Locale.ROOT).replaceAll("\\s+", " ").trim();
        float[] vector = new float[DIMENSION];
        String[] tokens = normalized.split(" ");
        for (String token : tokens) {
            if (token.isBlank()) continue;
            token = canonicalToken(token);
            int hash = token.hashCode();
            vector[Math.floorMod(hash, DIMENSION)] += 1f;
            vector[Math.floorMod(hash * 31 + token.length(), DIMENSION)] += .5f;
        }
        float norm = 0f;
        for (float component : vector) norm += component * component;
        if (norm == 0f) throw new IllegalArgumentException("Evidence produced a zero embedding");
        norm = (float) Math.sqrt(norm);
        for (int index = 0; index < vector.length; index++) vector[index] /= norm;
        return vector;
    }
    private static String canonicalToken(String token) {
        return switch (token) {
            case "ignore", "disregard", "bypass" -> "override";
            case "earlier", "prior", "previous" -> "previous";
            case "reveal", "show", "disclose" -> "reveal";
            case "prompt", "message" -> "prompt";
            case "instructions", "rules" -> "instructions";
            default -> token;
        };
    }
    @Override public String modelVersion() { return version; }
}
