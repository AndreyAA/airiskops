package com.bank.airiskops.infra.embedding;

import com.bank.airiskops.app.config.SimilarPromptInjectionRuleConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.langchain4j.model.embedding.onnx.OnnxEmbeddingModel;
import dev.langchain4j.model.embedding.onnx.PoolingMode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;

/**
 * In-process ONNX implementation backed by LangChain4j and locally packaged artifacts.
 *
 * <p>Construction validates the immutable model manifest before native ONNX initialization;
 * it never downloads an artifact or falls back to a different embedding implementation.
 */
public final class LangChain4jOnnxEvidenceEmbedder implements EvidenceEmbedder {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final OnnxEmbeddingModel model;
    private final String modelVersion;
    private final String inputPrefix;
    private final int expectedDimension;

    public LangChain4jOnnxEvidenceEmbedder(SimilarPromptInjectionRuleConfig config) {
        Path modelPath = requiredPath(config.embeddingModelPath(), "model");
        Path tokenizerPath = requiredPath(config.embeddingTokenizerPath(), "tokenizer");
        Path manifestPath = requiredPath(config.embeddingManifestPath(), "manifest");
        Manifest manifest = readManifest(manifestPath);
        verify(manifest, config, modelPath, tokenizerPath);
        this.model = new OnnxEmbeddingModel(modelPath, tokenizerPath, PoolingMode.MEAN);
        this.modelVersion = config.embeddingModelVersion();
        this.inputPrefix = config.embeddingInputPrefix();
        this.expectedDimension = config.embeddingExpectedDimension();
    }

    @Override
    public float[] embed(String evidenceSnippet) {
        float[] vector = model.embed(inputPrefix + evidenceSnippet).content().vector().clone();
        if (vector.length != expectedDimension) {
            throw new IllegalStateException("Unexpected embedding dimension: " + vector.length);
        }
        float normSquared = 0F;
        for (float component : vector) {
            if (!Float.isFinite(component)) {
                throw new IllegalStateException("Embedding contains a non-finite component");
            }
            normSquared += component * component;
        }
        if (!Float.isFinite(normSquared) || normSquared == 0F) {
            throw new IllegalStateException("Embedding has zero or invalid norm");
        }
        float norm = (float) Math.sqrt(normSquared);
        for (int index = 0; index < vector.length; index++) {
            vector[index] /= norm;
        }
        return vector;
    }

    @Override
    public String modelVersion() {
        return modelVersion;
    }

    private static Path requiredPath(String value, String artifactName) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing ONNX " + artifactName + " path configuration");
        }
        Path path = Path.of(value);
        if (!Files.isRegularFile(path)) {
            throw new IllegalStateException("ONNX " + artifactName + " file is unavailable: " + path);
        }
        return path;
    }

    private static Manifest readManifest(Path path) {
        try {
            return OBJECT_MAPPER.readValue(path.toFile(), Manifest.class);
        } catch (IOException exception) {
            throw new IllegalStateException("Cannot read ONNX embedding manifest: " + path, exception);
        }
    }

    private static void verify(Manifest manifest, SimilarPromptInjectionRuleConfig config, Path model, Path tokenizer) {
        if (!config.embeddingModelVersion().equals(manifest.modelVersion)
                || manifest.embeddingDimension != config.embeddingExpectedDimension()
                || !"MEAN".equals(manifest.pooling)
                || !config.embeddingInputPrefix().equals(manifest.inputPrefix)) {
            throw new IllegalStateException("ONNX embedding manifest does not match runtime configuration");
        }
        if (!sha256(model).equalsIgnoreCase(manifest.modelSha256)
                || !sha256(tokenizer).equalsIgnoreCase(manifest.tokenizerSha256)) {
            throw new IllegalStateException("ONNX embedding artifact checksum mismatch");
        }
    }

    private static String sha256(Path path) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot checksum ONNX embedding artifact", exception);
        }
    }

    /** JSON manifest copied beside the model into the immutable runtime image. */
    public static final class Manifest {
        public String modelVersion;
        public String modelSha256;
        public String tokenizerSha256;
        public int embeddingDimension;
        public String pooling;
        public String inputPrefix;
        public int maxTokens;
    }
}
