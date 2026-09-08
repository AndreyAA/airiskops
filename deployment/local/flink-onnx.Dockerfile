# Immutable local Flink runtime image with a pre-fetched multilingual-e5-small artifact.
# Build context must provide models/multilingual-e5-small/{model.onnx,tokenizer.json,manifest.json}.
FROM flink:1.20.2-scala_2.12-java17
COPY models/multilingual-e5-small/ /opt/airiskops/models/multilingual-e5-small/
RUN test -s /opt/airiskops/models/multilingual-e5-small/model.onnx \
 && test -s /opt/airiskops/models/multilingual-e5-small/tokenizer.json \
 && test -s /opt/airiskops/models/multilingual-e5-small/manifest.json \
 && model_expected="$(sed -nE 's/.*"modelSha256"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p' /opt/airiskops/models/multilingual-e5-small/manifest.json)" \
 && tokenizer_expected="$(sed -nE 's/.*"tokenizerSha256"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p' /opt/airiskops/models/multilingual-e5-small/manifest.json)" \
 && test -n "$model_expected" \
 && test -n "$tokenizer_expected" \
 && test "$(sha256sum /opt/airiskops/models/multilingual-e5-small/model.onnx | awk '{print $1}')" = "$model_expected" \
 && test "$(sha256sum /opt/airiskops/models/multilingual-e5-small/tokenizer.json | awk '{print $1}')" = "$tokenizer_expected"
