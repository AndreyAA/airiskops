# Immutable local Flink runtime image with a pre-fetched multilingual-e5-small artifact.
# Build context must provide models/multilingual-e5-small/{model.onnx,tokenizer.json,manifest.json}.
FROM flink:1.20.2-scala_2.12-java17
COPY models/multilingual-e5-small/ /opt/airiskops/models/multilingual-e5-small/
RUN test -s /opt/airiskops/models/multilingual-e5-small/model.onnx \
 && test -s /opt/airiskops/models/multilingual-e5-small/tokenizer.json \
 && test -s /opt/airiskops/models/multilingual-e5-small/manifest.json
