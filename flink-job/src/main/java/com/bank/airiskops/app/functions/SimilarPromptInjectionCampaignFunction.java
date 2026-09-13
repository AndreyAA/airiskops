package com.bank.airiskops.app.functions;

import com.bank.airiskops.app.config.SimilarPromptInjectionRuleConfig;
import com.bank.airiskops.model.BasicIncident;
import com.bank.airiskops.model.EmbeddedGuardrailFinding;
import com.bank.airiskops.model.IncidentRuleNames;
import com.bank.airiskops.model.SimilarAttackCluster;
import com.bank.airiskops.model.SimilarAttackKey;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Correlates normalized local embeddings in a bounded rolling event-time state.
 *
 * <p>The key excludes session intentionally. State is pruned at the window boundary and its
 * collection limits make replay/checkpoint size independent of sustained input volume.
 */
public final class SimilarPromptInjectionCampaignFunction
        extends KeyedProcessFunction<SimilarAttackKey, EmbeddedGuardrailFinding, BasicIncident> {
    private static final String STATE_NAME = "similar-prompt-injection-clusters-v1";
    private final SimilarPromptInjectionRuleConfig config;
    private final boolean emitUpdates;
    private transient ListState<SimilarAttackCluster> clusterState;
    private transient Counter evictedCounter;
    private transient Counter overflowCounter;
    private transient Counter emittedCounter;
    private transient Counter ruleCounter;
    private transient Counter severityCounter;
    private transient Counter updatedCounter;

    public SimilarPromptInjectionCampaignFunction(SimilarPromptInjectionRuleConfig config) { this(config, false); }
    public SimilarPromptInjectionCampaignFunction(SimilarPromptInjectionRuleConfig config, boolean emitUpdates) {
        this.config = config; this.emitUpdates = emitUpdates;
    }
    @Override public void open(Configuration parameters) {
        clusterState = getRuntimeContext().getListState(new ListStateDescriptor<>(STATE_NAME, SimilarAttackCluster.class));
        var metrics = getRuntimeContext().getMetricGroup().addGroup("airiskops").addGroup("similar_prompt_injection");
        evictedCounter = metrics.counter("clusters_evicted_total");
        overflowCounter = metrics.counter("cluster_request_overflow_total");
        emittedCounter = getRuntimeContext().getMetricGroup().addGroup("airiskops").addGroup("incident")
                .counter("incidents_emitted_total");
        ruleCounter = getRuntimeContext().getMetricGroup().addGroup("airiskops").addGroup("incident")
                .addGroup("rule", IncidentRuleNames.SIMILAR_PROMPT_INJECTION_CAMPAIGN).counter("incidents_emitted_total");
        severityCounter = getRuntimeContext().getMetricGroup().addGroup("airiskops").addGroup("incident")
                .addGroup("severity", config.severity().name().toLowerCase()).counter("incidents_emitted_total");
        updatedCounter = getRuntimeContext().getMetricGroup().addGroup("airiskops").addGroup("incident")
                .counter("incident_updates_total");
    }
    @Override public void processElement(EmbeddedGuardrailFinding finding, Context context, Collector<BasicIncident> out) throws Exception {
        validateEmbedding(finding.embedding());
        long boundary = finding.eventTimeMillis() - config.window().toMillis();
        List<SimilarAttackCluster> clusters = activeClusters(boundary);
        SimilarAttackCluster cluster = bestCluster(clusters, finding.embedding());
        if (cluster == null || cosineSimilarity(cluster.centroid, finding.embedding()) < config.similarityThreshold()) {
            cluster = newCluster(finding);
            if (clusters.size() >= config.maxClustersPerKey()) {
                clusters.sort(Comparator.comparingLong((SimilarAttackCluster item) -> item.lastEventTimeMillis)
                        .thenComparing(item -> item.clusterId));
                clusters.remove(0); evictedCounter.inc();
            }
            clusters.add(cluster);
        }
        if (!cluster.requestIds.contains(finding.requestId())) {
            if (cluster.requestIds.size() >= config.maxTrackedRequestIdsPerCluster()) {
                overflowCounter.inc();
            } else {
                add(cluster.requestIds, finding.requestId(), config.maxTrackedRequestIdsPerCluster());
                add(cluster.sessionIds, finding.sessionId(), config.maxSessionIdsPerCluster());
                add(cluster.evidenceSnippets, truncate(finding.evidenceSnippet(), config.maxStoredEvidenceSampleLength()),
                        config.maxEvidenceSamplesPerCluster());
                cluster.centroid = updateCentroid(cluster.centroid, cluster.requestIds.size() - 1, finding.embedding());
                cluster.lastEventTimeMillis = Math.max(cluster.lastEventTimeMillis, finding.eventTimeMillis());
                cluster.firstEventTimeMillis = Math.min(cluster.firstEventTimeMillis, finding.eventTimeMillis());
                if (eligible(cluster) && (!cluster.emitted || emitUpdates)) emitIfAllowed(cluster, finding, out);
            }
        }
        clusterState.update(clusters);
        // The window's left boundary is inclusive, hence cleanup runs one millisecond after it.
        context.timerService().registerEventTimeTimer(finding.eventTimeMillis() + config.window().toMillis() + 1L);
    }
    private void emitIfAllowed(SimilarAttackCluster cluster, EmbeddedGuardrailFinding finding, Collector<BasicIncident> out) {
        // Once emitted, only the configured revision mode may produce another event.
        if (cluster.emitted && !emitUpdates) return;
        cluster.emitted = true;
        cluster.emissionRevision++;
        BasicIncident incident = new BasicIncident();
        incident.setIncidentId("similar-pi|" + cluster.clusterId);
        incident.setTenantId(finding.tenantId()); incident.setAgentId(finding.agentId()); incident.setSessionId(finding.sessionId());
        incident.setRuleName(IncidentRuleNames.SIMILAR_PROMPT_INJECTION_CAMPAIGN); incident.setSeverity(config.severity());
        incident.setRequestIds(new ArrayList<>(cluster.requestIds.subList(
                0,
                Math.min(cluster.requestIds.size(), config.maxRequestIdsPerIncident())
        ))); incident.setSessionIds(new ArrayList<>(cluster.sessionIds));
        incident.setEvidenceSnippets(new ArrayList<>(cluster.evidenceSnippets)); incident.setEmbeddingModelVersion(finding.embeddingModelVersion());
        incident.setFirstEventTimeMillis(cluster.firstEventTimeMillis); incident.setLastEventTimeMillis(cluster.lastEventTimeMillis);
        incident.setEmittedAtEventTimeMillis(finding.eventTimeMillis()); incident.setTriggeredFindingsCount(cluster.requestIds.size());
        incident.setEmissionRevision(cluster.emissionRevision);
        incident.setSummary("Similar prompt-injection campaign: " + cluster.requestIds.size() + " attacks across "
                + cluster.sessionIds.size() + " sessions");
        emittedCounter.inc(); ruleCounter.inc(); severityCounter.inc();
        if (cluster.emissionRevision > 1) updatedCounter.inc();
        out.collect(incident);
    }
    @Override public void onTimer(long timestamp, OnTimerContext context, Collector<BasicIncident> out) throws Exception {
        List<SimilarAttackCluster> active = activeClusters(timestamp - config.window().toMillis());
        if (active.isEmpty()) clusterState.clear(); else clusterState.update(active);
    }
    private List<SimilarAttackCluster> activeClusters(long boundary) throws Exception {
        List<SimilarAttackCluster> result = new ArrayList<>();
        for (SimilarAttackCluster cluster : clusterState.get()) if (cluster.lastEventTimeMillis >= boundary) result.add(cluster);
        return result;
    }
    private boolean eligible(SimilarAttackCluster cluster) { return cluster.requestIds.size() >= config.minUniqueRequests()
            && cluster.sessionIds.size() >= config.minDistinctSessions(); }
    private SimilarAttackCluster newCluster(EmbeddedGuardrailFinding finding) {
        SimilarAttackCluster cluster = new SimilarAttackCluster(); cluster.clusterId = finding.requestId(); cluster.centroid = finding.embedding().clone();
        cluster.firstEventTimeMillis = finding.eventTimeMillis(); cluster.lastEventTimeMillis = finding.eventTimeMillis(); return cluster;
    }
    private static void add(ArrayList<String> values, String value, int max) { if (!values.contains(value) && values.size() < max) values.add(value); }
    private static String truncate(String value, int max) { return value.length() <= max ? value : value.substring(0, max); }
    public static float cosineSimilarity(float[] a, float[] b) { validateEmbedding(a); validateEmbedding(b); if (a.length != b.length) throw new IllegalArgumentException("Embedding dimensions differ"); float total=0; for(int i=0;i<a.length;i++) total+=a[i]*b[i]; return total; }
    public static float[] updateCentroid(float[] prior, int priorCount, float[] next) { if (prior.length != next.length) throw new IllegalArgumentException("Embedding dimensions differ"); float[] result=new float[prior.length]; float norm=0; for(int i=0;i<result.length;i++){ result[i]=(prior[i]*priorCount+next[i])/(priorCount+1); norm+=result[i]*result[i]; } norm=(float)Math.sqrt(norm); if(norm==0) throw new IllegalArgumentException("Zero centroid"); for(int i=0;i<result.length;i++)result[i]/=norm; return result; }
    private static SimilarAttackCluster bestCluster(List<SimilarAttackCluster> clusters, float[] embedding) { return clusters.stream().max(Comparator.comparingDouble((SimilarAttackCluster c)->cosineSimilarity(c.centroid, embedding)).thenComparing(c->c.clusterId, Comparator.reverseOrder())).orElse(null); }
    private static void validateEmbedding(float[] vector) { if(vector==null||vector.length==0)throw new IllegalArgumentException("Embedding is empty"); for(float item:vector)if(!Float.isFinite(item))throw new IllegalArgumentException("Embedding is not finite"); }
}
