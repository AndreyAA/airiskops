package com.bank.aisafetyops.model;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Minimal incident contract emitted by Increment 3 session correlation logic.
 *
 * <p>This class is implemented as a Flink-friendly POJO rather than a record so
 * operator tests and runtime serialization remain predictable on Java 17.
 */
public final class BasicIncident implements Serializable {
    private String incidentId;
    private String tenantId;
    private String agentId;
    private String sessionId;
    private String ruleName;
    private IncidentSeverity severity;
    private ArrayList<String> requestIds;
    private ArrayList<String> guardrailNames;
    private ArrayList<String> guardrailVersions;
    private ArrayList<String> policyVersions;
    private String appliedPolicyVersion;
    private long firstEventTimeMillis;
    private long lastEventTimeMillis;
    private long emittedAtEventTimeMillis;
    private long triggeredFindingsCount;
    private int emissionRevision;
    private String summary;

    public BasicIncident() {
        requestIds = new ArrayList<>();
        guardrailNames = new ArrayList<>();
        guardrailVersions = new ArrayList<>();
        policyVersions = new ArrayList<>();
    }

    public BasicIncident(
            String incidentId,
            String tenantId,
            String agentId,
            String sessionId,
            String ruleName,
            IncidentSeverity severity,
            ArrayList<String> requestIds,
            ArrayList<String> guardrailNames,
            ArrayList<String> guardrailVersions,
            ArrayList<String> policyVersions,
            String appliedPolicyVersion,
            long firstEventTimeMillis,
            long lastEventTimeMillis,
            long emittedAtEventTimeMillis,
            long triggeredFindingsCount,
            int emissionRevision,
            String summary
    ) {
        this.incidentId = incidentId;
        this.tenantId = tenantId;
        this.agentId = agentId;
        this.sessionId = sessionId;
        this.ruleName = ruleName;
        this.severity = severity;
        this.requestIds = requestIds;
        this.guardrailNames = guardrailNames;
        this.guardrailVersions = guardrailVersions;
        this.policyVersions = policyVersions;
        this.appliedPolicyVersion = appliedPolicyVersion;
        this.firstEventTimeMillis = firstEventTimeMillis;
        this.lastEventTimeMillis = lastEventTimeMillis;
        this.emittedAtEventTimeMillis = emittedAtEventTimeMillis;
        this.triggeredFindingsCount = triggeredFindingsCount;
        this.emissionRevision = emissionRevision;
        this.summary = summary;
    }

    public String incidentId() {
        return incidentId;
    }

    public String getIncidentId() {
        return incidentId;
    }

    public void setIncidentId(String incidentId) {
        this.incidentId = incidentId;
    }

    public String tenantId() {
        return tenantId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String agentId() {
        return agentId;
    }

    public String getAgentId() {
        return agentId;
    }

    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    public String sessionId() {
        return sessionId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String ruleName() {
        return ruleName;
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public IncidentSeverity severity() {
        return severity;
    }

    public IncidentSeverity getSeverity() {
        return severity;
    }

    public void setSeverity(IncidentSeverity severity) {
        this.severity = severity;
    }

    public ArrayList<String> requestIds() {
        return requestIds;
    }

    public ArrayList<String> getRequestIds() {
        return requestIds;
    }

    public void setRequestIds(ArrayList<String> requestIds) {
        this.requestIds = requestIds;
    }

    public ArrayList<String> guardrailNames() {
        return guardrailNames;
    }

    public ArrayList<String> getGuardrailNames() {
        return guardrailNames;
    }

    public void setGuardrailNames(ArrayList<String> guardrailNames) {
        this.guardrailNames = guardrailNames;
    }

    public ArrayList<String> guardrailVersions() {
        return guardrailVersions;
    }

    public ArrayList<String> getGuardrailVersions() {
        return guardrailVersions;
    }

    public void setGuardrailVersions(ArrayList<String> guardrailVersions) {
        this.guardrailVersions = guardrailVersions;
    }

    public ArrayList<String> policyVersions() {
        return policyVersions;
    }

    public ArrayList<String> getPolicyVersions() {
        return policyVersions;
    }

    public void setPolicyVersions(ArrayList<String> policyVersions) {
        this.policyVersions = policyVersions;
    }

    public String appliedPolicyVersion() {
        return appliedPolicyVersion;
    }

    public String getAppliedPolicyVersion() {
        return appliedPolicyVersion;
    }

    public void setAppliedPolicyVersion(String appliedPolicyVersion) {
        this.appliedPolicyVersion = appliedPolicyVersion;
    }

    public long firstEventTimeMillis() {
        return firstEventTimeMillis;
    }

    public long getFirstEventTimeMillis() {
        return firstEventTimeMillis;
    }

    public void setFirstEventTimeMillis(long firstEventTimeMillis) {
        this.firstEventTimeMillis = firstEventTimeMillis;
    }

    public long lastEventTimeMillis() {
        return lastEventTimeMillis;
    }

    public long getLastEventTimeMillis() {
        return lastEventTimeMillis;
    }

    public void setLastEventTimeMillis(long lastEventTimeMillis) {
        this.lastEventTimeMillis = lastEventTimeMillis;
    }

    public long emittedAtEventTimeMillis() {
        return emittedAtEventTimeMillis;
    }

    public long getEmittedAtEventTimeMillis() {
        return emittedAtEventTimeMillis;
    }

    public void setEmittedAtEventTimeMillis(long emittedAtEventTimeMillis) {
        this.emittedAtEventTimeMillis = emittedAtEventTimeMillis;
    }

    public long triggeredFindingsCount() {
        return triggeredFindingsCount;
    }

    public long getTriggeredFindingsCount() {
        return triggeredFindingsCount;
    }

    public void setTriggeredFindingsCount(long triggeredFindingsCount) {
        this.triggeredFindingsCount = triggeredFindingsCount;
    }

    public int emissionRevision() {
        return emissionRevision;
    }

    public int getEmissionRevision() {
        return emissionRevision;
    }

    public void setEmissionRevision(int emissionRevision) {
        this.emissionRevision = emissionRevision;
    }

    public String summary() {
        return summary;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }
}
