package com.bank.airiskops.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import org.junit.jupiter.api.Test;

class BasicIncidentTest {
    @Test
    void defaultConstructorInitializesEmptyCollections() {
        BasicIncident incident = new BasicIncident();

        assertTrue(incident.requestIds().isEmpty());
        assertTrue(incident.guardrailNames().isEmpty());
        assertTrue(incident.guardrailVersions().isEmpty());
        assertTrue(incident.policyVersions().isEmpty());
    }

    @Test
    void settersAndAccessorsExposeSameValues() {
        BasicIncident incident = new BasicIncident();
        ArrayList<String> requestIds = new ArrayList<>();
        requestIds.add("req-1");
        ArrayList<String> guardrailNames = new ArrayList<>();
        guardrailNames.add("PROMPT_INJECTION");
        ArrayList<String> guardrailVersions = new ArrayList<>();
        guardrailVersions.add("pi-v1");
        ArrayList<String> policyVersions = new ArrayList<>();
        policyVersions.add("policy-v2");

        incident.setIncidentId("incident-1");
        incident.setTenantId("tenant-1");
        incident.setAgentId("agent-1");
        incident.setSessionId("session-1");
        incident.setRuleName("PROMPT_INJECTION_BURST");
        incident.setSeverity(IncidentSeverity.HIGH);
        incident.setRequestIds(requestIds);
        incident.setGuardrailNames(guardrailNames);
        incident.setGuardrailVersions(guardrailVersions);
        incident.setPolicyVersions(policyVersions);
        incident.setAppliedPolicyVersion("policy-v2");
        incident.setFirstEventTimeMillis(101L);
        incident.setLastEventTimeMillis(202L);
        incident.setEmittedAtEventTimeMillis(303L);
        incident.setTriggeredFindingsCount(4L);
        incident.setEmissionRevision(5);
        incident.setSummary("summary");

        assertEquals("incident-1", incident.incidentId());
        assertEquals("incident-1", incident.getIncidentId());
        assertEquals("tenant-1", incident.tenantId());
        assertEquals("tenant-1", incident.getTenantId());
        assertEquals("agent-1", incident.agentId());
        assertEquals("agent-1", incident.getAgentId());
        assertEquals("session-1", incident.sessionId());
        assertEquals("session-1", incident.getSessionId());
        assertEquals("PROMPT_INJECTION_BURST", incident.ruleName());
        assertEquals("PROMPT_INJECTION_BURST", incident.getRuleName());
        assertEquals(IncidentSeverity.HIGH, incident.severity());
        assertEquals(IncidentSeverity.HIGH, incident.getSeverity());
        assertEquals(requestIds, incident.requestIds());
        assertEquals(requestIds, incident.getRequestIds());
        assertEquals(guardrailNames, incident.guardrailNames());
        assertEquals(guardrailNames, incident.getGuardrailNames());
        assertEquals(guardrailVersions, incident.guardrailVersions());
        assertEquals(guardrailVersions, incident.getGuardrailVersions());
        assertEquals(policyVersions, incident.policyVersions());
        assertEquals(policyVersions, incident.getPolicyVersions());
        assertEquals("policy-v2", incident.appliedPolicyVersion());
        assertEquals("policy-v2", incident.getAppliedPolicyVersion());
        assertEquals(101L, incident.firstEventTimeMillis());
        assertEquals(101L, incident.getFirstEventTimeMillis());
        assertEquals(202L, incident.lastEventTimeMillis());
        assertEquals(202L, incident.getLastEventTimeMillis());
        assertEquals(303L, incident.emittedAtEventTimeMillis());
        assertEquals(303L, incident.getEmittedAtEventTimeMillis());
        assertEquals(4L, incident.triggeredFindingsCount());
        assertEquals(4L, incident.getTriggeredFindingsCount());
        assertEquals(5, incident.emissionRevision());
        assertEquals(5, incident.getEmissionRevision());
        assertEquals("summary", incident.summary());
        assertEquals("summary", incident.getSummary());
    }

    @Test
    void fullConstructorAssignsAllFields() {
        ArrayList<String> requestIds = new ArrayList<>();
        requestIds.add("req-1");
        ArrayList<String> guardrailNames = new ArrayList<>();
        guardrailNames.add("TOXICITY");
        ArrayList<String> guardrailVersions = new ArrayList<>();
        guardrailVersions.add("tox-v1");
        ArrayList<String> policyVersions = new ArrayList<>();
        policyVersions.add("policy-v3");

        BasicIncident incident = new BasicIncident(
                "incident-2",
                "tenant-2",
                "agent-2",
                "session-2",
                "TOXICITY_CAMPAIGN",
                IncidentSeverity.MEDIUM,
                requestIds,
                guardrailNames,
                guardrailVersions,
                policyVersions,
                "policy-v3",
                11L,
                22L,
                33L,
                44L,
                2,
                "constructed"
        );

        assertEquals("incident-2", incident.incidentId());
        assertEquals("tenant-2", incident.tenantId());
        assertEquals("agent-2", incident.agentId());
        assertEquals("session-2", incident.sessionId());
        assertEquals("TOXICITY_CAMPAIGN", incident.ruleName());
        assertEquals(IncidentSeverity.MEDIUM, incident.severity());
        assertEquals(requestIds, incident.requestIds());
        assertEquals(guardrailNames, incident.guardrailNames());
        assertEquals(guardrailVersions, incident.guardrailVersions());
        assertEquals(policyVersions, incident.policyVersions());
        assertEquals("policy-v3", incident.appliedPolicyVersion());
        assertEquals(11L, incident.firstEventTimeMillis());
        assertEquals(22L, incident.lastEventTimeMillis());
        assertEquals(33L, incident.emittedAtEventTimeMillis());
        assertEquals(44L, incident.triggeredFindingsCount());
        assertEquals(2, incident.emissionRevision());
        assertEquals("constructed", incident.summary());
    }
}
