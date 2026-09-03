package com.bank.airiskops.model;

import java.io.Serializable;
import java.util.ArrayList;

/** Flink-state payload for one active semantic attack family. */
public final class SimilarAttackCluster implements Serializable {
    public String clusterId;
    public float[] centroid;
    public ArrayList<String> requestIds = new ArrayList<>();
    public ArrayList<String> sessionIds = new ArrayList<>();
    public ArrayList<String> evidenceSnippets = new ArrayList<>();
    public long firstEventTimeMillis;
    public long lastEventTimeMillis;
    public boolean emitted;
    public int emissionRevision;
}
