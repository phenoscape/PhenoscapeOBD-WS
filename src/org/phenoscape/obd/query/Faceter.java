package org.phenoscape.obd.query;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;

public abstract class Faceter {

    private final PhenoscapeDataStore dataStore;
    private final int optimalSize; 

    public Faceter(PhenoscapeDataStore dataStore, int optimalSize) {
        this.dataStore = dataStore;
        this.optimalSize = optimalSize;
    }

    protected abstract List<String> getChildren(String focalTermUID) throws SQLException;

    protected abstract int getDataCount(String focalTermUID) throws SolrServerException;

    protected PhenoscapeDataStore getDataStore() {
        return this.dataStore;
    }

    public int getOptimalSize() {
        return this.optimalSize;
    }

    public Map<String, Integer> facetTerm(String uid) throws SQLException, SolrServerException {
        final Map<String, Integer> counts = new HashMap<String, Integer>();
        for (Partition partition : this.optimizePartitions(uid)) {
            counts.put(partition.getTerm(), partition.getCount());
        }
        return counts;
    }

    private List<Partition> optimizePartitions(String term) throws SQLException, SolrServerException {
        final List<Partition> partitions = new ArrayList<Partition>();
        partitions.addAll(this.getPartitions(term));
        log().debug("Number of partitions for top term: " + partitions.size());
        if (partitions.isEmpty()) {
            return partitions;
        }
        while (partitions.size() < this.getOptimalSize()) {
            log().debug("Partitions size: " + partitions.size());
            final Partition largest = Collections.max(partitions);
            final List<Partition> subpartitions = this.getPartitions(largest.getTerm());
            if (subpartitions.size() < 1) { break; } //2
            log().debug("Expanding largest: " + largest.getTerm() + ", " + largest.getCount());
            partitions.remove(largest);
            partitions.addAll(subpartitions);
        }
        final List<Partition> maxDepthedPartitions = new ArrayList<Partition>();
        for (Partition partition : partitions) {
            maxDepthedPartitions.add(this.maxDepth(partition));
        }
        return maxDepthedPartitions;
    }

    private List<Partition> getPartitions(String term) throws SQLException, SolrServerException {
        final List<Partition> partitions = new ArrayList<Partition>();
        for (String child : this.getChildren(term)) {
            final int subCount = this.getDataCount(child);
            if (subCount > 0) {
                partitions.add(new Partition(child, subCount));    
            }
        }
        return partitions;
    }

    private Partition maxDepth(Partition upperPartition) throws SQLException, SolrServerException {
        log().debug("Checking max depth for: " + upperPartition.term);
        final List<Partition> partitions = this.getPartitions(upperPartition.getTerm());
        if ((partitions.size() == 1) && (partitions.get(0).getCount() == upperPartition.getCount()) && (!partitions.get(0).getTerm().equals(upperPartition.getTerm()))) {
            return this.maxDepth(partitions.get(0));
        } else {
            return upperPartition;
        }
    }

    private static class Partition implements Comparable<Partition> {

        private final String term;
        private final int count;

        public Partition(String term, int count) {
            this.term = term;
            this.count = count;
        }

        public String getTerm() {
            return this.term;
        }

        public int getCount() {
            return this.count;
        }

        @Override
        public int compareTo(Partition other) {
            return Integer.valueOf(this.count).compareTo(Integer.valueOf(other.count));
        }

    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
