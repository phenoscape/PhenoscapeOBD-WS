package org.phenoscape.obd.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.phenoscape.obd.model.DefaultTerm;
import org.phenoscape.obd.model.LinkedTerm;
import org.phenoscape.obd.model.Synonym;
import org.phenoscape.obd.model.TaxonTerm;
import org.phenoscape.obd.model.Term;
import org.phenoscape.obd.model.Vocab.OBO;
import org.phenoscape.obd.query.SearchHit.MatchType;

import com.eekboom.utils.Strings;

public class PhenoscapeDataStore {

    private final DataSource dataSource;

    public PhenoscapeDataStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Returns the date on which the Knowledgebase data were loaded.
     */
    public Date getRefreshDate() throws SQLException {
        final DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
        //TODO don't put date in "notes" column
        final SimpleQuery query = new SimpleQuery("SELECT notes from obd_schema_metadata");
        final QueryExecutor<Date> executor = new QueryExecutor<Date>(this.dataSource, query) {
            @Override
            public Date processResult(ResultSet result) throws SQLException {
                try {
                    while (result.next()) {
                        final String date = result.getString("notes");
                        if (date != null) {
                            return formatter.parse(result.getString("notes"));    
                        }
                    }
                    // if a date was not found, return dummy date
                    return formatter.parse("1859-11-24_00:00:00");
                } catch (ParseException e) {
                    throw new SQLException(e);
                }
            }
        };
        return executor.executeQuery();
    }

    public Term getTerm(String uid) throws SQLException {
        return this.queryForTerm(uid);
    }

    public LinkedTerm getLinkedTerm(String uid) throws SQLException {
        final DefaultTerm term = this.queryForTerm(uid);
        if (term != null) {
            this.addLinksToTerm(term);    
        }
        return term;
    }

    private DefaultTerm queryForTerm(String uid) throws SQLException {
        Connection connection = null;
        try {
            connection = this.dataSource.getConnection();
            PreparedStatement termStatement = null;
            ResultSet termResult = null;
            try {
                final String termQuery = 
                    "SELECT term.*, description.label AS definition, tagval.val AS comment " +
                    "FROM node term " +
                    "LEFT OUTER JOIN description ON (description.node_id = term.node_id) " +
                    "LEFT OUTER JOIN node comment_rel ON (comment_rel.uid = '" + OBO.COMMENT + "') " +
                    "LEFT OUTER JOIN tagval ON (tagval.tag_id = comment_rel.node_id AND tagval.node_id = term.node_id) " +
                    "WHERE term.uid = ?";
                termStatement = connection.prepareStatement(termQuery);
                termStatement.setString(1, uid);
                termResult = termStatement.executeQuery();
                while (termResult.next()) {
                    final DefaultTerm term = this.createTerm(termResult);
                    return term;
                }
            } finally {
                if (termStatement != null) { termStatement.close(); }
            }
        } finally {
            if (connection != null) { connection.close(); }
        }
        return null;
    }

    private DefaultTerm createTerm(ResultSet result) throws SQLException {
        final DefaultTerm term = new DefaultTerm(result.getInt("node_id"), result.getInt("source_id"));
        term.setUID(result.getString("uid"));
        term.setLabel(result.getString("label"));
        term.setDefinition(result.getString("definition"));
        term.setComment(result.getString("comment"));
        this.addSynonymsToTerm(term);
        return term;
    }

    private void addLinksToTerm(DefaultTerm term) throws SQLException {
        Connection connection = null;
        try {
            connection = this.dataSource.getConnection();
            PreparedStatement subjectStatement = null;
            ResultSet subjectResult = null;
            try {
                final String subjectQuery = 
                    "SELECT link.*, relation.uid AS relation_uid, relation.label AS relation_label, target.uid AS other_uid, target.label AS other_label " +
                    "FROM link " +
                    "JOIN node relation ON (relation.node_id = link.predicate_id) " +
                    "JOIN node target ON (target.node_id = link.object_id) " +
                    "WHERE link.node_id = ? AND link.source_id = ?"; //TODO
                subjectStatement = connection.prepareStatement(subjectQuery);
                subjectStatement.setInt(1, term.getNodeID());
                subjectStatement.setInt(2, term.getSourceID());
                subjectResult = subjectStatement.executeQuery();
                log().debug("Source: " + term.getSourceID());
                while (subjectResult.next()) {
                    //TODO add links
                    log().debug(subjectResult.toString());
                }
            } finally {
                if (subjectStatement != null) { subjectStatement.close(); }
            }
            connection = this.dataSource.getConnection();
            PreparedStatement objectStatement = null;
            ResultSet objectResult = null;
            try {
                final String objectQuery = 
                    "SELECT link.*, relation.uid AS relation_uid, relation.label AS relation_label, subject.uid AS other_uid, subject.label AS other_label " +
                    "FROM link " +
                    "JOIN node relation ON (relation.node_id = link.predicate_id) " +
                    "JOIN node subject ON (subject.node_id = link.object_id) " +
                    "WHERE link.object_id = ? AND link.source_id = ?"; //TODO
                objectStatement = connection.prepareStatement(objectQuery);
                objectStatement.setInt(1, term.getNodeID());
                objectStatement.setInt(2, term.getSourceID());
                objectResult = objectStatement.executeQuery();
                while (objectResult.next()) {
                    //TODO add links
                    log().debug(objectResult.toString());
                }
            } finally {
                if (objectStatement != null) { objectStatement.close(); }
            }
        } finally {
            if (connection != null) { connection.close(); }
        }
    }

    /**
     * Return a TaxonTerm object for the given UID. Returns null if no taxon with that UID exists.
     */
    public TaxonTerm getTaxonTerm(String uid) throws SQLException {
        Connection connection = null;
        try {
            connection = this.dataSource.getConnection();
            PreparedStatement taxonStatement = null;
            ResultSet taxonResult = null;
            try {
                final String taxonQuery = 
                    "SELECT focal_taxon.*, parent.uid AS parent_uid, parent.label AS parent_label, parent.is_extinct AS parent_is_extinct, parent.rank_node_id AS parent_rank_node_id, parent.rank_uid AS parent_rank_uid, parent.rank_label AS parent_rank_label " +
                    "FROM taxon focal_taxon " +
                    "LEFT OUTER JOIN taxon parent ON (parent.node_id = focal_taxon.parent_node_id) " +
                    "WHERE focal_taxon.uid = ?";
                taxonStatement = connection.prepareStatement(taxonQuery);
                taxonStatement.setString(1, uid);
                taxonResult = taxonStatement.executeQuery();
                while (taxonResult.next()) {
                    final TaxonTerm taxon = this.createTaxonTermWithProperties(taxonResult);
                    if (taxonResult.getString("parent_uid") != null) {
                        final TaxonTerm parent = new TaxonTerm(taxonResult.getInt("parent_node_id"), null);
                        parent.setUID(taxonResult.getString("parent_uid"));
                        parent.setLabel(taxonResult.getString("parent_label"));
                        parent.setExtinct(taxonResult.getBoolean("parent_is_extinct"));
                        if (taxonResult.getString("parent_rank_uid") != null) {
                            final Term parentRank = new DefaultTerm(taxonResult.getInt("parent_rank_node_id"), null);
                            parentRank.setUID(taxonResult.getString("parent_rank_uid"));
                            parentRank.setLabel(taxonResult.getString("parent_rank_label"));
                            parent.setRank(parentRank);
                        }
                        taxon.setParent(parent);
                    }
                    PreparedStatement childrenStatement = null;
                    ResultSet childrenResult = null;
                    try {
                        final String childrenQuery = 
                            "SELECT * " +
                            "FROM taxon " +
                            "WHERE parent_node_id = ?";
                        childrenStatement = connection.prepareStatement(childrenQuery);
                        childrenStatement.setInt(1, taxon.getNodeID());
                        childrenResult = childrenStatement.executeQuery();
                        while (childrenResult.next()) {
                            final TaxonTerm child = this.createTaxonTermWithProperties(childrenResult);
                            taxon.addChild(child);
                        }
                    } finally {
                        if (childrenStatement != null) { childrenStatement.close(); }
                    }
                    this.addSynonymsToTerm(taxon);
                    return taxon;
                }
            }  finally {
                if (taxonStatement != null) { taxonStatement.close(); }
            }
        } finally {
            if (connection != null) { connection.close(); }
        }
        //no taxon with this ID
        return null;
    }

    /**
     * Creates a new TaxonTerm and extracts its uid, label, isExtinct, and rank from the ResultSet
     */
    private TaxonTerm createTaxonTermWithProperties(ResultSet result) throws SQLException {
        final TaxonTerm taxon = new TaxonTerm(result.getInt("node_id"), null);
        taxon.setUID(result.getString("uid"));
        taxon.setLabel(result.getString("label"));
        taxon.setExtinct(result.getBoolean("is_extinct"));
        if (result.getString("rank_uid") != null) {
            final Term rank = new DefaultTerm(result.getInt("node_id"), null);
            rank.setUID(result.getString("rank_uid"));
            rank.setLabel(result.getString("rank_label"));
            taxon.setRank(rank);
        }
        return taxon;
    }

    private void addSynonymsToTerm(DefaultTerm term) throws SQLException {
        Connection connection = null;
        try {
            connection = this.dataSource.getConnection();
            PreparedStatement synonymsStatement = null;
            ResultSet synonymsResult = null;
            try {
                final String synonymsQuery = 
                    "SELECT * " +
                    "FROM alias " +
                    "WHERE node_id = ?";
                synonymsStatement = connection.prepareStatement(synonymsQuery);
                synonymsStatement.setInt(1, term.getNodeID());
                synonymsResult = synonymsStatement.executeQuery();
                while (synonymsResult.next()) {
                    final Synonym synonym = this.createSynonym(synonymsResult);
                    term.addSynonym(synonym);
                }
            } finally {
                if (synonymsStatement != null) { synonymsStatement.close(); }
            }
        } finally {
            if (connection != null) { connection.close(); }
        }
    }

    private Synonym createSynonym(ResultSet result) throws SQLException {
        final Synonym synonym = new Synonym();
        synonym.setLabel(result.getString("label"));
        return synonym;
    }

    public int getCountOfTaxonomicAnnotations(boolean includeInferredAnnotations) throws SQLException {
        //TODO inferred
        final QueryBuilder query = new SimpleQuery("SELECT count(*) FROM annotation");
        final QueryExecutor<Integer> queryExecutor = new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        };
        return queryExecutor.executeQuery();
    }

    public int getCountOfAnnotatedTaxa(boolean includeInferredAnnotations) throws SQLException {
        //TODO inferred
        final QueryBuilder query = new SimpleQuery("SELECT count(DISTINCT taxon_node_id) FROM annotation");
        final QueryExecutor<Integer> queryExecutor = new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        };
        return queryExecutor.executeQuery();
    }

    public int getCountOfGeneAnnotations() {
        //TODO
        return 0;
    }

    public int getCountOfAnnotatedGenes() {
        //TODO
        return 0;
    }

    public AutocompleteResult getAutocompleteMatches(final SearchConfig config) throws SQLException {
        final AutocompleteResult matches = new AutocompleteResult(config);
        if (config.getNamespaces().isEmpty()) {
            log().warn("No namespaces provided for autocomplete search. Returning empty result.");
            return matches;
        }
        final List<SearchHit> sortedHits = new ArrayList<SearchHit>();
        if (config.searchNames()) {
            final Collection<SearchHit> nameMatches = this.queryNameMatches(config, false);
            sortedHits.addAll(nameMatches);    
        }
        if (config.searchSynonyms()) {
            final Collection<SearchHit> synonymMatches = this.querySynonymMatches(config, false);
            sortedHits.addAll(synonymMatches);
        }
        Collections.sort(sortedHits, new Comparator<SearchHit>() {
            public int compare(SearchHit a, SearchHit b) {
                final boolean aStartsWithMatch = a.getMatchText().toLowerCase().startsWith(config.getSearchText().toLowerCase());
                final boolean bStartsWithMatch = b.getMatchText().toLowerCase().startsWith(config.getSearchText().toLowerCase());
                if (aStartsWithMatch && bStartsWithMatch) {
                    return Strings.compareNatural(a.getMatchText(), b.getMatchText());
                } else if (aStartsWithMatch) {
                    return -1;
                } else if (bStartsWithMatch) {
                    return 1;
                } else {
                    return Strings.compareNatural(a.getMatchText(), b.getMatchText());
                }
            }
        });
        if ((config.getLimit() > 0) && (sortedHits.size() > config.getLimit())) {
            matches.addAllSearchHits(sortedHits.subList(0, config.getLimit()));
        } else {
            matches.addAllSearchHits(sortedHits);    
        }
        return matches;
    }

    private List<SearchHit> queryNameMatches(SearchConfig config, boolean startsWith) throws SQLException {
        final AutocompleteNameQueryBuilder queryBuilder = new AutocompleteNameQueryBuilder(config, startsWith);
        final QueryExecutor<List<SearchHit>> queryExecutor = new QueryExecutor<List<SearchHit>>(this.dataSource, queryBuilder) {
            @Override
            public List<SearchHit> processResult(ResultSet result) throws SQLException {
                final List<SearchHit> hits = new ArrayList<SearchHit>();
                while (result.next()) {
                    final SearchHit hit = createSearchHit(result, MatchType.NAME);
                    hits.add(hit);
                }
                return hits;
            }
        };
        return queryExecutor.executeQuery();
    }

    private List<SearchHit> querySynonymMatches(SearchConfig config, boolean startsWith) throws SQLException {
        final AutocompleteSynonymQueryBuilder queryBuilder = new AutocompleteSynonymQueryBuilder(config, startsWith);
        final QueryExecutor<List<SearchHit>> queryExecutor = new QueryExecutor<List<SearchHit>>(this.dataSource, queryBuilder) {
            @Override
            public List<SearchHit> processResult(ResultSet result) throws SQLException {
                final List<SearchHit> hits = new ArrayList<SearchHit>();
                while (result.next()) {
                    final SearchHit hit = createSearchHit(result, MatchType.SYNONYM);
                    hits.add(hit);
                }
                return hits;
            }
        };
        return queryExecutor.executeQuery();
    }

    private SearchHit createSearchHit(ResultSet nodeResult, MatchType type) throws SQLException {
        final DefaultTerm term = new DefaultTerm(nodeResult.getInt("node_id"), nodeResult.getInt("source_id"));
        term.setUID(nodeResult.getString("uid"));
        final String label = nodeResult.getString("label");
        term.setLabel(label);
        final String matchText;
        if (type == MatchType.SYNONYM) {
            matchText = nodeResult.getString("synonym_label");
        } else {
            matchText = label;
        }
        final SearchHit hit = new SearchHit(term, matchText, type);
        return hit;
    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
