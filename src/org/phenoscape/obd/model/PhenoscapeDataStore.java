package org.phenoscape.obd.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.phenoscape.obd.model.Vocab.OBO;

public class PhenoscapeDataStore {

    private final DataSource dataSource;

    public PhenoscapeDataStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }
    
    public Date getRefreshDate() throws ParseException {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        //TODO this is a placeholder date - retrieve from database
        return formatter.parse("1859-11-24");
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
        Connection connection = null;
        try {
            connection = this.dataSource.getConnection();
            PreparedStatement statement = null;
            ResultSet result = null;
            try {
                final String annotationsQuery = 
                    "SELECT count(*) " +
                    "FROM annotation";
                statement = connection.prepareStatement(annotationsQuery);
                result = statement.executeQuery();
                while (result.next()) {
                    return result.getInt(1);
                }
            } finally {
                if (statement != null) { statement.close(); }
            }
        } finally {
            if (connection != null) { connection.close(); }
        }
        return 0;
    }

    public int getCountOfAnnotatedTaxa(boolean includeInferredAnnotations) throws SQLException {
        Connection connection = null;
        try {
            connection = this.dataSource.getConnection();
            PreparedStatement statement = null;
            ResultSet result = null;
            try {
                final String annotationsQuery = 
                    "SELECT count(DISTINCT taxon_node_id) " +
                    "FROM annotation";
                statement = connection.prepareStatement(annotationsQuery);
                result = statement.executeQuery();
                while (result.next()) {
                    return result.getInt(1);
                }
            } finally {
                if (statement != null) { statement.close(); }
            }
        } finally {
            if (connection != null) { connection.close(); }
        }
        return 0;
    }

    public int getCountOfGeneAnnotations() {
        //TODO
        return 0;
    }

    public int getCountOfAnnotatedGenes() {
        //TODO
        return 0;
    }
    
    public AutocompleteResult getAutocompleteMatches() {
        //TODO
        return null;
    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
