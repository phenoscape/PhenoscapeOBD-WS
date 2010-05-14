package org.phenoscape.obd.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.phenoscape.obd.model.DefaultTerm;
import org.phenoscape.obd.model.LinkedTerm;
import org.phenoscape.obd.model.Synonym;
import org.phenoscape.obd.model.TaxonTerm;
import org.phenoscape.obd.model.Term;

public class PhenoscapeDataStore {

    private final DataSource dataSource;

    public PhenoscapeDataStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Term getTerm(String uid) throws SQLException {
        return this.queryForTerm(uid);
    }

    public LinkedTerm getLinkedTerm(String uid) throws SQLException {
        final DefaultTerm term = this.queryForTerm(uid);
        this.addLinksToTerm(term);
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
                    "SELECT * FROM node term " +
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
        final DefaultTerm term = new DefaultTerm(result.getInt("node_id"));
        term.setUID(result.getString("uid"));
        term.setLabel(result.getString("label"));
        this.addSynonymsToTerm(term);
        return term;
    }

    private void addLinksToTerm(DefaultTerm term) {
        //TODO
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
                        final TaxonTerm parent = new TaxonTerm(taxonResult.getInt("parent_node_id"));
                        parent.setUID(taxonResult.getString("parent_uid"));
                        parent.setLabel(taxonResult.getString("parent_label"));
                        parent.setExtinct(taxonResult.getBoolean("parent_is_extinct"));
                        if (taxonResult.getString("parent_rank_uid") != null) {
                            final Term parentRank = new DefaultTerm(taxonResult.getInt("parent_rank_node_id"));
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
        final TaxonTerm taxon = new TaxonTerm(result.getInt("node_id"));
        taxon.setUID(result.getString("uid"));
        taxon.setLabel(result.getString("label"));
        taxon.setExtinct(result.getBoolean("is_extinct"));
        if (result.getString("rank_uid") != null) {
            final Term rank = new DefaultTerm(result.getInt("node_id"));
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

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
