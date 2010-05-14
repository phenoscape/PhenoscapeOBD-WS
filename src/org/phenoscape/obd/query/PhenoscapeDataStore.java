package org.phenoscape.obd.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.phenoscape.obd.model.TaxonTerm;
import org.phenoscape.obd.model.Term;

public class PhenoscapeDataStore {

    private final DataSource dataSource;

    public PhenoscapeDataStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Return a TaxonTerm object for the given UID. Returns null if no taxon with that UID exists.
     */
    public TaxonTerm getTaxonTerm(String uid) throws SQLException {
        Connection connection = null;
        PreparedStatement taxonStatement = null;
        ResultSet taxonResult = null;
        PreparedStatement childrenStatement = null;
        ResultSet childrenResult = null;
        try {
            connection = dataSource.getConnection();
            final String taxonQuery = 
                "SELECT focal_taxon.*, parent.uid AS parent_uid, parent.label AS parent_label, parent.is_extinct AS parent_is_extinct, parent.rank_uid AS parent_rank_uid, parent.rank_label AS parent_rank_label " +
                "FROM taxon focal_taxon " +
                "LEFT OUTER JOIN taxon parent ON (parent.node_id = focal_taxon.parent_node_id) " +
                "WHERE focal_taxon.uid = ?";
            taxonStatement = connection.prepareStatement(taxonQuery);
            taxonStatement.setString(1, uid);
            taxonResult = taxonStatement.executeQuery();
            while (taxonResult.next()) {
                final TaxonTerm taxon = this.createTaxonTermWithProperties(taxonResult);
                final int taxonNode = taxonResult.getInt("node_id");
                if (taxonResult.getString("parent_uid") != null) {
                    final TaxonTerm parent = new TaxonTerm();
                    parent.setUID(taxonResult.getString("parent_uid"));
                    parent.setLabel(taxonResult.getString("parent_label"));
                    parent.setExtinct(taxonResult.getBoolean("parent_is_extinct"));
                    if (taxonResult.getString("parent_rank_uid") != null) {
                        final Term parentRank = new Term();
                        parentRank.setUID(taxonResult.getString("parent_rank_uid"));
                        parentRank.setLabel(taxonResult.getString("parent_rank_label"));
                        parent.setRank(parentRank);
                    }
                    taxon.setParent(parent);
                }
                final String childrenQuery = 
                    "SELECT * " +
                    "FROM taxon " +
                    "WHERE parent_node_id = ?";
                childrenStatement = connection.prepareStatement(childrenQuery);
                childrenStatement.setInt(1, taxonNode);
                childrenResult = childrenStatement.executeQuery();
                while (childrenResult.next()) {
                    final TaxonTerm child = this.createTaxonTermWithProperties(childrenResult);
                    taxon.addChild(child);
                }
                return taxon;
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            if (taxonStatement != null) {
                taxonStatement.close();
            }
            if (childrenStatement != null) {
                childrenStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        //no taxon with this ID
        return null;
    }

    /**
     * Creates a new TaxonTerm and extracts its uid, label, isExtinct, and rank from the ResultSet
     */
    private TaxonTerm createTaxonTermWithProperties(ResultSet result) throws SQLException {
        final TaxonTerm taxon = new TaxonTerm();
        taxon.setUID(result.getString("uid"));
        taxon.setLabel(result.getString("label"));
        taxon.setExtinct(result.getBoolean("is_extinct"));
        if (result.getString("rank_uid") != null) {
            final Term rank = new Term();
            rank.setUID(result.getString("rank_uid"));
            rank.setLabel(result.getString("rank_label"));
            taxon.setRank(rank);
        }
        return taxon;
    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
