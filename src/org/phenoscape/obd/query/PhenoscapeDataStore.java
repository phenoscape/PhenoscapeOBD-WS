package org.phenoscape.obd.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.phenoscape.obd.model.TaxonTerm;

public class PhenoscapeDataStore {
    
    private final DataSource dataSource;
    
    public PhenoscapeDataStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }
    
    public TaxonTerm getTaxonTerm(String uid) throws SQLException {
        Connection connection = null;
        PreparedStatement query = null;
        ResultSet result = null;
        try {
        connection = dataSource.getConnection();
        query = connection.prepareStatement("SELECT * FROM taxon WHERE uid = ?");
        query.setString(1, uid);
        result = query.executeQuery();
        while (result.next()) {
            final TaxonTerm taxon = new TaxonTerm();
            taxon.setUID(result.getString("uid"));
            taxon.setLabel(result.getString("label"));
            //TODO finish properties and add children query
            return taxon;
        }
        } catch (SQLException e) {
            throw e;
        } finally {
            if (query != null) {
                query.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        //should not happen
        log().error("Did not find taxon with uid in database: " + uid);
        return new TaxonTerm();
    }
    
    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
