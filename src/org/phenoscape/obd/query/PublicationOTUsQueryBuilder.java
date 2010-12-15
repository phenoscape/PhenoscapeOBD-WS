package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PublicationOTUsQueryBuilder extends QueryBuilder {
    
    final String publicationID;
    
    public PublicationOTUsQueryBuilder(String pubID) {
        this.publicationID = pubID;
    }

    @Override
    protected String getQuery() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        // TODO Auto-generated method stub

    }

}
