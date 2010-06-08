package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A QueryBuilder which represents a static SQL query requiring no parameters.
 */
public class SimpleQuery extends QueryBuilder {
    
    final String query;
    
    /**
     * Create a new SimpleQuery.
     * @param query The SQL to be executed.
     */
    public SimpleQuery(String query) {
        this.query = query;
    }

    @Override
    protected String getQuery() {
        return this.query;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {}

}
