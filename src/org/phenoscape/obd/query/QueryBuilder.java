package org.phenoscape.obd.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Represents an SQL query to be executed.  Subclasses can be created to provide an API 
 * to build up particular queries that require multiple inputs.
 */
public abstract class QueryBuilder {

    /**
     * Return the SQL query string, which may contain wildcards (?).
     */
    protected abstract String getQuery();

    /**
     * Subclasses should set wildcard parameter values for the given Statement.
     */
    protected abstract void fillStatement(PreparedStatement statement) throws SQLException;

    /**
     * Returns a PreparedStatement ready to be executed, with any parameter values 
     * already set.
     */
    public final PreparedStatement prepareStatement(Connection connection) throws SQLException {
        final PreparedStatement statement = connection.prepareStatement(this.getQuery());
        this.fillStatement(statement);
        return statement;
    }

    protected String createPlaceholdersList(int count) {
        final StringBuffer buffer = new StringBuffer();
        buffer.append("(");
        for (int i = 0; i < count; i++) {
            buffer.append("?");
            if ((i + 1) < count) { buffer.append(", "); }
        }
        buffer.append(") ");
        return buffer.toString();
    }

}
