package org.phenoscape.obd.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * QueryExecutor encapsulates proper closing of open SQL statements and connections 
 * after executing a query in which SQLExceptions may be thrown.
 * @param <T> The type of object returned when processing the query result.
 */
public abstract class QueryExecutor<T> {

    private final DataSource dataSource;
    private final QueryBuilder builder;

    /**
     * @param dataSource The DataSource from which to obtain the database Connection.
     * @param builder The query to be executed.
     */
    public QueryExecutor(DataSource dataSource, QueryBuilder builder) {
        this.dataSource = dataSource;
        this.builder = builder;
    }

    /**
     * Must be overridden by subclasses to process the ResultSet returned 
     * by the SQL query. The ResultSet does not need to be closed - this is handled
     * by the QueryExecutor.executeQuery() implementation.
     */
    public abstract T processResult(ResultSet result) throws SQLException;

    /**
     * Execute the SQL query and return the processed results. This method 
     * will close the open Connection and Statements.
     */
    public final T executeQuery() throws SQLException {
        Connection connection = null;
        try {
            connection = this.dataSource.getConnection();
            PreparedStatement statement = null;
            ResultSet result = null;
            try {
                statement = builder.prepareStatement(connection);
                result = statement.executeQuery();
                return this.processResult(result);
            } finally {
                if (statement != null) { statement.close(); }
            }
        } finally {
            if (connection != null) { connection.close(); }
        } 
    }

}
