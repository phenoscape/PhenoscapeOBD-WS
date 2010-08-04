package org.phenoscape.obd.sparql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.sql.DataSource;

import name.levering.ryan.sparql.common.GraphStatement;
import name.levering.ryan.sparql.common.RdfSource;
import name.levering.ryan.sparql.common.impl.StatementImpl;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.phenoscape.obd.query.QueryBuilder;
import org.phenoscape.obd.query.QueryExecutor;

public class OBDRDFSource implements RdfSource {

    private static final List<GraphStatement> empty = Collections.emptyList();
    private final DataSource dataSource;

    public OBDRDFSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Iterator<GraphStatement> getDefaultStatements(Value subject, URI predicate, Value object) {
        return this.getStatements(subject, predicate, object);
    }

    @Override
    public Iterator<GraphStatement> getStatements(Value subject, URI predicate, Value object) {
        if ((subject == null) || (subject instanceof URI)) {
            final String subjectUID = uriToUID((URI)subject);
            final String predicateUID = uriToUID(predicate);
            if (object == null) {
                final List<GraphStatement> links = this.getLinkStatements(subjectUID, predicateUID, null);
                links.addAll(this.getLiteralStatements(subjectUID, predicateUID, null));
                return links.iterator();
            } else if (object instanceof URI) {
                final String objectUID = uriToUID((URI)object);
                return this.getLinkStatements(subjectUID, predicateUID, objectUID).iterator();
            } else {
                return this.getLiteralStatements(subjectUID, predicateUID, object.toString()).iterator();
            }
        } else {
            return empty.iterator();
        }
    }

    @Override
    public Iterator<GraphStatement> getStatements(Value subject, URI predicate, Value object, URI graph) {
        // TODO ignoring graph for now
        return this.getStatements(subject, predicate, object);
    }

    private List<GraphStatement> getLinkStatements(String subject, String predicate, String object) {
        log().debug(String.format("S:%s P:%s O:%s", subject, predicate, object));
        final QueryBuilder query = new SPARQLLinkQueryBuilder(subject, predicate, object);
        try {
            return (new QueryExecutor<List<GraphStatement>>(this.dataSource, query) {
                @Override
                public List<GraphStatement> processResult(final ResultSet result) throws SQLException {
                    final List<GraphStatement> statements = new ArrayList<GraphStatement>();
                    while (result.next()) {
                        statements.add(translate(result));
                    }
                    return statements;
                }
            }).executeQuery();
        } catch (SQLException e) {
            log().error("Error querying database", e);
            return empty;
        }
    }

    private GraphStatement translate(ResultSet row) throws SQLException {
        return new StatementImpl(uidToURI(row.getString("subject_uid")), uidToURI(row.getString("predicate_uid")), uidToURI(row.getString("object_uid")));
    }

    private List<GraphStatement> getLiteralStatements(String subject, String predicate, String object) {
        log().debug(String.format("S:%s P:%s O:%s", subject, predicate, object));
        if (predicate != null) {
            if (predicate.equals("OBO:name")) {
                
            } else if (predicate.equals("OBO:def")) {
                
            } else if (predicate.equals("OBO:comment")) {
                
            } else if (predicate.equals("OBO:subset")) {
                
            } else if (predicate.equals("OBO:synonym")) {
                
            }
        }
        return empty;
    }

    @Override
    public ValueFactory getValueFactory() {
        return new ValueFactoryImpl();
    }

    @Override
    public boolean hasDefaultStatement(Value subject, URI predicate, Value object) {
        return this.getDefaultStatements(subject, predicate, object).hasNext();
    }

    @Override
    public boolean hasStatement(Value subject, URI predicate, Value object) {
        return this.getStatements(subject, predicate, object).hasNext();
    }

    @Override
    public boolean hasStatement(Value subject, URI predicate, Value object, URI graph) {
        return this.getStatements(subject, predicate, object, graph).hasNext();
    }

    public static String uriToUID(URI uri) {
        return uri == null ? null : uri.toString();
    }

    public static URI uidToURI(String uid) {
        return new URIImpl(uid);
    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
