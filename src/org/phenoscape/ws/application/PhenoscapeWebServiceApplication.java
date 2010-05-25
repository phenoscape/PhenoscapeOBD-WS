package org.phenoscape.ws.application;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.bbop.dataadapter.DataAdapterException;
import org.obd.query.impl.OBDSQLShard;
import org.phenoscape.util.Queries;
import org.phenoscape.ws.resource.TaxonTermResource;
import org.phenoscape.ws.resource.TermInfoResource;
import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.routing.Router;
import org.restlet.routing.Template;

public class PhenoscapeWebServiceApplication extends Application {

    private Queries queries;
    /** A structure which maps ontology prefixes to their 
     * default namespaces */
    final private Map<String, Set<String>> prefixToDefaultNamespacesMap = new HashMap<String, Set<String>>();
    /** A structure to map default namespaces of ontologies to their
     * node ids in the database */
    final private Map<String, String> defaultNamespaceToNodeIdMap = new HashMap<String, String>();
    
    /** GETTER for the map from default namespaces of ontologies 
     * to their node ids in the database */
    public Map<String, String> getDefaultNamespaceToNodeIdMap() {
        return defaultNamespaceToNodeIdMap;
    }
    /** GETTER for the map from ontology prefixes to default namespaces
     */
    public Map<String, Set<String>> getPrefixToDefaultNamespacesMap() {
        return prefixToDefaultNamespacesMap;
    }

    private static final String JNDI_KEY = "java:/comp/env/jdbc/OBD";
    public static final String DATA_SOURCE_KEY = "org.phenoscape.jndi.obd.datasource";
    public static final String PREFIX_TO_NS_FILE = "PrefixToDefaultNamespaceOfOntology.properties";
    public static final String PREFIX_TO_DEFAULT_NAMESPACE_MAP_STRING = "prefixToDefaultNamespacesMap";
    public static final String DEFAULT_NAMESPACE_TO_SOURCE_ID_MAP_STRING = "defaultNamespacesToSourceIdMap";
    public static final String QUERIES_STRING = "queries";  

    /**
     * Selects the Shard pointing to the most recently updated database to be used by the 
     * data services
     * Then this method sets a number of context level parameters
     * @throws IOException 
     * @throws SQLException 
     * @throws DataAdapterException 
     * @throws ClassNotFoundException 
     */
    private void connect() throws IOException, SQLException, DataAdapterException, ClassNotFoundException {
        //TODO this method should probably be removed or significantly revised
        final OBDSQLShard shard = new OBDSQLShard();
        shard.connect((DataSource)(this.getContext().getAttributes().get(DATA_SOURCE_KEY)));
        queries = new Queries(shard);
        this.getContext().getAttributes().put(QUERIES_STRING, queries);
        this.constructPrefixToDefaultNamespacesMap();
        this.constructDefaultNamespaceToNodeIdMap();
        this.getContext().getAttributes().put(PREFIX_TO_DEFAULT_NAMESPACE_MAP_STRING, this.prefixToDefaultNamespacesMap);
        this.getContext().getAttributes().put(DEFAULT_NAMESPACE_TO_SOURCE_ID_MAP_STRING, this.defaultNamespaceToNodeIdMap);
    }

    /**
     * The router method. 
     * It holds mappings from URL patterns to the appropriate REST service to be invoked
     */
    @Override
    public Restlet createInboundRoot() {
        this.initializeDataSource();
        try {
            connect();
        } catch (SQLException e) {
            log().fatal("Error connecting to SQL shard", e);
        } catch (IOException e) {
            log().fatal("Error reading connection properties file", e);
        } catch (DataAdapterException e) {
            log().fatal("Error reading in the OBO files", e);
        } catch (ClassNotFoundException e) {
            log().fatal("Error connecting to SQL shard", e);
        }
        final Router router = new Router(this.getContext());
        // URL mappings
        router.attach("/term/taxon/{termID}", TaxonTermResource.class);
        router.attach("/term/search", org.obd.ws.resources.AutoCompleteResource.class);
        router.attach("/term/{termID}", TermInfoResource.class);
        router.attach("/timestamp", org.obd.ws.resources.KBTimestampResource.class).setMatchingMode(Template.MODE_STARTS_WITH);
        router.attach("/taxon/{taxonID}/treemap/",org.obd.ws.resources.SquarifiedTaxonMapResource.class);
        /* These resources generate data consistency reports*/
        router.attach("/statistics/consistencyreports/relationalqualitieswithoutrelatedentities",
                org.phenoscape.ws.resource.statistics.reports.DataConsistencyReportGeneratorForQuestion21A.class);
        router.attach("/statistics/consistencyreports/nonrelationalqualitieswithrelatedentities",
                org.phenoscape.ws.resource.statistics.reports.DataConsistencyReportGeneratorForQuestion21B.class);
        router.attach("/statistics/consistencyreports/characterswithonlyoneannotatedstate",
                org.phenoscape.ws.resource.statistics.reports.DataConsistencyReportGeneratorForQuestion9.class);
        router.attach("/statistics/consistencyreports/characterswithonlyoneoftwopossibleannotations",
                org.phenoscape.ws.resource.statistics.reports.DataConsistencyReportGeneratorForQuestion13.class);
        /* These resources generate summary statistics of the data */
        router.attach("/statistics/countsofgenesandcharactersbyattribute",
                org.phenoscape.ws.resource.statistics.CharactersAndGenesByAttribute.class);
        router.attach("/statistics/countsofgenesandcharactersbysystem",
                org.phenoscape.ws.resource.statistics.CharactersAndGenesBySystem.class);
        router.attach("/statistics/countsofgenesandcharactersbysystemandclade",
                org.phenoscape.ws.resource.statistics.CharactersAndGenesBySystemAndClade.class);
        router.attach("/statistics/countsofcharactersdatasetsandtaxabyclade",
                org.phenoscape.ws.resource.statistics.CharactersDatasetsAndTaxaByClade.class);
        return router;
    }
    
    private void initializeDataSource() {
        try {
            final InitialContext initialContext = new InitialContext();
            final DataSource dataSource = (DataSource)(initialContext.lookup(JNDI_KEY));
            this.getContext().getAttributes().put(DATA_SOURCE_KEY, dataSource);
        } catch (NamingException e) {
            log().fatal("Unable to configure database connection via JNDI", e);
        }
    }

    /**
     * PURPOSE This method reads in the list of default namespaces from a file and
     * adds the corresponding node ids to a map
     */
    private void constructDefaultNamespaceToNodeIdMap() throws SQLException {
        final String sourceNodeQuery = queries.getQueryForNodeIdsForOntologies();
        String nodeId, uid;
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = this.createConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sourceNodeQuery);
            while(rs.next()){
                nodeId = rs.getString(1);
                uid = rs.getString(2);
                if (uid.length() > 0) {
                    this.defaultNamespaceToNodeIdMap.put(uid, nodeId);
                }
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            if (rs != null) { rs.close(); }
            if (stmt != null) { stmt.close(); }
            if (conn != null) { conn.close(); }
        }
    }
    
    private Connection createConnection() throws SQLException {
        return ((DataSource)(this.getContext().getAttributes().get(PhenoscapeWebServiceApplication.DATA_SOURCE_KEY))).getConnection();
    }

    /**
     * PURPOSE This method constructs a mapping
     * from every prefix used in the autocompletion
     * service to the set of default namespaces of the
     * ontologies the prefix comes from\n
     * PROCEDURE This method reads the allowed 
     * prefix to namespace mappings from a static text 
     * file. This is converted into a map
     * @throws IOException
     */
    private void constructPrefixToDefaultNamespacesMap() throws IOException {
        final Properties props = new Properties();
        props.load(new StringReader(
                "oboInOwl=oboInOwl,oboInOwl:Subset\n" +
                "OBO_REL=relationship\n" +
                "PATO=quality,pato.ontology\n" +
                "ZFA=zebrafish_anatomy\n" +
                "TAO=teleost_anatomy\n" +
                "TTO=teleost-taxonomy\n" +
                "COLLECTION=museum\n" +
                "BSPO=spatial\n" +
                "SO=sequence\n" +
                "UO=unit.ontology\n" +
                "PHENOSCAPE=phenoscape_vocab\n" +
                "GO=gene_ontology,biological_process,molecular_function,cellular_component\n" +
                "ECO=evidence_code2.obo\n"));
        for (Object key : props.keySet()) {
            final Set<String> namespaceSet = new HashSet<String>();
            final String prefix = key.toString();
            final String commaDelimitedNamespaces = props.get(key).toString();
            for(String namespace : commaDelimitedNamespaces.split(",")){
                namespaceSet.add(namespace);
            }
            prefixToDefaultNamespacesMap.put(prefix, namespaceSet);
        }
    }
    
    private Logger log() {
        return Logger.getLogger(this.getClass());
    }
}
