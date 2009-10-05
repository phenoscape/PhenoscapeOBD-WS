package org.obd.ws.application;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.bbop.dataadapter.DataAdapterException;
import org.obd.query.impl.OBDSQLShard;
import org.obd.ws.exceptions.PhenoscapeDbConnectionException;
import org.obd.ws.util.Queries;
import org.obd.ws.util.TTOTaxonomy;
import org.restlet.Application;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.Router;

public class OBDApplication extends Application {

	private Queries queries;
	private OBDSQLShard obdsql;
	/* Database connection parameters */
	private String selectedDatabaseName,dbHost,uid,pwd;
	
	/** A structure which maps ontology prefixes to their 
	 * default namespaces */
	private Map<String, Set<String>> prefixToDefaultNamespacesMap;
	/** A structure to map default namespaces of ontologies to their
	 * node ids in the database */
	private Map<String, String> defaultNamespaceToNodeIdMap;

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

	/* Some static Strings */
	public static final String PREFIX_TO_NS_FILE = 
		"PrefixToDefaultNamespaceOfOntology.properties";
	public static final String PREFIX_TO_DEFAULT_NAMESPACE_MAP_STRING = 
		"prefixToDefaultNamespacesMap";
	public static final String DEFAULT_NAMESPACE_TO_SOURCE_ID_MAP_STRING = 
		"defaultNamespacesToSourceIdMap";
	public static final String TTO_TAXONOMY_STRING = "ttoTaxonomy";
	public static final String QUERIES_STRING = "queries";
	public static final String SHARD_STRING = "shard";
	public static final String SELECTED_DATABASE_NAME_STRING = "selectedDatabaseName";
	public static final String DB_HOST_NAME_STRING = "dbHost";
	public static final String UID_STRING = "uid";
	public static final String PWD_STRING = "pwd";

	/**
	 * Constructor extends the default superclass constructor
	 * @param context
	 */
	public OBDApplication(Context context){
        super(context);
    }

	/**
	 * Selects the Shard pointing to the most recently updated database to be used by the 
	 * data services
	 * Then this method sets a number of context level parameters
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws ParseException
	 * @throws PhenoscapeDbConnectionException
	 * @throws DataAdapterException
	 */
    private void connect() throws SQLException, ClassNotFoundException, IOException, ParseException, 
    				PhenoscapeDbConnectionException, DataAdapterException{
    	
    	DatabaseToggler dbToggler = new DatabaseToggler();
    	this.prefixToDefaultNamespacesMap = new HashMap<String, Set<String>>();
    	this.defaultNamespaceToNodeIdMap = new HashMap<String, String>();
        
    	obdsql = dbToggler.chooseDatabase();
        selectedDatabaseName = dbToggler.getSelectedDatabaseName();
        dbHost = dbToggler.getDbHost();
        uid = dbToggler.getUid();
        pwd = dbToggler.getPwd();
        
        if(obdsql != null && selectedDatabaseName != null && dbHost != null && uid != null && pwd != null){
        	this.getContext().getAttributes().put(SHARD_STRING, obdsql);
        	this.getContext().getAttributes().put(SELECTED_DATABASE_NAME_STRING, selectedDatabaseName);
        	this.getContext().getAttributes().put(DB_HOST_NAME_STRING, dbHost);
        	this.getContext().getAttributes().put(UID_STRING, uid);
        	this.getContext().getAttributes().put(PWD_STRING, pwd);
        	queries = new Queries(obdsql);
        	this.getContext().getAttributes().put(QUERIES_STRING, queries);
        	this.constructPrefixToDefaultNamespacesMap();
        	this.constructDefaultNamespaceToNodeIdMap();
        	this.getContext().getAttributes().put(PREFIX_TO_DEFAULT_NAMESPACE_MAP_STRING, this.prefixToDefaultNamespacesMap);
        	this.getContext().getAttributes().put(DEFAULT_NAMESPACE_TO_SOURCE_ID_MAP_STRING, this.defaultNamespaceToNodeIdMap);
        }else
        	throw new PhenoscapeDbConnectionException("Failed to obtain a connection to the database. " +
        			"This is because neither database is ready to be queried. ");
        
        TTOTaxonomy ttoTaxonomy = new TTOTaxonomy();
        this.getContext().getAttributes().put(TTO_TAXONOMY_STRING, ttoTaxonomy);
    }

    /**
     * The router method. 
     * It holds mappings from URL patterns to the appropriate REST service to be invoked
     */
    public Restlet createRoot() {
        try {
				connect();
        } catch (SQLException e) {
            log().fatal("Error connecting to SQL shard", e);
        } catch (ClassNotFoundException e) {
            log().fatal("Error creating SQL shard", e);
        } catch (IOException e) {
            log().fatal("Error reading connection properties file", e);
        } catch (ParseException e) {
        	log().fatal("Error parsing the date", e);
        } catch (PhenoscapeDbConnectionException e) {
        	log().fatal("Error with the database connection", e);
        } catch (DataAdapterException e) {
        	log().fatal("Error reading in the OBO files", e);
		}
        
        final Router router = new Router(this.getContext());
        // URL mappings
        router.attach("/phenotypes", org.obd.ws.resources.PhenotypeDetailsResource.class);
        router.attach("/phenotypes/summary", org.obd.ws.resources.PhenotypeSummaryResource.class);
        router.attach("/phenotypes/source/{annotation_id}", org.obd.ws.resources.AnnotationResource.class);
        router.attach("/term/search", org.obd.ws.resources.AutoCompleteResource.class);
        router.attach("/term/{termID}", org.obd.ws.resources.TermResource.class);
        router.attach("/term/{termID}/homology", org.obd.ws.resources.HomologyResource.class);
        router.attach("/timestamp", org.obd.ws.resources.KbRefreshTimestampResource.class);
        return router;
    }
    
    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

    /**
	 * PURPOSE This method reads in the list of default namespaces from a file and
	 * adds the corresponding node ids to a map
	 * @throws IOException
	 * @throws SQLException 
	 */
	private void constructDefaultNamespaceToNodeIdMap() throws SQLException{
		String sourceNodeQuery = queries.getQueryForNodeIdsForOntologies();
		String nodeId, uid;
		Connection conn = obdsql.getConnection();
		java.sql.Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sourceNodeQuery);
		while(rs.next()){
			nodeId = rs.getString(1);
			uid = rs.getString(2);
			if(uid.length() > 0){
				this.defaultNamespaceToNodeIdMap.put(uid, nodeId);
			}
		}
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
	private void constructPrefixToDefaultNamespacesMap() 
										throws IOException{
		InputStream inStream = 
			this.getClass().getResourceAsStream(PREFIX_TO_NS_FILE);
		Properties props = new Properties();
		props.load(inStream);
		Set<String> namespaceSet;
		for(Object key : props.keySet()){
			String prefix = key.toString();
			String commaDelimitedNamespaces = props.get(key).toString();
			namespaceSet = new HashSet<String>();
			for(String namespace : commaDelimitedNamespaces.split(",")){
				namespaceSet.add(namespace);
			}
			prefixToDefaultNamespacesMap.put(prefix, namespaceSet);
		}
	}
}
