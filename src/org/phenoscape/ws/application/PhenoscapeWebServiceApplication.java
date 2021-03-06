package org.phenoscape.ws.application;

import java.net.MalformedURLException;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.phenoscape.obd.sparql.SPARQLResource;
import org.phenoscape.ws.resource.AttributeQualitiesResource;
import org.phenoscape.ws.resource.AutocompleteResource;
import org.phenoscape.ws.resource.BulkTermNameResource;
import org.phenoscape.ws.resource.DistinctPhenotypesResource;
import org.phenoscape.ws.resource.GeneAnnotationsResource;
import org.phenoscape.ws.resource.GeneAnnotationsSourceResource;
import org.phenoscape.ws.resource.GenesResource;
import org.phenoscape.ws.resource.KBTimestampResource;
import org.phenoscape.ws.resource.PathToRootResource;
import org.phenoscape.ws.resource.PhenotypeVariationResource;
import org.phenoscape.ws.resource.PhenotypesFacetResource;
import org.phenoscape.ws.resource.ProfileMatchResource;
import org.phenoscape.ws.resource.PublicationMatrixResource;
import org.phenoscape.ws.resource.PublicationOTUsResource;
import org.phenoscape.ws.resource.PublicationTermResource;
import org.phenoscape.ws.resource.PublicationsResource;
import org.phenoscape.ws.resource.TaxaResource;
import org.phenoscape.ws.resource.TaxonAnnotationSourceResource;
import org.phenoscape.ws.resource.TaxonAnnotationsResource;
import org.phenoscape.ws.resource.TaxonTermResource;
import org.phenoscape.ws.resource.TermInfoResource;
import org.phenoscape.ws.resource.report.AnnotatedCharacterCountsResource;
import org.phenoscape.ws.resource.report.AnnotatedCharacterStateCountsResource;
import org.phenoscape.ws.resource.report.CuratedGenotypeAnnotationCountsResource;
import org.phenoscape.ws.resource.report.CuratedPhenotypeCountsResource;
import org.phenoscape.ws.resource.report.CuratedTaxonAnnotationCountsResource;
import org.phenoscape.ws.resource.report.DistinctGeneAnnotationCountsResource;
import org.phenoscape.ws.resource.report.DistinctGenePhenotypeCountsResource;
import org.phenoscape.ws.resource.report.DistinctPhenotypeCountsResource;
import org.phenoscape.ws.resource.report.DistinctTaxonAnnotationCountsResource;
import org.phenoscape.ws.resource.report.GeneCountsResource;
import org.phenoscape.ws.resource.report.KBStatisticsResource;
import org.phenoscape.ws.resource.report.OTUCountsResource;
import org.phenoscape.ws.resource.report.PublicationCountsResource;
import org.phenoscape.ws.resource.report.PublishedCharacterCountsResource;
import org.phenoscape.ws.resource.report.PublishedCharacterStateCountsResource;
import org.phenoscape.ws.resource.report.TaxonCountsResource;
import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.routing.Router;
import org.restlet.routing.Template;

public class PhenoscapeWebServiceApplication extends Application {

    private static final String JNDI_KEY = "java:/comp/env/jdbc/OBD";
    public static final String DATA_SOURCE_KEY = "org.phenoscape.jndi.obd.datasource";
    private static final String SOLR_SERVER_PROPERTY = "org.phenoscape.obd.solr-uri";
    public static final String SOLR_SERVER_KEY = "org.phenoscape.obd.solrserver";

    /**
     * Create a router holding mappings from URL patterns to the appropriate REST service to be invoked.
     */
    @Override
    public Restlet createInboundRoot() {
        this.initializeDataSource();
        this.initializeSolrServer();
        final Router router = new Router(this.getContext());
        // URL mappings
        router.attach("/term/taxon/{termID}", TaxonTermResource.class);
        router.attach("/term/publication/{publicationID}/matrix", PublicationMatrixResource.class);
        router.attach("/term/publication/{publicationID}/otus", PublicationOTUsResource.class);
        router.attach("/term/publication/{publicationID}", PublicationTermResource.class);
        router.attach("/term/search", AutocompleteResource.class);
        router.attach("/term/names", BulkTermNameResource.class);
        router.attach("/term/attributes", AttributeQualitiesResource.class);
        router.attach("/term/{termID}/path", PathToRootResource.class);
        router.attach("/term/{termID}", TermInfoResource.class);
        router.attach("/timestamp", KBTimestampResource.class).setMatchingMode(Template.MODE_STARTS_WITH);
        router.attach("/annotation/gene", GeneAnnotationsResource.class);
        router.attach("/annotation/gene/source", GeneAnnotationsSourceResource.class);
        router.attach("/annotation/taxon/distinct", TaxonAnnotationsResource.class);
        router.attach("/annotation/taxon/source", TaxonAnnotationSourceResource.class);
        router.attach("/taxon/annotated", TaxaResource.class);
        router.attach("/gene/annotated", GenesResource.class);
        router.attach("/phenotype/profile", ProfileMatchResource.class);
        router.attach("/phenotype/variationsets", PhenotypeVariationResource.class);
        router.attach("/phenotype/facet/{facet}", PhenotypesFacetResource.class);
        router.attach("/phenotype", DistinctPhenotypesResource.class);
        router.attach("/publication/annotated", PublicationsResource.class);
        router.attach("/report/count/publications", PublicationCountsResource.class);
        router.attach("/report/count/characters/published", PublishedCharacterCountsResource.class);
        router.attach("/report/count/characters/annotated", AnnotatedCharacterCountsResource.class);
        router.attach("/report/count/characterstates/published", PublishedCharacterStateCountsResource.class);
        router.attach("/report/count/characterstates/annotated", AnnotatedCharacterStateCountsResource.class);
        router.attach("/report/count/taxa/annotated", TaxonCountsResource.class);
        router.attach("/report/count/otus/published", OTUCountsResource.class);
        router.attach("/report/count/phenotypes/curated", CuratedPhenotypeCountsResource.class);
        router.attach("/report/count/phenotypes/annotated", DistinctPhenotypeCountsResource.class);
        router.attach("/report/count/annotations/taxa/curated", CuratedTaxonAnnotationCountsResource.class);
        router.attach("/report/count/annotations/taxa/distinct", DistinctTaxonAnnotationCountsResource.class);
        router.attach("/report/count/genes/annotated", GeneCountsResource.class);
        router.attach("/report/count/annotations/genotypes/curated", CuratedGenotypeAnnotationCountsResource.class);
        router.attach("/report/count/annotations/genes/distinct", DistinctGeneAnnotationCountsResource.class);
        router.attach("/report/count/phenotypes/genes/annotated", DistinctGenePhenotypeCountsResource.class);
        router.attach("/statistics", KBStatisticsResource.class).setMatchingMode(Template.MODE_STARTS_WITH);
        router.attach("/sparql", SPARQLResource.class);
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
    
    private void initializeSolrServer() {
        try {
            final String solrURI = System.getProperty(SOLR_SERVER_PROPERTY);
            //final String solrURI = "http://localhost:8983/solr/";
            final SolrServer solr = new CommonsHttpSolrServer(solrURI);
            this.getContext().getAttributes().put(SOLR_SERVER_KEY, solr);
        } catch (MalformedURLException e) {
            log().fatal("Unable to configure Solr server", e);
        }
    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
