package org.phenoscape.ws.application;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.phenoscape.ws.resource.AutocompleteResource;
import org.phenoscape.ws.resource.GeneAnnotationsResource;
import org.phenoscape.ws.resource.KBStatisticsResource;
import org.phenoscape.ws.resource.KBTimestampResource;
import org.phenoscape.ws.resource.TaxonTermResource;
import org.phenoscape.ws.resource.TermInfoResource;
import org.phenoscape.ws.resource.report.PublicationCountsResource;
import org.phenoscape.ws.resource.statistics.CharactersAndGenesByAttribute;
import org.phenoscape.ws.resource.statistics.CharactersAndGenesBySystem;
import org.phenoscape.ws.resource.statistics.CharactersAndGenesBySystemAndClade;
import org.phenoscape.ws.resource.statistics.CharactersDatasetsAndTaxaByClade;
import org.phenoscape.ws.resource.statistics.reports.DataConsistencyReportGeneratorForQuestion13;
import org.phenoscape.ws.resource.statistics.reports.DataConsistencyReportGeneratorForQuestion21A;
import org.phenoscape.ws.resource.statistics.reports.DataConsistencyReportGeneratorForQuestion21B;
import org.phenoscape.ws.resource.statistics.reports.DataConsistencyReportGeneratorForQuestion9;
import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.routing.Router;
import org.restlet.routing.Template;

public class PhenoscapeWebServiceApplication extends Application {

    private static final String JNDI_KEY = "java:/comp/env/jdbc/OBD";
    public static final String DATA_SOURCE_KEY = "org.phenoscape.jndi.obd.datasource";

    /**
     * Create a router holding mappings from URL patterns to the appropriate REST service to be invoked.
     */
    @Override
    public Restlet createInboundRoot() {
        this.initializeDataSource();
        final Router router = new Router(this.getContext());
        // URL mappings
        router.attach("/term/taxon/{termID}", TaxonTermResource.class);
        router.attach("/term/search", AutocompleteResource.class);
        router.attach("/term/{termID}", TermInfoResource.class);
        router.attach("/timestamp", KBTimestampResource.class).setMatchingMode(Template.MODE_STARTS_WITH);
        router.attach("/annotation/gene", GeneAnnotationsResource.class);
        router.attach("/report/count/publications", PublicationCountsResource.class);
        // These resources generate data consistency reports
        router.attach("/statistics/consistencyreports/relationalqualitieswithoutrelatedentities", DataConsistencyReportGeneratorForQuestion21A.class);
        router.attach("/statistics/consistencyreports/nonrelationalqualitieswithrelatedentities", DataConsistencyReportGeneratorForQuestion21B.class);
        router.attach("/statistics/consistencyreports/characterswithonlyoneannotatedstate", DataConsistencyReportGeneratorForQuestion9.class);
        router.attach("/statistics/consistencyreports/characterswithonlyoneoftwopossibleannotations", DataConsistencyReportGeneratorForQuestion13.class);
        // These resources generate summary statistics of the data
        router.attach("/statistics/countsofgenesandcharactersbyattribute", CharactersAndGenesByAttribute.class);
        router.attach("/statistics/countsofgenesandcharactersbysystem", CharactersAndGenesBySystem.class);
        router.attach("/statistics/countsofgenesandcharactersbysystemandclade", CharactersAndGenesBySystemAndClade.class);
        router.attach("/statistics/countsofcharactersdatasetsandtaxabyclade", CharactersDatasetsAndTaxaByClade.class);
        router.attach("/statistics", KBStatisticsResource.class).setMatchingMode(Template.MODE_STARTS_WITH);
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

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
