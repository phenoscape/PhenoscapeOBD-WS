package org.obd.ws.statistics.reports.resources;

import java.sql.SQLException;

import org.obd.query.impl.OBDSQLShard;
import org.obd.ws.resources.AbstractOBDResource;
import org.restlet.Context;
import org.restlet.data.Request;
import org.restlet.data.Response;

public abstract class AbstractStatisticalReportGenerator extends AbstractOBDResource{

	protected final String RELATIONAL_SPATIAL_QUALITY_IDENTIFIER = "PATO:0001631";
	protected final String RELATIONAL_SHAPE_QUALITY_IDENTIFIER = "PATO:0001647";
	protected final String RELATIONAL_STRUCTURAL_QUALITY_IDENTIFIER = "PATO:0001452";
	protected final String SIZE_IDENTIFIER = "PATO:0000117";

    public AbstractStatisticalReportGenerator(Context context, Request request, Response response) {
        super(context, request, response);
        try {
            this.shard = new OBDSQLShard();
        } catch (SQLException e) {
            log().fatal("Failed to create shard", e);
        } catch (ClassNotFoundException e) {
            log().fatal("Failed to create shard", e);
        }
    }

}
