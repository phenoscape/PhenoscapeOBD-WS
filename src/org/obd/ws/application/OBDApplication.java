package org.obd.ws.application;

import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.Router;

public class OBDApplication extends Application {

    @Override
    public Restlet createRoot() {
        // TODO Auto-generated method stub
        final Router router = new Router(this.getContext());
        // URL mappings
        return router;
    }

}
