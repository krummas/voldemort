package voldemort.server.rest;

import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.log4j.Logger;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import voldemort.server.AbstractService;
import voldemort.server.ServiceType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortServer;
import voldemort.server.storage.StorageService;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: Feb 13, 2010 Time: 12:36:13 PM To change this template use File |
 * Settings | File Templates.
 */
public class RestService extends AbstractService {
    private final Logger logger = Logger.getLogger(RestService.class);
    private final VoldemortServer voldemortServer;
    private final StorageService storageService;
    private final StoreRepository storeRepository;
    public static final String REPOSITORY_KEY = "restrepository";
    public RestService(VoldemortServer server,
                       StorageService storageService,
                       StoreRepository storeRepository) {
        super(ServiceType.REST);
        this.voldemortServer = server;
        this.storageService = storageService;
        this.storeRepository = storeRepository;
        logger.info("Starting REST service.");
    }

    @Override
    protected void startInner() {
        ServletHolder servletHolder = new ServletHolder(ServletContainer.class);
        servletHolder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass",
                                       "com.sun.jersey.api.core.PackagesResourceConfig");
        servletHolder.setInitParameter("com.sun.jersey.config.property.packages", "voldemort.server.rest");
        Server server = new Server(9999);
        Context context = new Context(server, "/", Context.SESSIONS);
        context.setAttribute(RestService.REPOSITORY_KEY, storeRepository);
        context.addServlet(servletHolder, "/*");
        try {
            server.start();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    protected void stopInner() {
        logger.info("STOP INNER");
    }
}
