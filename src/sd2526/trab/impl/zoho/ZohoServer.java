package sd2526.trab.impl.zoho;

import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.rest.servers.AbstractRestServer;
import java.util.logging.Logger;

public class ZohoServer extends AbstractRestServer  {

    public static final int PORT = 4568;
    public final boolean flag;

    private static Logger Log = Logger.getLogger(ZohoServer.class.getName());

    ZohoServer(boolean fresh) {
        super(Log, Messages.SERVICE_NAME, PORT);
        this.flag = fresh;
    }

    @Override
    protected void registerResources(ResourceConfig config) {
        ZohoMessages.getInstance(flag);
        config.register(ZohoResource.class);
    }

    public static void main(String[] args) {
         boolean fresh = args.length > 0 && Boolean.parseBoolean(args[0]);
         new ZohoServer(fresh).start();
    }
}
