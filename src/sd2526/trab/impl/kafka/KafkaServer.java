package sd2526.trab.impl.kafka;

import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.rest.servers.AbstractRestServer;
import sd2526.trab.impl.rest.servers.RestMessagesResource;

import java.util.logging.Logger;

public class KafkaServer extends AbstractRestServer {

    public static final int PORT = 4567;

    private static final Logger Log = Logger.getLogger(KafkaServer.class.getName());

    private final ReplicationManager repManager;

    KafkaServer(boolean fresh) {
        super(Log, Messages.SERVICE_NAME, PORT);
        this.repManager = ReplicationManager.getInstance(fresh);
    }

    @Override
    protected void registerResources(ResourceConfig config) {
        config.registerInstances(new RestMessagesResource());
        config.register(new VersionHeaderHandler(repManager));
    }

    public static void main(String[] args) {
        boolean fresh = args.length == 0 || Boolean.parseBoolean(args[0]);
        new KafkaServer(fresh).start();
    }
}
