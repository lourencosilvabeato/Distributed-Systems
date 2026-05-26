package sd2526.trab.impl.kafka;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import sd2526.trab.api.rest.RestMessages;

import java.io.IOException;

@Provider
public class VersionHeaderHandler implements ContainerResponseFilter, ContainerRequestFilter {

    private final ReplicationManager repManager;

    public VersionHeaderHandler(ReplicationManager repManager) {
        this.repManager = repManager;
    }

    @Override
    public void filter(ContainerRequestContext reqCtx) throws IOException {

        String value = reqCtx.getHeaderString(RestMessages.HEADER_VERSION);
        if( value != null && ! value.isEmpty()) {
            version.set( Long.valueOf( value ) );
            }
        }

    @Override
    public void filter(ContainerRequestContext reqCtx, ContainerResponseContext resCtx) throws IOException {
        resCtx.getHeaders().add(RestMessages.HEADER_VERSION, Long.toString(repManager.getCurrentVersion()));
    }

    public static final ThreadLocal<Long> version = new ThreadLocal<>();
}

