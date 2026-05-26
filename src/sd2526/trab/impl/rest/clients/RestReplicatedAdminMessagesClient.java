package sd2526.trab.impl.rest.clients;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.impl.api.java.ReplicatedAdminMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;

public class RestReplicatedAdminMessagesClient extends RestAdminMessagesClient implements ReplicatedAdminMessages {

    public RestReplicatedAdminMessagesClient(String serverURI) {
        super(serverURI);
    }

    @Override
    public Result<Void> remotePostMessage(Message m, Long sid) {
        return super.reTry(() -> doRemotePostMessageSid(m, sid));
    }

    @Override
    public Result<Void> remoteDeleteMessage(String mid, Long sid) {
        return super.reTry(() -> doRemoteDeleteMessageSid(mid, sid));
    }

    private Result<Void> doRemotePostMessageSid(Message msg, Long sid) {
        var req = target.path(RestAdminMessages.ADMIN);
        if (sid != null) req = req.queryParam("sid", sid);
        return super.toJavaResult(req.request().post(Entity.entity(msg, MediaType.APPLICATION_JSON)));
    }

    private Result<Void> doRemoteDeleteMessageSid(String mid, Long sid) {
        var req = target.path(RestAdminMessages.ADMIN).path(mid);
        if (sid != null) req = req.queryParam("sid", sid);
        return super.toJavaResult(req.request().delete());
    }
}
