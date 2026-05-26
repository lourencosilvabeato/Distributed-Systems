package sd2526.trab.impl.kafka;

import sd2526.trab.api.Message;

import java.util.Set;

public record ReplicatedOp(
        Type type,
        String name,
        String mid,
        Message msg,
        Set<String> knownLocalRecipients,
        Set<String> unknownLocalRecipients,
        Set<String> destinations,
        String msgOriginId,
        String requestId,
        Long sid
) {
    public enum Type {
        POST_MESSAGE, DELETE_MESSAGE, REMOVE_INBOX_MESSAGE,
        REMOTE_POST_MESSAGE, REMOTE_DELETE_MESSAGE, REMOTE_DELETE_USER_INBOX
    }

    static ReplicatedOp postMessage(Message msg, String originId, Set<String> known, Set<String> unknown, Set<String> destinations) {
        return new ReplicatedOp(Type.POST_MESSAGE, null, null, msg, known, unknown, destinations, originId, null, null);
    }

    static ReplicatedOp removeInboxMessage(String name, String mid) {
        return new ReplicatedOp(Type.REMOVE_INBOX_MESSAGE, name, mid, null, null, null, null, null, null, null);
    }

    static ReplicatedOp deleteMessage(String mid, Set<String> destinations) {
        return new ReplicatedOp(Type.DELETE_MESSAGE, null, mid, null, null, null, destinations, null, null, null);
    }

    static ReplicatedOp remotePostMessage(Message msg, Set<String> known, Set<String> unknown, Long sid) {
        return new ReplicatedOp(Type.REMOTE_POST_MESSAGE, null, null, msg, known, unknown, null, null, null, sid);
    }

    static ReplicatedOp remoteDeleteMessage(String mid, Long sid) {
        return new ReplicatedOp(Type.REMOTE_DELETE_MESSAGE, null, mid, null, null, null, null, null, null, sid);
    }

    static ReplicatedOp remoteDeleteUserInbox(String name) {
        return new ReplicatedOp(Type.REMOTE_DELETE_USER_INBOX, name, null, null, null, null, null, null, null, null);
    }

    ReplicatedOp withRequestId(String reqId) {
        return new ReplicatedOp(type, name, mid, msg, knownLocalRecipients, unknownLocalRecipients,
                destinations, msgOriginId, reqId, sid);
    }
}
