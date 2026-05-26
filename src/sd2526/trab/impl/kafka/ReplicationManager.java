package sd2526.trab.impl.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.impl.db.DB;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.java.servers.JavaMessages;
import sd2526.trab.impl.utils.IP;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import sd2526.trab.impl.api.java.ReplicatedAdminMessages;
import static sd2526.trab.api.java.Result.ErrorCode.FORBIDDEN;
import static sd2526.trab.api.java.Result.ErrorCode.INTERNAL_ERROR;
import static sd2526.trab.api.java.Result.*;

public class ReplicationManager extends JavaMessages implements Messages, ReplicatedAdminMessages {

    private final String topic;
    private final KafkaPublisher publisher;
    private final SyncPoint syncPoint;
    private final ConcurrentHashMap<String, CompletableFuture<Object>> pending = new ConcurrentHashMap<>();
    private final AtomicLong requestIdCounter = new AtomicLong(0);
    private final Gson gson = new Gson();
    private final int replicaIndex;
    private final ConcurrentHashMap<String, Long> lastSidByDomain = new ConcurrentHashMap<>();
    private static ReplicationManager instance;

    public static synchronized ReplicationManager getInstance() {
        return instance;
    }

    public static synchronized ReplicationManager getInstance(boolean fresh) {
        if (instance == null)
            instance = new ReplicationManager(fresh);
        return instance;
    }

    private static int extractReplicaIndex(String hostname) {
        return hostname.charAt("messages".length()) - '0';
    }

    private ReplicationManager(boolean fresh) {
        System.setProperty("hibernate.hbm2ddl.auto", fresh ? "create" : "update");

        JavaMessages.setInstance(this);

        this.replicaIndex = extractReplicaIndex(IP.hostname());
        this.topic = IP.domain();
        this.syncPoint = SyncPoint.getSyncPoint();

        KafkaUtils.createTopic(topic);
        this.publisher = KafkaPublisher.createPublisher(KafkaUtils.KAFKA_ADDR);

        KafkaSubscriber subscriber = KafkaSubscriber.createSubscriber(KafkaUtils.KAFKA_ADDR, List.of(topic));
        subscriber.start(this::onReceive);
    }

    public long getCurrentVersion() {
        return syncPoint.getVersion();
    }

    // request thread
    @SuppressWarnings("unchecked")
    private <T> Result<T> publish(ReplicatedOp op) {

        String reqId = IP.hostname() + ":" + requestIdCounter.getAndIncrement();
        ReplicatedOp tagged = op.withRequestId(reqId);
        CompletableFuture<Object> future = new CompletableFuture<>();
        pending.put(reqId, future);
        String json = gson.toJson(tagged);
        long offset = publisher.publish(topic, json);

        if (offset < 0) {
            pending.remove(reqId);
            return error(INTERNAL_ERROR);
        }

        try {
            return (Result<T>) future.get(30, TimeUnit.SECONDS);

        } catch (Exception e) {
            pending.remove(reqId);
            return error(INTERNAL_ERROR);
        }
    }

    // consumer thread
    private void onReceive(ConsumerRecord<String, String> r) {
        long offset = r.offset();
        ReplicatedOp op = null;

        try {
            op = gson.fromJson(r.value(), ReplicatedOp.class);
            Result<?> result = executeLocally(op, offset);
            syncPoint.setResult(offset, null);
            if (op.requestId() != null) {
                CompletableFuture<Object> future = pending.remove(op.requestId());
                if (future != null)
                    future.complete(result);
            }

        } catch (Exception e) {
            e.printStackTrace();
            syncPoint.setResult(offset, null);
            if (op != null && op.requestId() != null) {
                CompletableFuture<Object> future = pending.remove(op.requestId());
                if (future != null)
                    future.complete(error(INTERNAL_ERROR));
            }
        }
    }

    // operations
    private Result<?> executeLocally(ReplicatedOp op, long offset) {
        return switch (op.type()) {
            case POST_MESSAGE -> applyPostMessage(op, offset);
            case DELETE_MESSAGE -> applyDeleteMessage(op, offset);
            case REMOVE_INBOX_MESSAGE -> silentOk(dbRemoveInboxMessage(op.name(), op.mid()));
            case REMOTE_POST_MESSAGE -> applyRemotePostMessage(op);
            case REMOTE_DELETE_MESSAGE -> applyRemoteDeleteMessage(op);
            case REMOTE_DELETE_USER_INBOX -> silentOk(dbRemoteDeleteUserInbox(op.name()));
        };
    }

    // asssigns unique id to message, stores in cache and sends message to each destination.
    private Result<String> applyPostMessage(ReplicatedOp op, long offset) {
        Message msg = op.msg();
        msg.setId("%s+%04d".formatted(THIS_DOMAIN, offset));
        String originId = op.msgOriginId() != null ? op.msgOriginId() : msg.originId();

        // skip if already done
        var existing = getCachedMessage(msg.getId());
        if (!existing.isOK()) existing = DB.getOne(msg.getId(), Message.class);
        if (existing.isOK()) {
            messagesCache.put(originId, existing.value());
            messagesCache.put(msg.getId(), existing.value());
            return ok(msg.getId());
        }

        messagesCache.put(originId, msg);
        messagesCache.put(msg.getId(), msg);

        boolean hasLocalRecipients = op.knownLocalRecipients() != null && !op.knownLocalRecipients().isEmpty();
        if (hasLocalRecipients)
            deliverToKnownLocalRecipients(op.knownLocalRecipients(), msg);
        else
            DB.persistOne(msg);

        if (op.unknownLocalRecipients() != null && !op.unknownLocalRecipients().isEmpty())
            reportUnknownLocalRecipients(op.unknownLocalRecipients(), msg);

        if (op.destinations() != null) {
            final Message m = msg;
            final int idx = replicaIndex;
            final long sid = offset;

            // forward message to each remote domain that has recipients
            op.destinations().stream()
                .map(this::getDomain)
                .filter(d -> !isLocalDomain(d))
                .collect(Collectors.toSet())
                .forEach(domain ->
                    jobs.submit(domain, () ->
                        reTry(() -> Clients.ReplicatedAdminMessagesClient.get(domain, idx).remotePostMessage(m, sid),
                              REMOTE_COMM_DEADLINE)));
        }
        return ok(msg.getId());
    }

    // deletes message from each destination, idempotent function
    private Result<Void> applyDeleteMessage(ReplicatedOp op, long offset) {
        silentOk(deleteFromLocalInbox(op.mid()));

        if (op.destinations() != null) {
            final String mid = op.mid();
            final int idx = replicaIndex;
            final long sid = offset;

            // delete message from each remote domain
            op.destinations().stream()
                .map(this::getDomain)
                .filter(d -> !isLocalDomain(d))
                .collect(Collectors.toSet())
                .forEach(domain ->
                    jobs.submit(domain, () ->
                        reTry(() -> Clients.ReplicatedAdminMessagesClient.get(domain, idx).remoteDeleteMessage(mid, sid),
                              REMOTE_COMM_DEADLINE)));
        }
        return ok();
    }

    // delivers a message forwarded from a remote domain
    private Result<Void> applyRemotePostMessage(ReplicatedOp op) {
        Message msg = op.msg();

        if (op.sid() != null) {
            String src = sourceDomain(msg.getId());
            long current = lastSidByDomain.getOrDefault(src, -1L);
            if (op.sid() <= current) return ok();
            lastSidByDomain.put(src, op.sid());
        }

        var existing = getCachedMessage(msg.getId());
        if (!existing.isOK()) existing = DB.getOne(msg.getId(), Message.class);
        if (existing.isOK()) {
            messagesCache.put(msg.getId(), existing.value());
            return ok();
        }

        messagesCache.put(msg.getId(), msg);
        boolean hasLocalRecipients = op.knownLocalRecipients() != null && !op.knownLocalRecipients().isEmpty();
        if (hasLocalRecipients)
            deliverToKnownLocalRecipients(op.knownLocalRecipients(), msg);
        else
            DB.persistOne(msg);
        if (op.unknownLocalRecipients() != null && !op.unknownLocalRecipients().isEmpty())
            reportUnknownLocalRecipients(op.unknownLocalRecipients(), msg);
        return ok();
    }

    // removes a message forwarded as deleted from a remote domain
    private Result<Void> applyRemoteDeleteMessage(ReplicatedOp op) {
        if (op.sid() != null) {
            String src = sourceDomain(op.mid());
            long current = lastSidByDomain.getOrDefault(src, -1L);
            if (op.sid() <= current) return ok();
            lastSidByDomain.put(src, op.sid());
        }
        return silentOk(deleteFromLocalInbox(op.mid()));
    }

    private static String sourceDomain(String messageId) {
        int plus = messageId.indexOf('+');
        return plus >= 0 ? messageId.substring(0, plus) : messageId;
    }

    private <T> Result<T> silentOk(Result<T> r) {
        return (r != null && r.isOK()) ? r : ok();
    }

    //  publishes message so all replicas assign the same id
    @Override
    public Result<String> postMessage(String pwd, Message msg) {
        Result<User> userResult = getUser(msg.getSender(), pwd);
        if (!userResult.isOK())
            return error(userResult.error());

        var user = userResult.value();
        String originId = msg.originId();

        var cached = getCachedMessage(originId).mapValue(Message::getId);
        if (cached.isOK())
            return cached;

        msg.setSender("%s <%s@%s>".formatted(user.getDisplayName(), user.getName(), user.getDomain()));
        var localAddresses = msg.getDestination().stream()
            .filter(this::isLocalAddress).collect(Collectors.toList());

        Set<String> unknown = Set.of();
        Set<String> known = Set.of();
        if (!localAddresses.isEmpty()) {
            var check = checkUsers(localAddresses);
            unknown = check.isOK() ? check.value() : Set.of();
            known = new HashSet<>(localAddresses);
            known.removeAll(unknown);
        }

        return publish(ReplicatedOp.postMessage(msg, originId, known, unknown, new HashSet<>(msg.getDestination())));
    }

    @Override
    public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
        var userResult = getUser(name, pwd);
        if (!userResult.isOK())
            return error(userResult.error());
        return publish(ReplicatedOp.removeInboxMessage(name, mid));
    }

    @Override
    public Result<Void> deleteMessage(String name, String mid, String pwd) {
        var userResult = getUser(name, pwd);
        if (!userResult.isOK())
            return error(userResult.error());

        Result<Message> msgResult = getCachedMessage(mid);
        if (!msgResult.isOK()) msgResult = DB.getOne(mid, Message.class);
        if (!msgResult.isOK())
            return error(FORBIDDEN);
        Message msg = msgResult.value();

        if (!name.equals(msg.senderName()))
            return error(FORBIDDEN);

        return publish(ReplicatedOp.deleteMessage(mid, new HashSet<>(msg.getDestination())));
    }

    @Override
    public Result<Message> getInboxMessage(String name, String mid, String pwd) {
        Long clientVersion = VersionHeaderHandler.version.get();
        if (clientVersion != null)
            syncPoint.waitForVersion(clientVersion);
        return super.getInboxMessage(name, mid, pwd);
    }

    @Override
    public Result<List<String>> getAllInboxMessages(String name, String pwd) {
        Long clientVersion = VersionHeaderHandler.version.get();
        if (clientVersion != null)
            syncPoint.waitForVersion(clientVersion);
        return super.getAllInboxMessages(name, pwd);
    }

    @Override
    public Result<List<String>> searchInbox(String name, String pwd, String query) {
        Long clientVersion = VersionHeaderHandler.version.get();
        if (clientVersion != null)
            syncPoint.waitForVersion(clientVersion);
        return super.searchInbox(name, pwd, query);
    }

    @Override
    public Result<Void> remotePostMessage(Message m, Long sid) {
        var localAddresses = m.getDestination().stream()
            .filter(this::isLocalAddress).collect(Collectors.toList());

        Set<String> unknown = Set.of();
        Set<String> known = Set.of();
        if (!localAddresses.isEmpty()) {
            var check = checkUsers(localAddresses);
            unknown = check.isOK() ? check.value() : Set.of();
            known = new HashSet<>(localAddresses);
            known.removeAll(unknown);
        }

        return publish(ReplicatedOp.remotePostMessage(m, known, unknown, sid));
    }

    @Override
    public Result<Void> remoteDeleteMessage(String mid, Long sid) {
        return publish(ReplicatedOp.remoteDeleteMessage(mid, sid));
    }

    @Override
    public Result<Void> remoteDeleteUserInbox(String name) {
        return publish(ReplicatedOp.remoteDeleteUserInbox(name));
    }
}
