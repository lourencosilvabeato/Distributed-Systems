package sd2526.trab.impl.zoho;

import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.google.gson.Gson;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.impl.java.servers.JavaMessages;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import static sd2526.trab.api.java.Result.ErrorCode.*;
import static sd2526.trab.api.java.Result.*;

public class ZohoMessages extends JavaMessages {

    private static final Logger Log = Logger.getLogger(ZohoMessages.class.getName());

    private static final String SUBJ_PREFIX = "SDMSG:";
    private static final int LIST_LIMIT = 200;

    private final Zoho zoho = Zoho.getInstance();
    private final Gson gson = new Gson();
    private String accountId;
    private String accountEmail;

    static ZohoMessages instance;

    protected ZohoMessages() {
        try {
            var account = zoho.getAccount();
            if (account == null)
                throw new RuntimeException("Zoho account not found");
            accountId = account.accountId();
            accountEmail = account.primaryEmailAddress();

        } catch (Exception e) {
            throw new RuntimeException("ZohoMessages init failed", e);
        }
    }

    // continue from previous state
    public static synchronized ZohoMessages getInstance() {
        return getInstance(false);
    }

    // create new clean state
    public static synchronized ZohoMessages getInstance(boolean fresh) {
        if (instance == null)
            instance = new ZohoMessages();
        if (fresh)
            instance.cleanupAllEmails();
        return instance;
    }

    // delete all emails from zoho
    private void cleanupAllEmails() {
        try {
            var emails = listEmails(SUBJ_PREFIX);
            for (var email : emails)
                deleteEmail(email.messageId(), email.folderId());

        } catch (Exception e) {
            Log.warning("cleanupAllEmails failed: " + e.getMessage());
        }
    }

    @Override
    protected void deliverToKnownLocalRecipients(Collection<String> addresses, Message msg) {
        var body = formatBody(msg);

        for (var address : addresses) {
            var name = getName(address);
            var subject = subjectFor(name, msg.getId());

            try {
                var exists = listEmails(SUBJ_PREFIX + name + ":").stream()
                        .anyMatch(e -> subject.equals(e.subject()));
                if (exists)
                    continue;

                sendEmail(subject, body);

            } catch (Exception e) {
                Log.warning("deliverToKnownLocalRecipients failed for " + address + ": " + e.getMessage());
            }
        }
    }

    private String formatBody(Message msg) {
        return Base64.getEncoder().encodeToString(gson.toJson(msg).getBytes(StandardCharsets.UTF_8));
    }

    private Message parseBody(String html) {
        var b64 = html.replaceAll("<[^>]+>", "").replaceAll("\\s+", "");

        if (b64.isEmpty())
            return null;

        try {
            var json = new String(Base64.getDecoder().decode(b64), StandardCharsets.UTF_8);
            return gson.fromJson(json, Message.class);

        } catch (Exception e) {
            Log.warning("parseBody failed: " + e.getMessage());
            return null;
        }
    }

    @Override
    protected Result<Void> deleteFromLocalInbox(String mid) {
        try {
            for (var email : listEmails(SUBJ_PREFIX))
                if (email.subject().endsWith(":" + mid))
                    deleteEmail(email.messageId(), email.folderId());
            return ok();

        } catch (Exception e) {
            Log.warning("deleteFromLocalInbox failed: " + e.getMessage());
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    protected Result<Message> dbGetInboxMessage(String name, String mid) {
        try {
            var subject = subjectFor(name, mid);

            var match = listEmails(SUBJ_PREFIX + name + ":").stream()
                    .filter(e -> subject.equals(e.subject()))
                    .findFirst();

            if (match.isEmpty())
                return error(NOT_FOUND);

            var msg = fetchContent(match.get().messageId(), match.get().folderId());

            return msg != null ? ok(msg) : error(NOT_FOUND);

        } catch (Exception e) {
            Log.warning("dbGetInboxMessage failed: " + e.getMessage());
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    protected Result<List<String>> dbGetAllInboxMessages(String name) {
        try {
            var mids = listEmails(SUBJ_PREFIX + name + ":").stream()
                    .map(e -> midFromSubject(e.subject()))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            return ok(mids);

        } catch (Exception e) {
            Log.warning("dbGetAllInboxMessages failed: " + e.getMessage());
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    protected Result<List<String>> dbSearchInbox(String name, String query) {
        try {
            var emails = listEmails(SUBJ_PREFIX + name + ":");
            var hits = new ArrayList<String>();
            var q = query.toUpperCase();
            for (var email : emails) {
                var msg = fetchContent(email.messageId(), email.folderId());

                if (msg == null)
                    continue;

                if ((msg.getSubject() != null && msg.getSubject().toUpperCase().contains(q)) ||
                    (msg.getContents() != null && msg.getContents().toUpperCase().contains(q)))
                    hits.add(msg.getId());
            }
            return ok(hits);

        } catch (Exception e) {
            Log.warning("dbSearchInbox failed: " + e.getMessage());
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    protected Result<Void> dbRemoveInboxMessage(String name, String mid) {
        try {
            var subject = subjectFor(name, mid);
            var found = false;

            for (var email : listEmails(SUBJ_PREFIX + name + ":")) {
                if (subject.equals(email.subject())) {
                    deleteEmail(email.messageId(), email.folderId());
                    found = true;
                }
            }
            return found ? ok() : error(NOT_FOUND);

        } catch (Exception e) {
            Log.warning("dbRemoveInboxMessage failed: " + e.getMessage());
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    protected Result<Void> dbRemoteDeleteUserInbox(String name) {
        try {
            var emails = listEmails(SUBJ_PREFIX + name + ":");
            for (var email : emails)
                deleteEmail(email.messageId(), email.folderId());

            return ok();

        } catch (Exception e) {
            Log.warning("dbRemoteDeleteUserInbox failed: " + e.getMessage());
            return error(INTERNAL_ERROR);
        }
    }

    // zoho api
    private void sendEmail(String subject, String content) {
        try {
            var url = Zoho.MAIL_API_BASE + "/accounts/" + accountId + "/messages";
            var req = new OAuthRequest(Verb.POST, url);
            req.addHeader("Content-Type", "application/json");
            req.setPayload(gson.toJson(new SendMailReq(accountEmail, accountEmail, subject, content, "plaintext")));
            zoho.service.signRequest(accessToken(), req);

            try (Response resp = zoho.service.execute(req)) {
                if (!resp.isSuccessful())
                    Log.warning("sendEmail failed: %d %s".formatted(resp.getCode(), resp.getBody()));
            }

        } catch (Exception e) {
            Log.warning("sendEmail exception: " + e.getMessage());
        }
    }

    // zoho api
    private List<ZohoEmailSummary> listEmails(String subjectPrefix) throws Exception {
        var url = "%s/accounts/%s/messages/view?limit=%d"
                .formatted(Zoho.MAIL_API_BASE, accountId, LIST_LIMIT);

        var req = new OAuthRequest(Verb.GET, url);
        zoho.service.signRequest(accessToken(), req);
        try (Response resp = zoho.service.execute(req)) {
            if (!resp.isSuccessful()) {
                Log.warning("listEmails failed: %d %s".formatted(resp.getCode(), resp.getBody()));
                return List.of();
            }

            var reply = gson.fromJson(resp.getBody(), ZohoListReply.class);
            if (reply == null || reply.data() == null)
                return List.of();

            return reply.data().stream()
                    .filter(e -> e.subject() != null && e.subject().startsWith(subjectPrefix))
                    .collect(Collectors.toList());
        }
    }

    // zoho api
    private Message fetchContent(String emailId, String folderId) throws Exception {
        var url = "%s/accounts/%s/folders/%s/messages/%s/content"
                .formatted(Zoho.MAIL_API_BASE, accountId, folderId, emailId);

        var req = new OAuthRequest(Verb.GET, url);
        zoho.service.signRequest(accessToken(), req);

        try (Response resp = zoho.service.execute(req)) {
            if (!resp.isSuccessful()) {
                Log.warning("fetchContent failed: %d for id=%s".formatted(resp.getCode(), emailId));
                return null;
            }
            var reply = gson.fromJson(resp.getBody(), ZohoContentReply.class);

            if (reply == null || reply.data() == null || reply.data().content() == null) return null;
            return parseBody(reply.data().content());
        }
    }

    // zoho api
    private void deleteEmail(String emailId, String folderId) throws Exception {
        var url = "%s/accounts/%s/folders/%s/messages/%s"
                .formatted(Zoho.MAIL_API_BASE, accountId, folderId, emailId);

        var req = new OAuthRequest(Verb.DELETE, url);
        zoho.service.signRequest(accessToken(), req);

        try (Response resp = zoho.service.execute(req)) {
            if (!resp.isSuccessful())
                Log.warning("deleteEmail failed: %d for id=%s".formatted(resp.getCode(), emailId));
        }
    }

    private OAuth2AccessToken accessToken() throws Exception {
        return new OAuth2AccessToken(zoho.tokenManager.getValidAccessToken());
    }

    private String subjectFor(String recipient, String mid) {
        return SUBJ_PREFIX + recipient + ":" + mid;
    }

    private String midFromSubject(String subject) {
        if (subject == null || !subject.startsWith(SUBJ_PREFIX))
            return null;

        var rest = subject.substring(SUBJ_PREFIX.length());
        var colonIdx = rest.indexOf(':');

        return colonIdx >= 0 ? rest.substring(colonIdx + 1) : null;
    }

    private record SendMailReq(String fromAddress, String toAddress, String subject, String content, String mailFormat) {}
    private record ZohoListReply(Zoho.ZohoStatus status, List<ZohoEmailSummary> data) {}
    private record ZohoEmailSummary(String messageId, String subject, String folderId) {}
    private record ZohoContentReply(Zoho.ZohoStatus status, ZohoEmailData data) {}
    private record ZohoEmailData(String content) {}
}
