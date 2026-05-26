package sd2526.trab.impl.zoho;

import sd2526.trab.api.Message;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import jakarta.inject.Singleton;
import sd2526.trab.impl.rest.servers.RestResource;

import java.util.List;

@Singleton
public class ZohoResource extends RestResource implements RestMessages, RestAdminMessages {

    Messages impl;

    synchronized Messages impl() {
        return impl;
    }

    public ZohoResource() {
        this.impl = ZohoMessages.getInstance();
    }

    @Override
    public String postMessage(String pwd, Message msg) {
        return super.resultOrThrow( impl().postMessage(pwd, msg));
    }

    @Override
    public Message getMessage(String name, String mid, String pwd) {
        return super.resultOrThrow( impl().getInboxMessage(name, mid, pwd));
    }

    @Override
    public List<String> getMessages(String name, String pwd, String query) {
        if( query != null && ! query.isEmpty() )
            return super.resultOrThrow( impl().searchInbox(name, pwd, query));
        else
            return super.resultOrThrow(impl().getAllInboxMessages(name, pwd));
    }

    @Override
    public void removeFromUserInbox(String name, String mid, String pwd) {
        super.resultOrThrow( impl().removeInboxMessage(name, mid, pwd) );

    }

    @Override
    public void deleteMessage(String name, String mid, String pwd) {
        super.resultOrThrow( impl().deleteMessage(name, mid, pwd));
    }

    @Override
    public void remotePostMessage(Message m, Long sid) {
        super.resultOrThrow( ((AdminMessages)impl()).remotePostMessage(m));
    }

    @Override
    public void remoteDeleteMessage(String mid, Long sid) {
        super.resultOrThrow( ((AdminMessages)impl()).remoteDeleteMessage(mid));
    }

    @Override
    public void remoteDeleteUserInbox(String name) {
        super.resultOrThrow( ((AdminMessages)impl()).remoteDeleteUserInbox(name));
    }
}
