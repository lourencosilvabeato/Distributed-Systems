package sd2526.trab.impl.api.java;

import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;

public interface ReplicatedAdminMessages extends AdminMessages {

    Result<Void> remotePostMessage(Message m, Long sid);

    Result<Void> remoteDeleteMessage(String mid, Long sid);

    @Override
    default Result<Void> remotePostMessage(Message m) {
        return remotePostMessage(m, null);
    }

    @Override
    default Result<Void> remoteDeleteMessage(String mid) {
        return remoteDeleteMessage(mid, null);
    }
}
