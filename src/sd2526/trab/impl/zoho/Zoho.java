package sd2526.trab.impl.zoho;

import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;

import java.util.List;

public class Zoho {
    static final String MAIL_API_BASE = "https://mail.zoho.eu/api";
    static final String CLIENT_ID = "1000.I8O94GCT6HVIE8EIUYZ7SFCWPGDYEX";
    static final String CLIENT_SECRET = "d1f96a394a86af5c46d73a1fc92cb860770756041c";
    static final String REFRESH_TOKEN = "1000.037296054bb84367bd6c64b8a5b5d85f.099079d5b72be0b9829f32a6d0e9f2a5";

    private static final String ACCOUNTS = "/accounts";

    final OAuth20Service service;
    final ZohoTokenManager tokenManager;

    static Zoho instance;

    private Zoho() {
        service = ZohoServiceFactory.buildService(CLIENT_ID, CLIENT_SECRET);
        tokenManager = new ZohoTokenManager(service, REFRESH_TOKEN);
    }

    synchronized public static Zoho getInstance() {
        if( instance == null)
            instance = new Zoho();
        return instance;
    }

    public ZohoAccount getAccount() throws Exception {
        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest request = new OAuthRequest(Verb.GET, MAIL_API_BASE + ACCOUNTS);
        service.signRequest(accessToken, request);
        try (Response response = service.execute(request)) {
            if (response.isSuccessful()) {
                var body = response.getBody();
                var data = JSON.decode(body, ZohoAccountReply.class).data();
                if (data == null || data.isEmpty()) return null;
                return data.get(0);
            } else {
                System.err.println(response.getCode() + "/" + response.getBody());
                return null;
            }
        }
    }

    public record ZohoStatus(int code, String description) {}

    public record ZohoAccountReply(ZohoStatus status, List<ZohoAccount> data) {}

    public record ZohoAccount(
            String incomingUserName,
            String firstName,
            String accountId,
            String mailboxAddress,
            String accountDisplayName,
            String role,
            String gender,
            String accountName,
            String displayName,
            String primaryEmailAddress,
            boolean enabled
    ) {}


}


