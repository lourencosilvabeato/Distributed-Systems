package sd2526.trab.impl.zoho;

import com.google.gson.Gson;

class JSON {
    private static final Gson gson = new Gson();

    static <T> T decode(String json, Class<T> cls) {
        return gson.fromJson(json, cls);
    }

}
