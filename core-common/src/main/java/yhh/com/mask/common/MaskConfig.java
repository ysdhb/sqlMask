package yhh.com.mask.common;

public class MaskConfig {

    public String getJdbcSourceDialect() {
        return "";
    }

    private String driver;
    private String url;
    private String user;
    private String pass;

    public String getDriver() {
        return driver;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPass() {
        return pass;
    }
}
