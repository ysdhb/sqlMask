package yhh.com.mask.metadata.jdbc;

public class DBConf {
    private String driver;
    private String url;
    private String user;
    private String pass;

    public DBConf(String driver, String url, String user, String pass) {
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.pass = pass;
    }
}
