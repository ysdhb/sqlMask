package yhh.com.mask.common;

public enum MetadataDialect {
    MYSQL("mysql"),
    UNKNOWN("unknown");

    String source;

    MetadataDialect(String source) {
        this.source = source;
    }

    public static MetadataDialect getDialect(String name) {
        return UNKNOWN;
    }
}