package yhh.com.mask.metadata.jdbc;

import yhh.com.mask.common.MetadataDialect;

import java.util.List;

public class MysqlJdbcMetadata extends DefaultJdbcMetadata {
    public MysqlJdbcMetadata(MetadataDialect dialect, DBConf dbConf) {
        super(dialect, dbConf);
    }

    @Override
    public List<String> listDatabases() throws Exception {
        return null;
    }

    @Override
    public List<String> listTables(String database) throws Exception {
        return null;
    }

    @Override
    public List<String> loadSvcMetadata(String database, String table) throws Exception {
        return null;
    }
}