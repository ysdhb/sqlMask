package yhh.com.mask.metadata.jdbc;

import yhh.com.mask.common.MetadataDialect;
import yhh.com.mask.metadata.IMetadata;

import java.util.List;

public class DefaultJdbcMetadata implements IMetadata {
    private IMetadata jdbcMetadata;

    public DefaultJdbcMetadata(MetadataDialect dialect,DBConf dbConf) {
        this.jdbcMetadata = JdbcMetadataFactory.getJdbcMeta(dialect, dbConf);
    }

    @Override
    public List<String> listDatabases() throws Exception {
        return jdbcMetadata.listDatabases();
    }

    @Override
    public List<String> listTables(String database) throws Exception {
        return jdbcMetadata.listTables(database);
    }

    @Override
    public List<String> loadSvcMetadata(String database, String table) throws Exception {
        return jdbcMetadata.loadSvcMetadata(database, table);
    }
}