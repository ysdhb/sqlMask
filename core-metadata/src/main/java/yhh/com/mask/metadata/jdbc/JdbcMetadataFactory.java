package yhh.com.mask.metadata.jdbc;

import yhh.com.mask.common.MetadataDialect;
import yhh.com.mask.metadata.IMetadata;

public class JdbcMetadataFactory {
    private JdbcMetadataFactory() {

    }

    public static IMetadata getJdbcMeta(MetadataDialect dialect,DBConf dbConf){

        switch (dialect){
            case MYSQL:
                return new MysqlJdbcMetadata(dialect,dbConf);
            default:
                return new DefaultJdbcMetadata(dialect,dbConf);
        }
    }
}