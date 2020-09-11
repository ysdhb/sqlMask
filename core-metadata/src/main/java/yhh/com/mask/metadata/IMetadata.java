package yhh.com.mask.metadata;

import java.util.List;

public interface IMetadata {
    List<String> listDatabases() throws Exception;

    List<String> listTables(String database) throws Exception;

    List<String> loadSvcMetadata(String database, String table) throws Exception;

}