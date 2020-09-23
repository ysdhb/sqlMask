package yhh.com.mask.query;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

import java.util.ArrayList;
import java.util.List;

public class QueryUtil {

    //todo:获取别名的方法，简单判断，还需要再改
    public static String getColumnAlias(SqlNode node) {
        if (node instanceof SqlBasicCall) {
            return ((SqlBasicCall) node).operands[1].toString();
        } else {
            String name = node.toString();
            if (name.split("\\.").length > 1)
                return name.split("\\.")[1];
            return name;
        }
    }

    public static SqlNode getSqlNode(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, SqlParser.configBuilder()
                .setParserFactory(SqlDdlParserImpl.FACTORY)
                .build());
        return parser.parseStmt();
    }
}