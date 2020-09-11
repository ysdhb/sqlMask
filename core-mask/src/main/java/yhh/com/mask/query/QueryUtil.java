package yhh.com.mask.query;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

import java.util.ArrayList;
import java.util.List;

public class QueryUtil {
    public String removeCommentInSql(String sql) {
        // match two patterns, one is "-- comment", the other is "/* comment */"
        final String[] commentPatterns = new String[]{"--(?!.*\\*/).*?[\r\n]", "/\\*(.|\r|\n)*?\\*/"};

        for (String commentPattern : commentPatterns) {
            sql = sql.replaceAll(commentPattern, "");
        }

        sql = sql.trim();

        return sql;
    }

    public String addAliasInSql(String sql) {
        return sql;
    }

    public SqlSelect getSelectSql(SqlNode node) {
        for (; ; ) {
            if (node instanceof SqlSelect) {
                return (SqlSelect) node;
            } else if (node instanceof SqlWith) {
                return getSelectSql(((SqlWith) node).body);
            }
        }
    }

    private List<SqlNode> getSelectList(SqlNode node) {
        node = getSelectSql(node);
        return ((SqlSelect) node).getSelectList().getList();

    }

    private SqlNode getSqlNode(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, SqlParser.configBuilder()
                .setParserFactory(SqlDdlParserImpl.FACTORY)
                .build());
        return parser.parseStmt();
    }

    public static void main(String[] args) throws Exception {
        QueryUtil util = new QueryUtil();
        String sql = "select a ,c c from b";
        SqlNode node = util.getSqlNode(sql);
        List<SqlNode> nodes = util.getSelectList(node);
        List<SqlParserPos> posList = new ArrayList<>();
        for (SqlNode node1 : nodes) {
            if (node1 instanceof SqlIdentifier) {
                System.out.println(1);
                SqlParserPos pos = node1.getParserPosition();
                posList.add(pos);
            }
        }
        System.out.println(posList);
    }
}