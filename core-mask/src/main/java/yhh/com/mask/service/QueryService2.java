package yhh.com.mask.service;

import org.apache.calcite.mask.ColumnDesc;
import org.apache.calcite.mask.MaskContext;
import org.apache.calcite.mask.MaskContextFacade;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import yhh.com.mask.common.MaskException;
import yhh.com.mask.query.QueryConnection;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
public class QueryService2 {
    public static void main(String[] args) throws Exception {
        QueryService2 qs = new QueryService2();
        qs.getMaskSql("select nn from (select name nn from emps) a".toUpperCase(Locale.ROOT));
    }

    @Value("${mask.calcite.model.path}")
    private String modelPath;

    public String getMaskSql(String sql) throws Exception {
        MaskContext context = MaskContextFacade.current();
        Thread.currentThread().setName(context.getMaskId());
        sql = getSelectPartFromDdlSql(sql);
        context.setSql(sql);

        Connection conn = QueryConnection.getConnection(Paths.get(modelPath).toAbsolutePath().toString());
        Statement stmt = conn.createStatement();
        stmt.executeQuery(sql);
        addAliasForColumn(context);

        Map<SqlNode, String> sqlNodeAndOriginColumnStringMap = new HashMap<>();
        Map<SqlNode, List<SqlNode>> nodeMap = context.getNodeMap();

        for (SqlNode node : getSelectList(context.getNode())) {
            for (SqlNode node1 : nodeMap.get(node)) {
                getOriginColumn(node1, context, sqlNodeAndOriginColumnStringMap);
            }
        }

        System.out.println();
        context.resetContext(sql);
        System.out.println(sql);
        return sql;
    }

    private void addAliasForColumn(MaskContext context) {
        Map<SqlNode, List<SqlNode>> nodeMap = context.getNodeMap();
        Map<SqlNode, ColumnDesc> detailedColumnMap = context.getDetailedColumnMap();
        for (SqlNode node : nodeMap.keySet()) {
            if (node instanceof SqlBasicCall && ((SqlBasicCall) node).getOperator() instanceof SqlAsOperator) {
                for (SqlNode sqlNode : nodeMap.get(node)) {
                    detailedColumnMap.get(sqlNode).setAlias(((SqlBasicCall) node).operands[1].toString());
                }
            } else if (node instanceof SqlIdentifier) {
                detailedColumnMap.get(node).setAlias(getColumnAlias(node));
            }
        }
    }

    private static void getOriginColumn(SqlNode node, MaskContext context, Map<SqlNode, String> sqlNodeAndOriginColumnStringMap) {
        Map<SqlNode, ColumnDesc> dcs = context.getDetailedColumnMap();
        System.out.println(node.toString());
        System.out.println("start: " + node.getParserPosition().getColumnNum() + ";end: " + node.getParserPosition().getEndColumnNum());
        if (node instanceof SqlIdentifier) {
            ColumnDesc dc = dcs.get(node);
            if (dc.getFromSelects() == null) {
                if (!sqlNodeAndOriginColumnStringMap.containsKey(node)) {
                    sqlNodeAndOriginColumnStringMap.put(node, dc.getId() + "." + node.toString().split("\\.")[1]);
                }
            } else {
                for (SqlNode node1 : dc.getSqlSelectsInFromSelects()) {
                    Map<SqlNode, ColumnDesc> descMap = context.getDetailedColumnMap();
                    for (SqlNode node2 : descMap.keySet()) {
                        String columnName = getColumnAlias(node);
                        ColumnDesc desc = descMap.get(node2);
                        if (desc.getSelect() == node1 && columnName.equals(desc.getAlias())) {
                            getOriginColumn(node2, context, sqlNodeAndOriginColumnStringMap);
                        }
                    }
                }
            }
        } else if (node instanceof SqlBasicCall) {
            //as
            SqlOperator operator = ((SqlBasicCall) node).getOperator();
            if (operator instanceof SqlAsOperator) {
                getOriginColumn(((SqlBasicCall) node).operands[0], context, sqlNodeAndOriginColumnStringMap);
            } else {
                for (int i = 0; i < ((SqlBasicCall) node).operands.length; i++) {
                    getOriginColumn(((SqlBasicCall) node).operands[i], context, sqlNodeAndOriginColumnStringMap);
                }
            }
            //后面的不需要了，留着防万一
        } else if (node instanceof SqlCase) {
            List<SqlNode> whenList = ((SqlCase) node).getWhenOperands().getList();
            for (SqlNode node1 : whenList) {
                getOriginColumn(node1, context, sqlNodeAndOriginColumnStringMap);
            }
            List<SqlNode> thenList = ((SqlCase) node).getThenOperands().getList();
            for (SqlNode node2 : thenList) {
                getOriginColumn(node2, context, sqlNodeAndOriginColumnStringMap);
            }
            SqlNode elseExpr = ((SqlCase) node).getElseOperand();
            getOriginColumn(elseExpr, context, sqlNodeAndOriginColumnStringMap);
        } else if (node instanceof SqlWindow) {
            System.out.println("to do");
        } else {
            System.out.println("other situation");
        }
    }

    private static String getColumnAlias(SqlNode node) {
        if (node instanceof SqlBasicCall) {
            return ((SqlBasicCall) node).operands[1].toString();
        } else {
            String name = node.toString();
            if (name.split("\\.").length > 1)
                return name.split("\\.")[1];
            return name;
        }
    }

    private List<SqlNode> getSelectList(SqlNode node) {
        node = getSelectSql(node);
        return ((SqlSelect) node).getSelectList().getList();

    }

    private SqlSelect getSelectSql(SqlNode node) {
        for (; ; ) {
            if (node instanceof SqlSelect) {
                return (SqlSelect) node;
            } else if (node instanceof SqlWith) {
                return getSelectSql(((SqlWith) node).body);
            }
        }
    }

    //处理ddl语句
    public String getSelectPartFromDdlSql(String sql) {
        SqlNode sqlNode = null;
        MaskContext context = MaskContextFacade.getCurrentContext(Thread.currentThread().getName());

        try {
            sqlNode = getSqlNode(sql);
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
        SqlNode queryNode = getQuerySql(sqlNode);
        int startColumn = queryNode.getParserPosition().getColumnNum();
        context.setDdlPrefix(sql.substring(0, startColumn - 1));
        return sql.substring(startColumn - 1);
    }

    //处理ddl语句的sqlParseFactory
    private SqlNode getSqlNode(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, SqlParser.configBuilder()
                .setParserFactory(SqlDdlParserImpl.FACTORY)
                .build());
        return parser.parseStmt();
    }

    private SqlNode getQuerySql(SqlNode node) {
        for (; ; ) {
            if (node instanceof SqlSelect || node instanceof SqlWith) {
                return node;
            } else if (node instanceof SqlOrderBy) {
                node = ((SqlOrderBy) node).query;
            } else if (node instanceof SqlCreateTable) {
                List<SqlNode> nodes = ((SqlCreateTable) node).getOperandList();
                node = nodes.get(nodes.size() - 1);
            } else if (node instanceof SqlCreateView) {
                List<SqlNode> nodes = ((SqlCreateView) node).getOperandList();
                node = nodes.get(nodes.size() - 1);
            } else if (node instanceof SqlInsert) {
                node = ((SqlInsert) node).getSource();
            } else {
                throw new MaskException();
            }
        }
    }
}