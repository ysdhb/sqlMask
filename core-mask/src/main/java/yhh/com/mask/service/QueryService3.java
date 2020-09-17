package yhh.com.mask.service;

import org.apache.calcite.mask.ColumnDesc;
import org.apache.calcite.mask.MaskContext;
import org.apache.calcite.mask.MaskContextFacade;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import yhh.com.mask.common.MaskException;
import yhh.com.mask.policy.MaskPolicyFactory;
import yhh.com.mask.policy.PolicyStorage;
import yhh.com.mask.query.QueryConnection;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

@Service
public class QueryService3 {

    @Value("${mask.calcite.model.path}")
    private String modelPath;

    public static void main(String[] args) throws Exception {
        QueryService3 q3 = new QueryService3();
        String sql = "select nn from (select name nn from emps) a".toUpperCase(Locale.ROOT);
        sql = "select name from (select name from emps) a".toUpperCase(Locale.ROOT);
        sql = "select name from emps";
        sql = "with t1 as (select name from emps union select name from depts) select name from t1";
        sql = "with t1 as (select name aa from emps union select name aa from depts) select aa from t1";
        sql = "select a.name, a.aa from (select emps.name, emps.deptno as aa from sales.emps as emps union select depts.name, depts.deptno as aa from sales.depts as depts) as a";
        sql = "with t1 as (select name from emps), t2 as (select name from t1) select name from t2";
        sql = "with t1 as (select name ss,deptno from emps union select name ss,deptno from depts) select concat(ss,ss) dd ,case deptno when 10 then deptno when 20 then deptno + 10 else 3 end dd2 from t1";
        sql = "select case when deptno = 10 then deptno when deptno = 20 then deptno + 10 else 3 end dd2 from emps";

        System.out.println(q3.getMaskSql(sql.toUpperCase(Locale.ROOT)));
    }

    public String getMaskSql(String sql) throws Exception {
        MaskContext context = MaskContextFacade.current();
        Thread.currentThread().setName(context.getMaskId());
        try {
            //1. 处理 ddl 语句 转换为 select 语句 并将 前缀保存
            sql = getSelectPartFromDdlSql(sql);
            context.setSql(sql);

            //2. 连接 并 执行
            modelPath = "core-mask/src/main/resources/sales-csv.json";
            Connection conn = QueryConnection.getConnection(Paths.get(modelPath).toAbsolutePath().toString());
            Statement stmt = conn.createStatement();
            stmt.executeQuery(sql);

            //3. 添加别名 确认唯一列
            addAliasForColumn(context);

            //4. 查找原始列
            Map<SqlNode, String> sqlNodeAndOriginColumnStringMap = new HashMap<>();
            Map<SqlNode, List<SqlNode>> nodeMap = context.getNodeMap();

            for (SqlNode node : getSelectList(context.getNode())) {
                for (SqlNode node1 : nodeMap.get(node)) {
                    getOriginColumn(node1, context, sqlNodeAndOriginColumnStringMap);
                }
            }

            // 5. 重写sql
            String ret = rewriteSqlWithPolicy(context.getSql(), sqlNodeAndOriginColumnStringMap);
            ret = getSqlNode(ret).toSqlString(null, true)
                    .getSql().replace("`", "").toLowerCase(Locale.ROOT);
            return context.getDdlPrefix() + ret;
        } finally {
            context.resetContext(sql);
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

    private String getSelectPartFromDdlSql(String sql) {
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

    //处理ddl语句的sqlParseFactory
    private SqlNode getSqlNode(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, SqlParser.configBuilder()
                .setParserFactory(SqlDdlParserImpl.FACTORY)
                .build());
        return parser.parseStmt();
    }

    public String rewriteSqlWithPolicy(String sql, Map<SqlNode, String> sqlNodeAndOriginColumnStringMap) {
        Map<String, String> policies = getPolicy("mysql");
        //将需要重写的sql语句按顺序重新排列
        List<Pair<Integer, Integer>> posList = getAllPos(sqlNodeAndOriginColumnStringMap.keySet());
        posList.sort(Comparator.comparingInt(Pair::getLeft));
        //将对应的sqlNode也重新排列
        List<SqlNode> nodeList = new ArrayList<>(sqlNodeAndOriginColumnStringMap.keySet());
        nodeList.sort(Comparator.comparingInt(t -> t.getParserPosition().getColumnNum()));

        String[] ret = new String[posList.size() + 1];
        String[] columns = new String[posList.size()];
        int point = 0;
        for (int i = 0; i < posList.size(); i++) {
            ret[i] = sql.substring(point, posList.get(i).getLeft() - 1);
            point = posList.get(i).getRight();
            columns[i] = sql.substring(posList.get(i).getLeft() - 1, posList.get(i).getRight());
        }
        ret[posList.size()] = sql.substring(posList.get(posList.size() - 1).getRight());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ret.length - 1; i++) {
            sb.append(ret[i]);
            String maskFunction = policies.get(sqlNodeAndOriginColumnStringMap.get(nodeList.get(i)).toLowerCase(Locale.ROOT));
            if (StringUtils.isNoneEmpty(maskFunction)) {
                sb.append(maskFunction.replace("col", columns[i]));
                //先简单加个别名 后面可以放在前置处理中
                if (ret[i + 1].trim().startsWith("FROM") || ret[i + 1].trim().startsWith(",")) {
                    if (columns[i].contains(".")) {
                        sb.append(" ").append(columns[i].split("\\.")[1]);
                    } else {
                        sb.append(columns[i]);
                    }
                }
            } else {
                sb.append(columns[i]);
            }
        }
        sb.append(ret[ret.length - 1]);
        return sb.toString().toLowerCase(Locale.ROOT);
    }

    private static Map<String, String> getPolicy(String source) {
        PolicyStorage policyStorage = MaskPolicyFactory.getInstance("yhh.com.mask.policy.file.PropertiesFilePolicyStorage");
        return policyStorage.loadPolicies(source);

    }

    private static List<Pair<Integer, Integer>> getAllPos(Set<SqlNode> nodes) {
        List<Pair<Integer, Integer>> posSet = new ArrayList<>();
        for (SqlNode node : nodes) {
            SqlParserPos sqlParserPos = node.getParserPosition();
            posSet.add(Pair.of(sqlParserPos.getColumnNum(), sqlParserPos.getEndColumnNum()));
        }
        return posSet;
    }

}
