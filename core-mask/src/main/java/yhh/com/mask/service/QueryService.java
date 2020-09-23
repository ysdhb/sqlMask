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
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;
import yhh.com.mask.common.MaskException;
import yhh.com.mask.policy.PolicyStorage;
import yhh.com.mask.policy.MaskPolicyFactory;
import yhh.com.mask.policy.PolicyStorage;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static yhh.com.mask.common.Constants.CALCITE_AUTH_PASSWD;
import static yhh.com.mask.common.Constants.CALCITE_AUTH_USER;


@Service
public class QueryService {

    private Statement stmt;

//    public QueryService(String path) {
//        //path是自己生成的，这个以后改
//        init(path);
//    }

//    public QueryService(){}

    public String getMaskSql(String sql) throws Exception {
        MaskContext context = MaskContextFacade.getCurrentContext(Thread.currentThread().getName());
        sql = getSelectPartFromDdlSql(sql);
        context.setSql(sql);
        checkStarOrAmbiguousColumn(sql);

        String newSql = context.getSql();
        if (newSql.contains("EXPR$")) {
            System.out.println("please check you sql");
            throw new Exception("Ambiguous column,please check you sql");
        }
        context.resetContext(newSql.replace("`","").replaceAll("\\r\\n"," ").replaceAll("\\r"," ").replaceAll("\\n"," ").toLowerCase(Locale.ROOT));
        System.out.println("newSql: " + context.getSql());
        stmt.executeQuery(context.getSql().toUpperCase(Locale.ROOT));
        addAliasForColumn(context);
        Map<SqlNode, String> sqlNodeAndOriginColumnStringMap = new HashMap<>();
        Map<SqlNode, List<SqlNode>> nodeMap = context.getNodeMap();

        for (SqlNode node : getSelectList(context.getNode())) {
            for (SqlNode node1 : nodeMap.get(node)) {
                getOriginColumn(node1, context, sqlNodeAndOriginColumnStringMap);
            }
        }
        System.out.println(context.getSql());
        String ret = rewriteSqlWithPolicy(context.getSql(),sqlNodeAndOriginColumnStringMap);
        ret = getSqlNode(ret).toSqlString(null,true)
                .getSql().replace("`","").toLowerCase(Locale.ROOT);
//        System.out.println(rewriteSqlWithPolicy(newSql,sqlNodeAndOriginColumnStringMap).replace("`",""));
        return context.getDdlPrefix() + ret;
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

    //暂时使用，还需测试
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

    public String rewriteSqlWithPolicy(String sql,Map<SqlNode, String> sqlNodeAndOriginColumnStringMap) {
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
            if (StringUtils.isNoneEmpty(maskFunction))
                sb.append(maskFunction.replace("col", columns[i]));
            else {
                sb.append(columns[i]);
            }
        }
        sb.append(ret[ret.length - 1]);
        return sb.toString().toLowerCase(Locale.ROOT);
    }

    private static Map<String, String> getPolicy(String source) {
        PolicyStorage policyStorage = (PolicyStorage) MaskPolicyFactory.getInstance("yhh.com.policy.PropertiesFilePolicyStorage");
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

    //处理查询中带 * 号 和 有的多列组合成的列没有别名的情况
    //有的组合列中可能也有 * ，暂时不考虑
    private void checkStarOrAmbiguousColumn(String sql) {
        try {
            stmt.executeQuery(sql.toUpperCase(Locale.ROOT));
        } catch (SQLException e) {
            System.out.println("catch exception: " + e.getCause().getMessage());
            System.out.println("rewrite sql,catch npe cause return null");
        }
    }

    private String SqlNode2Sql(SqlNode sqlNode) {
        return sqlNode.toSqlString(null, true).getSql();
    }

//    @PostConstruct
    public void init() {
        String path = "D:\\code\\新建文件夹\\sqlMask\\core-mask\\src\\main\\resources\\sales-csv.json";
        MaskContext context = MaskContextFacade.current();
        Thread.currentThread().setName(context.getMaskId());

        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        info.put("model", path);
        info.put("user", CALCITE_AUTH_USER);
        info.put("password", CALCITE_AUTH_PASSWD);
        try {
            Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
            this.stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
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

//    public static void main(String[] args) throws Exception {
//        String path = "C:\\work\\sqlmask\\web-app\\src\\main\\resources\\sales-csv.json";
//        QueryService queryService = new QueryService();
//        queryService.init(path);
//        String sql = "select concat(name,city) from emps aa";
//        sql = "with t1 as (select name ss,deptno from emps union select name ss,deptno from depts) select concat(ss,ss) dd ,case deptno when 10 then deptno  when 20 then deptno + 10 else  3 end dd2 from t1";
//        sql = "select concat(name,city) from emps";
//        sql = "select * from emps a";
//        sql = "select * from (select empno+deptno nn,concat(name,city) nn2 from emps) t";
////        sql = "select dd from (select empno + empid dd from emps) a";
////        sql = "select t.nn, t.nn2 from (select (emps.empno + emps.deptno) as nn, concat(emps.name, emps.city) as nn2 from sales.emps as emps) as t";
//        sql = "create table t as select * from emps";
//        String newSql = queryService.getMaskSql(sql);
//        System.out.println(newSql.replace("`",""));
//    }
}