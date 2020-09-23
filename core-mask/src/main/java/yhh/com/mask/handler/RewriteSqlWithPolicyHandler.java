package yhh.com.mask.handler;

import org.apache.calcite.mask.MaskContext;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import yhh.com.mask.policy.MaskPolicyFactory;
import yhh.com.mask.policy.PolicyStorage;

import java.util.*;

public class RewriteSqlWithPolicyHandler implements Handler {

    private final MaskContext context;

    public RewriteSqlWithPolicyHandler(MaskContext context) {
        this.context = context;
    }

    @Override
    public String processSql(String sql) {

        Set<SqlParserPos> simpleColumnInFunctionCheck = new HashSet<>();
        for (SqlNode node : context.getNodeMap().keySet()) {
            checkColumnInFunction(node,simpleColumnInFunctionCheck);
//            if (node instanceof SqlBasicCall) {
//                SqlParserPos pos = node.getParserPosition();
//                simpleColumnInFunctionCheck.add(pos);
//            }
        }
        Map<SqlNode, String> sqlNodeAndOriginColumnStringMap = context.getNodeColumnMap();
        return rewriteSqlWithPolicy(context.getSql(), sqlNodeAndOriginColumnStringMap, simpleColumnInFunctionCheck);
    }


    //检查如果column在函数中，则不需要添加别名
    private void checkColumnInFunction(SqlNode node, Set<SqlParserPos> simpleColumnInFunctionCheck) {
//        for (SqlNode node : context.getNodeMap().keySet()) {
        if (node instanceof SqlBasicCall) {
            if (((SqlBasicCall) node).getOperator() instanceof SqlFunction) {
                SqlParserPos pos = node.getParserPosition();
                simpleColumnInFunctionCheck.add(pos);
            } else {
                for (int i = 0; i < ((SqlBasicCall) node).operands.length; i++) {
                    checkColumnInFunction(((SqlBasicCall) node).operands[i], simpleColumnInFunctionCheck);
                }
            }
        }
//        }
    }


    private String rewriteSqlWithPolicy(String sql, Map<SqlNode, String> sqlNodeAndOriginColumnStringMap, Set<SqlParserPos> posSet) {
        Map<String, String> policies = getPolicy("mysql");
        //将需要重写的sql语句按顺序重新排列
        List<Pair<Integer, Integer>> posList = getAllPos(sqlNodeAndOriginColumnStringMap.keySet());
        if (posList.size() == 0) {
            return sql;
        }

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
                //如果在函数中，不添加别名
                boolean inFunction = false;
                for (SqlParserPos pos : posSet) {
                    if (posList.get(i).getLeft() > pos.getColumnNum()
                            && posList.get(i).getRight() < pos.getEndColumnNum()) {
                        inFunction = true;
                    }
                    System.out.println();
                }
                //先简单加个别名 后面可以放在前置处理中
                if (!inFunction) {
                    if (ret[i + 1].trim().startsWith("FROM") || ret[i + 1].trim().startsWith(",")) {
                        if (columns[i].contains(".")) {
                            sb.append(" ").append(columns[i].split("\\.")[1]);
                        } else {
                            sb.append(columns[i]);
                        }
                    }
                }
            } else {
                sb.append(columns[i]);
            }
        }
        sb.append(ret[ret.length - 1]);
        return sb.toString().toLowerCase(Locale.ROOT);
    }

    private Map<String, String> getPolicy(String source) {
        PolicyStorage policyStorage = MaskPolicyFactory.getInstance("yhh.com.mask.policy.file.PropertiesFilePolicyStorage");
        return policyStorage.loadPolicies(source);

    }

    private List<Pair<Integer, Integer>> getAllPos(Set<SqlNode> nodes) {
        List<Pair<Integer, Integer>> posSet = new ArrayList<>();
        for (SqlNode node : nodes) {
            SqlParserPos sqlParserPos = node.getParserPosition();
            posSet.add(Pair.of(sqlParserPos.getColumnNum(), sqlParserPos.getEndColumnNum()));
        }
        return posSet;
    }
}
