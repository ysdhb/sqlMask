package yhh.com.mask.handler;

import org.apache.calcite.mask.ColumnDesc;
import org.apache.calcite.mask.MaskContext;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlCase;
import yhh.com.mask.query.QueryUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExtractOriginColumnHandler implements Handler {

    private final MaskContext context;

    public ExtractOriginColumnHandler(MaskContext context) {
        this.context = context;
    }

    @Override
    public String processSql(String sql) {

        Map<SqlNode, String> sqlNodeAndOriginColumnStringMap = new HashMap<>();
        Map<SqlNode, List<SqlNode>> nodeMap = context.getNodeMap();

        for (SqlNode node2 : nodeMap.keySet()) {
            getOriginColumn(node2, context, sqlNodeAndOriginColumnStringMap);
        }

        context.setNodeColumnMap(sqlNodeAndOriginColumnStringMap);

        return null;
    }

    private void getOriginColumn(SqlNode node, MaskContext context, Map<SqlNode, String> sqlNodeAndOriginColumnStringMap) {
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
                        String columnName = QueryUtil.getColumnAlias(node);
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
            //改了之后就需要了
            //原来是只从select list 中
            //后来又改成了 从node map中获取
            //这种 从 select list 中 不行select name,(select city dd from emps where name = 'ss') tname from emps
        } else if (node instanceof SqlCase) {
//            List<SqlNode> whenList = ((SqlCase) node).getWhenOperands().getList();
//            for (SqlNode node1 : whenList) {
//                getOriginColumn(node1, context, sqlNodeAndOriginColumnStringMap);
//            }
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
}
