package yhh.com.mask.handler;

import org.apache.calcite.mask.ColumnDesc;
import org.apache.calcite.mask.MaskContext;
import org.apache.calcite.sql.*;
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
        }
    }
}
