package yhh.com.mask.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.mask.ColumnDesc;
import org.apache.calcite.mask.MaskContext;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParserPos;
import yhh.com.mask.query.QueryUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ExtractOriginColumnHandler implements Handler {

    private final MaskContext context;

    public ExtractOriginColumnHandler(MaskContext context) {
        this.context = context;
    }

    @Override
    public String processSql(String sql) {

        Map<SqlNode, String> sqlNodeAndOriginColumnStringMap = new HashMap<>();
        Map<SqlNode, List<SqlNode>> nodeMap = context.getNodeMap();

        SqlNode selectNode = context.getNode();
        if (context.getNode() instanceof SqlWith) {
            selectNode = ((SqlWith) context.getNode()).body;
        }
        SqlParserPos selectNodePos = ((SqlSelect) selectNode).getSelectList().getParserPosition();
        int startNum = selectNodePos.getColumnNum();
        int endNum = selectNodePos.getEndColumnNum();

        for (SqlNode node2 : nodeMap.keySet()) {
            SqlParserPos node2Pos = node2.getParserPosition();
            if (startNum <= node2Pos.getColumnNum() && endNum >= node2Pos.getEndColumnNum()) {
                getOriginColumn(node2, context, sqlNodeAndOriginColumnStringMap);
            }
        }

        context.setNodeColumnMap(sqlNodeAndOriginColumnStringMap);

        return sql;
    }

    private void getOriginColumn(SqlNode node, MaskContext context, Map<SqlNode, String> sqlNodeAndOriginColumnStringMap) {
        Map<SqlNode, ColumnDesc> dcs = context.getDetailedColumnMap();
//        log.info(node.toString());
//        log.info("start: " + node.getParserPosition().getColumnNum() + ";end: " + node.getParserPosition().getEndColumnNum());
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
            log.error("sql window function to do");
        } else {
            log.error("other situation");
        }
    }
}
