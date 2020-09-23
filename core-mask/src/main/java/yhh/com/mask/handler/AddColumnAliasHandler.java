package yhh.com.mask.handler;

import org.apache.calcite.mask.ColumnDesc;
import org.apache.calcite.mask.MaskContext;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import yhh.com.mask.query.QueryUtil;

import java.util.List;
import java.util.Map;

public class AddColumnAliasHandler implements Handler {
    private final MaskContext context;

    public AddColumnAliasHandler(MaskContext context) {
        this.context = context;
    }

    @Override
    public String processSql(String sql) {
        addAliasForColumn(context);
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
                detailedColumnMap.get(node).setAlias(QueryUtil.getColumnAlias(node));
            }
        }
    }
}