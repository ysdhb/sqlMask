package org.apache.calcite.mask;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.List;

public class ColumnDesc {
    public ColumnDesc() {
    }

    private String id;
    private String alias;
    private List<SqlNode> fromSelects;
    private SqlSelect select;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<SqlNode> getFromSelects() {
        return fromSelects;
    }

    public void setFromSelects(List<SqlNode> fromSelects) {
        this.fromSelects = fromSelects;
    }

    public SqlSelect getSelect() {
        return select;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getAlias() {
        return alias;
    }

    public void setSelect(SqlSelect select) {
        this.select = select;
    }

    public List<SqlNode> getSqlSelectsInFromSelects() {
        List<SqlNode> sqlNodes = new ArrayList<>();
        for (SqlNode node : fromSelects) {
            getSqlSelect(node, sqlNodes);
        }
        return sqlNodes;
    }

    private void getSqlSelect(SqlNode node, List<SqlNode> sqlNodes) {
        if (node instanceof SqlSelect) {
            sqlNodes.add(node);
        } else if (node instanceof SqlBasicCall) {
            getSqlSelect((((SqlBasicCall) node).operands[0]), sqlNodes);
            getSqlSelect((((SqlBasicCall) node).operands[1]), sqlNodes);
        }
    }
}
