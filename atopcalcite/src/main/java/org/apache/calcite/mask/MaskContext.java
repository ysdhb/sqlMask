package org.apache.calcite.mask;

import com.google.common.collect.Maps;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class MaskContext {

    private final String maskId;
    private String sql;
    private SqlNode node;
    private String ddlPrefix;
    private Map<SqlNode, ColumnDesc> detailedColumnMap = Maps.newConcurrentMap();
    //2 node 1 originNode node(name) in originNode(concat(name,city))
    private Map<SqlNode, List<SqlNode>> nodeMap = Maps.newConcurrentMap();

    public MaskContext() {
        this.maskId = UUID.randomUUID().toString();
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public String getMaskId() {
        return maskId;
    }

    public AtomicBoolean isMasking = new AtomicBoolean(true);
    public AtomicBoolean preCheck = new AtomicBoolean(true);


    public Map<SqlNode, ColumnDesc> getDetailedColumnMap() {
        return detailedColumnMap;
    }

    public void addDetailedColumn(SqlNode node, ColumnDesc dc) {
        if (node != null)
            this.detailedColumnMap.put(node, dc);
    }

    public Map<SqlNode, List<SqlNode>> getNodeMap() {
        return nodeMap;
    }

    //    //for example : node(name) in originNode(concat(name,city))
//    public void addNodePos(SqlNode originNode, SqlNode node) {
//        this.nodeMap.put(originNode, node);
//    }

    //    添加node,遇到函数或者sqlcase...
//    把node作为key是因为select *，originNode 是同一个(delete)
    public void addNodeList(List<SqlNode> nodes, SqlNode node) {
        SqlParserPos nodePos = node.getParserPosition();
        int start = nodePos.getColumnNum();
        int end = nodePos.getEndColumnNum();
        List<SqlNode> whenList = new ArrayList<>();
        for (SqlNode sqlNode : nodes) {
            if (sqlNode instanceof SqlCase ||
                    (sqlNode instanceof SqlBasicCall && ((SqlBasicCall) sqlNode).getOperator() instanceof SqlAsOperator
                            && ((SqlBasicCall) sqlNode).operands[0] instanceof SqlCase)) {
                removeSqlCaseWhenList(whenList, sqlNode);
            }
            if (whenList.contains(node)) {
                return;
            }
            SqlParserPos pos = sqlNode.getParserPosition();
            if (pos.getColumnNum() <= start && pos.getEndColumnNum() >= end) {
                List<SqlNode> nodeList = new ArrayList<>();
                if (this.nodeMap.get(sqlNode) != null) {
                    nodeList = this.nodeMap.get(sqlNode);
                }
                nodeList.add(node);
                this.nodeMap.put(sqlNode, nodeList);
                break;
            }
        }
    }

    public void removeSqlCaseWhenList(List<SqlNode> nodes, SqlNode node) {
        if (node instanceof SqlCase) {
            List<SqlNode> whenList = ((SqlCase) node).getWhenOperands().getList();
            for (SqlNode sn : whenList) {
                removeSqlCaseWhenList(nodes, sn);
            }
        } else if (node instanceof SqlBasicCall) {
            if (((SqlBasicCall) node).getOperator() instanceof SqlAsOperator || ((SqlBasicCall) node).getOperator() instanceof SqlBinaryOperator) {
                removeSqlCaseWhenList(nodes, ((SqlBasicCall) node).getOperands()[0]);
            }
        } else if (node instanceof SqlIdentifier) {
            nodes.add(node);
        }
    }

    public void setNode(SqlNode node) {
        this.node = node;
    }

    public SqlNode getNode() {
        return node;
    }

    public String getDdlPrefix() {
        return ddlPrefix;
    }

    public void setDdlPrefix(String ddlPrefix) {
        this.ddlPrefix = ddlPrefix;
    }

    public void resetContext(String sql) {
        detailedColumnMap = Maps.newConcurrentMap();
        nodeMap = Maps.newConcurrentMap();
        this.isMasking.set(true);
        this.preCheck.set(false);
        this.setNode(null);
        this.setSql(sql);
    }
}