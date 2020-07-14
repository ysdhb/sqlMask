package org.apache.calcite.mask;

import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlNode;
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
    public AtomicBoolean needCheck = new AtomicBoolean(true);


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
        for (SqlNode sqlNode : nodes) {
            SqlParserPos pos = sqlNode.getParserPosition();
            if (pos.getColumnNum() <= start && pos.getEndColumnNum() >= end) {
                List<SqlNode> nodeList = new ArrayList<>();
                if (this.nodeMap.get(sqlNode) != null){
                    nodeList = this.nodeMap.get(sqlNode);
                }
                nodeList.add(node);
                this.nodeMap.put(sqlNode,nodeList);
                break;
            }
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
        this.needCheck.set(false);
        this.setNode(null);
        this.setSql(sql);
    }
}