package yhh.com.mask.handler;

import lombok.extern.slf4j.Slf4j;

import java.sql.Statement;

@Slf4j
public class ExecuteValidatorHandler implements Handler {
    private final Statement stmt;

    public ExecuteValidatorHandler(Statement stmt) {
        this.stmt = stmt;
    }

    @Override
    public String processSql(String sql) {
        try {
            stmt.executeQuery(sql);
        } catch (Throwable e) {
            //todo:以后写了提前跳出逻辑后删除
            log.error(e.getMessage());
        }
        return sql;
    }
}