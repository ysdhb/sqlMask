package yhh.com.mask.handler;

import java.sql.Statement;

public class ExecuteValidatorHanler implements Handler {
    private final Statement stmt;

    public ExecuteValidatorHanler(Statement stmt) {
        this.stmt = stmt;
    }

    @Override
    public String processSql(String sql) {
        try {
            stmt.executeQuery(sql);
        } catch (Throwable e) {
            System.out.println(e.getMessage());
        }
        return null;
    }
}