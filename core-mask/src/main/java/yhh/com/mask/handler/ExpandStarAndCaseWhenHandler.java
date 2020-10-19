package yhh.com.mask.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.mask.MaskContext;
import yhh.com.mask.common.MaskException;

import java.sql.Statement;
import java.util.Locale;

@Slf4j
public class ExpandStarAndCaseWhenHandler implements Handler {

    private final MaskContext context;
    private final Statement stmt;

    public ExpandStarAndCaseWhenHandler(MaskContext context, Statement stmt) {
        this.context = context;
        this.stmt = stmt;
    }

    @Override
    public String processSql(String sql) {
        try {
            stmt.executeQuery(sql.toUpperCase(Locale.ROOT));
        } catch (Throwable e) {
            //pre check 提前跳出
            log.error(e.getMessage());
        }
        if (context.getSql().contains("EXPR$")) {
            log.error("calcite add column alias EXPR$, " + context.getSql() + " need check");
            throw new MaskException(context.getSql() + " need check");
//            return context.getSql() + " need check";
        }
        context.resetContext(context.getSql());
        return context.getSql();
    }
}
