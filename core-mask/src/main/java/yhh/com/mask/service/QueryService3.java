package yhh.com.mask.service;

import org.apache.calcite.mask.MaskContext;
import org.apache.calcite.mask.MaskContextFacade;
import org.apache.calcite.sql.parser.SqlParseException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import yhh.com.mask.handler.AddColumnAliasHandler;
import yhh.com.mask.handler.ExecuteValidatorHandler;
import yhh.com.mask.handler.ExpandStarAndCaseWhenHandler;
import yhh.com.mask.handler.ExtractOriginColumnHandler;
import yhh.com.mask.handler.ExtractSelectPartFromDdlSqlHandler;
import yhh.com.mask.handler.HandlerChain;
import yhh.com.mask.handler.RemoveSqlCommentHandler;
import yhh.com.mask.handler.RewriteSqlWithPolicyHandler;
import yhh.com.mask.query.QueryConnection;
import yhh.com.mask.query.QueryUtil;

import javax.annotation.PostConstruct;
import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Locale;

@Service
public class QueryService3 {

    @Value("${mask.calcite.model.path}")
    private String modelPath;

    private Statement stmt;

    public String getMaskSql(String originSql) throws SqlParseException {
        MaskContext context = MaskContextFacade.current();
        Thread.currentThread().setName(context.getMaskId());
        try {
            HandlerChain handlerChain = new HandlerChain();
            addHandler(context, handlerChain);
            String ret = handlerChain.handler(originSql);
            ret = QueryUtil.getSqlNode(ret).toSqlString(null, true)
                    .getSql().replace("`", "")
                    .toLowerCase(Locale.ROOT);
            return context.getDdlPrefix() + ret;
        } finally {
            MaskContextFacade.resetCurrent();
        }
    }

    @PostConstruct
    private void init() throws Exception {
        ClassPathResource resource = new ClassPathResource("sales-csv.json");
        File file = resource.getFile();
        Connection conn = QueryConnection.getConnection(file.getAbsolutePath());
        stmt = conn.createStatement();
    }

    private void addHandler(MaskContext context, HandlerChain handlerChain) {
        handlerChain.addHandler(new RemoveSqlCommentHandler());
        handlerChain.addHandler(new ExtractSelectPartFromDdlSqlHandler(context));
        handlerChain.addHandler(new ExpandStarAndCaseWhenHandler(context, stmt));
        handlerChain.addHandler(new ExecuteValidatorHandler(stmt));
        handlerChain.addHandler(new AddColumnAliasHandler(context));
        handlerChain.addHandler(new ExtractOriginColumnHandler(context));
        handlerChain.addHandler(new RewriteSqlWithPolicyHandler(context));
    }
}