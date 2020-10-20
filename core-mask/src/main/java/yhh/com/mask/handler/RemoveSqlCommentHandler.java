package yhh.com.mask.handler;

public class RemoveSqlCommentHandler implements Handler {
    @Override
    public String processSql(String sql) {
        final String[] commentPatterns = new String[] { "--(?!.*\\*/).*?[\r\n]", "/\\*(.|\r|\n)*?\\*/" };

        for (String commentPattern : commentPatterns) {
            sql = sql.replaceAll(commentPattern, "");
        }

        sql = sql.trim();

        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        return sql;
    }
}
