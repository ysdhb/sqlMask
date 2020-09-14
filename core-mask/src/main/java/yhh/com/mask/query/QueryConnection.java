package yhh.com.mask.query;

import org.apache.calcite.mask.MaskContext;
import org.apache.calcite.mask.MaskContextFacade;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import static yhh.com.mask.common.Constants.CALCITE_AUTH_PASSWD;
import static yhh.com.mask.common.Constants.CALCITE_AUTH_USER;


public class QueryConnection {

    public static Connection getConnection() throws Exception {
        String path = "D:\\code\\新建文件夹\\sqlMask\\core-mask\\src\\main\\resources\\sales-csv.json";
//        MaskContext context = MaskContextFacade.current();
//        Thread.currentThread().setName(context.getMaskId());

        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        info.put("model", path);
        info.put("user", CALCITE_AUTH_USER);
        info.put("password", CALCITE_AUTH_PASSWD);
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:calcite:", info);
        } finally {
            closeQuietly(conn);
        }
        return conn;
    }

    public static void closeQuietly(final AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (final Exception ioe) {
//            logger.debug("", ioe);
        }
    }
}