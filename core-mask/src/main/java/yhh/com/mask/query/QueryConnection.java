package yhh.com.mask.query;

import org.apache.calcite.mask.MaskContext;
import org.apache.calcite.mask.MaskContextFacade;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import static yhh.com.mask.common.Constants.CALCITE_AUTH_PASSWD;
import static yhh.com.mask.common.Constants.CALCITE_AUTH_USER;

@Component
public class QueryConnection {

    public static Connection getConnection(String modelPath) throws Exception {
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        info.put("model", modelPath);
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
            ioe.printStackTrace();
        }
    }
}