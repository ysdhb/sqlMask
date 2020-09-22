package yhh.com.mask.sql;

import org.junit.Assert;
import org.junit.Test;
import yhh.com.mask.service.QueryService3;

public class SqlMaskTest {

    private QueryService3 qs3 = new QueryService3();

    @Test
    public void sqlTest1() throws Exception {
        String originSql = "select name from emps";
        String expectSql = ("select hash_fun(1, 5, emps.name, '*') as name\n" +
                "from sales.emps as emps").replaceAll("\\n"," ");
        String maskSql = qs3.getMaskSql(originSql.toUpperCase()).replaceAll("\\r\\n|\\n"," ");
        Assert.assertEquals(expectSql, maskSql);
    }
}
