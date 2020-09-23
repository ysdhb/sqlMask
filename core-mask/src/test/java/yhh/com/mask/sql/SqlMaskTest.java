package yhh.com.mask.sql;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import yhh.com.mask.service.QueryService3;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class SqlMaskTest {

    @Autowired
    private QueryService3 qs3;


    private void assertEquals(String maskSql,String expectSql){
        Assert.assertEquals(maskSql.replace("\r", " ").replace("\n", System.getProperty("line.separator")).replaceAll("\\s+", " "),
                expectSql.replace("\r", " ").replace("\n", System.getProperty("line.separator")).replaceAll("\\s+", " "));
    }

    @Test
    public void sqlTest1() throws Exception {
        String originSql = "select name from emps";
        String expectSql = "select hash_fun(1, 5, emps.name, '*') as name\n" +
                "from sales.emps as emps";
        String maskSql = qs3.getMaskSql(originSql);
        assertEquals(maskSql, expectSql);

    }
}
