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


    private void compareSql(String maskSql, String expectSql){
        Assert.assertEquals(maskSql.replace("\r", " ").replace("\n", System.getProperty("line.separator")).replaceAll("\\s+", " "),
                expectSql.replace("\r", " ").replace("\n", System.getProperty("line.separator")).replaceAll("\\s+", " "));
    }

    @Test
    public void sqlTest1() throws Exception {
        String originSql = "select name from emps";
        String expectSql = "select hash_fun(1, 5, emps.name, '*') as name\n" +
                "from sales.emps as emps";
        String maskSql = qs3.getMaskSql(originSql);
        compareSql(maskSql, expectSql);
    }

    @Test
    public void sqlTest2() throws Exception {
        String originSql = "select nn from (select name nn from emps) a";
        String expectSql = "select a.nn\n" +
                "from (select hash_fun(1, 5, emps.name, '*') as nn\n" +
                "from sales.emps as emps) as a";
        String maskSql = qs3.getMaskSql(originSql);
        compareSql(maskSql, expectSql);
    }

    @Test
    public void sqlTest3() throws Exception {
        String originSql = "select name from (select name from emps) a";
        String expectSql = "select a.name\n" +
                "from (select hash_fun(1, 5, emps.name, '*') as name\n" +
                "from sales.emps as emps) as a";
        String maskSql = qs3.getMaskSql(originSql);
        compareSql(maskSql, expectSql);
    }
    @Test
    public void sqlTest4() throws Exception {
        String originSql = "with t1 as (select name from emps union select name from depts) select name from t1";
        String expectSql = "with t1 as (select hash_fun(1, 5, emps.name, '*') as name\n" +
                "from sales.emps as emps\n" +
                "union\n" +
                "select hash_fun3(1, 7, depts.name, '*') as name\n" +
                "from sales.depts as depts) (select t1.name\n" +
                "from t1 as t1)";
        String maskSql = qs3.getMaskSql(originSql);
        compareSql(maskSql, expectSql);
    }
    @Test
    public void sqlTest5() throws Exception {
        String originSql = "with t1 as (select name aa from emps union select name aa from depts) select aa from t1";
        String expectSql = "with t1 as (select hash_fun(1, 5, emps.name, '*') as aa\n" +
                "from sales.emps as emps\n" +
                "union\n" +
                "select hash_fun3(1, 7, depts.name, '*') as aa\n" +
                "from sales.depts as depts) (select t1.aa\n" +
                "from t1 as t1)";
        String maskSql = qs3.getMaskSql(originSql);
        compareSql(maskSql, expectSql);
    }
    @Test
    public void sqlTest6() throws Exception {
        String originSql = "select a.name, a.aa from (select emps.name, emps.deptno as aa from sales.emps as emps union select depts.name, depts.deptno as aa from sales.depts as depts) as a";
        String expectSql = "select a.name, a.aa\n" +
                "from (select hash_fun(1, 5, emps.name, '*') as name, hash_fun2(1, 6, emps.deptno, '*') as aa\n" +
                "from sales.emps as emps\n" +
                "union\n" +
                "select hash_fun3(1, 7, depts.name, '*') as name, depts.deptno as aa\n" +
                "from sales.depts as depts) as a";
        String maskSql = qs3.getMaskSql(originSql);
        compareSql(maskSql, expectSql);
    }
    @Test
    public void sqlTest7() throws Exception {
        String originSql = "with t1 as (select name from emps), t2 as (select name from t1) select name from t2";
        String expectSql = "with t1 as (select hash_fun(1, 5, emps.name, '*') as name\n" +
                "from sales.emps as emps), t2 as (select t1.name\n" +
                "from t1 as t1) (select t2.name\n" +
                "from t2 as t2)";
        String maskSql = qs3.getMaskSql(originSql);
        compareSql(maskSql, expectSql);
    }
    @Test
    public void sqlTest8() throws Exception {
        String originSql = "with t1 as (select name ss,deptno from emps union select name ss,deptno from depts) select concat(ss,ss) dd ,case deptno when 10 then deptno when 20 then deptno + 10 else 3 end dd2 from t1";
        String expectSql = "with t1 as (select hash_fun(1, 5, emps.name, '*') as ss, hash_fun2(1, 6, emps.deptno, '*') as deptno\n" +
                "from sales.emps as emps\n" +
                "union\n" +
                "select hash_fun3(1, 7, depts.name, '*') as ss, depts.deptno\n" +
                "from sales.depts as depts) (select concat(t1.ss, t1.ss) as dd, (case when (t1.deptno = 10) then t1.deptno when (t1.deptno = 20) then (t1.deptno + 10) else 3 end) as dd2\n" +
                "from t1 as t1)";
        String maskSql = qs3.getMaskSql(originSql);
        compareSql(maskSql, expectSql);
    }
    @Test
    public void sqlTest9() throws Exception {
        String originSql = "select case when deptno = 10 then deptno when deptno = 20 then deptno + 10 else 3 end dd2 from emps";
        String expectSql = "select (case when (emps.deptno = 10) then hash_fun2(1, 6, emps.deptno, '*') when (emps.deptno = 20) then (hash_fun2(1, 6, emps.deptno, '*') + 10) else 3 end) as dd2\n" +
                "from sales.emps as emps";
        String maskSql = qs3.getMaskSql(originSql);
        compareSql(maskSql, expectSql);
    }
    @Test
    public void sqlTest10() throws Exception {
        String originSql = "select name,(select concat(city,name) dd from emps where name = 'ss') tname from emps";
        String expectSql = "select hash_fun(1, 5, emps.name, '*') as name, (select concat(emps.city, hash_fun(1, 5, emps.name, '*')) as dd\n" +
                "from sales.emps as emps\n" +
                "where (emps.name = 'ss')) as tname\n" +
                "from sales.emps as emps";
        String maskSql = qs3.getMaskSql(originSql);
        compareSql(maskSql, expectSql);
    }
    @Test
    public void sqlTest11() throws Exception {
        String originSql = "select name,(select city dd from emps where name = 'ss') tname from emps";
        String expectSql = "select hash_fun(1, 5, emps.name, '*') as name, (select emps.city as dd\n" +
                "from sales.emps as emps\n" +
                "where (emps.name = 'ss')) as tname\n" +
                "from sales.emps as emps";
        String maskSql = qs3.getMaskSql(originSql);
        compareSql(maskSql, expectSql);
    }
    @Test
    public void sqlTest12() throws Exception {
        String originSql = "select * from emps";
        String expectSql = "select emps.empno, hash_fun(1, 5, emps.name, '*') as name, hash_fun2(1, 6, emps.deptno, '*') as deptno, emps.gender, emps.city, emps.empid, emps.age, emps.slacker, emps.manager, emps.joinedat\n" +
                "from sales.emps as emps";
        String maskSql = qs3.getMaskSql(originSql);
        compareSql(maskSql, expectSql);
    }
    @Test
    public void sqlTest13() throws Exception {
        String originSql = "select * from (select * from emps) t1";
        String expectSql = "select t1.empno, t1.name, t1.deptno, t1.gender, t1.city, t1.empid, t1.age, t1.slacker, t1.manager, t1.joinedat\n" +
                "from (select emps.empno, hash_fun(1, 5, emps.name, '*') as name, hash_fun2(1, 6, emps.deptno, '*') as deptno, emps.gender, emps.city, emps.empid, emps.age, emps.slacker, emps.manager, emps.joinedat\n" +
                "from sales.emps as emps) as t1";
        String maskSql = qs3.getMaskSql(originSql);
        compareSql(maskSql, expectSql);
    }
    @Test
    public void sqlTest14() throws Exception {
        String originSql = "with t1 as (select * from emps) select * from t1";
        String expectSql = "with t1 as (select emps.empno, hash_fun(1, 5, emps.name, '*') as name, hash_fun2(1, 6, emps.deptno, '*') as deptno, emps.gender, emps.city, emps.empid, emps.age, emps.slacker, emps.manager, emps.joinedat\n" +
                "from sales.emps as emps) (select t1.empno, t1.name, t1.deptno, t1.gender, t1.city, t1.empid, t1.age, t1.slacker, t1.manager, t1.joinedat\n" +
                "from t1 as t1)";
        String maskSql = qs3.getMaskSql(originSql);
        compareSql(maskSql, expectSql);
    }
}
