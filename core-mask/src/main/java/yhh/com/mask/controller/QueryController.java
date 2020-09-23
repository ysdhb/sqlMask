package yhh.com.mask.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import yhh.com.mask.bean.SqlRequest;
import yhh.com.mask.service.QueryService;
import yhh.com.mask.service.QueryService2;
import yhh.com.mask.service.QueryService3;

import java.util.Locale;

@Controller
public class QueryController {
    @Autowired
    private QueryService3 queryService;

    @PostMapping(path = "/query")
    @ResponseBody
    public String query(@RequestBody SqlRequest sql) throws Exception {
        return queryService.getMaskSql(sql.getSql());
    }

    @GetMapping(path = "/test")
    @ResponseBody
    public String hello() throws Exception {
        queryService.getMaskSql("select nn from (select name nn from emps) a".toUpperCase(Locale.ROOT));
        return "Hello World!";
    }
}