package yhh.com.mask.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import yhh.com.mask.bean.SqlRequest;
import yhh.com.mask.service.QueryService;

@RestController
@RequestMapping(value = "/")
public class QueryController {
    @Autowired
    QueryService queryService;

    @RequestMapping(value = "/query", method = RequestMethod.POST, produces = { "application/json" })
    @ResponseBody
    public String query(@RequestBody SqlRequest sql) throws Exception {
//        String path = "/opt/yhh/sqlmask/web-app/src/main/resources/sales-csv.json";
//        queryService.init();
        return queryService.getMaskSql(sql.getSql());
    }

    @RequestMapping("/test")
    public String hello() {
        return "Hello World!";
    }
}