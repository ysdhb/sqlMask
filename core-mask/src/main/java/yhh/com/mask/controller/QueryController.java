package yhh.com.mask.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import yhh.com.mask.bean.SqlRequest;
import yhh.com.mask.service.QueryService3;

@Controller
public class QueryController {
    @Autowired
    private QueryService3 queryService;

    @PostMapping(path = "/query")
    @ResponseBody
    public String query(@RequestBody SqlRequest sql) throws Exception {
        return queryService.getMaskSql(sql.getSql());
    }
}