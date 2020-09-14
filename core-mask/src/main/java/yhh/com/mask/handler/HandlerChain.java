package yhh.com.mask.handler;

import java.util.ArrayList;
import java.util.List;

public class HandlerChain {
    private List<Handler> handlerList = new ArrayList<>();

    public String handler(String sql){
        String newSql = sql;
        for (Handler handler: handlerList){
            newSql = handler.processSql(newSql);
        }
        return newSql;
    }

    public void addHandler(Handler handler){
        handlerList.add(handler);
    }
}
