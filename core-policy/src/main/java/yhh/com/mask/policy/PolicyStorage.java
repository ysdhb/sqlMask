package yhh.com.mask.policy;

import java.util.Map;

public interface PolicyStorage {

    Map<String, String> loadPolicies(String source);

    String getPolicy(String key);

    //自动更新和手动更新两种
    void updatePolicy();
}