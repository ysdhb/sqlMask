package yhh.com.mask.policy;

import java.util.Map;

public class AbstractPolicyStorage implements PolicyStorage {

    private static final int interval = 10;

    @Override
    public Map<String, String> loadPolicies(String source) {
        return null;
    }

    @Override
    public String getPolicy(String source) {
        return null;
    }

    @Override
    public void updatePolicy() {
//        schd.scheduleAtFixedRate()

    }

}