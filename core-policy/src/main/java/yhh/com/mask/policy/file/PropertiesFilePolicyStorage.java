package yhh.com.mask.policy.file;


import org.springframework.core.io.ClassPathResource;
import yhh.com.mask.policy.AbstractPolicyStorage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesFilePolicyStorage extends AbstractPolicyStorage {

    @Override
    public Map<String, String> loadPolicies(String source) {
        ClassPathResource resource = new ClassPathResource("mysql-mask-policies.properties");
        Properties properties = new Properties();
        Map<String, String> map = new HashMap<>();
        try {
            properties.load(resource.getInputStream());
            map = new HashMap<String, String>((Map) properties);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;

    }
}