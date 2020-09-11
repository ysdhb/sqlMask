package yhh.com.mask.policy;

public class MaskPolicyFactory {


    private MaskPolicyFactory() {
    }

    public static PolicyStorage getInstance(String className) {
        PolicyStorage api = null;
        try {
            api = (PolicyStorage) Class.forName(className)
                    .newInstance();
        } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return api;
    }
}