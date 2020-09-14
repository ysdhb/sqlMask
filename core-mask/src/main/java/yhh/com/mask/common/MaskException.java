package yhh.com.mask.common;

public class MaskException extends RuntimeException {
    public MaskException() {
        super();
    }

    public MaskException(String message) {
        super(message);
    }

    public MaskException(String message, Throwable cause) {
        super(message, cause);
    }
}