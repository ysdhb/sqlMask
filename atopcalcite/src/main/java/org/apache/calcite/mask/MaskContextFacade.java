package org.apache.calcite.mask;

import com.google.common.collect.Maps;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class MaskContextFacade {
    private static final ConcurrentMap<String, MaskContext> RUNNING_CTX_MAP = Maps.newConcurrentMap();
    private static final ThreadLocal<MaskContext> CURRENT_CTX = new ThreadLocal<MaskContext>() {
        @Override
        protected MaskContext initialValue() {
            MaskContext context = new MaskContext();
            RUNNING_CTX_MAP.put(context.getMaskId(), context);
            return context;
        }
    };

    public static MaskContext current() {
        return CURRENT_CTX.get();
    }

    public static void resetCurrent() {
        MaskContext maskContext = CURRENT_CTX.get();
        if (maskContext != null) {
            RUNNING_CTX_MAP.remove(maskContext.getMaskId());
            CURRENT_CTX.remove();
        }
    }

    public static MaskContext getCurrentContext(String maskId){
        return RUNNING_CTX_MAP.get(maskId);
    }
}