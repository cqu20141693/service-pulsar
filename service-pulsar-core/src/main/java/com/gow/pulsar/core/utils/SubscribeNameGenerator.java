package com.gow.pulsar.core.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author gow
 * @date 2021/8/10
 */
public enum SubscribeNameGenerator {


    IPGenerator() {
        public String getName() {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                return "127.0.0.1";
            }
        }
    },

    UNKNOWN() {
        public String getName() {
            return null;
        }
    };

    private final static Map<String, SubscribeNameGenerator> INNER;

    static {
        INNER = new HashMap<>();
        for (SubscribeNameGenerator generator : SubscribeNameGenerator.values()) {
            INNER.put(generator.name(), generator);
        }
    }

    public static SubscribeNameGenerator parseFromCode(String name) {
        return INNER.getOrDefault(name, UNKNOWN);
    }

    public abstract String getName();

}
