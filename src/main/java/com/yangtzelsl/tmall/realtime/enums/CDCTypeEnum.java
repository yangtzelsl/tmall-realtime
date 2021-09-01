package com.yangtzelsl.tmall.realtime.enums;

import java.util.HashMap;
import java.util.Map;

public enum CDCTypeEnum {

    /**
     * CDC 中的 c 操作类型 转成 INSERT
     */
    INSERT("c"),
    /**
     * CDC 中的 u 操作类型 转成 UPDATE
     */
    UPDATE("u"),
    /**
     * CDC 中的 d 操作类型 转成 DELETE
     */
    DELETE("d");

    private static final Map<String, CDCTypeEnum> MAP = new HashMap<>();

    static {
        for (CDCTypeEnum cdcTypeEnum : CDCTypeEnum.values()) {
            MAP.put(cdcTypeEnum.op, cdcTypeEnum);
        }
    }

    String op;

    CDCTypeEnum(String op) {
        this.op = op;
    }

    public static CDCTypeEnum of(String c) {
        return MAP.get(c);
    }
}

