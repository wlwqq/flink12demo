package com.wlh.p0;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;

public class Demo01TypeHint {

    public static void main(String[] args) {

        TypeHint<Tuple2<String, Integer>> typeHint = new TypeHint<Tuple2<String, Integer>>() {
        };

        System.out.println(typeHint.getClass());

        System.out.println(typeHint.getTypeInfo());

    }
}
