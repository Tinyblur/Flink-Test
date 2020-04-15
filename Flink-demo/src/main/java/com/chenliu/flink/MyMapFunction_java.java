package com.chenliu.flink;

import org.apache.flink.api.common.functions.MapFunction;

public class MyMapFunction_java implements MapFunction<String,String>{
    @Override
    public String map(String value) throws Exception {
        return value + "ovo";
    }
}
