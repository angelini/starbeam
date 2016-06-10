package com.angelini.starbeam.udfs;

import java.util.function.Function;

public class Base {

    public static Function<Object, Object> identity() {
        return i -> i;
    }

    public static Function<Object, Object> stringToInt() {
        return str -> Integer.parseInt(str.toString());
    }

}
