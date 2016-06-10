package com.angelini.starbeam.udfs;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Base {
    public static Function<List<Object>, Object> identity() {
        return args -> args.get(0);
    }

    public static Function<List<Object>, Object> stringToInt() {
        return args -> Integer.parseInt(args.get(0).toString());
    }

    public static Function<List<Object>, Object> combineStrings() {
        return args -> args.stream()
                .map(arg -> arg.toString())
                .collect(Collectors.joining("-"));
    }
}
