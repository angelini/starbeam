package com.angelini.starbeam.udfs;

import java.util.List;
import java.util.function.Function;

public class Election {
    public static Function<List<Object>, Object> keepEnglishName() {
        return args -> args.get(0).toString().split("/")[0];
    }
}
