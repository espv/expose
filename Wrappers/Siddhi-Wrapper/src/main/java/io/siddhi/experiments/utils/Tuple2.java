package io.siddhi.experiments.utils;

public class Tuple2<X, Y> {
    X x;
    Y y;

    public Tuple2(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    public X getFirst() { return x; }
    public Y getSecond() { return y; }
}
