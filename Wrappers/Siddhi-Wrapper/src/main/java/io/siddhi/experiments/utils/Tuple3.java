package io.siddhi.experiments.utils;

public class Tuple3<X, Y, Z> {
    X x;
    Y y;
    Z z;

    public Tuple3(X x, Y y, Z z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public X getFirst() { return x; }
    public Y getSecond() { return y; }
    public Z getThird() { return z; }
}
