package pl.allegro.tech.hermes.common.metric;

import com.codahale.metrics.Gauge;

public class StaticValueGauge<T> implements Gauge<T> {

    private T value;

    public StaticValueGauge(T value) {
        this.value = value;
    }

    @Override
    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}
