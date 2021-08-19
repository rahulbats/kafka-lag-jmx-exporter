package com.confluent.lag.beans;

public class GroupMetrics implements GroupMetricsMbean{
    public GroupMetrics(long end, long current, long lag) {

        this.end=end;
        this.current=current;
        this.lag=lag;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public long getCurrent() {
        return current;
    }

    public void setCurrent(long current) {
        this.current = current;
    }

    public long getLag() {
        return lag;
    }

    public void setLag(long lag) {
        this.lag = lag;
    }

    long end;
    long current;
    long lag;

}
