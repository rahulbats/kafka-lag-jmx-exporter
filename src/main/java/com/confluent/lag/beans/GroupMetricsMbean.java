package com.confluent.lag.beans;

public interface GroupMetricsMbean {
    public long getEnd();
    public void setEnd(long end);
    public long getCurrent();
    public void setCurrent(long current);
    public long getLag();
    public void setLag(long lag);
}
