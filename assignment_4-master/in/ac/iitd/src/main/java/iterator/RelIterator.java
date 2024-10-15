package iterator;

public interface RelIterator {
    public boolean open();
    public boolean hasNext();
    public Object[] next();
    public void close();
}