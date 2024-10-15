package rel;

import iterator.RelIterator;
import org.apache.calcite.rel.RelNode;

import org.apache.log4j.Logger;

public interface PRel extends RelNode, RelIterator {
    
    static final Logger logger = Logger.getLogger(PRel.class);
}