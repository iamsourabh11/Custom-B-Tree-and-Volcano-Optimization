package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;

import java.util.Set;

/*
    * Implement Hash Join
    * The left child is blocking, the right child is streaming
*/
public class PJoin extends Join implements PRel { 

    public PJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
                super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
                assert getConvention() instanceof PConvention;
    }

    @Override
    public PJoin copy(
            RelTraitSet relTraitSet,
            RexNode condition,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new PJoin(getCluster(), relTraitSet, left, right, condition, variablesSet, joinType);
    }

    @Override
    public String toString() {
        return "PJoin";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PJoin");
        /* Write your code here */
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PJoin");
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PJoin has next");
        /* Write your code here */
        return false;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PJoin");
        /* Write your code here */
        return null;
    }
}
