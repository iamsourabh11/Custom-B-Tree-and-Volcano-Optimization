package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import convention.PConvention;

import java.math.BigDecimal;
import java.util.List;

public class PProject extends Project implements PRel {
    private final List<? extends RexNode> projects;
    private RelNode input;
    private boolean isOpen = false;

    public PProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        this.projects = projects;
        this.input = input;
        assert getConvention() instanceof PConvention;
    }

    @Override
    public PProject copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new PProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public String toString() {
        return "PProject";
    }

    @Override
    public boolean open() {
        logger.trace("Opening PProject");
        if (input instanceof PRel) {
            isOpen = ((PRel) input).open();
        }
        return isOpen;
    }

    @Override
    public void close() {
        logger.trace("Closing PProject");
        if (input instanceof PRel) {
            ((PRel) input).close();
        }
        isOpen = false;
    }

    @Override
    public boolean hasNext() {
        logger.trace("Checking if PProject has next");
        return isOpen && ((PRel) input).hasNext();
    }

    @Override
    public Object[] next() {
        logger.trace("Getting next row from PProject");
        if (hasNext()) {
            Object[] baseRow = ((PRel) input).next();
            Object[] projectedRow = new Object[projects.size()];
            for (int i = 0; i < projects.size(); i++) {
                projectedRow[i] = evaluateExpression(projects.get(i), baseRow);
            }
            return projectedRow;
        }
        return null;
    }

    private Object evaluateExpression(RexNode expression, Object[] baseRow) {
        switch (expression.getKind()) {
            case LITERAL:
                return evaluateLiteral((RexLiteral) expression);
            case DYNAMIC_PARAM:
                RexDynamicParam dynamicParam = (RexDynamicParam) expression;
                return baseRow[dynamicParam.getIndex()];
            case INPUT_REF:
                RexInputRef inputRef = (RexInputRef) expression;
                return baseRow[inputRef.getIndex()];
            case PLUS:
            case MINUS:
            case TIMES:
            case DIVIDE:
                return evaluateArithmeticOperation((RexCall) expression, baseRow);
            default:
                throw new UnsupportedOperationException("Unsupported expression type: " + expression.getKind());
        }
    }

    private Object evaluateLiteral(RexLiteral literal) {
        return literal.getValue2();
    }

    private Object evaluateArithmeticOperation(RexCall call, Object[] baseRow) {
        try {
            Object left = evaluateExpression(call.operands.get(0), baseRow);
            Object right = evaluateExpression(call.operands.get(1), baseRow);
            BigDecimal leftDecimal = new BigDecimal(left.toString());
            BigDecimal rightDecimal = new BigDecimal(right.toString());

            switch (call.getOperator().kind) {
                case PLUS:
                    return leftDecimal.add(rightDecimal);
                case MINUS:
                    return leftDecimal.subtract(rightDecimal);
                case TIMES:
                    return leftDecimal.multiply(rightDecimal);
                case DIVIDE:
                    return leftDecimal.divide(rightDecimal, 2, BigDecimal.ROUND_HALF_UP);
                default:
                    throw new UnsupportedOperationException("Unsupported operation: " + call.getOperator().getName());
            }
        } catch (NumberFormatException e) {
            logger.error("Error evaluating arithmetic operation", e);
            return null;
        }
    }
}
