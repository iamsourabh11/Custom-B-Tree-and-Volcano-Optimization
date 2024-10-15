package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;

import java.math.BigDecimal;
import java.util.NoSuchElementException;


import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.NoSuchElementException;


public class PFilter extends Filter implements PRel {
    private final RexNode condition;
    private boolean isOpen = false;
    private Object[] nextRow;

    public PFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
        this.condition = condition;
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new PFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public String toString() {
        return "PFilter";
    }

    @Override
    public boolean open() {
        logger.trace("Opening PFilter");
        if (input instanceof PRel) {
            isOpen = ((PRel) input).open();
        }
        return isOpen;
    }

    @Override
    public void close() {
        logger.trace("Closing PFilter");
        if (input instanceof PRel) {
            ((PRel) input).close();
        }
        isOpen = false;
    }

    @Override
    public boolean hasNext() {
        if (!isOpen) {
            throw new IllegalStateException("Filter is not open");
        }

        if (nextRow != null) {
            return true;
        }

        while (((PRel) input).hasNext()) {
            Object[] candidateRow = ((PRel) input).next();
            if (evaluateCondition(candidateRow)) {
                nextRow = candidateRow;
                return true;
            }
        }
        return false;
    }

    @Override
    public Object[] next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more rows available");
        }
        Object[] result = nextRow;
        nextRow = null;
        return result;
    }

    private boolean evaluateCondition(Object[] row) {
        return evaluateBoolean(this.condition, row);
    }

    // Recursive method to evaluate conditions based on the type of RexNode
    private boolean evaluateBoolean(RexNode node, Object[] row) {
        if (node instanceof RexLiteral) {
            return evaluateLiteral((RexLiteral) node);
        } else if (node instanceof RexInputRef) {
            return evaluateInputRef((RexInputRef) node, row);
        } else if (node instanceof RexCall) { // Directly handle RexCall without checking for 'CALL'
            return evaluateCall((RexCall) node, row);
        } else {
            throw new UnsupportedOperationException("Unsupported RexNode kind: " + node.getKind());
        }
    }


    private boolean evaluateLiteral(RexLiteral literal) {
        if (literal.getValue() == null) {
            return false;  // Treat null values as false
        }

        SqlTypeName typeName = literal.getType().getSqlTypeName();

        // Check if the literal's type is BOOLEAN and return the value directly.
        if (typeName == SqlTypeName.BOOLEAN) {
            // Assuming the value is correctly typed; otherwise, add error handling as needed.
            return (Boolean) literal.getValue();
        }

        // Use the helper function to check if the type is numeric and evaluate accordingly.
        if (isNumeric(typeName)) {
            // Convert the value to BigDecimal and check if it is not equal to zero.
            BigDecimal number = new BigDecimal(literal.getValue().toString());
            return number.compareTo(BigDecimal.ZERO) != 0;
        }

        // Optionally handle other types if needed. For example, treating string literals "true" as true.
        if (typeName == SqlTypeName.VARCHAR || typeName == SqlTypeName.CHAR) {
            String value = literal.getValue().toString().trim().toLowerCase();
            return "true".equals(value);  // Treat the literal string "true" as boolean true.
        }

        return false;  // Default to false for all other types that are not explicitly handled.
    }

    private boolean evaluateInputRef(RexInputRef inputRef, Object[] row) {
        Object value = row[inputRef.getIndex()];
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0;
        }
        throw new IllegalArgumentException("Unsupported input reference type for boolean evaluation");
    }

    private boolean evaluateCall(RexCall call, Object[] row) {
        switch (call.getOperator().getKind()) {
            case AND:
                return call.operands.stream().allMatch(operand -> evaluateBoolean(operand, row));
            case OR:
                return call.operands.stream().anyMatch(operand -> evaluateBoolean(operand, row));
            case NOT:
                return !evaluateBoolean(call.operands.get(0), row);
            case EQUALS:
                return compare(call.operands.get(0), call.operands.get(1), row) == 0;
            case NOT_EQUALS:
                return compare(call.operands.get(0), call.operands.get(1), row) != 0;
            case GREATER_THAN:
                return compare(call.operands.get(0), call.operands.get(1), row) > 0;
            case LESS_THAN:
                return compare(call.operands.get(0), call.operands.get(1), row) < 0;
            case GREATER_THAN_OR_EQUAL:
                return compare(call.operands.get(0), call.operands.get(1), row) >= 0;
            case LESS_THAN_OR_EQUAL:
                return compare(call.operands.get(0), call.operands.get(1), row) <= 0;
            default:
                throw new UnsupportedOperationException("Unsupported operation: " + call.getOperator().getName());
        }
    }


    // Helper to compare values based on operands and row data
    private int compare(RexNode left, RexNode right, Object[] row) {
        Object leftVal = evaluateValue(left, row);
        Object rightVal = evaluateValue(right, row);
        return new BigDecimal(leftVal.toString()).compareTo(new BigDecimal(rightVal.toString()));
    }

    // Generic evaluation of RexNode values, extend as needed
    private Object evaluateValue(RexNode node, Object[] row) {
        if (node instanceof RexLiteral) {
            return ((RexLiteral) node).getValue();
        } else if (node instanceof RexInputRef) {
            return row[((RexInputRef) node).getIndex()];
        } else if (node instanceof RexCall) {
            // Assuming simplistic arithmetic operations for example purposes
            RexCall call = (RexCall) node;
            Object left = evaluateValue(call.operands.get(0), row);
            Object right = evaluateValue(call.operands.get(1), row);
            return evaluateArithmetic(call.getOperator().getKind(), left, right);
        }
        throw new UnsupportedOperationException("Unsupported expression type for value evaluation: " + node.getClass());
    }

    // Evaluate basic arithmetic operations
    private Object evaluateArithmetic(SqlKind kind, Object left, Object right) {
        BigDecimal leftDecimal = new BigDecimal(left.toString());
        BigDecimal rightDecimal = new BigDecimal(right.toString());
        switch (kind) {
            case PLUS:
                return leftDecimal.add(rightDecimal);
            case MINUS:
                return leftDecimal.subtract(rightDecimal);
            case TIMES:
                return leftDecimal.multiply(rightDecimal);
            case DIVIDE:
                return leftDecimal.divide(rightDecimal, BigDecimal.ROUND_HALF_UP); // Rounding mode as needed
            default:
                throw new IllegalArgumentException("Unsupported arithmetic operation: " + kind);
        }
    }
    private boolean isNumeric(SqlTypeName typeName) {
        switch (typeName) {
            case INTEGER:
            case TINYINT:
            case SMALLINT:
            case BIGINT:
            case DECIMAL:
            case FLOAT:
            case REAL:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }
}