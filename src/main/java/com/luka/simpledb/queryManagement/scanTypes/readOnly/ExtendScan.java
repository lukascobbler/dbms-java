package com.luka.simpledb.queryManagement.scanTypes.readOnly;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanDefinitions.UnaryScan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;

/// An extend scan does not represent any specific relational algebra operator,
/// although it can be thought of as a projection (adds a new column that is an expression)
/// plus a rename (renames that expression's result as a field name).
/// It is a unary table read-only scan. The user specifies the expression
/// and the name for it.
public class ExtendScan extends UnaryScan {
    private final Expression expression;
    private final String newFieldName;

    /// An extend scan requires the expression that will be
    /// evaluated for every row, a name for that expression (that
    /// will be treated as a field from this scan upwards) and
    /// a child scan.
    public ExtendScan(Scan childScan, Expression expression, String newFieldName) {
        super(childScan);
        this.expression = expression;
        this.newFieldName = newFieldName;
    }

    /// This scan has a field if its super scan has a field, or
    /// if the given field name equals the name of the named expression.
    ///
    /// @return True if the field exists, from all fields of the super
    /// scan or if the field equals the name of the named expression.
    @Override
    public boolean hasField(String fieldName) {
        return fieldName.equals(newFieldName) || super.hasField(fieldName);
    }

    /// For the named expression, its result is calculated on the child
    /// scan and returned, and for every other field, the result is just
    /// the child scan's result.
    ///
    /// @return The int for the corresponding named expression or any other field.
    @Override
    protected int internalGetInt(String fieldName) {
        if (fieldName.equals(newFieldName)) {
            return expression.evaluate(childScan).asInt();
        }

        return super.internalGetInt(fieldName);
    }

    /// For the named expression, its result is calculated on the child
    /// scan and returned, and for every other field, the result is just
    /// the child scan's result.
    ///
    /// @return The string for the corresponding named expression or any other field.
    @Override
    protected String internalGetString(String fieldName) {
        if (fieldName.equals(newFieldName)) {
            return expression.evaluate(childScan).asString();
        }

        return super.internalGetString(fieldName);
    }

    /// For the named expression, its result is calculated on the child
    /// scan and returned, and for every other field, the result is just
    /// the child scan's result.
    ///
    /// @return The boolean for the corresponding named expression or any other field.
    @Override
    protected boolean internalGetBoolean(String fieldName) {
        if (fieldName.equals(newFieldName)) {
            return expression.evaluate(childScan).asBoolean();
        }

        return super.internalGetBoolean(fieldName);
    }

    /// For the named expression, its result is calculated on the child
    /// scan and returned, and for every other field, the result is just
    /// the child scan's result.
    ///
    /// @return The constant for the corresponding named expression or any other field.
    @Override
    protected Constant internalGetValue(String fieldName) {
        if (fieldName.equals(newFieldName)) {
            return expression.evaluate(childScan);
        }

        return super.internalGetValue(fieldName);
    }
}
