package com.luka.simpledb.queryManagement.scanTypes.readOnly;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanDefinitions.UnaryScan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;

import java.util.Map;

/// An extend project scan represents the "generalized projection" relational algebra operator
/// (removes and adds columns) plus a rename (renames that expression's result as a field name).
/// It is a unary table read-only scan. The user specifies the list of projection expressions
/// and the names for them.
public class ExtendProjectScan extends UnaryScan {
    private final Map<String, Expression> projectionFields;

    /// An extend project scan requires the expressions that will be
    /// evaluated for every row, and names for them. Each expression
    /// will be treated as a field from this scan upwards, and
    /// a child scan.
    public ExtendProjectScan(Scan childScan, Map<String, Expression> projectionFields) {
        super(childScan);
        this.projectionFields = projectionFields;
    }

    /// This scan has a field if its super scan has a field, or
    /// if the given field name equals the name of the named expression.
    ///
    /// @return True if the field exists, from all fields of the super
    /// scan or if the field equals the name of the named expression.
    @Override
    public boolean hasField(String fieldName) {
        return projectionFields.containsKey(fieldName);
    }

    /// For the named expression, its result is calculated on the child
    /// scan and returned, and for every other field, the result is just
    /// the child scan's result.
    ///
    /// @return The int for the corresponding named expression or any other field.
    @Override
    protected int internalGetInt(String fieldName) {
        if (projectionFields.containsKey(fieldName)) {
            return projectionFields.get(fieldName).evaluate(childScan).asInt();
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
        if (projectionFields.containsKey(fieldName)) {
            return projectionFields.get(fieldName).evaluate(childScan).asString();
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
        if (projectionFields.containsKey(fieldName)) {
            return projectionFields.get(fieldName).evaluate(childScan).asBoolean();
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
        if (projectionFields.containsKey(fieldName)) {
            return projectionFields.get(fieldName).evaluate(childScan);
        }

        return super.internalGetValue(fieldName);
    }
}
