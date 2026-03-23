package com.luka.simpledb.queryManagement.virtualEntities.term;

import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.exceptions.TermOperatorNotSupportedException;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.NullConstant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.*;
import com.luka.simpledb.recordManagement.schema.Schema;

import java.util.Objects;

/// The term class represents the logic for comparison operators
/// between two expressions. It also has logic for how much will
/// the result of the given comparison affect the query.
public class Term {
    private Expression lhs, rhs;
    private final TermOperator termOperator;

    /// An expression comparison is done between two expressions and the
    /// operator that is between them.
    public Term(Expression lhs, TermOperator termOperator, Expression rhs) {
        this.lhs = lhs;
        this.termOperator = termOperator;
        this.rhs = rhs;
    }

    /// Both expressions are evaluated to their constant form
    /// and the term operator is applied between them via the `Comparable`
    /// Java interface. If the `IS` operator is used, comparing NULL
    /// values is allowed, but for any other operator, comparing
    /// between NULLs returns false.
    ///
    /// @return Whether two expressions satisfy the operator defined between
    /// them for a given scan.
    /// @throws TermOperatorNotSupportedException if a comparison operator
    /// is used that isn't supported by the database.
    public boolean isSatisfied(Scan scan) {
        Constant rhsValue = rhs.evaluate(scan);
        Constant lhsValue = lhs.evaluate(scan);

        if (termOperator == TermOperator.IS) {
            return lhsValue.equals(rhsValue);
        }

        if (lhsValue instanceof NullConstant || rhsValue instanceof NullConstant) {
            return false;
        }

        return switch (termOperator) {
            case EQUALS -> lhsValue.equals(rhsValue);
            case NOT_EQUALS -> !lhsValue.equals(rhsValue);
            case GREATER_THAN -> lhsValue.compareTo(rhsValue) > 0;
            case LESS_THAN -> lhsValue.compareTo(rhsValue) < 0;
            case GREATER_OR_EQUAL -> lhsValue.compareTo(rhsValue) >= 0;
            case LESS_OR_EQUAL -> lhsValue.compareTo(rhsValue) <= 0;
            default -> throw new TermOperatorNotSupportedException();
        };
    }

    /// @return True if both expressions apply to the given schema.
    public boolean appliesTo(Schema schema) {
        return lhs.appliesTo(schema) && rhs.appliesTo(schema);
    }

    /// A reduction factor for a term is the dividing factor for how many rows
    /// this term will affect. For example, when the operator is `<=,`
    /// by some estimations the number of queried rows that this term
    /// will reduce for a given plan is n/2. In the case of equality operators,
    /// the reduction factor is calculated based on distinct values for some
    /// field name. `Integer.MAX_VALUE` denotes an "infinite" reduction factor
    /// meaning a small number of rows will pass the term comparison.
    ///
    /// @return The calculated reduction factor for this term.
    public <T extends Scan> int reductionFactor(Plan<T> plan) {
        if (termOperator != TermOperator.EQUALS && termOperator != TermOperator.IS) {
            return 2;
        }

        // todo check reduction factor and equates

        return switch (lhs) {
            case Expression leftExpr when getUniqueField(leftExpr) instanceof String leftField -> switch (rhs) {
                case Expression rightExpr when getUniqueField(rightExpr) instanceof String rightField ->
                        Math.max(plan.distinctValues(leftField), plan.distinctValues(rightField));
                case Expression r when r.isConstant() ->
                        plan.distinctValues(leftField);
                default -> Integer.MAX_VALUE;
            };
            case Expression leftExpr when leftExpr.isConstant() -> switch (rhs) {
                case Expression rightExpr when getUniqueField(rightExpr) instanceof String rField -> {
                    if (leftExpr.evaluate(null) instanceof NullConstant) yield Integer.MAX_VALUE;
                    yield plan.distinctValues(rField);
                }
                case Expression r when r.isConstant() ->
                        leftExpr.evaluate(null).equals(r.evaluate(null)) ? 1 : Integer.MAX_VALUE;
                default -> Integer.MAX_VALUE;
            };
            default -> Integer.MAX_VALUE;
        };
    }

    /// Checks for "Field = Constant" or "Constant = Field" cases
    /// and if that is true, returns the constant.
    ///
    /// @return The constant that some field equates to. `null`
    /// in any other case.
    public Constant equatesWithConstant(String fieldName) {
        if (termOperator != TermOperator.EQUALS) return null;

        return switch (new Pair(lhs, rhs)) {
            case Pair(FieldNameExpression exp, ConstantExpression(Constant c))
                    when exp.qualifiedName().equals(fieldName) -> c;
            case Pair(ConstantExpression(Constant c), FieldNameExpression exp)
                    when exp.qualifiedName().equals(fieldName) -> c;
            default -> null;
        };
    }

    /// Checks for "Field1 = Field2" or "Field2 = Field1" cases
    /// and if that is true, returns the right field.
    ///
    /// @return The right field for two field equality comparisons. `null`
    /// in any other case.
    public String equatesWithFieldName(String fieldName) {
        if (termOperator != TermOperator.EQUALS) return null;

        return switch (new Pair(lhs, rhs)) {
            case Pair(FieldNameExpression exp1, FieldNameExpression exp2)
                    when exp1.qualifiedName().equals(fieldName) -> exp2.qualifiedName();
            case Pair(FieldNameExpression exp1, FieldNameExpression exp2)
                    when exp2.qualifiedName().equals(fieldName) -> exp1.qualifiedName();
            default -> null;
        };
    }

    /// Extracts the unique field from a given expression for the
    /// distinct value count.
    ///
    /// @return The extracted field's name if there is one field,
    /// null if there is none or more than one.
    private String getUniqueField(Expression expr) {
        return switch (expr) {
            case FieldNameExpression exp -> exp.qualifiedName();
            case UnaryArithmeticExpression(var op, Expression operand) -> getUniqueField(operand);
            case BinaryArithmeticExpression(Expression left, var op, Expression right) -> {
                String leftField = getUniqueField(left);
                String rightField = getUniqueField(right);

                if (leftField != null && rightField != null) yield null;

                yield (leftField != null) ? leftField : rightField;
            }
            case ConstantExpression c -> null;
            case WildcardExpression wildcardExpression -> null;
        };
    }

    /// Folds the expressions contained in the term object.
    public void foldExpressions() {
        lhs = PartialEvaluator.evaluate(lhs);
        rhs = PartialEvaluator.evaluate(rhs);
    }

    public TermOperator getTermOperator() {
        return termOperator;
    }

    public Expression getLhs() {
        return lhs;
    }

    public Expression getRhs() {
        return rhs;
    }

    @Override
    public String toString() {
        return lhs.toString() + " " + termOperator.toString() + " " + rhs.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Term term = (Term) o;
        return Objects.equals(lhs, term.lhs) && Objects.equals(rhs, term.rhs) && termOperator == term.termOperator;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lhs, termOperator, rhs);
    }

    /// Helper record for easier comparisons.
    private record Pair(Expression left, Expression right) { }
}
