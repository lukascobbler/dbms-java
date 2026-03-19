package com.luka.simpledb.queryManagement.virtualEntities;

import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.virtualEntities.term.Term;
import com.luka.simpledb.recordManagement.Schema;

import java.util.*;

/// A predicate is the topmost structure that binds all terms,
/// which hold all expressions. It defines logical operators between
/// terms. A predicate object holds a list of terms that implicitly have an
/// `AND` between them.
public class Predicate {
    private final List<Term> terms = new ArrayList<>();

    /// Initializes a predicate with no terms, that is equivalent
    /// to a `TRUE` value that satisfies everything.
    public Predicate() {}

    /// Initializes a predicate with one term.
    public Predicate(Term term) {
        terms.add(term);
    }

    /// Initialize a predicate with multiple terms.
    public Predicate(Term... terms) {
        this.terms.addAll(List.of(terms));
    }

    /// Adds all terms of some predicate to this one.
    public void conjoinWith(Predicate predicate) {
        terms.addAll(predicate.terms);
    }

    /// A predicate satisfies some scan if all terms that it holds
    /// satisfy that scan.
    ///
    /// @return Whether a predicate satisfies some scan.
    public boolean isSatisfied(Scan scan) {
        return terms.stream()
                .allMatch(t -> t.isSatisfied(scan));
    }

    /// A reduction factor of a predicate is the multiplication
    /// of reduction factors of all terms that it holds.
    ///
    /// @return The total reduction factor of all predicates.
    public <T extends Scan> int reductionFactor(Plan<T> plan) {
        double totalFactor = 1.0;
        for (Term term : terms) {
            totalFactor *= term.reductionFactor(plan);
            if (totalFactor > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
        }
        return (int) totalFactor;
    }

    // todo add docs once heuristic table planner is complete
    public Predicate selectSubPredicate(Schema schema) {
        Predicate result = new Predicate();

        for (Term term : terms) {
            if (term.appliesTo(schema)) {
                result.terms.add(term);
            }
        }

        if (result.terms.isEmpty()) {
            return null;
        }

        return result;
    }

    // todo add docs once heuristic table planner is complete
    public Predicate joinSubPredicate(Schema schema1, Schema schema2) {
        Predicate result = new Predicate();
        Schema newSchema = new Schema();
        newSchema.addAll(schema1);
        newSchema.addAll(schema2);

        for (Term term : terms) {
            if (!term.appliesTo(schema1) && !term.appliesTo(schema2) && term.appliesTo(newSchema)) {
                result.terms.add(term);
            }
        }

        if (result.terms.isEmpty()) {
            return null;
        }

        return result;
    }

    /// Checks for "Field = Constant" or "Constant = Field" cases
    /// and if that is true, returns the constant. Does this check
    /// for all terms, and returns the first found constant.
    ///
    /// @return The constant that some field equates to for any term.
    /// `null` in any other case.
    public Constant equatesWithConstant(String fieldName) {
        for (Term term : terms) {
            Constant c = term.equatesWithConstant(fieldName);
            if (c != null) {
                return c;
            }
        }

        return null;
    }

    /// Checks for "Field1 = Field2" or "Field2 = Field1" cases
    /// and if that is true, returns the right field. Does this check
    /// for all terms, and returns the first found field name.
    ///
    /// @return The right field for two field equality comparisons for any term.
    /// `null` in any other case.
    public String equatesWithFieldName(String fieldName) {
        for (Term term : terms) {
            String s = term.equatesWithFieldName(fieldName);
            if (s != null) {
                return s;
            }
        }

        return null;
    }

    /// Folds every term in the predicate. Uses `PartialEvaluator` internally.
    public void fold() {
        terms.forEach(Term::foldExpressions);
    }

    public List<Term> getTerms() {
        return terms;
    }

    @Override
    public String toString() {
        Iterator<Term> iter = terms.iterator();
        if (!iter.hasNext()) {
            return "";
        }
        StringBuilder result = new StringBuilder(iter.next().toString());
        while (iter.hasNext()) {
            result.append(" AND ").append(iter.next().toString());
        }

        return result.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Predicate predicate = (Predicate) o;

        if (terms.size() != predicate.terms.size()) return false;

        return new HashSet<>(terms).equals(new HashSet<>(predicate.terms));
    }

    @Override
    public int hashCode() {
        int h = 0;
        for (Term t : terms) {
            h += (t != null ? t.hashCode() : 0);
        }
        return h;
    }
}
