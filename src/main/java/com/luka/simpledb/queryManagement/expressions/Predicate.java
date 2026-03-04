package com.luka.simpledb.queryManagement.expressions;

import com.luka.simpledb.planningManagement.Plan;
import com.luka.simpledb.queryManagement.expressions.constants.Constant;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.recordManagement.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Predicate {
    private final List<Term> terms = new ArrayList<>();

    public Predicate() {}

    public Predicate(Term term) {
        terms.add(term);
    }

    public void conjoinWith(Predicate predicate) {
        terms.addAll(predicate.terms);
    }

    public boolean isSatisfied(Scan scan) {
        return terms.stream()
                .allMatch(t -> t.isSatisfied(scan));
    }

    public int reductionFactor(Plan plan) {
        return terms.stream()
                .mapToInt(term -> term.reductionFactor(plan))
                .reduce(1, (a, b) -> a * b);
    }

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

    public Constant equatesWithConstant(String fieldName) {
        for (Term term : terms) {
            Constant c = term.equatesWithConstant(fieldName);
            if (c != null) {
                return c;
            }
        }

        return null;
    }

    public String equatesWithFieldName(String fieldName) {
        for (Term term : terms) {
            String s = term.equatesWithFieldName(fieldName);
            if (s != null) {
                return s;
            }
        }

        return null;
    }

    @Override
    public String toString() {
        Iterator<Term> iter = terms.iterator();
        if (!iter.hasNext()) {
            return "";
        }
        StringBuilder result = new StringBuilder(iter.next().toString());
        while (iter.hasNext()) {
            result.append(" and ").append(iter.next().toString());
        }

        return result.toString();
    }
}
