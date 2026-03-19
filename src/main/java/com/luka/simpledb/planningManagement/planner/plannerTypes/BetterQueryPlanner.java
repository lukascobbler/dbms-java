package com.luka.simpledb.planningManagement.planner.plannerTypes;

import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.metadataManagement.exceptions.ViewDefinitionNotFoundException;
import com.luka.simpledb.parsingManagement.parser.Parser;
import com.luka.simpledb.parsingManagement.statement.SelectStatement;
import com.luka.simpledb.parsingManagement.statement.select.ProjectionFieldInfo;
import com.luka.simpledb.parsingManagement.statement.select.TableInfo;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.planningManagement.plan.planTypes.readOnly.*;
import com.luka.simpledb.planningManagement.planner.plannerDefinitions.QueryPlanner;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.transactionManagement.Transaction;

import java.util.ArrayList;
import java.util.List;

public class BetterQueryPlanner extends QueryPlanner {
    public BetterQueryPlanner(MetadataManager metadataManager) {
        super(metadataManager);
    }

    @Override
    protected Plan<Scan> createPlan(SelectStatement selectStatement, Transaction transaction) {
        List<Plan<Scan>> differentTablePlans = new ArrayList<>();
        for (TableInfo tableInfo : selectStatement.unionizedSelections().getFirst().tables()) { // todo unions
            String tableQualifier = tableInfo.qualifier();
            Plan<Scan> initialPlan;
            try {
                String viewDefinition = metadataManager.getViewDefinition(tableQualifier, transaction);
                Parser parser = new Parser(viewDefinition);
                SelectStatement viewSelectStatement = (SelectStatement) parser.parse();
                differentTablePlans.add(createPlan(viewSelectStatement, transaction)); // todo what to do with views
            } catch (ViewDefinitionNotFoundException e) {
                initialPlan = new TableReadOnlyPlan(transaction, tableQualifier, metadataManager);

                // prefixing all table's fields with the table qualifier because all
                // used fields will have a qualifier
                for (String noQualificationFieldName : initialPlan.schema().getFields()) {
                    String fullyQualifiedFieldName = tableQualifier + "." + noQualificationFieldName;
                    initialPlan = new RenameReadOnlyPlan(initialPlan, noQualificationFieldName, fullyQualifiedFieldName);
                }
                differentTablePlans.add(initialPlan);
            }
        }

        // joining all tables
        Plan<Scan> plan = differentTablePlans.removeFirst();

        for (Plan<Scan> nextPlan : differentTablePlans) {
            Plan<Scan> p1 = new ProductPlan(nextPlan, plan);
            Plan<Scan> p2 = new ProductPlan(plan, nextPlan);
            plan = p1.blocksAccessed() < p2.blocksAccessed() ? p1 : p2;
        }

        // selecting only on matching rows
        plan = new SelectReadOnlyPlan(plan, selectStatement.unionizedSelections().getFirst().predicate()); // todo unions

        // calculating all projection expressions
        for (ProjectionFieldInfo projectionFieldInfo : selectStatement.unionizedSelections().getFirst().projectionFields()) {
            plan = new ExtendReadOnlyPlan(plan, projectionFieldInfo);
        }

        // renaming all fields back to their requested name
        // some fields are requested fully-qualified and some fields
        // are requested only by the field identifier without the
        // qualificator
        List<String> projectionFields = selectStatement.unionizedSelections()
                .getFirst()
                .projectionFields()
                .stream()
                .map(ProjectionFieldInfo::name)
                .toList();

        for (String requestedField : projectionFields) {
            String sourceName = getFullyQualifiedFieldName(requestedField, plan.schema());

            if (!sourceName.equals(requestedField)) {
                plan = new RenameReadOnlyPlan(plan, sourceName, requestedField);
            }
        }

        return new ProjectReadOnlyPlan(plan, projectionFields); // todo unions
    }

    private String getFullyQualifiedFieldName(String requestedField, Schema underlyingSchema) {
        if (underlyingSchema.hasField(requestedField)) {
            return requestedField;
        }

        // because the statement is checked and the field exists for sure
        //noinspection OptionalGetWithoutIsPresent
        return underlyingSchema.getFields()
                .stream()
                .filter(f -> f.endsWith("." + requestedField))
                .findFirst()
                .get();
    }
}
