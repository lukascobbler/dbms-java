package com.luka.simpledb.planningManagement.planner.plannerTypes;

import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.metadataManagement.exceptions.ViewDefinitionNotFoundException;
import com.luka.simpledb.parsingManagement.parser.Parser;
import com.luka.simpledb.parsingManagement.statement.SelectStatement;
import com.luka.simpledb.parsingManagement.statement.select.TableInfo;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.planningManagement.plan.planTypes.readOnly.*;
import com.luka.simpledb.planningManagement.planner.plannerDefinitions.QueryPlanner;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.transactionManagement.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BetterQueryPlanner extends QueryPlanner {
    public BetterQueryPlanner(MetadataManager metadataManager) {
        super(metadataManager);
    }

    @Override
    protected Plan<Scan> createPlan(SelectStatement selectStatement, Transaction transaction) {
        List<Plan<Scan>> differentTablePlans = new ArrayList<>();
        for (TableInfo tableInfo : selectStatement.unionizedSelections().getFirst().tables()) { // todo unions
            String tableOrViewName = tableInfo.tableName();
            Plan<Scan> initialPlan;
            try {
                String viewDefinition = metadataManager.getViewDefinition(tableOrViewName, transaction);
                Parser parser = new Parser(viewDefinition);
                SelectStatement viewSelectStatement = (SelectStatement) parser.parse();
                differentTablePlans.add(createPlan(viewSelectStatement, transaction)); // todo what to do with views
            } catch (ViewDefinitionNotFoundException e) {
                initialPlan = new TableReadOnlyPlan(transaction, tableOrViewName, metadataManager);

                String tableQualifier = tableInfo.qualifier();
                Map<String, String> fieldNameMapping = new HashMap<>();

                // renaming all table scan's fields to be fully qualified
                // because they are fully qualified in the statement
                for (String noQualificationFieldName : initialPlan.outputSchema().getFields()) {
                    String fullyQualifiedFieldName = tableQualifier + "." + noQualificationFieldName;
                    fieldNameMapping.put(fullyQualifiedFieldName, noQualificationFieldName);
                }

                initialPlan = new RenamePlan(initialPlan, fieldNameMapping);
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
        if (!selectStatement.unionizedSelections().getFirst().predicate().getTerms().isEmpty()) {
            plan = new SelectReadOnlyPlan(plan, selectStatement.unionizedSelections().getFirst().predicate()); // todo unions
        }

        // projecting fields
        return new ExtendProjectPlan(plan, selectStatement.unionizedSelections().getFirst().projectionFields()); // todo unions
    }
}
