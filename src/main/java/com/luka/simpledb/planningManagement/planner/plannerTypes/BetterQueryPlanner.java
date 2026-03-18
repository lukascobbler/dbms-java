package com.luka.simpledb.planningManagement.planner.plannerTypes;

import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.metadataManagement.exceptions.ViewDefinitionNotFoundException;
import com.luka.simpledb.parsingManagement.parser.Parser;
import com.luka.simpledb.parsingManagement.statement.SelectStatement;
import com.luka.simpledb.parsingManagement.statement.select.ProjectionFieldInfo;
import com.luka.simpledb.parsingManagement.statement.select.TableInfo;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.planningManagement.plan.planTypes.readOnly.ProductPlan;
import com.luka.simpledb.planningManagement.plan.planTypes.readOnly.ProjectReadOnlyPlan;
import com.luka.simpledb.planningManagement.plan.planTypes.readOnly.SelectReadOnlyPlan;
import com.luka.simpledb.planningManagement.plan.planTypes.readOnly.TableReadOnlyPlan;
import com.luka.simpledb.planningManagement.planner.plannerDefinitions.QueryPlanner;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
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
        for (TableInfo tableInfo : selectStatement.unionizedSelections().getFirst().tables()) { // todo unions and table names (rename scan)
            String tableName = tableInfo.tableName();
            try {
                String viewDefinition = metadataManager.getViewDefinition(tableName, transaction);
                Parser parser = new Parser(viewDefinition);
                SelectStatement viewSelectStatement = (SelectStatement) parser.parse();
                differentTablePlans.add(createPlan(viewSelectStatement, transaction));
            } catch (ViewDefinitionNotFoundException e) {
                differentTablePlans.add(new TableReadOnlyPlan(transaction, tableName, metadataManager));
            }
        }

        Plan<Scan> plan = differentTablePlans.removeFirst();

        for (Plan<Scan> nextPlan : differentTablePlans) {
            Plan<Scan> p1 = new ProductPlan(nextPlan, plan);
            Plan<Scan> p2 = new ProductPlan(plan, nextPlan);
            plan = p1.blocksAccessed() < p2.blocksAccessed() ? p1 : p2;
        }

        plan = new SelectReadOnlyPlan(plan, selectStatement.unionizedSelections().getFirst().predicate()); // todo unions

        return new ProjectReadOnlyPlan(plan,
                selectStatement.unionizedSelections()
                        .getFirst()
                        .projectionFields()
                        .stream()
                        .map(ProjectionFieldInfo::name)
                        .toList()); // todo unions and extends and *
    }
}
