package com.luka.simpledb.simpleDB.settings;

import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.planningManagement.planner.plannerDefinitions.QueryPlanner;
import com.luka.simpledb.planningManagement.planner.plannerDefinitions.UpdatePlanner;
import com.luka.simpledb.planningManagement.planner.plannerTypes.query.BasicQueryPlanner;
import com.luka.simpledb.planningManagement.planner.plannerTypes.BasicUpdatePlanner;
import com.luka.simpledb.planningManagement.planner.plannerTypes.query.BetterQueryPlanner;

/// Settings class for changing the instantiation parameters
/// of the system, useful for tests. The defaults are pretty good
/// for a working database.
public class SimpleDBSettings {
    public boolean UNDO_ONLY_RECOVERY = true;
    public int BLOCK_SIZE = 4096;
    public int BUFFER_SIZE = 8;
    public String LOG_FILE = "log_file";
    public QueryPlannerType queryPlannerType = QueryPlannerType.BETTER; // todo change default when heuristic is implemented
    public UpdatePlannerType updatePlannerType = UpdatePlannerType.BASIC;

    public QueryPlanner getQueryPlanner(MetadataManager metadataManager) {
        return switch (queryPlannerType) {
            case BASIC -> new BasicQueryPlanner(metadataManager);
            case BETTER -> new BetterQueryPlanner(metadataManager);
            case HEURISTIC -> throw new UnsupportedOperationException("implement heuristic planner");
        };
    }

    public UpdatePlanner getUpdatePlanner(MetadataManager metadataManager) {
        return switch (updatePlannerType) {
            case BASIC -> new BasicUpdatePlanner(metadataManager);
        };
    }
}
