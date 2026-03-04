package com.luka.simpledb.metadataManagement;

import com.luka.simpledb.metadataManagement.exceptions.ViewDefinitionNotFoundException;
import com.luka.simpledb.queryManagement.scanTypes.TableScan;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.transactionManagement.Transaction;

/// `ViewMetadataManager` worries about which view name corresponds to which view
/// definition. It uses an internal table ("viewcatalog") to store these mappings.
public class ViewMetadataManager {
    private static final int MAX_VIEW_DEF = 1200;
    private final Layout viewCatalogLayout;

    /// Instantiates a new `ViewMetadataManager` that has an always in-memory layout
    /// for the view catalog table to improve efficiency. If the system is initialized for the first time
    /// (noted by `isNew`), the manager creates the view catalog using the table metadata manager.
    public ViewMetadataManager(boolean isNew, TableMetadataManager tableMetadataManager, Transaction transaction) {
        Schema viewCatalogSchema = new Schema();
        viewCatalogSchema.addStringField("viewname", TableMetadataManager.MAX_NAME_LENGTH, false);
        viewCatalogSchema.addStringField("viewdefinition", MAX_VIEW_DEF, false);
        viewCatalogLayout = new Layout(viewCatalogSchema, transaction.blockSize());

        if (isNew) {
            tableMetadataManager.createTable("viewcatalog", viewCatalogSchema, transaction);
        }
    }

    /// Create a new view from its name and its definition in the current transaction.
    public void createView(String viewName, String viewDefinition, Transaction transaction) {
        TableScan viewTableScan = new TableScan(transaction, "viewcatalog", viewCatalogLayout);

        try (viewTableScan) {
            viewTableScan.insert();
            viewTableScan.setString("viewname", viewName);
            viewTableScan.setString("viewdefinition", viewDefinition);
        }
    }

    /// @return The view definition corresponding to the view name.
    /// @throws ViewDefinitionNotFoundException if the view name does not match any view definition.
    public String getViewDef(String viewName, Transaction transaction) {
        TableScan viewTableScan = new TableScan(transaction, "viewcatalog", viewCatalogLayout);

        try (viewTableScan) {
            while (viewTableScan.next()) {
                if (viewTableScan.getString("viewname").equals(viewName)) {
                    return viewTableScan.getString("viewdefinition");
                }
            }
        }

        throw new ViewDefinitionNotFoundException();
    }
}
