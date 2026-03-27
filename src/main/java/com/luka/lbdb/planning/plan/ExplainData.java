package com.luka.lbdb.planning.plan;

import com.luka.lbdb.querying.virtualEntities.constant.Constant;
import com.luka.lbdb.querying.virtualEntities.constant.StringConstant;

import java.util.ArrayList;
import java.util.List;

/// Contains all data required for explaining a plan.
public record ExplainData(
        int indentation,
        String planName,
        int blocks,
        int records,
        String specifics
) {
    /// Converts a full list of ExplainData nodes into database tuples, so that
    /// unified table printing logic can be used in the client.
    ///
    /// @return A list of tuples of the explain statement.
    public static List<List<Constant>> toTuples(List<ExplainData> dataList) {
        List<List<Constant>> tuples = new ArrayList<>(dataList.size());

        for (int i = 0; i < dataList.size(); i++) {
            ExplainData current = dataList.get(i);

            StringBuilder prefix = new StringBuilder();
            if (current.indentation() > 0) {
                for (int j = 0; j < current.indentation() - 1; j++) {
                    prefix.append(hasMoreChildrenAtDepth(dataList, i, j + 1) ? "│  " : "   ");
                }
                prefix.append(isLastChild(dataList, i) ? "└─ " : "├─ ");
            }

            String formattedScanName = prefix + current.planName()
                    .replace("ReadOnly", "")
                    .replace("Plan", "Scan");

            String detail = current.specifics() == null ? "" : current.specifics();
            if (detail.length() > 50) {
                detail = detail.substring(0, 40) + "...";
            }

            tuples.add(List.of(
                    new StringConstant(formattedScanName),
                    new StringConstant(String.valueOf(current.blocks())),
                    new StringConstant(String.valueOf(current.records())),
                    new StringConstant(detail)
            ));
        }

        return tuples;
    }

    /// @return True if the current plan has more children at the provided depth.
    private static boolean hasMoreChildrenAtDepth(List<ExplainData> list, int currentIndex, int depth) {
        for (int i = currentIndex + 1; i < list.size(); i++) {
            if (list.get(i).indentation() == depth) return true;
            if (list.get(i).indentation() < depth) break;
        }
        return false;
    }

    /// @return True if the current plan is the last subplan.
    private static boolean isLastChild(List<ExplainData> list, int index) {
        int currentDepth = list.get(index).indentation();
        for (int i = index + 1; i < list.size(); i++) {
            if (list.get(i).indentation() == currentDepth) return false;
            if (list.get(i).indentation() < currentDepth) return true;
        }
        return true;
    }
}