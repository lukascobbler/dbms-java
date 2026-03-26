package com.luka.lbdb.planning.plan;

import java.util.List;

// todo the logic for table generation may be moved to a different file and reused across every table print

/// Contains all data required for explaining a plan.
public record ExplainData(
        int indentation,
        String planName,
        int blocks,
        int records,
        String specifics
) {
    public static String explainAllPlans(List<ExplainData> dataList) {
        if (dataList.isEmpty()) return "";

        String[] treeLines = new String[dataList.size()];
        String[] truncatedDetails = new String[dataList.size()];

        int maxScanWidth = "Scan".length();
        int maxBWidth = "Block est.".length();
        int maxRWidth = "Record est.".length();
        int maxDetWidth = "Details".length();

        for (int i = 0; i < dataList.size(); i++) {
            ExplainData current = dataList.get(i);

            StringBuilder prefix = new StringBuilder();
            if (current.indentation() > 0) {
                for (int j = 0; j < current.indentation() - 1; j++) {
                    prefix.append(hasMoreChildrenAtDepth(dataList, i, j + 1) ? "│  " : "   ");
                }
                prefix.append(isLastChild(dataList, i) ? "└─ " : "├─ ");
            }
            treeLines[i] = prefix + current.planName().replace("ReadOnly", "").replace("Plan", "Scan");
            maxScanWidth = Math.max(maxScanWidth, treeLines[i].length());

            String detail = current.specifics() == null ? "" : current.specifics();
            if (detail.length() > 50) {
                detail = detail.substring(0, 40) + "...";
            }
            truncatedDetails[i] = detail;
            maxDetWidth = Math.max(maxDetWidth, detail.length());

            maxBWidth = Math.max(maxBWidth, String.valueOf(current.blocks()).length());
            maxRWidth = Math.max(maxRWidth, String.valueOf(current.records()).length());
        }

        String top    = "┌" + "─".repeat(maxScanWidth + 2) + "┬" + "─".repeat(maxBWidth + 2) + "┬" + "─".repeat(maxRWidth + 2) + "┬" + "─".repeat(maxDetWidth + 2) + "┐\n";
        String headSep = "├" + "─".repeat(maxScanWidth + 2) + "┼" + "─".repeat(maxBWidth + 2) + "┼" + "─".repeat(maxRWidth + 2) + "┼" + "─".repeat(maxDetWidth + 2) + "┤\n";
        String bottom = "└" + "─".repeat(maxScanWidth + 2) + "┴" + "─".repeat(maxBWidth + 2) + "┴" + "─".repeat(maxRWidth + 2) + "┴" + "─".repeat(maxDetWidth + 2) + "┘\n";

        StringBuilder sb = new StringBuilder();

        sb.append(top);
        sb.append(String.format("│ %-" + maxScanWidth + "s │ %" + maxBWidth + "s │ %" + maxRWidth + "s │ %-" + maxDetWidth + "s │\n",
                "Scan", "Block est.", "Record est.", "Details"));
        sb.append(headSep);

        for (int i = 0; i < dataList.size(); i++) {
            ExplainData d = dataList.get(i);
            sb.append(String.format("│ %-" + maxScanWidth + "s │ %" + maxBWidth + "d │ %" + maxRWidth + "d │ %-" + maxDetWidth + "s │\n",
                    treeLines[i], d.blocks(), d.records(), truncatedDetails[i]));
        }

        sb.append(bottom);
        return sb.toString();
    }

    private static String center(String text, int width) {
        if (text.length() >= width) return text;
        int padding = (width - text.length()) / 2;
        return " ".repeat(padding) + text + " ".repeat(width - text.length() - padding);
    }

    private static boolean hasMoreChildrenAtDepth(List<ExplainData> list, int currentIndex, int depth) {
        for (int i = currentIndex + 1; i < list.size(); i++) {
            if (list.get(i).indentation() == depth) return true;
            if (list.get(i).indentation() < depth) break;
        }
        return false;
    }

    private static boolean isLastChild(List<ExplainData> list, int index) {
        int currentDepth = list.get(index).indentation();
        for (int i = index + 1; i < list.size(); i++) {
            if (list.get(i).indentation() == currentDepth) return false;
            if (list.get(i).indentation() < currentDepth) return true;
        }
        return true;
    }
}