package com.luka.simpledb.parsingManagement;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/// Keywords available to the system's queries.
public enum Keyword {
    SELECT, FROM, WHERE, AND, INSERT, INTO, VALUES,
    DELETE, UPDATE, SET, CREATE, TABLE, VARCHAR,
    INT, VIEW, AS, INDEX, ON, NULL, TRUE, FALSE,
    IS, NOT, BOOLEAN;

    /// Computed once during class initialization.
    private static final List<String> CACHED_STRINGS =
            Arrays.stream(Keyword.values())
                    .map(k -> k.name().toLowerCase())
                    .collect(Collectors.collectingAndThen(
                            Collectors.toList(),
                            Collections::unmodifiableList
                    ));

    /// @return Keywords as a string list.
    public static List<String> asStrings() {
        return CACHED_STRINGS;
    }

    /// @return The lowercased string keyword.
    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }
}
