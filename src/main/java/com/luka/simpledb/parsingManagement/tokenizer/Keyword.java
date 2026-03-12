package com.luka.simpledb.parsingManagement.tokenizer;

import java.util.*;

/// Keywords available to the system's queries.
public enum Keyword {
    SELECT, FROM, WHERE, AND, INSERT, INTO, VALUES,
    DELETE, UPDATE, SET, CREATE, TABLE, VARCHAR,
    INT, VIEW, AS, INDEX, ON, NULL, TRUE, FALSE,
    IS, NOT, BOOLEAN, JOIN, UNION;

    private static final Map<String, Keyword> LOOKUP = new HashMap<>();

    static {
        for (Keyword k : Keyword.values()) {
            LOOKUP.put(k.name().toUpperCase(), k);
        }
    }

    /// @return The token from the string. Case-insensitive.
    public static Keyword fromString(String text) {
        return LOOKUP.get(text.toUpperCase());
    }
}
