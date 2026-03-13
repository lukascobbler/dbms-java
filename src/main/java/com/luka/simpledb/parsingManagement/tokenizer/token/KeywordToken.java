package com.luka.simpledb.parsingManagement.tokenizer.token;

import com.luka.simpledb.parsingManagement.tokenizer.Keyword;

public record KeywordToken(Keyword keyword) implements Token {
    @Override
    public String toString() { return keyword.name().toUpperCase(); }
}