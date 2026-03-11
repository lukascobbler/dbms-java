package com.luka.simpledb.parsingManagement.tokenizer.token;

import com.luka.simpledb.parsingManagement.tokenizer.Keyword;

public record KeywordToken(Keyword keyword) implements Token {}