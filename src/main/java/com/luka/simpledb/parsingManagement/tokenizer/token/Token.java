package com.luka.simpledb.parsingManagement.tokenizer.token;

public sealed interface Token permits
        KeywordToken, IdentifierToken, StringToken, IntegerToken, SymbolToken, InvalidToken, EofToken {}