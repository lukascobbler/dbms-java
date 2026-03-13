package com.luka.simpledb.parsingManagement.tokenizer.token;

/// A token represents a single character or a collection
/// of character that together have a syntactic meaning.
/// It is represented as a sealed interface to allow easier
/// structure matching using `switch`. Only the `toString` method
/// is required for its implementors, and the string conversion
/// should be implemented as a sensible default for printing to
/// the user when they write a syntactically incorrect query.
public sealed interface Token permits
        KeywordToken, IdentifierToken, StringToken,
        IntegerToken, SymbolToken, InvalidToken, EofToken {
    @Override
    String toString();
}