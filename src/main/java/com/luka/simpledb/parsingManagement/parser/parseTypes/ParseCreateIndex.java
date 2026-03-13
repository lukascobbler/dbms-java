package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.statement.CreateIndexStatement;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.token.KeywordToken;
import com.luka.simpledb.parsingManagement.tokenizer.token.SymbolToken;

/// The class responsible for parsing index creation.
/// Its subgrammar is defined like this:
///
/// ```
/// <IndexName>             := IdentificationToken
/// <TableName>             := IdentificationToken
/// <FieldName>             := IdentificationToken
/// <ParseCreateIndex>      := INDEX <IndexName> ON <TableName> (<FieldName>)
/// ```
public class ParseCreateIndex {
    private final ParserContext ctx;

    /// Every syntactic category requires the parse context to
    /// be initialized.
    public ParseCreateIndex(ParserContext ctx) {
        this.ctx = ctx;
    }

    /// Parses the index creation statement according to the sub-grammar
    /// defined in the class documentation.
    ///
    /// @return Parsed data required for index creation.
    public CreateIndexStatement parse() {
        ctx.eat(KeywordToken.INDEX);
        String indexName = indexName();
        ctx.eat(KeywordToken.ON);
        String tableName = tableName();
        ctx.eat(SymbolToken.LEFT_PAREN);
        String fieldName = fieldName();
        ctx.eat(SymbolToken.RIGHT_PAREN);

        return new CreateIndexStatement(indexName, tableName, fieldName);
    }

    /// @return The index name identifier string.
    private String indexName() {
        return ctx.eatIdentifier();
    }

    /// @return The field name identifier string.
    private String fieldName() {
        return ctx.eatIdentifier();
    }

    /// @return The table name identifier string.
    private String tableName() {
        return ctx.eatIdentifier();
    }
}
