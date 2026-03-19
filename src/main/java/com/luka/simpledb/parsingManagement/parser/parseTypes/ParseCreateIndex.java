package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.metadataManagement.infoClasses.IndexType;
import com.luka.simpledb.parsingManagement.exceptions.ParsingException;
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
/// <ParseCreateIndex>      := INDEX <IndexName> ON <TableName> (<FieldName>) [TYPE BTREE | HASH]
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

        IndexType type = IndexType.BTREE;
        if (ctx.eatIfMatches(KeywordToken.TYPE)) {
            if (ctx.eatIfMatches(KeywordToken.HASH)) type = IndexType.HASH;
            else if (ctx.eatIfMatches(KeywordToken.BTREE)) {}
            else throw new ParsingException("No correct index type provided");
        }

        return new CreateIndexStatement(indexName, tableName, fieldName, type);
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
