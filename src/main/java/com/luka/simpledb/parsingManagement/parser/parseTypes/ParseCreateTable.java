package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.statement.CreateTableStatement;
import com.luka.simpledb.parsingManagement.exceptions.BadSyntaxException;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.token.KeywordToken;
import com.luka.simpledb.parsingManagement.tokenizer.token.SymbolToken;
import com.luka.simpledb.recordManagement.Schema;

public class ParseCreateTable {
    private final ParserContext ctx;

    public ParseCreateTable(ParserContext ctx) {
        this.ctx = ctx;
    }

    public CreateTableStatement parse() {
        String tableName = ctx.eatIdentifier();

        ctx.eatDelimiter(SymbolToken.LEFT_PAREN);
        Schema schema = fieldDefinitions();
        ctx.eatDelimiter(SymbolToken.RIGHT_PAREN);

        return new CreateTableStatement(tableName, schema);
    }

    private Schema fieldDefinitions() {
        Schema schema = fieldDefinition();

        while (ctx.eatIfMatches(SymbolToken.COMMA)) {
            schema.addAll(fieldDefinition());
        }

        return schema;
    }

    private Schema fieldDefinition() {
        String fieldName = ctx.eatIdentifier();
        Schema schema = new Schema();

        if (ctx.eatIfMatches(new KeywordToken(Keyword.INT))) {
            schema.addIntField(fieldName, isNullable());
        } else if (ctx.eatIfMatches(new KeywordToken(Keyword.VARCHAR))) {
            ctx.eatDelimiter(SymbolToken.LEFT_PAREN);
            int stringLength = ctx.eatIntConstant();
            ctx.eatDelimiter(SymbolToken.RIGHT_PAREN);
            schema.addStringField(fieldName, stringLength, isNullable());
        } else if (ctx.eatIfMatches(new KeywordToken(Keyword.BOOLEAN))) {
            schema.addBooleanField(fieldName, isNullable());
        } else {
            throw new BadSyntaxException();
        }

        return schema;
    }

    private boolean isNullable() {
        if (ctx.eatIfMatches(new KeywordToken(Keyword.NOT))) {
            if (ctx.eatIfMatches(new KeywordToken(Keyword.NULL))) {
                return false;
            }
            throw new BadSyntaxException();
        }
        return true;
    }
}
