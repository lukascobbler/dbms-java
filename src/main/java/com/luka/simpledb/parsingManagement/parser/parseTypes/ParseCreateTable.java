package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.exceptions.ParsingException;
import com.luka.simpledb.parsingManagement.statement.CreateTableStatement;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.token.KeywordToken;
import com.luka.simpledb.parsingManagement.tokenizer.token.SymbolToken;
import com.luka.simpledb.queryManagement.exceptions.IncompatibleConstantTypeException;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;
import com.luka.simpledb.recordManagement.PhysicalSchema;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.recordManagement.exceptions.FieldDuplicateNameException;
import com.luka.simpledb.recordManagement.exceptions.FieldLimitException;

/// The class responsible for parsing table creation.
/// Its subgrammar is defined like this:
///
/// ```
/// <TableName>             := IdentificationToken
/// <FieldName>             := IdentificationToken
/// <ParseCreateTable>      := TABLE <TableName> (<FieldDefs>)
/// <FieldDefinitions>      := <FieldDefinition> [, <FieldDefinitions>]
/// <FieldDefinition>       := <FieldName> INT | VARCHAR (<Expression>) | BOOLEAN [NOT NULL]
/// ```
///
/// For CREATE TABLE commands, putting expressions as VARCHAR length values is technically
/// valid SQL, but these expressions must be constant and will be evaluated in the parser
/// instead of in the scan.
public class ParseCreateTable {
    private final ParserContext ctx;

    /// Every syntactic category requires the parse context to
    /// be initialized.
    public ParseCreateTable(ParserContext ctx) {
        this.ctx = ctx;
    }

    /// Parses the table creation statement according to the sub-grammar
    /// defined in the class documentation.
    ///
    /// @return Parsed data required for table creation.
    public CreateTableStatement parse() {
        ctx.eat(KeywordToken.TABLE);
        String tableName = tableName();

        ctx.eat(SymbolToken.LEFT_PAREN);
        Schema schema = fieldDefinitions();
        ctx.eat(SymbolToken.RIGHT_PAREN);

        return new CreateTableStatement(tableName, schema);
    }

    /// @return Parsed field definitions as a schema.
    /// @throws ParsingException if the schema has too many fields;
    /// if a field's name is duplicated.
    private Schema fieldDefinitions() {
        Schema allFieldsSchema = fieldDefinition();

        while (ctx.eatIfMatches(SymbolToken.COMMA)) {
            Schema schemaWithNewField = fieldDefinition();
            try {
                allFieldsSchema.addAll(schemaWithNewField);
            } catch (FieldLimitException e) {
                throw new ParsingException(String.format(
                        "The table has too many fields (MAX: %d)",
                        PhysicalSchema.MAX_FIELDS
                ));
            } catch (FieldDuplicateNameException e) {
                throw new ParsingException(String.format(
                        "Duplicated field: '%s'",
                        schemaWithNewField.getFields().getFirst()
                ));
            }
        }

        return allFieldsSchema;
    }

    /// @return Parsed one field definition wrapped in a schema object for
    /// easier addition of schemas.
    /// @throws ParsingException if the VARCHAR length isn't a constant or if it
    /// isn't an integer; if the DB type isn't recognized.
    private Schema fieldDefinition() {
        String fieldName = fieldName();
        PhysicalSchema schema = new PhysicalSchema();

        if (ctx.eatIfMatches(KeywordToken.INT)) {
            schema.addIntField(fieldName, isNullable());
        } else if (ctx.eatIfMatches(KeywordToken.VARCHAR)) {
            ctx.eat(SymbolToken.LEFT_PAREN);

            // Since VARCHAR can contain expressions as the length, they
            // must be calculated here instead of the planner to not complicate
            // Schema objects. The calculation fails if the user provides a
            // non-constant non-int expression as the value. Constant expressions
            // can be calculated without scans if they are 100% constant.
            Expression constantExpression = new ParseExpression(ctx).parse();

            if (!constantExpression.isConstant()) {
                throw new ParsingException("The varchar string length must be a constant expression");
            }

            int stringLength;
            try {
                stringLength = constantExpression.evaluate(null).asInt();
            } catch (IncompatibleConstantTypeException e) {
                throw new ParsingException("The varchar string length must be an integer");
            }

            ctx.eat(SymbolToken.RIGHT_PAREN);
            schema.addStringField(fieldName, stringLength, isNullable());
        } else if (ctx.eatIfMatches(KeywordToken.BOOLEAN)) {
            schema.addBooleanField(fieldName, isNullable());
        } else {
            throw new ParsingException("DB type not recognized");
        }

        return schema;
    }

    /// @return Whether a field is nullable, i.e. containing "NOT" + "NULL"
    /// keywords one after the other.
    /// @throws ParsingException if "NOT" is not followed by "NULL".
    private boolean isNullable() {
        if (ctx.eatIfMatches(KeywordToken.NOT)) {
            if (ctx.eatIfMatches(KeywordToken.NULL)) {
                return false;
            }
            throw new ParsingException("\"NOT\" not followed by \"NULL\" in a create table constraint");
        }
        return true;
    }

    /// @return The table name identifier string.
    private String tableName() {
        return ctx.eatIdentifier();
    }

    /// @return The field name identifier string.
    private String fieldName() {
        return ctx.eatIdentifier();
    }
}
