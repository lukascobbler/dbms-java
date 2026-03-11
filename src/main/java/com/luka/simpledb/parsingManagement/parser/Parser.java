package com.luka.simpledb.parsingManagement.parser;

import com.luka.simpledb.parsingManagement.exceptions.ParserException;
import com.luka.simpledb.parsingManagement.parser.parseTypes.*;
import com.luka.simpledb.parsingManagement.statement.Statement;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.token.*;

/// The parsing system's grammar, which corresponds to the operations that
/// the virtual machine is able to execute, can be defined as this:
///
/// ```
/// <Field>         := IdentificationToken
/// <Constant>      := StringToken | IntToken | BooleanToken
/// <Expression>    := <Field> | <Constant>
/// <Term>          := <Expression> = <Expression>
/// <Predicate>     := <Term> [AND<Predicate>]
///
/// <Query>         := SELECT <SelectList> FROM <TableList> [WHERE<Predicate>]
/// <SelectList>    := <Field> [, <SelectList>]
/// <TableList>     := IdentificationToken [, <TableList>]
///
/// <UpdateCmd>     := <Insert> | <Delete> | <Modify> | <Create>
/// <Create>        := CREATE <CreateTable> | <CreateView> | <CreateIndex>
///
/// <Insert>        := INSERT INTO IdentificationToken (<FieldList> ) VALUES ( <ConstList>)
/// <FieldList>     := <Field> [, <FieldList>]
/// <ConstList>     := <Constant> [, <ConstList>]
///
/// <Delete>        := DELETE FROM IdentificationToken [WHERE<Predicate>]
///
/// <Modify>        := UPDATE IdentificationToken SET <Field> = <Expression> [WHERE<Predicate>]
///
/// <CreateTable>   := TABLE IdentificationToken (<FieldDefs>)
/// <FieldDefs>     := <FieldDef> [, <FieldDefs>]
/// <FieldDef>      := IdentificationToken INT | VARCHAR (IntToken) | BOOLEAN [NOT NULL]
///
/// <CreateView>    := VIEW IdentificationToken AS <Query>
/// <CreateIndex>   := INDEX IdentificationToken ON IdentificationToken (<Field>)
/// ```
/// todo update grammar on every modification of it
/// A parser is responsible for enforcing this grammar on provided
/// user queries and creating the parse tree. A parser can't know the semantic
/// correctness of the parse tree, and it can't enforce the lists being the same
/// size where they are used. For that, the planner is needed.
/// The concrete implementation is based on a "Recursive Descent" parsing strategy
/// with the Pratt parser strategy for parsing arithmetic expressions.
public class Parser {
    private final ParserContext ctx;

    public Parser(String query) {
        ctx = new ParserContext(query);
    }

    public Statement parse() {
        Statement statement = switch (ctx.current()) {
            case KeywordToken(Keyword keyword) -> {
                ctx.advance();
                yield switch (keyword) {
                    case INSERT -> new ParseInsert(ctx).parse();
                    case UPDATE -> new ParseUpdate(ctx).parse();
                    case DELETE -> new ParseDelete(ctx).parse();
                    case SELECT -> new ParseSelect(ctx).parse();
                    case CREATE -> switch (ctx.current()) {
                        case KeywordToken(Keyword subKeyword) -> {
                            ctx.advance();
                            yield switch (subKeyword) {
                                case TABLE -> new ParseCreateTable(ctx).parse();
                                case VIEW  -> new ParseCreateView(ctx).parse();
                                case INDEX -> new ParseCreateIndex(ctx).parse();
                                default -> throw new ParserException("Invalid CREATE target: " + subKeyword);
                            };
                        }
                        default -> throw new ParserException("Expected target after CREATE");
                    };
                    default -> throw new ParserException("Unexpected start of statement: " + keyword);
                };
            }
            case EofToken() -> throw new ParserException("Unexpected end of input");
            default -> throw new ParserException("Expected keyword, found: " + ctx.current());
        };

        ctx.eatDelimiter(SymbolToken.SEMICOLON);
        return statement;
    }
}