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
/// <Parse>     := <ParseSelect> | <ParseInsert> | <ParseUpdate> | <ParseDelete> |
///                CREATE <ParseCreate> | CREATE <ParseCreateView> | CREATE <ParseCreateIndex>
///```
///
/// todo update grammar on every modification of it
/// Each syntactic category is defined within its own class, for maintainability
/// and readability of the code. The main syntactic category (and the main entry point)
/// is the `<Parse>` category which references all other categories.
/// A parser is responsible for enforcing this grammar on provided
/// user queries and creating the parse tree. A parser can't know the semantic
/// correctness of the parse tree, and it can't enforce the lists being the same
/// size where they are used. For that, the planner is needed.
/// The concrete implementation is based on a "Recursive Descent" parsing strategy
/// with the "Pratt Parser" strategy for parsing arithmetic expressions.
public class Parser {
    private final ParserContext ctx;

    public Parser(String query) {
        ctx = new ParserContext(query);
    }

    public Statement parse() {
        Statement statement = switch (ctx.current()) {
            case KeywordToken(Keyword keyword) -> switch (keyword) {
                case SELECT -> new ParseSelect(ctx).parse();
                case INSERT -> new ParseInsert(ctx).parse();
                case UPDATE -> new ParseUpdate(ctx).parse();
                case DELETE -> new ParseDelete(ctx).parse();
                case CREATE -> {
                    ctx.advance();
                    yield switch (ctx.current()) {
                        case KeywordToken(Keyword subKeyword) -> switch (subKeyword) {
                            case TABLE -> new ParseCreateTable(ctx).parse();
                            case VIEW  -> new ParseCreateView(ctx).parse();
                            case INDEX -> new ParseCreateIndex(ctx).parse();
                            default -> throw new ParserException("Invalid CREATE target: " + subKeyword);
                        };
                        default -> throw new ParserException("Expected target after CREATE");
                    };
                }
                default -> throw new ParserException("Unexpected start of statement: " + keyword);
            };
            case EofToken() -> throw new ParserException("Unexpected end of input");
            default -> throw new ParserException("Expected keyword, found: " + ctx.current());
        };

        ctx.eat(SymbolToken.SEMICOLON);
        return statement;
    }
}