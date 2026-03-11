package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.exceptions.ParserException;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.token.KeywordToken;
import com.luka.simpledb.parsingManagement.tokenizer.token.SymbolToken;
import com.luka.simpledb.parsingManagement.tokenizer.token.Token;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;
import com.luka.simpledb.queryManagement.virtualEntities.term.Term;
import com.luka.simpledb.queryManagement.virtualEntities.term.TermOperator;

/// The class responsible for parsing predicates.
/// Its subgrammar is defined like this:
///
/// ```
/// <Term>              := <ParseExpression> "=" | "!=" | ">" | "<" | ">=" | "<=" | "IS" <ParseExpression>
/// <Predicate>         := <Term> [AND<Predicate>]
/// ```
public class ParsePredicate {
    private final ParserContext ctx;
    private final ParseExpression exprParser;

    public ParsePredicate(ParserContext ctx) {
        this.ctx = ctx;
        this.exprParser = new ParseExpression(ctx);
    }

    public Predicate parse() {
        Predicate predicate = new Predicate(parseTerm());

        while (ctx.eatIfMatches(Keyword.AND)) {
            predicate.conjoinWith(new Predicate(parseTerm()));
        }

        return predicate;
    }

    private Term parseTerm() {
        Expression lhs = exprParser.parse();
        TermOperator op = parseTermOperator();
        Expression rhs = exprParser.parse();
        // todo decide where to call PartialEvaluator
        //  must be before code enters the scan, as to not be
        //  executed on the VM, but must be after the parser to
        //  correctly test the parser
        return new Term(lhs, op, rhs);
    }

    private TermOperator parseTermOperator() {
        Token token = ctx.current();
        ctx.advance();

        return switch (token) {
            case SymbolToken st -> switch (st) {
                case EQUAL -> TermOperator.EQUALS;
                case NOT_EQUAL -> TermOperator.NOT_EQUALS;
                case GREATER_THAN -> TermOperator.GREATER_THAN;
                case LESS_THAN -> TermOperator.LESS_THAN;
                case GREATER_THAN_OR_EQUAL -> TermOperator.GREATER_OR_EQUAL;
                case LESS_THAN_OR_EQUAL -> TermOperator.LESS_OR_EQUAL;
                default -> throw new ParserException("Expected comparison operator, found: " + st);
            };
            case KeywordToken kt when kt.keyword() == Keyword.IS -> TermOperator.IS;
            default -> throw new ParserException("Expected comparison operator, found: " + token);
        };
    }
}