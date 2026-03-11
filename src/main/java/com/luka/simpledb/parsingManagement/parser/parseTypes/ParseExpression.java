package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.exceptions.ParserException;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.token.*;
import com.luka.simpledb.queryManagement.virtualEntities.constant.BooleanConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.IntConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.NullConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.StringConstant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.*;

/// The class responsible for parsing expressions using "Pratt Parsing".
/// Its subgrammar is defined like this:
///
/// ```
/// <Constant>                      := StringToken | IntToken | BooleanToken | NullKeyword
/// <Expression>                    := <BinaryArithmeticExpression> | <UnaryArithmeticExpression> |
///                                    <PrimaryExpression>
/// <PrimaryExpression>             := <FieldExpression> | <ConstantExpression> |
///                                    <WildcardExpression> | "(" <Expression> ")"
/// <BinaryArithmeticExpression>    := <Expression> ( "*" | "/" | "+" | "-" ) <Expression>
/// <UnaryArithmeticExpression>     := ( "+" | "-" ) <Expression>
/// ```
public class ParseExpression {
    private static final int PREFIX_PRECEDENCE = 30;
    private final ParserContext ctx;

    public ParseExpression(ParserContext ctx) {
        this.ctx = ctx;
    }

    public Expression parse() {
        return parseExpression(0);
    }

    private Expression parseExpression(int precedence) {
        Expression left = parsePrefix();

        while (precedence < getPrecedence(ctx.current())) {
            Token opToken = ctx.current();
            ctx.advance();
            left = parseInfix(left, opToken);
        }

        return left;
    }

    private Expression parsePrefix() {
        Token current = ctx.current();
        ctx.advance();

        return switch (current) {
            case IdentifierToken(String name) -> new FieldNameExpression(name);
            case IntegerToken(int val) -> new ConstantExpression(new IntConstant(val));
            case StringToken(String val) -> new ConstantExpression(new StringConstant(val));
            case KeywordToken(Keyword kw) -> switch (kw) {
                case TRUE -> new ConstantExpression(new BooleanConstant(true));
                case FALSE -> new ConstantExpression(new BooleanConstant(false));
                case NULL -> new ConstantExpression(NullConstant.INSTANCE);
                default -> throw new ParserException("Unexpected keyword in expression: " + kw);
            };
            case SymbolToken sym -> switch (sym) {
                case MINUS -> new UnaryArithmeticExpression(ArithmeticOperator.SUB, parseExpression(PREFIX_PRECEDENCE));
                case PLUS  -> new UnaryArithmeticExpression(ArithmeticOperator.ADD, parseExpression(PREFIX_PRECEDENCE));
                case STAR  -> new WildcardExpression();
                case LEFT_PAREN -> {
                    Expression inner = parseExpression(0);
                    ctx.eat(SymbolToken.RIGHT_PAREN);
                    yield inner;
                }
                default -> throw new ParserException("Unexpected symbol: " + sym);
            };
            default -> throw new ParserException("Expected expression, found: " + current);
        };
    }

    private Expression parseInfix(Expression left, Token opToken) {
        int precedence = getPrecedence(opToken);
        Expression right = parseExpression(precedence);

        ArithmeticOperator op = switch (opToken) {
            case SymbolToken st when st == SymbolToken.PLUS -> ArithmeticOperator.ADD;
            case SymbolToken st when st == SymbolToken.MINUS -> ArithmeticOperator.SUB;
            case SymbolToken st when st == SymbolToken.STAR -> ArithmeticOperator.MUL;
            case SymbolToken st when st == SymbolToken.DIVIDE -> ArithmeticOperator.DIV;
            default -> throw new ParserException("Unknown arithmetic operator: " + opToken);
        };

        return new BinaryArithmeticExpression(left, op, right);
    }

    private int getPrecedence(Token token) {
        if (token == SymbolToken.STAR || token == SymbolToken.DIVIDE) return 20;
        if (token == SymbolToken.PLUS || token == SymbolToken.MINUS) return 10;
        return 0;
    }
}