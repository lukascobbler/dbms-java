package com.luka.simpledb.parsingManagement;

import com.luka.simpledb.parsingManagement.exceptions.BadSyntaxException;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Collection;

/// A lexer is a parsing concept that converts a character stream (the string query)
/// into a more grouped concept called tokens. Tokens are syntactically more valuable
/// than characters because they group these characters together into meaningful wholes.
public class Lexer {
    private final Collection<String> keywords;
    private final StreamTokenizer streamTokenizer;

    /// Initialize a lexer with the provided query.
    ///
    /// @throws BadSyntaxException if the provided query is empty.
    public Lexer(String query) {
        if (query.isEmpty()) {
            throw new BadSyntaxException();
        }
        keywords = Keyword.asStrings();
        streamTokenizer = new StreamTokenizer(new StringReader(query));
        streamTokenizer.ordinaryChar('.');
        streamTokenizer.wordChars('_', '_');
        streamTokenizer.lowerCaseMode(true);
        nextToken();
    }

    /// @return True if the current token is the passed delimiter.
    public boolean matchDelimiter(char d) {
        return d == (char)streamTokenizer.ttype;
    }

    /// @return True if the current token is an integer constant.
    public boolean matchIntConstant() {
        return streamTokenizer.ttype == StreamTokenizer.TT_NUMBER;
    }

    /// @return True if the current token is a string constant.
    public boolean matchStringConstant() {
        return '\'' == (char)streamTokenizer.ttype;
    }

    /// @return True if the current token is a boolean constant.
    public boolean matchBooleanConstant() {
        return streamTokenizer.ttype == StreamTokenizer.TT_WORD &&
                (streamTokenizer.sval.equals(Keyword.TRUE.toString()) ||
                        streamTokenizer.sval.equals(Keyword.FALSE.toString()));
    }

    /// @return True if the current token is a null constant.
    public boolean matchNullConstant() {
        return streamTokenizer.ttype == StreamTokenizer.TT_WORD &&
                streamTokenizer.sval.equals(Keyword.NULL.toString());
    }

    /// @return True if the current token is the passed keyword.
    public boolean matchKeyword(Keyword keyword) {
        return streamTokenizer.ttype == StreamTokenizer.TT_WORD &&
                streamTokenizer.sval.equals(keyword.toString());
    }

    /// @return True if the current token is an identifier (it's not a keyword.)
    public boolean matchIdentifier() {
        return streamTokenizer.ttype == StreamTokenizer.TT_WORD &&
                !keywords.contains(streamTokenizer.sval);
    }

    /// Consume the delimiter token.
    ///
    /// @throws BadSyntaxException if the current token isn't
    /// a delimiter.
    public void eatDelimiter(char d) {
        if (!matchDelimiter(d)) throw new BadSyntaxException();

        nextToken();
    }

    /// Consume the integer constant token.
    ///
    /// @throws BadSyntaxException if the current token isn't
    /// an integer constant.
    public int eatIntConstant() {
        if (!matchIntConstant()) throw new BadSyntaxException();

        int i = (int) streamTokenizer.nval;
        nextToken();
        return i;
    }

    /// Consume the string constant token.
    ///
    /// @throws BadSyntaxException if the current token isn't
    /// a string constant.
    public String eatStringConstant() {
        if (!matchStringConstant()) throw new BadSyntaxException();

        String s = streamTokenizer.sval;
        nextToken();
        return s;
    }

    /// Consume the boolean constant token.
    ///
    /// @throws BadSyntaxException if the current token isn't
    /// a boolean constant.
    public boolean eatBooleanConstant() {
        if (!matchBooleanConstant()) throw new BadSyntaxException();

        boolean b = streamTokenizer.sval.equals(Keyword.TRUE.toString());
        nextToken();
        return b;
    }

    /// Consume the null constant token.
    ///
    /// @throws BadSyntaxException if the current token isn't
    /// a null constant.
    public void eatNullConstant() {
        if (!matchNullConstant()) throw new BadSyntaxException();

        nextToken();
    }

    /// Consume the keyword token.
    ///
    /// @throws BadSyntaxException if the current token isn't
    /// the passed keyword.
    public void eatKeyword(Keyword word) {
        if (!matchKeyword(word)) throw new BadSyntaxException();

        nextToken();
    }

    /// Consume multiple keyword tokens.
    ///
    /// @throws BadSyntaxException any of the current tokens aren't
    /// the passed keywords in order.
    public void eatKeywords(Keyword... words) {
        for (Keyword w : words) {
            if (!matchKeyword(w)) throw new BadSyntaxException();
            nextToken();
        }

        nextToken();
    }

    /// Consume the identifier token.
    ///
    /// @throws BadSyntaxException if the current token isn't
    /// an identifier token.
    public String eatIdentifier() {
        if (!matchIdentifier()) throw new BadSyntaxException();

        String s = streamTokenizer.sval;
        nextToken();
        return s;
    }

    /// Advance the token to the next token.
    ///
    /// @throws BadSyntaxException if the internal stream tokenizer
    /// encounters an io error.
    private void nextToken() {
        try {
            streamTokenizer.nextToken();
        } catch (IOException e) {
            throw new BadSyntaxException();
        }
    }
}
