package com.luka.simpledb.parsingManagement;

import com.luka.simpledb.parsingManagement.data.*;
import com.luka.simpledb.parsingManagement.exceptions.BadSyntaxException;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.constant.*;
import com.luka.simpledb.queryManagement.virtualEntities.expression.ConstantExpression;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;
import com.luka.simpledb.queryManagement.virtualEntities.expression.FieldNameExpression;
import com.luka.simpledb.queryManagement.virtualEntities.term.Term;
import com.luka.simpledb.queryManagement.virtualEntities.term.TermOperator;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.recordManagement.exceptions.DatabaseTypeNotImplementedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
/// The concrete implementation is based on a "Recursive Descent" parsing strategy.
/// todo and the "Pratt parser" expression parser (term parser, expression parser)
/// todo every syntactic category <> is a separate class
/// Every method can implicitly throw `BadSyntaxException` if the syntax isn't
/// according to the grammar.
public class Parser {
    private final Lexer lexer;

    /// Initialization of the parser with the user provided query.
    public Parser(String query) {
        lexer = new Lexer(query);
    }

    // predicate parsing

    /// @return The identifier from the lexer.
    public String field() {
        return lexer.eatIdentifier();
    }

    /// Matches a constant value from the lexer.
    ///
    /// @return The matched constant as a `Constant`.
    /// @throws DatabaseTypeNotImplementedException if the type of the constant
    /// isn't implemented in the system.
    public Constant constant() {
        if (lexer.matchBooleanConstant()) {
            return new BooleanConstant(lexer.eatBooleanConstant());
        } else if (lexer.matchIntConstant()) {
            return new IntConstant(lexer.eatIntConstant());
        } else if (lexer.matchStringConstant()) {
            return new StringConstant(lexer.eatStringConstant());
        } else if (lexer.matchNullConstant()) {
            return NullConstant.INSTANCE;
        }

        throw new DatabaseTypeNotImplementedException();
    }

    /// Creates an expression by matching identifiers, constants
    /// and arithmetic operations.
    public Expression expression() {
        if (lexer.matchIdentifier()) {
            return new FieldNameExpression(field());
        } else {
            return new ConstantExpression(constant());
        }

        // todo arithmetic expression
    }

    /// @return A term of two expressions and the operator
    /// between them.
    public Term term() {
        Expression lhs = expression();
        TermOperator op;
        if (lexer.matchDelimiter('=')) {
            lexer.eatDelimiter('=');
            op = TermOperator.EQUALS;
        } else if (lexer.matchDelimiter('>')) {
            lexer.eatDelimiter('>');
            op = TermOperator.GREATER_THAN;
        } else if (lexer.matchDelimiter('<')) {
            lexer.eatDelimiter('<');
            op = TermOperator.LESS_THAN;
        } // todo multi line delimiters such as: >=, <=, !=, IS
        Expression rhs = expression();
        return new Term(lhs, TermOperator.EQUALS, rhs);
    }

    /// A predicate of multiple terms, with "AND" implicitly
    /// between them.
    public Predicate predicate() {
        Predicate predicate = new Predicate(term());
        if (lexer.matchKeyword(Keyword.AND)) {
            lexer.eatKeyword(Keyword.AND);
            predicate.conjoinWith(predicate());
        }
        return predicate;
    }

    // query commands

    /// @return Data required for the appropriate query command.
    public QueryData query() {
        lexer.eatKeyword(Keyword.SELECT);
        List<String> fields = selectList();

        lexer.eatKeyword(Keyword.FROM);
        Collection<String> tables = tableList();

        Predicate predicate = new Predicate();
        if (lexer.matchKeyword(Keyword.WHERE)) {
            lexer.eatKeyword(Keyword.WHERE);
            predicate = predicate();
        }

        lexer.eatDelimiter(';');

        return new QueryData(fields, tables, predicate);
    }

    /// @return The list of all selection expressions.
    private List<String> selectList() {
        List<String> selectList = new ArrayList<>();
        selectList.add(field()); // todo add expressions here instead of fields

        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            selectList.addAll(selectList());
        }

        return selectList;
    }

    /// @return The list of all table names. todo change collection to list if possible
    private Collection<String> tableList() {
        Collection<String> tableList = new ArrayList<>();
        tableList.add(lexer.eatIdentifier());

        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            tableList.addAll(tableList());
        }

        return tableList;
    }

    // modify commands

    /// Parses a modification command:
    /// - creation: table, view, index
    /// - updating
    /// - insertion
    /// - deletion
    ///
    /// @return Data required for the appropriate modification command.
    public Object modifyCommand() { // todo refactor to not return object
        if (lexer.matchKeyword(Keyword.INSERT)) {
            return insert();
        } else if (lexer.matchKeyword(Keyword.CREATE)) {
            return create();
        } else if (lexer.matchKeyword(Keyword.UPDATE)) {
            return update();
        } else if (lexer.matchKeyword(Keyword.DELETE)) {
            return delete();
        }

        lexer.eatDelimiter(';');

        throw new BadSyntaxException();
    }

    /// Parses a create command:
    /// - table
    /// - view
    /// - index
    ///
    /// @return Data required for the appropriate creation command.
    private Object create() {
        lexer.eatKeyword(Keyword.CREATE);

        if (lexer.matchKeyword(Keyword.TABLE)) {
            return createTable();
        } else if (lexer.matchKeyword(Keyword.VIEW)) {
            return createView();
        } else if (lexer.matchKeyword(Keyword.INDEX)) {
            return createIndex();
        }

        lexer.eatDelimiter(';');

        throw new BadSyntaxException();
    }

    // deletion commands

    /// @return Data required for the appropriate delete command.
    public DeleteData delete() {
        lexer.eatKeywords(Keyword.DELETE, Keyword.FROM);

        String tableName = lexer.eatIdentifier();
        Predicate predicate = new Predicate();

        if (lexer.matchKeyword(Keyword.WHERE)) {
            lexer.eatKeyword(Keyword.WHERE);
            predicate = predicate();
        }

        return new DeleteData(tableName, predicate);
    }

    // insertion commands

    /// @return Data required for the appropriate insert command.
    public InsertData insert() {
        lexer.eatKeywords(Keyword.INSERT, Keyword.INTO);

        String tableName = lexer.eatIdentifier();

        lexer.eatDelimiter('(');
        List<String> fields = fieldList();
        lexer.eatDelimiter(')');

        lexer.eatKeyword(Keyword.VALUES);

        lexer.eatDelimiter('(');
        List<Constant> values = constantList(); // todo possibly not constants but expressions that arent fieldNames
        lexer.eatDelimiter(')');

        return new InsertData(tableName, fields, values);
    }

    /// @return The list of all constants.
    private List<Constant> constantList() {
        List<Constant> constantList = new ArrayList<>();
        constantList.add(constant());

        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            constantList.addAll(constantList());
        }

        return constantList;
    }

    /// @return The list of all fields.
    private List<String> fieldList() {
        List<String> fieldList = new ArrayList<>();
        fieldList.add(field());

        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            fieldList.addAll(fieldList());
        }

        return fieldList;
    }

    // update commands

    /// @return Data required for the appropriate update command.
    public UpdateData update() {
        lexer.eatKeyword(Keyword.UPDATE);
        String tableName = lexer.eatIdentifier();
        lexer.eatKeyword(Keyword.SET);

        String fieldName = field();
        lexer.eatDelimiter('=');

        Expression newValue = expression();

        Predicate predicate = new Predicate();
        if (lexer.matchKeyword(Keyword.WHERE)) {
            lexer.eatKeyword(Keyword.WHERE);
            predicate = predicate();
        }

        return new UpdateData(tableName, fieldName, newValue, predicate);
    }

    // create table commands

    /// @return Data required for the appropriate create table command.
    public CreateTableData createTable() {
        lexer.eatKeyword(Keyword.TABLE);
        String tableName = lexer.eatIdentifier();

        lexer.eatDelimiter('(');
        Schema schema = fieldDefinitions();
        lexer.eatDelimiter(')');

        return new CreateTableData(tableName, schema);
    }

    /// @return The schema that corresponds to the field definitions.
    private Schema fieldDefinitions() {
        Schema schema = fieldDefinition();

        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');

            Schema schema2 = fieldDefinitions();
            schema.addAll(schema2);
        }

        return schema;
    }

    /// @return The schema that corresponds to one field definition.
    private Schema fieldDefinition() {
        String fieldName = field();

        Schema schema = new Schema();

        if (lexer.matchKeyword(Keyword.INT)) {
            lexer.eatKeyword(Keyword.INT);

            schema.addIntField(fieldName, isNullable());
        } else if (lexer.matchKeyword(Keyword.VARCHAR)) {
            lexer.eatKeyword(Keyword.VARCHAR);
            lexer.eatDelimiter('(');
            int stringLength = lexer.eatIntConstant();
            lexer.eatDelimiter(')');

            schema.addStringField(fieldName, stringLength, isNullable());
        } else if (lexer.matchKeyword(Keyword.BOOLEAN)) {
            lexer.eatKeyword(Keyword.BOOLEAN);

            schema.addBooleanField(fieldName, isNullable());
        }

        return schema;
    }

    /// @return Whether the filed is nullable, i.e. if keywords "NOT NULL" are
    /// after the field type.
    private boolean isNullable() {
        if (lexer.matchKeyword(Keyword.NOT)) {
            lexer.eatKeyword(Keyword.NOT);

            if (lexer.matchKeyword(Keyword.NULL)) {
                lexer.eatKeyword(Keyword.NULL);
                return false;
            }
        }

        return true;
    }

    // create view commands

    /// @return Data required for the appropriate create view command.
    public CreateViewData createView() {
        lexer.eatKeyword(Keyword.VIEW);
        String viewName = lexer.eatIdentifier();
        lexer.eatKeyword(Keyword.AS);
        QueryData queryData = query();

        return new CreateViewData(viewName, queryData);
    }

    // create index commands

    /// @return Data required for the appropriate create index command.
    public CreateIndexData createIndex() {
        lexer.eatKeyword(Keyword.INDEX);
        String indexName = lexer.eatIdentifier();
        lexer.eatKeyword(Keyword.ON);
        String tableName = lexer.eatIdentifier();
        lexer.eatDelimiter('(');
        String fieldName = field();
        lexer.eatDelimiter(')');

        return new CreateIndexData(indexName, tableName, fieldName);
    }
}
