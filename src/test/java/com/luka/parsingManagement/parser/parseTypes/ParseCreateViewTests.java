package com.luka.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.exceptions.ParsingException;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.parser.parseTypes.ParseCreateView;
import com.luka.simpledb.parsingManagement.statement.CreateViewStatement;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ParseCreateViewTests {
    private CreateViewStatement parse(String query) {
        ParserContext ctx = new ParserContext(query);
        return new ParseCreateView(ctx).parse();
    }

    @Test
    public void parseSimpleView() {
        String query = "VIEW active_users AS SELECT * FROM users WHERE status = 'active'";
        String expected = "CREATE VIEW active_users AS SELECT * FROM users WHERE status = 'active';";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseViewWithExpressionsAndAliases() {
        String query = "VIEW report_view AS SELECT id, sal + bonus AS total_pay FROM employees";
        String expected = "CREATE VIEW report_view AS SELECT id, (sal + bonus) AS total_pay FROM employees;";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseViewWithJoins() {
        String query = "VIEW student_dept AS SELECT SName, DName FROM STUDENT JOIN DEPT ON MajorId = Did";
        String expected = "CREATE VIEW student_dept AS SELECT sname, dname FROM student, dept WHERE majorid = did;";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseViewWithUnion() {
        String query = "VIEW all_contacts AS SELECT name FROM customers UNION SELECT name FROM suppliers";
        String expected = "CREATE VIEW all_contacts AS SELECT name FROM customers UNION SELECT name FROM suppliers;";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseFailMissingAsKeyword() {
        assertThrows(ParsingException.class, () -> parse("VIEW my_view SELECT * FROM t"));
    }

    @Test
    public void parseFailMissingViewName() {
        assertThrows(ParsingException.class, () -> parse("VIEW AS SELECT * FROM t"));
    }

    @Test
    public void parseFailMissingSelectStatement() {
        assertThrows(ParsingException.class, () -> parse("VIEW my_view AS"));
    }

    @Test
    public void parseFailWithBrokenSelect() {
        assertThrows(ParsingException.class, () -> parse("VIEW my_view AS SELECT FROM table"));
    }

    @Test
    public void parseFailWithDanglingUnion() {
        assertThrows(ParsingException.class, () -> parse("my_view AS SELECT a FROM b UNION"));
    }
}
