package com.luka.lbdbclient;

import com.luka.lbdb.network.protocol.Protocol;
import com.luka.lbdb.network.protocol.response.EmptySet;
import com.luka.lbdb.network.protocol.response.ErrorResponse;
import com.luka.lbdb.network.protocol.response.QuerySet;
import com.luka.lbdb.network.protocol.response.Response;
import com.luka.lbdb.parsing.tokenizer.token.KeywordToken;
import org.jline.reader.*;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/// The class containing a main function, responsible for parsing the
/// arguments for starting a LBDB client instance.
public class LBDBClient implements AutoCloseable {
    private final Socket socket;
    private final DataInputStream in;
    private final OutputStream out;

    /// A client needs to know what is the port of the server.
    public LBDBClient(int port) throws IOException {
        this.socket = new Socket("localhost", port);
        this.in = new DataInputStream(socket.getInputStream());
        this.out = socket.getOutputStream();
    }

    /// The main client loop. Uses a custom terminal wrapper by [Jline](https://jline.org/) that
    /// helps with history, autocomplete, and the general feeling of a shell-like app.
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("USAGE: LBDBClient <port>");
            System.exit(1);
        }

        int port = -1;
        try {
            port = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.printf("Invalid port: %s%n", args[0]);
            System.exit(2);
        }

        try (Terminal terminal = TerminalBuilder.builder().system(true).build();
             LBDBClient client = new LBDBClient(port)) {

            List<String> autoCompletableStrings = new java.util.ArrayList<>(
                    Arrays.stream(KeywordToken.values())
                    .map(k -> k.name().toUpperCase())
                    .toList()
            );

            autoCompletableStrings.addAll(
                    Arrays.stream(KeywordToken.values())
                    .map(k -> k.name().toLowerCase()).toList()
            );

            Completer sqlCompleter = new StringsCompleter(autoCompletableStrings);

            LineReader reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .completer(sqlCompleter)
                    .history(new DefaultHistory())
                    .variable(LineReader.SECONDARY_PROMPT_PATTERN, "    > ")
                    .variable(LineReader.HISTORY_FILE, "./lbdbclient_history.txt")
                    .build();

            StringBuilder queryBuffer = new StringBuilder();
            PrintWriter writer = terminal.writer();

            writer.printf("LBDB Shell. Connected to port %d\n", port);
            writer.println("Type 'exit' to close.\n");
            terminal.flush();

            while (true) {
                String prompt = queryBuffer.isEmpty() ? "LBDB> " : "    > ";
                String line;

                try {
                    line = reader.readLine(prompt);
                    if (line == null) break;
                    line = line.trim();
                } catch (UserInterruptException e) {
                    queryBuffer.setLength(0);
                    writer.println("^C");
                    continue;
                } catch (EndOfFileException e) {
                    break;
                }

                if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) break;
                if (line.isEmpty()) continue;

                queryBuffer.append(line).append(" ");

                if (line.endsWith(";")) {
                    String fullQuery = queryBuffer.toString().trim();
                    queryBuffer.setLength(0);

                    long start = System.nanoTime();

                    try {
                        Response response = client.executeQuery(fullQuery);

                        long end = System.nanoTime();
                        double durationMs = (end - start) / 1_000_000.0;

                        handleResponse(response, terminal);
                        writer.printf("(%.2f ms)%n", durationMs);
                        terminal.flush();

                    } catch (IOException e) {
                        writer.println("Execution error: " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Shell error: " + e.getMessage());
        }
    }

    /// Sends a query object to the socket, and reads the whole
    /// output.
    ///
    /// @return The deserialized response object from the server.
    public Response executeQuery(String query) throws IOException {
        byte[] queryBytes = query.getBytes(StandardCharsets.UTF_8);

        ByteBuffer header = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        header.putInt(queryBytes.length);
        out.write(header.array());

        out.write(queryBytes);
        out.flush();

        byte[] responseHeader = new byte[4];
        in.readFully(responseHeader);
        int responseLength = ByteBuffer.wrap(responseHeader)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();

        byte[] responsePayload = new byte[responseLength];
        in.readFully(responsePayload);

        return Protocol.deserialize(responsePayload);
    }

    /// Different prints based on different server responses.
    private static void handleResponse(Response response, Terminal terminal) {
        PrintWriter out = terminal.writer();
        switch (response) {
            case EmptySet emptySet -> out.println("Rows affected: " + emptySet.rowsAffected());
            case ErrorResponse errorResponse -> out.println("Database error: " + errorResponse.error());
            case QuerySet querySet -> {
                out.print(TablePrinter.print(querySet.schema(), querySet.tuples()));
                out.println(querySet.tuples().size() + " rows");
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (socket != null) socket.close();
    }
}
