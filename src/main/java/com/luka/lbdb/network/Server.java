package com.luka.lbdb.network;

import com.luka.lbdb.db.LBDB;
import com.luka.lbdb.network.protocol.Protocol;
import com.luka.lbdb.network.protocol.response.Response;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/// The main entry point for receiving packets from a client.
/// Handles each client in a separate thread. Threads are managed
/// by a thread pool.
@SuppressWarnings("InfiniteLoopStatement")
public class Server {
    private final LBDB db;
    private boolean isShutdown = false;
    private final int port;

    private final ExecutorService threadPool;

    /// A LBDB server needs a port to work on (host is localhost), and the path
    /// representing a directory where the database will operate.
    public Server(int port, Path dbPath) {
        db = new LBDB(dbPath);
        this.port = port;
        this.threadPool = Executors.newFixedThreadPool(8);
    }

    /// Starts an infinite loop of accepting user connections, and
    /// sends their packets to a handler thread that is chosen from the
    /// thread pool.
    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("LBDB Server started on port " + port);
            while (!isShutdown) {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(() -> handleClient(clientSocket));
            }
        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
    }

    public void shutdown() {
        if (isShutdown) return;

        System.out.println("Shutting down DBMS server...");
        isShutdown = true;

        threadPool.shutdown();

        try {
            if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                System.err.println("Queries took too long, forcing shutdown.");
                threadPool.shutdownNow(); // todo will this safely shutdown
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        db.shutdown();
        System.out.println("Server stopped safely.");
    }

    /// Clients send packets in the format `4 BYTE PAYLOAD LENGTH | N BYTES STRING PAYLOAD`.
    /// A payload represents some command or query that should be executed on the system.
    /// The handler processes the command which outputs a [Response] object, that is then
    /// serialized using the [Protocol] implementation and sent to the client as bytes.
    private void handleClient(Socket socket) {
        try (socket; DataInputStream in = new DataInputStream(socket.getInputStream());
             OutputStream out = socket.getOutputStream()) {
            long sessionId = socket.getRemoteSocketAddress().hashCode();
            try {
                while (true) {
                    byte[] header = new byte[4];
                    in.readFully(header);
                    int payloadLength = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN).getInt();

                    byte[] payload = new byte[payloadLength];
                    in.readFully(payload);

                    String queryOrCommand = new String(payload, StandardCharsets.UTF_8);

                    if (queryOrCommand.equals("SHUTDOWN;")) {
                        shutdown();
                        break; // todo shutdown callbacks
                    }

                    Response response = db.getPlanner().execute(queryOrCommand, sessionId);

                    byte[] responseBytes = Protocol.serialize(response);
                    out.write(responseBytes);
                    out.flush();
                }
            } catch (EOFException e) {
                db.getTransactionManager().rollbackManualTransaction(sessionId);
                System.out.println("Client disconnected.");
            } catch (IOException e) {
                db.getTransactionManager().rollbackManualTransaction(sessionId);
                System.err.println("Client handler error: " + e.getMessage());
            }
        } catch (IOException ignored) {
        }
    }
}
