package com.luka.lbdb;

import com.luka.lbdb.network.Server;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;

/// The class containing a main function, responsible for parsing the
/// arguments for starting a LBDB server instance.
public class LBDBServer {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("USAGE: LBDB <db path> <port>");
            System.exit(1);
        }

        try {
            Path dbPath = Path.of(args[0]);
            int port = Integer.parseInt(args[1]);

            Server server = new Server(port, dbPath);
            server.start();
        } catch (InvalidPathException e) {
            System.out.printf("Invalid path: %s", args[0]);
            System.exit(2);
        } catch (NumberFormatException e) {
            System.out.printf("Invalid port: %s", args[1]);
            System.exit(3);
        }
    }
}
