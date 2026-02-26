package com.luka.simpledb.simpleDB;

/// Settings class for changing the instantiation parameters
/// of the system, useful for tests. The defaults are pretty good
/// for a working database.
public class SimpleDBSettings {
    public boolean UNDO_ONLY_RECOVERY = true;
    public int BLOCK_SIZE = 4096;
    public int BUFFER_SIZE = 8;
    public String LOG_FILE = "log_file";
}
