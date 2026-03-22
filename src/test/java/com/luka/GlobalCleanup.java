package com.luka;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;

/// Runs before all tests and cleans up the whole test directory.
/// Best choice for maximum separation of environments, allows
/// concurrent tests (only really useful if disk tests are toggled on).
public class GlobalCleanup implements BeforeAllCallback {
    private static final AtomicBoolean started = new AtomicBoolean(false);

    @Override
    public void beforeAll(ExtensionContext context) throws IOException {
        if (started.compareAndSet(false, true)) {
            System.out.println("GLOBAL CLEANUP");
            Path root = Paths.get(System.getProperty("java.io.tmpdir"), "simpledb");
            if (Files.exists(root)) {
                try (var stream = Files.walk(root)) {
                    stream.sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(file -> {
                                if (!file.delete()) {
                                    System.out.println("FILE DELETION FAILED");
                                }
                            });
                }
            }
        }
    }
}