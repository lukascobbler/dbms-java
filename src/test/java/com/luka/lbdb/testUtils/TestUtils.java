package com.luka.lbdb.testUtils;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/// Shared code across all tests.
public abstract class TestUtils {
    private static final Map<String, AtomicInteger> testIterationCounters = new ConcurrentHashMap<>();
    private static final AtomicLong counter = new AtomicLong(0);

    static Configuration jimfsTestConfig = System.getProperty("os.name").toLowerCase().contains("win")
            ? Configuration.windows()
            : Configuration.unix();
    private static final FileSystem memoryFileSystem = Jimfs.newFileSystem(jimfsTestConfig);

    public enum TmpDirType {
        IN_MEMORY,
        DISK
    }

    /// Default - in memory temp directory. Change type here to turn off disk mode
    /// for debugging (or any other) purposes.
    ///
    /// In memory tests use **around** 1GB/500 tests.
    public static Path setUpTempDirectory() throws IOException {
        return setUpTempDirectory(TmpDirType.IN_MEMORY);
    }

    /// Initialize temp directory based on type, either on disk (for easier debugging),
    /// or in memory for faster test execution.
    public static Path setUpTempDirectory(TmpDirType type) throws IOException {
        return switch (type) {
            case DISK -> setUpTempDiskDirectory();
            case IN_MEMORY -> setUpTempInMemoryDirectory();
        };
    }

    /// Gives a unique and semantically meaningful directory name for each test run.
    ///
    /// @return The full directory path of the temporary directory.
    private static Path setUpTempDiskDirectory() {
        String baseFolderName = StackWalker.getInstance()
                .walk(frames -> frames
                        .skip(3)
                        .findFirst()
                        .map(frame -> {
                            String fullClass = frame.getClassName();
                            String simpleClass = fullClass.substring(fullClass.lastIndexOf('.') + 1);
                            return simpleClass + "." + frame.getMethodName();
                        })
                        .orElse("anonymous_test"));

        AtomicInteger counter = testIterationCounters.computeIfAbsent(baseFolderName, k -> new AtomicInteger(0));

        int iteration = counter.incrementAndGet();
        String uniqueFolderName = baseFolderName + "_" + iteration;

        Path path = Paths.get(System.getProperty("java.io.tmpdir"), "lbdb", uniqueFolderName);

        return path.toAbsolutePath();
    }

    /// Gives a unique directory name for each test run, in an in-memory file system.
    ///
    /// @return The full directory path of the temporary directory.
    private static Path setUpTempInMemoryDirectory() throws IOException {
        String uniqueFolderName = "run_" + counter.incrementAndGet();
        Path root = memoryFileSystem.getRootDirectories().iterator().next();
        Path path = root.resolve("lbdb").resolve(uniqueFolderName);
        Files.createDirectories(path);
        return path.toAbsolutePath();
    }

    /// @return The private field's value using Java reflection.
    public static Object getPrivateField(Object instance, String fieldName) throws Exception {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(instance);
    }

    /// @return Whether a file exists.
    public static boolean fileExists(Path dbDirectory, String fileName) {
        Path filePath = dbDirectory.resolve(fileName);
        return Files.isRegularFile(filePath);
    }
}
