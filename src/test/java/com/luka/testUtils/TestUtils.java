package com.luka.testUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/// Shared code across all tests.
public abstract class TestUtils {
    private static final Map<String, AtomicInteger> testIterationCounters = new ConcurrentHashMap<>();

    /// Gives a unique directory name for each test run.
    ///
    /// @return The full directory path of the temporary directory as a string.
    public static String setUpTempDirectory() throws IOException {
        String baseFolderName = StackWalker.getInstance()
                .walk(frames -> frames
                        .skip(1)
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

        Path path = Paths.get(System.getProperty("java.io.tmpdir"), "simpledb", uniqueFolderName);

        return path.toAbsolutePath().toString();
    }

    /// @return The private field's value using Java reflection.
    public static Object getPrivateField(Object instance, String fieldName) throws Exception {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(instance);
    }

    /// @return Whether a file exists.
    public static boolean fileExists(File dbDirectory, String fileName) {
        File file = new File(dbDirectory, fileName);
        return file.exists() && file.isFile();
    }
}
