package com.luka.testUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

/// Shared code across all tests.
public abstract class TestUtils {
    /// Removes all files in the directory and prepares it for the same test to be run again.
    ///
    /// @return The full directory path of the temporary directory as a string.
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static String setUpTempDirectory(String testDirectoryName) throws IOException {
        Path path = Paths.get(System.getProperty("java.io.tmpdir"), testDirectoryName);

        if (Files.exists(path)) {
            try (var stream = Files.walk(path)) {
                stream.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }

        return path.toAbsolutePath().toString();
    }

    /// @return The private field's value using Java reflection.
    public static Object getPrivateField(Object instance, String fieldName) throws Exception {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(instance);
    }
}
