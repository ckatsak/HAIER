package gr.ntua.ece.cslab.e2datascheduler.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Utility class to dynamically load at runtime unknown user-defined classes as found in Flink users' JAR files.
 */
public final class HaierUDFLoader {

    private static final Logger logger = Logger.getLogger(HaierUDFLoader.class.getCanonicalName());

    private static HaierUDFLoader haierUDFLoader = null;

    private HaierUDFLoader() {
        super();
    }

    public static HaierUDFLoader getInstance() {
        if (null == HaierUDFLoader.haierUDFLoader) {
            HaierUDFLoader.haierUDFLoader = new HaierUDFLoader();
            logger.finest("Singleton " + HaierUDFLoader.class.getSimpleName() + " just instantiated");
        }
        return HaierUDFLoader.haierUDFLoader;
    }

    // --------------------------------------------------------------------------------------------


    public static List<Class<?>> scanJarFileForMapFunctionClasses(final File file) throws IOException {
        return scanJarFileForAssignableClasses(file, MapFunction.class);
    }

    public static List<Class<?>> scanJarFileForReduceFunctionClasses(final File file) throws IOException {
        return scanJarFileForAssignableClasses(file, ReduceFunction.class);
    }

    private static List<Class<?>> scanJarFileForAssignableClasses(final File file, final Class<?> assignableTo)
            throws IOException {
        final List<Class<?>> ret = new ArrayList<>();

        for (String classFile : scanJarFileForClasses(file)) {
            final Class<?> clazz;
            try {
                final URLClassLoader urlClassLoader = URLClassLoader.newInstance(
                        new URL[] { file.toURI().toURL() },
                        HaierUDFLoader.class.getClassLoader()
                );
                clazz = Class.forName(classFile, true, urlClassLoader);
                if (assignableTo.isAssignableFrom(clazz) && !clazz.equals(assignableTo)) {
                    ret.add(clazz);
                }
            } catch (final ClassNotFoundException e) {
                HaierUDFLoader.logger.log(Level.WARNING, e.getMessage(), e);
            }
        }

        return ret;
    }


    /**
     * Scans a JAR file for .class files and returns a {@link List} containing the full name of found classes (in the
     * form: `packageName.className`).
     *
     * @param file JAR-file to be searched for `.class` files
     * @return Returns all found `.class` files with their full-name as a List of Strings
     * @throws IOException If an error occurred during processing of the JAR file
     * @throws IllegalArgumentException If the provided file is null, or it does not exist, or it is not a JAR file
     */
    public static List<String> scanJarFileForClasses(final File file)
            throws IOException, IllegalArgumentException {
        if (null == file || !file.exists())
            throw new IllegalArgumentException("Invalid JAR file to scan provided");
        if (!file.getName().endsWith(".jar")) {
            throw new IllegalArgumentException("No JAR file provided ('" + file.getName() + "')");
        }

        final JarFile jarFile = new JarFile(file);
        final Enumeration<JarEntry> jarEntries = jarFile.entries();
        final List<String> containedClasses = new ArrayList<>();
        while (jarEntries.hasMoreElements()) {
            final JarEntry jarEntry = jarEntries.nextElement();
            if (jarEntry.getName().endsWith(".class")) {
                String jarEntryName = jarEntry.getName();
                jarEntryName = jarEntryName.substring(0, jarEntryName.lastIndexOf(".class"));
                if (jarEntryName.contains("/")) {
                    jarEntryName = jarEntryName.replaceAll("/", ".");
                }
                if (jarEntryName.contains("\\")) {
//                    jarEntryName = jarEntryName.replaceAll("\\", ".");
                    throw new IllegalArgumentException(
                            "FIXME(ckatsak): Weird name containing backslashes: '" + jarEntryName + "'");
                }
                containedClasses.add(jarEntryName);
            }
        }
        return containedClasses;
    }

}
