package gr.ntua.ece.cslab.e2datascheduler.util.asm.map;


/**
 * Classloader that will be used to load the produced class after the ASM transformation.
 */
public class AsmClassLoader extends ClassLoader {
    public AsmClassLoader() {
        super();
    }

    public Class defineClass(String name, byte[] b) {
        return defineClass(name, b, 0, b.length);
    }
}
