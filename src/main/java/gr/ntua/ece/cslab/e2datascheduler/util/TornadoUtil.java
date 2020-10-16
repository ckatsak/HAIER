package gr.ntua.ece.cslab.e2datascheduler.util;

import gr.ntua.ece.cslab.e2datascheduler.util.asm.ExamineUDF;
import gr.ntua.ece.cslab.e2datascheduler.util.asm.TransformUDF;
import gr.ntua.ece.cslab.e2datascheduler.util.asm.map.AsmClassLoader;
import gr.ntua.ece.cslab.e2datascheduler.util.asm.map.MiddleMap;
import gr.ntua.ece.cslab.e2datascheduler.util.asm.map.MiddleMap2;
import gr.ntua.ece.cslab.e2datascheduler.util.asm.map.MiddleMap3;
import gr.ntua.ece.cslab.e2datascheduler.util.asm.map.TornadoMap;
import gr.ntua.ece.cslab.e2datascheduler.util.asm.map.TornadoMap2;
import gr.ntua.ece.cslab.e2datascheduler.util.asm.map.TornadoMap3;
import gr.ntua.ece.cslab.e2datascheduler.util.asm.reduce.MiddleReduce;
import gr.ntua.ece.cslab.e2datascheduler.util.asm.reduce.TornadoReduce;
import gr.ntua.ece.cslab.e2datascheduler.util.tornadoflink.AccelerationData;
import gr.ntua.ece.cslab.e2datascheduler.util.tornadoflink.FlinkCompilerInfo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;

import java.util.ArrayList;

import static gr.ntua.ece.cslab.e2datascheduler.util.asm.ExamineUDF.setTypeVariablesMap;
import static gr.ntua.ece.cslab.e2datascheduler.util.asm.ExamineUDF.setTypeVariablesReduce;


/**
 * TODO(ckatsak): Documentation
 */
public class TornadoUtil {

    public static TornadoMap transformUDF(String name) {
        try {
            TransformUDF.mapUserClassName = name.replace("class ", "").replace(".", "/");
            TransformUDF.tornadoMapName = "org/apache/flink/runtime/asm/map/MapASMSkeleton";
            ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
            ClassReader flinkClassReader = new ClassReader(TransformUDF.mapUserClassName);
            flinkClassReader.accept(flinkVisit, 0);

            setTypeVariablesMap();

            // ASM work for map
            // patch udf into the appropriate MapASMSkeleton
            String desc = "L" + TransformUDF.mapUserClassName + ";";
            ClassReader readerMap = new ClassReader("org.apache.flink.runtime.asm.map.MapASMSkeleton");
            ClassWriter writerMap = new ClassWriter(readerMap, ClassWriter.COMPUTE_MAXS);
            writerMap.visitField(Opcodes.ACC_PUBLIC, "udf", desc, null, null).visitEnd();
            //TraceClassVisitor printer = new TraceClassVisitor(writerMap, new PrintWriter(System.out));
            TransformUDF.MapClassAdapter adapterMap = new TransformUDF.MapClassAdapter(writerMap);
            readerMap.accept(adapterMap, ClassReader.EXPAND_FRAMES);
            // tornado
            byte[] b = writerMap.toByteArray();
            AsmClassLoader loader = new AsmClassLoader();
            Class<?> clazzMap = loader.defineClass("org.apache.flink.runtime.asm.map.MapASMSkeleton", b);
            MiddleMap md = (MiddleMap) clazzMap.newInstance();
            TornadoMap msk = new TornadoMap(md);
            return msk;
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }

    public static TornadoMap2 transformUDF2(String name) {
        try {
            TransformUDF.mapUserClassName = name.replace("class ", "").replace(".", "/");
            TransformUDF.tornadoMapName = "org/apache/flink/runtime/asm/map/MapASMSkeleton2";
            ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
            ClassReader flinkClassReader = new ClassReader(TransformUDF.mapUserClassName);
            flinkClassReader.accept(flinkVisit, 0);

            setTypeVariablesMap();

            // ASM work for map
            // patch udf into the appropriate MapASMSkeleton
            String desc = "L" + TransformUDF.mapUserClassName + ";";
            ClassReader readerMap = new ClassReader("org.apache.flink.runtime.asm.map.MapASMSkeleton2");
            ClassWriter writerMap = new ClassWriter(readerMap, ClassWriter.COMPUTE_MAXS);
            writerMap.visitField(Opcodes.ACC_PUBLIC, "udf", desc, null, null).visitEnd();
            //TraceClassVisitor printer = new TraceClassVisitor(writerMap, new PrintWriter(System.out));
            TransformUDF.MapClassAdapter adapterMap = new TransformUDF.MapClassAdapter(writerMap);
            readerMap.accept(adapterMap, ClassReader.EXPAND_FRAMES);
            // tornado
            byte[] b = writerMap.toByteArray();
            AsmClassLoader loader = new AsmClassLoader();
            Class<?> clazzMap = loader.defineClass("org.apache.flink.runtime.asm.map.MapASMSkeleton2", b);
            MiddleMap2 md = (MiddleMap2) clazzMap.newInstance();
            TornadoMap2 msk = new TornadoMap2(md);
            return msk;
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }

    public static TornadoMap3 transformUDF3(String name) {
        try {
            TransformUDF.mapUserClassName = name.replace("class ", "").replace(".", "/");
            TransformUDF.tornadoMapName = "org/apache/flink/runtime/asm/map/MapASMSkeleton3";
            ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
            ClassReader flinkClassReader = new ClassReader(TransformUDF.mapUserClassName);
            flinkClassReader.accept(flinkVisit, 0);

            setTypeVariablesMap();

            // ASM work for map
            // patch udf into the appropriate MapASMSkeleton
            String desc = "L" + TransformUDF.mapUserClassName + ";";
            ClassReader readerMap = new ClassReader("org.apache.flink.runtime.asm.map.MapASMSkeleton3");
            ClassWriter writerMap = new ClassWriter(readerMap, ClassWriter.COMPUTE_MAXS);
            writerMap.visitField(Opcodes.ACC_PUBLIC, "udf", desc, null, null).visitEnd();
            //TraceClassVisitor printer = new TraceClassVisitor(writerMap, new PrintWriter(System.out));
            TransformUDF.MapClassAdapter adapterMap = new TransformUDF.MapClassAdapter(writerMap);
            readerMap.accept(adapterMap, ClassReader.EXPAND_FRAMES);
            // tornado
            byte[] b = writerMap.toByteArray();
            AsmClassLoader loader = new AsmClassLoader();
            Class<?> clazzMap = loader.defineClass("org.apache.flink.runtime.asm.map.MapASMSkeleton3", b);
            MiddleMap3 md = (MiddleMap3) clazzMap.newInstance();
            TornadoMap3 msk = new TornadoMap3(md);
            return msk;
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }

    public static TornadoReduce transformReduceUDF(String name) {
        try {
            TransformUDF.redUserClassName = name.replace("class ", "").replace(".", "/");
            // examine udf
            ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
            ClassReader flinkClassReader = new ClassReader(TransformUDF.redUserClassName);
            flinkClassReader.accept(flinkVisit, 0);

            setTypeVariablesReduce();

            // ASM work for reduce
            ClassReader readerRed = new ClassReader("org.apache.flink.runtime.asm.reduce.ReduceASMSkeleton");
            ClassWriter writerRed = new ClassWriter(readerRed, ClassWriter.COMPUTE_MAXS);
            //TraceClassVisitor printerRed = new TraceClassVisitor(writerRed, new PrintWriter(System.out));
            // to remove debugging info, just replace the printer in class adapter call with
            // the writer
            TransformUDF.ReduceClassAdapter adapterRed = new TransformUDF.ReduceClassAdapter(writerRed);
            readerRed.accept(adapterRed, ClassReader.EXPAND_FRAMES);
            byte[] b = writerRed.toByteArray();
            AsmClassLoader loader = new AsmClassLoader();
            Class<?> clazzRed = loader.defineClass("org.apache.flink.runtime.asm.reduce.ReduceASMSkeleton", b);
            MiddleReduce mdr = (MiddleReduce) clazzRed.newInstance();
            TornadoReduce rsk = new TornadoReduce(mdr);
            return rsk;
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }

    public static int examineTypeInfoForFlinkUDFs(
            TypeInformation inputType,
            TypeInformation returnType,
            FlinkCompilerInfo flinkCompilerInfo,
            AccelerationData acdata) {
        if (inputType.getClass() == TupleTypeInfo.class) {
            TupleTypeInfo tinfo = (TupleTypeInfo) inputType;
            TypeInformation[] tuparray = tinfo.getTypeArray();

            ArrayList<Class> tupleFieldKind = new ArrayList<>();
            ArrayList<String> fieldTypes = new ArrayList<>();
            ArrayList<Integer> fieldSizes = new ArrayList<>();

            int tupleSize = tuparray.length;
            boolean hasTuples = true;
            boolean arrayfield = false;
            int arrayFieldPos = -1;
            int arrayFieldSize = 0;

            for (int i = 0; i < tuparray.length; i++) {
                Class typeClass = tuparray[i].getTypeClass();
                if (typeClass.toString().contains("Double")) {
                    tupleFieldKind.add(double.class);
                    fieldSizes.add(8);
                    fieldTypes.add("double");
                } else if (tuparray[i] instanceof TupleTypeInfo) {
                    TupleTypeInfo nestedTuple = (TupleTypeInfo) tuparray[i];
                    TypeInformation[] nestedtuparray = nestedTuple.getTypeArray();
                    tupleSize += (nestedtuparray.length - 1);
                    flinkCompilerInfo.setNestedTuples(true);
                    flinkCompilerInfo.setNestedTupleField(i);
                    flinkCompilerInfo.setSizeOfNestedTuple(nestedtuparray.length);
                    for (int j = 0; j < nestedtuparray.length; j++) {
                        Class nestedTypeClass = nestedtuparray[i].getTypeClass();
                        if (nestedTypeClass.toString().contains("Double")) {
                            tupleFieldKind.add(double.class);
                            fieldSizes.add(8);
                            fieldTypes.add("double");
                        } else if (nestedtuparray[i] instanceof TupleTypeInfo) {
                            System.out.println("We can currently support only 2 factor nesting for Tuples!");
                            return -1;
                        } else if (nestedTypeClass.toString().contains("Float")) {
                            tupleFieldKind.add(float.class);
                            fieldSizes.add(4);
                            fieldTypes.add("float");
                        } else if (nestedTypeClass.toString().contains("Long")) {
                            tupleFieldKind.add(long.class);
                            fieldSizes.add(8);
                            fieldTypes.add("long");
                        } else if (nestedTypeClass.toString().contains("Integer")) {
                            tupleFieldKind.add(int.class);
                            fieldSizes.add(4);
                            fieldTypes.add("int");
                        } else if (nestedTypeClass.toString().contains("[I")) {
                            tupleFieldKind.add(int.class);
                            fieldSizes.add(4);
                            fieldTypes.add("int");
                            arrayfield = true;
                            // TODO: CHECK THIS
                            arrayFieldPos = i + j;
                            arrayFieldSize = 4;
                            flinkCompilerInfo.setArrayField(true);
                            flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
                            flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
                        } else if (nestedTypeClass.toString().contains("[D")) {
                            tupleFieldKind.add(double.class);
                            fieldSizes.add(8);
                            fieldTypes.add("double");
                            arrayfield = true;
                            arrayFieldPos = i + j;
                            arrayFieldSize = 8;
                            flinkCompilerInfo.setArrayField(true);
                            flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
                            flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
                        } else if (nestedTypeClass.toString().contains("[F")) {
                            tupleFieldKind.add(float.class);
                            fieldSizes.add(4);
                            fieldTypes.add("float");
                            arrayfield = true;
                            arrayFieldPos = i + j;
                            arrayFieldSize = 4;
                            flinkCompilerInfo.setArrayField(true);
                            flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
                            flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
                        } else if (nestedTypeClass.toString().contains("[J")) {
                            tupleFieldKind.add(long.class);
                            fieldSizes.add(8);
                            fieldTypes.add("long");
                            arrayfield = true;
                            arrayFieldPos = i + j;
                            arrayFieldSize = 8;
                            flinkCompilerInfo.setArrayField(true);
                            flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
                            flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
                        }
                    }
                } else if (typeClass.toString().contains("Float")) {
                    tupleFieldKind.add(float.class);
                    fieldSizes.add(4);
                    fieldTypes.add("float");
                } else if (typeClass.toString().contains("Long")) {
                    tupleFieldKind.add(long.class);
                    fieldSizes.add(8);
                    fieldTypes.add("long");
                } else if (typeClass.toString().contains("Integer")) {
                    tupleFieldKind.add(int.class);
                    fieldSizes.add(4);
                    fieldTypes.add("int");
                } else if (typeClass.toString().contains("[I")) {
                    tupleFieldKind.add(int.class);
                    fieldSizes.add(4);
                    fieldTypes.add("int");
                    arrayfield = true;
                    arrayFieldPos = i;
                    arrayFieldSize = 4;
                    flinkCompilerInfo.setArrayField(true);
                    flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
                    flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
                } else if (typeClass.toString().contains("[D")) {
                    tupleFieldKind.add(double.class);
                    fieldSizes.add(8);
                    fieldTypes.add("double");
                    arrayfield = true;
                    arrayFieldPos = i;
                    arrayFieldSize = 8;
                    flinkCompilerInfo.setArrayField(true);
                    flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
                    flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
                } else if (typeClass.toString().contains("[F")) {
                    tupleFieldKind.add(float.class);
                    fieldSizes.add(4);
                    fieldTypes.add("float");
                    arrayfield = true;
                    arrayFieldPos = i;
                    arrayFieldSize = 4;
                    flinkCompilerInfo.setArrayField(true);
                    flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
                    flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
                } else if (typeClass.toString().contains("[J")) {
                    tupleFieldKind.add(long.class);
                    fieldSizes.add(8);
                    fieldTypes.add("long");
                    arrayfield = true;
                    arrayFieldPos = i;
                    arrayFieldSize = 8;
                    flinkCompilerInfo.setArrayField(true);
                    flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
                    flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
                }

            }

            flinkCompilerInfo.setTupleSize(tupleSize);
            boolean differentTypes = false;

            for (int i = 0; i < tupleSize; i++) {
                for (int j = i + 1; j < tupleSize; j++) {
                    if (arrayfield) {
                        if (i == arrayFieldPos) {
                            if (!fieldSizes.get(j).equals(arrayFieldSize)) {
                                differentTypes = true;
                                break;
                            }
                        } else if (j == arrayFieldPos) {
                            if (!fieldSizes.get(i).equals(arrayFieldSize)) {
                                differentTypes = true;
                                break;
                            }
                        } else {
                            if (!(fieldSizes.get(i).equals(fieldSizes.get(j)))) {
                                differentTypes = true;
                            }
                        }
                    } else {
                        if (!(fieldSizes.get(i).equals(fieldSizes.get(j)))) {
                            differentTypes = true;
                        }
                    }
                }
            }

            flinkCompilerInfo.setHasTuples(hasTuples);
            flinkCompilerInfo.setTupleFieldKind(tupleFieldKind);
            flinkCompilerInfo.setFieldTypes(fieldTypes);
            flinkCompilerInfo.setFieldSizes(fieldSizes);
            flinkCompilerInfo.setDifferentTypes(differentTypes);
        }
        int returnSize = 0;
        if (returnType.getClass() == TupleTypeInfo.class) {
            int numOfFields = 0;
            TupleTypeInfo tinfo = (TupleTypeInfo) returnType;
            TypeInformation[] tuparray = tinfo.getTypeArray();
            flinkCompilerInfo.setReturnTuple(true);

            ArrayList<Class> returnFieldKind = new ArrayList<>();
            ArrayList<String> fieldTypesRet = new ArrayList<>();
            ArrayList<Integer> fieldSizesRet = new ArrayList<>();

            int retTupleSize = tuparray.length;
            ArrayList<Integer> tupleReturnSizes = new ArrayList<>();


            for (int i = 0; i < tuparray.length; i++) {
                Class typeClass = tuparray[i].getTypeClass();
                if (typeClass.toString().contains("Double")) {
                    returnFieldKind.add(double.class);
                    returnSize += 8;
                    numOfFields++;
                    fieldSizesRet.add(8);
                    tupleReturnSizes.add(8);
                    fieldTypesRet.add("double");
                } else if (tuparray[i] instanceof TupleTypeInfo) {
                    //flinkCompilerInfo.setReturnNestedTuple();
                    TupleTypeInfo nestedTuple = (TupleTypeInfo) tuparray[i];
                    TypeInformation[] nestedtuparray = nestedTuple.getTypeArray();
                    retTupleSize += (nestedtuparray.length - 1);
                    for (int j = 0; j < nestedtuparray.length; j++) {
                        Class nestedTypeClass = nestedtuparray[j].getTypeClass();
                        if (nestedTypeClass.toString().contains("Double")) {
                            returnFieldKind.add(double.class);
                            returnSize += 8;
                            numOfFields++;
                            fieldSizesRet.add(8);
                            fieldTypesRet.add("double");
                            tupleReturnSizes.add(8);
                        } else if (nestedtuparray[i] instanceof TupleTypeInfo) {
                            System.out.println("We can currently support only 2 factor nesting for Tuples!");
                            return -1;
                        } else if (nestedTypeClass.toString().contains("Float")) {
                            returnFieldKind.add(float.class);
                            returnSize += 4;
                            numOfFields++;
                            fieldSizesRet.add(4);
                            fieldTypesRet.add("float");
                            tupleReturnSizes.add(4);
                        } else if (nestedTypeClass.toString().contains("Long")) {
                            returnFieldKind.add(long.class);
                            returnSize += 8;
                            numOfFields++;
                            fieldSizesRet.add(8);
                            fieldTypesRet.add("long");
                            tupleReturnSizes.add(8);
                        } else if (nestedTypeClass.toString().contains("Integer")) {
                            returnFieldKind.add(int.class);
                            returnSize += 4;
                            numOfFields++;
                            fieldSizesRet.add(4);
                            fieldTypesRet.add("int");
                            tupleReturnSizes.add(4);

                        } else if (nestedTypeClass.toString().contains("[D")) {
                            returnFieldKind.add(double.class);
                            returnSize += 8*83;
                            numOfFields++;
                            fieldSizesRet.add(8);
                            fieldTypesRet.add("double");
                            tupleReturnSizes.add(8);
                            flinkCompilerInfo.setReturnArrayField(true);
                            flinkCompilerInfo.setReturnArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
                            //flinkCompilerInfo.setTupleArrayFieldNo(i + j);
                        }
                    }
                } else if (typeClass.toString().contains("Float")) {
                    returnFieldKind.add(float.class);
                    returnSize += 4;
                    numOfFields++;
                    fieldSizesRet.add(4);
                    fieldTypesRet.add("float");
                    tupleReturnSizes.add(4);
                } else if (typeClass.toString().contains("Long")) {
                    returnFieldKind.add(long.class);
                    returnSize += 8;
                    numOfFields++;
                    fieldSizesRet.add(8);
                    fieldTypesRet.add("long");
                    tupleReturnSizes.add(8);
                } else if (typeClass.toString().contains("Integer")) {
                    returnFieldKind.add(int.class);
                    returnSize += 4;
                    numOfFields++;
                    fieldSizesRet.add(4);
                    fieldTypesRet.add("int");
                    tupleReturnSizes.add(4);
                } else if (typeClass.toString().contains("[D")) {
                    returnFieldKind.add(double.class);
                    flinkCompilerInfo.setReturnArrayField(true);
                    flinkCompilerInfo.setReturnArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
                    flinkCompilerInfo.setArrayType("double");
                    returnSize += tupleArrayFieldTotalBytes;
                    numOfFields++;
                    fieldSizesRet.add(tupleArrayFieldTotalBytes);
                    fieldTypesRet.add("double");
                    tupleReturnSizes.add(8);
                    acdata.hasArrayField();
                }
            }

            flinkCompilerInfo.setReturnTupleSize(retTupleSize);
            boolean differentTypesRet = false;

            for (int i = 0; i < retTupleSize; i++) {
                for (int j = i + 1; j < retTupleSize; j++) {
                    if (!(fieldSizesRet.get(i).equals(fieldSizesRet.get(j)))) {
                        differentTypesRet = true;
                        returnSize = numOfFields * 8;
                    }
                }
            }

            flinkCompilerInfo.setReturnFieldKind(returnFieldKind);
            flinkCompilerInfo.setFieldTypesRet(fieldTypesRet);
            flinkCompilerInfo.setFieldSizesRet(fieldSizesRet);
            flinkCompilerInfo.setDifferentTypesRet(differentTypesRet);
            acdata.setDifferentReturnTupleFields(differentTypesRet);
            acdata.setReturnFieldSizes(tupleReturnSizes);
        }
        return returnSize;
    }


    //--------------------------------------------------------------------------------------------


    }
