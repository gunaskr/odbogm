/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.odbogm;

import net.odbogm.exceptions.DuplicateLink;
import net.odbogm.cache.ClassCache;
import net.odbogm.cache.ClassDef;
import net.odbogm.exceptions.CollectionNotSupported;
import net.odbogm.utils.ReflexionUtils;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import java.util.HashMap;
import java.util.List;
import net.odbogm.proxy.ILazyCollectionCalls;
import net.odbogm.proxy.ILazyMapCalls;
import net.odbogm.proxy.IObjectProxy;
import net.odbogm.proxy.ObjectProxyFactory;

/**
 *
 * @author SShadow
 */
public class ObjectMapper {

    private final static Logger LOGGER = Logger.getLogger(ObjectMapper.class.getName());
    static {
        LOGGER.setLevel(Level.INFO);
    }
//    private static int newObjectCounter = 0;

    private SessionManager sessionManager;
    private ClassCache classCache;

    public ObjectMapper(SessionManager sm) {
//        LOGGER.setLevel(Level.INFO);

        // inicializar le caché de clases
        classCache = new ClassCache();
        this.sessionManager = sm;
    }

    /**
     * Devuelve la definición de la clase para el objeto pasado por parámetro
     *
     * @param o
     * @return
     */
    public ClassDef getClassDef(Object o) {
        if (o instanceof IObjectProxy) {
            return classCache.get(((IObjectProxy) o).___getBaseClass());
        } else {
            return classCache.get(o.getClass());
        }
    }
    
    
    /**
     * Devuelve un mapeo rápido del Objeto. No procesa los link o linklist. Simplemente devuelve todos los atributos del objeto en un map
     *
     * @param o
     * @return
     */
    public Map<String, Object> simpleMap(Object o) {
        HashMap<String, Object> data = new HashMap<>();
        ClassDef classmap = classCache.get(o.getClass());
        simpleFastMap(o, classmap, data);
        return data;
    }

    public void simpleFastMap(Object o, ClassDef classmap, HashMap<String, Object> data) {
        // procesar todos los campos
        classmap.fields.entrySet().stream().forEach((entry) -> {
            try {
                String field = entry.getKey();
                Class<?> c = entry.getValue();

                Field f = ReflexionUtils.findField(o.getClass(), field);
                boolean acc = f.isAccessible();
                f.setAccessible(true);

                // determinar si no es nulo
                if (f.get(o) != null) {
                    data.put(f.getName(), f.get(o));
                }
                f.setAccessible(acc);

            } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException ex) {
                Logger.getLogger(ObjectMapper.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
    }
    //============================================================================================

    /**
     * Devuelve un Map con todos los K,V de cada campo del objeto.
     *
     * @param o
     * @return
     */
    public ObjectStruct objectStruct(Object o) {
        ObjectStruct oStruct = new ObjectStruct();

        // buscar la definición de la clase en el caché
        ClassDef classmap;
        if (o instanceof IObjectProxy) {
            classmap = classCache.get(o.getClass().getSuperclass());
        } else {
            classmap = classCache.get(o.getClass());
        }

//        this.map(o, o.getDBClass(),mappedObject);      
        this.fastmap(o, classmap, oStruct);
        return oStruct;
    }

    /**
     * Realiza un mapeo a partir de las definiciones existentes en el caché
     *
     * @param o
     * @param oStruct
     */
    private void fastmap(Object o, ClassDef classmap, ObjectStruct oStruct) {

        // procesar todos los campos
        classmap.fields.entrySet().stream().forEach((entry) -> {
            try {
                String field = entry.getKey();
                Class<?> c = entry.getValue();

                Field f = ReflexionUtils.findField(o.getClass(), field);
                boolean acc = f.isAccessible();
                f.setAccessible(true);
                // determinar si no es nulo
                if (f.get(o) != null) {
                    oStruct.fields.put(f.getName(), f.get(o));
                }
                f.setAccessible(acc);

            } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException ex) {
                Logger.getLogger(ObjectMapper.class.getName()).log(Level.SEVERE, null, ex);
            }
        });

        // procesar todos los Enums
        classmap.enumFields.entrySet().stream().forEach((entry) -> {
            try {
                String field = entry.getKey();
                Class<?> c = entry.getValue();

                Field f = ReflexionUtils.findField(o.getClass(), field);
                boolean acc = f.isAccessible();
                f.setAccessible(true);
                // determinar si no es nulo
                if (f.get(o) != null) {
                    oStruct.fields.put(f.getName(), "" + f.get(o));
                }
                f.setAccessible(acc);

            } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException ex) {
                Logger.getLogger(ObjectMapper.class.getName()).log(Level.SEVERE, null, ex);
            }
        });

        // procesar todos los links
        classmap.links.entrySet().stream().forEach((entry) -> {
            try {
                String field = entry.getKey();
                Class<?> c = entry.getValue();

                Field f = ReflexionUtils.findField(o.getClass(), field);
                boolean acc = f.isAccessible();
                f.setAccessible(true);
                // determinar si no es nulo
                if (f.get(o) != null) {
                    oStruct.links.put(f.getName(), f.get(o));
                }
                f.setAccessible(acc);

            } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException ex) {
                Logger.getLogger(ObjectMapper.class.getName()).log(Level.SEVERE, null, ex);
            }
        });

        // procesar todos los linksList
        classmap.linkLists.entrySet().stream().forEach((entry) -> {
            try {
                String field = entry.getKey();
                Class<?> c = entry.getValue();

                Field f = ReflexionUtils.findField(o.getClass(), field);
                boolean acc = f.isAccessible();
                f.setAccessible(true);
                // determinar si no es nulo
                if (f.get(o) != null) {
                    oStruct.linkLists.put(f.getName(), f.get(o));
                }
                f.setAccessible(acc);

            } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException ex) {
                Logger.getLogger(ObjectMapper.class.getName()).log(Level.SEVERE, null, ex);
            }
        });

    }

    /**
     * Crea y llena un objeto con los valores correspondintes obtenidos del Vertice asignado.
     *
     * @param <T>
     * @param c
     * @param v
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws NoSuchFieldException
     */
    public <T> T hydrate(Class<T> c, OrientVertex v) throws InstantiationException, IllegalAccessException, NoSuchFieldException, CollectionNotSupported {
//        T o = c.newInstance();

        // crear un proxy sobre el objeto y devolverlo
        T oproxied = ObjectProxyFactory.create(c, v, sessionManager);

        LOGGER.log(Level.FINER, "**************************************************");
        LOGGER.log(Level.FINER, "Hydratando: {0}", c.getName());
        LOGGER.log(Level.FINER, "**************************************************");
        // recuperar la definición de la clase desde el caché
        ClassDef classdef = classCache.get(c);
        Map<String, Class<?>> fieldmap = classdef.fields;

        Field f;
        for (String prop : v.getPropertyKeys()) {
            Object value = v.getProperty(prop);
            LOGGER.log(Level.FINER, "Buscando campo {0} ....", new String[]{prop});
            // obtener la clase a la que pertenece el campo
            Class<?> fc = fieldmap.get(prop);

            f = ReflexionUtils.findField(c, prop);

            boolean acc = f.isAccessible();
            f.setAccessible(true);
            if (f.getType().isEnum()) {
                LOGGER.log(Level.FINER, "Enum field: " + f.getName() + " type: " + f.getType() + "  value: " + value + "   Enum val: " + Enum.valueOf(f.getType().asSubclass(Enum.class), value.toString()));
                f.set(oproxied, Enum.valueOf(f.getType().asSubclass(Enum.class), value.toString()));
            } else {
                f.set(oproxied, value);
            }
            LOGGER.log(Level.FINER, "hidratado campo: " + prop + "=" + value);
            f.setAccessible(acc);
        }
        // insertar el objeto en el getTransactionCache
        this.sessionManager.getTransactionCache.put(v.getId().toString(), oproxied);
        
        LOGGER.log(Level.FINER, "Procesando los Links......... ");

        // hidratar los atributos @links
        // procesar todos los links
        for (Map.Entry<String, Class<?>> entry : classdef.links.entrySet()) {
//        classdef.links.entrySet().stream().forEach((entry) -> {
            try {
                String field = entry.getKey();
                Class<?> fc = entry.getValue();
                String graphRelationName = c.getSimpleName() + "_" + field;
                LOGGER.log(Level.FINER, "Field: {0}   RelationName: {1}", new String[]{field, graphRelationName});

                Field fLink = ReflexionUtils.findField(c, field);
                boolean acc = fLink.isAccessible();
                fLink.setAccessible(true);

                // recuperar de la base el vértice correspondiente
                boolean duplicatedLinkGuard = false;
                for (Vertex vertice : v.getVertices(Direction.OUT, graphRelationName)) {
                    LOGGER.log(Level.FINER, "hydrate innerO: " + vertice.getId());

                    if (!duplicatedLinkGuard) {
//                        Object innerO = this.hydrate(fc, vertice);
                        /* FIXME: esto genera una dependencia cruzada. Habría que revisar
                           como solucionarlo. Esta llamada se hace para que quede el objeto
                           mapeado 
                         */
                        Object innerO = this.sessionManager.get(fc, vertice.getId().toString());
                        LOGGER.log(Level.FINER, "Inner object " + field + ": " + (innerO == null ? "NULL" : "" + innerO.toString()) + "  FC: " + fc.getSimpleName() + "   innerO.class: " + innerO.getClass().getSimpleName());
                        fLink.set(oproxied, fc.cast(innerO));
                        duplicatedLinkGuard = true;
                    } else if (false) {
                        throw new DuplicateLink();
                    }
                }
                fLink.setAccessible(acc);

            } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException ex) {
                Logger.getLogger(ObjectMapper.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        // hidratar las colecciones
        // procesar todos los linkslist
        LOGGER.log(Level.FINER, "preparando las colecciones...");
        for (Map.Entry<String, Class<?>> entry : classdef.linkLists.entrySet()) {

            try {
                // FIXME: se debería considerar agregar una annotation EAGER!
                String field = entry.getKey();
                Class<?> fc = entry.getValue();
                LOGGER.log(Level.FINER, "Field: {0}   Class: {1}", new String[]{field, fc.getName()});
                Field fLink = ReflexionUtils.findField(c, field);
                String graphRelationName = c.getSimpleName() + "_" + field;
                boolean acc = fLink.isAccessible();
                fLink.setAccessible(true);

                if (v.countEdges(Direction.OUT, graphRelationName) > 0) {
                    this.colecctionToLazy(oproxied, field, fc, v);
                }

                fLink.setAccessible(acc);

            } catch (NoSuchFieldException ex) {
                Logger.getLogger(ObjectMapper.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IllegalArgumentException ex) {
                Logger.getLogger(ObjectMapper.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

//        LOGGER.log(Level.FINER, "Objeto hydratado: " + oproxied.toString());
        LOGGER.log(Level.FINER, "******************* FIN HYDRATE *******************");
        return oproxied;
    }

    public void colecctionToLazy(Object o, String field, OrientVertex v) {
        ClassDef classdef;
        if (o instanceof IObjectProxy) {
            classdef = classCache.get(o.getClass().getSuperclass());
        } else {
            classdef = classCache.get(o.getClass());
        }

        Class<?> fc = classdef.linkLists.get(field);
        colecctionToLazy(o, field, fc, v);
    }

    /**
     * Convierte una colección común en una Lazy para futuras operaciones.
     *
     *
     * @param o objeto base sobre el que se trabaja
     * @param field campo a modificar
     * @param fc clase original del campo
     * @param v vértice con el cual se conecta.
     *
     */
    public void colecctionToLazy(Object o, String field, Class<?> fc, OrientVertex v) {
        try {
            Class<?> c;
            if (o instanceof IObjectProxy) {
                c = o.getClass().getSuperclass();
            } else {
                c = o.getClass();
            }
            
            Field fLink = ReflexionUtils.findField(c, field);
            String graphRelationName = c.getSimpleName() + "_" + field;
            boolean acc = fLink.isAccessible();
            fLink.setAccessible(true);

            Class<?> lazyClass = Primitives.LAZY_COLLECTION.get(fc);
            LOGGER.log(Level.FINER, "lazyClass: "+lazyClass.getName());
            Object col = lazyClass.newInstance();
            // dependiendo de si la clase hereda de Map o List, inicalizar
            if (col instanceof List) {
                ParameterizedType listType = (ParameterizedType) fLink.getGenericType();
                Class<?> listClass = (Class<?>) listType.getActualTypeArguments()[0];
                // inicializar la colección
                ((ILazyCollectionCalls) col).init(sessionManager, v, graphRelationName, listClass);

//                LOGGER.log(Level.FINER, "col: "+col.getDBClass());
            } else if (col instanceof Map) {
                ParameterizedType listType = (ParameterizedType) fLink.getGenericType();
                Class<?> keyClass = (Class<?>) listType.getActualTypeArguments()[0];
                Class<?> valClass = (Class<?>) listType.getActualTypeArguments()[1];
                // inicializar la colección
                ((ILazyMapCalls) col).init(sessionManager, v, graphRelationName, keyClass, valClass);
            } else {
                throw new CollectionNotSupported();
            }

            fLink.set(o, col);
            fLink.setAccessible(acc);

        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException | InstantiationException ex) {
            Logger.getLogger(ObjectMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Hidrata un objeto a partir de los atributos guardados en un Edge
     *
     * @param <T> clase del objeto a devolver
     * @param c : clase del objeto a devolver
     * @param e : Edge desde el que recuperar los datos
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws NoSuchFieldException
     */
    public <T> T hydrate(Class<T> c, OrientEdge e) throws InstantiationException, IllegalAccessException, NoSuchFieldException {
        T oproxied = ObjectProxyFactory.create(c, e, sessionManager);
        // recuperar la definición de la clase desde el caché
        ClassDef classdef = classCache.get(c);
        Map<String, Class<?>> fieldmap = classdef.fields;

        Field f;
        for (String prop : e.getPropertyKeys()) {
            Object value = e.getProperty(prop);

            // obtener la clase a la que pertenece el campo
            Class<?> fc = fieldmap.get(prop);
//            LOGGER.log(Level.FINER, "hidratando campo: "+prop);

            f = ReflexionUtils.findField(c, prop);

            boolean acc = f.isAccessible();
            f.setAccessible(true);
            f.set(oproxied, value);
            f.setAccessible(acc);
        }

        return oproxied;
    }

    public static void setFieldValue(Object o, String field, Object value) {
        try {
            Field f = ReflexionUtils.findField(o.getClass(), field);
            boolean acc = f.isAccessible();
            f.setAccessible(true);
            // determinar si no es nulo
            f.set(o, value);
            f.setAccessible(acc);
        } catch (NoSuchFieldException | IllegalArgumentException | IllegalAccessException ex) {
            Logger.getLogger(ObjectMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
