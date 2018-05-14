/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.odbogm;

import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.odbogm.annotations.Audit;
import net.odbogm.annotations.CascadeDelete;
import net.odbogm.annotations.RemoveOrphan;
import net.odbogm.auditory.Auditor;
import net.odbogm.cache.ClassDef;
import net.odbogm.exceptions.ClassToVertexNotFound;
import net.odbogm.exceptions.CollectionNotSupported;
import net.odbogm.exceptions.IncorrectRIDField;
import net.odbogm.exceptions.NoOpenTx;
import net.odbogm.exceptions.NoUserLoggedIn;
import net.odbogm.exceptions.ReferentialIntegrityViolation;
import net.odbogm.exceptions.UnknownObject;
import net.odbogm.exceptions.UnknownRID;
import net.odbogm.exceptions.UnmanagedObject;
import net.odbogm.exceptions.VertexJavaClassNotFound;
import net.odbogm.proxy.IObjectProxy;
import net.odbogm.proxy.ObjectProxyFactory;
import net.odbogm.security.SObject;
import net.odbogm.utils.ReflectionUtils;
import net.odbogm.utils.ThreadHelper;

/**
 * Constituye un marco de control para los objetos recuperados.
 *
 * @author Marcelo D. Ré {@literal <marcelo.re@gmail.com>}
 */
public class Transaction implements IActions.IStore, IActions.IGet, IActions.IQuery {

    private final static Logger LOGGER = Logger.getLogger(Transaction.class.getName());
    private static final long serialVersionUID = 1L;

    static {
        if (LOGGER.getLevel() == null) {
            LOGGER.setLevel(LogginProperties.Transaction);
        }
    }

    // Es el único objectMapper para todo el SM. 
    private ObjectMapper objectMapper;

    // cache de los objetos recuperados de la base. Si se encuentra en el caché en un get, se recupera desde 
    // acá. En caso contrario se recupera desde la base.
    private ConcurrentHashMap<String, WeakReference<Object>> objectCache = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, Object> dirty = new ConcurrentHashMap<>();

    // utilizado para determinar si existen transacciones anidadas.
    private int nestedTransactionLevel = 0;

    // se utiliza para guardar los objetos recuperados durante un get a fin de evitar los loops
    private int getTransactionCount = 0;
    ConcurrentHashMap<String, Object> transactionCache = new ConcurrentHashMap<>();

    // Los RIDs temporales deben ser convertidos a los permanentes en el proceso de commit
    List<String> newrids = new ArrayList<>();

    // determina si se está en el proceso de commit.
    private boolean commiting = false;
    private ConcurrentHashMap<Object, Object> commitedObject = new ConcurrentHashMap<>();

    int newObjectCount = 0;

    // Auditor asociado a la transacción
    private Auditor auditor;

    SessionManager sm;
    OrientGraph orientdbTransact;

    /**
     * Devuelve una transacción ya inicializada contra la base de datos.
     *
     * @param sm
     */
    Transaction(SessionManager sm) {
        this.sm = sm;
        orientdbTransact = this.sm.getFactory().getTx();
        this.objectMapper = this.sm.getObjectMapper();
    }

    /**
     * Elimina cualquier objeto que esté marcado como dirty en esta transacción
     */
    public void clear() {
        this.dirty.clear();
    }

    /**
     * Marca un objecto como dirty para ser procesado en el commit
     *
     * @param o objeto de referencia.
     */
    public synchronized void setAsDirty(Object o) throws UnmanagedObject {
//        graphdb.getRawGraph().activateOnCurrentThread();
        if (o instanceof IObjectProxy) {
            String rid = ((IObjectProxy) o).___getVertex().getId().toString();
            LOGGER.log(Level.FINER, "Marcando como dirty: " + o.getClass().getSimpleName() + " - " + o.toString());
            LOGGER.log(Level.FINEST, ThreadHelper.getCurrentStackTrace());
            this.dirty.put(rid, o);
        } else {
            throw new UnmanagedObject();
        }
    }

    /**
     * Agrega un objeto al cache de la transacción
     *
     * @param rid record id
     * @param o objeto a referenciar
     */
    public synchronized void addToCache(String rid, WeakReference<Object> o) {
        this.objectCache.put(rid, o);
    }

    /**
     * Vuelve a cargar todos los objetos que han sido marcados como modificados con los datos desde las base. Los objetos marcados como Dirty forman
     * parte del siguiente commit
     */
    public synchronized void refreshDirtyObjects() {
        if (this.orientdbTransact == null) {
            throw new NoOpenTx();
        }
        for (Map.Entry<String, Object> e : dirty.entrySet()) {
            String rid = e.getKey();
            IObjectProxy o = (IObjectProxy) e.getValue();

            // actualizar todos los objetos a nivel de cache de la base sin proceder a 
            // bajarlos efectívamente.
            o.___reload();
        }
    }

    /**
     * Vuelve a cargar el objeto con los datos desde las base. Esta accion no se propaga sobre los objetos que lo componen.
     *
     * @param o objeto recuperado de la base a ser actualizado.
     */
    public synchronized void refreshObject(IObjectProxy o) {
        if (this.orientdbTransact == null) {
            throw new NoOpenTx();
        }
        o.___reload();
    }

    /**
     * Devuelve el objeto de comunicación con la base.
     *
     * @return retorna la referencia directa al driver del la base.
     */
    public OrientGraph getGraphdb() {
        return orientdbTransact;
    }

    /**
     * Inicia una transacción anidada
     */
    public synchronized void begin() {
        this.nestedTransactionLevel++;
    }

    /**
     * Finaliza la comunicación contra la base de datos.
     */
    public synchronized void close() {
        this.orientdbTransact.shutdown();
    }

    /**
     * Persistir la información pendiente en la transacción
     *
     * @throws NoOpenTx si no hay una trnasacción abierta.
     */
    public synchronized void commit() throws NoOpenTx, OConcurrentModificationException {
        if (this.nestedTransactionLevel == 0) {
            if (this.orientdbTransact == null) {
                throw new NoOpenTx();
            }
//        this.graphdb.getRawGraph().activateOnCurrentThread();

            // bajar todos los objetos a los vértices
            // this.commitObjectChanges();
            // cambiar el estado a comiteando
            this.commiting = true;
            LOGGER.log(Level.FINER, "Iniciando COMMIT ==================================");
            LOGGER.log(Level.FINER, "Objetos marcados como Dirty: " + dirty.size());
            for (Map.Entry<String, Object> e : dirty.entrySet()) {
                String rid = e.getKey();
                IObjectProxy o = (IObjectProxy) e.getValue();
                LOGGER.log(Level.FINER, "Commiting: " + rid + "   class: " + o.___getBaseClass() + " isValid: " + o.___isValid());
                // actualizar todos los objetos antes de bajarlos.
                o.___commit();
            }
            LOGGER.log(Level.FINER, "Fin persistencia. <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
            // comitear los vértices
            LOGGER.log(Level.FINER, "llamando al commit de la base");
            this.orientdbTransact.commit();
            LOGGER.log(Level.FINER, "finalizado.");

            // si se está en modalidad audit, grabar los logs
            if (this.isAuditing()) {
                LOGGER.log(Level.FINER, "grabando auditoría...");
                this.getAuditor().commit();
                this.orientdbTransact.commit();
                LOGGER.log(Level.FINER, "finalizado.");
            }
            // vaciar el caché de elementos modificados.
            this.dirty.clear();
            this.commiting = false;
            this.commitedObject.clear();

//        // refrescar las referencias del caché
//        String newRid;
//        for (Iterator<String> iterator = newrids.iterator(); iterator.hasNext();) {
//            String tempRid = iterator.next();
//            if (objectCache.get(tempRid).get()!=null) {
//                // reemplazar el rid con el que le asignó la base luego de persistir el objeto
//                Object o = objectCache.get(tempRid).get();
//                objectCache.remove(tempRid);
//                newRid = this.getRID(o);
//                objectCache.put(newRid, new WeakReference<>(o));
//            }
//        }
            // se opta por eliminar el caché de objetos recuperados de la base en un commit o rollback
            // por lo que futuros pedidos a la base fuera de la transacción devolverá una nueva instancia
            // del objeto.
            this.objectCache.clear();
            newrids.clear();
        } else {
            this.nestedTransactionLevel--;
        }
        LOGGER.log(Level.FINER, "FIN DE COMMIT! ----------------------------");
    }

    /**
     * realiza un rollback sobre la transacción activa.
     */
    public synchronized void rollback() {
        LOGGER.log(Level.FINER, "Rollback ------------------------");
        LOGGER.log(Level.FINER, "Dirty objects: " + dirty.size());
        if (this.orientdbTransact == null) {
            throw new NoOpenTx();
        }

        this.orientdbTransact.rollback();

        // refrescar todos los objetos
        for (Map.Entry<String, Object> entry : dirty.entrySet()) {
            String key = entry.getKey();
            IObjectProxy value = (IObjectProxy) entry.getValue();
            value.___rollback();
        }

        // se opta por eliminar el caché de objetos recuperados de la base en un commit o rollback
        // por lo que futuros pedidos a la base fuera de la transacción devolverá una nueva instancia
        // del objeto.
        this.objectCache.clear();

        // limpiar el caché de objetos modificados
        this.dirty.clear();
        this.nestedTransactionLevel = 0;
        LOGGER.log(Level.FINER, "FIN ROLLBACK.");
    }

    /**
     * Retorna el nivel de transacciones anidadas. El nivel superior es 0.
     *
     * @return nivel de transacciones anidadas.
     */
    public int getTransactionLevel() {
        return this.nestedTransactionLevel;
    }

    /**
     * Crea a un *NUEVO* vértice en la base de datos a partir del objeto. Retorna el RID del objeto que se agregó a la base.
     *
     * @param <T> clase base del objeto.
     * @param o objeto de referencia a almacenar.
     */
    @Override
    public synchronized <T> T store(T o) throws IncorrectRIDField, NoOpenTx, ClassToVertexNotFound {
//        graphdb.getRawGraph().activateOnCurrentThread();
        T proxied = null;
        
        // si el objeto ya fue guardado con anterioridad, devolver la instancia creada previamente.
        proxied = (T)this.commitedObject.get(o);
        if (proxied != null) {
            // devolver la instancia recuperada
            LOGGER.log(Level.FINER, "El objeto original ya había sido persistido. Se devuelve la instancia creada inicialmente.");
            
            // Aplicar los controles de seguridad.
            if ((this.sm.getLoggedInUser() != null) && (proxied instanceof SObject)) {
                LOGGER.log(Level.FINER, "SObject detectado. Aplicando seguridad de acuerdo al usuario logueado: " + this.sm.getLoggedInUser().getName());
                ((SObject) proxied).validate(this.sm.getLoggedInUser());
            }
            
            return proxied;
        }
        
        try {
            // si no hay una tx abierta, disparar una excepción
            if (this.orientdbTransact == null) {
                throw new NoOpenTx();
            }

            String classname;
            if (o instanceof IObjectProxy) {
                classname = o.getClass().getSuperclass().getSimpleName();
            } else {
                classname = o.getClass().getSimpleName();
            }
            LOGGER.log(Level.FINER, "STORE: guardando objeto de la clase " + classname);

            // Recuperar la definición de clase del objeto.
            ClassDef oClassDef = this.sm.getObjectMapper().getClassDef(o);
            // Obtener un map del objeto.
            ObjectStruct oStruct = objectMapper.objectStruct(o);
            Map<String, Object> omap = oStruct.fields;

            // verificar que la clase existe
            if (this.sm.getDBClass(classname) == null) {
                // arrojar una excepción en caso contrario.
                throw new ClassToVertexNotFound("No se ha encontrado la definición de la clase " + classname + " en la base!");
                //graphdb.createVertexType(classname);
            }

            OrientVertex v = this.orientdbTransact.addVertex("class:" + classname, omap);

            proxied = ObjectProxyFactory.create(o, v, this);

            // registrar el rid temporal para futuras referencias.
            newrids.add(v.getId().toString());

            // transferir todos los valores al proxy
            ReflectionUtils.copyObject(o, proxied);
            // convertir los embedded
            this.sm.getObjectMapper().collectionsToEmbedded(proxied, oClassDef, this);

            if (this.isAuditing()) {
                this.auditLog((IObjectProxy) proxied, Audit.AuditType.WRITE, "STORE", omap);
            }

            LOGGER.log(Level.FINER, "Marcando como dirty: " + proxied.getClass().getSimpleName());
            this.dirty.put(v.getId().toString(), proxied);

            // si se está en proceso de commit, registrar el objeto junto con el proxy para 
            // que no se genere un loop con objetos internos que lo referencien.
            this.commitedObject.put(o, proxied);

            // utlizado para analizar las anotations de campo.
            Field f;
            /* 
            procesar los objetos internos. Primero se debe determinar
            si los objetos ya existían en el contexto actual. Si no existen
            deben ser creados.
             */
            LOGGER.log(Level.FINER, "Procesando los Links");
            for (Map.Entry<String, Object> link : oStruct.links.entrySet()) {
                String field = link.getKey();
                String graphRelationName = classname + "_" + field;

                // verificar si no formaba parte de los objetos que se están comiteando
                Object innerO = this.commitedObject.get(link.getValue());
                // si no es así, recuperar el valor del campo
                if (innerO == null) {
                    LOGGER.log(Level.FINER, field + ": No existe el objeto en el cache de objetos creados.");
                    innerO = link.getValue();
                }
                
                // verificar si ya está en el contexto
                if (!(innerO instanceof IObjectProxy)) {
                    LOGGER.log(Level.FINER, "innerO nuevo. Crear un vértice y un link");
                    innerO = this.store(innerO);
                    // actualizar la referencia del objeto.
                    ObjectMapper.setFieldValue(proxied, field, innerO);

//                    innerRID = ((IObjectProxy)innerO).___getVertex().getId().toString();
                } else {
                    ObjectMapper.setFieldValue(proxied, field, innerO);
                }

                // crear un link entre los dos objetos.
                OrientEdge oe = this.orientdbTransact.addEdge("class:" + graphRelationName, v, ((IObjectProxy) innerO).___getVertex(), graphRelationName);
                if (this.isAuditing()) {
                    this.auditLog((IObjectProxy) proxied, Audit.AuditType.WRITE, "STORE: " + graphRelationName, oe);
                }
            }

            /* 
            procesar los LinkList. Primero se deber determinar
            si los objetos ya existían en el contexto actual. Si no existen
            deben ser creados.
             */
            LOGGER.log(Level.FINER, "Procesando los LinkList");
            LOGGER.log(Level.FINER, "LinkLists: " + oStruct.linkLists.size());

            final T finalProxied = proxied;

            for (Map.Entry<String, Object> link : oStruct.linkLists.entrySet()) {
                String field = link.getKey();
                Object value = link.getValue();
                
                final String graphRelationName = classname + "_" + field;
                
                LOGGER.log(Level.FINER, "field: " + field + " clase: " + value.getClass().getName());
                if (value instanceof List) {
                    // crear un objeto de la colección correspondiente para poder trabajarlo
                    // Class<?> oColection = oClassDef.linkLists.get(field);
                    Collection innerCol = (Collection) value;

                    // recorrer la colección verificando el estado de cada objeto.
                    LOGGER.log(Level.FINER, "Nueva lista: " + graphRelationName + ": " + innerCol.size() + " elementos");
                    for (Object llObject : innerCol) {
                        IObjectProxy ioproxied;
                        // verificar si ya está en el contexto
                        // verificar si no formaba parte de los objetos que se están comiteando
                        Object llO = this.commitedObject.get(llObject);
                        // si no es así, recuperar el valor del campo
                        if (llO == null) {
                            llO = llObject;
                        }

                        if (!(llO instanceof IObjectProxy)) {
                            LOGGER.log(Level.FINER, "llObject nuevo. Crear un vértice y un link");
                            ioproxied = (IObjectProxy) this.store(llO);
                        } else {
                            ioproxied = (IObjectProxy) llO;
                        }

                        // crear un link entre los dos objetos.
                        LOGGER.log(Level.FINE, "-----> agregando un edge a: " + ioproxied.___getVertex().getId());
                        OrientEdge oe = this.orientdbTransact.addEdge("class:" + graphRelationName, v, ioproxied.___getVertex(), graphRelationName);
                        if (this.isAuditing()) {
                            this.auditLog((IObjectProxy) proxied, Audit.AuditType.WRITE, "STORE: " + graphRelationName, oe);
                        }
                    }
                } else if (value instanceof Map) {
                    HashMap innerMap = (HashMap) value;
//                    final String ffield = field;
//                    final String frid = rid;
//                    final ConcurrentHashMap<String, OrientVertex> fVertexs = vertexs;
                    innerMap.forEach(new BiConsumer() {
                        @Override
                        public void accept(Object imk, Object imV) {
                            // para cada entrada, verificar la existencia del objeto y crear un Edge.
                            IObjectProxy ioproxied;

                            // verificar si ya no se había guardardo
                            Object imO = commitedObject.get(imV);
                            // si no es así, recuperar el valor del campo
                            if (imO == null) {
                                imO = imV;
                            }
                            if (imO instanceof IObjectProxy) {
                                ioproxied = (IObjectProxy) imO;
                            } else {
                                LOGGER.log(Level.FINER, "Link Map Object nuevo. Crear un vértice y un link");
                                ioproxied = (IObjectProxy) store(imO);
                            }
                            // crear un link entre los dos objetos.
                            LOGGER.log(Level.FINER, "-----> agregando el edges de " + v.getId().toString() + " para " + ioproxied.___getVertex().toString() + " key: " + imk);
                            OrientEdge oe = orientdbTransact.addEdge("class:" + graphRelationName, v, ioproxied.___getVertex(), graphRelationName);
                            if (isAuditing()) {
                                auditLog((IObjectProxy) finalProxied, Audit.AuditType.WRITE, "STORE: " + graphRelationName, oe);
                            }
                            // agragar la key como atributo.
                            if (Primitives.PRIMITIVE_MAP.get(imk.getClass()) != null) {
                                LOGGER.log(Level.FINER, "la prop del edge es primitiva");
                                // es una primitiva, agregar el valor como propiedad
                                oe.setProperty("key", imk);
                            } else {
                                LOGGER.log(Level.FINER, "la prop del edge es un Objeto. Se debe mapear!! ");
                                // mapear la key y asignarla como propiedades
                                oe.setProperties(sm.getObjectMapper().simpleMap(imk));
                            }
                        }
                    });

                }
                // convertir la colección a Lazy para futuras referencias.
                this.sm.getObjectMapper().colecctionToLazy(proxied, field, v, this);
            }

            // guardar el objeto en el cache. Se usa el RID como clave
            objectCache.put(v.getId().toString(), new WeakReference<>(proxied));

        } catch (IllegalArgumentException ex) {
            Logger.getLogger(SessionManager.class.getName()).log(Level.SEVERE, null, ex);
        }

        LOGGER.log(Level.FINER, "FIN del Store ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
        
        // Aplicar los controles de seguridad.
        if ((this.sm.getLoggedInUser() != null) && (proxied instanceof SObject)) {
            LOGGER.log(Level.FINER, "SObject detectado. Aplicando seguridad de acuerdo al usuario logueado: " + this.sm.getLoggedInUser().getName());
            ((SObject) proxied).validate(this.sm.getLoggedInUser());
        }
        
        return proxied;
    }

    /**
     * Remueve un vértice y todos los vértices apuntados por él y marcados con @RemoveOrphan
     *
     * @param toRemove referencia al objeto a remover
     */
    public void delete(Object toRemove) throws ReferentialIntegrityViolation, UnknownObject {
        LOGGER.log(Level.FINER, "Remove: " + toRemove.getClass().getName());

        // si no hereda de IObjectProxy, el objeto no pertenece a la base y no se debe hacer nada.
        if (toRemove instanceof IObjectProxy) {
            // verificar que la integridad referencial no se viole.
            OrientVertex ovToRemove = ((IObjectProxy) toRemove).___getVertex();
            // reacargar preventivamente el objeto.
            ovToRemove.reload();
            LOGGER.log(Level.FINER, "Referencias IN: " + ovToRemove.countEdges(Direction.IN));
            if (ovToRemove.countEdges(Direction.IN) > 0) {
                throw new ReferentialIntegrityViolation();
            }
            // Activar todos los campos que estén marcados con CascadeDelete o RemoveOrphan para poder procesarlos.
            // obtener el classDef del objeto
            ClassDef classDef = this.objectMapper.getClassDef(toRemove);

            // analizar el objeto
            Field f;
            
            for (Map.Entry<String, Class<?>> entry : classDef.links.entrySet()) {
                try {
                    String field = entry.getKey();
//                    f = ReflectionUtils.findField(((IObjectProxy) toRemove).___getBaseObject().getClass(), field);
                    f = ReflectionUtils.findField(toRemove.getClass(), field);

                    if (f.isAnnotationPresent(CascadeDelete.class) || f.isAnnotationPresent(RemoveOrphan.class)) {
                        LOGGER.log(Level.FINER, "CascadeDelete|RemoveOrphan presente. Activando el objeto...");
                        ((IObjectProxy) toRemove).___loadLazyLinks();
                    }
                } catch (NoSuchFieldException ex) {
                    Logger.getLogger(Transaction.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            // procesar los linkslist
            for (Map.Entry<String, Class<?>> entry : classDef.linkLists.entrySet()) {
                try {
                    String field = entry.getKey();
                    final String graphRelationName = toRemove.getClass().getSimpleName() + "_" + field;
                    Class<? extends Object> fieldClass = entry.getValue();

//                    f = ReflectionUtils.findField(((IObjectProxy) toRemove).___getBaseObject().getClass(), field);
                    f = ReflectionUtils.findField(toRemove.getClass(), field);
                    boolean acc = f.isAccessible();
                    f.setAccessible(true);

                    LOGGER.log(Level.FINER, "procesando campo: " + field);

                    // si hay una colección y corresponde hacer la cascada.
                    if (f.isAnnotationPresent(CascadeDelete.class)||f.isAnnotationPresent(RemoveOrphan.class)) {
                        LOGGER.log(Level.FINER, "CascadeDelete|RemoveOrphan presente. Activando el objeto...");
                        // activar el campo.
                        Collection oCol = (Collection) f.get(toRemove);
                        if (oCol != null) {
                            String garbage = oCol.toString();
                        }
                    }
                } catch (NoSuchFieldException | IllegalArgumentException | IllegalAccessException ex) {
                    Logger.getLogger(Transaction.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            // fin de la activación.
            
            // elimino el nodo de la base para que se actualicen los vértices a los que apuntaba.
            // de esta forma, los inner quedan libres y pueden ser borrados por un delete simple
            ovToRemove.remove();

            //Lista de vértices a remover
            List<OrientVertex> vertexToRemove = new ArrayList<>();

            // procesar los links
            for (Map.Entry<String, Class<?>> entry : classDef.links.entrySet()) {
                try {
                    String field = entry.getKey();
//                    f = ReflectionUtils.findField(((IObjectProxy) toRemove).___getBaseObject().getClass(), field);
                    f = ReflectionUtils.findField(toRemove.getClass(), field);

                    if (f.isAnnotationPresent(CascadeDelete.class)) {
                        // si se apunta a un objeto, removerlo
                        // CascadeDelete fuerza el borrado y falla si no puede. 
                        // Tiene precedencia sobre RemoveOrphan
                        Object value = f.get(toRemove);
                        if (value != null) {
                            this.delete(value);
                        }
                    } else if (f.isAnnotationPresent(RemoveOrphan.class)) {
                        // si se apunta a un objeto, removerlo
                        Object value = f.get(toRemove);
                        if (value != null) {
                            try {
                                this.delete(value);
                            } catch (ReferentialIntegrityViolation riv) {
                                LOGGER.log(Level.FINER, "RemoveOrphan: El objeto aún tiene vínculos.");
                            }
                        }
                    }

                } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException ex) {
                    Logger.getLogger(Transaction.class.getName()).log(Level.SEVERE, null, ex);
                }

            }

            // procesar los linkslist
            // la eliminación del Vertex actual elimina de la base los Edges correspondientes por lo que 
            // solo es necesario verificar si es necesario realizar una cascada en el borrado 
            // para eliminar vértices relacionads
            for (Map.Entry<String, Class<?>> entry : classDef.linkLists.entrySet()) {
                try {
                    String field = entry.getKey();
                    final String graphRelationName = toRemove.getClass().getSimpleName() + "_" + field;
                    Class<? extends Object> fieldClass = entry.getValue();

//                    f = ReflectionUtils.findField(((IObjectProxy) toRemove).___getBaseObject().getClass(), field);
                    f = ReflectionUtils.findField(toRemove.getClass(), field);
                    boolean acc = f.isAccessible();
                    f.setAccessible(true);

                    LOGGER.log(Level.FINER, "procesando campo: " + field);

//                    Collection oCol = (Collection) f.get(((IObjectProxy) toRemove).___getBaseObject());
                    Collection oCol = (Collection) f.get(toRemove);

                    // si hay una colección y corresponde hacer la cascada.
                    if ((oCol != null) && (f.isAnnotationPresent(CascadeDelete.class))) {
                        if (oCol instanceof List) {
                            for (Object object : oCol) {
                                this.delete(object);
                            }

                        } else if (oCol instanceof Map) {
                            HashMap oMapCol = (HashMap) oCol;
                            oMapCol.forEach((k, v) -> {
                                this.delete(v);
                            });

                        } else {
                            LOGGER.log(Level.FINER, "********************************************");
                            LOGGER.log(Level.FINER, "field: {0}", field);
                            LOGGER.log(Level.FINER, "********************************************");
                            throw new CollectionNotSupported(oCol.getClass().getSimpleName());
                        }
                        f.setAccessible(acc);
                    } else if ((oCol != null) && (f.isAnnotationPresent(RemoveOrphan.class))) {

                        if (oCol instanceof List) {
                            for (Object object : oCol) {

                                try {
                                    this.delete(object);
                                } catch (ReferentialIntegrityViolation riv) {
                                    LOGGER.log(Level.FINER, "RemoveOrphan: El objeto aún tiene vínculos.");
                                }

                            }

                        } else if (oCol instanceof Map) {
                            HashMap oMapCol = (HashMap) oCol;
                            oMapCol.forEach((k, v) -> {
                                try {
                                    this.delete(v);
                                } catch (ReferentialIntegrityViolation riv) {
                                    LOGGER.log(Level.FINER, "RemoveOrphan: El objeto aún tiene vínculos.");
                                }
                            });

                        } else {
                            LOGGER.log(Level.FINER, "********************************************");
                            LOGGER.log(Level.FINER, "field: {0}", field);
                            LOGGER.log(Level.FINER, "********************************************");
                            throw new CollectionNotSupported(oCol.getClass().getSimpleName());
                        }
                        f.setAccessible(acc);
                    }

                } catch (NoSuchFieldException | IllegalArgumentException | IllegalAccessException ex) {
                    Logger.getLogger(SessionManager.class.getName()).log(Level.SEVERE, null, ex);
                }

            }

            if (this.isAuditing()) {
                this.auditLog((IObjectProxy) toRemove, Audit.AuditType.DELETE, "DELETE", "");
            }

            //ovToRemove.remove(); movido arriba.
            // si tengo un RID, proceder a removerlo de las colecciones.
            this.dirty.remove(((IObjectProxy) toRemove).___getVertex().getId().toString());
            // invalidar el objeto
            ((IObjectProxy) toRemove).___setDeletedMark();
        } else {
            throw new UnknownObject();
        }

    }

    /**
     * Transfiere todos los cambios de los objetos a las estructuras subyacentes.
     */
    public synchronized void flush() {
        if (this.orientdbTransact == null) {
            throw new NoOpenTx();
        }
        for (Map.Entry<String, Object> e : dirty.entrySet()) {
            String rid = e.getKey();
            IObjectProxy o = (IObjectProxy) e.getValue();

            // actualizar todos los objetos a nivel de cache de la base sin proceder a 
            // bajarlos efectívamente.
            o.___commit();
        }
    }

    /**
     * retorna el SessionManager asociado a la transacción
     *
     * @return sm
     */
    public SessionManager getSessionManager() {
        return this.sm;
    }

    /**
     * Devuelve el ObjectMapper asociado a la transacción
     *
     * @return objectMapper
     */
    public ObjectMapper getObjectMapper() {
        return this.objectMapper;
    }

    /**
     * Agrega si no existe un objeto al cache de la transacción acutal a fin de evitar los loops cuando se comletan los links dentro del ObjectProxy
     *
     * @param rid RID del objeto.
     * @param o objeto a controlar.
     */
    public void addToTransactionCache(String rid, Object o) {
        getTransactionCount++;
        if (this.transactionCache.get(rid) == null) {
            LOGGER.log(Level.FINER, "Forzando el agregado al TransactionCache de " + rid);
            this.transactionCache.put(rid, o);
        }
    }

    /**
     * invocado desde ObjectProxy al completar los links.
     */
    public void decreseTransactionCache() {
        getTransactionCount--;
        if (getTransactionCount == 0) {
            transactionCache.clear();
        }
    }

    /**
     * Retorna la cantidad de objetos marcados como Dirty. Utilizado para los test
     *
     * @return retorna la cantidad de objetos marcados para el próximo commit
     */
    public int getDirtyCount() {
        return this.dirty.size();
    }

    /**
     * Recupera un objecto desde la base a partir del RID del Vértice.
     *
     * @param rid: ID del vértice a recupear
     * @return Retorna un objeto de la clase javaClass del vértice.
     */
    @Override
    public Object get(String rid) throws UnknownRID, VertexJavaClassNotFound {
        try {
            if (this.orientdbTransact == null) {
                throw new NoOpenTx();
            }
            if (rid == null) {
                throw new UnknownRID();
            }
            Object ret = null;

            if (objectCache.get(rid) != null) {
                ret = objectCache.get(rid).get();
                // si fue recuperado del caché, determinar si se ha modificado.
                // si no fue modificado, hacer un reload para actualizar con la última 
                // versión de la base de datos.
                if (!((IObjectProxy) ret).___isDirty()) {
                    ((IObjectProxy) ret).___reload();
                }
            }

            // si ret == null, recuperar el objeto desde la base, en caso contrario devolver el objeto desde el caché
            if (ret == null) {
                objectCache.remove(rid);

                OrientVertex v = this.orientdbTransact.getVertex(rid);
                if (v == null) {
                    throw new UnknownRID(rid);
                }
                String javaClass = v.getType().getCustom("javaClass");
                if (javaClass == null) {
                    throw new VertexJavaClassNotFound("La clase del Vértice no tiene la propiedad javaClass");
                }
                javaClass = javaClass.replaceAll("[\'\"]", "");
                Class<?> c = Class.forName(javaClass);
                ret = this.get(c, rid);
            } else {
                LOGGER.log(Level.FINER, "Objeto Recupeardo del caché.");
                // validar contra el usuario actualmente logueado si corresponde.
                if ((this.sm.getLoggedInUser() != null) && (ret instanceof SObject)) {
                    ((SObject) ret).validate(this.sm.getLoggedInUser());
                }
            }
            return ret;

        } catch (ClassNotFoundException ex) {
            Logger.getLogger(SessionManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    /**
     * Recupera un objeto a partir de la clase y el RID correspondiente.
     *
     * @param <T> clase a devolver
     * @param type clase a devolver
     * @param rid RID del vértice de la base
     * @return objeto de la clase T
     */
    @Override
    public <T> T get(Class<T> type, String rid) throws UnknownRID {
        if (this.orientdbTransact == null) {
            throw new NoOpenTx();
        }
        if (rid == null) {
            throw new UnknownRID();
        }
        T o = null;

        // si está en el caché, devolver la referencia desde ahí.
        if (objectCache.get(rid) != null) {
            if (objectCache.get(rid).get() != null) {
                LOGGER.log(Level.FINER, "Objeto Recupeardo del caché.");
                o = (T) objectCache.get(rid).get();

                // si fue recuperado del caché, determinar si se ha modificado.
                // si no fue modificado, hacer un reload para actualizar con la última 
                // versión de la base de datos.
                if (!((IObjectProxy) o).___isDirty()) {
                    ((IObjectProxy) o).___reload();
                }

            } else {
                objectCache.remove(rid);
            }
        }

        if (o == null) {
            // iniciar el conteo de gets. Todos los gets se guardan en un mapa 
            // para impedir que un único get entre en un loop cuando un objeto
            // tiene referencias a su padre.
            getTransactionCount++;

            LOGGER.log(Level.FINER, "Obteniendo objeto type: " + type.getSimpleName() + " en RID: " + rid);

            // verificar si ya no se ha cargado
            o = (T) this.transactionCache.get(rid);

            if (o == null) {
                // recuperar el vértice solicitado
                OrientVertex v = this.orientdbTransact.getVertex(rid);

                // hidratar un objeto
                try {
                    o = objectMapper.hydrate(type, v, this);
//                entities.put(rid, o);

                } catch (InstantiationException | IllegalAccessException | NoSuchFieldException ex) {
                    Logger.getLogger(SessionManager.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else {
                LOGGER.log(Level.FINER, "Objeto recuperado del dirty cache! : " + o.getClass().getSimpleName());
            }
            getTransactionCount--;
            if (getTransactionCount == 0) {
                LOGGER.log(Level.FINER, "Fin de la transacción. Reseteando el cache.....................");
                this.transactionCache.clear();
            }

            // cuardar el objeto en el caché
            objectCache.put(rid, new WeakReference<>(o));
        }

        // Aplicar los controles de seguridad.
        if ((this.sm.getLoggedInUser() != null) && (o instanceof SObject)) {
            LOGGER.log(Level.FINER, "SObject detectado. Aplicando seguridad de acuerdo al usuario logueado: " + this.sm.getLoggedInUser().getName());
            ((SObject) o).validate(this.sm.getLoggedInUser());
        }

        if (this.isAuditing()) {
            this.auditLog((IObjectProxy) o, Audit.AuditType.READ, "READ", "");
        }

        LOGGER.log(Level.FINER, "Fin get -------------------------------------\n");
        return o;
    }

    @Override
    public <T> T getEdgeAsObject(Class<T> type, OrientEdge e) {
        if (this.orientdbTransact == null) {
            throw new NoOpenTx();
        }

        T o = null;
        try {
            // verificar si ya no se ha cargado
            o = this.objectMapper.hydrate(type, e, this);

            // Aplicar los controles de seguridad.
            if ((this.sm.getLoggedInUser() != null) && (o instanceof SObject)) {
                ((SObject) o).validate(this.sm.getLoggedInUser());
            }

        } catch (InstantiationException ex) {
            Logger.getLogger(SessionManager.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(SessionManager.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchFieldException ex) {
            Logger.getLogger(SessionManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return o;
    }

    /**
     * Realiza un query direto a la base de datos y devuelve el resultado directamente sin procesarlo.
     *
     * @param <T> clase a devolver
     * @param sql sentencia a ejecutar
     * @return resutado de la ejecución de la sentencia SQL
     */
    @Override
    public <T> T query(String sql) {
        if (this.orientdbTransact == null) {
            throw new NoOpenTx();
        }
        flush();

        OCommandSQL osql = new OCommandSQL(sql);
        return this.orientdbTransact.command(osql).execute();
    }

    /**
     * Realiza un query direto a la base de datos y devuelve el resultado directamente sin procesarlo.
     *
     * @param <T> clase a devolver
     * @param sql sentencia a ejecutar
     * @param param parámetros a utilizar en el query
     * @return resutado de la ejecución de la sentencia SQL
     */
    @Override
    public <T> T query(String sql, Object... param) {
        if (this.orientdbTransact == null) {
            throw new NoOpenTx();
        }
        flush();

        OCommandSQL osql = new OCommandSQL(sql);
        return this.orientdbTransact.command(osql).execute(param);
    }

    /**
     * Ejecuta un comando que devuelve un número. El valor devuelto será el primero que se encuentre en la lista de resultado.
     *
     * @param sql comando a ejecutar
     * @param retVal nombre de la propiedad a devolver. Puede ser "". En ese caso se devolverá el primer valor encontrado.
     * @return retorna el valor de la propiedad indacada obtenida de la ejecución de la consulta
     *
     * ejemplo: int size = sm.query("select count(*) as size from TestData","size");
     */
    @Override
    public long query(String sql, String retVal) {
        if (this.orientdbTransact == null) {
            throw new NoOpenTx();
        }
        this.flush();

        OCommandSQL osql = new OCommandSQL(sql);
        OrientVertex ov = (OrientVertex) ((OrientDynaElementIterable) this.orientdbTransact.command(osql).execute()).iterator().next();
        if (retVal.isEmpty()) {
            retVal = ov.getProperties().keySet().iterator().next();
        }

        return ov.getProperty(retVal);
    }

    /**
     * Return all record of the reference class. Devuelve todos los registros a partir de una clase base.
     *
     * @param <T> Reference class
     * @param clazz reference class
     * @return return a list of object of the refecence class.
     */
    @Override
    public <T> List<T> query(Class<T> clazz) {
        if (this.orientdbTransact == null) {
            throw new NoOpenTx();
        }
        this.flush();

        long init = System.currentTimeMillis();

        ArrayList<T> ret = new ArrayList<>();

        Iterable<Vertex> vertices = this.orientdbTransact.getVerticesOfClass(clazz.getSimpleName());
        LOGGER.log(Level.FINER, "Enlapsed ODB response: " + (System.currentTimeMillis() - init));
        for (Vertex verticesOfClas : vertices) {
            ret.add(this.get(clazz, verticesOfClas.getId().toString()));
//            LOGGER.log(Level.FINER, "vertex: " + verticesOfClas.getId().toString() + "  class: " + ((OrientVertex) verticesOfClas).getType().getName());
        }
        LOGGER.log(Level.FINER, "Enlapsed time query to List: " + (System.currentTimeMillis() - init));
        return ret;
    }

    /**
     * Devuelve todos los registros a partir de una clase base en una lista, filtrando los datos por lo que se agregue en el body.
     *
     * @param <T> clase base que se utilizará para el armado de la lista
     * @param clase clase base.
     * @param body cuerpo a agregar a la sentencia select. Ej: "where ...."
     * @return Lista con todos los objetos recuperados.
     */
    @Override
    public <T> List<T> query(Class<T> clase, String body) {
        if (this.orientdbTransact == null) {
            throw new NoOpenTx();
        }
        this.flush();

        ArrayList<T> ret = new ArrayList<>();

        String cSQL = "SELECT FROM " + clase.getSimpleName() + " " + body;
        LOGGER.log(Level.FINER, cSQL);
        for (Vertex v : (Iterable<Vertex>) this.orientdbTransact.command(
                new OCommandSQL(cSQL)).execute()) {
            ret.add(this.get(clase, v.getId().toString()));
        }

        return ret;
    }

    /**
     * Ejecuta un prepared query y devuelve una lista de la clase indicada.
     *
     * @param <T> clase de referencia para crear la lista de resultados
     * @param clase clase de referencia
     * @param sql comando a ejecutar
     * @param param parámetros extras para el query parametrizado.
     * @return una lista de la clase solicitada con los objetos lazy inicializados.
     */
    @Override
    public <T> List<T> query(Class<T> clase, String sql, Object... param) {
        OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<>(sql);
        ArrayList<T> ret = new ArrayList<>();

        LOGGER.log(Level.FINER, sql + " param: "+param);
        for (Vertex v : (Iterable<Vertex>) this.orientdbTransact.command(query).execute(param)) {
            ret.add(this.get(clase, v.getId().toString()));
        }
        return ret;
    }

    /**
         * Ejecuta un prepared query y devuelve una lista de la clase indicada.
         * Esta consulta acepta parámetros por nombre. 
         * Ej:
         * <pre> {@code 
         *  Map<String, Object> params = new HashMap<String, Object>();
         *  params.put("theName", "John");
         *  params.put("theSurname", "Smith");
         *
         *  graph.command(
         *       new OCommandSQL("UPDATE Customer SET local = true WHERE name = :theName and surname = :theSurname")
         *      ).execute(params)
         *  );
         *  }
         * </pre>
         * @param <T> clase de referencia para crear la lista de resultados
         * @param clase clase de referencia
         * @param sql comando a ejecutar
         * @param param parámetros extras para el query parametrizado.
         * @return una lista de la clase solicitada con los objetos lazy inicializados.
         */
    @Override
    public <T> List<T> query(Class<T> clase, String sql, HashMap<String,Object> param) {
        OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<>(sql);
        ArrayList<T> ret = new ArrayList<>();

        LOGGER.log(Level.FINER, sql);
        for (Vertex v : (Iterable<Vertex>) this.orientdbTransact.command(query).execute(param)) {
            ret.add(this.get(clase, v.getId().toString()));
        }
        return ret;
    }

    /**
     * Devuelve el objecto de definición de la clase en la base.
     *
     * @param clase nombre de la clase
     * @return OClass o null si la clase no existe
     */
    public OClass getDBClass(String clase) {
        return this.getGraphdb().getRawGraph().getMetadata().getSchema().getClass(clase);
    }

    // Procesos de auditoría
    /**
     * Comienza a auditar los objetos y los persiste con el nombre de usuario.
     *
     * @param user UserSID String only.
     */
    public void setAuditOnUser(String user) {

        this.auditor = new Auditor(this, user);

    }

    /**
     * Comienza a auditar los objetos y los persiste con el nombre de usuario actualmente logueado.
     *
     */
    public void setAuditOnUser() throws NoUserLoggedIn {
        if (this.sm.getLoggedInUser() == null) {
            throw new NoUserLoggedIn();
        }

        this.auditor = new Auditor(this, this.sm.getLoggedInUser().getUUID());
    }

    /**
     * realiza una auditoría a partir del objeto indicado.
     *
     * @param o IOBjectProxy a auditar
     * @param at AuditType
     * @param label etiqueta de referencia
     * @param data objeto a loguear con un toString
     */
    public synchronized void auditLog(IObjectProxy o, int at, String label, Object data) {
        if (this.isAuditing()) {
            auditor.auditLog(o, at, label, data);
        }
    }

    /**
     * determina si se está guardando un log de auditoría
     *
     * @return true si la auditoría está activa
     */
    public boolean isAuditing() {
        return this.auditor != null;
    }

    Auditor getAuditor() {
        return this.auditor;
    }
}
