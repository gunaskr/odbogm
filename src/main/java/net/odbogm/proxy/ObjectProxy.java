package net.odbogm.proxy;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.Iterators;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import net.odbogm.LogginProperties;
import net.odbogm.ObjectMapper;
import net.odbogm.ObjectStruct;
import net.odbogm.SessionManager;
import net.odbogm.Transaction;
import net.odbogm.agent.ITransparentDirtyDetector;
import net.odbogm.annotations.Audit.AuditType;
import net.odbogm.annotations.Indirect;
import net.odbogm.annotations.RemoveOrphan;
import net.odbogm.cache.ClassDef;
import net.odbogm.exceptions.CollectionNotSupported;
import net.odbogm.exceptions.DuplicateLink;
import net.odbogm.exceptions.InvalidObjectReference;
import net.odbogm.exceptions.ObjectMarkedAsDeleted;
import net.odbogm.exceptions.OdbogmException;
import net.odbogm.utils.ReflectionUtils;
import net.odbogm.utils.ThreadHelper;
import net.odbogm.utils.VertexUtils;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

/**
 *
 * @author Marcelo D. Ré {@literal <marcelo.re@gmail.com>}
 */
public class ObjectProxy implements IObjectProxy, MethodInterceptor {

    private final static Logger LOGGER = Logger.getLogger(ObjectProxy.class.getName());
    static {
        if (LOGGER.getLevel() == null) {
            LOGGER.setLevel(LogginProperties.ObjectProxy);
        }
    }

    // the real object      
    private Object ___proxiedObject;

    private final Class<?> ___baseClass;

    // Vértice desde el que se obtiene el objeto.
    // private OrientVertex baseVertex;
    private OElement ___baseElement;

    // permite marcar el objeto como inválida en caso que se haga un rollback 
    // sobre un objeto que nunca se persistió.
    private boolean ___isValidObject = true;

    private final Transaction ___transaction;

    private boolean ___dirty = false;

    // determina si ya se han cargado los links o no
    private boolean ___loadLazyLinks = true;

    // determina si el objeto ya ha sido completamente inicializado.
    // sirve para impedir que se invoquen a los métodos durante el setup inicial del construtor.
    private boolean ___objectReady = false;

    // si esta marca está activa indica que el objeto ha sido eliminado de la base de datos 
    // y toda comunicación con el mismo debe ser abortada
    private boolean ___deletedMark = false;


    public ObjectProxy(Class c, OElement e, Transaction t) {
        this.___baseClass = c;
        this.___baseElement = e;
        this.___transaction = t;
    }
    
    
    // GCLib interceptor 
    @Override
    public Object intercept(Object o,
            Method method,
            Object[] args,
            MethodProxy methodProxy) throws Throwable {
        // response object
        Object res = null;

        // el estado del objeto se debe poder consultar siempre
        
        if (method.getName().equals("___isValid")) {
            return this.___isValid();
        }
        
        if (method.getName().equals("___isDeleted")) {
            return this.___isDeleted();
        }

        if (method.getName().equals("___getVertex")) {
            if (this.___objectReady) {
                return this.___getVertex();
            }
        }

        if (method.getName().equals("___getBaseClass")) {
            if (this.___objectReady) {
                return this.___getBaseClass();
            }
        }

        if (!this.___isValidObject) {
            LOGGER.log(Level.FINER, "El objeto está marcado como inválido!!!");
            throw new InvalidObjectReference();
        }

        if (method.getName().equals("___rollback")) {
            if (this.___objectReady) {
                this.___rollback();
                return true;
            }
        }

        if (this.___baseElement.getIdentity().isNew()) {
            LOGGER.log(Level.FINER, "RID nuevo. No procesar porque el store preparó todo y no hay nada que recuperar de la base.");
            this.___loadLazyLinks = false;
        }
        // BEFORE
        // measure the current time         
//        long time1 = System.currentTimeMillis();
//        System.out.println("intercepted: " + method.getName());
        // modificar el llamado
        if (!this.___deletedMark) {
            switch (method.getName()) {
                case "___getRid":
                    if (this.___objectReady) {
                        res = this.___getRid();
                    }
                    break;
                case "___injectRid":
                    if (this.___objectReady) {
                        this.___injectRid();
                    }
                    break;
                case "___getProxiedObject":
                    if (this.___objectReady) {
                        res = this.___getProxiedObject();
                    }
                    break;
                case "___loadLazyLinks":
                    if (this.___objectReady) {
                        this.___loadLazyLinks();
                    }
                    break;
                case "___updateIndirectLinks":
                    if (this.___objectReady) {
                        this.___updateIndirectLinks();
                    }
                    break;
                case "___isDirty":
                    if (this.___objectReady) {
                        res = this.___isDirty();
                    }
                    break;
                case "___setDirty":
                    if (this.___objectReady) {
                        this.___setDirty();
                    }
                    break;
                case "___removeDirtyMark":
                    if (this.___objectReady) {
                        this.___removeDirtyMark();
                    }
                    break;
                case "___commit":
                    /**
                     * FIXME: se podría evitar si se controlara si los links se
                     * han cargado o no al momento de hacer el commit para
                     * evitar realizar el
                     * load sin necesidad.
                     */
                    if (this.___objectReady) {
                        if (this.___loadLazyLinks) {
                            this.___loadLazyLinks();
                        }
                        this.___commit();
                    }
                    break;
                case "___reload":
                    if (this.___objectReady) {
                        this.___reload();
                    }
                    break;
                case "___setDeletedMark":
                    this.___setDeletedMark();
                    break;

                case "___ogm___setDirty":
                    res = methodProxy.invokeSuper(o, args);
                    break;
                case "___ogm___isDirty":
//                    LOGGER.log(Level.INFO, "Method: sup.name: "+methodProxy.getSuperName()+
//                                                       " - sig: "+methodProxy.getSignature()+
//                                                       " - sup idx: "+methodProxy.getSuperIndex()
//                            );
                    res = methodProxy.invokeSuper(o, args);
                    break;
                default:
                    // invoke the method on the real object with the given params
                    if (this.___objectReady) {
                        if (this.___loadLazyLinks) {
                            this.___loadLazyLinks();
                        }
                    }

                    if (method.getName().equals("toString")) {
                        try {
                            ReflectionUtils.findMethod(this.___baseClass, "toString", (Class<?>[]) null);
                            res = methodProxy.invokeSuper(o, args);
                        } catch (NoSuchMethodException nsme) {
                            res = this.___baseElement.getIdentity().toString();
                        }
                    } else {
                        LOGGER.log(Level.FINEST, "invocando: " + method.getName());
                        res = methodProxy.invokeSuper(o, args);
                    }

                    // Check if there are differences between the objects depending on the selected strategy.
                    if (this.___objectReady) {
                        switch (this.___transaction.getSessionManager().getActivationStrategy()) {
                            case CLASS_INSTRUMENTATION:
                                // if class instrumentation is being used, directly check on the object
                                // What is your status?
                                LOGGER.log(Level.FINEST, "o: " + o.getClass().getName() + " ITrans: " + (o instanceof ITransparentDirtyDetector));
                                if (((ITransparentDirtyDetector) o).___ogm___isDirty()) {
                                    LOGGER.log(Level.FINEST, "objeto {0} marked as dirty by ASM. Add it to the pending list.", o.getClass().getName());
                                    this.___setDirty();
                                }
                        }
                    }

                    break;
            }
        } else {
            throw new ObjectMarkedAsDeleted("The object " + this.___baseElement.getIdentity().toString() + " was deleted from the database. Trying to call to " + method.getName());
        }
        // AFTER
        // print how long it took to execute the method on the proxified object
//        System.out.println("Took: " + (System.currentTimeMillis() - time1) + " ms");
        // return the result         
        return res;
    }


    /**
     * Establece el objeto base sobre el que trabaja el proxy.
     *
     * @param po objeto de referencia
     */
    public void ___setProxiedObject(Object po) {
        this.___proxiedObject = po;
        this.___injectRid();
        this.___objectReady = true;
    }
    
    
    @Override
    public void ___injectRid() {
        //inyectar RID si está definido el campo
        ClassDef classdef = ___transaction.getObjectMapper().getClassDef(___baseClass);
        if (classdef.ridField != null) {
            try {
                classdef.ridField.set(___proxiedObject, ___baseElement.getIdentity().toString());
            } catch (IllegalArgumentException | IllegalAccessException ex) {
                LOGGER.log(Level.WARNING, "Couldn't inject RID in proxy.", ex);
            }
        }
    }


    /**
     * retorna el vértice asociado a este proxi o null en caso que no exista
     * uno.
     *
     * @return referencia al OrientVertex
     */
    @Override
    public OVertex ___getVertex() {
        if (this.___baseElement instanceof OVertex) {
            return (OVertex) this.___baseElement;
        } else {
            return null;
        }
    }


    /**
     * retorna el vértice asociado a este proxi o null en caso que no exista
     * uno.
     *
     * @return el RID del object en la base
     */
    @Override
    public ORID ___getRid() {
        if (this.___baseElement != null) {
            return this.___baseElement.getIdentity();
        } else {
            return null;
        }
    }


    /**
     *
     * establece el elemento base como un vértice.
     *
     * @param v vétice de referencia
     */
    @Override
    public void ___setVertex(OVertex v) {
        this.___baseElement = v;
    }


    /**
     * returns the vertex associated with this proxy or null in case there is none.
     *
     * @return the reference to the OVertex
     */
    @Override
    public OVertex ___getEdge() {
        if (this.___baseElement instanceof OEdge) {
            return (OVertex) this.___baseElement;
        } else {
            return null;
        }
    }


    /**
     *
     * establece el elemento base como un vértice.
     *
     * @param e Edge de referencia
     */
    @Override
    public void ___setEdge(OEdge e) {
        this.___baseElement = e;
    }


    @Override
    public Object ___getProxiedObject() {
        return this.___proxiedObject;
    }


    @Override
    public Class<?> ___getBaseClass() {
        return this.___baseClass;
    }


    @Override
    public void ___setDeletedMark() {
        this.___deletedMark = true;
    }


    @Override
    public boolean ___isDeleted() {
        return this.___deletedMark;
    }


    /**
     * Carga todos los links del objeto
     */
    @Override
    public synchronized void ___loadLazyLinks() {
        if (this.___loadLazyLinks) {
            this.___transaction.initInternalTx();

            LOGGER.log(Level.FINER, "Base class: {0}", this.___baseClass.getSimpleName());
            LOGGER.log(Level.FINER, "iniciando loadLazyLinks...");
            boolean currentDirtyState = this.___isDirty();
            // marcar que ya se han incorporado todo los links
            this.___loadLazyLinks = false;

            if (this.___baseElement instanceof OrientVertex) {
                OrientVertex ov = (OrientVertex) this.___baseElement;
                ClassDef classdef = this.___transaction.getObjectMapper().getClassDef(this.___proxiedObject);

                // hidratar los atributos @links
                // procesar todos los links y los indirectLinks
                LOGGER.log(Level.FINER, "procesando {0} links ", classdef.links.size());
                for (Map.Entry<String, Class<?>> entry : classdef.links.entrySet()) {
                    try {
                        String field = entry.getKey();
                        Class<?> fc = entry.getValue();
                        Field fLink = classdef.fieldsObject.get(field);

                        String graphRelationName = classdef.entityName + "_" + field;
                        Direction direction = Direction.OUT;
                        LOGGER.log(Level.FINER, "Field: {0}.{1}   Class: {2}  RelationName: {3}",
                                new String[]{this.___baseClass.getSimpleName(), field,
                                    fc.getSimpleName(), graphRelationName});

                        // recuperar de la base el vértice correspondiente
                        boolean duplicatedLinkGuard = false;
                        for (Vertex vertice : ov.getVertices(direction, graphRelationName)) {
                            LOGGER.log(Level.FINER, "hydrate innerO: {0}", vertice.getId());

                            if (!duplicatedLinkGuard) {
                                /*
                                 * FIXME: esto genera una dependencia cruzada.
                                 * Habría que revisar
                                 * como solucionarlo. Esta llamada se hace para
                                 * que quede el objeto
                                 * mapeado
                                 */
                                this.___transaction.addToTransactionCache(this.___getRid(), ___proxiedObject);

                                // si es una interface llamar a get solo con el RID.
                                Object innerO = null;

                                innerO = fc.isInterface() ? this.___transaction.get(vertice.getId().toString()) : this.___transaction.get(fc, vertice.getId().toString());

                                LOGGER.log(Level.FINER, "Inner object " + field + ": "
                                        + (innerO == null ? "NULL" : "" + innerO.toString())
                                        + "  FC: " + fc.getSimpleName()
                                        + "   innerO.class: " + innerO.getClass().getSimpleName()
                                        + " hashCode: " + System.identityHashCode(innerO));
                                fLink.set(this.___proxiedObject, fc.cast(innerO));
                                duplicatedLinkGuard = true;

                                ___transaction.decreseTransactionCache();
                            } else if (false) {
                                throw new DuplicateLink();
                            }
                            LOGGER.log(Level.FINER, "FIN hydrate innerO: " + vertice.getId() + "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
                        }
                    } catch (SecurityException | IllegalArgumentException | IllegalAccessException ex) {
                        Logger.getLogger(ObjectMapper.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }

                // actualizar los indirectLinks
                this.___updateIndirectLinks();
            }

            // resetear dirty si corresponde.
            this.___dirty = currentDirtyState;

            this.___transaction.closeInternalTx();
        }
    }


    @Override
    public void ___updateIndirectLinks() {
        if (this.___baseElement instanceof OrientVertex) {
            boolean preservDirtyState = this.___dirty;

            OVertex ov = (OVertex) this.___baseElement;
            ClassDef classdef = this.___transaction.getObjectMapper().getClassDef(this.___proxiedObject);

            // hidratar los atributos @indirectLinks
            Map<String, Class<?>> lnks = new HashMap<>();
            lnks.putAll(classdef.indirectLinks);
            LOGGER.log(Level.FINER, "procesando {0} indirected links", new Object[]{classdef.indirectLinks.size()});
            for (Map.Entry<String, Class<?>> entry : lnks.entrySet()) {
                try {
                    String field = entry.getKey();
                    Class<?> fc = entry.getValue();
                    Field fLink = classdef.fieldsObject.get(field);

                    // se debe usar el nombre de la relación propuesto por la anotation
                    Indirect in = fLink.getAnnotation(Indirect.class);
                    String graphRelationName = in.linkName();
                    ODirection direction = ODirection.IN;
                    LOGGER.log(Level.FINER, "Se ha detectado un indirect. Linkname = {0}", new Object[]{in.linkName()});
                    LOGGER.log(Level.FINER, "Field: {0}.{1}   Class: {2}  RelationName: {3}", new String[]{this.___baseClass.getSimpleName(), field, fc.getSimpleName(), graphRelationName});

                    // recuperar de la base el vértice correspondiente
                    boolean duplicatedLinkGuard = false;
                    for (OVertex vertice : ov.getVertices(direction, graphRelationName)) {
                        LOGGER.log(Level.FINER, "hydrate innerO: " + vertice.getIdentity());

                        if (!duplicatedLinkGuard) {
//                        Object innerO = this.hydrate(fc, vertice);
                            /*
                             * FIXME: esto genera una dependencia cruzada.
                             * Habría que revisar
                             * como solucionarlo. Esta llamada se hace para que
                             * quede el objeto
                             * mapeado
                             */
                            this.___transaction.addToTransactionCache(this.___getRid(), ___proxiedObject);

                            // si es una interface llamar a get solo con el RID.
                            Object innerO = null;

                            innerO = fc.isInterface() ? this.___transaction.get(vertice.getIdentity().toString()) : this.___transaction.get(fc, vertice.getIdentity().toString());

                            LOGGER.log(Level.FINER, "Inner object " + field + ": "
                                    + (innerO == null ? "NULL" : "" + innerO.toString())
                                    + "  FC: " + fc.getSimpleName()
                                    + "   innerO.class: " + innerO.getClass().getSimpleName()
                                    + " hashCode: " + System.identityHashCode(innerO));
                            fLink.set(this.___proxiedObject, fc.cast(innerO));
                            duplicatedLinkGuard = true;

                            ___transaction.decreseTransactionCache();
                        } else if (false) {
                            throw new DuplicateLink();
                        }
                        LOGGER.log(Level.FINER, "FIN hydrate innerO: " + vertice.getIdentity() + "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
                    }
                } catch (SecurityException | IllegalArgumentException | IllegalAccessException ex) {
                    Logger.getLogger(ObjectMapper.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            // forzar la recarga de las colecciones.
            LOGGER.log(Level.FINER, "Refrescando las colecciones indirectas...");
            // ********************************************************************************************
            // hidratar las colecciones indirectas
            // procesar todos los indirectLinkslist
            // ********************************************************************************************
            for (Map.Entry<String, Class<?>> entry : classdef.indirectLinkLists.entrySet()) {
                try {
                    // FIXME: se debería considerar agregar una annotation EAGER!
                    String field = entry.getKey();
                    Class<?> fc = entry.getValue();
                    LOGGER.log(Level.FINER, "Field: {0}   Class: {1}", new String[]{field, fc.getName()});

                    Field fLink = classdef.fieldsObject.get(field);
                    ODirection relationDirection = ODirection.IN;

                    Indirect in = fLink.getAnnotation(Indirect.class);
                    String graphRelationName = in.linkName();

                    // if there are connected vertices or if the object's constructor has initialized the vectors, convert them
                    if ((Iterators.size(ov.getEdges(relationDirection, graphRelationName).iterator()) > 0) || (fLink.get(___proxiedObject) != null)) {
                    	
                        this.___transaction.getObjectMapper().colecctionToLazy(___proxiedObject, field, fc, ov, ___transaction);
                    }

                } catch (IllegalAccessException | IllegalArgumentException ex) {
                    Logger.getLogger(ObjectProxy.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            // volver a establecer el estado de Dirty.
            this.___dirty = preservDirtyState;
        }
    }


    @Override
    public boolean ___isValid() {
        return ___isValidObject;
    }


    @Override
    public boolean ___isDirty() {
        return ___dirty;
    }


    /**
     * Marca el objeto como dirty para que sea considerado en el próximo commit
     *
     */
    @Override
    public void ___setDirty() {
        if (!this.___dirty) {
            this.___dirty = true;
            // agregarlo a la lista de dirty para procesarlo luego
            LOGGER.log(Level.FINER, "Dirty: " + this.___proxiedObject);
            LOGGER.log(Level.FINER, "Objeto marcado como dirty! <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
            LOGGER.log(Level.FINEST, ThreadHelper.getCurrentStackTrace());
        }
    }


    @Override
    public void ___removeDirtyMark() {
        this.___dirty = false;
        // Verify the activation strategy.
        // if the strategy is ONCOMMIT, it must be validated first that there are changes in the objects
        // before proceeding.
        if (this.___transaction.getSessionManager().getActivationStrategy() == SessionManager.ActivationStrategy.CLASS_INSTRUMENTATION) {
            LOGGER.log(Level.FINER, "CLASS_INSTRUMENTATION Strategy.");
            ((ITransparentDirtyDetector) this.___proxiedObject).___ogm___setDirty(false);
        }
    }


    @Override
    public synchronized void ___commit() {
        //ante ciertas condiciones los métodos de Orient pueden lanzar una excepción
        //que hay que atraparlas en algún momento
        try {
            doCommit();
        } catch (OException ex) {
        	throw ex;
            //throw new OdbogmException(ex, ___transaction);
        }
    }
    
    
    private void doCommit() {
        LOGGER.log(Level.FINER, "Iniciando ___commit() ....");
        LOGGER.log(Level.FINER, "valid: {0}", this.___isValidObject);
        LOGGER.log(Level.FINER, "dirty: {0}", this.___dirty);

        if (this.___dirty || this.___baseElement.getIdentity().isNew()) {
            this.___transaction.initInternalTx();

            // make sure you are attached
            // may be we do not need this.
            /*if (this.___baseElement.getGraph() == null) {
                LOGGER.log(Level.FINER, "El objeto no está atachado!");
                this.___transaction.attach(this.___baseElement);
            }*/
            //this.___baseElement.save();

            // get the class definition
            ClassDef cDef = this.___transaction.getObjectMapper().getClassDef(this.___proxiedObject);

            // get an updated map of the contained object
            ObjectStruct oStruct = this.___transaction.getObjectMapper().objectStruct(this.___proxiedObject);
            Map<String, Object> omap = oStruct.fields;

            // add everything to the vertex
            // trying to cast to oelement to orientelement to use set properties as is.
            for(Map.Entry<String, Object> mapEntry: omap.entrySet()) {
            	this.___baseElement.setProperty(mapEntry.getKey(), mapEntry.getValue());
            }
            //((OElement)this.___baseElement).setProperties(omap);
            oStruct.removedProperties.forEach(prop -> this.___baseElement.removeProperty(prop));

            // save audit log if applicable.
            /*if (this.___transaction.isAuditing() && !this.___baseElement.getIdentity().isNew()) {
                this.___transaction.auditLog(this, AuditType.WRITE, "UPDATE", omap);
            }*/

            // if it is a vertex
            if (this.___baseElement instanceof OVertex) {
            	OVertex ov = (OVertex) this.___baseElement;
                // Analyze if the vertices changed
                /*
                 * Process internal objects. First it must be determined
                 * if the objects already existed in the current context. If not
                 * They exist must be created.
                 */
                for (Map.Entry<String, Class<?>> link : cDef.links.entrySet()) {
                    String field = link.getKey();
                    String graphRelationName = cDef.entityName + "_" + field;
                    // determine field status
                    if (oStruct.links.get(field) == null) {
                        // if it is null, the object may have been deleted
                        // therefore the corresponding vertex should be removed
                        // if it exists
                    	Iterable<OEdge> edges = ov.getEdges(ODirection.OUT,graphRelationName);
                    	for(OEdge edge: ov.getEdges(ODirection.OUT, graphRelationName)) {
                    		this.removeEdge(edge, field);
                    	}
                        /*if (ov.countEdges(Direction.OUT, graphRelationName) > 0) {
                            // se ha eliminado el objeto y debe ser removido el Vértice o el Edge correspondiente
                            OrientEdge removeEdge = null;
                            for (Edge edge : ov.getEdges(Direction.OUT, graphRelationName)) {
                                removeEdge = (OrientEdge) edge;

                                if (this.___transaction.isAuditing()) {
                                    this.___transaction.auditLog(this, AuditType.WRITE, "REMOVE LINK: " + graphRelationName, removeEdge);
                                }
                                this.removeEdge(removeEdge, field);
                            }
                        }*/
                    } else {
                        Object innerO = oStruct.links.get(field);
                        // Check if it is already in context. If it was created fit
                        // separated and associated with the main object, it may be the case
                        // that the main object has RID and the aggregate does not.
                        if (innerO instanceof IObjectProxy) {
                            // The object existed.
                            // It should be checked if the axis between the two objects already existed.
                            if (!VertexUtils.isConectedTo(ov, ((IObjectProxy) innerO).___getVertex(), graphRelationName)) {
                                // There is no axis. It must be created
                                LOGGER.log(Level.FINER, "Los objetos no están conectados. ({0} |--|{1}",
                                        new Object[]{ov.getIdentity(), ((IObjectProxy) innerO).___getVertex().getIdentity()});
                                
                                // First check if there was no previous relationship with another object to remove it.
                                Iterable<OEdge> edges = ov.getEdges(ODirection.OUT,graphRelationName);
                            	for(OEdge edge: ov.getEdges(ODirection.OUT, graphRelationName)) {
                            		this.removeEdge(edge, field);
                            	}
                                /*if (ov.countEdges(Direction.OUT, graphRelationName) > 0) {
                                    LOGGER.log(Level.FINER, "Existía una relación previa. Se debe eliminar.");
                                    // existé una relación. Elimnarla antes de proceder a establecer la nueva.
                                    OrientEdge removeEdge = null;
                                    for (Edge edge : ov.getEdges(Direction.OUT, graphRelationName)) {
                                        removeEdge = (OrientEdge) edge;
                                        LOGGER.log(Level.FINER, "Eliminar relación previa a " + removeEdge.getInVertex());

                                        if (this.___transaction.isAuditing()) {
                                            this.___transaction.auditLog(this, AuditType.WRITE, "REMOVE LINK: " + graphRelationName, removeEdge);
                                        }

                                        this.removeEdge(removeEdge, field);
                                    }
                                }*/
                                LOGGER.log(Level.FINER, "Agregar un link entre dos objetos existentes.");
                                //OrientEdge oe = this.___transaction.getSessionManager().getGraphdb().addEdge("class:" + graphRelationName, ov, ((IObjectProxy) innerO).___getVertex(), graphRelationName);
                                OEdge oe = this.___transaction.getSessionManager().getTransaction().getODatabaseSession().newEdge(ov, ((IObjectProxy) innerO).___getVertex(), graphRelationName);
                            }
                        } else {
                            // the object is new
                            // First check if there was no previous relationship with another object to remove it.
                        	Iterable<OEdge> edges = ov.getEdges(ODirection.OUT,graphRelationName);
                        	for(OEdge edge: ov.getEdges(ODirection.OUT, graphRelationName)) {
                        		this.removeEdge(edge, field);
                        	}
                            /*if (ov.countEdges(Direction.OUT, graphRelationName) > 0) {
                                LOGGER.log(Level.FINER, "Existía una relación previa. Se debe eliminar.");
                                // existé una relación. Elimnarla antes de proceder a establecer la nueva.
                                OrientEdge removeEdge = null;
                                for (Edge edge : ov.getEdges(Direction.OUT, graphRelationName)) {
                                    removeEdge = (OrientEdge) edge;
                                    LOGGER.log(Level.FINER, "Eliminar relación previa a " + removeEdge.getOutVertex());
                                    if (this.___transaction.isAuditing()) {
                                        this.___transaction.auditLog(this, AuditType.WRITE, "REMOVE LINK: " + graphRelationName, removeEdge);
                                    }
                                    this.removeEdge(removeEdge, field);
                                }
                            }*/

                            // create the new relationship
                            LOGGER.log(Level.FINER, "innerO nuevo. Crear un vértice y un link");
                            innerO = this.___transaction.store(innerO);
                            this.___transaction.getObjectMapper().setFieldValue(this.___proxiedObject, field, innerO);

                            // if class instrumentation is active, uncheck the object as dirty
                            if (innerO instanceof ITransparentDirtyDetector) {
                                ((ITransparentDirtyDetector) innerO).___ogm___setDirty(false);
                            }

                            OEdge oe = this.___transaction.getODatabaseSession().newEdge(ov, ((IObjectProxy) innerO).___getVertex(), graphRelationName);
                        }
                    }
                }

                /**
                 * Procesar los linklists.
                 */
                Field f;
                for (Map.Entry<String, Class<?>> entry : cDef.linkLists.entrySet()) {
                    try {
                        String field = entry.getKey();
                        LOGGER.log(Level.FINER, "procesando campo: {0} clase: {1}",
                                new Object[]{field, this.___proxiedObject.getClass()});

                        f = cDef.fieldsObject.get(field);

                        // preparar el nombre de la relación
                        final String graphRelationName = cDef.entityName + "_" + field;

                        Object collectionFieldValue = f.get(this.___proxiedObject);

                        // verificar si existe algún cambio en la colecciones
                        // ingresa si la colección es distinta de null y
                        // collectionFieldValue es instancia de ILazyCalls y está marcado como dirty
                        // o collectionFieldValue no es instancia de ILazyCalls, lo que 
                        // significa que es una colección nueva y debe ser procesada completamente.
                        if ((collectionFieldValue != null)
                                && ((ILazyCalls.class.isAssignableFrom(collectionFieldValue.getClass()) && ((ILazyCalls) collectionFieldValue).isDirty())
                                || (!ILazyCalls.class.isAssignableFrom(collectionFieldValue.getClass())))) {
                            LOGGER.log(Level.FINER, (!ILazyCalls.class.isAssignableFrom(collectionFieldValue.getClass()))
                                    ? "No es instancia de ILazyCalls"
                                    : "Es instancia de Lazy y está marcado como DIRTY");

                            if (collectionFieldValue instanceof List) {
                                ILazyCollectionCalls lazyCollectionCalls;
                                // procesar la colección

                                if (ILazyCollectionCalls.class.isAssignableFrom(
                                        collectionFieldValue.getClass())) {
                                    lazyCollectionCalls = (ILazyCollectionCalls) collectionFieldValue;
                                } else {
                                    // se ha asignado una colección original y se debe exportar todo
                                    this.___transaction.getObjectMapper().colecctionToLazy(
                                            this.___proxiedObject, field, ov, this.___transaction);

                                    //recuperar la nueva colección
                                    Collection inter = (Collection) f.get(this.___proxiedObject);

                                    //agregar todos los valores que existían
                                    inter.addAll((Collection) collectionFieldValue);
                                    //preparar la interface para que se continúe con el acceso.
                                    lazyCollectionCalls = (ILazyCollectionCalls) inter;
                                    // reasignar el objeto oCol
                                    collectionFieldValue = f.get(this.___proxiedObject);
                                }

                                List listFieldValue = (List) collectionFieldValue;
                                Map<Object, ObjectCollectionState> colState = lazyCollectionCalls.collectionState();

                                // procesar los elementos presentes en la colección
                                for (int i = 0; i < listFieldValue.size(); i++) {
                                    Object colObject = listFieldValue.get(i);
                                    // verificar el estado del objeto en la colección
                                    if (colState.get(colObject) == ObjectCollectionState.ADDED) {
                                        // si se agregó uno, determinar si era o no manejado por el SM
                                        if (!(colObject instanceof IObjectProxy)) {
                                            LOGGER.log(Level.FINER, "Objeto nuevo. Insertando en la base y reemplazando el original...");
                                            // no es un objeto que se haya almacenado.
                                            colObject = this.___transaction.store(colObject);
                                            // reemplazar en la colección el objeto por uno administrado
                                            listFieldValue.set(i, colObject);

                                            // si está activa la instrumentación de clases, desmarcar el objeto como dirty
                                            if (colObject instanceof ITransparentDirtyDetector) {
                                                ((ITransparentDirtyDetector) colObject).___ogm___setDirty(false);
                                            }

                                        }

                                        // vincular el nodo
                                        OEdge oe = this.___transaction.getODatabaseSession().newEdge( this.___getVertex(), ((IObjectProxy) colObject).___getVertex(), graphRelationName);
                                    }
                                }

                                // process removals only if the annotation is in the field
                                for (Map.Entry<Object, ObjectCollectionState> entry1 : colState.entrySet()) {
                                    Object colObject = entry1.getKey();
                                    ObjectCollectionState colObjState = entry1.getValue();

                                    if (colObjState == ObjectCollectionState.REMOVED) {
                                        // remover el link
                                    	Iterable<OEdge> edges = ((OVertex) this.___baseElement).getEdges(ODirection.OUT, graphRelationName);
                                    	
                                    	for(OEdge edge: edges) {
                                    		if(edge.getTo().equals(((IObjectProxy) colObject).___getVertex())) {
                                    			edge.detach();
                                    		}
                                    	}
                                    	
                                    	
                                        /*for (Edge edge : ((OVertex) this.___baseElement)
                                                .getEdges(((IObjectProxy) colObject).___getVertex(),
                                                        Direction.OUT,
                                                        graphRelationName)) {
                                            if (this.___transaction.isAuditing()) {
                                                this.___transaction.auditLog(this, AuditType.WRITE, "LINKLIST REMOVE: " + graphRelationName, edge);
                                            }
                                            edge.remove();
                                        }*/
                                        // si existe la anotación, remover tambien el vertex
                                        if (f.isAnnotationPresent(RemoveOrphan.class)) {
                                            this.___transaction.delete(colObject);
                                        }
                                    }
                                }

                                // resetear el estado
                                lazyCollectionCalls.clearState();

                            } else if (collectionFieldValue instanceof Map) {

                                Map mapFieldValue;
                                // procesar la colección

                                if (ILazyMapCalls.class.isAssignableFrom(collectionFieldValue.getClass())) {
                                    mapFieldValue = (Map) collectionFieldValue;
                                } else {
                                    // se ha asignado una colección original y se debe exportar todo
                                    // this.sm.getObjectMapper().colecctionToLazy(this.realObj, field, ov);
                                    this.___transaction.getObjectMapper().colecctionToLazy(this.___proxiedObject, field, ov, this.___transaction);
                                    //recuperar la nueva colección
                                    // Collection inter = (Collection) f.get(this.realObj);
                                    Map inter = (Map) f.get(this.___proxiedObject);
                                    //agregar todos los valores que existían
                                    inter.putAll((Map) collectionFieldValue);
                                    //preparar la interface para que se continúe con el acceso.
                                    mapFieldValue = (Map) inter;
                                }

                                // refrescar los estados
                                final Map<Object, ObjectCollectionState> keysState = ((ILazyMapCalls) mapFieldValue).collectionState();
                                final Map<Object, OEdge> keysToEdges = ((ILazyMapCalls) mapFieldValue).getKeyToEdge();
                                final Map<Object, ObjectCollectionState> entitiesState = ((ILazyMapCalls) mapFieldValue).getEntitiesState();

                                // recorrer todas las claves del mapa
                                for (Map.Entry<Object, ObjectCollectionState> entry1 : keysState.entrySet()) {
                                    Object key = entry1.getKey();
                                    ObjectCollectionState keyState = entry1.getValue();

                                    LOGGER.log(Level.FINER, "imk: {0} state: {1}", new Object[]{key, keyState});
                                    // para cada entrada, verificar la existencia del objeto y crear un Edge.
                                    Object linkedO = mapFieldValue.get(key);

                                    if (keyState != ObjectCollectionState.REMOVED &&
                                            !(linkedO instanceof IObjectProxy)) {
                                        LOGGER.log(Level.FINER, "Link Map Object nuevo. Crear un vértice y un link");
                                        linkedO = this.___transaction.store(linkedO);
                                        mapFieldValue.replace(key, linkedO);
                                        if (linkedO instanceof ITransparentDirtyDetector) {
                                            ((ITransparentDirtyDetector) linkedO).___ogm___setDirty(false);
                                        }
                                    }

                                    OEdge oe;
                                    // check the status of the object in the collection.
                                    switch (keyState) {
                                        case ADDED:
                                            // Create a link between the two objects.
                                            LOGGER.log(Level.FINER, "-----> agregando un LinkList al Map!");
                                            oe = this.___transaction.getODatabaseSession().newEdge(
                                                    (OVertex) this.___baseElement, ((IObjectProxy) linkedO).___getVertex(),
                                                    graphRelationName);
                                            // update the edge with the key data.
										Map<String, Object> simpleMap = this.___transaction.getObjectMapper().simpleMap(key);
										for(Map.Entry<String, Object> mapEntry: simpleMap.entrySet()) {
											oe.setProperty(mapEntry.getKey(), mapEntry.getValue());
										}
										//oe.setProperties(simpleMap);

                                            break;

                                        case NOCHANGE:
                                            // el link no se ha modificado. 
                                            break;

                                        case REMOVED:
                                            // quitar el Edge
                                            OEdge oeRemove = keysToEdges.get(key);
                                            if (oeRemove == null) {
                                                throw new IllegalStateException("The edge object couldn't be found. "
                                                        + "Make sure its hashCode is change-proof.");
                                            }
                                            oeRemove.detach();
                                            // The link has been removed. Delete and verify if applicable delete
                                            // the vertex if it is marked with @RemoveOrphan.
                                            if (f.isAnnotationPresent(RemoveOrphan.class)) {
                                                if (entitiesState.get(key) == ObjectCollectionState.REMOVED) {
                                                    this.___transaction.delete(entitiesState.get(key));
                                                }
                                            }
                                            break;
                                    }
                                }
                                ((ILazyMapCalls) mapFieldValue).clearState();
                            } else {
                                LOGGER.log(Level.FINER, "********************************************");
                                LOGGER.log(Level.FINER, "field: {0}", field);
                                LOGGER.log(Level.FINER, "********************************************");
                                throw new CollectionNotSupported(collectionFieldValue.getClass().getSimpleName());
                            }
                        }
                    } catch (IllegalArgumentException | IllegalAccessException ex) {
                        Logger.getLogger(SessionManager.class
                                .getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
            //moving the save here. Why not
            //this.___baseElement.save();
            //this.___transaction.getODatabaseSession().getTransaction().
            this.___transaction.closeInternalTx();
        }
        LOGGER.log(Level.FINER, "fin commit ----");
    }
    

    /**
     * Refresca el objeto base recuperándolo nuevamente desde la base de datos.
     */
    @Override
    public void ___reload() {
        this.___transaction.initInternalTx();

        this.___baseElement.reload();

        this.___transaction.closeInternalTx();
    }


    /**
     * Función de uso interno para remover un eje
     *
     * @param edgeToRemove
     * @param field
     */
    private synchronized void removeEdge(OEdge edgeToRemove, String field) {
        try {
            ClassDef classdef = this.___transaction.getObjectMapper().getClassDef(___baseClass);
            Field f = classdef.fieldsObject.get(field);
//            Field f = ReflectionUtils.findField(this.___baseClass, field);

            // In the Edge, IN comes from the targeted object. Rare but it is like that :(
            
            //String outRid = edgeToRemove.getInVertex().getIdentity().toString();
            String outRid = edgeToRemove.getFrom().getIdentity().toString();
            LOGGER.log(Level.FINER, "El edge {0} apunta IN: {1} apunta OUT: {2}",
                    new Object[]{edgeToRemove,
                        edgeToRemove.getFrom().getIdentity().toString(),
                        edgeToRemove.getTo().getIdentity().toString()});
            // first remove the shaft
            //edgeToRemove.remove();
            boolean detach = edgeToRemove.detach();
            LOGGER.log(Level.INFO, "edge of id {0} is detached = {1}" , new Object[]{edgeToRemove.getIdentity().toString(),detach
            		});
            

            // if appropriate
            if (f.isAnnotationPresent(RemoveOrphan.class)) {
                LOGGER.log(Level.FINER, "Remove orphan presente");
                //auditar
                // eliminar el objecto
                // this.sm.delete(f.get(realObj));
                if (f.get(this.___proxiedObject) != null) {
                    LOGGER.log(Level.FINER, "La referencia aún existe. Eliminar el objeto directamente");
                    this.___transaction.delete(f.get(this.___proxiedObject));
                } else {
                    LOGGER.log(Level.FINER, "la referencia estaba en null, recupear y eliminar el objeto.");
                    this.___transaction.delete(this.___transaction.get(outRid));
                }
            }

        } catch (SecurityException | IllegalArgumentException | IllegalAccessException ex) {
            Logger.getLogger(ObjectProxy.class.getName()).log(Level.SEVERE, null, ex);
        }
    }


    /**
     * Revierte el objeto al estado que tiene el Vertex original.
     */
    @Override
    public synchronized void ___rollback() {
        LOGGER.log(Level.FINER, "\n\n******************* ROLLBACK *******************\n\n");
        LOGGER.log(Level.FINER, ThreadHelper.getCurrentStackTrace());

        this.___transaction.initInternalTx();

        // si es un objeto nuevo
        boolean isNew = this.___baseElement.getIdentity().isNew();
        LOGGER.log(Level.FINER, "RID: {0} Nueva?: {1}", new Object[]{this.___baseElement.getIdentity().toString(), isNew});
        if (isNew) {
            // invalidar el objeto
            LOGGER.log(Level.FINER, "El objeto aún no se ha persistido en la base. Invalidar");
            this.___isValidObject = false;
            return;
        }
        // asegurarse que la marca de borrado sea eliminada.
        this.___deletedMark = false;

        // recargar todo.
        this.___baseElement.reload();

        LOGGER.log(Level.FINER, "vmap: {0}", this.___baseElement.getPropertyNames());
        // restaurar los atributos al estado original.
        ObjectMapper objectMapper = this.___transaction.getObjectMapper();
        ClassDef classdef = objectMapper.getClassDef(this.___proxiedObject);
        
        
        LOGGER.log(Level.FINER, "Reverting basic attributes.........");
        for (Map.Entry<String, Class<?>> entry : classdef.fields.entrySet()) {
            String prop = entry.getKey();
            if (!classdef.embeddedFields.containsKey(prop)) {
                LOGGER.log(Level.FINER, "Rollingback field {0} ....", new String[]{prop});
                Object value = this.___baseElement.getProperty(prop);
                objectMapper.setFieldValue(___proxiedObject, prop, value);
            }
        }
        

        LOGGER.log(Level.FINER, "Reverting embedded collections.........");
        this.___transaction.getObjectMapper().hydrateEmbeddedCollections(
                classdef, (IObjectProxy)this.___proxiedObject, this.___baseElement);
        

        // procesar los enum
        Field f;
        LOGGER.log(Level.FINER, "Reverting enums...");
        for (Map.Entry<String, Class<?>> entry : classdef.enumFields.entrySet()) {
            String prop = entry.getKey();
            LOGGER.log(Level.FINER, "Buscando campo {0} ....", new String[]{prop});
            Object value = this.___baseElement.getProperty(prop);
            try {
                f = classdef.fieldsObject.get(prop);
                if (value != null) {
                    f.set(this.___proxiedObject, Enum.valueOf(f.getType().asSubclass(Enum.class), value.toString()));
                } else {
                    f.set(this.___proxiedObject, null);
                }
                LOGGER.log(Level.FINER, "hidratado campo: {0}={1}", new Object[]{prop, value});
            } catch (IllegalArgumentException | IllegalAccessException ex) {
                Logger.getLogger(ObjectProxy.class.getName()).log(Level.SEVERE, null, ex);
            }
        }


        
        LOGGER.log(Level.FINER, "Reverting enum collections.........");
        this.___transaction.getObjectMapper().hydrateEnumCollections(
                classdef, (IObjectProxy)this.___proxiedObject, this.___baseElement);
        

        
        LOGGER.log(Level.FINER, "Revirtiendo los Links......... ");
        // hidratar los atributos @links
        // procesar todos los links
        for (Map.Entry<String, Class<?>> entry : classdef.links.entrySet()) {
            try {
                String field = entry.getKey();
                Field fLink = classdef.fieldsObject.get(field);
                fLink.set(this.___proxiedObject, null);
            } catch (SecurityException | IllegalArgumentException | IllegalAccessException ex) {
                Logger.getLogger(ObjectMapper.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        // volver a activar la carga de los links
        this.___loadLazyLinks = true;

        // revertir las colecciones
        // procesar todos los linkslist
        LOGGER.log(Level.FINER, "Revirtiendo las colecciones...");
        for (Map.Entry<String, Class<?>> entry : classdef.linkLists.entrySet()) {
            try {
                // FIXME: se debería considerar agregar una annotation EAGER!
                String field = entry.getKey();
                Class<?> fc = entry.getValue();
                LOGGER.log(Level.FINER, "Field: {0}   Class: {1}", new String[]{field, fc.getName()});
                Field fLink = classdef.fieldsObject.get(field);
                ILazyCalls lc = (ILazyCalls) fLink.get(___proxiedObject);
                if (lc != null) {
                    lc.rollback();
                }
            } catch (IllegalArgumentException ex) {
                Logger.getLogger(ObjectMapper.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IllegalAccessException ex) {
                Logger.getLogger(ObjectProxy.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        this.___removeDirtyMark();
        this.___transaction.closeInternalTx();
    }

}
