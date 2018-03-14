/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.odbogm.proxy;

import net.odbogm.Primitives;
import net.odbogm.SessionManager;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.odbogm.LogginProperties;
import net.odbogm.Transaction;

/**
 *
 * @author Marcelo D. Ré {@literal <marcelo.re@gmail.com>}
 */
public class HashMapLazyProxy extends HashMap<Object, Object> implements ILazyMapCalls {

    private final static Logger LOGGER = Logger.getLogger(HashMapLazyProxy.class.getName());
    static {
        if (LOGGER.getLevel() == null) {
            LOGGER.setLevel(LogginProperties.HashMapLazyProxy);
        }
    }
    
    private boolean dirty = false;
    
    private boolean lazyLoad = true;
    private boolean lazyLoading = false;
    
    private Transaction transaction;
    private OrientVertex relatedTo;
    private String field;
    private Class<?> keyClass;
    private Class<?> valueClass;

    // referencia debil al objeto padre. Se usa para notificar al padre que la colección ha cambiado.
    private WeakReference<IObjectProxy> parent;
    
    /**
     * Crea un ArrayList lazy.
     *
     * @param t Vínculo a la transacción actual
     * @param relatedTo: Vértice con el cual se relaciona la colección
     * @param field: atributo de relación
     * @param k: clase del key.
     * @param v: clase del value.
     */
    @Override
    public void init(Transaction t, OrientVertex relatedTo, IObjectProxy parent, String field, Class<?> k, Class<?> v) {
        this.transaction = t;
        this.relatedTo = relatedTo;
        this.parent = new WeakReference<>(parent);
        this.field = field;
        this.keyClass = k;
        this.valueClass = v;
    }

    //********************* change control **************************************
    private Map<Object, ObjectCollectionState> entitiesState = new ConcurrentHashMap<>();
    private Map<Object, ObjectCollectionState> keyState = new ConcurrentHashMap<>();
    private Map<Object, OrientEdge> keyToEdge = new ConcurrentHashMap<>();

    private void lazyLoad() {
//        LOGGER.log(Level.FINER, "Lazy Load.....");
        this.lazyLoad = false;
        this.lazyLoading = true;

        // recuperar todos los elementos desde el vértice y agregarlos a la colección
        for (Iterator<Vertex> iterator = relatedTo.getVertices(Direction.OUT, field).iterator(); iterator.hasNext();) {
            OrientVertex next = (OrientVertex) iterator.next();
//            LOGGER.log(Level.FINER, "loading: " + next.getId().toString());

            Object o = transaction.get(valueClass, next.getId().toString());

            // para cada vértice conectado, es necesario mapear todos los Edges que los unen.
            for (Edge edge : relatedTo.getEdges(next, Direction.OUT, field)) {
                OrientEdge oe = (OrientEdge) edge;
                Object k = null;
                LOGGER.log(Level.FINER, "edge keyclass: "+this.keyClass+"  OE RID:"+oe.getId().toString());
                // si el keyClass no es de tipo nativo, hidratar un objeto.
                if (Primitives.PRIMITIVE_MAP.containsKey(this.keyClass)) {
                    LOGGER.log(Level.FINER, "primitive!!");
                    for (String prop : oe.getPropertyKeys()) {
                        k = oe.getProperty(prop);
                    }
                } else {
                    LOGGER.log(Level.FINER, "clase como key");
                    k = transaction.getEdgeAsObject(keyClass, oe);
                }
                this.put(k, o);
                this.keyState.put(k, ObjectCollectionState.REMOVED);
                this.keyToEdge.put(k, oe);
            }

            // como puede estar varias veces un objecto agregado al map con distintos keys
            // primero verificamos su existencia para no duplicarlos.
            if (this.entitiesState.get(o) == null) {
                // se asume que todos fueron borrados
                this.entitiesState.put(o, ObjectCollectionState.REMOVED);
            }
        }
        this.lazyLoading = false;
    }

    /**
     * Vuelve  establecer el punto de verificación.
     */
    public void clearState() {
        this.entitiesState.clear();
        this.keyState.clear();
        Map<Object, OrientEdge> newOE  = new ConcurrentHashMap<>();
        
        for (Entry<Object, Object> entry : this.entrySet()) {
            Object k = entry.getKey();
            Object o = entry.getValue();
            
            this.keyState.put(k, ObjectCollectionState.REMOVED);
            
            // verificar si existe una relación con en Edge
            if (this.keyToEdge.get(k)!=null)
                newOE.put(k,this.keyToEdge.get(k));
            
            // como puede estar varias veces un objecto agregado al map con distintos keys
            // primero verificamos su existencia para no duplicarlos.
            if (this.entitiesState.get(o) == null) {
                // se asume que todos fueron borrados
                this.entitiesState.put(o, ObjectCollectionState.REMOVED);
            }
            
        }
        this.keyToEdge = newOE;
        this.dirty = false;
    }
    
    /**
     * Actualiza el estado de todo el MAP y devuelve la referencia al estado de los keys
     * 
     * @return  retorna un mapa con el estado de la colección
     */
    @Override
    public Map<Object, ObjectCollectionState> collectionState() {
        for (Entry<Object, Object> entry : this.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();

            // actualizar el estado de la clave
            if (this.keyState.get(key) == null) {
                // se agregó un objeto
                this.keyState.put(key, ObjectCollectionState.ADDED);
            } else {
                // el objeto existe. Marcarlo como sin cambio para la colección
                this.keyState.replace(key, ObjectCollectionState.NOCHANGE);
            }
            
            // actualizar el estado del valor
            if (this.entitiesState.get(value) == null ) {
                // se agregó un objeto
                this.entitiesState.put(value, ObjectCollectionState.ADDED);
            } else {
                // el objeto existe. Marcarlo como sin cambio para la colección
                this.entitiesState.replace(value, ObjectCollectionState.NOCHANGE);
            }
        }
        return this.keyState;
    }

    public Map<Object, ObjectCollectionState> getEntitiesState() {
        return entitiesState;
    }

    public Map<Object, ObjectCollectionState> getKeyState() {
        return keyState;
    }

    public Map<Object, OrientEdge> getKeyToEdge() {
        return keyToEdge;
    }
    
    private void setDirty() {
        LOGGER.log(Level.FINER, "Colección marcada como Dirty. Avisar al padre.");
        this.dirty = true;
        LOGGER.log(Level.FINER, "weak:"+this.parent.get());
        // si el padre no está marcado como garbage, notificarle el cambio de la colección.
        if (this.parent.get()!=null)
            this.parent.get().___setDirty();
    }
    
    @Override
    public boolean isDirty() {
        return this.dirty;
    }
    
    @Override
    public void rollback() {
        //FIXME: Analizar si se puede implementar una versión que no borre todos los elementos
        this.clear();
        this.entitiesState.clear();
        this.keyToEdge.clear();
        this.keyState.clear();
        this.dirty = false;
        this.lazyLoad = true;
    }
    
    //====================================================================================

    /**
     * Crea un map utilizando los atributos del Edge como key. Si se utiliza un objeto para representar los atributos, se debe declarar en el
     * annotation.
     */
    public HashMapLazyProxy() {
        super();
    }

    public HashMapLazyProxy(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    public HashMapLazyProxy(int initialCapacity) {
        super(initialCapacity);
    }

    @Override
    public Object clone() {
        if (lazyLoad) {
            this.lazyLoad();
        }

        return super.clone(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void replaceAll(BiFunction<? super Object, ? super Object, ? extends Object> function) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        if(!this.lazyLoading) this.setDirty();
        super.replaceAll(function); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void forEach(BiConsumer<? super Object, ? super Object> action) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        super.forEach(action); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object merge(Object key, Object value, BiFunction<? super Object, ? super Object, ? extends Object> remappingFunction) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        if(!this.lazyLoading) this.setDirty();
        return super.merge(key, value, remappingFunction); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object compute(Object key, BiFunction<? super Object, ? super Object, ? extends Object> remappingFunction) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.compute(key, remappingFunction); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object computeIfPresent(Object key, BiFunction<? super Object, ? super Object, ? extends Object> remappingFunction) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.computeIfPresent(key, remappingFunction); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object computeIfAbsent(Object key, Function<? super Object, ? extends Object> mappingFunction) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.computeIfAbsent(key, mappingFunction); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object replace(Object key, Object value) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        if(!this.lazyLoading) this.setDirty();
        return super.replace(key, value); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean replace(Object key, Object oldValue, Object newValue) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        if(!this.lazyLoading) this.setDirty();
        return super.replace(key, oldValue, newValue); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean remove(Object key, Object value) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        if(!this.lazyLoading) this.setDirty();
        return super.remove(key, value); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object putIfAbsent(Object key, Object value) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        Object res = super.putIfAbsent(key, value); //To change body of generated methods, choose Tools | Templates.
        if (res!=null)
            this.setDirty();
        return res;
    }

    @Override
    public Object getOrDefault(Object key, Object defaultValue) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.getOrDefault(key, defaultValue); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Set<Entry<Object, Object>> entrySet() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.entrySet(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Collection<Object> values() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.values(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Set<Object> keySet() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.keySet(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean containsValue(Object value) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.containsValue(value); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void clear() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        this.setDirty();
        super.clear(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object remove(Object key) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        this.setDirty();
        return super.remove(key); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void putAll(Map<? extends Object, ? extends Object> m) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        this.setDirty();
        super.putAll(m); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object put(Object key, Object value) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        this.setDirty();
        return super.put(key, value); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean containsKey(Object key) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.containsKey(key); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object get(Object key) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.get(key); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isEmpty() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.isEmpty(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int size() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.size(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String toString() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.toString(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int hashCode() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.hashCode(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean equals(Object o) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.equals(o); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void finalize() throws Throwable {
        if (lazyLoad) {
            this.lazyLoad();
        }
        super.finalize(); //To change body of generated methods, choose Tools | Templates.
    }

}
