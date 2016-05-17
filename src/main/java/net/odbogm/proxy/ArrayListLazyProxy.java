/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.odbogm.proxy;

import net.odbogm.SessionManager;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 *
 * @author SShadow
 */
public class ArrayListLazyProxy extends ArrayList implements ILazyCollectionCalls {

    private final static Logger LOGGER = Logger.getLogger(ArrayListLazyProxy.class.getName());
    private static final long serialVersionUID = 923118982357962428L;

    private boolean dirty = false;
    private boolean lazyLoad = true;
    private SessionManager sm;
    private OrientVertex relatedTo;
    private String field;
    private Class<?> fieldClass;

    /**
     * Crea un ArrayList lazy.
     *
     * @param sm Vínculo al SessionManager actual
     * @param relatedTo: Vértice con el cual se relaciona la colección
     * @param field: atributo de relación
     * @param c: clase genérica de la colección.
     */
    @Override
    public void init(SessionManager sm, OrientVertex relatedTo, String field, Class<?> c) {
        this.sm = sm;
        this.relatedTo = relatedTo;
        this.field = field;
        this.fieldClass = c;
    }

    //********************* change control **************************************
    private Map<Object, ObjectCollectionState> listState = new ConcurrentHashMap<>();
    
    private void lazyLoad() {
//        LOGGER.log(Level.INFO, "Lazy Load.....");
        this.lazyLoad = false;

        // recuperar todos los elementos desde el vértice y agregarlos a la colección
        for (Iterator<Vertex> iterator = relatedTo.getVertices(Direction.OUT, field).iterator(); iterator.hasNext();) {
            OrientVertex next = (OrientVertex) iterator.next();
//            LOGGER.log(Level.INFO, "loading: " + next.getId().toString());
            Object o = sm.get(fieldClass, next.getId().toString());
            this.add(o);
            // se asume que todos fueron borrados
            this.listState.put(o, ObjectCollectionState.REMOVED);
        }
    }

    public Map<Object, ObjectCollectionState> collectionState() {
        // si se ha hecho referencia al contenido de la colección, realizar la verificación
        if (!this.lazyLoad) {
            for (Object o : this) {
                // actualizar el estado
                if (this.listState.get(o) == null) {
                    // se agregó un objeto
                    this.listState.put(o, ObjectCollectionState.ADDED);
                } else {
                    // el objeto existe. Removerlo para que solo queden los que se agregaron o eliminaron 
                    this.listState.remove(o);
                    // el objeto existe. Marcarlo como sin cambio para la colección
//                    this.listState.replace(o, ObjectCollectionState.NOCHANGE);
                }
            }
        }
        return this.listState;
    }
    
    /**
     * Vuelve establecer el punto de verificación.
     */
    @Override
    public void clearState() {
        this.dirty = false;
        
        this.listState.clear();

        for (Object o : this) {
            if (this.listState.get(o) == null) {
                // se asume que todos fueron borrados
                this.listState.put(o, ObjectCollectionState.REMOVED);
            }
        }
    }
    

    @Override
    public boolean isDirty() {
        return this.dirty;
    }
    
    //====================================================================================

    public ArrayListLazyProxy() {
        super();
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
    public String toString() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.toString(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean containsAll(Collection c) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.containsAll(c); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream parallelStream() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.parallelStream(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream stream() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.stream(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void finalize() throws Throwable {
        if (lazyLoad) {
            this.lazyLoad();
        }
        super.finalize(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void sort(Comparator c) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        super.sort(c);
    }

    @Override
    public void replaceAll(UnaryOperator operator) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        this.dirty = true;
        super.replaceAll(operator);
    }

    @Override
    public boolean removeIf(Predicate filter) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        boolean removed = super.removeIf(filter);
        if (removed)
            this.dirty = true;
        return removed;
    }

    @Override
    public Spliterator spliterator() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.spliterator();
    }

    @Override
    public void forEach(Consumer action) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        super.forEach(action);
    }

    @Override
    public List subList(int fromIndex, int toIndex) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.subList(fromIndex, toIndex);
    }

    @Override
    public Iterator iterator() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.iterator();
    }

    @Override
    public ListIterator listIterator() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.listIterator();
    }

    @Override
    public ListIterator listIterator(int index) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.listIterator(index);
    }

    @Override
    public boolean retainAll(Collection c) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        boolean changeDetected = super.retainAll(c);
        if (changeDetected)
            this.dirty = true;
        return changeDetected;
    }

    @Override
    public boolean removeAll(Collection c) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        boolean changeDetected = super.removeAll(c);
        if (changeDetected)
            this.dirty = true;
        return changeDetected;
    }

    @Override
    protected void removeRange(int fromIndex, int toIndex) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        this.dirty = true;
        super.removeRange(fromIndex, toIndex);
    }

    @Override
    public boolean addAll(int index, Collection c) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        this.dirty = true;
        return super.addAll(index, c);
    }

    @Override
    public boolean addAll(Collection c) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        this.dirty = true;
        return super.addAll(c);
    }

    @Override
    public void clear() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        this.dirty = true;
        super.clear();
    }

    @Override
    public boolean remove(Object o) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        
        boolean changeDetected = super.remove(o);
        if (changeDetected)
            this.dirty = true;
        return changeDetected;
    }

    @Override
    public Object remove(int index) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        this.dirty = true;
        return super.remove(index);
    }

    @Override
    public void add(int index, Object element) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        this.dirty=true;
        super.add(index, element);
    }

    @Override
    public boolean add(Object e) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        this.dirty=true;
        return super.add(e);
    }

    @Override
    public Object set(int index, Object element) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        this.dirty = true;
        return super.set(index, element);
    }

    @Override
    public Object get(int index) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.get(index);
    }

    @Override
    public Object[] toArray(Object[] a) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.toArray(a);
    }

    @Override
    public Object[] toArray() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.toArray();
    }

    @Override
    public Object clone() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.clone();
    }

    @Override
    public int lastIndexOf(Object o) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.lastIndexOf(o);
    }

    @Override
    public int indexOf(Object o) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.indexOf(o);
    }

    @Override
    public boolean contains(Object o) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.contains(o);
    }

    @Override
    public boolean isEmpty() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.isEmpty();
    }

    @Override
    public int size() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        return super.size();
    }

    @Override
    public void ensureCapacity(int minCapacity) {
        if (lazyLoad) {
            this.lazyLoad();
        }
        super.ensureCapacity(minCapacity);
    }

    @Override
    public void trimToSize() {
        if (lazyLoad) {
            this.lazyLoad();
        }
        this.dirty=true;
        super.trimToSize();
    }

    
}
