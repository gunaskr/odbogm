/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.odbogm.cache;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Basado en https://explainjava.com/simple-in-memory-cache-java/
 *
 * @author Marcelo D. Ré {@literal <marcelo.re@gmail.com>}
 */
public class SimpleCache implements Cache {

    private final static Logger LOGGER = Logger.getLogger(SimpleCache.class.getName());

    static {
        if (LOGGER.getLevel() == null) {
            LOGGER.setLevel(Level.INFO);
        }
    }

    private int CLEAN_UP_PERIOD_IN_SEC = 3;

    private final ConcurrentHashMap<String, WeakReference<Object>> cache = new ConcurrentHashMap<>();
    
    private ReferenceQueue<Object> referenceQueue = new ReferenceQueue<>();
    
    
    public SimpleCache() {
        Thread cleanerThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    LOGGER.log(Level.FINER, "Limpiando el cache...");
                    synchronized (this) {
                        cache.entrySet().removeIf((t) -> t.getValue().get() == null);
                    }
                    Thread.sleep(CLEAN_UP_PERIOD_IN_SEC * 1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        cleanerThread.setDaemon(true);
        cleanerThread.start();
    }

    /**
     * agrega una entrada al cache.
     *
     * @param key clave a agregar
     * @param value objeto para el cache
     */
    @Override
    public void add(String key, Object value) {
        LOGGER.log(Level.FINER, "adding: {0} {1} value: {2}", new Object[]{key, value.getClass().getSimpleName()});
        if (key == null) {
            return;
        }
        if (value == null) {
            cache.remove(key);
        } else {
//            long expiryTime = System.currentTimeMillis() + periodInMillis;
            cache.put(key, new WeakReference<>(value,referenceQueue));
        }
    }

    /**
     * remueve una entrada en el cache.
     *
     * @param key clave a remover
     */
    @Override
    public void remove(String key) {
        cache.remove(key);
    }

    /**
     * obtiene un objeto desde el cache. Si el objeto no existe devuelve null.
     *
     * @param key clave a buscar.
     * @return el objeto solicitado o null en caso de no encontrarlo.
     */
    @Override
    public Object get(String key) {
        Object r = null;
        WeakReference<Object> wr = this.cache.get(key);
        if (wr != null) {
            LOGGER.log(Level.FINEST, "\n\n\ncache enqueued: "+wr.isEnqueued()+"\n\n\n");
            if (!wr.isEnqueued())
                r = wr.get();
            if (r == null) {
                remove(key);
            }
        }
        return r;
//        return Optional.ofNullable(cache.get(key)).map(WeakReference::get).filter(cacheObject -> !cacheObject.isExpired()).map(CacheObject::getValue).orElse(null);
    }

    /**
     * elimina todo el cache.
     */
    @Override
    public void clear() {
        cache.clear();
    }

    /**
     * devuevle el tamaño actual del cache. Este tamaño incluye también las entradas derefereniciadas.
     *
     * @return long
     */
    @Override
    public long size() {
//        return cache.entrySet().stream().filter(entry -> Optional.ofNullable(entry.getValue()).map(WeakReference::get).map(cacheObject -> !cacheObject.isExpired()).orElse(false)).count();
        return cache.size();
    }

    /**
     * Retorna el Mapa de los objetos que se encuentran en el cache.
     *
     * @return una referencia al map interno.
     */
    public synchronized Map<String, Object> getCachedObjects() {
        Map<String, Object> ret = new HashMap<>();

        for (Iterator<Map.Entry<String, WeakReference<Object>>> iterator = this.cache.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<String, WeakReference<Object>> next = iterator.next();

            String key = next.getKey();
            WeakReference<Object> value = next.getValue();
            if (value.get() != null) {
                ret.put(key, System.identityHashCode(value.get()));
            } else {
                iterator.remove();
            }
        }

        return ret;
    }

    /**
     * Establece el tiempo entre cada ejecucion del hilo que limpia el caché.
     *
     * @param seconds segundos entre cada ejecución
     * @return this
     */
    public SimpleCache setTimeInterval(int seconds) {
        this.CLEAN_UP_PERIOD_IN_SEC = seconds;
        return this;
    }
//    private static class CacheObject {
// 
//        @Getter
//        private Object value;
//        private long expiryTime;
// 
//        boolean isExpired() {
//            return System.currentTimeMillis() > expiryTime;
//        }
//    }
}
