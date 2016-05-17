/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.odbogm.proxy;

import net.odbogm.SessionManager;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.util.Map;

/**
 *
 * @author SShadow
 */
public interface ILazyMapCalls extends ILazyCalls {
    public void init(SessionManager sm, OrientVertex relatedTo, String field, Class<?> keyClass, Class<?> valueClass);
    public Map<Object,ObjectCollectionState> collectionState();
    
    
    public Map<Object, ObjectCollectionState> getEntitiesState();
    public Map<Object, ObjectCollectionState> getKeyState();
    public Map<Object, OrientEdge> getKeyToEdge();
}
