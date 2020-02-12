package net.odbogm;

import com.tinkerpop.blueprints.Vertex;
import java.util.List;
import net.odbogm.utils.ODBOrientDynaElementIterable;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import test.Foo;
import test.SimpleVertex;
import test.SimpleVertexEx;

/**
 *
 * @author jbertinetti
 */
public class QueryTest extends IntegrationTest {

    @After
    public void tearDown() {
        sm.shutdown();
    }
    
    @Test
    public void testSimpleSave() {
        
        SimpleVertexEx sve = new SimpleVertexEx();

        System.out.println("guardado del objeto limpio.");
        SimpleVertexEx stored = this.sm.store(sve);
        sm.commit();

        System.out.println("consultando por SimpleVertex....");
        List<SimpleVertexEx> list = sm.query(SimpleVertexEx.class);
        assertTrue(list.size() > 0);
        
        assertTrue("rid " + stored.getRid() + "is not saved in db",list.stream().anyMatch(simpleVertexEx -> {
        	return simpleVertexEx.getRid().equalsIgnoreCase(stored.getRid());
        }));
    }


    /**
  ††* Verify that a simple query based on a class returns the list
  ††* correct objects.
    **/
    @Test
    public void testSimpleQuery() {
        
        SimpleVertexEx sve = new SimpleVertexEx();
        sve.initEnum();
        sve.initInner();
        sve.initArrayList();
        sve.initHashMap();

        System.out.println("guardado del objeto limpio.");
        SimpleVertexEx stored = this.sm.store(sve);
        sm.commit();
        
        /* testing inner field */
        SimpleVertex svinner = stored.svinner;
        
        List<SimpleVertex> list = sm.query(SimpleVertex.class);
        assertTrue(list.size() > 0);
        
        assertTrue("rid " + stored.getRid() + "is not saved in db",list.stream().anyMatch(simpleVertexEx -> {
        	return simpleVertexEx.getRid().equalsIgnoreCase(svinner.getRid());
        }));
        
        /* testing nested objects */
        assertTrue("not getting all the items", stored.alSV.size() == 3);
        assertTrue("not getting all items in the map", stored.hmSV.size() == 3);
        assertNotNull("value not present in map", stored.hmSV.get("key1"));
        
    }

    
    
    /*
     * Testea la consulta que devuelve una lista de objetos espec√≠ficos.
     */
    @Test
    public void listQuery() throws Exception {
        Foo foo = new Foo("test query");
        foo.add(new SimpleVertex("related vertex"));
        sm.store(foo); //debe ir en los resultados
        sm.store(new Foo("excluded")); //no debe ir en los resultados
        sm.commit();
        sm.getTransaction().clearCache();
        
        List<Foo> res = sm.query(Foo.class, "where text = 'test query'");
        assertFalse(res.isEmpty());
        for (Foo f : res) {
            assertEquals("test query", f.getText());
            SimpleVertex sv = f.getLsve().iterator().next();
            assertEquals("related vertex", sv.getS());
        }
    }
    
    
    @Test
    public void testQueryUncommitted() throws Exception {
        Foo foo = new Foo();
        foo = sm.store(foo);
        sm.commit();
        String rid = sm.getRID(foo);
        sm.getTransaction().clearCache();
        
        //tener en cuenta que un query tipado devuelve objetos que est√°n en el 
        //cach√© del ogm, por lo que lo siguiente se cumple:
        foo.setText("modified");
        List<Foo> res = sm.query(Foo.class, "where @rid = " + rid);
        assertEquals("modified", res.iterator().next().getText());
    }
    
}
