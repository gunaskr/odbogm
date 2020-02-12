package net.odbogm;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;

import net.odbogm.exceptions.ClassToVertexNotFound;
import net.odbogm.exceptions.ConcurrentModification;
import net.odbogm.exceptions.IncorrectRIDField;
import net.odbogm.exceptions.NoOpenTx;
import net.odbogm.exceptions.OdbogmException;
import net.odbogm.exceptions.ReferentialIntegrityViolation;
import net.odbogm.exceptions.UnknownObject;
import net.odbogm.exceptions.UnknownRID;
import net.odbogm.exceptions.VertexJavaClassNotFound;
import net.odbogm.proxy.IObjectProxy;
import net.odbogm.utils.ODBOrientDynaElementIterable;

/**
 *
 * @author Marcelo D. Ré {@literal <marcelo.re@gmail.com>}
 */
public class SessionManager implements IActions.IStore, IActions.IGet {

    private final static Logger LOGGER = Logger.getLogger(SessionManager.class.getName());
    static {
        if (LOGGER.getLevel() == null) {
            LOGGER.setLevel(LogginProperties.SessionManager);
        }
    }
    
    private final DataSource datasource;

    // Using single objectMapper to save memory. These instances are shared among all transactions.
    private ObjectMapper objectMapper;
    
    public enum ActivationStrategy {
        CLASS_INSTRUMENTATION // modify the class with an agent to add write detection
    }
    
    private ActivationStrategy activationStrategy;
    
    private List<WeakReference<Transaction>> openTransactionsList = new ArrayList<>();
    private Transaction transaction; 

    public SessionManager(DataSource datasource) throws SQLException {
    	this.datasource = datasource;
    	this.objectMapper = new ObjectMapper();
    	this.setActivationStrategy(ActivationStrategy.CLASS_INSTRUMENTATION, true);
    }

    /**
     * Establece la estrategia a utilizar para detectar los cambios en los objetos.
     * 
     * @param as Estrategia de detección de dirty.
     * @param loadAgent Determina si se debe cargar el agente.
     * @return this
     */    
    private SessionManager setActivationStrategy(ActivationStrategy as, boolean loadAgent) {
        this.activationStrategy = as;
        LOGGER.log(Level.INFO, "ActivationStrategy using {0}", as);
        return this;
    }
    
    public ActivationStrategy getActivationStrategy() {
        return this.activationStrategy;
    }
    
    /**
     * Inicia una transacción contra el servidor.
     */
    public void begin() {
        if (this.transaction == null) {
            // si no hay una transacción creada, abrir una...
            transaction = getTransaction();
        } else {
            this.transaction.begin();
        }
    }

    /**
     * Devuelve una transacción privada. Los objetos solicitados a través de esta transacción se mantienen en 
     * forma independiente de los recuperados en otras, pero se comparte la comunicación subyacente a la base 
     * de datos.
     * 
     * @return un objeto Transaction para operar.
     */    
    public Transaction getTransaction() {
        Transaction t = new Transaction(this);
        openTransactionsList.add(new WeakReference<>(t));
        return t;
    }
    
    /**
     * Devuelve la transacción por defecto que está utilizando el SessionManager.
     * @return publicTransaction
     */
    public Transaction getCurrentTransaction() {
        return this.transaction;
    }
    
    /**
     * Crea a un *NUEVO* vértice en la base de datos a partir del objeto. Retorna el RID del objeto que se agregó a la base.
     *
     * @param <T> clase base del objeto.
     * @param o objeto de referencia a almacenar.
     */
    @Override
    public synchronized <T> T store(T o) throws IncorrectRIDField, NoOpenTx, ClassToVertexNotFound {
        return this.transaction.store(o);
    }

    /**
     * Remueve un vértice y todos los vértices apuntados por él y marcados con @RemoveOrphan
     *
     * @param toRemove referencia al objeto a remover
     */
    public void delete(Object toRemove) throws ReferentialIntegrityViolation, UnknownObject {
        this.transaction.delete(toRemove);
    }

    /**
     * Retorna el ObjectMapper asociado a la sesión
     *
     * @return retorna un mapa del objeto
     */
    public ObjectMapper getObjectMapper() {
        return this.objectMapper;
    }

    /**
     * Recupera el @RID asociado al objeto. Se debe tener en cuenta que si el objeto aún no se ha persistido la base devolverá un RID temporal con los
     * ids en negativo. Ej: #-10:-2
     *
     * @param o objeto de referencia
     * @return el RID del objeto o null.
     */
    public String getRID(Object o) {
        if ((o != null) && (o instanceof IObjectProxy)) {
            return ((IObjectProxy) o).___getVertex().getIdentity().toString();
        }
        return null;
    }

    /**
     * Persistir la información pendiente en la transacción.
     *
     * @throws ConcurrentModification Si un elemento fue modificado por una transacción anterior.
     * @throws OdbogmException Si ocurre alguna otra excepción de la base de datos.
     */
    public synchronized void commit() throws ConcurrentModification, OdbogmException {
        this.transaction.commit();
    }

    
    /**
     * Vuelve a cargar el objeto con los datos desde las base.
     * Esta accion no se propaga sobre los objetos que lo componen.
     * 
     * @param o objeto recuperado de la base a ser actualizado.
     */
    public synchronized void refreshObject(IObjectProxy o) {
        this.transaction.refreshObject(o);
    }
    
    
    /**
     * realiza un rollback sobre la transacción activa.
     */
    public synchronized void rollback() {
        this.transaction.getODatabaseSession().getTransaction().rollback();
    }

    /**
     * Finaliza la comunicación con la base. 
     * Todas las transacciones abiertas son ROLLBACK y finalizadas.
     */
	
	  public void shutdown() {
		  System.out.println("shut down ");
		  }

    
    /**
     * Recupera un objecto desde la base a partir del RID del Vértice.
     *
     * @param rid: ID del vértice a recupear
     * @return Retorna un objeto de la clase javaClass del vértice.
     */
    @Override
    public Object get(String rid) throws UnknownRID, VertexJavaClassNotFound {
        return this.transaction.get(rid);
    }

    
    /**
     * Recupera un objeto a partir de la clase y el RID correspondiente.
     *
     * @param <T> clase a devolver
     * @param type clase a devolver
     * @param rid RID del vértice de la base
     * @return objeto de la clase T
     * @throws UnknownRID si no se encuentra ningún vértice con ese RID
     */
    @Override
    public <T> T get(Class<T> type, String rid) throws UnknownRID {
        return this.transaction.get(type, rid);
    }
    
    
    @Override
    public <T> T getEdgeAsObject(Class<T> type, OEdge e) {
        return this.transaction.getEdgeAsObject(type, e);
    }

    /**
     * Realiza un query direto a la base de datos y devuelve el resultado directamente sin procesarlo.
     *
     * @param sql sentencia a ejecutar 
     * @return resutado de la ejecución de la sentencia SQL
     */
    public ODBOrientDynaElementIterable query(String sql) {
        return this.transaction.query(sql);
    }

    /**
     * Ejecuta un comando que devuelve un número. El valor devuelto será el primero que se encuentre en la lista de resultado.
     *
     * @param sql comando a ejecutar
     * @param retVal nombre de la propiedad a devolver
     * @return retorna el valor de la propiedad indacada obtenida de la ejecución de la consulta
     *
     * ejemplo: int size = sm.query("select count(*) as size from TestData","size");
     */
    public long query(String sql, String retVal) {
        
        return this.transaction.query(sql, retVal);
    }

    /**
     * Realiza un query direto a la base de datos y devuelve el resultado directamente sin procesarlo.
     *
     * @param sql sentencia a ejecutar
     * @param param parámetros a utilizar en el query
     * @return resutado de la ejecución de la sentencia SQL
     */
    public ODBOrientDynaElementIterable query(String sql, Object... param) {
        return this.transaction.query(sql, param);
    }
    
    /**
     * Return all record of the reference class.
     * Devuelve todos los registros a partir de una clase base.
     *
     * @param <T> Reference class
     * @param clazz reference class 
     * @return return a list of object of the refecence class.
     */
    public <T> List<T> query(Class<T> clazz) {
        return this.transaction.query(clazz);
    }

    /**
     * Devuelve todos los registros a partir de una clase base en una lista, filtrando los datos por lo que se agregue en el body.
     *
     * @param <T> clase base que se utilizará para el armado de la lista
     * @param clase clase base.
     * @param body cuerpo a agregar a la sentencia select. Ej: "where ...."
     * @return Lista con todos los objetos recuperados.
     */
    public <T> List<T> query(Class<T> clase, String body) {
        return this.transaction.query(clase, body);
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
    public <T> List<T> query(Class<T> clase, String sql, Object... param) {
        
        return this.transaction.query(clase, sql, param);
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
    public <T> List<T> query(Class<T> clase, String sql, HashMap<String,Object> param) {
        return this.transaction.query(clase, sql, param);
    }
    
    
    
    /**
     * Devuelve el objecto de definición de la clase en la base.
     *
     * @param clase nombre de la clase
     * @return OClass o null si la clase no existe
     */
    public OClass getDBClass(String clase) {
        return this.transaction.getDBClass(clase);
    }
    
    
    public SessionManager setClassLevelLog(Class<?> clazz, Level level) {
        Logger L = Logger.getLogger(clazz.getName());
        L.setLevel(level);
        return this;
    }
    
    
    public int openTransactionsCount() {
        return (int)openTransactionsList.stream().filter(w -> w.get() != null).count();
    }

	ODatabaseSession getConnection() {
		try {
			Connection connection = this.datasource.getConnection();
			return (ODatabaseSession) connection.unwrap(OrientJdbcConnection.class).getDatabase();
		} catch (SQLException e) {
			throw new RuntimeException(e.getCause());
		}
	}
    
}
