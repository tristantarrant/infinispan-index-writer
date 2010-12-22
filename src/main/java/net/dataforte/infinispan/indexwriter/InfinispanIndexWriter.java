package net.dataforte.infinispan.indexwriter;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.LockObtainFailedException;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.jgroups.Address;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.Request;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.mux.MuxRpcDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfinispanIndexWriter {
	public int DEFAULT_REMOTE_TIMEOUT = 10000;

	static final Logger log = LoggerFactory.getLogger(InfinispanIndexWriter.class);

	private IndexWriter writer;
	private Analyzer analyzer;
	private InfinispanDirectory directory;
	private MaxFieldLength maxFieldLength = MaxFieldLength.UNLIMITED;
	private MergePolicy mergePolicy;

	private RpcDispatcher dispatcher;

	private Method addDocument_doc_method;
	private Method commit_method;
	private Method rollback_method;
	private Method deleteDocuments_query_method;
	private Method deleteDocuments_queries_method;
	private Method deleteDocuments_term_method;
	private Method deleteDocuments_terms_method;
	private Method message_method;

	private EmbeddedCacheManager cacheManager;

	private int maxBufferedDocs = IndexWriter.DEFAULT_MAX_BUFFERED_DOCS;
	private double ramBufferSizeMB = IndexWriter.DEFAULT_RAM_BUFFER_SIZE_MB;

	private int remoteTimeout = DEFAULT_REMOTE_TIMEOUT;
	private boolean remoteCommit = false;
	

	/**
	 * Builds an InfinispanIndexWriter
	 * 
	 * @param cacheManager
	 * @param directory
	 * @param scopeId a unique id for each IndexWriter (must be identical on all nodes)
	 */
	public InfinispanIndexWriter(EmbeddedCacheManager cacheManager, InfinispanDirectory directory, short scopeId) {
		if(!cacheManager.isDefaultRunning()) {
			throw new IllegalStateException("Cache must be running to initialize InfinispanIndexWriter");
		}
		this.cacheManager = cacheManager;
		this.directory = directory;		
		this.dispatcher = new MuxRpcDispatcher(scopeId, MuxChannelLookup.getChannel(), null, null, this);
		
		try {
			addDocument_doc_method = this.getClass().getMethod("addDocument", Document.class);
			commit_method = this.getClass().getMethod("commit");
			deleteDocuments_query_method = this.getClass().getMethod("deleteDocuments", Query.class);
			deleteDocuments_queries_method = this.getClass().getMethod("deleteDocuments", Query[].class);
			deleteDocuments_term_method = this.getClass().getMethod("deleteDocuments", Term.class);
			deleteDocuments_terms_method = this.getClass().getMethod("deleteDocuments", Term[].class);
			rollback_method = this.getClass().getMethod("rollback");
			message_method = this.getClass().getMethod("message", String.class);
		} catch (Throwable t) {
			log.error("", t);
		}
	}

	public Analyzer getAnalyzer() {
		return analyzer;
	}

	public void setAnalyzer(Analyzer analyzer) {
		this.analyzer = analyzer;
	}

	public MaxFieldLength getMaxFieldLength() {
		return maxFieldLength;
	}

	public void setMaxFieldLength(MaxFieldLength maxFieldLength) {
		this.maxFieldLength = maxFieldLength;
	}

	public MergePolicy getMergePolicy() {
		return mergePolicy;
	}

	public void setMergePolicy(MergePolicy mergePolicy) {
		this.mergePolicy = mergePolicy;
	}

	public int getRemoteTimeout() {
		return remoteTimeout;
	}

	public void setRemoteTimeout(int remoteTimeout) {
		this.remoteTimeout = remoteTimeout;
	}

	public boolean isRemoteCommit() {
		return remoteCommit;
	}

	public void setRemoteCommit(boolean remoteCommit) {
		this.remoteCommit = remoteCommit;
	}	
	
	public void message(String s) {
		if(cacheManager.isCoordinator()) {
			System.out.println(s);
		} else {
			// Send to the coordinator
			MethodCall call = new MethodCall(message_method, s);
			try {
				sendToCoordinator(call);
			} catch (Throwable t) {
				log.error("", t);
			}
		}
	}

	// Proxied IndexWriter methods

	public void addDocument(Document doc) throws CorruptIndexException, IOException {
		if (isWriter()) {
			writer.addDocument(doc);
		} else {
			// Send to the coordinator
			MethodCall call = new MethodCall(addDocument_doc_method, doc);
			try {
				sendToCoordinator(call);
			} catch (Throwable t) {
				log.error("", t);
			}
		}
	}

	public void deleteDocuments(Query query) throws CorruptIndexException, IOException {
		if (isWriter()) {
			writer.deleteDocuments(query);
		} else {
			MethodCall call = new MethodCall(deleteDocuments_query_method, query);
			try {
				sendToCoordinator(call);
			} catch (Throwable t) {
				log.error("", t);
			}
		}
	}

	public void deleteDocuments(Query... queries) throws CorruptIndexException, IOException {
		if (isWriter()) {
			writer.deleteDocuments(queries);
		} else {
			MethodCall call = new MethodCall(deleteDocuments_queries_method, (Object[]) queries);
			try {
				sendToCoordinator(call);
			} catch (Throwable t) {
				log.error("", t);
			}
		}
	}

	public void deleteDocuments(Term term) throws CorruptIndexException, IOException {
		if (isWriter()) {
			writer.deleteDocuments(term);
		} else {
			MethodCall call = new MethodCall(deleteDocuments_term_method, term);
			try {
				sendToCoordinator(call);
			} catch (Throwable t) {
				log.error("", t);
			}
		}
	}

	public void deleteDocuments(Term... terms) throws CorruptIndexException, IOException {
		if (isWriter()) {
			writer.deleteDocuments(terms);
		} else {
			MethodCall call = new MethodCall(deleteDocuments_terms_method, (Object[]) terms);
			try {
				sendToCoordinator(call);
			} catch (Throwable t) {
				log.error("", t);
			}
		}
	}

	public void commit() throws CorruptIndexException, IOException {
		if (isWriter()) {
			writer.commit();
		} else if (remoteCommit) {
			MethodCall call = new MethodCall(commit_method);
			try {
				sendToCoordinator(call);
			} catch (Throwable t) {
				log.error("", t);
			}
		}
	}

	public void rollback() throws IOException {
		if (isWriter()) {
			writer.rollback();
		} else if (remoteCommit) {
			MethodCall call = new MethodCall(rollback_method);
			try {
				sendToCoordinator(call);
			} catch (Throwable t) {
				log.error("", t);
			}
		}
	}

	public void close() throws CorruptIndexException, IOException {
		if (isWriter()) {
			writer.close();
		}
		if (dispatcher != null) {
			dispatcher.stop();
		}
	}

	// INTERNAL METHODS

	private void sendToCoordinator(MethodCall call) throws Throwable {
		dispatcher.callRemoteMethod(getCoordinator(), call, new RequestOptions(Request.GET_FIRST, remoteTimeout));
	}
	
	private synchronized IndexWriter getWriter() throws CorruptIndexException, LockObtainFailedException, IOException {
		if (writer == null) {
			log.info("Initializing IndexWriter");
			writer = new IndexWriter(directory, analyzer, maxFieldLength);
			writer.setMaxBufferedDocs(maxBufferedDocs);
			writer.setRAMBufferSizeMB(ramBufferSizeMB);
			if (mergePolicy != null) {
				writer.setMergePolicy(mergePolicy);
			}
		}
		return writer;
	}

	private boolean isWriter() throws CorruptIndexException, LockObtainFailedException, IOException {
		if (cacheManager.isCoordinator()) {
			getWriter();
			return true;
		} else {
			return false;
		}
	}

	private Address getCoordinator() {
		JGroupsAddress coordinator = (JGroupsAddress) cacheManager.getCoordinator();
		return coordinator.getJGroupsAddress();
	}

	
}
