package org.apache.hive.service.auth;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslServerTransport;

public class TUGIContainingProcessor implements TProcessor{

  private final TProcessor wrapped;
  private final HadoopShims shim;
  private final boolean isFsCacheDisabled;

  // Keep all session UGIs to close their filesystems at Statement.close() and Connection.close()
  // it's done to prevent LeaseChecker thread leak and to clean FileSystem.CACHE
  private static final ThreadLocal<HashSet<UserGroupInformation>> ugis = new ThreadLocal<HashSet<UserGroupInformation>>() {
    @Override
    protected HashSet<UserGroupInformation> initialValue() {
      return new HashSet<UserGroupInformation>();
    }
  };

  /**
   * Close FileSystem for session UGIs
   */
  public static void closeAllFsForUGIs() {
    HashSet<UserGroupInformation> ugisSet = ugis.get();
    HadoopShims sh = ShimLoader.getHadoopShims();
    for (UserGroupInformation ugi : ugisSet) {
      sh.closeAllForUGI(ugi);
    }
    ugisSet.clear();
  }

  public TUGIContainingProcessor(TProcessor wrapped, Configuration conf) {
    this.wrapped = wrapped;
    this.isFsCacheDisabled = conf.getBoolean(String.format("fs.%s.impl.disable.cache",
      FileSystem.getDefaultUri(conf).getScheme()), false);
    this.shim = ShimLoader.getHadoopShims();
  }

  @Override
  public boolean process(final TProtocol in, final TProtocol out) throws TException {
    UserGroupInformation clientUgi = null;

    try {
      clientUgi = shim.createRemoteUser(((TSaslServerTransport)in.getTransport()).
          getSaslServer().getAuthorizationID(), new ArrayList<String>());
      // remember all UGIs created during the session
      ugis.get().add(clientUgi);
      return shim.doAs(clientUgi, new PrivilegedExceptionAction<Boolean>() {
        public Boolean run() {
          try {
            return wrapped.process(in, out);
          } catch (TException te) {
            throw new RuntimeException(te);
          }
        }
      });
    }
    catch (RuntimeException rte) {
      if (rte.getCause() instanceof TException) {
        throw (TException)rte.getCause();
      }
      throw rte;
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie); // unexpected!
    } catch (IOException ioe) {
      throw new RuntimeException(ioe); // unexpected!
    }
  }
}
