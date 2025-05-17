/**
 * SQL Server adapter for MCP database server (CommonJS version)
 * Provides dual-approach connection to SQL Server with named instance support
 */

// CommonJS imports
const sql = require('mssql');
const msnodesqlv8 = require('msnodesqlv8');

class SqlServerAdapter {
  constructor(connectionInfo) {
    this.pool = null;
    this.directConnection = null;
    
    // Store original server string for metadata
    this.server = connectionInfo.server;
    this.database = connectionInfo.database;
    this.user = connectionInfo.user;
    this.password = connectionInfo.password;
    
    // Parse server string to handle named instances
    let serverName = connectionInfo.server;
    let instanceName = undefined;
    
    // Check if server includes a named instance (SERVER\INSTANCE format)
    if (connectionInfo.server.includes("\\")) {
      const parts = connectionInfo.server.split("\\");
      serverName = parts[0];
      instanceName = parts[1];
    }
    
    // Create SQL Server connection config
    this.config = {
      server: serverName,
      database: connectionInfo.database,
      port: connectionInfo.port || 1433,
      driver: "msnodesqlv8", // Explicitly use msnodesqlv8 driver
      options: {
        trustServerCertificate: connectionInfo.trustServerCertificate ?? true,
        ...(instanceName && { instanceName }),
        ...connectionInfo.options,
      },
    };
    
    // Add authentication options
    if (connectionInfo.user && connectionInfo.password) {
      this.config.user = connectionInfo.user;
      this.config.password = connectionInfo.password;
    }
    else {
      // Use Windows authentication if no username/password provided
      this.config.options.trustedConnection = true;
      this.config.options.enableArithAbort = true;
    }
    
    // Create direct ODBC connection string as fallback
    if (connectionInfo.user && connectionInfo.password) {
      // SQL Authentication
      this.directConnectionString = `Driver={ODBC Driver 17 for SQL Server};Server=${connectionInfo.server};Database=${connectionInfo.database};UID=${connectionInfo.user};PWD=${connectionInfo.password};`;
    } else {
      // Windows Authentication
      this.directConnectionString = `Driver={ODBC Driver 17 for SQL Server};Server=${connectionInfo.server};Database=${connectionInfo.database};Trusted_Connection=Yes;`;
    }
  }
  
  /**
   * Initialize SQL Server connection
   */
  async init() {
    console.log(`[INFO] Connecting to SQL Server: ${this.server}, Database: ${this.database}`);
    
    // Log connection details for debugging (remove sensitive data)
    const configForLogging = { ...this.config };
    if (configForLogging.password) configForLogging.password = "*****";
    console.log(`[DEBUG] Connection config: ${JSON.stringify(configForLogging, null, 2)}`);
    
    try {
      // First try using mssql with msnodesqlv8 driver
      await this.connectWithMssql();
      console.log(`[INFO] SQL Server connection established successfully using mssql`);
      return;
    } catch (err) {
      console.log(`[WARN] mssql connection failed: ${err.message}`);
      console.log(`[INFO] Trying fallback direct connection with msnodesqlv8...`);
    }
    
    try {
      // Fallback to direct msnodesqlv8 connection
      await this.connectWithMsnodesqlv8();
      console.log(`[INFO] SQL Server connection established successfully using direct msnodesqlv8`);
    } catch (directErr) {
      // Both connection methods failed
      console.error(`[ERROR] SQL Server connection error: ${directErr.message}`);
      this.provideConnectionDiagnostics(directErr);
      throw new Error(`Failed to connect to SQL Server: ${directErr.message}`);
    }
  }
  
  /**
   * Connect using mssql with msnodesqlv8 driver
   */
  async connectWithMssql() {
    try {
      this.pool = await sql.connect(this.config);
      return true;
    } catch (err) {
      throw err;
    }
  }
  
  /**
   * Connect using direct msnodesqlv8 driver
   */
  async connectWithMsnodesqlv8() {
    return new Promise((resolve, reject) => {
      console.log(`[DEBUG] Direct connection string (sanitized): ${this.directConnectionString.replace(/PWD=[^;]+/, "PWD=*****")}`);
      
      // Test the connection with a simple query
      msnodesqlv8.query(this.directConnectionString, "SELECT @@VERSION AS Version", (err, results) => {
        if (err) {
          reject(err);
          return;
        }
        
        // Store connection type for later use
        this.usingDirectConnection = true;
        resolve(true);
      });
    });
  }
  
  /**
   * Provide detailed diagnostics for connection failures
   */
  provideConnectionDiagnostics(error) {
    const errorMsg = error.message || '';
    
    if (errorMsg.includes("timeout")) {
      console.error("[ERROR] Connection timeout - check that:");
      console.error("  - SQL Server is running and accessible on the network");
      console.error("  - SQL Browser service is running (required for named instances)");
      console.error("  - Firewall allows connections to SQL Server (port 1433) and SQL Browser (UDP 1434)");
    } else if (errorMsg.includes("named") || errorMsg.includes("instance")) {
      console.error("[ERROR] Named instance issue - check that:");
      console.error("  - SQL Browser service is running");
      console.error("  - Instance name is correct");
      console.error("  - UDP port 1434 is open in firewall");
    } else if (errorMsg.includes("login") || errorMsg.includes("password")) {
      console.error("[ERROR] Authentication issue - check username and password");
    } else if (errorMsg.includes("driver") || errorMsg.includes("ODBC")) {
      console.error("[ERROR] Driver issue - check ODBC Driver for SQL Server is installed");
      console.error("  - Download from: https://go.microsoft.com/fwlink/?linkid=2249004");
    }
  }
  
  /**
   * Execute a SQL query and get all results
   */
  async all(query, params = []) {
    if (!this.pool && !this.usingDirectConnection) {
      throw new Error("Database not initialized");
    }
    
    // If using direct connection
    if (this.usingDirectConnection) {
      return this.allDirect(query, params);
    }
    
    // Otherwise use mssql pool
    try {
      const request = this.pool.request();
      // Add parameters to the request
      params.forEach((param, index) => {
        request.input(`param${index}`, param);
      });
      // Replace ? with named parameters
      const preparedQuery = query.replace(/\?/g, (_, i) => `@param${i}`);
      const result = await request.query(preparedQuery);
      return result.recordset;
    }
    catch (err) {
      throw new Error(`SQL Server query error: ${err.message}`);
    }
  }
  
  /**
   * Execute a SQL query with direct connection
   */
  async allDirect(query, params = []) {
    // For direct msnodesqlv8 connection
    return new Promise((resolve, reject) => {
      // Simple implementation for direct connection
      // This doesn't handle parameter binding robustly, but works for basic queries
      msnodesqlv8.query(this.directConnectionString, query, (err, results) => {
        if (err) {
          reject(new Error(`SQL Server direct query error: ${err.message}`));
          return;
        }
        resolve(results);
      });
    });
  }
  
  /**
   * Close the database connection
   */
  async close() {
    if (this.pool) {
      await this.pool.close();
      this.pool = null;
    }
    // No explicit close needed for direct msnodesqlv8 connection
    this.usingDirectConnection = false;
  }
  
  /**
   * Get database metadata
   */
  getMetadata() {
    return {
      name: "SQL Server",
      type: "sqlserver",
      server: this.server,
      database: this.database,
      connectionMethod: this.usingDirectConnection ? "direct msnodesqlv8" : "mssql pool"
    };
  }
}

// Export the adapter
module.exports = SqlServerAdapter; 