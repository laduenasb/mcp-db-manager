import { DbAdapter } from "./adapter.js";
import sql from "mssql";
import msnodesqlv8 from "msnodesqlv8";

/**
 * SQL Server connection options
 */
export interface SqlServerConnectionInfo {
  server: string;
  database: string;
  user?: string;
  password?: string;
  port?: number;
  trustServerCertificate?: boolean;
  driver?: "msnodesqlv8"; // Allow specifying preferred driver
  driverVersion?: string; // ODBC Driver version (default: "17")
  options?: any;
}

/**
 * SQL Server database adapter implementation
 */
export class SqlServerAdapter implements DbAdapter {
  private pool: sql.ConnectionPool | null = null;
  private config!: sql.config; // Use definite assignment assertion
  private connectionString: string = "";
  private server: string;
  private database: string;
  private user?: string;
  private password?: string;
  private useNativeDriver: boolean = false;
  private driverVersion: string;

  constructor(connectionInfo: SqlServerConnectionInfo) {
    // Store original server string for metadata
    this.server = connectionInfo.server;
    this.database = connectionInfo.database;
    this.user = connectionInfo.user;
    this.password = connectionInfo.password;
    this.driverVersion = connectionInfo.driverVersion || "17";

    // Determine if we should use msnodesqlv8 (native driver) or mssql
    // User can explicitly choose a driver, otherwise we detect based on environment
    if (connectionInfo.driver === "msnodesqlv8") {
      this.useNativeDriver = true;
    } else {
      // Auto-detect: try to use native driver by default for better named instance support
      try {
        // Simple test to see if the module is available
        if (msnodesqlv8) {
          this.useNativeDriver = true;
          console.error(`[INFO] Using native SQL Server driver (msnodesqlv8)`);
        }
      } catch (err) {
        console.error(
          `[INFO] Native SQL Server driver not available, using mssql module`
        );
        this.useNativeDriver = false;
      }
    }

    // Parse server string to handle named instances
    let serverName = connectionInfo.server;
    let instanceName: string | undefined = undefined;

    // Check if server includes a named instance (SERVER\INSTANCE format)
    if (connectionInfo.server.includes("\\")) {
      const parts = connectionInfo.server.split("\\");
      serverName = parts[0];
      instanceName = parts[1];
    }

    // For msnodesqlv8, prepare connection string
    if (this.useNativeDriver) {
      this.prepareConnectionString(connectionInfo);
    } else {
      // Create SQL Server connection config for mssql module
      this.config = {
        server: serverName + "\\" + instanceName,
        database: connectionInfo.database,
        port: connectionInfo.port || 1433,
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
      } else {
        // Use Windows authentication if no username/password provided
        this.config.options!.trustedConnection = true;
        this.config.options!.enableArithAbort = true;
      }
    }
  }

  /**
   * Prepare connection string for msnodesqlv8 driver
   */
  private prepareConnectionString(
    connectionInfo: SqlServerConnectionInfo
  ): void {
    if (connectionInfo.user && connectionInfo.password) {
      // SQL Server Authentication
      this.connectionString = `Driver={ODBC Driver ${this.driverVersion} for SQL Server};Server=${connectionInfo.server};Database=${connectionInfo.database};UID=${connectionInfo.user};PWD=${connectionInfo.password};`;
    } else {
      // Windows Authentication
      this.connectionString = `Driver={ODBC Driver ${this.driverVersion} for SQL Server};Server=${connectionInfo.server};Database=${connectionInfo.database};Trusted_Connection=Yes;`;
    }

    // Add additional options if provided
    if (connectionInfo.trustServerCertificate) {
      this.connectionString += "TrustServerCertificate=Yes;";
    }

    // Add port if specified
    if (connectionInfo.port) {
      this.connectionString += `Port=${connectionInfo.port};`;
    }

    // Add custom options if provided
    if (connectionInfo.options) {
      Object.entries(connectionInfo.options).forEach(([key, value]) => {
        this.connectionString += `${key}=${value};`;
      });
    }
  }

  /**
   * Initialize SQL Server connection
   */
  async init(): Promise<void> {
    try {
      console.error(
        `[INFO] Connecting to SQL Server: ${this.server}, Database: ${
          this.database
        } using ${this.useNativeDriver ? "msnodesqlv8" : "mssql"} driver`
      );

      if (this.useNativeDriver) {
        console.error(
          `[DEBUG] Connection string template: ${this.connectionString.replace(
            /PWD=.*?;/,
            "PWD=*****;"
          )}`
        );

        // For msnodesqlv8, we'll verify the connection by running a simple query
        await this.testConnection();
        console.error(
          `[INFO] SQL Server connection (msnodesqlv8) established successfully`
        );
      } else {
        // Log connection details for debugging (remove sensitive data)
        const configForLogging = { ...this.config };
        if (configForLogging.password) configForLogging.password = "*****";
        console.error(
          `[DEBUG] Connection config: ${JSON.stringify(
            configForLogging,
            null,
            2
          )}`
        );

        this.pool = await new sql.ConnectionPool(this.config).connect();
        console.error(
          `[INFO] SQL Server connection (mssql) established successfully`
        );
      }
    } catch (err) {
      // Format error message with more details
      const message = (err as Error).message;
      console.error(`[ERROR] SQL Server connection error: ${message}`);

      // Add more specific error information
      if (message.includes("named")) {
        console.error(
          "[ERROR] This appears to be a named instance issue. Verify SQL Browser service is running."
        );
      } else if (message.includes("getaddrinfo")) {
        console.error(
          "[ERROR] Cannot resolve the server name. Check network connectivity and DNS."
        );
      }

      throw new Error(`Failed to connect to SQL Server: ${message}`);
    }
  }

  /**
   * Test connection with msnodesqlv8 driver
   */
  private testConnection(): Promise<void> {
    return new Promise((resolve, reject) => {
      msnodesqlv8.query(
        this.connectionString,
        "SELECT 1 AS TestConnection",
        (err, results) => {
          if (err) {
            reject(err);
            return;
          }
          resolve();
        }
      );
    });
  }

  /**
   * Execute a SQL query and get all results
   * @param query SQL query to execute
   * @param params Query parameters
   * @returns Promise with query results
   */
  async all(query: string, params: any[] = []): Promise<any[]> {
    if (this.useNativeDriver) {
      return this.allWithNativeDriver(query, params);
    } else {
      return this.allWithMssql(query, params);
    }
  }

  /**
   * Execute query with msnodesqlv8 driver
   */
  private allWithNativeDriver(
    query: string,
    params: any[] = []
  ): Promise<any[]> {
    // Prepare the query with parameter substitution
    const { preparedQuery, preparedParams } = this.prepareNativeQuery(
      query,
      params
    );

    return new Promise((resolve, reject) => {
      msnodesqlv8.query(
        this.connectionString,
        preparedQuery,
        preparedParams,
        (err, results) => {
          if (err) {
            reject(new Error(`SQL Server query error: ${err.message}`));
            return;
          }
          resolve(results || []); // Ensure we always return an array
        }
      );
    });
  }

  /**
   * Execute query with mssql driver
   */
  private async allWithMssql(
    query: string,
    params: any[] = []
  ): Promise<any[]> {
    if (!this.pool) {
      throw new Error("Database not initialized");
    }

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
    } catch (err) {
      throw new Error(`SQL Server query error: ${(err as Error).message}`);
    }
  }

  /**
   * Execute a SQL query that modifies data
   * @param query SQL query to execute
   * @param params Query parameters
   * @returns Promise with result info
   */
  async run(
    query: string,
    params: any[] = []
  ): Promise<{ changes: number; lastID: number }> {
    if (this.useNativeDriver) {
      return this.runWithNativeDriver(query, params);
    } else {
      return this.runWithMssql(query, params);
    }
  }

  /**
   * Execute modifying query with msnodesqlv8 driver
   */
  private async runWithNativeDriver(
    query: string,
    params: any[] = []
  ): Promise<{ changes: number; lastID: number }> {
    // Check if it's an INSERT query to handle lastID
    const isInsert = query.trim().toUpperCase().startsWith("INSERT");

    // For INSERT queries, we need to get the last inserted ID
    if (isInsert) {
      // Add SCOPE_IDENTITY() to get the last ID
      const modifiedQuery = `${query}; SELECT SCOPE_IDENTITY() AS LastID`;
      const { preparedQuery, preparedParams } = this.prepareNativeQuery(
        modifiedQuery,
        params
      );

      return new Promise((resolve, reject) => {
        msnodesqlv8.query(
          this.connectionString,
          preparedQuery,
          preparedParams,
          (err, results) => {
            if (err) {
              reject(new Error(`SQL Server query error: ${err.message}`));
              return;
            }

            // The LastID will be in the second result set
            let lastID = 0;
            if (
              Array.isArray(results) &&
              results.length > 0 &&
              results[results.length - 1].LastID
            ) {
              lastID = Number(results[results.length - 1].LastID);
            }

            resolve({
              changes: lastID > 0 ? 1 : 0, // If we got an ID, at least one row was affected
              lastID: lastID,
            });
          }
        );
      });
    } else {
      // For non-INSERT queries
      const { preparedQuery, preparedParams } = this.prepareNativeQuery(
        query,
        params
      );

      return new Promise((resolve, reject) => {
        msnodesqlv8.query(
          this.connectionString,
          preparedQuery,
          preparedParams,
          (err, results) => {
            if (err) {
              reject(new Error(`SQL Server query error: ${err.message}`));
              return;
            }

            // For UPDATE/DELETE, try to determine affected rows
            let changes = 0;
            if (typeof results === "number") {
              changes = results;
            } else if (
              results &&
              typeof results === "object" &&
              "rowsAffected" in results
            ) {
              // Check if rowsAffected exists as a property
              changes = results.rowsAffected as number;
            }

            resolve({
              changes,
              lastID: 0, // Non-INSERT operations don't return a last ID
            });
          }
        );
      });
    }
  }

  /**
   * Execute modifying query with mssql driver
   */
  private async runWithMssql(
    query: string,
    params: any[] = []
  ): Promise<{ changes: number; lastID: number }> {
    if (!this.pool) {
      throw new Error("Database not initialized");
    }

    try {
      const request = this.pool.request();

      // Add parameters to the request
      params.forEach((param, index) => {
        request.input(`param${index}`, param);
      });

      // Replace ? with named parameters
      const preparedQuery = query.replace(/\?/g, (_, i) => `@param${i}`);

      // Add output parameter for identity value if it's an INSERT
      let lastID = 0;
      if (query.trim().toUpperCase().startsWith("INSERT")) {
        request.output("insertedId", sql.Int, 0);
        const updatedQuery = `${preparedQuery}; SELECT @insertedId = SCOPE_IDENTITY();`;
        const result = await request.query(updatedQuery);
        lastID = result.output.insertedId || 0;
      } else {
        const result = await request.query(preparedQuery);
        lastID = 0;
      }

      return {
        changes: this.getAffectedRows(query, lastID),
        lastID: lastID,
      };
    } catch (err) {
      throw new Error(`SQL Server query error: ${(err as Error).message}`);
    }
  }

  /**
   * Prepare a query with parameters for the native driver
   */
  private prepareNativeQuery(
    query: string,
    params: any[]
  ): { preparedQuery: string; preparedParams: any[] } {
    // If there are no parameters, return the query as is
    if (!params || params.length === 0) {
      return { preparedQuery: query, preparedParams: [] };
    }

    // For msnodesqlv8, we need to use ? placeholders
    // The driver already supports ? placeholders, so we just pass the params array
    return { preparedQuery: query, preparedParams: params };
  }

  /**
   * Execute multiple SQL statements
   * @param query SQL statements to execute
   * @returns Promise that resolves when execution completes
   */
  async exec(query: string): Promise<void> {
    if (this.useNativeDriver) {
      return this.execWithNativeDriver(query);
    } else {
      return this.execWithMssql(query);
    }
  }

  /**
   * Execute batch with msnodesqlv8 driver
   */
  private execWithNativeDriver(query: string): Promise<void> {
    return new Promise((resolve, reject) => {
      msnodesqlv8.query(this.connectionString, query, (err, results) => {
        if (err) {
          reject(new Error(`SQL Server batch error: ${err.message}`));
          return;
        }
        resolve();
      });
    });
  }

  /**
   * Execute batch with mssql driver
   */
  private async execWithMssql(query: string): Promise<void> {
    if (!this.pool) {
      throw new Error("Database not initialized");
    }

    try {
      const request = this.pool.request();
      await request.batch(query);
    } catch (err) {
      throw new Error(`SQL Server batch error: ${(err as Error).message}`);
    }
  }

  /**
   * Close the database connection
   */
  async close(): Promise<void> {
    if (this.useNativeDriver) {
      // msnodesqlv8 doesn't maintain persistent connections that need explicit closing
      return Promise.resolve();
    } else if (this.pool) {
      await this.pool.close();
      this.pool = null;
    }
  }

  /**
   * Get database metadata
   */
  getMetadata(): {
    name: string;
    type: string;
    server: string;
    database: string;
  } {
    return {
      name: "SQL Server",
      type: "sqlserver",
      server: this.server,
      database: this.database,
    };
  }

  /**
   * Get database-specific query for listing tables
   */
  getListTablesQuery(): string {
    return "SELECT TABLE_NAME as name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME";
  }

  /**
   * Get database-specific query for describing a table
   * @param tableName Table name
   */
  getDescribeTableQuery(tableName: string): string {
    return `
      SELECT 
        c.COLUMN_NAME as name,
        c.DATA_TYPE as type,
        CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END as notnull,
        CASE WHEN pk.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 1 ELSE 0 END as pk,
        c.COLUMN_DEFAULT as dflt_value
      FROM 
        INFORMATION_SCHEMA.COLUMNS c
      LEFT JOIN 
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON c.TABLE_NAME = kcu.TABLE_NAME AND c.COLUMN_NAME = kcu.COLUMN_NAME
      LEFT JOIN 
        INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ON kcu.CONSTRAINT_NAME = pk.CONSTRAINT_NAME AND pk.CONSTRAINT_TYPE = 'PRIMARY KEY'
      WHERE 
        c.TABLE_NAME = '${tableName}'
      ORDER BY 
        c.ORDINAL_POSITION
    `;
  }

  /**
   * Helper to get the number of affected rows based on query type
   */
  private getAffectedRows(query: string, lastID: number): number {
    const queryType = query.trim().split(" ")[0].toUpperCase();
    if (queryType === "INSERT" && lastID > 0) {
      return 1;
    }
    return 0; // For SELECT, unknown for UPDATE/DELETE without additional query
  }
}
