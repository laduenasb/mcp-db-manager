import { DbAdapter } from "./adapter.js";
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
  driverVersion?: string; // ODBC Driver version (default: "17")
  options?: any;
}

/**
 * SQL Server database adapter implementation
 */
export class SqlServerAdapter implements DbAdapter {
  private connectionString: string = "";
  private server: string;
  private database: string;
  private user?: string;
  private password?: string;
  private driverVersion: string;

  constructor(connectionInfo: SqlServerConnectionInfo) {
    // Store original server string for metadata
    this.server = connectionInfo.server;
    this.database = connectionInfo.database;
    this.user = connectionInfo.user;
    this.password = connectionInfo.password;
    this.driverVersion = connectionInfo.driverVersion || "17";

    console.error(`[INFO] Using native SQL Server driver (msnodesqlv8)`);

    // Prepare connection string
    this.prepareConnectionString(connectionInfo);
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
        `[INFO] Connecting to SQL Server: ${this.server}, Database: ${this.database} using msnodesqlv8 driver`
      );

      console.error(
        `[DEBUG] Connection string template: ${this.connectionString.replace(
          /PWD=.*?;/,
          "PWD=*****;"
        )}`
      );

      // Verify the connection by running a simple query
      await this.testConnection();
      console.error(`[INFO] SQL Server connection established successfully`);
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
    // Prepare the query with parameter substitution
    const { preparedQuery, preparedParams } = this.prepareQuery(query, params);

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
   * Execute a SQL query that modifies data
   * @param query SQL query to execute
   * @param params Query parameters
   * @returns Promise with result info
   */
  async run(
    query: string,
    params: any[] = []
  ): Promise<{ changes: number; lastID: number }> {
    // Check if it's an INSERT query to handle lastID
    const isInsert = query.trim().toUpperCase().startsWith("INSERT");

    // For INSERT queries, we need to get the last inserted ID
    if (isInsert) {
      // Add SCOPE_IDENTITY() to get the last ID
      const modifiedQuery = `${query}; SELECT SCOPE_IDENTITY() AS LastID`;
      const { preparedQuery, preparedParams } = this.prepareQuery(
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
      const { preparedQuery, preparedParams } = this.prepareQuery(
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
   * Prepare a query with parameters
   */
  private prepareQuery(
    query: string,
    params: any[]
  ): { preparedQuery: string; preparedParams: any[] } {
    // If there are no parameters, return the query as is
    if (!params || params.length === 0) {
      return { preparedQuery: query, preparedParams: [] };
    }

    // The driver already supports ? placeholders, so we just pass the params array
    return { preparedQuery: query, preparedParams: params };
  }

  /**
   * Execute multiple SQL statements
   * @param query SQL statements to execute
   * @returns Promise that resolves when execution completes
   */
  async exec(query: string): Promise<void> {
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
   * Close the database connection
   */
  async close(): Promise<void> {
    // msnodesqlv8 doesn't maintain persistent connections that need explicit closing
    return Promise.resolve();
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
}
