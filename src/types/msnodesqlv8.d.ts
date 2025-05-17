/**
 * Type definitions for msnodesqlv8
 * Native SQL Server driver for Node.js using Microsoft's SQL ODBC driver
 */

declare module "msnodesqlv8" {
  /**
   * Execute a query with optional parameters
   * @param connectionString ODBC connection string
   * @param query SQL query to execute
   * @param callback Callback function for results
   */
  export function query(
    connectionString: string,
    query: string,
    callback: (err: Error | null, results: any[] | any) => void
  ): void;

  /**
   * Execute a query with parameters
   * @param connectionString ODBC connection string
   * @param query SQL query to execute
   * @param params Array of parameter values
   * @param callback Callback function for results
   */
  export function query(
    connectionString: string,
    query: string,
    params: any[],
    callback: (err: Error | null, results: any[] | any) => void
  ): void;

  /**
   * Open a connection to the database
   * @param connectionString ODBC connection string
   * @param callback Callback function
   */
  export function open(
    connectionString: string,
    callback: (err: Error | null, connection: Connection) => void
  ): void;

  /**
   * Connection interface
   */
  export interface Connection {
    /**
     * Close the connection
     * @param callback Callback function
     */
    close(callback: (err: Error | null) => void): void;

    /**
     * Execute a query with optional parameters
     * @param query SQL query to execute
     * @param callback Callback function for results
     */
    query(
      query: string,
      callback: (err: Error | null, results: any[] | any) => void
    ): void;

    /**
     * Execute a query with parameters
     * @param query SQL query to execute
     * @param params Array of parameter values
     * @param callback Callback function for results
     */
    query(
      query: string,
      params: any[],
      callback: (err: Error | null, results: any[] | any) => void
    ): void;

    /**
     * Begin a transaction
     * @param callback Callback function
     */
    beginTransaction(callback: (err: Error | null) => void): void;

    /**
     * Commit a transaction
     * @param callback Callback function
     */
    commit(callback: (err: Error | null) => void): void;

    /**
     * Rollback a transaction
     * @param callback Callback function
     */
    rollback(callback: (err: Error | null) => void): void;
  }
}
