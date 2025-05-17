// Test script for SQL Server Adapter with msnodesqlv8 driver
import { createDbAdapter } from '../dist/src/db/adapter.js';

// Configuration - Update these values for your environment
const config = {
  // SQL Server connection config
  server: 'SERVERNAME\\SQLEXPRESS',  // Replace with your server\instance
  database: 'master',                // Using master for testing
  user: 'sa',                        // Replace with your username
  password: 'YourPassword',          // Replace with your password
  
  // Driver selection options
  // Uncomment one of these to explicitly select a driver
  // driver: 'msnodesqlv8',
  // driver: 'mssql',
};

// Test basic connectivity and query
async function testConnection() {
  console.log('Testing SQL Server connection...');
  
  try {
    // Create adapter
    const db = createDbAdapter('sqlserver', config);
    
    // Initialize connection
    console.log('Initializing connection...');
    await db.init();
    
    // Get server version
    console.log('Executing test query...');
    const result = await db.all('SELECT @@VERSION AS Version');
    console.log(`Connected to: ${result[0].Version}`);
    
    // Close connection
    console.log('Closing connection...');
    await db.close();
    
    console.log('Connection test completed successfully');
    return true;
  } catch (error) {
    console.error('Connection test failed:', error.message);
    return false;
  }
}

// Test parameter binding
async function testParameters() {
  console.log('\nTesting parameter binding...');
  
  try {
    // Create adapter
    const db = createDbAdapter('sqlserver', config);
    
    // Initialize connection
    await db.init();
    
    // Test with different parameter types
    const tests = [
      { query: 'SELECT ? AS StringValue', params: ['test string'], desc: 'String parameter' },
      { query: 'SELECT ? AS NumberValue', params: [42], desc: 'Number parameter' },
      { query: 'SELECT ? AS BoolValue', params: [true], desc: 'Boolean parameter' },
      { query: 'SELECT ? AS DateValue', params: [new Date()], desc: 'Date parameter' },
      { query: 'SELECT ?, ? AS MultipleValues', params: ['Value1', 'Value2'], desc: 'Multiple parameters' }
    ];
    
    for (const test of tests) {
      console.log(`Testing ${test.desc}...`);
      const result = await db.all(test.query, test.params);
      console.log(`Result:`, result[0]);
    }
    
    // Close connection
    await db.close();
    
    console.log('Parameter binding test completed successfully');
    return true;
  } catch (error) {
    console.error('Parameter binding test failed:', error.message);
    return false;
  }
}

// Test table operations
async function testTableOperations() {
  console.log('\nTesting table operations...');
  
  try {
    // Create adapter
    const db = createDbAdapter('sqlserver', config);
    
    // Initialize connection
    await db.init();
    
    // Create a test table
    const tableName = `test_table_${Math.floor(Math.random() * 10000)}`;
    console.log(`Creating test table: ${tableName}...`);
    
    await db.exec(`
      CREATE TABLE ${tableName} (
        id INT PRIMARY KEY IDENTITY(1,1),
        name NVARCHAR(100) NOT NULL,
        value INT,
        created_at DATETIME DEFAULT GETDATE()
      )
    `);
    
    // Insert data
    console.log('Inserting test data...');
    const insertResult1 = await db.run(
      `INSERT INTO ${tableName} (name, value) VALUES (?, ?)`,
      ['Test Item 1', 100]
    );
    console.log(`Inserted row with ID: ${insertResult1.lastID}`);
    
    const insertResult2 = await db.run(
      `INSERT INTO ${tableName} (name, value) VALUES (?, ?)`,
      ['Test Item 2', 200]
    );
    console.log(`Inserted row with ID: ${insertResult2.lastID}`);
    
    // Query data
    console.log('Querying data...');
    const rows = await db.all(`SELECT * FROM ${tableName} ORDER BY id`);
    console.log(`Found ${rows.length} rows:`);
    console.log(rows);
    
    // Update data
    console.log('Updating data...');
    await db.run(
      `UPDATE ${tableName} SET value = ? WHERE id = ?`,
      [150, insertResult1.lastID]
    );
    
    // Query updated data
    const updatedRows = await db.all(`SELECT * FROM ${tableName} ORDER BY id`);
    console.log('Updated data:');
    console.log(updatedRows);
    
    // Clean up - drop the test table
    console.log('Cleaning up...');
    await db.exec(`DROP TABLE ${tableName}`);
    
    // Close connection
    await db.close();
    
    console.log('Table operations test completed successfully');
    return true;
  } catch (error) {
    console.error('Table operations test failed:', error.message);
    return false;
  }
}

// Run all tests
async function runTests() {
  console.log('=== SQL Server Adapter Tests ===');
  console.log('Configuration:', { ...config, password: '*****' });
  
  const connectionTest = await testConnection();
  if (!connectionTest) return;
  
  const parameterTest = await testParameters();
  if (!parameterTest) return;
  
  const tableTest = await testTableOperations();
  if (!tableTest) return;
  
  console.log('\n=== All tests completed successfully ===');
}

runTests().catch(error => {
  console.error('Test execution failed:', error);
}); 