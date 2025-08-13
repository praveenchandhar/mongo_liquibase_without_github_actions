// context: liquibase_test
// This file contains changes for the liquibase_test database

//createCollection

db.createCollection("liquibase_demo");

//insert data
db.getCollection("liquibase_demo").insertMany([
    { name: "John Doe", email: "john@example.com", age: 30 },
    { name: "Jane Smith", email: "jane@example.com", age: 25 }
]);

// Update operation
db.getCollection("liquibase_demo").updateOne(
    { email: "john@example.com" },
    { $set: { age: 31, lastUpdated: new Date() } }
);
