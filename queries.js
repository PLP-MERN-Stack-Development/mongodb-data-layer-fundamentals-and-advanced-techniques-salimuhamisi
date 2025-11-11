// queries.js
const { MongoClient } = require("mongodb");

const uri = "mongodb://127.0.0.1:27017";
const client = new MongoClient(uri);

async function run() {
  try {
    await client.connect();
    const db = client.db("plp_bookstore");
    const books = db.collection("books");

    // TASK 2: Basic Queries

    // 1. Find all books in a specific genre
    console.log("Books in genre 'Fiction':");
    console.log(await books.find({ genre: "Fiction" }).toArray());

    // 2. Find books published after a certain year
    console.log("Books published after 2015:");
    console.log(await books.find({ published_year: { $gt: 2015 } }).toArray());

    // 3. Find books by a specific author
    console.log("Books by 'John Doe':");
    console.log(await books.find({ author: "John Doe" }).toArray());

    // 4. Update the price of a specific book
    await books.updateOne({ title: "Book Title 1" }, { $set: { price: 25.99 } });
    console.log("Updated price of Book Title 1");

    // 5. Delete a book by its title
    await books.deleteOne({ title: "Old Book" });
    console.log("Deleted book with title 'Old Book'");

    // TASK 3: Advanced Queries

    // 1. Find books that are in stock and published after 2010
    console.log(await books.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray());

    // 2. Projection (only title, author, price)
    console.log(await books.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).toArray());

    // 3. Sort by price ascending
    console.log(await books.find().sort({ price: 1 }).toArray());

    // 4. Sort by price descending
    console.log(await books.find().sort({ price: -1 }).toArray());

    // 5. Pagination (limit + skip)
    console.log("Page 1:");
    console.log(await books.find().limit(5).toArray());
    console.log("Page 2:");
    console.log(await books.find().skip(5).limit(5).toArray());

    // TASK 4: Aggregation Pipelines

    // 1. Average price of books by genre
    console.log(await books.aggregate([
      { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } }
    ]).toArray());

    // 2. Author with the most books
    console.log(await books.aggregate([
      { $group: { _id: "$author", totalBooks: { $sum: 1 } } },
      { $sort: { totalBooks: -1 } },
      { $limit: 1 }
    ]).toArray());

    // 3. Group books by publication decade and count them
    console.log(await books.aggregate([
      { $addFields: { decade: { $subtract: [{ $divide: ["$published_year", 10] }, { $mod: [{ $divide: ["$published_year", 10] }, 1] }] } } },
      { $group: { _id: "$decade", totalBooks: { $sum: 1 } } },
      { $sort: { _id: 1 } }
    ]).toArray());

    // -----------------------
    // 🧩 TASK 5: Indexing
    // -----------------------

    await books.createIndex({ title: 1 });
    await books.createIndex({ author: 1, published_year: 1 });

    // Use explain to check performance
    console.log("Explain query with index:");
    console.log(await books.find({ title: "Book Title 1" }).explain("executionStats"));

  } finally {
    await client.close();
  }
}

run().catch(console.dir);
