const { Client } = require('pg');
const fs = require('fs');
const csv = require('csv-parser');
require('dotenv').config();

// PostgreSQL Client Setup using environment variables
const client = new Client({
  host: process.env.PGHOST,
  database: process.env.PGDATABASE,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  port: 5432,  // Default PostgreSQL port
  ssl: process.env.PGSSL === 'true' ? { rejectUnauthorized: false } : false
});

client.connect();

async function syncProducts() {
  const productsFromCsv = [];
  
  // Read CSV File
  fs.createReadStream('products.csv')
    .pipe(csv())
    .on('data', (row) => {
      productsFromCsv.push(row);
    })
    .on('end', async () => {
      console.log('CSV file successfully processed');
      
      const productsFromDb = await getProductsFromDb();
      await syncWithDatabase(productsFromCsv, productsFromDb);
      
      client.end();
    });
}

async function getProductsFromDb() {
  const res = await client.query('SELECT * FROM public.products ORDER BY sku');
  return res.rows;
}

async function syncWithDatabase(csvProducts, dbProducts) {
  const csvSkus = csvProducts.map(product => product.sku);
  const dbSkus = dbProducts.map(product => product.sku);

  const skusToAdd = csvSkus.filter(sku => !dbSkus.includes(sku));
  const skusToDelete = dbSkus.filter(sku => !csvSkus.includes(sku));
  const skusToUpdate = csvSkus.filter(sku => dbSkus.includes(sku));

  for (const sku of skusToAdd) {
    const product = csvProducts.find(product => product.sku === sku);
    await addProductToDb(product);
    console.log(`Added SKU ${sku} to the database.`);
  }

  for (const sku of skusToDelete) {
    await deleteProductFromDb(sku);
    console.log(`Deleted SKU ${sku} from the database.`);
  }

  for (const sku of skusToUpdate) {
    const product = csvProducts.find(product => product.sku === sku);
    await updateProductInDb(product);
    console.log(`Updated SKU ${sku} in the database.`);
  }
}

async function addProductToDb(product) {
  const query = 'INSERT INTO public.products (sku, name, price, count) VALUES ($1, $2, $3, $4)';
  const values = [product.sku, product.name, parseInt(product.price), parseInt(product.count)];
  await client.query(query, values);
}

async function updateProductInDb(product) {
  const query = 'UPDATE public.products SET name = $1, price = $2, count = $3 WHERE sku = $4';
  const values = [product.name, parseInt(product.price), parseInt(product.count), product.sku];
  await client.query(query, values);
}

async function deleteProductFromDb(sku) {
  const query = 'DELETE FROM public.products WHERE sku = $1';
  await client.query(query, [sku]);
}

// Example usage: sync products from CSV
syncProducts();