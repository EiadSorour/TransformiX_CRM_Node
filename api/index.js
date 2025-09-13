import 'dotenv/config';
import express from 'express';
import axios from 'axios';
import multer from 'multer';
import csv from "csvtojson";
import cors from "cors";
import FormData from 'form-data';
import { v2 as cloudinary } from 'cloudinary';
import { neon } from '@neondatabase/serverless';

// Cloudinary configuration
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

const app = express();

app.use(cors({
  origin: process.env.FRONTEND_URL, 
  allowedHeaders: ["Content-Type", "Authorization"],
  credentials: true,
  methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]
}));
app.use(express.json());

const getDbClient = () => neon(process.env.DATABASE_URL);
const storage = multer.memoryStorage(); // Use memoryStorage
const upload = multer({ storage: storage });
const port = process.env.PORT || 3000; 

// getDbClient().query('CREATE TABLE IF NOT EXISTS "UploadedFile" ("id" SERIAL PRIMARY KEY,"publicId" VARCHAR(255) NOT NULL UNIQUE,"secureUrl" VARCHAR(255) NOT NULL,"originalFilename" VARCHAR(255) NOT NULL,"createdAt" TIMESTAMP WITH TIME ZONE NOT NULL,"updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL);').then(()=>{
//   console.log("UploadedFile table created."); 
  
// })

function generateDynamicCreateTableSql(tableName, userData, inferSqlTypeFunction) {
  if (!tableName) {
      throw new Error("Table name must be provided.");
  }
  if (!userData || userData.length === 0) {
      // If userData is empty, we can't infer schema. Return a basic table or throw an error.
      // For now, let's assume we need at least one row to infer.
      throw new Error("User data is empty, cannot infer schema for dynamic table creation.");
  }

  const columns = ['"id" SERIAL PRIMARY KEY'];

  const firstRow = userData[0];
  Object.keys(firstRow).forEach(feature => {
      // Sanitize feature name to be a valid SQL identifier
      const sanitizedFeature = feature.replace(/[^a-zA-Z0-9_]/g, '_');
      const sqlType = inferSqlTypeFunction(feature, userData); // Use the provided inference function
      columns.push(`"${sanitizedFeature}" ${sqlType} `); // Add inferred type
  });

  // Add standard timestamp columns
  columns.push('"createdAt" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP');
  columns.push('"updatedAt" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP');


  return `CREATE TABLE IF NOT EXISTS "${tableName}" (\n    ${columns.join(',\n    ')}\n);`;
}

const inferSqlType = (columnName, data) => {
  let hasString = false;
  let hasNumber = false;
  let hasBoolean = false;
  let hasDate = false;

  for (const row of data) {
    const value = row[columnName];
    if (value === null || value === undefined || value === '') continue; // Ignore empty values for type inference

    if (typeof value === 'string') {
      hasString = true;
      if (!isNaN(Number(value)) && !isNaN(parseFloat(value))) {
        hasNumber = true;
      } else if (value.toLowerCase() === 'true' || value.toLowerCase() === 'false') {
        hasBoolean = true;
      } else if (!isNaN(new Date(value).getTime())) {
        hasDate = true;
      }
    } else if (typeof value === 'number') {
      hasNumber = true;
    } else if (typeof value === 'boolean') {
      hasBoolean = true;
    } else if (value instanceof Date) {
      hasDate = true;
    }
  }

  // Prioritize specific types if purely that type, otherwise broaden
  if (hasNumber && !hasString && !hasDate && !hasBoolean) return 'NUMERIC';
  if (hasDate && !hasString && !hasNumber && !hasBoolean) return 'TIMESTAMP WITH TIME ZONE';
  if (hasBoolean && !hasString && !hasNumber && !hasDate) return 'BOOLEAN';
  
  // If it's a mix or default to string
  if (hasString) return 'VARCHAR(255)'; // Default string length, adjust as needed for larger text
  if (hasNumber) return 'NUMERIC'; // Fallback to numeric
  if (hasBoolean) return 'BOOLEAN'; // Fallback to boolean
  if (hasDate) return 'TIMESTAMP WITH TIME ZONE'; // Fallback to date

  return 'VARCHAR(255)'; // Ultimate fallback
};

function generateDynamicInsertSql(tableName, userData) {
  if (!tableName) {
      throw new Error("Table name must be provided.");
  }
  if (!userData || userData.length === 0) {
      return ""; // No data to insert, return empty query or handle as an error
  }

  const firstRecord = userData[0];
  // Dynamically get column names from the first record's keys
  let columnNames = Object.keys(firstRecord);

  // Sanitize column names for SQL identifiers
  let sanitizedColumnNames = columnNames.map(name => `"${name.replace(/[^a-zA-Z0-9_]/g, '_')}"`);

  // Add standard timestamp columns to the column list
  sanitizedColumnNames.push('"createdAt"', '"updatedAt"');

  const valuesClauses = userData.map(record => {
      const values = columnNames.map(column => {
          const value = record[column];
          if (value === null || value === undefined || value === '') { // Also handle empty strings as NULL
              return 'NULL';
          } else if (typeof value === 'string') {
              // Escape single quotes within strings for SQL
              return `'${value.replace(/'/g, "''")}'`;
          } else if (typeof value === 'number' || typeof value === 'boolean') {
              return String(value);
          } else if (value instanceof Date) {
              return `'${value.toISOString()}'`; // Convert Date objects to ISO string for TIMESTAMP WITH TIME ZONE
          }
          // Fallback for any other types, treat as string
          return `'${String(value).replace(/'/g, "''")}'`;
      });
      // Append CURRENT_TIMESTAMP for createdAt and updatedAt directly into each row's values
      return `(${values.join(', ')}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`;
  });

  return `INSERT INTO "${tableName}" (${sanitizedColumnNames.join(', ')})\nVALUES\n${valuesClauses.join(',\n')};`;
}

async function generatePaginatedDataQuery(sql, tableName, offset, limit) {
  if (!tableName) {
    console.log('Error: Table name missing');
    throw new Error("Table name must be provided.");
  }

  console.log('Starting generatePaginatedDataQuery', { tableName, offset, limit });

  // Query 1: Fetch column names
  console.log('Query 1: Fetching column names from information_schema.columns');
  const columnsData = await sql`SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = ${tableName}
    ORDER BY ordinal_position`;
  console.log('Query 1 result:', columnsData);

  // Ensure columnsData is an array
  if (!Array.isArray(columnsData)) {
    console.log('Error: columnsData is not an array:', columnsData);
    throw new Error("Expected sql() to return an array of objects for column names.");
  }

  // Extract column names and filter out excluded ones
  const columnsToExclude = ['createdAt', 'updatedAt'];
  const desiredColumns = columnsData
    .map(row => row.column_name)
    .filter(col => !columnsToExclude.includes(col));
  console.log('Desired columns:', desiredColumns);

  if (desiredColumns.length === 0) {
    console.log('Error: No selectable columns found for table', tableName);
    throw new Error(`No selectable columns found for table "${tableName}" after exclusion.`);
  }

  // Sanitize column names
  const selectClause = desiredColumns.map(col => `"${col.replace(/[^a-zA-Z0-9_]/g, '_')}"`).join(', ');
  console.log('Generated SELECT clause:', selectClause);

  // Query 2: Count all records
  console.log('Query 2: Counting records in', tableName);
  const countQuery = `SELECT COUNT(*) FROM "${tableName}"`;
  console.log('Generated COUNT query:', countQuery);
  const countResult = await sql.query(countQuery); // Use sql() for raw query
  console.log('Query 2 result:', countResult);
  const count = countResult[0].count; // Adjust based on actual result structure

  // Query 3: Fetch paginated records
  console.log('Query 3: Fetching paginated records');
  const rowsQuery = `
    SELECT ${selectClause}
    FROM "${tableName}"
    ORDER BY "id" ASC
    OFFSET ${offset}
    LIMIT ${limit}`;
  console.log('Generated ROWS query:', rowsQuery);
  const rows = await sql.query(rowsQuery); // Use sql() for raw query
  console.log('Query 3 result:', rows);

  return {
    count,
    rows
  };
}

app.get("/api/test", async (req,res)=>{
  console.log("test");
  return res.status(200).json({message: "okay", error: "this is error alo"})
})

app.post("/api/smart-question-examples", async (req,res)=>{
  const sql = getDbClient();
  
  const uploadedFile = await sql`SELECT * FROM "UploadedFile" WHERE "publicId" = 'data.csv';`;

  const csvDataBuffer = await axios.get(uploadedFile[0].secureUrl, { responseType: 'arraybuffer' });
  const formData = new FormData();
  formData.append('data', Buffer.from(csvDataBuffer.data), { filename: uploadedFile[0].originalFilename, contentType: 'text/csv' });

  const response = await axios.post(
    `${process.env.AI_SERVICE_URL}/smart-question-examples`,
    formData,
    {
      headers: formData.getHeaders(),
    }
  );

  res.status(200).json(response.data);
});

app.post("/api/question-answer", async (req,res)=>{
  const sql = getDbClient();
  
  const uploadedFile = await sql`SELECT * FROM "UploadedFile" WHERE "publicId" = 'data.csv';`;

  const csvDataBuffer = await axios.get(uploadedFile[0].secureUrl, { responseType: 'arraybuffer' });
  const formData = new FormData();


  formData.append('data', Buffer.from(csvDataBuffer.data), { filename: uploadedFile[0].originalFilename, contentType: 'text/csv' });
  formData.append('question', req.body.question);

  const response = await axios.post(
    `${process.env.AI_SERVICE_URL}/question-answer`,
    formData,
    {
      headers: formData.getHeaders(),
    }
  );

  res.status(200).json(response.data);
})

app.delete("/api/table", async (req,res)=>{
  const sql = getDbClient();

  try {
    // 1. Delete the "data" table from the database
    await sql`DROP TABLE IF EXISTS "data";`;

    // 2. Delete the uploaded file metadata from PostgreSQL and optionally from Cloudinary
    const uploadedFile = await sql`SELECT * FROM "UploadedFile" WHERE "publicId" = 'data.csv';`;
    if (uploadedFile.length > 0) {
      // Delete from Cloudinary
      await sql`DELETE FROM "UploadedFile" WHERE "publicId" = 'data.csv';`; 
      await cloudinary.uploader.destroy(uploadedFile[0].publicId);
    }

    return res.status(200).json({ message: 'Table "data" and uploaded file data deleted successfully.' });
  } catch (error) {
    console.error("Error deleting table or file:", error);
    return res.status(500).json({ error: error.message || 'Failed to delete table or file.' });
  }
})

app.get("/api/display-cards", async (req,res)=>{
  const sql = getDbClient();
  
  try {
    const uploadedFile = await sql`SELECT * FROM "UploadedFile" WHERE "publicId" = 'data.csv';`;

    if (uploadedFile.length == 0) {
      return res.status(200).json({
        "domain": "No data uploaded",
        "missing_data_ratio": 0,
        "num_numeric_columns": 0,
        "total_columns": 0,
        "total_rows": 0
      });
    }

    const csvDataBuffer = await axios.get(uploadedFile[0].secureUrl, { responseType: 'arraybuffer' });
    const formData = new FormData();
    formData.append('data', Buffer.from(csvDataBuffer.data), { filename: uploadedFile[0].originalFilename, contentType: 'text/csv' });

  const response = await axios.post(
    `${process.env.AI_SERVICE_URL}/upload`,
    formData,
    {
      headers: formData.getHeaders(),
    }
  );
  res.status(200).send(response.data);
  } catch (error) {
    console.error("Error in /api/display-cards:", error);
    res.status(500).json({ error: error.message || 'Failed to fetch card data.' });
  }
})

app.get("/api/check-table", async (req,res)=>{
  const sql = getDbClient();
  
  try {
    const uploadedFile = await sql`SELECT * FROM "UploadedFile" WHERE "publicId" = 'data.csv';`;
    if (uploadedFile.length > 0) {
      return res.status(200).json({status: true});
    } else {
      return res.status(200).json({status: false});
    }
  } catch (error) {
    console.error("Error in /api/check-table:", error);
    res.status(500).json({ error: error.message || 'Failed to check table status.' });
  }
})

app.get("/api/data", async (req, res) => {
  const sql = getDbClient();

  try {
    console.log('Starting /api/data', { query: req.query });

    // Check if data table exists
    console.log('Query 0: Checking if data table exists');
    const dataTable = await sql`SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_name = 'data'
    )`;
    console.log('Query 0 result:', dataTable);

    if (!dataTable[0].exists) {
      console.log('No data table found');
      return res.status(404).json({ error: 'Data table not found. Please upload a CSV first.' });
    }

    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const offset = (page - 1) * limit;
    console.log('Pagination params:', { page, limit, offset });

    // Fetch paginated data
    console.log('Calling generatePaginatedDataQuery');
    const { count, rows } = await generatePaginatedDataQuery(sql, "data", offset, limit);
    console.log('generatePaginatedDataQuery result:', { count, rows });

    const lastPage = Math.ceil(count / limit);
    console.log('Response data:', { lastPage, totalCount: count, rowsLength: rows.length });

    res.status(200).json({ data: rows, lastPage, totalCount: count });
  } catch (error) {
    console.error('Error in /api/data:', error.message, error.stack);
    res.status(500).json({ error: error });
  }
});

app.post('/api/upload', upload.single('data'), async (req, res) => {
  const sql = getDbClient();

  try {
    console.log('Starting /api/upload');

    // Check if file uploaded
    if (!req.file) {
      console.log('No file uploaded');
      return res.status(400).json({ message: 'No CSV file uploaded' });
    }
    console.log('File received:', req.file.originalname, req.file.mimetype);

    // Check existing file
    console.log('Query 1: Checking for existing file in UploadedFile');
    const existingFile = await sql`SELECT * FROM "UploadedFile" WHERE "publicId" = 'data.csv'`;
    console.log('Query 1 result:', existingFile);

    if (existingFile.length > 0) {
      console.log('Query 2: Checking if data table exists');
      const dataTable = await sql`SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'data'
      )`;
      console.log('Query 2 result:', dataTable);

      if (dataTable[0].exists) {
        console.log('Query 3: Dropping data table');
        await sql`DROP TABLE IF EXISTS "data"`;
        console.log('Query 3 completed');
      }

      console.log('Query 4: Deleting from Cloudinary:', existingFile[0].publicId);
      await cloudinary.uploader.destroy(existingFile[0].publicId);
      console.log('Query 4: Deleting from UploadedFile');
      await sql`DELETE FROM "UploadedFile" WHERE "publicId" = 'data.csv'`;
      console.log('Query 4 completed');
    }

    // Upload to Cloudinary
    console.log('Uploading to Cloudinary');
    const cloudinaryUploadResult = await cloudinary.uploader.upload(
      `data:${req.file.mimetype};base64,${req.file.buffer.toString('base64')}`,
      {
        resource_type: 'raw',
        public_id: 'data.csv',
        format: 'csv',
      }
    );
    console.log('Cloudinary upload result:', cloudinaryUploadResult);

    // Insert into UploadedFile
    console.log('Query 5: Inserting into UploadedFile');
    await sql`INSERT INTO "UploadedFile" ("publicId", "secureUrl", "originalFilename", "createdAt", "updatedAt")
      VALUES (${cloudinaryUploadResult.public_id}, ${cloudinaryUploadResult.secure_url}, ${req.file.originalname}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`;
    console.log('Query 5 completed');

    const newUploadedFile = (await sql`SELECT * FROM "UploadedFile" WHERE "publicId" = 'data.csv'`)[0];
    console.log('Query 6: Fetched new uploaded file:', newUploadedFile);

    // Parse CSV
    console.log('Parsing CSV');
    const userData = await csv({ checkType: true }).fromString(req.file.buffer.toString());
    console.log('CSV parsed, rows:', userData.length);

    // Send to AI service
    console.log('Fetching CSV from Cloudinary for AI service');
    const csvDataBuffer = await axios.get(newUploadedFile.secureUrl, { responseType: 'arraybuffer' });
    const formData = new FormData();
    formData.append('data', Buffer.from(csvDataBuffer.data), { filename: newUploadedFile.originalFilename, contentType: req.file.mimetype });
    console.log('Sending to AI service:', process.env.AI_SERVICE_URL);
    const response = await axios.post(
      `${process.env.AI_SERVICE_URL}/upload`,
      formData,
      { headers: formData.getHeaders() }
    );
    console.log('AI service response:', response.data);

    // Create table
    console.log('Query 7: Generating and executing CREATE TABLE');
    const createTableSql = generateDynamicCreateTableSql("data", userData, inferSqlType);
    console.log('Generated CREATE TABLE SQL:', createTableSql);
    await sql.query(createTableSql);
    console.log('Query 7 completed');

    // Insert data
    console.log('Query 8: Generating and executing INSERT');
    const insertSql = generateDynamicInsertSql("data", userData);
    console.log('Generated INSERT SQL:', insertSql);
    await sql.query(insertSql);
    console.log('Query 8 completed');

    console.log('Upload completed successfully');
    return res.status(200).json(response.data);
  } catch (error) {
    console.error('Error in /api/upload:', error.message, error.stack);
    res.status(500).json({ error: error });
  }
});

// production
export default app;


// development
// app.listen(port , ()=>{
//   console.log(`Server is running on port ${port}`);
// });