import express from 'express';
import axios from 'axios';
import multer from 'multer';
import csv from "csvtojson";
import { Sequelize, DataTypes } from "sequelize";
import cors from "cors";
import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import FormData from 'form-data';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

app.use(cors({
  origin: process.env.FRONTEND_URL, 
  allowedHeaders: ["Content-Type", "Authorization"],
  credentials: true,
  methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]
}));
app.use(express.json());

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const uploadDir = path.join(__dirname, 'uploads');
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir);
    }
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    cb(null, 'data.csv'); // Name the file "data.csv"
  },
});
const upload = multer({ storage: storage });
const port = process.env.PORT || 3000; 
let DataTable;
const map_typeof_to_sequelize = {
  "string": DataTypes.STRING,
  "number": DataTypes.DECIMAL,
  "boolean": DataTypes.BOOLEAN,
  "bigint": DataTypes.BIGINT,
  "date": DataTypes.DATE,
};


// Function to infer Sequelize data type based on column values
const inferSequelizeType = (columnName, data) => {
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

  if (hasNumber && !hasString) return DataTypes.DECIMAL; // If it's purely numbers, use DECIMAL
  if (hasDate && !hasString && !hasNumber && !hasBoolean) return DataTypes.DATE; // If purely dates
  if (hasBoolean && !hasString && !hasNumber && !hasDate) return DataTypes.BOOLEAN; // If purely booleans
  if (hasString && hasNumber) return DataTypes.TEXT; // If it's a mix of string and number, assume TEXT to be safe
  if (hasString) return DataTypes.STRING; // Default to string if any string is found
  if (hasNumber) return DataTypes.DECIMAL; // Fallback to number
  if (hasBoolean) return DataTypes.BOOLEAN; // Fallback to boolean
  if (hasDate) return DataTypes.DATE; // Fallback to date

  return DataTypes.STRING; // Default fallback
};

// Sequelize ORM object for easier database connection and queries
const sequelize = new Sequelize(process.env.DATABASE_URL, {
  dialect: "postgres",
  dialectOptions: {
    ssl: {
      require: true,
      rejectUnauthorized: false, // For self-signed certificates, if any. Adjust as needed.
    },
  },
});

// Connect to database
sequelize.authenticate()
  .then(()=>{
    console.log("Connected sucessfully to the database.");
  })
  .catch((error)=>{
    console.log('Unable to connect to the database:', error);
  })

app.get("/api/test", (req,res)=>{
  console.log("test");
  return res.status(200).json({message: "okay", error: "this is error alo"})
})

app.delete("/api/table", async (req,res)=>{
  try {
    // 1. Delete the "data" table from the database
    await sequelize.query('DROP TABLE IF EXISTS "data";');
    DataTable = null;

    // 2. Delete the "data.csv" file from the uploads folder
    const csvFilePath = path.join(__dirname, 'uploads', 'data.csv');
    if (fs.existsSync(csvFilePath)) {
      fs.unlinkSync(csvFilePath);
      console.log(`Deleted file: ${csvFilePath}`);
    }

    return res.status(200).json({ message: 'Table "data" and file "data.csv" deleted successfully.' });
  } catch (error) {
    console.error("Error deleting table or file:", error);
    return res.status(500).json({ error: error.message || 'Failed to delete table or file.' });
  }
})

app.get("/api/display-cards", async (req,res)=>{
  const csvFilePath = path.join(__dirname, 'uploads', 'data.csv');
  const formData = new FormData();
  const csvReadStream = fs.createReadStream(csvFilePath);
  formData.append('data', csvReadStream, 'data.csv'); // 'data' is the field name, 'data.csv' is the filename

  const response = await axios.post(
    `${process.env.AI_SERVICE_URL}/upload`,
    formData,
    {
      headers: formData.getHeaders(),
    }
  );
  res.status(200).send(response.data);
})

app.get("/api/check-table", async (req,res)=>{
  // Check if table already exists in the database
  const tableExists = await sequelize.getQueryInterface().tableExists({tableName: "data"});
  if(tableExists){
      return res.status(200).json({status: true});
    }else{
      return res.status(200).json({status: false});
  }
})

app.get("/api/data", async (req,res)=>{
  try{
    if (!DataTable) {
      return res.status(404).json({ error: 'Data table not found. Please upload a CSV first.' });
    }

    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10; // Default limit to 10
    const offset = (page - 1) * limit;

    const { count, rows } = await DataTable.findAndCountAll({
      offset: offset,
      limit: limit,
      attributes: {
        exclude: ['createdAt', 'updatedAt']
      }
    });
    const lastPage = Math.ceil(count / limit);
    res.status(200).json({ data: rows, lastPage, totalCount: count });
  }catch(error){
    console.log("error" , error);
    res.status(500).json({ error: 'Failed to fetch data' });
  }
})

app.post('/api/upload', upload.single('data'), async (req, res) => {
  try {

    // Check if file uploaded
    if (!req.file) {
      return res.status(400).json({ message: 'No CSV file uploaded' });
    }

    // Read the uploaded CSV file from disk
    const csvFilePath = path.join(__dirname, 'uploads', 'data.csv');
    const userData = await csv({checkType: true}).fromFile(csvFilePath);
    const formData = new FormData();
    const csvReadStream = fs.createReadStream(csvFilePath);
    formData.append('data', csvReadStream, 'data.csv'); // 'data' is the field name, 'data.csv' is the filename

    const response = await axios.post(
      `${process.env.AI_SERVICE_URL}/upload`,
      formData,
      {
        headers: formData.getHeaders(),
      }
    );

    // Set table dynamic schema
    const databaseColumns = {};
    Object.keys(userData[0]).forEach(feature => { 
        databaseColumns[feature] = {
            type: inferSequelizeType(feature, userData),
            allowNull: true,
        };
    });

    // Define table with name "data"
    DataTable = sequelize.define("data", databaseColumns, {freezeTableName:true});

    // Create table inside th database
    await sequelize.sync(); 

    // Pre-process userData to ensure type consistency
    const processedUserData = userData.map(row => {
      const newRow = {};
      for (const feature in row) {
        const value = row[feature];
        const inferredType = inferSequelizeType(feature, userData); // Re-infer type for individual value processing

        if (value === null || value === undefined || value === '') {
          newRow[feature] = null; // Treat empty string as null for all types
        } else if (inferredType === DataTypes.DECIMAL) {
          const numValue = Number(value);
          newRow[feature] = isNaN(numValue) ? null : numValue; // Convert to number, or null if invalid
        } else if (inferredType === DataTypes.BOOLEAN) {
          if (typeof value === 'string') {
            newRow[feature] = value.toLowerCase() === 'true';
          } else {
            newRow[feature] = Boolean(value);
          }
        } else if (inferredType === DataTypes.DATE) {
          const dateValue = new Date(value);
          newRow[feature] = isNaN(dateValue.getTime()) ? null : dateValue; // Convert to Date object, or null if invalid
        } else {
          newRow[feature] = String(value); // Ensure all others are strings
        }
      }
      return newRow;
    });

    // Insert all records to the table
    try{
        await DataTable.bulkCreate(processedUserData);
    }catch(error){
        await DataTable.drop();
        return res.status(500).json({ error: error });
    }

    return res.status(200).json(response.data);

  } catch (error) {
    res.status(500).json({error: error});
  }
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
