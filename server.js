import express from 'express';
import axios from 'axios';
import multer from 'multer';
import csv from "csvtojson";
import { Sequelize, DataTypes } from "sequelize";
import 'dotenv/config';

const app = express();
app.use(express.json());

const storage = multer.memoryStorage();
const upload = multer({ storage: storage });
const port = process.env.PORT || 3000; 
const map_typeof_to_sequelize = {
  "string": DataTypes.STRING,
  "number": DataTypes.DECIMAL,
  "boolean": DataTypes.BOOLEAN,
  "bigint": DataTypes.BIGINT
}

// Sequelize ORM object for easier database connection and queries
const sequelize = new Sequelize(process.env.DB_NAME, process.env.DB_USERNAME, process.env.DB_PASSWORD, {
  host: process.env.DB_HOST, 
  dialect: "postgres"
});

// Connect to database
sequelize.authenticate()
  .then(()=>{
    console.log("Connected sucessfully to the database.");
  })
  .catch((error)=>{
    console.log('Unable to connect to the database:', error);
  })

app.post('/api/upload', upload.single('data'), async (req, res) => {
  try {

    // Check if file uploaded
    if (!req.file) {
      return res.status(400).json({ message: 'No CSV file uploaded' });
    }

    // Convert CSV file buffer to string
    const csvBufferToString = req.file.buffer.toString();
    // Convert CSV string to array of objects with respect to each feature type
    const userData = await csv({checkType: true}).fromString(csvBufferToString);

    // Set table dynamic schema
    const databaseColumns = {};
    Object.keys(userData[0]).forEach(feature => { 
        databaseColumns[feature] = {
            type: map_typeof_to_sequelize[typeof(userData[0][feature])],
            allowNull: true,
        };
    });

    // Define table with name "data"
    const DataTable = sequelize.define("data", databaseColumns, {freezeTableName:true});

    // Create table inside th database
    await sequelize.sync(); 

    // Insert all records to the table
    try{
        await DataTable.bulkCreate(userData);
    }catch(error){
        await DataTable.drop();
        return res.status(500).json({ error: error });
    }

    // Send JSON to Flask service
    const response = await axios.post(`${process.env.AI_SERVICE_URL}/upload`, userData);

    return res.status(200).json(response.data);

  } catch (error) {
    res.status(500).json({error: error});
  }
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
