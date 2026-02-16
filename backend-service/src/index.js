import express from 'express';
import pkg from 'pg';
import fetch from 'node-fetch';
import dotenv from 'dotenv';

dotenv.config();

const { Pool } = pkg;

const app = express();
app.use(express.json());

const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'user',
  password: process.env.DB_PASSWORD || 'password',
  database: process.env.DB_NAME || 'characters',
  port: 5432
});

// Crear tabla si no existe
await pool.query(`
  CREATE TABLE IF NOT EXISTS characters (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    status VARCHAR(50),
    species VARCHAR(50)
  )
`);

app.get('/characters', async (req, res) => {
  try {
    const response = await fetch('https://rickandmortyapi.com/api/character/1');
    const data = await response.json();

    await pool.query(
      'INSERT INTO characters(name, status, species) VALUES($1, $2, $3)',
      [data.name, data.status, data.species]
    );

    res.json(data);

  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Error fetching character' });
  }
});

app.get('/characters/db', async (req, res) => {
  const result = await pool.query('SELECT * FROM characters');
  res.json(result.rows);
});

app.listen(3002, () => {
  console.log('Backend running on port 3002');
});
