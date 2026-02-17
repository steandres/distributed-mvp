import express from 'express';
import pkg from 'pg';
import fetch from 'node-fetch';
import dotenv from 'dotenv';
import { Kafka } from 'kafkajs';

dotenv.config();

const { Pool } = pkg;

const app = express();
app.use(express.json());

// 🔹 PostgreSQL connection
const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'user',
  password: process.env.DB_PASSWORD || 'password',
  database: process.env.DB_NAME || 'characters',
  port: 5432
});

// 🔹 Crear tabla si no existe
async function initDB() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS characters (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        status VARCHAR(50),
        species VARCHAR(50)
      )
    `);
    console.log("✅ Database ready");
  } catch (error) {
    console.error("❌ Database not ready. Retrying in 5 seconds...");
    setTimeout(initDB, 5000);
  }
}

initDB();


// 🔹 Endpoint que guarda personaje
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

// 🔹 Obtener personajes desde DB
app.get('/characters/db', async (req, res) => {
  const result = await pool.query('SELECT * FROM characters');
  res.json(result.rows);
});

// 🔹 Iniciar servidor HTTP
app.listen(3002, () => {
  console.log('🚀 Backend running on port 3002');
});


// ===============================
// 🔥 KAFKA CONSUMER RESILIENTE
// ===============================

async function startConsumer() {
  try {
    const kafka = new Kafka({
      clientId: 'backend-service',
      brokers: ['kafka:9092']
    });

    const consumer = kafka.consumer({ groupId: 'login-group' });

    await consumer.connect();
    await consumer.subscribe({ topic: 'user-login', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ message }) => {
        console.log("📥 Login event received:", message.value.toString());
      },
    });

    console.log("✅ Kafka consumer running");

  } catch (error) {
    console.error("❌ Kafka consumer failed. Retrying in 5 seconds...");
    setTimeout(startConsumer, 5000);
  }
}

// Iniciar consumidor sin bloquear el servidor
startConsumer();
