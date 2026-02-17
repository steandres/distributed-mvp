import express from 'express';
import jwt from 'jsonwebtoken';
import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(express.json());

let producer = null;

// 🔹 Función resiliente de conexión a Kafka
async function connectKafka() {
  try {
    const kafka = new Kafka({
      clientId: 'auth-service',
      brokers: ['kafka:9092']
    });

    producer = kafka.producer();
    await producer.connect();
    console.log("✅ Connected to Kafka");
  } catch (error) {
    console.error("❌ Kafka connection failed. Retrying in 5 seconds...");
    setTimeout(connectKafka, 5000);
  }
}

// Iniciar conexión sin bloquear el servidor
connectKafka();

app.post('/login', async (req, res) => {
  const { username } = req.body;

  if (!username) {
    return res.status(400).json({ message: 'Username required' });
  }

  const token = jwt.sign(
    { username },
    process.env.JWT_SECRET,
    { expiresIn: '1h' }
  );

  // 🔹 Enviar evento solo si Kafka está conectado
  if (producer) {
    try {
      await producer.send({
        topic: 'user-login',
        messages: [
          { value: JSON.stringify({ username, date: new Date() }) }
        ]
      });
      console.log("📨 Login event sent to Kafka");
    } catch (err) {
      console.error("⚠️ Failed to send Kafka message:", err.message);
    }
  }

  res.json({ token });
});

app.listen(3001, () => {
  console.log('🚀 Auth service running on port 3001');
});
