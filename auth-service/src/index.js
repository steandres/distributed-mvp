import express from 'express';
import jwt from 'jsonwebtoken';
import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: 'auth-service',
  brokers: ['kafka:9092']
});

const producer = kafka.producer();

//await producer.connect();

let kafkaEnabled = false;

try {
  await producer.connect();
  kafkaEnabled = true;
  console.log("Connected to Kafka");
} catch (error) {
  console.log("Kafka not available, running without it");
}


app.post('/login', async (req, res) => {
  const { username } = req.body;

  if (!username) {
    return res.status(400).json({ message: 'Username required' });
  }

  const token = jwt.sign({ username }, process.env.JWT_SECRET, {
    expiresIn: '1h'
  });

/*   await producer.send({
    topic: 'user-login',
    messages: [
      { value: JSON.stringify({ username, date: new Date() }) }
    ]
  }); */

  if (kafkaEnabled) {
  await producer.send({
    topic: 'user-login',
    messages: [
      { value: JSON.stringify({ username, date: new Date() }) }
    ]
  });
}


  res.json({ token });
});

app.listen(3001, () => {
  console.log('Auth service running on port 3001');
});
