import express from 'express';
import fetch from 'node-fetch';
import bodyParser from 'body-parser';

const app = express();

app.set('view engine', 'ejs');
app.set('views', './views');

app.use(bodyParser.urlencoded({ extended: true }));

// Login page
app.get('/', (req, res) => {
  res.render('login');
});

// Handle login
app.post('/login', async (req, res) => {
  const { username } = req.body;

  const response = await fetch('http://auth-service:3001/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username })
  });

  const data = await response.json();

  if (data.token) {
    res.redirect('/dashboard');
  } else {
    res.send('Login failed');
  }
});

// Dashboard
app.get('/dashboard', async (req, res) => {
  const response = await fetch('http://backend-service:3002/characters');
  const character = await response.json();

  res.render('dashboard', { character });
});

app.listen(3000, () => {
  console.log('Frontend running on port 3000');
});
