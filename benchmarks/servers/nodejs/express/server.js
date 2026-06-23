const express = require('express');

const app = express();
app.use(express.json());

const users = Array.from({ length: 100 }, (_, i) => ({
  id: i + 1,
  name: `User ${i + 1}`,
}));

app.get('/plaintext', (req, res) => {
  res.send('Hello, World!');
});

app.get('/json', (req, res) => {
  res.json({ message: 'Hello, World!' });
});

app.get('/users/:id', (req, res) => {
  res.json({ id: 0, name: `User ${req.params.id}` });
});

app.get('/search', (req, res) => {
  res.json({ query: req.query.q || '' });
});

app.post('/echo', (req, res) => {
  res.json(req.body);
});

app.get('/users', (req, res) => {
  res.json(users);
});

app.listen(3201, () => {
  console.log('Express server on :3201');
});
