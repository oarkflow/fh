const fastify = require('fastify');

const app = fastify({ logger: false });

const users = Array.from({ length: 100 }, (_, i) => ({
  id: i + 1,
  name: `User ${i + 1}`,
}));

app.get('/plaintext', async (req, reply) => {
  return reply.send('Hello, World!');
});

app.get('/json', async (req, reply) => {
  return { message: 'Hello, World!' };
});

app.get('/users/:id', async (req, reply) => {
  return { id: 0, name: `User ${req.params.id}` };
});

app.get('/search', async (req, reply) => {
  return { query: req.query.q || '' };
});

app.post('/echo', async (req, reply) => {
  return req.body;
});

app.get('/users', async (req, reply) => {
  return users;
});

app.listen({ port: 3202 }, () => {
  console.log('Fastify server on :3202');
});
