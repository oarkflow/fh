<?php
use Psr\Http\Message\ResponseInterface as Response;
use Psr\Http\Message\ServerRequestInterface as Request;
use Slim\Factory\AppFactory;

require __DIR__ . '/vendor/autoload.php';

$app = AppFactory::create();

$users = [];
for ($i = 1; $i <= 100; $i++) {
    $users[] = ['id' => $i, 'name' => "User $i"];
}

$app->get('/plaintext', function (Request $request, Response $response) {
    $response->getBody()->write('Hello, World!');
    return $response;
});

$app->get('/json', function (Request $request, Response $response) {
    $payload = json_encode(['message' => 'Hello, World!']);
    $response->getBody()->write($payload);
    return $response->withHeader('Content-Type', 'application/json');
});

$app->get('/users/{id}', function (Request $request, Response $response, array $args) {
    $id = $args['id'];
    $payload = json_encode(['id' => 0, 'name' => "User $id"]);
    $response->getBody()->write($payload);
    return $response->withHeader('Content-Type', 'application/json');
});

$app->get('/search', function (Request $request, Response $response) {
    $params = $request->getQueryParams();
    $q = $params['q'] ?? '';
    $payload = json_encode(['query' => $q]);
    $response->getBody()->write($payload);
    return $response->withHeader('Content-Type', 'application/json');
});

$app->post('/echo', function (Request $request, Response $response) {
    $body = $request->getBody()->getContents();
    $response->getBody()->write($body);
    return $response->withHeader('Content-Type', 'application/json');
});

$app->get('/users', function (Request $request, Response $response) {
    global $users;
    $payload = json_encode($users);
    $response->getBody()->write($payload);
    return $response->withHeader('Content-Type', 'application/json');
});

$app->run();
