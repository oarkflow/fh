#include <drogon/drogon.h>
#include <string>
#include <vector>

using namespace drogon;

struct User {
    int id;
    std::string name;
};

std::vector<User> users;

void initUsers() {
    users.reserve(100);
    for (int i = 1; i <= 100; i++) {
        users.push_back({i, "User " + std::to_string(i)});
    }
}

std::string usersJson() {
    std::string json = "[";
    for (size_t i = 0; i < users.size(); i++) {
        if (i > 0) json += ",";
        json += "{\"id\":" + std::to_string(users[i].id) + ",\"name\":\"" + users[i].name + "\"}";
    }
    json += "]";
    return json;
}

int main() {
    initUsers();

    auto& app = app().setLogLevel(trantor::Logger::kWarn);

    app.registerHandler("/plaintext", [](const HttpRequestPtr& req,
                                         std::function<void(const HttpResponsePtr&)>&& callback) {
        auto resp = HttpResponse::newHttpResponse();
        resp->setBody("Hello, World!");
        callback(resp);
    });

    app.registerHandler("/json", [](const HttpRequestPtr& req,
                                    std::function<void(const HttpResponsePtr&)>&& callback) {
        Json::Value json;
        json["message"] = "Hello, World!";
        auto resp = HttpResponse::newHttpJsonResponse(json);
        callback(resp);
    });

    app.registerHandler("/users/{id}", [](const HttpRequestPtr& req,
                                          std::function<void(const HttpResponsePtr&)>&& callback,
                                          const std::string& id) {
        Json::Value json;
        json["id"] = 0;
        json["name"] = "User " + id;
        auto resp = HttpResponse::newHttpJsonResponse(json);
        callback(resp);
    });

    app.registerHandler("/search", [](const HttpRequestPtr& req,
                                      std::function<void(const HttpResponsePtr&)>&& callback) {
        auto q = req->getParameter("q");
        Json::Value json;
        json["query"] = q;
        auto resp = HttpResponse::newHttpJsonResponse(json);
        callback(resp);
    });

    app.registerHandler("/echo", [](const HttpRequestPtr& req,
                                    std::function<void(const HttpResponsePtr&)>&& callback) {
        auto resp = HttpResponse::newHttpJsonResponse(req->getJsonObject());
        callback(resp);
    }, {Post});

    app.registerHandler("/users", [](const HttpRequestPtr& req,
                                     std::function<void(const HttpResponsePtr&)>&& callback) {
        auto resp = HttpResponse::newHttpResponse();
        resp->setContentTypeCode(CT_APPLICATION_JSON);
        resp->setBody(usersJson());
        callback(resp);
    });

    app.addListener("0.0.0.0", 3401).run();
}
