package bench;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.IntStream;

@SpringBootApplication
@RestController
public class Application {

    private static final List<Map<String, Object>> users = new ArrayList<>();

    static {
        IntStream.rangeClosed(1, 100).forEach(i -> {
            Map<String, Object> u = new HashMap<>();
            u.put("id", i);
            u.put("name", "User " + i);
            users.add(u);
        });
    }

    @GetMapping("/plaintext")
    public String plaintext() {
        return "Hello, World!";
    }

    @GetMapping("/json")
    public Map<String, String> json() {
        return Map.of("message", "Hello, World!");
    }

    @GetMapping("/users/{id}")
    public Map<String, Object> getUser(@PathVariable String id) {
        Map<String, Object> m = new HashMap<>();
        m.put("id", 0);
        m.put("name", "User " + id);
        return m;
    }

    @GetMapping("/search")
    public Map<String, String> search(@RequestParam(defaultValue = "") String q) {
        return Map.of("query", q);
    }

    @PostMapping("/echo")
    public Map<String, Object> echo(@RequestBody Map<String, Object> body) {
        return body;
    }

    @GetMapping("/users")
    public List<Map<String, Object>> listUsers() {
        return users;
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
